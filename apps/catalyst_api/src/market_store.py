"""Market query executor — deterministic entity-grain queries over the spine-derived
L2 Lance datasets (gtm_entity_behavior_rollup / gtm_entity_code_lanes / gtm_sam_entities).

LLM-free and SQL-injection-impossible by construction: the compile discipline is
``lance_store``'s, reused verbatim — columns come ONLY from the registry
(market_registry.ENTITY_FIELDS), ops/types/enums are validated fail-closed
(``MapCompileError`` → 422 at the route), and every caller string passes through
``_sql_str`` quote-doubling before it touches a Lance filter.

EXECUTION PLAN (entity grain):
  1. Scalar clauses split by source table and compile to one predicate per table
     (rollup / entities). Lane clauses (explicit ``{"lane": {...}}`` objects OR the
     workbench pseudo-fields prime_naics/sub_naics/prime_psc/sub_psc) each compile to a
     predicate over gtm_entity_code_lanes.
  2. Lane + rollup predicates each scan to a UEI set (uei column only, streamed,
     NULL-guarded). Sets INTERSECT; an empty intersection short-circuits (no further
     scans).
  3. An entities predicate joins in one of three ways:
       • sole source → fast path: exact count_rows + a streamed limit scan (never
         materializes a millions-row UEI set);
       • small candidate set → semi-join via chunked ``uei IN (...)`` scans (chunks of
         ~500 — Lance IN lists are batched, never unbounded);
       • large candidate set → one predicate scan of entities, intersected in-process
         (cheaper than thousands of point scans).
  4. Hydration: the surviving UEIs (sorted, capped) point-lookup BOTH 1-row/uei tables
     via chunked IN scans and merge on uei — an entity absent from the rollup (no
     contract behavior) hydrates its rollup columns as NULLs, honestly.

Lance traps respected: scanners are ONE-SHOT (a fresh scanner per scan);
``scanner(limit=)`` is never combined with a filter (the pylance limit-before-filter
planner under-returns — streamed batches cut at the bound instead); IN lists are
NULL-guarded and chunked.
"""
from __future__ import annotations

import logging
import re
import threading
from datetime import date as dt_date
from typing import Any

from . import config, market_registry
from .lance_store import (
    MapCompileError,
    _dataset,
    _map_clause_sql,
    _map_jsonable,
    _sql_str,
    valid_uei,
)

log = logging.getLogger("catalyst_api.market_store")

# Hard result-row bound. Hydration is 2 chunked IN scans per 500 rows, so the cap keeps
# the worst case at a handful of indexed point scans (the workbench sends limit=200).
MARKET_HARD_ROW_CAP = 1_000
MARKET_DEFAULT_LIMIT = 100
# IN-list chunk size (the batched-IN trap: never hand Lance an unbounded IN list).
IN_CHUNK = 500
# Semi-join crossover: at most this many candidate UEIs go through chunked IN scans;
# a larger candidate set flips to one predicate scan + in-process intersection.
SEMI_JOIN_MAX = 10_000
# The entities `uei` filter's IN-list bound (the cross-grain join key, registry v5):
# one house chunk (the batched-IN trap: never hand Lance an unbounded IN list). A
# larger set is a 422 — callers chunk client-side.
UEI_IN_CAP = 500

# A NAICS/PSC code is short alnum (NAICS 2-6 digits; PSC 1-4 alnum). Validated before
# interpolation — defense-in-depth alongside _sql_str quote-doubling.
_CODE_OK = re.compile(r"^[A-Za-z0-9]{1,10}$")

_SOURCE_URIS = {
    "rollup": lambda: config.GTM_ENTITY_BEHAVIOR_ROLLUP_URI,
    "entities": lambda: config.GTM_SAM_ENTITIES_URI,
    "lanes": lambda: config.GTM_ENTITY_CODE_LANES_URI,
    "geo": lambda: config.GTM_ENTITY_GEO_URI,
    # capability-inference projections (kind → dataset; see market_registry verb doctrine)
    "primeable": lambda: config.GTM_INFERRED_PRIMEABLE_URI,
    "subbable": lambda: config.GTM_INFERRED_SUBBABLE_URI,
}

# Table-grain sources (market_registry.TableGrainSpec.source → URI).
TABLE_GRAIN_URIS = {
    "prime_awards": lambda: config.FPDS_PRIME_AWARD_STATE_URI,
    "transactions": lambda: config.FPDS_CANONICAL_TXN_URI,
}


# ── Low-level I/O seams (monkeypatch targets for the hermetic tests) ──────────
def _count_rows(uri: str, predicate: str | None) -> int:
    """Exact match count (count_rows pushdown — no row materialization)."""
    return _dataset(uri).count_rows(filter=predicate)


def _scan_to_pylist(uri: str, columns: list[str], predicate: str | None) -> list[dict[str, Any]]:
    """One fresh scanner (one-shot by contract) → rows. Bounded by construction: every
    caller passes either a chunked IN predicate or a projection it will stream-cut."""
    return _dataset(uri).scanner(columns=columns, filter=predicate).to_table().to_pylist()


def _stream_ueis(uri: str, predicate: str | None, limit: int) -> list[str]:
    """First ``limit`` non-null UEIs of a filtered scan, via streamed batches (never
    ``scanner(limit=)`` — the pylance limit-before-filter planner under-returns)."""
    scanner = _dataset(uri).scanner(columns=["uei"], filter=predicate)
    out: list[str] = []
    for batch in scanner.to_batches():
        for u in batch.column("uei").to_pylist():
            if u:
                out.append(u)
                if len(out) >= limit:
                    return out
    return out


def _uei_set(uri: str, predicate: str | None) -> set[str]:
    """The full matching UEI set for a predicate (uei projection only, streamed,
    NULL-guarded). Callers keep this off the multi-million-row unfiltered paths."""
    scanner = _dataset(uri).scanner(columns=["uei"], filter=predicate)
    out: set[str] = set()
    for batch in scanner.to_batches():
        out.update(u for u in batch.column("uei").to_pylist() if u)
    return out


# ── Compile: filters → per-table predicates + lane predicates ─────────────────
def _chunks(ids: list[str], size: int = IN_CHUNK) -> list[list[str]]:
    return [ids[i:i + size] for i in range(0, len(ids), size)]


def _in_predicate(column: str, ids: list[str]) -> str:
    """NULL-guarded, escaped IN list over a chunk of ids."""
    lits = ", ".join(_sql_str(v) for v in ids if v)
    if not lits:
        raise MapCompileError("empty id list for IN predicate")
    return f"{column} IN ({lits})"


def _validate_lane(lane: Any) -> dict[str, Any]:
    """Fail-closed lane-object validation → a normalized lane dict. Every key is
    checked against the registry contract; codes are charset-validated BEFORE they can
    reach a predicate (then _sql_str-escaped anyway — defense in depth)."""
    if not isinstance(lane, dict):
        raise MapCompileError("lane must be an object")
    unknown = set(lane) - market_registry.LANE_ALLOWED_KEYS
    if unknown:
        raise MapCompileError(f"unknown lane key(s) {sorted(unknown)!r}")
    side = lane.get("side")
    if side not in market_registry.LANE_SIDES:
        raise MapCompileError(f"lane.side must be one of {list(market_registry.LANE_SIDES)}")
    code_type = lane.get("code_type")
    if code_type not in market_registry.LANE_CODE_TYPES:
        raise MapCompileError(f"lane.code_type must be one of {list(market_registry.LANE_CODE_TYPES)}")
    codes = lane.get("codes")
    if not isinstance(codes, list) or not codes:
        raise MapCompileError("lane.codes must be a non-empty array of code strings")
    for c in codes:
        if not isinstance(c, str) or not _CODE_OK.match(c):
            raise MapCompileError(f"lane code {c!r} is not a valid NAICS/PSC code")
    out: dict[str, Any] = {"side": side, "code_type": code_type, "codes": list(codes)}
    for key in market_registry.LANE_MIN_OBL_COLUMNS:
        if key in lane:
            v = lane[key]
            if isinstance(v, bool) or not isinstance(v, (int, float)) or v < 0:
                raise MapCompileError(f"lane.{key} must be a non-negative number")
            out[key] = v
    return out


def _compile_lane_predicate(lane: dict[str, Any]) -> str:
    """Normalized lane dict → gtm_entity_code_lanes filter string. side/code_type ride
    the BITMAPs, code IN(...) the BTREE; thresholds bind to the LANE's obligation."""
    parts = [
        f"side = {_sql_str(lane['side'])}",
        f"code_type = {_sql_str(lane['code_type'])}",
        _in_predicate("code", lane["codes"]),
    ]
    for key, col in market_registry.LANE_MIN_OBL_COLUMNS.items():
        if key in lane:
            parts.append(f"{col} >= {float(lane[key])!r}")
    return " AND ".join(parts)


def _validate_inferred(inf: Any) -> dict[str, Any]:
    """Fail-closed inferred-object validation → a normalized dict. kind/code_type/codes
    are required; the support floor is OPTIONAL and caller-supplied (pre-weighting: the
    engine never defaults a threshold). Codes charset-validated then escaped."""
    if not isinstance(inf, dict):
        raise MapCompileError("inferred must be an object")
    unknown = set(inf) - market_registry.INFERRED_ALLOWED_KEYS
    if unknown:
        raise MapCompileError(f"unknown inferred key(s) {sorted(unknown)!r}")
    kind = inf.get("kind")
    if kind not in market_registry.INFERRED_KINDS:
        raise MapCompileError(f"inferred.kind must be one of {list(market_registry.INFERRED_KINDS)}")
    code_type = inf.get("code_type")
    if code_type not in market_registry.LANE_CODE_TYPES:
        raise MapCompileError(f"inferred.code_type must be one of {list(market_registry.LANE_CODE_TYPES)}")
    codes = inf.get("codes")
    if not isinstance(codes, list) or not codes:
        raise MapCompileError("inferred.codes must be a non-empty array of code strings")
    for c in codes:
        if not isinstance(c, str) or not _CODE_OK.match(c):
            raise MapCompileError(f"inferred code {c!r} is not a valid NAICS/PSC code")
    out: dict[str, Any] = {"kind": kind, "code_type": code_type, "codes": list(codes)}
    key = market_registry.INFERRED_MIN_SUPPORT_KEY
    if key in inf:
        v = inf[key]
        if isinstance(v, bool) or not isinstance(v, int) or v < 1:
            raise MapCompileError(f"inferred.{key} must be a positive whole number")
        out[key] = v
    return out


def _compile_inferred_predicate(inf: dict[str, Any]) -> str:
    """Normalized inferred dict → filter string over the matching projection dataset
    (grain (uei, code_type, code); BTREE code serves the IN pushdown)."""
    parts = [
        f"code_type = {_sql_str(inf['code_type'])}",
        _in_predicate("code", inf["codes"]),
    ]
    key = market_registry.INFERRED_MIN_SUPPORT_KEY
    if key in inf:
        parts.append(f"supporting_bothsider_firm_ct >= {int(inf[key])}")
    return " AND ".join(parts)


def compile_market_filters(
    filters: list[dict[str, Any]], today: "dt_date | None" = None
) -> tuple[dict[str, str | None], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """Validate + compile the request's filter list. Each entry is EXACTLY ONE of: a
    scalar clause ``{field, op, value}`` (registry field; lane/inferred pseudo-fields
    desugar to their object forms), a lane object ``{"lane": {...}}``, or an inferred
    object ``{"inferred": {...}}`` — anything else raises ``MapCompileError`` (→ 422).
    Returns ``(predicates_by_source, lanes, inferred, executed)``."""
    today = today or dt_date.today()
    parts: dict[str, list[str]] = {src: [] for src in market_registry.SOURCES}
    lanes: list[dict[str, Any]] = []
    inferred: list[dict[str, Any]] = []
    executed: list[dict[str, Any]] = []
    for clause in filters:
        if not isinstance(clause, dict):
            raise MapCompileError("each filter must be an object")
        kinds_present = [k for k in ("lane", "inferred", "field") if clause.get(k) is not None]
        if len(kinds_present) > 1:
            raise MapCompileError(
                "a filter is EXACTLY ONE of {field, op, value} | {lane: {...}} | {inferred: {...}}")
        if clause.get("lane") is not None:
            lane = _validate_lane(clause["lane"])
            lanes.append(lane)
            executed.append({"lane": lane})
            continue
        if clause.get("inferred") is not None:
            inf = _validate_inferred(clause["inferred"])
            inferred.append(inf)
            executed.append({"inferred": inf})
            continue
        field, op, value = clause.get("field"), clause.get("op"), clause.get("value")
        if field is None:
            raise MapCompileError("a filter needs a 'field' (or a 'lane'/'inferred' object)")
        pseudo = market_registry.LANE_PSEUDO_FIELDS.get(field)
        if pseudo is not None:
            lane = _desugar_lane_pseudo(field, pseudo, op, value)
            lanes.append(lane)
            executed.append({"lane": lane})
            continue
        inf_pseudo = market_registry.INFERRED_PSEUDO_FIELDS.get(field)
        if inf_pseudo is not None:
            inf = _desugar_inferred_pseudo(field, inf_pseudo, op, value)
            inferred.append(inf)
            executed.append({"inferred": inf})
            continue
        spec = market_registry.ENTITY_FIELDS.get(field)
        if spec is None:
            raise MapCompileError(f"field {field!r} not in the entity registry")
        if field == "uei":
            # the cross-grain join key: charset-validated fail-closed (defense in
            # depth alongside _sql_str escaping) and IN-list capped at one house chunk.
            values = value if isinstance(value, list) else [value]
            if op == "in" and isinstance(value, list) and len(value) > UEI_IN_CAP:
                raise MapCompileError(
                    f"uei 'in' list exceeds the {UEI_IN_CAP} cap ({len(value)} given) "
                    "— chunk client-side")
            for v in values:
                if not isinstance(v, str) or not valid_uei(v.strip()):
                    raise MapCompileError(
                        f"uei value {v!r} is not a 12-char alphanumeric SAM UEI")
        if spec.expr is not None:
            # registry-level compiled boolean expression ('=' bool only; both branches
            # written out explicitly — see the field's description for the exact SQL).
            if op != "=" or not isinstance(value, bool):
                raise MapCompileError(f"field {field!r} takes '=' with a boolean value")
            parts[spec.source].append(spec.expr[0] if value else spec.expr[1])
        else:
            parts[spec.source].append(_map_clause_sql(spec, op, value, today))
        executed.append({"field": field, "op": op, "value": value})
    predicates = {src: (" AND ".join(p) if p else None) for src, p in parts.items()}
    return predicates, lanes, inferred, executed


def _desugar_inferred_pseudo(field: str, pseudo: tuple[str, str], op: Any, value: Any) -> dict[str, Any]:
    """A workbench scalar clause on inferred_primeable_*/inferred_subbable_* → the
    equivalent inferred object (codes only; the support floor needs the object form)."""
    kind, code_type = pseudo
    if op == "=":
        codes = [value]
    elif op == "in":
        if not isinstance(value, list) or not value:
            raise MapCompileError(f"{field}: 'in' needs a non-empty array value")
        codes = list(value)
    else:
        raise MapCompileError(f"op {op!r} not allowed for inferred field {field!r} ('=' or 'in')")
    return _validate_inferred({"kind": kind, "code_type": code_type, "codes": codes})


def _desugar_lane_pseudo(field: str, pseudo: tuple[str, str], op: Any, value: Any) -> dict[str, Any]:
    """A workbench scalar clause on prime_naics/sub_naics/prime_psc/sub_psc → the
    equivalent lane object (codes only; thresholds need the explicit lane form)."""
    side, code_type = pseudo
    if op == "=":
        codes = [value]
    elif op == "in":
        if not isinstance(value, list) or not value:
            raise MapCompileError(f"{field}: 'in' needs a non-empty array value")
        codes = list(value)
    else:
        raise MapCompileError(f"op {op!r} not allowed for lane field {field!r} ('=' or 'in')")
    return _validate_lane({"side": side, "code_type": code_type, "codes": codes})


# ── Execute ───────────────────────────────────────────────────────────────────
def _intersect(sets: list[set[str]]) -> set[str]:
    sets = sorted(sets, key=len)
    out = sets[0]
    for s in sets[1:]:
        out = out & s
        if not out:
            break
    return out


def _rows_by_uei(uri: str, ueis: list[str], columns: list[str]) -> dict[str, dict[str, Any]]:
    """Chunked ``uei IN (...)`` point scans → {uei: row}. Bounded by the row cap."""
    out: dict[str, dict[str, Any]] = {}
    for chunk in _chunks(ueis):
        for row in _scan_to_pylist(uri, columns, _in_predicate("uei", chunk)):
            u = row.get("uei")
            if u:
                out[u] = row
    return out


def _hydrate(ueis: list[str]) -> list[dict[str, Any]]:
    """Join gtm_sam_entities + gtm_entity_behavior_rollup + the gtm_entity_geo HQ geo
    sidecar (LEFT) on the surviving UEIs. A UEI absent from a table hydrates that
    table's columns as NULLs (honest absence — e.g. a SAM-registered entity with no
    contract behavior has no rollup row; an ungeocodable entity has no geo row, so the
    map adapter emits geometry:null, never a fake centroid). Values are JSON-shaped
    (date32 → ISO) and keyed in RESULT_ROW_ORDER."""
    if not ueis:
        return []
    ent = _rows_by_uei(_SOURCE_URIS["entities"](), ueis,
                       list(market_registry.RESULT_COLUMNS_ENTITIES))
    rol = _rows_by_uei(_SOURCE_URIS["rollup"](), ueis,
                       list(market_registry.RESULT_COLUMNS_ROLLUP))
    geo = _rows_by_uei(_SOURCE_URIS["geo"](), ueis,
                       list(market_registry.RESULT_COLUMNS_GEO))
    rows: list[dict[str, Any]] = []
    for u in ueis:
        merged = {"uei": u, **(ent.get(u) or {}), **(rol.get(u) or {}), **(geo.get(u) or {})}
        rows.append({k: _map_jsonable(merged.get(k)) for k in market_registry.RESULT_ROW_ORDER})
    return rows


def to_entity_features(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    """Result rows → GeoJSON features for the /api/v1/map/entities/query adapter.
    latitude/longitude LEAVE the properties and become Point geometry as ``[lon, lat]``
    (GeoJSON axis order) when both are present; a row without coordinates is served with
    ``geometry: null`` (valid per RFC 7946 §3.2) so the TABLE view stays complete while
    the dot layer skips it. geo_precision STAYS in properties ('address' | 'county' |
    None). Returns ``(features, plottable)``."""
    features: list[dict[str, Any]] = []
    plottable = 0
    for row in rows:
        props = dict(row)
        lat, lon = props.pop("latitude", None), props.pop("longitude", None)
        geometry = None
        if lat is not None and lon is not None:
            geometry = {"type": "Point", "coordinates": [lon, lat]}
            plottable += 1
        features.append({"type": "Feature", "geometry": geometry, "properties": props})
    return features, plottable


def _try_sidecar(fn_name: str, *args) -> "tuple[bool, dict[str, Any] | None]":
    """Attempt the query-sidecar executor (Phase 4 wiring, flag-gated via
    QUERY_SIDECAR_EXECUTE). Returns (served, result). NotServable → (False, None)
    silently — deliberate tiering (e.g. transactions row queries stay on Lance).
    Any other failure logs and falls back, so this lane can never be worse than
    the Lance path. MapCompileError re-raises (validation semantics unchanged)."""
    from . import sidecar_executor

    if not sidecar_executor.enabled():
        return False, None
    try:
        return True, getattr(sidecar_executor, fn_name)(*args)
    except sidecar_executor.NotServable:
        return False, None
    except MapCompileError:
        raise
    except Exception as exc:  # noqa: BLE001 — availability fallback, never a hard fail
        print(f"[sidecar] {fn_name} failed ({type(exc).__name__}: {exc}); falling back to Lance")
        return False, None


def execute_entity_query(
    filters: list[dict[str, Any]], limit: int | None, today: "dt_date | None" = None
) -> dict[str, Any]:
    """The entity-grain plan (module docstring). Returns
    ``{rows, total, returned, capped, executed}``; ``total`` is the EXACT match count
    (set-intersection size, or count_rows pushdown on the single-table fast paths).
    Raises ``MapCompileError`` (→ 422) on any off-registry filter."""
    served, result = _try_sidecar("execute_entity_query", filters, limit, today)
    if served:
        return result
    predicates, lanes, inferred, executed = compile_market_filters(filters, today)
    cap = max(1, min(limit or MARKET_DEFAULT_LIMIT, MARKET_HARD_ROW_CAP))

    sets: list[set[str]] = []
    for lane in lanes:
        sets.append(_uei_set(_SOURCE_URIS["lanes"](), _compile_lane_predicate(lane)))
    for inf in inferred:
        # semi-join against the matching inference projection (BTREE uei + code),
        # identical shape to the demonstrated-lane semi-join.
        sets.append(_uei_set(_SOURCE_URIS[inf["kind"]](), _compile_inferred_predicate(inf)))
    if predicates["rollup"] is not None:
        sets.append(_uei_set(_SOURCE_URIS["rollup"](), predicates["rollup"]))

    entities_pred = predicates["entities"]
    entities_uri = _SOURCE_URIS["entities"]()

    if sets and not all(sets):
        # Empty-set short-circuit: some source matched nothing — no further scans.
        final: set[str] = set()
        total = 0
        ueis: list[str] = []
    elif entities_pred is not None and sets:
        candidate = _intersect(sets)
        if not candidate:
            final, total, ueis = set(), 0, []
        elif len(candidate) <= SEMI_JOIN_MAX:
            # Semi-join: chunked (uei IN chunk) AND entities_pred point scans.
            final = set()
            for chunk in _chunks(sorted(candidate)):
                pred = f"{_in_predicate('uei', chunk)} AND {entities_pred}"
                final.update(_uei_set(entities_uri, pred))
            total = len(final)
            ueis = sorted(final)[:cap]
        else:
            # Wide candidate set: one predicate scan of entities, intersect in-process.
            final = _uei_set(entities_uri, entities_pred) & candidate
            total = len(final)
            ueis = sorted(final)[:cap]
    elif entities_pred is not None:
        # Entities-only fast path: exact pushdown count + streamed limit scan (never
        # materializes a millions-row UEI set for a broad predicate like in_sam=true).
        total = _count_rows(entities_uri, entities_pred)
        ueis = _stream_ueis(entities_uri, entities_pred, cap)
    elif sets:
        final = _intersect(sets)
        total = len(final)
        ueis = sorted(final)[:cap]
    else:
        # No filters: the base universe is the full entity spine, capped.
        total = _count_rows(entities_uri, None)
        ueis = _stream_ueis(entities_uri, None, cap)

    rows = _hydrate(ueis)
    return {
        "rows": rows,
        "total": total,
        "returned": len(rows),
        "capped": total > len(rows),
        "executed": {"grain": "entity", "filters": executed, "limit": cap},
    }


# ── Table grains (prime_awards / transactions): one predicate over one table ──
def _stream_rows(uri: str, columns: list[str], predicate: str | None, limit: int) -> list[dict[str, Any]]:
    """First ``limit`` rows of a filtered projection via streamed batches (never
    ``scanner(limit=)`` — the pylance limit-before-filter planner under-returns)."""
    scanner = _dataset(uri).scanner(columns=columns, filter=predicate)
    out: list[dict[str, Any]] = []
    for batch in scanner.to_batches():
        out.extend(batch.to_pylist())
        if len(out) >= limit:
            break
    return out[:limit]


def compile_table_filters(
    spec, filters: list[dict[str, Any]], today: "dt_date | None" = None
) -> tuple[str | None, list[dict[str, Any]]]:
    """Validate + compile a table grain's filter list to ONE predicate. Scalar
    ``{field, op, value}`` clauses only — lane objects are entity-grain vocabulary and
    are rejected here. ``require_filter`` grains refuse an empty filter list (a full
    scan of an 82.9M/108M-row table is never an accident worth serving)."""
    today = today or dt_date.today()
    parts: list[str] = []
    executed: list[dict[str, Any]] = []
    for clause in filters:
        if not isinstance(clause, dict):
            raise MapCompileError("each filter must be an object")
        if clause.get("lane") is not None:
            raise MapCompileError(
                f"lane predicates apply to the entity grain only, not {spec.dataset_key!r}")
        field, op, value = clause.get("field"), clause.get("op"), clause.get("value")
        if field is None:
            raise MapCompileError("a filter needs a 'field'")
        fspec = spec.fields.get(field)
        if fspec is None:
            raise MapCompileError(f"field {field!r} not in the {spec.dataset_key} registry")
        parts.append(_map_clause_sql(fspec, op, value, today))
        executed.append({"field": field, "op": op, "value": value})
    if not parts and spec.require_filter:
        raise MapCompileError(
            f"{spec.dataset_key} requires at least one filter — an unfiltered scan of "
            f"this table is not served (use a recency, code, agency, or UEI predicate)")
    return (" AND ".join(parts) if parts else None), executed


def _hydrate_recipient_hq(rows: list[dict[str, Any]]) -> None:
    """LEFT-join the gtm_entity_geo HQ sidecar onto table-grain rows by recipient_uei
    (in place). Rows whose recipient has no sidecar row get NULL lat/lon/precision —
    the adapter emits geometry:null for them. This is the RECIPIENT'S HQ, not place of
    performance (the award state table carries no PoP columns — probed 2026-07-05)."""
    ueis = sorted({r.get("recipient_uei") for r in rows if r.get("recipient_uei")})
    geo = _rows_by_uei(_SOURCE_URIS["geo"](), ueis,
                       list(market_registry.RESULT_COLUMNS_GEO)) if ueis else {}
    for r in rows:
        g = geo.get(r.get("recipient_uei")) or {}
        r["latitude"] = g.get("latitude")
        r["longitude"] = g.get("longitude")
        r["geo_precision"] = g.get("geo_precision")


def execute_table_query(
    spec, filters: list[dict[str, Any]], limit: int | None, today: "dt_date | None" = None
) -> dict[str, Any]:
    """Single-table grain plan: compile ONE predicate (fail-closed, min-one-filter on
    require_filter grains) → exact ``count_rows`` total → streamed limit scan of the
    result projection → optional recipient-HQ geometry hydration. Returns the same
    ``{rows, total, returned, capped, executed}`` contract as the entity executor."""
    served, result = _try_sidecar("execute_table_query", spec, filters, limit, today)
    if served:
        return result
    predicate, executed = compile_table_filters(spec, filters, today)
    cap = max(1, min(limit or MARKET_DEFAULT_LIMIT, MARKET_HARD_ROW_CAP))
    uri = TABLE_GRAIN_URIS[spec.source]()
    total = _count_rows(uri, predicate)
    raw = _stream_rows(uri, list(spec.result_columns), predicate, cap) if total else []
    rows = [{k: _map_jsonable(r.get(k)) for k in spec.result_columns} for r in raw]
    if spec.geometry == "recipient_hq":
        _hydrate_recipient_hq(rows)
    return {
        "rows": rows,
        "total": total,
        "returned": len(rows),
        "capped": total > len(rows),
        "executed": {"grain": spec.grain, "filters": executed, "limit": cap},
    }


# ── Recipient collapse (Cycle-1: "companies that …" as ONE honest call) ────────
# A request mode on the TABLE grains: instead of raw rows, return the DISTINCT
# recipients behind the matching rows, each with match_ct / Σ$ / last date. The
# aggregation streams the filtered scan up to COLLAPSE_SCAN_CAP rows (a named bound
# — its application is explicit on the wire via scan_capped, never silent).
COLLAPSE_MODES = ("recipients",)
COLLAPSE_SCAN_CAP = 500_000
# Per-grain aggregation columns: (money column, date column).
_COLLAPSE_COLUMNS = {
    "transactions": ("federal_action_obligation", "action_date"),
    "prime_awards": ("life_to_date_obligated", "last_action_date"),
}


def execute_table_collapse(
    spec, filters: list[dict[str, Any]], limit: int | None, today: "dt_date | None" = None
) -> dict[str, Any]:
    """The recipient-collapse plan: compile ONE predicate (same fail-closed rules as
    the row query, min-one-filter enforced) → exact matching-ROW count (pushdown) →
    stream [recipient_uei, recipient_name, $, date] up to COLLAPSE_SCAN_CAP rows,
    aggregating per recipient in-process → recipients sorted by Σ$ DESC (uei
    tiebreak), capped by ``limit``. Returns ``{rows, total_rows,
    distinct_recipients, returned, capped, scanned_rows, scan_capped, executed}`` —
    ``distinct_recipients`` is exact iff ``scan_capped`` is false."""
    served, result = _try_sidecar("execute_table_collapse", spec, filters, limit, today)
    if served:
        return result
    predicate, executed = compile_table_filters(spec, filters, today)
    cap = max(1, min(limit or MARKET_DEFAULT_LIMIT, MARKET_HARD_ROW_CAP))
    money_col, date_col = _COLLAPSE_COLUMNS[spec.source]
    uri = TABLE_GRAIN_URIS[spec.source]()
    total_rows = _count_rows(uri, predicate)
    agg: dict[str, dict[str, Any]] = {}
    scanned = 0
    if total_rows:
        raw = _stream_rows(uri, ["recipient_uei", "recipient_name", money_col, date_col],
                           predicate, COLLAPSE_SCAN_CAP)
        scanned = len(raw)
        for r in raw:
            uei = r.get("recipient_uei")
            if not uei:
                continue
            entry = agg.get(uei)
            if entry is None:
                entry = agg[uei] = {"uei": uei, "recipient_name": None,
                                    "match_ct": 0, "amt_total": 0.0,
                                    "last_action_date": None}
            if entry["recipient_name"] is None and r.get("recipient_name"):
                entry["recipient_name"] = r["recipient_name"]
            entry["match_ct"] += 1
            entry["amt_total"] += float(r.get(money_col) or 0.0)
            d = r.get(date_col)
            if d is not None and (entry["last_action_date"] is None
                                  or d > entry["last_action_date"]):
                entry["last_action_date"] = d
    recipients = sorted(agg.values(),
                        key=lambda e: (-e["amt_total"], e["uei"]))
    distinct = len(recipients)
    rows = [{**e, "amt_total": round(e["amt_total"], 2),
             "last_action_date": _map_jsonable(e["last_action_date"])}
            for e in recipients[:cap]]
    return {
        "rows": rows,
        "total_rows": total_rows,
        "distinct_recipients": distinct,
        "returned": len(rows),
        "capped": distinct > len(rows),
        "scanned_rows": scanned,
        "scan_capped": scanned >= COLLAPSE_SCAN_CAP and total_rows > scanned,
        "executed": {"grain": spec.grain, "collapse": "recipients",
                     "filters": executed, "limit": cap},
    }


# ── Code typeahead (GET /api/v1/market/codes) ─────────────────────────────────
# All three code systems load ONCE into memory on first request (lazy, thread-safe,
# per-process) and rank in-process. naics_reference / psc_reference are tiny reference
# dimensions (~2.1k / ~6.1k rows; PSC keeps only is_active rows with a name). AGENCY has
# no reference dimension yet — the pairs come from a streamed DISTINCT over
# usaspending_award_canonical (awarding_agency_code, awarding_agency_name): ~136 distinct
# pairs off 30.7M rows, a one-time ~20s first-request cost per process, then in-memory.
_CODES_LOCK = threading.Lock()
_codes_cache: dict[str, list[tuple[str, str]]] = {}

CODES_DEFAULT_LIMIT = 20
CODES_MAX_LIMIT = 100


def _stream_agency_pairs() -> "dict[tuple[str, str], int]":
    """Streamed DISTINCT (awarding_agency_code, awarding_agency_name) with row counts,
    from the canonical prime-award fact. Counts feed the one-name-per-code dedupe (a few
    codes carry historical name variants — the majority name wins)."""
    from collections import Counter
    scanner = _dataset(config.USASPENDING_AWARD_CANONICAL_URI).scanner(
        columns=["awarding_agency_code", "awarding_agency_name"])
    pairs: "Counter[tuple[str, str]]" = Counter()
    for batch in scanner.to_batches():
        pairs.update(zip(batch.column("awarding_agency_code").to_pylist(),
                         batch.column("awarding_agency_name").to_pylist()))
    return dict(pairs)


def _dedupe_agency_pairs(pair_counts: "dict[tuple[str, str], int]") -> list[tuple[str, str]]:
    """One (code, name) per agency code: NULL-guarded, majority name wins (probed live
    2026-07-05: 136 distinct pairs, 2 codes with historical name variants), ties break
    lexicographically for determinism. Pure — unit-tested without R2."""
    best: dict[str, tuple[int, str]] = {}          # code -> (-count, name); min = winner
    for (code, name), n in pair_counts.items():
        if not code or not name:
            continue
        key = (-n, name)
        if code not in best or key < best[code]:
            best[code] = key
    return sorted((code, name) for code, (_negn, name) in best.items())


def _load_codes(code_type: str) -> list[tuple[str, str]]:
    """(code, description) pairs for one code system, sorted by code. I/O seam —
    monkeypatched in tests; cached by code_search."""
    if code_type == "naics":
        rows = _scan_to_pylist(config.NAICS_REFERENCE_URI, ["naics_code", "naics_title"], None)
        pairs = [(r.get("naics_code"), r.get("naics_title")) for r in rows]
    elif code_type == "agency":
        return _dedupe_agency_pairs(_stream_agency_pairs())
    else:
        rows = _scan_to_pylist(config.PSC_REFERENCE_URI,
                               ["psc_code", "psc_name"], "is_active = true")
        pairs = [(r.get("psc_code"), r.get("psc_name")) for r in rows]
    return sorted((c, d) for c, d in pairs if c and d)


def _codes_for(code_type: str) -> list[tuple[str, str]]:
    with _CODES_LOCK:
        cached = _codes_cache.get(code_type)
        if cached is None:
            cached = _load_codes(code_type)
            _codes_cache[code_type] = cached
        return cached


def code_search(code_type: str, q: str, limit: int | None = None) -> list[dict[str, str]]:
    """Ranked typeahead over one code system (naics | psc | agency): CODE-PREFIX matches
    first (shortest code first — sectors before industries), then case-insensitive
    DESCRIPTION-substring matches (by code). Raises ``MapCompileError`` (→ 422) on a bad
    type or empty q. NOTE: lane predicates accept only naics|psc — agency serves the
    top_agency_code scalar field's typeahead, not a lane axis."""
    if code_type not in market_registry.CODE_SYSTEMS:
        raise MapCompileError(f"type must be one of {list(market_registry.CODE_SYSTEMS)}")
    q = (q or "").strip()
    if not q:
        raise MapCompileError("q (search text) is required")
    lim = max(1, min(limit or CODES_DEFAULT_LIMIT, CODES_MAX_LIMIT))
    q_up, q_lc = q.upper(), q.lower()
    prefix: list[tuple[str, str]] = []
    substr: list[tuple[str, str]] = []
    for code, desc in _codes_for(code_type):
        if code.upper().startswith(q_up):
            prefix.append((code, desc))
        elif q_lc in desc.lower():
            substr.append((code, desc))
    prefix.sort(key=lambda cd: (len(cd[0]), cd[0]))
    ranked = prefix + substr                     # substr already code-sorted from the cache
    return [{"code": c, "description": d} for c, d in ranked[:lim]]
