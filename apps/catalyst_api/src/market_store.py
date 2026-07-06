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

# A NAICS/PSC code is short alnum (NAICS 2-6 digits; PSC 1-4 alnum). Validated before
# interpolation — defense-in-depth alongside _sql_str quote-doubling.
_CODE_OK = re.compile(r"^[A-Za-z0-9]{1,10}$")

_SOURCE_URIS = {
    "rollup": lambda: config.GTM_ENTITY_BEHAVIOR_ROLLUP_URI,
    "entities": lambda: config.GTM_SAM_ENTITIES_URI,
    "lanes": lambda: config.GTM_ENTITY_CODE_LANES_URI,
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


def compile_market_filters(
    filters: list[dict[str, Any]], today: "dt_date | None" = None
) -> tuple[dict[str, str | None], list[dict[str, Any]], list[dict[str, Any]]]:
    """Validate + compile the request's filter list. Each entry is EITHER a scalar
    clause ``{field, op, value}`` (field from the registry; lane pseudo-fields desugar
    to lane predicates) OR a lane object ``{"lane": {...}}`` — anything else raises
    ``MapCompileError`` (→ 422). Returns ``(predicates_by_source, lanes, executed)``:
    one AND-combined predicate string (or None) per scalar source table, the normalized
    lane dicts, and the validated/normalized filter list echoed for ``meta.executed``."""
    today = today or dt_date.today()
    parts: dict[str, list[str]] = {src: [] for src in market_registry.SOURCES}
    lanes: list[dict[str, Any]] = []
    executed: list[dict[str, Any]] = []
    for clause in filters:
        if not isinstance(clause, dict):
            raise MapCompileError("each filter must be an object")
        has_lane = clause.get("lane") is not None
        has_field = clause.get("field") is not None
        if has_lane and has_field:
            raise MapCompileError("a filter is EITHER {field, op, value} OR {lane: {...}}, not both")
        if has_lane:
            lane = _validate_lane(clause["lane"])
            lanes.append(lane)
            executed.append({"lane": lane})
            continue
        if not has_field:
            raise MapCompileError("a filter needs a 'field' (or a 'lane' object)")
        field, op, value = clause.get("field"), clause.get("op"), clause.get("value")
        pseudo = market_registry.LANE_PSEUDO_FIELDS.get(field)
        if pseudo is not None:
            lane = _desugar_lane_pseudo(field, pseudo, op, value)
            lanes.append(lane)
            executed.append({"lane": lane})
            continue
        spec = market_registry.ENTITY_FIELDS.get(field)
        if spec is None:
            raise MapCompileError(f"field {field!r} not in the entity registry")
        parts[spec.source].append(_map_clause_sql(spec, op, value, today))
        executed.append({"field": field, "op": op, "value": value})
    predicates = {src: (" AND ".join(p) if p else None) for src, p in parts.items()}
    return predicates, lanes, executed


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
    """Join gtm_sam_entities + gtm_entity_behavior_rollup on the surviving UEIs. A UEI
    absent from a table hydrates that table's columns as NULLs (honest absence — e.g.
    a SAM-registered entity with no contract behavior has no rollup row). Values are
    JSON-shaped (date32 → ISO) and keyed in RESULT_ROW_ORDER."""
    if not ueis:
        return []
    ent = _rows_by_uei(_SOURCE_URIS["entities"](), ueis,
                       list(market_registry.RESULT_COLUMNS_ENTITIES))
    rol = _rows_by_uei(_SOURCE_URIS["rollup"](), ueis,
                       list(market_registry.RESULT_COLUMNS_ROLLUP))
    rows: list[dict[str, Any]] = []
    for u in ueis:
        merged = {"uei": u, **(ent.get(u) or {}), **(rol.get(u) or {})}
        rows.append({k: _map_jsonable(merged.get(k)) for k in market_registry.RESULT_ROW_ORDER})
    return rows


def execute_entity_query(
    filters: list[dict[str, Any]], limit: int | None, today: "dt_date | None" = None
) -> dict[str, Any]:
    """The entity-grain plan (module docstring). Returns
    ``{rows, total, returned, capped, executed}``; ``total`` is the EXACT match count
    (set-intersection size, or count_rows pushdown on the single-table fast paths).
    Raises ``MapCompileError`` (→ 422) on any off-registry filter."""
    predicates, lanes, executed = compile_market_filters(filters, today)
    cap = max(1, min(limit or MARKET_DEFAULT_LIMIT, MARKET_HARD_ROW_CAP))

    sets: list[set[str]] = []
    for lane in lanes:
        sets.append(_uei_set(_SOURCE_URIS["lanes"](), _compile_lane_predicate(lane)))
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
