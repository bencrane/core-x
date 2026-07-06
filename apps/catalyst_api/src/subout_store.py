"""Subout-opportunities recipe — the per-UEI "open prime awards likely to be subbed
out to companies like yours" query (POST /api/v1/market/subout-opportunities).

A READ-TIME NAMED RECIPE over the pre-weighting substrate (pre-weighting doctrine: the
datasets carry raw evidence — counts, sums, dates — and every weight/threshold lives
HERE, versioned under RECIPE_ID, never baked into a build). Every score is returned
WITH its components on the wire: (name, raw_value, weight, contribution) per component,
score = Σ contributions. Fail-closed like the market compiler: any unknown body key,
off-vocabulary lens, or malformed value raises ``MapCompileError`` (→ 422 at the route).

EXECUTION PLAN (every hop an indexed lookup — no full scans):
  1. probe_codes    — the target's codes BY LENS (how the code is known):
       awarded_prime_contracts_in_code   gtm_entity_code_lanes side='prime' (BTREE uei)
       delivered_subawards_under_code    gtm_entity_code_lanes side='sub' — the PRIME
                                         award's code on subawards the firm delivered
                                         under, never a claim of the firm's own work
       sam_registered_naics              gtm_sam_entities primary_naics + naics_codes
       inferred_primeable                gtm_entity_inferred_primeable_codes — both-sider
                                         cooccurrence evidence, NOT a demonstration
       caller_declared                   codes_override (prospect-declared probe codes)
  2. prime_subout_cube — gtm_prime_subout_by_recipient_code probed on recipient_code IN
     (target codes): the primes whose sub-out history hits firms with the target's code
     profile. Recipients' inferred codes are deliberately absent from the cube (past
     dollar flows are characterized by demonstration; inference rides the PROBE side).
  3. active_awards  — usaspending_award_canonical: recipient_uei IN (matched primes,
     chunked) AND (pop current end >= today OR ordering period end >= today). Date
     pushdown always rides the IN list — the 30.7M-row spine is never scanned bare.
  4. geo            — usaspending_award_pop_centroids per award (BTREE
     generated_unique_award_id) × gtm_entity_geo target HQ → haversine distance_mi.
  5. score          — COMPONENT_WEIGHTS applied per award, components explicit.
  6. peers          — top distinct_recipient-heavy matched cube cells → their codes →
     gtm_subaward_recipient_code_evidence (BTREE code): recipients sharing them.

A UEI with no code signals is a 200 with empty data + meta.reason — an empty market is
an answer, not an error. Per-stage wall times ride meta.timings_ms.
"""
from __future__ import annotations

import logging
import math
import time
from datetime import date as dt_date
from typing import Any

from . import config, market_registry
from .lance_store import MapCompileError, _dataset, _map_jsonable, _sql_str, valid_uei
from .market_store import _CODE_OK, _chunks, _in_predicate

log = logging.getLogger("catalyst_api.subout_store")

# ── The recipe (id + weights live together — bump the id on ANY weight change) ─
RECIPE_ID = "subout_opportunities.v1"
# Component weights, normalized (Σ = 1.0). Each component contributes
# weight × normalized_value (normalized_value ∈ [0, 1]); score = Σ contributions.
COMPONENT_WEIGHTS: dict[str, float] = {
    "prime_subout_history": 0.30,   # log-scaled Σ subaward_amt_total over matched cells
    "award_already_subbing": 0.15,  # the award ALREADY reports subaward activity
    "subcontracting_plan": 0.10,    # a subcontracting plan attached to the award
    "lens_strength": 0.15,          # how strongly the target carries the matched codes
    "proximity": 0.10,              # HQ → place-of-performance distance decay
    "expiring_window": 0.20,        # nearer PoP end = nearer recompete/backfill window
}

# ── Lens vocabulary (self-describing: each name states HOW the code is known) ──
LENS_AWARDED_PRIME = "awarded_prime_contracts_in_code"
LENS_DELIVERED_SUB = "delivered_subawards_under_code"
LENS_SAM_NAICS = "sam_registered_naics"
LENS_INFERRED_PRIMEABLE = "inferred_primeable"
LENS_CALLER_DECLARED = "caller_declared"        # codes_override only — never selectable
SELECTABLE_LENSES = (LENS_AWARDED_PRIME, LENS_DELIVERED_SUB,
                     LENS_SAM_NAICS, LENS_INFERRED_PRIMEABLE)

CODE_TYPES = ("naics", "psc")
ALLOWED_BODY_KEYS = frozenset(
    {"uei", "lenses", "codes_override", "code_type", "limit", "include_peers"})

DEFAULT_LIMIT = 50
LIMIT_CAP = 200
# Bounded fan-out: at most this many matched primes probe the award spine (ranked by
# matched sub-out $ so the cut keeps the strongest history), and at most this many
# active awards are scored before the limit cut.
PRIME_PROBE_CAP = 500
AWARD_SCAN_CAP = 2_000
# Peers: how many top matched codes probe the evidence table, how many evidence rows
# stream before the cut, and the peer cap itself.
PEER_CODE_PROBE = 3
PEER_EVIDENCE_ROW_SCAN = 2_000
PEER_CAP = 10

# ── Normalization constants (deterministic — pinned by tests) ──────────────────
LOG_DOLLAR_CEILING_EXP = 9.0        # log10 $ scale: $1B+ normalizes to 1.0
INFERRED_SUPPORT_CEILING = 20       # supporting_bothsider_firm_ct at which norm = 1.0
DECLARED_LENS_STRENGTH = 0.5        # sam_registered / caller_declared: a claim, not $
PROXIMITY_NEUTRAL = 0.5             # geo_precision != 'zip5' or distance unknown
PROXIMITY_ZERO_MI = 500.0           # linear decay: 0 mi → 1.0, ≥500 mi → 0.0
EXPIRING_HORIZON_DAYS = 1_080       # ~3y: ends today → 1.0, ≥horizon → 0.0
PLAN_REQUIRED_CODES = frozenset({"C", "D", "E", "F", "G", "H"})  # FAR 19.7 plan attached


# ── I/O seams (monkeypatch targets for the hermetic tests) ─────────────────────
def _scan_to_pylist(uri: str, columns: list[str], predicate: str | None) -> list[dict[str, Any]]:
    """One fresh scanner (one-shot by contract) → rows. Every caller passes an indexed
    point/IN predicate — never an unfiltered projection."""
    return _dataset(uri).scanner(columns=columns, filter=predicate).to_table().to_pylist()


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


# ── Request validation (fail-closed — MapCompileError → 422 at the route) ─────
def validate_request(body: Any) -> dict[str, Any]:
    """Body dict → normalized request. Unknown keys, off-vocabulary lenses, malformed
    codes, and mistyped knobs all refuse to compile — nothing off-contract reaches a
    Lance filter."""
    if not isinstance(body, dict):
        raise MapCompileError("request body must be an object")
    unknown = set(body) - ALLOWED_BODY_KEYS
    if unknown:
        raise MapCompileError(f"unknown body key(s) {sorted(unknown)!r}")

    uei = body.get("uei")
    if not isinstance(uei, str) or not valid_uei(uei.strip()):
        raise MapCompileError("uei is required and must be a 12-char alphanumeric SAM UEI")
    uei = uei.strip()

    lenses = body.get("lenses")
    if lenses is None:
        lenses = list(SELECTABLE_LENSES)
    else:
        if not isinstance(lenses, list) or not lenses:
            raise MapCompileError(
                f"lenses must be a non-empty array of {list(SELECTABLE_LENSES)} (omit for all)")
        for lens in lenses:
            if lens not in SELECTABLE_LENSES:
                raise MapCompileError(
                    f"unknown lens {lens!r} — one of {list(SELECTABLE_LENSES)}")
        lenses = list(dict.fromkeys(lenses))            # dedupe, order-preserving

    codes_override = body.get("codes_override")
    if codes_override is None:
        codes_override = []
    else:
        if not isinstance(codes_override, list):
            raise MapCompileError("codes_override must be an array of code strings")
        for c in codes_override:
            if not isinstance(c, str) or not _CODE_OK.match(c):
                raise MapCompileError(f"codes_override code {c!r} is not a valid NAICS/PSC code")
        codes_override = list(dict.fromkeys(codes_override))

    code_type = body.get("code_type")
    if code_type is not None and code_type not in CODE_TYPES:
        raise MapCompileError(f"code_type must be one of {list(CODE_TYPES)} (or omitted)")

    limit = body.get("limit")
    if limit is None:
        limit = DEFAULT_LIMIT
    elif isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
        raise MapCompileError("limit must be a positive whole number")
    limit = min(limit, LIMIT_CAP)

    include_peers = body.get("include_peers")
    if include_peers is None:
        include_peers = True
    elif not isinstance(include_peers, bool):
        raise MapCompileError("include_peers must be a boolean")

    return {"uei": uei, "lenses": lenses, "codes_override": codes_override,
            "code_type": code_type, "limit": limit, "include_peers": include_peers}


# ── Normalizations (pure — component-math determinism is pinned by tests) ─────
def _log_dollar_norm(amount: Any) -> float:
    """USD → [0, 1] on a log10 scale: $0 → 0.0, $1B+ → 1.0."""
    amt = float(amount or 0.0)
    if amt <= 0:
        return 0.0
    return min(1.0, math.log10(1.0 + amt) / LOG_DOLLAR_CEILING_EXP)


def _lens_strength(lens: str, evidence: dict[str, Any]) -> float:
    """One lens entry → [0, 1] strength. Demonstrated $ log-scales; inferred support
    counts normalize against INFERRED_SUPPORT_CEILING; a bare claim (SAM registration /
    caller-declared) is DECLARED_LENS_STRENGTH — a claim, never dollars."""
    if lens in (LENS_AWARDED_PRIME, LENS_DELIVERED_SUB):
        return _log_dollar_norm(evidence.get("obl_lifetime"))
    if lens == LENS_INFERRED_PRIMEABLE:
        ct = evidence.get("supporting_bothsider_firm_ct") or 0
        return min(1.0, float(ct) / INFERRED_SUPPORT_CEILING)
    return DECLARED_LENS_STRENGTH


def _plan_norm(subcontracting_plan_code: Any) -> float:
    """Subcontracting-plan code → [0, 1]. Plan-attached codes (FAR 19.7 C/D/E/F/G/H)
    → 1.0; 'B' (below thresholds) → 0.25; 'A' (no subcontracting possibilities) and
    NULL → 0.0."""
    code = (subcontracting_plan_code or "").strip().upper()
    if code in PLAN_REQUIRED_CODES:
        return 1.0
    if code == "B":
        return 0.25
    return 0.0


def _proximity_norm(distance_mi: float | None, geo_precision: Any) -> float:
    """Distance decay, honest about precision: only a 'zip5' place-of-performance
    centroid with a known distance participates (linear: 0 mi → 1.0, ≥500 mi → 0.0);
    anything coarser or unknown is NEUTRAL (0.5), never a fake signal."""
    if distance_mi is None or geo_precision != "zip5":
        return PROXIMITY_NEUTRAL
    return max(0.0, 1.0 - distance_mi / PROXIMITY_ZERO_MI)


def _expiring_norm(days_to_end: int | None) -> float:
    """Days until the nearest open end date → [0, 1]: ends today → 1.0, decaying
    linearly to 0.0 at EXPIRING_HORIZON_DAYS. No open end date → 0.0."""
    if days_to_end is None or days_to_end < 0:
        return 0.0
    return max(0.0, 1.0 - days_to_end / EXPIRING_HORIZON_DAYS)


def _haversine_mi(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in statute miles (R = 3958.8 mi)."""
    rlat1, rlon1, rlat2, rlon2 = map(math.radians, (lat1, lon1, lat2, lon2))
    dlat, dlon = rlat2 - rlat1, rlon2 - rlon1
    a = math.sin(dlat / 2) ** 2 + math.cos(rlat1) * math.cos(rlat2) * math.sin(dlon / 2) ** 2
    return 3958.8 * 2.0 * math.asin(min(1.0, math.sqrt(a)))


# ── Stage 1: the target's probe codes, by lens ─────────────────────────────────
def _probe_codes(uei: str, lenses: list[str], code_type: str | None,
                 codes_override: list[str]) -> list[dict[str, Any]]:
    """The target's code signals as lens entries
    ``{lens, code_type, code, evidence, strength}`` — deduped on (lens, code_type,
    code). Every scan is a BTREE uei point-lookup."""
    entries: dict[tuple[str, str | None, str], dict[str, Any]] = {}

    def add(lens: str, ct: str | None, code: Any, evidence: dict[str, Any]) -> None:
        code = (code or "").strip() if isinstance(code, str) else code
        if not code or not isinstance(code, str) or not _CODE_OK.match(code):
            return
        if code_type is not None and ct is not None and ct != code_type:
            return
        key = (lens, ct, code)
        entry = {"lens": lens, "code_type": ct, "code": code, "evidence": evidence,
                 "strength": _lens_strength(lens, evidence)}
        prior = entries.get(key)
        if prior is None or entry["strength"] > prior["strength"]:
            entries[key] = entry

    uei_pred = f"uei = {_sql_str(uei)}"
    if LENS_AWARDED_PRIME in lenses or LENS_DELIVERED_SUB in lenses:
        pred = uei_pred
        if code_type is not None:
            pred += f" AND code_type = {_sql_str(code_type)}"
        for row in _scan_to_pylist(config.GTM_ENTITY_CODE_LANES_URI,
                                   ["uei", "side", "code_type", "code", "obl_lifetime"], pred):
            lens = LENS_AWARDED_PRIME if row.get("side") == "prime" else LENS_DELIVERED_SUB
            if lens in lenses:
                add(lens, row.get("code_type"), row.get("code"),
                    {"obl_lifetime": row.get("obl_lifetime")})

    if LENS_SAM_NAICS in lenses and code_type in (None, "naics"):
        for row in _scan_to_pylist(config.GTM_SAM_ENTITIES_URI,
                                   ["uei", "primary_naics", "naics_codes"], uei_pred):
            add(LENS_SAM_NAICS, "naics", row.get("primary_naics"),
                {"registration": "primary_naics"})
            for c in (row.get("naics_codes") or []):
                add(LENS_SAM_NAICS, "naics", c, {"registration": "naics_codes"})

    if LENS_INFERRED_PRIMEABLE in lenses:
        pred = uei_pred
        if code_type is not None:
            pred += f" AND code_type = {_sql_str(code_type)}"
        for row in _scan_to_pylist(config.GTM_INFERRED_PRIMEABLE_URI,
                                   ["uei", "code_type", "code",
                                    "supporting_bothsider_firm_ct"], pred):
            add(LENS_INFERRED_PRIMEABLE, row.get("code_type"), row.get("code"),
                {"supporting_bothsider_firm_ct": row.get("supporting_bothsider_firm_ct")})

    for c in codes_override:
        # caller-declared probe codes carry the request's code_type restriction when
        # set, else no type claim (the cube probe matches on recipient_code alone).
        add(LENS_CALLER_DECLARED, code_type, c, {"declared_by_caller": True})

    return list(entries.values())


# ── Stage 2: primes that sub out into the target's codes (the cube probe) ─────
def _match_primes(lens_entries: list[dict[str, Any]], code_type: str | None,
                  notes: list[str]) -> dict[str, list[dict[str, Any]]]:
    """gtm_prime_subout_by_recipient_code probed on recipient_code IN (probe codes) →
    matched cells grouped by prime, capped at PRIME_PROBE_CAP primes ranked by matched
    sub-out $. Every recipient_code_source present in the cube is demonstrated/declared
    by construction (recipients' inferred codes are deliberately absent), so no source
    restriction applies. Defensive: an unreachable cube (still building) degrades to
    zero matches with a meta note, never a 500."""
    codes = sorted({e["code"] for e in lens_entries})
    if not codes:
        return {}
    columns = ["prime_awardee_uei", "context_code_type", "context_code",
               "recipient_code_source", "recipient_code_type", "recipient_code",
               "subaward_edge_ct", "subaward_amt_total", "distinct_recipient_ct",
               "last_subaward_action_date"]
    cells: list[dict[str, Any]] = []
    try:
        for chunk in _chunks(codes):
            pred = _in_predicate("recipient_code", chunk)
            if code_type is not None:
                pred += f" AND recipient_code_type = {_sql_str(code_type)}"
            cells.extend(_scan_to_pylist(
                config.GTM_PRIME_SUBOUT_BY_RECIPIENT_CODE_URI, columns, pred))
    except MapCompileError:
        raise
    except Exception as exc:  # noqa: BLE001 — the cube may not be materialized yet
        log.warning("subout cube unreachable (%s): serving zero prime matches", exc)
        notes.append("gtm_prime_subout_by_recipient_code unreachable — no prime matches served")
        return {}

    by_prime: dict[str, list[dict[str, Any]]] = {}
    for cell in cells:
        prime = cell.get("prime_awardee_uei")
        if prime:
            by_prime.setdefault(prime, []).append(cell)
    if len(by_prime) > PRIME_PROBE_CAP:
        ranked = sorted(
            by_prime,
            key=lambda p: (-sum(float(c.get("subaward_amt_total") or 0.0)
                                for c in by_prime[p]), p))
        by_prime = {p: by_prime[p] for p in ranked[:PRIME_PROBE_CAP]}
    return by_prime


# ── Stage 3: those primes' ACTIVE awards ───────────────────────────────────────
AWARD_COLUMNS = [
    "generated_unique_award_id", "award_id_piid", "recipient_uei", "naics_code",
    "product_or_service_code", "total_obligation", "base_and_all_options_value",
    "subaward_count", "total_subaward_amount", "subcontracting_plan_code",
    "period_of_performance_current_end_date", "ordering_period_end_date",
    "awarding_agency_code", "awarding_agency_name",
]


def _active_awards(prime_ueis: list[str], today: dt_date) -> list[dict[str, Any]]:
    """Chunked ``recipient_uei IN (...)`` + open-end-date pushdown over the 30.7M-row
    award spine (BTREE recipient_uei; the date predicate always rides the IN list —
    never a bare scan). Bounded at AWARD_SCAN_CAP rows."""
    date_lit = f"DATE '{today.isoformat()}'"
    active_pred = (f"(period_of_performance_current_end_date >= {date_lit}"
                   f" OR ordering_period_end_date >= {date_lit})")
    out: list[dict[str, Any]] = []
    for chunk in _chunks(sorted(prime_ueis)):
        remaining = AWARD_SCAN_CAP - len(out)
        if remaining <= 0:
            break
        pred = f"{_in_predicate('recipient_uei', chunk)} AND {active_pred}"
        out.extend(_stream_rows(config.USASPENDING_AWARD_CANONICAL_URI,
                                AWARD_COLUMNS, pred, remaining))
    return out


# ── Stage 4: geo (award PoP centroid × target HQ → distance_mi) ────────────────
def _award_geo(award_ids: list[str]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for chunk in _chunks(sorted(award_ids)):
        for row in _scan_to_pylist(
                config.USASPENDING_AWARD_POP_CENTROIDS_URI,
                ["generated_unique_award_id", "latitude", "longitude", "geo_precision"],
                _in_predicate("generated_unique_award_id", chunk)):
            key = row.get("generated_unique_award_id")
            if key:
                out[key] = row
    return out


def _target_hq(uei: str) -> tuple[float, float] | None:
    rows = _scan_to_pylist(config.GTM_ENTITY_GEO_URI,
                           ["uei", "latitude", "longitude"], f"uei = {_sql_str(uei)}")
    for row in rows:
        lat, lon = row.get("latitude"), row.get("longitude")
        if lat is not None and lon is not None:
            return float(lat), float(lon)
    return None


# ── Stage 5: score (components EXPLICIT on the wire) ───────────────────────────
def _matched_evidence(cells: list[dict[str, Any]],
                      entries_by_code: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    """The (lens, code) hits connecting the target to one prime, each carrying BOTH
    sides of the evidence: how the TARGET knows the code (lens evidence) and how the
    PRIME's sub-out history hits it (the cube cell measures). Deduped on (lens, code),
    keeping the largest-$ cell."""
    best: dict[tuple[str, str], dict[str, Any]] = {}
    for cell in cells:
        code = cell.get("recipient_code")
        for entry in entries_by_code.get(code, []):
            if (entry["code_type"] is not None
                    and cell.get("recipient_code_type") is not None
                    and entry["code_type"] != cell.get("recipient_code_type")):
                continue
            key = (entry["lens"], code)
            matched = {
                "lens": entry["lens"],
                "code": code,
                "evidence": {
                    **entry["evidence"],
                    "recipient_code_source": cell.get("recipient_code_source"),
                    "context_code_type": cell.get("context_code_type"),
                    "context_code": cell.get("context_code"),
                    "subaward_edge_ct": cell.get("subaward_edge_ct"),
                    "subaward_amt_total": cell.get("subaward_amt_total"),
                    "distinct_recipient_ct": cell.get("distinct_recipient_ct"),
                    "last_subaward_action_date": _map_jsonable(
                        cell.get("last_subaward_action_date")),
                },
            }
            prior = best.get(key)
            if (prior is None or float(matched["evidence"].get("subaward_amt_total") or 0.0)
                    > float(prior["evidence"].get("subaward_amt_total") or 0.0)):
                best[key] = matched
    return sorted(best.values(), key=lambda m: (m["lens"], m["code"]))


def _components(award: dict[str, Any], cells: list[dict[str, Any]],
                matched_strength: float, distance_mi: float | None,
                pop_geo_precision: Any, today: dt_date) -> list[dict[str, Any]]:
    """The six scored components for one award: (name, raw_value, weight, contribution)
    each — contribution = weight × normalized raw signal. Deterministic and pure."""
    subout_total = sum(float(c.get("subaward_amt_total") or 0.0) for c in cells)

    sub_ct = award.get("subaward_count") or 0
    sub_amt = float(award.get("total_subaward_amount") or 0.0)
    already_subbing = bool(sub_ct > 0 or sub_amt > 0)

    plan_code = award.get("subcontracting_plan_code")

    ends = [d for d in (award.get("period_of_performance_current_end_date"),
                        award.get("ordering_period_end_date"))
            if isinstance(d, dt_date) and d >= today]
    days_to_end = min((d - today).days for d in ends) if ends else None

    norms = {
        "prime_subout_history": (subout_total, _log_dollar_norm(subout_total)),
        "award_already_subbing": (already_subbing, 1.0 if already_subbing else 0.0),
        "subcontracting_plan": (plan_code, _plan_norm(plan_code)),
        "lens_strength": (round(matched_strength, 6), matched_strength),
        "proximity": (distance_mi, _proximity_norm(distance_mi, pop_geo_precision)),
        "expiring_window": (days_to_end, _expiring_norm(days_to_end)),
    }
    return [
        {"name": name, "raw_value": raw, "weight": weight,
         "contribution": round(weight * norms[name][1], 6)}
        for name, weight in COMPONENT_WEIGHTS.items()
        for raw in (norms[name][0],)
    ]


# ── Stage 6: peers ─────────────────────────────────────────────────────────────
def _peers(cells_by_prime: dict[str, list[dict[str, Any]]], target_uei: str,
           notes: list[str]) -> list[dict[str, Any]]:
    """Recipients sharing the target's top matched codes — the "companies like yours"
    the primes already sub to. Top codes = the matched cube cells heaviest in
    distinct_recipient_ct; peer UEIs stream off gtm_subaward_recipient_code_evidence
    (BTREE code), deduped, target excluded, capped at PEER_CAP. Defensive like the
    cube probe."""
    all_cells = [c for cells in cells_by_prime.values() for c in cells]
    if not all_cells:
        return []
    ranked = sorted(all_cells,
                    key=lambda c: (-(c.get("distinct_recipient_ct") or 0),
                                   c.get("recipient_code") or ""))
    top_codes: list[str] = []
    for cell in ranked:
        code = cell.get("recipient_code")
        if code and code not in top_codes:
            top_codes.append(code)
        if len(top_codes) >= PEER_CODE_PROBE:
            break
    if not top_codes:
        return []
    try:
        rows = _stream_rows(config.GTM_SUBAWARD_RECIPIENT_CODE_EVIDENCE_URI,
                            ["subawardee_uei", "code"],
                            _in_predicate("code", top_codes), PEER_EVIDENCE_ROW_SCAN)
    except Exception as exc:  # noqa: BLE001 — evidence table may not be materialized yet
        log.warning("peer evidence table unreachable (%s): serving zero peers", exc)
        notes.append("gtm_subaward_recipient_code_evidence unreachable — no peers served")
        return []
    peers: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        peer = row.get("subawardee_uei")
        if not peer or peer == target_uei or peer in seen:
            continue
        seen.add(peer)
        peers.append({"uei": peer, "shared_code": row.get("code")})
        if len(peers) >= PEER_CAP:
            break
    return peers


# ── The recipe executor ────────────────────────────────────────────────────────
def execute_subout_opportunities(body: Any, today: "dt_date | None" = None) -> dict[str, Any]:
    """The full plan (module docstring). Returns the wire envelope
    ``{meta: {recipeId, componentWeights, timings_ms, total, ...}, data:
    {opportunities, peers}}``. Raises ``MapCompileError`` (→ 422) on any off-contract
    body; a UEI with no code signals is a 200 with empty data + meta.reason."""
    req = validate_request(body)
    today = today or dt_date.today()
    timings: dict[str, float] = {}
    notes: list[str] = []
    t0 = time.monotonic()

    def _mark(stage: str, since: float) -> float:
        now = time.monotonic()
        timings[stage] = round((now - since) * 1000.0, 1)
        return now

    def _meta(total: int, reason: str | None = None) -> dict[str, Any]:
        timings["total"] = round((time.monotonic() - t0) * 1000.0, 1)
        meta: dict[str, Any] = {
            "recipeId": RECIPE_ID,
            "registryVersion": market_registry.REGISTRY_VERSION,
            "componentWeights": dict(COMPONENT_WEIGHTS),
            "uei": req["uei"],
            "lenses": req["lenses"],
            "timings_ms": timings,
            "total": total,
        }
        if reason is not None:
            meta["reason"] = reason
        if notes:
            meta["notes"] = notes
        return meta

    # 1. probe codes by lens
    t = time.monotonic()
    lens_entries = _probe_codes(req["uei"], req["lenses"], req["code_type"],
                                req["codes_override"])
    t = _mark("probe_codes", t)
    if not lens_entries:
        return {"meta": _meta(0, reason="uei has no code signals"),
                "data": {"opportunities": [], "peers": []}}

    entries_by_code: dict[str, list[dict[str, Any]]] = {}
    for e in lens_entries:
        entries_by_code.setdefault(e["code"], []).append(e)

    # 2. primes whose sub-out history hits those codes
    cells_by_prime = _match_primes(lens_entries, req["code_type"], notes)
    t = _mark("prime_subout_cube", t)

    # 3. their ACTIVE awards
    awards = _active_awards(list(cells_by_prime), today) if cells_by_prime else []
    t = _mark("active_awards", t)

    # 4. geo: award PoP centroid × target HQ
    geo = _award_geo([a["generated_unique_award_id"] for a in awards
                      if a.get("generated_unique_award_id")]) if awards else {}
    hq = _target_hq(req["uei"]) if awards else None
    t = _mark("geo", t)

    # 5. score — every component explicit on the wire
    opportunities: list[dict[str, Any]] = []
    for award in awards:
        prime = award.get("recipient_uei")
        cells = cells_by_prime.get(prime, [])
        matched = _matched_evidence(cells, entries_by_code)
        matched_codes = {c.get("recipient_code") for c in cells}
        matched_strength = max(
            (e["strength"] for code in matched_codes for e in entries_by_code.get(code, [])),
            default=0.0)
        g = geo.get(award.get("generated_unique_award_id")) or {}
        distance_mi = None
        if hq is not None and g.get("latitude") is not None and g.get("longitude") is not None:
            distance_mi = round(_haversine_mi(hq[0], hq[1],
                                              float(g["latitude"]), float(g["longitude"])), 1)
        components = _components(award, cells, matched_strength, distance_mi,
                                 g.get("geo_precision"), today)
        score = round(sum(c["contribution"] for c in components), 6)
        opportunities.append({
            "generated_unique_award_id": award.get("generated_unique_award_id"),
            "award_id_piid": award.get("award_id_piid"),
            "prime_awardee_uei": prime,
            "naics_code": award.get("naics_code"),
            "product_or_service_code": award.get("product_or_service_code"),
            "awarding_agency_code": award.get("awarding_agency_code"),
            "awarding_agency_name": award.get("awarding_agency_name"),
            "total_obligation": award.get("total_obligation"),
            "base_and_all_options_value": award.get("base_and_all_options_value"),
            "subaward_count": award.get("subaward_count"),
            "total_subaward_amount": award.get("total_subaward_amount"),
            "subcontracting_plan_code": award.get("subcontracting_plan_code"),
            "period_of_performance_current_end_date": _map_jsonable(
                award.get("period_of_performance_current_end_date")),
            "ordering_period_end_date": _map_jsonable(award.get("ordering_period_end_date")),
            "pop_geo_precision": g.get("geo_precision"),
            "distance_mi": distance_mi,
            "matched": matched,
            "score": score,
            "components": components,
        })
    opportunities.sort(key=lambda o: (-o["score"], o["generated_unique_award_id"] or ""))
    total = len(opportunities)
    opportunities = opportunities[:req["limit"]]
    t = _mark("score", t)

    # 6. peers
    peers: list[dict[str, Any]] = []
    if req["include_peers"]:
        peers = _peers(cells_by_prime, req["uei"], notes)
        _mark("peers", t)

    return {"meta": _meta(total), "data": {"opportunities": opportunities, "peers": peers}}
