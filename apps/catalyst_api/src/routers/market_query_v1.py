"""market-query — the predicate-grammar router (the general composition engine).

One endpoint takes a LIST of typed predicates — each an ontology term plus its
dials — compiles each into a leg reading its own mart, and intersects the legs
on `uei` in ONE deterministic sidecar statement. Any combination of any number
of legs, no new code per combination. This is the blank-slate composer ruled
2026-07-17 (GC_PLATFORM_TEST_LOG entries 2/4/6/7): no leg is mandatory, every
former "core truth" (active award, FY23–25 window, curated pair scope) is an
optional predicate, and collections become presets that prefill predicates.

Naming law (operator ruling, entry 10): government nouns untouched + transparent
aggregation qualifiers. No invented metaphors ("book", "seats", "won") in term keys.

Vocabulary (30 terms; family → terms):
  money        total_obligations · total_current_value_of_active_awards ·
               remaining_current_value_of_active_awards · median_active_award_value ·
               total_potential_value_of_open_idvs · remaining_potential_value_of_open_idvs
  work         awarded_under_codes · principal_naics_or_psc ·
               obligations_under_naics_psc_pairs · inferred_capable_of_codes
  lifecycle    active_awards · awards_expiring · open_idvs
  buyers       awarded_by_agency · distinct_agency_breadth
  geography    registered_in_state · place_of_performance_of_active_awards · performed_in (states + FY lookback) · place_of_performance_within (zip|lat/lon radius)
  designations small_business_designations · set_aside_obligations · employee_size ·
               sam_registration_active
  pricing      active_award_pricing_mix
  relationships contracting_role · subcontractor_to · issues_subawards
  momentum     recent_award_actions · sam_profile_changes
  risk         ucc_financing · union_collective_bargaining
  firmographics year_founded

Excluded BY STANDING RULING: person-level contactability is never a customer-facing
predicate (2026-07-16). Available internally only.

Endpoints (service-token gated):
  GET  /api/v1/market-query/vocabulary          → the machine-readable term list
  POST /api/v1/market-query/count   {"predicates": [...]} → {"count", "spec",
        "disclosures", "elapsed_ms", "artifact"}
  POST /api/v1/market-query/entities {"predicates": [...], "limit"?: ≤1000}
        → sample members {"entities": [{uei, legal_business_name}], ...}

Compile rules (the forbidden-shape doctrine, substrate survey §2):
- every leg reads a uei-keyed mart with its filter on that mart's own columns;
  the only large-table legs (obligations_under_naics_psc_pairs over the 108M
  txn mart, uei-sorted) are the measured-acceptable shapes (~0.8s whole-universe);
- inferred_capable_of_codes REQUIRES exact codes (the 263M/160M marts are
  code-sorted; an uncoded scan is refused, never attempted);
- affirmative legs INTERSECT on uei; negated legs (value:false) EXCEPT from the
  running set; a spec with no affirmative leg runs against the full SAM
  registrant universe (gtm_sam_entities) — blank slate is honest;
- all SQL fragments are server-side; every user token is validated against a
  closed vocabulary or a typed range and never interpolated as free text.

Windows: fiscal-year dials accept ANY range FY1962–FY2026 (FY26 = year-to-date;
the mart carries it). Nothing is frozen to FY23–25 (entry 1/3 ruling).
"""
from __future__ import annotations

import logging
import math
import os
import re
from typing import Any, Callable

import httpx
from fastapi import APIRouter, Depends, HTTPException

from ..service_token import require_service_token

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/market-query", tags=["market-query"])

SIDECAR_URL = os.environ.get("QUERY_SIDECAR_URL", "https://query-sidecar-api.onrender.com")

_US_STATES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL",
    "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT",
    "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI",
    "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", "DC", "PR",
    "GU", "VI", "AS", "MP",
}

_FY_MIN, _FY_MAX = 1962, 2026  # verified live: min(fy), max(fy) in gtm_entity_fy_won

_TRAILING_WINDOWS = ("12mo", "24mo", "36mo", "60mo", "lifetime")

# (side, window) → (table, column) for the trailing clock. prime rides the
# behavior rollup (only home of 36mo); sub/total ride the audience spine.
_TRAILING_COL: dict[tuple[str, str], tuple[str, str]] = {
    ("prime", "12mo"): ("gtm_entity_behavior_rollup", "prime_obl_12mo"),
    ("prime", "24mo"): ("gtm_entity_behavior_rollup", "prime_obl_24mo"),
    ("prime", "36mo"): ("gtm_entity_behavior_rollup", "prime_obl_36mo"),
    ("prime", "60mo"): ("gtm_entity_behavior_rollup", "prime_obl_60mo"),
    ("prime", "lifetime"): ("gtm_entity_behavior_rollup", "prime_obl_lifetime"),
    ("sub", "12mo"): ("gtm_audience_entities", "sub_amt_12mo"),
    ("sub", "24mo"): ("gtm_audience_entities", "sub_amt_24mo"),
    ("sub", "60mo"): ("gtm_audience_entities", "sub_amt_60mo"),
    ("sub", "lifetime"): ("gtm_audience_entities", "sub_amt_lifetime"),
    ("total", "12mo"): ("gtm_audience_entities", "total_amt_12mo"),
    ("total", "24mo"): ("gtm_audience_entities", "total_amt_24mo"),
    ("total", "60mo"): ("gtm_audience_entities", "total_amt_60mo"),
    ("total", "lifetime"): ("gtm_audience_entities", "total_amt_lifetime"),
}

# gtm_entity_award_book columns behind the active-award value terms. The mart
# already excludes vehicles from committed_* (standing blend rule).
_BOOK_TERM_COL = {
    "total_current_value_of_active_awards": "committed_value",
    "remaining_current_value_of_active_awards": "committed_runway",
    "median_active_award_value": "committed_award_median",
    "total_potential_value_of_open_idvs": "vehicle_ceiling",
    "remaining_potential_value_of_open_idvs": "vehicle_headroom",
}

_DESIGNATION_COLS = {
    "dsbs_8a": "dsbs_8a",
    "dsbs_hubzone": "dsbs_hubzone",
    "dsbs_wosb": "dsbs_wosb",
    "dsbs_edwosb": "dsbs_edwosb",
    "dsbs_sdvosb": "dsbs_sdvosb",
    "dsbs_vosb": "dsbs_vosb",
    "fsrs_sdvosb": "fsrs_sdvosb",
    "fsrs_vosb": "fsrs_vosb",
    "fsrs_wosb": "fsrs_wosb",
    "fsrs_edwosb": "fsrs_edwosb",
    "fsrs_woman_owned": "fsrs_woman_owned",
    "fsrs_hubzone": "fsrs_hubzone",
    "fsrs_8a": "fsrs_8a",
    "fsrs_sdb": "fsrs_sdb",
    "fsrs_minority": "fsrs_minority",
    "fsrs_any_designation": "fsrs_any_designation",
    "in_dsbs": "in_dsbs",
}

_EMPLOYEE_BANDS = {
    "1-10", "11-50", "51-200", "201-500", "501-1000",
    "1001-5000", "5001-10000", "10001+",
}

_SET_ASIDE_COL = {
    "any": "won_obl_set_aside",
    "8a": "won_obl_8a",
    "sdvosb": "won_obl_sdvosb",
    "wosb": "won_obl_wosb",
    "hubzone": "won_obl_hubzone",
}

# Verified live: DISTINCT field over sam_master_profile_deltas.
_PROFILE_FIELDS = {
    "registration_status", "psc_removed", "naics_added", "psc_added",
    "naics_removed", "legal_business_name", "bus_type_added", "primary_naics",
    "naics_sb_flag_changed", "bus_type_removed", "purpose_of_registration",
    "entity_structure", "cage_code",
}

# recent_award_actions family → server-side condition, served by the
# (action_type_code, month)-sorted copy txn_recipient_month_by_type so the
# class+window predicate PRUNES (routing fix, gap report 2026-07-17 entry 2:
# the uei-sorted base table full-scanned at 3.1s). Literal code lists (zone-map
# prunable, unlike IN-subqueries) transcribed from action_type_vocab flags —
# vocab remains the semantic authority; parity asserted in tests.
# new_award is FPDS's NULL action type (the base award row).
_ACTION_FAMILY_COND = {
    "new_award": "action_type_code IS NULL",
    "more_work": "action_type_code IN ('A','B','D','G','L')",
    "funding_released": "action_type_code IN ('C','G')",
    "termination": "action_type_code IN ('E','F','N','X')",
}

_UEI_RE = re.compile(r"^[A-Z0-9]{12}$")
_AGENCY_RE = re.compile(r"^[A-Z0-9]{2,4}$")
_CODE_RE = re.compile(r"^[A-Z0-9]{1,6}$")

_MAX_PREDICATES = 20
_MAX_PAIRS = 13000

# Disclosure sentences, attached per-term when the leg's coverage is partial.
_DISCLOSURES = {
    "place_of_performance_of_active_awards":
        "Place-of-performance is filtered over the open-award universe; awards without "
        "a resolvable PoP state do not satisfy the filter.",
    "employee_size":
        "Employee size is known for a minority of registrants (PDL-bridged); entities "
        "with unknown size are excluded when bands are specified.",
    "ucc_financing":
        "UCC filings cover CA and CO secretary-of-state corpora only.",
    "union_collective_bargaining":
        "Collective-bargaining coverage is OLMS-filed agreements name-matched to SAM "
        "registrants; partial by construction.",
    "year_founded":
        "Year founded is known only for PDL-bridged registrants (~40% coverage).",
    "inferred_capable_of_codes":
        "Inferred capability is a model over demonstrated-adjacent behavior, not "
        "demonstrated performance.",
}


def _refuse(detail: str) -> HTTPException:
    return HTTPException(status_code=422, detail=detail)


def _req_num(spec: dict, key: str, term: str, *, allow_zero: bool = True) -> float | None:
    v = spec.get(key)
    if v is None:
        return None
    if (not isinstance(v, (int, float)) or isinstance(v, bool) or not math.isfinite(v)
            or v < 0 or (v == 0 and not allow_zero)):
        raise _refuse(f"{term}.{key} must be a non-negative number")
    return float(v)


def _req_int(spec: dict, key: str, term: str, lo: int, hi: int, default: int | None = None) -> int | None:
    v = spec.get(key, default)
    if v is None:
        return None
    if not isinstance(v, int) or isinstance(v, bool) or not (lo <= v <= hi):
        raise _refuse(f"{term}.{key} must be an integer in [{lo}, {hi}]")
    return v


def _min_max_where(col: str, mn: float | None, mx: float | None, term: str) -> str:
    if mn is None and mx is None:
        raise _refuse(f"{term} needs min and/or max")
    preds = []
    if mn is not None:
        preds.append(f"COALESCE({col}, 0) >= {mn}")
    if mx is not None:
        preds.append(f"COALESCE({col}, 0) <= {mx}")
    return " AND ".join(preds)


def _parse_states(spec: dict, term: str) -> list[str]:
    raw = spec.get("states")
    if not isinstance(raw, list) or not raw:
        raise _refuse(f"{term}.states must be a non-empty list of state codes")
    out = []
    for s in raw:
        code = str(s).strip().upper()
        if code not in _US_STATES:
            raise _refuse(f"unknown state '{s}' in {term}.states")
        out.append(code)
    return sorted(set(out))


def _parse_codes(spec: dict, term: str, code_type: str, *, exact_only: bool = False,
                 max_codes: int = 200) -> tuple[list[str], list[str]]:
    """→ (exact codes, prefixes). NAICS exact = 6 chars, PSC exact = 4 chars."""
    raw = spec.get("codes")
    if not isinstance(raw, list) or not raw or len(raw) > max_codes:
        raise _refuse(f"{term}.codes must be a non-empty list (≤{max_codes})")
    exact_len = 6 if code_type == "naics" else 4
    exact, prefixes = [], []
    for c in raw:
        code = str(c).strip().upper()
        if not _CODE_RE.match(code) or len(code) > exact_len:
            raise _refuse(f"invalid {code_type} code '{c}' in {term}.codes")
        if len(code) == exact_len:
            exact.append(code)
        elif exact_only:
            raise _refuse(f"{term}.codes accepts exact {code_type} codes only ('{c}' is a prefix)")
        else:
            prefixes.append(code)
    return sorted(set(exact)), sorted(set(prefixes))


def _code_match_pred(exact: list[str], prefixes: list[str]) -> str:
    parts = []
    if exact:
        parts.append("code IN (" + ",".join(f"'{c}'" for c in exact) + ")")
    for p in prefixes:
        parts.append(f"code LIKE '{p}%'")
    return "(" + " OR ".join(parts) + ")"


def _fy_range(spec: dict, term: str) -> tuple[int, int]:
    a = _req_int(spec, "fy_start", term, _FY_MIN, _FY_MAX)
    b = _req_int(spec, "fy_end", term, _FY_MIN, _FY_MAX, default=_FY_MAX)
    if a is None:
        raise _refuse(f"{term}.fy_start is required (FY{_FY_MIN}–FY{_FY_MAX})")
    if b is None or b < a:
        raise _refuse(f"{term}.fy_end must be ≥ fy_start")
    return a, b


# ── per-term compilers: spec → (kind, leg_sql, echo). kind ∈ affirmative|negative ──

def _c_total_obligations(spec: dict) -> tuple[str, str, dict]:
    term = "total_obligations"
    side = str(spec.get("side", "total"))
    if side not in ("prime", "sub", "total"):
        raise _refuse(f"{term}.side must be prime | sub | total")
    clock = spec.get("clock") or {"basis": "trailing", "window": "24mo"}
    if not isinstance(clock, dict):
        raise _refuse(f"{term}.clock must be an object")
    basis = clock.get("basis", "trailing")
    mn = _req_num(spec, "min", term)
    mx = _req_num(spec, "max", term)
    if basis == "trailing":
        window = str(clock.get("window", "24mo"))
        key = (side, window)
        if window not in _TRAILING_WINDOWS or key not in _TRAILING_COL:
            raise _refuse(
                f"{term}: trailing window '{window}' unavailable for side '{side}' "
                f"(prime: 12/24/36/60mo/lifetime · sub/total: 12/24/60mo/lifetime)"
            )
        table, col = _TRAILING_COL[key]
        where = _min_max_where(col, mn, mx, term)
        sql = f"SELECT uei FROM {table} WHERE {where}"
        echo = {"term": term, "side": side, "clock": {"basis": "trailing", "window": window},
                "min": mn, "max": mx}
        return "affirmative", sql, echo
    if basis == "fiscal_years":
        if side != "prime":
            raise _refuse(f"{term}: the fiscal-year clock serves prime obligations only "
                          "(gtm_entity_fy_won is a prime-obligation mart)")
        a, b = _fy_range(clock, term)
        having = _min_max_where("sum(won_obl)", mn, mx, term)
        sql = (f"SELECT uei FROM gtm_entity_fy_won WHERE fy BETWEEN {a} AND {b} "
               f"GROUP BY uei HAVING {having}")
        echo = {"term": term, "side": side, "clock": {"basis": "fiscal_years", "fy_start": a, "fy_end": b},
                "min": mn, "max": mx}
        return "affirmative", sql, echo
    raise _refuse(f"{term}.clock.basis must be trailing | fiscal_years")


def _c_book_value(term: str) -> Callable[[dict], tuple[str, str, dict]]:
    col = _BOOK_TERM_COL[term]

    def compile_(spec: dict) -> tuple[str, str, dict]:
        mn = _req_num(spec, "min", term)
        mx = _req_num(spec, "max", term)
        where = _min_max_where(col, mn, mx, term)
        sql = f"SELECT uei FROM gtm_entity_award_book WHERE {where}"
        return "affirmative", sql, {"term": term, "min": mn, "max": mx}

    return compile_


def _c_awarded_under_codes(spec: dict) -> tuple[str, str, dict]:
    term = "awarded_under_codes"
    side = str(spec.get("side", "prime"))
    if side not in ("prime", "sub"):
        raise _refuse(f"{term}.side must be prime | sub")
    code_type = str(spec.get("code_type", ""))
    if code_type not in ("naics", "psc"):
        raise _refuse(f"{term}.code_type must be naics | psc")
    window = str(spec.get("window", "24mo"))
    if window not in ("12mo", "24mo", "60mo", "lifetime"):
        raise _refuse(f"{term}.window must be 12mo | 24mo | 60mo | lifetime")
    exact, prefixes = _parse_codes(spec, term, code_type)
    mn = _req_num(spec, "min_dollars", term)
    dollars_pred = f"obl_{window} >= {mn}" if mn is not None else f"obl_{window} > 0"
    sql = (f"SELECT DISTINCT uei FROM gtm_entity_code_lanes "
           f"WHERE side = '{side}' AND code_type = '{code_type}' "
           f"AND {_code_match_pred(exact, prefixes)} AND {dollars_pred}")
    echo = {"term": term, "side": side, "code_type": code_type,
            "codes": exact + [p + "*" for p in prefixes], "window": window, "min_dollars": mn}
    return "affirmative", sql, echo


def _c_principal_code(spec: dict) -> tuple[str, str, dict]:
    term = "principal_naics_or_psc"
    code_type = str(spec.get("code_type", ""))
    if code_type not in ("naics", "psc"):
        raise _refuse(f"{term}.code_type must be naics | psc")
    basis = str(spec.get("basis", "share_24mo"))
    if basis not in ("share_24mo", "share_lifetime", "rank_24mo", "rank_lifetime"):
        raise _refuse(f"{term}.basis must be share_24mo | share_lifetime | rank_24mo | rank_lifetime")
    exact, prefixes = _parse_codes(spec, term, code_type)
    if basis.startswith("share"):
        share = spec.get("min_share")
        if not isinstance(share, (int, float)) or isinstance(share, bool) or not (0 < float(share) <= 1):
            raise _refuse(f"{term}.min_share must be a number in (0, 1]")
        metric_pred = f"{basis} >= {float(share)}"
        echo_metric = {"min_share": float(share)}
    else:
        rank = _req_int(spec, "max_rank", term, 1, 50)
        if rank is None:
            raise _refuse(f"{term}.max_rank is required for rank basis")
        metric_pred = f"{basis} <= {rank}"
        echo_metric = {"max_rank": rank}
    sql = (f"SELECT DISTINCT uei FROM gtm_prime_code_signature "
           f"WHERE code_type = '{code_type}' AND {_code_match_pred(exact, prefixes)} "
           f"AND {metric_pred}")
    echo = {"term": term, "code_type": code_type,
            "codes": exact + [p + "*" for p in prefixes], "basis": basis, **echo_metric}
    return "affirmative", sql, echo


def _c_pairs(spec: dict) -> tuple[str, str, dict]:
    term = "obligations_under_naics_psc_pairs"
    raw = spec.get("pairs")
    if not isinstance(raw, list) or not raw or len(raw) > _MAX_PAIRS:
        raise _refuse(f"{term}.pairs must be a non-empty list of [naics6, psc4] (≤{_MAX_PAIRS})")
    pairs = set()
    for item in raw:
        if not isinstance(item, (list, tuple)) or len(item) != 2:
            raise _refuse(f"{term}.pairs entries must be [naics6, psc4]")
        n, p = str(item[0]).strip().upper(), str(item[1]).strip().upper()
        if not (_CODE_RE.match(n) and len(n) == 6 and _CODE_RE.match(p) and len(p) == 4):
            raise _refuse(f"invalid pair ['{item[0]}', '{item[1]}'] in {term}.pairs")
        pairs.add((n, p))
    a, b = _fy_range(spec, term)
    start = f"{a - 1}-10-01"
    end_expr = "current_date" if b >= _FY_MAX else f"DATE '{b}-09-30'"
    mn = _req_num(spec, "min", term)
    mx = _req_num(spec, "max", term)
    having = _min_max_where("sum(obligation)", mn, mx, term)
    values = ",".join(f"('{n}','{p}')" for n, p in sorted(pairs))
    sql = (f"SELECT t.uei FROM gtm_txn_events_slim t "
           f"JOIN (VALUES {values}) pr(naics_code, psc_code) "
           f"ON t.naics_code = pr.naics_code AND t.psc_code = pr.psc_code "
           f"WHERE t.action_date BETWEEN DATE '{start}' AND {end_expr} "
           f"GROUP BY t.uei HAVING {having}")
    echo = {"term": term, "pair_count": len(pairs), "fy_start": a, "fy_end": b,
            "min": mn, "max": mx}
    return "affirmative", sql, echo


def _c_inferred(spec: dict) -> tuple[str, str, dict]:
    term = "inferred_capable_of_codes"
    side = str(spec.get("side", "prime"))
    if side not in ("prime", "sub"):
        raise _refuse(f"{term}.side must be prime | sub")
    table = ("gtm_entity_inferred_primeable_codes" if side == "prime"
             else "gtm_entity_inferred_subbable_codes")
    code_type = str(spec.get("code_type", ""))
    if code_type not in ("naics", "psc"):
        raise _refuse(f"{term}.code_type must be naics | psc")
    exact, _ = _parse_codes(spec, term, code_type, exact_only=True, max_codes=50)
    in_list = ",".join(f"'{c}'" for c in exact)
    sql = (f"SELECT DISTINCT uei FROM {table} "
           f"WHERE code_type = '{code_type}' AND code IN ({in_list})")
    echo = {"term": term, "side": side, "code_type": code_type, "codes": exact}
    return "affirmative", sql, echo


# instrument dial → gtm_entity_pricing_mix rider columns (standalone split,
# test-log entry 9: definitive contract D vs purchase order B; active + lifetime).
_INSTRUMENT_COL = {
    ("definitive_contract", "active"): "active_definitive_ct",
    ("definitive_contract", "lifetime"): "lifetime_definitive_ct",
    ("purchase_order", "active"): "active_purchase_order_ct",
    ("purchase_order", "lifetime"): "lifetime_purchase_order_ct",
}


def _c_active_awards(spec: dict) -> tuple[str, str, dict]:
    term = "active_awards"
    value = spec.get("value")
    mn = _req_int(spec, "min_count", term, 1, 100000)
    mx = _req_int(spec, "max_count", term, 0, 100000)
    instrument = spec.get("instrument")
    scope = str(spec.get("instrument_scope", "active"))
    if instrument is not None:
        col = _INSTRUMENT_COL.get((str(instrument), scope))
        if col is None:
            raise _refuse(f"{term}.instrument must be definitive_contract | purchase_order "
                          "(instrument_scope: active | lifetime)")
        if value is False:
            if mn is not None or mx is not None:
                raise _refuse(f"{term}: value:false cannot combine with count dials")
            sql = f"SELECT uei FROM gtm_entity_pricing_mix WHERE {col} >= 1"
            return "negative", sql, {"term": term, "value": False,
                                     "instrument": instrument, "instrument_scope": scope}
        preds = [f"{col} >= {mn if mn is not None else 1}"]
        if mx is not None:
            preds.append(f"{col} <= {mx}")
        sql = "SELECT uei FROM gtm_entity_pricing_mix WHERE " + " AND ".join(preds)
        return "affirmative", sql, {"term": term, "instrument": instrument,
                                    "instrument_scope": scope,
                                    "min_count": mn if mn is not None else 1, "max_count": mx}
    if value is False:
        if mn is not None or mx is not None:
            raise _refuse(f"{term}: value:false cannot combine with count dials")
        sql = "SELECT uei FROM gtm_entity_behavior_rollup WHERE active_award_ct >= 1"
        return "negative", sql, {"term": term, "value": False}
    preds = [f"active_award_ct >= {mn if mn is not None else 1}"]
    if mx is not None:
        preds.append(f"active_award_ct <= {mx}")
    sql = "SELECT uei FROM gtm_entity_behavior_rollup WHERE " + " AND ".join(preds)
    echo = {"term": term, "min_count": mn if mn is not None else 1, "max_count": mx}
    return "affirmative", sql, echo


def _c_awards_expiring(spec: dict) -> tuple[str, str, dict]:
    term = "awards_expiring"
    months = _req_int(spec, "within_months", term, 1, 36)
    if months is None:
        raise _refuse(f"{term}.within_months is required (1–36)")
    mn_ct = _req_int(spec, "min_count", term, 1, 100000, default=1)
    mn_obl = _req_num(spec, "min_obligated", term)
    having = [f"sum(n_awards) >= {mn_ct}"]
    if mn_obl is not None:
        having.append(f"sum(obligated) >= {mn_obl}")
    sql = (f"SELECT uei FROM gtm_award_expiry_months "
           f"WHERE end_month >= date_trunc('month', current_date) "
           f"AND end_month <= date_trunc('month', current_date) + INTERVAL '{months} months' "
           f"GROUP BY uei HAVING " + " AND ".join(having))
    echo = {"term": term, "within_months": months, "min_count": mn_ct, "min_obligated": mn_obl}
    return "affirmative", sql, echo


def _c_active_award_value(spec: dict) -> tuple[str, str, dict]:
    """Deal-size fit: the firm holds ≥1 OPEN award whose life-to-date
    obligations fall in [min, max]. Open-award universe (gtm_open_awards),
    same as the geography terms; obligated dollars — the measure every
    surface already displays — never ceiling/options value."""
    term = "active_award_value"
    mn = _req_num(spec, "min", term)
    mx = _req_num(spec, "max", term)
    where = _min_max_where("total_obligation", mn, mx, term)
    sql = f"SELECT DISTINCT recipient_uei AS uei FROM gtm_open_awards WHERE {where}"
    return "affirmative", sql, {"term": term, "min": mn, "max": mx}


def active_award_value_award_keys(spec: dict) -> tuple[str, dict]:
    """AWARD-GRAIN projection of active_award_value: the open-award KEYS in
    the dollar range — the award property applied to the paper itself."""
    term = "active_award_value"
    mn = _req_num(spec, "min", term)
    mx = _req_num(spec, "max", term)
    where = _min_max_where("total_obligation", mn, mx, term)
    sql = f"SELECT generated_unique_award_id FROM gtm_open_awards WHERE {where}"
    return sql, {"term": term, "min": mn, "max": mx, "grain": "award"}


def _c_avg_annual_obligations(spec: dict) -> tuple[str, str, dict]:
    """Throughput fit: the firm's AVERAGE prime obligations per year over the
    trailing five years (prime_obl_60mo / 5 — years with nothing count as
    zero, so the one-hit wonder reads as run-rate, not peak). Whole-firm by
    construction (disclosed): the size of the operation, not the slice."""
    term = "avg_annual_obligations"
    mn = _req_num(spec, "min", term)
    mx = _req_num(spec, "max", term)
    where = _min_max_where("prime_obl_60mo / 5.0", mn, mx, term)
    sql = f"SELECT uei FROM gtm_entity_behavior_rollup WHERE {where}"
    return "affirmative", sql, {"term": term, "min": mn, "max": mx}


def _c_open_idvs(spec: dict) -> tuple[str, str, dict]:
    term = "open_idvs"
    value = spec.get("value")
    mn = _req_int(spec, "min_count", term, 1, 10000)
    if value is False:
        if mn is not None:
            raise _refuse(f"{term}: value:false cannot combine with min_count")
        sql = "SELECT uei FROM gtm_entity_award_book WHERE vehicle_ct >= 1"
        return "negative", sql, {"term": term, "value": False}
    sql = f"SELECT uei FROM gtm_entity_award_book WHERE vehicle_ct >= {mn if mn is not None else 1}"
    return "affirmative", sql, {"term": term, "min_count": mn if mn is not None else 1}


def _c_awarded_by_agency(spec: dict) -> tuple[str, str, dict]:
    term = "awarded_by_agency"
    raw = spec.get("agencies")
    if not isinstance(raw, list) or not raw or len(raw) > 50:
        raise _refuse(f"{term}.agencies must be a non-empty list of agency codes (≤50)")
    agencies = []
    for a in raw:
        code = str(a).strip().upper()
        if not _AGENCY_RE.match(code):
            raise _refuse(f"invalid agency code '{a}' in {term}.agencies")
        agencies.append(code)
    agencies = sorted(set(agencies))
    scope = str(spec.get("scope", "lifetime"))
    if scope not in ("active", "lifetime"):
        raise _refuse(f"{term}.scope must be active | lifetime")
    col = "obligated_active" if scope == "active" else "obligated_lifetime"
    mn = _req_num(spec, "min_dollars", term)
    having = f"sum({col}) >= {mn}" if mn is not None else f"sum({col}) > 0"
    in_list = ",".join(f"'{a}'" for a in agencies)
    sql = (f"SELECT uei FROM gtm_award_recipient_rollup "
           f"WHERE awarding_agency_code IN ({in_list}) "
           f"GROUP BY uei HAVING {having}")
    echo = {"term": term, "agencies": agencies, "scope": scope, "min_dollars": mn}
    return "affirmative", sql, echo


def _c_agency_breadth(spec: dict) -> tuple[str, str, dict]:
    term = "distinct_agency_breadth"
    scope = str(spec.get("scope", "active"))
    if scope not in ("active", "lifetime"):
        raise _refuse(f"{term}.scope must be active | lifetime")
    mn = _req_int(spec, "min", term, 1, 500)
    mx = _req_int(spec, "max", term, 1, 500)
    if mn is None and mx is None:
        raise _refuse(f"{term} needs min and/or max")
    if scope == "active":
        table, col = "gtm_entity_award_book", "active_agency_ct"
    else:
        table, col = "gtm_entity_behavior_rollup", "distinct_agency_ct"
    preds = []
    if mn is not None:
        preds.append(f"{col} >= {mn}")
    if mx is not None:
        preds.append(f"{col} <= {mx}")
    sql = f"SELECT uei FROM {table} WHERE " + " AND ".join(preds)
    return "affirmative", sql, {"term": term, "scope": scope, "min": mn, "max": mx}


def _c_registered_in_state(spec: dict) -> tuple[str, str, dict]:
    term = "registered_in_state"
    states = _parse_states(spec, term)
    in_list = ",".join(f"'{s}'" for s in states)
    sql = f"SELECT uei FROM gtm_sam_entities WHERE physical_state IN ({in_list})"
    return "affirmative", sql, {"term": term, "states": states}


def _c_pop_active(spec: dict) -> tuple[str, str, dict]:
    term = "place_of_performance_of_active_awards"
    states = _parse_states(spec, term)
    in_list = ",".join(f"'{s}'" for s in states)
    sql = (f"SELECT DISTINCT recipient_uei AS uei FROM gtm_open_awards "
           f"WHERE primary_place_of_performance_state_code IN ({in_list})")
    return "affirmative", sql, {"term": term, "states": states}


def pop_states_award_keys(spec: dict) -> tuple[str, dict]:
    """AWARD-GRAIN projection of place_of_performance_of_active_awards: the
    open-award KEYS whose own PoP state matches — the paper itself, not the
    holders (the Awards-drawer semantics, operator-ruled 2026-07-22). Same
    validation, same universe (gtm_open_awards) as the holder compiler."""
    term = "place_of_performance_of_active_awards"
    states = _parse_states(spec, term)
    in_list = ",".join(f"'{s}'" for s in states)
    sql = (f"SELECT generated_unique_award_id FROM gtm_open_awards "
           f"WHERE primary_place_of_performance_state_code IN ({in_list})")
    return sql, {"term": term, "states": states, "grain": "award"}


def _c_performed_in(spec: dict) -> tuple[str, str, dict]:
    """Place-of-performance by state with an optional FY lookback — the txn-grain
    complement to the active-grain term above. "Performed work in TX during FY25"
    reads txn_events_combo_by_geo (sorted pop_state — the state predicate prunes)."""
    term = "performed_in"
    states = _parse_states(spec, term)
    in_list = ",".join(f"'{s}'" for s in states)
    clock = spec.get("clock")
    if clock is not None:
        if not isinstance(clock, dict) or clock.get("basis") != "fiscal_years":
            raise _refuse(f"{term}.clock must be {{basis: fiscal_years, fy_start, fy_end}}")
        a, b = _fy_range(clock, term)
        sql = (f"SELECT DISTINCT uei FROM txn_events_combo_by_geo "
               f"WHERE pop_state IN ({in_list}) AND fy BETWEEN {a} AND {b}")
        return "affirmative", sql, {"term": term, "states": states,
                                    "clock": {"basis": "fiscal_years", "fy_start": a, "fy_end": b}}
    sql = f"SELECT DISTINCT uei FROM txn_events_combo_by_geo WHERE pop_state IN ({in_list})"
    return "affirmative", sql, {"term": term, "states": states}


_ZIP_RE = re.compile(r"^\d{5}$")


def _pop_within_parts(spec: dict) -> tuple[str, float, dict]:
    """Shared validation + anchor resolution for the radius cut — used by the
    holder compiler below and the award-grain projection. Returns
    (anchor_cte, miles, echo)."""
    term = "place_of_performance_within"
    miles = spec.get("miles", 150)
    if not isinstance(miles, (int, float)) or isinstance(miles, bool) or not (1 <= float(miles) <= 500):
        raise _refuse(f"{term}.miles must be a number in [1, 500]")
    miles = float(miles)
    zip5 = spec.get("zip")
    lat, lon = spec.get("lat"), spec.get("lon")
    if zip5 is not None:
        zip5 = str(zip5)
        if not _ZIP_RE.match(zip5):
            raise _refuse(f"{term}.zip must be a 5-digit zip code")
        anchor_cte = (f"SELECT avg(latitude) AS alat, avg(longitude) AS alon "
                      f"FROM usaspending_award_pop_centroids "
                      f"WHERE zip5 = '{zip5}' AND latitude IS NOT NULL")
        echo_anchor: dict = {"zip": zip5}
    elif isinstance(lat, (int, float)) and isinstance(lon, (int, float)) \
            and not isinstance(lat, bool) and not isinstance(lon, bool) \
            and -90 <= float(lat) <= 90 and -180 <= float(lon) <= 180:
        anchor_cte = f"SELECT {float(lat)} AS alat, {float(lon)} AS alon"
        echo_anchor = {"lat": float(lat), "lon": float(lon)}
    else:
        raise _refuse(f"{term} needs a zip (5-digit) or a lat/lon anchor")
    label = spec.get("label")
    if label is not None:
        label = str(label)[:80]
        echo_anchor["label"] = label
    return anchor_cte, miles, {"term": term, "miles": miles, **echo_anchor}


# 3958.8 = earth radius in MILES; haversine over the active-award centroids.
_HAVERSINE = (
    "2 * 3958.8 * asin(sqrt("
    "pow(sin(radians(o.latitude - c.alat) / 2), 2) + "
    "cos(radians(c.alat)) * cos(radians(o.latitude)) * "
    "pow(sin(radians(o.longitude - c.alon) / 2), 2)))"
)


def _c_pop_within(spec: dict) -> tuple[str, str, dict]:
    """Radius cut: firms with an ACTIVE award performed within N miles of an
    anchor (a zip5 or an explicit lat/lon, e.g. a military installation).
    Anchor resolves in-query (zip -> avg of that zip's award-PoP centroids);
    the haversine runs over gtm_open_awards (163k, centroid pre-joined) — a
    full pass is ms-class, no pruning needed. Miles is a query-time dial."""
    anchor_cte, miles, echo = _pop_within_parts(spec)
    sql = (
        f"SELECT DISTINCT o.recipient_uei AS uei "
        f"FROM gtm_open_awards o CROSS JOIN ({anchor_cte}) c "
        f"WHERE o.latitude IS NOT NULL AND c.alat IS NOT NULL "
        f"AND {_HAVERSINE} <= {miles}"
    )
    return "affirmative", sql, echo


def pop_within_award_keys(spec: dict) -> tuple[str, dict]:
    """AWARD-GRAIN projection of place_of_performance_within: the open-award
    KEYS whose own PoP centroid sits inside the ring — the paper itself, not
    the holders (the Awards-drawer semantics, operator-ruled 2026-07-22)."""
    anchor_cte, miles, echo = _pop_within_parts(spec)
    sql = (
        f"SELECT o.generated_unique_award_id "
        f"FROM gtm_open_awards o CROSS JOIN ({anchor_cte}) c "
        f"WHERE o.latitude IS NOT NULL AND c.alat IS NOT NULL "
        f"AND {_HAVERSINE} <= {miles}"
    )
    return sql, {**echo, "grain": "award"}


def _c_designations(spec: dict) -> tuple[str, str, dict]:
    term = "small_business_designations"
    raw = spec.get("designations")
    if not isinstance(raw, list) or not raw:
        raise _refuse(f"{term}.designations must be a non-empty list")
    match = str(spec.get("match", "all"))
    if match not in ("all", "any"):
        raise _refuse(f"{term}.match must be all | any")
    cols = []
    for token in raw:
        col = _DESIGNATION_COLS.get(str(token))
        if col is None:
            raise _refuse(f"unknown designation '{token}' in {term}.designations")
        cols.append(col)
    joiner = " AND " if match == "all" else " OR "
    pred = "(" + joiner.join(f"{c} = TRUE" for c in cols) + ")"
    sql = f"SELECT uei FROM gtm_audience_entities WHERE {pred}"
    echo = {"term": term, "designations": [str(t) for t in raw], "match": match}
    return "affirmative", sql, echo


def _c_set_aside(spec: dict) -> tuple[str, str, dict]:
    term = "set_aside_obligations"
    fam = str(spec.get("set_aside", "any"))
    col = _SET_ASIDE_COL.get(fam)
    if col is None:
        raise _refuse(f"{term}.set_aside must be any | 8a | sdvosb | wosb | hubzone")
    a, b = _fy_range(spec, term)
    mn = _req_num(spec, "min", term)
    having = f"sum({col}) >= {mn}" if mn is not None else f"sum({col}) > 0"
    sql = (f"SELECT uei FROM gtm_entity_fy_won WHERE fy BETWEEN {a} AND {b} "
           f"GROUP BY uei HAVING {having}")
    echo = {"term": term, "set_aside": fam, "fy_start": a, "fy_end": b, "min": mn}
    return "affirmative", sql, echo


def _c_employee_size(spec: dict) -> tuple[str, str, dict]:
    term = "employee_size"
    raw = spec.get("bands")
    if not isinstance(raw, list) or not raw:
        raise _refuse(f"{term}.bands must be a non-empty list")
    for b in raw:
        if str(b) not in _EMPLOYEE_BANDS:
            raise _refuse(f"unknown employee band '{b}' in {term}.bands")
    bands = sorted(set(str(b) for b in raw))
    in_list = ",".join(f"'{b}'" for b in bands)
    sql = f"SELECT uei FROM gtm_audience_entities WHERE employee_size_band IN ({in_list})"
    return "affirmative", sql, {"term": term, "bands": bands}


def _c_sam_active(spec: dict) -> tuple[str, str, dict]:
    term = "sam_registration_active"
    value = spec.get("value", True)
    if not isinstance(value, bool):
        raise _refuse(f"{term}.value must be true or false")
    sql = f"SELECT uei FROM gtm_sam_entities WHERE sam_is_active = {'TRUE' if value else 'FALSE'}"
    return "affirmative", sql, {"term": term, "value": value}


_PRICING_CLASS_COL = {"fixed": "fixed", "cost": "cost", "tm_lh": "tm_lh", "other": "other"}
_FINANCING_CLASS_COL = {
    "unfinanced": "unfin",
    "progress_payments": "prog",
    "performance_based": "perf",
    "commercial": "comm",
    "other": "othfin",
}


def _c_pricing_mix(spec: dict) -> tuple[str, str, dict]:
    term = "active_award_pricing_mix"
    fixed = spec.get("min_fixed_share")
    ffp = spec.get("min_ffp_unfinanced_share")
    financed = spec.get("min_financed_share")
    mn_obl = _req_num(spec, "min_active_obligations", term)
    preds = []
    for name, v, col in (("min_fixed_share", fixed, "active_fixed_share"),
                         ("min_ffp_unfinanced_share", ffp, "active_ffp_unfinanced_share"),
                         ("min_financed_share", financed, "active_financed_share")):
        if v is not None:
            if not isinstance(v, (int, float)) or isinstance(v, bool) or not (0 < float(v) <= 1):
                raise _refuse(f"{term}.{name} must be a number in (0, 1]")
            preds.append(f"COALESCE({col}, 0) >= {float(v)}")
    if mn_obl is not None:
        preds.append(f"COALESCE(active_obl, 0) >= {mn_obl}")
    # pricing×financing combo cells (2026-07-17 operator directive: first-class).
    # combos: [{pricing: fixed|cost|tm_lh|other|any, financing: <class>,
    #           min_dollars? , min_share?}] — entries AND together.
    combos = spec.get("combos")
    combos_echo = None
    if combos is not None:
        if not isinstance(combos, list) or not combos or len(combos) > 8:
            raise _refuse(f"{term}.combos must be a non-empty list (≤8)")
        combos_echo = []
        for i, cb in enumerate(combos):
            if not isinstance(cb, dict):
                raise _refuse(f"{term}.combos[{i}] must be an object")
            pc = str(cb.get("pricing", "any"))
            fc = str(cb.get("financing", ""))
            fcol = _FINANCING_CLASS_COL.get(fc)
            if fcol is None:
                raise _refuse(f"{term}.combos[{i}].financing must be one of "
                              f"{' | '.join(sorted(_FINANCING_CLASS_COL))}")
            if pc == "any":
                col = f"active_obl_fin_{fcol}"
            elif pc in _PRICING_CLASS_COL:
                col = f"active_obl_{pc}_{fcol}"
            else:
                raise _refuse(f"{term}.combos[{i}].pricing must be fixed | cost | tm_lh | other | any")
            mn_d = _req_num(cb, "min_dollars", f"{term}.combos[{i}]")
            mn_s = cb.get("min_share")
            if mn_s is not None:
                if not isinstance(mn_s, (int, float)) or isinstance(mn_s, bool) or not (0 < float(mn_s) <= 1):
                    raise _refuse(f"{term}.combos[{i}].min_share must be a number in (0, 1]")
                preds.append(f"COALESCE({col}, 0) / NULLIF(COALESCE(active_obl, 0), 0) >= {float(mn_s)}")
            if mn_d is not None:
                preds.append(f"COALESCE({col}, 0) >= {mn_d}")
            if mn_d is None and mn_s is None:
                preds.append(f"COALESCE({col}, 0) > 0")
            combos_echo.append({"pricing": pc, "financing": fc, "min_dollars": mn_d, "min_share": mn_s})
    if not preds:
        raise _refuse(f"{term} needs at least one of min_fixed_share | min_ffp_unfinanced_share | "
                      "min_financed_share | min_active_obligations | combos")
    sql = "SELECT uei FROM gtm_entity_pricing_mix WHERE " + " AND ".join(preds)
    echo = {"term": term, "min_fixed_share": fixed, "min_ffp_unfinanced_share": ffp,
            "min_financed_share": financed, "min_active_obligations": mn_obl, "combos": combos_echo}
    return "affirmative", sql, echo


def _c_contracting_role(spec: dict) -> tuple[str, str, dict]:
    term = "contracting_role"
    role = str(spec.get("role", ""))
    col = {"prime": "is_prime_24mo", "subcontractor": "is_sub_60mo", "both": "prime_and_sub"}.get(role)
    if col is None:
        raise _refuse(f"{term}.role must be prime | subcontractor | both "
                      "(mart windows: prime = trailing 24mo, subcontractor = trailing 60mo)")
    sql = f"SELECT uei FROM gtm_entity_behavior_rollup WHERE {col} = TRUE"
    return "affirmative", sql, {"term": term, "role": role}


def _c_subcontractor_to(spec: dict) -> tuple[str, str, dict]:
    term = "subcontractor_to"
    raw = spec.get("prime_ueis")
    if not isinstance(raw, list) or not raw or len(raw) > 500:
        raise _refuse(f"{term}.prime_ueis must be a non-empty list of UEIs (≤500)")
    ueis = []
    for u in raw:
        uei = str(u).strip().upper()
        if not _UEI_RE.match(uei):
            raise _refuse(f"invalid UEI '{u}' in {term}.prime_ueis")
        ueis.append(uei)
    ueis = sorted(set(ueis))
    window = str(spec.get("window", "5y"))
    if window not in ("5y", "lifetime"):
        raise _refuse(f"{term}.window must be 5y | lifetime")
    col = "edge_dollars_5y" if window == "5y" else "edge_dollars_lifetime"
    mn = _req_num(spec, "min_dollars", term)
    pred = f"{col} >= {mn}" if mn is not None else f"{col} > 0"
    in_list = ",".join(f"'{u}'" for u in ueis)
    sql = (f"SELECT DISTINCT sub_uei AS uei FROM gtm_prime_sub_pairs "
           f"WHERE prime_uei IN ({in_list}) AND {pred}")
    echo = {"term": term, "prime_ueis": ueis, "window": window, "min_dollars": mn}
    return "affirmative", sql, echo


def _c_issues_subawards(spec: dict) -> tuple[str, str, dict]:
    term = "issues_subawards"
    window = str(spec.get("window", "5y"))
    if window not in ("5y", "lifetime"):
        raise _refuse(f"{term}.window must be 5y | lifetime")
    dcol = "edge_dollars_5y" if window == "5y" else "edge_dollars_lifetime"
    ccol = "edge_count_5y" if window == "5y" else "edge_count_lifetime"
    mn = _req_num(spec, "min_dollars", term)
    mn_ct = _req_int(spec, "min_sub_count", term, 1, 100000)
    having = [f"sum({dcol}) >= {mn}" if mn is not None else f"sum({dcol}) > 0"]
    if mn_ct is not None:
        having.append(f"sum({ccol}) >= {mn_ct}")
    sql = (f"SELECT prime_uei AS uei FROM gtm_prime_sub_pairs "
           f"GROUP BY prime_uei HAVING " + " AND ".join(having))
    echo = {"term": term, "window": window, "min_dollars": mn, "min_sub_count": mn_ct}
    return "affirmative", sql, echo


def _c_recent_actions(spec: dict) -> tuple[str, str, dict]:
    term = "recent_award_actions"
    fam = str(spec.get("family", ""))
    cond = _ACTION_FAMILY_COND.get(fam)
    if cond is None:
        raise _refuse(f"{term}.family must be new_award | more_work | funding_released | termination")
    months = _req_int(spec, "within_months", term, 1, 60)
    if months is None:
        raise _refuse(f"{term}.within_months is required (1–60)")
    mn = _req_num(spec, "min_dollars", term)
    mn_actions = _req_int(spec, "min_actions", term, 1, 100000)
    having = []
    if mn is not None:
        having.append(f"sum(obligation_sum) >= {mn}")
    if mn_actions is not None:
        having.append(f"sum(n_actions) >= {mn_actions}")
    having_sql = (" HAVING " + " AND ".join(having)) if having else ""
    sql = (f"SELECT DISTINCT uei FROM txn_recipient_month_by_type "
           f"WHERE {cond} "
           f"AND month >= date_trunc('month', current_date) - INTERVAL '{months} months' "
           f"GROUP BY uei{having_sql}")
    echo = {"term": term, "family": fam, "within_months": months,
            "min_dollars": mn, "min_actions": mn_actions}
    return "affirmative", sql, echo


def _c_profile_changes(spec: dict) -> tuple[str, str, dict]:
    term = "sam_profile_changes"
    raw = spec.get("fields")
    if not isinstance(raw, list) or not raw:
        raise _refuse(f"{term}.fields must be a non-empty list")
    for f in raw:
        if str(f) not in _PROFILE_FIELDS:
            raise _refuse(f"unknown profile field '{f}' in {term}.fields "
                          f"(known: {', '.join(sorted(_PROFILE_FIELDS))})")
    fields = sorted(set(str(f) for f in raw))
    months = _req_int(spec, "within_months", term, 1, 36)
    if months is None:
        raise _refuse(f"{term}.within_months is required (1–36)")
    in_list = ",".join(f"'{f}'" for f in fields)
    sql = (f"SELECT DISTINCT uei FROM sam_master_profile_deltas "
           f"WHERE field IN ({in_list}) "
           f"AND to_date >= current_date - INTERVAL '{months} months'")
    return "affirmative", sql, {"term": term, "fields": fields, "within_months": months}


def _c_ucc(spec: dict) -> tuple[str, str, dict]:
    term = "ucc_financing"
    status = str(spec.get("status", "active"))
    if status not in ("active", "any"):
        raise _refuse(f"{term}.status must be active | any")
    preds = ["filing_class = 'financing'"]
    if status == "active":
        preds.append("is_active_financing = TRUE")
    is_lease = spec.get("is_lease")
    if is_lease is not None:
        if not isinstance(is_lease, bool):
            raise _refuse(f"{term}.is_lease must be true or false")
        preds.append(f"is_lease = {'TRUE' if is_lease else 'FALSE'}")
    months = _req_int(spec, "last_filing_within_months", term, 1, 120)
    if months is not None:
        preds.append(f"last_filing_date >= current_date - INTERVAL '{months} months'")
    sql = "SELECT DISTINCT uei FROM sam_ucc_filings WHERE " + " AND ".join(preds)
    echo = {"term": term, "status": status, "is_lease": is_lease,
            "last_filing_within_months": months}
    return "affirmative", sql, echo


def _c_union(spec: dict) -> tuple[str, str, dict]:
    term = "union_collective_bargaining"
    active_only = spec.get("active_only", False)
    if not isinstance(active_only, bool):
        raise _refuse(f"{term}.active_only must be true or false")
    pred = "uei IS NOT NULL" + (" AND is_active = TRUE" if active_only else "")
    sql = f"SELECT DISTINCT uei FROM olms_cba_crosswalk WHERE {pred}"
    return "affirmative", sql, {"term": term, "active_only": active_only}


def _c_year_founded(spec: dict) -> tuple[str, str, dict]:
    term = "year_founded"
    mn = _req_int(spec, "min", term, 1700, 2026)
    mx = _req_int(spec, "max", term, 1700, 2026)
    if mn is None and mx is None:
        raise _refuse(f"{term} needs min and/or max")
    preds = ["year_founded IS NOT NULL"]
    if mn is not None:
        preds.append(f"year_founded >= {mn}")
    if mx is not None:
        preds.append(f"year_founded <= {mx}")
    sql = "SELECT uei FROM gtm_entity_firmographics WHERE " + " AND ".join(preds)
    return "affirmative", sql, {"term": term, "min": mn, "max": mx}


_COMPILERS: dict[str, Callable[[dict], tuple[str, str, dict]]] = {
    "total_obligations": _c_total_obligations,
    "total_current_value_of_active_awards": _c_book_value("total_current_value_of_active_awards"),
    "remaining_current_value_of_active_awards": _c_book_value("remaining_current_value_of_active_awards"),
    "median_active_award_value": _c_book_value("median_active_award_value"),
    "total_potential_value_of_open_idvs": _c_book_value("total_potential_value_of_open_idvs"),
    "remaining_potential_value_of_open_idvs": _c_book_value("remaining_potential_value_of_open_idvs"),
    "awarded_under_codes": _c_awarded_under_codes,
    "principal_naics_or_psc": _c_principal_code,
    "obligations_under_naics_psc_pairs": _c_pairs,
    "inferred_capable_of_codes": _c_inferred,
    "active_awards": _c_active_awards,
    "awards_expiring": _c_awards_expiring,
    "open_idvs": _c_open_idvs,
    "active_award_value": _c_active_award_value,
    "avg_annual_obligations": _c_avg_annual_obligations,
    "awarded_by_agency": _c_awarded_by_agency,
    "distinct_agency_breadth": _c_agency_breadth,
    "registered_in_state": _c_registered_in_state,
    "place_of_performance_of_active_awards": _c_pop_active,
    "performed_in": _c_performed_in,
    "place_of_performance_within": _c_pop_within,
    "small_business_designations": _c_designations,
    "set_aside_obligations": _c_set_aside,
    "employee_size": _c_employee_size,
    "sam_registration_active": _c_sam_active,
    "active_award_pricing_mix": _c_pricing_mix,
    "contracting_role": _c_contracting_role,
    "subcontractor_to": _c_subcontractor_to,
    "issues_subawards": _c_issues_subawards,
    "recent_award_actions": _c_recent_actions,
    "sam_profile_changes": _c_profile_changes,
    "ucc_financing": _c_ucc,
    "union_collective_bargaining": _c_union,
    "year_founded": _c_year_founded,
}

# Machine-readable vocabulary for GET /vocabulary — the BFF/model contract.
# One entry per term: family, dials (name → summary), one-line definition in
# government vocabulary. This block IS the API's self-description; ontology.ts
# mirrors it on the platform side.
VOCABULARY: list[dict[str, Any]] = [
    {"term": "total_obligations", "family": "money",
     "definition": "Sum of the firm's obligations over a clock (trailing window or fiscal-year range), by side.",
     "dials": {"side": "prime | sub | total (default total)",
               "clock": "{basis: trailing, window: 12mo|24mo|36mo|60mo|lifetime} | {basis: fiscal_years, fy_start, fy_end} (fiscal clock is prime-side only)",
               "min/max": "dollars"}},
    {"term": "total_current_value_of_active_awards", "family": "money",
     "definition": "Sum of current total value of the firm's active, non-terminated definitive contracts and task orders (IDVs excluded).",
     "dials": {"min/max": "dollars"}},
    {"term": "remaining_current_value_of_active_awards", "family": "money",
     "definition": "Current total value of active awards minus obligations to date (the unbilled remainder).",
     "dials": {"min/max": "dollars"}},
    {"term": "median_active_award_value", "family": "money",
     "definition": "Median current total value across the firm's active non-IDV awards (award-size texture).",
     "dials": {"min/max": "dollars"}},
    {"term": "total_potential_value_of_open_idvs", "family": "money",
     "definition": "Sum of potential total value (ceiling) across the firm's IDVs with open ordering windows.",
     "dials": {"min/max": "dollars"}},
    {"term": "remaining_potential_value_of_open_idvs", "family": "money",
     "definition": "Open-IDV ceilings minus obligations to date (headroom; never blended with committed work).",
     "dials": {"min/max": "dollars"}},
    {"term": "awarded_under_codes", "family": "work",
     "definition": "The firm has obligations under the named NAICS/PSC codes (exact or prefix), by side and window.",
     "dials": {"side": "prime | sub", "code_type": "naics | psc", "codes": "exact or prefix, ≤200",
               "window": "12mo|24mo|60mo|lifetime", "min_dollars": "optional floor (default > 0)"}},
    {"term": "principal_naics_or_psc", "family": "work",
     "definition": "The named codes are among the firm's principal prime work by share-of-obligations or rank.",
     "dials": {"code_type": "naics | psc", "codes": "exact or prefix",
               "basis": "share_24mo | share_lifetime | rank_24mo | rank_lifetime",
               "min_share": "(0,1] for share basis", "max_rank": "≥1 for rank basis"}},
    {"term": "obligations_under_naics_psc_pairs", "family": "work",
     "definition": "Sum of obligations within an explicit (NAICS6, PSC4) pair scope over a fiscal-year range (the generalized collections leg).",
     "dials": {"pairs": "[[naics6, psc4], …] ≤13000", "fy_start/fy_end": "any FY1962–FY2026 (fy_end omitted = present)",
               "min/max": "dollars"}},
    {"term": "inferred_capable_of_codes", "family": "work",
     "definition": "A capability model infers the firm could perform the named codes (never demonstrated performance).",
     "dials": {"side": "prime | sub", "code_type": "naics | psc", "codes": "EXACT codes only, ≤50"}},
    {"term": "active_awards", "family": "lifecycle",
     "definition": "The firm holds active (period of performance running, not terminated) prime awards.",
     "dials": {"value": "false to require NO active awards", "min_count/max_count": "award count bounds",
               "instrument": "definitive_contract | purchase_order (optional standalone-instrument filter)",
               "instrument_scope": "active | lifetime (default active)"}},
    {"term": "awards_expiring", "family": "lifecycle",
     "definition": "The firm has awards whose period of performance ends within N months.",
     "dials": {"within_months": "1–36 (required)", "min_count": "default 1", "min_obligated": "dollars"}},
    {"term": "open_idvs", "family": "lifecycle",
     "definition": "The firm holds IDVs whose ordering window is open.",
     "dials": {"value": "false to require NONE", "min_count": "default 1"}},
    {"term": "active_award_value", "family": "lifecycle",
     "definition": "The firm holds at least one open award whose life-to-date obligations fall in the dollar range.",
     "dials": {"min/max": "dollars (at least one required)"}},
    {"term": "avg_annual_obligations", "family": "momentum",
     "definition": "The firm's average prime obligations per year over the trailing five years (trailing-60-month total / 5; whole-firm).",
     "dials": {"min/max": "dollars per year (at least one required)"}},
    {"term": "awarded_by_agency", "family": "buyers",
     "definition": "The firm has obligations awarded by the named agencies (FPDS awarding-agency codes).",
     "dials": {"agencies": "codes, ≤50", "scope": "active | lifetime (default lifetime)", "min_dollars": "optional floor"}},
    {"term": "distinct_agency_breadth", "family": "buyers",
     "definition": "Count of distinct awarding agencies the firm serves (active or lifetime).",
     "dials": {"scope": "active | lifetime (default active)", "min/max": "agency count"}},
    {"term": "registered_in_state", "family": "geography",
     "definition": "The firm's SAM-registered physical address is in the named state(s).",
     "dials": {"states": "state codes"}},
    {"term": "place_of_performance_of_active_awards", "family": "geography",
     "definition": "The firm has active awards whose reported place of performance is in the named state(s).",
     "dials": {"states": "state codes"}},
    {"term": "performed_in", "family": "geography",
     "definition": "The firm performed work (any FPDS action) whose place of performance is "
                   "in the named state(s) — optionally within a fiscal-year lookback window.",
     "dials": {"states": "state codes",
               "clock": "optional {basis: fiscal_years, fy_start, fy_end}; omitted = all-time"}},
    {"term": "place_of_performance_within", "family": "geography",
     "definition": "The firm has an ACTIVE award whose place of performance lies within N miles "
                   "of the anchor (a zip5 centroid, or an explicit lat/lon such as a military "
                   "installation).",
     "dials": {"zip": "5-digit zip anchor", "lat": "anchor latitude (with lon)",
               "lon": "anchor longitude (with lat)", "miles": "radius in miles, 1-500 (default 150)",
               "label": "optional anchor display name (echoed)"}},
    {"term": "small_business_designations", "family": "designations",
     "definition": "SBA/registration designations on the firm (8(a), HUBZone, WOSB, SDVOSB, …).",
     "dials": {"designations": "tokens (dsbs_* / fsrs_* / in_dsbs)", "match": "all | any (default all)"}},
    {"term": "set_aside_obligations", "family": "designations",
     "definition": "Obligations the firm won under set-aside competition (not merely holding the certification), over a fiscal-year range.",
     "dials": {"set_aside": "any | 8a | sdvosb | wosb | hubzone", "fy_start/fy_end": "FY range", "min": "dollars (default > 0)"}},
    {"term": "employee_size", "family": "designations",
     "definition": "The firm's known employee-size band (PDL-bridged; unknown size excluded).",
     "dials": {"bands": "1-10 | 11-50 | 51-200 | 201-500 | 501-1000 | 1001-5000 | 5001-10000 | 10001+"}},
    {"term": "sam_registration_active", "family": "designations",
     "definition": "The firm's SAM registration is currently active (or explicitly inactive with value:false).",
     "dials": {"value": "true | false (default true)"}},
    {"term": "active_award_pricing_mix", "family": "pricing",
     "definition": "Shape of the firm's active obligations by contract pricing type × government financing (fixed/cost/T&M crossed with unfinanced, progress payments, performance-based, commercial).",
     "dials": {"min_fixed_share": "(0,1]", "min_ffp_unfinanced_share": "(0,1]",
               "min_financed_share": "(0,1] (any government financing)",
               "min_active_obligations": "dollars",
               "combos": "[{pricing: fixed|cost|tm_lh|other|any, financing: unfinanced|progress_payments|performance_based|commercial|other, min_dollars?, min_share?}] ≤8, ANDed"}},
    {"term": "contracting_role", "family": "relationships",
     "definition": "The firm acts as prime (trailing 24mo), subcontractor (trailing 60mo), or both.",
     "dials": {"role": "prime | subcontractor | both"}},
    {"term": "subcontractor_to", "family": "relationships",
     "definition": "The firm has received subawards under the named prime contractors.",
     "dials": {"prime_ueis": "UEIs ≤500", "window": "5y | lifetime (default 5y)", "min_dollars": "optional floor"}},
    {"term": "issues_subawards", "family": "relationships",
     "definition": "The firm, as prime, issues subawards (farms work out).",
     "dials": {"window": "5y | lifetime (default 5y)", "min_dollars": "optional floor", "min_sub_count": "optional"}},
    {"term": "recent_award_actions", "family": "momentum",
     "definition": "The firm had contract actions of the named class within N months (new award, more work, funding released, termination).",
     "dials": {"family": "new_award | more_work | funding_released | termination",
               "within_months": "1–60 (required)", "min_dollars": "optional", "min_actions": "optional"}},
    {"term": "sam_profile_changes", "family": "momentum",
     "definition": "The firm changed the named SAM registration fields within N months (NAICS added, CAGE change, name change, …).",
     "dials": {"fields": sorted(_PROFILE_FIELDS), "within_months": "1–36 (required)"}},
    {"term": "ucc_financing", "family": "risk",
     "definition": "The firm has UCC financing filings (CA/CO corpora): active financing, leases, recency.",
     "dials": {"status": "active | any (default active)", "is_lease": "optional bool",
               "last_filing_within_months": "optional 1–120"}},
    {"term": "union_collective_bargaining", "family": "risk",
     "definition": "The firm is matched to an OLMS-filed collective bargaining agreement.",
     "dials": {"active_only": "bool (default false)"}},
    {"term": "year_founded", "family": "firmographics",
     "definition": "The firm's founding year (PDL-bridged; unknown excluded).",
     "dials": {"min/max": "year"}},
]


def compile_predicates(body: dict[str, Any]) -> tuple[str, list[dict], list[str]]:
    """Compile {"predicates": [...]} → (uei-set SQL expression, echo list, disclosures).

    Pure (no I/O) — the single compile path count and entities share.
    """
    if not isinstance(body, dict):
        raise _refuse("body must be an object")
    raw = body.get("predicates")
    if not isinstance(raw, list) or not raw:
        raise _refuse("predicates must be a non-empty list")
    if len(raw) > _MAX_PREDICATES:
        raise _refuse(f"at most {_MAX_PREDICATES} predicates per query")

    affirmative: list[str] = []
    negative: list[str] = []
    echoes: list[dict] = []
    disclosures: list[str] = []

    for i, spec in enumerate(raw):
        if not isinstance(spec, dict):
            raise _refuse(f"predicates[{i}] must be an object")
        term = spec.get("term")
        compiler = _COMPILERS.get(str(term))
        if compiler is None:
            raise _refuse(f"unknown term '{term}' in predicates[{i}] "
                          "(GET /api/v1/market-query/vocabulary lists the closed vocabulary)")
        kind, sql, echo = compiler(spec)
        (affirmative if kind == "affirmative" else negative).append(sql)
        echoes.append(echo)
        d = _DISCLOSURES.get(str(term))
        if d and d not in disclosures:
            disclosures.append(d)

    if affirmative:
        expr = "\nINTERSECT\n".join(f"({leg})" for leg in affirmative)
    else:
        expr = "(SELECT uei FROM gtm_sam_entities)"
        disclosures.append(
            "No affirmative predicate: the universe is every SAM registrant, minus exclusions."
        )
    for leg in negative:
        expr = f"({expr})\nEXCEPT\n({leg})"
    return expr, echoes, disclosures


async def _run_sidecar(sql: str, limit: int) -> dict[str, Any]:
    token = os.environ.get("QUERY_SIDECAR_TOKEN")
    if not token:
        raise HTTPException(status_code=503, detail="QUERY_SIDECAR_TOKEN not configured")
    async with httpx.AsyncClient(timeout=90.0) as client:
        r = await client.post(
            f"{SIDECAR_URL}/api/v1/sql",
            headers={"Authorization": f"Bearer {token}"},
            json={"sql": sql, "limit": limit},
        )
    if r.status_code != 200:
        logger.error("sidecar market-query failed: %s %s", r.status_code, r.text[:300])
        raise HTTPException(status_code=502, detail="sidecar query failed")
    return r.json()


@router.get("/vocabulary", dependencies=[Depends(require_service_token)])
async def vocabulary() -> dict[str, Any]:
    return {"terms": VOCABULARY, "max_predicates": _MAX_PREDICATES}


@router.post("/count", dependencies=[Depends(require_service_token)])
async def count(body: dict[str, Any]) -> dict[str, Any]:
    expr, echoes, disclosures = compile_predicates(body if isinstance(body, dict) else {})
    sql = f"SELECT count(*) AS n FROM (\n{expr}\n)"
    payload = await _run_sidecar(sql, 1)
    n = payload["rows"][0][0] if payload.get("rows") else 0
    return {
        "count": n,
        "spec": {"predicates": echoes},
        "disclosures": disclosures,
        "elapsed_ms": payload.get("elapsed_ms"),
        "artifact": payload.get("artifact"),
    }


@router.post("/entities", dependencies=[Depends(require_service_token)])
async def entities(body: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(body, dict):
        raise _refuse("body must be an object")
    limit = body.get("limit", 100)
    if not isinstance(limit, int) or isinstance(limit, bool) or not (1 <= limit <= 1000):
        raise _refuse("limit must be an integer in [1, 1000]")
    expr, echoes, disclosures = compile_predicates(body)
    sql = (
        f"SELECT m.uei, s.legal_business_name FROM (\n{expr}\n) m "
        f"LEFT JOIN gtm_sam_entities s USING (uei) ORDER BY m.uei LIMIT {limit}"
    )
    payload = await _run_sidecar(sql, limit)
    rows = payload.get("rows") or []
    return {
        "entities": [{"uei": r[0], "legal_business_name": r[1]} for r in rows],
        "returned": len(rows),
        "spec": {"predicates": echoes},
        "disclosures": disclosures,
        "elapsed_ms": payload.get("elapsed_ms"),
        "artifact": payload.get("artifact"),
    }
