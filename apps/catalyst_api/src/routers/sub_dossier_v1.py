"""sub-dossier — the per-UEI subawardee dossier compiler (gc-hq).

Program authority: ~/Desktop/hq/SUBAWARDEE_DOSSIER_PROGRAM.md (v2 — the
adversarially-reviewed plan; findings F1–F15 + S1–S5 folded). For a subawardee
UEI it compiles the full dossier document in one call:

  reality   — identity, FY prime/sub series (set-aside family included),
              subaward chunk texture, capped history + separate SUM totals,
              monthly cadence.
  seeds     — the primes the firm subs under (5y edges).
  market    — lookalike primes composed from the seeds' prime code signatures
              (IDF-damped lane overlap), gated ON SELECTION by demonstrated
              sub-out behavior [F2], JV-filtered [F4], family-deduped [F3],
              hydrated with active book, farm-out in the target's work shape,
              subaward deal size, and set-aside dollars won [S3].
  closing   — the co-sub triangulation: subs of market primes who also sub
              under the seed primes, scored by the ruled three-layer calculus:
              (1) target-relative shape lens — evidence ladder over the firms'
                  OWN records (prime-signature cosine / signature-vs-lanes /
                  declared∪inferred overlap coefficient) [F1, F11];
              (2) ubiquity down-weight 1/sqrt(1+n_distinct_primes) [F10];
              (3) size-band nix as a DISPLAY layer only, never in the score.
              Peers with no lens substrate are 'insufficient' and excluded
              from the scored table [F1].

Hard-won rules encoded here (do not relax):
- The inferred-code marts are CODE-sorted (160M/263M). NEVER probe them by
  uei (measured 14.4s for 3 ueis). All inferred reads anchor on the TARGET's
  codes and filter the peer set [F1].
- Every sidecar statement passes an explicit limit — the sidecar's silent
  default is 1000 rows [F8].
- Self-pairs (subawardee == prime awardee) are excluded everywhere [F7];
  family self-pairs excluded after family resolution [F3].
- All statements after the first pin require_artifact; a 409 (snapshot moved
  mid-build) restarts the whole build exactly once.
- Zero model calls. Deterministic. All user tokens validated against typed
  ranges/regexes; nothing user-supplied is interpolated as free text.

Archetypes (2026-07-18 operator ruling — the hybrid prime+sub dossier):
  "sub"        — the original pure-subawardee calculus above (default).
  "prime_sub"  — firms with a healthy prime business AND meaningful subawarding
                 (band floors: prime ≥ hybrid_prime_floor AND sub ≥
                 hybrid_sub_floor within [band_min, band_max] total). Same
                 market chassis (seeds → seed-lookalike primes), with two
                 sharpenings, both ruled:
                 (1) the target's WORK SHAPE is its own PRIME lanes (side=
                     'prime' in gtm_entity_code_lanes) — the prime record is
                     the evidence-grade capability statement; sub-reported
                     codes are inherited from the prime award and stay
                     secondary;
                 (2) the market gate is demonstrated sub-out TO THE TARGET'S
                     SHAPE: candidates must have subaward edges to recipients
                     whose OWN PRIME HISTORY sits in the target's prime NAICS
                     codes (gtm_prime_subout_by_recipient_code,
                     recipient_code_source='awarded_prime_contracts_in_code',
                     single context type to keep dollars dedup-clean).
                 The market is never firms shaped like the target — it is
                 buyers: primes that look like the seeds and verifiably farm
                 out work to firms like the target. Payload adds a
                 prime_business section (own prime signature + book); the
                 competitor ladder needs no change (branch 1 prime-sig cosine
                 already fires when the target primes).

Endpoints (service-token gated):
  POST /api/v1/sub-dossier/build     {"uei": "...", "archetype"?: "sub"|"prime_sub",
                                      "dials"?: {...}}
       → the frozen dossier payload (version sub_dossier_v2)
  GET  /api/v1/sub-dossier/eligible?band_min&band_max&limit&archetype
       → the target-band population with targeting-richness columns
         (n_seeds, max_seed_last_action, family_flag) [F6, S4];
         archetype=prime_sub adds the per-side floors
"""
from __future__ import annotations

import logging
import math
import os
import re
import time
from datetime import date
from typing import Any

import httpx
from fastapi import APIRouter, Depends, HTTPException

from ..service_token import require_service_token

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/sub-dossier", tags=["sub-dossier"])

SIDECAR_URL = os.environ.get("QUERY_SIDECAR_URL", "https://query-sidecar-api.onrender.com")

_UEI_RE = re.compile(r"^[A-Za-z0-9]{12}$")
_CODE_RE = re.compile(r"^[A-Za-z0-9]{1,6}$")
_JV_NAME_RE = re.compile(r"\b(JV|JOINT VENTURE)\b", re.IGNORECASE)
_NAME_SUFFIX_RE = re.compile(
    r"\b(INC|INCORPORATED|LLC|L L C|LTD|LIMITED|CORP|CORPORATION|CO|COMPANY|LP|LLP|PLLC|PC|JV)\b\.?"
)
_NON_ALNUM_RE = re.compile(r"[^A-Z0-9 ]")

_BAND_FYS = (2023, 2024, 2025)
_JV_SIGNAL_TYPES = ("jv_8a_certified", "jv_econ_disadv", "jv_women_owned")
_ARCHETYPES = ("sub", "prime_sub")
# The recipient-shape lens for the hybrid market gate. ONE source lens per
# query (the cube's lenses overlap); ONE context type so dollars within a
# recipient_code are dedup-clean (a subaward has exactly one context NAICS).
_SHAPE_LENS_SOURCE = "awarded_prime_contracts_in_code"
_SHAPE_CONTEXT_TYPE = "naics"
_SHAPE_RECIPIENT_TYPE = "naics"

# Dials (SUBAWARDEE_DOSSIER_PROGRAM.md §3/§8). Every value is returned
# verbatim in method.dials; overrides are range-validated in _merge_dials.
DEFAULT_DIALS: dict[str, Any] = {
    "band_min": 1_000_000.0,
    # 2026-07-20 operator ruling: NO top cap on target eligibility — a $300M
    # sub is a subject like any other. The lookalike-prime market never had a
    # ceiling (market_prime_floor only); this band gates only the TARGET.
    "band_max": 1e12,
    "shape_floor": 250_000.0,       # target lane obl_lifetime floor for "top shape"
    "sig_rank": 5,
    "sig_share": 0.05,
    "min_lane_hits": 2,
    "market_prime_floor": 10_000_000.0,
    "market_size": 50,
    "recency_months": 24,
    "exclude_jv": True,
    # 2026-07-20 operator ruling: the demonstrated-sub-out gate on market
    # primes is DROPPED by default ("let's see") — composition match + the
    # $10M prime floor stand alone. One-flag revert; sub-out evidence still
    # rides every row (subout_5y / last_sub NULL when absent) and proven
    # primes outrank unknowns at equal composition weight.
    "require_subout": False,
    "history_limit": 500,           # [F8]
    "peer_scoring_cap": 400,        # triangle rows carried into scoring [F6]
    "ubiquity_fn": "1/sqrt(1+n)",  # [F10]; alternative: "1/ln(1+n)"
    "size_cap": 250_000_000.0,      # layer-3 display nix [ruled: last, display only]
    "competitor_limit": 15,
    "min_market_rows": 5,           # render floors [F6]
    "min_competitors": 3,
    "hybrid_prime_floor": 1_000_000.0,  # prime_sub archetype: per-side band floors
    "hybrid_sub_floor": 1_000_000.0,
}

_INT_DIALS = {"sig_rank", "min_lane_hits", "market_size", "recency_months",
              "history_limit", "peer_scoring_cap", "competitor_limit",
              "min_market_rows", "min_competitors"}
_FLOAT_DIALS = {"band_min", "band_max", "shape_floor", "sig_share",
                "market_prime_floor", "size_cap",
                "hybrid_prime_floor", "hybrid_sub_floor"}
_DIAL_BOUNDS: dict[str, tuple[float, float]] = {
    "band_min": (0, 1e12), "band_max": (0, 1e12), "shape_floor": (0, 1e10),
    "sig_rank": (1, 20), "sig_share": (0.0, 1.0), "min_lane_hits": (1, 10),
    "market_prime_floor": (0, 1e12), "market_size": (1, 200),
    "recency_months": (1, 120), "history_limit": (1, 5000),
    "peer_scoring_cap": (10, 1200), "size_cap": (0, 1e13),
    "competitor_limit": (1, 100), "min_market_rows": (0, 50),
    "min_competitors": (0, 50),
    "hybrid_prime_floor": (0, 1e12), "hybrid_sub_floor": (0, 1e12),
}

_PEER_CHUNK = 400          # IN-list chunk size for peer hydration statements
_TRIANGLE_LIMIT = 5000     # explicit statement limit on the triangle [F8]
_SIG_ROW_LIMIT = 50_000    # peers × ~20 signature rows


def _refuse(detail: str) -> HTTPException:
    return HTTPException(status_code=422, detail=detail)


def _valid_uei(raw: Any) -> str:
    if not isinstance(raw, str) or not _UEI_RE.match(raw.strip()):
        raise _refuse("uei must be a 12-character SAM UEI")
    return raw.strip().upper()


def _valid_archetype(raw: Any) -> str:
    if raw is None:
        return "sub"
    if not isinstance(raw, str) or raw not in _ARCHETYPES:
        raise _refuse(f"archetype must be one of {_ARCHETYPES}")
    return raw


def _merge_dials(overrides: Any) -> dict[str, Any]:
    dials = dict(DEFAULT_DIALS)
    if overrides is None:
        return dials
    if not isinstance(overrides, dict):
        raise _refuse("dials must be an object")
    for key, value in overrides.items():
        if key not in DEFAULT_DIALS:
            raise _refuse(f"unknown dial: {key}")
        if key in ("exclude_jv", "require_subout"):
            if not isinstance(value, bool):
                raise _refuse(f"{key} must be a boolean")
            dials[key] = value
            continue
        if key == "ubiquity_fn":
            if value not in ("1/sqrt(1+n)", "1/ln(1+n)"):
                raise _refuse("ubiquity_fn must be '1/sqrt(1+n)' or '1/ln(1+n)'")
            dials[key] = value
            continue
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            raise _refuse(f"dial {key} must be a number")
        lo, hi = _DIAL_BOUNDS[key]
        if not (lo <= value <= hi):
            raise _refuse(f"dial {key} out of range [{lo}, {hi}]")
        dials[key] = int(value) if key in _INT_DIALS else float(value)
    if dials["band_min"] > dials["band_max"]:
        raise _refuse("band_min must not exceed band_max")
    return dials


def _uei_list_sql(ueis: list[str]) -> str:
    """Validated UEIs → SQL IN-list body. Every element re-checked."""
    for u in ueis:
        if not _UEI_RE.match(u):
            raise _refuse("invalid uei in internal list")
    return ", ".join(f"'{u}'" for u in ueis)


def _code_list_sql(codes: list[str]) -> str:
    for c in codes:
        if not _CODE_RE.match(c):
            raise _refuse("invalid code in internal list")
    return ", ".join(f"'{c}'" for c in codes)


def _fy_list_sql() -> str:
    return ", ".join(str(fy) for fy in _BAND_FYS)


# ---------------------------------------------------------------------------
# Pure SQL builders (no I/O — the compile-test surface)
# ---------------------------------------------------------------------------

def sql_identity(uei: str) -> str:
    return f"""
SELECT e.uei, e.legal_business_name, e.physical_state, e.physical_city,
       e.primary_naics, e.sam_is_active, e.cage_code,
       f.employee_size_range, f.industry, f.year_founded,
       h.ultimate_parent_uei, h.ultimate_parent_name,
       b.prime_obl_lifetime, b.sub_amt_lifetime, b.active_award_ct,
       b.last_action_date,
       sp.sub_amt_lifetime AS sp_sub_lifetime, sp.median_chunk_lifetime,
       sp.p20_chunk_lifetime, sp.p80_chunk_lifetime,
       sp.n_distinct_primes_lifetime, sp.top_buyer_uei,
       sp.top_buyer_share_lifetime_pct, sp.cagr_5y_pct,
       sp.first_action_date AS sub_first_action, sp.last_action_date AS sub_last_action
FROM gtm_sam_entities e
LEFT JOIN gtm_entity_firmographics f USING(uei)
LEFT JOIN entity_hierarchy h USING(uei)
LEFT JOIN gtm_entity_behavior_rollup b USING(uei)
LEFT JOIN gtm_sub_profiles sp ON sp.uei = e.uei
WHERE e.uei = '{uei}'"""


def sql_fy_prime(uei: str) -> str:
    return f"""
SELECT fy, SUM(won_obl) AS prime_won,
       SUM(won_obl_set_aside) AS sa_any, SUM(won_obl_8a) AS sa_8a,
       SUM(won_obl_sdvosb) AS sa_sdvosb, SUM(won_obl_wosb) AS sa_wosb,
       SUM(won_obl_hubzone) AS sa_hubzone
FROM gtm_entity_fy_won WHERE uei = '{uei}' GROUP BY 1 ORDER BY 1"""


def sql_fy_sub(uei: str) -> str:
    return f"""
SELECT subaward_action_date_fiscal_year AS fy, SUM(subaward_amount_num) AS sub_amt,
       COUNT(*) AS sub_ct
FROM subaward_canonical_slim_by_sub
WHERE subawardee_uei = '{uei}' AND subawardee_uei <> prime_awardee_uei
GROUP BY 1 ORDER BY 1"""


def sql_history(uei: str, history_limit: int) -> str:
    return f"""
SELECT prime_awardee_uei, prime_awardee_name, subaward_amount_num,
       subaward_action_date, subaward_action_date_fiscal_year,
       prime_award_naics_code, prime_award_product_or_service_code,
       prime_award_awarding_agency_name, subaward_description
FROM subaward_canonical_slim_by_sub
WHERE subawardee_uei = '{uei}' AND subawardee_uei <> prime_awardee_uei
ORDER BY subaward_action_date DESC
LIMIT {int(history_limit)}"""


def sql_history_totals(uei: str) -> str:
    return f"""
SELECT COUNT(*) AS n_rows, SUM(subaward_amount_num) AS total_amt,
       COUNT(DISTINCT prime_awardee_uei) AS distinct_primes
FROM subaward_canonical_slim_by_sub
WHERE subawardee_uei = '{uei}' AND subawardee_uei <> prime_awardee_uei"""


def sql_monthly_sub(uei: str) -> str:
    return f"""
SELECT date_trunc('month', subaward_action_date) AS month,
       SUM(subaward_amount_num) AS amt, COUNT(*) AS n
FROM subaward_canonical_slim_by_sub
WHERE subawardee_uei = '{uei}' AND subawardee_uei <> prime_awardee_uei
GROUP BY 1 ORDER BY 1"""


def sql_seeds(uei: str) -> str:
    return f"""
SELECT prime_uei, prime_name, edge_dollars_5y, edge_count_5y,
       edge_dollars_lifetime, first_action_date, last_action_date
FROM gtm_prime_sub_pairs_by_sub
WHERE sub_uei = '{uei}' AND edge_dollars_5y > 0 AND prime_uei <> '{uei}'
ORDER BY edge_dollars_5y DESC"""


def sql_target_lanes(uei: str) -> str:
    return f"""
SELECT side, code_type, code, obl_lifetime, obl_60mo, action_ct
FROM gtm_entity_code_lanes WHERE uei = '{uei}'
ORDER BY obl_lifetime DESC"""


def sql_signature_rows(ueis: list[str]) -> str:
    """FULL signature rows — vector construction ignores rank/share filters [F11]."""
    return f"""
SELECT uei, code_type, code, share_lifetime, rank_lifetime, obl_lifetime
FROM gtm_prime_code_signature WHERE uei IN ({_uei_list_sql(ueis)})"""


# The candidate-fetch honesty ceiling: market SQL and the run() limit BOTH
# use it — market.total must never be shaped by market_size (2026-07-20; the
# first fix raised only the run() limit while the SQL kept LIMIT size*2,
# quietly capping totals near 100 — caught when a dropped gate DECREASED a
# total, which is impossible under a pure superset).
_MARKET_FETCH_CEILING = 25_000


def sql_market(uei: str, seeds: list[str], target_naics: list[str],
               target_psc: list[str], dials: dict[str, Any],
               shape_naics: list[str] | None = None) -> str:
    """shape_naics (prime_sub archetype only): candidates must ALSO have
    demonstrated sub-out to recipients whose own prime history sits in these
    codes — the ruled recipient-shape gate. None = the original sub calculus."""
    seeds_sql = _uei_list_sql(seeds)
    excl_sql = _uei_list_sql(seeds + [uei])
    if target_naics or target_psc:
        conds = []
        if target_naics:
            conds.append(f"naics_code IN ({_code_list_sql(target_naics)})")
        if target_psc:
            conds.append(f"psc_code IN ({_code_list_sql(target_psc)})")
        fo_where = " OR ".join(conds)
    else:
        fo_where = "1 = 0"
    if shape_naics:
        shape_cte = f""",
shape_out AS (
  SELECT prime_awardee_uei AS uei, MAX(last_subaward_action_date) AS shape_last
  FROM gtm_prime_subout_by_recipient_code
  WHERE recipient_code_source = '{_SHAPE_LENS_SOURCE}'
    AND recipient_code_type = '{_SHAPE_RECIPIENT_TYPE}'
    AND context_code_type = '{_SHAPE_CONTEXT_TYPE}'
    AND recipient_code IN ({_code_list_sql(shape_naics)})
    AND subaward_amt_total > 0
  GROUP BY 1)"""
        shape_join = "JOIN shape_out sh ON sh.uei = c.uei"
    else:
        shape_cte = ""
        shape_join = ""
    fetch = _MARKET_FETCH_CEILING  # FULL candidate set; display cap applies post-trim
    if dials.get("require_subout"):
        subout_join = "JOIN"
        subout_where = (f"AND (so.last_sub >= current_date - INTERVAL "
                        f"{int(dials['recency_months'])} MONTH OR COALESCE(fo.fo_amt, 0) > 0)")
    else:
        subout_join = "LEFT JOIN"
        subout_where = ""
        shape_join = shape_join.replace("JOIN shape_out", "LEFT JOIN shape_out")
    return f"""
WITH seed_sig AS (
  SELECT code_type, code, COUNT(DISTINCT uei) AS seed_ct
  FROM gtm_prime_code_signature
  WHERE uei IN ({seeds_sql})
    AND rank_lifetime <= {int(dials["sig_rank"])} AND share_lifetime >= {float(dials["sig_share"])}
  GROUP BY 1, 2),
lane_freq AS (
  SELECT s.code_type, s.code, COUNT(DISTINCT s.uei) AS n_with_lane
  FROM gtm_prime_code_signature s
  JOIN seed_sig g ON g.code_type = s.code_type AND g.code = s.code
  WHERE s.rank_lifetime <= {int(dials["sig_rank"])} AND s.share_lifetime >= {float(dials["sig_share"])}
  GROUP BY 1, 2),
cand AS (
  SELECT s.uei, COUNT(*) AS lane_hits,
         SUM(g.seed_ct * s.share_lifetime / LN(1 + f.n_with_lane)) AS wt
  FROM gtm_prime_code_signature s
  JOIN seed_sig g ON g.code_type = s.code_type AND g.code = s.code
  JOIN lane_freq f ON f.code_type = s.code_type AND f.code = s.code
  WHERE s.rank_lifetime <= {int(dials["sig_rank"])} AND s.share_lifetime >= {float(dials["sig_share"])}
    AND s.uei NOT IN ({excl_sql})
  GROUP BY 1 HAVING COUNT(*) >= {int(dials["min_lane_hits"])}),
sub_out AS (
  SELECT prime_uei, MAX(last_action_date) AS last_sub, SUM(edge_dollars_5y) AS subout_5y
  FROM gtm_prime_sub_pairs WHERE edge_dollars_5y > 0 GROUP BY 1),
fo AS (
  SELECT uei, SUM(farmout_amt_60mo) AS fo_amt
  FROM gtm_prime_farmout_combo_lanes
  WHERE {fo_where}
  GROUP BY 1){shape_cte}
SELECT c.uei, c.lane_hits, c.wt, so.subout_5y, so.last_sub, b.prime_obl_60mo
FROM cand c
JOIN gtm_entity_behavior_rollup b USING(uei)
{subout_join} sub_out so ON so.prime_uei = c.uei
LEFT JOIN fo ON fo.uei = c.uei
{shape_join}
WHERE b.prime_obl_60mo >= {float(dials["market_prime_floor"])}
  {subout_where}
ORDER BY c.wt DESC, so.subout_5y DESC NULLS LAST, c.lane_hits DESC, c.uei
LIMIT {fetch}"""


def sql_market_hydrate(ueis: list[str]) -> str:
    return f"""
SELECT e.uei, e.legal_business_name, e.physical_state,
       h.ultimate_parent_uei, h.ultimate_parent_name,
       bk.committed_award_ct, bk.committed_value, bk.committed_runway,
       bk.next_committed_end_date, bk.active_agency_ct
FROM gtm_sam_entities e
LEFT JOIN entity_hierarchy h USING(uei)
LEFT JOIN gtm_entity_award_book bk USING(uei)
WHERE e.uei IN ({_uei_list_sql(ueis)})"""


def sql_jv_flags(ueis: list[str]) -> str:
    types = ", ".join(f"'{t}'" for t in _JV_SIGNAL_TYPES)
    return f"""
SELECT DISTINCT uei FROM gtm_fpds_entity_signal_events
WHERE uei IN ({_uei_list_sql(ueis)}) AND signal_type IN ({types})"""


def sql_market_farmout(ueis: list[str], target_naics: list[str],
                       target_psc: list[str]) -> str:
    conds = []
    if target_naics:
        conds.append(f"naics_code IN ({_code_list_sql(target_naics)})")
    if target_psc:
        conds.append(f"psc_code IN ({_code_list_sql(target_psc)})")
    fo_where = " OR ".join(conds) if conds else "1 = 0"
    return f"""
SELECT uei, SUM(farmout_amt_60mo) AS fo_amt_60mo, SUM(prime_obl_60mo) AS prime_obl_60mo
FROM gtm_prime_farmout_combo_lanes
WHERE uei IN ({_uei_list_sql(ueis)}) AND ({fo_where})
GROUP BY 1"""


def sql_market_setaside(ueis: list[str]) -> str:
    return f"""
SELECT uei, SUM(won_obl_set_aside) AS sa_any, SUM(won_obl_8a) AS sa_8a,
       SUM(won_obl_sdvosb) AS sa_sdvosb, SUM(won_obl_wosb) AS sa_wosb,
       SUM(won_obl_hubzone) AS sa_hubzone
FROM gtm_entity_fy_won
WHERE uei IN ({_uei_list_sql(ueis)}) AND fy IN ({_fy_list_sql()})
GROUP BY 1"""


def sql_market_dealsize(ueis: list[str], target_naics: list[str]) -> str:
    naics_pred = (f"AND prime_award_naics_code IN ({_code_list_sql(target_naics)})"
                  if target_naics else "")
    return f"""
SELECT prime_awardee_uei AS uei, COUNT(*) AS n,
       MEDIAN(subaward_amount_num) AS median_deal, AVG(subaward_amount_num) AS avg_deal
FROM subaward_canonical_slim
WHERE prime_awardee_uei IN ({_uei_list_sql(ueis)})
  AND subaward_action_date_fiscal_year >= {_BAND_FYS[0]}
  AND subawardee_uei <> prime_awardee_uei
  {naics_pred}
GROUP BY 1"""


def sql_market_shape_subout(ueis: list[str], shape_naics: list[str]) -> str:
    """Hybrid hydration: per (candidate, recipient_code) the CLEAN dollars
    subbed out to recipients whose own prime history sits in that code
    (single source lens + single context type — no double count within a
    code; dollars are NEVER summed across recipient codes, the same subaward
    edge can satisfy several shape codes)."""
    return f"""
SELECT prime_awardee_uei AS uei, recipient_code,
       SUM(subaward_amt_total) AS amt, SUM(subaward_edge_ct) AS edge_ct,
       MAX(last_subaward_action_date) AS last_action
FROM gtm_prime_subout_by_recipient_code
WHERE prime_awardee_uei IN ({_uei_list_sql(ueis)})
  AND recipient_code_source = '{_SHAPE_LENS_SOURCE}'
  AND recipient_code_type = '{_SHAPE_RECIPIENT_TYPE}'
  AND context_code_type = '{_SHAPE_CONTEXT_TYPE}'
  AND recipient_code IN ({_code_list_sql(shape_naics)})
  AND subaward_amt_total > 0
GROUP BY 1, 2"""


def sql_triangle(uei: str, market: list[str], seeds: list[str], cap: int) -> str:
    return f"""
WITH tri AS (
  SELECT p.sub_uei, p.sub_name,
         COUNT(DISTINCT p.prime_uei) AS market_primes_ct,
         SUM(p.edge_dollars_5y) AS dollars_from_market_5y,
         MAX(p.last_action_date) AS last_action,
         list(DISTINCT p.prime_uei) AS market_prime_ueis
  FROM gtm_prime_sub_pairs p
  WHERE p.prime_uei IN ({_uei_list_sql(market)}) AND p.edge_dollars_5y > 0
    AND p.sub_uei <> '{uei}' AND p.sub_uei <> p.prime_uei
  GROUP BY 1, 2),
shared AS (
  SELECT p2.sub_uei, COUNT(DISTINCT p2.prime_uei) AS shared_seed_primes
  FROM gtm_prime_sub_pairs_by_sub p2
  WHERE p2.prime_uei IN ({_uei_list_sql(seeds)}) AND p2.edge_dollars_5y > 0
  GROUP BY 1)
SELECT t.sub_uei, t.sub_name, t.market_primes_ct, t.dollars_from_market_5y,
       t.last_action, t.market_prime_ueis, s.shared_seed_primes
FROM tri t JOIN shared s USING(sub_uei)
ORDER BY s.shared_seed_primes DESC, t.dollars_from_market_5y DESC, t.sub_uei
LIMIT {int(cap)}"""


def sql_peer_pack(ueis: list[str]) -> str:
    return f"""
SELECT e.uei, e.legal_business_name,
       h.ultimate_parent_uei, h.ultimate_parent_name,
       sp.n_distinct_primes_lifetime
FROM gtm_sam_entities e
LEFT JOIN entity_hierarchy h USING(uei)
LEFT JOIN gtm_sub_profiles sp ON sp.uei = e.uei
WHERE e.uei IN ({_uei_list_sql(ueis)})"""


def sql_peer_band(ueis: list[str]) -> str:
    lst = _uei_list_sql(ueis)
    return f"""
WITH s AS (
  SELECT subawardee_uei AS uei, SUM(subaward_amount_num) AS sub_amt
  FROM subaward_canonical_slim_by_sub
  WHERE subawardee_uei IN ({lst})
    AND subaward_action_date_fiscal_year IN ({_fy_list_sql()})
    AND subawardee_uei <> prime_awardee_uei
  GROUP BY 1),
p AS (
  SELECT uei, SUM(won_obl) AS prime_won FROM gtm_entity_fy_won
  WHERE uei IN ({lst}) AND fy IN ({_fy_list_sql()}) GROUP BY 1)
SELECT COALESCE(s.uei, p.uei) AS uei, COALESCE(s.sub_amt, 0) AS sub_amt,
       COALESCE(p.prime_won, 0) AS prime_won
FROM s FULL OUTER JOIN p ON s.uei = p.uei"""


def sql_peer_declared(ueis: list[str]) -> str:
    return f"""
SELECT uei, code_type, code FROM v_sam_declared_codes
WHERE uei IN ({_uei_list_sql(ueis)}) AND is_active"""


def sql_peer_inferred(ueis: list[str], code_type: str, codes: list[str]) -> str:
    """CODE-anchored [F1] — the sort key leads; NEVER uei-first on this mart."""
    if code_type not in ("naics", "psc"):
        raise _refuse("inferred code_type must be naics or psc")
    return f"""
SELECT uei, code_type, code FROM gtm_entity_inferred_subbable_codes
WHERE code_type = '{code_type}' AND code IN ({_code_list_sql(codes)})
  AND uei IN ({_uei_list_sql(ueis)})"""


def sql_competitor_dollars_24mo(market: list[str], peers: list[str]) -> str:
    return f"""
SELECT subawardee_uei AS uei, SUM(subaward_amount_num) AS amt_24mo,
       COUNT(*) AS n_24mo
FROM subaward_canonical_slim
WHERE prime_awardee_uei IN ({_uei_list_sql(market)})
  AND subawardee_uei IN ({_uei_list_sql(peers)})
  AND subaward_action_date >= current_date - INTERVAL 24 MONTH
  AND subawardee_uei <> prime_awardee_uei
GROUP BY 1"""


def sql_freshness() -> str:
    return """
SELECT date_trunc('month', subaward_action_date) AS month, COUNT(*) AS n
FROM subaward_canonical_slim
WHERE subaward_action_date >= current_date - INTERVAL 30 MONTH
GROUP BY 1 ORDER BY 1"""


def sql_eligible(band_min: float, band_max: float, limit: int,
                 prime_floor: float = 0.0, sub_floor: float = 0.0) -> str:
    """prime_floor/sub_floor > 0 = the prime_sub archetype's per-side floors."""
    floors = ""
    if prime_floor > 0:
        floors += f"\n  AND COALESCE(p.prime_won, 0) >= {float(prime_floor)}"
    if sub_floor > 0:
        floors += f"\n  AND s.sub_amt >= {float(sub_floor)}"
    return f"""
WITH sub AS (
  SELECT subawardee_uei AS uei, SUM(subaward_amount_num) AS sub_amt,
         COUNT(DISTINCT prime_awardee_uei) AS primes
  FROM subaward_canonical_slim_by_sub
  WHERE subaward_action_date_fiscal_year IN ({_fy_list_sql()})
    AND subawardee_uei <> prime_awardee_uei
  GROUP BY 1),
pw AS (
  SELECT uei, SUM(won_obl) AS prime_won FROM gtm_entity_fy_won
  WHERE fy IN ({_fy_list_sql()}) GROUP BY 1),
seeds AS (
  SELECT sub_uei AS uei, COUNT(*) AS n_seeds, MAX(last_action_date) AS max_seed_last_action
  FROM gtm_prime_sub_pairs_by_sub
  WHERE edge_dollars_5y > 0 AND prime_uei <> sub_uei
  GROUP BY 1)
SELECT s.uei, e.legal_business_name, e.physical_state,
       s.sub_amt, COALESCE(p.prime_won, 0) AS prime_won, s.primes,
       COALESCE(sd.n_seeds, 0) AS n_seeds, sd.max_seed_last_action,
       h.ultimate_parent_uei, h.ultimate_parent_name,
       COALESCE(pb.prime_obl_60mo, 0) AS parent_prime_obl_60mo
FROM sub s
LEFT JOIN pw p USING(uei)
JOIN gtm_sam_entities e USING(uei)
LEFT JOIN seeds sd USING(uei)
LEFT JOIN entity_hierarchy h USING(uei)
LEFT JOIN gtm_entity_behavior_rollup pb ON pb.uei = h.ultimate_parent_uei
WHERE s.sub_amt + COALESCE(p.prime_won, 0) BETWEEN {float(band_min)} AND {float(band_max)}{floors}
ORDER BY s.sub_amt + COALESCE(p.prime_won, 0) DESC
LIMIT {int(limit)}"""


# ---------------------------------------------------------------------------
# Pure scoring helpers (unit-testable, no I/O)
# ---------------------------------------------------------------------------

def normalize_name_key(name: str | None) -> str | None:
    """Family fallback key when entity_hierarchy has no row [F3]."""
    if not name:
        return None
    key = _NON_ALNUM_RE.sub(" ", name.upper())
    key = _NAME_SUFFIX_RE.sub(" ", key)
    key = " ".join(key.split())
    return key or None


def family_key(uei: str, ultimate_parent_uei: str | None, name: str | None) -> str:
    if ultimate_parent_uei:
        return f"h:{ultimate_parent_uei}"
    nk = normalize_name_key(name)
    return f"n:{nk}" if nk else f"u:{uei}"


def is_jv_name(name: str | None) -> bool:
    return bool(name and _JV_NAME_RE.search(name))


def cosine(a: dict[str, float], b: dict[str, float]) -> float:
    if not a or not b:
        return 0.0
    dot = sum(w * b.get(k, 0.0) for k, w in a.items())
    na = math.sqrt(sum(w * w for w in a.values()))
    nb = math.sqrt(sum(w * w for w in b.values()))
    if na == 0.0 or nb == 0.0:
        return 0.0
    return dot / (na * nb)


def peer_share_in_set(peer_weights: dict[str, float], target_codes: set[str]) -> float:
    """Branch 2: fraction of the peer's own prime allocation inside the
    target's code universe (target-relative by construction)."""
    total = sum(peer_weights.values())
    if total <= 0.0:
        return 0.0
    hit = sum(w for k, w in peer_weights.items() if k in target_codes)
    return hit / total


def overlap_coefficient(peer_codes: set[str], target_codes: set[str]) -> float:
    """Branch 3 [F1]: |peer ∩ target| / |target| — the target-relative overlap
    coefficient (peer-side denominator deliberately not used: the inferred
    marts are code-sorted, so only the intersection with the target's codes is
    cheaply knowable)."""
    if not target_codes:
        return 0.0
    return len(peer_codes & target_codes) / len(target_codes)


def ubiquity_weight(n_distinct_primes: int | None, fn: str) -> float:
    n = max(0, int(n_distinct_primes or 0))
    if fn == "1/ln(1+n)":
        return 1.0 / math.log(1 + max(n, 1) + 1e-9)
    return 1.0 / math.sqrt(1 + n)


def last_complete_month(monthly: list[tuple[str, int]]) -> str | None:
    """[F5] Walking backward from the latest month, the first whose row count
    is >= 50% of the median of the 12 months preceding the last 4."""
    if len(monthly) < 8:
        return monthly[-1][0] if monthly else None
    counts = [n for _, n in monthly]
    base = sorted(counts[max(0, len(counts) - 16):len(counts) - 4])
    if not base:
        return monthly[-1][0]
    median = base[len(base) // 2]
    threshold = 0.5 * median
    for month, n in reversed(monthly):
        if n >= threshold:
            return month
    return monthly[0][0]


# ---------------------------------------------------------------------------
# Sidecar executor
# ---------------------------------------------------------------------------

class ArtifactMoved(Exception):
    """Sidecar 409 — the serving snapshot changed mid-build."""


async def _run_sidecar(client: httpx.AsyncClient, sql: str, limit: int,
                       require_artifact: str | None = None) -> dict[str, Any]:
    token = os.environ.get("QUERY_SIDECAR_TOKEN")
    if not token:
        raise HTTPException(status_code=503, detail="QUERY_SIDECAR_TOKEN not configured")
    body: dict[str, Any] = {"sql": sql, "limit": limit}
    if require_artifact:
        body["require_artifact"] = require_artifact
    r = await client.post(
        f"{SIDECAR_URL}/api/v1/sql",
        headers={"Authorization": f"Bearer {token}"},
        json=body,
    )
    if r.status_code == 409:
        raise ArtifactMoved()
    if r.status_code != 200:
        logger.error("sidecar sub-dossier failed: %s %s", r.status_code, r.text[:300])
        raise HTTPException(status_code=502, detail="sidecar query failed")
    return r.json()


def _rows_as_dicts(payload: dict[str, Any]) -> list[dict[str, Any]]:
    cols = payload.get("columns") or []
    return [dict(zip(cols, row)) for row in payload.get("rows") or []]


def _chunks(items: list[str], size: int) -> list[list[str]]:
    return [items[i:i + size] for i in range(0, len(items), size)]


# ---------------------------------------------------------------------------
# Build orchestration
# ---------------------------------------------------------------------------

async def _build_dossier(uei: str, dials: dict[str, Any],
                         archetype: str = "sub") -> dict[str, Any]:
    timings: list[dict[str, Any]] = []
    artifact: str | None = None
    statement_count = 0

    async with httpx.AsyncClient(timeout=90.0) as client:

        async def run(stage: str, sql: str, limit: int) -> list[dict[str, Any]]:
            nonlocal artifact, statement_count
            t0 = time.monotonic()
            payload = await _run_sidecar(client, sql, limit, require_artifact=artifact)
            if artifact is None:
                artifact = payload.get("artifact")
            statement_count += 1
            timings.append({
                "stage": stage,
                "elapsed_ms": payload.get("elapsed_ms"),
                "wall_ms": round((time.monotonic() - t0) * 1000, 1),
            })
            return _rows_as_dicts(payload)

        # ---- identity (404 gate) ------------------------------------------
        ident_rows = await run("identity", sql_identity(uei), 2)
        if not ident_rows:
            raise HTTPException(status_code=404, detail="unknown_uei")
        ident = ident_rows[0]
        target_name = ident.get("legal_business_name")
        target_family = family_key(uei, ident.get("ultimate_parent_uei"), target_name)

        # ---- reality ------------------------------------------------------
        fy_prime = await run("fy_prime", sql_fy_prime(uei), 100)
        fy_sub = await run("fy_sub", sql_fy_sub(uei), 100)
        texture_source = ident  # gtm_sub_profiles columns joined in identity
        history_rows = await run("history", sql_history(uei, dials["history_limit"]),
                                 dials["history_limit"])
        totals = (await run("history_totals", sql_history_totals(uei), 2) or [{}])[0]
        monthly_sub = await run("monthly_sub", sql_monthly_sub(uei), 1000)

        prime_by_fy = {int(r["fy"]): r for r in fy_prime if r.get("fy") is not None}
        sub_by_fy = {int(r["fy"]): r for r in fy_sub if r.get("fy") is not None}
        all_fys = sorted(set(prime_by_fy) | set(sub_by_fy))
        fy_series = [{
            "fy": fy,
            "prime_won": float(prime_by_fy.get(fy, {}).get("prime_won") or 0),
            "sub_amt": float(sub_by_fy.get(fy, {}).get("sub_amt") or 0),
        } for fy in all_fys]
        band_prime = sum(float(prime_by_fy.get(fy, {}).get("prime_won") or 0) for fy in _BAND_FYS)
        band_sub = sum(float(sub_by_fy.get(fy, {}).get("sub_amt") or 0) for fy in _BAND_FYS)
        in_band = dials["band_min"] <= (band_prime + band_sub) <= dials["band_max"]
        if archetype == "prime_sub":
            in_band = (in_band and band_prime >= dials["hybrid_prime_floor"]
                       and band_sub >= dials["hybrid_sub_floor"])
        target_primes_ever = any(float(r.get("prime_won") or 0) != 0 for r in fy_prime)

        n_history_total = int(totals.get("n_rows") or 0)
        reality = {
            "fy_series": fy_series,
            "sub_texture": {
                "median_chunk": texture_source.get("median_chunk_lifetime"),
                "p20": texture_source.get("p20_chunk_lifetime"),
                "p80": texture_source.get("p80_chunk_lifetime"),
                "n_primes_lifetime": texture_source.get("n_distinct_primes_lifetime"),
                "top_buyer": {
                    "uei": texture_source.get("top_buyer_uei"),
                    "share_pct": texture_source.get("top_buyer_share_lifetime_pct"),
                },
                "cagr_5y_pct": texture_source.get("cagr_5y_pct"),
                "first_action": texture_source.get("sub_first_action"),
                "last_action": texture_source.get("sub_last_action"),
            },
            "history": [{
                "prime_uei": r.get("prime_awardee_uei"),
                "prime_name": r.get("prime_awardee_name"),
                "amount": r.get("subaward_amount_num"),
                "date": r.get("subaward_action_date"),
                "fy": r.get("subaward_action_date_fiscal_year"),
                "naics": r.get("prime_award_naics_code"),
                "psc": r.get("prime_award_product_or_service_code"),
                "agency": r.get("prime_award_awarding_agency_name"),
                "description": r.get("subaward_description"),
            } for r in history_rows],
            "history_truncated": n_history_total > len(history_rows),
            "history_totals": {
                "n_rows": n_history_total,
                "total_amt": float(totals.get("total_amt") or 0),
                "distinct_primes": int(totals.get("distinct_primes") or 0),
            },
            "monthly_sub": [{"month": r.get("month"), "amt": r.get("amt"), "n": r.get("n")}
                            for r in monthly_sub],
        }

        # ---- seeds --------------------------------------------------------
        seed_rows = await run("seeds", sql_seeds(uei), 500)
        seeds = [r["prime_uei"] for r in seed_rows if r.get("prime_uei")]
        seed_payload = [{
            "uei": r.get("prime_uei"), "name": r.get("prime_name"),
            "edge_dollars_5y": r.get("edge_dollars_5y"),
            "edge_count_5y": r.get("edge_count_5y"),
            "edge_dollars_lifetime": r.get("edge_dollars_lifetime"),
            "first": r.get("first_action_date"), "last": r.get("last_action_date"),
        } for r in seed_rows]

        # ---- target shape + signature ------------------------------------
        lanes = await run("target_lanes", sql_target_lanes(uei), 1000)
        sub_lanes = [r for r in lanes if r.get("side") == "sub"]
        target_sub_codes = {f"{r['code_type']}:{r['code']}" for r in sub_lanes if r.get("code")}

        target_sig_rows = await run("target_signature", sql_signature_rows([uei]),
                                    _SIG_ROW_LIMIT)
        target_vec = {f"{r['code_type']}:{r['code']}": float(r.get("share_lifetime") or 0)
                      for r in target_sig_rows if r.get("code")}
        target_has_signature = bool(target_vec)
        target_decl_rows = await run("target_declared", sql_peer_declared([uei]),
                                     _SIG_ROW_LIMIT)
        target_decl = {f"{r['code_type']}:{r['code']}" for r in target_decl_rows
                       if r.get("code")}

        # Work-shape codes feed the market's farm-out/deal-size context and,
        # for prime_sub, the recipient-shape gate. sub archetype: demonstrated
        # sub lanes. prime_sub archetype: the target's OWN PRIME lanes (ruled —
        # the prime record is the evidence-grade shape statement), with the
        # prime signature as fallback when every lane sits under the floor.
        def _lane_codes(rows: list[dict[str, Any]], code_type: str) -> list[str]:
            return [r["code"] for r in rows
                    if r.get("code_type") == code_type
                    and float(r.get("obl_lifetime") or 0) >= dials["shape_floor"]][:12]

        def _sig_codes(code_type: str) -> list[str]:
            pref = f"{code_type}:"
            ranked = sorted(target_vec.items(), key=lambda kv: -kv[1])
            return [k.split(":", 1)[1] for k, _ in ranked if k.startswith(pref)][:12]

        shape_naics: list[str] | None = None
        if archetype == "prime_sub":
            prime_lanes = [r for r in lanes if r.get("side") == "prime"]
            top_naics = _lane_codes(prime_lanes, "naics") or _sig_codes("naics")
            top_psc = _lane_codes(prime_lanes, "psc") or _sig_codes("psc")
            shape_naics = top_naics
        else:
            top_naics = _lane_codes(sub_lanes, "naics")
            top_psc = _lane_codes(sub_lanes, "psc")

        # ---- freshness [F5] ----------------------------------------------
        fresh_rows = await run("freshness", sql_freshness(), 100)
        monthly_counts = [(str(r.get("month")), int(r.get("n") or 0)) for r in fresh_rows]
        max_sub_date = max((r.get("subaward_action_date") for r in history_rows
                            if r.get("subaward_action_date")), default=None)
        freshness = {
            "last_complete_month": last_complete_month(monthly_counts),
            "max_subaward_action_date": max_sub_date,
        }

        method_base = {
            "timings": timings,
            "freshness": freshness,
            "dials": dials,
            "disclosures": (_DISCLOSURES + _HYBRID_DISCLOSURES
                            if archetype == "prime_sub" else _DISCLOSURES),
        }

        # ---- prime business (prime_sub archetype) -------------------------
        prime_business: dict[str, Any] | None = None
        if archetype == "prime_sub":
            sig_sorted = sorted(target_sig_rows,
                                key=lambda r: (r.get("rank_lifetime") or 9999))
            prime_business = {
                "signature": [{
                    "code_type": r.get("code_type"), "code": r.get("code"),
                    "share_lifetime": r.get("share_lifetime"),
                    "rank_lifetime": r.get("rank_lifetime"),
                    "obl_lifetime": r.get("obl_lifetime"),
                } for r in sig_sorted[:20]],
                "prime_obl_lifetime": ident.get("prime_obl_lifetime"),
                "active_award_ct": ident.get("active_award_ct"),
                "fy_set_aside": [{
                    "fy": int(r["fy"]),
                    "any": float(r.get("sa_any") or 0),
                    "8a": float(r.get("sa_8a") or 0),
                    "sdvosb": float(r.get("sa_sdvosb") or 0),
                    "wosb": float(r.get("sa_wosb") or 0),
                    "hubzone": float(r.get("sa_hubzone") or 0),
                } for r in fy_prime if r.get("fy") is not None],
                "shape_naics": list(shape_naics or []),
            }

        # ---- degradation ladder: no seeds → reality-only dossier [F14] ----
        if not seeds:
            method_base["statement_count"] = statement_count
            return _payload(uei, ident, in_band, band_prime, band_sub, reality,
                            seed_payload, None, "no_seeds", None, "no_seeds",
                            target_family, method_base, [], artifact,
                            archetype, prime_business)

        # ---- hybrid guard: no prime shape → market not composable ---------
        if archetype == "prime_sub" and not shape_naics:
            method_base["statement_count"] = statement_count
            return _payload(uei, ident, in_band, band_prime, band_sub, reality,
                            seed_payload, None, "no_prime_shape", None,
                            "no_prime_shape", target_family, method_base, [],
                            artifact, archetype, prime_business)

        # ---- market composition [F2, F9] ---------------------------------
        # Fetch ALL gate-passing candidates (bounded only by the honesty
        # ceiling) so the TRUE market total is exact - the display list is
        # capped by market_size, the COUNT never is (operator ruling
        # 2026-07-20: guarantees are priced against the real number).
        market_rows = await run(
            "market", sql_market(uei, seeds, top_naics, top_psc, dials,
                                 shape_naics=shape_naics),
            _MARKET_FETCH_CEILING)
        market_ueis_raw = [r["uei"] for r in market_rows if r.get("uei")]

        market: list[dict[str, Any]] = []
        market_reason: str | None = None
        market_total = 0
        market_total_capped = False
        if market_ueis_raw:
            hyd = {r["uei"]: r for r in await run(
                "market_hydrate", sql_market_hydrate(market_ueis_raw),
                len(market_ueis_raw) + 10)}
            jv_flagged: set[str] = set()
            if dials["exclude_jv"]:
                jv_flagged = {r["uei"] for r in await run(
                    "jv_flags", sql_jv_flags(market_ueis_raw),
                    len(market_ueis_raw) + 10)}

            seen_families: set[str] = set()
            kept: list[dict[str, Any]] = []
            market_total = 0                       # exact post-trim total
            for r in market_rows:
                mu = r["uei"]
                h = hyd.get(mu, {})
                name = h.get("legal_business_name")
                fam = family_key(mu, h.get("ultimate_parent_uei"), name)
                if fam == target_family:
                    continue                       # [F3] target's own family
                if dials["exclude_jv"] and (mu in jv_flagged or is_jv_name(name)):
                    continue                       # [F4]
                if fam in seen_families:
                    continue                       # [F3] one row per family
                seen_families.add(fam)
                market_total += 1
                if len(kept) < dials["market_size"]:
                    kept.append({**r, **h, "family_key": fam})

            market_total_capped = len(market_rows) >= _MARKET_FETCH_CEILING
            market_final_ueis = [r["uei"] for r in kept]
            if market_final_ueis:
                fo = {r["uei"]: r for r in await run(
                    "market_farmout",
                    sql_market_farmout(market_final_ueis, top_naics, top_psc),
                    len(market_final_ueis) + 10)}
                sa = {r["uei"]: r for r in await run(
                    "market_setaside", sql_market_setaside(market_final_ueis),
                    len(market_final_ueis) + 10)}
                ds = {r["uei"]: r for r in await run(
                    "market_dealsize", sql_market_dealsize(market_final_ueis, top_naics),
                    len(market_final_ueis) + 10)}
                for r in kept:
                    mu = r["uei"]
                    f, s, d = fo.get(mu, {}), sa.get(mu, {}), ds.get(mu, {})
                    fo_amt = float(f.get("fo_amt_60mo") or 0)
                    fo_obl = float(f.get("prime_obl_60mo") or 0)
                    share_raw = (fo_amt / fo_obl) if fo_obl > 0 else None
                    market.append({
                        "uei": mu,
                        "name": r.get("legal_business_name"),
                        "state": r.get("physical_state"),
                        "family_key": r.get("family_key"),
                        "wt": round(float(r.get("wt") or 0), 4),
                        "lane_hits": r.get("lane_hits"),
                        "subout_5y": r.get("subout_5y"),
                        "last_sub_action": r.get("last_sub"),
                        "active_committed_value": r.get("committed_value"),
                        "active_committed_ct": r.get("committed_award_ct"),
                        "active_agency_ct": r.get("active_agency_ct"),
                        "next_expiry": r.get("next_committed_end_date"),
                        "farmout_in_shape": {
                            "amt_60mo": fo_amt,
                            "share_60mo_raw": share_raw,
                            "share_60mo_display": (min(share_raw, 1.0)
                                                    if share_raw is not None else None),
                        },
                        "sub_deal": {
                            "n": d.get("n") or 0,
                            "median": d.get("median_deal"),
                            "avg": d.get("avg_deal"),
                        },
                        "set_aside_won_fy23_25": {
                            "any": float(s.get("sa_any") or 0),
                            "8a": float(s.get("sa_8a") or 0),
                            "sdvosb": float(s.get("sa_sdvosb") or 0),
                            "wosb": float(s.get("sa_wosb") or 0),
                            "hubzone": float(s.get("sa_hubzone") or 0),
                        },
                    })
        if not market:
            market_reason = "no_market"

        # ---- hybrid: recipient-shape sub-out hydration --------------------
        # Displayed per-code (clean dollars); never summed across shape codes
        # (one subaward edge can satisfy several codes).
        if archetype == "prime_sub" and market and shape_naics:
            shp: dict[str, list[dict[str, Any]]] = {}
            for r in await run(
                    "market_shape_subout",
                    sql_market_shape_subout([m["uei"] for m in market], shape_naics),
                    _SIG_ROW_LIMIT):
                shp.setdefault(r["uei"], []).append(r)
            for m in market:
                rows = sorted(shp.get(m["uei"], []),
                              key=lambda r: -(float(r.get("amt") or 0)))
                m["subout_to_your_shape"] = {
                    "matched_code_ct": len(rows),
                    "top_code": rows[0]["recipient_code"] if rows else None,
                    "top_code_amt": float(rows[0].get("amt") or 0) if rows else 0.0,
                    "top_code_edge_ct": int(rows[0].get("edge_ct") or 0) if rows else 0,
                    "last_action": max((str(r.get("last_action")) for r in rows
                                        if r.get("last_action")), default=None),
                }

        # ---- closing: triangle + three-layer calculus ---------------------
        competitors: list[dict[str, Any]] = []
        closing_reason: str | None = None
        closing_stats: dict[str, Any] = {}
        if market:
            market_final_ueis = [m["uei"] for m in market]
            tri_rows = await run(
                "triangle",
                sql_triangle(uei, market_final_ueis, seeds, dials["peer_scoring_cap"]),
                _TRIANGLE_LIMIT)
            peers = [r["sub_uei"] for r in tri_rows if r.get("sub_uei")]

            if peers:
                pack: dict[str, dict[str, Any]] = {}
                band: dict[str, dict[str, Any]] = {}
                sig_rows_by_uei: dict[str, list[dict[str, Any]]] = {}
                declared: dict[str, set[str]] = {}
                for chunk in _chunks(peers, _PEER_CHUNK):
                    for r in await run("peer_pack", sql_peer_pack(chunk), len(chunk) + 10):
                        pack[r["uei"]] = r
                    for r in await run("peer_band", sql_peer_band(chunk), len(chunk) + 10):
                        band[r["uei"]] = r
                    for r in await run("peer_signature", sql_signature_rows(chunk),
                                       _SIG_ROW_LIMIT):
                        sig_rows_by_uei.setdefault(r["uei"], []).append(r)
                    for r in await run("peer_declared", sql_peer_declared(chunk),
                                       _SIG_ROW_LIMIT):
                        declared.setdefault(r["uei"], set()).add(
                            f"{r['code_type']}:{r['code']}")

                # The target's code universe for tiers 2/3: its own signature
                # when it primes, else its demonstrated sub lanes + declarations.
                target_code_set = ((set(target_vec) | target_sub_codes | target_decl)
                                   if target_has_signature
                                   else (target_sub_codes | target_decl))
                anchor_naics = sorted({c.split(":", 1)[1] for c in target_code_set
                                       if c.startswith("naics:")})[:16]
                anchor_psc = sorted({c.split(":", 1)[1] for c in target_code_set
                                     if c.startswith("psc:")})[:16]

                # peers with no signature — inferred reads, code-anchored [F1]
                b3_peers = [p for p in peers if p not in sig_rows_by_uei]
                inferred: dict[str, set[str]] = {}
                if b3_peers:
                    for ct, codes in (("naics", anchor_naics), ("psc", anchor_psc)):
                        if not codes:
                            continue
                        for chunk in _chunks(b3_peers, _PEER_CHUNK):
                            for r in await run(
                                    "peer_inferred",
                                    sql_peer_inferred(chunk, ct, codes),
                                    _SIG_ROW_LIMIT):
                                inferred.setdefault(r["uei"], set()).add(
                                    f"{r['code_type']}:{r['code']}")

                scored: list[dict[str, Any]] = []
                for r in tri_rows:
                    pu = r["sub_uei"]
                    p = pack.get(pu, {})
                    name = p.get("legal_business_name") or r.get("sub_name")
                    fam = family_key(pu, p.get("ultimate_parent_uei"), name)
                    if fam == target_family:
                        continue                   # [F3]
                    peer_sig = sig_rows_by_uei.get(pu)
                    peer_decl = declared.get(pu, set())
                    peer_inf = inferred.get(pu, set())

                    if peer_sig and target_has_signature:
                        vec = {f"{s['code_type']}:{s['code']}":
                               float(s.get("share_lifetime") or 0) for s in peer_sig}
                        lens = cosine(target_vec, vec)
                        branch, tier = 1, "demonstrated"
                    elif peer_sig:
                        vec = {f"{s['code_type']}:{s['code']}":
                               float(s.get("share_lifetime") or 0) for s in peer_sig}
                        lens = peer_share_in_set(vec, target_sub_codes | target_decl)
                        branch, tier = 2, "demonstrated"
                    elif peer_decl or peer_inf:
                        lens = overlap_coefficient(peer_decl | peer_inf, target_code_set)
                        branch, tier = 3, "inferred"
                    else:
                        continue                   # [F1] insufficient — never a zero row

                    ub = ubiquity_weight(p.get("n_distinct_primes_lifetime"),
                                         dials["ubiquity_fn"])
                    shared_ct = int(r.get("shared_seed_primes") or 0)
                    score = shared_ct * lens * ub
                    b = band.get(pu, {})
                    peer_band_amt = float(b.get("sub_amt") or 0) + float(b.get("prime_won") or 0)
                    scored.append({
                        "uei": pu, "name": name, "family_key": fam,
                        "score": round(score, 5),
                        "evidence_tier": tier, "lens_branch": branch,
                        "lens_overlap": round(lens, 2),
                        "ubiquity": round(ub, 4),
                        "shared_seed_primes": shared_ct,
                        "market_primes_engaging": r.get("market_prime_ueis") or [],
                        "dollars_from_market_5y": r.get("dollars_from_market_5y"),
                        "last_action": r.get("last_action"),
                        "band_dollars_fy23_25": peer_band_amt,
                    })

                scored.sort(key=lambda x: (-x["score"], -(x["shared_seed_primes"]),
                                           x["uei"]))
                # family dedup among peers [F3]
                seen_fam: set[str] = set()
                deduped = []
                for s in scored:
                    if s["family_key"] in seen_fam:
                        continue
                    seen_fam.add(s["family_key"])
                    deduped.append(s)
                # layer 3 — size-band nix, DISPLAY ONLY, after scoring [ruled]
                displayed = [s for s in deduped
                             if s["band_dollars_fy23_25"] <= dials["size_cap"]]
                size_nixed = len(deduped) - len(displayed)
                competitors = displayed[:dials["competitor_limit"]]

                if competitors:
                    comp_ueis = [c["uei"] for c in competitors]
                    d24 = {r["uei"]: r for r in await run(
                        "competitor_dollars_24mo",
                        sql_competitor_dollars_24mo(market_final_ueis, comp_ueis),
                        len(comp_ueis) + 10)}
                    engaged_market_primes: set[str] = set()
                    total_24 = 0.0
                    cutoff = _months_ago_iso(dials["recency_months"])
                    for c in competitors:
                        row = d24.get(c["uei"], {})
                        c["dollars_from_market_24mo"] = float(row.get("amt_24mo") or 0)
                        total_24 += c["dollars_from_market_24mo"]
                        la = c.get("last_action")
                        if la and str(la) >= cutoff:
                            engaged_market_primes.update(c["market_primes_engaging"])
                    closing_stats = {
                        "market_primes_engaging_competitors": len(engaged_market_primes),
                        "reported_sub_dollars_to_competitors_24mo": round(total_24, 2),
                        "size_nixed_ct": size_nixed,
                    }
        if not competitors:
            closing_reason = "no_competitors" if market else "no_market"

        method_base["statement_count"] = statement_count
        return _payload(uei, ident, in_band, band_prime, band_sub, reality,
                        seed_payload, market or None, market_reason,
                        {"competitors": competitors, "stats": closing_stats}
                        if competitors else None,
                        closing_reason, target_family, method_base,
                        _below_floor(market, competitors, dials), artifact,
                        archetype, prime_business,
                        market_total=market_total,
                        market_total_capped=market_total_capped)


def _months_ago_iso(months: int) -> str:
    today = date.today()
    total = today.year * 12 + (today.month - 1) - int(months)
    return date(total // 12, total % 12 + 1, min(today.day, 28)).isoformat()


def _below_floor(market: list | None, competitors: list, dials: dict[str, Any]) -> list[str]:
    below = []
    if market is not None and len(market) < dials["min_market_rows"]:
        below.append("market")
    if len(competitors) < dials["min_competitors"]:
        below.append("closing")
    return below


_DISCLOSURES = [
    "Subaward figures are FSRS-reported subawards; reporting is subject to thresholds and compliance gaps. Reported subawards equal roughly 14% of prime obligation dollars in FY2024.",
    "FSRS subaward reporting typically completes several months in arrears; figures for the most recent months are incomplete.",
    "Employee-size and industry fields cover the bridged subset of registrants and may be absent.",
    "Corporate-family grouping uses the SAM hierarchy where available (partial coverage) with a name-based fallback.",
    "Joint-venture entities are excluded from the market table.",
    "Farm-out shares can exceed obligations on pass-through arrangements; displayed shares are capped at 100%.",
    "Inferred-tier comparisons use SAM-registered and co-occurrence-inferred codes; firms without sufficient records are excluded from the competitor table.",
    "The competitor table excludes firms whose combined FY2023-FY2025 prime and subaward dollars exceed the size cap (display filter; does not affect scoring).",
]

_HYBRID_DISCLOSURES = [
    "This dossier's work shape is taken from the firm's own prime award record (contracting-officer-coded NAICS/PSC); FSRS-reported subaward codes are inherited from the prime award and are not used to characterize the firm.",
    "Market rows are additionally gated on demonstrated sub-out to recipients whose own prime history sits in the firm's prime NAICS codes; 'subs out to your shape' dollars are reported for the single best-matching code and are never summed across codes (one subaward can satisfy several).",
]


def _payload(uei: str, ident: dict[str, Any], in_band: bool, band_prime: float,
             band_sub: float, reality: dict[str, Any],
             seed_payload: list[dict[str, Any]], market: list | None,
             market_reason: str | None, closing: dict[str, Any] | None,
             closing_reason: str | None, target_family: str,
             method: dict[str, Any], sections_below_floor: list[str],
             artifact: str | None, archetype: str = "sub",
             prime_business: dict[str, Any] | None = None,
             market_total: int = 0, market_total_capped: bool = False) -> dict[str, Any]:
    method = dict(method)
    method["sections_below_floor"] = sections_below_floor
    return {
        "version": "sub_dossier_v2",
        "archetype": archetype,
        "artifact": artifact,
        "target": {
            "uei": uei,
            "name": ident.get("legal_business_name"),
            "state": ident.get("physical_state"),
            "city": ident.get("physical_city"),
            "employee_size_range": ident.get("employee_size_range"),
            "industry": ident.get("industry"),
            "family_key": target_family,
            "ultimate_parent_name": ident.get("ultimate_parent_name"),
            "in_band": in_band,
            "prime_won_fy23_25": round(band_prime, 2),
            "sub_amt_fy23_25": round(band_sub, 2),
        },
        "prime_business": prime_business,
        "reality": reality,
        "seed_primes": seed_payload,
        "market": {"reason": market_reason, "primes": market or [],
                   "total": market_total, "shown": len(market or []),
                   "total_capped": market_total_capped},
        "closing": {
            "reason": closing_reason,
            "competitors": (closing or {}).get("competitors", []),
            "stats": (closing or {}).get("stats", {}),
        },
        "method": method,
    }


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post("/build", dependencies=[Depends(require_service_token)])
async def build(body: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(body, dict):
        raise _refuse("body must be an object")
    uei = _valid_uei(body.get("uei"))
    archetype = _valid_archetype(body.get("archetype"))
    dials = _merge_dials(body.get("dials"))
    t0 = time.monotonic()
    try:
        payload = await _build_dossier(uei, dials, archetype)
    except ArtifactMoved:
        logger.warning("sub-dossier build restarted once: artifact moved (uei=%s)", uei)
        payload = await _build_dossier(uei, dials, archetype)   # restart exactly once
    payload["method"]["build_wall_ms"] = round((time.monotonic() - t0) * 1000, 1)
    return payload


@router.get("/eligible", dependencies=[Depends(require_service_token)])
async def eligible(band_min: float = 1_000_000.0, band_max: float = 1e12,
                   limit: int = 100, archetype: str = "sub",
                   prime_floor: float = 1_000_000.0,
                   sub_floor: float = 1_000_000.0) -> dict[str, Any]:
    if not (0 <= band_min <= band_max <= 1e12):
        raise _refuse("band_min/band_max out of range")
    if not (1 <= limit <= 5000):
        raise _refuse("limit must be 1..5000")
    archetype = _valid_archetype(archetype)
    if not (0 <= prime_floor <= 1e12 and 0 <= sub_floor <= 1e12):
        raise _refuse("prime_floor/sub_floor out of range")
    pf, sf = (prime_floor, sub_floor) if archetype == "prime_sub" else (0.0, 0.0)
    async with httpx.AsyncClient(timeout=90.0) as client:
        payload = await _run_sidecar(
            client, sql_eligible(band_min, band_max, limit, pf, sf), limit + 10)
    rows = _rows_as_dicts(payload)
    out = []
    for r in rows:
        parent_big = float(r.get("parent_prime_obl_60mo") or 0) >= DEFAULT_DIALS["size_cap"]
        has_parent = bool(r.get("ultimate_parent_uei")) and r.get("ultimate_parent_uei") != r.get("uei")
        out.append({
            "uei": r.get("uei"),
            "name": r.get("legal_business_name"),
            "state": r.get("physical_state"),
            "sub_amt_fy23_25": r.get("sub_amt"),
            "prime_won_fy23_25": r.get("prime_won"),
            "distinct_primes_fy23_25": r.get("primes"),
            "n_seeds": r.get("n_seeds"),
            "max_seed_last_action": r.get("max_seed_last_action"),
            "family_flag": {
                "has_parent": has_parent,
                "ultimate_parent_name": r.get("ultimate_parent_name"),
                "parent_exceeds_size_cap": parent_big,
            },
        })
    return {
        "entities": out,
        "count": len(out),
        "band": {"min": band_min, "max": band_max, "archetype": archetype,
                 "prime_floor": pf, "sub_floor": sf},
        "elapsed_ms": payload.get("elapsed_ms"),
        "artifact": payload.get("artifact"),
    }
