"""active-awards-query — the Q1 canonical query shape, executed deterministically.

Q1 (approved 2026-07-15): "companies with active (total|single) awards
[to <job phrase>] [in <state>] [over $X]" — companies whose CURRENTLY-ACTIVE
prime awards in the job's combo set, PERFORMED in the state, sum to (total) or
include one award (single) over $X net obligations. Geo is ALWAYS place of
performance. Omitted slots widen: no job = all work; no state = anywhere; no $
= any amount.

Endpoints (service-token gated):
  GET  /api/v1/market/jtbd-vocab          → the canonical job-phrase vocabulary
                                            [{phrase, combo_count}] (opus-4.8-canonical)
  POST /api/v1/market/active-awards-query → run Q1
      {"grain": "total"|"single", "job_phrase"?: "to: …",
       "state"?: "CA", "min_amt"?: 15000000, "limit"?: 100}

Execution: job phrase → combo set (gtm.combo_job_to_be_done, HQX pg, cached
in-process 10 min) → one SQL statement against the query-sidecar
(usaspending_fpds_prime_award_state × usaspending_award_pop_centroids ×
gtm_sam_entities; the two award-key forms are the same string — verified
2026-07-15, 312 ms). Deterministic: no LLM, closed vocabulary, refuses unknown
phrases/states with the token named.
"""
from __future__ import annotations

import logging
import os
import re
import time
from typing import Any

import httpx
from fastapi import APIRouter, Depends, HTTPException

from ..db import get_db_connection
from ..service_token import require_service_token

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/market", tags=["active-awards-query"])

SIDECAR_URL = os.environ.get("QUERY_SIDECAR_URL", "https://query-sidecar-api.onrender.com")
CANONICAL_MODEL = "opus-4.8-canonical"

_US_STATES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL",
    "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT",
    "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI",
    "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", "DC", "PR",
    "GU", "VI", "AS", "MP",
}

# phrase -> [(naics, psc)] and the vocab listing, cached in-process.
_cache: dict[str, Any] = {"at": 0.0, "combos": {}, "vocab": []}
_CACHE_TTL_S = 600

_PHRASE_RE = re.compile(r"^to: [a-z0-9&/'\- ]{3,80}$")


async def _load_vocab() -> None:
    if time.monotonic() - _cache["at"] < _CACHE_TTL_S and _cache["combos"]:
        return
    combos: dict[str, list[tuple[str, str]]] = {}
    occupations: dict[str, list[str]] = {}
    equipment: dict[str, list[tuple[str, str]]] = {}
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT output_sentence, naics_code, psc_code FROM gtm.combo_job_to_be_done "
                "WHERE model_id = %s",
                (CANONICAL_MODEL,),
            )
            for phrase, naics, psc in await cur.fetchall():
                combos.setdefault(phrase, []).append((naics, psc))
            # Occupation tokens ("and need <token>") — title tokens map to their SOC
            # codes; group tokens expand to every code in the 2-digit major group.
            try:
                await cur.execute("SELECT soc_code, token, group_token FROM gtm.occupation_tokens")
                rows = await cur.fetchall()
                for soc, token, group_token in rows:
                    occupations.setdefault(token, []).append(soc)
                    if group_token:
                        occupations.setdefault(group_token, []).append(soc)
            except Exception:  # table not landed yet — occupations vocab empty
                await conn.rollback()
            # Granular equipment tokens ("and need <token>") — free-text equipment
            # nouns → the (naics, psc) combos whose work implies that gear. The generic
            # `equipment` token is EXCLUDED from the resolvable vocabulary (too broad —
            # thousands of combos); use one of the 5 equipment buckets instead.
            try:
                await cur.execute(
                    "SELECT token, naics_code, psc_code FROM gtm.equipment_tokens "
                    "WHERE token <> 'equipment'"
                )
                for token, naics, psc in await cur.fetchall():
                    equipment.setdefault(token, []).append((naics, psc))
            except Exception:  # table not landed yet — equipment vocab empty
                await conn.rollback()
    _cache["combos"] = combos
    _cache["occupations"] = occupations
    _cache["equipment"] = equipment
    # The typeahead completes occupations, the 5 equipment buckets, AND granular
    # equipment tokens off the SAME `occupations` array (shape {token, soc_count};
    # soc_count is an opaque completion weight — combo count for equipment tokens).
    # A token that is both a bucket and a granular token (e.g. "cranes") appears
    # once; resolution order at query time is bucket → occupation → granular.
    occ_counts: dict[str, int] = {t: len(set(c)) for t, c in occupations.items()}
    for t, pairs in equipment.items():
        occ_counts[t] = len(set(pairs))
    for t in _EQUIPMENT_BUCKETS:
        occ_counts.setdefault(t, len(set(equipment.get(t, []))))
    _cache["occupation_vocab"] = sorted(
        ({"token": t, "soc_count": n} for t, n in occ_counts.items()),
        key=lambda x: -x["soc_count"],
    )
    _cache["vocab"] = sorted(
        ({"phrase": p, "combo_count": len(c)} for p, c in combos.items()),
        key=lambda x: -x["combo_count"],
    )
    _cache["at"] = time.monotonic()


@router.get("/jtbd-vocab", dependencies=[Depends(require_service_token)])
async def jtbd_vocab() -> dict[str, Any]:
    await _load_vocab()
    return {"model_id": CANONICAL_MODEL, "phrases": _cache["vocab"],
            "occupations": _cache.get("occupation_vocab", [])}


# Industry vocabulary (approved 2026-07-15): "<industry> companies …" — a frozen
# name → NAICS-prefix-set table. Membership (operator-corrected 2026-07-15):
#   • entity HAS prime history → ≥10% share of PRIME-side lifetime $ in the set
#     (what they win outright is what they do). NEVER the sub-side lane codes —
#     those carry the PRIME award's combo, and a staffing firm subbing under an
#     aerospace prime is not an aerospace company.
#   • entity has NO prime history → their SAM-declared NAICS matches the set
#     (self-identity, never inherited).
# Lifetime for stable identity; 10% because the share distribution is bimodal
# (measured 2026-07-15: any→10% cuts incidental-work members; 10→50% barely moves).
# Equipment demand buckets (approved 2026-07-15) — natural names → the 5 canonical
# equipment_buckets on naics_psc_equipment_needs. Posited (LLM-inferred from the
# combo's work), not observed order sheets — disclosed on calls, kept sharp.
_EQUIPMENT_BUCKETS: dict[str, str] = {
    "earthmoving equipment": "heavy_earthmoving_civil",
    "cranes": "material_handling_cranes",
    "heavy haul trucks": "trucks_heavy_haul",
    "aerial lifts": "aerial_access",
    "power generation equipment": "industrial_power_support",
}

_INDUSTRY_SHARE_MIN = 0.10
_INDUSTRIES: dict[str, list[str]] = {
    "construction": ["23"],
    "engineering": ["5413"],
    "environmental": ["562"],
    "facilities": ["5612", "5617"],
    "janitorial": ["56172"],
    "landscaping": ["56173"],
    "security": ["5616"],
    "real estate": ["531"],
    "logistics": ["48", "49"],
    "trucking": ["484"],
    "aerospace": ["3364"],
    "shipbuilding": ["3366"],
    "defense": ["3364", "3366", "3369", "33299", "3345"],
    "manufacturing": ["31", "32", "33"],
    "electronics": ["334"],
    "machinery": ["333"],
    "chemical": ["325"],
    "pharmaceutical": ["3254"],
    "energy": ["22", "211", "213"],
    "mining": ["212"],
    "it": ["5415"],
    "software": ["5112", "5415"],
    "telecom": ["517"],
    "consulting": ["5416"],
    "accounting": ["5412"],
    "legal": ["5411"],
    "healthcare": ["62"],
    "staffing": ["5613"],
    "financial": ["52"],
    "agriculture": ["11"],
}

# Q2 (approved 2026-07-15): "companies that have won (total|single) awards …
# in the last <window>" — awards FIRST AWARDED within the window (event, not
# status; an already-completed recent win still counts). $ = the award's full
# life-to-date obligations. Fixed window vocabulary only.
_WINDOWS_DAYS = {30, 45, 60, 90, 180, 365, 730, 1095}

# Q3 (approved 2026-07-15): "companies that <event verb> … in the last <window>"
# — FPDS modification EVENTS on active awards, keyed by action_type_code. Unlike
# Q1/Q2 there is NO total/single grain marker: the $ threshold is the per-company
# Σ obligations ON THOSE ACTIONS within the window (the money the events moved).
# Window REQUIRED. Verbs map to action_type_code sets; two are rollups. Served by
# the sidecar month-rollup mart `txn_recipient_month_by_type` (uei × action_type_code
# × naics × psc × agency × month). When `state` is passed the aggregation switches to
# the PoP-dimensioned mart `txn_recipient_month_pop` (uei × action_type_code × pop_state
# × pop_county × month) filtered on pop_state — closing the previously-disclosed 422.
# That mart carries NO naics/psc, so `in <state>` cannot combine with job_phrase/need
# (combo grain) — that combination is refused (422).
_EVENT_VERBS: dict[str, list[str]] = {
    "picked up additional work": ["A"],
    "had work added in scope": ["B"],
    "received new funding": ["C"],
    "got change orders": ["D"],
    "had change orders priced": ["L"],
    "had options exercised": ["G"],
    "were terminated for default": ["E"],
    "were terminated for convenience": ["F"],
    "were novated": ["J"],
    "picked up more work": ["A", "B", "G", "D", "L"],  # rollup
    "were terminated": ["E", "F"],  # rollup
}

# Q4 STEP-GROWTH (approved 2026-07-15): "companies whose prime obligations grew
# {2x|3x|4x|5x|10x} in the last {12 months vs the prior 24 | 6 vs prior 12 | 90 days
# vs prior 90}". Per company, Σ obligations in window A ≥ N × Σ in the immediately-
# preceding window B, AND Σ B > 0 (new entrants with zero prior excluded). Measure is
# ALL prime obligation actions (no action-type filter). Served by the month-rollup mart
# `gtm_txn_recipient_month_rollup`. When `state` is passed the aggregation switches to
# the PoP-dimensioned mart `txn_recipient_month_pop` (summing obligation across ALL
# action types within pop_state) — closing the previously-disclosed 422. That mart
# carries no naics/psc, so `in <state>` cannot combine with job_phrase/need (refused
# 422). No total/single grain, no percent.
_MULTIPLIERS = {2, 3, 4, 5, 10}
# window_pair → (a_days, b_days): A is the recent window, B the immediately-preceding.
_WINDOW_PAIRS: dict[str, tuple[int, int]] = {
    "12v24": (365, 730),
    "6v12": (180, 365),
    "90v90": (90, 90),
}

# Billing arrangement families (approved 2026-07-16) — body param `billing` ∈
# {"fixed price","cost plus","time and materials"}, Q1/Q2 (active/won) ONLY.
# Award-latest-state pricing on award_plan_state.latest_pricing_code, joined by
# contract_award_unique_key. Sets derived deterministically from fpds_code_vocab
# (field='pricing') official descriptions, verified against the live sidecar
# 2026-07-16 (award_plan_state stores single-letter codes for pricing):
#   fixed price — official description CONTAINS "FIXED PRICE":
#       A  FIXED PRICE REDETERMINATION
#       B  FIXED PRICE LEVEL OF EFFORT
#       J  FIRM FIXED PRICE
#       K  FIXED PRICE WITH ECONOMIC PRICE ADJUSTMENT
#       L  FIXED PRICE INCENTIVE
#       M  FIXED PRICE AWARD FEE
#   cost plus — description CONTAINS "COST PLUS" (S COST NO FEE and T COST SHARING
#               are cost-reimbursement but NOT cost-plus → deliberately excluded):
#       R  COST PLUS AWARD FEE
#       U  COST PLUS FIXED FEE
#       V  COST PLUS INCENTIVE FEE
#   time and materials — "TIME AND MATERIALS" + "LABOR HOUR":
#       Y  TIME AND MATERIALS
#       Z  LABOR HOURS
_BILLING_PRICING_CODES: dict[str, frozenset[str]] = {
    "fixed price": frozenset({"A", "B", "J", "K", "L", "M"}),
    "cost plus": frozenset({"R", "U", "V"}),
    "time and materials": frozenset({"Y", "Z"}),
}

# Financing arrangement (approved 2026-07-16) — body param `financing` ∈
# {"with progress payments","without progress payments"}, Q1/Q2 ONLY. Award-latest-
# state financing on award_plan_state.latest_financing_code. FPDS stores this field
# as BOTH single-letter codes AND the spelled-out description; both forms appear
# live in award_plan_state (verified 2026-07-16), so the progress set carries both.
# Progress-payment codes = official description CONTAINS "PROGRESS PAYMENTS"
# (fpds_code_vocab field='financing'):
#       A  / "FAR 52.232-16 PROGRESS PAYMENTS"
#       C  / "PERCENTAGE OF COMPLETION PROGRESS PAYMENTS"
#       D  / "UNUSUAL PROGRESS PAYMENTS OR ADVANCE PAYMENTS"
# (E/"COMMERCIAL FINANCING", F/"PERFORMANCE-BASED FINANCING", Z/"NOT APPLICABLE"
#  are financing arrangements but NOT progress payments → the "without" complement.)
# NULL decision (verified 2026-07-16): latest_financing_code is NULL on ~32M award-
# state rows (financing field not reported) and "NOT APPLICABLE"/Z on ~49M more. A
# NULL means no financing arrangement was reported → it is "without progress
# payments". So "without" = plan row whose financing_code IS NULL OR is not one of
# the progress-payment codes; "with" = plan row whose financing_code IS one of them.
_FINANCING_PROGRESS_CODES: frozenset[str] = frozenset({
    "A", "C", "D",
    "FAR 52.232-16 PROGRESS PAYMENTS",
    "PERCENTAGE OF COMPLETION PROGRESS PAYMENTS",
    "UNUSUAL PROGRESS PAYMENTS OR ADVANCE PAYMENTS",
})
_FINANCING_TOKENS = frozenset({"with progress payments", "without progress payments"})


@router.post("/active-awards-query", dependencies=[Depends(require_service_token)])
async def active_awards_query(body: dict[str, Any]) -> dict[str, Any]:
    mode = body.get("mode") or "active"
    if mode not in ("active", "won", "events", "growth"):
        raise HTTPException(
            status_code=422, detail="mode must be 'active', 'won', 'events' or 'growth'"
        )

    # Event verbs (Q3) and step-growth (Q4) carry NO total/single grain marker. Q1/Q2
    # require the grain marker; growth's measure is the window A vs B obligation ratio.
    grain = body.get("grain")
    if mode in ("events", "growth"):
        if grain is not None:
            raise HTTPException(
                status_code=422,
                detail=f"grain does not apply to mode '{mode}' (no total/single grain)",
            )
    elif grain not in ("total", "single"):
        raise HTTPException(status_code=422, detail="grain must be 'total' or 'single'")

    # Q4 step-growth params — multiplier (fixed set) and window_pair (fixed set).
    multiplier = body.get("multiplier")
    window_pair = body.get("window_pair")
    if mode == "growth":
        if multiplier not in _MULTIPLIERS:
            raise HTTPException(
                status_code=422,
                detail=f"multiplier required for mode 'growth' — one of {sorted(_MULTIPLIERS)}",
            )
        if window_pair not in _WINDOW_PAIRS:
            raise HTTPException(
                status_code=422,
                detail=f"window_pair required for mode 'growth' — one of {sorted(_WINDOW_PAIRS)}",
            )
    else:
        if multiplier is not None:
            raise HTTPException(status_code=422, detail="multiplier only applies to mode 'growth'")
        if window_pair is not None:
            raise HTTPException(status_code=422, detail="window_pair only applies to mode 'growth'")

    event_verb = body.get("event_verb")
    if mode == "events":
        if event_verb not in _EVENT_VERBS:
            raise HTTPException(
                status_code=422,
                detail=f"unknown event_verb '{event_verb}' — one of {sorted(_EVENT_VERBS)}",
            )
    elif event_verb is not None:
        raise HTTPException(status_code=422, detail="event_verb only applies to mode 'events'")

    window_days = body.get("window_days")
    if mode in ("won", "events"):
        if window_days not in _WINDOWS_DAYS:
            raise HTTPException(
                status_code=422,
                detail=f"window_days required for mode '{mode}' — one of {sorted(_WINDOWS_DAYS)}",
            )
    elif window_days is not None:
        raise HTTPException(status_code=422, detail="window_days only applies to mode 'won'/'events'")

    state = body.get("state")
    if state is not None:
        state = str(state).strip().upper()
        if state not in _US_STATES:
            raise HTTPException(status_code=422, detail=f"unknown state '{state}'")
        # events/growth + state → served by the PoP-dimensioned mart (below). The
        # combo-grain guard (state cannot combine with job_phrase/need on those modes)
        # is enforced after job/need resolution.

    billing = body.get("billing")
    if billing is not None:
        billing = str(billing).strip().lower()
        if billing not in _BILLING_PRICING_CODES:
            raise HTTPException(
                status_code=422,
                detail=f"unknown billing '{billing}' — one of {sorted(_BILLING_PRICING_CODES)}",
            )
        if mode in ("events", "growth"):
            # Pricing is award-latest-state (award grain); events/growth are transaction
            # grain — the two do not compose. Billing rides Q1 (active) / Q2 (won) only.
            raise HTTPException(
                status_code=422,
                detail=f"billing not supported on mode '{mode}' — pricing is award-latest-state (award grain), not transaction grain",
            )

    financing = body.get("financing")
    if financing is not None:
        financing = str(financing).strip().lower()
        if financing not in _FINANCING_TOKENS:
            raise HTTPException(
                status_code=422,
                detail=f"unknown financing '{financing}' — one of {sorted(_FINANCING_TOKENS)}",
            )
        if mode in ("events", "growth"):
            raise HTTPException(
                status_code=422,
                detail=f"financing not supported on mode '{mode}' — financing is award-latest-state (award grain), not transaction grain",
            )

    min_amt = body.get("min_amt")
    if min_amt is not None:
        try:
            min_amt = float(min_amt)
        except (TypeError, ValueError):
            raise HTTPException(status_code=422, detail="min_amt must be a number")

    hq_state = body.get("hq_state")
    if hq_state is not None:
        hq_state = str(hq_state).strip().upper()
        if hq_state not in _US_STATES:
            raise HTTPException(status_code=422, detail=f"unknown hq_state '{hq_state}'")

    industry = body.get("industry")
    if industry is not None:
        industry = str(industry).strip().lower()
        if industry not in _INDUSTRIES:
            raise HTTPException(
                status_code=422,
                detail=f"unknown industry '{industry}' — not in the industry vocabulary",
            )

    # "and need <occupation>" (approved 2026-07-15): keep companies whose QUALIFYING
    # awards' combos require the occupation. Work-scoping invariant applies — evaluated
    # on the same award slice the sentence selected. Default core-deliverable roles only
    # (support roles ride nearly every combo and would make 'need' non-discriminating).
    need = body.get("need")
    need_socs: list[str] = []
    need_bucket: str | None = None
    # Granular equipment token → its combo pairs, inlined into the qualifying-award
    # predicate (Q1/Q2 use the `s`-aliased FPDS table; events uses `t`).
    need_equip_pred = ""
    need_equip_pred_ev = ""
    if need is not None:
        need = str(need).strip().lower()
        # Resolution order (approved 2026-07-15): equipment bucket → occupation →
        # granular equipment token. Equipment tokens share the `need` slot (one word,
        # posited demand — "they aren't tearing down buildings by hand").
        need_bucket = _EQUIPMENT_BUCKETS.get(need)
        if need_bucket is None:
            await _load_vocab()
            need_socs = sorted(set(_cache.get("occupations", {}).get(need, [])))
            if not need_socs:
                equip_combos = _cache.get("equipment", {}).get(need)
                if not equip_combos:
                    raise HTTPException(
                        status_code=422,
                        detail=f"unknown need token '{need}' — not in the occupation or equipment vocabulary",
                    )
                if len(equip_combos) > 2000:
                    raise HTTPException(
                        status_code=422,
                        detail=f"token '{need}' too broad — use a bucket",
                    )
                pairs = ",".join(f"('{n}','{p}')" for n, p in equip_combos)  # server-side vocab, not user input
                need_equip_pred = f" AND (s.naics_code, s.product_or_service_code) IN ({pairs})"
                need_equip_pred_ev = f" AND (t.naics_code, t.psc_code) IN ({pairs})"
    include_support = bool(body.get("include_support"))

    limit = min(int(body.get("limit") or 100), 1000)

    combo_pred = ""
    combo_pairs = ""  # reused by events mode against t.naics_code/t.psc_code
    job_phrase = body.get("job_phrase")
    if job_phrase is not None:
        job_phrase = str(job_phrase).strip().lower()
        if not _PHRASE_RE.match(job_phrase):
            raise HTTPException(status_code=422, detail="job_phrase malformed")
        await _load_vocab()
        combos = _cache["combos"].get(job_phrase)
        if not combos:
            raise HTTPException(
                status_code=422,
                detail=f"unknown job phrase '{job_phrase}' — not in the canonical vocabulary",
            )
        combo_pairs = ",".join(f"('{n}','{p}')" for n, p in combos)  # codes are validated vocab, not user input
        combo_pred = f"AND (naics_code, product_or_service_code) IN ({combo_pairs})"

    # events/growth + state + job/need COMPOSE since the 2026-07-16 billing-latency
    # cycle widened txn_recipient_month_pop with naics_code/psc_code — the historical
    # refusal ("PoP mart carries no combo grain") is retired.

    # ── Q4 step-growth: a self-contained SQL path against the month-rollup mart. ──
    if mode == "growth":
        a_days, b_days = _WINDOW_PAIRS[window_pair]
        span = a_days + b_days
        # PoP CAPABILITY: `in <state>` sums obligation across ALL action types within
        # the PoP state on txn_recipient_month_pop (combo-grain since 2026-07-16 —
        # job/need preds compose below). Without state, the non-PoP rollup.
        if state:
            gr_fact = "txn_recipient_month_pop"
            gr_state_pred = f" AND t.pop_state = '{state}'"
        else:
            gr_fact = "gtm_txn_recipient_month_rollup"
            gr_state_pred = ""
        gr_combo_pred = (
            f" AND (t.naics_code, t.psc_code) IN ({combo_pairs})" if combo_pairs else ""
        )
        gr_need_pred = ""
        if need_bucket:
            gr_need_pred = (
                f" AND EXISTS (SELECT 1 FROM naics_psc_equipment_needs eq "
                f"WHERE eq.naics_code = t.naics_code AND eq.psc_code = t.psc_code "
                f"AND eq.in_scope AND list_contains(eq.equipment_buckets, '{need_bucket}'))"
            )
        elif need_socs:
            socs = ",".join(f"'{s}'" for s in need_socs)  # codes come from the server-side vocab
            role_pred = "" if include_support else " AND lc.role_class = 'core_deliverable'"
            gr_need_pred = (
                f" AND EXISTS (SELECT 1 FROM naics_psc_labor_profile_categories lc "
                f"WHERE lc.naics_code = t.naics_code AND lc.psc_code = t.psc_code "
                f"AND lc.soc_code IN ({socs}){role_pred})"
            )
        entity_preds = []
        if hq_state:
            entity_preds.append(f"e.physical_state = '{hq_state}'")
        ind_cte = ""
        ind_join = ""
        if industry:
            prefixes = _INDUSTRIES[industry]
            in_set = " OR ".join(f"code LIKE '{p}%'" for p in prefixes)
            declared = " OR ".join(f"e.primary_naics LIKE '{p}%'" for p in prefixes)
            ind_cte = f""", ind AS (
  SELECT uei, SUM(CASE WHEN {in_set} THEN obl_lifetime ELSE 0 END) AS in_set_obl,
         SUM(obl_lifetime) AS tot_obl
  FROM gtm_entity_code_lanes WHERE side = 'prime' AND code_type = 'naics' GROUP BY 1
)"""
            ind_join = "LEFT JOIN ind ip ON ip.uei = g.uei"
            entity_preds.append(
                f"((ip.tot_obl > 0 AND ip.in_set_obl / ip.tot_obl >= {_INDUSTRY_SHARE_MIN}) "
                f"OR (COALESCE(ip.tot_obl, 0) = 0 AND ({declared})))"
            )
        entity_where = ("WHERE " + " AND ".join(entity_preds)) if entity_preds else ""
        sql = f"""
WITH g AS (
  SELECT t.uei,
         SUM(CASE WHEN t.month >= CURRENT_DATE - INTERVAL {a_days} DAY
                  THEN t.obligation_sum ELSE 0 END) AS a_obl,
         SUM(CASE WHEN t.month < CURRENT_DATE - INTERVAL {a_days} DAY
                   AND t.month >= CURRENT_DATE - INTERVAL {span} DAY
                  THEN t.obligation_sum ELSE 0 END) AS b_obl
  FROM {gr_fact} t
  WHERE t.month >= CURRENT_DATE - INTERVAL {span} DAY{gr_state_pred}{gr_combo_pred}{gr_need_pred}{need_equip_pred_ev}
  GROUP BY 1
  HAVING b_obl > 0 AND a_obl >= {int(multiplier)} * b_obl
){ind_cte}
SELECT g.uei AS uei, e.legal_business_name, e.physical_city, e.physical_state,
       e.normalized_domain, ROUND(g.a_obl, 0) AS window_obl, ROUND(g.b_obl, 0) AS prior_obl,
       ROUND(g.a_obl / g.b_obl, 1) AS growth_ratio,
       geo.latitude, geo.longitude,
       COUNT(*) OVER () AS total_count
FROM g LEFT JOIN gtm_sam_entities e ON e.uei = g.uei
LEFT JOIN gtm_entity_geo geo ON geo.uei = g.uei {ind_join}
{entity_where}
ORDER BY g.a_obl - g.b_obl DESC
LIMIT {limit}
"""
        token = os.environ.get("QUERY_SIDECAR_TOKEN")
        if not token:
            raise HTTPException(status_code=503, detail="QUERY_SIDECAR_TOKEN not configured")
        async with httpx.AsyncClient(timeout=60.0) as client:
            r = await client.post(
                f"{SIDECAR_URL}/api/v1/sql",
                headers={"Authorization": f"Bearer {token}"},
                json={"sql": sql, "limit": limit},
            )
        if r.status_code != 200:
            logger.error("sidecar growth query failed: %s %s", r.status_code, r.text[:300])
            raise HTTPException(status_code=502, detail="sidecar query failed")
        payload = r.json()
        cols = payload["columns"]
        rows = [dict(zip(cols, row)) for row in payload["rows"]]
        return {
            "query": {
                "mode": mode, "multiplier": multiplier, "window_pair": window_pair,
                "job_phrase": job_phrase, "state": state, "hq_state": hq_state,
                "industry": industry, "need": need, "include_support": include_support,
            },
            "total": (rows[0].get("total_count") if rows else 0) or len(rows),
            "rows": rows,
            "elapsed_ms": payload.get("elapsed_ms"),
            "artifact": payload.get("artifact"),
        }

    # ── Q3 event verbs: a self-contained SQL path against the month-rollup mart. ──
    if mode == "events":
        codes_sql = ",".join(f"'{c}'" for c in _EVENT_VERBS[event_verb])
        # PoP CAPABILITY: `in <state>` filters the same action-code set on the PoP-
        # dimensioned mart txn_recipient_month_pop by pop_state (combo-grain since
        # 2026-07-16 — job/need preds compose below). Without state, the by-type rollup.
        if state:
            ev_fact = "txn_recipient_month_pop"
            ev_state_pred = f" AND t.pop_state = '{state}'"
        else:
            ev_fact = "txn_recipient_month_by_type"
            ev_state_pred = ""
        ev_combo_pred = (
            f" AND (t.naics_code, t.psc_code) IN ({combo_pairs})" if combo_pairs else ""
        )
        ev_need_pred = ""
        if need_bucket:
            ev_need_pred = (
                f" AND EXISTS (SELECT 1 FROM naics_psc_equipment_needs eq "
                f"WHERE eq.naics_code = t.naics_code AND eq.psc_code = t.psc_code "
                f"AND eq.in_scope AND list_contains(eq.equipment_buckets, '{need_bucket}'))"
            )
        elif need_socs:
            socs = ",".join(f"'{s}'" for s in need_socs)  # codes come from the server-side vocab
            role_pred = "" if include_support else " AND lc.role_class = 'core_deliverable'"
            ev_need_pred = (
                f" AND EXISTS (SELECT 1 FROM naics_psc_labor_profile_categories lc "
                f"WHERE lc.naics_code = t.naics_code AND lc.psc_code = t.psc_code "
                f"AND lc.soc_code IN ({socs}){role_pred})"
            )
        ev_having = f"HAVING SUM(t.obligation_sum) > {min_amt}" if min_amt is not None else ""
        entity_preds = []
        if hq_state:
            entity_preds.append(f"e.physical_state = '{hq_state}'")
        ind_cte = ""
        ind_join = ""
        if industry:
            prefixes = _INDUSTRIES[industry]
            in_set = " OR ".join(f"code LIKE '{p}%'" for p in prefixes)
            declared = " OR ".join(f"e.primary_naics LIKE '{p}%'" for p in prefixes)
            ind_cte = f""", ind AS (
  SELECT uei, SUM(CASE WHEN {in_set} THEN obl_lifetime ELSE 0 END) AS in_set_obl,
         SUM(obl_lifetime) AS tot_obl
  FROM gtm_entity_code_lanes WHERE side = 'prime' AND code_type = 'naics' GROUP BY 1
)"""
            ind_join = "LEFT JOIN ind ip ON ip.uei = g.uei"
            entity_preds.append(
                f"((ip.tot_obl > 0 AND ip.in_set_obl / ip.tot_obl >= {_INDUSTRY_SHARE_MIN}) "
                f"OR (COALESCE(ip.tot_obl, 0) = 0 AND ({declared})))"
            )
        entity_where = ("WHERE " + " AND ".join(entity_preds)) if entity_preds else ""
        sql = f"""
WITH ev AS (
  SELECT t.uei, SUM(t.obligation_sum) AS event_obl, SUM(t.n_actions) AS event_actions
  FROM {ev_fact} t
  WHERE t.action_type_code IN ({codes_sql})
    AND t.month >= CURRENT_DATE - INTERVAL {int(window_days)} DAY{ev_state_pred}{ev_combo_pred}{ev_need_pred}{need_equip_pred_ev}
  GROUP BY 1 {ev_having}
){ind_cte}
SELECT g.uei AS uei, e.legal_business_name, e.physical_city, e.physical_state,
       e.normalized_domain, ROUND(g.event_obl, 0) AS event_obl, g.event_actions AS event_actions,
       geo.latitude, geo.longitude,
       COUNT(*) OVER () AS total_count
FROM ev g LEFT JOIN gtm_sam_entities e ON e.uei = g.uei
LEFT JOIN gtm_entity_geo geo ON geo.uei = g.uei {ind_join}
{entity_where}
ORDER BY g.event_obl DESC
LIMIT {limit}
"""
        token = os.environ.get("QUERY_SIDECAR_TOKEN")
        if not token:
            raise HTTPException(status_code=503, detail="QUERY_SIDECAR_TOKEN not configured")
        async with httpx.AsyncClient(timeout=60.0) as client:
            r = await client.post(
                f"{SIDECAR_URL}/api/v1/sql",
                headers={"Authorization": f"Bearer {token}"},
                json={"sql": sql, "limit": limit},
            )
        if r.status_code != 200:
            logger.error("sidecar events query failed: %s %s", r.status_code, r.text[:300])
            raise HTTPException(status_code=502, detail="sidecar query failed")
        payload = r.json()
        cols = payload["columns"]
        rows = [dict(zip(cols, row)) for row in payload["rows"]]
        return {
            "query": {
                "mode": mode, "event_verb": event_verb, "job_phrase": job_phrase,
                "state": state, "min_amt": min_amt, "window_days": window_days,
                "hq_state": hq_state, "industry": industry, "need": need,
                "include_support": include_support,
            },
            "total": (rows[0].get("total_count") if rows else 0) or len(rows),
            "rows": rows,
            "elapsed_ms": payload.get("elapsed_ms"),
            "artifact": payload.get("artifact"),
        }

    state_join = (
        "JOIN usaspending_award_pop_centroids c "
        "ON c.generated_unique_award_id = a.contract_award_unique_key "
        f"AND c.state_code = '{state}'"
        if state
        else ""
    )
    measure = "SUM(life_to_date_obligated)" if grain == "total" else "MAX(life_to_date_obligated)"
    having = f"HAVING {measure} > {min_amt}" if min_amt is not None else ""

    base_pred = (
        "current_end_date >= CURRENT_DATE AND is_terminated = FALSE"
        if mode == "active"
        else f"first_action_date >= CURRENT_DATE - INTERVAL {int(window_days)} DAY"
    )
    entity_preds = []
    if hq_state:
        entity_preds.append(f"e.physical_state = '{hq_state}'")
    ind_cte = ""
    ind_join = ""
    if industry:
        prefixes = _INDUSTRIES[industry]
        in_set = " OR ".join(f"code LIKE '{p}%'" for p in prefixes)
        declared = " OR ".join(f"e.primary_naics LIKE '{p}%'" for p in prefixes)
        ind_cte = f""", ind AS (
  SELECT uei, SUM(CASE WHEN {in_set} THEN obl_lifetime ELSE 0 END) AS in_set_obl,
         SUM(obl_lifetime) AS tot_obl
  FROM gtm_entity_code_lanes WHERE side = 'prime' AND code_type = 'naics' GROUP BY 1
)"""
        ind_join = "LEFT JOIN ind ip ON ip.uei = g.recipient_uei"
        entity_preds.append(
            f"((ip.tot_obl > 0 AND ip.in_set_obl / ip.tot_obl >= {_INDUSTRY_SHARE_MIN}) "
            f"OR (COALESCE(ip.tot_obl, 0) = 0 AND ({declared})))"
        )
    entity_where = ("WHERE " + " AND ".join(entity_preds)) if entity_preds else ""
    need_pred = ""
    if need_bucket:
        need_pred = (
            f" AND EXISTS (SELECT 1 FROM naics_psc_equipment_needs eq "
            f"WHERE eq.naics_code = s.naics_code AND eq.psc_code = s.product_or_service_code "
            f"AND eq.in_scope AND list_contains(eq.equipment_buckets, '{need_bucket}'))"
        )
    elif need_socs:
        socs = ",".join(f"'{s}'" for s in need_socs)  # codes come from the server-side vocab
        role_pred = "" if include_support else " AND lc.role_class = 'core_deliverable'"
        # NOTE: outer table aliased `s` — an unqualified column inside EXISTS binds
        # to the inner table first (lc.naics_code = naics_code would be a tautology).
        need_pred = (
            f" AND EXISTS (SELECT 1 FROM naics_psc_labor_profile_categories lc "
            f"WHERE lc.naics_code = s.naics_code AND lc.psc_code = s.product_or_service_code "
            f"AND lc.soc_code IN ({socs}){role_pred})"
        )
    # Billing (pricing) + financing gate on award-latest-state — since the 2026-07-16
    # billing-latency cycle the plan-state columns (latest_pricing_code,
    # latest_financing_code) live DENORMALIZED on usaspending_fpds_prime_award_state
    # itself, so the gate is plain single-table predicates (the historical 83M×83M
    # query-time EXISTS join to award_plan_state ran ~45s and is retired — never
    # reintroduce it). Codes are the frozen server-side vocab above, never user input.
    # An award with no reported plan state has NULL latest_pricing_code and is excluded
    # by a billing IN-list (documented behaviour, unchanged).
    plan_pred = ""
    if billing is not None or financing is not None:
        plan_conds = []
        if billing is not None:
            codes = ",".join(f"'{c}'" for c in sorted(_BILLING_PRICING_CODES[billing]))
            plan_conds.append(f"s.latest_pricing_code IN ({codes})")
        if financing is not None:
            codes = ",".join(f"'{c}'" for c in sorted(_FINANCING_PROGRESS_CODES))
            if financing == "with progress payments":
                plan_conds.append(f"s.latest_financing_code IN ({codes})")
            else:  # "without progress payments": complement, NULL treated as without
                plan_conds.append(
                    f"(s.latest_financing_code IS NULL OR s.latest_financing_code NOT IN ({codes}))"
                )
        plan_pred = " AND " + " AND ".join(plan_conds)
    sql = f"""
WITH awd AS (
  SELECT recipient_uei, contract_award_unique_key, life_to_date_obligated
  FROM usaspending_fpds_prime_award_state s
  WHERE {base_pred} {combo_pred}{need_pred}{need_equip_pred}{plan_pred}
), located AS (
  SELECT a.* FROM awd a {state_join}
), agg AS (
  SELECT recipient_uei, SUM(life_to_date_obligated) AS total_obl,
         MAX(life_to_date_obligated) AS max_single, COUNT(*) AS award_ct
  FROM located GROUP BY 1 {having}
){ind_cte}
SELECT g.recipient_uei AS uei, e.legal_business_name, e.physical_city, e.physical_state,
       e.normalized_domain, ROUND(g.total_obl, 0) AS active_total_obl,
       ROUND(g.max_single, 0) AS active_max_single, g.award_ct AS active_award_ct,
       geo.latitude, geo.longitude,
       COUNT(*) OVER () AS total_count
FROM agg g LEFT JOIN gtm_sam_entities e ON e.uei = g.recipient_uei
LEFT JOIN gtm_entity_geo geo ON geo.uei = g.recipient_uei {ind_join}
{entity_where}
ORDER BY {"g.total_obl" if grain == "total" else "g.max_single"} DESC
LIMIT {limit}
"""
    token = os.environ.get("QUERY_SIDECAR_TOKEN")
    if not token:
        raise HTTPException(status_code=503, detail="QUERY_SIDECAR_TOKEN not configured")
    async with httpx.AsyncClient(timeout=60.0) as client:
        r = await client.post(
            f"{SIDECAR_URL}/api/v1/sql",
            headers={"Authorization": f"Bearer {token}"},
            json={"sql": sql, "limit": limit},
        )
    if r.status_code != 200:
        logger.error("sidecar query failed: %s %s", r.status_code, r.text[:300])
        raise HTTPException(status_code=502, detail="sidecar query failed")
    payload = r.json()
    cols = payload["columns"]
    rows = [dict(zip(cols, row)) for row in payload["rows"]]
    return {
        "query": {"mode": mode, "grain": grain, "job_phrase": job_phrase, "state": state,
                  "billing": billing, "financing": financing,
                  "min_amt": min_amt, "window_days": window_days, "hq_state": hq_state,
                  "industry": industry, "need": need, "include_support": include_support},
        "total": (rows[0].get("total_count") if rows else 0) or len(rows),
        "rows": rows,
        "elapsed_ms": payload.get("elapsed_ms"),
        "artifact": payload.get("artifact"),
    }
