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
    _cache["combos"] = combos
    _cache["occupations"] = occupations
    _cache["occupation_vocab"] = sorted(
        ({"token": t, "soc_count": len(set(c))} for t, c in occupations.items()),
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


@router.post("/active-awards-query", dependencies=[Depends(require_service_token)])
async def active_awards_query(body: dict[str, Any]) -> dict[str, Any]:
    grain = body.get("grain")
    if grain not in ("total", "single"):
        raise HTTPException(status_code=422, detail="grain must be 'total' or 'single'")

    mode = body.get("mode") or "active"
    if mode not in ("active", "won"):
        raise HTTPException(status_code=422, detail="mode must be 'active' or 'won'")
    window_days = body.get("window_days")
    if mode == "won":
        if window_days not in _WINDOWS_DAYS:
            raise HTTPException(
                status_code=422,
                detail=f"window_days required for mode 'won' — one of {sorted(_WINDOWS_DAYS)}",
            )
    elif window_days is not None:
        raise HTTPException(status_code=422, detail="window_days only applies to mode 'won'")

    state = body.get("state")
    if state is not None:
        state = str(state).strip().upper()
        if state not in _US_STATES:
            raise HTTPException(status_code=422, detail=f"unknown state '{state}'")

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
    if need is not None:
        need = str(need).strip().lower()
        # Equipment bucket tokens share the `need` slot (approved 2026-07-15: one
        # word, posited demand — "they aren't tearing down buildings by hand").
        need_bucket = _EQUIPMENT_BUCKETS.get(need)
        if need_bucket is None:
            await _load_vocab()
            need_socs = sorted(set(_cache.get("occupations", {}).get(need, [])))
            if not need_socs:
                raise HTTPException(
                    status_code=422,
                    detail=f"unknown need token '{need}' — not in the occupation or equipment vocabulary",
                )
    include_support = bool(body.get("include_support"))

    limit = min(int(body.get("limit") or 100), 1000)

    combo_pred = ""
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
        pairs = ",".join(f"('{n}','{p}')" for n, p in combos)  # codes are validated vocab, not user input
        combo_pred = f"AND (naics_code, product_or_service_code) IN ({pairs})"

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
    sql = f"""
WITH awd AS (
  SELECT recipient_uei, contract_award_unique_key, life_to_date_obligated
  FROM usaspending_fpds_prime_award_state s
  WHERE {base_pred} {combo_pred}{need_pred}
), located AS (
  SELECT a.* FROM awd a {state_join}
), agg AS (
  SELECT recipient_uei, SUM(life_to_date_obligated) AS total_obl,
         MAX(life_to_date_obligated) AS max_single, COUNT(*) AS award_ct
  FROM located GROUP BY 1 {having}
){ind_cte}
SELECT g.recipient_uei AS uei, e.legal_business_name, e.physical_city, e.physical_state,
       e.normalized_domain, ROUND(g.total_obl, 0) AS active_total_obl,
       ROUND(g.max_single, 0) AS active_max_single, g.award_ct AS active_award_ct
FROM agg g LEFT JOIN gtm_sam_entities e ON e.uei = g.recipient_uei {ind_join}
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
                  "min_amt": min_amt, "window_days": window_days, "hq_state": hq_state,
                  "industry": industry},
        "total": len(rows),
        "rows": rows,
        "elapsed_ms": payload.get("elapsed_ms"),
        "artifact": payload.get("artifact"),
    }
