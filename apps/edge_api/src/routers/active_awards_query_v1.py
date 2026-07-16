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
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT output_sentence, naics_code, psc_code FROM gtm.combo_job_to_be_done "
                "WHERE model_id = %s",
                (CANONICAL_MODEL,),
            )
            for phrase, naics, psc in await cur.fetchall():
                combos.setdefault(phrase, []).append((naics, psc))
    _cache["combos"] = combos
    _cache["vocab"] = sorted(
        ({"phrase": p, "combo_count": len(c)} for p, c in combos.items()),
        key=lambda x: -x["combo_count"],
    )
    _cache["at"] = time.monotonic()


@router.get("/jtbd-vocab", dependencies=[Depends(require_service_token)])
async def jtbd_vocab() -> dict[str, Any]:
    await _load_vocab()
    return {"model_id": CANONICAL_MODEL, "phrases": _cache["vocab"]}


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
    sql = f"""
WITH awd AS (
  SELECT recipient_uei, contract_award_unique_key, life_to_date_obligated
  FROM usaspending_fpds_prime_award_state
  WHERE {base_pred} {combo_pred}
), located AS (
  SELECT a.* FROM awd a {state_join}
), agg AS (
  SELECT recipient_uei, SUM(life_to_date_obligated) AS total_obl,
         MAX(life_to_date_obligated) AS max_single, COUNT(*) AS award_ct
  FROM located GROUP BY 1 {having}
)
SELECT g.recipient_uei AS uei, e.legal_business_name, e.physical_city, e.physical_state,
       e.normalized_domain, ROUND(g.total_obl, 0) AS active_total_obl,
       ROUND(g.max_single, 0) AS active_max_single, g.award_ct AS active_award_ct
FROM agg g LEFT JOIN gtm_sam_entities e ON e.uei = g.recipient_uei
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
                  "min_amt": min_amt, "window_days": window_days},
        "total": len(rows),
        "rows": rows,
        "elapsed_ms": payload.get("elapsed_ms"),
        "artifact": payload.get("artifact"),
    }
