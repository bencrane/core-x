"""market-collections — the durable market collections, servable.

The 22 pair-defined market collections (authority: hqx Postgres
``gtm.market_collections``; program record: hq/MARKET_COLLECTIONS_PROGRAM.md v2)
exposed to the platform's Market tab: list the collections, and count the firms
fitting a selected combination + tuning parameters, live against the
query-sidecar.

Definitions (fixed, matching the viewer's Bucket Explorer and the pg rows):
- Member = won FY23-25 (2022-10-01 -> 2025-09-30) prime obligations within the
  selected collections' (award-NAICS6, award-PSC4) pairs in [min, max)
  (max exclusive), AND >= 1 active prime award (current_end_date >= today AND
  NOT is_terminated) within those pairs.
- Collections combine by pair-set union; the sets are disjoint by construction
  (one pair, one home), so per-firm sums never double-count.
- "based_in" = HQ state (gtm_sam_entities.physical_state) — an entity attribute.
- "working_in" = the firm's CURRENT ACTIVE in-scope award(s) are performed in
  the given state(s) (operator ruling 2026-07-16: PoP relates to active awards,
  never to aggregate FY23-25 history). PoP comes from
  usaspending_award_pop_centroids joined on the award key; awards without a
  resolvable PoP state do not satisfy an affirmative working_in filter
  (~29% of active awards lack one — disclosed in the response).

Endpoints (service-token gated):
  GET  /api/v1/market-collections
    -> {"collections": [{slug, title, description, pair_count}], "as_of"}
  POST /api/v1/market-collections/count
    {"collections": ["equipment-heavy-construction", ...],   (required)
     "based_in"?:   ["TX", ...],
     "working_in"?: ["TX", ...],
     "min_won"?:    number,           (dollars; default 0)
     "max_won"?:    number}           (dollars; default uncapped; exclusive)
    -> {"count": N, "won_total": $, "won_median": $, "won_avg": $,
        "spec": <echo>, "elapsed_ms", "artifact"}

Execution: one deterministic sidecar statement — pairs VALUES join (explicit
lists straight from the pg rows, cached in-process 10 min), won CTE over
gtm_txn_events_slim, active CTE over usaspending_fpds_prime_award_state
(+ centroids when working_in), optional HQ-state semi-join. All SQL fragments
are server-side; user input is validated against closed vocabularies (slugs,
state codes) and numbers — never interpolated as text.
"""
from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

import httpx
from fastapi import APIRouter, Depends, HTTPException

from ..db import get_db_connection
from ..service_token import require_service_token

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/market-collections", tags=["market-collections"])

SIDECAR_URL = os.environ.get("QUERY_SIDECAR_URL", "https://query-sidecar-api.onrender.com")

_US_STATES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL",
    "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT",
    "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI",
    "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", "DC", "PR",
    "GU", "VI", "AS", "MP",
}

_WINDOW_START = "2022-10-01"
_WINDOW_END = "2025-09-30"

# slug -> {"title", "description", "pairs": [(naics, psc)]}; cached in-process.
_cache: dict[str, Any] = {"at": 0.0, "collections": {}}
_CACHE_TTL_S = 600


async def _load_collections() -> None:
    if time.monotonic() - _cache["at"] < _CACHE_TTL_S and _cache["collections"]:
        return
    collections: dict[str, dict[str, Any]] = {}
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT slug, title, description, params FROM gtm.market_collections "
                "WHERE status = 'active' ORDER BY title"
            )
            for slug, title, description, params in await cur.fetchall():
                if isinstance(params, str):  # psycopg may hand back text
                    params = json.loads(params)
                # explicit_pairs rows carry combo_pairs; the one naics_anchored row
                # (aerospace-defense-space-manufacturing) serves its observed-pair
                # expansion snapshot (>= $100K pairs; the dropped tail nets ~0).
                pairs = params.get("combo_pairs") or params.get("observed_pairs_ge_100k") or []
                clean = [
                    (str(n), str(p))
                    for n, p in pairs
                    if n and p and str(n).isalnum() and len(str(n)) == 6 and len(str(p)) == 4
                ]
                collections[slug] = {
                    "title": title,
                    "description": description,
                    "pairs": clean,
                }
    _cache["collections"] = collections
    _cache["at"] = time.monotonic()


def _refuse(detail: str) -> HTTPException:
    return HTTPException(status_code=422, detail=detail)


def _parse_states(value: Any, field: str) -> list[str] | None:
    if value is None:
        return None
    if not isinstance(value, list) or not value:
        raise _refuse(f"{field} must be a non-empty list of state codes (omit for All)")
    states = []
    for s in value:
        code = str(s).strip().upper()
        if code not in _US_STATES:
            raise _refuse(f"unknown state '{s}' in {field}")
        states.append(code)
    return sorted(set(states))


@router.get("", dependencies=[Depends(require_service_token)])
async def list_collections() -> dict[str, Any]:
    await _load_collections()
    return {
        "collections": [
            {
                "slug": slug,
                "title": c["title"],
                "description": c["description"],
                "pair_count": len(c["pairs"]),
            }
            for slug, c in sorted(_cache["collections"].items(), key=lambda kv: kv[1]["title"])
        ],
    }


@router.post("/count", dependencies=[Depends(require_service_token)])
async def count_market(body: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(body, dict):
        raise _refuse("body must be an object")
    await _load_collections()
    registry = _cache["collections"]

    slugs_in = body.get("collections")
    if not isinstance(slugs_in, list) or not slugs_in:
        raise _refuse("collections must be a non-empty list of collection slugs")
    slugs = []
    for s in slugs_in:
        slug = str(s)
        if slug not in registry:
            raise _refuse(f"unknown collection '{s}'")
        slugs.append(slug)
    slugs = sorted(set(slugs))

    pairs: set[tuple[str, str]] = set()
    for slug in slugs:
        pairs.update(registry[slug]["pairs"])
    if not pairs:
        raise _refuse("selected collections carry no pairs")

    based_in = _parse_states(body.get("based_in"), "based_in")
    working_in = _parse_states(body.get("working_in"), "working_in")

    min_won = body.get("min_won", 0)
    max_won = body.get("max_won")
    for name, v in (("min_won", min_won), ("max_won", max_won)):
        if v is not None and (not isinstance(v, (int, float)) or isinstance(v, bool) or v < 0):
            raise _refuse(f"{name} must be a non-negative number")

    values = ",".join(f"('{n}','{p}')" for n, p in sorted(pairs))

    act_join = ""
    act_pred = ""
    if working_in:
        in_list = ",".join(f"'{s}'" for s in working_in)
        act_join = (
            " JOIN usaspending_award_pop_centroids c"
            " ON c.generated_unique_award_id = a.contract_award_unique_key"
        )
        act_pred = f" AND c.state_code IN ({in_list})"

    won_preds = [f"w.won >= {float(min_won or 0)}"]
    if max_won is not None:
        won_preds.append(f"w.won < {float(max_won)}")

    hq_pred = ""
    if based_in:
        in_list = ",".join(f"'{s}'" for s in based_in)
        hq_pred = (
            " AND m.uei IN (SELECT uei FROM gtm_sam_entities"
            f" WHERE physical_state IN ({in_list}))"
        )

    sql = f"""
WITH pairs(naics_code, psc_code) AS (VALUES {values}),
won AS (
  SELECT t.uei, sum(t.obligation) AS won
  FROM gtm_txn_events_slim t JOIN pairs pr
    ON t.naics_code = pr.naics_code AND t.psc_code = pr.psc_code
  WHERE t.action_date BETWEEN DATE '{_WINDOW_START}' AND DATE '{_WINDOW_END}'
  GROUP BY 1
),
act AS (
  SELECT DISTINCT a.recipient_uei AS uei
  FROM usaspending_fpds_prime_award_state a
  JOIN pairs pr ON a.naics_code = pr.naics_code AND a.product_or_service_code = pr.psc_code
  {act_join}
  WHERE a.current_end_date >= current_date AND a.is_terminated = FALSE{act_pred}
),
m AS (
  SELECT w.uei, w.won FROM won w JOIN act USING (uei)
  WHERE {' AND '.join(won_preds)}
)
SELECT count(*) AS n, round(coalesce(sum(m.won), 0), 0) AS won_total,
       round(coalesce(median(m.won), 0), 0) AS won_median,
       round(coalesce(avg(m.won), 0), 0) AS won_avg
FROM m WHERE TRUE{hq_pred}
"""
    token = os.environ.get("QUERY_SIDECAR_TOKEN")
    if not token:
        raise HTTPException(status_code=503, detail="QUERY_SIDECAR_TOKEN not configured")
    async with httpx.AsyncClient(timeout=90.0) as client:
        r = await client.post(
            f"{SIDECAR_URL}/api/v1/sql",
            headers={"Authorization": f"Bearer {token}"},
            json={"sql": sql, "limit": 5},
        )
    if r.status_code != 200:
        logger.error("sidecar market-collections count failed: %s %s", r.status_code, r.text[:300])
        raise HTTPException(status_code=502, detail="sidecar query failed")
    payload = r.json()
    row = payload["rows"][0] if payload.get("rows") else [0, 0, 0, 0]
    return {
        "count": row[0],
        "won_total": row[1],
        "won_median": row[2],
        "won_avg": row[3],
        "spec": {
            "collections": slugs,
            "based_in": based_in,
            "working_in": working_in,
            "min_won": min_won or 0,
            "max_won": max_won,
            "window": f"FY23-25 ({_WINDOW_START} -> {_WINDOW_END})",
            "member": "won-in-band within the collections' pairs AND >= 1 active in-scope award"
            + (" whose place of performance is in working_in (awards without a resolvable PoP state do not qualify)" if working_in else ""),
        },
        "elapsed_ms": payload.get("elapsed_ms"),
        "artifact": payload.get("artifact"),
    }
