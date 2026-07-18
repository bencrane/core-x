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

# Canonical /count SQL column contract, in SELECT order (the sidecar echoes the
# SQL aliases back as ``columns``; any drift is a 502, never a mislabeled stat).
EXPECTED_COLS = (
    "count", "won_total", "won_median", "won_avg", "book_total", "book_median",
    "book_avg", "awards_median", "committed_award_ct", "award_median", "award_avg",
    "runway_total", "runway_median", "runway_avg", "vehicle_seats", "vehicle_holders",
    "vehicle_ceiling_total", "vehicle_headroom_total",
    "pricing_fixed_value", "pricing_cost_value", "pricing_tm_lh_value",
    "pricing_fixed_ct", "pricing_cost_ct", "pricing_tm_lh_ct", "pricing_other_ct",
    "ffp_unfinanced_value", "ffp_unfinanced_ct",
)

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


def _add_pricing_other(stats: dict[str, Any]) -> None:
    """pricing_other_value is the post-rounding residual so the four classes sum
    to book_total exactly; never a fifth independently rounded SQL partition."""
    stats["pricing_other_value"] = (
        stats["book_total"]
        - stats["pricing_fixed_value"]
        - stats["pricing_cost_value"]
        - stats["pricing_tm_lh_value"]
    )


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


@router.get("/{slug}/scope", dependencies=[Depends(require_service_token)])
async def collection_scope(slug: str) -> dict[str, Any]:
    """The collection's pair-set scope, with the plain-language work rendering
    (naics_psc_deliverable.what_was_done) per pair — the Markets Explore
    surface's seed (2026-07-17 filter-first cycle). One sidecar VALUES probe."""
    await _load_collections()
    c = _cache["collections"].get(slug)
    if c is None:
        raise _refuse(f"unknown collection '{slug}'")
    pairs = sorted(c["pairs"])
    language: dict[tuple[str, str], str] = {}
    token = os.environ.get("QUERY_SIDECAR_TOKEN")
    if token and pairs:
        values = ",".join(f"('{n}','{p_}')" for n, p_ in pairs)
        sql = (
            "SELECT pr.naics_code, pr.psc_code, d.what_was_done "
            f"FROM (VALUES {values}) pr(naics_code, psc_code) "
            "LEFT JOIN naics_psc_deliverable d "
            "ON d.naics_code = pr.naics_code AND d.psc_code = pr.psc_code"
        )
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                r = await client.post(
                    f"{SIDECAR_URL}/api/v1/sql",
                    headers={"Authorization": f"Bearer {token}"},
                    json={"sql": sql, "limit": 20000},
                )
            if r.status_code == 200:
                for n, p_, w in r.json().get("rows") or []:
                    if w:
                        language[(n, p_)] = w
        except httpx.HTTPError:
            logger.warning("scope language probe failed for %s; serving codes only", slug)
    return {
        "slug": slug,
        "title": c["title"],
        "description": c["description"],
        "pairs": [
            {"naics": n, "psc": p_, "what_was_done": language.get((n, p_))}
            for n, p_ in pairs
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
    won_where = " AND ".join(won_preds)

    hq_pred = ""
    if based_in:
        in_list = ",".join(f"'{s}'" for s in based_in)
        hq_pred = (
            " AND w.uei IN (SELECT uei FROM gtm_sam_entities"
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
  WHERE {won_where}{hq_pred}
),
awards AS (
  SELECT a.recipient_uei AS uei,
         (a.award_topology = 'vehicle') AS is_vehicle,
         greatest(coalesce(a.current_total_value_of_award, 0), 0) AS cvalue,
         greatest(coalesce(a.current_total_value_of_award, 0) - coalesce(a.life_to_date_obligated, 0), 0) AS crunway,
         greatest(coalesce(a.potential_ceiling, 0), 0) AS vceiling,
         greatest(coalesce(a.potential_ceiling, 0) - coalesce(a.life_to_date_obligated, 0), 0) AS vheadroom,
         (CASE WHEN a.latest_pricing_code IN ('A','B','J','K','L','M') THEN 'fixed'
               WHEN a.latest_pricing_code IN ('R','S','T','U','V') THEN 'cost'
               WHEN a.latest_pricing_code IN ('Y','Z') THEN 'tm_lh'
               ELSE 'other' END) AS pricing_class,
         (a.latest_pricing_code = 'J'
          AND coalesce(a.latest_financing_code, 'Z') IN ('Z','NOT APPLICABLE')) AS is_ffp_unfin
  FROM usaspending_fpds_prime_award_state a
  JOIN pairs pr ON a.naics_code = pr.naics_code AND a.product_or_service_code = pr.psc_code
  JOIN m ON m.uei = a.recipient_uei
  WHERE a.current_end_date >= current_date AND a.is_terminated = FALSE
),
firm AS (
  SELECT m.uei, m.won,
         coalesce(f.committed_ct, 0) AS committed_ct,
         coalesce(f.committed_value, 0) AS committed_value,
         coalesce(f.committed_runway, 0) AS committed_runway,
         coalesce(f.vehicle_ct, 0) AS vehicle_ct,
         coalesce(f.vehicle_ceiling, 0) AS vehicle_ceiling,
         coalesce(f.vehicle_headroom, 0) AS vehicle_headroom
  FROM m LEFT JOIN (
    SELECT uei,
           count(*) FILTER (WHERE NOT is_vehicle) AS committed_ct,
           sum(cvalue) FILTER (WHERE NOT is_vehicle) AS committed_value,
           sum(crunway) FILTER (WHERE NOT is_vehicle) AS committed_runway,
           count(*) FILTER (WHERE is_vehicle) AS vehicle_ct,
           sum(vceiling) FILTER (WHERE is_vehicle) AS vehicle_ceiling,
           sum(vheadroom) FILTER (WHERE is_vehicle) AS vehicle_headroom
    FROM awards GROUP BY 1
  ) f USING (uei)
)
SELECT count(*) AS "count",
       round(coalesce(sum(won), 0), 0) AS won_total,
       round(coalesce(median(won), 0), 0) AS won_median,
       round(coalesce(avg(won), 0), 0) AS won_avg,
       round(coalesce(sum(committed_value), 0), 0) AS book_total,
       round(coalesce(median(committed_value), 0), 0) AS book_median,
       round(coalesce(avg(committed_value), 0), 0) AS book_avg,
       coalesce(median(committed_ct), 0) AS awards_median,
       (SELECT count(*) FROM awards WHERE NOT is_vehicle) AS committed_award_ct,
       (SELECT round(coalesce(median(cvalue), 0), 0) FROM awards WHERE NOT is_vehicle) AS award_median,
       (SELECT round(coalesce(avg(cvalue), 0), 0) FROM awards WHERE NOT is_vehicle) AS award_avg,
       round(coalesce(sum(committed_runway), 0), 0) AS runway_total,
       round(coalesce(median(committed_runway), 0), 0) AS runway_median,
       round(coalesce(avg(committed_runway), 0), 0) AS runway_avg,
       coalesce(sum(vehicle_ct), 0) AS vehicle_seats,
       count(*) FILTER (WHERE vehicle_ct > 0) AS vehicle_holders,
       round(coalesce(sum(vehicle_ceiling), 0), 0) AS vehicle_ceiling_total,
       round(coalesce(sum(vehicle_headroom), 0), 0) AS vehicle_headroom_total,
       (SELECT round(coalesce(sum(cvalue), 0), 0) FROM awards WHERE NOT is_vehicle AND pricing_class = 'fixed') AS pricing_fixed_value,
       (SELECT round(coalesce(sum(cvalue), 0), 0) FROM awards WHERE NOT is_vehicle AND pricing_class = 'cost') AS pricing_cost_value,
       (SELECT round(coalesce(sum(cvalue), 0), 0) FROM awards WHERE NOT is_vehicle AND pricing_class = 'tm_lh') AS pricing_tm_lh_value,
       (SELECT count(*) FROM awards WHERE NOT is_vehicle AND pricing_class = 'fixed') AS pricing_fixed_ct,
       (SELECT count(*) FROM awards WHERE NOT is_vehicle AND pricing_class = 'cost') AS pricing_cost_ct,
       (SELECT count(*) FROM awards WHERE NOT is_vehicle AND pricing_class = 'tm_lh') AS pricing_tm_lh_ct,
       (SELECT count(*) FROM awards WHERE NOT is_vehicle AND pricing_class = 'other') AS pricing_other_ct,
       (SELECT round(coalesce(sum(cvalue), 0), 0) FROM awards WHERE NOT is_vehicle AND is_ffp_unfin) AS ffp_unfinanced_value,
       (SELECT count(*) FROM awards WHERE NOT is_vehicle AND is_ffp_unfin) AS ffp_unfinanced_ct
FROM firm
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
    cols = payload.get("columns")
    if cols is not None and tuple(cols) != EXPECTED_COLS:
        logger.error("sidecar market-collections columns mismatch: %s", cols)
        raise HTTPException(status_code=502, detail="sidecar column contract mismatch")
    if payload.get("rows"):
        stats = dict(zip(cols or EXPECTED_COLS, payload["rows"][0]))
    else:
        stats = dict.fromkeys(EXPECTED_COLS, 0)
    _add_pricing_other(stats)
    return {
        **stats,
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
