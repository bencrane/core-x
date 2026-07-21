"""growth-market-slice — the Growth lane's market cards (per work-lane, live).

The Growth lane (gc Markets, 2026-07-20 operator ruling): each construction
work-lane is its OWN card — per-lane qualification is structural, never a
multi-select rule. At rest a card shows the FULL market (mega numbers, zero
editorial — the restraint move is delivered live, never by the page); the
growth cut (recent-vs-baseline multiple, windows, band, new entrants) is a
CUT-RAIL dial applied on the call. A later "master" view unions qualifying
sets across cards (per-lane, never summed) — deferred, zero rework.

Substrate: ``gtm_construction_lane_months`` (firm × lane × month; #1242 —
535,123 rows; measured 16–34ms for every growth shape). Columns per row:
obligation_sum, n_actions, n_awards, n_new_awards, new_award_obligation_sum,
n_agencies. WATERMARK DOCTRINE: every window anchors to max(month), never
current_date (FPDS publication lag: freshest ~1 month empty, months 2–3
~half-reported under DoD's ~90-day embargo — disclosed on every response).

Design law (unchanged): predicates are the ONLY firm-filter vocabulary —
``compile_predicates`` imported, never reimplemented. Growth dials are scope
mechanics (window sums + ratio gates), not predicates — they are validated
typed ranges here, never free text.

Endpoints (service-token gated):
  GET  /api/v1/growth-market-slice/catalog       → the 5 lane cards + watermark
  POST /api/v1/growth-market-slice/pack
       {"lane", "growth"?: {"multiple", "recent_months", "baseline_months",
        "band_min", "band_max", "new_entrants"}, "predicates"?}
  POST /api/v1/growth-market-slice/count          → qualifying-firm count only

Pack sections (concurrent): tiles (firms, recent in-lane $, new-work share,
median firm $, texture), monthly flow (36mo, new-award overlay), states,
top firms (names always carried — reveal is a surface dial; growth ratio when
dialed), and when dialed: qualifying aggregates + the separate new-entrant
count (no-baseline firms are NEVER mixed into a multiple cut).
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Any

import httpx
from fastapi import APIRouter, Depends, HTTPException

from ..service_token import require_service_token
from .market_query_v1 import compile_predicates

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/growth-market-slice", tags=["growth-market-slice"])

SIDECAR_URL = os.environ.get("QUERY_SIDECAR_URL", "https://query-sidecar-api.onrender.com")

MART = "gtm_construction_lane_months"

# Lane registry — keys MUST match the mart's lane values (builder constant,
# authority hq construction_pairsets_v1.json). Titles/descriptions are the
# card-front language; free-hand copy beyond this rides the label manifest.
LANES: dict[str, dict[str, str]] = {
    "construction-vertical-building": {
        "title": "Vertical Building",
        "members_are": "Firms erecting government buildings — new construction of offices, housing, and facilities",
    },
    "construction-building-repair-alteration": {
        "title": "Building Repair & Alteration",
        "members_are": "Firms altering and repairing government buildings — renovation, conversion, rehabilitation",
    },
    "construction-building-maintenance": {
        "title": "Building Maintenance",
        "members_are": "Firms performing recurring maintenance of government real property",
    },
    "construction-civil-infrastructure": {
        "title": "Civil Infrastructure",
        "members_are": "Firms building civil works — roads, waterways, utilities-scale and site infrastructure",
    },
    "construction-industrial-defense-facilities": {
        "title": "Industrial & Defense Facilities",
        "members_are": "Firms constructing industrial and defense-mission facilities",
    },
}

_DISCLOSURES = [
    "Windows anchor to the last complete data month (the watermark), never the calendar: "
    "FPDS publication lags — the freshest month is largely unreported and months 2–3 are "
    "partially reported (DoD publishes on a ~90-day delay).",
    "In-lane obligations only — a firm's work outside this lane never enters this card "
    "(per-lane qualification; lanes are never blended).",
    "Month grain; a 'new award' is FPDS's base-award action (mods and funding actions are not new work).",
]


def _refuse(detail: str) -> HTTPException:
    return HTTPException(status_code=422, detail=detail)


def _resolve(lane: str) -> dict[str, str]:
    meta = LANES.get(lane)
    if meta is None:
        raise _refuse(f"unknown lane '{lane}' (GET /api/v1/growth-market-slice/catalog lists the set)")
    return meta


def parse_growth(body: dict[str, Any]) -> dict[str, Any] | None:
    """Typed-range validation of the growth dial. None = at rest (full market)."""
    g = body.get("growth")
    if g in (None, {}):
        return None
    if not isinstance(g, dict):
        raise _refuse("growth must be an object")
    out: dict[str, Any] = {}
    mult = g.get("multiple", 2.0)
    if not isinstance(mult, (int, float)) or isinstance(mult, bool) or not (1.0 <= float(mult) <= 100.0):
        raise _refuse("growth.multiple must be a number in [1, 100]")
    out["multiple"] = float(mult)
    for key, default, lo, hi in (("recent_months", 12, 1, 60), ("baseline_months", 24, 1, 120)):
        v = g.get(key, default)
        if not isinstance(v, int) or isinstance(v, bool) or not (lo <= v <= hi):
            raise _refuse(f"growth.{key} must be an integer in [{lo}, {hi}]")
        out[key] = v
    for key, default in (("band_min", 1_000_000.0), ("band_max", 1_000_000_000.0)):
        v = g.get(key, default)
        if v is None:
            v = default
        if not isinstance(v, (int, float)) or isinstance(v, bool) or v < 0:
            raise _refuse(f"growth.{key} must be a non-negative number")
        out[key] = float(v)
    if out["band_max"] <= out["band_min"]:
        raise _refuse("growth.band_max must be greater than growth.band_min")
    ne = g.get("new_entrants", False)
    if not isinstance(ne, bool):
        raise _refuse("growth.new_entrants must be true or false")
    out["new_entrants"] = ne
    return out


def build_pack_sql(lane: str, growth: dict[str, Any] | None,
                   predicate_expr: str | None) -> dict[str, str]:
    """Pure SQL assembly. All sections share the per-firm window CTE; every
    window is watermark-anchored. At rest the cohort is the FULL market
    (recent in-lane $ > 0); a growth dial gates it to the compounding subset
    (annualization factor recent/baseline months — rates, not raw sums)."""
    g = growth or {}
    rm = int(g.get("recent_months", 12))
    bm = int(g.get("baseline_months", 24))
    pred_leg = f"\n    AND w.uei IN (\n{predicate_expr}\n)" if predicate_expr else ""

    if growth is None:
        gate = "w.recent > 0"
    elif g["new_entrants"]:
        gate = (f"w.baseline IS NULL AND w.recent >= {g['band_min']} "
                f"AND w.recent <= {g['band_max']}")
    else:
        factor = rm / bm
        gate = (f"w.recent >= {g['band_min']} AND w.recent <= {g['band_max']} "
                f"AND w.baseline > 0 AND w.recent >= {g['multiple']} * w.baseline * {factor}")

    base = f"""
WITH edge AS (SELECT max(month) AS mx FROM {MART}),
w AS (
  SELECT uei,
         sum(obligation_sum) FILTER (WHERE month >  (SELECT mx FROM edge) - INTERVAL '{rm} months') AS recent,
         sum(obligation_sum) FILTER (WHERE month <= (SELECT mx FROM edge) - INTERVAL '{rm} months'
                                 AND month >  (SELECT mx FROM edge) - INTERVAL '{rm + bm} months') AS baseline,
         sum(new_award_obligation_sum) FILTER (WHERE month > (SELECT mx FROM edge) - INTERVAL '{rm} months') AS recent_new,
         sum(n_awards) FILTER (WHERE month > (SELECT mx FROM edge) - INTERVAL '{rm} months') AS recent_awards
  FROM {MART}
  WHERE lane = '{lane}'
    AND month > (SELECT mx FROM edge) - INTERVAL '{rm + bm} months'
  GROUP BY 1
),
m AS (
  SELECT w.* FROM w
  WHERE {gate}{pred_leg}
)"""

    return {
        "tiles": base + """
SELECT (SELECT mx FROM edge)                                             AS watermark,
       count(*)                                                          AS firms,
       round(coalesce(sum(recent), 0), 0)                                AS recent_usd,
       round(coalesce(median(recent), 0), 0)                             AS median_firm_usd,
       round(coalesce(quantile_cont(recent, 0.8), 0), 0)                 AS p80_firm_usd,
       round(100.0 * sum(recent_new) / nullif(sum(recent), 0), 1)        AS new_work_pct,
       round(coalesce(sum(recent_awards), 0), 0)                         AS recent_awards,
       round(coalesce(median(CASE WHEN baseline > 0 THEN recent / baseline END), 3), 3) AS median_ratio_raw
FROM m""",
        "monthly_series": f"""
WITH edge AS (SELECT max(month) AS mx FROM {MART})
SELECT month, round(sum(obligation_sum), 0) AS usd,
       round(sum(new_award_obligation_sum), 0) AS new_award_usd
FROM {MART}
WHERE lane = '{lane}' AND month > (SELECT mx FROM edge) - INTERVAL '36 months'
GROUP BY 1 ORDER BY 1""",
        "states": base + """
SELECT e.physical_state AS state, count(*) AS firms
FROM m JOIN gtm_sam_entities e USING (uei)
WHERE e.physical_state IS NOT NULL
GROUP BY 1 ORDER BY 2 DESC LIMIT 12""",
        "top_firms": base + """
SELECT m.uei, any_value(e.legal_business_name) AS name,
       round(m.recent, 0) AS recent_usd,
       round(m.baseline, 0) AS baseline_usd,
       round(CASE WHEN m.baseline > 0 THEN m.recent / m.baseline END, 2) AS ratio_raw,
       round(100.0 * m.recent_new / nullif(m.recent, 0), 0) AS new_work_pct
FROM m LEFT JOIN gtm_sam_entities e USING (uei)
GROUP BY m.uei, m.recent, m.baseline, m.recent_new
ORDER BY m.recent DESC LIMIT 15""",
    }


def build_count_sql(lane: str, growth: dict[str, Any] | None,
                    predicate_expr: str | None) -> str:
    prefix = build_pack_sql(lane, growth, predicate_expr)["tiles"].split(
        "SELECT (SELECT mx FROM edge)")[0]
    return prefix + "SELECT count(*) AS firms FROM m"


async def _run_sidecar(client: httpx.AsyncClient, sql: str, limit: int = 100) -> dict[str, Any]:
    token = os.environ.get("QUERY_SIDECAR_TOKEN")
    if not token:
        raise HTTPException(status_code=503, detail="QUERY_SIDECAR_TOKEN not configured")
    r = await client.post(
        f"{SIDECAR_URL}/api/v1/sql",
        headers={"Authorization": f"Bearer {token}"},
        json={"sql": sql, "limit": limit},
    )
    if r.status_code != 200:
        logger.error("sidecar growth-market-slice failed: %s %s", r.status_code, r.text[:300])
        raise HTTPException(status_code=502, detail="sidecar query failed")
    return r.json()


def _row(payload: dict[str, Any]) -> dict[str, Any]:
    cols, rows = payload.get("columns") or [], payload.get("rows") or []
    return dict(zip(cols, rows[0])) if rows else {}


def _rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    cols = payload.get("columns") or []
    return [dict(zip(cols, r)) for r in payload.get("rows") or []]


# One grouped statement: every lane's at-rest summary (recent-12 from the
# watermark) — the wall's card fronts carry numbers, never just description.
_CATALOG_SQL = f"""
WITH edge AS (SELECT max(month) AS mx FROM {MART}),
w AS (
  SELECT lane, uei, sum(obligation_sum) AS recent,
         sum(new_award_obligation_sum) AS recent_new
  FROM {MART}
  WHERE month > (SELECT mx FROM edge) - INTERVAL '12 months'
  GROUP BY 1, 2
)
SELECT lane,
       (SELECT mx FROM edge)                                      AS watermark,
       count(*) FILTER (WHERE recent > 0)                         AS firms,
       round(coalesce(sum(recent), 0), 0)                         AS recent_usd,
       round(coalesce(median(recent) FILTER (WHERE recent > 0), 0), 0) AS median_firm_usd,
       round(100.0 * sum(recent_new) / nullif(sum(recent), 0), 1) AS new_work_pct
FROM w GROUP BY 1"""


@router.get("/catalog", dependencies=[Depends(require_service_token)])
async def catalog() -> dict[str, Any]:
    async with httpx.AsyncClient(timeout=30.0) as client:
        payload = await _run_sidecar(client, _CATALOG_SQL, limit=20)
    stats = {r["lane"]: r for r in _rows(payload)}
    watermark = next(iter(stats.values()), {}).get("watermark")
    return {
        "cards": [
            {"lane": lane, **meta,
             "summary": {k: stats.get(lane, {}).get(k)
                         for k in ("firms", "recent_usd", "median_firm_usd", "new_work_pct")}}
            for lane, meta in LANES.items()
        ],
        "watermark": watermark,
        "disclosures": _DISCLOSURES,
        "artifact": payload.get("artifact"),
    }


@router.post("/pack", dependencies=[Depends(require_service_token)])
async def pack(body: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(body, dict):
        raise _refuse("body must be an object")
    lane = str(body.get("lane") or "")
    meta = _resolve(lane)
    growth = parse_growth(body)
    preds = body.get("predicates")
    expr, echoes, disclosures = (None, [], [])
    if preds not in (None, []):
        if not isinstance(preds, list):
            raise _refuse("predicates must be a list of {term, ...dials} objects")
        expr, echoes, disclosures = compile_predicates({"predicates": preds})

    sections = build_pack_sql(lane, growth, expr)
    t0 = time.monotonic()
    async with httpx.AsyncClient(timeout=90.0) as client:
        results = await asyncio.gather(*[_run_sidecar(client, sql) for sql in sections.values()])
    payloads = dict(zip(sections.keys(), results))

    tiles = _row(payloads["tiles"])
    return {
        "lane": lane,
        "title": meta["title"],
        "members_are": meta["members_are"],
        "growth": growth,
        "predicates": echoes,
        "disclosures": _DISCLOSURES + disclosures,
        "tiles": tiles,
        "monthly_series": _rows(payloads["monthly_series"]),
        "states": _rows(payloads["states"]),
        "top_firms": _rows(payloads["top_firms"]),
        "count": tiles.get("firms", 0),
        "elapsed_ms": round((time.monotonic() - t0) * 1000, 1),
        "artifact": payloads["tiles"].get("artifact"),
    }


@router.post("/count", dependencies=[Depends(require_service_token)])
async def count(body: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(body, dict):
        raise _refuse("body must be an object")
    lane = str(body.get("lane") or "")
    _resolve(lane)
    growth = parse_growth(body)
    preds = body.get("predicates")
    expr, echoes, disclosures = (None, [], [])
    if preds not in (None, []):
        if not isinstance(preds, list):
            raise _refuse("predicates must be a list of {term, ...dials} objects")
        expr, echoes, disclosures = compile_predicates({"predicates": preds})
    sql = build_count_sql(lane, growth, expr)
    async with httpx.AsyncClient(timeout=90.0) as client:
        payload = await _run_sidecar(client, sql, limit=1)
    return {
        "lane": lane,
        "growth": growth,
        "count": _row(payload).get("firms", 0),
        "predicates": echoes,
        "disclosures": _DISCLOSURES + disclosures,
        "elapsed_ms": payload.get("elapsed_ms"),
        "artifact": payload.get("artifact"),
    }
