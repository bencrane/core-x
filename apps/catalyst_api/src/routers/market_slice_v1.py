"""market-slice — the capital-card aggregate pack, recomputable under predicates.

The gc Markets capital cards (catalog v1.4, baked fixture) become LIVE surfaces:
one endpoint takes a card scope (slug) plus an optional list of market-query
predicates, and returns the card's FULL anatomy recomputed for the slice —
tiles, pricing mix, win-cadence, committed book / vehicle capacity, and
distributions — every section artifact-stamped.

Design law (ruled 2026-07-19): the predicate grammar is the ONLY filter
vocabulary — `compile_predicates` (market_query_v1) is imported, never
reimplemented; a slice therefore always has an exact predicate-JSON
representation (save-as-spec composability, one semantic authority). The
aggregation SQL is bespoke to the card anatomy (shared semantics, specific
presentation).

Card definition (reconciled to the bake, Phase-0 probe 2026-07-19: exact on
29/30 cards at artifact 20260718T021418Z; facility-maintenance-repair carries
a disclosed +1.8% delta — see capital_card_scopes.json):
  cohort  = band firms (gtm_entity_pricing_mix.active_obl in [band.min,
            band.max]) holding ≥1 ACTIVE (end ≥ today, not terminated)
            UNFINANCED (financing NULL/Z/NOT APPLICABLE) award in the card's
            pair scope — vehicles count for membership and award counts
  dollars = life-to-date obligations over those unfinanced active awards
  mix     = fixed (A,B,J,K,L,M) / cost (R,S,T,U,V) / T&M (Y,Z) split
  cadence = FY-won shares (gtm_entity_fy_won) + multi-award/agency + median
            active awards (gtm_entity_award_book; median over firms with book
            rows — the bake's convention)

Scope resolution: canonical cards = hqx pg ``gtm.market_collections`` BASE
pairs ∪ overlay extensions; sweep/new/staffing cards = overlay pair lists
(``capital_card_scopes.json``, generated + live-verified by
``tools/generate_capital_card_scopes.py``).

Endpoints (service-token gated):
  GET  /api/v1/market-slice/catalog          → the 30 card scopes (slug, title,
        lens, pair_count) — the sliceable set
  POST /api/v1/market-slice/pack   {"slug", "predicates"?: [...], "band"?}
        → the full aggregate pack (sections below)
  POST /api/v1/market-slice/count  {"slug", "predicates"?: [...], "band"?}
        → {"count"} only (the question branch: "how many…" answers)
  POST /api/v1/market-slice/entities {"slug"?, "predicates"?: [...], "band"?,
        "limit"? ≤2000} → the cohort as per-firm rows with HQ geo (the Explore
        map/table read). slug → card cohort, money = unfinanced-in-scope;
        slug-less → pure grammar cohort (predicates required), money = active
        obligations. `count` always the full cohort (parity with /count).

Sections run concurrently against the sidecar (measured: whole pack < 1s warm,
largest card + predicate ≈ 2s serial worst-case → ~450ms gathered).
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from pathlib import Path
from typing import Any

import httpx
from fastapi import APIRouter, Depends, HTTPException

from ..service_token import require_service_token
from .market_collections_v1 import _load_collections, _cache as _collections_cache
from .market_query_v1 import compile_predicates

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/market-slice", tags=["market-slice"])

SIDECAR_URL = os.environ.get("QUERY_SIDECAR_URL", "https://query-sidecar-api.onrender.com")

_OVERLAY_PATH = Path(__file__).resolve().parent.parent / "capital_card_scopes.json"
_overlay: dict[str, Any] | None = None

_DEFAULT_BAND = {"min": 1_000_000.0, "max": 100_000_000.0}

_FIXED = "'A','B','J','K','L','M'"
_COST = "'R','S','T','U','V'"
_TM = "'Y','Z'"


def _load_overlay() -> dict[str, Any]:
    global _overlay
    if _overlay is None:
        _overlay = json.loads(_OVERLAY_PATH.read_text())
    return _overlay


def _refuse(detail: str) -> HTTPException:
    return HTTPException(status_code=422, detail=detail)


async def _resolve_scope(slug: str) -> dict[str, Any]:
    """slug → {title, lens, pairs} (pg base ∪ overlay extensions, or overlay card)."""
    overlay = _load_overlay()
    ext = {tuple(p) for p in overlay["extensions"].get(slug, {}).get("pairs", [])}

    card = overlay["cards"].get(slug)
    if card is not None:
        pairs = {tuple(p) for p in card["pairs"]} | ext
        return {"title": card["title"], "lens": card["lens"], "pairs": sorted(pairs)}

    await _load_collections()
    c = _collections_cache["collections"].get(slug)
    if c is None:
        raise _refuse(f"unknown card '{slug}' (GET /api/v1/market-slice/catalog lists the sliceable set)")
    pairs = {tuple(p) for p in c["pairs"]} | ext
    return {"title": c["title"], "lens": "canonical", "pairs": sorted(pairs)}


def _parse_band(body: dict[str, Any]) -> dict[str, float]:
    band = body.get("band")
    if band is None:
        return dict(_DEFAULT_BAND)
    if not isinstance(band, dict):
        raise _refuse("band must be an object {min, max}")
    out = dict(_DEFAULT_BAND)
    for key in ("min", "max"):
        v = band.get(key)
        if v is None:
            continue
        if not isinstance(v, (int, float)) or isinstance(v, bool) or v < 0:
            raise _refuse(f"band.{key} must be a non-negative number")
        out[key] = float(v)
    if out["max"] <= out["min"]:
        raise _refuse("band.max must be greater than band.min")
    return out


def _compile_body_predicates(body: dict[str, Any]) -> tuple[str | None, list[dict], list[str]]:
    preds = body.get("predicates")
    if preds in (None, []):
        return None, [], []
    if not isinstance(preds, list):
        raise _refuse("predicates must be a list of {term, ...dials} objects")
    return compile_predicates({"predicates": preds})


def build_pack_sql(
    pairs: list[tuple[str, str]],
    band: dict[str, float],
    predicate_expr: str | None,
) -> dict[str, str]:
    """Pure SQL assembly: the pack's per-section statements over one cohort CTE."""
    values = ",".join(f"('{n}','{p}')" for n, p in pairs)
    pred_leg = f"\n    AND s.uei IN (\n{predicate_expr}\n)" if predicate_expr else ""

    def cohort(band_where: str) -> str:
        return f"""
WITH pairs(naics_code, psc_code) AS (VALUES {values}),
scoped AS (
  SELECT a.recipient_uei AS uei,
         greatest(coalesce(a.life_to_date_obligated,0),0) AS obl,
         (coalesce(a.latest_financing_code,'Z') IN ('Z','NOT APPLICABLE')) AS unfin,
         (CASE WHEN a.latest_pricing_code IN ({_FIXED}) THEN 'fixed'
               WHEN a.latest_pricing_code IN ({_COST}) THEN 'cost'
               WHEN a.latest_pricing_code IN ({_TM}) THEN 'tm'
               ELSE 'other' END) AS pclass,
         (a.award_topology = 'vehicle') AS is_vehicle,
         a.naics_code AS naics, a.product_or_service_code AS psc,
         greatest(coalesce(a.current_total_value_of_award,0),0) AS cvalue,
         greatest(coalesce(a.current_total_value_of_award,0)-coalesce(a.life_to_date_obligated,0),0) AS crunway,
         greatest(coalesce(a.potential_ceiling,0),0) AS vceiling,
         greatest(coalesce(a.potential_ceiling,0)-coalesce(a.life_to_date_obligated,0),0) AS vheadroom
  FROM usaspending_fpds_prime_award_state a
  JOIN pairs pr ON a.naics_code = pr.naics_code AND a.product_or_service_code = pr.psc_code
  WHERE a.current_end_date >= current_date AND a.is_terminated = FALSE
),
band AS (SELECT uei FROM gtm_entity_pricing_mix WHERE {band_where}),
m AS (
  SELECT DISTINCT s.uei FROM scoped s JOIN band USING (uei)
  WHERE s.unfin{pred_leg}
),
m_any AS (
  SELECT DISTINCT s.uei FROM scoped s JOIN band USING (uei)
  WHERE TRUE{pred_leg}
)"""

    in_band = f"active_obl >= {band['min']} AND active_obl <= {band['max']}"
    over_band = f"active_obl > {band['max']}"

    return {
        "tiles": cohort(in_band) + """
SELECT (SELECT count(*) FROM m) AS firms,
       count(*) AS awards_unfin,
       round(coalesce(sum(s.obl), 0), 0) AS unfin_usd,
       round(coalesce(sum(s.obl) FILTER (WHERE s.pclass='fixed'), 0), 0) AS fixed_usd,
       round(coalesce(sum(s.obl) FILTER (WHERE s.pclass='cost'), 0), 0) AS cost_usd,
       round(coalesce(sum(s.obl) FILTER (WHERE s.pclass='tm'), 0), 0) AS tm_usd,
       round(coalesce(sum(s.obl) FILTER (WHERE s.pclass='other'), 0), 0) AS other_usd,
       (SELECT round(coalesce(sum(s2.obl), 0), 0)
          FROM scoped s2 JOIN m_any USING (uei)) AS tot_usd
FROM scoped s JOIN m USING (uei) WHERE s.unfin""",
        "over_band": cohort(over_band) + """
SELECT (SELECT count(*) FROM m) AS firms,
       round(coalesce(sum(s.obl), 0), 0) AS unfin_usd
FROM scoped s JOIN m USING (uei) WHERE s.unfin""",
        "cadence": cohort(in_band) + """
SELECT round(100.0 * count(*) FILTER (WHERE fy26 > 0) / nullif(count(*), 0), 1) AS pct_won_fy26,
       round(100.0 * count(*) FILTER (WHERE fy26 > 0 AND fy25 > 0) / nullif(count(*), 0), 1) AS pct_won_both,
       round(100.0 * count(*) FILTER (WHERE committed_ct >= 2) / nullif(count(*), 0), 1) AS pct_multi_award,
       round(100.0 * count(*) FILTER (WHERE agency_ct >= 2) / nullif(count(*), 0), 1) AS pct_multi_agency,
       median(committed_ct) FILTER (WHERE has_book) AS med_awards
FROM (
  SELECT m.uei,
         coalesce((SELECT sum(won_obl) FROM gtm_entity_fy_won w WHERE w.uei = m.uei AND w.fy = 2026), 0) AS fy26,
         coalesce((SELECT sum(won_obl) FROM gtm_entity_fy_won w WHERE w.uei = m.uei AND w.fy = 2025), 0) AS fy25,
         coalesce(b.committed_award_ct, 0) AS committed_ct,
         coalesce(b.active_agency_ct, 0) AS agency_ct,
         (b.uei IS NOT NULL) AS has_book
  FROM m LEFT JOIN gtm_entity_award_book b USING (uei)
)""",
        "book": cohort(in_band) + """
SELECT round(coalesce(sum(cvalue) FILTER (WHERE NOT is_vehicle), 0), 0) AS book_total,
       round(coalesce(median(cvalue) FILTER (WHERE NOT is_vehicle), 0), 0) AS award_median,
       round(coalesce(avg(cvalue) FILTER (WHERE NOT is_vehicle), 0), 0) AS award_avg,
       round(coalesce(sum(crunway) FILTER (WHERE NOT is_vehicle), 0), 0) AS runway_total,
       count(*) FILTER (WHERE is_vehicle) AS vehicle_seats,
       count(DISTINCT uei) FILTER (WHERE is_vehicle) AS vehicle_holders,
       round(coalesce(sum(vceiling) FILTER (WHERE is_vehicle), 0), 0) AS vehicle_ceiling,
       round(coalesce(sum(vheadroom) FILTER (WHERE is_vehicle), 0), 0) AS vehicle_headroom
FROM scoped s JOIN m USING (uei)""",
        "states": cohort(in_band) + """
SELECT e.physical_state AS state, count(*) AS firms
FROM m JOIN gtm_sam_entities e USING (uei)
WHERE e.physical_state IS NOT NULL
GROUP BY 1 ORDER BY 2 DESC LIMIT 12""",
        "top_pairs": cohort(in_band) + """
SELECT s.naics, s.psc,
       round(sum(s.obl), 0) AS unfin_usd,
       count(DISTINCT s.uei) AS firms,
       any_value(d.what_was_done) AS what_was_done
FROM scoped s JOIN m USING (uei)
LEFT JOIN naics_psc_deliverable d ON d.naics_code = s.naics AND d.psc_code = s.psc
WHERE s.unfin
GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 10""",
        "size_bands": cohort(in_band) + """
SELECT coalesce(e.employee_size_band, 'unknown') AS band, count(*) AS firms
FROM m LEFT JOIN gtm_audience_entities e USING (uei)
GROUP BY 1 ORDER BY 2 DESC""",
        "series": cohort(in_band) + """
SELECT w.fy AS fy,
       round(coalesce(sum(w.won_obl), 0), 0) AS won_usd,
       count(DISTINCT w.uei) AS firms
FROM m JOIN gtm_entity_fy_won w USING (uei)
WHERE w.fy >= 2019
GROUP BY 1 ORDER BY 1""",
    }


def build_entities_sql(
    pairs: list[tuple[str, str]],
    band: dict[str, float],
    predicate_expr: str | None,
    limit: int,
) -> str:
    """The slice cohort as per-firm rows with geo — the Explore map/table read.

    Same cohort CTE as the pack (band + unfinanced-active-in-pair-scope ∪
    predicates); one row per firm, HQ coordinates LEFT-JOINed from
    gtm_entity_geo (a firm without a geocode still returns — the map discloses
    plotted-vs-total, the table carries everyone).
    """
    sections = build_pack_sql(pairs, band, predicate_expr)
    prefix = sections["tiles"].split("SELECT (SELECT count(*) FROM m)")[0]
    return prefix + f"""
SELECT m.uei,
       any_value(e.legal_business_name) AS legal_business_name,
       any_value(e.physical_city) AS physical_city,
       any_value(e.physical_state) AS physical_state,
       any_value(g.latitude) AS latitude,
       any_value(g.longitude) AS longitude,
       round(coalesce(sum(s.obl) FILTER (WHERE s.unfin), 0), 0) AS unfin_usd,
       count(*) FILTER (WHERE s.unfin) AS awards_unfin
FROM m
JOIN scoped s USING (uei)
LEFT JOIN gtm_sam_entities e ON e.uei = m.uei
LEFT JOIN gtm_entity_geo g ON g.uei = m.uei
GROUP BY m.uei
ORDER BY unfin_usd DESC, m.uei
LIMIT {limit}"""


def build_grammar_entities_sql(
    predicate_expr: str,
    band: dict[str, float] | None,
    limit: int,
) -> tuple[str, str]:
    """Slug-less cohort (pure predicate grammar) as per-firm rows with geo.

    Money column is the firm's ACTIVE OBLIGATIONS (gtm_entity_pricing_mix.active_obl
    — the band column), not unfinanced-in-scope: without a card there is no pair
    scope to restrict to. Returns (entities_sql, count_sql) over the same cohort.
    """
    band_where = (
        f"\nWHERE coalesce(p.active_obl, 0) >= {band['min']}"
        f" AND coalesce(p.active_obl, 0) <= {band['max']}"
        if band is not None
        else ""
    )
    entities = f"""
SELECT m.uei,
       e.legal_business_name,
       e.physical_city,
       e.physical_state,
       g.latitude,
       g.longitude,
       round(coalesce(p.active_obl, 0), 0) AS active_obl
FROM (
{predicate_expr}
) m
LEFT JOIN gtm_sam_entities e USING (uei)
LEFT JOIN gtm_entity_geo g USING (uei)
LEFT JOIN gtm_entity_pricing_mix p USING (uei){band_where}
ORDER BY active_obl DESC, m.uei
LIMIT {limit}"""
    count = f"""
SELECT count(*) AS firms
FROM (
{predicate_expr}
) m
LEFT JOIN gtm_entity_pricing_mix p USING (uei){band_where}"""
    return entities, count


def build_count_sql(pairs: list[tuple[str, str]], band: dict[str, float],
                    predicate_expr: str | None) -> str:
    sections = build_pack_sql(pairs, band, predicate_expr)
    prefix = sections["tiles"].split("SELECT (SELECT count(*) FROM m)")[0]
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
        logger.error("sidecar market-slice failed: %s %s", r.status_code, r.text[:300])
        raise HTTPException(status_code=502, detail="sidecar query failed")
    return r.json()


def _row(payload: dict[str, Any]) -> dict[str, Any]:
    cols = payload.get("columns") or []
    rows = payload.get("rows") or []
    return dict(zip(cols, rows[0])) if rows else {}


def _rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    cols = payload.get("columns") or []
    return [dict(zip(cols, r)) for r in payload.get("rows") or []]


@router.get("/catalog", dependencies=[Depends(require_service_token)])
async def catalog() -> dict[str, Any]:
    overlay = _load_overlay()
    await _load_collections()
    out = []
    for slug, c in _collections_cache["collections"].items():
        ext = overlay["extensions"].get(slug, {}).get("pairs", [])
        out.append({"slug": slug, "title": c["title"], "lens": "canonical",
                    "pair_count": len({tuple(p) for p in c["pairs"]} | {tuple(p) for p in ext})})
    for slug, c in overlay["cards"].items():
        ext = overlay["extensions"].get(slug, {}).get("pairs", [])
        out.append({"slug": slug, "title": c["title"], "lens": c["lens"],
                    "pair_count": len({tuple(p) for p in c["pairs"]} | {tuple(p) for p in ext})})
    return {"cards": sorted(out, key=lambda c: c["slug"]), "default_band": _DEFAULT_BAND}


@router.post("/pack", dependencies=[Depends(require_service_token)])
async def pack(body: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(body, dict):
        raise _refuse("body must be an object")
    slug = str(body.get("slug") or "")
    if not slug:
        raise _refuse("slug is required")
    scope = await _resolve_scope(slug)
    band = _parse_band(body)
    expr, echoes, disclosures = _compile_body_predicates(body)

    sections = build_pack_sql(scope["pairs"], band, expr)
    t0 = time.monotonic()
    async with httpx.AsyncClient(timeout=90.0) as client:
        results = await asyncio.gather(*[_run_sidecar(client, sql) for sql in sections.values()])
    payloads = dict(zip(sections.keys(), results))

    tiles = _row(payloads["tiles"])
    return {
        "slug": slug,
        "title": scope["title"],
        "lens": scope["lens"],
        "pair_count": len(scope["pairs"]),
        "band": band,
        "predicates": echoes,
        "disclosures": disclosures,
        "tiles": tiles,
        "over_band": _row(payloads["over_band"]),
        "cadence": _row(payloads["cadence"]),
        "book": _row(payloads["book"]),
        "distributions": {
            "states": _rows(payloads["states"]),
            "top_pairs": _rows(payloads["top_pairs"]),
            "size_bands": _rows(payloads["size_bands"]),
        },
        "series": _rows(payloads["series"]),
        "count": tiles.get("firms", 0),
        "elapsed_ms": round((time.monotonic() - t0) * 1000, 1),
        "artifact": payloads["tiles"].get("artifact"),
    }


@router.post("/count", dependencies=[Depends(require_service_token)])
async def count(body: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(body, dict):
        raise _refuse("body must be an object")
    slug = str(body.get("slug") or "")
    if not slug:
        raise _refuse("slug is required")
    scope = await _resolve_scope(slug)
    band = _parse_band(body)
    expr, echoes, disclosures = _compile_body_predicates(body)
    sql = build_count_sql(scope["pairs"], band, expr)
    async with httpx.AsyncClient(timeout=90.0) as client:
        payload = await _run_sidecar(client, sql, limit=1)
    row = _row(payload)
    return {
        "slug": slug,
        "count": row.get("firms", 0),
        "band": band,
        "predicates": echoes,
        "disclosures": disclosures,
        "elapsed_ms": payload.get("elapsed_ms"),
        "artifact": payload.get("artifact"),
    }

_GEO_DISCLOSURE = (
    "coordinates are the firm's SAM registration location (gtm_entity_geo, HQ grain); "
    "firms without a geocode are served in the rows and the count, never dropped"
)

_ENTITIES_MAX = 2000


@router.post("/entities", dependencies=[Depends(require_service_token)])
async def entities(body: dict[str, Any]) -> dict[str, Any]:
    """The compiled cohort as per-firm rows with geo — the Explore map/table read.

    With slug: the card cohort (band + unfinanced-active-in-pair-scope ∪
    predicates); money = unfinanced $ in the card's pair scope. Without slug:
    the pure predicate-grammar cohort (predicates required); money = active
    obligations. `count` is always the FULL cohort — parity with /count for
    the identical body; `returned` is the row page under `limit`.
    """
    if not isinstance(body, dict):
        raise _refuse("body must be an object")
    limit = body.get("limit", _ENTITIES_MAX)
    if not isinstance(limit, int) or isinstance(limit, bool) or not (1 <= limit <= _ENTITIES_MAX):
        raise _refuse(f"limit must be an integer in [1, {_ENTITIES_MAX}]")
    slug = str(body.get("slug") or "")
    expr, echoes, disclosures = _compile_body_predicates(body)

    t0 = time.monotonic()
    if slug:
        scope = await _resolve_scope(slug)
        band = _parse_band(body)
        ent_sql = build_entities_sql(scope["pairs"], band, expr, limit)
        count_sql = build_count_sql(scope["pairs"], band, expr)
        money_basis = "unfinanced_in_scope"
        title, lens = scope["title"], scope["lens"]
    else:
        if expr is None:
            raise _refuse(
                "slug or predicates required (an unscoped read of the whole universe is refused)"
            )
        band = _parse_band(body) if body.get("band") is not None else None
        ent_sql, count_sql = build_grammar_entities_sql(expr, band, limit)
        money_basis = "active_obligations"
        title, lens = None, None

    async with httpx.AsyncClient(timeout=90.0) as client:
        ent_payload, count_payload = await asyncio.gather(
            _run_sidecar(client, ent_sql, limit=limit),
            _run_sidecar(client, count_sql, limit=1),
        )
    rows = _rows(ent_payload)
    return {
        "slug": slug or None,
        "title": title,
        "lens": lens,
        "band": band,
        "predicates": echoes,
        "disclosures": [*disclosures, _GEO_DISCLOSURE],
        "money_basis": money_basis,
        "entities": rows,
        "returned": len(rows),
        "count": _row(count_payload).get("firms", 0),
        "elapsed_ms": round((time.monotonic() - t0) * 1000, 1),
        "artifact": ent_payload.get("artifact"),
    }
