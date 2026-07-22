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
from .market_query_v1 import (
    active_award_value_award_keys,
    active_work_category_award_keys,
    compile_predicates,
    pop_states_award_keys,
    pop_within_award_keys,
)

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


_BAND_UNBOUNDED = 1e18  # open upper edge (SQL-safe scientific literal; all money ≪ 1e18)


def _parse_band(body: dict[str, Any]) -> dict[str, float]:
    band = body.get("band")
    if band is None:
        return dict(_DEFAULT_BAND)
    if not isinstance(band, dict):
        raise _refuse("band must be an object {min, max}")
    # An explicit band is open-ended where unspecified: {min} = "min and up",
    # {max} = "up to max". The capital default fills nothing here — it exists
    # only for the band-less card endpoints ("under $1M" / "$100M+" presets
    # must never collapse into the $1M–$100M default).
    out = {"min": 0.0, "max": _BAND_UNBOUNDED}
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


def _parse_when(body: dict[str, Any]) -> dict[str, int] | None:
    """The WHEN frame (operator-ruled 2026-07-22): None = "holding now" (the
    standing active book — every read as before). {"mode": "won", fy_start,
    fy_end} = the introduction frame: membership and money become obligations
    INTRODUCED in the FY window on the in-scope combos (txn grain)."""
    w = body.get("when")
    if w is None:
        return None
    if not isinstance(w, dict):
        raise _refuse('when must be an object {"mode": "active"|"won", fy_start?, fy_end?}')
    mode = str(w.get("mode") or "active")
    if mode not in ("active", "won"):
        raise _refuse("when.mode must be 'active' | 'won'")
    if mode == "active":
        return None
    fys: dict[str, int] = {}
    for key in ("fy_start", "fy_end"):
        v = w.get(key)
        if not isinstance(v, int) or isinstance(v, bool) or not (2008 <= v <= 2026):
            raise _refuse(f"when.{key} must be an integer fiscal year in [2008, 2026]")
        fys[key] = v
    if fys["fy_end"] < fys["fy_start"]:
        raise _refuse("when.fy_end must be >= when.fy_start")
    return fys


async def _resolve_scope_union(body: dict[str, Any]) -> dict[str, Any] | None:
    """One scope from `slugs` (union of pair sets — the Work layer, multi-
    select) or the back-compat single `slug`. None when neither is sent."""
    slugs = body.get("slugs")
    if isinstance(slugs, list) and slugs:
        if not all(isinstance(s, str) and s for s in slugs):
            raise _refuse("slugs must be a list of non-empty strings")
        scopes = [await _resolve_scope(s) for s in slugs]
        pairs = sorted({tuple(p) for sc in scopes for p in sc["pairs"]})
        return {"title": " + ".join(sc["title"] for sc in scopes),
                "lens": "union" if len(scopes) > 1 else scopes[0]["lens"],
                "pairs": pairs}
    slug = str(body.get("slug") or "")
    if slug:
        return await _resolve_scope(slug)
    return None


def _parse_basis(body: dict[str, Any]) -> str:
    """Measure basis for a market scope — the audience-lens abstraction
    (operator-ruled 2026-07-22): the market IS the pair set (work scope,
    audience-agnostic); the capital reading (unfinanced-$ membership + money)
    is one BASIS, not the market's definition.

    "unfinanced" (default, the capital reading — back-compatible) |
    "active"     (any active in-scope work; money = all in-scope obligations).
    """
    basis = str(body.get("basis") or "unfinanced")
    if basis not in ("unfinanced", "active"):
        raise _refuse("basis must be 'unfinanced' | 'active'")
    return basis


def _parse_rollup(body: dict[str, Any]) -> str:
    """Entity altitude: "entity" (default — one row per UEI) | "family"
    (one row per ultimate parent via entity_hierarchy; money summed, the row
    keyed + geocoded at the family head)."""
    rollup = str(body.get("rollup") or "entity")
    if rollup not in ("entity", "family"):
        raise _refuse("rollup must be 'entity' | 'family'")
    return rollup


def _compile_body_predicates(body: dict[str, Any]) -> tuple[str | None, list[dict], list[str]]:
    preds = body.get("predicates")
    if preds in (None, []):
        return None, [], []
    if not isinstance(preds, list):
        raise _refuse("predicates must be a list of {term, ...dials} objects")
    return compile_predicates({"predicates": preds})


def build_pack_sql(
    pairs: list[tuple[str, str]],
    band: dict[str, float] | None,
    predicate_expr: str | None,
    basis: str = "unfinanced",
) -> dict[str, str]:
    """Pure SQL assembly: the pack's per-section statements over one cohort CTE.

    `basis` gates cohort membership: "unfinanced" = at least one unfinanced
    active in-scope award (the capital reading); "active" = any active
    in-scope award (audience-agnostic work scope)."""
    values = ",".join(f"('{n}','{p}')" for n, p in pairs)
    pred_leg = f"\n    AND s.uei IN (\n{predicate_expr}\n)" if predicate_expr else ""
    member_gate = "s.unfin" if basis == "unfinanced" else "TRUE"

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
  WHERE {member_gate}{pred_leg}
),
m_any AS (
  SELECT DISTINCT s.uei FROM scoped s JOIN band USING (uei)
  WHERE TRUE{pred_leg}
)"""

    # band=None (operator-ruled 2026-07-22): NO size gate — the band is a rail
    # dial now, not a baked default. over_band degenerates to FALSE (no "above
    # the band" lane when there is no band).
    in_band = (
        f"active_obl >= {band['min']} AND active_obl <= {band['max']}" if band else "TRUE"
    )
    over_band = f"active_obl > {band['max']}" if band else "FALSE"

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
    band: dict[str, float] | None,
    predicate_expr: str | None,
    limit: int,
    basis: str = "unfinanced",
) -> str:
    """The slice cohort as per-firm rows with geo — the Explore map/table read.

    Same cohort CTE as the pack (band + unfinanced-active-in-pair-scope ∪
    predicates); one row per firm, HQ coordinates LEFT-JOINed from
    gtm_entity_geo (a firm without a geocode still returns — the map discloses
    plotted-vs-total, the table carries everyone).
    """
    sections = build_pack_sql(pairs, band, predicate_expr, basis=basis)
    prefix = sections["tiles"].split("SELECT (SELECT count(*) FROM m)")[0]
    # Money follows the basis: unfinanced-in-scope (capital) or ALL active
    # in-scope obligations (agnostic). Wire aliases stay stable; money_basis
    # on the envelope names the semantics.
    money_filter = " FILTER (WHERE s.unfin)" if basis == "unfinanced" else ""
    return prefix + f"""
SELECT m.uei,
       any_value(e.legal_business_name) AS legal_business_name,
       any_value(e.physical_city) AS physical_city,
       any_value(e.physical_state) AS physical_state,
       any_value(g.latitude) AS latitude,
       any_value(g.longitude) AS longitude,
       round(coalesce(sum(s.obl){money_filter}, 0), 0) AS unfin_usd,
       count(*){money_filter} AS awards_unfin
FROM m
JOIN scoped s USING (uei)
LEFT JOIN gtm_sam_entities e ON e.uei = m.uei
LEFT JOIN gtm_entity_geo g ON g.uei = m.uei
GROUP BY m.uei
ORDER BY unfin_usd DESC, m.uei
LIMIT {limit}"""


def build_won_entities_sql(
    pairs: list[tuple[str, str]] | None,
    band: dict[str, float] | None,
    predicate_expr: str | None,
    limit: int,
    when: dict[str, int],
) -> tuple[str, str]:
    """The WON frame's per-firm read: membership = any obligation introduced in
    [fy_start, fy_end] on the in-scope combos (txn_events_combo, txn grain);
    money = the sum of those introductions. Financing/basis gates do not apply
    in this frame (introductions are pre-financing-composition by definition);
    the size band stays a holder property (standing active_obl). Returns
    (entities_sql, count_sql). Wire aliases match the standing read
    (unfin_usd carries the window money; awards_unfin = awards touched)."""
    a, b = when["fy_start"], when["fy_end"]
    pair_values = ",".join(f"('{n}','{p}')" for n, p in (pairs or []))
    pair_cte = f"pairs(naics_code, psc_code) AS (VALUES {pair_values}),\n" if pairs else ""
    pair_join = (
        "\n  JOIN pairs pr ON t.naics_code = pr.naics_code AND t.psc_code = pr.psc_code"
        if pairs
        else ""
    )
    pred_leg = f"\n    AND t.uei IN (\n{predicate_expr}\n)" if predicate_expr else ""
    band_join = (
        f"\nJOIN (SELECT uei FROM gtm_entity_pricing_mix WHERE active_obl >= {band['min']}"
        f" AND active_obl <= {band['max']}) bd USING (uei)"
        if band
        else ""
    )
    won_cte = f"""
WITH {pair_cte}won AS (
  SELECT t.uei,
         sum(t.obligation) AS won_usd,
         count(DISTINCT t.award_key) AS awards_touched
  FROM txn_events_combo t{pair_join}
  WHERE t.fy BETWEEN {a} AND {b}{pred_leg}
  GROUP BY t.uei
  HAVING sum(t.obligation) > 0
)"""
    entities = f"""{won_cte}
SELECT won.uei,
       e.legal_business_name,
       e.physical_city,
       e.physical_state,
       g.latitude,
       g.longitude,
       round(won.won_usd, 0) AS unfin_usd,
       won.awards_touched AS awards_unfin
FROM won{band_join}
LEFT JOIN gtm_sam_entities e ON e.uei = won.uei
LEFT JOIN gtm_entity_geo g ON g.uei = won.uei
ORDER BY unfin_usd DESC, won.uei
LIMIT {limit}"""
    count = f"""{won_cte}
SELECT count(*) AS firms
FROM won{band_join}"""
    return entities, count


def wrap_family_rollup(entities_sql: str) -> str:
    """Fold a per-UEI entities statement to FAMILY grain: group by the
    ultimate parent (entity_hierarchy; self when unparented), sum the money,
    key + name + geocode the row at the family head. Column aliases stay
    wire-identical; `members` rides as the family size."""
    return f"""
WITH per_uei AS ({entities_sql})
SELECT coalesce(h.ultimate_parent_uei, per_uei.uei) AS uei,
       coalesce(any_value(h.ultimate_parent_name), any_value(per_uei.legal_business_name)) AS legal_business_name,
       any_value(pe.physical_city) AS physical_city,
       any_value(pe.physical_state) AS physical_state,
       any_value(pg.latitude) AS latitude,
       any_value(pg.longitude) AS longitude,
       round(sum(per_uei.unfin_usd), 0) AS unfin_usd,
       sum(per_uei.awards_unfin) AS awards_unfin,
       count(*) AS members
FROM per_uei
LEFT JOIN entity_hierarchy h ON h.uei = per_uei.uei
LEFT JOIN gtm_sam_entities pe ON pe.uei = coalesce(h.ultimate_parent_uei, per_uei.uei)
LEFT JOIN gtm_entity_geo pg ON pg.uei = coalesce(h.ultimate_parent_uei, per_uei.uei)
GROUP BY 1
ORDER BY unfin_usd DESC, uei"""


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


def build_count_sql(pairs: list[tuple[str, str]], band: dict[str, float] | None,
                    predicate_expr: str | None, basis: str = "unfinanced") -> str:
    sections = build_pack_sql(pairs, band, predicate_expr, basis=basis)
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

# ── mega market — everything unfinanced-in-force in FPDS (ruled 2026-07-21) ──
#
# The vantage point one level above every card: active awards (end ≥ today,
# not terminated) whose latest FPDS contract-financing code is absent / Z /
# NOT APPLICABLE, across the WHOLE corpus — no pair scope, no band on the
# headline. Entity-grain single pass over gtm_entity_pricing_mix (the 71-col
# billing lens); the FY series reads gtm_entity_fy_won over the
# unfinanced-holding cohort. No predicates: the mega page is a standing
# statement; cuts belong to Explore and the cards.

_MEGA_PC = ("fixed", "cost", "tm_lh", "other")
_MEGA_FC = ("unfin", "prog", "perf", "comm", "othfin")

_MEGA_DEFINITION = (
    "unfinanced = latest FPDS contract-financing code absent or 'not applicable'; "
    "in force = award end date >= today and not terminated; "
    "dollars = life-to-date obligations on those awards"
)


def build_mega_sql(band: dict[str, float]) -> dict[str, str]:
    """Pure SQL assembly for the mega pack — one statement per section."""
    tile_cols = """
       count(*) FILTER (WHERE active_obl_fin_unfin > 0) AS firms,
       coalesce(sum(active_fin_unfin_ct), 0) AS awards_unfin,
       round(coalesce(sum(active_obl), 0), 0) AS tot_usd,
       round(coalesce(sum(active_obl_fin_unfin), 0), 0) AS unfin_usd,
       round(coalesce(sum(active_obl_fixed_unfin), 0), 0) AS fixed_usd,
       round(coalesce(sum(active_obl_cost_unfin), 0), 0) AS cost_usd,
       round(coalesce(sum(active_obl_tm_lh_unfin), 0), 0) AS tm_usd,
       round(coalesce(sum(active_obl_other_unfin), 0), 0) AS other_usd,
       round(coalesce(sum(active_obl_fin_prog), 0), 0) AS prog_usd,
       round(coalesce(sum(active_obl_fin_perf), 0), 0) AS perf_usd,
       round(coalesce(sum(active_obl_fin_comm), 0), 0) AS comm_usd,
       round(coalesce(sum(active_obl_fin_othfin), 0), 0) AS othfin_usd"""
    matrix_cols = ",\n".join(
        f"       round(coalesce(sum(active_obl_{pc}_{fc}), 0), 0) AS {pc}_{fc}_usd,\n"
        f"       coalesce(sum(active_{pc}_{fc}_ct), 0) AS {pc}_{fc}_ct"
        for pc in _MEGA_PC for fc in _MEGA_FC
    )
    return {
        "tiles": f"SELECT{tile_cols}\nFROM gtm_entity_pricing_mix",
        "matrix": f"SELECT\n{matrix_cols}\nFROM gtm_entity_pricing_mix",
        "band_tier": f"SELECT{tile_cols}\nFROM gtm_entity_pricing_mix\n"
                     f"WHERE active_obl >= {band['min']} AND active_obl <= {band['max']}",
        "series": """
WITH m AS (SELECT uei FROM gtm_entity_pricing_mix WHERE active_obl_fin_unfin > 0)
SELECT w.fy AS fy,
       round(coalesce(sum(w.won_obl), 0), 0) AS won_usd,
       count(DISTINCT w.uei) AS firms
FROM gtm_entity_fy_won w JOIN m USING (uei)
WHERE w.fy >= 2019
GROUP BY 1 ORDER BY 1""",
    }


@router.post("/mega", dependencies=[Depends(require_service_token)])
async def mega(body: dict[str, Any]) -> dict[str, Any]:
    """The mega-market pack — the aggregate the cards are constituents of.

    Body: optionally {"band": {min, max}} for the tier section (defaults to
    the capital band). Sections: tiles (whole corpus), the pricing×financing
    matrix, the banded tier, and the FY-won series of the unfinanced-holding
    cohort. Every number artifact-stamped; the definition rides the response.
    """
    if not isinstance(body, dict):
        raise _refuse("body must be an object")
    band = _parse_band(body)
    sections = build_mega_sql(band)
    t0 = time.monotonic()
    async with httpx.AsyncClient(timeout=90.0) as client:
        results = await asyncio.gather(*[_run_sidecar(client, sql) for sql in sections.values()])
    payloads = dict(zip(sections.keys(), results))

    mrow = _row(payloads["matrix"])
    matrix = [
        {"pricing": pc, "financing": fc,
         "usd": mrow.get(f"{pc}_{fc}_usd", 0), "awards": mrow.get(f"{pc}_{fc}_ct", 0)}
        for pc in _MEGA_PC for fc in _MEGA_FC
    ]
    return {
        "definition": _MEGA_DEFINITION,
        "band": band,
        "tiles": _row(payloads["tiles"]),
        "matrix": matrix,
        "band_tier": _row(payloads["band_tier"]),
        "series": _rows(payloads["series"]),
        "elapsed_ms": round((time.monotonic() - t0) * 1000, 1),
        "artifact": payloads["tiles"].get("artifact"),
    }


# ── active awards as points — the award-grain map read (2026-07-21) ──────────
#
# The Explore "active awards" lens: the awards themselves (not the firms) as
# place-of-performance points. Award-state ⋈ pop-centroids on the known key
# (contract_award_unique_key = generated_unique_award_id — NOT a shared column
# name). Optional predicate leg narrows to the grammar cohort's holders; no
# pair scope (the lens is universe-wide by design — the "present tense" read).

_AWARDS_MAX = 4000

_AWARDS_GEO_DISCLOSURE = (
    "coordinates are the award's primary place of performance "
    "(usaspending_award_pop_centroids, zip5-resolved); awards without a "
    "resolvable PoP are served in the count, never plotted"
)


def build_awards_sql(
    predicate_expr: str | None,
    limit: int,
    pairs: list[tuple[str, str]] | None = None,
    uei: str | None = None,
    band: dict[str, float] | None = None,
    award_key_exprs: list[str] | None = None,
) -> tuple[str, str]:
    """(points_sql, count_sql) over the active-award universe ∪ predicates.

    `pairs` narrows to a market's WORK SCOPE (award naics×psc in the card's
    pair set — semantics (b), operator-ruled 2026-07-22: the market's actual
    paper, not every award its cohort firms hold). `uei` narrows to one firm
    (the firm-dot PoP overlay). `band` applies THROUGH THE HOLDER (firm-identity
    dials follow the holder across grains — operator-ruled 2026-07-22): awards
    held by firms whose total active obligations (gtm_entity_pricing_mix
    .active_obl, the same gate the firm cohort uses) fall in the band. All
    legs compose.
    """
    pred_leg = (
        f"\n  AND a.recipient_uei IN (\n{predicate_expr}\n)" if predicate_expr else ""
    )
    uei_leg = f"\n  AND a.recipient_uei = '{uei}'" if uei else ""
    band_leg = (
        "\n  AND a.recipient_uei IN (SELECT uei FROM gtm_entity_pricing_mix"
        f" WHERE active_obl >= {band['min']} AND active_obl <= {band['max']})"
        if band
        else ""
    )
    # Award-grain geography (the Awards-drawer semantics): each expr selects
    # open-award KEYS whose OWN PoP matches — the paper itself, never a
    # holder gate.
    key_legs = "".join(
        f"\n  AND a.contract_award_unique_key IN (\n{expr}\n)"
        for expr in (award_key_exprs or [])
    )
    pair_values = ",".join(f"('{n}','{p}')" for n, p in (pairs or []))
    pair_cte = f"pairs(naics_code, psc_code) AS (VALUES {pair_values}),\n" if pairs else ""
    pair_join = (
        "\nJOIN pairs pr ON a.naics_code = pr.naics_code AND a.product_or_service_code = pr.psc_code"
        if pairs
        else ""
    )
    base = f"""
FROM usaspending_fpds_prime_award_state a{pair_join}
WHERE a.current_end_date >= current_date AND a.is_terminated = FALSE{pred_leg}{uei_leg}{band_leg}{key_legs}"""
    # Rank first, hydrate geo after: the centroid join runs over the ≤limit
    # page, never as a 30.7M hash build (measured 15.3s → sub-second).
    points = f"""
WITH {pair_cte}page AS (
  SELECT a.contract_award_unique_key AS award_key,
         a.award_id_piid AS piid,
         a.recipient_uei AS uei,
         a.recipient_name,
         greatest(coalesce(a.life_to_date_obligated, 0), 0) AS obl,
         (coalesce(a.latest_financing_code,'Z') IN ('Z','NOT APPLICABLE')) AS unfin,
         a.current_end_date AS end_date
{base}
  ORDER BY obl DESC, award_key
  LIMIT {limit}
)
SELECT p.*, c.latitude, c.longitude
FROM page p
LEFT JOIN usaspending_award_pop_centroids c ON c.generated_unique_award_id = p.award_key
ORDER BY p.obl DESC, p.award_key"""
    count_cte = f"WITH pairs(naics_code, psc_code) AS (VALUES {pair_values})\n" if pairs else ""
    count = f"""
{count_cte}SELECT count(*) AS awards,
       round(coalesce(sum(greatest(coalesce(a.life_to_date_obligated, 0), 0)), 0), 0) AS obl_usd,
       count(DISTINCT a.recipient_uei) AS firms
{base}"""
    return points, count


@router.post("/awards", dependencies=[Depends(require_service_token)])
async def awards(body: dict[str, Any]) -> dict[str, Any]:
    """Active awards as PoP points — the Explore award-lens map read.

    Body: {"predicates"?: [...], "limit"? ≤ 4000, "slug"?, "uei"?, "band"?}.
    Rows are the top-N active awards by life-to-date obligations (with PoP geo
    LEFT-joined); `count` / `obl_usd` / `firms` are always the FULL cohort.
    Unscoped reads are allowed here by design: the universe statement ("a
    quarter-million active awards") IS the surface's opening state. With
    `slug`, the read narrows to the market's WORK SCOPE (award naics×psc in
    the card's pair set — the market's actual paper). With `uei`, one firm's
    active awards (the firm-dot PoP overlay). With `band`, awards held by
    firms in the size band — the firm-grain dial following the holder, so the
    firm⇄award grain flip is continuous.
    """
    if not isinstance(body, dict):
        raise _refuse("body must be an object")
    limit = body.get("limit", _AWARDS_MAX)
    if not isinstance(limit, int) or isinstance(limit, bool) or not (1 <= limit <= _AWARDS_MAX):
        raise _refuse(f"limit must be an integer in [1, {_AWARDS_MAX}]")
    # Geography splits by grain here (the Awards-drawer semantics, operator-
    # ruled 2026-07-22): PoP terms filter the PAPER (award keys by the award's
    # own place of performance); every other predicate stays a holder gate.
    preds_in = body.get("predicates")
    if preds_in is not None and not isinstance(preds_in, list):
        raise _refuse("predicates must be a list of {term, ...dials} objects")
    award_geo_specs = [p for p in (preds_in or [])
                       if isinstance(p, dict)
                       and p.get("term") in ("place_of_performance_of_active_awards",
                                             "place_of_performance_within",
                                             "active_award_value",
                                             "active_work_category")]
    holder_specs = [p for p in (preds_in or []) if p not in award_geo_specs]
    expr, echoes, disclosures = _compile_body_predicates({"predicates": holder_specs})
    award_key_exprs: list[str] = []
    for spec in award_geo_specs:
        if spec.get("term") == "place_of_performance_of_active_awards":
            key_sql, echo = pop_states_award_keys(spec)
        elif spec.get("term") == "place_of_performance_within":
            key_sql, echo = pop_within_award_keys(spec)
        elif spec.get("term") == "active_work_category":
            key_sql, echo = active_work_category_award_keys(spec)
        else:
            key_sql, echo = active_award_value_award_keys(spec)
        award_key_exprs.append(key_sql)
        echoes = [*echoes, echo]
    if award_key_exprs:
        disclosures = [*disclosures,
                       "geography filters the paper itself: awards whose own place of performance "
                       "matches (open-award universe; awards without a resolvable PoP do not satisfy)"]

    scope = await _resolve_scope_union(body)
    pairs: list[tuple[str, str]] | None = None
    title = None
    if scope is not None:
        pairs = scope["pairs"]
        title = scope["title"]
        disclosures = [*disclosures,
                       "narrowed to the selected work: award NAICS x PSC within the union of the "
                       "selected collections' pair sets (the paper)"]
    uei = str(body.get("uei") or "") or None
    if uei is not None and not _AWARD_KEY_RE.match(uei):
        raise _refuse("uei must be alphanumeric")
    band = _parse_band(body) if body.get("band") is not None else None
    if band is not None:
        disclosures = [*disclosures,
                       "size band applies through the holder: awards held by firms whose total active "
                       "obligations fall in the band (gtm_entity_pricing_mix — the same gate the firm cohort uses)"]

    points_sql, count_sql = build_awards_sql(expr, limit, pairs=pairs, uei=uei, band=band,
                                             award_key_exprs=award_key_exprs)
    t0 = time.monotonic()
    async with httpx.AsyncClient(timeout=90.0) as client:
        points_payload, count_payload = await asyncio.gather(
            _run_sidecar(client, points_sql, limit=limit),
            _run_sidecar(client, count_sql, limit=1),
        )
    rows = _rows(points_payload)
    totals = _row(count_payload)
    return {
        "slug": str(body.get("slug") or "") or None,
        "slugs": body.get("slugs") if isinstance(body.get("slugs"), list) else None,
        "title": title,
        "predicates": echoes,
        "disclosures": [*disclosures, _AWARDS_GEO_DISCLOSURE],
        "awards": rows,
        "returned": len(rows),
        "count": totals.get("awards", 0),
        "obl_usd": totals.get("obl_usd", 0),
        "firms": totals.get("firms", 0),
        "elapsed_ms": round((time.monotonic() - t0) * 1000, 1),
        "artifact": points_payload.get("artifact"),
    }


# ── obligation flow by fiscal year — the Explore flow lens (2026-07-21) ──────
#
# Two modes:
#   whole-corpus — dollars obligated per FY across ALL actions (the annual
#     flow; a bar can include money on awards that have since ended).
#   active_only  — the VINTAGE DECOMPOSITION of the active book: each bar is
#     the obligations from that FY that sit on awards STILL ACTIVE today. These
#     bars sum to the map's stock ($2.44T), so "click → the years peel the map
#     apart" is exact. Joins txn_events_combo to the active-award key set.
# Predicates narrow to the cohort's holders in either mode. FY2026 partial-year
# disclosure rides only when the window includes it.

_FLOW_DISCLOSURE_FY26 = (
    "FY2026 is in progress and reporting-lagged (DoD actions post up to ~90 "
    "days late) — its bar is a floor, not a year"
)
_FLOW_DISCLOSURE_ACTIVE = (
    "each bar is obligations from that fiscal year that sit on awards still "
    "active today (end date >= today, not terminated) — the vintage "
    "decomposition of the active book, not total annual obligations"
)


def build_flow_sql(
    predicate_expr: str | None, fy_start: int, fy_end: int, active_only: bool = False
) -> str:
    if active_only:
        pred_leg = f"\n  AND t.uei IN (\n{predicate_expr}\n)" if predicate_expr else ""
        return f"""
WITH act AS (
  SELECT contract_award_unique_key AS k
  FROM usaspending_fpds_prime_award_state
  WHERE current_end_date >= current_date AND is_terminated = FALSE
)
SELECT t.fy AS fy,
       round(sum(t.obligation), 0) AS obligated,
       count(*) AS actions,
       count(DISTINCT t.uei) AS firms
FROM txn_events_combo t
JOIN act ON t.award_key = act.k
WHERE t.fy >= {fy_start} AND t.fy <= {fy_end}{pred_leg}
GROUP BY 1 ORDER BY 1"""
    pred_leg = f"\n  AND uei IN (\n{predicate_expr}\n)" if predicate_expr else ""
    return f"""
SELECT fy,
       round(sum(obligation), 0) AS obligated,
       count(*) AS actions,
       count(DISTINCT uei) AS firms
FROM txn_events_combo
WHERE fy >= {fy_start} AND fy <= {fy_end}{pred_leg}
GROUP BY 1 ORDER BY 1"""


@router.post("/flow", dependencies=[Depends(require_service_token)])
async def flow(body: dict[str, Any]) -> dict[str, Any]:
    """Obligations by fiscal year — the Explore flow-lens read.

    Body: {"predicates"?, "fy_start"?, "fy_end"?, "active_only"?}. Defaults
    FY2019–FY2026 whole-corpus. active_only intersects the active-award set so
    the bars decompose the map's stock by obligation vintage. Unscoped allowed.
    """
    if not isinstance(body, dict):
        raise _refuse("body must be an object")
    fy_start = body.get("fy_start", 2019)
    fy_end = body.get("fy_end", 2026)
    for name, v in (("fy_start", fy_start), ("fy_end", fy_end)):
        if not isinstance(v, int) or isinstance(v, bool) or not (2001 <= v <= 2030):
            raise _refuse(f"{name} must be an integer fiscal year in [2001, 2030]")
    if fy_end < fy_start:
        raise _refuse("fy_end must be >= fy_start")
    active_only = bool(body.get("active_only", False))
    expr, echoes, disclosures = _compile_body_predicates(body)

    sql = build_flow_sql(expr, fy_start, fy_end, active_only)
    extra_disc: list[str] = []
    if active_only:
        extra_disc.append(_FLOW_DISCLOSURE_ACTIVE)
    if fy_end >= 2026:
        extra_disc.append(_FLOW_DISCLOSURE_FY26)
    t0 = time.monotonic()
    async with httpx.AsyncClient(timeout=90.0) as client:
        payload = await _run_sidecar(client, sql, limit=40)
    return {
        "predicates": echoes,
        "disclosures": [*disclosures, *extra_disc],
        "fy_start": fy_start,
        "fy_end": fy_end,
        "active_only": active_only,
        "series": _rows(payload),
        "elapsed_ms": round((time.monotonic() - t0) * 1000, 1),
        "artifact": payload.get("artifact"),
    }


# ── the award profile — one award's full anatomy, sectioned (2026-07-21) ─────
#
# The award-dot click read: everything the substrate knows about ONE award,
# grouped into presentation-ready sections. Language comes from the
# combo-grain layers (naics_psc_labor_profile.work_summary +
# naics_psc_deliverable.what_was_done + the pg JTBD phrases) — NEVER
# award_descriptions (sparse, low quality; operator ruling 2026-07-21).

import re as _re

_AWARD_KEY_RE = _re.compile(r"^[A-Za-z0-9_.\-]{8,120}$")


def _safe_award_key(raw: Any) -> str:
    key = str(raw or "")
    if not _AWARD_KEY_RE.match(key):
        raise _refuse("award_key must be a contract_award_unique_key (alnum/_/./- only)")
    return key


async def _award_jtbd(naics: str | None, psc: str | None) -> list[str]:
    """Canonical job-to-be-done phrases for the award's combo (pg; soft-fail)."""
    if not naics or not psc:
        return []
    try:
        from ..db import get_db_connection
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT DISTINCT job_phrase FROM gtm.combo_job_to_be_done "
                    "WHERE naics_code = %s AND psc_code = %s AND model_id = %s "
                    "ORDER BY job_phrase",
                    (naics, psc, "opus-4.8-canonical"),
                )
                return [r[0] for r in await cur.fetchall()]
    except Exception:  # language garnish — never fail the profile over it
        logger.warning("award jtbd lookup failed", exc_info=True)
        return []


@router.post("/award", dependencies=[Depends(require_service_token)])
async def award_profile(body: dict[str, Any]) -> dict[str, Any]:
    """One award's profile — sectioned for the Explore award drawer.

    Body: {"award_key": "<contract_award_unique_key>"}. Sections: header,
    work (combo language + JTBD), money, payment, clock, ledger (FY sums +
    recent actions), vehicle, subcontracting, place. Artifact-stamped.
    """
    if not isinstance(body, dict):
        raise _refuse("body must be an object")
    key = _safe_award_key(body.get("award_key"))

    t0 = time.monotonic()
    async with httpx.AsyncClient(timeout=90.0) as client:
        state_payload = await _run_sidecar(client, f"""
SELECT award_id_piid, recipient_uei, recipient_name, naics_code,
       product_or_service_code, award_topology, idv_type_code, award_type_code,
       type_of_set_aside_code, awarding_agency_code, awarding_sub_agency_code,
       greatest(coalesce(life_to_date_obligated,0),0) AS obligated,
       greatest(coalesce(current_total_value_of_award,0),0) AS current_value,
       greatest(coalesce(potential_ceiling,0),0) AS potential_ceiling,
       remaining_ceiling_headroom, consumed_pct,
       pop_start_date, current_end_date, potential_end_date, days_to_expiry,
       first_action_date, last_action_date, is_terminated,
       latest_pricing_code, latest_financing_code, latest_business_size, latest_plan,
       parent_award_id_piid, parent_awarding_agency_code, parent_idv_type_code,
       parent_award_type_code, parent_match_flag
FROM prime_award_state_by_key
WHERE award_key_pfx = substr('{key}', 10, 12) AND contract_award_unique_key = '{key}'""", limit=1)
        s = _row(state_payload)
        if not s:
            raise HTTPException(status_code=404, detail=f"unknown award '{key}'")
        naics = s.get("naics_code")
        psc = s.get("product_or_service_code")
        # Build-cycle 2026-07-21: every per-award probe rides an award-key-
        # sorted companion table (prime_award_state_by_key, txn_events_combo_
        # by_award, txn_rows_by_award, award_pop_centroids_by_key) — the #1299
        # uei-pruning legs are gone; key probes prune zone maps directly.

        lang_sql = f"""
SELECT lp.naics_title, lp.psc_title, lp.work_summary,
       d.what_was_done, d.work_type, d.regime,
       n.naics_title AS naics_name_fallback, p.psc_name AS psc_name_fallback
FROM (SELECT 1) one
LEFT JOIN naics_psc_labor_profile lp ON lp.naics_code = '{naics}' AND lp.psc_code = '{psc}'
LEFT JOIN naics_psc_deliverable d ON d.naics_code = '{naics}' AND d.psc_code = '{psc}'
LEFT JOIN v_naics_names n ON n.naics_code = '{naics}'
LEFT JOIN v_psc_names p ON p.psc_code = '{psc}'"""
        fy_sql = f"""
SELECT fy, round(sum(obligation),0) AS obligated, count(*) AS actions
FROM txn_events_combo_by_award
WHERE award_key_pfx = substr('{key}', 10, 12) AND award_key = '{key}' GROUP BY 1 ORDER BY 1"""
        recent_sql = f"""
SELECT action_date, action_type_description, round(federal_action_obligation,0) AS obligation
FROM txn_rows_by_award
WHERE award_key_pfx = substr('{key}', 10, 12) AND contract_award_unique_key = '{key}'
ORDER BY action_date DESC LIMIT 8"""
        vocab_sql = "SELECT field, code, name FROM fpds_code_vocab"
        agency_sql = f"""
SELECT (SELECT any_value(name) FROM agency_vocab WHERE code = '{s.get("awarding_agency_code")}') AS agency,
       (SELECT any_value(name) FROM agency_sub_vocab WHERE code = '{s.get("awarding_sub_agency_code")}') AS sub_agency,
       (SELECT any_value(name) FROM agency_vocab WHERE code = '{s.get("parent_awarding_agency_code")}') AS parent_agency"""
        subout_sql = f"""
SELECT sub_ct, distinct_subs, round(sub_amount_total,0) AS sub_amount_total,
       first_sub_date, last_sub_date
FROM award_subout_rollup WHERE prime_award_unique_key = '{key}'"""
        place_sql = f"""
SELECT (SELECT zip5 FROM award_pop_centroids_by_key WHERE award_key_pfx = substr('{key}', 10, 12) AND generated_unique_award_id = '{key}' LIMIT 1) AS zip5,
       (SELECT state_code FROM award_pop_centroids_by_key WHERE award_key_pfx = substr('{key}', 10, 12) AND generated_unique_award_id = '{key}' LIMIT 1) AS state_code,
       (SELECT pop_county_name FROM txn_events_combo_by_award WHERE award_key_pfx = substr('{key}', 10, 12) AND award_key = '{key}'
          AND pop_county_name IS NOT NULL ORDER BY action_date DESC LIMIT 1) AS county,
       (SELECT pop_country_code FROM txn_events_combo_by_award WHERE award_key_pfx = substr('{key}', 10, 12) AND award_key = '{key}'
          ORDER BY action_date DESC LIMIT 1) AS country_code"""

        results = await asyncio.gather(
            _run_sidecar(client, lang_sql, limit=1),
            _run_sidecar(client, fy_sql, limit=40),
            _run_sidecar(client, recent_sql, limit=8),
            _run_sidecar(client, vocab_sql, limit=200),
            _run_sidecar(client, agency_sql, limit=1),
            _run_sidecar(client, subout_sql, limit=1),
            _run_sidecar(client, place_sql, limit=1),
            _award_jtbd(str(naics) if naics else None, str(psc) if psc else None),
        )
    lang = _row(results[0])
    by_fy = _rows(results[1])
    recent = _rows(results[2])
    vocab = {(r["field"], r["code"]): r["name"] for r in _rows(results[3])}
    agencies = _row(results[4])
    subout = _row(results[5]) or None
    place = _row(results[6])
    jtbd = results[7]

    def vname(field: str, code: Any) -> str | None:
        return vocab.get((field, str(code))) if code is not None else None

    fin_code = s.get("latest_financing_code")
    unfin = (fin_code is None) or (str(fin_code) in ("Z", "NOT APPLICABLE"))
    obligated = float(s.get("obligated") or 0)
    current_value = float(s.get("current_value") or 0)

    return {
        "award_key": key,
        "header": {
            "piid": s.get("award_id_piid"),
            "recipient_name": s.get("recipient_name"),
            "uei": s.get("recipient_uei"),
            "agency": agencies.get("agency"),
            "sub_agency": agencies.get("sub_agency"),
            "topology": s.get("award_topology"),
            "award_type_code": s.get("award_type_code"),
            "idv_type_code": s.get("idv_type_code"),
            "set_aside_code": s.get("type_of_set_aside_code"),
            "is_terminated": s.get("is_terminated"),
        },
        "work": {
            "naics": {"code": naics, "title": lang.get("naics_title") or lang.get("naics_name_fallback")},
            "psc": {"code": psc, "title": lang.get("psc_title") or lang.get("psc_name_fallback")},
            "work_summary": lang.get("work_summary"),
            "what_was_done": lang.get("what_was_done"),
            "work_type": lang.get("work_type"),
            "regime": lang.get("regime"),
            "jtbd": jtbd,
        },
        "money": {
            "obligated": round(obligated),
            "current_value": round(current_value),
            "potential_ceiling": round(float(s.get("potential_ceiling") or 0)),
            "remaining_ceiling_headroom": s.get("remaining_ceiling_headroom"),
            "consumed_pct": s.get("consumed_pct"),
            "unfunded_value": round(max(current_value - obligated, 0)),
        },
        "payment": {
            "pricing": {"code": s.get("latest_pricing_code"),
                        "name": vname("pricing", s.get("latest_pricing_code"))},
            "financing": {"code": fin_code, "name": vname("financing", fin_code)},
            "unfinanced": unfin,
            "business_size": {"code": s.get("latest_business_size"),
                              "name": vname("business_size_determination", s.get("latest_business_size"))},
            "subcontracting_plan": s.get("latest_plan"),
        },
        "clock": {
            "pop_start": s.get("pop_start_date"),
            "current_end": s.get("current_end_date"),
            "potential_end": s.get("potential_end_date"),
            "days_to_expiry": s.get("days_to_expiry"),
            "first_action": s.get("first_action_date"),
            "last_action": s.get("last_action_date"),
        },
        "ledger": {"by_fy": by_fy, "recent": recent},
        "vehicle": {
            "parent_piid": s.get("parent_award_id_piid"),
            "parent_agency": agencies.get("parent_agency"),
            "parent_idv_type_code": s.get("parent_idv_type_code"),
            "parent_award_type_code": s.get("parent_award_type_code"),
            "parent_match_flag": s.get("parent_match_flag"),
        },
        "subcontracting": subout,
        "place": place,
        "elapsed_ms": round((time.monotonic() - t0) * 1000, 1),
        "artifact": state_payload.get("artifact"),
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
    expr, echoes, disclosures = _compile_body_predicates(body)
    basis = _parse_basis(body)
    rollup = _parse_rollup(body)
    when = _parse_when(body)
    # Family grain folds over the FULL cohort, then pages — a page-then-fold
    # would under-count family members.
    inner_limit = 1_000_000 if rollup == "family" else limit

    t0 = time.monotonic()
    scope = await _resolve_scope_union(body)
    # No band unless the caller sends one (operator-ruled 2026-07-22): the
    # Explore surface reads the whole market; the band is a rail dial.
    band = _parse_band(body) if body.get("band") is not None else None
    if when is not None:
        # The WON frame: membership + money = obligations INTRODUCED in the
        # window on the in-scope combos. Basis does not apply (introductions
        # precede financing composition); band stays a holder property.
        if scope is None and expr is None:
            raise _refuse(
                "slug(s) or predicates required (an unscoped read of the whole universe is refused)"
            )
        a, b = when["fy_start"], when["fy_end"]
        ent_sql, count_sql = build_won_entities_sql(
            scope["pairs"] if scope else None, band, expr, inner_limit, when)
        money_basis = f"won_fy{a}_fy{b}"
        title, lens = (scope["title"], scope["lens"]) if scope else (None, None)
        disclosures = [*disclosures,
                       f"won frame: dollars are obligations introduced FY{a}–FY{b} on the in-scope "
                       "combos (txn grain); membership = any such introduction; the financing basis "
                       "does not apply in this frame"]
    elif scope is not None:
        ent_sql = build_entities_sql(scope["pairs"], band, expr, inner_limit, basis=basis)
        count_sql = build_count_sql(scope["pairs"], band, expr, basis=basis)
        money_basis = "unfinanced_in_scope" if basis == "unfinanced" else "active_in_scope"
        title, lens = scope["title"], scope["lens"]
    else:
        if expr is None:
            raise _refuse(
                "slug or predicates required (an unscoped read of the whole universe is refused)"
            )
        ent_sql, count_sql = build_grammar_entities_sql(expr, band, inner_limit)
        money_basis = "active_obligations"
        title, lens = None, None
    if rollup == "family":
        folded = wrap_family_rollup(ent_sql)
        ent_sql = folded + f"\nLIMIT {limit}"
        count_sql = f"SELECT count(*) AS firms FROM ({folded}) fam"
        disclosures = [*disclosures,
                       "family rollup: one row per ultimate parent (entity_hierarchy); money summed "
                       "across members; the row keyed and geocoded at the family head"]

    async with httpx.AsyncClient(timeout=90.0) as client:
        ent_payload, count_payload = await asyncio.gather(
            _run_sidecar(client, ent_sql, limit=limit),
            _run_sidecar(client, count_sql, limit=1),
        )
    rows = _rows(ent_payload)
    return {
        "slug": str(body.get("slug") or "") or None,
        "slugs": body.get("slugs") if isinstance(body.get("slugs"), list) else None,
        "title": title,
        "lens": lens,
        "band": band,
        "when": when,
        "rollup": rollup,
        "predicates": echoes,
        "disclosures": [*disclosures, _GEO_DISCLOSURE],
        "money_basis": money_basis,
        "entities": rows,
        "returned": len(rows),
        "count": _row(count_payload).get("firms", 0),
        "elapsed_ms": round((time.monotonic() - t0) * 1000, 1),
        "artifact": ent_payload.get("artifact"),
    }


# ── the firm profile (the award-drawer FLIP read) ─────────────────────────────
#
# The award⇄firm flip: everything the substrate knows about ONE firm, grouped
# into presentation-ready sections. Every leg is a point-read on a uei-sorted
# mart, so unlike /award (award-key probes on non-award-sorted txn tables,
# ~13s cold) this profile is ms-class end to end. FY-won series is capped at
# FY2025 — FY2026 does not exist on any camera-facing surface (operator
# ruling 2026-07-21).

_FIRM_FY_START = 2001
_FIRM_FY_END = 2025

_POC_DISCLOSURE = (
    "people are served from gtm_person_channels (SAM people x enrichment "
    "contactability): email/phone/linkedin where resolved, else name/title only"
)


def _safe_uei(raw: Any) -> str:
    uei = str(raw or "").strip().upper()
    if not _re.match(r"^[A-Z0-9]{12}$", uei):
        raise _refuse("uei must be a 12-char alphanumeric UEI")
    return uei


@router.post("/firm", dependencies=[Depends(require_service_token)])
async def firm_profile(body: dict[str, Any]) -> dict[str, Any]:
    """One firm's profile — sectioned for the Explore award-drawer flip.

    Body: {"uei": "<UEI>"}. Sections: identity, posture (active book +
    committed/vehicle split), mix (pricing x financing composition of the
    active book), fy_won (prime $ won per completed FY, 2001-2025), agencies
    (who funds them, by $), sub_work (their subawardee side), contacts
    (SAM POCs + contactability coverage). Artifact-stamped.
    """
    if not isinstance(body, dict):
        raise _refuse("body must be an object")
    uei = _safe_uei(body.get("uei"))

    core_sql = f"""
SELECT e.legal_business_name, e.physical_city, e.physical_state, e.physical_zip,
       e.primary_naics, e.sam_is_active, e.in_dsbs, e.cage_code, e.normalized_domain,
       f.industry, f.employee_size_range, f.year_founded, f.linkedin_slug,
       b.prime_obl_24mo, b.prime_obl_lifetime, b.sub_amt_24mo, b.sub_amt_lifetime,
       b.active_award_ct, b.active_obl, b.last_action_date, b.top_naics, b.top_agency_code,
       (SELECT any_value(name) FROM agency_vocab WHERE code = b.top_agency_code) AS top_agency_name,
       k.committed_award_ct, k.committed_value, k.committed_obligated, k.committed_runway,
       k.committed_award_median, k.vehicle_ct, k.vehicle_ceiling,
       k.next_committed_end_date, k.active_agency_ct,
       p.active_fixed_share, p.active_financed_share, p.active_ffp_unfinanced_share,
       p.active_obl_fixed, p.active_obl_cost, p.active_obl_tm_lh, p.active_obl_other,
       p.active_obl_fin_unfin, p.active_obl_fin_prog, p.active_obl_fin_perf,
       p.active_obl_fin_comm, p.active_obl_fin_othfin,
       a.dsbs_8a, a.dsbs_hubzone, a.dsbs_wosb, a.dsbs_sdvosb,
       a.n_dialable, a.n_emailable, a.total_amt_24mo, a.total_amt_lifetime
FROM gtm_sam_entities e
LEFT JOIN gtm_entity_firmographics f ON f.uei = e.uei
LEFT JOIN gtm_entity_behavior_rollup b ON b.uei = e.uei
LEFT JOIN gtm_entity_award_book k ON k.uei = e.uei
LEFT JOIN gtm_entity_pricing_mix p ON p.uei = e.uei
LEFT JOIN gtm_audience_entities a ON a.uei = e.uei
WHERE e.uei = '{uei}'"""

    fy_sql = f"""
SELECT fy, won_obl, award_ct, action_ct, won_obl_set_aside
FROM gtm_entity_fy_won
WHERE uei = '{uei}' AND fy BETWEEN {_FIRM_FY_START} AND {_FIRM_FY_END}
ORDER BY fy"""

    agency_sql = f"""
SELECT t.awarding_agency_code AS code, any_value(v.name) AS name,
       SUM(t.obligation) AS obligated, COUNT(*) AS actions
FROM gtm_txn_events_slim t
LEFT JOIN agency_vocab v ON v.code = t.awarding_agency_code
WHERE t.uei = '{uei}'
GROUP BY 1 ORDER BY 3 DESC LIMIT 8"""

    sub_sql = f"""
SELECT COUNT(*) AS subaward_ct, SUM(subaward_amount_num) AS subaward_amount,
       COUNT(DISTINCT prime_awardee_uei) AS prime_ct
FROM subaward_canonical_slim_by_sub
WHERE subawardee_uei = '{uei}'"""

    sub_primes_sql = f"""
SELECT s.prime_awardee_uei AS uei, any_value(e.legal_business_name) AS name,
       SUM(s.subaward_amount_num) AS amount, COUNT(*) AS subawards
FROM subaward_canonical_slim_by_sub s
LEFT JOIN gtm_sam_entities e ON e.uei = s.prime_awardee_uei
WHERE s.subawardee_uei = '{uei}'
GROUP BY 1 ORDER BY 3 DESC LIMIT 5"""

    poc_sql = f"""
SELECT display_name, first_name, last_name, best_title AS title,
       email, email_verification_status, phone, phone_status,
       person_linkedin_url_norm AS linkedin, is_govt_poc, is_ebiz_poc, n_sources
FROM gtm_person_channels
WHERE uei = '{uei}'
ORDER BY n_sources DESC, sam_person_id LIMIT 24"""

    t0 = time.monotonic()
    async with httpx.AsyncClient(timeout=90.0) as client:
        core_p, fy_p, agency_p, sub_p, sub_primes_p, poc_p = await asyncio.gather(
            _run_sidecar(client, core_sql, limit=1),
            _run_sidecar(client, fy_sql, limit=64),
            _run_sidecar(client, agency_sql, limit=8),
            _run_sidecar(client, sub_sql, limit=1),
            _run_sidecar(client, sub_primes_sql, limit=5),
            _run_sidecar(client, poc_sql, limit=24),
        )
    c = _row(core_p)
    if not c:
        raise HTTPException(status_code=404, detail=f"unknown uei '{uei}'")
    sub = _row(sub_p)

    return {
        "uei": uei,
        "identity": {
            "legal_business_name": c.get("legal_business_name"),
            "city": c.get("physical_city"),
            "state": c.get("physical_state"),
            "zip": c.get("physical_zip"),
            "primary_naics": c.get("primary_naics"),
            "cage_code": c.get("cage_code"),
            "domain": c.get("normalized_domain"),
            "linkedin_slug": c.get("linkedin_slug"),
            "sam_is_active": c.get("sam_is_active"),
            "in_dsbs": c.get("in_dsbs"),
            "industry": c.get("industry"),
            "employee_size_range": c.get("employee_size_range"),
            "year_founded": c.get("year_founded"),
            "designations": {
                "dsbs_8a": c.get("dsbs_8a"),
                "dsbs_hubzone": c.get("dsbs_hubzone"),
                "dsbs_wosb": c.get("dsbs_wosb"),
                "dsbs_sdvosb": c.get("dsbs_sdvosb"),
            },
        },
        "posture": {
            "active_obl": c.get("active_obl"),
            "active_award_ct": c.get("active_award_ct"),
            "committed_award_ct": c.get("committed_award_ct"),
            "committed_value": c.get("committed_value"),
            "committed_obligated": c.get("committed_obligated"),
            "committed_runway": c.get("committed_runway"),
            "committed_award_median": c.get("committed_award_median"),
            "vehicle_ct": c.get("vehicle_ct"),
            "vehicle_ceiling": c.get("vehicle_ceiling"),
            "next_committed_end_date": c.get("next_committed_end_date"),
            "active_agency_ct": c.get("active_agency_ct"),
            "prime_obl_24mo": c.get("prime_obl_24mo"),
            "prime_obl_lifetime": c.get("prime_obl_lifetime"),
            "total_amt_24mo": c.get("total_amt_24mo"),
            "total_amt_lifetime": c.get("total_amt_lifetime"),
            "last_action_date": c.get("last_action_date"),
            "top_naics": c.get("top_naics"),
            "top_agency_code": c.get("top_agency_code"),
            "top_agency_name": c.get("top_agency_name"),
        },
        "mix": {
            "active_fixed_share": c.get("active_fixed_share"),
            "active_financed_share": c.get("active_financed_share"),
            "active_ffp_unfinanced_share": c.get("active_ffp_unfinanced_share"),
            "by_pricing": {
                "fixed": c.get("active_obl_fixed"),
                "cost": c.get("active_obl_cost"),
                "tm_lh": c.get("active_obl_tm_lh"),
                "other": c.get("active_obl_other"),
            },
            "by_financing": {
                "unfinanced": c.get("active_obl_fin_unfin"),
                "progress": c.get("active_obl_fin_prog"),
                "performance": c.get("active_obl_fin_perf"),
                "commercial": c.get("active_obl_fin_comm"),
                "other_financed": c.get("active_obl_fin_othfin"),
            },
        },
        "fy_won": _rows(fy_p),
        "agencies": _rows(agency_p),
        "sub_work": {
            "subaward_ct": sub.get("subaward_ct", 0),
            "subaward_amount": sub.get("subaward_amount"),
            "prime_ct": sub.get("prime_ct", 0),
            "sub_amt_24mo": c.get("sub_amt_24mo"),
            "sub_amt_lifetime": c.get("sub_amt_lifetime"),
            "top_primes": _rows(sub_primes_p),
        },
        "contacts": {
            "pocs": _rows(poc_p),
            "n_dialable": c.get("n_dialable"),
            "n_emailable": c.get("n_emailable"),
        },
        "disclosures": [_POC_DISCLOSURE],
        "fy_range": [_FIRM_FY_START, _FIRM_FY_END],
        "elapsed_ms": round((time.monotonic() - t0) * 1000, 1),
        "artifact": core_p.get("artifact"),
    }
