"""sub-market-slice — the sub-under demand market cards, recomputable under predicates.

The demand-side inversion of the subawardee dossier (SUBAWARDEE_DOSSIER_PROGRAM v2):
instead of firm-first (a page about one UEI), a STANDING market card per major
prime-combo family that subawardees sub under — the outreach object that never
has to describe what a subawardee does. On-call the card narrows per-UEI via the
EXISTING `/api/v1/sub-dossier/build` (seeds → seed-lookalike primes → co-sub
triangle); this router serves only the standing card + its predicate slices.

Scope: explicit prime-combo pair-sets in ``sub_market_card_scopes.json``
(Phase-0 composition, ≥99% family coverage, artifact-stamped + adversarially
verified). Fact table: ``subaward_canonical_slim_by_sub`` (FSRS disclosed
subawards, 1.3M rows — whole-table scans are ms-class; no sort dependency).

Design law (unchanged from market_slice_v1): predicates are the ONLY filter
vocabulary — ``compile_predicates`` imported, never reimplemented; the
predicate uei-set restricts the SUBAWARDEE cohort (state, size, designations…
all apply to the subs). Aggregation SQL is bespoke to the card anatomy.

Endpoints (service-token gated):
  GET  /api/v1/sub-market-slice/catalog                → the 6 card scopes
  POST /api/v1/sub-market-slice/pack  {"slug", "predicates"?} → the anatomy pack
  POST /api/v1/sub-market-slice/count {"slug", "predicates"?} → {"count"} (subs)

Pack sections (concurrent): tiles (sub $ FY23-25 + trailing-12mo, subaward/sub/
prime counts, chunk texture p20/median/p80, DoD share), top_primes (by farm-out
$ in scope — name-reveal is a SURFACE dial, the payload always carries names),
monthly_series (36mo flow), states (sub HQ), top_pairs (work language).
Disclosures: FSRS = disclosed subawards only; ~3.9% of universe $ lacks combo
codes; DoD share via awarding-agency name prefix. Self-subawards
(subawardee == prime awardee) are excluded everywhere — sub-dossier parity
(review finding 2026-07-20; composition re-stamped with the same guard).
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
from .market_query_v1 import compile_predicates

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/sub-market-slice", tags=["sub-market-slice"])

SIDECAR_URL = os.environ.get("QUERY_SIDECAR_URL", "https://query-sidecar-api.onrender.com")

_SCOPES_PATH = Path(__file__).resolve().parent.parent / "sub_market_card_scopes.json"
_scopes: dict[str, Any] | None = None

_WINDOW = "subaward_action_date BETWEEN DATE '2022-10-01' AND DATE '2025-09-30'"
_DOD = "prime_award_awarding_agency_name LIKE 'Department of Defense%'"

_DISCLOSURES = [
    "Disclosed (FSRS-reported) subawards only — undisclosed teaming is invisible to this record.",
    "Window FY2023-FY2025; trailing-12mo tiles measure the 12 months before the window end.",
    "~3.9% of universe subaward dollars carry no prime combo codes and cannot enter any card scope.",
]


def _load_scopes() -> dict[str, Any]:
    global _scopes
    if _scopes is None:
        _scopes = json.loads(_SCOPES_PATH.read_text())
    return _scopes


def _refuse(detail: str) -> HTTPException:
    return HTTPException(status_code=422, detail=detail)


def _resolve(slug: str) -> dict[str, Any]:
    card = _load_scopes()["cards"].get(slug)
    if card is None:
        raise _refuse(f"unknown card '{slug}' (GET /api/v1/sub-market-slice/catalog lists the set)")
    return card


def _scope_pairs(card: dict[str, Any], body: dict[str, Any]) -> tuple[list[tuple[str, str]], str | None]:
    """The pack scope: the whole card, or one named cohort (a pair-subset —
    the population-framed 'firms like you' cut; v1.1 cohort segments)."""
    cohort = body.get("cohort")
    if cohort in (None, ""):
        return [tuple(p) for p in card["pairs"]], None
    co = (card.get("cohorts") or {}).get(str(cohort))
    if co is None:
        known = ", ".join(sorted((card.get("cohorts") or {}).keys())) or "none"
        raise _refuse(f"unknown cohort '{cohort}' for this card (known: {known})")
    return [tuple(p) for p in co["pairs"]], str(cohort)


def _compile_body_predicates(body: dict[str, Any]) -> tuple[str | None, list[dict], list[str]]:
    preds = body.get("predicates")
    if preds in (None, []):
        return None, [], []
    if not isinstance(preds, list):
        raise _refuse("predicates must be a list of {term, ...dials} objects")
    return compile_predicates({"predicates": preds})


def build_cohort_cte(pairs: list[tuple[str, str]], predicate_expr: str | None) -> str:
    """The scoped-rows CTE shared by every pack section and the count SQL."""
    values = ",".join(f"('{n}','{p}')" for n, p in pairs)
    pred_leg = (
        f"\n    AND s.subawardee_uei IN (\n{predicate_expr}\n)" if predicate_expr else ""
    )
    return f"""
WITH pairs(naics_code, psc_code) AS (VALUES {values}),
t AS (
  SELECT s.subawardee_uei AS uei, s.prime_awardee_uei AS prime_uei,
         s.prime_awardee_name AS prime_name,
         s.subaward_amount_num AS amt, s.subaward_action_date AS d,
         s.prime_award_naics_code AS naics, s.prime_award_product_or_service_code AS psc,
         s.subawardee_state_code AS sub_state,
         ({_DOD}) AS dod
  FROM subaward_canonical_slim_by_sub s
  JOIN pairs pr ON s.prime_award_naics_code = pr.naics_code
               AND s.prime_award_product_or_service_code = pr.psc_code
  WHERE {_WINDOW} AND s.subaward_amount_num > 0
    AND s.subawardee_uei <> s.prime_awardee_uei{pred_leg}
)"""


def build_pack_sql(pairs: list[tuple[str, str]], predicate_expr: str | None) -> dict[str, str]:
    """Pure SQL assembly — every section shares the scoped-rows CTE."""
    cohort = build_cohort_cte(pairs, predicate_expr)
    return {
        "tiles": cohort + """
SELECT round(coalesce(sum(amt), 0), 0)                                   AS sub_usd,
       round(coalesce(sum(amt) FILTER (WHERE d >= DATE '2024-10-01'), 0), 0) AS sub_usd_12mo,
       count(*)                                                          AS n_subawards,
       count(DISTINCT uei)                                               AS n_subs,
       count(DISTINCT prime_uei)                                         AS n_primes,
       round(coalesce(median(amt), 0), 0)                                AS median_chunk,
       round(coalesce(quantile_cont(amt, 0.2), 0), 0)                    AS p20_chunk,
       round(coalesce(quantile_cont(amt, 0.8), 0), 0)                    AS p80_chunk,
       round(100.0 * sum(amt) FILTER (WHERE dod) / nullif(sum(amt), 0), 1) AS dod_pct
FROM t""",
        "top_primes": cohort + """
SELECT prime_uei, any_value(prime_name) AS name,
       round(sum(amt), 0) AS sub_usd,
       count(DISTINCT uei) AS n_subs,
       round(median(amt), 0) AS median_chunk
FROM t GROUP BY 1 ORDER BY 3 DESC LIMIT 15""",
        "monthly_series": cohort + """
SELECT date_trunc('month', d)::DATE AS month, round(sum(amt), 0) AS sub_usd
FROM t WHERE d >= DATE '2022-10-01'
GROUP BY 1 ORDER BY 1""",
        "states": cohort + """
SELECT sub_state AS state, count(DISTINCT uei) AS subs
FROM t WHERE sub_state IS NOT NULL
GROUP BY 1 ORDER BY 2 DESC LIMIT 12""",
        "top_pairs": cohort + """
SELECT t.naics, t.psc, round(sum(t.amt), 0) AS sub_usd,
       count(DISTINCT t.uei) AS subs,
       any_value(d2.what_was_done) AS what_was_done
FROM t LEFT JOIN naics_psc_deliverable d2
  ON d2.naics_code = t.naics AND d2.psc_code = t.psc
GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 10""",
    }


def build_count_sql(pairs: list[tuple[str, str]], predicate_expr: str | None) -> str:
    return build_cohort_cte(pairs, predicate_expr) + "\nSELECT count(DISTINCT uei) AS subs FROM t"


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
        logger.error("sidecar sub-market-slice failed: %s %s", r.status_code, r.text[:300])
        raise HTTPException(status_code=502, detail="sidecar query failed")
    return r.json()


def _row(payload: dict[str, Any]) -> dict[str, Any]:
    cols, rows = payload.get("columns") or [], payload.get("rows") or []
    return dict(zip(cols, rows[0])) if rows else {}


def _rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    cols = payload.get("columns") or []
    return [dict(zip(cols, r)) for r in payload.get("rows") or []]


@router.get("/catalog", dependencies=[Depends(require_service_token)])
async def catalog() -> dict[str, Any]:
    scopes = _load_scopes()
    return {
        "cards": [
            {"slug": slug, "title": c["title"], "members_are": c["members_are"],
             "pair_count": len(c["pairs"]), "verified": c["verified"],
             "cohorts": {
                 k: {"family_name": co["family_name"], "one_liner": co["one_liner"],
                     "language_phrases": co["language_phrases"],
                     "share_of_card_pct": co["share_of_card_pct"], "stats": co["stats"]}
                 for k, co in (c.get("cohorts") or {}).items()
             }}
            for slug, c in sorted(scopes["cards"].items())
        ],
        "window": scopes["window"],
    }


@router.post("/pack", dependencies=[Depends(require_service_token)])
async def pack(body: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(body, dict):
        raise _refuse("body must be an object")
    slug = str(body.get("slug") or "")
    if not slug:
        raise _refuse("slug is required")
    card = _resolve(slug)
    expr, echoes, disclosures = _compile_body_predicates(body)
    pairs, cohort = _scope_pairs(card, body)

    sections = build_pack_sql(pairs, expr)
    t0 = time.monotonic()
    async with httpx.AsyncClient(timeout=90.0) as client:
        results = await asyncio.gather(*[_run_sidecar(client, sql, limit=100) for sql in sections.values()])
    payloads = dict(zip(sections.keys(), results))

    tiles = _row(payloads["tiles"])
    return {
        "slug": slug,
        "title": card["title"],
        "members_are": card["members_are"],
        "cohort": cohort,
        "cohort_meta": (card.get("cohorts") or {}).get(cohort) and {
            "family_name": card["cohorts"][cohort]["family_name"],
            "one_liner": card["cohorts"][cohort]["one_liner"],
            "share_of_card_pct": card["cohorts"][cohort]["share_of_card_pct"],
        } if cohort else None,
        "pair_count": len(pairs),
        "predicates": echoes,
        "disclosures": _DISCLOSURES + disclosures,
        "tiles": tiles,
        "top_primes": _rows(payloads["top_primes"]),
        "monthly_series": _rows(payloads["monthly_series"]),
        "states": _rows(payloads["states"]),
        "top_pairs": _rows(payloads["top_pairs"]),
        "count": tiles.get("n_subs", 0),
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
    card = _resolve(slug)
    expr, echoes, disclosures = _compile_body_predicates(body)
    pairs, cohort = _scope_pairs(card, body)
    sql = build_count_sql(pairs, expr)
    async with httpx.AsyncClient(timeout=90.0) as client:
        payload = await _run_sidecar(client, sql, limit=1)
    row = _row(payload)
    return {
        "slug": slug,
        "cohort": cohort,
        "count": row.get("subs", 0),
        "predicates": echoes,
        "disclosures": _DISCLOSURES + disclosures,
        "elapsed_ms": payload.get("elapsed_ms"),
        "artifact": payload.get("artifact"),
    }
