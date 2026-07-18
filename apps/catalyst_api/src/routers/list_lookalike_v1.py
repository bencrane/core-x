"""list-lookalike — expansion market composed from a customer list's prime record (gc-hq).

Operator ruling (2026-07-18 session): the PRIMARY lens is demonstrated prime
behavior — the signature is what the customer firms have actually been paid to
do as primes, at (NAICS, PSC) combo grain. The sub side is a SEPARATE,
secondary lens: firms that receive subawards under the same work ("under what
work they sub" is the instructive frame for sub-heavy candidates); it is never
blended into the prime ranking.

Three uei/combo probes per request (measured 2026-07-18, 20-firm list:
41ms + 766ms + 228ms cold):

  1. signature   — the list's prime combo record from `gtm_prime_combo_lanes`,
                   aggregated per (naics, psc): distinct customer firms +
                   obligations. NULL-code lanes and placeholder PSC '9999'
                   excluded (disclosed). Top combos by (firms, obligations).
  2. prime lens  — firms NOT on the list with prime obligations under the
                   signature combos. Score = Σ over matched combos of that
                   combo's customer-firm share (combos common among the
                   customers weigh more); ties broken by matched obligations.
                   Firms already receiving subawards under a customer are
                   flagged (prime→sub edge rollup) — a strong reason, shown,
                   never silently boosting rank.
  3. sub lens    — firms receiving subawards under the signature combos
                   (`gtm_sub_combo_lanes`), with their own prime record
                   alongside so "mostly subs" is visible, ranked by matched
                   combos then subaward dollars.

The signature doubles as a market definition: its pairs compile directly to
the market-query predicate `obligations_under_naics_psc_pairs` — saving the
lookalike market reuses the existing predicates grammar, no new grammar.

Endpoint (service-token gated):
  POST /api/v1/list-lookalike   {"ueis": [...], "limit_prime"?, "limit_sub"?}
"""
from __future__ import annotations

import logging
import os
import re
from typing import Any

import httpx
from fastapi import APIRouter, Depends, HTTPException

from ..service_token import require_service_token

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/list-lookalike", tags=["list-lookalike"])

SIDECAR_URL = os.environ.get("QUERY_SIDECAR_URL", "https://query-sidecar-api.onrender.com")

_UEI_RE = re.compile(r"^[A-Za-z0-9]{12}$")
_CODE_RE = re.compile(r"^[A-Za-z0-9]{1,6}$")
_MAX_BATCH = 2000
_SIGNATURE_COMBOS = 25
_DEFAULT_PRIME_LIMIT = 100
_DEFAULT_SUB_LIMIT = 50
_MAX_LIMIT = 500


def _values_clause(items: list[str]) -> str:
    # validated against closed shapes above — safe to embed.
    return ", ".join(f"('{i}')" for i in items)


async def _sidecar(client: httpx.AsyncClient, sql: str, limit: int) -> dict[str, Any]:
    token = os.environ.get("QUERY_SIDECAR_TOKEN")
    if not token:
        raise HTTPException(status_code=503, detail="QUERY_SIDECAR_TOKEN not configured")
    r = await client.post(
        f"{SIDECAR_URL}/api/v1/sql",
        headers={"Authorization": f"Bearer {token}"},
        json={"sql": sql, "limit": limit},
    )
    if r.status_code != 200:
        logger.error("sidecar list-lookalike failed: %s %s", r.status_code, r.text[:300])
        raise HTTPException(status_code=502, detail="sidecar query failed")
    return r.json()


def _rows_as_dicts(payload: dict[str, Any]) -> list[dict[str, Any]]:
    cols = payload.get("columns") or []
    return [dict(zip(cols, row)) for row in payload.get("rows") or []]


def _sql_signature(cvals: str) -> str:
    return (
        f"WITH u(uei) AS (VALUES {cvals}) "
        "SELECT l.naics_code, l.psc_code, "
        "any_value(l.naics_title) AS naics_title, any_value(l.psc_title) AS psc_title, "
        "COUNT(DISTINCT l.uei) AS customer_firms, "
        "SUM(l.prime_obl_lifetime) AS customer_obl_lifetime, "
        "SUM(l.prime_obl_24mo) AS customer_obl_24mo "
        "FROM u JOIN gtm_prime_combo_lanes l ON l.uei = u.uei "
        "WHERE l.naics_code IS NOT NULL AND l.psc_code IS NOT NULL AND l.psc_code <> '9999' "
        "GROUP BY 1, 2 "
        f"ORDER BY customer_firms DESC, customer_obl_lifetime DESC LIMIT {_SIGNATURE_COMBOS}"
    )


def _sql_prime_active_customer_ct(cvals: str) -> str:
    return (
        f"WITH u(uei) AS (VALUES {cvals}) "
        "SELECT COUNT(DISTINCT l.uei) AS ct FROM u JOIN gtm_prime_combo_lanes l ON l.uei = u.uei"
    )


def _sql_prime_lens(cvals: str, weighted_pairs: str, limit: int) -> str:
    # weighted_pairs rows: ('naics','psc', <customer_share as float literal>)
    return (
        f"WITH u(uei) AS (VALUES {cvals}), "
        f"sig(naics, psc, weight) AS (VALUES {weighted_pairs}), "
        "under_customers AS ("
        "  SELECT p.sub_uei, COUNT(DISTINCT p.prime_uei) AS customer_primes, "
        "         SUM(p.edge_dollars_lifetime) AS customer_sub_dollars "
        "  FROM gtm_prime_sub_pairs p JOIN u ON u.uei = p.prime_uei GROUP BY 1"
        "), hits AS ("
        "  SELECT l.uei, COUNT(*) AS matched_combos, SUM(s.weight) AS score, "
        "         SUM(l.prime_obl_lifetime) AS matched_obl_lifetime, "
        "         SUM(l.prime_obl_24mo) AS matched_obl_24mo, "
        "         list(l.naics_code || '/' || l.psc_code ORDER BY l.prime_obl_lifetime DESC) AS matched_pairs "
        "  FROM gtm_prime_combo_lanes l JOIN sig s ON s.naics = l.naics_code AND s.psc = l.psc_code "
        "  WHERE l.uei NOT IN (SELECT uei FROM u) GROUP BY 1"
        ") "
        "SELECT h.uei, e.legal_business_name, e.physical_state, "
        "h.score, h.matched_combos, h.matched_pairs[:5] AS matched_pairs, "
        "h.matched_obl_lifetime, h.matched_obl_24mo, "
        "b.prime_obl_lifetime AS own_prime_obl_lifetime, b.active_award_ct, "
        "f.employee_size_range, "
        "uc.customer_primes AS subs_under_customer_ct, uc.customer_sub_dollars "
        "FROM hits h "
        "JOIN gtm_sam_entities e ON e.uei = h.uei "
        "LEFT JOIN gtm_entity_behavior_rollup b ON b.uei = h.uei "
        "LEFT JOIN gtm_entity_firmographics f ON f.uei = h.uei "
        "LEFT JOIN under_customers uc ON uc.sub_uei = h.uei "
        "WHERE e.legal_business_name NOT LIKE 'MISCELLANEOUS%' "
        f"ORDER BY h.score DESC, h.matched_obl_lifetime DESC LIMIT {limit}"
    )


def _sql_sub_lens(cvals: str, pairs: str, limit: int) -> str:
    return (
        f"WITH u(uei) AS (VALUES {cvals}), sig(naics, psc) AS (VALUES {pairs}), "
        "under_customers AS ("
        "  SELECT p.sub_uei, COUNT(DISTINCT p.prime_uei) AS customer_primes "
        "  FROM gtm_prime_sub_pairs p JOIN u ON u.uei = p.prime_uei GROUP BY 1"
        "), hits AS ("
        "  SELECT l.uei, COUNT(*) AS matched_combos, "
        "         SUM(l.sub_amt_lifetime) AS matched_sub_amt_lifetime, "
        "         SUM(l.n_distinct_primes_lifetime) AS distinct_prime_relationships, "
        "         list(l.naics_code || '/' || l.psc_code ORDER BY l.sub_amt_lifetime DESC) AS matched_pairs "
        "  FROM gtm_sub_combo_lanes l JOIN sig s ON s.naics = l.naics_code AND s.psc = l.psc_code "
        "  WHERE l.uei NOT IN (SELECT uei FROM u) GROUP BY 1"
        ") "
        "SELECT h.uei, e.legal_business_name, e.physical_state, "
        "h.matched_combos, h.matched_pairs[:5] AS matched_pairs, h.matched_sub_amt_lifetime, "
        "h.distinct_prime_relationships, "
        "coalesce(b.prime_obl_lifetime, 0) AS own_prime_obl_lifetime, "
        "f.employee_size_range, uc.customer_primes AS subs_under_customer_ct "
        "FROM hits h "
        "JOIN gtm_sam_entities e ON e.uei = h.uei "
        "LEFT JOIN gtm_entity_behavior_rollup b ON b.uei = h.uei "
        "LEFT JOIN gtm_entity_firmographics f ON f.uei = h.uei "
        "LEFT JOIN under_customers uc ON uc.sub_uei = h.uei "
        "WHERE e.legal_business_name NOT LIKE 'MISCELLANEOUS%' "
        f"ORDER BY h.matched_combos DESC, h.matched_sub_amt_lifetime DESC LIMIT {limit}"
    )


DISCLOSURES = [
    "The signature is demonstrated prime performance: obligations the customer firms actually received as primes, at (NAICS, PSC) combo grain — not SAM-declared intent.",
    "Lanes without both codes and placeholder PSC 9999 are excluded from the signature.",
    "Prime lookalikes are ranked by weighted combo overlap (combos common among more of the customers weigh more), then by matched obligations. A firm already receiving subawards under a customer is flagged, not silently boosted.",
    "The subawardee lens is separate by design: it shows firms receiving disclosed (FFATA) subawards under the same work, alongside their own prime record.",
    "Employee size comes from a commercial-data bridge covering a minority of registrants.",
    "FPDS placeholder recipients (the MISCELLANEOUS* aggregation entities) are excluded from candidates.",
]


@router.post("", dependencies=[Depends(require_service_token)])
async def list_lookalike(body: dict[str, Any]) -> dict[str, Any]:
    raw = body.get("ueis")
    if not isinstance(raw, list) or not raw:
        raise HTTPException(status_code=422, detail="ueis must be a non-empty list")
    if len(raw) > _MAX_BATCH:
        raise HTTPException(status_code=422, detail=f"batch limit {_MAX_BATCH}")
    ueis = sorted({u.strip().upper() for u in raw if isinstance(u, str) and _UEI_RE.match(u.strip())})
    if not ueis:
        raise HTTPException(status_code=422, detail="no valid 12-character UEIs in ueis")

    def _limit(key: str, default: int) -> int:
        v = body.get(key, default)
        if not isinstance(v, int) or v < 1 or v > _MAX_LIMIT:
            return default
        return v

    limit_prime = _limit("limit_prime", _DEFAULT_PRIME_LIMIT)
    limit_sub = _limit("limit_sub", _DEFAULT_SUB_LIMIT)

    cvals = _values_clause(ueis)
    out: dict[str, Any] = {"elapsed_ms": 0.0, "artifact": None}

    async with httpx.AsyncClient(timeout=120.0) as client:
        async def run(sql: str, limit: int) -> list[dict[str, Any]]:
            payload = await _sidecar(client, sql, limit)
            out["elapsed_ms"] += payload.get("elapsed_ms") or 0
            out["artifact"] = payload.get("artifact") or out["artifact"]
            return _rows_as_dicts(payload)

        signature = await run(_sql_signature(cvals), _SIGNATURE_COMBOS + 5)
        if not signature:
            return {
                "customer_ct": len(ueis),
                "prime_active_customer_ct": 0,
                "signature": [],
                "prime_lookalikes": [],
                "sub_lookalikes": [],
                "disclosures": DISCLOSURES,
                **out,
            }
        active_rows = await run(_sql_prime_active_customer_ct(cvals), 5)
        prime_active = int(active_rows[0]["ct"]) if active_rows else 0

        for s in signature:
            s["customer_share"] = (
                round(s["customer_firms"] / prime_active, 4) if prime_active else 0.0
            )

        # combo codes come from the sidecar's own lanes, but re-validate against
        # the closed code shape before re-embedding.
        safe = [
            s for s in signature
            if _CODE_RE.match(str(s["naics_code"])) and _CODE_RE.match(str(s["psc_code"]))
        ]
        pairs = ", ".join(f"('{s['naics_code']}', '{s['psc_code']}')" for s in safe)
        weighted = ", ".join(
            f"('{s['naics_code']}', '{s['psc_code']}', {float(s['customer_share'])})" for s in safe
        )

        prime_lookalikes = await run(_sql_prime_lens(cvals, weighted, limit_prime), limit_prime + 10)
        sub_lookalikes = await run(_sql_sub_lens(cvals, pairs, limit_sub), limit_sub + 10)

    return {
        "customer_ct": len(ueis),
        "prime_active_customer_ct": prime_active,
        "signature": signature,
        "prime_lookalikes": prime_lookalikes,
        "sub_lookalikes": sub_lookalikes,
        "disclosures": DISCLOSURES,
        **out,
    }
