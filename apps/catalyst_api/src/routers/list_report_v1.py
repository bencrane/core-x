"""list-report — the federal state of affairs for an uploaded entity list (gc-hq).

One request, one list of UEIs (≤2000), six pruned sidecar statements — every
mart involved is uei-sorted, so each statement is a VALUES-join probe:

  1. per-member wide row   (spine ⋈ behavior rollup ⋈ award book ⋈
                            firmographics ⋈ pricing mix ⋈ audience designations)
  2. fiscal-year series    (gtm_entity_fy_won, last 10 FYs, list-level)
  3. contract actions      (gtm_txn_recipient_month_rollup × action_type_vocab,
                            trailing 12 months, family × month, list-level)
  4. principal codes       (gtm_prime_code_signature rank≤3, list-level,
                            names via v_naics_names / v_psc_names)
  5. expiring awards       (gtm_award_expiry_months, next 24 months, list-level)
  6. awarding agencies     (month rollup, trailing 60 months, list-level)

Aggregate shares/coverage are computed HERE (deterministic Python over the
member rows) — the consumer renders; it never re-derives.

Vocabulary discipline: response field names use the government-literal terms
of the gc-hq ontology (obligations, active awards, period of performance,
IDV/vehicle, set-aside) — no invented metaphors.

Endpoint (service-token gated):
  POST /api/v1/list-report   {"ueis": [...]}   (≤2000)
"""
from __future__ import annotations

import logging
import os
import re
from datetime import date
from typing import Any

import httpx
from fastapi import APIRouter, Depends, HTTPException

from ..service_token import require_service_token

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/list-report", tags=["list-report"])

SIDECAR_URL = os.environ.get("QUERY_SIDECAR_URL", "https://query-sidecar-api.onrender.com")

_UEI_RE = re.compile(r"^[A-Za-z0-9]{12}$")
_MAX_BATCH = 2000


def _values_clause(ueis: list[str]) -> str:
    # ueis are validated against _UEI_RE (closed alnum shape) — safe to embed.
    return ", ".join(f"('{u}')" for u in ueis)


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
        logger.error("sidecar list-report failed: %s %s", r.status_code, r.text[:300])
        raise HTTPException(status_code=502, detail="sidecar query failed")
    return r.json()


def _rows_as_dicts(payload: dict[str, Any]) -> list[dict[str, Any]]:
    cols = payload.get("columns") or []
    return [dict(zip(cols, row)) for row in payload.get("rows") or []]


def _current_fy(today: date) -> int:
    return today.year + (1 if today.month >= 10 else 0)


# ── the six statements ────────────────────────────────────────────────────────

def _sql_members(vals: str) -> str:
    return (
        f"WITH u(uei) AS (VALUES {vals}) "
        "SELECT u.uei, "
        # spine
        "e.legal_business_name, e.physical_city, e.physical_state, e.primary_naics, "
        "e.sam_is_active, e.in_dsbs, e.registration_expiration_date, "
        # behavior rollup (obligations by trailing window, activity, recency)
        "b.prime_obl_24mo, b.prime_obl_lifetime, b.sub_amt_24mo, b.sub_amt_lifetime, "
        "b.active_award_ct, b.active_obl, b.pop_expiring_180d_ct, b.last_action_date, "
        "b.is_prime_24mo, b.is_sub_60mo, b.top_naics, b.top_agency_code, "
        # award book (active committed work vs vehicle capacity — never blended)
        "k.committed_award_ct AS active_award_ct_committed, "
        "k.committed_value AS current_value_of_active_awards, "
        "k.committed_runway AS remaining_current_value_of_active_awards, "
        "k.committed_award_median AS median_active_award_value, "
        "k.vehicle_ct AS open_idv_ct, k.vehicle_ceiling AS open_idv_potential_value, "
        "k.next_committed_end_date, k.active_agency_ct, "
        # firmographics (bridged coverage — disclosed)
        "f.employee_size_range, f.industry, f.year_founded, "
        # pricing mix of active obligations
        "p.active_fixed_share, p.active_financed_share, "
        # designations + contactability (audience spine)
        "a.dsbs_8a, a.dsbs_hubzone, a.dsbs_wosb, a.dsbs_sdvosb, "
        "a.n_dialable, a.n_emailable, "
        "a.total_amt_24mo, a.total_amt_lifetime "
        "FROM u "
        "LEFT JOIN gtm_sam_entities e ON e.uei = u.uei "
        "LEFT JOIN gtm_entity_behavior_rollup b ON b.uei = u.uei "
        "LEFT JOIN gtm_entity_award_book k ON k.uei = u.uei "
        "LEFT JOIN gtm_entity_firmographics f ON f.uei = u.uei "
        "LEFT JOIN gtm_entity_pricing_mix p ON p.uei = u.uei "
        "LEFT JOIN gtm_audience_entities a ON a.uei = u.uei "
        "ORDER BY coalesce(b.prime_obl_lifetime, 0) + coalesce(b.sub_amt_lifetime, 0) DESC"
    )


def _sql_fy_series(vals: str, fy_start: int) -> str:
    return (
        f"WITH u(uei) AS (VALUES {vals}) "
        "SELECT w.fy, COUNT(DISTINCT w.uei) AS firms, "
        "SUM(w.won_obl) AS obligations, SUM(w.won_obl_set_aside) AS set_aside_obligations, "
        "SUM(w.award_ct) AS awards "
        f"FROM u JOIN gtm_entity_fy_won w ON w.uei = u.uei WHERE w.fy >= {fy_start} "
        "GROUP BY w.fy ORDER BY w.fy"
    )


def _sql_actions_12mo(vals: str) -> str:
    return (
        f"WITH u(uei) AS (VALUES {vals}) "
        "SELECT date_trunc('month', r.month) AS month, coalesce(v.family, 'new_award') AS family, "
        "COUNT(DISTINCT r.uei) AS firms, SUM(r.n_actions) AS actions, "
        "SUM(r.obligation_sum) AS obligations "
        "FROM u JOIN gtm_txn_recipient_month_rollup r ON r.uei = u.uei "
        "LEFT JOIN action_type_vocab v "
        "  ON coalesce(v.action_type_code, '(base)') = coalesce(r.action_type_code, '(base)') "
        "WHERE r.month >= date_trunc('month', current_date) - INTERVAL 11 MONTH "
        "GROUP BY 1, 2 ORDER BY 1, 2"
    )


def _sql_top_codes(vals: str) -> str:
    return (
        f"WITH u(uei) AS (VALUES {vals}) "
        "SELECT s.code_type, s.code, "
        "coalesce(nn.naics_title, pn.psc_name) AS name, "
        "COUNT(DISTINCT s.uei) AS firms, SUM(s.obl_lifetime) AS obligations_lifetime, "
        "SUM(s.obl_24mo) AS obligations_24mo "
        "FROM u JOIN gtm_prime_code_signature s ON s.uei = u.uei "
        "LEFT JOIN v_naics_names nn ON s.code_type = 'naics' AND nn.naics_code = s.code "
        "LEFT JOIN v_psc_names pn ON s.code_type = 'psc' AND pn.psc_code = s.code "
        "WHERE s.rank_lifetime <= 3 "
        "GROUP BY 1, 2, 3 ORDER BY firms DESC, obligations_lifetime DESC LIMIT 40"
    )


def _sql_expiry_months(vals: str) -> str:
    return (
        f"WITH u(uei) AS (VALUES {vals}) "
        "SELECT x.end_month, COUNT(DISTINCT x.uei) AS firms, "
        "SUM(x.n_awards) AS awards, SUM(x.obligated) AS obligated "
        "FROM u JOIN gtm_award_expiry_months x ON x.uei = u.uei "
        "WHERE x.end_month >= date_trunc('month', current_date) "
        "  AND x.end_month < date_trunc('month', current_date) + INTERVAL 24 MONTH "
        "GROUP BY 1 ORDER BY 1"
    )


def _sql_agencies(vals: str) -> str:
    return (
        f"WITH u(uei) AS (VALUES {vals}) "
        "SELECT r.awarding_agency_code AS code, any_value(v.name) AS name, "
        "COUNT(DISTINCT r.uei) AS firms, SUM(r.obligation_sum) AS obligations_60mo "
        "FROM u JOIN gtm_txn_recipient_month_rollup r ON r.uei = u.uei "
        "LEFT JOIN agency_vocab v ON v.code = r.awarding_agency_code "
        "WHERE r.month >= date_trunc('month', current_date) - INTERVAL 59 MONTH "
        "  AND r.awarding_agency_code IS NOT NULL "
        "GROUP BY 1 ORDER BY obligations_60mo DESC LIMIT 15"
    )


# ── aggregation (deterministic, over the member rows) ─────────────────────────

def _f(v: Any) -> float:
    return float(v) if isinstance(v, (int, float)) and v == v else 0.0


def aggregate_members(members: list[dict[str, Any]], requested: int) -> dict[str, Any]:
    sam = [m for m in members if m.get("legal_business_name")]
    behavior = [m for m in members if m.get("prime_obl_lifetime") is not None
                or m.get("sub_amt_lifetime") is not None]
    active = [m for m in members if _f(m.get("active_award_ct")) > 0]
    return {
        "coverage": {
            "requested": requested,
            "sam_registered": len(sam),
            "with_award_history": len(behavior),
            "firmographics_known": sum(1 for m in members if m.get("employee_size_range")),
            "pricing_mix_known": sum(1 for m in members if m.get("active_fixed_share") is not None),
        },
        "counts": {
            "with_active_awards": len(active),
            "without_active_awards": len(members) - len(active),
            "registered_no_award_history": len(sam) - len(behavior),
            "sam_registration_inactive": sum(1 for m in sam if m.get("sam_is_active") is False),
            "with_award_expiring_180d": sum(1 for m in members if _f(m.get("pop_expiring_180d_ct")) > 0),
            "holding_open_idvs": sum(1 for m in members if _f(m.get("open_idv_ct")) > 0),
            "prime_24mo": sum(1 for m in members if m.get("is_prime_24mo")),
            "subawardee_60mo": sum(1 for m in members if m.get("is_sub_60mo")),
            "with_any_designation": sum(
                1 for m in members
                if any(m.get(k) for k in ("dsbs_8a", "dsbs_hubzone", "dsbs_wosb", "dsbs_sdvosb"))
            ),
            "with_contactable_people": sum(
                1 for m in members if _f(m.get("n_dialable")) + _f(m.get("n_emailable")) > 0
            ),
        },
        "sums": {
            "prime_obligations_24mo": sum(_f(m.get("prime_obl_24mo")) for m in members),
            "prime_obligations_lifetime": sum(_f(m.get("prime_obl_lifetime")) for m in members),
            "subaward_amount_24mo": sum(_f(m.get("sub_amt_24mo")) for m in members),
            "active_award_ct": sum(int(_f(m.get("active_award_ct"))) for m in members),
            "current_value_of_active_awards": sum(
                _f(m.get("current_value_of_active_awards")) for m in members
            ),
            "remaining_current_value_of_active_awards": sum(
                _f(m.get("remaining_current_value_of_active_awards")) for m in members
            ),
            "open_idv_potential_value": sum(_f(m.get("open_idv_potential_value")) for m in members),
        },
    }


DISCLOSURES = [
    "Obligations are dollars the government has legally committed on contract actions — not contract ceilings and not revenue.",
    "Current value of active awards covers standalone awards and task orders whose period of performance is running and not terminated; IDV/vehicle ceilings are reported separately and never blended in.",
    "Subaward figures cover FFATA-disclosed subawards only.",
    "Employee size, industry, and year founded come from a commercial-data bridge covering a minority of registrants — coverage is disclosed per report.",
    "The current federal fiscal year counts year-to-date.",
]


@router.post("", dependencies=[Depends(require_service_token)])
async def list_report(body: dict[str, Any]) -> dict[str, Any]:
    raw = body.get("ueis")
    if not isinstance(raw, list) or not raw:
        raise HTTPException(status_code=422, detail="ueis must be a non-empty list")
    if len(raw) > _MAX_BATCH:
        raise HTTPException(status_code=422, detail=f"batch limit {_MAX_BATCH}")
    ueis = sorted({u.strip().upper() for u in raw if isinstance(u, str) and _UEI_RE.match(u.strip())})
    if not ueis:
        raise HTTPException(status_code=422, detail="no valid 12-character UEIs in ueis")

    vals = _values_clause(ueis)
    fy_now = _current_fy(date.today())

    out: dict[str, Any] = {"elapsed_ms": 0.0, "artifact": None}

    async with httpx.AsyncClient(timeout=90.0) as client:
        async def run(sql: str, limit: int) -> list[dict[str, Any]]:
            payload = await _sidecar(client, sql, limit)
            out["elapsed_ms"] += payload.get("elapsed_ms") or 0
            out["artifact"] = payload.get("artifact") or out["artifact"]
            return _rows_as_dicts(payload)

        members = await run(_sql_members(vals), len(ueis) + 10)
        fy_series = await run(_sql_fy_series(vals, fy_now - 9), 200)
        actions = await run(_sql_actions_12mo(vals), 500)
        top_codes = await run(_sql_top_codes(vals), 100)
        expiry = await run(_sql_expiry_months(vals), 100)
        agencies = await run(_sql_agencies(vals), 50)

    return {
        "member_ct": len(members),
        "current_fy": fy_now,
        "members": members,
        "aggregate": aggregate_members(members, requested=len(ueis)),
        "fy_series": fy_series,
        "actions_12mo": actions,
        "principal_codes": {
            "naics": [c for c in top_codes if c.get("code_type") == "naics"][:12],
            "psc": [c for c in top_codes if c.get("code_type") == "psc"][:12],
        },
        "expiry_months": expiry,
        "agencies": agencies,
        "disclosures": DISCLOSURES,
        **out,
    }
