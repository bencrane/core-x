"""lender-book — a capital provider's UCC debtor book read against the federal record.

One request, one lender_key (the normalized secured-party key from
`ucc_lenders_all`), one composed response: the full CA/CO debtor book off the
`ucc_lender_filings` bridge (pruned probe — never a corpus scan), the federal
state of affairs of the SAM-resolved slice (list-report composition), how the
book engages (prime vs subawardee split), how the financing relationship maps
onto active awards (current vs former borrowers), the book's prime combo
signature + lookalike lenses (list-lookalike composition), and the DERIVED
MARKET: the signature restated as a market-query predicate spec — every dial
explicit and tunable — counted through the same compile path the platform uses.

Composition over duplication: SQL builders and aggregation are imported from
list_report_v1 / list_lookalike_v1; the market count reuses
market_query_v1.compile_predicates. This router owns only the UCC-side reads
and the splits.

STANDING RULING (operator, 2026-07-17): no imputed economics. This response
carries government-record and state-record figures only — no estimated deal
value, no loan-amount inference, no award-to-loan dollar arithmetic. The
financing record contributes TIMING and existence (filing dates, active
liens), never amounts.

Endpoint (service-token gated):
  POST /api/v1/lender-book   {"lender_key": "...", "limit_prime"?, "limit_sub"?}
"""
from __future__ import annotations

import logging
import re
from typing import Any

import httpx
from fastapi import APIRouter, Depends, HTTPException

from ..service_token import require_service_token
from .list_lookalike_v1 import (
    _CODE_RE,
    _sql_prime_active_customer_ct,
    _sql_prime_lens,
    _sql_signature,
    _sql_sub_lens,
)
from .list_lookalike_v1 import DISCLOSURES as LOOKALIKE_DISCLOSURES
from .list_report_v1 import (
    DISCLOSURES as REPORT_DISCLOSURES,
    _current_fy,
    _rows_as_dicts,
    _sidecar,
    _sql_actions_12mo,
    _sql_agencies,
    _sql_expiry_months,
    _sql_fy_series,
    _sql_members,
    _sql_top_codes,
    _values_clause,
    aggregate_members,
)
from .market_query_v1 import _run_sidecar as _run_market_sidecar
from .market_query_v1 import compile_predicates

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/lender-book", tags=["lender-book"])

# lender_key is minted by the corpus normalization (upper, strip punctuation,
# strip suffix tokens) — the only characters that survive are A-Z, 0-9, space.
_LENDER_KEY_RE = re.compile(r"^[A-Z0-9 ]{4,80}$")

_SEED_MAX = 2000          # list-report / lookalike batch ceiling
_UEI_READ_MAX = 10000     # per-uei lien-state read ceiling (disclosed if hit)
_MARKET_PAIRS = 25        # signature pairs carried into the derived market
_MARKET_FY_LOOKBACK = 2   # fy_start = current FY - 2 (a tunable dial, echoed)

DISCLOSURES = [
    "Debtor-book coverage is the California and Colorado Secretary of State UCC "
    "records only; secured-party attribution is an exact match on the normalized "
    "lender name.",
    "Financing-statement figures describe the UCC record only — no payment, "
    "loan-amount, or deal-value estimates are computed anywhere in this response.",
    "The derived market is a restatement of the book's demonstrated prime combo "
    "record as an editable predicate specification; every parameter is a dial, "
    "nothing is inferred beyond the government record.",
]


def _k(lender_key: str) -> str:
    key = lender_key.strip().upper()
    if not _LENDER_KEY_RE.match(key):
        raise HTTPException(status_code=422,
                            detail="lender_key must be 4-80 chars of A-Z, 0-9, space "
                                   "(the normalized key from ucc_lenders_all)")
    return key


# ── UCC-side SQL (all pruned probes on the lender_key sort) ───────────────────

def _sql_lender_row(key: str) -> str:
    return f"SELECT * FROM ucc_lenders_all WHERE lender_key = '{key}'"


def _sql_book_aggregates(key: str) -> str:
    fin = "filing_class = 'financing'"
    return (
        "SELECT count(DISTINCT debtor_key) AS debtors, "
        "count(DISTINCT CASE WHEN is_org THEN debtor_key END) AS organization_debtors, "
        "count(DISTINCT CASE WHEN in_sam THEN debtor_key END) AS sam_registered_debtors, "
        "count(DISTINCT uei) AS distinct_ueis, "
        "count(*) AS filings, "
        f"count(*) FILTER (WHERE {fin}) AS financing_filings, "
        f"count(*) FILTER (WHERE {fin} AND is_active_financing) AS active_financing_filings, "
        "count(*) FILTER (WHERE is_lease) AS lease_filings, "
        f"count(*) FILTER (WHERE NOT ({fin})) AS tax_or_judgment_filings, "
        f"count(DISTINCT CASE WHEN {fin} AND is_active_financing THEN debtor_key END) "
        "AS debtors_with_active_financing, "
        "count(DISTINCT CASE WHEN ucc_state = 'CA' THEN debtor_key END) AS ca_debtors, "
        "count(DISTINCT CASE WHEN ucc_state = 'CO' THEN debtor_key END) AS co_debtors "
        f"FROM ucc_lender_filings WHERE lender_key = '{key}'"
    )


def _sql_filings_by_year(key: str) -> str:
    return (
        "SELECT year(first_filing_date) AS filing_year, "
        "count(*) AS financing_filings, "
        "count(*) FILTER (WHERE is_active_financing) AS still_active "
        f"FROM ucc_lender_filings WHERE lender_key = '{key}' "
        "AND filing_class = 'financing' AND first_filing_date IS NOT NULL "
        "GROUP BY 1 ORDER BY 1"
    )


def _sql_uei_financing_state(key: str) -> str:
    return (
        "SELECT uei, "
        "max(CASE WHEN filing_class = 'financing' AND is_active_financing "
        "THEN 1 ELSE 0 END) = 1 AS active_financing_with_lender, "
        "max(CASE WHEN filing_class = 'financing' THEN first_filing_date END) "
        "AS last_financing_filing_date, "
        "count(*) AS filings_with_lender "
        f"FROM ucc_lender_filings WHERE lender_key = '{key}' AND uei IS NOT NULL "
        "GROUP BY 1 "
        "ORDER BY active_financing_with_lender DESC, filings_with_lender DESC, uei"
    )


# ── deterministic composition (pure Python over member rows) ──────────────────

def _fnum(v: Any) -> float:
    return float(v) if isinstance(v, (int, float)) and v == v else 0.0


def contracting_role_split(members: list[dict[str, Any]]) -> dict[str, Any]:
    """Prime vs subawardee engagement of the SAM-resolved book (lifetime basis;
    trailing-window activity counts alongside)."""
    prime = {m["uei"] for m in members if _fnum(m.get("prime_obl_lifetime")) > 0}
    sub = {m["uei"] for m in members if _fnum(m.get("sub_amt_lifetime")) > 0}
    return {
        "basis": "lifetime federal record; activity flags are trailing windows",
        "prime_only_firms": len(prime - sub),
        "subawardee_only_firms": len(sub - prime),
        "both_prime_and_subawardee_firms": len(prime & sub),
        "no_award_history_firms": len(members) - len(prime | sub),
        "prime_obligations_lifetime": sum(_fnum(m.get("prime_obl_lifetime")) for m in members),
        "subaward_amount_lifetime": sum(_fnum(m.get("sub_amt_lifetime")) for m in members),
        "prime_obligations_24mo": sum(_fnum(m.get("prime_obl_24mo")) for m in members),
        "subaward_amount_24mo": sum(_fnum(m.get("sub_amt_24mo")) for m in members),
        "prime_24mo_firms": sum(1 for m in members if m.get("is_prime_24mo")),
        "subawardee_60mo_firms": sum(1 for m in members if m.get("is_sub_60mo")),
    }


def financing_relationship_split(members: list[dict[str, Any]],
                                 lien_by_uei: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Active-award holders × the state of their financing statements with THIS
    lender: current borrowers (an active financing statement) vs former
    borrowers (every filing lapsed or terminated)."""
    holders = [m for m in members if _fnum(m.get("active_award_ct")) > 0]
    current = [m for m in holders
               if lien_by_uei.get(m["uei"], {}).get("active_financing_with_lender")]
    former = [m for m in holders
              if not lien_by_uei.get(m["uei"], {}).get("active_financing_with_lender")]

    def _val(rows: list[dict[str, Any]], col: str) -> float:
        return sum(_fnum(m.get(col)) for m in rows)

    return {
        "active_award_holders": len(holders),
        "current_borrower_holders": len(current),
        "former_borrower_holders": len(former),
        "current_value_of_active_awards_current_borrowers":
            _val(current, "current_value_of_active_awards"),
        "current_value_of_active_awards_former_borrowers":
            _val(former, "current_value_of_active_awards"),
        "remaining_current_value_former_borrowers":
            _val(former, "remaining_current_value_of_active_awards"),
    }


def derived_market_spec(signature: list[dict[str, Any]], current_fy: int) -> dict[str, Any] | None:
    """The book's prime combo signature restated as a market-query predicate
    spec. Every parameter is an explicit dial; the consumer tunes, the grammar
    recompiles — nothing here is bespoke."""
    safe = [
        s for s in signature
        if _CODE_RE.match(str(s.get("naics_code") or ""))
        and _CODE_RE.match(str(s.get("psc_code") or ""))
    ][:_MARKET_PAIRS]
    if not safe:
        return None
    return {
        "predicates": [
            {
                "term": "obligations_under_naics_psc_pairs",
                "pairs": [[str(s["naics_code"]), str(s["psc_code"])] for s in safe],
                "fy_start": current_fy - _MARKET_FY_LOOKBACK,
                "fy_end": current_fy,
                "min": 1,
            }
        ]
    }


# ── endpoint ──────────────────────────────────────────────────────────────────

@router.post("", dependencies=[Depends(require_service_token)])
async def lender_book(body: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(body, dict):
        raise HTTPException(status_code=422, detail="body must be an object")
    key = _k(str(body.get("lender_key") or ""))

    def _limit(name: str, default: int) -> int:
        v = body.get(name, default)
        return v if isinstance(v, int) and 1 <= v <= 500 else default

    limit_prime = _limit("limit_prime", 100)
    limit_sub = _limit("limit_sub", 50)

    out: dict[str, Any] = {"elapsed_ms": 0.0, "artifact": None}

    async with httpx.AsyncClient(timeout=120.0) as client:
        async def run(sql: str, limit: int) -> list[dict[str, Any]]:
            payload = await _sidecar(client, sql, limit)
            out["elapsed_ms"] += payload.get("elapsed_ms") or 0
            out["artifact"] = payload.get("artifact") or out["artifact"]
            return _rows_as_dicts(payload)

        lender_rows = await run(_sql_lender_row(key), 5)
        if not lender_rows:
            raise HTTPException(status_code=404,
                                detail="lender_key not found in ucc_lenders_all")
        lender = lender_rows[0]

        book_rows = await run(_sql_book_aggregates(key), 5)
        book = book_rows[0] if book_rows else {}
        book["financing_filings_by_year"] = await run(_sql_filings_by_year(key), 200)

        lien_rows = await run(_sql_uei_financing_state(key), _UEI_READ_MAX)
        seed_truncated = len(lien_rows) >= _SEED_MAX
        seed = [r["uei"] for r in lien_rows[:_SEED_MAX]]
        lien_by_uei = {r["uei"]: r for r in lien_rows[:_SEED_MAX]}

        disclosures = list(dict.fromkeys(
            DISCLOSURES + REPORT_DISCLOSURES + LOOKALIKE_DISCLOSURES))
        if seed_truncated:
            disclosures.append(
                f"The SAM-resolved book exceeds {_SEED_MAX} registrants; federal "
                f"figures cover the {_SEED_MAX} with the strongest financing "
                "relationship (active statements first, then filing count).")

        response: dict[str, Any] = {
            "lender": lender,
            "book": book,
            "seed": {"ueis_in_book": len(lien_rows), "seed_ct": len(seed),
                     "truncated": seed_truncated},
            "disclosures": disclosures,
        }

        if not seed:
            return {**response,
                    "report": None, "contracting_role_split": None,
                    "financing_relationship_split": None, "prime_signature": [],
                    "prime_lookalikes": [], "sub_lookalikes": [], "market": None,
                    **out}

        vals = _values_clause(seed)
        from datetime import date as _date
        fy_now = _current_fy(_date.today())

        members = await run(_sql_members(vals), _SEED_MAX + 10)
        for m in members:
            lien = lien_by_uei.get(m["uei"], {})
            m["active_financing_with_lender"] = bool(
                lien.get("active_financing_with_lender"))
            m["last_financing_filing_date"] = lien.get("last_financing_filing_date")

        report = {
            "aggregates": aggregate_members(members, len(seed)),
            "fiscal_year_series": await run(_sql_fy_series(vals, fy_now - 9), 20),
            "contract_actions_12mo": await run(_sql_actions_12mo(vals), 200),
            "principal_codes": await run(_sql_top_codes(vals), 50),
            "expiring_awards_24mo": await run(_sql_expiry_months(vals), 30),
            "awarding_agencies": await run(_sql_agencies(vals), 20),
            "members": members,
        }

        role_split = contracting_role_split(members)
        fin_split = financing_relationship_split(members, lien_by_uei)

        signature = await run(_sql_signature(vals), _MARKET_PAIRS + 5)
        prime_lookalikes: list[dict[str, Any]] = []
        sub_lookalikes: list[dict[str, Any]] = []
        market: dict[str, Any] | None = None

        if signature:
            active_rows = await run(_sql_prime_active_customer_ct(vals), 5)
            prime_active = int(active_rows[0]["ct"]) if active_rows else 0
            for s in signature:
                s["customer_share"] = (
                    round(s["customer_firms"] / prime_active, 4) if prime_active else 0.0)
            safe = [s for s in signature
                    if _CODE_RE.match(str(s["naics_code"]))
                    and _CODE_RE.match(str(s["psc_code"]))]
            pairs = ", ".join(f"('{s['naics_code']}', '{s['psc_code']}')" for s in safe)
            weighted = ", ".join(
                f"('{s['naics_code']}', '{s['psc_code']}', {float(s['customer_share'])})"
                for s in safe)
            if safe:
                prime_lookalikes = await run(
                    _sql_prime_lens(vals, weighted, limit_prime), limit_prime + 10)
                sub_lookalikes = await run(
                    _sql_sub_lens(vals, pairs, limit_sub), limit_sub + 10)

            spec = derived_market_spec(signature, fy_now)
            if spec:
                expr, echoes, spec_disclosures = compile_predicates(spec)
                count_payload = await _run_market_sidecar(
                    f"SELECT count(*) AS n FROM (\n{expr}\n)", 1)
                market_count = (count_payload["rows"][0][0]
                                if count_payload.get("rows") else 0)
                overlap_payload = await _run_market_sidecar(
                    f"SELECT count(*) AS n FROM ((\n{expr}\n) "
                    f"INTERSECT (SELECT uei FROM (VALUES {vals}) b(uei)))", 1)
                overlap = (overlap_payload["rows"][0][0]
                           if overlap_payload.get("rows") else 0)
                out["elapsed_ms"] += (count_payload.get("elapsed_ms") or 0) + (
                    overlap_payload.get("elapsed_ms") or 0)
                market = {
                    "spec": spec,
                    "compiled_spec": {"predicates": echoes},
                    "market_firm_ct": market_count,
                    "book_firms_in_market": overlap,
                    "expansion_firm_ct": market_count - overlap,
                    "disclosures": spec_disclosures,
                    "artifact": count_payload.get("artifact"),
                }

    return {
        **response,
        "report": report,
        "contracting_role_split": role_split,
        "financing_relationship_split": fin_split,
        "prime_signature": signature,
        "prime_lookalikes": prime_lookalikes,
        "sub_lookalikes": sub_lookalikes,
        "market": market,
        **out,
    }
