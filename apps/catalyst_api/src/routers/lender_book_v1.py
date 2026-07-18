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

# ── derived-market composition (first-principles cycle, operator-directed
#    2026-07-17 late session). The market is derived from the substrate FIRST;
#    the predicate grammar is an EXPORT format, never the constraint.
#    Derivation: blended work signature (top pairs by book-firm count UNION
#    top pairs by book prime dollars — firm-ranked alone buries where the
#    book's balance sheet is), a broad base market (anyone paid under the
#    signature in the window), then an articulated TUNING LADDER of literal
#    filters down to the working-capital core, with credit-moment overlays
#    as segments. Measured on CNB 2026-07-17: base 12,910 → ≥$250K 5,473 →
#    FFP-unfinanced≥50% 2,945 → 11-500 employees / CA as further dials.
_SIG_TOP_FIRMS = 15       # signature pairs ranked by distinct book firms
_SIG_TOP_DOLLARS = 15     # signature pairs ranked by book prime obligations
_MARKET_FY_LOOKBACK = 2   # window: current FY - 2 → current (a dial, echoed)
_CORE_FFP_SHARE = 0.5     # working-capital structure rung (FFP-unfinanced)
_SIZE_BANDS = ("11-50", "51-200", "201-500")
_FLOOR_MIN, _FLOOR_MAX, _FLOOR_ROUND = 50_000, 1_000_000, 50_000

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


def _sql_blended_signature(cvals: str) -> str:
    """Top pairs by book-firm count UNION top pairs by book prime dollars."""
    return (
        f"WITH u(uei) AS (VALUES {cvals}), "
        "lanes AS (SELECT l.naics_code, l.psc_code, "
        "  COUNT(DISTINCT l.uei) AS book_firms, SUM(l.prime_obl_lifetime) AS book_obligations "
        "  FROM gtm_prime_combo_lanes l JOIN u ON u.uei = l.uei "
        "  WHERE l.naics_code IS NOT NULL AND l.psc_code IS NOT NULL "
        "    AND l.psc_code <> '9999' GROUP BY 1, 2), "
        f"byf AS (SELECT *, 'firms' AS basis FROM lanes ORDER BY book_firms DESC, book_obligations DESC LIMIT {_SIG_TOP_FIRMS}), "
        f"byo AS (SELECT *, 'dollars' AS basis FROM lanes ORDER BY book_obligations DESC LIMIT {_SIG_TOP_DOLLARS}) "
        "SELECT naics_code, psc_code, max(book_firms) AS book_firms, "
        "max(book_obligations) AS book_obligations, "
        "string_agg(DISTINCT basis, '+' ORDER BY basis) AS basis "
        "FROM (SELECT * FROM byf UNION ALL SELECT * FROM byo) "
        "GROUP BY 1, 2 ORDER BY book_obligations DESC"
    )


def _sql_book_scale(cvals: str, fy_start: int) -> str:
    """Median of per-firm obligations won in the window — the scale floor seed."""
    return (
        f"WITH u(uei) AS (VALUES {cvals}), "
        "w AS (SELECT w.uei, SUM(w.won_obl) AS tot FROM gtm_entity_fy_won w "
        f"JOIN u ON u.uei = w.uei WHERE w.fy >= {fy_start} "
        "GROUP BY 1 HAVING SUM(w.won_obl) > 0) "
        "SELECT median(tot) AS median_won FROM w"
    )


def _sql_book_texture(cvals: str) -> str:
    """The book's active pricing×financing texture — the evidence behind the
    working-capital rung's default."""
    return (
        f"WITH u(uei) AS (VALUES {cvals}) "
        "SELECT COUNT(*) AS firms, SUM(m.active_obl) AS active_obl, "
        "SUM(m.active_obl_ffp_unfinanced) AS active_obl_ffp_unfinanced "
        "FROM u JOIN gtm_entity_pricing_mix m ON m.uei = u.uei "
        "WHERE m.active_award_ct > 0"
    )


def _scale_floor(median_won: float | None) -> int:
    if not median_won or median_won != median_won:
        return _FLOOR_MIN
    stepped = int(round(median_won / _FLOOR_ROUND)) * _FLOOR_ROUND
    return max(_FLOOR_MIN, min(_FLOOR_MAX, stepped))


def _market_base_expr(pair_values: str, window_start: str, floor: int) -> str:
    """The broad market: every firm paid as prime under the signature pairs in
    the window, at or above the floor (floor 0 = the base, net-positive only)."""
    having = f"SUM(t.obligation) >= {floor}" if floor > 0 else "SUM(t.obligation) > 0"
    return (
        "SELECT t.uei FROM gtm_txn_events_slim t "
        f"JOIN (VALUES {pair_values}) pr(n, p) "
        "ON t.naics_code = pr.n AND t.psc_code = pr.p "
        f"WHERE t.action_date >= DATE '{window_start}' "
        f"GROUP BY t.uei HAVING {having}"
    )


def _core_expr(pair_values: str, window_start: str, floor: int) -> str:
    return (
        f"WITH m AS ({_market_base_expr(pair_values, window_start, floor)}) "
        "SELECT m.uei FROM m JOIN gtm_entity_pricing_mix x ON x.uei = m.uei "
        f"WHERE x.active_ffp_unfinanced_share >= {_CORE_FFP_SHARE}"
    )


def exportable_predicates(pairs: list[list[str]], fy_start: int, fy_end: int,
                          floor: int) -> dict[str, Any]:
    """The derived market re-expressed in the market-query grammar — an EXPORT
    of the derivation, not its source. Optional legs list the further rungs."""
    return {
        "predicates": [
            {"term": "obligations_under_naics_psc_pairs", "pairs": pairs,
             "fy_start": fy_start, "fy_end": fy_end, "min": floor},
            {"term": "active_award_pricing_mix",
             "min_ffp_unfinanced_share": _CORE_FFP_SHARE},
        ],
        "optional_predicates": [
            {"term": "employee_size", "note": f"bands {list(_SIZE_BANDS)} mirror the book"},
            {"term": "registered_in_state", "note": "footprint segment — default open"},
            {"term": "awards_expiring", "note": "the re-compete / refinance moment"},
            {"term": "recent_award_actions", "note": "the mobilization moment"},
            {"term": "ucc_financing", "note": "known CA/CO credit consumers"},
        ],
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

        signature = await run(_sql_signature(vals), 30)
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

        # ── derived market: first-principles composition (grammar = export) ──
        blended = await run(_sql_blended_signature(vals),
                            _SIG_TOP_FIRMS + _SIG_TOP_DOLLARS + 5)
        blended = [b for b in blended
                   if _CODE_RE.match(str(b["naics_code"]))
                   and _CODE_RE.match(str(b["psc_code"]))]
        if blended:
            pv = ", ".join(f"('{b['naics_code']}', '{b['psc_code']}')" for b in blended)
            fy_start = fy_now - _MARKET_FY_LOOKBACK
            window_start = f"{fy_start - 1}-10-01"

            scale_rows = await run(_sql_book_scale(vals, fy_start), 5)
            floor = _scale_floor(scale_rows[0]["median_won"] if scale_rows else None)
            texture_rows = await run(_sql_book_texture(vals), 5)
            texture = texture_rows[0] if texture_rows else {}

            async def cnt(sql: str) -> int:
                rows = await run(f"SELECT count(*) AS n FROM ({sql})", 5)
                return int(rows[0]["n"]) if rows else 0

            base = _market_base_expr(pv, window_start, 0)
            floored = _market_base_expr(pv, window_start, floor)
            core = _core_expr(pv, window_start, floor)

            base_ct = await cnt(base)
            floored_ct = await cnt(floored)
            core_ct = await cnt(core)
            size_ct = await cnt(
                f"SELECT c.uei FROM ({core}) c JOIN gtm_entity_firmographics f "
                f"ON f.uei = c.uei WHERE f.employee_size_range IN "
                f"({', '.join(repr(b) for b in _SIZE_BANDS)})")
            state = lender.get("ca_firms", 0) >= lender.get("co_firms", 0) and "CA" or "CO"
            state_ct = await cnt(
                f"SELECT c.uei FROM ({core}) c JOIN gtm_sam_entities e "
                f"ON e.uei = c.uei WHERE e.physical_state = '{state}'")

            moment_rows = await run(
                f"WITH c AS ({core}) SELECT "
                "(SELECT COUNT(DISTINCT x.uei) FROM gtm_award_expiry_months x "
                " JOIN c ON c.uei = x.uei "
                " WHERE x.end_month >= date_trunc('month', current_date) "
                "  AND x.end_month < date_trunc('month', current_date) + INTERVAL 12 MONTH) "
                " AS awards_expiring_12mo, "
                "(SELECT COUNT(*) FROM c JOIN gtm_entity_behavior_rollup b "
                " ON b.uei = c.uei WHERE b.last_action_date >= current_date - 90) "
                " AS award_action_90d, "
                "(SELECT COUNT(*) FROM c JOIN "
                " (SELECT DISTINCT uei FROM sam_ucc_debtor_overlap) d ON d.uei = c.uei) "
                " AS known_ucc_debtor_ca_co, "
                f"(SELECT COUNT(*) FROM c WHERE c.uei NOT IN "
                f" (SELECT uei FROM (VALUES {vals}) b(uei))) AS expansion_firms", 5)
            moments = moment_rows[0] if moment_rows else {}

            export = exportable_predicates(
                [[b["naics_code"], b["psc_code"]] for b in blended],
                fy_start, fy_now, floor)
            # importability guarantee: the export must compile in the grammar
            compile_predicates({"predicates": export["predicates"]})

            market = {
                "derivation": {
                    "signature_pairs": blended,
                    "book_texture": {
                        "active_firms": texture.get("firms"),
                        "active_obligations": texture.get("active_obl"),
                        "active_obligations_ffp_unfinanced":
                            texture.get("active_obl_ffp_unfinanced"),
                    },
                    "scale_floor": floor,
                    "window": {"fy_start": fy_start, "fy_end": fy_now},
                },
                "definition": {
                    "description": "every firm paid as prime under the book's "
                                   "work signature in the window",
                    "count": base_ct,
                },
                "ladder": [
                    {"step": "scale_floor",
                     "filter": f"≥ ${floor:,} obligations under the signature "
                               "in the window", "count": floored_ct},
                    {"step": "working_capital_structure",
                     "filter": f"firm-fixed-price, unfinanced ≥ "
                               f"{int(_CORE_FFP_SHARE * 100)}% of active "
                               "obligations", "count": core_ct},
                    {"step": "employee_size",
                     "filter": f"{_SIZE_BANDS[0]}–{_SIZE_BANDS[-1].split('-')[-1]} "
                               "employees (the book's bands)", "count": size_ct},
                    {"step": "registered_in_state",
                     "filter": f"registered in {state} (footprint)",
                     "count": state_ct},
                ],
                "moments": {"basis": "working_capital_structure rung", **moments},
                "exportable_predicates": export,
                "artifact": out["artifact"],
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
