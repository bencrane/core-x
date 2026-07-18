"""lender_book_v1 — pure-composition tests (no network)."""

import pytest
from fastapi import HTTPException

from apps.catalyst_api.src.routers.lender_book_v1 import (
    _LENDER_KEY_RE,
    _k,
    _sql_book_aggregates,
    _sql_filings_by_year,
    _sql_uei_financing_state,
    contracting_role_split,
    derived_market_spec,
    financing_relationship_split,
)


def test_lender_key_shape_refuses_quote_bearing_input():
    # only characters the corpus normalization can mint reach SQL
    assert _LENDER_KEY_RE.match("CITY NATIONAL BANK")
    assert not _LENDER_KEY_RE.match("CITY'NATIONAL")
    assert not _LENDER_KEY_RE.match("ab")
    assert _k("  city national bank ") == "CITY NATIONAL BANK"
    with pytest.raises(HTTPException):
        _k("x'); DROP TABLE ucc_lender_filings; --")


def test_book_sql_probes_the_bridge_never_the_corpus():
    for sql in (_sql_book_aggregates("CITY NATIONAL BANK"),
                _sql_filings_by_year("CITY NATIONAL BANK"),
                _sql_uei_financing_state("CITY NATIONAL BANK")):
        assert "ucc_lender_filings" in sql
        assert "ucc_filings_all" not in sql            # the scan this cycle killed
        assert "lender_key = 'CITY NATIONAL BANK'" in sql


def test_uei_financing_state_orders_deterministically_for_the_seed():
    sql = _sql_uei_financing_state("K BANK")
    assert "ORDER BY active_financing_with_lender DESC, filings_with_lender DESC, uei" in sql


def test_contracting_role_split_partitions_exactly():
    members = [
        {"uei": "A", "prime_obl_lifetime": 100.0, "sub_amt_lifetime": 0.0,
         "prime_obl_24mo": 10.0, "sub_amt_24mo": 0.0, "is_prime_24mo": True,
         "is_sub_60mo": False},
        {"uei": "B", "prime_obl_lifetime": 0.0, "sub_amt_lifetime": 50.0,
         "prime_obl_24mo": 0.0, "sub_amt_24mo": 5.0, "is_prime_24mo": False,
         "is_sub_60mo": True},
        {"uei": "C", "prime_obl_lifetime": 20.0, "sub_amt_lifetime": 30.0},
        {"uei": "D"},
    ]
    s = contracting_role_split(members)
    assert s["prime_only_firms"] == 1
    assert s["subawardee_only_firms"] == 1
    assert s["both_prime_and_subawardee_firms"] == 1
    assert s["no_award_history_firms"] == 1
    # the four buckets partition the membership
    assert (s["prime_only_firms"] + s["subawardee_only_firms"]
            + s["both_prime_and_subawardee_firms"]
            + s["no_award_history_firms"]) == len(members)
    assert s["prime_obligations_lifetime"] == 120.0
    assert s["subaward_amount_lifetime"] == 80.0


def test_financing_relationship_split_current_vs_former():
    members = [
        {"uei": "A", "active_award_ct": 2, "current_value_of_active_awards": 100.0,
         "remaining_current_value_of_active_awards": 40.0},
        {"uei": "B", "active_award_ct": 1, "current_value_of_active_awards": 60.0,
         "remaining_current_value_of_active_awards": 10.0},
        {"uei": "C", "active_award_ct": 0},
    ]
    lien = {"A": {"active_financing_with_lender": True},
            "B": {"active_financing_with_lender": False}}
    s = financing_relationship_split(members, lien)
    assert s["active_award_holders"] == 2
    assert s["current_borrower_holders"] == 1
    assert s["former_borrower_holders"] == 1
    assert s["current_value_of_active_awards_former_borrowers"] == 60.0
    assert s["remaining_current_value_former_borrowers"] == 10.0


def test_derived_market_spec_is_a_tunable_predicate_list():
    signature = [
        {"naics_code": "236220", "psc_code": "Y1AA"},
        {"naics_code": "561720", "psc_code": "S201"},
        {"naics_code": "bad'code", "psc_code": "S201"},   # refused, not raised
    ]
    spec = derived_market_spec(signature, 2026)
    assert spec is not None
    (p,) = spec["predicates"]
    assert p["term"] == "obligations_under_naics_psc_pairs"
    assert p["pairs"] == [["236220", "Y1AA"], ["561720", "S201"]]
    # every parameter is an explicit dial the consumer can edit
    assert p["fy_start"] == 2024 and p["fy_end"] == 2026 and p["min"] == 1
    assert derived_market_spec([], 2026) is None


def test_derived_market_spec_compiles_through_the_platform_grammar():
    from apps.catalyst_api.src.routers.market_query_v1 import compile_predicates

    spec = derived_market_spec([{"naics_code": "236220", "psc_code": "Y1AA"}], 2026)
    expr, echoes, _ = compile_predicates(spec)
    assert "gtm_txn_events_slim" in expr
    assert echoes[0]["term"] == "obligations_under_naics_psc_pairs"
