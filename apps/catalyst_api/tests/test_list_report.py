"""list_report_v1 — pure-composition tests (no network)."""
from datetime import date

from apps.catalyst_api.src.routers.list_report_v1 import (
    _UEI_RE,
    _current_fy,
    _sql_members,
    _sql_top_codes,
    _values_clause,
    aggregate_members,
)


def test_uei_regex_refuses_quote_bearing_input():
    # inputs reaching _values_clause are pre-validated against the closed
    # UEI shape — nothing quote-bearing can reach SQL.
    assert _UEI_RE.match("ABC123DEF456")
    assert not _UEI_RE.match("ABC'23DEF456")
    assert not _UEI_RE.match("ABC123DEF45")
    assert _values_clause(["ABC123DEF456"]) == "('ABC123DEF456')"


def test_current_fy_rolls_on_october_first():
    assert _current_fy(date(2026, 7, 17)) == 2026
    assert _current_fy(date(2026, 10, 1)) == 2027
    assert _current_fy(date(2026, 9, 30)) == 2026


def test_member_statement_never_blends_vehicle_into_committed():
    sql = _sql_members("('ABC123DEF456')")
    # vehicle capacity and committed work stay separate named columns
    assert "vehicle_ceiling AS open_idv_potential_value" in sql
    assert "committed_value AS current_value_of_active_awards" in sql
    # every mart join is a LEFT JOIN off the VALUES spine (absence = coverage stat)
    assert sql.count("LEFT JOIN") == 6


def test_top_codes_uses_precomputed_ranks():
    sql = _sql_top_codes("('ABC123DEF456')")
    # doctrine: never re-derive ranks with window functions over code lanes
    assert "rank_lifetime <= 3" in sql
    assert "OVER" not in sql


def test_aggregate_counts_and_coverage():
    members = [
        {  # active prime, fully covered
            "legal_business_name": "A", "sam_is_active": True,
            "prime_obl_24mo": 100.0, "prime_obl_lifetime": 500.0,
            "sub_amt_24mo": 0.0, "sub_amt_lifetime": 0.0,
            "active_award_ct": 2, "pop_expiring_180d_ct": 1,
            "open_idv_ct": 1, "open_idv_potential_value": 1000.0,
            "is_prime_24mo": True, "is_sub_60mo": False,
            "current_value_of_active_awards": 400.0,
            "remaining_current_value_of_active_awards": 50.0,
            "employee_size_range": "11-50", "active_fixed_share": 0.9,
            "dsbs_8a": True, "n_dialable": 2, "n_emailable": 0,
        },
        {  # registered, no award history
            "legal_business_name": "B", "sam_is_active": False,
            "prime_obl_lifetime": None, "sub_amt_lifetime": None,
        },
        {  # not a registrant (all joins null)
            "legal_business_name": None,
        },
    ]
    agg = aggregate_members(members, requested=4)
    assert agg["coverage"] == {
        "requested": 4,
        "sam_registered": 2,
        "with_award_history": 1,
        "firmographics_known": 1,
        "pricing_mix_known": 1,
    }
    c = agg["counts"]
    assert c["with_active_awards"] == 1
    assert c["without_active_awards"] == 2
    assert c["registered_no_award_history"] == 1
    assert c["sam_registration_inactive"] == 1
    assert c["with_award_expiring_180d"] == 1
    assert c["holding_open_idvs"] == 1
    assert c["with_any_designation"] == 1
    assert c["with_contactable_people"] == 1
    s = agg["sums"]
    assert s["prime_obligations_lifetime"] == 500.0
    assert s["current_value_of_active_awards"] == 400.0
    assert s["open_idv_potential_value"] == 1000.0
