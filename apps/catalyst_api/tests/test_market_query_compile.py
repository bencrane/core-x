"""compile_predicates — the predicate-grammar compiler (pure, no I/O)."""
import pytest
from fastapi import HTTPException

from apps.catalyst_api.src.routers.market_query_v1 import (
    VOCABULARY,
    _COMPILERS,
    compile_predicates,
)


def _compile(preds):
    return compile_predicates({"predicates": preds})


# ── vocabulary integrity ─────────────────────────────────────────────────────

def test_vocabulary_matches_compilers_exactly() -> None:
    assert {t["term"] for t in VOCABULARY} == set(_COMPILERS)


def test_thirty_terms() -> None:
    assert len(_COMPILERS) == 30


# ── assembly ─────────────────────────────────────────────────────────────────

def test_single_affirmative_leg() -> None:
    expr, echoes, _ = _compile([{"term": "registered_in_state", "states": ["co"]}])
    assert "physical_state IN ('CO')" in expr
    assert "INTERSECT" not in expr
    assert echoes[0] == {"term": "registered_in_state", "states": ["CO"]}


def test_conjunction_intersects_on_uei() -> None:
    expr, _, _ = _compile([
        {"term": "registered_in_state", "states": ["CO"]},
        {"term": "total_current_value_of_active_awards", "min": 50_000_000},
    ])
    assert expr.count("INTERSECT") == 1
    assert "committed_value" in expr


def test_negated_leg_excepts() -> None:
    expr, _, _ = _compile([
        {"term": "registered_in_state", "states": ["CO"]},
        {"term": "active_awards", "value": False},
    ])
    assert "EXCEPT" in expr
    assert "active_award_ct >= 1" in expr


def test_blank_slate_negative_only_uses_sam_universe() -> None:
    expr, _, disclosures = _compile([{"term": "active_awards", "value": False}])
    assert "FROM gtm_sam_entities" in expr
    assert any("universe" in d for d in disclosures)


def test_unknown_term_refuses_with_vocabulary_pointer() -> None:
    with pytest.raises(HTTPException) as e:
        _compile([{"term": "coolness"}])
    assert "vocabulary" in e.value.detail


def test_predicate_cap() -> None:
    with pytest.raises(HTTPException):
        _compile([{"term": "registered_in_state", "states": ["CO"]}] * 21)


# ── money ────────────────────────────────────────────────────────────────────

def test_total_obligations_default_total_24mo() -> None:
    expr, echoes, _ = _compile([{"term": "total_obligations", "min": 5e6}])
    assert "gtm_audience_entities" in expr and "total_amt_24mo" in expr
    assert echoes[0]["clock"] == {"basis": "trailing", "window": "24mo"}


def test_total_obligations_prime_36mo_rides_rollup() -> None:
    expr, _, _ = _compile([{"term": "total_obligations", "side": "prime",
                            "clock": {"basis": "trailing", "window": "36mo"}, "min": 1}])
    assert "gtm_entity_behavior_rollup" in expr and "prime_obl_36mo" in expr


def test_total_obligations_sub_36mo_refuses() -> None:
    with pytest.raises(HTTPException):
        _compile([{"term": "total_obligations", "side": "sub",
                   "clock": {"basis": "trailing", "window": "36mo"}, "min": 1}])


def test_fiscal_year_clock_any_window() -> None:
    expr, echoes, _ = _compile([{"term": "total_obligations", "side": "prime",
                                 "clock": {"basis": "fiscal_years",
                                           "fy_start": 2020, "fy_end": 2026},
                                 "min": 5_000_000}])
    assert "fy BETWEEN 2020 AND 2026" in expr
    assert "sum(won_obl)" in expr
    assert echoes[0]["clock"]["fy_start"] == 2020


def test_fiscal_year_clock_defaults_end_to_present_fy() -> None:
    expr, echoes, _ = _compile([{"term": "total_obligations", "side": "prime",
                                 "clock": {"basis": "fiscal_years", "fy_start": 2021},
                                 "min": 1}])
    assert "fy BETWEEN 2021 AND 2026" in expr
    assert echoes[0]["clock"]["fy_end"] == 2026


def test_fiscal_year_clock_sub_side_refuses() -> None:
    with pytest.raises(HTTPException) as e:
        _compile([{"term": "total_obligations", "side": "sub",
                   "clock": {"basis": "fiscal_years", "fy_start": 2023}, "min": 1}])
    assert "prime" in e.value.detail


def test_book_terms_map_to_named_columns() -> None:
    for term, col in [
        ("total_current_value_of_active_awards", "committed_value"),
        ("remaining_current_value_of_active_awards", "committed_runway"),
        ("median_active_award_value", "committed_award_median"),
        ("total_potential_value_of_open_idvs", "vehicle_ceiling"),
        ("remaining_potential_value_of_open_idvs", "vehicle_headroom"),
    ]:
        expr, _, _ = _compile([{"term": term, "min": 1_000_000}])
        assert col in expr, term
        assert "gtm_entity_award_book" in expr


def test_min_or_max_required_on_value_terms() -> None:
    with pytest.raises(HTTPException):
        _compile([{"term": "total_current_value_of_active_awards"}])


# ── work ─────────────────────────────────────────────────────────────────────

def test_awarded_under_codes_exact_and_prefix() -> None:
    expr, echoes, _ = _compile([{"term": "awarded_under_codes", "side": "prime",
                                 "code_type": "naics", "codes": ["561320", "5613"],
                                 "window": "24mo"}])
    assert "code IN ('561320')" in expr
    assert "code LIKE '5613%'" in expr
    assert "obl_24mo > 0" in expr
    assert echoes[0]["codes"] == ["561320", "5613*"]


def test_inferred_codes_refuse_prefixes() -> None:
    with pytest.raises(HTTPException) as e:
        _compile([{"term": "inferred_capable_of_codes", "code_type": "naics",
                   "codes": ["5613"]}])
    assert "exact" in e.value.detail


def test_pairs_generalized_collections_leg() -> None:
    expr, echoes, _ = _compile([{"term": "obligations_under_naics_psc_pairs",
                                 "pairs": [["237310", "Y1DA"]],
                                 "fy_start": 2021, "fy_end": 2025, "min": 1_000_000}])
    assert "VALUES ('237310','Y1DA')" in expr
    assert "DATE '2020-10-01'" in expr and "DATE '2025-09-30'" in expr
    assert echoes[0]["pair_count"] == 1


def test_pairs_fy26_end_runs_to_present() -> None:
    expr, _, _ = _compile([{"term": "obligations_under_naics_psc_pairs",
                            "pairs": [["237310", "Y1DA"]], "fy_start": 2023, "min": 1}])
    assert "current_date" in expr


# ── lifecycle / buyers / geo ─────────────────────────────────────────────────

def test_awards_expiring_months_window() -> None:
    expr, _, _ = _compile([{"term": "awards_expiring", "within_months": 6,
                            "min_obligated": 1_000_000}])
    assert "gtm_award_expiry_months" in expr
    assert "INTERVAL '6 months'" in expr
    assert "sum(obligated) >= 1000000.0" in expr


def test_awarded_by_agency_active_scope() -> None:
    expr, _, _ = _compile([{"term": "awarded_by_agency", "agencies": ["097"],
                            "scope": "active", "min_dollars": 5e6}])
    assert "awarding_agency_code IN ('097')" in expr
    assert "sum(obligated_active) >= 5000000.0" in expr


def test_pop_active_reads_open_award_universe() -> None:
    expr, _, disclosures = _compile([{"term": "place_of_performance_of_active_awards",
                                      "states": ["VA"]}])
    assert "gtm_open_awards" in expr
    assert any("resolvable PoP" in d for d in disclosures)


# ── designations / momentum / risk ───────────────────────────────────────────

def test_designations_any_ors() -> None:
    expr, _, _ = _compile([{"term": "small_business_designations",
                            "designations": ["dsbs_8a", "dsbs_wosb"], "match": "any"}])
    assert "dsbs_8a = TRUE OR dsbs_wosb = TRUE" in expr


def test_set_aside_actual_wins_fy_window() -> None:
    expr, _, _ = _compile([{"term": "set_aside_obligations", "set_aside": "8a",
                            "fy_start": 2024, "fy_end": 2025, "min": 1}])
    assert "won_obl_8a" in expr and "fy BETWEEN 2024 AND 2025" in expr


def test_recent_actions_new_award_is_null_action_type() -> None:
    expr, _, _ = _compile([{"term": "recent_award_actions", "family": "new_award",
                            "within_months": 3, "min_dollars": 250_000}])
    assert "action_type_code IS NULL" in expr
    assert "INTERVAL '3 months'" in expr


def test_recent_actions_more_work_via_vocab() -> None:
    expr, _, _ = _compile([{"term": "recent_award_actions", "family": "more_work",
                            "within_months": 12}])
    assert "action_type_vocab WHERE is_more_work" in expr


def test_profile_changes_closed_field_set() -> None:
    with pytest.raises(HTTPException):
        _compile([{"term": "sam_profile_changes", "fields": ["ceo_changed"],
                   "within_months": 6}])


def test_ucc_active_financing_default() -> None:
    expr, _, disclosures = _compile([{"term": "ucc_financing"}])
    assert "is_active_financing = TRUE" in expr
    assert any("CA and CO" in d for d in disclosures)


# ── injection surface ────────────────────────────────────────────────────────

def test_no_free_text_reaches_sql() -> None:
    hostile = "CO'); DROP TABLE x;--"
    for preds in (
        [{"term": "registered_in_state", "states": [hostile]}],
        [{"term": "awarded_under_codes", "side": "prime", "code_type": "naics",
          "codes": [hostile]}],
        [{"term": "awarded_by_agency", "agencies": [hostile]}],
        [{"term": "subcontractor_to", "prime_ueis": [hostile]}],
        [{"term": "sam_profile_changes", "fields": [hostile], "within_months": 6}],
        [{"term": "employee_size", "bands": [hostile]}],
    ):
        with pytest.raises(HTTPException):
            _compile(preds)
