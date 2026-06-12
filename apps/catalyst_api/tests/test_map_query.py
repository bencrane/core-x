"""Unit tests for the map EXECUTE compiler — pure, no R2 / network.

Pins the filter-object → Lance-predicate compile (the security surface): hardcoded
columns from the decoder, type-validated + _sql_str-escaped values, the AND-combined
clauses, the always-appended plottable-only clause, and the GeoJSON shaping. Every
off-allowlist / mistyped / injection case must raise MapCompileError (→ 422 at the
route), never reach Lance.
"""

from __future__ import annotations

from datetime import date

import pytest

from apps.catalyst_api.src import lance_store
from apps.catalyst_api.src.map_decoders import COMPANY, DECODERS, WINNERS, OPS

# Frozen anchor for days_ago clauses — injected so the expected DATE literals are stable.
TODAY = date(2026, 6, 12)


def _compile(decoder, filters):
    return lance_store.compile_map_filter(decoder, filters, today=TODAY)


# ── compile correctness ──────────────────────────────────────────────────────
def test_winners_construction_over_150k():
    pred = _compile(WINNERS, [
        {"field": "naics2", "op": "=", "value": "23"},
        {"field": "total_obligation", "op": ">=", "value": 150000},
    ])
    assert pred == "naics2 = '23' AND total_obligation >= 150000.0 AND latitude IS NOT NULL"


def test_company_bool_literal_is_unquoted():
    pred = _compile(COMPANY, [{"field": "has_federal_awards", "op": "=", "value": True}])
    # Arrow boolean literal, NOT the string 'true'
    assert pred == "has_federal_awards = true AND latitude IS NOT NULL"


def test_in_clause():
    pred = _compile(WINNERS, [{"field": "naics2", "op": "in", "value": ["23", "11"]}])
    assert pred == "naics2 IN ('23', '11') AND latitude IS NOT NULL"


def test_between_desugars_to_inclusive_range():
    pred = _compile(COMPANY, [{"field": "founded_year", "op": "between", "value": [2010, 2020]}])
    assert pred == "founded_year >= 2010 AND founded_year <= 2020 AND latitude IS NOT NULL"


def test_query_name_state_maps_to_physical_address_state():
    pred = _compile(COMPANY, [{"field": "state", "op": "=", "value": "TX"}])
    assert pred == "physical_address_state = 'TX' AND latitude IS NOT NULL"


def test_empty_filters_returns_plottable_only():
    assert _compile(COMPANY, []) == "latitude IS NOT NULL"


def test_multi_clause_and():
    pred = _compile(COMPANY, [
        {"field": "naics2", "op": "=", "value": "23"},
        {"field": "has_federal_awards", "op": "=", "value": True},
        {"field": "state", "op": "in", "value": ["TX", "CA"]},
    ])
    assert pred == ("naics2 = '23' AND has_federal_awards = true "
                    "AND physical_address_state IN ('TX', 'CA') AND latitude IS NOT NULL")


# ── safety: every bad clause raises (→ 422), never reaches Lance ──────────────
def test_off_allowlist_field_rejected():
    with pytest.raises(lance_store.MapCompileError):
        _compile(COMPANY, [{"field": "linkedin_url", "op": "=", "value": "x"}])


def test_off_allowlist_op_for_field_rejected():
    # `>=` is not in has_federal_awards.ops (= only)
    with pytest.raises(lance_store.MapCompileError):
        _compile(COMPANY, [{"field": "has_federal_awards", "op": ">=", "value": True}])


def test_string_field_given_int_rejected():
    with pytest.raises(lance_store.MapCompileError):
        _compile(COMPANY, [{"field": "naics2", "op": "=", "value": 123}])


def test_bool_field_given_string_rejected():
    with pytest.raises(lance_store.MapCompileError):
        _compile(COMPANY, [{"field": "has_federal_awards", "op": "=", "value": "true"}])


def test_int_field_given_bool_rejected():
    # bool is a subclass of int in Python — the coercer must reject it explicitly
    with pytest.raises(lance_store.MapCompileError):
        _compile(WINNERS, [{"field": "award_count", "op": ">=", "value": True}])


def test_in_empty_list_rejected():
    with pytest.raises(lance_store.MapCompileError):
        _compile(WINNERS, [{"field": "naics2", "op": "in", "value": []}])


def test_between_wrong_arity_rejected():
    with pytest.raises(lance_store.MapCompileError):
        _compile(COMPANY, [{"field": "founded_year", "op": "between", "value": [2010]}])


def test_enum_violation_rejected():
    with pytest.raises(lance_store.MapCompileError):
        _compile(WINNERS, [{"field": "winner_type", "op": "=", "value": "not_a_type"}])


def test_company_employee_size_band_enum_violation_rejected():
    with pytest.raises(lance_store.MapCompileError):
        _compile(COMPANY, [{"field": "employee_size_band", "op": "=", "value": "50-100"}])


def test_company_type_enum_violation_rejected():
    with pytest.raises(lance_store.MapCompileError):
        _compile(COMPANY, [{"field": "company_type", "op": "=", "value": "LLC"}])


def test_company_employee_size_band_valid_enum_compiles():
    pred = _compile(COMPANY, [{"field": "employee_size_band", "op": "=", "value": "51-200"}])
    assert pred == "employee_size_band = '51-200' AND latitude IS NOT NULL"


def test_company_type_valid_enum_compiles():
    pred = _compile(COMPANY, [{"field": "company_type", "op": "=", "value": "Nonprofit"}])
    assert pred == "company_type = 'Nonprofit' AND latitude IS NOT NULL"


def test_injection_value_is_escaped_not_executed():
    pred = _compile(COMPANY, [{"field": "state", "op": "=", "value": "TX' OR '1'='1"}])
    # the quote is doubled → a harmless string literal; no predicate breakout
    assert pred == "physical_address_state = 'TX'' OR ''1''=''1' AND latitude IS NOT NULL"
    assert "''" in pred


# ── days_ago: relative-time clauses resolve to DATE literals at request time ──
def test_company_won_within_days_inverts_onto_date_column():
    # "won in the last 7 days" → days_since <= 7 → action date ON/AFTER today-7
    pred = _compile(COMPANY, [{"field": "days_since_last_award", "op": "<=", "value": 7}])
    assert pred == "latest_award_action_date >= DATE '2026-06-05' AND latitude IS NOT NULL"


def test_winners_won_within_one_day():
    pred = _compile(WINNERS, [{"field": "days_since_last_award", "op": "<=", "value": 1}])
    assert pred == "last_action_date >= DATE '2026-06-11' AND latitude IS NOT NULL"


def test_days_ago_gte_means_on_or_before_cutoff():
    # "no award in over a year" → days_since >= 365 → action date ON/BEFORE today-365
    pred = _compile(COMPANY, [{"field": "days_since_last_award", "op": ">=", "value": 365}])
    assert pred == "latest_award_action_date <= DATE '2025-06-12' AND latitude IS NOT NULL"


def test_days_ago_between_is_a_days_window():
    # 7..30 days ago → dates today-30 .. today-7
    pred = _compile(COMPANY, [{"field": "days_since_last_award", "op": "between", "value": [7, 30]}])
    assert pred == ("latest_award_action_date >= DATE '2026-05-13' AND "
                    "latest_award_action_date <= DATE '2026-06-05' AND latitude IS NOT NULL")


def test_days_ago_zero_is_today():
    pred = _compile(COMPANY, [{"field": "days_since_last_award", "op": "<=", "value": 0}])
    assert pred == "latest_award_action_date >= DATE '2026-06-12' AND latitude IS NOT NULL"


def test_days_ago_rejects_negative_float_bool_and_string():
    for bad in (-1, 1.5, True, "7"):
        with pytest.raises(lance_store.MapCompileError):
            _compile(COMPANY, [{"field": "days_since_last_award", "op": "<=", "value": bad}])


def test_days_ago_rejects_equality_op():
    # '=' is not in the field's ops — a point-in-time day count is not a supported axis.
    with pytest.raises(lance_store.MapCompileError):
        _compile(COMPANY, [{"field": "days_since_last_award", "op": "=", "value": 7}])


def test_days_ago_default_today_is_request_date():
    # No injected anchor → resolves against date.today() (the memo-staleness guarantee).
    pred = lance_store.compile_map_filter(
        COMPANY, [{"field": "days_since_last_award", "op": "<=", "value": 0}])
    assert f"latest_award_action_date >= DATE '{date.today().isoformat()}'" in pred


# ── GeoJSON shaping ──────────────────────────────────────────────────────────
def test_to_geojson_shape_and_axis_order():
    rows = [
        {"uei": "ABC", "company_name": "Acme", "longitude": -97.1, "latitude": 31.2,
         "industry": "Construction"},
    ]
    fc = lance_store.to_geojson(COMPANY, rows)
    assert fc["type"] == "FeatureCollection"
    f = fc["features"][0]
    assert f["geometry"] == {"type": "Point", "coordinates": [-97.1, 31.2]}   # [lon, lat]
    assert set(f["properties"]) == set(COMPANY.properties)
    assert f["properties"]["company_name"] == "Acme"


def test_to_geojson_drops_null_coordinate_rows():
    rows = [
        {"uei": "A", "longitude": -97.0, "latitude": 31.0},
        {"uei": "B", "longitude": None, "latitude": 31.0},
        {"uei": "C", "longitude": -97.0, "latitude": None},
    ]
    fc = lance_store.to_geojson(COMPANY, rows)
    assert len(fc["features"]) == 1


# ── decoder integrity (the load-bearing allowlist) ───────────────────────────
def test_decoders_are_internally_consistent():
    for decoder in DECODERS.values():
        for name, spec in decoder.fields.items():
            assert set(spec.ops) <= set(OPS), f"{name}: ops outside global OPS"
            assert spec.type in ("string", "int", "float", "bool", "days_ago")
        for term, clause in decoder.synonyms.items():
            assert clause["field"] in decoder.fields, f"synonym {term!r} → unknown field"
            assert clause["op"] in decoder.fields[clause["field"]].ops
