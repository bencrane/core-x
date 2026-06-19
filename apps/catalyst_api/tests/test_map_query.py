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
from apps.catalyst_api.src.map_decoders import AWARDS, COMPANY, DECODERS, WINNERS, OPS

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
    assert pred == "naics2 = '23' AND total_obligation >= 150000.0"


def test_company_bool_literal_is_unquoted():
    pred = _compile(COMPANY, [{"field": "has_federal_awards", "op": "=", "value": True}])
    # Arrow boolean literal, NOT the string 'true'
    assert pred == "has_federal_awards = true"


def test_in_clause():
    pred = _compile(WINNERS, [{"field": "naics2", "op": "in", "value": ["23", "11"]}])
    assert pred == "naics2 IN ('23', '11')"


def test_between_desugars_to_inclusive_range():
    pred = _compile(COMPANY, [{"field": "founded_year", "op": "between", "value": [2010, 2020]}])
    assert pred == "founded_year >= 2010 AND founded_year <= 2020"


def test_query_name_state_maps_to_physical_address_state():
    pred = _compile(COMPANY, [{"field": "state", "op": "=", "value": "TX"}])
    assert pred == "physical_address_state = 'TX'"


def test_empty_filters_compile_to_none():
    # No clauses -> None (unfiltered scan). The scan is no longer restricted to
    # plottable rows: ungeocoded qualifying rows must reach the TABLE view.
    assert _compile(COMPANY, []) is None


def test_multi_clause_and():
    pred = _compile(COMPANY, [
        {"field": "naics2", "op": "=", "value": "23"},
        {"field": "has_federal_awards", "op": "=", "value": True},
        {"field": "state", "op": "in", "value": ["TX", "CA"]},
    ])
    assert pred == ("naics2 = '23' AND has_federal_awards = true "
                    "AND physical_address_state IN ('TX', 'CA')")


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
    assert pred == "employee_size_band = '51-200'"


def test_company_type_valid_enum_compiles():
    pred = _compile(COMPANY, [{"field": "company_type", "op": "=", "value": "Nonprofit"}])
    assert pred == "company_type = 'Nonprofit'"


def test_injection_value_is_escaped_not_executed():
    pred = _compile(COMPANY, [{"field": "state", "op": "=", "value": "TX' OR '1'='1"}])
    # the quote is doubled → a harmless string literal; no predicate breakout
    assert pred == "physical_address_state = 'TX'' OR ''1''=''1'"
    assert "''" in pred


# ── days_ago: relative-time clauses resolve to DATE literals at request time ──
def test_company_won_within_days_inverts_onto_date_column():
    # "won in the last 7 days" → days_since <= 7 → action date ON/AFTER today-7
    pred = _compile(COMPANY, [{"field": "days_since_last_award", "op": "<=", "value": 7}])
    assert pred == "latest_award_action_date >= DATE '2026-06-05'"


def test_winners_won_within_one_day():
    pred = _compile(WINNERS, [{"field": "days_since_last_award", "op": "<=", "value": 1}])
    assert pred == "last_action_date >= DATE '2026-06-11'"


def test_days_ago_gte_means_on_or_before_cutoff():
    # "no award in over a year" → days_since >= 365 → action date ON/BEFORE today-365
    pred = _compile(COMPANY, [{"field": "days_since_last_award", "op": ">=", "value": 365}])
    assert pred == "latest_award_action_date <= DATE '2025-06-12'"


def test_days_ago_between_is_a_days_window():
    # 7..30 days ago → dates today-30 .. today-7
    pred = _compile(COMPANY, [{"field": "days_since_last_award", "op": "between", "value": [7, 30]}])
    assert pred == ("latest_award_action_date >= DATE '2026-05-13' AND "
                    "latest_award_action_date <= DATE '2026-06-05'")


def test_days_ago_zero_is_today():
    pred = _compile(COMPANY, [{"field": "days_since_last_award", "op": "<=", "value": 0}])
    assert pred == "latest_award_action_date >= DATE '2026-06-12'"


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


# ── awards.v1: the award-EVENT axis ──────────────────────────────────────────
def test_awards_acceptance_sentence_compiles():
    # "construction companies in Texas that won an award over $1M in the last 30 days"
    pred = _compile(AWARDS, [
        {"field": "naics2", "op": "=", "value": "23"},
        {"field": "state", "op": "=", "value": "TX"},
        {"field": "award_amount", "op": ">=", "value": 1000000},
        {"field": "days_since_action", "op": "<=", "value": 30},
    ])
    assert pred == ("naics2 = '23' AND state = 'TX' AND award_amount >= 1000000.0 "
                    "AND action_date >= DATE '2026-05-13'")


def test_awards_pop_state_is_distinct_from_recipient_state():
    pred = _compile(AWARDS, [{"field": "pop_state", "op": "=", "value": "TX"}])
    assert pred == "pop_state = 'TX'"
    pred2 = _compile(AWARDS, [{"field": "state", "op": "=", "value": "TX"}])
    assert pred2 == "state = 'TX'"


def test_awards_city_and_agency_compile():
    pred = _compile(AWARDS, [
        {"field": "city", "op": "=", "value": "DALLAS"},
        {"field": "awarding_agency", "op": "=", "value": "Department of Veterans Affairs"},
    ])
    assert pred == "city = 'DALLAS' AND awarding_agency = 'Department of Veterans Affairs'"


def test_awards_set_aside_enum_enforced():
    pred = _compile(AWARDS, [{"field": "set_aside", "op": "in", "value": ["8A", "8AN"]}])
    assert pred == "set_aside IN ('8A', '8AN')"
    with pytest.raises(lance_store.MapCompileError):
        _compile(AWARDS, [{"field": "set_aside", "op": "=", "value": "8(a)"}])


def test_awards_amount_equality_rejected():
    # '=' on a dollar amount is not a supported axis (range ops only).
    with pytest.raises(lance_store.MapCompileError):
        _compile(AWARDS, [{"field": "award_amount", "op": "=", "value": 1000000}])


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


def test_to_geojson_emits_null_geometry_for_ungeocoded_rows():
    # A qualifying row without coords reaches the table as a geometry:null feature
    # (valid GeoJSON per RFC 7946 §3.2); the dot layer skips it.
    rows = [
        {"uei": "A", "longitude": -97.0, "latitude": 31.0},
        {"uei": "B", "longitude": None, "latitude": 31.0},
        {"uei": "C", "longitude": -97.0, "latitude": None},
    ]
    fc = lance_store.to_geojson(COMPANY, rows)
    assert len(fc["features"]) == 3
    geoms = [f["geometry"] for f in fc["features"]]
    assert geoms[0] == {"type": "Point", "coordinates": [-97.0, 31.0]}
    assert geoms[1] is None and geoms[2] is None


# ── decoder integrity (the load-bearing allowlist) ───────────────────────────
def test_decoders_are_internally_consistent():
    for decoder in DECODERS.values():
        for name, spec in decoder.fields.items():
            assert set(spec.ops) <= set(OPS), f"{name}: ops outside global OPS"
            assert spec.type in ("string", "int", "float", "bool", "days_ago", "list")
            # list ops are has/has_any only; non-list fields never use them.
            if spec.type == "list":
                assert set(spec.ops) <= {"has", "has_any"}, f"{name}: list field uses scalar ops"
            else:
                assert not ({"has", "has_any"} & set(spec.ops)), f"{name}: non-list field uses list ops"
        for term, clause in decoder.synonyms.items():
            assert clause["field"] in decoder.fields, f"synonym {term!r} → unknown field"
            assert clause["op"] in decoder.fields[clause["field"]].ops


# ── PHASE 3: capability list axis (array_has) ────────────────────────────────
def test_capability_tag_has_compiles_with_scope_gate():
    # "winners that do electrical work" → array_has + the deterministic scope gate.
    pred = _compile(WINNERS, [{"field": "capability_tag", "op": "has", "value": "electrical_systems"}])
    assert pred == ("array_has(capability_tags, 'electrical_systems') "
                    "AND has_extracted_scope = true")


def test_labor_category_has_any_compiles():
    pred = _compile(WINNERS, [
        {"field": "labor_category", "op": "has_any", "value": ["electrician", "welder"]},
    ])
    assert pred == ("array_has_any(labor_categories, ['electrician', 'welder']) "
                    "AND has_extracted_scope = true")


def test_capability_tag_enum_violation_rejected():
    with pytest.raises(lance_store.MapCompileError):
        _compile(WINNERS, [{"field": "capability_tag", "op": "has", "value": "not_a_tag"}])


def test_capability_list_has_requires_string_value():
    with pytest.raises(lance_store.MapCompileError):
        _compile(WINNERS, [{"field": "capability_tag", "op": "has", "value": ["electrical_systems"]}])


def test_capability_list_rejects_scalar_op():
    # '=' is not in a list field's ops (has/has_any only).
    with pytest.raises(lance_store.MapCompileError):
        _compile(WINNERS, [{"field": "capability_tag", "op": "=", "value": "electrical_systems"}])


def test_capability_value_is_escaped_not_executed():
    # enum-bounded so a raw injection won't pass the enum check; but the escaping path
    # still doubles quotes (defense in depth) — assert via an unbounded-style probe by
    # confirming a valid value emits a single-quoted, array_has literal.
    pred = _compile(WINNERS, [{"field": "labor_category", "op": "has", "value": "electrician"}])
    assert pred.startswith("array_has(labor_categories, 'electrician')")


# ── PHASE 3: the has_extracted_scope safety gate (anti-pattern #4) ────────────
def test_clearance_filter_auto_ands_scope_gate():
    pred = _compile(WINNERS, [{"field": "requires_clearance", "op": "=", "value": True}])
    assert pred == "requires_clearance = true AND has_extracted_scope = true"


def test_clearance_level_in_auto_ands_scope_gate():
    pred = _compile(WINNERS, [
        {"field": "req_clearance_level_max", "op": "in", "value": ["SECRET", "TOP_SECRET", "TS_SCI"]},
    ])
    assert pred == ("req_clearance_level_max IN ('SECRET', 'TOP_SECRET', 'TS_SCI') "
                    "AND has_extracted_scope = true")


def test_gate_not_double_added_when_caller_filters_scope():
    pred = _compile(WINNERS, [
        {"field": "has_extracted_scope", "op": "=", "value": True},
        {"field": "requires_cmmc", "op": "=", "value": True},
    ])
    # exactly one has_extracted_scope clause (the caller's) — gate not re-appended.
    assert pred == "has_extracted_scope = true AND requires_cmmc = true"
    assert pred.count("has_extracted_scope") == 1


def test_non_capability_query_has_no_scope_gate():
    # A plain firmographic winners query must NOT acquire the scope gate (it would filter
    # the full universe through the ~1% coverage ceiling).
    pred = _compile(WINNERS, [{"field": "naics2", "op": "=", "value": "23"}])
    assert pred == "naics2 = '23'"
    assert "has_extracted_scope" not in pred


def test_has_extracted_scope_alone_is_not_gated():
    # has_extracted_scope is the gate target, not a gated field — no self-AND.
    pred = _compile(WINNERS, [{"field": "has_extracted_scope", "op": "=", "value": True}])
    assert pred == "has_extracted_scope = true"


def test_northstar_construction_secret_electrical_compiles():
    # "construction winners requiring SECRET clearance with cleared electrical trades"
    pred = _compile(WINNERS, [
        {"field": "naics2", "op": "=", "value": "23"},
        {"field": "req_clearance_level_max", "op": "in", "value": ["SECRET", "TOP_SECRET", "TS_SCI"]},
        {"field": "labor_category", "op": "has", "value": "electrician"},
    ])
    assert pred == ("naics2 = '23' "
                    "AND req_clearance_level_max IN ('SECRET', 'TOP_SECRET', 'TS_SCI') "
                    "AND array_has(labor_categories, 'electrician') "
                    "AND has_extracted_scope = true")


# ── SUB-only teaming axis: the /ask teaming queries compile to valid Lance predicates ──
def test_teaming_dollars_threshold_compiles_no_scope_gate():
    # "subs with $10M+ teaming" → a plain range predicate; teaming is NOT gated, so the
    # has_extracted_scope coverage gate must NOT be appended (it would wrongly drop subs).
    pred = _compile(WINNERS, [{"field": "teaming_dollars_5y", "op": ">=", "value": 10000000}])
    assert pred == "teaming_dollars_5y >= 10000000.0"
    assert "has_extracted_scope" not in pred


def test_n_teaming_primes_between_compiles():
    pred = _compile(WINNERS, [{"field": "n_teaming_primes", "op": "between", "value": [2, 10]}])
    assert pred == "n_teaming_primes >= 2 AND n_teaming_primes <= 10"


def test_teaming_prime_has_compiles_to_array_has():
    # exact stored legal name → array_has; NOT gated, so no scope gate.
    pred = _compile(WINNERS, [
        {"field": "teaming_prime", "op": "has", "value": "LOCKHEED MARTIN CORPORATION"}])
    assert pred == "array_has(teaming_prime_names, 'LOCKHEED MARTIN CORPORATION')"
    assert "has_extracted_scope" not in pred


def test_teaming_prime_has_any_compiles_to_array_has_any():
    pred = _compile(WINNERS, [
        {"field": "teaming_prime", "op": "has_any",
         "value": ["LOCKHEED MARTIN CORPORATION", "LOCKHEED MARTIN CORP"]}])
    assert pred == ("array_has_any(teaming_prime_names, "
                    "['LOCKHEED MARTIN CORPORATION', 'LOCKHEED MARTIN CORP'])")


def test_teamed_with_lockheed_via_synonym_resolves_to_canonical_variants():
    # The NL phrase "teamed with lockheed" decodes through the WINNERS synonym to a has_any
    # over the EXACT stored variants (so "Lockheed" actually hits "LOCKHEED MARTIN CORPORATION").
    syn = WINNERS.synonyms["lockheed"]
    pred = _compile(WINNERS, [syn])
    assert pred == ("array_has_any(teaming_prime_names, "
                    "['LOCKHEED MARTIN CORPORATION', 'LOCKHEED MARTIN CORP'])")


def test_teaming_prime_has_rejects_non_string():
    with pytest.raises(lance_store.MapCompileError):
        _compile(WINNERS, [{"field": "teaming_prime", "op": "has", "value": 123}])


def test_teaming_prime_value_is_escaped_not_executed():
    # open-valued (no enum) → the _sql_str quote-doubling is the only escaping; assert it holds.
    pred = _compile(WINNERS, [{"field": "teaming_prime", "op": "has", "value": "ACME' OR '1'='1"}])
    assert pred == "array_has(teaming_prime_names, 'ACME'' OR ''1''=''1')"


def test_teaming_fields_are_not_gated():
    # Teaming is on every profiled sub (not the scope-extracted slice) — must NOT be gated.
    for name in ("teaming_dollars_5y", "n_teaming_primes", "teaming_prime"):
        assert WINNERS.fields[name].gated is False, f"{name} must not be gated"


# ── SUB-only SELF-REPORTED axis: the /ask self-report + cert queries compile + stay UNGATED ──
def test_self_reported_capability_has_compiles_no_scope_gate():
    # "subs that self-report software development" → array_has on the self-reported column with
    # NO has_extracted_scope gate (the whole point: it must reach the 13,792-sub long tail, not
    # the ~4,220 scope-extracted slice the gated capability_tag axis would clip to).
    pred = _compile(WINNERS, [
        {"field": "self_reported_capability_tag", "op": "has", "value": "software_development"}])
    assert pred == "array_has(self_reported_capability_tags, 'software_development')"
    assert "has_extracted_scope" not in pred


def test_self_reported_capability_distinct_from_gated_capability_tag():
    # The gated capability_tag axis DOES acquire the scope gate; the self-reported axis does not —
    # proving they are distinct fields with opposite gating, never collapsed.
    gated_pred = _compile(WINNERS, [{"field": "capability_tag", "op": "has", "value": "software_development"}])
    assert gated_pred == ("array_has(capability_tags, 'software_development') "
                          "AND has_extracted_scope = true")
    sr_pred = _compile(WINNERS, [{"field": "self_reported_capability_tag", "op": "has", "value": "software_development"}])
    assert "has_extracted_scope" not in sr_pred


def test_self_reported_capability_reuses_controlled_vocab_enum():
    # Same 77-tag enum as capability_tag → an off-vocab value is rejected (→ 422), never reaches Lance.
    with pytest.raises(lance_store.MapCompileError):
        _compile(WINNERS, [{"field": "self_reported_capability_tag", "op": "has", "value": "not_a_tag"}])


def test_self_reported_capability_via_synonym_compiles_ungated():
    syn = WINNERS.synonyms["self-reported aircraft maintenance"]
    pred = _compile(WINNERS, [syn])
    assert pred == "array_has(self_reported_capability_tags, 'aircraft_maintenance')"
    assert "has_extracted_scope" not in pred


def test_req_cert_tag_has_any_compiles_no_scope_gate():
    # "subs with an ISO 9001 / CMMC / AS9100 certification" → array_has_any over exact cert tokens;
    # open-valued (no enum) → any string token is accepted; UNGATED.
    pred = _compile(WINNERS, [
        {"field": "req_cert_tag", "op": "has_any", "value": ["iso_9001", "as9100", "cmmc"]}])
    assert pred == "array_has_any(req_cert_tags, ['iso_9001', 'as9100', 'cmmc'])"
    assert "has_extracted_scope" not in pred


def test_req_cert_tag_synonym_resolves_to_exact_tokens():
    # A cert synonym ("cmmc certification") decodes to has_any over every observed CMMC token.
    syn = WINNERS.synonyms["cmmc certification"]
    pred = _compile(WINNERS, [syn])
    assert pred == "array_has_any(req_cert_tags, ['cmmc', 'cmmc_l1', 'cmmc_l2'])"
    assert "has_extracted_scope" not in pred


def test_req_cert_tag_is_open_valued_and_escaped():
    # Open-valued (no enum) → _sql_str quote-doubling is the only escaping; assert it holds.
    pred = _compile(WINNERS, [{"field": "req_cert_tag", "op": "has", "value": "iso_9001' OR '1'='1"}])
    assert pred == "array_has(req_cert_tags, 'iso_9001'' OR ''1''=''1')"


def test_self_reported_and_cert_fields_are_not_gated():
    for name in ("self_reported_capability_tag", "req_cert_tag"):
        assert WINNERS.fields[name].gated is False, f"{name} must not be gated"
    # And the self-reported enum is the SAME object/value-set as the gated capability_tag enum.
    assert WINNERS.fields["self_reported_capability_tag"].enum == WINNERS.fields["capability_tag"].enum


# ── PHASE 3: CUI egress invariant — chunk-derived text never in any decoder ───
def test_no_chunk_derived_text_in_catalyst_decoders():
    banned = {"evidence_quote", "requirement_detail", "scope_summary"}
    for decoder in DECODERS.values():
        for spec in decoder.fields.values():
            assert spec.column not in banned, f"{spec.column} is chunk-derived text — never a filter column"
        assert not (set(decoder.properties) & banned), f"{decoder.dataset_key}: chunk-derived text in properties"
