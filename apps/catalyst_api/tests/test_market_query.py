"""Unit tests for the spine-backed market query engine — pure, no R2 / network.

Pins (1) registry integrity against the live-probed schemas (frozen below, probed
2026-07-05), (2) the fail-closed compiler (off-registry field/op/enum/lane → 422-class
MapCompileError, values escaped, never a predicate breakout), (3) the executor plan
with the Lance I/O seams monkeypatched (UEI-set intersection, empty-set short-circuit,
IN-list chunking, semi-join vs wide-scan crossover, hydration NULLs, meta correctness),
(4) the code typeahead ranking, and (5) the fields payloads (legacy flags + the
workbench-parsable entities entry).
"""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from apps.catalyst_api.src import config, lance_store, market_registry, market_store
from apps.catalyst_api.src.market_registry import ENTITY_FIELDS, LANE_PSEUDO_FIELDS

# Frozen anchor for days_ago clauses — injected so expected DATE literals are stable.
TODAY = date(2026, 7, 5)

# ── Live schemas, probed 2026-07-05 (the registry's ground truth mirror) ──────
ROLLUP_SCHEMA = {
    "uei", "prime_obl_12mo", "prime_obl_24mo", "prime_obl_36mo", "prime_obl_60mo",
    "prime_obl_lifetime", "prime_award_ct_24mo", "prime_award_ct_60mo",
    "prime_award_ct_lifetime", "sub_amt_24mo", "sub_amt_60mo", "sub_amt_lifetime",
    "sub_ct_lifetime", "first_action_date", "last_action_date", "days_since_last_action",
    "distinct_naics_ct", "distinct_psc_ct", "distinct_agency_ct", "top_naics",
    "top_agency_code", "is_prime_24mo", "is_sub_60mo", "prime_and_sub", "as_of",
    "built_from_version", "param_set_id",
}
LANES_SCHEMA = {
    "uei", "side", "code_type", "code", "obl_12mo", "obl_24mo", "obl_60mo",
    "obl_lifetime", "action_ct", "last_action_date",
}
# gtm_entity_geo — written by scripts/build_gtm_entity_geo.py (same PR), built 2026-07-05.
GEO_SCHEMA = {
    "uei", "latitude", "longitude", "geo_precision", "geo_source",
    "as_of", "built_from_version", "param_set_id",
}
ENTITIES_SCHEMA = {
    "uei", "cage_code", "legal_business_name", "normalized_legal_name", "legal_name_base",
    "dba_name", "in_sam", "sam_is_active", "in_dsbs", "is_subawardee", "is_prime_recipient",
    "purpose_of_registration", "initial_registration_date", "registration_expiration_date",
    "exclusion_status_flag", "ever_inactive", "primary_naics", "naics_codes", "psc_codes",
    "business_types", "sba_business_type_codes", "physical_city", "physical_state",
    "physical_zip", "normalized_domain", "domain_source", "sam_extract_label",
    "build_id", "built_at",
}
_SCHEMAS = {"rollup": ROLLUP_SCHEMA, "entities": ENTITIES_SCHEMA}


def _compile(filters):
    """3-slot view (preds, lanes, executed) — the pre-inference contract most tests pin.
    Inferred-aware tests call market_store.compile_market_filters directly."""
    preds, lanes, inferred, executed = market_store.compile_market_filters(filters, today=TODAY)
    assert inferred == []              # these tests never carry inferred clauses
    return preds, lanes, executed


# ── registry integrity ────────────────────────────────────────────────────────
def test_every_field_column_exists_in_its_declared_table():
    for name, spec in ENTITY_FIELDS.items():
        assert spec.source in _SCHEMAS, f"{name}: unknown source {spec.source!r}"
        assert spec.column in _SCHEMAS[spec.source], \
            f"{name}: column {spec.column!r} missing from {spec.source} schema"


def test_lane_contract_columns_exist_in_lanes_schema():
    assert {"uei", "side", "code_type", "code"} <= LANES_SCHEMA
    for col in market_registry.LANE_MIN_OBL_COLUMNS.values():
        assert col in LANES_SCHEMA, f"lane threshold column {col!r} missing from lanes schema"


def test_result_columns_exist_and_row_order_is_their_union():
    assert set(market_registry.RESULT_COLUMNS_ENTITIES) <= ENTITIES_SCHEMA
    assert set(market_registry.RESULT_COLUMNS_ROLLUP) <= ROLLUP_SCHEMA
    assert set(market_registry.RESULT_COLUMNS_GEO) <= GEO_SCHEMA
    union = (set(market_registry.RESULT_COLUMNS_ENTITIES)
             | set(market_registry.RESULT_COLUMNS_ROLLUP)
             | set(market_registry.RESULT_COLUMNS_GEO))
    assert set(market_registry.RESULT_ROW_ORDER) == union
    assert market_registry.RESULT_ROW_ORDER[0] == "uei"


def test_registry_internal_consistency_and_descriptions():
    for name, spec in ENTITY_FIELDS.items():
        assert set(spec.ops) <= set(market_registry.OPS) | {"<=", ">="}, f"{name}: bad ops"
        assert spec.type in ("string", "int", "float", "bool", "days_ago"), f"{name}: bad type"
        assert not spec.gated, f"{name}: no gated axes exist on the entity grain"
        # The descriptions ARE the product: every one states the grain explicitly.
        assert "Grain: one row per UEI" in spec.description, f"{name}: grain missing"
    # money doctrine: every prime money field names the IDV exclusion; every sub-side
    # short window names the FSRS floor.
    for name in ("prime_obl_24mo", "prime_obl_60mo", "prime_obl_lifetime"):
        assert "IDV" in ENTITY_FIELDS[name].description
    for name in ("sub_amt_24mo", "sub_amt_60mo"):
        assert "FLOOR" in ENTITY_FIELDS[name].description.upper()
    # NO fabricated active-contract axis (upstream PoP defect — doctrine).
    assert not any("active_contract" in n for n in ENTITY_FIELDS)


def test_legal_business_name_is_display_only_not_filterable():
    assert "legal_business_name" not in ENTITY_FIELDS
    assert "legal_business_name" in market_registry.RESULT_ROW_ORDER


def test_lane_pseudo_fields_never_shadow_registry_fields():
    assert not set(LANE_PSEUDO_FIELDS) & set(ENTITY_FIELDS)


def test_codes_attribute_is_a_known_system_and_only_on_code_valued_fields():
    # exactly these registry fields are code-valued; every codes value names a system
    # the /market/codes endpoint actually serves.
    assert {n for n, s in ENTITY_FIELDS.items() if s.codes is not None} == \
        {"top_naics", "top_agency_code"}
    assert ENTITY_FIELDS["top_naics"].codes == "naics"
    assert ENTITY_FIELDS["top_agency_code"].codes == "agency"
    for n, s in ENTITY_FIELDS.items():
        if s.codes is not None:
            assert s.codes in market_registry.CODE_SYSTEMS, f"{n}: unknown code system"


def test_state_enum_is_live_probed_usps_codes():
    enum = ENTITY_FIELDS["state"].enum
    assert enum is not None and enum == market_registry.US_STATE_CODES
    assert list(enum) == sorted(enum)                       # sorted, stable
    assert all(len(c) == 2 and c.isupper() for c in enum)   # 2-char uppercase only
    # canonical members present; free-text noise and foreign codes are NOT
    for present in ("VA", "TX", "DC", "PR", "GU", "AE"):
        assert present in enum
    for absent in ("AB", "ON", "QC", "ALABAMA", "ZZ", ""):  # AB/ON/QC live in the raw column
        assert absent not in enum


# ── compiler: scalar clauses split per source table ───────────────────────────
def test_scalar_clauses_split_by_source_and_compile():
    preds, lanes, executed = _compile([
        {"field": "state", "op": "=", "value": "VA"},
        {"field": "prime_obl_24mo", "op": ">=", "value": 1_000_000},
        {"field": "in_dsbs", "op": "=", "value": True},
    ])
    assert preds["entities"] == "physical_state = 'VA' AND in_dsbs = true"
    assert preds["rollup"] == "prime_obl_24mo >= 1000000.0"
    assert lanes == []
    assert executed == [
        {"field": "state", "op": "=", "value": "VA"},
        {"field": "prime_obl_24mo", "op": ">=", "value": 1_000_000},
        {"field": "in_dsbs", "op": "=", "value": True},
    ]


def test_empty_filters_compile_to_no_predicates():
    preds, lanes, executed = _compile([])
    assert preds == {"rollup": None, "entities": None} and lanes == [] and executed == []


def test_days_ago_axis_inverts_onto_last_action_date():
    preds, _, _ = _compile([{"field": "last_action_date", "op": "<=", "value": 90}])
    assert preds["rollup"] == f"last_action_date >= DATE '{(TODAY - timedelta(days=90)).isoformat()}'"


def test_between_desugars_to_inclusive_range():
    preds, _, _ = _compile([{"field": "days_since_last_action", "op": "between", "value": [30, 365]}])
    assert preds["rollup"] == "days_since_last_action >= 30 AND days_since_last_action <= 365"


def test_injection_value_is_escaped_not_executed():
    preds, _, _ = _compile([{"field": "normalized_domain", "op": "=", "value": "x.com' OR '1'='1"}])
    assert preds["entities"] == "normalized_domain = 'x.com'' OR ''1''=''1'"


# ── compiler: fail-closed (every bad clause raises → 422, never reaches Lance) ─
def test_unknown_field_rejected():
    with pytest.raises(lance_store.MapCompileError):
        _compile([{"field": "legal_business_name", "op": "=", "value": "ACME"}])


def test_illegal_op_for_field_rejected():
    with pytest.raises(lance_store.MapCompileError):
        _compile([{"field": "in_dsbs", "op": ">=", "value": True}])


def test_type_mismatch_rejected():
    with pytest.raises(lance_store.MapCompileError):
        _compile([{"field": "in_dsbs", "op": "=", "value": "true"}])
    with pytest.raises(lance_store.MapCompileError):
        _compile([{"field": "prime_obl_24mo", "op": ">=", "value": "1000000"}])
    with pytest.raises(lance_store.MapCompileError):
        _compile([{"field": "prime_award_ct_24mo", "op": ">=", "value": True}])


def test_state_enum_violation_rejected():
    # closed enum: canonical codes compile; anything off-list (foreign province code,
    # spelled-out state, lowercase) is a 422-class error, never a silent zero-row scan.
    preds, _, _ = _compile([{"field": "state", "op": "in", "value": ["VA", "GU"]}])
    assert preds["entities"] == "physical_state IN ('VA', 'GU')"
    for bad in ("ZZ", "AB", "ALABAMA", "va"):
        with pytest.raises(lance_store.MapCompileError):
            _compile([{"field": "state", "op": "=", "value": bad}])
    with pytest.raises(lance_store.MapCompileError):
        _compile([{"field": "state", "op": "in", "value": ["VA", "ON"]}])


def test_clause_with_neither_field_nor_lane_rejected():
    with pytest.raises(lance_store.MapCompileError):
        _compile([{"op": "=", "value": "VA"}])


def test_clause_with_both_field_and_lane_rejected():
    with pytest.raises(lance_store.MapCompileError):
        _compile([{"field": "state", "op": "=", "value": "VA",
                   "lane": {"side": "prime", "code_type": "naics", "codes": ["541512"]}}])


# ── compiler: lane predicates ─────────────────────────────────────────────────
def test_lane_predicate_compiles():
    preds, lanes, executed = _compile([
        {"lane": {"side": "prime", "code_type": "naics", "codes": ["541512", "541511"],
                  "min_obl_24mo": 1_000_000}},
    ])
    assert preds == {"rollup": None, "entities": None}
    assert lanes == [{"side": "prime", "code_type": "naics",
                      "codes": ["541512", "541511"], "min_obl_24mo": 1_000_000}]
    assert executed == [{"lane": lanes[0]}]
    assert market_store._compile_lane_predicate(lanes[0]) == (
        "side = 'prime' AND code_type = 'naics' AND code IN ('541512', '541511') "
        "AND obl_24mo >= 1000000.0"
    )


def test_lane_validation_fail_closed():
    ok = {"side": "prime", "code_type": "naics", "codes": ["541512"]}
    with pytest.raises(lance_store.MapCompileError):        # bad side (enum violation)
        _compile([{"lane": {**ok, "side": "primes"}}])
    with pytest.raises(lance_store.MapCompileError):        # bad code_type (enum violation)
        _compile([{"lane": {**ok, "code_type": "duns"}}])
    with pytest.raises(lance_store.MapCompileError):        # empty codes
        _compile([{"lane": {**ok, "codes": []}}])
    with pytest.raises(lance_store.MapCompileError):        # injection-shaped code fails charset
        _compile([{"lane": {**ok, "codes": ["541512' OR '1'='1"]}}])
    with pytest.raises(lance_store.MapCompileError):        # unknown lane key
        _compile([{"lane": {**ok, "min_obl_12mo": 1}}])
    with pytest.raises(lance_store.MapCompileError):        # negative threshold
        _compile([{"lane": {**ok, "min_obl_lifetime": -1}}])
    with pytest.raises(lance_store.MapCompileError):        # bool threshold
        _compile([{"lane": {**ok, "min_obl_24mo": True}}])
    with pytest.raises(lance_store.MapCompileError):        # lane not an object
        _compile([{"lane": "prime"}])
    with pytest.raises(lance_store.MapCompileError):        # side/code_type/codes required
        _compile([{"lane": {"side": "prime"}}])


def test_lane_pseudo_field_desugars_to_lane():
    _, lanes, executed = _compile([{"field": "prime_naics", "op": "in",
                                    "value": ["541512", "541511"]}])
    assert lanes == [{"side": "prime", "code_type": "naics", "codes": ["541512", "541511"]}]
    # executed echoes the DESUGARED truth, not the pseudo-field sugar.
    assert executed == [{"lane": lanes[0]}]
    _, lanes2, _ = _compile([{"field": "sub_psc", "op": "=", "value": "R425"}])
    assert lanes2 == [{"side": "sub", "code_type": "psc", "codes": ["R425"]}]


def test_lane_pseudo_field_rejects_range_ops_and_bad_values():
    with pytest.raises(lance_store.MapCompileError):
        _compile([{"field": "prime_naics", "op": ">=", "value": "541512"}])
    with pytest.raises(lance_store.MapCompileError):
        _compile([{"field": "prime_naics", "op": "in", "value": []}])
    with pytest.raises(lance_store.MapCompileError):
        _compile([{"field": "prime_naics", "op": "=", "value": 541512}])


# ── executor (Lance I/O seams monkeypatched) ─────────────────────────────────
ROLLUP_ROWS = {
    "UEIAAAAAAAA1": {"uei": "UEIAAAAAAAA1", "prime_obl_24mo": 2_000_000.0,
                     "prime_obl_60mo": 5_000_000.0, "prime_obl_lifetime": 9_000_000.0,
                     "sub_amt_lifetime": 0.0, "last_action_date": date(2026, 6, 1),
                     "top_naics": "541512", "top_agency_code": "097",
                     "is_prime_24mo": True, "is_sub_60mo": False, "prime_and_sub": False},
    "UEIBBBBBBBB2": {"uei": "UEIBBBBBBBB2", "prime_obl_24mo": 1_500_000.0,
                     "prime_obl_60mo": 3_000_000.0, "prime_obl_lifetime": 4_000_000.0,
                     "sub_amt_lifetime": 250_000.0, "last_action_date": date(2026, 5, 20),
                     "top_naics": "541511", "top_agency_code": "047",
                     "is_prime_24mo": True, "is_sub_60mo": True, "prime_and_sub": True},
}
ENTITY_ROWS = {
    "UEIAAAAAAAA1": {"uei": "UEIAAAAAAAA1", "legal_business_name": "ACME FEDERAL LLC",
                     "physical_state": "VA", "normalized_domain": "acmefederal.com",
                     "in_dsbs": True},
    "UEIBBBBBBBB2": {"uei": "UEIBBBBBBBB2", "legal_business_name": "BRAVO SYSTEMS INC",
                     "physical_state": "VA", "normalized_domain": None, "in_dsbs": True},
    "UEICCCCCCCC3": {"uei": "UEICCCCCCCC3", "legal_business_name": "CHARLIE CO",
                     "physical_state": "TX", "normalized_domain": None, "in_dsbs": False},
}
# HQ geo sidecar: ACME rooftop-geocoded, BRAVO county-centroid, CHARLIE has NO geo row
# (ungeocodable — absence, never a fake centroid).
GEO_ROWS = {
    "UEIAAAAAAAA1": {"uei": "UEIAAAAAAAA1", "latitude": 38.8816, "longitude": -77.0910,
                     "geo_precision": "address"},
    "UEIBBBBBBBB2": {"uei": "UEIBBBBBBBB2", "latitude": 37.5407, "longitude": -77.4360,
                     "geo_precision": "county"},
}


class Seams:
    """Recording fakes for the market_store I/O seams. The fake entities table honors
    the two predicate shapes the executor emits (IN-chunk semi-join + plain scan) by
    substring inspection — enough to pin the PLAN, not Lance semantics."""

    def __init__(self):
        self.uei_set_calls: list[tuple[str, str | None]] = []
        self.count_calls: list[tuple[str, str | None]] = []
        self.stream_calls: list[tuple[str, str | None, int]] = []
        self.scan_calls: list[tuple[str, tuple[str, ...], str | None]] = []
        self.lane_sets: list[set[str]] = []
        self.rollup_set: set[str] = set(ROLLUP_ROWS)
        self.entities_matching: set[str] = set(ENTITY_ROWS)

    def _ueis_in_pred(self, predicate: str) -> set[str]:
        return {u for u in list(ROLLUP_ROWS) + list(ENTITY_ROWS) if f"'{u}'" in predicate}

    def uei_set(self, uri, predicate):
        self.uei_set_calls.append((uri, predicate))
        if uri == config.GTM_ENTITY_CODE_LANES_URI:
            return set(self.lane_sets.pop(0))
        if uri == config.GTM_ENTITY_BEHAVIOR_ROLLUP_URI:
            return set(self.rollup_set)
        # entities: semi-join chunk (uei IN (...) AND pred) or wide predicate scan
        if "uei IN (" in (predicate or ""):
            return self._ueis_in_pred(predicate) & self.entities_matching
        return set(self.entities_matching)

    def count_rows(self, uri, predicate):
        self.count_calls.append((uri, predicate))
        return len(self.entities_matching) if predicate else len(ENTITY_ROWS)

    def stream_ueis(self, uri, predicate, limit):
        self.stream_calls.append((uri, predicate, limit))
        return sorted(self.entities_matching)[:limit]

    def scan_to_pylist(self, uri, columns, predicate):
        self.scan_calls.append((uri, tuple(columns), predicate))
        table = {config.GTM_SAM_ENTITIES_URI: ENTITY_ROWS,
                 config.GTM_ENTITY_BEHAVIOR_ROLLUP_URI: ROLLUP_ROWS,
                 config.GTM_ENTITY_GEO_URI: GEO_ROWS}[uri]
        return [{c: row.get(c) for c in columns}
                for u, row in table.items() if f"'{u}'" in (predicate or "")]


@pytest.fixture()
def seams(monkeypatch):
    s = Seams()
    monkeypatch.setattr(market_store, "_uei_set", s.uei_set)
    monkeypatch.setattr(market_store, "_count_rows", s.count_rows)
    monkeypatch.setattr(market_store, "_stream_ueis", s.stream_ueis)
    monkeypatch.setattr(market_store, "_scan_to_pylist", s.scan_to_pylist)
    return s


def test_lane_and_scalar_sets_intersect_and_hydrate(seams):
    seams.lane_sets = [{"UEIAAAAAAAA1", "UEIBBBBBBBB2", "UEIZZZZZZZZ9"}]
    out = market_store.execute_entity_query(
        [{"lane": {"side": "prime", "code_type": "naics", "codes": ["541512"],
                   "min_obl_24mo": 1_000_000}},
         {"field": "in_dsbs", "op": "=", "value": True},
         {"field": "state", "op": "=", "value": "VA"},
         {"field": "prime_obl_24mo", "op": ">=", "value": 1_000_000}],
        limit=10, today=TODAY)
    assert out["total"] == 2 and out["returned"] == 2 and out["capped"] is False
    rows = {r["uei"]: r for r in out["rows"]}
    assert set(rows) == {"UEIAAAAAAAA1", "UEIBBBBBBBB2"}
    # hydration merged both tables; date32 → ISO string on the wire
    assert rows["UEIAAAAAAAA1"]["legal_business_name"] == "ACME FEDERAL LLC"
    assert rows["UEIAAAAAAAA1"]["prime_obl_24mo"] == 2_000_000.0
    assert rows["UEIAAAAAAAA1"]["last_action_date"] == "2026-06-01"
    # every row carries the full wire column set, in order
    assert list(out["rows"][0].keys()) == list(market_registry.RESULT_ROW_ORDER)
    # executed echoes the validated object
    assert out["executed"]["grain"] == "entity"
    assert out["executed"]["filters"][0]["lane"]["codes"] == ["541512"]


def test_empty_lane_set_short_circuits_before_entities_scan(seams):
    seams.lane_sets = [set()]
    out = market_store.execute_entity_query(
        [{"lane": {"side": "sub", "code_type": "psc", "codes": ["R425"]}},
         {"field": "state", "op": "=", "value": "VA"}],
        limit=10, today=TODAY)
    assert out == {**out, "rows": [], "total": 0, "returned": 0, "capped": False}
    # ONLY the lane scan ran — no entities semi-join, no hydration scans
    assert [u for u, _ in seams.uei_set_calls] == [config.GTM_ENTITY_CODE_LANES_URI]
    assert seams.scan_calls == [] and seams.count_calls == []


def test_entities_only_fast_path_uses_count_and_stream(seams):
    out = market_store.execute_entity_query(
        [{"field": "in_dsbs", "op": "=", "value": True}], limit=2, today=TODAY)
    # never materializes the full set: count_rows + streamed limit scan
    assert seams.uei_set_calls == []
    assert seams.count_calls == [(config.GTM_SAM_ENTITIES_URI, "in_dsbs = true")]
    assert seams.stream_calls == [(config.GTM_SAM_ENTITIES_URI, "in_dsbs = true", 2)]
    assert out["total"] == 3 and out["returned"] == 2 and out["capped"] is True


def test_no_filters_serves_the_capped_base_universe(seams):
    out = market_store.execute_entity_query([], limit=2, today=TODAY)
    assert seams.count_calls == [(config.GTM_SAM_ENTITIES_URI, None)]
    assert out["total"] == 3 and out["returned"] == 2 and out["capped"] is True


def test_wide_candidate_set_flips_to_predicate_scan(seams, monkeypatch):
    monkeypatch.setattr(market_store, "SEMI_JOIN_MAX", 1)   # force the crossover
    seams.lane_sets = [{"UEIAAAAAAAA1", "UEIBBBBBBBB2", "UEICCCCCCCC3"}]
    out = market_store.execute_entity_query(
        [{"lane": {"side": "prime", "code_type": "naics", "codes": ["541512"]}},
         {"field": "state", "op": "in", "value": ["VA", "TX"]}],
        limit=10, today=TODAY)
    # entities side ran ONE plain predicate scan (no IN chunks)
    ent_calls = [p for u, p in seams.uei_set_calls if u == config.GTM_SAM_ENTITIES_URI]
    assert ent_calls == ["physical_state IN ('VA', 'TX')"]
    assert out["total"] == 3


def test_in_lists_chunk_at_500(seams, monkeypatch):
    ids = [f"U{i:011d}" for i in range(1_200)]
    preds: list[str] = []
    monkeypatch.setattr(market_store, "_scan_to_pylist",
                        lambda uri, cols, pred: preds.append(pred) or [])
    market_store._rows_by_uei(config.GTM_SAM_ENTITIES_URI, ids,
                              list(market_registry.RESULT_COLUMNS_ENTITIES))
    assert len(preds) == 3                                   # 500 + 500 + 200
    assert all(p.count("', '") <= market_store.IN_CHUNK - 1 for p in preds)
    assert preds[0].startswith("uei IN (") and "U00000000000" in preds[0]
    assert "U00000001199" in preds[-1]


def test_chunks_and_in_predicate_null_guard():
    assert [len(c) for c in market_store._chunks(list(range(1001)), 500)] == [500, 500, 1]
    # NULL/empty ids are guarded out of IN lists; an all-empty list refuses to compile
    assert market_store._in_predicate("uei", ["A", "", "B"]) == "uei IN ('A', 'B')"
    with pytest.raises(lance_store.MapCompileError):
        market_store._in_predicate("uei", ["", ""])


def test_hydration_nulls_for_uei_missing_from_rollup(seams):
    # CHARLIE has no rollup row (no contract behavior): rollup columns hydrate as None.
    seams.entities_matching = {"UEICCCCCCCC3"}
    out = market_store.execute_entity_query(
        [{"field": "state", "op": "=", "value": "TX"}], limit=10, today=TODAY)
    row = out["rows"][0]
    assert row["uei"] == "UEICCCCCCCC3" and row["legal_business_name"] == "CHARLIE CO"
    assert row["prime_obl_24mo"] is None and row["is_prime_24mo"] is None


def test_hydration_left_joins_geo_sidecar(seams):
    # ACME rooftop, BRAVO county centroid, CHARLIE absent from the sidecar → NULLs.
    out = market_store.execute_entity_query(
        [{"field": "state", "op": "in", "value": ["VA", "TX"]}], limit=10, today=TODAY)
    rows = {r["uei"]: r for r in out["rows"]}
    assert rows["UEIAAAAAAAA1"]["latitude"] == 38.8816
    assert rows["UEIAAAAAAAA1"]["longitude"] == -77.0910
    assert rows["UEIAAAAAAAA1"]["geo_precision"] == "address"
    assert rows["UEIBBBBBBBB2"]["geo_precision"] == "county"
    assert rows["UEICCCCCCCC3"]["latitude"] is None
    assert rows["UEICCCCCCCC3"]["longitude"] is None
    assert rows["UEICCCCCCCC3"]["geo_precision"] is None
    # the geo sidecar was hydrated via chunked IN point scans like the other two tables
    assert any(u == config.GTM_ENTITY_GEO_URI for u, _, _ in seams.scan_calls)


def test_to_entity_features_geometry_precision_and_null_preservation(seams):
    out = market_store.execute_entity_query(
        [{"field": "state", "op": "in", "value": ["VA", "TX"]}], limit=10, today=TODAY)
    features, plottable = market_store.to_entity_features(out["rows"])
    assert len(features) == 3 and plottable == 2
    by_uei = {f["properties"]["uei"]: f for f in features}
    # Point geometry in GeoJSON [lon, lat] axis order; geo_precision stays in properties
    acme = by_uei["UEIAAAAAAAA1"]
    assert acme["geometry"] == {"type": "Point", "coordinates": [-77.0910, 38.8816]}
    assert acme["properties"]["geo_precision"] == "address"
    assert by_uei["UEIBBBBBBBB2"]["geometry"] == {"type": "Point",
                                                  "coordinates": [-77.4360, 37.5407]}
    # no geo row → geometry null, row PRESERVED (table view stays complete)
    charlie = by_uei["UEICCCCCCCC3"]
    assert charlie["geometry"] is None
    assert charlie["properties"]["legal_business_name"] == "CHARLIE CO"
    assert charlie["properties"]["geo_precision"] is None
    # latitude/longitude move INTO geometry — never duplicated in properties
    for f in features:
        assert "latitude" not in f["properties"] and "longitude" not in f["properties"]


def test_to_entity_features_partial_coordinates_are_not_plottable():
    # A row with only one coordinate (defensive) must not emit a broken Point.
    rows = [{"uei": "X", "latitude": 31.0, "longitude": None, "geo_precision": "county"}]
    features, plottable = market_store.to_entity_features(rows)
    assert plottable == 0 and features[0]["geometry"] is None


def test_limit_clamped_to_hard_cap(seams):
    out = market_store.execute_entity_query([], limit=10**9, today=TODAY)
    assert out["executed"]["limit"] == market_store.MARKET_HARD_ROW_CAP


# ── code typeahead ────────────────────────────────────────────────────────────
NAICS_FIXTURE = [
    ("23", "Construction"),
    ("236220", "Commercial and Institutional Building Construction"),
    ("5415", "Computer Systems Design and Related Services"),
    ("541511", "Custom Computer Programming Services"),
    ("541512", "Computer Systems Design Services"),
    ("562910", "Remediation Services"),
]


@pytest.fixture()
def codes(monkeypatch):
    monkeypatch.setattr(market_store, "_load_codes",
                        lambda kind: sorted(NAICS_FIXTURE))
    market_store._codes_cache.clear()
    yield
    market_store._codes_cache.clear()


def test_code_prefix_beats_description_substring(codes):
    # '5415' is a code prefix for three codes AND no description contains it; 'construction'
    # only hits descriptions. A mixed probe: '23' prefix-matches 23/236220 and
    # substring-nothing; prefix block must lead and sort shortest-code-first.
    out = market_store.code_search("naics", "5415")
    assert [c["code"] for c in out] == ["5415", "541511", "541512"]
    out2 = market_store.code_search("naics", "computer")
    assert [c["code"] for c in out2] == ["5415", "541511", "541512"]
    out3 = market_store.code_search("naics", "23")
    assert [c["code"] for c in out3] == ["23", "236220"]


def test_code_search_mixed_prefix_and_substring_ranks_prefix_first(codes, monkeypatch):
    monkeypatch.setattr(market_store, "_load_codes",
                        lambda kind: sorted([("562910", "Remediation Services"),
                                             ("R499", "Support services incl 562910 overlap")]))
    market_store._codes_cache.clear()
    out = market_store.code_search("naics", "5629")
    # '5629' prefix-matches 562910; substring-matches R499's description — prefix first.
    assert [c["code"] for c in out] == ["562910", "R499"]


def test_code_search_case_insensitive_and_limit(codes):
    assert market_store.code_search("naics", "CONSTRUCTION", limit=1) == [
        {"code": "23", "description": "Construction"}]


def test_code_search_fail_closed(codes):
    with pytest.raises(lance_store.MapCompileError):
        market_store.code_search("naics", "   ")            # empty q → 422
    with pytest.raises(lance_store.MapCompileError):
        market_store.code_search("duns", "54")              # bad type → 422


# ── agency code system ────────────────────────────────────────────────────────
AGENCY_FIXTURE = [
    ("012", "Department of Agriculture"),
    ("047", "General Services Administration"),
    ("057", "Department of the Air Force"),
    ("097", "Department of Defense"),
    ("9700", "DEPT OF DEFENSE"),
]


@pytest.fixture()
def agency_codes(monkeypatch):
    monkeypatch.setattr(market_store, "_load_codes",
                        lambda kind: sorted(AGENCY_FIXTURE) if kind == "agency"
                        else sorted(NAICS_FIXTURE))
    market_store._codes_cache.clear()
    yield
    market_store._codes_cache.clear()


def test_agency_type_accepted_with_prefix_beats_substring(agency_codes):
    # '097' is a pure code-prefix probe; 'defense' a pure name-substring probe.
    assert [c["code"] for c in market_store.code_search("agency", "097")] == ["097"]
    out2 = market_store.code_search("agency", "defense")
    assert [c["code"] for c in out2] == ["097", "9700"]     # case-insensitive, code-sorted
    # '0' prefix-matches three codes (shortest-first tiebreak is code order at equal len)
    out3 = market_store.code_search("agency", "0")
    assert [c["code"] for c in out3] == ["012", "047", "057", "097"]


def test_agency_ranking_prefix_first_deterministic(agency_codes):
    out = market_store.code_search("agency", "Department")
    # pure name-substring probe → code-sorted ("DEPT OF DEFENSE" does not contain it)
    assert [c["code"] for c in out] == ["012", "057", "097"]


def test_lane_code_type_agency_rejected():
    # 'agency' is a /market/codes system, NOT a lane axis — lanes stay naics|psc.
    with pytest.raises(lance_store.MapCompileError):
        _compile([{"lane": {"side": "prime", "code_type": "agency", "codes": ["097"]}}])


def test_dedupe_agency_pairs_majority_name_wins_null_guarded():
    pairs = {
        ("097", "Department of Defense"): 900,
        ("097", "DEPT OF DEFENSE"): 5,          # historical variant loses on count
        ("020", "Department of the Treasury"): 50,
        ("020", "Department of the Interior"): 50,   # tie → lexicographically first
        (None, "General Services Administration"): 10,  # NULL code guarded out
        ("013", None): 10,                                # NULL name guarded out
        ("012", "Department of Agriculture"): 1,
    }
    out = market_store._dedupe_agency_pairs(pairs)
    assert out == [
        ("012", "Department of Agriculture"),
        ("020", "Department of the Interior"),
        ("097", "Department of Defense"),
    ]


def test_codes_cache_loads_once(codes, monkeypatch):
    calls = {"n": 0}

    def loader(kind):
        calls["n"] += 1
        return sorted(NAICS_FIXTURE)

    monkeypatch.setattr(market_store, "_load_codes", loader)
    market_store._codes_cache.clear()
    market_store.code_search("naics", "54")
    market_store.code_search("naics", "23")
    assert calls["n"] == 1


# ── fields payloads (the workbench contract) ─────────────────────────────────
def test_entities_fields_payload_is_workbench_parsable():
    payload = market_registry.fields_payload()
    assert payload["decoderVersion"] == market_registry.REGISTRY_VERSION
    assert payload["legacy"] is False and payload["grain"] == "entity"
    fields = {f["name"]: f for f in payload["fields"]}
    # every registry field + every lane/inferred pseudo-field is published exactly once
    assert set(fields) == (set(ENTITY_FIELDS) | set(LANE_PSEUDO_FIELDS)
                           | set(market_registry.INFERRED_PSEUDO_FIELDS))
    assert len(payload["fields"]) == len(fields)
    for f in payload["fields"]:
        # the workbench WorkbenchFieldType/WorkbenchOp unions
        assert f["type"] in ("string", "int", "float", "bool", "days_ago", "list")
        assert set(f["ops"]) <= {"=", ">=", "<=", "in", "between", "has", "has_any"}
        assert f["description"]                              # the product surface
    assert fields["prime_naics"]["ops"] == ["=", "in"]
    assert payload["lane"]["sides"] == ["prime", "sub"]
    assert payload["resultColumns"] == list(market_registry.RESULT_ROW_ORDER)


def test_fields_payload_codes_attribute_present_only_on_code_valued_fields():
    # The typeahead contract: `codes` names the /market/codes system on EXACTLY the six
    # code-valued fields; on every other field the key is ABSENT (never null) — the
    # workbench keys the typeahead off key presence.
    fields = {f["name"]: f for f in market_registry.fields_payload()["fields"]}
    expected = {
        "prime_naics": "naics", "sub_naics": "naics",
        "prime_psc": "psc", "sub_psc": "psc",
        "top_naics": "naics", "top_agency_code": "agency",
        "inferred_primeable_naics": "naics", "inferred_primeable_psc": "psc",
        "inferred_subbable_naics": "naics", "inferred_subbable_psc": "psc",
    }
    for name, system in expected.items():
        assert fields[name]["codes"] == system, f"{name}: wrong codes system"
        assert system in market_registry.CODE_SYSTEMS
    for name, f in fields.items():
        if name not in expected:
            assert "codes" not in f, f"{name}: codes key must be omitted entirely"


def test_fields_payload_state_is_closed_enum():
    fields = {f["name"]: f for f in market_registry.fields_payload()["fields"]}
    assert fields["state"]["enum"] == list(market_registry.US_STATE_CODES)


def test_map_fields_payload_marks_legacy_and_carries_entities():
    from apps.catalyst_api.main import map_fields
    import json

    body = json.loads(bytes(map_fields().body))
    datasets = body["data"]["datasets"]
    for legacy in ("winners", "company", "awards", "active", "contracts"):
        assert datasets[legacy]["legacy"] is True
    assert datasets["entities"]["legacy"] is False
    assert datasets["entities"]["decoderVersion"] == market_registry.REGISTRY_VERSION


def test_market_fields_alias_shape():
    from apps.catalyst_api.main import market_fields
    import json

    body = json.loads(bytes(market_fields().body))
    assert set(body["data"]["datasets"]) == {"entities", "prime_awards", "transactions"}


# ═══════════════════════════════════════════════════════════════════════════════
# TABLE GRAINS — prime_awards (award grain) + transactions (raw FPDS action grain)
# ═══════════════════════════════════════════════════════════════════════════════
# Live-probed 2026-07-05 (usaspending_fpds_prime_award_state v22 / _fpds_canonical_txn
# v19) — the subset of each schema the registry touches.
PRIME_AWARD_SCHEMA = {
    "contract_award_unique_key", "award_topology", "idv_type_code", "award_id_piid",
    "awarding_agency_code", "awarding_sub_agency_code", "recipient_uei", "recipient_name",
    "naics_code", "product_or_service_code", "life_to_date_obligated",
    "current_total_value_of_award", "potential_ceiling", "rollup_obligated",
    "rollup_order_count", "first_action_date", "last_action_date", "current_end_date",
    "parent_award_id_piid", "days_to_expiry", "is_terminated",
}
TXN_SCHEMA = {
    "contract_transaction_unique_key", "contract_award_unique_key", "award_id_piid",
    "recipient_uei", "recipient_name", "action_date", "action_type_code",
    "action_type_description", "subcontracting_plan", "subcontracting_plan_desc",
    "federal_action_obligation", "base_and_all_options_value", "naics_code",
    "product_or_service_code", "awarding_agency_code", "awarding_agency_name",
}
_TABLE_SCHEMAS = {"prime_awards": PRIME_AWARD_SCHEMA, "transactions": TXN_SCHEMA}


def test_table_grain_registry_integrity():
    for spec in (market_registry.PRIME_AWARDS, market_registry.TRANSACTIONS):
        schema = _TABLE_SCHEMAS[spec.dataset_key]
        for name, f in spec.fields.items():
            assert f.column in schema, f"{spec.dataset_key}.{name}: column {f.column!r} not in schema"
            assert f.type in ("string", "int", "float", "bool", "days_ago"), name
            assert set(f.ops) <= set(market_registry.OPS), name
            if f.codes is not None:
                assert f.codes in market_registry.CODE_SYSTEMS, name
            assert f.description, name
        assert set(spec.result_columns) <= schema, spec.dataset_key
        assert spec.require_filter is True          # 82.9M / 108M rows — never a full scan
    # probed enum vocabularies, frozen exactly
    assert market_registry.PRIME_AWARD_FIELDS["award_topology"].enum == (
        "standalone", "vehicle", "vehicle_order")
    assert market_registry.TRANSACTION_FIELDS["subcontracting_plan"].enum == (
        "A", "B", "C", "D", "E", "F", "G", "H")     # H (DoD comprehensive) IS live
    assert len(market_registry.TRANSACTION_FIELDS["action_type_code"].enum) == 21
    # code-valued fields carry the typeahead attribute
    assert market_registry.PRIME_AWARD_FIELDS["naics_code"].codes == "naics"
    assert market_registry.PRIME_AWARD_FIELDS["psc_code"].codes == "psc"
    assert market_registry.PRIME_AWARD_FIELDS["awarding_agency_code"].codes == "agency"
    assert market_registry.TRANSACTION_FIELDS["awarding_agency_code"].codes == "agency"
    # grain dispatch accepts singular + plural for both grains
    assert set(market_registry.TABLE_GRAINS) == {
        "prime_award", "prime_awards", "transaction", "transactions"}


def test_table_grain_descriptions_carry_doctrine():
    # award_topology meanings spelled out; txn = per-action deltas, stated.
    topo = market_registry.PRIME_AWARD_FIELDS["award_topology"].description
    for term in ("standalone", "vehicle_order", "IDV", "rollup_obligated"):
        assert term in topo
    assert "per-action delta" in market_registry.TRANSACTION_FIELDS[
        "federal_action_obligation"].description.lower() or \
        "by this action" in market_registry.TRANSACTION_FIELDS[
            "federal_action_obligation"].description.lower()
    # subcontracting plan codes carry the government definitions
    subk = market_registry.TRANSACTION_FIELDS["subcontracting_plan"].description
    for frag in ("INDIVIDUAL SUBCONTRACT PLAN", "COMMERCIAL SUBCONTRACT PLAN",
                 "DOD COMPREHENSIVE", "FAR 19.701"):
        assert frag in subk


def _tcompile(spec, filters):
    return market_store.compile_table_filters(spec, filters, today=TODAY)


def test_table_compile_and_min_one_filter_guard():
    pred, executed = _tcompile(market_registry.PRIME_AWARDS, [
        {"field": "award_topology", "op": "=", "value": "vehicle"},
        {"field": "rollup_obligated", "op": ">=", "value": 100_000_000},
    ])
    assert pred == "award_topology = 'vehicle' AND rollup_obligated >= 100000000.0"
    assert executed == [
        {"field": "award_topology", "op": "=", "value": "vehicle"},
        {"field": "rollup_obligated", "op": ">=", "value": 100_000_000},
    ]
    # the SUBK trigger compiles (enum + days_ago + range on the txn grain)
    pred2, _ = _tcompile(market_registry.TRANSACTIONS, [
        {"field": "subcontracting_plan", "op": "in", "value": ["C", "D", "F", "G"]},
        {"field": "action_date", "op": "<=", "value": 90},
        {"field": "base_and_all_options_value", "op": ">=", "value": 750_000},
    ])
    assert pred2 == ("subcontracting_plan IN ('C', 'D', 'F', 'G') AND "
                     f"action_date >= DATE '{(TODAY - timedelta(days=90)).isoformat()}' AND "
                     "base_and_all_options_value >= 750000.0")
    # EMPTY filters on a require_filter grain → 422-class error, never a full scan
    with pytest.raises(lance_store.MapCompileError):
        _tcompile(market_registry.PRIME_AWARDS, [])
    with pytest.raises(lance_store.MapCompileError):
        _tcompile(market_registry.TRANSACTIONS, [])


def test_table_compile_fail_closed():
    with pytest.raises(lance_store.MapCompileError):    # lane is entity-grain vocabulary
        _tcompile(market_registry.PRIME_AWARDS,
                  [{"lane": {"side": "prime", "code_type": "naics", "codes": ["541512"]}}])
    with pytest.raises(lance_store.MapCompileError):    # unknown field
        _tcompile(market_registry.PRIME_AWARDS, [{"field": "state", "op": "=", "value": "VA"}])
    with pytest.raises(lance_store.MapCompileError):    # topology enum violation
        _tcompile(market_registry.PRIME_AWARDS,
                  [{"field": "award_topology", "op": "=", "value": "idv"}])
    with pytest.raises(lance_store.MapCompileError):    # subk enum violation
        _tcompile(market_registry.TRANSACTIONS,
                  [{"field": "subcontracting_plan", "op": "=", "value": "Z"}])
    with pytest.raises(lance_store.MapCompileError):    # illegal op
        _tcompile(market_registry.PRIME_AWARDS,
                  [{"field": "award_id_piid", "op": "in", "value": ["X", "Y"]}])


# ── table executor (seams stubbed) ────────────────────────────────────────────
PA_ROWS = [
    {"contract_award_unique_key": "CONT_AWD_1", "award_topology": "vehicle",
     "award_id_piid": "GS00Q14OADU101", "parent_award_id_piid": None,
     "recipient_uei": "UEIAAAAAAAA1", "recipient_name": "ACME FEDERAL LLC",
     "naics_code": "541512", "product_or_service_code": "D302",
     "awarding_agency_code": "047", "life_to_date_obligated": 1_000_000.0,
     "current_total_value_of_award": 2_000_000.0, "potential_ceiling": 5_000_000_000.0,
     "rollup_obligated": 250_000_000.0, "rollup_order_count": 120,
     "first_action_date": date(2020, 1, 15), "last_action_date": date(2026, 6, 1),
     "current_end_date": date(2029, 1, 14)},
    {"contract_award_unique_key": "CONT_AWD_2", "award_topology": "vehicle",
     "award_id_piid": "W52P1J18DA000", "parent_award_id_piid": None,
     "recipient_uei": "UEIZZZZZZZZ9", "recipient_name": "NOGEO CORP",
     "naics_code": "541511", "product_or_service_code": "D399",
     "awarding_agency_code": "097", "life_to_date_obligated": 0.0,
     "current_total_value_of_award": 0.0, "potential_ceiling": 900_000_000.0,
     "rollup_obligated": 180_000_000.0, "rollup_order_count": 60,
     "first_action_date": date(2018, 3, 1), "last_action_date": date(2026, 5, 1),
     "current_end_date": None},
]


@pytest.fixture()
def table_seams(monkeypatch):
    calls = {"count": [], "stream": [], "geo": []}

    def fake_count(uri, predicate):
        calls["count"].append((uri, predicate))
        return 41

    def fake_stream(uri, columns, predicate, limit):
        calls["stream"].append((uri, tuple(columns), predicate, limit))
        return [dict(r) for r in PA_ROWS][:limit]

    def fake_rows_by_uei(uri, ueis, columns):
        calls["geo"].append((uri, tuple(ueis), tuple(columns)))
        return {"UEIAAAAAAAA1": dict(GEO_ROWS["UEIAAAAAAAA1"])}   # NOGEO absent

    monkeypatch.setattr(market_store, "_count_rows", fake_count)
    monkeypatch.setattr(market_store, "_stream_rows", fake_stream)
    monkeypatch.setattr(market_store, "_rows_by_uei", fake_rows_by_uei)
    return calls


def test_execute_prime_awards_with_recipient_hq_geometry(table_seams):
    out = market_store.execute_table_query(
        market_registry.PRIME_AWARDS,
        [{"field": "award_topology", "op": "=", "value": "vehicle"},
         {"field": "rollup_obligated", "op": ">=", "value": 100_000_000}],
        limit=10, today=TODAY)
    assert out["total"] == 41 and out["returned"] == 2 and out["capped"] is True
    assert out["executed"] == {"grain": "prime_award", "limit": 10, "filters": [
        {"field": "award_topology", "op": "=", "value": "vehicle"},
        {"field": "rollup_obligated", "op": ">=", "value": 100_000_000}]}
    r1, r2 = out["rows"]
    # rows are keyed result_columns + hydrated geo; dates JSON-shaped
    assert list(r1)[:len(market_registry.PRIME_AWARD_RESULT_COLUMNS)] == \
        list(market_registry.PRIME_AWARD_RESULT_COLUMNS)
    assert r1["last_action_date"] == "2026-06-01"
    assert r1["latitude"] == 38.8816 and r1["geo_precision"] == "address"
    assert r2["latitude"] is None and r2["geo_precision"] is None    # honest absence
    # geo join keyed on recipient_uei against the gtm_entity_geo sidecar
    assert table_seams["geo"][0][0] == config.GTM_ENTITY_GEO_URI
    assert table_seams["geo"][0][1] == ("UEIAAAAAAAA1", "UEIZZZZZZZZ9")
    # adapter shaping: ACME plots, NOGEO stays as a null-geometry table row
    features, plottable = market_store.to_entity_features(out["rows"])
    assert plottable == 1
    assert features[0]["geometry"] == {"type": "Point", "coordinates": [-77.0910, 38.8816]}
    assert features[1]["geometry"] is None
    assert features[0]["properties"]["geo_precision"] == "address"
    assert "latitude" not in features[0]["properties"]


def test_execute_transactions_no_geometry(table_seams, monkeypatch):
    tx_row = {c: None for c in market_registry.TRANSACTION_RESULT_COLUMNS}
    tx_row.update({"contract_transaction_unique_key": "TXN_1", "recipient_uei": "UEIAAAAAAAA1",
                   "subcontracting_plan": "F", "federal_action_obligation": 1_000_000.0,
                   "action_date": date(2026, 6, 20)})
    monkeypatch.setattr(market_store, "_stream_rows",
                        lambda uri, cols, pred, limit: [dict(tx_row)])
    out = market_store.execute_table_query(
        market_registry.TRANSACTIONS,
        [{"field": "subcontracting_plan", "op": "=", "value": "F"}], limit=5, today=TODAY)
    row = out["rows"][0]
    assert list(row) == list(market_registry.TRANSACTION_RESULT_COLUMNS)
    assert "latitude" not in row                       # geometry null in v1 — no geo keys
    assert row["action_date"] == "2026-06-20"
    features, plottable = market_store.to_entity_features(out["rows"])
    assert plottable == 0 and features[0]["geometry"] is None
    # the geo sidecar was NOT consulted for a geometry-less grain
    assert table_seams["geo"] == []


def test_execute_table_zero_total_skips_stream(table_seams, monkeypatch):
    monkeypatch.setattr(market_store, "_count_rows", lambda uri, pred: 0)
    out = market_store.execute_table_query(
        market_registry.TRANSACTIONS,
        [{"field": "subcontracting_plan", "op": "=", "value": "H"}], limit=5, today=TODAY)
    assert out == {**out, "rows": [], "total": 0, "returned": 0, "capped": False}
    assert table_seams["stream"] == []                 # short-circuit: no row scan


def test_table_fields_payloads_and_map_fields_carry_new_datasets():
    import json
    from apps.catalyst_api.main import map_fields, market_fields

    pa = market_registry.table_fields_payload(market_registry.PRIME_AWARDS)
    tx = market_registry.table_fields_payload(market_registry.TRANSACTIONS)
    for payload in (pa, tx):
        assert payload["legacy"] is False and payload["requireFilter"] is True
        assert payload["aggregate"] is None
        names = {f["name"] for f in payload["fields"]}
        assert names == set(
            (market_registry.PRIME_AWARD_FIELDS if payload is pa
             else market_registry.TRANSACTION_FIELDS))
        for f in payload["fields"]:
            assert f["type"] in ("string", "int", "float", "bool", "days_ago", "list")
            if f["name"] in ("naics_code", "psc_code", "awarding_agency_code"):
                assert f["codes"] in market_registry.CODE_SYSTEMS
    # geometry contract: prime_awards hydrates recipient-HQ geo columns; txn does not
    assert pa["geometry"] == "recipient_hq" and tx["geometry"] is None
    assert pa["resultColumns"][-3:] == ["latitude", "longitude", "geo_precision"]
    assert "latitude" not in tx["resultColumns"]
    body = json.loads(bytes(map_fields().body))["data"]["datasets"]
    assert body["prime_awards"]["decoderVersion"] == "prime_awards.v1"
    assert body["transactions"]["decoderVersion"] == "transactions.v1"
    market = json.loads(bytes(market_fields().body))["data"]["datasets"]
    assert set(market) == {"entities", "prime_awards", "transactions"}


# ═══════════════════════════════════════════════════════════════════════════════
# CAPABILITY-INFERENCE LAYER — inferred_* pseudo-fields + is_sub_only_lifetime
# ═══════════════════════════════════════════════════════════════════════════════
def test_is_sub_only_lifetime_compiles_expr_both_branches():
    preds, _, _ = _compile([{"field": "is_sub_only_lifetime", "op": "=", "value": True}])
    assert preds["rollup"] == "(sub_ct_lifetime > 0 AND prime_award_ct_lifetime = 0)"
    preds_f, _, _ = _compile([{"field": "is_sub_only_lifetime", "op": "=", "value": False}])
    assert preds_f["rollup"] == "(sub_ct_lifetime = 0 OR prime_award_ct_lifetime > 0)"
    # '=' with a bool is the only accepted shape
    for bad_op, bad_val in ((">=", True), ("=", "true"), ("in", [True])):
        with pytest.raises(lance_store.MapCompileError):
            _compile([{"field": "is_sub_only_lifetime", "op": bad_op, "value": bad_val}])
    # description states the exact predicate; column anchor exists in the rollup schema
    spec = ENTITY_FIELDS["is_sub_only_lifetime"]
    assert "(sub_ct_lifetime > 0 AND prime_award_ct_lifetime = 0)" in spec.description
    assert spec.column in ROLLUP_SCHEMA


def test_inferred_object_validates_and_compiles():
    preds, lanes, inferred, executed = market_store.compile_market_filters(
        [{"inferred": {"kind": "primeable", "code_type": "naics", "codes": ["562910"],
                       "min_supporting_bothsider_firm_ct": 3}}], today=TODAY)
    assert preds == {"rollup": None, "entities": None} and lanes == []
    assert inferred == [{"kind": "primeable", "code_type": "naics", "codes": ["562910"],
                         "min_supporting_bothsider_firm_ct": 3}]
    assert executed == [{"inferred": inferred[0]}]
    assert market_store._compile_inferred_predicate(inferred[0]) == (
        "code_type = 'naics' AND code IN ('562910') AND supporting_bothsider_firm_ct >= 3")
    # support floor is OPTIONAL — omitted means NO floor clause (pre-weighting doctrine)
    _, _, inf2, _ = market_store.compile_market_filters(
        [{"inferred": {"kind": "subbable", "code_type": "psc", "codes": ["R425", "R499"]}}],
        today=TODAY)
    assert market_store._compile_inferred_predicate(inf2[0]) == (
        "code_type = 'psc' AND code IN ('R425', 'R499')")
    assert "supporting" not in market_store._compile_inferred_predicate(inf2[0])


def test_inferred_validation_fail_closed():
    ok = {"kind": "primeable", "code_type": "naics", "codes": ["562910"]}
    for bad in (
        {**ok, "kind": "possible"},                        # bad kind
        {**ok, "code_type": "agency"},                     # agency is not a lane/inference axis
        {**ok, "codes": []},                               # empty codes
        {**ok, "codes": ["562910' OR '1'='1"]},            # injection-shaped code
        {**ok, "min_supporting_bothsider_firm_ct": 0},     # floor must be >= 1
        {**ok, "min_supporting_bothsider_firm_ct": True},  # bool is not a count
        {**ok, "min_supporting_bothsider_firm_ct": 2.5},   # fractional firms don't exist
        {**ok, "weight": 0.5},                             # unknown key (pre-weighting!)
        {"kind": "primeable"},                             # missing required keys
        "primeable",                                       # not an object
    ):
        with pytest.raises(lance_store.MapCompileError):
            market_store.compile_market_filters([{"inferred": bad}], today=TODAY)
    # a clause carrying two predicate kinds at once is rejected
    with pytest.raises(lance_store.MapCompileError):
        market_store.compile_market_filters(
            [{"field": "state", "op": "=", "value": "VA", "inferred": ok}], today=TODAY)
    with pytest.raises(lance_store.MapCompileError):
        market_store.compile_market_filters(
            [{"lane": {"side": "sub", "code_type": "naics", "codes": ["541512"]},
              "inferred": ok}], today=TODAY)


def test_inferred_pseudo_fields_desugar():
    _, _, inferred, executed = market_store.compile_market_filters(
        [{"field": "inferred_primeable_naics", "op": "=", "value": "562910"}], today=TODAY)
    assert inferred == [{"kind": "primeable", "code_type": "naics", "codes": ["562910"]}]
    assert executed == [{"inferred": inferred[0]}]        # desugared truth echoed
    _, _, inf2, _ = market_store.compile_market_filters(
        [{"field": "inferred_subbable_psc", "op": "in", "value": ["R425", "R499"]}], today=TODAY)
    assert inf2 == [{"kind": "subbable", "code_type": "psc", "codes": ["R425", "R499"]}]
    with pytest.raises(lance_store.MapCompileError):      # range ops rejected
        market_store.compile_market_filters(
            [{"field": "inferred_primeable_naics", "op": ">=", "value": "5"}], today=TODAY)
    # pseudo-fields never shadow registry fields or lane pseudo-fields
    assert not set(market_registry.INFERRED_PSEUDO_FIELDS) & set(ENTITY_FIELDS)
    assert not set(market_registry.INFERRED_PSEUDO_FIELDS) & set(LANE_PSEUDO_FIELDS)


def test_inferred_set_intersects_with_scalar_and_lane_sets(seams, monkeypatch):
    inferred_calls = []
    orig = seams.uei_set

    def routed(uri, predicate):
        if uri in (config.GTM_INFERRED_PRIMEABLE_URI, config.GTM_INFERRED_SUBBABLE_URI):
            inferred_calls.append((uri, predicate))
            return {"UEIAAAAAAAA1", "UEIZZZZZZZZ9"}
        return orig(uri, predicate)

    monkeypatch.setattr(market_store, "_uei_set", routed)
    out = market_store.execute_entity_query(
        [{"field": "is_sub_only_lifetime", "op": "=", "value": True},
         {"inferred": {"kind": "primeable", "code_type": "naics", "codes": ["562910"]}}],
        limit=10, today=TODAY)
    # inferred set {A, Z} ∩ rollup expr set {A, B} → {A}
    assert inferred_calls == [(config.GTM_INFERRED_PRIMEABLE_URI,
                               "code_type = 'naics' AND code IN ('562910')")]
    assert out["total"] == 1 and out["rows"][0]["uei"] == "UEIAAAAAAAA1"
    # the rollup side ran the compiled expression, not a plain column comparison
    rollup_preds = [p for u, p in seams.uei_set_calls
                    if u == config.GTM_ENTITY_BEHAVIOR_ROLLUP_URI]
    assert rollup_preds == ["(sub_ct_lifetime > 0 AND prime_award_ct_lifetime = 0)"]


def test_inferred_payload_contract():
    payload = market_registry.fields_payload()
    fields = {f["name"]: f for f in payload["fields"]}
    for name, (kind, ct) in market_registry.INFERRED_PSEUDO_FIELDS.items():
        f = fields[name]
        assert f["ops"] == ["=", "in"] and f["source"] == "inferred"
        assert f["codes"] == ct                            # typeahead lights up
        # verb doctrine verbatim: inference is never a demonstration
        assert "NOT a demonstration" in f["description"]
        if kind == "primeable":
            assert "never a claim of what it can prime" in f["description"]
    inf = payload["inferred"]
    assert inf["kinds"] == ["primeable", "subbable"]
    assert inf["minSupportKey"] == "min_supporting_bothsider_firm_ct"
    assert "pre-weighting" in inf["semantics"]
