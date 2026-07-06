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
    return market_store.compile_market_filters(filters, today=TODAY)


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
    union = set(market_registry.RESULT_COLUMNS_ENTITIES) | set(market_registry.RESULT_COLUMNS_ROLLUP)
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
        table = ENTITY_ROWS if uri == config.GTM_SAM_ENTITIES_URI else ROLLUP_ROWS
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
    # every registry field + every lane pseudo-field is published exactly once
    assert set(fields) == set(ENTITY_FIELDS) | set(LANE_PSEUDO_FIELDS)
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
    assert set(body["data"]["datasets"]) == {"entities"}
