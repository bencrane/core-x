"""Acceptance tests for the deterministic phrase compiler (phrase.v1) — pure, no
R2 / network. THE FIXTURE SET IS THE PLAN'S CSV
(~/Desktop/hq/plans/2026-07-06-phrase-compiler-examples.csv): every runnable row's
canonical phrase is pinned here to its EXACT emitted plan (verbatim wire bodies),
plus the v1.1 amendment behaviors — reserved booleans refuse (same-axis 'or'
carve-out compiles to an in-list), bounded time (calendar year / FFY / since)
emits days-ago between-clauses against the injectable today, subject-less phrases
refuse, sub-tier agency names refuse with the Cycle-3 pointer, and EVERY token is
accounted for in meta.bindings (nothing is ever silently dropped)."""

from __future__ import annotations

from datetime import date

import pytest

from apps.catalyst_api.src import market_registry, market_store, phrase_compiler
from apps.catalyst_api.src.lance_store import MapCompileError
from apps.catalyst_api.src.phrase_compiler import CONSTRUCTION_NAICS

TODAY = date(2026, 7, 6)

KNOWN_NAICS = set(CONSTRUCTION_NAICS) | {
    "541511", "541512", "541513", "541519", "541330", "541690", "561612",
    "561621", "561720", "561210", "561730", "237310",
}
KNOWN_PSC = {"R499", "D302", "Y1AA"}


@pytest.fixture(autouse=True)
def _stub_reference_dims(monkeypatch):
    monkeypatch.setattr(phrase_compiler, "_known_naics", lambda: KNOWN_NAICS)
    monkeypatch.setattr(phrase_compiler, "_known_psc", lambda: KNOWN_PSC)


def _plan(phrase: str):
    return phrase_compiler.compile_phrase(phrase, today=TODAY)["plan"]


def _tokens_accounted(phrase: str):
    compiled = phrase_compiler.compile_phrase(phrase, today=TODAY)
    bound = [t for b in compiled["bindings"] for t in b["tokens"]]
    assert bound == phrase_compiler._lex(phrase), "every token binds exactly once"
    return compiled


# ── The CSV rows, pinned verbatim ──────────────────────────────────────────────
CONSTR = list(CONSTRUCTION_NAICS)


def test_ex01_flagship_code_a_construction_companies():
    compiled = _tokens_accounted(
        "construction companies that received a code A mod in the last 90 days")
    assert compiled["grain"] == "entity"
    assert compiled["plan"] == [
        {"grain": "transaction", "collapse": "recipients",
         "filters": [
             {"field": "action_type_code", "op": "=", "value": "A"},
             {"field": "naics_code", "op": "in", "value": CONSTR},
             {"field": "action_date", "op": "<=", "value": 90}],
         "limit": 1000},
        {"grain": "entity",
         "filters": [{"field": "uei", "op": "in",
                      "value": ["<step1 ueis, chunks of <=500>"]}],
         "limit": 1000},
    ]


def test_ex02_dsbs_variant_adds_the_entity_gate():
    plan = _plan("construction companies in dsbs that received a code A mod "
                 "in the last 90 days")
    assert plan[1]["filters"] == [
        {"field": "uei", "op": "in", "value": ["<step1 ueis, chunks of <=500>"]},
        {"field": "in_dsbs", "op": "=", "value": True}]


def test_ex03_terminated_for_default_last_year():
    plan = _plan("companies terminated for default in the last year")
    assert plan[0]["filters"] == [
        {"field": "action_type_code", "op": "=", "value": "E"},
        {"field": "action_date", "op": "<=", "value": 365}]
    assert plan[0]["collapse"] == "recipients"


def test_ex04_it_services_option_exercised_last_quarter():
    plan = _plan("it services companies whose option was exercised in the last quarter")
    assert plan[0]["filters"] == [
        {"field": "action_type_code", "op": "=", "value": "G"},
        {"field": "naics_code", "op": "in",
         "value": ["541511", "541512", "541513", "541519"]},
        {"field": "action_date", "op": "<=", "value": 90}]


def test_ex05_novated_last_2_years():
    plan = _plan("companies novated in the last 2 years")
    assert plan[0]["filters"] == [
        {"field": "action_type_code", "op": "=", "value": "J"},
        {"field": "action_date", "op": "<=", "value": 730}]


def test_ex06_vehicles_carrying_naics():
    compiled = _tokens_accounted("vehicles carrying naics 541512")
    assert compiled["plan"] == [
        {"grain": "prime_award",
         "filters": [
             {"field": "award_topology", "op": "=", "value": "vehicle"},
             {"field": "naics_code", "op": "=", "value": "541512"}],
         "limit": 1000}]


def test_ex07_orders_under_vehicle_piid():
    plan = _plan("orders under vehicle 47QFCA20D0001 acted in the last 180 days")
    assert plan == [
        {"grain": "prime_award",
         "filters": [
             {"field": "award_topology", "op": "=", "value": "vehicle_order"},
             {"field": "parent_award_id_piid", "op": "=", "value": "47QFCA20D0001"},
             {"field": "last_action_date", "op": "<=", "value": 180}],
         "limit": 1000}]


def test_ex08_actions_with_a_subcontracting_plan():
    plan = _plan("actions with a subcontracting plan in psc R499 in the last 180 days")
    assert plan == [
        {"grain": "transaction",
         "filters": [
             {"field": "subcontracting_plan", "op": "in",
              "value": ["C", "D", "E", "F", "G", "H"]},
             {"field": "psc_code", "op": "=", "value": "R499"},
             {"field": "action_date", "op": "<=", "value": 180}],
         "limit": 1000}]


def test_ex09_code_y_on_construction_last_year():
    # 'awards' after the subject is a connective no-op (first subject wins)
    plan = _plan("companies that received a code Y mod on construction awards "
                 "in the last year")
    assert plan[0]["filters"] == [
        {"field": "action_type_code", "op": "=", "value": "Y"},
        {"field": "naics_code", "op": "in", "value": CONSTR},
        {"field": "action_date", "op": "<=", "value": 365}]


def test_ex10_entity_only_lane_cut():
    compiled = _tokens_accounted(
        "dsbs companies in VA that delivered subs under naics 236220")
    assert compiled["plan"] == [
        {"grain": "entity",
         "filters": [
             {"field": "in_dsbs", "op": "=", "value": True},
             {"field": "sub_naics", "op": "=", "value": "236220"},
             {"field": "state", "op": "=", "value": "VA"}],
         "limit": 1000}]


def test_ex11_primed_in_and_also_sub():
    plan = _plan("companies that primed in 541690 and also sub")
    assert plan == [
        {"grain": "entity",
         "filters": [
             {"field": "prime_naics", "op": "=", "value": "541690"},
             {"field": "prime_and_sub", "op": "=", "value": True}],
         "limit": 1000}]


def test_ex12_sub_only_inferred_primeable():
    plan = _plan("sub-only companies with inferred primeable 541330")
    assert plan == [
        {"grain": "entity",
         "filters": [
             {"field": "is_sub_only_lifetime", "op": "=", "value": True},
             {"field": "inferred_primeable_naics", "op": "=", "value": "541330"}],
         "limit": 1000}]


def test_ex13_actions_money_floor():
    plan = _plan("actions over $5m in naics 237310 in the last 90 days")
    assert plan == [
        {"grain": "transaction",
         "filters": [
             {"field": "naics_code", "op": "=", "value": "237310"},
             {"field": "federal_action_obligation", "op": ">=", "value": 5_000_000.0},
             {"field": "action_date", "op": "<=", "value": 90}],
         "limit": 1000}]


def test_ex14_awards_from_gsa_psc():
    plan = _plan("awards from gsa in psc D302 acted in the last 90 days")
    assert plan == [
        {"grain": "prime_award",
         "filters": [
             {"field": "psc_code", "op": "=", "value": "D302"},
             {"field": "awarding_agency_code", "op": "=", "value": "047"},
             {"field": "last_action_date", "op": "<=", "value": 90}],
         "limit": 1000}]


# ── Amendment A: reserved booleans ─────────────────────────────────────────────
def test_reserved_booleans_refuse_never_silently_and():
    for tok in ("not", "excluding", "except", "without"):
        with pytest.raises(MapCompileError, match="phrase refused"):
            phrase_compiler.compile_phrase(
                f"construction companies {tok} engineering", today=TODAY)


def test_cross_axis_or_refuses():
    with pytest.raises(MapCompileError, match="cross-axis OR"):
        phrase_compiler.compile_phrase(
            "companies terminated for default or from gsa", today=TODAY)


def test_same_axis_or_merges_into_the_in_list():
    plan = _plan("actions in naics 236220 or 237310 in the last 90 days")
    assert plan[0]["filters"][0] == {
        "field": "naics_code", "op": "in", "value": ["236220", "237310"]}
    plan = _plan("dsbs companies in VA or MD that delivered subs under naics 236220")
    assert {"field": "state", "op": "in", "value": ["MD", "VA"]} in plan[0]["filters"]


# ── Amendment B: bounded time ──────────────────────────────────────────────────
def test_calendar_year_binds_between_days_ago():
    plan = _plan("actions in naics 236220 in 2024")
    lo = (TODAY - date(2024, 12, 31)).days
    hi = (TODAY - date(2024, 1, 1)).days
    assert plan[0]["filters"][-1] == {
        "field": "action_date", "op": "between", "value": [lo, hi]}


def test_fiscal_year_binds_ffy_window():
    for phrase in ("actions in naics 236220 in fy25",
                   "actions in naics 236220 in fiscal year 2025"):
        plan = _plan(phrase)
        lo = (TODAY - date(2025, 9, 30)).days
        hi = (TODAY - date(2024, 10, 1)).days
        assert plan[0]["filters"][-1] == {
            "field": "action_date", "op": "between", "value": [lo, hi]}


def test_since_month_year():
    plan = _plan("actions in naics 236220 since march 2025")
    assert plan[0]["filters"][-1] == {
        "field": "action_date", "op": "<=",
        "value": (TODAY - date(2025, 3, 1)).days}


def test_entity_money_with_bounded_time_refuses():
    with pytest.raises(MapCompileError, match="sub income"):
        phrase_compiler.compile_phrase(
            "companies with sub income over $5m in fy24", today=TODAY)


# ── Refusals: the fail-closed contract ─────────────────────────────────────────
def test_unbound_token_refuses_naming_it():
    with pytest.raises(MapCompileError, match="'profitable'"):
        phrase_compiler.compile_phrase("profitable construction companies",
                                       today=TODAY)


def test_subtier_agency_refuses_with_cycle3_pointer():
    with pytest.raises(MapCompileError, match="Cycle 3"):
        phrase_compiler.compile_phrase(
            "companies that received a code A mod from the navy", today=TODAY)


def test_subjectless_phrase_refuses():
    with pytest.raises(MapCompileError, match="no subject"):
        phrase_compiler.compile_phrase("terminated for default in the last year",
                                       today=TODAY)


def test_bare_code_letter_without_context_refuses():
    with pytest.raises(MapCompileError, match="context word"):
        phrase_compiler.compile_phrase("companies with a code A", today=TODAY)


def test_unknown_code_refuses():
    with pytest.raises(MapCompileError, match="not a known NAICS"):
        phrase_compiler.compile_phrase("actions in naics 999999 in the last "
                                       "90 days", today=TODAY)


def test_bare_code_on_companies_needs_capability_context():
    with pytest.raises(MapCompileError, match="capability context"):
        phrase_compiler.compile_phrase("companies in naics 541690", today=TODAY)


# ── Vocabulary integrity ───────────────────────────────────────────────────────
def test_vocabulary_letters_exist_in_the_registry_enums():
    for letter in phrase_compiler.EVENTS.values():
        assert letter in market_registry.ACTION_TYPE_CODES, letter
    for v in phrase_compiler.PLAN_PHRASES.values():
        for letter in (v if isinstance(v, list) else [v]):
            assert letter in market_registry.SUBCONTRACTING_PLANS, letter
    assert len(CONSTRUCTION_NAICS) == 31
    assert all(len(c) == 6 and c.startswith("23") for c in CONSTRUCTION_NAICS)


# ── Execution: the pipeline chunks + unions honestly ───────────────────────────
def test_execute_plan_chunks_uei_in_and_unions(monkeypatch):
    compiled = phrase_compiler.compile_phrase(
        "construction companies that received a code A mod in the last 90 days",
        today=TODAY)
    ueis = [f"UEIX{i:08d}" for i in range(5)]
    monkeypatch.setattr(market_store, "execute_table_collapse",
                        lambda spec, filters, limit, today=None: {
                            "rows": [{"uei": u} for u in ueis],
                            "total_rows": 5, "distinct_recipients": 5,
                            "returned": 5, "capped": False,
                            "scanned_rows": 5, "scan_capped": False,
                            "executed": {}})
    calls: list[list[str]] = []

    def fake_entity(filters, limit, today=None):
        calls.append(list(filters[0]["value"]))
        return {"rows": [{"uei": u} for u in filters[0]["value"]],
                "total": len(filters[0]["value"]),
                "returned": len(filters[0]["value"]), "capped": False,
                "executed": {}}

    monkeypatch.setattr(market_store, "execute_entity_query", fake_entity)
    monkeypatch.setattr(market_store, "UEI_IN_CAP", 2)
    out = phrase_compiler.execute_plan(compiled["plan"], today=TODAY)
    assert calls == [ueis[0:2], ueis[2:4], ueis[4:5]]   # chunked at the cap
    assert [r["uei"] for r in out["result"]["rows"]] == ueis
    assert out["resolved_step2"]["filters"][0]["value"] == ueis


def test_compile_and_execute_envelope(monkeypatch):
    monkeypatch.setattr(market_store, "execute_entity_query",
                        lambda filters, limit, today=None: {
                            "rows": [{"uei": "UEIA11111111"}], "total": 1,
                            "returned": 1, "capped": False, "executed": {}})
    out = phrase_compiler.compile_and_execute(
        {"phrase": "sub-only companies with inferred primeable 541330"},
        today=TODAY)
    assert out["meta"]["compilerVersion"] == "phrase.v1"
    assert out["meta"]["refused"] is None
    assert out["meta"]["grain"] == "entity"
    assert out["data"]["rows"] == [{"uei": "UEIA11111111"}]
    with pytest.raises(MapCompileError, match="unknown body key"):
        phrase_compiler.compile_and_execute({"phrase": "x", "mode": "y"}, today=TODAY)
