"""Unit tests for the edge_api map TRANSLATE decoders — pure, no network.

The contract that matters: the prompt-facing allowlist edge_api shows the model MUST
match the authoritative allowlist catalyst_api EXECUTE enforces (same field names, same
version, ops a subset). A drift means the model is invited to emit fields/ops EXECUTE
will reject. Column names must never leak into the prompt (they live only in EXECUTE).
"""

from __future__ import annotations

from apps.catalyst_api.src import map_decoders as cat
from apps.edge_api.src import map_decoders as edge


def test_versions_match_catalyst():
    for ds, dec in edge.DECODERS.items():
        assert ds in cat.DECODERS, f"edge dataset {ds} missing in catalyst"
        assert dec["version"] == cat.DECODERS[ds].version, f"{ds}: version drift edge vs catalyst"


def test_edge_field_names_match_catalyst_allowlist():
    for ds, dec in edge.DECODERS.items():
        assert set(dec["fields"]) == set(cat.DECODERS[ds].fields), f"{ds}: field-set drift"


def test_edge_ops_subset_of_catalyst_ops():
    for ds, dec in edge.DECODERS.items():
        cat_fields = cat.DECODERS[ds].fields
        for name, spec in dec["fields"].items():
            assert set(spec["ops"]) <= set(cat_fields[name].ops), f"{ds}.{name}: ops drift"


def test_edge_field_types_match_catalyst():
    # A type drift means the model is prompted to emit (and EXECUTE is told to type-check)
    # the wrong scalar shape — e.g. a string where catalyst expects a float.
    for ds, dec in edge.DECODERS.items():
        cat_fields = cat.DECODERS[ds].fields
        for name, spec in dec["fields"].items():
            if name in cat_fields:
                assert spec["type"] == cat_fields[name].type, f"{ds}.{name}: type drift edge vs catalyst"


def test_edge_field_enums_match_catalyst():
    # Enum parity is bidirectional: if catalyst constrains a field's values, edge MUST offer
    # exactly that set (no invalid value → EXECUTE 422; no dropped value → silent capability
    # loss), and edge must not invent an enum catalyst does not enforce.
    for ds, dec in edge.DECODERS.items():
        cat_fields = cat.DECODERS[ds].fields
        for name, spec in dec["fields"].items():
            if name not in cat_fields:
                continue
            cat_enum = cat_fields[name].enum
            edge_enum = spec.get("enum")
            if cat_enum is not None:
                assert edge_enum is not None, f"{ds}.{name}: catalyst declares enum, edge does not"
            if edge_enum is not None:
                assert cat_enum is not None, f"{ds}.{name}: edge declares enum, catalyst does not"
            if cat_enum is not None and edge_enum is not None:
                assert set(edge_enum) == set(cat_enum), f"{ds}.{name}: enum value-set drift edge vs catalyst"


def test_synonyms_reference_known_fields():
    for ds, dec in edge.DECODERS.items():
        for term, clause in dec["synonyms"].items():
            assert clause["field"] in dec["fields"], f"{ds}: synonym {term!r} → unknown field"
            assert clause["op"] in dec["fields"][clause["field"]]["ops"]


def test_emit_filter_tool_schema_is_enum_bounded():
    for ds, dec in edge.DECODERS.items():
        tool = edge.build_emit_filter_tool(ds)
        assert tool["name"] == "emit_filter"
        item = tool["input_schema"]["properties"]["filters"]["items"]["properties"]
        assert set(item["field"]["enum"]) == set(dec["fields"])
        assert set(item["op"]["enum"]) == set(edge.OPS)


def test_emit_filter_tool_requires_unmapped():
    # The honesty contract: the forced tool MUST always emit `unmapped` (possibly empty),
    # so a constraint the allowlist cannot express is surfaced, never silently dropped.
    for ds in edge.DECODERS:
        tool = edge.build_emit_filter_tool(ds)
        schema = tool["input_schema"]
        assert "unmapped" in schema["properties"]
        assert schema["properties"]["unmapped"]["items"] == {"type": "string"}
        assert set(schema["required"]) == {"title", "filters", "unmapped"}


def test_render_prompt_mentions_fields_and_synonyms_but_no_columns():
    p = edge.render_decoder_prompt("company")
    assert "naics2" in p and "has_federal_awards" in p
    assert "construction" in p and "emit_filter" in p
    # The relative-time axis and the never-silently-drop rule are prompt-load-bearing.
    assert "days_since_last_award" in p and "unmapped" in p
    # Lance column names must NEVER reach the model — only EXECUTE knows them.
    assert "physical_address_state" not in p
    assert "latest_award_action_date" not in p


def test_awards_prompt_carries_geo_disambiguation_and_window_note():
    p = edge.render_decoder_prompt("awards")
    assert "award_amount" in p and "days_since_action" in p
    assert "pop_state" in p and "WORK IS PERFORMED" in p
    # The per-dataset notes (geo disambiguation + 90d window honesty) must render.
    assert "Geo disambiguation" in p and "last 90 days" in p


# ── dataset routing (the AUTO surface) ────────────────────────────────────────
def test_router_tool_dataset_enum_and_union_fields():
    tool = edge.build_router_tool()
    schema = tool["input_schema"]
    assert set(schema["properties"]["dataset"]["enum"]) == set(edge.DECODERS)
    union = set()
    for d in edge.DECODERS.values():
        union |= set(d["fields"])
    item = schema["properties"]["filters"]["items"]["properties"]
    assert set(item["field"]["enum"]) == union
    assert set(schema["required"]) == {"dataset", "title", "filters", "unmapped"}


def test_router_prompt_mentions_every_dataset_and_cues():
    p = edge.render_router_prompt()
    for key in edge.DECODERS:
        assert f"dataset = '{key}'" in p
    assert "won an award" in p and "lifetime" in p.lower()
    assert "unmapped" in p


def test_router_memo_version_folds_every_dataset_version():
    v = edge.router_memo_version()
    assert edge.ROUTER_VERSION in v
    for d in edge.DECODERS.values():
        assert d["version"] in v


def test_routed_filter_reconciliation_moves_offaxis_clauses_to_unmapped():
    filt = {"dataset": "company", "title": "t", "unmapped": [], "filters": [
        {"field": "naics2", "op": "=", "value": "23"},          # on-axis: kept
        {"field": "award_amount", "op": ">=", "value": 1000000},  # awards-only: moved
    ]}
    out = edge.reconcile_routed_filters(filt)
    assert out["filters"] == [{"field": "naics2", "op": "=", "value": "23"}]
    assert any("award_amount" in u for u in out["unmapped"])
