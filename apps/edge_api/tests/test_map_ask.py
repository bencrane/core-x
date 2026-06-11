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


def test_render_prompt_mentions_fields_and_synonyms_but_no_columns():
    p = edge.render_decoder_prompt("company")
    assert "naics2" in p and "has_federal_awards" in p
    assert "construction" in p and "emit_filter" in p
    # Lance column names must NEVER reach the model — only EXECUTE knows them.
    assert "physical_address_state" not in p
