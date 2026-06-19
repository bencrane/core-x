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


# ── PHASE 3: CUI egress invariant (defense-in-depth alongside write-side NULLing) ──
_BANNED_CHUNK_TEXT = {"evidence_quote", "requirement_detail", "scope_summary"}


def test_no_chunk_derived_text_in_either_decoder():
    # Verbatim chunk-derived text must never become a filter field, Lance column, prompt
    # field, or emitted property in EITHER app — the portal egresses structured fields only.
    for dec in cat.DECODERS.values():
        for qname, spec in dec.fields.items():
            assert qname not in _BANNED_CHUNK_TEXT, f"catalyst field {qname!r} is chunk-derived text"
            assert spec.column not in _BANNED_CHUNK_TEXT, f"catalyst column {spec.column!r} is chunk-derived text"
        assert not (set(dec.properties) & _BANNED_CHUNK_TEXT), \
            f"catalyst {dec.dataset_key}: chunk-derived text in properties"
    for ds, dec in edge.DECODERS.items():
        assert not (set(dec["fields"]) & _BANNED_CHUNK_TEXT), f"edge {ds}: chunk-derived text in fields"


def test_capability_axis_present_and_gated_winners_only():
    # The winners capability axis exists on both sides, and EXECUTE marks the right fields gated.
    cap = {"requires_clearance", "req_clearance_level_max", "requires_cmmc",
           "capability_tag", "labor_category", "has_extracted_scope"}
    assert cap <= set(cat.DECODERS["winners"].fields)
    assert cap <= set(edge.DECODERS["winners"]["fields"])
    gated = {n for n, s in cat.DECODERS["winners"].fields.items() if s.gated}
    # has_extracted_scope is the gate target, NOT gated; the rest of the capability axis is.
    # The SUB-only teaming axis is NOT gated (teaming is on all profiled subs, not the scope slice).
    assert gated == cap - {"has_extracted_scope"}
    assert cat.DECODERS["winners"].fields["has_extracted_scope"].gated is False


# ── SUB-only teaming axis: present on both sides, ungated, synonyms in sync ───
def test_teaming_axis_present_both_sides_and_ungated():
    teaming = {"teaming_dollars_5y", "n_teaming_primes", "teaming_prime"}
    assert teaming <= set(cat.DECODERS["winners"].fields)
    assert teaming <= set(edge.DECODERS["winners"]["fields"])
    # Teaming must NOT be gated (would wrongly AND has_extracted_scope and drop subs).
    for n in teaming:
        assert cat.DECODERS["winners"].fields[n].gated is False, f"{n} must not be gated"


def test_teaming_prime_synonym_values_match_catalyst():
    # The canonical prime-variant sets are part of the version-keyed allowlist: edge prompts the
    # model with them and EXECUTE resolves them, so a drift means a "lockheed" sentence translates
    # to a different array_has_any on each side. Assert the synonym values are byte-identical.
    cat_syn = cat.DECODERS["winners"].synonyms
    edge_syn = edge.DECODERS["winners"]["synonyms"]
    for term in ("lockheed", "lockheed martin", "boeing", "raytheon", "rtx", "northrop",
                 "northrop grumman", "general dynamics", "leidos", "booz allen", "saic"):
        assert term in cat_syn and term in edge_syn, f"teaming synonym {term!r} missing a side"
        assert cat_syn[term] == edge_syn[term], f"teaming synonym {term!r} drift edge vs catalyst"
        assert cat_syn[term]["field"] == "teaming_prime"
        assert cat_syn[term]["op"] == "has_any"


# ── SUB-only SELF-REPORTED axis: present both sides, UNGATED, enum reused, cert synonyms in sync ──
def test_self_reported_axis_present_both_sides_and_ungated():
    sr = {"self_reported_capability_tag", "req_cert_tag"}
    assert sr <= set(cat.DECODERS["winners"].fields)
    assert sr <= set(edge.DECODERS["winners"]["fields"])
    # Must NOT be gated — gating would clip the 13,792-sub long tail to the scope-extracted slice.
    for n in sr:
        assert cat.DECODERS["winners"].fields[n].gated is False, f"{n} must not be gated"


def test_self_reported_capability_reuses_controlled_capability_enum_both_sides():
    # Same 77-tag controlled vocab as the gated capability_tag axis — on both sides.
    cap_enum = set(cat.DECODERS["winners"].fields["capability_tag"].enum)
    assert set(cat.DECODERS["winners"].fields["self_reported_capability_tag"].enum) == cap_enum
    assert set(edge.DECODERS["winners"]["fields"]["self_reported_capability_tag"]["enum"]) == cap_enum
    # req_cert_tag is OPEN-valued on both sides (no enum) — the 172-distinct cert vocab is too granular.
    assert cat.DECODERS["winners"].fields["req_cert_tag"].enum is None
    assert edge.DECODERS["winners"]["fields"]["req_cert_tag"].get("enum") is None


def test_self_reported_and_cert_synonym_values_match_catalyst():
    # The self-report cues + cert-token sets are version-keyed allowlist values: a drift means a
    # "self-report software development" or "iso 9001" sentence resolves differently per side.
    cat_syn = cat.DECODERS["winners"].synonyms
    edge_syn = edge.DECODERS["winners"]["synonyms"]
    for term in ("self-report software development", "self-reported software development",
                 "self-report aircraft maintenance", "self-reported aircraft maintenance",
                 "cmmc certification", "iso 9001", "iso 27001", "iso 14001", "iso 17025",
                 "as9100", "faa part 145", "cissp", "pmp", "osha 30"):
        assert term in cat_syn and term in edge_syn, f"self-reported/cert synonym {term!r} missing a side"
        assert cat_syn[term] == edge_syn[term], f"synonym {term!r} drift edge vs catalyst"
    # Self-report cues route to the UNGATED self_reported field, not the gated capability_tag.
    assert cat_syn["self-report software development"]["field"] == "self_reported_capability_tag"
    # Cert cues route to req_cert_tag via has_any over exact tokens.
    assert cat_syn["iso 9001"]["field"] == "req_cert_tag"
    assert cat_syn["iso 9001"]["op"] == "has_any"
