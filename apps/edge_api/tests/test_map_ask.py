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


def test_awards_prompt_carries_geo_disambiguation_and_no_window_clamp():
    p = edge.render_decoder_prompt("awards")
    assert "award_amount" in p and "days_since_action" in p
    assert "pop_state" in p and "WORK IS PERFORMED" in p
    # The geo-disambiguation note must render; the stale ~90-day window claim must NOT —
    # the serving tables span full history, so a long window is applied, never flagged unmapped.
    assert "Geo disambiguation" in p
    assert "90 days" not in p


def test_no_stale_90day_window_claim_in_any_prompt():
    # The serving tables span full history now — no prompt may tell the model the data is
    # ~90-day-bounded or instruct it to push a longer window to unmapped (that produced the
    # misleading "window limited to last 90 days" NOT-APPLIED chip on e.g. "last 180 days").
    prompts = [edge.render_decoder_prompt(ds) for ds in edge.DECODERS]
    prompts.append(edge.render_router_prompt())
    for p in prompts:
        assert "90 days" not in p, "stale 90-day window claim still in a prompt"
        assert "window limited" not in p
        assert "rolling window" not in p


def test_awards_has_active_period_of_performance_axis_both_sides():
    # The active/PoP axis: prime contract actions whose period of performance covers today.
    assert "is_active" in cat.DECODERS["awards"].fields
    assert cat.DECODERS["awards"].fields["is_active"].type == "bool"
    assert cat.DECODERS["awards"].fields["is_active"].index == "BITMAP"
    assert "is_active" in edge.DECODERS["awards"]["fields"]
    assert edge.DECODERS["awards"]["fields"]["is_active"]["type"] == "bool"
    # emitted as properties (so the boot contract covers the new columns) + an "active" synonym
    assert {"is_active", "pop_end"} <= set(cat.DECODERS["awards"].properties)
    assert cat.DECODERS["awards"].synonyms["active"]["field"] == "is_active"
    assert edge.DECODERS["awards"]["synonyms"]["active"]["field"] == "is_active"


def test_awards_has_psc_axis_both_sides():
    # The PSC axis: what the contract BUYS (distinct from NAICS). Present on both sides as two
    # string fields; psc_category is BITMAP-indexed (low cardinality) + emitted as a property so
    # the boot contract covers the new column; psc_code is BTREE (full code). Prime-only signal.
    for field, idx in (("psc_category", "BITMAP"), ("psc_code", "BTREE")):
        assert field in cat.DECODERS["awards"].fields, f"catalyst awards missing {field}"
        assert cat.DECODERS["awards"].fields[field].type == "string"
        assert cat.DECODERS["awards"].fields[field].index == idx
        assert field in edge.DECODERS["awards"]["fields"], f"edge awards missing {field}"
        assert edge.DECODERS["awards"]["fields"][field]["type"] == "string"
    # New filter columns must ride as properties (so verify_decoder_contract validates them live).
    assert {"psc_category", "psc_code"} <= set(cat.DECODERS["awards"].properties)
    # The headline NL win: "transportation services" → PSC category 'V', byte-identical both sides.
    for term in ("transportation services", "freight services", "psc v"):
        assert cat.DECODERS["awards"].synonyms[term] == {"field": "psc_category", "op": "=", "value": "V"}
        assert edge.DECODERS["awards"]["synonyms"][term] == {"field": "psc_category", "op": "=", "value": "V"}


def test_awards_has_label_axes_both_sides():
    # awards.v9 GTM label axes: vertical / work_type / equipment_intensity — all three live as
    # BITMAP columns on usaspending_awards_map_serving, award-grain (they describe the award's
    # (NAICS, PSC) pair). Present on both sides as enum'd string fields; emitted as properties so
    # the boot contract (verify_decoder_contract) validates the new columns live.
    for field in ("vertical", "work_type", "equipment_intensity"):
        assert field in cat.DECODERS["awards"].fields, f"catalyst awards missing {field}"
        assert cat.DECODERS["awards"].fields[field].type == "string"
        assert cat.DECODERS["awards"].fields[field].index == "BITMAP"
        assert field in edge.DECODERS["awards"]["fields"], f"edge awards missing {field}"
        assert edge.DECODERS["awards"]["fields"][field]["type"] == "string"
        # Enum parity is bidirectional and byte-exact (a drift → EXECUTE 422 or silent loss).
        cat_enum = cat.DECODERS["awards"].fields[field].enum
        edge_enum = edge.DECODERS["awards"]["fields"][field].get("enum")
        assert cat_enum is not None and edge_enum is not None, f"{field}: enum must exist both sides"
        assert set(edge_enum) == set(cat_enum), f"{field}: enum value-set drift edge vs catalyst"
    # The 24-name vertical taxonomy must carry the comma-bearing labels byte-exact (a wrong comma
    # or & → zero rows on a Lance BITMAP scan).
    cat_vertical = set(cat.DECODERS["awards"].fields["vertical"].enum)
    assert "Facilities, Maintenance & Janitorial" in cat_vertical
    assert "Food, Agriculture & Beverage" in cat_vertical
    assert "Staffing & Human Capital" in cat_vertical
    assert len(cat_vertical) == 24
    assert set(cat.DECODERS["awards"].fields["work_type"].enum) == {
        "services_labor", "manufacture", "distribute_resell", "construct", "RnD", "maintain_repair"}
    assert set(cat.DECODERS["awards"].fields["equipment_intensity"].enum) == {"low", "medium", "high"}
    # All three label columns + the what_was_done display gloss ride as properties so
    # verify_decoder_contract validates them live; what_was_done is a DISPLAY column ONLY —
    # never a filter field (would become emittable + demand a non-existent index), never indexed.
    assert {"vertical", "work_type", "equipment_intensity", "what_was_done"} <= set(
        cat.DECODERS["awards"].properties)
    assert "what_was_done" not in cat.DECODERS["awards"].fields
    assert "what_was_done" not in edge.DECODERS["awards"]["fields"]
    # Headline NL wins compile byte-identically both sides (the lexicon is mirrored).
    assert cat.DECODERS["awards"].synonyms["aerospace"] == {
        "field": "vertical", "op": "=", "value": "Aerospace & Defense"}
    assert edge.DECODERS["awards"]["synonyms"]["aerospace"] == {
        "field": "vertical", "op": "=", "value": "Aerospace & Defense"}


def test_active_has_label_axes_both_sides():
    # active.v2 mirrors the awards.v9 GTM label axes onto the FORWARD recompete dataset
    # (govcon_active_awards_map_serving): vertical / work_type / equipment_intensity, all three
    # BITMAP, award-grain. So "aerospace contracts up for recompete" / "IT vertical expiring in
    # 90 days" route on vertical AND-ed with the days_until_expiry window. Present both sides as
    # enum'd string fields; emitted as properties so the boot contract validates the live columns.
    for field in ("vertical", "work_type", "equipment_intensity"):
        assert field in cat.DECODERS["active"].fields, f"catalyst active missing {field}"
        assert cat.DECODERS["active"].fields[field].type == "string"
        assert cat.DECODERS["active"].fields[field].index == "BITMAP"
        assert field in edge.DECODERS["active"]["fields"], f"edge active missing {field}"
        assert edge.DECODERS["active"]["fields"][field]["type"] == "string"
        cat_enum = cat.DECODERS["active"].fields[field].enum
        edge_enum = edge.DECODERS["active"]["fields"][field].get("enum")
        assert cat_enum is not None and edge_enum is not None, f"{field}: enum must exist both sides"
        assert set(edge_enum) == set(cat_enum), f"{field}: enum value-set drift edge vs catalyst"
        # The label taxonomy is ONE source of truth — active's enum must equal awards' enum.
        assert set(cat_enum) == set(cat.DECODERS["awards"].fields[field].enum), \
            f"{field}: active enum drifted from awards (must share the same tuple)"
    # The 24-name vertical taxonomy carries the comma-bearing labels byte-exact (a wrong comma or
    # & → zero rows on a Lance BITMAP scan).
    cat_vertical = set(cat.DECODERS["active"].fields["vertical"].enum)
    assert "Facilities, Maintenance & Janitorial" in cat_vertical
    assert "Food, Agriculture & Beverage" in cat_vertical
    assert len(cat_vertical) == 24
    # All three label columns + the what_was_done display gloss ride as properties so
    # verify_decoder_contract validates them live; what_was_done is DISPLAY-only — never a filter
    # field (would become emittable + demand a non-existent index), never indexed.
    assert {"vertical", "work_type", "equipment_intensity", "what_was_done"} <= set(
        cat.DECODERS["active"].properties)
    assert "what_was_done" not in cat.DECODERS["active"].fields
    assert "what_was_done" not in edge.DECODERS["active"]["fields"]
    # Headline NL wins compile byte-identically both sides (the lexicon is mirrored).
    assert cat.DECODERS["active"].synonyms["aerospace"] == {
        "field": "vertical", "op": "=", "value": "Aerospace & Defense"}
    assert edge.DECODERS["active"]["synonyms"]["aerospace"] == {
        "field": "vertical", "op": "=", "value": "Aerospace & Defense"}


# ── aggregate capability: parity + tool schema + routing honesty ─────────────
def test_awards_aggregate_axis_parity_both_sides():
    cat_agg = cat.DECODERS["awards"].aggregate
    assert cat_agg is not None, "catalyst awards must declare an AggregateSpec"
    edge_agg = edge.DECODERS["awards"]["aggregate"]
    # dims + metrics value-sets must match edge↔catalyst (the model's output space == EXECUTE's gate)
    assert set(edge_agg["dims"]) == set(cat_agg.dims)
    assert set(edge_agg["metrics"]) == set(cat_agg.metrics)
    assert set(edge_agg["pseudo_dims"]) == {"winner", "size_band"}
    assert cat_agg.measure == "action_obligated_usd"
    assert cat_agg.winner_key == ("winner_uei", "winner_name") and cat_agg.size_band_edges


def test_emit_filter_tool_aggregate_is_optional_and_enum_bounded():
    tool = edge.build_emit_filter_tool("awards")
    schema = tool["input_schema"]
    assert "aggregate" in schema["properties"]
    assert "aggregate" not in schema["required"]              # optional — row queries omit it
    agg = schema["properties"]["aggregate"]["properties"]
    gb = set(agg["group_by"]["enum"])
    assert {"awarding_agency", "psc_category", "winner", "size_band"} <= gb
    assert set(agg["metrics"]["items"]["enum"]) == set(edge.DECODERS["awards"]["aggregate"]["metrics"])


def test_all_datasets_expose_optional_aggregate_tool_property():
    # Every map dataset now declares an aggregate (awards / active / winners / company).
    for ds in edge.DECODERS:
        schema = edge.build_emit_filter_tool(ds)["input_schema"]
        assert "aggregate" in schema["properties"], f"{ds}: missing aggregate tool property"
        assert "aggregate" not in schema["required"], f"{ds}: aggregate must stay optional"


def test_winners_company_aggregate_parity_both_sides():
    for ds, measure in (("winners", "entity_obligated_usd"), ("company", "entity_active_obligated_usd")):
        cat_agg = cat.DECODERS[ds].aggregate
        assert cat_agg is not None and cat_agg.measure == measure
        edge_agg = edge.DECODERS[ds]["aggregate"]
        assert set(edge_agg["dims"]) == set(cat_agg.dims)
        assert set(edge_agg["metrics"]) == set(cat_agg.metrics)
        assert set(edge_agg["pseudo_dims"]) == {"winner", "size_band"}


def test_router_tool_carries_optional_aggregate_union():
    schema = edge.build_router_tool()["input_schema"]
    assert "aggregate" in schema["properties"]
    assert "aggregate" not in schema["required"]


def test_reconcile_preserves_aggregate_for_every_real_dataset():
    # Every map dataset is now aggregate-capable, so a routed aggregate is preserved, never dropped.
    for ds in edge.DECODERS:
        out = edge.reconcile_routed_filters(
            {"dataset": ds, "title": "t", "unmapped": [], "filters": [],
             "aggregate": {"group_by": "naics2"}})
        assert out.get("aggregate") == {"group_by": "naics2"}, f"{ds}: aggregate wrongly dropped"


def test_reconcile_drops_aggregate_for_a_dataset_without_aggregate(monkeypatch):
    # Defensive (honesty contract): if a dataset that declares NO aggregate ever receives one —
    # a future dataset, or a routing glitch — reconcile surfaces it as unmapped, never runs it
    # against a table that cannot aggregate. Exercised via a synthetic no-aggregate dataset.
    fake = {"version": "x", "description": "", "fields": {}, "synonyms": {}}
    monkeypatch.setitem(edge.DECODERS, "_noagg", fake)
    out = edge.reconcile_routed_filters(
        {"dataset": "_noagg", "title": "t", "unmapped": [], "filters": [],
         "aggregate": {"group_by": "awarding_agency"}})
    assert "aggregate" not in out
    assert any("aggregate by awarding_agency" in u for u in out["unmapped"])


def test_aggregate_capability_renders_in_every_dataset_prompt():
    # Every dataset now carries an aggregate, so each prompt advertises the optional axis.
    for ds in edge.DECODERS:
        p = edge.render_decoder_prompt(ds)
        assert "Aggregate (optional)" in p and "group_by" in p and "size_band" in p, f"{ds}: no aggregate in prompt"


# ── active.v1: the forward-looking recompete dataset ─────────────────────────
def test_active_recompete_axis_present_both_sides():
    assert "active" in cat.DECODERS and "active" in edge.DECODERS
    # the forward expiry axis (days_ahead on pop_current_end) — the reason the dataset exists
    assert cat.DECODERS["active"].fields["days_until_expiry"].type == "days_ahead"
    assert cat.DECODERS["active"].fields["days_until_expiry"].column == "pop_current_end"
    assert edge.DECODERS["active"]["fields"]["days_until_expiry"]["type"] == "days_ahead"
    # business_size (the small/large split) + its enum, both sides
    assert set(cat.DECODERS["active"].fields["business_size"].enum) == {
        "SMALL BUSINESS", "OTHER THAN SMALL BUSINESS"}
    # recompete synonyms route to the expiry axis, byte-identical
    assert cat.DECODERS["active"].synonyms["recompete"] == {
        "field": "days_until_expiry", "op": "<=", "value": 180}
    assert edge.DECODERS["active"]["synonyms"]["recompete"] == {
        "field": "days_until_expiry", "op": "<=", "value": 180}
    # aggregate measure is the contract ceiling (contract_current_value_usd), dims match
    assert cat.DECODERS["active"].aggregate.measure == "contract_current_value_usd"
    assert set(edge.DECODERS["active"]["aggregate"]["dims"]) == set(cat.DECODERS["active"].aggregate.dims)


def test_active_is_routable_with_expiry_in_tool_and_prompt():
    assert "active" in edge.build_router_tool()["input_schema"]["properties"]["dataset"]["enum"]
    tool = edge.build_emit_filter_tool("active")
    field_enum = tool["input_schema"]["properties"]["filters"]["items"]["properties"]["field"]["enum"]
    assert "days_until_expiry" in field_enum and "business_size" in field_enum
    p = edge.render_decoder_prompt("active")
    assert "days_until_expiry" in p and "recompete" in p.lower() and "Aggregate (optional)" in p


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
           "solicitation_scope_tag", "labor_category", "has_extracted_scope"}
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
    sr = {"subaward_description_tag", "req_cert_tag"}
    assert sr <= set(cat.DECODERS["winners"].fields)
    assert sr <= set(edge.DECODERS["winners"]["fields"])
    # Must NOT be gated — gating would clip the 13,792-sub long tail to the scope-extracted slice.
    for n in sr:
        assert cat.DECODERS["winners"].fields[n].gated is False, f"{n} must not be gated"


def test_self_reported_capability_reuses_controlled_capability_enum_both_sides():
    # Same 77-tag controlled vocab as the gated solicitation_scope_tag axis — on both sides.
    cap_enum = set(cat.DECODERS["winners"].fields["solicitation_scope_tag"].enum)
    assert set(cat.DECODERS["winners"].fields["subaward_description_tag"].enum) == cap_enum
    assert set(edge.DECODERS["winners"]["fields"]["subaward_description_tag"]["enum"]) == cap_enum
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
    # Self-report cues route to the UNGATED self_reported field, not the gated solicitation_scope_tag.
    assert cat_syn["self-report software development"]["field"] == "subaward_description_tag"
    # Cert cues route to req_cert_tag via has_any over exact tokens.
    assert cat_syn["iso 9001"]["field"] == "req_cert_tag"
    assert cat_syn["iso 9001"]["op"] == "has_any"
