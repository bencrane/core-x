"""Unit tests for the PHASE-2 LLM lane (sam_labor_demand_extract) — pure functions only, no
R2/Lance. Covers the mandate's required set: selection determinism, bracket exclusion (operator
decision A), validator accept/reject (incl. quote-mismatch rejection and out-of-vocab tag
rejection), and delete-before-merge scoping. Plus: marked-chunk staging hard-assert, stratified
pilot determinism, prompt_hash/artifact integrity.

    python -m pytest pipelines/sam_gov/tests/test_govcon_llm_lane.py -q
"""
from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipelines.sam_gov.sam_attachment_extract import _chunk_text, _normalize_ws  # noqa: E402
from pipelines.sam_gov.sam_labor_demand_extract import (  # noqa: E402
    DOC_TOKEN_BUDGET, EXCLUDED_MARKED, EXCLUDED_OUT_OF_SCOPE, LLM_CONF_NORM, LLM_CONF_RAW,
    LLM_PROMPT_VERSION, N_OPENING_CHUNKS,
    bracket_target, build_task_payload, compute_input_set, estimate_tokens, llm_delete_predicate,
    llm_extractor_tag, load_llm_artifacts, requirements_delete_predicate, select_chunks,
    stratified_pilot, validate_result,
)

NOW = dt.datetime(2026, 6, 12, tzinfo=dt.timezone.utc)
RUN = "test_run"


def _chunk_rows(texts: list[str], rid: str = "R1", marking=None):
    return [{"chunk_id": f"{rid}:{ix:04d}", "resource_id": rid, "chunk_ix": ix, "text": t,
             "content_marking": marking or [], "notice_id": "N1", "solicitation_number": "S1",
             "naics_code": "236220", "contract_award_unique_key": "CONT_AWD_X"}
            for ix, t in enumerate(texts)]


# ════════════════════════════════════════════════════════════════ selection (deterministic policy)
def test_selection_deterministic_and_ordered():
    texts = (["opening text " * 20] * 8
             + ["filler with nothing notable " * 20] * 4
             + ["PERFORMANCE WORK STATEMENT section follows " * 10]
             + ["the contractor shall employ electricians and labor categories " * 10])
    rows = _chunk_rows(texts)
    a = select_chunks(rows)
    b = select_chunks(list(reversed(rows)))            # input order must not matter
    assert [r["chunk_id"] for r in a["selected"]] == [r["chunk_id"] for r in b["selected"]]
    ixs = [r["chunk_ix"] for r in a["selected"]]
    assert ixs == sorted(ixs)                          # staged in chunk_ix order
    got = set(ixs)
    assert set(range(N_OPENING_CHUNKS)) <= got         # opening chunks in
    assert 12 in got and 13 in got                     # header + lexicon chunks in
    assert not {8, 9, 10, 11} & got                    # ineligible filler out
    assert a["coverage_truncated"] is False


def test_selection_hard_budget_truncates_and_flags():
    # every chunk eligible (opening or header), budget too small for all
    texts = ["STATEMENT OF WORK " + ("x" * 4000) for _ in range(20)]
    rows = _chunk_rows(texts)
    out = select_chunks(rows, budget_tokens=3000)
    assert out["coverage_truncated"] is True and out["over_budget"] is True
    assert out["est_tokens_selected"] <= 3000          # hard budget never exceeded
    assert out["n_chunks_selected"] >= 1
    # opening tier wins over later header tier
    assert out["selected"][0]["chunk_ix"] == 0


def test_estimate_tokens_chars_over_4():
    assert estimate_tokens("") == 0
    assert estimate_tokens("abcd") == 1
    assert estimate_tokens("abcde") == 2


# ════════════════════════════════════════════════════════════════ bracket (operator decision A)
def test_bracket_target_partition_law():
    # in input + marked -> excluded_marked
    assert bracket_target("pending", True, True) == EXCLUDED_MARKED
    # in input + unmarked -> stays/returns to pending
    assert bracket_target("pending", True, False) is None
    assert bracket_target(EXCLUDED_MARKED, True, False) == "pending"      # reversible
    assert bracket_target(EXCLUDED_OUT_OF_SCOPE, True, False) == "pending"
    # outside the input set -> excluded_out_of_scope (marked or not)
    assert bracket_target("pending", False, False) == EXCLUDED_OUT_OF_SCOPE
    assert bracket_target("pending", False, True) == EXCLUDED_OUT_OF_SCOPE
    # idempotent: already-correct rows untouched
    assert bracket_target(EXCLUDED_MARKED, True, True) is None
    assert bracket_target(EXCLUDED_OUT_OF_SCOPE, False, False) is None
    # lane events are never erased by bracket
    for terminal in ("done", "quarantined", "submitted", "results_fetched", "failed",
                     "truncated", "marked_local_only"):
        assert bracket_target(terminal, True, True) is None
        assert bracket_target(terminal, False, False) is None


def test_compute_input_set_union_rule():
    sink_counts = {"scope": {"s1": 5}, "pricing": {"p1": 3},
                   "unknown": {"u_lex": 4, "u_hit": 2, "u_miss": 9}}
    ledger = [
        {"resource_id": "s1", "lexicon_hit_fullbody": False, "n_requirements_regex": 0},
        {"resource_id": "p1", "lexicon_hit_fullbody": False, "n_requirements_regex": 0},
        {"resource_id": "u_lex", "lexicon_hit_fullbody": True, "n_requirements_regex": 0},
        {"resource_id": "u_hit", "lexicon_hit_fullbody": False, "n_requirements_regex": 2},
        {"resource_id": "u_miss", "lexicon_hit_fullbody": False, "n_requirements_regex": 0},
    ]
    got = compute_input_set(sink_counts, ledger)
    # scope+pricing unconditional; unknown needs lexicon hit OR regex-hit residue
    assert got == {"s1", "p1", "u_lex", "u_hit"}


def test_marked_chunk_never_staged():
    rows = _chunk_rows(["clean text"] * 3)
    rows[1]["content_marking"] = ["cui"]
    sel = select_chunks(rows)
    template, schema, vocab, phash = load_llm_artifacts()
    with pytest.raises(RuntimeError, match="DECISION-A VIOLATION"):
        build_task_payload("R1", "scope", rows, sel, template, schema, vocab, phash,
                           "session-fable", "/tmp/x.result.json")


# ════════════════════════════════════════════════════════════════ stratified pilot (seeded)
def test_stratified_pilot_deterministic_and_stratified():
    cands = ([(f"sA{i}", "scope", 10) for i in range(50)]
             + [(f"sB{i}", "scope", 500) for i in range(50)]
             + [(f"u{i}", "unknown", 30) for i in range(50)]
             + [(f"p{i}", "pricing", 5) for i in range(50)])
    a = stratified_pilot(cands, 8, 20260612)
    b = stratified_pilot(list(reversed(cands)), 8, 20260612)
    assert a == b and len(a) == 8                      # seed + candidates fully determine output
    assert a != stratified_pilot(cands, 8, 999)        # seed matters
    picked = set(a)
    assert any(r.startswith("sB") for r in picked)     # large-bucket stratum represented
    assert any(r.startswith("p") for r in picked)      # pricing sink represented
    # n larger than population: returns everything once
    assert len(stratified_pilot(cands[:5], 50, 1)) == 5


# ════════════════════════════════════════════════════════════════ validator (hard rule 3)
def _mk_task(texts: list[str] | None = None, rid: str = "R1"):
    template, schema, vocab, phash = load_llm_artifacts()
    if texts is None:
        long = ("The contractor shall provide all labor and materials. All personnel shall "
                "possess an active Secret security clearance prior to performance start. "
                + " ".join(f"Scope item {i} covers electrical distribution upgrades." for i in range(120)))
        texts = [piece for _ix, piece in _chunk_text(long)]
        assert len(texts) >= 2                          # real chunker, real overlap
    rows = _chunk_rows(texts, rid)
    sel = select_chunks(rows, budget_tokens=10**6)
    task = build_task_payload(rid, "scope", rows, sel, template, schema, vocab, phash,
                              "session-fable", f"/tmp/{rid}.result.json")
    return task, vocab


def _req(task, quote, cited=None, **over):
    row = {"requirement_type": "clearance", "requirement_value": "clearance:secret",
           "requirement_detail": None, "mandatory": True, "headcount": None,
           "clearance_level": "SECRET", "pop_start": None, "pop_end": None,
           "place_of_performance_text": None, "wage_floor": None,
           "source_chunk_ids": cited or [task["chunks"][0]["chunk_id"]],
           "evidence_quote": quote}
    row.update(over)
    return row


def _result(task, reqs, tags=None, summary="The contractor performs electrical upgrades."):
    return {"resource_id": task["resource_id"], "engine": "session-fable",
            "scope_summary": summary,
            "capability_tags": tags if tags is not None else ["electrical_systems"],
            "requirements": reqs}


def test_validator_accepts_verbatim_quote_with_raw_confidence():
    task, vocab = _mk_task()
    quote = task["chunks"][0]["text"][:120]
    v = validate_result(_result(task, [_req(task, quote)]), task, vocab,
                        llm_extractor_tag("session-fable"), RUN, NOW)
    assert v["status"] == "pass" and v["n_rows_passed"] == 1 and v["pass_rate"] == 1.0
    row = v["req_rows"][0]
    assert row["confidence"] == pytest.approx(LLM_CONF_RAW)
    assert row["confidence"] < 1.0                     # hard rule 1
    assert row["extractor"] == f"llm:session-fable@{LLM_PROMPT_VERSION}"
    assert row["validated"] is True and row["marked_resource"] is False
    assert v["doc_row"]["capability_tags"] == ["electrical_systems"]


def test_validator_normalized_match_gets_lower_confidence():
    task, vocab = _mk_task()
    quote = task["chunks"][0]["text"][:100].replace(" ", "  ")   # ws drift, content intact
    v = validate_result(_result(task, [_req(task, quote)]), task, vocab,
                        llm_extractor_tag("session-fable"), RUN, NOW)
    assert v["n_rows_passed"] == 1
    assert v["req_rows"][0]["confidence"] == pytest.approx(LLM_CONF_NORM)


def test_validator_rejects_quote_mismatch():
    task, vocab = _mk_task()
    v = validate_result(_result(task, [_req(task, "this sentence is not in the document")]),
                        task, vocab, llm_extractor_tag("session-fable"), RUN, NOW)
    assert v["n_rows_passed"] == 0 and v["status"] == "partial"
    assert any(r["reason"] == "quote_mismatch" for r in v["rejects"])
    assert v["req_rows"] == []                          # rejected output never built into rows


def test_validator_boundary_straddling_quote_passes_via_shared_reassembly():
    task, vocab = _mk_task()
    c0, c1 = task["chunks"][0], task["chunks"][1]
    body = _normalize_ws(" ".join(f"Scope item {i} covers electrical distribution upgrades."
                                  for i in range(0)))  # noqa: F841 (clarity only)
    # quote spanning the c0/c1 boundary: last 60 chars of c0 + continuation from c1 beyond overlap
    from pipelines.sam_gov.sam_labor_demand_extract import reassemble_with_offsets
    joined, _ = reassemble_with_offsets([(c0["chunk_ix"], c0["chunk_id"], c0["text"]),
                                         (c1["chunk_ix"], c1["chunk_id"], c1["text"])])
    cut = len(c0["text"]) - 30
    quote = joined[cut:cut + 80]
    assert quote not in c0["text"] or quote not in c1["text"]
    v = validate_result(_result(task, [_req(task, quote,
                                            cited=[c0["chunk_id"], c1["chunk_id"]])]),
                        task, vocab, llm_extractor_tag("session-fable"), RUN, NOW)
    assert v["n_rows_passed"] == 1, v["rejects"]


def test_validator_rejects_unknown_citation():
    task, vocab = _mk_task()
    quote = task["chunks"][0]["text"][:80]
    v = validate_result(_result(task, [_req(task, quote, cited=["NOT_A_CHUNK"])]),
                        task, vocab, llm_extractor_tag("session-fable"), RUN, NOW)
    assert v["n_rows_passed"] == 0
    assert any(r["reason"].startswith("citation_unknown_chunk") for r in v["rejects"])


def test_validator_rejects_out_of_vocab_tag_and_keeps_in_vocab():
    task, vocab = _mk_task()
    quote = task["chunks"][0]["text"][:80]
    v = validate_result(_result(task, [_req(task, quote)],
                                tags=["electrical_systems", "made_up_tag"]),
                        task, vocab, llm_extractor_tag("session-fable"), RUN, NOW)
    assert v["doc_row"]["capability_tags"] == ["electrical_systems"]
    assert v["doc_row"]["n_capability_tags_rejected"] == 1
    assert any(r["reason"].startswith("tag_out_of_vocab") for r in v["rejects"])


def test_validator_rejects_out_of_vocab_labor_category_and_bad_type_and_dup():
    task, vocab = _mk_task()
    quote = task["chunks"][0]["text"][:80]
    rows = [
        _req(task, quote, requirement_type="labor_category", requirement_value="underwater_basket_weaver",
             clearance_level=None),
        _req(task, quote, requirement_type="not_a_type", clearance_level=None),
        _req(task, quote),                              # good
        _req(task, quote),                              # duplicate (type,value)
        _req(task, quote, clearance_level="ULTRA"),     # bad enum
    ]
    v = validate_result(_result(task, rows), task, vocab,
                        llm_extractor_tag("session-fable"), RUN, NOW)
    assert v["n_rows_passed"] == 1
    reasons = {r["reason"].split(":", 1)[0] for r in v["rejects"]}
    assert {"labor_category_out_of_vocab", "bad_requirement_type",
            "duplicate_requirement", "bad_clearance_level"} <= reasons


def test_validator_quarantines_wrong_resource_and_nonobject():
    task, vocab = _mk_task()
    v = validate_result({"resource_id": "OTHER", "requirements": []}, task, vocab,
                        llm_extractor_tag("session-fable"), RUN, NOW)
    assert v["status"] == "quarantined" and v["doc_row"] is None
    v2 = validate_result(["not", "an", "object"], task, vocab,
                         llm_extractor_tag("session-fable"), RUN, NOW)
    assert v2["status"] == "quarantined"


def test_validator_empty_result_is_valid_and_pass():
    task, vocab = _mk_task()
    v = validate_result(_result(task, [], tags=[], summary=None), task, vocab,
                        llm_extractor_tag("session-fable"), RUN, NOW)
    assert v["status"] == "pass" and v["pass_rate"] == 1.0
    assert v["doc_row"]["scope_summary"] is None and v["doc_row"]["capability_tags"] == []


# ════════════════════════════════════════════════════════════════ delete-before-merge scoping
def test_llm_delete_predicate_scopes_to_llm_lane_only():
    pred = llm_delete_predicate(["r1", "r'2"])
    assert "extractor LIKE 'llm:%'" in pred
    assert "resource_id IN ('r1','r''2')" in pred       # quoting safe
    # the two lane predicates are disjoint by construction: regex lane never matches llm rows
    rpred = requirements_delete_predicate(["r1"])
    assert "LIKE 'regex:%'" in rpred and "LIKE 'llm:%'" not in rpred
    assert "LIKE 'regex:%'" not in pred


# ════════════════════════════════════════════════════════════════ artifacts / prompt_hash
def test_artifacts_load_and_prompt_hash_is_stable_and_content_bound():
    t1, s1, v1, h1 = load_llm_artifacts()
    t2, s2, v2, h2 = load_llm_artifacts()
    assert h1 == h2 and len(h1) == 64                   # sha256 hex, deterministic
    assert "electrical_systems" in v1["capability_tags"]          # plan Phase-2 gate tag
    assert "electrician" in v1["labor_categories"]
    assert set(v1["clearance_levels"]) == {"PUBLIC_TRUST", "CONFIDENTIAL", "SECRET",
                                           "TOP_SECRET", "TS_SCI"}
    assert len(v1["requirement_types"]) == 11
    assert "result_path" in t1                          # template tells the engine where to write
    assert s1["properties"]["requirements"]["items"]["required"] == [
        "requirement_type", "requirement_value", "source_chunk_ids", "evidence_quote"]
    import hashlib
    base = Path(__file__).resolve().parents[1] / "reference" / "govcon_llm_lane_v1"
    raw = (base / "prompt_template.md").read_bytes() + (base / "vocabulary.json").read_bytes() \
        + (base / "output_schema.json").read_bytes()
    assert h1 == hashlib.sha256(raw).hexdigest()        # template+vocab+schema, that order
