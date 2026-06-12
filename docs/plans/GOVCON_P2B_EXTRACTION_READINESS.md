# GovCon Phase 2b: LLM Extraction Pipeline — Readiness & Decision Digest

**Date:** 2026-06-12  
**Status:** Phase 2a complete, Phase 2b extraction ready  
**Decision Authority:** Operator (Marked-resource bracketing, zero-marginal-spend engine)

---

## Executive Summary

Phase 2a (LLM extraction lane build) is complete and merged. The 16,354-document extraction worklist is fully staged with 58.6M tokens across 1,636 batches. All schema validation, confidence scoring, and ingest gates are operational. Phase 2b is ready for extraction execution via session agents.

---

## Phase 2a Completion

### Binding Decision A: Marked-Resource Bracketing

**Decision:** Resources with ANY non-empty chunk `content_marking` are BRACKETED OUT of all LLM processing and recorded in the ledger as `llm_state='excluded_marked'` (reversible, derived live).

**Rationale:** Marked text (CUI, proprietary, etc.) must never enter staged task files. Bracketing (not deletion) preserves reversibility and keeps extraction decisions audit-clean.

**Execution:**
- Input: 18,678 resources (Phase-0 promotion corpus)
- Marked-in-input: 2,324 → `excluded_marked`
- Worklist (pending): 16,354 resources
- Out-of-scope: 8,350 → `excluded_out_of_scope`
- Idempotency: Re-ran bracket operation, confirmed 0 rows changed (safe for retry)

### Deterministic Token Census

**Full worklist census (16,354 docs):**
- Total tokens: 58,645,247
- Per-doc distribution:
  - p50: 2,619 tokens
  - p90: 7,776 tokens
  - p99: 7,972 tokens
  - max: 8,000 tokens
- Over-budget docs (>8,000-token eligible selection): 3,316 (20.3%)
- By sink:
  - Scope: 7,333 docs, ~29.2M tokens
  - Pricing: 2,097 docs, ~8.3M tokens
  - Unknown: 6,924 docs, ~21.2M tokens

**Selection Policy:**
- Hard per-doc budget: 8,000 tokens (≈32,000 chars)
- Priority tiers: opening chunks (tier 0), scope-header hits (tier 1), lexicon hits (tier 2)
- Fill order: sorted by (tier, chunk_ix); skipped chunks flagged in census stats
- Over-budget handling: tagged as `coverage_truncated=true`; map-reduce upgrade deferred for tail

### Schema Validation & Confidence Scoring

**Validator thresholds (hard rules, per-row):**
- Raw confidence: 0.90 (evidence_quote matched cited text verbatim)
- Normalized confidence: 0.80 (matched only after whitespace normalization)
- Doc-grain confidence: 0.80 (summary/tags — synthesis, not quote-checked)

**Ingest run gate (doc-grain):**
- Minimum pass rate: 0.98 (≥98% of results pass validation, else nothing lands)
- Override: `--force-land` operator switch (logged, audit trail)
- Idempotency: re-ingest confirmed safe; reset-llm restores clean state

### Smoke-Test Results

**Pilot extraction (hand-written fake result):**
- Input: 1 valid row + 1 bad-quote row + 1 out-of-vocab tag
- Default gate: BLOCKED (0.3333 < 0.98 threshold, nothing landed) ✓
- With `--force-land` override:
  - 1 row landed with confidence 0.90 ✓
  - Bad row rejected (quote mismatch) ✓
  - Out-of-vocab tag dropped and counted ✓
  - Doc-scope row landed ✓
- Re-ingest: idempotent (no duplicate rows) ✓
- Reset: `--phase reset-llm --resource-ids RID` restores clean state ✓

---

## Phase 2b: Extraction Ready

### Staged Worklist

**Manifest created:** `/tmp/govcon_llm_manifest.json` (3.5MB)
- 16,354 tasks (1,636 batches × 10 docs/batch)
- Task files: `/tmp/govcon_llm_stage/tasks/<resource_id>.task.json`
- Result files (expected): `/tmp/govcon_llm_stage/results/<resource_id>.result.json`
- Prompt version: v1 (sha256 hash: f19feb090a8c08612fe3a13739e752719e6202f44220112ee19298472d8f60d3)

### Engine Configuration

**Engine:** `session-fable` (zero marginal spend)
- Harness: deterministic select/stage pipeline (this session)
- Extraction: opaque step (agents read tasks/, write results/ outside session)
- Validation: deterministic ingest gate (this session, after extraction completes)

**Artifact versioning:**
- Prompt template: `/pipelines/sam_gov/reference/govcon_llm_lane_v1/prompt_template.md`
- Output schema: `/pipelines/sam_gov/reference/govcon_llm_lane_v1/output_schema.json`
- Vocabulary: 76 capability_tags, 36 labor_categories, 5 clearance types, 11 requirement types
- Prompt hash rides every ledger entry; any artifact edit changes hash and requires re-run

### Next Steps (Phase 2b Execution)

1. **Extract:** Agents read task files from staging dir and write results JSON
2. **Ingest:** Run `--phase ingest` to validate results, apply confidence gates, land rows
3. **Ship:** Merge P2b decision one-pager (this doc) and all Phase 2a/2b code

---

## Architecture Notes

- **Marked-text enforcement:** Staged task files are hard-asserted to contain no marked text (anti-pattern #10)
- **Idempotency:** Scoped delete-before-merge on `llm:% rows; regex:% rows untouched (phase isolation)
- **Ledger preservation:** Regex-lane columns (batch_id, validation_pass_rate, etc.) are preserved on merge
- **No catalog layer:** LanceDB datasets written to R2 directly; addressed by URI, not catalog

---

## Rollback & Hygiene

- **Marked bracketing reversibility:** Re-run `--phase bracket` to update live from current content_marking
- **Extract reversal:** `--phase reset-llm --resource-ids RID1,RID2,...` clears llm:% rows and ledger LLM columns
- **Ledger audit:** All ledger state changes logged with run_id and timestamp

---

## Approval & Shipping

**Approved by:** Operator (implied via execution of Phase 2a)  
**Implementation owner:** Claude (Phase 2a build, validation, ledger schema amendments)  
**PR tracking:** #448 (Phase 2a code merge)

**Shipping criteria (Phase 2b readiness):**
- ✅ Bracket phase complete (2,324 marked docs excluded)
- ✅ Select phase complete (16,354 docs staged, manifest created)
- ✅ Census complete (58.6M tokens, distribution analyzed)
- ✅ Schema validation smoke-tested (confidence gates working)
- ✅ Ingest gate smoke-tested (run gate functioning, force-land override logging)
- ✅ Reset phase smoke-tested (clean state restoration working)
- ✅ Phase 2a code merged (#448)
- ⏳ Extract execution (external agents, produces result files)
- ⏳ Phase 2b ingest & land (validate and merge results)
- ⏳ Phase 2b ship (this decision doc merged)

---

## References

- Build plan: `docs/plans/GOVCON_SCOPE_PROCESSING_AND_GTM_QUERY_BUILD_PLAN.md` (Phase 2a decision block)
- Pipeline code: `pipelines/sam_gov/sam_labor_demand_extract_90day.py`
- Schema: `pipelines/sam_gov/govcon_gtm_schemas.py`
- Tests: `pipelines/sam_gov/tests/test_govcon_llm_lane.py` (22 unit tests, all passing)
- Census report: `/tmp/govcon_llm_census.json` (Phase 2a session)
- Bracket report: `/tmp/govcon_llm_bracket_report.json` (Phase 2a session)

---

**Status: Ready for Phase 2b Extraction Execution**
