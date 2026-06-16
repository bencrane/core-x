# Subaward Scope-Enrichment Lift — CANONICAL RUN RECORD

**Date:** 2026-06-15 (execution) / record finalized 2026-06-16 · **Status:** ✅ **LIFT COMPLETE & DELIVERED** — one non-blocking housekeeping item open (rebuild the `govcon_unknown_90day` vector index over the new chunks).
**Companions (ground truth — cross-referenced, not duplicated here):** runbook `docs/plans/SUBAWARD_SCOPE_ENRICHMENT_EXECUTION_PLAN.md`; the open item `docs/plans/SUBAWARD_SCOPE_ENRICHMENT_REINDEX_HANDOFF.md`; root-cause/worklist `docs/plans/SUBAWARD_SCOPE_ENRICHMENT_LIFT_PLAN.md`.
**Verification basis:** repo HEAD = `bbb7c8e` (all 5 lift PRs merged to `main`); read-only R2 probes over `core-x/prd` (`s3://data-sink/active/`) executed 2026-06-16; `gh pr view` for each PR.

> **Verification legend** — `✓ verified` = independently re-probed/re-read this session with the observed value cited. `↪ doc-only` = stated in a companion doc and consistent with the present state but not re-measured this session (the intermediate run-time count is no longer directly recoverable from end state).

---

## 1. Executive summary

The lift **unstuck 3,969 subawardee-solicitation PDFs** that were frozen at `skipped_out_of_scope` in the shared prime extraction ledger — a GTM "Strained Middle" prime-cohort scope gate that had been recorded as if it were universal. They were routed through a **throwaway ledger/sinks** (`_sublift_*`) via a default-OFF resource-id filter + GTM-gate bypass, extracted/chunked, then their chunks were **appended idempotently (by `chunk_id`)** into the shared chunk sinks, CUI-marked, embedded, requirement-extracted (regex + LLM), and the subawardee capability profile was rebuilt.

**Headline number:** `govcon_subawardee_capability_profiles.has_extracted_scope` rose **3,302 → 4,220 (+918 subs)**, i.e. **50.1% → 64.1%** of the 6,586 bridge UEIs, moving toward the ~71.4% content ceiling. `✓ verified` (probe: 4,220 / 6,586 = 64.1%).

**Hard invariant held throughout:** the shared prime extraction ledger `sam_attachment_extraction_90day`'s extraction **verdicts were never mutated** — the only events it gained are 3,056 sanctioned `marking_fullbody` AUDIT events (run_id `sublift-marking`), which the resolution view excludes. **0** route/expand/extract lift events reached it. `✓ verified` (probe below). Every shared write was idempotent (append re-run delta 0; merge_insert by `chunk_id`; profile overwrite by content hash).

**One open item:** the `govcon_unknown_90day` ANN (IVF_PQ) index was not refreshed over the lift's 268,164 new chunks — they are embedded-but-unindexed, so vector search over them is brute-forced (correct, slower). Non-blocking; needs **zero account/LLM tokens**; the deliverable does not use vector search. See §8.

---

## 2. Objective & root cause

**Root cause:** the prime attachment-extraction pipeline applies a GTM scope gate (`_read_scope_gate`) that selects the in-scope prime cohort. Subawardee-solicitation PDFs fall outside that prime cohort, so the gate stamped all 3,969 as `skipped_out_of_scope` / `lane='out_of_scope'` in the **shared** ledger. Because the shared ledger is the system of record for extraction verdicts, those docs could never be chunked under normal operation — a prime-cohort gate was acting as a universal scope decision.

**Objective:** chunk those 3,969 docs and fold their content into the shared chunk sinks **without** mutating the shared ledger's extraction verdicts, so the subawardee capability profile's solicitation-scope leg lifts from 50.1% toward the ~71.4% ceiling.

**Mechanism:** default-OFF `--resource-ids`/`--resource-ids-file` id-filter + GTM-gate bypass (an explicit id set IS the scope decision) routed the ids through a THROWAWAY ledger + throwaway sinks (`_sublift_*`); extract/chunk there; append chunks to the SHARED sinks idempotently by `chunk_id`; mark/embed/extract over only the new ids; rebuild the profile.

---

## 3. Outcome / deliverable

### Deliverable: `govcon_subawardee_capability_profiles`

| Metric | Pre-lift | Post-lift | Verification |
|---|---|---|---|
| `has_extracted_scope = true` | 3,302 | **4,220** (+918 subs) | `✓ verified` — probe: `count_rows(filter="has_extracted_scope = true")` = 4,220 |
| Coverage rate (of 6,586 bridge UEIs) | 50.1% | **64.1%** | `✓ verified` — 4,220 / 6,586 = 64.1% |
| Universe (total bridge sub UEIs) | 6,586 | 6,586 | `✓ verified` — `count_rows()` = 6,586 |
| CUI: `scope_summary_without_flag` (want 0) | — | **0** | `✓ verified` — `scope_summary IS NOT NULL AND has_extracted_scope = false` = 0 |
| Idempotency content hash | — | `ac5c523cefbbc4a7f1a056c457e99a61` | `↪ doc-only` — recorded by `verify --content-hash`; not re-derived this session |
| `row_eq_universe` / `row_eq_distinct_uei` | — | true / true | `↪ doc-only` — module `verify` output |

**Note on the summary's `clearance_level_without_flag` CUI check:** the profile dataset has **no `clearance_level` column** (`✓ verified` — column absent from schema). The load-bearing, verifiable CUI gate is `scope_summary_without_flag == 0` (verified 0). The `clearance_level` check is a no-op against the current schema; treat the `scope_summary` check as the authoritative CUI proof.

### Why +918 vs the ~+1,274 projected ceiling

The +918 delivered (vs the ~+1,274 ceiling-projection, vs ~71.4% = 4,704/6,586) is the **measured content tail**, not data loss: OCR/content-noise docs that yield zero usable chunks, 862 coverage-truncated documents, and CUI marked-bracketed content. This is the runbook's **risk R8** ("delivered lift < ceiling") disclaimer — a scope-truth property of the corpus, measured at profile `verify`, never a pipeline failure.

---

## 4. Execution timeline (Phase B → J)

Order executed: **B → A → C → D → E → F → G → H1 → H2 → I → J** (the two hard orderings — **F before H2** for the CUI egress gate, **F before G** so embed buckets on live `content_marking` — were honored).

| Ph | What ran | Measured result | Verification |
|---|---|---|---|
| **B** | Code PRs #478/#479 (id-filter + gate-bypass + marking relax + labor file-flag + `--inner-uri` + reserved-word fix) | Merged before any data run; pure-function tests green | `✓ verified` — PRs MERGED (§5); helpers present in code (§5 notes) |
| **A** | Derive the 3,969 allow-list → `$IDS` (manifest ∩ downloaded ∩ `skipped_out_of_scope`/`out_of_scope` ∩ absent-from-all-3-sinks) | **3,969** ids | `↪ doc-only` — runbook §4 / LIFT_PLAN; end-state cannot re-derive the pre-append membership |
| **A-fix** | duckdb `.arrow()` one-shot-reader bug corrected to `.to_arrow_table()` | False "0 rows" → correct 3,969 | `↪ doc-only` (incident #1) |
| **C** | Throwaway route + expand into `_sublift_*` | 3,969 routed (L3_triage 3,227 / L4_structured 444 / L1_scope 212 / container 86); **0 `skipped_out_of_scope`** (gate bypass worked), no leak; 86 zips → 1,925 inner routed | `↪ doc-only` — `_sublift_*` deleted in Phase J, not re-probeable; gate-OFF coupling `✓ verified` in code |
| **D** | Throwaway extract → finalize dedup | extract 457,574 chunks → finalize **453,656** (scope 132,184 + unknown 268,164 + pricing 53,308). Tail: requires_ocr 474, dropped_content_noise 365, skipped_non_text 294, dropped_boilerplate 49, dropped_duplicate 27, extract_failed 3 | `✓ verified (indirectly)` — the per-sink E deltas below sum to exactly 453,656 |
| **E** | Append `_sublift_*` chunks → SHARED sinks (`merge_insert` by `chunk_id`; re-run delta 0) | see per-sink table below; prime ledger byte-identical (v264) at end of E | `✓ verified` — post-lift sink counts match the claimed `after` values exactly (§4.1) |
| **F** | CUI marking reconcile | **PASS**, 552 targets marked; prime ledger v264 → **v267** = +3,056 events, ALL `marking_fullbody` AUDIT (run_id `sublift-marking`), excluded from the resolution view; extraction verdicts unchanged | `✓ verified` — ledger v267; `sublift-marking` events = **3,056**, of which **0** are non-`marking_fullbody` |
| **G** | Embed unmarked NULL tail (self-hosted MPS) + reindex | `embedding IS NULL == 0` for the unmarked set on BOTH sinks; **scope IVF_PQ index fully built**; **unknown index NOT refreshed** over new rows (pyo3 `PanicException: RecvError` in `compact_files`) | `✓ verified` — scope `null_unmarked=0`, idx_unindexed=0; unknown `null_unmarked=0`, **idx_unindexed=268,164** (stale) |
| **H1** | Regex lane scoped to `$IDS` | 3,107 docs → **9,282 requirement rows + 1,710 labor rows** (0 failures, 509 marked redaction-applied) | `↪ doc-only` — sinks are cumulative; the lift-specific delta is not isolable from end state (see §4.1 note) |
| **H2** | LLM grind (session-agent, account-burning) | bracket → 1,620 pending whole-corpus; `_scope_pending` intersection left **2 unrelated prod ids for prod** → 1,618 in-scope; census 1,618 / ~5.87M tokens; select staged 1,614 (+4 pilot); grind via Workflow harness; ingest **run_pass_rate 0.984 ≥ 0.98, gate_ok, landed**, 3,269 requirement rows + doc_scope | `✓ verified (code path)` — `_scope_pending`, `run_pass_rate`/`gate_ok`, `_assert_marking_complete` present; row deltas `↪ doc-only` (cumulative sinks) |
| **I** | Rebuild capability profile (overwrite) | `has_extracted_scope` 3,302 → 4,220 (§3) | `✓ verified` — profile probe (§3) |
| **J** | Throwaway cleanup (scoped boto3 delete) | deleted **585** `_sublift_*` objects (six datasets) | `✓ verified` — `active/_sublift_` lists **0** objects |

### 4.1 Per-sink append deltas (Phase E) — `✓ verified`

Probe (2026-06-16): `lance.dataset(...).count_rows()` on each shared sink.

| Shared sink | Pre-lift rows | Post-lift rows (probed) | Delta | Matches summary? |
|---|---|---|---|---|
| `govcon_scope_vectors_90day` | 1,348,983 | **1,481,167** | +132,184 | ✅ exact |
| `govcon_unknown_90day` | 1,042,059 | **1,310,223** | +268,164 | ✅ exact |
| `govcon_pricing_90day` | 102,809 | **156,117** | +53,308 | ✅ exact |
| **Total** | 2,493,851 | **2,947,507** | **+453,656** | ✅ exact (= Phase D finalize count) |

The three deltas (132,184 + 268,164 + 53,308) sum to exactly the 453,656 finalize-dedup count, independently corroborating both the Phase D finalize total and the Phase E append.

**Note on the cumulative requirement/labor/doc_scope sinks:** `govcon_award_requirements_90day` (probed 193,845 rows), `govcon_labor_demand_90day` (probed 16,942), `govcon_doc_scope_90day` (probed 17,970) are append targets shared with the prime pipeline and have advanced since the lift ran; the H1/H2 row counts in the table are the run-time figures from the companion docs and are not isolable from the present cumulative totals. They are marked `↪ doc-only` accordingly.

---

## 5. Code changes — the 5 PRs (all `✓ verified` MERGED)

`✓ verified` via `gh pr view <n> --json number,title,state,mergeCommit,mergedAt,files`.

| PR | Commit | State | What | Files |
|---|---|---|---|---|
| **#478** | `5ab7366` | MERGED 2026-06-15 18:05Z | Phase B: default-OFF `--resource-ids`/`--resource-ids-file` id-filter + GTM-gate bypass; Guard #1 (empty-set raise) + Guard #2 (routed ⊆ allow-list); marking-pass index-safe relax; labor `--resource-ids-file`; pure-function tests | `sam_attachment_extract_90day.py`, `sam_labor_demand_extract_90day.py`, `sam_marking_fullbody_90day.py`, `tests/test_sam_attachment_id_filter.py` |
| **#479** | `a9a4435` | MERGED 2026-06-15 18:39Z | Gap fix: `--inner-uri` override + reserved-word `JOIN inner` → `JOIN inner_wl` in `_build_tasks` | `sam_attachment_extract_90day.py`, `tests/test_sam_attachment_id_filter.py` |
| **#480** | `29c502e` | MERGED 2026-06-15 19:43Z | Phase E append script (shared-sink append, idempotent by `chunk_id`) | `subaward_scope_append.py` |
| **#481** | `a62a758` | MERGED 2026-06-15 20:37Z | F–J pre-execution hardening: mechanical H2 CUI egress gate `_assert_marking_complete` + structural scoping + runbook CLI/ordering/cost corrections | `sam_labor_demand_extract_90day.py`, `tests/test_sam_labor_h2_gates.py`, runbook `.md` |
| **#482** | `0d3a171` | MERGED 2026-06-15 21:15Z | H2 scope-by-intersection `_scope_pending` (replaced the raise-on-leak assertion; leaves unrelated prod ids pending for prod) | `sam_labor_demand_extract_90day.py`, `tests/test_sam_labor_h2_gates.py` |

**Code-claim spot-checks (`✓ verified` by reading the functions):**
- `_id_filter_sql` (extract:531), `_assert_routed_subset` (extract:545, `ROUTE LEAK` raise at :553), GATE BYPASS coupling `scope = None if (max_files or only_resource_ids is not None)` (extract:609), Guard #2 call site (extract:639) — all present.
- `--inner-uri` CLI (extract:2002), `INNER_URI` override (extract:2033), and the reserved-word fix: `con.register("inner_wl", …)` with the inline comment "NOT 'inner' — INNER is a DuckDB reserved keyword" (extract:1491), `JOIN inner_wl i` (extract:1497) — all present.
- `_assert_marking_writeback_safe` (marking:262) swapped in at the two former `_assert_no_vector_index` call sites (marking:301, :327) — present; broad guard retained on the overwrite/compaction path.
- `subaward_scope_append.py`: `merge_insert("chunk_id").when_matched_update_all().when_not_matched_insert_all()` under `SinkCommitLease`, embedding column excluded from read — present.
- `_assert_marking_complete` (labor:1410, refuses unless `reconcile_overall == PASS`), `_scope_pending` (labor:1419, intersection), `run_pass_rate`/`gate_ok` ingest gate (labor:2062-2063) — all present.

---

## 6. Incidents, deviations & lessons

| # | What | Cause | Resolution | Blast |
|---|---|---|---|---|
| 1 | Phase A probe reported false **0 rows** | duckdb 1.5.3 `.arrow()` returns a one-shot `RecordBatchReader` exhausted by a later `execute()` | Materialize with `.to_arrow_table()` | **None** — read-only probe; no data touched |
| 2 | Phase C expand wrote the **SHARED** `sam_attachment_inner_files_90day` (created it at v1, 1,974 rows) | `INNER_URI` was not isolated by the runbook | Detected **before any extract chunk landed**; reverted (copied rows to throwaway `_sublift_inner`, deleted the shared dataset back to pre-lift absent); fixed by PR #479's `--inner-uri` | Shared inner worklist temporarily created then fully reverted; **prime ledger never affected** |
| 3 | `JOIN inner` parser crash | `INNER` is a DuckDB reserved keyword | PR #479 — `con.register("inner_wl", …)` + `JOIN inner_wl` | **None** — caught pre-extract |
| 4 | Session-suspend twice killed the `run_in_background` extract | Ephemeral `uv run` died with the session | Switched to a persistent venv (`/tmp/sublift_venv`) + true `--daemon`; cleared leaked throwaway leases. An abrupt kill left **84 stale-checkpoint entries** that an R2-eventual-consistency read showed as "routed" but which resolved as `dropped_content_noise`; `finalize` dedup removed crash-window dup chunks (scope 3,722, pricing 196) | **No loss** — finalize dedup is by-design idempotent; throwaway-only |
| 5 | LLM grind ran an **EMPTY** prior dir `/tmp/p2b_grind_c6` (285 agents, ~13.2M tokens) | The Workflow `args` did not propagate → the harness fell back to hardcoded defaults pointing at an empty dir | Fixed by hardcoding constants in a per-cycle copy `/tmp/sublift_grind.js` | **ZERO data impact** (no task files, no results, no ingest) — wasted tokens only |
| 6 | A user interrupt mid-run cascaded into the background grind workflow, killing an in-flight agent → wave-barrier stall | Interrupt propagation to the child Workflow | Re-launched; resumable via result-file skip | **None** — per-resource resume, no double-pay |
| 7 | Embed "hang" raised **TWICE** as a false alarm | Low CPU = MPS offload; flat counter = `FLUSH_ROWS` cadence | An Opus diagnostic subagent returned **HEALTHY** (measured ~11.6 passages/s, vectors valid 1024-d unit-norm, 0 dup chunk_ids) | **None** — **lesson: do not infer a hang from CPU% on an MPS-offloaded embed** |
| 8 | Grind under-produced **600/1,614** on the first full pass | The **5-hour account session cap** (agent transcripts: "session limit · resets 8pm") | Re-ground the missing 1,014 on the reset budget → **1,614/1,614, 0 missing** | **None** — resumable; final corpus complete |
| 9 | Embed `index --sink unknown` pyo3 panic | `pyo3_runtime.PanicException: RecvError(())` inside `ds.optimize.compact_files()` is a `BaseException`, not `Exception`, so it escaped the `except Exception` best-effort guard (embed:191) and aborted the whole index command | **The single OPEN item** — see §8 + handoff doc (1-line catch broadening) | Unknown-sink new chunks embedded-but-unindexed → vector search brute-forced (correct, slower); **deliverable unaffected** (does not use vector search) |

---

## 7. Invariants & safety

| Invariant | Status | Verification |
|---|---|---|
| Prime ledger `sam_attachment_extraction_90day` extraction **verdicts byte-identical** (only sanctioned `marking_fullbody` audit events added by Phase F) | ✅ held | `✓ verified` — ledger at **v267**; `sublift-marking` events = **3,056**, of which **0** carry a non-`marking_fullbody` state |
| **0** route/expand/extract lift run_ids in the shared ledger | ✅ held | `✓ verified` — `run_id LIKE '%sublift%' AND state <> 'marking_fullbody'` = **0** |
| Every shared write idempotent | ✅ held | append re-run delta 0 (`↪ doc-only`); `merge_insert` by `chunk_id` (`✓ verified` in code); profile overwrite by content hash |
| CUI: F-before-H2 mechanically gated; redaction-at-write; profile CUI checks 0 | ✅ held | `✓ verified` — `_assert_marking_complete` enforces `reconcile_overall == PASS` (labor:1410); profile `scope_summary_without_flag` = **0** |
| Embed completion contract (`embedding IS NULL == 0` for the unmarked set) | ✅ held both sinks | `✓ verified` — scope `null_unmarked = 0`, unknown `null_unmarked = 0` |

**Note on the marking-event total:** the ledger carries **5,551** total `marking_fullbody` events; **3,056** of those are this lift's `sublift-marking` run_id, the remainder belong to the prior prime `marking_fullbody_20260612` pass. The summary's "+3,056" refers correctly to the lift's contribution. `✓ verified`.

---

## 8. Current state & the ONE open item

**Open item:** rebuild the `govcon_unknown_90day` IVF_PQ vector index over the lift's new chunks. Handoff: `docs/plans/SUBAWARD_SCOPE_ENRICHMENT_REINDEX_HANDOFF.md`.

**Precise current state (`✓ verified` this session):**
- `govcon_scope_vectors_90day` — `embedding_idx` covers **all 1,481,167 rows** (`idx_indexed = 1,481,167`, `idx_unindexed = 0`). Fully indexed. ✅
- `govcon_unknown_90day` — `embedding_idx` exists but is **STALE**: it covers only the pre-lift **1,042,059** rows; the **268,164** newly-appended chunks are `idx_unindexed`. The vectors ARE present (`null_unmarked = 0`); only the ANN index was not refreshed over them.

**Refinement vs the handoff doc's wording:** the handoff says the unknown index "FAILED" / new chunks are "not in the ANN index." That is substantively correct — the new chunks are unindexed and vector search over them is brute-forced. The precise mechanism: an `embedding_idx` object from a prior build still exists; Phase G's attempt to *refresh* it over the new rows aborted on the pyo3 panic in `compact_files`, so the index was never extended to the 268,164 new rows. The fix and DoD in the handoff are unchanged: broaden the best-effort compact catch (embed:188-193) and re-run `index --sink unknown` (and confirm scope, which is already complete).

**Properties of the open item:** **non-blocking** · **zero account/LLM tokens** (self-hosted Lance/MPS local compute) · **deliverable unaffected** (the capability profile reads requirements/doc_scope, not vectors; coverage locked at 64.1%).

`null_marked` (intentionally NULL, CUI-bracketed, never embedded/indexed): scope **326,866** (`✓ verified`, matches summary); unknown **350,099** (`✓ verified` — note: the summary's "~267k" for unknown is **low**; the actual marked-NULL count is 350,099).

---

## 9. Artifacts & pointers

**Docs**
- Runbook (ordered executable steps, risk register R1–R11, checkpoint map): `docs/plans/SUBAWARD_SCOPE_ENRICHMENT_EXECUTION_PLAN.md`
- Open-item handoff (reindex unknown): `docs/plans/SUBAWARD_SCOPE_ENRICHMENT_REINDEX_HANDOFF.md`
- Root-cause/worklist/blast: `docs/plans/SUBAWARD_SCOPE_ENRICHMENT_LIFT_PLAN.md`
- This record: `docs/plans/SUBAWARD_SCOPE_ENRICHMENT_RUN_RECORD.md`

**Key modules + functions**
- `pipelines/sam_gov/sam_attachment_extract_90day.py` — `_id_filter_sql` (:531), `_assert_routed_subset` (:545), gate-bypass in `phase1_route` (:609), Guard #2 (:639), `phase15_expand` id-filter (:715), `_build_tasks` (`inner_wl` register :1491, JOIN :1497), `--inner-uri` (:2002), `INNER_URI` override (:2033)
- `pipelines/sam_gov/sam_marking_fullbody_90day.py` — `_assert_marking_writeback_safe` (:262), swapped call sites (:301, :327)
- `pipelines/sam_gov/subaward_scope_append.py` — Phase E shared-sink append, `merge_insert("chunk_id")` under `SinkCommitLease`
- `pipelines/sam_gov/sam_labor_demand_extract_90day.py` — `_assert_marking_complete` (:1410), `_scope_pending` (:1419), `_pending_worklist` (:1606), ingest gate `run_pass_rate`/`gate_ok` (:2062-2063)
- `pipelines/sam_gov/sam_attachment_embed_90day.py` — `index_sink` (~:162-211), the best-effort compact guard to broaden (~:188-193)
- `pipelines/sam_gov/build_subawardee_capability_profiles.py` — `build` / `verify --content-hash` (deliverable)
- `pipelines/sam_gov/reference/p2b_extract_grind_workflow.js` — LLM grind harness (a **Workflow-tool** script, NOT `node`; `✓ verified` exists, 2,509 bytes)

**Datasets touched (shared, R2 `s3://data-sink/active/`)**
- Chunk sinks (appended): `govcon_scope_vectors_90day` (v286), `govcon_unknown_90day` (v297), `govcon_pricing_90day` (v240)
- Requirements/labor/doc_scope (cumulative): `govcon_award_requirements_90day`, `govcon_labor_demand_90day`, `govcon_doc_scope_90day`
- Profile (overwritten): `govcon_subawardee_capability_profiles` (v49)
- Prime ledger (audit-only, verdict-intact): `sam_attachment_extraction_90day` (v267)
- Throwaway (deleted in Phase J): six `_sublift_*` datasets under `active/_sublift_` — now 0 objects

**Scratch / logs (under `/tmp`, ephemeral — may no longer exist)**
- `/tmp/sublift_ids.txt` (the 3,969 allow-list), `/tmp/sublift_extract_ckpt.jsonl`, `/tmp/sublift_append_ckpt.json`, `/tmp/sam_marking_fullbody_report.json` (the F gate report), `/tmp/sublift_grind.js` (per-cycle harness copy), `/tmp/sublift_embed.log`, persistent venv `/tmp/sublift_venv`

---

## 10. Verification appendix

All probes read-only; run from the repo root via `doppler run --project core-x --config prd -- /Users/benjamincrane/core-x/.venv/bin/python` with R2 `storage_options` from Doppler. Repo HEAD `bbb7c8e`.

### A. PRs merged — `gh pr view`
```
for n in 478 479 480 481 482; do gh pr view $n --json number,title,state,mergeCommit,mergedAt; done
```
→ all `state=MERGED`; mergeCommits `5ab7366` / `a9a4435` / `29c502e` / `a62a758` / `0d3a171`; mergedAt 18:05 / 18:39 / 19:43 / 20:37 / 21:15 Z on 2026-06-15.

### B. Shared sinks + profile + profile CUI
```python
for s in ("govcon_scope_vectors_90day","govcon_unknown_90day","govcon_pricing_90day"):
    ds=lance.dataset(B+s,storage_options=so); print(s, ds.count_rows(), ds.version)
prof=lance.dataset(B+"govcon_subawardee_capability_profiles",storage_options=so)
print(prof.count_rows(), prof.count_rows(filter="has_extracted_scope = true"),
      prof.count_rows(filter="scope_summary IS NOT NULL AND has_extracted_scope = false"))
```
→ `govcon_scope_vectors_90day 1481167 286` · `govcon_unknown_90day 1310223 297` · `govcon_pricing_90day 156117 240`
→ profile: total **6586**, has_extracted_scope=true **4220** (= 64.1%), scope_summary_without_flag **0**, version 49
→ `clearance_level` column **absent** from profile schema (the summary's clearance CUI check is a no-op against the current schema).

### C. Prime ledger invariant
```python
led=lance.dataset(B+"sam_attachment_extraction_90day",storage_options=so)        # version, rows
t=led.to_table(columns=["state","run_id"]); con.register("led",t)
con.execute("SELECT count(*) FROM led WHERE run_id='sublift-marking'")                       # 3056
con.execute("SELECT count(*) FROM led WHERE run_id='sublift-marking' AND state<>'marking_fullbody'")  # 0
con.execute("SELECT count(*) FROM led WHERE run_id LIKE '%sublift%' AND state<>'marking_fullbody'")   # 0
con.execute("SELECT count(*) FROM led WHERE state='marking_fullbody'")                       # 5551 (3056 sublift + prior prime)
```
→ ledger **version 267**, rows 249,352; `sublift-marking` = **3,056** (all `marking_fullbody`); **0** sublift events in any verdict state.

### D. Embed completion + index coverage (the open item)
```python
for s in ("govcon_scope_vectors_90day","govcon_unknown_90day"):
    ds=lance.dataset(B+s,storage_options=so)
    ds.count_rows(filter="embedding IS NULL AND array_length(content_marking)=0")   # null_unmarked
    ds.count_rows(filter="embedding IS NULL AND array_length(content_marking)>0")   # null_marked
    ds.stats.index_stats("embedding_idx")                                            # indexed / unindexed
```
→ scope: `null_unmarked=0`, `null_marked=326866`, indexed **1,481,167**, unindexed **0** (complete).
→ unknown: `null_unmarked=0`, `null_marked=350099`, indexed **1,042,059**, unindexed **268,164** (STALE — the open item).

### E. Phase J cleanup
```python
boto3 s3.list_objects_v2(Bucket="data-sink", Prefix="active/_sublift_")  # KeyCount
```
→ **0** objects under `active/_sublift_`.

### F. Grind harness
```
ls -la pipelines/sam_gov/reference/p2b_extract_grind_workflow.js
```
→ exists, 2,509 bytes (a Workflow-tool script, not invoked via `node`).
