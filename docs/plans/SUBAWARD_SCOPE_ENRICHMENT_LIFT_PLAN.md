# Subaward Scope-Enrichment Lift — Non-Destructive Throwaway-Route + Append

**Date:** 2026-06-15 · **Status:** PLAN — uncommitted, operator-review-gated · **Author scope:** investigation + plan only (no extraction run, no shared-state mutation, no commit) · **Ground truth:** live R2 probes 2026-06-15 over `core-x/prd` creds (`pylance` against `s3://data-sink/active/`), repo verification with `file:line` citations.

> **One-line job:** unstick the sub-solicitation docs frozen at `skipped_out_of_scope` so the subawardee profile's solicitation-scope leg lifts from **50.1%** toward its measured **~71%** ceiling — via a default-OFF id-filter that routes ONLY the sub docs through a throwaway ledger + throwaway sinks (bypassing the shared GTM skip), extracts/chunks them, then appends the chunks to the shared sinks idempotently by `chunk_id`.

---

## 0. Verdict — DEFER (conditional GO)

**Recommendation: DEFER.** The lift is real, the mechanics are sound, and the worklist is measured — but it moves **one of four legs** on a profile whose lead signal (`top_subaward_description`) is already **100.0%** filled (6,585/6,586), and the canonical build plan's own §4 tradeoff line says **"deepen, not expand"** and explicitly classes container/skip recovery as "a separate track." Worse, a current-state blocker discovered during investigation (the chunk sinks are now IVF_PQ-indexed, which hard-refuses the CUI marking pass) means this is **no longer the cheap pre-embed write it was scoped as** — it now also requires a code patch to the marking pass and an embed-refresh + reindex tail. 

**Flip to GO the moment requirement-filtered sub targeting becomes a live priority** (Phase 4 of the build plan — "do-X companies under primes that need A,B,C"). At that point the solicitation-scope leg stops being a vanity coverage number and becomes the join key for the outreach product, and +1,274 newly-covered subs is direct funnel. Until then, the lead signal already carries manual outreach.

---

## 1. Measured worklist (live, 2026-06-15)

All numbers probed read-only; `embedding` column never materialized.

| Metric | Value | Source |
|---|--:|---|
| Sub-solicitation manifest distinct `resource_id` | 6,514 | `subawardee_solicitations_manifest` |
| …`status='downloaded'` in files SoR | 5,777 | × `sam_attachment_files_90day` |
| …`downloaded` AND **NOT** in any chunk sink (full unstick set) | **4,620** | × union(scope/unknown/pricing) |
| **…stuck at `skipped_out_of_scope` (THE throwaway-route target)** | **3,969** | × `_read_resolution(sam_attachment_extraction_90day)` |
| …`dropped_boilerplate` (filename L2_drop — re-drops, NOT recoverable this way) | 310 | |
| …`dropped_content_noise` (body boilerplate — re-drops) | 147 | |
| …`dropped_duplicate` (sha256 == a doc already chunked elsewhere) | 109 | |
| …`skipped_non_text` / `requires_ocr` / no-event / `routed` | 27 / 26 / 31 / 1 | |

**Reconciliation of the "3,969 / 3,862" figures:** the directive's **3,969 is exact** — it is precisely the count of sub-manifest downloaded-but-unchunked resources whose latest extraction-ledger resolution is `skipped_out_of_scope`, lane `out_of_scope`. The PDF-only subset of those is **2,979**; the full PDF-only worklist (any stuck state) is **3,530**. The "3,862" is not reproduced by any single cut today (window drift since it was recorded); use **3,969** as the target and **4,620** as the full unstick ceiling.

### 1.1 Mime mix of the 3,969 target (declared) and lane projection
Declared mime: `pdf` 2,979 · `docx`/`doc` ~430 · `xlsx`/`xls` ~445 · `zip` 86 · `txt`/`rtf`/`other` residual.
Lane projection (applying `SCOPE_RX`/`DROP_RX` at `sam_attachment_extract_90day.py:109–112` to `file_name`):

| Lane | Count | Sink after content triage |
|---|--:|---|
| L3_triage | 3,227 | mostly `govcon_unknown_90day` (some scope/pricing by header) |
| L4_structured (xlsx/xls) | 444 | `govcon_pricing_90day` / unknown |
| L1_scope | 212 | `govcon_scope_vectors_90day` |
| container (zip) | 86 | expands to inner files, re-routed |
| L2_drop / non_text | 0 / 0 | (none in the target set) |

### 1.2 CUI-marked subset (estimate — not measurable pre-extraction)
The 3,969 are not yet extracted, so no body markings exist on them. Of the **1,157** sub-solicitation resources already chunked, **181 (15.6%)** carry ≥1 marked chunk. Applying that rate: **~620 of the 3,969 are expected to carry a `content_marking` after extraction + the full-body marking pass**, and will be bracketed out of the LLM lane (`llm_state='excluded_marked'`). The regex lane still runs on them (local, no egress); only external-LLM egress and verbatim profile fields are gated. The actual marked count is **measured at marking-pass + bracket time, never assumed.**

### 1.3 Coverage ceiling (the value being bought)
| Stage | Subs covered | Rate (of 6,586 bridge UEIs) |
|---|--:|--:|
| `has_extracted_scope=true` in profile today | 3,302 | **50.1%** |
| Bridge UEIs with ≥1 chunked solicitation today | 3,430 | 52.1% |
| …after the 3,969 chunk (all extract to text — optimistic) | 4,704 | **71.4%** |
| Newly-covered subs | **+1,274** | +19.3 pp |

The directive's "~78%" is the upper bound; the **measured ceiling is ~71.4%**, and the *delivered* number will be lower — some of the 3,969 will hit `requires_ocr` / `dropped_content_noise` on extraction and yield zero chunks (no scope credit). Realistic delivered lift: **~50% → mid-60s%**, not 78%.

---

## 2. Root cause (verified against code)

1. **Stickiness — `_read_resolution` (`sam_attachment_extract_90day.py:493–514`).** Latest-state ranking is `row_number() OVER (PARTITION BY resource_id ORDER BY (state NOT IN _INTERMEDIATE) DESC, attempt DESC, completed_at DESC)`. `skipped_out_of_scope` is terminal (not in `_INTERMEDIATE = {routed, extracted_spreadsheet, requires_ocr}`, line 161), so it **outranks any later `routed`** for the same `resource_id`. Re-routing into the *shared* ledger is therefore a no-op for these docs.
2. **The skip is GTM-prime-specific but recorded as universal.** `phase1_route` (line 575) reads the shared GTM gate `sam_attachment_gtm_scope_90day` and stamps `skipped_out_of_scope` for any `gtm_scope <> 'in_scope'` canonical downloaded resource (lines 611–623). **All 3,969 targets are `gtm_scope='out_of_scope'`** in that gate (probed). The gate is the "Strained Middle" prime cohort lens — it was never meant to bind the subaward corpus.
3. **Consequence for the fix:** swapping to a throwaway ledger is **necessary but not sufficient** — under an empty throwaway ledger the gate (read from the shared table, line 575) would re-derive `skipped_out_of_scope` for the same 3,969. **The fix must also bypass the gate for the chosen id set.** This is the central design constraint.

---

## 3. The surgical code change (highest-risk step) — default-OFF id-filter

### 3.1 Where it goes
Two CTEs in `sam_attachment_extract_90day.py`, plus the CLI:

**(a) `phase1_route` — candidate CTE (`canon`, lines 584–602).** Add a resource-id predicate to the `WHERE` and force the gate OFF when the filter is active:

```python
# new signature: phase1_route(*, so, run_id, max_files=0, only_resource_ids: set[str] | None = None)
id_filter_sql = ""
if only_resource_ids is not None:
    if not only_resource_ids:                                  # GUARD #1: empty set is a hard error
        raise RuntimeError("phase1_route: --resource-ids resolved to an EMPTY set; refusing to run "
                           "(an empty filter would fall through to the full corpus).")
    ids = sorted(only_resource_ids)
    id_filter_sql = "AND f.resource_id IN (" + ",".join("'" + i.replace("'", "''") + "'" for i in ids) + ")"

# GATE BYPASS: an explicit id set is itself the scope decision — the GTM gate must NOT re-skip them.
scope = None if (max_files or only_resource_ids is not None) else _read_scope_gate(so)
```
Then interpolate `{id_filter_sql}` into BOTH the `canon` CTE `WHERE` (line 590) and the `oos`/`noncanon` selects (lines 605–623) so the throwaway run never emits `skipped_out_of_scope`/`dropped_duplicate` events for out-of-set resources. **GUARD #2 (post-route assertion):** after building `routed`, assert `set(rid for rid,*_ in routed) <= only_resource_ids` — raise if any out-of-set id leaked. This is the assertion that makes "accidentally process 11,067 prime files impossible."

**(b) `_build_tasks` — candidate CTE (`cand`, lines 1420–1423).** Same predicate on the resolution-view selection:
```python
# new signature: _build_tasks(so, lanes, run_id, only_resource_ids: set[str] | None = None)
extra = ""
if only_resource_ids is not None:
    extra = "AND resource_id IN (" + ",".join("'" + i.replace("'", "''") + "'" for i in sorted(only_resource_ids)) + ")"
cand = con.execute(f"""
    SELECT resource_id, parent_resource_id, lane FROM res
    WHERE state IN ('routed','extract_failed') AND lane IN ({lane_pred}) {extra}
""").to_arrow_table()
```
(With a throwaway ledger this is belt-and-suspenders — the throwaway `res` only contains the routed targets anyway — but it makes the filter authoritative regardless of which ledger is pointed at.)

**(c) CLI (`_cli`, after line 1952).** Add:
```python
p.add_argument("--resource-ids", default=None, help="comma-separated ids; route/extract ONLY these (gate forced OFF)")
p.add_argument("--resource-ids-file", default=None, help="newline-delimited id file (preferred for 3,969 ids)")
```
Resolve to `only_resource_ids: set[str] | None` (None when both unset → **default OFF, current behavior byte-for-byte unchanged**), thread into `phase1_route(...)` (line 1979) and `phase2_extract(...)`→`_build_tasks(...)` (line 1986/1501). `phase15_expand` (containers, line 656) takes the same optional set so the 86 zip targets expand only their own inner files.

### 3.2 Default-OFF proof & in-scope override semantics
- `only_resource_ids` defaults to `None`. Every existing call path passes nothing → `id_filter_sql=""`, `scope` unchanged, behavior identical. **No existing run is affected.**
- When set: the filter is a hard allow-list, the gate is forced OFF (the ids ARE the scope decision), and two guards (empty-set raise + post-route subset assertion) bound the blast.
- The throwaway ledger/sinks are supplied independently via the **existing** `--extraction-uri / --scope-uri / --pricing-uri / --unknown-uri / --dedup-uri` overrides (CLI lines 1948–1952, applied at 1963–1968). No new URI plumbing.

---

## 4. Phase-by-phase execution plan

Throwaway namespace (all under `active/_sublift_*`, deletable after append):
```
EXT_TW=s3://data-sink/active/_sublift_extract_ledger/
SCOPE_TW=s3://data-sink/active/_sublift_scope/
PRICE_TW=s3://data-sink/active/_sublift_pricing/
UNK_TW=s3://data-sink/active/_sublift_unknown/
DEDUP_TW=s3://data-sink/active/_sublift_dedup/
IDS=/tmp/sublift_ids.txt   # the 3,969 newline-delimited resource_ids
```
Dependency invocation matches the module docstring (lines 37–41): `doppler run --project core-x --config prd -- uv run --with pylance --with pyarrow --with duckdb --with boto3 --with 'psycopg[binary]' --with pypdfium2 --with python-docx --with openpyxl --with xlrd --with pdfplumber --with striprtf --with charset-normalizer python …`. `soffice` must be on PATH (`_assert_soffice`, line 1400) for the 71 `.doc` + legacy `.xls` targets.

### Phase A — Materialize the id worklist (read-only, local)
**Action:** run the §1 probe, write the 3,969 `skipped_out_of_scope` sub-manifest resource_ids to `$IDS`.
**Touches:** nothing (read-only).
**Idempotency:** deterministic query; re-run overwrites the file.
**Cost:** local CPU, seconds.
**DoD gate:** `wc -l $IDS == 3969`; every id ∈ manifest, `status='downloaded'`, `state='skipped_out_of_scope'`, not in any chunk sink.
**Guardrail:** this file IS the allow-list; the route guard asserts the routed set ⊆ this file.

### Phase B — Patch + unit-smoke the id-filter (code; the §3 change)
**Action:** implement §3 (a/b/c) + patch the marking-pass blocker (§4 Phase F note). Add a unit test mirroring `tests/test_sam_attachment_finalize_dedup.py` asserting: empty-set raises; out-of-set id never appears in routed events; gate forced OFF when filter set.
**Touches:** `sam_attachment_extract_90day.py`, `sam_marking_fullbody_90day.py`, a new test file. No data.
**DoD gate:** unit test green; `--phase route --resource-ids <2 fake ids> --extraction-uri $EXT_TW …` on throwaway sinks emits exactly ≤2 events and `phase1: GTM gate ON` does **not** print.
**Guardrail:** test proves the default-OFF path is unchanged (no `--resource-ids` → gate ON, full corpus selected).

### Phase C — Throwaway route + expand (writes ONLY throwaway namespace)
```
--phase route   --resource-ids-file $IDS --extraction-uri $EXT_TW --scope-uri $SCOPE_TW \
                --pricing-uri $PRICE_TW --unknown-uri $UNK_TW --dedup-uri $DEDUP_TW
--phase expand  --resource-ids-file $IDS --extraction-uri $EXT_TW … (same overrides)   # 86 zip containers
```
**Touches:** `_sublift_*` only. **The shared ledger and shared sinks are NOT opened for write.**
**Idempotency:** route anti-joins the throwaway resolution view (line 564); re-run is a no-op.
**Cost:** local CPU, minutes.
**DoD gate:** `$EXT_TW` has ~3,969 `routed` events (+ expanded inner files); zero `skipped_out_of_scope` events (gate was OFF); routed set ⊆ `$IDS` (the assertion).
**Guardrail:** post-route subset assertion (Guard #2) — aborts before any extract if an out-of-set id leaked.

### Phase D — Throwaway extract (local CPU, multi-hour, no cap)
```
--phase extract --lane all --resource-ids-file $IDS --daemon --resume \
                --extraction-uri $EXT_TW --scope-uri $SCOPE_TW --pricing-uri $PRICE_TW --unknown-uri $UNK_TW \
                --ckpt /tmp/sublift_extract_ckpt.jsonl
--phase finalize --extraction-uri $EXT_TW --scope-uri $SCOPE_TW --pricing-uri $PRICE_TW --unknown-uri $UNK_TW
```
**Touches:** `_sublift_*` chunk sinks + ledger only. Parallel pdfium/docx/xlsx pool + serialized soffice lane (`.doc`/`.xls`), single committer, `SinkCommitLease` on the throwaway sink URIs (slugs differ from prod → never contend, class docstring lines 244–245).
**Idempotency:** resolution-view ∪ JSONL checkpoint resume (line 1502); `finalize` row-address dedup restores `chunk_id` uniqueness.
**Cost:** **local-CPU, multi-hour** — ~3,969 docs, p50 ~8.5k chars but a long tail to 4 MM chars; this is the dominant wall-clock leg. No account/LLM spend. Daemonize + `--resume`.
**DoD gate:** `_sublift_*` chunk sinks populated; ledger terminal-state distribution sane (most `extracted_unknown`/`extracted_scope`, small `requires_ocr`/`dropped_content_noise` tail); `finalize` reports `chunk_id` dupes removed == small.
**Guardrail:** throwaway sinks are brand-new (no vector index) → `finalize` compaction path is allowed and safe; shared prod sinks untouched.

### Phase E — Append throwaway chunks → SHARED sinks (idempotent by `chunk_id`)
**Action:** a small dedicated append script (new, e.g. `pipelines/sam_gov/subaward_scope_append.py`) that, per sink, reads `_sublift_*` chunk rows (projection only — **never the `embedding` column**), casts to the shared sink schema (`embedding`/`lexicon_hit`/`cells` columns set to their write-time defaults exactly as `_build_chunks` does, lines 1260–1279), and `ds.merge_insert("chunk_id").when_matched_update_all().when_not_matched_insert_all().execute(src)` into the SHARED sink, under that sink's `SinkCommitLease`.
**Touches:** **shared `govcon_scope_vectors_90day` / `govcon_unknown_90day` / `govcon_pricing_90day`** — the first and only shared-state write. New rows land with `embedding = NULL` (picked up by the embed refresh, Phase G).
**Idempotency:** `chunk_id = f"{rid}:{ix:04d}"` is deterministic (line 1265). A re-run merges the same keys → `when_matched_update_all` rewrites identical values → **no-op** (zero net row delta). `merge_insert("chunk_id")` is proven safe on these IVF_PQ-indexed sinks — the embed module does exactly this at `sam_attachment_embed_90day.py:115` (Lance rewrites only matched fragments; the index covers unmatched rows; the new NULL-embedding tail is brute-forced until reindex).
**Cost:** local CPU, minutes (≤ a few hundred MB of new chunk rows).
**DoD gate:** shared sink `count_rows()` increases by exactly the throwaway chunk count; `count(*) == count(DISTINCT chunk_id)` holds (re-run uniqueness); spot-check 5 target resource_ids now present in the shared sink.
**Guardrail:** projection excludes `embedding`; schema cast asserts column/type parity (Lance rejects a `string`-vs-`large_string` mismatch — the `text`/`cells` columns are `large_string`, lines 403/425); a `_assert_no_vector_index`-style check is NOT applied (merge_insert is the index-safe path, unlike overwrite).

### Phase F — CUI marking pass on the new chunks (BEFORE any LLM) — **requires the §3 marking patch**
**BLOCKER (current state):** `govcon_scope_vectors_90day` and `govcon_unknown_90day` now carry `embedding_idx` (IVF_PQ) — the embed pipeline has run since the recon doc. `sam_marking_fullbody_90day.py` calls `_assert_no_vector_index` at lines 288 and 314, which **RAISES on a vector-indexed sink** → the marking pass cannot run as written. This is a current-state contradiction (the marking pass was scoped for the pre-embed unindexed window, which has closed).
**Patch (part of Phase B):** in `sam_marking_fullbody_90day.py`, replace the two `_assert_no_vector_index(...)` calls on the write-back with a no-op-allow for the **subset-column `merge_insert("chunk_id")`** path — this is the SAME index-safe pattern the embed module already uses at line 115 (subset/`when_matched_update_all` rewrites only matched fragments; the docstring lines 28–32 already assert `text`/`embedding` are untouched and `embedding` is never read). Keep the assert on any overwrite/compaction path. Document the deviation inline. (Alternative if a code change to the marking module is undesirable: run marking on the throwaway sinks BEFORE Phase E and append the already-marked chunks — but that splits the marking pass across two corpora and weakens the single-enforcement-point guarantee; the patch is cleaner.)
**Action (post-patch):**
```
sam_marking_fullbody_90day.py --phase scan --daemon       # full-sink reassembly + detection (heavy)
                              --phase writeback            # promotions → chunk rows (merge_insert chunk_id)
                              --phase reconcile             # write-back==expansion assert
```
**Touches:** shared scope/unknown/pricing `content_marking` column only (subset merge_insert). Whole-sink scan (no `--resource-ids` filter exists in this module) — but the writeback worklist is derived from LIVE state where `promote(existing,detected) != existing`, so **only the new chunks needing a promotion are written** (idempotent, double-apply-proof, docstring lines 33–36).
**Cost:** local CPU, **multi-hour full-sink scan** (re-assembles every resource in all three sinks — ~2.5 MM chunks). Daemonize.
**DoD gate:** `reconcile_overall == PASS`; every promoted resource's chunk-level `content_marking` equals its decided post-set; the new target resources that carry markings now show non-empty `content_marking` on their chunks.
**Guardrail:** this pass MUST complete before Phase H (LLM). Chunk-level `content_marking` is the single egress enforcement point (build plan §6 anti-pattern #10). The marking pass is whole-sink, so it also re-validates the existing corpus (safe — promotion-only).

### Phase G — Embed refresh + reindex on the new chunks (self-hosted; optional-but-recommended)
**Action:** `sam_attachment_embed_90day.py` — `embed_sink` worklist is `embedding IS NULL` (already 18–25% NULL pre-lift; the new chunks add to it), then `index_sink` (compact best-effort → `create_index IVF_PQ replace=True` → scalar campaign).
**Touches:** shared scope/unknown `embedding` column + indices.
**Idempotency:** `embedding IS NULL` worklist is free-resume; `merge_insert("chunk_id")` full-row (line 115); `create_index(replace=True)`.
**Cost:** **self-hosted GPU/MPS** (BGE-large), ~tens of minutes for the new-chunk delta + reindex; **zero account/API spend** (model is self-hosted regardless of policy, build plan §4). Marked rows embed too (gate is consumption, not embedding) but stay out of external egress.
**DoD gate:** `embedding IS NULL == 0` for the UNMARKED set per sink (the completion contract, anti-pattern #6); vector index present.
**Guardrail:** queries between append and reindex are correct-but-slower (brute-forced NULL tail) — acceptable, documented in the embed re-entry runbook.

### Phase H — Regex lane (free) + LLM lane (account-burning) scoped to the new ids
**Regex lane (local, free):**
```
sam_labor_demand_extract_90day.py --phase extract --resource-ids-file $IDS --resume --daemon
                                  --phase index    # after merges settle
```
`--resource-ids` is already supported (lines 1029–1034, filtered-slice path 1079–1094). Scoped delete-before-merge on `govcon_award_requirements_90day` + `govcon_labor_demand_90day` (idempotent, anti-pattern #3). **Redaction-at-write:** marked resources get NULL `evidence_quote`/`requirement_detail`/`place_of_performance_text` (docstring lines 24–29) — the write-side CUI gate.
*Cost: local CPU, free. DoD: every target id terminal in the ledger regex lane; sampled evidence substring-asserts green.*

**LLM lane (account-burning, resumable):**
```
--phase bracket                                   # derive marked set LIVE from chunk content_marking;
                                                  #   stamps excluded_marked (the ~620 est.) — reversible
--phase select  [--pilot N] --manifest-out /tmp/m.json   # stage task files (marked HARD-asserted out, lines 1529–1534)
<grind: pipelines/sam_gov/reference/p2b_extract_grind_workflow.js — agents read tasks/, write results/>
--phase ingest                                    # validate (≥98% pass-rate gate) + scoped land + ledger
```
The grind harness (`p2b_extract_grind_workflow.js`) launches session-agent lanes in throttled groups of `CONC` with an **all-failure circuit breaker** (2 consecutive dry groups → stop) that detects the **5-hour session cap / outage** and halts cleanly; resume by re-running (existing result files are skipped, line 11 NOTE). The ledger `llm_state` passes `pending → … → done`, so a mid-grind stop **resumes per-resource, never re-pays** (build plan §5). For a corpus this small (≈3,969 minus ~620 marked ≈ **~3,350 LLM docs**, mostly L3_triage/unknown), this is **a single grind session or two** — far under the multi-batch concern of the full 15,570-doc corpus. **Multi-session lane option:** raise `CONC`/`NB` per cycle in the harness constants; each cycle is independently resumable.
*Cost: **account-burning** (session-agent token spend), **resumable across 5h caps**. DoD: run pass-rate ≥98% or quarantine-wholesale; per-resource ledger terminal.*
**Guardrail:** `bracket` + the `build_task_payload` hard-assert (lines 1529–1534) + the `select`/`census` belt-checks (lines 1601–1602, 1653–1654) make it structurally impossible for a marked-doc chunk to reach a staged task file — three independent gates behind the single chunk-level `content_marking` signal.

### Phase I — Rebuild the subawardee capability profile (idempotent overwrite)
**Action:** `build_subawardee_capability_profiles.py --phase build` then `--phase verify`.
**Touches:** overwrites `govcon_subawardee_capability_profiles` (line 511, `mode="overwrite"`). It re-derives `sub_res_ids` from the bridge→manifest join and re-rolls `has_extracted_scope` / `n_scope_solicitations` / requirements / `scope_summary` / `capability_tags` from `govcon_award_requirements_90day` + `govcon_doc_scope_90day` over those resources — so the new chunks/requirements are **picked up automatically**; no profile-code change needed.
**Idempotency:** overwrite-mode snapshot, `PRAGMA threads=1` deterministic aggregation (line 189), stamped with consumed run_ids.
**Cost:** local CPU, minutes.
**DoD gate:** `verify` shows `has_extracted_scope` risen from 3,302 toward ~4,700; `row_eq_universe`/`row_eq_distinct_uei` true; **CUI checks `scope_summary_without_flag == 0` and `clearance_level_without_flag == 0`** (lines 557–558); the `_assemble` CUI pre-flight (lines 148–159) did not raise (refuses the build if any `govcon_doc_scope_90day` marked row or any requirements verbatim-text leak exists).
**Guardrail:** the build itself REFUSES to run on a CUI-invariant violation — a hard backstop that the marking pass + redaction-at-write held.

### Phase J — Throwaway cleanup
Delete the `_sublift_*` datasets + leases (operator action; tripped the session safety guard during investigation, so do it from a terminal). The shared sinks already carry the appended chunks; the throwaway namespace is pure scratch.

---

## 5. CUI / egress invariant (end to end)

1. **Marking before LLM (Phase F before Phase H):** the full-body marking pass back-propagates promotions onto chunk-level `content_marking` — the single enforcement point. It MUST complete before any external/LLM call (in-session agent reading counts as egress, build plan §6 anti-pattern #10).
2. **LLM bracket (Phase H):** `phase bracket` derives the marked set LIVE and stamps `excluded_marked`; `build_task_payload` hard-asserts no marked chunk reaches a task file; `select`/`census` re-assert. Three gates behind one signal.
3. **Redaction at write (regex lane):** marked resources get NULL `evidence_quote`/`requirement_detail`/`place_of_performance_text`.
4. **Profile build refuses on violation:** `_assemble` raises if `govcon_doc_scope_90day` has marked rows or requirements rows leak verbatim text; `verify` asserts the consistency invariants == 0.
5. **Embedding (Phase G):** marked rows embed on the self-hosted model but their NULL/indexed vectors are kept out of external egress; the consumption gate (`array_length(content_marking)=0`) lives in the query layer, not the embed.

---

## 6. Effort / scale

| Leg | Engine | Scale | Wall clock | Spend |
|---|---|---|---|---|
| Throwaway route/expand (C) | local CPU | 3,969 + 86 zips | minutes | $0 |
| **Throwaway extract (D)** | **local CPU** | **3,969 docs, no cap** | **multi-hour (dominant)** | $0 |
| Append (E) | local CPU | ≤ few hundred MB chunks | minutes | $0 |
| **Marking pass (F)** | **local CPU** | **whole-sink scan ~2.5 MM chunks** | **multi-hour** | $0 |
| Embed refresh + reindex (G) | self-hosted GPU/MPS | NULL delta + reindex | tens of min | $0 (self-hosted) |
| Regex lane (H) | local CPU | 3,969 ids | minutes–hour | $0 |
| **LLM lane (H)** | **session-agent (account-burning)** | **~3,350 unmarked docs** | **1–2 grind sessions, resumable across 5h caps** | **account token spend** |
| Profile rebuild (I) | local CPU | overwrite | minutes | $0 |

Two multi-hour local-CPU legs (extract D, marking F) dominate wall clock. The only account-burning leg (LLM H) is small for this corpus (~3,350 docs) and fully resumable.

---

## 7. Risks register

| # | Risk | Neutralization |
|---|---|---|
| R1 | **Shared-ledger re-stickiness** — re-routing into the shared ledger re-derives `skipped_out_of_scope`. | Throwaway ledger (`$EXT_TW`) + **gate forced OFF when the id-filter is set** (§3.1a). Verified all 3,969 are `out_of_scope` in the gate, so ledger-swap alone is insufficient — the gate bypass is mandatory and is the core of the fix. |
| R2 | **Unrouted-prime blast** — filter absent/defaulted-on-empty → route processes the in-scope prime backlog. Measured: **11,067 in-scope canonical (3,054 unchunked)** would route under an empty throwaway ledger with gate ON. | Default-OFF (`None`) leaves behavior unchanged; **empty-set raise (Guard #1)** + **post-route subset assertion (Guard #2)** make out-of-set processing impossible; unit test in Phase B proves it. |
| R3 | **Schema drift on append** — `large_string` vs `string` on `text`/`cells`; missing `lexicon_hit`/`cells`/`embedding` defaults. | Phase E casts to the live shared schema (asserts parity; Lance rejects mismatch) and sets write-time defaults exactly as `_build_chunks` (lines 1260–1279). |
| R4 | **IVF_PQ-indexed sinks block the marking pass** (current-state blocker; `_assert_no_vector_index` raises at lines 288/314). | Phase B patches the two write-back asserts to allow the subset-column `chunk_id` merge_insert — the proven index-safe pattern from `sam_attachment_embed_90day.py:115`. Append (E) and embed (G) already use the index-safe merge_insert path. |
| R5 | **Partial-run resumability** — crash mid-extract/marking/LLM. | Extract: resolution-view ∪ JSONL checkpoint (line 1502) + `finalize` dedup. Marking: live-state worklist (double-apply-proof). LLM: ledger `llm_state` + existing-result-file skip + circuit breaker. All daemonized with `--resume`. |
| R6 | **Throwaway-artifact cleanup** — `_sublift_*` left behind. | Phase J deletes them from a terminal (session safety guard blocks `rm` of absolute R2-adjacent paths); leases use distinct slugs so they never contend with prod even if left. |
| R7 | **Append double-write** — re-run inflates rows. | `chunk_id` deterministic + `merge_insert` `when_matched_update_all` → re-run is a zero-delta no-op; DoD asserts `count == count(DISTINCT chunk_id)`. |
| R8 | **Delivered lift < ceiling** — OCR/content-noise tail yields no chunks for some of the 3,969. | Honest framing: ceiling is ~71.4%, delivered ~mid-60s%. Measured at profile `verify`, not assumed. Not a failure mode — a scope-truth disclaimer. |
| R9 | **Embed-index staleness** — new NULL-embedding tail un-indexed until reindex. | Documented acceptable (brute-forced tail); Phase G `index_sink` gate requires `embedding IS NULL == 0` for the unmarked set before declaring done. |

---

## 8. Durable fix (recommendation only — NOT part of this lift)

This collision recurs for **every** non-prime corpus (subs, other-entity cohorts) because the GTM "Strained Middle" gate is **prime-use-case-specific but stamped into the SHARED extraction ledger as a universal terminal**. The throwaway-route + append is a one-off workaround, not a structural fix.

**Durable fix: per-use-case extraction scope.** Decouple the skip verdict from the shared ledger by either (a) making `gtm_scope` a **multi-cohort tag** (`in_scope_prime`, `in_scope_subaward`, …) so a resource can be in-scope for one corpus and out for another, with `phase1_route` consulting the cohort relevant to the run; or (b) recording `skipped_out_of_scope` with a **cohort qualifier in `lane`/`error`** and making `_read_resolution` cohort-aware so a skip for cohort-X never blocks routing for cohort-Y. Either removes the "skip verdict was use-case-specific but recorded as universal" defect (root cause §2) so no future non-prime corpus needs a throwaway route. Scope this as a follow-on; it is the right home for the engineering effort if sub/other-entity extraction becomes recurring.

---

## 9. Appendix — reproduction

All §1 numbers reproduce with `doppler run -- /Users/benjamincrane/core-x/.venv/bin/python` over `R2_ACCESS_KEY_ID`/`R2_SECRET_ACCESS_KEY`/`R2_ENDPOINT`, reading `subawardee_solicitations_manifest`, `subawardee_solicitations_bridge`, `sam_attachment_files_90day`, `sam_attachment_extraction_90day` (resolved via the `_read_resolution` ranking: terminal-first / attempt-desc / completed-desc, excluding `marking_fullbody`), the three chunk sinks, `sam_attachment_gtm_scope_90day`, and `govcon_subawardee_capability_profiles`. Projection-only; `embedding` never materialized. NOTE: run probe scripts from inside the repo working dir — a script under `/tmp` puts `/tmp` on `sys.path[0]` and a stray `/tmp/inspect.py` shadows stdlib `inspect`, breaking `import numpy`/`pyarrow`.
