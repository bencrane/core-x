# Stage-3 Download + Extraction Backlog — Multi-Account Wave Plan

**Mode:** PLANNING + READ-ONLY SIZING. **Snapshot:** 2026-06-21 (UTC), R2 Gen-3 Lance SoR `s3://data-sink/active/`. Zero writes, zero harvest, zero LLM spend in producing this plan.
**Business goal:** grow the PDF-derived signals exposed by `govcon_award_scope_requirements` — the `insurance_bonding` + `equipment_capability` arrays and `labor_category` — for the GovCon Capital Partners GTM (payroll funders, factoring, equipment finance, **and surety/bonding brokers**). The surety/bonding signal (`payment_bond`, `performance_bond`, `bid_bond`, `insurance:*`) is a first-class co-product, not an afterthought. Priority cohort: Small-Business primes (`business_size_code='S'`), especially `current_total_value_of_award > $500K`.
**Probes (reproducible, read-only):** [`scripts/_plan_stage3_backlog_probe.py`](../../scripts/_plan_stage3_backlog_probe.py) (funnel + bonding yield), [`scripts/_plan_static_partition_probe.py`](../../scripts/_plan_static_partition_probe.py) (static shard verification), [`scripts/_plan_claim_model_sim.py`](../../scripts/_plan_claim_model_sim.py) (rejected dynamic model, kept as the counter-example). Raw JSON: `/tmp/_plan_stage3_backlog.json`, `/tmp/_plan_static_partition.json`.

---

## BLUF

Stage-2 link manifests are complete (589,166 distinct files / 181,438 solicitations). The backlog that gates GTM signal is everything downstream of links for the SB priority cohort:

- **Two backlogs, SB cohort (bridged to active SB awards via Sol# ∪ winners award-key):**
  - **Stage-3 download-pending:** **12,419 files** harvested-as-links but not yet `status='downloaded'`.
  - **Extract-pending:** **34,045 files** downloaded but with no row in `govcon_award_requirements`.
  - **Fully-pending (links-ready, no requirements yet):** **46,464 files** across **10,102 SB awards** — this reconciles to the prior ~10K SB-award figure exactly.
- **SB > $500K slice** (the money cohort): **19,745 fully-pending files** across **1,338 awards**; SB>$1M 15,847 files / 887 awards; SB>$5M 8,501 files / 296 awards.
- **Bonding co-product is real and high-yield:** of the **10,300** already-extracted SB files, **17.5%** (1,802) carry ≥1 `insurance_bonding` requirement; **1,512** carry an explicit surety bond. Applying that rate to the 34,045 SB extract-pending files forecasts **≈5,900 newly bond-mandated SB files** surfaced, lifting ≈1,000–1,800 distinct SB awards into the surety-broker target list.
- **Token/cost:** the LLM grind is the only metered stage and it is trivially cheap. Per the BIGTHREE_V2 run record, **799 docs → 5.26M input tokens, gate 0.9959, ~3.5h wall on CONC=5 across 6 waves, ~$0 marginal cash** (`session-opus` is the Max-subscription agent-handoff lane). The full SB extract-pending corpus (~34K files, of which only a token-budgeted, lexicon-gated subset is staged) is **≈25–35M input tokens total, ≈$250–350 economic API-equivalent / ≈$0 cash, single-digit days of paced wall-clock** across all accounts. The regex lane (P3a) carries the deterministic majority for free.

**Single recommended sequencing:** P0 snapshot → P1 Stage-3 byte download (priority-tiered, WAF-paced) → P2 text-extract+chunk → P3a regex lane (free, deterministic, lands bonding immediately) → P3b LLM grind (multi-account, **static disjoint shards**, priority-ordered waves) → P4 ingest+validate → P5 rematerialize serving MVs → P6 verify uplift. **Value lands first**: every stage orders work SB>$5M → >$1M → >$500K → remainder.

---

## Sizing table (live probe numbers)

All counts at **file grain** unless noted; cohort bridge = active SB award (`active_current OR active_potential`, `business_size_code='S'`) → solicitation_identifier (FPDS) **∪** PIID→SAM-universe-recovered Sol# **∪** winners-manifest award-key → manifest `resource_id`. Reproduce every row with:

```
doppler run -p core-x -c prd -- uv run --no-project --with boto3 --with pylance --with duckdb \
  python3 scripts/_plan_stage3_backlog_probe.py > /tmp/_plan_stage3_backlog.json
```

| Cohort | Link files | **Download-pending** | **Extract-pending** | **Fully-pending** | Awards w/ link | **Awards fully extract-pending** | Sol# w/ link |
|---|---:|---:|---:|---:|---:|---:|---:|
| all_active | 81,492 | 19,652 | 47,851 | 67,503 | 36,077 | 15,660 | 16,737 |
| **small_business** | 56,764 | **12,419** | **34,045** | **46,464** | 17,404 | **10,102** | 11,371 |
| **sb_gt_500k** | 27,644 | **5,400** | **14,345** | **19,745** | 3,066 | **1,338** | 2,753 |
| sb_gt_1m | 22,460 | 4,313 | 11,534 | 15,847 | 2,126 | 887 | 1,913 |
| sb_gt_5m | 12,246 | 2,226 | 6,275 | 8,501 | 798 | 296 | 737 |

**Definitions** (stage predicates, exact):
- **download-pending** = manifest `resource_id` ∉ `sam_attachment_files(status='downloaded')`.
- **extract-pending** = `resource_id` ∈ downloaded ∧ ∉ `govcon_award_requirements`.
- **fully-pending** = `resource_id` ∉ `govcon_award_requirements` (= download-pending ∪ extract-pending; this is the LLM/regex worklist universe).
- **awards fully extract-pending** = distinct award keys with **no** bridged `resource_id` yet in `govcon_award_requirements`.

**Global ledger context (probe):** manifest 589,166 distinct files / 181,438 Sol#; `sam_attachment_files` downloaded = 126,932; `govcon_award_requirements` distinct extracted `resource_id` = 21,580. (All three match the established facts.)

### Bonding yield (the surety co-product) — `bonding_yield_sb`

Among the **10,300** already-extracted SB files:

| Metric | Value |
|---|---:|
| SB extracted files | 10,300 |
| Files with ≥1 `insurance_bonding` | **1,802 (17.5%)** |
| Files with an explicit surety bond (`%bond%`) | **1,512** |

**Bond-type distribution (top, file counts):** `payment_bond` 1,192 · `performance_bond` 951 · `bid_bond` 905 · `insurance:general_liability` 234 · `insurance:workers_compensation` 153 · `insurance:automobile_liability` 140 · `insurance:professional_liability(:1)` 82 · `payment_bond:100pct` 21 · `insurance:general_liability:5000000` 19 · `performance_bond:100pct` 17 · `bid_bond:20pct` 13. The exact GTM-target literals named in the brief (`payment_bond:100pct`, `performance_bond:100pct`, `insurance:general_liability:5000000`) are present and counted.

**Forecast:** 17.5% × 34,045 SB extract-pending files ≈ **5,958 newly bond-mandated SB files**. De-duplicated to award grain (the extract-pending files span 10,102 awards), this is on the order of **1,000–1,800 net-new SB awards** entering the surety-broker target list — the single highest-leverage GTM output of the wave.

**Caveat — `equipment_capability` is noisy (do not over-promise).** The extracted-SB distribution is long-tailed and heterogeneous (`spark_arrester` 6, `water_tank_truck_or_trailer` 5, `crane` 5, `primavera_p6` 3, `n_plus_1_redundancy` 3, `copper_conductors_only_no_aluminum` 2). Useful as a secondary equipment-finance signal but not a clean array; surface it, don't headline it.

---

## The multi-account segmentation design (centerpiece)

### Design rationale — why STATIC, not dynamic runtime claiming

The earlier instinct was a runtime atomic claim/lease ledger. **That is wrong for this substrate and is explicitly rejected.** R2/Lance is **append-only with eventual landing and has no transactional claim store**. A worker can be mid-extraction (work-in-process, not yet committed to R2) while another worker claims the same `resource_id`; the *in-process-before-commit* gap is a race a non-transactional store cannot arbitrate, producing double-work and, absent perfect idempotency, double-append. A simulation of the dynamic model ([`_plan_claim_model_sim.py`](../../scripts/_plan_claim_model_sim.py)) "passes" **only because it assumes an atomic queue pop that the real substrate does not provide** — it is retained here as the counter-example, not the design.

**First-principles derivation** (objective / constraints / asset):
- **Objective:** maximize net-new extracted awards — especially SB>$500K and the surety/bonding co-product — per unit agent-time, with **zero double-work** and **safe resume**.
- **Constraints:** append-only R2, no transactional claim store, eventual landing, residential-IP + WAF pacing on download, finite token budget, and **variable accounts × sessions** concurrency (the operator spins sessions up/down ad hoc).
- **Asset:** several accounts × several sessions is **embarrassingly parallel — iff the partition is disjoint by construction.**

The resolution: **guarantee disjointness at ASSIGNMENT, not at runtime.** Compute the partition up front so every `resource_id` maps to exactly one shard deterministically; commit timing then becomes irrelevant because no two workers ever target the same id.

### The partition

```
shard = abs(hash(resource_id)) % N_TOTAL_SHARDS        # N_TOTAL_SHARDS = 96
```

`N=96` is chosen generously so it exceeds any plausible accounts×sessions count (3 accounts × ~10 sessions ≈ 30 ≪ 96), leaving headroom to add sessions/accounts **without repartitioning**. Many small shards = scheduling flexibility.

**Assignment hierarchy** (mirrors the precedent's `batch_NNN.txt` file-list model, [`p2b_extract_grind_workflow.js:11,21`](../../pipelines/sam_gov/reference/p2b_extract_grind_workflow.js) — agents read *only* their assigned file-list, `Use ONLY the chunk text inside each task file`):
1. **Account ← disjoint shard RANGE.** Account 0 owns shards `[0,31]`, account 1 `[32,63]`, account 2 `[64,95]`. A hard cross-account guard: an account's sessions only ever read shard files in its range.
2. **Session ← individual shard(s),** handed out within the account's range as sessions spin up. One shard owned by one session at a time, recorded once in the per-shard owner field. Round-robin within the range (`acct_range[sess::n_sessions]`).
3. **Pre-materialize** each shard as `shard_NN.ids` (the file-list). A session reads only its assigned shard file(s). This is the only artifact a session needs — no R2 access for assignment.

### Verification — `_plan_static_partition_probe.py` (live)

```
doppler run -p core-x -c prd -- uv run --no-project --with boto3 --with pylance --with duckdb \
  python3 scripts/_plan_static_partition_probe.py > /tmp/_plan_static_partition.json
```

Over the **46,464** pending SB `resource_id`s:

| Property | Result | Pass |
|---|---|---|
| `rows == distinct` resource_ids | 46,464 == 46,464 | ✓ |
| **ids in >1 shard** | **0** | ✓ disjoint-by-construction |
| Σ(shard sizes) == total | 46,464 == 46,464 | ✓ |
| Per-shard split (ideal 484) | min 430 / max 543 (±12.2% on small buckets) | granular skew, see below |
| **Account-range aggregate** (32 shards each) | 15,494 / 15,480 / 15,490 = 33.35 / 33.32 / 33.34% | ✓ **even within 0.05%** |
| Ownership sim (3 accounts, **variable** 4/7/3 sessions = 14 total) | 96/96 shards assigned, **0 double-owned**, all 46,464 ids covered exactly once | ✓ |

The per-shard ±12% skew is the expected variance of 484-row buckets and is **irrelevant to load balance** — work is assigned at the account-range grain, which is even to 0.05%, and sessions pull additional shards as they free up, so a session that drew a heavy shard simply takes one fewer next. **Disjointness holds at the id grain regardless of how many sessions any account runs** (the round-robin `[sess::n]` slicing of a shard range never assigns a shard twice).

### Waves WITHIN a shard

Each shard is processed in **ordered waves of N files**, committing each wave's results to R2 **before** the next wave starts — durable, resumable checkpoints. Waves are **priority-ordered by award value**: `sb_gt_5m → sb_1m_5m → sb_500k_1m → sb_remainder`, so dollar-weighted signal lands first. Per the partition probe, every priority band is present in all 96 shards (`sb_gt_5m`: 8,501 files spread 63–118 per shard, avg 88.6), so **every session contributes top-band value early** — no shard is starved of high-value work.

**Recommended wave size: 50 files/wave** (matches the BIGTHREE/p2b empirical batch of ~10 docs/agent × ~5 concurrent = one paced group; the regex lane already flushes at `FLUSH_RESOURCES=200`, [`sam_labor_demand_extract_90day.py:108`](../../pipelines/sam_gov/sam_labor_demand_extract_90day.py)).

### The ledger's role — DONE-MARKER, not claim arbiter

`govcon_requirements_extract_ledger` (and the per-resource JSONL checkpoint) is a **per-resource done-marker** for two purposes only:
1. **Idempotent resume within a lane** — skip `resource_id`s already terminal. The regex lane resumes on `_ledger_done` = `regex_state ∈ {done,quarantined}` ∪ checkpoint ([`sam_labor_demand_extract_90day.py:988-996,1024-1027`](../../pipelines/sam_gov/sam_labor_demand_extract_90day.py)); the LLM lane skips any task whose result file already exists ([`bigthree_v2_grind_wave.js:23`](../../scripts/bigthree_v2_grind_wave.js): `If that result file ALREADY EXISTS (test -f), SKIP`).
2. **Progress/verification** — `llm_state ∈ {pending, claimed-via-shard-assignment, done, quarantined, excluded_marked, excluded_out_of_scope}`.

It is **never** consulted to decide *which* worker may touch a `resource_id` — the shard math already decided that, offline. **Idempotent landing** keeps a re-run of an abandoned shard safe: every sink write is **scoped delete-before-merge keyed on a content hash**, one row per logical key:
- requirements: `requirement_id = sha256(resource_id|requirement_type|value_norm)[:24]`, scoped delete `resource_id IN (…) AND extractor LIKE 'regex:%'` / `'llm:%'` then `merge_insert("requirement_id")` ([`sam_labor_demand_extract_90day.py:266-269,829-836,936-940,1237-1240,1988-1992`](../../pipelines/sam_gov/sam_labor_demand_extract_90day.py)).
- labor demand: `demand_id` deterministic rank, unscoped per-resource delete then `merge_insert("demand_id")`.
- a re-run of an abandoned shard re-derives identical keys → updates in place, **never double-appends**.

### Deviations from specific past waves (called out)

- **BIGTHREE_V2** used a **2-hex resource-id prefix** as the partition ([`bigthree_v2_grind_wave.js:3,18-19`](../../scripts/bigthree_v2_grind_wave.js)). That is a 256-bucket static partition and is fine, but prefix buckets are **uneven** (the backlog probe shows 2-hex prefixes ranging 134–218 over this set) and tie one agent to one prefix string. **This plan uses `abs(hash)%96`** instead: provably even at the account grain (0.05%), decoupled from id formatting, and sized to the actual concurrency. Same disjoint-by-construction guarantee, better balance.
- **p2b/subaward** used a "committer daemon" landing results asynchronously ([`p2b_extract_handoff_workflow.js:3`](../../pipelines/sam_gov/reference/p2b_extract_handoff_workflow.js)). This plan keeps the **deterministic `--phase ingest`** as the committer (run by the operator per wave/shard, [`sam_labor_demand_extract_90day.py:2017`](../../pipelines/sam_gov/sam_labor_demand_extract_90day.py)) rather than a long-lived daemon — simpler, and the gate (0.98) is enforced per ingest. No new infrastructure.
- Past waves were **not uniform** (BIGTHREE re-extraction vs subaward lift vs p2b cold start). The mechanisms that held up across all three and are adopted here: **(a)** session agents read self-contained task files, never the network/R2 ([all three grind scripts]); **(b)** result-exists skip for resume; **(c)** scoped delete-before-merge on content-hash keys; **(d)** the 0.98 ingest gate; **(e)** Decision-A CUI bracketing. The mechanism **changed** is the partition (hash-shard, not prefix) and the **explicit rejection of runtime claiming**.

---

## Phased plan

Every phase: objective · commands · ENTRY · EXIT (measurable) · idempotency · blast-radius · ledger row.

### P0 — Scope + ledger snapshot
- **Objective:** freeze the worklist and baseline metrics before any mutation.
- **Commands:** re-run `_plan_stage3_backlog_probe.py` + `_plan_static_partition_probe.py`; materialize `shard_NN.ids` file-lists from the pending SB set (priority-band column carried); record baseline serving counts via `materialize_award_scope_requirements.py --cmd verify`.
- **ENTRY:** none (read-only).
- **EXIT:** 96 shard file-lists written under `/tmp/stage3_shards/`; Σ(file-list rows)=46,464=pending SB; 0 ids in >1 file; baseline `has_insurance_bonding` count recorded.
- **Idempotency:** pure recompute; deterministic shard math → byte-identical file-lists on re-run.
- **Blast radius:** none (no R2 writes).
- **Ledger:** snapshot JSON to `docs/plans/` companion / `/tmp`; no SoR write.

### P1 — Stage-3 byte download backlog
- **Objective:** download the 12,419 SB download-pending files (priority-tiered), into the CAS blob tier + `sam_attachment_files` ledger.
- **Commands:** `sam_attachment_download_90day.py --daemon --resume`, **scoped to the priority worklist** via the manifest gate (`access_level='public' AND file_name IS NOT NULL AND size_bytes>=1`, [`sam_attachment_download_90day.py:73`](../../pipelines/sam_gov/sam_attachment_download_90day.py)). Run SB>$5M → >$1M → >$500K → remainder by ordering the worklist.
- **ENTRY:** P0 done; residential-IP path confirmed.
- **EXIT:** `sam_attachment_files(status='downloaded')` rises by ≈12,419 for the SB set (re-probe `download_pending_files → ~0`); WAF blocks 429/403 within envelope; `oversize`/`gone`/`restricted` ledgered (not silent).
- **Idempotency:** `_load_done` = Lance ledger ∪ JSONL checkpoint; `resume` skips `downloaded/restricted/gone` ([`sam_attachment_download_90day.py:405-422,481-483`](../../pipelines/sam_gov/sam_attachment_download_90day.py)). CAS key = `resource_id` (re-PUT is idempotent).
- **Blast radius:** isolated blob prefix + ledger; circuit breaker trips on WAF cluster and drains ([`Breaker`, :221-251,575-578`](../../pipelines/sam_gov/sam_attachment_download_90day.py)) — protects the residential IP.
- **Ledger:** `ops.sam_attachment_download_runs` (one row per batch, [`OPS_DDL` :88-98`](../../pipelines/sam_gov/sam_attachment_download_90day.py)).

### P2 — Text-extract + chunk
- **Objective:** parse downloaded bytes → 1,200/180-char chunks into `govcon_scope_vectors` / `govcon_pricing` / `govcon_unknown`; route SF-boilerplate to drop, image-only to `requires_ocr`.
- **Commands:** `sam_attachment_extract_90day.py --phase route` → `--phase expand` → `--phase extract --lane L1_scope/L4_structured/L3_triage --daemon --resume` ([header :36-46`](../../pipelines/sam_gov/sam_attachment_extract_90day.py)).
- **ENTRY:** P1 EXIT met for the priority bands.
- **EXIT:** every newly-downloaded SB `resource_id` reaches a terminal extraction verdict (chunked, dropped, or `requires_ocr`); chunk sinks grow; 0 resources stuck mid-route.
- **Idempotency:** chunk `merge_insert("chunk_id")`; per-result checkpoint written only after chunks+ledger durable ([extract header §7.6/C11](../../pipelines/sam_gov/sam_attachment_extract_90day.py)). Caps `MAX_CHUNKS_PER_FILE=4000`, `MAX_EXTRACT_CHARS=4M` ([:100-101`](../../pipelines/sam_gov/sam_attachment_extract_90day.py)).
- **Blast radius:** read-only on `sam_attachment_files`; new sinks only; one committing process per dataset (D3).
- **Ledger:** `sam_attachment_extraction` append-only event ledger (resolution view = latest terminal).

### P3a — Regex lane (deterministic, free, lands bonding immediately)
- **Objective:** run the versioned pattern library over all newly-chunked resources — `bonding_insurance`, `staffing`, `labor_category`, `clearance`, `standard`, `set_aside`, etc. **This lane alone lands the surety/bonding signal at confidence 1.0 for free** (the `_h_bond` / `_h_insurance` handlers, [`sam_labor_demand_extract_90day.py:337-347,550-559`](../../pipelines/sam_gov/sam_labor_demand_extract_90day.py)).
- **Commands:** `--phase extract --resume --daemon` then `--phase index` after merges settle.
- **ENTRY:** P2 done.
- **EXIT:** `govcon_award_requirements` rows for the new resources; re-probe SB `insurance_bonding` file count rises toward the 17.5% forecast; 100% `validated=true`.
- **Idempotency:** scoped delete `resource_id IN (…) AND extractor LIKE 'regex:%'` then `merge_insert("requirement_id")`; failed resources ledgered `regex_state='failed'` without deleting prior good rows ([:35-50,921-965`](../../pipelines/sam_gov/sam_labor_demand_extract_90day.py)). LLM-lane columns preserved on ledger merge ([:848-881`](../../pipelines/sam_gov/sam_labor_demand_extract_90day.py)).
- **Blast radius:** regex lane scoped — never touches `llm:%` rows. Redaction-at-write for `content_marking` resources.
- **Ledger:** `govcon_requirements_extract_ledger.regex_state`.

### P3b — LLM grind (multi-account, static shards, priority waves)
- **Objective:** recover free-form `labor_category` + the requirement types regex misses, on the **static-shard / priority-wave** model above. Zero marginal cash (`session-opus` handoff).
- **Commands (per account, per shard):**
  - Operator (once): `--phase bracket` (Decision A: CUI `content_marking` → `excluded_marked`; CUI gate requires marking `reconcile_overall=PASS`, [:1416-1478`](../../pipelines/sam_gov/sam_labor_demand_extract_90day.py)); `--phase census` (zero-spend token count); `--phase select --resource-ids-file shard_NN.ids --engine session-opus --token-budget 32000 --staging-dir /tmp/stage3_stage/shard_NN` to stage task files.
  - Per account: a grind harness modeled on [`bigthree_v2_grind_wave.js`](../../scripts/bigthree_v2_grind_wave.js) but keyed on **assigned shard file-lists in the account's range** (`CONC=5`, `model:'opus'`, result-exists skip). Variable sessions per account are safe by construction.
- **ENTRY:** P3a done (regex residue informs the unknown-sink input set, [`compute_input_set` :1330-1337`](../../pipelines/sam_gov/sam_labor_demand_extract_90day.py)); marking report PASS.
- **EXIT:** every staged shard task has a result file; per-shard waves committed in priority order.
- **Idempotency:** task staging is deterministic; **result-exists skip** = resume; landing is P4's job (gated). A re-staged abandoned shard overwrites task files identically (same `prompt_hash`).
- **Blast radius:** agents read **only** their task files — no R2/network/dataset access ([grind NOTE](../../scripts/bigthree_v2_grind_wave.js)). Marked text never staged (hard assert, [`build_task_payload` :1579-1584`](../../pipelines/sam_gov/sam_labor_demand_extract_90day.py)).
- **Ledger:** `llm_state` pending→done per resource on ingest.

### P4 — Ingest + validate
- **Objective:** validate result JSON against staged tasks, enforce the **≥0.98 run gate**, land passing rows via scoped delete-before-merge.
- **Commands:** `--phase ingest --staging-dir /tmp/stage3_stage/shard_NN [--engine session-opus] [GOVCON_LLM_FREEFORM_LABOR=1]` per shard/wave.
- **ENTRY:** P3b results present for the shard.
- **EXIT:** `run_pass_rate ≥ 0.98` (else **nothing lands** — resources stay pending, [:2099-2105`](../../pipelines/sam_gov/sam_labor_demand_extract_90day.py)); landed rows carry `extractor='llm:session-opus@v2-freeform-labor'`, confidence <1.0, verbatim `evidence_quote` citation-checked.
- **Idempotency:** scoped delete `extractor LIKE 'llm:%'` then `merge_insert("requirement_id")` + doc-scope `merge_insert("resource_id")` ([`_land_llm_batch` :1976-2014`](../../pipelines/sam_gov/sam_labor_demand_extract_90day.py)); quarantined docs leave prior rows untouched. Re-ingest is safe (BIGTHREE/P2b smoke-confirmed).
- **Blast radius:** `prompt_hash` mismatch hard-errors unless `--allow-prompt-hash` ([:2038-2041`](../../pipelines/sam_gov/sam_labor_demand_extract_90day.py)) — prevents silent prompt-version mixing.
- **Ledger:** `llm_state='done'` + `validation_pass_rate` + `prompt_hash` per resource.

### P5 — Rematerialize serving MVs
- **Objective:** turn the new `govcon_award_requirements` rows into the GTM-facing arrays.
- **Commands:** `materialize_award_scope_requirements.py --cmd build` then `materialize_active_award_labor_demand.py` (build).
- **ENTRY:** P4 landed; **note** the spine is `govcon_award_solicitation_profiles.source_resource_ids` (snapshot-frozen, [`materialize_award_scope_requirements.py:13-18`](../../pipelines/serving/materialize_award_scope_requirements.py)) — the award_solicitation profile must be rebuilt first if new awards must enter the spine, else only requirements for already-profiled awards refresh.
- **EXIT:** `govcon_award_scope_requirements.has_insurance_bonding`/`insurance_bonding_values` populated for the new SB awards; `--cmd verify` deltas positive.
- **Idempotency:** snapshot-overwrite by content; BTREE/BITMAP rebuilt ([:251-266`](../../pipelines/serving/materialize_award_scope_requirements.py)).
- **Blast radius:** derived MV overwrite; additive LEFT-join to `govcon_active_awards` (absence = UNKNOWN, never a gate).
- **Ledger:** `ops.award_scope_requirements_serving_runs`.

### P6 — Verify coverage + surety-signal uplift
- **Objective:** prove the wave moved the GTM numbers.
- **Commands:** `_plan_stage3_backlog_probe.py` (extract-pending → down); a verify probe modeled on [`bigthree_v2_verify.py`](../../scripts/bigthree_v2_verify.py) for landed-row counts; `materialize_award_scope_requirements.py --cmd verify` for `has_insurance_bonding` before/after.
- **EXIT thresholds (below).**

---

## Wave sizing & cost

**Token model** (the pipeline's own `chars/4`, [`estimate_tokens` :1243-1245`](../../pipelines/sam_gov/sam_labor_demand_extract_90day.py)) + measured BIGTHREE_V2 throughput:

| Lever | Source | Value |
|---|---|---|
| Per-doc selected payload | BIGTHREE_V2: 799 docs / 2.68M selected tok | ~3,350 tok/doc |
| Fixed prompt/vocab/schema overhead | BIGTHREE sizing | 3,036 tok/doc (re-paid per task) |
| **Total input per doc** | measured | **~6,580 tok** |
| Output per doc (free-form labor) | estimated | ~1,250 tok |
| Wall-clock | BIGTHREE_V2 run record | 799 docs / CONC=5 / 6 waves ≈ **3.5h**, gate 0.9959, 0 rate-limit failures |
| Marginal cash | `session-opus` Max subscription | **~$0** |

**Recommended wave: 50 files/wave/session** (one paced CONC=5 group × ~10/agent). **Per-shard ≈ 484 files ≈ 10 waves.** With 3 accounts × ~10 sessions = 30 sessions, ~3 shards per session.

**Waves to clear the SB>$500K slice** (the priority money cohort): not all 14,345 extract-pending files stage to the LLM — only the Decision-A input set (scope-ALL ∪ pricing-ALL ∪ lexicon-hit unknown) minus CUI-marked, then token-budgeted. By BIGTHREE proportions (~58% of chunk-bearing files stage), expect **≈8,000 staged SB>$500K docs ≈ 160 waves of 50**, parallelized across 30 sessions = **~6 paced session-rounds** (each session ~5–6 waves). At ~3.5h per ~800-doc round, **≈1 paced day of wall-clock** to clear SB>$500K, single-digit dollars cash.

**Full SB slice** (34,045 extract-pending → ~20K staged): **~400 waves**, **≈25–35M input tokens**, **≈$250–350 economic API-equivalent / ≈$0 cash**, **~3–5 paced days** across all accounts. The regex lane (P3a) carries the deterministic bonding/labor majority *before* any LLM spend, so the LLM lane is pure recall uplift, not the critical path for the surety co-product.

---

## Verification & acceptance (per-wave probes + thresholds)

After each wave/shard ingest and again at P6:

| Metric | Probe | Target |
|---|---|---|
| SB extract-pending files | `_plan_stage3_backlog_probe.py` → `small_business.extract_pending_files` | monotonically ↓; ≈0 at full clear |
| SB awards fully extract-pending | same → `awards_fully_extract_pending` | 10,102 → ↓ toward 0 |
| **SB files with `insurance_bonding`** | `req WHERE requirement_type='insurance_bonding'` over SB extracted set | rises toward **17.5% × extracted** (forecast +≈5,900 files) |
| SB awards with populated `insurance_bonding` (serving) | `materialize_award_scope_requirements.py --cmd verify` → `has_insurance_bonding` | **+1,000–1,800** vs P0 baseline |
| LLM run pass-rate | ingest report `run_pass_rate` | **≥ 0.98** or nothing lands |
| Partition integrity | `_plan_static_partition_probe.py` | ids-in->1-shard = **0**; account aggregate within **±1%** |
| coverage_of_gettable (award grain) | substrate probe `link_covered → extracted_reach` | extracted_reach / gettable ↑ |

---

## Risks & mitigations

| Risk | Mitigation |
|---|---|
| **WAF / residential-IP pacing** (P1) | Proven envelope conc=6 @ ~8 req/s global token bucket; circuit breaker drains on 429/403 cluster ([`RateLimiter`/`Breaker`](../../pipelines/sam_gov/sam_attachment_download_90day.py)). Run download off-peak; tier by value so the scarce top band completes even if the breaker trips mid-run. |
| **Token budget** (P3b) | Census (`--phase census`, zero spend) gates every shard before staging. Default 8K/doc budget; raise to 32K only for SB>$1M tail (BIGTHREE precedent) — still sub-$400 total. |
| **OCR-deferred scanned PDFs** (`requires_ocr`) | These yield zero text and are out of scope for this wave — count them, don't retry. They are a recall floor, not a failure (subaward R8). A future OCR stage is the only way to recover them; not built here. |
| **`equipment_capability` noise** | Surface, don't headline (distribution is heterogeneous). Do not promise a clean array to GTM. |
| **No-Sol# floor** | Awards with no FPDS sol AND no PIID recovery AND no winners award-key are **un-bridgeable** and out of scope. Stated, not pursued. |
| **`prompt_hash` drift** | Ingest hard-errors on mismatch unless `--allow-prompt-hash`; rows record the *staged* hash (true provenance, [:2038-2058`](../../pipelines/sam_gov/sam_labor_demand_extract_90day.py)). Never mix versions silently. |
| **Append-only safety / double-work** | The whole point of the static partition: disjoint-by-construction at assignment (probe-proven 0 double-owned). Landing is scoped delete-before-merge on content-hash keys → re-run of an abandoned shard updates in place, never double-appends. |
| **Serving spine freshness** (P5) | `govcon_award_scope_requirements` spine is snapshot-frozen to `govcon_award_solicitation_profiles`. Rebuild the profile first if new awards must enter the spine; otherwise only already-profiled awards refresh. The BIGTHREE_V2 record flags this exact gap (DISTINCT cap-25 alpha rollup is lossy on high-cardinality free-form labor) — a serving-design decision to confirm before the P5 refresh. |
| **Session/account variability** | No repartition needed: `N=96 ≫` any concurrency; sessions pull shards as they free up; account ranges are fixed and disjoint. Adding a 4th account = reassign a sub-range, zero re-hash. |

### Minimal additions required (capabilities not already in the repo)

1. **A shard file-list materializer** (`_plan`-style script → `shard_NN.ids` from the pending SB set with the priority-band column). Trivial; the partition SQL already exists in `_plan_static_partition_probe.py`.
2. **A per-account grind harness** = a thin edit of [`bigthree_v2_grind_wave.js`](../../scripts/bigthree_v2_grind_wave.js) swapping the 2-hex-prefix arg for an assigned-shard-file-list arg scoped to the account's range. No new framework.

Everything else — download, extract, regex lane, LLM bracket/select/census/ingest/reset, serving rematerialization, the ledgers — already exists and is cited above.
