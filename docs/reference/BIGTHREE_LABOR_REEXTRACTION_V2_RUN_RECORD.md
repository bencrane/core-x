# Big-Three labor re-extraction — v2 free-form run record (Scenario B)

**Mode:** EXECUTED production run (LLM re-extraction → SoR land). **Completed:** 2026-06-21 (UTC).
**Objective:** maximize IT-labor recall for the active Big-Three cohort `psc_code ∈ (DA01,R425,R499)` by (1) removing the controlled-vocabulary constraint on `labor_category` (free-form raw job titles) and (2) uncapping the per-doc token budget 8k→32k. Sized in [`BIGTHREE_LABOR_REEXTRACTION_SIZING.md`](BIGTHREE_LABOR_REEXTRACTION_SIZING.md).

## Outcome — LANDED, gate passed

| | |
|---|---:|
| Result pass-rate (gate ≥ 0.98) | **0.9959** ✓ |
| Docs: pass / partial / quarantined / missing | 792 / 7 / 0 / 0 |
| **Rows landed** (`govcon_award_requirements`, `llm:session-opus@v2-freeform-labor`) | **4,171** (17 rejected, 0.4%) |
| Distinct cohort resources with v2 rows | 546 |
| doc-scope rows (`govcon_doc_scope`) | 799 |
| **labor_category rows / distinct titles** | **2,564 / 1,498** |
| Distinct labor titles OUT-OF-VOCAB (impossible under v1) | **1,496 (99.9%)** |
| Distinct IT/engineering/technical titles | **770** |

Landed types: labor_category 2,564 · deliverable 768 · standard_compliance 329 · certification 123 · clearance 111 · past_performance 74 · vehicle_constraint 63 · staffing_constraint 45 · equipment_capability 43 · license 30 · insurance_bonding 21.

Sample free-form IT/eng titles now in the SoR (lowercased+ws-collapsed on store; verbatim case in `evidence_quote`): *software engineer (senior), systems administrator (senior), systems engineer 1/2/3, software test engineer (senior), human factors engineer (expert), stress analysis engineer, flight operations engineer, chief engineer, cloud system architect, lead cloud computer scientist/software engineer*.

## Config (locked — PR [#586](https://github.com/bencrane/core-x/pull/586), `baa636f`)
- **`reference/govcon_llm_lane_v2/`** — prompt rule 3 = extract EXACT raw job title; do NOT map to the 36-token vocab, do NOT skip non-members. v1 artifacts untouched (provenance for the 53,746 v1 rows preserved). `prompt_hash f3567fc8`.
- **`LLM_PROMPT_VERSION = v2-freeform-labor`**, `LLM_ARTIFACT_DIR → govcon_llm_lane_v2`, extractor tag `llm:<engine>@v2-freeform-labor`.
- **`GOVCON_LLM_FREEFORM_LABOR=1`** — validator accepts out-of-vocab labor values as raw titles; **verbatim `evidence_quote` citation check still enforced** on every labor row (17 rejects were quote/enum failures, not vocab).
- **Budget 32k** via `--token-budget 32000` (per-run; no global env edit) — truncation **122 docs → 16 docs**.

## Execution (phase-by-phase)
1. **reset-llm** `--resource-ids-file` (1,359 chunk-bearing cohort) — re-pended, cleared prior v1 `llm:%` rows. Regex lane untouched.
2. **bracket** — whole-corpus re-partition; **197 CUI/`content_marking` docs → `excluded_marked`** (egress-blocked, never staged); out-of-scope → excluded. Marking-report gate `reconcile_overall=PASS`.
3. **census** (32k, cohort-scoped, zero spend) — **799 staged docs**, 3.93M selected tokens, 16 docs truncated. Verify gate before spend.
4. **select** `--engine session-opus --token-budget 32000` → 799 task files (`/tmp/govcon_llm_stage_v2`); zero `content_marking` leakage verified.
5. **agent lane** — session-opus, `model:'opus'`, **CONC=5**, 6 paced prefix-partitioned waves (each agent owns a disjoint 2-hex resource-id prefix; resumable via result-exists skip). 799/799 results, 0 unparseable, **no rate-limit failures**. ~22M agent tokens, ~3.5h wall across waves.
6. **ingest** `GOVCON_LLM_FREEFORM_LABOR=1` — validated 799 results, gate 0.9959 ≥ 0.98, scoped delete-before-merge land under sink leases; ledger → done.

## Reproducibility
- Harness: [`scripts/bigthree_v2_grind_wave.js`](../../scripts/bigthree_v2_grind_wave.js) (one paced wave; `args = {prefixes,tasksDir,resultsDir,conc}`), [`scripts/bigthree_reextract_preflight.py`](../../scripts/bigthree_reextract_preflight.py) (allow-list + ledger state), [`scripts/bigthree_v2_verify.py`](../../scripts/bigthree_v2_verify.py) (SoR verify).
- Ingest report: `/tmp/govcon_llm_ingest_report.json`. Staging: `/tmp/govcon_llm_stage_v2/{tasks,results}`.

## Downstream — NOT auto-run (operator decision)
The v2 rows are landed in `govcon_award_requirements`. The serving rollup `govcon_award_scope_requirements.labor_category_values` ([`materialize_award_scope_requirements.py`](../../pipelines/serving/materialize_award_scope_requirements.py)) is a **materialized snapshot** and must be re-run to surface the new labor in serving/GTM. **Deliberately deferred**: that rollup does `DISTINCT requirement_value … alpha-sorted, cap-25, no extractor filter` — applying it to 1,498 free-form titles changes the field's character (controlled-vocab → free-form, cap-25 alpha truncation is lossier on high-cardinality values). Refresh strategy (re-materialize as-is vs. add an extractor-aware / dedup-normalized labor surface) is a serving-design call to make before refreshing.
