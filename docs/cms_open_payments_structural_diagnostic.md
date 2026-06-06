# CMS Open Payments — Lance Structural Diagnostic

**Targets (Gen-3 SoR, R2 bucket `data-sink`):**
- `s3://data-sink/active/cms_general_payments/` — General Payments
- `s3://data-sink/active/cms_research_payments/` — Research Payments
- `s3://data-sink/active/cms_ownership/` — Ownership Payments

**Mode:** Read-only, first-principles. Zero DDL / zero index ops / zero dataset writes. Assessed independent of all downstream consumers.
**Date:** 2026-06-06 · **Vintage:** program years 2018–2024 (CMS publication date `2026-01-23`).
**Method:** `pylance 7.x` manifest / fragment / index introspection (`count_rows`, `get_fragments`, `physical_rows`, `list_indices`, `stats.index_stats`) + R2 `ListObjectsV2` byte census (boto3) + DuckDB `1.5.x` streaming aggregates over the live R2 datasets (null density **exact**; NDV via HyperLogLog `approx_count_distinct`, marked *exact* where computed exactly) + Lance physical-plan pushdown probe. Cross-checked against `ops.cms_open_payments_runs` (Postgres). **All three datasets were read live from R2; none was sampled.** Telemetry harness: a throwaway read-only `modal run` function mirroring the worker image — opened R2, computed, wrote nothing back.

> **This document supersedes the prior diagnostic of the same name** (PR #210, commit `6c05379`), which characterized `cms_general_payments` as a destroyed partial-publish corpse profiled from data-file footers + a 1.8M-row orphaned-fragment sample. **That premise is dead:** general was re-ingested from CMS and is now a healthy, fully-indexed 82.29M-row dataset (verified below). Every number here is measured against the **current** live state. The teardown of the prior *remediation plan* built on that obsolete diagnostic is embedded as the Appendix, so future readers see why it was retired.

---

## 1. Headline Posture

**All three datasets are STRUCTURALLY HEALTHY and query-ready. Zero tombstones, zero fragmentation debt, 100% index coverage, correct storage format. The dataset that was a corpse is fully restored. The remaining findings are logical-hygiene and compute-envelope-headroom items — none is a storage problem, none blocks a query today.**

- **`cms_general_payments` — RESTORED, HEALTHY (was P0-broken; no longer).** 82,290,893 rows · 95 columns · **83 data fragments, 0 tombstoned** · v2.1 · **version 17** · **10/10 scalar indices, each covering all 82,290,893 rows (0 unindexed)**. R2 holds a complete, consistent object set (83 `data/`, 10 `_indices/` dirs, 18 `_versions/` manifests, 17 `_transactions/`) — no orphaned fragments, **no `__staging` debris**. The ledger corroborates: run #101 `refresh_all` = `success`; `publish` (id 99) and `verify` (id 100) both `success` at 82,290,893 rows. The hardened staging-swap publish (PR #213) + read-back verify gate (PR #224) did exactly what they were built to do. **Restored on the `/tmp`-staged from-scratch rebuild** (the Modal-Volume staging path was tried this session and rejected the Lance commit rename with `EPERM` — ledger ids 76/77/89/90 carry the exact `os error 1` — so the worker stages giants on the `/tmp` overlay; see §4 and the Appendix).
- **`cms_research_payments` — physically excellent, logically dirty.** 5,936,454 rows · 256 columns · 8 fragments, 0 tombstoned · v2.1 · v17 · **10/10 indices cover all rows**; index storage a lean **5.1%** of data (174.9 MiB / 3.34 GiB). Logical issues persist (they are source-data properties, not publish defects): the declared key `covered_recipient_npi` is **96.39% NULL** (the populated resolution key is `principal_investigator_1_npi`, 4.27% null); **~15 columns 100% NULL** + dozens ≥98% NULL (empty PI 2–5 repeating groups push the column count to 256); `date_of_payment` carries an impossible **floor `0002-11-30`**; `recipient_state` un-normalized (64 NDV); `program_year` is a strict subset of `payment_year` (5 vs 7 distinct).
- **`cms_ownership` — physically healthy, trivially small.** 27,480 rows · 34 columns · 7 fragments, 0 tombstoned · v2.1 · v15 · **8/8 indices cover all rows**. Index plan optimal (correctly omits `date_of_payment` — an ownership interest is not a dated payment). Minor: `recipient_province` / `recipient_postal_code` 99.99% null (foreign-address columns); same constant provenance columns as the others. Nothing structurally actionable — at this scale indices and layout are free.
- **ROOT-CAUSE STATUS (the cross-cutting finding of the prior diagnostic): RESOLVED.** The destructive publish primitive `_replace_r2_prefix` (wipe-prefix-then-reupload, no retry, no atomic swap) that destroyed general is **gone from this worker.** It was replaced (PR #213, commit `3858f07`) by two non-destructive primitives — `_publish_full_swap` (stage to a `__staging` sibling → verify object set + per-file sizes == local → swap with manifest-last ordering) and `_publish_incremental` (append-only, upload only new/changed files, never wipe) — and gated by `_verify_published` (reopen fresh from R2, assert rows + full index plan + a BTREE point-probe before "success" is recorded, PR #224). The corpse-producing failure mode is structurally eliminated for this feed.

**Verdict:** all three are query-ready *today* for NPI / `record_id` / manufacturer-id / state / year resolution via indexed pushdown. The schema across all three still carries dead-column and constant ballast, but because Lance v2.1 RLE/dictionary-compresses constants and nulls to near-zero on disk (measured compression: research **2.99×**, ownership **5.19×**, general **~4.0×**), this is a **logical-hygiene and scan-width** concern, **not** a storage problem. Do not frame the remaining schema work as disk reclamation. There is **no compaction debt** and **no mandated structural rewrite** — the optimization blueprint (separate plan) is entirely optional polish.

---

## 2. Telemetry Grid

All figures measured live 2026-06-06. Bytes are exact from the R2 `ListObjectsV2` census; rows/fragments/indices from pylance; null density exact; NDV is HLL unless marked *exact*.

| Metric | general | research | ownership |
|---|---:|---:|---:|
| **Structural status** | **HEALTHY (restored)** | healthy | healthy |
| Logical rows (readable) | **82,290,893** | **5,936,454** | **27,480** |
| Rows intended (ledger ingest) | 82,290,893 | 5,936,454 | 27,480 |
| **Rows lost in publish** | **0** | 0 | 0 |
| Deleted / tombstoned | **0 (0.000%)** | **0 (0.000%)** | **0 (0.000%)** |
| Columns | 95 | 256 | 34 |
| Data fragments | **83** | **8** | **7** |
| Rows/frag — min · avg · max | 17,952 · 991,457 · 1,048,576 | 31,223 · 742,057 · 1,048,576 | 3,046 · 3,926 · 4,591 |
| Frags at 1,048,576 cap | **76 / 83** | 1 / 8 | 0 / 7 |
| `data/` on disk (R2, compressed) | **19,033,785,897 B · 17.727 GiB** | **3,583,399,689 B · 3.337 GiB** | 3,641,409 B · 3.47 MiB |
| Scalar-index storage (R2) | **2,420,216,523 B · 2,308.1 MiB** (10 idx dirs, 14 obj) | **183,437,887 B · 174.9 MiB** (10 idx, 15 obj) | 886,066 B · 0.85 MiB (8 idx, 11 obj) |
| Index : data ratio | **0.127× (12.7%)** | **0.051× (5.1%)** | 0.243× (free at this scale) |
| Manifests (`_versions/`) / transactions | 18 / 17 | 18 / 17 | 16 / 15 |
| `__staging` debris present | **0 (clean)** | 0 | 0 |
| Total R2 footprint | **19.981 GiB (132 obj)** | **3.509 GiB (58 obj)** | 4.37 MiB (49 obj) |
| Storage format version | **2.1** | **2.1** | **2.1** |
| Dataset version | **17** (7 year-writes + 10 index commits) | **17** (7 + 10) | **15** (7 + 8) |
| On-disk compression (decoded ÷ data) | **~4.0×** (est.) | **2.99×** (measured, prior) | **5.19×** (measured, prior) |

**Footprint class split** (authoritative per-class bytes from the R2 listing; the `_indices/*.lance` files are counted as indices, not data):

| | general | research | ownership |
|---|---:|---:|---:|
| `data/` | 83 obj · 17.727 GiB | 8 obj · 3.337 GiB | 7 obj · 3.47 MiB |
| `_indices/` | 14 obj · 2,308.1 MiB | 15 obj · 174.9 MiB | 11 obj · 0.85 MiB |
| `_versions/` | 18 obj · 0.44 MiB | 18 obj · 0.40 MiB | 16 obj · 0.05 MiB |
| `_transactions/` | 17 obj · 0.03 MiB | 17 obj · 0.03 MiB | 15 obj · 0.005 MiB |

**Version-encoding note (so "version 17" is unambiguous):** Lance writes manifests under `_versions/<u64::MAX − version>.manifest`. General's manifest set runs `…598.manifest` … `…614.manifest` (+ `latest_version_hint.json`) = `MAX−17` … `MAX−1` → **versions 1–17 present, highest = 17**. 7 year-appends (create + 6 appends) then 10 sequential `create_scalar_index` commits = 17. Research identical (17); ownership 7 + 8 = 15. The version counters confirm a clean from-scratch rebuild lineage, not an append onto a salvaged corpse.

**Fragment topology (read-amplification).** All three follow an **append-per-program-year** topology: roughly one logical year per write, split into 1,048,576-row files at the `max_rows_per_file` cap. General's 82.29M rows → 76 full-cap fragments + 7 tail fragments (the per-year remainders: 723,652 / 704,983 / 607,185 / 452,212 / 72,411 / 20,722 / 17,952) = 83. Research 8 (2023 = 1,079,799 rows → 1,048,576 + 31,223). Ownership 7 (one small fragment per year, none near cap). **Fragments are large and few relative to row count → no read-amplification, and with 0 tombstones across all three there is zero compaction debt.** This is not a fragmentation problem anywhere.

---

## 3. Schema & Index Ledger

NDV = HLL estimate unless *exact*. "Optimal" assessed purely on cardinality + structural role, independent of any consumer. Only **load-bearing** columns (resolution/join keys, indexed categoricals, money/temporal) and **defective** columns are enumerated; dead/constant columns are rolled up per family. **Every existing index covers 100% of rows on all three datasets** (`num_unindexed_rows = 0` everywhere — measured via `stats.index_stats`).

### 3A. `cms_general_payments` (95 cols; null exact + NDV HLL over all 82,290,893 rows — RESTORED)

| Column | Type | Null % | NDV | Existing | Optimal | Verdict |
|---|---|---:|---:|:--|:--|:--|
| `covered_recipient_npi` | string | **0.40%** | ~1,682,185 | **BTREE** | BTREE | ✅ correct, well-populated key (contrast research's 96% null) |
| `record_id` | string | 0.00% | ~83.2M *(unique PK; HLL overest)* | **BTREE** | BTREE | ✅ PK · numeric surrogate (`int64` candidate; see fork) |
| `applicable_manufacturer_or_applicable_gpo_making_payment_id` | string | 0.00% | **2,648** | **BTREE** | BTREE (join) | ✅ join key · medium-card (BITMAP also defensible) |
| `date_of_payment` | date32 | 0.00% | 2,835 | **BTREE** | BTREE | ✅ temporal · ❌ **dirt floor `min = 0002-11-30`** (impossible; OP starts 2013) |
| `payment_year` | int16 | 0.00% | **7** *exact* | **BITMAP** | BITMAP | ✅ matched (typed authoritative partition key) |
| `covered_recipient_type` | string | 0.00% | **3** | **BITMAP** | BITMAP | ✅ matched |
| `nature_of_payment_or_transfer_of_value` | string | 0.00% | **15** | **BITMAP** | BITMAP | ✅ matched |
| `form_of_payment_or_transfer_of_value` | string | 0.00% | **6** | **BITMAP** | BITMAP | ✅ matched |
| `recipient_state` | string | 0.0042% | **69** | **BITMAP** | BITMAP | ✅ type-matched · ⚠ un-normalized (>51 USPS) |
| `dispute_status_for_publication` | string | 0.00% | **2** | **BITMAP** | BITMAP | ✅ matched |
| `total_amount_of_payment_usdollars` | decimal128(14,2) | 0.00% | ~267,868 | — | none | ✅ exact money type (no change) |
| `physician_ownership_indicator` | string | 8.79% | 2 | — | (◻ BITMAP candidate) | low-card flag — not load-bearing; index only if a consumer filters it |
| `related_product_indicator` | string | 0.00% | 2 | — | (◻ BITMAP candidate) | low-card flag — optional parity |
| `associated_device_or_medical_supply_pdi_{1,2,…}` | string | 57–65%+ | thousands | — | none | ⚠ free-form device IDs — see sentinel probe §3D (NOT a `nullif('')` target) |
| `program_year` | string | 0.00% | **5** | — | none | ❌ redundant **and narrower** than `payment_year` (5 ⊂ 7) — never index/key on it |
| `payment_publication_date` | date32 | 0.00% | **1** (`2026-01-23`) | — | none | ❌ constant per vintage (→ metadata, unless vintage key — see fork) |
| `delay_in_publication_indicator` | string | 0.00% | **1** (`No`) | — | none | ❌ constant |
| `covered_recipient_primary_type_{3..6}`, `_specialty_{3..6}` | string | 99.99–100% | 0–5 | — | none | ❌ effectively/fully null (e.g. `_specialty_6` = 100% null, 0 NDV) |

Committed index plan (registry, verified live): **BTREE** ×4 = `covered_recipient_npi`, `applicable_manufacturer_…_id`, `date_of_payment`, `record_id`; **BITMAP** ×6 = `payment_year`, `covered_recipient_type`, `nature_of_payment_…`, `form_of_payment_…`, `recipient_state`, `dispute_status_for_publication`. **10/10, all covering 82,290,893 rows. No mismatches** (no BTREE-on-tiny-categorical, no BITMAP-on-high-card).

### 3B. `cms_research_payments` (256 cols; null exact, NDV HLL over all 5,936,454 rows)

| Column | Type | Null % | NDV | Existing | Optimal | Verdict |
|---|---|---:|---:|:--|:--|:--|
| `principal_investigator_1_npi` | string | **4.27%** | ~70,323 | **BTREE** | BTREE | ✅ **the real resolution key for research** |
| `covered_recipient_npi` | string | **96.39%** | ~22,073 | **BTREE** | BTREE (keep) | ⚠ **96% null** — indexes mostly absence; reframe consumers, not a defect |
| `record_id` | string | 0.00% | ~7.10M *(unique PK; HLL overest)* | **BTREE** | BTREE | ✅ PK · numeric surrogate (`int64` candidate) |
| `applicable_manufacturer_…_making_payment_id` | string | 0.00% | **1,369** | **BTREE** | BTREE / ▵BITMAP | ✅ join key · medium-card |
| `date_of_payment` | date32 | 0.00% | 2,835 | **BTREE** | BTREE | ✅ · ❌ **dirt floor `min = 0002-11-30`** (impossible) |
| `payment_year` | int16 | 0.00% | **7** *exact* | **BITMAP** | BITMAP | ✅ matched |
| `covered_recipient_type` | string | 0.00% | **5** | **BITMAP** | BITMAP | ✅ matched |
| `related_product_indicator` | string | 0.00% | **2** | **BITMAP** | BITMAP | ✅ matched |
| `recipient_state` | string | 0.144% | **64** | **BITMAP** | BITMAP | ✅ · ⚠ un-normalized (>51) |
| `dispute_status_for_publication` | string | 0.00% | **2** | **BITMAP** | BITMAP | ✅ matched |
| `form_of_payment_or_transfer_of_value` | string | 0.00% | **5** | **—** | ▵BITMAP | ◻ candidate (general indexes it; add for parity — non-blocking) |
| `total_amount_of_payment_usdollars` | decimal128(14,2) | 0.00% | ~839,725 | — | none | ✅ exact money type |
| `program_year` | string | 0.00% | **5** | — | none | ❌ redundant + narrower than `payment_year` (5 ⊂ 7) |
| `payment_publication_date` | date32 | 0.00% | **1** | — | none | ❌ constant per vintage |
| `delay_in_publication_indicator` | string | 0.00% | **1** | — | none | ❌ constant |
| `covered_recipient_primary_type_{2..6}` / `_specialty_{2..6}` / `principal_investigator_{1..5}_primary_type_{2..6}` / `_specialty_{2..6}` (≈15 fully-null + dozens ≥98% null) | string | **100% / ≥98%** | 0 | — | none | ❌ empty PI 2–5 repeating groups (drive the 256 col count) |

**Index state:** 10/10 cover all 5,936,454 rows; **no mismatches**. Lean 5.1% index:data.

### 3C. `cms_ownership` (34 cols; null exact, NDV HLL over all 27,480 rows)

| Column | Type | Null % | NDV | Existing | Optimal | Verdict |
|---|---|---:|---:|:--|:--|:--|
| `physician_npi` | string | 0.073% | ~5,515 | **BTREE** | BTREE | ✅ correct key |
| `record_id` | string | 0.00% | ~32,659 *(unique PK; HLL overest)* | **BTREE** | BTREE | ✅ PK |
| `applicable_manufacturer_…_making_payment_id` | string | 0.00% | **455** | **BTREE** | BTREE / ▵BITMAP | ✅ low-card join (BITMAP viable) |
| `payment_year` | int16 | 0.00% | **7** *exact* | **BITMAP** | BITMAP | ✅ matched |
| `physician_primary_type` | string | 0.00% | **6** | **BITMAP** | BITMAP | ✅ matched |
| `recipient_state` | string | 0.011% | **60** | **BITMAP** | BITMAP | ✅ · ⚠ un-normalized |
| `dispute_status_for_publication` | string | 0.00% | **2** | **BITMAP** | BITMAP | ✅ matched |
| `interest_held_by_physician_or_an_immediate_family_member` | string | 0.00% | **2** | **BITMAP** | BITMAP | ✅ matched |
| `total_amount_invested_usdollars` / `value_of_interest` | decimal128(14,2) | 0.00% | 3,658 / 8,311 | — | none | ✅ exact · identical `0.00–344,292,301.95` range (distinct NDV ⇒ genuinely different columns, not a dup) |
| `recipient_province` / `recipient_postal_code` | string | **99.99%** | 2 | — | none | ◻ near-empty foreign-address cols (3 non-null rows each) |
| `program_year`, `payment_publication_date`, `delay_…` | string/date | 0% | 5 / 1 / — | — | none | ❌ redundant / constant |

`date_of_payment` correctly **absent** (an ownership interest is not a dated payment) — index plan correctly substitutes `physician_npi` + payment-id + `record_id`. **No mismatches; 8/8 cover all 27,480 rows.**

### 3D. Cross-family hygiene rollup (corrected against current data)

- **Dead columns:** research ≈15 fully-null + dozens ≥98%-null (empty PI 2–5 groups → 256 cols); general's `covered_recipient_primary_type_{3..6}` / `_specialty_{3..6}` run 99.99–100% null (`_specialty_6` exactly 100% / 0 NDV); ownership negligible. **Disk cost ≈ 0** (Lance crushes nulls); the only cost is scan width + schema legibility.
- **`program_year` is narrower than `payment_year`, not equal.** Measured NDV: `payment_year` = **7** (2018–2024, the catalog-derived authoritative partition key), `program_year` = **5** in all three families. The source `program_year` does not cover all 7 years — confirming `payment_year` (added by the transform from the catalog) is the correct key and `program_year` is strictly inferior. The prior diagnostic's "`program_year ⊂ payment_year`" is correct and now quantified.
- **Temporal dirt:** **both** general and research carry `date_of_payment` floor `0002-11-30` (impossible; Open Payments inception is 2013). The prior diagnostic flagged this only for research and *speculated* it for general; it is now **confirmed exact in both.** Max is a clean `2024-12-31`. The floor poisons the BTREE min zone-map (neutral for equality, breaks range-pruning assumptions at the low end).
- **Un-normalized geography:** `recipient_state` runs **69 / 64 / 60** NDV (general / research / ownership) vs ~51 legitimate USPS — foreign + territory + dirt mixed in (general min/max span `AA`…`WY`, i.e. military `AA`/`AE`/`AP` codes present). BITMAP absorbs it for query, but it pollutes by-state analytics.
- **Redundant/constant provenance:** `payment_publication_date` (1 value, `2026-01-23` — the vintage stamp), `delay_in_publication_indicator` (1 value, `No`), `source_file` / `source_url` / `ingested_at` (1 per year). Constant under the current single-vintage `overwrite` model (see the architectural fork in the plan).
- **Sentinel-as-value (`associated_*_pdi_*` / `_ndc_*`):** see the measured probe in §4.2 — the prior plan's central "1.2M `'N/A'` rows" claim must be re-validated against the restored data before any sentinel-nulling is prescribed.

---

## 4. Execution Runtime Specs

Exact configuration to **query** and **safely mutate** these datasets out-of-core without OOM. Values reflect the **current** Modal envelope (verified in `pipelines/cms_open_payments/ingest.py`): `refresh_all` / `reindex_family` `memory=49152` (48 GiB), `ingest_family_year` `memory=32768` (32 GiB), all `cpu=8.0`, `ephemeral_disk=524288` (512 GiB), `SCRATCH_DIR=/tmp/cms_open_payments`, DuckDB `memory_limit='24GB'` / `threads=8` / `temp_directory` under `/tmp`, image env `LANCE_BYPASS_SPILLING=true`.

### 4.1 Query / read (DuckDB out-of-core)
```sql
SET threads            = 8;            -- = vCPU
SET memory_limit       = '24GB';       -- ~75% of a 32 GiB container; headroom for Lance range-GET buffers + Arrow
SET preserve_insertion_order = false;  -- streaming-friendly; no row-order retention buffers
SET temp_directory     = '/tmp/cms_open_payments/duckdb_spill';   -- LOCAL fast ephemeral, NEVER root '/'
```
Environment:
```
TMPDIR=/tmp            # local ephemeral on the container; keeps any Lance/DataFusion spill off '/'
```

#### `LANCE_MEM_POOL_SIZE` — definitive verdict (corrects the prior diagnostic)

The prior diagnostic asserted, in this same section: *"`LANCE_MEM_POOL_SIZE` is not a knob this stack uses … no fleet worker sets it … Do not introduce a phantom variable."* **That assertion is factually wrong on both counts and is retracted here.**

- **It is a real Lance/DataFusion variable.** Lance parses it via `s.parse::<u64>()` as **raw bytes** (a string like `"24GB"` fails the parse and silently reverts to the default). It sizes the **`FairSpillPool`** working set used by the index-build external sort — it raises the ~100 MB/partition default that crashed the merge in `lance-format/lance#2650`. (Source of truth in-repo: `pipelines/ingest_epa/materialize_epa_history.py` module docstring + `docs/pdl_companies_structural_diagnostic.md` §4.2, which states plainly: *"it is a real Lance variable."*)
- **A fleet worker DOES set it.** `pipelines/ingest_epa/materialize_epa_history.py` sets `LANCE_MEM_POOL_SIZE = str(24 * 1024**3)` (24 GiB) alongside `LANCE_MAX_TEMP_DIRECTORY_SIZE = 250 GB` on its `index_image`, precisely for the 422M-row disk-spilled BTREE build. The claim "no fleet worker sets it" is false.
- **But it is INERT under this worker's CURRENT config — by design.** The CMS image sets `LANCE_BYPASS_SPILLING=true`. Lance keys `LANCE_BYPASS_SPILLING` on **presence, not value** (`env::var(...).map(|_| false).unwrap_or(true)` — *any* value, including `"false"`, forces the in-memory sort). With spilling bypassed, the `FairSpillPool` is never instantiated, so `LANCE_MEM_POOL_SIZE` would do nothing if set today. **Setting it now would be cargo-cult.**

**Operative rule for CMS:** `LANCE_MEM_POOL_SIZE` is the correct lever **only on the spill-to-disk index path** — i.e. only if/when general's BTREE build is moved off the in-RAM bypass (because the in-RAM working set strains 48 GiB as the dataset grows past ~100M rows). In that scenario the canonical combination is: **remove `LANCE_BYPASS_SPILLING` from the image** (so spilling re-enables), set `TMPDIR=/tmp` (local NVMe — the DataFusion `DiskManager(OsTmpDirectory)` follows it), and set `LANCE_MEM_POOL_SIZE` (e.g. 24 GiB raw bytes) + `LANCE_MAX_TEMP_DIRECTORY_SIZE` (raise the 100 GB cap). Until that switch is made, the operative out-of-core controls are `memory_limit` + `temp_directory` (DuckDB) and `LANCE_BYPASS_SPILLING` (Lance index build). **Do not set `LANCE_MEM_POOL_SIZE` while `LANCE_BYPASS_SPILLING` is present — it is dead config in that combination.**

### 4.2 Predicate pushdown — the integration contract (measured)
Pushdown is realized **at the Lance scanner**, not the DuckDB SQL layer. Measured `analyze_plan` over the live R2 datasets:

| Predicate (general) | Existing index | Physical plan (Lance scanner) | Matched rows | Pushdown |
|---|---|---|---:|:--|
| `record_id = '…'` | BTREE | `ScalarIndexQuery@record_id_idx` → LanceRead | **1** | ✅ surgical PK point-lookup |
| `covered_recipient_npi = '…'` | BTREE | `ScalarIndexQuery@covered_recipient_npi_idx` → LanceRead | **21** | ✅ resolves to row addresses (NPI recurs across year-fragments) |
| `payment_year = 2023` | BITMAP | `ScalarIndexQuery@payment_year_idx` → LanceRead | 14,700,786 | ✅ index-resolved — no full scan |
| `recipient_state = 'CA'` | BITMAP | `ScalarIndexQuery@recipient_state_idx` → LanceRead | 7,712,705 | ✅ index-resolved |
| `recipient_city = 'Chicago'` | **none** | full `LanceScan` + post-hoc `refine_filter` (no `ScalarIndexQuery`) | 82,581 | ❌ scans all 82,290,893 rows |

*Measured via `scanner(filter=…, prefilter=True).analyze_plan()` against the live R2 dataset. Every indexed predicate (BTREE + BITMAP) lowers to a `ScalarIndexQuery`: the index is consulted and only matching row addresses are read. The single unindexed column (`recipient_city`) degrades to a full `LanceScan` + `refine_filter` over all 82.29M rows. (analyze_plan wall-times here are cold R2 range-GETs — not the warm sub-100 ms point-lookup the `apps/gtm_mcp` gateway sees against a staged copy; the structural truth is the operator, not the latency.)*

**Contract:** push predicates into `lance.dataset(uri, storage_options=so).scanner(filter="…", prefilter=True)` (or `.to_table(filter=…)`), then hand the **pruned** `RecordBatchReader` to DuckDB. A DuckDB query over an **unfiltered** Lance Arrow stream filters post-hoc → full column scan. This is the `apps/gtm_mcp` "BTREE pushdown, sub-100 ms point-lookup" path. **Fragment-level skipping does not occur for NPI** (an NPI recurs across year-fragments → the BTREE resolves to row addresses spread over many fragments); row-level pushdown is surgical regardless. The `record_id` PK is the cleanest point-lookup (resolves to a single row).

**`'N/A'` / sentinel probe (corrects the prior plan's central Phase-2 premise).** The prior diagnostic claimed `associated_device_or_medical_supply_pdi_*` stores literal `"N/A"` on ~1.2M general rows, and the prior plan's D5 hinged on nulling it. Measured against the **restored** general:

| Column (repeating group) | literal `'N/A'` | `'NA'`/variants | NULL | NULL % | Verdict |
|---|---:|---:|---:|---:|:--|
| `associated_device_or_medical_supply_pdi_1` | **27,346,208** | 25 | 47,205,292 | 57.4% | ❌ `'N/A'` on **33.2%** of all rows |
| `associated_device_or_medical_supply_pdi_2` | **27,346,208** | 0 | 53,872,257 | 65.5% | ❌ |
| `associated_device_or_medical_supply_pdi_3` | **27,346,208** | 0 | 54,491,554 | 66.2% | ❌ |
| `associated_device_or_medical_supply_pdi_4` | **27,346,208** | 0 | 54,632,117 | 66.4% | ❌ |
| `associated_device_or_medical_supply_pdi_5` | **27,346,208** | 0 | 54,731,371 | 66.5% | ❌ |
| `associated_drug_or_biological_ndc_{1..5}` | **0** | 0 | 23.4M – 81.8M | 28.4% – 99.4% | ✅ NULLs only — **no sentinel** |

*Two corrections to the prior plan's D5 premise, both measured against the restored general (82,290,893 rows): (1) the literal `'N/A'` sentinel is **27,346,208 rows — 33.2% of the table, ~23× the prior diagnostic's "~1.2M" estimate** (a material undercount that would have mis-sized the fix); (2) it is **confined to the `associated_device_or_medical_supply_pdi_*` group** — the `associated_drug_or_biological_ndc_*` group carries **zero** `'N/A'` (NULLs only), so D5's "null `'N/A'` on the device-PDI **and** drug-NDC columns" was half-wrong: the NDC half is a no-op. The identical 27,346,208 across all five `pdi_*` slots means the same rows carry `'N/A'` in every device-PDI slot — a single CMS export convention, cleanly targetable by a column-scoped `nullif`.*

### 4.3 Mutate — per-operation compute constraints (blast-radius isolated; assessed from first principles for general @ 82M)

| Operation | Memory | Disk / staging | Key env / PRAGMA | OOM / failure risk |
|---|---|---|---|---|
| **General re-ingest** (DuckDB CSV→Arrow→Lance, ~8 GB 2024 CSV) | `memory_limit='24GB'` on 32 GiB (or 48 GiB on `refresh_all`) | one CSV at a time; **stage on `/tmp` overlay**, 512 GiB; `temp_directory` on `/tmp` | `parallel=false` (mandatory — quoted newlines + `null_padding`), streaming `to_arrow_reader(1048576)` | **low** — streaming; never materializes the 8 GB file. Proven: held the full 82.29M-row restore at ~23 GiB peak. |
| **General BTREE build** (4 indices, 82M rows) | **48 GiB** (in-RAM sort, `LANCE_BYPASS_SPILLING=true`) | — | in-RAM bypass | **medium → rising.** Working set ≈ rows × (key + rowid) × overhead; 82M fits 48 GiB today, but this is the **OOM-fragile link as the dataset grows.** ~100M+ rows on a load-bearing column is the empirical threshold (ARCHITECTURE.md) where the in-RAM path strains and a direct-R2 index write would also trip R2 `InvalidPart`. Build sequentially (current code does). Past the threshold → spill path (§4.1) or the local-stage-then-boto3 publish (already in place). |
| **General BITMAP build** (6 cols) | modest (roaring bitmaps) | — | default | low |
| **Publish (full_swap)** | n/a (boto3 streaming) | stage full dataset on `/tmp`; `__staging` sibling on R2 | boto3 uniform-part multipart; manifest-LAST; verify==local | **low now (was the corruption source; hardened).** Cost: re-uploads the full ~20 GiB tree on a from-scratch refresh. |
| **Publish (incremental)** | n/a | stage from R2 | append-only, only new files | low — the steady-state path (reindex / single-year); does not re-push the data tree |
| **Research / ownership reindex** | 48 GiB ≫ need | — | `LANCE_BYPASS_SPILLING=true` | low (5.9M / 27k rows) |

#### Modal-Volume vs `/tmp` staging — the hard constraint (measured this session)
The ARCHITECTURE.md "Giants — Volume-staged, append-only" rule **does not hold for `lance.write_dataset`'s dataset commit.** Lance's commit performs an atomic `rename()` that the storage layer must accept. A Modal Volume's FUSE layer **rejects it** with `EPERM` (`LanceError(IO): … Unable to rename file: Operation not permitted (os error 1), …/lance-table/src/…`) — observed live this session at ledger ids **76, 77, 89, 90** during the general restore's Volume attempt. `/dev/shm` (tmpfs, rename-safe) caps at 16 GiB — too small for general. **The `/tmp` overlay accepts the commit rename and held the full 82.29M-row write** (≈23 GiB peak: ~15–18 GiB dataset + one ≤8 GiB CSV, deleted per year). `ephemeral_disk=524288` is requested for **space + write speed** (it backs `/tmp` with 512 GiB; a no-`ephemeral_disk` overlay is also ~10× slower on multi-GB writes) — **not** to change the FS type. Consequence: the ARCHITECTURE.md rule, as written, applies to the **direct-R2 `create_scalar_index` giant path** (where staging to a Volume avoids R2's multipart ceiling and the build is sort-spill, not a dataset commit) — it is **not transferable** to a full `write_dataset` rebuild, which must use `/tmp`.

#### `ephemeral_disk` floor (corrects a prior-plan prescription)
The prior plan's D2 proposed lowering `refresh_all` `ephemeral_disk` from 524288 → **131072 (128 GiB)** "for DuckDB CSV spill only" once general moved to a Volume. **Both halves are infeasible:** general cannot move to a Volume (above), and **Modal's `ephemeral_disk` floor is 512 GiB** — a request below 524288 is hard-rejected at deploy (commit `d711bdb`/#22 raised it *to* the floor for exactly this reason). 512 GiB is therefore the **minimum**, not a tunable ceiling; the cost is that it forces preemptible spot capacity (a giant backfill can be mid-run preempted — ledger id 64), which is absorbed by `retries=3` + the idempotent, read-back-verified publish.

> **Spill vs bypass tension (unchanged, restated).** The directive's "disk-spilled index rebuilds routed to high-I/O volumes" is the *out-of-core* posture; this worker currently sets `LANCE_BYPASS_SPILLING=true` (in-RAM, the documented fleet default at 32–64 GiB). These are opposite knobs. At general's 82M+ scale the in-RAM build is the OOM-fragile link as it grows. The canonical switch once it strains 48 GiB is in §4.1 (remove the bypass, route Lance temp to `/tmp`, size `LANCE_MEM_POOL_SIZE`).

---

## 5. Optimization Blueprint (pointer)

**There is no longer a mandatory tier.** The prior diagnostic's Tier 0/0.5 (harden publish, restore general) are **done**. The remaining work — temporal-dirt sanitization, geography normalization, dead-column hygiene, optional index parity, optional `record_id` recast, optional resolution-key clustering — is **entirely optional logical polish on healthy datasets**, sequenced by blast radius. It is specified in full, with blast radius + rollback per step and the architectural fork (single-vintage `overwrite` vs vintage-accumulation) that must be resolved before any column-dropping, in the companion plan:

> **`docs/plans/CMS_OPEN_PAYMENTS_OPTIMIZATION_PLAN.md`** — the brand-new plan that replaces the retired `CMS_OPEN_PAYMENTS_REMEDIATION_PLAN.md`.

### Explicitly NOT required (measured)
- **Compaction.** 0 tombstones, large few fragments, append-per-year topology on all three. Not a fragmentation remedy anywhere. General was *rebuilt because it was broken*, not because it was fragmented — and it is no longer broken.
- **Restore / re-ingest.** General is whole (82,290,893 rows, 10/10 indices, verified). Nothing to restore.
- **Publish hardening.** Shipped (PR #213/#224). The corpse-producing primitive is gone from this worker.
- **Dictionary/enum recasts for storage.** Lance v2.1 already RLE/dictionary-encodes low-cardinality strings physically (2.99×–5.19× measured). The value is the BITMAP indices (already present), not the logical type.

---

## Appendix A — Teardown of the retired remediation plan

The retired plan (`docs/plans/CMS_OPEN_PAYMENTS_REMEDIATION_PLAN.md`, **now deleted**) was authored against pre-PR-#213 code and a since-falsified diagnostic. Every claim below is verified against the shipped `pipelines/cms_open_payments/ingest.py` on `main` (HEAD `2cb4538`), the git history, the live R2 state, and the `ops.cms_open_payments_runs` ledger. Offending text is quoted. The operator's instruction was explicit: the stale plan "is only going to confuse any future agents… it should not 'amend' stale data" — hence deletion, with this teardown preserved so the death is documented.

**Confirmed/refuted against the assignment's ammunition (items 1–6):**

1. **CONFIRMED — the central Phase-0 fix was already shipped; the whole §3 is moot.** The plan's §3.3 prescribes building a new `_publish_dataset` to "replace `_replace_r2_prefix`", and §7's change map lists "delete `_replace_r2_prefix`; repoint 3 callers." **This work shipped in PR #213 (commit `3858f07`)** under different names: `_publish_full_swap` (stage→verify==local→swap, manifest-LAST) and `_publish_incremental` (append-only, never wipe), gated by `_verify_published` (PR #224, `4cbc7ee`). In the current worker, `_replace_r2_prefix` **exists only as a word in a comment** (`ingest.py:461`, "The retired `_replace_r2_prefix`…") — the function is gone. The plan's `_upload_file_with_retry` / `_remote_index` / `_publish_dataset` / `_verify_published` specs all describe already-shipped behavior. **Every dead line citation in §3.5/§7:** `ingest.py:431`, `:436–451`, `:444`, `:925`, `:959`, `:1011`, `:1067`, `:1088`, `:848` — none maps to the code it claims (the file is now 1554 lines with an entirely different publish section). A future executor following §3 would re-implement, under a third name, a primitive that already exists — pure waste and a merge hazard.

2. **CONFIRMED — D2 "Volume-staged" is infeasible for a `write_dataset` rebuild.** The plan's D2: *"Stage general's local dataset on a **Modal Volume** (`modal.Volume`), not large `ephemeral_disk`… the Volume persists across container restarts so `resume=True` skips already-landed years."* **Measured false this session.** Lance's dataset commit performs an atomic `rename()`; a Modal Volume's FUSE layer rejects it with `EPERM` — the ledger carries the exact error at ids **76, 77, 89, 90**: `LanceError(IO): … Unable to rename file: Operation not permitted (os error 1), …/lance-table/src/…`. Giants stage on the `/tmp` overlay (which accepts the rename and held the full 82.29M-row write); `/dev/shm` (tmpfs, rename-safe) is too small at 16 GiB. The plan inverts the actual constraint — it routes the giant to the one filesystem that **cannot** hold it.

3. **CONFIRMED — "lower `ephemeral_disk` 524288 → 131072 (128 GiB)" is infeasible.** The plan's §3.6/D2: *"Lower `refresh_all` `ephemeral_disk` from 524288 → e.g. 131072 (128 GiB, DuckDB CSV spill only)."* **Modal's `ephemeral_disk` floor is 512 GiB** — a request below 524288 is hard-rejected at deploy. The repo's own history proves it: commit `d711bdb` (PR #22) is titled *"raise ephemeral_disk to Modal's 512 GiB floor."* 512 GiB is the **minimum**, not a tunable ceiling. The prescription would fail `modal deploy` outright.

4. **CONFIRMED — per-year Volume RESUME (D2/O3) is dead with the Volume.** The plan's O3: *"general backfill runs Volume-staged; a mid-run kill + re-run with `resume=True` completes without re-downloading already-landed years"*, and §3.5 adds a `resume` param + `vol.commit()` per year. Since the Volume itself is infeasible (item 2), the entire resume mechanism it depends on is dead. The shipped reality: spot-preemption recovery is `retries=3` (re-run from scratch on a fresh container) + the idempotent, read-back-verified publish — no Volume, no per-year checkpoint, no `resume` flag.

5. **CONFIRMED — §4.3 acceptance references a `_versions != 0` / `--clobber-broken` flow that does not exist.** The plan's §4.1 pre-flight gates on *"If `_versions != 0` → STOP"* and §4.2 wires *"a `--clobber-broken` local-entrypoint flag wired to allow_clobber on the general publish."* The shipped `_publish_full_swap` **deletes-at-swap unconditionally** (after staging + verifying the replacement) — there is no `allow_clobber`, no `--clobber-broken` flag, no `_versions == 0` guard anywhere in `ingest.py`. The restore that actually happened used plain `backfill --only-family general` (no clobber flag); `refresh_all` already `rmtree`s and rebuilds from scratch. The plan's entire D3 "guarded clobber" decision and its §4.2 command block describe a mechanism that was never built and is unnecessary (the swap is inherently safe).

6. **CONFIRMED the variable is REAL; REFUTED the prior diagnostic's "phantom" claim — and the assignment's instinct to flag it is right.** The companion diagnostic (`docs/cms_open_payments_structural_diagnostic.md` §4.1, prior version) asserted: *"`LANCE_MEM_POOL_SIZE` is not a knob this stack uses… no fleet worker sets it… Do not introduce a phantom variable."* **Both factual claims are false.** (a) It is a **real Lance/DataFusion variable** — parsed as raw bytes via `s.parse::<u64>()`, sizing the `FairSpillPool` working set on the index-build external sort (raises the ~100 MB/partition default that crashed `lance-format/lance#2650`). `docs/pdl_companies_structural_diagnostic.md` §4.2 states plainly: *"it is a real Lance variable."* (b) A **fleet worker does set it**: `pipelines/ingest_epa/materialize_epa_history.py` sets `LANCE_MEM_POOL_SIZE = str(24 * 1024**3)` on its `index_image` for the 422M-row spilled BTREE build. **However**, the *operational conclusion* (don't set it on CMS now) is correct **for a different reason**: the CMS image sets `LANCE_BYPASS_SPILLING=true`, and because Lance keys that var on **presence not value**, the `FairSpillPool` is never instantiated — so `LANCE_MEM_POOL_SIZE` is **inert under the current config**. Setting it today would be cargo-cult. It becomes the correct lever **only** if general's BTREE build is moved off the in-RAM bypass to the spill path (see diagnostic §4.1). The new diagnostic states this precisely; the retired one was wrong on the facts even where it reached a defensible end-state.

**Additional defects found (beyond items 1–6):**

7. **The companion diagnostic's foundational premise is dead.** It opens: *"`cms_general_payments` — STRUCTURALLY BROKEN (P0). It is not a dataset; it is a partial-publish corpse: 71 orphaned `data/*.lance` fragments (14.98 GiB), and zero `_versions/` manifests."* General is now **82,290,893 rows, 83 fragments, 10/10 indices, version 17, 0 tombstones** (measured live). Any agent reading the old diagnostic would act on a corpse that no longer exists — the precise confusion the operator wants eliminated.

8. **The "1.2M `'N/A'` sentinel rows" claim (plan D5 / diagnostic §3A) was derived from the corpse sample, not the live data, and is unverified for the restored dataset.** The plan makes sentinel-nulling a Phase-2 deliverable on this basis. The figure came from *"a 1.8M-row sample of its orphaned fragments."* It must be re-measured against the restored 82.29M-row general before any sentinel logic is prescribed (the new diagnostic §4.2 does exactly this). Building a code change on a corpse-sample statistic is unsound.

9. **Stale telemetry throughout the plan.** Plan §0.3: research *"5,936,454 rows… indices cover 100% of fragments (research 10/10→8/8…)"* and the column counts (*"general 95… research 256"*) were correct, but the plan's "rows lost in publish = 12,239,319" / "70,051,574 rows on R2" framing for general is now entirely obsolete (0 lost, 82.29M present). The plan's whole "Current state" §0 is a snapshot of a moment that has been overwritten.

10. **`EXPECTED_INDEX_COUNT` is already shipped — the plan presents it as new work.** Plan §3.4: *"Add `EXPECTED_INDEX_COUNT = {"general": 10, "research": 10, "ownership": 8}`."* This constant **already exists** in `ingest.py` (with an import-time assertion that it tracks the `FAMILIES` registry). Another already-done item dressed as a task.

11. **Plan §3.7 / §4 test+restore choreography is retrospective.** The plan's Phase-0 acceptance tests ("kill-test on ownership", "integration on ownership") and Phase-1 restore were the *process that already ran* to produce the current healthy state (the ledger shows the ownership publish/verify cycles at ids 72/73, 86/87, 109/110 and the general restore at 92–100). Presenting them as forward work would cause a future agent to needlessly re-run a completed restore against a healthy dataset — at best wasteful, at worst a gratuitous 20 GiB rebuild.

**Net:** the retired plan's two P0 objectives are complete; its central code spec targets a deleted function via dead line numbers; three of its baked-in engineering decisions (D2 Volume, D2 ephemeral lowering, D3 guarded clobber) are infeasible or nonexistent against shipped reality; and its companion diagnostic profiled a corpse that no longer exists. It is not salvageable by amendment — it is a snapshot of a problem that has been solved by different means. **Deleted.** The only forward-looking content it held (date-floor sanitization, geography normalization, optional index parity, the dead-column/clustering rejections) is carried forward — re-validated against live data — in this plan's §3.

---

## Appendix B — Provenance
- **Telemetry:** `pylance 7.x` (`count_rows`, `get_fragments`, `physical_rows`, `list_indices`, `stats.index_stats`), R2 `list_objects_v2` byte census (boto3), `duckdb 1.5.x` streaming aggregates over the live R2 datasets (null exact; NDV via HLL `approx_count_distinct`), Lance `analyze_plan` for pushdown. Harness: a throwaway read-only `modal run` function mirroring the worker image (`pipelines/cms_open_payments/ingest.py`).
- **Read path:** `lance.dataset(uri, storage_options={aws_access_key_id, aws_secret_access_key, aws_endpoint, aws_region='auto', aws_virtual_hosted_style_request='false'})` against R2. All three datasets opened directly from their committed R2 manifests.
- **Ledger:** `ops.cms_open_payments_runs` (Postgres, `HQX_DB_URL_POOLED`). Latest restore: run #101 `refresh_all=success`; general `publish` (id 99) + `verify` (id 100) = `success` @ 82,290,893 rows; the Volume-rename `EPERM` failures at ids 76/77/89/90.
- **No dataset mutation occurred.** No DDL, no index ops, no writes to any `cms_*` prefix or ops table. Credentials injected via `doppler run --project core-x --config prd`; no secret values persisted.
