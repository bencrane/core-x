# CMS Open Payments — Lance Structural Diagnostic

**Targets (Gen-3 SoR, R2):**
- `s3://data-sink/active/cms_general_payments/` — General Payments
- `s3://data-sink/active/cms_research_payments/` — Research Payments
- `s3://data-sink/active/cms_ownership/` — Ownership Payments

**Mode:** Read-only, first-principles. Zero DDL / zero index ops / zero dataset writes. Assessed independent of all downstream consumers.
**Date:** 2026-06-06 · **Vintage:** program years 2018–2024 (CMS publication `2026-01-23`), ingest run #58 `2026-06-01/02`.
**Method:** `pylance 7.0.0` manifest/fragment/index introspection + R2 `ListObjectsV2` byte census + DuckDB `1.5.3` streaming aggregates (null density exact; NDV via HyperLogLog `approx_count_distinct`, ≈±1.6% σ, marked *exact* where computed exactly) + Lance physical-plan `analyze_plan` pushdown probe. `general` is unreadable (no manifest) and was characterized from data-file footers + a 1.8M-row sample of its orphaned fragments. Corroborated against `ops.cms_open_payments_runs`.

---

## 1. Headline Posture

**One dataset is destroyed; two are physically healthy but schema-bloated and mis-framed on their resolution key. The publish path that broke the first will break it again.**

- **`cms_general_payments` — STRUCTURALLY BROKEN (P0).** It is not a dataset; it is a partial-publish corpse: **71 orphaned `data/*.lance` fragments (14.98 GiB), and *zero* `_versions/` manifests, `_indices/`, or `_transactions/`.** Lance cannot open it (`Not found: …/_versions`). The data is also **incomplete** — footers sum to **70,051,574 rows of the 82,290,893** that ingested locally (run #58), so **~12.24M rows and ~12 data files never uploaded**. The ledger confirms: every 2018–2024 year ingested locally, then `phase=publish` returned `error` — *"Failed to upload …/general_lance/data/…"*. Currently **unreadable, unindexed, and unrecoverable from R2** (the wipe-first publish destroyed any prior good copy; the local rebuild is gone with the Modal container). It must be **re-ingested from CMS**.
- **`cms_research_payments` — physically excellent, logically dirty.** 5,936,454 rows, 0 tombstones, 8 well-sized fragments (v2.1), **10/10 scalar indices cover 8/8 fragments**, index storage a lean 5.1% of data. But: **the declared resolution key `covered_recipient_npi` is 96.40% NULL** (research pays entities, not covered recipients — the populated key is `principal_investigator_1_npi` at 4.27% null); **~15 columns are 100% NULL and ~40 are ≥98% NULL** (empty PI 2–5 repeating groups); `date_of_payment` carries an impossible **dirt floor `0002-11-30`**; `recipient_state` is un-normalized (64 NDV); `program_year` duplicates `payment_year`; several constants (`payment_publication_date`, `delay_in_publication_indicator`).
- **`cms_ownership` — physically healthy, trivially small.** 27,480 rows, 0 tombstones, 7 fragments, **8/8 indices cover 7/7**. Index plan optimal (correctly omits `date_of_payment`). Minor: foreign-address columns ~99.99% null, same constant columns. Nothing structurally actionable — at this scale indices and layout are free.
- **ROOT CAUSE (cross-cutting).** The publish primitive `_replace_r2_prefix` (`pipelines/cms_open_payments/ingest.py`) **wipes the R2 prefix, then re-uploads every data file with no retry and no atomic swap.** For a 16 GiB / ~83-file giant this is failure-prone *and destructive-first*. It directly **contradicts the documented fleet rule** (`ARCHITECTURE.md`, "Giants — Volume-staged, append-only"): *upload only the new index/manifest files; **never wipe or re-upload data files.*** General is precisely the giant that rule exists for. **It will recur on every general refresh until the publish is hardened.**

**Verdict:** research + ownership are query-ready today for NPI / `record_id` / manufacturer-id / state / year resolution (sub-10 ms indexed pushdown, measured); general delivers **nothing** until restored. The schema across all three carries large dead-column and constant ballast — though, because Lance v2.1 already RLE/dictionary-compresses constants and nulls to near-zero on disk, this is a **logical-hygiene and scan-width** problem, **not** a storage problem (measured compression: research 2.99×, all-string schema). Do not frame the schema work as a disk reclamation.

---

## 2. Telemetry Grid

| Metric | general | research | ownership |
|---|---:|---:|---:|
| **Structural status** | **BROKEN (no manifest)** | healthy | healthy |
| Logical rows (readable) | **— (unreadable)** | **5,936,454** | **27,480** |
| Rows intended (ledger, local commit) | 82,290,893 | 5,936,454 | 27,480 |
| Rows present on R2 (footer sum) | **70,051,574** | 5,936,454 | 27,480 |
| **Rows lost in publish** | **12,239,319** | 0 | 0 |
| Deleted / tombstoned | n/a | **0 (0.000%)** | **0 (0.000%)** |
| Columns | 95 (footer) | 256 | 34 |
| Fragments | 71 data files / **0 manifest frags** | **8** | **7** |
| Rows/frag — min · avg · max | 17,952 · 986,641 · 1,048,576 † | 31,223 · 742,056 · 1,048,576 | 3,046 · 3,925 · 4,591 |
| Frags at 1,048,576 cap | — | 1 / 8 | 0 / 7 |
| Data on disk (R2, compressed) | **16,086,469,644 B · 14.98 GiB** (incomplete) | **3,583,? B · 3.417 GiB** | 3.5 MiB |
| Scalar-index storage (R2) | **0 B (none — index tree never written)** | **174.9 MiB** (15 files, 10 idx) | 0.8 MiB (11 files, 8 idx) |
| Index : data ratio | — | **0.051× (5.1%)** | ~0.23× (free at this scale) |
| Manifests / transactions | **0 / 0** | 18 / 17 | 16 / 15 |
| Total R2 footprint | 14.98 GiB (71 obj) | **3.509 GiB (58 obj)** | 4.4 MiB (49 obj) |
| Decoded (uncompressed Arrow) | ~75.8 GiB (extrapolated) ‡ | **10.226 GiB** | 18.18 MiB |
| On-disk compression (decoded ÷ data) | ~4.3× (orphaned subset) | **2.99×** | 5.19× |
| Storage format version | **2.1** (footer) | **2.1** | **2.1** |
| Dataset version | n/a (no manifest) | 17 (=1 write/yr ×7 + 10 idx, last write wins) | 15 |

† general rows/frag from data-file footers (no fragment manifest exists). ‡ general decoded extrapolated from a 1.8M-row sample (989.9 B/row decoded × 82.29M intended).

**Footprint class split** (authoritative; the recon census conflates `_indices/*.lance` into "data" since both end `.lance` — these are the corrected per-class numbers from the R2 listing):

| | general | research | ownership |
|---|---:|---:|---:|
| `data/` | 71 obj · 14.98 GiB | 8 obj · 3,417.4 MiB | 7 obj · 3.5 MiB |
| `_indices/` | **0** | 15 obj · 174.9 MiB | 11 obj · 0.8 MiB |
| `_versions/` | **0** | 18 obj · 0.4 MiB | 16 obj · 0.1 MiB |
| `_transactions/` | **0** | 17 obj · ~0 | 15 obj · ~0 |

**Fragment topology (read-amplification).** Both healthy datasets follow an **append-per-program-year** topology: one fragment per year, plus a spillover when a year exceeds the 1,048,576-row file cap (research 2023 = 1,079,799 → 1,048,576 + 31,223, hence 8 fragments / 7 years). Fragments are large and few (research avg 427 MiB) → **no read-amplification, no compaction debt** (0 tombstones). This is *not* a fragmentation problem. General intended ~83 fragments across 7 years (≈12 missing).

---

## 3. Schema & Index Ledger

NDV = HLL estimate unless *exact*. "Optimal" assessed purely on cardinality + structural role. Only **load-bearing** columns (resolution/join keys, indexed categoricals, money/temporal) and **defective** columns are enumerated; dead/constant columns are rolled up per family.

### 3A. `cms_general_payments` (95 cols; cardinality from 1.8M-row orphaned-fragment sample — **all "Existing index" = NONE because the dataset is broken/unindexed**)

| Column | Type | Null % | NDV (sample) | Existing | Optimal | Verdict |
|---|---|---:|---:|:--|:--|:--|
| `covered_recipient_npi` | string | **0.60%** | 446k → millions | — (broken) | **BTREE** | ✅ correct, well-populated key (contrast research) |
| `record_id` | string | 0.00% | unique (2.5M/1.8M HLL) | — | **BTREE** | ✅ PK · ⚠ numeric → `int64` candidate |
| `applicable_manufacturer_or_applicable_gpo_making_payment_id` | string | 0.00% | ~604 (sample) | — | BTREE (join) | ✅ join key · medium-card, BITMAP also viable |
| `date_of_payment` | date32 | 0.00% | ~1,525 | — | **BTREE** | ✅ temporal range (verify dirt floor as in research) |
| `payment_year` | int16 | 0.00% | 7 *exact* | — | **BITMAP** | ✅ matched (typed partition key) |
| `covered_recipient_type` | string | 0.00% | 3 | — | **BITMAP** | ✅ (Physician 81.5% / NPP 17.9% / Teaching Hosp 0.5%) |
| `nature_of_payment_or_transfer_of_value` | string | 0.00% | 13 | — | **BITMAP** | ✅ matched |
| `form_of_payment_or_transfer_of_value` | string | 0.00% | 6 | — | **BITMAP** | ✅ matched |
| `recipient_state` | string | 0.01% | **67** | — | **BITMAP** | ✅ type-matched · ⚠ un-normalized (>51) |
| `dispute_status_for_publication` | string | 0.00% | 2 | — | **BITMAP** | ✅ matched |
| `associated_device_or_medical_supply_pdi_{2,3,4}` | string | 33% | low | — | none | ❌ **literal `"N/A"` sentinel** = 1.2M rows (not nulled) |
| `program_year` | string | 0.00% | 7 | — | none | ❌ redundant with `payment_year` (drop) |
| `payment_publication_date` | date32 | 0.00% | 1 | — | none | ❌ constant (→ metadata, unless vintage key) |
| `delay_in_publication_indicator` | string | 0.00% | 1 ("No") | — | none | ❌ constant |
| `covered_recipient_primary_type_{3..6}`, `_specialty_{3..6}` | string | 100% | 0 | — | none | ❌ fully null (drop) |

Index plan to apply on restore (registry): **BTREE** ×4 = `covered_recipient_npi`, `applicable_manufacturer_…_id`, `date_of_payment`, `record_id`; **BITMAP** ×6 = `payment_year`, `covered_recipient_type`, `nature_of_payment_…`, `form_of_payment_…`, `recipient_state`, `dispute_status_for_publication`.

### 3B. `cms_research_payments` (256 cols; null exact, NDV HLL over all 5,936,454 rows)

| Column | Type | Null % | NDV | Existing | Optimal | Verdict |
|---|---|---:|---:|:--|:--|:--|
| `principal_investigator_1_npi` | string | **4.27%** | ~70,323 | **BTREE** | BTREE | ✅ **the real resolution key for research** |
| `covered_recipient_npi` | string | **96.40%** | ~22,073 | **BTREE** | BTREE (keep) | ⚠ **96% null** — indexes mostly absence; reframe, not primary |
| `record_id` | string | 0.00% | ~7.10M *(unique, HLL overest)* | **BTREE** | BTREE | ✅ PK · ⚠ numeric → `int64` candidate |
| `applicable_manufacturer_…_making_payment_id` | string | 0.00% | **1,369** | **BTREE** | BTREE/▵BITMAP | ✅ join key · medium-card (BITMAP also defensible) |
| `date_of_payment` | date32 | 0.00% | 2,835 | **BTREE** | BTREE | ✅ · ❌ **dirt floor `min = 0002-11-30`** (impossible; OP starts 2013) |
| `payment_year` | int16 | 0.00% | 7 *exact* | **BITMAP** | BITMAP | ✅ matched |
| `covered_recipient_type` | string | 0.00% | 5 | **BITMAP** | BITMAP | ✅ matched (Non-cov Entity 81.7%) |
| `related_product_indicator` | string | 0.00% | 2 | **BITMAP** | BITMAP | ✅ matched |
| `recipient_state` | string | 0.14% | **64** | **BITMAP** | BITMAP | ✅ · ⚠ un-normalized (>51) |
| `dispute_status_for_publication` | string | 0.00% | 2 | **BITMAP** | BITMAP | ✅ matched |
| `form_of_payment_or_transfer_of_value` | string | 0.00% | 5 | **—** | ▵BITMAP | ◻ candidate (general indexes it; add for parity) |
| `total_amount_of_payment_usdollars` | decimal128(14,2) | 0.00% | 839,725 | — | none | ✅ exact money type (no change) |
| `program_year` | string | 0.00% | 7 | — | none | ❌ redundant with `payment_year` |
| `payment_publication_date` | date32 | 0.00% | 1 | — | none | ❌ constant |
| `delay_in_publication_indicator` | string | 0.00% | 1 | — | none | ❌ constant |
| `covered_recipient_primary_type_{2..6}`, `_specialty_{2..6}`, `principal_investigator_{1..5}_primary_type_{2..6}`/`_specialty_{2..6}` (≈15 cols) | string | **100%** | 0 | — | none | ❌ **fully null** (drop) |

**Index state:** 10/10 cover 8/8 fragments; **no mismatches** (no BTREE-on-tiny-categorical, no BITMAP-on-high-card). Lean 5.1% index:data.

### 3C. `cms_ownership` (34 cols; null exact, NDV HLL over all 27,480 rows)

| Column | Type | Null % | NDV | Existing | Optimal | Verdict |
|---|---|---:|---:|:--|:--|:--|
| `physician_npi` | string | 0.073% | ~5,515 | **BTREE** | BTREE | ✅ correct key |
| `record_id` | string | 0.00% | ~32,659 *(unique, HLL overest)* | **BTREE** | BTREE | ✅ PK |
| `applicable_manufacturer_…_making_payment_id` | string | 0.00% | **455** | **BTREE** | BTREE/▵BITMAP | ✅ · low-card join (BITMAP viable) |
| `payment_year` | int16 | 0.00% | 7 *exact* | **BITMAP** | BITMAP | ✅ matched |
| `physician_primary_type` | string | 0.00% | 6 | **BITMAP** | BITMAP | ✅ matched |
| `recipient_state` | string | 0.011% | 60 | **BITMAP** | BITMAP | ✅ · ⚠ un-normalized |
| `dispute_status_for_publication` | string | 0.00% | 2 | **BITMAP** | BITMAP | ✅ matched |
| `interest_held_by_physician_or_an_immediate_family_member` | string | 0.00% | 2 | **BITMAP** | BITMAP | ✅ matched |
| `total_amount_invested_usdollars` / `value_of_interest` | decimal128(14,2) | 0.00% | 3,658 / 8,311 | — | none | ✅ exact · ⚠ identical 0–344,292,301.95 range (verify not duplicated) |
| `recipient_province` / `recipient_postal_code` | string | **99.99%** | 2 | — | none | ◻ near-empty foreign-address cols |
| `payment_publication_date`, `delay_…` (n/a), constants | — | — | 1 | — | none | ❌ constant |

`date_of_payment` correctly **absent** (an ownership interest is not a dated payment) — index plan correctly substitutes. **No mismatches; 8/8 cover 7/7.**

### Cross-family hygiene rollup
- **Dead columns:** research ≈15 fully-null + ≈40 ≥98% null (empty PI 2–5 groups); general ≈10 fully-null (`*_type_{3..6}`, `*_specialty_{3..6}`); ownership negligible. Disk cost ~0 (Lance crushes nulls); cost is scan width + schema legibility.
- **Sentinel-as-value:** `associated_device_or_medical_supply_pdi_*` stores literal `"N/A"` (general: 1.2M rows). The transform's `nullif(trim(x),'')` nulls empty strings only — `"N/A"` survives, inflating cardinality and masking true null density.
- **Un-normalized geography:** `recipient_state` (and `*_license_state_code*`) run 60–67 NDV vs ~51 legitimate USPS — foreign + dirt mixed in. Tightens nothing today (BITMAP absorbs it) but pollutes by-state analytics.
- **Redundant/constant provenance:** `program_year` (⊂ `payment_year`), `payment_publication_date` (1 value), `delay_in_publication_indicator` (1 value), `source_file`/`source_url`/`ingested_at` (1 per year).
- **Temporal dirt:** research `date_of_payment` floor `0002-11-30` (poisons BTREE min zone map; effectively neutral for equality but breaks range-pruning assumptions).

---

## 4. Execution Runtime Specs

Exact configuration to **query** and **safely mutate** these datasets out-of-core without OOM. Values reflect the current Modal envelope (worker: 32 GiB / 8 vCPU / 512 GiB ephemeral; reindex: 48 GiB) and fleet convention (`ARCHITECTURE.md`, `docs/reference/0{1,2}_*`).

### 4.1 Query / read (DuckDB out-of-core)
```sql
SET threads            = 8;            -- = vCPU
SET memory_limit       = '24GB';       -- ~75% of a 32 GiB container; leaves headroom for Lance range-GET buffers + Arrow
SET preserve_insertion_order = false;  -- streaming-friendly; avoids row-order retention buffers
SET temp_directory     = '/tmp/duckdb_spill';   -- LOCAL NVMe-backed ephemeral, NEVER the root fs
```
Environment:
```
TMPDIR=/tmp            # NVMe-backed ephemeral on the container; keeps spills off '/'
```
> **`LANCE_MEM_POOL_SIZE` is not a knob this stack uses.** Lance-python governs read memory through range-GET concurrency + the DuckDB consumer, not a single pool env var; no fleet worker sets it. The operative out-of-core controls are `memory_limit` + `temp_directory` (DuckDB) and `LANCE_BYPASS_SPILLING` (index build, below). Do not introduce a phantom variable.

### 4.2 Predicate pushdown — the integration contract (measured)
Pushdown is realized **at the Lance scanner**, not at the DuckDB SQL layer. Measured `analyze_plan` on staged research:

| Predicate | Index | Physical plan | rows_scanned | bytes_read |
|---|---|---|---:|---:|
| `record_id = …` | BTREE | `ScalarIndexQuery@record_id_idx(BTree)` | **1** | 2.27 KB |
| `principal_investigator_1_npi = …` | BTREE | `ScalarIndexQuery@…(BTree)` | 450 | 409.8 KB |
| `recipient_state = 'CA'` | BITMAP | `ScalarIndexQuery@…(Bitmap)` | 575,500 | 42.15 MB |
| `recipient_city = …` | **none** | full `LanceRead` + `refine_filter` | **5,940,000** | 9.45 MB |

**Contract:** push predicates into `lance.dataset(uri, storage_options=so).scanner(filter="…", prefilter=True)` (or `.to_table(filter=…)`), then hand the *pruned* `RecordBatchReader` to DuckDB. A DuckDB query over an **unfiltered** Lance Arrow stream filters post-hoc → full column scan (the `recipient_city` row). This is the `apps/gtm_mcp` "BTREE pushdown, sub-100 ms point-lookup" path. **Fragment-level skipping does not occur for NPI** (an NPI recurs in every year-fragment → BTREE hits 7–8/8 fragments); row-level pushdown is surgical regardless (see §5 clustering).

### 4.3 Mutate — per-operation compute constraints (blast-radius isolated)

| Operation | Memory | Disk | Key env / PRAGMA | OOM / failure risk |
|---|---|---|---|---|
| **General re-ingest** (DuckDB CSV→Arrow→Lance, ~8 GB 2023 CSV) | `memory_limit='24GB'` on 32 GiB | one CSV at a time on 512 GiB ephemeral; `temp_directory` on NVMe | `parallel=false` (mandatory — quoted newlines + `null_padding`), streaming `to_arrow_reader(1048576)` | low (streaming; never materializes the 8 GB file) |
| **General BTREE build** (4 indices, 82M rows) | **48 GiB** (in-RAM sort) | — | `LANCE_BYPASS_SPILLING=true` | **medium → rising.** In-RAM sort working set ≈ rows × (key+rowid) × overhead; 82M is within 48 GiB but **approaching the ~100M threshold** where a direct-R2 index write trips R2 `400 InvalidPart`. Past that → **Volume-staged build** (`ARCHITECTURE.md` giant rule). For headroom now, build sequentially (current code does). |
| **General BITMAP build** (6 cols) | modest (roaring) | — | default | low |
| **Publish** | n/a | stage full dataset locally | boto3 uniform-part multipart | **HIGH (current cause of corruption)** — see §5 Tier 0.5 |
| **Research/ownership reindex** | 48 GiB ≫ need | — | `LANCE_BYPASS_SPILLING=true` | low (5.9M / 27k rows) |

> **Spill vs bypass tension.** The directive's "disk-spilled index rebuilds routed to NVMe" is the *out-of-core* posture; the pipeline currently sets `LANCE_BYPASS_SPILLING=true` (in-RAM, the documented fleet default at 32–64 GiB). These are opposite knobs. At general's 82M+ scale the in-RAM build is the OOM-fragile link as the dataset grows. Canonical alternative once it strains 48 GiB: **unset `LANCE_BYPASS_SPILLING`**, route Lance/DuckDB temp to NVMe, accept the slower external sort — *or* move to the Volume-staged giant pattern (which also fixes the multipart ceiling).

---

## 5. Optimization Blueprint

Sequenced by blast radius. **Tier 0/0.5 are mandatory and must precede everything** — there is no point optimizing a dataset that does not exist, and re-ingesting general through the unfixed publish reproduces the corruption.

### Tier 0.5 — Harden the publish (root-cause fix; do FIRST)
Replace the destructive `_replace_r2_prefix` with the documented **Giants — Volume-staged, append-only** pattern (`ARCHITECTURE.md`):
1. **Never wipe the live prefix before the new copy is fully staged + verified.** Stage to a sibling prefix (`…/cms_general_payments__staging/`), upload, **verify object count + per-file size == local**, then swap (delete-old-then-copy, or repoint).
2. **Per-file upload retry with backoff** (the single transient R2 error on file ~72 is what aborted run #58).
3. **Upload order: data → `_indices/` → `_transactions/` → `_versions/` last** so a manifest is never visible before its data (a half-uploaded dataset stays invisible rather than corrupt). The wipe-first behavior currently guarantees the opposite.
4. For steady-state refresh of a giant, **upload only new files** (new `_indices/<uuid>/`, new `_versions/<n>.manifest`, `_transactions/*.txn`) — do not re-push the multi-GB data tree every cycle.

### Tier 0 — Restore `cms_general_payments` (P0)
After Tier 0.5: `modal run pipelines/cms_open_payments/ingest.py::backfill --only-family general` (re-ingest 2018–2024 → local rebuild → reindex → hardened publish). Verify: `lance.dataset(uri).count_rows()` == 82,290,893 (±CMS revision) and `list_indices()` == 10. Blast-radius isolated: research/ownership are intact and untouched.

### Tier 1 — Index hygiene (cheap, no data rewrite)
- **research:** document that `principal_investigator_1_npi` is the populated resolution key and `covered_recipient_npi` is 96% null — keep both BTREEs (entity-resolution still wants the covered-recipient join when present) but reframe consumers' primary lookup. *(Optional)* add **BITMAP `form_of_payment_or_transfer_of_value`** for cross-family parity (general indexes it). Ship via `reindex_family` — index-only, never touches data fragments.
- **ownership:** already optimal. No-op.

### Tier 2 — Single append-only rewrite per family (structural optimum)
Because the SoR is immutable and the publish path is wipe-and-reupload, batch **all** transform-side changes into one `_build_sql` projection → one `lance.write_dataset(mode="overwrite")` → full reindex → one hardened publish. Fold:
1. **Drop dead/constant columns** — the ~15 fully-null research cols + ~10 general cols; `delay_in_publication_indicator`; `program_year` (keep typed `payment_year`); `payment_publication_date` **iff** the overwrite single-snapshot model is retained (see fork). *Win: scan width + schema legibility, ~0 disk (Lance already compresses these).*
2. **Null the `"N/A"` sentinel** — extend the transform to `nullif(nullif(trim(x),''),'N/A')` on `associated_device_or_medical_supply_pdi_*`. Restores true null density / cardinality.
3. **Sanitize `date_of_payment`** — flag/drop rows with `date_of_payment < DATE '2013-01-01'` (Open Payments inception; `0002-11-30` is impossible). Removes the BTREE zone-map dirt floor.
4. **Normalize geography** — `upper(trim(recipient_state))`, map full names → USPS, null/flag non-US. Collapses 60–67 → ~51 clean codes; tightens by-state analytics and the BITMAP.
5. **(Optional) `record_id` `string → int64`** — CMS surrogate is numeric, no leading zeros; halves the PK column and speeds the BTREE. NPIs stay VARCHAR (leading-zero safety). *Flag for explicit decision.*
6. **Impose clustering on the resolution key** — `ORDER BY covered_recipient_npi` (general) / `principal_investigator_1_npi` (research) before write. Today an NPI lands in every year-fragment (measured: BTREE point lookup hits 7–8/8 fragments → zero file pruning). Clustering tightens per-fragment zone maps so by-NPI lookups prune whole `.lance` files. **This is the only genuine re-sort** — an external sort bounded by **disk, not RAM**: pin `temp_directory`/Lance temp to NVMe, run isolated from the standard quarterly refresh. *(Trade-off: it fights the append-per-year topology — re-evaluate per refresh; lower priority than Tiers 0–1.)*

**Architectural fork (resolve before Tier 2 step 1).** `payment_publication_date` / `source_file` / `ingested_at` are dead weight **only** under the current `overwrite` single-snapshot model. If the SoR is meant to accumulate CMS vintages (annual republish + late submissions — which is why this is scheduled *quarterly*), `payment_publication_date` becomes the **vintage discriminator** and must stay. Resolve the model first; demote to metadata only if overwrite stays.

### Not required
- **Compaction.** research (8 frags) / ownership (7 frags) are append-per-year, **0 tombstones**, large fragments — topology is already optimal. Tier 2 is a *clustering + hygiene* rewrite, **not** a fragmentation remedy. General needs a *rebuild* because it is broken, **not** because it is fragmented. Do not frame any of this as compaction debt.
- **Dictionary/enum recasts for storage.** Lance v2.1 already RLE/dictionary-encodes low-cardinality strings physically (measured 2.99× on an all-string research schema). Re-typing categoricals buys negligible disk; the value is the **BITMAP indices (already present)**, not the logical type. Do not over-invest here.

**Sequence:** Tier 0.5 (harden publish) → Tier 0 (restore general) → Tier 1 (research index reframe/parity) → resolve the overwrite/vintage fork → one Tier 2 rewrite per family folding steps 1–6 → full reindex → one hardened publish.

---

### Appendix — Provenance
- **Telemetry:** `pylance 7.0.0` (`count_rows`, `get_fragments`, `list_indices`, `LanceFileReader.metadata`), R2 `list_objects_v2` byte census, `duckdb 1.5.3` streaming aggregates (null exact; NDV via HLL; decoded via Arrow batch `nbytes`), Lance `analyze_plan` for pushdown.
- **Read path:** `lance.dataset(uri, storage_options={aws_*…, region=auto, virtual_hosted_style_request=false})` against R2; research + ownership staged locally (boto3) for multi-pass profiling; general read from data-file footers + a 1.8M-row / 6-fragment sample (it has no manifest to open).
- **Ledger:** `ops.cms_open_payments_runs` (run #58, `partial`; general `phase=publish` → `error`).
- **No dataset mutation occurred.** No DDL, no index ops, no writes to any `cms_*` prefix. Credentials injected via `doppler run --project core-x --config prd`; no secret values were persisted.
