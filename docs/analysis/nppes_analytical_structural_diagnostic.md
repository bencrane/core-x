# NPPES Analytical Layer — Lance Structural & Compute Diagnostic

**Targets:** `s3://data-sink/active/nppes_provider/snapshot=2026-05/` · `…/nppes_provider_taxonomy/…` · `…/nppes_provider_identifier/…` (Gen-3 derived serving layer, R2)
**Mode:** Read-only, first-principles. Zero DDL / zero mutation / zero index ops. Assessed on mathematical + physical structure alone, independent of every downstream consumer.
**Date:** 2026-06-06 · **Vintage:** single snapshot `snapshot=2026-05` (only partition present for all three datasets; the 2026-06 raw monthly has not yet landed). Derived from raw SoR `nppes/snapshot=2026-05` (`source_version=4`).
**Method:** `pylance 7.0.0` manifest/fragment/index introspection (`count_rows`, `get_fragments`→`physical_rows`/`count_rows`/`deletion_file`, `list_indices`, `stats.index_stats`, `stats.dataset_stats`) + R2 `ListObjects` byte census + `duckdb 1.5.3` over the registered `LanceDataset` (one full per-column null+NDV pass per table; one exact-distinct pass on the load-bearing categoricals) + `lance` scanner `analyze_plan()` pushdown battery. NDV is **exact** where stated; otherwise HLL (`approx_count_distinct`), which **over-estimates at the top of the range** — measured `npi` HLL 10.92M vs 9.55M true = **+14.3%**, and `taxonomy_code` HLL 1,104 vs **873 exact = +26.5%**. High-card NDV is indicative; categorical NDV is exact.
**Companion:** reverses every defect catalogued in [`nppes_structural_diagnostic.md`](nppes_structural_diagnostic.md); built per [`nppes_analytical_implementation_plan.md`](nppes_analytical_implementation_plan.md).

---

## 1. Headline Posture

**Physically optimal and access-correct — the photographic negative of the raw layer. The only first-principles headroom is the inverse of the raw layer's problem: this layer is now over-indexed on its non-clustered axes, carrying ~150 MiB of scalar index that delivers row-selection but no fragment pruning.**

Where the raw NPPES snapshot was *physically pristine but access-narrow and under-indexed* (2.1% index:data, `npi` unclustered, dates-as-strings, specialty shattered across 15 columns), the derived layer inverts all four defects and is structurally clean on every physical axis:

- **Zero tombstones, zero small files, all three datasets.** `num_deleted_rows=0`, `num_small_files=0` everywhere. Fragments sit at the 1,048,576-row cap with a single tail each (provider 10 frags, taxonomy 12, identifier 3). **No read-amplification, no compaction debt** — and the per-snapshot overwrite model means fragmentation can never accrue.
- **100% index coverage.** Every one of the 19 committed scalar indices (provider 11, taxonomy 4, identifier 4) reports `num_unindexed_rows=0` over **all** fragments. The built plan matches the directive `INDEX_PLAN` exactly — no missing index, no orphan index, no type mismatch.
- **The temporal axis is fixed.** Five `date32[day]` columns (`enumeration_date`, `last_update_date`, `deactivation_date`, `reactivation_date`, `certification_date`); range filters push down and return correct counts (`enumeration_date >= 2020-01-01` → 3,292,670, vs the raw layer's silent 0). Min/max bounds are clean (`enumeration_date` 2005-05-23 → 2026-05-09).
- **The specialty axis is fixed and it is the structural win of the build.** Taxonomy is a long child table with a scalar `BITMAP(taxonomy_code)`; sorted `(taxonomy_code, npi)`, a single-specialty filter **prunes to 2 of 12 fragments, reads 4.87 MB, 2 IOPs** (the highest-volume code `106S00000X`, 586,363 rows; rarer codes hit 1 fragment). Secondary specialties are included (every populated slot → a row).
- **`npi` is clustered where it is the join key.** Provider and identifier are `ORDER BY npi`; a 1,000-`npi` batch prefilter prunes to **3 of 10** provider fragments (11.42 KB, 3 IOPs).

**The defect class is now the opposite of the raw layer's: index weight, not index absence.** Index:data ratio has swung from the raw layer's 0.0214× to **0.318× (provider), 0.800× (taxonomy), 1.178× (identifier — index exceeds data)**. A serving layer *should* be index-heavy, so most of this is correct spend. But three allocations do not earn their keep on first principles:

1. **The 147 MiB `npi` BTREE on `nppes_provider_taxonomy` is the layer's single largest index and delivers zero fragment pruning.** Because the table is `(taxonomy_code, npi)`-clustered, `npi` is scattered across all 12 fragments: a 1,000-`npi` batch lookup scans **12/12 fragments, 19.97 MB, 320 IOPs** — a ~1,750× bytes and ~107× IOP penalty vs the same lookup on the npi-clustered provider table. The BTREE is 71% of the table's *data* size and 90% of its index budget, for a non-pruning access path. **This is the highest-value scrutiny target in the layer.**
2. **`enumeration_year` BITMAP duplicates the `enumeration_date` BTREE axis.** `enumeration_year` (NDV 22) is a pure function of `enumeration_date` (date32 BTREE); `year = 2024` ≡ `enumeration_date ∈ [2024-01-01, 2025-01-01)`, which the date BTREE already serves. Two indices on one temporal axis.
3. **The `npi` BTREEs on the clustered children are largely redundant with their own zone-maps.** On `nppes_provider_identifier` (3 fragments, npi-sorted) and for the join-back path generally, the sort already yields npi zone-map pruning; the explicit BTREE adds row-level take but little prune the clustering doesn't supply.

Everything else is correct: typing is tight (32 string / 5 date32 / 1 int16 / 1 bool on provider — no wide floats, no string-typed dates, no string-typed flags worth recasting since Lance dict-encodes them to ~0); all BITMAP choices are validated by **exact** cardinality (every flag NDV=2, every state 59–61, `taxonomy_code` 873, `enumeration_year` 22); the clustering choices are validated by measured pruning.

**Verdict: ship-grade and structurally optimal on the physical/storage axis; no compaction or recast is warranted. The remaining optimization is index right-sizing — trimming ~150–180 MiB of low-yield scalar index — not a data rewrite.** The build correctly solved the raw layer's access problem; it slightly over-corrected on index spend.

---

## 2. Telemetry Grid

| Metric | `nppes_provider` | `nppes_provider_taxonomy` | `nppes_provider_identifier` |
|---|---:|---:|---:|
| Grain | 1 / NPI | 1 / (NPI, taxonomy slot) | 1 / (NPI, identifier slot) |
| Sort / clustering | `ORDER BY npi` | `ORDER BY taxonomy_code, npi` | `ORDER BY npi` |
| **Logical rows** | **9,551,447** | **11,952,809** | **2,759,800** |
| Physical rows (pre-deletion) | 9,551,447 | 11,952,809 | 2,759,800 |
| **Deleted / tombstoned** | **0 (0.0000%)** | **0 (0.0000%)** | **0 (0.0000%)** |
| Small files (`num_small_files`) | **0** | **0** | **0** |
| Columns | 39 (32 str·5 date32·1 int16·1 bool) | 8 (6 str·1 int8·1 bool) | 7 (6 str·1 int8) |
| Fragments | **10** | **12** | **3** |
| Rows/frag — min · avg · max | 114,263 · 955,145 · 1,048,576 | 418,473 · 996,067 · 1,048,576 | 662,648 · 919,933 · 1,048,576 |
| Data on disk (R2, compressed) | 1,499,998,537 B · **1.3969 GiB** | 216,665,874 B · **206.63 MiB** | 61,078,689 B · **58.25 MiB** |
| Data files | 10 (9 @ ~157 MiB + 17.82 MiB tail) | 12 (~17 MiB ea) | 3 (~22 MiB ×2 + 13.97 MiB) |
| Avg bytes / fragment (data) | 143.05 MiB | 17.22 MiB | 19.42 MiB |
| On-disk bytes / row (data) | 157.04 B | 18.13 B | 22.13 B |
| **Est. uncompressed (Arrow, sampled 200k)** | 312.3 B/row · **2.78 GiB** | 59.6 B/row · **678.95 MiB** | 60.8 B/row · **160.11 MiB** |
| **Compression (uncompressed ÷ on-disk)** | **1.99×** | **3.29×** | **2.75×** |
| **Scalar-index storage (R2)** | 477,008,377 B · **454.91 MiB** (17 files / 11 idx) | 173,230,008 B · **165.21 MiB** (5 / 4) | 71,945,368 B · **68.61 MiB** (6 / 4) |
| **Index : data ratio** | **0.318×** | **0.800×** | **1.178×** |
| Manifests + transactions | 64,876 B (14) + 5,978 B (13) | 14,832 B (7) + 2,579 B (6) | 8,413 B (7) + 1,657 B (6) |
| **Total R2 footprint** | 1,977,077,768 B · **1.84 GiB** (54 obj) | 389,913,293 B · **371.85 MiB** (30 obj) | 133,034,127 B · **126.87 MiB** (22 obj) |
| Storage format version | **2.1** (current) | **2.1** | **2.1** |
| Dataset version | 13 (1 write + 11 idx + 1 meta) | 6 (1 + 4 + 1) | 6 (1 + 4 + 1) |

**Layer totals:** 24,264,056 rows · data **1.6556 GiB** · index **688.73 MiB** · total footprint **2.328 GiB** · overall index:data **0.406×** · est. uncompressed **≈ 3.60 GiB**.

> **First-principles scale note.** The entire derived serving layer is **≈3.60 GiB uncompressed** — it fits in RAM with room to spare. The "scale exceeds container RAM" reality applies to the **raw** SoR (11.46 GiB on disk; decoded sort spill 25–35 GiB) and to the **build**, *not* to querying this layer. The layer is deliberately right-sized so reads are RAM-resident; out-of-core management is a build-time concern only (§4).

**Per-fragment integrity:** every fragment is a single data file, zero deletion files, physical_rows == live_rows across all 25 fragments. Topology is at the theoretical optimum (N−1 fragments at the row cap + one tail).

---

## 3. Schema & Index Ledger

NDV **exact** unless marked *(HLL)*. "Optimal" = assessed purely on measured cardinality + structural role + the table's clustering. Null floors trace to cohort structure: provider's **3.5944%** floor = the 343,321 deactivated-stub cohort (kept, `is_active=false`, per D5); the 23.78% / 79.82% bands = the individual/organization split (orgs have no `last_name`; individuals no `organization_name`).

### 3.1 `nppes_provider` — 9,551,447 rows · 11 indices · 454.91 MiB index

| Column | Type | Null % | NDV | Index (size) | Prunes frags? | Verdict |
|---|---|---:|---:|:--|:--|:--|
| `npi` | `string` | 0.000 | 9,551,447 *(unique, G2)* | **BTREE** 114.32 MiB | **✅ yes (3/10)** | ✅ PK + universal join key; clustered → prunes |
| `last_name` | `string` | 23.778 | 801,180 *(HLL)* | **BTREE** 56.12 MiB | no | ✅ individual resolution blocking key |
| `practice_address_line1` | `string` | 3.594 | 2,911,466 *(HLL)* | **BTREE** 120.69 MiB | no | ✅ geo-join/address blocking (largest index; high-card by nature) |
| `practice_zip5` | `string` | 3.608 | 43,671 *(HLL)* | **BTREE** 41.55 MiB | no | ✅ zip blocking / range |
| `enumeration_date` | `date32` | 3.594 | 6,791 *(HLL)* | **BTREE** 41.07 MiB | no | ✅ temporal range (2005-05-23 → 2026-05-09) |
| `last_update_date` | `date32` | 3.594 | 6,336 *(HLL)* | **BTREE** 40.83 MiB | no | ✅ delta-scan key (2007-07-08 → 2026-05-11) |
| `primary_taxonomy_code` | `string` | 3.594 | **871** | **BITMAP** 18.35 MiB | no | ✅ correct; at the BITMAP/BTREE seam (≈871), access is categorical-eq → BITMAP right |
| `practice_state` | `string` | 3.645 | **61** | **BITMAP** 15.32 MiB | no | ✅ USPS-clean (≤63, G9); type-correct |
| `enumeration_year` | `int16` | 3.594 | **22** | **BITMAP** 3.26 MiB | no | ⚠️ **redundant** with `enumeration_date` BTREE (year = date range) |
| `entity_type_code` | `string` | 3.594 | **2** | **BITMAP** 2.41 MiB | no | ✅ individual/org partition key |
| `is_active` | `bool` | 0.000 | **2** | **BITMAP** 0.99 MiB | no | ✅ active/deactivated filter |
| `entity_type` | `string` | 3.594 | **2** | — | — | decoded convenience; covered by `entity_type_code` BITMAP |
| `mailing_state` | `string` | 3.649 | **61** | — | — | ⚠️ unindexed; (opt.) BITMAP if mailing-geo filtering is hot |
| `practice_zip` | `string` | 3.595 | 2,110,286 *(HLL)* | — | — | full postal; `practice_zip5` carries the indexed form |
| `practice_city` | `string` | 3.594 | 30,494 *(HLL)* | — | — | (opt.) BITMAP-borderline / BTREE if city filtering is hot |
| `practice_country` | `string` | 3.594 | **142** | — | — | low analytical value |
| `organization_name` | `string` | 79.817 | 1,149,597 *(HLL)* | — | — | ⚠️ org resolution blocking key — unindexed (see §5) |
| `first_name` | `string` | 23.778 | 442,840 *(HLL)* | — | — | resolution support |
| `provider_name` | `string` | 3.595 | 9,011,202 *(HLL)* | — | — | composed display name |
| `credential` | `string` | 50.005 | 178,173 *(HLL)* | — | — | — |
| `sex_code` | `string` | 23.778 | **4** | — | — | F/M/X/U; low value |
| `is_sole_proprietor` | `string` | 23.778 | **3** | — | — | flag; Lance dict-encodes → no recast |
| `is_organization_subpart` | `string` | 79.817 | **2** | — | — | flag |
| `middle_name`/`name_prefix`/`name_suffix` | `string` | 58.9 / 78.2 / 98.8 | 251,658 / 6 / 15 | — | — | — |
| `practice_address_line2`/`practice_city`/`practice_phone`/`practice_fax` | `string` | 85.8 / 3.6 / 3.6 / 63.4 | — | — | — | descriptive |
| `mailing_city`/`mailing_zip5` | `string` | 3.595 / 3.621 | 34,296 / 42,399 | — | — | descriptive |
| `deactivation_date` | `date32` | 96.214 | 5,117 *(HLL)* | — | — | (361,585 populated; min 2005-05-23) |
| `reactivation_date` | `date32` | 99.809 | 3,171 *(HLL)* | — | — | (18,267 populated) |
| `certification_date` | `date32` | 48.942 | 2,347 *(HLL)* | — | — | — |
| `authorized_official_*` (3) | `string` | 79.817 | 329k / 113k / 87k | — | — | org-only |
| `parent_organization_lbn` | `string` | 98.367 | 56,985 *(HLL)* | — | — | — |
| `snapshot_month` | `string` | 0.000 | **1** *(`'2026-05'`)* | — | — | vintage key (constant in-partition; cross-month UNION discriminator) |

**Index mismatches:** none. **Coverage:** 11/11 indices cover 10/10 fragments, 0 unindexed rows.

### 3.2 `nppes_provider_taxonomy` — 11,952,809 rows · 4 indices · 165.21 MiB index

| Column | Type | Null % | NDV | Index (size) | Prunes frags? | Verdict |
|---|---|---:|---:|:--|:--|:--|
| `taxonomy_code` | `string` | 0.000 | **873** | **BITMAP** **1.10 MiB** | **✅ yes (2/12)** | ✅ **the load-bearing index**; clustered → tiny + prunes |
| `npi` | `string` | 0.000 | 10,510,044 *(HLL)* | **BTREE** **147.43 MiB** | **❌ no (12/12)** | ⚠️ **largest index, no prune** — scrutinize (see §5.1) |
| `is_primary` | `bool` | 0.000 | **2** | **BITMAP** 2.18 MiB | no | ✅ primary-specialty filter (9,208,126 true) |
| `license_state` | `string` | 33.762 | **59** | **BITMAP** 14.49 MiB | no | ✅ USPS-clean; type-correct |
| `license_number` | `string` | 37.049 | 5,570,359 *(HLL)* | — | — | high-card; unindexed (license lookup not a stated path) |
| `taxonomy_rank` | `int8` | 0.000 | **15** | — | — | slot 1–15; correct narrow type |
| `taxonomy_group` | `string` | 87.006 | **3** | — | — | — |
| `snapshot_month` | `string` | 0.000 | **1** | — | — | vintage |

> **The clustering dividend, in two numbers.** `taxonomy_code` (NDV 873) BITMAP = **1.10 MiB**; `primary_taxonomy_code` (NDV 871) BITMAP on the npi-clustered provider table = **18.35 MiB**. Near-identical cardinality, **~17× size difference** — because clustering the table by the code makes each value's rows contiguous, so its roaring bitmap is a dense run that compresses to almost nothing. The same physics makes the *non*-clustered `npi` BTREE here expensive (§5.1).

**Coverage:** 4/4 indices cover 12/12 fragments, 0 unindexed.

### 3.3 `nppes_provider_identifier` — 2,759,800 rows · 4 indices · 68.61 MiB index (> data)

| Column | Type | Null % | NDV | Index (size) | Prunes frags? | Verdict |
|---|---|---:|---:|:--|:--|:--|
| `npi` | `string` | 0.000 | 1,608,391 *(HLL)* | **BTREE** 30.51 MiB | partial (3/3, small) | ⚠️ ~redundant with npi zone-maps at 3 frags (§5.3) |
| `identifier_value` | `string` | 0.000 | 2,444,311 *(HLL)* | **BTREE** 32.75 MiB | no | ✅ *only if* reverse lookup (external-ID → provider) is real |
| `identifier_type_code` | `string` | 0.000 | **2** | **BITMAP** 0.67 MiB | no | ✅ Medicaid/Medicare-class filter |
| `identifier_state` | `string` | 9.438 | **59** | **BITMAP** 4.68 MiB | no | ✅ USPS-clean |
| `identifier_issuer` | `string` | 50.740 | 179,666 *(HLL)* | — | — | high-card; unindexed |
| `identifier_rank` | `int8` | 0.000 | **50** | — | — | slot 1–50; correct narrow type |
| `snapshot_month` | `string` | 0.000 | **1** | — | — | vintage |

**Coverage:** 4/4 indices cover 3/3 fragments, 0 unindexed. **Index (68.61 MiB) exceeds data (58.25 MiB)** — driven entirely by the two high-card BTREEs (npi + identifier_value = 63.26 MiB).

---

## 4. Execution Runtime Specs

Three workloads, three configs. The deployed worker envelope (`materialize_analytical.py`): **32 GiB RAM · 8 vCPU · 512 GiB ephemeral disk** (Modal `memory=32768, cpu=8.0, ephemeral_disk=524288`). **There is no `/mnt/nvme` mount** — all scratch/spill/stage routes to `/tmp` on the 512 GiB ephemeral disk.

### A. Read / query (the common path — fits in RAM, out-of-core NOT required)

The whole layer is ≈3.60 GiB uncompressed; a query container does not need the build envelope.

```sql
PRAGMA threads=8;                         -- = vCPU
SET memory_limit='8GB';                   -- ample; layer is RAM-resident (do not over-provision)
SET temp_directory='/tmp/duckdb_spill';   -- only joins/aggregates of last resort spill; mkdir at entry
SET preserve_insertion_order=false;       -- streaming aggregates; lowers peak RSS
```
```bash
export LANCE_MEM_POOL_SIZE=2147483648     # 2 GiB Lance IO/buffer pool — sufficient for these dataset sizes
export TMPDIR=/tmp/duckdb_spill
```
- **Push predicates into the Lance scanner**, not a post-materialization DuckDB filter: `ds.scanner(columns=[…needed…], filter="taxonomy_code='X'", batch_size=131072)`. Verified: this fires the BITMAP/BTREE prefilter; a `SELECT * … WHERE` that leans on replacement-scan pushdown can silently degrade to a full column scan.
- **Know which key prunes (the §6 result):** on `nppes_provider`/`nppes_provider_identifier`, push `npi` to prune fragments; on `nppes_provider_taxonomy`, push `taxonomy_code` to prune (an `npi` filter there reads all 12 fragments — route batch `npi`→taxonomy through the provider join, §5.1).
- **Specialty×geo** is `taxonomy ⋈ provider`: filter `taxonomy_code` (prunes taxonomy to ≤2 frags) → dynamic `npi` range-prune on provider (npi-sorted). Not a two-sided npi-BTREE take.

### B. Reindex (scalar-index rebuild — the only index-mutation path)

```bash
export LANCE_BYPASS_SPILLING=true          # REQUIRED for the high-card string BTREE trains
                                           # (practice_address_line1 2.9M, identifier_value 2.4M, last_name 0.8M).
                                           # Lance's bounded spill sorter under-sizes and OOMs on these (lance#2650);
                                           # in-RAM sort of one ~30-char column is <1 GiB ≪ 32 GiB; trains run sequentially.
export LANCE_MEM_POOL_SIZE=4294967296      # 4 GiB; the build currently sets neither — make it explicit
```
- **Transport is non-negotiable:** build the dataset + indices on **local** disk → **boto3 uniform-part publish**. Building indices with `storage_options` straight to R2 trips the multipart rule (`400 InvalidPart`) once a BTREE `page_data.lance` (here 114–147 MB) escalates part size mid-upload. (Already the pipeline's pattern.)
- `ephemeral_disk ≥ 8 GiB` for an index-only rebuild (data ≤1.4 GiB + indices ≤455 MiB + scratch).

### C. Mutate / rebuild (full overwrite — clustering sorts + recast; the only out-of-core workload)

```sql
PRAGMA threads=8;
SET memory_limit='20GB';                   -- ~62% of 32 GiB; leaves RAM for the Lance pool + Arrow + OS
SET temp_directory='/tmp/nppes_analytical/duck_spill';   -- ORDER BY spills the DECODED payload → ephemeral disk
SET max_temp_directory_size='128GB';       -- sized defensively; the three sorts are the only spill-heavy steps
SET preserve_insertion_order=true;         -- carry the ORDER BY into the Lance write (clustering)
```
- The heaviest single step is the **taxonomy `ORDER BY taxonomy_code, npi`** on 11.95M rows (decoded ≈679 MiB → modest spill) plus the high-card string BTREE trains. All three derived tables are far smaller than the raw SoR, so the build is comfortable in the 32 GiB / 512 GiB envelope — **no OOM risk at current scale.**
- Streaming write via `to_arrow_reader(131072)` (bounded write RSS); `max_rows_per_file=1048576`, `max_bytes_per_file=90 GiB`, `data_storage_version='2.1'`.
- **Blast radius:** a full wipe-and-republish of the month's three prefixes (`_replace_r2_prefix`, idempotent). Run isolated from the raw monthly capture; the materializer is a separate Modal app reading raw read-only — a failure here cannot corrupt the raw SoR.

> **Scaling cliff to watch (raw-diag §6.6).** `LANCE_BYPASS_SPILLING=true` trades OOM-safety for an in-RAM sort. Safe at today's row counts. It becomes the first thing to blow the 32 GiB envelope **only if** the layer ever switches from per-snapshot-overwrite to cross-month *append* (the npi / identifier_value / practice_address_line1 trains would grow unbounded). Currently per-snapshot → bounded.

---

## 5. Optimization Blueprint

All physical axes are already optimal — **no compaction, no recast, no re-sort is warranted.** The headroom is index right-sizing on the non-clustered axes. Every item below is **index-only** (the §4-B reindex path: rebuild scalar indices on a local stage → boto3 publish, no data-fragment rewrite, isolated from the data plane). Sequenced by value-to-cost.

### Tier 1 — Index right-sizing (decoupled, cheap, reversible)

1. **Resolve the 147 MiB `npi` BTREE on `nppes_provider_taxonomy` against its actual access pattern.** Measured: it provides row-selection but **zero fragment pruning** (12/12, 19.97 MB, 320 IOPs for 1,000 npis). Decide by the dominant `npi`→taxonomy pattern:
   - *If single-NPI reverse lookup dominates* ("show this provider's specialties" — provider-detail path): **keep it.** A single-npi take spanning ≤12 fragments is acceptable; the BTREE saves a full column scan.
   - *If batch `npi`→taxonomy dominates* (enrichment/resolution joins): the BTREE does not help — route those through the provider table (npi-clustered, prunes 3/10) and join, **or** drop the taxonomy `npi` BTREE and rely on the join-back mechanism (taxonomy_code BITMAP prune → dynamic npi range-prune on provider), which never reads taxonomy by npi. Dropping it reclaims **147 MiB (90% of this table's index budget)**.
   - First-principles default: **keep for single-NPI, but never use it for batch** — and do not let a future batch path silently pay the 320-IOP penalty.
2. **Drop `enumeration_year` BITMAP (reclaim 3.26 MiB) — or keep as a deliberate exact-equality fast path.** It double-indexes the `enumeration_date` axis; `WHERE enumeration_year = Y` is `enumeration_date ∈ [Y-01-01, (Y+1)-01-01)`, served by the date BTREE. Low cost, so this is a cleanliness call, not a pressure point; drop unless `enumeration_year =` is a proven hot predicate the planner won't rewrite to a range.
3. **Reassess the `npi` BTREE on `nppes_provider_identifier` (30.51 MiB).** At 3 npi-sorted fragments, npi zone-maps already prune (3/3, 13 KB, 3 IOPs — the index adds little the clustering doesn't). Keep only if single-npi identifier lookup needs sub-fragment take; otherwise it is the cheapest 30 MiB to reclaim. Keep `identifier_value` BTREE **iff** reverse lookup (external ID → provider) is a real path; if not, that is another 32.75 MiB.

### Tier 2 — Optional coverage additions (only if the access pattern is proven)

4. **`BTREE(organization_name)` on `nppes_provider`** (NDV ~1.15M, 79.8% null) — the org-side entity-resolution blocking key, the symmetric partner to the existing `last_name` BTREE. Add only if org-name resolution is a hot path; it will cost ~50–60 MiB (last_name BTREE is 56 MiB at comparable card).
5. *(Opt.)* `BITMAP(mailing_state)` (NDV 61) if mailing-geo filtering becomes hot — mirrors `practice_state`.

### Explicitly NOT warranted

- **Compaction** — 0 tombstones, 0 small files, fragments at cap + single tail across all three; topology is at the optimum and the overwrite model keeps it there.
- **Schema recast** — typing is already tight: dates are `date32`, ranks are `int8`, `enumeration_year` is `int16`, flags are dict-encoded strings (recast yields ~0 on disk). No wide floats exist.
- **Re-clustering** — the sort choices are validated by measured pruning (§6). You cluster on one axis; provider/identifier correctly chose `npi` (universal join key), taxonomy correctly chose `taxonomy_code` (the #1 analytical axis, D2). A *second* npi-clustered taxonomy replica would only be justified if batch `npi`→taxonomy proves to be a dominant, latency-critical path (item 1).

---

## 6. Compute Engine Integration (DuckDB ⋈ Lance) — Empirical

§4 specified configs from first principles; this section **measures** them. Every figure is a **cold** R2 read from a remote (laptop) vantage via `lance` scanner `analyze_plan()`; absolute latencies are egress-bound and environment-relative (in-region Modal↔R2 they are sub-second), so the **fragment-prune counts, bytes_read, and IOPs are the load-bearing results — not wall-clock ms.**

### 6.1 The clustered key prunes; the non-clustered key does not — the whole architecture in one table

`npi IN (1000 ids spanning head/mid/tail of the sorted scan)`:

| Table | Sort order | fragments_scanned | rows | bytes_read | IOPs | uses index |
|---|:--|---:|---:|---:|---:|:--|
| `nppes_provider` | `npi` | **3 / 10** | 1.00 K | **11.42 KB** | **3** | ✓ |
| `nppes_provider_taxonomy` | `taxonomy_code, npi` | **12 / 12** | 3.00 K | **19.97 MB** | **320** | ✓ |
| `nppes_provider_identifier` | `npi` | **3 / 3** | 1.00 K | **13.32 KB** | **3** | ✓ |

The taxonomy `npi` lookup reads **~1,750× the bytes** and **~107× the IOPs** of the provider lookup for the same predicate — the exact cost of indexing a scattered column. This is the empirical basis for §5.1.

### 6.2 Single-predicate pushdown — index used everywhere; pruning only on the clustered axis

| Table | Predicate | Index | frags | rows | bytes_read | IOPs | cold ms |
|---|:--|:--|---:|---:|---:|---:|---:|
| taxonomy | `taxonomy_code='106S00000X'` | Bitmap | **2 / 12** | 586.4 K | **4.87 MB** | **2** | 793 |
| identifier | `identifier_type_code='05'` | Bitmap | 3 / 3 | 1.40 M | 22.25 MB | 6 | 534 |
| taxonomy | `is_primary=true` | Bitmap | 12 / 12 | 9.21 M | 100.7 MB | 23 | 1,962 |
| provider | `practice_state='TX'` | Bitmap | 10 / 10 | 592.4 K | 78.81 MB | 19 | 2,197 |
| provider | `entity_type_code='1'` | Bitmap | 10 / 10 | 7.28 M | 78.81 MB | 19 | 7,062 |
| provider | `enumeration_date >= 2020-01-01` | BTree-range | 10 / 10 | 3.29 M | 29.66 MB | 97 | 9,018 |

**Findings.** (1) Every predicate fires a `ScalarIndexQuery` — pushdown across the DuckDB↔Lance boundary is healthy on all 19 indices. (2) **Fragment pruning happens only on the clustered axis**: `taxonomy_code` prunes taxonomy to 2/12 (4.87 MB); every non-clustered predicate uses its index for *row selection* but the matched rows are scattered across all fragments, so I/O touches them all. This is correct and unavoidable (one cluster axis per dataset) — the indices still avoid full-column materialization. (3) Boolean/low-selectivity predicates (`is_primary=true` = 77% of rows; `entity_type_code='1'` = 7.28M) legitimately read everything; the index adds nothing a scan wouldn't, but costs nothing either.

### 6.3 Correctness reconciliation (against the build's §8 gate, independently re-measured)

| Quantity | Measured here | Gate target | ✓ |
|---|---:|---:|:--|
| provider rows | 9,551,447 | 9,551,447 (G1) | ✓ |
| `taxonomy_code='106S00000X'` rows | 586,363 *(586.4 K)* | 582,200 distinct providers (G3) | ✓ (count(*) ≥ distinct; multi-slot dupes expected) |
| `is_primary=true` | 9,208,126 *(9.21 M)* | 9,208,126 (G4) | ✓ |
| `enumeration_date ≥ 2020-01-01` | 3,292,670 *(3.29 M)* | 3,292,670 (G5) | ✓ |
| `practice_state` distinct (clean) | **61** | ≤63 (G9) | ✓ |
| deactivated cohort (provider null floor) | 343,321 | 343,321 (G11) | ✓ |

Every invariant the build asserts reproduces from a cold, independent read. The data is correct; the diagnostic's findings are purely about index economics, not correctness.

### 6.4 Out-of-core reality — confirms §4

The full per-column null+NDV passes completed in **16.9 s (provider, 39 cols), 3.9 s (taxonomy), 0.9 s (identifier)** inside a small `memory_limit` with zero spill — aggregation is not a memory risk at this scale. The pressure lives entirely in the **build's `ORDER BY` sorts + high-card string BTREE trains** (§4-C), the only steps that touch the decoded payload. Querying this layer is RAM-resident; the "exceeds container RAM" mandate is a build-time constraint, satisfied by the existing config.

---

## Appendix — Provenance

- **Telemetry:** `pylance 7.0.0` (`count_rows`, `get_fragments`→`physical_rows`/`count_rows`/`deletion_file`, `list_indices`→name/type/fields/uuid/fragment_ids, `stats.index_stats`→`num_indexed_rows`/`num_unindexed_rows`, `stats.dataset_stats`→`num_deleted_rows`/`num_small_files`); R2 `list_objects_v2` byte census (data / `_indices`/uuid / `_versions` / `_transactions`); `duckdb 1.5.3` over `con.register(name, lance.dataset(...))` — one full per-column null+NDV pass per table, one exact-distinct pass on the load-bearing categoricals; `lance` scanner `analyze_plan()` pushdown battery (single-predicate + 1,000-`npi` batch prune).
- **Read path:** `lance.dataset(uri, storage_options=…)` against R2 (`endpoint` + `region=auto`), secrets via `doppler run --project core-x --config prd`. Toolchain pinned to the Modal image (`pylance 7.0.0`, `duckdb 1.5.3`, `pyarrow 24.0.0`) in a `uv` Python-3.12 venv.
- **NDV:** exact where stated (all BITMAP-relevant categoricals; provider `npi` uniqueness via the build's G2). HLL elsewhere — over-estimates at the top of the range (`npi` +14.3%, `taxonomy_code` +26.5% vs exact 873); high-card figures are indicative.
- **Uncompressed footprint:** Arrow `table.nbytes` on a 200,000-row scan, scaled to row count — an estimate (string-length variance), labelled as such.
- **Latencies:** cold R2 reads from one remote vantage; **ratios / prune-counts / bytes / IOPs are the result**, absolute ms are environment-relative.
- **No dataset mutation occurred.** No DDL, no index ops, no writes to R2. Read-only throughout; the only artifact produced is this document.
