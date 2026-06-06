# NPPES — Lance Structural & Compute Diagnostic

**Target:** `s3://data-sink/active/nppes/snapshot=2026-05/` (Gen-3 SoR, R2)
**Mode:** Read-only, first-principles. Zero DDL / zero mutation. Assessed independent of all downstream consumers.
**Date:** 2026-06-06 · **Vintage:** single snapshot `snapshot=2026-05` (source `NPPES_Data_Dissemination_May_2026_V2.zip`, member `npidata_pfile_20050523-20260510.csv`).
**Method:** `pylance 7.0.0` manifest/fragment/index introspection + R2 `ListObjects` byte census + `duckdb 1.5.3` streaming passes — one full 334-column null+NDV pass (9.55M rows, 121 s), one narrow exact pass (PK uniqueness, state cardinality/hygiene, low-card enumerations), one exact provenance-ballast pass. NDV via HyperLogLog (`approx_count_distinct`) **unless marked *exact***; HLL **over**estimates at the top of the cardinality range (measured `npi` HLL = 10.92M vs 9.55M true = **+14.3%**), so high-cardinality NDV is *indicative, not exact*.

---

## 1. Headline Posture

**Physically pristine; access-narrow; schema is a wide, sparse, all-VARCHAR registry whose single load-bearing axis (`npi`) is unclustered.**

Physical layout is effectively perfect. **Zero tombstones**, 10 fragments of one data file each, 9 at the exact 1,048,576-row cap plus a 114,263-row tail — **no read-amplification, no compaction debt**. Storage format is current (Lance v2.1). The ~1.5× byte spread across the nine equal-row fragments (976 MB → 1,499 MB) is **pure string-content variance, not a layout defect** — the registry's per-block fill density varies because providers populate wildly different subsets of the 334 columns. All three scalar indices fully cover all ten fragments with zero unindexed rows. **`npi` is verified exactly unique** (9,551,447 distinct = 9,551,447 rows) — PK integrity holds.

The defects are structural and access-shaped, not physical:

1. **`npi` is unclustered across fragments — the highest-value defect.** Every one of the 10 fragments spans essentially the entire issued NPI space (≈`1003xxxxxx` → `1992xxxxxx`); the disjoint-ascending test is **False**. The `npi` BTREE delivers row-level pushdown but its **fragment zone maps cannot prune a single fragment**. Because `npi` is *the* universal join key for the entire US provider graph, every point/batch resolution lookup currently fans out to all 10 fragments instead of 1.

2. **The schema is 333 `string` + 1 `timestamp` — everything is VARCHAR.** Correct for *identifiers* (NPI, license/EIN/TIN — the leading-zero/lexical-join rule), but five date columns (`provider_enumeration_date`, `last_update_date`, `npi_deactivation_date`, `npi_reactivation_date`, `certification_date`) are MM/DD/YYYY strings where `date32` would give temporal range pushdown, zone-map pruning, and ~50%+ narrowing.

3. **The categorical access axes are unindexed.** `entity_type_code` (the individual/organization partition key, NDV=2, 96.4% populated) and `healthcare_provider_taxonomy_code_1` (the primary-specialty filter, NDV=1,104, 96.4% populated) carry no index — the two clearly missing scalar indices for any analytical or segmentation access.

4. **No name/identity-resolution BTREEs.** The original index plan was geo-join-shaped (`npi` + practice address line 1 + practice state). The entity-resolution blocking keys — `provider_last_name_legal_name` (801k distinct) and `provider_organization_name_legal_business_name` (1.15M distinct) — are unindexed, so name-based resolution is a full scan.

5. **74% of columns are ≥99% empty; 3 columns are pure CMS redaction sentinels; 1 is fully dead.** 246 of 334 columns are ≥99% null — the flattened repeating-group slots (`other_provider_identifier_1..50` × 4 sub-fields = 200 cols; taxonomy/license `_1..15` groups). `employer_identification_number_ein`, `parent_organization_tin`, and `provider_other_organization_name` are constant `'<UNAVAIL>'` (redaction placeholders carrying zero information); `npi_deactivation_reason_code` is **100% null**.

6. **A coherent 343,321-row (3.594%) deactivated-NPI stub cohort** sets the null floor: these rows carry only `npi` + deactivation dates, every descriptive field null. This is why every "always-populated" column bottoms out at exactly 3.5944% null.

The dirty-data exposure is *small*: practice-state shows 1,063 distinct, but **99.954% of rows are clean `[A-Z]{2}`** — only 4,230 rows (0.046%) across 943 foreign/free-text values inflate the count. The state BITMAP is **type-correct**; it merely carries ~1,006 near-empty bitmaps.

**Index:data ratio is 0.0214× (2.1%)** — the inverse of a resolution-heavy dataset (overture ran 1.18×). NPPES is under-indexed relative to its role as the provider-graph anchor; there is abundant headroom to add the four missing scalar indices.

**Verdict:** query-ready for single-`npi` lookups and practice-address/practice-state filters; **sub-optimal for batch-`npi` joins (no fragment pruning), for `entity_type`/`taxonomy` categorical filtering and name-based resolution (unindexed), and for any temporal predicate (dates stored as strings)**; carrying 819.81 MiB of decoded per-row provenance ballast and 275 sparse repeating-group columns that belong in a nested layout.

---

## 2. Telemetry Grid

| Metric | Value |
|---|---:|
| Logical rows | **9,551,447** |
| Physical rows (pre-deletion) | 9,551,447 |
| Deleted / tombstoned rows | **0 (0.0000%)** |
| `npi` distinct (**exact**) | **9,551,447 — unique (PK integrity ✓)** |
| Columns | **334** (333 `string` + 1 `timestamp[us,UTC]`) |
| Fragments | **10** (1 data file each) |
| Rows / fragment — min · avg · max | 114,263 · 955,145 · **1,048,576** (= `max_rows_per_file` cap) |
| Data on disk (R2, compressed) | **12,300,116,994 B · 11.4554 GiB** |
| Avg bytes / fragment (data) | 1,230,011,699 B · 1.1455 GiB |
| **Byte spread, 9 equal-row fragments** | 976,136,609 → 1,499,381,539 B · **1.54× (content variance, not a defect)** |
| On-disk bytes / row | 1,287.8 B |
| Scalar-index storage (R2) | **262,686,372 B · 250.52 MiB** (5 files, 3 indices) |
| **Index : data ratio** | **0.0214× (2.1%)** |
| Manifests + transactions | 166,757 B (5) + 33,618 B (4) |
| **Total R2 footprint** | **12,563,003,741 B · 11.700 GiB (24 objects)** |
| **Per-row provenance ballast (decoded)** | **859,630,230 B · 819.81 MiB** (decode-time cost; compresses to ~0 on disk) |
| Storage format version | **2.1** (current) |
| Dataset version | 4 (= 1 write + 3 index commits; single snapshot) |

**Per-index storage** (R2 byte census):

| Index | Type | Files | Bytes | MiB |
|---|:--|---:|---:|---:|
| `npi_idx` | BTree | 2 | 120,261,746 | 114.69 |
| `provider_first_line_business_practice_location_address_idx` | BTree | 2 | 126,404,634 | 120.55 |
| `provider_business_practice_location_address_state_name_idx` | Bitmap | 1 | 16,019,992 | 15.28 |

**Null-density histogram** (334 columns):

| Null band | Columns | Note |
|---|---:|---|
| 0% (fully populated) | **5** | `npi` + the 4 per-row provenance constants only |
| <1% | 0 | — |
| 1–50% | 24 | the descriptive core (floor at 3.594%) |
| 50–99% | 58 | secondary fields, slots 1–2 of groups |
| 99–<100% | **246** | repeating-group sparsity |
| 100% (all-null) | **1** | `npi_deactivation_reason_code` |

**Per-fragment `npi` zone-map spread** (evidence for §1.1 and §5 Tier-2):

| Frag | rows | npi min | npi max |
|---:|---:|---:|---:|
| 0 | 1,048,576 | 1003800012 | 1992819981 |
| 2 | 1,048,576 | **1003000100** | **1992999890** |
| 5 | 1,048,576 | 1003300013 | 1992369995 |
| 9 | 114,263 | 1003743006 | 1992653406 |
| *(all 10)* | — | ~1003xxxxxx | ~1992xxxxxx |

Every fragment spans the full issued NPI range → **fragment-level pruning on `npi` is impossible**; a batch `npi IN (…)` lookup touches all 10 fragments.

---

## 3. Schema & Index Ledger

9,551,447 rows. NDV = HLL estimate unless *exact*. "Optimal" assessed purely on cardinality + structural role. The 3.594% null floor = the 343,321-row deactivated-NPI stub cohort.

| Column | Type | Nulls % | NDV (cardinality) | Existing index | Optimal index | Verdict |
|---|---|---:|---:|:--|:--|:--|
| `npi` | `string` | 0.000% | **9,551,447** *(exact, unique)* | **BTREE** | BTREE | ✅ matched (resolution PK) |
| `provider_first_line_business_practice_location_address` | `string` | 3.594% | ~2,911,466 | **BTREE** | BTREE | ✅ matched (geo-join / address blocking) |
| `provider_business_practice_location_address_state_name` | `string` | 3.595% | **1,063** *(exact)* | **BITMAP** | BITMAP | ✅ type-matched · ⚠ 0.046% dirty tail inflates to 1,063 (≈57 real) |
| `entity_type_code` | `string` | 3.594% | **2** *(exact: `1`/`2`)* | **— (none)** | **BITMAP** | ❌ **missing** — the individual/org partition key |
| `healthcare_provider_taxonomy_code_1` | `string` | 3.594% | ~1,104 | **— (none)** | **BITMAP** | ❌ **missing** — primary-specialty filter |
| `provider_last_name_legal_name` | `string` | 23.778% | ~801,180 | **— (none)** | **BTREE** | ❌ **missing** — individual resolution blocking key |
| `provider_organization_name_legal_business_name` | `string` | 79.817% | ~1,149,597 | **— (none)** | **BTREE** | ❌ **missing** — org resolution blocking key |
| `provider_first_name` | `string` | 23.778% | ~442,840 | — | (opt.) BTREE | ⚠ resolution support |
| `provider_business_practice_location_address_postal_code` | `string` | 3.595% | ~2,110,286 | — | (opt.) BTREE | ⚠ postal blocking (high-card → BTREE, not BITMAP) |
| `provider_business_mailing_address_state_name` | `string` | 3.595% | **1,154** *(exact)* | — | (opt.) BITMAP | ⚠ mailing-state filter; same dirty tail |
| `provider_enumeration_date` | `string` | 3.594% | ~8,339 | — | **recast `date32`** | ❌ date-as-string; no range pushdown |
| `last_update_date` | `string` | 3.594% | ~8,203 | — | **recast `date32`** (+opt. BTREE) | ❌ date-as-string; delta-scan key |
| `npi_deactivation_date` | `string` | 96.214% | ~6,164 | — | **recast `date32`** | ❌ date-as-string |
| `npi_reactivation_date` | `string` | 99.809% | ~2,762 | — | **recast `date32`** | ❌ date-as-string |
| `certification_date` | `string` | 48.942% | ~2,327 | — | **recast `date32`** | ❌ date-as-string |
| `provider_sex_code` | `string` | 23.778% | **4** *(exact: `F`/`M`/`X`/`U`)* | — | (opt.) BITMAP | ⚠ low analytical value |
| `is_sole_proprietor` | `string` | 23.778% | **3** *(`N`/`Y`/`X`)* | — | None (Lance dict-encodes) | ⚠ flag; index low-value |
| `is_organization_subpart` | `string` | 79.817% | **2** *(`N`/`Y`)* | — | None | ⚠ flag |
| `employer_identification_number_ein` | `string` | 79.817% | **1** *(`'<UNAVAIL>'`)* | — | **None → DROP** | ❌ redaction sentinel (zero info) |
| `parent_organization_tin` | `string` | 98.366% | **1** *(`'<UNAVAIL>'`)* | — | **None → DROP** | ❌ redaction sentinel |
| `provider_other_organization_name` | `string` | 94.621% | **1** *(`'<UNAVAIL>'`)* | — | **None → DROP** | ❌ redaction sentinel |
| `npi_deactivation_reason_code` | `string` | **100.000%** | **0** *(exact)* | — | **None → DROP** | ❌ fully dead column |
| `other_provider_identifier_1..50` (+ type/state/issuer) | `string` | 83.7% → 99.9999% | 2 → 1.58M | — | nested `list<struct>` (§5.5) | ⚠ 200 sparse cols |
| `healthcare_provider_taxonomy_code_2..15` / `_license_*` / `_switch_*` / `_group_*` | `string` | 81.7% → 99.98% | 2 → 4M | — | nested `list<struct>` (§5.5) | ⚠ ~75 sparse cols |
| `source_file` | `string` | 0.000% | **1** *(`'NPPES_Data_Dissemination_May_2026_V2.zip'`)* | — | **→ schema metadata** | ❌ 364.4 MiB decoded ballast |
| `source_member` | `string` | 0.000% | **1** *(`'npidata_pfile_…'`)* | — | **→ schema metadata** | ❌ 318.8 MiB decoded ballast |
| `snapshot_month` | `string` | 0.000% | **1** *(`'2026-05'`)* | — | keep (vintage key) or metadata † | ⚠ 63.8 MiB; constant within partition |
| `ingested_at` | `timestamp[us,UTC]` | 0.000% | **1** *(exact)* | — | **→ schema metadata** | ❌ 72.9 MiB decoded ballast |

**Index mismatches:** none on type. **Index coverage:** 3/3 indices cover 10/10 fragments; 0 unindexed rows; npi BTREE 2,333 pages, address BTREE 2,242 pages (min=`NULL`), state BITMAP 1,064 bitmaps.

† **Architectural note.** `snapshot_month` is constant *within* this partition (the dataset is physically partitioned at `snapshot=YYYY-MM/`), so it is per-partition ballast — but it is the **vintage discriminator** when UNION-ing months for cross-snapshot reads. Keep it (cheap, 63.8 MiB) or move it to schema metadata and re-derive on union. The other three provenance columns are pure lineage → demote to metadata unconditionally.

**State hygiene:** practice-state 1,063 distinct / mailing-state 1,154 distinct, but the dirty tail is 0.046% of rows (943 foreign/free-text values: `BAJA CALIFORNIA` 639, `PUERTO RICO` 300, `ONTARIO` 293, `CHIHUAHUA`, `GERMANY`, `QUEBEC`, `OKINAWA`, `ISRAEL`, …). Top legit: CA 1.15M · NY 0.65M · FL 0.64M · TX 0.59M · OH 0.40M.

---

## 4. Execution Runtime Specs

Hardware envelope (the deployed worker, `pipelines/nppes/ingest.py`): **32 GiB RAM · 8 vCPU · 512 GiB ephemeral disk**. The full 334-column scan in this diagnostic ran out-of-core inside ~10 GiB with no OOM. Three workloads, three configs.

### A. Read / query (out-of-core scan, point + range lookup)

```sql
PRAGMA threads=8;                            -- = vCPU count
SET memory_limit='20GB';                     -- ~62% of 32 GiB; leaves RAM for Lance pool + Arrow + OS
SET temp_directory='/mnt/nvme/duck_spill';   -- local NVMe — NOT the '/' root FS
SET max_temp_directory_size='64GB';          -- cap spill
SET preserve_insertion_order=false;          -- streaming aggregates don't need order; lowers peak RSS
```
```bash
export LANCE_MEM_POOL_SIZE=4294967296        # 4 GiB Lance IO/buffer pool — R2 read throughput within envelope
export TMPDIR=/mnt/nvme/duck_spill           # belt-and-suspenders: keep any library temp off '/'
```
- **Force index pushdown:** pass the predicate *into the Lance scanner*, not into a post-materialization DuckDB filter — `ds.scanner(columns=[…only needed…], filter="npi IN ('…','…')", batch_size=131072)`. This guarantees the BTREE/BITMAP prefilter fires; a `SELECT * FROM <lance> WHERE …` that relies on replacement-scan pushdown can silently degrade to a full column scan.
- **Project narrowly.** A `SELECT *` materializes 334 columns including 819.81 MiB of provenance ballast and 275 mostly-null slots. List only the columns the query needs.

### B. Reindex (BTREE/BITMAP rebuild — `reindex_nppes`)

```bash
export LANCE_BYPASS_SPILLING=true            # REQUIRED — Lance's bounded spill sorter under-sizes and OOMs on
                                             # high-cardinality string columns (lance#2650). Safe here: the in-RAM
                                             # sort of one ~30-char column @ 9.55M rows is <1 GiB ≪ 32 GiB.
```
- Build path is **stage R2→local → index locally → boto3 publish**. Building indices with `storage_options` directly against R2 trips R2's multipart rule (`400 InvalidPart`) once a BTREE `page_data.lance` (here 120–126 MB) forces adaptive multipart to escalate part size mid-upload. Local build + uniform-part publish is the only R2-compliant transport.
- `ephemeral_disk ≥ 16 GiB` (11.46 data + 0.25 index + scratch).

### C. Mutate / rewrite (Tier-2 global `ORDER BY npi` re-sort + recasts)

```sql
PRAGMA threads=8;
SET memory_limit='24GB';                     -- keep more of the sort hot in RAM
SET temp_directory='/mnt/nvme/duck_spill';   -- the ORDER BY spills the DECODED payload — must be high-I/O NVMe
SET max_temp_directory_size='128GB';         -- decoded sort spill ≈ 25–35 GiB (2–3× the 11.46 GiB on-disk); size generously
SET preserve_insertion_order=true;           -- preserve the ORDER BY npi ordering into the Lance write
```
- Write streams via `to_arrow_reader(131072)` → bounded write RSS; `max_rows_per_file=1048576`, `max_bytes_per_file=90 GiB`, `data_storage_version='2.1'`.
- `ephemeral_disk ≥ 64 GiB` (new local stage ~12 GiB + sort spill ~35 GiB); the 512 GiB envelope is ample.
- **Blast radius:** the rewrite is a full overwrite of the `snapshot=2026-05/` prefix (`_replace_r2_prefix` is idempotent — wipe then uniform upload). Run it **isolated from any concurrent monthly capture**; keep the heavy re-sort off the standard append path.

---

## 5. Optimization Blueprint

Two tiers. **Tier 1 is index-only — no data rewrite** — ship via the existing `reindex` entrypoint for an immediate access win. **Tier 2 folds every transform-side change into one append-only overwrite** — because the SoR is immutable and the publish path is wipe-and-reupload, batching the sort/recast/drop changes into a single overwrite is the only blast-radius-contained way to do them and avoids repeated R2 churn.

### Tier 1 — Index-only (cheap, immediate, no rewrite)

1. **Add `BITMAP` on `entity_type_code`** (NDV=2). The individual-vs-organization partition key that nearly every downstream query branches on. Highest value-to-cost ratio of any single change here.
2. **Add `BITMAP` on `healthcare_provider_taxonomy_code_1`** (NDV≈1,104, 96.4% populated). The primary-specialty filter — the dominant categorical predicate for healthcare segmentation. At ~1,104 distinct it sits at the BITMAP/BTREE boundary but the access pattern is categorical equality (`taxonomy_code_1 = '…'`), so roaring BITMAP is correct.
3. **Add `BTREE` on `provider_last_name_legal_name`** (≈801k distinct) **and `provider_organization_name_legal_business_name`** (≈1.15M distinct). The identity-resolution blocking keys the original geo-shaped plan omitted; without them name resolution is a full scan.
4. *(Optional)* `BITMAP` on `provider_business_mailing_address_state_name`; `BTREE` on `provider_business_practice_location_address_postal_code` if zip-blocking is a hot path.

Edit `INDEX_PLAN` in `pipelines/nppes/ingest.py` (`btree += [provider_last_name_legal_name, provider_organization_name_legal_business_name]`; `bitmap += [entity_type_code, healthcare_provider_taxonomy_code_1]`), then `modal run pipelines/nppes/ingest.py::reindex --snapshot-month 2026-05`. Index-only — does not touch data fragments (isolated from the data plane). Index storage will rise from 250 MiB but the ratio (2.1%) has enormous headroom.

### Tier 2 — Single append-only rewrite (structural optimum)

Fold **all** of the following into the `_build_transform_sql` projection and one `lance.write_dataset(mode="overwrite")`, then rebuild every scalar index (Tier 1 included), then one boto3 publish:

5. **`ORDER BY npi` before the write — the single highest-value layout change.** The current write inherits CMS file order, which (measured) leaves every fragment spanning the entire NPI space. A global sort on `npi` makes the 10 fragments npi-disjoint → **point and batch `npi` lookups prune to 1 fragment instead of 10.** Since `npi` is the universal join key into the provider graph, this accelerates every downstream resolution join. *(Decision: if a geo access pattern dominates instead, `ORDER BY provider_business_practice_location_address_state_name, npi` clusters by state while keeping npi locally sorted — default to plain `npi` unless geo is proven hotter.)* This is an external-sort, **disk-bound** — run under the §4-C config, isolated from the monthly append.

6. **Recast the 5 date columns `string → date32`** — parse MM/DD/YYYY → DATE for `provider_enumeration_date`, `last_update_date`, `npi_deactivation_date`, `npi_reactivation_date`, `certification_date`. Enables temporal range pushdown + zone-map pruning + ~50%+ column narrowing. **Hold every identifier as VARCHAR** (`npi`, all license/identifier/EIN/TIN) — the deliberate leading-zero/lexical-join rule; do **not** cast `npi` to integer.

7. **Drop the dead + redaction columns.** Remove `npi_deactivation_reason_code` (100% null), `employer_identification_number_ein`, `parent_organization_tin`, `provider_other_organization_name` (all constant `'<UNAVAIL>'` redaction sentinels — zero information). Reclaims schema width + decode cost.

8. **Demote per-row provenance to schema metadata.** Carry `source_file`, `source_member`, `ingested_at` as Arrow schema key-value metadata (or a 1-row provenance sidecar), not per-row columns. **Reclaims 756 MiB of decoded payload** (`SELECT *` materialization cost). Keep `snapshot_month` (vintage discriminator for cross-month UNION; §3 †). *On-disk these compress to ~0 already — this is a decode/projection-time win, not a storage win; do not frame it as reclaiming disk.*

9. *(Optional, hygiene)* **Normalize the state/country tail** — `upper(trim(state))`, map foreign provinces → country, null/flag free-text. Collapses practice-state 1,063 → ~57 and mailing-state 1,154 → ~57, shrinking those BITMAPs and tightening every state-blocking join. Low row-impact (0.046%); lower priority than the sort/recast.

10. *(Forward-looking schema decision — not a mechanical fix)* **Pivot the repeating groups to nested `list<struct>`.** The 200 `other_provider_identifier_*` columns and the taxonomy/license `_1..15` groups (275 cols, ≥99% null beyond slot 1–2) are the mathematically-optimal candidates for `other_provider_identifiers: list<struct<id,type_code,state,issuer>>` and `taxonomy: list<struct<code,switch,license,license_state,group>>`. This collapses 275 columns → 2, eliminates the wide-schema metadata + sparse-projection overhead, and makes the repeating data queryable as arrays. **Caveat:** it breaks the flat NPPES schema that downstream consumers expect — an architectural decision, not a drop-in. Raise it deliberately; it is the deepest correct layout for a repeating-group registry but carries the largest consumer-migration cost.

**Not required: compaction.** 0 tombstones, 10 clean fragments (9 at cap + tail), 1 file each — topology is already optimal. The Tier-2 rewrite is a **clustering + schema** pass, not a fragmentation remedy; do not frame it as compaction debt.

**Sequence:** Tier 1 indices now (decoupled, cheap, immediate). Then one Tier-2 rewrite folding steps 5–9 (and a decision on 10) → full reindex → single publish, under §4-C, isolated from the monthly capture.

---

## 6. Compute Engine Integration (DuckDB ⋈ Lance) — Empirical

§4 specified runtime configs from first principles. This section **measures** them against the live dataset. Every figure is a cold/warm R2 read from this vantage; `duckdb 1.5.3` over `lance.dataset(...)` registered as a relation (`con.register(name, ds)` — the general analytical path) and `lance` scanner pushdown. The bedrock is assessed on its own structure; how today's `apps/*` consumers happen to query it is not a constraint and is excluded from the verdict.

### 6.1 Pushdown survives the DuckDB SQL boundary — the engine is not the bottleneck

Same query shape (`count(*) WHERE col = val`), indexed vs unindexed column, is the controlled experiment:

| DuckDB SQL over registered `LanceDataset` | Index | Rows | Latency | Reading |
|---|:--|---:|---:|:--|
| `count(*) WHERE npi = <one>` | BTREE | 1 | **~100 ms** | index pushed into Lance (a full `npi` scan ≈ 1 s+) |
| `count(*) WHERE entity_type_code = '2'` | **none** | 1,927,780 | **1,243 ms** | **scan floor** — nothing to push to (same shape, 12× slower) |
| `SELECT * WHERE npi = <one>` | BTREE | 1 × 334 col | **1.9 s** | pushdown **+ late materialization** — else 11.46 GiB ≈ 120 s |
| `SELECT npi WHERE npi = <one>` | BTREE | 1 | 1.5 s | pushdown + one R2 "take" round-trip (cold open) |

**Finding:** DuckDB pushes scalar-index equality predicates straight into Lance, and projects late — proven by `SELECT *` returning one row in 1.9 s instead of dragging the full 11.46 GiB. The index↔engine path is healthy. **A correctly-written analytical query gets pushdown for free — *if the predicate column is indexed*.** The 12× gap to the unindexed control is the entire story: the problem is missing indices on the analytical axes, not a broken pushdown path. (Cold point-lookups land at 1.5–1.9 s including dataset-open + R2 takes, not the "sub-100 ms" of warm metadata-only counts — R2 round-trips dominate the cold path.)

### 6.2 The unindexed scan floor — where the bedrock actually fails

The columns GTM segments on carry no index, so every analytical sweep hits the scan floor:

| Analytical query | Latency | Cause |
|---|---:|:--|
| count of specialty `X` across all 15 taxonomy slots (15-col `OR`) | **6,652 ms** | unindexed + 15-column smear (§6.4) |
| market-map cell: NPI+name+city of specialty `X` in TX (BITMAP state ⋀ 15-col `OR`, project 3) | **8,729 ms** | one selective index + unindexed specialty scan |
| `count(*)` unindexed categorical (`entity_type_code`) | 1,243 ms | full column scan |

**Effective analytical scan throughput ≈ 97 MiB/s** (the §2 full pass: 11.46 GiB read + 668 aggregates in 121 s). Query wall-clock ≈ *scanned-column-bytes ÷ 97 MiB/s* whenever the predicate is unindexed — which, for GTM, is always. Seconds-per-query is not interactive; a full specialty×geo map (≈1,100 taxonomies × 51 states) iterated this way is minutes-to-hours.

### 6.3 Temporal axis is semantically broken — no consumer rewrite can fix it cheaply

Dates are stored as `MM/DD/YYYY` strings (sample: `05/23/2005`). Measured:

```
lexical    '12/31/1999' < '01/01/2020'                         -> FALSE     (string order is NOT chronological)
naive      WHERE provider_enumeration_date >= '2020-01-01'     -> 0 rows    (silently wrong: compares 'MM/..' to '2020..')
correct    WHERE try_strptime(...,'%m/%d/%Y') >= DATE '2020-01-01' -> 3,292,670 rows  (222 ms full-scan reparse)
```

`MM/DD/YYYY` strings do not sort chronologically, so **range filters and `ORDER BY date` are wrong**, and a naive consumer gets a silent empty/garbage result. The only correct path is a per-query `strptime` reparse — a computed expression over a **full scan** that **defeats any index or zone-map pruning**. Time-cohorting ("enumerated in the last 2 years," "recently reactivated") — a core GTM primitive — is unusable on the bedrock as-typed. This is a typing defect in the SoR; it must be fixed in the data (`date32`, §5.6), not downstream.

### 6.4 Specialty axis is shattered across 15 columns

A provider's specialties live in `healthcare_provider_taxonomy_code_1..15` (with parallel 15-tuple `switch`/`license`/`group` columns). There is **no single indexable specialty column**. Measured on the highest-volume primary code `106S00000X`:

```
slot_1 only       564,452
any of 15 slots   582,200    (15-column OR scan, 6,652 ms)
missed by slot1   17,748  (3.0%)
```

A primary-slot-only index (the §5 Tier-1 `BITMAP` on `taxonomy_code_1`) is a partial fix: it accelerates the *primary* specialty but **misses every secondary-held specialty** — 3% for a primary-dominant code, structurally much higher for specialties typically held as a second/third taxonomy. Clean "all providers with specialty X" inherently requires unpivoting 15 columns × 9.55M rows. Specialty market-mapping — the #1 healthcare-GTM axis — is structurally hostile in this layout.

### 6.5 Out-of-core & memory — measured, confirms §4

The full 334-column null+NDV pass (9.55M rows, 668 streaming aggregates) completed in **121 s inside a 10 GiB `memory_limit` with zero spill** — HLL/count states are tiny, so *aggregation* is not the memory risk. The §4-A read config (`threads=8`, `memory_limit≈20 GB`, `temp_directory` on NVMe, narrow projection, push predicates into the scanner) is confirmed sufficient for query/scan workloads. The memory/disk pressure lives entirely in the **Tier-2 `ORDER BY npi` global re-sort** (§4-C): that external sort spills the *decoded* payload (≈25–35 GiB), and its `temp_directory` **must** be high-I/O local NVMe — on the container root FS it either exhausts disk or throttles.

### 6.6 I/O bottlenecks — DuckDB ⋈ Lance COPY/INSERT (write path)

Three memory/I-O hazards on writes into the Lance SoR, in rising severity:

1. **R2 multipart `400 InvalidPart` (already mitigated).** A direct Lance write to R2 trips R2's "all non-trailing parts equal length" rule once a scalar-index `page_data.lance` is large enough to escalate object_store's adaptive multipart mid-upload. NPPES is squarely in range — the address BTREE `page_data.lance` is **126 MB**, and the proposed name BTREEs (§5 Tier-1) are the same class. Mandatory pattern (already in the pipeline): **build local → boto3 publish (uniform parts)**. Never write indices straight to R2.
2. **BTREE-train OOM / `LANCE_BYPASS_SPILLING` scaling cliff.** The high-cardinality string BTREEs (`provider_first_line_business_practice_location_address` 2.9M distinct; the proposed `provider_organization_name_legal_business_name` 1.15M, `provider_last_name_legal_name` 0.8M) train by sorting the column. Lance's bounded spill sorter under-sizes and OOMs on these, so the pipeline sets `LANCE_BYPASS_SPILLING=true` (in-RAM sort). Safe at 9.55M rows (each key set <1 GiB ≪ 32 GiB), and trains run **sequentially** so peak is one-at-a-time — but this trades OOM-safety for speed and is a **scaling cliff**: at multi-month accumulation or a larger registry the in-RAM sort is the first thing to blow the 32 GiB envelope. Watch it as index count and row count grow.
3. **Tier-2 re-sort spill (the real ceiling).** The `ORDER BY npi` rewrite is the heaviest COPY-class op: decoded sort spill ≈25–35 GiB to `temp_directory`, plus the new local Lance stage (~12 GiB) and `READ_BATCH_ROWS`-bounded write RSS. Size `ephemeral_disk ≥ 64 GiB`, `max_temp_directory_size ≥ 128 GiB`, NVMe temp, and run isolated from the monthly capture (blast-radius containment).

---

## 7. GTM-Usability Verdict & the Derived-Layer Mandate

**The data is complete and correct; the *bedrock is stored in raw CMS dissemination shape, not analytical shape*. It is a faithful cold archive masquerading as a serving layer.** The earlier "physically pristine" verdict (§1) stands and is not in tension with this: the bytes are healthy, the *model* is not. Four structural disqualifiers make it functionally unusable for interactive GTM / market-mapping / analysis as it sits — **each independent of any consumer code, none fixable downstream**:

1. **Temporal axis broken** (§6.3) — date-as-string; range filters silently wrong; correct answers require full-scan reparse. Time-cohorting impossible.
2. **Specialty axis shattered** (§6.4) — the primary healthcare-GTM segmentation dimension smeared across 15 columns with no indexable form; "all providers of specialty X" is a 15-column full scan.
3. **Analytical axes unindexed** (§6.2) — `entity_type`, taxonomy, names, dates carry no index, so every segment query hits the ~97 MiB/s scan floor (seconds-to-minutes, non-interactive). Pushdown works (§6.1) but has nothing to push to.
4. **`npi` unclustered** (§1.1) — batch enrichment/resolution joins fan out to all 10 fragments instead of pruning.

**This is not a tuning problem and not a consumer problem.** The engine pushes down correctly; the consumers are rewritable. The defect is that the SoR was never modeled for analysis — it is the CSV, transposed to columns.

**Mandate — separate the archive from the serving layer.** The raw monthly snapshot stays as the immutable, append-only SoR (it is correct and should not be contorted to serve queries). GTM/analysis hits a **derived analytical projection** — call it `nppes_analytical` — built once per snapshot from the raw SoR, carrying:

- **Dates → `date32`** (parse `MM/DD/YYYY`), enabling temporal range pushdown + zone-map pruning.
- **Taxonomy unpivoted to a long/nested specialty model** — either a child table `nppes_taxonomy(npi, taxonomy_code, is_primary, license, license_state, group, slot)` or a `list<struct>` column — so "specialty = X" is one indexable predicate, secondary specialties included, and specialty×geo is a clean `GROUP BY`. **This single change is the difference between a usable and an unusable provider market-map.**
- **Scalar indices on the analytical axes** — `BITMAP(entity_type_code)`, `BITMAP(taxonomy_code)` on the unpivoted table, `BTREE(provider_last_name_legal_name)` / `BTREE(provider_organization_name_legal_business_name)` for resolution, `date32` BTREE/zone-maps for temporal.
- **`ORDER BY npi`** (or `entity_type, npi`) clustering so batch joins prune fragments.
- **The noise dropped** — the 100%-null column, the three `'<UNAVAIL>'` redaction sentinels, the 200-column `other_provider_identifier` sprawl folded into nested form, per-row provenance demoted to metadata.

Until that derived layer exists, the honest status is: **NPPES-as-stored answers single-`npi` point lookups well and nothing else interactively.** Confirmed — and the fix is a derived analytical dataset (§5 Tier-2 is its first iteration), not index tuning on the raw snapshot.

---

### Appendix — Provenance

- Telemetry: `pylance 7.0.0` (`count_rows`, `get_fragments` → `physical_rows`/`count_rows`/`data_files`, `list_indices`, `stats.index_stats`), R2 `list_objects_v2` byte census, `duckdb 1.5.3` streaming aggregates (null density **exact**; NDV via HLL; `npi`/state distinct **exact**; provenance ballast via `octet_length` **exact**).
- Read path: `lance.dataset(uri, storage_options=…)` against R2 (path-style, `region=auto`); one full 334-column null+NDV pass (121 s), one narrow exact pass, one provenance-ballast pass; per-fragment `npi` zone-map pass.
- HLL caveat: `approx_count_distinct` over-estimated `npi` by +14.3% (10.92M vs 9.55M true); high-cardinality NDV figures are indicative, ±~2–15% at the top of the range.
- §6 compute battery: `lance` scanner `filter=` pushdown vs `duckdb 1.5.3` over a registered `LanceDataset` (`con.register`), same-shape indexed/unindexed controls, `try_strptime` date-semantics tests, 15-slot taxonomy `OR` scans, and a specialty×geo market-map query — all timed as cold/warm R2 reads from one vantage; absolute latencies are environment-relative, the **ratios** (indexed vs scan floor) are the load-bearing result.
- No dataset mutation occurred. No DDL, no index ops, no writes. Read-only throughout.
