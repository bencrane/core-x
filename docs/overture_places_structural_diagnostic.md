# Overture Places — Lance Structural Diagnostic

**Target:** `s3://data-sink/active/overture_places/` (Gen-3 SoR, R2)
**Mode:** Read-only, first-principles. Zero DDL / zero mutation. Assessed independent of all downstream consumers.
**Date:** 2026-06-06 · **Vintage:** single snapshot, `snapshot_date = 2026-06-05`
**Method:** `pylance 7.0.0` manifest/fragment/index introspection + R2 `ListObjects` byte census + bounded streaming `duckdb 1.5.3` passes (column profile; per-fragment zone-map) + an empirical predicate-pushdown probe (§5) reading Lance physical plans (`scanner.explain_plan`) and timing the DuckDB integration paths. NDV via HyperLogLog (`approx_count_distinct`, ≈±1.6% σ) unless marked *exact*.

---

## 1. Headline Posture

**Physically healthy; schema-dirty; access-suboptimal for two of its dominant axes.**

The dataset is in excellent *physical* condition. Zero tombstones. A textbook fragment topology — 16 fragments, one data file each, 15 at the 1,048,576-row cap plus a single 544k-row tail — so there is **no read-amplification and no compaction debt**. All seven scalar indices fully cover all sixteen fragments with **zero unindexed appended fragments**, and **every index is type-matched to its column's measured cardinality** (no BTREE-on-categorical, no BITMAP-on-high-card). Storage format is current (Lance v2.1).

The defects are not in the layout — they are in the **schema** and in **physical row order**:

1. **Four constant (cardinality-1) provenance columns are stamped per-row** — `country`, `release_tag`, `snapshot_date`, `ingested_at`. Together they are **403.5 MiB (18.5%) of the decoded payload**; `release_tag` alone is **195 MB** of a single repeated 12-character string. Structurally this is dataset-level lineage masquerading as row data.
2. **The primary categorical access key, `category` (1,574 distinct), is unindexed** — the one clearly missing scalar index.
3. **No spatial/region clustering.** Measured per-fragment zone maps overlap across the entire US extent; every fragment contains 58–74 of the 131 region values. Consequence: **bbox and by-state scans cannot prune a single fragment.** The lon/lat BTREEs deliver row-level pushdown but the file-level zone maps are useless.
4. **`region` is un-normalized** — 131 distinct values where ~57 are legitimate (USPS + territories) and ~74 are dirt (`ca`, `Florida`, `California`, `QC`, `Barcelona`, `Dhaka`, …).
5. **`confidence` is `double` where `float32` is exact-enough** for a 0..1 score — a free 50% narrowing of that column.

Index storage (1.34 GiB) **exceeds data storage (1.13 GiB)** — the structurally-justified cost of a six-BTREE entity-resolution posture, not a defect, but the headroom argues against adding indices casually.

**Verdict:** query-ready for `id` / `name` / `postcode` / `locality` / `region` resolution joins; **sub-optimal for `category` filtering and for any geographic (bbox / by-state) scan**; carrying ~403 MiB of decoded constant-column ballast that belongs in metadata.

**Compute layer (§5, measured):** the index pushdown machinery is correct — every indexed predicate is genuinely served by a `ScalarIndexQuery` — but two realities cap it. (a) **The DuckDB handoff decides everything:** pushing the filter into the Lance scanner is **28× faster** than handing DuckDB an unfiltered reader and filtering in SQL (which bypasses the index and streams all 16.27M rows). (b) **A bbox over the two per-axis BTREEs costs 38.9 s** — 150× a same-selectivity BITMAP lookup — because two 1-D indices cannot serve a 2-D range. Both are fixable (consumer contract + a 1-D quadkey), neither is a corruption.

---

## 2. Telemetry Grid

| Metric | Value |
|---|---:|
| Logical rows | **16,273,123** |
| Physical rows (pre-deletion) | 16,273,123 |
| Deleted / tombstoned rows | **0 (0.0000%)** |
| Fragments | **16** (1 data file each) |
| Rows / fragment — min · avg · max | 544,483 · 1,017,070 · **1,048,576** (= `max_rows_per_file` cap) |
| Data on disk (R2, compressed) | **1,216,216,839 B · 1.133 GiB** |
| Avg bytes / fragment (data) | 76,013,552 B · 72.49 MiB |
| Scalar-index storage (R2) | **1,439,227,001 B · 1.340 GiB** (13 files, 7 indices) |
| Index : data ratio | **1.18×** |
| Manifests + transactions | 27,559 B (9) + 3,791 B (8) |
| **Total R2 footprint** | **2,655,475,190 B · 2.473 GiB** (46 objects) |
| Decoded payload (computed) | 2,286,023,030 B · 2.129 GiB |
| On-disk compression (decoded ÷ data) | **1.88×** |
| **Constant-column decoded ballast** | **423,101,198 B · 403.5 MiB · 18.5% of payload** |
| Storage format version | **2.1** (current) |
| Dataset version | 8 (= 1 write + 7 index commits; single snapshot — no cross-vintage bloat) |

**Fragment zone-map spread** (per-fragment min/max — evidence for §1.3 and §3-E):

| Frag | lon span (°) | lat span (°) | distinct regions | mode |
|---:|---:|---:|---:|:--|
| 0 | 102.6 | 120.3 | ~67 | FL |
| 6 | 72.2 | 50.2 | ~62 | WA |
| 7 | 33.5 | 50.1 | ~74 | CO |
| 13 | 83.9 | 156.0 | ~63 | NJ |
| 15 | **253.2** | **178.0** | ~70 | MA |
| *(all 16)* | 5.4 – 253.2 | 1.8 – 178.0 | **58 – 74 / 131** | — |

Every fragment carries a near-complete mix of all states; longitude ranges overlap end-to-end. Coordinate outliers (`lat` −89.9 / −88.0, `lon` +179.8 / +4.28) on `country='US'` rows are mislocated geometries that further blow out the zone maps.

---

## 3. Schema & Index Ledger

16,273,123 rows. NDV = HLL estimate unless *exact*. "Optimal" is assessed purely on cardinality + structural role.

| Column | Type | Nulls (count / %) | NDV (cardinality) | Existing index | Optimal index | Verdict |
|---|---|---:|---:|:--|:--|:--|
| `id` | `string` | 0 / 0.000% | ~16,354,544 → **unique** | **BTREE** | BTREE | ✅ matched (resolution PK) |
| `longitude` | `double` | 0 / 0.000% | ~16,935,461 (near-unique) | **BTREE** | BTREE | ✅ matched (bbox range) |
| `latitude` | `double` | 0 / 0.000% | ~17,568,890 (near-unique) | **BTREE** | BTREE | ✅ matched (bbox range) |
| `name` | `string` | 0 / 0.000% | ~11,652,709 | **BTREE** | BTREE | ✅ matched (exact-match resolution) |
| `postcode` | `string` | 450,149 / 2.766% | ~4,042,621 | **BTREE** | BTREE | ✅ matched (postal blocking) |
| `confidence` | `double` | 0 / 0.000% | ~3,626,765 | — | (opt.) BTREE + **recast `float32`** | ⚠ wide float; index optional (range) |
| `locality` | `string` | 52,755 / 0.324% | ~65,818 | **BTREE** | BTREE | ✅ matched (locality blocking) |
| `category` | `string` | 689,154 / 4.235% | ~1,574 | **— (none)** | **BITMAP** | ❌ **missing index** |
| `region` | `string` | 114,233 / 0.702% | **131** *(exact)* | **BITMAP** | BITMAP | ✅ matched · ⚠ values un-normalized |
| `country` | `string` | 0 / 0.000% | **1** *(exact)* | — | **None → drop to metadata** | ❌ constant (32.5 MB ballast) |
| `snapshot_date` | `date32[day]` | 0 / 0.000% | **1** *(exact)* | — | **None → drop to metadata** † | ❌ constant (65.1 MB ballast) |
| `release_tag` | `string` | 0 / 0.000% | **1** *(exact)* | — | **None → drop to metadata** † | ❌ constant (**195.3 MB ballast**) |
| `ingested_at` | `timestamp[us,UTC]` | 0 / 0.000% | **1** *(exact)* | — | **None → drop to metadata** | ❌ constant (130.2 MB ballast) |

† **Architectural fork — RESOLVED (operator, 2026-06-06): overwrite model stays** ("no continual re-ingest"). `snapshot_date` / `release_tag` are therefore constant-per-snapshot dead weight and are demoted alongside `country` / `ingested_at`. Provenance moves to **dataset-level schema metadata**, so a manual re-ingest still stamps the new `release_tag` / `snapshot_date` once at the dataset level rather than on all 16.27M rows. (Had append-history been chosen, these two would have become the vintage discriminators and `snapshot_date` the natural temporal sort key — that path is now closed.)

**Index mismatches:** none. **Index coverage:** 7/7 indices cover 16/16 fragments; 0 unindexed fragments.
**`region` value hygiene:** ~57 valid (50 states + DC + PR/AS/VI/GU/MP/FM) + ~74 dirty variants (case: `ca`/`Ca`/`Calif`; full names: `Florida`/`California`/`Ohio`; foreign: `QC`/`BC`/`ON`/`NSW`/`Cumbria`/`Barcelona`/`Dhaka`/`London`). Top legit: CA 1.69M · TX 1.45M · FL 1.21M · NY 0.89M.

---

## 4. Optimization Blueprint

Two tiers. **Tier 1 is a pure index add (no data rewrite)**; ship it via the existing `reindex` entrypoint for an immediate win. **Tier 2 folds every transform-side change into one append-only rewrite** — because the SoR is immutable and the publish path is wipe-and-reupload, batching the schema/sort/recast changes into a single overwrite is the only blast-radius-contained way to do them, and it avoids repeated R2 churn.

### Tier 1 — Index-only (cheap, immediate, no rewrite)

1. **Add `BITMAP` on `category`.** 1,574 distinct over 16.27M rows; roaring-bitmap equality filtering is the correct structure (a BTREE on a 1.5k-cardinality categorical would be strictly worse for `category = …`). Note the cardinality is *medium*, not tiny — still well within Lance BITMAP's roaring range.
   - Edit `OVERTURE_BITMAP_INDEXES = ["region", "category"]` in `pipelines/overture_maps/places.py`, then `modal run pipelines/overture_maps/places.py::reindex`. The reindex stages R2→local, builds locally (avoids R2's multipart part-size rule), publishes once. Index-only — does not touch data fragments (blast-radius isolated from the data plane).

### Tier 2 — Single append-only rewrite (structural optimum)

Fold **all** of the following into the `_build_sql` projection and one `lance.write_dataset(mode="overwrite")`, then rebuild every scalar index (Tier 1 included), then one boto3 publish:

2. **Demote all 4 constant columns to schema metadata.** Drop `country`, `ingested_at`, `release_tag`, `snapshot_date` from the row projection — the overwrite model is **confirmed** (§3 fork resolved), so all four are constant. Carry them as Arrow schema key-value metadata (`schema.with_metadata({b"country": b"US", b"release_tag": …, b"snapshot_date": …, b"ingested_at": …})`) or a 1-row provenance sidecar. **Reclaims ~403 MiB decoded** (the dominant `release_tag` 195 MB included), shrinking every scan's decoded footprint.

3. **Recast `confidence` `double → float32`** (`CAST(confidence AS FLOAT)`). A 0..1 quality score loses zero usable precision; halves the column. **Hold `longitude`/`latitude` at `double`** — `float32` costs ~1 m at the equator, an unacceptable trade for a canonical spatial SoR. (Flagged for explicit decision; default = keep double.) **Also evaluate `id` `string → fixed_size_binary(16)`** — the probe shows `id` is a uniform 36-char UUID (585.8 MB decoded, the largest column and largest BTREE); parsing it to 16-byte binary ≈ 260 MB and shrinks the dominant index. Trade-off: binary `id` costs text readability and forces byte-wise downstream joins — optional, gated on whether `id` is surfaced/joined as text (§5-F.3).

4. **Normalize `region`** in-transform: `upper(trim(region))`, map full names → USPS, null/flag non-US subdivisions. Collapses 131 → ~57 clean codes; tightens the BITMAP and every region-blocking join.

5. **Impose spatial clustering + a 1-D spatial key — the single highest-value layout change.** The current write inherits Overture's geohash-ish order, which (measured) leaves fragment zone maps spanning the whole US. **Measured cost of the status quo (§5-F.1): a bbox served by the two per-axis BTREEs took 38.9 s** — the plan intersects `longitude_idx ∧ latitude_idx`, each matching millions of row-ids before the AND. Two fixes, applied together:
   - **Materialize a space-filling key** — a quadkey / geohash / S2 cell over `(longitude, latitude)` — `ORDER BY` it before the write (so fragment min/max tightens and **bbox scans prune whole `.lance` files** instead of touching all 16), **and index it `BTREE`** so bbox queries translate to a **1-D quadkey-prefix RANGE** predicate (`quadkey >= … AND quadkey < …`) instead of a 2-D lon/lat range — collapsing the 38.9 s two-BTREE intersection into a single contiguous index range. (`ORDER BY region, locality` is the cheaper by-state-only alternative if geographic bbox is not a real access pattern.)
   - **Execution reality:** a 16.27M-row global re-sort is an external-sort bounded by **disk, not RAM**. Pin `temp_directory` to local NVMe, size the spill, and run it isolated from the standard monthly append. Note `LANCE_BYPASS_SPILLING=true` is already set for the in-memory BTREE train (32 GiB) — the *sort* spill is DuckDB's, governed separately by `temp_directory` + `memory_limit` (§5-F.3).

6. *(Optional, low priority)* **Coordinate-sanity gate** — drop/flag rows whose geometry is implausibly non-US (the `lat` −89.9 / `lon` +179.8 / +4.28 outliers). Removes zone-map poisoning at the source.

**Not required:** compaction. Topology is already optimal (0 tombstones, 16 capped fragments, 1 file each) — the Tier-2 rewrite is a *clustering* pass, not a fragmentation remedy. Do **not** frame it as compaction debt.

**Sequence:** Tier 1 now (decouples the cheap categorical-index win from the heavy rewrite). Then resolve the §3 overwrite-vs-append-history fork. Then one Tier-2 rewrite folding steps 2–6 → full reindex → single publish.

---

## 5. Compute Engine Integration (DuckDB)

DuckDB has **no native Lance reader**. Every DuckDB↔Lance interaction flows through `lance.dataset(uri).scanner(columns=…, filter=…)` → Arrow stream → DuckDB. **Index pushdown is therefore a property of the handoff, not of DuckDB.** All numbers below are measured against the live SoR; latencies are cold-cache, single-client, over R2 — treat absolute ms as network-round-trip-dominated and read the *relative* structure as the durable signal.

### F.1 — Predicate pushdown, measured per index type

Physical plans (`scanner.explain_plan`) are the proof: `ScalarIndexQuery@<idx>` means the scalar index is consulted; its absence means a full column scan + residual `FilterExec` + `Take`.

| Predicate | Column index | Physical-plan node | Rows | Latency\* | Pushdown |
|---|---|---|---:|---:|:--|
| `region = 'WY'` | BITMAP | `ScalarIndexQuery@region_idx(Bitmap)` | 41,165 | 0.26 s | ✅ |
| `id = '<uuid>'` | BTREE | `ScalarIndexQuery@id_idx(BTree)` | 1 | 1.78 s | ✅ |
| `postcode = '82007'` | BTREE | `ScalarIndexQuery@postcode_idx(BTree)` | 382 | 7.75 s | ✅ |
| `locality = 'Cheyenne'` | BTREE | `ScalarIndexQuery@locality_idx(BTree)` | 5,764 | 3.73 s | ✅ |
| bbox `lon∈[-111,-104] ∧ lat∈[41,45]` | BTREE×2 | `ScalarIndexQuery AND(longitude_idx, latitude_idx)` | 42,550 | **38.9 s** | ✅ *pathological* |
| `category = 'landmark_…'` | none | `LanceScan(category)+FilterExec+Take` | 178,568 | 7.26 s | ❌ full scan |
| `confidence > 0.99` | none | `LanceScan(confidence)+FilterExec+Take` | 1,710,403 | 6.06 s | ❌ full scan |

<sub>\*Cold cache, single client, over R2 — latency is network-round-trip-dominated, not CPU-bound.</sub>

**Findings:**
1. **The indices are live.** Every indexed `=`/range predicate is genuinely served by `ScalarIndexQuery` — no silent full-scan fallback. The baseline pushdown contract holds.
2. **Bbox via two 1-D BTREEs is pathological (38.9 s).** The plan intersects `longitude_idx ∧ latitude_idx`; each axis range matches millions of row-ids nationwide before the AND, then `Take`. A per-axis BTREE is *not* a 2-D spatial index — see §4-step-5 (materialize + index a 1-D quadkey).
3. **Over R2, a clean scan can beat an index lookup.** The `postcode` point query (382 rows) took **7.75 s — slower than full-scanning the entire `category` column** (178k rows, 7.26 s), because the BTREE `Take` issues scattered random row reads while a scan streams sequential ranges. Index selectivity must overcome a steep random-access penalty on object storage; for low-selectivity predicates a scan is the better plan.
4. **Unindexed `category`/`confidence` full-scan the projected column** + residual filter (expected). `category` is the Tier-1 BITMAP fix.

### F.2 — The DuckDB handoff determines whether indices are used (measured)

Same predicate (`region='WY'`, 41,165 matching rows), three handoffs:

| # | Handoff | Mechanism | Rows crossing Arrow | Latency | Index |
|---|---|---|---:|---:|:--|
| A | `ds.scanner(filter="region='WY'").to_reader()` | filter pushed **into** Lance | 41,165 | **0.21 s** | ✅ |
| B | `ds.scanner().to_reader()`, then `WHERE region='WY'` in SQL | filter applied **after** a full read | 16,273,123 | 5.89 s (**28×**) | ❌ bypassed |
| C | `SELECT … FROM <LanceDataset> WHERE region='WY'` | DuckDB replacement-scan pushes the predicate down | 41,165 | 0.40 s | ✅ |

**Consumer contract (load-bearing):** **never apply a `WHERE` in DuckDB over an unfiltered Lance reader (Path B)** — it streams all 16.27M rows across the Arrow boundary and discards 99.7% of them, bypassing the index entirely (28× penalty here, unbounded at scale). Either:
- pass the **`LanceDataset` object** straight to DuckDB and let the replacement scan push the predicate (Path C), or
- construct the scanner **with** the filter + projection — `ds.scanner(filter=…, columns=[…])` (Path A).

Path C's pushdown is bounded to predicate shapes Lance/Arrow accept — simple comparisons (`=`, `<`, `>`, `IN`, `IS NULL`), conjunctions, and projection. Functions, complex expressions, and join predicates **do not push** and silently fall back to a Path-B-style full scan; for selective work prefer the explicit Path A scanner filter.

### F.3 — Out-of-core configuration & I/O bottlenecks

**Canonical DuckDB runtime for querying this SoR at scale:**
```sql
SET memory_limit            = '24GB';                 -- ~70–75% of container RAM (32 GiB worker); leave headroom for Arrow + object_store buffers
SET temp_directory          = '/mnt/nvme/duckdb_spill'; -- DEDICATED local NVMe, never the overlay/root fs
SET max_temp_directory_size = '256GB';                -- bound the spill so a runaway hash-agg fails loud, not by filling the disk
SET threads                 = 8;                      -- = physical cores (pipeline uses 8)
SET preserve_insertion_order = false;                 -- highest-value flag for large unordered scans/aggregations — frees the row-order buffer
SET enable_progress_bar      = false;
-- Reads of the Lance SoR traverse Lance's object_store (R2), NOT DuckDB httpfs.
-- DuckDB httpfs is only the upstream public-Parquet read path in the ingest worker.
```
Why these, for *this* dataset: the decoded payload is **~2.13 GiB**, so a `SELECT *` materializes comfortably in ≥8 GiB — but a `GROUP BY` / `DISTINCT` / `JOIN` on the high-cardinality keys (`id` 16.27M unique, `name` 11.65M) builds hash tables far larger than the raw column. Without an honest `memory_limit` + a real NVMe `temp_directory`, those operators OOM instead of spilling. During a rebuild the external sort and the local Lance staging dir **compete for the same NVMe** — size both inside the worker's `ephemeral_disk` (the pipeline pins 512 GiB).

**Write-path bottlenecks (ingest → Lance SoR; code-evidenced from `places.py`):**
1. **`to_arrow_table()` full materialization** — the entire result set in RAM (~2.13 GiB Arrow here, plus DuckDB execution buffers). Run in a 32 GiB container with a pre-authorized streaming `to_arrow_reader(1_048_576)` fallback on `MemoryError` / `duckdb.OutOfMemoryException`. This is the primary OOM surface at larger-than-US scale.
2. **BTREE training with `LANCE_BYPASS_SPILLING=true` — the hard scale ceiling.** Lance's `ExternalSorter` under-accounts memory and OOMs, so the pipeline disables spill and sorts the unique `id` + lon/lat fully **in memory** (32 GiB). Index-build memory grows with `rows × key-width` and **cannot spill** — this, not the data write, caps row scale. (`id` is 16.27M *unique* 36-char UUIDs — the single largest sort; the §3 `fixed_size_binary(16)` recast would more than halve it.)
3. **R2 multipart part-size escalation (400 InvalidPart)** — direct Lance→R2 writes trip R2's "non-trailing parts must be equal length" rule once a near-unique index page forces `object_store` to escalate part size mid-upload. Mitigated by build-local + boto3 publish (uniform parts). **The publish is the residual I/O bottleneck:** `_replace_r2_prefix` uploads objects **sequentially, single-threaded** (`s3.upload_file` per object) — 46 objects / 2.47 GiB here; at scale this serializes with no upload concurrency or multipart-threshold tuning. A `ThreadPoolExecutor` over the upload loop (or `TransferConfig` tuning) is the lever if publish wall-time becomes binding.

**Read-path bottlenecks (measured in F.1):**
4. **Random-access `Take` penalty over R2** — selective index lookups gather rows by scattered row-address GETs; per-GET R2 latency can erase the selectivity win (postcode point query 7.75 s). Mitigation: colocate compute in the R2 region, warm/cache index pages, or prefer scan+filter for low-selectivity predicates.
5. **2-D range over 1-D BTREEs (38.9 s) + zero fragment pruning** (§2 zone maps overlap US-wide) — every query also considers all 16 fragments. Both are addressed by the §4-step-5 spatial rework.

**Net compute verdict:** the pushdown machinery is correct and the indices are genuinely consulted — but the engine is only as good as the handoff (Path B silently bypasses every index) and the access geometry (2-D bbox over per-axis BTREEs is 150× slower than an equivalent-selectivity BITMAP). The two structural fixes — a 1-D quadkey for geo, and a disciplined consumer contract — convert the compute layer from "works, sometimes slowly" to "prunes."

---

### Appendix — Provenance

- Telemetry: `pylance 7.0.0` (`count_rows`, `get_fragments`, `list_indices`), R2 `list_objects_v2` byte census, `duckdb 1.5.3` streaming aggregates (null density exact; NDV via HLL; decoded bytes via `octet_length(encode(·))`).
- Read path: `lance.dataset(uri, storage_options=…)` against R2 (path-style, `region=auto`); bounded streaming passes (column profile, per-fragment zone-map, region distribution).
- §5 compute probe: per-predicate `scanner.explain_plan` capture (index-node classification) + wall-clock timing of the three DuckDB handoff paths, all over R2. Single-client, cold-cache — relative structure is the signal, not absolute latency.
- No dataset mutation occurred. No DDL, no index ops, no writes.
