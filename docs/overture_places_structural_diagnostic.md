# Overture Places — Lance Structural Diagnostic

**Target:** `s3://data-sink/active/overture_places/` (Gen-3 SoR, R2)
**Mode:** Read-only, first-principles. Zero DDL / zero mutation. Assessed independent of all downstream consumers.
**Date:** 2026-06-06 · **Vintage:** single snapshot, `snapshot_date = 2026-06-05`
**Method:** `pylance 7.0.0` manifest/fragment/index introspection + R2 `ListObjects` byte census + two bounded streaming `duckdb 1.5.3` passes (column profile; per-fragment zone-map). NDV via HyperLogLog (`approx_count_distinct`, ≈±1.6% σ) unless marked *exact*.

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

† **Architectural fork.** `snapshot_date` / `release_tag` are dead weight **only under the current `mode="overwrite"` single-snapshot model**. If the SoR is meant to accumulate vintages (append-history), they become the vintage discriminators — non-constant, load-bearing, and `snapshot_date` becomes the natural temporal sort/partition key. Resolve the model first; demote to metadata only if overwrite stays.

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

2. **Demote the 4 constant columns to schema metadata.** Drop `country`, `ingested_at` from the row projection unconditionally; drop `release_tag` + `snapshot_date` **iff** the overwrite model is retained (§3 fork). Carry them as Arrow schema key-value metadata (`schema.with_metadata({b"country": b"US", b"release_tag": …, b"snapshot_date": …, b"ingested_at": …})`) or a 1-row provenance sidecar. **Reclaims ~403 MiB decoded** (and the dominant `release_tag` 195 MB), shrinking every scan's decoded footprint.

3. **Recast `confidence` `double → float32`** (`CAST(confidence AS FLOAT)`). A 0..1 quality score loses zero usable precision; halves the column. **Hold `longitude`/`latitude` at `double`** — `float32` costs ~1 m at the equator, an unacceptable trade for a canonical spatial SoR. (Flagged for explicit decision; default = keep double.)

4. **Normalize `region`** in-transform: `upper(trim(region))`, map full names → USPS, null/flag non-US subdivisions. Collapses 131 → ~57 clean codes; tightens the BITMAP and every region-blocking join.

5. **Impose spatial clustering — the single highest-value layout change.** The current write inherits Overture's geohash-ish order, which (measured) leaves fragment zone maps spanning the whole US. Add an `ORDER BY` on a space-filling key before the write — a computed quadkey/geohash/S2 cell over `(longitude, latitude)`, or pragmatically `ORDER BY region, locality` if by-state is the dominant pattern — so fragment min/max becomes tight and **bbox / by-state scans prune whole `.lance` files** instead of scanning all 16. This is the only operation here that is a genuine re-sort.
   - **Execution reality:** a 16.27M-row global re-sort is an external-sort bounded by **disk, not RAM**. Pin `temp_directory` to local NVMe, size the spill, and run it isolated from the standard monthly append. Note `LANCE_BYPASS_SPILLING=true` is already set for the in-memory BTREE train (32 GiB) — the *sort* spill is DuckDB's, governed separately by `temp_directory` + `memory_limit`.

6. *(Optional, low priority)* **Coordinate-sanity gate** — drop/flag rows whose geometry is implausibly non-US (the `lat` −89.9 / `lon` +179.8 / +4.28 outliers). Removes zone-map poisoning at the source.

**Not required:** compaction. Topology is already optimal (0 tombstones, 16 capped fragments, 1 file each) — the Tier-2 rewrite is a *clustering* pass, not a fragmentation remedy. Do **not** frame it as compaction debt.

**Sequence:** Tier 1 now (decouples the cheap categorical-index win from the heavy rewrite). Then resolve the §3 overwrite-vs-append-history fork. Then one Tier-2 rewrite folding steps 2–6 → full reindex → single publish.

---

### Appendix — Provenance

- Telemetry: `pylance 7.0.0` (`count_rows`, `get_fragments`, `list_indices`), R2 `list_objects_v2` byte census, `duckdb 1.5.3` streaming aggregates (null density exact; NDV via HLL; decoded bytes via `octet_length(encode(·))`).
- Read path: `lance.dataset(uri, storage_options=…)` against R2 (path-style, `region=auto`); two bounded streaming passes (column profile, per-fragment zone-map) + one narrow region-distribution pass.
- No dataset mutation occurred. No DDL, no index ops, no writes.
