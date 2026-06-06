# PDL Companies — Structural & Compute Diagnostic

Read-only, first-principles interrogation of the **live R2-backed Lance system of record** for the
People Data Labs (PDL) company universe — physical layout, schema/type efficiency, fragment
topology, index manifest + trained truth, predicate-pushdown behavior, and the out-of-core compute
envelope required to query and to safely rebuild it. Companion to
`docs/reference/PDL_INDEX_TOPOLOGY_PUSHDOWN_DIAGNOSTIC.md` (which proved the index/pushdown spine);
this report adds the storage-physics, type-efficiency, fragmentation/compaction, footprint, and
DuckDB/Lance memory-bound axes that diagnostic did not cover.

- **Targets interrogated (Gen-3 SoR, `s3://data-sink/active/`):**
  - `pdl_companies` — firmographic SoR (Lance **v11**, **35,446,771 rows**, 12 flat cols, **34
    fragments**, **7.176 GB / 73 objects**).
  - `pdl_normalized_companies` — derived blocking-key sidecar (Lance **v7**, **35,446,771 rows**, 15
    flat cols, **34 fragments**, **8.100 GB / 60 objects**).
  - "PDL companies Lance tables" = these two. There is **no** `pdl_person` dataset and no
    person-level PDL data live (the directive-misnomer flagged in the prior diagnostic still holds;
    the SoR is company-level).
- **Live evidence harness (non-mutating, zero writes):** `pylance 7.0.0` / `pyarrow 24.0.0` /
  `duckdb 1.5.x` / `boto3` direct R2 reads — `dataset.schema` · `get_fragments()` (+ per-fragment
  `count_rows`/`physical_rows`/`data_files`/deletion-vector) · `list_indices()` +
  `stats.index_stats()` (type + trained-row truth) · boto3 `list_objects_v2` (exact compressed bytes
  per object, by directory) · a bounded decoded-width sample (`RecordBatchReader.nbytes`) for the
  uncompressed estimate · **one full streaming aggregate** in DuckDB (exact null density +
  `approx_count_distinct` cardinality + integer min/max, per column, both datasets) ·
  `LanceScanner.explain_plan(verbose=True)` / `analyze_plan()` on indexed predicates. All reads are
  R2-direct; **no `write_dataset`, no `create_scalar_index`, no `delete`, no `optimize`, no R2
  put/delete.**
- **As-of:** probed **2026-06-06** against the committed datasets, R2 creds via Doppler `core-x/prd`.
- **Attestation:** every figure below is a live read of the committed datasets or the physical plan
  the Lance/DataFusion engine emitted and executed — not a recon estimate. Cardinalities marked `≈`
  are `approx_count_distinct` (HyperLogLog, ±~2%); on near-unique columns HLL can report **above** the
  row count — those are capped at ≤ 35,446,771 and read as "near-unique." Exact `pdl_company_id`
  distinct (the PK) is **35,446,771** (ops-ledger verified, 1:1).

---

## 1. Headline posture

**Clinical verdict: structurally sound and compute-ready. Both PDL tables are at or near the
mathematical optimum for an overwrite-snapshot Lance dataset — zero fragmentation drift, zero
tombstones, 100%-trained cardinality-correct indices, flat scalar schema, working projection +
index pushdown. There is no compaction to run, no dead index to retrain, no nested-key trap, and no
schema recast that materially moves storage. The architecture does not require surgery; it requires
a small set of targeted hardening edits and two governance corrections.** The dominant physical fact
is that **scalar indices are ~half of total footprint** (53.3% on the base, 46.5% on the sidecar) —
which is the *correct* cost of a fully-indexed resolution spine, not waste, with exactly one
first-principles trim candidate (the raw-`company_name` BTREE). The only live anomaly is a cold-WAN
point-lookup-latency disparity on the `company_name_norm` BTREE (below), which is a watch item, not a
defect.

| Axis | Verdict |
|---|---|
| **Schema typing** | ✅ Clean. 12/15 flat scalar cols, **zero** `List`/`Struct`/`Map`, **zero floats** (so no wide-float waste). `is_generic_domain` is a native **`bool`** (not a stringly-typed flag — the directive's anti-pattern is *absent*). Three micro-recasts exist (`year_founded` int32→int16; sidecar `source_version` int64→int32), all marginal because Lance already constant-/bit-packs them on disk. |
| **Null density** | ✅ Measured exactly, all columns. Resolution keys are well-populated (`pdl_company_id`/`linkedin_url`/`company_name` 0% null; `company_name_norm` 3.10%; `normalized_domain`/`domain` 33.8%). `year_founded` is **63.4% null** — sparse but correctly typed/indexed. No load-bearing key is unexpectedly null. |
| **Fragment topology** | ✅ Optimal. **34 fragments = `ceil(35,446,771 / 1,048,576)`**, 1 data file each, near-uniform (base 79–104 MB, sidecar 100–140 MB). No shattering, no skew → no read-amplification. |
| **Compaction** | ✅ **Not mandated.** 0 tombstoned rows, 0 deletion vectors, 0 fragmentation drift on both. A compaction pass would be a no-op. |
| **Indexing** | ✅ All **10** (base) + **6** (sidecar) scalar indices `num_indexed_rows == 35,446,771, num_unindexed_rows == 0` — 100% trained, zero FEC-style dead-BTREE. Cardinality-correct (BTREE on high-card keys, BITMAP on low-card categoricals). |
| **Pushdown** | ✅ Live-proven. BTREE point lookups → `ScalarIndexQuery`, prune to **1 of 34 fragments**, `refine_filter=--`. BITMAP equality → `ScalarIndexQuery@*_idx(Bitmap)`, sub-second index resolution, no post-scan refine. |
| **Out-of-core compute** | ⚠️ One real gap: the **base ingest worker sets no DuckDB `memory_limit` and no `temp_directory`** (the sidecar worker does). Safe today (7 GiB fits 32 GiB), fragile if the snapshot grows. Fix is two lines. |
| **Storage footprint** | ⚠️ Indices ≈ half of footprint (justified), and the **wipe-republish publish model voids cross-snapshot time-travel** despite a docstring that claims MVCC retention — a governance correction, acceptable for a single-snapshot manual feed. |

---

## 2. Telemetry grid

### 2.1 Footprint, fragments, versions (exact)

| Metric | `pdl_companies` | `pdl_normalized_companies` |
|---|--:|--:|
| Lance version | **v11** (v1 data + v2–v11 = 10 index commits) | **v7** (v1 data + v2–v7 = 6 index commits) |
| `data_storage_version` | **2.1** | **2.1** |
| Rows (exact) | **35,446,771** | **35,446,771** |
| Distinct `pdl_company_id` | 35,446,771 (perfect PK, ledger-verified) | 35,446,771 (1:1 passthrough) |
| Columns | 12 (all flat scalar) | 15 (all flat scalar) |
| Fragments | **34** | **34** |
| Data files | 34 (1 / fragment) | 34 (1 / fragment) |
| Rows / fragment | 33 × 1,048,576 + 1 × 843,763 (avg 1,042,552) | identical |
| **Tombstoned rows** | **0** (0 deletion vectors) | **0** (0 deletion vectors) |
| Data-file bytes (min / avg / max) | 79.2 / **98.7** / 104.2 MB | 99.9 / **127.4** / 139.5 MB |
| **Data on disk (compressed)** | **3.355 GB** (3,354,717,879 B) | **4.333 GB** (4,332,705,302 B) |
| **Scalar indices on disk** | **3.822 GB** (3,821,693,951 B) | **3.767 GB** (3,766,889,276 B) |
| Manifests (`_versions` + `_transactions`) | 71 KB | 49 KB |
| **Total footprint** | **7.176 GB** (73 objects) | **8.100 GB** (60 objects) |
| Indices as % of total | **53.3%** | **46.5%** |
| Indices as % of data | **113.9%** | 86.9% |
| **Uncompressed (decoded Arrow)** | ≈ **7.027 GB** (198.2 B/row) | ≈ **8.098 GB** (228.4 B/row) |
| Data compression (decoded ÷ on-disk) | **2.09×** | **1.87×** |

> **Read-amplification:** none. Point predicates on indexed keys prune to a single 98.7 MB (base) /
> 127 MB (sidecar) data file; the near-uniform fragment sizing means no single fragment dominates a
> scan. The combined two-table index footprint is **7.589 GB (49.7% of the 15.276 GB combined
> total)** — the price of a fully-indexed resolution spine.

### 2.2 Null density (exact, full-scan) + cardinality (≈ HLL)

**`pdl_companies`:**

| Column | Arrow type | Null % | Cardinality (≈) | Note |
|---|---|--:|--:|---|
| `pdl_company_id` | string | 0.000 | **35,446,771** (exact) | perfect PK |
| `company_name` | string | 0.000 | ≈ near-unique (HLL 38.2M) | raw, dirty (modes `x`/`closed`/`test`) |
| `domain` | string | 33.797 | ≈ 22.32 M | |
| `linkedin_url` | string | 0.000 | ≈ near-unique (HLL 37.9M) | **100% populated** |
| `industry` | string | 17.045 | **152** | categorical |
| `employee_size_range` | string | 0.000 | **9** | 8 buckets + residual |
| `year_founded` | int32 | **63.409** | 1,038 (range **[1001, 2026]**) | sparse; `1001` implausible |
| `locality` | string | 17.621 | ≈ 282,113 | |
| `region` | string | 15.764 | ≈ 4,176 | |
| `country` | string | 11.118 | **263** | |
| `snapshot_date` | date32[day] | 0.000 | **1** (constant) | provenance |
| `ingested_at` | timestamp[us,tz] | 0.000 | **1** (constant) | provenance |

**`pdl_normalized_companies`** (adds the materialized blocking keys; re-stores 7 base cols as inline
tiebreaks):

| Column | Arrow type | Null % | Cardinality (≈) | Note |
|---|---|--:|--:|---|
| `pdl_company_id` | string | 0.000 | 35,446,771 (exact) | PK |
| `company_name_norm` | string | 3.099 | ≈ near-unique (HLL 31.98M) | `name_norm()`; null = all-non-ASCII |
| `company_legal_base` | string | 3.099 | ≈ near-unique (HLL 38.37M) | legal-suffix-stripped |
| `normalized_domain` | string | 33.798 | ≈ 22.16 M | |
| `is_generic_domain` | **bool** | 33.798 | **2** | correctly typed flag |
| `linkedin_slug` | string | 0.000 | ≈ near-unique (HLL 47.0M) | 100% populated |
| `company_name` | string | 0.000 | ≈ near-unique | inline (tiebreak/display) |
| `locality` | string | 17.621 | ≈ 282,113 | inline tiebreak |
| `region` | string | 15.764 | ≈ 4,176 | inline tiebreak |
| `country` | string | 11.118 | 263 | inline tiebreak |
| `industry` | string | 17.045 | 152 | inline tiebreak |
| `employee_size_range` | string | 0.000 | 9 | inline tiebreak |
| `year_founded` | int32 | 63.409 | 1,038 (range [1001,2026]) | inline tiebreak |
| `source_version` | **int64** | 0.000 | **1** (constant **[11,11]**) | lineage stamp |
| `built_at` | timestamp[us,tz] | 0.000 | **1** (constant) | provenance |

> **Nested-column trap: absent.** Both schemas are 100% flat scalar — `nested_cols = []` on each.
> No resolution key is array-trapped; no `LABEL_LIST` need exists.

### 2.3 Predicate pushdown (live `explain_plan` / `analyze_plan`)

Every indexed predicate resolves through a `ScalarIndexQuery` node with **`refine_filter=--`** (no
post-scan filter). Wall times are **cold-WAN** (probe host → public R2 endpoint); the load-bearing
proofs are the plan shape, fragment pruning, and `rows_scanned`.

| # | Dataset · predicate | Index node | Frags scanned | `rows_scanned` | Index resolve | Wall (cold WAN) |
|---|---|---|--:|--:|--:|--:|
| A | base · `linkedin_url = '…/blackhawk-liquor-and-smoke'` | `@linkedin_url_idx(BTree)` | **1 / 34** | **1** | 2.65 s · **1 part** | 2.85 s |
| B | base · `country = 'united states'` | `@country_idx(Bitmap)` | 34 / 34 | 9.01 M (matched) | **0.68 s · 1 part** | 11.5 s¹ |
| C | sidecar · `company_name_norm = 'NATIONAL COACH AND BUS FRANCHISING LTD'` | `@company_name_norm_idx(BTree)` | **1 / 34** | **1** | 8.63 s · **270 parts**² | 8.69 s |
| D | sidecar · `NOT is_generic_domain` | `@is_generic_domain_idx(Bitmap)` | 34 / 34 | 22.93 M (matched) | **0.91 s · 1 part** | 13.3 s¹ |

¹ The 11–13 s wall on B/D is **result materialization** of a low-selectivity answer (9.01 M / 22.93 M
`pdl_company_id` values streamed over WAN), **not** a full-table refine — the bitmap index itself
resolves in <1 s and emits `refine_filter=--`. In-region (Modal, peered R2) this collapses by 1–2
orders of magnitude. The 9.01 M `united states` count exactly matches the prior diagnostic.

² **The one anomaly.** The `company_name_norm` BTREE point-lookup loaded **270 index parts / 1.11 M
comparisons** for a single-row exact match, vs `linkedin_url`'s **1 part / 4.1 K comparisons** —
equivalent near-unique BTREEs, a 270× page-load disparity. The lookup is *correct* (1 row, 1
fragment, `ScalarIndexQuery`, `refine_filter=--`); only the page-load count is anomalous. This is a
**watch item**: re-measure in-region before acting — it is most likely a cold-cache prefetch artifact,
but if it reproduces in-region, the `company_name_norm` index is the one rebuild candidate (§5.5).

---

## 3. Schema & index ledger — load-bearing columns

Per column: current type, measured cardinality profile, existing index, and the **required optimal
index on first principles** (cardinality + access shape, independent of current consumers).

### 3.1 `pdl_companies`

| Column | Type | Cardinality | Null % | Existing | Optimal | Verdict |
|---|---|--:|--:|---|---|---|
| `pdl_company_id` | string | PK (35.4 M) | 0 | **BTREE** | BTREE | ✅ correct |
| `company_name` | string | near-unique | 0 | **BTREE** | *BTREE, but low-value* | ⚠️ raw name is a weak join key (case/punct/suffix variance); the indexed-resolution form is the sidecar's `company_name_norm`. **Sole first-principles trim candidate** (§5.2). |
| `linkedin_url` | string | near-unique | 0 | **BTREE** | BTREE | ✅ correct |
| `domain` | string | ≈22.3 M | 33.8 | **BTREE** | BTREE | ✅ correct |
| `locality` | string | ≈282 K | 17.6 | **BTREE** | BTREE | ✅ correct (high-card for bitmap; equality + prefix) |
| `year_founded` | int32 | 1,038 (range) | 63.4 | **BTREE** | BTREE | ✅ correct — BTREE services the *range* scans ("founded since X") a bitmap cannot. Recast int32→int16 (§5.3). |
| `industry` | string | 152 | 17.0 | **BITMAP** | BITMAP | ✅ correct |
| `country` | string | 263 | 11.1 | **BITMAP** | BITMAP | ✅ correct (proven live, §2.3-B) |
| `region` | string | ≈4,176 | 15.8 | **BITMAP** | BITMAP (borderline) | ✅ acceptable — 4,176 distinct is the high end for bitmap, but values are low-selectivity geos (england 2.76 M, california 1.32 M) and Lance roaring-compresses the posting lists; BTREE would not help (no range use). Monitor; not mandated. |
| `employee_size_range` | string | 9 | 0 | **BITMAP** | BITMAP | ✅ correct |
| `snapshot_date` | date32 | 1 (const) | 0 | — | — | ✅ correct (constant → index valueless) |
| `ingested_at` | timestamp | 1 (const) | 0 | — | — | ✅ correct (provenance) |

### 3.2 `pdl_normalized_companies`

| Column | Type | Cardinality | Null % | Existing | Optimal | Verdict |
|---|---|--:|--:|---|---|---|
| `pdl_company_id` | string | PK (35.4 M) | 0 | **BTREE** | BTREE | ✅ |
| `company_name_norm` | string | near-unique | 3.1 | **BTREE** | BTREE | ✅ (the materialized fix for read-time `name_norm()`); page-load watch §5.5 |
| `company_legal_base` | string | near-unique | 3.1 | **BTREE** | BTREE | ✅ |
| `normalized_domain` | string | ≈22.2 M | 33.8 | **BTREE** | BTREE | ✅ |
| `linkedin_slug` | string | near-unique | 0 | **BTREE** | BTREE | ✅ |
| `is_generic_domain` | bool | 2 | 33.8 | **BITMAP** | BITMAP | ✅ correct type + index (proven live, §2.3-D) |
| `company_name`, `locality`, `region`, `country`, `industry`, `employee_size_range`, `year_founded` | mixed | inline | — | — | — | ✅ **deliberately unindexed** — post-block tiebreaks over an already-tiny candidate set; the base table carries these indexed, hydration is by PK. Consistent with the consumer contract. |
| `source_version` | int64 | 1 (const) | 0 | — | — | ✅ correct; recast int64→int32 (§5.3) |
| `built_at` | timestamp | 1 (const) | 0 | — | — | ✅ provenance |

**Net:** no high-card resolution key lacks a BTREE; no low-card categorical lacks a justified BITMAP;
no index is mismatched to its cardinality except the noted `region`-bitmap borderline (acceptable).
The only index whose *value* (not correctness) is questionable on first principles is the raw
`company_name` BTREE on the base (§5.2).

---

## 4. Execution runtime specs

Two distinct envelopes: a **read/query** path (runs anywhere; index + projection pushdown make it
I/O-minimal) and a **mutate/rebuild** path (the heavy external-sort that builds the near-unique
string BTREEs — the only OOM-capable operation in the lifecycle).

### 4.1 Read / query path (any host)

```sql
-- DuckDB over a Lance Arrow stream. Cardinality/null profiling, ad-hoc analytics.
PRAGMA threads=8;                              -- match cores; raise to 16 on a fat box
SET memory_limit='24GB';                       -- streaming aggregates stay bounded well under this
SET temp_directory='/<fast-nvme>/duckdb_spill';-- spill target on local NVMe — NEVER root FS
SET preserve_insertion_order=false;            -- frees the order buffer on big aggregates
```

- **Always project** (`scanner(columns=[…])`) — only requested columns egress from R2 (proven: a
  point lookup reads **3.90 KB**).
- **Filter on the raw indexed column, never `func(col)`** — `WHERE company_name = 'X'` /
  `WHERE company_name_norm = 'X'` → `ScalarIndexQuery`; `WHERE name_norm(company_name)='X'` →
  35.4 M-row full scan (the 67,646× cliff in the prior diagnostic). For normalized-name blocking,
  query the **sidecar's `company_name_norm`**, which is materialized + BTREE-indexed.
- **At scale, stream** (`to_arrow_reader(batch_size)`) into Lance/DuckDB — never `to_pandas()`.

### 4.2 Mutate / rebuild path (the index external-sort)

The build/`reindex` sorts near-unique 35.4 M-row string columns (`company_name`, `linkedin_url`,
`company_name_norm`, `linkedin_slug`, …) to construct each BTREE — the memory-bounded operation.
Verified production envelope (both PDL Modal workers):

```python
# Modal worker:  memory=32768 (32 GiB) · cpu=8.0 · ephemeral_disk=524288 (512 GiB local NVMe)
image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2", "lancedb>=0.15", "pylance>=7", "pyarrow>=17", "boto3>=1.35", ...
).env({
    "LANCE_BYPASS_SPILLING": "true",   # ← the load-bearing lever (see below)
})
# DuckDB connection (per fleet convention — CMS / USASpending / the sidecar worker):
con.execute("PRAGMA threads=8;")
con.execute("SET memory_limit='24GB';")                       # < the 32 GiB container floor
con.execute("SET temp_directory='/tmp/<feed>/duckdb_spill';") # on the 512 GiB ephemeral NVMe
# Free the ~7–8 GiB Arrow table BEFORE the index sorts run:  del table
```

| Knob | Value | Why |
|---|---|---|
| Modal `memory` | **32 GiB** | Holds the ~7–8 GiB decoded Arrow table + one column sort with headroom. |
| Modal `cpu` | **8.0** | Matches `PRAGMA threads=8`. |
| Modal `ephemeral_disk` | **512 GiB NVMe** | DuckDB spill + local Lance staging (data + `_indices`) before the boto3 publish. |
| DuckDB `memory_limit` | **`24GB`** | Bounded below the container floor; spills the rest. |
| DuckDB `temp_directory` | **`/tmp/<feed>/duckdb_spill`** | High-I/O local NVMe — **explicitly not** the overlay root FS. |
| `LANCE_BYPASS_SPILLING` | **`true`** | Lance's bounded `ExternalSorter` under-sizes its pool and OOMs on multi-million-row string-column BTREE sorts (lance-format/lance#2650). Forcing the in-memory sort is the only path that completes at this scale; it fits because `del table` frees the Arrow buffer first and 32 GiB > one column sort. |

> **On `LANCE_MEM_POOL_SIZE` (named in the directive):** it is a real Lance variable, but **not the
> lever this write path uses.** PDL bypasses the spill pool entirely (`LANCE_BYPASS_SPILLING=true`)
> rather than sizing it. The pool-sizing alternative — `LANCE_MEM_POOL_SIZE` (FairSpillPool) +
> `LANCE_MAX_TEMP_DIRECTORY_SIZE` — is used by exactly one fleet feed (`materialize_epa_history.py`,
> 24 GiB pool / 250 GB spill). It becomes relevant for PDL **only** if a future build is forced to
> spill the sort (e.g. RAM shrinks below the working set); the bypass is the tested default. Do not
> introduce a phantom value.

### 4.3 Spill routing — verify the base worker

`temp_directory`/`TMPDIR` must point at the container's ephemeral NVMe, never the overlay root.

| Worker | `threads` | `memory_limit` | `temp_directory` | Status |
|---|---|---|---|---|
| `pdl_normalized_companies.py` | 8 | `24GB` | `{SCRATCH}/duckdb_spill` (NVMe) | ✅ correct |
| `free_company_dataset.py` (base) | 8 | **— (unset)** | **— (unset)** | ⚠️ **gap** — see §5.1 |

The base ingest relies on the full ~7 GiB Arrow materialization fitting the 32 GiB container, with no
`memory_limit` and no `temp_directory`. It works **today**. If a future PDL snapshot grows past the
RAM envelope, DuckDB would spill to its default temp (potentially the size-limited/slow root FS) or
OOM. The fix is two lines (§5.1).

---

## 5. Optimization blueprint (sequenced, executable)

**Posture: this is a hardening + governance list, not a rebuild.** No compaction, no reindex, and no
schema recast is *mandated* by the physics. Ordered by value ÷ blast-radius. Items 1, 4 are pure
code/doc edits (zero data risk); 2, 3, 5 touch the dataset and are gated on the next overwrite or an
explicit decision. **All index/data mutations follow the existing isolation rule: build on local NVMe
(no R2 multipart), then boto3 wipe-republish — never an in-place R2 Lance write.**

1. **[code · zero data risk] Close the base worker's spill gap.** Add to the DuckDB connection in
   `pipelines/pdl_companies/free_company_dataset.py::ingest_pdl_companies` (mirroring the sidecar and
   the CMS/USASpending convention):
   ```python
   con.execute("SET memory_limit='24GB';")
   con.execute(f"SET temp_directory='{SCRATCH_DIR}/duckdb_spill';")  # + os.makedirs(...) like the sidecar
   ```
   Removes the only OOM/root-FS-spill exposure in the lifecycle. Blast radius: the ingest worker only.

2. **[data · gated on decision] Re-evaluate the raw `company_name` BTREE on `pdl_companies`.** It is
   the largest first-principles trim candidate: a near-unique 35.4 M-key index (a meaningful slice of
   the 3.822 GB index footprint) on a column whose *raw* form is a poor resolution key — the dataset's
   own modes are junk sentinels (`x`, `closed`, `test`), and cross-source name matching goes through
   the sidecar's `company_name_norm` (indexed) by design. Dropping it would not be a mutation of data,
   only of the index set, and is reversible (`reindex`). **Gate:** confirm no access pattern needs raw
   exact-name equality/prefix on the base; if none, drop it on the next `reindex` and reclaim ~0.5–1 GB.
   `locality_idx` (≈282 K distinct) is a weaker second candidate on the same logic — keep unless geo
   point-lookups on the base are real.

3. **[data · fold into next overwrite only] Type micro-recasts.** Marginal; do **not** trigger a
   standalone rebuild for them — apply on the next snapshot/`reindex`:
   - `year_founded` `int32 → int16` (observed range [1001, 2026] ⊂ int16). ~2 B/row decoded; on-disk
     gain ~nil (Lance bit-packs).
   - sidecar `source_version` `int64 → int32` (constant 11). Constant-encoded on disk already.
   - `employee_size_range` **stays `VARCHAR`** (correctly BITMAP-indexed); an in-engine DuckDB `ENUM`
     is a compute-boundary nicety, not a storage change, and is not worth the contract churn.

4. **[doc · zero data risk] Correct the time-travel claim.** `free_company_dataset.py:48` states
   "immutable-version MVCC retains prior snapshots for time travel." The publish path
   (`_replace_r2_prefix`: wipe the R2 prefix, re-upload the fresh local build) means **only the latest
   snapshot's fragments physically exist on R2** — cross-snapshot time travel is *voided* (a
   `version=N` from a prior drop would reference wiped data files). This is acceptable for a
   manual-single-snapshot feed (and it auto-reclaims, so `cleanup_old_versions` is unnecessary), but
   the docstring should say so. If multi-snapshot history is ever wanted, the publish model must
   change to a non-wiping `append`/`merge_insert` + `cleanup_old_versions(retain_versions=…)`.

5. **[measure-first] In-region re-measure the `company_name_norm` BTREE point-lookup** (§2.3-C: 270
   parts / 1.11 M comparisons cold-WAN vs `linkedin_url`'s 1 part). Run the same `analyze_plan` from a
   Modal worker (peered R2). If parts-loaded stays ~1, it was a cold-cache artifact — no action. If it
   reproduces, rebuild that single index (`reindex`, local-NVMe sort, boto3 republish) under the §4.2
   envelope. Do **not** rebuild speculatively on a cold-WAN sample.

6. **[non-structural · noted, out of scope] Data-quality sentinels.** `year_founded` min `1001` is
   implausible, and `company_name` junk modes (`x`/`closed`/`test`) still flow into
   `company_name_norm`. Name-sentinel suppression remains open (already declared out-of-scope in the
   build report §8). Not a storage/compute issue; a downstream resolution-quality one.

---

## 6. Reproduction (read-only)

```bash
# pylance 7 / pyarrow 24 / duckdb 1.5.x / boto3; R2 creds via Doppler core-x/prd
doppler run -p core-x -c prd -- uv run --no-project \
  --with 'pylance>=7' --with 'pyarrow>=17' --with 'duckdb>=1.5,<2' --with 'boto3>=1.35' \
  python3 <probe.py>
```

The probe calls only `lance.dataset()` (R2-direct), `schema`, `get_fragments()`, `list_indices()`,
`stats.index_stats()`, `scanner().explain_plan()/analyze_plan()`, a bounded `to_reader()` decoded-width
sample, one full DuckDB streaming aggregate (null density + `approx_count_distinct` + int min/max),
and boto3 `list_objects_v2` (object sizes). **Zero mutation:** no `write_dataset`, no
`create_scalar_index`, no `delete`, no `optimize`, no R2 put/delete.
