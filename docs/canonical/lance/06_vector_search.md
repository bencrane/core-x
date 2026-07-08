# Vector Indices & ANN Search — IVF_PQ / HNSW, nprobes, refine, multivector

> Canonical upstream reference — fetched 2026-07-08 from official Lance documentation and source repositories. Documents upstream behavior as-is; this is NOT core-x doctrine. Treat the version numbers and API signatures here as fetched-date ground truth.
>
> Primary sources:
> - https://raw.githubusercontent.com/lance-format/lance/main/python/python/lance/dataset.py — pylance source of truth; `LanceDataset.create_index` and `LanceDataset.scanner`/`ScannerBuilder.nearest` verbatim signatures and docstrings.
> - https://docs.lancedb.com/indexing/vector-index — LanceDB vector-index type catalog, parameter sizing heuristics, and search-parameter semantics.
> - https://lance.org/quickstart/vector-search/ — Lance vector-search quickstart (`create_index` + `to_table(nearest=...)` examples).
> - https://lance-format.github.io/lance-python-doc/index-and-search.html — pylance "Indexing and Searching" API-reference page (index-type list, `nearest` dict keys). (The repo and doc site were renamed from the `lancedb` org to the `lance-format` org; `github.com/lancedb/lance` now 301-redirects to `github.com/lance-format/lance`, and the old `lancedb.github.io/lance-python-doc/` doc host now 404s.)
> - https://pypi.org/project/pylance/ , https://pypi.org/project/lancedb/ — released version numbers.

Scope: Creating vector (ANN) indices on Lance datasets with the pylance `LanceDataset.create_index` API — IVF_FLAT / IVF_PQ / IVF_SQ / IVF_RQ and their HNSW variants — and running approximate-nearest-neighbor queries via `scanner`/`to_table(nearest=...)` with `nprobes`, `refine_factor`, prefilter, distance range, and multi-vector (batch) query semantics.

---

## Version ground truth (as of 2026-07-08)

| Package | Latest released | Notes |
|---|---|---|
| `pylance` (the `lance` Python module) | **8.0.0** (2026-07-01) | The `create_index` API below is quoted from the `main` branch source, which tracks 8.x. Marked **Experimental API** in-source. |
| `lancedb` (the LanceDB database) | **0.34.0** (2026-07-02) | Separate package; wraps Lance with its own `Table.create_index(config=...)` builder. This file documents **pylance**; LanceDB's table API lives in `11_lancedb_table_api.md`. |
| `duckdb` | **1.5.4** (2026-06-17) | Relevant only for reading Lance via Arrow; see `10_duckdb_arrow_interop.md`. |

> Two distinct surfaces exist. **pylance** (`import lance`) operates directly on a Lance dataset — that is what this file documents. **LanceDB** (`import lancedb`) is a higher-level database with a different builder-style index API (`Index.ivf_pq(...)`, `minimum_nprobes`/`maximum_nprobes` config objects). Do not mix the two APIs. This file is pylance-only except where explicitly cross-linked.

---

## 1. The vector index-type set

Lance vector indices are all **IVF-based** (Inverted File / coarse quantizer partitioning), optionally composed with a per-partition graph (HNSW) and/or a residual quantizer (PQ / SQ / RQ). The catalog (per docs.lancedb.com):

| `index_type` | Structure | Quantization | Primary use |
|---|---|---|---|
| `IVF_FLAT` | IVF, brute-force within partition | none (full fp32 vectors) | Highest recall, largest footprint; also the path for binary vectors. |
| `IVF_PQ` | IVF | Product Quantization | Default workhorse — strong compression, good recall. |
| `IVF_SQ` | IVF | Scalar Quantization | Lighter than PQ to build; int8 mapping. |
| `IVF_RQ` | IVF | RaBitQ-style quantization | 1-bit-per-dim default; supports `approx_mode` speed/recall tuning. |
| `IVF_HNSW_FLAT` | IVF + HNSW graph | none | Graph traversal, full-precision vectors. |
| `IVF_HNSW_PQ` | IVF + HNSW graph | Product Quantization | Graph + compression. |
| `IVF_HNSW_SQ` | IVF + HNSW graph | Scalar Quantization | Best recall/latency/size trade-off per LanceDB docs. |

> **Footgun — docstring vs. reality.** The pylance `create_index` docstring states only `"IVF_PQ, IVF_HNSW_PQ and IVF_HNSW_SQ are supported now."` The broader catalog (`IVF_FLAT`, `IVF_SQ`, `IVF_RQ`, `IVF_HNSW_FLAT`) is documented on docs.lancedb.com and accepted by the engine, but the Python docstring lags. There is a tracked gap where `IVF_HNSW_FLAT` was documented but historically not wired through the Python SDK (lancedb/lancedb#3331). **If a variant errors as unsupported, fall back to `IVF_PQ` or `IVF_HNSW_SQ`, which are unambiguously supported in pylance.**

### Quantization notes (verbatim from source)

- **SQ**: *"The SQ (Scalar Quantization) is available for only `IVF_HNSW_SQ` index type … it maps the float vectors to integer vectors, each integer is of `num_bits`, now only 8 bits are supported."*
- **PQ `num_bits`**: *"The number of bits for PQ (Product Quantization). Default is 8. Only 4, 8 are supported."*
- **RQ `num_bits`**: *"The number of bits for RQ (Rabit Quantization). Default is 1."*

---

## 2. `LanceDataset.create_index` — full signature

Quoted verbatim from `python/python/lance/dataset.py` (`main`, pylance 8.x). Marked **Experimental API** in-source.

```python
def create_index(
    self,
    column: Union[str, List[str]],
    index_type: str,
    name: Optional[str] = None,
    metric: str = "L2",
    replace: bool = False,
    num_partitions: Optional[int] = None,
    ivf_centroids: Optional[
        Union[np.ndarray, pa.FixedSizeListArray, pa.FixedShapeTensorArray]
    ] = None,
    pq_codebook: Optional[
        Union[np.ndarray, pa.FixedSizeListArray, pa.FixedShapeTensorArray]
    ] = None,
    num_sub_vectors: Optional[int] = None,
    accelerator: Optional[Union[str, "torch.Device"]] = None,
    index_cache_size: Optional[int] = None,
    shuffle_partition_batches: Optional[int] = None,
    shuffle_partition_concurrency: Optional[int] = None,
    # experimental parameters
    ivf_centroids_file: Optional[str] = None,
    precomputed_partition_dataset: Optional[str] = None,
    storage_options: Optional[Dict[str, str]] = None,
    filter_nan: bool = True,
    train: bool = True,
    # distributed indexing parameters
    fragment_ids: Optional[List[int]] = None,
    index_uuid: Optional[str] = None,
    *,
    target_partition_size: Optional[int] = None,
    streaming_sample_rate: Optional[int] = None,
    streaming_coreset_rate: Optional[int] = None,
    streaming_refine_passes: Optional[int] = None,
    skip_transpose: bool = False,
    progress_callback: Optional[Callable[[IndexProgress], None]] = None,
    **kwargs,
) -> LanceDataset:
```

### Parameter table

| Parameter | Type | Default | Meaning / accepted values |
|---|---|---|---|
| `column` | `str \| List[str]` | — | Column to index. A single vector column for standard ANN. |
| `index_type` | `str` | — | See §1. Docstring guarantees `IVF_PQ`, `IVF_HNSW_PQ`, `IVF_HNSW_SQ`; catalog adds `IVF_FLAT`, `IVF_SQ`, `IVF_RQ`, `IVF_HNSW_FLAT`. |
| `name` | `str \| None` | `None` | Index name; auto-generated from column name if omitted. |
| `metric` | `str` | `"L2"` | Distance metric: `"L2"` (alias `"euclidean"`), `"cosine"`, or `"dot"` (dot product). |
| `replace` | `bool` | `False` | Replace an existing index of the same name if present. |
| `num_partitions` | `int \| None` | `None` | Number of IVF partitions. **Deprecated** in-source: *"Use `target_partition_size` instead."* Still functional. |
| `ivf_centroids` | `np.ndarray \| pa.FixedSizeListArray \| pa.FixedShapeTensorArray \| None` | `None` | Pre-trained `num_partitions × dimension` K-means centroids. If omitted, a new KMeans model is trained. |
| `pq_codebook` | `np.ndarray \| pa.FixedSizeListArray \| pa.FixedShapeTensorArray \| None` | `None` | Pre-trained PQ codebook, shape `num_sub_vectors × (2^num_bits) × (dim // num_sub_vectors)`. `num_bits` defaults to 8. |
| `num_sub_vectors` | `int \| None` | `None` | Number of PQ sub-vectors. **Required** for any `*PQ*` index type. Suggested start: `dimension // 8`. |
| `accelerator` | `str \| torch.Device \| None` | `None` | `"cuda"` (Nvidia) or `"mps"` (Apple Silicon) to GPU-train. Requires PyTorch. **GPU is supported only for `IVF_PQ`; other types silently fall back to CPU.** |
| `index_cache_size` | `int \| None` | `None` (256) | Index cache entries. Default 256. |
| `shuffle_partition_batches` | `int \| None` | `None` (10240) | Row-group-sized batches per shuffle partition. Lower → less memory, slower. |
| `shuffle_partition_concurrency` | `int \| None` | `None` (2) | Shuffle partitions processed concurrently. Lower → less memory, slower. |
| `ivf_centroids_file` | `str \| None` | `None` | *(experimental)* Path to a centroids file. |
| `precomputed_partition_dataset` | `str \| None` | `None` | *(experimental)* Precomputed partition assignments. |
| `storage_options` | `Dict[str,str] \| None` | `None` | Object-store connection params (credentials, endpoint, region). See `07_storage_object_stores.md`. |
| `filter_nan` | `bool` | `True` | Keep the null/NaN filter. **`False` is UNSAFE** — crashes if any null/NaN present; small speed boost only. |
| `train` | `bool` | `True` | If `False`, create an empty (untrained) index structure to populate later. |
| `fragment_ids` | `List[int] \| None` | `None` | Restrict indexing to specific fragments — enables distributed/fragment-level indexing; creates a segment but does **not** commit. |
| `index_uuid` | `str \| None` | `None` | UUID for the segment written by this call (distributed path). |
| `target_partition_size` | `int \| None` (kw-only) | `None` | Preferred over `num_partitions`; partition count derived from target size. If unset, defaults per index type. |
| `streaming_sample_rate` | `int \| None` (kw-only) | `None` | Incremental IVF kmeans; samples ≤ `num_partitions * streaming_sample_rate` vectors per step. |
| `streaming_coreset_rate` | `int \| None` (kw-only) | `None` | Final weighted-coreset budget = `num_partitions * streaming_coreset_rate`. |
| `streaming_refine_passes` | `int \| None` (kw-only) | `None` | Extra streaming Lloyd refinement passes. |
| `skip_transpose` | `bool` (kw-only) | `False` | Skip the transpose step. |
| `progress_callback` | `Callable[[IndexProgress], None] \| None` (kw-only) | `None` | Receives `lance.progress.IndexProgress` events during build. |
| `**kwargs` | — | — | Additional build params passed through — this is how `num_bits`, `m`, `ef_construction`, `max_level`, `index_file_version` are supplied (see §3). |

### Required-parameter rules (verbatim)

- *"If `index_type` is `"IVF_*"`, then the following parameters are required: `num_partitions`."*
- *"If `index_type` is with `"PQ"`, then the following parameters are required: `num_sub_vectors`."*

---

## 3. Index-type-specific params passed via `**kwargs`

These are **not** named positional parameters; they flow through `**kwargs` into the build. Documented in the `create_index` docstring:

**`IVF_PQ` optional:**
| kwarg | Meaning | Default / accepted |
|---|---|---|
| `ivf_centroids` | Existing K-means centroids for IVF clustering. | — |
| `num_bits` | Bits for PQ. | Default `8`; only `4`, `8` supported. |
| `index_file_version` | Index file format version. | Default `"V3"`. |

**`IVF_RQ` optional:**
| kwarg | Meaning | Default |
|---|---|---|
| `num_bits` | Bits for RQ (RaBitQ). | Default `1`. |

**`IVF_HNSW_*` optional:**
| kwarg | Meaning |
|---|---|
| `max_level` | Max number of levels in the HNSW graph. |
| `m` | Number of edges per node in the HNSW graph. |
| `ef_construction` | Number of nodes examined during graph construction. |

> The LanceDB docs describe `m` as *"the number of neighbors to select for each vector in the HNSW graph"* and `ef_construction` as *"the number of candidates to evaluate during the construction of the HNSW graph."* Semantically equivalent.

### Partition / sub-vector sizing heuristics (docs.lancedb.com)

- `num_partitions` ≈ `num_rows // 4096` for `IVF_PQ` / `IVF_RQ`; ≈ `num_rows // 1_048_576` for HNSW-backed variants.
- `num_sub_vectors` ≈ `dimension // 8`.
- Query-time `ef` (HNSW): start around `1.5 * k`.

---

## 4. ANN query — `scanner(nearest=...)` / `to_table(nearest=...)`

Two equivalent entry points:

1. **Dict form** — pass a `nearest` dict to `dataset.scanner(...)`, `dataset.to_table(...)`, or `dataset.to_batches(...)`.
2. **Builder form** — `dataset.scanner_builder().nearest(...)` (typed keyword args, adds `ef`, `query_parallelism`, `approx_mode`).

### 4.1 The `nearest` dict (verbatim from `scanner` docstring)

```python
{
    "column": "<embedding col name>",
    "q": "<query vector as pa.Float32Array>",
    "k": 10,
    "minimum_nprobes": 1,
    "maximum_nprobes": 50,
    "refine_factor": 1,
    "distance_range": (0.0, 1.0),
}
```

Additional recognized keys (seen in source examples): `"metric"`, `"nprobes"` (sets both min and max), `"use_index"` (bool).

### 4.2 `ScannerBuilder.nearest` — full typed signature (verbatim)

```python
def nearest(
    self,
    column: str,
    q: QueryVectorLike,
    k: Optional[int] = None,
    metric: Optional[str] = None,
    nprobes: Optional[int] = None,
    minimum_nprobes: Optional[int] = None,
    maximum_nprobes: Optional[int] = None,
    refine_factor: Optional[int] = None,
    use_index: bool = True,
    ef: Optional[int] = None,
    query_parallelism: Optional[int] = None,
    approx_mode: Literal["fast", "normal", "accurate"] = "normal",
    distance_range: Optional[tuple[Optional[float], Optional[float]]] = None,
) -> ScannerBuilder:
```

### Query-parameter table

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `column` | `str` | — | The indexed embedding column. |
| `q` | `QueryVectorLike` | — | Single query vector, **or** a 2-D array / list of vectors for batch search (see §5). |
| `k` | `int \| None` | — | Number of nearest rows to return (top-k). |
| `metric` | `str \| None` | index default | Override distance metric (`L2` / `cosine` / `dot`). Must be compatible with the index's build metric. |
| `nprobes` | `int \| None` | — | Convenience: sets both `minimum_nprobes` and `maximum_nprobes` to this value. Setting min == max **disables** adaptive probing. |
| `minimum_nprobes` | `int \| None` | — | Partitions **always** scanned. |
| `maximum_nprobes` | `int \| None` | — | Upper bound on partitions scanned. When a filter is present, Lance starts at `minimum_nprobes` and extends toward `maximum_nprobes` only if fewer than `k` rows survive the filter (adaptive). |
| `refine_factor` | `int \| None` | — | Retrieve `k * refine_factor` candidates from the quantized index, then **re-rank in memory** using full-precision vectors and return the top `k`. Improves recall at the cost of a `take` + rescore. `refine_factor=1` (or `None`) = no refinement. |
| `use_index` | `bool` | `True` | If `False`, forces exact (flat) KNN, ignoring the vector index. |
| `ef` | `int \| None` | — | HNSW query-time exploration factor (`IVF_HNSW_*` only). Start ≈ `1.5 * k`. |
| `query_parallelism` | `int \| None` | `0` | Per-query partition-search concurrency. `0` = auto (sequential today); `-1` = CPU-pool size; `1` = sequential; `>=2` = partition-parallel, clamped to CPU-pool size. |
| `approx_mode` | `Literal["fast","normal","accurate"]` | `"normal"` | Speed/recall tradeoff. **Only affects RQ-quantized indexes (`IVF_RQ`)**; other index types ignore it. `fast` → lower latency/recall, `accurate` → higher recall/latency. |
| `distance_range` | `tuple[float\|None, float\|None] \| None` | `None` | `(low, high)` distance band filter; only rows whose distance falls in range are returned. Either bound may be `None`. |

### 4.3 Prefilter vs. postfilter (from `scanner` docstring)

The `prefilter` argument lives on `scanner(...)`, **not** in the `nearest` dict:

- **`prefilter=True`** — the SQL/expression `filter` is applied **before** the vector query. *"More correct results but … more costly … generally good when the filter is highly selective."*
- **`prefilter=False`** (default) — filter applied **after** the vector query. *"Performs well but the results may have fewer than the requested number of rows (or be empty) if the rows closest to the query do not match the filter … good when the filter is not very selective."*

> **Footgun:** with `prefilter=False` and a selective filter you can silently get **fewer than `k`** (or zero) rows back. If you must have exactly `k` matching a selective predicate, set `prefilter=True`.

### 4.4 The `_distance` / `_score` output column

From the `scanner`/`to_table` docstring: *"a … column (`_distance`, `_score`) is added to the end of the output even when a projection is applied."* For vector (`nearest`) queries the appended column is **`_distance`** (lower = closer, in the index's metric); for full-text search it is `_score` (BM25). The column is added even if you project a subset of columns.

### 4.5 Related `scanner` knobs for ANN

| `scanner` arg | Default | Relevance to ANN |
|---|---|---|
| `prefilter` | `False` | See §4.3. |
| `fast_search` | `False` | If `True`, search **only indexed fragments** — faster but **skips recently appended, unindexed rows**. |
| `use_scalar_index` | `True` | Whether scalar indices assist the accompanying filter. |
| `filter` | `None` | SQL where-clause / `pa.compute.Expression` combined with the vector search (pre- or post-, per `prefilter`). |
| `index_segments` | `None` | Restrict vector search to specific index-segment UUIDs (distributed indexing). |

---

## 5. Multi-vector (batch) query

`q` may be a 2-D array-like or a list of vectors (fixed-size vector columns only). Behavior, verbatim:

> *"Lance runs a batch nearest-neighbor query, returns up to `k` rows for each query vector, and adds an Int32 non-null `query_index` as the first output column to identify the source query for each result row."*

Rules and footguns:

- **Flattened 1-D input is rejected** — a 1-D array whose length happens to be a multiple of the vector dimension raises rather than being reshaped. Pass an explicit 2-D array or a list of vectors.
- A dataset that **already contains a `query_index` column cannot** be used for batch search (name collision).
- When `use_index=True` and a vector index exists, each query vector goes through the index path; otherwise the flat batch path is used.

> This is *batch multi-query*, distinct from *multi-vector-per-row* document storage. For per-row multi-vector storage plus hybrid (vector + full-text/BM25) fusion, that surface is in **LanceDB** — see `11_lancedb_table_api.md`. This file covers pylance vector search only.

### Multiple indices on one dataset

A dataset may carry several indices simultaneously — e.g., one vector index per embedding column plus BTREE/BITMAP scalar indices on filter columns (see `05_scalar_indices.md`). A `nearest` query targets exactly one vector column via `nearest["column"]`; scalar indices on other columns accelerate the accompanying `filter` (subject to `use_scalar_index`).

---

## 6. Worked example — build IVF_PQ, run a filtered top-k search

```python
import lance
import numpy as np
import pyarrow as pa

# Assume a dataset with a 768-dim "vector" column and a "category" column.
ds = lance.dataset("s3://data-sink/active/embeddings.lance")

# --- Build an IVF_PQ index (cosine) ---
ds.create_index(
    column="vector",
    index_type="IVF_PQ",
    metric="cosine",
    num_partitions=256,   # ~ num_rows // 4096; deprecated in favor of target_partition_size
    num_sub_vectors=96,   # ~ dimension // 8  -> 768 // 8
    num_bits=8,           # via **kwargs; 4 or 8
    replace=True,
)

# --- Filtered ANN top-k, refined, with a selective prefilter ---
q = np.random.rand(768).astype(np.float32)

tbl = ds.to_table(
    nearest={
        "column": "vector",
        "q": q,
        "k": 10,
        "minimum_nprobes": 20,
        "maximum_nprobes": 50,
        "refine_factor": 5,   # fetch 50 candidates, rescore, return 10
    },
    filter="category = 'geography'",
    prefilter=True,           # apply the filter BEFORE the vector search
)

# Results carry an appended `_distance` column (lower = closer).
print(tbl.column_names)       # [...projected cols..., "_distance"]
```

Batch (multi-query) variant:

```python
qs = np.random.rand(4, 768).astype(np.float32)   # explicit 2-D — NOT flattened
batch = ds.to_table(nearest={"column": "vector", "q": qs, "k": 5})
# batch has a leading Int32 `query_index` (0..3) plus `_distance`.
```

Force exact/flat search (no index) for a ground-truth recall check:

```python
gt = ds.to_table(nearest={"column": "vector", "q": q, "k": 10, "use_index": False})
```

---

## 7. Deprecations, renames, and footguns

- **`num_partitions` is deprecated** in the pylance source in favor of the keyword-only `target_partition_size`. Both still work; new code should prefer `target_partition_size`.
- **Docstring under-reports index types** — only `IVF_PQ` / `IVF_HNSW_PQ` / `IVF_HNSW_SQ` are named as "supported now," despite the wider catalog being real. Verify a variant works before depending on it (lancedb/lancedb#3331 tracked `IVF_HNSW_FLAT` doc/SDK drift).
- **`filter_nan=False` is UNSAFE** — crashes on any null/NaN. Leave it `True` unless the column is provably non-null.
- **`accelerator` GPU path is `IVF_PQ`-only** — passing `"cuda"`/`"mps"` for other types silently falls back to CPU.
- **`prefilter=False` can return < k rows** on a selective filter (post-filtering after ANN).
- **`fast_search=True` skips unindexed fragments** — freshly appended rows are invisible until indexed/compacted (see `08_compaction_maintenance.md`).
- **`approx_mode` only affects `IVF_RQ`** — a no-op silently ignored by every other index type.
- **Batch query rejects flat 1-D input** and collides with an existing `query_index` column.
- **pylance vs. LanceDB API mismatch** — LanceDB (the DB, `11_lancedb_table_api.md`) uses `Index.ivf_pq(...)` config objects and a `create_index(config=...)` builder; do not copy pylance `create_index(column, index_type, ...)` calls into LanceDB code or vice versa.

---

## 8. Relevance to core-x

> **Relevance to core-x:** The core-x data plane (SAM.gov Opps and the resolution feeds) stores **no vector columns today** — resolution keys carry hard `BTREE` scalar indices (`05_scalar_indices.md`), not ANN indices. This file is forward-looking: it applies only when a future embedding/vector feed lands in the R2 system of record (`s3://data-sink/active/`). When that happens: (1) build indices against the same append-only immutable-fragment Lance dataset, passing R2 `storage_options` exactly as the scalar-index and write paths do (`07_storage_object_stores.md`); (2) prefer `IVF_PQ` (GPU-trainable, unambiguously supported) or `IVF_HNSW_SQ` (best recall/latency/size); (3) remember `fast_search`/index staleness on appended fragments — new embeddings are invisible to ANN until the index is optimized (`08_compaction_maintenance.md`); and (4) the `_distance` column plus `refine_factor` rescoring compose cleanly with DuckDB downstream via zero-copy Arrow (`10_duckdb_arrow_interop.md`).

---

## 9. Unverified / needs confirmation

- **`index_file_version` accepted values** — the docstring states default `"V3"` for `IVF_PQ` but does not enumerate the full set of legal versions. Confirm against the format spec (`01_file_format.md`) before pinning a non-default value.
- **`IVF_SQ` / `IVF_HNSW_FLAT` availability in pylance 8.0.0** — documented in the LanceDB catalog and accepted by the engine, but the pylance docstring omits them and issue #3331 showed historical SDK drift for `IVF_HNSW_FLAT`. Verify empirically on the installed pylance build before relying on either.
- **Whether `ef` is accepted inside the `nearest` **dict** form** — it is a typed parameter on `ScannerBuilder.nearest`; the raw `nearest` dict example in the `scanner` docstring does not list `ef`. If dict-form `ef` is silently ignored, use the builder form for HNSW `ef` tuning.
- **`metric` casing** — source default is the string `"L2"` (uppercase). Lowercase `"l2"`/`"cosine"`/`"dot"` appear in the LanceDB docs and examples; both are believed accepted (case-insensitive) but this was not confirmed against the parser.
