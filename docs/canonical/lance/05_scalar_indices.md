# Scalar Indices — BTREE, BITMAP, LABEL_LIST, INVERTED/FTS, NGRAM (and any others)

> Canonical upstream reference — fetched 2026-07-08 from official Lance documentation and source repositories. Documents upstream behavior as-is; this is NOT core-x doctrine. Treat the version numbers and API signatures here as fetched-date ground truth.
>
> Primary sources:
> - https://raw.githubusercontent.com/lancedb/lance/v8.0.0/python/python/lance/dataset.py — verbatim `create_scalar_index`, `describe_indices`, `list_indices`, `drop_index`, `prewarm_index`, `stats.index_stats`, and `DatasetOptimizer.optimize_indices` signatures + docstrings from the **released v8.0.0 tag** (the version currently on PyPI as `pylance==8.0.0`).
> - https://raw.githubusercontent.com/lancedb/lance/main/python/python/lance/dataset.py — cross-checked `main` HEAD to confirm the released enum matches the development branch.
> - https://docs.lancedb.com/indexing/scalar-index — LanceDB (higher-level DB) scalar-index guide; index-type descriptions and Table-level `create_scalar_index` behavior.
> - https://pypi.org/project/pylance/ — current released `pylance` version (8.0.0).

Scope: How to create, manage, and reason about **scalar** (non-vector) indices on a Lance dataset — the full `create_scalar_index` API surface, every index type in the current released enum, how these indices accelerate filter pushdown and vector pre-filtering, and the maintenance lifecycle (indices are not auto-updated; new data requires `optimize`).

---

## 0. Versions (fetched-date ground truth, 2026-07-08)

| Component | Current version | Notes |
|---|---|---|
| `pylance` (PyPI, the `lance` Python module) | **8.0.0** | Released **2026-07-01** (verified against the PyPI JSON API; 2026-05-27 is the release date of the prior `7.0.0`). The Lance repo git tag `v8.0.0` corresponds 1:1 to this wheel; signatures below are quoted from that tag. |
| Lance (Rust core / repo) | tag `v8.0.0` | Repo tags track the Python package versions (`v1.0.0` … `v8.0.0`). |
| DuckDB (for the DuckDB→Arrow→Lance pipeline) | **1.5.4** (Variegata) / **1.4.5** LTS (Andium) | DuckDB 1.5.4 released 2026-06-17; the 1.4.x line is LTS through Sept 2026. DuckDB v2.0 is planned for Sept 2026. Not a Lance API, listed only because these docs target DuckDB→Lance pipelines. |

`pip install -U pylance` gives you the module imported as `import lance`. The vector-index counterpart (`IVF_PQ`, `HNSW`, etc.) is documented in [`06_vector_search.md`](06_vector_search.md); this file is scalar-only.

> Terminology note: there are **two** `create_scalar_index` surfaces in the ecosystem.
> 1. `lance.LanceDataset.create_scalar_index(...)` — the low-level pylance dataset method. **This file documents this one** (verbatim from source).
> 2. `lancedb.table.Table.create_scalar_index(...)` — the higher-level LanceDB (the database) Table method, which wraps the same Rust core but adds async/enterprise semantics (`wait_timeout`, `wait_for_index()`). That surface is covered in [`11_lancedb_table_api.md`](11_lancedb_table_api.md).

---

## 1. `create_scalar_index` — full signature

Verbatim from `lance/dataset.py` (v8.0.0 tag), method on `LanceDataset`:

```python
def create_scalar_index(
    self,
    column: str,
    index_type: Union[
        Literal["BTREE"],
        Literal["BITMAP"],
        Literal["LABEL_LIST"],
        Literal["INVERTED"],
        Literal["FTS"],
        Literal["NGRAM"],
        Literal["ZONEMAP"],
        Literal["BLOOMFILTER"],
        Literal["RTREE"],
        IndexConfig,
    ],
    name: Optional[str] = None,
    *,
    replace: bool = True,
    train: bool = True,
    fragment_ids: Optional[List[int]] = None,
    index_uuid: Optional[str] = None,
    progress_callback: Optional[Callable[[IndexProgress], None]] = None,
    **kwargs,
):
```

> **Version note (corrected):** the released `v8.0.0` tag's `create_scalar_index` signature ends with `progress_callback` then `**kwargs` — it does **not** have an explicit `format_version` parameter. An explicit `format_version: Optional[Union[int, str]] = None` keyword-only parameter (which just sets `kwargs["format_version"]`) exists only on the **development `main` branch**, not in any released wheel as of 2026-07-08. On `v8.0.0`, an FTS on-disk format version is passed through `**kwargs` as `format_version=...` if the underlying core supports it — it is not a first-class named parameter.

The method has **no return value** (returns `None`). It commits a new dataset version containing the index (unless `fragment_ids` is used for uncommitted distributed builds — see below).

### 1.1 Core parameters

| Parameter | Type | Default | Accepted values / meaning |
|---|---|---|---|
| `column` | `str` | — (required) | The single column to index. Must be a boolean, integer, float, or string column (type constraints are enforced per index-type — see §3). **Multi-column scalar indices are not supported**: passing a list raises `NotImplementedError("Scalar indices currently only support a single column")`. |
| `index_type` | `str` or `IndexConfig` | — (required) | One of `"BTREE"`, `"BITMAP"`, `"LABEL_LIST"`, `"INVERTED"`, `"FTS"`, `"NGRAM"`, `"ZONEMAP"`, `"BLOOMFILTER"`, `"RTREE"` (case-insensitive; internally upper-cased). Or an `IndexConfig` object for advanced configs. Anything else raises `NotImplementedError`. |
| `name` | `Optional[str]` | `None` | Index name. If omitted, a name is **generated from the column name**. The generated (or explicit) name is what you pass to `drop_index` / `stats.index_stats` — it is *not* necessarily the field name. |
| `replace` | `bool` (kw-only) | `True` | If `True`, replace an existing index of the same name. **Note the default is `True`** for `create_scalar_index` — different from `create_index` (vector), whose `replace` defaults to `False`. |
| `train` | `bool` (kw-only) | `True` | If `True`, build the index over existing data. If `False`, create an **empty** index that can be populated later (used in distributed / staged build flows). |
| `fragment_ids` | `Optional[List[int]]` (kw-only) | `None` | Build the index only over the listed fragments (distributed / fragment-level indexing). When set, the call returns segment metadata **without committing**; segments are later merged/committed via `commit_existing_index_segments(...)`. For `BTREE`/`BITMAP`/`ZONEMAP` this raises — those "segment-native" types must go through `create_index_uncommitted(..., fragment_ids=...)` instead (see §1.3). |
| `index_uuid` | `Optional[str]` (kw-only) | `None` | UUID for the segment written by this call (distributed builds). Auto-generated if omitted. |
| `progress_callback` | `Optional[Callable[[IndexProgress], None]]` (kw-only) | `None` | Callback receiving `lance.progress.IndexProgress` events during the build. |
| `format_version` | (see note) | — | **Not an explicit parameter in the released `v8.0.0` signature** — it exists as a first-class keyword only on `main`. On `v8.0.0`, pass it through `**kwargs` as `format_version=...` for `INVERTED`/`FTS` to select the on-disk FTS format version (the `main` implementation accepts `1`, `2`, `"v1"`, or `"v2"`; unset → current default). Do not rely on it being a named/documented kwarg in the released wheel. |
| `**kwargs` | — | — | Extra index-specific options. For `INVERTED`/`FTS` this is where the tokenizer knobs go (see §1.2). |

### 1.2 `INVERTED`/`FTS`-only keyword options (passed via `**kwargs`)

These are documented in the `create_scalar_index` docstring but flow through `**kwargs`. They apply **only** to the full-text (`INVERTED`/`FTS`) index type:

| Keyword | Type | Default | Meaning |
|---|---|---|---|
| `with_position` | `bool` | `False` | Store token positions so **phrase queries** work. Significantly increases index size. Does not affect non-phrase query performance. |
| `memory_limit` | `int` | `None` (→ 2 GiB per worker) | Total build-time memory budget in **MiB**, split evenly across workers. Not persisted with the index. Larger budget → fewer shards → cheaper search (trade build resources for search cost). |
| `num_workers` | `int` | `None` (→ `num_compute_cpus`) | Workers for this build, clamped to `[1, num_compute_cpus]`. Overridden by `LANCE_FTS_NUM_SHARDS` if that env var is set. Not persisted. |
| `base_tokenizer` | `str` | `"simple"` | `"simple"` (split on whitespace + punctuation), `"whitespace"` (split on whitespace), `"raw"` (no tokenization), `"icu"` (ICU dictionary-based Unicode word segmentation), `"icu/split"` (ICU segmentation with simple-style delimiter splitting). |
| `language` | `str` | `"English"` | Language for stemming and stop-word removal (only used when `stem` or `remove_stop_words` is true). |
| `max_token_length` | `Optional[int]` | `40` | Tokens longer than this are dropped. |
| `lower_case` | `bool` | `True` | Lower-case all text. |
| `stem` | `bool` | `True` | Apply stemming. |
| `remove_stop_words` | `bool` | `True` | Remove stop words. |
| `custom_stop_words` | `Optional[List[str]]` | `None` | Custom stop-word list (only used when `remove_stop_words=True`). If `None`, the built-in list for `language` is used. |
| `ascii_folding` | `bool` | `True` | Fold non-ASCII to ASCII where possible (e.g. `"é" → "e"`). |

---

## 2. What is (and is NOT) in the released enum

**All nine of these are real, released index-type strings** in `pylance==8.0.0` — verified against both the `v8.0.0` tag and `main`:

`BTREE`, `BITMAP`, `LABEL_LIST`, `INVERTED`, `FTS`, `NGRAM`, `ZONEMAP`, `BLOOMFILTER`, `RTREE`.

The runtime validation (in `_prepare_scalar_index_request`) accepts exactly this set (upper-cased) and raises `NotImplementedError` otherwise. `FTS` is an **alias** for `INVERTED` (same underlying full-text index).

> **Correction to a common assumption:** older Lance docs and some downstream references list only five scalar index types (BTREE / BITMAP / LABEL_LIST / INVERTED-FTS / NGRAM). As of the current release, **`ZONEMAP` and `BLOOMFILTER` are genuinely present in the released enum** (not invented, not main-only), alongside a spatial **`RTREE`** type. Both `ZONEMAP` and `BLOOMFILTER` are described in-source as **inexact** indices (they narrow candidates but do not by themselves prove a match). Treat them as newer/less-battle-tested than BTREE/BITMAP. See §3 for exactly what each does.

---

## 3. The scalar index types — what each does, when it applies, and its column-type constraint

Descriptions are quoted/paraphrased from the in-source docstring; column-type constraints are the **actual runtime checks** in `_prepare_scalar_index_request`.

### BTREE — high-cardinality equality + range
- **What:** A btree-inspired structure; only the first few layers are cached in memory.
- **Best for:** columns with a **large number of unique values and few rows per value** (high cardinality). Primary keys, resolution keys, timestamps, monotonic IDs.
- **Accelerates:** equality (`=`), comparison (`<`, `>`, `<=`, `>=`), range (`BETWEEN`), and set membership (`IN`).
- **Column types allowed:** integer, floating, boolean, string / large-string, temporal, fixed-size-binary. `duration` is explicitly rejected.

### BITMAP — low-cardinality categoricals
- **What:** stores a bitmap per unique value.
- **Best for:** columns with a **small number of unique values and many rows per value** (low cardinality) — status enums, country codes, category/type flags, booleans.
- **Accelerates:** the same equality/comparison/range/`IN` filters as BTREE, but far more space-efficient when cardinality is low.
- **Column types allowed:** same set as BTREE (integer, float, bool, string/large-string, temporal, fixed-size-binary). `duration` rejected.

### LABEL_LIST — list/array membership
- **What:** indexes **list columns** whose element values have small cardinality (e.g. a `tags` column holding `["tag1","tag2","tag3"]`).
- **Accelerates:** list-membership filters — `array_has_any`, `array_has_all`, `array_has` / `array_contains`.
- **Column types allowed:** the column **must be a list** type (`pa.types.is_list`), else `TypeError`.

### NGRAM — substring / `LIKE` / `contains`
- **What:** builds a bitmap per n-gram in the string (trigrams by default).
- **Accelerates:** substring queries via the `contains` function in filters (the index-backed acceleration path for `LIKE '%...%'`-style substring search).
- **Column types allowed:** must be `string` or `large_string`, else `TypeError`.

### INVERTED (alias FTS) — full text
- **What:** an inverted index over document columns enabling **full-text search**, ranked by **BM25**.
- **Accelerates:** full-text/keyword search (`match`, phrase queries when `with_position=True`), not plain scalar comparisons.
- **Column types allowed:** `string`, `large_string`, a **list of strings**, or a JSON column (`large_binary` carrying the `lance.json` Arrow extension). Else `TypeError`.
- **Tokenizer / language / stemming knobs:** see §1.2.

### ZONEMAP — inexact, sorted-order min/max skipping
- **What:** breaks the column into fixed-size chunks ("zones") and stores per-zone summary stats (`min`, `max`, `null_count`, `nan_count`, `fragment_id`, `local_row_offset`).
- **Best for:** columns that are **at least approximately sorted** — it lets the scanner skip whole zones. Very small on disk.
- **Caveat:** **inexact** and only effective on (approximately) ordered data; useless on randomly-ordered columns.
- **Column types allowed:** same set as BTREE/BITMAP.

### BLOOMFILTER — inexact, equality-only
- **What:** a bloom filter over the column. Small on disk.
- **Best for:** equality (`=`) and not-equals (`!=`) membership pruning when a full BTREE/BITMAP is too large.
- **Caveat:** **inexact**; handles only `=` / `!=`, and may require **more I/O than a btree or bitmap index**. (Quoted from source.)

### RTREE — spatial
- **What:** a spatial (R-tree) index. Present in the released enum and the `_prepare_scalar_index_request` validation set. In-source prose focuses on the other eight types; use is spatial/geometry filtering.

> **Segment-native subset:** BTREE, BITMAP, INVERTED/FTS, and ZONEMAP are treated internally as "segment-native" scalar index types (`_is_segment_native_scalar_index_type`). BTREE, BITMAP, and ZONEMAP additionally require the uncommitted/distributed build path when built per-fragment (`_requires_uncommitted_scalar_index`).

---

## 4. How scalar indices accelerate queries (pushdown & pre-filter)

Scalar indices speed up two distinct query shapes:

1. **Filtered scans** — `dataset.scanner(filter="my_col != 7").to_table()`. The filter is pushed down to the index instead of scanning every row. See [`09_scanning_filtering.md`](09_scanning_filtering.md) for the full scan/filter API.
2. **Vector search pre-filtering** — a vector `nearest=...` search with `filter=...` and `prefilter=True` uses the scalar index to restrict the candidate set **before** the ANN search runs. See [`06_vector_search.md`](06_vector_search.md).

Rules governing when a scalar index is actually used (from the docstring):

- Only **basic filters** are accelerated: equality, comparison, range (`my_col BETWEEN 0 AND 100`), and set membership (`my_col IN (0, 1, 2)`).
- Multiple indexed columns can be combined with `AND` / `OR` (`my_col < 0 AND other_col > 100`).
- A filter mixing indexed and **non-indexed** columns *may* still use the index depending on structure — but e.g. `my_col = 0 OR not_indexed = 1` **cannot** use the index on `my_col`, because the `OR` with an un-indexed predicate forces a full evaluation.
- To confirm the index is used, call `explain_plan` on the scan: index-using queries show a **`ScalarIndexQuery`** relation or a **`MaterializeIndex`** operator in the plan.
- `dataset.scanner(..., use_scalar_index=<bool>)` can force-disable scalar-index usage for a given scan (defaults to enabled). Full-text queries use the `INVERTED` index automatically.

**On-disk layout & versioning:** indices are stored under the dataset's `_indices/` directory and are **versioned along with the dataset manifest**. Creating or optimizing an index commits a new dataset version (see [`04_versioning_time_travel.md`](04_versioning_time_travel.md) for the manifest/version model, and [`01_file_format.md`](01_file_format.md) for the on-disk `_indices/` layout).

---

## 5. Managing indices

### 5.1 Listing / describing indices

`describe_indices()` is the **current** API. `list_indices()` is **deprecated** (emits `DeprecationWarning`, "may be removed in a future version. Use describe_indices() instead.").

```python
def describe_indices(self) -> List[IndexDescription]:
    """Returns index information for all indices in the dataset."""
```

```python
def list_indices(self) -> List[IndexInformation]:   # DEPRECATED
    """Returns index information for all indices in the dataset.
    This method is deprecated.  Use describe_indices() instead ..."""
```

`list_indices()` returns, per index **segment**, a dict with keys: `name`, `type`, `uuid`, `fields`, `version`, `fragment_ids`, `base_id`. `describe_indices()` returns richer per-index `IndexDescription` objects (with `.name`, `.index_type`, `.field_names`, and `.segments`, each segment carrying `.uuid`, `.dataset_version_at_last_update`, `.fragment_ids`, `.base_id`).

Convenience property:
```python
@property
def has_index(self):
    return len(self.describe_indices()) > 0
```

### 5.2 Index statistics

`stats.index_stats(index_name)` is the current API; the top-level `LanceDataset.index_statistics(index_name)` is **deprecated** (use `LanceDataset.stats.index_stats()`).

```python
# LanceStats.index_stats
def index_stats(self, index_name: str) -> Dict[str, Any]:
    """Statistics about an index.

    Parameters
    ----------
    index_name: str
        The name of the index to get statistics for.
    """
```

Usage: `dataset.stats.index_stats("my_index_name")` → dict of index metrics.

### 5.3 Dropping an index

```python
def drop_index(self, name: str):
    """Drops an index from the dataset

    Note: Indices are dropped by "index name".  This is not the same as the field
    name. If you did not specify a name when you created the index then a name was
    generated for you.  You can use the `describe_indices` method to get the names
    of the indices.
    """
```

**Footgun:** you drop by **index name**, not column/field name. If you let `create_scalar_index` auto-generate the name, call `describe_indices()` to discover it before dropping.

### 5.4 Prewarming an index (optional latency optimization)

```python
# Released v8.0.0 signature:
def prewarm_index(self, name: str, *, with_position: bool = False):
```

Loads the index into the in-memory cache to avoid cold-start I/O. `with_position` (INVERTED only) also preloads phrase-query positions. If the index does not fit the cache, this wastes I/O.

> **Version note (corrected):** an additional `index_segments: Optional[Iterable[Union[str, uuid.UUID]]] = None` parameter exists only on `main`, not in the released `v8.0.0` tag — do not pass it against `pylance==8.0.0`. The `session().index_cache_size_bytes()` before/after cache-inspection guidance also comes from the `main` docstring; the v8.0.0 docstring does not mention it, though the `Session` method itself is usable.

---

## 6. New data is NOT auto-indexed — you must `optimize`

> **Critical operational fact.** After you append/insert new rows, existing indices do **not** cover them automatically. Queries then do an indexed search over the old data **plus a full unindexed scan of the new data** — latency degrades as unindexed data accumulates.

Fix it by optimizing indices (assigns new data into the existing index without a full retrain):

```python
# DatasetOptimizer.optimize_indices
def optimize_indices(self, **kwargs):
    """Optimizes index performance.

    As new data arrives it is not added to existing indexes automatically.
    ...
    This function does not retrain the index, it only assigns
    the new data to existing partitions. ...
    """
```

| kwarg | Type | Default | Meaning |
|---|---|---|---|
| `num_indices_to_merge` | `int` | `None` | Number of delta indices to merge. `0` → create a new delta index instead of merging. |
| `index_names` | `List[str]` | `None` | Which indices to optimize. `None` → all indices. |
| `retrain` | `bool` (**deprecated**) | `False` | Retrain the whole index (ignores `num_indices_to_merge`, merges everything into one). Useful when the data distribution shifted significantly. |

Called as: `dataset.optimize.optimize_indices()`. Full maintenance lifecycle — compaction, fragment management, and index optimization strategy — is in [`08_compaction_maintenance.md`](08_compaction_maintenance.md).

---

## 7. Runnable examples

### 7.1 BTREE on a resolution key

```python
import lance

ds = lance.dataset(
    "s3://data-sink/active/entities_lance",
    storage_options={
        "aws_access_key_id": "...",
        "aws_secret_access_key": "...",
        "aws_endpoint": "https://<accountid>.r2.cloudflarestorage.com",
        "region": "auto",
    },
)

# High-cardinality equality/range key → BTREE.
ds.create_scalar_index("entity_id", "BTREE")

# Range/equality filters now push down to the index.
hits = ds.scanner(filter="entity_id = 'ABC-123'").to_table()

# Confirm the index is used.
print(ds.scanner(filter="entity_id = 'ABC-123'").explain_plan())
# -> plan contains a ScalarIndexQuery / MaterializeIndex node
```

### 7.2 BITMAP on a low-cardinality categorical

```python
import lance

ds = lance.dataset("s3://data-sink/active/entities_lance", storage_options={...})

# Few distinct values, many rows each → BITMAP.
ds.create_scalar_index("status", "BITMAP")          # e.g. active/inactive/pending
ds.create_scalar_index("country_code", "BITMAP")    # ISO codes

# IN / equality over the categorical now uses the bitmap index.
active = ds.scanner(filter="status = 'active' AND country_code IN ('US','CA')").to_table()
```

### 7.3 Full-text (INVERTED/FTS) with a custom tokenizer

```python
import lance

ds = lance.dataset("s3://data-sink/active/docs_lance", storage_options={...})

ds.create_scalar_index(
    "body",
    "INVERTED",                 # or "FTS" — same index
    with_position=True,         # enable phrase queries
    base_tokenizer="simple",
    language="English",
    stem=True,
    remove_stop_words=True,
    ascii_folding=True,
)
```

### 7.4 NGRAM for substring / `contains`

```python
ds.create_scalar_index("name", "NGRAM")
partial = ds.scanner(filter="contains(name, 'acme')").to_table()
```

### 7.5 Inspect, then keep indices fresh after an append

```python
# After write_dataset(..., mode="append") lands new fragments:
ds = lance.dataset("s3://data-sink/active/entities_lance", storage_options={...})

# New rows are unindexed until this runs:
ds.optimize.optimize_indices()

# Inspect what exists and per-index stats:
for idx in ds.describe_indices():
    print(idx.name, idx.index_type, idx.field_names)
    print(ds.stats.index_stats(idx.name))

# Drop by index NAME (not column name):
ds.drop_index("entity_id_idx")   # name from describe_indices()
```

---

## 8. Footguns & deprecations

- **`replace=True` is the default** for `create_scalar_index` (unlike `create_index`, where it defaults to `False`). Re-running a create will silently overwrite an existing same-named index.
- **Single column only.** No composite scalar indices; pass one column. Multiple columns → `NotImplementedError`. To accelerate multi-column filters, index each column separately and let the planner AND/OR them.
- **Drop-by-name, not by column.** `drop_index(name)` needs the index name; auto-generated names come from `describe_indices()`.
- **Deprecated APIs:** `list_indices()` → use `describe_indices()`; `LanceDataset.index_statistics()` → use `LanceDataset.stats.index_stats()`.
- **Inexact types:** `ZONEMAP` (needs approximately-sorted data) and `BLOOMFILTER` (`=`/`!=` only, possibly more I/O) narrow candidates but do not by themselves confirm matches — prefer BTREE/BITMAP unless you specifically need their size profile.
- **New data is invisible to indices until `optimize_indices()`.** Any append/insert/merge silently degrades index-backed query latency until you optimize.
- **`format_version` is FTS-only and not a released named parameter.** It is a first-class keyword only on `main`; in `v8.0.0` supply it via `**kwargs` (`format_version=...`). Passing it for a non-INVERTED index has no effect.
- **`memory_limit` is in MiB** (not bytes, not GiB) and is per-build, not persisted.

---

## 9. Cross-links

- [`00_overview.md`](00_overview.md) — Lance/LanceDB overview, packaging, versions.
- [`01_file_format.md`](01_file_format.md) — on-disk dataset layout, including the `_indices/` directory.
- [`02_python_dataset_api.md`](02_python_dataset_api.md) — `lance.dataset`, `LanceDataset` object.
- [`03_writes_appends_upserts.md`](03_writes_appends_upserts.md) — append/merge_insert/update (what makes indices stale).
- [`04_versioning_time_travel.md`](04_versioning_time_travel.md) — index versions commit with the manifest.
- [`06_vector_search.md`](06_vector_search.md) — vector indices and how scalar pre-filters combine with ANN.
- [`07_storage_object_stores.md`](07_storage_object_stores.md) — R2/S3 `storage_options` used in the examples.
- [`08_compaction_maintenance.md`](08_compaction_maintenance.md) — `optimize_indices`, compaction, fragment maintenance.
- [`09_scanning_filtering.md`](09_scanning_filtering.md) — `scanner()`, filter pushdown, `explain_plan`, `use_scalar_index`.
- [`10_duckdb_arrow_interop.md`](10_duckdb_arrow_interop.md) — DuckDB→Arrow→Lance pipeline.
- [`11_lancedb_table_api.md`](11_lancedb_table_api.md) — the LanceDB `Table.create_scalar_index` surface (async, `wait_timeout`).

---

> **Relevance to core-x:** Every load-bearing resolution key in a Lance dataset on R2 gets a hard **`BTREE`** scalar index (`ds.create_scalar_index(<key>, "BTREE")`) — this is the pushdown/pre-filter path that keeps keyed lookups and joins from degrading into full scans at hundreds-of-millions-of-rows scale. Low-cardinality categoricals (status flags, region/country codes, type discriminators) take **`BITMAP`** instead of BTREE for a far smaller footprint. Because core-x fragments are **append-only and immutable**, every ingest lands unindexed rows: the index is *not* live until `ds.optimize.optimize_indices()` runs, so index-optimization must be a mandatory post-append step in the pipeline (see [`08_compaction_maintenance.md`](08_compaction_maintenance.md)), not an afterthought. Drop-and-rebuild is by **index name**, so pin explicit `name=` values on creation to make the maintenance job deterministic rather than relying on generated names.

---

## 10. Unverified / needs confirmation

- **`RTREE` semantics.** `RTREE` is confirmed present in the released v8.0.0 enum and validation set, but the in-source `create_scalar_index` docstring does not describe its column-type constraint or supported filter functions in detail. Confirm spatial-column requirements and supported predicates against the Rust core or a dedicated spatial-index doc before relying on it.
- **`IndexConfig` parameter schema.** `index_type` accepts an `IndexConfig` object (its `.index_type` and `.parameters` are serialized to JSON and passed as `config`), but the full set of valid `parameters` keys per index type is not enumerated in `dataset.py`. Confirm against `lance.indices` if you need non-default index configuration.
- **`ZONEMAP` / `BLOOMFILTER` maturity.** Both are in the released enum and validated, but are described as inexact and appear newer than the classic four. No release-version gate for their introduction was confirmed from the fetched sources; treat them as current-release features and validate behavior on representative data before production use.
