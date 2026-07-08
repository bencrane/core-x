# pylance Python SDK — lance.dataset, lance.write_dataset, LanceDataset

> Canonical upstream reference — fetched 2026-07-08 from official Lance documentation and source repositories. Documents upstream behavior as-is; this is NOT core-x doctrine. Treat the version numbers and API signatures here as fetched-date ground truth.
>
> Primary sources:
> - https://raw.githubusercontent.com/lancedb/lance/main/python/python/lance/dataset.py — verbatim source for `write_dataset`, `LanceDataset`, `LanceScanner`, and every method/signature quoted below (fetched 2026-07-08, `main` branch, 7955 lines).
> - https://raw.githubusercontent.com/lancedb/lance/main/python/python/lance/__init__.py — top-level `lance.dataset` signature and the package `__all__` export list.
> - https://raw.githubusercontent.com/lancedb/lance/main/python/python/lance/types.py — the `ReaderLike` union that defines accepted `data` inputs to `write_dataset`.
> - https://pypi.org/project/pylance/ — current released version (`pylance` 8.0.0, released 2026-07-01; requires Python >=3.9, supports 3.9–3.14).

Scope: the pylance Python SDK read/open surface — opening a dataset with `lance.dataset(...)`, writing one with `lance.write_dataset(...)`, and the read-side methods on `LanceDataset` (`scanner`, `to_table`, `to_batches`, `head`, `take`, `count_rows`, `get_fragments`, `versions`, and key properties). Write mutation semantics live in `03_writes_appends_upserts.md`; scan internals in `09_scanning_filtering.md`.

---

## 0. Versions & packaging (as of 2026-07-08)

| Package | Current version | Notes |
|---|---|---|
| `pylance` (PyPI) | **8.0.0** (released 2026-07-01) | The Python wrapper for the Lance columnar format. `import lance`. Requires Python >=3.9 (supports 3.9–3.14). Wheels bundle the compiled Rust core; no separate install needed. |
| `lancedb` (PyPI) | 0.34.0 (released 2026-07-02) | The *database* layer (tables, vector search convenience API). Different package; see `11_lancedb_table_api.md`. Do not confuse with pylance. |
| `duckdb` (PyPI) | 1.5.4 (stable, 2026-06); 1.4.x LTS line | Query engine used in the core-x pipeline; see `10_duckdb_arrow_interop.md`. |

- Install: `pip install pylance`. The import name is `lance`, **not** `pylance`.
- The PyPI project name `pylance` is unrelated to Microsoft's "Pylance" VS Code Python language server — same string, different product.

> Version note: the signatures below are quoted from the `main` branch source on 2026-07-08. A pinned release (e.g. `pylance==8.0.0`) may lag `main` on the newest parameters. Parameters explicitly flagged *Experimental* or *Unstable* below can change without a major-version bump. Anything sourced from `main` that may not yet be in a tagged release is flagged under **Unverified / needs confirmation**.

---

## 1. `lance.dataset(...)` — open an existing dataset

`lance.dataset` is the canonical entry point for opening a dataset for reading. It returns a `LanceDataset`. Opening is cheap: it reads the manifest, not the data.

Verbatim signature (source: `lance/__init__.py`):

```python
def dataset(
    uri: Optional[Union[str, Path]] = None,
    version: Optional[int | str] = None,
    asof: Optional[ts_types] = None,
    block_size: Optional[int] = None,
    commit_lock: Optional[CommitLock] = None,
    index_cache_size: Optional[int] = None,
    storage_options: Optional[Dict[str, str]] = None,
    default_scan_options: Optional[Dict[str, str]] = None,
    metadata_cache_size_bytes: Optional[int] = None,
    index_cache_size_bytes: Optional[int] = None,
    read_params: Optional[Dict[str, any]] = None,
    session: Optional[Session] = None,
    namespace_client: Optional[LanceNamespace] = None,
    table_id: Optional[List[str]] = None,
    base_store_params: Optional[Dict[str, Dict[str, str]]] = None,
) -> LanceDataset
```

`lance.dataset(...)` is the sole top-level open function — it is exported in `lance.__all__` and is the canonical, fully-parameterized entry point. **There is no `lance.open(...)`** on `main` as of 2026-07-08 (not defined in `__init__.py` or `dataset.py`, not in `__all__`); do not use it.

### Parameter table

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `uri` | `str \| Path \| None` | `None` | Dataset location (a directory). For object stores, a URI like `s3://bucket/path/name.lance`. Either `uri` **or** (`namespace_client` + `table_id`) must be provided. |
| `version` | `int \| str \| None` | `None` | Open a specific historical version (time travel). Integer version number **or** a tag name (str). `None` = latest. See `04_versioning_time_travel.md`. |
| `asof` | timestamp-like (`ts_types`), `datetime` or `str` | `None` | Open the latest version created on or before this timestamp. If `version` is also specified, `asof` is **ignored**. |
| `block_size` | `int \| None` | `None` | Block size (bytes) hint for the reader. |
| `commit_lock` | `CommitLock \| None` | `None` | Custom external commit lock — only needed when the object store lacks atomic commit (see `07_storage_object_stores.md`). |
| `index_cache_size` | `int \| None` | `None` | **Deprecated** count-based index cache. Prefer `index_cache_size_bytes`. |
| `storage_options` | `Dict[str, str] \| None` | `None` | Backend connection options — credentials, endpoint, region, etc. This is the R2/S3 config surface (see §7 cross-link and `07_storage_object_stores.md`). |
| `default_scan_options` | `Dict[str, str] \| None` | `None` | Default options applied to every scan created from this dataset handle (e.g. a default `with_row_id`). When set, `dataset.schema` reflects the *projected* schema of a default scan rather than the raw schema. |
| `metadata_cache_size_bytes` | `int \| None` | `None` | Byte-budget for the metadata cache. |
| `index_cache_size_bytes` | `int \| None` | `None` | Byte-budget for the index cache (replaces the count-based `index_cache_size`). |
| `read_params` | `Dict[str, any] \| None` | `None` | Low-level reader parameters. |
| `session` | `Session \| None` | `None` | Reuse an existing Lance `Session` (shared caches/handles). |
| `namespace_client` | `LanceNamespace \| None` | `None` | Open via a namespace/catalog client instead of a raw URI. |
| `table_id` | `List[str] \| None` | `None` | Table identifier path within the namespace. Used with `namespace_client`. |
| `base_store_params` | `Dict[str, Dict[str, str]] \| None` | `None` | Per-base storage params for multi-base datasets. |

> Relevance to core-x: `lance.dataset(uri, storage_options={...})` is the read entry point for every `*_lance` dataset addressed by its R2 URI. No catalog client is used — pass the raw `s3://data-sink/active/...` URI and the R2 `storage_options` directly. Pin `version=` for reproducible reads against an immutable snapshot.

---

## 2. `lance.write_dataset(...)` — create / append / overwrite

Verbatim signature (source: `lance/dataset.py`, module-level `write_dataset`):

```python
def write_dataset(
    data_obj: ReaderLike,
    uri: Optional[Union[str, Path, LanceDataset]] = None,
    schema: Optional[pa.Schema] = None,
    mode: str = "create",
    *,
    max_rows_per_file: int = 1024 * 1024,
    max_rows_per_group: int = 1024,
    max_bytes_per_file: int = 90 * 1024 * 1024 * 1024,
    commit_lock: Optional[CommitLock] = None,
    progress: Optional[FragmentWriteProgress] = None,
    storage_options: Optional[Dict[str, str]] = None,
    data_storage_version: Optional[
        Literal["stable", "2.0", "2.1", "2.2", "2.3", "next", "legacy", "0.1"]
    ] = None,
    use_legacy_format: Optional[bool] = None,
    enable_v2_manifest_paths: bool = True,
    enable_stable_row_ids: bool = False,
    auto_cleanup_options: Optional[AutoCleanupConfig] = None,
    commit_message: Optional[str] = None,
    transaction_properties: Optional[Dict[str, str]] = None,
    initial_bases: Optional[List[DatasetBasePath]] = None,
    target_bases: Optional[List[str]] = None,
    target_all_bases: Optional[bool] = None,
    base_store_params: Optional[Dict[str, Dict[str, str]]] = None,
    external_blob_mode: Literal["reference", "ingest"] = "reference",
    allow_external_blob_outside_bases: bool = False,
    blob_pack_file_size_threshold: Optional[int] = None,
    namespace_client: Optional[LanceNamespace] = None,
    table_id: Optional[List[str]] = None,
) -> LanceDataset
```

**Return type:** `LanceDataset` — the newly written/updated dataset handle.

### Accepted `data_obj` types (`ReaderLike`)

Source: `lance/types.py`. The `ReaderLike` union is:

```python
ReaderLike = Union[
    pd.Timestamp,          # (part of the union; not a real bulk-data input)
    pa.Table,
    pa.dataset.Dataset,
    pa.dataset.Scanner,
    pa.RecordBatch,
    Iterable[RecordBatch],
    pa.RecordBatchReader,
]
```

Concretely, `write_dataset` accepts:
- **`pyarrow.Table`**
- **`pyarrow.RecordBatch`**
- **`pyarrow.RecordBatchReader`** — the out-of-core streaming path (writer never materializes the whole input).
- **`Iterable[pyarrow.RecordBatch]`** — a plain Python iterator/generator of batches. If batches don't match `schema`, Lance attempts a cast per batch and raises `ValueError` on failure. **Footgun:** PyArrow defaults floats to `float64`, but Lance vector columns expect `float32` — supply an explicit `schema` when writing vectors so the cast lands where you want it.
- **`pyarrow.dataset.Dataset`** and **`pyarrow.dataset.Scanner`**
- **`pandas.DataFrame`** — converted via `pa.Table.from_pandas(df, schema=schema)`. Pass `schema` to override the default pandas→arrow type inference.
- **Hugging Face `datasets.Dataset`** — per the docstring, accepted and coerced.

### Parameter table

| Parameter | Type | Default | Accepted values / meaning |
|---|---|---|---|
| `data_obj` | `ReaderLike` | *(required)* | The data to write (see accepted types above). |
| `uri` | `str \| Path \| LanceDataset \| None` | `None` | Destination directory/URI. If a `LanceDataset` is passed, its session is reused. Either `uri` **or** (`namespace_client` + `table_id`) must be provided. |
| `schema` | `pa.Schema \| None` | `None` | Explicit Arrow schema. Primarily used to override pandas→arrow inference and to pin `float32` vector types. |
| `mode` | `str` | `"create"` | `"create"` — new dataset; **raises if `uri` already exists**. `"overwrite"` — write a new snapshot version (replaces logical contents; prior versions remain for time travel). `"append"` — new version = existing latest ∥ input; **creates the dataset if `uri` doesn't exist**. |
| `max_rows_per_file` | `int` | `1024*1024` (1,048,576) | Max rows before rolling to a new data file (fragment file). |
| `max_rows_per_group` | `int` | `1024` | Max rows per group within a file. |
| `max_bytes_per_file` | `int` | `90*1024^3` (90 GB) | **Soft** byte cap per file, checked after each group — large groups can overshoot. Default is 90 GB because object stores impose a hard 100 GB/file ceiling. |
| `commit_lock` | `CommitLock \| None` | `None` | Custom commit lock for stores lacking atomic commit. |
| `progress` | `FragmentWriteProgress \| None` | `None` | *Experimental.* Per-fragment write progress hooks. |
| `storage_options` | `Dict[str, str] \| None` | `None` | Backend connection options (credentials/endpoint/region). Supports per-base scoping via keys of the form `base_<id>.<key>` (e.g. `{"account_key": "shared", "base_1.account_key": "abc"}`). See `07_storage_object_stores.md`. |
| `data_storage_version` | `Literal["stable","2.0","2.1","2.2","2.3","next","legacy","0.1"] \| None` | `None` | On-disk file-format version. `None` = latest **stable**. Newer versions are more efficient but require a newer Lance to read. `"legacy"`/`"0.1"` = the old v1 format. See `01_file_format.md`. |
| `use_legacy_format` | `bool \| None` | `None` | **Deprecated.** Old boolean toggle for storage version; use `data_storage_version` instead. |
| `enable_v2_manifest_paths` | `bool` | `True` | For a **new** dataset, use V2 manifest paths (faster opening of many-version datasets on object stores). No effect on existing datasets — migrate an existing one with `LanceDataset.migrate_manifest_paths_v2()`. |
| `enable_stable_row_ids` | `bool` | `False` | *Experimental.* Use stable row IDs — stable across compaction (but not across updates), so compaction needn't rewrite secondary indices to remap row ids. |
| `auto_cleanup_options` | `AutoCleanupConfig \| None` | `None` | For a **new** dataset only, configure automatic old-version cleanup (`interval` + `older_than`). No effect on existing datasets — set `lance.auto_cleanup.*` config instead. See `04_versioning_time_travel.md`. |
| `commit_message` | `str \| None` | `None` | Message stored with the commit; retrievable via `read_transaction()`. Overrides any `lance.commit.message` key in `transaction_properties`. |
| `transaction_properties` | `Dict[str, str] \| None` | `None` | Custom key/value properties stored on the transaction. |
| `initial_bases` | `List[DatasetBasePath] \| None` | `None` | New base paths to register in the manifest. **CREATE mode only.** |
| `target_bases` | `List[str] \| None` | `None` | Base references (name or URI) where data should be written. Valid in all modes; must match `initial_bases` (create) or the existing manifest (append/overwrite). |
| `target_all_bases` | `bool \| None` | `None` | Write to all registered bases. |
| `base_store_params` | `Dict[str, Dict[str, str]] \| None` | `None` | Per-base storage params. |
| `external_blob_mode` | `Literal["reference","ingest"]` | `"reference"` | How external blob data is handled: reference in place vs. ingest into the dataset. |
| `allow_external_blob_outside_bases` | `bool` | `False` | Permit external blobs outside registered base paths. |
| `blob_pack_file_size_threshold` | `int \| None` | `None` | Threshold controlling blob packing file size. |
| `namespace_client` | `LanceNamespace \| None` | `None` | Write via a namespace/catalog client instead of a raw URI. |
| `table_id` | `List[str] \| None` | `None` | Table identifier path within the namespace. |

> Deprecations to know: `use_legacy_format` → use `data_storage_version`. Count-based `index_cache_size` (on `dataset()`) → byte-based `index_cache_size_bytes`.

> Footguns:
> - `mode="create"` **raises** if the URI already exists. To (re)initialize use `"overwrite"`; to add data use `"append"`.
> - `max_bytes_per_file` is a *soft* limit checked after each group — a very large `max_rows_per_group` can push files well past the target. Keep groups modest for predictable file sizes.
> - Vector columns: without an explicit `float32` `schema`, pandas/pyarrow default to `float64` and the writer will store double-width vectors (or fail an index later). Pin the schema.

> Relevance to core-x: the pipeline streams DuckDB output as a `pyarrow.RecordBatchReader` straight into `lance.write_dataset(reader, uri, mode="append", storage_options=<R2>)`. `mode="append"` produces new immutable fragments concatenated onto the latest version — nothing is rewritten in place, which is the intended append-only fragment model. Set `max_rows_per_file` to control fragment granularity at hundreds-of-millions-of-rows scale; default 90 GB `max_bytes_per_file` keeps every file under the object-store 100 GB hard cap.

### Writing at scale (streaming, out-of-core)

```python
import lance
import pyarrow as pa

# `reader` is a pyarrow.RecordBatchReader — e.g. produced by DuckDB's
# .fetch_record_batch(...) or arrow() on a streaming query. The writer
# pulls batches lazily; the full result set is never materialized.
ds = lance.write_dataset(
    reader,
    "s3://data-sink/active/my_dataset.lance",
    mode="append",
    max_rows_per_file=8 * 1024 * 1024,   # ~8M rows/fragment
    data_storage_version="stable",
    storage_options={
        "aws_access_key_id": "...",
        "aws_secret_access_key": "...",
        "aws_endpoint": "https://<accountid>.r2.cloudflarestorage.com",
        "aws_region": "auto",
    },
)
print(ds.count_rows())
```

---

## 3. `LanceDataset` — read surface

`LanceDataset` subclasses `pyarrow.dataset.Dataset`, so it drops into Arrow-native code paths (including as a scannable source in DuckDB). The read-side methods and properties below are quoted verbatim from `lance/dataset.py`.

### 3.1 Key properties

| Property | Type | Meaning |
|---|---|---|
| `schema` | `pa.Schema` | The PyArrow schema. If `default_scan_options` was set at open, returns the *projected* schema of a default scan; otherwise the raw dataset schema. |
| `lance_schema` | `LanceSchema` | The native Lance schema. |
| `uri` | `str` | The dataset location. |
| `version` | `int` | The currently checked-out version. |
| `latest_version` | `int` | The latest committed version (may differ from `version` under time travel). |
| `data_storage_version` | `str` | On-disk data-format version in use. |
| `has_stable_row_ids` | `bool` | Whether stable row IDs are enabled (manifest feature flag). |
| `max_field_id` | `int` | Max field id in the manifest. |
| `tags` | `Tags` | Git-like tag management (see `04_versioning_time_travel.md`). Tagged versions are **exempt** from `cleanup_old_versions()`. |
| `metadata` | `Dict[str, str]` | Table-level metadata key/value pairs. |
| `schema_metadata` | `Dict[str, str]` | Schema-level metadata key/value pairs. |
| `initial_storage_options` | `Dict[str, str] \| None` | The `storage_options` the dataset was opened with (no provider refresh); `None` if none given. |

### 3.2 `versions()`

```python
def versions(self):
    """Return all versions in this dataset."""
```

Returns a list of dicts, each with a `version` number and a `timestamp` (converted to a Python `datetime`, microsecond precision). Detailed version/time-travel semantics: `04_versioning_time_travel.md`.

### 3.3 `scanner(...)` — the lazy scan builder

`scanner` builds a `LanceScanner` (a lazy plan). Nothing executes until you call `.to_table()`, `.to_batches()`, `.count_rows()`, etc. Verbatim signature:

```python
def scanner(
    self,
    columns: Optional[Union[List[str], Dict[str, str]]] = None,
    filter: Optional[Union[str, pa.compute.Expression, FullTextQuery, VectorSearchQuery, dict]] = None,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
    nearest: Optional[dict] = None,
    batch_size: Optional[int] = None,
    batch_size_bytes: Optional[int] = None,
    batch_readahead: Optional[int] = None,
    fragment_readahead: Optional[int] = None,
    scan_in_order: Optional[bool] = None,
    fragments: Optional[Iterable[LanceFragment]] = None,
    index_segments: Optional[Iterable[Union[str, uuid.UUID]]] = None,
    full_text_query: Optional[Union[str, dict, FullTextQuery]] = None,
    *,
    prefilter: Optional[bool] = None,
    with_row_id: Optional[bool] = None,
    with_row_address: Optional[bool] = None,
    use_stats: Optional[bool] = None,
    fast_search: Optional[bool] = None,
    io_buffer_size: Optional[int] = None,
    late_materialization: Optional[bool | List[str]] = None,
    blob_handling: Optional[Literal["all_binary", "blobs_descriptions", "all_descriptions"]] = None,
    use_scalar_index: Optional[bool] = None,
    include_deleted_rows: Optional[bool] = None,
    scan_stats_callback: Optional[Callable[[ScanStatistics], None]] = None,
    strict_batch_size: Optional[bool] = None,
    order_by: Optional[List[Union[ColumnOrdering, str]]] = None,
    disable_scoring_autoprojection: Optional[bool] = None,
) -> LanceScanner
```

Parameter table:

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `columns` | `List[str] \| Dict[str,str] \| None` | `None` | Projection. List of column names, **or** a dict of output-name → SQL expression (computed columns). `None` = all columns. |
| `filter` | `str \| pa.compute.Expression \| FullTextQuery \| VectorSearchQuery \| dict \| None` | `None` | Row filter. A SQL-string predicate (e.g. `"state = 'CA' AND amount > 100"`) or a PyArrow compute expression. Pushed down to the scan. |
| `limit` | `int \| None` | `None` | Max rows to return. |
| `offset` | `int \| None` | `None` | Rows to skip before returning. |
| `nearest` | `dict \| None` | `None` | Vector-search spec (`column`, `q`, `k`, `nprobes`, `refine_factor`, …). See `06_vector_search.md`. |
| `batch_size` | `int \| None` | `None` | Rows per output `RecordBatch`. |
| `batch_size_bytes` | `int \| None` | `None` | Byte-based batching target. |
| `batch_readahead` | `int \| None` | `None` | Number of batches to read ahead (I/O parallelism). |
| `fragment_readahead` | `int \| None` | `None` | Number of fragments to read ahead. |
| `scan_in_order` | `bool \| None` | `None` | Preserve fragment/row order (vs. allowing reordering for throughput). |
| `fragments` | `Iterable[LanceFragment] \| None` | `None` | Restrict the scan to specific fragments (e.g. from `get_fragments()`). |
| `index_segments` | `Iterable[str \| uuid.UUID] \| None` | `None` | Restrict to specific index segments. |
| `full_text_query` | `str \| dict \| FullTextQuery \| None` | `None` | Full-text search query (requires an FTS/INVERTED index — see `05_scalar_indices.md`). |
| `prefilter` | `bool \| None` | `None` | Apply `filter` **before** the vector/index search rather than after. Keyword-only. |
| `with_row_id` | `bool \| None` | `None` | Include the internal `_rowid` column. Keyword-only. |
| `with_row_address` | `bool \| None` | `None` | Include the internal `_rowaddr` column. Keyword-only. |
| `use_stats` | `bool \| None` | `None` | Use per-fragment statistics for pushdown pruning. Keyword-only. |
| `fast_search` | `bool \| None` | `None` | Skip scanning unindexed fragments in vector search (index-only, faster, may miss un-indexed rows). Keyword-only. |
| `io_buffer_size` | `int \| None` | `None` | I/O buffer size. Keyword-only. |
| `late_materialization` | `bool \| List[str] \| None` | `None` | Enable late materialization globally (`bool`) or per named columns (`List[str]`). Keyword-only. |
| `blob_handling` | `Literal["all_binary","blobs_descriptions","all_descriptions"] \| None` | `None` | How blob columns are returned. Keyword-only. |
| `use_scalar_index` | `bool \| None` | `None` | Allow scalar indices to satisfy the filter. Keyword-only. |
| `include_deleted_rows` | `bool \| None` | `None` | Include soft-deleted rows. Keyword-only. |
| `scan_stats_callback` | `Callable[[ScanStatistics], None] \| None` | `None` | Callback invoked with scan statistics. Keyword-only. |
| `strict_batch_size` | `bool \| None` | `None` | Force every output batch to exactly `batch_size` (except possibly the last). Keyword-only. |
| `order_by` | `List[ColumnOrdering \| str] \| None` | `None` | Sort output by columns. Keyword-only. |
| `disable_scoring_autoprojection` | `bool \| None` | `None` | Disable automatic projection of scoring columns. Keyword-only. |

Full scan/filter/pushdown semantics: `09_scanning_filtering.md`.

### 3.4 `to_table(...)` — materialize to an Arrow Table

Verbatim signature:

```python
def to_table(
    self,
    columns: Optional[Union[List[str], Dict[str, str]]] = None,
    filter: Optional[Union[str, pa.compute.Expression]] = None,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
    nearest: Optional[dict] = None,
    batch_size: Optional[int] = None,
    batch_size_bytes: Optional[int] = None,
    batch_readahead: Optional[int] = None,
    fragment_readahead: Optional[int] = None,
    scan_in_order: Optional[bool] = None,
    *,
    prefilter: Optional[bool] = None,
    with_row_id: Optional[bool] = None,
    with_row_address: Optional[bool] = None,
    use_stats: Optional[bool] = None,
    fast_search: Optional[bool] = None,
    full_text_query: Optional[Union[str, dict, FullTextQuery]] = None,
    io_buffer_size: Optional[int] = None,
    late_materialization: Optional[bool | List[str]] = None,
    blob_handling: Optional[str] = None,
    use_scalar_index: Optional[bool] = None,
    include_deleted_rows: Optional[bool] = None,
    order_by: Optional[List[ColumnOrdering]] = None,
    disable_scoring_autoprojection: Optional[bool] = None,
) -> pa.Table
```

Returns a fully-materialized `pyarrow.Table`. Parameters mirror `scanner(...)`. Use for result sets that fit in memory; for large scans use `to_batches`.

### 3.5 `to_batches(...)` — stream Arrow RecordBatches

Verbatim signature:

```python
def to_batches(
    self,
    columns: Optional[Union[List[str], Dict[str, str]]] = None,
    filter: Optional[Union[str, pa.compute.Expression]] = None,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
    nearest: Optional[dict] = None,
    batch_size: Optional[int] = None,
    batch_size_bytes: Optional[int] = None,
    batch_readahead: Optional[int] = None,
    fragment_readahead: Optional[int] = None,
    scan_in_order: Optional[bool] = None,
    *,
    prefilter: Optional[bool] = None,
    with_row_id: Optional[bool] = None,
    with_row_address: Optional[bool] = None,
    use_stats: Optional[bool] = None,
    full_text_query: Optional[Union[str, dict]] = None,
    io_buffer_size: Optional[int] = None,
    late_materialization: Optional[bool | List[str]] = None,
    blob_handling: Optional[str] = None,
    use_scalar_index: Optional[bool] = None,
    strict_batch_size: Optional[bool] = None,
    order_by: Optional[List[ColumnOrdering]] = None,
    disable_scoring_autoprojection: Optional[bool] = None,
    **kwargs,
) -> Iterator[pa.RecordBatch]
```

Returns a lazy `Iterator[pyarrow.RecordBatch]`. This is the out-of-core read path — the dataset is never fully materialized. Feed it to a `pyarrow.RecordBatchReader` or straight into another `write_dataset`.

### 3.6 `head(num_rows, **kwargs)`

```python
def head(self, num_rows, **kwargs):
    ...
    kwargs["limit"] = num_rows
    return self.scanner(**kwargs).to_table()
```

Returns the first `num_rows` as a `pyarrow.Table`. It is exactly `scanner(limit=num_rows, **kwargs).to_table()` — `**kwargs` accepts any `scanner()` parameter.

### 3.7 `take(indices, columns=None)`

Verbatim signature:

```python
def take(
    self,
    indices: Union[List[int], pa.Array],
    columns: Optional[Union[List[str], Dict[str, str]]] = None,
) -> pa.Table
```

Random-access fetch of rows by **positional index** (0-based offset into the dataset, not a user id). Returns a `pyarrow.Table`. `columns` is a list of names or a dict of name → SQL expression.

- To fetch by internal **row id** (not positional), the unstable internal `_take_rows(row_ids, columns)` exists but is documented as internal-use-only.
- To fetch blob columns as file-like objects instead of materializing bytes, use `take_blobs(blob_column, ids=..., addresses=..., indices=...)` — exactly one of `ids`/`addresses`/`indices` must be given; returns `List[BlobFile]`.

### 3.8 `count_rows(filter=None, **kwargs)`

Verbatim signature:

```python
def count_rows(
    self, filter: Optional[Union[str, pa.compute.Expression]] = None, **kwargs
) -> int
```

Returns the number of rows matching `filter` (or the whole dataset if `None`). A SQL-string filter is pushed to the native counter; a PyArrow `Expression` is routed through `scanner(...).count_rows()`. This is a metadata/index-accelerated count — it does not materialize rows.

### 3.9 `get_fragments(filter=None)`

Verbatim signature:

```python
def get_fragments(self, filter: Optional[Expression] = None) -> List[LanceFragment]
```

Returns the dataset's fragments (each an immutable data file group) as `List[LanceFragment]`. Pass the result (or a subset) as `scanner(fragments=...)` to scan specific fragments, or use it for fragment-level maintenance. Fragment/compaction detail: `08_compaction_maintenance.md`.

---

## 4. Minimal examples

### Open, count, filtered scan to Arrow

```python
import lance

ds = lance.dataset("s3://data-sink/active/opps.lance", storage_options=r2_opts)

# metadata-accelerated count with a pushdown filter
n = ds.count_rows("state = 'CA'")

# projected, filtered scan materialized to an Arrow Table
tbl = ds.to_table(
    columns=["opp_id", "agency", "amount"],
    filter="amount > 100000 AND state = 'CA'",
    limit=10_000,
)
```

### Stream batches out-of-core (no full materialization)

```python
for batch in ds.to_batches(columns=["opp_id", "amount"], batch_size=65536):
    process(batch)   # batch is a pyarrow.RecordBatch
```

### Take rows by positional index

```python
rows = ds.take([0, 5, 42], columns=["opp_id", "agency"])   # -> pyarrow.Table
```

### Time travel — pin a version

```python
prev = lance.dataset("s3://data-sink/active/opps.lance",
                     version=17, storage_options=r2_opts)
print([v["version"] for v in ds.versions()])
```

---

## 5. Cross-references

| For | See |
|---|---|
| Ecosystem, packaging, version matrix | `00_overview.md` |
| On-disk file/fragment layout, `data_storage_version` | `01_file_format.md` |
| `append` / `overwrite` semantics, `merge_insert`, `delete`, `update`, `add_columns`, `LanceOperation`, commits | `03_writes_appends_upserts.md` |
| `version`/`asof` time travel, tags, `cleanup_old_versions`, `auto_cleanup_options` | `04_versioning_time_travel.md` |
| BTREE/BITMAP/LABEL_LIST/INVERTED/NGRAM scalar indices, FTS, `full_text_query` | `05_scalar_indices.md` |
| Vector search — `nearest`, `nprobes`, `refine`, IVF_PQ/HNSW | `06_vector_search.md` |
| `storage_options` for R2/S3/GCS/Azure, `commit_lock` | `07_storage_object_stores.md` |
| Compaction, fragment management, index optimization | `08_compaction_maintenance.md` |
| Scan internals, projection/filter pushdown, `take()` detail | `09_scanning_filtering.md` |
| Arrow/DuckDB/Polars/pandas interop, zero-copy | `10_duckdb_arrow_interop.md` |
| LanceDB (the database) — `connect`, tables, `add`/`search` | `11_lancedb_table_api.md` |

---

## 6. Unverified / needs confirmation

- **Source is `main`, not a pinned tag.** All signatures were quoted from the `lancedb/lance` `main` branch on 2026-07-08. The current PyPI release is `pylance` 8.0.0 (2026-07-01). Parameters newest on `main` — e.g. `initial_bases`/`target_bases`/`target_all_bases`, `external_blob_mode`, `blob_pack_file_size_threshold`, `namespace_client`/`table_id`, `disable_scoring_autoprojection`, `strict_batch_size`, `index_segments` — may not all be present in an installed 8.0.0 wheel. Confirm against `lance.__version__` and `help(lance.write_dataset)` in the target environment before relying on them.
- **`ts_types` / `ColumnOrdering` / `VectorSearchQuery` / `FullTextQuery`** are type aliases/classes imported within `lance/dataset.py`; their precise definitions were not expanded here. Treat `asof` as "any timestamp-like value pandas/pyarrow accepts" and confirm the exact alias in-repo if needed.
- **`pd.Timestamp` in the `ReaderLike` union** is present in the union definition but is not a meaningful bulk-data input to `write_dataset`; the practical inputs are the Arrow/pandas/HF types listed in §2.
- **pyarrow minimum version:** confirmed from PyPI JSON metadata (`requires_dist`) on 2026-07-08 — `pylance` 8.0.0 requires **`pyarrow>=14`**.
- The official rendered API-reference URL `https://lancedb.github.io/lance/api/python.html` returned **HTTP 404** on 2026-07-08 (docs have moved between the github.io site and docs.lancedb.com). Signatures here are therefore taken from source, which is authoritative; if you need the rendered reference, locate the current live docs page.
