# Interop — Apache Arrow, DuckDB, Polars/pandas; reading Lance from query engines

> Canonical upstream reference — fetched 2026-07-08 from official Lance documentation and source repositories. Documents upstream behavior as-is; this is NOT core-x doctrine. Treat the version numbers and API signatures here as fetched-date ground truth.
>
> Primary sources:
> - https://lance.org/guide/read_and_write/ — Lance read/write guide: `write_dataset`, `to_table`, `to_batches`, accepted input types
> - https://raw.githubusercontent.com/lance-format/lance/main/python/python/lance/dataset.py — pylance source: verbatim `to_table` / `to_batches` / `scanner` signatures
> - https://www.aidoczh.com/lance/api/python/write_dataset.html — `lance.write_dataset` full signature (mirror of the pylance API reference)
> - https://duckdb.org/docs/current/guides/python/sql_on_arrow.html — DuckDB replacement scans over Arrow Tables / Datasets / Scanners / RecordBatchReaders
> - https://duckdb.org/docs/current/clients/python/conversion — DuckDB → Arrow result conversion (`to_arrow_table`, `to_arrow_reader`); deprecations
> - https://duckdb.org/docs/current/core_extensions/lance — the official DuckDB `lance` core extension: `INSTALL/LOAD`, path scan, `COPY … (FORMAT lance)`, secrets
> - https://github.com/lance-format/lance-duckdb — the `lance` DuckDB extension repo (SQL reference, cloud/secret config)
> - https://duckdb.org/2026/05/21/test-driving-lance — DuckDB blog: `lance` extension on DuckDB 1.5.2, pushdown/perf notes
> - https://docs.pola.rs/api/python/stable/reference/api/polars.scan_pyarrow_dataset.html — `polars.scan_pyarrow_dataset` signature
> - https://pypi.org/project/pylance/ — current pylance version (8.0.0, 2026-07-01)
> - https://github.com/lance-format/lance/issues/642 — "[DuckDB] Support predicate pushdown via pyarrow dataset": the intended mapping of a DuckDB `WHERE` clause over a Lance pyarrow dataset to `Dataset.scanner(columns=…, filter="…SQL…")`
> - https://deepwiki.com/lance-format/lance-duckdb/4.1-scan-operations — `lance` extension scan internals: Filter IR serialization, native FFI scan (`__lance_scan`), index-aware scan-mode selection (contrasted with a plain pyarrow replacement scan)
>
> Talk / historical sources (committed clean transcript layer — spoken claims, marked where not independently verified):
> - docs/youtube-transcripts/clean/2023-06-lance-columnar-format-duckcon3.clean.md — "Bringing AI to DuckDB with Lance Columnar Format for Multi-Modal AI", DuckCon #3, San Francisco, June 2023 (Chang She). Early forward-looking talk; used here only for historical interop context (§10).

Scope: How Lance datasets move data in and out of Apache Arrow and how to read them from DuckDB (both the native `lance` extension and the pyarrow replacement-scan path), Polars, pandas, and — briefly — Spark and Ray.

---

## 0. Versions as of 2026-07-08

| Component | Current version | Notes |
| --- | --- | --- |
| `pylance` (Python SDK for Lance) | **8.0.0** (released 2026-07-01) | PyPI package name is `pylance`; import name is `lance`. Requires Python ≥ 3.9. Prior: 7.0.0 (2026-05-27), 6.0.1 (2026-05-20). |
| `lance` DuckDB core extension | ships as a DuckDB **core extension** (`INSTALL lance;`) | Introduced/usable on **DuckDB 1.5.x**; the DuckDB blog test-drive used **DuckDB 1.5.2** (2026-05-21). Repo: `lance-format/lance-duckdb`. |
| DuckDB | 1.5.x line (1.5.1 announced 2026-03-23; 1.5.2 referenced 2026-05) | The `lance` extension requires a matching DuckDB 1.5+ build. |
| `polars` | current stable (2026) | `scan_pyarrow_dataset` is marked **unstable** upstream. |

> Repository move: the canonical Lance repo is now **`github.com/lance-format/lance`** and docs are hosted at **`lance.org`** (older links under `github.com/lancedb/lance` and `lancedb.github.io/lance` still resolve for some pages but 404 for others — prefer `lance.org` and `lance-format/*`). `lancedb` (the embedded vector DB) remains a separate package/repo; see [11_lancedb_table_api.md](11_lancedb_table_api.md).

---

## 1. Lance ⇄ Apache Arrow

Lance is Arrow-native end to end. **Arrow is the intermediate representation on both the read and write path** — there is no separate Lance in-memory row model. A `LanceDataset` is itself a subclass of `pyarrow.dataset.Dataset`, which is what makes the replacement-scan and Polars paths below work with zero glue.

```python
import lance
import pyarrow as pa

ds = lance.dataset("./example.lance")
assert isinstance(ds, pa.dataset.Dataset)   # LanceDataset IS a pyarrow Dataset
```

### 1.1 Reading into Arrow

- `LanceDataset.to_table(...)` → returns a single in-memory `pyarrow.Table`.
- `LanceDataset.to_batches(...)` → returns an `Iterator[pyarrow.RecordBatch]` for out-of-core streaming.
- `LanceDataset.scanner(...)` → returns a `LanceScanner` (lazy) that can then produce a Table (`.to_table()`) or a `pyarrow.RecordBatchReader` (`.to_reader()`), and integrates with DuckDB/Polars as an Arrow dataset scan.

### 1.2 Writing from Arrow

`lance.write_dataset` consumes Arrow (and things convertible to Arrow) directly. Per the read/write guide, accepted input types are:

> `lance.write_dataset` supports writing `pyarrow.Table`, `pandas.DataFrame`, `pyarrow.dataset.Dataset`, and `Iterator[pyarrow.RecordBatch]`.

The API reference lists the accepted `data_obj` types as: **Pandas DataFrame, PyArrow Table, `pyarrow.dataset.Dataset`, `pyarrow.dataset.Scanner`, `pyarrow.RecordBatchReader`, HuggingFace `Dataset`.**

### 1.3 Zero-copy semantics

Arrow's memory model allows zero-copy sharing of columnar buffers between libraries that speak Arrow (the C Data Interface / same in-process Arrow buffers). In practice:

- **DuckDB ↔ Arrow** (`to_arrow_table()`, `from_arrow()`, replacement scans) exchanges Arrow C-Data buffers without re-serializing; DuckDB scans Arrow buffers in place.
- **Lance read → Arrow** decodes Lance's on-disk columnar encoding into Arrow arrays. This is a **decode**, not a byte-for-byte zero-copy of the file (Lance has its own on-disk encodings — see [01_file_format.md](01_file_format.md)); once decoded into an Arrow `Table`/`RecordBatch`, downstream Arrow consumers (DuckDB, Polars, pandas-via-Arrow) share those buffers without a further copy.
- `Table.to_pandas()` copies into NumPy-backed columns (not zero-copy for most types); `Table.to_pandas(types_mapper=pd.ArrowDtype)` keeps Arrow-backed columns and avoids the copy for supported types.

> Relevance to core-x: the read path back into DuckDB is the load-bearing interop here. A Lance scan decoded to Arrow `RecordBatch`es, streamed into DuckDB via a replacement scan, keeps the whole SoR→SQL hop in Arrow buffers with no Parquet round-trip. Combined with DuckDB `memory_limit` + `temp_directory` spill (§4.4), this reads hundreds-of-millions-of-rows Lance datasets out-of-core without materializing a full `to_table()`.

---

## 2. pylance read API — exact signatures

Quoted **verbatim** from pylance source (`python/python/lance/dataset.py`, `main`, fetched 2026-07-08). These are the `LanceDataset` methods. Parameters after the bare `*` are keyword-only.

### 2.1 `LanceDataset.to_table` → `pyarrow.Table`

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
) -> pa.Table:
```

### 2.2 `LanceDataset.to_batches` → `Iterator[pyarrow.RecordBatch]`

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
) -> Iterator[pa.RecordBatch]:
```

### 2.3 `LanceDataset.scanner` → `LanceScanner`

```python
def scanner(
    self,
    columns: Optional[Union[List[str], Dict[str, str]]] = None,
    filter: Optional[
        Union[str, pa.compute.Expression, FullTextQuery, VectorSearchQuery, dict]
    ] = None,
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
    blob_handling: Optional[
        Literal["all_binary", "blobs_descriptions", "all_descriptions"]
    ] = None,
    use_scalar_index: Optional[bool] = None,
    include_deleted_rows: Optional[bool] = None,
    scan_stats_callback: Optional[Callable[[ScanStatistics], None]] = None,
    strict_batch_size: Optional[bool] = None,
    order_by: Optional[List[Union[ColumnOrdering, str]]] = None,
    disable_scoring_autoprojection: Optional[bool] = None,
) -> LanceScanner:
```

### 2.4 Key parameters (read path)

| Parameter | Type | Default | Meaning |
| --- | --- | --- | --- |
| `columns` | `List[str]` or `Dict[str,str]` | `None` (all columns) | Projection. A dict maps output name → SQL expression (computed/renamed columns). Drives **column pushdown**. |
| `filter` | `str` (SQL) or `pa.compute.Expression` | `None` | Predicate. SQL string form (e.g. `"label = 2 AND text IS NOT NULL"`) enables scalar-index and stats pushdown; see [09_scanning_filtering.md](09_scanning_filtering.md). |
| `limit` / `offset` | `int` | `None` | Row limit / skip. |
| `batch_size` | `int` | `None` (engine default) | Rows per `RecordBatch` emitted by `to_batches` / scanner. |
| `batch_size_bytes` | `int` | `None` | Target bytes-per-batch (alternative to row count). |
| `with_row_id` | `bool` | `None`/`False` | Include the Lance `_rowid` column. |
| `prefilter` | `bool` | `None` | Apply the `filter` before (vs after) the ANN/vector search step. |
| `use_scalar_index` | `bool` | `None` (use if present) | Whether the planner may use BTREE/BITMAP scalar indices for the filter. See [05_scalar_indices.md](05_scalar_indices.md). |
| `nearest` | `dict` | `None` | Vector-search spec; see [06_vector_search.md](06_vector_search.md). |

`to_table` loads everything into memory; `to_batches` streams — use `to_batches` (or a `scanner().to_reader()`) for datasets larger than RAM.

```python
# Full-table load with projection + predicate pushdown
table = ds.to_table(
    columns=["image", "label"],
    filter="label = 2 AND text IS NOT NULL",
    limit=1000,
    offset=3000,
)

# Out-of-core streaming (does not materialize the whole dataset)
for batch in ds.to_batches(columns=["image"], filter="label = 10"):
    compute_on_batch(batch)   # batch is a pyarrow.RecordBatch

# Random access by row index
rows = ds.take([1, 100, 500], columns=["image", "label"])
```

---

## 3. pylance write API — `lance.write_dataset`

Quoted from the pylance API reference (mirror). Version-note: defaults shown are the current API surface.

```python
lance.write_dataset(
    data_obj: ReaderLike,
    uri: str | Path | LanceDataset | None = None,   # source: Optional, default None (uri OR namespace_client+table_id must be provided)
    schema: pa.Schema | None = None,
    mode: str = 'create',
    *,
    max_rows_per_file: int = 1048576,
    max_rows_per_group: int = 1024,
    max_bytes_per_file: int = 96636764160,
    commit_lock: CommitLock | None = None,
    progress: FragmentWriteProgress | None = None,
    storage_options: dict[str, str] | None = None,
    data_storage_version: str | None = None,
    use_legacy_format: bool | None = None,
    enable_v2_manifest_paths: bool = True,   # source default is True (verified from lance/dataset.py, main, 2026-07-08)
    enable_stable_row_ids: bool = False,     # param name is enable_stable_row_ids (NOT enable_move_stable_row_ids)
    auto_cleanup_options: AutoCleanupConfig | None = None,
    # ... (source also carries additional keyword-only params: commit_message,
    #      transaction_properties, external_blob_mode="reference", namespace_client, table_id, etc.)
) -> LanceDataset
```

| Parameter | Type | Default | Meaning |
| --- | --- | --- | --- |
| `data_obj` | `ReaderLike` — Pandas DataFrame, PyArrow `Table`, `pyarrow.dataset.Dataset`, `pyarrow.dataset.Scanner`, `pyarrow.RecordBatchReader`, HuggingFace `Dataset`, or `Iterator[pa.RecordBatch]` | — | Input data. Iterator/reader inputs stream (out-of-core write). |
| `uri` | `str \| Path \| LanceDataset` | — | Destination dataset URI (local path or object-store URI). |
| `schema` | `pa.Schema \| None` | `None` | Required when `data_obj` is a bare iterator with no inferable schema. |
| `mode` | `str` | `'create'` | `'create'` (fail if exists), `'append'` (concat onto latest version; fail if not exists), `'overwrite'` (new snapshot version). |
| `max_rows_per_file` | `int` | `1048576` | Fragment file row cap. |
| `max_rows_per_group` | `int` | `1024` | Row group size within a file. |
| `max_bytes_per_file` | `int` | `96636764160` (≈90 GiB) | Byte cap per file. |
| `storage_options` | `dict[str,str] \| None` | `None` | Object-store credentials/endpoint (S3/R2/GCS/Azure). See [07_storage_object_stores.md](07_storage_object_stores.md). |
| `data_storage_version` | `str \| None` | `None` | On-disk encoding version. Source `Literal`: `"stable"`, `"2.0"`, `"2.1"`, `"2.2"`, `"2.3"`, `"next"`, `"legacy"`, `"0.1"`. Newer = more efficient but needs newer readers. |

> `mode` accepts these three string values verbatim: **`create`**, **`overwrite`**, **`append`**. Full write semantics (append vs `merge_insert` upsert vs `delete`/`update`) live in [03_writes_appends_upserts.md](03_writes_appends_upserts.md).

```python
import lance, pyarrow as pa

table = pa.Table.from_pylist([{"name": "Alice", "age": 20},
                              {"name": "Bob", "age": 30}])
ds = lance.write_dataset(table, "./alice_and_bob.lance")

# Streaming write from an iterator of RecordBatches (schema required)
from typing import Iterator
def producer() -> Iterator[pa.RecordBatch]:
    yield pa.RecordBatch.from_pylist([{"name": "Alice", "age": 20}])
    yield pa.RecordBatch.from_pylist([{"name": "Bob", "age": 30}])

schema = pa.schema([("name", pa.string()), ("age", pa.int32())])
ds = lance.write_dataset(producer(), "./alice_and_bob.lance", schema=schema)
```

---

## 4. Reading Lance from DuckDB

There are **two distinct paths**, and both are real as of 2026-07-08. Pick per your DuckDB version and whether you want pure-SQL or Python glue.

### 4.1 Path A — the official `lance` DuckDB extension (SQL-native)

**Verified: an official DuckDB `lance` core extension exists** (repo `lance-format/lance-duckdb`; documented at `duckdb.org/docs/current/core_extensions/lance`). It is a collaboration between DuckDB and LanceDB. It supports both **reading** Lance datasets and **writing** them via `COPY … (FORMAT lance)`, plus native vector/FTS/hybrid search table functions.

Install & load:

```sql
INSTALL lance;
LOAD lance;
```

Read a Lance dataset by path (replacement-scan style — a `.lance` path is queried directly as a table):

```sql
SELECT * FROM 'path/to/dataset.lance' LIMIT 10;
SELECT * FROM 's3://bucket/path/to/out.lance' LIMIT 10;
```

Write / append via `COPY`:

```sql
-- overwrite (creates a new snapshot version)
COPY (
    SELECT 1::BIGINT AS id, 'a'::VARCHAR AS s
    UNION ALL
    SELECT 2::BIGINT AS id, 'b'::VARCHAR AS s
) TO 'path/to/dataset.lance' (FORMAT lance, MODE 'overwrite');

-- append
COPY (
    SELECT 3::BIGINT AS id, 'c'::VARCHAR AS s
) TO 'path/to/dataset.lance' (FORMAT lance, MODE 'append');
```

`COPY … (FORMAT lance)` options seen in the extension docs: `mode` (`'overwrite'` | `'append'`) and `write_empty_file`.

Native search table functions (extension-specific, beyond plain SQL):

```sql
-- Vector (ANN) search
SELECT id, label, _distance
FROM lance_vector_search(
    'path/to/dataset.lance', 'vec',
    [0.1, 0.2, 0.3, 0.4]::FLOAT[4],
    k = 5, prefilter = true
)
ORDER BY _distance ASC;

-- Full-text search
SELECT id, text, _score
FROM lance_fts('path/to/dataset.lance', 'text', 'puppy', k = 10, prefilter = true)
ORDER BY _score DESC;
```

Function parameter sets documented for the extension:
- `lance_vector_search(uri, vector_column, query_vector, ...)` — `k`, `use_index`, `nprobs`, `refine_factor`, `prefilter`, `filter`; returns dataset columns + `_distance`.
- `lance_fts(uri, text_column, query, ...)` — `k`, `prefilter`, `filter`; returns dataset columns + `_score`.
- `lance_hybrid_search(uri, vector_column, query_vector, text_column, query, ...)` — adds `alpha`, `oversample_factor`.

Object-store (S3 / S3-compatible, incl. Cloudflare R2) secret for the extension:

```sql
-- credential-chain (uses ambient AWS creds)
CREATE SECRET (
    TYPE lance,
    PROVIDER credential_chain,
    SCOPE 's3://bucket/'
);

-- explicit config (S3-compatible endpoints: MinIO / R2 / etc.)
CREATE SECRET (
    TYPE LANCE,
    PROVIDER config,
    SCOPE 's3://my-bucket/',
    ACCESS_KEY_ID '...',
    SECRET_ACCESS_KEY '...',
    REGION 'auto',            -- R2 uses 'auto'
    ENDPOINT '<accountid>.r2.cloudflarestorage.com'
);
```

Secret parameters: `ACCESS_KEY_ID`, `SECRET_ACCESS_KEY`, `REGION`, `ENDPOINT` (custom/non-AWS S3-compatible), `VIRTUAL_HOSTED_STYLE_REQUEST` (`true`/`false`), `ALLOW_HTTP` (`true`/`false`, for MinIO/local). For R2 use `ENDPOINT` = the R2 S3 API endpoint and `REGION 'auto'`; R2 wants **path-style** requests, so keep `VIRTUAL_HOSTED_STYLE_REQUEST false` (the default). Cross-reference the pylance-side R2 config in [07_storage_object_stores.md](07_storage_object_stores.md).

> The extension's own docs reference a `read_lance(...)` table function alongside the direct-path scan, but the current public docs demonstrate the **direct path form** (`FROM 'x.lance'`) rather than a fully enumerated `read_lance` signature. The full `read_lance` parameter list (e.g. version/tag time-travel, `with_row_id`) is not cleanly published as of the fetch date — see "Unverified / needs confirmation" below.

Platform support for the extension: Linux (AMD64/ARM64), macOS (ARM64), Windows (AMD64).

Version gate: the extension targets **DuckDB 1.5+** (the DuckDB blog test-drive ran DuckDB **1.5.2**). On older DuckDB, use Path B.

### 4.2 Path B — pyarrow replacement scan (works on any DuckDB, no extension)

Because a `LanceDataset` **is** a `pyarrow.dataset.Dataset`, DuckDB's Arrow **replacement scan** can query it by variable name with **column and filter pushdown into the Lance scan**. This is the portable path and works without the `lance` extension.

```python
import duckdb
import lance

lance_ds = lance.dataset("s3://data-sink/active/accounts.lance",
                         storage_options={...})   # R2 creds; see 07_storage_object_stores.md

con = duckdb.connect()
# `lance_ds` is referenced by name — DuckDB pushes the projection + WHERE
# down into the pyarrow/Lance scan, so only needed columns/rows are pulled.
res = con.execute("""
    SELECT account_id, revenue
    FROM lance_ds
    WHERE region = 'NA' AND revenue > 1000000
""").to_arrow_table()
```

DuckDB replacement scans work for four Arrow object types: **Arrow Tables**, **Arrow Datasets** (a `LanceDataset` qualifies; DuckDB pushes column selection + row filters into the dataset scan), **Arrow Scanners** (filters via Arrow compute, not DuckDB pushdown), and **RecordBatchReaders** (Arrow streaming format). You can also register explicitly or wrap it:

```python
# Explicit registration (equivalent to name-based replacement scan)
con.register("accounts", lance_ds)
con.sql("SELECT COUNT(*) FROM accounts WHERE region = 'NA'")

# Wrap an existing Arrow object as a DuckDB relation
rel = con.from_arrow(lance_ds.to_table(columns=["account_id", "revenue"]))
```

Streaming Lance → DuckDB out-of-core (no full materialization):

```python
reader = lance_ds.scanner(columns=["account_id", "revenue"],
                          filter="region = 'NA'").to_reader()  # pyarrow.RecordBatchReader
con.register("stream", reader)      # RecordBatchReaders are replacement-scannable
con.execute("SELECT region, SUM(revenue) FROM stream GROUP BY region")
```

> Footgun (Path B pushdown): DuckDB pushes filters into a pyarrow Dataset as **pyarrow compute expressions**, whereas Lance's own fast filtering expects **SQL-string** filters. Predicates DuckDB pushes through the pyarrow dataset interface do get applied, but they do **not** light up Lance's scalar/vector indices the way a native `ds.scanner(filter="…SQL…")` or the `lance` extension would. For index-accelerated filtering at scale, prefer Path A (the extension) or pre-filter with `ds.scanner(filter=...)`/`ds.to_batches(filter=...)` in pylance and hand DuckDB the already-narrowed reader. Full statement of the mechanism, verification status, and prescription: **§4.5**.

### 4.3 DuckDB → Arrow result conversion (exact API + deprecations)

| Method (on a DuckDB result / relation) | Returns | Notes |
| --- | --- | --- |
| `.to_arrow_table()` | `pyarrow.Table` | Current name for fetching results as an Arrow table. |
| `.to_arrow_reader(chunk_size)` | Arrow `RecordBatchReader` | Streams results; `chunk_size` = rows per batch. |
| `.arrow()` | Arrow `RecordBatchReader` | Older alias; DuckDB docs recommend `to_arrow_reader()` instead. |
| `con.from_arrow(arrow_object)` | `DuckDBPyRelation` | `from_arrow(self, arrow_object: object) -> DuckDBPyRelation` — accepts `pyarrow.Table` / `pyarrow.RecordBatch`. |
| `con.register(name, python_object)` | — | Registers a Python (Arrow) object as a named view for SQL. |

**Deprecated (do not use in new code):**

> `fetch_arrow_table()` and `fetch_record_batch()` are deprecated. Use `to_arrow_table()` and `to_arrow_reader()` instead.

### 4.4 Out-of-core DuckDB against Lance

> Relevance to core-x: at hundreds-of-millions-of-rows, do not `to_table()` a whole Lance dataset into DuckDB. Register the Lance dataset (Path A extension, or Path B replacement scan/`RecordBatchReader`) and let DuckDB stream. Bound memory with `SET memory_limit='…'` and give DuckDB a spill directory with `SET temp_directory='/path/to/spill'` (and `SET max_temp_directory_size='…'` on 1.x) so hash joins/aggregations/sorts that exceed RAM spill to disk instead of OOMing. Projection + SQL-string `WHERE` pushed into the Lance scan (or a pre-narrowed `ds.scanner(filter=…)` reader) is what keeps the scanned byte volume — and therefore the spill — small.

### 4.5 ⚠️ Index-pushdown footgun — a registered Arrow view does NOT engage Lance indices

This is the load-bearing query-optimization fact for structured retrieval over Lance. Read it before designing any DuckDB-over-Lance filter path.

**Mechanism (upstream).** Lance's scalar indices (BTREE / BITMAP / LABEL_LIST / INVERTED-FTS / NGRAM — see [05_scalar_indices.md](05_scalar_indices.md)) and vector/ANN indices (see [06_vector_search.md](06_vector_search.md)) are engaged **only through the Lance Scanner API** — i.e. via `ds.scanner(...)` / `ds.to_table(...)` / `ds.to_batches(...)` with:

- an SQL-**string** `filter=` predicate (this is what the scalar-index / stats-pushdown planner reads; `use_scalar_index=` gates whether the planner may use a scalar index), and
- for vector search, `nearest=` (plus `prefilter=True` to apply the scalar `filter` **before** the ANN step rather than after).

The verified signatures for these arguments are in §2.1–§2.4 above.

When a `LanceDataset` is instead handed to DuckDB as a **registered Arrow view / pyarrow replacement scan** (Path B, §4.2 — `con.register(name, lance_ds)` or a bare `FROM lance_ds`), the DuckDB scan does **not** engage Lance's scalar or vector indices. DuckDB pushes its `WHERE` down to the pyarrow dataset interface as **pyarrow compute expressions**; those predicates are applied (rows are filtered), but they do not go through the index-accelerated planner path that a native `ds.scanner(filter="…SQL…")` call exercises. Upstream framing: the pyarrow-dataset predicate-pushdown feature (Lance issue [#642](https://github.com/lance-format/lance/issues/642)) is defined as mapping a DuckDB `WHERE a > 1 AND b < 2` to `Dataset.scanner(columns=[…], filter="a > 1 AND b < 2")` — a **filter-application** contract, distinct from index engagement, which the public docs do not assert for the pyarrow-view path.

> Verification status (checked against `lance.org` / `duckdb.org`, 2026-07-08): **Confirmed** — (a) scalar/vector indices are documented as engaged via the scanner `filter=` (SQL string) / `nearest=` / `use_scalar_index=` / `prefilter=` arguments (§2 signatures; [05](05_scalar_indices.md)/[06](06_vector_search.md)); (b) the pyarrow-dataset pushdown contract (issue #642) is filter-application, not index engagement; (c) the DuckDB `lance` **extension** (Path A) is a **separate** native path — per its scan-operation docs it serializes a Filter IR to the Lance Rust FFI (`__lance_scan`) and its optimizer "selects the scan mode based on index availability … to leverage the index," so the extension can use indices where the pyarrow view cannot. **Not cleanly published / ambiguous:** the exact set of predicate shapes that light up each scalar-index type through the extension, and any threshold (e.g. `LIMIT` presence) at which its planner switches to an index-backed scan mode — treat those as version-dependent and confirm at your pinned extension version. Do not assume the pyarrow-view path silently uses an index just because one exists on the column.

**Prescription (structured retrieval over Lance):** push the predicate into the Lance scanner and hand the already-filtered/limited result to DuckDB; use DuckDB for the joins/aggregations over the reduced set. Two concrete forms:

```python
# Index-accelerated filter in pylance → DuckDB does the joins/aggregations.
# The SQL-string filter (and optional nearest=/prefilter=) is what engages
# Lance BTREE/BITMAP/vector indices; DuckDB never sees the unfiltered dataset.
reader = lance_ds.scanner(
    columns=["account_id", "revenue", "region"],
    filter="region = 'NA' AND revenue > 1000000",   # SQL string → scalar-index path
    use_scalar_index=True,
).to_reader()                                        # pyarrow.RecordBatchReader
con.register("narrowed", reader)
con.execute("""
    SELECT region, SUM(revenue)
    FROM narrowed JOIN dims USING (account_id)
    GROUP BY region
""")
```

Or use **Path A (the `lance` extension)** for pure-SQL index-accelerated filtering when on DuckDB 1.5+ (§4.1) — its native scan can leverage indices directly. The failure mode to avoid: registering the raw Lance dataset as an Arrow view and expecting `WHERE indexed_col = …` to be index-accelerated — it will scan-and-filter, not index-seek.

**⚠️ Nested-schema panic on the Path-B pushdown (lance#6130).** Worse than the missed index: on a dataset whose schema carries a **struct-under-list** column (`list<struct>`, `map`, or anything wrapping one — e.g. `entity_profile_gold.pocs`), *every* predicate DuckDB pushes into the registered view — plain `WHERE` equalities AND dynamic join filters, regardless of which column is filtered — **aborts the scan**: pylance converts the pushed pyarrow compute Expression to Substrait, and `lance-datafusion/substrait.rs` counts schema names with DataFusion's *deep* convention (recursing into list-element structs) while PyArrow emits *shallow* names, so the name index overruns the array and the Rust task panics (`index out of bounds: the len is <top-level cols> but the index is <flattened ordinal>`), surfacing as `_duckdb.Error: RuntimeError: Task was aborted`. Verified 2026-07-08 on pylance 7.0.0 **and 8.0.0** (duckdb 1.5.4): upstream [issue #6130](https://github.com/lance-format/lance/issues/6130) is open, the bug dates to PR #5015 (every pylance ≥3), and fix [PR #6469](https://github.com/lance-format/lance/pull/6469) is unmerged — `substrait.rs` is byte-identical v7.0.0 → v8.0.0 → main, so **upgrading pylance does not help**. Benign shapes verified unaffected: flat schemas, `list<primitive>`, bare top-level structs. Two facts that constrain any workaround, both verified: DuckDB **trusts** a pushed filter (it does not re-apply — dropping the filter silently returns unfiltered rows), and DuckDB always includes the filter's columns in the projection it pushes. The gateway's mitigation (apps/gtm_mcp/src/database.py `_bridge_safe`): struct-under-list datasets are registered as a `LanceDataset` subclass whose `scanner()` intercepts Expression filters, keeps the projection pushed into Lance, and applies the filter via `pyarrow.dataset.Scanner.from_batches` over the projected stream — SQL-string filters (the index-accelerated path above) are untouched. `SET disabled_optimizers = 'filter_pushdown,join_filter_pushdown'` also suppresses the crash but is GLOBAL-scoped in DuckDB — it degrades every other scan on the connection.

> The terser per-path callouts at §4.2 (DuckDB Path B) and §6 (Polars) are the same mechanism; this section is the consolidated statement.

---

## 5. Reading Lance from pandas

pandas has no native Lance reader; go through Arrow.

```python
import lance
ds = lance.dataset("./example.lance")

# Whole dataset (in-memory) → pandas
df = ds.to_table(columns=["id", "label"], filter="label = 2").to_pandas()

# Arrow-backed pandas (avoids the NumPy copy for supported dtypes)
import pandas as pd
df = ds.to_table().to_pandas(types_mapper=pd.ArrowDtype)

# Out-of-core: iterate batches, convert per-batch
for batch in ds.to_batches(columns=["id"], batch_size=1_000_000):
    part = batch.to_pandas()
    ...
```

Writing pandas → Lance is direct (`data_obj` accepts a `pandas.DataFrame`):

```python
lance.write_dataset(df, "./out.lance", mode="overwrite")
```

---

## 6. Reading Lance from Polars

A `LanceDataset` is a `pyarrow.dataset.Dataset`, so it plugs into Polars' pyarrow-dataset scan. Two options:

### 6.1 Eager / Arrow-bridge

```python
import lance, polars as pl
ds = lance.dataset("./example.lance")

# Eager DataFrame from an Arrow table
pdf = pl.from_arrow(ds.to_table(columns=["id", "label"], filter="label = 2"))
```

### 6.2 Lazy scan of the pyarrow dataset

`polars.scan_pyarrow_dataset` accepts any `pyarrow.dataset.Dataset` (thus a `LanceDataset`) and returns a `LazyFrame`.

```python
polars.scan_pyarrow_dataset(
    source: pa.dataset.Dataset,
    *,
    allow_pyarrow_filter: bool = True,
    batch_size: int | None = None,
) -> LazyFrame
```

| Parameter | Type | Default | Meaning |
| --- | --- | --- | --- |
| `source` | `pa.dataset.Dataset` | — | The pyarrow dataset to scan (a `LanceDataset` qualifies). |
| `allow_pyarrow_filter` | `bool` | `True` | Allow predicate pushdown into pyarrow. **Can change results vs Polars for null comparisons** (pyarrow null semantics differ). |
| `batch_size` | `int \| None` | `None` | Max rows per scanned pyarrow record batch. |

```python
import lance, polars as pl
ds = lance.dataset("./example.lance")
lf = pl.scan_pyarrow_dataset(ds)
out = lf.filter(pl.col("label") == 2).select(["id", "label"]).collect()
```

> Footguns (Polars):
> - `scan_pyarrow_dataset` is marked **unstable** upstream (may change without a breaking-change notice).
> - Polars only pushes down predicates pyarrow accepts (not the full Polars expression API), and it pushes them as **pyarrow compute expressions** — which pyarrow's Lance-dataset scanner does not translate into Lance's SQL-string fast-filter/index path. Net: Polars-through-pyarrow does **not** exploit Lance's scalar/vector index acceleration. For index-accelerated reads, filter in pylance first (`ds.scanner(filter="…SQL…").to_reader()` → `pl.from_arrow` per batch) or use the DuckDB `lance` extension.
> - `null`-value comparisons can differ between the pushed-down pyarrow filter and Polars' own semantics — verify results if nulls are in play.

---

## 7. Spark & Ray (brief)

### 7.1 Apache Spark

The **Apache Spark Connector for Lance** (`lance-format/lance-spark`) lets Spark read and write Lance datasets. It is built on Spark's **DataSourceV2 (DSv2)** API and supports distributed parallel scans plus column and filter pushdown. Docs: `lance.org/integrations/spark/`. Lance also integrates with catalogs (Apache Polaris, Unity Catalog, Apache Gravitino) via Lance Namespace for Spark-managed tables.

### 7.2 Ray

The **Lance–Ray integration** provides conversion between Ray Datasets and Lance with optimized parallel I/O. Write a Ray Dataset to Lance with `ray.data.Dataset.write_lance(...)`; read Lance into Ray for distributed processing. Docs: `lance.org/integrations/ray/`. This is the standard path for distributed writes at scale (see also `lance.org/guide/distributed_write/`).

Other documented ecosystem connectors: Trino, Apache Flink, Daft (`df.write_lance()` / Lance source), PyTorch.

---

## 8. Deprecations, renames, and footguns (consolidated)

- **DuckDB result → Arrow:** `fetch_arrow_table()` / `fetch_record_batch()` are **deprecated** → use `to_arrow_table()` / `to_arrow_reader()`. `arrow()` still works but `to_arrow_reader()` is preferred.
- **`lance` DuckDB extension is real** (as of 2026, DuckDB 1.5+). Do **not** assume you must go through pyarrow — but do check your DuckDB version; on < 1.5 the extension is unavailable and Path B (pyarrow replacement scan) is the fallback.
- **Repo/docs moved** to `lance-format/lance` and `lance.org`; many old `lancedb.github.io/lance/api/...` API pages now 404. Use `lance.org` or the pylance source.
- **Polars pushdown ≠ Lance index acceleration** through `scan_pyarrow_dataset` (see §6). Same caveat for DuckDB Path B (§4.2).
- **`write_dataset` iterator input requires `schema=`** — a bare `Iterator[RecordBatch]` has no inferable schema.
- **`to_table` is eager / in-memory.** For large Lance datasets use `to_batches` / `scanner().to_reader()`.
- **`to_pandas()` copies** unless you pass `types_mapper=pd.ArrowDtype`.
- **`nprobs`** (not `nprobes`) is the parameter name in the DuckDB `lance_vector_search` table function per the extension SQL reference; the pylance `nearest` dict uses `nprobes`. Watch the spelling per surface. (Flagged below as needs-confirmation.)

---

## 9. Unverified / needs confirmation

- **DuckDB `read_lance(...)` full signature:** the extension repo references a `read_lance` table function, but the current public docs demonstrate the direct-path scan (`FROM 'x.lance'`) rather than an enumerated `read_lance(uri, version=…, with_row_id=…, …)` signature. Time-travel-by-version/tag and `with_row_id` via the extension's SQL surface are **not confirmed** from a quoted source as of 2026-07-08. Confirm against `github.com/lance-format/lance-duckdb/blob/main/docs/sql.md` at your pinned extension version.
- **Exact DuckDB version that first shipped the `lance` core extension:** confirmed *usable* on DuckDB **1.5.2** (blog, 2026-05-21) and referenced around 1.5.1 (2026-03-23); the precise first-shipping version number is not pinned here.
- **`lance_vector_search` parameter spelling (`nprobs` vs `nprobes`):** the extension SQL reference lists `nprobs`; pylance's `nearest` dict uses `nprobes`. Verify per surface before relying on it.
- **pylance `to_batches` `blob_handling` typing:** source shows `Optional[str]` on `to_batches`/`to_table` but a `Literal["all_binary","blobs_descriptions","all_descriptions"]` on `scanner`; treat those three strings as the accepted values.
- **`polars.scan_pyarrow_dataset` stability:** upstream flags it **unstable**; the signature above may drift.

---

## 10. Historical context — the pre-extension interop era (2023)

Context for how far the DuckDB ⇄ Lance ⇄ Arrow interop has come. In the **DuckCon #3 talk** (San Francisco, June 2023 — Chang She; `docs/youtube-transcripts/clean/2023-06-lance-columnar-format-duckcon3.clean.md`), given years before the native `lance` DuckDB extension (§4.1) and its Filter IR / index-aware scan existed, the state of type interoperability was described as:

> "Arrow and DuckDB types are maybe 80% interoperable. Unfortunately, right now AI sort of falls into that missing 20%."
> — DuckCon #3, 2023-06 (`…/2023-06-lance-columnar-format-duckcon3.clean.md`)

That missing ~20% was, per the talk, three buckets: **nested types** (annotations, bounding boxes, labels), **extension types** (images, embeddings, videos, point clouds), and a few **ML-specific scalar types** (e.g. `bf16`, characterized as "fairly easy to add"). The talk also flagged the same push-down seam this file documents at §4.2/§4.5 — that DuckDB pushes predicates down as **PyArrow compute expressions**, which "is not a standard across different Arrow implementations," while Lance (Rust) uses **DataFusion** for predicate push-down — and floated **Substrait** as a possible "long-term interface" for that seam ("but who knows").

These are **talk-reported** framings, not independently verified — the "80% / 20%" split is a spoken round number characterizing 2023-era maturity, and the Substrait direction was an open speculation, not a shipped interface. They are recorded here only as historical baseline: the AI/nested/extension-type gap and the compute-expression push-down seam the speaker anticipated are precisely what the native `lance` extension's Arrow-type coercion (e.g. in-place Float16 buffer rewriting) and Filter-IR-to-Rust-FFI scan path (§4.1, §4.5) later addressed.

---

### Sibling files (this domain)

- [00_overview.md](00_overview.md) — Lance & LanceDB overview, ecosystem, packaging & versions
- [01_file_format.md](01_file_format.md) — Lance columnar file format & on-disk layout
- [02_python_dataset_api.md](02_python_dataset_api.md) — pylance SDK: `lance.dataset`, `lance.write_dataset`, `LanceDataset`
- [03_writes_appends_upserts.md](03_writes_appends_upserts.md) — writes: modes, append, `merge_insert`, delete, update, commits
- [04_versioning_time_travel.md](04_versioning_time_travel.md) — versioning, time travel, tags, cleanup
- [05_scalar_indices.md](05_scalar_indices.md) — scalar indices: BTREE, BITMAP, LABEL_LIST, INVERTED/FTS, NGRAM
- [06_vector_search.md](06_vector_search.md) — vector indices & ANN search
- [07_storage_object_stores.md](07_storage_object_stores.md) — object-store config: S3 / Cloudflare R2 / GCS / Azure
- [08_compaction_maintenance.md](08_compaction_maintenance.md) — compaction, index optimization, fragment management
- [09_scanning_filtering.md](09_scanning_filtering.md) — scanning, filtering, projection pushdown, `take()`
- [11_lancedb_table_api.md](11_lancedb_table_api.md) — LanceDB (the database): connect, tables, add/search, FTS, cloud/remote
