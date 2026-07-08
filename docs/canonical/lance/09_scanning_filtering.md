# Scanning, Filtering, Projection Pushdown & take()

> Canonical upstream reference — fetched 2026-07-08 from official Lance documentation and source repositories. Documents upstream behavior as-is; this is NOT core-x doctrine. Treat the version numbers and API signatures here as fetched-date ground truth.
>
> Primary sources:
> - https://raw.githubusercontent.com/lancedb/lance/main/python/python/lance/dataset.py — verbatim `LanceDataset` / `ScannerBuilder` / `LanceScanner` signatures and docstrings (`scanner`, `to_table`, `to_batches`, `take`, `take_blobs`, `read_blobs`, `count_rows`, `explain_plan`).
> - https://lance.org/guide/read_and_write/ — filter push-down SQL predicate expression language (operators, IN, IS NULL, LIKE, regexp_match, CAST, nested-field syntax, date/timestamp literals).
> - https://pypi.org/project/pylance/ — current released `pylance` version (8.0.0, uploaded 2026-07-01), `requires_python >=3.9`.

Scope: how to read data out of a Lance dataset — building a `Scanner` with column projection, SQL predicate filters, limit/offset, row-id/row-address materialization, and batch streaming; random access via `take()` and `count_rows()`; how projection/predicate pushdown and scalar indices reduce I/O; and how the scanner maps onto the PyArrow dataset protocol for zero-copy hand-off to DuckDB.

## Version ground truth (2026-07-08)

| Package | Current version | Notes |
|---|---|---|
| `pylance` (PyPI) | **8.0.0** (uploaded 2026-07-01) | `requires_python >=3.9`. Import name is `lance`; the PyPI distribution is `pylance`. |

Prior recent releases on PyPI: `7.0.0` (2026-05-27), `6.0.1`, `6.0.0`, `4.0.1`, `4.0.0`, `3.0.1`, `3.0.0`. Version numbers below are stated where a source confirms a behavior; where a feature's introducing version could not be confirmed from the fetched sources it is flagged under "Unverified / needs confirmation."

Signatures in this file are copied verbatim from `python/python/lance/dataset.py` on the `main` branch as fetched 2026-07-08. `main` may be slightly ahead of the `8.0.0` release; treat method-level details as `main`-as-of-fetch and pin your own version before relying on the newest parameters (`index_segments`, `read_blobs`, `order_by` string form, `disable_scoring_autoprojection`, batch vector search).

---

## 1. The Scanner

A **Scanner** (`LanceScanner`) is a lazily-evaluated query plan over a `LanceDataset`. Nothing reads from storage until you pull results (`to_table()`, `to_batches()`, `to_reader()`, `count_rows()`). `LanceScanner` subclasses `pyarrow.dataset.Scanner`, so it is a drop-in for any consumer that speaks the PyArrow dataset protocol (notably DuckDB — see [10_duckdb_arrow_interop.md](10_duckdb_arrow_interop.md)).

There are two ways to build one:

1. **`ds.scanner(...)`** — a single call that takes every option as a keyword argument and returns a `LanceScanner`. This is the common path.
2. **`ScannerBuilder(ds)`** — a fluent builder; each method returns `self` for chaining, and `.to_scanner()` finalizes. `ds.scanner(...)` is a thin wrapper that constructs a `ScannerBuilder`, applies the passed options, and calls `.to_scanner()`.

The convenience readers `ds.to_table(...)` and `ds.to_batches(...)` accept the same option set and forward to `scanner(...)` internally.

### 1.1 `LanceDataset.scanner(...)` — verbatim signature

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

Everything after the bare `*` is **keyword-only** (`prefilter`, `with_row_id`, `with_row_address`, `use_stats`, `fast_search`, `io_buffer_size`, `late_materialization`, `blob_handling`, `use_scalar_index`, `include_deleted_rows`, `scan_stats_callback`, `strict_batch_size`, `order_by`, `disable_scoring_autoprojection`). The parameters before it may be passed positionally, but keyword form is strongly preferred.

### 1.2 Parameter table (`scanner`)

Types and defaults are copied from the signature; the "Meaning" column paraphrases the verbatim docstring.

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `columns` | `List[str]` or `Dict[str, str]` | `None` | Projection. A list selects columns by name. A dict maps output column name → SQL expression (computed/renamed columns). `None` fetches all columns. Drives **projection pushdown** — see §3. |
| `filter` | `str`, `pa.compute.Expression`, `FullTextQuery`, `VectorSearchQuery`, or `dict` | `None` | Predicate. A `str` or `pa.compute.Expression` is an "expression filter" (a valid SQL WHERE clause — see §2). `FullTextQuery` / `VectorSearchQuery` are search filters. A `dict` combines both via keys `expr_filter` and `search_filter`. Drives **predicate pushdown** — see §3. |
| `limit` | `int` | `None` | Fetch up to this many rows. All rows if `None`. Must be non-negative. |
| `offset` | `int` | `None` | Start at this row (0 if `None`). Must be non-negative. |
| `nearest` | `dict` | `None` | Vector KNN search config: `{"column", "q", "k", "minimum_nprobes", "maximum_nprobes", "refine_factor", "distance_range", ...}`. Covered in [06_vector_search.md](06_vector_search.md). `q` may be a 2-D batch of query vectors (batch KNN, adds an Int32 `query_index` column). |
| `batch_size` | `int` | `None` | Max rows per output batch. Batches may be smaller. Can be overridden by `batch_size_bytes` or a dataset-level `FileReaderOptions` default. See §5. |
| `batch_size_bytes` | `int` | `None` | Target output batch size in **bytes**; when set, overrides row-based `batch_size`. Scanner-level setting takes precedence over the dataset-level `FileReaderOptions` default. |
| `batch_readahead` | `int` | `None` | Number of batches to read ahead. **Ignored when reading v2 files.** |
| `fragment_readahead` | `int` | `None` | Number of fragments to read ahead. |
| `scan_in_order` | `bool` | `True` | If `False`, fragments may be read concurrently and batches returned out of order — higher throughput, more memory. **Ignored for v2 files** (v2 always scans in order at no penalty). |
| `fragments` | `Iterable[LanceFragment]` | `None` | Restrict the scan to these fragments. With `scan_in_order=True` they are scanned in the given order. |
| `index_segments` | `Iterable[str \| uuid.UUID]` | `None` | Restrict vector index search to these index-segment UUIDs. **Vector search only.** If `fragments` is also set, rows in those fragments not covered by the selected segments are searched with flat KNN. |
| `full_text_query` | `str`, `dict`, or `FullTextQuery` | `None` | BM25 full-text search. A `str` matches documents containing any token; a `dict` takes `columns: list[str]` (currently a single column) and `query: str`. See [05_scalar_indices.md](05_scalar_indices.md) (INVERTED/FTS index). |
| `prefilter` | `bool` | `False` | If `True`, apply `filter` **before** the vector query (more correct, more costly; good for highly selective filters). If `False`, apply **after** the vector query (may return fewer than `k` rows). Only relevant when a vector/search query is present. See §3.3. |
| `with_row_id` | `bool` | `False` | Include the stable `_rowid` column. Row IDs survive modification/compaction. |
| `with_row_address` | `bool` | `False` | Include the `_rowaddr` column: `(fragment_id << 32) | row_offset_in_fragment`. **Unstable** — changes on modify/compact. Row IDs are generally preferred. |
| `use_stats` | `bool` | `True` | Use column statistics for query planning. Disable only for debugging/benchmarking. |
| `fast_search` | `bool` | `False` | If `True`, vector search, FTS, and scalar-indexed filters only search **indexed** fragments — faster, but skips recently appended unindexed data. |
| `io_buffer_size` | `int` | `None` | RAM reserved for holding I/O before processing. If the buffer fills, the scan blocks until drained. Should scale with concurrent I/O threads. **v2 only.** If unset, v2 chooses a default per object store; `LANCE_DEFAULT_IO_BUFFER_SIZE` overrides. Not a hard cap on total scanner RAM. |
| `late_materialization` | `bool` or `List[str]` | `None` | Controls late materialization (fetch non-query columns via a `take` after the filter). `True` = all columns late; `False` = all early; list = only those columns late. Default heuristic assumes filters select ~0.1% of rows. See §3.4. |
| `blob_handling` | `Literal["all_binary", "blobs_descriptions", "all_descriptions"]` | `None` | How blob columns are returned. `"all_binary"` = read blobs as binary/large_binary; `"blobs_descriptions"` = as descriptions (**effective default**); `"all_descriptions"` = all binary columns as descriptions. |
| `use_scalar_index` | `bool` | `True` | Whether scalar indices may be used to optimize filters. Disable only in corner cases where an index makes performance worse. See §3.2. |
| `include_deleted_rows` | `bool` | `False` | Return rows deleted-but-still-present in the fragment; their `_rowid` is set to null, other columns reflect on-disk values. **Not allowed for search or take (incl. scalar-indexed) operations.** |
| `scan_stats_callback` | `Callable[[ScanStatistics], None]` | `None` | Called with `ScanStatistics` after the scan completes. Callback errors are logged, not re-raised. |
| `strict_batch_size` | `bool` | `False` | If `True`, every batch except the last has exactly `batch_size` rows (requires merging small batches — small copy/perf cost). |
| `order_by` | `List[ColumnOrdering \| str]` | `None` | Output ordering. A bare `str` means ascending, nulls last. If unset, order follows file order when `scan_in_order` is true, else arbitrary. |
| `disable_scoring_autoprojection` | `bool` | `False` | Opt into future behavior where `_distance`/`_score` are only appended if unprojected or explicitly requested (today they are always appended after a search). |

> **Deprecation note.** `batch_readahead` and the `scan_in_order=False` fast path apply only to the legacy **v2** file reader — both are explicitly documented as "ignored when reading v2 files" / "ignored when using v2 files." New datasets use the current file format where in-order scanning carries no penalty; do not tune these for new data. See [01_file_format.md](01_file_format.md).

### 1.3 `ScannerBuilder` — fluent equivalents

`ScannerBuilder(ds)` exposes one method per option; each returns `self`. Verbatim method signatures (from `main`):

```python
ScannerBuilder(ds: LanceDataset)                                    # __init__

.apply_defaults(default_opts: Dict[str, Any]) -> ScannerBuilder
.batch_size(batch_size: int) -> ScannerBuilder
.batch_size_bytes(batch_size_bytes: int) -> ScannerBuilder
.io_buffer_size(io_buffer_size: int) -> ScannerBuilder
.batch_readahead(nbatches: Optional[int] = None) -> ScannerBuilder   # v2 only; must be >= 0
.fragment_readahead(nfragments: Optional[int] = None) -> ScannerBuilder  # must be >= 0
.scan_in_order(scan_in_order: bool = True) -> ScannerBuilder
.limit(n: Optional[int] = None) -> ScannerBuilder                    # must be >= 0
.offset(n: Optional[int] = None) -> ScannerBuilder                   # must be >= 0
.columns(cols: Optional[Union[List[str], Dict[str, str]]] = None) -> ScannerBuilder
.filter(filter: Union[str, pa.compute.Expression, FullTextQuery, VectorSearchQuery, dict]) -> ScannerBuilder
.prefilter(prefilter: bool) -> ScannerBuilder
.with_row_id(with_row_id: bool = True) -> ScannerBuilder
.with_row_address(with_row_address: bool = True) -> ScannerBuilder
.late_materialization(late_materialization: bool | List[str]) -> ScannerBuilder
.blob_handling(blob_handling: Optional[str]) -> ScannerBuilder
.use_stats(use_stats: bool = True) -> ScannerBuilder
.use_scalar_index(use_scalar_index: bool = True) -> ScannerBuilder
.with_fragments(fragments: Optional[Iterable[LanceFragment]]) -> ScannerBuilder
.with_index_segments(index_segments: Optional[Iterable[Union[str, uuid.UUID]]]) -> ScannerBuilder
.nearest(column, q, k=None, metric=None, nprobes=None, minimum_nprobes=None,
         maximum_nprobes=None, refine_factor=None, use_index=True, ef=None,
         query_parallelism=None, approx_mode="normal", distance_range=None) -> ScannerBuilder
.fast_search(flag: bool) -> ScannerBuilder
.include_deleted_rows(flag: bool) -> ScannerBuilder
.full_text_search(query: str | FullTextQuery, columns: Optional[List[str]] = None) -> ScannerBuilder
.scan_stats_callback(callback: Callable[[ScanStatistics], None]) -> ScannerBuilder
.strict_batch_size(strict_batch_size: bool = False) -> ScannerBuilder
.order_by(orderings: Optional[list[ColumnOrdering]]) -> ScannerBuilder
.disable_scoring_autoprojection(disable: bool = True) -> ScannerBuilder
.substrait_aggregate(aggregate: bytes) -> ScannerBuilder
.to_scanner() -> LanceScanner
```

Notes verbatim from source:
- `.filter(...)` accepts a `str` (kept as-is), a `pa.compute.Expression` (serialized to **Substrait** via `pyarrow.substrait.serialize_expressions`; if `pyarrow < 14` lacks that, it falls back to `str(filter)`), a `FullTextQuery`, a `VectorSearchQuery`, or a `dict` with `expr_filter`/`search_filter` keys.
- `.full_text_search(...)` is documented **Experimental** — "may remove it after we support to do this within `filter` SQL-like expression." Requires an inverted index on the searched column.
- `.batch_readahead(...)` / `.scan_in_order(...)` docstrings state they are ignored for v2 files.

> **`full_text_search` is experimental.** Prefer passing full-text queries through the `filter` / `full_text_query` parameters of `scanner(...)` rather than the builder's experimental `.full_text_search()` if you need forward-compatibility.

---

## 2. Filter expression language (SQL predicate strings)

The `filter=` string is a **SQL WHERE clause** evaluated by Lance's DataFusion-backed engine. `pa.compute.Expression` objects are converted to **Substrait** and pushed down the same way (falling back to stringification on `pyarrow < 14`). Per `lance.org/guide/read_and_write` (filter push-down):

### 2.1 Supported operators

| Category | Operators / forms |
|---|---|
| Comparison | `=`, `<`, `<=`, `>`, `>=` (and `!=` / `<>` — standard SQL inequality) |
| Logical | `AND`, `OR`, `NOT` |
| Null checks | `IS NULL`, `IS NOT NULL` |
| Boolean checks | `IS TRUE`, `IS NOT TRUE`, `IS FALSE`, `IS NOT FALSE` |
| Membership | `IN` (e.g. `label IN [10, 20]`) |
| Pattern match | `LIKE`, `NOT LIKE`; `regexp_match(column, pattern)` for regex |
| Type cast | `CAST` |

> The fetched guide lists the operators above explicitly. `BETWEEN` and `!=`/`<>` are standard DataFusion SQL and work in practice, but the fetched page does not enumerate them by name — see "Unverified / needs confirmation."

### 2.2 Field references

- Plain columns are referenced by name: `category = 'geography'`.
- Columns with special characters or SQL keywords use **backticks**: `` `CUBE` = 10 AND `column name with space` IS NOT NULL ``.
- **Nested fields**: wrap each path segment in backticks individually — `` `nested with space`.`inner with space` < 2 ``. Struct fields can also be addressed with subscript notation using the field name, e.g. `note['email'] IS NOT NULL`; list fields use numeric indices.
- **Limitation:** field names that contain a period (`.`) are **not supported**.

### 2.3 Literals

String literals use single quotes. Typed literals use the type-prefixed form:

```sql
date_col      = date '2021-01-01'
timestamp_col = timestamp '2021-01-01 00:00:00'
decimal_col   = decimal(8,3) '1.000'
```

For timestamps, precision (digit count) selects the unit: `timestamp(0)`=seconds, `timestamp(3)`=milliseconds, `timestamp(6)`=microseconds (default), `timestamp(9)`=nanoseconds.

### 2.4 Examples

```python
# simple equality
ds.scanner(filter="category = 'geography'")

# range + null check
ds.scanner(filter="price >= 100 AND price < 500 AND sku IS NOT NULL")

# IN membership
ds.scanner(filter="label IN [10, 20, 30]")

# LIKE / regex
ds.scanner(filter="name LIKE 'Acme%'")
ds.scanner(filter="regexp_match(domain, '\\.gov$')")

# nested struct field + combined predicate
ds.scanner(
    filter="((label IN [10, 20]) AND (note['email'] IS NOT NULL)) OR NOT note['created']"
)

# typed literal
ds.scanner(filter="created_at >= timestamp '2026-01-01 00:00:00'")

# backticked keyword / spaced column
ds.scanner(filter="`CUBE` = 10 AND `column name with space` IS NOT NULL")
```

A `pa.compute.Expression` is equivalent and pushed down via Substrait:

```python
import pyarrow.compute as pc
ds.scanner(filter=(pc.field("price") >= 100) & (pc.field("sku").is_valid()))
```

---

## 3. Pushdown and index acceleration

### 3.1 Projection pushdown (`columns=`)

Lance is columnar: passing `columns=[...]` (or a dict of computed expressions) means **only those columns' pages are read from storage**. Unlisted columns are never fetched. This is the single largest I/O lever on wide datasets — a 5-column projection over a 200-column table reads ~2.5% of the column data. The docstring: pushing predicates and projection to storage "significantly reduces" scan I/O, and Lance additionally understands that heavy columns (e.g. `image`) are expensive and plans to avoid reading them until needed (see late materialization, §3.4).

Computed/renamed projections use the dict form:

```python
ds.to_table(columns={"upper_name": "upper(name)", "gross": "price * qty"})
```

### 3.2 Predicate pushdown & scalar indices

The `filter` predicate is pushed to the storage layer so rows are excluded before materialization. When a **scalar index** exists on a filtered column (`BTREE`, `BITMAP`, `LABEL_LIST`, `NGRAM`, `INVERTED/FTS` — see [05_scalar_indices.md](05_scalar_indices.md)), Lance uses it to resolve the predicate to a set of matching row addresses **without a full column scan**:

- `BTREE` accelerates equality and range predicates (`=`, `<`, `<=`, `>`, `>=`, `BETWEEN`, `IN`) on high-cardinality keys.
- `BITMAP` accelerates equality/membership on low-cardinality categorical columns.
- `use_scalar_index=True` (default) enables this; set `False` to force a full scan in the rare cases where the index is slower.
- `fast_search=True` restricts index-accelerated filters to **indexed fragments only** — recently appended, not-yet-indexed rows are skipped.

Whether an index is actually used for a given plan can be inspected with `scanner.explain_plan(verbose=True)` (§6).

### 3.3 Prefilter vs. postfilter (vector/search queries)

When a `nearest=`/`filter=` combination runs alongside a vector or FTS query, `prefilter` decides ordering:

- `prefilter=True` — filter first, then search the surviving rows. Correct top-`k`, higher cost. Best when the filter is highly selective (and ideally backed by a scalar index so the prefilter itself is cheap).
- `prefilter=False` (default) — search first, then filter the results; may yield fewer than `k` rows. Best when the filter matches a large fraction of rows.

### 3.4 Late materialization

Late materialization fetches non-predicate columns via a `take` **after** the filter has selected rows, instead of reading them for every scanned row. Controlled by `late_materialization` (`True`/`False`/list). The default heuristic assumes the filter keeps ~0.1% of rows:

- Very selective filter (e.g. find-by-id) → set `True` (or rely on default) to avoid reading wide columns for discarded rows.
- Non-selective filter (e.g. matches ~20%) → set `False`; the take overhead outweighs the savings.

---

## 4. Random access & counting

### 4.1 `take(indices, columns)` — verbatim signature

```python
def take(
    self,
    indices: Union[List[int], pa.Array],
    columns: Optional[Union[List[str], Dict[str, str]]] = None,
) -> pa.Table:
    """Select rows of data by index."""
```

- `indices` — positional offsets of rows to select (a Python list or a `pa.Array`).
- `columns` — same projection semantics as `scanner` (list of names, or dict of computed SQL expressions). `None` = all columns.
- Returns a `pyarrow.Table`.

`take` is the primitive for gather-style random access — resolve a filter to row indices (or get them from a scalar-index lookup / `with_row_id` scan), then fetch full rows for just those indices.

```python
tbl = ds.take([0, 5, 42, 1000], columns=["id", "name", "vector"])
```

> **`LanceScanner.take(indices)` is NOT implemented** — it raises `NotImplementedError("take")`. `take` lives on the **dataset** (`LanceDataset.take`), not the scanner.

Related dataset methods (verbatim signatures):

```python
def take_blobs(
    self,
    blob_column: str,
    ids: Optional[Union[List[int], pa.Array]] = None,
    addresses: Optional[Union[List[int], pa.Array]] = None,
    indices: Optional[Union[List[int], pa.Array]] = None,
) -> List[BlobFile]:
    # Random access to blob columns as file-like BlobFile handles.
    # Exactly one of ids / addresses / indices must be specified.

def read_blobs(
    self,
    blob_column: str,
    ids: Optional[Union[List[int], pa.Array]] = None,
    addresses: Optional[Union[List[int], pa.Array]] = None,
    indices: Optional[Union[List[int], pa.Array]] = None,
    *,
    io_buffer_size: Optional[int] = None,
    preserve_order: Optional[bool] = None,
) -> List[Tuple[int, bytes]]:
    # Materialize blob bytes via the planned batched reader.
    # Returns [(row_address, blob_bytes), ...]. Exactly one selector required.

def sample(
    self,
    num_rows: int,
    columns: Optional[Union[List[str], Dict[str, str]]] = None,
    randomize_order: bool = True,
    **kwargs,
) -> pa.Table:
    # Random sample of num_rows rows (implemented as random.sample of indices + take).

def head(self, num_rows, **kwargs):
    # First N rows; equivalent to scanner(limit=num_rows, **kwargs).to_table().
```

`take_blobs` returns lazy `BlobFile` handles for random access; `read_blobs` eagerly materializes bytes (use it for training/preprocessing loaders that read each blob fully). Exactly one of `ids` / `addresses` / `indices` must be given.

### 4.2 `count_rows(filter)` — verbatim signature

```python
def count_rows(
    self, filter: Optional[Union[str, pa.compute.Expression]] = None, **kwargs
) -> int:
    """Count rows matching the scanner filter."""
```

- With no `filter`, returns the total row count (metadata-only — no data scan).
- With a `str` filter, delegates to the Rust `count_rows` fast path.
- With a `pa.compute.Expression` filter, it builds a scanner (`columns=[]`, `with_row_id=True`, `filter=...`) and counts — so it reads only what the predicate requires (index-accelerated where possible), not the full columns.

```python
total   = ds.count_rows()
matched = ds.count_rows("status = 'active' AND region = 'us-west'")
```

`LanceScanner.count_rows()` (no args) also exists and counts the rows a fully-built scanner would return.

---

## 5. Streaming readers & memory behavior

Three terminal readers on both `LanceDataset` and `LanceScanner`:

| Method | Returns | Memory |
|---|---|---|
| `to_table()` | `pa.Table` (fully materialized) | Holds the entire result set in RAM. Fine for filtered/projected slices; dangerous for full multi-hundred-million-row scans. |
| `to_batches()` | `Iterator[pa.RecordBatch]` | Streams — only `batch_size` rows (plus readahead/IO buffers) resident at a time. **Use this for out-of-core reads.** |
| `to_reader()` | `pa.RecordBatchReader` | Streaming reader; the PyArrow-native handle other engines consume. `to_table()` is `to_reader().read_all()`; `to_batches()` is `yield from to_reader()`. |

`LanceDataset.to_table(...)` / `LanceDataset.to_batches(...)` accept the full scanner option set and forward it (verbatim `to_batches` signature below).

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

**Batch-size controls:**
- `batch_size` caps rows per batch; actual batches may be smaller (unless `strict_batch_size=True`, which pads all but the last batch to exactly `batch_size` at a small copy cost).
- `batch_size_bytes` caps batch size in bytes and overrides `batch_size` — the right knob when row width varies (wide blob/vector rows).
- `io_buffer_size` bounds RAM reserved for in-flight I/O (v2 only); combined with `batch_size`/`batch_size_bytes` it lets you keep a hundred-million-row streaming scan inside a fixed memory envelope.

**Relationship to `pyarrow.dataset`:** `LanceScanner` subclasses `pyarrow.dataset.Scanner` and `LanceDataset` mirrors the PyArrow `Dataset` surface, so a Lance scanner/reader plugs directly into anything that consumes the PyArrow dataset/stream protocol — most importantly DuckDB's zero-copy Arrow ingestion (see [10_duckdb_arrow_interop.md](10_duckdb_arrow_interop.md)). The PyArrow-inherited `join`, `Scanner.take`, `Scanner.from_dataset/from_fragment/from_batches` are **not implemented** and raise `NotImplementedError`.

---

## 6. Inspecting the plan

`LanceScanner.explain_plan(verbose=False) -> str` returns the physical execution plan as text. Use it to confirm that projection pushdown, predicate pushdown, and scalar-index usage landed as intended before running a large scan.

```python
scanner = ds.scanner(columns=["id", "name"], filter="region = 'us-west'")
print(scanner.explain_plan(verbose=True))
```

`scan_stats_callback` (on `scanner(...)`) receives a `ScanStatistics` object after completion for I/O accounting.

---

## 7. Worked example — filtered, projected, streamed scan to Arrow

```python
import lance

ds = lance.dataset("s3://data-sink/active/entities_lance")

# Project 3 columns, push a selective predicate, stream in 64k-row batches.
scanner = ds.scanner(
    columns=["entity_id", "name", "score"],
    filter="region = 'us-west' AND score >= 0.75 AND name IS NOT NULL",
    batch_size=65_536,
    with_row_id=True,          # stable ids for a later take()/join
)

# Confirm pushdown + index use before pulling data.
print(scanner.explain_plan(verbose=True))

total = 0
for batch in scanner.to_batches():        # streaming: bounded memory
    total += batch.num_rows
    # hand each pa.RecordBatch to downstream compute...
print("rows:", total)

# Random access: gather full rows for specific offsets.
rows = ds.take([10, 250, 999_000], columns=["entity_id", "name", "score"])

# Metadata-only / index-accelerated count.
n_active = ds.count_rows("status = 'active'")
```

---

## 8. Common footguns

- **`to_table()` on a full unfiltered scan** materializes every row into RAM. At hundreds of millions of rows this OOMs — use `to_batches()`/`to_reader()`, or add `columns=`/`filter=`/`limit=`.
- **`LanceScanner.take(...)`, `Scanner.from_dataset/from_fragment/from_batches`, and `LanceDataset.join(...)` raise `NotImplementedError`.** Call `take` on the dataset; do joins in DuckDB/PyArrow.
- **`batch_readahead` and `scan_in_order=False` are no-ops on v2 files.** Don't tune them for current-format datasets; use `batch_size_bytes` / `io_buffer_size` for memory shaping instead.
- **Row addresses (`_rowaddr`) are unstable** — they change on modify/compaction. Persist `_rowid` (`with_row_id=True`) if you need identifiers that survive maintenance. See [08_compaction_maintenance.md](08_compaction_maintenance.md).
- **`prefilter=False` (default) can return fewer than `k` rows** on a filtered vector search. Set `prefilter=True` for exact top-`k` when the filter is selective.
- **Field names containing a period cannot be filtered**; backtick spaced/keyword names segment-by-segment.
- **`fast_search=True` silently skips unindexed fragments** — recently appended data won't appear until an index optimize/rebuild covers it. See [05_scalar_indices.md](05_scalar_indices.md).
- **`.full_text_search()` on `ScannerBuilder` is explicitly experimental** and may be removed; prefer `filter`/`full_text_query`.

---

> **Relevance to core-x:** For the DuckDB → Arrow → Lance-on-R2 plane, the load-bearing scan pattern is `ds.scanner(columns=[...], filter="<sql>", batch_size=...).to_batches()`: projection pushdown reads only the needed columns off R2, predicate pushdown against a `BTREE`/`BITMAP` scalar index on the resolution key resolves matching rows without a full column scan (keep `use_scalar_index=True`), and batch streaming holds the working set inside a fixed memory envelope for out-of-core reads. Because `LanceScanner` is a `pyarrow.dataset.Scanner`, the batches hand off zero-copy into DuckDB (`SELECT * FROM lance_scanner`), so the same filtered/projected scan feeds out-of-core DuckDB SQL that spills via `memory_limit`/`temp_directory`. Prefer `with_row_id=True` over `_rowaddr` for any id you persist across appends/compaction — fragments are append-only and immutable, but row addresses are not stable across maintenance. See [07_storage_object_stores.md](07_storage_object_stores.md) for R2 `storage_options` and [10_duckdb_arrow_interop.md](10_duckdb_arrow_interop.md) for the DuckDB bridge.

---

## Unverified / needs confirmation

- **`BETWEEN`, `!=`/`<>`:** standard DataFusion SQL and work in practice, but the fetched `lance.org/guide/read_and_write` page enumerates only `=`, `<`, `<=`, `>`, `>=`, `AND/OR/NOT`, `IN`, `IS [NOT] NULL`, `IS [NOT] TRUE/FALSE`, `LIKE/NOT LIKE`, `regexp_match`, and `CAST` by name. Treat `BETWEEN`/`!=` as expected-but-unlisted.
- **Introducing versions** for `index_segments`, `read_blobs`, `order_by` string form, batch vector search (2-D `q`/`query_index`), and `disable_scoring_autoprojection` were **not** confirmed from the fetched sources — these appear on `main` (ahead of PyPI `8.0.0`, 2026-07-01). Pin your installed `pylance` version and check its own `dataset.py` before depending on them.
- **`LanceDataset.take` and `count_rows` behavior with `_rowid` vs. positional indices:** `take` uses positional offsets; `_take_rows` (documented "Unstable API. Internal use only") uses row IDs. The public stable path for id-based gather is not fully specified in the fetched source beyond `_take_rows`.
- The upstream Python **API reference HTML** at `https://lancedb.github.io/lance/api/python.html` returned HTTP 404 on 2026-07-08; signatures here are taken from the source file (`python/python/lance/dataset.py`) instead, which is authoritative. The docs site appears to have moved (current guide content is served under `https://lance.org/`).
