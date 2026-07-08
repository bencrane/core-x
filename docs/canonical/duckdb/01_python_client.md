# DuckDB Python Client — connect, execute, relational API, replacement scans

> Canonical upstream reference — fetched 2026-07-08 from official DuckDB documentation and source repositories. Documents upstream behavior as-is; this is NOT core-x doctrine. Treat the version numbers and API signatures here as fetched-date ground truth.
>
> Primary sources:
> - https://duckdb.org/docs/current/clients/python/overview — connection model, `duckdb.sql()` vs `con.sql()` vs `con.execute()`, default global connection, thread-safety
> - https://duckdb.org/docs/current/clients/python/dbapi — DB-API `execute`/`executemany`, `?` and `$` parameter binding, `fetchone`/`fetchall`
> - https://duckdb.org/docs/current/clients/python/relational_api — `DuckDBPyRelation`, lazy evaluation, `from_*` constructors, transformation/materialization methods
> - https://duckdb.org/docs/current/clients/python/data_ingestion — replacement scans, `register`/`unregister`, `read_csv`/`read_parquet`/`read_json`
> - https://duckdb.org/docs/current/clients/python/conversion — result conversion to NumPy/pandas/Polars/Arrow, deprecated aliases
> - **Ground-truth signatures below were introspected directly from the installed `duckdb==1.5.4` C-extension** (`__doc__` on each pybind11 function), not reconstructed from prose. Where a signature is quoted, it is the verbatim pybind11 signature string emitted by the extension.

Scope: How to open a DuckDB connection from Python, execute parameterized SQL, use the lazy relational API (`DuckDBPyRelation`), pull results into pandas/Polars/NumPy/Arrow, and reference in-process Python dataframes directly in SQL via replacement scans — for out-of-core DuckDB → Arrow → Lance pipelines.

---

## 0. Version ground truth (as of 2026-07-08)

| Package | Current version | Notes |
|---|---|---|
| `duckdb` (Python) | **1.5.4** | The Python package version tracks the DuckDB engine version 1:1. All signatures in this file were introspected from this build. |
| `pylance` (Lance format Python lib) | **8.0.0** (2026-07-01) | The `import lance` package. Zero-copy Arrow bridge target for these pipelines — see `13_lance_interop.md`. |
| `lancedb` | **0.34.0** (2026-07-02) | Higher-level table/vector API over Lance. |
| `pyarrow` | Follow Arrow releases; DuckDB Arrow methods return `pyarrow.lib.Table` / `pyarrow.lib.RecordBatchReader`. |

DuckDB versioning: the Python client version **is** the engine version. There is no separate client SemVer line. See `00_overview.md` for release-line detail.

---

## 1. Connecting

### 1.1 `duckdb.connect(...)` — verbatim signature

```
connect(database: object = ':memory:', read_only: bool = False, config: dict = None) -> duckdb.DuckDBPyConnection
```

| Parameter | Type | Default | Accepted values / meaning |
|---|---|---|---|
| `database` | `object` (str/path) | `':memory:'` | File path → persistent DB (created if absent, unless `read_only=True`). `':memory:'` → private, non-persistent in-memory DB. `':memory:name'` → **named** in-memory DB shared across connections that pass the same name. `':default:'` → the shared global default connection (same one `duckdb.sql()` uses). |
| `read_only` | `bool` | `False` | `True` opens the file read-only. Required if **multiple OS processes** must open the same on-disk database file concurrently. A missing file is **not** created in read-only mode. |
| `config` | `dict` | `None` | Key→value map of DuckDB settings applied at connect time, e.g. `{'threads': 4, 'memory_limit': '8GB', 'storage_compatibility_version': 'latest'}`. See `06_configuration_memory_spill.md`. |

```python
import duckdb

con = duckdb.connect()                              # private in-memory
con = duckdb.connect("warehouse.duckdb")            # persistent file, read-write
con = duckdb.connect("warehouse.duckdb", read_only=True)   # multi-process safe read
con = duckdb.connect(config={"threads": 8, "memory_limit": "16GB"})
con = duckdb.connect(":memory:shared")              # named in-memory, shareable
```

`DuckDBPyConnection` is a context manager and should be closed when done:

```python
with duckdb.connect("warehouse.duckdb") as con:
    con.execute("CREATE TABLE t AS SELECT * FROM range(1000)")
# connection closed on block exit
```

### 1.2 The default (global) connection

`duckdb.sql(...)`, `duckdb.execute(...)`, `duckdb.read_parquet(...)` and the other **module-level** functions run against a single shared global in-memory connection, equivalent to `duckdb.connect(':default:')`.

> **Footgun — thread safety.** The global default connection is **not thread-safe**. Running queries on it from multiple threads causes data races. For concurrency, give each thread its own `con = duckdb.connect(...)`, or call `con.cursor()` to get an independent cursor over the same database (see §6.4). This matters directly for parallel ingest workers.

### 1.3 `duckdb.sql()` vs `con.sql()` vs `con.execute()`

Three distinct entry points with different return types and semantics:

| Call | Verbatim signature | Returns | Semantics |
|---|---|---|---|
| `duckdb.sql(query)` | `sql(query: object, *, alias: str = '', params: object = None, connection: DuckDBPyConnection = None) -> DuckDBPyRelation` | `DuckDBPyRelation` | **Lazy.** Builds a relation on the global connection. No execution until you fetch/materialize. |
| `con.sql(query)` | `sql(self, query: object, *, alias: str = '', params: object = None) -> DuckDBPyRelation` | `DuckDBPyRelation` | **Lazy.** Same, but on `con`. `con.query(...)` is an alias with the identical signature. |
| `con.execute(sql, params)` | `execute(self, query: object, parameters: object = None) -> DuckDBPyConnection` | `DuckDBPyConnection` (self) | **Eager.** Runs the statement immediately; returns the connection so you then call `.fetchall()`, `.df()`, `.arrow()`, etc. This is the DB-API path and the one that binds parameters. |

Key distinction: `sql()`/`query()` return a **relation** (a symbolic query plan) and accept read-only `params` for a single query; `execute()` runs a statement now, supports full DB-API parameter binding (including `executemany`), and returns the cursor to fetch from.

> **Footgun — `params` on `sql()`/`query()` is discouraged.** Upstream explicitly warns (relational-API docs → known issues, "Parameterized queries in relational API") that passing `params` to `sql()`, `query()`, or `from_query()` carries **significant performance overhead** vs. the DB-API path. For parameterized queries, prefer `con.execute(sql, params)`. If you need a lazy relation from a parameterized query on a hot path, run `execute()` then wrap the result, rather than binding params directly on `sql()`. (Verified against the 1.5.x relational-API page, 2026-07-08.)

```python
rel = con.sql("SELECT 42 AS answer")     # DuckDBPyRelation, nothing run yet
rows = con.execute("SELECT 42").fetchall()   # [(42,)] — ran immediately
```

---

## 2. Executing queries (DB-API)

### 2.1 `execute` / `executemany` — verbatim signatures

```
execute(self, query: object, parameters: object = None) -> DuckDBPyConnection
executemany(self, query: object, parameters: object = None) -> DuckDBPyConnection
```

Module-level equivalents on the global connection:

```
duckdb.execute(query: object, parameters: object = None, *, connection: DuckDBPyConnection = None) -> DuckDBPyConnection
duckdb.executemany(query: object, parameters: object = None, *, connection: DuckDBPyConnection = None) -> DuckDBPyConnection
```

Both return the connection/cursor for chaining a fetch call.

### 2.2 Parameterized queries

DuckDB supports two prepared-statement placeholder styles. **Always** use these instead of f-string interpolation — it is the SQL-injection-safe path and lets DuckDB cache the plan.

**Qmark (`?`) — positional.** Pass a list/tuple; bound left to right:

```python
con.execute("INSERT INTO items VALUES (?, ?, ?)", ["laptop", 2000, 1])
con.execute("SELECT * FROM items WHERE price > ? AND qty < ?", [100, 5]).fetchall()
```

**Dollar (`$1`, `$2` / `$name`) — numbered or named, reusable.** A numbered placeholder can appear multiple times and binds once:

```python
con.execute("SELECT $1, $1, $2", ["duck", "goose"]).fetchall()
# -> [('duck', 'duck', 'goose')]
```

Named placeholders bind from a `dict`:

```python
con.execute(
    "SELECT $my_param AS a, $other_param AS b",
    {"my_param": 5, "other_param": "DuckDB"},
).fetchall()
# -> [(5, 'DuckDB')]
```

> **Footgun.** Do not mix `?` and `$` styles in one statement. With qmark, the parameter object must be a sequence; with named `$`, it must be a dict.

### 2.3 `executemany` — batched prepared statement

Runs the same statement once per parameter set:

```python
con.executemany(
    "INSERT INTO items VALUES (?, ?, ?)",
    [["chainsaw", 500, 10], ["iphone", 300, 2]],
)
```

> **Footgun — do NOT use `executemany` for bulk load.** Upstream explicitly warns against `executemany` for inserting large amounts of data: it is a per-row round trip. For hundreds-of-millions-of-rows ingest, register the source as a relation/dataframe/Arrow and do a single set-based `INSERT INTO t SELECT * FROM source` / `CREATE TABLE t AS SELECT ...` / `COPY`. See §5 (replacement scans) and `02_arrow_integration.md`.

---

## 3. Fetching / materializing results

Call these on a `DuckDBPyConnection` after `execute()`, or directly on a `DuckDBPyRelation`. **Verbatim signatures (connection methods):**

| Method | Verbatim signature | Materializes? | Result |
|---|---|---|---|
| `fetchone` | `fetchone(self) -> Optional[tuple]` | Streams one row per call | Single row tuple, or `None` when exhausted |
| `fetchall` | `fetchall(self) -> list` | **Yes — all rows into memory** | `list[tuple]` |
| `fetchmany` | `fetchmany(self, size: SupportsInt \| SupportsIndex = 1) -> list` | Streams a batch | `list[tuple]` of up to `size` rows |
| `fetchnumpy` | `fetchnumpy(self) -> dict` | **Yes** | `dict[str, numpy.ndarray]` (one masked/ndarray per column) |
| `df` / `fetchdf` / `fetch_df` | `df(self, *, date_as_object: bool = False) -> pandas.DataFrame` | **Yes** | pandas `DataFrame`. `fetchdf`/`fetch_df` are exact aliases. |
| `fetch_df_chunk` | `fetch_df_chunk(self, vectors_per_chunk: SupportsInt \| SupportsIndex = 1, *, date_as_object: bool = False) -> pandas.DataFrame` | Streams a chunk | pandas `DataFrame` of `vectors_per_chunk * 2048` rows (DuckDB vector size is 2048). |
| `pl` | `pl(self, rows_per_batch: SupportsInt \| SupportsIndex = 1000000, *, lazy: bool = False) -> PolarsDataFrame` | **Yes** (eager) unless `lazy=True` | Polars `DataFrame` (or `LazyFrame` when `lazy=True`). |
| `fetch_arrow_table` | `fetch_arrow_table(self, rows_per_batch: SupportsInt \| SupportsIndex = 1000000) -> pyarrow.lib.Table` | **Yes** | Arrow `Table` (full result in memory). |
| `arrow` | `arrow(self, rows_per_batch: SupportsInt \| SupportsIndex = 1000000) -> pyarrow.lib.RecordBatchReader` | **Streaming** | Arrow `RecordBatchReader` — pull batches lazily. |
| `fetch_record_batch` | `fetch_record_batch(self, rows_per_batch: SupportsInt \| SupportsIndex = 1000000) -> pyarrow.lib.RecordBatchReader` | **Streaming** | Same as `arrow`; the explicit reader name. |
| `torch` | `torch(self) -> dict` | **Yes** | `dict[str, torch.Tensor]`. |
| `tf` | `tf(self) -> dict` | **Yes** | `dict[str, tf.Tensor]` (TensorFlow). |

> **Note on `arrow()` vs `fetch_arrow_table()`.** On a **connection/cursor** (1.5.4), `con.arrow(...)` returns a **`RecordBatchReader`** (streaming), while `con.fetch_arrow_table(...)` returns a materialized **`Table`**. This differs from the relational API, where `rel.arrow(...)` also returns a reader and `rel.to_arrow_table(...)` returns a Table (§4.3). Do not assume `arrow()` gives you a Table.

```python
con.execute("SELECT * FROM range(5) t(i)")
con.fetchall()          # [(0,), (1,), (2,), (3,), (4,)]  — all rows
con.execute("SELECT * FROM range(5) t(i)").df()      # pandas DataFrame
con.execute("SELECT * FROM range(5) t(i)").pl()      # Polars DataFrame
tbl = con.execute("SELECT * FROM range(5) t(i)").fetch_arrow_table()  # Arrow Table
```

`description` (DB-API property) exposes column names after execution.

> **Relevance to core-x:** for the DuckDB → Arrow → Lance path at hundreds-of-millions-of-rows scale, **never** call `fetchall()`/`df()`/`fetch_arrow_table()` on a full result — they materialize every row in RAM. Use the streaming `RecordBatchReader` from `con.arrow(...)` / `rel.to_arrow_reader(...)` and hand it straight to `lance.write_dataset(reader, uri, storage_options=...)`. Combined with a bounded `memory_limit` and a `temp_directory` for spill (`06_configuration_memory_spill.md`), this keeps the whole pipeline out-of-core with zero-copy Arrow batches. See `02_arrow_integration.md` and `13_lance_interop.md`.

---

## 4. The Relational API (`DuckDBPyRelation`)

A `DuckDBPyRelation` is a **symbolic representation of a SQL query** — a lazy query plan. Building and chaining relations runs **nothing**; execution is triggered only by a materialization/fetch call (`.show()`, `.fetchall()`, `.to_df()`, `.to_arrow_table()`, `.to_table()`, iterating a reader, etc.).

### 4.1 Constructors (`from_*` and friends)

Available as both module-level functions (global connection) and `con.` methods. Verbatim connection-method signatures (1.5.4):

| Constructor | Verbatim signature |
|---|---|
| `from_df` | `from_df(self, df: pandas.DataFrame) -> DuckDBPyRelation` |
| `from_arrow` | `from_arrow(self, arrow_object: object) -> DuckDBPyRelation` |
| `from_parquet` | `from_parquet(self, path_or_buffer: object, binary_as_string: bool = False, *, file_row_number: bool = False, filename: bool = False, hive_partitioning: bool = False, union_by_name: bool = False, compression: ... ) -> DuckDBPyRelation` |
| `from_csv_auto` | `from_csv_auto(self, path_or_buffer: object, **kwargs) -> DuckDBPyRelation` |
| `from_query` / `sql` / `query` | `from_query(self, query: object, *, alias: str = '', params: object = None) -> DuckDBPyRelation` |
| `read_csv` | `read_csv(self, path_or_buffer: object, **kwargs) -> DuckDBPyRelation` |
| `read_parquet` | `read_parquet(self, path_or_buffer: object, binary_as_string: bool = False, *, file_row_number: bool = False, filename: bool = False, hive_partitioning: bool = False, union_by_name: bool = False, compression: ...) -> DuckDBPyRelation` |
| `read_json` | `read_json(self, path_or_buffer: object, *, columns=None, sample_size=None, maximum_depth=None, records: Optional[str] = None, ...) -> DuckDBPyRelation` |
| `table` | `table(self, table_name: str) -> DuckDBPyRelation` — relation over an existing catalog table |
| `view` | `view(self, view_name: str) -> DuckDBPyRelation` — relation over an existing view |
| `values` | `values(self, *args) -> DuckDBPyRelation` — inline literal rows |

`from_arrow`, `from_df`, `read_parquet`, `read_csv`, `read_json` are the workhorses. Module-level variants add a keyword-only `connection: DuckDBPyConnection = None`, e.g. `duckdb.from_arrow(arrow_object: object, *, connection=None) -> DuckDBPyRelation`.

```python
rel = con.from_parquet("s3://bucket/data/*.parquet")   # lazy scan, glob + pushdown
rel = con.read_csv("landing/*.csv")
rel = con.from_arrow(my_arrow_table)
rel = con.sql("SELECT * FROM range(1_000_000_000)")     # lazy — nothing scanned yet
```

### 4.2 Transformation methods (lazy, chainable)

Each returns a new `DuckDBPyRelation`; nothing executes. Verbatim signatures (1.5.4):

| Method | Verbatim signature |
|---|---|
| `filter` | `filter(self, filter_expr: object) -> DuckDBPyRelation` |
| `project` / `select` | `project(self, *args, groups: str = '') -> DuckDBPyRelation` (`select` is identical) |
| `aggregate` | `aggregate(self, aggr_expr: object, group_expr: str = '') -> DuckDBPyRelation` |
| `order` | `order(self, order_expr: str) -> DuckDBPyRelation` |
| `sort` | `sort(self, *args) -> DuckDBPyRelation` |
| `limit` | `limit(self, n: SupportsInt \| SupportsIndex, offset: SupportsInt \| SupportsIndex = 0) -> DuckDBPyRelation` |
| `join` | `join(self, other_rel: DuckDBPyRelation, condition: object, how: str = 'inner') -> DuckDBPyRelation` (`how`: `'inner'`, `'left'`, `'right'`, `'outer'`, `'semi'`, `'anti'`) |
| `union` | `union(self, union_rel: DuckDBPyRelation) -> DuckDBPyRelation` |
| `intersect` | `intersect(self, other_rel: DuckDBPyRelation) -> DuckDBPyRelation` |
| `except_` | `except_(self, other_rel: DuckDBPyRelation) -> DuckDBPyRelation` |
| `cross` | `cross(self, other_rel: DuckDBPyRelation) -> DuckDBPyRelation` |
| `distinct` | `distinct(self) -> DuckDBPyRelation` |
| `count` | `count(self, expression: str, groups: str = '', window_spec: str = '', projected_columns: str = '') -> DuckDBPyRelation` |
| `apply` | `apply(self, function_name: str, function_aggr: str, group_expr: str = '', function_parameter: str = '', projected_columns: str = '') -> DuckDBPyRelation` |

```python
result = (
    con.sql("SELECT * FROM range(1_000_000_000) t(value)")
       .filter("value > 5")
       .project("value, value * 2 AS doubled")
       .order("value DESC")
       .limit(100)
)
# still lazy — no compute yet
```

### 4.3 Materialization / inspection methods

Triggering methods (execute the plan). Verbatim signatures (1.5.4):

| Method | Verbatim signature | Result |
|---|---|---|
| `show` | `show(self, *, max_width=None, max_rows=None, max_col_width=None, null_value=None, ...) -> None` | Pretty-prints a preview |
| `to_table` | `to_table(self, table_name: str) -> None` | Materializes into a catalog table |
| `to_df` / `df` | `to_df(self, *, date_as_object: bool = False) -> pandas.DataFrame` | pandas `DataFrame` |
| `to_arrow_table` | `to_arrow_table(self, batch_size: SupportsInt \| SupportsIndex = 1000000) -> pyarrow.lib.Table` | Arrow `Table` (materialized) |
| `arrow` | `arrow(self, batch_size: SupportsInt \| SupportsIndex = 1000000) -> pyarrow.lib.RecordBatchReader` | Arrow **reader** (streaming) |
| `to_arrow_reader` | `to_arrow_reader(self, batch_size: SupportsInt \| SupportsIndex = 1000000) -> pyarrow.lib.RecordBatchReader` | Arrow **reader** (streaming) |
| `pl` | `pl(self, batch_size: SupportsInt \| SupportsIndex = 1000000, *, lazy: bool = False) -> PolarsDataFrame` | Polars frame |
| `fetchall` | `fetchall(self) -> list` | `list[tuple]` |
| `fetchone` | `fetchone(self) -> Optional[tuple]` | one row / `None` |
| `fetchmany` | `fetchmany(self, size: SupportsInt \| SupportsIndex = 1) -> list` | batch |
| `fetchnumpy` | `fetchnumpy(self) -> dict` | dict of ndarrays |

Non-materializing **properties** (schema inspection, no execution): `columns` (`list[str]`), `types` / `dtypes` (list of DuckDB types), `shape` (`(rows, cols)` — note: `shape` *does* count rows, so it executes). Example:

```python
rel = con.sql("SELECT * FROM range(10) t(i)")
rel.columns      # ['i']
rel.types        # [BIGINT]
```

> **Note.** `rel.arrow(...)` and `rel.to_arrow_reader(...)` both return a streaming `RecordBatchReader`; `rel.to_arrow_table(...)` returns a materialized `Table`. Prefer the reader for large out-of-core results.

---

## 5. Replacement scans — querying Python objects by name

DuckDB can query in-scope Python variables **as if they were tables**, by name, with no explicit registration. This is the "replacement scan" mechanism and it is the primary zero-copy on-ramp for dataframes.

Supported object types (upstream): pandas `DataFrame`, Polars `DataFrame`/`LazyFrame`, NumPy arrays, Apache Arrow `Table`s, Arrow `Dataset`s, Arrow `RecordBatchReader`s and scanners, and DuckDB relations.

```python
import duckdb, pandas as pd

test_df = pd.DataFrame({"i": [1, 2, 3, 4], "j": ["one", "two", "three", "four"]})
duckdb.sql("SELECT * FROM test_df WHERE i > 2").fetchall()
# -> [(3, 'three'), (4, 'four')]     # test_df resolved by variable name
```

Works for Arrow and Polars identically:

```python
import pyarrow as pa
arrow_tbl = pa.table({"i": [1, 2, 3]})
duckdb.sql("SELECT sum(i) FROM arrow_tbl").fetchone()   # (6,)
```

Disable replacement scans (e.g. to force explicit registration):

```sql
SET python_enable_replacements = false;
```

### 5.1 `register` / `unregister` — explicit registration

Use when the object is not a plain local variable (it lives in a dict, an attribute, another scope) or you want a stable name. Verbatim signatures (1.5.4):

```
register(self, view_name: str, python_object: object) -> DuckDBPyConnection
unregister(self, view_name: str) -> DuckDBPyConnection
```

Module-level: `duckdb.register(view_name: str, python_object: object, *, connection=None)`.

```python
con.register("df_view", my_dict["frame"])     # bind object to a SQL name
con.sql("SELECT * FROM df_view").fetchall()
con.unregister("df_view")                      # drop the binding
```

Registered names behave like views: DuckDB reads the live object at query time (zero-copy for Arrow). To snapshot into DuckDB storage instead:

```python
con.execute("CREATE TABLE t AS SELECT * FROM df_view")   # materialized copy
con.execute("INSERT INTO t SELECT * FROM df_view")       # append
```

> **Footgun.** Replacement scans resolve names from the calling Python frame's locals/globals. Inside library code, deep call stacks, or comprehensions the variable may not be visible — use explicit `register()` there. The name also collides with real catalog tables: a registered/replacement name is shadowed by an actual table of the same name.

---

## 6. Result conversion overview

DuckDB result → Python object mapping (Arrow specifics in `02_arrow_integration.md`):

| Target | Call | Materializes |
|---|---|---|
| Python tuples | `fetchall()` / `fetchone()` / `fetchmany(size)` | all / one / batch |
| NumPy | `fetchnumpy()` → `dict[str, ndarray]` | all |
| pandas | `df()` / `fetchdf()` / `fetch_df()`; streamed via `fetch_df_chunk(n)` | all / chunk |
| Polars | `pl(rows_per_batch=1_000_000, lazy=False)` | all (or lazy) |
| Arrow Table | `fetch_arrow_table()` (conn) / `to_arrow_table()` (relation) | all |
| Arrow reader (stream) | `arrow()` / `fetch_record_batch()` (conn), `arrow()` / `to_arrow_reader()` (relation) | streaming |

### 6.1 Type-inference precedence (Python → DuckDB)

When DuckDB infers a column type from Python values (e.g. via replacement scan of native Python lists), it widens to the most permissive compatible type. Integers are tried in order **BIGINT → INTEGER → UBIGINT → UINTEGER → DOUBLE**; floats **DOUBLE → FLOAT**; lists take the most permissive element type across all elements. Rely on explicit `CAST` / `TRY_CAST` for pipeline-critical typing rather than inference (see `12_sql_essentials.md`).

### 6.2 Deprecated / renamed aliases — footguns

- `fetch_df()` and `fetchdf()` are aliases of `df()`. All three exist in 1.5.4; `df()` is canonical.
- Upstream conversion docs recommend `to_arrow_reader()` over the older `arrow()` naming for streaming, and treat `fetch_arrow_table()` / `fetch_record_batch()` as the legacy spellings of `to_arrow_table()` / the reader. **All remain callable in 1.5.4** (confirmed by introspection) — but new code should prefer `to_arrow_table()` (materialize) and `to_arrow_reader()` (stream) on relations for clarity.
- `from_query` == `query` == `sql` (identical signature).
- `con.cursor()` returns an independent `DuckDBPyConnection` over the same database — the DB-API way to get a per-thread handle.

### 6.3 Example — parameterized query returning a streaming Arrow reader

The canonical core-x shape: bind params safely, stream Arrow batches, feed Lance without ever holding the full result in RAM.

```python
import duckdb
import lance

con = duckdb.connect(config={"threads": 8, "memory_limit": "8GB",
                             "temp_directory": "/mnt/spill"})

# Parameterized, injection-safe. Relation is lazy.
rel = con.sql(
    """
    SELECT resolution_key, payload, ingested_at
    FROM read_parquet('s3://landing/batch/*.parquet')
    WHERE ingested_at >= $since
    """,
    params={"since": "2026-07-01"},
)

# Stream Arrow RecordBatches (out-of-core) straight into Lance on R2.
reader = rel.to_arrow_reader(batch_size=131_072)   # pyarrow.RecordBatchReader
lance.write_dataset(
    reader,
    "s3://data-sink/active/resolved_lance",
    storage_options={
        "aws_endpoint": "https://<accountid>.r2.cloudflarestorage.com",
        "aws_access_key_id": "...",
        "aws_secret_access_key": "...",
        "region": "auto",
    },
)
```

> **Caveat on the example above.** Upstream **discourages** passing `params` directly to `sql()`/`query()`/`from_query()` because of significant performance overhead (relational-API docs → known issues). On a hot ingest path, prefer the DB-API form — `con.execute(query, {"since": "..."})` — and obtain the streaming reader from the cursor via `con.execute(...).fetch_record_batch(batch_size)` (or `con.arrow(batch_size)`), which returns the same `pyarrow.RecordBatchReader`. The `con.sql(..., params=...)` form shown is the concise/readable spelling but not the performance-optimal one for repeated large binds.
>
> **Relevance to core-x:** this is the load-bearing pattern for the data plane. Binding params safely + streaming Arrow keeps the query injection-safe and out-of-core; `to_arrow_reader()` / `fetch_record_batch()` yields a zero-copy `RecordBatchReader` that `lance.write_dataset` consumes batch-by-batch. Bounding `memory_limit` + setting `temp_directory` lets DuckDB spill hash tables/sorts to disk so a hundreds-of-millions-of-rows projection never OOMs. Lance fragments are append-only and immutable, so re-running writes new fragments rather than mutating; add `BTREE` scalar indices on resolution keys after write. See `06_configuration_memory_spill.md`, `07_httpfs_s3_r2.md`, `02_arrow_integration.md`, and `13_lance_interop.md`.

### 6.4 Concurrency recap

- The global default connection (`duckdb.sql`, `duckdb.execute`) is **not thread-safe**.
- Per-thread isolation: `con = duckdb.connect(...)` per thread, or `cur = con.cursor()` for an independent cursor on the same DB.
- Multi-**process** access to one on-disk file requires `read_only=True` on the file openers.

---

## 7. Unverified / needs confirmation

- **`from_parquet` / `read_parquet` / `read_json` full keyword list.** The introspected signature strings truncate the tail (`compression: ...`, additional `read_json` kwargs). The parameters shown (`binary_as_string`, `file_row_number`, `filename`, `hive_partitioning`, `union_by_name`, plus `compression`) are confirmed present in 1.5.4; the complete option set and defaults for these readers are documented in `04_parquet.md` and `05_json.md` — consult those for the exhaustive list rather than treating this file's truncated tail as complete.
- **`storage_compatibility_version` accepted values.** Confirmed as a valid `config` key (e.g. `'latest'`); the full enumeration of accepted version strings belongs in `00_overview.md` / `06_configuration_memory_spill.md` and was not re-derived here.
- **`join` `how` values.** `'inner'` is the confirmed default; the additional values listed (`left`/`right`/`outer`/`semi`/`anti`) are standard DuckDB join types but were not each re-confirmed against 1.5.4 introspection — verify against the relational API page if an exact accepted-set guarantee is needed.
