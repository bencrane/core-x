# Apache Arrow Integration — to_arrow_table/to_arrow_reader, from_arrow, register, ADBC

> Canonical upstream reference — fetched 2026-07-08 from official DuckDB documentation and source repositories. Documents upstream behavior as-is; this is NOT core-x doctrine. Treat the version numbers and API signatures here as fetched-date ground truth.
>
> Primary sources:
> - https://duckdb.org/docs/current/clients/python/reference/ — Python Client API reference; verbatim `DuckDBPyConnection` / `DuckDBPyRelation` method signatures (`to_arrow_table`, `to_arrow_reader`, `arrow`, `fetch_arrow_table`, `fetch_record_batch`, `fetch_arrow_reader`, `from_arrow`, `register`, `unregister`).
> - https://duckdb.org/docs/current/clients/python/conversion.html — DuckDB↔Arrow conversion table; deprecation of `fetch_arrow_table`/`fetch_record_batch`; recommended replacements.
> - https://duckdb.org/docs/current/guides/python/export_arrow.html — exporting query results to Arrow (`to_arrow_table`, `to_arrow_reader` streaming).
> - https://duckdb.org/docs/current/guides/python/import_arrow.html — importing Arrow into DuckDB via `CREATE TABLE AS` / `INSERT INTO`.
> - https://duckdb.org/docs/current/guides/python/sql_on_arrow — querying Arrow objects directly (replacement scans, accepted input types, pushdown).
> - https://duckdb.org/docs/current/clients/adbc.html — DuckDB ADBC driver (`adbc_driver_duckdb`, Arrow C Data Interface).
> - https://arrow.apache.org/blog/2021/12/03/arrow-duckdb/ — "DuckDB quacks Arrow": zero-copy integration design over the Arrow C Data Interface.

Scope: How DuckDB exchanges columnar data with Apache Arrow from the Python client — exporting results to Arrow Tables and streaming `RecordBatchReader`s, importing/querying Arrow objects (replacement scans, `from_arrow`, `register`), the zero-copy C Data Interface, the ADBC driver, type-mapping seams, and bounded-memory streaming for out-of-core pipelines.

---

## Version ground truth (fetched 2026-07-08)

| Component | Current version | Notes |
|---|---|---|
| DuckDB | **1.5.4 "Variegata"** (stable line); **1.4.5 "Andium" LTS** | Since 1.4.0, every other release is an LTS with ~1 year community support. Andium (1.4.x) LTS end-of-life ~Sep 2026. |
| pylance (`pylance` on PyPI) | **8.0.0** (2026-07-01) | Python wrapper for the Lance columnar format; the `lance` import. Requires Python ≥ 3.9. |
| lancedb (`lancedb` on PyPI) | **0.34.0** (2026-07-02) | Higher-level DB layer; bundles pylance. Requires Python ≥ 3.10. |
| pyarrow | Track the version pinned by your `duckdb` / `lance` install | All Arrow interchange below is via pyarrow objects and the Arrow C Data Interface. |

The `to_arrow_reader` / `to_arrow_table` naming (replacing the deprecated `fetch_*` methods) is the current, recommended surface as of these versions. Both the new `to_arrow_*` methods and the deprecation of `fetch_arrow_table()` / `fetch_record_batch()` landed in **duckdb-python v1.5.0** (published 2026-03-09) — verified against the duckdb-python v1.5.0 release notes and the `_duckdb-stubs/__init__.pyi` type stub on `main`.

Sibling files: [`00_overview.md`](00_overview.md) (editions/versioning), [`01_python_client.md`](01_python_client.md) (connect/execute/relational API/replacement scans), [`06_configuration_memory_spill.md`](06_configuration_memory_spill.md) (`memory_limit`/`temp_directory`/out-of-core spill), [`07_httpfs_s3_r2.md`](07_httpfs_s3_r2.md) (object storage), [`12_sql_essentials.md`](12_sql_essentials.md) (types), [`13_lance_interop.md`](13_lance_interop.md) (DuckDB ↔ Lance).

---

## 1. Exporting query results to Arrow

Every DuckDB result object — the connection after `.execute()`/`.sql()`, and any `DuckDBPyRelation` — exposes the same Arrow export surface. Two shapes: a **materialized** `pyarrow.Table` (whole result in memory) and a **streaming** `pyarrow.RecordBatchReader` (bounded memory, pull one batch at a time).

### 1.1 Method signatures (verbatim from the Python Client API reference)

Signatures below are copied verbatim from the DuckDB Python Client API reference. The reference renders type links in brackets; those are the real annotated types (`SupportsInt`, `_duckdb.DuckDBPyConnection`, `pyarrow.lib.Table`, `pyarrow.lib.RecordBatchReader`).

```python
# --- Materialized Arrow Table ---
to_arrow_table(self: _duckdb.DuckDBPyConnection, batch_size: SupportsInt = 1000000) -> pyarrow.lib.Table

# --- Streaming RecordBatchReader ---
to_arrow_reader(self: _duckdb.DuckDBPyConnection, batch_size: SupportsInt = 1000000) -> pyarrow.lib.RecordBatchReader

# --- Alias of to_arrow_reader() (docs: "We recommend using to_arrow_reader() instead.") ---
arrow(self: _duckdb.DuckDBPyConnection, rows_per_batch: SupportsInt = 1000000) -> pyarrow.lib.RecordBatchReader

# --- DEPRECATED (see §1.3) ---
fetch_arrow_table(self: _duckdb.DuckDBPyConnection, rows_per_batch: SupportsInt = 1000000) -> pyarrow.lib.Table
fetch_record_batch(self: _duckdb.DuckDBPyConnection, rows_per_batch: SupportsInt = 1000000) -> pyarrow.lib.RecordBatchReader
# NOTE: fetch_arrow_reader is a DuckDBPyRelation-only method — it does NOT exist on DuckDBPyConnection
#       (per _duckdb-stubs/__init__.pyi, main). See the relation block in §1.1.
```

The identical export methods also exist on `DuckDBPyRelation`. Verified verbatim from the `_duckdb-stubs/__init__.pyi` type stub (`main`): the relation surface uses `batch_size` for `to_arrow_table` / `to_arrow_reader` / `arrow` / `fetch_arrow_table` / `fetch_arrow_reader`, and only the relation-level `fetch_record_batch` retains `rows_per_batch`. The parameter-name inconsistency is narrower than a naive reading suggests — see §1.2. Prefer passing positionally regardless.

```python
# On DuckDBPyRelation (relational API) — verbatim from _duckdb-stubs/__init__.pyi:
arrow(self, batch_size: SupportsInt = 1000000) -> pyarrow.lib.RecordBatchReader           # "Alias of to_arrow_reader()."
to_arrow_reader(self, batch_size: SupportsInt = 1000000) -> pyarrow.lib.RecordBatchReader
to_arrow_table(self, batch_size: SupportsInt = 1000000) -> pyarrow.lib.Table
fetch_arrow_reader(self, batch_size: SupportsInt = 1000000) -> pyarrow.lib.RecordBatchReader   # DEPRECATED
fetch_arrow_table(self, batch_size: SupportsInt = 1000000) -> pyarrow.lib.Table                # DEPRECATED
fetch_record_batch(self, rows_per_batch: SupportsInt = 1000000) -> pyarrow.lib.RecordBatchReader  # DEPRECATED
```

Note: `fetch_arrow_reader` exists on `DuckDBPyRelation` but **not** on `DuckDBPyConnection` (the connection surface has `fetch_arrow_table` / `fetch_record_batch` only). The connection-level `arrow` / `fetch_arrow_table` / `fetch_record_batch` use `rows_per_batch`; the connection-level `to_arrow_table` / `to_arrow_reader` use `batch_size` (see §1.1).

### 1.2 Parameter table

| Parameter | Type | Default | Where it appears (per `_duckdb-stubs/__init__.pyi`, `main`) |
|---|---|---|---|
| `batch_size` | `SupportsInt` | `1000000` | `to_arrow_table` / `to_arrow_reader` on **both** connection and relation; relation-level `arrow` / `fetch_arrow_table` / `fetch_arrow_reader`. |
| `rows_per_batch` | `SupportsInt` | `1000000` | Connection-level `arrow` / `fetch_arrow_table` / `fetch_record_batch`; relation-level `fetch_record_batch`. |

There is no `chunk_size` parameter anywhere on this surface — the stub uses only `batch_size` and `rows_per_batch`, both meaning rows-per-batch and both defaulting to 1,000,000.

> Footgun: two keyword names (`batch_size`, `rows_per_batch`) mean the same thing but are split unevenly across methods — notably `arrow()` uses `rows_per_batch` on the connection but `batch_size` on the relation. Passing the argument **positionally** (`.to_arrow_reader(1_000_000)`) is portable across all of them and immune to the naming drift.

### 1.3 Deprecation status — `fetch_arrow_table`, `fetch_record_batch`, `fetch_arrow_reader`

The conversion reference states explicitly:

> "`fetch_arrow_table()` and `fetch_record_batch()` are deprecated. Use `to_arrow_table()` and `to_arrow_reader()` instead."

The export guide adds `fetch_arrow_reader` to the deprecated set. Mapping:

| Deprecated | Replacement |
|---|---|
| `fetch_arrow_table(...)` | `to_arrow_table(batch_size=...)` |
| `fetch_record_batch(...)` | `to_arrow_reader(batch_size=...)` |
| `fetch_arrow_reader(...)` | `to_arrow_reader(batch_size=...)` |
| `arrow(...)` (alias) | `to_arrow_reader(batch_size=...)` — docs: "We recommend using `to_arrow_reader()` instead." |

The deprecated methods still function (downstream tools such as dlt hit the deprecation warning via `fetch_record_batch`), but new code MUST use `to_arrow_table` / `to_arrow_reader`. The deprecation landed in **duckdb-python v1.5.0** (2026-03-09) — the same release that introduced `to_arrow_table()` / `to_arrow_reader()` (verified against the v1.5.0 release notes: "Deprecated `fetch_arrow_table()` and `fetch_record_batch()` on connections and relations. Use the new `to_arrow_table()` and `to_arrow_reader()` methods instead.").

> Known upstream issue (context, not doctrine): duckdb/duckdb #14789 reports a memory-leak pattern with `fetch_arrow_reader()` → `read_next_batch()`. Another reason to standardize on `to_arrow_reader`.

### 1.4 Export examples

Materialized table:

```python
import duckdb
import pyarrow as pa

my_arrow_table = pa.Table.from_pydict({
    'i': [1, 2, 3, 4],
    'j': ["one", "two", "three", "four"],
})

# Whole result materialized into a pyarrow.Table
tbl = duckdb.sql("SELECT * FROM my_arrow_table").to_arrow_table()
```

Streaming reader (bounded memory — pull one batch at a time):

```python
import duckdb

batch_size = 1_000_000
reader = duckdb.sql("SELECT * FROM big_source").to_arrow_reader(batch_size)

# reader is a pyarrow.RecordBatchReader
while (batch := reader.read_next_batch()):   # raises StopIteration when exhausted
    handle(batch)   # batch is a pyarrow.RecordBatch of <= batch_size rows
```

`RecordBatchReader` is a standard pyarrow object: iterate with `read_next_batch()` (raises `StopIteration` at end), consume as a `for batch in reader:` loop, or hand the whole reader to any Arrow-consuming sink in one shot (see §5).

Relational API (same methods on a relation):

```python
con = duckdb.connect()
rel = con.table("integers")
rel_table  = rel.to_arrow_table()      # pyarrow.Table
rel_reader = rel.to_arrow_reader()     # pyarrow.RecordBatchReader
```

---

## 2. Importing / querying Arrow inside DuckDB

There are three ways to get an Arrow object into a DuckDB query. All are **zero-copy** where the underlying Arrow buffers permit (see §3).

### 2.1 Replacement scans (query a Python variable by name)

Any in-scope Python variable holding a supported Arrow object can be referenced in SQL **by its variable name** — no registration required. This is DuckDB's *replacement scan* mechanism (also covered in [`01_python_client.md`](01_python_client.md)).

```python
import duckdb
import pyarrow as pa

con = duckdb.connect()
my_arrow_table = pa.Table.from_pydict({
    'i': [1, 2, 3, 4],
    'j': ["one", "two", "three", "four"],
})

# 'my_arrow_table' resolves to the local variable — no register() call needed
results = con.execute("SELECT * FROM my_arrow_table WHERE i = 2").to_arrow_table()
```

### 2.2 `from_arrow` (build a relation from an Arrow object)

```python
from_arrow(self: _duckdb.DuckDBPyConnection, arrow_object: object) -> _duckdb.DuckDBPyRelation
```

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `arrow_object` | `object` | — (required) | A PyArrow Arrow object (Table, RecordBatch, RecordBatchReader, or `pyarrow.dataset` object). Returns a `DuckDBPyRelation` you can chain further relational ops onto or run SQL against. |

```python
rel = con.from_arrow(my_arrow_table)     # -> DuckDBPyRelation
rel.filter("i = 2").to_arrow_table()
```

### 2.3 `register` / `unregister` (name an Arrow object as a view)

```python
register(self: _duckdb.DuckDBPyConnection, view_name: str, python_object: object) -> _duckdb.DuckDBPyConnection
unregister(self: _duckdb.DuckDBPyConnection, view_name: str) -> _duckdb.DuckDBPyConnection
```

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `view_name` | `str` | — (required) | Name the object is exposed under as a temporary view. |
| `python_object` | `object` | — (required) | The Arrow (or pandas/Polars/etc.) object to bind to that name. |

```python
con.register("arrow_v", my_arrow_table)
con.execute("SELECT count(*) FROM arrow_v").fetchone()
con.unregister("arrow_v")
```

Use `register` when you want a stable, explicit name (independent of the local variable name), e.g. inside a function where the replacement-scan variable is not in the caller's scope.

### 2.4 Importing (materializing) Arrow into a DuckDB table

To copy Arrow into a persistent/native DuckDB table, run standard SQL against the Arrow object:

```python
# Create a new native table from an Arrow object
duckdb.sql("CREATE TABLE my_table AS SELECT * FROM my_arrow")

# Append into an existing table
duckdb.sql("INSERT INTO my_table SELECT * FROM my_arrow")
```

`CREATE TABLE AS` / `INSERT INTO` accept any query, so any of the reference methods above (replacement scan, `from_arrow`, `register`) feed them.

### 2.5 Accepted Arrow input types

Per the SQL-on-Arrow guide, DuckDB can query these Arrow object types directly (replacement scan or `from_arrow`):

| Arrow input | Behavior |
|---|---|
| `pyarrow.Table` | Queried as a regular table. Fully re-scannable. |
| `pyarrow.RecordBatch` | Accepted via `from_arrow`. |
| `pyarrow.RecordBatchReader` | Arrow streaming binary format; queryable directly as a table. **One-shot / single-pass** — a reader is a stream; once consumed it cannot be re-scanned. Do not reference it in a query that scans it more than once. |
| `pyarrow.dataset.Dataset` | Queried as a regular table. **DuckDB pushes column selection and row filters down into the dataset scan** so only necessary data is pulled into memory (see §6). |
| `pyarrow.dataset.Scanner` | Queried as a regular table; pushdown occurs through Arrow's compute layer rather than DuckDB's native operators. |

> Footgun: a `RecordBatchReader` is single-pass. Self-joins, `UNION`-with-rescan, or any plan that reads the same reader twice will fail or silently under-read. If a query must scan the input more than once, materialize to a `pyarrow.Table` first (`from_arrow(reader.read_all())`) or land it as a DuckDB table.

---

## 3. Zero-copy semantics & the Arrow C Data Interface

DuckDB and Arrow share the same fundamental columnar memory model, so interchange goes over the **Arrow C Data Interface** — a stable ABI for passing Arrow arrays/schemas between libraries in the same process by pointer, **without serializing or copying the buffers**. This is the mechanism behind "DuckDB quacks Arrow" (Apache Arrow blog, 2021-12-03).

Consequences that matter for pipelines:

- **Import** (Arrow → DuckDB): DuckDB scans the Arrow object's buffers in place. No deserialization step; predicate/projection pushdown means it may not even touch most columns/rows (see §6).
- **Export** (DuckDB → Arrow): `to_arrow_table` / `to_arrow_reader` hand out Arrow buffers that downstream Arrow consumers (Lance, Polars, pandas-via-Arrow, another DuckDB) read directly.
- **Ownership caveat**: "zero-copy" means the buffers are shared, not that lifetimes are free. Arrow objects backed by DuckDB-produced buffers must outlive their consumers; a streaming `RecordBatchReader` owns its batches only until the next `read_next_batch()`. Copy a batch (`pa.RecordBatch`/`pa.Table` deep-materialize) if you need to retain it past the next pull.
- Zero-copy is best-effort at the type level: types that map 1:1 stay zero-copy; types that require re-encoding (see §4) are converted, not aliased.

---

## 4. Arrow type-mapping notes & unsupported-type seams

DuckDB and Arrow share most primitives (integers, floats, `VARCHAR`↔`utf8`/`large_utf8`, `BLOB`↔`binary`, temporal types, `DECIMAL`, and nested `LIST`/`STRUCT`/`MAP`). For the DuckDB SQL type surface (STRUCT/LIST/MAP/VARIANT, etc.) see [`12_sql_essentials.md`](12_sql_essentials.md).

Practical seams to watch on export/import:

- **String width**: DuckDB `VARCHAR` maps to Arrow `utf8` or `large_utf8` depending on offsets needed; downstream sinks that only accept one variant may need a cast.
- **VARIANT / GEOMETRY**: DuckDB 1.5.0 added a native `VARIANT` type and a built-in `GEOMETRY` type. Round-tripping these through Arrow is newer surface — validate that your pyarrow/consumer version understands the produced extension/type before relying on it. See **Unverified / needs confirmation**.
- **Nested + dictionary encoding**: deeply nested `LIST<STRUCT<...>>` and dictionary-encoded columns are supported but are the most likely to force a re-encode (breaking strict zero-copy) or to surface consumer-side limitations.
- **Unsupported-type export**: the fetched pages do not enumerate an explicit list of Arrow types DuckDB refuses to export/import. When a type cannot be mapped, the conversion errors at query time rather than silently corrupting — treat any conversion error on a novel/extension type as an unsupported-type seam and cast explicitly (`CAST(col AS VARCHAR)` etc.) before crossing the boundary. The exact refusal list is **not documented on the fetched pages** — see **Unverified / needs confirmation**.

---

## 5. Streaming large results with bounded memory (`to_arrow_reader`)

For results too large to hold in RAM, `to_arrow_reader` is the primary tool: it produces batches lazily as they are pulled, so peak memory is roughly one batch (`batch_size` rows) plus DuckDB's own operator state.

```python
import duckdb

con = duckdb.connect(config={"memory_limit": "8GB", "temp_directory": "/mnt/fast/duckdb_tmp"})

reader = con.execute("""
    SELECT id, payload, ts
    FROM read_parquet('s3://bucket/huge/*.parquet')
    WHERE ts >= DATE '2026-01-01'
""").to_arrow_reader(batch_size=250_000)

for batch in reader:                 # pyarrow.RecordBatch, <= 250k rows each
    sink_write(batch)                # stream out; never materialize the full result
```

Sizing:
- **Larger `batch_size`** → fewer, bigger batches → better per-batch throughput, more peak memory.
- **Smaller `batch_size`** → lower peak memory, more per-batch overhead.
- The reader's bounded footprint is orthogonal to DuckDB's own out-of-core execution (hash joins, sorts, aggregations spilling to `temp_directory` under `memory_limit`). Configure both together — see [`06_configuration_memory_spill.md`](06_configuration_memory_spill.md).

---

## 6. Example — streaming reader out, Arrow table in for a second SQL pass

Pull a large result as a bounded stream, and separately feed a small Arrow table back into DuckDB as a queryable input for a follow-up pass:

```python
import duckdb
import pyarrow as pa

con = duckdb.connect()

# (1) Stream a large result OUT with bounded memory
reader = con.execute("SELECT * FROM read_parquet('s3://bucket/events/*.parquet')") \
            .to_arrow_reader(batch_size=500_000)

# (2) Consume the stream (e.g. accumulate a small dimension table as Arrow)
first = reader.read_next_batch()
dim_table = pa.Table.from_batches([first])   # small, in-memory Arrow Table

# (3) Feed that Arrow Table back IN for a second SQL pass (replacement scan)
result = con.execute("""
    SELECT category, count(*) AS n
    FROM dim_table            -- Arrow object referenced by variable name
    GROUP BY category
    ORDER BY n DESC
""").to_arrow_table()

# result is a pyarrow.Table produced zero-copy from the second pass
```

The stream out (`to_arrow_reader`) and the table back in (replacement scan of `dim_table`) both ride the Arrow C Data Interface — no serialization between DuckDB and pyarrow in either direction.

---

## 7. ADBC driver (`adbc_driver_duckdb`)

ADBC (Arrow Database Connectivity) is an Arrow-native database API: results and ingests move as Arrow streams over the Arrow C Data Interface, so DuckDB↔application transfer is zero-copy. Use it when a tool speaks ADBC generically, or when you want a standard DB-API surface that returns Arrow directly.

Install:

```bash
pip install adbc_driver_manager pyarrow
```

Connect + query (returns Arrow):

```python
import adbc_driver_duckdb.dbapi

with adbc_driver_duckdb.dbapi.connect("test.db") as conn, conn.cursor() as cur:
    cur.execute("SELECT 42")
    tbl = cur.fetch_arrow_table()   # ADBC cursor -> pyarrow.Table
    print(tbl)
```

Ingest an Arrow object into a table:

```python
import adbc_driver_duckdb.dbapi
import pyarrow

data = pyarrow.record_batch(
    [[1, 2, 3, 4], ["a", "b", "c", "d"]],
    names=["ints", "strs"],
)

with adbc_driver_duckdb.dbapi.connect("test.db") as conn, conn.cursor() as cur:
    cur.adbc_ingest("AnswerToEverything", data)
```

Notes:
- The docs describe ADBC as using "Arrow to transfer data between the database system and the application," i.e. zero-copy over the C Data Interface — the same interchange substrate as the native Python client's `to_arrow_*` / `from_arrow`.
- `cur.fetch_arrow_table()` here is the **ADBC cursor** method (from the ADBC/DB-API layer), distinct from — and not deprecated by — the DuckDB native client's deprecation of `fetch_arrow_table` in §1.3. Don't conflate the two surfaces.
- For most single-process Python pipelines the native `duckdb` client (§1–§6) is simpler and equally zero-copy; reach for ADBC when integrating with an ADBC-standard ecosystem or a non-Python driver.

---

## 8. Common footguns (consolidated)

- **Parameter-name drift**: only two names exist — `batch_size` and `rows_per_batch` (there is no `chunk_size`), split unevenly across methods (e.g. `arrow()` uses `rows_per_batch` on the connection but `batch_size` on the relation). Both mean rows-per-batch; pass positionally.
- **Deprecated `fetch_*`**: `fetch_arrow_table` / `fetch_record_batch` / `fetch_arrow_reader` are deprecated; use `to_arrow_table` / `to_arrow_reader`. A known memory-leak issue (#14789) affects `fetch_arrow_reader` → `read_next_batch()`.
- **`RecordBatchReader` is single-pass**: never reference a reader in a query that scans it twice; materialize first.
- **Reader batch lifetime**: a batch is valid only until the next `read_next_batch()`. Copy it if you must retain it.
- **`to_arrow_table` materializes fully**: for hundreds-of-millions of rows it will OOM. Use `to_arrow_reader` for anything that does not comfortably fit in RAM.
- **Type seams**: VARIANT/GEOMETRY (1.5.0+), dictionary-encoded, and deeply nested columns are the most likely to force re-encode or hit consumer limits; cast explicitly at the boundary if a conversion errors.

---

## Relevance to core-x

> **Relevance to core-x:** The DuckDB → Arrow → Lance-on-R2 write path rides the Arrow C Data Interface end to end — DuckDB's `to_arrow_reader(batch_size=...)` yields a `pyarrow.RecordBatchReader` whose batches are the zero-copy input to `lance.write_dataset(...)`, so no serialization step sits between compute and storage. For the hundreds-of-millions-of-rows scale that defines this plane, **`to_arrow_reader` (never `to_arrow_table`) is the mandatory export path**: it caps peak memory at ~one batch, letting DuckDB stream an out-of-core result (spilling hash joins/sorts to `temp_directory` under `memory_limit`, per [`06_configuration_memory_spill.md`](06_configuration_memory_spill.md)) straight into Lance's append-only immutable fragments without ever materializing the full result. Pick `batch_size` to align with target Lance fragment sizing. On the read side, Lance/pyarrow objects come back **into** DuckDB via replacement scan or `from_arrow` — also zero-copy — for a second SQL pass. Standardize on `to_arrow_reader`/`from_arrow`; the deprecated `fetch_*` methods (one with a documented reader memory leak, #14789) must not appear in pipeline code. See [`13_lance_interop.md`](13_lance_interop.md) for the verified Lance read/write reality and [`07_httpfs_s3_r2.md`](07_httpfs_s3_r2.md) for R2 storage_options.

---

## Unverified / needs confirmation

- ~~Exact version that introduced / deprecated the Arrow methods~~ — **RESOLVED**: duckdb-python v1.5.0 (2026-03-09) both introduced `to_arrow_table` / `to_arrow_reader` and deprecated `fetch_arrow_table` / `fetch_record_batch` (per the v1.5.0 release notes and the `_duckdb-stubs/__init__.pyi` type stub on `main`).
- **`from_arrow` accepted-input enumeration**: the reference types `arrow_object` as bare `object`. The SQL-on-Arrow guide confirms Table, RecordBatch(Reader), Dataset, and Scanner are queryable; whether `from_arrow` itself accepts every one of those (vs. only Table/RecordBatch) is inferred, not spelled out on the fetched pages.
- **Explicit unsupported-Arrow-type list** for export/import (§4): not enumerated on the fetched pages. Behavior described (query-time error, cast to work around) is the general contract, not a documented type list.
- **VARIANT / GEOMETRY (DuckDB 1.5.0+) Arrow round-trip fidelity**: whether these map to Arrow extension types cleanly across pyarrow versions is not covered by the fetched Arrow-integration pages; validate empirically before relying on it in a pipeline.
- **pyarrow version compatibility matrix** with duckdb 1.5.x / 1.4.x LTS: not on the fetched pages; pin per your install.
