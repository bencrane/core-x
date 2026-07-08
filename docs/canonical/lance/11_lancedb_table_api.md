# LanceDB (the database) — connect, tables, add/search, FTS, cloud/remote

> Canonical upstream reference — fetched 2026-07-08 from official Lance documentation and source repositories. Documents upstream behavior as-is; this is NOT core-x doctrine. Treat the version numbers and API signatures here as fetched-date ground truth.
>
> Primary sources:
> - https://pypi.org/project/lancedb/ — current released `lancedb` package version and packaging metadata.
> - https://lancedb.github.io/lancedb/python/python/ — Python SDK API reference (function/method signatures).
> - https://github.com/lancedb/lancedb (source files `python/python/lancedb/__init__.py`, `db.py`, `table.py`, `query.py`, `merge.py`) — verbatim signatures for `connect`/`connect_async`, `DBConnection`, `Table`, query builder, merge-insert builder.
> - https://docs.lancedb.com/ , https://docs.lancedb.com/quickstart , https://docs.lancedb.com/tables , https://docs.lancedb.com/search/full-text-search , https://docs.lancedb.com/embedding/quickstart — narrative docs and runnable examples for connect / tables / search / FTS / embeddings.

Scope: the high-level `lancedb` Python package — connecting to a database (local dir / S3-R2 / LanceDB Cloud+Enterprise `db://`), sync vs async clients, table lifecycle (create/open/add/merge_insert/update/delete/count), vector + full-text + hybrid search with rerankers, DB-layer index creation, and the Pydantic/embedding-registry auto-embedding path.

---

## 0. What this package is, and how it relates to `pylance`

There are **two distinct Python packages** in the Lance ecosystem:

| Package | Import | Purpose |
|---|---|---|
| `pylance` | `import lance` | Low-level columnar dataset SDK — `lance.dataset(...)`, `lance.write_dataset(...)`, fragments, commits, indices. See [`02_python_dataset_api.md`](02_python_dataset_api.md). |
| `lancedb` | `import lancedb` | High-level **database** API — a connection object, named tables, `.search()` query builders, FTS/hybrid/reranking, an embedding-function registry, and a cloud/remote client. |

**A LanceDB table IS a Lance dataset on disk.** When you `db.create_table("foo", ...)` against a local or object-store URI, LanceDB writes a standard Lance dataset (the same `.lance` directory layout — `data/`, `_versions/`, `_indices/` — documented in [`01_file_format.md`](01_file_format.md)). You can open the very same table directly with `pylance` (`lance.dataset("<db_uri>/foo.lance")`) and vice-versa. `lancedb` adds a catalog/namespace layer, embedding automation, and the search query builders on top of that shared on-disk format.

> Relevance to core-x: core-x writes and reads Lance datasets with **low-level `pylance`** (`lance.write_dataset` → R2, BTREE scalar indices on resolution keys). This file exists for completeness — when a table-style API (named tables, `.search()`, FTS, auto-embedding) is genuinely wanted, `lancedb` is the layer, and because its tables are the same on-disk Lance format, either package can read what the other wrote.

---

## 1. Versions (as of 2026-07-08)

- **`lancedb` (the database package): `0.34.0`** — released **2026-07-02** on PyPI. Requires **Python >= 3.10**. Development status is still classified "3 - Alpha"; upstream ships stable releases roughly every 2 weeks, plus preview releases. Source: https://pypi.org/project/lancedb/.
- Optional install extras: `azure`, `clip`, `embeddings`, `pylance`, `siglip` (e.g. `pip install "lancedb[embeddings]"`).
- `lancedb` depends on Apache Arrow (`pyarrow`) and bundles a native Rust core (`lancedb`/`lance` via `pylance`).

```bash
pip install lancedb              # core
pip install "lancedb[embeddings]"  # + embedding-function registry deps
```

Sibling reference for the columnar file format / `pylance` version: [`00_overview.md`](00_overview.md), [`02_python_dataset_api.md`](02_python_dataset_api.md).

---

## 2. Connecting

Two entry points: **synchronous** `lancedb.connect(...)` and **asynchronous** `lancedb.connect_async(...)`. Both dispatch on the URI form:

- **local directory** — `lancedb.connect("./mydb")` or an absolute path.
- **object store** — `lancedb.connect("s3://bucket/prefix")` (also `gs://`, `az://`). Pass credentials/endpoints via `storage_options=`.
- **LanceDB Cloud / Enterprise** — a `db://<database>` URI **plus** `api_key=` (and, for Enterprise, `host_override=` / `region=`). If an `api_key` is present the client talks to the remote service; otherwise it opens a filesystem/object-store database.

### 2.1 `lancedb.connect` (sync) — verbatim signature

Source: `python/python/lancedb/__init__.py`.

```python
def connect(
    uri: Optional[URI] = None,
    *,
    api_key: Optional[str] = None,
    region: str = "us-east-1",
    host_override: Optional[str] = None,
    read_consistency_interval: Optional[timedelta] = None,
    request_thread_pool: Optional[Union[int, ThreadPoolExecutor]] = None,
    client_config: Union[ClientConfig, Dict[str, Any], None] = None,
    storage_options: Optional[Dict[str, str]] = None,
    session: Optional[Session] = None,
    manifest_enabled: bool = False,
    namespace_client_impl: Optional[str] = None,
    namespace_client_properties: Optional[Dict[str, str]] = None,
    namespace_client_pushdown_operations: Optional[List[str]] = None,
    **kwargs: Any,
) -> DBConnection: ...
```

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `uri` | `Optional[URI]` (str/Path) | `None` | DB location: local path, `s3://…`, `gs://…`, `az://…`, or `db://<database>` for Cloud/Enterprise. May be `None` only when a `namespace_client_impl` is supplied. |
| `api_key` | `Optional[str]` | `None` | Presence switches the client to LanceDB Cloud/Enterprise remote mode. Can also come from env `LANCEDB_API_KEY`. |
| `region` | `str` | `"us-east-1"` | Cloud region. |
| `host_override` | `Optional[str]` | `None` | Override endpoint host (LanceDB Enterprise / self-hosted). |
| `read_consistency_interval` | `Optional[timedelta]` | `None` | How often an opened table re-checks the store for new commits. `None` = never re-check within a handle (open a fresh handle to see new data); `timedelta(0)` = strong consistency (check every read); a positive value = eventual consistency at that interval. |
| `request_thread_pool` | `Optional[int | ThreadPoolExecutor]` | `None` | Thread pool for remote requests. |
| `client_config` | `ClientConfig | dict | None` | `None` | Remote HTTP client tuning (timeouts, retries). |
| `storage_options` | `Optional[Dict[str, str]]` | `None` | Object-store credentials/config (S3/R2/GCS/Azure). See §2.4 and [`07_storage_object_stores.md`](07_storage_object_stores.md). |
| `session` | `Optional[Session]` | `None` | Shared cache/session object across connections. |
| `manifest_enabled` | `bool` | `False` | Enable manifest-based table listing. |
| `namespace_client_impl` | `Optional[str]` | `None` | Connect via a namespace/catalog client implementation instead of a raw URI. |
| `namespace_client_properties` | `Optional[Dict[str,str]]` | `None` | Properties for the namespace client. |
| `namespace_client_pushdown_operations` | `Optional[List[str]]` | `None` | Operations pushed to the namespace client. |

Returns a `DBConnection` (concretely `LanceDBConnection` for local/object-store, a remote connection for `db://`).

### 2.2 `lancedb.connect_async` (async) — verbatim signature

Source: `python/python/lancedb/__init__.py`.

```python
async def connect_async(
    uri: URI,
    *,
    api_key: Optional[str] = None,
    region: str = "us-east-1",
    host_override: Optional[str] = None,
    read_consistency_interval: Optional[timedelta] = None,
    client_config: Optional[Union[ClientConfig, Dict[str, Any]]] = None,
    storage_options: Optional[Dict[str, str]] = None,
    session: Optional[Session] = None,
    manifest_enabled: bool = False,
    namespace_client_properties: Optional[Dict[str, str]] = None,
    oauth_config=None,
) -> AsyncConnection: ...
```

Returns an `AsyncConnection`. Every table/DB operation on the async path is a coroutine (`await ...`).

### 2.3 Sync vs async — which to use

- **Sync** (`connect` → `DBConnection` → `Table`) is the mainstream path; every example below is sync unless marked async.
- **Async** (`connect_async` → `AsyncConnection` → `AsyncTable`) mirrors the same surface but returns coroutines; use it inside `asyncio` event loops / high-concurrency services. The underlying Rust core is the same; upstream builds the sync API on top of the async core. Some newer parameters land on the async API first.
- Do **not** share a sync `DBConnection` across an event loop expecting non-blocking behavior — use `connect_async` for that.

```python
# sync
import lancedb
db = lancedb.connect("./mydb")

# async
import lancedb, asyncio
async def main():
    db = await lancedb.connect_async("./mydb")
    tbl = await db.create_table("t", data=[{"id": 1, "vector": [0.1, 0.2]}])
    print(await tbl.count_rows())
asyncio.run(main())
```

### 2.4 Cloud / Enterprise (`db://`)

```python
db = lancedb.connect(
    uri="db://your-database",
    api_key="sk_...",        # or env LANCEDB_API_KEY
    region="us-east-1",      # Cloud
    # host_override="https://your-enterprise-host",  # Enterprise
)
```

`db://` connections are remote — table data lives on the managed service, not your local disk. The table/search API surface is otherwise the same.

---

## 3. Table lifecycle

`DBConnection` (sync) and `LanceDBConnection` (the local/object-store concrete class) expose identical signatures; `LanceDBConnection` returns `LanceTable`, the base returns `Table`.

### 3.1 `create_table` — verbatim signature

Source: `python/python/lancedb/db.py`.

```python
def create_table(
    self,
    name: str,
    data: Optional[DATA] = None,
    schema: Optional[Union[pa.Schema, LanceModel]] = None,
    mode: str = "create",
    exist_ok: bool = False,
    on_bad_vectors: str = "error",
    fill_value: float = 0.0,
    embedding_functions: Optional[List[EmbeddingFunctionConfig]] = None,
    *,
    namespace_path: Optional[List[str]] = None,
    storage_options: Optional[Dict[str, str]] = None,
    data_storage_version: Optional[str] = None,
    enable_v2_manifest_paths: Optional[bool] = None,
) -> Table: ...
```

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `name` | `str` | — | Table name. |
| `data` | `Optional[DATA]` | `None` | Initial rows: `list[dict]`, `pandas.DataFrame`, `polars.DataFrame`, a `pyarrow.Table`/`RecordBatch`, or an Arrow `RecordBatchReader`. Provide `data` and/or `schema`. |
| `schema` | `pa.Schema \| LanceModel \| None` | `None` | Explicit Arrow schema or a Pydantic `LanceModel` subclass. Required if `data` is `None` (creates an empty table). |
| `mode` | `str` | `"create"` | `"create"` = error if the table exists; `"overwrite"` = drop-and-replace any existing table. (Case-insensitive in practice.) |
| `exist_ok` | `bool` | `False` | With `mode="create"`: if `True` and the table already exists, **open** it instead of raising (data is **not** appended). Idempotent-open pattern. |
| `on_bad_vectors` | `str` | `"error"` | How to handle malformed vectors: `"error"`, `"drop"`, `"fill"`, `"null"`. |
| `fill_value` | `float` | `0.0` | Value used when `on_bad_vectors="fill"`. |
| `embedding_functions` | `Optional[List[EmbeddingFunctionConfig]]` | `None` | Registered embedding configs (usually supplied implicitly via a `LanceModel` schema — see §6). |
| `namespace_path` | `Optional[List[str]]` | `None` | Namespace/catalog path (keyword-only). |
| `storage_options` | `Optional[Dict[str,str]]` | `None` | Per-table object-store config (keyword-only). |
| `data_storage_version` | `Optional[str]` | `None` | Lance data storage format version (e.g. `"stable"` / `"2.1"`). Keyword-only. |
| `enable_v2_manifest_paths` | `Optional[bool]` | `None` | Use v2 manifest path scheme. Keyword-only. |

> Footgun: `mode` and `exist_ok` are **different knobs**. `mode="overwrite"` destroys existing data; `mode="create", exist_ok=True` preserves it and just returns a handle. A bare `mode="create"` on an existing table raises.

> Footgun (data shape): in Python a **single bare `dict`** or single `LanceModel` instance is rejected — pass a **list** (`[{...}]`) or a batch-like object.

### 3.2 `open_table` — verbatim signature

```python
def open_table(
    self,
    name: str,
    *,
    namespace_path: Optional[List[str]] = None,
    storage_options: Optional[Dict[str, str]] = None,
    index_cache_size: Optional[int] = None,
    branch: Optional[str] = None,
    version: Optional[int] = None,
) -> Table: ...
```

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `name` | `str` | — | Existing table name. |
| `namespace_path` | `Optional[List[str]]` | `None` | Namespace path. |
| `storage_options` | `Optional[Dict[str,str]]` | `None` | Object-store config. |
| `index_cache_size` | `Optional[int]` | `None` | Number of index partitions to cache in memory. |
| `branch` | `Optional[str]` | `None` | Open a specific branch (time-travel / branching — see [`04_versioning_time_travel.md`](04_versioning_time_travel.md)). |
| `version` | `Optional[int]` | `None` | Open a specific dataset version (time travel). |

### 3.3 `table_names` and `drop_table`

```python
def table_names(
    self,
    page_token: Optional[str] = None,
    limit: int = 10,
    *,
    namespace_path: Optional[List[str]] = None,
) -> Iterable[str]: ...

# base DBConnection
def drop_table(self, name: str, namespace_path: Optional[List[str]] = None): ...
# LanceDBConnection adds ignore_missing
def drop_table(
    self, name: str, namespace_path: Optional[List[str]] = None, ignore_missing: bool = False
): ...
```

> Footgun: `table_names(limit=...)` defaults to **10** — paginate with `page_token` to list all tables in a large database.

### 3.4 `Table.add` — verbatim signature

Source: `python/python/lancedb/table.py`.

```python
def add(
    self,
    data: DATA,
    mode: AddMode = "append",
    on_bad_vectors: OnBadVectorsType = "error",
    fill_value: float = 0.0,
    progress: Optional[Union[bool, Callable, Any]] = None,
) -> AddResult: ...
```

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `data` | `DATA` | — | Rows to add (same accepted types as `create_table.data`). |
| `mode` | `AddMode` | `"append"` | `"append"` = add rows; `"overwrite"` = replace all existing table data. |
| `on_bad_vectors` | `OnBadVectorsType` | `"error"` | `"error"` / `"drop"` / `"fill"` / `"null"`. |
| `fill_value` | `float` | `0.0` | Used with `on_bad_vectors="fill"`. |
| `progress` | `bool \| Callable \| None` | `None` | Progress reporting. |

Appends are additive commits (new immutable fragments), consistent with the Lance write model in [`03_writes_appends_upserts.md`](03_writes_appends_upserts.md).

### 3.5 `count_rows`, `delete`, `update`

```python
def count_rows(self, filter: Optional[str] = None) -> int: ...

def delete(self, where: Union[str, Expr]) -> DeleteResult: ...

def update(
    self,
    where: Optional[str] = None,
    values: Optional[dict] = None,
    *,
    values_sql: Optional[Dict[str, str]] = None,
) -> UpdateResult: ...
```

- `count_rows(filter=)` — total rows, or rows matching a SQL predicate.
- `delete(where=)` — delete rows matching a SQL `WHERE` expression (string uses SQL quoting: `table.delete("role = 'Traitor'")`).
- `update` — `values=` sets columns to **literal** Python values; `values_sql=` sets columns from **SQL expressions** (e.g. `{"x": "x + 1"}`). Pass one or the other.

```python
table.update(where="id = 3", values={"role": "Retired"})
table.update(values_sql={"visits": "visits + 1"})   # bump every row
```

### 3.6 `merge_insert` (upsert) — verbatim signatures

`table.merge_insert(on)` returns a **`LanceMergeInsertBuilder`** you configure with `when_*` clauses and finish with `.execute(new_data)`.

Source: `python/python/lancedb/table.py` and `python/python/lancedb/merge.py`.

```python
def merge_insert(self, on: Union[str, Iterable[str]]) -> LanceMergeInsertBuilder: ...

class LanceMergeInsertBuilder:
    def when_matched_update_all(self, *, where: Optional[str] = None) -> "LanceMergeInsertBuilder": ...
    def when_not_matched_insert_all(self) -> "LanceMergeInsertBuilder": ...
    def when_not_matched_by_source_delete(
        self, condition: Union[str, Expr, None] = None
    ) -> "LanceMergeInsertBuilder": ...
    def execute(
        self,
        new_data: DATA,
        on_bad_vectors: str = "error",
        fill_value: float = 0.0,
        timeout: Optional[timedelta] = None,
    ) -> MergeInsertResult: ...
```

| Clause | Effect |
|---|---|
| `.when_matched_update_all(where=None)` | For join-key matches (optionally gated by `where`), update **all** columns from `new_data`. |
| `.when_not_matched_insert_all()` | Insert new rows whose key is absent from the table. |
| `.when_not_matched_by_source_delete(condition=None)` | Delete table rows whose key is absent from `new_data` (optionally only those matching `condition`). |
| `.execute(new_data, ...)` | Run the merge against `new_data`; returns a `MergeInsertResult`. |

**Standard upsert** = update-all + insert-all:

```python
(
    table.merge_insert("id")               # join/match on column "id"
    .when_matched_update_all()
    .when_not_matched_insert_all()
    .execute(new_rows)                     # list[dict] / DataFrame / Arrow
)
```

**Full sync (mirror `new_data` into the table)** adds the delete clause:

```python
(
    table.merge_insert("id")
    .when_matched_update_all()
    .when_not_matched_insert_all()
    .when_not_matched_by_source_delete()
    .execute(new_rows)
)
```

See [`03_writes_appends_upserts.md`](03_writes_appends_upserts.md) for the equivalent low-level `pylance` merge-insert semantics.

---

## 4. Search

`table.search(...)` returns a **query builder** you chain (`.limit`, `.where`, `.select`, `.metric`, `.nprobes`, `.rerank`, …) then materialize (`.to_arrow`, `.to_pandas`, `.to_polars`, `.to_list`, `.to_pydantic`). The builder type depends on the query kind (vector / FTS / hybrid).

### 4.1 `Table.search` — verbatim signature

Source: `python/python/lancedb/table.py`.

```python
def search(
    self,
    query: Optional[
        Union[VEC, str, "PIL.Image.Image", Tuple, FullTextQuery]
    ] = None,
    vector_column_name: Optional[str] = None,
    query_type: QueryType = "auto",
    ordering_field_name: Optional[str] = None,
    fts_columns: Optional[Union[str, List[str]]] = None,
) -> LanceQueryBuilder: ...
```

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `query` | `VEC \| str \| PIL.Image \| Tuple \| FullTextQuery \| None` | `None` | A vector (`list[float]`/np array) → vector search; a `str` → FTS if the column has an FTS index, else auto-embedded if an embedding function is configured; a `FullTextQuery`/`PhraseQuery` object → structured FTS; `None` → pure filter/scan (combine with `.where()`). |
| `vector_column_name` | `Optional[str]` | `None` | Which vector column to search (defaults to the single vector column / `"vector"`). |
| `query_type` | `QueryType` | `"auto"` | `"auto"`, `"vector"`, `"fts"`, or `"hybrid"`. `"auto"` infers from the `query` type. |
| `ordering_field_name` | `Optional[str]` | `None` | Field used for ordering (FTS). |
| `fts_columns` | `str \| List[str] \| None` | `None` | Column(s) to run FTS against. |

### 4.2 Query-builder chain methods — verbatim signatures

Source: `python/python/lancedb/query.py`.

```python
# shared
def limit(self, limit: Union[int, None]) -> Self: ...
def where(self, where: Union[str, Expr], prefilter: bool = True) -> Self: ...
def select(self, columns: Union[list[str], dict[str, Union[str, Expr]]]) -> Self: ...

# vector-only
def metric(self, metric: Literal["l2", "cosine", "dot"]) -> "LanceVectorQueryBuilder": ...
def nprobes(self, nprobes: int) -> "LanceVectorQueryBuilder": ...
def refine_factor(self, refine_factor: int) -> "LanceVectorQueryBuilder": ...

# reranking
def rerank(self, reranker: "Reranker") -> Self: ...

# materializers
def to_arrow(self, *, timeout: Optional[timedelta] = None) -> pa.Table: ...
def to_pandas(
    self,
    flatten: Optional[Union[int, bool]] = None,
    *,
    blob_mode: BlobMode = "lazy",
    timeout: Optional[timedelta] = None,
    **kwargs,
) -> "pd.DataFrame": ...
def to_list(self, *, timeout: Optional[timedelta] = None) -> List[dict]: ...
def to_polars(self, *, timeout: Optional[timedelta] = None) -> "pl.DataFrame": ...
def to_pydantic(self, model: type[T], *, timeout: Optional[timedelta] = None) -> list[T]: ...
```

| Method | Notes |
|---|---|
| `.limit(k)` | Top-k rows. For vector/ANN this is the number of neighbors returned. |
| `.where(expr, prefilter=True)` | SQL predicate. **`prefilter=True` (default)** filters *before* the vector search (correct top-k over the filtered set); `prefilter=False` post-filters the ANN candidates (faster, but can return fewer than `limit`). |
| `.select(cols)` | Project columns; a `dict` form allows computed/aliased SQL expressions. |
| `.metric("l2" \| "cosine" \| "dot")` | Distance metric for vector search (default `"l2"`). Must match the metric the index was built with for best results. |
| `.nprobes(n)` | IVF partitions probed — higher = better recall, slower. |
| `.refine_factor(n)` | Re-rank `n × limit` candidates with exact distances (recovers PQ recall). |
| `.rerank(reranker)` | Apply a reranker to results (see §4.5). |
| `.to_arrow / to_pandas / to_polars / to_list / to_pydantic` | Materialize. Vector/FTS results include a distance/score column (`_distance` for vector, relevance score for FTS). |

### 4.3 Vector search

```python
import lancedb
db = lancedb.connect("./mydb")
tbl = db.open_table("characters")

result = (
    tbl.search([0.2, 0.8, 0.4, 0.9])          # query vector
       .metric("cosine")
       .nprobes(20)
       .where("stats.magic >= 4", prefilter=True)
       .select(["name", "role", "_distance"])
       .limit(5)
       .to_pandas()
)
```

Vector index creation is at §5.1; ANN internals (IVF_PQ / HNSW, nprobes, refine) are covered in [`06_vector_search.md`](06_vector_search.md).

### 4.4 Full-text search (FTS, BM25)

FTS uses a BM25 inverted index (the native Lance `INVERTED` index; the legacy Tantivy backend is available via `use_tantivy=True`). Build the index, then search with a string.

```python
# 1. build the FTS index on a text column
tbl.create_fts_index("text")                 # native BM25 (use_tantivy defaults to False)

# 2. keyword search — a str query auto-routes to FTS when an FTS index exists
results = (
    tbl.search("puppy")
       .select(["text"])
       .limit(10)
       .to_list()
)

# be explicit if the column is also embedded:
results = tbl.search("puppy", query_type="fts").limit(10).to_list()
```

**Phrase queries** need positions in the index:

```python
from lancedb.query import PhraseQuery
tbl.create_fts_index("text", with_position=True, replace=True)
phrase = (
    tbl.search(PhraseQuery("puppy runs", "text"))
       .select(["id", "text"])
       .limit(100)
       .to_pandas()
)
```

> Freshness footgun: FTS (and vector) indices do **not** automatically cover rows added after the index was built. Call `table.optimize()` after `add()` to fold new fragments into the index, or those rows go through a slower unindexed path / may be missed by index-only reads. See [`08_compaction_maintenance.md`](08_compaction_maintenance.md).

### 4.5 Hybrid search + rerankers

`query_type="hybrid"` runs vector **and** FTS and fuses the two result sets. A reranker (default **Reciprocal Rank Fusion**) merges them.

```python
def rerank(  # LanceHybridQueryBuilder
    self,
    reranker: Reranker = RRFReranker(),
    normalize: str = "score",
) -> "LanceHybridQueryBuilder": ...
```

```python
from lancedb.rerankers import RRFReranker

results = (
    tbl.search("brave knight of the round table", query_type="hybrid")
       .rerank(RRFReranker())           # default fusion
       .limit(10)
       .to_pandas()
)
```

Built-in rerankers exported from `lancedb.rerankers` (`__all__` as of `lancedb` 0.34.0): `RRFReranker` (reciprocal rank fusion, the hybrid default), `MRRReranker`, `LinearCombinationReranker`, `CohereReranker`, `ColbertReranker`, `CrossEncoderReranker`, `JinaReranker`, `OpenaiReranker`, `VoyageAIReranker`, `AnswerdotaiRerankers` (plus the `Reranker` base class). Availability of any given reranker at runtime still depends on the required provider SDK/extra being installed. Rerankers also work on pure vector or pure FTS results via `.rerank(...)`.

- `normalize` — `"score"` or `"rank"`: whether the reranker fuses on normalized scores or on ranks.

---

## 5. Indexing at the DB layer

Three creators on `Table`. All build indices on the underlying Lance dataset (same index artifacts described in [`05_scalar_indices.md`](05_scalar_indices.md) and [`06_vector_search.md`](06_vector_search.md)).

### 5.1 `create_index` (vector ANN) — verbatim signature

Source: `python/python/lancedb/table.py`.

```python
def create_index(
    self,
    metric: str = "l2",
    num_partitions: Optional[int] = None,
    num_sub_vectors: Optional[int] = None,
    vector_column_name: str = VECTOR_COLUMN_NAME,
    replace: bool = True,
    accelerator: Optional[str] = None,
    index_cache_size: Optional[int] = None,
    num_bits: int = 8,
    index_type: Literal[
        "IVF_FLAT", "IVF_SQ", "IVF_PQ", "IVF_RQ",
        "IVF_HNSW_SQ", "IVF_HNSW_PQ", "IVF_HNSW_FLAT",
    ] = "IVF_PQ",
    max_iterations: int = 50,
    sample_rate: int = 256,
    m: int = 20,
    ef_construction: int = 300,
    *,
    config: Optional[IndexConfigType] = None,
    wait_timeout: Optional[timedelta] = None,
    name: Optional[str] = None,
    train: bool = True,
    target_partition_size: Optional[int] = None,
): ...
```

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `metric` | `str` | `"l2"` | `"l2"`, `"cosine"`, `"dot"`. |
| `num_partitions` | `Optional[int]` | `None` | IVF partitions (auto-chosen if `None`). |
| `num_sub_vectors` | `Optional[int]` | `None` | PQ sub-vectors (auto if `None`). |
| `vector_column_name` | `str` | `"vector"` (`VECTOR_COLUMN_NAME`) | Column to index. |
| `replace` | `bool` | `True` | Replace an existing index of the same name. |
| `accelerator` | `Optional[str]` | `None` | e.g. `"cuda"` for GPU-accelerated training. |
| `index_cache_size` | `Optional[int]` | `None` | Cached index partitions. |
| `num_bits` | `int` | `8` | PQ code bits. |
| `index_type` | `Literal[...]` | `"IVF_PQ"` | One of the 7 IVF / IVF+HNSW variants listed. |
| `max_iterations` | `int` | `50` | k-means training iterations. |
| `sample_rate` | `int` | `256` | Training sample rate. |
| `m` | `int` | `20` | HNSW graph degree (HNSW variants). |
| `ef_construction` | `int` | `300` | HNSW build-time search width. |
| `config` | `Optional[IndexConfigType]` | `None` | Structured index config (alternative to the flat kwargs). Keyword-only. |
| `wait_timeout` | `Optional[timedelta]` | `None` | Block until the (async/remote) index build completes. Keyword-only. |
| `name` | `Optional[str]` | `None` | Index name. Keyword-only. |
| `train` | `bool` | `True` | Train the index now. Keyword-only. |
| `target_partition_size` | `Optional[int]` | `None` | Target rows per partition. Keyword-only. |

```python
tbl.create_index(metric="cosine", index_type="IVF_PQ", num_partitions=256, num_sub_vectors=16)
```

### 5.2 `create_scalar_index` — verbatim signature

```python
def create_scalar_index(
    self,
    column: str,
    *,
    replace: bool = True,
    index_type: ScalarIndexType = "BTREE",
    wait_timeout: Optional[timedelta] = None,
    name: Optional[str] = None,
): ...
```

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `column` | `str` | — | Column to index. |
| `replace` | `bool` | `True` | Replace existing scalar index (keyword-only). |
| `index_type` | `ScalarIndexType` | `"BTREE"` | `"BTREE"`, `"BITMAP"`, `"LABEL_LIST"` (and others — see [`05_scalar_indices.md`](05_scalar_indices.md)). Keyword-only. |
| `wait_timeout` | `Optional[timedelta]` | `None` | Keyword-only. |
| `name` | `Optional[str]` | `None` | Keyword-only. |

```python
tbl.create_scalar_index("id", index_type="BTREE")     # accelerate equality/range filters on a resolution key
```

> Relevance to core-x: a `BTREE` scalar index on the load-bearing resolution key is exactly the pattern core-x hard-codes on Lance datasets. Via `lancedb` it is `table.create_scalar_index("<key>", index_type="BTREE")`; via `pylance` it is `dataset.create_scalar_index("<key>", "BTREE")` ([`05_scalar_indices.md`](05_scalar_indices.md)). Same index artifact either way, since the table is the same on-disk Lance dataset.

### 5.3 `create_fts_index` — verbatim signature

```python
def create_fts_index(
    self,
    field_names: Union[str, List[str]],
    *,
    ordering_field_names: Optional[Union[str, List[str]]] = None,
    replace: bool = False,
    writer_heap_size: Optional[int] = 1024 * 1024 * 1024,
    use_tantivy: bool = False,
    tokenizer_name: Optional[str] = None,
    with_position: bool = False,
    base_tokenizer: BaseTokenizerType = "simple",
    language: str = "English",
    max_token_length: Optional[int] = 40,
    lower_case: bool = True,
    stem: bool = True,
    remove_stop_words: bool = True,
    ascii_folding: bool = True,
    ngram_min_length: int = 3,
    ngram_max_length: int = 3,
    prefix_only: bool = False,
    wait_timeout: Optional[timedelta] = None,
    name: Optional[str] = None,
): ...
```

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `field_names` | `str \| List[str]` | — | Text column(s) to index. |
| `ordering_field_names` | `str \| List[str] \| None` | `None` | Fields available for ordering FTS results. |
| `replace` | `bool` | `False` | Replace existing FTS index (note: **default `False`**, unlike scalar/vector). |
| `writer_heap_size` | `Optional[int]` | `1 GiB` | Index-writer heap. |
| `use_tantivy` | `bool` | `False` | `False` = native Lance BM25 `INVERTED` index (default); `True` = legacy Tantivy backend. |
| `with_position` | `bool` | `False` | Store token positions (needed for phrase queries). |
| `base_tokenizer` | `BaseTokenizerType` | `"simple"` | `"simple"`, `"raw"`, `"whitespace"`, `"ngram"`. |
| `language` | `str` | `"English"` | Stemmer/stop-word language. |
| `max_token_length` | `Optional[int]` | `40` | Drop longer tokens. |
| `lower_case` | `bool` | `True` | Lowercase tokens. |
| `stem` | `bool` | `True` | Apply stemming. |
| `remove_stop_words` | `bool` | `True` | Strip stop words. |
| `ascii_folding` | `bool` | `True` | Fold accents to ASCII. |
| `ngram_min_length` / `ngram_max_length` | `int` | `3` / `3` | N-gram bounds (ngram tokenizer). |
| `prefix_only` | `bool` | `False` | N-gram prefix-only mode. |
| `wait_timeout` | `Optional[timedelta]` | `None` | Keyword-only. |
| `name` | `Optional[str]` | `None` | Keyword-only. |

> Deprecation/footgun: the FTS backend **default flipped to the native Lance `INVERTED` index** (`use_tantivy=False`). The older Tantivy path (`use_tantivy=True`) is legacy and lacks incremental features; new code should use the default. Also note `replace` defaults to `False` here (rebuilding requires `replace=True`).

---

## 6. Auto-embedding: registry + Pydantic `LanceModel`

LanceDB can embed text/images for you at write and query time. Two pieces: a global **embedding-function registry** (`get_registry()`) and a Pydantic schema type **`LanceModel`** with `SourceField()` (the raw input to embed) and `VectorField()` (where the embedding lands).

```python
import lancedb
from lancedb.pydantic import LanceModel, Vector
from lancedb.embeddings import get_registry

# 1. pick an embedding function from the registry
func = (
    get_registry()
    .get("sentence-transformers")
    .create(name="BAAI/bge-small-en-v1.5", device="cpu")
)

# 2. declare a schema; the source text is embedded into `vector` automatically
class Words(LanceModel):
    text: str = func.SourceField()
    vector: Vector(func.ndims()) = func.VectorField()

db = lancedb.connect("./mydb")
table = db.create_table("words", schema=Words, mode="overwrite")

# 3. add ONLY the source text — the vector is computed on insert
table.add([{"text": "hello world"}, {"text": "goodbye world"}])

# 4. search with a raw string — the query is embedded with the same function
hit = table.search("greetings").limit(1).to_pydantic(Words)[0]
print(hit.text)
```

Key points:
- `get_registry().get("<provider>")` returns a registered embedding-function class; `.create(...)` instantiates it with model-specific kwargs. Providers include `sentence-transformers`, `openai`, `cohere`, `huggingface`, `bedrock`, `gemini-text`, `instructor`, `imagebind`, `open-clip`, and others (availability depends on installed extras / SDKs).
- `func.SourceField()` marks the column whose values get embedded; `func.VectorField()` marks the destination vector column; `Vector(func.ndims())` sets the fixed-size vector dimension from the model.
- Because the embedding config is captured in the `LanceModel`, `create_table(schema=Words)` wires it up — you don't pass `embedding_functions=` explicitly, and you don't compute vectors yourself on `add`/`search`.
- `Vector(dim)` maps to an Arrow `fixed_size_list<float32>[dim]`. `LanceModel.to_arrow_schema()` yields the equivalent `pyarrow.Schema` if you need it.

> core-x note: core-x computes/handles vectors and keys itself through `pylance` + DuckDB projections; the registry auto-embedding path is a convenience for table-style apps, not the core-x ingest pattern.

---

## 7. Cloud/object-store storage options (S3 / Cloudflare R2)

For S3-compatible stores (including **Cloudflare R2**), pass credentials/endpoint via `storage_options` on `connect(...)` (and/or per-table on `create_table`/`open_table`). LanceDB forwards these to the same object-store layer that `pylance` uses — full matrix in [`07_storage_object_stores.md`](07_storage_object_stores.md).

```python
import lancedb

db = lancedb.connect(
    "s3://my-bucket/lancedb",
    storage_options={
        "aws_access_key_id": "…",
        "aws_secret_access_key": "…",
        # Cloudflare R2: point at the R2 S3 endpoint and set a region
        "aws_endpoint": "https://<accountid>.r2.cloudflarestorage.com",
        "aws_region": "auto",
    },
)
tbl = db.create_table("t", data=[{"id": 1, "vector": [0.1, 0.2]}], mode="overwrite")
```

> Relevance to core-x: this is the identical R2 `storage_options` surface the core-x plane uses when addressing Lance datasets by R2 URI. Whether the write goes through `lancedb.connect("s3://…", storage_options=…)` or `lance.write_dataset(..., storage_options=…)`, it lands the same Lance dataset in R2. core-x standardizes on the `pylance` path; the keys here (`aws_endpoint`, `aws_region="auto"`, access-key pair) are the same ones documented in [`07_storage_object_stores.md`](07_storage_object_stores.md).

---

## 8. Common footguns (consolidated)

- **`mode` vs `exist_ok`.** `mode="overwrite"` destroys existing data; `mode="create", exist_ok=True` is the idempotent-open (keep data) pattern. A bare `mode="create"` on an existing table raises.
- **Single dict rejected.** Pass `data=[{...}]` (a list), not a bare `dict`, to `create_table`/`add`.
- **`table_names` default `limit=10`.** Paginate to list everything.
- **Index freshness.** New rows after index build are not indexed until `table.optimize()`. Affects both vector and FTS/hybrid recall.
- **`prefilter` default is `True`** for `.where()` on a search — correct top-k over the filtered set. `prefilter=False` post-filters ANN candidates and can under-fill `limit`.
- **`create_fts_index(replace=...)` defaults to `False`** (unlike `create_index`/`create_scalar_index`, which default `replace=True`). Rebuilding an FTS index requires `replace=True`.
- **FTS backend flip.** `use_tantivy` now defaults to `False` (native Lance `INVERTED` BM25). Tantivy is legacy.
- **Metric mismatch.** `.metric()` on a query should match the metric the vector index was built with, or recall degrades.
- **Read consistency.** By default an opened table handle does not see commits made after it was opened. Set `read_consistency_interval=timedelta(0)` for strong consistency, or re-open the table.
- **Sync/async don't mix.** Use `connect_async`/`AsyncTable` end-to-end inside asyncio; don't call blocking sync methods on the event loop.

---

## 9. Unverified / needs confirmation

- The **reranker class list** in `lancedb.rerankers` (§4.5) was verified against the `__all__` export in `lancedb` 0.34.0 `main` source; it can still change between releases, and runtime availability of each depends on the provider SDK/extra being installed.
- The full **embedding-provider list** in the registry (§6) depends on installed optional dependencies and the release; the registry names shown are representative, not exhaustive. Confirm with `from lancedb.embeddings import get_registry; get_registry()` in the target environment.
- `connect_async` shows an `oauth_config=None` parameter (present in current `main`); it is newer and may not exist in older releases. The async surface generally tracks `main` slightly ahead of the sync API.
- The `LanceModel`/`Vector`/`SourceField`/`VectorField` example (§6) reproduces the canonical upstream pattern (component names confirmed from docs); the specific model string `BAAI/bge-small-en-v1.5` is illustrative. `docs.lancedb.com/embedding/quickstart` is the live page (older `docs.lancedb.com/embeddings/*` paths 404).
