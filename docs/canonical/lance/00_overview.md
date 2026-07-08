# Lance & LanceDB — Overview, Ecosystem, Packaging & Versions

> Canonical upstream reference — fetched 2026-07-08 from official Lance documentation and source repositories. Documents upstream behavior as-is; this is NOT core-x doctrine. Treat the version numbers and API signatures here as fetched-date ground truth.
>
> Primary sources:
> - https://github.com/lance-format/lance — Lance source repository (README, packaging, Python bindings). The former `github.com/lancedb/lance` path now **301-redirects** here (verified 2026-07-08); `lance-format/lance` is the current canonical org.
> - https://lance.org/ — Current official Lance documentation site (overview, quickstart, user guide, format spec, SDK docs). This is the live docs home; the older `lancedb.github.io/lance/` API pages now 404 and redirect readers here.
> - https://pypi.org/project/pylance/ + `https://pypi.org/pypi/pylance/json` — PyPI release metadata for the low-level `pylance` package (`import lance`).
> - https://pypi.org/project/lancedb/ + `https://pypi.org/pypi/lancedb/json` — PyPI release metadata for the high-level `lancedb` package (`import lancedb`).
> - https://lancedb.github.io/lancedb/ — LanceDB (embedded vector DB) documentation.
> - Verbatim signatures quoted from source at tag `v8.0.0` (pylance) and `python-v0.34.0` (lancedb):
>   - `python/python/lance/dataset.py` (`write_dataset`), `python/python/lance/__init__.py` (`dataset`)
>   - `python/python/lancedb/__init__.py` (`connect`, `__all__`)

Scope: What Lance and LanceDB are, how the two distinct Python packages (`pylance` vs `lancedb`) relate and differ, the surrounding ecosystem and bindings, the current released versions and versioning scheme as of 2026-07-08, install commands, a runnable hello-world, and a map of the sibling reference files in this library.

---

## 1. The two Python packages (read this first — it is the #1 source of confusion)

There are **two separate PyPI packages** in the Lance world. They are installed under different names, they import under different names, and they sit at different layers of the stack. Conflating them is the most common footgun.

| | Low-level format | High-level database |
|---|---|---|
| **PyPI package** | `pylance` | `lancedb` |
| **Install** | `pip install pylance` | `pip install lancedb` |
| **Import** | `import lance` | `import lancedb` |
| **Layer** | Columnar file/table format + `Dataset` API | Embedded vector database built *on top of* the Lance format |
| **Core object** | `lance.LanceDataset` | `lancedb.DBConnection` → `lancedb.Table` |
| **You reach for it when** | You want direct control of fragments, versions, scalar/vector indices, `write_dataset`, `merge_insert`, time travel, Arrow/DuckDB interop | You want a batteries-included vector store: `connect()`, `create_table()`, `add()`, `.search()`, FTS, remote/cloud |
| **Source (Python)** | `lance-format/lance` → `python/python/lance/` | `lancedb/lancedb` → `python/python/lancedb/` |

### CRITICAL: `lancedb` does NOT re-export the low-level `lance` module

Installing `lancedb` does **not** give you `import lance`, and importing `lancedb` does not expose the low-level `LanceDataset` API. Verified against `lancedb/__init__.py` at `python-v0.34.0`: its imports are all from internal submodules (`._lancedb`, `.db`, `.table`, `.remote`, `.expr`, `.schema`, `.namespace`) — there is **no `from lance import ...` or `import lance`** anywhere in the package's public surface. Its `__all__` is:

```python
__all__ = [
    "connect", "connect_async", "connect_namespace", "connect_namespace_async",
    "AsyncConnection", "AsyncLanceNamespaceDBConnection", "AsyncTable",
    "col", "Expr", "func", "lit", "URI", "sanitize_uri", "vector",
    "DBConnection", "LanceDBConnection", "LanceNamespaceDBConnection",
    "RemoteDBConnection", "Session", "Table", "__version__",
]
```

Consequences:

- To use the low-level `Dataset` API you must `pip install pylance` explicitly and `import lance`. `pip install lancedb` alone will not satisfy `import lance`.
- `lancedb` depends on the same underlying Rust core, but it does not surface `pylance`'s Python API. The two packages carry independent version numbers on independent release cadences (see §4).
- If a pipeline needs both the raw format control (indexing, compaction, `merge_insert` on a bare dataset) *and* the vector-DB conveniences, install **both** packages.

> Relevance to core-x: the core-x data plane writes and indexes Lance datasets directly through the low-level format API (`lance.write_dataset`, `LanceDataset`, `create_scalar_index`). That means `pylance` (`import lance`) is the load-bearing dependency — **not** `lancedb`. Pin `pylance`, not `lancedb`, for the ingest/compute pipelines. `lancedb` is only relevant if a downstream service wants the embedded vector-DB ergonomics on top of the same R2 datasets.

---

## 2. What Lance is

**Lance** is an open-source, columnar **data file format + table format** (plus a catalog/namespace spec) purpose-built for machine-learning and AI workloads. Upstream frames it as "the open lakehouse format for multimodal AI" providing vector search, full-text search, fast random access, and feature engineering over a lakehouse, while keeping SQL analytics and ACID transactions. The core is written in **Rust** (the dominant language of the repo per GitHub's language breakdown; the README states the core Rust implementation lives under `rust/` but does not itself quote a percentage), with language bindings on top.

### Framed against Parquet

Lance is positioned as a modern alternative to Parquet/Iceberg for AI data, with three headline differences the docs and README emphasize:

1. **Fast random access.** Upstream claims Lance is "100x faster than Parquet or Iceberg for random access" (point lookups / `take` of scattered rows by id) **without sacrificing scan throughput**. Parquet's row-group + page layout makes random single-row access expensive; Lance's layout is designed for cheap random reads, which is what vector search and feature serving need.
2. **Zero-copy versioning + ACID + time travel.** Every write produces a new immutable version (snapshot). You get ACID transactions, time travel to any prior version, tags, and branches with no extra infrastructure — no separate metadata service or transaction log system.
3. **Cheap schema evolution.** You can add columns with backfilled values without rewriting the whole table (`add_columns` / `merge`), because column data lives in separate files and new columns can be appended as new column fragments.

On top of the file format, Lance layers native **vector similarity search** (ANN via IVF_PQ / HNSW), **full-text search** (BM25 / inverted index), and **scalar indices** (BTREE, BITMAP, etc.) so a single Lance dataset can serve analytics, point lookups, and vector search from one copy of the data.

> Relevance to core-x: the two properties the core-x plane leans on are (a) **append-only immutable fragments** — every `write_dataset(..., mode="append")` adds fragments and bumps the version rather than mutating in place, which is what makes the R2-backed SoR safe and replayable; and (b) **BTREE scalar indices on resolution keys** for fast point lookups/joins at hundreds-of-millions-of-rows scale. Both are first-class Lance features, not core-x inventions. See `05_scalar_indices.md` and `03_writes_appends_upserts.md`.

---

## 3. Ecosystem

The Lance ecosystem has three concentric layers plus bindings and engine integrations.

### 3.1 Layers

- **Lance format (the core).** Rust crates implementing the file format, table format, indices, and I/O. Exposed to Python as the `pylance` package (`import lance`). This is the substrate everything else sits on. See `01_file_format.md`, `02_python_dataset_api.md`.
- **LanceDB (embedded database).** An in-process ("embedded", à la SQLite/DuckDB) vector database that stores its tables as Lance datasets and adds a table-oriented API: connections, tables, `add`, `.search()`, hybrid search, FTS, index management. Package `lancedb` (`import lancedb`). See `11_lancedb_table_api.md`.
- **LanceDB Cloud / Enterprise.** Managed, hosted deployments of LanceDB. The same `lancedb` client connects to them by passing an `api_key` (and `region` / `host_override`) to `connect()` — the URI switches from a local/object-store path to a remote endpoint. Cloud/Enterprise add serverless scaling, managed indexing, and multi-tenant serving. See `11_lancedb_table_api.md`.

### 3.2 Language bindings

Lance/LanceDB ships bindings across languages (built on the shared Rust core):

- **Python** — `pylance` (PyO3 bindings, `import lance`) and `lancedb` (`import lancedb`).
- **Rust** — the native crates (`lance`, `lancedb`); everything else is a binding over these.
- **Java** — JNI bindings.
- **JavaScript / TypeScript (Node)** — `@lancedb/lancedb` (Node native addon).

### 3.3 Engine & framework integrations

The Lance dataset is Arrow-native, so it interoperates with the broader analytics/ML stack. Upstream lists integrations including: **Apache Arrow**, **pandas**, **Polars**, **DuckDB**, **PyArrow**, **PyTorch**, **Ray**, **Apache Spark**, **Trino**, **Apache Flink**, **Daft**, and **Hugging Face** datasets. Reading a Lance dataset into a query engine is typically zero-copy through Arrow. See `10_duckdb_arrow_interop.md`.

> Relevance to core-x: the DuckDB → Arrow → Lance path the core-x pipelines use is a supported, zero-copy integration. DuckDB reads/streams ephemeral Parquet, executes projections/casts, and the resulting Arrow `RecordBatchReader` is handed to `lance.write_dataset()` with no intermediate materialization. `data_obj` accepts a PyArrow `RecordBatchReader` directly (see §5 signature). See `10_duckdb_arrow_interop.md`.

---

## 4. Current released versions (as of 2026-07-08) & versioning scheme

All figures below were pulled from the PyPI JSON API and GitHub on the fetch date.

### 4.1 `pylance` (the `import lance` package)

- **Current version: `8.0.0`**, released **2026-07-01**.
- `requires_python: >=3.9` (classifiers advertise 3.9–3.14).
- Summary string: `python wrapper for Lance columnar format`.
- Recent release history (version → first-file upload time, UTC):

  | Version | Released |
  |---|---|
  | 8.0.0 | 2026-07-01 |
  | 7.0.0 | 2026-05-27 |
  | 6.0.1 | 2026-05-20 |
  | 6.0.0 | 2026-05-11 |
  | 4.0.1 | 2026-04-24 |
  | 4.0.0 | 2026-03-30 |
  | 3.0.1 | 2026-03-19 |
  | 3.0.0 | 2026-03-13 |
  | 2.0.1 | 2026-02-13 |
  | 2.0.0 | 2026-02-05 |

  The corresponding source tags in the repo are `v8.0.0`, `v7.0.0`, `v6.0.1`, … (stable tags observed via `git ls-remote`: `v1.0.0`…`v8.0.0`; note **`v5.x` is skipped** in the stable tag line — 4.x jumps to 6.x). Pre-release/beta tags like `v9.0.0-beta.*` exist ahead of the current stable line.

### 4.2 `lancedb` (the `import lancedb` package)

- **Current version: `0.34.0`**, released **2026-07-02**.
- `requires_python: >=3.10` (classifiers advertise 3.10–3.13).
- Recent release history:

  | Version | Released |
  |---|---|
  | 0.34.0 | 2026-07-02 |
  | 0.33.0 | 2026-05-28 |
  | 0.32.0 | 2026-05-27 |
  | 0.30.2 | 2026-03-31 |
  | 0.30.1 | 2026-03-20 |
  | 0.30.0 | 2026-03-16 |
  | 0.29.2 | 2026-02-09 |
  | 0.29.0 | 2026-02-06 |
  | 0.27.0 | 2026-01-22 |

  In the `lancedb/lancedb` repo the Python releases are tagged `python-v0.34.0` (etc.), distinct from the Rust-crate tags (`v0.31.0` etc.) — the two version lines are **not** the same number.

### 4.3 DuckDB (the query engine used upstream of Lance in core-x pipelines)

- **Current version: `1.5.4`** (PyPI `duckdb`, fetched 2026-07-08). Referenced here because the core-x pipelines run DuckDB → Arrow → Lance; see §5 and `10_duckdb_arrow_interop.md`.

### 4.4 Versioning scheme

- **`pylance`** uses a fast-moving `MAJOR.MINOR.PATCH` line where **major bumps are frequent** (roughly monthly) and do not necessarily signal breaking API changes in the everyday Python surface — treat any major bump as "read the changelog," but expect the common `write_dataset` / `dataset` / `LanceDataset` API to remain stable across them. There are `rc`/`beta` pre-releases ahead of stable tags. Development status on PyPI is still classified **Alpha**.
- **`lancedb`** is on a `0.MINOR.PATCH` line (still pre-1.0). Minor bumps can carry behavioral changes; pin exactly for reproducibility.
- The **on-disk data storage format** is versioned independently of the package version, via `write_dataset(data_storage_version=...)` which accepts `"stable"`, `"2.0"`, `"2.1"`, `"2.2"`, `"2.3"`, `"next"`, `"legacy"`, `"0.1"` (default `None` → latest stable). Newer storage versions are more efficient but require a newer `lance` to read. See `01_file_format.md`.

> Footgun: because `pylance` majors move fast and the storage format is separately versioned, a writer on a newer `pylance` can emit a `data_storage_version` that an older reader cannot open. Standardize the `pylance` version **and** the `data_storage_version` across every writer and reader in a pipeline.

---

## 5. Install & hello-world

### 5.1 Install

```bash
# Low-level format + Dataset API  (import lance)
pip install pylance

# High-level embedded vector DB   (import lancedb)
pip install lancedb

# Both, if a pipeline needs raw format control AND the vector-DB API
pip install pylance lancedb

# Preview / nightly of pylance (extra index):
pip install --pre --extra-index-url https://pypi.fury.io/lance-format pylance

# Preview / nightly of lancedb:
pip install --pre --extra-index-url https://pypi.fury.io/lancedb/ lancedb
```

### 5.2 Hello-world — low-level `lance` (write a tiny dataset, open, scan)

Adapted from the official quickstart (https://lance.org/quickstart/):

```python
import shutil
import lance
import pandas as pd

# 1. Create a small dataset from a pandas DataFrame
df = pd.DataFrame({"a": [5]})

shutil.rmtree("/tmp/test.lance", ignore_errors=True)
dataset = lance.write_dataset(df, "/tmp/test.lance")   # mode="create" (default)

# 2. Re-open it (Lance is immutable — reopen to see the latest version)
dataset = lance.dataset("/tmp/test.lance")

# 3. Scan / read it back
print(dataset.to_table().to_pandas())
#    a
# 0  5

# 4. Append more rows -> new immutable version
lance.write_dataset(pd.DataFrame({"a": [6, 7]}), "/tmp/test.lance", mode="append")
dataset = lance.dataset("/tmp/test.lance")   # reopen to observe the append
print(dataset.count_rows())   # 3
print(dataset.version)        # 2  (each write bumps the version)
```

Converting an existing Parquet dataset to Lance in two lines (the README's canonical pitch):

```python
import lance
import pyarrow.dataset as pa_ds

parquet = pa_ds.dataset("/path/to/data.parquet", format="parquet")
lance.write_dataset(parquet, "/tmp/test.lance")
```

### 5.3 Hello-world — high-level `lancedb` (connect, create table, search)

```python
import lancedb

db = lancedb.connect("/tmp/lancedb")           # local; pass api_key=... for Cloud
table = db.create_table(
    "vectors",
    data=[{"vector": [3.1, 4.1], "item": "foo"},
          {"vector": [5.9, 26.5], "item": "bar"}],
)
results = table.search([3.0, 4.0]).limit(1).to_pandas()
print(results)
```

### 5.4 Exact signature: `lance.write_dataset` (verbatim, pylance v8.0.0)

Copied verbatim from `python/python/lance/dataset.py` at tag `v8.0.0`:

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
    base_store_params: Optional[Dict[str, Dict[str, str]]] = None,
    external_blob_mode: Literal["reference", "ingest"] = "reference",
    allow_external_blob_outside_bases: bool = False,
    blob_pack_file_size_threshold: Optional[int] = None,
    namespace_client: Optional[LanceNamespace] = None,
    table_id: Optional[List[str]] = None,
) -> LanceDataset:
```

| Parameter | Type | Default | Notes / accepted values |
|---|---|---|---|
| `data_obj` | `ReaderLike` | — (required) | Pandas DataFrame, PyArrow `Table` / `Dataset` / `Scanner` / `RecordBatchReader`, or a Hugging Face dataset. This is where a DuckDB→Arrow `RecordBatchReader` plugs in. |
| `uri` | `str \| Path \| LanceDataset \| None` | `None` | Target directory. If a `LanceDataset` is passed, its session is reused. Either `uri` **or** (`namespace_client` + `table_id`) must be provided. |
| `schema` | `pa.Schema \| None` | `None` | Override schema; used when input is a pandas DataFrame. |
| `mode` | `str` | `"create"` | `"create"` (fail if exists), `"overwrite"` (new snapshot replacing data), `"append"` (concat input onto latest version, or create if absent). |
| `max_rows_per_file` | `int` | `1024*1024` (1,048,576) | Rows before rolling to a new file. |
| `max_rows_per_group` | `int` | `1024` | Rows before starting a new group within a file. |
| `max_bytes_per_file` | `int` | `90*1024**3` (~90 GB) | Soft byte cap per file; checked after each group, so may overshoot. 90 GB default because object stores have a 100 GB/file hard limit. |
| `commit_lock` | `CommitLock \| None` | `None` | Custom commit lock; only needed if the object store lacks atomic commits. |
| `progress` | `FragmentWriteProgress \| None` | `None` | *Experimental.* Per-fragment write progress hooks. |
| `storage_options` | `Dict[str,str] \| None` | `None` | Connection params (credentials, endpoint, region, etc.) for S3/R2/GCS/Azure. See `07_storage_object_stores.md`. |
| `data_storage_version` | `Literal["stable","2.0","2.1","2.2","2.3","next","legacy","0.1"] \| None` | `None` | On-disk format version; `None` = latest stable. Newer = more efficient, needs newer reader. |
| `use_legacy_format` | `bool \| None` | `None` | **Deprecated.** Use `data_storage_version` instead. |
| `enable_v2_manifest_paths` | `bool` | `True` | New datasets use V2 manifest paths (faster opening with many versions on object stores). No effect on existing datasets; migrate via `LanceDataset.migrate_manifest_paths_v2`. |
| `enable_stable_row_ids` | `bool` | `False` | *Experimental.* Stable row ids survive compaction (not updates); makes compaction cheaper (no secondary-index rewrite). |
| `auto_cleanup_options` | `AutoCleanupConfig \| None` | `None` | Auto-cleanup of old versions on new datasets. For existing datasets set config keys `lance.auto_cleanup.interval` and `lance.auto_cleanup.older_than` (both required). |
| `commit_message` | `str \| None` | `None` | Message attached to the commit. |
| `transaction_properties` | `Dict[str,str] \| None` | `None` | Arbitrary key/values recorded on the transaction. |
| `initial_bases` | `List[DatasetBasePath] \| None` | `None` | Multi-base dataset support (advanced). |
| `target_bases` | `List[str] \| None` | `None` | Advanced multi-base targeting. |
| `base_store_params` | `Dict[str,Dict[str,str]] \| None` | `None` | Per-base storage params. |
| `external_blob_mode` | `Literal["reference","ingest"]` | `"reference"` | How external blobs are handled. |
| `allow_external_blob_outside_bases` | `bool` | `False` | Permit external blobs outside declared bases. |
| `blob_pack_file_size_threshold` | `int \| None` | `None` | Threshold for packing blob files. |
| `namespace_client` | `LanceNamespace \| None` | `None` | Namespace/catalog client; alternative to `uri`. |
| `table_id` | `List[str] \| None` | `None` | Table id within a namespace (paired with `namespace_client`). |

> Relevance to core-x: `storage_options` is how R2 credentials/endpoint reach the writer, `mode="append"` gives the append-only immutable-fragment semantics the SoR relies on, and `data_storage_version` must be standardized across writers/readers. `max_bytes_per_file`/`max_rows_per_file` govern fragment sizing at hundreds-of-millions-of-rows scale. Details in `07_storage_object_stores.md` and `03_writes_appends_upserts.md`.

### 5.5 Exact signature: `lance.dataset` (verbatim, pylance v8.0.0)

Copied verbatim from `python/python/lance/__init__.py` at tag `v8.0.0`:

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
) -> LanceDataset:
```

| Parameter | Type | Default | Notes |
|---|---|---|---|
| `uri` | `str \| Path \| None` | `None` | Local path (`/tmp/data.lance`) or object-store URI (`s3://bucket/data.lance`). Either `uri` or (`namespace_client` + `table_id`) required. |
| `version` | `int \| str \| None` | `None` | Load a specific version by number (`int`) or **tag** (`str`); default loads latest. See `04_versioning_time_travel.md`. |
| `asof` | `datetime \| str \| None` | `None` | Latest version created on/before this timestamp. Ignored if `version` is set. |
| `block_size` | `int \| None` | `None` | Hint for minimal I/O request size (bytes). |
| `commit_lock` | `CommitLock \| None` | `None` | Custom commit lock (object stores lacking atomic commits). |
| `index_cache_size` | `int \| None` | `None` | Number of index pages (e.g. IVF partitions) cached in memory; LRU+TTL. Default `256`. |
| `storage_options` | `Dict[str,str] \| None` | `None` | Connection params (credentials, endpoint, region) for the object store. |
| `default_scan_options` | `Dict[str,str] \| None` | `None` | Default scan args applied to every scan (same args as `LanceDataset.scanner`); can expose meta fields like `_rowid` / `_rowaddr`. |
| `metadata_cache_size_bytes` | `int \| None` | `None` | Metadata cache size in bytes (schema/statistics). |
| `index_cache_size_bytes` | `int \| None` | `None` | Index cache size in bytes (newer byte-based alternative to `index_cache_size`). |
| `read_params` | `Dict[str,any] \| None` | `None` | Low-level read parameters. |
| `session` | `Session \| None` | `None` | Reuse a shared session (caches, config). |
| `namespace_client` | `LanceNamespace \| None` | `None` | Namespace/catalog client; alternative to `uri`. |
| `table_id` | `List[str] \| None` | `None` | Table id within a namespace. |
| `base_store_params` | `Dict[str,Dict[str,str]] \| None` | `None` | Per-base storage params (multi-base datasets). |

### 5.6 Exact signature: `lancedb.connect` (verbatim, lancedb python-v0.34.0)

Copied verbatim from `python/python/lancedb/__init__.py` at tag `python-v0.34.0`:

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
) -> DBConnection:
```

| Parameter | Type | Default | Notes |
|---|---|---|---|
| `uri` | `URI (str \| Path) \| None` | `None` | DB uri (local path or object-store path). May be omitted when `namespace_client_impl` is provided. |
| `api_key` | `str \| None` | `None` | If set, connect to **LanceDB Cloud**. Can be set via env `LANCEDB_API_KEY`. Sync Cloud requires an API key; OAuth is `connect_async`-only. |
| `region` | `str` | `"us-east-1"` | LanceDB Cloud region. |
| `host_override` | `str \| None` | `None` | Override URL for LanceDB Cloud/Enterprise. |
| `read_consistency_interval` | `timedelta \| None` | `None` | How often to check other processes' updates. `None` = no consistency checking (best perf). |
| `request_thread_pool` | `int \| ThreadPoolExecutor \| None` | `None` | Thread pool for requests. |
| `client_config` | `ClientConfig \| Dict \| None` | `None` | Remote client config (timeouts, retries). |
| `storage_options` | `Dict[str,str] \| None` | `None` | Object-store connection params (S3/R2/GCS/Azure). |
| `session` | `Session \| None` | `None` | Shared session. |
| `manifest_enabled` | `bool` | `False` | Enable manifest features. |
| `namespace_client_impl` | `str \| None` | `None` | Namespace/catalog client implementation name. |
| `namespace_client_properties` | `Dict[str,str] \| None` | `None` | Properties for the namespace client. |
| `namespace_client_pushdown_operations` | `List[str] \| None` | `None` | Operations to push down to the namespace client. |
| `**kwargs` | `Any` | — | Additional/forwarded options. |

---

## 6. Deprecations, renames & common footguns

- **`write_dataset(use_legacy_format=...)` is deprecated** — use `data_storage_version` instead (confirmed in the v8.0.0 docstring).
- **`pip install lancedb` ≠ `import lance`.** They are different packages; installing one does not provide the other's import (§1).
- **Immutability requires reopening.** After any write, the in-memory `LanceDataset` handle you already hold does **not** reflect the new version — call `lance.dataset(uri)` again (or `dataset.checkout_latest()` where available) to observe appends/updates. This bites people who `write_dataset(..., mode="append")` then read from the stale handle and see old row counts.
- **`mode="create"` raises if the URI already exists.** Use `"append"` or `"overwrite"` for existing datasets.
- **Storage format drift.** A newer `pylance` can write a `data_storage_version` an older reader cannot open; pin both the package and the storage version across a pipeline (§4.4).
- **The old API docs URLs (`lancedb.github.io/lance/api/python/...`) 404** as of 2026-07-08 — the Python SDK reference now lives under `lance.org` (`/sdk_docs/`, `/guide/`, `/quickstart/`). When a linked API page 404s, look on `lance.org`.
- **`v5.x` does not exist** in the stable `pylance` tag line (4.x → 6.x). Do not assume a `pylance==5.x` exists.
- **`lancedb` version ≠ Rust-crate version.** The `lancedb/lancedb` repo tags Python releases as `python-v0.34.0` and the Rust crate as `v0.31.0`; they are different numbers for the same release train.

---

## 7. Map of this reference library

Sibling files in `docs/canonical/lance/` (this domain). This file (`00_overview.md`) is self-contained; the others go deep on each area.

| File | Covers |
|---|---|
| `00_overview.md` | **(this file)** Lance & LanceDB overview, the two Python packages, ecosystem, versions, install, hello-world. |
| `01_file_format.md` | The Lance columnar file format & on-disk dataset layout (manifests, fragments, data files, `data_storage_version`). |
| `02_python_dataset_api.md` | `pylance` Python SDK — `lance.dataset`, `lance.write_dataset`, the `LanceDataset` class and its methods. |
| `03_writes_appends_upserts.md` | Writing data — `mode` create/append/overwrite, `merge_insert` (upsert), `delete`, `update`, `add_columns`, `LanceOperation` & manual commits. |
| `04_versioning_time_travel.md` | Versioning, time travel (`version`/`asof`), tags & branches, `cleanup_old_versions` / auto-cleanup. |
| `05_scalar_indices.md` | Scalar indices — BTREE, BITMAP, LABEL_LIST, INVERTED/FTS, NGRAM, and any others; `create_scalar_index`. |
| `06_vector_search.md` | Vector indices & ANN search — IVF_PQ / HNSW, `nprobes`, `refine_factor`, multivector. |
| `07_storage_object_stores.md` | Object-store configuration — `storage_options` for S3 / Cloudflare R2 / GCS / Azure. |
| `08_compaction_maintenance.md` | Dataset maintenance — compaction, index optimization, fragment management. |
| `09_scanning_filtering.md` | Scanning, filtering, projection pushdown & `take()`. |
| `10_duckdb_arrow_interop.md` | Interop — Apache Arrow, DuckDB, Polars/pandas; reading Lance from query engines. |
| `11_lancedb_table_api.md` | LanceDB (the database) — `connect`, tables, `add`/`search`, FTS, cloud/remote. |

---

## 8. Unverified / needs confirmation

- **Precise per-version changelog for pylance 8.0.0 / 7.0.0** was not fetched line-by-line; the version→date table is authoritative (from PyPI JSON) but the specific feature deltas between majors are not enumerated here. Consult the repo's release notes for exact per-version changes.
- **Full `LanceDataset` method inventory** (every method signature) is out of scope for this overview and is documented in `02_python_dataset_api.md`; only `write_dataset`, `dataset`, and `connect` signatures were quoted verbatim here.
- **`ReaderLike` / `ts_types` / `AutoCleanupConfig` exact type aliases** are referenced in the signatures as imported names; their full definitions live in the pylance source (`lance.dataset` / `lance.types`) and were not expanded here.
- Source-repo naming: `github.com/lance-format/lance` is the current canonical repository; the older `github.com/lancedb/lance` path **301-redirects** to it (verified 2026-07-08). Some sibling files still cite the `lancedb/lance` URL in their source blocks — it resolves to the same repo through the redirect, so those citations are not broken, only older. The current docs home is `lance.org`; the older `lancedb.github.io/lance/` host still renders the same content. See [`../README.md`](../README.md) for the authoritative canonical-source table.
