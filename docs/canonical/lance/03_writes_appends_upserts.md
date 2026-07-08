# Writing Data — modes, append, merge_insert, delete, update, add_columns, LanceOperation & commits

> Canonical upstream reference — fetched 2026-07-08 from official Lance documentation and source repositories. Documents upstream behavior as-is; this is NOT core-x doctrine. Treat the version numbers and API signatures here as fetched-date ground truth.
>
> Primary sources:
> - https://raw.githubusercontent.com/lancedb/lance/main/python/python/lance/dataset.py — the `pylance` Python SDK source; every signature, parameter table, and docstring below is quoted verbatim from this file (repo `main`, package version `9.0.0-beta.18` per `python/Cargo.toml`).
> - https://raw.githubusercontent.com/lancedb/lance/main/python/python/lance/udf.py — the `batch_udf` decorator and `BatchUDF` class used by `add_columns()`.
> - https://lancedb.github.io/lance/ — the official Lance documentation site (guides and Python API reference index).
> - PyPI JSON APIs (`pypi.org/pypi/{pylance,lancedb,duckdb}/json`) — current released version numbers.

Scope: Every mutating operation on an existing Lance dataset — the `write_dataset` modes, `merge_insert` upserts, `delete`/`update`, schema evolution (`add_columns`/`alter_columns`/`drop_columns`/`merge`), and the low-level `LanceDataset.commit` + `LanceOperation` API with its optimistic-concurrency retry model.

---

## 0. Versions as of 2026-07-08

| Package | Current released (PyPI) | Notes |
|---|---|---|
| `pylance` (the `lance` Python module) | **8.0.0** | Install as `pip install pylance`; imported as `import lance`. |
| `lancedb` (the higher-level DB) | **0.34.0** | Wraps pylance; see [11_lancedb_table_api.md](11_lancedb_table_api.md). |
| `duckdb` | **1.5.4** | Query engine used upstream of the writer; see [10_duckdb_arrow_interop.md](10_duckdb_arrow_interop.md). |

> **Version caveat.** The signatures and docstrings in this file are quoted from the Lance repo `main` branch, whose `python/Cargo.toml` declares `version = "9.0.0-beta.18"`. The current *released* wheel on PyPI is `pylance 8.0.0`. Most of the surface documented here is stable across both, but a handful of parameters are recent additions — they are flagged inline as **[main-branch / may be newer than 8.0.0]** where the released signature could differ. When exact behavior matters on a pinned version, confirm against that version's `dataset.py`.

> **Relevance to core-x:** The core-x data plane writes Lance to Cloudflare R2 via `lance.write_dataset(... , storage_options=...)` and mutates by key with `merge_insert`. This file is the authoritative reference for the two write paths the plane actually uses (append-only fragment growth + key-level upsert) plus the OCC retry semantics that make concurrent Modal workers safe against a single R2 dataset.

---

## 1. Write modes: `create` / `append` / `overwrite`

All bulk writes go through the module-level `write_dataset`. The `mode` parameter is a plain string with exactly three accepted values.

### Verbatim signature

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
) -> LanceDataset:
```

### The `mode` values (verbatim from docstring)

| `mode` | Effect | Version behavior |
|---|---|---|
| `"create"` | **create** — create a new dataset (**raises if uri already exists**). This is the default. | — |
| `"overwrite"` | **overwrite** — create a new snapshot version. The schema and contents are replaced; **prior versions are retained** (a new manifest version is written, the old data is not deleted until `cleanup_old_versions`). See [04_versioning_time_travel.md](04_versioning_time_travel.md). | — |
| `"append"` | **append** — create a new version that is the concat of the input and the latest version, **or a new dataset if uri doesn't exist**. | — |

> Note: `LanceDataset.insert(data, *, mode="append", **kwargs)` is a thin instance-method wrapper over `write_dataset(data, self, mode=mode, **kwargs)`. Its docstring differs slightly and says `append` *"raises if uri does not exist"*, whereas the module-level `write_dataset` docstring says append will create the dataset if missing. Treat the module-level `write_dataset` semantics (create-if-missing) as authoritative for the free function.

### Effect on versions and fragments

- **Every** call to `write_dataset` (any mode) produces a **new dataset version** (a new manifest). Versions are immutable and monotonically increasing.
- **`append`** adds **new fragments** to the existing set and leaves all existing fragments untouched — this is the append-only path. No existing data file is rewritten; the new manifest references the old fragments plus the new ones.
- **`overwrite`** writes a fresh set of fragments and points the new manifest at only those; old fragments become garbage collectable via cleanup.
- A **fragment** is a unit of data (one or more Lance data files + optional deletion file). `max_rows_per_file` (default `1024*1024`) and `max_bytes_per_file` (default `90 GB`, a *soft* limit checked per group) bound how many fragments a single write produces. `max_rows_per_group` (default `1024`) controls the row-group granularity inside a file.

### Key parameter reference (`write_dataset`)

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `data_obj` | `ReaderLike` | required | Data to write. Accepts pandas DataFrame, PyArrow Table, Dataset, Scanner, `RecordBatchReader`, or a HuggingFace dataset. Zero-copy from Arrow. |
| `uri` | `str \| Path \| LanceDataset \| None` | `None` | Target directory/URI. Passing a `LanceDataset` reuses its session. Either `uri` **or** (`namespace_client` + `table_id`) must be given, not both. |
| `schema` | `pa.Schema` | `None` | Overrides pandas→arrow inference when input is a DataFrame. |
| `mode` | `str` | `"create"` | `create` / `append` / `overwrite` (see above). |
| `max_rows_per_file` | `int` | `1048576` | Rows before rolling to a new file. |
| `max_rows_per_group` | `int` | `1024` | Rows before starting a new group within a file. |
| `max_bytes_per_file` | `int` | `90 GiB` | Soft byte cap per file (checked after each group; can overshoot). Object stores enforce a hard 100 GB/file limit. |
| `commit_lock` | `CommitLock` | `None` | Custom external commit lock; only needed if the object store lacks atomic commits. |
| `progress` | `FragmentWriteProgress` | `None` | *Experimental* per-fragment write hooks. |
| `storage_options` | `Dict[str, str]` | `None` | Object-store connection params (credentials, endpoint, region). See [07_storage_object_stores.md](07_storage_object_stores.md). Supports per-base scoping via `base_<id>.<key>` keys. |
| `data_storage_version` | `Literal["stable","2.0","2.1","2.2","2.3","next","legacy","0.1"]` | `None` | On-disk file-format version. `None` = latest stable. See [01_file_format.md](01_file_format.md). |
| `use_legacy_format` | `bool` | `None` | **Deprecated** — use `data_storage_version` instead. Emits `DeprecationWarning`; `True`→`"legacy"`, `False`→`"stable"`. |
| `enable_v2_manifest_paths` | `bool` | `True` | Use V2 manifest paths (efficient opening of many-version datasets). No effect on existing datasets. **Makes dataset unreadable by Lance < 0.17.0.** |
| `enable_stable_row_ids` | `bool` | `False` | *Experimental.* Row ids stable across compaction (not across updates); avoids reindex on compaction. |
| `auto_cleanup_options` | `AutoCleanupConfig` | `None` | `{"interval": int, "older_than_seconds": int}`. Only applied when **creating** a new dataset. To add to an existing dataset use `update_config` with `lance.auto_cleanup.interval` + `lance.auto_cleanup.older_than`. |
| `commit_message` | `str` | `None` | Message stored in dataset metadata; retrievable via `read_transaction()`. Overrides any `lance.commit.message` in `transaction_properties`. |
| `transaction_properties` | `Dict[str, str]` | `None` | Custom key/value props stored on the transaction. |
| `initial_bases` | `List[DatasetBasePath]` | `None` | New base paths to register in the manifest. **CREATE mode only.** |
| `target_bases` | `List[str]` | `None` | References (base name or path URI) of registered bases to write data files into. Valid in all modes. |
| `target_all_bases` | `bool` | `None` | Round-robin new files across every registered base. Cannot combine with `target_bases`. |
| `base_store_params` | `Dict[str, Dict[str,str]]` | `None` | Runtime-only per-base object-store params (not persisted to manifest). |
| `external_blob_mode` | `Literal["reference","ingest"]` | `"reference"` | `reference` = store external blob URI; `ingest` = read bytes at write time into Lance storage. **[main-branch / may be newer than 8.0.0]** |
| `allow_external_blob_outside_bases` | `bool` | `False` | Allow external blob URIs outside registered bases (only with `reference`). **[main-branch]** |
| `blob_pack_file_size_threshold` | `int` | `None` | Max bytes for blob v2 `.blob` pack sidecar files (default 1 GiB). **[main-branch]** |
| `namespace_client` | `LanceNamespace` | `None` | Namespace client; fetch table location + storage options via `describe_table()`. Must pair with `table_id`; excludes `uri`. |
| `table_id` | `List[str]` | `None` | Table identifier within a namespace (e.g. `["my_table"]`). Pairs with `namespace_client`. |

### Example

```python
import lance
import pyarrow as pa

tab = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})

# create (fails if the dataset already exists)
ds = lance.write_dataset(tab, "s3://bucket/ds.lance", mode="create",
                         storage_options={"region": "auto"})

# append new fragments (creates the dataset if missing)
more = pa.table({"id": [4, 5], "name": ["d", "e"]})
ds = lance.write_dataset(more, "s3://bucket/ds.lance", mode="append")

# overwrite — new snapshot version, prior versions retained until cleanup
ds = lance.write_dataset(tab, "s3://bucket/ds.lance", mode="overwrite")
```

---

## 2. `merge_insert` — SQL-MERGE upsert builder

`merge_insert(on)` returns a **builder** (`MergeInsertBuilder`). You chain `when_*` clauses to declare what happens to each of the three row categories, then call `.execute(data)`.

### The three row categories (verbatim)

> "Matched" records are records that exist in both the source table and the target table. "Not matched" records exist only in the source table (e.g. these are new data). "Not matched by source" records exist only in the target table (this is old data).

- **source table** = the `data` you pass to `.execute()`.
- **target table** = the existing dataset.

> Data is **reordered** by this operation. Updated rows are deleted then re-inserted at the end; new-insert order fluctuates because an internal hash-join is used.

### `merge_insert` verbatim signature

```python
def merge_insert(
    self,
    on: Optional[Union[str, Iterable[str]]] = None,
) -> MergeInsertBuilder:
```

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `on` | `str \| Iterable[str] \| None` | `None` | The join key column(s) matching source↔target rows. If `None`, uses the dataset's **unenforced primary key** from schema metadata; if no PK is configured and `on` is `None`, raises `ValueError`. |

### Builder surface

All builder methods return `MergeInsertBuilder` for chaining.

#### `when_matched_update_all(condition=None)` — verbatim

```python
def when_matched_update_all(
    self, condition: Optional[str] = None
) -> "MergeInsertBuilder":
```

Update rows present in **both** source and target: the target row is removed and the source row added. Optional `condition` is a SQL filter; only matched rows also satisfying it are updated. Use prefix `target.` and `source.` to reference columns, e.g. `"source.last_update < target.last_update"`. Failing the condition leaves the row unchanged (it does **not** reclassify as "not matched").

#### `when_not_matched_insert_all()` — verbatim

```python
def when_not_matched_insert_all(self) -> "MergeInsertBuilder":
```

Insert rows that exist **only in the source** (new data).

#### `when_not_matched_by_source_delete(expr=None)` — verbatim

```python
def when_not_matched_by_source_delete(
    self, expr: Optional[str] = None
) -> "MergeInsertBuilder":
```

Delete rows that exist **only in the target** (old data). Optional `expr` is a SQL filter limiting which target-only rows get deleted.

#### Additional matched clauses (verbatim)

```python
def when_matched_delete(self) -> "MergeInsertBuilder":
def when_matched_fail(self)   -> "MergeInsertBuilder":
```

- `when_matched_delete()` — delete matched rows in the target.
- `when_matched_fail()` — fail the whole operation with an exception if **any** rows match (useful to guarantee no existing rows are overwritten).

#### Concurrency / execution tuning (verbatim)

```python
def conflict_retries(self, max_retries: int) -> "MergeInsertBuilder":   # default 10
def retry_timeout(self, timeout: timedelta) -> "MergeInsertBuilder":    # default 30s
def use_index(self, use_index: bool) -> "MergeInsertBuilder":          # default True
def target_bases(self, bases: List[str]) -> "MergeInsertBuilder":
```

| Method | Default | Meaning |
|---|---|---|
| `conflict_retries(max_retries)` | `10` | Retries under contention. If `> 0`, keeps a copy of the input (memory or disk) to replay on conflict. |
| `retry_timeout(timeout: timedelta)` | `30s` | Wall-clock cap on retries; at least one attempt always runs. |
| `use_index(use_index: bool)` | `True` | `False` forces a full table scan even if a join-key index exists (benchmarking / optimizer override). |
| `target_bases(bases: List[str])` | — | Write new fragments round-robin across named/URI bases. Patches to existing fragments + deletion files always go to primary storage. |

#### `.execute()` / `.execute_uncommitted()` — verbatim

```python
def execute(self, data_obj: ReaderLike, *, schema: Optional[pa.Schema] = None):
def execute_uncommitted(
    self, data_obj: ReaderLike, *, schema: Optional[pa.Schema] = None
) -> Tuple[Transaction, Dict[str, Any]]:
```

- `execute(data)` — runs the merge, updates the dataset, and returns a stats dict `{'num_inserted_rows': int, 'num_updated_rows': int, 'num_deleted_rows': int}` (typed as `ExecuteResult`).
- `execute_uncommitted(data)` — same computation but returns `(Transaction, stats)` **without committing**, for distributed / batched commit flows (see §7).
- `schema` is only needed when `data_obj` is a generator-type source.

### Upsert example (verbatim from source docstring)

```python
import lance
import pyarrow as pa

table = pa.table({"a": [2, 1, 3], "b": ["a", "b", "c"]})
dataset = lance.write_dataset(table, "example")

new_table = pa.table({"a": [2, 3, 4], "b": ["x", "y", "z"]})

# "upsert": update existing keys, insert new keys
dataset.merge_insert("a")             \
    .when_matched_update_all()        \
    .when_not_matched_insert_all()    \
    .execute(new_table)
# -> {'num_inserted_rows': 1, 'num_updated_rows': 2, 'num_deleted_rows': 0}
```

### Common patterns

```python
# Insert-if-not-exists (no updates)
ds.merge_insert("id").when_not_matched_insert_all().execute(new_data)

# Full sync / mirror: upsert present rows AND delete rows no longer in source
ds.merge_insert("id")                 \
    .when_matched_update_all()        \
    .when_not_matched_insert_all()    \
    .when_not_matched_by_source_delete() \
    .execute(new_data)

# Replace a partition (e.g. all rows where month='january') with fresh data
ds.merge_insert("id")                 \
    .when_matched_update_all()        \
    .when_not_matched_insert_all()    \
    .when_not_matched_by_source_delete("month = 'january'") \
    .execute(new_data)

# Conditional update — only overwrite if source is newer
ds.merge_insert("id") \
    .when_matched_update_all("source.updated_at > target.updated_at") \
    .when_not_matched_insert_all() \
    .execute(new_data)
```

> **Partial-column updates:** You need not supply all columns. Omitted columns keep their existing value on update, or become **null** on insert (per the source docstring example).

> **Relevance to core-x:** `merge_insert(on=<resolution_key>).when_matched_update_all().when_not_matched_insert_all()` is the canonical key-level upsert for the plane. Back it with a `BTREE` scalar index on the join key (see [05_scalar_indices.md](05_scalar_indices.md)) so `use_index(True)` (the default) avoids a full-table scan. Raise `conflict_retries()` above the default 10 when many Modal workers upsert the same R2 dataset concurrently.

---

## 3. Schema evolution

### 3.1 `add_columns` — SQL expressions, UDFs, readers, or all-NULL columns

#### Verbatim signature

```python
def add_columns(
    self,
    transforms: (
        Dict[str, str]
        | BatchUDF
        | ReaderLike
        | pyarrow.Field
        | List[pyarrow.Field]
        | pyarrow.Schema
    ),
    read_columns: List[str] | None = None,
    reader_schema: Optional[pa.Schema] = None,
    batch_size: Optional[int] = None,
):
```

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `transforms` | `Dict[str,str]` \| `BatchUDF` \| `ReaderLike` \| `pyarrow.Field` \| `List[pyarrow.Field]` \| `pyarrow.Schema` | required | See modes below. |
| `read_columns` | `List[str] \| None` | `None` | Columns the UDF reads. `None` = all. Only used for UDF transforms (SQL infers reads automatically). May include `_rowid` / `_rowaddr`. |
| `reader_schema` | `pa.Schema` | `None` | Only valid when `transforms` is a `ReaderLike`; schema of the reader. |
| `batch_size` | `int` | `None` | Rows read per batch when applying the transform. Ignored for v1 datasets. |

**Four ways to specify new columns:**

1. **SQL expressions** — `Dict[str, str]` mapping new column name → SQL expression referencing existing columns. E.g. `{"triple_a": "a * 3"}`.
2. **Batch UDF** — a `BatchUDF` (built with the `@lance.batch_udf()` decorator) taking a `RecordBatch` and returning a `RecordBatch` of the new columns.
3. **Reader** — a `ReaderLike` (e.g. `RecordBatchReader`) supplying precomputed column values from an external source (often distributed staging).
4. **All-NULL columns** — a `pyarrow.Field` / `List[pyarrow.Field]` / `pyarrow.Schema`; adds NULL columns with that schema as a **metadata-only** operation.

#### `batch_udf` decorator — verbatim (`lance/udf.py`)

```python
def batch_udf(output_schema=None, checkpoint_file=None):
```

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `output_schema` | `pa.Schema` | `None` | Schema of the output batch (validated). If omitted, inferred from the first output batch. |
| `checkpoint_file` | `str \| Path` | `None` | Cache file for resumable UDF runs; on restart with the same file, resumes from last saved state. Can grow to a full data file's worth of results (multi-GB). |

Returns a `BatchUDF` instance (the `batch_udf` docstring's `Returns: AddColumnsUDF` is a stale doc label; the decorator's inner returns `BatchUDF(func, output_schema, checkpoint_file)`). The public exported symbol is `lance.batch_udf` (present in `lance/__init__.py`); `lance.add_columns_udf` is **not** an exported public API — it only appears as a stale cross-reference inside the `BatchUDF` class docstring.

#### Examples (verbatim from source docstring)

```python
import lance
import pyarrow as pa

table = pa.table({"a": [1, 2, 3]})
dataset = lance.write_dataset(table, "my_dataset")

# UDF form
@lance.batch_udf()
def double_a(batch):
    df = batch.to_pandas()
    return pd.DataFrame({'double_a': 2 * df['a']})
dataset.add_columns(double_a)

# SQL-expression form
dataset.add_columns({"triple_a": "a * 3"})
```

### 3.2 `alter_columns` — rename, retype, re-nullable

#### Verbatim signature

```python
def alter_columns(self, *alterations: Iterable[AlterColumn]):
```

Each alteration is a dict (`AlterColumn` TypedDict):

```python
class AlterColumn(TypedDict):
    path: str
    name: Optional[str]
    nullable: Optional[bool]
    data_type: Optional[pa.DataType]
```

| Key | Type | Meaning |
|---|---|---|
| `path` | `str` | Column path. Top-level = the name; nested = dot-separated (`"a.b.c"`). |
| `name` | `str`, optional | New name. Omitted = unchanged. |
| `nullable` | `bool`, optional | Change nullability. Non-nullable→nullable always allowed. Nullable→non-nullable only if no NULLs present, else error. |
| `data_type` | `pa.DataType`, optional | Cast target. Omitted = unchanged. |

**Index/cast rules (verbatim summary):** Renamed columns keep their indices. A column with an `IVF_PQ` index can keep it across a cast; **other index types cannot cast** and their indices are dropped on cast. Casts allowed within a general type family (int↔int, float↔float, string↔string) including up/downcast (downcast fails if values don't fit) and size variants (string↔large string, binary↔large binary, list↔large list).

```python
dataset.alter_columns({"path": "a", "name": "x"},
                      {"path": "b", "nullable": True})
dataset.alter_columns({"path": "x", "data_type": pa.int32()})
```

### 3.3 `drop_columns` — metadata-only

#### Verbatim signature

```python
def drop_columns(self, columns: List[str]):
```

| Parameter | Type | Meaning |
|---|---|---|
| `columns` | `List[str]` | Column names/paths to drop; nested allowed (`"a.b.c"`). |

**Metadata-only** — does **not** remove data from storage. To reclaim space, subsequently run `compact_files` (rewrite without the columns) then `cleanup_old_versions`. See [08_compaction_maintenance.md](08_compaction_maintenance.md).

```python
dataset.drop_columns(["a"])
```

### 3.4 `merge` — join precomputed columns in (left join)

#### Verbatim signature

```python
def merge(
    self,
    data_obj: ReaderLike,
    left_on: str,
    right_on: Optional[str] = None,
    schema=None,
):
```

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `data_obj` | `ReaderLike` | required | Right side of the join (DataFrame / Table / Dataset / Scanner / `Iterator[RecordBatch]` / `RecordBatchReader`). |
| `left_on` | `str` | required | Join column in the dataset. |
| `right_on` | `str` | `None` | Join column in `data_obj`; defaults to `left_on`. |
| `schema` | — | `None` | Optional schema for the reader. |

Left join: dataset is the left side. Rows in the dataset with no match get NULL for the new columns (error if a type disallows null). This is the bulk analog of `add_columns` when the new column values are already computed and keyed. See `LanceDataset.add_columns` for the compute-batch-by-batch alternative.

```python
new_df = pa.table({'x': [1, 2, 3], 'z': ['d', 'e', 'f']})
dataset.merge(new_df, 'x')   # adds column z, joined on x
```

> **Distinction:** `merge` (this method) = *add columns by join*. `merge_insert` (§2) = *add/update/delete rows by key*. They are unrelated despite the similar names.

---

## 4. `delete` — mark rows deleted

#### Verbatim signature

```python
def delete(
    self,
    predicate: Union[str, pa.compute.Expression],
    *,
    conflict_retries: int = 10,
    retry_timeout: timedelta = timedelta(seconds=30),
) -> DeleteResult:
```

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `predicate` | `str \| pa.compute.Expression` | required | SQL string or PyArrow expression selecting rows to delete. |
| `conflict_retries` | `int` | `10` | Retries under contention. |
| `retry_timeout` | `timedelta` | `30s` | Wall-clock retry cap; ≥1 attempt always runs. |

Returns `{'num_deleted_rows': int}` (`DeleteResult` TypedDict).

> **Soft delete:** rows are marked deleted (deletion vectors written); files are **not** physically rewritten, so existing indices stay valid. Reclaim space via compaction + cleanup.

```python
table = pa.table({"a": [1, 2, 3], "b": ["a", "b", "c"]})
dataset = lance.write_dataset(table, "example")
dataset.delete("a = 1 or b in ('a', 'b')")   # -> {'num_deleted_rows': 2}
```

Related: `truncate_table()` deletes all rows while preserving the schema and creating a new version.

---

## 5. `update` — SQL column updates in place

#### Verbatim signature

```python
def update(
    self,
    updates: Dict[str, str],
    where: Optional[str] = None,
    conflict_retries: int = 10,
    retry_timeout: timedelta = timedelta(seconds=30),
) -> UpdateResult:
```

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `updates` | `Dict[str, str]` | required | Column name → SQL expression to assign. |
| `where` | `str`, optional | `None` | SQL predicate selecting rows to update. `None` = all rows. |
| `conflict_retries` | `int` | `10` | Retries under contention. |
| `retry_timeout` | `timedelta` | `30s` | Wall-clock retry cap. |

Returns an update-count dict. (Note: the source defines `UpdateResult` with key `num_rows_updated`, while the `update` docstring example reads `num_updated_rows`. The `merge_insert`/`delete` results use `num_updated_rows` / `num_deleted_rows`; the exact key name returned by `update` is worth confirming against your pinned version — see the Unverified note.)

```python
table = pa.table({"a": [1, 2, 3], "b": ["a", "b", "c"]})
dataset = lance.write_dataset(table, "example")
dataset.update({"a": "a + 2"}, where="b != 'a'")
```

> `update` is for **bulk, expression-driven** column rewrites over a predicate. For **key-level** row replacement supplying new values from a source table, use `merge_insert` (§2).

---

## 6. `LanceOperation` — the low-level operation variants

`LanceOperation` is a namespace of dataclasses, each a `BaseOperation`, describing a change to commit via `LanceDataset.commit` (§7). These are the **advanced / distributed** API — a process (or many processes) writes fragments directly, then one commit makes the change visible. On a single machine, prefer `write_dataset` / the high-level methods above.

### The real variant set (verbatim class list)

| Operation | Fields (verbatim) | Purpose |
|---|---|---|
| `BaseOperation(ABC)` | — | Abstract base for all operations. |
| `Overwrite` | `new_schema: LanceSchema \| pa.Schema`, `fragments: Iterable[FragmentMetadata]`, `initial_bases: Optional[List[DatasetBasePath]] = None` | Overwrite or create a dataset from prebuilt fragments. `initial_bases` valid **CREATE mode only**. |
| `Append` | `fragments: Iterable[FragmentMetadata]` | Append new rows (new fragments) to the dataset. |
| `Delete` | `updated_fragments: Iterable[FragmentMetadata]`, `deleted_fragment_ids: Iterable[int]`, `predicate: str` | Remove rows (updated fragments carry new deletion vectors) or whole fragments (ids). |
| `Update` | `removed_fragment_ids: List[int]=[]`, `updated_fragments: List[FragmentMetadata]=[]`, `new_fragments: List[FragmentMetadata]=[]`, `fields_modified: List[int]=[]`, `fields_for_preserving_frag_bitmap: List[int]=[]`, `update_mode: str=""` | Update rows; `fields_modified` lists changed fields so covering indices can drop those fragments. |
| `Merge` | `fragments: Iterable[FragmentMetadata]`, `schema: LanceSchema \| pa.Schema` | Add columns without changing fragment structure (keeps existing indices). Passing a `pa.Schema` is **deprecated** — pass a `LanceSchema`. |
| `Restore` | `version: int` | Restore a previous dataset version. |
| `Rewrite` | `groups: Iterable[RewriteGroup]`, `rewritten_indices: Iterable[RewrittenIndex]` | Rewrite files+indices into new files+indices (compaction internals). Advanced/not general-use. |
| `CreateIndex` | `new_indices: List[Index]`, `removed_indices: List[Index]` | Create/replace an index on the dataset. |
| `DataReplacement` | `replacements: List[DataReplacementGroup]` | Replace existing data files in-place (each `DataReplacementGroup` = `fragment_id: int`, `new_file: DataFile`). |
| `Project` | `schema: LanceSchema` | Projection: drop / rename / swap columns via a new schema. |
| `UpdateConfig` | `config_updates: Optional[UpdateMap]`, `table_metadata_updates: Optional[UpdateMap]`, `schema_metadata_updates: Optional[UpdateMap]`, `field_metadata_updates: Optional[Dict[int, UpdateMap]]` | Update dataset config / table / schema / field metadata. |

Supporting dataclasses: `RewriteGroup(old_fragments, new_fragments)`, `RewrittenIndex(old_id, new_id, new_details_type_url, new_details_value, new_index_version)`, `DataReplacementGroup(fragment_id, new_file)`, `UpdateMap(updates: Dict[str, Optional[str]], replace: bool=False)` (a `None` value deletes the key; `replace=True` swaps the whole map).

### `Append` example (verbatim)

```python
import lance
import pyarrow as pa

tab1 = pa.table({"a": [1, 2], "b": ["a", "b"]})
dataset = lance.write_dataset(tab1, "example")

tab2 = pa.table({"a": [3, 4], "b": ["c", "d"]})
fragment = lance.fragment.LanceFragment.create("example", tab2)
operation = lance.LanceOperation.Append([fragment])
dataset = lance.LanceDataset.commit("example", operation,
                                    read_version=dataset.version)
```

---

## 7. `LanceDataset.commit` — low-level commit + optimistic concurrency

#### Verbatim signature

```python
def commit(
    base_uri: Union[str, Path, LanceDataset],
    operation: Union[LanceOperation.BaseOperation, Transaction],
    read_version: Optional[int] = None,
    commit_lock: Optional[CommitLock] = None,
    storage_options: Optional[Dict[str, str]] = None,
    enable_v2_manifest_paths: Optional[bool] = None,
    detached: Optional[bool] = False,
    max_retries: int = 20,
    *,
    commit_message: Optional[str] = None,
    enable_stable_row_ids: Optional[bool] = None,
    namespace_client: Optional["LanceNamespace"] = None,
    table_id: Optional[List[str]] = None,
    namespace_client_managed_versioning: bool = False,
    base_store_params: Optional[Dict[str, Dict[str, str]]] = None,
    commit_timeout: Optional[timedelta] = _DEFAULT_COMMIT_TIMEOUT,
) -> LanceDataset:
```

> `commit` is a `@staticmethod`-style entry point (first arg is `base_uri`, not `self`). It is called as `lance.LanceDataset.commit(...)`. There is also `LanceDataset.commit_batch(dest, transactions, ...)` for committing a sequence of `Transaction` objects (same concurrency params: `max_retries: int = 20`, `commit_timeout`).

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `base_uri` | `str \| Path \| LanceDataset` | required | Dataset URI or object (object reuses file-metadata cache). |
| `operation` | `LanceOperation.BaseOperation \| Transaction` | required | The change to apply (§6), or a `Transaction` from `execute_uncommitted`. |
| `read_version` | `int` | `None` | Base version the change was computed against. **Required for all operations except `Overwrite` and `Restore`** — omitting it otherwise raises `ValueError`. |
| `commit_lock` | `CommitLock` | `None` | Custom commit lock for stores lacking atomic commits. Must be callable. |
| `storage_options` | `Dict[str, str]` | `None` | Object-store connection params. |
| `enable_v2_manifest_paths` | `bool` | `None` | Use V2 manifest paths on a new dataset (default effectively `True`; unreadable by Lance < 0.17.0). No effect on existing datasets. |
| `detached` | `bool` | `False` | Commit outside dataset lineage — never becomes "latest"; only retrievable by its (random) version, which the caller must store. |
| `max_retries` | `int` | `20` | Max retries when the commit conflicts (OCC). |
| `commit_message` | `str` | `None` | Message stored in metadata (retrievable via `read_transaction()`). Not allowed when `operation` is a `Transaction` (set the message on the transaction instead). |
| `enable_stable_row_ids` | `bool` | `None` | Enable stable row ids on new-dataset create only. |
| `namespace_client` | `LanceNamespace` | `None` | Namespace client (pair with `table_id`). |
| `table_id` | `List[str]` | `None` | Table identifier within namespace. |
| `namespace_client_managed_versioning` | `bool` | `False` | Namespace manages versioning (commits go through the namespace API). |
| `base_store_params` | `Dict[str, Dict[str,str]]` | `None` | Runtime-only per-base store params. |
| `commit_timeout` | `timedelta` | `_DEFAULT_COMMIT_TIMEOUT` (30 min) | Max time for the commit incl. conflict retries. `None` disables. Must be positive. |

Returns a new `LanceDataset` at the new version.

### Optimistic concurrency, conflict detection, rebase/retry

Lance uses **optimistic concurrency control (OCC)**. The write model:

1. A writer computes its change against a known `read_version` (the base it read).
2. At commit, Lance attempts to atomically write the next manifest version (`read_version + 1`). Object stores that support atomic put-if-absent (or an external `commit_lock`) make this race-safe.
3. If another writer already claimed that version, this commit **conflicts**. Lance inspects whether the two operations are **compatible** — e.g. two independent `Append`s to disjoint fragments can be **rebased** onto the newer version and retried automatically; conflicting operations (e.g. overlapping deletes/updates) fail.
4. Retries repeat up to `max_retries` (default `20` for `commit`; the high-level `delete`/`update`/`merge_insert` paths default to `conflict_retries = 10`) or until `commit_timeout` (default 30 min).

Because appends are the most common concurrent case and are append-only, they rebase cleanly, which is what makes many-writer append-only ingestion safe.

### Distributed pattern with `execute_uncommitted`

```python
# Worker computes a merge but does not commit
txn, stats = ds.merge_insert("id") \
    .when_matched_update_all() \
    .when_not_matched_insert_all() \
    .execute_uncommitted(new_data)

# A coordinator commits the transaction (with OCC retry)
committed = lance.LanceDataset.commit(ds, txn, read_version=ds.version,
                                      max_retries=20)
```

> **Relevance to core-x:** For hundreds-of-millions-of-rows R2 ingestion, the high-level `write_dataset(mode="append")` / `merge_insert(...).execute()` paths already carry OCC + retries — you rarely touch `LanceOperation`/`commit` directly. Reach for the low-level API only when many Modal workers each build fragments independently and a single coordinator commits, or for distributed bulk update/rewrite. Keep `conflict_retries`/`max_retries` high and rely on append rebase for concurrent ingest into one R2 dataset. R2 credentials/endpoint flow through `storage_options` on both `write_dataset` and `commit` — see [07_storage_object_stores.md](07_storage_object_stores.md).

---

## 8. Deprecations, renames, and footguns

- **`use_legacy_format` is deprecated** — use `data_storage_version` (`"stable"` / `"legacy"` / explicit `"2.x"`). Passing it emits `DeprecationWarning`.
- **`LanceOperation.Merge(schema=...)` with a `pa.Schema` is deprecated** — pass a `LanceSchema` (`LanceSchema.from_pyarrow(...)`); a `pa.Schema` triggers `DeprecationWarning` and is auto-converted.
- **`enable_v2_manifest_paths=True` makes datasets unreadable by Lance < 0.17.0.** Default is `True` for new datasets; irrelevant for existing ones (migrate via `migrate_manifest_paths_v2()`).
- **`create` mode raises if the URI already exists.** Use `overwrite` to replace or `append` to add.
- **`delete`, `drop_columns`, and `update` do not reclaim storage.** Deletes are soft (deletion vectors), `drop_columns` is metadata-only. You must run `compact_files` + `cleanup_old_versions` to actually shrink storage.
- **`merge` (add-columns-by-join) vs `merge_insert` (upsert rows)** — same prefix, unrelated operations. Don't confuse them.
- **`merge_insert` reorders data** and randomizes insert order (hash-join internals). Do not depend on physical row order after an upsert.
- **`merge_insert(on=None)` requires a schema-metadata primary key**, else `ValueError`. Pass `on` explicitly unless a PK is configured.
- **`commit` requires `read_version`** for every operation except `Overwrite`/`Restore`, else `ValueError`.
- **`commit_message` cannot be combined with a `Transaction` operation** — set the message on the transaction's properties instead.
- **`when_matched_update_all(condition)` failing the condition does not reclassify the row** as not-matched; it simply leaves it unchanged.

---

## Unverified / needs confirmation

- **`update` return key name.** The `UpdateResult` TypedDict in `dataset.py` declares `num_rows_updated`, but the `update` docstring example writes `num_updated_rows`. The other mutating ops consistently use `num_updated_rows` / `num_deleted_rows` / `num_inserted_rows`. Confirm the exact dict key returned by `ds.update(...)` on your pinned `pylance` version before relying on it programmatically.
- **Released-vs-main signature drift.** Signatures here are from repo `main` (`9.0.0-beta.18`). PyPI released is `pylance 8.0.0`. Parameters flagged **[main-branch]** (`external_blob_mode`, `allow_external_blob_outside_bases`, `blob_pack_file_size_threshold`, and the namespace/`base_store_params`/`target_bases` family) may not all exist, or may have different defaults, on 8.0.0. Verify against the 8.0.0 `dataset.py` if pinning to the release.
- **`insert()` vs `write_dataset()` append semantics.** `LanceDataset.insert(mode="append")`'s docstring says append "raises if uri does not exist," while `write_dataset(mode="append")`'s docstring says it creates the dataset if missing. The free-function create-if-missing behavior is treated as authoritative here; confirm the instance-method behavior if it matters.

---

### Sibling files

- [00_overview.md](00_overview.md) — Lance & LanceDB — Overview, Ecosystem, Packaging & Versions
- [01_file_format.md](01_file_format.md) — The Lance Columnar File Format & On-Disk Dataset Layout
- [02_python_dataset_api.md](02_python_dataset_api.md) — pylance Python SDK — `lance.dataset`, `lance.write_dataset`, `LanceDataset`
- [04_versioning_time_travel.md](04_versioning_time_travel.md) — Versioning, Time Travel, Tags & `cleanup_old_versions`
- [05_scalar_indices.md](05_scalar_indices.md) — Scalar Indices — BTREE, BITMAP, LABEL_LIST, INVERTED/FTS, NGRAM
- [07_storage_object_stores.md](07_storage_object_stores.md) — Object Store Configuration — `storage_options` for S3 / Cloudflare R2 / GCS / Azure
- [08_compaction_maintenance.md](08_compaction_maintenance.md) — Dataset Maintenance — compaction, index optimization, fragment management
- [09_scanning_filtering.md](09_scanning_filtering.md) — Scanning, Filtering, Projection Pushdown & `take()`
- [10_duckdb_arrow_interop.md](10_duckdb_arrow_interop.md) — Interop — Apache Arrow, DuckDB, Polars/pandas
- [11_lancedb_table_api.md](11_lancedb_table_api.md) — LanceDB (the database) — connect, tables, add/search, FTS, cloud/remote
