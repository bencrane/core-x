# Dataset Maintenance — compaction, index optimization, fragment management

> Canonical upstream reference — fetched 2026-07-08 from official Lance documentation and source repositories. Documents upstream behavior as-is; this is NOT core-x doctrine. Treat the version numbers and API signatures here as fetched-date ground truth.
>
> Primary sources:
> - https://raw.githubusercontent.com/lancedb/lance/main/python/python/lance/dataset.py — verbatim `DatasetOptimizer.compact_files` / `optimize_indices` signatures and docstrings; `write_dataset` signature (`max_rows_per_file`, `max_rows_per_group`, `max_bytes_per_file`).
> - https://raw.githubusercontent.com/lancedb/lance/main/python/python/lance/optimize.py — `CompactionOptions` TypedDict (full option list + defaults) and the `lance.optimize` re-exports (`Compaction`, `CompactionMetrics`, `CompactionPlan`, `CompactionTask`, `RewriteResult`).
> - https://raw.githubusercontent.com/lancedb/lance/main/python/src/dataset/optimize.rs — authoritative `CompactionMetrics` attributes and the `Compaction.execute` / `.plan` / `.commit` static-method surface for distributed compaction.
> - https://lance.org/guide/performance/ and https://lance.org/guide/data_evolution/ — narrative behavior (skip-if-above-target, deletion materialization, index invalidation on rewrite, `defer_index_remap` / Fragment Reuse Index).
>
> Versions as of 2026-07-08: **pylance 8.0.0** (PyPI `pylance`, imported as `lance`), **lancedb 0.34.0**, **duckdb 1.5.4**. Signatures below are from `main` and match the 8.x line; where a param is version-gated it is flagged inline.

Scope: how to maintain a Lance dataset over its life — compacting the many small fragments and deletion vectors that accumulate from appends/deletes, incrementally folding new rows into existing indices, sizing fragments at write time to avoid the problem, and where compaction hands off to version cleanup.

---

## 1. Why maintenance is required

A Lance dataset is an **append-only, immutable-fragment** store. Every write operation (append, `merge_insert`, `update`, `delete`, `add_columns`) produces a new manifest version; data files are never edited in place. This design gives cheap time travel and atomic commits, but it means three kinds of debt accumulate:

1. **Fragment proliferation (small files).** Each append writes at least one new fragment. A pipeline that appends in small batches produces hundreds or thousands of tiny fragments. Scans pay per-fragment overhead (open, metadata read, statistics load) that dominates when fragments are small, so scan throughput degrades roughly with fragment count rather than row count.

2. **Unindexed new rows.** Indices (scalar BTREE/BITMAP/etc. and vector IVF_PQ/HNSW) are **not** updated automatically when new fragments are appended. A query against an indexed column runs an indexed lookup over the old data **plus** a brute-force scan over every unindexed fragment. As unindexed data grows, this "flat tail" dominates latency.

3. **Deletion vectors.** `delete` and the delete-half of `merge_insert`/`update` are *soft*: rows are marked in a per-fragment deletion file, not physically removed. Every scan must read and apply these deletion vectors to skip tombstoned rows. They accumulate until a fragment is rewritten.

Compaction addresses (1) and (3); `optimize_indices` addresses (2). They are complementary and are typically run together after a batch of appends.

> Relevance to core-x: the R2-hosted Lance datasets under `s3://data-sink/active/` are written by incremental DuckDB→Arrow→Lance appends. Each append lands new immutable fragments and leaves any BTREE scalar indices on resolution keys covering only the pre-append rows. Without a periodic `compact_files()` + `optimize.optimize_indices()` pass, resolution-key lookups silently degrade into a flat scan over the unindexed tail, and per-fragment overhead on R2 (where each fragment open is a network round-trip) compounds the cost. Maintenance is not optional housekeeping on this plane — it is what keeps indexed lookups indexed.

---

## 2. The `dataset.optimize` namespace

`LanceDataset.optimize` is a property returning a `DatasetOptimizer` bound to the dataset (source: `dataset.py`):

```python
@property
def optimize(self) -> "DatasetOptimizer":
    return DatasetOptimizer(self)
```

The `DatasetOptimizer` surface, as it actually exists on `main` (pylance 8.x):

| Member | Kind | Purpose |
| --- | --- | --- |
| `compact_files(...)` | method | Merge small fragments, drop deleted rows, drop dropped columns. Returns `CompactionMetrics`. |
| `optimize_indices(**kwargs)` | method | Incrementally fold new fragments into existing indices (scalar + vector). |
| `enable_auto_cleanup(auto_cleanup_config, **kwargs)` | method | Write `lance.auto_cleanup.*` manifest config so old versions are auto-pruned. |
| `disable_auto_cleanup(**kwargs)` | method | Delete the `lance.auto_cleanup.*` config keys. |

That is the complete method list on `DatasetOptimizer` in the fetched source. Version cleanup itself (`cleanup_old_versions`, `cleanup_partial_writes`) lives on `LanceDataset` directly, **not** under `.optimize` — see [`04_versioning_time_travel.md`](04_versioning_time_travel.md).

The lower-level building blocks (`Compaction`, `CompactionMetrics`, `CompactionPlan`, `CompactionTask`, `RewriteResult`) live in the `lance.optimize` module (section 5).

---

## 3. `compact_files` — merge small fragments

### 3.1 Verbatim signature (source: `dataset.py`)

```python
def compact_files(
    self,
    *,
    target_rows_per_fragment: Optional[int] = None,
    max_rows_per_group: Optional[int] = None,
    max_bytes_per_file: Optional[int] = None,
    materialize_deletions: Optional[bool] = None,
    materialize_deletions_threshold: Optional[float] = None,
    defer_index_remap: Optional[bool] = None,
    num_threads: Optional[int] = None,
    batch_size: Optional[int] = None,
    compaction_mode: Optional[
        Literal["reencode", "try_binary_copy", "force_binary_copy"]
    ] = None,
    binary_copy_read_batch_bytes: Optional[int] = None,
) -> CompactionMetrics:
```

Note the leading `*`: **every argument is keyword-only.** `dataset.optimize.compact_files(1_000_000)` raises `TypeError`; you must write `compact_files(target_rows_per_fragment=1_000_000)`.

### 3.2 Parameter table

All defaults are `None` at the Python layer; a `None` value means "fall back to the dataset manifest config (`lance.compaction.<key>`) if set, else the hardcoded default listed below." Precedence: **explicit argument > manifest config > hardcoded default**.

| Parameter | Type | Python default | Effective default | Meaning |
| --- | --- | --- | --- | --- |
| `target_rows_per_fragment` | `int`, optional | `None` | `1024 * 1024` (1,048,576) | Target rows per fragment after compaction. Fragments already **above** this are skipped, not rewritten. |
| `max_rows_per_group` | `int`, optional | `None` | `1024` | Max rows per group in rewritten files. Does not affect *which* fragments compact, only how selected ones are re-written. **Legacy storage format only** — the v2+ format has no row groups and ignores this. |
| `max_bytes_per_file` | `int`, optional | `None` | inherits `write_dataset` default (90 GB) | Max bytes per output file. A too-small value can leave fragments smaller than `target_rows_per_fragment`. |
| `materialize_deletions` | `bool`, optional | `None` | `True` | Whether to physically remove soft-deleted rows (drop the deletion file) when a fragment is rewritten. |
| `materialize_deletions_threshold` | `float`, optional | `None` | `0.1` (10%) | Fraction of a fragment's original rows that must be soft-deleted before deletion count alone makes it a compaction candidate. |
| `defer_index_remap` | `bool`, optional | `None` | `False` | Defer index remapping; instead records a **Fragment Reuse Index** so indices are remapped lazily/later. See §3.5. |
| `num_threads` | `int`, optional | `None` | number of machine cores | Compaction worker threads. |
| `batch_size` | `int`, optional | `None` | scanner default | Rows per batch when scanning input fragments. Lower it if compaction OOMs. |
| `compaction_mode` | `"reencode"` / `"try_binary_copy"` / `"force_binary_copy"` | `None` | `"reencode"` | `reencode` decodes and re-encodes data. `try_binary_copy` copies file bytes directly when fragments are compatible (much faster, no re-encode) and falls back to `reencode` otherwise. `force_binary_copy` uses binary copy or errors. |
| `binary_copy_read_batch_bytes` | `int`, optional | `None` | 16 MB | Bytes read per batch during binary-copy operations. |

`CompactionOptions` (the TypedDict in `optimize.py`) exposes two additional keys not surfaced as `compact_files` kwargs but honored via manifest config / the lower-level `Compaction` API: **`io_buffer_size`** (bytes queued in the scan I/O buffer; raising it avoids a deadlock when a single batch exceeds the buffer) and **`max_source_fragments`** (cap on source fragments compacted in one run, oldest-first, for incremental compaction — e.g. 20 at a time).

> Footgun — typo'd config key: the `CompactionOptions` TypedDict in upstream `optimize.py` spells the threshold field **`materialize_deletions_threadhold`** (sic — misspelled in source). The `compact_files` kwarg and the manifest config key `lance.compaction.materialize_deletions_threshold` are spelled correctly; only the TypedDict field name carries the typo. Use the kwarg.

### 3.3 What it does

Per the docstring, `compact_files`:
- **Removes deleted rows** from fragments (when `materialize_deletions=True`).
- **Removes dropped columns** from fragments (columns removed via `add_columns` inverse / schema evolution are physically dropped on rewrite).
- **Merges small fragments** into larger ones targeting `target_rows_per_fragment`.

It **preserves insertion order.** Fragments merge by fragment id, so the dataset's inherent ordering is retained. A consequence: an isolated small fragment sandwiched between two already-large fragments will **not** be compacted, because its neighbors do not themselves need compaction. Example from the docstring: fragments of 5M / 100 / 5M rows leave the 100-row fragment untouched.

### 3.4 Manifest config defaults

You can bake compaction defaults into the dataset manifest so every future `compact_files()` call inherits them without passing kwargs. Keys are prefixed `lance.compaction.`:

`target_rows_per_fragment`, `max_rows_per_group`, `max_bytes_per_file`, `materialize_deletions`, `materialize_deletions_threshold`, `defer_index_remap`, `batch_size`, `compaction_mode`, `binary_copy_read_batch_bytes`.

Example: setting `lance.compaction.target_rows_per_fragment` to `"500000"` makes 500,000 the default target. Values are stored as strings.

### 3.5 Index invalidation and `defer_index_remap`

**Rewriting fragments invalidates row addresses.** When `compact_files` rewrites a fragment, the physical row addresses inside it change. Any ANN/vector index that referenced the old addresses no longer covers those rows — the affected fragments effectively drop out of the index. Upstream guidance: **compact first, then (re)build/optimize indices**, not the reverse.

By default (`defer_index_remap=False`) compaction remaps indices inline so the resulting version's indices stay valid. Setting `defer_index_remap=True` instead records a **Fragment Reuse Index (FRI)**: index remapping is deferred, making the compaction commit cheaper, at the cost of a follow-up remap step. Use the default unless you have a specific distributed/large-scale reason to defer.

### 3.6 Minimal example

```python
import lance

ds = lance.dataset(
    "s3://data-sink/active/entities_lance",
    storage_options={
        "endpoint": "https://<accountid>.r2.cloudflarestorage.com",
        "aws_access_key_id": "<key>",
        "aws_secret_access_key": "<secret>",
        "region": "auto",
    },
)

metrics = ds.optimize.compact_files(
    target_rows_per_fragment=1024 * 1024,   # 1M rows/fragment
    materialize_deletions=True,             # physically drop tombstoned rows
)
print(metrics)
# CompactionMetrics(fragments_removed=812, fragments_added=6,
#                   files_removed=812, files_added=6)
```

`compact_files` commits a new dataset version; the `ds` handle is advanced to it in place.

---

## 4. `optimize_indices` — fold new rows into existing indices

### 4.1 Verbatim signature (source: `dataset.py`)

```python
def optimize_indices(self, **kwargs):
```

The public method takes `**kwargs` and forwards them to the native `self._dataset._ds.optimize_indices(**kwargs)`. The documented, accepted keyword arguments are:

| Parameter | Type | Default | Meaning |
| --- | --- | --- | --- |
| `num_indices_to_merge` | `int`, optional | `None` | Number of delta indices to merge. `0` creates a **new delta index** for the unindexed data instead of merging into existing ones. |
| `index_names` | `List[str]`, optional | `None` | Names of indices to optimize. `None` optimizes **all** indices. |
| `retrain` | `bool` | `False` (**deprecated**) | If `True`, retrains the whole index from current data — ignores `num_indices_to_merge` and merges everything into one. Marked *deprecated* in the docstring. |

### 4.2 What it does

New data appended after an index was built is **not** indexed automatically. Until optimized, queries pay an indexed search over old data plus a brute-force search over the new, unindexed fragments — latency that grows with the unindexed tail.

`optimize_indices` **assigns the new rows to the existing index structure** (for a vector index, to existing IVF partitions; for scalar indices, extends the index). Critically, it **does not retrain** the index — it only places new data into current partitions/structures. That makes it far cheaper than rebuilding, at the cost of some accuracy drift if the new data's distribution has shifted from what the index was trained on (new clusters, new value ranges).

It works incrementally by producing **delta indices**. `num_indices_to_merge` controls how aggressively those deltas fold together; leaving deltas separate keeps optimization cheap, merging them keeps read paths consolidated. When drift is severe enough that accuracy matters, rebuild the index from scratch (`create_index(..., replace=True)`) rather than relying on `optimize_indices` — see scalar/vector index files.

### 4.3 Minimal example

```python
# after appending new fragments and (optionally) compacting:
ds.optimize.optimize_indices()                       # optimize all indices
ds.optimize.optimize_indices(index_names=["id_idx"]) # just one
ds.optimize.optimize_indices(num_indices_to_merge=0) # new delta index only
```

Ordering when both are needed: **`compact_files` → `optimize_indices`.** Compaction rewrites fragments and shifts row addresses; running index optimization afterward folds the settled fragments into the indices cleanly. Doing it in the other order wastes work, since compaction would then invalidate what you just indexed.

---

## 5. Lower-level: the `lance.optimize` module

`compact_files` is a thin wrapper over `lance.optimize.Compaction.execute`. The `lance.optimize` module (source: `optimize.py`) re-exports these native classes:

```python
from .lance import Compaction as Compaction
from .lance import CompactionMetrics as CompactionMetrics
from .lance import CompactionPlan as CompactionPlan
from .lance import CompactionTask as CompactionTask
from .lance import RewriteResult as RewriteResult
```

### 5.1 `Compaction` — single-process and distributed compaction (source: `optimize.rs`)

`Compaction` is a class of **static methods** for running compaction, including distributed:

| Static method | Signature (conceptual) | Returns | Purpose |
| --- | --- | --- | --- |
| `Compaction.execute` | `execute(dataset, options)` | `CompactionMetrics` | Run a full compaction in-process. This is exactly what `DatasetOptimizer.compact_files` calls. Advances the dataset to the new version. |
| `Compaction.plan` | `plan(dataset, options)` | `CompactionPlan` | Produce a plan (a set of `CompactionTask`s) **without executing** — for distributed compaction. |
| `Compaction.commit` | `commit(dataset, rewrites, options=None)` | `CompactionMetrics` | Commit the `RewriteResult`s produced by executing tasks. You need not pass all original tasks — a subset (e.g. those that completed before a deadline) is valid. |

Distributed model (from the module docstring): `plan()` yields `CompactionTask` objects that are picklable and shippable to worker processes; each task's `.execute()` produces a picklable `RewriteResult`; results are shipped back and passed to `commit()`. `options` on `commit` defaults to `CompactionOptions::default()` when omitted/`None`.

Most pipelines never touch this — `dataset.optimize.compact_files(...)` is the single-process path. Reach for `plan`/`execute`/`commit` only when compaction must be fanned out across machines.

### 5.2 `CompactionMetrics` — the return value (source: `optimize.rs`)

`CompactionMetrics` (module `lance.optimize`) exposes exactly four integer attributes:

| Attribute | Type | Meaning |
| --- | --- | --- |
| `fragments_removed` | `int` | Number of fragments removed (the small ones that were merged away). |
| `fragments_added` | `int` | Number of new, larger fragments created. |
| `files_removed` | `int` | Number of data files removed. |
| `files_added` | `int` | Number of data files added. |

Its `repr` is `CompactionMetrics(fragments_removed=…, fragments_added=…, files_removed=…, files_added=…)`. If nothing needed compacting, all four are `0`.

### 5.3 `CompactionOptions` TypedDict

`CompactionOptions` (a `TypedDict` in `optimize.py`) is the option bag consumed by the native `Compaction` methods. Its fields mirror the `compact_files` kwargs plus `io_buffer_size` and `max_source_fragments` (see §3.2), and it carries the misspelled `materialize_deletions_threadhold` field noted above.

`CompactionPlan`, `CompactionTask`, and `RewriteResult` are the picklable plan/task/result objects of the distributed path; their attribute surfaces are stable enough for shipping across processes but are not documented in detail in the fetched Python sources (they are native `#[pyclass]` types — see `optimize.rs`).

---

## 6. Prevention at write time — `max_rows_per_file`

The cheapest small-fragment problem is the one you never create. `lance.write_dataset` controls output fragment sizing directly.

### 6.1 Relevant `write_dataset` parameters (source: `dataset.py`)

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
    ...
) -> LanceDataset:
```

| Parameter | Type | Default | Meaning |
| --- | --- | --- | --- |
| `max_rows_per_file` | `int` | `1024 * 1024` (1,048,576) | Max rows written before a new file/fragment starts. Governs fragment size directly. |
| `max_rows_per_group` | `int` | `1024` | Max rows per group within a file (legacy format only). |
| `max_bytes_per_file` | `int` | `90 * 1024 * 1024 * 1024` (90 GB) | Soft byte cap per file, checked after each group — large groups can overshoot. 90 GB default sits under object stores' 100 GB hard per-file limit. |

### 6.2 Guidance

- **Batch appends, don't drip them.** The single biggest lever against fragment proliferation is writing larger batches per append. An append of 5M rows with default `max_rows_per_file` produces ~5 fragments; the same 5M rows appended in 5,000 tiny batches produces thousands of fragments and a mandatory compaction pass.
- Keep `max_rows_per_file` near the default (1M) unless rows are very wide (raise `max_bytes_per_file` awareness) or you need finer fragment granularity. Very large `max_rows_per_file` bounded by the 90 GB byte cap yields fewer, bigger fragments and fewer files to open on scan.
- `max_rows_per_group` only matters on the **legacy** storage format; the v2+ format (`data_storage_version="stable"`/`"2.0"`+) has no row groups.

> Relevance to core-x: on the DuckDB→Arrow→Lance path, the natural unit is a full DuckDB query result streamed as an Arrow `RecordBatchReader`. Sizing each append so it lands a small number of ~1M-row fragments (rather than one fragment per micro-batch) keeps the post-ingest compaction bill small. For out-of-core DuckDB producing more rows than RAM, control memory with `memory_limit` and `temp_directory` on the DuckDB side and stream the reader into `write_dataset(mode="append", max_rows_per_file=1024*1024)` — Lance sizes fragments from the stream, so a right-sized `max_rows_per_file` is the write-time prevention that shrinks the later `compact_files` pass.

---

## 7. Cleanup handoff — old versions

Compaction removes old *fragments* logically, but the old data files remain on disk as long as older manifest versions reference them (time-travel guarantee). Reclaiming that storage is a **separate** step: `LanceDataset.cleanup_old_versions(...)`, plus `cleanup_partial_writes()` for orphaned files from failed writes, and the `lance.auto_cleanup.*` config (settable via `dataset.optimize.enable_auto_cleanup(...)`) for automatic pruning.

Full signatures, `older_than`/`delete_unverified` semantics, tag protection, and the R2 caveats live in [`04_versioning_time_travel.md`](04_versioning_time_travel.md). The canonical maintenance sequence:

```
append(s)  →  compact_files()  →  optimize_indices()  →  cleanup_old_versions()
```

---

## 8. Recommended maintenance recipe (after incremental appends)

```python
import lance

ds = lance.dataset(uri, storage_options=STORAGE_OPTIONS)

# 1. Merge the small fragments this batch of appends produced,
#    and physically drop soft-deleted rows.
m = ds.optimize.compact_files(
    target_rows_per_fragment=1024 * 1024,
    materialize_deletions=True,
)
print(m)  # fragments_removed / fragments_added / files_removed / files_added

# 2. Fold the (now settled) new fragments into existing indices.
#    Cheap: assigns new rows to existing partitions, does not retrain.
ds.optimize.optimize_indices()

# 3. Reclaim storage from superseded versions (see 04). Respect any tags
#    and time-travel windows you depend on before choosing `older_than`.
import datetime
ds.cleanup_old_versions(older_than=datetime.timedelta(days=7))
```

> Relevance to core-x: this is the exact post-ingest tail for the R2 datasets. Run it after each incremental append cycle so (a) BTREE scalar indices on resolution keys keep covering the full dataset instead of degrading to a flat scan over the newest fragments, (b) deletion vectors from upsert/merge cycles don't accumulate into per-scan overhead, and (c) R2 storage of superseded versions gets reclaimed. Compaction ordering matters: always `compact_files` **before** `optimize_indices`, because rewriting fragments shifts row addresses and would otherwise invalidate freshly-optimized indices.

---

## 9. Footguns and deprecations

- **`compact_files` args are keyword-only** (leading `*`). Positional calls raise `TypeError`.
- **Compact before you index.** Rewriting fragments invalidates row addresses; run `compact_files` first, then `optimize_indices`/rebuild. The reverse wastes the indexing work.
- **`materialize_deletions_threadhold`** is misspelled in the `CompactionOptions` TypedDict (source `optimize.py`). The `compact_files` kwarg and the `lance.compaction.materialize_deletions_threshold` manifest key are spelled correctly — prefer the kwarg.
- **`optimize_indices(retrain=...)` is deprecated** (per the docstring). For a genuine retrain use `create_index(..., replace=True)` on the target column.
- **`max_rows_per_group` is legacy-format-only.** On the v2+ storage format it is inert; don't tune it expecting fragment-size effects (that's `max_rows_per_file` / `target_rows_per_fragment`).
- **Isolated small fragments survive compaction.** Because insertion order is preserved and fragments merge only with adjacent candidates, a lone small fragment between two large ones is left as-is. This is expected, not a bug.
- **`compact_files` does not reclaim disk.** It creates a new version; the old files linger until `cleanup_old_versions` (§7) runs.
- **`binary_copy` modes require compatible fragments.** `force_binary_copy` errors if fragments aren't binary-compatible (e.g. differing encodings/schema evolution); `try_binary_copy` silently falls back to `reencode`.

---

## 10. Unverified / needs confirmation

- **`CompactionPlan` / `CompactionTask` / `RewriteResult` attribute surfaces** are native `#[pyclass]` types re-exported from Rust (`optimize.rs`). The distributed `plan → execute → commit` flow and their picklability are documented in the module docstring, but the exhaustive per-attribute list of these three classes was not fully enumerable from the fetched Python sources. Confirm against the built package's stubs (`lance/optimize.pyi`) if you build a distributed compaction driver.
- **`io_buffer_size` and `max_source_fragments`** appear in the `CompactionOptions` TypedDict but are **not** surfaced as explicit `compact_files` kwargs on `main`. They are honored via manifest config (`lance.compaction.*`) and the lower-level `Compaction` API; whether a future pylance release promotes them to `compact_files` kwargs is unconfirmed.
- **Version gating of `compaction_mode` / `binary_copy_*` / `defer_index_remap`.** These are present on `main` (8.x). The exact minor release that introduced binary-copy compaction and the Fragment Reuse Index was not pinned from the fetched sources — treat them as "current 8.x" and check the changelog if targeting an older pinned pylance.

---

## Related files

- [`00_overview.md`](00_overview.md) — Lance & LanceDB overview, ecosystem, packaging & versions
- [`01_file_format.md`](01_file_format.md) — Lance columnar file format & on-disk dataset/fragment layout
- [`02_python_dataset_api.md`](02_python_dataset_api.md) — `lance.dataset`, `write_dataset`, `LanceDataset`
- [`03_writes_appends_upserts.md`](03_writes_appends_upserts.md) — write modes, append, `merge_insert`, `delete`, `update`, `add_columns`, commits
- [`04_versioning_time_travel.md`](04_versioning_time_travel.md) — versioning, time travel, tags & `cleanup_old_versions` (the cleanup step compaction hands off to)
- [`05_scalar_indices.md`](05_scalar_indices.md) — BTREE, BITMAP, LABEL_LIST, INVERTED/FTS, NGRAM (what `optimize_indices` maintains)
- [`06_vector_search.md`](06_vector_search.md) — IVF_PQ / HNSW, and why compaction invalidates ANN indices
- [`07_storage_object_stores.md`](07_storage_object_stores.md) — `storage_options` for S3 / Cloudflare R2 / GCS / Azure
- [`09_scanning_filtering.md`](09_scanning_filtering.md) — scanning, filtering, projection pushdown & `take()`
- [`10_duckdb_arrow_interop.md`](10_duckdb_arrow_interop.md) — Arrow / DuckDB / Polars interop feeding `write_dataset`
- [`11_lancedb_table_api.md`](11_lancedb_table_api.md) — LanceDB table API (`optimize` also exists at the table level)
