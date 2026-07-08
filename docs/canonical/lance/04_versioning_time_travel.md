# Versioning, Time Travel, Tags & cleanup_old_versions

> Canonical upstream reference — fetched 2026-07-08 from official Lance documentation and source repositories. Documents upstream behavior as-is; this is NOT core-x doctrine. Treat the version numbers and API signatures here as fetched-date ground truth.
>
> Primary sources:
> - https://raw.githubusercontent.com/lancedb/lance/main/python/python/lance/dataset.py — `LanceDataset` methods (`versions`, `version`, `latest_version`, `checkout_version`, `checkout_latest`, `restore`, `cleanup_old_versions`, `explain_cleanup_old_versions`), the `Tags` class, and the `Tag` / `Version` TypedDicts. Signatures below are quoted verbatim from this file (`main` branch, HEAD as of 2026-07-08).
> - https://raw.githubusercontent.com/lancedb/lance/main/python/python/lance/__init__.py — the module-level `lance.dataset()` opener with `version=` / `asof=` time-travel parameters and the `__all__` export list.
> - https://raw.githubusercontent.com/lancedb/lance/main/python/python/lance/lance/__init__.pyi — the compiled-extension type stubs; source of the `CleanupStats` and `CleanupExplanation` dataclass field lists.
> - https://pypi.org/pypi/pylance/json , https://pypi.org/pypi/lancedb/json , https://pypi.org/pypi/duckdb/json — current released version numbers.
> - https://lancedb.github.io/lance/ — Lance docs site (landing / navigation; the `/api/python.html` reference page returned HTTP 404 at fetch time, so all signatures here come from source rather than the rendered API page).

Scope: How Lance models dataset history as an append-only sequence of immutable versions, how to read old versions (time travel by version number or timestamp), how to tag and restore versions, and how `cleanup_old_versions()` reclaims storage while respecting tags.

---

## Current released versions (as of 2026-07-08)

| Package | Version | Notes |
|---|---|---|
| `pylance` (the Python `lance` module) | **8.0.0** | Released 2026-07-01. All signatures in this file are from the `main` branch at fetch time, which corresponds to the 8.x line. |
| `lancedb` (the higher-level DB) | **0.34.0** | Wraps pylance; see `11_lancedb_table_api.md`. |
| `duckdb` | **1.5.4** | Reads Lance via Arrow zero-copy; see `10_duckdb_arrow_interop.md`. |

The `import lance` module is the PyPI package `pylance` — there is no package literally named `lance` on PyPI. Import as `import lance`; the installed version string is `lance.__version__`.

---

## 1. The versions model

Every write to a Lance dataset — `create`, `append`, `overwrite`, `merge_insert`, `delete`, `update`, `add_columns`, index creation, compaction, tag creation — produces a **new manifest** and increments the dataset version by one. Versions are **monotonic integers starting at 1**. Data fragments are immutable and content-addressed; a new version references the fragments it needs (old and new) rather than mutating anything in place. This is what makes time travel cheap: an old version is just an old manifest pointing at fragments that still exist on disk.

> Relevance to core-x: this is the load-bearing property behind daily overwrite snapshots. An `mode="overwrite"` write of the day's build does **not** delete yesterday's fragments — it writes a new manifest (version N+1) that references only the new fragments. Yesterday's snapshot remains addressable as version N until `cleanup_old_versions()` reclaims it. Append-only immutable fragments mean a reader pinned to version N is never torn by a concurrent overwrite.

### `LanceDataset.versions()`

```python
def versions(self):
    """
    Return all versions in this dataset.
    """
```

Returns a `list` of dicts, one per version, ordered oldest → newest. Each dict has the shape of the `Version` TypedDict:

```python
class Version(TypedDict):
    version: int
    timestamp: int | datetime
    metadata: Dict[str, str]
```

- `version` — the integer version number.
- `timestamp` — a Python `datetime` (converted from the on-disk nanosecond timestamp; **Python `datetime` is microsecond-precision**, so the sub-microsecond part of the true ns timestamp is truncated — see the source `TODO`).
- `metadata` — arbitrary key/value strings attached at commit time.

```python
import lance
ds = lance.dataset("s3://bucket/data.lance")
for v in ds.versions():
    print(v["version"], v["timestamp"], v["metadata"])
```

### `LanceDataset.version` (property)

```python
@property
def version(self) -> int:
    """
    Returns the currently checked out version of the dataset
    """
```

The version this handle is pinned to. For a handle opened with no `version=`/`asof=`, this equals `latest_version` at open time.

### `LanceDataset.latest_version` (property)

```python
@property
def latest_version(self) -> int:
    """
    Returns the latest version of the dataset.
    """
```

The newest committed version on the (current branch of the) dataset. Note `version` and `latest_version` diverge whenever you have checked out an older version, or whenever another writer has committed since you opened the handle.

---

## 2. Time-travel reads

Two ways to open a dataset at a point in the past, and two ways to move an existing handle.

### Opening a specific version — `lance.dataset(uri, version=...)`

Verbatim signature (module-level opener, from `__init__.py`):

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

Time-travel-relevant parameters:

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `version` | `int \| str`, optional | `None` (→ latest) | If an **int**, load that version number. If a **str**, resolve it as a **tag name** and load the version the tag points to. |
| `asof` | `datetime \| str` (`ts_types`), optional | `None` | Load the **latest version created on or earlier than** this timestamp. **Ignored if `version` is also specified.** Accepts a `datetime` or a timestamp string (sanitized internally). Raises if the cutoff is earlier than the first version. |

```python
# By version number
ds_v5 = lance.dataset("s3://bucket/data.lance", version=5)

# By tag name (string is resolved as a tag)
ds_prod = lance.dataset("s3://bucket/data.lance", version="v2-prod-20250203")

# By timestamp — latest version at or before that instant
from datetime import datetime
ds_asof = lance.dataset("s3://bucket/data.lance",
                        asof=datetime(2026, 7, 1, 0, 0, 0))
```

> Footgun: `version` and `asof` are mutually exclusive in effect — if you pass both, `asof` is silently ignored. There is no error; pass exactly one.

> Note on `lance.open`: several docstrings in the source use `lance.open("dataset.lance")` in their examples. `open` is **not** exported in `__all__` and is **not** the canonical top-level opener. Use `lance.dataset(...)`. Treat `lance.open` in upstream examples as shorthand for `lance.dataset`.

### Moving an existing handle — `checkout_version()`

```python
def checkout_version(
    self, version: int | str | Tuple[Optional[str], Optional[int]]
) -> "LanceDataset":
    """
    Load the given version of the dataset.

    Unlike the :func:`dataset` constructor, this will re-use the
    current cache.
    This is a no-op if the dataset is already at the given version.
    ...
    """
```

| Argument form | Meaning |
|---|---|
| `int` | A version number on the **current branch**. |
| `str` | A **tag name**. This is the "checkout by tag" path. |
| `Tuple[Optional[str], Optional[int]]` | `(branch, version)` — a version number in a **specified branch**. `(None, None)` = latest version on the main branch. |

Returns a **new** `LanceDataset` handle pinned to that version (it `copy.copy`s `self` and swaps the inner dataset). The original handle is unchanged. Because it reuses the existing cache, `checkout_version` is cheaper than re-opening with `lance.dataset(uri, version=...)` when you already hold a handle.

```python
ds = lance.dataset("s3://bucket/data.lance")
ds_old = ds.checkout_version(3)            # by version number
ds_tag = ds.checkout_version("nightly")    # by tag name
```

### `checkout_latest()`

```python
def checkout_latest(self):
    """Check out the latest version of the current branch."""
```

Mutates the handle **in place** to point at the newest committed version — the way to pick up commits made by another writer after you opened the handle. (Contrast `checkout_version`, which returns a new handle.)

```python
ds = lance.dataset("s3://bucket/data.lance")  # pinned to version at open time
# ... another process appends ...
ds.checkout_latest()                          # now sees the new version
```

---

## 3. Restore — promote an old version to the head

```python
def restore(self):
    """
    Restore the currently checked out version as the latest version of the dataset.

    This creates a new commit.
    """
```

`restore()` does **not** roll back by deleting newer versions. It takes the version the handle is currently checked out to and writes it as a **new commit** at the head. History is preserved; you gain a new latest version whose contents equal the checked-out one.

```python
ds = lance.dataset("s3://bucket/data.lance")
ds = ds.checkout_version(3)   # go back to version 3
ds.restore()                  # version 3's state becomes the new latest (e.g. version 8)
# versions 4..7 still exist and are still checkout-able until cleaned up
```

> Common footgun: `checkout_version()` alone changes only your in-memory handle — it does **not** change what other readers see. To make an old version the canonical head for everyone, `checkout_version(N)` then `restore()`.

---

## 4. Tags — named, human-readable pointers to versions

Version numbers are opaque. Tags give a stable string name to a specific version (like a Git tag). **Tagged versions are exempt from `cleanup_old_versions()`** — this is the primary mechanism for protecting a version you never want garbage-collected.

Access via the `ds.tags` property, which returns a `Tags` manager:

```python
@property
def tags(self) -> Tags:
    """Tag management for the dataset.

    Similar to Git, tags are a way to add metadata to a specific version of the
    dataset.
    ...
    """
```

### The `Tags` class API

```python
class Tags:
    """Dataset tag manager."""

    def list(self) -> dict[str, Tag]: ...

    def get_version(self, tag: str) -> Optional[int]: ...

    def list_ordered(self, order: Optional[str] = None) -> List[Tuple[str, Tag]]: ...

    def create(
        self,
        tag: str,
        reference: Optional[int | str | Tuple[Optional[str], Optional[int]]] = None,
    ) -> None: ...

    def delete(self, tag: str) -> None: ...

    def update(
        self,
        tag: str,
        reference: Optional[int | str | Tuple[Optional[str], Optional[int]]] = None,
    ) -> None: ...

    def replace_metadata(self, tag: str, metadata: Dict[str, str]) -> None: ...
```

| Method | Signature | Behavior |
|---|---|---|
| `create` | `create(tag: str, reference=None)` | Create a tag named `tag`. The name **must be unique** across the dataset's tags. `reference`: `int` = version on current branch; `str` = another tag name; `(branch, version)` tuple = version in a branch; `(None, None)` / `None` = latest version on main. |
| `list` | `list() -> dict[str, Tag]` | Map of tag name → `Tag` metadata (branch, version, timestamps, manifest size, metadata dict). |
| `get_version` | `get_version(tag: str) -> Optional[int]` | The version number a tag points to, or `None` if the tag does not exist. |
| `list_ordered` | `list_ordered(order: Optional[str] = None) -> List[Tuple[str, Tag]]` | Ordered list of `(name, Tag)`. `order` is `"asc"` or `"desc"`; default `"desc"`. |
| `update` | `update(tag: str, reference=None)` | Move an existing tag to a new version/reference. Updates the tag's `updated_at`. |
| `delete` | `delete(tag: str) -> None` | Remove the tag. After this, the version it protected becomes eligible for cleanup again. |
| `replace_metadata` | `replace_metadata(tag: str, metadata: Dict[str, str]) -> None` | Replace the **entire** metadata map for the tag (not a merge). Does not change the tag's reference and does **not** touch `updated_at` (only `update()` moving the reference changes `updated_at`). |

The `Tag` shape returned by `list()` / `list_ordered()`:

```python
class Tag(TypedDict):
    branch: Optional[str]
    version: int
    created_at: Optional[datetime]
    updated_at: Optional[datetime]
    manifest_size: int
    metadata: Dict[str, str]
```

```python
ds = lance.dataset("s3://bucket/data.lance")

# Tag the current latest version
ds.tags.create("v2-prod-20260703", ds.latest_version)

# Inspect
ds.tags.list()                    # {"v2-prod-20260703": {"version": 10, ...}}
ds.tags.get_version("v2-prod-20260703")   # -> 10

# Open / checkout by tag name (string reference)
ds_prod = lance.dataset("s3://bucket/data.lance", version="v2-prod-20260703")
ds_prod = ds.checkout_version("v2-prod-20260703")

# Move the tag to a newer version, then drop it
ds.tags.update("v2-prod-20260703", ds.latest_version)
ds.tags.delete("v2-prod-20260703")
```

---

## 5. `cleanup_old_versions()` — reclaim storage

Overwrites, deletes, and compaction leave behind fragments no longer referenced by the latest version. They are retained so you can time-travel/restore. `cleanup_old_versions()` deletes old **versions** (manifests) and the data files only they reference.

```python
def cleanup_old_versions(
    self,
    older_than: Optional[timedelta] = None,
    retain_versions: Optional[int] = None,
    *,
    delete_unverified: bool = False,
    error_if_tagged_old_versions: bool = True,
    delete_rate_limit: Optional[int] = None,
) -> CleanupStats:
```

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `older_than` | `timedelta`, optional | see note | Only versions older than this are removed. If **both** `older_than` and `retain_versions` are `None`, defaults to **two weeks** (`timedelta(days=14)`). |
| `retain_versions` | `int`, optional | `None` | Always keep the last **N** versions, regardless of age. |
| `delete_unverified` | `bool`, keyword-only | `False` | Files from a failed transaction can look like an in-progress write and are **not** deleted unless ≥ 7 days old. `True` deletes them regardless of age. **Only set `True` if you can guarantee no other process is touching the dataset** — otherwise you can corrupt it. |
| `error_if_tagged_old_versions` | `bool`, keyword-only | `True` | Tagged versions are never cleaned up. If `True` (default), raises if any tagged version matches the cleanup window. If `False`, tagged versions are silently skipped and only untagged ones are removed. |
| `delete_rate_limit` | `int`, optional keyword-only | `None` | Max delete ops/second. `None` = full speed. Set a positive int to avoid object-store rate limits (e.g. S3 `503 SlowDown`). E.g. `delete_rate_limit=100`. |

Returns a `CleanupStats` object:

```python
class CleanupStats:
    bytes_removed: int
    old_versions: int
    data_files_removed: int
    transaction_files_removed: int
    index_files_removed: int
    deletion_files_removed: int
```

### Interaction with tags — the safety net

Tagged versions are **exempt**. With the default `error_if_tagged_old_versions=True`, a cleanup that would otherwise sweep a tagged version raises instead of touching it. To actually remove a tagged version you must `ds.tags.delete(tag)` first, then re-run cleanup. To ignore tagged versions and clean everything else, pass `error_if_tagged_old_versions=False`.

### Examples

```python
from datetime import timedelta
import lance

ds = lance.dataset("s3://bucket/data.lance")

# Default: remove versions older than 14 days; error if any are tagged.
stats = ds.cleanup_old_versions()

# Keep only the last 3 versions, ignore tags, throttle deletes for R2/S3.
stats = ds.cleanup_old_versions(
    retain_versions=3,
    error_if_tagged_old_versions=False,
    delete_rate_limit=100,
)

print(stats.bytes_removed, stats.old_versions, stats.data_files_removed)
```

### Dry run — `explain_cleanup_old_versions()`

Preview what cleanup would remove **without deleting anything** (added alongside the newer cleanup surface; present in pylance 8.x):

```python
def explain_cleanup_old_versions(
    self,
    older_than: Optional[timedelta] = None,
    retain_versions: Optional[int] = None,
    *,
    delete_unverified: bool = False,
    error_if_tagged_old_versions: bool = True,
    delete_rate_limit: Optional[int] = None,
    include_files: bool = False,
    max_files: int = 1000,
) -> CleanupExplanation:
```

`CleanupExplanation` bundles `read_version: int`, a `stats: CleanupStats`, plus `candidate_files`, `candidate_files_truncated`, `candidate_file_limit`, `referenced_branches`, and `warnings`. Use it to size a cleanup before committing to the deletes.

> Relevance to core-x: on the daily-overwrite datasets under `s3://data-sink/active/`, unbounded version history is unbounded R2 storage growth — every overwrite orphans a full prior snapshot's fragments. Bound it: run `cleanup_old_versions()` on a schedule with an explicit `retain_versions=` (deterministic and independent of clock skew), and `tags.create(...)` the snapshots you must keep (e.g. a monthly reference cut) so cleanup cannot reclaim them. Set `delete_rate_limit` when cleaning large snapshots to stay under R2/S3 request-rate limits. Do **not** set `delete_unverified=True` while a Modal ingest could be mid-write against the same dataset — concurrent unverified-file deletion can corrupt an in-flight transaction.

---

## 6. Common footguns & deprecation notes

- **`version` vs `asof` precedence**: passing both silently ignores `asof`. Pass exactly one.
- **`versions()` timestamps are microsecond-truncated** — the on-disk timestamp is nanosecond-precision, but the Python `datetime` returned drops sub-microsecond digits. Do not use it for exact ns-level ordering.
- **`checkout_version()` returns a new handle; `checkout_latest()` mutates in place.** Mixing these up leads to "my checkout didn't take" bugs.
- **`restore()` never deletes newer versions** — it appends a new head. Rolling "back" grows history.
- **`cleanup_old_versions()` default window is 14 days**, and only when neither `older_than` nor `retain_versions` is given. If you pass `retain_versions` alone, there is no age floor — old-but-within-retain-count versions are kept, everything else beyond the count can go.
- **Tagged versions block cleanup by default (raise).** Set `error_if_tagged_old_versions=False` to skip-not-raise, or delete the tag to actually reclaim.
- **`delete_unverified=True` is dangerous under concurrency** — only safe when you own the dataset exclusively.
- **`lance.open` in upstream docstrings is not a real top-level export** — use `lance.dataset(...)`.
- **API surface has grown**: `cleanup_old_versions()` gained `retain_versions` and `delete_rate_limit` (keyword-only), and `explain_cleanup_old_versions()` was added. Older tutorials show only `older_than` / `delete_unverified` / `error_if_tagged_old_versions` — that older three-argument form is still valid, just a subset.

### Unverified / needs confirmation

- The rendered API-reference page `https://lancedb.github.io/lance/api/python.html` returned **HTTP 404** at fetch time (2026-07-08). All signatures above are taken from the `main`-branch source, which corresponds to the 8.x line but may run slightly ahead of the exact 8.0.0 tag. If you need the precise 8.0.0-tagged behavior, pin and read the source at the `python-v8.0.0` git tag.
- `versions()` has no return-type annotation in source; the element shape is documented by the `Version` TypedDict but the method itself returns a plain `list` of dicts.

---

## Cross-links (sibling canonical files)

- `00_overview.md` — Lance & LanceDB overview, ecosystem, packaging & versions.
- `01_file_format.md` — on-disk dataset layout: manifests, fragments, immutability (the substrate versioning is built on).
- `02_python_dataset_api.md` — `lance.dataset`, `lance.write_dataset`, the `LanceDataset` surface.
- `03_writes_appends_upserts.md` — the write modes (`append`/`overwrite`/`merge_insert`/`delete`/`update`/`add_columns`) whose commits each create the versions documented here.
- `07_storage_object_stores.md` — `storage_options` for Cloudflare R2 / S3 (needed to open and clean up datasets on R2).
- `08_compaction_maintenance.md` — **compaction & index optimization**: compaction produces new versions and orphaned fragments that `cleanup_old_versions()` later reclaims; run compaction and cleanup together as maintenance.
- `10_duckdb_arrow_interop.md` — reading a pinned/old version from DuckDB via zero-copy Arrow.
- `11_lancedb_table_api.md` — the LanceDB table layer, which exposes its own versioning/tagging conveniences over pylance.
