# 02 · LanceDB Storage — The Lakehouse Persistence Plane

Source of truth for the core-x persistence plane. LanceDB is the **system of
record** for the entire fleet. Every feed's transformed output is committed here,
every load-bearing resolution key is indexed here, and every time-travel read
resolves here. Deviations require updating
[`ARCHITECTURE.md`](../../ARCHITECTURE.md) first.

This file is loaded VERBATIM as system context. Every API name, parameter,
import path, and default below is exact per the verified Lance SDK surface
(`pylance` `7.0.0`, `lance-format/lance` main, current 2026-05-31). A wrong API
name here makes every downstream agent write broken code. Mirror these snippets
exactly.

Sibling references:
[`01_duckdb_processing.md`](01_duckdb_processing.md) ·
[`03_modal_compute.md`](03_modal_compute.md) ·
[`04_trigger_orchestration.md`](04_trigger_orchestration.md)

Canonical implementations mirrored throughout:
[`pipelines/sam_gov/sam_opps_bulk.py`](../../pipelines/sam_gov/sam_opps_bulk.py),
[`core/modal_dispatcher.py`](../../core/modal_dispatcher.py),
[`requirements.txt`](../../requirements.txt).

---

## 1. Role of the persistence plane

Lance is where the data plane terminates. DuckDB does 100% of the transform
([`01_duckdb_processing.md`](01_duckdb_processing.md)); its output Arrow table is
written **directly** to Cloudflare R2 as a Lance dataset. There is no catalog
round-trip, no intermediate format that survives the write, and no second system
that must agree about where the data lives. Lance on R2 **is** the record.

- **Lance is the system of record.** Every load-bearing resolution key gets a
  hard `BTREE` scalar index (§6). The dataset on R2 is authoritative; nothing
  downstream re-derives or shadow-copies it.
- **The write target is R2, addressed as `s3://`.** Lance speaks the S3 object
  store protocol; R2 is S3-compatible. The SAM.gov reference dataset is
  `s3://sam-gov-opps/active/` (`pipelines/sam_gov/sam_opps_bulk.py`,
  `LANCE_BASE_URI`). The `s3://` scheme plus R2 `storage_options` (§3) routes
  the write to Cloudflare, not AWS.
- **Parquet is transport only.** Where a raw payload lands as ephemeral
  ZSTD-compressed Parquet it is read once by DuckDB and discarded. Parquet never
  persists as a queryable tier. Lance is the only durable columnar store.

> ### Forbidden — do not reintroduce
> **No Iceberg. No Polaris. No catalog round-trip.** The Gen-3 data plane writes
> Lance to R2 directly and needs none of them. A worker that registers a table
> in a REST catalog, writes an Iceberg manifest, or resolves a dataset URI
> through Polaris has reintroduced a retired layer. The dataset URI is a plain
> `s3://<bucket>/<path>/` string, committed in the worker, resolved by Lance's
> own object store. See the forbidden list in
> [`ARCHITECTURE.md` §4](../../ARCHITECTURE.md).

> ### Zero-copy law — Apache Arrow is the only interchange
> Between DuckDB and Lance, **Apache Arrow is the only in-memory interchange.**
> **pandas is forbidden.** **Heavy nested-dict intermediates are forbidden.**
> DuckDB emits Arrow (`to_arrow_table` / `to_arrow_reader`) and
> `lance.write_dataset` consumes Arrow — the columns never leave the Arrow
> memory format on the path to R2. A `.to_pandas()` anywhere on the write path
> is a defect: it copies every column out of Arrow, doubles memory, and discards
> the type fidelity DuckDB's `TRY_CAST` established. See
> [`01_duckdb_processing.md`](01_duckdb_processing.md).

---

## 2. Lance Python SDK — packaging and the write surface

### 2.1 `import lance` comes from `pylance`, not `lancedb`

The low-level Lance Python module is pip-installed as **`pylance`** and imported
as **`import lance`**. The higher-level **`lancedb`** package is a *separate*
distribution and **does NOT re-export the low-level `lance` module**. A worker
that writes datasets with `lance.write_dataset(...)` MUST depend on `pylance` to
get `import lance` — depending on `lancedb` alone leaves `import lance`
unresolved at runtime.

| Symbol | pip package | Imported as | Used in |
|---|---|---|---|
| `lance.write_dataset`, `lance.dataset`, `lance.LanceOperation`, `LanceDataset` | `pylance` | `import lance` | [`pipelines/sam_gov/sam_opps_bulk.py`](../../pipelines/sam_gov/sam_opps_bulk.py) |
| `lancedb` (higher-level table API) | `lancedb` | `import lancedb` | not used by SAM.gov |

[`requirements.txt`](../../requirements.txt) lists both `lancedb` and `pylance`; the
worker image pins them in `image.pip_install(...)`
(`pipelines/sam_gov/sam_opps_bulk.py`). The comment in that worker —
`pylance ... provides import lance; lancedb does not re-export it` — is the law.
Keep both.

> ### Version reality — pylance left `0.x`
> `pylance` is **not** on `0.x` anymore. The current line is calendar/major
> versioned: `2.0.0` (Jan 2026) → `7.0.0` (2026-05-27, current). The
> `lance.write_dataset` signature has been **additively stable** across that
> entire line. The worker image currently pins `pylance>=0.19`; that floor is
> obsolete. Pin a current floor (`pylance>=7`, or a tested lower bound such as
> `>=4`) to express the post-`0.x` reality. `import lance` resolves identically;
> only the floor framing changes.

### 2.2 `lance.write_dataset` parameter mapping

The canonical write call is `lance.write_dataset(...)`. Exact signature and the
parameters that matter for core-x:

| Parameter | Type / accepted values | core-x rule |
|---|---|---|
| `data_obj` (1st positional, `data`) | `pa.Table` · `pa.RecordBatchReader` · `Iterable[pa.RecordBatch]` · `pa.dataset.Dataset` / `Scanner` (`ReaderLike`) | A DuckDB Arrow table (`to_arrow_table()`) or a DuckDB `RecordBatchReader` (`to_arrow_reader()`). Arrow only — never pandas. |
| `uri` (2nd positional) | `str` | The R2 dataset URI, `s3://<bucket>/<path>/`. SAM.gov: `s3://sam-gov-opps/active/`. |
| `schema` | `pa.Schema` | **REQUIRED when `data_obj` is a bare iterator / `RecordBatchReader`** without a settled schema. A `pa.Table` carries its own schema — omit it then. Pass `reader.schema` for a reader. |
| `mode` | `str`: `"create"` · `"append"` · `"overwrite"` | `"create"` (default) ERRORS if the dataset exists. SAM.gov passes `"overwrite"` (daily full snapshot). `"append"` adds fragments for incremental loads. |
| `data_storage_version` | `Optional[Literal["stable","2.0","2.1","2.2","2.3","next","legacy","0.1"]]` | Pin `"2.1"` (current default) or `"stable"` (auto-tracks default). See §2.3. NEVER `"legacy"` / `"0.1"` / `"next"`. |
| `storage_options` | `Optional[Dict[str,str]]` | The R2 object-store options (§3). |
| `max_rows_per_file` | `int`, default `1_048_576` | Bounds rows per data file → bounds fragment count at scale (§4). |
| `max_rows_per_group` | `int`, default `1024` | Bounds the in-file row group. |
| `max_bytes_per_file` | `int`, default `~90 GiB` (`96_636_764_160`) | Bounds bytes per data file. |

Returns a `LanceDataset`.

> ### Does not exist — do not write
> **`use_lsm_write` is NOT a parameter of `lance.write_dataset`.** It does not
> exist anywhere in the Lance write API (verified by grepping `pylance 7.0.0`
> source — `use_lsm_write`, `lsm_write`, `use_lsm` are all NOT FOUND). Passing
> `use_lsm_write=True` raises `TypeError: unexpected keyword argument`. The write
> path is controlled **only** by `mode`, `data_storage_version`, and the
> `max_rows_per_file` / `max_rows_per_group` / `max_bytes_per_file` knobs. There
> is no LSM write toggle. Do not introduce one.

### 2.3 `data_storage_version` — current default is `"2.1"`

The current Lance default (and what `"stable"` resolves to in `pylance 7.0.0`) is
**`"2.1"`** — the file-format enum carries its default on `V2_1`. Omitting
`data_storage_version` and passing `"stable"` both resolve to `2.1`.

- **New core-x datasets MUST pin `"2.1"`** (matches the current default, gets the
  latest stable encoding) **or `"stable"`** (auto-tracks the default).
- `"2.0"` is still a **first-class, fully read/write-supported** version and is
  forward-read-compatible — nothing breaks — but it is **one generation behind**
  the current default. New datasets written with `"2.0"` get the prior encoding.
- `"legacy"` (`= 0.1`) is the pre-v2 format and is **forbidden** for the system
  of record. `"next"` (`= 2.3`) is unstable — **forbidden** for the system of
  record.

> ### Reconcile the existing `"2.0"` pin
> `pipelines/sam_gov/sam_opps_bulk.py` and `ARCHITECTURE.md` §4 currently pin
> `data_storage_version="2.0"` (and label the data plane "LanceDB v2.0"). That
> is **valid and supported**, but `"2.1"` is now the default. New feeds MUST pin
> `"2.1"` or `"stable"`; the SAM.gov `"2.0"` pin should be advanced to `"2.1"`
> unless staying on `"2.0"` is a deliberate, documented choice. Both are
> read/write-supported and interoperable; the only effect is which encoding new
> fragments use.

---

## 3. R2 `storage_options` — exact object-store keys

Lance reaches R2 through its object store layer. R2 is S3-compatible: AWS-style
credentials plus an **explicit `endpoint`**, and `region` is **always `"auto"`**
(R2 has no real region). Mirror `_r2_storage_options()` in
[`pipelines/sam_gov/sam_opps_bulk.py`](../../pipelines/sam_gov/sam_opps_bulk.py)
exactly.

| Key | Value | Rule |
|---|---|---|
| `aws_access_key_id` | R2 access key id | From the `r2-credentials` Modal secret. |
| `aws_secret_access_key` | R2 secret access key | From the `r2-credentials` Modal secret. |
| `endpoint` | `https://<account_id>.r2.cloudflarestorage.com` | Supplied directly (`R2_ENDPOINT`) or derived from `R2_ACCOUNT_ID`. MUST be `https://` for R2. |
| `region` | `"auto"` | R2 has no real region. Never a real AWS region. |

The bare `access_key_id` / `secret_access_key` and `aws_endpoint` spellings are
also accepted by the object store, but core-x standardizes on the `aws_`-prefixed
forms above to match the worker. `allow_http` is **only** for an `http://`
endpoint (local MinIO) — **NEVER** set it for R2's `https://` endpoint.

```python
import os


def _r2_storage_options() -> dict[str, str]:
    """object_store options for Cloudflare R2, sourced from the Modal secret.

    R2 is S3-compatible: AWS-style credentials + an explicit ``endpoint`` and
    ``region`` ("auto" for R2). The endpoint is either supplied directly
    (``R2_ENDPOINT``) or derived from the account id. Only set ``allow_http`` for
    an http:// endpoint (local MinIO) — never for R2's https endpoint.
    """
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID in the Modal secret.")
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": endpoint,
        "region": "auto",
    }
```

Credentials arrive via the `r2-credentials` Modal secret attached to the worker
(`pipelines/sam_gov/sam_opps_bulk.py`, `@app.function(secrets=[...])`). They are
never committed; see [`03_modal_compute.md`](03_modal_compute.md) for the secret
wiring.

---

## 4. Large-scale writes — fragments, streaming, append, upsert

Lance writes data as **immutable fragments** (a fragment = 1+ immutable data
files + an optional deletion vector). `max_rows_per_file` and `max_bytes_per_file`
bound each data file; `max_rows_per_group` bounds the in-file row group. These
knobs — not any LSM toggle — control fragment count and file sizing at scale.

### 4.1 The daily-overwrite snapshot (SAM.gov canonical)

SAM.gov writes a daily **full snapshot** of currently-active notices:
`mode="overwrite"` creates a brand-new immutable version whose manifest
references only the new fragments. Prior versions are retained for time travel
(§4.4) until reclaimed (§5.4). This is the canonical pattern and matches
`pipelines/sam_gov/sam_opps_bulk.py`.

```python
import duckdb
import lance  # provided by the `pylance` pip package, NOT by lancedb

LANCE_BASE_URI = "s3://sam-gov-opps/active/"

con = duckdb.connect(":memory:")
try:
    con.execute("PRAGMA threads=4;")
    arrow_table = con.execute(TRANSFORM_SQL).to_arrow_table()  # Arrow, never pandas
finally:
    con.close()

# mode="overwrite": new immutable version; prior versions retained for time
# travel. Pin "2.1" to match the current Lance default ("2.0" is one generation
# behind but still supported); "stable" auto-tracks the default.
lance.write_dataset(
    arrow_table,
    LANCE_BASE_URI,
    mode="overwrite",
    data_storage_version="2.1",
    storage_options=_r2_storage_options(),
)
```

`mode="create"` is the SDK default and **ERRORS if the dataset already exists**.
The daily job MUST pass `mode="overwrite"` explicitly — relying on the default
would succeed on day 1 and fail on day 2. The worker passes `mode` through as a
parameter defaulting to `"overwrite"`; keep that default.

### 4.2 USASpending-scale streaming write — RecordBatchReader, no full materialization

The SAM.gov worker fully materializes the result in RAM before writing via `con.execute(sql).to_arrow_table()` (the on-disk worker still calls the deprecated `.fetch_arrow_table()` — see [`01_duckdb_processing.md`](01_duckdb_processing.md)). For SAM (~tens of MB) that is
fine. At **USASpending scale** it is not: switch to DuckDB's
`con.execute(sql).to_arrow_reader(batch_size)`, which returns a
`pyarrow.RecordBatchReader`, and hand **that reader** to `write_dataset`. Lance
consumes the reader **fragment-by-fragment** — the full result set never lands in
RAM at once. The columns stay in Arrow the entire way; pandas never appears.

```python
import duckdb
import lance

con = duckdb.connect(":memory:")
con.execute("PRAGMA threads=8;")

# to_arrow_reader returns a pyarrow.RecordBatchReader: write_dataset consumes
# it fragment-by-fragment, so the full result set never materializes in RAM.
reader = con.execute(LARGE_TRANSFORM_SQL).to_arrow_reader(batch_size=131072)

lance.write_dataset(
    reader,                        # RecordBatchReader is a valid ReaderLike
    "s3://usaspending/awards/",
    schema=reader.schema,          # REQUIRED when the source is a reader/iterator
    mode="overwrite",
    data_storage_version="2.1",
    max_rows_per_file=2_000_000,   # bound fragment / data-file size at scale
    max_rows_per_group=8192,
    storage_options=_r2_storage_options(),
)
con.close()
```

> **`schema=` is mandatory for a reader/iterator.** When `data_obj` is a bare
> `Iterable[pa.RecordBatch]` or a `RecordBatchReader` without a settled schema,
> `write_dataset` cannot infer the schema — pass `schema=reader.schema`. A
> `pa.Table` carries its own schema and needs no `schema=`. This is the single
> most common scale-write defect.

### 4.3 Append vs overwrite

| `mode` | Effect | When |
|---|---|---|
| `"overwrite"` | New version; manifest references only the new fragments. Old versions/fragments retained until cleanup. | Daily full snapshot (SAM.gov active set). |
| `"append"` | Adds new fragments to the existing version's data. | Incremental loads — new rows accrete without rewriting the dataset. |
| `"create"` | Errors if the dataset exists. | First-ever write only. Never the daily path. |

### 4.4 Key-level upsert — `merge_insert`

When a feed needs **key-level updates** instead of a full-snapshot replacement,
`dataset.merge_insert(on="<key>")` performs a SQL-`MERGE`-style atomic upsert in
one transaction: join on the key, write new fragments, mark superseded rows
deleted. This is the alternative to daily overwrite when SAM ever requires
incremental `notice_id`-keyed updates.

```python
import lance

ds = lance.dataset("s3://sam-gov-opps/active/", storage_options=_r2_storage_options())

# Atomic SQL-MERGE-style upsert keyed on notice_id: update matches, insert the rest.
(
    ds.merge_insert("notice_id")
    .when_matched_update_all()
    .when_not_matched_insert_all()
    .execute(new_arrow_table)  # Arrow in, single committed transaction
)
```

### 4.5 The fragment / file-sizing knobs

| Knob | Default | Effect |
|---|---|---|
| `max_rows_per_file` | `1_048_576` | Rows per data file. Lower = more, smaller fragments; higher = fewer, larger. Tune at scale. |
| `max_rows_per_group` | `1024` | Rows per in-file row group (read granularity). |
| `max_bytes_per_file` | `~90 GiB` | Hard byte ceiling per data file. |

> ### Does not exist — do not write
> **There is no `use_lsm_write` knob to "make large writes fast."** The real,
> verified mechanism for scale writes is: stream a `pyarrow.RecordBatchReader`
> into `write_dataset` (no full materialization), bound fragments with
> `max_rows_per_file` / `max_bytes_per_file`, and choose `append` vs `overwrite`
> vs `merge_insert` for the load shape. Name those parameters — never invent an
> LSM flag.

---

## 5. Crash safety & durable transactions — the manifest-MVCC model

Lance's durability is **versioned, copy-on-write, manifest-based MVCC**. It is
**NOT** an LSM tree. Understanding the real model is what makes a mid-write crash
provably non-corrupting.

### 5.1 The on-disk model

- **Immutable fragments.** Data lives as immutable data files under `data/`. A
  fragment is never mutated in place; a change writes new fragments.
- **Versioned manifests.** Each dataset version is described by an immutable
  manifest under `_versions/`. Listing manifests latest-first is a plain object
  listing; an optional `_versions/latest_version_hint.json` (`{"version": N}`)
  points at the newest.
- **Transaction log.** Each commit serializes a transaction protobuf to
  `_transactions/` **before** publishing the manifest.
- **Atomic version publish.** A commit becomes visible only when its new manifest
  is **atomically published** via rename-if-not-exists / put-if-not-exists,
  advancing a **monotonically increasing version**.
- **Indices** live under `_indices/` and are themselves versioned/committed (§6).

### 5.2 Optimistic concurrency + conflict retry

Concurrency is **optimistic (OCC)**. On a conflicting commit, Lance loads the
transaction files, detects the conflict, **rebases** the transaction if it is
compatible, and **retries**. `LanceDataset.commit(...)` defaults to
`max_retries=20`. The public commit surface is `lance.LanceOperation`
(`Append` / `Overwrite` / `Delete` / `Merge` / `Update` / `Restore` / …) plus the
static `LanceDataset.commit(base_uri, operation, read_version=None,
commit_lock=None, storage_options=None, max_retries=20, ...)`. The SAM.gov daily
job is single-writer and never hits a conflict, but the model holds for any
future concurrent writer.

### 5.3 Why a mid-write crash never corrupts a version

Because a version becomes visible **only** when its manifest is atomically
published, a crash mid-write leaves immutable data files in `data/` that **no
committed manifest references** — they are orphaned, invisible, and never
corrupt. Readers always see the **last fully-committed version**; a half-written
fragment set is simply unreferenced garbage, not a broken dataset. There is no
torn write to recover from and no partial version to repair.

> This is the crash-safety guarantee in full: **orphaned files, never a corrupt
> version.** A failed daily overwrite leaves the previous good snapshot as the
> latest committed version and some invisible orphan fragments. The next
> successful run commits cleanly; the orphans are reclaimed by cleanup (§5.4).

### 5.4 Reclaiming storage — `cleanup_old_versions`

Every daily `overwrite` creates a new immutable version and **retains old
fragments forever** until reclaimed. Schedule
`LanceDataset.cleanup_old_versions(older_than=None, retain_versions=None,
error_if_tagged_old_versions=True)` to bound R2 growth. It also deletes the
orphaned files a crashed mid-write commit left behind (those are never visible
either way).

```python
import lance

so = _r2_storage_options()
ds = lance.dataset("s3://sam-gov-opps/active/", storage_options=so)

# Reclaim storage from superseded versions (and any orphaned mid-write files).
# Without this, every daily overwrite accretes fragments and R2 grows unbounded.
ds.cleanup_old_versions(retain_versions=30)
```

### 5.5 Time travel over daily snapshots

Each daily overwrite is one addressable immutable version. Enumerate history with
`ds.versions()`, read a prior snapshot with `lance.dataset(uri, version=N)` (or
`ds.checkout_version(N)`), and reclaim with `cleanup_old_versions`. This is the
documented intent of the SAM.gov daily-snapshot design.

```python
import lance

so = _r2_storage_options()

# Enumerate committed versions — each daily overwrite is one immutable version.
ds = lance.dataset("s3://sam-gov-opps/active/", storage_options=so)
for v in ds.versions():
    print(v["version"], v["timestamp"])

# Read a prior snapshot by version number (time travel).
old = lance.dataset("s3://sam-gov-opps/active/", version=42, storage_options=so)
```

> ### Does not exist — do not write
> **There is no "MemWAL" / "Memory Write-Ahead Log" and no public "shard writer"
> in the core-x write path.** A MemTable & WAL spec exists in Lance, but it is
> flagged **EXPERIMENTAL**, is materially incomplete (upstream tracking issue
> open), is **NOT reachable through `lance.write_dataset`**, and exposes no public
> Python "shard writer" API (the `ShardWriter` name is internal protobuf only).
> core-x MUST NOT depend on it. **Lance core is NOT an LSM tree** — the
> persistence plane is versioned, copy-on-write, manifest-based MVCC with
> immutable fragments, atomic manifest commit, and OCC retries, exactly as
> described above. Do not describe the core-x persistence plane as an LSM tree,
> do not reference a MemWAL, and do not reach for a shard writer. The durable
> transaction log is `_transactions/` + atomically-published `_versions/`
> manifests — that is the real mechanism.

> ### Multi-writer caveat
> On object stores lacking atomic rename / put-if-absent, multi-writer commits
> need an external manifest store or a `commit_lock` — OCC alone is insufficient
> there. SAM.gov's single-writer daily job is safe. If multiple writers ever
> target one dataset, verify R2's conditional-write support and supply a
> `commit_lock` before going concurrent.

---

## 6. Scalar indexing — `BTREE` for every resolution key

Lance is the system of record, so every **load-bearing resolution key** gets a
hard scalar index. The call is `LanceDataset.create_scalar_index(column,
index_type, name=None, *, replace=True, train=True, fragment_ids=None,
index_uuid=None, progress_callback=None)`. Indexes are versioned and stored under
`_indices/`.

### 6.1 `index_type` values and when each applies

| `index_type` | Use for | SAM.gov example |
|---|---|---|
| `"BTREE"` | **High-cardinality load-bearing resolution keys** — equality AND range predicates. The default for any key joins/lookups resolve on. | `notice_id`, `solicitation_number` |
| `"BITMAP"` | Low-cardinality categoricals filtered frequently. | `notice_type`, `set_aside_code`, `pop_state` |
| `"LABEL_LIST"` | List / array-valued columns (membership over a set). | a future multi-value tag column |
| `"INVERTED"` / `"FTS"` | Full-text search over free text. | `description` (only if FTS is needed) |
| `"NGRAM"` | Substring / `LIKE` acceleration. | partial-match on a code field |

The current `index_type` Literal also accepts `"ZONEMAP"`, `"BLOOMFILTER"`, and
`"RTREE"` (newer additions), plus an `IndexConfig` object. SAM.gov needs only
`BTREE` and `BITMAP`.

> **`BTREE` is the rule for load-bearing resolution keys.** Per the core-x
> architecture, every key a downstream resolution joins or looks up on gets a
> hard `BTREE` scalar index. `notice_id` and `solicitation_number` are
> resolution keys; they MUST be `BTREE`-indexed. Categorical filter columns get
> `BITMAP`. See [`ARCHITECTURE.md` §4](../../ARCHITECTURE.md).

```python
import lance

ds = lance.dataset("s3://sam-gov-opps/active/", storage_options=_r2_storage_options())

# BTREE: high-cardinality resolution keys (equality + range predicates).
ds.create_scalar_index("notice_id", index_type="BTREE")
ds.create_scalar_index("solicitation_number", index_type="BTREE")

# BITMAP: low-cardinality categoricals filtered frequently.
ds.create_scalar_index("notice_type", index_type="BITMAP")
ds.create_scalar_index("pop_state", index_type="BITMAP")
```

### 6.2 Vector indices — not used by SAM.gov

Vector indices are created via the **separate** `LanceDataset.create_index(columns,
index_type=..., ...)`, whose Literal includes `"IVF_FLAT"`, `"IVF_PQ"`,
`"IVF_SQ"`, `"IVF_HNSW_PQ"`, `"IVF_HNSW_SQ"`. SAM.gov stores no embeddings, so
`create_index` is **not** used today. It becomes relevant only if a future
core-x feed persists vector columns — `create_scalar_index` (this section) is the
SAM.gov path, `create_index` is the vector path.

---

## 7. Adding a feed — the persistence-plane checklist

A new feed's write path mirrors SAM.gov exactly:

1. **URI** — a dedicated `s3://<bucket>/<path>/` Lance dataset URI, committed in
   the worker as a module constant (`LANCE_BASE_URI` in
   `pipelines/sam_gov/sam_opps_bulk.py`). No catalog, no Polaris, no Iceberg.
2. **Storage options** — reuse `_r2_storage_options()` verbatim (§3); the
   `r2-credentials` Modal secret supplies the keys.
3. **Write** — DuckDB Arrow → `lance.write_dataset(arrow_or_reader, uri,
   mode="overwrite", data_storage_version="2.1", storage_options=...)`. Stream a
   `RecordBatchReader` at scale (§4.2). Arrow only — pandas forbidden.
4. **Index** — `create_scalar_index(key, index_type="BTREE")` for every
   load-bearing resolution key; `"BITMAP"` for categoricals (§6).
5. **Reclaim** — schedule `cleanup_old_versions(retain_versions=...)` so daily
   overwrites do not grow R2 without bound (§5.4).

The compute that performs this write is governed by
[`03_modal_compute.md`](03_modal_compute.md); the cadence that triggers it by
[`04_trigger_orchestration.md`](04_trigger_orchestration.md); the DuckDB
transform that produces the Arrow by
[`01_duckdb_processing.md`](01_duckdb_processing.md).

> ### The one-line invariant
> DuckDB → **Apache Arrow** → `lance.write_dataset` → **LanceDB on R2**, indexed
> with `BTREE` on every resolution key, versioned by immutable manifests, with
> **no Iceberg, no Polaris, no catalog round-trip, no pandas, and no `use_lsm_write`
> / MemWAL / shard writer.** That is the entire persistence plane.
