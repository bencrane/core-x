# DuckDB ↔ Lance Interop — the verified reality of reading/writing Lance from DuckDB

> Canonical upstream reference — fetched 2026-07-08 from official DuckDB documentation and source repositories. Documents upstream behavior as-is; this is NOT core-x doctrine. Treat the version numbers and API signatures here as fetched-date ground truth.
>
> Primary sources:
> - https://duckdb.org/docs/current/core_extensions/overview.html — DuckDB core-extensions catalog; confirms `lance` is a listed core extension ("Adds support to read and write Lance tables"), third-party maintained.
> - https://duckdb.org/docs/lts/core_extensions/lance — DuckDB Lance extension reference: `INSTALL`/`LOAD`, read via replacement scan, `COPY … (FORMAT lance)`, search functions, `ATTACH` namespaces, Secret Manager.
> - https://duckdb.org/2026/05/21/test-driving-lance — DuckDB blog "Test-Driving the Lance Lakehouse Format in DuckDB" (uses DuckDB 1.5.2); worked read/write/hybrid-search SQL.
> - https://github.com/lance-format/lance-duckdb (docs/sql.md, docs/cloud.md, releases) — extension source: full SQL surface (search params, DML/DDL, index, OPTIMIZE/VACUUM), cloud Secret Manager (`TYPE LANCE`).
> - https://www.aidoczh.com/lance/api/python/write_dataset.html + https://lance.org/guide/object_store/ — pylance `write_dataset` signature and S3/R2 `storage_options` keys.
> - https://duckdb.org/docs/current/guides/python/export_arrow.html + https://duckdb.org/docs/current/guides/python/import_arrow.html — DuckDB ⇄ Arrow bridge (`to_arrow_table`, `to_arrow_reader`, replacement scans).

Scope: The two verified ways to move data between DuckDB and the Lance format — the native DuckDB `lance` core extension (SQL `INSTALL lance` / `COPY … (FORMAT lance)` / `lance_*_search`), and the pyarrow zero-copy bridge (DuckDB → Arrow → `lance.write_dataset`; `lance.dataset` → Arrow → DuckDB replacement scan) — with exact signatures, versions, cloud/R2 config, and footguns.

---

## 0. TL;DR — what is real as of 2026-07-08

There are **two** working paths. Both are real; pick by constraint.

| Path | Mechanism | When to use |
|------|-----------|-------------|
| **A. Native `lance` extension** | DuckDB core extension. `INSTALL lance; LOAD lance;` then `SELECT * FROM '….lance'` and `COPY … TO '….lance' (FORMAT lance, MODE 'overwrite'|'append')`. | Pure-SQL pipelines; DuckDB-driven vector/FTS/hybrid search; no Python in the loop. |
| **B. pyarrow bridge** | DuckDB exports Arrow (`to_arrow_table` / `to_arrow_reader`); pylance `lance.write_dataset` writes. Read-back: `lance.dataset(uri).to_table()` or the dataset object registered/replacement-scanned into DuckDB. | Python-orchestrated pipelines; streaming out-of-core writes; full control over Lance write options (`max_rows_per_file`, `data_storage_version`, `storage_options`). **This is the core-x plane.** |

**The `COPY … (FORMAT lance)` API is real** (it was NOT before mid-2026). Lance became a DuckDB **core extension** in 2026 (announced by LanceDB; DuckDB blog "Test-Driving the Lance Lakehouse Format in DuckDB" dated 2026-05-21, run on DuckDB 1.5.2). Prior to that, only the pyarrow bridge existed. If you are on an older DuckDB or an environment where the extension is unavailable, **Path B is the fallback and is always available.**

Verified current versions (fetched 2026-07-08):
- **DuckDB** stable: **1.5.4** (Variegata, 2026-06-17); LTS: **1.4.5** (Andium, 2026-06-17). The Lance blog used **1.5.2**.
- **pylance** (the `pylance` PyPI package — the Python binding to the Lance Rust core; import name `lance`): latest PyPI release **8.0.0** (2026-07-01), requires Python ≥ 3.9. (Do not confuse with Microsoft's VS Code "Pylance" language server — unrelated project, same name.)
- **lance-duckdb extension**: see [§1.5](#15-extension-version--maintenance--needs-confirmation) — version pinning is flagged as needs-confirmation.

---

## 1. Path A — the native DuckDB `lance` extension

`lance` is listed on the DuckDB **core extensions** overview page: *"Adds support to read and write Lance tables … maintained by a third party"* (source: lance-format/lance-duckdb, LanceDB + DuckLabs). It is **not** written by the DuckDB core team, but it is distributed through the core extension repository, so a plain `INSTALL lance` resolves it.

### 1.1 Install & load

```sql
INSTALL lance;
LOAD lance;
```

The extension source (`docs/sql.md`) also documents the community-repo and local-build forms:

```sql
-- from the community repository (if not resolved as core in your build)
INSTALL lance FROM community;
LOAD lance;

-- local dev build
LOAD 'build/release/extension/lance/lance.duckdb_extension';
```

Platform support (per extension docs): `linux_amd64`, `linux_arm64`, `osx_arm64`, `windows_amd64`.

### 1.2 Reading a Lance dataset (replacement scan)

A Lance dataset path used as a table name triggers a replacement scan — no explicit function needed:

```sql
SELECT * FROM 'path/to/dataset.lance' LIMIT 10;

-- object storage
SELECT * FROM 's3://bucket/path/to/dataset.lance' LIMIT 10;
```

> **Footgun:** there is no separately-documented `lance_scan(...)` table function in the fetched docs — reads go through the path-as-table replacement scan. If you need a function form, verify against your installed extension version; do not assume `lance_scan` exists.

### 1.3 Writing a Lance dataset — `COPY … (FORMAT lance)`

```sql
COPY (SELECT 1::BIGINT AS id, 'a'::VARCHAR AS s)
TO 'path/to/out.lance' (FORMAT lance, MODE 'overwrite');
```

`COPY … (FORMAT lance)` write options (from extension docs):

| Option | Type | Accepted values / default | Meaning |
|--------|------|---------------------------|---------|
| `FORMAT` | keyword | `lance` | Selects the Lance writer. Required. |
| `MODE` | string | `'overwrite'`, `'append'` | `overwrite` = create-or-replace (new snapshot version replacing rows); `append` = concatenate onto latest version. Quoted-string value; docs show lowercase. **`'create'` is NOT a documented COPY MODE value** — the DuckDB core-extension reference page enumerates only `'overwrite'` and `'append'` (verified 2026-07-08). `'create'` is a *pylance* `write_dataset` mode (Path B), not a SQL COPY mode; do not use it in `COPY … (FORMAT lance)`. |
| `WRITE_EMPTY_FILE` | bool | `true` (default false) | Materialize a schema-only (zero-row) dataset. |

> **Verify:** the exact set of write options beyond `MODE` / `WRITE_EMPTY_FILE` (e.g. row-group sizing, storage-version pinning at the SQL layer) was **not** enumerated in the fetched extension docs. For fine-grained write control (`max_rows_per_file`, `data_storage_version`), use Path B where those parameters are first-class. Treat SQL-side write tuning as needs-confirmation.

### 1.4 Namespaces via `ATTACH` (catalog-style DML/DDL)

Attaching a directory or REST namespace exposes Lance datasets as catalog tables, enabling full DML/DDL:

```sql
-- directory namespace
ATTACH 'path/to/dir' AS lance_ns (TYPE lance);

-- REST namespace (remote catalog)
ATTACH 'namespace_id' AS lance_ns (TYPE lance, ENDPOINT 'http://127.0.0.1:2333');

CREATE OR REPLACE TABLE lance_ns.main.my_dataset AS SELECT ...;
INSERT INTO lance_ns.main.my_dataset VALUES (...);
UPDATE lance_ns.main.my_dataset SET col = val WHERE ...;
DELETE FROM lance_ns.main.my_dataset WHERE ...;
MERGE INTO lance_ns.main.my_dataset ...            -- MATCHED / NOT MATCHED
TRUNCATE TABLE lance_ns.main.my_dataset;
DROP TABLE lance_ns.main.my_dataset;
ALTER TABLE lance_ns.main.my_dataset ADD COLUMN ...;   -- RENAME/ALTER/DROP COLUMN
COMMENT ON TABLE lance_ns.main.my_dataset IS '...';
```

Lance provides MVCC + ACID-style transactional semantics under these operations (per DuckDB blog).

### 1.5 Search functions (vector / FTS / hybrid)

These are table functions that read a `.lance` dataset and rank rows. They are the reason to keep search **in DuckDB SQL** rather than round-tripping to Python.

**Vector search** — `lance_vector_search(uri, vector_column, query_vector, ...)`:

```sql
SELECT id, label, _distance
FROM lance_vector_search(
    'path/to/dataset.lance', 'vec',
    [0.1, 0.2, 0.3, 0.4]::FLOAT[4],
    k = 5, prefilter = true
)
ORDER BY _distance ASC;
```

| Param | Default | Meaning |
|-------|---------|---------|
| `k` | `10` | number of results |
| `use_index` | `true` | use ANN (IVF) index if present |
| `nprobs` | — | IVF partitions to probe |
| `refine_factor` | — | over-fetch then re-rank |
| `prefilter` | `false` | apply WHERE filters before top-k |

Returns dataset columns + `_distance`.

**Full-text search** — `lance_fts(uri, text_column, query, ...)`:

```sql
SELECT id, text, _score
FROM lance_fts('path/to/dataset.lance', 'text', 'query', k = 10, prefilter = true)
ORDER BY _score DESC;
```

Params: `k` (default `10`), `prefilter` (default `false`). Returns dataset columns + `_score`.

**Hybrid search** — `lance_hybrid_search(uri, vector_column, query_vector, text_column, query, ...)`:

```sql
SELECT id, text, _hybrid_score, _distance, _score
FROM lance_hybrid_search(
    'path/to/dataset.lance',
    'vec', [0.1, 0.2, 0.3, 0.4]::FLOAT[4],
    'text', 'puppy',
    k = 10, prefilter = false, alpha = 0.5, oversample_factor = 4
)
ORDER BY _hybrid_score DESC;
```

Params: `k` (`10`), `alpha` (`0.5`, vector/text weighting), `oversample_factor` (`4`, candidate generation), `prefilter` (`false`). Returns `_hybrid_score`, `_distance`, `_score`.

### 1.6 Index / maintenance DDL (attached-path form)

From the extension `docs/sql.md`:

```sql
CREATE INDEX idx ON 'path/to/dataset.lance' (column)
    USING IVF_FLAT WITH (num_partitions = 1, metric_type = 'l2');   -- also BTREE, INVERTED
SHOW INDEXES ON 'path/to/dataset.lance';
ALTER INDEX idx ON 'path/to/dataset.lance' OPTIMIZE WITH (mode = 'append');  -- append|merge|retrain

OPTIMIZE 'path/to/dataset.lance' WITH (target_rows_per_fragment = 1048576);
VACUUM LANCE 'path/to/dataset.lance' WITH (older_than_seconds = 1209600);
ALTER TABLE 'path/to/dataset.lance'
    SET AUTO_CLEANUP WITH (interval = 1, older_than = '1h', retain_versions = 3);
```

Index types documented: `IVF_FLAT`, `BTREE`, `INVERTED`. `BTREE` is the scalar index type — see the core-x note in [§4](#4-relevance-to-core-x).

### 1.7 Cloud storage for Path A — Secret Manager (`TYPE LANCE`)

The extension reads/writes object stores when given a URI (`s3://…`, `gs://…`, `az://…`) plus a Secret. **The secret type is `lance`, not `s3`** — the extension carries its own object-store layer.

```sql
-- credential-chain (SDK resolves creds)
CREATE SECRET (TYPE lance, PROVIDER credential_chain, SCOPE 's3://bucket/');

-- explicit config (S3 / S3-compatible incl. R2 via ENDPOINT)
CREATE SECRET (
  TYPE lance,
  PROVIDER config,
  SCOPE 's3://bucket-prefix/',
  ACCESS_KEY_ID     '…',
  SECRET_ACCESS_KEY '…',
  REGION            'auto',
  ENDPOINT          'https://<ACCOUNT_ID>.r2.cloudflarestorage.com'
);
```

- Providers: `config` (explicit KV), `credential_chain` (SDK chain), `env` (environment variables).
- Longest matching `SCOPE` prefix wins when a dataset is opened.
- S3 keys: `ACCESS_KEY_ID`, `SECRET_ACCESS_KEY`, `SESSION_TOKEN`, `REGION`, `ENDPOINT`, `ALLOW_HTTP`. GCS: `SERVICE_ACCOUNT`, `SERVICE_ACCOUNT_KEY`, `GOOGLE_STORAGE_TOKEN`. Azure: `ACCOUNT_NAME`, `ACCOUNT_KEY`, `SAS_KEY`, `BEARER_TOKEN`.

> **R2 footgun (Path A):** the extension `docs/cloud.md` does **not** name Cloudflare R2 explicitly. R2 is reached as generic S3-compatible: set `ENDPOINT` to `https://<ACCOUNT_ID>.r2.cloudflarestorage.com` and `REGION 'auto'`. This differs from DuckDB's built-in `httpfs`, which has a dedicated `CREATE SECRET (TYPE r2, ACCOUNT_ID …)` form (see `08_secrets_manager.md`, `07_httpfs_s3_r2.md`). The `TYPE lance` secret and the `TYPE r2`/`TYPE s3` httpfs secrets are **separate secret namespaces** — a secret you created for `read_parquet('s3://…')` does not automatically apply to the Lance object-store layer.

### 1.8 Extension version & maintenance — needs confirmation

The `lance-duckdb` GitHub releases page fetch returned tags **v0.5.0 – v0.5.4** with 2025 dates and a bundled Lance-crates upgrade to `4.0.0` in v0.5.4 — but the DuckDB blog that demonstrates the same API is dated **2026-05-21** on DuckDB **1.5.2**. The relative-date rendering of the releases page is unreliable via the fetch tool.

> **Do not hard-code an extension version from this doc.** Verify against your environment: `SELECT extension_name, extension_version, installed FROM duckdb_extensions() WHERE extension_name = 'lance';` and check `github.com/lance-format/lance-duckdb/releases` for the tag that matches your DuckDB version. The DuckDB↔extension pairing is version-pinned (the extension is built against a specific DuckDB commit).

---

## 2. Path B — the pyarrow zero-copy bridge (the core-x plane)

This is the supported, always-available path and the one core-x runs. Interchange is Apache Arrow; the boundary is zero-copy where the memory layout allows.

### 2.1 DuckDB → Arrow (export)

DuckDB Python current API (source: DuckDB "Export to Apache Arrow" guide, DuckDB current docs):

| Method | Signature | Returns | Notes |
|--------|-----------|---------|-------|
| `to_arrow_table()` | `to_arrow_table(batch_size = 1_000_000)` | `pyarrow.Table` | Materializes the full result in memory. |
| `to_arrow_reader(batch_size)` | `to_arrow_reader(batch_size = 1_000_000)` | `pyarrow.RecordBatchReader` | **Streaming** — read one batch at a time; keeps memory bounded for hundreds-of-millions of rows. Default batch size `1_000_000` rows. |

> **Parameter name:** the streaming method's parameter is **`batch_size`** (verbatim from the DuckDB Python Client API reference / `_duckdb-stubs`), **not `chunk_size`**. The DuckDB "Export to Apache Arrow" *guide* page uses a local variable it happens to call `chunk_size` in its example, but the actual keyword is `batch_size` (with `rows_per_batch` on the `arrow()`/`fetch_*` aliases). Pass it positionally to be immune to the drift. See `02_arrow_integration.md` §1.2.

```python
import duckdb

con = duckdb.connect()
rel = con.sql("SELECT * FROM read_parquet('s3://bucket/staging/*.parquet')")

# full materialization
tbl = rel.to_arrow_table()

# streaming (out-of-core friendly)
reader = rel.to_arrow_reader(batch_size=1_000_000)
while (batch := reader.read_next_batch()) is not None:
    ...  # each `batch` is a pyarrow.RecordBatch
```

> **Deprecated — do not use in new code:** `fetch_arrow_table()`, `fetch_record_batch()`, `fetch_arrow_reader()`. Replaced by `to_arrow_table()` / `to_arrow_reader()`. Older code and third-party libs still emit `fetch_record_batch()` — migrate to `to_arrow_reader()`. (Historically `fetch_arrow_table(rows_per_batch=1_000_000)` also existed; both the old and new streaming paths default to `1_000_000` rows per batch. Note the new methods take `batch_size`, the `fetch_*`/`arrow` aliases take `rows_per_batch` — see `02_arrow_integration.md` §1.2.)

### 2.2 Arrow → Lance (write) — `lance.write_dataset`

Hand the Arrow object (Table **or** RecordBatchReader) straight to pylance. Passing the **reader** streams — Lance consumes batch-by-batch, so a query far larger than RAM writes without full materialization.

**Verbatim signature** (source: `www.aidoczh.com/lance/api/python/write_dataset.html`):

```python
lance.write_dataset(
    data_obj: ReaderLike,
    uri: str | Path | LanceDataset,
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
    enable_v2_manifest_paths: bool = False,
    enable_move_stable_row_ids: bool = False,
    auto_cleanup_options: AutoCleanupConfig | None = None,
) -> LanceDataset
```

| Parameter | Type | Default | Accepted values / meaning |
|-----------|------|---------|---------------------------|
| `data_obj` | `ReaderLike` | — | The data to write. Accepts pandas DataFrame, `pyarrow.Table`, `pyarrow.dataset.Dataset`, `pyarrow.dataset.Scanner`, **`pyarrow.RecordBatchReader`** (streaming), or a HuggingFace dataset. |
| `uri` | `str \| Path \| LanceDataset` | — | Destination dataset directory URI. Passing an existing `LanceDataset` reuses its session. |
| `schema` | `pa.Schema \| None` | `None` | Explicit schema; inferred from data if omitted. |
| `mode` | `str` | `'create'` | `'create'` (new dataset; **errors if `uri` exists**), `'overwrite'` (new snapshot version replacing rows), `'append'` (concatenate onto latest version). |
| `max_rows_per_file` | `int` | `1048576` | Max rows before starting a new data file (fragment sizing). |
| `max_rows_per_group` | `int` | `1024` | Max rows per group within a file. |
| `max_bytes_per_file` | `int` | `96636764160` (~90 GiB) | Max bytes before rolling a new file. |
| `commit_lock` | `CommitLock \| None` | `None` | External commit lock for concurrent writers. |
| `progress` | `FragmentWriteProgress \| None` | `None` | Per-fragment write progress hook. |
| `storage_options` | `dict[str,str] \| None` | `None` | Object-store connection params (creds, region, endpoint). See [§2.5](#25-cloudflare-r2-storage_options). |
| `data_storage_version` | `str \| None` | `None` | Storage format version. Newer = more efficient but requires newer Lance to read. See footgun below. |
| `use_legacy_format` | `bool \| None` | `None` | **Deprecated** toggle superseded by `data_storage_version`. |
| `enable_v2_manifest_paths` | `bool` | `False` | Use v2 manifest path scheme. |
| `enable_move_stable_row_ids` | `bool` | `False` | Stable row IDs across compaction/move. |
| `auto_cleanup_options` | `AutoCleanupConfig \| None` | `None` | Auto old-version cleanup config. |

> **`data_storage_version` footgun (version-drifted default — verify against your pylance):** upstream docstrings disagree across pylance versions. The **current** `write_dataset` docstring (fetched 2026-07-08) documents `data_storage_version=None` as *"will use the latest stable version"*; **older** pylance (the `use_legacy_format` era) defaulted `None` to the **legacy v1** format. Which one your install does is version-gated, so **do not rely on the default** — pass `data_storage_version` explicitly (e.g. `"2.0"` / `"stable"` / `"legacy"` per your pylance version) when the on-disk format matters, and confirm the exact accepted string vocabulary against your installed API reference. A silently-written legacy-v1 dataset is a real footgun for tooling that only reads v2.

```python
import lance
import duckdb

con = duckdb.connect()
rel = con.sql("""
    SELECT id, name, embedding
    FROM read_parquet('s3://bucket/staging/*.parquet')
""")

# streaming write — reader, not table; memory stays bounded
reader = rel.to_arrow_reader(batch_size=1_000_000)
lance.write_dataset(
    reader,
    "s3://data-sink/active/entities_lance",
    mode="overwrite",
    max_rows_per_file=1_048_576,
    storage_options={...},          # see §2.5
)
```

### 2.3 Lance → Arrow → DuckDB (read-back)

Open the dataset, hand it to DuckDB. Two forms:

```python
import lance, duckdb

ds = lance.dataset("s3://data-sink/active/entities_lance", storage_options={...})

# (a) replacement scan: a pyarrow Dataset/Table variable name is queryable directly
con = duckdb.connect()
con.sql("SELECT count(*) FROM ds").show()          # `ds` picked up from local scope

# (b) explicit register (stable, thread-safe, name-controlled)
con.register("entities", ds)
con.sql("SELECT id FROM entities WHERE name = 'acme' LIMIT 10").show()

# (c) materialize a subset via Lance, then hand the Arrow table over
tbl = ds.to_table(columns=["id", "name"], limit=1000)   # pyarrow.Table
con.sql("SELECT * FROM tbl").show()
```

Notes on the read side:
- **Replacement scan (a):** DuckDB's Python replacement scan resolves an unqualified table name against local/global Python variables that are pyarrow `Table` / `Dataset` / `RecordBatchReader` (and pandas/polars). A `lance.LanceDataset` exposes the pyarrow Dataset interface, so it is scannable. Column projection and simple predicate pushdown flow into the Lance scan.
- **`register` (b):** `con.register(name, obj)` binds an explicit name — preferred in library/pipeline code where relying on variable-name capture is fragile.
- **`ds.to_table(...)`:** `LanceDataset.to_table()` returns a `pyarrow.Table`; supports `columns=`, `filter=`, `limit=`, `offset=`, and vector-search kwargs. Use it to push projection/predicate into Lance before Arrow crosses into DuckDB.

> **`lance.dataset` / `LanceDataset` signature — partial, needs confirmation:** the fetched pylance index pages did not render the full verbatim `lance.dataset(uri, version=None, asof=None, ..., storage_options=None, ...)` signature. Confirmed present: the `storage_options` parameter, `LanceDataset.to_table() -> Table`, and the storage-options accessors (`initial_storage_options`, `latest_storage_options()`). Treat the remaining `lance.dataset` kwargs (`version`, `asof`, `block_size`, `index_cache_size`, etc.) as **needs-confirmation** against your installed pylance API reference — do not assume beyond `uri` + `storage_options`.

### 2.4 Why Arrow, and where zero-copy holds

Arrow is the interchange because DuckDB and Lance both speak the Arrow C data interface. Handing a `pyarrow.Table` / `RecordBatchReader` across the boundary transfers buffer pointers, not row copies, when the physical layouts match. Caveats:
- Zero-copy holds for the common columnar types (primitives, fixed-size lists/vectors, strings). Type coercion (e.g. a cast, a DuckDB-only type with no Arrow equivalent) forces a copy.
- Streaming with `to_arrow_reader(...)` is what keeps a hundreds-of-millions-of-rows job inside a bounded memory budget — the RecordBatchReader yields fixed-size batches instead of one giant table.

### 2.5 Cloudflare R2 `storage_options` (Path B)

pylance `storage_options` for S3-compatible stores (source: `lance.org/guide/object_store/`). R2 = S3-compatible; **you must set both `region` and `endpoint`.**

```python
storage_options = {
    "access_key_id":     "<R2_ACCESS_KEY_ID>",
    "secret_access_key": "<R2_SECRET_ACCESS_KEY>",
    "region":            "auto",   # R2 ignores region but the key is required
    "endpoint":          "https://<ACCOUNT_ID>.r2.cloudflarestorage.com",
}
```

Accepted keys (both bare and `aws_`-prefixed aliases work):

| Bare key | Alias | Purpose |
|----------|-------|---------|
| `access_key_id` | `aws_access_key_id` | credential |
| `secret_access_key` | `aws_secret_access_key` | credential |
| `session_token` | `aws_session_token` | temp credential |
| `region` | `aws_region` | required for S3-compatible; use `"auto"` for R2 |
| `endpoint` | `aws_endpoint` | R2: `https://<ACCOUNT_ID>.r2.cloudflarestorage.com` |
| `virtual_hosted_style_request` | `aws_virtual_hosted_style_request` | path vs vhost addressing |
| `allow_http` | — | permit non-TLS (MinIO/local only) |
| `connect_timeout` | — | connection timeout |
| `request_timeout` | — | per-request timeout |

The **same `storage_options` dict** is passed to both `lance.write_dataset(..., storage_options=...)` and `lance.dataset(uri, storage_options=...)`. It is Lance's own object-store config and is independent of any DuckDB `CREATE SECRET`.

---

## 3. Deprecations, renames, footguns — consolidated

- **`fetch_arrow_table()` / `fetch_record_batch()` / `fetch_arrow_reader()` (DuckDB Python)** → deprecated. Use **`to_arrow_table()`** and **`to_arrow_reader(batch_size=1_000_000)`** (the parameter is `batch_size`, not `chunk_size` — see §2.1). Third-party libs still emitting the deprecation warning should migrate.
- **pylance `use_legacy_format`** → superseded by **`data_storage_version`**. Default behavior of `data_storage_version=None` is version-drifted (current docstring: "latest stable"; older pylance: legacy v1). Set it explicitly when the on-disk format matters — do not rely on the default. ([§2.2](#22-arrow--lance-write--lancewrite_dataset))
- **Two separate secret namespaces:** DuckDB `httpfs` uses `CREATE SECRET (TYPE r2 / TYPE s3 …)`; the Lance extension uses `CREATE SECRET (TYPE lance …)`; pylance uses a Python `storage_options` dict. None of the three share state. Configure the one that matches the path you're on.
- **R2 not named in Lance docs:** both the extension and pylance reach R2 as generic S3-compatible (custom `endpoint` + `region`), not via an R2-specific type.
- **`mode='create'` is Path B (pylance) only, and errors if the dataset exists** — `write_dataset(mode='create')` is the default and fails if `uri` already exists. The SQL `COPY … (FORMAT lance)` writer does **not** document a `'create'` MODE (only `'overwrite'` / `'append'`); use `'overwrite'` to create-or-replace on Path A, `'append'` to add. See [§1.3](#13-writing-a-lance-dataset--copy--format-lance).
- **Name collision:** `pylance` on PyPI (Lance format binding, import `lance`) vs Microsoft's "Pylance" VS Code language server — unrelated.
- **Extension↔DuckDB version pinning:** the `lance` extension is built against a specific DuckDB build. A DuckDB upgrade can leave the installed extension incompatible — reinstall/update to the matching tag. See [§1.8](#18-extension-version--maintenance--needs-confirmation).

---

## 4. Relevance to core-x

> **Relevance to core-x:** This file describes the core-x plane directly. The write side of the plane is **Path B** verbatim: DuckDB executes the projection/DISTINCT/cast over ephemeral Parquet, exports Arrow via `to_arrow_reader(batch_size=1_000_000)`, and hands the streaming RecordBatchReader to `lance.write_dataset(..., mode='overwrite'|'append', storage_options={R2 endpoint})` writing to `s3://data-sink/active/…`. Append-only immutable fragments map to Lance `mode='append'` producing new snapshot versions (never mutating prior fragments). The mandated `BTREE` scalar index on load-bearing resolution keys is available on both paths — Path A: `CREATE INDEX … USING BTREE`; the pylance equivalent is `LanceDataset.create_scalar_index(column, index_type="BTREE")` (method and `index_type="BTREE"` value confirmed 2026-07-08 against the pylance index-and-search reference; confirm the full kwarg set against your installed version). Read-back into DuckDB is [§2.3](#23-lance--arrow--duckdb-read-back) — `lance.dataset(uri, storage_options=…)` → replacement scan or `con.register`. Cloudflare R2 requires `region:"auto"` + `endpoint:https://<ACCOUNT_ID>.r2.cloudflarestorage.com` in every `storage_options` (write and read). Out-of-core bound: keep the write streaming (reader, not table) and set DuckDB `memory_limit`/`temp_directory` so the DuckDB-side transform spills rather than OOMs — see `06_configuration_memory_spill.md`. Zero-copy Arrow at the DuckDB↔Lance boundary is what makes hundreds-of-millions-of-rows loads viable; a type coercion at the boundary silently reintroduces a full copy.**

---

## 5. Unverified / needs confirmation

1. **`lance-duckdb` extension version / DuckDB pin** — releases page rendered 2025-dated `v0.5.x` tags while the demonstrating blog is 2026-05-21 on DuckDB 1.5.2. Verify installed version via `duckdb_extensions()` and the GitHub releases tag matching your DuckDB build. ([§1.8](#18-extension-version--maintenance--needs-confirmation))
2. **`COPY … (FORMAT lance)` full option set** beyond `MODE` and `WRITE_EMPTY_FILE` — not enumerated in fetched docs. Fine-grained write tuning confirmed only on Path B. ([§1.3](#13-writing-a-lance-dataset--copy--format-lance))
3. **`lance_scan(...)` table function** — not found as a documented function; reads go through path-as-table replacement scan. Do not assume a function form exists.
4. **Full `lance.dataset(...)` signature** — only `uri` + `storage_options` and `to_table()` verified verbatim; other kwargs (`version`, `asof`, cache sizes) unconfirmed. ([§2.3](#23-lance--arrow--duckdb-read-back))
5. **`data_storage_version` default + accepted string vocabulary** (`"2.0"` / `"stable"` / `"legacy"`) — the `None` default is version-drifted (current docstring "latest stable" vs older "legacy v1"), and the value vocabulary is version-gated; pass it explicitly and confirm against installed pylance. ([§2.2](#22-arrow--lance-write--lancewrite_dataset))
6. **pylance BTREE scalar-index full signature** — `LanceDataset.create_scalar_index(column, index_type="BTREE", ...)` confirmed to exist (2026-07-08, pylance index-and-search reference); the complete kwarg set (`name`, `replace`, index-specific tuning) is version-dependent — confirm before relying on specifics.

---

## 6. Sibling files (this domain)

- `00_overview.md` — DuckDB overview, editions, clients, versioning & release lines
- `01_python_client.md` — Python client: connect, execute, relational API, **replacement scans**
- `02_arrow_integration.md` — Apache Arrow: `to_arrow_table`/`to_arrow_reader`, `from_arrow`, `register`, ADBC
- `03_csv_import.md` — CSV import
- `04_parquet.md` — Parquet read/write, metadata, partitioning, pushdown
- `05_json.md` — JSON
- `06_configuration_memory_spill.md` — `memory_limit`, `threads`, `temp_directory`, **out-of-core spilling**
- `07_httpfs_s3_r2.md` — httpfs, S3 API & **Cloudflare R2** object storage
- `08_secrets_manager.md` — `CREATE SECRET`, types (s3/r2/gcs/azure/http), persistence
- `09_extensions_system.md` — INSTALL/LOAD, autoloading, core vs community, signing
- `10_core_extensions_catalog.md` — full core extensions list (includes `lance`)
- `11_quack_extension.md` — the `quack` extension & the DuckDB extension template
- `12_sql_essentials.md` — TRY_CAST, STRUCT/LIST/MAP/VARIANT, QUALIFY, window
- **`13_lance_interop.md`** — this file
