# 01 · DuckDB Processing — The Data Ingestion Plane

The ingestion plane is the bottom layer of the core-x Gen-3 stack: `DuckDB → Apache Arrow → LanceDB v2.0 → R2`. It is governed by [`ARCHITECTURE.md`](../../ARCHITECTURE.md) §4 ("Data plane") and embodied by the reference worker [`pipelines/sam_gov/sam_opps_bulk.py`](../../pipelines/sam_gov/sam_opps_bulk.py). This document is the absolute API contract for that plane. Every method name, parameter, and import below is exact. A wrong name here propagates into every downstream worker.

Sibling references: [`02_lancedb_storage.md`](02_lancedb_storage.md) (persistence plane), [`03_modal_compute.md`](03_modal_compute.md) (the Modal worker the transform runs inside), [`04_trigger_orchestration.md`](04_trigger_orchestration.md) (the Trigger v4 control plane that dispatches it).

---

## 1. Role of the ingestion plane — Python is I/O only; DuckDB transforms 100%

The division of labor is law, not convention. Three roles, three boundaries:

| Stage | Owner | Allowed operations | Forbidden |
|---|---|---|---|
| Acquire | **Python** | Stream bytes to `/tmp`, transcode encoding, open connections, set credentials | Any parse, filter, projection, cast, or reshape |
| Transform | **DuckDB** | `read_csv` / `read_json` / `read_parquet`, `TRY_CAST`, projection, filter, dedup, nesting — **all of it** | — |
| Interchange | **Apache Arrow** | Zero-copy hand-off of DuckDB's columnar buffers to `lance.write_dataset` | pandas, `dict`/`list` row materialization, JSON-text round-trips |

**Python touches bytes, never rows.** The Python layer streams the source to local scratch and configures credentials. It never inspects, filters, or casts a value. Every projection, every coercion, every filter, every reshape happens inside one DuckDB SQL statement.

**DuckDB performs 100% of transformation.** Per [`ARCHITECTURE.md`](../../ARCHITECTURE.md) §4: `read_csv(..., all_varchar=true)` on ingest, `TRY_CAST` for every type coercion, all projection / filter / shaping in SQL. The transform is a single declarative statement, not a procedural pipeline.

**Output is zero-copy Apache Arrow.** The transform's result is exported as an Arrow table — DuckDB hands pyarrow its internal columnar buffers without a row-materialization round-trip. That Arrow object is passed *directly* to `lance.write_dataset`. Nothing sits between DuckDB and Lance except an Arrow buffer.

> ### Law: Apache Arrow is the only in-memory interchange
> **pandas is forbidden in the ingestion plane.** No `.df()`, no `.fetchdf()`, no `import pandas`. **Materializing rows as Python `dict`/`list` is forbidden** — no `.fetchall()`, no `.fetchone()`, no list-of-dicts. DuckDB → Arrow → Lance is a buffer hand-off, not a row loop. Any intermediate that copies columnar data into row-oriented Python objects violates the zero-copy mandate and is rejected on sight.

The canonical realization, mirrored exactly from [`pipelines/sam_gov/sam_opps_bulk.py`](../../pipelines/sam_gov/sam_opps_bulk.py):

```python
"""core-x data plane: Python = I/O only; DuckDB = 100% transform; Arrow zero-copy → Lance."""
import datetime as dt
import os

import duckdb
import lance  # provided by pylance; lancedb does NOT re-export `lance`
import requests

SCRATCH_CSV_PATH = "/tmp/sam_opps_full.csv"
LANCE_BASE_URI = os.environ.get("SAM_OPPS_LANCE_URI", "s3://sam-gov-opps/active/")

started_at = dt.datetime.now(dt.timezone.utc)
snapshot_date = started_at.date().isoformat()

# 1) PYTHON — I/O ONLY. Stream the bulk payload to fast local scratch; no parsing.
with requests.get(os.environ["SAM_OPPS_CSV_URL"], stream=True, timeout=(30, 600)) as resp:
    resp.raise_for_status()
    with open(SCRATCH_CSV_PATH, "wb") as fh:
        for chunk in resp.iter_content(chunk_size=1 << 20):
            if chunk:
                # cp1252 → utf-8 transcode-on-write. See §2.5 — this is an I/O
                # concern, not a transform, and it is lossless (cp1252 is single-byte).
                fh.write(chunk.decode("cp1252", errors="replace").encode("utf-8"))

# 2) DUCKDB — 100% of the transform; export ZERO-COPY to Arrow.
con = duckdb.connect(":memory:")
try:
    con.execute("PRAGMA threads=4;")
    # to_arrow_table() is the CURRENT/recommended export (see §4). Zero-copy:
    # DuckDB hands pyarrow its columnar buffers with no row round-trip.
    arrow_table = con.execute(TRANSFORM_SQL, [SCRATCH_CSV_PATH, snapshot_date]).to_arrow_table()
finally:
    con.close()

# 3) PYTHON — I/O ONLY. Commit the Arrow buffer directly to Lance on R2. See 02_lancedb_storage.md.
lance.write_dataset(
    arrow_table,
    LANCE_BASE_URI,
    mode="overwrite",
    data_storage_version="2.0",
    storage_options=_r2_storage_options(),
)
```

> **On-disk drift in the reference worker.** [`pipelines/sam_gov/sam_opps_bulk.py`](../../pipelines/sam_gov/sam_opps_bulk.py) line 245 currently calls `.fetch_arrow_table()`. That method is **deprecated** as of DuckDB 1.5.x (see §4). It is functional today but floats toward removal at v2.0. New workers MUST write `.to_arrow_table()`. Migrate the reference worker's single call when touched.

---

## 2. Streaming external datasets into DuckDB

DuckDB reads remote and local sources natively. Two paths exist; the choice is dictated by the source format and authentication.

### 2.1 `httpfs` — direct reads over HTTP(S) / S3 / R2

The `httpfs` extension enables `read_csv` / `read_json` / `read_parquet` directly against `https://`, `s3://`, `r2://`, and `gs://` URLs.

```sql
-- httpfs is an AUTOLOADABLE core extension: it loads on first use of any of its
-- functionality, so the explicit INSTALL/LOAD is OPTIONAL (but still valid).
INSTALL httpfs;
LOAD httpfs;

-- Direct query of a remote Parquet (range-request partial reads via metadata):
SELECT * FROM read_parquet('https://host/snapshot/part-0.parquet');
```

> ### Format nuance that decides the ingest path — CSV is downloaded WHOLE
> For CSV, `httpfs` downloads the file **entirely in most cases**, "due to the row-based nature of the format." Only **Parquet** gets HTTP range-request partial reads via its metadata footer. A direct `read_csv('https://…')` therefore buys **no streaming win** for a large CSV — the whole file is pulled regardless. This is why the SAM.gov bulk path (a large unauthenticated CSV) streams to `/tmp` first (§2.4): `httpfs` would download the whole CSV anyway, **and** `httpfs` performs no transcoding, so it would feed raw cp1252 bytes to DuckDB's utf-8/latin-1-only core reader and silently corrupt them (§2.5).

### 2.2 `CREATE SECRET` — DuckDB-side R2 / S3 credentials

When DuckDB itself reads an authenticated object store (e.g. re-reading a prior Parquet snapshot from R2), credentials are supplied through DuckDB's **Secrets Manager**. Cloudflare R2 has a **first-class secret type** that derives the endpoint from the account id.

```sql
-- R2: TYPE r2 auto-derives the endpoint from ACCOUNT_ID. R2 secrets bind ONLY
-- to r2:// URLs.
CREATE OR REPLACE SECRET r2_secret (
    TYPE r2,
    KEY_ID     '<R2_ACCESS_KEY_ID>',
    SECRET     '<R2_SECRET_ACCESS_KEY>',
    ACCOUNT_ID '<R2_ACCOUNT_ID>'
);
SELECT count(*) FROM read_parquet('r2://sam-gov-opps/active/part-0.parquet');

-- Equivalent via the generic S3 type with an EXPLICIT R2 endpoint (binds to
-- s3:// URLs; R2 is path-style, so URL_STYLE 'path' is mandatory).
CREATE OR REPLACE SECRET r2_via_s3 (
    TYPE s3, PROVIDER config,
    KEY_ID    '<R2_ACCESS_KEY_ID>',
    SECRET    '<R2_SECRET_ACCESS_KEY>',
    REGION    'auto',
    ENDPOINT  '<R2_ACCOUNT_ID>.r2.cloudflarestorage.com',
    URL_STYLE 'path',
    USE_SSL   true
);
```

S3/R2 secret parameter reference:

| Option | Applies to | Notes |
|---|---|---|
| `TYPE` | both | `r2` (auto-endpoint, `r2://` only) or `s3` (any S3-compatible store) |
| `KEY_ID` / `SECRET` | both | Access key id / secret access key |
| `ACCOUNT_ID` | **R2 only** | Derives the R2 endpoint; do **not** hand-build the `*.r2.cloudflarestorage.com` URL |
| `ENDPOINT` | both | Required for `TYPE s3` against R2 |
| `REGION` | both | `auto` for R2; default `us-east-1` for S3 |
| `URL_STYLE` | both | `vhost` (S3 default) / `path` (**required for R2 and GCS**) |
| `USE_SSL` | both | Default `true` |
| `SESSION_TOKEN` | both | Temporary credentials |
| `KMS_KEY_ID` / `REQUESTER_PAYS` | **S3 only** | Not valid for R2 |

> ### Two credential systems that MUST NOT be conflated
> DuckDB's `CREATE SECRET` and Lance's `storage_options` are **independent**. The reference worker authenticates **Lance** (pylance) via an `object_store` dict — `{aws_access_key_id, aws_secret_access_key, endpoint, region: "auto"}` on an `s3://` URI ([`pipelines/sam_gov/sam_opps_bulk.py`](../../pipelines/sam_gov/sam_opps_bulk.py) lines 114–132). That dict configures **Lance only**; it does **not** configure any DuckDB reader. A DuckDB R2 secret does **not** configure Lance. They cover different halves of the plane.
>
> **R2 scheme/secret coupling:** `TYPE r2` secrets bind only to `r2://` URLs. Querying an `s3://` URL against R2 requires `TYPE s3` with an explicit `ENDPOINT` and `URL_STYLE 'path'`. Mixing the URL scheme and secret type silently fails to authenticate.
>
> **CLI caveat:** `CREATE SECRET` statements are stored in the DuckDB CLI history as **plain text**. Never paste R2/S3 keys into an interactive CLI on a shared box; inject via env / `credential_chain` / programmatic connection.

For the SAM.gov reference feed today, the source is a **public unauthenticated CSV** read over plain `requests` — **no DuckDB secret is required at all**. The `CREATE SECRET` path is for the future case of DuckDB reading R2 directly.

### 2.3 HTTP-auth secret (rare — for token-gated bulk feeds)

```sql
-- Only when a bulk endpoint is token-gated. SAM.gov's canonical bulk path is
-- unauthenticated, so this is not used by the reference feed.
CREATE SECRET http_auth (TYPE http, BEARER_TOKEN '<token>');
-- or header form:
CREATE SECRET http_auth (TYPE http, EXTRA_HTTP_HEADERS MAP {'Authorization': 'Bearer <token>'});
```

### 2.4 The canonical pattern — stream to `/tmp`, then `read_csv`

For a **very large unauthenticated bulk CSV extract**, the mandated pattern is: Python streams the payload to local scratch, then DuckDB reads it off disk. This is **not** a regression versus a direct `httpfs` read (§2.1: `httpfs` downloads the whole CSV anyway) and it is what enables the cp1252 transcode (§2.5).

```python
import os
import requests

SCRATCH_CSV_PATH = "/tmp/sam_opps_full.csv"

# PYTHON I/O ONLY. Stream to fast local scratch (the container's ephemeral NVMe).
# `iter_content` keeps peak RSS flat regardless of file size — bytes flow
# straight to disk, never buffered whole in memory.
with requests.get(os.environ["SAM_OPPS_CSV_URL"], stream=True, timeout=(30, 600)) as resp:
    resp.raise_for_status()
    with open(SCRATCH_CSV_PATH, "wb") as fh:
        for chunk in resp.iter_content(chunk_size=1 << 20):
            if chunk:
                fh.write(chunk.decode("cp1252", errors="replace").encode("utf-8"))
```

### 2.5 The cp1252 → utf-8 transcode-on-write rule

Government CSV exports (SAM.gov included) are **Windows-1252 (cp1252)**, not UTF-8. DuckDB's **core** `read_csv` `encoding` option supports **only** `{utf-8, utf-16, latin-1}`.

> ### `latin-1` is NOT a safe substitute for cp1252
> Bytes `0x80`–`0x9F` — the smart quotes and em dashes that fill SAM's `Title` and `Description` fields — are **control characters in latin-1 (ISO-8859-1)** but **printable in cp1252**. Reading cp1252 bytes as latin-1 silently **mojibakes** them. The transcode is mandatory.

The fix lives in **Python**, on the write to `/tmp`: `chunk.decode("cp1252", errors="replace").encode("utf-8")`. This is an **I/O concern**, consistent with the "Python for I/O only" mandate — the SQL still performs 100% of the transform. It is **lossless**: cp1252 is single-byte, so a 1 MiB chunk boundary never splits a character and no rows are dropped.

> ### SQL-native alternative — the `encodings` core extension
> Recent DuckDB ships a core `encodings` extension adding 1000+ ICU encodings **including CP1252**:
> ```sql
> INSTALL encodings; LOAD encodings;
> FROM read_csv('f.csv', encoding = 'CP1252', all_varchar = true);
> ```
> core-x **keeps the Python transcode**: it holds the byte-level concern in Python (per the I/O-only mandate), avoids a heavier autoloaded dependency, and works even for the direct-`https` case where `httpfs` would otherwise feed raw cp1252 bytes to a utf-8/latin-1-only core reader. Document the extension as the SQL-native option; do not adopt it for the reference path.

### 2.6 Defensive ingest idiom — `all_varchar=true` + `TRY_CAST`

Two non-negotiable rules for messy government feeds, both already codified in [`ARCHITECTURE.md`](../../ARCHITECTURE.md) §4 and the reference worker's `TRANSFORM_SQL`:

1. **`read_csv(..., all_varchar = true)`** — skip type detection; read every column as `VARCHAR`. Eliminates type-inference surprises on a 40+ column feed where any cell may be malformed. `all_varchar` is **`read_csv`-only** (not `COPY`, and it overrides auto-detection).
2. **`TRY_CAST(expr AS T)` for every coercion** — returns `NULL` on failure instead of aborting the load. A bare `CAST` aborts the **entire** load on the first malformed cell in a 40+ column feed.

The canonical transform, mirrored from [`pipelines/sam_gov/sam_opps_bulk.py`](../../pipelines/sam_gov/sam_opps_bulk.py) lines 71–111 (parameterized: the `?` placeholders keep the controlled `/tmp` path and snapshot date out of string interpolation):

```python
TRANSFORM_SQL = """
WITH raw AS (
    SELECT *
    FROM read_csv(
        ?,                         -- controlled /tmp path, bound positionally
        all_varchar = true,        -- read_csv-only; no type-inference surprises
        header = true,
        sample_size = -1,          -- scan all rows for detection (moot under all_varchar; harmless)
        ignore_errors = false
    )
)
SELECT
    nullif(trim("NoticeId"), '')                 AS notice_id,
    nullif(trim("Title"), '')                    AS title,
    nullif(trim("Sol#"), '')                     AS solicitation_number,
    nullif(trim("Department/Ind.Agency"), '')    AS department_agency,
    TRY_CAST("PostedDate" AS TIMESTAMP)          AS posted_date,
    nullif(trim("Type"), '')                     AS notice_type,
    nullif(trim("SetASide"), '')                 AS set_aside,
    TRY_CAST("ResponseDeadLine" AS TIMESTAMPTZ)  AS response_deadline,
    nullif(trim("NaicsCode"), '')                AS naics_code,
    TRY_CAST("AwardDate" AS DATE)                AS award_date,
    -- strip '$' and thousands separators before the numeric coercion:
    TRY_CAST(replace(replace("Award$", '$', ''), ',', '') AS DOUBLE) AS award_amount,
    nullif(trim("Awardee"), '')                  AS awardee,
    nullif(trim("Description"), '')              AS description,
    CAST(? AS DATE)                              AS snapshot_date  -- snapshot_date bound positionally
FROM raw
WHERE upper(trim("Active")) = 'YES'
"""
```

Optional hardening: `store_rejects = true` (with a `rejects_table`) quarantines malformed rows into a side table instead of failing — an alternative to `ignore_errors` when bad lines must be captured rather than skipped.

---

## 3. Messy semi-structured JSON → optimized binary nested columns

The objective: land messy JSON as **optimized binary nested columns** that serialize to **nested binary Arrow** (struct / list / map) on export, never as text and never as a type the Arrow/Lance boundary cannot carry.

> ### Do not write a `VARIANT` column on the wire to Arrow / Lance
> DuckDB's native `VARIANT` type **is real** and binary-stored as of DuckDB 1.5.0 ("Variegata", 2026-03-09) — it stores typed, self-describing binary per row, distinct from the JSON type (which is physically `VARCHAR` text). It is GA, not preview. **But it MUST NOT be the on-the-wire column type into `to_arrow_table()` / Lance**, for two reasons:
> 1. **No canonical Arrow type.** `VARIANT` has no Arrow mapping; `SELECT`ing a `VARIANT` column into `to_arrow_table()` is the **unsupported export seam** — DuckDB already raises on several unsupported/extension Arrow types, and pylance has no native `VARIANT` mapping.
> 2. **Version floor.** `VARIANT` is **1.5.0+ only** (absent from the 1.4.x LTS line). The reference worker pins `duckdb>=1.1` ([`pipelines/sam_gov/sam_opps_bulk.py`](../../pipelines/sam_gov/sam_opps_bulk.py) line 52), which does **not** guarantee `VARIANT`.
>
> `VARIANT` is legitimate **inside DuckDB and for Parquet** (incl. Snowflake-compatible shredding). For the core-x `DuckDB → Arrow → Lance` plane, **project to `STRUCT` / `MAP` / `LIST` before Arrow export.** Use the path below.

### 3.1 The canonical path — `read_json` / `read_json_auto` → cast to `STRUCT`

`read_json_auto` is an alias of `read_json`; it auto-detects nested schema into native `STRUCT` / `LIST` / `MAP`. Pin types with `columns={...}` for a stable contract.

> The **JSON logical type is physically `VARCHAR`** (text). Leaving a column typed `JSON` exports as an Arrow **string**, not "optimized binary nesting." You get nested binary Arrow **only** by casting `JSON → STRUCT/MAP/LIST`. Do not conflate "JSON type" with "binary nested."

```python
import duckdb

con = duckdb.connect(":memory:")
try:
    sql = """
    SELECT
        nullif(trim(id), '')                       AS id,
        -- Cast a raw JSON column into a native STRUCT. THIS is what serializes
        -- to NESTED BINARY Arrow (struct layout) on export — not a text string.
        payload::JSON::STRUCT(name VARCHAR, tags VARCHAR[], score INTEGER) AS payload,
        -- Scalar pulls: ->> returns VARCHAR, -> returns JSON.
        payload ->> '$.name'                       AS name_flat
    FROM read_json(
        ?,
        columns = {id: 'VARCHAR', payload: 'JSON'},
        format  = 'newline_delimited'   -- 'auto' | 'newline_delimited' | 'array' | 'unstructured'
    )
    """
    # arrow_table['payload'] is a pyarrow StructArray (struct<name, tags, score>)
    # → Lance stores it as real nested columns: queryable and compressible.
    arrow_table = con.execute(sql, ["/tmp/records.ndjson"]).to_arrow_table()
finally:
    con.close()
```

### 3.2 JSON shaping reference

| Goal | Function / cast | Returns |
|---|---|---|
| Auto-detect nested schema | `read_json_auto('f.json')` / `read_json(..., columns={...})` | native `STRUCT`/`LIST`/`MAP` |
| Cast whole object to nesting | `payload::JSON::STRUCT(a INTEGER, b VARCHAR)` | `STRUCT` (→ nested binary Arrow) |
| Function-form shaping | `json_transform(payload, '{"a":"INTEGER","b":"VARCHAR"}')` (alias `from_json`) | typed value; `json_transform_strict` raises on mismatch |
| Derive a structure | `json_structure(payload)` | structure descriptor |
| Scalar extract (text) | `payload ->> '$.a'` / `json_extract_string` | `VARCHAR` |
| Scalar extract (json) | `payload -> '$.a'` / `json_extract` | `JSON` (text) |

The rule restated: extract scalars with `->>` / `json_extract_string`; for **whole-object nesting**, cast to `STRUCT`/`MAP`/`LIST` so `to_arrow_table()` emits nested binary Arrow that Lance stores as real nested columns.

---

## 4. Zero-copy Arrow — the only interchange

DuckDB exports its result by handing pyarrow its internal columnar buffers — **zero-copy**, no row round-trip. pyarrow then owns the buffers. The Arrow object is passed **directly** to `lance.write_dataset`.

### 4.1 Export — current API

| Method (on connection **or** relation) | Returns | Status |
|---|---|---|
| `.to_arrow_table(batch_size=1_000_000)` | `pyarrow.lib.Table` | **CURRENT — use this** |
| `.to_arrow_reader(batch_size=1_000_000)` | `pyarrow.lib.RecordBatchReader` (streaming) | **CURRENT — use this for very large results** |
| `.arrow(rows_per_batch=1_000_000)` | `pyarrow.lib.RecordBatchReader` | alias of `to_arrow_reader` |
| `.fetch_arrow_table(...)` | `pyarrow.lib.Table` | **DEPRECATED (1.5.x)** |
| `.fetch_record_batch(...)` | `pyarrow.lib.RecordBatchReader` | **DEPRECATED (1.5.x)** |

> ### Deprecated and renamed — do not write
> - **`fetch_arrow_table()` / `fetch_record_batch()` — DEPRECATED as of DuckDB 1.5.x.** The doc banner reads: *"The `fetch_arrow_table`, `fetch_record_batch`, and `fetch_arrow_reader` functions are deprecated. Use `to_arrow_table` and `to_arrow_reader` instead."* Still callable, slated for removal at/around v2.0 (Sept 2026). The reference worker ([`pipelines/sam_gov/sam_opps_bulk.py`](../../pipelines/sam_gov/sam_opps_bulk.py) line 245) still calls `.fetch_arrow_table()` — migrate to `.to_arrow_table()`.
> - **`.arrow()` does NOT return a `pyarrow.Table`.** It is an alias of `to_arrow_reader()` and returns a `pyarrow.lib.RecordBatchReader`. Code that does `.arrow().num_rows` or treats the result as a Table **will break**. Use `.to_arrow_table()` for a Table.
> - **There is no method named `record_batch`.** The streaming reader is `to_arrow_reader` (current) / `fetch_record_batch` (deprecated).

Full materialization (canonical for the SAM.gov snapshot, which fits in the 8 GiB worker):

```python
arrow_table = con.execute(TRANSFORM_SQL, [SCRATCH_CSV_PATH, snapshot_date]).to_arrow_table()
```

Streaming export when a result outgrows worker memory — bounded RSS regardless of row count:

```python
import duckdb, lance

con = duckdb.connect(":memory:")
con.execute("PRAGMA threads=4;")

CHUNK = 1_000_000
# to_arrow_reader(batch_size) returns a pyarrow.lib.RecordBatchReader (streaming).
reader = con.execute(TRANSFORM_SQL, [SCRATCH_CSV_PATH, snapshot_date]).to_arrow_reader(CHUNK)

# Lance accepts a RecordBatchReader directly; pass its schema explicitly. See 02_lancedb_storage.md.
lance.write_dataset(
    reader,
    LANCE_BASE_URI,
    schema=reader.schema,
    mode="overwrite",
    data_storage_version="2.0",
    storage_options=_r2_storage_options(),
)
# A RecordBatchReader is consumed ONCE (one-shot streaming). Manual loop equivalent:
# while (batch := reader.read_next_batch()):   # pyarrow.RecordBatch; StopIteration when empty
#     ...
```

### 4.2 Ingest — zero-copy Arrow back into DuckDB for a second SQL pass

To run a second SQL pass over an Arrow table (e.g. dedup before writing Lance) without copying, re-enter DuckDB via `from_arrow`, a **replacement scan**, or `register`. All three are zero-copy — DuckDB scans pyarrow's buffers in place.

```python
import duckdb

con = duckdb.connect(":memory:")

# (a) REPLACEMENT SCAN — reference the Python variable holding the Arrow table
#     directly by name inside SQL. Zero-copy.
deduped = con.execute(
    """
    SELECT * EXCLUDE (rn) FROM (
        SELECT *, row_number() OVER (PARTITION BY notice_id ORDER BY posted_date DESC) AS rn
        FROM arrow_table          -- the pyarrow.Table local variable, scanned in place
    ) WHERE rn = 1
    """
).to_arrow_table()

# (b) EXPLICIT HANDLE — no reliance on variable-name capture.
rel = con.from_arrow(arrow_table)         # -> DuckDBPyRelation, zero-copy
n = rel.aggregate("count(*) AS n").to_arrow_table()

# (c) NAMED VIEW — useful across multiple statements.
con.register("opps", arrow_table)
con.execute("SELECT naics_code, count(*) FROM opps GROUP BY 1").to_arrow_table()
```

Supported Arrow inputs for `from_arrow` / replacement scans: `pyarrow.Table`, `pyarrow.dataset.Dataset`, `pyarrow.dataset.Scanner`, `pyarrow.RecordBatchReader` (the reader is one-shot). `con.register("name", arrow_obj)` is the explicit alias if variable-name capture is undesirable.

> ### Law: Arrow is the only interchange — pandas and dict materialization are forbidden
> The entire plane is buffer-to-buffer. **Never** introduce pandas (`.df()`, `.fetchdf()`, `import pandas`) or row materialization (`.fetchall()`, list-of-dicts) between DuckDB and Lance. The transform's output is an Arrow `Table` or `RecordBatchReader` and nothing else. Any heavy nested-`dict` intermediate is a violation.

### 4.3 Version pinning

| Pin | Resolves to | core-x stance |
|---|---|---|
| `duckdb>=1.1` (reference worker, line 52) | newest at build time (today **1.5.2**) | **loose** — floats across the 1.5 → 2.0 boundary where deprecated `fetch_*` may be removed, and does **not** guarantee `VARIANT` (needs `>=1.5.0`) |
| `duckdb>=1.5,<2` | 1.5.x current line | **recommended** — includes `VARIANT` and `to_arrow_table`/`to_arrow_reader`; bounded below v2.0 |
| `duckdb~=1.4` | 1.4.x LTS ("Andium") | use only if `VARIANT` / friendly-CLI features are not needed (LTS EOL Sept 2026) |

Latest stable as of 2026-05-31 is **DuckDB 1.5.2** (2026-04-13). `to_arrow_table` / `to_arrow_reader` are stable on both the 1.4 LTS and 1.5 current lines. New workers SHOULD pin `duckdb>=1.5,<2` and MUST write `.to_arrow_table()` / `.to_arrow_reader()`.

> Optional future migration: a `lance` **core extension** (DuckDB 1.5.1+) can read/write Lance via `COPY ... TO ... (FORMAT lance, MODE 'overwrite'|'append')` plus a replacement scan, with R2/S3 through a `TYPE lance` secret — moving the Lance write out of pylance and into DuckDB SQL. This changes the write path and credential model; [`ARCHITECTURE.md`](../../ARCHITECTURE.md) §4 currently mandates pylance `lance.write_dataset(..., data_storage_version="2.0")`. Treat as a deliberate migration, not a drop-in.

---

## 5. Handoff to the persistence plane

The Arrow object — `pyarrow.Table` (full) or `pyarrow.RecordBatchReader` (streaming) — is passed **directly** to `lance.write_dataset`, which writes **LanceDB v2.0 to Cloudflare R2**. No catalog round-trip, no Parquet landing step, no Iceberg, no Polaris ([`ARCHITECTURE.md`](../../ARCHITECTURE.md) §4 and "Forbidden / retired").

```python
import lance  # pylance — provides `import lance`; lancedb does NOT re-export it

lance.write_dataset(
    arrow_table,                      # the Arrow buffer straight from DuckDB — no copy, no pandas
    LANCE_BASE_URI,                   # "s3://sam-gov-opps/active/"
    mode="overwrite",                 # daily full snapshot of currently-active notices
    data_storage_version="2.0",       # MANDATED — forces Lance v2.0 format
    storage_options=_r2_storage_options(),  # Lance/object_store creds — SEPARATE from DuckDB CREATE SECRET (§2.2)
)
```

The R2 `storage_options` builder (Lance's `object_store` path — distinct from DuckDB's `CREATE SECRET`), mirrored from [`pipelines/sam_gov/sam_opps_bulk.py`](../../pipelines/sam_gov/sam_opps_bulk.py) lines 114–132:

```python
def _r2_storage_options() -> dict[str, str]:
    """object_store options for Cloudflare R2, sourced from the Modal secret."""
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

This entire transform runs **inside a Modal compute worker** — the `@app.function` defined in [`pipelines/sam_gov/sam_opps_bulk.py`](../../pipelines/sam_gov/sam_opps_bulk.py), part of the `sam-gov-pipelines` Modal app. The worker is never exposed; it is spawned by the Universal Dispatcher ([`core/modal_dispatcher.py`](../../core/modal_dispatcher.py)) and, on terminal state, writes its run row to `ops.*` via psycopg and POSTs the Trigger waitpoint callback URL. Lance dataset layout, v2.0 format, and `BTREE` scalar indexing: see [`02_lancedb_storage.md`](02_lancedb_storage.md). The worker image, secrets, dispatch, and terminal-state contract: see [`03_modal_compute.md`](03_modal_compute.md). The Trigger v4 waitpoint-token control plane that dispatches it: see [`04_trigger_orchestration.md`](04_trigger_orchestration.md).
