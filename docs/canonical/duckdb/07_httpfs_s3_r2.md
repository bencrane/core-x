# httpfs, S3 API & Cloudflare R2 — reading/writing object storage

> Canonical upstream reference — fetched 2026-07-08 from official DuckDB documentation and source repositories. Documents upstream behavior as-is; this is NOT core-x doctrine. Treat the version numbers and API signatures here as fetched-date ground truth.
>
> Primary sources:
> - https://duckdb.org/docs/stable/core_extensions/httpfs/overview (redirects to `/docs/current/core_extensions/httpfs/overview.html`) — what the `httpfs` extension is, protocols supported, autoloading.
> - https://duckdb.org/docs/current/core_extensions/httpfs/https.html — HTTP(S) reads, range-request partial reading for Parquet vs whole-file CSV, `http_*` config, HTTP secret type.
> - https://duckdb.org/docs/current/core_extensions/httpfs/s3api.html — `s3://` scheme, S3 secret parameters, `PROVIDER config` vs `credential_chain`, Cloudflare R2 (`r2://`, `TYPE r2`), GCS, COPY TO, partitioned writes, globbing.
> - https://duckdb.org/docs/current/core_extensions/httpfs/hugging_face.html — `hf://` scheme, `TYPE huggingface` secret, revisions/branches.
> - https://duckdb.org/docs/current/core_extensions/httpfs/s3api_legacy_authentication.html — deprecated `SET s3_*` settings and their env-var mappings.
> - https://duckdb.org/2026/06/19/... / https://github.com/duckdb/duckdb/releases — release line: current stable **DuckDB 1.5.4** (2026-06-19); **1.4.x LTS** ("Andium") supported to Sept 2026.

Scope: How DuckDB's `httpfs` core extension reads and writes remote object storage — HTTP(S), the S3 API (`s3://`), Cloudflare R2 (`r2://`), GCS (`gcs://`/`gs://`), and Hugging Face (`hf://`) — including authentication, endpoint/URL-style config, partitioned writes, and the DuckDB-side R2 access path that is orthogonal to Lance's own storage layer.

---

## 1. What `httpfs` is and how it loads

`httpfs` is an **autoloadable core extension** that implements a virtual filesystem for remote files. Once present, any DuckDB function that takes a path — `read_csv`, `read_parquet`, `read_json` / `read_json_auto`, `COPY ... TO`, `glob()`, and bare `FROM '<uri>'` replacement scans — accepts a remote URI transparently.

Protocols it enables:

| Scheme | Backend | Read | Write | Glob |
|---|---|---|---|---|
| `http://`, `https://` | Any HTTP server | Yes | **No** (read-only) | No |
| `s3://` | AWS S3 & S3-compatible (MinIO, Tigris, lakeFS, Backblaze B2, etc.) | Yes | Yes (`COPY TO`) | Yes |
| `r2://` | Cloudflare R2 | Yes | Yes | Yes |
| `gcs://`, `gs://` | Google Cloud Storage (HMAC/interoperability API) | Yes | Yes | Yes |
| `hf://` | Hugging Face datasets | Yes | No | Yes |

### Autoloading vs explicit install

By default DuckDB **autoloads and auto-installs** `httpfs` the first time a remote URI is touched (governed by the `autoinstall_known_extensions` and `autoload_known_extensions` settings, both `true` by default). To be explicit or to work offline:

```sql
INSTALL httpfs;
LOAD httpfs;
```

`httpfs` is a signed core extension distributed from the official extension repository. See [`09_extensions_system.md`](09_extensions_system.md) for INSTALL/LOAD/autoload mechanics and [`10_core_extensions_catalog.md`](10_core_extensions_catalog.md) for where it sits in the core catalog.

---

## 2. HTTP(S) reads — the Parquet vs CSV nuance

Over `http(s)://`, **only reading is supported**. The critical performance property:

- **Parquet — partial reads via HTTP range requests.** DuckDB combines the Parquet footer/metadata with HTTP `Range:` requests to fetch **only the byte ranges the query needs**. A `count(*)` reads just the metadata; a projection of one column fetches only that column's pages. This is what makes remote Parquet over HTTP practical.

  ```sql
  -- reads only the Parquet metadata (row-group counts), not the data
  SELECT count(*) FROM 'https://domain.tld/file.parquet';

  -- reads only the byte ranges backing column_a
  SELECT column_a FROM 'https://domain.tld/file.parquet';
  ```

- **CSV / line-based JSON — whole-file download.** Row-based, non-seekable formats are downloaded in full before/while parsing; range requests give little benefit. Budget for pulling the entire object.

> **Footgun:** A single wide remote Parquet with predicate/projection pushdown is cheap; a directory of remote CSVs is not. Convert to Parquet before it lands remotely if you intend to query columns/subsets.

### HTTP config and authentication

Documented HTTP(S) knobs (set via `SET` or a `CREATE SECRET (TYPE http)`):

| Setting | Purpose |
|---|---|
| `http_proxy` | Proxy URL |
| `http_proxy_username` | Proxy auth username |
| `http_proxy_password` | Proxy auth password |
| `ca_cert_file` | Path to a custom CA certificate bundle |
| `enable_server_cert_verification` | Toggle TLS server-cert verification |

For authenticated HTTP endpoints, use an HTTP secret rather than embedding tokens in URLs:

```sql
CREATE SECRET http_auth (
    TYPE http,
    BEARER_TOKEN 'token'
);
```

> **Unverified / needs confirmation:** Additional retry/timeout/keep-alive knobs (`http_timeout`, `http_retries`, `http_retry_backoff`, `http_retry_wait_ms`, `http_keep_alive`, `force_download`) exist in various DuckDB versions but were **not present in the fetched HTTP(S) page**. Confirm names/defaults against your pinned DuckDB build's `duckdb_settings()` output before relying on them:
> ```sql
> SELECT name, value, description FROM duckdb_settings() WHERE name LIKE 'http\_%' ESCAPE '\';
> ```

---

## 3. S3 API (`s3://`)

Reading, writing, and globbing over the S3 API. Works against AWS S3 and any S3-compatible store (MinIO, Cloudflare R2, Google Cloud Storage via the interop API, lakeFS, Tigris, Backblaze B2, …) by pointing `ENDPOINT` and `URL_STYLE` at the target.

### 3.1 Reading

```sql
-- bare replacement scan (format inferred from extension)
SELECT * FROM 's3://your-bucket/filename.parquet';

-- explicit reader
SELECT * FROM read_parquet('s3://your-bucket/file.parquet');

-- a list of files
SELECT * FROM read_parquet([
    's3://your-bucket/file-1.parquet',
    's3://your-bucket/file-2.parquet'
]);

-- globbing (uses the ListObjectsV2 API)
SELECT * FROM read_parquet('s3://your-bucket/*.parquet');
SELECT count(*) FROM read_parquet('s3://your-bucket/folder*/100?/t[0-9].parquet');

-- add the source path as a column
SELECT * FROM read_parquet('s3://your-bucket/*.parquet', filename = true);
```

Glob wildcards: `*` (any run of characters), `?` (single character), `[0-9]` / `[a-z]` (character range). Hive-partitioned directory layouts (`.../key=value/...`) are supported for reading.

### 3.2 Writing (`COPY TO`)

```sql
-- single file
COPY table_name TO 's3://your-bucket/out.parquet' (FORMAT parquet);

-- Hive-partitioned dataset
COPY table TO 's3://your-bucket/partitioned' (
    FORMAT parquet,
    PARTITION_BY (part_col_a, part_col_b)
);
-- produces: s3://your-bucket/partitioned/part_col_a=<v>/part_col_b=<v>/data_<thread>.parquet

-- allow overwriting an existing partition tree
COPY table TO 's3://your-bucket/partitioned' (
    FORMAT parquet,
    PARTITION_BY (part_col_a, part_col_b),
    OVERWRITE_OR_IGNORE true
);
```

Writes use the S3 **multipart upload** API for large objects. Tunables:

| Setting | Effect |
|---|---|
| `s3_uploader_max_parts_per_file` | Caps parts per object; drives per-part size |
| `s3_uploader_max_filesize` | File-size threshold governing upload strategy |
| `s3_uploader_thread_limit` | Max concurrent upload threads |

### 3.3 S3 secret — `CREATE SECRET (TYPE s3)`

The modern, recommended path. See [`08_secrets_manager.md`](08_secrets_manager.md) for full CREATE SECRET semantics (scopes, `IN CONFIG` persistence, precedence). Minimal form:

```sql
CREATE OR REPLACE SECRET secret (
    TYPE s3,
    PROVIDER config,               -- explicit credentials
    KEY_ID 'AKIAIOSFODNN7EXAMPLE',
    SECRET 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
    REGION 'us-east-1'
);
```

**S3 secret parameters** (verbatim from the fetched s3api page; types/defaults as documented):

| Parameter | Type | Default | Accepted / notes |
|---|---|---|---|
| `KEY_ID` | STRING | — | Access key id |
| `SECRET` | STRING | — | Secret access key |
| `REGION` | STRING | `us-east-1` | AWS region |
| `SESSION_TOKEN` | STRING | — | Temporary STS session token |
| `ENDPOINT` | STRING | `s3.amazonaws.com` | Custom S3-compatible endpoint host[:port] |
| `URL_STYLE` | STRING | `vhost` (S3) | `vhost` or `path` — see §3.4 |
| `USE_SSL` | BOOLEAN | `true` | HTTPS vs HTTP |
| `VERIFY_SSL` | BOOLEAN | `true` | Verify TLS server certificate |
| `URL_COMPATIBILITY_MODE` | BOOLEAN | `true` | Escape/handle problematic URL characters in keys |
| `KMS_KEY_ID` | STRING | — | AWS KMS key ARN for SSE-KMS |
| `REQUESTER_PAYS` | BOOLEAN | `false` | Enable requester-pays buckets |
| `PROVIDER` | STRING | `config` | `config` (explicit keys) or `credential_chain` |
| `CHAIN` | STRING | — | Semicolon-separated chain order (with `credential_chain`) |
| `PROFILE` | STRING | — | Named AWS profile (with `credential_chain`) |
| `SCOPE` | STRING | — | URI prefix this secret applies to (secret routing) |

### 3.4 `URL_STYLE`: `vhost` vs `path`

- **`vhost`** (S3 default): `https://<bucket>.<endpoint>/<key>`. Requires the endpoint/DNS to resolve per-bucket subdomains.
- **`path`**: `https://<endpoint>/<bucket>/<key>`. Required by most S3-compatible stores (MinIO, and commonly R2 when addressed via the S3 endpoint) that don't do virtual-host-style bucket subdomains.

> **Footgun:** Pointing at a non-AWS endpoint while leaving `URL_STYLE 'vhost'` is the #1 cause of DNS/`NoSuchBucket`/TLS-SNI failures against MinIO and R2. Set `URL_STYLE 'path'` for those.

### 3.5 `PROVIDER config` vs `PROVIDER credential_chain`

- **`config`** — you supply `KEY_ID`/`SECRET` (and optionally `SESSION_TOKEN`) inline. Deterministic; no ambient dependency.
- **`credential_chain`** — DuckDB resolves credentials through the AWS SDK default chain. Optional `CHAIN` orders the sources (semicolon-separated); documented providers: `config`, `sts`, `sso`, `env`, `instance`, `process`. Optional `PROFILE` selects a named profile.

```sql
-- resolve via the AWS default chain
CREATE OR REPLACE SECRET secret (TYPE s3, PROVIDER credential_chain);

-- pin chain order: environment first, then shared config file
CREATE OR REPLACE SECRET secret (
    TYPE s3,
    PROVIDER credential_chain,
    CHAIN 'env;config'
);

-- named profile from ~/.aws/config
CREATE OR REPLACE SECRET secret (
    TYPE s3,
    PROVIDER credential_chain,
    CHAIN config,
    PROFILE 'my_profile'
);

-- SSE-KMS, scoped to a bucket sub-path
CREATE OR REPLACE SECRET secret (
    TYPE s3,
    PROVIDER credential_chain,
    CHAIN config,
    REGION 'eu-west-1',
    KMS_KEY_ID 'arn:aws:kms:region:account_id:key/key_id',
    SCOPE 's3://bucket-sub-path'
);
```

**Environment variables** picked up by `credential_chain` (`env`) and by the legacy settings (§6): `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, `AWS_REGION` / `AWS_DEFAULT_REGION`.

---

## 4. Cloudflare R2 (`r2://`)

Two equivalent ways to reach an R2 bucket from DuckDB.

### 4.1 Native R2 secret — `TYPE r2` (endpoint auto-derived from `ACCOUNT_ID`)

```sql
CREATE OR REPLACE SECRET secret (
    TYPE r2,
    KEY_ID 'AKIAIOSFODNN7EXAMPLE',
    SECRET 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
    ACCOUNT_ID 'my_account_id'   -- endpoint derived: <ACCOUNT_ID>.r2.cloudflarestorage.com
);

SELECT * FROM read_parquet('r2://my-bucket/some-file.parquet');
```

`TYPE r2` requires `ACCOUNT_ID` and derives the R2 endpoint for you. It supports both `PROVIDER config` and `PROVIDER credential_chain`; with the chain, credentials must be exposed as `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`. Use R2 **S3-API tokens** (Access Key ID + Secret) for `KEY_ID`/`SECRET` — not a Cloudflare API token.

### 4.2 Equivalent `TYPE s3` with explicit endpoint + `path` style

The same R2 bucket, addressed through the generic S3 secret and the `s3://` scheme:

```sql
CREATE OR REPLACE SECRET r2_via_s3 (
    TYPE s3,
    KEY_ID 'AKIAIOSFODNN7EXAMPLE',
    SECRET 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
    ENDPOINT '<ACCOUNT_ID>.r2.cloudflarestorage.com',
    URL_STYLE 'path',
    REGION 'auto'
);

SELECT * FROM read_parquet('s3://my-bucket/some-file.parquet');
```

Notes for R2:
- R2 has **no meaningful region**; `REGION 'auto'` (or `us-east-1`) is fine for signing.
- Use **`URL_STYLE 'path'`** with the explicit-endpoint form.
- Keep `USE_SSL true`.

> **Relevance to core-x:** DuckDB-side R2 access via `TYPE r2`/`TYPE s3` secrets is the path for reading raw ephemeral Parquet/CSV drops out of R2 and for any `COPY TO 's3://…'` staging. It is **completely separate** from how LanceDB reaches R2 — Lance uses its own `storage_options` dict (`aws_access_key_id`, `aws_secret_access_key`, `aws_endpoint`, `region`, `aws_virtual_hosted_style_request=false`) passed to `lance.write_dataset` / `lancedb.connect`, not a DuckDB secret. Configuring one does not configure the other; a pipeline that reads a Parquet from R2 with DuckDB and then writes a Lance dataset to R2 needs **both** credential paths wired independently. See [`13_lance_interop.md`](13_lance_interop.md).

---

## 5. Google Cloud Storage (`gcs://` / `gs://`) and Hugging Face (`hf://`)

### 5.1 GCS (interoperability / HMAC keys)

GCS is reached through its S3-compatible interoperability API using **HMAC keys**, not service-account JSON:

```sql
CREATE OR REPLACE SECRET secret (
    TYPE gcs,
    KEY_ID 'my_hmac_access_id',
    SECRET 'my_hmac_secret_key'
);

SELECT * FROM read_parquet('gcs://some/file.parquet');   -- gs:// also works
```

### 5.2 Hugging Face datasets (`hf://`)

Read-only access to Hugging Face dataset repos.

```
hf://datasets/<username>/<dataset>/<path_to_file>
```

```sql
SELECT * FROM 'hf://datasets/datasets-examples/doc-formats-csv-1/data.csv';

-- glob across a directory
SELECT count(*) AS count FROM 'hf://datasets/cais/mmlu/astronomy/*.parquet';
```

Revisions/branches use `@`:

```
hf://datasets/<username>/<dataset>@<branch>/<path_to_file>
```

`@~parquet` is a special branch where Hugging Face auto-generates Parquet versions of a dataset.

Authentication:

```sql
-- explicit token
CREATE SECRET hf_token (TYPE huggingface, TOKEN 'your_hf_token');

-- resolve from ~/.cache/huggingface/token
CREATE SECRET hf_token (TYPE huggingface, PROVIDER credential_chain);
```

---

## 6. Legacy S3 authentication (deprecated `SET s3_*`)

> **Deprecated.** The docs warn this "increases the risk of accidentally leaking secrets." The Secrets manager ([`08_secrets_manager.md`](08_secrets_manager.md)) has been the recommended path since **DuckDB 0.10.0**. Prefer `CREATE SECRET`. These settings remain for backward compatibility.

```sql
SET s3_region = 'us-east-1';
SET s3_endpoint = 'domain.tld:port';
SET s3_url_style = 'path';
SET s3_use_ssl = false;
SET s3_access_key_id = 'aws_access_key_id';
SET s3_secret_access_key = 'aws_secret_access_key';
SET s3_session_token = 'aws_session_token';
```

Environment-variable mappings (also consulted when the setting is unset):

| DuckDB setting | Environment variable(s) |
|---|---|
| `s3_region` | `AWS_REGION` or `AWS_DEFAULT_REGION` |
| `s3_access_key_id` | `AWS_ACCESS_KEY_ID` |
| `s3_secret_access_key` | `AWS_SECRET_ACCESS_KEY` |
| `s3_session_token` | `AWS_SESSION_TOKEN` |
| `s3_endpoint` | `DUCKDB_S3_ENDPOINT` |
| `s3_use_ssl` | `DUCKDB_S3_USE_SSL` |
| `s3_requester_pays` | `DUCKDB_S3_REQUESTER_PAYS` |

For R2 via legacy settings, set `s3_endpoint` to `<ACCOUNT_ID>.r2.cloudflarestorage.com` and `s3_url_style = 'path'`.

---

## 7. End-to-end examples

### 7.1 Read Parquet from R2 via an `r2://` secret

```sql
INSTALL httpfs; LOAD httpfs;

CREATE OR REPLACE SECRET r2_read (
    TYPE r2,
    KEY_ID '<R2_ACCESS_KEY_ID>',
    SECRET '<R2_SECRET_ACCESS_KEY>',
    ACCOUNT_ID '<CF_ACCOUNT_ID>'
);

SELECT count(*) FROM read_parquet('r2://data-sink/staging/batch-*.parquet');
```

### 7.2 Read the same bucket via an explicit `s3://` endpoint

```sql
INSTALL httpfs; LOAD httpfs;

CREATE OR REPLACE SECRET r2_via_s3 (
    TYPE s3,
    KEY_ID '<R2_ACCESS_KEY_ID>',
    SECRET '<R2_SECRET_ACCESS_KEY>',
    ENDPOINT '<CF_ACCOUNT_ID>.r2.cloudflarestorage.com',
    URL_STYLE 'path',
    REGION 'auto'
);

SELECT column_a, column_b
FROM read_parquet('s3://data-sink/staging/batch-*.parquet')
WHERE column_a > 0;   -- projection + predicate pushdown over HTTP range requests
```

### 7.3 Stage a projection back to R2 as partitioned Parquet

```sql
COPY (
    SELECT resolution_key, payload
    FROM read_parquet('r2://data-sink/staging/batch-*.parquet')
) TO 'r2://data-sink/active/derived' (
    FORMAT parquet,
    PARTITION_BY (resolution_key),
    OVERWRITE_OR_IGNORE true
);
```

> **Relevance to core-x:** This is the DuckDB half of the ingest path — raw ephemeral Parquet/CSV in R2 is read out-of-core, projected/DISTINCT'd/cast in DuckDB (with spill governed by `memory_limit`/`temp_directory`; see [`06_configuration_memory_spill.md`](06_configuration_memory_spill.md)), and streamed to Arrow ([`02_arrow_integration.md`](02_arrow_integration.md)) for the Lance writer. Writing the **Lance** system of record to R2 does **not** go through `COPY TO 's3://…'`; it goes through the Lance writer's own `storage_options`. Use DuckDB's `COPY TO`/httpfs only for transport-Parquet staging, never to produce the Lance SoR fragments. See [`13_lance_interop.md`](13_lance_interop.md).

---

## 8. Version notes, deprecations, footguns

- **Current stable: DuckDB 1.5.4** (released 2026-06-19). **1.4.x** ("Andium") is the LTS line, supported to ~Sept 2026; starting at 1.4, every other minor is LTS. See [`00_overview.md`](00_overview.md) for the release-line model.
- **Secrets manager since 0.10.0.** `CREATE SECRET` is the recommended auth path for all storage types; `SET s3_*` is legacy and leak-prone.
- **`TYPE r2` requires `ACCOUNT_ID`**; it derives the endpoint. The alternative `TYPE s3` form needs the endpoint spelled out **and** `URL_STYLE 'path'`.
- **`URL_STYLE 'path'` for non-AWS stores** (R2, MinIO). Leaving the `vhost` default is the most common connection failure.
- **HTTP(S) is read-only.** To write, use `s3://`/`r2://`/`gcs://`.
- **CSV over HTTP = full download.** Prefer Parquet for anything you query remotely with projections/filters.
- **R2 credentials = S3-API tokens**, not Cloudflare API tokens.
- **DuckDB R2 access ≠ Lance R2 access.** Two independent credential paths (§4.2 relevance note).

### Unverified / needs confirmation
- Full `http_*` retry/timeout/keep-alive setting names and defaults were not in the fetched HTTP(S) page. Verify against `duckdb_settings()` on your pinned build (query in §2).
- The fetched s3api page listed the S3 secret parameter table with `USE_SSL`/`VERIFY_SSL`/`URL_COMPATIBILITY_MODE` defaults as shown; confirm exact defaults for `URL_COMPATIBILITY_MODE` on your build if it is load-bearing, as this default has varied across versions.
