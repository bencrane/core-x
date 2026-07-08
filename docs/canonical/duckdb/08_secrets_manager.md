# Secrets Manager — CREATE SECRET, types (s3/r2/gcs/azure/http), persistence

> Canonical upstream reference — fetched 2026-07-08 from official DuckDB documentation and source repositories. Documents upstream behavior as-is; this is NOT core-x doctrine. Treat the version numbers and API signatures here as fetched-date ground truth.
>
> Primary sources:
> - https://duckdb.org/docs/stable/sql/statements/create_secret (redirects to /docs/current/sql/statements/create_secret.html) — `CREATE SECRET` / `DROP SECRET` statement page and the plaintext-history warning.
> - https://duckdb.org/docs/stable/configuration/secrets_manager (redirects to /docs/current/configuration/secrets_manager.html) — secret types table, providers, temporary vs persistent, storage location, scope resolution, `which_secret()`, `duckdb_secrets()`.
> - https://duckdb.org/docs/stable/core_extensions/httpfs/s3api (redirects to /docs/current/core_extensions/httpfs/s3api.html) — full S3/R2/GCS secret parameter table, `config` vs `credential_chain` providers, `CHAIN`, R2 and GCS secret examples.
> - https://raw.githubusercontent.com/duckdb/duckdb-web/main/js/current/statements/secrets.js — the machine-readable railroad grammar for `CREATE SECRET` / `DROP SECRET` (source of the exact clause order below).
> - https://duckdb.org/docs/current/core_extensions/httpfs/https.md — the `http` secret type (`BEARER_TOKEN`, `EXTRA_HTTP_HEADERS`, `HTTP_PROXY*`, `VERIFY_SSL`).
> - https://raw.githubusercontent.com/duckdb/duckdb-web/main/docs/current/sql/meta/duckdb_table_functions.md — column schema of `duckdb_secrets()`.
> - https://api.github.com/repos/duckdb/duckdb/releases/latest — current released version (v1.5.4, published 2026-06-17).

Scope: How DuckDB's Secrets Manager creates, scopes, persists, resolves, and drops credentials for object-storage and HTTP backends (s3/r2/gcs/azure/http and friends), with the exact `CREATE SECRET`/`DROP SECRET` grammar and the complete S3-family parameter reference.

---

## 0. Current version (as of 2026-07-08)

- **DuckDB current released version: `v1.5.4`** (Bugfix Release, published 2026-06-17), per the GitHub `releases/latest` API.
- The Secrets Manager and the `CONFIG`/`credential_chain` provider model have been the stable mechanism since DuckDB `0.10`; the `create_secret` and `secrets_manager` doc pages exist for `0.10`, `1.0`, `1.1`, `1.2`, `1.3`, `current`, and `lts`. The upstream docs `current` tree (the source of every signature below) reflects the `1.3+` line where `httpfs` moved from `docs/.../extensions/` to `docs/.../core_extensions/`.
- Secrets are the **preferred, non-deprecated** way to authenticate `httpfs`/S3. The older `SET s3_access_key_id = ...` / `load_aws_credentials(...)` mechanism is the **deprecated "legacy S3 API authentication"** — see [Footguns & deprecations](#8-footguns--deprecations).

> Relevance to core-x: this is the credential layer for the `s3://data-sink/active/` Cloudflare R2 system of record. A single R2 secret (temporary, `credential_chain`-fed from env, or persistent) authenticates DuckDB's reads/writes to R2 for the DuckDB → Arrow → Lance pipeline. Lance-on-R2 uses its own `storage_options` dict — see [§9](#9-crosslinks--relevance-to-core-x). The two credential planes are independent; a DuckDB secret does not configure Lance and vice versa.

---

## 1. What the Secrets Manager is

The Secrets Manager is a single, unified interface for credentials across every backend that needs them (S3, R2, GCS, Azure, HTTP, Hugging Face, Postgres, MySQL, Iceberg, DuckLake, …). Two properties matter:

1. **Scoping** — a secret can be bound to a URL-prefix scope, so different storage prefixes resolve to different secrets. This lets a single query join data across organizations/buckets.
2. **Persistence** — a secret can be persisted to disk so it survives across DuckDB instances and does not need to be re-declared each launch.

> **Warning (upstream, verbatim intent):** Persistent secrets are stored in **unencrypted binary format** on disk. See [§8](#8-footguns--deprecations).

---

## 2. `CREATE SECRET` — exact grammar

The statement page renders as a railroad diagram; the grammar below is transcribed verbatim from the upstream railroad source (`js/current/statements/secrets.js`). Clause order is load-bearing.

```
CREATE [ OR REPLACE ] [ PERSISTENT | TEMPORARY ] SECRET
    [ IF NOT EXISTS ]
    [ secret_name ]
    [ IN storage_specifier ]
    (
        TYPE secret_type
        [ , KEY_n VALUE_n ]...
    )
```

Element-by-element (from the railroad `Stack`/`Sequence` nodes):

| Element | Optional? | Meaning |
|:---|:---|:---|
| `CREATE` | required | Statement keyword. |
| `OR REPLACE` | optional | Replace an existing secret of the same name (see [§3](#3-create-or-replace-secret)). |
| `PERSISTENT` \| `TEMPORARY` | optional (`Choice`, default skip → **TEMPORARY**) | Persistence tier. Omitting both yields a **temporary** secret. |
| `SECRET` | required | Statement keyword. |
| `IF NOT EXISTS` | optional | No-op if a secret with that name already exists (mutually exclusive in practice with `OR REPLACE`). |
| `secret_name` | optional | Name of the secret. If omitted, an unnamed/default secret for the type is created. |
| `IN storage_specifier` | optional | Which storage backend to persist into (e.g. the default `local_file` backend). Only meaningful for persistent secrets. |
| `( TYPE secret_type, KEY_n VALUE_n, … )` | required | The parameter list. `TYPE` is mandatory and first; every other parameter is a `KEY value` pair, comma-separated. |

Key syntactic facts:

- **`TYPE` is mandatory and must be the first entry** inside the parentheses.
- Every other parameter is a bare `KEY 'value'` pair — **no `=` sign** inside `CREATE SECRET` (unlike `read_csv(..., key = value)`). Strings are single-quoted; booleans may be given as `true`/`false` or `1`/`0`.
- `PERSISTENT` / `TEMPORARY` sits **before** the `SECRET` keyword: `CREATE PERSISTENT SECRET …`, not `CREATE SECRET … PERSISTENT`.

### Minimal temporary secret (default `CONFIG` provider, verbatim from upstream)

```sql
CREATE SECRET my_secret (
    TYPE s3,
    KEY_ID 'my_secret_key',
    SECRET 'my_secret_value',
    REGION 'my_region'
);
```

The default (unnamed provider) is `CONFIG`.

---

## 3. `CREATE OR REPLACE SECRET`

`CREATE OR REPLACE SECRET` overwrites any existing secret with the same name (verbatim upstream form used throughout the S3 API page):

```sql
CREATE OR REPLACE SECRET secret (
    TYPE s3,
    PROVIDER config,
    KEY_ID 'AKIAIOSFODNN7EXAMPLE',
    SECRET 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
    REGION 'us-east-1'
);
```

Use `OR REPLACE` for idempotent pipeline bootstrapping (re-running the setup does not error on "secret already exists"). Use `IF NOT EXISTS` when you want the *first* declaration to win and later ones to be no-ops.

---

## 4. Temporary vs persistent, and where persistent secrets live

| | Temporary (default) | Persistent |
|:---|:---|:---|
| Keyword | none, or `TEMPORARY` | `PERSISTENT` |
| Lifetime | In-memory, for the life span of the DuckDB instance | Survives restarts; auto-loaded on startup |
| Storage | RAM only | **Unencrypted binary** files under `~/.duckdb/stored_secrets` |
| Read on startup | n/a | Yes — persistent secrets are read from the directory and automatically loaded |

Persistent secret (verbatim upstream):

```sql
CREATE PERSISTENT SECRET my_persistent_secret (
    TYPE s3,
    KEY_ID 'my_secret_key',
    SECRET 'my_secret_value'
);
```

Change where persistent secrets are written/read:

```sql
SET secret_directory = 'path/to/my_secrets_dir';
```

> Upstream note (verbatim intent): setting the `home_directory` configuration option has **no** effect on the location of the secrets — only `secret_directory` moves them. Default remains `~/.duckdb/stored_secrets`.

---

## 5. Secret types and providers

### 5.1 Secret types (verbatim from the upstream table)

Secrets are typed; the type identifies the service. Most types are **not** built in — they are registered by an extension that must be installed/loaded first.

| Secret type | Service / protocol | Extension |
|:---|:---|:---|
| `azure` | Azure Blob Storage | `azure` |
| `ducklake` | DuckLake | `ducklake` |
| `gcs` | Google Cloud Storage | `httpfs` |
| `http` | HTTP and HTTPS | `httpfs` |
| `huggingface` | Hugging Face | `httpfs` |
| `iceberg` | Iceberg REST Catalog | `httpfs`, `iceberg` |
| `mysql` | MySQL | `mysql` |
| `postgres` | PostgreSQL | `postgres` |
| `quack` | Quack | `quack` |
| `r2` | Cloudflare R2 | `httpfs` |
| `s3` | AWS S3 | `httpfs` |

For `s3`/`r2`/`gcs`/`azure`, load `httpfs` (or `azure`) before creating the secret: `INSTALL httpfs; LOAD httpfs;`. See [09_extensions_system.md](09_extensions_system.md) and [10_core_extensions_catalog.md](10_core_extensions_catalog.md).

### 5.2 Providers: `CONFIG` vs `credential_chain`

For the `s3`, `gcs`, `r2`, and `azure` types, two providers exist:

- **`CONFIG`** (default when no `PROVIDER` is given): the user supplies **all** configuration (`KEY_ID`, `SECRET`, `REGION`, …) explicitly in the statement.
- **`credential_chain`**: DuckDB **automatically fetches** credentials via the AWS SDK's provider chain (env vars, config/credential files, SSO, EC2/ECS instance metadata, STS web identity, process credentials).

`credential_chain`, minimal (verbatim):

```sql
CREATE OR REPLACE SECRET secret (
    TYPE s3,
    PROVIDER credential_chain
);
```

`credential_chain` with an explicit ordered chain via `CHAIN` (semicolon-separated `a;b;c`, tried in order):

```sql
CREATE OR REPLACE SECRET secret (
    TYPE s3,
    PROVIDER credential_chain,
    CHAIN 'env;config'
);
```

Accepted `CHAIN` values (verbatim upstream list): `config`, `sts`, `sso`, `env`, `instance`, `process`.

`credential_chain` can also **override** fetched config — e.g. auto-load then force the region:

```sql
CREATE OR REPLACE SECRET secret (
    TYPE s3,
    PROVIDER credential_chain,
    CHAIN config,
    REGION 'eu-west-1'
);
```

Load a specific named AWS profile (equivalent to the deprecated `load_aws_credentials('my_profile')`):

```sql
CREATE OR REPLACE SECRET secret (
    TYPE s3,
    PROVIDER credential_chain,
    CHAIN config,
    PROFILE 'my_profile'
);
```

---

## 6. S3-family secret parameter reference (s3 / r2 / gcs)

Complete supported-parameter table for **both** the `config` and `credential_chain` providers, transcribed verbatim from the S3 API page. The **Secret** column lists which secret types accept the parameter.

| Name | Description | Applies to | Type | Default |
|:---|:---|:---|:---|:---|
| `ENDPOINT` | Custom S3 endpoint | `S3`, `GCS`, `R2` | `STRING` | `s3.amazonaws.com` for `S3` (GCS/R2 auto-derive theirs) |
| `KEY_ID` | The ID of the key to use (access key) | `S3`, `GCS`, `R2` | `STRING` | — |
| `REGION` | Region to authenticate against (should match the bucket's region) | `S3`, `GCS`, `R2` | `STRING` | `us-east-1` |
| `SECRET` | The secret of the key to use | `S3`, `GCS`, `R2` | `STRING` | — |
| `SESSION_TOKEN` | Session token for temporary credentials | `S3`, `GCS`, `R2` | `STRING` | — |
| `URL_COMPATIBILITY_MODE` | Helps when URLs contain problematic characters | `S3`, `GCS`, `R2` | `BOOLEAN` | `true` |
| `URL_STYLE` | `vhost` or `path` | `S3`, `GCS`, `R2` | `STRING` | `vhost` for `S3`; `path` for `R2` and `GCS` |
| `USE_SSL` | Use HTTPS (`true`) or HTTP (`false`) | `S3`, `GCS`, `R2` | `BOOLEAN` | `true` |
| `VERIFY_SSL` | Verify the server's SSL certificate | `S3`, `GCS`, `R2` | `BOOLEAN` | `true` |
| `ACCOUNT_ID` | R2 account ID, used to generate the endpoint URL | `R2` | `STRING` | — |
| `KMS_KEY_ID` | AWS KMS key for server-side encryption | `S3` | `STRING` | — |
| `REQUESTER_PAYS` | Enable "requester pays" buckets | `S3` | `BOOLEAN` | `false` |

Provider-only parameters (not in the value table above, but part of the S3-family secret surface):

| Name | Description | Applies to | Type | Notes |
|:---|:---|:---|:---|:---|
| `PROVIDER` | `config` (default) or `credential_chain` | `S3`, `GCS`, `R2`, `AZURE` | `STRING` | Selects credential source |
| `CHAIN` | Ordered `;`-separated provider list for `credential_chain` | `S3`, `GCS`, `R2` | `STRING` | Values: `config`, `sts`, `sso`, `env`, `instance`, `process` |
| `PROFILE` | Named AWS profile (with `CHAIN config`) | `S3`, `GCS`, `R2` | `STRING` | Equivalent to legacy `load_aws_credentials(profile)` |
| `SCOPE` | URL prefix(es) the secret binds to | all types | `STRING` or `STRING[]` | See [§7](#7-scope--which-secret-is-chosen) |

### 6.1 `http` secret parameters

From the HTTPS extension page (verbatim examples):

| Name | Description | Type |
|:---|:---|:---|
| `BEARER_TOKEN` | Bearer token; sent as `Authorization: Bearer <token>` | `STRING` |
| `EXTRA_HTTP_HEADERS` | Arbitrary extra headers | `MAP(VARCHAR, VARCHAR)` |
| `HTTP_PROXY` | Proxy URL | `STRING` |
| `HTTP_PROXY_USERNAME` | Proxy auth username | `STRING` |
| `HTTP_PROXY_PASSWORD` | Proxy auth password | `STRING` |
| `VERIFY_SSL` | Verify SSL cert (`1`/`0`) | `BOOLEAN` |
| `SCOPE` | URL prefix(es) the secret applies to | `STRING` or `STRING[]` |

```sql
-- Bearer token auth for an HTTP endpoint
CREATE SECRET http_auth (
    TYPE http,
    BEARER_TOKEN 'token'
);

-- Or explicit headers
CREATE SECRET http_auth (
    TYPE http,
    EXTRA_HTTP_HEADERS MAP {
        'Authorization': 'Bearer token'
    }
);
```

> `azure`, `huggingface`, `postgres`, `mysql`, `iceberg`, `ducklake` each have their own parameter sets on their respective extension pages, which were not exhaustively fetched here. Their `TYPE`/`SCOPE`/`PROVIDER` surface follows the same grammar. See the per-extension pages linked from the type table in [§5.1](#51-secret-types-verbatim-from-the-upstream-table). Flagged under [Unverified](#10-unverified--needs-confirmation).

---

## 7. `SCOPE` — which secret is chosen

A secret's **scope** is a URL/path **prefix** it applies to. When DuckDB resolves a secret for a path, it compares the path against the scopes of all secrets of that type. **On multiple matches, the longest matching prefix wins.**

Two S3 secrets, scoped to different buckets (verbatim upstream):

```sql
CREATE SECRET secret1 (
    TYPE s3,
    KEY_ID 'my_secret_key1',
    SECRET 'my_secret_value1',
    SCOPE 's3://my-bucket'
);
```

```sql
CREATE SECRET secret2 (
    TYPE s3,
    KEY_ID 'my_secret_key2',
    SECRET 'my_secret_value2',
    SCOPE 's3://my-other-bucket'
);
```

Querying `s3://my-other-bucket/something` automatically selects `secret2`.

`SCOPE` also accepts a **list** of prefixes (used e.g. for HTTP proxy scoping):

```sql
CREATE SECRET http_proxy (
    TYPE HTTP,
    SCOPE ['https://duckdb.org', 'https://some-other-website.org'],
    HTTP_PROXY 'http_proxy_url'
);
```

### 7.1 `which_secret(path, type)` — inspect the resolution

Scalar function that returns which secret would be chosen for a given path + secret type (verbatim):

```sql
FROM which_secret('s3://my-other-bucket/file.parquet', 's3');
```

Signature (from usage; the docs describe it as "takes a path and a secret type as parameters"):

```
which_secret(path VARCHAR, type VARCHAR) -> VARCHAR   -- name of the matching secret
```

> The exact return-shape/nullability of `which_secret` (single scalar vs row) is described only by example upstream; treated as `VARCHAR` returning the selected secret's name. Flagged under [Unverified](#10-unverified--needs-confirmation).

### 7.2 `duckdb_secrets()` — list all secrets

Table function listing every secret in the instance; sensitive fields are redacted (verbatim):

```sql
FROM duckdb_secrets();
```

Column schema (verbatim from the meta table functions page):

| Column | Description | Type |
|:---|:---|:---|
| `name` | Name of the secret | `VARCHAR` |
| `type` | Type, e.g. `S3`, `GCS`, `R2`, `AZURE` | `VARCHAR` |
| `provider` | Provider of the secret | `VARCHAR` |
| `persistent` | Whether the secret is persistent | `BOOLEAN` |
| `storage` | Backend storing the secret | `VARCHAR` |
| `scope` | Scope of the secret | `VARCHAR[]` |
| `secret_string` | Secret content as a string; sensitive pieces (e.g. access key) are **redacted** | `VARCHAR` |

---

## 8. Footguns & deprecations

- **CLI history leaks plaintext.** Upstream warning, verbatim: "When using the command line client, the `CREATE SECRET` statements are stored in your DuckDB history as plain text." Anything typed interactively lands in `~/.duckdb/history` (or the CLI's history file) unredacted.
- **Persistent secrets are unencrypted on disk.** `~/.duckdb/stored_secrets` holds secrets in **unencrypted binary format**. Anyone with filesystem read access to that directory can recover the credentials. Prefer `credential_chain` (env / instance role) over persisting long-lived keys.
- **Prefer `credential_chain` or environment over inline keys.** For pipelines, feed credentials via the AWS SDK chain (`env`, `instance`, `sso`, `process`) rather than baking `KEY_ID`/`SECRET` into scripts or persistent secrets.
- **No `=` in the parameter list.** `CREATE SECRET` uses `KEY 'value'`, not `KEY = 'value'`. Using `=` is a syntax error.
- **`PERSISTENT`/`TEMPORARY` placement.** It precedes `SECRET` (`CREATE PERSISTENT SECRET`), not the parameter list.
- **`home_directory` does not move secrets.** Only `SET secret_directory` relocates persistent secrets.
- **Legacy S3 auth is deprecated.** The old `SET s3_access_key_id`/`SET s3_secret_access_key`/`SET s3_region` PRAGMAs and `load_aws_credentials(profile)` are the **deprecated** "legacy S3 API authentication" path. Migrate to a defined secret; a profile secret (`PROVIDER credential_chain, CHAIN config, PROFILE '<name>'`) is the drop-in replacement for `load_aws_credentials('<name>')`.
- **R2/GCS `credential_chain` reads AWS env vars.** Because DuckDB uses an AWS client internally, `credential_chain` for `r2`/`gcs` secrets looks for **AWS** credential locations. Your R2/GCS HMAC keys must be exported as `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` for the chain to pick them up.
- **R2/GCS URL prefixes are gated.** `r2` secrets only activate for `r2://` URLs; `gcs` secrets only for `gcs://` or `gs://` URLs. Reading the same bucket via an `s3://` URL will not use the `r2`/`gcs` secret.
- **GCS needs HMAC keys, not service-account JSON.** `KEY_ID`/`SECRET` for a `gcs` secret must be **HMAC interoperability keys**, not a GCP service-account key or OAuth token.

---

## 9. Worked examples for object storage

### 9.1 An R2 secret (Cloudflare, verbatim upstream)

```sql
INSTALL httpfs;
LOAD httpfs;

CREATE OR REPLACE SECRET secret (
    TYPE r2,
    KEY_ID 'AKIAIOSFODNN7EXAMPLE',
    SECRET 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
    ACCOUNT_ID 'my_account_id'
);

-- R2 secrets activate only for r2:// URLs:
SELECT *
FROM read_parquet('r2://some-file-that-uses-an-r2-secret.parquet');
```

`ACCOUNT_ID` is what lets DuckDB build the correct R2 endpoint (`<account_id>.r2.cloudflarestorage.com`) automatically — you do not set `ENDPOINT` yourself for the `r2` type.

### 9.2 An S3-typed secret pointed at R2 (S3-against-R2)

Cloudflare R2 speaks the plain S3 API, so you can authenticate against it with a **`TYPE s3`** secret by setting `ENDPOINT` and `URL_STYLE 'path'` explicitly instead of using the `r2` convenience type. This is the pattern to use when downstream code addresses the bucket with `s3://` URLs (e.g. tooling that only understands the S3 scheme).

```sql
INSTALL httpfs;
LOAD httpfs;

CREATE OR REPLACE SECRET r2_via_s3 (
    TYPE s3,
    PROVIDER config,
    KEY_ID 'AKIAIOSFODNN7EXAMPLE',
    SECRET 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
    ENDPOINT '<account_id>.r2.cloudflarestorage.com',
    URL_STYLE 'path',
    REGION 'auto',
    SCOPE 's3://data-sink'
);

SELECT *
FROM read_parquet('s3://data-sink/active/some_dataset/*.parquet');
```

Notes on the S3-against-R2 form:
- `ENDPOINT` must be the R2 S3 endpoint `<account_id>.r2.cloudflarestorage.com` (no scheme; `USE_SSL` defaults `true`).
- `URL_STYLE 'path'` is required — R2 does not support the default `vhost` style that the `s3` type otherwise assumes.
- `REGION 'auto'` is the conventional value for R2 (R2 is region-less; the S3 client still needs a region string). The `ENDPOINT`/`URL_STYLE` values here are the standard R2 S3 configuration; the `s3`-type secret parameters themselves (`ENDPOINT`, `URL_STYLE`, `KEY_ID`, `SECRET`) are all verbatim from the upstream parameter table, but the specific R2 endpoint hostname pattern is R2-provider knowledge, not a DuckDB doc string — flagged under [Unverified](#10-unverified--needs-confirmation).

### 9.3 Drop a secret

```
DROP [ PERSISTENT | TEMPORARY ] SECRET [ IF EXISTS ] secret_name [ FROM storage_specifier ]
```

Grammar transcribed verbatim from the railroad source. Example (verbatim upstream):

```sql
DROP PERSISTENT SECRET my_persistent_secret;
```

- `IF EXISTS` suppresses the error when the secret is absent.
- `FROM storage_specifier` targets a specific storage backend when dropping a persistent secret from a non-default location.

---

## 10. Crosslinks & Relevance to core-x

Sibling canonical files:

- [00_overview.md](00_overview.md) — editions, clients, versioning/release lines (v1.5.4 context).
- [01_python_client.md](01_python_client.md) — running `CREATE SECRET` via `con.execute(...)` from Python.
- [07_httpfs_s3_r2.md](07_httpfs_s3_r2.md) — the `httpfs` extension, reading/writing S3 & R2 objects (the consumer of these secrets).
- [09_extensions_system.md](09_extensions_system.md) — `INSTALL`/`LOAD httpfs` (prerequisite for s3/r2/gcs secrets).
- [10_core_extensions_catalog.md](10_core_extensions_catalog.md) — which extension registers which secret type.
- [13_lance_interop.md](13_lance_interop.md) — DuckDB ↔ Lance handoff on R2.

> Relevance to core-x: For the `s3://data-sink/active/` R2 system of record, the operational choice is (a) a `TYPE r2` secret with `ACCOUNT_ID` for `r2://` URLs, or (b) a `TYPE s3` secret with `ENDPOINT <account_id>.r2.cloudflarestorage.com` + `URL_STYLE 'path'` for `s3://` URLs — pick one scheme and address the bucket consistently. Feed credentials via `credential_chain` from `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` env vars rather than persisting plaintext keys under `~/.duckdb/stored_secrets`, since that store is unencrypted. **This secret authenticates DuckDB only.** The Lance writer (`lance.write_dataset(...)` / LanceDB) authenticates R2 through its own `storage_options` dict (`aws_access_key_id`, `aws_secret_access_key`, `aws_endpoint`/`endpoint_url`, `region`) — a DuckDB secret has no effect on Lance and must be configured separately. The zero-copy Arrow handoff between DuckDB and Lance crosses no credential boundary; each side holds its own R2 credentials.

---

## 11. Unverified / needs confirmation

- **`which_secret()` exact return shape** — documented only by example (`FROM which_secret(path, type)`); modeled here as `which_secret(path VARCHAR, type VARCHAR) -> VARCHAR` returning the secret name. The precise signature/nullability was not found in a dedicated function-reference page.
- **`IN` / `FROM storage_specifier` accepted values** — the grammar exposes an `IN storage_specifier` (create) and `FROM storage_specifier` (drop) clause for selecting the storage backend of a persistent secret, but the enumerated backend names (beyond the implicit default local-file store) were not in the fetched pages.
- **Per-type parameter sets for `azure`, `huggingface`, `postgres`, `mysql`, `iceberg`, `ducklake`, `quack`** — only the `s3`/`r2`/`gcs`/`http` parameter surfaces were fetched exhaustively. The others follow the same `TYPE`/`PROVIDER`/`SCOPE` grammar; consult their extension pages for the full key list.
- **The R2 S3-endpoint hostname pattern** `<account_id>.r2.cloudflarestorage.com` and `REGION 'auto'` in [§9.2](#92-an-s3-typed-secret-pointed-at-r2-s3-against-r2) are Cloudflare R2 provider conventions, not literal strings from the DuckDB docs. The DuckDB-side parameters used (`ENDPOINT`, `URL_STYLE`, `KEY_ID`, `SECRET`, `REGION`) are all verbatim from the upstream S3 parameter table.
- **`CHAIN` string vs bareword** — upstream shows both `CHAIN 'env;config'` (quoted) and `CHAIN config` (bareword single provider). Both forms appear verbatim in the docs; treat quoted-semicolon-list as canonical for multi-provider chains.
