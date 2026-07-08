# Object Store Configuration — storage_options for S3 / Cloudflare R2 / GCS / Azure

> Canonical upstream reference — fetched 2026-07-08 from official Lance documentation and source repositories. Documents upstream behavior as-is; this is NOT core-x doctrine. Treat the version numbers and API signatures here as fetched-date ground truth.
>
> Primary sources:
> - https://lance.org/guide/object_store/ — the authoritative Object Store Configuration guide (general options, S3, GCS, Azure key tables verbatim).
> - https://raw.githubusercontent.com/lancedb/lance/main/docs/src/guide/object_store.md — repo source of that same guide (key tables quoted verbatim below).
> - https://raw.githubusercontent.com/lancedb/lance/main/python/python/lance/dataset.py — `lance.write_dataset` signature + `storage_options` / `commit_lock` docstrings.
> - https://raw.githubusercontent.com/lancedb/lance/main/python/python/lance/__init__.py — `lance.dataset` signature.
> - https://docs.lancedb.com/storage/configuration — LanceDB storage config (R2/Tigris `region: "auto"`, credential resolution, GCS HTTP/2 note).
> - https://lance.org/format/table/transaction/ — commit protocol: put-if-not-exists / rename-if-not-exists requirements, external manifest store, conflict resolution.
> - https://pypi.org/project/pylance/ — current released version.

Scope: exactly which keys go in the `storage_options` dict passed to `lance.dataset()` / `lance.write_dataset()`, how they map onto S3 / S3-compatible / Cloudflare R2 / GCS / Azure, how credentials resolve, and how Lance commits safely on object stores — with the canonical Cloudflare R2 recipe.

---

## Versions (as of 2026-07-08)

| Package | Current release | Notes |
|---|---|---|
| `pylance` (PyPI, the Python SDK — `import lance`) | **8.0.0**, released 2026-07-01 | Requires Python `>=3.9` (3.9–3.14). The object-store layer is `object_store` (Rust) under the hood. |
| `lancedb` (the higher-level DB / table API) | Separate package; see `11_lancedb_table_api.md`. LanceDB reuses the *same* `storage_options` keys documented here. |

> The PyPI package is named **`pylance`** but is imported as **`lance`**. Do not confuse it with the Microsoft "Pylance" VS Code extension — unrelated.

`storage_options` is a flat `Dict[str, str]` — **every value is a string**, including booleans (`"true"`/`"false"`) and durations (`"60s"`).

---

## Entry points that accept `storage_options`

Every dataset-opening / writing call takes the same `storage_options` dict. Two module-level functions are the ones you use in a DuckDB→Arrow→Lance pipeline.

### `lance.dataset(...)` — open an existing dataset

Verbatim from `python/python/lance/__init__.py`:

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

### `lance.write_dataset(...)` — create / append / overwrite

Verbatim from `python/python/lance/dataset.py`:

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

Docstring for the two relevant parameters, verbatim:

> `commit_lock : CommitLock, optional` — "A custom commit lock. Only needed if your object store does not support atomic commits. See the user guide for more details."
>
> `storage_options : optional, dict` — "Extra options that make sense for a particular storage connection. This is used to store connection parameters like credentials, endpoint, etc."

The URI **scheme selects the store**: `s3://` → S3 (and S3-compatible, incl. R2), `gs://` → GCS, `az://` → Azure, a bare path → local filesystem. (Source: object_store guide, opening paragraph.)

Other methods that take `storage_options` with the identical key set: `LanceDataset.__init__`, `lance.LanceDataset.merge_insert(...).execute(...)` writers, `LanceFragment.create(...)`, `lance.LanceOperation` commit paths — all forward the same dict down to the Rust `object_store` layer.

---

## How to pass options: env vars vs. `storage_options`

There are two equivalent mechanisms (source: object_store guide):

1. **Environment variables** — global, uppercase. E.g. `export TIMEOUT=60s`, `export AWS_ACCESS_KEY_ID=...`, `export AWS_ENDPOINT=...`.
2. **The `storage_options` dict** — per-dataset, lowercase keys. Wins over nothing implicitly, but is scoped to the one call.

Most keys below list both spellings as `aws_region` / `region` — the `aws_`-prefixed form is the env-var-style name; the short form is the dict-style alias. **Both are accepted in the dict.** Values are always strings.

> AWS SSO is the exception: "If you are using AWS SSO, you can specify the `AWS_PROFILE` environment variable. It cannot be specified in the `storage_options` parameter." (object_store guide, S3 section.)

---

## General options (apply to ALL object stores)

Verbatim from the object_store guide's general table:

| Key | Description |
|---|---|
| `allow_http` | Allow non-TLS, i.e. non-HTTPS connections. Default `False`. |
| `allow_invalid_certificates` | Skip certificate validation on HTTPS connections. Default `False`. |
| `connect_timeout` | Timeout for only the connect phase of a client. Default `5s`. |
| `request_timeout` / `timeout` | Timeout for the entire request, from connection until the response body has finished. Default `30s`. (Env var: `TIMEOUT`.) |
| `user_agent` | User agent string to use in requests. |
| `proxy_url` | URL of a proxy server to use for requests. Default `None`. |
| `proxy_ca_certificate` | PEM-formatted CA certificate for proxy connections. |
| `proxy_excludes` | List of hosts that bypass the proxy — comma-separated list of domains and IP masks. Any subdomain of a provided domain is bypassed. E.g. `example.com, 192.168.1.0/24`. |
| `download_retry_count` | Number of times to retry a download. Default `3`. |
| `client_max_retries` | Number of times for the object store client to retry the request. Default `3`. |
| `client_retry_timeout` | Timeout for the object store client to retry the request, in seconds. Default `180`. |

Durations are strings like `"5s"`, `"30s"`, `"60s"`. Booleans are strings `"true"` / `"false"`.

> **Footgun:** `allow_http` defaults to `False`. If you point `endpoint` at an `http://` URL (e.g. local MinIO) you MUST set `"allow_http": "true"`, or the connection is rejected. For an `https://` endpoint (R2, real S3) leave `allow_http` unset/false.

---

## S3 & S3-compatible configuration

Credentials can be supplied through any of the following (source: object_store guide S3 section). The upstream guide presents the `storage_options` dict and the environment variables as equivalent alternatives rather than a strict precedence chain; when neither is set, the underlying `object_store` layer falls back to the AWS default credential chain (profile / instance metadata):

- Explicit keys in `storage_options` (`access_key_id`, `secret_access_key`, `session_token`).
- Environment variables `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`.
- `AWS_PROFILE` (env var only) for AWS SSO.
- Instance/container metadata (IAM role) when running inside AWS — no explicit config needed.

Full S3 key table, verbatim from the object_store guide:

| Key | Description |
|---|---|
| `aws_region` / `region` | The AWS region the bucket is in. This can be automatically detected when using AWS S3, but **must be specified for S3-compatible stores**. |
| `aws_access_key_id` / `access_key_id` | The AWS access key ID to use. |
| `aws_secret_access_key` / `secret_access_key` | The AWS secret access key to use. |
| `aws_session_token` / `session_token` | The AWS session token to use. |
| `aws_endpoint` / `endpoint` | The endpoint to use for S3-compatible stores. |
| `aws_virtual_hosted_style_request` / `virtual_hosted_style_request` | Whether to use virtual hosted-style requests, where bucket name is part of the endpoint. Meant to be used with `aws_endpoint`. Default `False`. |
| `aws_s3_express` / `s3_express` | Whether to use S3 Express One Zone endpoints. Default `False`. |
| `aws_server_side_encryption` | The server-side encryption algorithm to use. Must be one of `"AES256"`, `"aws:kms"`, or `"aws:kms:dsse"`. Default `None`. |
| `aws_sse_kms_key_id` | The KMS key ID to use for server-side encryption. If set, `aws_server_side_encryption` must be `"aws:kms"` or `"aws:kms:dsse"`. |
| `aws_sse_bucket_key_enabled` | Whether to use bucket keys for server-side encryption. |

### Real AWS S3 — minimal

```python
import lance

# Credentials from env / IAM role; region auto-detected on real S3.
ds = lance.dataset("s3://my-bucket/my-dataset.lance")
```

### Explicit credentials

```python
ds = lance.dataset(
    "s3://bucket/path",
    storage_options={
        "access_key_id": "my-access-key",
        "secret_access_key": "my-secret-key",
        "session_token": "my-session-token",  # optional
    },
)
```

### S3-compatible (MinIO etc.) — `region` AND `endpoint` are mandatory

```python
ds = lance.dataset(
    "s3://bucket/path",
    storage_options={
        "region": "us-east-1",
        "endpoint": "http://minio:9000",
        "allow_http": "true",          # required for an http:// endpoint
    },
)
```

Equivalent env vars: `AWS_ENDPOINT` and `AWS_DEFAULT_REGION`.

### S3 Express One Zone (directory buckets)

Lance auto-recognizes the `--x-s3` suffix; no special config needed for the common case. Only set `s3_express` explicitly when an access point / private link hides the bucket name. S3 Express buckets only connect from an EC2 instance in the same region.

```python
ds = lance.dataset(
    "s3://my-bucket--use1-az4--x-s3/path/imagenet.lance",
    storage_options={"region": "us-east-1", "s3_express": "true"},
)
```

---

## Cloudflare R2 (S3-compatible)

R2 exposes an S3-compatible API, so it uses the **`s3://` scheme** and the **S3 key set** above. The canonical configuration:

- **Scheme:** `s3://<bucket>/<path>` (NOT `r2://` — no such scheme).
- **`endpoint`:** `https://<ACCOUNT_ID>.r2.cloudflarestorage.com` (account-level S3 endpoint).
- **`region`:** `"auto"`. Per Cloudflare's S3 API: "the region for an R2 bucket is `auto`. For compatibility with tools that do not allow you to specify a region, an empty value and `us-east-1` will alias to the `auto` region." Set `"auto"` explicitly to be safe.
- **`allow_http`:** **do NOT set it** — the R2 endpoint is HTTPS, so leave it at the `False` default. Setting `allow_http` is only for plaintext `http://` endpoints.
- **Credentials:** an R2 API token → an S3 access-key-id / secret-access-key pair, passed as `access_key_id` / `secret_access_key`. R2 does not use session tokens.
- **Virtual-hosted vs path-style:** the default (path-style, `virtual_hosted_style_request` unset/`False`) works against the `<account>.r2.cloudflarestorage.com` endpoint. Leave it default unless you deliberately front R2 with a bucket-hostname endpoint.

### Canonical R2 recipe

```python
import lance

R2 = {
    "endpoint":          "https://<ACCOUNT_ID>.r2.cloudflarestorage.com",
    "region":            "auto",
    "access_key_id":     "<R2_ACCESS_KEY_ID>",
    "secret_access_key": "<R2_SECRET_ACCESS_KEY>",
    # allow_http NOT set — endpoint is https
    # optional hardening for large writes:
    "request_timeout":   "300s",
    "connect_timeout":   "10s",
    "client_max_retries": "5",
}

# Open
ds = lance.dataset("s3://my-r2-bucket/datasets/foo.lance", storage_options=R2)

# Write / append
import pyarrow as pa
tbl = pa.table({"id": [1, 2, 3]})
lance.write_dataset(
    tbl,
    "s3://my-r2-bucket/datasets/foo.lance",
    mode="append",
    storage_options=R2,
)
```

> **Relevance to core-x:** This is THE canonical R2 `storage_options` recipe for the Gen-3 system of record under `s3://data-sink/active/`. Scheme is `s3://`, `endpoint` is the account R2 S3 URL, `region` is `"auto"`, `allow_http` stays unset because R2 is HTTPS. Every dataset open (`lance.dataset`) and every DuckDB→Arrow→`lance.write_dataset` append reuses one shared `storage_options` dict. Keep credentials out of source — resolve them from the environment (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` still work, since R2 speaks S3) or a secrets manager, and inject into the dict at runtime.

---

## Google Cloud Storage (GCS)

Scheme: `gs://bucket/path`. Credentials via the `GOOGLE_SERVICE_ACCOUNT` env var (path to a JSON key file) or the dict:

| Key | Description |
|---|---|
| `google_service_account` / `service_account` | Path to the service account JSON file. |
| `google_service_account_key` / `service_account_key` | The serialized service account key (inline JSON string). |
| `google_application_credentials` / `application_credentials` | Path to the application credentials. |

```python
ds = lance.dataset(
    "gs://my-bucket/my-dataset",
    storage_options={"service_account": "path/to/service-account.json"},
)
```

> **GCS footgun:** GCS defaults to HTTP/1. Set `HTTP1_ONLY=false` (env var) to enable HTTP/2 if you need it. (Source: LanceDB storage config.)

---

## Azure Blob Storage

Scheme: `az://container/path`. Account-key auth via `AZURE_STORAGE_ACCOUNT_NAME` / `AZURE_STORAGE_ACCOUNT_KEY` env vars, or the dict. Full key table, verbatim from the object_store guide:

| Key | Description |
|---|---|
| `azure_storage_account_name` / `account_name` | The name of the azure storage account. |
| `azure_storage_account_key` / `account_key` | The serialized service account key. |
| `azure_client_id` / `client_id` | Service principal client id for authorizing requests. |
| `azure_client_secret` / `client_secret` | Service principal client secret for authorizing requests. |
| `azure_tenant_id` / `tenant_id` | Tenant id used in oauth flows. |
| `azure_storage_sas_key` / `azure_storage_sas_token` / `sas_key` / `sas_token` | Shared access signature. Should be percent-encoded. |
| `azure_storage_token` / `bearer_token` / `token` | Bearer token. |
| `azure_storage_use_emulator` / `object_store_use_emulator` / `use_emulator` | Use object store with azurite storage emulator. |
| `azure_endpoint` / `endpoint` | Override the endpoint used to communicate with blob storage. |
| `azure_use_fabric_endpoint` / `use_fabric_endpoint` | Use object store with url scheme `account.dfs.fabric.microsoft.com`. |
| `azure_msi_endpoint` / `azure_identity_endpoint` / `identity_endpoint` / `msi_endpoint` | Endpoint to request an imds managed identity token. |
| `azure_object_id` / `object_id` | Object id for use with managed identity authentication. |
| `azure_msi_resource_id` / `msi_resource_id` | Msi resource id for use with managed identity authentication. |
| `azure_federated_token_file` / `federated_token_file` | File containing token for Azure AD workload identity federation. |
| `azure_use_azure_cli` / `use_azure_cli` | Use azure cli for acquiring access token. |
| `azure_disable_tagging` / `disable_tagging` | Disables tagging objects. Use this if your backing store does not support tags. |

```python
ds = lance.dataset(
    "az://my-container/my-dataset",
    storage_options={"account_name": "some-account", "account_key": "some-key"},
)
```

---

## Per-base configuration (multi-bucket datasets)

A dataset can register additional base paths (bases) that store part of its data, each potentially in a different bucket/account/provider. From the object_store guide:

> "A storage option key of the form `base_<id>.<key>` applies `<key>` only to the base path with that manifest id. Every base inherits the unscoped options; base-scoped entries add to or override them for that base only."

```python
ds = lance.dataset(
    "az://account-a/path",
    storage_options={
        "account_name": "account-a",   # shared default, inherited by bases
        "account_key": "key-a",
        "base_1.account_name": "account-b",  # override for base id 1
        "base_1.account_key": "key-b",
    },
)
```

Base ids for `initial_bases` are assigned sequentially starting at 1, in order. Keys not matching `base_<id>.<key>` exactly (e.g. `base_url`) are treated as regular options. The exact per-base map `base_store_params` (keyed by base path URI) takes precedence over `base_<id>.<key>` for that base.

---

## Commit handling & concurrency on object stores

### The atomic-primitive requirement

Lance's manifest commit protocol (source: transaction spec) requires the store to support one of two atomic operations, so that "exactly one writer succeeds when multiple writers attempt to create the same manifest file concurrently":

- **put-if-not-exists** — atomically write a file only if it does not already exist.
- **rename-if-not-exists** — atomically rename a file only if the target does not exist.

Modern **AWS S3 and S3 Express support atomic conditional writes natively** (`If-None-Match: *` / put-if-absent). Lance uses these directly — "S3 and S3 Express now support atomic writes natively, so LanceDB handles concurrent writers against the same table out-of-the-box — no external commit coordinator is required." The `ConditionalPutCommitHandler` reduced a manifest commit from 3 IOPS (put, copy-if-not-exists, delete) to 1.

### Cloudflare R2 and the single-writer guarantee

R2 supports S3 conditional writes (`If-None-Match`), so Lance's native put-if-absent commit path applies. Regardless: **a single writer per dataset is always safe** — with only one process committing, there is no race for the next manifest version, so no external coordinator or lock is ever needed. Serialize your writers (one appender per dataset at a time) and you never touch commit locks.

### External manifest store (for stores lacking atomic put-if-absent)

When the backing store does NOT support atomic put/rename-if-not-exists, an **external manifest store** — a key-value store that supports put-if-not-exists — coordinates concurrent writers. From the transaction spec, the four-step protocol:

1. Stage the manifest with a UUID suffix in object storage.
2. Commit the staged path to the external store using put-if-not-exists.
3. Finalize by copying the staged manifest to the standard path.
4. Update the external-store pointer to the finalized location.

> "The external manifest store supplements but does not replace the manifests in object storage. A reader unaware of the external manifest store can still read the table, but may observe a version up to one commit behind the true latest version." Readers detect an un-finalized commit and attempt to complete the synchronization; if that fails, the reader refuses to load, to guarantee portability.

### DynamoDB commit store (`s3+ddb://`)

The canonical external manifest store for S3 is **DynamoDB**, addressed via a dedicated URI scheme:

```
s3+ddb://<bucket>/<path>?ddbTableName=<table>
```

The `ddbTableName` query parameter names the DynamoDB table used as the put-if-not-exists coordinator. This was the standard way to get safe concurrent writers on S3 **before** S3 shipped native conditional puts; with native atomic writes now available, plain `s3://` handles concurrency and `s3+ddb://` is only needed for legacy setups or S3-compatible stores that lack conditional writes.

> **Unverified / needs confirmation:** the current `docs/src/guide/object_store.md` no longer contains a DynamoDB / `s3+ddb://` section, and the exact DynamoDB table schema (attribute names such as `base_uri` / `version`, key definitions) was not present on any page fetched on 2026-07-08. The `s3+ddb://...?ddbTableName=...` scheme itself is confirmed from LanceDB usage docs; treat the internal table-schema specifics as unconfirmed and consult the current LanceDB S3/DynamoDB guide before provisioning a table.

### `commit_lock` — a custom lock

Both `lance.dataset` and `lance.write_dataset` accept `commit_lock: Optional[CommitLock]` (`from lance.commit import CommitLock`). Docstring: "A custom commit lock. Only needed if your object store does not support atomic commits." This is the programmatic alternative to a URI-embedded external store: supply a `CommitLock` implementation that provides mutual exclusion around the commit. Not needed for S3/R2 native atomic writes or single-writer workflows.

### Conflict resolution

When two commits do race, Lance classifies the loser's transaction (transaction spec):

- **Rebasable** — the transaction is modified to incorporate the concurrent change and re-applied.
- **Retryable** — the operation can be re-executed at the application level.
- **Incompatible** — a fundamental conflict; non-retryable failure.

---

## Multipart upload cleanup

Large writes use S3 multipart uploads. Graceful shutdown aborts incomplete uploads; a hard crash may leave orphaned multipart uploads. Configure an S3/R2 **lifecycle rule to abort incomplete multipart uploads** so they don't accrue storage cost. (Source: LanceDB storage config.)

---

## Common footguns (summary)

- **`storage_options` values must be strings.** `"true"`, `"false"`, `"60s"`, `"3"` — never Python `bool`/`int`.
- **S3-compatible stores (incl. custom endpoints) REQUIRE both `region` and `endpoint`.** Region auto-detection only works against real AWS S3.
- **R2 uses `s3://` + `region: "auto"` + the account R2 endpoint. Do NOT set `allow_http`** (endpoint is HTTPS); do NOT invent an `r2://` scheme.
- **`allow_http` is only for plaintext `http://` endpoints** (local MinIO). Setting it against an HTTPS endpoint is pointless and, if it downgrades, insecure.
- **`AWS_PROFILE` (SSO) is env-var only** — it cannot go in the `storage_options` dict.
- **`use_legacy_format` is deprecated** — use `data_storage_version` instead (see `write_dataset` signature).
- **Single writer per dataset is always safe.** Reach for `commit_lock` / external stores only for genuine multi-writer concurrency on a store lacking atomic put-if-absent.

---

## Cross-references

- `00_overview.md` — Lance & LanceDB overview, ecosystem, packaging & versions.
- `02_python_dataset_api.md` — full `lance.dataset` / `lance.write_dataset` / `LanceDataset` surface (this file covers only the `storage_options` slice).
- `03_writes_appends_upserts.md` — write modes (`create`/`append`/`overwrite`), `merge_insert`, `LanceOperation` commits — all take the same `storage_options`.
- `04_versioning_time_travel.md` — manifest versions, tags, `cleanup_old_versions` (the versions committed by the protocol described here).
- `08_compaction_maintenance.md` — compaction / fragment management, which also commits via this protocol.
- `10_duckdb_arrow_interop.md` — zero-copy Arrow between DuckDB and Lance; the read/write side of the R2 pipeline.
- `11_lancedb_table_api.md` — the LanceDB DB/table API, which reuses these exact `storage_options` keys.
