"""Compute worker — People Data Labs (PDL) Free Company Dataset bulk ingest.

Part of the ``pdl-companies`` Modal app. Endpoint-less; spawned by the Universal
Dispatcher (core/modal_dispatcher.py) or driven by the local entrypoints. A full
point-in-time snapshot of PDL's free company firmographic file (~35.4M companies).

Source shape (verified by structural probe — see the diagnostic report):
    The operator landed a single payload at
    s3://data-sink/landing/pdl_companies/free_company_dataset.pipe.zip
    (2,277,891,927 bytes). Despite the .zip extension the container is GZIP
    (magic 1f 8b), NOT PKZIP — a single gzip stream of a pipe-delimited,
    double-quoted ("), backslash-escaped, UTF-8 CSV (5,563,726,561 bytes
    decompressed; 35,446,771 rows; 10 columns). The .csv.zip / .json.zip
    siblings are NOT touched.

Data plane (clean-room — DuckDB does 100% of the transform):
    R2 landing gzip
      → boto3 download → /tmp/pdl/free_company_dataset.pipe.gz   (Python: I/O only)
      → DuckDB read_csv(delim='|', compression='gzip', all_varchar) → project/cast (100% in SQL)
      → Arrow (to_arrow_table; reader fallback at scale)
      → lance.write_dataset(LOCAL /tmp, v2.1, overwrite) + BTREE/BITMAP indexes
      → boto3 publish (wipe + upload) → s3://data-sink/active/pdl_companies/

Why LOCAL stage + boto3 publish (NOT a direct Lance write to R2): Lance's direct
write to R2 trips R2's multipart rule — "all non-trailing parts must have the same
length" (400 InvalidPart) — once a scalar-index page_data.lance file is large
enough to force object_store's adaptive multipart to ESCALATE part size mid-upload
(AWS S3 tolerates unequal non-trailing parts; R2 does not). At 35.4M rows the
near-unique string indexes (pdl_company_id / company_name / linkedin_url) cross
that threshold and fail. So the whole dataset (data + indices) is built on local
disk (no multipart) and published once via boto3 (s3transfer = uniform parts →
R2-compliant). This mirrors pipelines/sam_gov/entity_registrations_bulk.py, which
solved the identical constraint.

Encoding: GENUINE UTF-8 (verified — 0 invalid sequences across 5.56 GB; 39.96M
high bytes are legitimate multibyte). NO cp1252 transcode (a transcode would
corrupt valid multibyte). Python streams the landed bytes untouched.

Typing (verified): employee_size_range (src ``size``) is a CATEGORICAL bucket
("1-10" … "10001+", 8 distinct, 100% non-integer) — strict VARCHAR; a numeric
cast would null every row. year_founded (src ``founded``) is 100% integer-castable
(0 failures; 36.6% populated) — TRY_CAST to INTEGER for numeric range scans.
pdl_company_id (src ``id``) is a perfect unique key (35,446,771 distinct /
35,446,771 rows, verified). company_name, domain (src ``website``), linkedin_url,
locality/region/country all stay VARCHAR. No VARIANT.

Refresh: MANUAL R2 drops only (standalone snapshot). Lance overwrite rebuilds the
dataset wholesale; immutable-version MVCC retains prior snapshots for time travel.

    modal deploy pipelines/pdl_companies/free_company_dataset.py
    modal run    pipelines/pdl_companies/free_company_dataset.py::initdb       # create ops.* table
    modal run    pipelines/pdl_companies/free_company_dataset.py::run          # ingest landed snapshot → Lance
    modal run    pipelines/pdl_companies/free_company_dataset.py::reindex      # rebuild scalar indexes only
    modal run    pipelines/pdl_companies/free_company_dataset.py::show_ledger  # print ops ledger
"""

from __future__ import annotations

import os

import modal

BUCKET = "data-sink"
# Manual landing key (NO upstream fetch — manual R2 drops only). The R2 object is
# named .pipe.zip but is gzip content; downloaded to a .gz scratch path.
LANDING_KEY = "landing/pdl_companies/free_company_dataset.pipe.zip"
# Lance system-of-record tier (NOT the landing/raw zone). R2 key prefix + URI.
DATASET_PREFIX = "active/pdl_companies/"
DATASET_URI = os.environ.get("PDL_COMPANIES_LANCE_URI", f"s3://{BUCKET}/{DATASET_PREFIX}")
SCRATCH_DIR = "/tmp/pdl"
LOCAL_GZ = os.path.join(SCRATCH_DIR, "free_company_dataset.pipe.gz")
LOCAL_DATASET = os.path.join(SCRATCH_DIR, "pdl_lance")  # local Lance staging (pre-publish)
FEED = "pdl_companies"

# Lance fragment sizing.
#   max_rows_per_file = 1048576 — exact (also the Lance default).
#   max_bytes_per_file: the directive (diagnostic + approval) wrote `90 * 10243`
#   annotated "(90 GiB)". The ONLY value equal to 90 GiB is `90 * 1024**3`
#   (= 96,636,764,160). The literal `90 * 10243` (= 921,870 ≈ 900 KB) would
#   shatter the 35.4M-row snapshot into tens of thousands of fragments — the
#   exact storage failure the approval credited the diagnostic with catching.
#   The approved INTENT is 90 GiB; honor it. (Same correction as ny_sos / ca_ucc /
#   entity_registrations.)
MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3
# Net-new dataset → pin the current Lance default (per 02_lancedb_storage.md §2.3).
DATA_STORAGE_VERSION = "2.1"
# Streaming fallback batch (pre-authorized): one Arrow RecordBatch per 1,048,576
# rows if the full materialization hits a catchable allocation error.
STREAM_BATCH_ROWS = 1048576

# Scalar index plan (approved; cardinality from the structural probe over 35.4M).
# BTREE for high-cardinality load-bearing resolution keys (equality + range);
# BITMAP for low-cardinality categoricals filtered frequently.
PDL_BTREE_INDEXES = [
    "pdl_company_id",   # canonical PDL key (35,446,771 distinct = perfect PK)
    "company_name",     # name resolution (high-card, 100% populated)
    "linkedin_url",     # near-unique resolution key (100% populated)
    "domain",           # ~22.3M distinct, 66% populated
    "locality",         # ~282k distinct geo resolution
    "year_founded",     # INTEGER range scans ("founded since X")
]
PDL_BITMAP_INDEXES = [
    "industry",             # 152 distinct
    "country",              # 263 distinct
    "region",               # ~4,176 distinct
    "employee_size_range",  # 8 distinct buckets
]

# Terminal-state ledger. CREATE SCHEMA + CREATE TABLE run as SEPARATE statements
# (psycopg sends one command per execute()). Mirrors the ops.* contract
# (ARCHITECTURE.md §5); adds distinct_ids + write_path as integrity/audit signals.
_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS ops.pdl_company_runs (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed            TEXT        NOT NULL,
    dataset_uri     TEXT        NOT NULL,
    source_file     TEXT        NOT NULL,
    landing_key     TEXT        NOT NULL,
    snapshot_date   DATE,
    rows_processed  BIGINT,
    distinct_ids    BIGINT,
    write_path      TEXT,
    status          TEXT        NOT NULL,
    error           TEXT,
    started_at      TIMESTAMPTZ NOT NULL,
    completed_at    TIMESTAMPTZ NOT NULL
)
"""

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",       # >=1.5 guarantees to_arrow_table; <2 stays below the v2.0 break
    "lancedb>=0.15",
    "pylance>=7",           # provides `import lance`; lancedb does not re-export it
    "pyarrow>=17",
    "boto3>=1.35",          # R2 landing read + dataset publish (uniform-part multipart)
    "requests>=2.32",       # Trigger waitpoint callback
    "psycopg[binary]>=3.2",  # ops.* terminal state
).env(
    # BTREE training sorts the column; bypass Lance's bounded spill-to-disk
    # ExternalSorter, whose memory accounting under-sizes the pool and exhausts
    # on multi-million-row string columns. PDL sorts company_name / linkedin_url
    # (35.4M each), domain (23.5M), locality (29.2M) — in-memory sort is the only
    # path that completes. See lance-format/lance#2650.
    {"LANCE_BYPASS_SPILLING": "true"}
)

app = modal.App("pdl-companies", image=image)


def _r2_storage_options() -> dict[str, str]:
    """object_store options for Cloudflare R2, sourced from the Modal secret.
    AWS-style creds + explicit endpoint + region 'auto'. Endpoint supplied
    directly (R2_ENDPOINT) or derived from R2_ACCOUNT_ID. Used by the boto3
    client; Lance writes/reads are LOCAL (no storage_options)."""
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


def _s3_client():
    """boto3 S3 client for R2. Forces checksum behaviour to ``when_required``:
    botocore's default flexible-checksum validation does not match R2's
    semantics and otherwise raises on get/put."""
    import boto3
    from botocore.config import Config

    so = _r2_storage_options()
    cfg = Config(request_checksum_calculation="when_required",
                 response_checksum_validation="when_required")
    return boto3.client(
        "s3", endpoint_url=so["endpoint"],
        aws_access_key_id=so["aws_access_key_id"],
        aws_secret_access_key=so["aws_secret_access_key"],
        region_name="auto", config=cfg,
    )


def _build_sql(gz_path: str, snapshot_date: str) -> str:
    """100% of the transform. read_csv(compression='gzip', all_varchar=true) with
    the probe-verified dialect (delim='|', quote='"', escape='\\') →
    nullif/trim on every string + one TRY_CAST(year_founded AS INTEGER) →
    semantic snake_case projection. All interpolated values are repo-controlled
    (our /tmp path, the ISO snapshot date) — no user input — and single-quote
    escaped. NO cp1252 transcode (source is UTF-8). NO VARIANT."""

    def lit(s: str) -> str:
        return s.replace("'", "''")

    backslash = chr(92)  # emit  escape='\'  without Python-escaping noise
    read_opts = (
        f"delim='|', quote='\"', escape='{backslash}', header=true, "
        f"all_varchar=true, compression='gzip', sample_size=-1, ignore_errors=false"
    )
    return f"""
WITH raw AS (
    SELECT * FROM read_csv('{lit(gz_path)}', {read_opts})
)
SELECT
    nullif(trim(id), '')                           AS pdl_company_id,
    nullif(trim(name), '')                         AS company_name,
    nullif(trim(website), '')                      AS domain,
    nullif(trim(linkedin_url), '')                 AS linkedin_url,
    nullif(trim(industry), '')                     AS industry,
    nullif(trim(size), '')                         AS employee_size_range,
    TRY_CAST(nullif(trim(founded), '') AS INTEGER) AS year_founded,
    nullif(trim(locality), '')                     AS locality,
    nullif(trim(region), '')                       AS region,
    nullif(trim(country), '')                      AS country,
    CAST('{lit(snapshot_date)}' AS DATE)           AS snapshot_date,
    now()                                          AS ingested_at
FROM raw
"""


def _create_indexes(local_path: str) -> list[str]:
    """Build BTREE + BITMAP scalar indexes on the LOCAL dataset (no storage_options
    — local writes avoid R2's multipart part-size rule). replace=True → idempotent.
    Per-column try/except: an index miss must not fail an otherwise-good load."""
    import lance

    ds = lance.dataset(local_path)
    built: list[str] = []
    for col in PDL_BTREE_INDEXES:
        try:
            ds.create_scalar_index(col, "BTREE", replace=True)
            built.append(f"BTREE:{col}")
            print(f"  BTREE  ✓ {col}")
        except Exception as exc:  # noqa: BLE001 — an index miss must not fail the load
            print(f"  WARN: BTREE index on {col} failed: {exc}")
    for col in PDL_BITMAP_INDEXES:
        try:
            ds.create_scalar_index(col, "BITMAP", replace=True)
            built.append(f"BITMAP:{col}")
            print(f"  BITMAP ✓ {col}")
        except Exception as exc:  # noqa: BLE001
            print(f"  WARN: BITMAP index on {col} failed: {exc}")
    return built


def _list_committed_indices(ds) -> list:
    """Best-effort read of committed scalar indices. Tolerant of pylance
    return-shape drift (dict vs object, list_indices vs list_indexes)."""
    for attr in ("list_indices", "list_indexes"):
        fn = getattr(ds, attr, None)
        if fn is None:
            continue
        try:
            out = []
            for ix in fn():
                if isinstance(ix, dict):
                    out.append({k: ix.get(k) for k in ("name", "type", "fields")})
                else:
                    out.append({
                        "name": getattr(ix, "name", None),
                        "type": str(getattr(ix, "type", None)),
                        "fields": getattr(ix, "fields", None),
                    })
            return out
        except Exception as exc:  # noqa: BLE001
            return [{"error": f"{attr}: {exc}"}]
    return [{"error": "no list_indices/list_indexes method on dataset"}]


def _replace_r2_prefix(s3, prefix: str, local_dir: str) -> int:
    """Idempotent publish: wipe the R2 prefix, then upload the local Lance dataset
    (boto3/s3transfer = uniform-part multipart, R2-compliant). Returns files
    uploaded. Mirrors pipelines/sam_gov/entity_registrations_bulk.py."""
    to_del = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        for o in page.get("Contents", []):
            to_del.append({"Key": o["Key"]})
            if len(to_del) == 1000:
                s3.delete_objects(Bucket=BUCKET, Delete={"Objects": to_del, "Quiet": True})
                to_del = []
    if to_del:
        s3.delete_objects(Bucket=BUCKET, Delete={"Objects": to_del, "Quiet": True})

    uploaded = 0
    for root, _, files in os.walk(local_dir):
        for fn in files:
            lp = os.path.join(root, fn)
            rel = os.path.relpath(lp, local_dir).replace(os.sep, "/")
            s3.upload_file(lp, BUCKET, prefix + rel)
            uploaded += 1
    return uploaded


def _download_r2_prefix(s3, prefix: str, local_dir: str) -> int:
    """Stage the committed R2 dataset back to local disk (for an in-place reindex
    without re-ingesting). Returns files downloaded."""
    import shutil

    shutil.rmtree(local_dir, ignore_errors=True)
    n = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        for o in page.get("Contents", []):
            rel = o["Key"][len(prefix):]
            if not rel:  # prefix placeholder
                continue
            lp = os.path.join(local_dir, rel.replace("/", os.sep))
            os.makedirs(os.path.dirname(lp), exist_ok=True)
            s3.download_file(BUCKET, o["Key"], lp)
            n += 1
    return n


def _record_run(dataset_uri, source_file, landing_key, snapshot_date, rows,
                distinct_ids, write_path, status, error, started_at,
                completed_at) -> None:
    """Terminal run row → ops.pdl_company_runs (psycopg). Best-effort: never let
    an audit-write failure crash an otherwise-good ingest."""
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* write.")
        return
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops.pdl_company_runs
                    (feed, dataset_uri, source_file, landing_key, snapshot_date,
                     rows_processed, distinct_ids, write_path, status, error,
                     started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (FEED, dataset_uri, source_file, landing_key, snapshot_date,
                 rows, distinct_ids, write_path, status, error,
                 started_at, completed_at),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must not mask the ingest
        print(f"WARN: ops.* write failed: {exc}")


def _post_callback(url, payload, attempts: int = 3) -> None:
    """POST the RAW terminal payload to the Trigger waitpoint url. The whole body
    becomes result.output — NO API key, NO {"data": ...} envelope (flat)."""
    if not url:
        print("No trigger_callback_url (manual run); skipping callback.")
        return
    import time

    import requests

    for i in range(attempts):
        try:
            resp = requests.post(url, json=payload, timeout=30)
            if resp.status_code < 300:
                print(f"Callback delivered: {payload}")
                return
            print(f"Callback attempt {i + 1} non-2xx: {resp.status_code} {resp.text[:200]}")
        except Exception as exc:  # noqa: BLE001
            print(f"Callback attempt {i + 1} failed: {exc}")
        time.sleep(2 * (i + 1))
    print(f"WARN: callback delivery failed after {attempts} attempts → {url}")


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 5)
def init_schema() -> dict:
    """Create the ops schema + pdl_company_runs ledger (idempotent)."""
    import psycopg

    dsn = os.environ["HQX_DB_URL_POOLED"]
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS ops;")
        cur.execute(_CREATE_TABLE_SQL)
        conn.commit()
    print("ops.pdl_company_runs ready")
    return {"status": "ok", "table": "ops.pdl_company_runs"}


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 90,
    memory=32768,
    cpu=8.0,
    ephemeral_disk=524288,  # Modal's explicit floor (512 GiB); local staging needs ~10 GiB
)
def ingest_pdl_companies(key: str = LANDING_KEY,
                         trigger_callback_url: str | None = None) -> dict:
    """Download landed gzip (UTF-8, NO transcode) → DuckDB project/cast → Arrow →
    Lance overwrite on LOCAL disk → BTREE/BITMAP index locally → boto3 publish to
    R2 (uniform-part multipart). Full to_arrow_table() materialization in the
    32 GiB container; pre-authorized streaming to_arrow_reader fallback on a
    catchable allocation error. Records ops.* state, wakes the Trigger run.
    Re-raises on failure so the Modal call is marked failed."""
    import datetime as dt
    import shutil

    import duckdb
    import lance

    started_at = dt.datetime.now(dt.timezone.utc)
    snapshot_date = started_at.date().isoformat()
    source_file = key.rsplit("/", 1)[-1]
    rows = 0
    distinct_ids: int | None = None
    write_path = "materialize"
    status = "error"
    error: str | None = None
    built: list[str] = []

    try:
        s3 = _s3_client()
        os.makedirs(SCRATCH_DIR, exist_ok=True)
        shutil.rmtree(LOCAL_DATASET, ignore_errors=True)
        # I/O ONLY — gzip bytes straight to scratch, NO transcode, NO extract.
        print(f"Downloading s3://{BUCKET}/{key} → {LOCAL_GZ}")
        s3.download_file(BUCKET, key, LOCAL_GZ)
        print(f"  downloaded {os.path.getsize(LOCAL_GZ):,} bytes")

        sql = _build_sql(LOCAL_GZ, snapshot_date)
        con = duckdb.connect(":memory:")
        try:
            con.execute("PRAGMA threads=8;")
            con.execute("SET enable_progress_bar=false;")
            try:
                # PRIMARY — full materialization (~7 GiB Arrow; fits 32 GiB).
                table = con.sql(sql).to_arrow_table()
                rows = table.num_rows
                # Approved build-time integrity check: exact DISTINCT on the
                # PK-grade key (zero-copy replacement scan over the Arrow table).
                con.register("proj", table)
                distinct_ids = con.sql(
                    "SELECT count(DISTINCT pdl_company_id) FROM proj"
                ).fetchone()[0]
                con.unregister("proj")
                print(f"  materialized {rows:,} rows; distinct pdl_company_id = {distinct_ids:,}")
                # LOCAL write (no storage_options) — avoids R2 multipart rule.
                lance.write_dataset(
                    table,
                    LOCAL_DATASET,
                    mode="overwrite",
                    data_storage_version=DATA_STORAGE_VERSION,
                    max_rows_per_file=MAX_ROWS_PER_FILE,
                    max_bytes_per_file=MAX_BYTES_PER_FILE,
                )
            except (MemoryError, duckdb.OutOfMemoryException) as mem_exc:
                # FALLBACK (pre-authorized) — stream RecordBatches; flat RAM.
                # distinct-id is skipped here to preserve bounded memory.
                write_path = "stream"
                print(f"  materialization hit {type(mem_exc).__name__}; "
                      f"falling back to streaming to_arrow_reader: {mem_exc}")
                con.close()
                con = duckdb.connect(":memory:")
                con.execute("PRAGMA threads=8;")
                con.execute("SET enable_progress_bar=false;")
                reader = con.sql(sql).to_arrow_reader(STREAM_BATCH_ROWS)
                lance.write_dataset(
                    reader,
                    LOCAL_DATASET,
                    schema=reader.schema,   # REQUIRED for a reader/iterator source
                    mode="overwrite",
                    data_storage_version=DATA_STORAGE_VERSION,
                    max_rows_per_file=MAX_ROWS_PER_FILE,
                    max_bytes_per_file=MAX_BYTES_PER_FILE,
                )
        finally:
            con.close()

        if write_path == "stream":
            rows = lance.dataset(LOCAL_DATASET).count_rows()
            print(f"  streamed {rows:,} rows (distinct-id check skipped on stream path)")

        # Index on LOCAL disk (all 10 succeed — no R2 multipart), then publish.
        built = _create_indexes(LOCAL_DATASET)
        uploaded = _replace_r2_prefix(s3, DATASET_PREFIX, LOCAL_DATASET)
        print(f"Published {uploaded} files → {DATASET_URI}")
        status = "success"
    except Exception as exc:  # noqa: BLE001 — terminal handling below + re-raise
        error = str(exc)
        status = "error"
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run(DATASET_URI, source_file, key, snapshot_date, int(rows),
                    distinct_ids, write_path, status, error, started_at, completed_at)
        _post_callback(
            trigger_callback_url,
            {"status": status, "rows": int(rows), "feed": FEED,
             "dataset_uri": DATASET_URI, "snapshot_date": snapshot_date,
             "distinct_ids": distinct_ids, "write_path": write_path},
        )

    if status != "success":
        raise RuntimeError(f"pdl_companies ingest failed for {key}: {error}")

    # Fan out the derived-sidecar rebuild so a new base snapshot cannot leave
    # pdl_normalized_companies silently stale (the staleness-trigger contract —
    # docs/plans/PDL_NORMALIZED_COMPANIES_SIDECAR_PLAN.md §9). Best-effort: the base ingest
    # has already succeeded and published; a fan-out miss must not fail it (the consumer
    # contract's source_version fail-closed assert is the backstop).
    try:
        modal.Function.from_name(
            "pdl-normalized-companies", "ingest_normalized_companies"
        ).spawn(trigger_callback_url=None)
        print("Fanned out pdl_normalized_companies rebuild.")
    except Exception as exc:  # noqa: BLE001 — fan-out is additive; never fail the base ingest
        print(f"WARN: could not fan out pdl_normalized_companies rebuild: {exc}")

    return {"feed": FEED, "source_file": source_file, "rows_processed": int(rows),
            "distinct_ids": distinct_ids, "write_path": write_path,
            "dataset_uri": DATASET_URI, "snapshot_date": snapshot_date,
            "indices": built, "status": status}


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials")],
    timeout=60 * 90,
    memory=32768,
    cpu=8.0,
    ephemeral_disk=524288,
)
def reindex_pdl_companies() -> dict:
    """(Re)build the scalar indexes without re-ingesting: stage the committed R2
    dataset to local disk, index locally (no R2 multipart), publish back via
    boto3. Idempotent (replace=True)."""
    import lance

    s3 = _s3_client()
    staged = _download_r2_prefix(s3, DATASET_PREFIX, LOCAL_DATASET)
    print(f"Staged {staged} files from {DATASET_URI} → {LOCAL_DATASET}")
    built = _create_indexes(LOCAL_DATASET)
    uploaded = _replace_r2_prefix(s3, DATASET_PREFIX, LOCAL_DATASET)
    print(f"Published {uploaded} files → {DATASET_URI}")
    ds = lance.dataset(LOCAL_DATASET)
    committed = _list_committed_indices(ds)
    print(f"Committed indices: {committed}")
    return {"dataset": DATASET_URI, "built": built, "committed_indices": committed}


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 5)
def ledger(limit: int = 5) -> list:
    """Read the most recent ops.pdl_company_runs rows."""
    import psycopg

    dsn = os.environ["HQX_DB_URL_POOLED"]
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT id, feed, dataset_uri, source_file, snapshot_date, "
            "rows_processed, distinct_ids, write_path, status, error, "
            "started_at, completed_at "
            "FROM ops.pdl_company_runs ORDER BY id DESC LIMIT %s",
            (limit,),
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


@app.local_entrypoint()
def initdb() -> None:
    """Create the ops ledger table (run once before the first ingest)."""
    import json

    print(json.dumps(init_schema.remote(), indent=2, default=str))


@app.local_entrypoint()
def run(key: str = LANDING_KEY) -> None:
    """Ingest the landed snapshot → Lance (manual ops path; no Trigger callback)."""
    import json

    print(json.dumps(ingest_pdl_companies.remote(key, trigger_callback_url=None),
                     indent=2, default=str))


@app.local_entrypoint()
def reindex() -> None:
    """Rebuild the scalar indexes on the existing dataset (no re-ingest)."""
    import json

    print(json.dumps(reindex_pdl_companies.remote(), indent=2, default=str))


@app.local_entrypoint()
def show_ledger(limit: int = 5) -> None:
    """Print the most recent ops ledger rows."""
    import json

    print(json.dumps(ledger.remote(limit), indent=2, default=str))
