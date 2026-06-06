"""Compute worker — ``pdl_normalized_companies`` derived blocking-key sidecar.

Part of the ``pdl-normalized-companies`` Modal app. A thin, fully-indexed projection over the
firmographic SoR ``pdl_companies`` (Lance, R2) that materializes the fleet-canonical entity-resolution
blocking keys ONCE — ``company_name_norm`` / ``company_legal_base`` / ``normalized_domain`` /
``linkedin_slug`` — each BTREE-indexed, plus an ``is_generic_domain`` BITMAP flag and inline
geo/sector/size/age tiebreak columns. Downstream ``name|domain|linkedin → pdl_company_id`` bridges pay
a ``ScalarIndexQuery`` point-lookup instead of a 35.4M-row ``name_norm()``-at-read-time full scan
(67,646× — see docs/reference/PDL_INDEX_TOPOLOGY_PUSHDOWN_DIAGNOSTIC.md §3.1).

Plan of record: docs/plans/PDL_NORMALIZED_COMPANIES_SIDECAR_PLAN.md.

Architecture — sidecar, NOT columns on the SoR (plan §2):
    pdl_companies is NEVER mutated. This worker reads it read-only (projection pushdown, 10 cols) and
    writes ONLY the new prefix active/pdl_normalized_companies/. The SoR's only write paths are full
    wipe-republish; inlining would rewrite a 35.4M-row SoR other consumers read. A derived sidecar is a
    pure create — idempotent overwrite-from-immutable-source, zero blast radius.

Data plane (clean-room — DuckDB does 100% of the transform):
    Lance scanner (R2, projection-pushed, version-pinned)
      → DuckDB register reader → name_norm/web_norm projection (100% in SQL)
      → Arrow (to_arrow_table; streaming to_arrow_reader fallback at scale)
      → lance.write_dataset(LOCAL /tmp, v2.1, overwrite) + 5 BTREE + 1 BITMAP indexes
      → boto3 publish (wipe + upload) → s3://data-sink/active/pdl_normalized_companies/

Why LOCAL stage + boto3 publish (NOT a direct Lance write to R2): R2's multipart "all non-trailing
parts equal length" rule trips once a scalar-index page_data.lance file is large enough to force
object_store's adaptive multipart to escalate part size mid-upload (400 InvalidPart). Build the whole
dataset (data + indices) on local disk (no multipart) and publish once via boto3 (s3transfer = uniform
parts → R2-compliant). Mirrors pipelines/pdl_companies/free_company_dataset.py.

Canonical keys come from core.name_norm + core.web_norm — the single rule definition. NEVER re-inline.

    modal deploy pipelines/pdl_companies/pdl_normalized_companies.py
    modal run    pipelines/pdl_companies/pdl_normalized_companies.py::initdb       # create ops.* table
    modal run    pipelines/pdl_companies/pdl_normalized_companies.py::run          # build sidecar → Lance
    modal run    pipelines/pdl_companies/pdl_normalized_companies.py::reindex      # rebuild scalar indexes
    modal run    pipelines/pdl_companies/pdl_normalized_companies.py::show_ledger  # print ops ledger
"""

from __future__ import annotations

import os

import modal

from core.name_norm import legal_name_base, name_norm
from core.web_norm import _bare_host, is_generic_domain, linkedin_slug, normalized_domain

BUCKET = "data-sink"
# Read-only source: the firmographic SoR. NEVER written by this worker.
SRC_PREFIX = "active/pdl_companies/"
SRC_URI = os.environ.get("PDL_COMPANIES_LANCE_URI", f"s3://{BUCKET}/{SRC_PREFIX}")
# Output: net-new derived sidecar prefix.
DATASET_PREFIX = "active/pdl_normalized_companies/"
DATASET_URI = os.environ.get("PDL_NORMALIZED_LANCE_URI", f"s3://{BUCKET}/{DATASET_PREFIX}")
SCRATCH_DIR = "/tmp/pdl_norm"
LOCAL_DATASET = os.path.join(SCRATCH_DIR, "pdl_norm_lance")
FEED = "pdl_normalized_companies"

# Projection pushdown — only these 10 source columns leave R2 (never the full 16).
SCAN_COLS = [
    "pdl_company_id", "company_name", "domain", "linkedin_url",
    "locality", "region", "country", "industry", "employee_size_range", "year_founded",
]

# Lance fragment sizing (exact; mirrors free_company_dataset.py / the Lance defaults).
MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3
DATA_STORAGE_VERSION = "2.1"
STREAM_BATCH_ROWS = 1048576

# Scalar index plan (plan §6). BTREE for the high-cardinality blocking keys (equality + range);
# BITMAP for the low-card is_generic_domain seek flag.
PDL_NORM_BTREE_INDEXES = [
    "pdl_company_id", "company_name_norm", "company_legal_base", "normalized_domain", "linkedin_slug",
]
PDL_NORM_BITMAP_INDEXES = ["is_generic_domain"]

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS ops.pdl_normalized_runs (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed            TEXT        NOT NULL,
    dataset_uri     TEXT        NOT NULL,
    source_dataset  TEXT        NOT NULL,
    source_version  BIGINT,
    source_rows     BIGINT,
    rows_processed  BIGINT,
    distinct_ids    BIGINT,
    status          TEXT        NOT NULL,
    error           TEXT,
    started_at      TIMESTAMPTZ NOT NULL,
    completed_at    TIMESTAMPTZ NOT NULL
)
"""

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",
    "lancedb>=0.15",
    "pylance>=7",
    "pyarrow>=17",
    "boto3>=1.35",
    "requests>=2.32",
    "psycopg[binary]>=3.2",
).env(
    # BTREE training sorts the column; bypass Lance's bounded spill-to-disk ExternalSorter, whose
    # memory accounting under-sizes the pool and exhausts on multi-million-row string columns
    # (company_name_norm ~30M distinct, linkedin_slug ~unique). See lance-format/lance#2650.
    {"LANCE_BYPASS_SPILLING": "true"}
).add_local_python_source("core.name_norm", "core.web_norm")

app = modal.App("pdl-normalized-companies", image=image)


def _r2_storage_options() -> dict[str, str]:
    """object_store options for Cloudflare R2 (AWS-style creds + explicit endpoint + region auto)."""
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
    """boto3 S3 client for R2 (checksum behaviour forced to when_required for R2 semantics)."""
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


def build_normalized_companies_sql(source_version: int) -> str:
    """Project pdl_companies → normalized blocking-key sidecar. Reads a `src` relation (the scanned
    mirror, SCAN_COLS). 1 row/pdl_company_id passthrough. Three-stage CTE so each heavy regex runs
    once and is_generic_domain reads the fully-materialized normalized_domain column (no forward-alias
    dependency). Canonical builders interpolated from core.name_norm / core.web_norm — never inlined."""
    return f"""
    WITH norm AS (
        SELECT
            pdl_company_id, company_name, locality, region, country,
            industry, employee_size_range, year_founded,
            {name_norm("company_name")}      AS company_name_norm,
            {_bare_host("domain")}           AS _host,
            {linkedin_slug("linkedin_url")}  AS linkedin_slug
        FROM src
    ),
    keyed AS (
        SELECT
            pdl_company_id, company_name, locality, region, country,
            industry, employee_size_range, year_founded,
            company_name_norm, linkedin_slug,
            {legal_name_base("company_name_norm")} AS company_legal_base,
            {normalized_domain("_host")}           AS normalized_domain
        FROM norm
    )
    SELECT
        nullif(trim(pdl_company_id), '')           AS pdl_company_id,
        company_name_norm,
        company_legal_base,
        normalized_domain,
        {is_generic_domain("normalized_domain")}   AS is_generic_domain,
        linkedin_slug,
        nullif(trim(company_name), '')             AS company_name,
        nullif(trim(locality), '')                 AS locality,
        nullif(trim(region), '')                   AS region,
        nullif(trim(country), '')                  AS country,
        nullif(trim(industry), '')                 AS industry,
        nullif(trim(employee_size_range), '')      AS employee_size_range,
        year_founded                               AS year_founded,
        CAST({int(source_version)} AS BIGINT)      AS source_version,
        now()                                      AS built_at
    FROM keyed
    WHERE nullif(trim(pdl_company_id), '') IS NOT NULL
    """


def _create_indexes(local_path: str) -> list[str]:
    """Build 5 BTREE + 1 BITMAP scalar indexes on the LOCAL dataset (no storage_options — local writes
    avoid R2's multipart part-size rule). replace=True → idempotent."""
    import lance

    ds = lance.dataset(local_path)
    built: list[str] = []
    for col in PDL_NORM_BTREE_INDEXES:
        try:
            ds.create_scalar_index(col, "BTREE", replace=True)
            built.append(f"BTREE:{col}")
            print(f"  BTREE  ✓ {col}")
        except Exception as exc:  # noqa: BLE001 — an index miss must not fail an otherwise-good build
            print(f"  WARN: BTREE index on {col} failed: {exc}")
    for col in PDL_NORM_BITMAP_INDEXES:
        try:
            ds.create_scalar_index(col, "BITMAP", replace=True)
            built.append(f"BITMAP:{col}")
            print(f"  BITMAP ✓ {col}")
        except Exception as exc:  # noqa: BLE001
            print(f"  WARN: BITMAP index on {col} failed: {exc}")
    return built


def _replace_r2_prefix(s3, prefix: str, local_dir: str) -> int:
    """Idempotent publish: wipe the R2 prefix, then upload the local Lance dataset (boto3/s3transfer =
    uniform-part multipart, R2-compliant). Returns files uploaded. Targets ONLY the sidecar prefix."""
    paginator = s3.get_paginator("list_objects_v2")
    to_delete: list[dict] = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for o in page.get("Contents", []):
            to_delete.append({"Key": o["Key"]})
            if len(to_delete) == 1000:
                s3.delete_objects(Bucket=BUCKET, Delete={"Objects": to_delete, "Quiet": True})
                to_delete = []
    if to_delete:
        s3.delete_objects(Bucket=BUCKET, Delete={"Objects": to_delete, "Quiet": True})
    uploaded = 0
    for root, _dirs, files in os.walk(local_dir):
        for fn in files:
            lp = os.path.join(root, fn)
            rel = os.path.relpath(lp, local_dir).replace(os.sep, "/")
            s3.upload_file(lp, BUCKET, prefix + rel)
            uploaded += 1
    return uploaded


def _record_run(source_version, source_rows, rows, distinct_ids, status, error,
                started_at, completed_at) -> None:
    """Terminal run row → ops.pdl_normalized_runs (psycopg). Best-effort; never masks the build."""
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* write.")
        return
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops.pdl_normalized_runs
                    (feed, dataset_uri, source_dataset, source_version, source_rows,
                     rows_processed, distinct_ids, status, error, started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (FEED, DATASET_URI, SRC_URI, source_version, source_rows,
                 rows, distinct_ids, status, error, started_at, completed_at),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must not mask the build
        print(f"WARN: ops.* write failed: {exc}")


def _post_callback(url, payload, attempts: int = 3) -> None:
    """POST the raw terminal payload to the Trigger waitpoint url (flat body, no envelope)."""
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
    """Create the ops schema + pdl_normalized_runs ledger (idempotent)."""
    import psycopg

    dsn = os.environ["HQX_DB_URL_POOLED"]
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS ops;")
        cur.execute(_CREATE_TABLE_SQL)
        conn.commit()
    print("ops.pdl_normalized_runs ready")
    return {"status": "ok", "table": "ops.pdl_normalized_runs"}


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 90,
    memory=32768,
    cpu=8.0,
    ephemeral_disk=524288,  # 512 GiB local NVMe for the index external-sort
)
def ingest_normalized_companies(trigger_callback_url: str | None = None) -> dict:
    """Read pdl_companies (read-only, projection-pushed, version-pinned) → DuckDB name_norm/web_norm
    projection → Arrow → Lance overwrite on LOCAL disk → 5 BTREE + 1 BITMAP index locally → boto3
    publish to the NEW prefix. pdl_companies is NEVER written. Records ops.* state, wakes the Trigger
    run. Re-raises on failure so the Modal call is marked failed."""
    import datetime as dt
    import shutil

    import duckdb
    import lance

    started_at = dt.datetime.now(dt.timezone.utc)
    rows = 0
    distinct_ids: int | None = None
    source_version: int | None = None
    source_rows: int | None = None
    status = "error"
    error: str | None = None
    built: list[str] = []

    try:
        so = _r2_storage_options()
        s3 = _s3_client()
        os.makedirs(SCRATCH_DIR, exist_ok=True)
        os.makedirs(os.path.join(SCRATCH_DIR, "duckdb_spill"), exist_ok=True)
        shutil.rmtree(LOCAL_DATASET, ignore_errors=True)

        # Open the source ONCE; pin the version so every read is the same immutable snapshot (no
        # overwrite-during-build race). Only SCAN_COLS leave R2.
        ds_head = lance.dataset(SRC_URI, storage_options=so)
        source_version = ds_head.version
        ds = lance.dataset(SRC_URI, version=source_version, storage_options=so)
        source_rows = ds.count_rows()
        print(f"source pdl_companies v{source_version}: {source_rows:,} rows (projection={SCAN_COLS})")

        con = duckdb.connect(":memory:")
        con.execute("PRAGMA threads=8;")
        con.execute("SET enable_progress_bar=false;")
        con.execute(f"SET temp_directory='{SCRATCH_DIR}/duckdb_spill';")
        con.execute("SET memory_limit='24GB';")

        sql = build_normalized_companies_sql(source_version)
        try:
            # PRIMARY — full materialization (~7.7 GiB Arrow; fits 32 GiB).
            con.register("src", ds.scanner(columns=SCAN_COLS).to_reader())
            table = con.sql(sql).to_arrow_table()
            rows = table.num_rows
            # Build-time parity asserts (B2) — against the LIVE source under the same null/empty-PK
            # filter, never a hardcoded constant.
            con.register("proj", table)
            distinct_ids = con.sql("SELECT count(DISTINCT pdl_company_id) FROM proj").fetchone()[0]
            con.unregister("proj")
            con.register("srcpk", ds.scanner(columns=["pdl_company_id"]).to_reader())
            src_valid = con.sql(
                "SELECT count(*) FROM srcpk WHERE nullif(trim(pdl_company_id),'') IS NOT NULL"
            ).fetchone()[0]
            con.unregister("srcpk")
            assert rows == distinct_ids, f"PK not 1:1: rows={rows} distinct={distinct_ids}"
            assert rows == src_valid, (
                f"parity drift: committed={rows} src_valid={src_valid} "
                f"(source {source_rows}, dropped {source_rows - src_valid} null-PK)"
            )
            print(f"  parity OK: committed={rows:,} == distinct={distinct_ids:,} == src_valid={src_valid:,}")
            lance.write_dataset(
                table, LOCAL_DATASET, mode="overwrite",
                data_storage_version=DATA_STORAGE_VERSION,
                max_rows_per_file=MAX_ROWS_PER_FILE, max_bytes_per_file=MAX_BYTES_PER_FILE,
            )
            # N5 — free the ~7.7 GiB Arrow buffer BEFORE the 6 index sort-builds run.
            del table
        except (MemoryError, duckdb.OutOfMemoryException) as mem_exc:
            # FALLBACK (pre-authorized) — stream RecordBatches; flat RAM. Parity checked post-write.
            print(f"  materialization hit {type(mem_exc).__name__}; streaming fallback: {mem_exc}")
            con.close()
            con = duckdb.connect(":memory:")
            con.execute("PRAGMA threads=8;")
            con.execute("SET enable_progress_bar=false;")
            con.execute(f"SET temp_directory='{SCRATCH_DIR}/duckdb_spill';")
            con.register("src", ds.scanner(columns=SCAN_COLS).to_reader())
            reader = con.sql(sql).to_arrow_reader(STREAM_BATCH_ROWS)
            lance.write_dataset(
                reader, LOCAL_DATASET, schema=reader.schema, mode="overwrite",
                data_storage_version=DATA_STORAGE_VERSION,
                max_rows_per_file=MAX_ROWS_PER_FILE, max_bytes_per_file=MAX_BYTES_PER_FILE,
            )
            rows = lance.dataset(LOCAL_DATASET).count_rows()
            print(f"  streamed {rows:,} rows (distinct-id check skipped on stream path)")
        finally:
            con.close()

        # Index on LOCAL disk (no R2 multipart), then publish to the NEW prefix.
        built = _create_indexes(LOCAL_DATASET)
        uploaded = _replace_r2_prefix(s3, DATASET_PREFIX, LOCAL_DATASET)
        print(f"Published {uploaded} files → {DATASET_URI} (source pdl_companies UNTOUCHED)")
        status = "success"
    except Exception as exc:  # noqa: BLE001 — terminal handling below + re-raise
        error = str(exc)
        status = "error"
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run(source_version, source_rows, int(rows), distinct_ids, status, error,
                    started_at, completed_at)
        _post_callback(
            trigger_callback_url,
            {"status": status, "rows": int(rows), "feed": FEED, "dataset_uri": DATASET_URI,
             "source_version": source_version, "distinct_ids": distinct_ids},
        )

    if status != "success":
        raise RuntimeError(f"pdl_normalized_companies build failed: {error}")
    return {"feed": FEED, "rows_processed": int(rows), "distinct_ids": distinct_ids,
            "source_version": source_version, "source_rows": source_rows,
            "dataset_uri": DATASET_URI, "indices": built, "status": status}


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials")],
    timeout=60 * 90,
    memory=32768,
    cpu=8.0,
    ephemeral_disk=524288,
)
def reindex_normalized_companies() -> dict:
    """(Re)build the scalar indexes without re-projecting: stage the committed R2 sidecar to local
    disk, index locally (no R2 multipart), publish back via boto3. Idempotent (replace=True)."""
    import lance

    s3 = _s3_client()
    import shutil
    shutil.rmtree(LOCAL_DATASET, ignore_errors=True)
    n = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=DATASET_PREFIX):
        for o in page.get("Contents", []):
            rel = o["Key"][len(DATASET_PREFIX):]
            if not rel:
                continue
            lp = os.path.join(LOCAL_DATASET, rel.replace("/", os.sep))
            os.makedirs(os.path.dirname(lp), exist_ok=True)
            s3.download_file(BUCKET, o["Key"], lp)
            n += 1
    print(f"Staged {n} files from {DATASET_URI}")
    built = _create_indexes(LOCAL_DATASET)
    uploaded = _replace_r2_prefix(s3, DATASET_PREFIX, LOCAL_DATASET)
    print(f"Published {uploaded} files → {DATASET_URI}")
    return {"dataset": DATASET_URI, "built": built}


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 5)
def ledger(limit: int = 5) -> list:
    """Most-recent ops.pdl_normalized_runs rows."""
    import psycopg

    dsn = os.environ["HQX_DB_URL_POOLED"]
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT id, source_version, source_rows, rows_processed, distinct_ids, status, "
            "completed_at FROM ops.pdl_normalized_runs ORDER BY id DESC LIMIT %s",
            (limit,),
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


@app.local_entrypoint()
def initdb() -> None:
    """Create the ops ledger table (run once before the first build)."""
    import json

    print(json.dumps(init_schema.remote(), indent=2, default=str))


@app.local_entrypoint()
def run() -> None:
    """Build the sidecar → Lance (manual ops path; no Trigger callback)."""
    import json

    print(json.dumps(ingest_normalized_companies.remote(trigger_callback_url=None),
                     indent=2, default=str))


@app.local_entrypoint()
def reindex() -> None:
    """Rebuild the scalar indexes on the existing sidecar (no re-projection)."""
    import json

    print(json.dumps(reindex_normalized_companies.remote(), indent=2, default=str))


@app.local_entrypoint()
def show_ledger(limit: int = 5) -> None:
    """Print the most recent ops ledger rows."""
    import json

    print(json.dumps(ledger.remote(limit), indent=2, default=str))
