"""Compute worker — Colorado UCC filing-transaction ledger bulk ingest.

Part of the ``co-ucc-filings`` Modal app. NOT directly exposed — spawned by the
Universal Dispatcher (core/modal_dispatcher.py), the only proxy-authed endpoint in
the fleet. This worker has no web endpoint. Clean-room data plane: no Iceberg, no
Polaris — DuckDB does 100% of the transform, Lance is written straight to R2.

Grain (verified by structural probe over the full 2,555,824-row file): ONE row per
UCC filing TRANSACTION (event), keyed within the file by ``master_document_id``
(financing statement) → N transaction events (initial filing + amendments /
continuations / terminations). This file carries NO debtor / secured-party data —
those land as separate CO SOS CSV drops later and will join back on
``master_document_id`` (BTREE-indexed here precisely to prepare for that).

Data plane (single Modal invocation, full-snapshot overwrite):
    CO UCC bulk CSV (already landed in R2 by the operator; clean UTF-8, no transcode)
      → boto3 get to /tmp                       (Python: I/O only)
      → DuckDB read_csv(all_varchar) → 12-col project / cast   (100% in SQL)
      → Arrow table  → con.sql(...).to_arrow_table()
      → lance.write_dataset(s3://data-sink/active/co_ucc_transactions/, v2.1, overwrite)
      → BTREE on the 3 resolution keys + BITMAP on the 6 categoricals

Identifiers (transaction_id / master_document_id / file_id) are STRICT VARCHAR:
transaction_id and master_document_id carry an alphanumeric ``F`` era form
(e.g. ``2007F074276``) that would hard-fail a numeric cast; file_id is numeric but
kept VARCHAR for format safety. No VARIANT anywhere — every coercion is explicit.

Control plane (Trigger v4 durable callback): the worker accepts
``trigger_callback_url`` and, on terminal state (success OR failure), (1) writes the
run row to ``ops.co_ucc_runs`` via psycopg and (2) POSTs a FLAT JSON body
``{status, rows, feed, dataset_uri, snapshot_date}`` to that url to wake the
suspended Trigger run. No ``{"data": ...}`` envelope.

    modal run    pipelines/co_ucc/transactions_bulk.py::init_ops   # create ops.co_ucc_runs
    modal deploy pipelines/co_ucc/transactions_bulk.py             # make dispatcher-resolvable
    modal run    pipelines/co_ucc/transactions_bulk.py::run        # execute the ingest (manual)
    modal run    pipelines/co_ucc/transactions_bulk.py::reindex    # rebuild indexes only
"""

from __future__ import annotations

import os

import modal

BUCKET = "data-sink"
# The landed bulk file (operator-uploaded). Date stamp decodes to the snapshot date.
LANDING_KEY = (
    "landing/co_ucc/"
    "Uniform_Commercial_Code_(UCC)_Filing_Information_in_Colorado_20260531.csv"
)
# Lance system-of-record tier — transaction grain. Env-overridable.
DATASET_URI = os.environ.get(
    "CO_UCC_TRANSACTIONS_LANCE_URI", "s3://data-sink/active/co_ucc_transactions/"
)
SCRATCH_DIR = "/tmp"
FEED = "co_ucc_transactions"

# Lance fragment sizing (directive constraints).
#   max_rows_per_file = 1048576 — exact.
#   max_bytes_per_file: the directive wrote `90 * 10243` and annotated it "(90 GiB)".
#   The only reading that equals 90 GiB is `90 * 1024**3` (= 96,636,764,160), which
#   is also Lance's documented default. A literal `90 * 10243` (~900 KB) would
#   shatter this 2.56M-row set into hundreds of fragments — not the intent.
MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3
# Net-new dataset → pin the current Lance default (02_lancedb_storage.md §2.3).
DATA_STORAGE_VERSION = "2.1"

# Scalar index plan. BTREE for high-cardinality load-bearing resolution keys
# (equality + range); BITMAP for low-cardinality categoricals filtered frequently.
# Built inline post-write under LANCE_BYPASS_SPILLING=true (set on the image).
BTREE_INDEXES = [
    "master_document_id",  # financing-statement key → future debtor/secured-party joins
    "transaction_id",      # transaction (event) identity
    "file_id",             # near-unique per-record id / direct lookup
]
BITMAP_INDEXES = [
    "transaction_type",                # 4 values: Initial Filing / Amendment / ...
    "filing_type",                     # 10 lien-class tokens: ucc / lien_irs / ...
    "document_type",                   # 51 document categories
    "continuation",                    # boolean flag
    "termination_flag",                # boolean flag
    "manufactured_home_transactions",  # nullable boolean flag
]

# ── ops.co_ucc_runs DDL. Verbatim mirror of pipelines/co_ucc/ops_co_ucc_runs.sql
# (canonical copy). Applied by the `init_ops` entrypoint. Keep the two in sync. ──
OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.co_ucc_runs (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed           text        NOT NULL,
    dataset_uri    text        NOT NULL,
    snapshot_date  date,
    source_key     text,
    source_file    text,
    rows_processed bigint,
    status         text        NOT NULL,
    error          text,
    started_at     timestamptz,
    completed_at   timestamptz,
    recorded_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS co_ucc_runs_feed_idx        ON ops.co_ucc_runs (feed);
CREATE INDEX IF NOT EXISTS co_ucc_runs_status_idx      ON ops.co_ucc_runs (status);
CREATE INDEX IF NOT EXISTS co_ucc_runs_snapshot_idx    ON ops.co_ucc_runs (snapshot_date);
CREATE INDEX IF NOT EXISTS co_ucc_runs_recorded_at_idx ON ops.co_ucc_runs (recorded_at DESC);
"""

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",       # >=1.5 guarantees to_arrow_table; <2 stays below the v2.0 break
    "lancedb>=0.15",
    "pylance>=7",           # provides `import lance`; lancedb does not re-export it
    "pyarrow>=17",
    "boto3>=1.35",          # R2 landing read
    "requests>=2.32",       # Trigger callback
    "psycopg[binary]>=3.2",  # terminal state → ops.co_ucc_runs
).env(
    # BTREE index builds sort the column. Lance's spill-to-disk sorter under-sizes
    # its DataFusion pool and OOMs on high-cardinality string columns even in a
    # 16 GiB container (lance#2650). Force the in-memory sort path so every index
    # builds. Applies to the inline post-write index step.
    {"LANCE_BYPASS_SPILLING": "true"}
)

app = modal.App("co-ucc-filings", image=image)


# ── DuckDB projection. all_varchar read → defensive nullif/trim + TRY_CAST /
# TRY_STRPTIME. Source headers are camelCase; date wire format is %m/%d/%Y. The
# three identifier columns are strict VARCHAR. No bare VARIANT. ──────────────────
def _build_sql(csv_path: str, source_file: str, snapshot_date: str) -> str:
    """100% of the transform in one statement. Literals (our /tmp path, the basename,
    the derived snapshot date) are repo/derived-controlled — no user input — and
    single-quote-escaped regardless."""

    def lit(s: str) -> str:
        return s.replace("'", "''")

    return f"""
WITH raw AS (
    SELECT *
    FROM read_csv(
        '{lit(csv_path)}',
        all_varchar = true,
        header = true,
        sample_size = -1,
        ignore_errors = false
    )
)
SELECT
    nullif(trim("transactionId"), '')             AS transaction_id,
    nullif(trim("masterDocumentId"), '')          AS master_document_id,
    nullif(trim("fileId"), '')                    AS file_id,
    nullif(trim("transactionType"), '')           AS transaction_type,
    nullif(trim("documentType"), '')              AS document_type,
    nullif(trim("filingType"), '')                AS filing_type,
    nullif(trim("financialStatementType"), '')    AS financial_statement_type,
    TRY_CAST(TRY_STRPTIME(nullif(trim("filingDate"), ''), '%m/%d/%Y') AS DATE) AS filing_date,
    TRY_CAST(TRY_STRPTIME(nullif(trim("lapseDate"), ''),  '%m/%d/%Y') AS DATE) AS lapse_date,
    TRY_CAST(nullif(trim("continuation"), '')                 AS BOOLEAN) AS continuation,
    TRY_CAST(nullif(trim("terminationFlag"), '')              AS BOOLEAN) AS termination_flag,
    TRY_CAST(nullif(trim("manufacturedHomeTransactions"), '') AS BOOLEAN) AS manufactured_home_transactions,
    CAST('{lit(snapshot_date)}' AS DATE)          AS snapshot_date,
    '{lit(source_file)}'                          AS source_file,
    now()                                         AS ingested_at
FROM raw
"""


def _snapshot_date_from_key(key: str) -> str:
    """Decode the YYYYMMDD stamp in the filename → ISO date. Falls back to the
    current UTC date if the basename carries no stamp."""
    import datetime as dt
    import re

    base = key.rsplit("/", 1)[-1]
    m = re.search(r"(\d{8})\.csv$", base)
    if m:
        s = m.group(1)
        return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
    return dt.datetime.now(dt.timezone.utc).date().isoformat()


def _r2_storage_options() -> dict[str, str]:
    """object_store options for Cloudflare R2, sourced from the Modal secret.
    AWS-style creds + explicit endpoint and region ("auto" for R2)."""
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
    botocore's default flexible-checksum validation does not match R2's semantics
    and otherwise raises on get/put."""
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


def _download(s3, key: str, out_path: str) -> int:
    """Stream the landed CSV from R2 to scratch. RAW bytes — the file is clean
    UTF-8 (probe-verified: zero 0x80-0x9F bytes), so NO cp1252 transcode. I/O only."""
    s3.download_file(BUCKET, key, out_path)
    return os.path.getsize(out_path)


def _create_indexes(so: dict) -> list[str]:
    """Build the BTREE + BITMAP scalar indexes on the active dataset. create_scalar_index
    defaults to replace=True → idempotent. Returns the committed index names."""
    import lance

    ds = lance.dataset(DATASET_URI, storage_options=so)
    for col in BTREE_INDEXES:
        ds.create_scalar_index(col, index_type="BTREE")
        print(f"  BTREE  ✓ {col}")
    for col in BITMAP_INDEXES:
        ds.create_scalar_index(col, index_type="BITMAP")
        print(f"  BITMAP ✓ {col}")

    ds = lance.dataset(DATASET_URI, storage_options=so)  # reopen → read committed set
    names = []
    for ix in ds.list_indices():
        names.append(ix.get("name", str(ix)) if isinstance(ix, dict)
                     else getattr(ix, "name", str(ix)))
    return sorted(names)


def _record_run(dataset_uri, snapshot_date, source_key, source_file, rows,
                status, error, started_at, completed_at) -> None:
    """Terminal run row → ops.co_ucc_runs (psycopg). Best-effort: never let an
    audit-write failure crash an otherwise-good ingest."""
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* state write.")
        return
    source_file = source_key.rsplit("/", 1)[-1]
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops.co_ucc_runs
                    (feed, dataset_uri, snapshot_date, source_key, source_file,
                     rows_processed, status, error, started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (FEED, dataset_uri, snapshot_date, source_key, source_file,
                 rows, status, error, started_at, completed_at),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must not mask the ingest
        print(f"WARN: ops.* state write failed: {exc}")


def _post_callback(url, payload, attempts: int = 3) -> None:
    """POST terminal metadata to the Trigger waitpoint url. FLAT JSON body — no
    ``{"data": ...}`` envelope. The whole body becomes result.output."""
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


@app.function(
    secrets=[
        modal.Secret.from_name("r2-credentials"),
        modal.Secret.from_name("hqx-postgres"),
    ],
    timeout=60 * 60,
    memory=16384,
    cpu=4.0,
)
def ingest_co_ucc_transactions(
    source_key: str = LANDING_KEY,
    trigger_callback_url: str | None = None,
) -> dict:
    """Land the CO UCC CSV → DuckDB project/cast → Lance overwrite → BTREE+BITMAP
    index, then record ops.* state and wake the Trigger run. Re-raises on failure so
    the Modal call is marked failed."""
    import datetime as dt

    import duckdb
    import lance

    started_at = dt.datetime.now(dt.timezone.utc)
    source_file = source_key.rsplit("/", 1)[-1]
    snapshot_date = _snapshot_date_from_key(source_key)
    rows = 0
    status = "error"
    error: str | None = None
    indices: list[str] = []
    committed_rows = 0

    try:
        so = _r2_storage_options()
        s3 = _s3_client()

        raw = os.path.join(SCRATCH_DIR, source_file)
        nbytes = _download(s3, source_key, raw)
        print(f"Downloaded s3://{BUCKET}/{source_key} ({nbytes/(1024**2):.1f} MiB)")

        con = duckdb.connect(":memory:")
        try:
            con.execute("PRAGMA threads=4;")
            table = con.sql(_build_sql(raw, source_file, snapshot_date)).to_arrow_table()
        finally:
            con.close()
        rows = table.num_rows
        print(f"Transformed {rows:,} rows → Arrow ({table.num_columns} cols)")

        lance.write_dataset(
            table,
            DATASET_URI,
            mode="overwrite",
            data_storage_version=DATA_STORAGE_VERSION,
            max_rows_per_file=MAX_ROWS_PER_FILE,
            max_bytes_per_file=MAX_BYTES_PER_FILE,
            storage_options=so,
        )
        print(f"Wrote Lance dataset (overwrite, v{DATA_STORAGE_VERSION}) → {DATASET_URI}")

        indices = _create_indexes(so)
        committed_rows = lance.dataset(DATASET_URI, storage_options=so).count_rows()
        status = "success"
    except Exception as exc:  # noqa: BLE001 — terminal handling below + re-raise
        error = str(exc)
        status = "error"
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run(DATASET_URI, snapshot_date, source_key, source_file, int(rows),
                    status, error, started_at, completed_at)
        _post_callback(
            trigger_callback_url,
            {"status": status, "rows": int(rows), "feed": FEED,
             "dataset_uri": DATASET_URI, "snapshot_date": snapshot_date},
        )

    if status != "success":
        raise RuntimeError(f"co_ucc ingest failed for {source_key}: {error}")
    return {"feed": FEED, "rows_processed": int(rows),
            "committed_rows": int(committed_rows), "dataset_uri": DATASET_URI,
            "snapshot_date": snapshot_date, "indices": indices, "status": status}


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials")],
    timeout=60 * 45,
    memory=16384,
    cpu=4.0,
)
def reindex() -> dict:
    """(Re)build the scalar indexes on the already-written dataset without
    re-ingesting. Idempotent (create_scalar_index defaults replace=True)."""
    so = _r2_storage_options()
    indices = _create_indexes(so)
    import lance

    rows = lance.dataset(DATASET_URI, storage_options=so).count_rows()
    return {"dataset_uri": DATASET_URI, "rows": rows,
            "indices": indices, "index_count": len(indices)}


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 5)
def apply_ops_ddl() -> dict:
    """Create ops.co_ucc_runs (idempotent). Mirrors ops_co_ucc_runs.sql."""
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        raise RuntimeError("HQX_DB_URL_POOLED not set in the hqx-postgres secret.")
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(OPS_DDL)
        conn.commit()
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema='ops' AND table_name='co_ucc_runs'
            ORDER BY ordinal_position
        """)
        cols = [r[0] for r in cur.fetchall()]
    print(f"ops.co_ucc_runs ready — columns: {cols}")
    return {"table": "ops.co_ucc_runs", "columns": cols}


@app.local_entrypoint()
def init_ops() -> None:
    """Apply the ops.co_ucc_runs DDL."""
    print(apply_ops_ddl.remote())


@app.local_entrypoint()
def run(source_key: str = LANDING_KEY) -> None:
    """Manual ingest (no Trigger callback). ops.* write still fires."""
    import json

    print(json.dumps(
        ingest_co_ucc_transactions.remote(source_key=source_key, trigger_callback_url=None),
        indent=2, default=str,
    ))


@app.local_entrypoint()
def reindex_only() -> None:
    """Rebuild indexes on the existing dataset (no re-ingest)."""
    import json

    print(json.dumps(reindex.remote(), indent=2, default=str))
