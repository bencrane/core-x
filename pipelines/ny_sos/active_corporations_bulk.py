"""Compute worker — NY Department of State active corporations bulk ingest.

Part of the ``ny-sos-corporations`` Modal app. Endpoint-less; spawned by the
Universal Dispatcher (core/modal_dispatcher.py) or driven by the local
entrypoints. Snapshot ingest of the NY SoS "Active Corporations: Beginning 1800"
bulk extract — a complete point-in-time export of every active corporation, LLC,
and limited partnership registered in NY (1800 → present).

Topology: ``mode="overwrite"`` — the file is a full snapshot, so the dataset is
rebuilt wholesale each refresh and Lance's immutable-version MVCC retains prior
snapshots for month-over-month time travel. An append-only structure is rejected
(it would duplicate ~4.2M near-identical rows per refresh for no gain).

Encoding: this feed is GENUINE UTF-8 (verified by a full-file byte scan — zero
invalid sequences across 843 MB; the 2,595 high bytes are legitimate multibyte
continuations). Unlike the SBA/SAM cp1252 feeds there is NO transcode; a cp1252
pass here would corrupt valid multibyte sequences. Python streams the landed
bytes to /tmp untouched; DuckDB does 100% of the transform.

Identifiers (strict VARCHAR, verified): ``dos_id`` is a perfect unique key
(4,219,360 distinct / 4,219,360 rows, 0 null, 0 non-numeric) and every ``*_zip``
carries leading zeros (78,074 leading-zero ZIPs in the process block alone). A
numeric cast would corrupt both — all stay VARCHAR.

Refresh: MANUAL R2 drops only. The worker reads the already-landed key; there is
NO upstream fetch phase until the Socrata export URL is verified.

    modal deploy pipelines/ny_sos/active_corporations_bulk.py
    modal run    pipelines/ny_sos/active_corporations_bulk.py::initdb       # create ops.* table
    modal run    pipelines/ny_sos/active_corporations_bulk.py::run          # ingest landed snapshot → Lance
    modal run    pipelines/ny_sos/active_corporations_bulk.py::reindex      # rebuild scalar indexes only
    modal run    pipelines/ny_sos/active_corporations_bulk.py::show_ledger  # print ops ledger
"""

from __future__ import annotations

import os

import modal

BUCKET = "data-sink"
# Manual landing key (NO fetch phase — manual R2 drops only, per directive).
LANDING_KEY = "landing/ny_sos/NY_Active_Corporations___Beginning_1800.csv"
# Lance system-of-record tier (NOT the landing/raw zone). Env-overridable.
DATASET_URI = os.environ.get("NY_SOS_LANCE_URI", "s3://data-sink/active/ny_sos/")
SCRATCH_DIR = "/tmp"
FEED = "ny_sos_corporations"

# Lance fragment sizing (directive constraints).
#   max_rows_per_file = 1048576 — exact.
#   max_bytes_per_file: the directive wrote `90 * 10243` annotated "(90 GiB)".
#   The only reading that equals 90 GiB is `90 * 1024**3` (= 96,636,764,160) —
#   Lance's documented default and the constant used by the existing PPP / FOIA
#   workers. A literal `90 * 10243` (~900 KB) would shatter the 4.2M-row snapshot
#   into thousands of fragments. Honor the stated "90 GiB".
MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3
# Net-new dataset → pin the current Lance default (per 02_lancedb_storage.md §2.3).
DATA_STORAGE_VERSION = "2.1"

# Scalar index plan (approved). BTREE for high-cardinality load-bearing
# resolution keys (equality + range predicates); BITMAP for low-cardinality
# categoricals filtered frequently. Built inline after the overwrite write.
NY_SOS_BTREE_INDEXES = [
    "dos_id",                   # unique NY entity key (4.22M distinct)
    "current_entity_name",      # name resolution (4.22M distinct)
    "initial_dos_filing_date",  # "formed since X" range scans
    "dos_process_name",         # process/agent resolution (~3.18M distinct)
    "registered_agent_name",    # agent resolution (~370k distinct)
]
NY_SOS_BITMAP_INDEXES = [
    "entity_type",        # 54 distinct
    "county",             # 63 distinct
    "jurisdiction",       # 81 distinct
    "dos_process_state",  # 63 distinct
]

# 30 source columns in CSV header order → snake_case. ONE date; everything else
# VARCHAR. dos_id + every *_zip stay VARCHAR (leading-zero / format-drift
# safety). No VARIANT. kind ∈ {"str", "date"}.
_COLUMNS: list[tuple[str, str, str]] = [
    ("DOS ID", "dos_id", "str"),
    ("Current Entity Name", "current_entity_name", "str"),
    ("Initial DOS Filing Date", "initial_dos_filing_date", "date"),
    ("County", "county", "str"),
    ("Jurisdiction", "jurisdiction", "str"),
    ("Entity Type", "entity_type", "str"),
    ("DOS Process Name", "dos_process_name", "str"),
    ("DOS Process Address 1", "dos_process_address_1", "str"),
    ("DOS Process Address 2", "dos_process_address_2", "str"),
    ("DOS Process City", "dos_process_city", "str"),
    ("DOS Process State", "dos_process_state", "str"),
    ("DOS Process Zip", "dos_process_zip", "str"),
    ("CEO Name", "ceo_name", "str"),
    ("CEO Address 1", "ceo_address_1", "str"),
    ("CEO Address 2", "ceo_address_2", "str"),
    ("CEO City", "ceo_city", "str"),
    ("CEO State", "ceo_state", "str"),
    ("CEO Zip", "ceo_zip", "str"),
    ("Registered Agent Name", "registered_agent_name", "str"),
    ("Registered Agent Address 1", "registered_agent_address_1", "str"),
    ("Registered Agent Address 2", "registered_agent_address_2", "str"),
    ("Registered Agent City", "registered_agent_city", "str"),
    ("Registered Agent State", "registered_agent_state", "str"),
    ("Registered Agent Zip", "registered_agent_zip", "str"),
    ("Location Name", "location_name", "str"),
    ("Location Address 1", "location_address_1", "str"),
    ("Location Address 2", "location_address_2", "str"),
    ("Location City", "location_city", "str"),
    ("Location State", "location_state", "str"),
    ("Location Zip", "location_zip", "str"),
]

# Terminal-state ledger. CREATE SCHEMA + CREATE TABLE run as SEPARATE statements
# (psycopg sends one command per execute()).
_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS ops.ny_sos_corporation_runs (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed            TEXT        NOT NULL,
    dataset_uri     TEXT        NOT NULL,
    source_file     TEXT        NOT NULL,
    landing_key     TEXT        NOT NULL,
    snapshot_date   DATE,
    rows_processed  BIGINT,
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
    "boto3>=1.35",          # R2 landing read
    "requests>=2.32",       # Trigger waitpoint callback
    "psycopg[binary]>=3.2",  # ops.* terminal state
).env(
    # BTREE training sorts the column; bypass Lance's bounded spill-to-disk
    # ExternalSorter, whose memory accounting under-sizes the pool and exhausts
    # on multi-million-row string columns (current_entity_name 4.22M,
    # dos_process_name 3.18M) even in a 16 GiB container. In-memory sort is well
    # within RAM here. See lance-format/lance#2650.
    {"LANCE_BYPASS_SPILLING": "true"}
)

app = modal.App("ny-sos-corporations", image=image)


def _r2_storage_options() -> dict[str, str]:
    """object_store options for Cloudflare R2, sourced from the Modal secret.
    AWS-style creds + explicit endpoint + region 'auto'. Endpoint supplied
    directly (R2_ENDPOINT) or derived from R2_ACCOUNT_ID."""
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


def _build_sql(csv_path: str, source_file: str, snapshot_date: str) -> str:
    """100% of the transform. read_csv(all_varchar=true) → nullif/trim on every
    string + a single defensive TRY_STRPTIME date cast → snake_case projection.
    All interpolated values are repo-controlled (our /tmp path, the registry
    filename, fixed literals) — no user input, no injection surface — and
    single-quote-escaped regardless. NO cp1252 transcode (source is UTF-8)."""

    def lit(s: str) -> str:
        return s.replace("'", "''")

    def strcol(src: str, dst: str) -> str:
        return f'nullif(trim("{src}"), \'\') AS {dst}'

    def datecol(src: str, dst: str) -> str:
        # Wire format MM/DD/YYYY (verified: 0 parse failures, span 1800→present).
        return (
            f"TRY_CAST(TRY_STRPTIME(nullif(trim(\"{src}\"), ''), '%m/%d/%Y') AS DATE) AS {dst}"
        )

    projections = [
        datecol(src, dst) if kind == "date" else strcol(src, dst)
        for src, dst, kind in _COLUMNS
    ]
    projection_sql = ",\n    ".join(projections)
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
    {projection_sql},
    '{lit(source_file)}' AS source_file,
    CAST('{lit(snapshot_date)}' AS DATE) AS snapshot_date,
    now() AS ingested_at
FROM raw
"""


def _create_indexes(so: dict) -> list[str]:
    """Build the BTREE + BITMAP scalar indexes on the active dataset.
    replace=True → idempotent rebuild. Per-column try/except: an index miss must
    not fail an otherwise-good load (the data is the critical path)."""
    import lance

    ds = lance.dataset(DATASET_URI, storage_options=so)
    built: list[str] = []
    for col in NY_SOS_BTREE_INDEXES:
        try:
            ds.create_scalar_index(col, "BTREE", replace=True)
            built.append(f"BTREE:{col}")
            print(f"  BTREE  ✓ {col}")
        except Exception as exc:  # noqa: BLE001 — an index miss must not fail the load
            print(f"  WARN: BTREE index on {col} failed: {exc}")
    for col in NY_SOS_BITMAP_INDEXES:
        try:
            ds.create_scalar_index(col, "BITMAP", replace=True)
            built.append(f"BITMAP:{col}")
            print(f"  BITMAP ✓ {col}")
        except Exception as exc:  # noqa: BLE001
            print(f"  WARN: BITMAP index on {col} failed: {exc}")
    return built


def _list_committed_indices(ds) -> list:
    """Best-effort read of committed scalar indices (name/type/fields). Tolerant
    of pylance return-shape drift (dict vs object, list_indices vs list_indexes)."""
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


def _record_run(dataset_uri, source_file, landing_key, snapshot_date, rows,
                status, error, started_at, completed_at) -> None:
    """Terminal run row → ops.ny_sos_corporation_runs (psycopg). Best-effort:
    never let an audit-write failure crash an otherwise-good ingest."""
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* write.")
        return
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops.ny_sos_corporation_runs
                    (feed, dataset_uri, source_file, landing_key, snapshot_date,
                     rows_processed, status, error, started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (FEED, dataset_uri, source_file, landing_key, snapshot_date,
                 rows, status, error, started_at, completed_at),
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
    """Create the ops schema + ny_sos_corporation_runs ledger (idempotent)."""
    import psycopg

    dsn = os.environ["HQX_DB_URL_POOLED"]
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS ops;")
        cur.execute(_CREATE_TABLE_SQL)
        conn.commit()
    print("ops.ny_sos_corporation_runs ready")
    return {"status": "ok", "table": "ops.ny_sos_corporation_runs"}


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 90,
    memory=16384,
    cpu=4.0,
)
def ingest_ny_sos(key: str = LANDING_KEY, trigger_callback_url: str | None = None) -> dict:
    """Download landed CSV (UTF-8, NO transcode) → DuckDB project/cast → Arrow →
    Lance overwrite → BTREE/BITMAP index, then record ops.* state and wake the
    Trigger run. Re-raises on failure so the Modal call is marked failed."""
    import datetime as dt
    import os.path

    import duckdb
    import lance

    started_at = dt.datetime.now(dt.timezone.utc)
    snapshot_date = started_at.date().isoformat()
    source_file = key.rsplit("/", 1)[-1]
    rows = 0
    status = "error"
    error: str | None = None
    built: list[str] = []

    try:
        so = _r2_storage_options()
        s3 = _s3_client()
        scratch = os.path.join(SCRATCH_DIR, source_file)
        # I/O ONLY — UTF-8 bytes straight to scratch, NO transcode.
        s3.download_file(BUCKET, key, scratch)

        con = duckdb.connect(":memory:")
        try:
            con.execute("PRAGMA threads=4;")
            con.execute("SET enable_progress_bar=false;")
            table = con.sql(_build_sql(scratch, source_file, snapshot_date)).to_arrow_table()
        finally:
            con.close()
        rows = table.num_rows

        lance.write_dataset(
            table,
            DATASET_URI,
            mode="overwrite",
            data_storage_version=DATA_STORAGE_VERSION,
            max_rows_per_file=MAX_ROWS_PER_FILE,
            max_bytes_per_file=MAX_BYTES_PER_FILE,
            storage_options=so,
        )
        built = _create_indexes(so)
        status = "success"
    except Exception as exc:  # noqa: BLE001 — terminal handling below + re-raise
        error = str(exc)
        status = "error"
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run(DATASET_URI, source_file, key, snapshot_date, int(rows),
                    status, error, started_at, completed_at)
        _post_callback(
            trigger_callback_url,
            {"status": status, "rows": int(rows), "feed": FEED,
             "dataset_uri": DATASET_URI, "snapshot_date": snapshot_date},
        )

    if status != "success":
        raise RuntimeError(f"ny_sos ingest failed for {key}: {error}")
    return {"feed": FEED, "source_file": source_file, "rows_processed": int(rows),
            "dataset_uri": DATASET_URI, "snapshot_date": snapshot_date,
            "indices": built, "status": status}


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials")],
    timeout=60 * 60,
    memory=16384,
    cpu=4.0,
)
def reindex_ny_sos() -> dict:
    """(Re)build the scalar indexes on the already-written dataset without
    re-ingesting. Idempotent (replace=True)."""
    import lance

    so = _r2_storage_options()
    built = _create_indexes(so)
    ds = lance.dataset(DATASET_URI, storage_options=so)  # reopen → committed set
    committed = _list_committed_indices(ds)
    print(f"Committed indices: {committed}")
    return {"dataset": DATASET_URI, "built": built, "committed_indices": committed}


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 5)
def ledger(limit: int = 5) -> list:
    """Read the most recent ops.ny_sos_corporation_runs rows."""
    import psycopg

    dsn = os.environ["HQX_DB_URL_POOLED"]
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT id, feed, dataset_uri, source_file, snapshot_date, rows_processed, "
            "status, error, started_at, completed_at "
            "FROM ops.ny_sos_corporation_runs ORDER BY id DESC LIMIT %s",
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

    print(json.dumps(ingest_ny_sos.remote(key, trigger_callback_url=None), indent=2, default=str))


@app.local_entrypoint()
def reindex() -> None:
    """Rebuild the scalar indexes on the existing dataset (no re-ingest)."""
    import json

    print(json.dumps(reindex_ny_sos.remote(), indent=2, default=str))


@app.local_entrypoint()
def show_ledger(limit: int = 5) -> None:
    """Print the most recent ops ledger rows."""
    import json

    print(json.dumps(ledger.remote(limit), indent=2, default=str))
