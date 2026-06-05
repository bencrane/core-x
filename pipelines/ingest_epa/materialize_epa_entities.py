"""Compute worker — EPA CAA (Air) & RCRA (HazWaste) legal-entity materialization.

Closes the orphaned-name gap diagnosed in
``docs/reference/EPA_CAA_RCRA_LEGAL_ENTITY_DIAGNOSTIC.md``: the air-facility and RCRA-handler
legal/operating names live only inside the raw landing ZIPs (the flattened ``epa_pipeline_caa``
/ ``epa_pipeline_rcra`` carry an enforcement-slice name at best, none at worst). This worker
materializes the two facility/handler MASTERS as standing, ``REGISTRY_ID``-keyed Lance nodes —
exactly parallel to ``epa_permits`` / ``epa_defendants`` (materialize_epa.py).

Part of the ``epa-entity-pipelines`` Modal app. NOT directly exposed — spawned by the Universal
Dispatcher (core/modal_dispatcher.py) or run manually via ``modal run``. No web endpoint.

Datasets (2):
  epa_air_facilities  ← ICIS-AIR_downloads.zip::ICIS-AIR_FACILITIES.csv  (REGISTRY_ID inline)
                        grain 1 row / PGM_SYS_ID · BTREE REGISTRY_ID, PGM_SYS_ID
  epa_rcra_handlers   ← rcra_downloads.zip::RCRA_FACILITIES.csv ⋈ epa_program_links(RCRAINFO)
                        grain 1 row / HANDLER_ID · BTREE REGISTRY_ID, HANDLER_ID

Both: clean-room data plane — boto3 random-access extract of ONE member (central-directory
seek), raw-deflate→gzip rewrap to /tmp (only the COMPRESSED member touches disk), DuckDB
read_csv(all_varchar=true) does 100% of the transform, fetch→ZSTD-parquet→STREAM to
lance.write_dataset(s3://data-sink/active/<name>/, data_storage_version="2.1"), then
create_scalar_index BTREE/BITMAP direct-to-R2. Verbatim passthrough of every source column
(geographic + operational-status); only the two RCRA geocodes are typed DOUBLE.

Control plane (Trigger v4 durable callback): the orchestrator accepts ``trigger_callback_url``
and, on terminal state, (1) writes run rows to ``ops.epa_ingest_runs`` via psycopg and (2) POSTs
``{status, rows, feed}`` to the waitpoint URL.

    modal run    pipelines/ingest_epa/materialize_epa_entities.py::init     # ops table (shared)
    modal run    pipelines/ingest_epa/materialize_epa_entities.py::air      # epa_air_facilities
    modal run    pipelines/ingest_epa/materialize_epa_entities.py::rcra     # epa_rcra_handlers
    modal run    pipelines/ingest_epa/materialize_epa_entities.py::run      # both
    modal run    pipelines/ingest_epa/materialize_epa_entities.py::verify   # read-back proof
    modal deploy pipelines/ingest_epa/materialize_epa_entities.py           # dispatcher-resolvable
"""

from __future__ import annotations

import os

import modal

# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #
LANDING_BUCKET = "data-sink"
LANDING_PREFIX = "landing/epa/"
ACTIVE_BASE = os.environ.get("EPA_ACTIVE_BASE", "s3://data-sink/active/")
PROGRAM_LINKS_URI = os.environ.get(
    "EPA_PROGRAM_LINKS_URI", "s3://data-sink/active/epa_program_links/"
)
FEED = "epa_legal_entities"

DATA_STORAGE_VERSION = "2.1"          # net-new datasets → EPA-family Lance default
MAX_ROWS_PER_FILE = 1_048_576
PARQUET_ROW_GROUP = 1_048_576         # transport-parquet row-group size (bounded COPY memory)
LANCE_READ_BATCH = 100_000            # parquet→Lance streaming batch
DUCKDB_THREADS = 4
DUCKDB_MEMORY_LIMIT = "24GB"
SPILL_DIR = "/tmp/duckdb_spill"
SCRATCH_DIR = "/tmp/epa_entities"

image = (
    modal.Image.debian_slim(python_version="3.12")
    .pip_install(
        "duckdb>=1.5,<2",
        "lancedb>=0.15",
        "pylance>=7",            # provides `import lance`
        "pyarrow>=17",
        "boto3>=1.34",           # random-access R2 range reads
        "requests>=2.32",        # Trigger callback
        "psycopg[binary]>=3.2",  # terminal state → ops.epa_ingest_runs
    )
    .env({"LANCE_BYPASS_SPILLING": "true"})  # in-memory BTREE sort for high-card string keys
)

app = modal.App("epa-entity-pipelines", image=image)


# --------------------------------------------------------------------------- #
# ops.epa_ingest_runs — terminal-state ledger (shared with materialize_epa.py; idempotent DDL)
# --------------------------------------------------------------------------- #
OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.epa_ingest_runs (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id          text        NOT NULL,
    feed            text        NOT NULL,
    dataset         text        NOT NULL,
    dataset_uri     text,
    source_archives text,
    rows_written    bigint,
    indexes_built   text,
    status          text        NOT NULL,
    error           text,
    metrics         jsonb,
    started_at      timestamptz,
    completed_at    timestamptz,
    recorded_at     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS epa_ingest_runs_run_idx      ON ops.epa_ingest_runs (run_id);
CREATE INDEX IF NOT EXISTS epa_ingest_runs_dataset_idx  ON ops.epa_ingest_runs (dataset);
CREATE INDEX IF NOT EXISTS epa_ingest_runs_status_idx   ON ops.epa_ingest_runs (status);
CREATE INDEX IF NOT EXISTS epa_ingest_runs_recorded_idx ON ops.epa_ingest_runs (recorded_at DESC);
"""


# --------------------------------------------------------------------------- #
# SQL builders — importable & pure (verified verbatim by the local dry-run harness)
# --------------------------------------------------------------------------- #
def read_csv_expr(gz_token: str) -> str:
    # parallel=false: EPA bulk CSVs are non-splittable gzip and some carry embedded newlines in
    # quoted free-text; the single-threaded reader is the safe, zero-cost choice on gzip.
    return (
        f"read_csv('{gz_token}', all_varchar=true, header=true, parallel=false, "
        "compression='gzip', quote='\"', escape='\"', strict_mode=false, ignore_errors=false)"
    )


def _t(c: str) -> str:                      # trimmed, '' → NULL
    return f"nullif(trim({c}),'')"


def air_select(gz_token: str) -> str:
    """epa_air_facilities — verbatim passthrough of all 19 ICIS-AIR_FACILITIES columns.
    REGISTRY_ID is inline (direct hub bind); filter to non-null REGISTRY_ID (drops ~0.10%).
    Grain is one row per PGM_SYS_ID (source is already unique on it — no dedup, strictly lossless)."""
    return f"""
SELECT
    {_t('REGISTRY_ID')}               AS REGISTRY_ID,
    {_t('PGM_SYS_ID')}                AS PGM_SYS_ID,
    {_t('FACILITY_NAME')}             AS FACILITY_NAME,
    {_t('STREET_ADDRESS')}            AS STREET_ADDRESS,
    {_t('CITY')}                      AS CITY,
    {_t('COUNTY_NAME')}               AS COUNTY_NAME,
    {_t('STATE')}                     AS STATE,
    {_t('ZIP_CODE')}                  AS ZIP_CODE,
    {_t('EPA_REGION')}                AS EPA_REGION,
    {_t('SIC_CODES')}                 AS SIC_CODES,
    {_t('NAICS_CODES')}               AS NAICS_CODES,
    {_t('FACILITY_TYPE_CODE')}        AS FACILITY_TYPE_CODE,
    {_t('AIR_POLLUTANT_CLASS_CODE')}  AS AIR_POLLUTANT_CLASS_CODE,
    {_t('AIR_POLLUTANT_CLASS_DESC')}  AS AIR_POLLUTANT_CLASS_DESC,
    {_t('AIR_OPERATING_STATUS_CODE')} AS AIR_OPERATING_STATUS_CODE,
    {_t('AIR_OPERATING_STATUS_DESC')} AS AIR_OPERATING_STATUS_DESC,
    {_t('CURRENT_HPV')}               AS CURRENT_HPV,
    {_t('LOCAL_CONTROL_REGION_CODE')} AS LOCAL_CONTROL_REGION_CODE,
    {_t('LOCAL_CONTROL_REGION_NAME')} AS LOCAL_CONTROL_REGION_NAME
FROM {read_csv_expr(gz_token)}
WHERE {_t('REGISTRY_ID')} IS NOT NULL
"""


def pl_rcra_temp_sql(reader_relation: str) -> str:
    """RCRAINFO blocking set from epa_program_links, deduped to one REGISTRY_ID per PGM_SYS_ID
    (deterministic). Source is already 1:1; the QUALIFY guards against future drift without
    dropping any currently-resolvable handler."""
    return f"""
CREATE TEMP TABLE pl_rcra AS
SELECT {_t('PGM_SYS_ID')} AS pgm, {_t('REGISTRY_ID')} AS rid
FROM {reader_relation}
WHERE PGM_SYS_ACRNM = 'RCRAINFO'
  AND {_t('PGM_SYS_ID')} IS NOT NULL
  AND {_t('REGISTRY_ID')} IS NOT NULL
QUALIFY row_number() OVER (PARTITION BY {_t('PGM_SYS_ID')} ORDER BY {_t('REGISTRY_ID')}) = 1
"""


def rcra_select(gz_token: str) -> str:
    """epa_rcra_handlers — verbatim passthrough of all 15 RCRA_FACILITIES columns + the resolved
    REGISTRY_ID. ID_NUMBER is emitted as HANDLER_ID (its canonical EPA name; same value). INNER
    JOIN to pl_rcra binds the hub key and drops the ~1.20% unlinked handlers. Source ID_NUMBER is
    unique and pl_rcra is unique on pgm → exactly one row per HANDLER_ID (strictly lossless).
    Only the two geocodes are typed DOUBLE; every other column is verbatim text."""
    return f"""
SELECT
    pl.rid                            AS REGISTRY_ID,
    {_t('r.ID_NUMBER')}               AS HANDLER_ID,
    {_t('r.FACILITY_NAME')}           AS FACILITY_NAME,
    {_t('r.ACTIVITY_LOCATION')}       AS ACTIVITY_LOCATION,
    {_t('r.FULL_ENFORCEMENT')}        AS FULL_ENFORCEMENT,
    {_t('r.HREPORT_UNIVERSE_RECORD')} AS HREPORT_UNIVERSE_RECORD,
    {_t('r.STREET_ADDRESS')}          AS STREET_ADDRESS,
    {_t('r.CITY_NAME')}               AS CITY_NAME,
    {_t('r.STATE_CODE')}              AS STATE_CODE,
    {_t('r.ZIP_CODE')}                AS ZIP_CODE,
    TRY_CAST({_t('r.LATITUDE83')}  AS DOUBLE) AS LATITUDE83,
    TRY_CAST({_t('r.LONGITUDE83')} AS DOUBLE) AS LONGITUDE83,
    {_t('r.FED_WASTE_GENERATOR')}     AS FED_WASTE_GENERATOR,
    {_t('r.TRANSPORTER')}             AS TRANSPORTER,
    {_t('r.ACTIVE_SITE')}             AS ACTIVE_SITE,
    {_t('r.OPERATING_TSDF')}          AS OPERATING_TSDF
FROM {read_csv_expr(gz_token)} r
JOIN pl_rcra pl ON pl.pgm = {_t('r.ID_NUMBER')}
"""


# Per-dataset spec (source + index plan). SQL is built inline (air is single-member; rcra joins
# the live program_links Lance dataset, so it needs a registered reader first).
SPECS = {
    "epa_air_facilities": dict(
        archive="ICIS-AIR_downloads.zip", member="ICIS-AIR_FACILITIES.csv", key="PGM_SYS_ID",
        btree=["REGISTRY_ID", "PGM_SYS_ID"],
        bitmap=["STATE", "AIR_OPERATING_STATUS_CODE", "CURRENT_HPV", "AIR_POLLUTANT_CLASS_CODE"],
    ),
    "epa_rcra_handlers": dict(
        archive="rcra_downloads.zip", member="RCRA_FACILITIES.csv", key="HANDLER_ID",
        btree=["REGISTRY_ID", "HANDLER_ID"],
        bitmap=["STATE_CODE", "ACTIVE_SITE", "OPERATING_TSDF", "FED_WASTE_GENERATOR"],
    ),
}


# --------------------------------------------------------------------------- #
# R2 / S3 helpers  (verbatim from materialize_epa.py — fleet-consistent)
# --------------------------------------------------------------------------- #
def _r2_storage_options() -> dict[str, str]:
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
    import boto3
    from botocore.config import Config

    so = _r2_storage_options()
    # botocore >=1.36 validates a flexible (CRC32) checksum on GetObject by default; on a RANGE
    # GET it compares partial bytes against the full-object checksum header R2 returns → spurious
    # mismatch. Scope checksums to "when_required" so range reads succeed.
    return boto3.client(
        "s3",
        endpoint_url=so["endpoint"],
        aws_access_key_id=so["aws_access_key_id"],
        aws_secret_access_key=so["aws_secret_access_key"],
        region_name="auto",
        config=Config(
            signature_version="s3v4",
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
            retries={"max_attempts": 5, "mode": "standard"},
        ),
    )


class _S3RangeReader:
    """Minimal seekable file-like over an R2 object via boto3 Range GETs — enough for zipfile to
    parse the central directory and locate a member."""

    def __init__(self, client, bucket: str, key: str):
        self.c, self.b, self.k = client, bucket, key
        self._pos = 0
        self._size = client.head_object(Bucket=bucket, Key=key)["ContentLength"]

    def seekable(self) -> bool:
        return True

    def readable(self) -> bool:
        return True

    def tell(self) -> int:
        return self._pos

    def seek(self, off: int, whence: int = 0) -> int:
        self._pos = off if whence == 0 else (self._pos + off if whence == 1 else self._size + off)
        return self._pos

    def read(self, n: int = -1) -> bytes:
        if n is None or n < 0:
            n = self._size - self._pos
        if n <= 0:
            return b""
        end = min(self._pos + n, self._size) - 1
        if end < self._pos:
            return b""
        body = self.c.get_object(Bucket=self.b, Key=self.k, Range=f"bytes={self._pos}-{end}")["Body"].read()
        self._pos += len(body)
        return body


def _member_to_gz(archive: str, member: str, out_path: str) -> tuple[int, int]:
    """Random-access extract one ZIP member from R2 and rewrap its raw deflate stream as a valid
    .csv.gz WITHOUT decompressing/recompressing. /tmp holds only the COMPRESSED member; ZIP64
    offsets are resolved by `zipfile`. Returns (uncompressed_size, compress_size)."""
    import struct
    import zipfile

    key = LANDING_PREFIX + archive
    client = _s3_client()
    reader = _S3RangeReader(client, LANDING_BUCKET, key)
    zf = zipfile.ZipFile(reader)
    zi = zf.getinfo(member)
    if zi.compress_type != zipfile.ZIP_DEFLATED:
        raise RuntimeError(f"{archive}:{member} compress_type={zi.compress_type} (expected deflate)")

    reader.seek(zi.header_offset)
    lh = reader.read(30)
    if lh[:4] != b"PK\x03\x04":
        raise RuntimeError(f"{archive}:{member} bad local header signature")
    name_len = struct.unpack("<H", lh[26:28])[0]
    extra_len = struct.unpack("<H", lh[28:30])[0]
    data_off = zi.header_offset + 30 + name_len + extra_len

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "wb") as out:
        out.write(b"\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff")  # gzip header (deflate, no flags)
        remaining = zi.compress_size
        pos = data_off
        chunk = 16 << 20
        while remaining > 0:
            end = pos + min(chunk, remaining) - 1
            body = client.get_object(Bucket=LANDING_BUCKET, Key=key, Range=f"bytes={pos}-{end}")["Body"].read()
            if not body:
                raise RuntimeError(f"{archive}:{member} short read at {pos}")
            out.write(body)
            pos += len(body)
            remaining -= len(body)
        out.write(struct.pack("<II", zi.CRC & 0xFFFFFFFF, zi.file_size & 0xFFFFFFFF))  # gzip trailer
    return zi.file_size, zi.compress_size


# --------------------------------------------------------------------------- #
# DuckDB / Lance
# --------------------------------------------------------------------------- #
def _new_con():
    import duckdb

    con = duckdb.connect(":memory:")
    con.execute(f"SET threads TO {DUCKDB_THREADS}")
    con.execute(f"SET memory_limit='{DUCKDB_MEMORY_LIMIT}'")
    con.execute(f"SET temp_directory='{SPILL_DIR}'")
    con.execute("SET preserve_insertion_order=false")
    return con


def _build_indexes(uri: str, btree: list[str], bitmap: list[str], so: dict) -> list[str]:
    import lance

    ds = lance.dataset(uri, storage_options=so)
    built = []
    for col in btree:
        ds.create_scalar_index(col, index_type="BTREE", replace=True)
        built.append(f"{col}:BTREE")
        print(f"  BTREE ✓ {col}")
    for col in bitmap:
        ds.create_scalar_index(col, index_type="BITMAP", replace=True)
        built.append(f"{col}:BITMAP")
        print(f"  BITMAP ✓ {col}")
    return built


def _sql_to_lance(con, final_sql: str, uri: str, so: dict) -> int:
    """DuckDB final SELECT → ZSTD parquet (streaming COPY sink, memory bounded by row-group) →
    stream parquet → Lance overwrite. Returns committed row count."""
    import lance
    import pyarrow.dataset as pds

    os.makedirs(SCRATCH_DIR, exist_ok=True)
    pq = f"{SCRATCH_DIR}/_emit.parquet"
    con.execute(
        f"COPY ({final_sql}) TO '{pq}' "
        f"(FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE {PARQUET_ROW_GROUP})"
    )
    reader = pds.dataset(pq).scanner(batch_size=LANCE_READ_BATCH).to_reader()
    lance.write_dataset(
        reader, uri, mode="overwrite",
        data_storage_version=DATA_STORAGE_VERSION,
        max_rows_per_file=MAX_ROWS_PER_FILE, storage_options=so,
    )
    try:
        os.remove(pq)
    except OSError:
        pass
    return lance.dataset(uri, storage_options=so).count_rows()


# --------------------------------------------------------------------------- #
# ops ledger + Trigger callback  (verbatim pattern from materialize_epa.py)
# --------------------------------------------------------------------------- #
def _pg_connect():
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* write.")
        return None
    return psycopg.connect(dsn)


def _record_run(*, run_id, dataset, dataset_uri, source_archives, rows_written, indexes_built,
                status, error, metrics, started_at, completed_at) -> None:
    import json  # noqa: F401

    from psycopg.types.json import Jsonb

    conn = _pg_connect()
    if conn is None:
        return
    try:
        with conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
            cur.execute(
                """
                INSERT INTO ops.epa_ingest_runs
                    (run_id, feed, dataset, dataset_uri, source_archives, rows_written,
                     indexes_built, status, error, metrics, started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (run_id, FEED, dataset, dataset_uri, source_archives, rows_written,
                 indexes_built, status, error,
                 Jsonb(metrics) if metrics is not None else None, started_at, completed_at),
            )
    except Exception as exc:  # noqa: BLE001 — audit must not mask the ingest
        print(f"WARN: ops.epa_ingest_runs write failed: {exc}")
    finally:
        conn.close()


def _post_callback(url, payload, attempts: int = 3) -> None:
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


# --------------------------------------------------------------------------- #
# Builder: epa_air_facilities (single-member, REGISTRY_ID inline → direct)
# --------------------------------------------------------------------------- #
@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 45,
    memory=16384,
    cpu=4.0,
)
def build_air(run_id: str) -> dict:
    import datetime as dt

    name = "epa_air_facilities"
    spec = SPECS[name]
    uri = ACTIVE_BASE + name + "/"
    so = _r2_storage_options()
    archives = spec["archive"]
    started = dt.datetime.now(dt.timezone.utc)
    status, error, rows, built, metrics = "error", None, 0, [], {}
    os.makedirs(SPILL_DIR, exist_ok=True)
    os.makedirs(SCRATCH_DIR, exist_ok=True)
    try:
        gz = f"{SCRATCH_DIR}/{name}.csv.gz"
        unc, comp = _member_to_gz(spec["archive"], spec["member"], gz)
        print(f"[{name}] extracted {spec['member']} comp={comp/1e6:.0f}MB unc={unc/1e9:.2f}GB")
        con = _new_con()
        try:
            rows = _sql_to_lance(con, air_select(gz), uri, so)
            distinct_key = con.execute(
                f"SELECT count(DISTINCT {spec['key']}) FROM {read_csv_expr(gz)} "
                f"WHERE {_t('REGISTRY_ID')} IS NOT NULL"
            ).fetchone()[0]
        finally:
            con.close()
        try:
            os.remove(gz)
        except OSError:
            pass
        metrics = {"rows": int(rows), "distinct_key": int(distinct_key),
                   "grain_ok": int(rows) == int(distinct_key)}
        built = _build_indexes(uri, spec["btree"], spec["bitmap"], so)
        print(f"[{name}] committed rows={rows:,} grain_ok={metrics['grain_ok']} indexes={built}")
        status = "success"
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
        print(f"[{name}] FAILED: {error}")
    finally:
        completed = dt.datetime.now(dt.timezone.utc)
        _record_run(run_id=run_id, dataset=name, dataset_uri=uri, source_archives=archives,
                    rows_written=int(rows), indexes_built=",".join(built), status=status,
                    error=error, metrics=metrics or None, started_at=started, completed_at=completed)
    return {"dataset": name, "uri": uri, "rows": int(rows), "status": status,
            "indexes": built, "metrics": metrics, "error": error}


# --------------------------------------------------------------------------- #
# Builder: epa_rcra_handlers (RCRA_FACILITIES ⋈ epa_program_links RCRAINFO → REGISTRY_ID)
# --------------------------------------------------------------------------- #
@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 90,
    memory=49152,
    cpu=4.0,
)
def build_rcra(run_id: str) -> dict:
    import datetime as dt

    import lance

    name = "epa_rcra_handlers"
    spec = SPECS[name]
    uri = ACTIVE_BASE + name + "/"
    so = _r2_storage_options()
    archives = f"{spec['archive']},epa_program_links"
    started = dt.datetime.now(dt.timezone.utc)
    status, error, rows, built, metrics = "error", None, 0, [], {}
    os.makedirs(SPILL_DIR, exist_ok=True)
    os.makedirs(SCRATCH_DIR, exist_ok=True)
    try:
        gz = f"{SCRATCH_DIR}/{name}.csv.gz"
        unc, comp = _member_to_gz(spec["archive"], spec["member"], gz)
        print(f"[{name}] extracted {spec['member']} comp={comp/1e6:.0f}MB unc={unc/1e9:.2f}GB")
        con = _new_con()
        try:
            # 1) RCRAINFO blocking set from the live epa_program_links Lance dataset.
            con.register("plinks_rdr", lance.dataset(PROGRAM_LINKS_URI, storage_options=so).scanner(
                columns=["REGISTRY_ID", "PGM_SYS_ID", "PGM_SYS_ACRNM"]).to_reader())
            con.execute(pl_rcra_temp_sql("plinks_rdr"))
            con.unregister("plinks_rdr")
            n_links = con.execute("SELECT count(*) FROM pl_rcra").fetchone()[0]
            print(f"[{name}] RCRAINFO blocking set rows={n_links:,}")
            # 2) Join → Lance.
            rows = _sql_to_lance(con, rcra_select(gz), uri, so)
            distinct_key = con.execute(
                f"SELECT count(DISTINCT {_t('r.ID_NUMBER')}) "
                f"FROM {read_csv_expr(gz)} r JOIN pl_rcra pl ON pl.pgm = {_t('r.ID_NUMBER')}"
            ).fetchone()[0]
        finally:
            con.close()
        try:
            os.remove(gz)
        except OSError:
            pass
        metrics = {"rows": int(rows), "rcrainfo_links": int(n_links),
                   "distinct_key": int(distinct_key), "grain_ok": int(rows) == int(distinct_key)}
        built = _build_indexes(uri, spec["btree"], spec["bitmap"], so)
        print(f"[{name}] committed rows={rows:,} grain_ok={metrics['grain_ok']} indexes={built}")
        status = "success"
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
        print(f"[{name}] FAILED: {error}")
    finally:
        completed = dt.datetime.now(dt.timezone.utc)
        _record_run(run_id=run_id, dataset=name, dataset_uri=uri, source_archives=archives,
                    rows_written=int(rows), indexes_built=",".join(built), status=status,
                    error=error, metrics=metrics or None, started_at=started, completed_at=completed)
    return {"dataset": name, "uri": uri, "rows": int(rows), "status": status,
            "indexes": built, "metrics": metrics, "error": error}


# --------------------------------------------------------------------------- #
# Orchestrator — build both (one container each), record summary, fire callback
# --------------------------------------------------------------------------- #
@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 120,
    memory=2048,
)
def run_entities(trigger_callback_url: str | None = None, only: list[str] | None = None) -> dict:
    import datetime as dt

    started = dt.datetime.now(dt.timezone.utc)
    run_id = started.strftime("epa_entities_%Y%m%dT%H%M%SZ")
    todo = set(only) if only else set(SPECS)

    calls = []
    if "epa_air_facilities" in todo:
        calls.append(("epa_air_facilities", build_air.spawn(run_id)))
    if "epa_rcra_handlers" in todo:
        calls.append(("epa_rcra_handlers", build_rcra.spawn(run_id)))

    results = []
    for nm, call in calls:
        try:
            results.append(call.get())
        except Exception as exc:  # noqa: BLE001 — one bad dataset must not sink the batch
            print(f"[orchestrator] {nm} raised: {exc}")
            results.append({"dataset": nm, "rows": 0, "status": "error", "error": str(exc)})

    ok = [r for r in results if r.get("status") == "success"]
    bad = [r for r in results if r.get("status") != "success"]
    total_rows = sum(int(r.get("rows", 0)) for r in results)
    status = "success" if not bad else ("partial" if ok else "error")
    completed = dt.datetime.now(dt.timezone.utc)

    _record_run(run_id=run_id, dataset="__run__", dataset_uri=ACTIVE_BASE,
                source_archives=f"{len(results)} datasets", rows_written=total_rows,
                indexes_built="", status=status,
                error=(None if not bad else ",".join(r["dataset"] for r in bad)),
                metrics={"datasets": {r["dataset"]: r.get("rows") for r in results},
                         "failed": [r["dataset"] for r in bad]},
                started_at=started, completed_at=completed)
    _post_callback(trigger_callback_url, {"status": status, "rows": total_rows, "feed": FEED})

    summary = {"run_id": run_id, "status": status, "total_rows": total_rows,
               "datasets": {r["dataset"]: {"rows": r.get("rows"), "status": r.get("status")}
                            for r in results}}
    print(summary)
    if status == "error":
        raise RuntimeError(f"EPA entity ingest failed: {[r['dataset'] for r in bad]}")
    return summary


# --------------------------------------------------------------------------- #
# Ops + verification entrypoints
# --------------------------------------------------------------------------- #
@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=120)
def init_ops() -> dict:
    conn = _pg_connect()
    if conn is None:
        raise RuntimeError("HQX_DB_URL_POOLED not set.")
    try:
        with conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
    finally:
        conn.close()
    return {"status": "ok", "table": "ops.epa_ingest_runs"}


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=900)
def verify_entities() -> dict:
    """Read-back proof: open each committed dataset, report rows + indices + REGISTRY_ID null
    count + grain (rows == distinct key)."""
    import duckdb
    import lance

    so = _r2_storage_options()
    out = {}
    for name, spec in SPECS.items():
        uri = ACTIVE_BASE + name + "/"
        try:
            ds = lance.dataset(uri, storage_options=so)
            idx = [i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i))
                   for i in ds.list_indices()]
            con = duckdb.connect(":memory:")
            con.register("rdr", ds.scanner(columns=["REGISTRY_ID", spec["key"]]).to_reader())
            rows, nulls, distinct_key = con.execute(
                "SELECT count(*), count(*) FILTER (WHERE REGISTRY_ID IS NULL), "
                f"count(DISTINCT {spec['key']}) FROM rdr"
            ).fetchone()
            con.close()
            out[name] = {"rows": ds.count_rows(), "indices": idx, "columns": len(ds.schema),
                         "version": ds.version, "registry_id_nulls": int(nulls),
                         "distinct_key": int(distinct_key), "grain_ok": int(rows) == int(distinct_key)}
        except Exception as exc:  # noqa: BLE001
            out[name] = {"error": str(exc)}
    print(out)
    return out


# --------------------------------------------------------------------------- #
# Local entrypoints (manual ops)
# --------------------------------------------------------------------------- #
@app.local_entrypoint()
def init() -> None:
    print(init_ops.remote())


@app.local_entrypoint()
def air() -> None:
    import datetime as dt

    run_id = dt.datetime.now(dt.timezone.utc).strftime("epa_air_%Y%m%dT%H%M%SZ")
    print(build_air.remote(run_id))


@app.local_entrypoint()
def rcra() -> None:
    import datetime as dt

    run_id = dt.datetime.now(dt.timezone.utc).strftime("epa_rcra_%Y%m%dT%H%M%SZ")
    print(build_rcra.remote(run_id))


@app.local_entrypoint()
def run(only: str = "") -> None:
    names = [n for n in only.split(",") if n] or None
    print(run_entities.remote(trigger_callback_url=None, only=names))


@app.local_entrypoint()
def verify() -> None:
    import json

    print(json.dumps(verify_entities.remote(), indent=2, default=str))
