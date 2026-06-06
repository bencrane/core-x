"""Compute worker — EPA NPDES **GTM compliance layer**.

Turns the 422 M-row raw discharge-event trove (``epa_npdes_dmrs``) into a GTM-grade,
entity-resolvable, index-fast compliance graph. Three net-new Lance datasets under
``s3://data-sink/active/`` (the raw DMR table is re-indexed separately by
``materialize_epa_history.py::reindex``):

  1) epa_permits             — THE BRIDGE. EXTERNAL_PERMIT_NMBR → REGISTRY_ID + PERMIT_NAME
                               (+ facility address/geo). ICIS_PERMITS ⟕ ICIS_FACILITIES.
                               Without this a DMR permit cannot become a targetable entity.
                               ~1.69 M permit-version rows · ~1.01 M distinct REGISTRY_ID.
  2) epa_permit_compliance   — per-PERMIT compliance resume (one row per EXTERNAL_PERMIT_NMBR):
                               the 422 M DMR rows rolled up (violations, exceedances, parameters,
                               recency, FY span) with the entity (REGISTRY_ID, PERMIT_NAME,
                               normalized_legal_name, geo) attached. The contractor_award_summary
                               pattern: heavy event data → small per-entity resume → BTREE on the
                               key → sub-100 ms agent lookups instead of an 18 s GROUP BY scan.
  3) epa_entity_compliance   — per-ENTITY rollup (one row per REGISTRY_ID): a company's whole
                               NPDES footprint across its permits. The GTM-actionable grain.

Why this is the unlock: the DMR table is fast for permit/time-keyed retrieval (its BTREE/BITMAP
indices push down through DuckDB), but it carries NO name/domain/REGISTRY_ID and the
violation/parameter analytics scan 422 M rows. This layer adds (a) the permit→entity bridge,
(b) a precomputed compliance resume keyed for instant lookup by permit | entity | name | state,
and (c) the entity rollup — so a GTM agent answers "active permits with recent violations in TX
discharging <parameter>, resolved to a company" by index lookup, not a full scan.

Net-new datasets are SMALL (~1–1.7 M rows) so their scalar indices stay well under R2's multipart
escalation threshold — direct-to-R2 ``create_scalar_index`` is safe here (unlike the 422 M-row DMR
table, which requires the local round-trip in materialize_epa_history.py). Each build is
SELF-FINALIZING (re-reads R2, records a terminal ops row, prints DONE — VERIFIED) and DETACHABLE.

    modal run pipelines/ingest_epa/materialize_epa_gtm.py::permits        # 1) the bridge
    modal run pipelines/ingest_epa/materialize_epa_gtm.py::permit_summary # 2) per-permit resume
    modal run pipelines/ingest_epa/materialize_epa_gtm.py::entity_summary # 3) per-entity rollup
    modal run pipelines/ingest_epa/materialize_epa_gtm.py::all            # 1→2→3 in order
    modal run pipelines/ingest_epa/materialize_epa_gtm.py::verify
"""

from __future__ import annotations

import os

import modal

from core.name_norm import name_norm

# --------------------------------------------------------------------------- #
LANDING_BUCKET = "data-sink"
LANDING_PREFIX = "landing/epa/"
ACTIVE_BASE = os.environ.get("EPA_ACTIVE_BASE", "s3://data-sink/active/")
FEED = "epa_gtm_layer"
DATA_STORAGE_VERSION = "2.1"
MAX_ROWS_PER_FILE = 1_048_576
PARQUET_ROW_GROUP = 1_048_576
LANCE_READ_BATCH = 100_000
DUCKDB_THREADS = 8
DUCKDB_MEMORY_LIMIT = "48GB"
SPILL_DIR = "/tmp/duckdb_spill"
SCRATCH_DIR = "/tmp/epa_gtm"

DMR_URI = ACTIVE_BASE + "epa_npdes_dmrs/"
PERMITS_URI = ACTIVE_BASE + "epa_permits/"
PERMIT_SUMMARY_URI = ACTIVE_BASE + "epa_permit_compliance/"
ENTITY_SUMMARY_URI = ACTIVE_BASE + "epa_entity_compliance/"

image = (
    modal.Image.debian_slim(python_version="3.12")
    .pip_install(
        "duckdb>=1.5,<2",
        "lancedb>=0.15",
        "pylance>=7",
        "pyarrow>=17",
        "boto3>=1.34",
        "psycopg[binary]>=3.2",
    )
    .add_local_python_source("core.name_norm")  # byte-identical blocking key (joins to companies/SoS)
)
app = modal.App("epa-gtm-layer", image=image)


# --------------------------------------------------------------------------- #
# SQL cast helpers (verbatim fleet convention)
# --------------------------------------------------------------------------- #
def _txt(c): return f"nullif(trim({c}),'')"
def _num(c): return f"TRY_CAST({_txt(c)} AS DOUBLE)"
def _int(c): return f"TRY_CAST({_txt(c)} AS BIGINT)"
def _date(c): return f"CAST(try_strptime({_txt(c)}, '%m/%d/%Y') AS DATE)"


def _read_gz(gz: str) -> str:
    return (
        f"read_csv('{gz}', all_varchar=true, header=true, parallel=false, "
        "compression='gzip', quote='\"', escape='\"', strict_mode=false, ignore_errors=false)"
    )


# --------------------------------------------------------------------------- #
# R2 / S3 (random-access ZIP member extract) — verbatim fleet helpers
# --------------------------------------------------------------------------- #
def _r2_storage_options() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID.")
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
    return boto3.client(
        "s3", endpoint_url=so["endpoint"], aws_access_key_id=so["aws_access_key_id"],
        aws_secret_access_key=so["aws_secret_access_key"], region_name="auto",
        config=Config(signature_version="s3v4", request_checksum_calculation="when_required",
                      response_checksum_validation="when_required",
                      retries={"max_attempts": 5, "mode": "standard"}),
    )


class _S3RangeReader:
    def __init__(self, client, bucket, key):
        self.c, self.b, self.k = client, bucket, key
        self._pos = 0
        self._size = client.head_object(Bucket=bucket, Key=key)["ContentLength"]

    def seekable(self): return True
    def readable(self): return True
    def tell(self): return self._pos

    def seek(self, off, whence=0):
        self._pos = off if whence == 0 else (self._pos + off if whence == 1 else self._size + off)
        return self._pos

    def read(self, n=-1):
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


def _member_to_gz(client, archive, member, out_path) -> tuple[int, int]:
    import struct
    import zipfile

    key = LANDING_PREFIX + archive
    reader = _S3RangeReader(client, LANDING_BUCKET, key)
    zf = zipfile.ZipFile(reader)
    zi = zf.getinfo(member)
    reader.seek(zi.header_offset)
    lh = reader.read(30)
    if lh[:4] != b"PK\x03\x04":
        raise RuntimeError(f"{archive}:{member} bad local header")
    name_len = struct.unpack("<H", lh[26:28])[0]
    extra_len = struct.unpack("<H", lh[28:30])[0]
    data_off = zi.header_offset + 30 + name_len + extra_len
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "wb") as out:
        out.write(b"\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff")
        remaining = zi.compress_size
        pos = data_off
        chunk = 16 << 20
        while remaining > 0:
            end = pos + min(chunk, remaining) - 1
            body = client.get_object(Bucket=LANDING_BUCKET, Key=key, Range=f"bytes={pos}-{end}")["Body"].read()
            if not body:
                raise RuntimeError(f"{archive}:{member} short read")
            out.write(body)
            pos += len(body)
            remaining -= len(body)
        out.write(struct.pack("<II", zi.CRC & 0xFFFFFFFF, zi.file_size & 0xFFFFFFFF))
    return zi.file_size, zi.compress_size


def _new_con():
    import duckdb

    con = duckdb.connect(":memory:")
    con.execute(f"SET threads TO {DUCKDB_THREADS}")
    con.execute(f"SET memory_limit='{DUCKDB_MEMORY_LIMIT}'")
    con.execute(f"SET temp_directory='{SPILL_DIR}'")
    con.execute("SET preserve_insertion_order=false")
    return con


# --------------------------------------------------------------------------- #
# ops ledger (resilient connect — never raises; ledger is best-effort)
# --------------------------------------------------------------------------- #
OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.epa_ingest_runs (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id text NOT NULL, feed text NOT NULL, dataset text NOT NULL, dataset_uri text,
    source_archives text, rows_written bigint, indexes_built text, status text NOT NULL,
    error text, metrics jsonb, started_at timestamptz, completed_at timestamptz,
    recorded_at timestamptz NOT NULL DEFAULT now());
CREATE INDEX IF NOT EXISTS epa_ingest_runs_dataset_idx ON ops.epa_ingest_runs (dataset);
CREATE INDEX IF NOT EXISTS epa_ingest_runs_status_idx  ON ops.epa_ingest_runs (status);
"""


def _pg_connect():
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        return None
    try:
        return psycopg.connect(dsn, connect_timeout=10)
    except Exception as exc:  # noqa: BLE001 — ledger best-effort
        print(f"WARN: ops DB connect failed ({type(exc).__name__}); skipping ops.* write.")
        return None


def _record_run(*, run_id, dataset, dataset_uri, source, rows, indexes, status, error, metrics,
                started, completed) -> None:
    from psycopg.types.json import Jsonb

    conn = _pg_connect()
    if conn is None:
        return
    try:
        with conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
            cur.execute(
                """INSERT INTO ops.epa_ingest_runs
                   (run_id, feed, dataset, dataset_uri, source_archives, rows_written,
                    indexes_built, status, error, metrics, started_at, completed_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (run_id, FEED, dataset, dataset_uri, source, rows, indexes, status, error,
                 Jsonb(metrics) if metrics is not None else None, started, completed),
            )
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: ops write failed: {exc}")
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# Lance write + index + self-verify (shared by all three builders)
# --------------------------------------------------------------------------- #
def _write_lance_from_parquet(uri, pq, so):
    import lance
    import pyarrow.dataset as pds

    reader = pds.dataset(pq).scanner(batch_size=LANCE_READ_BATCH).to_reader()
    lance.write_dataset(reader, uri, mode="overwrite", data_storage_version=DATA_STORAGE_VERSION,
                        max_rows_per_file=MAX_ROWS_PER_FILE, storage_options=so)


def _build_indexes(uri, btree, bitmap, so) -> list[str]:
    import lance

    ds = lance.dataset(uri, storage_options=so)
    built = []
    for col in btree:
        ds.create_scalar_index(col, index_type="BTREE", replace=True)
        built.append(f"{col}:BTREE")
        print(f"  BTREE  ✓ {col}")
    for col in bitmap:
        ds.create_scalar_index(col, index_type="BITMAP", replace=True)
        built.append(f"{col}:BITMAP")
        print(f"  BITMAP ✓ {col}")
    return built


def _verify(uri, so) -> dict:
    import lance

    ds = lance.dataset(uri, storage_options=so)
    idx = [i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i)) for i in ds.list_indices()]
    return {"rows": ds.count_rows(), "indices": idx, "version": ds.version}


def _finish(run_id, dataset, uri, source, btree, bitmap, so, started, rows) -> dict:
    """Index direct-to-R2 (safe at this scale), self-verify, record the terminal ledger row."""
    import datetime as dt

    status, error, built, verify_out = "error", None, [], {}
    try:
        built = _build_indexes(uri, btree, bitmap, so)
        verify_out = _verify(uri, so)
        status = "success"
        print(f"[{dataset}] ✅ DONE — VERIFIED {verify_out}")
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
        print(f"[{dataset}] index/verify FAILED: {error}")
    finally:
        _record_run(run_id=run_id, dataset=dataset, dataset_uri=uri, source=source,
                    rows=int(rows), indexes=",".join(built), status=status, error=error,
                    metrics={"verify": verify_out}, started=started,
                    completed=dt.datetime.now(dt.timezone.utc))
    if status != "success":
        raise RuntimeError(f"{dataset} finish failed: {error}")
    return {"dataset": dataset, "rows": int(rows), "indexes": built, "verify": verify_out}


# ════════════════════════ 1) epa_permits — THE BRIDGE ═════════════════════════
# ICIS_PERMITS (permittee legal name) ⟕ ICIS_FACILITIES (REGISTRY_ID + address/geo),
# both in npdes_downloads.zip. INNER on f.NPDES_ID = p.EXTERNAL_PERMIT_NMBR with a
# resolvable REGISTRY_ID. Lifted from EPA_LEGAL_ENTITY_MATERIALIZATION_PLAN.md §2.1.
PERMITS_SQL = f"""
SELECT
    {_txt('f.FACILITY_UIN')}                       AS REGISTRY_ID,
    {_txt('p.PERMIT_NAME')}                        AS PERMIT_NAME,
    {name_norm('p.PERMIT_NAME')}                   AS normalized_legal_name,
    {_txt('p.EXTERNAL_PERMIT_NMBR')}               AS EXTERNAL_PERMIT_NMBR,
    {_txt('f.NPDES_ID')}                           AS NPDES_ID,
    {_int('p.VERSION_NMBR')}                       AS VERSION_NMBR,
    {_txt('p.ACTIVITY_ID')}                        AS ACTIVITY_ID,
    {_txt('p.PERMIT_TYPE_CODE')}                   AS PERMIT_TYPE_CODE,
    {_txt('p.PERMIT_STATUS_CODE')}                 AS PERMIT_STATUS_CODE,
    {_txt('p.MAJOR_MINOR_STATUS_FLAG')}            AS MAJOR_MINOR_STATUS_FLAG,
    {_txt('p.FACILITY_TYPE_INDICATOR')}            AS FACILITY_TYPE_INDICATOR,
    {_txt('p.ISSUING_AGENCY')}                     AS ISSUING_AGENCY,
    {_txt('p.MASTER_EXTERNAL_PERMIT_NMBR')}        AS MASTER_EXTERNAL_PERMIT_NMBR,
    {_date('p.ORIGINAL_ISSUE_DATE')}               AS ORIGINAL_ISSUE_DATE,
    {_date('p.EFFECTIVE_DATE')}                    AS EFFECTIVE_DATE,
    {_date('p.EXPIRATION_DATE')}                   AS EXPIRATION_DATE,
    {_date('p.TERMINATION_DATE')}                  AS TERMINATION_DATE,
    {_num('p.TOTAL_DESIGN_FLOW_NMBR')}             AS TOTAL_DESIGN_FLOW_NMBR,
    {_num('p.ACTUAL_AVERAGE_FLOW_NMBR')}           AS ACTUAL_AVERAGE_FLOW_NMBR,
    {_txt('f.FACILITY_NAME')}                      AS FAC_NAME,
    {_txt('f.LOCATION_ADDRESS')}                   AS FAC_LOCATION_ADDRESS,
    {_txt('f.CITY')}                               AS FAC_CITY,
    {_txt('f.STATE_CODE')}                         AS FAC_STATE_CODE,
    {_txt('f.ZIP')}                                AS FAC_ZIP,
    {_txt('f.COUNTY_CODE')}                        AS FAC_COUNTY_CODE,
    {_num('f.GEOCODE_LATITUDE')}                   AS FAC_LATITUDE,
    {_num('f.GEOCODE_LONGITUDE')}                  AS FAC_LONGITUDE
FROM icis_permits p
JOIN icis_facilities f ON f.NPDES_ID = p.EXTERNAL_PERMIT_NMBR
WHERE {_txt('f.FACILITY_UIN')} IS NOT NULL
"""


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 60, memory=32768, cpu=8.0, retries=2,
)
def build_permits(run_id: str = "manual") -> dict:
    """1) epa_permits — the EXTERNAL_PERMIT_NMBR → REGISTRY_ID + PERMIT_NAME bridge."""
    import datetime as dt

    so = _r2_storage_options()
    started = dt.datetime.now(dt.timezone.utc)
    os.makedirs(SPILL_DIR, exist_ok=True)
    os.makedirs(SCRATCH_DIR, exist_ok=True)
    cl = _s3_client()
    for tbl, member in (("icis_permits", "ICIS_PERMITS.csv"), ("icis_facilities", "ICIS_FACILITIES.csv")):
        gz = f"{SCRATCH_DIR}/{member}.gz"
        _member_to_gz(cl, "npdes_downloads.zip", member, gz)
        print(f"[permits] extracted {member}")

    con = _new_con()
    try:
        for tbl, member in (("icis_permits", "ICIS_PERMITS.csv"), ("icis_facilities", "ICIS_FACILITIES.csv")):
            con.execute(f"CREATE TEMP TABLE {tbl} AS SELECT * FROM {_read_gz(f'{SCRATCH_DIR}/{member}.gz')}")
        pq = f"{SCRATCH_DIR}/epa_permits.parquet"
        con.execute(f"COPY ({PERMITS_SQL}) TO '{pq}' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE {PARQUET_ROW_GROUP})")
        rows = con.execute(f"SELECT count(*) FROM read_parquet('{pq}')").fetchone()[0]
    finally:
        con.close()
    _write_lance_from_parquet(PERMITS_URI, pq, so)
    print(f"[permits] wrote {rows:,} rows → {PERMITS_URI}")
    return _finish(run_id, "epa_permits", PERMITS_URI, "npdes_downloads.zip",
                   btree=["REGISTRY_ID", "EXTERNAL_PERMIT_NMBR", "NPDES_ID", "normalized_legal_name"],
                   bitmap=["FAC_STATE_CODE", "PERMIT_STATUS_CODE", "MAJOR_MINOR_STATUS_FLAG", "PERMIT_TYPE_CODE"],
                   so=so, started=started, rows=rows)


# ═══════════════ 2) epa_permit_compliance — per-PERMIT GTM resume ══════════════
# Roll the 422 M DMR rows up to one row per EXTERNAL_PERMIT_NMBR, attach the entity
# (REGISTRY_ID, PERMIT_NAME, normalized_legal_name, geo) from epa_permits' latest version.
PERMIT_SUMMARY_SQL = """
WITH agg AS (
    SELECT
        EXTERNAL_PERMIT_NMBR,
        count(*)                                                          AS n_dmr_rows,
        count(DMR_VALUE_NMBR)                                             AS n_measured,
        approx_count_distinct(PARAMETER_CODE)                            AS n_distinct_parameters,
        min(MONITORING_PERIOD_END_DATE)                                  AS first_period,
        max(MONITORING_PERIOD_END_DATE)                                  AS last_period,
        min(FISCAL_YEAR)                                                 AS first_fy,
        max(FISCAL_YEAR)                                                 AS last_fy,
        count(VIOLATION_CODE)                                            AS n_violations,
        approx_count_distinct(VIOLATION_CODE)                            AS n_distinct_violation_codes,
        max(CASE WHEN VIOLATION_CODE IS NOT NULL THEN MONITORING_PERIOD_END_DATE END) AS last_violation_period,
        count(*) FILTER (WHERE EXCEEDENCE_PCT IS NOT NULL AND EXCEEDENCE_PCT > 0)     AS n_exceedances,
        max(EXCEEDENCE_PCT)                                              AS max_exceedence_pct,
        count(NODI_CODE)                                                 AS n_nodi
    FROM dmr
    WHERE EXTERNAL_PERMIT_NMBR IS NOT NULL
    GROUP BY 1
),
permit_latest AS (
    SELECT EXTERNAL_PERMIT_NMBR, REGISTRY_ID, PERMIT_NAME, normalized_legal_name,
           FAC_STATE_CODE, FAC_CITY, FAC_ZIP, FAC_COUNTY_CODE, FAC_LATITUDE, FAC_LONGITUDE,
           PERMIT_TYPE_CODE, PERMIT_STATUS_CODE, MAJOR_MINOR_STATUS_FLAG, FACILITY_TYPE_INDICATOR
    FROM permits
    QUALIFY row_number() OVER (PARTITION BY EXTERNAL_PERMIT_NMBR ORDER BY VERSION_NMBR DESC NULLS LAST) = 1
)
SELECT
    a.EXTERNAL_PERMIT_NMBR,
    p.REGISTRY_ID, p.PERMIT_NAME, p.normalized_legal_name,
    p.FAC_STATE_CODE AS FAC_STATE, p.FAC_CITY, p.FAC_ZIP, p.FAC_COUNTY_CODE,
    p.FAC_LATITUDE, p.FAC_LONGITUDE,
    p.PERMIT_TYPE_CODE, p.PERMIT_STATUS_CODE, p.MAJOR_MINOR_STATUS_FLAG, p.FACILITY_TYPE_INDICATOR,
    a.n_dmr_rows, a.n_measured, a.n_distinct_parameters,
    a.first_period, a.last_period, a.first_fy, a.last_fy,
    a.n_violations, a.n_distinct_violation_codes, a.last_violation_period,
    a.n_exceedances, a.max_exceedence_pct, a.n_nodi,
    (a.n_violations > 0)                                                 AS has_violations,
    (a.last_fy >= 2024)                                                  AS is_active,
    (p.REGISTRY_ID IS NOT NULL)                                         AS entity_resolved,
    CASE WHEN a.n_violations = 0 THEN 'none'
         WHEN a.n_violations < 12 THEN 'low'
         WHEN a.n_violations < 120 THEN 'medium'
         ELSE 'high' END                                                AS violation_tier
FROM agg a
LEFT JOIN permit_latest p ON p.EXTERNAL_PERMIT_NMBR = a.EXTERNAL_PERMIT_NMBR
"""


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 60 * 3, memory=65536, cpu=16.0, ephemeral_disk=524288, retries=2,
)
def build_permit_summary(run_id: str = "manual") -> dict:
    """2) epa_permit_compliance — per-permit compliance resume (DMR rollup ⟕ epa_permits)."""
    import datetime as dt

    import lance

    so = _r2_storage_options()
    started = dt.datetime.now(dt.timezone.utc)
    os.makedirs(SPILL_DIR, exist_ok=True)
    os.makedirs(SCRATCH_DIR, exist_ok=True)
    con = _new_con()
    try:
        con.register("dmr", lance.dataset(DMR_URI, storage_options=so))
        con.register("permits", lance.dataset(PERMITS_URI, storage_options=so))
        pq = f"{SCRATCH_DIR}/epa_permit_compliance.parquet"
        print("[permit_summary] aggregating 422M DMR rows by EXTERNAL_PERMIT_NMBR …")
        con.execute(f"COPY ({PERMIT_SUMMARY_SQL}) TO '{pq}' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE {PARQUET_ROW_GROUP})")
        rows = con.execute(f"SELECT count(*) FROM read_parquet('{pq}')").fetchone()[0]
    finally:
        con.close()
    _write_lance_from_parquet(PERMIT_SUMMARY_URI, pq, so)
    print(f"[permit_summary] wrote {rows:,} rows → {PERMIT_SUMMARY_URI}")
    return _finish(run_id, "epa_permit_compliance", PERMIT_SUMMARY_URI, "epa_npdes_dmrs+epa_permits",
                   btree=["EXTERNAL_PERMIT_NMBR", "REGISTRY_ID", "normalized_legal_name"],
                   bitmap=["FAC_STATE", "PERMIT_STATUS_CODE", "has_violations", "is_active",
                           "violation_tier", "entity_resolved"],
                   so=so, started=started, rows=rows)


# ═══════════════ 3) epa_entity_compliance — per-ENTITY rollup ══════════════════
ENTITY_SUMMARY_SQL = """
SELECT
    REGISTRY_ID,
    any_value(PERMIT_NAME)               AS entity_name,
    any_value(normalized_legal_name)     AS normalized_legal_name,
    any_value(FAC_STATE)                 AS FAC_STATE,
    any_value(FAC_CITY)                  AS FAC_CITY,
    any_value(FAC_ZIP)                   AS FAC_ZIP,
    any_value(FAC_LATITUDE)              AS FAC_LATITUDE,
    any_value(FAC_LONGITUDE)             AS FAC_LONGITUDE,
    count(*)                             AS n_permits,
    sum(n_dmr_rows)                      AS n_dmr_rows,
    sum(n_violations)                    AS n_violations,
    sum(n_exceedances)                   AS n_exceedances,
    max(max_exceedence_pct)              AS max_exceedence_pct,
    min(first_period)                    AS first_period,
    max(last_period)                     AS last_period,
    min(first_fy)                        AS first_fy,
    max(last_fy)                         AS last_fy,
    max(last_violation_period)           AS last_violation_period,
    bool_or(has_violations)              AS has_violations,
    bool_or(is_active)                   AS is_active,
    CASE WHEN sum(n_violations) = 0 THEN 'none'
         WHEN sum(n_violations) < 12 THEN 'low'
         WHEN sum(n_violations) < 120 THEN 'medium'
         ELSE 'high' END                AS violation_tier
FROM psummary
WHERE REGISTRY_ID IS NOT NULL
GROUP BY REGISTRY_ID
"""


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 60, memory=32768, cpu=8.0, retries=2,
)
def build_entity_summary(run_id: str = "manual") -> dict:
    """3) epa_entity_compliance — per-REGISTRY_ID rollup (a company's whole NPDES footprint)."""
    import datetime as dt

    import lance

    so = _r2_storage_options()
    started = dt.datetime.now(dt.timezone.utc)
    os.makedirs(SPILL_DIR, exist_ok=True)
    os.makedirs(SCRATCH_DIR, exist_ok=True)
    con = _new_con()
    try:
        con.register("psummary", lance.dataset(PERMIT_SUMMARY_URI, storage_options=so))
        pq = f"{SCRATCH_DIR}/epa_entity_compliance.parquet"
        con.execute(f"COPY ({ENTITY_SUMMARY_SQL}) TO '{pq}' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE {PARQUET_ROW_GROUP})")
        rows = con.execute(f"SELECT count(*) FROM read_parquet('{pq}')").fetchone()[0]
    finally:
        con.close()
    _write_lance_from_parquet(ENTITY_SUMMARY_URI, pq, so)
    print(f"[entity_summary] wrote {rows:,} rows → {ENTITY_SUMMARY_URI}")
    return _finish(run_id, "epa_entity_compliance", ENTITY_SUMMARY_URI, "epa_permit_compliance",
                   btree=["REGISTRY_ID", "normalized_legal_name"],
                   bitmap=["FAC_STATE", "has_violations", "is_active", "violation_tier"],
                   so=so, started=started, rows=rows)


# --------------------------------------------------------------------------- #
@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=300)
def verify_all() -> dict:
    import lance

    so = _r2_storage_options()
    out = {}
    for name, uri in (("epa_permits", PERMITS_URI), ("epa_permit_compliance", PERMIT_SUMMARY_URI),
                      ("epa_entity_compliance", ENTITY_SUMMARY_URI)):
        try:
            out[name] = _verify(uri, so)
        except Exception as exc:  # noqa: BLE001
            out[name] = {"error": str(exc)}
    print(out)
    return out


# --------------------------------------------------------------------------- #
@app.local_entrypoint()
def permits():
    import datetime as dt
    print(build_permits.remote(dt.datetime.now(dt.timezone.utc).strftime("epa_gtm_%Y%m%dT%H%M%SZ")))


@app.local_entrypoint()
def permit_summary():
    import datetime as dt
    print(build_permit_summary.remote(dt.datetime.now(dt.timezone.utc).strftime("epa_gtm_%Y%m%dT%H%M%SZ")))


@app.local_entrypoint()
def entity_summary():
    import datetime as dt
    print(build_entity_summary.remote(dt.datetime.now(dt.timezone.utc).strftime("epa_gtm_%Y%m%dT%H%M%SZ")))


@app.local_entrypoint()
def all():
    """Build all three in dependency order: permits → permit_compliance → entity_compliance."""
    import datetime as dt

    run_id = dt.datetime.now(dt.timezone.utc).strftime("epa_gtm_%Y%m%dT%H%M%SZ")
    print(build_permits.remote(run_id))
    print(build_permit_summary.remote(run_id))
    print(build_entity_summary.remote(run_id))


@app.local_entrypoint()
def verify():
    import json
    print(json.dumps(verify_all.remote(), indent=2, default=str))
