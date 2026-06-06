"""Compute worker — EPA Multi-Media Compliance materialization (Directive 30).

Part of the ``epa-pipelines`` Modal app. NOT directly exposed — spawned by the
Universal Dispatcher (core/modal_dispatcher.py) or run manually via ``modal run``.
No web endpoint.

What it builds (clean-room data plane — no Iceberg, no Polaris):
    s3://data-sink/landing/epa/*.zip   (raw ECHO/FRS bulk archives)
      → boto3 random-access extract of ONE member (central-directory seek, ZIP64-safe)
      → raw-deflate→gzip rewrap to /tmp/*.csv.gz   (no recompression; /tmp holds only the
        COMPRESSED member, never the multi-GB decompressed CSV — Directive 27 technique)
      → DuckDB read_csv(compression='gzip', all_varchar=true)  (100% of the transform in SQL)
      → fetch_record_batch (STREAMING — giants never materialize in RAM)
      → lance.write_dataset(s3://data-sink/active/<name>/, data_storage_version="2.1")
      → create_scalar_index BTREE on every load-bearing key (direct-R2, in place)

Datasets (11):
  Spine        epa_facilities            FRS_FACILITIES            BTREE registry_id
               epa_program_links         FRS_PROGRAM_LINKS         BTREE registry_id, pgm_sys_id
  Compliance   epa_npdes_dmrs            DMR FY2024+FY2025+FY2026   BTREE ext_permit, period_end
               epa_npdes_qncr_history    NPDES_QNCR_HISTORY        BTREE npdes_id, yearqtr
               epa_npdes_eff_violations  NPDES_EFF_VIOLATIONS      BTREE npdes_id, period_end
  Enforcement  epa_case_enforcements     CASE_ENFORCEMENTS         BTREE activity_id, case_number
               epa_case_milestones       CASE_MILESTONES           BTREE activity_id, actual_date
               epa_pipeline_caa          PIPELINE_CAA_00_COMPLETE  BTREE registry_id, source_id
               epa_pipeline_rcra         PIPELINE_RCRA_00_COMPLETE BTREE registry_id, source_id
               epa_aim_triggering_events aim_triggering_events     BTREE npdes_id
  Bridge       epa_to_sos_bridge         REGISTRY_ID ↔ sos_normalized_master  BTREE registry_id, name

Control plane (Trigger v4 durable callback): the orchestrator accepts
``trigger_callback_url`` and, on terminal state, (1) writes run rows to
``ops.epa_ingest_runs`` via psycopg and (2) POSTs ``{status, rows, feed}`` to the
waitpoint URL. Per-dataset rows are also written so a partial batch is auditable.

    modal run    pipelines/ingest_epa/materialize_epa.py::init       # create ops table
    modal run    pipelines/ingest_epa/materialize_epa.py::one --name epa_aim_triggering_events
    modal run    pipelines/ingest_epa/materialize_epa.py::run        # full batch (map) + bridge
    modal run    pipelines/ingest_epa/materialize_epa.py::bridge     # bridge only
    modal run    pipelines/ingest_epa/materialize_epa.py::verify     # read-back proof
    modal deploy pipelines/ingest_epa/materialize_epa.py             # dispatcher-resolvable
"""

from __future__ import annotations

import os

import modal

# Canonical cross-spine blocking-key macros (THE single source of truth). Imported as
# SQL-expression builders and shipped into the container via add_local_python_source.
from core.name_norm import legal_name_base, name_norm

# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #
LANDING_BUCKET = "data-sink"
LANDING_PREFIX = "landing/epa/"
ACTIVE_BASE = os.environ.get("EPA_ACTIVE_BASE", "s3://data-sink/active/")
SOS_MASTER_URI = os.environ.get(
    "SOS_NORMALIZED_MASTER_URI", "s3://data-sink/active/sos_normalized_master/"
)
FEED = "epa_multimedia"

DATA_STORAGE_VERSION = "2.1"          # net-new datasets → current Lance default
MAX_ROWS_PER_FILE = 1_048_576
PARQUET_ROW_GROUP = 1_048_576         # transport-parquet row-group size (bounded COPY memory)
LANCE_READ_BATCH = 100_000            # parquet→Lance streaming batch
DUCKDB_THREADS = 4
DUCKDB_MEMORY_LIMIT = "24GB"          # headroom for the single-threaded read of the 15.9 GB EFF
SPILL_DIR = "/tmp/duckdb_spill"
SCRATCH_DIR = "/tmp/epa"

# Backfill-protection floor (Directive 30). epa_npdes_dmrs accumulates the full
# FY1982–FYnow DMR history (appended by materialize_epa_history.py). The monthly
# spec below carries only the rolling FY window and writes source[0] with
# mode="overwrite", which would truncate that history. materialize_one refuses any
# DMR overwrite while the live table already exceeds this floor. The rolling window
# alone is ~67.6M rows; the unified table is ~422M — 350M cleanly separates them.
DMR_HISTORICAL_FLOOR = 350_000_000

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
    .env({"LANCE_BYPASS_SPILLING": "true"})  # in-memory BTREE sort for high-card string keys (lance#2650)
    .add_local_python_source("core.name_norm")
)

app = modal.App("epa-pipelines", image=image)


# --------------------------------------------------------------------------- #
# ops.epa_ingest_runs — terminal-state ledger (idempotent DDL, self-bootstrapping)
# Canonical copy mirrored at pipelines/ingest_epa/ops_epa_ingest_runs.sql.
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
# SQL cast helpers — defensive, NULL-safe (TRY_CAST → NULL, never a hard failure)
# --------------------------------------------------------------------------- #
def _txt(c: str) -> str:                       # trimmed, '' → NULL
    return f"nullif(trim({c}),'')"


def _num(c: str) -> str:                        # numeric → DOUBLE
    return f"TRY_CAST({_txt(c)} AS DOUBLE)"


def _int(c: str) -> str:                        # integer → BIGINT
    return f"TRY_CAST({_txt(c)} AS BIGINT)"


def _money(c: str) -> str:                      # "$1,234" / "1234" → DOUBLE
    return f"TRY_CAST(replace(replace({_txt(c)},'$',''),',','') AS DOUBLE)"


def _date(c: str) -> str:                       # MM/DD/YYYY → DATE (NULL on miss)
    return f"CAST(try_strptime({_txt(c)}, '%m/%d/%Y') AS DATE)"


def _read(gz_token: str = "__GZ__") -> str:
    # parallel=false: several EPA tables (CASE_ENFORCEMENTS et al.) carry embedded newlines
    # inside quoted free-text fields (ENF_SUMMARY_TEXT) and are CRLF; DuckDB's parallel CSV
    # reader cannot do a full read of such files through the streaming Arrow interface. The
    # source is gzip (already non-splittable → single-threaded decode), so this costs nothing.
    return (
        f"read_csv('{gz_token}', all_varchar=true, header=true, parallel=false, "
        "compression='gzip', quote='\"', escape='\"', strict_mode=false, ignore_errors=false)"
    )


def _retype(exclude: list[str], recasts: dict[str, str], extra: dict[str, str] | None = None) -> str:
    """SELECT * EXCLUDE(<typed cols>) , <typed casts AS same name> [, <extra literals>].
    Preserves EPA's native (clean) column identifiers; only load-bearing columns are typed."""
    cols = ", ".join(recasts[c] + f" AS {c}" for c in exclude)
    tail = ("," + ", ".join(f"{v} AS {k}" for k, v in (extra or {}).items())) if extra else ""
    excl = ", ".join(exclude)
    return f"SELECT * EXCLUDE ({excl}), {cols}{tail} FROM {_read()}"


# --------------------------------------------------------------------------- #
# Table specs — one entry per Lance dataset (the bridge is built separately)
# --------------------------------------------------------------------------- #
def build_specs() -> list[dict]:
    specs: list[dict] = []

    # 1) epa_facilities ← FRS_FACILITIES
    specs.append(dict(
        name="epa_facilities",
        sources=[dict(archive="frs_downloads.zip", member="FRS_FACILITIES.csv")],
        sql=_retype(
            ["REGISTRY_ID", "LATITUDE_MEASURE", "LONGITUDE_MEASURE"],
            {"REGISTRY_ID": _txt("REGISTRY_ID"),
             "LATITUDE_MEASURE": _num("LATITUDE_MEASURE"),
             "LONGITUDE_MEASURE": _num("LONGITUDE_MEASURE")},
        ),
        btree=["REGISTRY_ID"], bitmap=["FAC_STATE"],
    ))

    # 2) epa_program_links ← FRS_PROGRAM_LINKS (the cross-media bridge)
    specs.append(dict(
        name="epa_program_links",
        sources=[dict(archive="frs_downloads.zip", member="FRS_PROGRAM_LINKS.csv")],
        sql=_retype(
            ["REGISTRY_ID", "PGM_SYS_ID", "PGM_SYS_ACRNM"],
            {"REGISTRY_ID": _txt("REGISTRY_ID"),
             "PGM_SYS_ID": _txt("PGM_SYS_ID"),
             "PGM_SYS_ACRNM": _txt("PGM_SYS_ACRNM")},
        ),
        btree=["REGISTRY_ID", "PGM_SYS_ID"], bitmap=["PGM_SYS_ACRNM"],
    ))

    # 3) epa_npdes_dmrs ← FY2024 + FY2025 + FY2026 (one dataset, FISCAL_YEAR partition col)
    dmr_excl = ["EXTERNAL_PERMIT_NMBR", "LIMIT_VALUE_NMBR", "DMR_VALUE_NMBR",
                "LIMIT_VALUE_STANDARD_UNITS", "DMR_VALUE_STANDARD_UNITS", "EXCEEDENCE_PCT",
                "DAYS_LATE", "MONITORING_PERIOD_END_DATE", "LIMIT_BEGIN_DATE", "LIMIT_END_DATE",
                "VALUE_RECEIVED_DATE", "RNC_DETECTION_DATE", "RNC_RESOLUTION_DATE"]
    dmr_recasts = {
        "EXTERNAL_PERMIT_NMBR": _txt("EXTERNAL_PERMIT_NMBR"),
        "LIMIT_VALUE_NMBR": _num("LIMIT_VALUE_NMBR"), "DMR_VALUE_NMBR": _num("DMR_VALUE_NMBR"),
        "LIMIT_VALUE_STANDARD_UNITS": _num("LIMIT_VALUE_STANDARD_UNITS"),
        "DMR_VALUE_STANDARD_UNITS": _num("DMR_VALUE_STANDARD_UNITS"),
        "EXCEEDENCE_PCT": _num("EXCEEDENCE_PCT"), "DAYS_LATE": _int("DAYS_LATE"),
        "MONITORING_PERIOD_END_DATE": _date("MONITORING_PERIOD_END_DATE"),
        "LIMIT_BEGIN_DATE": _date("LIMIT_BEGIN_DATE"), "LIMIT_END_DATE": _date("LIMIT_END_DATE"),
        "VALUE_RECEIVED_DATE": _date("VALUE_RECEIVED_DATE"),
        "RNC_DETECTION_DATE": _date("RNC_DETECTION_DATE"),
        "RNC_RESOLUTION_DATE": _date("RNC_RESOLUTION_DATE"),
    }
    specs.append(dict(
        name="epa_npdes_dmrs",
        sources=[
            dict(archive="npdes_dmrs_fy2024.zip", member="NPDES_DMRS_FY2024.csv", fy=2024),
            dict(archive="npdes_dmrs_fy2025.zip", member="NPDES_DMRS_FY2025.csv", fy=2025),
            dict(archive="npdes_dmrs_fy2026.zip", member="NPDES_DMRS_FY2026.csv", fy=2026),
        ],
        sql=_retype(dmr_excl, dmr_recasts, extra={"FISCAL_YEAR": "CAST(__FY__ AS INTEGER)"}),
        btree=["EXTERNAL_PERMIT_NMBR", "MONITORING_PERIOD_END_DATE"], bitmap=["FISCAL_YEAR"],
        # Rolling window over accumulated history: the live SoR carries FY1982+ (appended
        # by materialize_epa_history.py); these sources are only the recent FY window.
        # materialize_one replaces the window in place (delete FISCAL_YEAR>=min(source FY),
        # then append) instead of overwriting — history is never truncated. THE ONLY spec
        # with this marker; every other spec is a correct full cumulative-snapshot overwrite.
        replace_partition="FISCAL_YEAR",
    ))

    # 4) epa_npdes_qncr_history ← NPDES_QNCR_HISTORY (quarterly trend baseline)
    qncr_excl = ["NPDES_ID", "NUME90Q", "NUMCVDT", "NUMSVCD", "NUMPSCH", "NUMD8090Q"]
    specs.append(dict(
        name="epa_npdes_qncr_history",
        sources=[dict(archive="npdes_downloads.zip", member="NPDES_QNCR_HISTORY.csv")],
        sql=_retype(qncr_excl,
                    {"NPDES_ID": _txt("NPDES_ID"), **{c: _int(c) for c in qncr_excl[1:]}}),
        btree=["NPDES_ID", "YEARQTR"], bitmap=[],
    ))

    # 5) epa_npdes_eff_violations ← NPDES_EFF_VIOLATIONS (cumulative infractions, ZIP64 giant)
    eff_excl = ["NPDES_ID", "DMR_VALUE_NMBR", "ADJUSTED_DMR_VALUE_NMBR",
                "LIMIT_VALUE_STANDARD_UNITS", "DMR_VALUE_STANDARD_UNITS",
                "ADJUSTED_DMR_STANDARD_UNITS", "STATISTICAL_BASE_MONTHLY_AVG", "EXCEEDENCE_PCT",
                "DAYS_LATE", "MONITORING_PERIOD_END_DATE", "VALUE_RECEIVED_DATE",
                "RNC_DETECTION_DATE", "RNC_RESOLUTION_DATE"]
    eff_recasts = {"NPDES_ID": _txt("NPDES_ID"), "DAYS_LATE": _int("DAYS_LATE"),
                   "MONITORING_PERIOD_END_DATE": _date("MONITORING_PERIOD_END_DATE"),
                   "VALUE_RECEIVED_DATE": _date("VALUE_RECEIVED_DATE"),
                   "RNC_DETECTION_DATE": _date("RNC_DETECTION_DATE"),
                   "RNC_RESOLUTION_DATE": _date("RNC_RESOLUTION_DATE")}
    for c in eff_excl:
        eff_recasts.setdefault(c, _num(c))
    specs.append(dict(
        name="epa_npdes_eff_violations",
        sources=[dict(archive="npdes_eff_downloads.zip", member="NPDES_EFF_VIOLATIONS.csv")],
        sql=_retype(eff_excl, eff_recasts),
        btree=["NPDES_ID", "MONITORING_PERIOD_END_DATE"], bitmap=[],
    ))

    # 6) epa_case_enforcements ← CASE_ENFORCEMENTS (penalties; note: source is CRLF)
    ce_excl = ["ACTIVITY_ID", "CASE_NUMBER", "FISCAL_YEAR", "TOTAL_PENALTY_ASSESSED_AMT",
               "TOTAL_COST_RECOVERY_AMT", "TOTAL_COMP_ACTION_AMT",
               "ACTIVITY_STATUS_DATE", "CASE_STATUS_DATE"]
    specs.append(dict(
        name="epa_case_enforcements",
        sources=[dict(archive="case_downloads.zip", member="CASE_ENFORCEMENTS.csv")],
        sql=_retype(ce_excl, {
            "ACTIVITY_ID": _txt("ACTIVITY_ID"), "CASE_NUMBER": _txt("CASE_NUMBER"),
            "FISCAL_YEAR": _int("FISCAL_YEAR"),
            "TOTAL_PENALTY_ASSESSED_AMT": _money("TOTAL_PENALTY_ASSESSED_AMT"),
            "TOTAL_COST_RECOVERY_AMT": _money("TOTAL_COST_RECOVERY_AMT"),
            "TOTAL_COMP_ACTION_AMT": _money("TOTAL_COMP_ACTION_AMT"),
            "ACTIVITY_STATUS_DATE": _date("ACTIVITY_STATUS_DATE"),
            "CASE_STATUS_DATE": _date("CASE_STATUS_DATE")}),
        btree=["ACTIVITY_ID", "CASE_NUMBER"], bitmap=[],
    ))

    # 7) epa_case_milestones ← CASE_MILESTONES (compliance schedule dates)
    specs.append(dict(
        name="epa_case_milestones",
        sources=[dict(archive="case_downloads.zip", member="CASE_MILESTONES.csv")],
        sql=_retype(["ACTIVITY_ID", "CASE_NUMBER", "ACTUAL_DATE"],
                    {"ACTIVITY_ID": _txt("ACTIVITY_ID"), "CASE_NUMBER": _txt("CASE_NUMBER"),
                     "ACTUAL_DATE": _date("ACTUAL_DATE")}),
        btree=["ACTIVITY_ID", "ACTUAL_DATE"], bitmap=[],
    ))

    # 8) epa_pipeline_caa ← PIPELINE_CAA_00_COMPLETE (flattened CAA timeline, REGISTRY_ID inline)
    caa_excl = ["REGISTRY_ID", "SOURCE_ID", "EA_PENALTY_AMT", "EA_COMP_ACTION_COST",
                "SORT_DATE", "EVAL_DATE", "VIOL_START_DATE", "VIOL_END_DATE", "EA_DATE"]
    specs.append(dict(
        name="epa_pipeline_caa",
        sources=[dict(archive="pipeline_caa_downloads.zip", member="PIPELINE_CAA_00_COMPLETE.csv")],
        sql=_retype(caa_excl, {
            "REGISTRY_ID": _txt("REGISTRY_ID"), "SOURCE_ID": _txt("SOURCE_ID"),
            "EA_PENALTY_AMT": _money("EA_PENALTY_AMT"),
            "EA_COMP_ACTION_COST": _money("EA_COMP_ACTION_COST"),
            "SORT_DATE": _date("SORT_DATE"), "EVAL_DATE": _date("EVAL_DATE"),
            "VIOL_START_DATE": _date("VIOL_START_DATE"), "VIOL_END_DATE": _date("VIOL_END_DATE"),
            "EA_DATE": _date("EA_DATE")}),
        btree=["REGISTRY_ID", "SOURCE_ID"], bitmap=["FOUND_VIOLATION"],
    ))

    # 9) epa_pipeline_rcra ← PIPELINE_RCRA_00_COMPLETE (flattened RCRA timeline, REGISTRY_ID inline)
    rcra_excl = ["REGISTRY_ID", "SOURCE_ID", "PENALTY_AMOUNT", "ENF_COMP_ACTION_COST",
                 "EVAL_DATE", "VIOL_DETERMINED_DATE", "ACTUAL_RTC_DATE", "VIOL_RTC_DATE",
                 "ENF_ACTION_DATE"]
    specs.append(dict(
        name="epa_pipeline_rcra",
        sources=[dict(archive="pipeline_rcra_downloads.zip", member="PIPELINE_RCRA_00_COMPLETE.csv")],
        sql=_retype(rcra_excl, {
            "REGISTRY_ID": _txt("REGISTRY_ID"), "SOURCE_ID": _txt("SOURCE_ID"),
            "PENALTY_AMOUNT": _money("PENALTY_AMOUNT"),
            "ENF_COMP_ACTION_COST": _money("ENF_COMP_ACTION_COST"),
            "EVAL_DATE": _date("EVAL_DATE"), "VIOL_DETERMINED_DATE": _date("VIOL_DETERMINED_DATE"),
            "ACTUAL_RTC_DATE": _date("ACTUAL_RTC_DATE"), "VIOL_RTC_DATE": _date("VIOL_RTC_DATE"),
            "ENF_ACTION_DATE": _date("ENF_ACTION_DATE")}),
        btree=["REGISTRY_ID", "SOURCE_ID"], bitmap=["FOUND_VIOLATION"],
    ))

    # 10) epa_aim_triggering_events ← aim_triggering_events (active exceedance triggers)
    aim_excl = ["NPDES_ID", "MGP", "THRESHOLD", "ANNUAL_AVERAGE", "ELG",
                "MONITORING_PERIOD_TRIGGERED_STRT", "MONITORING_PERIOD_TRIGGERED_END"]
    specs.append(dict(
        name="epa_aim_triggering_events",
        sources=[dict(archive="aim_triggering_events_dl.zip", member="aim_triggering_events.csv")],
        sql=_retype(aim_excl, {
            "NPDES_ID": _txt("NPDES_ID"), "MGP": _txt("MGP"),
            "THRESHOLD": _num("THRESHOLD"), "ANNUAL_AVERAGE": _num("ANNUAL_AVERAGE"),
            "ELG": _num("ELG"),
            "MONITORING_PERIOD_TRIGGERED_STRT": _date("MONITORING_PERIOD_TRIGGERED_STRT"),
            "MONITORING_PERIOD_TRIGGERED_END": _date("MONITORING_PERIOD_TRIGGERED_END")}),
        btree=["NPDES_ID"], bitmap=["ACTIVE_EXCEPTION"],
    ))

    return specs


def _spec(name: str) -> dict:
    for s in build_specs():
        if s["name"] == name:
            return s
    raise KeyError(f"unknown dataset {name!r}; known: {[s['name'] for s in build_specs()]}")


# --------------------------------------------------------------------------- #
# R2 / S3 helpers
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
    # botocore >=1.36 validates a flexible (CRC32) checksum on GetObject responses by
    # default; on a RANGE GET it compares the partial bytes against the full-object
    # checksum header R2 returns → spurious "checksum did not match". Scope checksums to
    # "when_required" so range reads (our central-directory + member streaming) succeed.
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
    """Minimal seekable file-like over an R2 object via boto3 Range GETs — enough for
    zipfile to parse the central directory (a few tail reads) and locate a member."""

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
    """Random-access extract one ZIP member from R2 and write it as a valid .csv.gz to
    `out_path`, WITHOUT decompressing or recompressing: the ZIP member's stored deflate
    stream IS a gzip body, so we copy its compressed bytes between a 10-byte gzip header
    and an 8-byte trailer carrying the central-directory CRC32 + ISIZE (low 32 bits — valid
    even for >4 GB members, which is exactly what zlib's gzip reader validates against).
    /tmp holds only the COMPRESSED member. ZIP64 offsets are resolved by `zipfile`.
    Returns (uncompressed_size, compress_size)."""
    import struct
    import zipfile

    key = LANDING_PREFIX + archive
    client = _s3_client()
    reader = _S3RangeReader(client, LANDING_BUCKET, key)
    zf = zipfile.ZipFile(reader)
    zi = zf.getinfo(member)
    if zi.compress_type != zipfile.ZIP_DEFLATED:
        raise RuntimeError(f"{archive}:{member} compress_type={zi.compress_type} (expected deflate)")

    # Parse the LOCAL header to find the start of the raw deflate stream (its name/extra
    # lengths can differ from the central directory's).
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


def _dataset_exists(uri: str, so: dict) -> bool:
    import lance

    try:
        lance.dataset(uri, storage_options=so)
        return True
    except Exception:  # noqa: BLE001 — absent / not-found
        return False


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


# --------------------------------------------------------------------------- #
# ops ledger + Trigger callback
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
    """Terminal row → ops.epa_ingest_runs (psycopg). Best-effort; never masks the ingest."""
    import json

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
# Per-dataset materialization (mapped — one container per dataset)
# --------------------------------------------------------------------------- #
@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 120,
    memory=49152,           # headroom for the 24 GB DuckDB read + 69M/49M-row in-memory BTREE sort
    cpu=4.0,
)
def materialize_one(spec: dict, run_id: str) -> dict:
    """Extract each source member → gz → DuckDB transform → STREAM to Lance; index; record.
    Multi-source specs (DMRs) overwrite on the first source and append the rest into one
    dataset. Returns a status dict; records an ops.epa_ingest_runs row either way."""
    import datetime as dt

    import duckdb  # noqa: F401 — ensures the wheel is present in this container
    import lance

    name = spec["name"]
    uri = ACTIVE_BASE + name + "/"
    so = _r2_storage_options()
    archives = ",".join(s["archive"] for s in spec["sources"])
    started = dt.datetime.now(dt.timezone.utc)
    status, error, rows, built = "error", None, 0, []
    os.makedirs(SPILL_DIR, exist_ok=True)
    os.makedirs(SCRATCH_DIR, exist_ok=True)

    try:
        import pyarrow.dataset as pds

        # Write-mode routing (Directive 30). A spec carrying `replace_partition` is a
        # rolling window over an accumulated-history dataset (epa_npdes_dmrs): the live SoR
        # holds FY1982+ appended by materialize_epa_history.py, while this spec's sources
        # are only the recent FY window. Overwriting would truncate that history. So when
        # the dataset already exists, REPLACE the window in place — delete FISCAL_YEAR>=floor
        # (floor=min source FY), then append every source. History (FISCAL_YEAR<floor) is
        # never touched; the op is idempotent on re-run; this is the fleet idiom
        # (hmda/cms/fec/sba). Cold build (no dataset) falls through to the standard overwrite
        # path. Every other spec is a full cumulative-snapshot overwrite.
        replace_col = spec.get("replace_partition")
        existed = _dataset_exists(uri, so)
        if replace_col and existed:
            floor_fy = min(int(s["fy"]) for s in spec["sources"])
            print(f"[{name}] partition-replace: delete {replace_col}>={floor_fy}, then append "
                  f"{len(spec['sources'])} source(s) (history < {floor_fy} preserved)")
            lance.dataset(uri, storage_options=so).delete(f"{replace_col} >= {floor_fy}")
        elif name == "epa_npdes_dmrs" and existed:
            # Belt-and-suspenders: only reachable if replace_partition were removed from the
            # spec. Refuse rather than overwrite an existing populated DMR table.
            live_rows = lance.dataset(uri, storage_options=so).count_rows()
            if live_rows >= DMR_HISTORICAL_FLOOR:
                raise RuntimeError(
                    f"GUARD: epa_npdes_dmrs holds {live_rows:,} rows (>= floor "
                    f"{DMR_HISTORICAL_FLOOR:,}) but replace_partition is unset; refusing to "
                    f"overwrite accumulated FY1982+ history."
                )

        for i, src in enumerate(spec["sources"]):
            gz = f"{SCRATCH_DIR}/{name}_{i}.csv.gz"
            unc, comp = _member_to_gz(src["archive"], src["member"], gz)
            print(f"[{name}] extracted {src['member']} comp={comp/1e6:.0f}MB unc={unc/1e9:.2f}GB → {gz}")

            sql = spec["sql"].replace("__GZ__", gz)
            if "fy" in src:
                sql = sql.replace("__FY__", str(src["fy"]))

            # DuckDB transform → ZSTD parquet (transport; streaming COPY sink, memory bounded
            # by row-group size — does NOT buffer the whole file the way fetch_record_batch did
            # on the 15.9 GB EFF). Then free the gz before reading the parquet back.
            pq = f"{SCRATCH_DIR}/{name}_{i}.parquet"
            con = _new_con()
            try:
                con.execute(
                    f"COPY ({sql}) TO '{pq}' "
                    f"(FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE {PARQUET_ROW_GROUP})"
                )
            finally:
                con.close()
            try:
                os.remove(gz)
            except OSError:
                pass

            # Stream parquet → Lance (bounded by LANCE_READ_BATCH, independent of file size).
            reader = pds.dataset(pq).scanner(batch_size=LANCE_READ_BATCH).to_reader()
            # replace-mode: the window FYs were deleted above → every source appends.
            # otherwise the first source overwrites (full snapshot) and the rest append.
            write_mode = "append" if (replace_col and existed) else ("overwrite" if i == 0 else "append")
            lance.write_dataset(
                reader, uri,
                mode=write_mode,
                data_storage_version=DATA_STORAGE_VERSION,
                max_rows_per_file=MAX_ROWS_PER_FILE,
                storage_options=so,
            )
            try:
                os.remove(pq)
            except OSError:
                pass
            print(f"[{name}] wrote source {i + 1}/{len(spec['sources'])} ({src['member']})")

        rows = lance.dataset(uri, storage_options=so).count_rows()
        if replace_col and existed:
            # Incremental index refresh — extends the EXISTING indices (all of them, incl.
            # any added out-of-band) to the appended fragments. NOT a full rebuild: a
            # create_scalar_index over the whole table trips R2 multipart at this scale
            # (the cause of the 2026-06 reindex failures) and would drop indices absent from
            # the spec lists. optimize_indices touches only the new/changed fragments.
            lance.dataset(uri, storage_options=so).optimize.optimize_indices()
            built = [ix["name"] for ix in lance.dataset(uri, storage_options=so).list_indices()]
            if name == "epa_npdes_dmrs" and rows < DMR_HISTORICAL_FLOOR:
                # Tripwire: a normal replace leaves history (≥350M) intact, so rows<floor means
                # history is missing — surface loudly rather than publish a silently-truncated SoR.
                raise RuntimeError(
                    f"POST-WRITE FLOOR BREACH: epa_npdes_dmrs={rows:,} < floor "
                    f"{DMR_HISTORICAL_FLOOR:,} after partition-replace — investigate."
                )
        else:
            built = _build_indexes(uri, spec.get("btree", []), spec.get("bitmap", []), so)
        print(f"[{name}] committed rows={rows:,} indexes={built}")
        status = "success"
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
        print(f"[{name}] FAILED: {error}")
    finally:
        completed = dt.datetime.now(dt.timezone.utc)
        _record_run(run_id=run_id, dataset=name, dataset_uri=uri, source_archives=archives,
                    rows_written=int(rows), indexes_built=",".join(built), status=status,
                    error=error, metrics=None, started_at=started, completed_at=completed)

    return {"dataset": name, "uri": uri, "rows": int(rows), "status": status,
            "indexes": built, "error": error}


# --------------------------------------------------------------------------- #
# Pattern-B bridge — epa_to_sos_bridge (REGISTRY_ID ↔ sos_normalized_master)
# --------------------------------------------------------------------------- #
SEP = "chr(31)"   # unit-separator join for the composite (source_state, original_entity_id) key


def _zip5(c: str) -> str:
    return f"nullif(left(regexp_replace(CAST({c} AS VARCHAR), '[^0-9]', '', 'g'), 5), '')"


# Public/government entities won't resolve to the commercial SoS registry and would only
# add false positives + join blow-up on ultra-common names. Excluded from the match attempt.
PUBLIC_RE = (
    "(CITY OF|COUNTY OF|TOWN OF|VILLAGE OF|TOWNSHIP|BOROUGH OF|PARISH OF|STATE OF|"
    "COMMONWEALTH OF|UNITED STATES|DEPARTMENT OF|DEPT OF|MUNICIPAL|METROPOLITAN|"
    "WASTEWATER|WASTE WATER|WATER WORKS|WATER TREATMENT|WATER DISTRICT|WATER AUTHORITY|"
    "WATER SYSTEM|WATER POLLUTION|SANITAT|SEWER|SEWERAGE|PUBLIC SCHOOL|SCHOOL DISTRICT|"
    "UNIFIED SCHOOL|UNIVERSITY|COLLEGE|HOUSING AUTHORITY|FIRE DISTRICT|FIRE DEPARTMENT|"
    "PARK DISTRICT|UTILITY DISTRICT|TRANSIT AUTHORITY|AIRPORT AUTHORITY|PORT AUTHORITY|"
    "ARMY|NAVY|AIR FORCE|NATIONAL GUARD|VETERANS|FORT |USDA|^US |^USA |FEDERAL )"
)


def build_bridge_sql() -> str:
    """Final SELECT producing one best row per matched REGISTRY_ID. Assumes the EPA name
    candidates (cand_ok) and the three spine blocking sets (sp_name, sp_name_state,
    sp_base_zip) are already built as TEMP tables. Tiered exact→base+zip→exact+state match
    (recon_ca_ucc_sos.py precedent); name→entity is many-to-many so a deterministic
    representative is chosen and the ambiguity (sos_candidate_count) is preserved."""
    return f"""
WITH matches AS (
    SELECT c.registry_id, c.name_source, c.src_priority, c.raw_name, c.nln,
           1 AS tier_rank, 'base_name_zip' AS match_tier, s.n_ent, s.rep
    FROM cand_ok c JOIN sp_base_zip s ON c.lnb = s.lnb AND c.zip5 = s.zip5
    UNION ALL
    SELECT c.registry_id, c.name_source, c.src_priority, c.raw_name, c.nln,
           2, 'exact_name_state', s.n_ent, s.rep
    FROM cand_ok c JOIN sp_name_state s ON c.nln = s.nln AND c.st = s.st
    UNION ALL
    SELECT c.registry_id, c.name_source, c.src_priority, c.raw_name, c.nln,
           3, 'exact_name', s.n_ent, s.rep
    FROM cand_ok c JOIN sp_name s ON c.nln = s.nln
),
best AS (
    SELECT * FROM matches
    QUALIFY row_number() OVER (
        PARTITION BY registry_id
        ORDER BY tier_rank ASC, src_priority ASC, n_ent ASC, rep ASC) = 1
)
SELECT
    registry_id                                   AS REGISTRY_ID,
    name_source                                   AS epa_name_source,
    raw_name                                      AS epa_matched_name,
    nln                                           AS normalized_legal_name,
    match_tier,
    split_part(rep, {SEP}, 1) || ':' || split_part(rep, {SEP}, 2)  AS sos_company_id,
    split_part(rep, {SEP}, 1)                     AS sos_source_state,
    split_part(rep, {SEP}, 2)                     AS sos_original_entity_id,
    n_ent                                         AS sos_candidate_count,
    CASE
        WHEN match_tier = 'base_name_zip'    AND n_ent = 1 THEN 'high'
        WHEN match_tier = 'exact_name_state' AND n_ent = 1 THEN 'high'
        WHEN match_tier IN ('base_name_zip', 'exact_name_state')   THEN 'medium'
        WHEN match_tier = 'exact_name'       AND n_ent = 1 THEN 'medium'
        ELSE 'low' END                            AS confidence
FROM best
"""


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 90,
    memory=49152,
    cpu=4.0,
)
def build_bridge(run_id: str, trigger_callback_url: str | None = None) -> dict:
    """Resolve commercial EPA entities → sos_normalized_master and emit epa_to_sos_bridge.

    Name priority permit > defendant > facility (ICIS_PERMITS.PERMIT_NAME and
    CASE_DEFENDANTS are true legal-entity names; FRS.FAC_NAME is a site name). Tiers:
    exact `normalized_legal_name`, exact `normalized_legal_name` + state, suffix-stripped
    `legal_name_base` + ZIP5 — all against the spine's stored BTREE blocking keys, applying
    the SAME core.name_norm / core.legal_name_base macros on the EPA side (zero drift).
    The SoS spine carries NO lat/long, so geo disambiguation is state + ZIP5."""
    import datetime as dt

    import lance

    uri = ACTIVE_BASE + "epa_to_sos_bridge/"
    so = _r2_storage_options()
    started = dt.datetime.now(dt.timezone.utc)
    status, error, rows, built, metrics = "error", None, 0, [], {}
    os.makedirs(SPILL_DIR, exist_ok=True)
    os.makedirs(SCRATCH_DIR, exist_ok=True)

    # EPA name-source members (random-access extract; multi-member archives, so only the
    # member is pulled — never the whole zip).
    members = {
        "icis_permits":  ("npdes_downloads.zip", "ICIS_PERMITS.csv"),
        "icis_fac":      ("npdes_downloads.zip", "ICIS_FACILITIES.csv"),
        "case_def":      ("case_downloads.zip", "CASE_DEFENDANTS.csv"),
        "case_fac":      ("case_downloads.zip", "CASE_FACILITIES.csv"),
        "frs_fac":       ("frs_downloads.zip", "FRS_FACILITIES.csv"),
    }
    try:
        con = _new_con()
        try:
            # 1) Land + register each EPA name source.
            for tbl, (arch, mem) in members.items():
                gz = f"{SCRATCH_DIR}/bridge_{tbl}.csv.gz"
                _member_to_gz(arch, mem, gz)
                con.execute(f"CREATE TEMP TABLE {tbl} AS SELECT * FROM {_read(gz)}")
                os.remove(gz)
                print(f"[bridge] loaded {tbl} ({mem})")

            # 2) EPA candidates: registry_id + best name + state/zip, three prioritized sources.
            nn = name_norm("raw_name")
            lb = legal_name_base(name_norm("raw_name"))
            con.execute(f"""
                CREATE TEMP TABLE cand_ok AS
                WITH cand_raw AS (
                    SELECT {_zip5('f.ZIP')} AS zip5, upper(trim(f.STATE_CODE)) AS st,
                           nullif(trim(f.FACILITY_UIN),'') AS registry_id,
                           'permit' AS name_source, 1 AS src_priority, p.PERMIT_NAME AS raw_name
                    FROM icis_permits p JOIN icis_fac f ON f.NPDES_ID = p.EXTERNAL_PERMIT_NMBR
                    WHERE nullif(trim(p.PERMIT_NAME),'') IS NOT NULL
                      AND nullif(trim(f.FACILITY_UIN),'') IS NOT NULL
                    UNION ALL
                    SELECT {_zip5('cf.ZIP')}, upper(trim(cf.STATE_CODE)),
                           nullif(trim(cf.REGISTRY_ID),''), 'defendant', 2, cd.DEFENDANT_NAME
                    FROM case_def cd JOIN case_fac cf ON cf.ACTIVITY_ID = cd.ACTIVITY_ID
                    WHERE nullif(trim(cd.DEFENDANT_NAME),'') IS NOT NULL
                      AND nullif(trim(cf.REGISTRY_ID),'') IS NOT NULL
                    UNION ALL
                    SELECT {_zip5('FAC_ZIP')}, upper(trim(FAC_STATE)),
                           nullif(trim(REGISTRY_ID),''), 'facility', 3, FAC_NAME
                    FROM frs_fac
                    WHERE nullif(trim(FAC_NAME),'') IS NOT NULL
                      AND nullif(trim(REGISTRY_ID),'') IS NOT NULL
                )
                SELECT registry_id, name_source, src_priority, raw_name,
                       {nn} AS nln, {lb} AS lnb, zip5, st
                FROM cand_raw
                WHERE {nn} IS NOT NULL AND NOT regexp_matches({nn}, '{PUBLIC_RE}')
            """)
            for t in members:
                con.execute(f"DROP TABLE {t}")
            n_cand = con.execute("SELECT count(*), count(DISTINCT registry_id) FROM cand_ok").fetchone()
            print(f"[bridge] commercial candidates rows={n_cand[0]:,} distinct_registry={n_cand[1]:,}")

            # 3) Spine blocking sets (one scan of sos_normalized_master, projected).
            # v9 sos_normalized_master persists legal_name_base (BTREE-indexed) — it is the SAME
            # canonical legal_name_base(normalized_legal_name) the spine side used to re-derive,
            # verified byte-identical on live data (0 / 17,926,543 rows differ). Read the stored
            # column directly instead of recomputing the suffix-strip per row. The EPA candidate
            # side still derives lnb from the raw EPA name (cand_ok below) — EPA carries no
            # precomputed base — so the Tier-B base-name join stays byte-identical on both sides.
            con.register("sos_rdr", lance.dataset(SOS_MASTER_URI, storage_options=so).scanner(
                columns=["normalized_legal_name", "legal_name_base", "zip_code",
                         "state", "source_state", "original_entity_id"]).to_reader())
            con.execute(f"""
                CREATE TEMP TABLE spine_raw AS
                SELECT normalized_legal_name AS nln,
                       legal_name_base AS lnb,
                       {_zip5('zip_code')} AS zip5, upper(trim(state)) AS st,
                       source_state || {SEP} || original_entity_id AS rep
                FROM sos_rdr WHERE normalized_legal_name IS NOT NULL
            """)
            con.unregister("sos_rdr")
            con.execute("CREATE TEMP TABLE sp_name AS SELECT nln, count(DISTINCT rep) n_ent, min(rep) rep FROM spine_raw GROUP BY 1")
            con.execute("CREATE TEMP TABLE sp_name_state AS SELECT nln, st, count(DISTINCT rep) n_ent, min(rep) rep FROM spine_raw WHERE st IS NOT NULL GROUP BY 1,2")
            con.execute("CREATE TEMP TABLE sp_base_zip AS SELECT lnb, zip5, count(DISTINCT rep) n_ent, min(rep) rep FROM spine_raw WHERE lnb IS NOT NULL AND zip5 IS NOT NULL GROUP BY 1,2")
            con.execute("DROP TABLE spine_raw")

            # 4) Resolve + pick best per registry_id.
            con.execute(f"CREATE TEMP TABLE bridge AS {build_bridge_sql()}")
            table = con.sql("SELECT * FROM bridge").to_arrow_table()
            rows = table.num_rows
            by_conf = dict(con.execute("SELECT confidence, count(*) FROM bridge GROUP BY 1").fetchall())
            by_tier = dict(con.execute("SELECT match_tier, count(*) FROM bridge GROUP BY 1").fetchall())
            by_src = dict(con.execute("SELECT epa_name_source, count(*) FROM bridge GROUP BY 1").fetchall())
            metrics = {"matched_registry_ids": int(rows), "by_confidence": by_conf,
                       "by_tier": by_tier, "by_name_source": by_src}
            print(f"[bridge] matches={rows:,} {metrics}")
        finally:
            con.close()

        lance.write_dataset(table, uri, mode="overwrite",
                            data_storage_version=DATA_STORAGE_VERSION,
                            max_rows_per_file=MAX_ROWS_PER_FILE, storage_options=so)
        built = _build_indexes(uri, ["REGISTRY_ID", "normalized_legal_name"], [], so)
        status = "success"
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
        print(f"[bridge] FAILED: {error}")
    finally:
        completed = dt.datetime.now(dt.timezone.utc)
        _record_run(run_id=run_id, dataset="epa_to_sos_bridge", dataset_uri=uri,
                    source_archives="frs_downloads.zip,npdes_downloads.zip,case_downloads.zip",
                    rows_written=int(rows), indexes_built=",".join(built), status=status,
                    error=error, metrics=metrics or None, started_at=started, completed_at=completed)
        if trigger_callback_url:
            _post_callback(trigger_callback_url,
                           {"status": status, "rows": int(rows), "feed": FEED + "_bridge"})

    return {"dataset": "epa_to_sos_bridge", "uri": uri, "rows": int(rows),
            "status": status, "metrics": metrics, "error": error}


# --------------------------------------------------------------------------- #
# Orchestrator — fan out materializations (one container each) then the bridge
# --------------------------------------------------------------------------- #
@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 180,
    memory=2048,
)
def run_epa_ingest(trigger_callback_url: str | None = None, only: list[str] | None = None,
                   skip_bridge: bool = False) -> dict:
    """Materialize all (or `only`) datasets in parallel containers, then build the bridge.
    Writes a per-dataset ops row (inside each worker) + a `__run__` summary row, and POSTs
    one terminal callback for the feed."""
    import datetime as dt

    started = dt.datetime.now(dt.timezone.utc)
    specs = build_specs()
    if only:
        specs = [s for s in specs if s["name"] in set(only)]
    run_id = started.strftime("epa_%Y%m%dT%H%M%SZ")

    # Fan out one container per dataset (spawn → get; robust positional arg passing).
    calls = [(s["name"], materialize_one.spawn(s, run_id)) for s in specs]
    results = []
    for name, call in calls:
        try:
            results.append(call.get())
        except Exception as exc:  # noqa: BLE001 — one bad dataset must not sink the batch
            print(f"[orchestrator] {name} raised: {exc}")
            results.append({"dataset": name, "rows": 0, "status": "error", "error": str(exc)})
    if not skip_bridge:
        results.append(build_bridge.remote(run_id=run_id))

    ok = [r for r in results if r.get("status") == "success"]
    bad = [r for r in results if r.get("status") != "success"]
    total_rows = sum(int(r.get("rows", 0)) for r in results)
    status = "success" if not bad else ("partial" if ok else "error")
    completed = dt.datetime.now(dt.timezone.utc)

    _record_run(run_id=run_id, dataset="__run__", dataset_uri=ACTIVE_BASE,
                source_archives=str(len(specs)) + " datasets",
                rows_written=total_rows, indexes_built="",
                status=status, error=(None if not bad else ",".join(r["dataset"] for r in bad)),
                metrics={"datasets": {r["dataset"]: r.get("rows") for r in results},
                         "failed": [r["dataset"] for r in bad]},
                started_at=started, completed_at=completed)
    _post_callback(trigger_callback_url, {"status": status, "rows": total_rows, "feed": FEED})

    summary = {"run_id": run_id, "status": status, "total_rows": total_rows,
               "datasets": {r["dataset"]: {"rows": r.get("rows"), "status": r.get("status")}
                            for r in results}}
    print(summary)
    if status == "error":
        raise RuntimeError(f"EPA ingest failed: {[r['dataset'] for r in bad]}")
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


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=600)
def verify_epa() -> dict:
    """Read-back proof: open each committed dataset from R2, report row count + indices."""
    import lance

    so = _r2_storage_options()
    out = {}
    names = [s["name"] for s in build_specs()] + ["epa_to_sos_bridge"]
    for name in names:
        uri = ACTIVE_BASE + name + "/"
        try:
            ds = lance.dataset(uri, storage_options=so)
            idx = [i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i))
                   for i in ds.list_indices()]
            out[name] = {"rows": ds.count_rows(), "indices": idx,
                         "columns": len(ds.schema), "version": ds.version}
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
def one(name: str) -> None:
    import datetime as dt

    run_id = dt.datetime.now(dt.timezone.utc).strftime("epa_one_%Y%m%dT%H%M%SZ")
    print(materialize_one.remote(_spec(name), run_id))


@app.local_entrypoint()
def bridge() -> None:
    import datetime as dt

    run_id = dt.datetime.now(dt.timezone.utc).strftime("epa_bridge_%Y%m%dT%H%M%SZ")
    print(build_bridge.remote(run_id=run_id))


@app.local_entrypoint()
def run(skip_bridge: bool = False, only: str = "") -> None:
    names = [n for n in only.split(",") if n] or None
    print(run_epa_ingest.remote(trigger_callback_url=None, only=names, skip_bridge=skip_bridge))


@app.local_entrypoint()
def verify() -> None:
    import json

    print(json.dumps(verify_epa.remote(), indent=2, default=str))
