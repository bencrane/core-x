"""Compute worker — EPA NPDES DMR **historical backfill** (FY1982 → FY2023).

Append-only extension of the LIVE Lance SoR ``s3://data-sink/active/epa_npdes_dmrs/``
(currently FY2024–FY2026, 67,597,592 rows) backward to fiscal year 1982. Materializes the
16-archive payload staged at ``s3://data-sink/landing/epa/``:

    npdes_dmrs_prefy2009.zip   NPDES_DMRS_PREFY2009.csv   66,924,459 rows  (FY1982–FY2008)
    npdes_dmrs_fy2009.zip … fy2023.zip                   287,925,385 rows (one FY each)
    ───────────────────────────────────────────────────  ─────────────
    backfill total                                        354,849,844 rows → unified ~422,447,436

Diagnostic of record: ``docs/reference/EPA_NPDES_DMR_HISTORICAL_BACKFILL_PLAN.md``. The source
schema is byte-identical across all 16 eras and to the live SoR, so the transform is the LIVE
``epa_npdes_dmrs`` projection (``dmr_excl`` / ``dmr_recasts``) VERBATIM — only the FISCAL_YEAR
handling differs for the pre-2009 bundle.

──────────────────────────────────────────────────────────────────────────────────────────────
NOT YET EXECUTED. Importing this module fires nothing. Every write requires an explicit local
entrypoint (`modal run … ::backfill`). Gated on final review per directive.
──────────────────────────────────────────────────────────────────────────────────────────────

Build contract (the three authorized constraints):

  1. TRANSFORM — verbatim ``dmr_excl`` / ``dmr_recasts`` typed projection (13 typed cols +
     44 string passthrough). FISCAL_YEAR: per-archive literal for FY2009–FY2023; **per-row
     derivation** ``year(mpe)+(month(mpe)>=10)`` for prefy2009 (bundles FY1982–FY2008).

  2. LOAD — ``mode="append"`` only (the live 67.6 M rows are NEVER overwritten; there is no
     overwrite path in this module). Executed **sequentially, single-writer** (one
     ``append_one.remote()`` at a time → no Lance manifest-version collision). Each archive is
     **ledger-guarded** against ``ops.epa_ingest_runs`` for resumable idempotency.

  3. INDEX — a SINGLE full rebuild (``create_scalar_index(replace=True)``) AFTER all 16 appends, via
     the R2-SAFE LOCAL ROUND-TRIP (``reindex_local``): stage the dataset → local disk, build the
     index there, publish only the new index+manifest files via boto3. A DIRECT Lance→R2 index write
     trips R2's "all non-trailing parts must have the same length" multipart rule once page_data.lance
     is large enough that object_store escalates its part size mid-upload (HTTP 400 InvalidPart; AWS
     S3 tolerates it, R2 does not). boto3/s3transfer uses uniform parts → R2-compliant. The local
     build's DataFusion sort also spills to local NVMe (fast), so this fixes the R2 write AND the spill.

Spill configuration (source-verified against lance-format/lance
``rust/lance-datafusion/src/exec.rs`` and apache/datafusion ``disk_manager.rs``):

  • ``LANCE_BYPASS_SPILLING`` — Lance keys on **PRESENCE, not value**
    (``env::var(...).map(|_| false).unwrap_or(true)``). ANY value — including "false" — forces an
    in-memory sort. It is therefore **completely ABSENT** from the index image. ``reindex_local``
    asserts its absence at runtime and refuses to run otherwise.
  • Spill path — with spilling on, Lance builds ``DiskManagerBuilder::default()`` →
    ``DiskManagerMode::OsTmpDirectory`` → ``tempfile::tempdir()`` → ``std::env::temp_dir()`` →
    honors **TMPDIR**. Spill goes to the worker's LOCAL NVMe (``TMPDIR=/tmp``): the ~tens-of-GB
    sort fits local disk and runs far faster than a networked modal.Volume, so the build finishes
    inside a preemption-free window. Run ``::reindex`` **detached** (``modal run --detach``) so the
    build survives client disconnect and Modal's preemption-retry can complete it server-side.
  • Spill cap — ``LANCE_MAX_TEMP_DIRECTORY_SIZE`` and ``LANCE_MEM_POOL_SIZE`` parse via
    ``s.parse::<u64>()`` = **RAW BYTES**. A "250GB" string fails the parse and silently reverts to
    the 100 GB default. Both are passed as integer byte strings below.

    modal run    pipelines/ingest_epa/materialize_epa_history.py::status      # ledger: which archives done
    modal run    pipelines/ingest_epa/materialize_epa_history.py::append --archive npdes_dmrs_fy2015.zip
    modal run    pipelines/ingest_epa/materialize_epa_history.py::backfill    # 16 sequential appends + reindex
    modal run    pipelines/ingest_epa/materialize_epa_history.py::backfill --skip-index   # appends only
    modal run    pipelines/ingest_epa/materialize_epa_history.py::reindex     # full disk-spilled rebuild only
    modal run    pipelines/ingest_epa/materialize_epa_history.py::verify      # read-back proof
    modal deploy pipelines/ingest_epa/materialize_epa_history.py
"""

from __future__ import annotations

import os

import modal

# --------------------------------------------------------------------------- #
# Constants — identical storage contract to the live epa_npdes_dmrs (do not drift)
# --------------------------------------------------------------------------- #
LANDING_BUCKET = "data-sink"
LANDING_PREFIX = "landing/epa/"
ACTIVE_BASE = os.environ.get("EPA_ACTIVE_BASE", "s3://data-sink/active/")
DATASET = "epa_npdes_dmrs"                 # the LIVE dataset we APPEND into (never create/overwrite)
FEED = "epa_npdes_dmr_history"

DATA_STORAGE_VERSION = "2.1"               # MUST match the live dataset for a clean append
MAX_ROWS_PER_FILE = 1_048_576
PARQUET_ROW_GROUP = 1_048_576              # bounded COPY memory (streaming sink, not a buffer)
LANCE_READ_BATCH = 100_000                 # parquet→Lance streaming batch (size-independent)
DUCKDB_THREADS = 4
# 48 GB (in a 64 GB container): the prefy2009 member is a 23.6 GB single gzip (non-splittable →
# single-threaded all_varchar read of 57 columns), 1.5× the fleet's previous giant (15.9 GB EFF).
# Measured: this file's in-memory transform needs ~42 GB, so 24 GB OOMs the COPY. The other 15
# archives (≤9.4 GB) clear this with wide margin.
DUCKDB_MEMORY_LIMIT = "48GB"
SPILL_DIR = "/tmp/duckdb_spill"            # LOCAL scratch on the append workers (DuckDB temp)
SCRATCH_DIR = "/tmp/epa"                   # LOCAL scratch on the append workers (gz + parquet)

# Index-sort spill target ------------------------------------------------------ #
# DataFusion's DiskManager(OsTmpDirectory) follows TMPDIR. The BTREE external sort for 422 M rows
# spills only ~tens of GB — which fits the worker's LOCAL NVMe — and local disk is *far* faster than
# a networked modal.Volume, so the build finishes inside a preemption-free window instead of being a
# slow-moving target. (A networked-Volume spill ran 40+ min on the heavy index and got preempted
# mid-build; create_scalar_index has no checkpoint, so a preemption restarts it from zero.)
# /tmp here is a plain local dir — Modal only forbids MOUNTING a Volume at /tmp, not writing to it.
SPILL_MOUNT = "/tmp"                         # local NVMe spill dir == TMPDIR
_GiB = 1024 ** 3
# RAW BYTES (parse::<u64>): a "250GB"/"24GB" string would fail the parse and revert to defaults.
LANCE_MAX_TEMP_BYTES = str(250 * _GiB)     # 268435456000  — raise the 100 GB DataFusion cap
LANCE_MEM_POOL_BYTES = str(24 * _GiB)      # 25769803776   — FairSpillPool working set (raises the
                                           #                 100 MB/partition default that crashed lance#2650's merge)

# --------------------------------------------------------------------------- #
# Images
#   base   — NO LANCE_BYPASS_SPILLING (so the index worker can spill to disk).
#   index  — base + TMPDIR/cap/pool env; Volume is mounted on the FUNCTION, not the image.
# --------------------------------------------------------------------------- #
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
    # NOTE: LANCE_BYPASS_SPILLING is deliberately NOT set here. Lance treats the var's mere
    # presence (any value) as "bypass" → in-memory sort. Absence == spill-to-disk enabled.
)

index_image = image.env(
    {
        "TMPDIR": SPILL_MOUNT,                              # → std::env::temp_dir() → DiskManager spill dir
        "LANCE_MAX_TEMP_DIRECTORY_SIZE": LANCE_MAX_TEMP_BYTES,
        "LANCE_MEM_POOL_SIZE": LANCE_MEM_POOL_BYTES,
        # LANCE_BYPASS_SPILLING intentionally absent (see module docstring).
    }
)

# (No modal.Volume: the index sort spills to the worker's local NVMe at TMPDIR=/tmp — faster and
# preemption-resilient. The spill (~tens of GB) is well within the container's default local disk.)

app = modal.App("epa-dmr-history", image=image)

INDEX_PLAN: list[tuple[str, str]] = [
    ("EXTERNAL_PERMIT_NMBR", "BTREE"),          # hub key — high-card string (the heavy spill)
    ("MONITORING_PERIOD_END_DATE", "BTREE"),    # period lookup — date32
    ("FISCAL_YEAR", "BITMAP"),                  # ~45 distinct values (FY1982–FY2026)
]


# --------------------------------------------------------------------------- #
# SQL cast helpers — VERBATIM from materialize_epa.py (zero drift vs the live SoR)
# --------------------------------------------------------------------------- #
def _txt(c: str) -> str:
    return f"nullif(trim({c}),'')"


def _num(c: str) -> str:
    return f"TRY_CAST({_txt(c)} AS DOUBLE)"


def _int(c: str) -> str:
    return f"TRY_CAST({_txt(c)} AS BIGINT)"


def _date(c: str) -> str:
    return f"CAST(try_strptime({_txt(c)}, '%m/%d/%Y') AS DATE)"


def _read(gz_token: str = "__GZ__") -> str:
    # parallel=false: gzip is non-splittable (single-threaded decode regardless) and a handful of
    # DMR free-text fields (PARAMETER_DESC) are quoted — the streaming reader must not split them.
    return (
        f"read_csv('{gz_token}', all_varchar=true, header=true, parallel=false, "
        "compression='gzip', quote='\"', escape='\"', strict_mode=false, ignore_errors=false)"
    )


def _retype(exclude: list[str], recasts: dict[str, str], extra: dict[str, str] | None = None) -> str:
    """SELECT * EXCLUDE(<typed cols>), <typed casts AS same name> [, <extra AS name>].
    EXCLUDE only affects the star expansion; `extra` expressions may still reference the raw
    (pre-recast) columns from the FROM — which is how prefy2009 derives FISCAL_YEAR from the
    raw MONITORING_PERIOD_END_DATE while that same column is also recast to DATE."""
    cols = ", ".join(recasts[c] + f" AS {c}" for c in exclude)
    tail = ("," + ", ".join(f"{v} AS {k}" for k, v in (extra or {}).items())) if extra else ""
    excl = ", ".join(exclude)
    return f"SELECT * EXCLUDE ({excl}), {cols}{tail} FROM {_read()}"


# --------------------------------------------------------------------------- #
# The DMR projection — VERBATIM dmr_excl / dmr_recasts (materialize_epa.py spec #3)
# --------------------------------------------------------------------------- #
DMR_EXCL = [
    "EXTERNAL_PERMIT_NMBR", "LIMIT_VALUE_NMBR", "DMR_VALUE_NMBR",
    "LIMIT_VALUE_STANDARD_UNITS", "DMR_VALUE_STANDARD_UNITS", "EXCEEDENCE_PCT",
    "DAYS_LATE", "MONITORING_PERIOD_END_DATE", "LIMIT_BEGIN_DATE", "LIMIT_END_DATE",
    "VALUE_RECEIVED_DATE", "RNC_DETECTION_DATE", "RNC_RESOLUTION_DATE",
]
DMR_RECASTS = {
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

# prefy2009 FISCAL_YEAR — derived per row from the RAW MONITORING_PERIOD_END_DATE (100% populated;
# 0 null dates measured). EPA FY = calendar year + 1 for Oct–Dec. Range observed: FY1982–FY2008.
_MPE = "try_strptime(nullif(trim(MONITORING_PERIOD_END_DATE),''), '%m/%d/%Y')"
FY_DERIVE_SQL = f"CAST(year({_MPE}) + CASE WHEN month({_MPE}) >= 10 THEN 1 ELSE 0 END AS INTEGER)"


def build_history_sources() -> list[dict]:
    """16 append units, chronological (prefy2009 first → fragments stay time-clustered)."""
    srcs: list[dict] = []
    srcs.append(dict(
        archive="npdes_dmrs_prefy2009.zip", member="NPDES_DMRS_PREFY2009.csv", fy="DERIVE",
        sql=_retype(DMR_EXCL, DMR_RECASTS, extra={"FISCAL_YEAR": FY_DERIVE_SQL}),
    ))
    for fy in range(2009, 2024):  # FY2009 … FY2023 (one fiscal year per archive)
        srcs.append(dict(
            archive=f"npdes_dmrs_fy{fy}.zip", member=f"NPDES_DMRS_FY{fy}.csv", fy=fy,
            sql=_retype(DMR_EXCL, DMR_RECASTS, extra={"FISCAL_YEAR": f"CAST({fy} AS INTEGER)"}),
        ))
    return srcs


def _source(archive: str) -> dict:
    for s in build_history_sources():
        if s["archive"] == archive:
            return s
    raise KeyError(f"unknown archive {archive!r}; known: {[s['archive'] for s in build_history_sources()]}")


# --------------------------------------------------------------------------- #
# R2 / S3 — VERBATIM from materialize_epa.py (random-access ZIP member extract)
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
    return boto3.client(
        "s3",
        endpoint_url=so["endpoint"],
        aws_access_key_id=so["aws_access_key_id"],
        aws_secret_access_key=so["aws_secret_access_key"],
        region_name="auto",
        config=Config(
            signature_version="s3v4",
            request_checksum_calculation="when_required",   # range GETs vs full-object checksum
            response_checksum_validation="when_required",
            retries={"max_attempts": 5, "mode": "standard"},
        ),
    )


class _S3RangeReader:
    """Seekable file-like over an R2 object via boto3 Range GETs — enough for zipfile to parse
    the central directory (ZIP64-safe) and locate a member without pulling the whole archive."""

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
    """Random-access extract one ZIP member from R2 → valid .csv.gz WITHOUT decompress/recompress
    (the stored deflate stream IS a gzip body between a synthesized 10-byte header and an 8-byte
    CRC/ISIZE trailer). /tmp holds only the COMPRESSED member. Returns (uncompressed, compressed)."""
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


def _new_con():
    import duckdb

    con = duckdb.connect(":memory:")
    con.execute(f"SET threads TO {DUCKDB_THREADS}")
    con.execute(f"SET memory_limit='{DUCKDB_MEMORY_LIMIT}'")
    con.execute(f"SET temp_directory='{SPILL_DIR}'")
    con.execute("SET preserve_insertion_order=false")
    return con


# --------------------------------------------------------------------------- #
# ops.epa_ingest_runs ledger — reuse the EXISTING table (no schema change)
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


def _pg_connect():
    """Best-effort pooled connection. Returns None (never raises) if the DSN is unset OR the
    connection fails — e.g. the Supabase pooler hitting `EMAXCONNSESSION max clients ... pool_size`.
    The ops ledger is audit state, not the system of record; a pool hiccup must never fail or mask
    the ingest/index work."""
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* access.")
        return None
    try:
        return psycopg.connect(dsn, connect_timeout=10)
    except Exception as exc:  # noqa: BLE001 — ledger is best-effort; never raise from here
        print(f"WARN: ops DB connect failed ({type(exc).__name__}); skipping ops.* write.")
        return None


def _archive_done(archive: str) -> bool:
    """Resumable idempotency: True iff this archive already appended successfully (any run)."""
    conn = _pg_connect()
    if conn is None:
        return False
    try:
        with conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)  # self-bootstrap; harmless if present
            cur.execute(
                "SELECT 1 FROM ops.epa_ingest_runs "
                "WHERE dataset=%s AND source_archives=%s AND status='success' "
                "AND feed=%s LIMIT 1",
                (DATASET, archive, FEED),
            )
            return cur.fetchone() is not None
    except Exception as exc:  # noqa: BLE001 — a guard-read failure must not skip work
        print(f"WARN: ledger guard read failed ({exc}); treating archive as NOT done.")
        return False
    finally:
        conn.close()


def _record_run(*, run_id, source_archives, rows_written, indexes_built, status, error,
                metrics, started_at, completed_at) -> None:
    import json  # noqa: F401 — Jsonb handles serialization
    from psycopg.types.json import Jsonb

    conn = _pg_connect()
    if conn is None:
        return
    uri = ACTIVE_BASE + DATASET + "/"
    try:
        with conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
            cur.execute(
                """
                INSERT INTO ops.epa_ingest_runs
                    (run_id, feed, dataset, dataset_uri, source_archives, rows_written,
                     indexes_built, status, error, metrics, started_at, completed_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (run_id, FEED, DATASET, uri, source_archives, rows_written, indexes_built,
                 status, error, Jsonb(metrics) if metrics is not None else None,
                 started_at, completed_at),
            )
    except Exception as exc:  # noqa: BLE001 — audit must not mask the ingest
        print(f"WARN: ops.epa_ingest_runs write failed: {exc}")
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# Append worker — ONE archive, mode="append" only, ledger-guarded. No index build.
# --------------------------------------------------------------------------- #
@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 180,
    memory=65536,        # 64 GB — headroom for the prefy2009 23.6 GB single-gzip transform (DuckDB
                         # capped at 48 GB; leaves ~16 GB for decode buffers + the Lance write step)
    cpu=4.0,
)
def append_one(src: dict, run_id: str, force: bool = False) -> dict:
    """Extract member → gz → DuckDB COPY→parquet (streaming) → stream parquet → Lance APPEND.
    The decompressed CSV never materializes in RAM. Writes an ops row; raises on failure so the
    sequential orchestrator halts (never silently skips an archive)."""
    import datetime as dt

    import lance
    import pyarrow.dataset as pds

    archive, member = src["archive"], src["member"]
    uri = ACTIVE_BASE + DATASET + "/"
    so = _r2_storage_options()
    started = dt.datetime.now(dt.timezone.utc)
    os.makedirs(SPILL_DIR, exist_ok=True)
    os.makedirs(SCRATCH_DIR, exist_ok=True)

    if not force and _archive_done(archive):
        print(f"[{archive}] already appended (ledger success) — skip.")
        return {"archive": archive, "status": "skipped", "rows": 0}

    status, error, rows_before, rows_after, written = "error", None, 0, 0, 0
    try:
        # The dataset MUST already exist — this module only APPENDS to the live SoR, never creates.
        # count_rows() raises if the URI is absent → fail loud rather than fabricate a dataset.
        rows_before = lance.dataset(uri, storage_options=so).count_rows()

        gz = f"{SCRATCH_DIR}/{member}.csv.gz"
        unc, comp = _member_to_gz(archive, member, gz)
        print(f"[{archive}] extracted {member} comp={comp/1e6:.0f}MB unc={unc/1e9:.2f}GB")

        pq = f"{SCRATCH_DIR}/{member}.parquet"
        con = _new_con()
        try:
            con.execute(
                f"COPY ({src['sql'].replace('__GZ__', gz)}) TO '{pq}' "
                f"(FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE {PARQUET_ROW_GROUP})"
            )
        finally:
            con.close()
        try:
            os.remove(gz)
        except OSError:
            pass

        reader = pds.dataset(pq).scanner(batch_size=LANCE_READ_BATCH).to_reader()
        # ── HARD CONSTRAINT: append only. There is no mode="overwrite" anywhere in this module. ──
        lance.write_dataset(
            reader, uri, mode="append",
            data_storage_version=DATA_STORAGE_VERSION,
            max_rows_per_file=MAX_ROWS_PER_FILE, storage_options=so,
        )
        try:
            os.remove(pq)
        except OSError:
            pass

        rows_after = lance.dataset(uri, storage_options=so).count_rows()
        written = rows_after - rows_before
        status = "success"
        print(f"[{archive}] APPENDED rows={written:,}  ({rows_before:,} → {rows_after:,})")
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
        print(f"[{archive}] FAILED: {error}")
    finally:
        completed = dt.datetime.now(dt.timezone.utc)
        _record_run(run_id=run_id, source_archives=archive,
                    rows_written=int(written if status == "success" else 0),
                    indexes_built="", status=status, error=error,
                    metrics={"mode": "append", "rows_before": int(rows_before),
                             "rows_after": int(rows_after), "fiscal_year": src.get("fy")},
                    started_at=started, completed_at=completed)

    if status != "success":
        raise RuntimeError(f"append_one failed for {archive}: {error}")
    return {"archive": archive, "status": status, "rows": int(written)}


# --------------------------------------------------------------------------- #
# R2-safe index build — LOCAL round-trip.
# Writing a scalar index DIRECTLY to R2 trips R2's "all non-trailing parts must have the same
# length" multipart rule once page_data.lance is large enough that object_store ESCALATES its
# adaptive part size mid-upload (HTTP 400 InvalidPart). AWS S3 tolerates unequal parts; R2 does
# NOT. At 67.6 M rows the index stayed under that threshold; at 422 M it does not. Proven fleet
# fix (pipelines/hmda, pdl_companies, cms_open_payments, fmcsa, sam): stage the dataset → LOCAL
# disk, build the index there (no multipart), then publish ONLY the new files (index dirs + new
# manifest) via boto3 — whose s3transfer uses UNIFORM parts, which R2 accepts. The 57.8 GB data
# fragments are already in R2 and unchanged, so they are never re-uploaded. The local build's
# DataFusion sort also spills to local NVMe (fast), so this fixes the R2 write AND the spill.
# --------------------------------------------------------------------------- #
_ACTIVE_PREFIX = "active/" + DATASET + "/"            # key prefix within LANDING_BUCKET ("data-sink")


def _download_r2_prefix(s3, prefix: str, local_dir: str, workers: int = 8) -> set[str]:
    """Stage every object under an R2 prefix → local disk (parallel). Returns the relative keys
    already in R2 so the publish step skips re-uploading unchanged data fragments."""
    import shutil
    from concurrent.futures import ThreadPoolExecutor

    shutil.rmtree(local_dir, ignore_errors=True)
    keys: list[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=LANDING_BUCKET, Prefix=prefix):
        for o in page.get("Contents", []):
            rel = o["Key"][len(prefix):]
            if rel:
                keys.append(rel)

    def _dl(rel: str) -> None:
        lp = os.path.join(local_dir, rel.replace("/", os.sep))
        os.makedirs(os.path.dirname(lp), exist_ok=True)
        s3.download_file(LANDING_BUCKET, prefix + rel, lp)

    with ThreadPoolExecutor(max_workers=workers) as ex:
        list(ex.map(_dl, keys))
    return set(keys)


def _upload_new_files(s3, prefix: str, local_dir: str, existing: set[str]) -> int:
    """Upload local files whose relative key is NOT already in R2 (the freshly-built index files +
    the new manifest). boto3/s3transfer = uniform-part multipart → R2-safe. Manifest/version files
    upload LAST so the new version only resolves once every file it references is present — a kill
    mid-upload leaves the prior committed version intact, never a dangling manifest."""
    new: list[tuple[str, str]] = []
    for root, _, files in os.walk(local_dir):
        for f in files:
            lp = os.path.join(root, f)
            rel = os.path.relpath(lp, local_dir).replace(os.sep, "/")
            if rel not in existing:
                new.append((rel, lp))
    # False (index/data) sorts before True (manifest/version) → manifests upload last.
    new.sort(key=lambda t: ("_versions/" in t[0] or t[0].endswith(".manifest"), t[0]))
    for rel, lp in new:
        s3.upload_file(lp, LANDING_BUCKET, prefix + rel)
    return len(new)


@app.function(
    image=index_image,                                   # TMPDIR=/tmp + cap + pool for the LOCAL sort spill; NO bypass var
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 60 * 6,
    memory=65536,                                        # 64 GB; the LOCAL create_scalar_index sort spills to local disk
    cpu=16.0,
    ephemeral_disk=524288,                               # 512 GiB — holds the 57.8 GB dataset + index-build scratch
    retries=2,                                           # idempotent (replace=True) → safe to retry on preemption/transient
)
def reindex_local(run_id: str) -> dict:
    """R2-SAFE full index rebuild. Stage active/epa_npdes_dmrs/ → local disk, build all 3 indices
    LOCALLY (no R2 multipart), publish ONLY the new index + manifest files via boto3 (uniform-part
    multipart → R2-compliant). SELF-FINALIZING: re-reads R2 (count, index manifest, FISCAL_YEAR
    span), records a terminal ledger row, prints DONE — VERIFIED. Run DETACHED so it survives client
    disconnect and Modal completes it server-side."""
    import datetime as dt

    import lance

    bypass = os.environ.get("LANCE_BYPASS_SPILLING")
    if bypass is not None:
        raise RuntimeError(f"LANCE_BYPASS_SPILLING set to {bypass!r}; must be ABSENT for disk spilling.")
    os.makedirs(SPILL_MOUNT, exist_ok=True)
    os.makedirs(SPILL_DIR, exist_ok=True)

    uri = ACTIVE_BASE + DATASET + "/"
    so = _r2_storage_options()
    s3 = _s3_client()
    local = "/tmp/epa_npdes_dmrs_local"
    started = dt.datetime.now(dt.timezone.utc)
    status, error, built, verify_out = "error", None, [], {}
    print(
        f"[reindex_local] preflight | TMPDIR={os.environ.get('TMPDIR')} "
        f"LANCE_MAX_TEMP_DIRECTORY_SIZE={os.environ.get('LANCE_MAX_TEMP_DIRECTORY_SIZE')} "
        f"LANCE_MEM_POOL_SIZE={os.environ.get('LANCE_MEM_POOL_SIZE')} "
        f"LANCE_BYPASS_SPILLING={bypass!r} (absent=good)"
    )
    try:
        print(f"[reindex_local] staging s3://{LANDING_BUCKET}/{_ACTIVE_PREFIX} → {local} …")
        existing = _download_r2_prefix(s3, _ACTIVE_PREFIX, local)
        print(f"[reindex_local] staged {len(existing)} files to local disk")

        ds = lance.dataset(local)              # LOCAL — index writes go to disk, never R2 multipart
        total = ds.count_rows()
        cols = set(ds.schema.names)
        print(f"[reindex_local] local dataset rows={total:,} — building indices on local NVMe")
        for col, kind in INDEX_PLAN:
            if col not in cols:
                raise RuntimeError(f"index column {col!r} absent from local schema")
            print(f"[reindex_local] building {kind} on {col} …")
            ds.create_scalar_index(col, index_type=kind, replace=True)
            built.append(f"{col}:{kind}")
            print(f"[reindex_local]   {kind} ✓ {col} (local)")

        published = _upload_new_files(s3, _ACTIVE_PREFIX, local, existing)
        print(f"[reindex_local] published {published} new files (index + manifest) via boto3 → R2")
        status = "success"

        # ── self-verify against the published R2 version ─────────────────────────────────────────
        try:
            ds2 = lance.dataset(uri, storage_options=so)
            idx = [i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i))
                   for i in ds2.list_indices()]
            con = _new_con()
            try:
                con.register("dmr_fy", ds2.scanner(columns=["FISCAL_YEAR"]).to_reader())
                fy_min, fy_max = con.execute("SELECT min(FISCAL_YEAR), max(FISCAL_YEAR) FROM dmr_fy").fetchone()
            finally:
                con.close()
            verify_out = {"rows": ds2.count_rows(), "indices": idx, "published": published,
                          "fiscal_year_min": fy_min, "fiscal_year_max": fy_max}
            print(f"[reindex_local] ✅ DONE — VERIFIED {verify_out}")
        except Exception as vexc:  # noqa: BLE001 — verify is a read-back proof, not the publish itself
            verify_out = {"verify_error": str(vexc), "published": published}
            print(f"[reindex_local] published OK; read-back verify hiccup: {vexc}")
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
        print(f"[reindex_local] FAILED: {error}")
    finally:
        completed = dt.datetime.now(dt.timezone.utc)
        _record_run(run_id=run_id, source_archives="(reindex)", rows_written=0,
                    indexes_built=",".join(built), status=status, error=error,
                    metrics={"reindex": True, "r2_safe_local_roundtrip": True,
                             "index_plan": [f"{c}:{k}" for c, k in INDEX_PLAN], "verify": verify_out},
                    started_at=started, completed_at=completed)

    if status != "success":
        raise RuntimeError(f"reindex_local failed: {error}")
    return {"dataset": DATASET, "indexes": built, "status": status, "verify": verify_out}


# --------------------------------------------------------------------------- #
# Orchestrator — SEQUENTIAL single-writer appends, then one full reindex.
# --------------------------------------------------------------------------- #
@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 60 * 18,
    memory=2048,
)
def run_backfill(only: list[str] | None = None, skip_index: bool = False, force: bool = False) -> dict:
    """16 append units, ONE AT A TIME (``.remote()`` blocks → serialized → no manifest collision),
    then — only if every append succeeded — a SINGLE full disk-spilled index rebuild."""
    import datetime as dt

    started = dt.datetime.now(dt.timezone.utc)
    run_id = started.strftime("epa_dmr_hist_%Y%m%dT%H%M%SZ")

    srcs = build_history_sources()
    if only:
        keep = set(only)
        srcs = [s for s in srcs if s["archive"] in keep]

    appended, skipped = [], []
    for s in srcs:
        # .remote() is a BLOCKING call — the next archive does not start until this commit lands.
        r = append_one.remote(s, run_id, force)
        (skipped if r.get("status") == "skipped" else appended).append(r["archive"])
        print(f"[backfill] {r['archive']}: {r['status']} (+{r.get('rows', 0):,} rows)")

    index_result = None
    if not skip_index:
        index_result = reindex_local.remote(run_id)   # R2-safe local round-trip (not direct-to-R2)

    completed = dt.datetime.now(dt.timezone.utc)
    summary = {
        "run_id": run_id,
        "appended": appended,
        "skipped": skipped,
        "indexes": index_result,
        "skip_index": skip_index,
    }
    _record_run(run_id=run_id, source_archives="__run__",
                rows_written=0, indexes_built=",".join(f"{c}:{k}" for c, k in INDEX_PLAN) if not skip_index else "",
                status="success", error=None, metrics=summary,
                started_at=started, completed_at=completed)
    print(summary)
    return summary


# --------------------------------------------------------------------------- #
# Ops + verification
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


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=120)
def status() -> dict:
    """Which historical archives have already appended (resumability view)."""
    conn = _pg_connect()
    if conn is None:
        raise RuntimeError("HQX_DB_URL_POOLED not set.")
    out = {"done": [], "all": [s["archive"] for s in build_history_sources()]}
    try:
        with conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
            cur.execute(
                "SELECT source_archives, max(completed_at) FROM ops.epa_ingest_runs "
                "WHERE dataset=%s AND feed=%s AND status='success' AND source_archives LIKE 'npdes_dmrs%%' "
                "GROUP BY 1 ORDER BY 1", (DATASET, FEED))
            out["done"] = [{"archive": a, "at": str(t)} for a, t in cur.fetchall()]
    finally:
        conn.close()
    out["remaining"] = [a for a in out["all"] if a not in {d["archive"] for d in out["done"]}]
    print(out)
    return out


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=600, memory=16384)
def verify() -> dict:
    """Read-back proof over the LIVE dataset: row count, index manifest, FISCAL_YEAR span.
    The FY span is a streaming DuckDB aggregate over the projected column (constant memory —
    never materializes the 422 M-row dataset)."""
    import lance

    so = _r2_storage_options()
    uri = ACTIVE_BASE + DATASET + "/"
    ds = lance.dataset(uri, storage_options=so)
    idx = [i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i)) for i in ds.list_indices()]

    con = _new_con()
    try:
        con.register("dmr_fy", ds.scanner(columns=["FISCAL_YEAR"]).to_reader())
        fy_min, fy_max, fy_null = con.execute(
            "SELECT min(FISCAL_YEAR), max(FISCAL_YEAR), count(*) FILTER (WHERE FISCAL_YEAR IS NULL) FROM dmr_fy"
        ).fetchone()
    finally:
        con.close()

    out = {
        "rows": ds.count_rows(),
        "indices": idx,
        "version": ds.version,
        "fiscal_year_min": fy_min,
        "fiscal_year_max": fy_max,
        "fiscal_year_null": fy_null,
    }
    print(out)
    return out


# --------------------------------------------------------------------------- #
# Local entrypoints — NOTHING runs on import. Each must be invoked explicitly.
# --------------------------------------------------------------------------- #
@app.local_entrypoint()
def init() -> None:
    print(init_ops.remote())


@app.local_entrypoint()
def append(archive: str, force: bool = False) -> None:
    import datetime as dt

    run_id = dt.datetime.now(dt.timezone.utc).strftime("epa_dmr_hist_one_%Y%m%dT%H%M%SZ")
    print(append_one.remote(_source(archive), run_id, force))


@app.local_entrypoint()
def backfill(only: str = "", skip_index: bool = False, force: bool = False) -> None:
    names = [n for n in only.split(",") if n] or None
    print(run_backfill.remote(only=names, skip_index=skip_index, force=force))


@app.local_entrypoint()
def reindex() -> None:
    import datetime as dt

    run_id = dt.datetime.now(dt.timezone.utc).strftime("epa_dmr_hist_reindex_%Y%m%dT%H%M%SZ")
    print(reindex_local.remote(run_id))


@app.local_entrypoint()
def show_status() -> None:
    import json

    print(json.dumps(status.remote(), indent=2, default=str))


@app.local_entrypoint()
def verify_backfill() -> None:
    import json

    print(json.dumps(verify.remote(), indent=2, default=str))
