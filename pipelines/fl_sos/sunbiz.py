"""Compute worker — Florida SoS (Sunbiz) corporate registry bulk ingest.

Part of the ``fl-sos-corporations`` Modal app. Endpoint-less functions, spawned by
the Universal Dispatcher (core/modal_dispatcher.py) or driven by the local
entrypoints. Clean-room data plane: no Iceberg, no Polaris — DuckDB does 100% of the
transform, Lance is written straight to R2.

Topology (proven by the diagnostic probe). Two fixed-width payloads in R2 landing,
two distinct Lance datasets joined on ``document_number``:

    cordata.zip  -> cordata0.txt (1.69 GiB, 1,260,599 rows)  master entity spine
                 -> s3://data-sink/active/fl_sos_corporations/
    corevent.zip -> corevt.txt   (9.60 GiB, 14,455,118 rows) child event history
                 -> s3://data-sink/active/fl_sos_events/

Three load-bearing diagnostic findings shaped this worker:
  1. cordata.zip is ZIP **compression method 9 (Deflate64)** — Python's stdlib
     zipfile/zlib CANNOT decode it. The explode phase shells out to **p7zip** (7z),
     which reads Deflate64. corevent.zip is ordinary Deflate (stdlib-capable) but is
     extracted by the same 7z path for uniformity.
  2. The "1442 / 664 byte" record sizes are **physical line lengths incl. CRLF**.
     The data window is **1440 / 662** chars; every substring offset lives there and
     never touches the trailing \\r\\n.
  3. Both files are **pure 7-bit ASCII** (full-member byte census: 0 bytes > 0x7F) —
     NO cp1252 transcode (unlike SBA/SAM). Python streams bytes untouched; DuckDB
     does the shaping.

Parsing strategy (approved Option A): DuckDB ``read_csv`` ingests each 1440/662-byte
line as ONE VARCHAR column (delimiter ``\\x01``, a byte absent from the clean-ASCII
payload; quote/escape disabled), then ``substring()`` projects the fixed-width fields
in SQL — 100% DuckDB transform, ``.to_arrow_table()``, identifiers strictly VARCHAR,
no VARIANT. The 6-officer repeating group is left unparsed as ``raw_officer_block``
(per directive; parse downstream when needed); the inter-block report region is kept
losslessly as ``raw_report_block`` so no source bytes are dropped.

Two-phase ingest (mirrors the CA SoS precedent):
  Phase 1 — explode_zip (Python I/O only): R2 zip -> 7z extract member -> stream
     zstd-compress -> upload s3://data-sink/landing/fl_sos/members/<member>.zst
  Phase 2 — ingest_master / ingest_events (DuckDB 100% transform): landed .zst ->
     DuckDB read_csv(compression='zstd', single col) -> substring projection ->
     con.execute(...).to_arrow_table() -> lance.write_dataset(overwrite, v2.1) ->
     BTREE + BITMAP scalar indexes. The two datasets are DISTINCT, so Phase 2 fans
     out (master ‖ events) with no shared-writer manifest conflict.

Control plane (Trigger v4 durable callback): each function accepts
``trigger_callback_url`` and, on terminal state, (1) writes a run row to
``ops.fl_sos_runs`` via psycopg and (2) POSTs a FLAT JSON body to that url. No
``{"data": ...}`` envelope.

    modal deploy pipelines/fl_sos/sunbiz.py
    modal run    pipelines/fl_sos/sunbiz.py::init_state            # create ops.fl_sos_runs
    modal run    pipelines/fl_sos/sunbiz.py::run_all               # explode + parallel ingest
    modal run    pipelines/fl_sos/sunbiz.py::explode               # Phase 1 only
    modal run    pipelines/fl_sos/sunbiz.py::ingest --target master|events
    modal run    pipelines/fl_sos/sunbiz.py::reindex --target events
"""

from __future__ import annotations

import os

import modal

BUCKET = "data-sink"
LANDING_PREFIX = "landing/fl_sos/"
LANDING_MEMBERS_PREFIX = "landing/fl_sos/members/"
SCRATCH_DIR = "/tmp"
AS_OF_DEFAULT = "2026-05-31"

# Lance system-of-record tier (env-overridable). Two datasets, joined on document_number.
CORP_URI = os.environ.get("FL_SOS_CORP_LANCE_URI", "s3://data-sink/active/fl_sos_corporations/")
EVENTS_URI = os.environ.get("FL_SOS_EVENTS_LANCE_URI", "s3://data-sink/active/fl_sos_events/")

# source registry: logical target -> source zip, ZIP member (Deflate64 for master),
# durable per-member .zst landing key, Lance dataset URI.
SOURCES: dict[str, dict[str, str]] = {
    "master": {"zip": "cordata.zip", "member": "cordata0.txt",
               "zst_key": f"{LANDING_MEMBERS_PREFIX}cordata0.txt.zst", "uri": CORP_URI},
    "events": {"zip": "corevent.zip", "member": "corevt.txt",
               "zst_key": f"{LANDING_MEMBERS_PREFIX}corevt.txt.zst", "uri": EVENTS_URI},
}

# Lance fragment sizing (directive constraints).
#   max_rows_per_file = 1048576 — exact.
#   max_bytes_per_file: the directive wrote `90 * 10243` annotated "(90 GiB)". The only
#   reading that equals 90 GiB is `90 * 1024**3` (= 96,636,764,160) — Lance's documented
#   default and the constant used by every other worker in this fleet (NY/CA SoS, PPP).
#   A literal `90 * 10243` (~900 KB) would shatter the 14.4M-row events dataset into
#   thousands of fragments. Honor the stated "90 GiB".
MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3
DATA_STORAGE_VERSION = "2.1"

# ── Fixed-width field maps (1-based start, length within the data window). ──
# Offsets EMPIRICALLY LOCKED against the date_filed anchor across 8 diverse records.
# kind ∈ {"str","date","raw"}. date wire format MMDDYYYY. "raw" right-trims only
# (preserves internal byte offsets for downstream re-parse); "str" full-trims.
# document_number is 6-char legacy or 12-char modern → strictly VARCHAR, never cast.
MASTER_COLS: list[tuple[str, int, int, str]] = [
    ("document_number", 1, 12, "str"),
    ("corporate_name", 13, 192, "str"),
    ("status", 205, 1, "str"),
    ("filing_type", 206, 15, "str"),
    ("principal_address_1", 221, 42, "str"),
    ("principal_address_2", 263, 42, "str"),
    ("principal_city", 305, 28, "str"),
    ("principal_state", 333, 2, "str"),
    ("principal_zip", 335, 10, "str"),
    ("principal_country", 345, 2, "str"),
    ("mailing_address_1", 347, 42, "str"),
    ("mailing_address_2", 389, 42, "str"),
    ("mailing_city", 431, 28, "str"),
    ("mailing_state", 459, 2, "str"),
    ("mailing_zip", 461, 10, "str"),
    ("mailing_country", 471, 2, "str"),
    ("date_filed", 473, 8, "date"),
    ("fei_number", 481, 14, "str"),
    ("raw_report_block", 495, 50, "raw"),     # flag + last-action date + state + annual reports
    ("registered_agent_name", 545, 42, "str"),
    ("registered_agent_type", 587, 1, "str"),
    ("registered_agent_address_1", 588, 42, "str"),
    ("registered_agent_city", 630, 28, "str"),
    ("registered_agent_state", 658, 2, "str"),
    ("registered_agent_zip", 660, 9, "str"),
    ("raw_officer_block", 669, 772, "raw"),    # 6-officer repeating group (669..1440)
]

EVENTS_COLS: list[tuple[str, int, int, str]] = [
    ("document_number", 1, 12, "str"),
    ("event_seq", 13, 5, "str"),
    ("event_code", 18, 10, "str"),
    ("event_description", 28, 58, "str"),
    ("event_date", 86, 8, "date"),
    ("raw_event_detail", 94, 569, "raw"),      # trailing detail (94..662), mostly blank
]

TARGET_COLS = {"master": MASTER_COLS, "events": EVENTS_COLS}

# Scalar index plan (approved). BTREE for high-cardinality resolution keys; BITMAP for
# low-cardinality categoricals. Built post-write.
INDEX_PLAN: dict[str, dict[str, list[str]]] = {
    "master": {"btree": ["document_number", "corporate_name"],
               "bitmap": ["status", "filing_type"]},
    "events": {"btree": ["document_number"], "bitmap": ["event_code"]},
}

# Mirrored verbatim by pipelines/fl_sos/ops_fl_sos_runs.sql. Applied by init_state.
OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.fl_sos_runs (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    phase          text        NOT NULL,
    target         text,
    dataset_uri    text,
    as_of          date,
    source_zip     text,
    landing_key    text,
    rows_processed bigint,
    rejected_rows  bigint,
    status         text        NOT NULL,
    error          text,
    started_at     timestamptz,
    completed_at   timestamptz,
    recorded_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS fl_sos_runs_target_idx      ON ops.fl_sos_runs (target);
CREATE INDEX IF NOT EXISTS fl_sos_runs_phase_idx       ON ops.fl_sos_runs (phase);
CREATE INDEX IF NOT EXISTS fl_sos_runs_status_idx      ON ops.fl_sos_runs (status);
CREATE INDEX IF NOT EXISTS fl_sos_runs_recorded_at_idx ON ops.fl_sos_runs (recorded_at DESC);
"""

image = modal.Image.debian_slim(python_version="3.12").apt_install(
    "p7zip-full",            # 7z — decodes cordata.zip's Deflate64 (method 9); stdlib cannot
).pip_install(
    "duckdb>=1.5,<2",        # >=1.5 guarantees to_arrow_table; <2 stays below the v2.0 break
    "lancedb>=0.15",
    "pylance>=7",            # provides `import lance`; lancedb does not re-export it
    "pyarrow>=17",
    "boto3>=1.35",           # R2 landing read/write
    "zstandard>=0.22",       # per-member .zst landing compression
    "psycopg[binary]>=3.2",  # ops.* terminal state
).env(
    {"LANCE_BYPASS_SPILLING": "true"}  # in-memory BTREE sort (lance-format/lance#2650)
)

app = modal.App("fl-sos-corporations", image=image)


def _r2_storage_options() -> dict[str, str]:
    """object_store options for Cloudflare R2, sourced from the Modal secret."""
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
    """boto3 S3 client for R2. checksum behaviour forced to ``when_required`` (R2 semantics)."""
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


def _sevenzip() -> str:
    import shutil
    for cand in ("7z", "7za", "7zr"):
        path = shutil.which(cand)
        if path:
            return path
    raise RuntimeError("p7zip not found on PATH (expected from apt p7zip-full)")


def _build_sql(cols: list[tuple[str, int, int, str]]) -> str:
    """100% of the transform. read_csv ingests each fixed-width line as ONE VARCHAR
    column (delim=\\x01 absent from the clean-ASCII payload; quote/escape off;
    compression=zstd reads the landed .zst directly), then substring() projects the
    fixed-width fields. ? placeholders bind, in textual order, [csv_path, source_file,
    snapshot_date]. substring lengths cap at the data window so the trailing CRLF is
    never read. NO cp1252 transcode (source is pure ASCII)."""
    delim = "\x01"
    parts: list[str] = []
    for name, start, length, kind in cols:
        seg = f"substring(raw, {start}, {length})"
        if kind == "date":
            parts.append(
                f"TRY_CAST(TRY_STRPTIME(nullif(trim({seg}), ''), '%m%d%Y') AS DATE) AS {name}")
        elif kind == "raw":
            parts.append(f"nullif(rtrim({seg}), '') AS {name}")
        else:
            parts.append(f"nullif(trim({seg}), '') AS {name}")
    projection = ",\n    ".join(parts)
    return (
        "WITH raw_t AS (\n"
        "  SELECT raw FROM read_csv(?,\n"
        "    columns={'raw': 'VARCHAR'},\n"
        f"    delim='{delim}', quote='', escape='', header=false,\n"
        "    compression='zstd', strict_mode=false, null_padding=true,\n"
        "    store_rejects=true, max_line_size=4000000)\n"
        ")\n"
        f"SELECT\n    {projection},\n"
        "    ? AS source_file,\n"
        "    CAST(? AS DATE) AS snapshot_date,\n"
        "    now() AS ingested_at\n"
        "FROM raw_t\n"
    )


def _build_indexes(target: str, so: dict) -> list[str]:
    """Build BTREE + BITMAP scalar indexes for one dataset. create_scalar_index defaults
    to replace=True → idempotent. An index miss must not fail the load."""
    import lance

    ds = lance.dataset(SOURCES[target]["uri"], storage_options=so)
    built: list[str] = []
    for col in INDEX_PLAN[target]["btree"]:
        try:
            ds.create_scalar_index(col, index_type="BTREE", replace=True)
            built.append(f"BTREE:{col}")
            print(f"  BTREE  ✓ {col}")
        except Exception as exc:  # noqa: BLE001
            print(f"  WARN BTREE {col} failed: {exc}")
    for col in INDEX_PLAN[target]["bitmap"]:
        try:
            ds.create_scalar_index(col, index_type="BITMAP", replace=True)
            built.append(f"BITMAP:{col}")
            print(f"  BITMAP ✓ {col}")
        except Exception as exc:  # noqa: BLE001
            print(f"  WARN BITMAP {col} failed: {exc}")
    return built


def _record_run(phase, target, dataset_uri, as_of, source_zip, landing_key, rows,
                rejected, status, error, started_at, completed_at) -> None:
    """Terminal run row → ops.fl_sos_runs (psycopg). Best-effort: never let an
    audit-write failure crash an otherwise-good ingest."""
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* state write.")
        return
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops.fl_sos_runs
                    (phase, target, dataset_uri, as_of, source_zip, landing_key,
                     rows_processed, rejected_rows, status, error, started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (phase, target, dataset_uri, as_of, source_zip, landing_key,
                 rows, rejected, status, error, started_at, completed_at),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: ops.* state write failed: {exc}")


def _post_callback(url, payload, attempts: int = 3) -> None:
    """POST terminal metadata to the Trigger waitpoint URL. FLAT JSON body — no
    ``{"data": ...}`` envelope."""
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
def apply_state_schema() -> dict:
    """Apply the idempotent ops.fl_sos_runs DDL. Run once before the first ingest."""
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        raise RuntimeError("HQX_DB_URL_POOLED not set in the hqx-postgres secret.")
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(OPS_DDL)
        conn.commit()
    print("Applied ops.fl_sos_runs schema.")
    return {"status": "success", "table": "ops.fl_sos_runs"}


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 60,
    memory=8192,
    cpu=4.0,
)
def explode_zip(as_of: str = AS_OF_DEFAULT, trigger_callback_url: str | None = None) -> dict:
    """Phase 1 — Python I/O ONLY. For each source: download the R2 landing zip, 7z-extract
    the member (Deflate64-capable), stream zstd-compress, and re-land as .zst. No parsing."""
    import datetime as dt
    import os.path
    import subprocess

    import zstandard as zstd

    started_at = dt.datetime.now(dt.timezone.utc)
    status, error = "error", None
    landed: dict[str, str] = {}
    try:
        zbin = _sevenzip()
        s3 = _s3_client()
        for target, meta in SOURCES.items():
            src_key = f"{LANDING_PREFIX}{meta['zip']}"
            local_zip = os.path.join(SCRATCH_DIR, meta["zip"])
            zst_path = os.path.join(SCRATCH_DIR, f"{meta['member']}.zst")
            print(f"Downloading s3://{BUCKET}/{src_key} -> {local_zip}")
            s3.download_file(BUCKET, src_key, local_zip)

            # 7z -so streams the (Deflate64) member to stdout; pipe straight into zstd.
            print(f"7z extract {meta['member']} | zstd -> {zst_path}")
            proc = subprocess.Popen([zbin, "e", "-so", local_zip, meta["member"]],
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            cctx = zstd.ZstdCompressor(level=10)
            with open(zst_path, "wb") as fo, cctx.stream_writer(fo) as comp:
                while True:
                    chunk = proc.stdout.read(16 << 20)
                    if not chunk:
                        break
                    comp.write(chunk)
            rc = proc.wait()
            if rc != 0:
                raise RuntimeError(f"7z exited {rc} for {meta['member']}: "
                                   f"{proc.stderr.read()[:300]!r}")

            s3.upload_file(zst_path, BUCKET, meta["zst_key"])
            landed[target] = meta["zst_key"]
            size = os.path.getsize(zst_path)
            print(f"landed {target}: {meta['member']} -> s3://{BUCKET}/{meta['zst_key']} "
                  f"({size / 1024**2:.1f} MiB zst)")
            for p in (local_zip, zst_path):
                try:
                    os.remove(p)
                except OSError:
                    pass
        status = "success"
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
        status = "error"
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run("explode", None, None, as_of, None, None, None, None,
                    status, error, started_at, completed_at)
        _post_callback(trigger_callback_url,
                       {"status": status, "phase": "explode", "as_of": as_of, "members": landed})

    if status != "success":
        raise RuntimeError(f"fl_sos explode failed: {error}")
    return {"status": status, "phase": "explode", "as_of": as_of, "members": landed}


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 90,
    memory=32768,
    cpu=8.0,
)
def ingest_target(target: str, as_of: str = AS_OF_DEFAULT,
                  trigger_callback_url: str | None = None) -> dict:
    """Phase 2 — download the landed .zst → DuckDB read_csv(zstd, single col) → substring
    project/cast → Arrow → Lance overwrite → BTREE/BITMAP indexes; record ops.* + wake
    Trigger. Re-raises on failure so the Modal call is marked failed."""
    import datetime as dt
    import os.path

    import duckdb
    import lance

    target = target.strip().lower()
    if target not in SOURCES:
        raise ValueError(f"target must be one of {sorted(SOURCES)}, got {target!r}")

    meta = SOURCES[target]
    dataset_uri = meta["uri"]
    landing_key = meta["zst_key"]
    source_file = meta["member"]
    started_at = dt.datetime.now(dt.timezone.utc)
    rows, rejected = 0, 0
    status, error = "error", None
    built: list[str] = []

    try:
        so = _r2_storage_options()
        s3 = _s3_client()
        zst_path = os.path.join(SCRATCH_DIR, f"{source_file}.zst")
        print(f"Downloading s3://{BUCKET}/{landing_key} -> {zst_path}")
        s3.download_file(BUCKET, landing_key, zst_path)

        con = duckdb.connect(":memory:")
        try:
            con.execute("PRAGMA threads=8;")
            con.execute("SET enable_progress_bar=false;")
            con.execute("SET memory_limit='26GB';")
            con.execute("SET temp_directory='/tmp/duckdb_spill';")
            table = con.execute(
                _build_sql(TARGET_COLS[target]), [zst_path, source_file, as_of]
            ).to_arrow_table()
            rows = table.num_rows
            try:
                rj = con.execute("SELECT count(*) AS n FROM reject_errors").to_arrow_table()
                rejected = int(rj.to_pylist()[0]["n"])
            except Exception:  # noqa: BLE001 — table absent ⇒ zero rejects
                rejected = 0
        finally:
            con.close()
        print(f"{target}: parsed {rows:,} rows, {rejected:,} rejected")

        lance.write_dataset(
            table,
            dataset_uri,
            mode="overwrite",
            data_storage_version=DATA_STORAGE_VERSION,
            max_rows_per_file=MAX_ROWS_PER_FILE,
            max_bytes_per_file=MAX_BYTES_PER_FILE,
            storage_options=so,
        )
        del table
        print(f"{target}: wrote Lance dataset -> {dataset_uri}")
        built = _build_indexes(target, so)
        status = "success"
    except Exception as exc:  # noqa: BLE001 — terminal handling below + re-raise
        error = str(exc)
        status = "error"
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run("ingest", target, dataset_uri, as_of, meta["zip"], landing_key,
                    int(rows), int(rejected), status, error, started_at, completed_at)
        _post_callback(trigger_callback_url,
                       {"status": status, "phase": "ingest", "target": target,
                        "rows": int(rows), "rejected_rows": int(rejected),
                        "dataset_uri": dataset_uri, "as_of": as_of})

    if status != "success":
        raise RuntimeError(f"fl_sos ingest failed for target={target}: {error}")
    return {"status": status, "phase": "ingest", "target": target,
            "rows_processed": int(rows), "rejected_rows": int(rejected),
            "indices": built, "dataset_uri": dataset_uri, "as_of": as_of}


@app.function(secrets=[modal.Secret.from_name("r2-credentials")],
              timeout=60 * 60, memory=32768, cpu=8.0)
def reindex_target(target: str) -> dict:
    """(Re)build the scalar indexes on an already-written dataset (no re-ingest)."""
    target = target.strip().lower()
    if target not in SOURCES:
        raise ValueError(f"target must be one of {sorted(SOURCES)}, got {target!r}")
    built = _build_indexes(target, _r2_storage_options())
    return {"target": target, "dataset_uri": SOURCES[target]["uri"],
            "indexes": built, "index_count": len(built)}


@app.local_entrypoint()
def init_state() -> None:
    """Create ops.fl_sos_runs (idempotent)."""
    print(apply_state_schema.remote())


@app.local_entrypoint()
def explode(as_of: str = AS_OF_DEFAULT) -> None:
    """Phase 1 — explode both source zips into per-member .zst landing artifacts."""
    print(explode_zip.remote(as_of, trigger_callback_url=None))


@app.local_entrypoint()
def ingest(target: str, as_of: str = AS_OF_DEFAULT) -> None:
    """Phase 2 — ingest a single target (master|events) into its Lance dataset."""
    print(ingest_target.remote(target, as_of=as_of, trigger_callback_url=None))


@app.local_entrypoint()
def run_all(as_of: str = AS_OF_DEFAULT) -> None:
    """End-to-end manual run: explode, then ingest master ‖ events in PARALLEL (distinct
    datasets → no shared-writer conflict). Prints final row counts."""
    import json

    print("=== Phase 1 — explode ===")
    print(json.dumps(explode_zip.remote(as_of, trigger_callback_url=None), indent=2, default=str))

    print("\n=== Phase 2 — parallel ingest ===")
    calls = {t: ingest_target.spawn(t, as_of=as_of, trigger_callback_url=None) for t in SOURCES}
    results: dict[str, dict] = {}
    for t, call in calls.items():
        results[t] = call.get()
        print(json.dumps(results[t], default=str))

    print("\n=== FINAL ROW COUNTS ===")
    total = 0
    for t, r in results.items():
        n = r.get("rows_processed", 0)
        total += n
        print(f"  {t:10s} rows={n:>12,}  rejected={r.get('rejected_rows'):>6,}  -> {r.get('dataset_uri')}")
    print(f"  {'TOTAL':10s} rows={total:>12,}")


@app.local_entrypoint()
def reindex(target: str = "") -> None:
    """Rebuild scalar indexes on existing dataset(s) (no re-ingest). Default: all."""
    import json

    for t in ([target] if target else list(SOURCES)):
        print(json.dumps(reindex_target.remote(t), default=str))
