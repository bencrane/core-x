"""Compute worker — FEC Individual Contributions bulk ingest (1980 → 2026).

Part of the ``fec-contributions`` Modal app. Endpoint-less; spawned by the
Universal Dispatcher (core/modal_dispatcher.py) or driven by the local
entrypoints. Ingests the FEC bulk ``indiv{YY}.zip`` releases — itemized
individual contributions for every even-year election cycle 1980-2026 — into a
SINGLE unified Lance dataset partitioned by an explicit ``cycle_year`` column.

Three-phase hybrid (approved topology + strategy):
  Phase 1 — fetch_indiv_to_landing (FAN-OUT, parallel; Python I/O only):
      FEC https indiv{YY}.zip (~4 GiB for recent cycles, GovCloud S3 redirect)
        → requests stream            → /tmp/indiv{YY}.zip
        → zipfile: extract the EXACT canonical member 'itcont.txt'
          (the zip ALSO bundles 32 redundant by_date/*.txt splits — pinning the
           root member by exact name avoids ~2x double-counting)
        → zstd recompress            → /tmp/indiv{YY}.txt.zst
        → boto3 upload               → s3://data-sink/landing/fec_indiv/indiv{YY}.txt.zst

  Phase 2 — ingest_indiv_cycle (SEQUENTIAL; one writer per dataset → no Lance OCC
            commit conflict; DuckDB does 100% of the transform):
      R2 landing .zst
        → boto3 get + zstd decompress + cp1252→UTF-8 transcode → /tmp utf8 csv
        → DuckDB read_csv(delim='|', header=false, 21 injected cols, strict)
          → 21-col project/cast + cycle_year/source_file/ingested_at  (100% in SQL)
        → to_arrow_reader (streaming; the 2024 cycle alone is ~58M rows)
        → idempotent: delete WHERE cycle_year=N, then Lance append (v2.1)

  Phase 3 — build_fec_indiv_indexes (ONCE, post-backfill):
      BTREE on the load-bearing resolution keys + BITMAP on the categoricals,
      over the complete ~400-480M-row unified dataset (LANCE_BYPASS_SPILLING).

Reconnaissance (verified across cycles 1980 / 1996 / 2008 / 2024):
  • Layout: 21 columns, NO header row, one record per line — IDENTICAL across all
    eras (strict fail-loud parse passes; parsed rows == newline count, 0 rejects).
    Older cycles leave late-added columns (entity_tp, occupation, tran_id,
    file_num, memo_cd, other_id, transaction_pgi) EMPTY — handled as NULL by
    nullif(trim()); no COALESCE / schema-alignment needed.
  • Encoding: PURE 7-bit ASCII in every probed cycle (0 bytes in 0x80-0x9F). The
    cp1252→UTF-8 transcode is therefore a NO-OP, retained as approved insurance
    against an un-probed cycle (it is lossless for ASCII).
  • Dates: TRANSACTION_DT is MMDDYYYY (len 8) in every cycle → TRY_STRPTIME('%m%d%Y').
  • Identifiers VARCHAR (mandate + verified): sub_id (19-digit), zip_code
    (4.8M leading-zero rows in 2024 alone), cmte_id, other_id, tran_id, image_num,
    file_num. A numeric cast would corrupt leading zeros / precision. No VARIANT.

    modal deploy pipelines/fec/indiv_contributions.py
    modal run    pipelines/fec/indiv_contributions.py::initdb               # create ops.* ledger
    modal run    pipelines/fec/indiv_contributions.py::land                 # Phase 1: fan-out land all 24 cycles
    modal run    pipelines/fec/indiv_contributions.py::land --only 1980     # land one cycle
    modal run    pipelines/fec/indiv_contributions.py::backfill             # Phase 2: sequential ingest all 24
    modal run    pipelines/fec/indiv_contributions.py::backfill --only 2024 # ingest one cycle
    modal run    pipelines/fec/indiv_contributions.py::index                # Phase 3: build indexes once
    modal run    pipelines/fec/indiv_contributions.py::show_ledger
"""

from __future__ import annotations

import os

import modal

BUCKET = "data-sink"
LANDING_PREFIX = "landing/fec_indiv/"
# Unified Lance system-of-record tier (NOT the raw landing zone). Env-overridable.
DATASET_URI = os.environ.get(
    "FEC_INDIV_LANCE_URI", "s3://data-sink/active/fec_individual_contributions/"
)
SCRATCH_DIR = "/tmp"
FEED = "fec_indiv"

# Even-year election cycles 1980-2026 (24). The 2-digit suffix is the indiv{YY}.zip
# stamp; the full year is the cycle_year partition key and the FEC URL path segment.
CYCLE_YEARS = list(range(1980, 2027, 2))

# Lance fragment sizing (directive constraints).
#   max_rows_per_file = 1048576 — exact.
#   max_bytes_per_file: the directive wrote `90 * 10243` annotated "(90 GiB)". The
#   only reading that equals 90 GiB is `90 * 1024**3` (= 96,636,764,160) — Lance's
#   documented default and the constant used by the existing PPP / FOIA / SoS
#   workers. A literal `90 * 10243` (~900 KB) would shatter the ~450M-row dataset
#   into hundreds of thousands of fragments. Honor the stated "90 GiB".
MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3
# Net-new dataset → pin the current Lance default (per 02_lancedb_storage.md §2.3).
DATA_STORAGE_VERSION = "2.1"

# Scalar index plan (approved). Built ONCE post-backfill by build_fec_indiv_indexes
# (NOT per-cycle: a sequential 24-cycle append would otherwise rebuild indexes 24
# times and each append invalidates them). BTREE for high-cardinality load-bearing
# resolution keys (equality + range predicates); BITMAP for low-cardinality
# categoricals filtered frequently (per 02_lancedb_storage.md §6).
FEC_BTREE_INDEXES = [
    "sub_id",          # FEC transaction id — primary resolution key (~near-unique)
    "name",            # contributor-name resolution (the headline donor lookup)
    "cmte_id",         # recipient committee FK
    "other_id",        # linked committee/candidate FK (earmarks / transfers)
    "employer",        # employer resolution (high cardinality)
    "transaction_dt",  # temporal range scans (BTREE supports range predicates)
]
FEC_BITMAP_INDEXES = [
    "cycle_year",        # 24 cycles — logical partition pruning
    "entity_tp",         # ~8 values (IND dominates)
    "state",             # ~60-80 (states + territories + foreign + APO/FPO)
    "rpt_tp",            # ~26 report types
    "transaction_tp",    # ~21 transaction types
    "transaction_pgi",   # primary/general/special × year (~140)
    "amndt_ind",         # 3 — new / amend / terminate
    "memo_cd",           # 2
]

# 21 source columns in itcont.txt order (NO header in the data) → snake_case.
# kind ∈ {"str", "date", "amt"}. Every identifier stays VARCHAR. No VARIANT.
_COLUMNS: list[tuple[str, str, str]] = [
    ("CMTE_ID", "cmte_id", "str"),
    ("AMNDT_IND", "amndt_ind", "str"),
    ("RPT_TP", "rpt_tp", "str"),
    ("TRANSACTION_PGI", "transaction_pgi", "str"),
    ("IMAGE_NUM", "image_num", "str"),
    ("TRANSACTION_TP", "transaction_tp", "str"),
    ("ENTITY_TP", "entity_tp", "str"),
    ("NAME", "name", "str"),
    ("CITY", "city", "str"),
    ("STATE", "state", "str"),
    ("ZIP_CODE", "zip_code", "str"),
    ("EMPLOYER", "employer", "str"),
    ("OCCUPATION", "occupation", "str"),
    ("TRANSACTION_DT", "transaction_dt", "date"),
    ("TRANSACTION_AMT", "transaction_amt", "amt"),
    ("OTHER_ID", "other_id", "str"),
    ("TRAN_ID", "tran_id", "str"),
    ("FILE_NUM", "file_num", "str"),
    ("MEMO_CD", "memo_cd", "str"),
    ("MEMO_TEXT", "memo_text", "str"),
    ("SUB_ID", "sub_id", "str"),
]

# Terminal-state ledger. CANONICAL COPY lives at pipelines/fec/ops_fec_indiv_runs.sql;
# kept in sync here as the OPS_DDL constant (applied via the `initdb` entrypoint).
# CREATE SCHEMA + CREATE TABLE run as SEPARATE statements (one command per execute()).
_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS ops.fec_indiv_runs (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed            text        NOT NULL,
    dataset_uri     text        NOT NULL,
    cycle_year      smallint    NOT NULL,
    source_file     text        NOT NULL,
    landing_key     text,
    rows_processed  bigint,
    status          text        NOT NULL,
    error           text,
    started_at      timestamptz NOT NULL,
    completed_at    timestamptz NOT NULL,
    recorded_at     timestamptz NOT NULL DEFAULT now()
)
"""

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",       # >=1.5 guarantees to_arrow_table/to_arrow_reader; <2 stays below the v2.0 break
    "lancedb>=0.15",
    "pylance>=7",           # provides `import lance`; lancedb does not re-export it
    "pyarrow>=17",
    "requests>=2.32",       # FEC stream → landing + Trigger waitpoint callback
    "boto3>=1.35",          # R2 landing get/put
    "zstandard>=0.22",      # .txt.zst landing (re)compression
    "psycopg[binary]>=3.2",  # ops.* terminal state
).env(
    # BTREE training sorts the column; bypass Lance's bounded spill-to-disk
    # ExternalSorter, whose memory accounting under-sizes the pool and exhausts on
    # large string columns. At ~450M rows the `name` / `sub_id` / `employer` sorts
    # are the ceiling; the in-memory path uses the indexer container's RAM. See
    # lance-format/lance#2650.
    {"LANCE_BYPASS_SPILLING": "true"}
)

app = modal.App("fec-contributions", image=image)


def _yy(year: int) -> str:
    """2-digit cycle suffix used in the indiv{YY}.zip filename (1980→'80', 2002→'02')."""
    return f"{year % 100:02d}"


def _zip_url(year: int) -> str:
    return f"https://www.fec.gov/files/bulk-downloads/{year}/indiv{_yy(year)}.zip"


def _landing_key(year: int) -> str:
    return f"{LANDING_PREFIX}indiv{_yy(year)}.txt.zst"


def _source_file(year: int) -> str:
    return f"indiv{_yy(year)}.zip"


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


def _download_decompress_transcode(s3, key: str, out_path: str) -> int:
    """Stream the landed .zst from R2, zstd-decompress, cp1252→UTF-8 transcode, and
    write UTF-8 to scratch. Returns the newline count (the row count — exact for the
    one-record-per-line FEC files, verified == parsed rows). cp1252 is single-byte
    so 8 MiB chunk boundaries never split a character; the transcode is a no-op for
    the pure-ASCII FEC bodies but is retained as approved insurance. I/O only — the
    SQL still does 100% of the transform."""
    import zstandard as zstd

    obj = s3.get_object(Bucket=BUCKET, Key=key)
    body = obj["Body"]
    dctx = zstd.ZstdDecompressor()
    newlines = 0
    with open(out_path, "wb") as out, dctx.stream_reader(body) as reader:
        while True:
            chunk = reader.read(8 << 20)
            if not chunk:
                break
            newlines += chunk.count(b"\n")
            out.write(chunk.decode("cp1252", errors="replace").encode("utf-8"))
    return newlines


def _build_sql(csv_path: str, cycle_year: int, source_file: str) -> str:
    """100% of the transform. read_csv with 21 INJECTED column names (the data has
    no header), delim='|', every column VARCHAR → defensive snake_case projection.
    Strict parse (ignore_errors=false, null_padding=false): the layout is verified
    clean across 1980-2024, so any structural drift in an un-probed cycle fails LOUD
    rather than silently dropping rows. All interpolated values are repo-controlled
    (our /tmp path, an int cycle, fixed literals) — no user input — and single-quote
    escaped regardless."""

    def lit(s: str) -> str:
        return s.replace("'", "''")

    def strcol(src: str, dst: str) -> str:
        return f'nullif(trim("{src}"), \'\') AS {dst}'

    def datecol(src: str, dst: str) -> str:
        # FEC wire format MMDDYYYY (verified len 8, 0 parse failures across cycles).
        return f"TRY_CAST(TRY_STRPTIME(nullif(trim(\"{src}\"), ''), '%m%d%Y') AS DATE) AS {dst}"

    def amtcol(src: str, dst: str) -> str:
        # Whole-dollar signed money (verified 0 uncastable; range -7M..90M). Exact
        # DECIMAL over float to avoid money rounding.
        return f"TRY_CAST(nullif(trim(\"{src}\"), '') AS DECIMAL(14,2)) AS {dst}"

    builders = {"str": strcol, "date": datecol, "amt": amtcol}
    projections = [builders[kind](src, dst) for src, dst, kind in _COLUMNS]
    cols_map = "{" + ", ".join(f"'{src}': 'VARCHAR'" for src, _, _ in _COLUMNS) + "}"
    projection_sql = ",\n    ".join(projections)
    return f"""
WITH raw AS (
    SELECT *
    FROM read_csv(
        '{lit(csv_path)}',
        delim = '|',
        header = false,
        columns = {cols_map},
        quote = '',
        escape = '',
        ignore_errors = false,
        null_padding = false
    )
)
SELECT
    {projection_sql},
    CAST({int(cycle_year)} AS SMALLINT) AS cycle_year,
    '{lit(source_file)}' AS source_file,
    now() AS ingested_at
FROM raw
"""


def _append_idempotent(reader, cycle_year: int, so: dict) -> None:
    """Append this cycle's batches to the unified dataset, first deleting any prior
    rows for the same cycle_year so re-runs are idempotent. Creates the dataset on
    the first write. Run SERIALLY (one writer per dataset) — concurrent writers to
    one Lance dataset can hit OCC commit conflicts."""
    import lance

    common = dict(
        schema=reader.schema,  # REQUIRED when the source is a RecordBatchReader
        data_storage_version=DATA_STORAGE_VERSION,
        max_rows_per_file=MAX_ROWS_PER_FILE,
        max_bytes_per_file=MAX_BYTES_PER_FILE,
        storage_options=so,
    )
    try:
        ds = lance.dataset(DATASET_URI, storage_options=so)
    except Exception:
        ds = None

    if ds is None:
        lance.write_dataset(reader, DATASET_URI, mode="create", **common)
        return
    try:
        ds.delete(f"cycle_year = {int(cycle_year)}")
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: pre-append delete for cycle_year={cycle_year} failed (continuing): {exc}")
    lance.write_dataset(reader, DATASET_URI, mode="append", **common)


def _create_indexes(so: dict) -> list[str]:
    """Build the BTREE + BITMAP scalar indexes on the unified dataset. replace=True
    → idempotent rebuild. Per-column try/except: at ~450M rows a single heavy BTREE
    (name) must not abort the others — an index miss is logged, not fatal."""
    import lance

    ds = lance.dataset(DATASET_URI, storage_options=so)
    built: list[str] = []
    for col in FEC_BTREE_INDEXES:
        try:
            ds.create_scalar_index(col, "BTREE", replace=True)
            built.append(f"BTREE:{col}")
            print(f"  BTREE  ✓ {col}")
        except Exception as exc:  # noqa: BLE001 — an index miss must not fail the load
            print(f"  WARN: BTREE index on {col} failed: {exc}")
    for col in FEC_BITMAP_INDEXES:
        try:
            ds.create_scalar_index(col, "BITMAP", replace=True)
            built.append(f"BITMAP:{col}")
            print(f"  BITMAP ✓ {col}")
        except Exception as exc:  # noqa: BLE001
            print(f"  WARN: BITMAP index on {col} failed: {exc}")
    return built


def _list_committed_indices(ds) -> list:
    """Best-effort read of committed scalar indices (name/type/fields). Tolerant of
    pylance return-shape drift (dict vs object, list_indices vs list_indexes)."""
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


def _record_run(cycle_year, source_file, landing_key, rows, status, error,
                started_at, completed_at) -> None:
    """Terminal run row → ops.fec_indiv_runs (psycopg). Best-effort: never let an
    audit-write failure crash an otherwise-good ingest."""
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* write.")
        return
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops.fec_indiv_runs
                    (feed, dataset_uri, cycle_year, source_file, landing_key,
                     rows_processed, status, error, started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (FEED, DATASET_URI, int(cycle_year), source_file, landing_key,
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
    """Create the ops schema + fec_indiv_runs ledger (idempotent)."""
    import psycopg

    dsn = os.environ["HQX_DB_URL_POOLED"]
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS ops;")
        cur.execute(_CREATE_TABLE_SQL)
        conn.commit()
    print("ops.fec_indiv_runs ready")
    return {"status": "ok", "table": "ops.fec_indiv_runs"}


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials")],
    timeout=60 * 45,
    memory=4096,
    cpu=2.0,
    retries=2,            # FEC GovCloud is the fragile hop; idempotent overwrite of the landing key
    max_containers=8,     # be polite to FEC under the 24-way fan-out
    ephemeral_disk=524288,  # hold the ~4 GiB zip + ~2.3 GiB .zst (recent cycles)
)
def fetch_indiv_to_landing(year: int, trigger_callback_url: str | None = None) -> dict:
    """Phase 1 — Python I/O ONLY. Stream indiv{YY}.zip from FEC, extract the EXACT
    canonical member 'itcont.txt' (NOT the by_date/ splits), zstd-recompress, and
    upload to the R2 landing zone. No parse, no transform here."""
    import os.path
    import zipfile

    import requests
    import zstandard as zstd

    yy = _yy(year)
    url = _zip_url(year)
    key = _landing_key(year)
    zip_path = os.path.join(SCRATCH_DIR, f"indiv{yy}.zip")
    zst_path = os.path.join(SCRATCH_DIR, f"indiv{yy}.txt.zst")

    zip_bytes = 0
    with requests.get(url, stream=True, timeout=(30, 1800)) as resp:
        resp.raise_for_status()
        with open(zip_path, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=1 << 20):
                if chunk:
                    fh.write(chunk)
                    zip_bytes += len(chunk)

    zf = zipfile.ZipFile(zip_path)
    if "itcont.txt" not in zf.namelist():
        raise RuntimeError(
            f"{year}: canonical 'itcont.txt' not in zip members {zf.namelist()[:6]}"
        )
    rows = 0
    cctx = zstd.ZstdCompressor(level=10)
    with zf.open("itcont.txt", "r") as src, open(zst_path, "wb") as fo, \
            cctx.stream_writer(fo) as comp:
        while True:
            b = src.read(8 << 20)
            if not b:
                break
            rows += b.count(b"\n")
            comp.write(b)

    s3 = _s3_client()
    s3.upload_file(zst_path, BUCKET, key)
    for p in (zip_path, zst_path):
        try:
            os.remove(p)
        except OSError:
            pass

    print(f"Landed {_source_file(year)}: zip={zip_bytes} B, ~{rows} rows → s3://{BUCKET}/{key}")
    result = {"year": year, "landing_key": key, "zip_bytes": zip_bytes,
              "rows": rows, "feed": FEED, "status": "success"}
    _post_callback(trigger_callback_url, result)
    return result


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 90,
    memory=16384,
    cpu=4.0,
    ephemeral_disk=524288,  # hold the ~10 GiB transcoded UTF-8 CSV (2024 cycle)
)
def ingest_indiv_cycle(
    key: str,
    cycle_year: int,
    trigger_callback_url: str | None = None,
) -> dict:
    """Phase 2 — landed .zst → decompress + transcode → DuckDB project/cast →
    streaming Arrow → idempotent (delete-cycle + append) into the unified Lance
    dataset, then record ops.* state and wake the Trigger run. SEQUENTIAL by design
    (single writer per dataset). Re-raises on failure so the Modal call is failed."""
    import datetime as dt
    import os.path

    import duckdb

    started_at = dt.datetime.now(dt.timezone.utc)
    cycle_year = int(cycle_year)
    yy = _yy(cycle_year)
    source_file = f"indiv{yy}.zip"
    rows = 0
    status = "error"
    error: str | None = None

    try:
        so = _r2_storage_options()
        s3 = _s3_client()
        utf8_path = os.path.join(SCRATCH_DIR, f"indiv{yy}.utf8.csv")
        newlines = _download_decompress_transcode(s3, key, utf8_path)

        con = duckdb.connect(":memory:")
        try:
            con.execute("PRAGMA threads=4;")
            con.execute("SET enable_progress_bar=false;")
            # Streaming reader: the 2024 cycle alone is ~58M rows — never materialize
            # the whole Arrow table. Lance consumes the reader fragment-by-fragment.
            reader = con.sql(_build_sql(utf8_path, cycle_year, source_file)).to_arrow_reader(
                MAX_ROWS_PER_FILE
            )
            _append_idempotent(reader, cycle_year, so)
        finally:
            con.close()
        try:
            os.remove(utf8_path)
        except OSError:
            pass

        rows = newlines  # exact for the one-record-per-line FEC files (verified)
        status = "success"
    except Exception as exc:  # noqa: BLE001 — terminal handling below + re-raise
        error = str(exc)
        status = "error"
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run(cycle_year, source_file, key, int(rows), status, error,
                    started_at, completed_at)
        _post_callback(
            trigger_callback_url,
            {"status": status, "rows": int(rows), "feed": FEED,
             "cycle_year": cycle_year, "source_file": source_file},
        )

    if status != "success":
        raise RuntimeError(f"fec_indiv ingest failed for cycle {cycle_year} ({key}): {error}")
    return {"feed": FEED, "cycle_year": cycle_year, "source_file": source_file,
            "rows_processed": int(rows), "dataset_uri": DATASET_URI, "status": status}


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials")],
    timeout=60 * 180,
    memory=65536,   # ~450M-row BTREE sorts (name/sub_id/employer) in-memory via LANCE_BYPASS_SPILLING
    cpu=8.0,
)
def build_fec_indiv_indexes() -> dict:
    """Phase 3 — build the BTREE + BITMAP scalar indexes on the COMPLETE unified
    dataset. Run ONCE, after the 24-cycle backfill finishes. Idempotent (replace=True)."""
    import lance

    so = _r2_storage_options()
    ds = lance.dataset(DATASET_URI, storage_options=so)
    rows = ds.count_rows()
    print(f"Indexing {DATASET_URI} — {rows:,} rows")

    built = _create_indexes(so)

    ds = lance.dataset(DATASET_URI, storage_options=so)  # reopen → committed index set
    committed = _list_committed_indices(ds)
    print(f"Committed indices: {committed}")
    return {"dataset": DATASET_URI, "rows": rows, "built": built,
            "committed_indices": committed}


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 5)
def ledger(limit: int = 30) -> list:
    """Read the most recent ops.fec_indiv_runs rows."""
    import psycopg

    dsn = os.environ["HQX_DB_URL_POOLED"]
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT id, feed, cycle_year, source_file, rows_processed, status, error, "
            "started_at, completed_at FROM ops.fec_indiv_runs ORDER BY id DESC LIMIT %s",
            (limit,),
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def _select_years(only: str) -> list[int]:
    """Resolve the cycle list for an entrypoint. ``--only`` matches a year substring
    (e.g. '2024' → [2024]; '198' → [1980,1982,...,1988])."""
    if not only:
        return CYCLE_YEARS
    return [y for y in CYCLE_YEARS if only in str(y)]


@app.local_entrypoint()
def initdb() -> None:
    """Create the ops ledger table (run once before the first ingest)."""
    import json

    print(json.dumps(init_schema.remote(), indent=2, default=str))


@app.local_entrypoint()
def land(only: str = "", dry_run: bool = False) -> None:
    """Phase 1 — fan out the cycle downloads in PARALLEL (independent landing keys,
    no Lance writes → safe to parallelize; Modal caps concurrency at max_containers)."""
    years = _select_years(only)
    print(f"Phase 1 — landing {len(years)} cycle(s) → s3://{BUCKET}/{LANDING_PREFIX}")
    for y in years:
        print(f"   {y}  {_zip_url(y)}")
    if dry_run:
        return
    for result in fetch_indiv_to_landing.map(years):
        print(result)


@app.local_entrypoint()
def backfill(only: str = "", dry_run: bool = False) -> None:
    """Phase 2 — ingest the landed cycles into the unified dataset SEQUENTIALLY (one
    writer per dataset avoids Lance OCC commit conflicts)."""
    years = _select_years(only)
    print(f"Phase 2 — ingesting {len(years)} cycle(s) → {DATASET_URI}")
    if dry_run:
        for y in years:
            print(f"   {y}  {_landing_key(y)}")
        return
    for y in years:
        print(f"\n=== cycle {y} ===")
        print(ingest_indiv_cycle.remote(_landing_key(y), y, trigger_callback_url=None))


@app.local_entrypoint()
def index() -> None:
    """Phase 3 — build the scalar indexes on the completed unified dataset (run once
    after the backfill finishes)."""
    import json

    print(json.dumps(build_fec_indiv_indexes.remote(), indent=2, default=str))


@app.local_entrypoint()
def show_ledger(limit: int = 30) -> None:
    """Print the most recent ops ledger rows."""
    import json

    print(json.dumps(ledger.remote(limit), indent=2, default=str))
