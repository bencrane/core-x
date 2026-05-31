"""Compute worker — SBA Paycheck Protection Program (PPP FOIA) bulk ingest.

Part of the ``sba-ppp-pipelines`` Modal app. Two endpoint-less functions, spawned
by the Universal Dispatcher (core/modal_dispatcher.py) or driven by the local
entrypoints. This is a BOUNDED backfill of a single point-in-time FOIA release
(snapshot 2024-09-30, the ``240930`` filename stamp) — there is no Trigger cron.

Two-plane network strategy (approved): LAND raw bytes to R2 first, transcode in
Python at ingest. Direct ``httpfs`` reads are rejected — for CSV, httpfs pulls
the whole file anyway (no range-read win) and gives no hook for the mandatory
cp1252→UTF-8 transcode (see docs/reference/01_duckdb_processing.md §2.1/§2.5).

  Phase 1 — fetch_ppp_to_landing (Python: I/O only, no parse):
      SBA CKAN CSV (https)
        → requests stream      → /tmp/<file>.csv
        → boto3 upload         → s3://data-sink/landing/ppp/<file>.csv   (RAW bytes; cp1252 preserved)

  Phase 2 — ingest_ppp_extract (DuckDB does 100% of the transform):
      R2 landing CSV
        → boto3 get_object     → cp1252→UTF-8 transcode → /tmp/<file>.utf8.csv   (Python: I/O only)
        → DuckDB read_csv      → 53-col project / cast (100% in SQL)
        → Arrow table          → con.sql(...).to_arrow_table()
        → lance.write_dataset(s3://data-sink/active/ppp/, v2.1, idempotent append)

Encoding: PPP FOIA bodies are Windows-1252 (cp1252), NOT UTF-8 — verified by
deep mid-file probes (lone 0xBF etc., zero 0x80-0x9F bytes). The ASCII header
masks it. cp1252 is single-byte, so 8 MiB transcode chunk boundaries never split
a character and no rows drop. Redaction is ingested faithfully: LoanStatus may be
the literal 'Exemption 4', LoanStatusDate is blank in that case → NULL. Published
demographic categories ('Unanswered', 'Unknown/NotStated') are kept verbatim.

    modal deploy pipelines/sba_ppp/ppp_loans_bulk.py
    modal run    pipelines/sba_ppp/ppp_loans_bulk.py::fetch              # Phase 1: land all 13 CSVs → R2
    modal run    pipelines/sba_ppp/ppp_loans_bulk.py::fetch --dry-run    # print the file list
    modal run    pipelines/sba_ppp/ppp_loans_bulk.py::backfill           # Phase 2: sequential ingest → Lance
    modal run    pipelines/sba_ppp/ppp_loans_bulk.py::backfill --only 150k_plus
"""

from __future__ import annotations

import os

import modal

BUCKET = "data-sink"
LANDING_PREFIX = "landing/ppp/"
# Lance system-of-record tier (NOT the landing/raw zone). Exact path per directive.
DATASET_URI = os.environ.get("PPP_LANCE_URI", "s3://data-sink/active/ppp/")
SCRATCH_DIR = "/tmp"
FEED = "ppp_loans"

# Point-in-time FOIA snapshot date, decoded from the `240930` filename stamp.
SNAPSHOT_DATE = "2024-09-30"

# Lance fragment sizing (directive constraints).
#   max_rows_per_file = 1048576 — exact.
#   max_bytes_per_file: the directive wrote `90 * 10243` and annotated it "(90 GiB)".
#   The only reading that equals 90 GiB is `90 * 1024**3` (= 96,636,764,160), which
#   is also Lance's documented default and the existing entity-worker constant. A
#   literal `90 * 10243` (~900 KB) would shatter each ~400 MB extract into ~450
#   fragments — not the intent. Honor the stated "90 GiB".
MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3
# Net-new dataset → pin the current Lance default (per 02_lancedb_storage.md §2.3).
DATA_STORAGE_VERSION = "2.1"

# Scalar index plan — built ONCE, post-backfill, by the `index` entrypoint (per
# 02_lancedb_storage.md §6). BTREE for high-cardinality load-bearing resolution
# keys (equality + range predicates); BITMAP for low-cardinality categoricals
# filtered frequently. Indexing a growing dataset per file is wasteful, so this
# runs only after all 13 fragments are appended.
PPP_BTREE_INDEXES = [
    "loan_number",                    # unique loan identifier
    "naics_code",                     # 6-digit industry join key
    "servicing_lender_location_id",   # lender resolution key
    "originating_lender_location_id",  # lender resolution key
]
PPP_BITMAP_INDEXES = [
    "processing_method",  # PPP / PPS
    "loan_status",        # Paid in Full / Exemption 4 / Charged Off
    "borrower_state",
    "project_state",
    "business_type",
]

# CKAN dataset UUID (the directive's resource links are HTML pages; the real
# download artifacts hang off the dataset UUID, resolved via package_show).
PPP_DATASET_UUID = "8aa276e2-6cab-4f86-aca4-a7dde42adf24"

# (resource_id, filename, loan_bracket). All 13 share one byte-identical 53-col
# header (verified), so one schema covers every file.
PPP_RESOURCES: list[tuple[str, str, str]] = [
    ("c1275a03-c25c-488a-bd95-403c4b2fa036", "public_150k_plus_240930.csv", "150k_plus"),
    ("cff06664-1f75-4969-ab3d-6fa7d6b4c41e", "public_up_to_150k_1_240930.csv", "up_to_150k"),
    ("1e6b6629-a5aa-46e6-a442-6e67366d2362", "public_up_to_150k_2_240930.csv", "up_to_150k"),
    ("644c304a-f5ad-4cfa-b128-fe2cbcb7b26e", "public_up_to_150k_3_240930.csv", "up_to_150k"),
    ("98af633d-eb1b-4d4b-995d-330962e6c38d", "public_up_to_150k_4_240930.csv", "up_to_150k"),
    ("3b407e04-f269-47a0-a5fe-661d1a08a76c", "public_up_to_150k_5_240930.csv", "up_to_150k"),
    ("7b7b5b58-9645-4b88-a675-a8a825e77076", "public_up_to_150k_6_240930.csv", "up_to_150k"),
    ("dabdddb5-1807-44f6-97c6-d624a5372525", "public_up_to_150k_7_240930.csv", "up_to_150k"),
    ("1fc6ddc4-ccb0-49d4-b632-0749e3292e57", "public_up_to_150k_8_240930.csv", "up_to_150k"),
    ("e9f2c718-b95e-47da-8f3e-17154aab1c86", "public_up_to_150k_9_240930.csv", "up_to_150k"),
    ("d9972f0d-c377-46ac-8637-a5c1265377c8", "public_up_to_150k_10_240930.csv", "up_to_150k"),
    ("8db19ddc-f036-40df-89f9-d0d309aa58b5", "public_up_to_150k_11_240930.csv", "up_to_150k"),
    ("7e4f672f-d163-4735-a5ec-f23afa2835db", "public_up_to_150k_12_240930.csv", "up_to_150k"),
]
_RESOURCE_BY_FILE = {name: (rid, bracket) for rid, name, bracket in PPP_RESOURCES}

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",       # >=1.5 guarantees to_arrow_table; <2 stays below the v2.0 break
    "lancedb>=0.15",
    "pylance>=7",           # provides `import lance`; lancedb does not re-export it
    "pyarrow>=17",
    "requests>=2.32",       # SBA stream → R2 landing
    "boto3>=1.35",          # R2 landing get/put
    "psycopg[binary]>=3.2",  # ops.* terminal state
)

app = modal.App("sba-ppp-pipelines", image=image)

# 53 source columns, in CSV header order, → snake_case Lance schema. Demographic
# sentinels are kept verbatim (nullif only collapses EMPTY cells, per D-2).
# Money is plain decimal (no $/commas) → direct TRY_CAST. Dates are MM/DD/YYYY.
# Leading-zero codes (office, zips, NAICS, loan number) stay VARCHAR. No VARIANT.
_STR_COLS = [
    ("LoanNumber", "loan_number"),
    ("SBAOfficeCode", "sba_office_code"),
    ("ProcessingMethod", "processing_method"),
    ("BorrowerName", "borrower_name"),
    ("BorrowerAddress", "borrower_address"),
    ("BorrowerCity", "borrower_city"),
    ("BorrowerState", "borrower_state"),
    ("BorrowerZip", "borrower_zip"),
    ("LoanStatus", "loan_status"),
    ("FranchiseName", "franchise_name"),
    ("ServicingLenderLocationID", "servicing_lender_location_id"),
    ("ServicingLenderName", "servicing_lender_name"),
    ("ServicingLenderAddress", "servicing_lender_address"),
    ("ServicingLenderCity", "servicing_lender_city"),
    ("ServicingLenderState", "servicing_lender_state"),
    ("ServicingLenderZip", "servicing_lender_zip"),
    ("RuralUrbanIndicator", "rural_urban_indicator"),
    ("HubzoneIndicator", "hubzone_indicator"),
    ("LMIIndicator", "lmi_indicator"),
    ("BusinessAgeDescription", "business_age_description"),
    ("ProjectCity", "project_city"),
    ("ProjectCountyName", "project_county_name"),
    ("ProjectState", "project_state"),
    ("ProjectZip", "project_zip"),
    ("CD", "congressional_district"),
    ("NAICSCode", "naics_code"),
    ("Race", "race"),
    ("Ethnicity", "ethnicity"),
    ("BusinessType", "business_type"),
    ("OriginatingLenderLocationID", "originating_lender_location_id"),
    ("OriginatingLender", "originating_lender"),
    ("OriginatingLenderCity", "originating_lender_city"),
    ("OriginatingLenderState", "originating_lender_state"),
    ("Gender", "gender"),
    ("Veteran", "veteran"),
    ("NonProfit", "non_profit"),
]
_INT_COLS = [
    ("Term", "term_months"),
    ("SBAGuarantyPercentage", "sba_guaranty_percentage"),
    ("JobsReported", "jobs_reported"),
]
_DBL_COLS = [
    ("InitialApprovalAmount", "initial_approval_amount"),
    ("CurrentApprovalAmount", "current_approval_amount"),
    ("UndisbursedAmount", "undisbursed_amount"),
    ("UTILITIES_PROCEED", "utilities_proceed"),
    ("PAYROLL_PROCEED", "payroll_proceed"),
    ("MORTGAGE_INTEREST_PROCEED", "mortgage_interest_proceed"),
    ("RENT_PROCEED", "rent_proceed"),
    ("REFINANCE_EIDL_PROCEED", "refinance_eidl_proceed"),
    ("HEALTH_CARE_PROCEED", "health_care_proceed"),
    ("DEBT_INTEREST_PROCEED", "debt_interest_proceed"),
    ("ForgivenessAmount", "forgiveness_amount"),
]
_DATE_COLS = [
    ("DateApproved", "date_approved"),
    ("LoanStatusDate", "loan_status_date"),
    ("ForgivenessDate", "forgiveness_date"),
]


def _sba_download_url(resource_id: str, filename: str) -> str:
    return (
        f"https://data.sba.gov/dataset/{PPP_DATASET_UUID}"
        f"/resource/{resource_id}/download/{filename}"
    )


def _landing_key(filename: str) -> str:
    return f"{LANDING_PREFIX}{filename}"


def _classify_bracket(filename: str) -> str:
    return "150k_plus" if "150k_plus" in filename else "up_to_150k"


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


def _download_transcode(s3, key: str, out_path: str) -> None:
    """Stream the landed RAW (cp1252) CSV from R2 and write a UTF-8 copy to
    scratch. cp1252 is single-byte → 8 MiB chunk boundaries never split a
    character; errors='replace' covers the 5 undefined cp1252 bytes. This is an
    I/O concern (per the I/O-only mandate); the SQL still does 100% of the
    transform, and it is lossless at the row level (no rows dropped)."""
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    body = obj["Body"]
    chunk_size = 8 << 20
    with open(out_path, "wb") as out:
        while True:
            chunk = body.read(chunk_size)
            if not chunk:
                break
            out.write(chunk.decode("cp1252", errors="replace").encode("utf-8"))


def _build_sql(csv_path: str, source_file: str, bracket: str, snapshot_date: str) -> str:
    """100% of the transform. read_csv(all_varchar=true) → defensive TRY_CAST on
    every coercion → snake_case projection. All values interpolated here are
    repo-controlled (our /tmp path, our registry filename, fixed literals) — no
    user input, no injection surface — and single-quote-escaped regardless."""

    def lit(s: str) -> str:
        return s.replace("'", "''")

    def strcol(src: str, dst: str) -> str:
        return f'nullif(trim("{src}"), \'\') AS {dst}'

    def intcol(src: str, dst: str) -> str:
        return f'TRY_CAST(nullif(trim("{src}"), \'\') AS INTEGER) AS {dst}'

    def dblcol(src: str, dst: str) -> str:
        return f'TRY_CAST(nullif(trim("{src}"), \'\') AS DOUBLE) AS {dst}'

    def datecol(src: str, dst: str) -> str:
        # PPP dates are MM/DD/YYYY; TRY_STRPTIME→DATE, blank/garbage → NULL.
        return (
            f"TRY_CAST(TRY_STRPTIME(nullif(trim(\"{src}\"), ''), '%m/%d/%Y') AS DATE) AS {dst}"
        )

    projections = (
        [strcol(s, d) for s, d in _STR_COLS]
        + [intcol(s, d) for s, d in _INT_COLS]
        + [dblcol(s, d) for s, d in _DBL_COLS]
        + [datecol(s, d) for s, d in _DATE_COLS]
    )
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
    '{lit(bracket)}' AS loan_bracket,
    CAST('{lit(snapshot_date)}' AS DATE) AS snapshot_date,
    now() AS ingested_at
FROM raw
"""


def _append_idempotent(table, source_file: str, so: dict) -> None:
    """Append to the Lance dataset, replacing any prior rows for this
    source_file so re-runs are idempotent. Creates the dataset on first write.
    Run serially — concurrent writers to one dataset can hit commit conflicts."""
    import lance

    try:
        ds = lance.dataset(DATASET_URI, storage_options=so)
    except Exception:
        ds = None

    common = dict(
        data_storage_version=DATA_STORAGE_VERSION,
        max_rows_per_file=MAX_ROWS_PER_FILE,
        max_bytes_per_file=MAX_BYTES_PER_FILE,
        storage_options=so,
    )
    if ds is None:
        lance.write_dataset(table, DATASET_URI, mode="create", **common)
        return
    try:
        ds.delete(f"source_file = '{source_file.replace(chr(39), chr(39) * 2)}'")
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: pre-append delete failed (continuing): {exc}")
    lance.write_dataset(table, DATASET_URI, mode="append", **common)


def _record_run(source_file, bracket, rows, status, error, started_at, completed_at) -> None:
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* write.")
        return
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops.ppp_loan_runs
                    (source_file, loan_bracket, rows_processed, status, error,
                     started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (source_file, bracket, rows, status, error, started_at, completed_at),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must not mask the ingest
        print(f"WARN: ops.* write failed: {exc}")


def _post_callback(url, payload, attempts: int = 3) -> None:
    """POST the RAW terminal payload to the Trigger waitpoint url. The whole body
    becomes result.output — NO API key, NO {"data": ...} envelope."""
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
    secrets=[modal.Secret.from_name("r2-credentials")],
    timeout=60 * 45,
    memory=2048,
    cpu=2.0,
    retries=2,           # SBA CKAN is the fragile hop; idempotent overwrite of the landing key
    max_containers=6,    # be polite to data.sba.gov under the 13-way fan-out
)
def fetch_ppp_to_landing(filename: str) -> dict:
    """Phase 1 — Python I/O ONLY. Stream one SBA CSV to /tmp, then upload the RAW
    bytes (cp1252 preserved, byte-faithful FOIA provenance) to the R2 landing
    zone. No parsing, no transcode here."""
    import os.path

    import requests

    if filename not in _RESOURCE_BY_FILE:
        raise RuntimeError(f"Unknown PPP file: {filename}")
    resource_id, bracket = _RESOURCE_BY_FILE[filename]
    url = _sba_download_url(resource_id, filename)
    key = _landing_key(filename)
    tmp = os.path.join(SCRATCH_DIR, filename)

    written = 0
    with requests.get(url, stream=True, timeout=(30, 1200)) as resp:
        resp.raise_for_status()
        with open(tmp, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=1 << 20):
                if chunk:
                    fh.write(chunk)
                    written += len(chunk)

    s3 = _s3_client()
    s3.upload_file(tmp, BUCKET, key)
    try:
        os.remove(tmp)
    except OSError:
        pass

    print(f"Landed {filename}: {written} bytes → s3://{BUCKET}/{key}")
    return {"filename": filename, "landing_key": key, "bytes": written,
            "loan_bracket": bracket, "feed": FEED, "status": "success"}


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 60,
    memory=16384,
    cpu=4.0,
)
def ingest_ppp_extract(key: str, trigger_callback_url: str | None = None) -> dict:
    """Phase 2 — download landed CSV → cp1252→UTF-8 transcode → DuckDB project/cast
    → Arrow → idempotent Lance append, then record ops.* state and wake Trigger.
    Re-raises on failure so the Modal call is marked failed."""
    import datetime as dt
    import os.path

    import duckdb

    started_at = dt.datetime.now(dt.timezone.utc)
    filename = key.rsplit("/", 1)[-1]
    bracket = _classify_bracket(filename)
    rows = 0
    status = "error"
    error: str | None = None

    try:
        s3 = _s3_client()
        scratch = os.path.join(SCRATCH_DIR, filename + ".utf8.csv")
        _download_transcode(s3, key, scratch)

        con = duckdb.connect(":memory:")
        try:
            con.execute("PRAGMA threads=4;")
            table = con.sql(_build_sql(scratch, filename, bracket, SNAPSHOT_DATE)).to_arrow_table()
        finally:
            con.close()
        rows = table.num_rows

        _append_idempotent(table, filename, _r2_storage_options())
        status = "success"
    except Exception as exc:  # noqa: BLE001 — terminal handling below + re-raise
        error = str(exc)
        status = "error"
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run(filename, bracket, int(rows), status, error, started_at, completed_at)
        _post_callback(
            trigger_callback_url,
            {"status": status, "rows": int(rows), "feed": FEED, "source_file": filename},
        )

    if status != "success":
        raise RuntimeError(f"PPP ingest failed for {key}: {error}")
    return {"feed": FEED, "source_file": filename, "rows_processed": int(rows),
            "loan_bracket": bracket, "status": status}


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


@app.function(
    secrets=[
        modal.Secret.from_name("r2-credentials"),
        # BTREE training sorts the column; bypass Lance's bounded spill-to-disk
        # ExternalSorter, whose memory accounting under-sizes the pool to ~19 MB
        # and exhausts on an 11.5M-row column even in a 16 GiB container. Sorting
        # in-memory is well within RAM here. See lance-format/lance#2650.
        modal.Secret.from_dict({"LANCE_BYPASS_SPILLING": "true"}),
    ],
    timeout=60 * 90,
    memory=16384,
    cpu=4.0,
)
def build_ppp_indexes() -> dict:
    """Build the BTREE + BITMAP scalar indexes on the active PPP Lance dataset.
    Run ONCE, after the full backfill. create_scalar_index defaults to
    replace=True, so re-running rebuilds cleanly (idempotent)."""
    import lance

    so = _r2_storage_options()
    ds = lance.dataset(DATASET_URI, storage_options=so)
    rows = ds.count_rows()
    print(f"Indexing {DATASET_URI} — {rows:,} rows")

    for col in PPP_BTREE_INDEXES:
        ds.create_scalar_index(col, index_type="BTREE")
        print(f"  BTREE  ✓ {col}")
    for col in PPP_BITMAP_INDEXES:
        ds.create_scalar_index(col, index_type="BITMAP")
        print(f"  BITMAP ✓ {col}")

    ds = lance.dataset(DATASET_URI, storage_options=so)  # reopen → read committed index set
    committed = _list_committed_indices(ds)
    print(f"Committed indices: {committed}")
    return {
        "dataset": DATASET_URI,
        "rows": rows,
        "btree": PPP_BTREE_INDEXES,
        "bitmap": PPP_BITMAP_INDEXES,
        "committed_indices": committed,
    }


@app.local_entrypoint()
def fetch(only: str = "", dry_run: bool = False) -> None:
    """Phase 1 backfill — land all 13 CSVs to R2 in parallel (independent keys,
    so no serialization needed). ``--only SUBSTR`` filters; ``--dry-run`` lists."""
    files = [name for _, name, _ in PPP_RESOURCES]
    if only:
        files = [f for f in files if only in f]
    print(f"Phase 1 — landing {len(files)} file(s) to s3://{BUCKET}/{LANDING_PREFIX}")
    for f in files:
        print("  ", f)
    if dry_run:
        return
    for result in fetch_ppp_to_landing.map(files):
        print(result)


@app.local_entrypoint()
def backfill(only: str = "", dry_run: bool = False) -> None:
    """Phase 2 backfill — ingest landed CSVs into Lance SEQUENTIALLY (one writer
    per dataset avoids Lance commit conflicts). ``--only SUBSTR`` filters."""
    files = [name for _, name, _ in PPP_RESOURCES]
    if only:
        files = [f for f in files if only in f]
    keys = [_landing_key(f) for f in files]
    print(f"Phase 2 — ingesting {len(keys)} landed file(s) → {DATASET_URI}")
    for k in keys:
        print("  ", k)
    if dry_run:
        return
    for k in keys:
        print(f"\n=== {k} ===")
        print(ingest_ppp_extract.remote(k, trigger_callback_url=None))


@app.local_entrypoint()
def index() -> None:
    """Build the PPP scalar indexes on the completed active dataset. Run after the
    Phase 2 backfill finishes (all 13 fragments appended)."""
    import json

    print(json.dumps(build_ppp_indexes.remote(), indent=2, default=str))
