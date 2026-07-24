"""Compute worker — SBA 7(a) & 504 FOIA loan-level bulk ingest.

Part of the ``sba-7a-504`` Modal app. NOT directly exposed — it is spawned by the
Universal Dispatcher (core/modal_dispatcher.py), the only proxy-authed endpoint in
the fleet. This worker has no web endpoint. Clean-room data plane: no Iceberg, no
Polaris — DuckDB does 100% of the transform, Lance is written straight to R2.

Data plane (one Modal invocation per program; both programs run independently):
    SBA CKAN CSVs (per program: 2 era files for 504, 4 for 7(a))
      → land each era file to R2 as .csv.zst    (durable as-of snapshot; boto3/zstd, I/O only)
      → DuckDB read_csv(union_by_name, all_varchar)  → project / cast   (100% in SQL)
      → Arrow table  → con.sql(...).to_arrow_table()
      → lance.write_dataset(s3://data-sink/active/sba_{program}/, v2.0, overwrite)
      → BTREE scalar indexes on the load-bearing resolution keys

Topology: TWO distinct datasets, one per program, sharing a 31-field core column
contract (identical names + types in both). The divergent lender / financial
columns are program-exclusive — 7(a) carries the Bank block + guaranty + interest
+ revolver/secondary-market fields; 504 carries the CDC block + third-party lender
block. ``mode="overwrite"`` is canonical: each FOIA file is a complete historical
snapshot of its era, so the dataset = union of all eras, rebuilt wholesale each
"as of" refresh. Lance's internal versioning retains prior snapshots for time-travel.

Control plane (Trigger v4 durable callback): the worker accepts
``trigger_callback_url`` and, on terminal state (success OR failure), (1) writes the
run row to ``ops.sba_foia_runs`` via psycopg and (2) POSTs a FLAT JSON body
``{status, rows, feed, program, dataset_uri, as_of}`` to that url to wake the
suspended Trigger run. No ``{"data": ...}`` envelope.

Launch (spawn-on-deployed): ``modal deploy pipelines/sba_foia/ingest.py`` first, then

    modal run    pipelines/sba_foia/ingest.py::run_all                 # both programs (spawn-only)
    modal run    pipelines/sba_foia/ingest.py::run_one --program 7a    # one program (spawn-only)

Each entrypoint deploy-if-stales, spawns ``ingest_sba_program`` on the DEPLOYED app,
prints the ``fc-…`` id(s), and returns — it does not wait for or print the result dict;
the worker's terminal row in ``ops.sba_foia_runs`` and the app logs are the record.
Follow with ``modal app logs sba-7a-504``; probe with
``modal.FunctionCall.from_id('fc-…').get(timeout=0)``.
"""

from __future__ import annotations

import os

import modal

BUCKET = "data-sink"
# Durable landing prefix (NOT the dataset tier). Per directive: data-sink/landing/sba-7a-504/.
LANDING_PREFIX = "landing/sba-7a-504/"

# Lance system-of-record tier — one dataset per program, env-overridable.
DATASET_URI = {
    "504": os.environ.get("SBA_504_LANCE_URI", "s3://data-sink/active/sba_504/"),
    "7a": os.environ.get("SBA_7A_LANCE_URI", "s3://data-sink/active/sba_7a/"),
}

# SBA CKAN package (resources are resolved at runtime so URLs/filenames stay fresh).
CKAN_PACKAGE_SHOW = "https://data.sba.gov/api/3/action/package_show?id=7-a-504-foia"
# CSV resource name discriminator per program (case-insensitive substring).
PROGRAM_NAME_MATCH = {"504": "foia - 504", "7a": "foia - 7(a)"}
FEED = {"504": "sba_504", "7a": "sba_7a"}

SCRATCH_DIR = "/tmp"
AS_OF_DEFAULT = "260331"
_UA = "core-x/sba-foia (+https://data.sba.gov)"

# Lance fragment sizing.
# NOTE: the directive wrote `max_bytes_per_file=90 * 10243`. That is read here as
# `90 * 1024**3` (90 GiB) — Lance's documented default. A literal `90 * 10243`
# (~900 KB) would shatter the 7(a) dataset (~1.95M rows) into hundreds of
# fragments; flip the constant below if 900 KB was genuinely intended.
MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",       # >=1.5 guarantees to_arrow_table; <2 stays below the v2.0 break
    "lancedb>=0.15",
    "pylance>=7",           # provides `import lance`; lancedb does not re-export it
    "pyarrow>=17",
    "boto3>=1.35",          # R2 landing read/write
    "requests>=2.32",       # SBA CKAN download + Trigger callback
    "zstandard>=0.22",      # .csv.zst landing compression
    "psycopg[binary]>=3.2",  # terminal state write to ops.*
    "pandas>=2.2",          # lance.add_columns path imports pandas (normalize_transform shim)
).env(
    # BTREE scalar-index builds sort the column. Lance's spill-to-disk sorter
    # uses a small bounded DataFusion memory pool that OOMs on the ~1.95M-row
    # 7(a) high-cardinality string columns (datafusion#10073). Force the
    # in-memory sort path (uses the container's 16 GiB) so every index builds.
    {"LANCE_BYPASS_SPILLING": "true"}
)

app = modal.App("sba-7a-504", image=image)


# ── DuckDB projection (validated against DuckDB 1.5.3 over the full 2.17M-row
# corpus: zero cast failures, exact row counts). all_varchar=true → every cast is
# explicit and defensive (TRY_CAST/TRY_STRPTIME → NULL, never a hard parse error).
# Source headers are lowercase; the date wire format is non-padded M/D/YYYY. The
# 31-line core block is IDENTICAL in both programs. ───────────────────────────
_CORE = """    TRY_STRPTIME(nullif(trim(asofdate), ''), '%-m/%-d/%Y')::DATE AS as_of_date,
    upper(trim(program)) AS program,
    nullif(trim(locationid), '') AS location_id,
    nullif(trim(borrname), '') AS borr_name,
    nullif(trim(borrstreet), '') AS borr_street,
    nullif(trim(borrcity), '') AS borr_city,
    nullif(trim(borrstate), '') AS borr_state,
    nullif(trim(borrzip), '') AS borr_zip,
    TRY_CAST(nullif(trim(grossapproval), '') AS DECIMAL(18,2)) AS gross_approval,
    TRY_STRPTIME(nullif(trim(approvaldate), ''), '%-m/%-d/%Y')::DATE AS approval_date,
    TRY_CAST(nullif(trim(approvalfy), '') AS SMALLINT) AS approval_fy,
    TRY_STRPTIME(nullif(trim(firstdisbursementdate), ''), '%-m/%-d/%Y')::DATE AS first_disbursement_date,
    nullif(trim(processingmethod), '') AS processing_method,
    nullif(trim(subprogram), '') AS subprogram,
    TRY_CAST(nullif(trim(terminmonths), '') AS INTEGER) AS term_in_months,
    nullif(trim(naicscode), '') AS naics_code,
    nullif(CASE WHEN trim(naicsdescription) LIKE '"%"' THEN trim(both '"' FROM trim(naicsdescription)) ELSE trim(naicsdescription) END, '') AS naics_description,
    nullif(trim(franchisecode), '') AS franchise_code,
    nullif(trim(franchisename), '') AS franchise_name,
    nullif(trim(projectcounty), '') AS project_county,
    nullif(trim(projectstate), '') AS project_state,
    nullif(trim(sbadistrictoffice), '') AS sba_district_office,
    nullif(trim(congressionaldistrict), '') AS congressional_district,
    nullif(trim(businesstype), '') AS business_type,
    nullif(trim(businessage), '') AS business_age,
    nullif(trim(loanstatus), '') AS loan_status,
    TRY_STRPTIME(nullif(trim(paidinfulldate), ''), '%-m/%-d/%Y')::DATE AS paid_in_full_date,
    TRY_STRPTIME(nullif(trim(chargeoffdate), ''), '%-m/%-d/%Y')::DATE AS charge_off_date,
    TRY_CAST(nullif(trim(grosschargeoffamount), '') AS DECIMAL(18,2)) AS gross_charge_off_amount,
    TRY_CAST(nullif(trim(jobssupported), '') AS INTEGER) AS jobs_supported,
    TRY_CAST(nullif(trim(collateralind), '') AS BOOLEAN) AS collateral_ind,
"""

# 7(a)-only: Bank lender block + guaranty + interest + revolver/secondary-market.
_SEVENA_X = """    nullif(trim(bankname), '') AS bank_name,
    nullif(trim(bankfdicnumber), '') AS bank_fdic_number,
    nullif(trim(bankncuanumber), '') AS bank_ncua_number,
    nullif(trim(bankstreet), '') AS bank_street,
    nullif(trim(bankcity), '') AS bank_city,
    nullif(trim(bankstate), '') AS bank_state,
    nullif(trim(bankzip), '') AS bank_zip,
    TRY_CAST(nullif(trim(sbaguaranteedapproval), '') AS DECIMAL(18,2)) AS sba_guaranteed_approval,
    TRY_CAST(nullif(trim(initialinterestrate), '') AS DECIMAL(7,4)) AS initial_interest_rate,
    nullif(trim(fixedorvariableinterestind), '') AS fixed_or_variable_interest_ind,
    TRY_CAST(nullif(trim(revolverstatus), '') AS BOOLEAN) AS revolver_status,
    nullif(upper(trim(soldsecmrktind)), '') AS sold_sec_mrkt_ind,
"""

# 504-only: CDC lender block + third-party lender block.
_F504_X = """    nullif(trim(cdc_name), '') AS cdc_name,
    nullif(trim(cdc_street), '') AS cdc_street,
    nullif(trim(cdc_city), '') AS cdc_city,
    nullif(trim(cdc_state), '') AS cdc_state,
    nullif(trim(cdc_zip), '') AS cdc_zip,
    nullif(trim(thirdpartylender_name), '') AS third_party_lender_name,
    nullif(trim(thirdpartylender_city), '') AS third_party_lender_city,
    nullif(trim(thirdpartylender_state), '') AS third_party_lender_state,
    TRY_CAST(nullif(trim(thirdpartydollars), '') AS DECIMAL(18,2)) AS third_party_dollars,
"""

# Provenance (both). extract_label + source_file are derived from the read_csv
# `filename` pseudo-column; the era token is parsed straight off the basename.
_PROV = """    regexp_replace(filename, '.*/', '') AS source_file,
    lower(regexp_extract(regexp_replace(filename, '.*/', ''), '(fy[0-9]{4}-(?:fy[0-9]{4}|present))', 1)) AS extract_label,
    now() AS ingested_at"""

# BTREE scalar indexes on the load-bearing resolution / common-filter keys.
# sba_surrogate_id leads — it is the minted primary key (SBA published no loan id).
_INDEX_COLS = {
    "_core": ["sba_surrogate_id", "borr_name", "borr_state", "project_state", "naics_code",
              "approval_fy", "loan_status", "location_id", "congressional_district",
              "business_type", "sba_district_office"],
    "7a": ["bank_name", "bank_fdic_number"],
    "504": ["cdc_name", "third_party_lender_name"],
}

# ── Surrogate primary key (sba_surrogate_id) ────────────────────────────────────
# SBA publishes NO loan identifier, so we MINT one: a deterministic sha256 over every
# published row attribute (EXCEPT the per-run-volatile ingested_at) + a per-identical-
# group ordinal. The ordinal is required because the FOIA corpus contains byte-identical
# duplicate rows (7a: 1,909; 504: 74) that no pure content hash can separate; the hash
# alone keeps the key idempotent across overwrite rebuilds, the ordinal makes it unique
# per physical row. Both the canonical rebuild (_build_sql) and the in-place patch
# (patch_surrogate_id) hash the SAME ordered column set → identical ids (up to an
# immaterial permutation among byte-duplicate rows).
#
# Column names below MIRROR _CORE / _SEVENA_X / _F504_X / _PROV (projection order). Keep
# in sync if those projection fragments change.
_CORE_COLS = [
    "as_of_date", "program", "location_id", "borr_name", "borr_street", "borr_city",
    "borr_state", "borr_zip", "gross_approval", "approval_date", "approval_fy",
    "first_disbursement_date", "processing_method", "subprogram", "term_in_months",
    "naics_code", "naics_description", "franchise_code", "franchise_name", "project_county",
    "project_state", "sba_district_office", "congressional_district", "business_type",
    "business_age", "loan_status", "paid_in_full_date", "charge_off_date",
    "gross_charge_off_amount", "jobs_supported", "collateral_ind",
]
_SEVENA_COLS = [
    "bank_name", "bank_fdic_number", "bank_ncua_number", "bank_street", "bank_city",
    "bank_state", "bank_zip", "sba_guaranteed_approval", "initial_interest_rate",
    "fixed_or_variable_interest_ind", "revolver_status", "sold_sec_mrkt_ind",
]
_F504_COLS = [
    "cdc_name", "cdc_street", "cdc_city", "cdc_state", "cdc_zip", "third_party_lender_name",
    "third_party_lender_city", "third_party_lender_state", "third_party_dollars",
]


def _hash_cols(program: str) -> list[str]:
    """Ordered hash-input set: all published attributes EXCEPT ingested_at (volatile) and
    the surrogate itself. Identical between the rebuild and the in-place patch."""
    extra = _SEVENA_COLS if program == "7a" else _F504_COLS
    return _CORE_COLS + extra + ["source_file", "extract_label"]


def _surrogate_serialization(cols: list[str]) -> str:
    """Delimiter-safe, injective serialization of the hash columns. chr(31)=unit
    separator between fields; chr(30)=NULL sentinel — neither byte occurs in SBA text."""
    return ("concat_ws(chr(31), "
            + ", ".join(f'coalesce(CAST("{c}" AS VARCHAR), chr(30))' for c in cols)
            + ")")


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
    and otherwise raises FlexibleChecksumError on download/upload."""
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


def _slug(name: str) -> str:
    """Filesystem/key-safe slug from a CKAN resource name, extension dropped."""
    import re

    base = name[:-4] if name.lower().endswith(".csv") else name
    return re.sub(r"[^a-z0-9]+", "-", base.lower()).strip("-")


def _resolve_resources(program: str, as_of: str) -> list[dict]:
    """Resolve this program's CSV era files from the live CKAN catalog. Prefer
    resources whose name carries the as_of label; fall back to all program CSVs."""
    import requests

    match = PROGRAM_NAME_MATCH[program]
    resp = requests.get(CKAN_PACKAGE_SHOW, timeout=60, headers={"User-Agent": _UA})
    resp.raise_for_status()
    body = resp.json()
    if not body.get("success"):
        raise RuntimeError("CKAN package_show returned success=false")
    csvs = [
        {"name": r["name"], "url": r["url"], "id": r.get("id")}
        for r in body["result"]["resources"]
        if r.get("format", "").upper() == "CSV" and match in r.get("name", "").lower()
    ]
    if not csvs:
        raise RuntimeError(f"No CSV resources for program={program} (match={match!r})")
    dated = [c for c in csvs if as_of in c["name"].replace(" ", "")]
    return sorted(dated or csvs, key=lambda c: c["name"])


def _land_or_reuse(s3, url: str, name: str, as_of: str) -> tuple[str, str]:
    """Land one era CSV to R2 as .csv.zst for durability, return (local_raw_csv, r2_key).
    Idempotent: if the durable copy already exists, reuse it instead of re-downloading."""
    import zstandard as zstd
    from botocore.exceptions import ClientError

    slug = _slug(name)
    raw = os.path.join(SCRATCH_DIR, f"{slug}.csv")
    zpath = f"{raw}.zst"
    r2_key = f"{LANDING_PREFIX}asof={as_of}/{slug}.csv.zst"

    try:
        s3.head_object(Bucket=BUCKET, Key=r2_key)
        s3.download_file(BUCKET, r2_key, zpath)
        with open(zpath, "rb") as fi, open(raw, "wb") as fo:
            zstd.ZstdDecompressor().copy_stream(fi, fo)
        print(f"reuse   s3://{BUCKET}/{r2_key} -> {raw}")
        return raw, r2_key
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if code not in ("404", "NoSuchKey", "NotFound", "403"):
            raise

    import requests

    with requests.get(url, stream=True, timeout=(30, 1200), headers={"User-Agent": _UA}) as resp:
        resp.raise_for_status()
        with open(raw, "wb") as fo:
            for chunk in resp.iter_content(chunk_size=1 << 20):
                if chunk:
                    fo.write(chunk)
    with open(raw, "rb") as fi, open(zpath, "wb") as fo:
        zstd.ZstdCompressor(level=10).copy_stream(fi, fo)
    s3.upload_file(zpath, BUCKET, r2_key)
    print(f"landed  {url} -> s3://{BUCKET}/{r2_key}")
    return raw, r2_key


def _build_sql(program: str, files: list[str]) -> str:
    extra = _SEVENA_X if program == "7a" else _F504_X
    file_list = ", ".join("'" + f.replace("'", "''") + "'" for f in files)
    ser = _surrogate_serialization(_hash_cols(program))
    # Project the canonical columns, fingerprint each row (sha256 over _hash_cols), then
    # mint sba_surrogate_id = fingerprint || '-' || per-identical-group ordinal. The
    # ordinal disambiguates byte-duplicate FOIA rows so the PK is unique on every row.
    return (
        "WITH raw AS (\n"
        "    SELECT * FROM read_csv([" + file_list + "], "
        "union_by_name=true, filename=true, all_varchar=true, "
        "header=true, sample_size=-1, ignore_errors=false)\n"
        "),\n"
        "proj AS (\n"
        "    SELECT\n" + _CORE + extra + _PROV + "\n    FROM raw\n"
        "),\n"
        "hashed AS (SELECT *, sha256(" + ser + ") AS _sk_hash FROM proj)\n"
        "SELECT * EXCLUDE (_sk_hash),\n"
        "    _sk_hash || '-' || CAST(row_number() OVER "
        "(PARTITION BY _sk_hash ORDER BY _sk_hash) AS VARCHAR) AS sba_surrogate_id\n"
        "FROM hashed\n"
    )


def _create_indexes(program: str, so: dict) -> None:
    import lance

    ds = lance.dataset(DATASET_URI[program], storage_options=so)
    for col in _INDEX_COLS["_core"] + _INDEX_COLS[program]:
        try:
            ds.create_scalar_index(col, "BTREE", replace=True)
        except Exception as exc:  # noqa: BLE001 — an index miss must not fail the load
            print(f"WARN: BTREE index on {col} failed: {exc}")


def _record_run(program, dataset_uri, as_of, landing_prefix, files_processed,
                source_files, rows, status, error, started_at, completed_at) -> None:
    """Terminal run row → ops.sba_foia_runs (psycopg). Best-effort: never let an
    audit-write failure crash an otherwise-good ingest."""
    import psycopg
    from psycopg.types.json import Jsonb

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* state write.")
        return
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops.sba_foia_runs
                    (program, dataset_uri, as_of, landing_prefix, files_processed,
                     source_files, rows_processed, status, error, started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (program, dataset_uri, as_of, landing_prefix, files_processed,
                 Jsonb(source_files), rows, status, error, started_at, completed_at),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must not mask the ingest
        print(f"WARN: ops.* state write failed: {exc}")


def _post_callback(url, payload, attempts: int = 3) -> None:
    """POST terminal metadata to the Trigger waitpoint URL. FLAT JSON body — no
    ``{"data": ...}`` envelope. A few retries for delivery reliability."""
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
def ingest_sba_program(
    program: str,
    as_of: str = AS_OF_DEFAULT,
    trigger_callback_url: str | None = None,
) -> dict:
    """Land this program's era CSVs → DuckDB project/cast → Lance overwrite →
    BTREE index, then record ops.* state and wake the Trigger run. Re-raises on
    failure so the Modal call is marked failed."""
    import datetime as dt

    import duckdb
    import lance

    program = program.strip().lower()
    if program not in DATASET_URI:
        raise ValueError(f"program must be one of {sorted(DATASET_URI)}, got {program!r}")

    started_at = dt.datetime.now(dt.timezone.utc)
    landing_prefix = f"s3://{BUCKET}/{LANDING_PREFIX}asof={as_of}/"
    rows = 0
    status = "error"
    error: str | None = None
    landed_keys: list[str] = []
    local_files: list[str] = []

    try:
        so = _r2_storage_options()
        s3 = _s3_client()

        for rsc in _resolve_resources(program, as_of):
            raw, key = _land_or_reuse(s3, rsc["url"], rsc["name"], as_of)
            local_files.append(raw)
            landed_keys.append(key)

        con = duckdb.connect(":memory:")
        try:
            con.execute("PRAGMA threads=4;")
            table = con.sql(_build_sql(program, local_files)).to_arrow_table()
        finally:
            con.close()
        rows = table.num_rows

        lance.write_dataset(
            table,
            DATASET_URI[program],
            mode="overwrite",
            data_storage_version="2.0",
            max_rows_per_file=MAX_ROWS_PER_FILE,
            max_bytes_per_file=MAX_BYTES_PER_FILE,
            storage_options=so,
        )
        _create_indexes(program, so)
        status = "success"
    except Exception as exc:  # noqa: BLE001 — terminal handling below + re-raise
        error = str(exc)
        status = "error"
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run(program.upper(), DATASET_URI[program], as_of, landing_prefix,
                    len(local_files), landed_keys, int(rows), status, error,
                    started_at, completed_at)
        _post_callback(
            trigger_callback_url,
            {"status": status, "rows": int(rows), "feed": FEED[program],
             "program": program, "dataset_uri": DATASET_URI[program], "as_of": as_of},
        )

    if status != "success":
        raise RuntimeError(f"sba_foia ingest failed for program={program}: {error}")
    return {"feed": FEED[program], "program": program, "rows_processed": int(rows),
            "files": len(local_files), "dataset_uri": DATASET_URI[program], "status": status}


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials")],
    timeout=60 * 30,
    memory=16384,
    cpu=4.0,
)
def reindex_program(program: str) -> dict:
    """(Re)build the BTREE scalar indexes on an already-written program dataset
    without re-ingesting. Idempotent (replace=True)."""
    import lance

    program = program.strip().lower()
    if program not in DATASET_URI:
        raise ValueError(f"program must be one of {sorted(DATASET_URI)}, got {program!r}")
    so = _r2_storage_options()
    _create_indexes(program, so)
    ds = lance.dataset(DATASET_URI[program], storage_options=so)
    names = sorted(
        (ix.get("name", str(ix)) if isinstance(ix, dict) else str(ix))
        for ix in ds.list_indices()
    )
    return {"program": program, "dataset_uri": DATASET_URI[program],
            "indices": names, "index_count": len(names)}


# --------------------------------------------------------------------------- #
# In-place additive surrogate-key patch (no recreate) + ops ledger
# --------------------------------------------------------------------------- #
# Cross-worker ledger for additive in-place schema patches. Idempotent DDL; mirrored
# verbatim from resolution/crosswalk_sam_usaspending.py.
OPS_PATCH_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.schema_patch_runs (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dataset_uri     text        NOT NULL,
    operation       text        NOT NULL,
    column_added    text,
    index_built     text,
    rows            bigint,
    exact_dup_rows  bigint,
    version_before  bigint,
    version_after   bigint,
    status          text        NOT NULL,
    error           text,
    started_at      timestamptz,
    completed_at    timestamptz,
    recorded_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS schema_patch_runs_dataset_idx  ON ops.schema_patch_runs (dataset_uri);
CREATE INDEX IF NOT EXISTS schema_patch_runs_recorded_idx ON ops.schema_patch_runs (recorded_at DESC);
"""


def _record_patch(dataset_uri, operation, column_added, index_built, rows, exact_dup_rows,
                  version_before, version_after, status, error, started_at, completed_at) -> None:
    """Terminal row → ops.schema_patch_runs (psycopg). Best-effort; never masks the patch."""
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.schema_patch_runs write.")
        return
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(OPS_PATCH_DDL)
            cur.execute(
                """
                INSERT INTO ops.schema_patch_runs
                    (dataset_uri, operation, column_added, index_built, rows, exact_dup_rows,
                     version_before, version_after, status, error, started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (dataset_uri, operation, column_added, index_built, rows, exact_dup_rows,
                 version_before, version_after, status, error, started_at, completed_at),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must not mask the patch
        print(f"WARN: ops.schema_patch_runs write failed: {exc}")


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 60,
    memory=16384,
    cpu=4.0,
)
def patch_surrogate_id(program: str, trigger_callback_url: str | None = None) -> dict:
    """ADDITIVE IN-PLACE: mint + BTREE-index sba_surrogate_id on a program dataset WITHOUT
    recreating it. One global DuckDB pass keyed to _rowid computes the deterministic
    sha256+ordinal id (LANCE_BYPASS_SPILLING is set on the image for the high-cardinality
    index sort); add_columns zips it on positionally; create_scalar_index builds the BTREE.
    Idempotent (skips the add if the column already exists; always (re)builds the index).
    Integrity-gated: every stored id's hash-prefix must equal the macro recomputed from the
    row, the id must be globally unique + non-null, and the row count unchanged — else the
    dataset is rolled back to its pre-patch version and the run fails."""
    import datetime as dt

    import duckdb
    import lance

    program = program.strip().lower()
    if program not in DATASET_URI:
        raise ValueError(f"program must be one of {sorted(DATASET_URI)}, got {program!r}")
    uri = DATASET_URI[program]
    so = _r2_storage_options()
    started = dt.datetime.now(dt.timezone.utc)
    status, error, added, result = "error", None, None, {}
    cols = _hash_cols(program)
    ser = _surrogate_serialization(cols)

    ds = lance.dataset(uri, storage_options=so)
    v_before = ds.version
    n0 = ds.count_rows()
    v_after = v_before
    try:
        missing = [c for c in cols if c not in {f.name for f in ds.schema}]
        if missing:
            raise RuntimeError(f"hash columns absent from schema: {missing}")

        if "sba_surrogate_id" not in {f.name for f in ds.schema}:
            con = duckdb.connect(":memory:")
            con.execute("PRAGMA threads=4;")
            con.execute("SET memory_limit='12GB';")
            con.execute("SET temp_directory='/tmp/sba_patch_spill';")
            con.register("rdr", ds.scanner(columns=cols, with_row_id=True).to_reader())
            con.execute("CREATE TABLE t AS SELECT * FROM rdr")
            con.unregister("rdr")
            q = (
                "WITH hashed AS (SELECT _rowid, sha256(" + ser + ") AS _h FROM t)\n"
                "SELECT _h || '-' || CAST(row_number() OVER "
                "(PARTITION BY _h ORDER BY _h) AS VARCHAR) AS sba_surrogate_id\n"
                "FROM hashed ORDER BY _rowid"
            )
            arrow = con.execute(q).to_arrow_table().combine_chunks()
            con.close()
            ds.add_columns(arrow, batch_size=65536)   # positional zip in _rowid order
            ds = lance.dataset(uri, storage_options=so)
            added = "sba_surrogate_id"

        ds.create_scalar_index("sba_surrogate_id", index_type="BTREE", replace=True)
        ds = lance.dataset(uri, storage_options=so)
        v_after = ds.version

        # ── integrity gate ────────────────────────────────────────────────────
        con = duckdb.connect(":memory:")
        con.execute("PRAGMA threads=4;")
        con.execute("SET memory_limit='12GB';")
        con.execute("SET temp_directory='/tmp/sba_patch_spill';")
        con.register("rdr", ds.scanner(columns=cols + ["sba_surrogate_id"]).to_reader())
        con.execute("CREATE TABLE v AS SELECT * FROM rdr")
        con.unregister("rdr")
        bad = con.execute(
            f"SELECT count(*) FROM v WHERE split_part(sba_surrogate_id, '-', 1) <> sha256({ser})"
        ).fetchone()[0]
        n1, dist, nulls = con.execute(
            "SELECT count(*), count(DISTINCT sba_surrogate_id), "
            "count(*) FILTER (WHERE sba_surrogate_id IS NULL) FROM v"
        ).fetchone()
        dup_rows = con.execute(
            f"SELECT count(*) - count(DISTINCT {ser}) FROM v"
        ).fetchone()[0]
        con.close()
        idx = {i.get("name") if isinstance(i, dict) else getattr(i, "name", None)
               for i in ds.list_indices()}
        ok = (bad == 0 and n1 == n0 and n1 == dist and nulls == 0
              and "sba_surrogate_id_idx" in idx)
        if not ok:
            lance.dataset(uri, storage_options=so, version=v_before).restore()
            raise RuntimeError(
                f"integrity gate failed (rolled back to v{v_before}): hash_prefix_mismatch={bad} "
                f"rows={n1}/{n0} distinct={dist} nulls={nulls} indices={sorted(idx)}")
        result = {"rows": n1, "distinct_surrogate": dist, "exact_dup_rows": dup_rows,
                  "hash_prefix_mismatches": bad, "nulls": nulls, "indices": sorted(idx)}
        status = "success"
    except Exception as exc:  # noqa: BLE001 — terminal handling below + re-raise
        error = str(exc)
        status = "error"
    finally:
        completed = dt.datetime.now(dt.timezone.utc)
        _record_patch(uri, f"add_sba_surrogate_id:{program}", added,
                      "sba_surrogate_id_idx" if status == "success" else None,
                      result.get("rows"), result.get("exact_dup_rows"),
                      v_before, v_after, status, error, started, completed)
        _post_callback(trigger_callback_url,
                       {"status": status, "dataset_uri": uri, "program": program,
                        "operation": "add_sba_surrogate_id", **result})

    if status != "success":
        raise RuntimeError(f"patch_surrogate_id failed for {program}: {error}")
    return {"dataset_uri": uri, "program": program, "operation": "add_sba_surrogate_id",
            "version_before": v_before, "version_after": v_after, **result}


@app.local_entrypoint()
def run_all(as_of: str = AS_OF_DEFAULT) -> None:
    """Spawn both programs on the deployed app (distinct datasets → independent).
    No callback (manual). Prints one fc-id per program and returns."""
    from pipelines._shared.launch import spawn_deployed

    for program in ("504", "7a"):
        print(f"\n=== {program} ===")
        spawn_deployed("sba-7a-504", "ingest_sba_program", deploy_file=__file__,
                       program=program, as_of=as_of, trigger_callback_url=None)


@app.local_entrypoint()
def run_one(program: str, as_of: str = AS_OF_DEFAULT) -> None:
    from pipelines._shared.launch import spawn_deployed

    spawn_deployed("sba-7a-504", "ingest_sba_program", deploy_file=__file__,
                   program=program, as_of=as_of, trigger_callback_url=None)


@app.local_entrypoint()
def reindex(program: str = "") -> None:
    """Rebuild BTREE indexes on existing datasets (no re-ingest). Defaults to both."""
    for p in ([program] if program else ["504", "7a"]):
        print(reindex_program.remote(p))


@app.local_entrypoint()
def patch_surrogate(program: str = "") -> None:
    """In-place additive patch: mint + BTREE-index sba_surrogate_id on the existing
    program dataset(s) WITHOUT recreating them. Defaults to both programs."""
    import json

    for p in ([program] if program else ["504", "7a"]):
        print(f"\n=== {p} ===")
        print(json.dumps(patch_surrogate_id.remote(p), indent=2, default=str))
