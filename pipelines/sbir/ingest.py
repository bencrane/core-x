"""SBIR/STTR award ingest — wholesale projection + Lance write (Gen-3 SoR).

Manual in-session load: the operator landed two CSVs at s3://data-sink/landing/sbir/
(award_data.csv 394MB w/ Abstract; award_data_no_abstract.csv 91MB). This worker
ingests the FULL file only (the lean file is a strict column-subset). Clean-room data
plane: DuckDB does 100% of the transform, Lance is written straight to R2. Enforces the
cleaning contract proven in docs/sbir_structural_diagnostic.md.

  doppler run -- python pipelines/sbir/ingest.py                                  # read R2 landing
  doppler run -- python pipelines/sbir/ingest.py --source /tmp/sbir_audit/award_data.csv
  doppler run -- python pipelines/sbir/ingest.py --reindex-only                   # rebuild indices

Env (Doppler core-x/prd): R2_ENDPOINT R2_ACCESS_KEY_ID R2_SECRET_ACCESS_KEY [HQX_DB_URL_POOLED]

Topology: ONE standalone dataset, s3://data-sink/active/sbir_awards/. mode="overwrite"
(wholesale snapshot of a manually-landed export). No natural PK exists (diagnostic: best
composite ATN+Phase+Agency collides on 2,041 rows) -> sbir_surrogate_id = sha256 over all
published attributes + per-hash ordinal disambiguates the 3 byte-identical rows and keeps
the key idempotent across overwrite rebuilds.
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import sys

BUCKET = "data-sink"
DEFAULT_SOURCE = f"s3://{BUCKET}/landing/sbir/award_data.csv"
DATASET_URI = os.environ.get("SBIR_AWARDS_LANCE_URI", f"s3://{BUCKET}/active/sbir_awards/")
SCRATCH = "/tmp/sbir_ingest"

# ── Cleaned, published columns (snake_case). ORDER IS LOAD-BEARING: this exact list
# is the sha256 hash-input set (excludes the surrogate, uei_valid, ingested_at,
# source_file). Keep in sync with the projection below. ───────────────────────────
PUBLISHED = [
    "company", "award_title", "agency", "branch", "phase", "program",
    "agency_tracking_number", "contract", "proposal_award_date", "contract_end_date",
    "solicitation_number", "solicitation_year", "solicitation_close_date",
    "proposal_receipt_date", "date_of_notification", "topic_code", "award_year",
    "award_amount", "uei", "duns", "hubzone_owned",
    "socially_economically_disadvantaged", "woman_owned", "number_employees",
    "company_website", "address1", "address2", "city", "state", "zip", "abstract",
    "contact_name", "contact_title", "contact_phone", "contact_email", "pi_name",
    "pi_title", "pi_phone", "pi_email", "ri_name", "ri_poc_name", "ri_poc_phone",
]

# BTREE on high-cardinality resolution keys; BITMAP on low-cardinality categoricals.
BTREE_COLS = ["sbir_surrogate_id", "uei", "duns", "company",
              "agency_tracking_number", "contract"]
BITMAP_COLS = ["phase", "program", "agency", "state", "award_year",
               "hubzone_owned", "woman_owned", "socially_economically_disadvantaged"]

# Full USPS map for the 55 distinct State values present (50 states + DC + 4 territories).
# Every landed value maps -> zero rows nulled by the geo step (verified pre-run).
_USPS = {
    "ALABAMA": "AL", "ALASKA": "AK", "ARIZONA": "AZ", "ARKANSAS": "AR",
    "CALIFORNIA": "CA", "COLORADO": "CO", "CONNECTICUT": "CT", "DELAWARE": "DE",
    "DISTRICT OF COLUMBIA": "DC", "FLORIDA": "FL", "GEORGIA": "GA", "GUAM": "GU",
    "HAWAII": "HI", "IDAHO": "ID", "ILLINOIS": "IL", "INDIANA": "IN", "IOWA": "IA",
    "KANSAS": "KS", "KENTUCKY": "KY", "LOUISIANA": "LA", "MAINE": "ME",
    "MARSHALL ISLANDS": "MH", "MARYLAND": "MD", "MASSACHUSETTS": "MA",
    "MICHIGAN": "MI", "MINNESOTA": "MN", "MISSISSIPPI": "MS", "MISSOURI": "MO",
    "MONTANA": "MT", "NEBRASKA": "NE", "NEVADA": "NV", "NEW HAMPSHIRE": "NH",
    "NEW JERSEY": "NJ", "NEW MEXICO": "NM", "NEW YORK": "NY", "NORTH CAROLINA": "NC",
    "NORTH DAKOTA": "ND", "OHIO": "OH", "OKLAHOMA": "OK", "OREGON": "OR",
    "PENNSYLVANIA": "PA", "PUERTO RICO": "PR", "RHODE ISLAND": "RI",
    "SOUTH CAROLINA": "SC", "SOUTH DAKOTA": "SD", "TENNESSEE": "TN", "TEXAS": "TX",
    "UTAH": "UT", "VERMONT": "VT", "VIRGIN ISLANDS": "VI", "VIRGINIA": "VA",
    "WASHINGTON": "WA", "WEST VIRGINIA": "WV", "WISCONSIN": "WI", "WYOMING": "WY",
}

# Literal null-sentinels purged from textual columns (diagnostic §5.2).
_SENTINELS = ("n/a", "na", "none", "null", "nan", "tbd", "#n/a")
# Abstract placeholder set (normalized: lowercased, trailing '.' stripped). The <50-char
# rule already catches every observed placeholder; this is the belt-and-suspenders match.
_ABS_PLACEHOLDERS = (
    "n/a", "na", "none", "null", "nan", "tbd", "redacted", "not available",
    "not avaiable", "not avaialble", "xxx", "blank", "abstract",
    "in process for public release",
)


def _so() -> dict:
    """object_store options for Cloudflare R2 (lance storage_options)."""
    endpoint = os.environ.get("R2_ENDPOINT")
    acct = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and acct:
        endpoint = f"https://{acct}.r2.cloudflarestorage.com"
    if not endpoint:
        raise SystemExit("Set R2_ENDPOINT (or R2_ACCOUNT_ID). Run under `doppler run`.")
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": endpoint,
        "region": "auto",
    }


def _fetch_source(source: str) -> str:
    """If source is an s3:// URI, download it to SCRATCH (reuse if present); else
    return the local path. DuckDB then reads a local file (one clean pass)."""
    if not source.startswith("s3://"):
        return source
    import boto3
    from botocore.config import Config

    os.makedirs(SCRATCH, exist_ok=True)
    key = source.split(f"s3://{BUCKET}/", 1)[1]
    local = os.path.join(SCRATCH, os.path.basename(key))
    if os.path.exists(local) and os.path.getsize(local) > 0:
        print(f"reuse   {local}")
        return local
    so = _so()
    cfg = Config(request_checksum_calculation="when_required",
                 response_checksum_validation="when_required")
    s3 = boto3.client("s3", endpoint_url=so["endpoint"],
                      aws_access_key_id=so["aws_access_key_id"],
                      aws_secret_access_key=so["aws_secret_access_key"],
                      region_name="auto", config=cfg)
    print(f"download s3://{BUCKET}/{key} -> {local}")
    s3.download_file(BUCKET, key, local)
    return local


def _macros() -> str:
    cases = " ".join(f"WHEN '{k}' THEN '{v}'" for k, v in _USPS.items())
    sent = ", ".join(f"'{s}'" for s in _SENTINELS)
    return f"""
CREATE OR REPLACE TEMP MACRO clean_text(x) AS (
  CASE WHEN lower(trim(x)) IN ({sent}) THEN NULL ELSE nullif(trim(x), '') END
);
CREATE OR REPLACE TEMP MACRO vdate(x) AS (
  CASE WHEN TRY_CAST(nullif(trim(x), '') AS DATE)
            BETWEEN DATE '1982-01-01' AND (current_date + INTERVAL 2 YEAR)
       THEN TRY_CAST(nullif(trim(x), '') AS DATE) ELSE NULL END
);
CREATE OR REPLACE TEMP MACRO usps(s) AS (
  CASE upper(trim(s)) {cases}
       ELSE CASE WHEN regexp_full_match(upper(trim(s)), '[A-Z]{{2}}')
                 THEN upper(trim(s)) ELSE NULL END END
);
"""


def _build_sql(local_source: str) -> str:
    src = local_source.replace("'", "''")
    base = os.path.basename(local_source).replace("'", "''")
    abs_ph = ", ".join(f"'{p}'" for p in _ABS_PLACEHOLDERS)
    serial = "concat_ws(chr(31), " + ", ".join(
        f"coalesce(CAST({c} AS VARCHAR), chr(30))" for c in PUBLISHED) + ")"
    return f"""
WITH src AS (
  SELECT * FROM read_csv('{src}', all_varchar=true, header=true,
                         sample_size=-1, ignore_errors=false)
),
proj AS (
  SELECT
    clean_text("Company")                              AS company,
    clean_text("Award Title")                          AS award_title,
    clean_text("Agency")                               AS agency,
    clean_text("Branch")                               AS branch,
    clean_text("Phase")                                AS phase,
    clean_text("Program")                              AS program,
    clean_text("Agency Tracking Number")               AS agency_tracking_number,
    clean_text("Contract")                             AS contract,
    vdate("Proposal Award Date")                       AS proposal_award_date,
    vdate("Contract End Date")                          AS contract_end_date,
    clean_text("Solicitation Number")                  AS solicitation_number,
    TRY_CAST(nullif(trim("Solicitation Year"), '') AS SMALLINT) AS solicitation_year,
    vdate("Solicitation Close Date")                   AS solicitation_close_date,
    vdate("Proposal Receipt Date")                     AS proposal_receipt_date,
    vdate("Date of Notification")                       AS date_of_notification,
    clean_text("Topic Code")                           AS topic_code,
    TRY_CAST(nullif(trim("Award Year"), '') AS SMALLINT) AS award_year,
    TRY_CAST(replace(replace(nullif(trim("Award Amount"), ''), ',', ''), '$', '') AS DECIMAL(18,2)) AS award_amount,
    clean_text("UEI")                                  AS uei,
    CASE WHEN regexp_full_match(trim("Duns"), '[0-9]{{3,9}}')
         THEN lpad(trim("Duns"), 9, '0') ELSE NULL END AS duns,
    nullif(upper(trim("HUBZone Owned")), '')           AS hubzone_owned,
    nullif(upper(trim("Socially and Economically Disadvantaged")), '') AS socially_economically_disadvantaged,
    nullif(upper(trim("Woman Owned")), '')             AS woman_owned,
    TRY_CAST(nullif(trim("Number Employees"), '') AS INTEGER) AS number_employees,
    clean_text("Company Website")                      AS company_website,
    clean_text("Address1")                             AS address1,
    clean_text("Address2")                             AS address2,
    clean_text("City")                                 AS city,
    usps("State")                                      AS state,
    CASE WHEN length(regexp_replace(trim("Zip"), '[^0-9]', '', 'g')) >= 5
         THEN left(regexp_replace(trim("Zip"), '[^0-9]', '', 'g'), 5) ELSE NULL END AS zip,
    CASE
      WHEN nullif(trim("Abstract"), '') IS NULL THEN NULL
      WHEN length(trim("Abstract")) < 50 THEN NULL
      WHEN lower(rtrim(trim("Abstract"), '.')) IN ({abs_ph}) THEN NULL
      ELSE regexp_replace(trim("Abstract"), '\\s+', ' ', 'g')
    END                                                AS abstract,
    clean_text("Contact Name")                         AS contact_name,
    clean_text("Contact Title")                        AS contact_title,
    clean_text("Contact Phone")                        AS contact_phone,
    clean_text("Contact Email")                        AS contact_email,
    clean_text("PI Name")                              AS pi_name,
    clean_text("PI Title")                             AS pi_title,
    clean_text("PI Phone")                             AS pi_phone,
    clean_text("PI Email")                             AS pi_email,
    clean_text("RI Name")                              AS ri_name,
    clean_text("RI POC Name")                          AS ri_poc_name,
    clean_text("RI POC Phone")                         AS ri_poc_phone,
    -- derived + provenance (NOT in the hash)
    CASE WHEN clean_text("UEI") IS NULL THEN NULL
         ELSE regexp_full_match(clean_text("UEI"), '[A-Za-z0-9]{{12}}') END AS uei_valid,
    now()                                              AS ingested_at,
    '{base}'                                           AS source_file
  FROM src
),
hashed AS (SELECT *, sha256({serial}) AS _h FROM proj)
SELECT
  _h || '-' || CAST(row_number() OVER (PARTITION BY _h ORDER BY _h) AS VARCHAR) AS sbir_surrogate_id,
  * EXCLUDE (_h)
FROM hashed
"""


def _create_indexes(ds, so) -> dict:
    import lance

    built = {"BTREE": [], "BITMAP": [], "failed": []}
    for col in BTREE_COLS:
        try:
            ds.create_scalar_index(col, "BTREE", replace=True)
            built["BTREE"].append(col)
        except Exception as exc:  # noqa: BLE001
            built["failed"].append(f"BTREE:{col}:{exc}")
    for col in BITMAP_COLS:
        try:
            ds.create_scalar_index(col, "BITMAP", replace=True)
            built["BITMAP"].append(col)
        except Exception as exc:  # noqa: BLE001
            built["failed"].append(f"BITMAP:{col}:{exc}")
    return built


OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.sbir_awards_runs (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dataset_uri   text        NOT NULL,
    source_file   text,
    rows          bigint,
    distinct_pk   bigint,
    exact_dup_rows bigint,
    indices       text,
    status        text        NOT NULL,
    error         text,
    started_at    timestamptz,
    completed_at  timestamptz,
    recorded_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS sbir_awards_runs_recorded_idx ON ops.sbir_awards_runs (recorded_at DESC);
"""


def _record_run(**row) -> None:
    """Terminal run row -> ops.sbir_awards_runs (best-effort; never masks the load)."""
    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* write.")
        return
    try:
        import psycopg

        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
            cur.execute(
                """INSERT INTO ops.sbir_awards_runs
                   (dataset_uri, source_file, rows, distinct_pk, exact_dup_rows,
                    indices, status, error, started_at, completed_at)
                   VALUES (%(dataset_uri)s, %(source_file)s, %(rows)s, %(distinct_pk)s,
                    %(exact_dup_rows)s, %(indices)s, %(status)s, %(error)s,
                    %(started_at)s, %(completed_at)s)""", row)
            conn.commit()
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: ops.sbir_awards_runs write failed: {exc}")


def run(source: str, reindex_only: bool = False) -> dict:
    import duckdb
    import lance

    so = _so()
    started = dt.datetime.now(dt.timezone.utc)

    if reindex_only:
        ds = lance.dataset(DATASET_URI, storage_options=so)
        built = _create_indexes(ds, so)
        print(f"reindex: {built}")
        return {"dataset_uri": DATASET_URI, "indices": built}

    local = _fetch_source(source)
    os.makedirs(f"{SCRATCH}/spill", exist_ok=True)

    status, error, n, distinct_pk, dup_rows, built = "error", None, 0, 0, 0, {}
    try:
        con = duckdb.connect(":memory:")
        try:
            con.execute("PRAGMA threads=4;")
            con.execute("SET memory_limit='8GB';")
            con.execute(f"SET temp_directory='{SCRATCH}/spill';")
            con.execute("SET preserve_insertion_order=false;")
            con.execute(_macros())
            print(f"project  {local} -> Arrow ...")
            table = con.sql(_build_sql(local)).to_arrow_table()
        finally:
            con.close()
        print(f"rows     {table.num_rows:,}")

        lance.write_dataset(
            table, DATASET_URI, mode="overwrite",
            data_storage_version="2.0", max_rows_per_file=1_048_576,
            storage_options=so,
        )
        ds = lance.dataset(DATASET_URI, storage_options=so)
        built = _create_indexes(ds, so)

        # post-write verification on the committed dataset (PK uniqueness gate)
        con2 = duckdb.connect(":memory:")
        con2.register("rdr", ds.scanner(columns=["sbir_surrogate_id"]).to_reader())
        distinct_pk = con2.execute(
            "SELECT count(DISTINCT sbir_surrogate_id) FROM rdr").fetchone()[0]
        con2.close()
        n = ds.count_rows()
        dup_rows = n - distinct_pk
        status = "success"
    except Exception as exc:  # noqa: BLE001 — record terminal state, then re-raise
        error = str(exc)
        status = "error"
    finally:
        completed = dt.datetime.now(dt.timezone.utc)
        idx_summary = (f"BTREE={built.get('BTREE')} BITMAP={built.get('BITMAP')} "
                       f"failed={built.get('failed')}")
        _record_run(dataset_uri=DATASET_URI, source_file=os.path.basename(local), rows=n,
                    distinct_pk=distinct_pk, exact_dup_rows=dup_rows, indices=idx_summary,
                    status=status, error=error, started_at=started, completed_at=completed)

    if status != "success":
        raise SystemExit(f"ingest failed: {error}")
    if n != distinct_pk:
        raise SystemExit(f"PK NOT UNIQUE: rows={n} distinct={distinct_pk}")
    return {"dataset_uri": DATASET_URI, "rows": n, "distinct_pk": distinct_pk,
            "pk_unique": n == distinct_pk, "indices": built, "status": status}


def main() -> None:
    ap = argparse.ArgumentParser(description="SBIR/STTR award Lance ingest")
    ap.add_argument("--source", default=DEFAULT_SOURCE,
                    help=f"CSV path or s3:// URI (default: {DEFAULT_SOURCE})")
    ap.add_argument("--reindex-only", action="store_true",
                    help="rebuild scalar indices on the existing dataset, no re-ingest")
    args = ap.parse_args()
    import json

    result = run(args.source, reindex_only=args.reindex_only)
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    sys.exit(main())
