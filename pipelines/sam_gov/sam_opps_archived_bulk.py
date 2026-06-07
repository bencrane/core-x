"""SAM.gov Contract Opportunities *archived* (inactive) universe ingest — local runner.

WHY THIS EXISTS (the correction): the active daily extract
``Contract Opportunities/datagov/ContractOpportunitiesFullCSV.csv`` is **active-only**
— measured 2026-06-07 it is 100% ``Active=YES`` (~77.7K rows), so the ``WHERE
Active='YES'`` filter in ``sam_opps_bulk.py`` drops nothing and the historical
universe is simply absent from that file. SAM publishes archived (inactive)
opportunities as a SEPARATE, weekly-refreshed dataset — one CSV per fiscal year:

    s3://falextracts/Contract Opportunities/Archived Data/FY{YYYY}_archived_opportunities.csv

Coverage FY1998..FY2026 (~19.9 GB total; substantive FY2002+). The archived schema
is a SUPERSET of the active one and crucially carries ``NoticeId`` (32-char) — so the
attachment manifest harvester (``sam_attachment_manifest.py``) runs UNCHANGED on this
universe via ``SAM_OPPS_LANCE_URI`` once it is materialized here.

This is the Play-1 (PE target-profiling) substrate: the historical notice/solicitation/
award rows whose ``award_number`` (PIID) and ``solicitation_number`` join back to
USASpending FPDS (clean ``recipient_uei``) and forward to ``/resources`` attachments.

DATA PLANE — write LOCAL, publish via boto3 (the proven R2-safe pattern, see
usaspending_bulk.py). Lance's own object-store writer escalates multipart part sizes
mid-upload for large files; R2 rejects unequal non-trailing parts (400 InvalidPart).
So:
    S3 archived CSV (per FY)
      → requests stream + cp1252->utf8 transcode → /tmp cache        (Python I/O)
      → DuckDB read_csv → cast/clean/shape (100% SQL)
      → lance.write_dataset(LOCAL /tmp/..lance, append per FY)        (no R2 multipart)
      → create_scalar_index on the LOCAL dataset
      → boto3 s3.upload_file walk → s3://data-sink/sam-gov-opps/archived/  (uniform parts)

IDEMPOTENT at the dataset grain: the publish wipes + replaces the R2 prefix, so a
re-run fully rebuilds (CSV cache makes re-runs cheap). Per-FY terminal rows →
``ops.sam_opps_archived_runs``.

    doppler run --project core-x --config prd -- \
      uv run --with pylance --with pyarrow --with duckdb --with requests \
        --with boto3 --with 'psycopg[binary]' \
      python pipelines/sam_gov/sam_opps_archived_bulk.py \
        --fy-list 2026,2025,2024,2023,2022,2021,2020,2019

Smoke (tiny FY, throwaway URI, no index):
    ... python pipelines/sam_gov/sam_opps_archived_bulk.py \
        --fy-list 1998 --dataset-uri s3://data-sink/active/_smoke_archived_universe/ --no-index
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import shutil

ARCHIVE_BASE = ("https://s3.amazonaws.com/falextracts/"
                "Contract%20Opportunities/Archived%20Data/FY{fy}_archived_opportunities.csv")
DATASET_URI = os.environ.get("SAM_OPPS_ARCHIVED_URI", "s3://data-sink/sam-gov-opps/archived/")
FEED = "sam_opps_archived"

# Local Lance fragment hygiene (these are LOCAL writes, no R2 multipart concern; the
# publish step is what makes R2 happy). Keeps fragment count + memory sane.
MAX_ROWS_PER_FILE = 250_000
MAX_BYTES_PER_FILE = 2 * 1024 ** 3

# DuckDB does 100% of the transform. all_varchar + explicit TRY_CAST → NULL (never a
# hard parse failure) over a 47-column government CSV. Output column names match the
# active universe (sam_opps_bulk.py) so the harvester reads either interchangeably;
# the archived superset adds profiling-grade fields (FPDS code, POC, org type, vendor
# address) the active projection omits. __CSV_PATH__ / __FY__ are controlled tokens.
TRANSFORM_SQL = """
WITH raw AS (
    SELECT * FROM read_csv('__CSV_PATH__', all_varchar = true, header = true,
                           sample_size = -1, ignore_errors = true)
)
SELECT
    nullif(trim("NoticeId"), '')                 AS notice_id,
    nullif(trim("Title"), '')                    AS title,
    nullif(trim("Sol#"), '')                     AS solicitation_number,
    nullif(trim("Department/Ind.Agency"), '')    AS department_agency,
    nullif(trim("Sub-Tier"), '')                 AS sub_tier,
    nullif(trim("FPDS Code"), '')                AS fpds_code,
    nullif(trim("Office"), '')                   AS office,
    TRY_CAST("PostedDate" AS TIMESTAMP)          AS posted_date,
    nullif(trim("Type"), '')                     AS notice_type,
    nullif(trim("BaseType"), '')                 AS base_type,
    nullif(trim("ArchiveType"), '')              AS archive_type,
    TRY_CAST("ArchiveDate" AS DATE)              AS archive_date,
    nullif(trim("SetASideCode"), '')             AS set_aside_code,
    nullif(trim("SetASide"), '')                 AS set_aside,
    TRY_CAST("ResponseDeadLine" AS TIMESTAMPTZ)  AS response_deadline,
    nullif(trim("NaicsCode"), '')                AS naics_code,
    nullif(trim("ClassificationCode"), '')       AS classification_code,
    nullif(trim("PopCity"), '')                  AS pop_city,
    nullif(trim("PopState"), '')                 AS pop_state,
    nullif(trim("PopZip"), '')                   AS pop_zip,
    nullif(trim("PopCountry"), '')               AS pop_country,
    nullif(trim("Active"), '')                   AS active,
    nullif(trim("AwardNumber"), '')              AS award_number,
    TRY_CAST("AwardDate" AS DATE)                AS award_date,
    TRY_CAST(replace(replace("Award$", '$', ''), ',', '') AS DOUBLE) AS award_amount,
    nullif(trim("Awardee"), '')                  AS awardee,
    nullif(trim("PrimaryContactFullname"), '')   AS primary_contact_fullname,
    nullif(trim("PrimaryContactEmail"), '')      AS primary_contact_email,
    nullif(trim("PrimaryContactPhone"), '')      AS primary_contact_phone,
    nullif(trim("OrganizationType"), '')         AS organization_type,
    nullif(trim("State"), '')                    AS vendor_state,
    nullif(trim("City"), '')                     AS vendor_city,
    nullif(trim("ZipCode"), '')                  AS vendor_zip,
    nullif(trim("CountryCode"), '')              AS vendor_country,
    nullif(trim("AdditionalInfoLink"), '')       AS additional_info_link,
    nullif(trim("Link"), '')                     AS link,
    nullif(trim("Description"), '')              AS description,
    '__FY__'                                      AS source_fy
FROM raw
WHERE nullif(trim("NoticeId"), '') IS NOT NULL
"""


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
    """boto3 S3 client for R2. checksum behaviour ``when_required`` — botocore's
    default flexible-checksum validation does not match R2 (breaks get/put/multipart)."""
    import boto3
    from botocore.config import Config

    so = _r2_storage_options()
    cfg = Config(request_checksum_calculation="when_required",
                 response_checksum_validation="when_required")
    return boto3.client(
        "s3", endpoint_url=so["endpoint"], region_name="auto",
        aws_access_key_id=so["aws_access_key_id"],
        aws_secret_access_key=so["aws_secret_access_key"], config=cfg)


def _publish_to_r2(dataset_uri: str, local_dir: str) -> int:
    """Wipe the R2 key prefix, then upload the local Lance dataset via boto3
    s3transfer (uniform-part multipart, R2-compliant — unlike Lance's object-store
    writer). Returns files uploaded."""
    assert dataset_uri.startswith("s3://"), dataset_uri
    bucket, prefix = dataset_uri[len("s3://"):].split("/", 1)
    if not prefix.endswith("/"):
        prefix += "/"
    s3 = _s3_client()
    to_del: list[dict] = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for o in page.get("Contents", []):
            to_del.append({"Key": o["Key"]})
            if len(to_del) == 1000:
                s3.delete_objects(Bucket=bucket, Delete={"Objects": to_del, "Quiet": True})
                to_del = []
    if to_del:
        s3.delete_objects(Bucket=bucket, Delete={"Objects": to_del, "Quiet": True})
    uploaded = 0
    for root, _, files in os.walk(local_dir):
        for fn in files:
            lp = os.path.join(root, fn)
            rel = os.path.relpath(lp, local_dir).replace(os.sep, "/")
            s3.upload_file(lp, bucket, prefix + rel)
            uploaded += 1
    return uploaded


def _record_run(dsn: str | None, fy: str, rows: int, status: str,
                started_at, completed_at, error: str | None) -> None:
    if not dsn:
        print("WARN: no HQX_DB_URL_POOLED; skipping ops.* write.", flush=True)
        return
    try:
        import psycopg

        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS ops.sam_opps_archived_runs (
                    id bigserial PRIMARY KEY, feed text NOT NULL, source_fy text NOT NULL,
                    rows_processed int, status text NOT NULL, error text,
                    started_at timestamptz, completed_at timestamptz)
                """
            )
            cur.execute(
                """
                INSERT INTO ops.sam_opps_archived_runs
                    (feed, source_fy, rows_processed, status, error, started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (FEED, fy, rows, status, error, started_at, completed_at),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must not mask the ingest
        print(f"WARN: ops.* write failed: {exc}", flush=True)


def _download_fy(fy: str, cache_dir: str) -> str:
    """Stream the archived FY CSV to local cache, transcoding cp1252→utf8 (lossless;
    SAM exports are cp1252, DuckDB rejects its 0x80-0x9F printables). Reuse cache."""
    import requests

    dst = os.path.join(cache_dir, f"sam_archived_FY{fy}.csv")
    if os.path.exists(dst) and os.path.getsize(dst) > 1_000_000:
        print(f"  [FY{fy}] cache hit ({os.path.getsize(dst) / 1e9:.2f} GB)", flush=True)
        return dst
    url = ARCHIVE_BASE.format(fy=fy)
    n = 0
    with requests.get(url, stream=True, timeout=(30, 3600)) as r:
        r.raise_for_status()
        with open(dst, "wb") as fh:
            for chunk in r.iter_content(1 << 20):
                if chunk:
                    fh.write(chunk.decode("cp1252", errors="replace").encode("utf-8"))
                    n += len(chunk)
    print(f"  [FY{fy}] downloaded ~{n / 1e9:.2f} GB -> {dst}", flush=True)
    return dst


def run(fy_list: list[str], dataset_uri: str, cache_dir: str, build_index: bool,
        publish: bool) -> None:
    import duckdb
    import lance

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    local_ds = os.path.join(cache_dir, "sam_archived_lance")
    shutil.rmtree(local_ds, ignore_errors=True)  # clean rebuild (dataset-grain idempotent)

    grand_total = 0
    for fy in fy_list:
        started_at = dt.datetime.now(dt.timezone.utc)
        rows, status, error = 0, "error", None
        try:
            path = _download_fy(fy, cache_dir)
            sql = TRANSFORM_SQL.replace("__CSV_PATH__", path).replace("__FY__", fy)
            con = duckdb.connect(":memory:")
            try:
                con.execute("PRAGMA threads=4;")
                con.execute(f"PRAGMA temp_directory='{cache_dir}/duckdb_spill';")
                at = con.execute(sql).to_arrow_table()
            finally:
                con.close()
            rows = at.num_rows
            mode = "append" if os.path.exists(local_ds) else "create"
            lance.write_dataset(at, local_ds, mode=mode,
                                data_storage_version="2.0",
                                max_rows_per_file=MAX_ROWS_PER_FILE,
                                max_bytes_per_file=MAX_BYTES_PER_FILE)
            grand_total += rows
            status = "success"
            print(f"[FY{fy}] {mode} {rows:,} rows  (local cumulative {grand_total:,})", flush=True)
        except Exception as exc:  # noqa: BLE001
            error = str(exc)
            print(f"[FY{fy}] FAILED: {exc}", flush=True)
        finally:
            _record_run(dsn, fy, int(rows), status, started_at,
                        dt.datetime.now(dt.timezone.utc), error)

    if build_index:
        print("building scalar indices on LOCAL dataset ...", flush=True)
        ds = lance.dataset(local_ds)
        for col, it in [("notice_id", "BTREE"), ("solicitation_number", "BTREE"),
                        ("award_number", "BTREE"), ("naics_code", "BTREE"),
                        ("notice_type", "BITMAP"), ("active", "BITMAP")]:
            try:
                ds.create_scalar_index(col, index_type=it)
                print(f"  index {col} ({it}) ok", flush=True)
            except Exception as ie:  # noqa: BLE001
                print(f"  index {col} skipped: {ie}", flush=True)

    if publish:
        print(f"publishing LOCAL -> {dataset_uri} via boto3 (uniform parts) ...", flush=True)
        n = _publish_to_r2(dataset_uri, local_ds)
        print(f"published {n} files -> {dataset_uri}", flush=True)
        # verify
        ds = lance.dataset(dataset_uri, storage_options=_r2_storage_options())
        print(f"VERIFY: R2 dataset rows = {ds.count_rows():,}", flush=True)

    print(f"DONE. local rows built = {grand_total:,}  (local_ds={local_ds})", flush=True)


def _cli() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--fy-list", required=True, help="comma-separated fiscal years, e.g. 2026,2025,2024")
    p.add_argument("--dataset-uri", default=DATASET_URI)
    p.add_argument("--cache-dir", default="/tmp")
    p.add_argument("--no-index", dest="build_index", action="store_false", default=True)
    p.add_argument("--no-publish", dest="publish", action="store_false", default=True,
                   help="build + index LOCAL only; skip the R2 publish (smoke)")
    a = p.parse_args()
    fys = [x.strip() for x in a.fy_list.split(",") if x.strip()]
    run(fys, a.dataset_uri, a.cache_dir, a.build_index, a.publish)


if __name__ == "__main__":
    _cli()
