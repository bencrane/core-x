"""Compute worker — FMCSA census → direct-mail-ready serving dataset (Pattern-B).

Modal app ``fmcsa-derived`` (the serving/projection tier of the FMCSA domain). It
is a separate app from the raw-ingest ``fmcsa-pipelines`` deliberately: Modal
deploys one module per app, so co-hosting would force this and ``fmcsa_bulk.py``
into a single file or clobber each other on deploy. This mirrors the established
precedent that derived datasets get their own app (the SAM↔FMCSA bridge runs under
``resolution-pipelines``). Both still live under ``pipelines/fmcsa/`` per the
domain-grouping rule. A NON-DESTRUCTIVE derived projection of
the census system-of-record: it reads ``active/fmcsa/census/`` (read-only) and
materializes a focused, Lob-shaped serving dataset at
``active/fmcsa/census_mail_ready/``. It never mutates the census SoR, so it cannot
race the daily Trigger.dev ingest and cannot break the SAM↔FMCSA domain bridge,
both of which depend on census's named columns.

Why a derived dataset and NOT an in-place rewrite of census:
    The census active feed is ALREADY semantically named (the tabular ``az4n-8mr2``
    bulk export carries a header row; ``fmcsa_bulk.py`` projects it with
    ``normalize_names=true``) and is ALREADY BTREE-indexed on ``carrier_dot`` and
    ``proxy_domain``. Live read-back (2026-06-01, 4,437,561 rows):
        legal_name 100%  phy_street ~100%  phy_zip 99.97%  phone 96.6%
        company_officer_1 85.6%  email_address 65.5%  dba_name 26.5%
        proxy_domain 16.6% (consumer-mailbox-suppressed corporate domain)
    There is therefore no positional ``column00`` problem to solve on census and
    no field to "extract" by re-parsing — the mail block is present and clean. The
    only ``column00`` feed is ``active/fmcsa/carrier/`` (5,369 rows), which is the
    authority/status file (docket@0, USDOT@1, A/I status, N/Y authority flags) and
    carries NO address / officer / email — so it is not a mail source.

What this worker adds on top of census (the achievable direct-mail intent):
  - Standardized mailing block: structured physical + mailing address fields plus a
    ready-to-render ``mail_to_block`` (delivery address preferred, physical
    fallback) and a ``mailable`` deliverability flag.
  - Officer names and the company email kept as STRICTLY SEPARATE columns
    (``company_officer_1`` / ``company_officer_2`` vs ``email_address``). There is
    deliberately NO glued officer<->email anchor: census does not assert the officer
    owns the mailbox, and the on-file email is frequently a generic company inbox
    (``info@`` / ``dispatch@`` / ``safety@``). Materializing a 1:1 officer<->email
    string would manufacture a false identity and mis-target downstream GTM.
  - Contact anchors: phone / fax / cell, ``email_address`` + raw ``email_domain`` +
    the suppressed corporate ``proxy_domain`` (passed through from census, not
    recomputed — it already encodes the bridge's consumer-mailbox suppression).
  - MC number recovered from the ``docket{1,2,3}{,prefix}`` pairs (census carries
    no single docket column; ``carrier_docket`` is NULL in the tabular feed).
  - Low-cardinality status flags cleanly labelled and BTREE-indexed: status
    (Active/Inactive), carrier_operation (Inter/Intrastate), entity type
    (business_org), power units.

Data plane (clean-room — DuckDB does 100% of the transform):
    Lance(census, read-only)
      → DuckDB: address standardization + officer/email anchor + MC recovery
      → Arrow  → con.sql(...).to_arrow_table()
      → lance.write_dataset(LOCAL, v2.0) + BTREE(carrier_dot, proxy_domain,
            status_code, carrier_operation, business_org_id, power_units)
      → boto3 mirror → s3://data-sink/active/fmcsa/census_mail_ready/

    R2 NOTE (same as every other worker): Lance's native object-store writer emits
    variable-size multipart chunks that R2 rejects; so Lance is written to LOCAL
    scratch and mirrored with boto3 (uniform 8 MiB parts). DuckDB still does 100%
    of the transform; Lance is the format and R2 the system of record.

Idempotency: full overwrite of the derived prefix (cleared + replaced each run).
``ops.fmcsa_derived_runs`` records terminal state.

    modal run    pipelines/fmcsa/census_mail_ready.py            # build + verify
    modal run    pipelines/fmcsa/census_mail_ready.py --dry-run  # counts, no write
    modal run    pipelines/fmcsa/census_mail_ready.py --sample   # print 5 records
    modal deploy pipelines/fmcsa/census_mail_ready.py
"""

from __future__ import annotations

import os

import modal

BUCKET = "data-sink"
CENSUS_SRC_URI = "s3://data-sink/active/fmcsa/census/"
ACTIVE_PREFIX = os.environ.get("MAIL_READY_ACTIVE_PREFIX", "active/fmcsa/census_mail_ready")
DATASET_URI = f"s3://{BUCKET}/{ACTIVE_PREFIX}/"
LOCAL_DATASET = "/tmp/census_mail_ready"
FEED = "fmcsa/census_mail_ready"

# Lance fragment sizing — 90 GiB is Lance's documented default (one fragment for a
# dataset this size). Mirrors fmcsa_bulk.py / the bridge worker.
MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3

# Resolution keys + low-cardinality status flags get a hard BTREE scalar index
# (ARCHITECTURE.md §4). carrier_dot + proxy_domain preserve the census guardrail
# keys; the rest are the directive's status flags (Active/Inactive, entity type,
# power units, operation class).
INDEX_COLS = ("carrier_dot", "proxy_domain", "status_code",
              "carrier_operation", "business_org_id", "power_units")

# Only the columns the projection needs (pushdown — census is 155 cols wide).
SRC_COLS = [
    "carrier_dot", "legal_name", "dba_name",
    "phy_street", "phy_city", "phy_state", "phy_zip", "phy_country",
    "carrier_mailing_street", "carrier_mailing_city", "carrier_mailing_state",
    "carrier_mailing_zip", "carrier_mailing_country", "undeliv_phy",
    "phone", "fax", "cell_phone", "email_address", "proxy_domain",
    "company_officer_1", "company_officer_2",
    "status_code", "carrier_operation", "business_org_id", "business_org_desc",
    "power_units", "truck_units", "bus_units", "fleetsize",
    "docket1prefix", "docket1", "docket2prefix", "docket2", "docket3prefix", "docket3",
    "snapshot_date",
]

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",
    "lancedb>=0.15",
    "pylance>=0.19",
    "pyarrow>=17",
    "boto3>=1.35",
    "psycopg[binary]>=3.2",
)

app = modal.App("fmcsa-derived", image=image)


# --------------------------------------------------------------------------- #
# R2 / S3  (identical helper contract to the other fmcsa workers)
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
    cfg = Config(request_checksum_calculation="when_required",
                 response_checksum_validation="when_required")
    return boto3.client(
        "s3", endpoint_url=so["endpoint"],
        aws_access_key_id=so["aws_access_key_id"],
        aws_secret_access_key=so["aws_secret_access_key"],
        region_name="auto", config=cfg,
    )


def _clear_r2_prefix(s3, prefix: str) -> int:
    deleted, batch = 0, []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix + "/"):
        for o in page.get("Contents", []):
            batch.append({"Key": o["Key"]})
            if len(batch) == 1000:
                s3.delete_objects(Bucket=BUCKET, Delete={"Objects": batch, "Quiet": True})
                deleted += len(batch); batch = []
    if batch:
        s3.delete_objects(Bucket=BUCKET, Delete={"Objects": batch, "Quiet": True})
        deleted += len(batch)
    return deleted


def _upload_dir_to_r2(s3, local_dir: str, prefix: str) -> int:
    count = 0
    for root, _dirs, files in os.walk(local_dir):
        for fn in files:
            full = os.path.join(root, fn)
            rel = os.path.relpath(full, local_dir).replace(os.sep, "/")
            s3.upload_file(full, BUCKET, f"{prefix}/{rel}")
            count += 1
    return count


# --------------------------------------------------------------------------- #
# DuckDB transform — pure SQL (importable without modal/auth for unit tests)
# --------------------------------------------------------------------------- #
def _norm(col: str) -> str:
    r"""Trim, collapse internal whitespace, NULL-out blanks. concat_ws() skips only
    NULLs (not ''), so every block component must normalize to NULL when empty."""
    return (rf"nullif(regexp_replace(trim(CAST({col} AS VARCHAR)), '\s+', ' ', 'g'), '')")


def _mc_number_sql() -> str:
    """Recover the MC docket from the census docket{1,2,3}{,prefix} pairs (same
    rule as the SAM↔FMCSA bridge). Normalized to an unpadded integer string."""
    return (
        "CASE "
        "WHEN docket1prefix='MC' THEN CAST(TRY_CAST(docket1 AS BIGINT) AS VARCHAR) "
        "WHEN docket2prefix='MC' THEN CAST(TRY_CAST(docket2 AS BIGINT) AS VARCHAR) "
        "WHEN docket3prefix='MC' THEN CAST(TRY_CAST(docket3 AS BIGINT) AS VARCHAR) END"
    )


def build_mail_ready_sql(src: str = "census_src") -> str:
    """Full projection statement over a registered ``census_src`` relation."""
    legal = _norm("legal_name")
    dba = _norm("dba_name")
    p_st, p_ci, p_state, p_zip, p_cty = (_norm("phy_street"), _norm("phy_city"),
                                         _norm("phy_state"), _norm("phy_zip"),
                                         _norm("phy_country"))
    m_st, m_ci, m_state, m_zip, m_cty = (_norm("carrier_mailing_street"),
                                         _norm("carrier_mailing_city"),
                                         _norm("carrier_mailing_state"),
                                         _norm("carrier_mailing_zip"),
                                         _norm("carrier_mailing_country"))
    email = _norm("email_address")
    officer1 = _norm("company_officer_1")
    officer2 = _norm("company_officer_2")
    # raw email domain (no consumer suppression) — take after last @, cut illegal.
    email_domain = (r"nullif(regexp_replace(regexp_replace(lower(trim(email_address)),"
                    r"'^.*@',''),'[^a-z0-9.-].*$',''),'')")
    return f"""
WITH base AS (
    SELECT
        carrier_dot,
        {_mc_number_sql()}                                  AS mc_number,
        {legal}                                             AS legal_name,
        {dba}                                               AS dba_name,
        {p_st} AS phy_street, {p_ci} AS phy_city, {p_state} AS phy_state,
        {p_zip} AS phy_zip, {p_cty} AS phy_country,
        {m_st} AS mail_street, {m_ci} AS mail_city, {m_state} AS mail_state,
        {m_zip} AS mail_zip, {m_cty} AS mail_country,
        {_norm("phone")} AS phone, {_norm("fax")} AS fax,
        {_norm("cell_phone")} AS cell_phone,
        {email} AS email_address, {email_domain} AS email_domain,
        {_norm("proxy_domain")} AS proxy_domain,
        {officer1} AS company_officer_1, {officer2} AS company_officer_2,
        {_norm("status_code")} AS status_code,
        {_norm("carrier_operation")} AS carrier_operation,
        {_norm("business_org_id")} AS business_org_id,
        {_norm("business_org_desc")} AS business_org_desc,
        TRY_CAST(power_units AS BIGINT) AS power_units,
        TRY_CAST(truck_units AS BIGINT) AS truck_units,
        TRY_CAST(bus_units AS BIGINT)   AS bus_units,
        TRY_CAST(fleetsize AS BIGINT)   AS fleetsize,
        {_norm("undeliv_phy")} AS undeliv_phy,
        snapshot_date
    FROM {src}
)
SELECT
    carrier_dot,
    mc_number,
    legal_name,
    dba_name,
    -- physical address
    phy_street, phy_city, phy_state, phy_zip, phy_country,
    -- mailing (delivery) address
    mail_street, mail_city, mail_state, mail_zip, mail_country,
    -- ready-to-render delivery block: mailing preferred, physical fallback
    trim(BOTH chr(10) FROM concat_ws(chr(10),
        legal_name,
        CASE WHEN dba_name IS NOT NULL AND upper(dba_name) <> upper(legal_name)
             THEN 'DBA ' || dba_name END,
        COALESCE(mail_street, phy_street),
        nullif(trim(concat_ws(' ',
            nullif(COALESCE(mail_city, phy_city) || ',', ','),
            COALESCE(mail_state, phy_state),
            COALESCE(mail_zip, phy_zip))), ''),
        CASE WHEN COALESCE(mail_country, phy_country) NOT IN ('US', 'USA')
             THEN COALESCE(mail_country, phy_country) END
    ))                                                      AS mail_to_block,
    (COALESCE(mail_street, phy_street) IS NOT NULL
        AND COALESCE(mail_state, phy_state) IS NOT NULL
        AND COALESCE(mail_zip, phy_zip) IS NOT NULL)        AS mailable,
    -- contact anchors (company-level email; NOT asserted to belong to an officer)
    phone, fax, cell_phone, email_address, email_domain, proxy_domain,
    -- officer names — kept STRICTLY separate from email_address. No glued
    -- officer<->email anchor: the company mailbox (often generic info@/dispatch@)
    -- does not map 1:1 to a named officer, and asserting it would mis-target GTM.
    company_officer_1, company_officer_2,
    -- low-cardinality status flags (BTREE-indexed)
    status_code,
    CASE status_code WHEN 'A' THEN 'Active' WHEN 'I' THEN 'Inactive'
         ELSE status_code END                              AS status_label,
    carrier_operation,
    CASE carrier_operation WHEN 'A' THEN 'Interstate'
         WHEN 'B' THEN 'Intrastate Hazmat'
         WHEN 'C' THEN 'Intrastate Non-Hazmat'
         ELSE carrier_operation END                        AS carrier_operation_label,
    COALESCE(business_org_desc, business_org_id)           AS entity_type,
    business_org_id,
    power_units, truck_units, bus_units, fleetsize,
    undeliv_phy,
    -- provenance
    snapshot_date,
    '{FEED}'  AS source_feed,
    now()     AS derived_at
FROM base
"""


# --------------------------------------------------------------------------- #
# ops ledger + Trigger callback
# --------------------------------------------------------------------------- #
OPS_DDL = (
    "CREATE SCHEMA IF NOT EXISTS ops",
    """
    CREATE TABLE IF NOT EXISTS ops.fmcsa_derived_runs (
        id             bigserial PRIMARY KEY,
        feed           text        NOT NULL,
        rows_written   bigint      NOT NULL DEFAULT 0,
        mailable_rows  bigint,
        email_rows     bigint,
        officer_rows   bigint,
        status         text        NOT NULL,
        error          text,
        started_at     timestamptz NOT NULL,
        completed_at   timestamptz
    )
    """,
)


def _pg_connect():
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* write.")
        return None
    return psycopg.connect(dsn)


def _record_run(*, rows, mailable, email_rows, officer_rows, status, error,
                started_at, completed_at) -> None:
    conn = _pg_connect()
    if conn is None:
        return
    try:
        with conn, conn.cursor() as cur:
            for stmt in OPS_DDL:
                cur.execute(stmt)
            cur.execute(
                """
                INSERT INTO ops.fmcsa_derived_runs
                    (feed, rows_written, mailable_rows, email_rows, officer_rows,
                     status, error, started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (FEED, rows, mailable, email_rows, officer_rows, status, error,
                 started_at, completed_at),
            )
    except Exception as exc:  # noqa: BLE001 — audit must not mask the build
        print(f"WARN: ops.* write failed: {exc}")
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
        except Exception as exc:  # noqa: BLE001
            print(f"Callback attempt {i + 1} failed: {exc}")
        time.sleep(2 * (i + 1))
    print(f"WARN: callback delivery failed after {attempts} attempts → {url}")


# --------------------------------------------------------------------------- #
# Core build
# --------------------------------------------------------------------------- #
def _materialize(con, cen):
    con.register("census_src", cen.scanner(columns=SRC_COLS).to_reader())
    return con.sql(build_mail_ready_sql()).to_arrow_table()


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 60,
    memory=16384,
    cpu=4.0,
)
def build_mail_ready(trigger_callback_url: str | None = None) -> dict:
    """Build the census mail-ready serving dataset and publish to R2 active.
    Idempotent full overwrite of the derived prefix (census SoR is untouched)."""
    import datetime as dt
    import shutil

    import duckdb
    import lance

    started_at = dt.datetime.now(dt.timezone.utc)
    so = _r2_storage_options()
    status, error, rows, mailable, email_rows, officer_rows = "error", None, 0, 0, 0, 0
    try:
        cen = lance.dataset(CENSUS_SRC_URI, storage_options=so)
        con = duckdb.connect(":memory:")
        try:
            con.execute("PRAGMA threads=4;")
            table = _materialize(con, cen)
            rows = table.num_rows
            # Aggregate proof metrics straight off the built Arrow table.
            con.register("out", table)
            mailable = con.sql("SELECT count(*) FILTER (WHERE mailable) FROM out").fetchone()[0]
            email_rows = con.sql("SELECT count(email_address) FROM out").fetchone()[0]
            officer_rows = con.sql("SELECT count(company_officer_1) FROM out").fetchone()[0]
        finally:
            con.close()

        shutil.rmtree(LOCAL_DATASET, ignore_errors=True)
        lance.write_dataset(table, LOCAL_DATASET, mode="overwrite",
                            data_storage_version="2.0",
                            max_rows_per_file=MAX_ROWS_PER_FILE,
                            max_bytes_per_file=MAX_BYTES_PER_FILE)
        ds = lance.dataset(LOCAL_DATASET)
        for col in INDEX_COLS:
            try:
                ds.create_scalar_index(col, index_type="BTREE")
                print(f"Created BTREE index on {col}")
            except Exception as exc:  # noqa: BLE001
                print(f"WARN: BTREE index on {col} failed (non-fatal): {exc}")

        s3 = _s3_client()
        cleared = _clear_r2_prefix(s3, ACTIVE_PREFIX)
        uploaded = _upload_dir_to_r2(s3, LOCAL_DATASET, ACTIVE_PREFIX)
        print(f"Published {FEED}: cleared {cleared}, uploaded {uploaded} files → {DATASET_URI}")
        shutil.rmtree(LOCAL_DATASET, ignore_errors=True)
        status = "success"
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run(rows=int(rows), mailable=int(mailable), email_rows=int(email_rows),
                    officer_rows=int(officer_rows), status=status, error=error,
                    started_at=started_at, completed_at=completed_at)
        _post_callback(trigger_callback_url,
                       {"status": status, "rows": int(rows), "feed": FEED})

    if status != "success":
        raise RuntimeError(f"census_mail_ready build failed: {error}")
    return {"feed": FEED, "rows": int(rows), "mailable": int(mailable),
            "email_rows": int(email_rows), "officer_rows": int(officer_rows),
            "dataset": DATASET_URI}


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=60 * 60, memory=16384, cpu=4.0)
def dry_run_mail_ready() -> dict:
    """Materialize the projection from census and return coverage counts WITHOUT
    writing anything. Validates the SQL in-image before a full publish."""
    import duckdb
    import lance

    so = _r2_storage_options()
    cen = lance.dataset(CENSUS_SRC_URI, storage_options=so)
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=4;")
    table = _materialize(con, cen)
    con.register("out", table)

    def c(expr: str) -> int:
        return int(con.sql(f"SELECT {expr} FROM out").fetchone()[0])

    out = {
        "rows": table.num_rows,
        "mailable": c("count(*) FILTER (WHERE mailable)"),
        "email": c("count(email_address)"),
        "proxy_domain": c("count(proxy_domain)"),
        "officer": c("count(company_officer_1)"),
        "mc_number": c("count(mc_number)"),
        "schema": [f.name for f in table.schema],
    }
    con.close()
    return out


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=600)
def verify_mail_ready(limit: int = 5) -> dict:
    """Read-back proof: open the published dataset from R2, report rows / schema /
    indices, and return ``limit`` fully-parsed best-quality records — with the
    company email_address and officer names projected as DISTINCT columns."""
    import duckdb
    import lance

    so = _r2_storage_options()
    ds = lance.dataset(DATASET_URI, storage_options=so)
    n = ds.count_rows()
    idx = []
    for i in ds.list_indices():
        name = i["name"] if isinstance(i, dict) else getattr(i, "name", str(i))
        cols = i.get("fields") if isinstance(i, dict) else getattr(i, "fields", None)
        idx.append((name, cols))

    con = duckdb.connect()
    con.register("m", ds.scanner().to_reader())
    rel = con.sql(f"""
        SELECT carrier_dot, mc_number, legal_name, dba_name, mail_to_block,
               email_address, email_domain, proxy_domain,
               company_officer_1, company_officer_2,
               status_label, carrier_operation_label, entity_type, power_units
        FROM m
        WHERE legal_name IS NOT NULL AND mail_to_block IS NOT NULL
          AND email_address IS NOT NULL AND company_officer_1 IS NOT NULL
        LIMIT {int(limit)}
    """)
    cols = rel.columns
    records = [dict(zip(cols, r)) for r in rel.fetchall()]
    return {"rows": n, "indices": idx,
            "schema": [f"{f.name}:{f.type}" for f in ds.schema],
            "sample": records}


# --------------------------------------------------------------------------- #
# Entrypoint
# --------------------------------------------------------------------------- #
@app.local_entrypoint()
def main(dry_run: bool = False, sample: bool = False) -> None:
    if dry_run:
        d = dry_run_mail_ready.remote()
        print(f"[dry-run] rows={d['rows']:,} mailable={d['mailable']:,} "
              f"email_address={d['email']:,} proxy_domain={d['proxy_domain']:,} "
              f"company_officer_1={d['officer']:,} mc_number={d['mc_number']:,}")
        print(f"[dry-run] out_cols={len(d['schema'])}: {d['schema']}")
        return

    if sample:
        v = verify_mail_ready.remote(5)
        print(f"rows={v['rows']:,}  indices={v['indices']}")
        for i, rec in enumerate(v["sample"], 1):
            print(f"\n───────── record {i} ─────────")
            for k, val in rec.items():
                print(f"  {k:24s} {val}")
        return

    print(build_mail_ready.remote(trigger_callback_url=None))
    v = verify_mail_ready.remote(5)
    print(f"\n=== read-back: rows={v['rows']:,} indices={v['indices']} ===")
    for i, rec in enumerate(v["sample"], 1):
        print(f"\n───────── record {i} ─────────")
        for k, val in rec.items():
            print(f"  {k:24s} {val}")
