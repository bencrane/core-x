#!/usr/bin/env python3
"""Seed builder — active/sfnet_main_contacts (Gen-3 Lance SoR).

Materializes the authoritative SFNet member-directory **main contact** (one per member
company) into native Gen-3 Lance, resolved/connected to the canonical identity graph.
This is the load-bearing "who is the primary person at this account" signal that
enrichment cohorts do NOT carry: active/people has no is_main_contact field, and the
work_emails_sfnet_dm lineage is a decision-maker enrichment pass, not the directory's
designated main contact. This dataset supplies that designation, verbatim from SFNet.

SOURCE (durable raw archive in R2 — transport-only, the SoR is the active/ Lance below):
    s3://data-sink/raw/sfnet_main_contacts/sfnet_directory_main_contacts_2026-06-26.csv
    A manual scrape of the SFNet member directory. 168 rows / 165 companies (3 companies
    list two main contacts). Columns: company_name, company_source_url, company_website,
    person_name, person_title, person_profile_url (the SFNet employee directory URL, which
    carries a stable per-person UUID), is_main_contact (all TRUE), + three LinkedIn columns
    (col 1 == col 2; col 3 always empty; "No LinkedIn profile found" is a literal sentinel).
    Pass --csv <localpath> to build from a local file instead of the R2 archive.

TARGET (Gen-3 system of record — native Lance v2.1, written straight to R2):
    s3://data-sink/active/sfnet_main_contacts/        ── standalone SoR, mode=overwrite (idempotent)

RESOLUTION (connect each directory main contact to the canonical grains):
    contact_id  ← active/people, cascade:
        1) normalized person LinkedIn URL  (scheme/www/trailing-slash/query stripped, lowered)
        2) fallback: normalized full_name + normalized_domain
        On a multi-hit (active/people carries duplicate person rows — a known, deferred
        cleanup), the best match is chosen deterministically: prefer a source_platform='sfnet'
        row, then lexically-min contact_id. matched_contact_count records the multiplicity so
        the duplication is never hidden; match_method records which rule fired.
    company_id  ← active/companies_canonical by normalized_domain (prefer the sfnet-tagged
        company row — the sfnet tag now comes from a company_source_platforms sidecar join on
        company_id, not a companies column).
    ~78% of contacts resolve; the residual ~37 are senior execs at large banks whose directory
    main contact is simply absent from active/people — i.e. net-new contacts to seed later.
    Unmatched rows are KEPT (match_method='unmatched'); the directory facts stand on their own.

SCHEMA (every id String; is_main_contact bool; matched_contact_count int32; ts tz-UTC):
    sfnet_person_id        VARCHAR  NOT NULL  (PK — UUID parsed from person_profile_url)
    sfnet_company_id       VARCHAR            (UUID parsed from company_source_url)
    company_name           VARCHAR
    company_source_url      VARCHAR           (SFNet member-directory company URL)
    company_website        VARCHAR            (raw, as given)
    normalized_domain      VARCHAR            (the anchor; null when no website)
    person_name            VARCHAR
    person_title           VARCHAR
    person_profile_url     VARCHAR            (SFNet employee-directory URL)
    is_main_contact        BOOL     NOT NULL  (all TRUE — directory designation)
    linkedin_url           VARCHAR            (normalized canonical, e.g. linkedin.com/in/xxx)
    linkedin_url_raw       VARCHAR            (as given; null if "No LinkedIn profile found"/empty)
    resolved_contact_id    VARCHAR            (→ active/people.contact_id; null if unmatched)
    resolved_company_id    VARCHAR            (→ active/companies_canonical.company_id; null if no domain match)
    matched_contact_count  INT32    NOT NULL  (# active/people rows the person hit; 0 if unmatched)
    match_method           VARCHAR  NOT NULL  (linkedin_url | name_domain | unmatched)
    source_platform        VARCHAR  NOT NULL  ('sfnet-directory')
    materialized_at        TIMESTAMP[us,UTC] NOT NULL

INDEXES (scalar BTREE only):
    normalized_domain, linkedin_url, resolved_contact_id, resolved_company_id,
    sfnet_person_id, sfnet_company_id

RUN (R2 creds + GTM URIs from the core-x/prd Doppler config):
    doppler run -p core-x -c prd -- uv run --no-project \
        --with pylance --with duckdb --with pyarrow --with boto3 \
        python3 pipelines/gtm/build_sfnet_main_contacts.py
    # local source override:  ... python3 ...build_sfnet_main_contacts.py --csv /path/to.csv
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import tempfile

import duckdb
import lance
import pyarrow as pa

ACTIVE = os.environ.get("GTM_ACTIVE_ROOT", "s3://data-sink/active")
URI = os.environ.get("SFNET_MC_URI", f"{ACTIVE}/sfnet_main_contacts/")
PEOPLE_URI = os.environ.get("GTM_PEOPLE_URI", f"{ACTIVE}/people/")
# REPOINT → companies_canonical (source_platform extracted to the company_source_platforms
# sidecar; the sfnet company-tag tie-break below is a sidecar join on company_id).
COMPANIES_URI = os.environ.get("GTM_COMPANIES_URI", f"{ACTIVE}/companies_canonical/")
COMPANY_SOURCE_PLATFORMS_URI = os.environ.get(
    "COMPANY_SOURCE_PLATFORMS_URI", f"{ACTIVE}/company_source_platforms/")

RAW_BUCKET = "data-sink"
RAW_KEY = "raw/sfnet_main_contacts/sfnet_directory_main_contacts_2026-06-26.csv"

SOURCE_PLATFORM = "sfnet-directory"
INDEXES = ["normalized_domain", "linkedin_url", "resolved_contact_id",
           "resolved_company_id", "sfnet_person_id", "sfnet_company_id"]

SCHEMA = pa.schema([
    pa.field("sfnet_person_id", pa.string(), nullable=False),
    pa.field("sfnet_company_id", pa.string(), nullable=True),
    pa.field("company_name", pa.string(), nullable=True),
    pa.field("company_source_url", pa.string(), nullable=True),
    pa.field("company_website", pa.string(), nullable=True),
    pa.field("normalized_domain", pa.string(), nullable=True),
    pa.field("person_name", pa.string(), nullable=True),
    pa.field("person_title", pa.string(), nullable=True),
    pa.field("person_profile_url", pa.string(), nullable=True),
    pa.field("is_main_contact", pa.bool_(), nullable=False),
    pa.field("linkedin_url", pa.string(), nullable=True),
    pa.field("linkedin_url_raw", pa.string(), nullable=True),
    pa.field("resolved_contact_id", pa.string(), nullable=True),
    pa.field("resolved_company_id", pa.string(), nullable=True),
    pa.field("matched_contact_count", pa.int32(), nullable=False),
    pa.field("match_method", pa.string(), nullable=False),
    pa.field("source_platform", pa.string(), nullable=False),
    pa.field("materialized_at", pa.timestamp("us", tz="UTC"), nullable=False),
])


def _storage_options() -> dict:
    ep = os.environ.get("R2_ENDPOINT")
    if not ep and os.environ.get("R2_ACCOUNT_ID"):
        ep = f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
    if not ep:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID (r2-credentials).")
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": ep,
        "region": "auto",
    }


def _fetch_csv(local: str | None) -> str:
    """Return a local CSV path: the --csv override, else the R2 raw archive downloaded to tmp."""
    if local:
        return local
    import boto3

    so = _storage_options()
    s3 = boto3.client("s3", endpoint_url=so["endpoint"],
                      aws_access_key_id=so["aws_access_key_id"],
                      aws_secret_access_key=so["aws_secret_access_key"],
                      region_name="auto")
    fd, path = tempfile.mkstemp(suffix=".csv", prefix="sfnet_mc_")
    os.close(fd)
    s3.download_file(RAW_BUCKET, RAW_KEY, path)
    print(f"fetched s3://{RAW_BUCKET}/{RAW_KEY} -> {path}", flush=True)
    return path


# ── normalization (DuckDB SQL fragments) ────────────────────────────────────
def _norm_li(col: str) -> str:
    return (
        f"CASE WHEN lower(trim({col})) IN ('', 'no linkedin profile found') THEN NULL ELSE "
        f"nullif(regexp_replace(regexp_replace(regexp_replace(regexp_replace("
        f"lower(trim({col})), '\\?.*$',''), '^https?://',''), '^www\\.',''), '/+$',''), '') END"
    )


def _norm_dom(col: str) -> str:
    return (
        f"nullif(regexp_replace(regexp_replace(regexp_replace(regexp_replace("
        f"lower(trim(CAST({col} AS VARCHAR))), '^https?://','','g'), '^www\\.','','g'),"
        f" '/.*$','','g'), '\\.+$','','g'), '')"
    )


def _norm_name(col: str) -> str:
    return f"nullif(regexp_replace(regexp_replace(lower(trim({col})), '[^a-z0-9 ]','','g'), ' +',' ','g'), '')"


def _rule(t: str) -> None:
    print(f"\n{'=' * 78}\n{t}\n{'=' * 78}", flush=True)


def build(csv_path: str) -> None:
    so = _storage_options()
    con = duckdb.connect()
    con.execute("SET memory_limit='6GB';")

    uuid_re = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
    con.execute(f"""
        CREATE TABLE mc AS
        SELECT
            regexp_extract(column5, '{uuid_re}') AS sfnet_person_id,
            regexp_extract(column1, '{uuid_re}') AS sfnet_company_id,
            nullif(trim(column0),'') AS company_name,
            nullif(trim(column1),'') AS company_source_url,
            nullif(trim(column2),'') AS company_website,
            {_norm_dom('column2')}   AS normalized_domain,
            nullif(trim(column3),'') AS person_name,
            nullif(trim(column4),'') AS person_title,
            nullif(trim(column5),'') AS person_profile_url,
            true                     AS is_main_contact,
            {_norm_li('column7')}    AS linkedin_url,
            CASE WHEN lower(trim(column7)) IN ('', 'no linkedin profile found') THEN NULL
                 ELSE trim(column7) END AS linkedin_url_raw,
            {_norm_name('column3')}  AS name_norm
        FROM read_csv('{csv_path}', header=false, skip=1, all_varchar=true)
    """)
    n_csv = con.execute("SELECT count(*) FROM mc").fetchone()[0]
    n_pk = con.execute("SELECT count(DISTINCT sfnet_person_id) FROM mc").fetchone()[0]
    if n_pk != n_csv:
        raise SystemExit(f"PK not unique: {n_pk} distinct sfnet_person_id for {n_csv} rows")
    if con.execute("SELECT count(*) FROM mc WHERE coalesce(sfnet_person_id,'')=''").fetchone()[0]:
        raise SystemExit("null/empty sfnet_person_id — PK contract violated (bad person_profile_url?)")

    con.register("p_r", lance.dataset(PEOPLE_URI, storage_options=so).scanner(
        columns=["person_id", "company_id", "normalized_domain", "full_name",
                 "person_linkedin_url", "source_platform"]).to_reader())
    con.execute(f"""
        CREATE TABLE ppl AS
        SELECT person_id AS contact_id, normalized_domain,
               {_norm_li('person_linkedin_url')} AS li_norm,
               {_norm_name('full_name')} AS name_norm,
               (source_platform='sfnet') AS is_sfnet
        FROM p_r
    """)
    con.register("c_r", lance.dataset(COMPANIES_URI, storage_options=so).scanner(
        columns=["company_id", "normalized_domain"]).to_reader())
    # source_platform now lives in the company_source_platforms sidecar → join on company_id to
    # derive the sfnet tie-break flag (is_sfnet) instead of reading a companies column.
    con.register("csp_r", lance.dataset(COMPANY_SOURCE_PLATFORMS_URI, storage_options=so).scanner(
        columns=["company_id", "source_platform"],
        filter="source_platform IN ('sfnet', 'sfnet-manual-resolve')").to_reader())
    con.execute("""
        CREATE TABLE comp AS
        SELECT c.company_id, c.normalized_domain,
               (s.company_id IS NOT NULL) AS is_sfnet
        FROM c_r c
        LEFT JOIN (SELECT DISTINCT company_id FROM csp_r) s ON s.company_id = c.company_id
        WHERE c.normalized_domain IS NOT NULL
    """)

    con.execute("""
        CREATE TABLE li_res AS
        SELECT sfnet_person_id, contact_id AS li_contact, cnt AS li_count FROM (
          SELECT mc.sfnet_person_id, p.contact_id,
                 row_number() OVER (PARTITION BY mc.sfnet_person_id ORDER BY p.is_sfnet DESC, p.contact_id) rn,
                 count(*)     OVER (PARTITION BY mc.sfnet_person_id) cnt
          FROM mc JOIN ppl p ON p.li_norm = mc.linkedin_url AND mc.linkedin_url IS NOT NULL
        ) WHERE rn=1
    """)
    con.execute("""
        CREATE TABLE nd_res AS
        SELECT sfnet_person_id, contact_id AS nd_contact, cnt AS nd_count FROM (
          SELECT mc.sfnet_person_id, p.contact_id,
                 row_number() OVER (PARTITION BY mc.sfnet_person_id ORDER BY p.is_sfnet DESC, p.contact_id) rn,
                 count(*)     OVER (PARTITION BY mc.sfnet_person_id) cnt
          FROM mc JOIN ppl p ON p.name_norm = mc.name_norm AND p.normalized_domain = mc.normalized_domain
                  AND mc.name_norm IS NOT NULL AND mc.normalized_domain IS NOT NULL
        ) WHERE rn=1
    """)
    con.execute("""
        CREATE TABLE co_res AS
        SELECT normalized_domain, company_id AS resolved_company_id FROM (
          SELECT normalized_domain, company_id,
                 row_number() OVER (PARTITION BY normalized_domain ORDER BY is_sfnet DESC, company_id) rn
          FROM comp
        ) WHERE rn=1
    """)

    final = con.execute(f"""
        SELECT
            mc.sfnet_person_id, mc.sfnet_company_id, mc.company_name, mc.company_source_url,
            mc.company_website, mc.normalized_domain, mc.person_name, mc.person_title,
            mc.person_profile_url, mc.is_main_contact, mc.linkedin_url, mc.linkedin_url_raw,
            coalesce(li.li_contact, nd.nd_contact)                 AS resolved_contact_id,
            co.resolved_company_id,
            CAST(coalesce(li.li_count, nd.nd_count, 0) AS INTEGER) AS matched_contact_count,
            CASE WHEN li.li_contact IS NOT NULL THEN 'linkedin_url'
                 WHEN nd.nd_contact IS NOT NULL THEN 'name_domain'
                 ELSE 'unmatched' END                              AS match_method,
            '{SOURCE_PLATFORM}'                                    AS source_platform
        FROM mc
        LEFT JOIN li_res li USING (sfnet_person_id)
        LEFT JOIN nd_res nd USING (sfnet_person_id)
        LEFT JOIN co_res co ON co.normalized_domain = mc.normalized_domain
    """).to_arrow_table()

    ts = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
    final = final.append_column(
        "materialized_at", pa.array([ts] * final.num_rows, pa.timestamp("us", tz="UTC")))
    final = final.select([f.name for f in SCHEMA]).cast(SCHEMA)

    _rule("resolution summary")
    for m, c in con.execute("""
        SELECT match_method, count(*) FROM (
          SELECT CASE WHEN li.li_contact IS NOT NULL THEN 'linkedin_url'
                      WHEN nd.nd_contact IS NOT NULL THEN 'name_domain'
                      ELSE 'unmatched' END AS match_method
          FROM mc LEFT JOIN li_res li USING (sfnet_person_id)
                  LEFT JOIN nd_res nd USING (sfnet_person_id)
        ) GROUP BY 1 ORDER BY 2 DESC
    """).fetchall():
        print(f"    {m:<14} {c:>4}", flush=True)
    co_n = con.execute("""SELECT count(*) FROM mc LEFT JOIN co_res co
        ON co.normalized_domain=mc.normalized_domain WHERE co.resolved_company_id IS NOT NULL""").fetchone()[0]
    print(f"    company_id resolved : {co_n}/{n_csv}\n    rows to write       : {final.num_rows}", flush=True)

    _rule(f"write -> {URI}")
    try:
        existing = lance.dataset(URI, storage_options=so)
        print(f"    NOTE: dataset exists ({existing.count_rows():,} rows) — overwriting (idempotent).", flush=True)
    except Exception:
        print("    net-new dataset.", flush=True)
    lance.write_dataset(final, URI, mode="overwrite", data_storage_version="2.1",
                        max_rows_per_file=1048576, storage_options=so)
    ds = lance.dataset(URI, storage_options=so)
    for col in INDEXES:
        try:
            ds.create_scalar_index(col, index_type="BTREE")
            print(f"    BTREE ✓ {col}", flush=True)
        except Exception as e:  # noqa: BLE001 — an index miss must not fail a good load
            print(f"    BTREE ✗ {col}: {e}", flush=True)

    _rule("verify — read-back")
    ds = lance.dataset(URI, storage_options=so)
    idx = [i.get('name') if isinstance(i, dict) else getattr(i, 'name', str(i)) for i in ds.list_indices()]
    print(f"    rows={ds.count_rows():,}  fields={len(ds.schema)}  indices={idx}", flush=True)


def main() -> int:
    ap = argparse.ArgumentParser(description="Build active/sfnet_main_contacts from the SFNet directory CSV.")
    ap.add_argument("--csv", default=None, help="Local CSV path (default: download the R2 raw archive).")
    args = ap.parse_args()
    build(_fetch_csv(args.csv))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
