"""DSBS Icypeas company-LinkedIn profile scrapes -> s3://data-sink/active/icypeas_dsbs_company_profiles/.

Custody for the 2026-07-05 UI bulk batch (Company Profile Scraper) run against the DSBS
coverage-gap cohort: DSBS entities with a best_domain but no firmographics_blitz coverage,
whose company-LinkedIn URL came from the Icypeas Company URL Finder (source
'icypeas_finder') or from pdl_normalized_companies on best_domain (source 'pdl').

Grain: 1 row per (uei, li_slug). A slug shared by sister UEIs fans out — the page is
scraped once, the ruling applies to each UEI. Slugs already inside the in-flight
icypeas_company_scrape_worklist sweep scope were excluded from this batch and will land
in gtm.icypeas_company_scrapes via that rail instead.

Both inputs are transport-only local CSVs (doctrine: raw is ephemeral; this Lance is the
custody). Lineage (file sha256, rowcounts at read) is recorded in-row and printed.

Run:
    LANCE_BYPASS_SPILLING=true doppler run -- python \
        pipelines/enrichment_icypeas/ingest_dsbs_company_li_profiles.py
"""
from __future__ import annotations

import hashlib
import os
from datetime import datetime, timezone

import duckdb
import lance

URI = "s3://data-sink/active/icypeas_dsbs_company_profiles/"
DATA_STORAGE_VERSION = "2.1"
BATCH_LABEL = "dsbs-ui-bulk-2026-07-05"

RESULTS_CSV = os.environ.get(
    "ICYPEAS_RESULTS_CSV",
    "/Users/benjamincrane/Downloads/dsbs_company_profile_scraper_2026-07-05_processed_by_icypeas.csv")
COMBINED_CSV = os.environ.get(
    "DSBS_COMBINED_CSV",
    "/Users/benjamincrane/Desktop/dsbs_company_li_scrape_combined_2026-07-05.csv")

# Icypeas echoes the RESOLVED page URL: a /company/ input can come back as /school/,
# and special characters come back percent-encoded (™ -> %e2%84%a2) — decode before keying.
SLUG = "lower(coalesce(try(url_decode(regexp_extract({c}, 'linkedin\\.com/(?:company|school)/([^/?#]+)', 1))), regexp_extract({c}, 'linkedin\\.com/(?:company|school)/([^/?#]+)', 1)))"
PATH_TYPE = "regexp_extract({c}, 'linkedin\\.com/(company|school)/', 1)"


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


def _sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def main() -> None:
    now = datetime.now(timezone.utc).isoformat()
    res_sha, comb_sha = _sha256(RESULTS_CSV), _sha256(COMBINED_CSV)
    con = duckdb.connect()

    con.execute(f"""
        CREATE TABLE res AS
        SELECT {SLUG.format(c='"LINKEDIN COMPANY"')} AS li_slug,
               {PATH_TYPE.format(c='"LINKEDIN COMPANY"')}     AS resolved_path_type,
               nullif(trim("Company website"), '')            AS li_website,
               nullif(trim("Company description"), '')        AS li_description,
               try_cast(nullif(trim("Company headcount"), '') AS INTEGER) AS li_headcount,
               nullif(trim("Company industries"), '')         AS li_industries,
               nullif(trim("Company registered address"), '') AS li_registered_address,
               ("Deleted from LinkedIn" = 'Y')                AS deleted_from_linkedin,
               "Status"                                       AS scrape_status
        FROM read_csv('{RESULTS_CSV}', header=true, all_varchar=true)
    """)
    n_res, n_slugs = con.execute("SELECT count(*), count(DISTINCT li_slug) FROM res").fetchone()
    if n_res != n_slugs:
        raise RuntimeError(f"results not 1/slug: rows={n_res} slugs={n_slugs}")

    con.execute(f"""
        CREATE TABLE master AS
        SELECT uei, legal_business_name, best_domain,
               lower(coalesce(try(url_decode(li_slug)), li_slug)) AS li_slug,
               'https://www.linkedin.com/company/' || li_slug AS company_linkedin_url,
               source AS li_source, in_active_scrape_worklist
        FROM read_csv('{COMBINED_CSV}', header=true, all_varchar=true)
    """)
    n_master = con.execute("SELECT count(*) FROM master").fetchone()[0]

    con.execute(f"""
        CREATE TABLE out AS
        SELECT m.uei, m.li_slug, m.company_linkedin_url, m.li_source,
               m.legal_business_name, m.best_domain,
               r.scrape_status, r.resolved_path_type, r.deleted_from_linkedin,
               r.li_website, r.li_description, r.li_headcount,
               r.li_industries, r.li_registered_address,
               '{BATCH_LABEL}' AS batch_label,
               '{os.path.basename(RESULTS_CSV)}' AS source_results_file,
               '{res_sha}' AS source_results_sha256,
               '{comb_sha}' AS source_combined_sha256,
               '{now}' AS materialized_at
        FROM master m JOIN res r USING (li_slug)
        ORDER BY m.uei, m.li_slug
    """)

    n_out, n_uei, n_null = con.execute("""
        SELECT count(*), count(DISTINCT uei),
               count(*) FILTER (WHERE uei IS NULL OR li_slug IS NULL OR li_slug = '')
        FROM out""").fetchone()
    n_expected = con.execute(
        "SELECT count(*) FROM master WHERE NOT cast(in_active_scrape_worklist AS BOOLEAN)").fetchone()[0]
    found, with_desc, with_hc = con.execute("""
        SELECT count(*) FILTER (WHERE scrape_status = 'Found'),
               count(*) FILTER (WHERE li_description IS NOT NULL),
               count(*) FILTER (WHERE li_headcount IS NOT NULL) FROM out""").fetchone()

    print(f"inputs: results {n_res:,} rows sha={res_sha[:12]} | combined {n_master:,} rows sha={comb_sha[:12]}")
    print(f"out: rows={n_out:,} (expected {n_expected:,}) ueis={n_uei:,} "
          f"found={found:,} desc={with_desc:,} headcount={with_hc:,}")
    if n_null:
        raise RuntimeError(f"{n_null} rows with NULL uei/slug")
    if n_out != n_expected:
        raise RuntimeError(f"fan-out mismatch: out={n_out} expected={n_expected}")

    table = con.execute("SELECT * FROM out").to_arrow_table()
    so = _r2_storage_options()
    try:
        v_before = lance.dataset(URI, storage_options=so).version
    except Exception:  # noqa: BLE001 — first write
        v_before = None
    lance.write_dataset(table, URI, mode="overwrite",
                        data_storage_version=DATA_STORAGE_VERSION, storage_options=so)
    ds = lance.dataset(URI, storage_options=so)
    for col in ("uei", "li_slug"):
        ds.create_scalar_index(col, index_type="BTREE")
        print(f"  BTREE ✓ {col}")
    print(f"{URI} v{v_before} -> v{ds.version} rows={ds.count_rows():,}")


if __name__ == "__main__":
    main()
