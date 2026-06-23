"""One-shot: build the single never-attempted DSBS firmo cohort (Workflow B input).

DSBS certified firms → candidate domains (website ∪ additional_website ∪ email ∪ SAM,
non-generic) → PDL company → linkedin_url, KEPT only when the firm's domain has never
been through Blitz firmo (absent from firmographics_blitz AND from any firmo ops.task_runs
attempt). Writes ONE Parquet (no chunking) — the Modal worker (2h timeout, ≤5 RPS) eats
the whole ~4k in one shot.

    doppler run --project core-x --config prd -- python3 scripts/build_dsbs_neverattempted_cohort.py
"""

from __future__ import annotations

import os

import boto3
import duckdb
import lance
import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.config import Config

A = "s3://data-sink/active"
SINK_BUCKET = "data-sink"
COHORT_KEY = "cohorts/enrichment_blitz/sba_dsbs_neverattempted_linkedin.parquet"

FIRMO_TASK_TYPES = ("blitz_firmo_direct", "modal_hydrate_firmo_cascade")

_GENERIC = (
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "aol.com", "icloud.com", "msn.com",
    "live.com", "protonmail.com", "proton.me", "ymail.com", "gmx.com", "mail.com", "me.com",
    "facebook.com", "web.facebook.com", "m.facebook.com", "fb.com", "instagram.com", "twitter.com",
    "x.com", "youtube.com", "youtu.be", "linkedin.com", "linktr.ee", "medium.com", "wordpress.com",
    "blogspot.com", "sites.google.com", "g.page", "behance.net", "wa.me", "t.me", "calendly.com",
    "indiamart.com", "yelp.com", "etsy.com", "amazon.com", "ebay.com", "tiktok.com", "pinterest.com",
    "github.io", "wixsite.com", "weebly.com", "godaddysites.com",
)


def _so() -> dict:
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": os.environ["R2_ENDPOINT"],
        "region": "auto",
    }


def _norm(col: str) -> str:
    return (rf"regexp_replace(regexp_replace(regexp_replace(lower(trim({col})),"
            rf"'^https?://',''),'^www\.',''),'/.*$','')")


def main() -> None:
    so = _so()
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=8")
    con.execute("SET memory_limit='12GB'")
    con.register("dsbs", lance.dataset(f"{A}/sba_dsbs_certified_firms/", storage_options=so))
    con.register("smd",  lance.dataset(f"{A}/sam_master_domains/", storage_options=so))
    con.register("pnc",  lance.dataset(f"{A}/pdl_normalized_companies/", storage_options=so))
    con.register("pco",  lance.dataset(f"{A}/pdl_companies/", storage_options=so))
    con.register("blz",  lance.dataset(f"{A}/firmographics_blitz/", storage_options=so))

    gen = "(" + ",".join("'" + g + "'" for g in _GENERIC) + ")"

    # firmo-attempted domains from the HQX event log (transaction pooler; never the :5432 session pool).
    dsn = os.environ["HQX_DB_URL_TRANSACTION"]
    tt = "(" + ",".join("'" + t + "'" for t in FIRMO_TASK_TYPES) + ")"
    with psycopg.connect(dsn, prepare_threshold=None) as pg, pg.cursor() as cur:
        cur.execute(f"SELECT DISTINCT lower(domain) FROM ops.task_runs "
                    f"WHERE task_type IN {tt} AND domain IS NOT NULL")
        firmo_domains = [r[0] for r in cur.fetchall() if r[0]]
    con.register("firmo_dom", pa.table({"d": pa.array(firmo_domains, type=pa.string())}))
    print(f"firmo-attempted domains (ops.task_runs): {len(firmo_domains):,}")

    con.execute("CREATE TEMP TABLE blz_dom AS "
                "SELECT DISTINCT lower(domain_norm) d FROM blz WHERE domain_norm IS NOT NULL")

    # DSBS candidate domains (4-source union, non-generic)
    con.execute(f"""
        CREATE TEMP TABLE dsbs_dom AS
        SELECT DISTINCT d FROM (
            SELECT {_norm('website')} d FROM dsbs WHERE website IS NOT NULL
            UNION SELECT {_norm('additional_website')} FROM dsbs WHERE additional_website IS NOT NULL
            UNION SELECT {_norm("regexp_extract(email, '@(.+)$', 1)")} FROM dsbs WHERE email IS NOT NULL
            UNION SELECT lower(normalized_domain) FROM smd
                   WHERE uei IN (SELECT uei FROM dsbs) AND normalized_domain IS NOT NULL
        ) WHERE d IS NOT NULL AND d <> '' AND d LIKE '%.%' AND d NOT IN {gen}
    """)

    # never-attempted = candidate domain absent from BOTH firmo output and the attempt log
    con.execute("""
        CREATE TEMP TABLE na_dom AS
        SELECT d FROM dsbs_dom
        WHERE d NOT IN (SELECT d FROM blz_dom)
          AND d NOT IN (SELECT d FROM firmo_dom)
    """)
    n_na = con.execute("SELECT count(*) FROM na_dom").fetchone()[0]
    print(f"never-attempted DSBS candidate domains: {n_na:,}")

    # domain → pdl_company_id (non-generic) → linkedin_url
    con.execute("""
        CREATE TEMP TABLE dom_pid AS
        SELECT DISTINCT p.pdl_company_id
        FROM pnc p
        WHERE lower(p.normalized_domain) IN (SELECT d FROM na_dom)
          AND coalesce(p.is_generic_domain, false) = false
          AND p.pdl_company_id IS NOT NULL
    """)
    con.execute("""
        CREATE TEMP TABLE urls AS
        SELECT DISTINCT trim(c.linkedin_url) AS company_linkedin_url
        FROM pco c
        WHERE c.pdl_company_id IN (SELECT pdl_company_id FROM dom_pid)
          AND c.linkedin_url IS NOT NULL AND trim(c.linkedin_url) <> ''
    """)
    urls = [r[0] for r in con.execute(
        "SELECT company_linkedin_url FROM urls ORDER BY company_linkedin_url").fetchall()]
    con.close()
    print(f"never-attempted distinct linkedin_urls (cohort size): {len(urls):,}")
    assert urls, "empty cohort — nothing to enrich"

    local = "/tmp/sba_dsbs_neverattempted_linkedin.parquet"
    pq.write_table(pa.table({"company_linkedin_url": pa.array(urls, type=pa.string())}), local)
    s3 = boto3.client(
        "s3", endpoint_url=so["endpoint"],
        aws_access_key_id=so["aws_access_key_id"], aws_secret_access_key=so["aws_secret_access_key"],
        region_name="auto", config=Config(retries={"max_attempts": 5, "mode": "standard"}))
    s3.upload_file(local, SINK_BUCKET, COHORT_KEY)
    print(f"COHORT → s3://{SINK_BUCKET}/{COHORT_KEY}  ({len(urls):,} urls)")


if __name__ == "__main__":
    main()
