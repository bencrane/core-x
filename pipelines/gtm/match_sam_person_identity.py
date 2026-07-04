"""match_sam_person_identity — Tier-A identity match (name_key × domain, exact).

Strategy: docs/plans/GTM_SAM_AUDIENCE_MART_STRATEGY.md §4.4/§7; build report
docs/reference/GTM_SAM_MART_BUILD_REPORT_2026-07-04.md §6.

Supervised in-session tool — NOT a scheduled pipeline. Default run is a
DRY-RUN report; `--apply` performs the validated append into
gtm_sam_person_identity (PK-deduped, first append builds indices).

Sources (co-equal, adjudicated before append — never sequential first-wins):
    clay_find_people    person linkedin via (name_key × domain_norm)
    blitz_find_people   person linkedin via (name_key × company_domain)

Adjudication per sam_person_id:
    corroborated  both sources unique and same slug          score 1.00
    single-source exactly one source resolves uniquely       score 0.90
    conflict      both unique, different slugs               EXCLUDED
    ambiguous     a source yields >1 distinct slug           that source
                                                             abstains
Slug canon: lower(regexp_extract(url, 'linkedin.com/in/<slug>')) — both
sources already normalize to this family; extraction is defensive.

name_key: identical SQL convention as the gtm_sam_people build (imported).

Run:
    doppler run -- python3 pipelines/gtm/match_sam_person_identity.py           # dry-run
    doppler run -- python3 pipelines/gtm/match_sam_person_identity.py --apply   # append
"""

from __future__ import annotations

import datetime as dt
import os
import sys
import uuid

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from pipelines.gtm.gtm_sam_people import name_key_sql  # single name_key convention
from pipelines.gtm import gtm_sam_person_identity as identity

SRC = {
    "gtm_sam_entities": "s3://data-sink/active/gtm_sam_entities/",
    "gtm_sam_people": "s3://data-sink/active/gtm_sam_people/",
    "clay_find_people": "s3://data-sink/active/clay_find_people/",
    "blitz_find_people": "s3://data-sink/active/blitz_find_people/",
}

DUCKDB_MEMORY_LIMIT = os.environ.get("GTM_DUCKDB_MEM", "16GB")
SPILL_DIR = os.environ.get("GTM_DUCKDB_SPILL", "/tmp/duckdb_spill")

SLUG_RE = r"linkedin\.com/in/([^/?#]+)"


def _slug_sql(col: str) -> str:
    return (f"CASE WHEN regexp_extract(lower(coalesce({col},'')), '{SLUG_RE}', 1) <> '' "
            f"THEN 'linkedin.com/in/' || regexp_extract(lower({col}), '{SLUG_RE}', 1) END")


def _r2_storage_options() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": endpoint,
        "region": "auto",
    }


def run(apply: bool = False) -> dict:
    import duckdb
    import lance

    so = _r2_storage_options()
    started = dt.datetime.now(dt.timezone.utc)
    build_id = f"match-{started:%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}"
    lineage: list[dict] = []

    def opends(name):
        ds = lance.dataset(SRC[name], storage_options=so)
        lineage.append({"name": name, "uri": SRC[name], "version": ds.version,
                        "rows_at_read": ds.count_rows()})
        return ds

    con = duckdb.connect(":memory:")
    con.execute(f"SET memory_limit='{DUCKDB_MEMORY_LIMIT}'")
    con.execute(f"SET temp_directory='{SPILL_DIR}'")
    os.makedirs(SPILL_DIR, exist_ok=True)

    ent = opends("gtm_sam_entities")
    con.register("ent", ent.scanner(columns=["uei", "normalized_domain"],
                                    filter="normalized_domain IS NOT NULL").to_reader())
    con.execute("CREATE TEMP TABLE t_ent AS SELECT * FROM ent")

    ppl = opends("gtm_sam_people")
    con.register("ppl", ppl.scanner(
        columns=["sam_person_id", "uei", "name_key"]).to_reader())
    con.execute("""CREATE TEMP TABLE t_ppl AS
        SELECT p.sam_person_id, p.uei, p.name_key, e.normalized_domain
        FROM ppl p JOIN t_ent e USING (uei)""")

    clay = opends("clay_find_people")
    con.register("clay", clay.scanner(
        columns=["record_id", "full_name", "domain_norm", "linkedin_url_norm"],
        filter="domain_norm IS NOT NULL AND linkedin_url_norm IS NOT NULL").to_reader())
    con.execute(f"""CREATE TEMP TABLE t_clay AS
        SELECT record_id, domain_norm AS domain,
               {name_key_sql('full_name')} AS name_key,
               {_slug_sql('linkedin_url_norm')} AS slug
        FROM clay
        WHERE {name_key_sql('full_name')} IS NOT NULL
          AND {_slug_sql('linkedin_url_norm')} IS NOT NULL""")

    blitz = opends("blitz_find_people")
    con.register("blitz", blitz.scanner(
        columns=["record_id", "full_name", "company_domain", "person_linkedin_norm"],
        filter="company_domain IS NOT NULL AND person_linkedin_norm IS NOT NULL"
    ).to_reader())
    con.execute(f"""CREATE TEMP TABLE t_blitz AS
        SELECT record_id, lower(company_domain) AS domain,
               {name_key_sql('full_name')} AS name_key,
               {_slug_sql('person_linkedin_norm')} AS slug
        FROM blitz
        WHERE {name_key_sql('full_name')} IS NOT NULL
          AND {_slug_sql('person_linkedin_norm')} IS NOT NULL""")

    for src in ("clay", "blitz"):
        con.execute(f"""CREATE TEMP TABLE cand_{src} AS
            SELECT p.sam_person_id,
                   count(DISTINCT s.slug)  AS n_slugs,
                   min(s.slug)             AS slug,
                   min(s.record_id)        AS record_id
            FROM t_ppl p
            JOIN t_{src} s ON s.domain = p.normalized_domain
                          AND s.name_key = p.name_key
            GROUP BY 1""")

    con.execute("""CREATE TEMP TABLE adjudicated AS
        WITH merged AS (
            SELECT coalesce(c.sam_person_id, b.sam_person_id) AS sam_person_id,
                   CASE WHEN c.n_slugs = 1 THEN c.slug END AS clay_slug,
                   CASE WHEN b.n_slugs = 1 THEN b.slug END AS blitz_slug,
                   c.n_slugs AS clay_n, b.n_slugs AS blitz_n,
                   c.record_id AS clay_rid, b.record_id AS blitz_rid
            FROM cand_clay c FULL OUTER JOIN cand_blitz b USING (sam_person_id))
        SELECT sam_person_id,
            CASE
                WHEN clay_slug IS NOT NULL AND blitz_slug IS NOT NULL
                     AND clay_slug = blitz_slug THEN 'corroborated'
                WHEN clay_slug IS NOT NULL AND blitz_slug IS NOT NULL
                     THEN 'conflict'
                WHEN clay_slug IS NOT NULL THEN 'clay_only'
                WHEN blitz_slug IS NOT NULL THEN 'blitz_only'
                ELSE 'ambiguous'
            END AS verdict,
            coalesce(clay_slug, blitz_slug) AS slug,
            clay_rid, blitz_rid
        FROM merged""")

    stats = dict(con.execute(
        "SELECT verdict, count(*) FROM adjudicated GROUP BY 1").fetchall())
    n_ppl_dom = con.execute("SELECT count(*) FROM t_ppl").fetchone()[0]
    print(f"build_id={build_id}")
    for e in lineage:
        print(f"  input {e['name']} v{e['version']} rows={e['rows_at_read']:,}")
    print(f"people with entity domain: {n_ppl_dom:,}")
    print(f"adjudication: {stats}")
    accepted = con.execute("""
        SELECT count(*) FROM adjudicated
        WHERE verdict IN ('corroborated','clay_only','blitz_only')""").fetchone()[0]
    print(f"acceptable matches: {accepted:,}")
    sample = con.execute("""
        SELECT a.verdict, p.name_key, p.uei, a.slug
        FROM adjudicated a JOIN t_ppl p USING (sam_person_id)
        WHERE a.verdict = 'corroborated' LIMIT 5""").fetchall()
    for r in sample:
        print("  ", r)

    if not apply:
        print("\nDRY-RUN — no append. Re-run with --apply to write.")
        return {"build_id": build_id, "stats": stats, "accepted": accepted}

    by = {e["name"]: e for e in lineage}
    con.execute(f"""CREATE TEMP TABLE final AS
        SELECT a.sam_person_id, p.uei, p.name_key,
               a.slug AS person_linkedin_url_norm,
               CASE a.verdict
                   WHEN 'corroborated' THEN 'clay:' || a.clay_rid || ';blitz:' || a.blitz_rid
                   WHEN 'clay_only'    THEN 'clay:' || a.clay_rid
                   ELSE 'blitz:' || a.blitz_rid END AS match_source,
               CASE a.verdict
                   WHEN 'corroborated' THEN 'clay+blitz_name_domain_exact'
                   WHEN 'clay_only'    THEN 'clay_name_domain_exact'
                   ELSE 'blitz_name_domain_exact' END AS match_method,
               CASE a.verdict WHEN 'corroborated' THEN 1.0 ELSE 0.9 END AS match_score,
               CASE a.verdict WHEN 'blitz_only'
                    THEN '{SRC["blitz_find_people"]}'
                    ELSE '{SRC["clay_find_people"]}' END AS matched_against_uri,
               CASE a.verdict WHEN 'blitz_only'
                    THEN {by["blitz_find_people"]["version"]}::BIGINT
                    ELSE {by["clay_find_people"]["version"]}::BIGINT
                    END AS matched_against_version,
               TIMESTAMP '{started:%Y-%m-%d %H:%M:%S}' AS matched_at,
               '{build_id}' AS build_id
        FROM adjudicated a JOIN t_ppl p USING (sam_person_id)
        WHERE a.verdict IN ('corroborated','clay_only','blitz_only')""")
    table = con.sql("SELECT * FROM final").to_arrow_table().cast(identity.schema())
    con.close()
    result = identity.append_matches(table, match_lineage=lineage)
    print(f"applied: {result}")
    return {"build_id": build_id, "stats": stats, "accepted": accepted, **result}


if __name__ == "__main__":
    print(run(apply="--apply" in sys.argv))
