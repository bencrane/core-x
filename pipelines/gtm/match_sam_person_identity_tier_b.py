"""match_sam_person_identity_tier_b — Tier-B identity match (name_key × company-LinkedIn).

Runs AFTER Tier A (match_sam_person_identity.py), residue-scoped: only
domain-bearing people with no gtm_sam_person_identity row. Blocking key is the
COMPANY LinkedIn slug instead of the domain — reaches source rows whose domain
is missing/divergent (subsidiary domains, changed domains) but whose company
LinkedIn agrees.

Entity-side company-LinkedIn (residue entities only — never full-spine here):
    dsbs   crosswalk_dsbs_sam.company_linkedin_url    uei-direct (examined)
    pdl    pdl_normalized_companies.linkedin_slug     via normalized_domain,
                                                      is_generic_domain excluded
    clayc  clay_find_companies.linkedin_slug          via domain_norm
  Per-domain adjudication: pdl vs clayc conflict → domain excluded; dsbs
  (uei-scoped) takes precedence over domain votes.

Source-side company-LinkedIn:
    blitz_find_people.company_linkedin_url            native
    clay_find_people → company_record_id →
        clay_find_companies.linkedin_slug             FK hop

Person adjudication identical to Tier A (corroborated / single-source /
conflict-excluded / ambiguous-abstains); scores 0.95 / 0.85 (one inference hop
below Tier A's 1.0 / 0.9 — the entity company-LI is itself derived).
append_matches PK-dedup guarantees Tier-A rows are never overwritten.

Run:
    doppler run -- python3 pipelines/gtm/match_sam_person_identity_tier_b.py           # dry-run
    doppler run -- python3 pipelines/gtm/match_sam_person_identity_tier_b.py --apply   # append
"""

from __future__ import annotations

import datetime as dt
import os
import sys
import uuid

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from pipelines.gtm.gtm_sam_people import name_key_sql
from pipelines.gtm import gtm_sam_person_identity as identity
from pipelines.gtm.match_sam_person_identity import _r2_storage_options

SRC = {
    "gtm_sam_entities": "s3://data-sink/active/gtm_sam_entities/",
    "gtm_sam_people": "s3://data-sink/active/gtm_sam_people/",
    "gtm_sam_person_identity": "s3://data-sink/active/gtm_sam_person_identity/",
    "clay_find_people": "s3://data-sink/active/clay_find_people/",
    "clay_find_companies": "s3://data-sink/active/clay_find_companies/",
    "blitz_find_people": "s3://data-sink/active/blitz_find_people/",
    "pdl_normalized_companies": "s3://data-sink/active/pdl_normalized_companies/",
    "crosswalk_dsbs_sam": "s3://data-sink/active/crosswalk_dsbs_sam/",
}

DUCKDB_MEMORY_LIMIT = os.environ.get("GTM_DUCKDB_MEM", "16GB")
SPILL_DIR = os.environ.get("GTM_DUCKDB_SPILL", "/tmp/duckdb_spill")

PERSON_SLUG_RE = r"linkedin\.com/in/([^/?#]+)"
COMPANY_SLUG_RE = r"linkedin\.com/company/([^/?#]+)"


def _pslug(col: str) -> str:
    return (f"CASE WHEN regexp_extract(lower(coalesce({col},'')), '{PERSON_SLUG_RE}', 1) <> '' "
            f"THEN 'linkedin.com/in/' || regexp_extract(lower({col}), '{PERSON_SLUG_RE}', 1) END")


def _cslug_url(col: str) -> str:
    return f"NULLIF(regexp_extract(lower(coalesce({col},'')), '{COMPANY_SLUG_RE}', 1), '')"


def run(apply: bool = False) -> dict:
    import duckdb
    import lance

    so = _r2_storage_options()
    started = dt.datetime.now(dt.timezone.utc)
    build_id = f"match-tierb-{started:%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}"
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

    # ── residue: domain-bearing people with no identity row ──
    ent = opends("gtm_sam_entities")
    con.register("ent", ent.scanner(columns=["uei", "normalized_domain", "in_dsbs"],
                                    filter="normalized_domain IS NOT NULL").to_reader())
    con.execute("CREATE TEMP TABLE t_ent AS SELECT * FROM ent")
    ppl = opends("gtm_sam_people")
    con.register("ppl", ppl.scanner(
        columns=["sam_person_id", "uei", "name_key"]).to_reader())
    idn = opends("gtm_sam_person_identity")
    con.register("idn", idn.scanner(columns=["sam_person_id"]).to_reader())
    con.execute("""CREATE TEMP TABLE t_res AS
        SELECT p.sam_person_id, p.uei, p.name_key, e.normalized_domain, e.in_dsbs
        FROM ppl p
        JOIN t_ent e USING (uei)
        WHERE p.sam_person_id NOT IN (SELECT sam_person_id FROM idn)""")
    con.execute("""CREATE TEMP TABLE t_res_dom AS
        SELECT DISTINCT normalized_domain FROM t_res""")

    # ── entity-side company-LI votes (residue domains only) ──
    pdl = opends("pdl_normalized_companies")
    con.register("pdl", pdl.scanner(
        columns=["normalized_domain", "linkedin_slug"],
        filter="normalized_domain IS NOT NULL AND linkedin_slug IS NOT NULL "
               "AND NOT is_generic_domain").to_reader())
    con.execute("""CREATE TEMP TABLE v_pdl AS
        SELECT p.normalized_domain,
               count(DISTINCT lower(p.linkedin_slug)) AS n,
               min(lower(p.linkedin_slug)) AS slug
        FROM pdl p JOIN t_res_dom d USING (normalized_domain)
        GROUP BY 1""")
    con.unregister("pdl")

    clayc = opends("clay_find_companies")
    con.register("clayc", clayc.scanner(
        columns=["record_id", "domain_norm", "linkedin_slug"],
        filter="domain_norm IS NOT NULL AND linkedin_slug IS NOT NULL").to_reader())
    con.execute("CREATE TEMP TABLE t_clayc AS SELECT * FROM clayc")
    con.execute("""CREATE TEMP TABLE v_clayc AS
        SELECT c.domain_norm AS normalized_domain,
               count(DISTINCT lower(c.linkedin_slug)) AS n,
               min(lower(c.linkedin_slug)) AS slug
        FROM t_clayc c JOIN t_res_dom d ON d.normalized_domain = c.domain_norm
        GROUP BY 1""")

    cw = opends("crosswalk_dsbs_sam")
    con.register("cw", cw.scanner(
        columns=["uei", "company_linkedin_url"],
        filter="company_linkedin_url IS NOT NULL").to_reader())
    con.execute(f"""CREATE TEMP TABLE v_dsbs AS
        SELECT uei, min({_cslug_url('company_linkedin_url')}) AS slug
        FROM cw WHERE {_cslug_url('company_linkedin_url')} IS NOT NULL
        GROUP BY 1 HAVING count(DISTINCT {_cslug_url('company_linkedin_url')}) = 1""")

    # domain verdict: pdl×clayc agreement rules; dsbs overrides per-uei
    con.execute("""CREATE TEMP TABLE dom_li AS
        SELECT d.normalized_domain,
               CASE
                   WHEN p.slug IS NOT NULL AND c.slug IS NOT NULL
                        AND p.n = 1 AND c.n = 1 AND p.slug = c.slug THEN p.slug
                   WHEN p.slug IS NOT NULL AND p.n = 1 AND c.slug IS NULL THEN p.slug
                   WHEN c.slug IS NOT NULL AND c.n = 1 AND p.slug IS NULL THEN c.slug
               END AS slug
        FROM t_res_dom d
        LEFT JOIN v_pdl p USING (normalized_domain)
        LEFT JOIN v_clayc c USING (normalized_domain)""")
    con.execute("""CREATE TEMP TABLE ent_li AS
        SELECT r.uei, coalesce(v.slug, dl.slug) AS company_slug
        FROM (SELECT DISTINCT uei, normalized_domain FROM t_res) r
        LEFT JOIN v_dsbs v USING (uei)
        LEFT JOIN dom_li dl USING (normalized_domain)
        WHERE coalesce(v.slug, dl.slug) IS NOT NULL""")

    # ── source-side rows keyed by (name_key, company slug) ──
    blitz = opends("blitz_find_people")
    con.register("blitz", blitz.scanner(
        columns=["record_id", "full_name", "company_linkedin_url", "person_linkedin_norm"],
        filter="company_linkedin_url IS NOT NULL AND person_linkedin_norm IS NOT NULL"
    ).to_reader())
    con.execute(f"""CREATE TEMP TABLE s_blitz AS
        SELECT record_id, {_cslug_url('company_linkedin_url')} AS company_slug,
               {name_key_sql('full_name')} AS name_key,
               {_pslug('person_linkedin_norm')} AS slug
        FROM blitz
        WHERE {_cslug_url('company_linkedin_url')} IS NOT NULL
          AND {name_key_sql('full_name')} IS NOT NULL
          AND {_pslug('person_linkedin_norm')} IS NOT NULL""")

    clay = opends("clay_find_people")
    con.register("clay", clay.scanner(
        columns=["record_id", "full_name", "company_record_id", "linkedin_url_norm"],
        filter="company_record_id IS NOT NULL AND linkedin_url_norm IS NOT NULL"
    ).to_reader())
    con.execute(f"""CREATE TEMP TABLE s_clay AS
        SELECT c.record_id, lower(cc.linkedin_slug) AS company_slug,
               {name_key_sql('c.full_name')} AS name_key,
               {_pslug('c.linkedin_url_norm')} AS slug
        FROM clay c
        JOIN t_clayc cc ON cc.record_id = c.company_record_id
        WHERE cc.linkedin_slug IS NOT NULL
          AND {name_key_sql('c.full_name')} IS NOT NULL
          AND {_pslug('c.linkedin_url_norm')} IS NOT NULL""")

    for src in ("clay", "blitz"):
        con.execute(f"""CREATE TEMP TABLE cand_{src} AS
            SELECT r.sam_person_id,
                   count(DISTINCT s.slug) AS n_slugs,
                   min(s.slug)            AS slug,
                   min(s.record_id)       AS record_id
            FROM t_res r
            JOIN ent_li e USING (uei)
            JOIN s_{src} s ON s.company_slug = e.company_slug
                          AND s.name_key = r.name_key
            GROUP BY 1""")

    con.execute("""CREATE TEMP TABLE adjudicated AS
        WITH merged AS (
            SELECT coalesce(c.sam_person_id, b.sam_person_id) AS sam_person_id,
                   CASE WHEN c.n_slugs = 1 THEN c.slug END AS clay_slug,
                   CASE WHEN b.n_slugs = 1 THEN b.slug END AS blitz_slug,
                   c.record_id AS clay_rid, b.record_id AS blitz_rid
            FROM cand_clay c FULL OUTER JOIN cand_blitz b USING (sam_person_id))
        SELECT sam_person_id,
            CASE
                WHEN clay_slug IS NOT NULL AND blitz_slug IS NOT NULL
                     AND clay_slug = blitz_slug THEN 'corroborated'
                WHEN clay_slug IS NOT NULL AND blitz_slug IS NOT NULL THEN 'conflict'
                WHEN clay_slug IS NOT NULL THEN 'clay_only'
                WHEN blitz_slug IS NOT NULL THEN 'blitz_only'
                ELSE 'ambiguous'
            END AS verdict,
            coalesce(clay_slug, blitz_slug) AS slug, clay_rid, blitz_rid
        FROM merged""")

    n_res, n_res_ent, n_ent_li = con.execute("""
        SELECT (SELECT count(*) FROM t_res),
               (SELECT count(DISTINCT uei) FROM t_res),
               (SELECT count(*) FROM ent_li)""").fetchone()
    stats = dict(con.execute(
        "SELECT verdict, count(*) FROM adjudicated GROUP BY 1").fetchall())
    accepted = con.execute("""
        SELECT count(*) FROM adjudicated
        WHERE verdict IN ('corroborated','clay_only','blitz_only')""").fetchone()[0]
    print(f"build_id={build_id}")
    for e in lineage:
        print(f"  input {e['name']} v{e['version']} rows={e['rows_at_read']:,}")
    print(f"residue people: {n_res:,} across {n_res_ent:,} entities; "
          f"entities with company-LI resolved: {n_ent_li:,}")
    print(f"adjudication: {stats}")
    print(f"acceptable Tier-B matches: {accepted:,}")
    for r in con.execute("""
        SELECT a.verdict, r.name_key, r.uei, a.slug FROM adjudicated a
        JOIN t_res r USING (sam_person_id) LIMIT 6""").fetchall():
        print("  ", r)

    if not apply:
        print("\nDRY-RUN — no append. Re-run with --apply to write.")
        return {"build_id": build_id, "stats": stats, "accepted": accepted}

    by = {e["name"]: e for e in lineage}
    con.execute(f"""CREATE TEMP TABLE final AS
        SELECT a.sam_person_id, r.uei, r.name_key,
               a.slug AS person_linkedin_url_norm,
               CASE a.verdict
                   WHEN 'corroborated' THEN 'clay:' || a.clay_rid || ';blitz:' || a.blitz_rid
                   WHEN 'clay_only'    THEN 'clay:' || a.clay_rid
                   ELSE 'blitz:' || a.blitz_rid END AS match_source,
               CASE a.verdict
                   WHEN 'corroborated' THEN 'clay+blitz_name_companyli_exact'
                   WHEN 'clay_only'    THEN 'clay_name_companyli_exact'
                   ELSE 'blitz_name_companyli_exact' END AS match_method,
               CASE a.verdict WHEN 'corroborated' THEN 0.95 ELSE 0.85 END AS match_score,
               CASE a.verdict WHEN 'blitz_only'
                    THEN '{SRC["blitz_find_people"]}'
                    ELSE '{SRC["clay_find_people"]}' END AS matched_against_uri,
               CASE a.verdict WHEN 'blitz_only'
                    THEN {by["blitz_find_people"]["version"]}::BIGINT
                    ELSE {by["clay_find_people"]["version"]}::BIGINT
                    END AS matched_against_version,
               TIMESTAMP '{started:%Y-%m-%d %H:%M:%S}' AS matched_at,
               '{build_id}' AS build_id
        FROM adjudicated a JOIN t_res r USING (sam_person_id)
        WHERE a.verdict IN ('corroborated','clay_only','blitz_only')""")
    table = con.sql("SELECT * FROM final").to_arrow_table().cast(identity.schema())
    con.close()
    result = identity.append_matches(table, match_lineage=lineage)
    print(f"applied: {result}")
    return {"build_id": build_id, "stats": stats, "accepted": accepted, **result}


if __name__ == "__main__":
    print(run(apply="--apply" in sys.argv))
