"""harvest_owned_identity — Cycle 3: fold owned person-LinkedIn resolutions
into gtm_sam_person_identity.

Sources (uei-direct — no domain inference; strictly higher precision than
Tier A/B name×company matching):

    dsbs_poc_linkedin      serper-resolved, name-AND-company validated /in/
                           URLs for DSBS POCs (paid run; spend-ledgered;
                           docs/reference/DSBS_POC_LINKEDIN_RESOLUTION.md)
    sam_labor_poc_people   staffing-segment derivative carrying
                           person_linkedin_url + match_source provenance

Harvest discipline:
  * name_key is RECOMPUTED with the mart convention from each source's
    verbatim name — foreign name_key columns are never trusted.
  * sam_labor_poc_people rows are harvested ONLY where the matched person's
    name agrees with the POC's name (mart name_key equality) — rows where the
    enrichment matched a different human are excluded and counted.
  * Per (uei, name_key): >1 distinct person slug within a source → ambiguous,
    abstain.
  * Cross-source adjudication (same pattern as Tier A): agreement → 1.0;
    dsbs-only → 0.95 (externally validated); labor-only → 0.90 (derivative);
    cross-source conflict → excluded.
  * Rows whose sam_person_id already exists in identity are adjudicated
    read-only: agreement / conflict counted and reported, NEVER overwritten
    (a re-match is an explicit delete+append, per the identity loader).

Run:
    doppler run -- python3 pipelines/gtm/harvest_owned_identity.py           # dry-run
    doppler run -- python3 pipelines/gtm/harvest_owned_identity.py --apply   # append
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
    "gtm_sam_people": "s3://data-sink/active/gtm_sam_people/",
    "gtm_sam_person_identity": "s3://data-sink/active/gtm_sam_person_identity/",
    "dsbs_poc_linkedin": "s3://data-sink/active/dsbs_poc_linkedin/",
    "sam_labor_poc_people": "s3://data-sink/active/sam_labor_poc_people/",
}

DUCKDB_MEMORY_LIMIT = os.environ.get("GTM_DUCKDB_MEM", "16GB")
SPILL_DIR = os.environ.get("GTM_DUCKDB_SPILL", "/tmp/duckdb_spill")

PERSON_SLUG_RE = r"linkedin\.com/in/([^/?#]+)"


def _pslug(col: str) -> str:
    return (f"CASE WHEN regexp_extract(lower(coalesce({col},'')), '{PERSON_SLUG_RE}', 1) <> '' "
            f"THEN 'linkedin.com/in/' || regexp_extract(lower({col}), '{PERSON_SLUG_RE}', 1) END")


def run(apply: bool = False) -> dict:
    import duckdb
    import lance

    so = _r2_storage_options()
    started = dt.datetime.now(dt.timezone.utc)
    build_id = f"harvest-{started:%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}"
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

    ppl = opends("gtm_sam_people")
    con.register("ppl", ppl.scanner(
        columns=["sam_person_id", "uei", "name_key"]).to_reader())
    con.execute("CREATE TEMP TABLE t_ppl AS SELECT * FROM ppl")

    idn = opends("gtm_sam_person_identity")
    con.register("idn", idn.scanner(
        columns=["sam_person_id", "person_linkedin_url_norm"]).to_reader())
    con.execute("CREATE TEMP TABLE t_idn AS SELECT * FROM idn")

    # ── source: dsbs_poc_linkedin (serper-validated) ──
    dpl = opends("dsbs_poc_linkedin")
    con.register("dpl", dpl.scanner(
        columns=["subject_id", "uei", "full_name", "linkedin_url",
                 "name_consistent"],
        filter="linkedin_url IS NOT NULL").to_reader())
    con.execute(f"""CREATE TEMP TABLE s_dsbs AS
        SELECT uei, {name_key_sql('full_name')} AS name_key,
               count(DISTINCT {_pslug('linkedin_url')}) AS n_slugs,
               min({_pslug('linkedin_url')}) AS slug,
               min(subject_id) AS ref,
               bool_and(coalesce(name_consistent, TRUE)) AS name_ok
        FROM dpl
        WHERE {_pslug('linkedin_url')} IS NOT NULL
          AND {name_key_sql('full_name')} IS NOT NULL
        GROUP BY 1, 2""")

    # ── source: sam_labor_poc_people (derivative; strict name agreement) ──
    slp = opends("sam_labor_poc_people")
    con.register("slp", slp.scanner(
        columns=["uei", "poc_full_name", "person_full_name",
                 "person_linkedin_url", "match_source"],
        filter="person_linkedin_url IS NOT NULL").to_reader())
    con.execute(f"""CREATE TEMP TABLE slp_raw AS
        SELECT uei,
               {name_key_sql('poc_full_name')} AS name_key,
               {name_key_sql('person_full_name')} AS person_nk,
               {_pslug('person_linkedin_url')} AS slug,
               match_source
        FROM slp
        WHERE {_pslug('person_linkedin_url')} IS NOT NULL
          AND {name_key_sql('poc_full_name')} IS NOT NULL""")
    n_labor_raw, n_labor_nameok = con.execute("""
        SELECT count(*), count(*) FILTER (WHERE person_nk = name_key)
        FROM slp_raw""").fetchone()
    con.execute("""CREATE TEMP TABLE s_labor AS
        SELECT uei, name_key,
               count(DISTINCT slug) AS n_slugs,
               min(slug) AS slug,
               min(match_source) AS ref
        FROM slp_raw
        WHERE person_nk = name_key
        GROUP BY 1, 2""")

    # ── cross-source adjudication on the people spine ──
    con.execute("""CREATE TEMP TABLE adjudicated AS
        WITH merged AS (
            SELECT p.sam_person_id, p.uei, p.name_key,
                   CASE WHEN d.n_slugs = 1 AND d.name_ok THEN d.slug END AS dsbs_slug,
                   CASE WHEN l.n_slugs = 1 THEN l.slug END AS labor_slug,
                   d.ref AS dsbs_ref, l.ref AS labor_ref
            FROM t_ppl p
            LEFT JOIN s_dsbs d ON d.uei = p.uei AND d.name_key = p.name_key
            LEFT JOIN s_labor l ON l.uei = p.uei AND l.name_key = p.name_key
            WHERE d.uei IS NOT NULL OR l.uei IS NOT NULL)
        SELECT *,
            CASE
                WHEN dsbs_slug IS NOT NULL AND labor_slug IS NOT NULL
                     AND dsbs_slug = labor_slug THEN 'corroborated'
                WHEN dsbs_slug IS NOT NULL AND labor_slug IS NOT NULL THEN 'conflict'
                WHEN dsbs_slug IS NOT NULL THEN 'dsbs_only'
                WHEN labor_slug IS NOT NULL THEN 'labor_only'
                ELSE 'ambiguous'
            END AS verdict,
            coalesce(dsbs_slug, labor_slug) AS slug
        FROM merged""")

    # ── adjudicate vs existing identity rows (read-only) ──
    con.execute("""CREATE TEMP TABLE vs_existing AS
        SELECT a.*, i.person_linkedin_url_norm AS existing_slug
        FROM adjudicated a LEFT JOIN t_idn i USING (sam_person_id)""")
    stats = dict(con.execute(
        "SELECT verdict, count(*) FROM adjudicated GROUP BY 1").fetchall())
    (n_existing_agree, n_existing_conflict, n_new) = con.execute("""
        SELECT count(*) FILTER (WHERE existing_slug IS NOT NULL
                                AND existing_slug = slug),
               count(*) FILTER (WHERE existing_slug IS NOT NULL
                                AND slug IS NOT NULL
                                AND existing_slug <> slug),
               count(*) FILTER (WHERE existing_slug IS NULL
                                AND verdict IN ('corroborated','dsbs_only','labor_only'))
        FROM vs_existing""").fetchone()

    print(f"build_id={build_id}")
    for e in lineage:
        print(f"  input {e['name']} v{e['version']} rows={e['rows_at_read']:,}")
    print(f"labor rows w/ linkedin: {n_labor_raw:,}; name-agreement kept: "
          f"{n_labor_nameok:,} ({n_labor_raw - n_labor_nameok:,} excluded)")
    print(f"adjudication (spine-joined): {stats}")
    print(f"vs existing identity: agree={n_existing_agree:,} "
          f"conflict={n_existing_conflict:,} (read-only, never overwritten)")
    print(f"NEW appendable rows: {n_new:,}")
    for r in con.execute("""
        SELECT verdict, name_key, uei, slug FROM vs_existing
        WHERE existing_slug IS NULL
          AND verdict IN ('corroborated','dsbs_only','labor_only')
        LIMIT 6""").fetchall():
        print("  ", r)

    if not apply:
        print("\nDRY-RUN — no append. Re-run with --apply to write.")
        return {"build_id": build_id, "stats": stats, "new": n_new,
                "existing_agree": n_existing_agree,
                "existing_conflict": n_existing_conflict}

    by = {e["name"]: e for e in lineage}
    con.execute(f"""CREATE TEMP TABLE final AS
        SELECT sam_person_id, uei, name_key,
               slug AS person_linkedin_url_norm,
               CASE verdict
                   WHEN 'corroborated' THEN 'dsbs:' || dsbs_ref || ';labor:' || labor_ref
                   WHEN 'dsbs_only'    THEN 'dsbs:' || dsbs_ref
                   ELSE 'labor:' || labor_ref END AS match_source,
               CASE verdict
                   WHEN 'corroborated' THEN 'dsbs_serper+labor_direct'
                   WHEN 'dsbs_only'    THEN 'dsbs_serper_validated'
                   ELSE 'labor_poc_direct' END AS match_method,
               CASE verdict WHEN 'corroborated' THEN 1.0
                            WHEN 'dsbs_only' THEN 0.95
                            ELSE 0.90 END AS match_score,
               CASE verdict WHEN 'labor_only'
                    THEN '{SRC["sam_labor_poc_people"]}'
                    ELSE '{SRC["dsbs_poc_linkedin"]}' END AS matched_against_uri,
               CASE verdict WHEN 'labor_only'
                    THEN {by["sam_labor_poc_people"]["version"]}::BIGINT
                    ELSE {by["dsbs_poc_linkedin"]["version"]}::BIGINT
                    END AS matched_against_version,
               TIMESTAMP '{started:%Y-%m-%d %H:%M:%S}' AS matched_at,
               '{build_id}' AS build_id
        FROM vs_existing
        WHERE existing_slug IS NULL
          AND verdict IN ('corroborated','dsbs_only','labor_only')""")
    table = con.sql("SELECT * FROM final").to_arrow_table().cast(identity.schema())
    con.close()
    result = identity.append_matches(table, match_lineage=lineage)
    print(f"applied: {result}")
    return {"build_id": build_id, "stats": stats, "new": n_new, **result}


if __name__ == "__main__":
    print(run(apply="--apply" in sys.argv))
