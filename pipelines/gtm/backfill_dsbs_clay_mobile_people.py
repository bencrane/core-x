#!/usr/bin/env python3
"""Backfill — promote the clay-only DSBS-company people who already have a resolved MOBILE into
active/people (Gen-3 Lance SoR).

Cohort: people in clay_find_people who (a) work at a DSBS-certified firm (clay domain_norm ∈ the
crosswalk_dsbs_sam domain universe), (b) already have a mobile in phone_resolutions
(phone_type='mobile'), and (c) are NOT yet in active/people. Identity comes from clay, so tagged
source_platform='clay_find_people' (consistent with the existing clay-sourced people).

SAFETY (this mutates the people SoR):
    * APPEND ONLY — mode="append", a new fragment. NEVER overwrite.
    * IDEMPOTENT — a person is added only if its person_id (clay sha256(linkedin_url_norm)) is
      absent from active/people. Re-runs append nothing.
    * Asserts count_after == count_before + n_added; aborts the index step on mismatch.

SOURCES (live Lance, R2):
    active/clay_find_people/     — identity (person_id, name, title, linkedin) + domain_norm
    active/crosswalk_dsbs_sam/   — DSBS domain universe → uei (company_id) + best_domain
    active/phone_resolutions/    — the mobile gate (phone_type='mobile')
    active/people/               — existing person_id set (idempotency)
TARGET  : s3://data-sink/active/people/  (append)

COLUMN MAP (source → people; 9-col schema):
    clay person_id            → person_id            (PK; sha256(linkedin_url_norm))
    matched DSBS uei          → company_id           (FK → companies.company_id = uei)
    DSBS best_domain          → normalized_domain    (denormalized from the DSBS company row)
    clay full/first/last      → full_name/first_name/last_name
    clay matched_job_title    → title
    clay linkedin_url_raw     → person_linkedin_url
    (literal)                 → source_platform = 'clay_find_people'

INDEXES : person_id + company_id + normalized_domain + person_linkedin_url (BTREE) rebuilt.

RUN:
    doppler run -p core-x -c prd -- uv run --no-project \
        --with pylance --with duckdb --with pyarrow \
        python3 pipelines/gtm/backfill_dsbs_clay_mobile_people.py [--dry-run]
"""
from __future__ import annotations

import argparse
import os

import duckdb
import lance

from pipelines.gtm import _people_canonical as pc

ACTIVE = os.environ.get("GTM_ACTIVE_ROOT", "s3://data-sink/active")
# REPOINT: identity → canonical people; provenance → sidecar.
PEOPLE_URI = pc.PEOPLE_URI
CLAY_URI = os.environ.get("CLAY_FIND_PEOPLE_URI", f"{ACTIVE}/clay_find_people/")
XWALK_URI = os.environ.get("CROSSWALK_DSBS_SAM_URI", f"{ACTIVE}/crosswalk_dsbs_sam/")
PHONE_URI = os.environ.get("PHONE_RESOLUTIONS_URI", f"{ACTIVE}/phone_resolutions/")
SOURCE_PLATFORM = "clay_find_people"
SOURCE_REF = "backfill:dsbs_clay_mobile_people"


def _storage_options() -> dict:
    return pc.r2_storage_options()


def _rule(t: str) -> None:
    print(f"\n{'=' * 78}\n{t}\n{'=' * 78}", flush=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    so = _storage_options()
    people_ds = lance.dataset(PEOPLE_URI, storage_options=so)
    count_before = people_ds.count_rows()

    con = duckdb.connect()
    con.execute("SET memory_limit='6GB'; PRAGMA threads=4;")
    con.register("xw", lance.dataset(XWALK_URI, storage_options=so).scanner(
        columns=["uei", "best_domain", "matched_domain", "domain_entity_url",
                 "domain_website", "domain_email", "domain_additional"]).to_reader())
    con.register("cp", lance.dataset(CLAY_URI, storage_options=so).scanner(
        columns=["person_id", "domain_norm", "full_name", "first_name", "last_name",
                 "matched_job_title", "linkedin_url_raw"]).to_reader())
    con.register("ph", lance.dataset(PHONE_URI, storage_options=so).scanner(
        columns=["person_id", "phone_type", "phone"]).to_reader())

    con.execute("CREATE TABLE xwt AS SELECT * FROM xw")
    con.execute("CREATE TABLE clay AS SELECT * FROM cp")
    con.execute("CREATE TABLE phone AS SELECT * FROM ph")

    # DSBS domain → uei, and uei → best_domain (denorm anchor)
    con.execute("""CREATE TABLE dom_uei AS SELECT DISTINCT uei, lower(d) dom FROM (
        SELECT uei,best_domain d FROM xwt UNION ALL SELECT uei,matched_domain FROM xwt UNION ALL SELECT uei,domain_entity_url FROM xwt
        UNION ALL SELECT uei,domain_website FROM xwt UNION ALL SELECT uei,domain_email FROM xwt UNION ALL SELECT uei,domain_additional FROM xwt) WHERE d IS NOT NULL""")
    con.execute("CREATE TABLE uei_best AS SELECT uei, lower(best_domain) best_domain FROM xwt WHERE best_domain IS NOT NULL")
    con.execute("CREATE TABLE mobile AS SELECT DISTINCT person_id FROM phone WHERE phone_type='mobile' AND phone IS NOT NULL")

    # NO source_platform column — provenance routes to the sidecar via land_people; idempotency
    # is on canonical_person_id inside the merge_insert, so no existing-people anti-join here.
    cand = con.execute("""
        WITH clay_dsbs AS (
            SELECT c.person_id, lower(c.domain_norm) AS dom,
                   nullif(trim(c.full_name),'') full_name, nullif(trim(c.first_name),'') first_name,
                   nullif(trim(c.last_name),'') last_name, nullif(trim(c.matched_job_title),'') title,
                   nullif(trim(c.linkedin_url_raw),'') person_linkedin_url,
                   (SELECT min(u.uei) FROM dom_uei u WHERE u.dom = lower(c.domain_norm)) AS uei
            FROM clay c
            WHERE lower(c.domain_norm) IN (SELECT dom FROM dom_uei)
              AND c.person_id IN (SELECT person_id FROM mobile)
        ),
        picked AS (
            SELECT person_id,
                   arg_min(uei, dom)                 AS company_id,
                   max(full_name) full_name, max(first_name) first_name, max(last_name) last_name,
                   max(title) title, max(person_linkedin_url) person_linkedin_url
            FROM clay_dsbs GROUP BY person_id
        )
        SELECT p.person_id, p.company_id, b.best_domain AS normalized_domain,
               p.full_name, p.first_name, p.last_name, p.title, p.person_linkedin_url
        FROM picked p LEFT JOIN uei_best b ON b.uei = p.company_id
        ORDER BY p.company_id, p.person_id
    """).to_arrow_table()

    n = cand.num_rows
    got_title = sum(1 for v in cand.column("title").to_pylist() if v)
    got_li = sum(1 for v in cand.column("person_linkedin_url").to_pylist() if v)
    _rule(f"clay-only DSBS people WITH a mobile → canonical people + sidecar: {n:,} candidate rows")
    print(f"    source_platform            = '{SOURCE_PLATFORM}' (→ sidecar)", flush=True)
    print(f"    title populated            = {got_title:,} / {n:,}", flush=True)
    print(f"    person_linkedin_url        = {got_li:,} / {n:,}", flush=True)
    print(f"    people count               = {count_before:,} (merge_insert on canonical_person_id)", flush=True)

    if n == 0:
        print("\nNothing to land — cohort empty.", flush=True)
        return 0
    if args.dry_run:
        print(f"\n[dry-run] would land {n:,} candidate rows → {PEOPLE_URI} + sidecar. No write.", flush=True)
        for r in cand.slice(0, 8).to_pylist():
            print(f"      {str(r['full_name'])[:26]:26} cid={str(r['company_id'])[:14]:14} "
                  f"dom={str(r['normalized_domain'])[:22]:22} title={str(r['title'])[:26]}", flush=True)
        return 0

    _rule(f"land → {PEOPLE_URI} (identity) + sidecar (provenance)")
    res = pc.land_people(cand, SOURCE_PLATFORM, SOURCE_REF, so)
    count_after = lance.dataset(PEOPLE_URI, storage_options=so).count_rows()
    print(f"    people rows: {count_before:,} → {count_after:,}  (+{count_after - count_before:,})", flush=True)
    print(f"    sidecar candidates landed (merge_insert, idempotent): {res['sidecar_candidates']:,}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
