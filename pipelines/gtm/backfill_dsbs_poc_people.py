#!/usr/bin/env python3
"""Backfill — promote the resolved DSBS-POC ↔ person matches into active/people (Gen-3 Lance SoR).

Cohort: every person in active/dsbs_poc_people (the resolved DSBS-POC ↔ person bridge, commit 844)
whose person_id is NOT yet in active/people. These are SBA-DSBS certified-firm POCs (contact_person +
current_principals) already resolved to a LinkedIn identity — enrichment-ready people that belong in
the people SoR, and the staging ground for the downstream LeadMagic mobile-enrichment cohort.

SAFETY (this mutates the people SoR):
    * APPEND ONLY — mode="append", a new fragment. NEVER overwrite.
    * IDEMPOTENT — a person is added only if its person_id is absent from active/people. Re-runs
      append nothing.
    * Asserts count_after == count_before + n_added; aborts the index step on mismatch.

SOURCES (live Lance, R2):
    active/dsbs_poc_people/  — resolved bridge (person_id, uei, best_domain, person_*, poc_type)
    active/people/           — existing person_id set (idempotency) + the target schema
TARGET  : s3://data-sink/active/people/  (append)

COLUMN MAP (dsbs_poc_people → people; 9-col schema):
    person_id                 → person_id            (PK; sha256(linkedin_url_norm))
    matched DSBS uei          → company_id           (FK → companies.company_id = uei; a person who
                                                       is a POC at multiple firms picks ONE row:
                                                       both > contact_person > current_principal,
                                                       then min(uei) — deterministic)
    DSBS best_domain          → normalized_domain    (denormalized from the picked firm row)
    person_full_name          → full_name  (+ split on first space → first_name / last_name)
    person_title              → title
    person_linkedin_url       → person_linkedin_url
    (literal)                 → source_platform = 'dsbs_poc'

INDEXES : person_id + company_id + normalized_domain + person_linkedin_url (BTREE) rebuilt.

RUN:
    doppler run -p core-x -c prd -- uv run --no-project \\
        --with pylance --with duckdb --with pyarrow \\
        python3 pipelines/gtm/backfill_dsbs_poc_people.py [--dry-run]
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
POC_PEOPLE_URI = os.environ.get("DSBS_POC_PEOPLE_URI", f"{ACTIVE}/dsbs_poc_people/")
SOURCE_PLATFORM = "dsbs_poc"
SOURCE_REF = "backfill:dsbs_poc_people"


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
    con.register("poc", lance.dataset(POC_PEOPLE_URI, storage_options=so).scanner(
        columns=["person_id", "uei", "best_domain", "poc_type",
                 "person_full_name", "person_title", "person_linkedin_url"]).to_reader())

    con.execute("CREATE TABLE pocp AS SELECT * FROM poc")

    # One row per legacy person_id. A person who is a POC at multiple DSBS firms picks a single
    # company_id deterministically: both (contact_person AND principal) > contact_person >
    # current_principal, then min(uei). All person-level fields come from that picked row.
    # NO source_platform column — provenance routes to the sidecar via land_people; idempotency
    # is on canonical_person_id inside the merge_insert, so no existing-people anti-join here.
    cand = con.execute("""
        WITH ranked AS (
            SELECT
                person_id,
                uei AS company_id,
                lower(best_domain) AS normalized_domain,
                nullif(trim(person_full_name), '')     AS full_name,
                nullif(trim(person_title), '')         AS title,
                nullif(trim(person_linkedin_url), '')  AS person_linkedin_url,
                row_number() OVER (
                    PARTITION BY person_id
                    ORDER BY CASE poc_type WHEN 'both' THEN 0
                                           WHEN 'contact_person' THEN 1
                                           ELSE 2 END, uei
                ) AS rn
            FROM pocp
            WHERE person_id IS NOT NULL
              AND uei IS NOT NULL
              AND nullif(trim(person_linkedin_url), '') IS NOT NULL
        )
        SELECT
            person_id,
            company_id,
            normalized_domain,
            full_name,
            nullif(trim(split_part(full_name, ' ', 1)), '')                              AS first_name,
            nullif(trim(substr(full_name, length(split_part(full_name, ' ', 1)) + 2)), '') AS last_name,
            title,
            person_linkedin_url
        FROM ranked
        WHERE rn = 1
        ORDER BY company_id, person_id
    """).to_arrow_table()

    n = cand.num_rows
    got_title = sum(1 for v in cand.column("title").to_pylist() if v)
    got_li = sum(1 for v in cand.column("person_linkedin_url").to_pylist() if v)
    got_dom = sum(1 for v in cand.column("normalized_domain").to_pylist() if v)
    _rule(f"resolved DSBS-POC people → canonical people + sidecar: {n:,} candidate rows")
    print(f"    source_platform            = '{SOURCE_PLATFORM}' (→ sidecar)", flush=True)
    print(f"    company_id (uei) populated = {n:,} / {n:,}", flush=True)
    print(f"    normalized_domain          = {got_dom:,} / {n:,}", flush=True)
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
                  f"dom={str(r['normalized_domain'])[:24]:24} title={str(r['title'])[:24]}", flush=True)
        return 0

    _rule(f"land → {PEOPLE_URI} (identity) + sidecar (provenance)")
    res = pc.land_people(cand, SOURCE_PLATFORM, SOURCE_REF, so)
    count_after = lance.dataset(PEOPLE_URI, storage_options=so).count_rows()
    print(f"    people rows: {count_before:,} → {count_after:,}  (+{count_after - count_before:,})", flush=True)
    print(f"    sidecar candidates landed (merge_insert, idempotent): {res['sidecar_candidates']:,}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
