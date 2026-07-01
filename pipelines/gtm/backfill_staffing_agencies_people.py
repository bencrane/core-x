#!/usr/bin/env python3
"""Backfill — land the dex-archive staffing-agency people into active/people (Gen-3 Lance SoR).

Adds the 29,563 target_people at the vertical-tagged staffing agencies (the cohort landed by
backfill_staffing_agencies_companies.py) into the canonical people spine, tagged
source_platform='dexarchive_staffing_agencies'. Identity-only landing — work-email enrichment
is a separate downstream step (active/work_emails, keyed by person_id).

SAFETY (this mutates the ~69k-row people SoR):
    * APPEND ONLY — mode="append", a new fragment. NEVER overwrite.
    * IDEMPOTENT — a person is added only if its person_id (= target_people.id, a stable dex
      UUID) is absent from active/people. Re-runs append nothing.
    * Asserts count_after == count_before + n_added; aborts the index step on mismatch.

SOURCE  (cold archive Parquet, DuckDB-over-R2):
    s3://data-sink/archive/dex/entities/target_people.parquet    (30,578 rows; PK id)
    s3://data-sink/archive/dex/entities/clay_find_people.parquet  (title via clay_find_person_id)
SCOPE   : target_company_id ∈ the staffing cohort in active/companies
          (source_platform='dexarchive_staffing_agencies') → 29,563 people / 6,725 firms.
TARGET  : s3://data-sink/active/people/  (append)

COLUMN MAP (source → people; 9-col schema):
    id                    → person_id             (stable dex UUID PK; 0 collision with the spine)
    target_company_id     → company_id            (FK → companies.company_id; resolves 1:1 to the cohort)
    (cohort join)         → normalized_domain     (DENORMALIZED from the company row — anchor-exact)
    full_name/first/last  → full_name/first_name/last_name
    clay latest_experience_title → title          (15,008 populated via clay_find_person_id; else NULL)
    person_linkedin_url   → person_linkedin_url    (100%)
    (literal)             → source_platform = 'dexarchive_staffing_agencies'
    NOT CARRIED: business_concept (free text, no column); work_email (separate enrichment grain).

INDEXES : person_id + company_id + normalized_domain + person_linkedin_url (all BTREE) rebuilt
          (replace=True) so the resolution keys cover the new fragment.

RUN:
    doppler run -p core-x -c prd -- uv run --no-project \
        --with pylance --with duckdb --with pyarrow \
        python3 pipelines/gtm/backfill_staffing_agencies_people.py [--dry-run]
"""
from __future__ import annotations

import argparse
import os

import duckdb
import lance
import pyarrow as pa

ACTIVE = os.environ.get("GTM_ACTIVE_ROOT", "s3://data-sink/active")
PEOPLE_URI = os.environ.get("GTM_PEOPLE_URI", f"{ACTIVE}/people/")
COMPANIES_URI = os.environ.get("GTM_COMPANIES_URI", f"{ACTIVE}/companies/")
ARCHIVE = "s3://data-sink/archive/dex/entities"
TARGET_PEOPLE = f"{ARCHIVE}/target_people.parquet"
CLAY_FIND_PEOPLE = f"{ARCHIVE}/clay_find_people.parquet"
COHORT_SOURCE = "dexarchive_staffing_agencies"
SOURCE_PLATFORM = "dexarchive_staffing_agencies"
REINDEX_BTREE = ["person_id", "company_id", "normalized_domain", "person_linkedin_url"]


def _storage_options() -> dict:
    ep = os.environ.get("R2_ENDPOINT")
    if not ep and os.environ.get("R2_ACCOUNT_ID"):
        ep = f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": ep,
        "region": "auto",
    }


def _r2_host() -> str:
    ep = os.environ.get("R2_ENDPOINT") or f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
    return ep.replace("https://", "").replace("http://", "")


def _rule(t: str) -> None:
    print(f"\n{'=' * 78}\n{t}\n{'=' * 78}", flush=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="identify + print; do not write")
    args = ap.parse_args()

    so = _storage_options()
    people_ds = lance.dataset(PEOPLE_URI, storage_options=so)
    target_schema = people_ds.schema
    count_before = people_ds.count_rows()

    cohort = lance.dataset(COMPANIES_URI, storage_options=so).scanner(
        filter=f"source_platform = '{COHORT_SOURCE}'",
        columns=["company_id", "normalized_domain"]).to_table()
    existing = people_ds.to_table(columns=["person_id"])

    con = duckdb.connect()
    con.execute("SET memory_limit='6GB';")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""CREATE SECRET r2 (TYPE S3, KEY_ID '{os.environ['R2_ACCESS_KEY_ID']}',
        SECRET '{os.environ['R2_SECRET_ACCESS_KEY']}', ENDPOINT '{_r2_host()}',
        URL_STYLE 'path', REGION 'auto');""")
    con.register("cohort", cohort)
    con.register("existing_people", existing)

    # ── project the cohort's target_people → the people schema, deduped on person_id ──
    cand = con.execute(f"""
        WITH tp AS (
            SELECT
                CAST(id AS VARCHAR)                   AS person_id,
                CAST(target_company_id AS VARCHAR)    AS company_id,
                CAST(clay_find_person_id AS VARCHAR)  AS clay_pid,
                nullif(trim(full_name), '')           AS full_name,
                nullif(trim(first_name), '')          AS first_name,
                nullif(trim(last_name), '')           AS last_name,
                nullif(trim(person_linkedin_url), '') AS person_linkedin_url
            FROM read_parquet('{TARGET_PEOPLE}')
        ),
        cfp AS (
            SELECT CAST(id AS VARCHAR) AS clay_pid,
                   max(nullif(trim(latest_experience_title), '')) AS title
            FROM read_parquet('{CLAY_FIND_PEOPLE}')
            GROUP BY 1
        )
        SELECT
            tp.person_id,
            tp.company_id,
            c.normalized_domain                       AS normalized_domain,
            tp.full_name,
            tp.first_name,
            tp.last_name,
            cfp.title                                 AS title,
            tp.person_linkedin_url,
            '{SOURCE_PLATFORM}'                       AS source_platform
        FROM tp
        JOIN cohort c        ON tp.company_id = c.company_id
        LEFT JOIN cfp        ON tp.clay_pid   = cfp.clay_pid
        WHERE tp.person_id NOT IN (SELECT person_id FROM existing_people WHERE person_id IS NOT NULL)
        ORDER BY tp.company_id, tp.person_id
    """).to_arrow_table()

    n = cand.num_rows
    firms = con.execute("SELECT count(DISTINCT company_id) FROM cohort").fetchone()[0]
    got_title = sum(1 for v in cand.column("title").to_pylist() if v)
    _rule(f"staffing-agency people to add → active/people: {n:,}")
    print(f"    source_platform   = '{SOURCE_PLATFORM}'", flush=True)
    print(f"    title populated   = {got_title:,} / {n:,} (via clay; rest NULL — enrichment later)", flush=True)
    print(f"    people count      = {count_before:,} → {count_before + n:,}", flush=True)

    if n == 0:
        print("\nNothing to add — every person_id already present (idempotent no-op).", flush=True)
        return 0
    if args.dry_run:
        print(f"\n[dry-run] would append {n:,} rows to {PEOPLE_URI}. No write performed.", flush=True)
        for r in cand.slice(0, 8).to_pylist():
            print(f"      {str(r['full_name'])[:26]:26} dom={str(r['normalized_domain'])[:24]:24} "
                  f"title={str(r['title'])[:32]}", flush=True)
        return 0

    # ── conform to the exact people schema (all mapped; NULL-fill any absent) ──
    populated = {name: cand.column(name) for name in cand.schema.names}
    arrays = [populated[f.name].cast(f.type) if f.name in populated else pa.nulls(n, f.type)
              for f in target_schema]
    new_tbl = pa.Table.from_arrays(arrays, schema=target_schema)

    _rule(f"append → {PEOPLE_URI}")
    lance.write_dataset(new_tbl, PEOPLE_URI, mode="append", storage_options=so)
    count_after = lance.dataset(PEOPLE_URI, storage_options=so).count_rows()
    print(f"    rows: {count_before:,} → {count_after:,}  (+{count_after - count_before:,})", flush=True)
    if count_after != count_before + n:
        raise SystemExit(f"ABORT: expected +{n} rows, got +{count_after - count_before} — NOT reindexing.")

    ds = lance.dataset(PEOPLE_URI, storage_options=so)
    for col in REINDEX_BTREE:
        ds.create_scalar_index(col, index_type="BTREE")  # replace=True default → covers new rows
        print(f"    BTREE ✓ {col} (rebuilt)", flush=True)

    _rule("verify — new cohort resolvable + FK-linked")
    ds = lance.dataset(PEOPLE_URI, storage_options=so)
    got = ds.scanner(filter=f"source_platform = '{SOURCE_PLATFORM}'",
                     columns=["person_id", "company_id", "normalized_domain"]).to_table()
    print(f"    source_platform='{SOURCE_PLATFORM}' now returns {got.num_rows:,} people "
          f"across {len(set(got.column('company_id').to_pylist())):,} firms", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
