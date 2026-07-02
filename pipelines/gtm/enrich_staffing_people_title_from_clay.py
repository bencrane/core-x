#!/usr/bin/env python3
"""Enrich — fill NULL title on the dexarchive staffing people from active/clay_find_people.

The staffing-agency people landed by backfill_staffing_agencies_people.py carry a title on a
subset; the rest are NULL. This gap-fills those NULLs by matching person_linkedin_url (normalized,
canonical rule) against the live GTM clay corpus active/clay_find_people.linkedin_url_raw →
latest_experience_title.

SAFETY (in-place title-only UPDATE on the canonical people SoR):
    * COHORT via SIDECAR — the staffing cohort no longer carries source_platform on people; its
      canonical_person_ids are read from person_source_platforms where source_platform=
      'dexarchive_staffing_agencies' (BITMAP pushdown), then joined back to people_canonical.
    * TITLE-ONLY, NULL-FILL — only cohort rows with title IS NULL are candidates. Existing
      (non-null) titles are NEVER overwritten.
    * merge_insert(canonical_person_id).when_matched_update_all() with a source carrying the FULL
      current people row and ONLY title mutated — so no other column is disturbed. No inserts
      (no when_not_matched clause) → row count is invariant.
    * IDEMPOTENT — re-runs re-fill the same NULLs with the same clay title (a no-op in effect).
    * Asserts count_after == count_before (update, not append); no reindex (title is not indexed).

SOURCES : s3://data-sink/active/clay_find_people/            (linkedin_url_raw, latest_experience_title)
          s3://data-sink/active/person_source_platforms/     (cohort membership via source_platform)
MATCH   : canonical LinkedIn norm (lower → strip scheme → strip www. → strip trailing slash)
          applied to BOTH people.person_linkedin_url and clay.linkedin_url_raw.
TARGET  : s3://data-sink/active/people_canonical/  (title update on the staffing cohort)

RUN:
    doppler run -p core-x -c prd -- uv run --no-project \
        --with pylance --with duckdb --with pyarrow \
        python3 pipelines/gtm/enrich_staffing_people_title_from_clay.py [--dry-run]
"""
from __future__ import annotations

import argparse
import os

import duckdb
import lance
import pyarrow as pa

from pipelines.gtm import _people_canonical as pc

ACTIVE = os.environ.get("GTM_ACTIVE_ROOT", "s3://data-sink/active")
# REPOINT: title update on the canonical people dataset; cohort membership from the sidecar.
PEOPLE_URI = pc.PEOPLE_URI
SIDECAR_URI = pc.SIDECAR_URI
CLAY_URI = os.environ.get("CLAY_FIND_PEOPLE_URI", f"{ACTIVE}/clay_find_people/")
COHORT_SOURCE = "dexarchive_staffing_agencies"

# Fleet-canonical person-LinkedIn normalization (== backfill_people_from_title_enrichment._PLI_NORM):
# lower → strip scheme → strip leading www. → strip trailing slash → NULL if emptied.
def _norm(col: str) -> str:
    return (f"nullif(rtrim(regexp_replace(regexp_replace(lower(trim({col})), "
            f"'^https?://', ''), '^www\\.', ''), '/'), '')")


def _storage_options() -> dict:
    return pc.r2_storage_options()


def _rule(t: str) -> None:
    print(f"\n{'=' * 78}\n{t}\n{'=' * 78}", flush=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="identify + print; do not write")
    args = ap.parse_args()

    so = _storage_options()
    people_ds = lance.dataset(PEOPLE_URI, storage_options=so)
    schema = people_ds.schema
    count_before = people_ds.count_rows()

    # Cohort membership from the sidecar (source_platform is no longer a people column). BITMAP
    # pushdown on source_platform → the cohort's canonical_person_ids.
    cohort_ids = lance.dataset(SIDECAR_URI, storage_options=so).scanner(
        filter=f"source_platform = '{COHORT_SOURCE}'",
        columns=["canonical_person_id"]).to_table()

    con = duckdb.connect(":memory:")
    con.execute("SET memory_limit='6GB';")
    con.register("cohort_ids", cohort_ids)

    # Cohort rows in canonical people (full row so the update preserves every other column).
    cohort_rows = people_ds.to_table(columns=[f.name for f in schema])
    con.register("people_all", cohort_rows)
    cohort_tbl = con.execute(
        "SELECT p.* FROM people_all p "
        "WHERE p.canonical_person_id IN (SELECT canonical_person_id FROM cohort_ids)"
    ).to_arrow_table()
    cohort_n = cohort_tbl.num_rows
    have_before = sum(1 for v in cohort_tbl.column("title").to_pylist() if v)

    con.register("cohort_rows", cohort_tbl)
    clay_tbl = lance.dataset(CLAY_URI, storage_options=so).to_table(
        columns=["linkedin_url_raw", "latest_experience_title"])
    con.register("clay", clay_tbl)

    matched = con.execute(f"""
        WITH clay_t AS (
            SELECT {_norm('linkedin_url_raw')} AS k,
                   max(nullif(trim(latest_experience_title), '')) AS clay_title
            FROM clay
            WHERE latest_experience_title IS NOT NULL AND {_norm('linkedin_url_raw')} IS NOT NULL
            GROUP BY 1
        )
        SELECT n.* REPLACE (c.clay_title AS title)
        FROM cohort_rows n
        JOIN clay_t c ON {_norm('n.person_linkedin_url')} = c.k
        WHERE n.title IS NULL AND c.clay_title IS NOT NULL
    """).to_arrow_table()

    n = matched.num_rows
    null_before = cohort_n - have_before
    _rule(f"title gap-fill from clay → canonical people (cohort '{COHORT_SOURCE}', via sidecar)")
    print(f"    cohort people          = {cohort_n:,}", flush=True)
    print(f"    title populated (before) = {have_before:,}", flush=True)
    print(f"    NULL-title candidates    = {null_before:,}", flush=True)
    print(f"    newly titled via clay    = {n:,}", flush=True)
    print(f"    title populated (after)  = {have_before + n:,}  "
          f"({(have_before + n) / cohort_n * 100:.1f}%)" if cohort_n else "", flush=True)

    if n == 0:
        print("\nNo new titles resolved — no write.", flush=True)
        return 0
    if args.dry_run:
        print(f"\n[dry-run] would update title on {n:,} rows (row count unchanged). No write.", flush=True)
        for r in matched.slice(0, 8).to_pylist():
            print(f"      {str(r['full_name'])[:26]:26} {str(r['title'])[:40]}", flush=True)
        return 0

    # conform to the exact people schema (matched carries all cols by name; title replaced)
    src = pa.Table.from_arrays(
        [matched.column(f.name).cast(f.type) for f in schema], schema=schema)

    _rule(f"merge_insert(canonical_person_id) when_matched_update → {PEOPLE_URI}")
    people_ds.merge_insert("canonical_person_id").when_matched_update_all().execute(src)

    after = lance.dataset(PEOPLE_URI, storage_options=so)
    count_after = after.count_rows()
    print(f"    row count: {count_before:,} → {count_after:,}  (Δ {count_after - count_before:+,}; expect 0)", flush=True)
    if count_after != count_before:
        raise SystemExit(f"ABORT: update changed row count by {count_after - count_before} — investigate.")

    # verify cohort title coverage (re-resolve cohort via sidecar)
    chk = duckdb.connect(":memory:")
    chk.register("cohort_ids", cohort_ids)
    chk.register("people_all", after.to_table(columns=["canonical_person_id", "title"]))
    cov2 = chk.execute(
        "SELECT p.title FROM people_all p "
        "WHERE p.canonical_person_id IN (SELECT canonical_person_id FROM cohort_ids)"
    ).fetchall()
    have_after = sum(1 for (v,) in cov2 if v)
    _rule("verify — cohort title coverage")
    print(f"    title populated: {have_before:,} → {have_after:,}  (+{have_after - have_before:,})", flush=True)
    if have_after != have_before + n:
        raise SystemExit(f"ABORT: expected +{n} titles, got +{have_after - have_before}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
