#!/usr/bin/env python3
"""Enrich — fill NULL title on the dexarchive staffing people from active/clay_find_people.

The staffing-agency people landed by backfill_staffing_agencies_people.py carry a title on
15,008 / 29,563 (from the archive clay join); the rest are NULL. This gap-fills those NULLs by
matching person_linkedin_url (normalized, canonical rule) against the live GTM clay corpus
active/clay_find_people.linkedin_url_raw → latest_experience_title (~6.3k newly titled).

SAFETY (in-place column update on the ~99k-row people SoR):
    * TITLE-ONLY, NULL-FILL — only rows with source_platform='dexarchive_staffing_agencies'
      AND title IS NULL are candidates. Existing (non-null) titles are NEVER overwritten.
    * merge_insert(person_id).when_matched_update_all() with a source carrying the FULL current
      9-col row and ONLY title mutated — so no other column is disturbed. No inserts (no
      when_not_matched clause) → row count is invariant.
    * IDEMPOTENT — re-runs re-fill the same NULLs with the same clay title (a no-op in effect).
    * Asserts count_after == count_before (update, not append); no reindex (title is not indexed).

SOURCE  : s3://data-sink/active/clay_find_people/  (linkedin_url_raw, latest_experience_title)
MATCH   : canonical LinkedIn norm (lower → strip scheme → strip www. → strip trailing slash)
          applied to BOTH people.person_linkedin_url and clay.linkedin_url_raw.
TARGET  : s3://data-sink/active/people/  (title update on the staffing cohort)

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

ACTIVE = os.environ.get("GTM_ACTIVE_ROOT", "s3://data-sink/active")
PEOPLE_URI = os.environ.get("GTM_PEOPLE_URI", f"{ACTIVE}/people/")
CLAY_URI = os.environ.get("CLAY_FIND_PEOPLE_URI", f"{ACTIVE}/clay_find_people/")
COHORT_SOURCE = "dexarchive_staffing_agencies"

# Fleet-canonical person-LinkedIn normalization (== backfill_people_from_title_enrichment._PLI_NORM):
# lower → strip scheme → strip leading www. → strip trailing slash → NULL if emptied.
def _norm(col: str) -> str:
    return (f"nullif(rtrim(regexp_replace(regexp_replace(lower(trim({col})), "
            f"'^https?://', ''), '^www\\.', ''), '/'), '')")


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

    # cohort title coverage before
    cov_tbl = people_ds.scanner(filter=f"source_platform = '{COHORT_SOURCE}'",
                                columns=["title"]).to_table()
    cohort_n = cov_tbl.num_rows
    have_before = sum(1 for v in cov_tbl.column("title").to_pylist() if v)

    # NULL-title cohort rows — full 9-col row (so the update preserves every other column)
    null_rows = people_ds.scanner(
        filter=f"source_platform = '{COHORT_SOURCE}' AND title IS NULL",
        columns=[f.name for f in schema]).to_table()
    clay_tbl = lance.dataset(CLAY_URI, storage_options=so).to_table(
        columns=["linkedin_url_raw", "latest_experience_title"])

    con = duckdb.connect(":memory:")
    con.execute("SET memory_limit='6GB';")
    con.register("nulls", null_rows)
    con.register("clay", clay_tbl)

    matched = con.execute(f"""
        WITH clay_t AS (
            SELECT {_norm('linkedin_url_raw')} AS k,
                   max(nullif(trim(latest_experience_title), '')) AS clay_title
            FROM clay
            WHERE latest_experience_title IS NOT NULL AND {_norm('linkedin_url_raw')} IS NOT NULL
            GROUP BY 1
        )
        SELECT
            n.person_id, n.company_id, n.normalized_domain, n.full_name, n.first_name,
            n.last_name, c.clay_title AS title, n.person_linkedin_url, n.source_platform
        FROM nulls n
        JOIN clay_t c ON {_norm('n.person_linkedin_url')} = c.k
        WHERE c.clay_title IS NOT NULL
    """).to_arrow_table()

    n = matched.num_rows
    _rule(f"title gap-fill from clay → active/people (cohort '{COHORT_SOURCE}')")
    print(f"    cohort people          = {cohort_n:,}", flush=True)
    print(f"    title populated (before) = {have_before:,}", flush=True)
    print(f"    NULL-title candidates    = {null_rows.num_rows:,}", flush=True)
    print(f"    newly titled via clay    = {n:,}", flush=True)
    print(f"    title populated (after)  = {have_before + n:,}  "
          f"({(have_before + n) / cohort_n * 100:.1f}%)", flush=True)

    if n == 0:
        print("\nNo new titles resolved — no write.", flush=True)
        return 0
    if args.dry_run:
        print(f"\n[dry-run] would update title on {n:,} rows (row count unchanged). No write.", flush=True)
        for r in matched.slice(0, 8).to_pylist():
            print(f"      {str(r['full_name'])[:26]:26} {str(r['title'])[:40]}", flush=True)
        return 0

    # conform to the exact people schema (matched carries all 9 cols by name; title replaced)
    src = pa.Table.from_arrays(
        [matched.column(f.name).cast(f.type) for f in schema], schema=schema)

    _rule(f"merge_insert when_matched_update → {PEOPLE_URI}")
    people_ds.merge_insert("person_id").when_matched_update_all().execute(src)

    after = lance.dataset(PEOPLE_URI, storage_options=so)
    count_after = after.count_rows()
    print(f"    row count: {count_before:,} → {count_after:,}  (Δ {count_after - count_before:+,}; expect 0)", flush=True)
    if count_after != count_before:
        raise SystemExit(f"ABORT: update changed row count by {count_after - count_before} — investigate.")

    cov2 = after.scanner(filter=f"source_platform = '{COHORT_SOURCE}'", columns=["title"]).to_table()
    have_after = sum(1 for v in cov2.column("title").to_pylist() if v)
    _rule("verify — cohort title coverage")
    print(f"    title populated: {have_before:,} → {have_after:,}  (+{have_after - have_before:,})", flush=True)
    if have_after != have_before + n:
        raise SystemExit(f"ABORT: expected +{n} titles, got +{have_after - have_before}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
