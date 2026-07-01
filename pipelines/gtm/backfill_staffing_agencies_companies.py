#!/usr/bin/env python3
"""Backfill — land the dex-archive staffing agencies into active/companies (Gen-3 Lance SoR).

Adds the 24,398 vertical-tagged staffing agencies from active/staffing_agencies (materialized
from the archived legacy dex-db) into the canonical companies spine, tagged
source_platform='dexarchive_staffing_agencies'. Mirrors backfill_sfnet_companies.py.

SAFETY (this mutates the ~25k-row companies SoR):
    * APPEND ONLY — mode="append", a new fragment. NEVER overwrite (that would wipe the SoR).
    * IDEMPOTENT — a company is added only if its company_id (= staffing target_company_id,
      a stable dex UUID) is absent from active/companies. Re-runs append nothing. Domain-level
      collisions are NOT deduped — normalized_domain is a non-unique anchor and domain
      resolution is a downstream concern (per companies_people_bulk.py's contract).
    * Asserts count_after == count_before + n_added; aborts the index step on mismatch.

SOURCE  : s3://data-sink/active/staffing_agencies/  (24,398 rows; industries_served 100% populated)
TARGET  : s3://data-sink/active/companies/          (append)

COLUMN MAP (source → companies; every other companies column NULL, schema-conformant):
    target_company_id     → company_id            (stable dex UUID PK; 24,398 distinct, 0 collision)
    company_name          → company_name
    domain_norm           → normalized_domain      (already in the companies anchor space)
    company_linkedin_url  → company_linkedin_url
    (literal)             → source_platform = 'dexarchive_staffing_agencies'
    (literal)             → industry = 'Staffing and Recruiting'   (true by construction; live BITMAP value)
    employee_band         → employee_size_band     ('unknown' → NULL; vocab otherwise identical)
    year_founded (int64)  → founded_year (int32)
    NOT CARRIED: industries_served (11-vertical served verticals) and revenue_range have NO
      column in the companies spine. The canonical home for served-industries is the
      active/company_target_industries edge grain — materialized separately if required.

INDEXES : company_id + normalized_domain (BTREE) and industry + employee_size_band (BITMAP)
          rebuilt (replace=True) so the resolution + categorical filters cover the new rows.
          company_type / hq_region BITMAPs left as-is (new rows are NULL there → scan fallback).

RUN:
    doppler run -p core-x -c prd -- uv run --no-project \
        --with pylance --with duckdb --with pyarrow \
        python3 pipelines/gtm/backfill_staffing_agencies_companies.py [--dry-run]
"""
from __future__ import annotations

import argparse
import os

import duckdb
import lance
import pyarrow as pa

ACTIVE = os.environ.get("GTM_ACTIVE_ROOT", "s3://data-sink/active")
COMPANIES_URI = os.environ.get("GTM_COMPANIES_URI", f"{ACTIVE}/companies/")
STAFFING_URI = os.environ.get("STAFFING_AGENCIES_URI", f"{ACTIVE}/staffing_agencies/")
SOURCE_PLATFORM = "dexarchive_staffing_agencies"
REINDEX_BTREE = ["company_id", "normalized_domain"]
REINDEX_BITMAP = ["industry", "employee_size_band"]


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
    con = duckdb.connect()
    con.execute("SET memory_limit='6GB';")

    comp_ds = lance.dataset(COMPANIES_URI, storage_options=so)
    target_schema = comp_ds.schema
    count_before = comp_ds.count_rows()

    con.register("sa_r", lance.dataset(STAFFING_URI, storage_options=so).scanner(
        columns=["target_company_id", "company_name", "domain_norm",
                 "company_linkedin_url", "employee_band", "year_founded"]
    ).to_reader())
    con.execute("CREATE TABLE sa AS SELECT * FROM sa_r")
    con.register("co_r", comp_ds.scanner(columns=["company_id", "normalized_domain"]).to_reader())
    con.execute("CREATE TABLE companies AS SELECT * FROM co_r")

    # ── report-only: domains already present under other sources (NOT deduped) ──
    dom_overlap = con.execute("""
        SELECT count(*) FROM (
            SELECT DISTINCT domain_norm FROM sa WHERE domain_norm IS NOT NULL
        ) s WHERE s.domain_norm IN (SELECT normalized_domain FROM companies WHERE normalized_domain IS NOT NULL)
    """).fetchone()[0]

    # ── net-new company rows (idempotent on company_id = target_company_id) ──────
    cand = con.execute(f"""
        WITH src AS (
            SELECT
                CAST(target_company_id AS VARCHAR)                     AS company_id,
                nullif(trim(company_name), '')                        AS company_name,
                nullif(trim(domain_norm), '')                         AS normalized_domain,
                nullif(trim(company_linkedin_url), '')                AS company_linkedin_url,
                '{SOURCE_PLATFORM}'                                   AS source_platform,
                'Staffing and Recruiting'                             AS industry,
                CASE WHEN lower(trim(employee_band)) = 'unknown' THEN NULL
                     ELSE nullif(trim(employee_band), '') END         AS employee_size_band,
                CAST(year_founded AS INTEGER)                         AS founded_year
            FROM sa
            WHERE target_company_id IS NOT NULL AND trim(CAST(target_company_id AS VARCHAR)) <> ''
        )
        SELECT * FROM src
        WHERE company_id NOT IN (SELECT company_id FROM companies WHERE company_id IS NOT NULL)
        ORDER BY company_name
    """).to_arrow_table()

    n = cand.num_rows
    _rule(f"staffing agencies to add → companies: {n:,}")
    print(f"    source_platform          = '{SOURCE_PLATFORM}'", flush=True)
    print(f"    domains already in spine  = {dom_overlap:,} (kept as distinct rows, not deduped)", flush=True)
    print(f"    net-new domains           = {n - dom_overlap:,}", flush=True)
    print(f"    companies count           = {count_before:,} → {count_before + n:,}", flush=True)

    if n == 0:
        print("\nNothing to add — every target_company_id already present (idempotent no-op).", flush=True)
        return 0
    if args.dry_run:
        print(f"\n[dry-run] would append {n:,} rows to {COMPANIES_URI}. No write performed.", flush=True)
        # sample of what lands
        for r in cand.slice(0, 8).to_pylist():
            print(f"      {str(r['company_name'])[:40]:40} dom={r['normalized_domain'] or '—':28} "
                  f"band={r['employee_size_band'] or '—'}", flush=True)
        return 0

    # ── conform to the exact companies schema (populate the mapped cols, NULL the rest) ──
    populated = {name: cand.column(name) for name in cand.schema.names}
    arrays = [populated[f.name].cast(f.type) if f.name in populated else pa.nulls(n, f.type)
              for f in target_schema]
    new_tbl = pa.Table.from_arrays(arrays, schema=target_schema)

    _rule(f"append → {COMPANIES_URI}")
    lance.write_dataset(new_tbl, COMPANIES_URI, mode="append", storage_options=so)
    count_after = lance.dataset(COMPANIES_URI, storage_options=so).count_rows()
    print(f"    rows: {count_before:,} → {count_after:,}  (+{count_after - count_before:,})", flush=True)
    if count_after != count_before + n:
        raise SystemExit(f"ABORT: expected +{n} rows, got +{count_after - count_before} — NOT reindexing.")

    ds = lance.dataset(COMPANIES_URI, storage_options=so)
    for col in REINDEX_BTREE:
        ds.create_scalar_index(col, index_type="BTREE")  # replace=True default → covers new rows
        print(f"    BTREE  ✓ {col} (rebuilt)", flush=True)
    for col in REINDEX_BITMAP:
        ds.create_scalar_index(col, index_type="BITMAP")
        print(f"    BITMAP ✓ {col} (rebuilt)", flush=True)

    _rule("verify — new cohort resolvable")
    ds = lance.dataset(COMPANIES_URI, storage_options=so)
    got = ds.scanner(filter=f"source_platform = '{SOURCE_PLATFORM}'",
                     columns=["company_id", "company_name", "normalized_domain", "industry"]).to_table()
    print(f"    source_platform='{SOURCE_PLATFORM}' now returns {got.num_rows:,} rows", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
