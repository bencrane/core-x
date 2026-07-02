#!/usr/bin/env python3
"""Backfill — land the DSBS certified entities into active/companies (Gen-3 Lance SoR).

Adds the DSBS certified-firm universe (from the canonical crosswalk_dsbs_sam spine) into the
companies spine, tagged source_platform='dsbs'. Mirrors backfill_staffing_agencies_companies.py.

SAFETY (this mutates the companies SoR):
    * APPEND ONLY — mode="append", a new fragment. NEVER overwrite.
    * IDEMPOTENT — a company is added only if its company_id (= uei, the stable DSBS PK) is absent
      from active/companies. Re-runs append nothing.
    * NO domain-dedup — per operator direction and the companies contract, a domain already present
      under another source_platform is kept as a distinct row (normalized_domain is a non-unique
      anchor; domain resolution is a downstream concern).
    * Asserts count_after == count_before + n_added; aborts the index step on mismatch.

SOURCE  : s3://data-sink/active/crosswalk_dsbs_sam/  (1 row / uei; 67,234 DSBS firms)
TARGET  : s3://data-sink/active/companies/           (append)

COLUMN MAP (source → companies; every other companies column NULL, schema-conformant):
    uei                        → company_id            (stable DSBS PK; idempotency key)
    uei                        → uei                   (native — makes DSBS rows uei-resolvable)
    dsbs_legal_business_name   → company_name
    best_domain                → normalized_domain     (identity-first, junk-blocklisted; nullable)
    company_linkedin_url       → company_linkedin_url  (matched_domain → PDL; nullable)
    industry / size / founded left NULL — DSBS is multi-industry; no defensible literal.

    source_platform='dsbs' is NOT written onto the company row — it routes to the
    active/company_source_platforms sidecar via record_company_sources(). companies_canonical
    carries no source_platform column.

INDEXES : company_id + normalized_domain + uei (BTREE) rebuilt (replace=True) so resolution covers
          the new rows.

RUN:
    doppler run -p core-x -c prd -- uv run --no-project \
        --with pylance --with duckdb --with pyarrow \
        python3 pipelines/gtm/backfill_dsbs_companies.py [--dry-run]
"""
from __future__ import annotations

import argparse
import os

import duckdb
import lance
import pyarrow as pa

from pipelines.gtm import _company_source as cs

ACTIVE = os.environ.get("GTM_ACTIVE_ROOT", "s3://data-sink/active")
COMPANIES_URI = cs.COMPANIES_URI  # REPOINT → active/companies_canonical/ (no source_platform col)
DSBS_XWALK_URI = os.environ.get("CROSSWALK_DSBS_SAM_URI", f"{ACTIVE}/crosswalk_dsbs_sam/")
SOURCE_PLATFORM = "dsbs"
SOURCE_REF = "backfill:dsbs_companies"
REINDEX_BTREE = ["company_id", "normalized_domain", "uei"]


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

    con.register("dsbs_r", lance.dataset(DSBS_XWALK_URI, storage_options=so).scanner(
        columns=["uei", "dsbs_legal_business_name", "best_domain", "company_linkedin_url"]
    ).to_reader())
    con.execute("CREATE TABLE dsbs AS SELECT * FROM dsbs_r")
    con.register("co_r", comp_ds.scanner(columns=["company_id", "normalized_domain"]).to_reader())
    con.execute("CREATE TABLE companies AS SELECT * FROM co_r")

    dom_overlap = con.execute("""
        SELECT count(*) FROM (
            SELECT DISTINCT lower(best_domain) d FROM dsbs WHERE best_domain IS NOT NULL
        ) s WHERE s.d IN (SELECT lower(normalized_domain) FROM companies WHERE normalized_domain IS NOT NULL)
    """).fetchone()[0]

    cand = con.execute(f"""
        WITH src AS (
            SELECT
                CAST(uei AS VARCHAR)                        AS company_id,
                CAST(uei AS VARCHAR)                        AS uei,
                nullif(trim(dsbs_legal_business_name), '')  AS company_name,
                nullif(trim(best_domain), '')               AS normalized_domain,
                nullif(trim(company_linkedin_url), '')      AS company_linkedin_url
            FROM dsbs
            WHERE uei IS NOT NULL AND trim(CAST(uei AS VARCHAR)) <> ''
        )
        SELECT * FROM src
        WHERE company_id NOT IN (SELECT company_id FROM companies WHERE company_id IS NOT NULL)
        ORDER BY company_name
    """).to_arrow_table()

    n = cand.num_rows
    with_dom = sum(1 for v in cand.column("normalized_domain").to_pylist() if v)
    with_li = sum(1 for v in cand.column("company_linkedin_url").to_pylist() if v)
    _rule(f"DSBS entities to add → companies: {n:,}")
    print(f"    source_platform            = '{SOURCE_PLATFORM}'", flush=True)
    print(f"    with normalized_domain     = {with_dom:,}  (domainless = {n - with_dom:,})", flush=True)
    print(f"    with company_linkedin_url  = {with_li:,}", flush=True)
    print(f"    domains already in spine   = {dom_overlap:,} (kept as distinct rows, NOT deduped)", flush=True)
    print(f"    companies count            = {count_before:,} → {count_before + n:,}", flush=True)

    if n == 0:
        print("\nNothing to add — every uei already present (idempotent no-op).", flush=True)
        return 0
    if args.dry_run:
        print(f"\n[dry-run] would append {n:,} rows to {COMPANIES_URI}. No write performed.", flush=True)
        for r in cand.slice(0, 8).to_pylist():
            print(f"      {str(r['company_name'])[:38]:38} dom={r['normalized_domain'] or '—':26} "
                  f"li={'y' if r['company_linkedin_url'] else '—'}", flush=True)
        return 0

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
        if col in ds.schema.names:
            ds.create_scalar_index(col, index_type="BTREE")
            print(f"    BTREE  ✓ {col} (rebuilt)", flush=True)

    _rule(f"record provenance → {cs.COMPANY_SOURCE_PLATFORMS_URI}")
    added_ids = cand.column("company_id").to_pylist()
    res = cs.record_company_sources(added_ids, SOURCE_PLATFORM, SOURCE_REF, so)
    print(f"    sidecar (company_id, '{SOURCE_PLATFORM}') pairs merged (idempotent): "
          f"{res['sidecar_candidates']:,}", flush=True)

    _rule("verify — new cohort resolvable via sidecar")
    src_ds = lance.dataset(cs.COMPANY_SOURCE_PLATFORMS_URI, storage_options=so)
    got = src_ds.scanner(filter=f"source_platform = '{SOURCE_PLATFORM}'",
                         columns=["company_id", "source_platform"]).to_table()
    print(f"    source_platform='{SOURCE_PLATFORM}' now returns {got.num_rows:,} sidecar rows", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
