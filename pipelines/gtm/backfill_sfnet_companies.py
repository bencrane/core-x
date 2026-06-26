#!/usr/bin/env python3
"""Backfill — re-add absent SFNet member companies into active/companies (Gen-3 Lance SoR).

A prior cleanup deleted the large-bank SFNet members from active/companies. Policy
correction: those accounts should be KEPT and disregarded via a firm-size query filter,
not deleted. This backfill re-instates every SFNet member company (per the directory seed
active/sfnet_main_contacts) that is currently ABSENT from active/companies.

SAFETY (this mutates the 25k-row companies SoR):
    * APPEND ONLY — mode="append", a new fragment. NEVER overwrite (that would wipe the SoR).
    * IDEMPOTENT — a company is added only if its company_id, normalized_domain, AND
      normalized company_name are all absent from active/companies. Re-runs append nothing.
    * Asserts count_after == count_before + n_added; aborts the index step on mismatch.

SOURCE  : s3://data-sink/active/sfnet_main_contacts/  (resolved_company_id IS NULL ⇒ absent)
TARGET  : s3://data-sink/active/companies/            (append)
NEW ROWS: company_id = sfnet_company_id (stable SFNet directory UUID),
          company_name, normalized_domain populated from the directory; source_platform =
          'sfnet-directory'; every other firmographic field NULL (schema-conformant).
          NOTE: re-added rows have NULL employee_size_band / employees_on_linkedin — the
          firm-size filter cannot disregard them until firmo enrichment runs over
          source_platform='sfnet-directory'.

INDEXES : company_id + normalized_domain rebuilt (replace=True) so the resolution keys cover
          the new rows. Categorical indices (industry/size/type/region) are left as-is —
          the new rows are NULL there and Lance falls back to a flat scan for them.

RUN:
    doppler run -p core-x -c prd -- uv run --no-project \
        --with pylance --with duckdb --with pyarrow \
        python3 pipelines/gtm/backfill_sfnet_companies.py [--dry-run]
"""
from __future__ import annotations

import argparse
import os

import duckdb
import lance
import pyarrow as pa

ACTIVE = os.environ.get("GTM_ACTIVE_ROOT", "s3://data-sink/active")
COMPANIES_URI = os.environ.get("GTM_COMPANIES_URI", f"{ACTIVE}/companies/")
SFNET_MC_URI = os.environ.get("SFNET_MC_URI", f"{ACTIVE}/sfnet_main_contacts/")
SOURCE_PLATFORM = "sfnet-directory"
REINDEX = ["company_id", "normalized_domain"]


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

    con.register("mc_r", lance.dataset(SFNET_MC_URI, storage_options=so).scanner(
        columns=["sfnet_company_id", "company_name", "normalized_domain", "resolved_company_id"]
    ).to_reader())
    con.execute("CREATE TABLE mc AS SELECT * FROM mc_r")
    con.register("co_r", comp_ds.scanner(columns=["company_id", "company_name", "normalized_domain"]).to_reader())
    con.execute("CREATE TABLE companies AS SELECT * FROM co_r")

    # ── genuinely-absent SFNet directory companies ──────────────────────────
    cand = con.execute("""
        WITH dir AS (
            SELECT sfnet_company_id,
                   any_value(company_name)      AS company_name,
                   any_value(normalized_domain) AS normalized_domain,
                   max(CASE WHEN resolved_company_id IS NOT NULL THEN 1 ELSE 0 END) AS resolved_any
            FROM mc WHERE sfnet_company_id IS NOT NULL AND sfnet_company_id <> ''
            GROUP BY sfnet_company_id
        )
        SELECT d.sfnet_company_id AS company_id, d.company_name, d.normalized_domain
        FROM dir d
        WHERE d.resolved_any = 0
          AND d.sfnet_company_id NOT IN (SELECT company_id FROM companies)
          AND (d.normalized_domain IS NULL
               OR d.normalized_domain NOT IN (SELECT normalized_domain FROM companies WHERE normalized_domain IS NOT NULL))
          AND lower(trim(d.company_name)) NOT IN
              (SELECT lower(trim(company_name)) FROM companies WHERE company_name IS NOT NULL)
        ORDER BY d.company_name
    """).fetch_arrow_table()

    n = cand.num_rows
    _rule(f"absent SFNet member companies to re-add: {n}")
    for r in cand.to_pylist():
        print(f"    {r['company_name']:<48} dom={r['normalized_domain'] or '—'}", flush=True)

    if n == 0:
        print("\nNothing to add — active/companies already contains every SFNet member (idempotent no-op).", flush=True)
        return 0
    if args.dry_run:
        print(f"\n[dry-run] would append {n} rows to {COMPANIES_URI} (count {count_before:,} → {count_before + n:,}).", flush=True)
        return 0

    # ── build schema-conformant rows (populate 4 fields, NULL the rest) ──────
    populated = {
        "company_id": cand.column("company_id").cast(pa.string()),
        "company_name": cand.column("company_name").cast(pa.string()),
        "normalized_domain": cand.column("normalized_domain").cast(pa.string()),
        "source_platform": pa.array([SOURCE_PLATFORM] * n, pa.string()),
    }
    arrays = [populated[f.name].cast(f.type) if f.name in populated else pa.nulls(n, f.type)
              for f in target_schema]
    new_tbl = pa.Table.from_arrays(arrays, schema=target_schema)

    _rule(f"append → {COMPANIES_URI}")
    lance.write_dataset(new_tbl, COMPANIES_URI, mode="append", storage_options=so)
    count_after = lance.dataset(COMPANIES_URI, storage_options=so).count_rows()
    print(f"    rows: {count_before:,} → {count_after:,}  (+{count_after - count_before})", flush=True)
    if count_after != count_before + n:
        raise SystemExit(f"ABORT: expected +{n} rows, got +{count_after - count_before} — NOT reindexing.")

    ds = lance.dataset(COMPANIES_URI, storage_options=so)
    for col in REINDEX:
        ds.create_scalar_index(col, index_type="BTREE")  # replace=True default → covers new rows
        print(f"    BTREE ✓ {col} (rebuilt)", flush=True)

    _rule("verify — new rows resolvable via index")
    ds = lance.dataset(COMPANIES_URI, storage_options=so)
    got = ds.scanner(filter=f"source_platform = '{SOURCE_PLATFORM}'",
                     columns=["company_id", "company_name", "normalized_domain"]).to_table()
    print(f"    source_platform='{SOURCE_PLATFORM}' now returns {got.num_rows} rows", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
