"""BLS EP "occupation" workbook (occupation.xlsx) → Lance — the remaining data tables.

The landed occupation.xlsx (landing/bls/employment-projections/) is a 12-table workbook.
Table 1.9 seeded the industry×occupation matrix scrape; this worker lands the rest of the
DATA-bearing tables, each as its own Lance dataset:

    Table 1.2  Occupational projections + worker characteristics  1,119 → bls_employment_projections_2024_2034 (UPGRADE)
    Table 1.10 Occupational separations and openings              1,116 → bls_ep_occupation_separations_openings_2024_2034
    Table 1.12 Factors affecting occupational utilization         1,001 → bls_ep_occupation_utilization_factors_2024_2034
    Table 1.1  Employment by major occupational group                22 → bls_ep_major_group_employment_2024_2034
    Table 1.11 Employment in STEM occupations                         ~7 → bls_ep_stem_occupations_2024_2034
    Tables 1.3-1.6 Fastest growing / most growth / fastest             → bls_ep_occupation_rankings_2024_2034
                   declining / largest declines (consolidated,          (ranking_list + rank)

Table 1.2 is a strict enrichment of the standalone bls_employment_projections feed (adds
median wage, education, experience, on-the-job training, openings, self-employed %), so it
OVERWRITES that dataset in place — the thin TE1000 columns it drops (percent-of-industry /
percent-of-occupation) survive in bls_ep_industry_occupation_matrix's TE1000 slice.

DELIBERATELY NOT INGESTED (documented)
    Index sheet — table-of-contents navigation.
    Table 1.8 (industry-occupation matrix, by occupation) — a 1,116-row occupation *directory*
        of links; its cells are the transpose of bls_ep_industry_occupation_matrix_2024_2034
        (all 113,473 (industry,occupation) cells already landed). Re-scraping = duplication.
    Table 1.9 (industry directory) — already consumed as the matrix-scrape seed.

FIDELITY (no data left behind)
    Each table: title row skipped, row-2 header, footnote/source rows dropped (key column
    null or starting [ / Note / Source). Positional rename (avoids en-dash header drift).
    Measures coerced to DOUBLE (— / blank → NULL); codes and characteristic text kept verbatim
    (trimmed). Provenance (source_sheet, projections_vintage, ingested_at). BTREE on codes.

    doppler run -p core-x -c prd -- python3 -m pipelines.bls.ep_occupation_workbook --all
    doppler run -p core-x -c prd -- python3 -m pipelines.bls.ep_occupation_workbook --dataset separations_openings

Terminal state → ops.bls_runs, best-effort.
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
import tempfile

from pipelines.bls.ingest import (
    BUCKET,
    DATA_STORAGE_VERSION,
    MAX_BYTES_PER_FILE,
    MAX_ROWS_PER_FILE,
    _build_indexes,
    _record_run,
    _s3_client,
    _storage_options,
)

SCRATCH_DIR = tempfile.gettempdir()
LANDING_KEY = "landing/bls/employment-projections/occupation.xlsx"
RELEASE = "2024-2034"


def _a(name: str) -> str:
    return f"s3://{BUCKET}/active/{name}/"


# canonical column order per sheet (positional — dodges en-dash header matching)
C12 = ["occupation_title", "occupation_code", "occupation_type", "emp_2024", "emp_2034",
       "emp_distribution_pct_2024", "emp_distribution_pct_2034", "emp_change_2024_2034",
       "emp_pct_change_2024_2034", "pct_self_employed_2024", "openings_annual_avg",
       "median_annual_wage_2024", "typical_education", "work_experience",
       "typical_on_the_job_training", "ooh_content"]
C110 = ["occupation_title", "occupation_code", "occupation_type", "emp_2024", "emp_2034",
        "emp_change_2024_2034", "emp_pct_change_2024_2034", "labor_force_exit_rate",
        "occupational_transfer_rate", "total_separations_rate", "labor_force_exits_annual_avg",
        "occupational_transfers_annual_avg", "total_separations_annual_avg", "openings_annual_avg"]
C112 = ["occupation_title", "occupation_code", "industry_title", "industry_code",
        "utilization_factors"]
C111 = ["occupation_category", "emp_2024", "emp_2034", "emp_change_2024_2034",
        "emp_pct_change_2024_2034", "median_annual_wage_2024"]
C1 = ["occupation_title", "occupation_code", "emp_2024", "emp_2034", "emp_change_2024_2034",
      "emp_pct_change_2024_2034", "median_annual_wage_2024"]
CRANK = C1  # 1.3-1.6 share the major-group schema

# median_annual_wage stays VERBATIM (string): BLS topcodes the highest earners as
# ">=239200" — coercing to numeric silently nulls them (data left behind).
TEXT = {"occupation_title", "occupation_code", "occupation_type", "occupation_category",
        "industry_title", "industry_code", "typical_education", "work_experience",
        "typical_on_the_job_training", "ooh_content", "utilization_factors", "ranking_list",
        "median_annual_wage_2024"}

DATASETS: dict[str, dict] = {
    "employment_projections": {
        "uri": _a("bls_employment_projections_2024_2034"), "sheet": "Table 1.2",
        "cols": C12, "key": "occupation_code",
        "btree": ["occupation_code"], "bitmap": ["occupation_type"],
    },
    "separations_openings": {
        "uri": _a("bls_ep_occupation_separations_openings_2024_2034"), "sheet": "Table 1.10",
        "cols": C110, "key": "occupation_code",
        "btree": ["occupation_code"], "bitmap": ["occupation_type"],
    },
    "utilization_factors": {
        "uri": _a("bls_ep_occupation_utilization_factors_2024_2034"), "sheet": "Table 1.12",
        "cols": C112, "key": "occupation_code",
        "btree": ["occupation_code", "industry_code"], "bitmap": [],
    },
    "major_group": {
        "uri": _a("bls_ep_major_group_employment_2024_2034"), "sheet": "Table 1.1",
        "cols": C1, "key": "occupation_code",
        "btree": ["occupation_code"], "bitmap": [],
    },
    "stem": {
        "uri": _a("bls_ep_stem_occupations_2024_2034"), "sheet": "Table 1.11",
        "cols": C111, "key": "occupation_category",
        "btree": [], "bitmap": [],
    },
    "rankings": {
        "uri": _a("bls_ep_occupation_rankings_2024_2034"),
        "sheets": {"Table 1.3": "fastest_growing", "Table 1.4": "most_job_growth",
                   "Table 1.5": "fastest_declining", "Table 1.6": "largest_job_declines"},
        "cols": CRANK, "key": "occupation_code",
        "btree": ["occupation_code"], "bitmap": ["ranking_list"],
    },
}

_FOOTNOTE = ("[", "Note", "Source", "Footnote", "*")


def _read_clean(pd, xlsx: str, sheet: str, cols: list[str], key: str):
    """Skip title row, row-2 header, drop footnote rows, positional rename, type-clean."""
    df = pd.read_excel(xlsx, sheet_name=sheet, skiprows=1, header=0, dtype=object)
    df = df.iloc[:, :len(cols)].copy()
    df.columns = cols
    kc = df[key].astype("string")
    df = df[kc.notna() & ~kc.str.strip().str.startswith(_FOOTNOTE, na=False)].copy()
    for c in cols:
        if c in TEXT:
            s = df[c].astype("string").str.strip()
            df[c] = s.where(s.notna(), None)
        else:
            df[c] = pd.to_numeric(
                df[c].astype("string").str.replace(",", "", regex=False), errors="coerce")
    return df.reset_index(drop=True)


def _to_lance(df, uri, source_sheet, now, so):
    import lance
    import pyarrow as pa

    df = df.copy()
    df["source_sheet"] = source_sheet
    df["projections_vintage"] = RELEASE
    tbl = pa.Table.from_pandas(df, preserve_index=False)
    tbl = tbl.append_column("ingested_at", pa.array([now] * tbl.num_rows, pa.timestamp("us", tz="UTC")))
    # pandas StringDtype → from_pandas yields large_string; Lance scalar index requires 'string'.
    norm = [pa.field(f.name, pa.string() if pa.types.is_large_string(f.type)
                     else pa.binary() if pa.types.is_large_binary(f.type) else f.type)
            for f in tbl.schema]
    tbl = tbl.cast(pa.schema(norm))
    lance.write_dataset(tbl, uri, mode="overwrite", data_storage_version=DATA_STORAGE_VERSION,
                        max_rows_per_file=MAX_ROWS_PER_FILE, max_bytes_per_file=MAX_BYTES_PER_FILE,
                        storage_options=so)
    return tbl.num_rows


def ingest_dataset(name: str, xlsx: str, so, now) -> dict:
    import pandas as pd

    spec = DATASETS[name]
    uri = spec["uri"]
    started, status, error, built, rows = dt.datetime.now(dt.timezone.utc), "error", None, [], 0
    try:
        if name == "rankings":
            parts = []
            for sheet, label in spec["sheets"].items():
                d = _read_clean(pd, xlsx, sheet, spec["cols"], spec["key"])
                d.insert(0, "ranking_list", label)
                d.insert(1, "rank", range(1, len(d) + 1))
                parts.append(d)
            df = pd.concat(parts, ignore_index=True)
            src = "Tables 1.3-1.6"
        else:
            df = _read_clean(pd, xlsx, spec["sheet"], spec["cols"], spec["key"])
            src = spec["sheet"]
        rows = _to_lance(df, uri, src, now, so)
        print(f"[{name}] wrote {rows:,} rows -> {uri}")
        built = _build_indexes(uri, spec["btree"], spec["bitmap"], so)
        status = "success"
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
        raise
    finally:
        _record_run(name, uri, LANDING_KEY, RELEASE, int(rows), 0, built, status, error,
                    started, dt.datetime.now(dt.timezone.utc))
    return {"dataset": name, "rows": int(rows), "indexes": built, "uri": uri}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="BLS EP occupation.xlsx remaining tables → Lance.")
    ap.add_argument("--dataset", choices=sorted(DATASETS))
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--keep-temp", action="store_true")
    args = ap.parse_args(argv)
    targets = list(DATASETS) if args.all else ([args.dataset] if args.dataset else [])
    if not targets:
        ap.error("specify --all or --dataset <name>")

    so = _storage_options()
    s3 = _s3_client()
    now = dt.datetime.now(dt.timezone.utc)
    xlsx = os.path.join(SCRATCH_DIR, "bls_occupation_workbook.xlsx")
    s3.download_file(BUCKET, LANDING_KEY, xlsx)
    try:
        results = [ingest_dataset(n, xlsx, so, now) for n in targets]
    finally:
        if not args.keep_temp:
            try:
                os.remove(xlsx)
            except OSError:
                pass

    print("\n=== EP occupation-workbook ingest ===")
    total = 0
    for r in results:
        total += r["rows"]
        print(f"  {r['dataset']:24s} rows={r['rows']:>6,}  idx={len(r['indexes']):>2}  -> {r['uri']}")
    print(f"  {'TOTAL':24s} rows={total:>6,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
