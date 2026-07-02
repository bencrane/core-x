"""L1 dimension (sidecar) — soc_state_wage: one OEWS wage row per (SOC x state).

The per-state companion to soc_priced_skilled. soc_priced_skilled carries the NATIONAL priced+skilled
occupation vector; this sidecar carries the STATE-level wage vector so a query can answer "what does
this occupation cost to staff in state X" — the wage-arbitrage axis (e.g. Software Developers median
CA $174,410 vs AL $122,540, live 2026-07-02).

  source   bls_oews_2025   OEWS May-2025 STATE, cross-industry, detailed slice
                           (area_type='2', i_group='cross-industry', naics='000000',
                           o_group='detailed', own_code='1235'). 35,223 rows = 1 per (prim_state,
                           occ_code); 51 states+DC x ~690 detailed SOCs; grain live-verified 1:1
                           (zero (prim_state, occ_code) dups).

GRAIN     1 row per (soc_code, prim_state). FAIL-CLOSED: count(*) == count(distinct (soc_code,
          prim_state)) is asserted before publish, and the row count is gated to the expected band
          (30000..40000) — a violation raises, no write.

TABLE     s3://data-sink/active/soc_state_wage/    BTREE: soc_code, prim_state
          soc_code (dashed 6-digit 'NN-NNNN'), prim_state (USPS), state_fips (2-digit), state_name,
          tot_emp (BIGINT), + 12 OEWS wage measures (DOUBLE, cleaned), source, soc_vintage, ingested_at.

CLEAN     OEWS wage/emp columns are VARCHAR in the SoR with suppression sentinels: '*' = not released
          (-> NULL), '#' = at/above the topcode (marker stripped, floor value kept), and thousands
          commas can appear. Each measure is cleaned with
          TRY_CAST(NULLIF(REPLACE(REPLACE(col, ',', ''), '#', ''), '*') AS DOUBLE); tot_emp -> BIGINT
          the same way. soc_code = OEWS occ_code verbatim (dashed 6-digit) — joins 1:N to
          soc_priced_skilled / naics_psc_labor_profile_categories on the same key.

SOURCE    Derived L1 sidecar — reads only bls_oews_2025 under s3://data-sink/active/. No landing
          fetch, no external HTTP, no joins. Idempotent (mode='overwrite'): re-run after any OEWS
          refresh. Reuses pipelines.bls.ingest R2 plumbing (_storage_options) and the shared Lance
          index builder (_build_indexes) — no re-implementation.

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'duckdb>=1.1' --with 'pylance>=7' --with 'pyarrow>=17' --with 'boto3>=1.34' \
      python3 -m pipelines.reference.materialize_soc_state_wage build

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'duckdb>=1.1' --with 'pylance>=7' --with 'pyarrow>=17' --with 'boto3>=1.34' \
      python3 -m pipelines.reference.materialize_soc_state_wage verify
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys

# Reuse the repo's R2 plumbing + Lance index builder by import — do NOT re-implement.
from pipelines.bls.ingest import _build_indexes, _storage_options

ACTIVE = "s3://data-sink/active"

OEWS_URI = os.environ.get("BLS_OEWS_LANCE_URI", f"{ACTIVE}/bls_oews_2025")
TARGET_URI = os.environ.get("SOC_STATE_WAGE_URI", f"{ACTIVE}/soc_state_wage/")

DATA_STORAGE_VERSION = "2.1"       # net-new Lance dataset -> pin current default storage version
SOURCE = "OEWS_M2025_state"
SOC_VINTAGE = "SOC_2018"

BTREE = ["soc_code", "prim_state"]
BITMAP: list[str] = []

# Expected 1-row-per-(SOC,state) band (live-probed 35,223 = 51 states/DC x detailed SOCs).
EXPECTED_ROWS = 35223
ROW_BAND = (30000, 40000)

# Live-confirmed spot: Software Developers (15-1252) median annual wage in CA — asserted in verify()
# to catch a silent OEWS cleaning regression.
SPOT_SOC = "15-1252"
SPOT_STATE = "CA"
SPOT_A_MEDIAN = 174410.0

# OEWS STATE, cross-industry, all-ownership, detailed 6-digit slice — collapses to 35,223 distinct
# (prim_state, occ_code) with zero duplicates (live-probed 2026-07-02).
OEWS_FILTER = (
    "area_type = '2' "
    "AND i_group = 'cross-industry' "
    "AND naics = '000000' "
    "AND o_group = 'detailed' "
    "AND own_code = '1235'"
)

# tot_emp -> BIGINT; the 12 wage measures -> DOUBLE (sentinel-cleaned).
OEWS_WAGE_COLS = [
    "h_mean", "a_mean",
    "h_pct10", "h_pct25", "h_median", "h_pct75", "h_pct90",
    "a_pct10", "a_pct25", "a_median", "a_pct75", "a_pct90",
]

os.environ.setdefault("LANCE_BYPASS_SPILLING", "true")


def log(m):
    print(f"[{dt.datetime.now(dt.timezone.utc).isoformat()}] {m}", flush=True)


def _clean(col: str) -> str:
    """OEWS sentinel-cleaning cast (per-column): strip thousands commas, drop the '#' topcode marker
    (keep the floor value), map '*' -> NULL, TRY_CAST to DOUBLE."""
    return f"TRY_CAST(NULLIF(REPLACE(REPLACE({col}, ',', ''), '#', ''), '*') AS DOUBLE)"


def _build_sql() -> str:
    """Single DuckDB statement: the OEWS STATE detailed slice (cleaned, typed), one row per
    (soc_code, prim_state)."""
    wage_proj = ",\n        ".join(f"{_clean(c)} AS {c}" for c in OEWS_WAGE_COLS)
    return f"""
    SELECT
        occ_code AS soc_code,
        prim_state,
        area AS state_fips,
        area_title AS state_name,
        TRY_CAST(NULLIF(REPLACE(REPLACE(tot_emp, ',', ''), '#', ''), '*') AS BIGINT) AS tot_emp,
        {wage_proj},
        '{SOURCE}' AS source,
        '{SOC_VINTAGE}' AS soc_vintage,
        now() AS ingested_at
    FROM oews_src
    WHERE {OEWS_FILTER}
    ORDER BY occ_code, prim_state
    """


def build() -> dict:
    import duckdb
    import lance

    so = _storage_options()
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=8;")

    # Register the Lance dataset OBJECT (not a one-shot .to_reader() stream — that drains on the
    # first scan). DuckDB scans the Lance dataset directly and pushes the filter down.
    con.register("oews_src", lance.dataset(OEWS_URI, storage_options=so))
    log("registered bls_oews_2025 Lance scanner")

    tbl = con.execute(_build_sql()).to_arrow_table()
    con.close()
    rows = tbl.num_rows
    log(f"assembled soc_state_wage: {rows:,} rows x {tbl.num_columns} cols")

    # -- FAIL-CLOSED gate — structural 1:1 on (soc_code, prim_state) + band before any publish -----
    soc = tbl.column("soc_code").to_pylist()
    st = tbl.column("prim_state").to_pylist()
    n_null = sum(1 for a, b in zip(soc, st) if a is None or b is None or b == "")
    n_distinct = len({(a, b) for a, b in zip(soc, st)})
    if n_null:
        raise RuntimeError(f"GATE FAIL: {n_null} rows with NULL/empty soc_code or prim_state")
    if rows != n_distinct:
        raise RuntimeError(
            f"GATE FAIL: grain not 1:1 — rows={rows} != distinct (soc_code, prim_state)={n_distinct}")
    if not (ROW_BAND[0] <= rows <= ROW_BAND[1]):
        raise RuntimeError(
            f"GATE FAIL: row count {rows} outside expected band {ROW_BAND} (expected ~{EXPECTED_ROWS})")
    log(f"GATE PASS: rows={rows:,} distinct_key={n_distinct:,} band={ROW_BAND} null_key=0")

    lance.write_dataset(tbl, TARGET_URI, mode="overwrite",
                        data_storage_version=DATA_STORAGE_VERSION, storage_options=so)
    log(f"wrote Lance -> {TARGET_URI}")
    built = _build_indexes(TARGET_URI, BTREE, BITMAP, so)

    ds = lance.dataset(TARGET_URI, storage_options=so)
    written = ds.count_rows()
    if written != rows:
        raise RuntimeError(f"POST-WRITE FAIL: lance {written} != assembled {rows}")

    results = {
        "uri": TARGET_URI,
        "rows": written,
        "distinct_soc_state": n_distinct,
        "distinct_soc": len({a for a in soc}),
        "distinct_state": len({b for b in st}),
        "columns": tbl.schema.names,
        "a_median_castable": ds.count_rows(filter="a_median IS NOT NULL"),
        "indexes": built,
        "source": SOURCE,
    }
    print(json.dumps(results, indent=2, default=str))
    return results


def verify() -> dict:
    import lance

    so = _storage_options()
    ds = lance.dataset(TARGET_URI, storage_options=so)
    rows = ds.count_rows()
    keys = ds.scanner(columns=["soc_code", "prim_state"]).to_table()
    soc = keys.column("soc_code").to_pylist()
    st = keys.column("prim_state").to_pylist()
    distinct_key = len({(a, b) for a, b in zip(soc, st)})
    idx_cols = {f for i in ds.list_indices() for f in (i.get("fields") or [])}

    spot = ds.scanner(
        columns=["soc_code", "prim_state", "state_name", "tot_emp", "a_median", "h_mean"],
        filter=f"soc_code = '{SPOT_SOC}' AND prim_state IN ('CA', 'AL', 'TX', 'NY')",
    ).to_table().to_pylist()
    spot_median = next(
        (r["a_median"] for r in spot if r["prim_state"] == SPOT_STATE), None)

    out = {
        "uri": TARGET_URI,
        "rows": rows,
        "distinct_soc_state": distinct_key,
        "grain_1to1": rows == distinct_key,
        "distinct_state": len({b for b in st}),
        "columns": ds.schema.names,
        "index_cols": sorted(idx_cols),
        "a_median_castable": ds.count_rows(filter="a_median IS NOT NULL"),
        "spot_check": spot,
        f"{SPOT_SOC}_{SPOT_STATE}_a_median": spot_median,
    }
    print(json.dumps(out, indent=2, default=str))

    # -- FAIL-LOUD verify — grain, index presence, wage regression all non-zero-exit --------------
    errors: list[str] = []
    if rows != distinct_key:
        errors.append(f"grain broken: rows={rows} != distinct_key={distinct_key}")
    if "soc_code" not in idx_cols or "prim_state" not in idx_cols:
        errors.append(f"BTREE soc_code/prim_state index absent — index_cols={sorted(idx_cols)}")
    if spot_median != SPOT_A_MEDIAN:
        errors.append(
            f"wage regression: {SPOT_SOC}/{SPOT_STATE} a_median={spot_median} != expected {SPOT_A_MEDIAN}")
    if errors:
        raise RuntimeError("VERIFY FAIL: " + " | ".join(errors))
    return out


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "verify"
    if cmd == "build":
        build()
    elif cmd == "verify":
        verify()
    else:
        print(f"unknown command: {cmd} (build|verify)")
        sys.exit(2)


if __name__ == "__main__":
    main()
