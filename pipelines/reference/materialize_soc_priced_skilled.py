"""L1 dimension — soc_priced_skilled: one priced, skilled-context row per 6-digit SOC.

The canonical occupation dimension keyed on the dashed 6-digit SOC ('NN-NNNN'). It fuses
three live Lance datasets into a single 1-row-per-SOC fact-lite dimension:

  wages/employment  bls_oews_2025            OEWS May-2025 national, cross-industry, detailed
                                             slice (area_type='1', i_group='cross-industry',
                                             naics='000000', o_group='detailed', own_code='1235').
                                             830 distinct 6-digit SOCs, zero duplicate occ_code.
  skilled context   onet_occupation_data     O*NET title + description, collapsed 8->6 digit via
                                             a deterministic .00-base pick (867 distinct SOCs).
  outlook (LEFT)    bls_employment_projections_2024_2034                projected openings + growth
                    bls_ep_occupation_separations_openings_2024_2034    separations/churn rates
                                             both keyed 1:1 on dashed occupation_code, 100% cover
                                             of the OEWS detailed grain — attached LEFT so they can
                                             never drop or fan out an OEWS core row.

GRAIN     1 row per 6-digit SOC. OEWS detailed is the spine (830 SOCs); O*NET + EP attach LEFT.
          FAIL-CLOSED: count(*) == count(distinct soc_code) is asserted before publish, and the
          row count is gated to the expected band (800..1000) — a violation raises, no write.

TABLE     s3://data-sink/active/soc_priced_skilled/    BTREE: soc_code
          soc_code, soc_title (OEWS occ_title), onet_title, onet_description,
          tot_emp (BIGINT), + 12 OEWS wage measures (DOUBLE, cleaned),
          openings_annual_avg / emp_change_2024_2034 / emp_pct_change_2024_2034 (DOUBLE, EP proj),
          total_separations_rate / labor_force_exit_rate / occupational_transfer_rate (DOUBLE, EP sep),
          source, soc_vintage, ingested_at.

CLEAN     OEWS wage/emp columns are VARCHAR in the SoR with suppression sentinels: '*' = not
          released (-> NULL), '#' = at/above the topcode (marker stripped, topcode floor kept),
          and thousands commas can appear in other slices. Each measure is cleaned with
          TRY_CAST(NULLIF(REPLACE(REPLACE(col, ',', ''), '#', ''), '*') AS DOUBLE); tot_emp is
          cast to BIGINT the same way. O*NET ships a native 6-digit soc_code (used directly as the
          join key); the 8->6 collapse prefers the '.00' base O*NET-SOC row, else the lowest
          o_net_soc_code (rn=1). EP median wage is deliberately NOT pulled — wages come from OEWS.

SOURCE    Derived L1 — reads only Lance SoR datasets under s3://data-sink/active/. No landing
          fetch, no external HTTP. Idempotent (mode='overwrite'): re-run after any OEWS/O*NET/EP
          refresh. Reuses pipelines.bls.ingest R2 plumbing (_storage_options) and the shared
          Lance index builder (_build_indexes) — no re-implementation.

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'duckdb>=1.1' --with 'pylance>=7' --with 'pyarrow>=17' --with 'boto3>=1.34' \
      python3 -m pipelines.reference.materialize_soc_priced_skilled build

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'duckdb>=1.1' --with 'pylance>=7' --with 'pyarrow>=17' --with 'boto3>=1.34' \
      python3 -m pipelines.reference.materialize_soc_priced_skilled verify
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys

# Reuse the repo's R2 plumbing + Lance index builder by import — do NOT re-implement.
from pipelines.bls.ingest import _build_indexes, _storage_options

ACTIVE = "s3://data-sink/active"

# Source Lance datasets (SoR) — env-overridable for staging.
OEWS_URI = os.environ.get("BLS_OEWS_LANCE_URI", f"{ACTIVE}/bls_oews_2025")
ONET_URI = os.environ.get("ONET_OCCUPATION_LANCE_URI", f"{ACTIVE}/onet_occupation_data")
EP_PROJ_URI = os.environ.get(
    "BLS_EP_LANCE_URI", f"{ACTIVE}/bls_employment_projections_2024_2034")
EP_SEP_URI = os.environ.get(
    "BLS_EP_SEPARATIONS_LANCE_URI",
    f"{ACTIVE}/bls_ep_occupation_separations_openings_2024_2034")

# Output dimension.
TARGET_URI = os.environ.get("SOC_PRICED_SKILLED_URI", f"{ACTIVE}/soc_priced_skilled/")

DATA_STORAGE_VERSION = "2.1"       # net-new Lance dataset -> pin current default storage version
SOURCE = "OEWS_M2025+ONET+EP_2024_2034"
SOC_VINTAGE = "SOC_2018"

BTREE = ["soc_code"]
BITMAP: list[str] = []

# Expected 1-row-per-SOC band (OEWS detailed spine = 830; O*NET distinct = 867). The LEFT joins
# never add rows, so the published count must equal the OEWS detailed distinct SOC count.
EXPECTED_ROWS = 830
ROW_BAND = (800, 1000)

# Live-confirmed wage floor for the top-paid national SOC — asserted in verify() to catch a
# silent OEWS cleaning regression (a_median of 29-1243, Pediatric Surgeons).
SPOT_SOC = "29-1243"
SPOT_A_MEDIAN = 559030.0

# OEWS national, cross-industry, all-ownership, detailed 6-digit slice — collapses to 830 distinct
# occ_code with zero duplicates (live-probed).
OEWS_FILTER = (
    "area_type = '1' "
    "AND i_group = 'cross-industry' "
    "AND naics = '000000' "
    "AND o_group = 'detailed' "
    "AND own_code = '1235'"
)

# OEWS wage/employment measures. tot_emp -> BIGINT; the 12 wage measures -> DOUBLE. Sentinels:
# '*' = not released (-> NULL); '#' = topcode marker (stripped, floor value kept); ',' thousands
# separators stripped for cross-slice reuse.
OEWS_WAGE_COLS = [
    "h_mean", "a_mean",
    "h_pct10", "h_pct25", "h_median", "h_pct75", "h_pct90",
    "a_pct10", "a_pct25", "a_median", "a_pct75", "a_pct90",
]

os.environ.setdefault("LANCE_BYPASS_SPILLING", "true")


def log(m):
    print(f"[{dt.datetime.now(dt.timezone.utc).isoformat()}] {m}", flush=True)


def _clean(col: str) -> str:
    """OEWS sentinel-cleaning cast expression (per-column). Strips thousands commas, drops the '#'
    topcode marker (keeps the floor value), maps '*' -> NULL, TRY_CAST to DOUBLE."""
    return f"TRY_CAST(NULLIF(REPLACE(REPLACE({col}, ',', ''), '#', ''), '*') AS DOUBLE)"


def _build_sql() -> str:
    """Single DuckDB statement: OEWS detailed spine (cleaned, typed) LEFT JOIN the O*NET 8->6
    collapse and both EP outlook datasets, producing exactly one row per 6-digit SOC."""
    wage_proj = ",\n            ".join(f"{_clean(c)} AS {c}" for c in OEWS_WAGE_COLS)
    return f"""
    WITH oews AS (
        SELECT
            occ_code AS soc_code,
            occ_title AS soc_title,
            TRY_CAST(NULLIF(REPLACE(REPLACE(tot_emp, ',', ''), '#', ''), '*') AS BIGINT) AS tot_emp,
            {wage_proj}
        FROM oews_src
        WHERE {OEWS_FILTER}
    ),
    onet AS (
        SELECT soc_code, onet_title, onet_description
        FROM (
            SELECT
                soc_code,
                title AS onet_title,
                description AS onet_description,
                ROW_NUMBER() OVER (
                    PARTITION BY soc_code
                    ORDER BY (CASE WHEN o_net_soc_code LIKE '%.00' THEN 0 ELSE 1 END),
                             o_net_soc_code
                ) AS rn
            FROM onet_src
        )
        WHERE rn = 1
    ),
    ep_proj AS (
        SELECT
            occupation_code AS soc_code,
            TRY_CAST(openings_annual_avg AS DOUBLE) AS openings_annual_avg,
            TRY_CAST(emp_change_2024_2034 AS DOUBLE) AS emp_change_2024_2034,
            TRY_CAST(emp_pct_change_2024_2034 AS DOUBLE) AS emp_pct_change_2024_2034
        FROM ep_proj_src
        WHERE occupation_type = 'Line item'
    ),
    ep_sep AS (
        SELECT
            occupation_code AS soc_code,
            TRY_CAST(total_separations_rate AS DOUBLE) AS total_separations_rate,
            TRY_CAST(labor_force_exit_rate AS DOUBLE) AS labor_force_exit_rate,
            TRY_CAST(occupational_transfer_rate AS DOUBLE) AS occupational_transfer_rate
        FROM ep_sep_src
        WHERE occupation_type = 'Line item'
    )
    SELECT
        o.soc_code,
        o.soc_title,
        n.onet_title,
        n.onet_description,
        o.tot_emp,
        o.h_mean, o.a_mean,
        o.h_pct10, o.h_pct25, o.h_median, o.h_pct75, o.h_pct90,
        o.a_pct10, o.a_pct25, o.a_median, o.a_pct75, o.a_pct90,
        p.openings_annual_avg,
        p.emp_change_2024_2034,
        p.emp_pct_change_2024_2034,
        s.total_separations_rate,
        s.labor_force_exit_rate,
        s.occupational_transfer_rate,
        '{SOURCE}' AS source,
        '{SOC_VINTAGE}' AS soc_vintage,
        now() AS ingested_at
    FROM oews o
    LEFT JOIN onet    n ON n.soc_code = o.soc_code
    LEFT JOIN ep_proj p ON p.soc_code = o.soc_code
    LEFT JOIN ep_sep  s ON s.soc_code = o.soc_code
    ORDER BY o.soc_code
    """


def build() -> dict:
    import duckdb
    import lance

    so = _storage_options()
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=8;")

    # Register the Lance dataset OBJECTS (not one-shot .to_reader() streams — those drain on the
    # first scan and any re-scan yields 0 rows). DuckDB scans the Lance dataset directly and pushes
    # the projection+filter down, so only the projected/filtered rows are pulled through the worker.
    con.register("oews_src", lance.dataset(OEWS_URI, storage_options=so))
    con.register("onet_src", lance.dataset(ONET_URI, storage_options=so))
    con.register("ep_proj_src", lance.dataset(EP_PROJ_URI, storage_options=so))
    con.register("ep_sep_src", lance.dataset(EP_SEP_URI, storage_options=so))
    log("registered OEWS + O*NET + EP(proj) + EP(sep) Lance scanners")

    # .to_arrow_table() returns a materialized pyarrow.Table (has .num_rows/.column()); the older
    # .arrow() returns a one-shot RecordBatchReader that the fail-closed gate below cannot index.
    tbl = con.execute(_build_sql()).to_arrow_table()
    con.close()
    rows = tbl.num_rows
    log(f"assembled soc_priced_skilled: {rows:,} rows x {tbl.num_columns} cols")

    # -- FAIL-CLOSED gate — structural 1:1 + expected-band before any publish --------------
    soc_vals = tbl.column("soc_code").to_pylist()
    n_null = sum(1 for v in soc_vals if v is None)
    n_distinct = len({v for v in soc_vals if v is not None})
    if n_null:
        raise RuntimeError(f"GATE FAIL: {n_null} NULL soc_code rows (spine must be fully keyed)")
    if rows != n_distinct:
        raise RuntimeError(
            f"GATE FAIL: grain not 1:1 — rows={rows} != distinct soc_code={n_distinct}")
    if not (ROW_BAND[0] <= rows <= ROW_BAND[1]):
        raise RuntimeError(
            f"GATE FAIL: row count {rows} outside expected band {ROW_BAND} "
            f"(OEWS detailed spine expected ~{EXPECTED_ROWS})")
    log(f"GATE PASS: rows={rows:,} distinct_soc={n_distinct:,} band={ROW_BAND} null_soc=0")

    lance.write_dataset(tbl, TARGET_URI, mode="overwrite",
                        data_storage_version=DATA_STORAGE_VERSION, storage_options=so)
    log(f"wrote Lance -> {TARGET_URI}")
    built = _build_indexes(TARGET_URI, BTREE, BITMAP, so)

    ds = lance.dataset(TARGET_URI, storage_options=so)
    written = ds.count_rows()
    if written != rows:
        raise RuntimeError(f"POST-WRITE FAIL: lance {written} != assembled {rows}")

    onet_matched = ds.count_rows(filter="onet_title IS NOT NULL")
    ep_matched = ds.count_rows(filter="emp_pct_change_2024_2034 IS NOT NULL")
    results = {
        "uri": TARGET_URI,
        "rows": written,
        "distinct_soc_code": n_distinct,
        "columns": tbl.schema.names,
        "onet_matched": onet_matched,
        "ep_outlook_matched": ep_matched,
        "indexes": built,
        "source": SOURCE,
        "soc_vintage": SOC_VINTAGE,
    }
    print(json.dumps(results, indent=2, default=str))
    return results


def verify() -> dict:
    import lance

    so = _storage_options()
    ds = lance.dataset(TARGET_URI, storage_options=so)
    rows = ds.count_rows()
    soc_vals = ds.scanner(columns=["soc_code"]).to_table().column("soc_code").to_pylist()
    distinct_soc = len({v for v in soc_vals if v is not None})
    idx = [i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i))
           for i in ds.list_indices()]

    # Wage spot-check: top-paid national SOCs (a_median floor values). The hard assertion pins to
    # SPOT_SOC (29-1243) to catch a silent OEWS cleaning regression.
    spot = ds.scanner(
        columns=["soc_code", "soc_title", "onet_title", "tot_emp", "a_median",
                 "emp_pct_change_2024_2034"],
        filter="soc_code IN ('29-1243', '29-1212', '29-1224', '15-1252')",
    ).to_table().to_pylist()
    spot_median = next(
        (r["a_median"] for r in spot if r["soc_code"] == SPOT_SOC), None)

    out = {
        "uri": TARGET_URI,
        "rows": rows,
        "distinct_soc_code": distinct_soc,
        "grain_1to1": rows == distinct_soc,
        "columns": ds.schema.names,
        "indices": idx,
        "onet_matched": ds.count_rows(filter="onet_title IS NOT NULL"),
        "ep_outlook_matched": ds.count_rows(filter="emp_pct_change_2024_2034 IS NOT NULL"),
        "a_median_castable": ds.count_rows(filter="a_median IS NOT NULL"),
        "spot_check": spot,
        f"{SPOT_SOC}_a_median": spot_median,
    }
    print(json.dumps(out, indent=2, default=str))

    # -- FAIL-LOUD verify — grain, index presence, wage regression all non-zero-exit --------
    errors: list[str] = []
    if rows != distinct_soc:
        errors.append(f"grain broken: rows={rows} != distinct_soc={distinct_soc}")
    if not any(str(i).endswith("soc_code") or i == "soc_code" for i in idx):
        errors.append(f"BTREE soc_code index absent — indices={idx}")
    if spot_median != SPOT_A_MEDIAN:
        errors.append(
            f"wage regression: {SPOT_SOC} a_median={spot_median} != expected {SPOT_A_MEDIAN}")
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
