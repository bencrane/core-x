"""Phase 5 — view_county_wage_arbitrage_benchmark: NAF vs SCA vs OEWS wages at the county_fips grain.

The one queryable product surface behind the wage-arbitrage GTM motion. For each Census county it
aligns three independent wage regimes on the shared 5-digit FIPS key:
  NAF   the DoD Nonappropriated-Fund priced grade x step schedule (naf_wage_rates), taken from the
        CHRONOLOGICALLY LATEST CT-regular schedule for the county's wage area (via effective_date_parsed).
  SCA   the Service Contract Act canonical wage determination for the county (sca_wd_county_rollup ->
        sca_wd_rates), summarized to its occupation-rate band.
  OEWS  the BLS state market wage (soc_state_wage): the state All-Occupations median + a blue-collar
        trades band (SOC 47/49/51 families, the market analog to NAF Crafts & Trades).

GRAIN     1 row per county_fips (canonical NAF wage area picked deterministically per county).
          FAIL-CLOSED: grain 1:1; NAF band coverage above the gate; Cobb County GA (13067) spot-check.

TABLE     s3://data-sink/active/view_county_wage_arbitrage_benchmark/
          BTREE(county_fips, naf_wage_area) BITMAP(state, has_sca, has_oews)

SOURCE    Derived reference — reads only Lance SoR under active/. Idempotent (mode='overwrite').
          DEPENDS ON pipelines.naf.normalize_dates having added effective_date_parsed to naf_wage_rates.

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'duckdb>=1.1' --with 'pylance>=7' --with 'pyarrow>=17' --with 'boto3>=1.34' \
      python3 -m pipelines.reference.materialize_view_county_wage_arbitrage_benchmark build
    doppler run -p core-x -c prd -- uv run --no-project ... verify
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys

from pipelines.bls.ingest import _build_indexes, _storage_options

ACTIVE = "s3://data-sink/active"
TARGET_URI = os.environ.get("NAF_ARBITRAGE_VIEW_URI", f"{ACTIVE}/view_county_wage_arbitrage_benchmark/")
DATA_STORAGE_VERSION = "2.1"
SOURCE = "naf_wage_rates+naf_wage_area_county_fips+sca_wd_county_rollup+sca_wd_rates+soc_state_wage"

BTREE = ["county_fips", "naf_wage_area"]
BITMAP = ["state", "has_sca", "has_oews"]
MIN_NAF_FRAC = 0.85  # counties (from the NAF crosswalk) that must carry a NAF rate band

SOURCES = {
    "naf_xw": f"{ACTIVE}/naf_wage_area_county_fips",
    "naf_rates": f"{ACTIVE}/naf_wage_rates",
    "sca_roll": f"{ACTIVE}/sca_wd_county_rollup",
    "sca_rates": f"{ACTIVE}/sca_wd_rates",
    "soc_wage": f"{ACTIVE}/soc_state_wage",
}

os.environ.setdefault("LANCE_BYPASS_SPILLING", "true")


def log(m):
    print(f"[{dt.datetime.now(dt.timezone.utc).isoformat()}] {m}", flush=True)


def _build_sql() -> str:
    return """
    WITH cty AS (
        SELECT county_fips, state, wage_area, naf_area
        FROM (
            SELECT county_fips, state, wage_area, naf_area,
                   row_number() OVER (PARTITION BY county_fips ORDER BY wage_area) AS rn
            FROM naf_xw WHERE scope = 'county' AND county_fips IS NOT NULL
        ) WHERE rn = 1
    ),
    naf_latest AS (
        SELECT wage_area, max(effective_date_parsed) AS latest_eff
        FROM naf_rates
        WHERE schedule_family = 'CT' AND rate_type = 'regular' AND effective_date_parsed IS NOT NULL
        GROUP BY wage_area
    ),
    naf_rows AS (
        SELECT r.wage_area, r.series, r.grade, r.hourly_rate, r.schedule_number, r.effective_date_parsed
        FROM naf_rates r JOIN naf_latest l
          ON r.wage_area = l.wage_area AND r.effective_date_parsed = l.latest_eff
        WHERE r.schedule_family = 'CT' AND r.rate_type = 'regular'
    ),
    naf_bands AS (
        SELECT wage_area,
            max(schedule_number) AS naf_schedule_number,
            max(effective_date_parsed) AS naf_effective_date,
            min(hourly_rate) FILTER (WHERE series='NA') AS naf_na_min, max(hourly_rate) FILTER (WHERE series='NA') AS naf_na_max,
            min(hourly_rate) FILTER (WHERE series='NL') AS naf_nl_min, max(hourly_rate) FILTER (WHERE series='NL') AS naf_nl_max,
            min(hourly_rate) FILTER (WHERE series='NS') AS naf_ns_min, max(hourly_rate) FILTER (WHERE series='NS') AS naf_ns_max,
            min(hourly_rate) FILTER (WHERE series='NA' AND grade=5)  AS naf_na5_min,  max(hourly_rate) FILTER (WHERE series='NA' AND grade=5)  AS naf_na5_max,
            min(hourly_rate) FILTER (WHERE series='NA' AND grade=10) AS naf_na10_min, max(hourly_rate) FILTER (WHERE series='NA' AND grade=10) AS naf_na10_max
        FROM naf_rows GROUP BY wage_area
    ),
    sca_bands AS (
        SELECT c.county_fips, c.canonical_wd_id AS sca_canonical_wd_id,
            count(*) AS sca_occ_count,
            min(r.hourly_wage) AS sca_min_hourly, max(r.hourly_wage) AS sca_max_hourly,
            median(r.hourly_wage) AS sca_median_hourly
        FROM sca_roll c JOIN sca_rates r ON r.wd_id = c.canonical_wd_id
        WHERE r.hourly_wage IS NOT NULL
        GROUP BY c.county_fips, c.canonical_wd_id
    ),
    oews AS (
        -- Prefer the true OEWS All-Occupations row (SOC 00-0000); fall back to the median of per-SOC
        -- medians where a state's file omits it, so the overall-market column is always populated.
        SELECT prim_state,
            coalesce(max(h_median) FILTER (WHERE soc_code='00-0000'), median(h_median)) AS oews_all_occ_h_median,
            coalesce(max(h_pct25)  FILTER (WHERE soc_code='00-0000'), median(h_pct25))  AS oews_all_occ_h_p25,
            coalesce(max(h_pct75)  FILTER (WHERE soc_code='00-0000'), median(h_pct75))  AS oews_all_occ_h_p75,
            median(h_median) FILTER (WHERE soc_code LIKE '47-%' OR soc_code LIKE '49-%' OR soc_code LIKE '51-%') AS oews_trades_h_median
        FROM soc_wage GROUP BY prim_state
    )
    SELECT
        cty.county_fips, cty.state, cty.wage_area AS naf_wage_area, cty.naf_area,
        nb.naf_schedule_number, nb.naf_effective_date,
        nb.naf_na_min, nb.naf_na_max, nb.naf_nl_min, nb.naf_nl_max, nb.naf_ns_min, nb.naf_ns_max,
        nb.naf_na5_min, nb.naf_na5_max, nb.naf_na10_min, nb.naf_na10_max,
        sb.sca_canonical_wd_id, sb.sca_occ_count, sb.sca_min_hourly, sb.sca_max_hourly, sb.sca_median_hourly,
        o.oews_all_occ_h_median, o.oews_all_occ_h_p25, o.oews_all_occ_h_p75, o.oews_trades_h_median,
        (nb.wage_area IS NOT NULL) AS has_naf,
        (sb.county_fips IS NOT NULL) AS has_sca,
        (o.prim_state IS NOT NULL) AS has_oews,
        '""" + SOURCE + """' AS source, now() AS ingested_at
    FROM cty
    LEFT JOIN naf_bands nb ON nb.wage_area = cty.wage_area
    LEFT JOIN sca_bands sb ON sb.county_fips = cty.county_fips
    LEFT JOIN oews o ON o.prim_state = cty.state
    ORDER BY cty.county_fips
    """


def build() -> dict:
    import duckdb
    import lance

    so = _storage_options()
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=8;")
    for alias, uri in SOURCES.items():
        con.register(alias, lance.dataset(uri, storage_options=so))
    log(f"registered {len(SOURCES)} Lance scanners")

    tbl = con.execute(_build_sql()).to_arrow_table()
    rows = tbl.num_rows
    cfips = tbl.column("county_fips").to_pylist()
    has_naf = tbl.column("has_naf").to_pylist()
    has_sca = tbl.column("has_sca").to_pylist()
    has_oews = tbl.column("has_oews").to_pylist()

    distinct = len(set(cfips))
    if rows != distinct:
        con.close()
        raise RuntimeError(f"GATE FAIL: grain not 1:1 — rows={rows} != distinct county_fips={distinct}")
    naf_frac = sum(has_naf) / rows if rows else 0
    if naf_frac < MIN_NAF_FRAC:
        con.close()
        raise RuntimeError(f"GATE FAIL: NAF band coverage {sum(has_naf)}/{rows} ({naf_frac:.3%}) < {MIN_NAF_FRAC:.0%}")

    # Cobb County GA (13067) spot-check — chronologically latest CT schedule + aligned SCA/OEWS.
    cobb = con.execute("""
        SELECT county_fips, naf_wage_area, naf_schedule_number, naf_effective_date,
               naf_na5_min, naf_na5_max, naf_na10_min, naf_na10_max,
               sca_canonical_wd_id, sca_median_hourly, oews_all_occ_h_median, oews_trades_h_median
        FROM (""" + _build_sql() + """) WHERE county_fips = '13067'
    """).fetchall()
    con.close()

    lance.write_dataset(tbl, TARGET_URI, mode="overwrite",
                        data_storage_version=DATA_STORAGE_VERSION, storage_options=so)
    built = _build_indexes(TARGET_URI, BTREE, BITMAP, so)
    written = lance.dataset(TARGET_URI, storage_options=so).count_rows()
    if written != rows:
        raise RuntimeError(f"POST-WRITE FAIL: lance {written} != {rows}")

    results = {
        "uri": TARGET_URI, "rows": written, "distinct_county_fips": distinct,
        "columns": tbl.schema.names,
        "has_naf": f"{sum(has_naf)}/{rows} ({naf_frac:.1%})",
        "has_sca": f"{sum(has_sca)}/{rows} ({sum(has_sca)/rows:.1%})",
        "has_oews": f"{sum(has_oews)}/{rows} ({sum(has_oews)/rows:.1%})",
        "triple_covered": sum(1 for a, b, c in zip(has_naf, has_sca, has_oews) if a and b and c),
        "cobb_13067": cobb,
        "indexes": built, "source": SOURCE,
    }
    print(json.dumps(results, indent=2, default=str))
    return results


def verify() -> dict:
    import lance

    so = _storage_options()
    ds = lance.dataset(TARGET_URI, storage_options=so)
    rows = ds.count_rows()
    cobb = ds.scanner(filter="county_fips = '13067'").to_table().to_pylist()
    out = {
        "uri": TARGET_URI, "rows": rows,
        "has_naf": ds.count_rows(filter="has_naf = true"),
        "has_sca": ds.count_rows(filter="has_sca = true"),
        "has_oews": ds.count_rows(filter="has_oews = true"),
        "cobb_13067": cobb,
    }
    print(json.dumps(out, indent=2, default=str))
    if not cobb:
        raise RuntimeError("VERIFY FAIL: Cobb County (13067) absent from arbitrage view")
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
