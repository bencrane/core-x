"""L1 rollup — sca_wd_county_rollup: one active-SCA-coverage row per 5-digit Census county.

The county-grain answer to "which SCA wage determinations cover this county, and which one is
canonical?" It rolls the SAM.gov county→WD coverage feed up to the Census FIPS grain, keeping only
counties that carry at least one ACTIVE Service Contract Act (SCA) wage determination.

  coverage      sam_wd_county_coverage       33,156 (wd_id, state_code[USPS], county_name[bare;
                                             independent cities carry a trailing '*'/' city']).
  determination sam_wage_determinations      10,055 (_id, revision_number, type_code). SCA = 1521.
                                             Joined coverage.wd_id = wage_determinations._id, kept
                                             to type_code='SCA' AND is_active.
  fips resolver sam_wd_county_fips_xwalk      the operator-built crosswalk that resolves each
                                             (state_code, county_name) coverage pair to a 5-digit
                                             Census county_fips (scope='county'), a statewide marker
                                             (scope='statewide', county_fips NULL — expanded here),
                                             or an unmapped tail (scope='unmapped', dropped + counted).
  county spine  national_county2020           the gazetteer authority — used to fan statewide
                                             coverage out to every real county of a state
                                             (state_usps = state_code AND class_fp LIKE 'H%').

GRAIN     1 row per 5-digit county_fips. Only counties with >=1 active SCA WD survive. County-scope
          coverage rows land on their resolved county_fips; statewide coverage rows fan out to ALL of
          that state's county-equivalent FIPS (national_county2020 class_fp 'H%'); unmapped rows are
          dropped but COUNTED for the report. FAIL-CLOSED: count(*) == count(distinct county_fips),
          has_sca_coverage all-true, canonical_wd_id non-null, and the row count is gated to a sane
          band (1500..3300) — a violation raises, no write.

TABLE     s3://data-sink/active/sca_wd_county_rollup/    BTREE: county_fips
          county_fips, active_sca_wd_ids (list<string>), wd_count (int), canonical_wd_id (string),
          has_sca_coverage (bool, always true), source, ingested_at.

CANONICAL The canonical WD per county is the highest revision_number among that county's active SCA
          WDs; ties break on the lexicographically max _id. active_sca_wd_ids is the DISTINCT set of
          wd_ids covering the county (post statewide-expansion), wd_count its cardinality.

SOURCE    Derived L1 — reads only Lance SoR datasets under s3://data-sink/active/. Depends on
          sam_wd_county_fips_xwalk existing (the operator builds that crosswalk FIRST). No landing
          fetch, no external HTTP. Idempotent (mode='overwrite'): re-run after any SAM coverage /
          WD / crosswalk refresh. Reuses pipelines.bls.ingest R2 plumbing (_storage_options) and the
          shared Lance index builder (_build_indexes) — no re-implementation.

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'duckdb>=1.1' --with 'pylance>=7' --with 'pyarrow>=17' --with 'boto3>=1.34' \
      python3 -m pipelines.reference.materialize_sca_wd_county_rollup build

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'duckdb>=1.1' --with 'pylance>=7' --with 'pyarrow>=17' --with 'boto3>=1.34' \
      python3 -m pipelines.reference.materialize_sca_wd_county_rollup verify
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
COVERAGE_URI = os.environ.get("SAM_WD_COUNTY_COVERAGE_URI", f"{ACTIVE}/sam_wd_county_coverage")
WD_URI = os.environ.get("SAM_WAGE_DETERMINATIONS_URI", f"{ACTIVE}/sam_wage_determinations")
XWALK_URI = os.environ.get("SAM_WD_COUNTY_FIPS_XWALK_URI", f"{ACTIVE}/sam_wd_county_fips_xwalk")
COUNTY_URI = os.environ.get("NATIONAL_COUNTY2020_URI", f"{ACTIVE}/national_county2020")

# Output rollup.
TARGET_URI = os.environ.get("SCA_WD_COUNTY_ROLLUP_URI", f"{ACTIVE}/sca_wd_county_rollup/")

DATA_STORAGE_VERSION = "2.1"       # net-new Lance dataset -> pin current default storage version
SOURCE = "SAM_WD_COUNTY_COVERAGE+SCA_2026-07"

BTREE = ["county_fips"]
BITMAP: list[str] = []

# Expected 1-row-per-county band. Universe ceiling is 3,235 county-equivalents; only counties that
# carry an active SCA WD survive, so the published count sits well inside this band.
ROW_BAND = (1500, 3300)

# verify() spot-check — a DC-metro county that must carry active SCA coverage. Arlington County, VA
# (51013) is a canonical inside-the-Beltway county-equivalent; used as a coverage tripwire.
SPOT_COUNTY_FIPS = "51013"

os.environ.setdefault("LANCE_BYPASS_SPILLING", "true")


def log(m):
    print(f"[{dt.datetime.now(dt.timezone.utc).isoformat()}] {m}", flush=True)


def _index_fields(ds) -> set[str]:
    """Flatten the columns each Lance scalar index covers, robust to Lance's naming.

    Lance names a BTREE index '<col>_idx' (7.x), so matching on the display name is a false-negative
    trap. list_indices() carries the covered columns under 'fields' (a list of column names) — that
    is the load-bearing signal. Fall back to parsing the name only if 'fields' is absent."""
    cols: set[str] = set()
    for i in ds.list_indices():
        flds = i.get("fields") if isinstance(i, dict) else getattr(i, "fields", None)
        if flds:
            cols.update(str(f) for f in flds)
            continue
        nm = i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i))
        nm = str(nm)
        cols.add(nm[:-4] if nm.endswith("_idx") else nm)
    return cols


def _build_sql() -> str:
    """Single DuckDB statement: active-SCA coverage ⋈ crosswalk, statewide-expanded against the
    county spine, rolled up to one row per 5-digit county_fips with a canonical WD pick."""
    return f"""
    WITH active_sca AS (
        -- coverage rows whose WD is an ACTIVE Service Contract Act determination
        SELECT
            cov.state_code   AS state_code,
            cov.county_name  AS county_name,
            wd._id           AS wd_id,
            wd.revision_number AS revision_number
        FROM coverage_src cov
        JOIN wd_src wd
          ON wd._id = cov.wd_id
         AND wd.type_code = 'SCA'
         AND wd.is_active
    ),
    -- resolve each coverage pair to a scope + county_fips via the operator-built crosswalk
    resolved AS (
        SELECT
            a.wd_id,
            a.revision_number,
            x.scope       AS scope,
            x.county_fips AS county_fips,
            a.state_code  AS state_code
        FROM active_sca a
        LEFT JOIN xwalk_src x
          ON x.state_code = a.state_code
         AND x.county_name = a.county_name
    ),
    -- county-scope: land directly on the resolved 5-digit FIPS
    county_scope AS (
        SELECT county_fips, wd_id, revision_number
        FROM resolved
        WHERE scope = 'county' AND county_fips IS NOT NULL
    ),
    -- statewide-scope: fan the WD out to every county-equivalent FIPS of its state
    statewide_scope AS (
        SELECT c.county_fips, r.wd_id, r.revision_number
        FROM resolved r
        JOIN county_spine_src c
          ON c.state_usps = r.state_code
         AND c.class_fp LIKE 'H%'
        WHERE r.scope = 'statewide'
    ),
    -- union of every (county_fips, wd_id) pairing that survives resolution
    exploded AS (
        SELECT county_fips, wd_id, revision_number FROM county_scope
        UNION ALL
        SELECT county_fips, wd_id, revision_number FROM statewide_scope
    ),
    -- distinct WD per county (a WD can arrive via both direct + statewide paths)
    per_county_wd AS (
        SELECT
            county_fips,
            wd_id,
            MAX(revision_number) AS revision_number
        FROM exploded
        GROUP BY county_fips, wd_id
    ),
    -- canonical pick: highest revision_number, tie-break lexicographically max _id
    canonical AS (
        SELECT county_fips, wd_id AS canonical_wd_id
        FROM (
            SELECT
                county_fips,
                wd_id,
                ROW_NUMBER() OVER (
                    PARTITION BY county_fips
                    ORDER BY revision_number DESC, wd_id DESC
                ) AS rn
            FROM per_county_wd
        )
        WHERE rn = 1
    )
    SELECT
        p.county_fips,
        list_sort(list(DISTINCT p.wd_id))          AS active_sca_wd_ids,
        CAST(COUNT(DISTINCT p.wd_id) AS INTEGER)   AS wd_count,
        ANY_VALUE(c.canonical_wd_id)               AS canonical_wd_id,
        TRUE                                       AS has_sca_coverage,
        '{SOURCE}' AS source,
        now() AS ingested_at
    FROM per_county_wd p
    JOIN canonical c ON c.county_fips = p.county_fips
    GROUP BY p.county_fips
    ORDER BY p.county_fips
    """


def _count_unmapped_sql() -> str:
    """Report-only: distinct active-SCA coverage pairs that resolve to unmapped (dropped)."""
    return """
    WITH active_sca AS (
        SELECT DISTINCT cov.state_code AS state_code, cov.county_name AS county_name
        FROM coverage_src cov
        JOIN wd_src wd
          ON wd._id = cov.wd_id
         AND wd.type_code = 'SCA'
         AND wd.is_active
    )
    SELECT COUNT(*) AS unmapped_pairs
    FROM active_sca a
    LEFT JOIN xwalk_src x
      ON x.state_code = a.state_code
     AND x.county_name = a.county_name
    WHERE x.scope IS NULL
       OR x.scope = 'unmapped'
       OR (x.scope = 'county' AND x.county_fips IS NULL)
    """


def _register(con, so) -> None:
    import lance

    # Register the Lance dataset OBJECTS (not one-shot .to_reader() streams — those drain on the
    # first scan and any re-scan yields 0 rows). DuckDB scans the Lance dataset directly and pushes
    # the projection+filter down, so only the projected/filtered rows are pulled through the worker.
    con.register("coverage_src", lance.dataset(COVERAGE_URI, storage_options=so))
    con.register("wd_src", lance.dataset(WD_URI, storage_options=so))
    con.register("xwalk_src", lance.dataset(XWALK_URI, storage_options=so))
    con.register("county_spine_src", lance.dataset(COUNTY_URI, storage_options=so))
    log("registered coverage + wage_determinations + fips_xwalk + county_spine Lance scanners")


def build() -> dict:
    import duckdb
    import lance

    so = _storage_options()
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=8;")
    _register(con, so)

    # unmapped tail (report-only) — computed before the main assembly so a crosswalk gap surfaces
    unmapped_pairs = con.execute(_count_unmapped_sql()).fetchone()[0]
    log(f"unmapped active-SCA coverage pairs (dropped): {unmapped_pairs:,}")

    # .to_arrow_table() returns a materialized pyarrow.Table (has .num_rows/.column()); the older
    # .arrow() returns a one-shot RecordBatchReader that the fail-closed gate below cannot index.
    tbl = con.execute(_build_sql()).to_arrow_table()
    con.close()
    rows = tbl.num_rows
    log(f"assembled sca_wd_county_rollup: {rows:,} rows x {tbl.num_columns} cols")

    # -- FAIL-CLOSED gate — structural 1:1 + all-covered + canonical + band before any publish ----
    fips_vals = tbl.column("county_fips").to_pylist()
    n_null = sum(1 for v in fips_vals if v is None)
    n_distinct = len({v for v in fips_vals if v is not None})
    if n_null:
        raise RuntimeError(f"GATE FAIL: {n_null} NULL county_fips rows (grain must be fully keyed)")
    if rows != n_distinct:
        raise RuntimeError(
            f"GATE FAIL: grain not 1:1 — rows={rows} != distinct county_fips={n_distinct}")
    if rows == 0:
        raise RuntimeError("GATE FAIL: zero rows — coverage/xwalk join produced nothing")
    cov_vals = tbl.column("has_sca_coverage").to_pylist()
    if not all(v is True for v in cov_vals):
        n_bad = sum(1 for v in cov_vals if v is not True)
        raise RuntimeError(f"GATE FAIL: {n_bad} rows with has_sca_coverage != true")
    canon_vals = tbl.column("canonical_wd_id").to_pylist()
    n_canon_null = sum(1 for v in canon_vals if v is None)
    if n_canon_null:
        raise RuntimeError(f"GATE FAIL: {n_canon_null} rows with NULL canonical_wd_id")
    if not (ROW_BAND[0] <= rows <= ROW_BAND[1]):
        raise RuntimeError(
            f"GATE FAIL: row count {rows} outside expected band {ROW_BAND}")
    log(f"GATE PASS: rows={rows:,} distinct_fips={n_distinct:,} band={ROW_BAND} "
        f"null_fips=0 canonical_null=0 all_covered=true")

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
        "distinct_county_fips": n_distinct,
        "columns": tbl.schema.names,
        "unmapped_pairs_dropped": unmapped_pairs,
        "indexes": built,
        "source": SOURCE,
    }
    print(json.dumps(results, indent=2, default=str))
    return results


def verify() -> dict:
    import duckdb
    import lance

    so = _storage_options()
    ds = lance.dataset(TARGET_URI, storage_options=so)
    rows = ds.count_rows()
    fips_vals = ds.scanner(columns=["county_fips"]).to_table().column("county_fips").to_pylist()
    distinct_fips = len({v for v in fips_vals if v is not None})
    idx_cols = _index_fields(ds)

    all_covered = ds.count_rows(filter="has_sca_coverage = true") == rows
    n_canon_null = ds.count_rows(filter="canonical_wd_id IS NULL")

    # coverage tripwire — a DC-metro county must carry >=1 active SCA WD with a canonical pick
    spot = ds.scanner(
        columns=["county_fips", "wd_count", "canonical_wd_id", "has_sca_coverage"],
        filter=f"county_fips = '{SPOT_COUNTY_FIPS}'",
    ).to_table().to_pylist()

    # recompute the unmapped-dropped count live from the SoR for the report
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=8;")
    _register(con, so)
    unmapped_pairs = con.execute(_count_unmapped_sql()).fetchone()[0]
    con.close()

    out = {
        "uri": TARGET_URI,
        "rows": rows,
        "distinct_county_fips": distinct_fips,
        "grain_1to1": rows == distinct_fips,
        "columns": ds.schema.names,
        "index_cols": sorted(idx_cols),
        "all_has_sca_coverage_true": all_covered,
        "canonical_wd_id_null_count": n_canon_null,
        "unmapped_pairs_dropped": unmapped_pairs,
        "spot_check": spot,
        "spot_county_fips": SPOT_COUNTY_FIPS,
    }
    print(json.dumps(out, indent=2, default=str))

    # -- FAIL-LOUD verify — grain, index presence, coverage invariants all non-zero-exit ----------
    errors: list[str] = []
    if rows != distinct_fips:
        errors.append(f"grain broken: rows={rows} != distinct_fips={distinct_fips}")
    if "county_fips" not in idx_cols:
        errors.append(f"BTREE county_fips index absent — index_cols={sorted(idx_cols)}")
    if not (ROW_BAND[0] <= rows <= ROW_BAND[1]):
        errors.append(f"row count {rows} outside expected band {ROW_BAND}")
    if not all_covered:
        errors.append("has_sca_coverage not universally true")
    if n_canon_null:
        errors.append(f"{n_canon_null} rows with NULL canonical_wd_id")
    if not spot:
        errors.append(f"spot county {SPOT_COUNTY_FIPS} absent — expected active SCA coverage")
    elif not spot[0].get("canonical_wd_id"):
        errors.append(f"spot county {SPOT_COUNTY_FIPS} has NULL canonical_wd_id")
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
