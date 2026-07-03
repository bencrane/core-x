"""Reference crosswalk — naf_wage_area_county_fips: NAF wage-area county coverage → Census FIPS.

Resolves each distinct (wage_area, naf_area, state, county) named in naf_wage_area_geography (the
RSB "Schedule Back" county definitions) to a canonical 5-digit county_fips off national_county2020 —
the SAME gazetteer authority and normalization the SCA crosswalk uses — so NAF wage areas land on
real county geography and compose with the FPDS spine (usaspending_fpds_canonical_txn.pop_county_fips
== county_fips) and sca_wd_county_rollup / sca_wd_rates / soc_state_wage.

  input     naf_wage_area_geography  row_kind='county' rows: wage_area, naf_area, state (2-letter,
                                     NULL for stateless Pacific/territory rows), county (MIXED: bare
                                     'Lowndes' / suffixed 'Lowndes County' / independent-city
                                     'Alexandria City').
  authority national_county2020      3,235 rows; county_fips, state_usps, county_name (suffixed),
                                     class_fp (H% county-equivalent | C% independent city).

GRAIN     1 row per distinct (wage_area, naf_area, state, county). FAIL-CLOSED: grain 1:1;
          every scope='county' row carries a non-null county_fips; >=90% of STATED (non-territory)
          identities resolve to FIPS. Territory rows (state NULL) route to scope='unmapped' — there
          is no state to disambiguate the county name against, so they are NOT guessed.

RESOLUTION  Reuses the SCA xwalk's LOCKED name normalization (_nz) and its independent-city collision
            rule verbatim (imported, not re-implemented): _nz-normalize both the NAF county label and
            the Census county_name, join on (state, normalized). A single hit -> method='exact'. A
            multi-hit is the independent-city collision (an H-class county and a C-class city share a
            normalized name in VA/MD/MO): a city-flagged NAF label (' City'/' city' or trailing '*')
            takes the C-class row (collision_city), else the H-class row (collision_county).
            ASYMMETRIC-COLLISION SAFETY (inherited): a city-flagged label with no C-class row is NOT
            mapped to the H county, and vice-versa — the mismatch routes to unmapped.

TABLE     s3://data-sink/active/naf_wage_area_county_fips/   BTREE: county_fips, wage_area, naf_area
          wage_area, naf_area, state, county, county_fips (nullable), county_name_census (nullable),
          class_fp (nullable), scope ('county'|'unmapped'),
          match_method ('exact'|'collision_city'|'collision_county'|'unmapped'), source, ingested_at.

SOURCE    Derived reference — reads only Lance SoR under s3://data-sink/active/. No landing fetch, no
          external HTTP. Idempotent (mode='overwrite'). Reuses pipelines.bls.ingest R2 plumbing +
          the shared index builder, and the SCA xwalk's _nz / ambiguity gate / index introspection.

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'duckdb>=1.1' --with 'pylance>=7' --with 'pyarrow>=17' --with 'boto3>=1.34' \
      python3 -m pipelines.reference.materialize_naf_wage_area_county_fips build
    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'duckdb>=1.1' --with 'pylance>=7' --with 'pyarrow>=17' --with 'boto3>=1.34' \
      python3 -m pipelines.reference.materialize_naf_wage_area_county_fips verify
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys

from pipelines.bls.ingest import _build_indexes, _storage_options
# Reuse the SCA xwalk's locked normalization + ambiguity gate + index introspection — do NOT re-implement.
from pipelines.reference.materialize_sam_wd_county_fips_xwalk import (
    _ambiguity_probe_sql,
    _index_fields,
    _nz,
)

ACTIVE = "s3://data-sink/active"
NAF_GEOGRAPHY_URI = os.environ.get("NAF_WAGE_AREA_GEOGRAPHY_URI", f"{ACTIVE}/naf_wage_area_geography")
NATIONAL_COUNTY_URI = os.environ.get("NATIONAL_COUNTY2020_URI", f"{ACTIVE}/national_county2020")
TARGET_URI = os.environ.get("NAF_WAGE_AREA_COUNTY_FIPS_URI", f"{ACTIVE}/naf_wage_area_county_fips/")

DATA_STORAGE_VERSION = "2.1"
SOURCE = "naf_wage_area_geography+national_county2020"

BTREE = ["county_fips", "wage_area", "naf_area"]
BITMAP = ["scope", "match_method"]

MIN_RESOLVED_FRAC = 0.90  # of STATED (state IS NOT NULL) identities

# Hard (state, county-literal) -> county_fips overrides for names that mis-normalize. The shared
# _nz() strips a trailing 'island' as a county-equivalent suffix, which breaks 'Rock Island' /
# 'Kodiak Island'; 'Columbus' GA is the consolidated Columbus-Muscogee city-county. Matched
# case-sensitive against the raw NAF county label, pre-normalization.
NAF_ALIAS: dict[tuple[str, str], str] = {
    ("GA", "Columbus"): "13215",      # Muscogee County (consolidated)
    ("IL", "Rock Island"): "17161",   # Rock Island County
    ("AK", "Kodiak Island"): "02150",  # Kodiak Island Borough
}

os.environ.setdefault("LANCE_BYPASS_SPILLING", "true")


def _alias_values_sql() -> str:
    if not NAF_ALIAS:
        return "(SELECT NULL::VARCHAR state_code, NULL::VARCHAR county_name, NULL::VARCHAR county_fips WHERE 1=0)"
    rows = ",\n            ".join(
        f"('{st}', '{nm.replace(chr(39), chr(39) * 2)}', '{fips}')" for (st, nm), fips in NAF_ALIAS.items())
    return f"(VALUES\n            {rows}\n        ) AS a(state_code, county_name, county_fips)"


def log(m):
    print(f"[{dt.datetime.now(dt.timezone.utc).isoformat()}] {m}", flush=True)


def _build_sql() -> str:
    """Resolve each distinct NAF (wage_area, naf_area, state, county) to a Census county_fips.

    census_h / census_c hold the per-(state, nz_name) FIPS for the H-class county-equivalent and the
    C-class independent city; the collision picks between them by the NAF is_city flag with no
    cross-class fallback (a class mismatch stays NULL -> unmapped). Territory rows (state NULL) never
    match a (state_usps, nz_name) key and correctly route to unmapped."""
    return f"""
    WITH naf AS (
        SELECT DISTINCT wage_area, naf_area, state, county
        FROM naf_src
        WHERE row_kind = 'county' AND county IS NOT NULL AND trim(county) <> ''
    ),
    naf_flagged AS (
        SELECT
            wage_area, naf_area, state, county,
            (rtrim(trim(county)) LIKE '%*' OR lower(rtrim(trim(county))) LIKE '% city') AS is_city,
            {_nz('county')} AS nz_name
        FROM naf
    ),
    alias AS (SELECT state_code, county_name, county_fips FROM {_alias_values_sql()}),
    census_h AS (
        SELECT state_usps, {_nz('county_name')} AS nz_name,
               any_value(county_fips) AS county_fips,
               any_value(county_name) AS county_name_census,
               any_value(class_fp)    AS class_fp
        FROM county_src WHERE class_fp LIKE 'H%'
        GROUP BY state_usps, {_nz('county_name')}
    ),
    census_c AS (
        SELECT state_usps, {_nz('county_name')} AS nz_name,
               any_value(county_fips) AS county_fips,
               any_value(county_name) AS county_name_census,
               any_value(class_fp)    AS class_fp
        FROM county_src WHERE class_fp LIKE 'C%'
        GROUP BY state_usps, {_nz('county_name')}
    ),
    -- Territory resolver: a state-NULL NAF county name that maps to exactly ONE FIPS across the
    -- non-state territory gazetteer (PR/GU/AS/VI/MP) resolves unambiguously (e.g. PR municipios).
    census_terr AS (
        SELECT nz_name,
               any_value(county_fips) AS county_fips,
               any_value(county_name) AS county_name_census,
               any_value(class_fp)    AS class_fp
        FROM (SELECT DISTINCT state_usps, county_fips, county_name, class_fp, {_nz('county_name')} AS nz_name
              FROM county_src WHERE state_usps IN ('PR','GU','AS','VI','MP'))
        GROUP BY nz_name
        HAVING COUNT(DISTINCT county_fips) = 1
    ),
    census_all AS (
        SELECT county_fips, any_value(county_name) AS county_name_census, any_value(class_fp) AS class_fp
        FROM county_src GROUP BY county_fips
    ),
    resolved AS (
        SELECT
            n.wage_area, n.naf_area, n.state, n.county, n.is_city, n.nz_name,
            al.county_fips AS alias_fips,
            ch.county_fips AS h_fips, ch.county_name_census AS h_name, ch.class_fp AS h_class,
            cc.county_fips AS c_fips, cc.county_name_census AS c_name, cc.class_fp AS c_class,
            ct.county_fips AS t_fips, ct.county_name_census AS t_name, ct.class_fp AS t_class
        FROM naf_flagged n
        LEFT JOIN alias      al ON al.state_code = n.state AND al.county_name = n.county
        LEFT JOIN census_h   ch ON ch.state_usps = n.state AND ch.nz_name = n.nz_name
        LEFT JOIN census_c   cc ON cc.state_usps = n.state AND cc.nz_name = n.nz_name
        LEFT JOIN census_terr ct ON n.state IS NULL AND ct.nz_name = n.nz_name
    ),
    picked AS (
        SELECT
            r.*,
            CASE WHEN r.alias_fips IS NOT NULL THEN r.alias_fips
                 WHEN r.state IS NULL THEN r.t_fips
                 WHEN r.is_city THEN r.c_fips ELSE r.h_fips END AS picked_fips,
            CASE WHEN r.alias_fips IS NOT NULL THEN ca.county_name_census
                 WHEN r.state IS NULL THEN r.t_name
                 WHEN r.is_city THEN r.c_name ELSE r.h_name END AS picked_name,
            CASE WHEN r.alias_fips IS NOT NULL THEN ca.class_fp
                 WHEN r.state IS NULL THEN r.t_class
                 WHEN r.is_city THEN r.c_class ELSE r.h_class END AS picked_class,
            (r.h_fips IS NOT NULL AND r.c_fips IS NOT NULL) AS is_collision
        FROM resolved r
        LEFT JOIN census_all ca ON ca.county_fips = r.alias_fips
    )
    SELECT
        p.wage_area,
        p.naf_area,
        p.state,
        p.county,
        p.picked_fips AS county_fips,
        p.picked_name AS county_name_census,
        p.picked_class AS class_fp,
        CASE WHEN p.picked_fips IS NOT NULL THEN 'county' ELSE 'unmapped' END AS scope,
        CASE
            WHEN p.picked_fips IS NULL THEN 'unmapped'
            WHEN p.alias_fips IS NOT NULL THEN 'alias'
            WHEN p.state IS NULL THEN 'territory'
            WHEN p.is_city AND p.is_collision THEN 'collision_city'
            WHEN (NOT p.is_city) AND p.is_collision THEN 'collision_county'
            ELSE 'exact'
        END AS match_method,
        '{SOURCE}' AS source,
        now() AS ingested_at
    FROM picked p
    ORDER BY p.wage_area, p.naf_area, p.state, p.county
    """


def build() -> dict:
    import duckdb
    import lance

    so = _storage_options()
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=8;")
    con.register("naf_src", lance.dataset(NAF_GEOGRAPHY_URI, storage_options=so))
    con.register("county_src", lance.dataset(NATIONAL_COUNTY_URI, storage_options=so))
    log("registered naf_wage_area_geography + national_county2020 Lance scanners")

    # PRE-BUILD ambiguity gate (reused): no same-class (state, nz_name) may map to >1 FIPS.
    ambig = con.execute(_ambiguity_probe_sql()).fetchall()
    if ambig:
        detail = "; ".join(f"{st}/{cls}/{nz}->[{fips}] (n={n})" for st, cls, nz, n, fips in ambig[:20])
        con.close()
        raise RuntimeError(f"GATE FAIL: {len(ambig)} ambiguous same-class (state,nz_name) group(s). {detail}")
    log("AMBIGUITY GATE PASS: no same-class (state, nz_name) collapses to >1 FIPS")

    tbl = con.execute(_build_sql()).to_arrow_table()
    con.close()
    rows = tbl.num_rows
    log(f"assembled naf_wage_area_county_fips: {rows:,} rows x {tbl.num_columns} cols")

    wa = tbl.column("wage_area").to_pylist()
    na = tbl.column("naf_area").to_pylist()
    stv = tbl.column("state").to_pylist()
    cty = tbl.column("county").to_pylist()
    fips = tbl.column("county_fips").to_pylist()
    scope = tbl.column("scope").to_pylist()

    distinct_keys = {(a, b, c, d) for a, b, c, d in zip(wa, na, stv, cty)}
    if rows != len(distinct_keys):
        raise RuntimeError(f"GATE FAIL: grain not 1:1 — rows={rows} != distinct (wage_area,naf_area,state,county)={len(distinct_keys)}")

    bad_county = sum(1 for sc, fp in zip(scope, fips) if sc == "county" and fp is None)
    if bad_county:
        raise RuntimeError(f"GATE FAIL: {bad_county} rows scope='county' but county_fips IS NULL")

    n_stated = sum(1 for s in stv if s is not None)
    n_stated_resolved = sum(1 for s, fp in zip(stv, fips) if s is not None and fp is not None)
    frac = (n_stated_resolved / n_stated) if n_stated else 1.0
    if frac < MIN_RESOLVED_FRAC:
        raise RuntimeError(f"GATE FAIL: resolved {n_stated_resolved}/{n_stated} stated ({frac:.3%}) < {MIN_RESOLVED_FRAC:.0%}")
    log(f"GATE PASS: rows={rows:,} grain_1to1 county_null_fips=0 stated_resolved={n_stated_resolved}/{n_stated} ({frac:.3%})")

    lance.write_dataset(tbl, TARGET_URI, mode="overwrite",
                        data_storage_version=DATA_STORAGE_VERSION, storage_options=so)
    log(f"wrote Lance -> {TARGET_URI}")
    built = _build_indexes(TARGET_URI, BTREE, BITMAP, so)

    ds = lance.dataset(TARGET_URI, storage_options=so)
    written = ds.count_rows()
    if written != rows:
        raise RuntimeError(f"POST-WRITE FAIL: lance {written} != assembled {rows}")

    results = {
        "uri": TARGET_URI, "rows": written, "distinct_key": len(distinct_keys),
        "columns": tbl.schema.names,
        "distinct_county_fips": len({f for f in fips if f}),
        "distinct_wage_areas": len({a for a in wa}),
        "scope_county": ds.count_rows(filter="scope = 'county'"),
        "scope_unmapped": ds.count_rows(filter="scope = 'unmapped'"),
        "method_exact": ds.count_rows(filter="match_method = 'exact'"),
        "method_alias": ds.count_rows(filter="match_method = 'alias'"),
        "method_territory": ds.count_rows(filter="match_method = 'territory'"),
        "method_collision_city": ds.count_rows(filter="match_method = 'collision_city'"),
        "method_collision_county": ds.count_rows(filter="match_method = 'collision_county'"),
        "stated_resolved": f"{n_stated_resolved}/{n_stated} ({frac:.3%})",
        "territory_rows_state_null": sum(1 for s in stv if s is None),
        "territory_resolved": sum(1 for s, fp in zip(stv, fips) if s is None and fp is not None),
        "indexes": built, "source": SOURCE,
    }
    print(json.dumps(results, indent=2, default=str))
    return results


def verify() -> dict:
    import lance

    so = _storage_options()
    ds = lance.dataset(TARGET_URI, storage_options=so)
    rows = ds.count_rows()
    keys = ds.scanner(columns=["wage_area", "naf_area", "state", "county"]).to_table()
    distinct = len({t for t in zip(*[keys.column(c).to_pylist() for c in ("wage_area", "naf_area", "state", "county")])})
    idx_cols = _index_fields(ds)

    # Spot-check FIPS resolution against live-verified values (matches the geography adversarial anchors).
    spot_expect = {("MS", "Lowndes"): "28087", ("MS", "Lowndes County"): "28087",
                   ("AL", "Tuscaloosa County"): "01125", ("DE", "New Castle County"): "10003"}
    spot = ds.scanner(
        columns=["state", "county", "county_fips", "scope", "match_method"],
        filter=("(state='MS' AND county IN ('Lowndes','Lowndes County')) OR "
                "(state='AL' AND county='Tuscaloosa County') OR (state='DE' AND county='New Castle County')"),
    ).to_table().to_pylist()
    spot_got = {(r["state"], r["county"]): r["county_fips"] for r in spot}

    out = {
        "uri": TARGET_URI, "rows": rows, "distinct_key": distinct, "grain_1to1": rows == distinct,
        "columns": ds.schema.names, "index_cols": sorted(idx_cols),
        "scope_county": ds.count_rows(filter="scope = 'county'"),
        "scope_unmapped": ds.count_rows(filter="scope = 'unmapped'"),
        "county_null_fips": ds.count_rows(filter="scope = 'county' AND county_fips IS NULL"),
        "distinct_county_fips": len({r["county_fips"] for r in ds.scanner(columns=["county_fips"]).to_table().to_pylist() if r["county_fips"]}),
        "spot_check": spot,
    }
    print(json.dumps(out, indent=2, default=str))

    errors: list[str] = []
    if rows != distinct:
        errors.append(f"grain broken: rows={rows} != distinct={distinct}")
    if "county_fips" not in idx_cols:
        errors.append(f"BTREE county_fips index absent — index_cols={sorted(idx_cols)}")
    if ds.count_rows(filter="scope = 'county' AND county_fips IS NULL"):
        errors.append("scope='county' rows with NULL county_fips present")
    for key, want in spot_expect.items():
        got = spot_got.get(key)
        if got is not None and got != want:
            errors.append(f"spot fail {key}: county_fips={got} != expected {want}")
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
