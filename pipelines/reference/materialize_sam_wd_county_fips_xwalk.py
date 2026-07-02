"""Reference crosswalk — sam_wd_county_fips_xwalk: one row per distinct SAM (state, county) identity.

The SAM Wage-Determination coverage feed (``sam_wd_county_coverage``) names its geographies with
BARE county labels ('Autauga') and a SAM-internal ``county_code`` that is NOT a FIPS code. This
crosswalk resolves each distinct (state_code, county_name) SAM identity to a canonical 5-digit
county_fips off the Census gazetteer authority (``national_county2020``), so downstream joins can
land a WD onto real county geography.

  gazetteer authority  national_county2020   3,235 rows. county_fips (5-digit '01001'),
                                              state_usps ('AL'), county_name (SUFFIXED — 'Autauga
                                              County', 'Richmond city'), class_fp. class_fp H1/H4/
                                              H5/H6 = county-equivalent (county/parish/borough/
                                              census-area); C7 = INDEPENDENT CITY. Retains the
                                              legacy CT counties the 2023 gazetteer dropped.
  SAM geographies      sam_wd_county_coverage 33,156 rows. wd_id, state_code (ALREADY USPS 'AL'),
                                              county_name (BARE 'Autauga'; independent cities carry
                                              a trailing '*' e.g. 'Richmond*', or ' city'/' City').
                                              3,347 distinct (state_code, county_name) identities.

GRAIN     1 row per DISTINCT (state_code, county_name) in sam_wd_county_coverage (~3,347). The SAM
          identity is the spine; the resolved FIPS attaches to it. FAIL-CLOSED: count(*) ==
          count(distinct (state_code, county_name)) is asserted before publish; >=95% of the
          NON-statewide identities must resolve to a non-null county_fips; zero rows may carry
          scope='county' with a NULL county_fips.

TABLE     s3://data-sink/active/sam_wd_county_fips_xwalk/    BTREE: county_fips, county_name
          state_code, county_name, county_fips (nullable), county_name_census (nullable),
          class_fp (nullable), scope ('county'|'statewide'|'unmapped'),
          match_method ('exact'|'alias'|'collision_city'|'collision_county'|'statewide'|'unmapped'),
          source, ingested_at.

RESOLUTION (single DuckDB build; alias table rendered into a VALUES scan)
    1. STATEWIDE     county_name ILIKE '%statewide%'  -> scope='statewide', method='statewide',
                     county_fips NULL. (In the downstream rollup a statewide coverage row expands to
                     ALL of that state's real counties: national_county2020 WHERE state_usps=? AND
                     class_fp LIKE 'H%'. This xwalk carries the marker, not the expansion.)
    2. ALIAS         a hard (state_code, county_name literal) -> county_fips override table catches
                     the known spelling/abbreviation/mis-file cases (Dade->12086, Wade Hampton->
                     02158, VI St Croix->78010, the VA Charles City / James City real-county pair,
                     …). scope='county', method='alias'.
    3. NZ EXACT      normalize BOTH the SAM label and the Census county_name (see NZ below) and join
                     on (state, normalized). A single hit -> scope='county', method='exact'. A
                     multi-hit is the independent-city COLLISION (VA/MD/MO share a normalized name
                     between an H-class county and a C-class city): if the SAM label is a city
                     (trailing '*' or ' city'/' City', EXCEPT the VA real counties Charles City /
                     James City, which the alias table already resolved) pick the C-class row
                     (method='collision_city'); else pick the H-class row (method='collision_county').
                     ASYMMETRIC-COLLISION SAFETY: a city-flagged SAM label that finds NO C-class row
                     is NOT silently mapped to the H-class county — it routes to unmapped (must be
                     alias-covered to resolve). Symmetrically, a non-city label that finds ONLY a
                     C-class row routes to unmapped rather than taking the city FIPS.
    4. UNMAPPED      anything still unresolved (AK combined-area splits, Pacific
                     Wake/Midway/Johnston/Marianas, VA cities mis-filed under MD, the asymmetric
                     city/county-class mismatches above) -> scope='unmapped', method='unmapped',
                     county_fips NULL. Deliberately NOT guessed.

NZ (normalization, applied identically to SAM and Census names)
    rtrim '*' first, strip ONE trailing county-equivalent suffix (county|parish|borough|census
    area|city and borough|municipality|municipio|city|island|district), Saint->St, strip accents,
    drop non-alphanumerics, upper.

SOURCE    Derived reference — reads only Lance SoR datasets under s3://data-sink/active/. No landing
          fetch, no external HTTP. Idempotent (mode='overwrite'): re-run after any SAM coverage or
          gazetteer refresh. Reuses pipelines.bls.ingest R2 plumbing (_storage_options) and the
          shared Lance index builder (_build_indexes) — no re-implementation.

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'duckdb>=1.1' --with 'pylance>=7' --with 'pyarrow>=17' --with 'boto3>=1.34' \
      python3 -m pipelines.reference.materialize_sam_wd_county_fips_xwalk build

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'duckdb>=1.1' --with 'pylance>=7' --with 'pyarrow>=17' --with 'boto3>=1.34' \
      python3 -m pipelines.reference.materialize_sam_wd_county_fips_xwalk verify
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
SAM_COVERAGE_URI = os.environ.get(
    "SAM_WD_COUNTY_COVERAGE_URI", f"{ACTIVE}/sam_wd_county_coverage")
NATIONAL_COUNTY_URI = os.environ.get(
    "NATIONAL_COUNTY2020_URI", f"{ACTIVE}/national_county2020")

# Output crosswalk.
TARGET_URI = os.environ.get(
    "SAM_WD_COUNTY_FIPS_XWALK_URI", f"{ACTIVE}/sam_wd_county_fips_xwalk/")

DATA_STORAGE_VERSION = "2.1"       # net-new Lance dataset -> pin current default storage version
SOURCE = "sam_wd_county_coverage+national_county2020"

BTREE = ["county_fips", "county_name"]
BITMAP: list[str] = []

# Expected 1-row-per-(state,county) band. Distinct SAM identities live-probed at 3,347.
EXPECTED_ROWS = 3347
ROW_BAND = (3200, 3500)

# Fraction of NON-statewide identities that must resolve to a non-null county_fips.
MIN_RESOLVED_FRAC = 0.95

# Hard (state_code, county_name literal) -> county_fips overrides. Spelling variants, historical
# names, abbreviation truncations, the VA Charles City / James City real-county pair, and the
# territories that never normalize to a gazetteer row. Literals are matched pre-normalization,
# case-sensitive, against the raw SAM county_name. (All live-verified 2026-07-02.)
ALIAS: dict[tuple[str, str], str] = {
    ("FL", "Dade"): "12086",
    ("GA", "Columbus"): "13215",
    ("DC", "Washington, D.C."): "11001",
    ("VA", "Albermarle"): "51003",
    ("VA", "Colonial Hghts"): "51570",
    ("VA", "Charles City"): "51036",
    ("VA", "Charles*"): "51036",
    ("VA", "James City"): "51095",
    ("VA", "James*"): "51095",
    ("VA", "Clifton Forge*"): "51005",
    ("VA", "South Boston*"): "51083",
    ("MN", "Lake of The Woo"): "27077",
    ("PR", "Luqillo"): "72087",
    ("VI", "St Croix"): "78010",
    ("VI", "St John"): "78020",
    ("VI", "Saint John"): "78020",
    ("VI", "St Thomas"): "78030",
    ("AK", "Wade Hampton"): "02158",
    ("AK", "Northwest Artic"): "02188",
    ("AK", "Peninsula & Lake"): "02164",
    ("AK", "Southeast Fairb"): "02240",
    ("AK", "Fairbanks North"): "02090",
    ("AS", "Eastern"): "60010",
    ("AS", "Western"): "60050",
    ("AS", "Manu'a"): "60020",
}

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


def _nz(col: str) -> str:
    """Name-normalization SQL expression, applied identically to SAM and Census county names.

    rtrim '*' first, strip ONE trailing county-equivalent suffix, Saint->St, strip accents, drop
    non-alphanumerics, upper. Mirrors the LOCKED grounding normalization verbatim."""
    return (
        "upper(regexp_replace(strip_accents(regexp_replace(regexp_replace("
        f"rtrim(rtrim(trim({col})),'*'),"
        "'(?i)\\s+(county|parish|borough|census area|city and borough|municipality|municipio|city|island|district)$',''),"
        "'(?i)saint','st','g')),'[^A-Za-z0-9]','','g'))"
    )


def _alias_values_sql() -> str:
    """Render the ALIAS dict into a DuckDB VALUES scan: (state_code, county_name) -> county_fips."""
    def esc(s: str) -> str:
        return s.replace("'", "''")

    rows = ",\n            ".join(
        f"('{esc(st)}', '{esc(nm)}', '{fips}')" for (st, nm), fips in ALIAS.items()
    )
    return f"(VALUES\n            {rows}\n        ) AS a(state_code, county_name, county_fips)"


def _ambiguity_probe_sql() -> str:
    """Same-class (state_usps, nz_name) groups with >1 distinct FIPS — an ambiguous normalization.

    census_h/census_c collapse each (state, nz_name) group with any_value(); if a single state has
    two distinct H-class (or two distinct C-class) counties normalizing to the SAME token, that
    any_value() silently picks one arbitrarily and emits it as method='exact', passing the 1:1 grain
    gate. This probe surfaces such a group so the build FAILS CLOSED instead of resolving to noise.
    The live gazetteer has none; a future refresh that introduces one must not resolve silently."""
    return f"""
    WITH classed AS (
        SELECT
            state_usps,
            CASE WHEN class_fp LIKE 'H%' THEN 'H'
                 WHEN class_fp LIKE 'C%' THEN 'C' END AS cls,
            {_nz('county_name')} AS nz_name,
            county_fips
        FROM county_src
        WHERE class_fp LIKE 'H%' OR class_fp LIKE 'C%'
    )
    SELECT state_usps, cls, nz_name, COUNT(DISTINCT county_fips) AS n_fips,
           string_agg(DISTINCT county_fips, ',') AS fips_set
    FROM classed
    GROUP BY state_usps, cls, nz_name
    HAVING COUNT(DISTINCT county_fips) > 1
    ORDER BY state_usps, cls, nz_name
    """


def _build_sql() -> str:
    """Single DuckDB statement resolving each distinct SAM (state_code, county_name) to a FIPS.

    Priority: statewide -> alias -> NZ-exact (with the independent-city collision rule) -> unmapped.
    census_h / census_c hold the per-(state, normalized-name) FIPS for the H-class county and the
    C-class independent city respectively; the collision picks between them by the SAM is_city flag.

    ASYMMETRIC-COLLISION SAFETY: the class the SAM flag demands is authoritative. A city-flagged
    label with NO C-class row does NOT fall through to the H county; a non-city label with NO H-class
    row does NOT fall through to the C city. Class-mismatched labels route to unmapped (alias-only).
    """
    return f"""
    WITH sam AS (
        SELECT DISTINCT state_code, county_name
        FROM sam_src
        WHERE state_code IS NOT NULL AND county_name IS NOT NULL
    ),
    sam_flagged AS (
        SELECT
            state_code,
            county_name,
            (county_name ILIKE '%statewide%') AS is_statewide,
            -- independent-city marker: trailing '*' or ' city'/' City'. The VA real counties
            -- 'Charles City' / 'James City' are alias-resolved before the collision rule fires,
            -- so they never reach the is_city branch.
            (
                rtrim(trim(county_name)) LIKE '%*'
                OR lower(rtrim(trim(county_name))) LIKE '% city'
            ) AS is_city,
            {_nz('county_name')} AS nz_name
        FROM sam
    ),
    alias AS (
        SELECT state_code, county_name, county_fips FROM {_alias_values_sql()}
    ),
    -- Census gazetteer, normalized. Split into county-equivalent (H%) and independent-city (C%)
    -- so a collision on nz_name can be resolved by class.
    census_h AS (
        SELECT state_usps, {_nz('county_name')} AS nz_name,
               any_value(county_fips) AS county_fips,
               any_value(county_name) AS county_name_census,
               any_value(class_fp)   AS class_fp
        FROM county_src
        WHERE class_fp LIKE 'H%'
        GROUP BY state_usps, {_nz('county_name')}
    ),
    census_c AS (
        SELECT state_usps, {_nz('county_name')} AS nz_name,
               any_value(county_fips) AS county_fips,
               any_value(county_name) AS county_name_census,
               any_value(class_fp)   AS class_fp
        FROM county_src
        WHERE class_fp LIKE 'C%'
        GROUP BY state_usps, {_nz('county_name')}
    ),
    -- Full Census FIPS lookup for the alias branch (alias supplies the fips; we backfill the
    -- census name/class for the resolved fips so the row is fully populated).
    census_all AS (
        SELECT county_fips,
               any_value(county_name) AS county_name_census,
               any_value(class_fp)    AS class_fp
        FROM county_src
        GROUP BY county_fips
    ),
    resolved AS (
        SELECT
            s.state_code,
            s.county_name,
            s.is_statewide,
            s.is_city,
            s.nz_name,
            al.county_fips AS alias_fips,
            ch.county_fips AS h_fips,
            ch.county_name_census AS h_name,
            ch.class_fp AS h_class,
            cc.county_fips AS c_fips,
            cc.county_name_census AS c_name,
            cc.class_fp AS c_class
        FROM sam_flagged s
        LEFT JOIN alias    al ON al.state_code = s.state_code AND al.county_name = s.county_name
        LEFT JOIN census_h ch ON ch.state_usps = s.state_code AND ch.nz_name = s.nz_name
        LEFT JOIN census_c cc ON cc.state_usps = s.state_code AND cc.nz_name = s.nz_name
    ),
    -- class_pick: the FIPS the SAM flag DEMANDS. is_city -> C-class only; else -> H-class only.
    -- No cross-class fallback — an asymmetric class mismatch stays NULL and routes to unmapped.
    picked AS (
        SELECT
            r.*,
            CASE
                WHEN r.is_statewide THEN NULL
                WHEN r.alias_fips IS NOT NULL THEN r.alias_fips
                WHEN r.is_city THEN r.c_fips
                ELSE r.h_fips
            END AS picked_fips,
            CASE
                WHEN r.is_statewide THEN NULL
                WHEN r.alias_fips IS NOT NULL THEN ca.county_name_census
                WHEN r.is_city THEN r.c_name
                ELSE r.h_name
            END AS picked_name,
            CASE
                WHEN r.is_statewide THEN NULL
                WHEN r.alias_fips IS NOT NULL THEN ca.class_fp
                WHEN r.is_city THEN r.c_class
                ELSE r.h_class
            END AS picked_class,
            -- both classes present at this (state, nz) -> true collision; drives method labelling
            (r.h_fips IS NOT NULL AND r.c_fips IS NOT NULL) AS is_collision
        FROM resolved r
        LEFT JOIN census_all ca ON ca.county_fips = r.alias_fips
    )
    SELECT
        p.state_code,
        p.county_name,
        p.picked_fips AS county_fips,
        p.picked_name AS county_name_census,
        p.picked_class AS class_fp,
        CASE
            WHEN p.is_statewide THEN 'statewide'
            WHEN p.picked_fips IS NOT NULL THEN 'county'
            ELSE 'unmapped'
        END AS scope,
        CASE
            WHEN p.is_statewide THEN 'statewide'
            WHEN p.alias_fips IS NOT NULL THEN 'alias'
            WHEN p.picked_fips IS NULL THEN 'unmapped'
            WHEN p.is_city AND p.is_collision THEN 'collision_city'
            WHEN (NOT p.is_city) AND p.is_collision THEN 'collision_county'
            ELSE 'exact'
        END AS match_method,
        '{SOURCE}' AS source,
        now() AS ingested_at
    FROM picked p
    ORDER BY p.state_code, p.county_name
    """


def build() -> dict:
    import duckdb
    import lance

    so = _storage_options()
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=8;")

    # Register the Lance dataset OBJECTS (not one-shot .to_reader() streams — those drain on the
    # first scan and any re-scan yields 0 rows). DuckDB scans the Lance dataset directly and pushes
    # the projection down, so only the projected rows are pulled through the worker.
    con.register("sam_src", lance.dataset(SAM_COVERAGE_URI, storage_options=so))
    con.register("county_src", lance.dataset(NATIONAL_COUNTY_URI, storage_options=so))
    log("registered sam_wd_county_coverage + national_county2020 Lance scanners")

    # -- PRE-BUILD ambiguity gate — no same-class (state, nz_name) may map to >1 FIPS -----------
    ambig = con.execute(_ambiguity_probe_sql()).fetchall()
    if ambig:
        detail = "; ".join(
            f"{st}/{cls}/{nz}->[{fips}] (n={n})" for st, cls, nz, n, fips in ambig[:20])
        con.close()
        raise RuntimeError(
            f"GATE FAIL: {len(ambig)} ambiguous same-class (state,nz_name) group(s) in "
            f"national_county2020 — any_value() would resolve arbitrarily. {detail}")
    log("AMBIGUITY GATE PASS: no same-class (state, nz_name) collapses to >1 FIPS")

    # .to_arrow_table() returns a materialized pyarrow.Table (has .num_rows/.column()); the older
    # .arrow() returns a one-shot RecordBatchReader that the fail-closed gate below cannot index.
    tbl = con.execute(_build_sql()).to_arrow_table()
    con.close()
    rows = tbl.num_rows
    log(f"assembled sam_wd_county_fips_xwalk: {rows:,} rows x {tbl.num_columns} cols")

    # -- FAIL-CLOSED gate — structural 1:1 + resolution coverage before any publish -----------
    state_vals = tbl.column("state_code").to_pylist()
    county_vals = tbl.column("county_name").to_pylist()
    fips_vals = tbl.column("county_fips").to_pylist()
    scope_vals = tbl.column("scope").to_pylist()

    n_null_key = sum(1 for st, cn in zip(state_vals, county_vals) if st is None or cn is None)
    distinct_keys = {(st, cn) for st, cn in zip(state_vals, county_vals)}
    n_distinct = len(distinct_keys)
    if n_null_key:
        raise RuntimeError(
            f"GATE FAIL: {n_null_key} rows with NULL state_code/county_name (spine must be fully keyed)")
    if rows != n_distinct:
        raise RuntimeError(
            f"GATE FAIL: grain not 1:1 — rows={rows} != distinct (state_code, county_name)={n_distinct}")
    if not (ROW_BAND[0] <= rows <= ROW_BAND[1]):
        raise RuntimeError(
            f"GATE FAIL: row count {rows} outside expected band {ROW_BAND} "
            f"(distinct SAM identities expected ~{EXPECTED_ROWS})")

    # scope='county' must always carry a FIPS.
    bad_county = sum(
        1 for sc, fp in zip(scope_vals, fips_vals) if sc == "county" and fp is None)
    if bad_county:
        raise RuntimeError(
            f"GATE FAIL: {bad_county} rows with scope='county' but county_fips IS NULL")

    # >=95% of NON-statewide identities must resolve to a non-null FIPS.
    n_nonstatewide = sum(1 for sc in scope_vals if sc != "statewide")
    n_resolved = sum(
        1 for sc, fp in zip(scope_vals, fips_vals) if sc != "statewide" and fp is not None)
    resolved_frac = (n_resolved / n_nonstatewide) if n_nonstatewide else 1.0
    if resolved_frac < MIN_RESOLVED_FRAC:
        raise RuntimeError(
            f"GATE FAIL: resolved {n_resolved}/{n_nonstatewide} non-statewide "
            f"({resolved_frac:.3%}) < {MIN_RESOLVED_FRAC:.0%}")
    log(
        f"GATE PASS: rows={rows:,} distinct_key={n_distinct:,} band={ROW_BAND} null_key=0 "
        f"county_null_fips=0 resolved={n_resolved}/{n_nonstatewide} ({resolved_frac:.3%})")

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
        "distinct_key": n_distinct,
        "columns": tbl.schema.names,
        "scope_county": ds.count_rows(filter="scope = 'county'"),
        "scope_statewide": ds.count_rows(filter="scope = 'statewide'"),
        "scope_unmapped": ds.count_rows(filter="scope = 'unmapped'"),
        "method_exact": ds.count_rows(filter="match_method = 'exact'"),
        "method_alias": ds.count_rows(filter="match_method = 'alias'"),
        "method_collision_city": ds.count_rows(filter="match_method = 'collision_city'"),
        "method_collision_county": ds.count_rows(filter="match_method = 'collision_county'"),
        "resolved_nonstatewide": f"{n_resolved}/{n_nonstatewide} ({resolved_frac:.3%})",
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

    keys = ds.scanner(columns=["state_code", "county_name"]).to_table()
    st = keys.column("state_code").to_pylist()
    cn = keys.column("county_name").to_pylist()
    distinct_keys = len({(a, b) for a, b in zip(st, cn)})

    idx_cols = _index_fields(ds)

    # Spot-check identities — FIPS pinned to live-verified values (county / independent-city pairs,
    # the alias overrides, the DC special-case). Any drift here fails loud.
    spot_expect = {
        ("VA", "Fairfax"): "51059",
        ("VA", "Fairfax*"): "51600",
        ("VA", "Richmond"): "51159",
        ("VA", "Richmond*"): "51760",
        ("MD", "Baltimore"): "24005",
        ("FL", "Dade"): "12086",
        ("DC", "Washington, D.C."): "11001",
    }
    spot = ds.scanner(
        columns=["state_code", "county_name", "county_fips", "county_name_census",
                 "class_fp", "scope", "match_method"],
        filter=(
            "(state_code = 'VA' AND county_name IN ('Fairfax', 'Fairfax*', 'Richmond', 'Richmond*')) "
            "OR (state_code = 'MD' AND county_name = 'Baltimore') "
            "OR (state_code = 'FL' AND county_name = 'Dade') "
            "OR (state_code = 'DC' AND county_name = 'Washington, D.C.')"
        ),
    ).to_table().to_pylist()
    spot_got = {(r["state_code"], r["county_name"]): r["county_fips"] for r in spot}

    out = {
        "uri": TARGET_URI,
        "rows": rows,
        "distinct_key": distinct_keys,
        "grain_1to1": rows == distinct_keys,
        "columns": ds.schema.names,
        "index_cols": sorted(idx_cols),
        "scope_county": ds.count_rows(filter="scope = 'county'"),
        "scope_statewide": ds.count_rows(filter="scope = 'statewide'"),
        "scope_unmapped": ds.count_rows(filter="scope = 'unmapped'"),
        "county_null_fips": ds.count_rows(filter="scope = 'county' AND county_fips IS NULL"),
        "spot_check": spot,
    }
    print(json.dumps(out, indent=2, default=str))

    # -- FAIL-LOUD verify — grain, index presence, spot-check FIPS all non-zero-exit ----------
    errors: list[str] = []
    if rows != distinct_keys:
        errors.append(f"grain broken: rows={rows} != distinct_key={distinct_keys}")
    if "county_fips" not in idx_cols:
        errors.append(f"BTREE county_fips index absent — index_cols={sorted(idx_cols)}")
    if ds.count_rows(filter="scope = 'county' AND county_fips IS NULL"):
        errors.append("scope='county' rows with NULL county_fips present")
    for key, want in spot_expect.items():
        got = spot_got.get(key)
        if got != want:
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
