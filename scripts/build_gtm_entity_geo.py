#!/usr/bin/env python3
"""gtm_entity_geo — L2 entity HQ geo sidecar (one row per uei, best-available coordinates).

Derived GEO only — deterministic joins from the entity spine to the durable geocode
crosswalk, with a county-centroid fallback. No vendor data, no editorial scores.
Rebuilt-and-swapped snapshot, never appended. Consumed by catalyst_api's market query
engine (/api/v1/map/entities/query) to put real dots on the map.

PARAM SET v1 (every editorial decision, in one place):
  - population: every uei in gtm_sam_entities (the 2.03M SAM∪DSBS∪FSRS entity spine —
    the exact universe the market engine serves).
  - tier 'address': spine uei → sam_master_entities physical address (street/city/
    state/zip; the spine carries no street) → addr_hash via the CANONICAL
    pipelines/_shared/addr_hash.addr_hash_sql (the ONE join-key definition every
    geocode reader imports — never re-derived) → geocode_xwalk lat/lon. The xwalk
    stores only successfully geocoded addresses (rooftop; no-match rows are skipped at
    its build), so a hash hit IS a coordinate.
  - tier 'county': remaining ueis → zip5 (spine physical_zip, else the SAM zip) →
    DOMINANT county via census_zcta_county_rel_2020 (max alloc_land per zcta5;
    zip≈zcta, the standard approximation) → county centroid via
    census_county_gazetteer_2023. Precision is honest: 'county', never 'address'.
  - unmatched ueis get NO row. Absence = no geometry. NEVER a state-level fake
    centroid — a dot that lies about where an entity is, is worse than no dot.

TARGET  s3://data-sink/active/gtm_entity_geo/   (overwrite; BTREE uei, BITMAP geo_precision)
GRAIN   1 row per uei: uei, latitude, longitude, geo_precision ('address'|'county'),
        geo_source, as_of, built_from_version, param_set_id

    LANCE_BYPASS_SPILLING=true doppler run -p core-x -c prd -- python3 \
        scripts/build_gtm_entity_geo.py --as-of YYYY-MM-DD

Read-only sources; one Lance write. Doppler core-x/prd.
"""
from __future__ import annotations
import argparse
import os
import sys
from datetime import date
from pathlib import Path

import duckdb
import lance

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pipelines._shared.addr_hash import addr_hash_sql, _zip5_sql  # noqa: E402

A = "s3://data-sink/active"
OUT = f"{A}/gtm_entity_geo/"
PARAM_SET_ID = "v1"


def so() -> dict:
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": os.environ.get("R2_ENDPOINT") or f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
            "region": "auto"}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--as-of", default=date.today().isoformat())
    args = ap.parse_args()
    as_of = args.as_of

    opt = so()
    con = duckdb.connect()
    con.execute("SET memory_limit='14GB'; SET threads TO 4;")
    os.makedirs("/tmp/duck_spill", exist_ok=True)
    con.execute("SET temp_directory='/tmp/duck_spill';")

    # ── land the narrow projections (Lance scanners are one-shot: stream each into a
    # DuckDB table under its own name; every later query reads the DuckDB copy) ──────────
    spine_ds = lance.dataset(f"{A}/gtm_sam_entities/", storage_options=opt)
    sam_ds = lance.dataset(f"{A}/sam_master_entities/", storage_options=opt)
    xw_ds = lance.dataset(f"{A}/geocode_xwalk/", storage_options=opt)
    rel_ds = lance.dataset(f"{A}/census_zcta_county_rel_2020/", storage_options=opt)
    gaz_ds = lance.dataset(f"{A}/census_county_gazetteer_2023/", storage_options=opt)
    built_from = (f"gtm_sam_entities:v{spine_ds.version}|sam_master_entities:v{sam_ds.version}"
                  f"|geocode_xwalk:v{xw_ds.version}|census_zcta_county_rel_2020:v{rel_ds.version}"
                  f"|census_county_gazetteer_2023:v{gaz_ds.version}")
    print(f"sources: {built_from}  as_of={as_of}", flush=True)

    for name, reader in (
        ("spine", spine_ds.scanner(
            columns=["uei", "physical_zip"], filter="uei IS NOT NULL").to_reader()),
        ("sam", sam_ds.scanner(
            columns=["uei", "physical_address_line_1", "physical_address_city",
                     "physical_address_province_or_state", "physical_address_zip_postal_code"],
            filter="uei IS NOT NULL").to_reader()),
        # The xwalk stores only geocoded rows; the NULL guard is belt-and-braces.
        ("xw", xw_ds.scanner(
            columns=["addr_hash", "latitude", "longitude", "geocode_source"],
            filter="latitude IS NOT NULL AND longitude IS NOT NULL").to_reader()),
        ("rel", rel_ds.scanner(columns=["zcta5", "county_fips", "alloc_land"]).to_reader()),
        ("gaz", gaz_ds.scanner(columns=["county_fips", "lat", "lon"]).to_reader()),
    ):
        con.register("_r", reader)
        con.execute(f"CREATE TABLE {name} AS SELECT * FROM _r")
        con.unregister("_r")
        print(f"{name} landed: {con.execute(f'SELECT COUNT(*) FROM {name}').fetchone()[0]:,}", flush=True)

    # ── tier 'address': spine → SAM street address → canonical addr_hash → xwalk ─────────
    # sam_master_entities is 1 row/uei and geocode_xwalk is 1 row/addr_hash, but the join
    # is deduped defensively (ROW_NUMBER) so a substrate regression can never double a uei.
    hexpr = addr_hash_sql("m.physical_address_line_1", "m.physical_address_city",
                          "m.physical_address_province_or_state",
                          "m.physical_address_zip_postal_code")
    con.execute(f"""
    CREATE TABLE tier_address AS
    SELECT uei, latitude, longitude, geo_source FROM (
      SELECT s.uei, x.latitude, x.longitude,
             'geocode_xwalk:' || COALESCE(x.geocode_source, 'unknown') AS geo_source,
             ROW_NUMBER() OVER (PARTITION BY s.uei ORDER BY x.addr_hash) AS rn
      FROM spine s
      JOIN sam m ON s.uei = m.uei
      JOIN xw x ON {hexpr} = x.addr_hash
    ) WHERE rn = 1
    """)
    n_addr = con.execute("SELECT COUNT(*) FROM tier_address").fetchone()[0]
    print(f"tier address: {n_addr:,}", flush=True)

    # ── tier 'county': remaining ueis → zip5 → dominant county → county centroid ─────────
    # Dominant county = max land allocation for the ZCTA (ties break on county_fips for
    # determinism). zip5 prefers the spine zip, falls back to the SAM registration zip.
    spine_zip5 = _zip5_sql("s.physical_zip")
    sam_zip5 = _zip5_sql("m.physical_address_zip_postal_code")
    con.execute(f"""
    CREATE TABLE tier_county AS
    WITH dom AS (
      SELECT zcta5, county_fips FROM (
        SELECT zcta5, county_fips,
               ROW_NUMBER() OVER (PARTITION BY zcta5 ORDER BY alloc_land DESC, county_fips) AS rn
        FROM rel
      ) WHERE rn = 1
    ),
    remaining AS (
      SELECT s.uei,
             COALESCE(NULLIF({spine_zip5}, ''), NULLIF({sam_zip5}, '')) AS zip5
      FROM spine s
      LEFT JOIN sam m ON s.uei = m.uei
      ANTI JOIN tier_address t ON s.uei = t.uei
    )
    SELECT uei, lat AS latitude, lon AS longitude,
           'census_zcta_county_rel_2020+census_county_gazetteer_2023' AS geo_source
    FROM (
      SELECT r.uei, g.lat, g.lon,
             ROW_NUMBER() OVER (PARTITION BY r.uei ORDER BY d.county_fips) AS rn
      FROM remaining r
      JOIN dom d ON r.zip5 = d.zcta5
      JOIN gaz g ON d.county_fips = g.county_fips
    ) WHERE rn = 1
    """)
    n_county = con.execute("SELECT COUNT(*) FROM tier_county").fetchone()[0]
    print(f"tier county: {n_county:,}", flush=True)

    con.execute(f"""
    CREATE TABLE geo AS
    SELECT uei, latitude, longitude, 'address' AS geo_precision, geo_source,
           DATE '{as_of}' AS as_of, '{built_from}' AS built_from_version,
           '{PARAM_SET_ID}' AS param_set_id
    FROM tier_address
    UNION ALL
    SELECT uei, latitude, longitude, 'county', geo_source,
           DATE '{as_of}', '{built_from}', '{PARAM_SET_ID}'
    FROM tier_county
    """)

    # ── invariants (fail-closed before any write) ─────────────────────────────────────────
    n_geo, n_spine = con.execute(
        "SELECT (SELECT COUNT(*) FROM geo), (SELECT COUNT(*) FROM spine)").fetchone()
    dup = con.execute(
        "SELECT COUNT(*) FROM (SELECT uei FROM geo GROUP BY 1 HAVING COUNT(*) > 1)").fetchone()[0]
    assert dup == 0, f"geo uei not unique: {dup} dups"
    bad = con.execute("""
      SELECT COUNT(*) FROM geo
      WHERE latitude IS NULL OR longitude IS NULL
         OR NOT isfinite(latitude) OR NOT isfinite(longitude)
         OR latitude < -90 OR latitude > 90 OR longitude < -180 OR longitude > 180
    """).fetchone()[0]
    assert bad == 0, f"{bad} rows with missing/non-finite/out-of-range coordinates"
    assert n_geo == n_addr + n_county, "tier union lost rows"
    unmatched = n_spine - n_geo
    print(f"invariants OK  spine={n_spine:,}  address={n_addr:,} ({n_addr/n_spine:.1%})  "
          f"county={n_county:,} ({n_county/n_spine:.1%})  "
          f"unmatched-dropped={unmatched:,} ({unmatched/n_spine:.1%})", flush=True)

    # ── write + index ─────────────────────────────────────────────────────────────────────
    res = con.execute("SELECT * FROM geo")
    reader = res.to_arrow_reader(65536) if hasattr(res, "to_arrow_reader") else res.fetch_record_batch(65536)
    lance.write_dataset(reader, OUT, mode="overwrite", storage_options=opt)
    ds = lance.dataset(OUT, storage_options=opt)
    for col, idx in (("uei", "BTREE"), ("geo_precision", "BITMAP")):
        ds.create_scalar_index(col, idx)
    print(f"wrote {OUT}  v{ds.version}  rows={ds.count_rows():,}  indexes=['uei', 'geo_precision']",
          flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
