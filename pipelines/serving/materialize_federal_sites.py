#!/usr/bin/env python3
"""Serving build — federal_sites_lance: every federal site, one typed reference layer.

SoR  s3://data-sink/active/federal_sites_lance/  (Lance; derived, snapshot-overwrite;
     BTREE site_source / state_code / zip5)

WHAT THIS IS
The unified federal-footprint reference layer: military installations (NTAD polygons →
centroid + WKT retained), GSA buildings (IOLP rooftop points, with the building's lease
signal folded on via location_code), unmatched GSA lease locations, and FRPP civilian
real-property assets. One typed row per site; consumers (proximity recipes, the map)
filter by site_source and compute distance at read time — no radius baked anywhere.
Supersedes military_bases_lance as the serving layer (raw stays) and replaces the
retired govcon_firm_military_proximity derivation with nothing.

OVERLAP, KEPT HONEST: GSA's own buildings also appear inside FRPP (GSA reports ~8.6K
assets there ⊃ the 8,133 IOLP rows). Rows are NEVER merged — site_source labels each
origin, and frpp rows carry reporting_agency_code so a proximity recipe can exclude
FRPP-GSA shadows (site_source='frpp_asset' AND reporting_agency_code='047') instead of
double-counting. [047 = GSA per FRPP coding; verified at build, assert-printed.]

LEASE SIGNAL (rides gsa_building rows): active_lease_ct, lease_expiring_24mo_ct,
earliest_lease_expiration_date — aggregated from gsa_leases_lance by location_code.
Lease locations with no IOLP building row are emitted as site_source='gsa_lease'.

GRAIN: 1 row / (site_source, source_id). Fail-closed on duplication.

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'pylance>=8' --with 'pyarrow>=17' --with 'duckdb>=1.5,<2' --with boto3 \
      python3 pipelines/serving/materialize_federal_sites.py [--verify]
"""
from __future__ import annotations

import os
import sys
from datetime import date
from pathlib import Path

import duckdb
import lance

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pipelines._shared.lance_local_publish import write_indexed_dataset  # noqa: E402

A = "s3://data-sink/active"
BASES_URI = f"{A}/military_bases_lance/"
GSA_BLD_URI = f"{A}/gsa_buildings_lance/"
GSA_LEASE_URI = f"{A}/gsa_leases_lance/"
FRPP_URI = f"{A}/frpp_civilian_real_property/"
OUT = f"{A}/federal_sites_lance/"
PARAM_SET_ID = "v1"
BTREE_COLS = ["site_source", "state_code", "zip5"]


def so() -> dict:
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": os.environ.get("R2_ENDPOINT") or f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
            "region": "auto"}


def _load(con, name: str, uri: str, opt: dict, columns: list[str] | None = None):
    ds = lance.dataset(uri, storage_options=opt)
    con.register(f"_{name}", ds.scanner(columns=columns).to_reader())
    con.execute(f"CREATE TABLE {name} AS SELECT * FROM _{name}")
    con.unregister(f"_{name}")
    return ds


def build() -> int:
    opt = so()
    as_of = date.today().isoformat()
    con = duckdb.connect()
    con.execute("SET memory_limit='12GB'; SET threads TO 4; SET preserve_insertion_order=false;")
    con.execute("INSTALL spatial; LOAD spatial;")
    os.makedirs("/tmp/duck_spill", exist_ok=True)
    con.execute("SET temp_directory='/tmp/duck_spill';")

    bases = _load(con, "bases", BASES_URI, opt)
    bld = _load(con, "bld", GSA_BLD_URI, opt)
    lease = _load(con, "lease", GSA_LEASE_URI, opt)
    frpp = _load(con, "frpp", FRPP_URI, opt, columns=[
        "real_property_unique_identifier", "installation_name", "reporting_agency",
        "reporting_agency_code", "real_property_type", "asset_status",
        "legal_interest_indicator", "street_address", "city_name", "state_code",
        "zip_code", "country_code", "us_foreign", "latitude", "longitude",
        "square_feet_buildings"])

    gsa_frpp_ct = con.execute(
        "SELECT reporting_agency_code, COUNT(*) FROM frpp "
        "WHERE upper(reporting_agency) LIKE '%GENERAL SERVICES%' GROUP BY 1").fetchall()
    print(f"FRPP GSA shadow rows (agency code check): {gsa_frpp_ct}", flush=True)

    con.execute("""
    CREATE TABLE lease_by_loc AS
    SELECT location_code,
           COUNT(*) FILTER (lease_expiration_date >= CURRENT_DATE)  AS active_lease_ct,
           COUNT(*) FILTER (lease_expiration_date BETWEEN CURRENT_DATE
                            AND CURRENT_DATE + INTERVAL 24 MONTH)   AS lease_expiring_24mo_ct,
           MIN(lease_expiration_date) FILTER (lease_expiration_date >= CURRENT_DATE)
                                                                    AS earliest_lease_expiration_date
    FROM lease GROUP BY 1
    """)

    con.execute("""
    CREATE TABLE sites AS
    -- military installations: polygon centroid + WKT retained
    SELECT 'military_base' AS site_source,
           CAST(objectid AS VARCHAR) AS source_id,
           COALESCE(site_name, feature_name) AS site_name,
           'military_base' AS site_type,
           NULL AS owned_or_leased,
           NULL AS reporting_agency_code,
           NULL AS street_address, NULL AS city,
           state_name_code AS state_code, NULL AS zip5,
           ST_Y(ST_Centroid(ST_GeomFromText(geometry_wkt))) AS latitude,
           ST_X(ST_Centroid(ST_GeomFromText(geometry_wkt))) AS longitude,
           geometry_wkt, geometry_type,
           NULL::DOUBLE AS gross_square_feet, NULL::DOUBLE AS vacant_square_feet,
           NULL::BIGINT AS active_lease_ct, NULL::BIGINT AS lease_expiring_24mo_ct,
           NULL::DATE AS earliest_lease_expiration_date,
           operational_status AS asset_status
    FROM bases
    UNION ALL
    -- GSA buildings (IOLP) + folded lease signal
    SELECT 'gsa_building', b.location_code,
           COALESCE(b.real_property_asset_name, b.installation_name),
           COALESCE(b.real_property_asset_type, 'building'),
           b.owned_or_leased_indicator,
           NULL,
           b.street_address, b.city, b.state_cd, b.zipcode5,
           b.latitude, b.longitude, b.geometry_wkt, b.geometry_type,
           b.building_rsf, b.bld_vacant_rsf,
           l.active_lease_ct, l.lease_expiring_24mo_ct, l.earliest_lease_expiration_date,
           b.building_status
    FROM bld b LEFT JOIN lease_by_loc l USING (location_code)
    UNION ALL
    -- GSA lease locations with no IOLP building row (deduped: amendment rows collide
    -- on (location_code, lease_num); keep the latest expiration)
    SELECT 'gsa_lease', le.location_code || ':' || le.lease_num,
           COALESCE(le.real_property_asset_name, le.installation_name),
           COALESCE(le.real_property_asset_type, 'leased_space'),
           'LEASED', NULL,
           le.street_address, le.city, le.state_cd, le.zipcode5,
           le.latitude, le.longitude, NULL, 'POINT',
           le.building_rsf, le.bld_vacant_rsf,
           NULL, NULL, le.lease_expiration_date,
           NULL
    FROM (SELECT *, ROW_NUMBER() OVER (
              PARTITION BY location_code, lease_num
              ORDER BY lease_expiration_date DESC NULLS LAST) AS rn
          FROM lease) le
    ANTI JOIN bld b ON le.location_code = b.location_code
    WHERE le.rn = 1
    UNION ALL
    -- FRPP civilian assets (labeled; GSA shadows excludable via reporting_agency_code).
    -- FRPP grain is 1/reported asset-RECORD (multiple bureaus re-report one asset);
    -- this layer's grain is 1/SITE — dedupe to one row per rpuid, located rows first.
    SELECT 'frpp_asset', real_property_unique_identifier,
           installation_name, COALESCE(real_property_type, 'asset'),
           legal_interest_indicator, reporting_agency_code,
           street_address, city_name, state_code,
           regexp_extract(zip_code, '^[0-9]{5}'),
           latitude, longitude, NULL,
           CASE WHEN latitude IS NOT NULL THEN 'POINT' END,
           square_feet_buildings, NULL,
           NULL, NULL, NULL,
           asset_status
    FROM (SELECT *, ROW_NUMBER() OVER (
              PARTITION BY real_property_unique_identifier
              ORDER BY (latitude IS NOT NULL) DESC, reporting_agency_code, street_address NULLS LAST
          ) AS rn FROM frpp WHERE real_property_unique_identifier IS NOT NULL)
    WHERE rn = 1
    """)

    n = con.execute("SELECT COUNT(*) FROM sites").fetchone()[0]
    by = con.execute("SELECT site_source, COUNT(*), COUNT(latitude) FROM sites "
                     "GROUP BY 1 ORDER BY 2 DESC").fetchall()
    print(f"sites: {n:,}", flush=True)
    for s, c, geo in by:
        print(f"  {s}: {c:,} rows, {geo:,} with lat/lon", flush=True)
    dup = con.execute("""SELECT COUNT(*) FROM (
        SELECT site_source, source_id FROM sites GROUP BY 1,2 HAVING COUNT(*) > 1)""").fetchone()[0]
    assert dup == 0, f"grain not unique: {dup} dups"

    built_from = (f"military_bases_lance:v{bases.version}|gsa_buildings_lance:v{bld.version}|"
                  f"gsa_leases_lance:v{lease.version}|frpp_civilian_real_property:v{frpp.version}")
    res = con.execute(f"""
        SELECT *, DATE '{as_of}' AS as_of, '{built_from}' AS built_from_version,
               '{PARAM_SET_ID}' AS param_set_id FROM sites""")
    reader = res.to_arrow_reader(65536) if hasattr(res, "to_arrow_reader") else res.fetch_record_batch(65536)
    ds = write_indexed_dataset(reader, OUT, [(c, "BTREE") for c in BTREE_COLS],
                               storage_options=opt)
    print(f"wrote {OUT}  v{ds.version}  rows={ds.count_rows():,}  "
          f"indices={[i['name'] for i in ds.list_indices()]}", flush=True)
    return 0


def verify() -> int:
    opt = so()
    ds = lance.dataset(OUT, storage_options=opt)
    rows = ds.count_rows()
    idx = [i["name"] for i in ds.list_indices()]
    located = ds.count_rows(filter="latitude IS NOT NULL")
    bases_ct = ds.count_rows(filter="site_source = 'military_base'")
    bld_ct = ds.count_rows(filter="site_source = 'gsa_building'")
    ok = (200_000 < rows < 500_000 and located > 0.9 * rows
          and bases_ct == 824 and bld_ct == 8_133
          and all(any(c in n for n in idx) for c in BTREE_COLS))
    print(f"{OUT}: rows={rows:,} located={located:,} bases={bases_ct} gsa_bld={bld_ct} "
          f"indices={idx} -> {'OK' if ok else 'FAIL'}", flush=True)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(verify() if "--verify" in sys.argv else build())
