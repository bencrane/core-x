#!/usr/bin/env python3
"""gtm_entity_nearby_bases — nearest military sites per SAM entity (one-time computation).

SoR  s3://data-sink/active/gtm_entity_nearby_bases/
     (Lance; derived, snapshot-overwrite; BTREE uei)

One row per geocoded entity in gtm_entity_geo, two nearest-site answers side by side:
  nearest_site / _state / _miles              any of the 824 DoD sites (incl. small
                                              NG/reserve/support facilities)
  nearest_major_base / _state / _miles        FIRRMA-designated major installations only
                                              (223 sites; military_bases_lance flag)
Close.com sends nearest_major_base (operator-decided 2026-07-06); the any-site
answer stays queryable here. Haversine from registration coordinates
(federal_sites_lance military_base centroids). Static fact — computed once;
consumers join it.

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'pylance>=8' --with 'pyarrow>=17' --with 'duckdb>=1.5,<2' --with boto3 \
      python3 scripts/build_gtm_entity_nearest_base.py [--verify]
"""
from __future__ import annotations

import os
import sys
from datetime import date
from pathlib import Path

import duckdb
import lance

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pipelines._shared.lance_local_publish import write_indexed_dataset  # noqa: E402

A = "s3://data-sink/active"
GEO_URI = f"{A}/gtm_entity_geo/"
SITES_URI = f"{A}/federal_sites_lance/"
BASES_URI = f"{A}/military_bases_lance/"
OUT = f"{A}/gtm_entity_nearby_bases/"
PARAM_SET_ID = "v1"


def so() -> dict:
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": os.environ.get("R2_ENDPOINT") or f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
            "region": "auto"}


def build() -> int:
    opt = so()
    as_of = date.today().isoformat()
    con = duckdb.connect()
    con.execute("SET memory_limit='16GB'; SET threads TO 4; SET preserve_insertion_order=false;")
    os.makedirs("/tmp/duck_spill", exist_ok=True)
    con.execute("SET temp_directory='/tmp/duck_spill';")

    geo = lance.dataset(GEO_URI, storage_options=opt)
    con.register("_g", geo.scanner(
        columns=["uei", "latitude", "longitude", "geo_precision"],
        filter="latitude IS NOT NULL AND longitude IS NOT NULL").to_reader())
    con.execute("CREATE TABLE g AS SELECT * FROM _g")

    sites = lance.dataset(SITES_URI, storage_options=opt)
    con.register("_b", sites.scanner(
        columns=["site_name", "state_code", "latitude", "longitude"],
        filter="site_source = 'military_base' AND latitude IS NOT NULL").to_reader())
    bases = lance.dataset(BASES_URI, storage_options=opt)
    con.register("_mb", bases.scanner(
        columns=["site_name", "is_firrma_site"]).to_reader())
    con.execute("""CREATE TABLE b AS
        SELECT f.site_name, f.state_code, f.latitude AS blat, f.longitude AS blon,
               coalesce(m.is_major, false) AS is_major
        FROM _b f
        LEFT JOIN (SELECT site_name, bool_or(is_firrma_site) AS is_major
                   FROM _mb GROUP BY 1) m USING (site_name)""")
    nb, nmaj = con.execute("SELECT COUNT(*), COUNT(*) FILTER (is_major) FROM b").fetchone()

    # haversine miles over the full cross product, dual argmin per entity
    con.execute("""CREATE TABLE out AS
        SELECT uei,
               arg_min(site_name, dist_mi) AS nearest_site,
               arg_min(state_code, dist_mi) AS nearest_site_state,
               ROUND(MIN(dist_mi), 1) AS nearest_site_miles,
               arg_min(site_name, dist_mi) FILTER (is_major) AS nearest_major_base,
               arg_min(state_code, dist_mi) FILTER (is_major) AS nearest_major_base_state,
               ROUND(MIN(dist_mi) FILTER (is_major), 1) AS nearest_major_base_miles,
               any_value(geo_precision) AS geo_precision
        FROM (
            SELECT g.uei, g.geo_precision, b.site_name, b.state_code, b.is_major,
                   7917.6 * asin(sqrt(
                       sin(radians(b.blat - g.latitude) / 2) ^ 2
                       + cos(radians(g.latitude)) * cos(radians(b.blat))
                         * sin(radians(b.blon - g.longitude) / 2) ^ 2)) AS dist_mi
            FROM g CROSS JOIN b)
        GROUP BY 1""")

    n = con.execute("SELECT COUNT(*) FROM out").fetchone()[0]
    dup = con.execute("""SELECT COUNT(*) FROM (
        SELECT uei FROM out GROUP BY 1 HAVING COUNT(*) > 1)""").fetchone()[0]
    assert dup == 0, f"grain not unique: {dup} dups"
    meds = con.execute("SELECT median(nearest_site_miles), median(nearest_major_base_miles) FROM out").fetchone()
    print(f"entities: {n:,}  sites: {nb} (majors {nmaj})  "
          f"median miles: site {meds[0]:.1f} / major {meds[1]:.1f}", flush=True)

    res = con.execute(f"""
        SELECT *, DATE '{as_of}' AS as_of,
               'gtm_entity_geo:v{geo.version}|federal_sites_lance:v{sites.version}|military_bases_lance:v{bases.version}' AS built_from_version,
               '{PARAM_SET_ID}' AS param_set_id FROM out""")
    reader = res.to_arrow_reader(65536) if hasattr(res, "to_arrow_reader") else res.fetch_record_batch(65536)
    ds = write_indexed_dataset(reader, OUT, [("uei", "BTREE")], storage_options=opt)
    print(f"wrote {OUT}  v{ds.version}  rows={ds.count_rows():,}  "
          f"indices={[i['name'] for i in ds.list_indices()]}", flush=True)
    return 0


def verify() -> int:
    opt = so()
    ds = lance.dataset(OUT, storage_options=opt)
    rows = ds.count_rows()
    idx = [i["name"] for i in ds.list_indices()]
    t = ds.scanner(filter="uei = 'EV58RCKFSQN6'").to_table().to_pylist()  # Herdt Consulting, AL
    ok = rows > 1_000_000 and any("uei" in i for i in idx) and len(t) == 1
    print(f"{OUT}: rows={rows:,} indices={idx} spot={t} -> {'OK' if ok else 'FAIL'}", flush=True)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(verify() if "--verify" in sys.argv else build())
