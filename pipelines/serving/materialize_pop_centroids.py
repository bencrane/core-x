#!/usr/bin/env python3
"""Serving build — place-of-performance centroids for the award + subaward spines.

SoR  s3://data-sink/active/usaspending_award_pop_centroids/     (1 row / award)
     s3://data-sink/active/usaspending_subaward_pop_centroids/  (1 row / subaward)
     (Lance; derived, snapshot-overwrite; BTREE on the grain key)

WHAT THIS IS
The award-side half of the proximity layer. Entities already carry HQ geometry
(gtm_entity_geo, precision-flagged); this sidecar gives every award/subaward a work-site
point so distance is a read-time computation between ANY entity and ANY award — the
sidecar itself encodes NO radius, NO threshold, NO pairing (those are recipe parameters,
per the read-time weighting doctrine).

GEOCODE  pop zip5 → zcta_zip_centroids (33,780 ZCTA population-weighted points, WGS-84).
PoP has no street address, so ZCTA centroid is the honest best; precision is explicit:

  geo_precision   latitude/longitude
  'zip5'          ZCTA centroid (the zip5 matched)
  'state'         NULL — US row without a matchable zip5; state_code still supports
                  state-level matching (a state centroid would fake precision)
  'foreign'       NULL — PoP outside the US
  'none'          NULL — no usable geo at all

SOURCES (single-pass scans, versions stamped in built_from_version)
  usaspending_award_canonical     zip5 = COALESCE(pop_zip5, first-5 of the fresh-feed
                                  primary_place_of_performance_zip_4)  [~96.7% zip5]
  usaspending_subaward_canonical  subaward_primary_place_of_performance_address_zip_code
                                  [~86.4% zip5] — the SUB's work site, distinct from the
                                  prime award's PoP
  zcta_zip_centroids              zip5 → (lat, lon)

GRAIN: award → 1 row / generated_unique_award_id; subaward → 1 row / subaward_unique_key.
Fail-closed on grain duplication or a null grain key. Idempotent snapshot-overwrite.

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'pylance>=8' --with 'pyarrow>=17' --with 'duckdb>=1.5,<2' --with boto3 \
      python3 pipelines/serving/materialize_pop_centroids.py [--verify]
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
AWARD_URI = f"{A}/usaspending_award_canonical/"
SUB_URI = f"{A}/usaspending_subaward_canonical/"
ZCTA_URI = f"{A}/zcta_zip_centroids/"
AWARD_OUT = f"{A}/usaspending_award_pop_centroids/"
SUB_OUT = f"{A}/usaspending_subaward_pop_centroids/"
PARAM_SET_ID = "v1"
US_CODES = ("USA", "UNITED STATES", "US")


def so() -> dict:
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": os.environ.get("R2_ENDPOINT") or f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
            "region": "auto"}


def _load_zcta(con: duckdb.DuckDBPyConnection, opt: dict) -> str:
    z = lance.dataset(ZCTA_URI, storage_options=opt)
    con.register("_z", z.scanner(columns=["zcta5", "lat", "lon"]).to_reader())
    con.execute("CREATE TABLE zcta AS SELECT * FROM _z")
    con.unregister("_z")
    return f"zcta_zip_centroids:v{z.version}"


def _build(con: duckdb.DuckDBPyConnection, *, table: str, key: str, zip_expr: str,
           country_expr: str, state_expr: str, src: str) -> None:
    us = ", ".join(f"'{c}'" for c in US_CODES)
    con.execute(f"""
    CREATE TABLE {table} AS
    SELECT s.{key},
           s.zip5,
           z.lat  AS latitude,
           z.lon  AS longitude,
           s.state_code,
           s.country_code,
           CASE WHEN z.zcta5 IS NOT NULL                        THEN 'zip5'
                WHEN s.country_code IS NOT NULL
                     AND s.country_code NOT IN ({us})           THEN 'foreign'
                WHEN s.state_code IS NOT NULL                   THEN 'state'
                ELSE 'none' END AS geo_precision
    FROM (SELECT {key},
                 regexp_extract({zip_expr}, '^[0-9]{{5}}') AS zip5,
                 {state_expr}   AS state_code,
                 upper({country_expr}) AS country_code
          FROM {src}
          WHERE {key} IS NOT NULL) s
    LEFT JOIN zcta z
           ON s.zip5 = z.zcta5
          AND (s.country_code IS NULL OR s.country_code IN ({us}))
    """)
    dup = con.execute(f"""SELECT COUNT(*) FROM (
        SELECT {key} FROM {table} GROUP BY 1 HAVING COUNT(*) > 1)""").fetchone()[0]
    assert dup == 0, f"{table}: grain not unique on {key}: {dup} dups"


def _write(con: duckdb.DuckDBPyConnection, table: str, out: str, key: str,
           as_of: str, built_from: str, opt: dict) -> None:
    res = con.execute(f"""
        SELECT *, DATE '{as_of}' AS as_of, '{built_from}' AS built_from_version,
               '{PARAM_SET_ID}' AS param_set_id FROM {table}""")
    reader = res.to_arrow_reader(65536) if hasattr(res, "to_arrow_reader") else res.fetch_record_batch(65536)
    ds = write_indexed_dataset(reader, out, [(key, "BTREE")], storage_options=opt)
    print(f"wrote {out}  v{ds.version}  rows={ds.count_rows():,}  "
          f"indices={[i['name'] for i in ds.list_indices()]}", flush=True)


def build() -> int:
    opt = so()
    as_of = date.today().isoformat()
    con = duckdb.connect()
    con.execute("SET memory_limit='10GB'; SET threads TO 4; SET preserve_insertion_order=false;")
    os.makedirs("/tmp/duck_spill", exist_ok=True)
    con.execute("SET temp_directory='/tmp/duck_spill';")

    zver = _load_zcta(con, opt)

    aw = lance.dataset(AWARD_URI, storage_options=opt)
    con.register("_a", aw.scanner(columns=[
        "generated_unique_award_id", "pop_zip5", "primary_place_of_performance_zip_4",
        "primary_place_of_performance_state_code",
        "primary_place_of_performance_country_code"]).to_reader())
    con.execute("CREATE TABLE award_src AS SELECT * FROM _a")
    con.unregister("_a")
    _build(con, table="award_pop", key="generated_unique_award_id",
           zip_expr="COALESCE(pop_zip5, primary_place_of_performance_zip_4)",
           country_expr="primary_place_of_performance_country_code",
           state_expr="primary_place_of_performance_state_code", src="award_src")
    built_from_a = f"usaspending_award_canonical:v{aw.version}|{zver}"

    sb = lance.dataset(SUB_URI, storage_options=opt)
    con.register("_s", sb.scanner(columns=[
        "subaward_unique_key", "subaward_primary_place_of_performance_address_zip_code",
        "subaward_primary_place_of_performance_state_code",
        "subaward_primary_place_of_performance_country_code"]).to_reader())
    con.execute("CREATE TABLE sub_src AS SELECT * FROM _s")
    con.unregister("_s")
    _build(con, table="sub_pop", key="subaward_unique_key",
           zip_expr="subaward_primary_place_of_performance_address_zip_code",
           country_expr="subaward_primary_place_of_performance_country_code",
           state_expr="subaward_primary_place_of_performance_state_code", src="sub_src")
    built_from_s = f"usaspending_subaward_canonical:v{sb.version}|{zver}"

    for t in ("award_pop", "sub_pop"):
        rows = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        by = con.execute(f"SELECT geo_precision, COUNT(*) FROM {t} GROUP BY 1 ORDER BY 2 DESC").fetchall()
        print(f"{t}: {rows:,} rows  precision: {by}", flush=True)

    _write(con, "award_pop", AWARD_OUT, "generated_unique_award_id", as_of, built_from_a, opt)
    _write(con, "sub_pop", SUB_OUT, "subaward_unique_key", as_of, built_from_s, opt)
    return 0


def verify() -> int:
    opt = so()
    ok = True
    for out, key, floor in ((AWARD_OUT, "generated_unique_award_id", 30_000_000),
                            (SUB_OUT, "subaward_unique_key", 1_200_000)):
        ds = lance.dataset(out, storage_options=opt)
        rows = ds.count_rows()
        located = ds.count_rows(filter="geo_precision = 'zip5' AND latitude IS NOT NULL")
        idx = [i["name"] for i in ds.list_indices()]
        good = rows >= floor and located > 0.8 * rows and any(key in n for n in idx)
        print(f"{out}: rows={rows:,} zip5_located={located:,} ({100*located/rows:.1f}%) "
              f"indices={idx} -> {'OK' if good else 'FAIL'}", flush=True)
        ok = ok and good
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(verify() if "--verify" in sys.argv else build())
