"""Reference ingest — `gsa_buildings_lance` (GSA IOLP owned/leased buildings, point geometry).

SoR  s3://data-sink/active/gsa_buildings_lance/  (Lance v2.1; reference, snapshot-overwrite).

WHAT THIS IS
One row per GSA-inventoried building (owned or leased) from the Inventory of Owned and
Leased Properties (IOLP). The point analogue of `military_bases_lance`: instead of NTAD
installation polygons, this lands GSA building points with rooftop lat/lon. Every row carries
`Longitude`/`Latitude` (EPSG:4326) plus a POINT WKT, so proximity joins (haversine against an
entity HQ, or against a PoP centroid) work with zero further geocoding.

SOURCE (GSA IOLP ArcGIS Feature Service — layer 0 `FC_IOLP_BLDG`, updated weekly)
  https://services1.arcgis.com/eBupDfPlEJK3mdAm/arcgis/rest/services/IOLP_NEW/FeatureServer/0
  Paginated `query?where=1=1&outFields=*&f=json`. ~8.1k building points nationwide + territories.
  (Layer 1 `FC_IOLP_LEASE` is a lease-instrument grain, deliberately NOT ingested here.)

NORMALIZATION
  * Column names: snake_case projection of the verbatim IOLP field names (explicit map below).
  * Geometry: POINT(lon lat) WKT from the attribute lat/lon (falls back to the feature geometry).
    Lance has no native geometry type; consumers parse with ST_GeomFromText(geometry_wkt).
  * Numerics: longitude/latitude/rentable+vacant RSF → DOUBLE; walk/transit score → INT.
  * `owned_or_leased_indicator` is the raw IOLP code (F = federally owned, L = leased);
    `occupancy_right_desc` carries the human-readable OWNED/LEASED and is authoritative.

GRAIN: 1 row / location_code (idempotent snapshot-overwrite).
  BTREE(location_code, objectid, real_property_asset_name) · BITMAP(state_cd,
  owned_or_leased_indicator, occupancy_right_desc, region_code, building_status,
  real_property_asset_type).

    doppler run --project core-x --config prd -- python pipelines/serving/ingest_gsa_iolp_buildings.py
    doppler run --project core-x --config prd -- python pipelines/serving/ingest_gsa_iolp_buildings.py --verify
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys
import urllib.parse
import urllib.request

A = "s3://data-sink/active"
FEATURE_LAYER = ("https://services1.arcgis.com/eBupDfPlEJK3mdAm/arcgis/rest/services/"
                 "IOLP_NEW/FeatureServer/0")
SERVING_URI = os.environ.get("GSA_BUILDINGS_LANCE_URI", f"{A}/gsa_buildings_lance/")
DATA_STORAGE_VERSION = "2.1"
SOURCE_VERSION = "gsa_iolp_featureserver_fc_iolp_bldg"
PAGE = 2000  # ArcGIS transfer page; loop until exceededTransferLimit clears

BTREE_COLS = ["location_code", "objectid", "real_property_asset_name"]
BITMAP_COLS = ["state_cd", "owned_or_leased_indicator", "occupancy_right_desc",
               "region_code", "building_status", "real_property_asset_type"]

# IOLP field → normalized snake_case column. Explicit + auditable, like materialize_military_bases.
_COL_MAP = {
    "OBJECTID":                     "objectid",
    "LOCATION_CODE":                "location_code",
    "STREET_ADDRESS":               "street_address",
    "CITY":                         "city",
    "STATE_CD":                     "state_cd",
    "ZIPCODE5":                     "zipcode5",
    "YEAR_BUILT":                   "year_built",
    "CONGRESSIONAL_DISTRICT_CODE":  "congressional_district_code",
    "BLD_VACANT_RSF":               "bld_vacant_rsf",
    "BUILDING_RSF":                 "building_rsf",
    "BUILDING_STATUS":              "building_status",
    "OWNED_OR_LEASED_INDICATOR":    "owned_or_leased_indicator",
    "REGION_CODE":                  "region_code",
    "Occupancy_Right_Desc":         "occupancy_right_desc",
    "Longitude":                    "longitude",
    "Latitude":                     "latitude",
    "SENATOR1":                     "senator1",
    "SENATOR1_URL":                 "senator1_url",
    "SENATOR2":                     "senator2",
    "SENATOR2_URL":                 "senator2_url",
    "REPRESENTATIVE":               "representative",
    "REPRESENTATIVE_URL":           "representative_url",
    "WALK_SCORE":                   "walk_score",
    "TRANSIT_SCORE":                "transit_score",
    "General_POC":                  "general_poc",
    "General_POC_Email":            "general_poc_email",
    "Fed_POC":                      "fed_poc",
    "Fed_POC_Email":                "fed_poc_email",
    "General_POC_2":                "general_poc_2",
    "General_POC_2_Email":          "general_poc_2_email",
    "Real_Property_Asset_Name":     "real_property_asset_name",
    "Installation_Name":            "installation_name",
    "Real_Property_Asset_type":     "real_property_asset_type",
}
_FLOAT_COLS = {"longitude", "latitude", "bld_vacant_rsf", "building_rsf"}
_INT_COLS = {"objectid", "walk_score", "transit_score"}


def _r2_storage_options() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID.")
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": endpoint, "region": "auto"}


def _to_float(v) -> float | None:
    try:
        f = float(str(v).strip())
        return f
    except (ValueError, TypeError):
        return None


def _to_int(v) -> int | None:
    try:
        return int(float(str(v).strip()))
    except (ValueError, TypeError):
        return None


def _fetch_page(offset: int) -> dict:
    params = {
        "where": "1=1",
        "outFields": "*",
        "returnGeometry": "true",
        "outSR": "4326",
        "resultOffset": str(offset),
        "resultRecordCount": str(PAGE),
        "f": "json",
    }
    url = f"{FEATURE_LAYER}/query?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"User-Agent": "core-x-ingest/1.0"})
    with urllib.request.urlopen(req, timeout=120) as r:
        return json.loads(r.read().decode("utf-8"))


def _fetch_all() -> list[dict]:
    """Page the FeatureServer until the server stops flagging exceededTransferLimit."""
    feats: list[dict] = []
    offset = 0
    while True:
        page = _fetch_page(offset)
        if "error" in page:
            raise RuntimeError(f"ArcGIS error at offset {offset}: {page['error']}")
        batch = page.get("features", [])
        feats.extend(batch)
        print(f"  fetched {len(batch)} (offset {offset}) → total {len(feats)}")
        if not page.get("exceededTransferLimit") or not batch:
            break
        offset += len(batch)
    return feats


def build() -> dict:
    import lance
    import pyarrow as pa

    feats = _fetch_all()
    if not feats:
        raise RuntimeError("IOLP returned zero features")

    columns = list(_COL_MAP.values()) + ["geometry_wkt", "geometry_type",
                                         "source_version", "materialized_at"]
    arrays: dict[str, list] = {c: [] for c in columns}
    now = dt.datetime.now(dt.timezone.utc)
    point_geoms = 0
    for feat in feats:
        attrs = feat.get("attributes", {})
        rec: dict = {}
        for src, dst in _COL_MAP.items():
            v = attrs.get(src)
            if dst in _FLOAT_COLS:
                rec[dst] = _to_float(v)
            elif dst in _INT_COLS:
                rec[dst] = _to_int(v)
            else:
                s = "" if v is None else str(v).strip()
                rec[dst] = s or None
        # lat/lon: attribute-first, fall back to the point geometry
        lon, lat = rec["longitude"], rec["latitude"]
        if (lon is None or lat is None) and feat.get("geometry"):
            g = feat["geometry"]
            lon = lon if lon is not None else _to_float(g.get("x"))
            lat = lat if lat is not None else _to_float(g.get("y"))
            rec["longitude"], rec["latitude"] = lon, lat
        if lon is not None and lat is not None:
            rec["geometry_wkt"] = f"POINT ({lon} {lat})"
            rec["geometry_type"] = "Point"
            point_geoms += 1
        else:
            rec["geometry_wkt"] = None
            rec["geometry_type"] = None
        rec["source_version"] = SOURCE_VERSION
        rec["materialized_at"] = now
        for c in columns:
            arrays[c].append(rec.get(c))

    schema = pa.schema([
        pa.field("objectid", pa.int64()),
        pa.field("location_code", pa.string()),
        pa.field("street_address", pa.string()),
        pa.field("city", pa.string()),
        pa.field("state_cd", pa.string()),
        pa.field("zipcode5", pa.string()),
        pa.field("year_built", pa.string()),
        pa.field("congressional_district_code", pa.string()),
        pa.field("bld_vacant_rsf", pa.float64()),
        pa.field("building_rsf", pa.float64()),
        pa.field("building_status", pa.string()),
        pa.field("owned_or_leased_indicator", pa.string()),
        pa.field("region_code", pa.string()),
        pa.field("occupancy_right_desc", pa.string()),
        pa.field("longitude", pa.float64()),
        pa.field("latitude", pa.float64()),
        pa.field("senator1", pa.string()),
        pa.field("senator1_url", pa.string()),
        pa.field("senator2", pa.string()),
        pa.field("senator2_url", pa.string()),
        pa.field("representative", pa.string()),
        pa.field("representative_url", pa.string()),
        pa.field("walk_score", pa.int64()),
        pa.field("transit_score", pa.int64()),
        pa.field("general_poc", pa.string()),
        pa.field("general_poc_email", pa.string()),
        pa.field("fed_poc", pa.string()),
        pa.field("fed_poc_email", pa.string()),
        pa.field("general_poc_2", pa.string()),
        pa.field("general_poc_2_email", pa.string()),
        pa.field("real_property_asset_name", pa.string()),
        pa.field("installation_name", pa.string()),
        pa.field("real_property_asset_type", pa.string()),
        pa.field("geometry_wkt", pa.string()),
        pa.field("geometry_type", pa.string()),
        pa.field("source_version", pa.string(), nullable=False),
        pa.field("materialized_at", pa.timestamp("us", tz="UTC"), nullable=False),
    ])
    tbl = pa.table({c: arrays[c] for c in columns}, schema=schema)
    rows = tbl.num_rows
    print(f"parsed {rows:,} IOLP buildings (with point geometry: {point_geoms:,})")

    so = _r2_storage_options()
    lance.write_dataset(tbl, SERVING_URI, mode="overwrite",
                        data_storage_version=DATA_STORAGE_VERSION, storage_options=so)
    ds = lance.dataset(SERVING_URI, storage_options=so)
    present = set(ds.schema.names)
    for c in BTREE_COLS:
        if c in present:
            try:
                ds.create_scalar_index(c, index_type="BTREE"); print(f"  BTREE ✓ {c}")
            except Exception as exc:  # noqa: BLE001
                print(f"  WARN BTREE {c}: {exc}")
    for c in BITMAP_COLS:
        if c in present:
            try:
                ds.create_scalar_index(c, index_type="BITMAP"); print(f"  BITMAP ✓ {c}")
            except Exception as exc:  # noqa: BLE001
                print(f"  WARN BITMAP {c}: {exc}")
    back = ds.count_rows()
    assert back == rows, f"write-integrity gate: {back} != {rows}"
    print(f"WROTE {SERVING_URI} rows={back} cols={len(ds.schema)}")
    return {"uri": SERVING_URI, "rows": back, "point_geoms": point_geoms}


def verify() -> None:
    import duckdb
    import lance

    so = _r2_storage_options()
    ds = lance.dataset(SERVING_URI, storage_options=so)
    print(f"{SERVING_URI}  rows={ds.count_rows():,}  cols={len(ds.schema)}")
    print("indices:", sorted(
        (i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i))) for i in ds.list_indices()))
    con = duckdb.connect(":memory:"); con.register("b", ds)
    print("\n=== owned vs leased ===")
    print(con.execute("SELECT occupancy_right_desc, owned_or_leased_indicator, count(*) n "
                      "FROM b GROUP BY 1,2 ORDER BY 3 DESC").df().to_string(index=False))
    print("\n=== geometry + rsf coverage ===")
    print(con.execute("""SELECT
        count(*) total,
        count(*) FILTER (WHERE geometry_wkt IS NOT NULL) with_point,
        count(*) FILTER (WHERE building_rsf IS NOT NULL)  with_rsf,
        round(sum(building_rsf)/1e6, 1) total_rsf_millions
    FROM b""").df().to_string(index=False))
    print("\n=== top 12 states ===")
    print(con.execute("SELECT state_cd, count(*) n FROM b WHERE state_cd IS NOT NULL "
                      "GROUP BY 1 ORDER BY 2 DESC LIMIT 12").df().to_string(index=False))
    try:
        con.execute("INSTALL spatial; LOAD spatial;")
        r = con.execute("SELECT location_code, real_property_asset_name, "
                        "ST_GeometryType(ST_GeomFromText(geometry_wkt)) gtype "
                        "FROM b WHERE geometry_wkt IS NOT NULL LIMIT 1").df()
        print("\n=== spatial round-trip ===")
        print(r.to_string(index=False))
    except Exception as exc:  # noqa: BLE001
        print(f"WARN spatial round-trip: {exc}")


if __name__ == "__main__":
    (verify if "--verify" in sys.argv else build)()
