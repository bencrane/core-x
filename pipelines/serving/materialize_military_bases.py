"""Serving build — `military_bases_lance` (NTAD military installations with polygon geometry).

SoR  s3://data-sink/active/military_bases_lance/  (Lance v2.1; derived, snapshot-overwrite)

WHAT THIS IS
824 rows, one per NTAD-listed military installation. Joins the raw CSV (16 metadata cols)
with the raw GeoJSON (Polygon/MultiPolygon footprint) on OBJECTID. Geometry is promoted to
MultiPolygon uniformly and stored as WKT — DuckDB spatial reads via ST_GeomFromText.

RAW INPUTS (NTAD Military Bases distribution)
  CSV:     s3://data-sink/active/reference/military_bases/NTAD_Military_Bases_*.csv
  GeoJSON: s3://data-sink/active/reference/military_bases/NTAD_Military_Bases_*.geojson

NORMALIZATION
  * Geometry: every feature's geometry is coerced to MultiPolygon (single Polygons wrapped),
    serialized as WKT. Lance has no native geometry type; consumers parse with
    ST_GeomFromText(geometry_wkt). CRS is EPSG:4326 (WGS-84 lon/lat).
  * Booleans: 'yes' → true, 'no' → false (is_firrma_site, is_joint_base, controlled_unclassified_indicator).
  * Column names: snake_case projection of the verbatim CSV headers.
  * Numerics: shape_area, shape_length cast to DOUBLE.

GRAIN: 1 row / objectid. Idempotent snapshot-overwrite.

    doppler run --project core-x --config prd -- python pipelines/serving/materialize_military_bases.py
    doppler run --project core-x --config prd -- python pipelines/serving/materialize_military_bases.py --verify
"""
from __future__ import annotations

import csv
import io
import json
import os
import sys

A = "s3://data-sink/active"
RAW_PREFIX = f"{A}/reference/military_bases/"
SERVING_URI = os.environ.get("MILITARY_BASES_LANCE_URI", f"{A}/military_bases_lance/")
DATA_STORAGE_VERSION = "2.1"

BTREE_COLS = ["objectid", "site_name", "feature_name"]
BITMAP_COLS = ["country", "state_name_code", "operational_status",
               "site_reporting_component_code", "is_firrma_site", "is_joint_base"]

# CSV header → normalized snake_case column. Keeps the operator's mapping explicit + auditable.
_COL_MAP = {
    "OBJECTID":                                    "objectid",
    "Country":                                     "country",
    "Feature Description":                         "feature_description",
    "Feature Name":                                "feature_name",
    "Controlled Unclassified Information Indicator": "controlled_unclassified_indicator",
    "Is FIRRMA Site":                              "is_firrma_site",
    "Is Joint Base":                               "is_joint_base",
    "Media Identifier":                            "media_identifier",
    "Primary Key Identifier":                      "primary_key_identifier",
    "Globally Unique Identifier":                  "globally_unique_identifier",
    "Site Name":                                   "site_name",
    "Site Operational Status":                     "operational_status",
    "Site Reporting Component Code":               "site_reporting_component_code",
    "State Name Code":                             "state_name_code",
    "Shape__Area":                                 "shape_area",
    "Shape__Length":                               "shape_length",
}
_BOOL_COLS = {"is_firrma_site", "is_joint_base", "controlled_unclassified_indicator"}
_FLOAT_COLS = {"shape_area", "shape_length"}


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


def _r2_client():
    import boto3
    so = _r2_storage_options()
    return boto3.client("s3", endpoint_url=so["endpoint"],
                        aws_access_key_id=so["aws_access_key_id"],
                        aws_secret_access_key=so["aws_secret_access_key"], region_name="auto")


def _bool_or_none(v: str) -> bool | None:
    s = (v or "").strip().lower()
    if s == "yes":
        return True
    if s == "no":
        return False
    return None


def _to_int(v) -> int | None:
    try:
        return int(str(v).strip())
    except (ValueError, TypeError):
        return None


def _to_float(v) -> float | None:
    try:
        return float(str(v).strip())
    except (ValueError, TypeError):
        return None


def _polygon_to_multi_wkt(geom_dict: dict) -> str | None:
    """Promote any Polygon to a 1-element MultiPolygon, return WKT. MultiPolygon passes through.
    Returns None for empty/missing geometry — those rows can still land for metadata reference."""
    from shapely.geometry import Polygon, MultiPolygon, shape

    if not geom_dict:
        return None
    g = shape(geom_dict)
    if g.is_empty:
        return None
    if isinstance(g, Polygon):
        g = MultiPolygon([g])
    elif not isinstance(g, MultiPolygon):
        # NTAD ships only Polygon / MultiPolygon — bail loud if a feature is something else.
        raise RuntimeError(f"unexpected geometry type: {g.geom_type}")
    return g.wkt


def _find_raw_keys(s3) -> tuple[str, str]:
    """Locate the latest CSV + GeoJSON under the raw prefix. NTAD ships timestamped filenames."""
    resp = s3.list_objects_v2(Bucket="data-sink", Prefix=RAW_PREFIX.replace("s3://data-sink/", ""))
    csv_keys = sorted(o["Key"] for o in resp.get("Contents", []) if o["Key"].lower().endswith(".csv"))
    geo_keys = sorted(o["Key"] for o in resp.get("Contents", []) if o["Key"].lower().endswith(".geojson"))
    if not csv_keys:
        raise RuntimeError(f"no CSV under {RAW_PREFIX}")
    if not geo_keys:
        raise RuntimeError(f"no GeoJSON under {RAW_PREFIX}")
    return csv_keys[-1], geo_keys[-1]


def build() -> dict:
    import datetime as dt

    import lance
    import pyarrow as pa

    s3 = _r2_client()
    csv_key, geo_key = _find_raw_keys(s3)
    print(f"CSV:     s3://data-sink/{csv_key}")
    print(f"GeoJSON: s3://data-sink/{geo_key}")

    # ── CSV → metadata dict keyed by OBJECTID ──
    csv_body = s3.get_object(Bucket="data-sink", Key=csv_key)["Body"].read().decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(csv_body))
    meta: dict[int, dict] = {}
    for raw in reader:
        rec: dict = {}
        for src, dst in _COL_MAP.items():
            if dst in _BOOL_COLS:
                rec[dst] = _bool_or_none(raw.get(src, ""))
            elif dst in _FLOAT_COLS:
                rec[dst] = _to_float(raw.get(src))
            elif dst == "objectid":
                rec[dst] = _to_int(raw.get(src))
            else:
                v = (raw.get(src) or "").strip()
                rec[dst] = v or None
        if rec["objectid"] is not None:
            meta[rec["objectid"]] = rec
    print(f"CSV rows parsed: {len(meta)}")

    # ── GeoJSON → geometry WKT keyed by OBJECTID (feature.id) ──
    geo_body = s3.get_object(Bucket="data-sink", Key=geo_key)["Body"].read().decode("utf-8")
    fc = json.loads(geo_body)
    if fc.get("type") != "FeatureCollection":
        raise RuntimeError("GeoJSON root is not a FeatureCollection")
    geom: dict[int, str | None] = {}
    polygon_promoted = 0
    multi_passthrough = 0
    empty_geoms = 0
    for feat in fc["features"]:
        oid = _to_int(feat.get("id"))
        if oid is None:
            # NTAD also stamps OBJECTID inside properties as a fallback
            oid = _to_int((feat.get("properties") or {}).get("OBJECTID"))
        if oid is None:
            continue
        g_dict = feat.get("geometry")
        wkt = _polygon_to_multi_wkt(g_dict) if g_dict else None
        geom[oid] = wkt
        if wkt is None:
            empty_geoms += 1
        elif g_dict.get("type") == "Polygon":
            polygon_promoted += 1
        else:
            multi_passthrough += 1
    print(f"GeoJSON features parsed: {len(geom)}"
          f"  (Polygon→MultiPolygon promoted: {polygon_promoted},"
          f" MultiPolygon passthrough: {multi_passthrough}, empty: {empty_geoms})")

    # ── Join on OBJECTID. CSV is authoritative for the metadata grain; geometry is NULL if missing. ──
    joined: list[dict] = []
    geom_missing = 0
    meta_missing_for_geom = set(geom.keys()) - set(meta.keys())
    if meta_missing_for_geom:
        print(f"WARN: {len(meta_missing_for_geom)} geometry-only ids (no CSV row): "
              f"{sorted(list(meta_missing_for_geom))[:8]}...")
    for oid, m in meta.items():
        wkt = geom.get(oid)
        if wkt is None:
            geom_missing += 1
        row = dict(m)
        row["geometry_wkt"] = wkt
        row["geometry_type"] = "MultiPolygon" if wkt else None
        row["materialized_at"] = dt.datetime.now(dt.timezone.utc)
        joined.append(row)
    print(f"joined rows: {len(joined)}  (geometry missing for {geom_missing} csv rows)")
    if not joined:
        raise RuntimeError("no rows to write")

    # ── Build Arrow table with an explicit schema so types are stable across rebuilds. ──
    columns = (
        list(_COL_MAP.values())                              # 16 native columns from the CSV
        + ["geometry_wkt", "geometry_type", "materialized_at"]
    )
    arrays: dict[str, list] = {c: [] for c in columns}
    for r in joined:
        for c in columns:
            arrays[c].append(r.get(c))

    schema = pa.schema([
        pa.field("objectid", pa.int64(), nullable=False),
        pa.field("country", pa.string()),
        pa.field("feature_description", pa.string()),
        pa.field("feature_name", pa.string()),
        pa.field("controlled_unclassified_indicator", pa.bool_()),
        pa.field("is_firrma_site", pa.bool_()),
        pa.field("is_joint_base", pa.bool_()),
        pa.field("media_identifier", pa.string()),
        pa.field("primary_key_identifier", pa.string()),
        pa.field("globally_unique_identifier", pa.string()),
        pa.field("site_name", pa.string()),
        pa.field("operational_status", pa.string()),
        pa.field("site_reporting_component_code", pa.string()),
        pa.field("state_name_code", pa.string()),
        pa.field("shape_area", pa.float64()),
        pa.field("shape_length", pa.float64()),
        pa.field("geometry_wkt", pa.string()),
        pa.field("geometry_type", pa.string()),
        pa.field("materialized_at", pa.timestamp("us", tz="UTC"), nullable=False),
    ])
    tbl = pa.table({c: arrays[c] for c in columns}, schema=schema)
    rows = tbl.num_rows

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
    return {"uri": SERVING_URI, "rows": back,
            "polygon_promoted": polygon_promoted, "multi_passthrough": multi_passthrough,
            "geometry_missing": geom_missing}


def verify() -> None:
    import duckdb
    import lance

    so = _r2_storage_options()
    ds = lance.dataset(SERVING_URI, storage_options=so)
    print(f"{SERVING_URI}  rows={ds.count_rows():,}  cols={len(ds.schema)}")
    print("indices:", sorted(
        (i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i))) for i in ds.list_indices()))
    con = duckdb.connect(":memory:"); con.register("mb", ds)
    print("\n=== operational_status mix ===")
    print(con.execute("SELECT operational_status, count(*) n FROM mb GROUP BY 1 ORDER BY 2 DESC"
                      ).df().to_string(index=False))
    print("\n=== firrma / joint_base flags ===")
    print(con.execute("""SELECT
        count(*) total,
        count(*) FILTER (WHERE is_firrma_site)  firrma_sites,
        count(*) FILTER (WHERE is_joint_base)   joint_bases,
        count(*) FILTER (WHERE geometry_wkt IS NOT NULL) with_geometry,
        count(*) FILTER (WHERE geometry_wkt IS NULL) no_geometry
    FROM mb""").df().to_string(index=False))
    print("\n=== top 12 states by count ===")
    print(con.execute("SELECT state_name_code, count(*) n FROM mb WHERE state_name_code IS NOT NULL "
                      "GROUP BY 1 ORDER BY 2 DESC LIMIT 12").df().to_string(index=False))
    # Spatial sanity: try to parse one geometry via DuckDB spatial to confirm WKT round-trips.
    try:
        con.execute("INSTALL spatial; LOAD spatial;")
        r = con.execute("SELECT objectid, site_name, ST_GeometryType(ST_GeomFromText(geometry_wkt)) AS gtype "
                        "FROM mb WHERE geometry_wkt IS NOT NULL LIMIT 1").df()
        print("\n=== spatial round-trip ===")
        print(r.to_string(index=False))
    except Exception as exc:  # noqa: BLE001
        print(f"WARN spatial round-trip: {exc}")


if __name__ == "__main__":
    (verify if "--verify" in sys.argv else build)()
