"""Serving build — `govcon_firm_military_proximity` (firms × nearby active military bases).

SoR  s3://data-sink/active/govcon_firm_military_proximity/  (Lance v2.1; derived, snapshot-overwrite)

WHAT THIS IS
The universal proximity funnel: every domain-having firm in `company_addresses` cross-joined
to every active NTAD military base whose polygon footprint sits within ~50 miles (0.72 degrees
WGS-84) of the firm's ZCTA centroid. One row per (firm × base) match. Powers the
Equipment-Rental / Material-Supplier / Physical-Security GTM scrape cohorts: filter by NAICS
to slice the universe.

UPSTREAMS
  IDENTITY  company_addresses      grain 1/entity (uei or 'dom:' + domain_norm); filtered
                                    to domain_norm IS NOT NULL (scrapeable cohort)
  GEOCODE   zcta_zip_centroids     winner_postal_code = zcta5 → lat/lon
  BASES     military_bases_lance   operational_status = 'act'; geometry_wkt parsed via
                                    ST_GeomFromText (MultiPolygon)

SPATIAL JOIN: ST_DWithin(ST_Point(firm_lon, firm_lat), ST_GeomFromText(mb.geometry_wkt), 0.72).
0.72 degrees ≈ 50 mi at mid-latitudes. WGS-84 EPSG:4326 on both sides.

GRAIN: 1 row / (entity_key × base_objectid). Idempotent snapshot-overwrite.

    doppler run --project core-x --config prd -- python pipelines/serving/materialize_govcon_firm_military_proximity.py
    doppler run --project core-x --config prd -- python pipelines/serving/materialize_govcon_firm_military_proximity.py --verify
"""
from __future__ import annotations

import os
import sys

A = "s3://data-sink/active"
CA = f"{A}/company_addresses/"
ZCTA = f"{A}/zcta_zip_centroids/"
MB = f"{A}/military_bases_lance/"
SERVING_URI = os.environ.get("GOVCON_FIRM_MILITARY_PROXIMITY_URI",
                             f"{A}/govcon_firm_military_proximity/")
DATA_STORAGE_VERSION = "2.1"
DUCK_MEM = os.environ.get("DUCK_MEM", "12GB")
RADIUS_DEG = float(os.environ.get("RADIUS_DEG", "0.72"))   # ≈ 50 mi at mid-latitudes

BTREE_COLS = ["entity_key", "uei", "domain_norm", "primary_naics", "base_objectid",
              "legal_business_name", "base_site_name"]
BITMAP_COLS = ["winner_state", "base_state_code", "base_site_reporting_component_code",
               "base_is_firrma_site", "base_is_joint_base"]


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


def build() -> dict:
    import duckdb
    import lance

    so = _r2_storage_options()
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=8")
    con.execute(f"SET memory_limit='{DUCK_MEM}'")
    con.execute("INSTALL spatial; LOAD spatial;")
    con.register("ca",  lance.dataset(CA,   storage_options=so))
    con.register("z",   lance.dataset(ZCTA, storage_options=so))
    con.register("mb",  lance.dataset(MB,   storage_options=so))

    # ── Cohort: scrapeable firms with a usable postal centroid. domain_norm IS NOT NULL
    # because the scrape agent needs a domain to fetch. Inner-join to ZCTA on postal_code
    # to anchor a lat/lon for the spatial check. ──
    con.execute("""
        CREATE TEMP TABLE firms AS
        SELECT ca.entity_key, ca.uei, ca.domain_norm, ca.legal_business_name, ca.primary_naics,
               ca.winner_city, ca.winner_state, ca.winner_postal_code,
               z.lat AS firm_lat, z.lon AS firm_lon
        FROM ca
        JOIN z ON ca.winner_postal_code = z.zcta5
        WHERE ca.domain_norm IS NOT NULL
    """)
    nf = con.execute("SELECT count(*) FROM firms").fetchone()[0]
    print(f"firms with domain_norm + zcta centroid: {nf:,}")

    # ── Active bases with parsed geometry. Materialize ST_GeomFromText once so the spatial
    # join doesn't reparse 791 WKT strings per firm. ──
    con.execute("""
        CREATE TEMP TABLE bases AS
        SELECT objectid AS base_objectid,
               site_name AS base_site_name,
               feature_name AS base_feature_name,
               state_name_code AS base_state_code,
               site_reporting_component_code AS base_site_reporting_component_code,
               is_firrma_site AS base_is_firrma_site,
               is_joint_base AS base_is_joint_base,
               ST_GeomFromText(geometry_wkt) AS base_geom
        FROM mb
        WHERE operational_status = 'act' AND geometry_wkt IS NOT NULL
    """)
    nb = con.execute("SELECT count(*) FROM bases").fetchone()[0]
    print(f"active bases with geometry: {nb}")

    # ── Spatial proximity join. ST_DWithin on geographic-degree distance (≈ 50mi at 0.72°). ──
    con.execute(f"""
        CREATE TEMP TABLE m AS
        SELECT f.*,
               b.base_objectid, b.base_site_name, b.base_feature_name,
               b.base_state_code, b.base_site_reporting_component_code,
               b.base_is_firrma_site, b.base_is_joint_base,
               now() AS materialized_at
        FROM firms f
        JOIN bases b ON ST_DWithin(ST_Point(f.firm_lon, f.firm_lat), b.base_geom, {RADIUS_DEG})
    """)
    rows = con.execute("SELECT count(*) FROM m").fetchone()[0]
    print(f"proximity matches: {rows:,}")

    tbl = con.execute("SELECT * FROM m").fetch_arrow_table()
    assert tbl.num_rows == rows

    lance.write_dataset(tbl, SERVING_URI, mode="overwrite",
                        data_storage_version=DATA_STORAGE_VERSION, storage_options=so)
    del tbl
    con.close()

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
    print(f"WROTE {SERVING_URI} rows={back} cols={len(ds.schema)}")
    return {"uri": SERVING_URI, "rows": back}


def verify() -> None:
    import duckdb
    import lance

    so = _r2_storage_options()
    ds = lance.dataset(SERVING_URI, storage_options=so)
    print(f"{SERVING_URI}  rows={ds.count_rows():,}  cols={len(ds.schema)}")
    print("indices:", sorted(
        (i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i))) for i in ds.list_indices()))
    con = duckdb.connect(":memory:"); con.register("d", ds)

    print("\n=== distinct firms × distinct bases ===")
    print(con.execute("""SELECT count(*)                              AS rows,
        count(DISTINCT entity_key)                                    AS distinct_firms,
        count(DISTINCT domain_norm)                                   AS distinct_domains,
        count(DISTINCT base_objectid)                                 AS distinct_bases
        FROM d""").df().to_string(index=False))

    print("\n=== validation cohort counts (operator-specified NAICS bundles) ===")
    er = ("532412","532490","532420","532120","532310","532411")
    sm = ("327320","324121","332312","327211")
    er_in = "(" + ",".join(f"'{c}'" for c in er) + ")"
    sm_in = "(" + ",".join(f"'{c}'" for c in sm) + ")"
    print(f"\n  Equipment-Rental cohort NAICS {er}:")
    print(con.execute(f"""SELECT
        count(*) AS rows,
        count(DISTINCT entity_key) AS distinct_firms,
        count(DISTINCT domain_norm) AS distinct_domains
        FROM d WHERE primary_naics IN {er_in}""").df().to_string(index=False))
    print(f"\n  Specialty-Building-Materials cohort NAICS {sm}:")
    print(con.execute(f"""SELECT
        count(*) AS rows,
        count(DISTINCT entity_key) AS distinct_firms,
        count(DISTINCT domain_norm) AS distinct_domains
        FROM d WHERE primary_naics IN {sm_in}""").df().to_string(index=False))


if __name__ == "__main__":
    (verify if "--verify" in sys.argv else build)()
