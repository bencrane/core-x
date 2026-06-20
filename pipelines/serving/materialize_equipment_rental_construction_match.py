"""Serving build — `govcon_equipment_rental_construction_match` (geo-matched named-account target list).

SoR  s3://data-sink/active/govcon_equipment_rental_construction_match/  (Lance v2.1; derived, snapshot-overwrite).

WHAT THIS IS
One row per (construction-prime award × equipment-rental firm) pair within drive-radius — the
operational target list behind the GTM inference: active large construction primes WITH
subcontracting plans, matched to the equipment-rental firms physically able to serve them.

SIDES (all reliable; NO FSRS subaward-propensity dependency)
  DEMAND  govcon_active_awards   naics LIKE '23%' · large · has_subcontracting_plan · active_potential
          (~376 awards / 164 primes / $57B; PoP worksite zip)
  SUPPLY  sam_master_entities    equip-rental NAICS bundle · is_active · USA
          (~8,431 geocodable firms; HQ/yard zip)
  GEO     zcta_zip_centroids (primary, full coverage) + geocode_xwalk zip5-rollup (fallback)

DISTANCE  centroid-to-centroid haversine × 1.3 road-circuity factor. Tier:
          local ≤50mi · regional ≤150mi · (pairs >150mi road are dropped).

KNOWN LIMITS (ship with the data)
  - supply_addr_is_hq_pin: SAM carries ONE registered HQ. National chains (United Rentals, Sunbelt,
    Herc…) register a corporate HQ, not their branch yards → HQ-radius match is advisory for them,
    reliable for single-location/regional firms.
  - PoP zip is worksite-grade (can be a base/installation centroid), so radius is metro-accurate,
    not parcel/drive-time-precise. The ×1.3 factor approximates roads; not a routing engine.
  - centroid_source flags zcta vs geocode_xwalk so coverage is visible.

Grain: 1 row / (contract_award_unique_key, sub_uei). Idempotent snapshot-overwrite.

    doppler run --project core-x --config prd -- python pipelines/serving/materialize_equipment_rental_construction_match.py
    doppler run --project core-x --config prd -- python pipelines/serving/materialize_equipment_rental_construction_match.py --verify
"""
from __future__ import annotations

import os
import sys

A = "s3://data-sink/active"
GAA = f"{A}/govcon_active_awards/"
SME = f"{A}/sam_master_entities/"
ZCTA = f"{A}/zcta_zip_centroids/"
GX = f"{A}/geocode_xwalk/"
SERVING_URI = os.environ.get("EQUIP_RENTAL_MATCH_URI", f"{A}/govcon_equipment_rental_construction_match/")
DATA_STORAGE_VERSION = "2.1"
DUCK_MEM = os.environ.get("DUCK_MEM", "14GB")

BUNDLE = ("532412", "532490", "532310", "532120")
ROAD_FACTOR = 1.3
LOCAL_MI = 50.0
REGIONAL_MI = 150.0   # road-miles cutoff (pairs beyond are dropped)

# National multi-branch chains: SAM HQ pin does not represent their delivery yards.
HQ_CHAINS = ["UNITED RENTAL", "SUNBELT RENTAL", "SUNBELT", "HERC RENTAL", "HERC ",
             "AHERN RENTAL", "H&E EQUIPMENT", "H & E EQUIPMENT", "NEFF ", "BLUELINE",
             "BLUE LINE RENTAL", "NES RENTAL", "BIGRENTZ", "RING POWER", "CAT RENTAL",
             "CATERPILLAR", "HOME DEPOT", "UNITED SITE SERVICES", "WILLSCOT", "MOBILE MINI"]

BTREE_COLS = ["sub_uei", "contract_award_unique_key", "prime_uei", "road_miles", "award_value"]
BITMAP_COLS = ["tier", "supply_addr_is_hq_pin", "sub_state", "pop_state_code",
               "supply_centroid_source", "demand_centroid_source"]


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
    con.register("gaa", lance.dataset(GAA, storage_options=so))
    con.register("sme", lance.dataset(SME, storage_options=so))
    con.register("zcta", lance.dataset(ZCTA, storage_options=so))
    con.register("gx", lance.dataset(GX, storage_options=so))
    inb = "(" + ",".join(f"'{c}'" for c in BUNDLE) + ")"
    hq = " OR ".join(f"upper(legal_business_name) LIKE '%{c}%'" for c in HQ_CHAINS)

    # ── unified zip5 → centroid: ZCTA primary, geocode_xwalk rollup fallback ──
    con.execute("""
        CREATE TEMP TABLE gxr AS
        SELECT zip5, avg(latitude) lat, avg(longitude) lon FROM gx
        WHERE length(trim(zip5))=5 AND latitude BETWEEN 17 AND 72 AND longitude BETWEEN -180 AND -64
        GROUP BY zip5
    """)
    con.execute("""
        CREATE TEMP TABLE cent AS
        SELECT zcta5 AS zip5, lat, lon, 'zcta' AS src FROM zcta
        UNION ALL
        SELECT zip5, lat, lon, 'geocode_xwalk' AS src FROM gxr WHERE zip5 NOT IN (SELECT zcta5 FROM zcta)
    """)

    # ── DEMAND: construction primes w/ plan, geocoded ──
    con.execute(f"""
        CREATE TEMP TABLE demand AS
        SELECT g.contract_award_unique_key, g.recipient_uei AS prime_uei, g.recipient_name AS prime_name,
               g.naics_code AS award_naics_code, g.naics_description AS award_naics_desc,
               g.current_total_value_of_award AS award_value,
               g.awarding_agency_name, g.pop_city, g.pop_state_code, left(g.pop_zip,5) AS pop_zip5,
               c.lat AS d_lat, c.lon AS d_lon, c.src AS demand_centroid_source
        FROM gaa g JOIN cent c ON left(g.pop_zip,5) = c.zip5
        WHERE g.naics_code LIKE '23%' AND g.business_size NOT LIKE 'SMALL%'
          AND g.has_subcontracting_plan AND g.active_potential
          AND g.pop_zip IS NOT NULL AND length(trim(g.pop_zip)) >= 5
    """)

    # ── SUPPLY: US-active equip-rental firms, geocoded ──
    con.execute(f"""
        CREATE TEMP TABLE supply AS
        SELECT m.uei AS sub_uei, m.legal_business_name AS sub_name, m.primary_naics AS sub_primary_naics,
               m.physical_address_city AS sub_city, m.physical_address_province_or_state AS sub_state,
               m.physical_address_zip_postal_code AS sub_zip5,
               ({hq}) AS supply_addr_is_hq_pin,
               c.lat AS s_lat, c.lon AS s_lon, c.src AS supply_centroid_source
        FROM sme m JOIN cent c ON m.physical_address_zip_postal_code = c.zip5
        WHERE (m.primary_naics IN {inb} OR len(list_filter(coalesce(m.naics_codes,[]), x -> x IN {inb})) > 0)
          AND m.is_active AND m.physical_address_country_code = 'USA'
    """)

    nd = con.execute("SELECT count(*) FROM demand").fetchone()[0]
    ns = con.execute("SELECT count(*) FROM supply").fetchone()[0]
    print(f"geocoded demand={nd}  supply={ns}")

    # ── MATCH: haversine × road factor, ≤ regional cutoff ──
    hav = ("3958.8*2*asin(sqrt(pow(sin(radians(s.s_lat-d.d_lat)/2),2)"
           "+cos(radians(d.d_lat))*cos(radians(s.s_lat))*pow(sin(radians(s.s_lon-d.d_lon)/2),2)))")
    con.execute(f"""
        CREATE TEMP TABLE m AS
        SELECT d.contract_award_unique_key, d.prime_uei, d.prime_name, d.award_naics_code,
               d.award_naics_desc, d.award_value, d.awarding_agency_name,
               d.pop_city, d.pop_state_code, d.pop_zip5, d.demand_centroid_source,
               s.sub_uei, s.sub_name, s.sub_primary_naics, s.sub_city, s.sub_state, s.sub_zip5,
               s.supply_addr_is_hq_pin, s.supply_centroid_source,
               round({hav}, 1) AS straight_miles,
               round({hav} * {ROAD_FACTOR}, 1) AS road_miles,
               CASE WHEN {hav} * {ROAD_FACTOR} <= {LOCAL_MI} THEN 'local'
                    WHEN {hav} * {ROAD_FACTOR} <= {REGIONAL_MI} THEN 'regional' END AS tier
        FROM demand d JOIN supply s ON {hav} * {ROAD_FACTOR} <= {REGIONAL_MI}
    """)
    tbl = con.execute("SELECT * FROM m").fetch_arrow_table()
    rows = tbl.num_rows
    pairs = con.execute("""SELECT count(DISTINCT contract_award_unique_key), count(DISTINCT sub_uei),
        count(*) FILTER(WHERE tier='local') FROM m""").fetchone()
    print(f"match rows={rows:,}  awards_covered={pairs[0]}  firms_covered={pairs[1]}  local_pairs={pairs[2]:,}")
    assert rows > 0, "no matches produced"

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
    return {"uri": SERVING_URI, "rows": back, "awards_covered": pairs[0], "firms_covered": pairs[1]}


def verify() -> None:
    import duckdb
    import lance

    so = _r2_storage_options()
    ds = lance.dataset(SERVING_URI, storage_options=so)
    print(f"{SERVING_URI}  rows={ds.count_rows():,}  cols={len(ds.schema)}")
    print("indices:", sorted(
        (i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i))) for i in ds.list_indices()))
    con = duckdb.connect(":memory:"); con.register("d", ds)
    print(con.execute("""SELECT tier, count(*) pairs, count(DISTINCT contract_award_unique_key) awards,
        count(DISTINCT sub_uei) firms, count(*) FILTER(WHERE supply_addr_is_hq_pin) hq_pin_pairs
        FROM d GROUP BY 1 ORDER BY 1""").df().to_string(index=False))
    print("\ncentroid source mix:")
    print(con.execute("SELECT demand_centroid_source, supply_centroid_source, count(*) n FROM d GROUP BY 1,2 ORDER BY 3 DESC").df().to_string(index=False))


if __name__ == "__main__":
    (verify if "--verify" in sys.argv else build)()
