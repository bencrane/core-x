"""Serving build — `govcon_equipment_rental_construction_match` (geo-matched named-account target list).

SoR  s3://data-sink/active/govcon_equipment_rental_construction_match/  (Lance v2.1; derived, snapshot-overwrite).

WHAT THIS IS
One row per (construction-prime award × equipment-rental firm) pair within drive-radius — the
operational target list behind the GTM inference: active construction primes matched to the
equipment-rental firms physically able to serve them.

DEMAND IS UNGATED. Every active construction job needs equipment rental regardless of prime size
or whether it filed a subcontracting plan, so the match does NOT filter on business_size or
has_subcontracting_plan — those are CARRIED COLUMNS for optional filter-on-top, never gates.
(Gating on them would shrink demand from ~5,987 active construction awards to ~376 — a compliance
artifact, not rental demand.)

SIDES (all reliable; NO FSRS subaward-propensity dependency)
  DEMAND  govcon_active_awards   naics LIKE '23%' · active_potential  — ALL active construction primes,
          any size, plan or not. business_size + has_subcontracting_plan carried as columns, not gates.
          (~5,987 active construction awards / 2,759 primes / $91B; PoP worksite zip)
  SUPPLY  sam_master_entities    equip-rental NAICS bundle · is_active · USA
          (~8,431 geocodable firms; HQ/yard zip)
  GEO     zcta_zip_centroids (primary) + geocode_xwalk zip5-rollup (fallback) + ZIP3-prefix
          centroid (last-resort, closes military/point-zip gaps e.g. 35898 Redstone Arsenal)

DESIGNATIONS: each pair carries the rental firm's socioeconomic designation flags (sub_sdvosb,
  sub_wosb, sub_hubzone, sub_8a, …, sub_any_designation), decoded from the firm's SAM Reps & Certs
  via the validated crosswalk in sam_business_type_code_dict — SAM current-registry lineage, NOT the
  FPDS award-stamped flags. Lets the frontend filter "SDVOSB rental firms within 50mi of this award"
  zero-join. 8(a)/HUBZone/EDWOSB are floors (SBA-cert string ~13% populated, ~68% recall).

DISTANCE  centroid-to-centroid haversine × 1.3 road-circuity factor. Tier:
          local ≤50mi · regional ≤150mi · (pairs >150mi road are dropped).

KNOWN LIMITS (ship with the data)
  - supply_addr_is_hq_pin: SAM carries ONE registered HQ. National chains (United Rentals, Sunbelt,
    Herc…) register a corporate HQ, not their branch yards → HQ-radius match is advisory for them,
    reliable for single-location/regional firms.
  - PoP zip is worksite-grade (can be a base/installation centroid), so radius is metro-accurate,
    not parcel/drive-time-precise. The ×1.3 factor approximates roads; not a routing engine.
  - centroid_source flags zcta vs geocode_xwalk vs zip3 so coverage + precision is visible.
    'zip3' rows resolved via the 3-digit-prefix (SCF/metro) centroid because the exact zip5 is
    a point/military zip absent from both ZCTA and the rooftop cache — metro-grade, not zip5-grade.

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
SMD = f"{A}/sam_master_domains/"   # canonical normalized entity_url → domain (blocklist-filtered)
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

# Rental-firm socioeconomic designations, decoded from the firm's SAM Reps & Certs via the
# validated crosswalk in sam_business_type_code_dict (SAM current-registry lineage — distinct
# from the FPDS award-stamped flags on govcon_active_awards). 8(a)/HUBZone/EDWOSB are floors
# (SBA-cert string ~13% populated); the business_types self-certs have no recall ceiling.
SUB_DESIG = ["sub_sdvosb", "sub_veteran_owned", "sub_wosb", "sub_edwosb", "sub_woman_owned",
             "sub_hubzone", "sub_8a", "sub_self_cert_sdb", "sub_minority_owned", "sub_jv_wosb",
             "sub_any_designation"]

# contract_award_unique_key is a long ~40-char string with only ~5,415 distinct values repeated
# ~232×/row across 1.26M rows — a BITMAP profile (equality pushdown for "firms for this award"),
# not BTREE. A BTREE external-sort over 1.26M long strings exhausts Lance's index-sorter pool.
BTREE_COLS = ["sub_uei", "prime_uei", "road_miles", "award_value"]
BITMAP_COLS = ["contract_award_unique_key", "tier", "supply_addr_is_hq_pin", "sub_state",
               "pop_state_code", "supply_centroid_source", "demand_centroid_source",
               "business_size", "has_subcontracting_plan"] + SUB_DESIG


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
    con.register("smd", lance.dataset(SMD, storage_options=so))
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
    # ── tier-3 fallback: ZIP3-prefix centroid (avg of all ZCTAs sharing the 3-digit prefix) ──
    # Closes military/point-zip gaps (e.g. 35898 Redstone Arsenal — its 14 active MILCON awards) that
    # are absent from BOTH ZCTA and the rooftop cache. SCF/metro-grade, consistent with the existing
    # centroid-to-centroid radius precision. Consumers coalesce(exact, zip3) and tag src='zip3'.
    con.execute("CREATE TEMP TABLE zip3 AS "
                "SELECT substr(zcta5, 1, 3) AS z3, avg(lat) AS lat, avg(lon) AS lon FROM zcta GROUP BY 1")

    # ── DEMAND: construction primes w/ plan, geocoded ──
    con.execute(f"""
        CREATE TEMP TABLE demand AS
        SELECT g.contract_award_unique_key, g.recipient_uei AS prime_uei, g.recipient_name AS prime_name,
               g.naics_code AS award_naics_code, g.naics_description AS award_naics_desc,
               g.current_total_value_of_award AS award_value,
               g.business_size, g.has_subcontracting_plan,
               g.awarding_agency_name, g.pop_city, g.pop_state_code, left(g.pop_zip,5) AS pop_zip5,
               coalesce(c.lat, z3.lat) AS d_lat, coalesce(c.lon, z3.lon) AS d_lon,
               coalesce(c.src, 'zip3') AS demand_centroid_source
        FROM gaa g
        LEFT JOIN cent c ON left(g.pop_zip,5) = c.zip5
        LEFT JOIN zip3 z3 ON substr(left(g.pop_zip,5), 1, 3) = z3.z3
        WHERE g.naics_code LIKE '23%' AND g.active_potential
          AND g.pop_zip IS NOT NULL AND length(trim(g.pop_zip)) >= 5
          AND coalesce(c.lat, z3.lat) IS NOT NULL
    """)

    # one normalized domain per uei (sam_master_domains is the canonical, blocklist-filtered
    # entity_url→domain index; collapse to 1/uei so the join can't fan out the match grain).
    con.execute("CREATE TEMP TABLE dom AS "
                "SELECT uei, min(normalized_domain) AS sub_website FROM smd GROUP BY uei")

    # ── SUPPLY: US-active equip-rental firms, geocoded, + SAM Reps & Certs designation flags ──
    # bt = self-cert business_types list; sbap = 2-char SBA-cert prefixes (date-suffix stripped).
    bt, sbap = "coalesce(m.business_types, [])", (
        "list_filter(list_transform(string_split(coalesce(m.sba_business_types_string,''),'~'),"
        "x -> substr(trim(x),1,2)), e -> e <> '')")
    con.execute(f"""
        CREATE TEMP TABLE supply AS
        WITH s0 AS (
          SELECT m.uei AS sub_uei, m.legal_business_name AS sub_name, m.primary_naics AS sub_primary_naics,
                 m.physical_address_city AS sub_city, m.physical_address_province_or_state AS sub_state,
                 m.physical_address_zip_postal_code AS sub_zip5,
                 ({hq}) AS supply_addr_is_hq_pin,
                 coalesce(c.lat, z3.lat) AS s_lat, coalesce(c.lon, z3.lon) AS s_lon,
                 coalesce(c.src, 'zip3') AS supply_centroid_source,
                 {bt} AS bt, {sbap} AS sbap
          FROM sme m
          LEFT JOIN cent c ON m.physical_address_zip_postal_code = c.zip5
          LEFT JOIN zip3 z3 ON substr(m.physical_address_zip_postal_code, 1, 3) = z3.z3
          WHERE (m.primary_naics IN {inb} OR len(list_filter(coalesce(m.naics_codes,[]), x -> x IN {inb})) > 0)
            AND m.is_active AND m.physical_address_country_code = 'USA'
            AND coalesce(c.lat, z3.lat) IS NOT NULL
        )
        SELECT s0.* EXCLUDE (bt, sbap), dom.sub_website,
          list_contains(bt,'QF')                                                       AS sub_sdvosb,
          (list_contains(bt,'A5') OR list_contains(bt,'QF'))                           AS sub_veteran_owned,
          (list_contains(bt,'8W') OR list_contains(sbap,'A9') OR list_contains(sbap,'A0')) AS sub_wosb,
          list_contains(sbap,'A0')                                                     AS sub_edwosb,
          (list_contains(bt,'A2') OR list_contains(bt,'8W')
             OR list_contains(sbap,'A9') OR list_contains(sbap,'A0'))                  AS sub_woman_owned,
          list_contains(sbap,'XX')                                                     AS sub_hubzone,
          list_contains(sbap,'A6')                                                     AS sub_8a,
          list_contains(bt,'27')                                                       AS sub_self_cert_sdb,
          list_contains(bt,'23')                                                       AS sub_minority_owned,
          list_contains(bt,'8C')                                                       AS sub_jv_wosb,
          (list_contains(bt,'QF') OR list_contains(bt,'A5') OR list_contains(bt,'8W')
             OR list_contains(bt,'A2') OR list_contains(bt,'27') OR list_contains(bt,'23')
             OR list_contains(bt,'8C') OR list_contains(sbap,'A6') OR list_contains(sbap,'XX')
             OR list_contains(sbap,'A9') OR list_contains(sbap,'A0'))                  AS sub_any_designation
        FROM s0 LEFT JOIN dom ON s0.sub_uei = dom.uei
    """)

    nd = con.execute("SELECT count(*) FROM demand").fetchone()[0]
    ns = con.execute("SELECT count(*) FROM supply").fetchone()[0]
    print(f"geocoded demand={nd}  supply={ns}")

    # ── MATCH: haversine × road factor, ≤ regional cutoff ──
    sub_desig_sel = ", ".join("s." + c for c in SUB_DESIG)
    hav = ("3958.8*2*asin(sqrt(pow(sin(radians(s.s_lat-d.d_lat)/2),2)"
           "+cos(radians(d.d_lat))*cos(radians(s.s_lat))*pow(sin(radians(s.s_lon-d.d_lon)/2),2)))")
    con.execute(f"""
        CREATE TEMP TABLE m AS
        SELECT d.contract_award_unique_key, d.prime_uei, d.prime_name, d.award_naics_code,
               d.award_naics_desc, d.award_value, d.business_size, d.has_subcontracting_plan,
               d.awarding_agency_name,
               d.pop_city, d.pop_state_code, d.pop_zip5, d.demand_centroid_source,
               s.sub_uei, s.sub_name, s.sub_primary_naics, s.sub_city, s.sub_state, s.sub_zip5,
               s.sub_website, s.supply_addr_is_hq_pin, s.supply_centroid_source,
               {sub_desig_sel},
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
    # Free the in-memory Arrow table + DuckDB pool before indexing — Lance's index sorter has its
    # own bounded pool and a 1.26M-row table held in memory starves the high-cardinality BTREE build.
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
