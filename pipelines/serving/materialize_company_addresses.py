"""Serving build — `company_addresses` (consolidated firm mailing addresses, multi-source).

SoR  s3://data-sink/active/company_addresses/  (Lance v2.1; derived, snapshot-overwrite)

WHAT THIS IS
UNION universe across SAM + Prospeo + Blitz. Grain is one row per firm identified by either a SAM
UEI or (when not in SAM) a normalized domain. Each row carries the WINNING mailing address —
selected by source precedence SAM physical > SAM mailing > Prospeo > Blitz — plus every per-source
raw address kept side-by-side so consumers can override the winner. Bridges via `domain_norm` (the
canonical key everything else joins on: firmographics_blitz, equipment_catalog, industries_served,
equipment_provider, prospeo_company_export). Built so any downstream answering "where is this firm
physically located" can hit one Lance instead of coalescing across three.

UNIVERSE (3 record sources, deduped by domain_norm)
  1. Every UEI in sam_master_entities                                          (~1.54M rows; uei + maybe domain_norm)
  2. Every Prospeo domain NOT already mapped to a SAM UEI                       (non-SAM-Prospeo orphans; uei=NULL)
  3. Every Blitz domain NOT in SAM AND NOT in Prospeo                            (non-SAM-non-Prospeo Blitz orphans; uei=NULL)

PRIMARY KEY  `entity_key` = uei when SAM-resolved, else 'dom:' + domain_norm. SAM rows carry both.

PRECEDENCE (operator standard — do not reorder without sign-off)
  1. SAM physical address (street + line2 + city + state + zip + country + congressional district)
  2. SAM mailing address  (when physical is empty)
  3. Prospeo               (city/state/country + free-text company_raw_address)
  4. Blitz                 (city/state — no street, no postal; floor of last resort)

The winner is selected as a WHOLE address — no field-level mixing across sources. If SAM physical
city is present then every winner field comes from SAM physical, never a Prospeo state grafted onto
a SAM city. Source provenance is stamped in `address_source` so downstream can pick differently.

SIDES
  IDENTITY  sam_master_entities       grain (~1.5M UEIs); legal_business_name + primary_naics
  DOMAIN    sam_master_domains        canonical normalized entity_url → domain (1 row / uei after rollup)
  PROSPEO   prospeo_company_export    domain-keyed; ~49 rows in the current export (very sparse coverage)
  BLITZ     firmographics_blitz       domain_norm → hq_city/hq_state/hq_region (no street/postal)

GRAIN: 1 row / uei. Idempotent snapshot-overwrite.

    doppler run --project core-x --config prd -- python pipelines/serving/materialize_company_addresses.py
    doppler run --project core-x --config prd -- python pipelines/serving/materialize_company_addresses.py --verify
"""
from __future__ import annotations

import os
import sys

A = "s3://data-sink/active"
SME = f"{A}/sam_master_entities/"
SMD = f"{A}/sam_master_domains/"
PRO = f"{A}/prospeo_company_export/"
BLZ = f"{A}/firmographics_blitz/"
SERVING_URI = os.environ.get("COMPANY_ADDRESSES_URI", f"{A}/company_addresses/")
DATA_STORAGE_VERSION = "2.1"
DUCK_MEM = os.environ.get("DUCK_MEM", "12GB")

# Resolution keys + categorical accelerators. `address_source` and state are low-cardinality;
# entity_key is the PK, uei + domain_norm are bridges (either or both may be NULL per row).
BTREE_COLS = ["entity_key", "uei", "domain_norm", "primary_naics", "legal_business_name"]
BITMAP_COLS = ["address_source", "winner_state", "winner_country_code",
               "had_sam_physical", "had_sam_mailing", "had_prospeo", "had_blitz"]


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
    con.register("sme", lance.dataset(SME, storage_options=so))
    con.register("smd", lance.dataset(SMD, storage_options=so))
    con.register("pro", lance.dataset(PRO, storage_options=so))
    con.register("blz", lance.dataset(BLZ, storage_options=so))

    # ── 1 row/uei from sam_master_domains (collapse multi-domain ueis to deterministic-min) ──
    con.execute("CREATE TEMP TABLE smd1 AS "
                "SELECT uei, min(normalized_domain) AS domain_norm FROM smd GROUP BY uei")

    # ── 1 row/domain_norm in firmographics_blitz (collapse near-duplicates to most-recent) ──
    # firmographics_blitz materializer already collapses to 1 row/domain_norm, so this is a passthrough.
    con.execute("""
        CREATE TEMP TABLE blz1 AS
        SELECT domain_norm,
               nullif(trim(hq_city), '')   AS blitz_hq_city,
               nullif(trim(hq_state), '')  AS blitz_hq_state,
               nullif(trim(hq_region), '') AS blitz_hq_region
        FROM blz
        WHERE domain_norm IS NOT NULL
    """)

    # ── prospeo (already 1 row/domain_norm by builder contract) ──
    con.execute("""
        CREATE TEMP TABLE pro1 AS
        SELECT domain_norm,
               nullif(trim(company_city), '')         AS prospeo_city,
               nullif(trim(company_state), '')        AS prospeo_state,
               nullif(trim(company_country), '')      AS prospeo_country,
               nullif(trim(company_country_code), '') AS prospeo_country_code,
               nullif(trim(company_raw_address), '')  AS prospeo_raw_address
        FROM pro
        WHERE domain_norm IS NOT NULL
    """)

    # ── per-source projections off sam_master_entities. Empty-string → NULL on every leaf so the
    # coalesce that picks the winning whole-address ignores blanks. SAM-anchored rows: 1 per UEI. ──
    con.execute("""
        CREATE TEMP TABLE base_sam AS
        SELECT s.uei,
               nullif(trim(s.legal_business_name), '') AS legal_business_name,
               nullif(trim(s.primary_naics), '')       AS primary_naics,
               d.domain_norm,

               -- SAM physical
               nullif(trim(s.physical_address_line_1), '')              AS sam_physical_line_1,
               nullif(trim(s.physical_address_line_2), '')              AS sam_physical_line_2,
               nullif(trim(s.physical_address_city), '')                AS sam_physical_city,
               nullif(trim(s.physical_address_province_or_state), '')   AS sam_physical_state,
               nullif(trim(s.physical_address_zip_postal_code), '')     AS sam_physical_postal_code,
               nullif(trim(s.physical_address_zip_code_4), '')          AS sam_physical_zip_plus_4,
               nullif(trim(s.physical_address_country_code), '')        AS sam_physical_country_code,
               nullif(trim(s.physical_address_congressional_district),'') AS sam_physical_congressional_district,

               -- SAM mailing
               nullif(trim(s.mailing_address_line_1), '')             AS sam_mailing_line_1,
               nullif(trim(s.mailing_address_line_2), '')             AS sam_mailing_line_2,
               nullif(trim(s.mailing_address_city), '')               AS sam_mailing_city,
               nullif(trim(s.mailing_address_state_or_province), '')  AS sam_mailing_state,
               nullif(trim(s.mailing_address_zip_postal_code), '')    AS sam_mailing_postal_code,
               nullif(trim(s.mailing_address_zip_code_4), '')         AS sam_mailing_zip_plus_4,
               nullif(trim(s.mailing_address_country), '')            AS sam_mailing_country
        FROM sme s
        LEFT JOIN smd1 d ON s.uei = d.uei
    """)

    # ── SAM-known domain universe (every domain that maps to a SAM UEI). Used to filter the
    # Prospeo + Blitz orphan sets so the universe stays grain-coherent (no double-counting). ──
    con.execute("""
        CREATE TEMP TABLE sam_domains AS
        SELECT DISTINCT domain_norm AS d FROM base_sam WHERE domain_norm IS NOT NULL
    """)

    # ── Prospeo orphans: domains in Prospeo that DO NOT resolve to a SAM UEI. uei=NULL.
    # Every per-source raw column from SAM stays NULL on these rows; winner picks from Prospeo. ──
    con.execute("""
        CREATE TEMP TABLE base_prospeo_orphan AS
        SELECT
          CAST(NULL AS VARCHAR) AS uei,
          CAST(NULL AS VARCHAR) AS legal_business_name,
          CAST(NULL AS VARCHAR) AS primary_naics,
          pr.domain_norm,
          CAST(NULL AS VARCHAR) AS sam_physical_line_1, CAST(NULL AS VARCHAR) AS sam_physical_line_2,
          CAST(NULL AS VARCHAR) AS sam_physical_city,   CAST(NULL AS VARCHAR) AS sam_physical_state,
          CAST(NULL AS VARCHAR) AS sam_physical_postal_code, CAST(NULL AS VARCHAR) AS sam_physical_zip_plus_4,
          CAST(NULL AS VARCHAR) AS sam_physical_country_code, CAST(NULL AS VARCHAR) AS sam_physical_congressional_district,
          CAST(NULL AS VARCHAR) AS sam_mailing_line_1,  CAST(NULL AS VARCHAR) AS sam_mailing_line_2,
          CAST(NULL AS VARCHAR) AS sam_mailing_city,    CAST(NULL AS VARCHAR) AS sam_mailing_state,
          CAST(NULL AS VARCHAR) AS sam_mailing_postal_code, CAST(NULL AS VARCHAR) AS sam_mailing_zip_plus_4,
          CAST(NULL AS VARCHAR) AS sam_mailing_country
        FROM pro1 pr
        WHERE pr.domain_norm NOT IN (SELECT d FROM sam_domains)
    """)

    # ── Blitz orphans: domains in firmographics_blitz that map to NEITHER a SAM UEI NOR a Prospeo
    # row. uei=NULL; winner picks from Blitz HQ (city/state only — no street/postal). ──
    con.execute("""
        CREATE TEMP TABLE base_blitz_orphan AS
        SELECT
          CAST(NULL AS VARCHAR) AS uei,
          CAST(NULL AS VARCHAR) AS legal_business_name,
          CAST(NULL AS VARCHAR) AS primary_naics,
          bz.domain_norm,
          CAST(NULL AS VARCHAR) AS sam_physical_line_1, CAST(NULL AS VARCHAR) AS sam_physical_line_2,
          CAST(NULL AS VARCHAR) AS sam_physical_city,   CAST(NULL AS VARCHAR) AS sam_physical_state,
          CAST(NULL AS VARCHAR) AS sam_physical_postal_code, CAST(NULL AS VARCHAR) AS sam_physical_zip_plus_4,
          CAST(NULL AS VARCHAR) AS sam_physical_country_code, CAST(NULL AS VARCHAR) AS sam_physical_congressional_district,
          CAST(NULL AS VARCHAR) AS sam_mailing_line_1,  CAST(NULL AS VARCHAR) AS sam_mailing_line_2,
          CAST(NULL AS VARCHAR) AS sam_mailing_city,    CAST(NULL AS VARCHAR) AS sam_mailing_state,
          CAST(NULL AS VARCHAR) AS sam_mailing_postal_code, CAST(NULL AS VARCHAR) AS sam_mailing_zip_plus_4,
          CAST(NULL AS VARCHAR) AS sam_mailing_country
        FROM blz1 bz
        WHERE bz.domain_norm NOT IN (SELECT d FROM sam_domains)
          AND bz.domain_norm NOT IN (SELECT domain_norm FROM base_prospeo_orphan)
    """)

    # ── UNION the three universes. Grain: SAM UEI (when present) else domain_norm. ──
    con.execute("""
        CREATE TEMP TABLE base AS
        SELECT * FROM base_sam
        UNION ALL
        SELECT * FROM base_prospeo_orphan
        UNION ALL
        SELECT * FROM base_blitz_orphan
    """)

    # ── presence flags + winner selection. `address_source` is the rank tag; the winner_* columns
    # are the WHOLE-address pick from the winning source. SAM physical is preferred to SAM mailing
    # because the physical address is the operational location; mailing is often a PO box. ──
    con.execute("""
        CREATE TEMP TABLE joined AS
        SELECT b.*,
               pr.prospeo_city, pr.prospeo_state, pr.prospeo_country, pr.prospeo_country_code,
               pr.prospeo_raw_address,
               bz.blitz_hq_city, bz.blitz_hq_state, bz.blitz_hq_region
        FROM base b
        LEFT JOIN pro1 pr ON b.domain_norm = pr.domain_norm
        LEFT JOIN blz1 bz ON b.domain_norm = bz.domain_norm
    """)

    con.execute("""
        CREATE TEMP TABLE m AS
        SELECT j.*,
               -- presence flags
               (j.sam_physical_city IS NOT NULL OR j.sam_physical_state IS NOT NULL
                  OR j.sam_physical_line_1 IS NOT NULL OR j.sam_physical_postal_code IS NOT NULL) AS had_sam_physical,
               (j.sam_mailing_city  IS NOT NULL OR j.sam_mailing_state  IS NOT NULL
                  OR j.sam_mailing_line_1  IS NOT NULL OR j.sam_mailing_postal_code  IS NOT NULL) AS had_sam_mailing,
               (j.prospeo_city IS NOT NULL OR j.prospeo_state IS NOT NULL
                  OR j.prospeo_raw_address IS NOT NULL)                                            AS had_prospeo,
               (j.blitz_hq_city IS NOT NULL OR j.blitz_hq_state IS NOT NULL)                       AS had_blitz,

               -- winner source: precedence SAM physical > SAM mailing > Prospeo > Blitz > 'none'
               CASE
                 WHEN j.sam_physical_city IS NOT NULL OR j.sam_physical_state IS NOT NULL
                   OR j.sam_physical_line_1 IS NOT NULL OR j.sam_physical_postal_code IS NOT NULL THEN 'sam_physical'
                 WHEN j.sam_mailing_city IS NOT NULL OR j.sam_mailing_state IS NOT NULL
                   OR j.sam_mailing_line_1 IS NOT NULL OR j.sam_mailing_postal_code IS NOT NULL  THEN 'sam_mailing'
                 WHEN j.prospeo_city IS NOT NULL OR j.prospeo_state IS NOT NULL
                   OR j.prospeo_raw_address IS NOT NULL                                          THEN 'prospeo'
                 WHEN j.blitz_hq_city IS NOT NULL OR j.blitz_hq_state IS NOT NULL                 THEN 'blitz'
                 ELSE 'none'
               END AS address_source
        FROM joined j
    """)

    # ── flatten the winner fields. WHOLE-address pick — never mix fields across sources. ──
    con.execute("""
        CREATE TEMP TABLE final AS
        SELECT *,
               CASE address_source
                 WHEN 'sam_physical' THEN sam_physical_line_1
                 WHEN 'sam_mailing'  THEN sam_mailing_line_1
                 WHEN 'prospeo'      THEN NULL                    -- Prospeo has no street
                 WHEN 'blitz'        THEN NULL                    -- Blitz has no street
                 ELSE NULL END AS winner_line_1,
               CASE address_source
                 WHEN 'sam_physical' THEN sam_physical_line_2
                 WHEN 'sam_mailing'  THEN sam_mailing_line_2
                 ELSE NULL END AS winner_line_2,
               CASE address_source
                 WHEN 'sam_physical' THEN sam_physical_city
                 WHEN 'sam_mailing'  THEN sam_mailing_city
                 WHEN 'prospeo'      THEN prospeo_city
                 WHEN 'blitz'        THEN blitz_hq_city
                 ELSE NULL END AS winner_city,
               CASE address_source
                 WHEN 'sam_physical' THEN sam_physical_state
                 WHEN 'sam_mailing'  THEN sam_mailing_state
                 WHEN 'prospeo'      THEN prospeo_state
                 WHEN 'blitz'        THEN blitz_hq_state
                 ELSE NULL END AS winner_state,
               CASE address_source
                 WHEN 'sam_physical' THEN sam_physical_postal_code
                 WHEN 'sam_mailing'  THEN sam_mailing_postal_code
                 ELSE NULL END AS winner_postal_code,
               CASE address_source
                 WHEN 'sam_physical' THEN sam_physical_zip_plus_4
                 WHEN 'sam_mailing'  THEN sam_mailing_zip_plus_4
                 ELSE NULL END AS winner_zip_plus_4,
               CASE address_source
                 WHEN 'sam_physical' THEN sam_physical_country_code
                 WHEN 'sam_mailing'  THEN nullif(trim(sam_mailing_country), '')
                 WHEN 'prospeo'      THEN coalesce(prospeo_country_code, prospeo_country)
                 ELSE NULL END AS winner_country_code,
               CASE address_source
                 WHEN 'sam_physical' THEN sam_physical_congressional_district
                 ELSE NULL END AS winner_congressional_district,
               now() AS materialized_at
        FROM m
    """)

    tbl = con.execute("""
        SELECT
          coalesce(uei, 'dom:' || domain_norm) AS entity_key,
          uei, domain_norm, legal_business_name, primary_naics,
          address_source,
          winner_line_1, winner_line_2, winner_city, winner_state,
          winner_postal_code, winner_zip_plus_4, winner_country_code,
          winner_congressional_district,
          had_sam_physical, had_sam_mailing, had_prospeo, had_blitz,
          sam_physical_line_1, sam_physical_line_2, sam_physical_city, sam_physical_state,
          sam_physical_postal_code, sam_physical_zip_plus_4, sam_physical_country_code,
          sam_physical_congressional_district,
          sam_mailing_line_1, sam_mailing_line_2, sam_mailing_city, sam_mailing_state,
          sam_mailing_postal_code, sam_mailing_zip_plus_4, sam_mailing_country,
          prospeo_city, prospeo_state, prospeo_country, prospeo_country_code, prospeo_raw_address,
          blitz_hq_city, blitz_hq_state, blitz_hq_region,
          materialized_at
        FROM final
    """).fetch_arrow_table()
    rows = tbl.num_rows
    src_mix = con.execute(
        "SELECT address_source, count(*) n FROM final GROUP BY 1 ORDER BY 2 DESC").fetchall()
    print(f"rows={rows:,}  source mix:")
    for src, n in src_mix:
        print(f"  {src:<14} {n:>9,}")
    assert rows > 0, "no rows produced"

    import lance
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
    assert back == rows, f"write-integrity gate: {back} != {rows}"
    print(f"WROTE {SERVING_URI} rows={back} cols={len(ds.schema)}")
    return {"uri": SERVING_URI, "rows": back, "source_mix": dict(src_mix)}


def verify() -> None:
    import duckdb
    import lance

    so = _r2_storage_options()
    ds = lance.dataset(SERVING_URI, storage_options=so)
    print(f"{SERVING_URI}  rows={ds.count_rows():,}  cols={len(ds.schema)}")
    print("indices:", sorted(
        (i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i))) for i in ds.list_indices()))
    con = duckdb.connect(":memory:"); con.register("d", ds)
    print("\n=== address_source mix ===")
    print(con.execute("""SELECT address_source, count(*) n,
        round(100.0*count(*)/sum(count(*)) OVER (),2) pct
        FROM d GROUP BY 1 ORDER BY 2 DESC""").df().to_string(index=False))
    print("\n=== presence-flag overlap ===")
    print(con.execute("""SELECT had_sam_physical, had_sam_mailing, had_prospeo, had_blitz,
        count(*) firms FROM d GROUP BY 1,2,3,4 ORDER BY 5 DESC LIMIT 16""").df().to_string(index=False))
    print("\n=== top winner states ===")
    print(con.execute("""SELECT winner_state, count(*) n FROM d WHERE winner_state IS NOT NULL
        GROUP BY 1 ORDER BY 2 DESC LIMIT 12""").df().to_string(index=False))
    print("\n=== domain_norm bridge populated ===")
    print(con.execute("""SELECT count(*) total,
        count(*) FILTER (WHERE domain_norm IS NOT NULL) with_domain_norm,
        round(100.0*count(*) FILTER (WHERE domain_norm IS NOT NULL)/count(*),2) pct
        FROM d""").df().to_string(index=False))


if __name__ == "__main__":
    (verify if "--verify" in sys.argv else build)()
