"""Serving build — `company_addresses` (consolidated firm mailing addresses, multi-source).

SoR  s3://data-sink/active/company_addresses/  (Lance v2.1; derived, snapshot-overwrite)

WHAT THIS IS
UNION universe across SAM + Prospeo + Blitz + GTM companies. Grain is one row per firm identified by either a SAM
UEI or (when not in SAM) a normalized domain. Each row carries the WINNING mailing address —
selected by source precedence SAM physical > SAM mailing > Prospeo > Blitz — plus every per-source
raw address kept side-by-side so consumers can override the winner. Bridges via `domain_norm` (the
canonical key everything else joins on: firmographics_blitz, equipment_catalog, industries_served,
equipment_provider, prospeo_company_export). Built so any downstream answering "where is this firm
physically located" can hit one Lance instead of coalescing across three.

UNIVERSE (4 record sources, deduped by domain_norm)
  1. Every UEI in sam_master_entities                                          (~1.54M rows; uei + maybe domain_norm)
  2. Every Prospeo domain NOT already mapped to a SAM UEI                       (non-SAM-Prospeo orphans; uei=NULL)
  3. Every Blitz domain NOT in SAM AND NOT in Prospeo                            (non-SAM-non-Prospeo Blitz orphans; uei=NULL)
  4. Every GTM companies domain NOT in SAM/Prospeo/Blitz                         (enrichment candidate orphans; uei=NULL)

PRIMARY KEY  `entity_key` = uei when SAM-resolved, else 'dom:' + domain_norm. SAM rows carry both.

OPTIONAL BRIDGE  `company_linkedin_url` (Prospeo > Blitz coalesce; NULL if neither source has it).
SAM is silent on LinkedIn, so this populates for: every Blitz orphan, every Prospeo orphan, and
every SAM-with-domain row whose domain matches a Blitz or Prospeo record. SAM-only rows with no
domain stay NULL — there's no bridge to LinkedIn for them.

PRECEDENCE (operator standard — do not reorder without sign-off)
  1. SAM physical address (street + line2 + city + state + zip + country + congressional district)
  2. SAM mailing address  (when physical is empty)
  3. Prospeo WITH postal   (postal extracted from company_raw_address, validated against a real ZCTA5)
  4. Overture (domain-join) (street + ZCTA postal + lat/lon, recovered from overture_places by domain)
  5. Prospeo city/state     (free-text company_raw_address, no usable postal)
  6. Blitz                  (city/state — no street, no postal; floor of last resort)

The winner is selected as a WHOLE address — no field-level mixing across sources. If SAM physical
city is present then every winner field comes from SAM physical, never a Prospeo state grafted onto
a SAM city. Source provenance is stamped in `address_source` so downstream can pick differently.

POSTAL RECOVERY (why tiers 3-4 exist). Prospeo ships no postal column, but ~83% of rows carry a ZIP
inside the free-text `company_raw_address`; we extract the trailing 5-digit token and keep it only
if it is a member of zcta_zip_centroids (rejects street numbers / PO-box ZIPs / garbage). For the
residual that has no usable text ZIP, we domain-join overture_places (16.3M rows, BTREE on domain)
to recover a real street + ZCTA postal + lat/lon. Both paths exist purely to populate
`winner_postal_code` so the firm becomes placeable in the downstream proximity matrix.

SIDES
  IDENTITY  sam_master_entities       grain (~1.5M UEIs); legal_business_name + primary_naics
  DOMAIN    sam_master_domains        canonical normalized entity_url → domain (1 row / uei after rollup)
  PROSPEO   prospeo_company_export    domain-keyed; ~10.7k rows; postal extracted from raw_address (ZCTA-validated)
  OVERTURE  overture_places           domain-keyed; best place per domain (highest confidence); street+postal+lat/lon
  BLITZ     firmographics_blitz       domain_norm → hq_city/hq_state/hq_region (no street/postal)
  COMPANIES companies                 domain-keyed GTM candidate firms; carries name only — address recovered via Overture

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
OVT = f"{A}/overture_places/"
ZCTA = f"{A}/zcta_zip_centroids/"
CO = f"{A}/companies/"
SERVING_URI = os.environ.get("COMPANY_ADDRESSES_URI", f"{A}/company_addresses/")
DATA_STORAGE_VERSION = "2.1"
DUCK_MEM = os.environ.get("DUCK_MEM", "12GB")

# Resolution keys + categorical accelerators. `address_source` and state are low-cardinality;
# entity_key is the PK, uei + domain_norm + company_linkedin_url are bridges (any may be NULL).
BTREE_COLS = ["entity_key", "uei", "domain_norm", "company_linkedin_url",
              "primary_naics", "legal_business_name"]
BITMAP_COLS = ["address_source", "winner_state", "winner_country_code",
               "had_sam_physical", "had_sam_mailing", "had_prospeo", "had_overture", "had_blitz"]


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
    con.register("ovt", lance.dataset(OVT, storage_options=so))
    con.register("zcta", lance.dataset(ZCTA, storage_options=so))
    con.register("co", lance.dataset(CO, storage_options=so))

    # ZCTA5 membership set — gates every recovered postal so only real, joinable ZIPs land
    # in winner_postal_code (rejects street numbers, PO-box ZIPs, and Overture non-US codes).
    con.execute("CREATE TEMP TABLE zcta5 AS SELECT DISTINCT zcta5 AS z FROM zcta WHERE zcta5 IS NOT NULL")

    # ── 1 row/uei from sam_master_domains (collapse multi-domain ueis to deterministic-min) ──
    con.execute("CREATE TEMP TABLE smd1 AS "
                "SELECT uei, min(normalized_domain) AS domain_norm FROM smd GROUP BY uei")

    # ── 1 row/domain_norm in firmographics_blitz (collapse near-duplicates to most-recent) ──
    # firmographics_blitz materializer already collapses to 1 row/domain_norm, so this is a passthrough.
    con.execute("""
        CREATE TEMP TABLE blz1 AS
        SELECT domain_norm,
               nullif(trim(hq_city), '')     AS blitz_hq_city,
               nullif(trim(hq_state), '')    AS blitz_hq_state,
               nullif(trim(hq_region), '')   AS blitz_hq_region,
               nullif(trim(linkedin_url), '') AS blitz_linkedin_url
        FROM blz
        WHERE domain_norm IS NOT NULL
    """)

    # ── prospeo (already 1 row/domain_norm by builder contract). prospeo_postal_code is the
    # trailing 5-digit token in company_raw_address, kept only if it is a real ZCTA5 (else NULL). ──
    con.execute(r"""
        CREATE TEMP TABLE pro1 AS
        SELECT domain_norm,
               nullif(trim(company_city), '')          AS prospeo_city,
               nullif(trim(company_state), '')         AS prospeo_state,
               nullif(trim(company_country), '')       AS prospeo_country,
               nullif(trim(company_country_code), '')  AS prospeo_country_code,
               nullif(trim(company_raw_address), '')   AS prospeo_raw_address,
               nullif(trim(company_linkedin_url), '')  AS prospeo_linkedin_url,
               (SELECT max(tok) FROM (
                   SELECT unnest(regexp_extract_all(coalesce(company_raw_address,''), '\d{5}')) AS tok
                ) s WHERE s.tok IN (SELECT z FROM zcta5))   AS prospeo_postal_code
        FROM pro
        WHERE domain_norm IS NOT NULL
    """)

    # ── GTM companies (active/companies) — 1 row/domain_norm. Enrichment-sourced candidate firms
    # (epd_lec_status, equipment_rental, …) that frequently sit OUTSIDE SAM/Prospeo/Blitz entirely.
    # Without this tier they get neither an Overture lookup nor an output row, so they stay
    # unplaceable in the downstream proximity matrix. legal_business_name carried from company_name. ──
    con.execute("""
        CREATE TEMP TABLE co1 AS
        SELECT nullif(trim(normalized_domain), '')        AS domain_norm,
               min(nullif(trim(company_name), ''))        AS company_name
        FROM co
        WHERE nullif(trim(normalized_domain), '') IS NOT NULL
        GROUP BY 1
    """)

    # ── Universe of domains we actually care about (SAM ∪ Prospeo ∪ Blitz ∪ GTM companies) — used
    # to prune the 16.3M-row Overture scan down to a windowable set before ranking best-place-per-
    # domain. GTM companies added so candidate-only domains are eligible for Overture recovery. ──
    con.execute("""
        CREATE TEMP TABLE universe_domains AS
        SELECT DISTINCT domain_norm AS d FROM (
            SELECT domain_norm FROM smd1 WHERE domain_norm IS NOT NULL
            UNION SELECT domain_norm FROM pro1
            UNION SELECT domain_norm FROM blz1
            UNION SELECT domain_norm FROM co1
        )
    """)

    # ── Overture recovery: best place per domain (highest confidence). domain is normalized to
    # match domain_norm; postcode kept only if ZCTA-valid. Carries lat/lon for future precise use. ──
    con.execute(r"""
        CREATE TEMP TABLE ovt1 AS
        WITH ov_norm AS (
            SELECT
              regexp_replace(regexp_replace(regexp_replace(lower(domain), '^https?://', ''),
                             '^www\.', ''), '/.*$', '')        AS domain_norm,
              substring(postcode, 1, 5)        AS pc5,
              latitude, longitude,
              nullif(trim(street), '')         AS overture_street,
              nullif(trim(locality), '')       AS overture_city,
              nullif(trim(region), '')         AS overture_region,
              confidence
            FROM ovt
            WHERE domain IS NOT NULL AND latitude IS NOT NULL
        ),
        ranked AS (
            SELECT o.*,
                   ROW_NUMBER() OVER (PARTITION BY o.domain_norm ORDER BY o.confidence DESC NULLS LAST) AS rn
            FROM ov_norm o
            WHERE o.domain_norm IN (SELECT d FROM universe_domains)
        )
        SELECT
          domain_norm,
          CASE WHEN pc5 IN (SELECT z FROM zcta5) THEN pc5 ELSE NULL END AS overture_postcode,
          latitude  AS overture_lat,
          longitude AS overture_lon,
          overture_street, overture_city, overture_region
        FROM ranked WHERE rn = 1
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

    # ── GTM-companies orphans: domains in active/companies mapping to NEITHER a SAM UEI NOR a
    # Prospeo row NOR a Blitz row. uei=NULL; legal_business_name from companies.company_name; winner
    # picks from the Overture domain-join (street + ZCTA postal + lat/lon) when present, else 'none'. ──
    con.execute("""
        CREATE TEMP TABLE base_companies_orphan AS
        SELECT
          CAST(NULL AS VARCHAR) AS uei,
          co.company_name       AS legal_business_name,
          CAST(NULL AS VARCHAR) AS primary_naics,
          co.domain_norm,
          CAST(NULL AS VARCHAR) AS sam_physical_line_1, CAST(NULL AS VARCHAR) AS sam_physical_line_2,
          CAST(NULL AS VARCHAR) AS sam_physical_city,   CAST(NULL AS VARCHAR) AS sam_physical_state,
          CAST(NULL AS VARCHAR) AS sam_physical_postal_code, CAST(NULL AS VARCHAR) AS sam_physical_zip_plus_4,
          CAST(NULL AS VARCHAR) AS sam_physical_country_code, CAST(NULL AS VARCHAR) AS sam_physical_congressional_district,
          CAST(NULL AS VARCHAR) AS sam_mailing_line_1,  CAST(NULL AS VARCHAR) AS sam_mailing_line_2,
          CAST(NULL AS VARCHAR) AS sam_mailing_city,    CAST(NULL AS VARCHAR) AS sam_mailing_state,
          CAST(NULL AS VARCHAR) AS sam_mailing_postal_code, CAST(NULL AS VARCHAR) AS sam_mailing_zip_plus_4,
          CAST(NULL AS VARCHAR) AS sam_mailing_country
        FROM co1 co
        WHERE co.domain_norm NOT IN (SELECT d FROM sam_domains)
          AND co.domain_norm NOT IN (SELECT domain_norm FROM base_prospeo_orphan)
          AND co.domain_norm NOT IN (SELECT domain_norm FROM base_blitz_orphan)
    """)

    # ── UNION the four universes. Grain: SAM UEI (when present) else domain_norm. ──
    con.execute("""
        CREATE TEMP TABLE base AS
        SELECT * FROM base_sam
        UNION ALL
        SELECT * FROM base_prospeo_orphan
        UNION ALL
        SELECT * FROM base_blitz_orphan
        UNION ALL
        SELECT * FROM base_companies_orphan
    """)

    # ── presence flags + winner selection. `address_source` is the rank tag; the winner_* columns
    # are the WHOLE-address pick from the winning source. SAM physical is preferred to SAM mailing
    # because the physical address is the operational location; mailing is often a PO box. ──
    con.execute("""
        CREATE TEMP TABLE joined AS
        SELECT b.*,
               pr.prospeo_city, pr.prospeo_state, pr.prospeo_country, pr.prospeo_country_code,
               pr.prospeo_raw_address, pr.prospeo_linkedin_url, pr.prospeo_postal_code,
               bz.blitz_hq_city, bz.blitz_hq_state, bz.blitz_hq_region, bz.blitz_linkedin_url,
               ov.overture_postcode, ov.overture_lat, ov.overture_lon,
               ov.overture_street, ov.overture_city, ov.overture_region
        FROM base b
        LEFT JOIN pro1 pr  ON b.domain_norm = pr.domain_norm
        LEFT JOIN blz1 bz  ON b.domain_norm = bz.domain_norm
        LEFT JOIN ovt1 ov  ON b.domain_norm = ov.domain_norm
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
               (j.overture_postcode IS NOT NULL OR j.overture_lat IS NOT NULL)                     AS had_overture,
               (j.blitz_hq_city IS NOT NULL OR j.blitz_hq_state IS NOT NULL)                       AS had_blitz,

               -- winner source precedence (postal-completeness aware for the spatial goal):
               --   sam_physical > sam_mailing > prospeo(WITH postal) > overture > prospeo(city only) > blitz
               CASE
                 WHEN j.sam_physical_city IS NOT NULL OR j.sam_physical_state IS NOT NULL
                   OR j.sam_physical_line_1 IS NOT NULL OR j.sam_physical_postal_code IS NOT NULL THEN 'sam_physical'
                 WHEN j.sam_mailing_city IS NOT NULL OR j.sam_mailing_state IS NOT NULL
                   OR j.sam_mailing_line_1 IS NOT NULL OR j.sam_mailing_postal_code IS NOT NULL  THEN 'sam_mailing'
                 WHEN j.prospeo_postal_code IS NOT NULL                                          THEN 'prospeo'
                 WHEN j.overture_postcode IS NOT NULL OR j.overture_lat IS NOT NULL              THEN 'overture'
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
                 WHEN 'overture'     THEN overture_street       -- Overture carries a street
                 WHEN 'prospeo'      THEN NULL                  -- Prospeo has no street
                 WHEN 'blitz'        THEN NULL                  -- Blitz has no street
                 ELSE NULL END AS winner_line_1,
               CASE address_source
                 WHEN 'sam_physical' THEN sam_physical_line_2
                 WHEN 'sam_mailing'  THEN sam_mailing_line_2
                 ELSE NULL END AS winner_line_2,
               CASE address_source
                 WHEN 'sam_physical' THEN sam_physical_city
                 WHEN 'sam_mailing'  THEN sam_mailing_city
                 WHEN 'prospeo'      THEN prospeo_city
                 WHEN 'overture'     THEN overture_city
                 WHEN 'blitz'        THEN blitz_hq_city
                 ELSE NULL END AS winner_city,
               CASE address_source
                 WHEN 'sam_physical' THEN sam_physical_state
                 WHEN 'sam_mailing'  THEN sam_mailing_state
                 WHEN 'prospeo'      THEN prospeo_state
                 WHEN 'overture'     THEN overture_region
                 WHEN 'blitz'        THEN blitz_hq_state
                 ELSE NULL END AS winner_state,
               CASE address_source
                 WHEN 'sam_physical' THEN sam_physical_postal_code
                 WHEN 'sam_mailing'  THEN sam_mailing_postal_code
                 WHEN 'prospeo'      THEN prospeo_postal_code    -- extracted, ZCTA-validated
                 WHEN 'overture'     THEN overture_postcode      -- domain-join, ZCTA-validated
                 ELSE NULL END AS winner_postal_code,
               CASE address_source
                 WHEN 'sam_physical' THEN sam_physical_zip_plus_4
                 WHEN 'sam_mailing'  THEN sam_mailing_zip_plus_4
                 ELSE NULL END AS winner_zip_plus_4,
               CASE address_source
                 WHEN 'sam_physical' THEN sam_physical_country_code
                 WHEN 'sam_mailing'  THEN nullif(trim(sam_mailing_country), '')
                 WHEN 'prospeo'      THEN coalesce(prospeo_country_code, prospeo_country)
                 WHEN 'overture'     THEN 'US'                   -- ZCTA-valid postal ⇒ US
                 ELSE NULL END AS winner_country_code,
               CASE address_source
                 WHEN 'sam_physical' THEN sam_physical_congressional_district
                 ELSE NULL END AS winner_congressional_district,
               -- LinkedIn URL — Prospeo (authoritative, sourced) > Blitz (fallback); NULL if neither.
               -- Independent of address_source: a SAM row with no Prospeo address can still pick
               -- up a LinkedIn URL from Blitz via the domain_norm bridge.
               COALESCE(prospeo_linkedin_url, blitz_linkedin_url) AS company_linkedin_url,
               now() AS materialized_at
        FROM m
    """)

    tbl = con.execute("""
        SELECT
          coalesce(uei, 'dom:' || domain_norm) AS entity_key,
          uei, domain_norm, legal_business_name, primary_naics,
          company_linkedin_url,
          address_source,
          winner_line_1, winner_line_2, winner_city, winner_state,
          winner_postal_code, winner_zip_plus_4, winner_country_code,
          winner_congressional_district,
          had_sam_physical, had_sam_mailing, had_prospeo, had_overture, had_blitz,
          sam_physical_line_1, sam_physical_line_2, sam_physical_city, sam_physical_state,
          sam_physical_postal_code, sam_physical_zip_plus_4, sam_physical_country_code,
          sam_physical_congressional_district,
          sam_mailing_line_1, sam_mailing_line_2, sam_mailing_city, sam_mailing_state,
          sam_mailing_postal_code, sam_mailing_zip_plus_4, sam_mailing_country,
          prospeo_city, prospeo_state, prospeo_country, prospeo_country_code, prospeo_raw_address,
          prospeo_postal_code, prospeo_linkedin_url,
          overture_street, overture_city, overture_region, overture_postcode,
          overture_lat, overture_lon,
          blitz_hq_city, blitz_hq_state, blitz_hq_region, blitz_linkedin_url,
          materialized_at
        FROM final
    """).to_arrow_table()
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
    print("\n=== winner_postal_code coverage (placeable in proximity) by source ===")
    print(con.execute("""SELECT address_source,
        count(*) firms,
        count(winner_postal_code) with_postal,
        round(100.0*count(winner_postal_code)/count(*),1) pct
        FROM d GROUP BY 1 ORDER BY firms DESC""").df().to_string(index=False))
    print("\n=== Overture recovery (firms placed only because of the domain join) ===")
    print(con.execute("""SELECT
        count(*) FILTER (WHERE had_overture) firms_with_overture_match,
        count(*) FILTER (WHERE address_source='overture') firms_won_by_overture,
        count(*) FILTER (WHERE address_source='overture' AND winner_postal_code IS NOT NULL) overture_winners_with_postal
        FROM d""").df().to_string(index=False))


if __name__ == "__main__":
    (verify if "--verify" in sys.argv else build)()
