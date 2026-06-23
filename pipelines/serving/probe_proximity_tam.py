"""Read-only DuckDB probe — Firm-to-Construction proximity density / geographic TAM.

Joins govcon_firm_construction_proximity (firm × PSC spatial matrix, ≤50 mi)
against the active/companies candidate universe. NO writes, NO index changes.
"""
from __future__ import annotations
import os
import duckdb
import lance

A = "s3://data-sink/active"


def so() -> dict[str, str]:
    ep = os.environ.get("R2_ENDPOINT")
    aid = os.environ.get("R2_ACCOUNT_ID")
    if not ep and aid:
        ep = f"https://{aid}.r2.cloudflarestorage.com"
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": ep, "region": "auto",
    }


CANDIDATE_PLATFORMS = (
    "enrichment:epd_lec_status_candidates", "epd_lec_status_candidates",
    "enrichment:equipment_rental_candidates", "equipment_rental_candidates",
)
EQUIP = ("enrichment:equipment_rental_candidates", "equipment_rental_candidates")
EPD = ("enrichment:epd_lec_status_candidates", "epd_lec_status_candidates")


def lst(xs):
    return ",".join(f"'{x}'" for x in xs)


def main() -> None:
    s = so()
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=8")
    con.execute("SET memory_limit='12GB'")
    con.register("co",   lance.dataset(f"{A}/companies/", storage_options=s))
    con.register("ca",   lance.dataset(f"{A}/company_addresses/", storage_options=s))
    con.register("zcta", lance.dataset(f"{A}/zcta_zip_centroids/", storage_options=s))
    con.register("aw",   lance.dataset(f"{A}/govcon_active_awards/", storage_options=s))
    con.register("prox", lance.dataset(f"{A}/govcon_firm_construction_proximity/", storage_options=s))

    print("=" * 78)
    print("0. CANDIDATE UNIVERSE COMPOSITION (active/companies)")
    print("=" * 78)
    print(con.execute("""
        SELECT source_platform,
               count(*)                                   AS rows,
               count(DISTINCT nullif(trim(normalized_domain),'')) AS distinct_domains
        FROM co GROUP BY 1 ORDER BY rows DESC
    """).df().to_string(index=False))

    # ---- candidate → bucket map (one row per distinct domain, flags for each tag) ----
    con.execute(f"""
        CREATE TEMP TABLE cand AS
        SELECT nullif(trim(normalized_domain),'') AS firm_domain,
               max(CASE WHEN source_platform IN ({lst(EQUIP)}) THEN 1 ELSE 0 END) AS is_equip,
               max(CASE WHEN source_platform IN ({lst(EPD)})   THEN 1 ELSE 0 END) AS is_epd
        FROM co
        WHERE source_platform IN ({lst(CANDIDATE_PLATFORMS)})
          AND nullif(trim(normalized_domain),'') IS NOT NULL
        GROUP BY 1
    """)
    tot_rows = con.execute(f"SELECT count(*) FROM co WHERE source_platform IN ({lst(CANDIDATE_PLATFORMS)})").fetchone()[0]
    null_dom = con.execute(f"SELECT count(*) FROM co WHERE source_platform IN ({lst(CANDIDATE_PLATFORMS)}) AND nullif(trim(normalized_domain),'') IS NULL").fetchone()[0]
    cand_n = con.execute("SELECT count(*) FROM cand").fetchone()[0]
    cand_equip = con.execute("SELECT count(*) FROM cand WHERE is_equip=1").fetchone()[0]
    cand_epd = con.execute("SELECT count(*) FROM cand WHERE is_epd=1").fetchone()[0]
    cand_both = con.execute("SELECT count(*) FROM cand WHERE is_equip=1 AND is_epd=1").fetchone()[0]
    print(f"\ncandidate rows (tag-filtered):        {tot_rows:,}")
    print(f"  of which NULL/blank domain:         {null_dom:,}   (can never spatially match)")
    print(f"distinct candidate firms (by domain): {cand_n:,}")
    print(f"  tagged equipment_rental:            {cand_equip:,}")
    print(f"  tagged epd_lec_status:              {cand_epd:,}")
    print(f"  tagged BOTH:                        {cand_both:,}")

    # ---- geo-resolution funnel (replicates the pipeline's target_firms build) ----
    con.execute("""
        CREATE TEMP TABLE cand_postal AS
        SELECT firm_domain, winner_postal_code FROM (
          SELECT c.firm_domain, ca.winner_postal_code,
                 row_number() OVER (PARTITION BY c.firm_domain
                   ORDER BY CASE WHEN ca.uei IS NOT NULL THEN 0 ELSE 1 END, ca.uei) rn
          FROM cand c JOIN ca ON ca.domain_norm = c.firm_domain
          WHERE ca.winner_postal_code IS NOT NULL
        ) WHERE rn = 1
    """)
    con.execute("""
        CREATE TEMP TABLE cand_geo AS
        SELECT p.firm_domain FROM cand_postal p
        JOIN zcta z ON z.zcta5 = p.winner_postal_code
        WHERE z.lat IS NOT NULL AND z.lon IS NOT NULL
    """)
    n_postal = con.execute("SELECT count(DISTINCT firm_domain) FROM cand_postal").fetchone()[0]
    n_geo = con.execute("SELECT count(DISTINCT firm_domain) FROM cand_geo").fetchone()[0]
    n_match = con.execute("SELECT count(DISTINCT firm_domain) FROM prox").fetchone()[0]

    print("\n" + "=" * 78)
    print("1. OVERALL HIT RATE  (geo-resolution funnel → demand match)")
    print("=" * 78)
    print(f"  distinct candidate firms .............. {cand_n:,}   (100.0%)")
    print(f"  └─ resolved to a postal code ......... {n_postal:,}   ({100*n_postal/cand_n:.1f}%)")
    print(f"     └─ resolved to lat/lon centroid ... {n_geo:,}   ({100*n_geo/cand_n:.1f}%)   = mappable universe")
    print(f"        └─ matched ≥1 award ≤50 mi ..... {n_match:,}   ({100*n_match/cand_n:.1f}% of all cand | {100*n_match/n_geo:.1f}% of mappable)")

    # ---- split of matched firms across buckets ----
    print("\n  --- split of matched firms by candidate tag ---")
    print(con.execute("""
        WITH m AS (SELECT DISTINCT firm_domain FROM prox)
        SELECT
          count(*) FILTER (WHERE c.is_equip=1 AND c.is_epd=0) AS equipment_rental_only,
          count(*) FILTER (WHERE c.is_epd=1 AND c.is_equip=0) AS epd_lec_only,
          count(*) FILTER (WHERE c.is_equip=1 AND c.is_epd=1) AS both_tags,
          count(*) FILTER (WHERE c.is_equip=1) AS equipment_rental_total,
          count(*) FILTER (WHERE c.is_epd=1)   AS epd_lec_total,
          count(*)                              AS matched_firms_total
        FROM m JOIN cand c USING (firm_domain)
    """).df().to_string(index=False))

    print("\n  --- per-bucket hit rate (matched / candidates in that bucket) ---")
    print(con.execute(f"""
        WITH m AS (SELECT DISTINCT firm_domain FROM prox)
        SELECT 'equipment_rental' AS bucket,
               {cand_equip} AS candidates,
               count(DISTINCT c.firm_domain) FILTER (WHERE c.is_equip=1) AS matched,
               round(100.0*count(DISTINCT c.firm_domain) FILTER (WHERE c.is_equip=1)/{cand_equip},1) AS hit_pct
        FROM m JOIN cand c USING (firm_domain)
        UNION ALL
        SELECT 'epd_lec_status',
               {cand_epd},
               count(DISTINCT c.firm_domain) FILTER (WHERE c.is_epd=1),
               round(100.0*count(DISTINCT c.firm_domain) FILTER (WHERE c.is_epd=1)/{cand_epd},1)
        FROM m JOIN cand c USING (firm_domain)
    """).df().to_string(index=False))

    # ---- validate the one-PSC-per-award invariant (so SUM(count)=distinct projects) ----
    inv = con.execute("""
        SELECT
          (SELECT sum(nearby_award_count) FROM prox)                                            AS sum_counts,
          (SELECT count(*) FROM (SELECT unnest(nearby_award_keys) k FROM prox))                 AS unnested_keys,
          (SELECT count(*) FROM (SELECT DISTINCT firm_domain, unnest(nearby_award_keys) k FROM prox)) AS distinct_firm_award
    """).df()
    print("\n" + "=" * 78)
    print("2. DEMAND DENSITY  (distinct nearby active projects per matched firm)")
    print("=" * 78)
    print("  invariant check (all three equal ⇒ SUM(nearby_award_count) = distinct projects):")
    print(inv.to_string(index=False))

    con.execute("""
        CREATE TEMP TABLE density AS
        SELECT firm_domain, sum(nearby_award_count) AS nearby_projects
        FROM prox GROUP BY 1
    """)
    print("\n  --- density distribution ---")
    print(con.execute("""
        SELECT
          count(*) FILTER (WHERE nearby_projects = 1)            AS exactly_1,
          count(*) FILTER (WHERE nearby_projects BETWEEN 2 AND 5) AS from_2_to_5,
          count(*) FILTER (WHERE nearby_projects > 5)            AS more_than_5,
          count(*)                                               AS total_matched_firms,
          round(avg(nearby_projects),1)                          AS avg_projects,
          median(nearby_projects)                                AS median_projects,
          max(nearby_projects)                                   AS max_projects
        FROM density
    """).df().to_string(index=False))

    print("\n  --- >5 'golden' segment split by tag ---")
    print(con.execute("""
        WITH g AS (SELECT firm_domain FROM density WHERE nearby_projects > 5)
        SELECT
          count(*) FILTER (WHERE c.is_equip=1 AND c.is_epd=0) AS equipment_rental_only,
          count(*) FILTER (WHERE c.is_epd=1 AND c.is_equip=0) AS epd_lec_only,
          count(*) FILTER (WHERE c.is_equip=1 AND c.is_epd=1) AS both_tags,
          count(*)                                            AS golden_total
        FROM g JOIN cand c USING (firm_domain)
    """).df().to_string(index=False))

    # ---- PSC name map ----
    con.execute("""
        CREATE TEMP TABLE psc_names AS
        SELECT psc_code, any_value(psc_description) AS psc_description
        FROM aw WHERE psc_code IS NOT NULL GROUP BY 1
    """)

    print("\n" + "=" * 78)
    print("3. PSC DISTRIBUTION — TOP 5 by total nearby_award_count (overlap volume)")
    print("=" * 78)
    print(con.execute("""
        SELECT p.psc_code,
               n.psc_description,
               sum(p.nearby_award_count)        AS total_nearby_award_count,
               count(DISTINCT p.firm_domain)    AS firms_in_radius,
               sum(p.nearby_total_award_value)::BIGINT AS total_award_value_usd
        FROM prox p LEFT JOIN psc_names n USING (psc_code)
        GROUP BY 1,2 ORDER BY total_nearby_award_count DESC LIMIT 5
    """).df().to_string(index=False))

    print("\n  --- cross-check: TOP 5 by firm reach (distinct firms in radius) ---")
    print(con.execute("""
        SELECT p.psc_code,
               n.psc_description,
               count(DISTINCT p.firm_domain)    AS firms_in_radius,
               sum(p.nearby_award_count)        AS total_nearby_award_count
        FROM prox p LEFT JOIN psc_names n USING (psc_code)
        GROUP BY 1,2 ORDER BY firms_in_radius DESC LIMIT 5
    """).df().to_string(index=False))

    print("\n  --- proximity-matrix totals (context) ---")
    print(con.execute("""
        SELECT count(*)                          AS matrix_rows,
               count(DISTINCT firm_domain)       AS distinct_firms,
               count(DISTINCT psc_code)          AS distinct_psc,
               sum(nearby_award_count)           AS total_proximity_pairs
        FROM prox
    """).df().to_string(index=False))

    con.close()


if __name__ == "__main__":
    main()
