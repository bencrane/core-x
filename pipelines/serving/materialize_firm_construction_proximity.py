"""Serving build — `govcon_firm_construction_proximity`.

SoR  s3://data-sink/active/govcon_firm_construction_proximity/  (Lance v2.1; derived, overwrite)

WHAT THIS IS
Pre-computed firm × PSC proximity matrix for the GTM Equipment Rental motion. For every
candidate firm flagged by an enrichment workflow, finds every active federal construction
award within 50 miles of the firm's HQ and aggregates by Product/Service Code. Purely
geometric — no industry filter on the firm side, no agency filter on the award side — so
the same matrix powers many downstream selection workflows without re-running the math.

UNIVERSE (firm side)
  companies_canonical (company_id 1:1 PK; source_platform extracted to the sidecar)
   ⨝ company_source_platforms ON company_id  WHERE source_platform IN
       (epd_lec_status_candidates, equipment_rental_candidates;  bare + 'enrichment:' prefix)
   ⨝ company_addresses ON normalized_domain = domain_norm           — picks up uei + postal
   ⨝ zcta_zip_centroids ON winner_postal_code = zcta5               — picks up (lat, lon)
   one row per firm_domain (collapsed across multi-UEI SAM domains, preferring uei-present)

UNIVERSE (award side)
  govcon_active_awards WHERE construction_wage_rate_requirements = 'YES'
                         AND pop_current_end >= current_date()
   ⨝ zcta_zip_centroids ON substring(pop_zip, 1, 5) = zcta5

SPATIAL FILTER
  Bounding-box pre-filter:   abs(Δlat) ≤ 0.75   AND   abs(Δlon) ≤ 1.00
    — 0.75° lat ≈ 52 mi (cos-of-latitude-invariant; tight to ~50 mi)
    — 1.00° lon ≈ 50 mi at 50°N (broader at mid-US latitudes) — keeps the box safe across
      the contiguous-US latitude range without false-negative drops near 50 mi east-west.
  Haversine strict:          ≤ 50.0 miles      (3958.8 mi earth radius)

GRAIN
  (firm_uei NULLABLE, firm_domain, psc_code) → COUNT(awards), SUM(value), LIST(award_keys).

PRIMARY IDs
  firm_domain + firm_uei both BTREE-indexed for instant point-lookups.

    doppler run --project core-x --config prd -- python3 pipelines/serving/materialize_firm_construction_proximity.py
    doppler run --project core-x --config prd -- python3 pipelines/serving/materialize_firm_construction_proximity.py --verify
"""

from __future__ import annotations

import os
import sys

A = "s3://data-sink/active"
# REPOINT → companies_canonical (source_platform extracted to the company_source_platforms
# sidecar; the source-tag firm filter below is a JOIN to the sidecar, not a companies column).
COMPANIES_URI = os.environ.get("GTM_COMPANIES_URI", f"{A}/companies_canonical/")
COMPANY_SOURCE_PLATFORMS_URI = os.environ.get(
    "COMPANY_SOURCE_PLATFORMS_URI", f"{A}/company_source_platforms/")
ADDRESSES_URI = f"{A}/company_addresses/"
AWARDS_URI    = f"{A}/govcon_active_awards/"
ZCTA_URI      = f"{A}/zcta_zip_centroids/"
SERVING_URI   = os.environ.get("PROXIMITY_URI", f"{A}/govcon_firm_construction_proximity/")
DATA_STORAGE_VERSION = "2.1"
DUCK_MEM = os.environ.get("DUCK_MEM", "12GB")

CANDIDATE_PLATFORMS = (
    "enrichment:epd_lec_status_candidates",
    "epd_lec_status_candidates",
    "enrichment:equipment_rental_candidates",
    "equipment_rental_candidates",
)

BTREE_COLS = ["firm_domain", "firm_uei", "psc_code"]
BITMAP_COLS: list[str] = []  # psc_code is high-cardinality; keep BTREE-only


def _r2_storage_options() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID.")
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": endpoint,
        "region": "auto",
    }


def build() -> dict:
    import duckdb
    import lance

    so = _r2_storage_options()
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=8")
    con.execute(f"SET memory_limit='{DUCK_MEM}'")
    con.register("co",   lance.dataset(COMPANIES_URI, storage_options=so))
    con.register("csp",  lance.dataset(COMPANY_SOURCE_PLATFORMS_URI, storage_options=so))
    con.register("ca",   lance.dataset(ADDRESSES_URI, storage_options=so))
    con.register("aw",   lance.dataset(AWARDS_URI, storage_options=so))
    con.register("zcta", lance.dataset(ZCTA_URI, storage_options=so))

    # ── 1. Candidate firms (filtered to ~enrichment tags only). source_platform now lives in
    #    the company_source_platforms sidecar (csp), so the firm filter is a JOIN on company_id
    #    into the sidecar, NOT a WHERE on a companies column. ──
    placeholders = ",".join(f"'{p}'" for p in CANDIDATE_PLATFORMS)
    con.execute(f"""
        CREATE TEMP TABLE target_candidates AS
        SELECT DISTINCT
               nullif(trim(co.normalized_domain), '') AS firm_domain
        FROM co
        JOIN csp ON csp.company_id = co.company_id
        WHERE csp.source_platform IN ({placeholders})
          AND co.normalized_domain IS NOT NULL
    """)

    # ── 2. Resolve candidates → (uei, domain, postal) via company_addresses, deduped to 1 row
    # per firm_domain (preferring uei-present rows over uei-null on the same domain). ──
    con.execute("""
        CREATE TEMP TABLE target_firm_postals AS
        SELECT firm_uei, firm_domain, winner_postal_code FROM (
            SELECT
              ca.uei                AS firm_uei,
              ca.domain_norm        AS firm_domain,
              ca.winner_postal_code,
              ROW_NUMBER() OVER (
                PARTITION BY ca.domain_norm
                ORDER BY CASE WHEN ca.uei IS NOT NULL THEN 0 ELSE 1 END, ca.uei
              ) AS rn
            FROM target_candidates c
            JOIN ca ON ca.domain_norm = c.firm_domain
            WHERE ca.winner_postal_code IS NOT NULL
        ) WHERE rn = 1
    """)

    # ── 3. Firm coords ──
    con.execute("""
        CREATE TEMP TABLE target_firms AS
        SELECT
          f.firm_uei,
          f.firm_domain,
          z.lat AS firm_lat,
          z.lon AS firm_lon
        FROM target_firm_postals f
        JOIN zcta z ON z.zcta5 = f.winner_postal_code
        WHERE z.lat IS NOT NULL AND z.lon IS NOT NULL
    """)

    # ── 4. Active construction awards + coords (column corrections vs directive: pop_zip,
    # not pop_zip_code; psc_code is the actual column name). ──
    con.execute("""
        CREATE TEMP TABLE active_awards AS
        SELECT
          a.contract_award_unique_key,
          a.psc_code,
          a.current_total_value_of_award,
          z.lat AS award_lat,
          z.lon AS award_lon
        FROM aw a
        JOIN zcta z ON z.zcta5 = substring(a.pop_zip, 1, 5)
        WHERE a.construction_wage_rate_requirements = 'YES'
          AND a.pop_current_end >= current_date()
          AND a.psc_code IS NOT NULL
          AND z.lat IS NOT NULL AND z.lon IS NOT NULL
    """)

    n_firms  = con.execute("SELECT count(*) FROM target_firms").fetchone()[0]
    n_awards = con.execute("SELECT count(*) FROM active_awards").fetchone()[0]
    print(f"target_firms: {n_firms:,}  active_construction_awards: {n_awards:,}")
    assert n_firms > 0 and n_awards > 0, "empty universe — nothing to materialize"

    # ── 5. Cross-join with bbox + strict Haversine. Bbox eliminates ~99% of pairs before
    # the trig runs. Single Haversine call via a CTE prevents double-evaluation. ──
    con.execute("""
        CREATE TEMP TABLE distance_matches AS
        WITH pairs AS (
          SELECT
            f.firm_uei, f.firm_domain,
            a.psc_code, a.contract_award_unique_key, a.current_total_value_of_award,
            a.award_lat, a.award_lon,
            f.firm_lat, f.firm_lon
          FROM target_firms f
          CROSS JOIN active_awards a
          WHERE abs(a.award_lat - f.firm_lat) <= 0.75
            AND abs(a.award_lon - f.firm_lon) <= 1.00
        )
        SELECT
          firm_uei, firm_domain, psc_code,
          contract_award_unique_key, current_total_value_of_award,
          (3958.8 * 2 * ASIN(SQRT(
              POWER(SIN(RADIANS(award_lat - firm_lat) / 2), 2) +
              COS(RADIANS(firm_lat)) * COS(RADIANS(award_lat)) *
              POWER(SIN(RADIANS(award_lon - firm_lon) / 2), 2)
          ))) AS distance_miles
        FROM pairs
        WHERE (3958.8 * 2 * ASIN(SQRT(
              POWER(SIN(RADIANS(award_lat - firm_lat) / 2), 2) +
              COS(RADIANS(firm_lat)) * COS(RADIANS(award_lat)) *
              POWER(SIN(RADIANS(award_lon - firm_lon) / 2), 2)
          ))) <= 50.0
    """)

    n_matches = con.execute("SELECT count(*) FROM distance_matches").fetchone()[0]
    print(f"surviving pairs (≤ 50 mi): {n_matches:,}")

    # ── 6. Aggregate to (firm × psc) grain ──
    tbl = con.execute("""
        SELECT
          firm_uei,
          firm_domain,
          psc_code,
          CAST(count(contract_award_unique_key) AS BIGINT)         AS nearby_award_count,
          CAST(sum(current_total_value_of_award) AS DOUBLE)        AS nearby_total_award_value,
          list(contract_award_unique_key)                          AS nearby_award_keys,
          now() AS materialized_at
        FROM distance_matches
        GROUP BY 1, 2, 3
        ORDER BY firm_domain, psc_code
    """).to_arrow_table()

    rows = tbl.num_rows
    distinct_firms = con.execute(
        "SELECT count(DISTINCT firm_domain) FROM distance_matches").fetchone()[0]
    distinct_psc = con.execute(
        "SELECT count(DISTINCT psc_code) FROM distance_matches").fetchone()[0]
    print(f"output rows: {rows:,}  distinct_firms_with_matches: {distinct_firms:,}  distinct_psc: {distinct_psc:,}")
    assert rows > 0, "no rows produced"

    lance.write_dataset(
        tbl, SERVING_URI, mode="overwrite",
        data_storage_version=DATA_STORAGE_VERSION, storage_options=so,
    )
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
    return {"uri": SERVING_URI, "rows": back,
            "firms_with_matches": distinct_firms, "distinct_psc": distinct_psc}


def verify() -> None:
    import duckdb
    import lance

    so = _r2_storage_options()
    ds = lance.dataset(SERVING_URI, storage_options=so)
    print(f"{SERVING_URI}  rows={ds.count_rows():,}  cols={len(ds.schema)}")
    print("indices:", sorted(
        (i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i))) for i in ds.list_indices()))
    con = duckdb.connect(":memory:"); con.register("d", ds)
    print("\n=== top PSCs by reach (firms × awards) ===")
    print(con.execute("""
        SELECT psc_code,
               count(DISTINCT firm_domain) firms_in_radius,
               sum(nearby_award_count) total_award_pairs,
               sum(nearby_total_award_value)::BIGINT total_value
        FROM d GROUP BY 1 ORDER BY 2 DESC LIMIT 15
    """).df().to_string(index=False))
    print("\n=== firm fan-out ===")
    print(con.execute("""
        SELECT
          count(DISTINCT firm_domain) AS distinct_firms,
          count(DISTINCT psc_code)    AS distinct_psc,
          count(*)                    AS total_rows,
          round(avg(nearby_award_count), 1)   AS avg_awards_per_firm_psc,
          max(nearby_award_count)             AS max_awards_in_a_psc_for_one_firm
        FROM d
    """).df().to_string(index=False))


if __name__ == "__main__":
    (verify if "--verify" in sys.argv else build)()
