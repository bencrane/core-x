#!/usr/bin/env python3
"""gtm_award_site_proximity / gtm_subaward_site_proximity — nearest federal site per award event.

SoR  s3://data-sink/active/gtm_award_site_proximity/     (prime award grain; BTREE uei, award key)
     s3://data-sink/active/gtm_subaward_site_proximity/  (subaward grain;   BTREE uei, subaward key)

The centroid↔site join (operator-authorized build 2026-07-06; v2 additions same day):
each award/subaward place-of-performance centroid matched to its NEAREST federal site —
any-source AND per-source (nearest GSA building/lease, nearest military base) — with
the awarding office code/name carried verbatim. The subaward lane additionally carries
the PRIME award's PoP nearest sites (join via prime_award_unique_key; FPDS contracts
only — grant primes have no FPDS row). No scoring, no tiers — raw facts; entity
rollups compose at query time.

SITES: federal_sites_lance restricted to military_base + gsa_building + gsa_lease
(9,237 sites). FRPP's 291K civilian assets are deliberately excluded (they'd make
"near a federal site" true nearly everywhere); addable later as another site_source.

NEAREST is computed within a 3x3 grid of 1-degree cells around the PoP (~70mi+ reach);
PoPs with no site inside that window get NULL site/miles — a documented cutoff, not a
claim that nothing exists farther out. US-only centroids (country_code = 'USA'/'US').

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'pylance>=8' --with 'pyarrow>=17' --with 'duckdb>=1.5,<2' --with boto3 \
      python3 scripts/build_gtm_award_site_proximity.py [--verify]
"""
from __future__ import annotations

import os
import sys
from datetime import date
from pathlib import Path

import duckdb
import lance

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pipelines._shared.lance_local_publish import write_indexed_dataset  # noqa: E402

A = "s3://data-sink/active"
OUT_PRIME = f"{A}/gtm_award_site_proximity/"
OUT_SUB = f"{A}/gtm_subaward_site_proximity/"
PARAM_SET_ID = "v1"
SITE_SOURCES = "('military_base', 'gsa_building', 'gsa_lease')"

HAVERSINE = """7917.6 * asin(sqrt(
    sin(radians(s.slat - c.lat) / 2) ^ 2
    + cos(radians(c.lat)) * cos(radians(s.slat))
      * sin(radians(s.slon - c.lon) / 2) ^ 2))"""


def so() -> dict:
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": os.environ.get("R2_ENDPOINT") or f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
            "region": "auto"}


def _cx():
    con = duckdb.connect()
    con.execute("SET memory_limit='20GB'; SET threads TO 4; SET preserve_insertion_order=false;")
    os.makedirs("/tmp/duck_spill", exist_ok=True)
    con.execute("SET temp_directory='/tmp/duck_spill';")
    return con


def _load_sites(con, opt) -> int:
    fs = lance.dataset(f"{A}/federal_sites_lance/", storage_options=opt)
    con.register("_fs", fs.scanner(
        columns=["site_name", "site_source", "state_code", "latitude", "longitude"],
        filter=f"site_source IN {SITE_SOURCES} AND latitude IS NOT NULL").to_reader())
    # expand each site into its 3x3 neighborhood of 1-degree cells
    con.execute("""CREATE TABLE sites AS
        SELECT f.site_name, f.site_source, f.state_code,
               f.latitude AS slat, f.longitude AS slon,
               CAST(floor(f.latitude) AS INT) + dy AS cell_lat,
               CAST(floor(f.longitude) AS INT) + dx AS cell_lon
        FROM _fs f
        CROSS JOIN (VALUES (-1), (0), (1)) AS oy(dy)
        CROSS JOIN (VALUES (-1), (0), (1)) AS ox(dx)""")
    return con.execute("SELECT COUNT(DISTINCT site_name || site_source) FROM sites").fetchone()[0]


def _nearest(con, cent_table: str, key_col: str) -> None:
    """cent_table(key, lat, lon, ...) -> TABLE nearest(key, nearest_site*, miles)."""
    con.execute(f"""CREATE TABLE nearest AS
        SELECT {key_col},
               arg_min(site_name, dist_mi) AS nearest_site_name,
               arg_min(site_source, dist_mi) AS nearest_site_source,
               arg_min(state_code, dist_mi) AS nearest_site_state,
               ROUND(MIN(dist_mi), 1) AS nearest_site_miles,
               arg_min(site_name, dist_mi) FILTER (site_source LIKE 'gsa%') AS nearest_gsa_name,
               ROUND(MIN(dist_mi) FILTER (site_source LIKE 'gsa%'), 1) AS nearest_gsa_miles,
               arg_min(site_name, dist_mi) FILTER (site_source = 'military_base') AS nearest_base_name,
               ROUND(MIN(dist_mi) FILTER (site_source = 'military_base'), 1) AS nearest_base_miles
        FROM (
            SELECT c.{key_col}, s.site_name, s.site_source, s.state_code,
                   {HAVERSINE} AS dist_mi
            FROM {cent_table} c
            JOIN sites s ON s.cell_lat = CAST(floor(c.lat) AS INT)
                        AND s.cell_lon = CAST(floor(c.lon) AS INT))
        GROUP BY 1""")


def build() -> int:
    opt = so()
    as_of = date.today().isoformat()
    con = _cx()
    n_sites = _load_sites(con, opt)
    print(f"sites: {n_sites:,} (military_base + gsa_building + gsa_lease)", flush=True)

    # ── prime side (first: the sub lane joins its nearest results) ────────────
    ac = lance.dataset(f"{A}/usaspending_award_pop_centroids/", storage_options=opt)
    con.register("_ac", ac.scanner(
        columns=["generated_unique_award_id", "latitude", "longitude",
                 "geo_precision", "country_code"],
        filter="latitude IS NOT NULL").to_reader())
    con.execute("""CREATE TABLE cent_pr AS
        SELECT generated_unique_award_id, latitude AS lat, longitude AS lon, geo_precision
        FROM _ac WHERE country_code IN ('USA', 'US', 'UNITED STATES')""")
    st = lance.dataset(f"{A}/usaspending_fpds_prime_award_state/", storage_options=opt)
    con.register("_st", st.scanner(
        columns=["contract_award_unique_key", "recipient_uei",
                 "life_to_date_obligated", "last_action_date"],
        filter="recipient_uei IS NOT NULL").to_reader())
    con.execute("CREATE TABLE pr_meta AS SELECT * FROM _st")
    tx = lance.dataset(f"{A}/usaspending_fpds_canonical_txn/", storage_options=opt)
    con.register("_tx", tx.scanner(
        columns=["contract_award_unique_key", "awarding_office_code",
                 "awarding_office_name"],
        filter="awarding_office_code IS NOT NULL").to_reader())
    con.execute("""CREATE TABLE office AS
        SELECT contract_award_unique_key,
               any_value(awarding_office_code) AS awarding_office_code,
               any_value(awarding_office_name) AS awarding_office_name
        FROM _tx GROUP BY 1""")
    _nearest(con, "cent_pr", "generated_unique_award_id")
    con.execute("ALTER TABLE nearest RENAME TO nearest_pr")
    con.execute("""CREATE TABLE out_pr AS
        SELECT m.contract_award_unique_key, m.recipient_uei AS uei,
               m.life_to_date_obligated, m.last_action_date,
               o.awarding_office_code, o.awarding_office_name,
               c.geo_precision,
               n.nearest_site_name, n.nearest_site_source, n.nearest_site_state,
               n.nearest_site_miles,
               n.nearest_gsa_name, n.nearest_gsa_miles,
               n.nearest_base_name, n.nearest_base_miles
        FROM pr_meta m
        JOIN cent_pr c ON c.generated_unique_award_id = m.contract_award_unique_key
        LEFT JOIN office o USING (contract_award_unique_key)
        LEFT JOIN nearest_pr n ON n.generated_unique_award_id = m.contract_award_unique_key""")
    for t in ("cent_pr", "pr_meta", "office"):
        con.execute(f"DROP TABLE {t}")
    np_, npm = con.execute(
        "SELECT COUNT(*), COUNT(nearest_site_name) FROM out_pr").fetchone()
    print(f"prime awards: {np_:,} rows, {npm:,} with a site in window", flush=True)

    # ── subaward side ─────────────────────────────────────────────────────────
    sc = lance.dataset(f"{A}/usaspending_subaward_pop_centroids/", storage_options=opt)
    con.register("_sc", sc.scanner(
        columns=["subaward_unique_key", "latitude", "longitude", "geo_precision", "country_code"],
        filter="latitude IS NOT NULL").to_reader())
    con.execute("""CREATE TABLE cent_sub AS
        SELECT subaward_unique_key, latitude AS lat, longitude AS lon, geo_precision
        FROM _sc WHERE country_code IN ('USA', 'US', 'UNITED STATES')""")
    se = lance.dataset(f"{A}/usaspending_subaward_canonical/", storage_options=opt)
    con.register("_se", se.scanner(
        columns=["subaward_unique_key", "subawardee_uei", "subaward_amount",
                 "subaward_action_date", "prime_award_unique_key",
                 "prime_award_awarding_office_code",
                 "prime_award_awarding_office_name"],
        filter="subawardee_uei IS NOT NULL").to_reader())
    con.execute("CREATE TABLE sub_meta AS SELECT * FROM _se")
    _nearest(con, "cent_sub", "subaward_unique_key")
    con.execute("""CREATE TABLE out_sub AS
        SELECT m.subaward_unique_key, m.subawardee_uei AS uei, m.subaward_amount,
               m.subaward_action_date, m.prime_award_unique_key,
               m.prime_award_awarding_office_code AS awarding_office_code,
               m.prime_award_awarding_office_name AS awarding_office_name,
               c.geo_precision,
               n.nearest_site_name, n.nearest_site_source, n.nearest_site_state,
               n.nearest_site_miles,
               n.nearest_gsa_name, n.nearest_gsa_miles,
               n.nearest_base_name, n.nearest_base_miles,
               p.nearest_site_name AS prime_pop_nearest_site_name,
               p.nearest_site_source AS prime_pop_nearest_site_source,
               p.nearest_site_miles AS prime_pop_nearest_site_miles,
               p.nearest_gsa_name AS prime_pop_nearest_gsa_name,
               p.nearest_gsa_miles AS prime_pop_nearest_gsa_miles,
               p.nearest_base_name AS prime_pop_nearest_base_name,
               p.nearest_base_miles AS prime_pop_nearest_base_miles
        FROM sub_meta m
        JOIN cent_sub c USING (subaward_unique_key)
        LEFT JOIN nearest n USING (subaward_unique_key)
        LEFT JOIN nearest_pr p ON p.generated_unique_award_id = m.prime_award_unique_key""")
    for t in ("nearest", "cent_sub", "sub_meta", "nearest_pr"):
        con.execute(f"DROP TABLE {t}")
    ns, nsm, npp = con.execute(
        "SELECT COUNT(*), COUNT(nearest_site_name), COUNT(prime_pop_nearest_site_name) "
        "FROM out_sub").fetchone()
    print(f"subawards: {ns:,} rows, {nsm:,} with a site in window, "
          f"{npp:,} with prime-PoP site", flush=True)

    # ── publish ───────────────────────────────────────────────────────────────
    fs_v = lance.dataset(f"{A}/federal_sites_lance/", storage_options=opt).version
    for table, uri, btree, built_from in (
        ("out_sub", OUT_SUB, ["uei", "subaward_unique_key"],
         f"usaspending_subaward_canonical:v{se.version}|usaspending_subaward_pop_centroids:v{sc.version}|federal_sites_lance:v{fs_v}"),
        ("out_pr", OUT_PRIME, ["uei", "contract_award_unique_key"],
         f"usaspending_fpds_prime_award_state:v{st.version}|usaspending_award_pop_centroids:v{ac.version}|"
         f"usaspending_fpds_canonical_txn:v{tx.version}|federal_sites_lance:v{fs_v}"),
    ):
        res = con.execute(f"""SELECT *, DATE '{as_of}' AS as_of,
            '{built_from}' AS built_from_version, '{PARAM_SET_ID}' AS param_set_id
            FROM {table}""")
        reader = res.to_arrow_reader(65536) if hasattr(res, "to_arrow_reader") else res.fetch_record_batch(65536)
        ds = write_indexed_dataset(reader, uri, [(c, "BTREE") for c in btree], storage_options=opt)
        print(f"wrote {uri}  v{ds.version}  rows={ds.count_rows():,}  "
              f"indices={[i['name'] for i in ds.list_indices()]}", flush=True)
    return 0


def verify() -> int:
    opt = so()
    ok = True
    for uri, floor_rows in ((OUT_SUB, 1_000_000), (OUT_PRIME, 10_000_000)):
        ds = lance.dataset(uri, storage_options=opt)
        rows = ds.count_rows()
        with_site = ds.count_rows(filter="nearest_site_name IS NOT NULL")
        idx = [i["name"] for i in ds.list_indices()]
        good = rows >= floor_rows and with_site > 0.5 * rows and "uei_idx" in idx
        ok &= good
        print(f"{uri}: rows={rows:,} with_site={with_site:,} ({100*with_site/rows:.0f}%) "
              f"indices={idx} -> {'OK' if good else 'FAIL'}", flush=True)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(verify() if "--verify" in sys.argv else build())
