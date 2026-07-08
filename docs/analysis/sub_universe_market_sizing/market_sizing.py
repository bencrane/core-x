#!/usr/bin/env python3
"""Sub-universe GTM market-sizing — the CANONICAL, re-runnable derivation.

Regenerates EVERY number in README.md in one pass, stamped with the exact Lance
dataset versions it ran against (Lance is versioned, so re-running against the
same versions is bit-exact). Prints all tables and writes results.json alongside
this file so the frozen numbers survive even without a rerun.

    doppler run -p core-x -c prd -- \
      /Users/benjamincrane/core-x/.venv/bin/python \
      docs/analysis/sub_universe_market_sizing/market_sizing.py

Sources (all read-only):
  gtm_sub_profiles         1 row/sub_uei   — sub_amt_60mo (5yr sub $), n_lanes_lifetime,
                                             n_distinct_primes_lifetime
  gtm_prime_combo_lanes    uei×naics×psc   — prime_obl_60mo (5yr prime $), summed per uei
  gtm_sam_entities         1 row/uei       — legal_business_name, primary_naics

Target set (proxy for the 56,672 edge_count_5y>0 universe): subs with sub_amt_60mo > 0.
Materiality = GREATEST(sub 5yr $, prime 5yr $)  ("either as sub or as prime").
Compute proxy = n_lanes_lifetime (distinct demonstrated NAICS×PSC combos) — correlates
with universe breadth, per-target build cost, and hot-blob size.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import duckdb
import lance

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[2]))            # repo root (docs/analysis/<pkg>/ -> root)
from apps.catalyst_api.src import config  # noqa: E402
THRESHOLDS = [0, 50_000, 100_000, 250_000, 500_000, 1_000_000, 5_000_000, 10_000_000]
CUT_TOP = [100, 250, 500, 1000, 2000]
NAICS2 = {"11": "Agriculture", "21": "Mining/Oil/Gas", "22": "Utilities", "23": "Construction",
          "31": "Manufacturing", "32": "Manufacturing", "33": "Manufacturing", "42": "Wholesale",
          "44": "Retail", "45": "Retail", "48": "Transport/Warehouse", "49": "Transport/Warehouse",
          "51": "Information", "52": "Finance", "53": "Real estate", "54": "Prof/Sci/Tech svc",
          "55": "Mgmt of companies", "56": "Admin/Support/Waste", "61": "Educational",
          "62": "Health care", "71": "Arts/Entertainment", "72": "Accommodation/Food",
          "81": "Other services", "92": "Public admin"}


def main() -> int:
    opt = config.r2_storage_options()
    sp = lance.dataset(config.GTM_SUB_PROFILES_URI, storage_options=opt)
    pcl = lance.dataset(config.GTM_PRIME_COMBO_LANES_URI, storage_options=opt)
    sam = lance.dataset(config.GTM_SAM_ENTITIES_URI, storage_options=opt)
    versions = {"gtm_sub_profiles": sp.version, "gtm_prime_combo_lanes": pcl.version,
                "gtm_sam_entities": sam.version}

    con = duckdb.connect()
    con.execute("SET memory_limit='8GB'; SET threads TO 4;")
    con.register("_sp", sp.scanner(columns=["uei", "sub_amt_60mo", "n_lanes_lifetime",
                                            "n_distinct_primes_lifetime"]).to_reader())
    con.register("_pcl", pcl.scanner(columns=["uei", "prime_obl_60mo"]).to_reader())
    con.register("_sam", sam.scanner(columns=["uei", "legal_business_name", "primary_naics"]).to_reader())
    con.execute("CREATE TABLE prime AS SELECT uei, SUM(prime_obl_60mo) p5 FROM _pcl GROUP BY 1")
    con.execute("CREATE TABLE sam AS SELECT uei, legal_business_name nm, primary_naics pn FROM _sam")
    con.execute("""CREATE TABLE t AS
        SELECT s.uei,
               COALESCE(s.sub_amt_60mo, 0) AS sub_5yr,
               COALESCE(p.p5, 0)          AS prime_5yr,
               GREATEST(COALESCE(s.sub_amt_60mo, 0), COALESCE(p.p5, 0)) AS max_5yr,
               COALESCE(s.n_lanes_lifetime, 0) AS n_lanes,
               COALESCE(s.n_distinct_primes_lifetime, 0) AS n_primes
        FROM _sp s LEFT JOIN prime p USING(uei)
        WHERE COALESCE(s.sub_amt_60mo, 0) > 0""")

    R: dict = {"sources": {"uris": {"gtm_sub_profiles": config.GTM_SUB_PROFILES_URI,
                                    "gtm_prime_combo_lanes": config.GTM_PRIME_COMBO_LANES_URI,
                                    "gtm_sam_entities": config.GTM_SAM_ENTITIES_URI},
                           "lance_versions": versions},
               "definitions": {"target_set": "subs with sub_amt_60mo > 0 (proxy for edge_count_5y>0)",
                               "materiality": "GREATEST(sub_amt_60mo, sum prime_obl_60mo per uei)",
                               "compute_proxy": "n_lanes_lifetime (distinct demonstrated NAICS x PSC)"}}

    tot_n, tot_lw = con.execute("SELECT COUNT(*), SUM(n_lanes) FROM t").fetchone()
    R["target_set"] = {"n_targets": tot_n, "total_lane_work": tot_lw}
    print(f"=== Lance versions: {versions} ===")
    print(f"target set: {tot_n:,} subs | total lane-work = {tot_lw:,}\n")

    # 1. materiality thresholds
    print("--- 1. materiality thresholds (either sub OR prime 5yr $) ---")
    print(f"{'threshold':>12}{'targets':>10}{'%kept':>8}{'lane-work':>13}{'%work':>8}")
    R["thresholds"] = []
    for thr in THRESHOLDS:
        n, lw = con.execute(f"SELECT COUNT(*), SUM(n_lanes) FROM t WHERE max_5yr >= {thr}").fetchone()
        row = {"threshold": thr, "targets": n, "pct_targets": round(100 * n / tot_n, 1),
               "lane_work": lw or 0, "pct_work": round(100 * (lw or 0) / tot_lw, 1)}
        R["thresholds"].append(row)
        print(f"{'$'+format(thr,','):>12}{n:>10,}{row['pct_targets']:>7.1f}%{(lw or 0):>13,}{row['pct_work']:>7.1f}%")

    # 2. sector composition of the $5M pool (dominant sector per sub by sub $)
    con.register("_scl", lance.dataset(config.GTM_SUB_COMBO_LANES_URI, storage_options=opt)
                 .scanner(columns=["uei", "naics_code", "sub_amt_60mo"]).to_reader())
    con.execute("CREATE TABLE scl AS SELECT * FROM _scl")
    con.execute("""CREATE TABLE topsec AS
        SELECT uei, arg_max(sector, amt) sector FROM (
          SELECT uei, SUBSTR(naics_code,1,2) sector, SUM(COALESCE(sub_amt_60mo,0)) amt
          FROM scl WHERE uei IN (SELECT uei FROM t WHERE max_5yr >= 5000000)
                     AND naics_code IS NOT NULL GROUP BY 1,2) GROUP BY 1""")
    sec = con.execute("SELECT sector, COUNT(*) subs FROM topsec GROUP BY 1 ORDER BY 2 DESC").fetchall()
    n5m = con.execute("SELECT COUNT(*) FROM t WHERE max_5yr >= 5000000").fetchone()[0]
    R["sector_composition_5M"] = {"pool_n": n5m,
                                  "sectors": [{"naics2": s, "name": NAICS2.get(s, "?"), "subs": c} for s, c in sec]}
    print(f"\n--- 2. sector composition of $5M pool ({n5m:,} subs) ---")
    for s, c in sec[:16]:
        print(f"  {s} {NAICS2.get(s,'?'):<22}{c:>8,}")

    # 3. cut-the-top by breadth
    con.execute("CREATE TABLE r5 AS SELECT *, ROW_NUMBER() OVER (ORDER BY n_lanes DESC) rk FROM t WHERE max_5yr >= 5000000")
    print(f"\n--- 3. cut the top by breadth ($5M pool, {n5m:,} subs, lane-work={con.execute('SELECT SUM(n_lanes) FROM r5').fetchone()[0]:,}) ---")
    lw5 = con.execute("SELECT SUM(n_lanes) FROM r5").fetchone()[0]
    R["cut_the_top"] = {"pool_n": n5m, "pool_lane_work": lw5, "rows": []}
    print(f"{'remove top':>12}{'subs left':>10}{'lane-work left':>16}{'%compute left':>15}")
    for cut in CUT_TOP:
        n, lw = con.execute(f"SELECT COUNT(*), SUM(n_lanes) FROM r5 WHERE rk > {cut}").fetchone()
        R["cut_the_top"]["rows"].append({"remove_top": cut, "subs_left": n, "lane_work_left": lw,
                                         "pct_compute_left": round(100 * lw / lw5, 1)})
        print(f"{cut:>12}{n:>10,}{lw:>16,}{100*lw/lw5:>14.1f}%")

    # 4. breadth distribution + size-budget risk
    q = {l: con.execute(f"SELECT QUANTILE_CONT(n_lanes,{p}) FROM t WHERE max_5yr>=5000000").fetchone()[0]
         for p, l in [(0.5, "p50"), (0.9, "p90"), (0.99, "p99"), (1.0, "max")]}
    wide = con.execute("SELECT COUNT(*) FROM t WHERE max_5yr>=5000000 AND n_lanes>=100").fetchone()[0]
    vwide = con.execute("SELECT COUNT(*) FROM t WHERE max_5yr>=5000000 AND n_lanes>=200").fetchone()[0]
    R["breadth_distribution_5M"] = {"quantiles": q, "n_lanes_ge_100": wide, "n_lanes_ge_200": vwide}
    print(f"\n--- 4. breadth distribution ($5M pool) ---")
    print(f"  n_lanes p50={q['p50']:.0f} p90={q['p90']:.0f} p99={q['p99']:.0f} max={q['max']:.0f} | >=100:{wide} >=200:{vwide}")

    # 5. widest subs by name (the reseller/distributor/staffing finding)
    wide_rows = con.execute("""
        SELECT COALESCE(a.nm,'(no SAM name)') nm, t.n_lanes, t.n_primes, t.sub_5yr, a.pn
        FROM t LEFT JOIN sam a USING(uei) ORDER BY t.n_lanes DESC LIMIT 20""").fetchall()
    R["widest_subs"] = [{"name": nm, "n_lanes": nl, "n_primes": npr, "sub_5yr": int(s5 or 0),
                         "primary_naics": pn} for nm, nl, npr, s5, pn in wide_rows]
    print(f"\n--- 5. widest subs by name ---")
    for nm, nl, npr, s5, pn in wide_rows:
        print(f"  {(nm or '')[:38]:<39}{nl:>5} lanes {npr:>4} primes  ${int(s5 or 0):>14,}  {pn or ''}")

    out = HERE / "results.json"
    out.write_text(json.dumps(R, indent=2, default=str))
    print(f"\nwrote {out}")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
