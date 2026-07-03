"""Report-prep — derive final report tables from the local parquet artifacts. Read-only, no R2."""
from __future__ import annotations
import os
import duckdb

OUT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "reports", "dsbs_overlap")
con = duckdb.connect()
ov = f"read_parquet('{OUT}/overlap_combos.parquet')"
dm = f"read_parquet('{OUT}/demand_combos.parquet')"
es = f"read_parquet('{OUT}/entity_saturation.parquet')"


def money(x):
    return f"${x/1e9:,.2f}B" if abs(x) >= 1e9 else f"${x/1e6:,.1f}M"


print("### TOP 25 ADDRESSABLE COMBOS (by active obligated $) — markdown rows")
rows = con.execute(f"""
  SELECT naics, psc, substr(naics_desc,1,34) nd, substr(psc_desc,1,34) pd,
         n_awards, obligated, unspent, n_dsbs_firms, n_dsbs_primed, n_dsbs_subbed, obl_per_firm
  FROM {ov} ORDER BY obligated DESC LIMIT 25""").fetchall()
for i, r in enumerate(rows, 1):
    naics, psc, nd, pd, na, obl, uns, nf, npd, nsb, opf = r
    print(f"| {i} | {naics} | {psc} | {nd} | {na} | ${obl/1e6:,.0f}M | ${uns/1e6:,.0f}M | {nf} | {npd}/{nsb} | ${opf/1e6:,.1f}M |")

print("\n### SATURATION — collapsed bands")
sat = con.execute(f"""
  SELECT
    count(*) FILTER (WHERE n_active_targets=1) b1,
    count(*) FILTER (WHERE n_active_targets BETWEEN 2 AND 5) b2_5,
    count(*) FILTER (WHERE n_active_targets>=6) b6p,
    count(*) FILTER (WHERE n_active_targets BETWEEN 6 AND 20) b6_20,
    count(*) FILTER (WHERE n_active_targets BETWEEN 21 AND 100) b21_100,
    count(*) FILTER (WHERE n_active_targets>100) b100p,
    count(*) total, round(median(n_active_targets),0) med, round(avg(n_active_targets),0) avg, max(n_active_targets) mx
  FROM {es}""").fetchone()
print(f"1:{sat[0]:,}  2-5:{sat[1]:,}  6+:{sat[2]:,}  (of which 6-20:{sat[3]:,} 21-100:{sat[4]:,} 100+:{sat[5]:,})  total_firms:{sat[6]:,}  med:{int(sat[7])} avg:{int(sat[8])} max:{sat[9]:,}")

print("\n### THIN-MARKET LEVERAGE — high $ + few capable DSBS firms (obligated>=$1B AND n_dsbs_firms<=5)")
thin = con.execute(f"""
  SELECT naics, psc, substr(naics_desc,1,32) nd, n_awards, obligated, unspent, n_dsbs_firms, n_dsbs_primed, n_dsbs_subbed
  FROM {ov} WHERE obligated>=1e9 AND n_dsbs_firms<=5 ORDER BY obligated DESC LIMIT 20""").fetchall()
print(f"(count of such combos: {con.execute(f'SELECT count(*) FROM {ov} WHERE obligated>=1e9 AND n_dsbs_firms<=5').fetchone()[0]})")
for r in thin:
    print(f"| {r[0]} | {r[1]} | {r[2]} | {r[3]} | ${r[4]/1e6:,.0f}M | ${r[5]/1e6:,.0f}M | {r[6]} | {r[7]}/{r[8]} |")

print("\n### DEPTH MARKETS — high $ + deep DSBS bench (obligated>=$1B AND n_dsbs_firms>=100)")
deep = con.execute(f"""
  SELECT count(*) n, sum(obligated) obl, sum(unspent) uns FROM {ov} WHERE obligated>=1e9 AND n_dsbs_firms>=100""").fetchone()
print(f"combos:{deep[0]}  obligated:{money(deep[1])}  unspent:{money(deep[2])}")

print("\n### SUPPLY-THICKNESS x DEMAND cross-tab (addressable combos)")
xt = con.execute(f"""
  WITH b AS (SELECT *,
     CASE WHEN n_dsbs_firms=1 THEN '1 firm' WHEN n_dsbs_firms<=5 THEN '2-5' WHEN n_dsbs_firms<=25 THEN '6-25'
          WHEN n_dsbs_firms<=100 THEN '26-100' ELSE '100+' END AS supply_band
     FROM {ov})
  SELECT supply_band, count(*) combos, sum(n_awards) awards, round(sum(obligated)/1e9,1) obl_gb, round(sum(unspent)/1e9,1) uns_gb
  FROM b GROUP BY supply_band
  ORDER BY CASE supply_band WHEN '1 firm' THEN 1 WHEN '2-5' THEN 2 WHEN '6-25' THEN 3 WHEN '26-100' THEN 4 ELSE 5 END""").fetchall()
for r in xt:
    print(f"| {r[0]:8s} | {r[1]:>5} combos | {r[2]:>6} awards | ${r[3]:>7}B obl | ${r[4]:>7}B unspent |")

print("\n### NON-ADDRESSABLE GAP — top active combos where NO DSBS firm has proven (by obligated $)")
gap = con.execute(f"""
  SELECT d.naics, d.psc, substr(d.naics_desc,1,34) nd, substr(d.psc_desc,1,30) pd, d.n_awards, d.obligated, d.unspent
  FROM {dm} d WHERE d.combo NOT IN (SELECT combo FROM {ov}) ORDER BY d.obligated DESC LIMIT 12""").fetchall()
gap_tot = con.execute(f"SELECT count(*), sum(n_awards), sum(obligated) FROM {dm} WHERE combo NOT IN (SELECT combo FROM {ov})").fetchone()
print(f"(non-addressable: {gap_tot[0]:,} combos, {gap_tot[1]:,} awards, {money(gap_tot[2])} obligated)")
for r in gap:
    print(f"| {r[0]} | {r[1]} | {r[2]} | {r[3]} | {r[4]} | ${r[5]/1e6:,.0f}M | ${r[6]/1e6:,.0f}M |")

print("\n### PROVENANCE of addressable overlap (prime-proof vs sub-proof depth)")
prov = con.execute(f"""
  SELECT
    count(*) FILTER (WHERE n_dsbs_primed>0 AND n_dsbs_subbed>0) via_both,
    count(*) FILTER (WHERE n_dsbs_primed>0 AND n_dsbs_subbed=0) prime_only,
    count(*) FILTER (WHERE n_dsbs_primed=0 AND n_dsbs_subbed>0) sub_only
  FROM {ov}""").fetchone()
print(f"addressable combos backed by: both prime&sub proof:{prov[0]:,}  prime-only:{prov[1]:,}  sub-only:{prov[2]:,}")
