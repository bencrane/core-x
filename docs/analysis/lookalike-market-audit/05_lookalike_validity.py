"""05 — Is the base lookalike actually a lookalike? (FRONT 2 core).

Four proofs, all against the pinned artifact:
  A. FALSE POSITIVE: the wt-ranked top-10 for CARAHSOFT (software reseller under
     IT-services giants) and GLENAIR (aerospace-connector maker under
     Lockheed/Boeing/Raytheon) are the SAME firms — tiny mono-line 541330 shops.
  B. THRESHOLD SENSITIVITY: how the count and the top-50 SET move as sig_rank /
     sig_share / min_lane_hits move within valid ranges.
  C. IDF-wt vs a principled cosine similarity to the seed centroid: top-50
     overlap, and what each surfaces.
  D. FALSE NEGATIVE / mis-ranking: where the seeds' true structural peers (the
     billion-dollar primes) actually land in the wt ranking.
"""
import math, os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lib_sidecar import q, cand_sql, inlist, shape_naics, PIN_ARTIFACT

CARA, GLEN = "DT8KJHZXVJH5", "YVBHN7GKLQ44"
print(f"# pinned artifact: {PIN_ARTIFACT}\n")

def names(ueis):
    out = {}
    for i in range(0, len(ueis), 4000):
        for r in q(f"SELECT uei,legal_business_name FROM gtm_sam_entities WHERE uei IN ({inlist(ueis[i:i+4000])})", 4100):
            out[r["uei"]] = r["legal_business_name"]
    return out

def cand(u):
    rows = q(cand_sql(u), limit=25000)   # ordered wt desc, uei
    return rows

print("== A. FALSE POSITIVE: wt-ranked top-10, two utterly different targets ==")
for u, label in [(CARA, "CARAHSOFT (software reseller)"), (GLEN, "GLENAIR (aerospace connectors)")]:
    rows = cand(u); top = rows[:10]; nm = names([r["uei"] for r in top])
    print(f"  -- {label}: top-10 by wt --")
    for r in top:
        print(f"     {str(nm.get(r['uei']))[:34]:<34} obl60={float(r['prime_obl_60mo'])/1e6:>7.0f}M "
              f"top_naics={r['top_naics']} wt={float(r['wt']):.4f} lane_hits={r['lane_hits']}")
cara_top10 = [r["uei"] for r in cand(CARA)[:10]]
glen_top10 = [r["uei"] for r in cand(GLEN)[:10]]
print(f"  IDENTICAL top-10 across the two targets: {sorted(cara_top10) == sorted(glen_top10)} "
      f"(shared {len(set(cara_top10) & set(glen_top10))}/10)\n")

print("== B. THRESHOLD SENSITIVITY (Carahsoft; raw candidate set) ==")
base = cand(CARA); base_top = set(r["uei"] for r in base[:50]); base_n = len(base)
print(f"  DEFAULT sig_rank=5 sig_share=0.05 min_lane_hits=2 -> count={base_n} (top-50 baseline)")
for lbl, kw in [("sig_rank=3", dict(sr=3)), ("sig_rank=8", dict(sr=8)),
                ("sig_share=0.02", dict(ss=0.02)), ("sig_share=0.10", dict(ss=0.10)),
                ("min_lane_hits=3", dict(mh=3)), ("min_lane_hits=4", dict(mh=4))]:
    r = q(cand_sql(CARA, **kw), limit=25000)
    top = set(x["uei"] for x in r[:50])
    keep = len(base_top & top); jac = keep / len(base_top | top) if top else 0
    print(f"  {lbl:<16} -> count={len(r):>6} ({len(r)-base_n:+d})   top50_kept={keep}/50   jaccard={jac:.2f}")

print("\n== C. IDF-wt vs cosine-to-seed-centroid (Carahsoft) ==")
seeds = [r["prime_uei"] for r in q(f"SELECT DISTINCT prime_uei FROM gtm_prime_sub_pairs_by_sub "
         f"WHERE sub_uei='{CARA}' AND edge_dollars_5y>0 AND prime_uei<>'{CARA}'")]
cent = {}
srows = []
for i in range(0, len(seeds), 2000):
    srows += q(f"SELECT uei,code_type,code,share_lifetime FROM gtm_prime_code_signature "
               f"WHERE uei IN ({inlist(seeds[i:i+2000])})", 50000)
for r in srows:
    cent[f"{r['code_type']}:{r['code']}"] = cent.get(f"{r['code_type']}:{r['code']}", 0.0) + float(r["share_lifetime"] or 0)
for k in cent: cent[k] /= len(seeds)
cnorm = math.sqrt(sum(v*v for v in cent.values()))
print("  seed centroid top codes:", [f"{k}={v:.3f}" for k, v in sorted(cent.items(), key=lambda x:-x[1])[:6]])
cu = [r["uei"] for r in base]; wt_rank = {r["uei"]: i for i, r in enumerate(base)}
cvec = {}
for i in range(0, len(cu), 2000):
    for r in q(f"SELECT uei,code_type,code,share_lifetime FROM gtm_prime_code_signature "
               f"WHERE uei IN ({inlist(cu[i:i+2000])})", 50000):
        cvec.setdefault(r["uei"], {})[f"{r['code_type']}:{r['code']}"] = float(r["share_lifetime"] or 0)
cos = {}
for u, v in cvec.items():
    dot = sum(w*cent.get(k, 0.0) for k, w in v.items()); nv = math.sqrt(sum(w*w for w in v.values()))
    cos[u] = dot/(nv*cnorm) if nv > 0 and cnorm > 0 else 0.0
cos_sorted = sorted(cos.items(), key=lambda x:-x[1])
print(f"  wt-top50 vs cosine-top50 overlap: {len(set(cu[:50]) & set(u for u,_ in cos_sorted[:50]))}/50")
nm = names([u for u, _ in cos_sorted[:12]]); prof = {r["uei"]: r for r in base}
print("  cosine top-12 (obl60 / top_naics / where it sits in wt rank):")
for u, c in cos_sorted[:12]:
    p = prof.get(u, {})
    print(f"     {str(nm.get(u))[:30]:<30} cos={c:.3f} obl60={float(p.get('prime_obl_60mo') or 0)/1e6:>6.0f}M "
          f"naics={p.get('top_naics')} wt_rank={wt_rank.get(u)}")

print("\n== D. FALSE NEGATIVE: where the billion-dollar primes (true peers) rank in wt ==")
big = sorted([(r["uei"], float(r["prime_obl_60mo"])) for r in base if float(r["prime_obl_60mo"]) >= 1e9],
             key=lambda x:-x[1])
bnm = names([b[0] for b in big[:12]])
print(f"  {len(big)} candidates have obl60>=$1B; all sit in the BOTTOM of the {len(base)}-long wt ranking:")
for u, o in big[:12]:
    print(f"     {str(bnm.get(u))[:30]:<30} obl60={o/1e6:>7.0f}M   wt_rank={wt_rank.get(u)}/{len(base)}")
