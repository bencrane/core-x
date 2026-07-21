"""02 — Independent reconstruction of market.total (FRONT 1 core).

For each sample UEI:
  * c_raw  = independent raw-SQL COUNT of the gate-OFF candidate universe
             (pre Python trim), self-contained on the target UEI.
  * engine = sub_dossier_v1.build()'s reported market.total.
  * my_total = re-run of the EXACT Python trim (family_key / is_jv_name from the
             engine module) over the full candidate rows, with every drop
             counted, so any hidden cap or double-drop is visible.
Certifies engine.market.total == my_total and that the only reducers are
JV-exclusion + corporate-family collapse + target-family exclusion.

NOTE: build() reads whatever snapshot is LATEST at run time; the raw-SQL side is
pinned. Reproduces exactly only while /healthz == the pinned artifact.
"""
import asyncio, os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, "/Users/benjamincrane/core-x")
from lib_sidecar import q, cand_sql, inlist, PIN_ARTIFACT, SAMPLE
from apps.catalyst_api.src.routers.sub_dossier_v1 import (
    build, family_key, is_jv_name, sql_market_hydrate, sql_jv_flags)

print(f"# pinned artifact (raw side): {PIN_ARTIFACT}\n")

def target_family(u):
    r = q(f"SELECT e.uei, e.legal_business_name, h.ultimate_parent_uei "
          f"FROM gtm_sam_entities e LEFT JOIN entity_hierarchy h USING(uei) WHERE e.uei='{u}'")[0]
    return family_key(u, r.get("ultimate_parent_uei"), r.get("legal_business_name"))

print(f"{'name':<26}{'c_raw':>7}{'engine':>8}{'my_total':>9}{'match':>7}  drops(tfam/jv/fam)")
for u, name in SAMPLE:
    rows = q(cand_sql(u, select="c.uei, c.wt"), limit=25000)   # ordered wt desc, uei
    ueis = [r["uei"] for r in rows]
    tfam = target_family(u)
    hyd = {}
    for i in range(0, len(ueis), 6000):
        for r in q(sql_market_hydrate(ueis[i:i+6000]), limit=6100, pin=False):
            hyd[r["uei"]] = r
    jv = set()
    for i in range(0, len(ueis), 6000):
        jv |= {r["uei"] for r in q(sql_jv_flags(ueis[i:i+6000]), limit=6100, pin=False)}
    seen = set(); total = 0; d_tf = d_jv = d_fam = 0
    for r in rows:
        mu = r["uei"]; h = hyd.get(mu, {}); nm = h.get("legal_business_name")
        fam = family_key(mu, h.get("ultimate_parent_uei"), nm)
        if fam == tfam: d_tf += 1; continue
        if (mu in jv) or is_jv_name(nm): d_jv += 1; continue
        if fam in seen: d_fam += 1; continue
        seen.add(fam); total += 1
    eng = asyncio.run(build({"uei": u}))["market"]["total"]
    ok = "OK" if eng == total else "**BAD**"
    print(f"{name[:26]:<26}{len(rows):>7}{eng:>8}{total:>9}{ok:>7}  {d_tf}/{d_jv}/{d_fam}")
