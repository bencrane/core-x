"""Bake — INDUSTRY_SHAPE sector rollup ('shape of the $2.4T' card). ONE BUTTON.

Active book (combo_award_active_state) by NAICS6 -> KLEMS industry -> 8 sectors.
Parity gate: sectors sum EXACTLY to the book. Emits the INDUSTRY_SHAPE rows block
to stdout AND (when GC_HQ_APP is set or the default checkout exists) rewrites the
block in gc-hq-new apps/platform-app/src/map/rehearsal.ts in place.

Run: doppler run -p core-x -c prd -- python3 scripts/demo_bakes/bake_industry_shape.py
"""
from __future__ import annotations
import os, re
from collections import defaultdict
from _shared import q, klems_mapping

MFG={"321","327","331","332","333","334","335","3361MV","3364OT","337","339",
     "311FT","313TT","315AL","322","323","324","325","326"}
GROUPS=[("Manufacturing (incl. aerospace & defense)",MFG,True),
 ("Professional, scientific & technical services",{"5411","5415","5412OP","55"},False),
 ("Construction",{"23"},True),
 ("Facilities, administrative & waste services",{"561","562"},False),
 ("Transportation & warehousing",{"481","482","483","484","485","486","487OS","493"},False),
 ("Information & software",{"513","PUB","512"},False),
 ("Health, education & social services",{"61","621","622HO","624"},False)]

def main():
    to_pc=klems_mapping()
    book=q("SELECT naics_code, sum(active_obligated) FROM combo_award_active_state GROUP BY 1")
    mapped=defaultdict(float); resid=0.0
    for n,v in book:
        pc=to_pc(n)
        if pc: mapped[pc]+=v or 0
        else: resid+=v or 0
    total=sum(mapped.values())+resid
    rows=[]; used=set()
    for name,pcs,acc in GROUPS:
        v=sum(w for pc,w in mapped.items() if pc in pcs); used|=pcs
        rows.append((name,v,acc))
    rows.append(("Everything else", sum(w for pc,w in mapped.items() if pc not in used)+resid, False))
    assert abs(sum(v for _,v,_ in rows)-total)<1, "parity gate failed"
    rows.sort(key=lambda r:-r[1])
    lines=[f'  {{ name: "{n}", usd_b: {v/1e9:.0f}, share_pct: {v/total*100:.1f}{", accent: true" if a else ""} }},'
           for n,v,a in rows]
    block="export const INDUSTRY_SHAPE = [\n"+"\n".join(lines)+"\n];"
    print(block)
    app=os.environ.get("GC_HQ_APP", os.path.expanduser("~/Desktop/gc-hq-new"))
    tgt=os.path.join(app,"apps/platform-app/src/map/rehearsal.ts")
    if os.path.exists(tgt):
        s=open(tgt).read()
        s2=re.sub(r"export const INDUSTRY_SHAPE = \[.*?\n\];", lambda m: block, s, flags=re.S)
        if s2!=s:
            open(tgt,"w").write(s2); print(f"-- rewrote {tgt}")
    print(f"-- book total ${total/1e9:.1f}B")

if __name__=="__main__": main()
