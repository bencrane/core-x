"""Bake — INDUSTRY_SHAPE sector rollups. ONE BUTTON.

Two blocks, same NAICS6 -> KLEMS -> 8-sector grouping, each parity-gated:
  INDUSTRY_SHAPE      — active book (combo_award_active_state), the call card.
  INDUSTRY_SHAPE_5YR  — FY21-25 obligations (txn_events_combo), the video's
                        '$3.65T -> its shape' continuation pie.
Emits both blocks to stdout AND (when GC_HQ_APP is set or the default checkout
exists) rewrites each block in gc-hq-new apps/platform-app/src/map/rehearsal.ts.

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

def shape_block(const_name, book, to_pc):
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
    assert abs(sum(v for _,v,_ in rows)-total)<1, f"parity gate failed: {const_name}"
    rows.sort(key=lambda r:-r[1])
    lines=[f'  {{ name: "{n}", usd_b: {v/1e9:.0f}, share_pct: {v/total*100:.1f}{", accent: true" if a else ""} }},'
           for n,v,a in rows]
    return "export const "+const_name+" = [\n"+"\n".join(lines)+"\n];", total

def main():
    to_pc=klems_mapping()
    blocks=[]
    b,t=shape_block("INDUSTRY_SHAPE",
        q("SELECT naics_code, sum(active_obligated) FROM combo_award_active_state GROUP BY 1"), to_pc)
    blocks.append(("INDUSTRY_SHAPE",b)); print(b); print(f"-- active book total ${t/1e9:.1f}B")
    b5,t5=shape_block("INDUSTRY_SHAPE_5YR",
        q("SELECT naics_code, sum(obligation) FROM txn_events_combo WHERE action_date BETWEEN DATE '2020-10-01' AND DATE '2025-09-30' GROUP BY 1"), to_pc)
    blocks.append(("INDUSTRY_SHAPE_5YR",b5)); print(b5); print(f"-- FY21-25 total ${t5/1e9:.1f}B")
    app=os.environ.get("GC_HQ_APP", os.path.expanduser("~/Desktop/gc-hq-new"))
    tgt=os.path.join(app,"apps/platform-app/src/map/rehearsal.ts")
    if os.path.exists(tgt):
        s=open(tgt).read()
        for name,block in blocks:
            s=re.sub(r"export const "+name+r" = \[.*?\n\];", lambda m: block, s, flags=re.S)
        open(tgt,"w").write(s); print(f"-- rewrote {tgt}")

if __name__=="__main__": main()
