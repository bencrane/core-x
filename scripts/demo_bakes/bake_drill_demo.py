"""Bake — drillDemo.ts: drill-region stats + archetype work orders. ONE BUTTON.

Grain: the DRILLS dict below (state-tier = state list; region-tier = county FIPS
resolved live from demo_region_catalog). Extend DRILLS when new deals'
demo regions appear. PLUS every macro region from reference/macro_region_catalog
(state-tier via its composed states) — keyed by macro label, so region-scoped
flows (video posture) read the same DRILL_DEMO record shape at macro grain.

Methods (authority: docs/reference/DEMO_NARRATIVE_BAKES.md):
  firms:   >=$500K FY23-25 firms; median over awards >=$250K; growth FY25/FY23-1;
           first-time = first in-region action >= 2024-10-01
  active:  award_geo_active (not terminated, end >= today); flow-down via
           equipment_flowdown_factors per award NAICS (v1) — never flat
  outlook: share = region FY23-25 / national obligations; uplift = $785B x share x 0.40
  window:  active + uplift; equipment = total x region factor-weighted FY23-25 ratio
  orders:  3 fixed archetypes, real awards $25-250M (>=$5M fallback), names via
           bridge_sam_pdl -> entity_hierarchy? -> entity_profile_gold, active counts
           via contractor_award_summary

SUBSTRATE (2026-07-26 region-grain cycle): every region aggregate reads a precomputed
place-grain mart instead of scanning the 108M-row transaction fact once per region.
Measured over all 21 regions: 584.5 s -> 6.8 s of server time (86x). The marts are
keyed by place, so adding a region costs nothing — no rebuild, just a DRILLS entry.

METHOD, unified (encapsulation rule — recorded here and in DEMO_NARRATIVE_BAKES.md):
the firms block now attributes dollars by TRANSACTION place-of-performance for ALL 21
regions. Previously the 14 macros already did this (the award-level join 408'd on them)
while the 7 drills used AWARD-level PoP — so two regions in the same table were computed
by different rules and were not comparable. They are now one rule. Measured against the
prior output:
  - 14 macros: f500k, growth, firstFy25 all EXACT (+-0.000%); median +0.1..+0.4%
  - 7 drills:  f500k +0.5..+0.9%, median +0.0..+0.5%, firstFy25 +0.0..+2.3%,
               growth +0.2..+1.7 percentage points (4 of 7 displayed strings move 1-2 pts)
Everything outside the firms block — active $, active firms, flow $, region $, equipment
ratio, national share, and every archetype pick — is byte-identical for all 21 regions.
To put the drills back on award-level PoP, read pop_award_fy filtered on
award_pop_state/award_pop_county_fips instead of pop_entity_fy; note that mart carries
only awards >= $100K, which breaks first-time-winners badly (-53..-57%, measured).

Run: doppler run -p core-x -c prd -- python3 scripts/demo_bakes/bake_drill_demo.py
"""
from __future__ import annotations
import os, json, datetime as dt
import lance
from _shared import q, so, klems_mapping, flowdown_factors, fb

TODAY=dt.date.today().isoformat()
# Thresholds are the CARD's definitions, applied at query time — no mart is baked at
# these values. Change one here and re-run; no sidecar rebuild involved.
FY_LO, FY_HI       = 2023, 2025     # FY23-25 == action_date 2022-10-01..2025-09-30
FIRM_WIN_FLOOR     = 500_000        # "firms that won >=$X in-region"
MEDIAN_AWARD_FLOOR = 250_000        # median taken over awards >= this
ORDER_BAND         = (25_000_000, 250_000_000)  # archetype pick: the real-work band
ORDER_FALLBACK     = 5_000_000      # ...and the floor when that band is empty
FIRST_TIME_FROM    = "2024-10-01"   # first in-region action on/after = first-time winner
FY=f"fy BETWEEN {FY_LO} AND {FY_HI}"

DRILLS={"TX":("state",["TX"]),"CO":("state",["CO"]),"IN + MI":("state",["IN","MI"]),
 "Southern California":("region","southern california"),"Northeast Ohio":("region","northeast ohio"),
 "Western TX":("region","western TX"),"Central California":("region","central california")}
ARCH={
 "roads":   [("237310","Y1LB"),("237990","Y1KA"),("237990","Y1KB"),("237990","Y1PZ"),("237310","Z2LB")],
 "newbuild":[("236220","Y1JZ"),("236220","Y1PZ"),("236220","Y1AA"),("236220","Y1AZ"),("236220","Y1DA")],
 "repair":  [("236220","Z2JZ"),("236220","Z2AA"),("236220","Z1JZ"),("236220","Z2AZ")],
}
FACE={"roads":"Highway, road and bridge construction — earthmoving, grading, structures, paving.",
 "newbuild":"New construction of federal facilities — sitework, foundations, vertical build-out.",
 "repair":"Repair and modernization of existing federal facilities — structural, mechanical, sitework."}
OBBA=785e9; RAMP=0.40

def region_counties(name):
    # demo_region_catalog rides the sidecar since 2026-07-26 — no Lance round-trip.
    return [r[0] for r in q(f"SELECT county_fips FROM demo_region_catalog WHERE demo_region = '{name}'")]

def where_clause(kind, spec):
    if kind=="state":
        return "pop_state IN ("+",".join(f"'{s}'" for s in spec)+")"
    return "pop_county_fips IN ("+",".join(f"'{c}'" for c in region_counties(spec))+")"

def tc(s): return " ".join(w.capitalize() for w in (s or "").split())

def macro_states():
    t=lance.dataset("s3://data-sink/active/reference/macro_region_catalog", storage_options=so())\
        .to_table(columns=["macro_region","state_usps"]).to_pydict()
    out={}
    for m,s in zip(t["macro_region"],t["state_usps"]):
        out.setdefault(m,[]).append(s)
    return out

def main():
    to_pc=klems_mapping(); factors=flowdown_factors()
    runs=dict(DRILLS)
    for m,sts in macro_states().items():
        runs[m]=("state",sts)
    def factor(n6):
        return factors.get(to_pc(n6), 0.039)
    national=q(f"SELECT sum(obligation_sum) FROM pop_combo_fy WHERE {FY}")[0][0]
    demo={}; sel_ueis=set(); picks_by={}
    states_of={k:(v[1] if v[0]=="state" else {"southern california":["CA"],"northeast ohio":["OH"],
               "western TX":["TX"],"central california":["CA"]}[v[1]]) for k,v in runs.items()}
    for label,(kind,spec) in runs.items():
        W=where_clause(kind,spec)
        # firms — one pass over the firm x place x FY atom. Firm counts, medians and
        # growth are NOT additive across places, so the region is rolled up per-uei
        # FIRST and the thresholds applied to the rolled-up firm.
        f_=q(f"""WITH g AS (
  SELECT uei,
         sum(obligation_sum) FILTER (WHERE {FY})       AS won,
         sum(obligation_sum) FILTER (WHERE fy={FY_LO}) AS won_lo,
         sum(obligation_sum) FILTER (WHERE fy={FY_HI}) AS won_hi,
         min(first_action_date)                        AS first_seen
  FROM pop_entity_fy WHERE {W} GROUP BY uei)
SELECT
 (SELECT count(*) FROM g WHERE won >= {FIRM_WIN_FLOOR}),
 (SELECT median(t) FROM (SELECT sum(obligation_sum) t FROM pop_award_fy
    WHERE {W} AND {FY} GROUP BY award_key HAVING sum(obligation_sum) >= {MEDIAN_AWARD_FLOOR})),
 (SELECT sum(won_hi)/nullif(sum(won_lo),0)-1 FROM g),
 (SELECT count(*) FROM g WHERE first_seen >= DATE '{FIRST_TIME_FROM}')""")[0]
        # active book — award_geo_active is the live book only (263k rows), already
        # 1 row/award, so the per-award collapse the old path needed is gone. Same
        # award-level PoP the old path used: award_geo_active inherits award_geo_state's.
        act=q(f"""SELECT naics_code, sum(obligated), count(DISTINCT uei) FROM award_geo_active
  WHERE {W} AND current_end_date >= DATE '{TODAY}' GROUP BY 1""")
        a_obl=sum(r[1] or 0 for r in act)
        a_firms=q(f"""SELECT count(DISTINCT uei) FROM award_geo_active
  WHERE {W} AND current_end_date >= DATE '{TODAY}'""")[0][0]
        a_flow=sum((r[1] or 0)*factor(r[0]) for r in act)
        w_=q(f"SELECT naics_code, sum(obligation_sum) FROM pop_combo_fy WHERE {W} AND {FY} GROUP BY 1")
        obl=sum(v or 0 for _,v in w_)
        ratio=(sum((v or 0)*factor(n) for n,v in w_)/obl) if obl else 0.039
        share=obl/national; uplift=OBBA*share*RAMP; z=a_obl+uplift
        picks={}
        for arch,pairs in ARCH.items():
            inp=",".join(f"('{n}','{p}')" for n,p in pairs)
            # award_key is a real tiebreaker, not decoration: two Central California
            # newbuild awards tie at exactly $19.00M, and without it the pick flips
            # between runs. Deterministic ordering is part of the method.
            for having in (f"BETWEEN {ORDER_BAND[0]} AND {ORDER_BAND[1]}", f">= {ORDER_FALLBACK}"):
                rows=q(f"""SELECT award_key, sum(obligation_sum) tot, any_value(uei), any_value(pop_city_name),
  any_value(pop_state), any_value(recipient_state)
FROM pop_award_fy WHERE (naics_code, psc_code) IN ({inp}) AND {W} AND {FY}
GROUP BY award_key HAVING sum(obligation_sum) {having} ORDER BY tot DESC, award_key""", limit=50)
                if rows: break
            if rows: picks[arch]=rows[0]; sel_ueis.add(rows[0][2])
        picks_by[label]=picks
        demo[label]={"firms":{"f500k":f"{int(f_[0] or 0):,}","median":(f"${(f_[1] or 0)/1e6:.1f}M" if (f_[1] or 0)>=1e6 else f"${(f_[1] or 0)/1e3:.0f}K"),
            "growth":("+" if (f_[2] or 0)>=0 else "−")+f"{abs(f_[2] or 0)*100:.0f}%","firstFy25":f"{int(f_[3] or 0):,}"},
          "active":{"obl":fb(a_obl),"firms":f"{int(a_firms or 0):,}","flow":"~"+fb(a_flow)},
          "outlook":{"sharePct":f"{share*100:.1f}%","uplift":"+"+fb(uplift)},
          "window":{"active":fb(a_obl),"uplift":"+"+fb(uplift),"total":"~"+fb(z),"equip":"~"+fb(z*ratio)}}
        print(label,"done",flush=True)
    inl=",".join(f"'{u}'" for u in sel_ueis)
    names={r[0]:r[1] for r in q(f"SELECT uei, any_value(legal_business_name) FROM bridge_sam_pdl WHERE uei IN ({inl}) GROUP BY uei")}
    epg=lance.dataset("s3://data-sink/active/entity_profile_gold", storage_options=so())\
        .to_table(columns=["uei","legal_business_name"], filter=f"uei IN ({inl})").to_pydict()
    for u,n in zip(epg["uei"],epg["legal_business_name"]):
        if n: names.setdefault(u,n)
    cas=lance.dataset("s3://data-sink/active/contractor_award_summary", storage_options=so())\
        .to_table(columns=["recipient_uei","prime_active_awards"], filter=f"recipient_uei IN ({inl})").to_pydict()
    active_ct=dict(zip(cas["recipient_uei"],cas["prime_active_awards"]))
    for label,picks in picks_by.items():
        st=set(states_of[label]); orders=[]
        for arch in ("roads","newbuild","repair"):
            r=picks.get(arch)
            if not r: continue
            uei=r[2]; n=int(active_ct.get(uei) or 0)
            orders.append({"summary":FACE[arch],"value":fb(r[1]),
                "place":(f"Performing at {tc(r[3])}, {r[4]}" if r[3] else f"Performing in {r[4]}"),
                "firm":{"name":tc(names.get(uei,"")) or "Undisclosed","hq":f"HQ: {r[5]}" if r[5] else "HQ: —",
                        "note":f"{n} active federal awards · {'local' if r[5] in st else 'non-local'}"}})
        orders.sort(key=lambda o: float(o["value"].replace("$","").replace("B","000").replace("M","")))
        demo[label]["orders"]=orders
    ts=("/** GENERATED "+TODAY+" — drill-region + macro-region demo stats + archetype work orders (bake_drill_demo.py).\n"
        " * Methods: docs/reference/DEMO_NARRATIVE_BAKES.md. flow = per-industry equipment_flowdown_factors v1. Do not hand-edit. */\n"
        "export type DrillOrder = { summary: string; value: string; place: string; firm: { name: string; hq: string; note: string } };\n"
        "export type DrillDemo = { firms: { f500k: string; median: string; growth: string; firstFy25: string }; active: { obl: string; firms: string; flow: string }; outlook: { sharePct: string; uplift: string }; window: { active: string; uplift: string; total: string; equip: string }; orders: DrillOrder[] };\n"
        "export const DRILL_DEMO: Record<string, DrillDemo> = "+json.dumps(demo,separators=(",",":"))+";\n")
    app=os.environ.get("GC_HQ_APP", os.path.expanduser("~/Desktop/gc-hq-new"))
    tgt=os.path.join(app,"apps/platform-app/src/map/drillDemo.ts")
    open(tgt,"w").write(ts)
    print("wrote",tgt)

if __name__=="__main__": main()
