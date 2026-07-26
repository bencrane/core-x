"""Bake — drillDemo.ts: drill-region stats + archetype work orders. ONE BUTTON.

Grain: the DRILLS dict below (state-tier = state list; region-tier = county FIPS
resolved live from reference/demo_region_catalog). Extend DRILLS when new deals'
demo regions appear. PLUS every macro region from reference/macro_region_catalog
(state-tier via its composed states) — keyed by macro label, so region-scoped
flows (video posture) read the same DRILL_DEMO record shape at macro grain.

Methods (authority: docs/reference/DEMO_NARRATIVE_BAKES.md):
  firms:   >=$500K FY23-25 firms; median over awards >=$250K; growth FY25/FY23-1;
           first-time = first in-region action >= 2024-10-01
  active:  award_geo_state active (not terminated, end >= today); flow-down via
           reference/equipment_flowdown_factors per award NAICS (v1) — never flat
  outlook: share = region FY23-25 / national obligations; uplift = $785B x share x 0.40
  window:  active + uplift; equipment = total x region factor-weighted FY23-25 ratio
  orders:  3 fixed archetypes, real awards $25-250M (>=$5M fallback), names via
           bridge_sam_pdl -> entity_hierarchy? -> entity_profile_gold, active counts
           via contractor_award_summary

Run: doppler run -p core-x -c prd -- python3 scripts/demo_bakes/bake_drill_demo.py
"""
from __future__ import annotations
import os, json, datetime as dt
import lance
from _shared import q, so, klems_mapping, flowdown_factors, fb

TODAY=dt.date.today().isoformat()
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
    t=lance.dataset("s3://data-sink/active/reference/demo_region_catalog", storage_options=so())\
        .to_table(columns=["demo_region","county_fips"]).to_pydict()
    return [f for r,f in zip(t["demo_region"],t["county_fips"]) if r==name]

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
    national=q("SELECT sum(obligation) FROM gtm_txn_events_slim WHERE action_date BETWEEN DATE '2022-10-01' AND DATE '2025-09-30'")[0][0]
    demo={}; sel_ueis=set(); picks_by={}
    states_of={k:(v[1] if v[0]=="state" else {"southern california":["CA"],"northeast ohio":["OH"],
               "western TX":["TX"],"central california":["CA"]}[v[1]]) for k,v in runs.items()}
    def qr(sql, **kw):
        import urllib.error, time
        for att in range(3):
            try: return q(sql, **kw)
            except urllib.error.HTTPError as e:
                if e.code!=408 or att==2: raise
                time.sleep(5)
    for label,(kind,spec) in runs.items():
        W=where_clause(kind,spec)
        # Drills keep the award-level join (numbers must regenerate identically);
        # macros filter txn_events_combo directly (transaction-level PoP — the
        # join form 408s on the big multi-state macros).
        if label in DRILLS:
            base=f"""WITH aw AS (SELECT award_key FROM award_geo_state WHERE {W} GROUP BY award_key),
tx AS (SELECT t.uei, t.award_key, t.obligation, t.action_date
  FROM gtm_txn_events_slim t JOIN aw ON t.award_key=aw.award_key)"""
        else:
            base=f"""WITH tx AS (SELECT uei, award_key, obligation, action_date
  FROM txn_events_combo WHERE {W})"""
        f_=qr(base+"""
SELECT
 (SELECT count(*) FROM (SELECT uei FROM tx WHERE action_date BETWEEN DATE '2022-10-01' AND DATE '2025-09-30' GROUP BY uei HAVING sum(obligation)>=500000)),
 (SELECT median(t) FROM (SELECT sum(obligation) t FROM tx WHERE action_date BETWEEN DATE '2022-10-01' AND DATE '2025-09-30' GROUP BY award_key HAVING sum(obligation)>=250000)),
 (SELECT sum(CASE WHEN action_date BETWEEN DATE '2024-10-01' AND DATE '2025-09-30' THEN obligation END)/nullif(sum(CASE WHEN action_date BETWEEN DATE '2022-10-01' AND DATE '2023-09-30' THEN obligation END),0)-1 FROM tx),
 (SELECT count(*) FROM (SELECT uei, min(action_date) m FROM tx GROUP BY uei HAVING m >= DATE '2024-10-01'))""")[0]
        act=qr(f"""WITH act AS (SELECT award_key, any_value(uei) uei, any_value(naics_code) n, any_value(obligated) ob
  FROM award_geo_state WHERE {W} AND is_terminated=false AND current_end_date >= DATE '{TODAY}' GROUP BY award_key)
SELECT n, sum(ob), count(DISTINCT uei) FROM act GROUP BY n""")
        a_obl=sum(r[1] or 0 for r in act)
        a_firms=len({None})  # placeholder replaced below
        a_firms=qr(f"""SELECT count(DISTINCT uei) FROM award_geo_state
  WHERE {W} AND is_terminated=false AND current_end_date >= DATE '{TODAY}'""")[0][0]
        a_flow=sum((r[1] or 0)*factor(r[0]) for r in act)
        w_=qr(f"""SELECT naics_code, sum(obligation) FROM txn_events_combo
  WHERE {W} AND action_date BETWEEN DATE '2022-10-01' AND DATE '2025-09-30' GROUP BY 1""")
        obl=sum(v or 0 for _,v in w_)
        ratio=(sum((v or 0)*factor(n) for n,v in w_)/obl) if obl else 0.039
        share=obl/national; uplift=OBBA*share*RAMP; z=a_obl+uplift
        picks={}
        for arch,pairs in ARCH.items():
            inp=",".join(f"('{n}','{p}')" for n,p in pairs)
            rows=q(f"""SELECT award_key, sum(obligation) tot, any_value(uei), any_value(pop_city_name),
  any_value(pop_state), any_value(recipient_state)
FROM txn_events_combo WHERE (naics_code, psc_code) IN ({inp}) AND {W}
  AND action_date BETWEEN DATE '2022-10-01' AND DATE '2025-09-30'
GROUP BY award_key HAVING sum(obligation) BETWEEN 25000000 AND 250000000 ORDER BY tot DESC""", limit=50)
            if not rows:
                rows=q(f"""SELECT award_key, sum(obligation) tot, any_value(uei), any_value(pop_city_name),
  any_value(pop_state), any_value(recipient_state)
FROM txn_events_combo WHERE (naics_code, psc_code) IN ({inp}) AND {W}
  AND action_date BETWEEN DATE '2022-10-01' AND DATE '2025-09-30'
GROUP BY award_key HAVING sum(obligation) >= 5000000 ORDER BY tot DESC""", limit=50)
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
