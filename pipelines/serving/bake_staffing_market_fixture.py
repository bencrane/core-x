"""Bake the staffing-market viewer fixture (rshq internal/staffing-market.json).

Durable home of the staffing agent's rebake (was scratchpad sizing/rebake_full.py
— taken over 2026-07-18). Reads: the staffing_market_inputs / research-mirror /
collections Lance datasets, four sidecar reference tables, the state-matrix
caches, and the operator's two outbound rosters. Writes the ~19MB fixture the
rshq cockpit StaffingMarket tab imports.

Caches + rosters live in CACHE_DIR (default ~/Desktop/hq/data-cache/staffing;
override STAFFING_CACHE_DIR). The matrices (matrix_active.pkl,
active_entities_by_state.pkl, sub_entities_by_state.pkl, subs_with_prime.pkl)
are point-in-time active-book state x combo aggregates built by the prior
session; regenerating them is a follow-on (they refresh the DEMAND side only —
firm-side rebakes reuse them unchanged).

Chain when new research payloads land (in order):
    1. pipelines/gtm/materialize_staffing_website_research.py   (mirror)
    2. pipelines/gtm/normalize_staffing_research.py             (inputs)
    3. pipelines/gtm/llm_map_staffing_role_tokens.py            (token map; agents)
    4. this script                                              (fixture)

Run: doppler run -p core-x -c prd -- python3 pipelines/serving/bake_staffing_market_fixture.py
"""
import os
CACHE_DIR = os.environ.get("STAFFING_CACHE_DIR",
                           os.path.expanduser("~/Desktop/hq/data-cache/staffing"))

import json, urllib.request, subprocess, pickle, csv, collections, statistics, lance
tok=subprocess.check_output(["doppler","secrets","get","QUERY_SIDECAR_TOKEN","-p","core-x","-c","prd","--plain"],text=True).strip()
def rows_of(sql):
    r=urllib.request.Request("https://query-sidecar-api.onrender.com/api/v1/sql",
        data=json.dumps({"sql":sql,"limit":50000}).encode(),
        headers={"Authorization":f"Bearer {tok}","content-type":"application/json"})
    b=json.load(urllib.request.urlopen(r,timeout=300)); assert not b["truncated"]; return b["rows"]
cs3=collections.defaultdict(set)
for n,p,s in rows_of("SELECT naics_code, psc_code, soc_code FROM naics_psc_labor_profile_categories WHERE rank <= 3 AND soc_code IS NOT NULL"):
    cs3[(n,p)].add(s)
share={r[0]:r[1] for r in rows_of("SELECT naics_code, loaded_labor_share FROM naics_labor_share")}
ws={(r[0],r[1]):r[2] for r in rows_of("SELECT naics_code, psc_code, work_summary FROM naics_psc_labor_profile")}
n2={r[0]:r[1] for r in rows_of("SELECT substr(naics_code,1,2) n2, min(naics_title) FROM v_naics_names GROUP BY 1")}
soc_titles=dict(rows_of("SELECT soc_code, min(soc_title) FROM naics_psc_labor_profile_categories WHERE soc_code IS NOT NULL GROUP BY 1"))
matrix=pickle.load(open(os.path.join(CACHE_DIR, "matrix_active.pkl"),"rb")); ALL=set(matrix.keys())
cell={}
for st,rr in matrix.items():
    for n,p,amt,recips,awards in rr:
        if amt and amt>0: cell.setdefault((n,p),{})[st]=(amt,recips,awards)
state_map=pickle.load(open(os.path.join(CACHE_DIR, "active_entities_by_state.pkl"),"rb"))
sub_map=pickle.load(open(os.path.join(CACHE_DIR, "sub_entities_by_state.pkl"),"rb"))
has_prime=pickle.load(open(os.path.join(CACHE_DIR, "subs_with_prime.pkl"),"rb"))
so={"aws_access_key_id":os.environ["R2_ACCESS_KEY_ID"],"aws_secret_access_key":os.environ["R2_SECRET_ACCESS_KEY"],"endpoint":os.environ["R2_ENDPOINT"],"region":"auto"}
inp=lance.dataset("s3://data-sink/active/staffing_market_inputs/", storage_options=so).to_table().to_pylist()
payloads={}
for r in lance.dataset("s3://data-sink/active/staffing_website_research/", storage_options=so).to_table(columns=["record_id","raw_payload"]).to_pylist():
    payloads[r["record_id"]]=json.loads(r["raw_payload"])
coll=collections.defaultdict(set)
for r in lance.dataset("s3://data-sink/active/staffing_market_collections/", storage_options=so).to_table().to_pylist():
    coll[r["slug"]].add((r["naics_code"],r["psc_code"]))
def esc(v): return "'"+v.replace("'","''")+"'"
ueis=sorted({r["uei"] for r in inp if r["uei"]})
names={}
for i in range(0,len(ueis),2000):
    for u,n in rows_of(f"SELECT uei, legal_business_name FROM gtm_sam_entities WHERE uei IN ({','.join(esc(u) for u in ueis[i:i+2000])})"):
        names[u]=n
meta_u={}
for r in csv.DictReader(open(os.path.join(CACHE_DIR, "staffing_agencies_sam_matched_1-500_2026-07-18.csv"))):
    for u in r["all_ueis"].split(";"): meta_u[u]=r
meta_d={r["domain"]:r for r in csv.DictReader(open(os.path.join(CACHE_DIR, "staffing_nonsam_aero_eng_it_2026-07-18.csv")))}
V2C={"healthcare_clinical":["medical-clinical-staffing"],"it_staffing":["federal-it-staffing"],
 "engineering":["engineering-technical-staffing"],"accounting":["finance-accounting-staffing"],
 "logistics_and_supply_chain":["logistics-supply-chain-staffing"],"trucking":["logistics-supply-chain-staffing"],
 "light_industrial_and_manufacturing":["light-industrial-trades-labor"],"skilled_trades":["light-industrial-trades-labor"],
 "construction":["light-industrial-trades-labor"],"facilities_services":["light-industrial-trades-labor"],
 "aerospace_and_defense":["engineering-technical-staffing","program-management-support-staffing-core"]}
out_rows=[]; details={}; seen=set()
for r in inp:
    key=r["uei"] or r.get("domain")
    if not key or key in seen: continue
    seen.add(key)
    firm_socs=set(r["soc_codes"]); combos=set(); grain="none"
    if firm_socs:
        grain="exact_soc"; need=2 if len(firm_socs)>=2 else 1
        for cmb,socs in cs3.items():
            if len(firm_socs & socs)>=need: combos.add(cmb)
    elif r["soc_major_groups"]:
        grain="major_group"; majs=set(r["soc_major_groups"])
        for cmb,socs in cs3.items():
            if any(x[:2] in majs for x in socs): combos.add(cmb)
    combosB=set()
    if firm_socs:
        for cmb,socs in cs3.items():
            if firm_socs & socs: combosB.add(cmb)
    else: combosB=combos
    sts=ALL if r["is_national"] or not r["states"] else set(r["states"])&ALL
    if r["uei"]:
        m=meta_u.get(r["uei"],{}); nm=names.get(r["uei"],""); band=m.get("employee_band",""); hq=m.get("sam_physical_state",""); dom=m.get("domain","")
        inds=m.get("industries_served") or ""
    else:
        m=meta_d.get(key,{}); nm=m.get("company_name",""); band=m.get("employee_band",""); hq=""; dom=key
        inds=m.get("all_industries_served") or ""
    verts=[v.strip() for v in inds.split(";") if v.strip()]
    slugs=sorted({s for v in verts for s in V2C.get(v,[])})
    mapped=set().union(*(coll[s] for s in slugs)) if slugs else set()
    cells=[]; fam=collections.Counter(); geo=collections.Counter(); lab=lab_in=award=0.0
    for cmb in combos:
        ls=share.get(cmb[0]) or 0
        for st,(amt,recips,awards) in cell.get(cmb,{}).items():
            if st in sts:
                v=amt*ls; lab+=v; award+=amt
                if cmb in mapped: lab_in+=v
                fam[cmb[0][:2]]+=v; geo[st]+=v
                cells.append((cmb[0],cmb[1],st,round(amt),round(v),recips))
    cells.sort(key=lambda x:-x[4])
    cv=0.0
    for cmb in mapped:
        ls=share.get(cmb[0]) or 0
        for st,(amt,_,_) in cell.get(cmb,{}).items():
            if st in sts: cv+=amt*ls
    conc=(lab_in/lab) if lab>0 else None
    p=payloads.get(r["record_id"],{})
    review="REVIEW" if (conc is not None and conc<0.2 and lab>1e8 and slugs) else ""
    n_pe=n_se=n_sp=None
    if sts and len(sts)<len(ALL):
        pe=set(); se=set()
        for st in sts:
            for n,pp,ruei in state_map.get(st,()):
                if (n,pp) in combosB: pe.add(ruei)
            for n,pp,suei in sub_map.get(st,()):
                if (n,pp) in combosB: se.add(suei)
        se-=pe
        n_pe=len(pe); n_se=len(se); n_sp=len(se & has_prime)
    details[key]={"say":{k:p.get(k) or "" for k in ("rolesPlaced","workCategories","geographiesServed","placementModel","clearanceAndFederalIntent","confidence")},
      "socs":[[s, soc_titles.get(s,"")] for s in r["soc_codes"]],
      "majors":r["soc_major_groups"],"states":r["states"],"national":r["is_national"],
      "placement":r["placement_models"],
      "fam":[[f, n2.get(f,""), round(v)] for f,v in fam.most_common(8)],
      "geo":[[g, round(v)] for g,v in geo.most_common(10)],
      "cells":[[c[0],c[1],c[2],c[3],c[4],c[5],(ws.get((c[0],c[1])) or "")[:160]] for c in cells[:15]],
      "lab":round(lab),"lab_in":round(lab_in)}
    top3=" | ".join(f"{c[0]}x{c[1]} {c[2]} labor${c[4]/1e9:.2f}B ({c[5]} active primes)" for c in cells[:3])
    out_rows.append([key,nm,band or "unknown",hq,dom,grain,len(combos),len(sts),len(cells),round(lab),round(award),
        inds,";".join(slugs),round(conc,3) if conc is not None else None,round(cv),review,top3,n_pe,n_se,n_sp])
out={"window":"ACTIVE book (current_end_date >= today, not terminated; committed value floored at 0/award)",
 "artifact":"query_sidecar_20260718T021418Z",
 "columns":["uei","name","band","hq_state","domain","grain","n_combos","n_states","n_cells","labor_dollars","award_dollars","industries","collections","in_vertical_share","collection_view_labor_dollars","divergence_flag","top_cells","n_entities","n_sub_entities","n_sub_with_prime"],
 "rows":out_rows,"details":details}
dest="/Users/benjamincrane/rare-structure-hq/apps/platform-app/src/internal/staffing-market.json"
json.dump(out,open(dest,"w"),separators=(",",":"))
sized=[r for r in out_rows if r[9]>0]
reg=[r for r in out_rows if r[17] is not None]
tot=sorted((r[17] or 0)+(r[18] or 0) for r in reg)
print(f"rebaked {len(out_rows)} firms ({os.path.getsize(dest)/1e6:.1f}MB) | sized {len(sized)}")
if reg: print(f"regional {len(reg)}: total universe median {statistics.median(tot):.0f} | p25 {tot[len(tot)//4]}")
print("REVIEW:",sum(1 for r in out_rows if r[15]))
