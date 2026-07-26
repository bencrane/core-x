"""Bake — equipment-company universe -> derived counties -> demo regions -> demoRegions.ts.
ONE BUTTON. Rebuilds, in order:
  s3://data-sink/active/equipment_company_region_counties/   (domain x county, basis)
  s3://data-sink/active/equipment_company_demo_region/       (macro + demo + states)
  gc-hq-new apps/platform-app/src/map/demoRegions.ts         (per-domain sketch geometry)

Chain + rules: docs/reference/DEMO_NARRATIVE_BAKES.md (universe, size filter, RAW-strings-only
service areas, DC rule, adjacency generosity, HQ fallback, >=80% assignment, macro containment).

Run: doppler run -p core-x -c prd -- python3 scripts/demo_bakes/bake_company_regions.py
"""
from __future__ import annotations
import os, re, json, math, datetime as dt
from collections import defaultdict, Counter
import lance, pyarrow as pa
from _shared import so, q, norm_domain as norm

STATES={"alabama":"AL","alaska":"AK","arizona":"AZ","arkansas":"AR","california":"CA","colorado":"CO","connecticut":"CT","delaware":"DE","florida":"FL","georgia":"GA","hawaii":"HI","idaho":"ID","illinois":"IL","indiana":"IN","iowa":"IA","kansas":"KS","kentucky":"KY","louisiana":"LA","maine":"ME","maryland":"MD","massachusetts":"MA","michigan":"MI","minnesota":"MN","mississippi":"MS","missouri":"MO","montana":"MT","nebraska":"NE","nevada":"NV","new hampshire":"NH","new jersey":"NJ","new mexico":"NM","new york":"NY","north carolina":"NC","north dakota":"ND","ohio":"OH","oklahoma":"OK","oregon":"OR","pennsylvania":"PA","rhode island":"RI","south carolina":"SC","south dakota":"SD","tennessee":"TN","texas":"TX","utah":"UT","vermont":"VT","virginia":"VA","washington":"WA","west virginia":"WV","wisconsin":"WI","wyoming":"WY"}
CODES=set(STATES.values())|{"DC"}
ST_RE=re.compile("|".join(re.escape(s) for s in sorted(STATES,key=len,reverse=True)))
DIR_RE=re.compile(r'(southern|northern|eastern|western|central|southeast|southwest|northeast|northwest|upstate|downstate|panhandle)\s')
CODE_RE=re.compile(r'\b([A-Z]{2})\b')
MACRO={"New England":["ME","NH","VT","MA","RI","CT"],"Mid-Atlantic":["NY","NJ","PA","DE","MD"],
 "Capital Region (DMV)":["DC","MD","VA"],"Southeast":["VA","NC","SC","GA","FL","AL","TN","KY"],
 "Gulf Coast":["TX","LA","MS","AL","FL"],"Great Lakes / Midwest":["OH","MI","IN","IL","WI","MN"],
 "Great Plains":["ND","SD","NE","KS","OK","IA","MO"],"Texas & Southern Plains":["TX","OK","NM"],
 "Mountain West":["CO","UT","WY","MT","ID"],"Southwest":["AZ","NM","NV"],
 "Pacific Northwest":["WA","OR","ID"],"West Coast":["CA","OR","WA"],"Alaska":["AK"],"Hawaii":["HI"]}

def L(name, **kw):
    return lance.dataset(f"s3://data-sink/active/{name}", storage_options=so()).to_table(**kw).to_pydict()

def entry_states(t):
    out={STATES[m] for m in ST_RE.findall(t.lower())}
    out|={c for c in CODE_RE.findall(t) if c in CODES}
    return out

def haversine(a,b):
    la1,lo1=a; la2,lo2=b; p=math.pi/180
    x=math.sin((la2-la1)*p/2)**2+math.cos(la1*p)*math.cos(la2*p)*math.sin((lo2-lo1)*p/2)**2
    return 2*3959*math.asin(math.sqrt(x))

def main():
    now=dt.datetime.now(dt.timezone.utc)
    # ---- universe: domain-plane yes + yard-plane yes (domains from research payload URLs) ----
    ep=L("equipment_provider", columns=["domain_norm","is_equipment_provider"])
    main_doms={norm(d) for d,y in zip(ep["domain_norm"],ep["is_equipment_provider"]) if y and d}
    prof=L("equipment_yard_profile", columns=["uei","is_equipment_provider"])
    yes_ueis={u for u,y in zip(prof["uei"],prof["is_equipment_provider"]) if y}
    res=L("equipment_yard_website_research", columns=["uei","raw_payload"])
    yard=defaultdict(set)
    for u,p in zip(res["uei"],res["raw_payload"]):
        if u in yes_ueis:
            yard[u] |= {norm(x) for x in re.findall(r'https?://[^\s"\'\\]+', p or "")}
    gov={u:sorted(d)[0] for u,d in yard.items() if d and not (d & main_doms)}
    universe=sorted(main_doms | set(gov.values()))
    # ---- size filter: <500 or unknown (clay_enrich > blitz > pdl > clay_find) ----
    ce=L("clay_enrich_companies", columns=["raw_payload"])
    clayen={}
    for p in ce["raw_payload"]:
        try:
            o=json.loads(p); d=norm(o.get("domain") or o.get("website") or "")
            if d and o.get("size"): clayen[d]=o["size"]
        except Exception: pass
    bl=L("firmographics_blitz", columns=["domain_norm","employee_size_band"])
    blitz={norm(d):b for d,b in zip(bl["domain_norm"],bl["employee_size_band"]) if d and b}
    cf=L("clay_find_companies", columns=["domain_norm","size"])
    clayf={norm(d):s for d,s in zip(cf["domain_norm"],cf["size"]) if d and s}
    inl=",".join(f"'{d}'" for d in universe)
    pdl={r[0]:r[1] for r in q(f"SELECT normalized_domain, any_value(employee_size_range) FROM pdl_normalized_companies WHERE normalized_domain IN ({inl}) AND employee_size_range IS NOT NULL GROUP BY 1")}
    def is_big(raw):
        s=str(raw).lower().replace("employees","").replace(",","").strip()
        m=re.match(r'(\d+)\s*[-–+]',s) or re.match(r'(\d+)$',s)
        return bool(m) and int(m.group(1))>=501
    target={d for d in universe if not ((clayen.get(d) or blitz.get(d) or pdl.get(d) or clayf.get(d)) and is_big(clayen.get(d) or blitz.get(d) or pdl.get(d) or clayf.get(d)))}
    print("market set:",len(target))
    # ---- raw service areas (both planes) ----
    dom_areas=defaultdict(list)
    for name,key in (("equipment_provider_service_areas","domain_norm"),):
        t=L(name, columns=[key,"raw_payload"])
        for d,p in zip(t[key],t["raw_payload"]):
            try:
                for a in (json.loads(p).get("serviceAreas") or []):
                    s=a.get("parsed") if isinstance(a,dict) else a
                    if s: dom_areas[norm(d)].append(s)
            except Exception: pass
    ys=L("equipment_yard_service_areas", columns=["uei","raw_payload"])
    for u,p in zip(ys["uei"],ys["raw_payload"]):
        d=gov.get(u)
        if not d: continue
        try:
            for a in (json.loads(p).get("serviceAreas") or []):
                s=a.get("parsed") if isinstance(a,dict) else a
                if s: dom_areas[d].append(s)
        except Exception: pass
    # ---- geo references ----
    gaz=L("census_county_gazetteer_2023")
    county_state=dict(zip(gaz["county_fips"],gaz["state_usps"]))
    county_latlon={f:(la,lo) for f,la,lo in zip(gaz["county_fips"],gaz["lat"],gaz["lon"])}
    SUF=re.compile(r'\s+(county|parish|census area|city and borough|borough|municipality)$')
    cname=defaultdict(dict)
    for f,s,n in zip(gaz["county_fips"],gaz["state_usps"],gaz["county_name"]):
        cname[s][SUF.sub('',n.lower())]=f
    state_counties=defaultdict(set)
    for f,s in county_state.items(): state_counties[s].add(f)
    rel=L("census_zcta_county_rel_2020", columns=["zcta5","county_fips"])
    zip2c=defaultdict(set)
    for z,f in zip(rel["zcta5"],rel["county_fips"]): zip2c[z].add(f)
    gx=L("geocode_xwalk", columns=["city","state","zip5"])
    city2zips=defaultdict(set)
    for c,s,z in zip(gx["city"],gx["state"],gx["zip5"]):
        if c and s and z: city2zips[(c.strip().lower(),s.upper())].add(z[:5])
    adj=defaultdict(set)
    for a,b in q("SELECT county_fips, neighbor_fips FROM census_county_adjacency"): adj[a].add(b)
    srm=L("reference/state_region_county_map", columns=["state_region","county_fips"])
    region_counties=defaultdict(set)
    for r,f in zip(srm["state_region"],srm["county_fips"]): region_counties[r].add(f)
    # ---- per-company county derivation ----
    CLEAN=re.compile(r'\b(greater|metro(politan)?( area)?|area(s)?|region|surrounding( areas| cities)?|and|the|hq|headquarters)\b')
    results={}
    for d in sorted(target):
        strs=dom_areas.get(d)
        if not strs: results[d]=(set(),"no_data"); continue
        counties=set(); bases=set()
        ctx=set()
        for s in strs: ctx|=entry_states(s)
        dc_context=bool({"MD","VA","DC"} & ctx)
        for s in strs:
            t=s.strip(); tl=t.lower()
            if "nationwide" in tl or ("national" in tl and "service" in tl): bases.add("nationwide"); continue
            sts=entry_states(t)
            if "washington" in tl and dc_context and "WA" in sts and not ({"OR","ID"} & ctx):
                sts.discard("WA"); sts.add("DC")
            mrad=re.search(r'(\d+)\s*(?:mile|mi\b)', tl)
            zips=re.findall(r'\b(\d{5})\b', tl)
            addr=bool(re.search(r'\d+\s+\w+', tl)) and bool(zips)
            statescale = sts and not addr and (len(sts)>1 or DIR_RE.search(tl) or re.search(r'(all of|statewide|throughout|entire)',tl) or not re.search(r'[a-z]+,\s*[a-z]', tl))
            if statescale and not mrad:
                m=DIR_RE.search(tl)
                if m and len(sts)==1:
                    st=list(sts)[0]
                    reg=f"{m.group(1)} {[k for k,v in STATES.items() if v==st][0]}" if st!="DC" else None
                    got=region_counties.get(reg)
                    if got: counties|=got; bases.add("state_region"); continue
                for st in sts:
                    if st=="DC": counties.add("11001")
                    else: counties|=state_counties[st]
                bases.add("state" if len(sts)==1 else "multi_state"); continue
            zc=set()
            for z in zips: zc|=zip2c.get(z,set())
            for st in (sts or ctx):
                if st=="DC": continue
                for m in re.finditer(r'([a-z .\-]+?)\s+count(?:y|ies)', tl):
                    f=cname[st].get(m.group(1).strip())
                    if f: zc.add(f); bases.add("county_named")
            cityc=set()
            for pth in [x.strip() for x in re.split(r'[;,/]|\band\b', tl) if x.strip()]:
                p2=CLEAN.sub('',pth).strip(' .-')
                if not p2 or len(p2)<3 or re.match(r'^\d',p2): continue
                if p2 in STATES or p2.upper() in CODES: continue
                for st in (sts or ctx):
                    if st=="DC": cityc.add("11001"); continue
                    for z in city2zips.get((p2,st),set()): cityc|=zip2c.get(z,set())
            got=zc|cityc
            if mrad and got:
                nmi=int(mrad.group(1))
                anchors=[county_latlon[f] for f in got if f in county_latlon]
                for f,ll in county_latlon.items():
                    if any(haversine(a,ll)<=nmi for a in anchors): got.add(f)
                bases.add("radius")
            if got:
                counties|=got; bases.add("city" if cityc else "zip")
        if counties and len(counties)<=2 and not ({"state","multi_state","state_region"} & bases):
            ring=set()
            for f in counties: ring|=adj.get(f,set())
            counties|=ring; bases.add("adjacency_ring")
        results[d]=(counties,"+".join(sorted(bases)) if bases else "unresolved")
    # HQ fallback for unresolved
    hq={}
    for p in ce["raw_payload"]:
        try:
            o=json.loads(p); d=norm(o.get("domain") or o.get("website") or "")
            for Lc in (o.get("locations") or []):
                il=(Lc or {}).get("inferred_location") or {}
                if il.get("postal_code"): hq.setdefault(d+"|zip", str(il["postal_code"])[:5])
        except Exception: pass
    fbz=L("firmographics_blitz", columns=["domain_norm","hq_city","hq_state"])
    for d,c,s in zip(fbz["domain_norm"],fbz["hq_city"],fbz["hq_state"]):
        if d and c and s: hq.setdefault(norm(d),(c,s))
    cfh=L("clay_find_companies", columns=["domain_norm","hq_city","hq_state"])
    for d,c,s in zip(cfh["domain_norm"],cfh["hq_city"],cfh["hq_state"]):
        if d and c and s: hq.setdefault(norm(d),(c,s))
    def to_code(s):
        s=str(s).strip(); return s.upper() if len(s)==2 else STATES.get(s.lower())
    for d,(cs,b) in list(results.items()):
        if cs or b=="no_data" or "nationwide" in b: continue
        got=set()
        z=hq.get(d+"|zip")
        if z: got|=zip2c.get(z,set())
        if not got and d in hq and isinstance(hq[d],tuple):
            c,s=hq[d]; sc=to_code(s)
            if sc:
                for zz in city2zips.get((c.lower(),sc),set()): got|=zip2c.get(zz,set())
        if got:
            ring=set()
            for f in got: ring|=adj.get(f,set())
            results[d]=(got|ring,"hq_fallback+adjacency_ring")
    # ---- land counties mart ----
    doms=[];fips=[];basis=[]
    for d,(cs,b) in sorted(results.items()):
        for f in sorted(cs): doms.append(d); fips.append(f); basis.append(b)
    t=pa.table({"domain_norm":doms,"county_fips":fips,"basis":basis,
        "materialized_at":pa.array([now]*len(doms),pa.timestamp("us",tz="UTC"))})
    ds=lance.write_dataset(t,"s3://data-sink/active/equipment_company_region_counties/",mode="overwrite",storage_options=so())
    ds.create_scalar_index("domain_norm","BTREE"); ds.create_scalar_index("county_fips","BTREE")
    print("counties mart:",ds.count_rows())
    # ---- demo/macro assignment ----
    cat=L("reference/demo_region_catalog")
    assign={f:(s,r) for s,r,f in zip(cat["state_usps"],cat["demo_region"],cat["county_fips"])}
    state2macros=defaultdict(list)
    for m,sts in MACRO.items():
        for s in sts: state2macros[s].append(m)
    def best_macro(states):
        states=set(states)
        cands=sorted((len(MACRO[m]),m) for m in MACRO if states<=set(MACRO[m]))
        if cands: return cands[0][1]
        cover=Counter({m:len(states&set(MACRO[m])) for m in MACRO})
        top=cover.most_common(1)[0]
        return None if top[1]==0 else sorted([m for m,n in cover.items() if n==top[1]], key=lambda m: len(MACRO[m]))[0]
    rows=[]
    for d,(cs,b) in sorted(results.items()):
        known=[f for f in cs if f in assign]
        states=sorted({assign[f][0] for f in known})
        if not known:
            dr="nationwide" if "nationwide" in b else ""
            rows.append((d,("Nationwide" if dr else ""),dr,"")); continue
        cstate=Counter(assign[f][0] for f in known)
        top,topn=cstate.most_common(1)[0]
        if len(cstate)>1 and topn/len(known)<0.8:
            dr="multi-state: "+"+".join(sorted(cstate))
            mac=best_macro(cstate) or "Multi-Region"
        else:
            st=top; stk=[f for f in known if assign[f][0]==st]
            if "state" in b.split("+") or len(stk)>=0.8*len(state_counties[st]):
                dr=f"state: {st}"
            else:
                creg=Counter(assign[f][1] for f in stk)
                rr,rn=creg.most_common(1)[0]
                dr=(f"region: {rr}" if rn/len(stk)>=0.8 and not rr.endswith("statewide") else f"state: {st}")
            mac=(sorted((len(MACRO[x]),x) for x in state2macros.get(st,[]))[0][1] if state2macros.get(st) else "")
        rows.append((d,mac,dr,",".join(states)))
    t2=pa.table({"domain_norm":[r[0] for r in rows],"macro_region":[r[1] for r in rows],
        "demo_region":[r[2] for r in rows],"declared_states":[r[3] for r in rows],
        "materialized_at":pa.array([now]*len(rows),pa.timestamp("us",tz="UTC"))})
    ds2=lance.write_dataset(t2,"s3://data-sink/active/equipment_company_demo_region/",mode="overwrite",storage_options=so())
    ds2.create_scalar_index("domain_norm","BTREE")
    print("assignment mart:",ds2.count_rows())
    # ---- demoRegions.ts ----
    def circle(fs):
        pts=[county_latlon[f] for f in fs if f in county_latlon]
        if not pts: return None
        la=sum(x[0] for x in pts)/len(pts); lo=sum(x[1] for x in pts)/len(pts)
        r=max((haversine((la,lo),x) for x in pts), default=100)
        return round(la,3), round(lo,3), max(60,min(900,round(r*1.15)))
    macro_circle={m:circle(set().union(*[state_counties[s] for s in sts])) for m,sts in MACRO.items()}
    out={}
    def title(s): return " ".join(w.upper() if len(w)==2 and w.isalpha() else w.capitalize() for w in s.split())
    for d,mac,dr,sts_ in rows:
        if not mac or mac in ("Nationwide","Multi-Region",""): continue
        cs,_=results[d]
        if not cs: continue
        if dr.startswith("state:"):
            st=dr.split(":")[1].strip(); dfs=state_counties.get(st,set()); dname=st
        elif dr.startswith("multi-state:"):
            ss=dr.split(":")[1].strip().split("+"); dfs=set().union(*[state_counties.get(x,set()) for x in ss]); dname=" + ".join(ss)
        elif dr.startswith("region:"):
            rn=dr.split(":",1)[1].strip(); dfs={f for f,(s,r) in assign.items() if r==rn}; dname=title(rn)
        else: continue
        mc=macro_circle.get(mac); dc=circle(dfs)
        if not mc or not dc: continue
        out[d]={"macro":mac,"mLat":mc[0],"mLon":mc[1],"mR":mc[2],"drill":dname,
                "dLat":dc[0],"dLon":dc[1],"dR":dc[2],"states":sts_}
    ts=("/** GENERATED "+dt.date.today().isoformat()+" — equipment-yard demo regions (bake_company_regions.py).\n"
        " * Source: equipment_company_demo_region + demo_region_catalog. Do not hand-edit. */\n"
        "export type DemoRegion = { macro: string; mLat: number; mLon: number; mR: number; drill: string; dLat: number; dLon: number; dR: number; states: string };\n"
        "export const DEMO_REGIONS: Record<string, DemoRegion> = "+json.dumps(out,separators=(",",":"))+";\n")
    app=os.environ.get("GC_HQ_APP", os.path.expanduser("~/Desktop/gc-hq-new"))
    open(os.path.join(app,"apps/platform-app/src/map/demoRegions.ts"),"w").write(ts)
    print("demoRegions.ts entries:",len(out))

if __name__=="__main__": main()
