"""Bake — cost-structure marts + equipment flow-down factors (as-run 2026-07-26).

Writes (Lance, overwrite):
  reference/cost_structure_vectors     per-KLEMS-industry granular shares (KLEMS-2024
                                       10 components, sum=1.0/industry; + VA comp share,
                                       FA equipment-share-of-investment, IO-2017
                                       equipment-rental input share side-by-side)
  reference/cost_structure_weighted    active-book dollar-weighted aggregate
  reference/ecec_comp_components       ECEC comp component split (national + health by
                                       industry group), private industry, latest quarter
  reference/equipment_flowdown_factors OUR equipment-related share per industry:
                                       purchase + bare rental + operated-rental est
                                       (bare x OP_MULT, capped at 50% of purchased svcs)

Method gates (do not regress):
  - NAICS6 -> KLEMS mapping >= 99% of active-book dollars (prefix fallback + the
    info-sector PATCH map; public admin -> explicit residual, never silently dropped)
  - KLEMS shares sum to 1.0 per industry
  - weighted payroll lands ~39% (sanity vs the known ~42.5% page figure)

Run: doppler run -p core-x -c prd -- python3 scripts/demo_bakes/bake_cost_structure.py
"""
from __future__ import annotations
import os, json, re, urllib.request, datetime as dt
from collections import defaultdict
import duckdb, lance, pyarrow as pa

SIDECAR="https://query-sidecar-api.onrender.com/api/v1/sql"
OP_MULT=3.0
PATCH={"5182":"513","5192":"513","5191":"513","5132":"PUB","5112":"PUB","5111":"PUB"}
COMPS=["labor_college","labor_noncollege","energy","materials","purchased_services",
       "capital_it","capital_software","capital_rd","capital_artistic","capital_other"]
SHEETS={"Labor_Col Compensation":"labor_college","Labor_NoCol Compensation":"labor_noncollege",
 "Energy Compensation":"energy","Materials Compensation":"materials","Service Compensation":"purchased_services",
 "Capital_IT Compensation":"capital_it","Capital_Software Compensation":"capital_software",
 "Capital_R&D Compensation":"capital_rd","Capital_Art Compensation":"capital_artistic",
 "Capital_Other Compensation":"capital_other","Gross Output":"gross_output"}

def so():
    return {"endpoint": os.environ["R2_ENDPOINT"], "region":"auto",
            "access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"]}

def sidecar(sql, limit=50000):
    req=urllib.request.Request(SIDECAR, data=json.dumps({"sql":sql,"limit":limit}).encode(),
        headers={"Authorization":f"Bearer {os.environ['QUERY_SIDECAR_TOKEN']}","content-type":"application/json"})
    return json.load(urllib.request.urlopen(req, timeout=130))["rows"]

def prefixes(nstr):
    out=[]
    for tokn in re.split(r'[,;]', str(nstr or "")):
        t=tokn.strip()
        m=re.match(r'^(\d+)\s*[-–]\s*(\d+)$', t)
        if m and len(m.group(1))==len(m.group(2)):
            out += [str(v).zfill(len(m.group(1))) for v in range(int(m.group(1)), int(m.group(2))+1)]
        elif re.match(r'^\d+$', t): out.append(t)
    return out

def main():
    con=duckdb.connect(); S=so()
    for n in ["bea_bls_klems","bea_industry_value_added","bea_fixed_assets_detail",
              "bea_io_use_detail","bea_naics_concordance","bls_ecec_costs"]:
        con.register(n, lance.dataset(f"s3://data-sink/active/{n}", storage_options=S).to_table())

    # KLEMS vectors (2024)
    vec=defaultdict(dict); desc={}
    for pc,d,sheet,v in con.sql("""SELECT coalesce(production_code,'PUB'), any_value(industry_desc), sheet, sum(value_num)
        FROM bea_bls_klems WHERE year=2024 GROUP BY 1,3""").fetchall():
        if sheet in SHEETS: vec[pc][SHEETS[sheet]]=v or 0; desc[pc]=d
    klems={}
    for pc,m in vec.items():
        go=m.get("gross_output") or 0
        if not go: continue
        klems[pc]={k:m.get(k,0)/go for k in SHEETS.values() if k!="gross_output"}
        assert abs(sum(klems[pc].values())-1.0)<0.02, f"shares !~1 for {pc}"

    # prefix map from KLEMS naics_2017
    pref2prod={}
    for pc,n in con.sql("SELECT production_code, any_value(naics_2017) FROM bea_bls_klems WHERE production_code IS NOT NULL GROUP BY 1").fetchall():
        for p in prefixes(n): pref2prod.setdefault(p,pc)
    def to_pc(n6):
        n6=str(n6 or "")
        for L in range(6,1,-1):
            if n6[:L] in pref2prod: return pref2prod[n6[:L]]
        return None

    # active-book weights
    book=sidecar("SELECT naics_code, sum(active_obligated) FROM combo_award_active_state GROUP BY 1")
    mapped=defaultdict(float); resid=0.0
    for n,v in book:
        v=v or 0; pc=to_pc(n)
        if not pc:
            for p,ppc in PATCH.items():
                if str(n or "").startswith(p): pc=ppc; break
        if pc: mapped[pc]+=v
        else: resid+=v
    total=sum(mapped.values())+resid
    assert sum(mapped.values())/total>=0.99, "mapping coverage regression"

    # FA equipment share of investment / IO rental share / VA comp share
    # FA industry codes carry letter suffixes for split industries (336M motor vehicles,
    # 336O other transport equip incl aerospace/ships, 5220 etc.) — map letters explicitly;
    # numeric codes map by prefix. Missing this dropped the equipment-purchase component
    # for exactly the transport-equipment industries (bug found 2026-07-26).
    FA_LETTER={"336M":"3361MV","336O":"3364OT","313T":"313TT","315A":"315AL","311A":"311FT","337A":"337","339A":"339"}
    fa=defaultdict(lambda:[0,0])
    for code,eq,tot in con.sql("""SELECT industry_code,
        sum(CASE WHEN asset_code LIKE 'E%' THEN value_musd ELSE 0 END), sum(value_musd)
        FROM bea_fixed_assets_detail WHERE measure='investment' AND year=2024 GROUP BY 1""").fetchall():
        pc=FA_LETTER.get(code)
        if not pc:
            digits=code[:4].rstrip("0")
            pc=to_pc(digits or code[:2]) or to_pc(code[:3]) or to_pc(code[:2])
        if pc: fa[pc][0]+=eq or 0; fa[pc][1]+=tot or 0
    fa_share={pc:(e/t if t else None) for pc,(e,t) in fa.items()}
    d2s={d:s for d,s in con.sql("SELECT bea_detail_code, bea_summary_code FROM bea_naics_concordance GROUP BY 1,2").fetchall() if d}
    iob=defaultdict(lambda:[0,0])
    for code,rent,outp in con.sql("""
        WITH inputs AS (SELECT industry_code,
            sum(CASE WHEN commodity_code IN ('532100','532400','532A00') THEN value_musd ELSE 0 END) rent,
            sum(value_musd) allin
          FROM bea_io_use_detail WHERE year=2017 AND col_kind='industry' AND row_kind='commodity' GROUP BY 1),
        va AS (SELECT industry_code, sum(value_musd) va FROM bea_io_use_detail
          WHERE year=2017 AND col_kind='industry' AND row_kind='total_or_va' GROUP BY 1)
        SELECT i.industry_code, i.rent, i.allin+coalesce(v.va,0) FROM inputs i LEFT JOIN va v USING(industry_code)""").fetchall():
        s=d2s.get(code) or code
        pc=to_pc(re.sub(r'[^0-9]','',s or "")[:4] or "x")
        if pc: iob[pc][0]+=rent or 0; iob[pc][1]+=outp or 0
    io_share={pc:(r/o if o else None) for pc,(r,o) in iob.items()}
    name2pc={(d or "").lower():pc for pc,d in desc.items()}
    va_share={}
    for nm,comp,go in con.sql("""SELECT industry_name,
        max(CASE WHEN component='compensation_of_employees' THEN value END),
        max(CASE WHEN component='gross_output' THEN value END)
        FROM bea_industry_value_added WHERE year=2024 GROUP BY 1""").fetchall():
        pc=name2pc.get((nm or "").lower())
        if pc and comp and go: va_share[pc]=comp/go

    now=dt.datetime.now(dt.timezone.utc)
    rows=[]
    for pc,v in klems.items():
        d=desc.get(pc) or ""
        rows += [(pc,d,c,v[c],"bea_bls_klems",2024) for c in COMPS]
        if va_share.get(pc) is not None: rows.append((pc,d,"payroll_va_comp_share",va_share[pc],"bea_industry_value_added",2024))
        if fa_share.get(pc) is not None: rows.append((pc,d,"equipment_share_of_investment",fa_share[pc],"bea_fixed_assets_detail",2024))
        if io_share.get(pc) is not None: rows.append((pc,d,"equipment_rental_share_of_output",io_share[pc],"bea_io_use_detail",2017))
    t=pa.table({"production_code":[r[0] for r in rows],"industry_desc":[r[1] for r in rows],
        "component":[r[2] for r in rows],"share":[float(r[3]) for r in rows],
        "source":[r[4] for r in rows],"source_year":[r[5] for r in rows],
        "materialized_at":pa.array([now]*len(rows),pa.timestamp("us",tz="UTC"))})
    ds=lance.write_dataset(t,"s3://data-sink/active/reference/cost_structure_vectors/",mode="overwrite",storage_options=S)
    ds.create_scalar_index("production_code","BTREE")

    agg=defaultdict(float); wcov=0.0
    for pc,w in mapped.items():
        v=klems.get(pc)
        if not v: continue
        wcov+=w
        for c in COMPS: agg[c]+=w*v[c]
    for c in COMPS: agg[c]/=wcov
    payroll=agg["labor_college"]+agg["labor_noncollege"]
    assert 0.30<payroll<0.50, "payroll sanity"
    weq=sum(w*(fa_share.get(pc) or 0) for pc,w in mapped.items() if klems.get(pc))/wcov
    wio=sum(w*(io_share.get(pc) or 0) for pc,w in mapped.items() if klems.get(pc))/wcov
    vaw=[(w,va_share[pc]) for pc,w in mapped.items() if klems.get(pc) and va_share.get(pc)]
    wva=sum(w*s for w,s in vaw)/sum(w for w,_ in vaw)
    wrows=[(k,v,v*total) for k,v in agg.items()]
    wrows += [("payroll_total",payroll,payroll*total),
              ("equipment_capital_est",agg["capital_other"]*weq+agg["capital_it"],(agg["capital_other"]*weq+agg["capital_it"])*total),
              ("equipment_rental_inputs",wio,wio*total),("payroll_va_alt",wva,wva*total)]
    t2=pa.table({"component":[x[0] for x in wrows],"weighted_share":[float(x[1]) for x in wrows],
        "dollars_vs_active_book":[float(x[2]) for x in wrows],
        "method":[f"active-book NAICS6 $ -> KLEMS (coverage {sum(mapped.values())/total*100:.1f}%, residual ${resid/1e9:.1f}B) x KLEMS-2024 shares; book=${total/1e9:.1f}B"]*len(wrows),
        "materialized_at":pa.array([now]*len(wrows),pa.timestamp("us",tz="UTC"))})
    lance.write_dataset(t2,"s3://data-sink/active/reference/cost_structure_weighted/",mode="overwrite",storage_options=S)

    # flow-down factors (OUR definition)
    frows=[]
    for pc,v in klems.items():
        p=v["capital_other"]*(fa_share.get(pc) or 0)+v["capital_it"]
        b=io_share.get(pc) or 0
        o=min(b*OP_MULT, v["purchased_services"]*0.5)
        frows.append((pc,desc.get(pc) or "",p,b,o,p+b+o))
    t3=pa.table({"production_code":[r[0] for r in frows],"industry_desc":[r[1] for r in frows],
        "purchase_share":[float(r[2]) for r in frows],"bare_rental_share":[float(r[3]) for r in frows],
        "operated_rental_share_est":[float(r[4]) for r in frows],"equipment_related_share":[float(r[5]) for r in frows],
        "method":[f"purchase=KLEMS capital_other x FA equip-share + capital_IT; bare=IO-2017 rental commodities; operated=bare x {OP_MULT} capped at 50% purchased services — OUR definition, not BEA's"]*len(frows),
        "materialized_at":pa.array([now]*len(frows),pa.timestamp("us",tz="UTC"))})
    ds3=lance.write_dataset(t3,"s3://data-sink/active/reference/equipment_flowdown_factors/",mode="overwrite",storage_options=S)
    ds3.create_scalar_index("production_code","BTREE")

    # ECEC
    nat=con.sql("""SELECT component, val, year, period FROM (
        SELECT component, year, period, TRY_CAST("value" AS DOUBLE) val,
               row_number() OVER (PARTITION BY component ORDER BY year DESC, period DESC) rn
        FROM bls_ecec_costs WHERE ownership='Private industry workers' AND datatype_code='P'
          AND occupation_group='All occupations' AND subcell='All workers' AND industry_group='All industries'
    ) WHERE rn=1""").fetchall()
    ig=con.sql("""SELECT industry_group, val, year FROM (
        SELECT industry_group, year, period, TRY_CAST("value" AS DOUBLE) val,
               row_number() OVER (PARTITION BY industry_group ORDER BY year DESC, period DESC) rn
        FROM bls_ecec_costs WHERE ownership='Private industry workers' AND datatype_code='P'
          AND component='Health insurance' AND occupation_group='All occupations' AND subcell='All workers'
    ) WHERE rn=1""").fetchall()
    t4=pa.table({"scope":["national"]*len(nat)+["industry_group"]*len(ig),
        "key":["All industries"]*len(nat)+[r[0] for r in ig],
        "component":[r[0] for r in nat]+["Health insurance"]*len(ig),
        "pct_of_total_comp":[float(r[1]) if r[1] is not None else None for r in nat]+[float(r[1]) for r in ig],
        "as_of":[f"{r[2]}{r[3]}" for r in nat]+[str(r[2]) for r in ig],
        "materialized_at":pa.array([now]*(len(nat)+len(ig)),pa.timestamp("us",tz="UTC"))})
    lance.write_dataset(t4,"s3://data-sink/active/reference/ecec_comp_components/",mode="overwrite",storage_options=S)
    print(json.dumps({"payroll":round(payroll,4),"equip_capital":round(agg["capital_other"]*weq+agg["capital_it"],4),
                      "coverage":round(sum(mapped.values())/total,4),"status":"success"},indent=1))

if __name__=="__main__": main()
