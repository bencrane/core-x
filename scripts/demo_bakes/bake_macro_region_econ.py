"""Bake — reference/macro_region_econ: the 'economics of {macro region}' card (as-run 2026-07-26).

Per macro region (14, MACRO below): FY23-25 (2022-10-01..2025-09-30) PoP obligations,
distinct firms, outside-HQ %% of dollars, equipment-scope obligations (in_scope
NAICS x PSC pairs), top-3 outside HQ states. The card's displayed flow-down should be
computed with reference/equipment_flowdown_factors (per-industry), NOT a flat factor —
see bake_drill_demo.py for the factor application pattern.

Query shape (sidecar): award_geo_state pop filter -> award_key -> gtm_txn_events_slim
join, date-windowed. ~10-60s/region; retry on 502 (Render instance restart).

Run: doppler run -p core-x -c prd -- python3 scripts/demo_bakes/bake_macro_region_econ.py
"""
from __future__ import annotations
import os, json, time, urllib.request, datetime as dt
import lance, pyarrow as pa

MACRO={"New England":["ME","NH","VT","MA","RI","CT"],"Mid-Atlantic":["NY","NJ","PA","DE","MD"],
 "Capital Region (DMV)":["DC","MD","VA"],"Southeast":["VA","NC","SC","GA","FL","AL","TN","KY"],
 "Gulf Coast":["TX","LA","MS","AL","FL"],"Great Lakes / Midwest":["OH","MI","IN","IL","WI","MN"],
 "Great Plains":["ND","SD","NE","KS","OK","IA","MO"],"Texas & Southern Plains":["TX","OK","NM"],
 "Mountain West":["CO","UT","WY","MT","ID"],"Southwest":["AZ","NM","NV"],
 "Pacific Northwest":["WA","OR","ID"],"West Coast":["CA","OR","WA"],"Alaska":["AK"],"Hawaii":["HI"]}
NAMES={"AL":"Alabama","AK":"Alaska","AZ":"Arizona","AR":"Arkansas","CA":"California","CO":"Colorado","CT":"Connecticut","DE":"Delaware","DC":"D.C.","FL":"Florida","GA":"Georgia","HI":"Hawaii","ID":"Idaho","IL":"Illinois","IN":"Indiana","IA":"Iowa","KS":"Kansas","KY":"Kentucky","LA":"Louisiana","ME":"Maine","MD":"Maryland","MA":"Massachusetts","MI":"Michigan","MN":"Minnesota","MS":"Mississippi","MO":"Missouri","MT":"Montana","NE":"Nebraska","NV":"Nevada","NH":"New Hampshire","NJ":"New Jersey","NM":"New Mexico","NY":"New York","NC":"North Carolina","ND":"North Dakota","OH":"Ohio","OK":"Oklahoma","OR":"Oregon","PA":"Pennsylvania","RI":"Rhode Island","SC":"South Carolina","SD":"South Dakota","TN":"Tennessee","TX":"Texas","UT":"Utah","VT":"Vermont","VA":"Virginia","WA":"Washington","WV":"West Virginia","WI":"Wisconsin","WY":"Wyoming"}

def q(sql, limit=100, tries=4):
    for i in range(tries):
        try:
            req=urllib.request.Request("https://query-sidecar-api.onrender.com/api/v1/sql",
                data=json.dumps({"sql":sql,"limit":limit}).encode(),
                headers={"Authorization":f"Bearer {os.environ['QUERY_SIDECAR_TOKEN']}","content-type":"application/json"})
            return json.load(urllib.request.urlopen(req, timeout=130))["rows"]
        except Exception:
            if i==tries-1: raise
            time.sleep(20*(i+1))

def main():
    S={"endpoint":os.environ["R2_ENDPOINT"],"region":"auto",
       "access_key_id":os.environ["R2_ACCESS_KEY_ID"],"secret_access_key":os.environ["R2_SECRET_ACCESS_KEY"]}
    out={}
    for m,sts in MACRO.items():
        SS="("+",".join(f"'{s}'" for s in sts)+")"
        base=f"""
WITH aw AS (SELECT award_key, any_value(hq_state) hq, any_value(naics_code) naics, any_value(psc_code) psc
  FROM award_geo_state WHERE pop_state IN {SS} GROUP BY award_key),
tx AS (SELECT t.uei, t.obligation, aw.hq, aw.naics, aw.psc
  FROM gtm_txn_events_slim t JOIN aw ON t.award_key=aw.award_key
  WHERE t.action_date >= DATE '2022-10-01' AND t.action_date <= DATE '2025-09-30')"""
        r=q(base+f"""
SELECT sum(obligation), count(DISTINCT uei),
  sum(CASE WHEN hq IS NOT NULL AND hq NOT IN {SS} THEN obligation ELSE 0 END)/nullif(sum(CASE WHEN hq IS NOT NULL THEN obligation END),0)*100,
  sum(CASE WHEN e.naics_code IS NOT NULL THEN obligation ELSE 0 END)
FROM tx LEFT JOIN (SELECT DISTINCT naics_code, psc_code FROM naics_psc_equipment_needs WHERE in_scope) e
  ON tx.naics=e.naics_code AND tx.psc=e.psc_code""")[0]
        top=q(base+f"SELECT hq, sum(obligation) s FROM tx WHERE hq IS NOT NULL AND hq NOT IN {SS} GROUP BY hq ORDER BY s DESC LIMIT 3")
        out[m]={"obl":r[0] or 0,"firms":int(r[1] or 0),"outside_pct":round(r[2] or 0,1),
                "equip_scope":r[3] or 0,"top_outside":[NAMES.get(x[0],x[0]) for x in top]}
        print(m, round(out[m]["obl"]/1e9,1),"B", flush=True)
    now=dt.datetime.now(dt.timezone.utc); rows=sorted(out.items())
    t=pa.table({"macro_region":[k for k,_ in rows],
        "obligated_fy23_25":[v["obl"] for _,v in rows],"distinct_firms":[v["firms"] for _,v in rows],
        "outside_hq_pct":[v["outside_pct"] for _,v in rows],"equip_scope_obligated":[v["equip_scope"] for _,v in rows],
        "top_outside_states":[json.dumps(v["top_outside"]) for _,v in rows],
        "window":["FY23-25 (2022-10-01..2025-09-30)"]*len(rows),
        "materialized_at":pa.array([now]*len(rows),pa.timestamp("us",tz="UTC"))})
    ds=lance.write_dataset(t,"s3://data-sink/active/reference/macro_region_econ/",mode="overwrite",storage_options=S)
    ds.create_scalar_index("macro_region","BTREE")
    print("landed", ds.count_rows())

if __name__=="__main__": main()
