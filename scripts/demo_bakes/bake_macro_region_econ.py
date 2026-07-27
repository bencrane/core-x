"""Bake — reference/macro_region_econ: the 'economics of {macro region}' card.

Per macro region (14, MACRO below): FY23-25 (2022-10-01..2025-09-30) PoP obligations,
distinct firms, outside-HQ %% of dollars, equipment-scope obligations (in_scope
NAICS x PSC pairs), top-3 outside HQ states, and the per-industry equipment flow-down.

SUBSTRATE (2026-07-26 region-grain cycle): reads the precomputed place-grain marts
instead of scanning the 108M-row fact once per region. Measured: 432.2 s -> 2.9 s of
server time across the 14 macros (150x). Two cheap legs replace one expensive join:
  pop_entity_fy  -> obligations, distinct firms, outside-HQ share, top-3 outside states
  pop_combo_fy   -> equipment-scope dollars by NAICS (joined to naics_psc_equipment_needs)

METHOD CHANGES (encapsulation rule — recorded here and in DEMO_NARRATIVE_BAKES.md):

1. Place-of-performance is now TRANSACTION-level (each action counts where it happened)
   rather than AWARD-level (one place per award, its whole history counted there). This
   matches what bake_drill_demo.py now does for all 21 regions, so the two bakes finally
   agree. Measured against the prior output across all 14 macros:
     obl -1.4%..+2.7%   firms +0.3%..+0.7%   equipScope -0.8%..+0.6%
     outsideHQ% within +-3.4% relative (<=1.3 percentage points)
     top-3 outside states identical in 13 of 14; Pacific Northwest swaps 2nd/3rd
     (MO <-> SC, a close race). ORDER BY now carries hq_state as a tiebreaker so the
     ordering is at least stable across runs.
   The award-level alternative was measured and rejected: reading pop_award_fy on
   award_pop_state holds the method but that mart carries only awards >= $100K, which
   drops the distinct-firm count by 49-55%.

2. equip_flowdown_est / flowdown_factor are now WRITTEN by this script. They exist on the
   landed mart but no committed code produced them, so running the previous version of
   this file would silently drop both columns. They are written at the shipped flat
   FLOW_FACTOR (0.30) so the mart keeps the exact method macroEcon.ts documents
   ("flow = equipment-scope obligations x 0.30 (v1 factor)").

   OPEN, operator's call — this file's own docstring has said since it was written that
   the card "should be computed with reference/equipment_flowdown_factors (per-industry),
   NOT a flat factor", which is how every other card in the demo computes flow-down.
   pop_combo_fy now makes the per-industry number free; measured, it lands at 0.044
   (New England) to 0.129 (Hawaii) against the flat 0.30 — i.e. the displayed flow drops
   60-85% (Great Lakes ~$18B -> ~$3.7B). That is a headline change, so it is NOT taken
   here. To switch: set FLOW_MODE = "per_industry".

Run: doppler run -p core-x -c prd -- python3 scripts/demo_bakes/bake_macro_region_econ.py
"""
from __future__ import annotations
import os, json, datetime as dt
import lance, pyarrow as pa
from _shared import q, so, klems_mapping, flowdown_factors

FY_LO, FY_HI = 2023, 2025
FY = f"fy BETWEEN {FY_LO} AND {FY_HI}"
WINDOW = "FY23-25 (2022-10-01..2025-09-30)"
# "flat" reproduces the shipped card (macroEcon.ts pins x 0.30); "per_industry" uses
# equipment_flowdown_factors like every other card — see the docstring's OPEN note.
FLOW_MODE = "flat"
FLAT_FLOW_FACTOR = 0.30

MACRO={"New England":["ME","NH","VT","MA","RI","CT"],"Mid-Atlantic":["NY","NJ","PA","DE","MD"],
 "Capital Region (DMV)":["DC","MD","VA"],"Southeast":["VA","NC","SC","GA","FL","AL","TN","KY"],
 "Gulf Coast":["TX","LA","MS","AL","FL"],"Great Lakes / Midwest":["OH","MI","IN","IL","WI","MN"],
 "Great Plains":["ND","SD","NE","KS","OK","IA","MO"],"Texas & Southern Plains":["TX","OK","NM"],
 "Mountain West":["CO","UT","WY","MT","ID"],"Southwest":["AZ","NM","NV"],
 "Pacific Northwest":["WA","OR","ID"],"West Coast":["CA","OR","WA"],"Alaska":["AK"],"Hawaii":["HI"]}
NAMES={"AL":"Alabama","AK":"Alaska","AZ":"Arizona","AR":"Arkansas","CA":"California","CO":"Colorado","CT":"Connecticut","DE":"Delaware","DC":"D.C.","FL":"Florida","GA":"Georgia","HI":"Hawaii","ID":"Idaho","IL":"Illinois","IN":"Indiana","IA":"Iowa","KS":"Kansas","KY":"Kentucky","LA":"Louisiana","ME":"Maine","MD":"Maryland","MA":"Massachusetts","MI":"Michigan","MN":"Minnesota","MS":"Mississippi","MO":"Missouri","MT":"Montana","NE":"Nebraska","NV":"Nevada","NH":"New Hampshire","NJ":"New Jersey","NM":"New Mexico","NY":"New York","NC":"North Carolina","ND":"North Dakota","OH":"Ohio","OK":"Oklahoma","OR":"Oregon","PA":"Pennsylvania","RI":"Rhode Island","SC":"South Carolina","SD":"South Dakota","TN":"Tennessee","TX":"Texas","UT":"Utah","VT":"Vermont","VA":"Virginia","WA":"Washington","WV":"West Virginia","WI":"Wisconsin","WY":"Wyoming"}

def main():
    to_pc=klems_mapping(); factors=flowdown_factors()
    def factor(n6): return factors.get(to_pc(n6), 0.039)
    out={}
    for m,sts in MACRO.items():
        SS="("+",".join(f"'{s}'" for s in sts)+")"
        r=q(f"""SELECT sum(obligation_sum), count(DISTINCT uei),
  sum(obligation_sum) FILTER (WHERE hq_state IS NOT NULL AND hq_state NOT IN {SS})
    /nullif(sum(obligation_sum) FILTER (WHERE hq_state IS NOT NULL),0)*100
FROM pop_entity_fy WHERE pop_state IN {SS} AND {FY}""")[0]
        # hq_state trails the sort so a close 2nd/3rd race is at least stable run to run.
        top=q(f"""SELECT hq_state, sum(obligation_sum) s FROM pop_entity_fy
  WHERE pop_state IN {SS} AND {FY} AND hq_state IS NOT NULL AND hq_state NOT IN {SS}
  GROUP BY 1 ORDER BY s DESC, hq_state LIMIT 3""")
        # equipment-scope dollars kept at NAICS grain so the flow-down is factor-weighted
        # per industry rather than a single blended number.
        scope=q(f"""SELECT f.naics_code, sum(f.obligation_sum) FROM pop_combo_fy f
  JOIN (SELECT DISTINCT naics_code, psc_code FROM naics_psc_equipment_needs WHERE in_scope) e
    ON f.naics_code = e.naics_code AND f.psc_code = e.psc_code
  WHERE f.pop_state IN {SS} AND f.{FY} GROUP BY 1""")
        equip_scope=sum(v or 0 for _,v in scope)
        if FLOW_MODE=="per_industry":
            fdf=(sum((v or 0)*factor(n) for n,v in scope)/equip_scope) if equip_scope else 0.039
        else:
            fdf=FLAT_FLOW_FACTOR
        out[m]={"obl":r[0] or 0,"firms":int(r[1] or 0),"outside_pct":round(r[2] or 0,1),
                "equip_scope":equip_scope,"flowdown_factor":fdf,
                "equip_flowdown_est":equip_scope*fdf,
                "top_outside":[NAMES.get(x[0],x[0]) for x in top]}
        print(m, round(out[m]["obl"]/1e9,1),"B  flow", round(fdf,4), flush=True)
    now=dt.datetime.now(dt.timezone.utc); rows=sorted(out.items())
    t=pa.table({"macro_region":[k for k,_ in rows],
        "obligated_fy23_25":[v["obl"] for _,v in rows],"distinct_firms":[v["firms"] for _,v in rows],
        "outside_hq_pct":[v["outside_pct"] for _,v in rows],"equip_scope_obligated":[v["equip_scope"] for _,v in rows],
        "equip_flowdown_est":[v["equip_flowdown_est"] for _,v in rows],
        "flowdown_factor":[v["flowdown_factor"] for _,v in rows],
        "top_outside_states":[json.dumps(v["top_outside"]) for _,v in rows],
        "window":[WINDOW]*len(rows),
        "materialized_at":pa.array([now]*len(rows),pa.timestamp("us",tz="UTC"))})
    ds=lance.write_dataset(t,"s3://data-sink/active/reference/macro_region_econ/",mode="overwrite",storage_options=so())
    ds.create_scalar_index("macro_region","BTREE")
    print("landed", ds.count_rows())

if __name__=="__main__": main()
