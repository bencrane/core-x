#!/usr/bin/env python3
"""READ-ONLY verify of the landed v2 free-form labor rows in govcon_award_requirements."""
from __future__ import annotations
import json, os, sys
import duckdb, lance

A = "s3://data-sink/active"
REQ = f"{A}/govcon_award_requirements/"
TAG = "llm:session-opus@v2-freeform-labor"
VOCAB = json.load(open("/Users/benjamincrane/core-x/pipelines/sam_gov/reference/govcon_llm_lane_v2/vocabulary.json"))
LSET = set(VOCAB["labor_categories"])

def r2_so():
    ep=os.environ.get("R2_ENDPOINT") or (f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com" if os.environ.get("R2_ACCOUNT_ID") else None)
    return {"aws_access_key_id":os.environ["R2_ACCESS_KEY_ID"],"aws_secret_access_key":os.environ["R2_SECRET_ACCESS_KEY"],"endpoint":ep,"region":"auto"}

con=duckdb.connect(":memory:"); con.execute("PRAGMA threads=8")
con.register("req", lance.dataset(REQ, storage_options=r2_so()))
out={}
out["v2_rows_total"]=con.execute("SELECT count(*) FROM req WHERE extractor=?",[TAG]).fetchone()[0]
out["v2_by_type"]=con.execute("SELECT requirement_type, count(*) n, count(DISTINCT requirement_value) d FROM req WHERE extractor=? GROUP BY 1 ORDER BY n DESC",[TAG]).fetchall()
out["v2_distinct_resources"]=con.execute("SELECT count(DISTINCT resource_id) FROM req WHERE extractor=?",[TAG]).fetchone()[0]
labor=con.execute("SELECT requirement_value, count(*) n FROM req WHERE extractor=? AND requirement_type='labor_category' GROUP BY 1 ORDER BY n DESC",[TAG]).fetchall()
out["v2_labor_distinct"]=len(labor)
out["v2_labor_total"]=sum(n for _,n in labor)
oov=[v for v,_ in labor if (v or '').lower() not in LSET]
out["v2_labor_out_of_vocab_distinct"]=len(oov)
out["v2_labor_oov_pct"]=round(100*len(oov)/max(1,len(labor)),1)
# IT/cyber/software signal — the recall goal
import re
itrx=re.compile(r"engineer|developer|software|cyber|system|network|data|cloud|analyst|administrator|architect|program|scientist|devops|security|information|technolog|it ", re.I)
out["v2_labor_it_signal_distinct"]=sum(1 for v,_ in labor if itrx.search(v or ''))
out["sample_it_titles"]=[v for v,_ in labor if itrx.search(v or '')][:30]
out["top_labor_titles"]=[{"v":v,"n":n} for v,n in labor[:20]]
json.dump(out, sys.stdout, indent=2, default=str); print()
