"""Shared plumbing for the demo bakes — creds, sidecar client, KLEMS mapping, formatting.

ENCAPSULATION RULE (operator-ruled 2026-07-26): these scripts are the single source of
truth for how every demo number is computed. Any change to a number's method is made
HERE (and in the sibling scripts) and re-run — never as ad-hoc session queries.
"""
from __future__ import annotations
import os, json, re, time, urllib.request
from collections import defaultdict

SIDECAR_URL="https://query-sidecar-api.onrender.com/api/v1/sql"

def so():
    return {"endpoint": os.environ["R2_ENDPOINT"], "region": "auto",
            "access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"]}

def q(sql, limit=50000, tries=4):
    for i in range(tries):
        try:
            req=urllib.request.Request(SIDECAR_URL, data=json.dumps({"sql":sql,"limit":limit}).encode(),
                headers={"Authorization":f"Bearer {os.environ['QUERY_SIDECAR_TOKEN']}","content-type":"application/json"})
            return json.load(urllib.request.urlopen(req, timeout=130))["rows"]
        except Exception:
            if i==tries-1: raise
            time.sleep(15*(i+1))

def norm_domain(d):
    d=(d or "").lower().strip()
    d=re.sub(r'^https?://','',d).split('/')[0]
    return d[4:] if d.startswith('www.') else d

PATCH={"5182":"513","5192":"513","5191":"513","5132":"PUB","5112":"PUB","5111":"PUB"}

def _prefixes(nstr):
    out=[]
    for tokn in re.split(r'[,;]', str(nstr or "")):
        t=tokn.strip()
        m=re.match(r'^(\d+)\s*[-–]\s*(\d+)$', t)
        if m and len(m.group(1))==len(m.group(2)):
            out += [str(v).zfill(len(m.group(1))) for v in range(int(m.group(1)), int(m.group(2))+1)]
        elif re.match(r'^\d+$', t): out.append(t)
    return out

def klems_mapping():
    """naics6 -> KLEMS production_code mapper (prefix fallback + info-sector PATCH)."""
    import lance
    kl=lance.dataset("s3://data-sink/active/bea_bls_klems", storage_options=so())\
        .to_table(columns=["production_code","naics_2017"]).to_pydict()
    pref2prod={}
    seen=set()
    for pc,n in zip(kl["production_code"],kl["naics_2017"]):
        if pc is None or (pc,n) in seen: continue
        seen.add((pc,n))
        for p in _prefixes(n): pref2prod.setdefault(p,pc)
    def to_pc(n6):
        n6=str(n6 or "")
        for L in range(6,1,-1):
            if n6[:L] in pref2prod: return pref2prod[n6[:L]]
        for p,pc in PATCH.items():
            if n6.startswith(p): return pc
        return None
    return to_pc

def flowdown_factors():
    """production_code -> equipment_related_share from reference/equipment_flowdown_factors."""
    import lance
    t=lance.dataset("s3://data-sink/active/reference/equipment_flowdown_factors", storage_options=so())\
        .to_table(columns=["production_code","equipment_related_share"]).to_pydict()
    return dict(zip(t["production_code"],t["equipment_related_share"]))

def fb(x):
    b=x/1e9
    if b>=10: return f"${b:.0f}B"
    if b>=1: return f"${b:.1f}B"
    return f"${x/1e6:.0f}M"
