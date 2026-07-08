"""Measured serve-path benchmark against the PERSISTED probe blob (no rebuild).
Answers: how long does the on-screen path actually take once the target is loaded?
  - deserialize cost (JSON parse, the real R2-fetch tax)
  - INSTANT class: a scalar predicate over 5,000 node dicts
  - EXPENSIVE class: an event-grain time-window predicate (scans all event rows)
  - the bucketing counterfactual: same time predicate answered from a prebuilt
    per-node monthly index instead of raw rows
Timing uses time.perf_counter; no wall-clock/random deps."""
import json, pickle, time
from datetime import date, timedelta

SP = "/private/tmp/claude-501/-Users-benjamincrane-core-x--claude-worktrees-objective-wozniak-5ded4e/d5ac7f55-4a32-4c98-a4b8-58ece6beb5a5/scratchpad"

def t():
    return time.perf_counter()

# --- deserialize cost (JSON parse == the real fetch-and-hydrate tax) ---
with open(f"{SP}/blob_YZBMKTR1HHL7.json") as f:
    raw = f.read()
a = t(); blob = json.loads(raw); json_ms = (t() - a) * 1000

a = t()
with open(f"{SP}/blob_YZBMKTR1HHL7.pkl", "rb") as f:
    _ = pickle.load(f)
pkl_ms = (t() - a) * 1000

nodes = blob["universe"]["nodes"]
n = len(nodes)

# --- INSTANT class: scalar predicate (HQ state == CT), over all nodes ---
a = t()
hits_scalar = [nd for nd in nodes
               if (nd.get("entity") or {}).get("physical_state") == "CT"]
scalar_ms = (t() - a) * 1000

# --- INSTANT class 2: farmout $ threshold (scalar node fact) ---
a = t()
hits_fo = [nd for nd in nodes
           if (nd.get("matched_farmout_60mo") or 0) >= 1_000_000]
fo_ms = (t() - a) * 1000

# --- EXPENSIVE class: "funded in the last 30 days" — scan event rows ---
cutoff = (date.today() - timedelta(days=30)).isoformat()
a = t()
hits_ev = []
scanned = 0
for nd in nodes:
    evs = nd["demand_events"]["events"]
    for e in evs:
        scanned += 1
        ad = e.get("action_date")
        if ad and str(ad) >= cutoff and (e.get("obligation_delta") or 0) > 0:
            hits_ev.append(nd["uei"]); break
ev_ms = (t() - a) * 1000

# --- EXPENSIVE class, heavier: full aggregation over ALL event rows ---
a = t()
total_recent = 0.0
for nd in nodes:
    for e in nd["demand_events"]["events"]:
        ad = e.get("action_date")
        if ad and str(ad) >= cutoff:
            total_recent += (e.get("obligation_delta") or 0)
agg_ms = (t() - a) * 1000

# --- BUCKETING counterfactual: build a per-node month index once, then query ---
a = t()
month_idx = []  # list of dict[month->$] per node
for nd in nodes:
    mi = {}
    for e in nd["demand_events"]["events"]:
        ad = e.get("action_date")
        if ad:
            mkey = str(ad)[:7]
            mi[mkey] = mi.get(mkey, 0.0) + (e.get("obligation_delta") or 0)
    month_idx.append(mi)
build_idx_ms = (t() - a) * 1000  # this would be BUILD-TIME, shown for scale

recent_months = {(date.today() - timedelta(days=d)).isoformat()[:7] for d in range(0, 95, 15)}
a = t()
hits_bucket = [nodes[i]["uei"] for i, mi in enumerate(month_idx)
               if any(m in mi and mi[m] > 0 for m in recent_months)]
bucket_ms = (t() - a) * 1000

print(f"=== SERVE-PATH BENCHMARK — YZBMKTR1HHL7 ({n} nodes, 282,540 event rows) ===")
print(f"deserialize JSON (130MB)          : {json_ms:8.1f} ms   <- the fat-blob fetch tax")
print(f"deserialize pickle (58MB)         : {pkl_ms:8.1f} ms")
print(f"--- INSTANT class (scalar node facts) ---")
print(f"filter HQ state == CT             : {scalar_ms:8.1f} ms   ({len(hits_scalar)} hits)")
print(f"filter farmout_60mo >= $1M        : {fo_ms:8.1f} ms   ({len(hits_fo)} hits)")
print(f"--- EXPENSIVE class (scan event rows) ---")
print(f"'funded in last 30d' (any-match)  : {ev_ms:8.1f} ms   ({len(hits_ev)} hits, {scanned} rows scanned)")
print(f"aggregate $ over recent events    : {agg_ms:8.1f} ms   (${total_recent:,.0f})")
print(f"--- BUCKETING counterfactual ---")
print(f"[build monthly index once]        : {build_idx_ms:8.1f} ms   (would be BUILD-TIME)")
print(f"same time query, from buckets     : {bucket_ms:8.1f} ms   ({len(hits_bucket)} hits)")
print("BENCH DONE")
