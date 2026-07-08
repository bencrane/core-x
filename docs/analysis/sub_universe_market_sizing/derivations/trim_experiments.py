"""Offline trim design-search against the persisted probe blob (no rebuild).
Determines the exact (node-tiering threshold x event-bucket granularity x
portfolio cap) that puts the HOT blob under the single-digit-MB budget and
therefore the load under sub-second. Also sizes the events sidecar.

Load-time model (measured, serve_bench.py): JSON parse ~3.06 ms/MB
(398 ms / 130 MB). Single-digit MB => ~<30 ms parse + sub-second R2 transfer.
"""
import json, pickle
from collections import defaultdict

SP = "/private/tmp/claude-501/-Users-benjamincrane-core-x--claude-worktrees-objective-wozniak-5ded4e/d5ac7f55-4a32-4c98-a4b8-58ece6beb5a5/scratchpad"
MS_PER_MB = 398.5 / 130.0  # measured parse rate

def mb(x):
    return len(json.dumps(x, default=str)) / 1e6

with open(f"{SP}/blob_YZBMKTR1HHL7.pkl", "rb") as f:
    blob = pickle.load(f)
nodes = blob["universe"]["nodes"]
N = len(nodes)

# ---- schema sanity: what fields does a node carry? ----
print("=== node schema (node0 top-level keys) ===")
print(sorted(nodes[0].keys()))
print("demand_events sub-keys:", sorted(nodes[0]["demand_events"].keys()))
ev0 = nodes[0]["demand_events"]["events"][0]
print("event row keys:", sorted(ev0.keys()))
print()

# ---- materiality signals distribution ----
disc = sum(1 for nd in nodes if nd.get("disclosed_sub_buyer"))
fo_pos = sum(1 for nd in nodes if (nd.get("matched_farmout_60mo") or 0) > 0)
fo_100k = sum(1 for nd in nodes if (nd.get("matched_farmout_60mo") or 0) >= 100_000)
print(f"=== materiality signals across {N} nodes ===")
print(f"disclosed_sub_buyer=True      : {disc}")
print(f"matched_farmout_60mo > 0      : {fo_pos}")
print(f"matched_farmout_60mo >= $100K : {fo_100k}")
print()

# ---- event bucketing: monthly / weekly footprint per node ----
def iso_week(d):
    # YYYY-Www without datetime parsing gymnastics: use ISO date string
    # cheap approx: year + week-of-year via ordinal is overkill; month is the
    # primary granularity. Weekly modeled as year+2-digit-week bucket key.
    return str(d)[:4] + "-W" + str((int(str(d)[5:7]) - 1) * 4 + min(4, (int(str(d)[8:10]) - 1)//7 + 1))

def build_buckets(evs, gran):
    """Compact per-node time buckets keyed by period; each cell holds the
    counts every time/plan/set-aside/action-type predicate needs."""
    cells = {}
    for e in evs:
        d = e.get("action_date")
        if not d:
            continue
        key = str(d)[:7] if gran == "month" else iso_week(d)
        c = cells.setdefault(key, {"n": 0, "obl": 0.0,
                                   "at": defaultdict(int), "plan": defaultdict(int),
                                   "sa": defaultdict(int), "first": 0, "needs": 0})
        c["n"] += 1
        c["obl"] += (e.get("obligation_delta") or 0)
        c["at"][e.get("action_type_code")] += 1
        c["plan"][e.get("subcontracting_plan")] += 1
        c["sa"][e.get("type_of_set_aside_code")] += 1
        if e.get("is_first_action"):
            c["first"] += 1
        if e.get("is_first_action") and not e.get("has_disclosed_subs"):
            c["needs"] += 1
    # freeze defaultdicts for json sizing
    return {k: {**v, "at": dict(v["at"]), "plan": dict(v["plan"]), "sa": dict(v["sa"])}
            for k, v in cells.items()}

# precompute buckets + summaries once
month_buckets = [build_buckets(nd["demand_events"]["events"], "month") for nd in nodes]
week_buckets = [build_buckets(nd["demand_events"]["events"], "week") for nd in nodes]
bucket_month_mb = sum(len(json.dumps(b, default=str)) for b in month_buckets) / 1e6
bucket_week_mb = sum(len(json.dumps(b, default=str)) for b in week_buckets) / 1e6
raw_events_mb = sum(len(json.dumps(nd["demand_events"]["events"], default=str)) for nd in nodes) / 1e6

print("=== event-grain representations (summed across all nodes) ===")
print(f"raw events (current)   : {raw_events_mb:7.2f} MB")
print(f"monthly buckets        : {bucket_month_mb:7.2f} MB   ({raw_events_mb/bucket_month_mb:.0f}x smaller)")
print(f"weekly buckets         : {bucket_week_mb:7.2f} MB   ({raw_events_mb/bucket_week_mb:.0f}x smaller)")
print()

# ---- node representations ----
STUB_FIELDS = ["uei", "name", "disclosed_sub_buyer", "matched_farmout_60mo",
               "matched_prime_obl_60mo", "n_matched_combos", "matched_via", "gate_facts"]

def stub_node(nd, matched_via_cap=5):
    s = {k: nd.get(k) for k in STUB_FIELDS if k != "matched_via"}
    mv = nd.get("matched_via") or []
    s["matched_via"] = mv[:matched_via_cap]
    s["matched_via_truncated"] = len(mv) > matched_via_cap
    return s

def material_node(nd, i, gran, port_cap):
    """Full hydration MINUS raw events (which go to the sidecar), PLUS buckets."""
    m = {k: v for k, v in nd.items() if k != "demand_events"}
    port = m.get("win_portfolio") or []
    m["win_portfolio"] = port[:port_cap]
    m["win_portfolio_truncated"] = len(port) > port_cap
    m["event_buckets"] = (month_buckets[i] if gran == "month" else week_buckets[i])
    m["demand_events_summary"] = nd["demand_events"].get("summary")
    return m

def is_material(nd, defn):
    fo = nd.get("matched_farmout_60mo") or 0
    if defn == "disc_or_fo>0":
        return bool(nd.get("disclosed_sub_buyer")) or fo > 0
    if defn == "disc_or_fo>=100k":
        return bool(nd.get("disclosed_sub_buyer")) or fo >= 100_000
    if defn == "disc_only":
        return bool(nd.get("disclosed_sub_buyer"))
    return True

def hot_blob_mb(defn, gran, port_cap):
    mat = 0
    total = 0
    for i, nd in enumerate(nodes):
        if is_material(nd, defn):
            total += len(json.dumps(material_node(nd, i, gran, port_cap), default=str))
            mat += 1
        else:
            total += len(json.dumps(stub_node(nd), default=str))
    # + target_analytics + meta (small, ~0.03 MB)
    total += len(json.dumps(blob["target_analytics"], default=str))
    return total / 1e6, mat

print("=== HOT-BLOB size under tiering x bucketing x portfolio cap ===")
print(f"{'materiality':<18}{'gran':<7}{'port':<6}{'material#':<11}{'hot MB':<9}{'~load ms':<10}{'verdict'}")
configs = []
for defn in ["all", "disc_or_fo>0", "disc_or_fo>=100k", "disc_only"]:
    for gran in ["month"]:
        for port_cap in [50, 10]:
            hot, mat = hot_blob_mb(defn, gran, port_cap)
            load = hot * MS_PER_MB
            verdict = "OK single-digit" if hot < 10 else ("close" if hot < 15 else "over")
            configs.append((defn, gran, port_cap, mat, hot, load, verdict))
            print(f"{defn:<18}{gran:<7}{port_cap:<6}{mat:<11}{hot:<9.2f}{load:<10.1f}{verdict}")

# weekly variant on the best-tier defn
for defn in ["disc_or_fo>=100k"]:
    hot, mat = hot_blob_mb(defn, "week", 10)
    print(f"{defn:<18}{'week':<7}{10:<6}{mat:<11}{hot:<9.2f}{hot*MS_PER_MB:<10.1f}{'(weekly)'}")

print()
# ---- sidecar size (raw events that move out, ALL nodes drillable) ----
sidecar_rows = sum(len(nd['demand_events']['events']) for nd in nodes)
print(f"=== events sidecar (gtm_sub_universe_events) ===")
print(f"raw event rows moved out : {sidecar_rows}  (~{raw_events_mb:.1f} MB JSON; point-lookup by (target_uei,node_uei), size does NOT affect hot load)")
print("TRIM EXPERIMENTS DONE")
