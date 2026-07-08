"""Build the probe blob ONCE, persist it, and print an exact byte breakdown by
section — so every subsequent trim experiment runs offline against the saved
payload (no 24-min rebuild). Byte proxy = len(json.dumps(x, default=str)),
identical to the gate's total-size measurement."""
import os, sys, json, pickle
sys.path.insert(0, os.getcwd())
from apps.catalyst_api.src import sub_universe_full as F

UEI = "YZBMKTR1HHL7"
OUT = "/private/tmp/claude-501/-Users-benjamincrane-core-x--claude-worktrees-objective-wozniak-5ded4e/d5ac7f55-4a32-4c98-a4b8-58ece6beb5a5/scratchpad"

def mb(x):
    return len(json.dumps(x, default=str)) / 1e6

blob = F.build_blob(UEI)

# persist raw payload for offline experiments (both pickle + json)
with open(f"{OUT}/blob_{UEI}.pkl", "wb") as f:
    pickle.dump(blob, f)
with open(f"{OUT}/blob_{UEI}.json", "w") as f:
    json.dump(blob, f, default=str)

nodes = blob["universe"]["nodes"]
n = len(nodes)
tot = mb(blob)

# section-level
sec = {
    "universe.nodes": mb(nodes),
    "target_analytics": mb(blob["target_analytics"]),
    "meta+top": tot - mb(nodes) - mb(blob["target_analytics"]),
}

# within-node breakdown (summed across all nodes)
def field_mb(key):
    return sum(len(json.dumps(nd.get(key), default=str)) for nd in nodes) / 1e6

node_events = field_mb("demand_events")
node_portfolio = field_mb("win_portfolio")
node_entity = field_mb("entity")
node_tcf = field_mb("target_combo_farmout")
node_award = field_mb("award_state")
node_rest = sec["universe.nodes"] - node_events - node_portfolio - node_entity - node_tcf - node_award

# event-count distribution across nodes
ev_counts = sorted((len(nd["demand_events"]["events"]) for nd in nodes), reverse=True)
port_counts = sorted((len(nd["win_portfolio"]) for nd in nodes), reverse=True)
total_events = sum(ev_counts)
at_cap = sum(1 for c in ev_counts if c >= 500)

print(f"=== BLOB SIZE BREAKDOWN — {UEI} ({n} nodes) ===")
print(f"TOTAL: {tot:.2f} MB")
print(f"  universe.nodes      : {sec['universe.nodes']:.2f} MB")
print(f"  target_analytics    : {sec['target_analytics']:.2f} MB")
print(f"  meta + top-level    : {sec['meta+top']:.2f} MB")
print(f"--- within universe.nodes (summed across {n} nodes) ---")
print(f"  demand_events       : {node_events:.2f} MB   ({total_events} event rows total, {at_cap} nodes at 500-cap)")
print(f"  win_portfolio       : {node_portfolio:.2f} MB")
print(f"  entity              : {node_entity:.2f} MB")
print(f"  target_combo_farmout: {node_tcf:.2f} MB")
print(f"  award_state         : {node_award:.2f} MB")
print(f"  scalar rest         : {node_rest:.2f} MB")
print(f"--- event-count distribution across nodes ---")
print(f"  events/node: max {ev_counts[0]}, p50 {ev_counts[n//2]}, p90 {ev_counts[int(n*0.1)]}, mean {total_events/n:.1f}")
print(f"  portfolio/node: max {port_counts[0]}, p50 {port_counts[n//2]}, mean {sum(port_counts)/n:.1f}")
print(f"--- projections ---")
print(f"  blob WITHOUT node demand_events : {tot - node_events:.2f} MB")
print(f"  blob WITHOUT events + portfolio : {tot - node_events - node_portfolio:.2f} MB")
print(f"saved: blob_{UEI}.pkl / .json")
print("MEASURE DONE")
