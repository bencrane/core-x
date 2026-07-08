"""Trim search v2 — per-field breakdown of a material node + progressive trims
to land the HOT blob under single-digit MB. Material = disclosed sub-buyers
(933); undisclosed (4067) = stubs. Events -> monthly buckets (hot) + raw sidecar."""
import json, pickle
from collections import defaultdict

SP = "/private/tmp/claude-501/-Users-benjamincrane-core-x--claude-worktrees-objective-wozniak-5ded4e/d5ac7f55-4a32-4c98-a4b8-58ece6beb5a5/scratchpad"
MS_PER_MB = 398.5 / 130.0

def jb(x):
    return len(json.dumps(x, default=str))

with open(f"{SP}/blob_YZBMKTR1HHL7.pkl", "rb") as f:
    blob = pickle.load(f)
nodes = blob["universe"]["nodes"]

def build_month_buckets(evs):
    cells = {}
    for e in evs:
        d = e.get("action_date")
        if not d:
            continue
        k = str(d)[:7]
        c = cells.setdefault(k, {"n": 0, "obl": 0.0, "at": defaultdict(int),
                                 "plan": defaultdict(int), "sa": defaultdict(int),
                                 "first": 0, "needs": 0})
        c["n"] += 1; c["obl"] += (e.get("obligation_delta") or 0)
        c["at"][e.get("action_type_code")] += 1
        c["plan"][e.get("subcontracting_plan")] += 1
        c["sa"][e.get("type_of_set_aside_code")] += 1
        if e.get("is_first_action"):
            c["first"] += 1
            if not e.get("has_disclosed_subs"):
                c["needs"] += 1
    return {k: {**v, "at": dict(v["at"]), "plan": dict(v["plan"]), "sa": dict(v["sa"])}
            for k, v in cells.items()}

# lean monthly bucket: drop per-plan/per-sa cross-tabs, keep action-type + flags
def build_lean_buckets(evs):
    cells = {}
    for e in evs:
        d = e.get("action_date")
        if not d:
            continue
        k = str(d)[:7]
        c = cells.setdefault(k, {"n": 0, "obl": 0.0, "at": defaultdict(int), "first": 0, "needs": 0})
        c["n"] += 1; c["obl"] += (e.get("obligation_delta") or 0)
        c["at"][e.get("action_type_code")] += 1
        if e.get("is_first_action"):
            c["first"] += 1
            if not e.get("has_disclosed_subs"):
                c["needs"] += 1
    return {k: {**v, "at": dict(v["at"])} for k, v in cells.items()}

material = [nd for nd in nodes if nd.get("disclosed_sub_buyer")]
stubs = [nd for nd in nodes if not nd.get("disclosed_sub_buyer")]
print(f"material (disclosed) = {len(material)}   stubs (undisclosed) = {len(stubs)}")
print()

# ---- per-field MB across the 933 material nodes ----
FIELDS = ["matched_via", "gate_facts", "entity", "win_portfolio", "teaming",
          "vehicles", "target_combo_farmout", "award_state"]
print("=== per-field footprint across material nodes (MB) ===")
for fld in FIELDS:
    tot = sum(jb(nd.get(fld)) for nd in material) / 1e6
    print(f"  {fld:<22}{tot:6.2f}")
buck_full = sum(jb(build_month_buckets(nd["demand_events"]["events"])) for nd in material) / 1e6
buck_lean = sum(jb(build_lean_buckets(nd["demand_events"]["events"])) for nd in material) / 1e6
print(f"  {'event_buckets(full)':<22}{buck_full:6.2f}")
print(f"  {'event_buckets(lean)':<22}{buck_lean:6.2f}")
# entity sub-fields
ent_codes = sum(jb([nd.get('entity',{}).get('naics_codes'), nd.get('entity',{}).get('psc_codes')]) for nd in material if nd.get('entity')) / 1e6
print(f"  (entity naics_codes+psc_codes arrays alone: {ent_codes:.2f} MB)")
print()

# ---- progressive hot-blob configs ----
def hot_mb(matched_via_cap, port_in_hot, port_cap, bucket_kind, slim_entity, stub_via_cap):
    total = jb(blob["target_analytics"])
    for nd in material:
        m = {"uei": nd["uei"], "name": nd["name"],
             "disclosed_sub_buyer": nd["disclosed_sub_buyer"],
             "matched_farmout_60mo": nd["matched_farmout_60mo"],
             "matched_prime_obl_60mo": nd["matched_prime_obl_60mo"],
             "n_matched_combos": nd["n_matched_combos"],
             "latitude": nd["latitude"], "longitude": nd["longitude"],
             "gate_facts": nd["gate_facts"], "teaming": nd["teaming"],
             "vehicles": nd["vehicles"], "target_combo_farmout": nd["target_combo_farmout"],
             "award_state": nd["award_state"], "pop": None}
        mv = nd.get("matched_via") or []
        m["matched_via"] = mv[:matched_via_cap]
        m["matched_via_truncated"] = len(mv) > matched_via_cap
        ent = nd.get("entity")
        if ent and slim_entity:
            m["entity"] = {k: ent.get(k) for k in ("cage", "name", "sam_is_active",
                           "in_dsbs", "primary_naics", "physical_city", "physical_state", "physical_zip")}
        else:
            m["entity"] = ent
        if port_in_hot:
            port = nd.get("win_portfolio") or []
            m["win_portfolio"] = port[:port_cap]
            m["win_portfolio_truncated"] = len(port) > port_cap
        m["event_buckets"] = (build_lean_buckets if bucket_kind == "lean" else build_month_buckets)(nd["demand_events"]["events"])
        total += jb(m)
    for nd in stubs:
        mv = nd.get("matched_via") or []
        s = {"uei": nd["uei"], "name": nd["name"], "disclosed_sub_buyer": False,
             "matched_farmout_60mo": nd["matched_farmout_60mo"],
             "matched_prime_obl_60mo": nd["matched_prime_obl_60mo"],
             "n_matched_combos": nd["n_matched_combos"],
             "matched_via": mv[:stub_via_cap], "matched_via_truncated": len(mv) > stub_via_cap,
             "gate_facts": nd["gate_facts"]}
        total += jb(s)
    return total / 1e6

print("=== HOT-BLOB configs (material=disclosed 933, stubs=4067) ===")
print(f"{'via_cap':<8}{'port':<14}{'bucket':<8}{'slim_ent':<10}{'stub_via':<10}{'hot MB':<9}{'~load ms':<10}{'verdict'}")
trials = [
    (25, "hot@10", "full", False, 5),
    (10, "hot@10", "full", True,  3),
    (10, "sidecar", "full", True,  3),
    (10, "sidecar", "lean", True,  3),
    (5,  "sidecar", "lean", True,  2),
    (5,  "sidecar", "lean", True,  0),
]
for via_cap, port_mode, bucket, slim, stub_via in trials:
    port_in_hot = (port_mode != "sidecar")
    hot = hot_mb(via_cap, port_in_hot, 10, bucket, slim, stub_via)
    load = hot * MS_PER_MB
    verdict = "OK <10MB" if hot < 10 else ("close" if hot < 12 else "over")
    print(f"{via_cap:<8}{port_mode:<14}{bucket:<8}{str(slim):<10}{stub_via:<10}{hot:<9.2f}{load:<10.1f}{verdict}")
print("DONE")
