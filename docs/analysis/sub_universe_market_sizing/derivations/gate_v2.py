"""Phase 3b v2 BUILD gate — validates the single-payload two-tier blob (no
sidecar; drilldown reads marts). Ships iff: hot blob single-digit MB, material
nodes carry monthly buckets (no raw events/portfolio), stubs are lean with a base
demand summary, bucket counts RECONCILE against a direct demand-events mart scan
(proving the mart drilldown reconstructs), and target_analytics invariants hold."""
import os, sys, json, time
sys.path.insert(0, os.getcwd())
import lance
from apps.catalyst_api.src import sub_universe_full as F
from apps.catalyst_api.src import config

UEI = "YZBMKTR1HHL7"
t0 = time.perf_counter()
blob = F.build_blob(UEI)
build_s = time.perf_counter() - t0

U = blob["universe"]; ta = blob["target_analytics"]
hot_mb = len(json.dumps(blob, default=str)) / 1e6
nodes = U["nodes"]
material = [n for n in nodes if n.get("tier") == "material"]
stubs = [n for n in nodes if n.get("tier") == "stub"]

print("recipe:", blob["recipe"], "| base:", blob["meta"]["base_recipe"])
print(f"HOT blob: {hot_mb:.2f} MB | est load {hot_mb*398.5/130:.0f} ms parse | build {build_s/60:.1f} min")
print(f"universe: {len(nodes)} nodes = {len(material)} material + {len(stubs)} stub "
      f"| n_material meta={U['n_material']} | of {U['n_total']} | truncated {U['nodes_truncated']}")
print("timings:", blob["meta"]["timings_ms"])
print("tiering:", json.dumps(blob["meta"]["tiering"]))

assert blob["recipe"] == "sub_universe_blob.v2"
assert hot_mb < 10.0, f"HOT blob {hot_mb:.2f} MB over single-digit budget"
assert material and stubs, "expected both tiers"

m0 = material[0]
assert m0["demand_events"]["grain"] == "month" and "buckets" in m0["demand_events"]
assert "events" not in m0["demand_events"], "raw events leaked into hot material node"
assert "win_portfolio" not in m0, "win_portfolio leaked into hot"
assert len(m0.get("matched_via", [])) <= F.MATCHED_VIA_HOT_CAP
print(f"material[0]: {m0['uei']} | entity? {m0['entity'] is not None} "
      f"| bucket months {len(m0['demand_events']['buckets'])} | matched_via {len(m0['matched_via'])}")

s0 = stubs[0]
assert "entity" not in s0 and "demand_events" not in s0, "stub not lean"
assert "gate_facts" in s0 and "demand_summary" in s0, "stub missing membership/summary"
print(f"stub[0]: {s0['uei']} | keys {sorted(s0.keys())} "
      f"| summary keys {sorted((s0['demand_summary'] or {}).keys())}")

# --- bucket reconciliation vs a DIRECT demand-events mart scan (proves the mart
#     drilldown reconstructs the same restricted grain the buckets summarize) ---
opt = config.r2_storage_options()
dm = lance.dataset(config.GTM_PRIME_DEMAND_EVENTS_URI, storage_options=opt)
checked = 0
for m in material[:25]:
    combos = {tuple(k.split("x", 1)) for k in (m.get("gate_facts") or {})} | \
             {(mm["naics_code"], mm["psc_code"]) for mm in (m.get("matched_via") or [])}
    raw = dm.scanner(columns=["naics_code", "psc_code", "action_date"],
                     filter=f"uei = '{m['uei']}'").to_table().to_pylist()
    raw_r = [e for e in raw if (e["naics_code"], e["psc_code"]) in combos and e["action_date"]]
    bn = sum(c["n"] for c in m["demand_events"]["buckets"].values())
    # buckets restrict to matched_via (capped to 5) UNION gate_facts; the direct
    # scan uses the SAME union from the hot node, so counts match exactly.
    assert bn == len(raw_r), f"{m['uei']}: bucket_n {bn} != mart {len(raw_r)}"
    checked += 1
print(f"bucket reconciliation vs mart: {checked} material nodes, exact")

# --- target_analytics invariants (unchanged) ---
comp = ta["current_performance"]["customer_composition"]
assert abs(sum(c["dollars"] for c in comp) - ta["entity"]["sub_dollars_lifetime"]) < 1.0
cs = ta["field"]["comparable_set"]
assert abs(cs["set_capture"] + cs["entity_capture"] + cs["others_capture"] - cs["pool_total"]) < 1.0
print("target_analytics invariants: OK")

# --- serve: a bucketed time predicate over material nodes ---
from datetime import date, timedelta
recent = {(date.today() - timedelta(days=d)).isoformat()[:7] for d in range(0, 95, 15)}
t = time.perf_counter()
hits = [m["uei"] for m in material
        if any(mo in m["demand_events"]["buckets"] and m["demand_events"]["buckets"][mo]["n"] > 0
               for mo in recent)]
print(f"serve: bucketed recent-activity filter over {len(material)} material: "
      f"{(time.perf_counter()-t)*1000:.2f} ms ({len(hits)} hits)")

print("GATE PASS")
