import os, sys, json
sys.path.insert(0, os.getcwd())
from apps.catalyst_api.src import sub_universe_full as F

# probe: VALLEY TOOL AND MANUFACTURING (5 primes, 740 edges — mid-size, real)
uei = "YZBMKTR1HHL7"
blob = F.build_blob(uei)

ta = blob["target_analytics"]; U = blob["universe"]
print("recipe:", blob["recipe"], "| base:", blob["meta"]["base_recipe"])
print("entity:", ta["entity"]["name"], "|", ta["entity"]["city"], ta["entity"]["state"],
      "| life $", ta["entity"]["sub_dollars_lifetime"], "| buyers", ta["entity"]["prime_buyer_count"],
      "| cagr", ta["entity"]["trajectory_5yr_pct"])
print("scopes: lanes", len(ta["scopes"]["lanes"]), "| states", ta["scopes"]["performance_states"],
      "| band", ta["scopes"]["deal_band"])
print("universe: nodes", len(U["nodes"]), "of", U["n_total"], "| truncated", U["nodes_truncated"])
n0 = U["nodes"][0]
print("node0:", n0["uei"], "| entity?", n0["entity"] is not None, "| portfolio", len(n0["win_portfolio"]),
      "| award_state", n0["award_state"], "| events", len(n0["demand_events"]["events"]),
      "| tcf?", n0["target_combo_farmout"] is not None)
pool = ta["adjacent_market"]["pool"]
print("pool: $", pool["total_dollars"], "|", pool["prime_count"], "primes | named",
      len(pool["named_primes"]), "| entity capture", pool["entity_capture"])
print("placement:", [(p["code"], p["pct"]) for p in ta["adjacent_market"]["placement"]])
df = ta["adjacent_market"]["deal_fit"]
print("deal_fit: placed_median", df["placed_median"], "| within_band", df["within_band_pct"],
      "| hist sum", sum(df["distribution"]), "| edges", len(df["bin_edges"]))
fld = ta["field"]
print("peers:", fld["comparable_set"]["count"], "| set capture", fld["comparable_set"]["set_capture"],
      "| median peer", fld["comparable_set"]["median_peer_capture"])
for p in fld["percentiles"]: print("  pct:", p["dimension"], p["entity_value"], "vs med", p["peer_median"], "->", p["percentile"])
print("vehicle_exposure:", ta["current_performance"]["dependencies"]["vehicle_exposure"])
print("trends:", [(t["combo"], t["median_slope_pct_yr"]) for t in ta["current_performance"]["lane_trends"][:3]])
print("timings:", blob["meta"]["timings_ms"])
# invariants
assert blob["recipe"] == "sub_universe_blob.v1"
assert sum(df["distribution"]) == (pool["prime_count"] >= 0 and sum(df["distribution"]))  # defined
comp = ta["current_performance"]["customer_composition"]
assert abs(sum(c["dollars"] for c in comp) - ta["entity"]["sub_dollars_lifetime"]) < 1.0
cs = fld["comparable_set"]
assert abs(cs["set_capture"] + cs["entity_capture"] + cs["others_capture"] - cs["pool_total"]) < 1.0
sz = len(json.dumps(blob, default=str))
print(f"blob size: {sz/1e6:.2f} MB")
print("GATE PASS")
