"""Unit tests for the sub-universe recipe v3 — pure, no R2 / network.

Pins (1) the fail-closed request contract ({uei, limit?} ONLY — unknown keys →
MapCompileError), (2) the universe definition: nodes are the FULL lookalike
winners (prime_obl_60mo > 0 in ≥1 anchor-portfolio combo), target + anchors
excluded, farm-out lanes LEFT-joined, (3) NULL SEMANTICS: undisclosed winners
carry matched_farmout_60mo null (not 0) + null per-combo farm-out fields with
candidate_prime_obl_60mo always present; nodes without pair rows carry null
teaming fields, (4) display order: disclosed sub-buyers by farm-out $ first,
then undisclosed winners by prime obl (disclosed in meta.display_order),
(5) gate_facts over the FULL matched set + matched_via_truncated, (6) NO
SCORING, (7) target defaults: mvs_n rides alongside, < 5 combo-bearing edges →
mvs_usd null + mvs_reason (the fixture target has 2 edges), (8) empty-anchor
targets are a valid 200 with meta.reason, (9) the boot-cache lifecycle, and
(10) v3 Definition C facts: target_combo_farmout keyed to the TARGET's own
demonstrated combos — independent of the anchor-portfolio matched set, null
(never an empty block) when no lane discloses."""

from __future__ import annotations

import pytest

from apps.catalyst_api.src import lance_store, sub_universe_store as S

TARGET = "UEITARGET001"
ANCHOR = "UEIANCHOR001"
BUYER = "UEIBUYER0001"    # disclosed sub-buyer winner
BUYER2 = "UEIBUYER0002"   # wins only OUTSIDE the anchor portfolio — excluded
BUYER3 = "UEIBUYER0003"   # undisclosed winner (no farm-out, no pair rows)

# gtm_prime_sub_pairs per-prime stat inputs (prime_uei, prime_name, edge_count_5y)
PAIRS = [
    {"prime_uei": BUYER, "prime_name": "BUYER ONE", "edge_count_5y": 5},
    {"prime_uei": BUYER, "prime_name": "BUYER ONE", "edge_count_5y": 1},
    # a lifetime-only pair (0 edges in 5y window) — not a 5y partner
    {"prime_uei": BUYER, "prime_name": "BUYER ONE", "edge_count_5y": 0},
    {"prime_uei": BUYER2, "prime_name": "BUYER TWO", "edge_count_5y": 1},
    # BUYER3 has NO pair rows — teaming must be null, not zero
]

FARMOUT = [
    # BUYER buys subs in the anchor's combo — disclosed
    {"uei": BUYER, "naics_code": "541330", "psc_code": "R425",
     "naics_title": "Engineering", "psc_title": "Eng Support",
     "farmout_amt_60mo": 2_000_000.0, "farmout_amt_lifetime": 3_000_000.0,
     "median_chunk_60mo": 400_000.0, "median_chunk_lifetime": 350_000.0,
     "p75_chunk_60mo": 800_000.0, "n_subawards_lifetime": 6,
     "n_distinct_subs_60mo": 3, "last_action_date": "2025-05-01"},
    # BUYER2 in a combo OUTSIDE the anchor portfolio — irrelevant
    {"uei": BUYER2, "naics_code": "336611", "psc_code": "1905",
     "naics_title": "Ships", "psc_title": "Combat Ships",
     "farmout_amt_60mo": 9_000_000.0, "farmout_amt_lifetime": 9_000_000.0,
     "median_chunk_60mo": 1_000_000.0, "median_chunk_lifetime": 1_000_000.0,
     "p75_chunk_60mo": 2_000_000.0, "n_subawards_lifetime": 2,
     "n_distinct_subs_60mo": 2, "last_action_date": "2025-02-01"},
    # the ANCHOR itself also farms out in its combo — must be excluded as a node
    {"uei": ANCHOR, "naics_code": "541330", "psc_code": "R425",
     "naics_title": "Engineering", "psc_title": "Eng Support",
     "farmout_amt_60mo": 5_000_000.0, "farmout_amt_lifetime": 5_000_000.0,
     "median_chunk_60mo": 250_000.0, "median_chunk_lifetime": 250_000.0,
     "p75_chunk_60mo": 500_000.0, "n_subawards_lifetime": 10,
     "n_distinct_subs_60mo": 5, "last_action_date": "2025-06-01"},
]

VEHICLES = [
    {"uei": BUYER, "parent_piid": "VEHICLE1", "farmout_amt_60mo": 1_500_000.0,
     "farmout_amt_lifetime": 1_500_000.0, "n_subawards_lifetime": 4,
     "last_action_date": "2025-05-01"},
]

# the winners inverted-index source (gtm_prime_combo_lanes, prime_obl_60mo > 0).
# BUYER3 out-wins BUYER on obl but is UNDISCLOSED — disclosed-first ordering
# must still put BUYER ahead of it.
WINNER_LANES = [
    {"uei": BUYER, "naics_code": "541330", "psc_code": "R425", "prime_obl_60mo": 3_000_000.0},
    {"uei": BUYER3, "naics_code": "541330", "psc_code": "R425", "prime_obl_60mo": 7_000_000.0},
    {"uei": ANCHOR, "naics_code": "541330", "psc_code": "R425", "prime_obl_60mo": 8_000_000.0},
    {"uei": TARGET, "naics_code": "541330", "psc_code": "R425", "prime_obl_60mo": 100_000.0},
    {"uei": BUYER2, "naics_code": "336611", "psc_code": "1905", "prime_obl_60mo": 4_000_000.0},
]

PRIME_LANES = {
    ANCHOR: [{"uei": ANCHOR, "naics_code": "541330", "psc_code": "R425",
              "prime_obl_60mo": 8_000_000.0, "last_action_date": "2025-06-01"}],
    TARGET: [{"uei": TARGET, "naics_code": "541330", "psc_code": "R425",
              "prime_obl_60mo": 100_000.0, "last_action_date": "2025-03-01"}],
}

TARGET_EDGES = [
    {"subaward_amount": 300_000.0, "subaward_action_date": "2024-05-01",
     "prime_awardee_uei": ANCHOR, "prime_awardee_name": "ANCHOR PRIME", "prime_award_naics_code": "541330",
     "prime_award_product_or_service_code": "R425",
     "prime_award_parent_piid": "VEHICLE9",
     "subaward_primary_place_of_performance_state_code": "AL"},
    {"subaward_amount": 100_000.0, "subaward_action_date": "2023-01-15",
     "prime_awardee_uei": ANCHOR, "prime_awardee_name": "ANCHOR PRIME", "prime_award_naics_code": "541330",
     "prime_award_product_or_service_code": "R425",
     "prime_award_parent_piid": None,
     "subaward_primary_place_of_performance_state_code": "DC"},
]

GEO = [{"uei": BUYER, "latitude": 34.7, "longitude": -86.5, "geo_precision": "address"}]

DEMAND = [
    # flagship recipe row: first action, type C, plan required, no subs yet
    {"uei": BUYER, "award_key": "AWD1", "action_date": "2026-06-01",
     "obligation_delta": 900_000.0, "naics_code": "541330", "psc_code": "R425",
     "action_type_code": None, "action_type_description": None,
     "award_type_code": "C", "subcontracting_plan": "D",
     "subcontracting_plan_desc": "PLAN REQUIRED - INCENTIVE INCLUDED",
     "is_first_action": True, "has_disclosed_subs": False},
    # Y mod + a termination on other awards
    {"uei": BUYER, "award_key": "AWD2", "action_date": "2026-05-01",
     "obligation_delta": 0.0, "naics_code": "541330", "psc_code": "R425",
     "action_type_code": "Y", "action_type_description": "ADD SUBCONTRACT PLAN",
     "award_type_code": "C", "subcontracting_plan": "B",
     "subcontracting_plan_desc": "PLAN NOT REQUIRED",
     "is_first_action": False, "has_disclosed_subs": True},
    {"uei": BUYER, "award_key": "AWD3", "action_date": "2026-04-01",
     "obligation_delta": -5_000.0, "naics_code": "541330", "psc_code": "R425",
     "action_type_code": "F", "action_type_description": "TERMINATE FOR CONVENIENCE (COMPLETE OR PARTIAL)",
     "award_type_code": "C", "subcontracting_plan": None,
     "subcontracting_plan_desc": None,
     "is_first_action": False, "has_disclosed_subs": True},
]


@pytest.fixture(autouse=True)
def _patch(monkeypatch):
    S.reset_caches_for_tests()
    monkeypatch.setattr(S, "_scan_pairs", lambda: [dict(r) for r in PAIRS])
    monkeypatch.setattr(S, "_scan_farmout", lambda: [dict(r) for r in FARMOUT])
    monkeypatch.setattr(S, "_scan_vehicles", lambda: [dict(r) for r in VEHICLES])
    monkeypatch.setattr(S, "_scan_winner_lanes", lambda: [dict(r) for r in WINNER_LANES])
    monkeypatch.setattr(S, "_scan_prime_lanes",
                        lambda ueis: [dict(r) for u in ueis for r in PRIME_LANES.get(u, [])])
    monkeypatch.setattr(S, "_scan_target_edges",
                        lambda uei: [dict(r) for r in TARGET_EDGES] if uei == TARGET else [])
    monkeypatch.setattr(S, "_scan_geo", lambda ueis: [dict(g) for g in GEO if g["uei"] in ueis])
    monkeypatch.setattr(S, "_scan_demand_events",
                        lambda ueis: [dict(e) for e in DEMAND if e["uei"] in ueis])
    yield
    S.reset_caches_for_tests()


def test_unknown_keys_fail_closed():
    with pytest.raises(lance_store.MapCompileError):
        S.validate_request({"uei": TARGET, "mode": "combos"})
    with pytest.raises(lance_store.MapCompileError):
        S.validate_request({"uei": "short"})
    with pytest.raises(lance_store.MapCompileError):
        S.validate_request({"uei": TARGET, "limit": 0})


def test_universe_is_full_winner_set_disclosed_first():
    out = S.execute_sub_universe({"uei": TARGET})
    ueis = [n["uei"] for n in out["data"]]
    # BUYER3 (7M obl) out-wins BUYER (3M) but is undisclosed — disclosed first.
    # BUYER2 (foreign combo) + ANCHOR + TARGET excluded.
    assert ueis == [BUYER, BUYER3]
    assert out["meta"]["total"] == 2
    assert "disclosed" in out["meta"]["display_order"]
    node = out["data"][0]
    assert node["name"] == "BUYER ONE"
    assert node["latitude"] == 34.7
    assert node["disclosed_sub_buyer"] is True
    assert node["matched_farmout_60mo"] == 2_000_000.0
    assert node["matched_prime_obl_60mo"] == 3_000_000.0
    mv = node["matched_via"][0]
    assert mv["combo"] == "541330xR425"
    assert mv["candidate_prime_obl_60mo"] == 3_000_000.0
    assert mv["median_chunk_60mo"] == 400_000.0
    assert mv["anchor_uei"] == ANCHOR and mv["anchor_obl_60mo"] == 8_000_000.0
    assert mv["prime_backed"] is True           # target primes 541330xR425 itself
    assert node["matched_via_truncated"] is False
    assert node["teaming"] == {"n_sub_partners_5y": 2,   # lifetime-only pair not counted
                               "deepest_repeat_edges_5y": 5,
                               "n_partners_ge_3_edges": 1}
    assert node["vehicles"][0]["parent_piid"] == "VEHICLE1"
    # no scoring anywhere
    assert "score" not in node and "rank" not in node
    de = node["demand_events"]
    assert de["n_events_24mo"] == 3
    assert de["n_plan_added_Y"] == 1 and de["n_terminations_EFX"] == 1
    assert de["needs_subs_now_total"] == 1
    hot = de["needs_subs_now"][0]
    assert hot["award_key"] == "AWD1" and hot["combo"] == "541330xR425"
    assert hot["subcontracting_plan"] == "D" and hot["obligation"] == 900_000.0


def test_undisclosed_winner_null_semantics():
    out = S.execute_sub_universe({"uei": TARGET})
    node = out["data"][1]
    assert node["uei"] == BUYER3
    assert node["disclosed_sub_buyer"] is False
    assert node["matched_farmout_60mo"] is None          # unknown ≠ zero
    assert node["matched_prime_obl_60mo"] == 7_000_000.0
    assert node["name"] is None                          # no pair rows
    # all three teaming fields null — absent pair history is an absent fact
    assert node["teaming"] == {"n_sub_partners_5y": None,
                               "deepest_repeat_edges_5y": None,
                               "n_partners_ge_3_edges": None}
    mv = node["matched_via"][0]
    assert mv["candidate_prime_obl_60mo"] == 7_000_000.0  # always present
    assert mv["farmout_amt_60mo"] is None                 # farm-out facts null
    assert mv["median_chunk_60mo"] is None
    assert mv["n_subawards_lifetime"] is None
    assert mv["anchor_uei"] == ANCHOR


def test_gate_facts_cover_full_matched_set():
    out = S.execute_sub_universe({"uei": TARGET})
    disclosed, undisclosed = out["data"][0], out["data"][1]
    assert disclosed["gate_facts"] == {"541330xR425": {"m": 400_000.0, "pb": True}}
    # undisclosed: median null (no farm-out lane), pb still stamped
    assert undisclosed["gate_facts"] == {"541330xR425": {"m": None, "pb": True}}
    assert len(disclosed["gate_facts"]) == disclosed["n_matched_combos"]


def test_target_block_and_low_n_defaults():
    out = S.execute_sub_universe({"uei": TARGET})
    t = out["target"]
    assert t["anchors"][0]["prime_uei"] == ANCHOR
    combo = t["demonstrated_combos"][0]
    assert combo["combo"] == "541330xR425" and combo["n_edges"] == 2
    assert combo["median_chunk_usd"] == 200_000.0
    d = t["defaults"]
    assert d["mvs_n"] == 2
    assert d["mvs_usd"] is None                          # 2 edges < 5 — no default floor
    assert d["mvs_reason"] == "insufficient history (n=2) to set a default floor"
    assert d["pop_states"] == ["AL", "DC"]               # $-ordered
    assert t["vehicles"][0]["parent_piid"] == "VEHICLE9"
    assert t["prime_combos"][0]["combo"] == "541330xR425"


def test_no_anchor_target_is_valid_empty():
    out = S.execute_sub_universe({"uei": "UEINOANCHOR1"})
    assert out["data"] == []
    assert out["meta"]["n_anchors"] == 0
    assert "no FSRS subaward edges" in out["meta"]["reason"]


def test_cache_cold_then_warm():
    a = S.execute_sub_universe({"uei": TARGET})
    b = S.execute_sub_universe({"uei": TARGET})
    assert a["meta"]["cache_state"] == "cold"
    assert b["meta"]["cache_state"] == "warm"
    assert a["meta"]["winners_index_combo_entries"] == 2  # 541330xR425 + 336611x1905


def test_limit_caps_with_honest_total():
    out = S.execute_sub_universe({"uei": TARGET, "limit": 1})
    assert out["meta"]["returned"] == 1
    assert out["meta"]["total"] == 2
    assert out["meta"]["capped"] is True


def test_meta_carries_tier_counts():
    # both tiers present in the fixture universe: BUYER disclosed, BUYER3 undisclosed.
    out = S.execute_sub_universe({"uei": TARGET})
    m = out["meta"]
    assert m["n_disclosed_universe"] == 1
    assert m["n_undisclosed_universe"] == 1
    assert m["returned_disclosed"] == 1
    assert m["returned_undisclosed"] == 1
    assert m["returned_disclosed"] + m["returned_undisclosed"] == m["returned"]


def test_target_combo_farmout_definition_c(monkeypatch):
    # v3: farm-out keyed to the TARGET's demonstrated combos. A node matched via
    # anchor combo X must carry disclosed farm-out at target combo Y even when Y
    # is NOT in the anchor portfolio (membership + matched_via unchanged).
    extra_edge = {"subaward_amount": 50_000.0, "subaward_action_date": "2024-08-01",
                  "prime_awardee_uei": ANCHOR, "prime_awardee_name": "ANCHOR PRIME",
                  "prime_award_naics_code": "561730",
                  "prime_award_product_or_service_code": "S208",
                  "prime_award_parent_piid": None,
                  "subaward_primary_place_of_performance_state_code": "AL"}
    buyer_fo_s208 = {"uei": BUYER, "naics_code": "561730", "psc_code": "S208",
                     "naics_title": "Landscaping", "psc_title": "Grounds Maintenance",
                     "farmout_amt_60mo": 750_000.0, "farmout_amt_lifetime": 900_000.0,
                     "median_chunk_60mo": 150_000.0, "median_chunk_lifetime": 140_000.0,
                     "p75_chunk_60mo": 250_000.0, "n_subawards_lifetime": 5,
                     "n_distinct_subs_60mo": 4, "last_action_date": "2025-04-01"}
    monkeypatch.setattr(S, "_scan_target_edges",
                        lambda uei: ([dict(r) for r in TARGET_EDGES] + [dict(extra_edge)])
                        if uei == TARGET else [])
    monkeypatch.setattr(S, "_scan_farmout",
                        lambda: [dict(r) for r in FARMOUT] + [dict(buyer_fo_s208)])
    S.reset_caches_for_tests()
    out = S.execute_sub_universe({"uei": TARGET})
    assert out["meta"]["recipe"] == "sub_universe.v3"
    node = next(n for n in out["data"] if n["uei"] == BUYER)
    # membership untouched: 561730xS208 is outside the anchor portfolio
    assert {m["combo"] for m in node["matched_via"]} == {"541330xR425"}
    tcf = node["target_combo_farmout"]
    assert tcf["n_combos"] == 2
    assert tcf["farmout_60mo"] == 2_750_000.0
    # $-desc order: the 2M anchor-overlap lane, then the 750K target-only lane
    assert [c["combo"] for c in tcf["combos"]] == ["541330xR425", "561730xS208"]
    s208 = tcf["combos"][1]
    assert s208["median_chunk_60mo"] == 150_000.0
    assert s208["p75_chunk_60mo"] == 250_000.0
    assert s208["n_distinct_subs_60mo"] == 4
    # undisclosed node: null, never an empty block (unknown != zero)
    b3 = next(n for n in out["data"] if n["uei"] == BUYER3)
    assert b3["target_combo_farmout"] is None


def test_undisclosed_quota_survives_disclosed_flood(monkeypatch):
    # Flood the disclosed tier past a small limit; the undisclosed winner must
    # still make the page via the reserved quota (H3 frontier never truncated).
    flood_farmout = [dict(FARMOUT[0], uei=f"UEIFLOOD{i:04d}") for i in range(20)]
    flood_lanes = {f"UEIFLOOD{i:04d}":
                   [{"uei": f"UEIFLOOD{i:04d}", "naics_code": "541330",
                     "psc_code": "R425", "prime_obl_60mo": 9_000_000.0 + i}]
                   for i in range(20)}
    monkeypatch.setattr(S, "_scan_farmout", lambda: [dict(r) for r in FARMOUT] + flood_farmout)
    merged = dict(PRIME_LANES, **flood_lanes)
    monkeypatch.setattr(S, "_scan_prime_lanes",
                        lambda ueis: [dict(r) for u in ueis for r in merged.get(u, [])])
    S.reset_caches_for_tests()
    out = S.execute_sub_universe({"uei": TARGET, "limit": 10})
    ueis = {n["uei"] for n in out["data"]}
    assert BUYER3 in ueis                      # undisclosed winner survives the flood
    assert out["meta"]["returned_undisclosed"] >= 1
    assert out["meta"]["returned_disclosed"] <= 9
