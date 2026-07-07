"""Unit tests for the sub-universe recipe v1 — pure, no R2 / network.

Pins (1) the fail-closed request contract ({uei, limit?} ONLY — unknown keys →
MapCompileError), (2) the universe definition: nodes are DISCLOSED sub-buyers
(farm-out lanes) in the anchors' portfolio combos, target + anchors excluded,
(3) NO SCORING: flat list ordered by matched farm-out $ with matched_via evidence
per node (combo, candidate farm-out facts, best-anchor obl, prime_backed stamp),
(4) target ground truth + form defaults derived from the target's own edges
(default MVS = their median chunk; PoP states $-ordered), (5) empty-anchor targets
are a valid 200 with meta.reason, and (6) the boot-cache lifecycle (cold→warm)."""

from __future__ import annotations

import pytest

from apps.catalyst_api.src import lance_store, sub_universe_store as S

TARGET = "UEITARGET001"
ANCHOR = "UEIANCHOR001"
BUYER = "UEIBUYER0001"
BUYER2 = "UEIBUYER0002"

TEAMING = [
    {"prime_uei": ANCHOR, "sub_uei": TARGET, "prime_name": "ANCHOR PRIME",
     "sub_name": "TARGET SUB", "edge_dollars_5y": 1_000_000.0, "edge_count_5y": 4,
     "last_action_date": "2025-06-01"},
    # BUYER's teaming stats (as a prime, to other subs)
    {"prime_uei": BUYER, "sub_uei": "UEIOTHERSUB1", "prime_name": "BUYER ONE",
     "sub_name": "OTHER SUB", "edge_dollars_5y": 500_000.0, "edge_count_5y": 5,
     "last_action_date": "2025-01-01"},
    {"prime_uei": BUYER2, "sub_uei": "UEIOTHERSUB2", "prime_name": "BUYER TWO",
     "sub_name": "OTHER SUB 2", "edge_dollars_5y": 100_000.0, "edge_count_5y": 1,
     "last_action_date": "2024-01-01"},
]

FARMOUT = [
    # BUYER buys subs in the anchor's combo — admitted
    {"uei": BUYER, "naics_code": "541330", "psc_code": "R425",
     "naics_title": "Engineering", "psc_title": "Eng Support",
     "farmout_amt_60mo": 2_000_000.0, "farmout_amt_lifetime": 3_000_000.0,
     "median_chunk_60mo": 400_000.0, "median_chunk_lifetime": 350_000.0,
     "p75_chunk_60mo": 800_000.0, "n_subawards_lifetime": 6,
     "n_distinct_subs_60mo": 3, "last_action_date": "2025-05-01"},
    # BUYER2 in a combo OUTSIDE the anchor portfolio — not admitted
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

PRIME_LANES = {
    ANCHOR: [{"uei": ANCHOR, "naics_code": "541330", "psc_code": "R425",
              "prime_obl_60mo": 8_000_000.0, "last_action_date": "2025-06-01"}],
    TARGET: [{"uei": TARGET, "naics_code": "541330", "psc_code": "R425",
              "prime_obl_60mo": 100_000.0, "last_action_date": "2025-03-01"}],
}

TARGET_EDGES = [
    {"subaward_amount": 300_000.0, "subaward_action_date": "2024-05-01",
     "prime_awardee_uei": ANCHOR, "prime_award_naics_code": "541330",
     "prime_award_product_or_service_code": "R425",
     "prime_award_parent_piid": "VEHICLE9",
     "subaward_primary_place_of_performance_state_code": "AL"},
    {"subaward_amount": 100_000.0, "subaward_action_date": "2023-01-15",
     "prime_awardee_uei": ANCHOR, "prime_award_naics_code": "541330",
     "prime_award_product_or_service_code": "R425",
     "prime_award_parent_piid": None,
     "subaward_primary_place_of_performance_state_code": "DC"},
]

GEO = [{"uei": BUYER, "latitude": 34.7, "longitude": -86.5, "geo_precision": "address"}]


@pytest.fixture(autouse=True)
def _patch(monkeypatch):
    S.reset_caches_for_tests()
    monkeypatch.setattr(S, "_scan_teaming", lambda: [dict(r) for r in TEAMING])
    monkeypatch.setattr(S, "_scan_farmout", lambda: [dict(r) for r in FARMOUT])
    monkeypatch.setattr(S, "_scan_vehicles", lambda: [dict(r) for r in VEHICLES])
    monkeypatch.setattr(S, "_scan_prime_lanes",
                        lambda ueis: [dict(r) for u in ueis for r in PRIME_LANES.get(u, [])])
    monkeypatch.setattr(S, "_scan_target_edges",
                        lambda uei: [dict(r) for r in TARGET_EDGES] if uei == TARGET else [])
    monkeypatch.setattr(S, "_scan_geo", lambda ueis: [dict(g) for g in GEO if g["uei"] in ueis])
    yield
    S.reset_caches_for_tests()


def test_unknown_keys_fail_closed():
    with pytest.raises(lance_store.MapCompileError):
        S.validate_request({"uei": TARGET, "mode": "combos"})
    with pytest.raises(lance_store.MapCompileError):
        S.validate_request({"uei": "short"})
    with pytest.raises(lance_store.MapCompileError):
        S.validate_request({"uei": TARGET, "limit": 0})


def test_universe_membership_and_evidence():
    out = S.execute_sub_universe({"uei": TARGET})
    ueis = [n["uei"] for n in out["data"]]
    assert ueis == [BUYER]                      # BUYER2 (foreign combo) + ANCHOR excluded
    node = out["data"][0]
    assert node["name"] == "BUYER ONE"
    assert node["latitude"] == 34.7
    mv = node["matched_via"][0]
    assert mv["combo"] == "541330xR425"
    assert mv["median_chunk_60mo"] == 400_000.0
    assert mv["anchor_uei"] == ANCHOR and mv["anchor_obl_60mo"] == 8_000_000.0
    assert mv["prime_backed"] is True           # target primes 541330xR425 itself
    assert node["teaming"]["deepest_repeat_edges_5y"] == 5
    assert node["teaming"]["n_partners_ge_3_edges"] == 1
    assert node["vehicles"][0]["parent_piid"] == "VEHICLE1"
    # no scoring anywhere
    assert "score" not in node and "rank" not in node


def test_target_block_and_defaults():
    out = S.execute_sub_universe({"uei": TARGET})
    t = out["target"]
    assert t["anchors"][0]["prime_uei"] == ANCHOR
    combo = t["demonstrated_combos"][0]
    assert combo["combo"] == "541330xR425" and combo["n_edges"] == 2
    assert combo["median_chunk_usd"] == 200_000.0
    assert t["defaults"]["mvs_usd"] == 200_000.0        # their own median chunk
    assert t["defaults"]["pop_states"] == ["AL", "DC"]  # $-ordered
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


def test_limit_caps_with_honest_total():
    out = S.execute_sub_universe({"uei": TARGET, "limit": 1})
    assert out["meta"]["returned"] == 1
    assert out["meta"]["total"] >= 1
