"""Unit tests for sub_universe_pairs — the v3 pair-grain precompute (no R2/network).

Pins the pair-grain contract that replaces the dead per-UEI blob (freeze-doc §0):
  (1) one pair row per node of the FULL sub_universe.v3 universe (paging stripped);
  (2) pair scalars carry the pair-specific facts ONLY — matched obl/farm-out,
      Definition-C tcf totals, teaming, band_fit, HQ geo, compact matched_via_json;
  (3) NULL SEMANTICS: undisclosed node → matched_farmout_60mo null (not 0) +
      tcf null; no-pair-row node → all teaming scalars null;
  (4) Definition C keying: farm-out at a TARGET combo OUTSIDE the anchor portfolio
      is captured in tcf even though membership/matched_via are unchanged;
  (5) band_fit vs the TARGET's own p20–p80 band (true/false/null);
  (6) NO node-grain hydration in the pair row (no award_state / demand_events /
      entity / win_portfolio keys) — those serve at query time;
  (7) NO SCORING.

Reuses the store fixture universe: TARGET has anchor ANCHOR; BUYER is a disclosed
sub-buyer winner; BUYER3 an undisclosed winner with no pair rows. The
target_analytics scans (pool/peers/sam/award-state) are stubbed empty — the pair
rows are the surface under test here; Acts 1–3 correctness is pinned by the
blob-era logic these reuse verbatim."""

from __future__ import annotations

import json

import pytest

from apps.catalyst_api.src import sub_universe_store as S, sub_universe_pairs as P

TARGET = "UEITARGET001"
ANCHOR = "UEIANCHOR001"
BUYER = "UEIBUYER0001"    # disclosed sub-buyer winner
BUYER3 = "UEIBUYER0003"   # undisclosed winner (no farm-out, no pair rows)

PAIRS = [
    {"prime_uei": BUYER, "prime_name": "BUYER ONE", "edge_count_5y": 5},
    {"prime_uei": BUYER, "prime_name": "BUYER ONE", "edge_count_5y": 1},
    {"prime_uei": BUYER, "prime_name": "BUYER ONE", "edge_count_5y": 0},
    # BUYER3 has NO pair rows — teaming must be null, not zero
]

FARMOUT = [
    {"uei": BUYER, "naics_code": "541330", "psc_code": "R425",
     "naics_title": "Engineering", "psc_title": "Eng Support",
     "farmout_amt_60mo": 2_000_000.0, "farmout_amt_lifetime": 3_000_000.0,
     "median_chunk_60mo": 400_000.0, "median_chunk_lifetime": 350_000.0,
     "p75_chunk_60mo": 800_000.0, "n_subawards_lifetime": 6,
     "n_distinct_subs_60mo": 3, "last_action_date": "2025-05-01"},
    {"uei": ANCHOR, "naics_code": "541330", "psc_code": "R425",
     "naics_title": "Engineering", "psc_title": "Eng Support",
     "farmout_amt_60mo": 5_000_000.0, "farmout_amt_lifetime": 5_000_000.0,
     "median_chunk_60mo": 250_000.0, "median_chunk_lifetime": 250_000.0,
     "p75_chunk_60mo": 500_000.0, "n_subawards_lifetime": 10,
     "n_distinct_subs_60mo": 5, "last_action_date": "2025-06-01"},
]

WINNER_LANES = [
    {"uei": BUYER, "naics_code": "541330", "psc_code": "R425", "prime_obl_60mo": 3_000_000.0},
    {"uei": BUYER3, "naics_code": "541330", "psc_code": "R425", "prime_obl_60mo": 7_000_000.0},
    {"uei": ANCHOR, "naics_code": "541330", "psc_code": "R425", "prime_obl_60mo": 8_000_000.0},
    {"uei": TARGET, "naics_code": "541330", "psc_code": "R425", "prime_obl_60mo": 100_000.0},
]

PRIME_LANES = {
    ANCHOR: [{"uei": ANCHOR, "naics_code": "541330", "psc_code": "R425",
              "prime_obl_60mo": 8_000_000.0, "last_action_date": "2025-06-01"}],
    TARGET: [{"uei": TARGET, "naics_code": "541330", "psc_code": "R425",
              "prime_obl_60mo": 100_000.0, "last_action_date": "2025-03-01"}],
}

# target deal band: five edges so p20..p80 is meaningful; median 300_000 so the
# BUYER node median chunk (400_000) sits INSIDE the band -> band_overlap True.
TARGET_EDGES = [
    {"subaward_amount": v, "subaward_action_date": "2024-05-01",
     "prime_awardee_uei": ANCHOR, "prime_awardee_name": "ANCHOR PRIME",
     "prime_award_naics_code": "541330", "prime_award_product_or_service_code": "R425",
     "prime_award_parent_piid": "VEHICLE9",
     "subaward_primary_place_of_performance_state_code": "AL"}
    for v in (200_000.0, 250_000.0, 300_000.0, 450_000.0, 600_000.0)
]

GEO = [{"uei": BUYER, "latitude": 34.7, "longitude": -86.5, "geo_precision": "address"}]


@pytest.fixture(autouse=True)
def _patch(monkeypatch):
    S.reset_caches_for_tests()
    # store-side seams (drive the universe)
    monkeypatch.setattr(S, "_scan_pairs", lambda: [dict(r) for r in PAIRS])
    monkeypatch.setattr(S, "_scan_farmout", lambda: [dict(r) for r in FARMOUT])
    monkeypatch.setattr(S, "_scan_vehicles", lambda: [])
    monkeypatch.setattr(S, "_scan_winner_lanes", lambda: [dict(r) for r in WINNER_LANES])
    monkeypatch.setattr(S, "_scan_prime_lanes",
                        lambda ueis: [dict(r) for u in ueis for r in PRIME_LANES.get(u, [])])
    monkeypatch.setattr(S, "_scan_target_edges",
                        lambda uei: [dict(r) for r in TARGET_EDGES] if uei == TARGET else [])
    monkeypatch.setattr(S, "_scan_geo", lambda ueis: [dict(g) for g in GEO if g["uei"] in ueis])
    monkeypatch.setattr(S, "_scan_demand_events", lambda ueis: [])
    # pairs-side analytics seams stubbed empty (Acts 1–3 not under test here)
    monkeypatch.setattr(P, "_scan_sam_entities", lambda ueis: [])
    monkeypatch.setattr(P, "_scan_award_rows_by_piid", lambda piids: [])
    monkeypatch.setattr(P, "_scan_pool", lambda lanes, states, w24: [])
    monkeypatch.setattr(P, "_scan_sub_lanes_for_combos", lambda lanes: [])
    monkeypatch.setattr(P, "_scan_sub_profiles", lambda ueis: [])
    yield
    S.reset_caches_for_tests()


def _pair(result, node_uei):
    return next(p for p in result["pairs"] if p["node_uei"] == node_uei)


def test_one_pair_row_per_node_full_universe():
    r = P.build_target(TARGET)
    node_ueis = {p["node_uei"] for p in r["pairs"]}
    assert node_ueis == {BUYER, BUYER3}          # full winner set, target+anchor excluded
    assert r["target"]["n_nodes"] == 2
    assert r["target"]["n_disclosed"] == 1
    assert r["target"]["n_undisclosed"] == 1
    assert all(p["target_uei"] == TARGET for p in r["pairs"])
    assert all(p["recipe"] == "sub_universe_pairs.v1" for p in r["pairs"])


def test_disclosed_pair_scalars_and_geo():
    r = P.build_target(TARGET)
    p = _pair(r, BUYER)
    assert p["node_name"] == "BUYER ONE"
    assert p["disclosed_sub_buyer"] is True
    assert p["matched_prime_obl_60mo"] == 3_000_000.0
    assert p["matched_farmout_60mo"] == 2_000_000.0
    assert p["n_matched_combos"] == 1
    # teaming scalars present (has pair rows): two 5y-active pair rows (edge 5 +
    # edge 1); the lifetime-only (0-edge) row is not counted.
    assert p["teaming_n_sub_partners_5y"] == 2
    assert p["teaming_deepest_repeat_edges_5y"] == 5
    assert p["teaming_n_partners_ge_3_edges"] == 1
    # HQ geo — the one inline per-node hydration
    assert p["latitude"] == 34.7 and p["longitude"] == -86.5
    assert p["geo_precision"] == "address"
    # compact matched_via_json (top-5)
    mv = json.loads(p["matched_via_json"])
    assert mv[0]["combo"] == "541330xR425"
    assert mv[0]["candidate_prime_obl_60mo"] == 3_000_000.0
    assert p["matched_via_truncated"] is False


def test_no_node_grain_hydration_in_pair_row():
    # award_state / demand_events / entity / win_portfolio must NOT be in the pair
    # row — they serve at query time from the indexed node-grain marts (freeze §0).
    r = P.build_target(TARGET)
    p = _pair(r, BUYER)
    for forbidden in ("award_state", "demand_events", "entity", "win_portfolio",
                      "gate_facts", "vehicles"):
        assert forbidden not in p
    # no scoring
    assert "score" not in p and "rank" not in p


def test_undisclosed_node_null_semantics():
    r = P.build_target(TARGET)
    p = _pair(r, BUYER3)
    assert p["disclosed_sub_buyer"] is False
    assert p["matched_farmout_60mo"] is None          # unknown != zero
    assert p["matched_prime_obl_60mo"] == 7_000_000.0
    assert p["node_name"] is None                     # no pair rows
    assert p["tcf_farmout_60mo"] is None              # no farm-out evidence
    assert p["tcf_n_combos"] is None
    # all teaming scalars null (absent pair history is an absent fact)
    assert p["teaming_n_sub_partners_5y"] is None
    assert p["teaming_deepest_repeat_edges_5y"] is None
    assert p["teaming_n_partners_ge_3_edges"] is None
    # no geo row -> null (not zero)
    assert p["latitude"] is None and p["geo_precision"] is None
    # band_fit null when node discloses no target-combo median
    assert p["node_median_chunk_60mo"] is None
    assert p["band_overlap"] is None


def test_band_fit_against_target_band():
    r = P.build_target(TARGET)
    p = _pair(r, BUYER)
    # BUYER discloses farm-out at target combo 541330xR425 with 60mo median 400K;
    # target band p20..p80 over (200K,250K,300K,450K,600K) brackets 400K -> True.
    assert p["node_median_chunk_60mo"] == 400_000.0
    assert p["band_overlap"] is True


def test_definition_c_captures_target_combo_outside_anchor_portfolio(monkeypatch):
    # v3 Definition C: a node matched via anchor combo X must carry tcf at target
    # combo Y OUTSIDE the anchor portfolio. Add a target-only combo 561730xS208 the
    # target performs and BUYER farms out under; membership/matched_via unchanged.
    extra_edge = {"subaward_amount": 300_000.0, "subaward_action_date": "2024-08-01",
                  "prime_awardee_uei": ANCHOR, "prime_awardee_name": "ANCHOR PRIME",
                  "prime_award_naics_code": "561730",
                  "prime_award_product_or_service_code": "S208",
                  "prime_award_parent_piid": None,
                  "subaward_primary_place_of_performance_state_code": "AL"}
    buyer_fo_s208 = {"uei": BUYER, "naics_code": "561730", "psc_code": "S208",
                     "naics_title": "Landscaping", "psc_title": "Grounds",
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
    r = P.build_target(TARGET)
    p = _pair(r, BUYER)
    # membership untouched — 561730xS208 is outside the anchor portfolio, not a
    # matched combo; the matched_via_json still only carries the anchor combo.
    mv = json.loads(p["matched_via_json"])
    assert {m["combo"] for m in mv} == {"541330xR425"}
    assert p["n_matched_combos"] == 1
    # tcf captures BOTH target combos: the anchor-overlap 541330xR425 (2M) + the
    # target-only 561730xS208 (750K).
    assert p["tcf_n_combos"] == 2
    assert p["tcf_farmout_60mo"] == 2_750_000.0
    # band_fit now medians the two disclosed target-combo 60mo medians (400K, 150K)
    # -> 275K, inside the target band -> True.
    assert p["node_median_chunk_60mo"] == 275_000.0
    assert p["band_overlap"] is True


def test_target_row_carries_analytics_json():
    r = P.build_target(TARGET)
    t = r["target"]
    assert t["uei"] == TARGET
    assert t["recipe"] == "sub_universe_pairs.v1"
    analytics = json.loads(t["target_analytics"])
    assert "entity" in analytics and "adjacent_market" in analytics and "field" in analytics
    assert analytics["scopes"]["window_months"] == 24
    # timings json parses
    assert isinstance(json.loads(t["timings_ms"]), dict)
