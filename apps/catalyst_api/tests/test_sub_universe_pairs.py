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
     "subaward_primary_place_of_performance_state_code": "AL",
     "sub_place_of_perform_county_code": "089",
     "sub_place_of_perform_county_name": "MADISON"}
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
    monkeypatch.setattr(P, "_scan_naics_reference",
                        lambda: [{"naics_code": "5413", "naics_title": "Engineering Services"}])
    monkeypatch.setattr(P, "_naics4_titles", None)
    monkeypatch.setattr(P, "_scan_geo_bulk",
                        lambda ueis: [dict(g) for g in GEO if g["uei"] in ueis])
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
    assert all(p["recipe"] == "sub_universe_pairs.v2" for p in r["pairs"])


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
                      "gate_facts", "vehicles", "matched_combos"):
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
    assert t["recipe"] == "sub_universe_pairs.v2"
    analytics = json.loads(t["target_analytics"])
    assert "entity" in analytics and "adjacent_market" in analytics and "field" in analytics
    assert analytics["scopes"]["window_months"] == 24
    # timings json parses
    assert isinstance(json.loads(t["timings_ms"]), dict)


# ── v2: family rollups (freeze §0.1.3) ────────────────────────────────────────
def test_family_key_corrected_definition():
    from apps.catalyst_api.src.psc_families import family_key, psc_family
    # services / R&D: single letter
    assert family_key("541330", "R425") == "5413xR"
    assert family_key("541712", "AC12") == "5417xA"
    # products: 2-digit FSC GROUP — missiles/aircraft/ships must NOT collapse
    assert family_key("336414", "1410") == "3364x14"
    assert family_key("336411", "1510") == "3364x15"
    assert family_key("336611", "1903") == "3366x19"
    assert psc_family("5985") == "59"
    # nulls: absent halves refuse (null ≠ zero)
    assert family_key(None, "R425") is None
    assert family_key("541330", "") is None
    assert family_key("54", "R425") is None


def test_pair_family_rollups_disclosed_only():
    r = P.build_target(TARGET)
    p = _pair(r, BUYER)
    fam_obl = json.loads(p["family_matched_obl_60mo"])
    assert fam_obl == {"5413xR": 3_000_000.0}
    fam_tcf = json.loads(p["family_tcf_farmout_60mo"])
    assert fam_tcf == {"5413xR": 2_000_000.0}
    # undisclosed node: matched families still present (obl is winners-index
    # evidence), tcf family dict NULL — no disclosed lane at any target combo.
    u = _pair(r, BUYER3)
    assert json.loads(u["family_matched_obl_60mo"]) == {"5413xR": 7_000_000.0}
    assert u["family_tcf_farmout_60mo"] is None


def test_family_tcf_sums_within_family_across_target_combos(monkeypatch):
    # Two disclosed target-combo lanes in DIFFERENT families sum separately;
    # negative farm-out passes through unclamped (no-scoring doctrine).
    extra_edge = {"subaward_amount": 300_000.0, "subaward_action_date": "2024-08-01",
                  "prime_awardee_uei": ANCHOR, "prime_awardee_name": "ANCHOR PRIME",
                  "prime_award_naics_code": "561730",
                  "prime_award_product_or_service_code": "S208",
                  "prime_award_parent_piid": None,
                  "subaward_primary_place_of_performance_state_code": "AL"}
    buyer_fo_s208 = {"uei": BUYER, "naics_code": "561730", "psc_code": "S208",
                     "naics_title": "Landscaping", "psc_title": "Grounds",
                     "farmout_amt_60mo": -750_000.0, "farmout_amt_lifetime": 900_000.0,
                     "median_chunk_60mo": None, "median_chunk_lifetime": None,
                     "p75_chunk_60mo": None, "n_subawards_lifetime": 5,
                     "n_distinct_subs_60mo": 4, "last_action_date": "2025-04-01"}
    monkeypatch.setattr(S, "_scan_target_edges",
                        lambda uei: ([dict(r) for r in TARGET_EDGES] + [dict(extra_edge)])
                        if uei == TARGET else [])
    monkeypatch.setattr(S, "_scan_farmout",
                        lambda: [dict(r) for r in FARMOUT] + [dict(buyer_fo_s208)])
    S.reset_caches_for_tests()
    r = P.build_target(TARGET)
    p = _pair(r, BUYER)
    fam_tcf = json.loads(p["family_tcf_farmout_60mo"])
    assert fam_tcf == {"5413xR": 2_000_000.0, "5617xS": -750_000.0}


def test_target_row_demonstrated_families_and_counties():
    r = P.build_target(TARGET)
    fams = json.loads(r["target"]["demonstrated_families"])
    assert len(fams) == 1
    f = fams[0]
    assert f["family"] == "5413xR"
    assert f["n_edges"] == 5 and f["total_usd"] == 1_800_000.0
    assert f["share_pct"] == 100.0
    assert f["combos"] == ["541330xR425"]
    # titles: naics4 from the reference seam × static PSC category name
    assert f["title"] == ("Engineering Services × Professional, Administrative "
                          "& Management Support")
    # input 1's county-grain footprint in scopes
    counties = json.loads(r["target"]["target_analytics"])["scopes"]["pop_counties"]
    assert counties == [{"state": "AL", "county_code": "089",
                         "county_name": "MADISON", "sub_usd": 1_800_000.0,
                         "n_edges": 5}]


# ── v2: uncapped build_mode + the mega-guard (freeze §0.1.2) ──────────────────
def test_build_mode_mega_guard_discloses_truncation(monkeypatch):
    monkeypatch.setattr(S, "BUILD_NODE_CAP", 1)
    r = P.build_target(TARGET)
    assert len(r["pairs"]) == 1                       # guard truncated the build
    assert r["target"]["nodes_truncated"] is True     # ...and disclosed it
    assert r["meta"]["nodes_truncated"] is True


def test_serving_path_unchanged_by_build_mode():
    # the serving quota page still hydrates node-grain facts and honors limit
    out = S.execute_sub_universe({"uei": TARGET, "limit": 1})
    assert out["meta"]["returned"] == 1
    node = out["data"][0]
    for key in ("demand_events", "vehicles", "gate_facts", "latitude"):
        assert key in node
    assert "matched_combos" not in node
