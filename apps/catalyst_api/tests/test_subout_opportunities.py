"""Unit tests for the subout-opportunities recipe v3 — pure, no R2 / network.

Pins (1) the fail-closed request contract ({uei, limit?} ONLY — v2's lenses /
codes_override / code_type / include_peers are unknown keys → 422-class
MapCompileError), (2) the relationship matching: RULE A (open awards of the primes
the target received subawards from) and RULE B (open awards with subaward edges to
peer firms that won prime awards in the target's own (agency, code) pairs) — and
NOTHING else (no code lenses, no SAM, no inference), (3) NO SCORING: a flat list
sorted by total_obligation DESC with no score/components keys anywhere, each row
carrying its matched_via relationship evidence (union dedupe: a dual-matched award
is ONE row with BOTH evidences), (4) the in-process cache architecture (cold→warm,
single load, background TTL refresh, failed-build degrade + retry, v3 drops the
cube/lanes/SAM caches — the ONLY remote reads are the four relationship scans),
and (5) the map-ready wire (row coordinates, meta.target_hq on every answer
including empty ones, nearest-site enrichment with its own point, distance as a
fact)."""

from __future__ import annotations

import time
from datetime import date

import pytest
from fastapi import HTTPException

from apps.catalyst_api.src import config, lance_store, subout_store

TODAY = date(2026, 7, 6)

TARGET = "UEITARGET001"
NO_SIGNAL_UEI = "UEINOSIGNAL1"
P1, P2 = "UEIPRIME0001", "UEIPRIME0002"
P_DEAD = "UEIPRIMEDEAD"                  # target's former prime with NO open awards
PEER1, PEER2 = "UEIPEER00001", "UEIPEER00002"

# ── The target's FSRS subaward edges (rule A substrate) ────────────────────────
TARGET_SUB_EDGES = [
    {"subawardee_uei": TARGET, "prime_awardee_uei": P1,
     "subaward_amount": 5_000_000.0, "subaward_action_date": date(2026, 1, 15),
     "prime_award_naics_code": "541690", "prime_award_product_or_service_code": "R499",
     "subaward_primary_place_of_performance_state_code": "VA"},
    {"subawardee_uei": TARGET, "prime_awardee_uei": P1,
     "subaward_amount": 1_000_000.0, "subaward_action_date": date(2025, 6, 1),
     "prime_award_naics_code": "541690", "prime_award_product_or_service_code": "R499",
     "subaward_primary_place_of_performance_state_code": None},
    {"subawardee_uei": TARGET, "prime_awardee_uei": P_DEAD,
     "subaward_amount": 99.0, "subaward_action_date": date(2020, 1, 1),
     "prime_award_naics_code": "999999", "prime_award_product_or_service_code": "Z999",
     "subaward_primary_place_of_performance_state_code": "CA"},
]

# ── The target's OWN prime awards (rule B pair substrate) ──────────────────────
TARGET_PRIME_AWARDS = [
    {"recipient_uei": TARGET, "awarding_agency_code": "097", "naics_code": "541690",
     "product_or_service_code": "R499", "total_obligation": 2_000_000.0,
     "primary_place_of_performance_state_code": "MD"},
    {"recipient_uei": TARGET, "awarding_agency_code": "047", "naics_code": None,
     "product_or_service_code": "D302", "total_obligation": 500_000.0,
     "primary_place_of_performance_state_code": None},
]
# → pairs: (097, naics, 541690) $2M · (097, psc, R499) $2M · (047, psc, D302) $0.5M

# Peer scans per pair: predicate → recipient rows (TARGET must be excluded).
PEER_SCAN_RESULTS = {
    ("097", "naics_code", "541690"): [{"recipient_uei": PEER1}],
    ("097", "product_or_service_code", "R499"): [{"recipient_uei": TARGET},
                                                 {"recipient_uei": PEER2}],
    ("047", "product_or_service_code", "D302"): [],
}

# Peers' subaward edges (rule B join onto the open-award cache by award id).
PEER_SUB_EDGES = [
    {"subawardee_uei": PEER1, "prime_award_unique_key": "CONT_AWD_A2"},
    {"subawardee_uei": PEER1, "prime_award_unique_key": "CONT_AWD_A5_EXPIRED"},
    {"subawardee_uei": PEER2, "prime_award_unique_key": "CONT_AWD_A1"},
]

# ── Combos-mode fixtures ───────────────────────────────────────────────────────
LOOK1, LOOK2, LOOK3 = "UEILOOK00001", "UEILOOK00002", "UEILOOK00003"
NOPRIME = "UEINOPRIME01"                 # highest overlap but never primed — SKIPPED

# candidate edges per target combo (streamed by combo predicate)
LOOKALIKE_CANDIDATE_EDGES = {
    ("541690", "R499"): [
        {"subawardee_uei": LOOK1, "subaward_amount": 9_000_000.0},
        {"subawardee_uei": NOPRIME, "subaward_amount": 8_000_000.0},
        {"subawardee_uei": LOOK2, "subaward_amount": 4_000_000.0},
        {"subawardee_uei": LOOK3, "subaward_amount": 2_000_000.0},
        {"subawardee_uei": TARGET, "subaward_amount": 1_000_000.0},  # self: excluded
    ],
    ("999999", "Z999"): [],
}
# rollup primed-check (the lookalike QUALIFIER)
LOOKALIKE_ROLLUP_ROWS = [
    {"uei": LOOK1, "prime_award_ct_lifetime": 3},
    {"uei": NOPRIME, "prime_award_ct_lifetime": 0},
    {"uei": LOOK2, "prime_award_ct_lifetime": 2},
    {"uei": LOOK3, "prime_award_ct_lifetime": 1},
]
LOOKALIKE_NAME_ROWS = [
    {"uei": LOOK1, "legal_business_name": "ALPHA BOTHSIDER LLC"},
    {"uei": LOOK2, "legal_business_name": "BRAVO BOTHSIDER INC"},
    {"uei": LOOK3, "legal_business_name": "CHARLIE BOTHSIDER CO"},
]
# the lookalikes' full sub histories (the expansion scan: subawardee_uei IN)
LOOKALIKE_SUB_HISTORY = [
    {"subawardee_uei": LOOK1, "prime_award_naics_code": "236220",
     "prime_award_product_or_service_code": "Y1AA", "subaward_amount": 3_000_000.0},
    {"subawardee_uei": LOOK2, "prime_award_naics_code": "236220",
     "prime_award_product_or_service_code": "Y1AA", "subaward_amount": 1_000_000.0},
    {"subawardee_uei": LOOK1, "prime_award_naics_code": "541690",
     "prime_award_product_or_service_code": "R499", "subaward_amount": 9_000_000.0},
    {"subawardee_uei": LOOK3, "prime_award_naics_code": "541519",
     "prime_award_product_or_service_code": "D302", "subaward_amount": 2_000_000.0},
]

GEO_ROWS = [
    {"uei": TARGET, "latitude": 38.8816, "longitude": -77.0910},
    {"uei": NO_SIGNAL_UEI, "latitude": 38.9072, "longitude": -77.0369},
]

# S1 sits EXACTLY on A1's zip5 centroid.
FEDERAL_SITE_POINTS = [
    (38.9586, -77.3570, "RESTON FEDERAL CENTER", "OFFICE", "gsa_building",
     3, date(2027, 1, 15)),
]

# Open awards: A1 (P1, open, zip5 — rule A, ALSO rule B via PEER2 = dual evidence);
# A2 (P2, open, no geo — rule B via PEER1 only); A5 (P1, EXPIRED — the request-time
# open-date re-check must drop it from BOTH rules).
OPEN_AWARD_ROWS = [
    {"generated_unique_award_id": "CONT_AWD_A1", "award_id_piid": "PIID_A1",
     "recipient_uei": P1, "recipient_name": "PRIME ONE LLC",
     "naics_code": "541519", "product_or_service_code": "D302",
     "total_obligation": 12_000_000.0, "base_and_all_options_value": 30_000_000.0,
     "subaward_count": 12, "total_subaward_amount": 8_000_000.0,
     "subcontracting_plan_code": "F",
     "period_of_performance_current_end_date": date(2026, 12, 31),
     "ordering_period_end_date": None,
     "award_or_idv_flag": "AWARD", "idv_type_code": None,
     "type_of_set_aside_code": "NONE",
     "awarding_agency_code": "097", "awarding_agency_name": "Department of Defense",
     "primary_place_of_performance_state_code": "VA",
     "latitude": 38.9586, "longitude": -77.3570, "geo_precision": "zip5"},
    {"generated_unique_award_id": "CONT_AWD_A2", "award_id_piid": "PIID_A2",
     "recipient_uei": P2, "recipient_name": "PRIME TWO INC",
     "naics_code": "236220", "product_or_service_code": "Y1AA",
     "total_obligation": 3_000_000.0, "base_and_all_options_value": 9_000_000.0,
     "subaward_count": 0, "total_subaward_amount": 0.0,
     "subcontracting_plan_code": None,
     "period_of_performance_current_end_date": None,
     "ordering_period_end_date": date(2027, 6, 30),
     "award_or_idv_flag": "IDV", "idv_type_code": "B",
     "type_of_set_aside_code": None,
     "awarding_agency_code": "047", "awarding_agency_name": "General Services Administration",
     "primary_place_of_performance_state_code": None,
     "latitude": None, "longitude": None, "geo_precision": None},
    {"generated_unique_award_id": "CONT_AWD_A5_EXPIRED", "award_id_piid": "PIID_A5",
     "recipient_uei": P1, "recipient_name": "PRIME ONE LLC",
     "naics_code": "541519", "product_or_service_code": "D302",
     "total_obligation": 1_000_000.0, "base_and_all_options_value": 1_000_000.0,
     "subaward_count": 0, "total_subaward_amount": 0.0,
     "subcontracting_plan_code": None,
     "period_of_performance_current_end_date": date(2026, 7, 1),   # < TODAY
     "ordering_period_end_date": None,
     "award_or_idv_flag": "AWARD", "idv_type_code": None,
     "type_of_set_aside_code": None,
     "awarding_agency_code": "097", "awarding_agency_name": "Department of Defense",
     "primary_place_of_performance_state_code": "VA",
     "latitude": 38.9586, "longitude": -77.3570, "geo_precision": "zip5"},
    {"generated_unique_award_id": "CONT_AWD_A6", "award_id_piid": "PIID_A6",
     "recipient_uei": "UEIPRIME0003", "recipient_name": "PRIME THREE CO",
     "naics_code": "541690", "product_or_service_code": "R499",
     "total_obligation": 20_000_000.0, "base_and_all_options_value": 25_000_000.0,
     "subaward_count": 2, "total_subaward_amount": 1_000_000.0,
     "subcontracting_plan_code": "C",
     "period_of_performance_current_end_date": date(2027, 3, 31),
     "ordering_period_end_date": None,
     "award_or_idv_flag": "AWARD", "idv_type_code": None,
     "type_of_set_aside_code": None,
     "awarding_agency_code": "097", "awarding_agency_name": "Department of Defense",
     "primary_place_of_performance_state_code": "CA",
     "latitude": 34.05, "longitude": -118.24, "geo_precision": "zip5"},
    {"generated_unique_award_id": "CONT_AWD_A7_TX", "award_id_piid": "PIID_A7",
     "recipient_uei": "UEIPRIME0003", "recipient_name": "PRIME THREE CO",
     "naics_code": "236220", "product_or_service_code": "Y1AA",
     "total_obligation": 9_000_000.0, "base_and_all_options_value": 9_000_000.0,
     "subaward_count": 0, "total_subaward_amount": 0.0,
     "subcontracting_plan_code": None,
     "period_of_performance_current_end_date": date(2027, 1, 1),
     "ordering_period_end_date": None,
     "award_or_idv_flag": "AWARD", "idv_type_code": None,
     "type_of_set_aside_code": None,
     "awarding_agency_code": "097", "awarding_agency_name": "Department of Defense",
     "primary_place_of_performance_state_code": "TX",
     "latitude": 32.77, "longitude": -96.79, "geo_precision": "zip5"},
]


class Seams:
    """Recording fakes for the v3 I/O seams: the four relationship reads
    (_scan_to_pylist / _stream_rows, routed by uri + predicate shape) and the
    cache loaders. Fails LOUD on any unexpected scan — the recipe must never
    touch the v2 cube/lanes/SAM/inferred surfaces again."""

    def __init__(self):
        self.scan_calls: list[tuple[str, tuple[str, ...], str | None]] = []
        self.stream_calls: list[tuple[str, tuple[str, ...], str | None, int]] = []
        self.open_award_loads = 0
        self.geo_loads = 0
        self.sites_loads = 0
        self.loader_error: Exception | None = None

    def scan_to_pylist(self, uri, columns, predicate):
        self.scan_calls.append((uri, tuple(columns), predicate))
        if uri == config.CONTRACT_SUBAWARD_URI:
            if "subawardee_uei IN" in predicate:
                # combos mode: the lookalikes' sub histories (expansion scan)
                rows = [r for r in LOOKALIKE_SUB_HISTORY
                        if f"'{r['subawardee_uei']}'" in predicate]
            else:
                # the target's own edges (rule A + combo profile): subawardee_uei = '<uei>'
                rows = [r for r in TARGET_SUB_EDGES if f"'{r['subawardee_uei']}'" in predicate]
        elif uri == config.USASPENDING_AWARD_CANONICAL_URI:
            # the target's own prime awards (rule B pairs + combos PoP states)
            rows = [r for r in TARGET_PRIME_AWARDS if f"'{r['recipient_uei']}'" in predicate]
        elif uri == config.GTM_ENTITY_BEHAVIOR_ROLLUP_URI:
            # combos mode: the lookalike primed-check (uei IN)
            rows = [r for r in LOOKALIKE_ROLLUP_ROWS if f"'{r['uei']}'" in predicate]
        elif uri == config.GTM_SAM_ENTITIES_URI:
            # combos mode: lookalike name hydration (uei IN)
            rows = [r for r in LOOKALIKE_NAME_ROWS if f"'{r['uei']}'" in predicate]
        else:
            raise AssertionError(f"unexpected remote scan uri {uri}")
        return [{c: r.get(c) for c in columns} for r in rows]

    def stream_rows(self, uri, columns, predicate, limit):
        self.stream_calls.append((uri, tuple(columns), predicate, limit))
        if uri == config.USASPENDING_AWARD_CANONICAL_URI:
            # peer-firm scans: awarding_agency_code = 'A' AND <col> = 'C'
            for (agency, col, code), rows in PEER_SCAN_RESULTS.items():
                if f"awarding_agency_code = '{agency}'" in predicate and \
                        f"{col} = '{code}'" in predicate:
                    return [{c: r.get(c) for c in columns} for r in rows][:limit]
            raise AssertionError(f"unexpected peer-scan predicate {predicate}")
        if uri == config.CONTRACT_SUBAWARD_URI:
            if "prime_award_naics_code = '" in predicate:
                # combos mode: lookalike-candidate edges per target combo
                for (naics, psc), rows in LOOKALIKE_CANDIDATE_EDGES.items():
                    if f"prime_award_naics_code = '{naics}'" in predicate and \
                            f"prime_award_product_or_service_code = '{psc}'" in predicate:
                        return [{c: r.get(c) for c in columns} for r in rows][:limit]
                raise AssertionError(f"unexpected combo predicate {predicate}")
            # peers' edges: subawardee_uei IN (...)
            rows = [r for r in PEER_SUB_EDGES if f"'{r['subawardee_uei']}'" in predicate]
            return [{c: r.get(c) for c in columns} for r in rows][:limit]
        raise AssertionError(f"unexpected remote stream uri {uri}")

    def load_open_awards(self):
        if self.loader_error is not None:
            raise self.loader_error
        self.open_award_loads += 1
        return [dict(r) for r in OPEN_AWARD_ROWS]

    def load_entity_geo(self):
        if self.loader_error is not None:
            raise self.loader_error
        self.geo_loads += 1
        import pyarrow as pa
        return pa.table({c: [r[c] for r in GEO_ROWS]
                         for c in ("uei", "latitude", "longitude")}) \
            .sort_by("uei").combine_chunks()

    def load_federal_sites(self):
        if self.loader_error is not None:
            raise self.loader_error
        self.sites_loads += 1
        return [tuple(s) for s in FEDERAL_SITE_POINTS]


@pytest.fixture()
def seams(monkeypatch):
    s = Seams()
    monkeypatch.setattr(subout_store, "_scan_to_pylist", s.scan_to_pylist)
    monkeypatch.setattr(subout_store, "_stream_rows", s.stream_rows)
    monkeypatch.setattr(subout_store, "_load_open_awards", s.load_open_awards)
    monkeypatch.setattr(subout_store, "_load_entity_geo", s.load_entity_geo)
    monkeypatch.setattr(subout_store, "_load_federal_sites", s.load_federal_sites)
    subout_store.reset_caches_for_tests()
    yield s
    subout_store.reset_caches_for_tests()


def _run(body, **kw):
    return subout_store.execute_subout_opportunities(body, today=TODAY, **kw)


# ── recipe constants (the versioned contract) ─────────────────────────────────
def test_recipe_id_and_bounds_are_the_published_contract():
    assert subout_store.RECIPE_ID == "subout_opportunities.v3"
    assert subout_store.ALLOWED_BODY_KEYS == {"uei", "limit", "mode"}
    assert subout_store.MODES == ("relationships", "combos")
    assert subout_store.COMBOS_RECIPE_ID == "subout_combos.v1"
    # combos-mode bounds are named module parameters too
    assert subout_store.TARGET_COMBO_CAP == 5
    assert subout_store.LOOKALIKE_CT == 3
    assert subout_store.EXPANSION_COMBO_CAP == 10
    # the rule B bounds are named module parameters — adjustable, never buried
    assert subout_store.PRIME_PAIR_CAP == 10
    assert subout_store.PEERS_PER_PAIR_CAP == 200
    assert subout_store.PEER_UNION_CAP == 1_000
    assert subout_store.PEER_EDGE_SCAN_CAP == 25_000
    # v2's scoring machinery is GONE from the module surface
    assert not hasattr(subout_store, "COMPONENT_WEIGHTS")
    assert not hasattr(subout_store, "SELECTABLE_LENSES")


# ── request validation (fail-closed; v2 keys are dead) ────────────────────────
def test_v2_body_keys_are_rejected():
    for key in ("lenses", "codes_override", "code_type", "include_peers"):
        with pytest.raises(lance_store.MapCompileError, match="unknown body key"):
            subout_store.validate_request({"uei": TARGET, key: True})


def test_validation_fail_closed():
    with pytest.raises(lance_store.MapCompileError, match="must be an object"):
        subout_store.validate_request(["not", "a", "dict"])
    with pytest.raises(lance_store.MapCompileError, match="12-char"):
        subout_store.validate_request({"uei": "SHORT"})
    with pytest.raises(lance_store.MapCompileError, match="12-char"):
        subout_store.validate_request({})
    with pytest.raises(lance_store.MapCompileError, match="positive whole number"):
        subout_store.validate_request({"uei": TARGET, "limit": 0})
    with pytest.raises(lance_store.MapCompileError, match="positive whole number"):
        subout_store.validate_request({"uei": TARGET, "limit": True})


def test_validation_defaults_and_limit_cap():
    req = subout_store.validate_request({"uei": f"  {TARGET}  "})
    assert req == {"uei": TARGET, "limit": subout_store.DEFAULT_LIMIT,
                   "mode": "relationships"}
    assert subout_store.validate_request(
        {"uei": TARGET, "limit": 10_000})["limit"] == subout_store.LIMIT_CAP


def test_route_maps_compile_error_to_422_invalid_filter():
    from apps.catalyst_api.main import market_subout_opportunities
    with pytest.raises(HTTPException) as exc:
        market_subout_opportunities({"uei": TARGET, "lenses": ["sam_registered_naics"]})
    assert exc.value.status_code == 422
    assert "invalid filter" in exc.value.detail


# ── empty answers (a target with no relationships is an answer, not an error) ──
def test_target_with_no_relationships_serves_empty_with_reason(seams):
    out = _run({"uei": NO_SIGNAL_UEI})
    assert out["data"] == {"opportunities": []}
    assert out["meta"]["total"] == 0
    assert "no matching relationships" in out["meta"]["reason"]
    assert "no prime awards of its own" in out["meta"]["reason"]
    # the map anchor still rides meta on the empty answer
    assert out["meta"]["target_hq"] == {"latitude": 38.9072, "longitude": -77.0369}
    assert out["meta"]["recipeId"] == "subout_opportunities.v3"


# ── the in-process cache architecture ──────────────────────────────────────────
def test_cold_then_warm_cache_states_and_single_load(seams):
    out1 = _run({"uei": TARGET})
    assert out1["meta"]["cache_state"] == "cold"
    assert out1["meta"]["cache_build_ms"] is not None
    out2 = _run({"uei": TARGET})
    assert out2["meta"]["cache_state"] == "warm"
    assert seams.open_award_loads == 1 and seams.geo_loads == 1
    assert [o["generated_unique_award_id"] for o in out1["data"]["opportunities"]] == \
        [o["generated_unique_award_id"] for o in out2["data"]["opportunities"]]


def test_remote_reads_are_exactly_the_four_relationship_scans(seams):
    _run({"uei": TARGET})
    scan_uris = [u for u, _, _ in seams.scan_calls]
    assert scan_uris == [config.CONTRACT_SUBAWARD_URI,          # target's edges (A)
                         config.USASPENDING_AWARD_CANONICAL_URI]  # target's primes (B)
    stream_uris = {u for u, _, _, _ in seams.stream_calls}
    # peer-firm scans (award canonical) + peers' edges (subaward canonical)
    assert stream_uris == {config.USASPENDING_AWARD_CANONICAL_URI,
                           config.CONTRACT_SUBAWARD_URI}
    # NOTHING touches the v2 surfaces (Seams raises on any other uri — this test
    # passing means no cube / lanes / SAM / inferred read exists on the path)


def test_failed_cold_build_is_never_silent_and_next_request_retries(seams):
    seams.loader_error = OSError("gtm_open_awards not materialized yet")
    out = _run({"uei": TARGET})
    assert out["meta"]["cache_state"] == "failed"
    assert out["data"]["opportunities"] == [] and out["meta"]["total"] == 0
    assert any("cache build FAILED" in n for n in out["meta"]["notes"])
    assert subout_store.last_build_error() == (
        "OSError: gtm_open_awards not materialized yet")
    seams.loader_error = None
    out2 = _run({"uei": TARGET})
    assert out2["meta"]["cache_state"] == "cold" and out2["meta"]["total"] == 2
    assert subout_store.last_build_error() is None


def test_stale_cache_refreshes_in_background_without_blocking(seams, monkeypatch):
    _run({"uei": TARGET})
    assert seams.open_award_loads == 1
    monkeypatch.setattr(subout_store, "CACHE_TTL_S", 0.0)
    with subout_store._cache_lock:
        subout_store._caches_built_at = time.monotonic() - 10.0
    out = _run({"uei": TARGET})
    assert out["meta"]["cache_state"] == "warm"         # served, not blocked
    deadline = time.monotonic() + 5.0
    while seams.open_award_loads < 2 and time.monotonic() < deadline:
        time.sleep(0.01)
    assert seams.open_award_loads == 2


# ── the matching rules ─────────────────────────────────────────────────────────
def test_rule_a_worked_under_prime_end_to_end(seams):
    out = _run({"uei": TARGET})
    by_id = {o["generated_unique_award_id"]: o for o in out["data"]["opportunities"]}
    a1 = by_id["CONT_AWD_A1"]
    rule_a = [m for m in a1["matched_via"] if m["rule"] == "worked_under_prime"]
    assert len(rule_a) == 1
    # relationship $ is the SUM of the target's edges from that prime
    assert rule_a[0]["prime_uei"] == P1
    assert rule_a[0]["subaward_amt_from_prime"] == pytest.approx(6_000_000.0)
    assert rule_a[0]["edge_ct"] == 2
    assert rule_a[0]["last_action_date"] == "2026-01-15"
    # P_DEAD (a real former prime with no open awards) contributes nothing
    assert all("PIID_A5" != o["award_id_piid"] for o in out["data"]["opportunities"])


def test_rule_b_peer_subawardee_end_to_end(seams):
    out = _run({"uei": TARGET})
    by_id = {o["generated_unique_award_id"]: o for o in out["data"]["opportunities"]}
    # A2 is matched ONLY via rule B: PEER1 (shares (097, naics, 541690)) subbed on it
    a2 = by_id["CONT_AWD_A2"]
    assert len(a2["matched_via"]) == 1
    ev = a2["matched_via"][0]
    assert ev["rule"] == "peer_subawardee"
    assert ev["peer_ct"] == 1
    assert ev["peers"][0]["uei"] == PEER1
    assert ev["peers"][0]["shared_pairs"] == [
        {"agency_code": "097", "code_type": "naics", "code": "541690"}]
    # the expired award PEER1 also subbed on is dropped by the open-date re-check
    assert "CONT_AWD_A5_EXPIRED" not in by_id


def test_dual_matched_award_is_one_row_with_both_evidences(seams):
    out = _run({"uei": TARGET})
    ids = [o["generated_unique_award_id"] for o in out["data"]["opportunities"]]
    assert ids.count("CONT_AWD_A1") == 1                # union dedupe
    a1 = {o["generated_unique_award_id"]: o for o in out["data"]["opportunities"]}["CONT_AWD_A1"]
    rules = sorted(m["rule"] for m in a1["matched_via"])
    assert rules == ["peer_subawardee", "worked_under_prime"]
    peer_ev = next(m for m in a1["matched_via"] if m["rule"] == "peer_subawardee")
    assert peer_ev["peers"][0]["uei"] == PEER2          # via the shared PSC pair
    assert peer_ev["peers"][0]["shared_pairs"] == [
        {"agency_code": "097", "code_type": "psc", "code": "R499"}]


def test_peer_scans_exclude_the_target_itself(seams):
    out = _run({"uei": TARGET})
    for o in out["data"]["opportunities"]:
        for m in o["matched_via"]:
            if m["rule"] == "peer_subawardee":
                assert all(p["uei"] != TARGET for p in m["peers"])


def test_prime_pair_cap_is_applied_and_noted(seams, monkeypatch):
    monkeypatch.setattr(subout_store, "PRIME_PAIR_CAP", 1)
    notes: list[str] = []
    pairs = subout_store._target_prime_pairs(TARGET, notes)
    # top pair by the target's own prime $ survives (both $2M pairs tie; naics
    # sorts first deterministically)
    assert len(pairs) == 1
    assert pairs[0]["agency_code"] == "097" and pairs[0]["target_prime_obl"] == 2_000_000.0
    assert any("capped to the top 1" in n for n in notes)


# ── no scoring: flat list, honest total ────────────────────────────────────────
def test_flat_list_sorted_by_obligation_desc_no_score_keys(seams):
    out = _run({"uei": TARGET})
    opps = out["data"]["opportunities"]
    assert [o["generated_unique_award_id"] for o in opps] == \
        ["CONT_AWD_A1", "CONT_AWD_A2"]                  # $12M then $3M
    assert out["meta"]["total"] == 2
    for o in opps:
        assert "score" not in o and "components" not in o and "matched" not in o
        assert o["prime_uei"] and o["prime_name"]
    # meta carries no weights — there is no score
    assert "componentWeights" not in out["meta"]
    # peers key is gone from the data envelope (v2's opt-in stage is dead)
    assert set(out["data"]) == {"opportunities"}


def test_limit_caps_opportunities_but_total_is_honest(seams):
    out = _run({"uei": TARGET, "limit": 1})
    assert len(out["data"]["opportunities"]) == 1
    assert out["meta"]["total"] == 2
    assert out["data"]["opportunities"][0]["generated_unique_award_id"] == "CONT_AWD_A1"


# ── the map-ready wire ─────────────────────────────────────────────────────────
def test_map_ready_wire_coordinates_hq_distance_and_site(seams):
    out = _run({"uei": TARGET})
    assert out["meta"]["target_hq"] == {"latitude": 38.8816, "longitude": -77.0910}
    by_id = {o["generated_unique_award_id"]: o for o in out["data"]["opportunities"]}
    a1 = by_id["CONT_AWD_A1"]
    assert a1["latitude"] == 38.9586 and a1["longitude"] == -77.3570
    assert a1["pop_geo_precision"] == "zip5"
    assert a1["distance_mi"] is not None and 5 < a1["distance_mi"] < 25
    site = a1["nearest_federal_site"]
    assert site["site_name"] == "RESTON FEDERAL CENTER"
    assert site["latitude"] == 38.9586 and site["longitude"] == -77.3570
    assert site["distance_mi"] == 0.0
    # A2 has no centroid: coords + distance null, site honestly absent
    a2 = by_id["CONT_AWD_A2"]
    assert a2["latitude"] is None and a2["longitude"] is None
    assert a2["distance_mi"] is None and a2["nearest_federal_site"] is None
    # dates are JSON-shaped
    assert a1["period_of_performance_current_end_date"] == "2026-12-31"
    # per-stage timings name the v3 plan
    for stage in ("cache_ensure", "geo", "target_primes", "rule_a", "prime_pairs",
                  "peer_firms", "peer_edges", "assemble", "total"):
        assert stage in out["meta"]["timings_ms"], stage


# ── ported invariants (helpers unchanged from v2) ──────────────────────────────
def test_rows_for_uei_binary_search_equal_range():
    import pyarrow as pa
    tbl = pa.table({
        "uei": ["UEIPRIME0001", "UEITARGET001", "UEITARGET001", "UEIZZZZZZZZ9"],
        "code": ["111110", "541511", "541512", "999999"],
    }).sort_by("uei").combine_chunks()
    rows = subout_store._rows_for_uei(tbl, TARGET)
    assert [r["code"] for r in rows] == ["541511", "541512"]
    assert subout_store._rows_for_uei(tbl, "UEIABSENT001") == []
    empty = pa.table({"uei": pa.array([], type=pa.string())})
    assert subout_store._rows_for_uei(empty, TARGET) == []


def test_haversine_mi():
    assert subout_store._haversine_mi(38.8816, -77.0910, 38.8816, -77.0910) == 0.0
    d = subout_store._haversine_mi(38.9072, -77.0369, 34.0522, -118.2437)
    assert 2_270 < d < 2_320


def test_is_open_recheck():
    assert subout_store._is_open(
        {"period_of_performance_current_end_date": date(2026, 12, 31),
         "ordering_period_end_date": None}, TODAY)
    assert not subout_store._is_open(
        {"period_of_performance_current_end_date": date(2026, 7, 1),
         "ordering_period_end_date": None}, TODAY)
    # neither date → trusts the builder's open-at-as_of guarantee
    assert subout_store._is_open(
        {"period_of_performance_current_end_date": None,
         "ordering_period_end_date": None}, TODAY)


def test_site_rows_to_points_excludes_gsa_frpp_shadows_and_unlocated_rows():
    assert subout_store.FRPP_GSA_REPORTING_AGENCY_CODE == "47"
    rows = [
        {"site_source": "frpp_asset", "reporting_agency_code": "47",
         "site_name": "SHADOW", "site_type": "OFFICE", "latitude": 38.9,
         "longitude": -77.0, "lease_expiring_24mo_ct": None,
         "earliest_lease_expiration_date": None},
        {"site_source": "frpp_asset", "reporting_agency_code": "97",
         "site_name": "DOD DEPOT", "site_type": "WAREHOUSE", "latitude": 38.9,
         "longitude": -77.0, "lease_expiring_24mo_ct": None,
         "earliest_lease_expiration_date": None},
        {"site_source": "military_base", "reporting_agency_code": None,
         "site_name": "UNLOCATED", "site_type": "BASE", "latitude": None,
         "longitude": None, "lease_expiring_24mo_ct": None,
         "earliest_lease_expiration_date": None},
    ]
    points = subout_store._site_rows_to_points(rows)
    assert [p[2] for p in points] == ["DOD DEPOT"]


def test_unreachable_federal_sites_layer_degrades_to_empty(monkeypatch):
    def raiser(uri):
        raise OSError("federal_sites_lance not materialized yet")

    monkeypatch.setattr(subout_store, "_dataset", raiser)
    assert subout_store._load_federal_sites() == []


# ═══════════════════════════════════════════════════════════════════════════════
# COMBOS MODE (subout_combos.v1) — lookalike sub-combo expansion + PoP-state POV
# ═══════════════════════════════════════════════════════════════════════════════
def test_mode_validation_and_default():
    with pytest.raises(lance_store.MapCompileError, match="mode must be one of"):
        subout_store.validate_request({"uei": TARGET, "mode": "scores"})
    assert subout_store.validate_request({"uei": TARGET})["mode"] == "relationships"
    assert subout_store.validate_request(
        {"uei": TARGET, "mode": "combos"})["mode"] == "combos"


def test_default_mode_still_serves_relationships(seams):
    out = _run({"uei": TARGET})
    assert out["meta"]["recipeId"] == "subout_opportunities.v3"
    assert "pov" not in out["meta"]


def test_combos_mode_end_to_end(seams):
    out = _run({"uei": TARGET, "mode": "combos"})
    meta, data = out["meta"], out["data"]
    assert meta["recipeId"] == "subout_combos.v1" and meta["mode"] == "combos"

    pov = meta["pov"]
    # POV 1: the target's demonstrated sub combos, $-ranked
    assert pov["target_combos"][0] == {"naics": "541690", "psc": "R499",
                                       "sub_amt": 6_000_000.0, "edge_ct": 2}
    # POV 2: lookalikes share the combos AND prime — NOPRIME ($8M overlap, never
    # primed) is SKIPPED; ranking by overlap $ among the primed
    lk_ueis = [lk["uei"] for lk in pov["lookalikes"]]
    assert lk_ueis == [LOOK1, LOOK2, LOOK3]
    assert NOPRIME not in lk_ueis
    assert pov["lookalikes"][0]["legal_business_name"] == "ALPHA BOTHSIDER LLC"
    assert pov["lookalikes"][0]["overlap_amt"] == 9_000_000.0
    # POV 3: the lookalikes' OTHER sub combos (target's own combo excluded)
    exp = {(e["naics"], e["psc"]): e for e in pov["expansion_combos"]}
    assert ("541690", "R499") not in exp
    assert exp[("236220", "Y1AA")]["lookalike_sub_amt"] == 4_000_000.0
    assert exp[("236220", "Y1AA")]["lookalikes"] == [LOOK1, LOOK2]
    assert exp[("541519", "D302")]["lookalikes"] == [LOOK3]
    # POV 4: geography default = the target's historical PoP states, basis stated
    assert pov["pop_states"] == ["CA", "MD", "VA"]
    assert "geography DEFAULT" in pov["pop_state_basis"]

    # Dots: combo ∈ (target ∪ expansion), open-checked, state-filtered, $-sorted.
    # A6 (541690×R499, CA, $20M) target combo; A1 (541519×D302, VA, $12M) expansion;
    # A2 (236220×Y1AA, state None) and A7 (TX) excluded by the geography default;
    # A5 (expired) dropped by the open-date re-check.
    ids = [o["generated_unique_award_id"] for o in data["opportunities"]]
    assert ids == ["CONT_AWD_A6", "CONT_AWD_A1"]
    assert meta["total"] == 2
    by_id = {o["generated_unique_award_id"]: o for o in data["opportunities"]}
    a6 = by_id["CONT_AWD_A6"]
    assert a6["matched_via"] == [{
        "rule": "target_sub_combo", "combo": {"naics": "541690", "psc": "R499"},
        "target_sub_amt": 6_000_000.0, "target_edge_ct": 2}]
    a1 = by_id["CONT_AWD_A1"]
    assert a1["matched_via"] == [{
        "rule": "lookalike_sub_combo", "combo": {"naics": "541519", "psc": "D302"},
        "lookalike_sub_amt": 2_000_000.0, "lookalikes": [LOOK3]}]
    # geography exclusions are counted on the wire, never silent
    assert any("excluded 2 open awards" in n for n in meta["notes"])
    # no scoring anywhere
    for o in data["opportunities"]:
        assert "score" not in o and "components" not in o
    # map-ready fields ride as in every mode
    assert a6["latitude"] == 34.05 and meta["target_hq"] is not None
    for stage in ("cache_ensure", "geo", "combo_profile", "pop_states",
                  "lookalikes", "expansion", "assemble", "total"):
        assert stage in meta["timings_ms"], stage


def test_combos_mode_no_sub_history_is_an_answer(seams):
    out = _run({"uei": NO_SIGNAL_UEI, "mode": "combos"})
    assert out["meta"]["total"] == 0
    assert "no demonstrated sub combos" in out["meta"]["reason"]
    assert out["meta"]["pov"]["target_combos"] == []
    assert out["meta"]["target_hq"] is not None    # the anchor still rides
