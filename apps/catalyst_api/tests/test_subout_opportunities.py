"""Unit tests for the subout-opportunities recipe — pure, no R2 / network.

Pins (1) the fail-closed request contract (unknown body key / bad lens / bad code /
mistyped knob → 422-class MapCompileError, never a silent ignore), (2) the IN-PROCESS
cache architecture (loader seams stubbed; cold→warm state transitions; the hot path
never touches the award spine or the cube remotely; TTL refresh runs in the background
and never blocks; a failed cold build degrades and retries), (3) the recipe plan
(lens probes with the inferred cap, cube-marginal matching, open-award index hits,
HQ-to-PoP distance actually computed and NONZERO), (4) component-math determinism
(every normalization pinned to hand-computed values; score = Σ contributions with the
published weights), and (5) the wire envelope (recipeId + registryVersion +
cache_state/cache_build_ms + per-stage timings + prime_uei/prime_name on every row).
"""

from __future__ import annotations

import math
import time
from datetime import date

import pytest
from fastapi import HTTPException

from apps.catalyst_api.src import config, lance_store, subout_store

TODAY = date(2026, 7, 6)

TARGET = "UEITARGET001"
NO_SIGNAL_UEI = "UEINOSIGNAL1"
P1, P2, P3, P4 = "UEIPRIME0001", "UEIPRIME0002", "UEIPRIME0003", "UEIPRIME0004"
PEER1, PEER2 = "UEIPEER00001", "UEIPEER00002"

# ── Remote point-lookup fixtures (per-request BTREE uei probes) ────────────────
LANE_ROWS = [
    {"uei": TARGET, "side": "prime", "code_type": "naics", "code": "541512",
     "obl_lifetime": 5_000_000.0},
    {"uei": TARGET, "side": "sub", "code_type": "naics", "code": "541511",
     "obl_lifetime": 1_200_000.0},
    {"uei": TARGET, "side": "sub", "code_type": "psc", "code": "R425",
     "obl_lifetime": 300_000.0},
]
SAM_ROWS = [
    {"uei": TARGET, "primary_naics": "541512", "naics_codes": ["541512", "238220"]},
]
INFERRED_ROWS = [
    {"uei": TARGET, "code_type": "naics", "code": "562910",
     "supporting_bothsider_firm_ct": 10},
]
GEO_ROWS = [
    {"uei": TARGET, "latitude": 38.8816, "longitude": -77.0910},
    {"uei": NO_SIGNAL_UEI, "latitude": 38.9072, "longitude": -77.0369},
]
EVIDENCE_ROWS = [
    {"subawardee_uei": PEER1, "code": "541512"},
    {"subawardee_uei": TARGET, "code": "541512"},   # the target is never its own peer
    {"subawardee_uei": PEER1, "code": "238220"},    # dupe UEI collapses
    {"subawardee_uei": PEER2, "code": "238220"},
]

# ── In-process cache fixtures (loader seams) ───────────────────────────────────
# The row tuples _iter_marginal_rows yields off the PRE-AGGREGATED
# gtm_primes_by_recipient_code table (MARGINAL_COLUMNS order):
# (code_type, code, prime, subaward_amt_total, subaward_edge_ct,
#  distinct_recipient_ct, last_subaward_action_date).
MARGINAL_ROWS = [
    ("naics", "541512", P1, 40_000_000.0, 120, 35, date(2026, 5, 15)),
    ("naics", "541511", P1, 5_000_000.0, 20, 8, date(2026, 2, 1)),
    ("naics", "238220", P2, 2_500_000.0, 15, 12, date(2025, 11, 3)),
    ("naics", "562910", P3, 1_000_000.0, 4, 5, date(2026, 1, 20)),
    ("naics", "611430", P4, 2_000_000.0, 6, 4, date(2026, 3, 1)),
]
# gtm_open_awards rows: pre-joined PoP geo rides each row. HQ = (38.8816, -77.0910).
# A1 = zip5 ~15 mi from HQ (Reston, VA — distance must be NONZERO); A2 has NO centroid
# (nulls); A3 county-precision (distance computed, proximity NEUTRAL); A4 zip5 across
# the country (decays to 0); A5 belongs to P1 but is EXPIRED (the cache may be hours
# old within TTL — the request-time open-date re-check must drop it).
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
    {"generated_unique_award_id": "CONT_AWD_A3", "award_id_piid": "PIID_A3",
     "recipient_uei": P3, "recipient_name": "PRIME THREE CO",
     "naics_code": "562910", "product_or_service_code": "F108",
     "total_obligation": 900_000.0, "base_and_all_options_value": 2_000_000.0,
     "subaward_count": 0, "total_subaward_amount": 0.0,
     "subcontracting_plan_code": "B",
     "period_of_performance_current_end_date": date(2026, 8, 1),
     "ordering_period_end_date": None,
     "award_or_idv_flag": "AWARD", "idv_type_code": None,
     "type_of_set_aside_code": "SBA",
     "awarding_agency_code": "012", "awarding_agency_name": "Department of Agriculture",
     "primary_place_of_performance_state_code": "VA",
     "latitude": 37.5407, "longitude": -77.4360, "geo_precision": "county"},
    {"generated_unique_award_id": "CONT_AWD_A4", "award_id_piid": "PIID_A4",
     "recipient_uei": P4, "recipient_name": "PRIME FOUR LTD",
     "naics_code": "611430", "product_or_service_code": "U008",
     "total_obligation": 500_000.0, "base_and_all_options_value": 1_500_000.0,
     "subaward_count": 0, "total_subaward_amount": 0.0,
     "subcontracting_plan_code": None,
     "period_of_performance_current_end_date": date(2026, 10, 1),
     "ordering_period_end_date": None,
     "award_or_idv_flag": "AWARD", "idv_type_code": None,
     "type_of_set_aside_code": None,
     "awarding_agency_code": "091", "awarding_agency_name": "Department of Education",
     "primary_place_of_performance_state_code": "CA",
     "latitude": 34.0522, "longitude": -118.2437, "geo_precision": "zip5"},
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
]


class Seams:
    """Recording fakes for the subout_store I/O seams: the per-request remote
    point-lookups (_scan_to_pylist / _stream_rows) AND the cache loader seams
    (_load_open_awards / _load_cube_marginal). Predicate handling mirrors the exact
    shapes the executor emits by quoted-literal inspection — enough to pin the PLAN,
    not Lance semantics."""

    def __init__(self):
        self.scan_calls: list[tuple[str, tuple[str, ...], str | None]] = []
        self.stream_calls: list[tuple[str, tuple[str, ...], str | None, int]] = []
        self.open_award_loads = 0
        self.cube_loads = 0
        self.loader_error: Exception | None = None
        self.evidence_error: Exception | None = None

    @staticmethod
    def _match(rows, predicate, key, code_type_col=None):
        out = []
        for row in rows:
            if f"'{row[key]}'" not in (predicate or ""):
                continue
            if code_type_col and f"{code_type_col} = '" in (predicate or ""):
                if f"{code_type_col} = '{row.get(code_type_col)}'" not in predicate:
                    continue
            out.append(row)
        return out

    def scan_to_pylist(self, uri, columns, predicate):
        self.scan_calls.append((uri, tuple(columns), predicate))
        if uri == config.GTM_ENTITY_CODE_LANES_URI:
            rows = self._match(LANE_ROWS, predicate, "uei", "code_type")
        elif uri == config.GTM_SAM_ENTITIES_URI:
            rows = self._match(SAM_ROWS, predicate, "uei")
        elif uri == config.GTM_INFERRED_PRIMEABLE_URI:
            rows = self._match(INFERRED_ROWS, predicate, "uei", "code_type")
        elif uri == config.GTM_ENTITY_GEO_URI:
            rows = self._match(GEO_ROWS, predicate, "uei")
        else:
            raise AssertionError(f"unexpected remote scan uri {uri}")
        return [{c: r.get(c) for c in columns} for r in rows]

    def stream_rows(self, uri, columns, predicate, limit):
        self.stream_calls.append((uri, tuple(columns), predicate, limit))
        if uri == config.GTM_SUBAWARD_RECIPIENT_CODE_EVIDENCE_URI:
            if self.evidence_error is not None:
                raise self.evidence_error
            rows = self._match(EVIDENCE_ROWS, predicate, "code")
        else:
            raise AssertionError(f"unexpected remote stream uri {uri}")
        return [{c: r.get(c) for c in columns} for r in rows][:limit]

    def load_open_awards(self):
        if self.loader_error is not None:
            raise self.loader_error
        self.open_award_loads += 1
        return [dict(r) for r in OPEN_AWARD_ROWS]

    def iter_marginal_rows(self):
        if self.loader_error is not None:
            raise self.loader_error
        self.cube_loads += 1
        return iter(MARGINAL_ROWS)


@pytest.fixture()
def seams(monkeypatch):
    s = Seams()
    monkeypatch.setattr(subout_store, "_scan_to_pylist", s.scan_to_pylist)
    monkeypatch.setattr(subout_store, "_stream_rows", s.stream_rows)
    monkeypatch.setattr(subout_store, "_load_open_awards", s.load_open_awards)
    monkeypatch.setattr(subout_store, "_iter_marginal_rows", s.iter_marginal_rows)
    subout_store.reset_caches_for_tests()
    yield s
    subout_store.reset_caches_for_tests()


def _run(body, **kw):
    return subout_store.execute_subout_opportunities(body, today=TODAY, **kw)


# ── recipe constants (the versioned contract) ─────────────────────────────────
def test_recipe_id_and_weights_are_the_published_contract():
    assert subout_store.RECIPE_ID == "subout_opportunities.v1"
    assert set(subout_store.COMPONENT_WEIGHTS) == {
        "prime_subout_history", "award_already_subbing", "subcontracting_plan",
        "lens_strength", "proximity", "expiring_window"}
    assert sum(subout_store.COMPONENT_WEIGHTS.values()) == pytest.approx(1.0)
    assert all(w > 0 for w in subout_store.COMPONENT_WEIGHTS.values())
    # every lens name is self-describing vocabulary, caller_declared never selectable
    assert subout_store.SELECTABLE_LENSES == (
        "awarded_prime_contracts_in_code", "delivered_subawards_under_code",
        "sam_registered_naics", "inferred_primeable")
    assert subout_store.LENS_CALLER_DECLARED not in subout_store.SELECTABLE_LENSES


# ── request validation (fail-closed) ──────────────────────────────────────────
def test_unknown_body_key_rejected():
    with pytest.raises(lance_store.MapCompileError, match="unknown body key"):
        subout_store.validate_request({"uei": TARGET, "naics": ["541512"]})


def test_validation_fail_closed():
    ok = {"uei": TARGET}
    for bad in (
        {},                                             # uei required
        {"uei": "SHORT"},                               # 12-char alnum only
        {"uei": 12},                                    # not a string
        {**ok, "lenses": []},                           # empty lens list (omit for all)
        {**ok, "lenses": ["primed"]},                   # off-vocabulary lens
        {**ok, "lenses": ["caller_declared"]},          # override-only lens, not selectable
        {**ok, "lenses": "sam_registered_naics"},       # not a list
        {**ok, "codes_override": "541512"},             # not a list
        {**ok, "codes_override": ["541512' OR '1'='1"]},  # injection-shaped code
        {**ok, "code_type": "duns"},                    # off-enum code system
        {**ok, "limit": 0},                             # positive whole number only
        {**ok, "limit": "50"},
        {**ok, "limit": True},
        {**ok, "include_peers": "yes"},                 # boolean only
        "UEITARGET001",                                 # body not an object
    ):
        with pytest.raises(lance_store.MapCompileError):
            subout_store.validate_request(bad)


def test_validation_defaults_and_limit_cap():
    req = subout_store.validate_request({"uei": f"  {TARGET}  "})
    assert req == {"uei": TARGET, "lenses": list(subout_store.SELECTABLE_LENSES),
                   "codes_override": [], "code_type": None,
                   "limit": subout_store.DEFAULT_LIMIT,
                   # peers are OPT-IN (the one remote non-point query left)
                   "include_peers": False, "peers_defaulted": True}
    assert subout_store.validate_request({"uei": TARGET, "limit": 999})["limit"] == 200
    explicit = subout_store.validate_request({"uei": TARGET, "include_peers": True})
    assert explicit["include_peers"] is True and explicit["peers_defaulted"] is False


def test_route_maps_compile_error_to_422_invalid_filter():
    from apps.catalyst_api.main import market_subout_opportunities

    with pytest.raises(HTTPException) as exc:
        market_subout_opportunities({"uei": TARGET, "bogus_knob": 1})
    assert exc.value.status_code == 422
    assert str(exc.value.detail).startswith("invalid filter:")


# ── the empty-market answer (200, never an error) ─────────────────────────────
def test_uei_with_no_code_signals_serves_empty_with_reason(seams):
    out = _run({"uei": NO_SIGNAL_UEI})
    assert out["data"] == {"opportunities": [], "peers": []}
    assert out["meta"]["total"] == 0
    assert out["meta"]["reason"] == "uei has no code signals"
    assert out["meta"]["recipeId"] == "subout_opportunities.v1"
    # short-circuits BEFORE the caches: no loads, no streams
    assert seams.open_award_loads == 0 and seams.cube_loads == 0
    assert seams.stream_calls == []
    assert "probe_codes" in out["meta"]["timings_ms"]
    assert "total" in out["meta"]["timings_ms"]


# ── the in-process cache architecture ──────────────────────────────────────────
def test_cold_then_warm_cache_states_and_single_load(seams):
    out1 = _run({"uei": TARGET})
    assert out1["meta"]["cache_state"] == "cold"
    assert out1["meta"]["cache_build_ms"] is not None
    out2 = _run({"uei": TARGET})
    assert out2["meta"]["cache_state"] == "warm"
    # both loaders ran EXACTLY once across the two requests
    assert seams.open_award_loads == 1 and seams.cube_loads == 1
    # identical answers off the same cache
    assert [o["generated_unique_award_id"] for o in out1["data"]["opportunities"]] == \
        [o["generated_unique_award_id"] for o in out2["data"]["opportunities"]]


def test_hot_path_makes_no_remote_scans_beyond_point_lookups(seams):
    _run({"uei": TARGET})
    # per-request remote reads are the lens probes + the HQ geo point-lookup ONLY —
    # the award spine, the cube, and the centroids are never scanned per request.
    allowed = {config.GTM_ENTITY_CODE_LANES_URI, config.GTM_SAM_ENTITIES_URI,
               config.GTM_INFERRED_PRIMEABLE_URI, config.GTM_ENTITY_GEO_URI}
    assert {u for u, _, _ in seams.scan_calls} <= allowed
    assert seams.stream_calls == []                     # peers off by default
    for _, _, pred in seams.scan_calls:
        assert f"uei = '{TARGET}'" in pred              # every remote read is a uei probe


def test_failed_cold_build_is_never_silent_and_next_request_retries(seams):
    # THE production incident pin: a dead build must never sit as an unexplained
    # 'unavailable' — the state is 'failed' and the ERROR STRING rides the wire.
    seams.loader_error = OSError("gtm_primes_by_recipient_code not materialized yet")
    out = _run({"uei": TARGET})
    assert out["meta"]["cache_state"] == "failed"
    assert out["meta"]["cache_build_ms"] is None
    assert out["data"]["opportunities"] == [] and out["meta"]["total"] == 0
    assert any("cache build FAILED" in n and
               "OSError: gtm_primes_by_recipient_code not materialized yet" in n
               for n in out["meta"]["notes"])
    assert subout_store.last_build_error() == (
        "OSError: gtm_primes_by_recipient_code not materialized yet")
    # the failure was NOT cached: clearing the fault lets the next request build
    seams.loader_error = None
    out2 = _run({"uei": TARGET})
    assert out2["meta"]["cache_state"] == "cold" and out2["meta"]["total"] == 3
    assert subout_store.last_build_error() is None      # cleared on success


def test_stale_cache_refreshes_in_background_without_blocking(seams, monkeypatch):
    _run({"uei": TARGET})                               # cold build
    assert seams.open_award_loads == 1
    # age the cache past TTL; the next request must serve WARM immediately (the old
    # cache) while a background thread rebuilds.
    monkeypatch.setattr(subout_store, "CACHE_TTL_S", 0.0)
    with subout_store._cache_lock:
        subout_store._caches_built_at = time.monotonic() - 10.0
    out = _run({"uei": TARGET})
    assert out["meta"]["cache_state"] == "warm"         # served, not blocked
    deadline = time.monotonic() + 5.0
    while seams.open_award_loads < 2 and time.monotonic() < deadline:
        time.sleep(0.01)
    assert seams.open_award_loads == 2                  # the background rebuild ran
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        with subout_store._cache_lock:
            if not subout_store._cache_refreshing:
                break
        time.sleep(0.01)
    with subout_store._cache_lock:
        assert subout_store._cache_refreshing is False


def test_index_builders_are_pure_and_complete():
    by_prime, by_naics, by_psc = subout_store._index_open_awards(OPEN_AWARD_ROWS)
    assert set(by_prime) == {P1, P2, P3, P4}
    assert [OPEN_AWARD_ROWS[i]["generated_unique_award_id"] for i in by_prime[P1]] == \
        ["CONT_AWD_A1", "CONT_AWD_A5_EXPIRED"]
    assert by_naics["541519"] == by_prime[P1]
    assert by_psc["D302"] == by_prime[P1]
    cube, n = subout_store._group_marginal(iter(MARGINAL_ROWS))
    assert n == len(MARGINAL_ROWS)
    # COLUMNAR block: (primes, amt_totals, edge_cts, distinct_cts, last_dates)
    primes, amts, edges, distincts, lasts = cube[("naics", "541512")]
    assert primes == (P1,)
    assert list(amts) == [40_000_000.0] and list(edges) == [120]
    assert list(distincts) == [35] and lasts == (date(2026, 5, 15),)
    # NULL-guarded: rows without a code or prime are dropped, not indexed
    cube2, n2 = subout_store._group_marginal(iter(
        [("naics", "", P1, 1.0, 1, 1, None), ("naics", "541512", None, 1.0, 1, 1, None)]))
    assert cube2 == {} and n2 == 0


# ── default all-lens flow ──────────────────────────────────────────────────────
def test_default_all_lens_flow_end_to_end(seams):
    out = _run({"uei": TARGET})
    meta, data = out["meta"], out["data"]
    assert meta["recipeId"] == "subout_opportunities.v1"
    assert meta["registryVersion"]
    assert meta["lenses"] == list(subout_store.SELECTABLE_LENSES)
    for stage in ("probe_codes", "cache_ensure", "cube_match", "open_awards",
                  "geo", "score", "total"):
        assert stage in meta["timings_ms"], stage
    assert "peers" not in meta["timings_ms"]            # opt-in stage did not run
    # P1/P2/P3 matched (P4's 611430 is nobody's probe code without an override);
    # P1's EXPIRED second award is dropped by the request-time open-date re-check.
    ids = [o["generated_unique_award_id"] for o in data["opportunities"]]
    assert set(ids) == {"CONT_AWD_A1", "CONT_AWD_A2", "CONT_AWD_A3"}
    assert meta["total"] == 3
    assert ids[0] == "CONT_AWD_A1"                      # strongest signal ranks first
    by_id = {o["generated_unique_award_id"]: o for o in data["opportunities"]}
    a1 = by_id["CONT_AWD_A1"]
    # THE two live-smoke bugs, pinned: the prime identity is populated...
    assert a1["prime_uei"] == P1 and a1["prime_name"] == "PRIME ONE LLC"
    for o in data["opportunities"]:
        assert o["prime_uei"] and o["prime_name"]
    # ...and the distance is actually computed — NONZERO (HQ Arlington → Reston zip5)
    assert a1["distance_mi"] is not None and a1["distance_mi"] > 0
    assert 5 < a1["distance_mi"] < 25
    assert a1["award_id_piid"] == "PIID_A1"
    assert a1["awarding_agency_name"] == "Department of Defense"
    assert a1["period_of_performance_current_end_date"] == "2026-12-31"  # JSON-shaped
    assert a1["pop_state_code"] == "VA" and a1["award_or_idv_flag"] == "AWARD"
    # matched evidence carries BOTH sides: the target's lens + the cube marginal cell
    matched = {(m["lens"], m["code"]) for m in a1["matched"]}
    assert ("awarded_prime_contracts_in_code", "541512") in matched
    assert ("sam_registered_naics", "541512") in matched
    assert ("delivered_subawards_under_code", "541511") in matched
    for m in a1["matched"]:
        assert m["evidence"]["subaward_amt_total"] is not None
        assert m["evidence"]["recipient_code_type"] == "naics"
    # every score rides with its explicit components; score = Σ contributions
    for o in data["opportunities"]:
        assert [c["name"] for c in o["components"]] == list(subout_store.COMPONENT_WEIGHTS)
        for c in o["components"]:
            assert c["weight"] == subout_store.COMPONENT_WEIGHTS[c["name"]]
        assert o["score"] == pytest.approx(
            sum(c["contribution"] for c in o["components"]), abs=1e-6)
    # A2 carries no PoP centroid: distance null, proximity neutral
    a2 = by_id["CONT_AWD_A2"]
    assert a2["distance_mi"] is None and a2["pop_geo_precision"] is None
    assert a2["prime_uei"] == P2 and a2["prime_name"] == "PRIME TWO INC"
    # A3 is county-precision: distance computed but proximity stays NEUTRAL
    a3 = by_id["CONT_AWD_A3"]
    assert a3["distance_mi"] is not None and a3["pop_geo_precision"] == "county"
    # peers are OPT-IN now — absent by default, with the note on the wire
    assert data["peers"] == []
    assert any("peers omitted by default" in n for n in meta["notes"])


def test_component_math_is_deterministic_on_the_wire(seams):
    out = _run({"uei": TARGET})
    a1 = {o["generated_unique_award_id"]: o for o in out["data"]["opportunities"]}["CONT_AWD_A1"]
    comps = {c["name"]: c for c in a1["components"]}
    w = subout_store.COMPONENT_WEIGHTS
    # prime_subout_history: Σ matched marginal $ for P1 = 40M + 5M, log-scaled to $1B
    assert comps["prime_subout_history"]["raw_value"] == pytest.approx(45_000_000.0)
    assert comps["prime_subout_history"]["contribution"] == pytest.approx(
        w["prime_subout_history"] * math.log10(1 + 45_000_000.0) / 9.0, abs=1e-6)
    # award_already_subbing: subaward_count 12 → full weight
    assert comps["award_already_subbing"]["raw_value"] is True
    assert comps["award_already_subbing"]["contribution"] == pytest.approx(
        w["award_already_subbing"])
    # subcontracting_plan 'F' (individual plan) → full weight
    assert comps["subcontracting_plan"]["raw_value"] == "F"
    assert comps["subcontracting_plan"]["contribution"] == pytest.approx(
        w["subcontracting_plan"])
    # lens_strength: strongest matched lens = awarded prime lane, $5M lifetime
    assert comps["lens_strength"]["contribution"] == pytest.approx(
        w["lens_strength"] * math.log10(1 + 5_000_000.0) / 9.0, abs=1e-6)
    # proximity: zip5 NONZERO distance → linear decay 1 - d/500
    d = a1["distance_mi"]
    assert d > 0
    assert comps["proximity"]["raw_value"] == d
    assert comps["proximity"]["contribution"] == pytest.approx(
        w["proximity"] * (1 - d / 500.0), abs=1e-6)
    # expiring_window: PoP ends 2026-12-31, 178 days from TODAY
    assert comps["expiring_window"]["raw_value"] == 178
    assert comps["expiring_window"]["contribution"] == pytest.approx(
        w["expiring_window"] * (1 - 178 / 1080), abs=1e-6)
    assert a1["score"] == pytest.approx(sum(c["contribution"] for c in a1["components"]))


# ── lenses / codes_override / code_type / inferred-cap parameters ─────────────
def test_lenses_param_filters_probe_codes(seams):
    out = _run({"uei": TARGET, "lenses": ["sam_registered_naics"]})
    ids = {o["generated_unique_award_id"] for o in out["data"]["opportunities"]}
    # SAM codes are 541512 + 238220 → P1 and P2 only; the inferred-only P3 drops out
    assert ids == {"CONT_AWD_A1", "CONT_AWD_A2"}
    for o in out["data"]["opportunities"]:
        assert {m["lens"] for m in o["matched"]} == {"sam_registered_naics"}
    # P1's 541511 cell no longer matches: the history $ is the 541512 cell alone
    a1 = {o["generated_unique_award_id"]: o for o in out["data"]["opportunities"]}["CONT_AWD_A1"]
    comps = {c["name"]: c for c in a1["components"]}
    assert comps["prime_subout_history"]["raw_value"] == pytest.approx(40_000_000.0)
    assert out["meta"]["lenses"] == ["sam_registered_naics"]


def test_codes_override_probes_as_caller_declared_lens(seams):
    out = _run({"uei": NO_SIGNAL_UEI, "codes_override": ["611430"]})
    ids = [o["generated_unique_award_id"] for o in out["data"]["opportunities"]]
    assert ids == ["CONT_AWD_A4"]
    a4 = out["data"]["opportunities"][0]
    assert a4["prime_uei"] == P4 and a4["prime_name"] == "PRIME FOUR LTD"
    assert a4["matched"] == [{
        "lens": "caller_declared", "code": "611430",
        "evidence": {"declared_by_caller": True,
                     "recipient_code_type": "naics",
                     "subaward_edge_ct": 6, "subaward_amt_total": 2_000_000.0,
                     "distinct_recipient_ct": 4,
                     "last_subaward_action_date": "2026-03-01"}}]
    comps = {c["name"]: c for c in a4["components"]}
    # a caller-declared code is a claim, never dollars: fixed strength
    assert comps["lens_strength"]["contribution"] == pytest.approx(
        subout_store.COMPONENT_WEIGHTS["lens_strength"] * 0.5)
    # A4 is zip5 far away (DC-area HQ → LA): proximity decays to 0 beyond 500 mi
    assert a4["distance_mi"] > 2_000
    assert comps["proximity"]["contribution"] == 0.0


def test_code_type_restriction_filters_probe_and_cube(seams):
    out = _run({"uei": TARGET, "code_type": "psc"})
    # the target's only PSC signal is the R425 sub lane — no cube cell carries it
    assert out["data"]["opportunities"] == []
    assert out["meta"]["total"] == 0
    lane_preds = [p for u, _, p in seams.scan_calls
                  if u == config.GTM_ENTITY_CODE_LANES_URI]
    assert lane_preds == [f"uei = '{TARGET}' AND code_type = 'psc'"]


def test_inferred_probe_capped_at_top_supported_codes(seams, monkeypatch):
    monkeypatch.setattr(subout_store, "INFERRED_PROBE_CAP", 1)
    # give the target a second, WEAKER inferred code — the cap must keep the stronger
    monkeypatch.setattr(
        subout_store, "_scan_to_pylist",
        lambda uri, cols, pred: (
            [{"uei": TARGET, "code_type": "naics", "code": "562910",
              "supporting_bothsider_firm_ct": 10},
             {"uei": TARGET, "code_type": "naics", "code": "611430",
              "supporting_bothsider_firm_ct": 2}]
            if uri == config.GTM_INFERRED_PRIMEABLE_URI else []))
    notes: list[str] = []
    entries = subout_store._probe_codes(TARGET, ["inferred_primeable"], None, [], notes)
    assert [e["code"] for e in entries] == ["562910"]   # top-by-support kept
    assert any("capped to the top 1" in n for n in notes)


def test_include_peers_true_runs_the_remote_evidence_query(seams):
    out = _run({"uei": TARGET, "include_peers": True})
    # top matched codes by distinct_recipient_ct: 541512 (35), 238220 (12), 541511 (8)
    assert out["data"]["peers"] == [{"uei": PEER1, "shared_code": "541512"},
                                    {"uei": PEER2, "shared_code": "238220"}]
    assert "peers" in out["meta"]["timings_ms"]
    assert not any("peers omitted" in n for n in out["meta"].get("notes", []))
    ev_calls = [c for c in seams.stream_calls
                if c[0] == config.GTM_SUBAWARD_RECIPIENT_CODE_EVIDENCE_URI]
    assert len(ev_calls) == 1
    assert ev_calls[0][3] == subout_store.PEER_EVIDENCE_ROW_SCAN


def test_unreachable_evidence_table_degrades_to_no_peers(seams):
    seams.evidence_error = OSError("dataset not found")
    out = _run({"uei": TARGET, "include_peers": True})
    assert out["meta"]["total"] == 3                    # opportunities unaffected
    assert out["data"]["peers"] == []
    assert any("gtm_subaward_recipient_code_evidence unreachable" in n
               for n in out["meta"]["notes"])


def test_limit_caps_opportunities_but_total_is_honest(seams):
    out = _run({"uei": TARGET, "limit": 1})
    assert len(out["data"]["opportunities"]) == 1
    assert out["meta"]["total"] == 3
    assert out["data"]["opportunities"][0]["generated_unique_award_id"] == "CONT_AWD_A1"


# ── normalization math (pure helpers, hand-computed pins) ─────────────────────
def test_log_dollar_norm():
    assert subout_store._log_dollar_norm(0) == 0.0
    assert subout_store._log_dollar_norm(None) == 0.0
    assert subout_store._log_dollar_norm(-5) == 0.0
    assert subout_store._log_dollar_norm(999_999) == pytest.approx(6 / 9, abs=1e-6)
    assert subout_store._log_dollar_norm(10**9) == pytest.approx(1.0, abs=1e-6)
    assert subout_store._log_dollar_norm(10**12) == 1.0     # capped


def test_lens_strength_by_lens():
    assert subout_store._lens_strength(
        "awarded_prime_contracts_in_code", {"obl_lifetime": 999_999}
    ) == pytest.approx(6 / 9, abs=1e-6)
    assert subout_store._lens_strength(
        "delivered_subawards_under_code", {"obl_lifetime": 0}) == 0.0
    assert subout_store._lens_strength(
        "inferred_primeable", {"supporting_bothsider_firm_ct": 10}) == 0.5
    assert subout_store._lens_strength(
        "inferred_primeable", {"supporting_bothsider_firm_ct": 40}) == 1.0
    assert subout_store._lens_strength("sam_registered_naics", {}) == 0.5
    assert subout_store._lens_strength("caller_declared", {}) == 0.5


def test_plan_norm():
    for required in ("C", "D", "E", "F", "G", "H", "f"):
        assert subout_store._plan_norm(required) == 1.0
    assert subout_store._plan_norm("B") == 0.25
    assert subout_store._plan_norm("A") == 0.0
    assert subout_store._plan_norm(None) == 0.0
    assert subout_store._plan_norm("") == 0.0


def test_proximity_norm_neutral_unless_zip5_with_distance():
    assert subout_store._proximity_norm(None, "zip5") == 0.5
    assert subout_store._proximity_norm(10.0, "county") == 0.5
    assert subout_store._proximity_norm(10.0, None) == 0.5
    assert subout_store._proximity_norm(0.0, "zip5") == 1.0
    assert subout_store._proximity_norm(100.0, "zip5") == pytest.approx(0.8)
    assert subout_store._proximity_norm(600.0, "zip5") == 0.0


def test_expiring_norm():
    assert subout_store._expiring_norm(0) == 1.0
    assert subout_store._expiring_norm(540) == pytest.approx(0.5)
    assert subout_store._expiring_norm(1080) == 0.0
    assert subout_store._expiring_norm(5000) == 0.0
    assert subout_store._expiring_norm(None) == 0.0
    assert subout_store._expiring_norm(-1) == 0.0


def test_haversine_mi():
    assert subout_store._haversine_mi(38.8816, -77.0910, 38.8816, -77.0910) == 0.0
    # DC ↔ LA ≈ 2,295 statute miles
    d = subout_store._haversine_mi(38.9072, -77.0369, 34.0522, -118.2437)
    assert 2_270 < d < 2_320
    # Arlington HQ ↔ Reston PoP is a NONZERO short hop (the live dist=None bug pin)
    d2 = subout_store._haversine_mi(38.8816, -77.0910, 38.9586, -77.3570)
    assert 5 < d2 < 25
