"""sub_dossier_v1 — compile tests (pure, no network) + dispatch-through-seams.

Regression gates from SUBAWARDEE_DOSSIER_PROGRAM.md §7 step 1 that are
expressible without the live sidecar:
  [F1] inferred-code SQL is code-anchored (code predicate BEFORE uei filter;
       builder refuses non-naics/psc types);
  [F3] family dedup + target-family exclusion in the dispatch fixture;
  [F4] JV name/flag filter removes market candidates;
  [F7] self-pair exclusion present in eligibility/history/deal-size/triangle SQL;
  [F8] every statement carries an explicit LIMIT;
  [F10/F11] scoring helpers (ubiquity forms, cosine, overlap) behave as specified;
  restart-once on ArtifactMoved.
"""
from __future__ import annotations

import asyncio
from typing import Any

import pytest
from fastapi import HTTPException

from apps.catalyst_api.src.routers import sub_dossier_v1 as sd


# ---------------------------------------------------------------------------
# compile tests
# ---------------------------------------------------------------------------

UEI = "JE4BKEP7KNA6"
SEEDS = ["HQBRRJBLDQ37", "MHJ2S6K2LKP5"]


def test_valid_uei_normalizes():
    assert sd._valid_uei(" je4bkep7kna6 ") == UEI


def test_invalid_uei_refused():
    for bad in ("", "short", "TOOLONGTOOLONG", "bad!chars!!!", None, 12):
        with pytest.raises(HTTPException):
            sd._valid_uei(bad)


def test_unknown_dial_refused():
    with pytest.raises(HTTPException):
        sd._merge_dials({"nope": 1})


def test_dial_bounds_enforced():
    with pytest.raises(HTTPException):
        sd._merge_dials({"market_size": 10_000})
    with pytest.raises(HTTPException):
        sd._merge_dials({"band_min": 5e6, "band_max": 1e6})
    d = sd._merge_dials({"market_size": 25, "exclude_jv": False})
    assert d["market_size"] == 25 and d["exclude_jv"] is False


def test_no_free_text_reaches_sql():
    with pytest.raises(HTTPException):
        sd._uei_list_sql(["'; DROP TABLE x; --"])
    with pytest.raises(HTTPException):
        sd._code_list_sql(["54' OR 1=1"])


def test_eligibility_excludes_self_pairs():  # [F7]
    sql = sd.sql_eligible(1e6, 1e8, 100)
    assert "subawardee_uei <> prime_awardee_uei" in sql
    assert "LIMIT 100" in sql


def test_history_excludes_self_pairs_and_limits():  # [F7, F8]
    sql = sd.sql_history(UEI, 500)
    assert "subawardee_uei <> prime_awardee_uei" in sql
    assert "LIMIT 500" in sql


def test_dealsize_excludes_self_pairs():  # [F7]
    sql = sd.sql_market_dealsize(SEEDS, ["541712"])
    assert "subawardee_uei <> prime_awardee_uei" in sql


def test_triangle_excludes_target_and_self_pairs():  # [F7]
    sql = sd.sql_triangle(UEI, SEEDS, SEEDS, 400)
    assert f"p.sub_uei <> '{UEI}'" in sql
    assert "p.sub_uei <> p.prime_uei" in sql
    assert "LIMIT 400" in sql


def test_market_sql_selection_side_gates():  # [F2, F9] + 2026-07-20 gate ruling
    # Default (ruling): sub-out DROPPED — evidence rides along, never gates.
    sql = sd.sql_market(UEI, SEEDS, ["541712"], ["R425"], dict(sd.DEFAULT_DIALS))
    assert "LEFT JOIN sub_out" in sql                           # evidence, not a gate
    assert "INTERVAL" not in sql.split("ORDER BY")[0].split("WHERE b.prime_obl_60mo")[1]
    assert "LN(1 + f.n_with_lane)" in sql                       # IDF damping
    assert "so.subout_5y DESC NULLS LAST" in sql                # proven primes outrank unknowns
    assert "LIMIT 25000" in sql          # FULL candidate fetch — totals never dial-shaped
    # Dial ON restores the reviewed gate exactly.
    gated = sd.sql_market(UEI, SEEDS, ["541712"], ["R425"],
                          {**sd.DEFAULT_DIALS, "require_subout": True})
    assert "\nJOIN sub_out" in gated and "LEFT JOIN sub_out" not in gated
    assert "INTERVAL 24 MONTH" in gated


def test_market_sql_no_shape_codes_degrades_safely():
    sql = sd.sql_market(UEI, SEEDS, [], [], dict(sd.DEFAULT_DIALS))
    assert "1 = 0" in sql   # farm-out OR-leg disabled, not broken


def test_archetype_validation():
    assert sd._valid_archetype(None) == "sub"
    assert sd._valid_archetype("prime_sub") == "prime_sub"
    with pytest.raises(HTTPException):
        sd._valid_archetype("peer")
    with pytest.raises(HTTPException):
        sd._valid_archetype(1)


def test_hybrid_dial_bounds():
    d = sd._merge_dials({"hybrid_prime_floor": 2e6, "hybrid_sub_floor": 5e5})
    assert d["hybrid_prime_floor"] == 2e6 and d["hybrid_sub_floor"] == 5e5
    with pytest.raises(HTTPException):
        sd._merge_dials({"hybrid_prime_floor": -1})


def test_market_sql_hybrid_recipient_shape_gate():
    dials = dict(sd.DEFAULT_DIALS)
    sql = sd.sql_market(UEI, SEEDS, ["541712"], ["R425"], dials,
                        shape_naics=["541712", "541330"])
    # the ruled gate: ONE source lens, ONE context type, INNER JOIN at selection
    assert "recipient_code_source = 'awarded_prime_contracts_in_code'" in sql
    assert "recipient_code_type = 'naics'" in sql
    assert "context_code_type = 'naics'" in sql
    assert "JOIN shape_out sh ON sh.uei = c.uei" in sql
    # sub archetype emits no shape gate
    sub_sql = sd.sql_market(UEI, SEEDS, ["541712"], ["R425"], dials)
    assert "shape_out" not in sub_sql


def test_shape_subout_sql_per_code_and_single_lens():
    sql = sd.sql_market_shape_subout(SEEDS, ["541712"])
    assert "recipient_code_source = 'awarded_prime_contracts_in_code'" in sql
    assert "context_code_type = 'naics'" in sql
    assert "GROUP BY 1, 2" in sql          # per (candidate, code) — never cross-code
    assert "subaward_amt_total > 0" in sql


def test_eligible_sql_hybrid_floors():
    plain = sd.sql_eligible(1e6, 1e8, 100)
    assert "COALESCE(p.prime_won, 0) >=" not in plain
    floored = sd.sql_eligible(1e6, 1e8, 100, prime_floor=1e6, sub_floor=2e6)
    assert "COALESCE(p.prime_won, 0) >= 1000000.0" in floored
    assert "s.sub_amt >= 2000000.0" in floored


def test_inferred_sql_is_code_anchored():  # [F1]
    sql = sd.sql_peer_inferred(SEEDS, "naics", ["541712", "541330"])
    code_pos = sql.index("code_type = 'naics'")
    uei_pos = sql.index("uei IN")
    assert code_pos < uei_pos   # the sort key leads
    with pytest.raises(HTTPException):
        sd.sql_peer_inferred(SEEDS, "bogus", ["541712"])


def test_signature_rows_unfiltered_for_vectors():  # [F11]
    sql = sd.sql_signature_rows([UEI])
    assert "rank_lifetime <=" not in sql and "share_lifetime >=" not in sql


# ---------------------------------------------------------------------------
# scoring helpers
# ---------------------------------------------------------------------------

def test_family_key_prefers_hierarchy_then_name():
    assert sd.family_key("A" * 12, "P" * 12, "X INC") == "h:" + "P" * 12
    assert sd.family_key("A" * 12, None, "Arrow Electronics, Inc.") == "n:ARROW ELECTRONICS"
    assert sd.family_key("A" * 12, None, None) == "u:" + "A" * 12


def test_name_key_strips_suffixes_and_punctuation():
    a = sd.normalize_name_key("NORTHROP GRUMMAN SYSTEMS CORPORATION")
    b = sd.normalize_name_key("Northrop Grumman Systems Corp.")
    assert a == b == "NORTHROP GRUMMAN SYSTEMS"


def test_jv_name_detection():  # [F4]
    assert sd.is_jv_name("PERNIX KASEMAN JOINT VENTURE")
    assert sd.is_jv_name("AMES 1-HWH JV")
    assert not sd.is_jv_name("JVC ELECTRONICS")   # token boundary, not substring


def test_cosine_basic():
    a = {"naics:1": 1.0}
    assert sd.cosine(a, a) == pytest.approx(1.0)
    assert sd.cosine(a, {"naics:2": 1.0}) == 0.0
    assert sd.cosine({}, a) == 0.0


def test_peer_share_in_set():
    w = {"naics:541712": 0.7, "naics:999999": 0.3}
    assert sd.peer_share_in_set(w, {"naics:541712"}) == pytest.approx(0.7)
    assert sd.peer_share_in_set({}, {"naics:541712"}) == 0.0


def test_overlap_coefficient_target_relative():  # [F1]
    assert sd.overlap_coefficient({"naics:1", "naics:2"}, {"naics:1", "naics:2", "naics:3"}) \
        == pytest.approx(2 / 3)
    assert sd.overlap_coefficient({"naics:1"}, set()) == 0.0


def test_ubiquity_sqrt_default_spread():  # [F10]
    lo = sd.ubiquity_weight(3, "1/sqrt(1+n)")
    hi = sd.ubiquity_weight(330, "1/sqrt(1+n)")
    assert lo / hi > 9.0   # measured 9.1× spread requirement
    assert sd.ubiquity_weight(3, "1/ln(1+n)") > sd.ubiquity_weight(330, "1/ln(1+n)")


def test_last_complete_month_walks_past_lag():  # [F5]
    months = [(f"2025-{m:02d}", 3500) for m in range(1, 13)] + \
             [("2026-01", 3600), ("2026-02", 3400), ("2026-03", 2100),
              ("2026-04", 1600), ("2026-05", 700), ("2026-06", 80)]
    assert sd.last_complete_month(months) == "2026-03"


# ---------------------------------------------------------------------------
# dispatch through seams — a tiny fixture universe driven end-to-end
# ---------------------------------------------------------------------------

T = UEI                       # target
SEED = "SEEDPRIME0001"[:12]   # 12 chars
MKT_OK = "MARKETPRIME1"[:12]
MKT_JV = "MARKETPRIMEJ"[:12]
MKT_FAM = "MARKETPRIMEF"[:12]  # same family as MKT_OK → deduped
PEER_GOOD = "PEERGOODUEI1"[:12]
PEER_SELFF = "PEERTARGFAM1"[:12]  # target's own family → excluded
PEER_NOEVD = "PEERNOEVIDE1"[:12]  # no lens substrate → insufficient, excluded


class FakeSidecar:
    """Routes each statement by stage-distinctive SQL content."""

    def __init__(self):
        self.calls: list[str] = []
        self.artifact = "query_sidecar_TEST"

    def respond(self, sql: str, limit: int) -> dict[str, Any]:
        self.calls.append(sql)
        cols_rows = self._match(sql)
        cols, rows = cols_rows
        return {"columns": cols, "rows": rows, "elapsed_ms": 1.0,
                "artifact": self.artifact}

    def _match(self, sql: str) -> tuple[list[str], list[list[Any]]]:
        if "FROM gtm_sam_entities e" in sql and f"e.uei = '{T}'" in sql:
            return (["uei", "legal_business_name", "physical_state", "physical_city",
                     "primary_naics", "sam_is_active", "cage_code",
                     "employee_size_range", "industry", "year_founded",
                     "ultimate_parent_uei", "ultimate_parent_name",
                     "prime_obl_lifetime", "sub_amt_lifetime", "active_award_ct",
                     "last_action_date", "sp_sub_lifetime", "median_chunk_lifetime",
                     "p20_chunk_lifetime", "p80_chunk_lifetime",
                     "n_distinct_primes_lifetime", "top_buyer_uei",
                     "top_buyer_share_lifetime_pct", "cagr_5y_pct",
                     "sub_first_action", "sub_last_action"],
                    [[T, "TARGET FIRM LLC", "CO", "DENVER", "541712", True, "ABC12",
                      "11-50", "Defense", 2005, None, None,
                      0.0, 9_000_000.0, 0, "2026-01-15", 9_000_000.0, 250_000.0,
                      100_000.0, 700_000.0, 4, SEED, 61.0, 12.0,
                      "2019-01-01", "2026-01-15"]])
        if "FROM gtm_entity_fy_won WHERE uei" in sql:
            return (["fy", "prime_won", "sa_any", "sa_8a", "sa_sdvosb", "sa_wosb",
                     "sa_hubzone"], [])
        if "subaward_action_date_fiscal_year AS fy" in sql:
            return (["fy", "sub_amt", "sub_ct"],
                    [[2023, 2_000_000.0, 4], [2024, 3_000_000.0, 5],
                     [2025, 4_000_000.0, 6]])
        if "ORDER BY subaward_action_date DESC" in sql:
            return (["prime_awardee_uei", "prime_awardee_name", "subaward_amount_num",
                     "subaward_action_date", "subaward_action_date_fiscal_year",
                     "prime_award_naics_code", "prime_award_product_or_service_code",
                     "prime_award_awarding_agency_name", "subaward_description"],
                    [[SEED, "SEED PRIME INC", 500_000.0, "2025-06-01", 2025,
                      "541712", "R425", "DEPT OF THE AIR FORCE", "engineering support"]])
        if "COUNT(*) AS n_rows" in sql:
            return (["n_rows", "total_amt", "distinct_primes"], [[1, 500_000.0, 1]])
        if "date_trunc('month', subaward_action_date)" in sql and "subawardee_uei" in sql:
            return (["month", "amt", "n"], [["2025-06-01", 500_000.0, 1]])
        if "FROM gtm_prime_sub_pairs_by_sub" in sql and f"sub_uei = '{T}'" in sql:
            return (["prime_uei", "prime_name", "edge_dollars_5y", "edge_count_5y",
                     "edge_dollars_lifetime", "first_action_date", "last_action_date"],
                    [[SEED, "SEED PRIME INC", 5_000_000.0, 4, 6_000_000.0,
                      "2021-01-01", "2025-06-01"]])
        if "FROM gtm_entity_code_lanes" in sql:
            return (["side", "code_type", "code", "obl_lifetime", "obl_60mo",
                     "action_ct"],
                    [["sub", "naics", "541712", 5_000_000.0, 4_000_000.0, 6],
                     ["sub", "psc", "R425", 4_000_000.0, 3_000_000.0, 5]])
        if "FROM gtm_prime_code_signature WHERE uei IN" in sql:
            if f"'{T}'" in sql:
                return (["uei", "code_type", "code", "share_lifetime",
                         "rank_lifetime", "obl_lifetime"], [])  # target never primes
            rows = []
            if f"'{PEER_GOOD}'" in sql:
                rows.append([PEER_GOOD, "naics", "541712", 0.8, 1, 1_000_000.0])
                rows.append([PEER_GOOD, "psc", "R425", 0.7, 1, 900_000.0])
            return (["uei", "code_type", "code", "share_lifetime", "rank_lifetime",
                     "obl_lifetime"], rows)
        if "FROM v_sam_declared_codes" in sql:
            return (["uei", "code_type", "code"], [])
        if "date_trunc('month', subaward_action_date) AS month, COUNT(*)" in sql:
            return (["month", "n"],
                    [[f"2025-{m:02d}-01", 3500] for m in range(1, 13)]
                    + [["2026-01-01", 3400], ["2026-02-01", 3300],
                       ["2026-03-01", 2000], ["2026-04-01", 1500],
                       ["2026-05-01", 700], ["2026-06-01", 80]])
        if "WITH seed_sig AS" in sql:
            return (["uei", "lane_hits", "wt", "subout_5y", "last_sub",
                     "prime_obl_60mo"],
                    [[MKT_OK, 2, 11.0, 9_000_000.0, "2026-02-01", 5e7],
                     [MKT_JV, 2, 10.5, 8_000_000.0, "2026-02-01", 4e7],
                     [MKT_FAM, 2, 10.0, 7_000_000.0, "2026-02-01", 3e7]])
        if "FROM gtm_sam_entities e" in sql and "LEFT JOIN gtm_entity_award_book" in sql:
            return (["uei", "legal_business_name", "physical_state",
                     "ultimate_parent_uei", "ultimate_parent_name",
                     "committed_award_ct", "committed_value", "committed_runway",
                     "next_committed_end_date", "active_agency_ct"],
                    [[MKT_OK, "BIG PRIME ALPHA INC", "VA", "PARENTPRIME1", "ALPHA PARENT",
                      4, 9e8, 4e8, "2027-01-01", 3],
                     [MKT_JV, "ALPHA-BETA JOINT VENTURE", "VA", None, None,
                      1, 1e8, 5e7, "2026-12-01", 1],
                     [MKT_FAM, "BIG PRIME ALPHA LLC", "TX", "PARENTPRIME1", "ALPHA PARENT",
                      2, 2e8, 1e8, "2026-10-01", 2]])
        if "FROM gtm_fpds_entity_signal_events" in sql:
            return (["uei"], [])   # JV caught by name regex instead
        if "FROM gtm_prime_farmout_combo_lanes" in sql and "uei IN" in sql:
            return (["uei", "fo_amt_60mo", "prime_obl_60mo"],
                    [[MKT_OK, 2e7, 5e7]])
        if "won_obl_set_aside" in sql and "uei IN" in sql:
            return (["uei", "sa_any", "sa_8a", "sa_sdvosb", "sa_wosb", "sa_hubzone"],
                    [[MKT_OK, 1e6, 5e5, 0, 0, 0]])
        if "MEDIAN(subaward_amount_num)" in sql:
            return (["uei", "n", "median_deal", "avg_deal"],
                    [[MKT_OK, 40, 160_000.0, 400_000.0]])
        if "WITH tri AS" in sql:
            return (["sub_uei", "sub_name", "market_primes_ct",
                     "dollars_from_market_5y", "last_action", "market_prime_ueis",
                     "shared_seed_primes"],
                    [[PEER_GOOD, "PEER GOOD LLC", 1, 3_000_000.0, "2026-01-10",
                      [MKT_OK], 1],
                     [PEER_SELFF, "TARGET FIRM HOLDINGS LLC", 1, 2_000_000.0,
                      "2026-01-05", [MKT_OK], 1],
                     [PEER_NOEVD, "MYSTERY SUB LLC", 1, 1_000_000.0, "2025-12-01",
                      [MKT_OK], 1]])
        if "FROM gtm_sam_entities e" in sql and "gtm_sub_profiles sp" in sql:
            return (["uei", "legal_business_name", "ultimate_parent_uei",
                     "ultimate_parent_name", "n_distinct_primes_lifetime"],
                    [[PEER_GOOD, "PEER GOOD LLC", None, None, 6],
                     [PEER_SELFF, "TARGET FIRM LLC", None, None, 3],
                     [PEER_NOEVD, "MYSTERY SUB LLC", None, None, 2]])
        if "FULL OUTER JOIN" in sql:
            return (["uei", "sub_amt", "prime_won"],
                    [[PEER_GOOD, 4_000_000.0, 1_000_000.0],
                     [PEER_SELFF, 2_000_000.0, 0.0],
                     [PEER_NOEVD, 1_000_000.0, 0.0]])
        if "FROM gtm_entity_inferred_subbable_codes" in sql:
            return (["uei", "code_type", "code"], [])
        if "amt_24mo" in sql:
            return (["uei", "amt_24mo", "n_24mo"], [[PEER_GOOD, 1_500_000.0, 2]])
        raise AssertionError(f"unmatched fixture SQL: {sql[:160]}")


@pytest.fixture()
def fake(monkeypatch):
    fs = FakeSidecar()

    async def fake_run(client, sql, limit, require_artifact=None):
        assert isinstance(limit, int) and limit >= 1   # [F8] explicit limit always
        return fs.respond(sql, limit)

    monkeypatch.setattr(sd, "_run_sidecar", fake_run)
    return fs


def test_build_end_to_end_through_seams(fake):
    payload = asyncio.run(sd._build_dossier(T, dict(sd.DEFAULT_DIALS)))
    assert payload["version"] == "sub_dossier_v2"
    assert payload["artifact"] == "query_sidecar_TEST"
    # band: sub 9M + prime 0 → in band
    assert payload["target"]["in_band"] is True
    assert payload["target"]["sub_amt_fy23_25"] == pytest.approx(9_000_000.0)
    # market: JV filtered by name [F4], family-dup dropped [F3]
    mkt = payload["market"]["primes"]
    assert [m["uei"] for m in mkt] == [MKT_OK]
    assert mkt[0]["farmout_in_shape"]["share_60mo_display"] == pytest.approx(0.4)
    assert mkt[0]["set_aside_won_fy23_25"]["8a"] == pytest.approx(5e5)
    # closing: target-family peer excluded [F3], no-evidence peer excluded [F1]
    comps = payload["closing"]["competitors"]
    assert [c["uei"] for c in comps] == [PEER_GOOD]
    c = comps[0]
    assert c["evidence_tier"] == "demonstrated" and c["lens_branch"] == 2
    assert c["dollars_from_market_24mo"] == pytest.approx(1_500_000.0)
    # freshness walked past the FSRS lag tail [F5]
    assert payload["method"]["freshness"]["last_complete_month"].startswith("2026-03")
    # floors: 1 market row < 5, 1 competitor < 3 → both flagged [F6]
    assert set(payload["method"]["sections_below_floor"]) == {"market", "closing"}


class FakeHybridSidecar(FakeSidecar):
    """The same fixture universe, but the target HAS a prime business:
    prime FY won rows, prime-side code lanes, and its own prime signature."""

    def _match(self, sql: str):
        if "WITH seed_sig AS" in sql:
            return super()._match(sql)          # market SQL (contains cube CTE)
        if "SUM(subaward_amt_total)" in sql:    # sql_market_shape_subout
            return (["uei", "recipient_code", "amt", "edge_ct", "last_action"],
                    [[MKT_OK, "541712", 3_500_000.0, 7, "2026-01-20"],
                     [MKT_OK, "541330", 800_000.0, 2, "2025-11-02"]])
        if "FROM gtm_entity_fy_won WHERE uei" in sql:
            return (["fy", "prime_won", "sa_any", "sa_8a", "sa_sdvosb",
                     "sa_wosb", "sa_hubzone"],
                    [[2023, 2_000_000.0, 1e6, 0, 0, 0, 0],
                     [2024, 2_000_000.0, 0, 0, 0, 0, 0],
                     [2025, 2_000_000.0, 0, 0, 0, 0, 0]])
        if "FROM gtm_entity_code_lanes" in sql:
            return (["side", "code_type", "code", "obl_lifetime", "obl_60mo",
                     "action_ct"],
                    [["prime", "naics", "541712", 6_000_000.0, 5_000_000.0, 9],
                     ["prime", "psc", "R425", 5_000_000.0, 4_000_000.0, 8],
                     ["sub", "naics", "561210", 4_000_000.0, 3_000_000.0, 5]])
        if "FROM gtm_prime_code_signature WHERE uei IN" in sql and f"'{T}'" in sql:
            return (["uei", "code_type", "code", "share_lifetime",
                     "rank_lifetime", "obl_lifetime"],
                    [[T, "naics", "541712", 0.9, 1, 6_000_000.0],
                     [T, "psc", "R425", 0.8, 1, 5_000_000.0]])
        return super()._match(sql)


@pytest.fixture()
def fake_hybrid(monkeypatch):
    fs = FakeHybridSidecar()

    async def fake_run(client, sql, limit, require_artifact=None):
        assert isinstance(limit, int) and limit >= 1
        return fs.respond(sql, limit)

    monkeypatch.setattr(sd, "_run_sidecar", fake_run)
    return fs


def test_hybrid_build_end_to_end(fake_hybrid):
    payload = asyncio.run(sd._build_dossier(T, dict(sd.DEFAULT_DIALS), "prime_sub"))
    assert payload["archetype"] == "prime_sub"
    # band: prime 6M + sub 9M, both over the $1M floors → in band
    assert payload["target"]["in_band"] is True
    assert payload["target"]["prime_won_fy23_25"] == pytest.approx(6_000_000.0)
    # prime business: own signature + shape codes from PRIME lanes
    pb = payload["prime_business"]
    assert pb is not None
    assert pb["shape_naics"] == ["541712"]
    assert pb["signature"][0]["code"] in ("541712", "R425")
    assert pb["fy_set_aside"][0]["any"] == pytest.approx(1e6)
    # the market SQL carried the recipient-shape gate
    market_sql = next(s for s in fake_hybrid.calls if "WITH seed_sig AS" in s)
    assert "recipient_code_source = 'awarded_prime_contracts_in_code'" in market_sql
    assert "JOIN shape_out sh ON sh.uei = c.uei" in market_sql
    # market rows carry per-code shape evidence, top code first, never summed
    mkt = payload["market"]["primes"]
    assert [m["uei"] for m in mkt] == [MKT_OK]
    shp = mkt[0]["subout_to_your_shape"]
    assert shp["matched_code_ct"] == 2
    assert shp["top_code"] == "541712"
    assert shp["top_code_amt"] == pytest.approx(3_500_000.0)
    # hybrid disclosures present
    assert any("own prime award record" in d
               for d in payload["method"]["disclosures"])


def test_hybrid_band_floor_enforced(fake_hybrid):
    dials = dict(sd.DEFAULT_DIALS)
    dials["hybrid_prime_floor"] = 10_000_000.0   # prime 6M sits under the floor
    payload = asyncio.run(sd._build_dossier(T, dials, "prime_sub"))
    assert payload["target"]["in_band"] is False


def test_sub_archetype_payload_untouched(fake):
    payload = asyncio.run(sd._build_dossier(T, dict(sd.DEFAULT_DIALS)))
    assert payload["archetype"] == "sub"
    assert payload["prime_business"] is None
    assert all("own prime award record" not in d
               for d in payload["method"]["disclosures"])


def test_build_unknown_uei_404(monkeypatch):
    async def fake_run(client, sql, limit, require_artifact=None):
        return {"columns": [], "rows": [], "artifact": "A", "elapsed_ms": 1.0}
    monkeypatch.setattr(sd, "_run_sidecar", fake_run)
    with pytest.raises(HTTPException) as exc:
        asyncio.run(sd._build_dossier("ZZZZZZZZZZZZ", dict(sd.DEFAULT_DIALS)))
    assert exc.value.status_code == 404 and exc.value.detail == "unknown_uei"


def test_artifact_moved_restarts_once(monkeypatch):
    attempts = {"n": 0}

    async def fake_build(uei, dials, archetype="sub"):
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise sd.ArtifactMoved()
        return {"version": "sub_dossier_v2", "artifact": "A",
                "method": {}, "target": {}, "reality": {}, "seed_primes": [],
                "market": {}, "closing": {}}

    monkeypatch.setattr(sd, "_build_dossier", fake_build)
    payload = asyncio.run(sd.build({"uei": T}))
    assert attempts["n"] == 2
    assert "build_wall_ms" in payload["method"]
