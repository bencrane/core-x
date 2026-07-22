"""market_slice_v1 — scope resolution, band parsing, pack SQL assembly (pure)."""
import pytest
from fastapi import HTTPException

from apps.catalyst_api.src.routers.market_query_v1 import compile_predicates
from apps.catalyst_api.src.routers.market_slice_v1 import (
    _DEFAULT_BAND,
    _load_overlay,
    _parse_band,
    build_count_sql,
    build_entities_sql,
    build_grammar_entities_sql,
    build_pack_sql,
)

PAIRS = [("562111", "S205"), ("562112", "S222")]


# ── overlay registry integrity ───────────────────────────────────────────────

def test_overlay_loads_with_eight_cards_and_extensions() -> None:
    o = _load_overlay()
    assert set(o["cards"]) == {
        "federal-it-solutions", "applied-research-development",
        "architect-engineering-services", "facility-maintenance-repair",
        "utility-services", "training-education-services",
        "finance-accounting-staffing", "logistics-supply-chain-staffing",
    }
    assert sum(len(e["pairs"]) for e in o["extensions"].values()) == 63
    for card in o["cards"].values():
        assert card["pairs"], "every overlay card carries explicit pairs"
        assert card["lens"] in ("canonical", "uncovered-sweep", "staffing")
        assert "verified" in card


def test_overlay_pairs_are_wellformed() -> None:
    o = _load_overlay()
    for card in o["cards"].values():
        for n, p in card["pairs"]:
            assert len(n) == 6 and len(p) == 4


def test_fm_delta_is_disclosed() -> None:
    o = _load_overlay()
    assert "known_delta" in o["cards"]["facility-maintenance-repair"]["verified"]


# ── band parsing ─────────────────────────────────────────────────────────────

def test_band_defaults_to_capital_band() -> None:
    assert _parse_band({}) == _DEFAULT_BAND == {"min": 1_000_000.0, "max": 100_000_000.0}


def test_band_explicit_is_open_ended_not_default_filled() -> None:
    # "$100M+" / "under $1M" presets: a partial band opens at the missing edge —
    # it must NEVER collapse into the $1M–$100M capital default (the old
    # fill-from-default made both presets refuse as empty ranges).
    from apps.catalyst_api.src.routers.market_slice_v1 import _BAND_UNBOUNDED
    assert _parse_band({"band": {"min": 5e6}}) == {"min": 5e6, "max": _BAND_UNBOUNDED}
    assert _parse_band({"band": {"min": 2e8}}) == {"min": 2e8, "max": _BAND_UNBOUNDED}
    assert _parse_band({"band": {"max": 1e6}}) == {"min": 0.0, "max": 1e6}


def test_band_inverted_refuses() -> None:
    with pytest.raises(HTTPException):
        _parse_band({"band": {"min": 2e8, "max": 1e8}})


def test_band_negative_refuses() -> None:
    with pytest.raises(HTTPException):
        _parse_band({"band": {"min": -1}})


# ── pack SQL assembly ────────────────────────────────────────────────────────

def test_pack_has_all_sections() -> None:
    sql = build_pack_sql(PAIRS, dict(_DEFAULT_BAND), None)
    assert set(sql) == {"tiles", "over_band", "cadence", "book", "states", "top_pairs",
                       "size_bands", "series"}


def test_cohort_encodes_card_definition() -> None:
    tiles = build_pack_sql(PAIRS, dict(_DEFAULT_BAND), None)["tiles"]
    assert "('562111','S205')" in tiles
    assert "current_end_date >= current_date" in tiles and "is_terminated = FALSE" in tiles
    assert "IN ('Z','NOT APPLICABLE')" in tiles          # unfinanced slice
    assert "active_obl >= 1000000.0 AND active_obl <= 100000000.0" in tiles
    # vehicles count for membership: the m CTE filters unfin only, never topology
    m_cte = tiles.split("m AS (")[1].split(")")[0]
    assert "is_vehicle" not in m_cte


def test_over_band_uses_band_max() -> None:
    over = build_pack_sql(PAIRS, {"min": 1e6, "max": 5e7}, None)["over_band"]
    assert "active_obl > 50000000.0" in over


def test_predicate_expr_intersects_cohort() -> None:
    expr, _, _ = compile_predicates(
        {"predicates": [{"term": "registered_in_state", "states": ["CA"]}]}
    )
    tiles = build_pack_sql(PAIRS, dict(_DEFAULT_BAND), expr)["tiles"]
    assert "s.uei IN (" in tiles and "physical_state IN ('CA')" in tiles


def test_no_predicates_no_leg() -> None:
    tiles = build_pack_sql(PAIRS, dict(_DEFAULT_BAND), None)["tiles"]
    assert "s.uei IN (" not in tiles


def test_cadence_median_over_book_rows_only() -> None:
    cadence = build_pack_sql(PAIRS, dict(_DEFAULT_BAND), None)["cadence"]
    assert "FILTER (WHERE has_book)" in cadence


def test_count_sql_is_cohort_count_only() -> None:
    sql = build_count_sql(PAIRS, dict(_DEFAULT_BAND), None)
    assert sql.strip().endswith("SELECT count(*) AS firms FROM m")
    assert "top_pairs" not in sql and "median" not in sql


def test_tot_denominator_respects_predicates() -> None:
    expr, _, _ = compile_predicates(
        {"predicates": [{"term": "registered_in_state", "states": ["TX"]}]}
    )
    tiles = build_pack_sql(PAIRS, dict(_DEFAULT_BAND), expr)["tiles"]
    # the any-financing total joins the predicated m_any cohort, never raw band
    assert "JOIN m_any USING (uei)" in tiles
    m_any = tiles.split("m_any AS (")[1].split("\n)")[0]
    assert "physical_state IN ('TX')" in m_any


# ── series section (Explore/page FY trend) ───────────────────────────────────

def test_series_reads_fy_won_over_cohort() -> None:
    series = build_pack_sql(PAIRS, dict(_DEFAULT_BAND), None)["series"]
    assert "gtm_entity_fy_won" in series
    assert "FROM m JOIN" in series  # cohort-joined, never universe-wide
    assert "GROUP BY 1 ORDER BY 1" in series


# ── entities SQL (the Explore map/table read) ────────────────────────────────

def test_entities_sql_shape() -> None:
    sql = build_entities_sql(PAIRS, dict(_DEFAULT_BAND), None, 500)
    assert "('562111','S205')" in sql  # same cohort CTE as the pack
    assert "LEFT JOIN gtm_sam_entities e" in sql
    assert "LEFT JOIN gtm_entity_geo g" in sql
    assert "GROUP BY m.uei" in sql
    assert "ORDER BY unfin_usd DESC" in sql
    assert sql.strip().endswith("LIMIT 500")
    # money = unfinanced-in-scope only
    assert "FILTER (WHERE s.unfin)" in sql


def test_entities_predicate_leg_rides_cohort() -> None:
    expr, _, _ = compile_predicates(
        {"predicates": [{"term": "registered_in_state", "states": ["CA"]}]}
    )
    sql = build_entities_sql(PAIRS, dict(_DEFAULT_BAND), expr, 100)
    assert "s.uei IN (" in sql and "physical_state IN ('CA')" in sql


def test_entities_count_pairs_with_count_sql() -> None:
    # /entities count parity: the endpoint runs build_count_sql for the SAME body —
    # the count statement must stay cohort-only (no hydration joins).
    cnt = build_count_sql(PAIRS, dict(_DEFAULT_BAND), None)
    assert "gtm_entity_geo" not in cnt


# ── grammar-only entities SQL (slug-less Explore) ────────────────────────────

def test_grammar_entities_sql_shape() -> None:
    ent, cnt = build_grammar_entities_sql("SELECT uei FROM gtm_sam_entities", None, 100)
    assert "LEFT JOIN gtm_entity_geo g USING (uei)" in ent
    assert "round(coalesce(p.active_obl, 0), 0) AS active_obl" in ent
    assert "ORDER BY active_obl DESC" in ent
    assert ent.strip().endswith("LIMIT 100")
    assert cnt.strip().startswith("SELECT count(*) AS firms")
    assert "gtm_entity_geo" not in cnt  # count never pays the hydration joins


def test_grammar_entities_band_filters_both_statements() -> None:
    ent, cnt = build_grammar_entities_sql(
        "SELECT uei FROM gtm_sam_entities", {"min": 1e6, "max": 1e8}, 100
    )
    for sql in (ent, cnt):
        assert "coalesce(p.active_obl, 0) >= 1000000.0" in sql
        assert "coalesce(p.active_obl, 0) <= 100000000.0" in sql


def test_grammar_entities_no_band_no_where() -> None:
    ent, cnt = build_grammar_entities_sql("SELECT uei FROM gtm_sam_entities", None, 50)
    assert "active_obl, 0) >=" not in ent and "active_obl, 0) >=" not in cnt


# ── mega pack (everything unfinanced-in-force, no scope) ─────────────────────

def test_mega_has_all_sections() -> None:
    from apps.catalyst_api.src.routers.market_slice_v1 import build_mega_sql
    sections = build_mega_sql(dict(_DEFAULT_BAND))
    assert set(sections) == {"tiles", "matrix", "band_tier", "series"}


def test_mega_tiles_are_single_table_entity_grain() -> None:
    from apps.catalyst_api.src.routers.market_slice_v1 import build_mega_sql
    sections = build_mega_sql(dict(_DEFAULT_BAND))
    for name in ("tiles", "matrix"):
        sql = sections[name]
        assert "FROM gtm_entity_pricing_mix" in sql
        assert "JOIN" not in sql, f"{name} must never join (the 83M-join class)"
        assert sql.rstrip().endswith("FROM gtm_entity_pricing_mix"), (
            f"{name} is the whole corpus — no top-level WHERE"
        )


def test_mega_band_tier_filters_on_band() -> None:
    from apps.catalyst_api.src.routers.market_slice_v1 import build_mega_sql
    sections = build_mega_sql({"min": 5e6, "max": 1e8})
    assert "active_obl >= 5000000.0 AND active_obl <= 100000000.0" in sections["band_tier"]


def test_mega_matrix_has_twenty_cells() -> None:
    from apps.catalyst_api.src.routers.market_slice_v1 import build_mega_sql
    sql = build_mega_sql(dict(_DEFAULT_BAND))["matrix"]
    for pc in ("fixed", "cost", "tm_lh", "other"):
        for fc in ("unfin", "prog", "perf", "comm", "othfin"):
            assert f"active_obl_{pc}_{fc}" in sql
            assert f"active_{pc}_{fc}_ct" in sql


def test_mega_series_reads_fy_won_over_unfinanced_holders() -> None:
    from apps.catalyst_api.src.routers.market_slice_v1 import build_mega_sql
    sql = build_mega_sql(dict(_DEFAULT_BAND))["series"]
    assert "active_obl_fin_unfin > 0" in sql
    assert "gtm_entity_fy_won" in sql and "w.fy >= 2019" in sql


# ── active awards as points (the award-grain lens) ───────────────────────────

def test_awards_sql_joins_centroids_on_the_known_key() -> None:
    from apps.catalyst_api.src.routers.market_slice_v1 import build_awards_sql
    points, _ = build_awards_sql(None, 4000)
    assert "c.generated_unique_award_id = p.award_key" in points
    assert "LEFT JOIN usaspending_award_pop_centroids" in points, "geo never drops awards"


def test_awards_sql_active_definition_and_ranking() -> None:
    from apps.catalyst_api.src.routers.market_slice_v1 import build_awards_sql
    points, count = build_awards_sql(None, 2500)
    for sql in (points, count):
        assert "a.current_end_date >= current_date" in sql
        assert "a.is_terminated = FALSE" in sql
    assert "ORDER BY obl DESC, award_key" in points and "LIMIT 2500" in points


def test_awards_predicate_leg_rides_both_statements() -> None:
    from apps.catalyst_api.src.routers.market_slice_v1 import build_awards_sql
    expr, _, _ = compile_predicates(
        {"predicates": [{"term": "active_award_pricing_mix", "min_ffp_unfinanced_share": 0.7}]}
    )
    points, count = build_awards_sql(expr, 4000)
    for sql in (points, count):
        assert "a.recipient_uei IN (" in sql
        assert "active_ffp_unfinanced_share" in sql


def test_awards_count_is_full_cohort_totals() -> None:
    from apps.catalyst_api.src.routers.market_slice_v1 import build_awards_sql
    _, count = build_awards_sql(None, 100)
    assert "count(*) AS awards" in count
    assert "count(DISTINCT a.recipient_uei) AS firms" in count
    assert "LIMIT" not in count


def test_awards_band_applies_through_the_holder() -> None:
    # Firm-identity dials follow the holder across grains (operator-ruled
    # 2026-07-22): band at award grain = awards held by banded firms, gated on
    # the SAME column the firm cohort uses (gtm_entity_pricing_mix.active_obl).
    from apps.catalyst_api.src.routers.market_slice_v1 import build_awards_sql
    points, count = build_awards_sql(None, 4000, band={"min": 1e6, "max": 1e8})
    for sql in (points, count):
        assert "gtm_entity_pricing_mix" in sql
        assert "active_obl >= 1000000.0 AND active_obl <= 100000000.0" in sql


def test_awards_no_band_no_holder_leg() -> None:
    from apps.catalyst_api.src.routers.market_slice_v1 import build_awards_sql
    points, count = build_awards_sql(None, 4000)
    for sql in (points, count):
        assert "gtm_entity_pricing_mix" not in sql


def test_awards_key_exprs_filter_the_paper() -> None:
    # Award-grain geography (Awards-drawer semantics): the leg gates
    # contract_award_unique_key, never recipient_uei.
    from apps.catalyst_api.src.routers.market_slice_v1 import build_awards_sql
    key_sql = ("SELECT generated_unique_award_id FROM gtm_open_awards "
               "WHERE primary_place_of_performance_state_code IN ('HI')")
    points, count = build_awards_sql(None, 4000, award_key_exprs=[key_sql])
    for sql in (points, count):
        assert "a.contract_award_unique_key IN (" in sql
        assert "primary_place_of_performance_state_code IN ('HI')" in sql


def test_pop_award_key_projections() -> None:
    from apps.catalyst_api.src.routers.market_query_v1 import (
        pop_states_award_keys, pop_within_award_keys)
    sql, echo = pop_states_award_keys({"states": ["hi", "TX"]})
    assert "SELECT generated_unique_award_id FROM gtm_open_awards" in sql
    assert "IN ('HI','TX')" in sql and echo["grain"] == "award"
    sql, echo = pop_within_award_keys({"zip": "76544", "miles": 150})
    assert "generated_unique_award_id" in sql and "asin(sqrt(" in sql
    assert echo["grain"] == "award" and echo["miles"] == 150.0


# ── the WHEN frame (won-in-window) ───────────────────────────────────────────

def test_parse_when_active_and_absent_mean_standing() -> None:
    from apps.catalyst_api.src.routers.market_slice_v1 import _parse_when
    assert _parse_when({}) is None
    assert _parse_when({"when": {"mode": "active"}}) is None


def test_parse_when_won_window() -> None:
    from apps.catalyst_api.src.routers.market_slice_v1 import _parse_when
    assert _parse_when({"when": {"mode": "won", "fy_start": 2025, "fy_end": 2026}}) == {
        "fy_start": 2025, "fy_end": 2026}
    for bad in ({"mode": "won", "fy_start": 2025},                       # missing end
                {"mode": "won", "fy_start": 2026, "fy_end": 2025},        # inverted
                {"mode": "won", "fy_start": 1999, "fy_end": 2025},        # out of range
                {"mode": "lifetime"}):                                     # unknown mode
        with pytest.raises(HTTPException):
            _parse_when({"when": bad})


def test_won_entities_sql_shape() -> None:
    from apps.catalyst_api.src.routers.market_slice_v1 import build_won_entities_sql
    ent, count = build_won_entities_sql(
        PAIRS, {"min": 1e6, "max": 1e8}, None, 2000,
        {"fy_start": 2025, "fy_end": 2026})
    for sql in (ent, count):
        assert "FROM txn_events_combo t" in sql
        assert "t.fy BETWEEN 2025 AND 2026" in sql
        assert "HAVING sum(t.obligation) > 0" in sql          # membership = introduction
        assert "gtm_entity_pricing_mix" in sql                # band = holder property
    assert "round(won.won_usd, 0) AS unfin_usd" in ent        # wire alias stable
    assert "count(DISTINCT t.award_key) AS awards_touched" in ent
    assert "ORDER BY unfin_usd DESC" in ent and "LIMIT 2000" in ent
    assert "count(*) AS firms" in count and "LIMIT" not in count


def test_won_entities_pairless_and_predicated() -> None:
    from apps.catalyst_api.src.routers.market_slice_v1 import build_won_entities_sql
    ent, _ = build_won_entities_sql(
        None, None, "SELECT uei FROM x", 100, {"fy_start": 2024, "fy_end": 2024})
    assert "JOIN pairs" not in ent
    assert "t.uei IN (" in ent                                # predicate prunes inside won


# ── award profile (the award-dot click read) ─────────────────────────────────

def test_award_key_validation_refuses_injection() -> None:
    from apps.catalyst_api.src.routers.market_slice_v1 import _safe_award_key
    assert _safe_award_key("CONT_AWD_DEAC0500OR22725_8900_-NONE-_-NONE-")
    for bad in ("x' OR 1=1 --", "a b", "short", "", None, "k;semicolons"):
        with pytest.raises(HTTPException):
            _safe_award_key(bad)


# ── flow lens (obligations by fiscal year) ───────────────────────────────────

def test_flow_sql_shape_and_window() -> None:
    from apps.catalyst_api.src.routers.market_slice_v1 import build_flow_sql
    sql = build_flow_sql(None, 2019, 2026)
    assert "FROM txn_events_combo" in sql
    assert "fy >= 2019 AND fy <= 2026" in sql
    assert "GROUP BY 1 ORDER BY 1" in sql
    assert "uei IN (" not in sql


def test_flow_predicate_leg() -> None:
    from apps.catalyst_api.src.routers.market_slice_v1 import build_flow_sql
    expr, _, _ = compile_predicates(
        {"predicates": [{"term": "registered_in_state", "states": ["TX"]}]}
    )
    sql = build_flow_sql(expr, 2019, 2026)
    assert "uei IN (" in sql and "physical_state IN ('TX')" in sql


def test_flow_active_only_intersects_active_awards() -> None:
    from apps.catalyst_api.src.routers.market_slice_v1 import build_flow_sql
    sql = build_flow_sql(None, 2019, 2025, active_only=True)
    assert "usaspending_fpds_prime_award_state" in sql
    assert "current_end_date >= current_date AND is_terminated = FALSE" in sql
    assert "JOIN act ON t.award_key = act.k" in sql
    assert "t.fy >= 2019 AND t.fy <= 2025" in sql


def test_flow_active_only_predicate_uses_aliased_uei() -> None:
    from apps.catalyst_api.src.routers.market_slice_v1 import build_flow_sql
    expr, _, _ = compile_predicates(
        {"predicates": [{"term": "registered_in_state", "states": ["CA"]}]}
    )
    sql = build_flow_sql(expr, 2019, 2025, active_only=True)
    assert "t.uei IN (" in sql and "physical_state IN ('CA')" in sql


# ── firm profile (the award-drawer flip) ─────────────────────────────────────
def test_firm_uei_guard_accepts_and_normalizes() -> None:
    from apps.catalyst_api.src.routers.market_slice_v1 import _safe_uei
    assert _safe_uei("wbbsnba9gbl7") == "WBBSNBA9GBL7"
    assert _safe_uei(" WBBSNBA9GBL7 ") == "WBBSNBA9GBL7"


def test_firm_uei_guard_refuses_injection_and_shape() -> None:
    import pytest
    from fastapi import HTTPException
    from apps.catalyst_api.src.routers.market_slice_v1 import _safe_uei
    for bad in ("", "short", "evil' OR '1'='1", "WBBSNBA9GBL7X", "WBBSNBA9GBL", None):
        with pytest.raises(HTTPException):
            _safe_uei(bad)


def test_firm_fy_series_never_shows_fy26() -> None:
    from apps.catalyst_api.src.routers import market_slice_v1 as m
    assert m._FIRM_FY_END == 2025, "FY2026 does not exist on camera (operator ruling)"
    assert m._FIRM_FY_START == 2001


# ── award lens x market composition (semantics (b), 2026-07-22) ──────────────
def test_awards_sql_pair_scope_rides_both_statements() -> None:
    from apps.catalyst_api.src.routers.market_slice_v1 import build_awards_sql
    points, count = build_awards_sql(None, 100, pairs=[("237310", "Y1DA"), ("237990", "Z2BB")])
    for stmt in (points, count):
        assert "pairs(naics_code, psc_code) AS (VALUES ('237310','Y1DA'),('237990','Z2BB'))" in stmt
        assert "JOIN pairs pr ON a.naics_code = pr.naics_code" in stmt


def test_awards_sql_uei_overlay_leg() -> None:
    from apps.catalyst_api.src.routers.market_slice_v1 import build_awards_sql
    points, count = build_awards_sql(None, 50, uei="WBBSNBA9GBL7")
    assert "a.recipient_uei = 'WBBSNBA9GBL7'" in points and "a.recipient_uei = 'WBBSNBA9GBL7'" in count


def test_awards_sql_unscoped_unchanged() -> None:
    from apps.catalyst_api.src.routers.market_slice_v1 import build_awards_sql
    points, count = build_awards_sql(None, 4000)
    assert "pairs" not in points and "pairs" not in count and "recipient_uei = '" not in points


# ── basis abstraction (agnostic market scope, 2026-07-22) ────────────────────
def test_basis_active_gates_membership_and_money() -> None:
    from apps.catalyst_api.src.routers.market_slice_v1 import build_entities_sql
    sql = build_entities_sql([("237310", "Y1DA")], {"min": 1e6, "max": 1e8}, None, 100, basis="active")
    assert "WHERE TRUE" in sql, "active basis admits any active in-scope firm"
    assert "FILTER (WHERE s.unfin)" not in sql, "active basis money = all in-scope obligations"


def test_basis_default_is_capital_reading() -> None:
    from apps.catalyst_api.src.routers.market_slice_v1 import build_entities_sql
    sql = build_entities_sql([("237310", "Y1DA")], {"min": 1e6, "max": 1e8}, None, 100)
    assert "WHERE s.unfin" in sql and "FILTER (WHERE s.unfin)" in sql


def test_basis_refuses_unknown() -> None:
    import pytest
    from fastapi import HTTPException
    from apps.catalyst_api.src.routers.market_slice_v1 import _parse_basis
    assert _parse_basis({}) == "unfinanced"
    assert _parse_basis({"basis": "active"}) == "active"
    with pytest.raises(HTTPException):
        _parse_basis({"basis": "equipment"})


# ── optional band (rail dial, 2026-07-22) ────────────────────────────────────
def test_entities_no_band_reads_whole_market() -> None:
    from apps.catalyst_api.src.routers.market_slice_v1 import build_entities_sql
    sql = build_entities_sql([("237310", "Y1DA")], None, None, 100)
    assert "band AS (SELECT uei FROM gtm_entity_pricing_mix WHERE TRUE)" in sql
    assert "active_obl >=" not in sql


def test_entities_explicit_band_still_gates() -> None:
    from apps.catalyst_api.src.routers.market_slice_v1 import build_entities_sql
    sql = build_entities_sql([("237310", "Y1DA")], {"min": 1e6, "max": 1e8}, None, 100)
    assert "active_obl >= 1000000.0 AND active_obl <= 100000000.0" in sql


# ── family rollup (entity altitude, 2026-07-22) ──────────────────────────────
def test_family_wrap_folds_to_ultimate_parent() -> None:
    from apps.catalyst_api.src.routers.market_slice_v1 import wrap_family_rollup
    sql = wrap_family_rollup("SELECT 1 AS uei")
    assert "coalesce(h.ultimate_parent_uei, per_uei.uei) AS uei" in sql
    assert "LEFT JOIN entity_hierarchy h" in sql
    assert "count(*) AS members" in sql
    assert "sum(per_uei.unfin_usd)" in sql


def test_rollup_parser() -> None:
    import pytest
    from fastapi import HTTPException
    from apps.catalyst_api.src.routers.market_slice_v1 import _parse_rollup
    assert _parse_rollup({}) == "entity"
    assert _parse_rollup({"rollup": "family"}) == "family"
    with pytest.raises(HTTPException):
        _parse_rollup({"rollup": "parent"})
