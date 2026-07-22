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


def test_band_override() -> None:
    assert _parse_band({"band": {"min": 5e6}}) == {"min": 5e6, "max": 1e8}


def test_band_inverted_refuses() -> None:
    with pytest.raises(HTTPException):
        _parse_band({"band": {"min": 2e8}})


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
