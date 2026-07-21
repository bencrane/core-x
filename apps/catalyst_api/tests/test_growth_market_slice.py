"""growth_market_slice_v1 — lane registry, growth-dial validation, SQL assembly (pure)."""
import pytest
from fastapi import HTTPException

from apps.catalyst_api.src.routers.growth_market_slice_v1 import (
    LANES,
    _resolve,
    build_count_sql,
    build_pack_sql,
    parse_growth,
)
from apps.catalyst_api.src.routers.market_query_v1 import compile_predicates

LANE = "construction-vertical-building"


def test_registry_matches_the_mart_lane_keys() -> None:
    assert set(LANES) == {
        "construction-vertical-building", "construction-building-repair-alteration",
        "construction-building-maintenance", "construction-civil-infrastructure",
        "construction-industrial-defense-facilities",
    }
    for meta in LANES.values():
        assert meta["title"] and meta["members_are"]


def test_unknown_lane_refuses() -> None:
    with pytest.raises(HTTPException) as e:
        _resolve("construction-vibes")
    assert "catalog" in e.value.detail


# ── growth dial validation ───────────────────────────────────────────────────

def test_growth_none_at_rest() -> None:
    assert parse_growth({}) is None and parse_growth({"growth": {}}) is None


def test_growth_defaults() -> None:
    g = parse_growth({"growth": {"multiple": 2}})
    assert g == {"multiple": 2.0, "recent_months": 12, "baseline_months": 24,
                 "band_min": 1_000_000.0, "band_max": 1_000_000_000.0, "new_entrants": False}


@pytest.mark.parametrize("bad", [
    {"multiple": 0.5}, {"multiple": "4x"}, {"recent_months": 0},
    {"baseline_months": 999}, {"band_min": -1}, {"band_min": 5e9, "band_max": 1e9},
    {"new_entrants": "yes"},
])
def test_growth_bad_dials_refuse(bad) -> None:
    with pytest.raises(HTTPException):
        parse_growth({"growth": bad})


# ── SQL assembly ─────────────────────────────────────────────────────────────

def test_at_rest_is_the_full_market() -> None:
    tiles = build_pack_sql(LANE, None, None)["tiles"]
    assert "w.recent > 0" in tiles            # every active firm, no gates
    assert "multiple" not in tiles
    assert f"lane = '{LANE}'" in tiles


def test_every_window_is_watermark_anchored() -> None:
    for sql in build_pack_sql(LANE, parse_growth({"growth": {"multiple": 3}}), None).values():
        assert "max(month)" in sql
        assert "current_date" not in sql


def test_growth_gate_annualizes_and_bands() -> None:
    g = parse_growth({"growth": {"multiple": 4, "recent_months": 12, "baseline_months": 24,
                                 "band_min": 1e6, "band_max": 1e9}})
    tiles = build_pack_sql(LANE, g, None)["tiles"]
    assert "w.recent >= 4.0 * w.baseline * 0.5" in tiles     # 12/24 annualization
    assert "w.recent >= 1000000.0 AND w.recent <= 1000000000.0" in tiles
    assert "w.baseline > 0" in tiles                          # new entrants never blend in


def test_new_entrants_is_a_separate_set() -> None:
    g = parse_growth({"growth": {"new_entrants": True}})
    tiles = build_pack_sql(LANE, g, None)["tiles"]
    assert "w.baseline IS NULL" in tiles and "multiple" not in tiles


def test_predicates_intersect_on_uei() -> None:
    expr, _, _ = compile_predicates(
        {"predicates": [{"term": "registered_in_state", "states": ["TX"]}]}
    )
    tiles = build_pack_sql(LANE, None, expr)["tiles"]
    assert "w.uei IN (" in tiles and "physical_state IN ('TX')" in tiles


def test_count_is_firms_only() -> None:
    sql = build_count_sql(LANE, parse_growth({"growth": {"multiple": 2}}), None)
    assert sql.strip().endswith("SELECT count(*) AS firms FROM m")
    assert "median" not in sql


def test_catalog_sql_is_watermark_anchored_and_lane_grouped() -> None:
    from apps.catalyst_api.src.routers.growth_market_slice_v1 import _CATALOG_SQL
    assert "max(month)" in _CATALOG_SQL and "current_date" not in _CATALOG_SQL
    assert "GROUP BY 1" in _CATALOG_SQL and "median" in _CATALOG_SQL
