"""market-collections pricing breakdown — definition-block guards (pure, no I/O).

The pricing classes, the FFP-no-financing-recorded predicate, the column
contract, and the residual rule are pinned by hq/PLAN_2026-07-16_market-billing-display.md
§2–3. These tests assert the definitions character-for-character against the
module source so a drive-by edit to the SQL cannot silently change the display's
meaning.
"""
import inspect

from apps.edge_api.src.routers import market_collections_v1 as mc

SRC = inspect.getsource(mc)


def test_pricing_class_sets_match_definition() -> None:
    assert "WHEN a.latest_pricing_code IN ('A','B','J','K','L','M') THEN 'fixed'" in SRC
    assert "WHEN a.latest_pricing_code IN ('R','S','T','U','V') THEN 'cost'" in SRC
    assert "WHEN a.latest_pricing_code IN ('Y','Z') THEN 'tm_lh'" in SRC
    assert "ELSE 'other' END" in SRC  # everything else INCLUDING NULL


def test_ffp_unfinanced_predicate() -> None:
    assert (
        "(a.latest_pricing_code = 'J'\n"
        "          AND coalesce(a.latest_financing_code, 'Z') IN ('Z','NOT APPLICABLE'))"
    ) in SRC
    # NOT part of this display's definition (guide pattern (j) carries it):
    assert "latest_business_size" not in SRC


def test_every_pricing_subselect_excludes_vehicles() -> None:
    for alias in (
        "pricing_fixed_value", "pricing_cost_value", "pricing_tm_lh_value",
        "pricing_fixed_ct", "pricing_cost_ct", "pricing_tm_lh_ct",
        "pricing_other_ct", "ffp_unfinanced_value", "ffp_unfinanced_ct",
    ):
        line = next(l for l in SRC.splitlines() if f" AS {alias}" in l)
        assert "WHERE NOT is_vehicle" in line, alias


def test_expected_cols_matches_contract() -> None:
    assert mc.EXPECTED_COLS == (
        "count", "won_total", "won_median", "won_avg", "book_total", "book_median",
        "book_avg", "awards_median", "committed_award_ct", "award_median", "award_avg",
        "runway_total", "runway_median", "runway_avg", "vehicle_seats", "vehicle_holders",
        "vehicle_ceiling_total", "vehicle_headroom_total",
        "pricing_fixed_value", "pricing_cost_value", "pricing_tm_lh_value",
        "pricing_fixed_ct", "pricing_cost_ct", "pricing_tm_lh_ct", "pricing_other_ct",
        "ffp_unfinanced_value", "ffp_unfinanced_ct",
    )
    # pricing_other_value is NEVER a SQL column — it is the Python residual.
    assert "pricing_other_value" not in mc.EXPECTED_COLS
    assert "AS pricing_other_value" not in SRC


def test_residual_rule_sums_exactly() -> None:
    stats = {
        "book_total": 1_000_003.0,
        "pricing_fixed_value": 600_001.0,
        "pricing_cost_value": 250_000.0,
        "pricing_tm_lh_value": 100_000.0,
    }
    mc._add_pricing_other(stats)
    assert stats["pricing_other_value"] == 50_002.0
    assert (
        stats["pricing_fixed_value"] + stats["pricing_cost_value"]
        + stats["pricing_tm_lh_value"] + stats["pricing_other_value"]
        == stats["book_total"]
    )


def test_residual_rule_zero_book() -> None:
    stats = dict.fromkeys(mc.EXPECTED_COLS, 0)
    mc._add_pricing_other(stats)
    assert stats["pricing_other_value"] == 0
