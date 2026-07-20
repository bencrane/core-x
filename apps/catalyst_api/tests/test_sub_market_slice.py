"""sub_market_slice_v1 — scope registry, SQL assembly, predicate integration (pure)."""
import pytest
from fastapi import HTTPException

from apps.catalyst_api.src.routers.market_query_v1 import compile_predicates
from apps.catalyst_api.src.routers.sub_market_slice_v1 import (
    _load_scopes,
    _resolve,
    build_count_sql,
    build_pack_sql,
)

PAIRS = [("336411", "1510"), ("336413", "1680")]


def test_registry_carries_six_cards_with_pairs_and_provenance() -> None:
    s = _load_scopes()
    assert set(s["cards"]) == {
        "defense-rd-programs", "professional-engineering-services",
        "airframes-aerostructures", "propulsion-engines",
        "equipment-maintenance-overhaul", "federal-it-applications",
    }
    for card in s["cards"].values():
        assert card["pairs"], "explicit pairs always"
        assert card["verified"]["pair_coverage_pct"] >= 99.0
        for n, p in card["pairs"]:
            assert len(n) == 6 and len(p) == 4


def test_unknown_slug_refuses_with_catalog_pointer() -> None:
    with pytest.raises(HTTPException) as e:
        _resolve("crypto-vibes")
    assert "catalog" in e.value.detail


def test_pack_sections_complete() -> None:
    sql = build_pack_sql(PAIRS, None)
    assert set(sql) == {"tiles", "top_primes", "monthly_series", "states", "top_pairs"}


def test_cohort_encodes_card_definition() -> None:
    tiles = build_pack_sql(PAIRS, None)["tiles"]
    assert "('336411','1510')" in tiles
    assert "BETWEEN DATE '2022-10-01' AND DATE '2025-09-30'" in tiles
    assert "subaward_amount_num > 0" in tiles
    assert "Department of Defense%" in tiles          # DoD share channel
    assert "DATE '2024-10-01'" in tiles               # trailing-12 tile


def test_predicates_restrict_the_sub_cohort() -> None:
    expr, _, _ = compile_predicates(
        {"predicates": [{"term": "registered_in_state", "states": ["TX"]}]}
    )
    tiles = build_pack_sql(PAIRS, expr)["tiles"]
    assert "s.subawardee_uei IN (" in tiles and "physical_state IN ('TX')" in tiles


def test_no_predicates_no_leg() -> None:
    assert "subawardee_uei IN (" not in build_pack_sql(PAIRS, None)["tiles"]


def test_count_is_distinct_subs_only() -> None:
    sql = build_count_sql(PAIRS, None)
    assert sql.strip().endswith("SELECT count(DISTINCT uei) AS subs FROM t")
    assert "median" not in sql
