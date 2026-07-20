"""sub_market_slice_v1 — scope registry, SQL assembly, predicate integration (pure)."""
import asyncio

import pytest
from fastapi import HTTPException

import apps.catalyst_api.src.routers.sub_market_slice_v1 as sms
from apps.catalyst_api.src.routers.market_query_v1 import compile_predicates
from apps.catalyst_api.src.routers.sub_market_slice_v1 import (
    _load_scopes,
    _resolve,
    build_cohort_cte,
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


def test_count_sql_carries_predicate_leg() -> None:
    expr, _, _ = compile_predicates(
        {"predicates": [{"term": "registered_in_state", "states": ["TX"]}]}
    )
    sql = build_count_sql(PAIRS, expr)
    assert "s.subawardee_uei IN (" in sql and "physical_state IN ('TX')" in sql
    assert sql.strip().endswith("SELECT count(DISTINCT uei) AS subs FROM t")


def test_count_sql_survives_select_round_token_in_predicate() -> None:
    # Regression: the old builder recovered the CTE by splitting the tiles SQL
    # on "SELECT round" — a predicate expr carrying that token truncated the
    # count SQL. The shared CTE builder must keep the full expr intact.
    expr = "SELECT uei FROM m WHERE score > (SELECT round(avg(score), 0) FROM m)"
    sql = build_count_sql(PAIRS, expr)
    assert expr in sql
    assert sql.strip().endswith("SELECT count(DISTINCT uei) AS subs FROM t")


def test_count_and_pack_share_the_cohort_cte() -> None:
    expr, _, _ = compile_predicates(
        {"predicates": [{"term": "registered_in_state", "states": ["TX"]}]}
    )
    cte = build_cohort_cte(PAIRS, expr)
    assert build_count_sql(PAIRS, expr).startswith(cte)
    for section in build_pack_sql(PAIRS, expr).values():
        assert section.startswith(cte)


def test_registry_carries_twelve_unique_cohorts_with_language() -> None:
    s = _load_scopes()
    keys = [k for c in s["cards"].values() for k in (c.get("cohorts") or {})]
    assert len(keys) == 12 and len(set(keys)) == 12
    for c in s["cards"].values():
        card_pairs = {tuple(p) for p in c["pairs"]}
        for co in (c.get("cohorts") or {}).values():
            assert co["family_name"], "vetted-mart name always resolves"
            assert {tuple(p) for p in co["pairs"]} <= card_pairs, "cohort ⊆ card"


def test_cohort_scopes_pack_to_subset() -> None:
    # ≤, not <: a cohort may legitimately span its whole card
    # (equipment-maintenance-overhaul carries a 62/62 cohort).
    from apps.catalyst_api.src.routers.sub_market_slice_v1 import _scope_pairs
    for slug, card in _load_scopes()["cards"].items():
        all_pairs, none = _scope_pairs(card, {})
        assert none is None and len(all_pairs) == len(card["pairs"]), slug
        for key in card.get("cohorts") or {}:
            sub_pairs, cohort = _scope_pairs(card, {"cohort": key})
            assert cohort == key, (slug, key)
            assert 0 < len(sub_pairs) <= len(all_pairs), (slug, key)
            assert {tuple(p) for p in sub_pairs} <= {tuple(p) for p in all_pairs}, (slug, key)


def test_unknown_cohort_refuses_naming_known() -> None:
    from apps.catalyst_api.src.routers.sub_market_slice_v1 import _scope_pairs
    card = _load_scopes()["cards"]["propulsion-engines"]
    with pytest.raises(HTTPException) as e:
        _scope_pairs(card, {"cohort": "vibes"})
    assert "known:" in e.value.detail


# ── endpoint seams (sidecar faked) ───────────────────────────────────────────

_TILE_COLS = ["sub_usd", "sub_usd_12mo", "n_subawards", "n_subs", "n_primes",
              "median_chunk", "p20_chunk", "p80_chunk", "dod_pct"]


@pytest.fixture()
def fake_sidecar(monkeypatch):
    sqls: list[str] = []

    async def fake_run(client, sql, limit=100):
        sqls.append(sql)
        if sql.strip().endswith("SELECT count(DISTINCT uei) AS subs FROM t"):
            return {"columns": ["subs"], "rows": [[42]],
                    "elapsed_ms": 7.5, "artifact": "query_sidecar_TEST"}
        if "AS sub_usd_12mo" in sql:
            return {"columns": _TILE_COLS,
                    "rows": [[1_000_000, 400_000, 10, 7, 3, 90_000, 40_000, 200_000, 55.0]],
                    "artifact": "query_sidecar_TEST"}
        return {"columns": [], "rows": []}

    monkeypatch.setattr(sms, "_run_sidecar", fake_run)
    return sqls


def test_pack_cohort_meta_shape(fake_sidecar) -> None:
    card = _load_scopes()["cards"]["airframes-aerostructures"]
    key = next(iter(card["cohorts"]))
    payload = asyncio.run(sms.pack({"slug": "airframes-aerostructures", "cohort": key}))
    assert payload["cohort"] == key
    meta = payload["cohort_meta"]
    assert set(meta) == {"family_name", "one_liner", "share_of_card_pct"}
    assert meta["family_name"] == card["cohorts"][key]["family_name"]
    assert meta["one_liner"] == card["cohorts"][key]["one_liner"]
    assert meta["share_of_card_pct"] == card["cohorts"][key]["share_of_card_pct"]


def test_pack_without_cohort_meta_is_none(fake_sidecar) -> None:
    payload = asyncio.run(sms.pack({"slug": "airframes-aerostructures"}))
    assert payload["cohort"] is None and payload["cohort_meta"] is None
    assert payload["count"] == 7  # n_subs off the tiles row


def test_pack_cohort_and_predicates_combine(fake_sidecar) -> None:
    card = _load_scopes()["cards"]["airframes-aerostructures"]
    key = next(iter(card["cohorts"]))
    payload = asyncio.run(sms.pack({
        "slug": "airframes-aerostructures", "cohort": key,
        "predicates": [{"term": "registered_in_state", "states": ["TX"]}],
    }))
    assert payload["cohort"] == key
    assert payload["pair_count"] == len(card["cohorts"][key]["pairs"])
    assert payload["predicates"] == [{"term": "registered_in_state", "states": ["TX"]}]
    assert len(fake_sidecar) == 5
    for sql in fake_sidecar:  # every section scoped to the cohort AND the predicate
        assert "s.subawardee_uei IN (" in sql and "physical_state IN ('TX')" in sql
        n, p = card["cohorts"][key]["pairs"][0]
        assert f"('{n}','{p}')" in sql


def test_count_endpoint_response_shape(fake_sidecar) -> None:
    payload = asyncio.run(sms.count({
        "slug": "propulsion-engines",
        "predicates": [{"term": "registered_in_state", "states": ["TX"]}],
    }))
    assert set(payload) == {"slug", "cohort", "count", "predicates",
                            "disclosures", "elapsed_ms", "artifact"}
    assert payload["slug"] == "propulsion-engines"
    assert payload["cohort"] is None
    assert payload["count"] == 42
    assert payload["predicates"] == [{"term": "registered_in_state", "states": ["TX"]}]
    assert payload["elapsed_ms"] == 7.5
    assert payload["artifact"] == "query_sidecar_TEST"
    assert len(fake_sidecar) == 1 and "physical_state IN ('TX')" in fake_sidecar[0]
