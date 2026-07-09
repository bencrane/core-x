"""sidecar_executor — execute compiled market-query plans against query-sidecar-api.

Phase 4 of the query-sidecar plan. The phrase.v2 / market-query compilers are
untouched: this module translates the SAME compiled plan (the same predicate
strings `compile_market_filters` / `compile_table_filters` emit — they are
plain SQL fragments valid in DuckDB) into one or two SQL statements against the
warm sidecar endpoint, returning byte-shape-identical envelopes to the
market_store executors. Measured basis: docs/plans/QUERY_SIDECAR_PHASE2_BENCHMARK.md
(14/14 parity, every family ≤134 ms vs 0.4–74 s on the Lance lane).

Serving tiers (what runs here vs. stays on the Lance path):
- entity grain                      → sidecar (lanes/inferred/rollup INTERSECT + hydrate join)
- prime_awards rows + collapse      → sidecar (usaspending_fpds_prime_award_state is in the artifact)
- transactions collapse             → sidecar (gtm_txn_events_slim + name join; collapse needs only
                                      uei/$/date — and the sidecar aggregation is EXACT, never
                                      scan-capped, unlike the 500k-row streamed live lane)
- transactions ROW queries          → NotServable → caller falls back to Lance
                                      (events_slim lacks names/descriptions; the live lane is
                                      already sub-second on these)

Wiring contract (the shims in market_store):
    enabled()  — flag + config present
    NotServable — raise → caller falls through to the Lance body
    any other exception — caller logs and falls through (the lane can never be
                          worse than the status quo)

Config (via Doppler → env, same channel as every catalyst secret):
    QUERY_SIDECAR_URL      e.g. https://query-sidecar-api.onrender.com
    QUERY_SIDECAR_TOKEN    bearer for /api/v1/sql
    QUERY_SIDECAR_EXECUTE  'on' to enable (default off — merge-safe)
"""

from __future__ import annotations

import json
import os
import urllib.request
from datetime import date as dt_date
from typing import Any

from . import market_registry

_SQL_TIMEOUT_S = 30

# sidecar table names (the artifact's tables, docs/plans/SIDECAR_PHASE0_MART_MANIFEST.md)
_T_ENTITIES = "gtm_sam_entities"
_T_ROLLUP = "gtm_entity_behavior_rollup"
_T_LANES = "gtm_entity_code_lanes"
_T_GEO = "gtm_entity_geo"
_T_INFERRED = {
    "inferred_primeable": "gtm_entity_inferred_primeable_codes",
    "inferred_subbable": "gtm_entity_inferred_subbable_codes",
}
_T_AWARD_STATE = "usaspending_fpds_prime_award_state"
_T_TXN_SLIM = "gtm_txn_events_slim"

# events_slim column mapping for transactions-collapse predicates: the compiled
# predicate speaks canonical-txn column names; the slim projection renames two.
_TXN_SLIM_RENAMES = (
    ("federal_action_obligation", "obligation"),
    ("product_or_service_code", "psc_code"),
    ("recipient_uei", "uei"),
)


class NotServable(Exception):
    """This plan shape is deliberately not served by the sidecar — fall back."""


def enabled() -> bool:
    return (os.environ.get("QUERY_SIDECAR_EXECUTE", "").lower() == "on"
            and bool(os.environ.get("QUERY_SIDECAR_URL"))
            and bool(os.environ.get("QUERY_SIDECAR_TOKEN")))


def _sql(sql: str, limit: int) -> dict[str, Any]:
    req = urllib.request.Request(
        os.environ["QUERY_SIDECAR_URL"].rstrip("/") + "/api/v1/sql",
        data=json.dumps({"sql": sql, "limit": limit}).encode(),
        headers={"content-type": "application/json",
                 "authorization": f"Bearer {os.environ['QUERY_SIDECAR_TOKEN']}"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=_SQL_TIMEOUT_S) as resp:  # noqa: S310 — config-controlled host
        return json.loads(resp.read())


def _rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    cols = payload["columns"]
    return [dict(zip(cols, r)) for r in payload["rows"]]


def _one(payload: dict[str, Any]) -> Any:
    return payload["rows"][0][0]


def _jsonable(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    # the sidecar returns JSON already (dates serialized by FastAPI) — passthrough hook
    return rows


# ── entity grain ──────────────────────────────────────────────────────────────

def _entity_legs(predicates, lanes, inferred) -> list[str]:
    from . import market_store  # late import — market_store shims import us

    legs: list[str] = []
    for lane in lanes:
        legs.append(f"SELECT DISTINCT uei FROM {_T_LANES} "
                    f"WHERE {market_store._compile_lane_predicate(lane)}")
    for inf in inferred:
        legs.append(f"SELECT DISTINCT uei FROM {_T_INFERRED[inf['kind']]} "
                    f"WHERE {market_store._compile_inferred_predicate(inf)}")
    if predicates["rollup"] is not None:
        legs.append(f"SELECT uei FROM {_T_ROLLUP} WHERE {predicates['rollup']}")
    if predicates["entities"] is not None:
        legs.append(f"SELECT uei FROM {_T_ENTITIES} WHERE {predicates['entities']}")
    return legs


_HYDRATE_SQL = None


def _hydrate_select() -> str:
    """SELECT list reproducing market_store._hydrate's RESULT_ROW_ORDER row."""
    global _HYDRATE_SQL
    if _HYDRATE_SQL is None:
        parts = []
        ent = set(market_registry.RESULT_COLUMNS_ENTITIES)
        rol = set(market_registry.RESULT_COLUMNS_ROLLUP)
        geo = set(market_registry.RESULT_COLUMNS_GEO)
        for col in market_registry.RESULT_ROW_ORDER:
            if col == "uei":
                parts.append("f.uei AS uei")
            elif col in ent:
                parts.append(f"e.{col} AS {col}")
            elif col in rol:
                parts.append(f"r.{col} AS {col}")
            elif col in geo:
                parts.append(f"g.{col} AS {col}")
        _HYDRATE_SQL = ", ".join(parts)
    return _HYDRATE_SQL


def execute_entity_query(filters: list[dict[str, Any]], limit: int | None,
                         today: "dt_date | None" = None) -> dict[str, Any]:
    from . import market_store

    predicates, lanes, inferred, executed = market_store.compile_market_filters(filters, today)
    cap = max(1, min(limit or market_store.MARKET_DEFAULT_LIMIT, market_store.MARKET_HARD_ROW_CAP))
    legs = _entity_legs(predicates, lanes, inferred)

    base = " INTERSECT ".join(legs) if legs else f"SELECT uei FROM {_T_ENTITIES}"
    total = int(_one(_sql(f"SELECT count(*) FROM ({base})", 1)))
    if total == 0:
        rows: list[dict[str, Any]] = []
    else:
        hydrate = (
            f"WITH f AS (SELECT uei FROM ({base}) ORDER BY uei LIMIT {cap}) "
            f"SELECT {_hydrate_select()} FROM f "
            f"LEFT JOIN {_T_ENTITIES} e USING (uei) "
            f"LEFT JOIN {_T_ROLLUP} r USING (uei) "
            f"LEFT JOIN {_T_GEO} g USING (uei) ORDER BY f.uei"
        )
        rows = _jsonable(_rows(_sql(hydrate, cap)))
    return {"rows": rows, "total": total, "returned": len(rows),
            "capped": total > len(rows),
            "executed": {"grain": "entity", "filters": executed, "limit": cap}}


# ── table grains ──────────────────────────────────────────────────────────────

def execute_table_query(spec, filters: list[dict[str, Any]], limit: int | None,
                        today: "dt_date | None" = None) -> dict[str, Any]:
    from . import market_store

    if spec.source != "prime_awards":
        raise NotServable(f"table rows for {spec.source} stay on the Lance path")
    predicate, executed = market_store.compile_table_filters(spec, filters, today)
    cap = max(1, min(limit or market_store.MARKET_DEFAULT_LIMIT, market_store.MARKET_HARD_ROW_CAP))
    total = int(_one(_sql(f"SELECT count(*) FROM {_T_AWARD_STATE} WHERE {predicate}", 1)))
    rows: list[dict[str, Any]] = []
    if total:
        cols = ", ".join(f"t.{c}" for c in spec.result_columns)
        geo_cols = ""
        join = ""
        if spec.geometry == "recipient_hq":
            geo_cols = ", g.latitude AS latitude, g.longitude AS longitude, g.geo_precision AS geo_precision"
            join = f" LEFT JOIN {_T_GEO} g ON g.uei = t.recipient_uei"
        q = (f"SELECT {cols}{geo_cols} FROM {_T_AWARD_STATE} t{join} "
             f"WHERE {predicate} LIMIT {cap}")
        rows = _jsonable(_rows(_sql(q, cap)))
    return {"rows": rows, "total": total, "returned": len(rows),
            "capped": total > len(rows),
            "executed": {"grain": spec.grain, "filters": executed, "limit": cap}}


def execute_table_collapse(spec, filters: list[dict[str, Any]], limit: int | None,
                           today: "dt_date | None" = None) -> dict[str, Any]:
    from . import market_store

    predicate, executed = market_store.compile_table_filters(spec, filters, today)
    cap = max(1, min(limit or market_store.MARKET_DEFAULT_LIMIT, market_store.MARKET_HARD_ROW_CAP))

    if spec.source == "prime_awards":
        table, uei_col, name_expr, money_col, date_col, name_join = (
            _T_AWARD_STATE, "recipient_uei", "any_value(t.recipient_name)",
            "life_to_date_obligated", "last_action_date", "")
    elif spec.source == "transactions":
        for old, new in _TXN_SLIM_RENAMES:
            predicate = predicate.replace(old, new)
        table, uei_col, money_col, date_col = _T_TXN_SLIM, "uei", "obligation", "action_date"
        name_expr = "any_value(e.legal_business_name)"
        name_join = f" LEFT JOIN {_T_ENTITIES} e ON e.uei = t.{uei_col}"
    else:
        raise NotServable(f"collapse for {spec.source} not mapped")

    total_rows = int(_one(_sql(f"SELECT count(*) FROM {table} t WHERE {predicate}", 1)))
    rows: list[dict[str, Any]] = []
    distinct = 0
    if total_rows:
        distinct = int(_one(_sql(
            f"SELECT count(DISTINCT t.{uei_col}) FROM {table} t WHERE {predicate}", 1)))
        q = (
            f"SELECT t.{uei_col} AS uei, {name_expr} AS recipient_name, "
            f"count(*) AS match_ct, round(sum(COALESCE(t.{money_col}, 0)), 2) AS amt_total, "
            f"max(t.{date_col}) AS last_action_date "
            f"FROM {table} t{name_join} WHERE {predicate} AND t.{uei_col} IS NOT NULL "
            f"GROUP BY t.{uei_col} ORDER BY amt_total DESC, uei LIMIT {cap}"
        )
        rows = _jsonable(_rows(_sql(q, cap)))
    return {"rows": rows, "total_rows": total_rows, "distinct_recipients": distinct,
            "returned": len(rows), "capped": distinct > len(rows),
            # the sidecar aggregates the FULL filtered set — never scan-capped
            "scanned_rows": total_rows, "scan_capped": False,
            "executed": {"grain": spec.grain, "collapse": "recipients",
                         "filters": executed, "limit": cap}}
