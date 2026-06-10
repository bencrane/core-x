"""Smoke + unit tests for the FEDERAL tool group (apps/gtm_mcp/src/tools/federal.py).

Pure composition + local-DuckDB, NO R2: these pin the SQL the federal tools emit, the
input-validation guards, the index-pushdown predicate shapes, and the canonical
name→blocking-key normalization (computed in a local in-memory DuckDB so it is
byte-identical to the indexed ``legal_name_base`` spine — no live Lance read). The live
R2 verification (real spend-by-industry, UEI/name lookups, filter list) is done out of
band; this suite is the deterministic regression net that runs without credentials.

It also asserts the SEPARATION invariant: the federal module never imports or mutates
the CRM audience tools, and registers its own distinct tool group.
"""

from __future__ import annotations

import duckdb
import pytest

from apps.gtm_mcp.src.tools import federal
from core.name_norm import legal_name_base, name_norm


# ── Input validation guards ──────────────────────────────────────────────────
def test_state_guard():
    assert federal._norm_state(None) is None
    assert federal._norm_state("ca") == "CA"
    assert federal._norm_state(" tx ") == "TX"
    for bad in ("CAL", "C", "1A", "evil' OR '1'='1"):
        with pytest.raises(ValueError):
            federal._norm_state(bad)


def test_naics_prefix_guard():
    assert federal._norm_naics_prefix(None) is None
    assert federal._norm_naics_prefix("33") == "33"
    assert federal._norm_naics_prefix("336411") == "336411"
    for bad in ("3", "3364110", "33A", "', '"):
        with pytest.raises(ValueError):
            federal._norm_naics_prefix(bad)


def test_sql_str_escapes_single_quote():
    assert federal._sql_str("O'Brien") == "'O''Brien'"


# ── Filter composition (predicate shape / index pushdown) ────────────────────
def test_gold_filter_pushes_indexed_predicates():
    clauses = federal._gold_filter(
        min_obligation=5_000_000, state="CA", naics_prefix="3364", active_only=True
    )
    joined = " AND ".join(clauses)
    assert "total_lifetime_obligations >= 5000000.0" in joined
    assert "physical_address_state = 'CA'" in joined
    assert "primary_naics LIKE '3364%'" in joined        # prefix → LIKE
    assert "is_active = TRUE" in joined                    # BITMAP


def test_gold_filter_full_naics_is_equality():
    # a full 6-digit code pushes onto the BTREE as '=' (point), not LIKE
    clauses = federal._gold_filter(naics_prefix="336411")
    assert "primary_naics = '336411'" in clauses


def test_gold_filter_empty_when_unconstrained():
    assert federal._gold_filter() == []


# ── Name → canonical blocking key (matches the indexed legal_name_base spine) ─
def test_blocking_key_matches_legal_name_base_spine():
    """The key the tool pushes into the BTREE must equal legal_name_base(name_norm(x))
    — computed here in a local DuckDB exactly as the tool does (no Python re-impl)."""
    con = duckdb.connect(":memory:")

    def key(raw: str):
        lit = "'" + raw.replace("'", "''") + "'"
        return con.execute("SELECT " + legal_name_base(name_norm(lit))).fetchone()[0]

    # bare brand: no trailing designator to strip
    assert key("Lockheed Martin") == "LOCKHEED MARTIN"
    assert key("LOCKHEED MARTIN") == "LOCKHEED MARTIN"   # case-insensitive
    # 'CO' / 'INC' ARE stripped designators; 'CORPORATION' is NOT
    assert key("Boeing Co.") == "BOEING"
    assert key("Acme, Inc.") == "ACME"
    assert key("Lockheed Martin Corporation") == "LOCKHEED MARTIN CORPORATION"


# ── Empty-input fast paths (no R2 touched) ───────────────────────────────────
def test_search_entity_by_uei_empty_returns_not_found():
    assert federal.search_entity_by_uei("") == {"uei": None, "found": False, "entity": None}
    assert federal.search_entity_by_uei("   ") == {"uei": None, "found": False, "entity": None}


def test_search_entity_by_name_empty_returns_empty():
    out = federal.search_entity_by_name("")
    assert out["match_count"] == 0 and out["entities"] == []


# ── Projection contracts (wire shape stability) ──────────────────────────────
def test_list_projection_columns_are_the_frontend_contract():
    assert federal._LIST_COLUMNS == [
        "uei", "legal_name_base", "physical_address_city", "physical_address_state",
        "primary_naics", "total_lifetime_obligations", "total_active_obligations",
        "active_award_count", "has_federal_awards",
    ]


def test_profile_projection_carries_four_name_forms_and_pocs():
    for col in (
        "legal_business_name", "dba_name", "normalized_legal_name", "legal_name_base",
        "pocs", "primary_naics", "total_lifetime_obligations", "total_active_obligations",
    ):
        assert col in federal._PROFILE_COLUMNS


# ── Separation invariant: federal group is distinct from the CRM group ───────
def test_federal_registers_its_own_distinct_group():
    registered: list[str] = []

    class _MCP:
        def add_tool(self, fn):
            registered.append(fn.__name__)

    federal.register(_MCP())
    assert set(registered) == {
        "federal_spend_by_industry", "federal_spend_by_state", "federal_spend_by_agency",
        "search_entity_by_uei", "search_entity_by_name", "federal_entities_by_filter",
    }
    # The federal group shares NO tool name with the CRM audience group.
    from apps.gtm_mcp.src.tools import audience

    crm: list[str] = []

    class _CRM_MCP:
        def add_tool(self, fn):
            crm.append(fn.__name__)

    audience.register(_CRM_MCP())
    assert set(registered).isdisjoint(set(crm))
    # And the CRM tools still import + expose their unchanged surface.
    assert {"search_company_by_domain", "search_people_by_domain", "search_company_by_name"} <= set(crm)
