"""Tests for the raw-SQL-lane index pushdown (database.extract_prefilters +
prefiltered registration in database.query).

WHY. Registering a ``LanceDataset`` as a DuckDB relation applies WHERE predicates
but never engages the Lance BTREE/BITMAP indices (the §4.5 interop footgun) — so
the join lane recovers each dataset's own single-table conjuncts from the SQL,
pushes them through the Lance scanner (``scan_table``), and registers the
pre-shrunk Arrow slice instead. These tests pin:

  * EXTRACTION — which conjuncts are recovered (qualified/unqualified attribution,
    IN / BETWEEN / LIKE / IS NULL / bool columns, literal rendering) and, more
    importantly, which are REFUSED (multi-reference datasets, CTE shadowing, outer
    joins, type-mismatched comparisons, functions, subqueries) — every refusal is
    a fall-back to full registration, never a wrong slice.
  * EQUIVALENCE — the prefiltered lane returns byte-identical rows to the full
    lane on a representative multi-hop join.
  * ENGAGEMENT — the slice path (``scan_table``) is actually hit with the expected
    filter, and skipped when disabled / oversized.
  * EXPLICIT FILTERS — ``prefilters=`` are semantic: applied before the SQL,
    unknown keys error, inapplicable filters raise instead of silently widening.
  * The read-only gate is untouched.

Local Lance fixtures stand in for the R2 sink: ``get_registry`` /
``_cached_dataset`` / ``get_connection`` are monkeypatched so no credentials or
network are needed (same style as test_sam_entities.py).
"""

from __future__ import annotations

import duckdb
import lance
import pyarrow as pa
import pytest

from apps.gtm_mcp.src import database

COMPANIES = [
    {"company_id": 1, "uei": "UEIAAA", "company_name": "Acme Federal", "state": "TX",
     "is_active": True, "revenue": 5_000_000.0},
    {"company_id": 2, "uei": "UEIBBB", "company_name": "Beta Systems", "state": "VA",
     "is_active": True, "revenue": 1_000_000.0},
    {"company_id": 3, "uei": "UEICCC", "company_name": "Gamma Labs", "state": "TX",
     "is_active": False, "revenue": 250_000.0},
    {"company_id": 4, "uei": "UEIDDD", "company_name": "Delta Corp", "state": "CA",
     "is_active": True, "revenue": 9_000_000.0},
]
AWARDS = [
    {"recipient_uei": "UEIAAA", "total_dollars": 2_500_000.0, "action_date": "2023-05-01",
     "agency": "DOD"},
    {"recipient_uei": "UEIAAA", "total_dollars": 400_000.0, "action_date": "2022-01-15",
     "agency": "GSA"},
    {"recipient_uei": "UEIBBB", "total_dollars": 9_000_000.0, "action_date": "2024-02-02",
     "agency": "DOE"},
    {"recipient_uei": "UEICCC", "total_dollars": 50_000.0, "action_date": "2023-09-09",
     "agency": "DOD"},
    {"recipient_uei": "UEIDDD", "total_dollars": 7_000_000.0, "action_date": "2023-11-11",
     "agency": "NASA"},
]
LOOKALIKES = [
    {"uei": "UEIAAA", "score": 0.91},
    {"uei": "UEIBBB", "score": 0.55},
    {"uei": "UEIDDD", "score": 0.88},
]


@pytest.fixture()
def wired(tmp_path, monkeypatch):
    paths = {
        "companies": str(tmp_path / "companies.lance"),
        "usaspending/award_search": str(tmp_path / "award_search.lance"),
        "lookalikes": str(tmp_path / "lookalikes.lance"),
    }
    lance.write_dataset(pa.Table.from_pylist(COMPANIES), paths["companies"], mode="create")
    lance.write_dataset(pa.Table.from_pylist(AWARDS), paths["usaspending/award_search"],
                        mode="create")
    lance.write_dataset(pa.Table.from_pylist(LOOKALIKES), paths["lookalikes"], mode="create")
    # Real BTREE indices on the join/filter anchors, mirroring the prod plane.
    lance.dataset(paths["companies"]).create_scalar_index("uei", index_type="BTREE")
    lance.dataset(paths["usaspending/award_search"]).create_scalar_index(
        "recipient_uei", index_type="BTREE")

    monkeypatch.setattr(database, "get_registry", lambda refresh=False: dict(paths))
    monkeypatch.setattr(database, "_cached_dataset", lambda uri: lance.dataset(uri))
    con = duckdb.connect(":memory:")
    monkeypatch.setattr(database, "get_connection", lambda: con)
    monkeypatch.setattr(database, "_hqx_attached", False)
    monkeypatch.setattr(database, "_PREFILTER_ENABLED", True)
    yield paths
    con.close()


MULTIHOP = """
    SELECT c.company_name, a.total_dollars
    FROM companies c
    JOIN "usaspending/award_search" a ON a.recipient_uei = c.uei
    JOIN lookalikes lk ON lk.uei = c.uei
    WHERE c.state = 'TX' AND c.is_active
      AND a.total_dollars > 1000000 AND a.action_date >= '2023-01-01'
      AND lk.score BETWEEN 0.8 AND 1.0
    ORDER BY a.total_dollars DESC
"""


# ── extraction ────────────────────────────────────────────────────────────────
def test_extract_multihop_per_dataset_conjuncts(wired):
    out = database.extract_prefilters(MULTIHOP, set(wired))
    assert out["companies"] == "(state = 'TX') AND (is_active)"
    assert out["usaspending/award_search"] == \
        "(total_dollars > 1000000) AND (action_date >= '2023-01-01')"
    assert out["lookalikes"] == "score BETWEEN 0.8 AND 1.0"


def test_extract_unqualified_single_table_and_shapes(wired):
    out = database.extract_prefilters(
        "SELECT * FROM companies WHERE state IN ('TX','CA') AND company_name LIKE 'A%'"
        " AND revenue IS NOT NULL AND NOT is_active",
        set(wired),
    )
    assert out["companies"] == ("(state IN ('TX', 'CA')) AND (company_name LIKE 'A%')"
                                " AND (revenue IS NOT NULL) AND (NOT (is_active))")


def test_extract_inner_join_on_condition_and_or(wired):
    out = database.extract_prefilters(
        """SELECT 1 FROM companies c
           JOIN lookalikes lk ON lk.uei = c.uei AND lk.score > 0.8
           WHERE c.state = 'TX' OR c.state = 'CA'""",
        set(wired),
    )
    assert out["lookalikes"] == "score > 0.8"
    assert out["companies"] == "(state = 'TX' OR state = 'CA')"


def test_extract_case_insensitive_rendered_with_stored_casing(wired):
    out = database.extract_prefilters(
        "SELECT * FROM Companies WHERE STATE = 'TX'", set(wired))
    assert out == {"companies": "state = 'TX'"}


def test_extract_explain_lead_is_stripped(wired):
    plain = database.extract_prefilters(MULTIHOP, set(wired))
    assert database.extract_prefilters("EXPLAIN " + MULTIHOP, set(wired)) == plain
    assert database.extract_prefilters("EXPLAIN ANALYZE " + MULTIHOP, set(wired)) == plain


def test_extract_refuses_multi_reference(wired):
    out = database.extract_prefilters(
        """SELECT * FROM companies a JOIN companies b ON a.uei = b.uei
           WHERE a.state = 'TX'""",
        set(wired),
    )
    assert "companies" not in out


def test_extract_refuses_cte_shadowing(wired):
    out = database.extract_prefilters(
        """WITH companies AS (SELECT 1 AS state)
           SELECT * FROM companies WHERE state = 'TX'""",
        set(wired),
    )
    assert out == {}


def test_extract_refuses_outer_join_scope(wired):
    out = database.extract_prefilters(
        """SELECT * FROM companies c
           LEFT JOIN lookalikes lk ON lk.uei = c.uei
           WHERE c.state = 'TX' AND lk.score IS NULL""",
        set(wired),
    )
    assert out == {}


def test_extract_refuses_unsafe_conjuncts_keeps_siblings(wired):
    out = database.extract_prefilters(
        """SELECT * FROM companies
           WHERE state = 'TX'                     -- eligible
             AND lower(company_name) = 'acme'     -- function → refused
             AND state = 5                        -- type mismatch → refused
             AND uei IN (SELECT uei FROM lookalikes)  -- subquery → refused
             AND missing_col = 'x'                -- not in schema → refused
        """,
        set(wired),
    )
    assert out["companies"] == "state = 'TX'"
    # the subquery's own scope still harvests its dataset
    assert "lookalikes" not in out  # no conjuncts on it


def test_extract_refuses_unqualified_in_multi_table_scope(wired):
    out = database.extract_prefilters(
        "SELECT * FROM companies c, lookalikes lk WHERE state = 'TX' AND lk.score > 0.8",
        set(wired),
    )
    assert "companies" not in out
    assert out["lookalikes"] == "score > 0.8"


def test_extract_harvests_inside_subquery_and_cte_scopes(wired):
    out = database.extract_prefilters(
        """WITH tx AS (SELECT uei FROM companies WHERE state = 'TX')
           SELECT count(*) FROM (SELECT * FROM lookalikes WHERE score > 0.8) s
           JOIN tx ON tx.uei = s.uei""",
        set(wired),
    )
    assert out == {"companies": "state = 'TX'", "lookalikes": "score > 0.8"}


def test_extract_literal_rendering(wired):
    out = database.extract_prefilters(
        "SELECT * FROM companies WHERE company_name = 'O''Neil & Co' AND revenue > 1.5e6"
        " AND company_id <> -2 AND revenue <= 1234567.89",
        set(wired),
    )
    assert out["companies"] == ("(company_name = 'O''Neil & Co') AND (revenue > 1500000.0)"
                                " AND (company_id <> -2) AND (revenue <= 1234567.89)")


# ── equivalence + engagement through query() ─────────────────────────────────
def _rows(res):
    return [tuple(r.values()) for r in res["rows"]]


def test_query_multihop_prefiltered_equals_full(wired, monkeypatch):
    pre = database.query(MULTIHOP, datasets=set(wired))
    monkeypatch.setattr(database, "_PREFILTER_ENABLED", False)
    full = database.query(MULTIHOP, datasets=set(wired))
    assert _rows(pre) == _rows(full) == [("Acme Federal", 2_500_000.0)]
    assert pre["columns"] == full["columns"]


def test_query_prefilter_engages_slice_path(wired, monkeypatch):
    calls = []
    real = database.scan_table

    def spy(name, **kw):
        calls.append((name, kw.get("filter")))
        return real(name, **kw)

    monkeypatch.setattr(database, "scan_table", spy)
    database.query(MULTIHOP, datasets=set(wired))
    assert dict((n, f) for n, f in calls) == {
        "companies": "(state = 'TX') AND (is_active)",
        "usaspending/award_search":
            "(total_dollars > 1000000) AND (action_date >= '2023-01-01')",
        "lookalikes": "score BETWEEN 0.8 AND 1.0",
    }


def test_query_row_guard_falls_back_to_full(wired, monkeypatch):
    monkeypatch.setattr(database, "_PREFILTER_MAX_ROWS", 0)
    res = database.query(MULTIHOP, datasets=set(wired))
    assert _rows(res) == [("Acme Federal", 2_500_000.0)]  # correct via the full lane


def test_query_disabled_skips_slice_path(wired, monkeypatch):
    monkeypatch.setattr(database, "_PREFILTER_ENABLED", False)
    monkeypatch.setattr(database, "scan_table",
                        lambda *a, **k: pytest.fail("slice path must not run when disabled"))
    res = database.query(MULTIHOP, datasets=set(wired))
    assert _rows(res) == [("Acme Federal", 2_500_000.0)]


def test_query_self_join_correct_rows(wired):
    # Multi-reference: no prefilter may apply, and the registered relation must
    # survive TWO scans (the one-shot-reader footgun this lane must never reintroduce).
    res = database.query(
        """SELECT count(*) AS n FROM companies a JOIN companies b ON a.uei = b.uei
           WHERE a.state = 'TX'""",
        datasets={"companies"},
    )
    assert res["rows"][0]["n"] == 2


# ── explicit prefilters (semantic) ───────────────────────────────────────────
def test_explicit_prefilter_restricts_result(wired):
    res = database.query(
        "SELECT company_name FROM companies ORDER BY company_name",
        datasets={"companies"},
        prefilters={"companies": "state = 'TX' AND is_active"},
    )
    assert _rows(res) == [("Acme Federal",)]


def test_explicit_prefilter_unknown_key_raises(wired):
    with pytest.raises(ValueError, match="not bound"):
        database.query("SELECT 1 FROM companies", datasets={"companies"},
                       prefilters={"people": "x = 1"})


def test_explicit_prefilter_inapplicable_raises(wired):
    with pytest.raises(ValueError, match="dataset_filters"):
        database.query("SELECT 1 FROM companies", datasets={"companies"},
                       prefilters={"companies": "no_such_column = 'x'"})


def test_explicit_prefilter_over_row_guard_raises(wired, monkeypatch):
    monkeypatch.setattr(database, "_PREFILTER_MAX_ROWS", 1)
    with pytest.raises(ValueError, match="dataset_filters"):
        database.query("SELECT 1 FROM companies", datasets={"companies"},
                       prefilters={"companies": "state IN ('TX','VA','CA')"})


# ── gates preserved ──────────────────────────────────────────────────────────
def test_read_only_gate_still_enforced(wired):
    with pytest.raises(ValueError, match="read-only"):
        database.query("DELETE FROM companies WHERE state = 'TX'",
                       datasets={"companies"},
                       prefilters={"companies": "state = 'TX'"})


def test_explain_analyze_runs_through_prefiltered_lane(wired, monkeypatch):
    calls = []
    real = database.scan_table

    def spy(name, **kw):
        calls.append(name)
        return real(name, **kw)

    monkeypatch.setattr(database, "scan_table", spy)
    res = database.query("EXPLAIN ANALYZE " + MULTIHOP, datasets=set(wired))
    assert sorted(calls) == ["companies", "lookalikes", "usaspending/award_search"]
    assert res["row_count"] >= 1  # plan text came back
