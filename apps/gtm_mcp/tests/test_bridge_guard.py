"""Tests for the substrait bridge guard (database._bridge_safe and friends).

WHY. pylance's DuckDB bridge panics on ANY pushed Expression filter when the
dataset schema carries a struct-under-list column (lance-format/lance#6130 —
DataFusion deep vs PyArrow shallow substrait naming; present in every release
>=3, verified through 8.0.0; fix PR #6469 unmerged). entity_profile_gold
(``pocs`` list<struct>) is the live casualty: the raw-SQL lane's full
registration aborted on every DuckDB-pushed predicate and dynamic join filter.
These tests pin:

  * DETECTION — ``_has_struct_under_list`` flags exactly the affected shapes
    (list<struct>, map, struct<list<struct>>) and passes the benign ones
    (flat, list<primitive>, bare struct) — benign schemas keep the native
    bridge and its Lance-side filtering.
  * GUARDED EXECUTION — with the guard engaged, every previously-aborting
    shape (pushed WHERE, dynamic join filter, IN-list, self-join rescan)
    returns rows byte-equivalent to the Lance-scanner ground truth. The
    projection still reaches Lance; nested columns remain selectable.
  * IDEMPOTENCE + NON-INTERFERENCE — the class swap is stable across repeated
    registration, and SQL-string filters on the swapped handle keep taking the
    native (DataFusion-parser) path.
  * CANARY — the UNguarded registration still aborts on this pylance. When
    this test starts failing, upstream fixed lance#6130: retire the guard
    (database section comment) and relax the requirements.txt note.

Local Lance fixtures stand in for the R2 sink (same style as
test_prefilter.py); no credentials or network are needed.
"""

from __future__ import annotations

import duckdb
import lance
import pyarrow as pa
import pytest

from apps.gtm_mcp.src import database

N = 200
POC = pa.struct([("poc_name", pa.string()), ("poc_email", pa.string()),
                 ("poc_role", pa.string())])


@pytest.fixture(scope="module")
def nested_ds(tmp_path_factory):
    """entity_profile_gold's shape in miniature: flat cols, a list<struct>
    column mid-schema, and the filtered flat column AFTER it (the ordinal
    overrun that panics the native bridge)."""
    schema = pa.schema([
        pa.field("uei", pa.string()),
        pa.field("company_name", pa.string()),
        pa.field("pocs", pa.list_(POC)),
        pa.field("primary_naics", pa.string()),
        pa.field("physical_address_state", pa.string()),
    ])
    tbl = pa.table({
        "uei": [f"UEI{i:05d}" for i in range(N)],
        "company_name": [f"Co {i}" for i in range(N)],
        "pocs": [[{"poc_name": f"p{i}", "poc_email": f"p{i}@x.com", "poc_role": "ceo"}]
                 for i in range(N)],
        "primary_naics": [f"33641{i % 10}" for i in range(N)],
        "physical_address_state": ["VA" if i % 2 else "MD" for i in range(N)],
    }, schema=schema)
    path = str(tmp_path_factory.mktemp("lance") / "entity_mini.lance")
    lance.write_dataset(tbl, path, mode="overwrite")
    return lance.dataset(path)


@pytest.fixture()
def flat_ds(tmp_path):
    tbl = pa.table({"uei": [f"U{i}" for i in range(50)],
                    "state": ["VA" if i % 2 else "MD" for i in range(50)]})
    path = str(tmp_path / "flat.lance")
    lance.write_dataset(tbl, path, mode="overwrite")
    return lance.dataset(path)


# ── detection ────────────────────────────────────────────────────────────────
@pytest.mark.parametrize("dtype,unsafe", [
    (pa.string(), False),
    (pa.list_(pa.string()), False),                       # list<primitive>: benign (verified)
    (pa.struct([("a", pa.string())]), False),             # bare struct: benign (verified)
    (pa.list_(POC), True),                                # the entity_profile_gold trigger
    (pa.large_list(POC), True),
    (pa.map_(pa.string(), pa.string()), True),            # map IS list<struct<k,v>>
    (pa.struct([("inner", pa.list_(POC))]), True),        # struct wrapping a list<struct>
    (pa.list_(pa.list_(POC)), True),
])
def test_struct_under_list_detection(dtype, unsafe):
    assert database._has_struct_under_list(dtype) is unsafe


def test_bridge_safe_leaves_flat_schema_untouched(flat_ds):
    cls_before = type(flat_ds)
    out = database._bridge_safe(flat_ds)
    assert out is flat_ds and type(out) is cls_before


def test_bridge_safe_swaps_nested_schema_and_is_idempotent(nested_ds):
    out = database._bridge_safe(nested_ds)
    assert out is nested_ds  # same warm handle, no reopen
    guard_cls = database._bridge_guard_class()
    assert isinstance(out, guard_cls)
    assert database._bridge_safe(out) is out
    assert type(database._bridge_safe(out)) is guard_cls  # one class object, stable


# ── guarded execution: every previously-aborting pushdown shape ─────────────
@pytest.fixture()
def con(nested_ds):
    c = duckdb.connect(":memory:")
    c.register("entity", database._bridge_safe(nested_ds))
    yield c
    c.close()


def test_pushed_where_matches_scanner_ground_truth(con, nested_ds):
    truth = nested_ds.scanner(filter="physical_address_state = 'VA'").to_table().num_rows
    got = con.execute(
        "SELECT count(*) FROM entity WHERE physical_address_state = 'VA'").fetchone()[0]
    assert got == truth == N // 2


def test_pushed_where_on_column_before_nested(con):
    got = con.execute("SELECT count(*) FROM entity WHERE uei = 'UEI00007'").fetchone()[0]
    assert got == 1


def test_dynamic_join_filter(con):
    got = con.execute("""
        WITH small AS (SELECT 'VA' AS k)
        SELECT count(*) FROM entity JOIN small ON entity.physical_address_state = small.k
    """).fetchone()[0]
    assert got == N // 2


def test_in_list_pushdown_and_projection(con):
    rows = con.execute("""
        SELECT uei, physical_address_state FROM entity
        WHERE uei IN ('UEI00003', 'UEI00004') ORDER BY uei
    """).fetchall()
    assert rows == [("UEI00003", "VA"), ("UEI00004", "MD")]


def test_nested_column_remains_selectable_under_filter(con):
    row = con.execute("""
        SELECT pocs FROM entity WHERE physical_address_state = 'VA' LIMIT 1
    """).fetchone()
    assert row[0][0]["poc_role"] == "ceo"


def test_self_join_rescans_safely(con):
    # two scans of the same registered handle + a dynamic range filter on the build side
    got = con.execute("""
        SELECT count(*) FROM entity x JOIN entity y USING (uei)
        WHERE x.physical_address_state = 'VA'
    """).fetchone()[0]
    assert got == N // 2


def test_unfiltered_scan_unaffected(con):
    assert con.execute("SELECT count(*) FROM entity").fetchone()[0] == N


# ── non-interference with the gateway's own scanner paths ───────────────────
def test_string_filters_stay_native_after_swap(nested_ds):
    ds = database._bridge_safe(nested_ds)
    tbl = ds.scanner(filter="physical_address_state = 'MD'",
                     columns=["uei"]).to_table()
    assert tbl.num_rows == N // 2 and tbl.column_names == ["uei"]


# ── canary: retire the guard when this starts failing ───────────────────────
def test_native_bridge_still_broken_canary(nested_ds):
    """Pins lance#6130's presence at the pinned pylance. A FAILURE here means the
    upstream fix landed: retire _bridge_safe/_bridge_guard_class (database.py
    section comment) and relax the requirements.txt pylance note."""
    raw = lance.dataset(nested_ds.uri)  # fresh, unswapped handle
    c = duckdb.connect(":memory:")
    c.register("entity", raw)
    with pytest.raises(duckdb.Error, match="aborted"):
        c.execute("SELECT count(*) FROM entity WHERE physical_address_state = 'VA'").fetchall()
    c.close()
