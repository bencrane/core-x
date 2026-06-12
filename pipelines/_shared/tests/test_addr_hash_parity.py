"""Parity test for the canonical geocode-crosswalk join key (pipelines/_shared/addr_hash.py).

The md5 ``addr_hash`` is the LEFT JOIN key between ``geocode_xwalk`` (address → coords) and
the two map builders that read coords back from it. If any builder's normalization drifts by
even one byte, the join silently yields all-NULL coords — a blank map, no error, no exception.
These tests pin the single shared definition:

  1. every import site emits a byte-identical SQL string (pure; no DuckDB needed), so
     re-introducing a divergent local copy trips immediately, and
  2. one fixture address hashes to one identical md5 through every import site, computed in a
     local in-memory DuckDB so it is byte-identical to the engine that writes/reads the crosswalk.

    python -m pytest pipelines/_shared/tests/test_addr_hash_parity.py -q
"""
from __future__ import annotations

import hashlib
import re
import sys
from pathlib import Path

import duckdb

# repo root = .../pipelines/_shared/tests/this_file → parents[3]
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipelines._shared import addr_hash as shared                  # noqa: E402
from pipelines.serving import materialize_company_map as company   # noqa: E402
from pipelines.serving import materialize_winners_map as winners   # noqa: E402
from pipelines.usaspending import geocode_xwalk as geocode         # noqa: E402

# Every site that resolves addr_hash_sql — all MUST point at the one shared definition.
IMPORT_SITES = {
    "pipelines/_shared/addr_hash.py": shared,
    "pipelines/usaspending/geocode_xwalk.py": geocode,
    "pipelines/serving/materialize_winners_map.py": winners,
    "pipelines/serving/materialize_company_map.py": company,
}

# Fixture exercises every normalization branch: a double space (\s+ collapse), surrounding
# whitespace (trim), a lowercase state (upper), and a zip+4 with a dash (digits-only, first 5).
FIXTURE = ("123  Main St.", " Springfield ", "il", "62704-1234")
# Canonical normalized key + its md5, pinned independently of the SQL builders. Mirrors
# addr_hash_sql byte-for-byte: street trim→collapse \s+→upper, city/state trim→upper,
# zip strip-non-digits→first 5, joined by '|'. Catches any silent change to the expression.
EXPECTED_KEY = "123 MAIN ST.|SPRINGFIELD|IL|62704"
EXPECTED_HASH = "952f5c0740c4b100123137ae5cde4893"


def _python_reference_hash(street: str, city: str, state: str, zip_: str) -> str:
    s = re.sub(r"\s+", " ", street.strip()).upper()
    c = city.strip().upper()
    st = state.strip().upper()
    z = re.sub(r"[^0-9]", "", zip_)[:5]
    return hashlib.md5(f"{s}|{c}|{st}|{z}".encode()).hexdigest()


def _sql_literal(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def _hash_via(mod, con) -> str:
    """md5 of FIXTURE computed through ``mod.addr_hash_sql``, evaluated in DuckDB."""
    expr = mod.addr_hash_sql(*(_sql_literal(v) for v in FIXTURE))
    return con.execute(f"SELECT {expr}").fetchone()[0]


def test_addr_hash_sql_byte_identical_across_import_sites():
    """Every import site must emit the exact same SQL string for the same column refs.
    Re-introducing a local copy that differs — the failure this dedup prevents — trips here
    without needing DuckDB."""
    cols = ("street", "city", "state", "zip")
    rendered = {name: mod.addr_hash_sql(*cols) for name, mod in IMPORT_SITES.items()}
    assert len(set(rendered.values())) == 1, f"addr_hash_sql diverged across import sites: {rendered}"


def test_zip5_sql_is_the_hash_zip_segment():
    """_zip5_sql (the crosswalk's stored zip5) must be byte-identical to the zip5 segment
    inside addr_hash_sql, else the stored zip5 and the join hash disagree."""
    assert shared._zip5_sql("zip") in shared.addr_hash_sql("street", "city", "state", "zip")


def test_fixture_address_hashes_identically_through_every_import_site():
    """The whole point: one fixture address → one md5, identical through every builder, equal
    to the independently-pinned canonical hash and to a pure-Python reference normalization."""
    assert _python_reference_hash(*FIXTURE) == EXPECTED_HASH  # reference agrees with the pin
    con = duckdb.connect(":memory:")
    try:
        hashes = {name: _hash_via(mod, con) for name, mod in IMPORT_SITES.items()}
    finally:
        con.close()
    assert set(hashes.values()) == {EXPECTED_HASH}, f"addr_hash diverged across import sites: {hashes}"
