"""Unit tests for the default-OFF id allow-list helpers (`_id_filter_sql`, `_assert_routed_subset`).

Pure-function coverage — no R2/Lance. Proves the blast-radius contract of the gate-bypass id-filter:
the filter is OFF by construction (None → ''), an empty allow-list HARD-RAISES instead of falling
through to the full corpus, the IN-clause is deterministic + injection-escaped, and the post-route
subset assertion refuses any out-of-set leak. These two helpers are the entire surface that makes
"accidentally route the prime backlog into shared state" structurally impossible.

    python -m pytest pipelines/sam_gov/tests/test_sam_attachment_id_filter.py -q
"""
from __future__ import annotations

import sys
from pathlib import Path

# repo root = .../pipelines/sam_gov/tests/this_file → parents[3]
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

import pytest  # noqa: E402

from pipelines.sam_gov.sam_attachment_extract_90day import (  # noqa: E402
    _assert_routed_subset,
    _id_filter_sql,
)


def test_none_is_default_off():
    # The contract that keeps every existing call path byte-identical.
    assert _id_filter_sql(None, col="f.resource_id") == ""


def test_empty_set_raises():
    # GUARD #1: an empty filter must NOT silently fall through to the full corpus.
    with pytest.raises(RuntimeError):
        _id_filter_sql(set(), col="f.resource_id")


def test_builds_quoted_in_clause_sorted():
    assert _id_filter_sql({"b", "a"}, col="x") == "AND x IN ('a','b')"


def test_sql_injection_escaped():
    # A single quote in an id is doubled, never breaks out of the literal.
    assert _id_filter_sql({"a'b"}, col="x") == "AND x IN ('a''b')"


def test_col_is_interpolated():
    assert _id_filter_sql({"z"}, col="d.resource_id") == "AND d.resource_id IN ('z')"


def test_subset_assertion_passes_on_subset():
    # routed ⊆ allow-list → no raise.
    _assert_routed_subset(["a", "b"], {"a", "b", "c"})


def test_subset_assertion_passes_on_exact_match():
    _assert_routed_subset(["a", "b", "c"], {"a", "b", "c"})


def test_subset_assertion_raises_on_leak():
    # GUARD #2: any routed id outside the allow-list aborts before extract.
    with pytest.raises(RuntimeError):
        _assert_routed_subset(["a", "z"], {"a", "b"})


def test_subset_assertion_noop_when_off():
    # Filter OFF (None) → assertion is a no-op even on arbitrary routed ids.
    _assert_routed_subset(["anything", "at", "all"], None)


def test_subset_assertion_consumes_generator():
    # phase1_route passes a generator (r[0] for r in routed); the assertion must materialize it.
    with pytest.raises(RuntimeError):
        _assert_routed_subset((x for x in ["a", "leak"]), {"a"})
