"""Unit tests for the Phase-H2 CUI egress gate + structural scoping guard.

Pure-function coverage — no R2/Lance. Proves the two pre-execution-hardening invariants:
  * _marking_gate_ok: the LLM lane refuses to stage unless Phase-F marking reconciled PASS
    (None/FAIL/missing => raise), with optional freshness so a stale prior-run PASS can't satisfy it.
  * _assert_h2_subset: when an allow-list is supplied, no out-of-scope resource enters the
    account-burning LLM lane; parent-aware for expanded-zip inner ids `<rid>::<inner>`.

    python -m pytest pipelines/sam_gov/tests/test_sam_labor_h2_gates.py -q
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

import pytest  # noqa: E402

from pipelines.sam_gov.sam_labor_demand_extract import (  # noqa: E402
    _marking_gate_ok,
    _scope_pending,
)


# ── CUI egress gate ──────────────────────────────────────────────────────────
def test_gate_none_report_raises():
    with pytest.raises(RuntimeError):
        _marking_gate_ok(None)


def test_gate_missing_overall_raises():
    with pytest.raises(RuntimeError):
        _marking_gate_ok({"completed_at": "2026-06-15T20:00:00"})


def test_gate_fail_raises():
    with pytest.raises(RuntimeError):
        _marking_gate_ok({"reconcile_overall": "FAIL"})


def test_gate_pass_no_freshness_ok():
    _marking_gate_ok({"reconcile_overall": "PASS"})


def test_gate_pass_fresh_enough_ok():
    _marking_gate_ok({"reconcile_overall": "PASS", "completed_at": "2026-06-15T21:00:00"},
                     require_after="2026-06-15T20:00:00")


def test_gate_pass_but_stale_raises():
    with pytest.raises(RuntimeError):
        _marking_gate_ok({"reconcile_overall": "PASS", "completed_at": "2026-06-15T19:00:00"},
                         require_after="2026-06-15T20:00:00")


def test_gate_pass_freshness_required_but_no_completed_at_raises():
    with pytest.raises(RuntimeError):
        _marking_gate_ok({"reconcile_overall": "PASS"}, require_after="2026-06-15T20:00:00")


# ── structural H2 scoping (filter, parent-aware) ─────────────────────────────
def test_scope_none_allow_keeps_all():
    assert _scope_pending({"anything", "at::all"}, None) == {"anything", "at::all"}


def test_scope_empty_allow_keeps_all():
    assert _scope_pending({"x"}, set()) == {"x"}


def test_scope_all_in_allow_kept():
    assert _scope_pending({"a", "b"}, {"a", "b", "c"}) == {"a", "b"}


def test_scope_inner_id_parent_in_allow_kept():
    # expanded-zip inner id qualifies via its parent rid
    assert _scope_pending({"a", "a::inner/doc.pdf"}, {"a"}) == {"a", "a::inner/doc.pdf"}


def test_scope_drops_out_of_allow():
    # the real case: unrelated prod pending ids are dropped (left for prod), in-scope kept
    assert _scope_pending({"a", "z", "prod1"}, {"a"}) == {"a"}


def test_scope_inner_id_parent_not_in_allow_dropped():
    assert _scope_pending({"q::inner", "a"}, {"a"}) == {"a"}
