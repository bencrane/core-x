"""Unit tests for the sam_pocs pre-write gate suite.

Pure-function coverage of `assert_pre_write_gates` — no R2/Modal/Postgres. Proves the
historically-observed failures (a zero build and a 30% v2-half partial that both recorded
`success` on 2026-06-02) would now be caught BEFORE any overwrite, and that a positional
name-offset regression (the silent-corruption class the worker docstring fears) is caught
by the content gates.

    python -m pytest pipelines/sam_gov/tests/test_sam_pocs_gates.py -q
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

# repo root = .../pipelines/sam_gov/tests/this_file → parents[3]
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipelines.sam_gov.sam_pocs import assert_pre_write_gates  # noqa: E402

_ALL_SLOTS = {
    "government_business", "government_business_alt", "past_performance",
    "past_performance_alt", "electronic_business", "electronic_business_alt",
}

# A clean, in-range materialization (live healthy values, 2026-06-05).
HEALTHY = {
    "rows": 8_065_116, "distinct_uei": 1_540_966, "distinct_cage": 1_167_572,
    "poc_rows_v2": 4_372_870, "poc_rows_legacy": 3_692_246,
    "name_present_frac": 1.0, "unkeyed_rows": 0, "null_poc_type": 0,
    "name_alpha_frac": 0.998, "zip_numeric_frac": 0.997,
    "present_poc_types": set(_ALL_SLOTS), "probe_uei": "DD1BCRF2QQG8",
}
BASELINE = {"rows_written": 8_065_116, "distinct_uei": 1_540_966,
            "poc_rows_v2": 4_372_870, "poc_rows_legacy": 3_692_246}


def _m(**overrides):
    d = dict(HEALTHY)
    d["present_poc_types"] = set(HEALTHY["present_poc_types"])
    d.update(overrides)
    return d


def test_healthy_passes():
    checks = assert_pre_write_gates(HEALTHY, BASELINE)
    assert all(c.startswith("PASS") for c in checks), checks
    assert len(checks) == 13  # gates 1-13 all evaluated, none skipped (baseline present)


def test_no_baseline_skips_delta():
    checks = assert_pre_write_gates(HEALTHY, None)
    assert any("SKIP" in c and "Δ-guards" in c for c in checks), checks
    assert not any(c.startswith("FAIL") for c in checks)


def test_zero_build_raises_row_floor():
    # 2026-06-02 02:08 — rows_written=0 recorded success.
    metrics = _m(rows=0, distinct_uei=0, distinct_cage=0, poc_rows_v2=0,
                 poc_rows_legacy=0, name_present_frac=0.0,
                 name_alpha_frac=0.0, zip_numeric_frac=0.0)
    with pytest.raises(RuntimeError, match="1 row floor"):
        assert_pre_write_gates(metrics, BASELINE)


def test_historical_partial_raises():
    # 2026-06-02 02:19 — 6.39M / 888K uei / 2.70M v2 recorded success. Caught at the uei floor.
    metrics = _m(rows=6_389_167, distinct_uei=888_361, distinct_cage=1_167_571,
                 poc_rows_v2=2_696_876, poc_rows_legacy=3_692_291)
    with pytest.raises(RuntimeError, match="2 uei floor"):
        assert_pre_write_gates(metrics, BASELINE)


def test_subtle_v2_half_collapse_caught_by_per_family_delta():
    # A v2-half collapse that CLEARS every floor and the total-rows Δ-guard, but the
    # per-family v2 Δ-guard catches it. This is the case a scalar guard waved through.
    metrics = _m(rows=6_590_000, distinct_uei=1_310_000, distinct_cage=1_167_572,
                 poc_rows_v2=2_900_000, poc_rows_legacy=3_690_000)
    with pytest.raises(RuntimeError, match="6 v2 Δ"):
        assert_pre_write_gates(metrics, BASELINE)


def test_offset_shift_raises_content_gate():
    # Positional-offset regression: digits land in first_name, letters in zip5. Counts
    # all healthy; only the content gates move.
    metrics = _m(name_alpha_frac=0.08, zip_numeric_frac=0.05)
    with pytest.raises(RuntimeError, match="12 name-alpha"):
        assert_pre_write_gates(metrics, BASELINE)


def test_zip_numeric_gate_independently():
    metrics = _m(zip_numeric_frac=0.10)  # names fine, zips corrupted
    with pytest.raises(RuntimeError, match="13 zip-numeric"):
        assert_pre_write_gates(metrics, BASELINE)


def test_cage_floor_raises():
    with pytest.raises(RuntimeError, match="3 cage floor"):
        assert_pre_write_gates(_m(distinct_cage=10_000), BASELINE)


def test_family_zero_raises():
    with pytest.raises(RuntimeError, match="4 both families"):
        assert_pre_write_gates(_m(poc_rows_legacy=0), BASELINE)


def test_unkeyed_rows_raises():
    with pytest.raises(RuntimeError, match="8 every row keyed"):
        assert_pre_write_gates(_m(unkeyed_rows=1), BASELINE)


def test_null_poc_type_raises():
    with pytest.raises(RuntimeError, match="9 no null poc_type"):
        assert_pre_write_gates(_m(null_poc_type=1), BASELINE)


def test_missing_mandatory_slot_raises():
    slots = set(_ALL_SLOTS) - {"electronic_business"}
    with pytest.raises(RuntimeError, match="10 mandatory slots"):
        assert_pre_write_gates(_m(present_poc_types=slots), BASELINE)


def test_optional_slot_absent_still_passes():
    # An empty OPTIONAL alternate must NOT roll back a good build (mandatory pair present).
    slots = set(_ALL_SLOTS) - {"past_performance_alt"}
    checks = assert_pre_write_gates(_m(present_poc_types=slots), BASELINE)
    assert all(c.startswith("PASS") for c in checks)


def test_rows_delta_guard_raises_high():
    # +26% recovery vs a (hypothetical) degraded baseline must trip the rows Δ-guard —
    # the reason the baseline query is floor-qualified to avoid ratcheting on a real run.
    degraded_baseline = {"rows_written": 6_389_167, "distinct_uei": 1_540_966,
                         "poc_rows_v2": 4_372_870, "poc_rows_legacy": 3_692_246}
    with pytest.raises(RuntimeError, match="5 rows Δ"):
        assert_pre_write_gates(HEALTHY, degraded_baseline)
