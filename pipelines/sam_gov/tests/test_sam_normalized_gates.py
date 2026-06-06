"""Unit tests for the sam_normalized_entities pre-write gate suite (pure; no R2/Modal/PG).

Proves the per-family Δ-guard covers distinct_legal_name_base (the coverage the retired
absolute target used to give), catches a distinct-key collapse the coarse floor misses,
the content gate catches a normalization regression, and the Δ-guards skip when there is
no floor-qualified baseline.

    python -m pytest pipelines/sam_gov/tests/test_sam_normalized_gates.py -q
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipelines.sam_gov.sam_normalized_entities import assert_pre_write_gates  # noqa: E402

ROWS = 1_541_566  # live; == distinct_uei (1:1 passthrough), == src_count
HEALTHY = {
    "rows": ROWS, "distinct_uei": ROWS,
    "distinct_normalized_name": 1_466_764, "distinct_legal_name_base": 1_450_598,
    "normalized_nonnull": ROWS, "geo_cofill": int(ROWS * 0.97),
    "name_alpha_frac": 0.9999, "probe_uei": "DD1BCRF2QQG8",
}
BASELINE = {"rows_written": ROWS, "distinct_normalized_name": 1_466_764,
            "distinct_legal_name_base": 1_450_598}


def _m(**ov):
    d = dict(HEALTHY); d.update(ov); return d


def test_healthy_passes_with_baseline():
    checks = assert_pre_write_gates(HEALTHY, ROWS, BASELINE)
    assert all(c.startswith("PASS") for c in checks), checks
    assert len(checks) == 11  # gates 1-11, none skipped (baseline present)


def test_no_baseline_skips_delta():
    checks = assert_pre_write_gates(HEALTHY, ROWS, None)
    assert any("SKIP" in c and "Δ-guards" in c for c in checks), checks
    assert not any(c.startswith("FAIL") for c in checks)


def test_row_floor_raises():
    with pytest.raises(RuntimeError, match="1 row floor"):
        assert_pre_write_gates(_m(rows=900_000, distinct_uei=900_000, normalized_nonnull=900_000), 900_000, BASELINE)


def test_passthrough_mismatch_raises():
    with pytest.raises(RuntimeError, match="2 1:1 passthrough"):
        assert_pre_write_gates(HEALTHY, ROWS + 100, BASELINE)


def test_uei_uniqueness_raises():
    with pytest.raises(RuntimeError, match="3 uei uniqueness"):
        assert_pre_write_gates(_m(distinct_uei=ROWS - 10), ROWS, BASELINE)


def test_norm_fill_raises():
    with pytest.raises(RuntimeError, match="4 normalized_legal_name fill"):
        assert_pre_write_gates(_m(normalized_nonnull=int(ROWS * 0.5)), ROWS, BASELINE)


def test_norm_distinct_floor_raises():
    with pytest.raises(RuntimeError, match="5 norm-distinct floor"):
        assert_pre_write_gates(_m(distinct_normalized_name=500_000), ROWS, BASELINE)


def test_base_distinct_floor_raises():
    with pytest.raises(RuntimeError, match="6 base-distinct floor"):
        assert_pre_write_gates(_m(distinct_legal_name_base=500_000), ROWS, BASELINE)


def test_geo_cofill_raises():
    with pytest.raises(RuntimeError, match="7 geo co-fill"):
        assert_pre_write_gates(_m(geo_cofill=int(ROWS * 0.5)), ROWS, BASELINE)


def test_name_alpha_content_raises():
    # normalization regression: keys are no longer alpha-dominant.
    with pytest.raises(RuntimeError, match="8 name-alpha"):
        assert_pre_write_gates(_m(name_alpha_frac=0.10), ROWS, BASELINE)


def test_base_distinct_collapse_caught_by_per_family_delta():
    # 1.06M CLEARS the 1.05M floor but is -27% vs the 1.45M baseline → the per-family Δ on
    # distinct_legal_name_base (gate 11) catches the suffix-peel regression the floor misses.
    # This is the coverage the retired absolute BASE_DISTINCT_TARGET used to provide.
    with pytest.raises(RuntimeError, match="11 base-distinct Δ"):
        assert_pre_write_gates(_m(distinct_legal_name_base=1_060_000), ROWS, BASELINE)


def test_norm_distinct_collapse_caught_by_per_family_delta():
    with pytest.raises(RuntimeError, match="10 norm-distinct Δ"):
        assert_pre_write_gates(_m(distinct_normalized_name=1_060_000), ROWS, BASELINE)
