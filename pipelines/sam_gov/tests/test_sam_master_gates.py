"""Unit tests for the sam_master pre-write gate suite (pure; no R2/Modal/PG).

Proves the per-family Δ-guard catches a satellite projection-regression collapse the
coarse floor misses (the L5/D7 class), the content gates catch a positional-offset
regression, NAICS-numeric is observational when fill is low, and the Δ-guards correctly
skip when there is no floor-qualified baseline (the first-hardened-run path).

    python -m pytest pipelines/sam_gov/tests/test_sam_master_gates.py -q
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipelines.sam_gov.sam_master import assert_pre_write_gates  # noqa: E402

# Live values (ops.sam_master_runs latest success).
HEALTHY = {
    "entities_rows": 1_541_566, "contacts_rows": 4_373_319, "domains_rows": 709_546,
    "distinct_uei": 1_541_566,
    "naics_numeric_frac": 0.991, "primary_naics_fill": 0.82, "name_alpha_frac": 0.999,
    "probe_uei": "DD1BCRF2QQG8",
}
BASELINE = {"entities_rows": 1_541_566, "contacts_rows": 4_373_319,
            "domains_rows": 709_546, "distinct_uei": 1_541_566}


def _m(**ov):
    d = dict(HEALTHY); d.update(ov); return d


def test_healthy_passes_with_baseline():
    checks = assert_pre_write_gates(HEALTHY, BASELINE)
    assert all(c.startswith("PASS") for c in checks), checks
    assert len(checks) == 9  # gates 1-9, none skipped (baseline present, naics fill high)


def test_no_baseline_skips_delta():
    checks = assert_pre_write_gates(HEALTHY, None)
    assert any("SKIP" in c and "Δ-guards" in c for c in checks), checks
    assert not any(c.startswith("FAIL") for c in checks)


def test_entities_floor_raises():
    with pytest.raises(RuntimeError, match="1 entities floor"):
        assert_pre_write_gates(_m(entities_rows=900_000, distinct_uei=900_000), BASELINE)


def test_uei_uniqueness_raises():
    with pytest.raises(RuntimeError, match="2 uei uniqueness"):
        assert_pre_write_gates(_m(distinct_uei=1_500_000), BASELINE)


def test_contacts_floor_raises():
    with pytest.raises(RuntimeError, match="3 contacts floor"):
        assert_pre_write_gates(_m(contacts_rows=2_000_000), BASELINE)


def test_domains_floor_raises():
    with pytest.raises(RuntimeError, match="4 domains floor"):
        assert_pre_write_gates(_m(domains_rows=100_000), BASELINE)


def test_contacts_half_collapse_caught_by_per_family_delta():
    # 3.1M CLEARS the 3.0M catastrophic floor but is -29% vs the 4.37M baseline → the
    # per-family Δ (gate 6) catches the POC-unpivot regression the floor misses.
    with pytest.raises(RuntimeError, match="6 contacts Δ"):
        assert_pre_write_gates(_m(contacts_rows=3_100_000), BASELINE)


def test_contacts_surge_caught_by_per_family_delta():
    # +37% (duplicating projection) also fails the per-family Δ.
    with pytest.raises(RuntimeError, match="6 contacts Δ"):
        assert_pre_write_gates(_m(contacts_rows=6_000_000), BASELINE)


def test_offset_shift_raises_name_content_gate():
    # Positional-offset regression: legal_business_name is no longer alpha-dominant.
    with pytest.raises(RuntimeError, match="8 name-alpha"):
        assert_pre_write_gates(_m(name_alpha_frac=0.10), BASELINE)


def test_naics_numeric_gate_when_fill_high():
    with pytest.raises(RuntimeError, match="9 naics-numeric"):
        assert_pre_write_gates(_m(naics_numeric_frac=0.10, primary_naics_fill=0.82), BASELINE)


def test_naics_observational_when_fill_low():
    # Low primary_naics fill → NAICS-numeric is NOT gated (observational); no raise.
    checks = assert_pre_write_gates(_m(naics_numeric_frac=0.10, primary_naics_fill=0.30), BASELINE)
    assert any("SKIP" in c and "naics-numeric" in c for c in checks), checks
    assert not any(c.startswith("FAIL") for c in checks)


def test_entities_delta_recovery_not_ratcheted():
    # A +26% jump vs a (hypothetically) degraded baseline trips the entities Δ — the reason
    # the baseline query is floor-qualified so a degraded success can't become the baseline.
    degraded = {"entities_rows": 1_180_000, "contacts_rows": 4_373_319,
                "domains_rows": 709_546, "distinct_uei": 1_180_000}
    with pytest.raises(RuntimeError, match="5 entities Δ"):
        assert_pre_write_gates(HEALTHY, degraded)
