"""Unit tests for the full-body marking pre-pass cores (`detect_markings_fullbody`, `promote`,
`classify_decision`) — pure functions, no R2/Lance.

Proves the two safety-critical contracts of Phase 0 item 3:
  * PROMOTION-ONLY union semantics — never remove an existing caveat, never downgrade
    ['unspecified'] to []; replace 'unspecified' with specifics ONLY when the full-body scan
    detected >=1 caveat.
  * UPPERCASE-ONLY matching for the bare acronyms EAR and CUI in the full-body pass (spelled-out
    phrases stay case-insensitive) — the mandated false-positive limiter.

    python -m pytest pipelines/sam_gov/tests/test_sam_marking_fullbody.py -q
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipelines.sam_gov.sam_marking_fullbody_90day import (  # noqa: E402
    classify_decision, detect_markings_fullbody, promote,
)


# ── detection: uppercase-only acronyms ─────────────────────────────────────────────────────────────
def test_bare_cui_uppercase_only():
    assert detect_markings_fullbody("This document is CUI per agency policy.") == ["cui"]
    assert detect_markings_fullbody("the cui marking handbook") == []          # lowercase: no match
    assert detect_markings_fullbody("Cui v. Department of Labor") == []        # mixed case: no match


def test_spelled_out_cui_stays_case_insensitive():
    assert detect_markings_fullbody("controlled unclassified information applies") == ["cui"]


def test_bare_ear_uppercase_only():
    assert detect_markings_fullbody("subject to the EAR (15 CFR 730)") == ["ear"]
    assert detect_markings_fullbody("inner ear protection shall be provided") == []
    assert detect_markings_fullbody("a 5-year period") == []
    assert detect_markings_fullbody("YEAR end close-out") == []                # \b guard inside words


def test_other_caveats_stay_case_insensitive():
    assert detect_markings_fullbody("for official use only") == ["fouo"]
    assert detect_markings_fullbody("this is itar controlled") == ["itar"]
    assert detect_markings_fullbody("Export Controlled data") == ["export_controlled"]


def test_distribution_statement_letter_capture():
    assert detect_markings_fullbody("DISTRIBUTION STATEMENT C. Distribution authorized") == ["dist_stmt_c"]
    assert detect_markings_fullbody("distribution statement f") == ["dist_stmt_f"]
    assert detect_markings_fullbody("DISTRIBUTION STATEMENT A") == []          # A = approved/public


def test_detection_orders_and_dedups():
    body = "ITAR and CUI. CONTROLLED UNCLASSIFIED INFORMATION. ITAR again. DISTRIBUTION STATEMENT B"
    assert detect_markings_fullbody(body) == ["cui", "itar", "dist_stmt_b"]


# ── promotion-only union semantics ─────────────────────────────────────────────────────────────────
def test_promote_empty_prior_gains_detected():
    assert promote([], ["itar", "cui"]) == ["cui", "itar"]


def test_promote_detected_empty_changes_nothing():
    assert promote([], []) == []
    assert promote(["unspecified"], []) == ["unspecified"]                     # never downgrade
    assert promote(["itar"], []) == ["itar"]


def test_promote_replaces_unspecified_only_when_detected():
    assert promote(["unspecified"], ["cui", "dist_stmt_d"]) == ["cui", "dist_stmt_d"]


def test_promote_never_removes_existing_specific_caveat():
    assert promote(["itar"], ["cui"]) == ["cui", "itar"]
    assert promote(["itar", "unspecified"], ["cui"]) == ["cui", "itar"]


def test_promote_idempotent_on_reapply():
    once = promote(["unspecified"], ["cui"])
    assert promote(once, ["cui"]) == once


# ── decision classification (ledger audit label) ───────────────────────────────────────────────────
def test_classify_decision_labels():
    assert classify_decision([], []) == "unchanged"
    assert classify_decision([], ["cui"]) == "promoted"
    assert classify_decision(["unspecified"], ["cui"]) == "upgraded"
    assert classify_decision(["itar"], ["cui", "itar"]) == "extended"
    assert classify_decision(["unspecified"], ["unspecified"]) == "unchanged"
