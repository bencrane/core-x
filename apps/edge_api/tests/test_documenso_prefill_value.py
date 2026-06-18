"""Unit guard for the prefill label→value resolver — no network.

Pins the base-name fallback: a label split for multiple placements (``participant_company_one`` /
``participant_company_two``) draws from the single ``participant_company`` value. The full
recipient-binding + token path is verified live in the PR (throwaway envelope against the real template).
"""
from __future__ import annotations

from apps.edge_api.src.services.documenso_client import _prefill_value_for_label

VALUES = {
    "participant_company": "Environmental Logistics",
    "fee_amount": "$35,000",
    "days": "90",
    "blank": "",
}


def test_exact_match_wins():
    assert _prefill_value_for_label("fee_amount", VALUES) == "$35,000"


def test_base_fallback_fans_out_split_labels():
    # Both placement-suffixed labels resolve to the single base value.
    assert _prefill_value_for_label("participant_company_one", VALUES) == "Environmental Logistics"
    assert _prefill_value_for_label("participant_company_two", VALUES) == "Environmental Logistics"


def test_exact_preferred_over_base():
    vals = {"participant_company": "BASE", "participant_company_one": "EXACT"}
    assert _prefill_value_for_label("participant_company_one", vals) == "EXACT"


def test_missing_label_is_none():
    assert _prefill_value_for_label("nonexistent", VALUES) is None


def test_no_underscore_label_no_false_base():
    # "days" has no suffix to strip; absent → None, present → exact.
    assert _prefill_value_for_label("days", VALUES) == "90"
    assert _prefill_value_for_label("missing", VALUES) is None


def test_empty_value_yields_none_not_blank():
    assert _prefill_value_for_label("blank", VALUES) is None


def test_numbers_coerced_to_string():
    assert _prefill_value_for_label("n", {"n": 90}) == "90"
