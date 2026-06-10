"""Unit tests for the ACH payment amount resolver + status-rank invariants. No network, no DB.

These pin the two pure-logic guarantees the payment flow depends on:
  1. The charge amount is the proposal's QUARTERLY total (billed in advance), resolved from the
     persisted proposal content — never hardcoded, never taken from the browser.
  2. The payment-status rank is strictly monotonic, so the webhook's forward-only guard holds
     (a late/duplicate 'processing' can never overwrite 'succeeded').
"""
from __future__ import annotations

import datetime as dt

from apps.edge_api.src.payments.amount import resolve_charge_cents
from apps.edge_api.src.payments.models import PAYMENT_RANK
from apps.edge_api.src.proposals.models import Proposal


def _proposal(**overrides) -> Proposal:
    base = dict(
        ref="rs_pay_test",
        template_id="strategic_origination_mandate",
        client_name="Angular Capital Partners LP",
        client_signer_name="Dana Reyes",
        client_title="Managing Partner",
        client_email="dana@angularcap.example",
        effective_date=dt.date(2026, 6, 9),
        monthly_fee_cents=2_500_000,
        quarterly_total_cents=7_500_000,
        rs_signer_name="Benjamin Crane",
        status="completed",
    )
    base.update(overrides)
    return Proposal(**base)


def test_charge_is_quarterly_total_not_monthly():
    """The ACH debit is the quarterly fee (billed 3 months in advance), not the monthly fee."""
    p = _proposal()
    assert resolve_charge_cents(p) == 7_500_000
    assert resolve_charge_cents(p) != p.monthly_fee_cents


def test_charge_reads_persisted_amount_verbatim():
    """A different persisted quarterly total flows straight through — nothing is hardcoded."""
    p = _proposal(monthly_fee_cents=5_000_000, quarterly_total_cents=15_000_000)
    assert resolve_charge_cents(p) == p.quarterly_total_cents


def test_payment_rank_is_strictly_monotonic():
    assert (
        PAYMENT_RANK["none"]
        < PAYMENT_RANK["requires_payment"]
        < PAYMENT_RANK["processing"]
        < PAYMENT_RANK["succeeded"]
    )
