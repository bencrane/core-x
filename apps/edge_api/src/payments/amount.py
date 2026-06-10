"""Charge-amount resolution — the SINGLE seam between the proposal and what Stripe debits.

The amount is NEVER hardcoded and NEVER taken from the browser: it is resolved server-side from
the persisted proposal content. Today that content is the ``business.engagement_proposals`` row;
the agreement is billed QUARTERLY IN ADVANCE, so the charge is ``quarterly_total_cents`` — the same
``<<quarterlyTotal>>`` figure rendered into the PDF the client signed. Charge == signed amount, by
construction.

This is the only place that decision lives. If per-proposal content moves upstream (e.g. a Lance
per-proposal record), repoint THIS function at that source; the payment router, the Stripe wiring,
and the page contract are all unchanged.
"""
from __future__ import annotations

from ..proposals.models import Proposal


def resolve_charge_cents(p: Proposal) -> int:
    """The amount (minor units) to debit for this engagement: the quarterly fee, billed in advance."""
    return int(p.quarterly_total_cents)
