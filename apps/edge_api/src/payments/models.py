"""Payment projections — what the public pay page (via the BFF) consumes.

Money is integer minor units (cents) end-to-end; the page formats for display. None of these carry
the Stripe secret key — only the publishable key (public by design) and the PaymentIntent
``client_secret`` (a per-intent capability the browser needs to confirm the ACH debit).
"""
from __future__ import annotations

from pydantic import BaseModel

# Stripe PaymentIntent status, narrowed to the ACH lifecycle we persist.
PAYMENT_STATUSES = ("none", "requires_payment", "processing", "succeeded", "failed", "canceled")

# Monotonic rank — a webhook may not regress a higher state (e.g. 'processing' cannot overwrite
# 'succeeded'). 'failed'/'canceled' are side-exits, applied unless already succeeded.
PAYMENT_RANK: dict[str, int] = {"none": 0, "requires_payment": 1, "processing": 2, "succeeded": 3}


class PaymentInitPublic(BaseModel):
    """POST /payment-intent → everything the browser needs to mount the ACH PaymentElement."""

    client_secret: str
    publishable_key: str
    amount_cents: int
    currency: str = "usd"
    payment_status: str


class PaymentStatePublic(BaseModel):
    """GET /payment → the authoritative (webhook-driven) state the page polls after confirm."""

    payment_status: str
    amount_cents: int | None = None
    currency: str = "usd"
    paid_at: str | None = None
