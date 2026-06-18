"""Document-payment request/response shapes — the prospect-facing public payment surface."""
from __future__ import annotations

from pydantic import BaseModel


class DocumentPaymentInitPublic(BaseModel):
    """What the SPA needs to mount Stripe Elements: the intent client secret + the publishable key +
    the frozen amount. The secret key and customer id NEVER leave the server."""

    client_secret: str
    publishable_key: str
    amount_cents: int
    currency: str = "usd"
    payment_status: str


class DocumentPaymentStatePublic(BaseModel):
    """The authoritative payment state the SPA polls. ``payment_status`` flips to ``succeeded`` (and
    ``paid_at`` is set) ONLY via the Stripe webhook, never by the mint/read endpoints. ``rail`` is the
    settled payment method (``'card'`` | ``'us_bank_account'``), stamped by the webhook; ``None`` until
    settlement. It is cosmetic — the SPA uses it only to tailor the paid-state copy."""

    payment_status: str
    amount_cents: int | None = None
    currency: str = "usd"
    paid_at: str | None = None
    rail: str | None = None
