"""Stripe ACH client — PaymentIntent (us_bank_account) for the engagement infrastructure fee.

Card is NOT offered: ``payment_method_types=['us_bank_account']`` only. The intent is created with
a Customer + ``setup_future_usage='off_session'`` so the ACH mandate is captured for the recurring
quarterly debit (future-cycle billing orchestration is a later piece). The amount is passed in by
the router (resolved from proposal content) — this module never decides the amount.

The Stripe Python SDK is synchronous; every network call runs in a worker thread
(``asyncio.to_thread``) so it never blocks the FastAPI event loop. The secret key is read from
``config`` and set per-process on first use; it never reaches the browser. ``construct_event``
verifies the inbound webhook signature against ``STRIPE_WEBHOOK_SECRET``.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any

import stripe

from .. import config

logger = logging.getLogger(__name__)

_CURRENCY = "usd"  # ACH (us_bank_account) is USD-only.


class StripeError(RuntimeError):
    """A Stripe API error or an unconfigured client."""


def _require_secret() -> str:
    key = config.stripe_secret_key()
    if not key:
        raise StripeError("STRIPE_SECRET_KEY is not set")
    stripe.api_key = key
    return key


async def ensure_customer(*, email: str, name: str | None, existing_id: str | None) -> str:
    """Return a Stripe Customer id for this client, reusing the one already on the row if present."""
    if existing_id:
        return existing_id
    _require_secret()
    try:
        customer = await asyncio.to_thread(
            lambda: stripe.Customer.create(
                email=email,
                name=name or None,
                metadata={"source": "edge_api/engagement"},
            )
        )
    except Exception as exc:  # noqa: BLE001 — surface as our own error type
        raise StripeError(f"customer create failed: {exc}") from exc
    return str(customer["id"])


async def create_payment_intent(
    *, amount_cents: int, customer_id: str, ref: str, idempotency_key: str,
) -> dict[str, Any]:
    """Create an ACH PaymentIntent for ``amount_cents``. Idempotent on ``idempotency_key`` so a
    retried create returns the same intent rather than minting a duplicate."""
    _require_secret()
    try:
        intent = await asyncio.to_thread(
            lambda: stripe.PaymentIntent.create(
                amount=int(amount_cents),
                currency=_CURRENCY,
                customer=customer_id,
                payment_method_types=["us_bank_account"],
                setup_future_usage="off_session",
                payment_method_options={"us_bank_account": {"verification_method": "automatic"}},
                description=f"Rare Structure engagement — {ref}",
                metadata={"ref": ref, "kind": "engagement"},
                idempotency_key=idempotency_key,
            )
        )
    except Exception as exc:  # noqa: BLE001
        raise StripeError(f"payment_intent create failed: {exc}") from exc
    return {"id": str(intent["id"]), "client_secret": intent["client_secret"], "status": intent["status"]}


async def retrieve_payment_intent(intent_id: str) -> dict[str, Any]:
    _require_secret()
    try:
        intent = await asyncio.to_thread(stripe.PaymentIntent.retrieve, intent_id)
    except Exception as exc:  # noqa: BLE001
        raise StripeError(f"payment_intent retrieve failed: {exc}") from exc
    return {
        "id": str(intent["id"]),
        "client_secret": intent.get("client_secret"),
        "status": intent["status"],
        "amount": intent.get("amount"),
    }


async def update_payment_intent_amount(intent_id: str, amount_cents: int) -> None:
    """Re-sync the intent amount if the proposal's resolved amount changed before payment."""
    _require_secret()
    try:
        await asyncio.to_thread(stripe.PaymentIntent.modify, intent_id, amount=int(amount_cents))
    except Exception as exc:  # noqa: BLE001
        raise StripeError(f"payment_intent modify failed: {exc}") from exc


def construct_event(payload: bytes, sig_header: str | None) -> Any:
    """Verify + parse a Stripe webhook. Raises StripeError on a missing secret or bad signature."""
    secret = config.stripe_webhook_secret()
    if not secret:
        raise StripeError("STRIPE_WEBHOOK_SECRET is not set")
    if not sig_header:
        raise StripeError("missing Stripe-Signature header")
    try:
        return stripe.Webhook.construct_event(payload, sig_header, secret)
    except Exception as exc:  # SignatureVerificationError / ValueError
        raise StripeError(f"webhook signature verification failed: {exc}") from exc
