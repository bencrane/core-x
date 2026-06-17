"""Stripe ACH (us_bank_account) wrapper for the document engagement fee — Customer + PaymentIntent.

Self-contained for the direct-to-documenso flow: the intent carries ``metadata.kind='document'`` plus
the ``(opportunity_id, document_id)`` pair, so the single Stripe webhook routes the advance to the
document_payments record. Card is NOT offered (``us_bank_account`` only); ``setup_future_usage=
'off_session'`` captures the ACH document for later quarterly debits. The Stripe SDK is synchronous —
every call runs in a worker thread so it never blocks the event loop. The amount is passed in by the
router (resolved from ``fee_amount``); this module never decides the amount, and the secret key never
reaches the browser.
"""
from __future__ import annotations

import asyncio
from typing import Any

import stripe

from .. import config

_CURRENCY = "usd"  # ACH (us_bank_account) is USD-only.


class StripeError(RuntimeError):
    """A Stripe API error or an unconfigured client."""


def _require_secret(mode: str) -> str:
    """Set the SDK key for the resolved Stripe ``mode`` ('test'|'live') and return it. The mode is the
    operator's selection (operator_settings.stripe_mode), resolved per-request — so a single global
    ``stripe.api_key`` is correct (the mode is constant across a request, not interleaved)."""
    key = config.stripe_secret_key_for_mode(mode)
    if not key:
        raise StripeError(f"STRIPE_SECRET_KEY ({mode}) is not set")
    stripe.api_key = key
    return key


async def ensure_customer(*, email: str, name: str | None, existing_id: str | None, mode: str) -> str:
    """A Stripe Customer id for this prospect, reusing the one already on the row if it still resolves
    in the CURRENT Stripe ``mode``. A persisted id can be stale — created under the other mode
    (test↔live) or since deleted — and passing it to PaymentIntent.create then 502s with "No such
    customer". So when an id is present we retrieve it first; if it doesn't resolve (or is deleted) we
    mint a fresh one, and the caller re-persists it so the record self-heals."""
    _require_secret(mode)
    if existing_id:
        try:
            cust = await asyncio.to_thread(stripe.Customer.retrieve, existing_id)
            if not getattr(cust, "deleted", False):
                return existing_id
        except Exception:  # noqa: BLE001 — stale/cross-mode/deleted id → fall through and mint fresh
            pass
    try:
        customer = await asyncio.to_thread(
            lambda: stripe.Customer.create(
                email=email,
                name=name or None,
                metadata={"source": "edge_api/document"},
            )
        )
    except Exception as exc:  # noqa: BLE001 — surface as our own error type
        raise StripeError(f"customer create failed: {exc}") from exc
    return str(customer["id"])


async def create_payment_intent(
    *,
    amount_cents: int,
    customer_id: str,
    opportunity_id: str,
    document_id: str,
    idempotency_key: str,
    mode: str,
) -> dict[str, Any]:
    """ACH PaymentIntent for the document fee in the resolved Stripe ``mode``. Idempotent on
    ``idempotency_key`` (``ach_document_{document_id}``) so a retried mint returns the same intent.
    ``metadata`` carries the routing pair (``kind='document'``, ``opportunity_id``, ``document_id``)
    for the webhook."""
    _require_secret(mode)
    try:
        intent = await asyncio.to_thread(
            lambda: stripe.PaymentIntent.create(
                amount=int(amount_cents),
                currency=_CURRENCY,
                customer=customer_id,
                payment_method_types=["us_bank_account"],
                setup_future_usage="off_session",
                payment_method_options={"us_bank_account": {"verification_method": "automatic"}},
                description=f"Rare Structure engagement — document {opportunity_id}/{document_id}",
                metadata={
                    "kind": "document",
                    "opportunity_id": opportunity_id,
                    "document_id": document_id,
                },
                idempotency_key=idempotency_key,
            )
        )
    except Exception as exc:  # noqa: BLE001
        raise StripeError(f"payment_intent create failed: {exc}") from exc
    return {
        "id": str(intent["id"]),
        "client_secret": intent["client_secret"],
        "status": intent["status"],
    }


async def retrieve_payment_intent(intent_id: str, mode: str) -> dict[str, Any]:
    _require_secret(mode)
    try:
        intent = await asyncio.to_thread(stripe.PaymentIntent.retrieve, intent_id)
    except Exception as exc:  # noqa: BLE001
        raise StripeError(f"payment_intent retrieve failed: {exc}") from exc
    # NB: this stripe SDK's StripeObject routes attribute access through __getattr__ and does NOT expose
    # a dict ``.get`` — use getattr(...,default) for optional fields (AttributeError→default), subscript
    # for always-present ones.
    return {
        "id": str(intent["id"]),
        "client_secret": getattr(intent, "client_secret", None),
        "status": intent["status"],
        "amount": getattr(intent, "amount", None),
    }


async def update_payment_intent_amount(intent_id: str, amount_cents: int, mode: str) -> None:
    """Re-sync the intent amount if the resolved fee changed before payment (no funds in flight yet)."""
    _require_secret(mode)
    try:
        await asyncio.to_thread(stripe.PaymentIntent.modify, intent_id, amount=int(amount_cents))
    except Exception as exc:  # noqa: BLE001
        raise StripeError(f"payment_intent modify failed: {exc}") from exc


def construct_event_any(payload: bytes, sig_header: str | None) -> Any:
    """Verify + parse a Stripe webhook against ANY configured signing secret (test + live). The single
    ``/webhooks/stripe`` endpoint receives events signed by whichever mode an intent was created in
    (the document flow's mode is operator-toggleable at runtime), so a single fixed secret would reject
    the other mode's events and silently stall ``paid``. Raises StripeError on a missing secret (so the
    route 503s) or when none verify (400)."""
    secrets = config.stripe_webhook_secrets()
    if not secrets:
        raise StripeError("STRIPE_WEBHOOK_SECRET is not set")
    if not sig_header:
        raise StripeError("missing Stripe-Signature header")
    last_exc: Exception | None = None
    for secret in secrets:
        try:
            return stripe.Webhook.construct_event(payload, sig_header, secret)
        except Exception as exc:  # noqa: BLE001 — try the next configured secret
            last_exc = exc
    raise StripeError(f"webhook signature verification failed: {last_exc}")
