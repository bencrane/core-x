"""Payments — Stripe ACH (us_bank_account) PaymentIntent + Elements for the engagement fee.

  POST /api/v1/proposals/{ref}/payment-intent   PUBLIC (ref) — mint/reuse the ACH PaymentIntent;
                                                                returns client_secret + publishable key
  GET  /api/v1/proposals/{ref}/payment          PUBLIC (ref) — authoritative payment state (poll target)

The ``ref`` is the bearer capability (same trust model as the proposal shell read). The amount is
resolved server-side from the proposal content (``payments.amount``) — never from the browser. A
PaymentIntent is minted only once the agreement is COMPLETED (signed); it is reused on refresh so a
client never accumulates duplicate intents. ``paid`` is set elsewhere — by the Stripe webhook only.
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException

from .. import config
from ..db import get_db_connection
from ..payments import amount as amount_resolver
from ..payments import queries as pay_queries
from ..payments import stripe_client
from ..payments.models import PaymentInitPublic, PaymentStatePublic
from ..proposals import queries as proposal_queries

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/proposals", tags=["payments"])

# The agreement must be executed (signed/completed) before money is collected.
_PAYABLE_STATUSES = {"signed", "completed"}
# Intent statuses whose amount is still mutable (no funds in flight yet).
_AMOUNT_MUTABLE = {"requires_payment_method", "requires_confirmation", "requires_action"}


@router.post("/{ref}/payment-intent", response_model=PaymentInitPublic)
async def create_payment_intent(ref: str) -> PaymentInitPublic:
    publishable_key = config.stripe_publishable_key()
    if config.stripe_secret_key() is None or not publishable_key:
        raise HTTPException(status_code=503, detail="Stripe is not configured")

    async with get_db_connection() as conn:
        proposal = await proposal_queries.get_by_ref(conn, ref)
        if proposal is None:
            raise HTTPException(status_code=404, detail="proposal not found")
        if proposal.status not in _PAYABLE_STATUSES:
            raise HTTPException(status_code=409, detail="agreement not yet signed")

        charge_cents = amount_resolver.resolve_charge_cents(proposal)
        if charge_cents <= 0:
            raise HTTPException(status_code=409, detail="proposal has no payable amount")

        pay = await pay_queries.get_payment_by_ref(conn, ref) or {}
        existing_intent = pay.get("stripe_payment_intent_id")
        existing_customer = pay.get("stripe_customer_id")
        existing_status = pay.get("payment_status") or "none"

        # Reuse an open intent (idempotent across refresh/retry); only re-mint after a hard failure.
        if existing_intent and existing_status not in ("failed", "canceled"):
            try:
                intent = await stripe_client.retrieve_payment_intent(existing_intent)
                if intent.get("amount") != charge_cents and intent.get("status") in _AMOUNT_MUTABLE:
                    await stripe_client.update_payment_intent_amount(existing_intent, charge_cents)
                    intent = await stripe_client.retrieve_payment_intent(existing_intent)
                if intent.get("client_secret"):
                    return PaymentInitPublic(
                        client_secret=intent["client_secret"],
                        publishable_key=publishable_key,
                        amount_cents=charge_cents,
                        currency=pay.get("payment_currency") or "usd",
                        payment_status=existing_status,
                    )
            except Exception as exc:  # noqa: BLE001
                # Reuse is a pure optimization (avoid duplicate intents); ANY failure here — a Stripe
                # error, a stale/absent client_secret, a projection hiccup — must degrade to a fresh
                # mint, never 500. The mint below is idempotent (idempotency_key=ach_{ref}), so it
                # returns the SAME intent rather than duplicating.
                logger.warning("reuse of intent %s failed (%s); minting a new one", existing_intent, exc)

        # Mint a new Customer (if needed) + ACH PaymentIntent.
        try:
            customer_id = await stripe_client.ensure_customer(
                email=proposal.client_email,
                name=proposal.client_signer_name,
                existing_id=existing_customer,
            )
            created = await stripe_client.create_payment_intent(
                amount_cents=charge_cents, customer_id=customer_id, ref=ref,
                idempotency_key=f"ach_{ref}",
            )
        except stripe_client.StripeError as exc:
            logger.error("payment-intent creation failed for %s: %s", ref, exc)
            raise HTTPException(status_code=502, detail=f"stripe: {exc}") from exc

        await pay_queries.attach_payment_intent(
            conn, ref, customer_id=customer_id, intent_id=created["id"], amount_cents=charge_cents,
            currency="usd", status="requires_payment",
        )
        await pay_queries.insert_event(
            conn, ref=ref, source="system", event_type="payment_intent.created",
            idempotency_key=created["id"],
            payload={"amount_cents": charge_cents, "intent": created["id"]},
        )

    return PaymentInitPublic(
        client_secret=created["client_secret"], publishable_key=publishable_key,
        amount_cents=charge_cents, currency="usd", payment_status="requires_payment",
    )


@router.get("/{ref}/payment", response_model=PaymentStatePublic)
async def get_payment_state(ref: str) -> PaymentStatePublic:
    async with get_db_connection() as conn:
        pay = await pay_queries.get_payment_by_ref(conn, ref)
    if pay is None:
        raise HTTPException(status_code=404, detail="proposal not found")
    paid_at = pay.get("paid_at")
    return PaymentStatePublic(
        payment_status=pay.get("payment_status") or "none",
        amount_cents=pay.get("amount_charged_cents"),
        currency=pay.get("payment_currency") or "usd",
        paid_at=paid_at.isoformat() if paid_at else None,
    )
