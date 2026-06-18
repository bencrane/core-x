"""Document payments — the prospect's direct-to-documenso engagement-fee surface (PUBLIC, pair-keyed).

  POST /api/v1/documenso/payment-intent/{opportunity_id}/{document_id}   PUBLIC — mint/reuse ACH intent
  GET  /api/v1/documenso/payment/{opportunity_id}/{document_id}          PUBLIC — authoritative state (poll)

PUBLIC: the ``(opportunity_id, document_id)`` pair IS the capability (same trust model as the
sign-token / sign-state reads). The amount is resolved server-side from the opportunity's
``fee_amount`` — never the browser. An intent is minted ONLY once the document is signed
(``DOCUMENT_COMPLETED`` for the pair, derived offline from the webhook capture); it is reused on
refresh so a prospect never accumulates duplicate intents. ``paid`` is advanced ONLY by the Stripe
webhook (``/webhooks/stripe``), never here.
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException

from .. import config
from ..db import get_db_connection
from ..documenso_webhooks import queries as sign_queries
from ..document_payments import amount as amount_resolver
from ..document_payments import queries as pay_queries
from ..document_payments import stripe as stripe_client
from ..document_payments.models import DocumentPaymentInitPublic, DocumentPaymentStatePublic

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/documenso", tags=["document-payments"])

# Intent statuses whose amount is still mutable (no funds in flight yet).
_AMOUNT_MUTABLE = {"requires_payment_method", "requires_confirmation", "requires_action"}


@router.post(
    "/payment-intent/{opportunity_id}/{document_id}",
    response_model=DocumentPaymentInitPublic,
)
async def create_document_payment_intent(
    opportunity_id: str, document_id: str
) -> DocumentPaymentInitPublic:
    async with get_db_connection() as conn:
        # Resolve the operator-selected Stripe mode (operator_settings.stripe_mode, augmenting the env
        # STRIPE_MODE) and its key set. Single-operator platform: this is the global selection (the
        # prospect mint has no operator session). Keys are mode-specific so the toggle takes effect
        # without a redeploy.
        mode = config.resolve_stripe_mode(await pay_queries.get_stripe_mode_selection(conn))
        publishable_key = config.stripe_publishable_key_for_mode(mode)
        if config.stripe_secret_key_for_mode(mode) is None or not publishable_key:
            raise HTTPException(status_code=503, detail="Stripe is not configured")

        # GATE: the document must be signed (DOCUMENT_COMPLETED for the pair). This also enforces the
        # pair — a document that does not belong to this opportunity is not "signed" for it → 409.
        state = await sign_queries.read_sign_state(
            conn, opportunity_id=opportunity_id, document_id=document_id
        )
        if not state["signed"]:
            raise HTTPException(status_code=409, detail="agreement not yet signed")

        # AMOUNT + customer, resolved from the opportunity (by its 8-char handle).
        info = await pay_queries.get_fee_and_contact(conn, opportunity_id)
        if not info:
            raise HTTPException(status_code=404, detail="opportunity not found")
        charge_cents = amount_resolver.resolve_fee_cents(info["field_values"])
        if charge_cents <= 0:
            raise HTTPException(status_code=409, detail="no payable fee_amount for this opportunity")
        if not info["recipient_email"]:
            raise HTTPException(status_code=422, detail="opportunity contact has no email")

        existing = await pay_queries.get_payment(conn, document_id) or {}
        if (existing.get("payment_status") or "none") == "succeeded":
            raise HTTPException(status_code=409, detail="already paid")
        existing_intent = existing.get("stripe_payment_intent_id")
        existing_customer = existing.get("stripe_customer_id")
        existing_status = existing.get("payment_status") or "none"

        # Reuse an open intent (idempotent across refresh/retry); only re-mint after a hard failure.
        # RECREATE a stale single-rail intent (minted before the card rail) — cancel it, then fall
        # through to the fresh dual-rail mint below — but ONLY when no funds are in flight
        # (amount-mutable status). An intent already mid-ACH (``processing``) is left alone so the
        # in-flight debit is never disrupted, even though it lacks the card tab.
        if existing_intent and existing_status not in ("failed", "canceled"):
            try:
                intent = await stripe_client.retrieve_payment_intent(existing_intent, mode)
                stale_single_rail = "card" not in (intent.get("payment_method_types") or [])
                if stale_single_rail and intent.get("status") in _AMOUNT_MUTABLE:
                    await stripe_client.cancel_payment_intent(existing_intent, mode)
                    # fall through to a fresh dual-rail mint (new idempotency-key namespace)
                else:
                    if intent.get("amount") != charge_cents and intent.get("status") in _AMOUNT_MUTABLE:
                        await stripe_client.update_payment_intent_amount(
                            existing_intent, charge_cents, mode
                        )
                        intent = await stripe_client.retrieve_payment_intent(existing_intent, mode)
                    if intent.get("client_secret"):
                        return DocumentPaymentInitPublic(
                            client_secret=intent["client_secret"],
                            publishable_key=publishable_key,
                            amount_cents=charge_cents,
                            currency=existing.get("currency") or "usd",
                            payment_status=existing_status,
                        )
            except Exception as exc:  # noqa: BLE001
                # Reuse is a pure optimization; ANY failure degrades to a fresh mint (idempotency_key
                # makes the mint return the SAME intent rather than duplicating), never a 500.
                logger.warning(
                    "reuse of document intent %s failed (%s); minting a new one", existing_intent, exc
                )

        try:
            customer_id = await stripe_client.ensure_customer(
                email=info["recipient_email"],
                name=info["recipient_name"],
                existing_id=existing_customer,
                mode=mode,
            )
            created = await stripe_client.create_payment_intent(
                amount_cents=charge_cents,
                customer_id=customer_id,
                opportunity_id=opportunity_id,
                document_id=document_id,
                idempotency_key=f"pay_document_{document_id}",
                mode=mode,
            )
        except stripe_client.StripeError as exc:
            logger.error(
                "document intent mint failed for %s/%s: %s", opportunity_id, document_id, exc
            )
            raise HTTPException(status_code=502, detail=f"stripe: {exc}") from exc

        await pay_queries.upsert_intent(
            conn,
            document_id=document_id,
            opportunity_id=opportunity_id,
            amount_cents=charge_cents,
            currency="usd",
            customer_id=customer_id,
            intent_id=created["id"],
            status="requires_payment",
        )

    return DocumentPaymentInitPublic(
        client_secret=created["client_secret"],
        publishable_key=publishable_key,
        amount_cents=charge_cents,
        currency="usd",
        payment_status="requires_payment",
    )


@router.get(
    "/payment/{opportunity_id}/{document_id}",
    response_model=DocumentPaymentStatePublic,
)
async def get_document_payment_state(
    opportunity_id: str, document_id: str
) -> DocumentPaymentStatePublic:
    """Authoritative payment state for the SPA poll. Returns ``none`` before the first mint (so the
    SPA can poll without erroring) and for a pair mismatch (no info leak about a guessed document id)."""
    async with get_db_connection() as conn:
        pay = await pay_queries.get_payment(conn, document_id)
    if pay is None or pay.get("opportunity_id") != opportunity_id:
        return DocumentPaymentStatePublic(payment_status="none")
    paid_at = pay.get("paid_at")
    return DocumentPaymentStatePublic(
        payment_status=pay.get("payment_status") or "none",
        amount_cents=pay.get("amount_cents"),
        currency=pay.get("currency") or "usd",
        paid_at=paid_at.isoformat() if paid_at else None,
        rail=pay.get("rail"),
    )
