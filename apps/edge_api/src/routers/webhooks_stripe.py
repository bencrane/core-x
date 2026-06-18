"""Stripe webhook — authoritative ACH payment-state advance + append-only audit.

  POST /webhooks/stripe   Stripe-Signature — payment_intent lifecycle for engagement ACH debits

Mirrors the cal.com webhook discipline:
  1. Verify the signature (400 on bad/missing; 503 if STRIPE_WEBHOOK_SECRET is unset — never accept
     an unverified event).
  2. Append the verbatim event to ``business.engagement_events`` (idempotent on the Stripe event id) —
     the audit trail, and the redelivery guard.
  3. Advance ``payment_status`` on the engagement row (monotonic; 'succeeded' is terminal).

ACH settles asynchronously, so this — NOT the browser confirm result — is the only thing that sets
'paid'. A normalization failure never loses the event: the ledger row is committed first.

SEAM: durable post-payment fulfillment (provision / receipt / CRM) belongs on
``payment_intent.succeeded`` and should be handed to a Trigger.dev task
(``services.trigger_dev_client``) — left as a marked hook below, intentionally not wired yet.

Mounted at ``/webhooks/stripe`` (NOT under ``/api/v1``) — the path Stripe posts to.
"""
from __future__ import annotations

import datetime as _dt
import json
import logging
from typing import Any

from fastapi import APIRouter, Header, HTTPException, Request

from ..db import get_db_connection
from ..document_payments import queries as doc_pay_queries
from ..document_payments import stripe as doc_pay_stripe
from ..payments import queries as pay_queries

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/webhooks", tags=["webhooks"])

# Stripe PaymentIntent event → our persisted payment_status.
_EVENT_TO_STATUS: dict[str, str] = {
    "payment_intent.processing": "processing",
    "payment_intent.succeeded": "succeeded",
    "payment_intent.payment_failed": "failed",
    "payment_intent.canceled": "canceled",
}


@router.post("/stripe")
async def stripe_webhook(
    request: Request, stripe_signature: str | None = Header(default=None),
) -> dict[str, Any]:
    raw = await request.body()
    try:
        # Verify against ANY configured secret (test + live) — the document-payment mode is
        # operator-toggleable at runtime, so events arrive signed by whichever mode minted the intent.
        # The legacy proposal events verify the same way (shared secrets, one Stripe account).
        event = doc_pay_stripe.construct_event_any(raw, stripe_signature)
    except doc_pay_stripe.StripeError as exc:
        # Unconfigured secret vs bad signature: 503 vs 400 (never accept unverified).
        if "not set" in str(exc):
            raise HTTPException(status_code=503, detail=str(exc))
        raise HTTPException(status_code=400, detail=str(exc))

    event_type = str(event.get("type") or "")
    event_id = str(event.get("id") or "")
    obj = (event.get("data") or {}).get("object") or {}
    intent_id = obj.get("id")

    # Dispatch by payment kind. Direct-to-documenso "document" payments carry metadata.kind="document"
    # + the (opportunity_id, document_id) pair → advance business.document_payments. Everything else
    # falls through to the legacy proposal-ref path below, which is left untouched.
    if (obj.get("metadata") or {}).get("kind") == "document":
        return await _handle_document_payment(event_type, event_id, obj, raw)

    ref = (obj.get("metadata") or {}).get("ref")

    status = _EVENT_TO_STATUS.get(event_type)
    if status is None or not intent_id:
        return {"ok": True, "ignored": True, "event": event_type}

    # The verbatim, signature-verified body (plain JSON) is the audit record.
    try:
        envelope = json.loads(raw)
    except ValueError:
        envelope = {"raw_unparseable": True}

    async with get_db_connection() as conn:
        if not ref:
            ref = await pay_queries.ref_for_intent(conn, intent_id)
        if not ref:
            return {"ok": True, "ignored": True, "event": event_type, "reason": "no matching proposal"}

        # 1) AUDIT — append the verbatim event first (idempotent on the Stripe event id).
        first_seen = await pay_queries.insert_event(
            conn, ref=ref, source="stripe", event_type=event_type, idempotency_key=event_id,
            payload=envelope,
        )
        # 2) ADVANCE — monotonic; out-of-order/duplicate deliveries cannot regress state.
        paid_at = _dt.datetime.now(_dt.timezone.utc) if status == "succeeded" else None
        amount_cents = obj.get("amount") if status == "succeeded" else None
        applied = await pay_queries.advance_payment_status(
            conn, intent_id=intent_id, status=status, paid_at=paid_at, amount_cents=amount_cents,
        )

    if status == "succeeded" and first_seen and applied:
        # SEAM: hand durable fulfillment to Trigger.dev here (provision, receipt, CRM/ledger fan-out).
        # Intentionally not wired yet — the engagement_events row above is the audit of record.
        logger.info("engagement %s PAID (intent %s) — fulfillment seam", ref, intent_id)

    logger.info(
        "stripe webhook %s ref=%s applied=%s first_seen=%s", event_type, ref, applied, first_seen
    )
    return {"ok": True, "event": event_type, "status": status, "applied": applied}


async def _handle_document_payment(
    event_type: str, event_id: str, obj: dict[str, Any], raw: bytes
) -> dict[str, Any]:
    """Advance ``business.document_payments`` for a direct-to-documenso payment (``metadata.kind=
    "document"``). Self-contained — no contact with the legacy proposal path. Append the verbatim
    event first (idempotent on the Stripe event id), then advance the pair's status. ACH settles
    asynchronously, so this — NOT the browser confirm — is the only thing that sets ``paid``."""
    status = _EVENT_TO_STATUS.get(event_type)
    intent_id = obj.get("id")
    md = obj.get("metadata") or {}
    document_id = md.get("document_id")
    opportunity_id = md.get("opportunity_id")
    if status is None or not document_id:
        return {"ok": True, "ignored": True, "event": event_type}

    try:
        envelope = json.loads(raw)
    except ValueError:
        envelope = {"raw_unparseable": True}

    paid = status == "succeeded"
    # Derive the settled rail from the event TYPE — reliable for this two-rail intent without a Stripe
    # call (the webhook stays call-free) and without parsing ``charges`` (absent from PaymentIntent
    # payloads on the pinned SDK). ACH is always async: ``processing`` ⟹ us_bank_account. Card captures
    # synchronously, jumping straight to ``succeeded`` with no prior ``processing`` ⟹ card. ``rail`` is
    # set-once in advance_status (COALESCE), so an ACH ``succeeded`` after its ``processing`` keeps
    # 'us_bank_account'.
    rail = (
        "us_bank_account"
        if status == "processing"
        else "card"
        if status == "succeeded"
        else None
    )
    async with get_db_connection() as conn:
        # 1) AUDIT + idempotency — append the verbatim event first; a redelivery returns first_seen=False.
        first_seen = await doc_pay_queries.record_event_if_new(
            conn,
            stripe_event_id=event_id,
            event_type=event_type,
            document_id=document_id,
            opportunity_id=opportunity_id,
            payload=envelope,
        )
        # 2) ADVANCE — only on first sight; monotonic (paid_at + rail set once).
        if first_seen:
            await doc_pay_queries.advance_status(
                conn,
                document_id=document_id,
                status=status,
                paid=paid,
                intent_id=intent_id,
                rail=rail,
            )
        await conn.commit()

    if paid and first_seen:
        # SEAM: durable post-payment fulfillment (provision / receipt / CRM) belongs here, handed to
        # Trigger.dev. Intentionally not wired — the document_payment_events row is the audit of record.
        logger.info(
            "document %s/%s PAID (intent %s) — fulfillment seam", opportunity_id, document_id, intent_id
        )
    logger.info(
        "stripe webhook(document) %s doc=%s first_seen=%s", event_type, document_id, first_seen
    )
    return {"ok": True, "event": event_type, "status": status, "first_seen": first_seen}
