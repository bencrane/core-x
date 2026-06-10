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
from ..payments import queries as pay_queries
from ..payments import stripe_client

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
        event = stripe_client.construct_event(raw, stripe_signature)
    except stripe_client.StripeError as exc:
        # Unconfigured secret vs bad signature: 503 vs 400 (never accept unverified).
        if "not set" in str(exc):
            raise HTTPException(status_code=503, detail=str(exc))
        raise HTTPException(status_code=400, detail=str(exc))

    event_type = str(event.get("type") or "")
    event_id = str(event.get("id") or "")
    obj = (event.get("data") or {}).get("object") or {}
    intent_id = obj.get("id")
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
