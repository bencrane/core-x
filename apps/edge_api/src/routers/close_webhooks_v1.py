"""Close.com webhook — RAW landing (system of record) + offline "now dialing" read.

  POST /webhooks/close   (Close-Sig-Hash / Close-Sig-Timestamp)  — capture every Close event verbatim
  GET  /api/v1/close/active-call/{auth_user_id}                  — derive the operator's current call

Mirrors the cal.com + Documenso receivers: signature-gated, append-only raw capture into
``business.close_webhook_events`` (no projection at write time), and an OFFLINE derivation the
Insights tab polls through the platform-api BFF. The Power Dialer fires an ``activity.call``
``created`` event per number as it advances; the derivation surfaces the latest outbound one,
resolved to its briefing anchor (normalized_domain) via public.close_crosswalk.
"""
from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException, Request

from .. import config
from ..cal import book
from ..close import queries
from ..close.signature import verify_signature
from ..db import get_db_connection
from ..service_token import require_service_token

logger = logging.getLogger(__name__)

# Webhook receiver — NOT under /api/v1 (the path Close posts to), unauthenticated except the
# HMAC signature. Mirrors /webhooks/cal and /webhooks/stripe.
webhook_router = APIRouter(prefix="/webhooks", tags=["webhooks"])

# Read surface — service-token gated; the platform-api BFF brokers it with the operator session.
read_router = APIRouter(
    prefix="/api/v1/close", tags=["close"], dependencies=[Depends(require_service_token)]
)


def _dig(obj: Any, *keys: str) -> Any:
    """First present, non-null key from a dict (defensive across payload-shape variants)."""
    if isinstance(obj, dict):
        for k in keys:
            if obj.get(k) is not None:
                return obj[k]
    return None


@webhook_router.post("/close")
async def close_webhook(
    request: Request,
    close_sig_hash: str | None = Header(default=None),
    close_sig_timestamp: str | None = Header(default=None),
) -> dict[str, Any]:
    """Capture a Close webhook delivery RAW. Signature-gated; append-only; no projection here."""
    secret = config.close_webhook_secret()
    if secret is None:
        raise HTTPException(status_code=503, detail="CLOSE_WEBHOOK_SECRET not configured")

    raw = await request.body()
    if not verify_signature(raw, close_sig_hash, close_sig_timestamp, secret):
        raise HTTPException(status_code=401, detail="invalid Close signature")

    try:
        body = await request.json()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail="invalid JSON body") from exc
    raw_obj: Any = body if isinstance(body, dict) else {"_raw": body}

    # Close wraps the Event under `event` (tolerate an unwrapped delivery too). The call object is
    # `event.data`; identity fields are on it (lead_id/contact_id/user_id/direction/status/phone).
    evt = _dig(raw_obj, "event") or raw_obj
    data = _dig(evt, "data") or {}

    event_id = await _land(raw_obj, evt, data)
    logger.info(
        "close webhook captured: id=%s object=%s action=%s lead=%s",
        event_id, _dig(evt, "object_type"), _dig(evt, "action"), _dig(data, "lead_id"),
    )

    # CUSTOM-ACTIVITY → cal.com booking kickoff. Best-effort: fires the cal-book Trigger.dev task ONLY
    # for a created custom activity matching the configured booking type; never fails the webhook (the
    # raw event above is already durable). Mirrors webhooks_cal.py's _kick_* pattern.
    booking = None
    if _dig(evt, "object_type") == "activity.custom_activity" and _dig(evt, "action") == "created":
        booking = await _kick_booking(data)

    return {"ok": True, "id": event_id, "booking": booking}


async def _land(raw_obj: Any, evt: Any, data: Any) -> str:
    async with get_db_connection() as conn:
        return await queries.insert_event(
            conn,
            event_id=_s(_dig(evt, "id")),
            object_type=_s(_dig(evt, "object_type")),
            action=_s(_dig(evt, "action")),
            close_user_id=_s(_dig(data, "user_id") or _dig(evt, "user_id")),
            close_lead_id=_s(_dig(data, "lead_id") or _dig(evt, "lead_id")),
            close_contact_id=_s(_dig(data, "contact_id")),
            direction=_s(_dig(data, "direction")),
            status=_s(_dig(data, "status")),
            remote_phone=_s(_dig(data, "remote_phone") or _dig(data, "phone")),
            payload=raw_obj,
        )


def _s(v: Any) -> str | None:
    return str(v) if v is not None else None


async def _kick_booking(data: Any) -> dict[str, Any]:
    """Fire the cal-book task when a CREATED custom activity matches the configured booking type.
    Best-effort — logs + returns on any miss/failure (the raw event is already durable). The actual
    booking (Close API read-back + cal.com create) runs in /internal/cal/book; this only enqueues."""
    target = config.close_booking_custom_activity_type_id()
    type_id = _s(_dig(data, "custom_activity_type_id"))
    activity_id = _s(_dig(data, "id"))
    # One decision-log line so config state (dormant / typo'd type id) is visible without guessing why
    # no booking fired.
    logger.info(
        "close custom-activity created: activity=%s type_id=%s configured_target=%s",
        activity_id, type_id, target,
    )
    if not target:
        return {"triggered": False, "reason": "booking custom-activity type not configured"}
    if type_id != target:
        return {"triggered": False, "reason": "custom_activity_type_id mismatch"}
    if not activity_id:
        return {"triggered": False, "reason": "no activity id"}
    run_id = await book.trigger_book(
        close_activity_id=activity_id,
        close_lead_id=_s(_dig(data, "lead_id")),
        close_contact_id=_s(_dig(data, "contact_id")),
        custom_activity_type_id=type_id,
    )
    if not run_id:
        return {"triggered": False, "reason": "trigger returned no run id"}
    return {"triggered": True, "run_id": run_id}


@read_router.get("/active-call")
async def active_call() -> dict[str, Any]:
    """The current outbound Close call, derived offline (NOT operator-scoped — single operator,
    "all users are me"). The Insights tab polls this (through platform-api) and loads the briefing
    on domain change. Still service-token gated: only the BFF, with a validated session, reaches it."""
    async with get_db_connection() as conn:
        return await queries.read_active_call(conn)
