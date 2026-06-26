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
    return {"ok": True, "id": event_id}


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


@read_router.get("/active-call/{auth_user_id}")
async def active_call(auth_user_id: str) -> dict[str, Any]:
    """The operator's current outbound call, derived offline from the raw events + crosswalk.
    The Insights tab polls this (through platform-api) and loads the briefing on domain change."""
    async with get_db_connection() as conn:
        return await queries.read_active_call(conn, auth_user_id=auth_user_id)
