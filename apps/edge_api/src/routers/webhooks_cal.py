"""cal.com webhook — raw capture (always) + booking normalization.

  POST /webhooks/cal   X-Cal-Signature-256 — cal.com booking lifecycle events

1. Verify the HMAC signature against the RAW body (401 on mismatch; 503 if the secret
   is unconfigured — never accept unverified).
2. Land the VERBATIM envelope in ``public.cal_raw_events`` (COMMITTED — guaranteed).
3. Best-effort normalize into ``corex.bookings``:
     • BOOKING_CREATED   → idempotent upsert on cal_event_uid (handles cal.com's
                           duplicate deliveries).
     • BOOKING_CANCELLED → status='cancelled' by cal_event_uid.
     • BOOKING_RESCHEDULED / everything else → raw-only (normalization wired once a real
       reschedule payload is captured; its uid semantics are not assumed here).
   A normalization failure NEVER fails the webhook — the raw row is already durable and
   carries ``processed`` / ``processed_by`` for reprocessing.

Mounted at ``/webhooks/cal`` (NOT under ``/api/v1``) — the path cal.com posts to.
"""
from __future__ import annotations

import json
import logging
from typing import Any

from fastapi import APIRouter, Header, HTTPException, Request

from .. import config
from ..cal import normalize, queries
from ..cal.signature import verify_signature
from ..db import get_db_connection

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/webhooks", tags=["webhooks"])

_PROCESSED_BY = "edge_api/webhooks_cal"


@router.post("/cal")
async def cal_webhook(
    request: Request,
    x_cal_signature_256: str | None = Header(default=None),
) -> dict[str, Any]:
    secret = config.cal_webhook_secret()
    if secret is None:
        raise HTTPException(status_code=503, detail="CAL_WEBHOOK_SECRET not configured")

    raw = await request.body()
    if not verify_signature(raw, x_cal_signature_256, secret):
        raise HTTPException(status_code=401, detail="invalid cal.com signature")

    try:
        envelope = json.loads(raw)
    except ValueError:
        raise HTTPException(status_code=400, detail="invalid JSON body")
    if not isinstance(envelope, dict):
        raise HTTPException(status_code=400, detail="payload must be a JSON object")

    # The full envelope is stored verbatim (source of truth); these convenience columns are
    # best-effort + null-safe.
    trigger_event = str(envelope.get("triggerEvent") or "UNKNOWN")
    inner = envelope.get("payload") if isinstance(envelope.get("payload"), dict) else {}
    organizer = inner.get("organizer") if isinstance(inner.get("organizer"), dict) else {}
    attendees = inner.get("attendees") if isinstance(inner.get("attendees"), list) else []
    event_type_id = inner.get("eventTypeId")

    async with get_db_connection() as conn:
        # 1) RAW CAPTURE — committed before any normalization. Guaranteed.
        raw_id = await queries.insert_raw_event(
            conn,
            trigger_event=trigger_event,
            payload=envelope,
            cal_event_uid=inner.get("uid"),
            organizer_email=organizer.get("email"),
            attendee_emails=[a["email"] for a in attendees if isinstance(a, dict) and a.get("email")],
            event_type_id=event_type_id if isinstance(event_type_id, int) else None,
        )
        # 2) NORMALIZE — best-effort; a failure never fails the webhook (raw is durable).
        try:
            normalized = await _normalize(conn, trigger_event, envelope, raw_id)
            await conn.commit()
        except Exception as exc:  # noqa: BLE001 — capture is durable; defer normalization
            await conn.rollback()
            logger.warning("cal normalize deferred for raw %s (%s): %s", raw_id, trigger_event, exc)
            normalized = {"action": "deferred", "error": str(exc)[:300]}

    logger.info("cal webhook captured raw %s (%s) -> %s", raw_id, trigger_event, normalized.get("action"))
    return {"ok": True, "raw_id": raw_id, "trigger_event": trigger_event, "normalized": normalized}


async def _normalize(conn, trigger_event: str, envelope: dict[str, Any], raw_id: str) -> dict[str, Any]:
    """Drain one raw event into corex.bookings. Caller owns the commit/rollback."""
    kind = normalize.event_kind(trigger_event)

    if kind == "created":
        fields = normalize.extract(trigger_event, envelope)
        uid = fields.get("cal_event_uid")
        if not uid:
            return {"action": "skipped_no_uid"}
        await queries.upsert_booking(conn, fields=fields, source_raw_event_id=raw_id)
        await queries.mark_raw_processed(conn, raw_id, _PROCESSED_BY)
        return {"action": "created", "cal_event_uid": uid}

    if kind == "cancelled":
        inner = envelope.get("payload") or {}
        uid = inner.get("uid")
        if not uid:
            return {"action": "skipped_no_uid"}
        matched = await queries.cancel_booking(conn, uid)
        await queries.mark_raw_processed(conn, raw_id, _PROCESSED_BY)
        return {"action": "cancelled", "cal_event_uid": uid, "matched": matched}

    # rescheduled + everything else: raw is captured; normalization is wired against the real
    # payload once one is observed (reschedule uid semantics are not assumed here).
    return {"action": "raw_only", "kind": kind}
