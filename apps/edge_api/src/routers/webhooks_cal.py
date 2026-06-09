"""cal.com webhook — RAW CAPTURE ONLY (Phase 1).

  POST /webhooks/cal   X-Cal-Signature-256 — cal.com events

1. Verify the HMAC signature against the RAW body (401 on mismatch; 503 if the secret
   is unconfigured — never accept unverified).
2. Land the VERBATIM envelope in ``public.cal_raw_events`` and return 200.

That is the entire job for now. Normalizing the payload into ``corex.bookings`` (and
splitting created / cancelled / rescheduled) is a SEPARATE step, wired against the real
captured payload shape — deliberately NOT modeled here. Mounted at ``/webhooks/cal``
(not under ``/api/v1``) — the path cal.com posts to.
"""
from __future__ import annotations

import json
import logging
from typing import Any

from fastapi import APIRouter, Header, HTTPException, Request

from .. import config
from ..cal import queries
from ..cal.signature import verify_signature
from ..db import get_db_connection

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/webhooks", tags=["webhooks"])


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

    # The full envelope is stored verbatim in the payload jsonb (the source of truth).
    # These convenience columns are best-effort + null-safe — no payload shape is assumed.
    trigger_event = str(envelope.get("triggerEvent") or "UNKNOWN")
    inner = envelope.get("payload") if isinstance(envelope.get("payload"), dict) else {}
    organizer = inner.get("organizer") if isinstance(inner.get("organizer"), dict) else {}
    attendees = inner.get("attendees") if isinstance(inner.get("attendees"), list) else []
    event_type_id = inner.get("eventTypeId")

    async with get_db_connection() as conn:
        raw_id = await queries.insert_raw_event(
            conn,
            trigger_event=trigger_event,
            payload=envelope,
            cal_event_uid=inner.get("uid"),
            organizer_email=organizer.get("email"),
            attendee_emails=[a["email"] for a in attendees if isinstance(a, dict) and a.get("email")],
            event_type_id=event_type_id if isinstance(event_type_id, int) else None,
        )

    logger.info("cal webhook captured raw %s (%s)", raw_id, trigger_event)
    return {"ok": True, "raw_id": raw_id, "trigger_event": trigger_event}
