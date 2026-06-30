"""cal.com webhook — raw capture (always) + booking normalization.

  POST /webhooks/cal   X-Cal-Signature-256 — cal.com booking lifecycle events

1. Verify the HMAC signature against the RAW body (401 on mismatch; 503 if the secret
   is unconfigured — never accept unverified).
2. Land the VERBATIM envelope in ``public.cal_raw_events`` (COMMITTED — guaranteed).
3. Best-effort normalize into ``corex.bookings`` (anchored on the stable ``iCalUID``):
     • BOOKING_CREATED / BOOKING_RESCHEDULED → idempotent upsert on ``ical_uid``. A
       reschedule advances the SAME row's live ``uid``/``bookingId`` + time (both roll on
       reschedule; ``iCalUID`` does not), guarded by monotonic ``bookingId`` so a stale /
       out-of-order delivery cannot regress it.
     • BOOKING_CANCELLED → status='cancelled' by ``ical_uid``.
     • everything else → raw-only.
   A normalization failure NEVER fails the webhook — the raw row is already durable and
   carries ``processed`` / ``processed_by`` for reprocessing.
4. On a NEW booking, best-effort fire the Parallel deep-research Trigger.dev task for the
   prospect's company (cheapest processor) and stamp the run id onto the booking — idempotent
   on iCalUID, so cal's duplicate deliveries resolve to ONE research run.

Mounted at ``/webhooks/cal`` (NOT under ``/api/v1``) — the path cal.com posts to.
"""
from __future__ import annotations

import json
import logging
from typing import Any

from fastapi import APIRouter, Header, HTTPException, Request

from .. import config
from ..cal import enrich, materialize, normalize, queries, research
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

    # 3) RESEARCH + ENRICH KICKOFF — only for a newly created booking (committed above). Both
    # best-effort; neither affects the webhook outcome (booking + raw are already durable).
    research_result = None
    enrich_result = None
    materialize_result = None
    if isinstance(normalized, dict) and normalized.get("action") == "created":
        ical_uid = normalized.get("ical_uid")
        research_result = await _kick_research(trigger_event, envelope, ical_uid)
        enrich_result = await _kick_enrich(trigger_event, envelope, ical_uid)
        materialize_result = await _kick_materialize(ical_uid)

    return {
        "ok": True,
        "raw_id": raw_id,
        "trigger_event": trigger_event,
        "normalized": normalized,
        "research": research_result,
        "enrich": enrich_result,
        "materialize": materialize_result,
    }


async def _kick_research(
    trigger_event: str, envelope: dict[str, Any], ical_uid: str | None
) -> dict[str, Any]:
    """Fire the company deep-research task for a new booking and stamp its run id. Best-effort —
    a missing domain or a trigger failure never affects the webhook."""
    if not ical_uid:
        return {"triggered": False, "reason": "no ical_uid"}
    fields = normalize.extract(trigger_event, envelope)
    domain = fields.get("domain")
    if not domain:
        return {"triggered": False, "reason": "no domain"}
    # The ACTIVE registry prompt is the source of truth; None → the built-in fallback.
    prompt_id: str | None = None
    template: str | None = None
    try:
        async with get_db_connection() as conn:
            active = await queries.get_active_research_prompt(conn, research.RESEARCH_PROMPT_SLUG)
        if active:
            prompt_id, template = active
    except Exception as exc:  # noqa: BLE001 — fall back to the built-in template
        logger.warning("active research prompt lookup failed (%s); using fallback", exc)
    run_id = await research.trigger_research(
        ical_uid=ical_uid, company_name=fields.get("company_name"), domain=domain, template=template
    )
    if not run_id:
        return {"triggered": False, "reason": "trigger returned no run id"}
    try:
        async with get_db_connection() as conn:
            await queries.set_research_refs(conn, ical_uid, run_id, prompt_id)
            await conn.commit()
    except Exception as exc:  # noqa: BLE001 — the run is created; stamping is best-effort
        logger.warning("research refs stamp failed for %s: %s", ical_uid, exc)
    return {"triggered": True, "run_id": run_id, "prompt_id": prompt_id}


async def _kick_enrich(
    trigger_event: str, envelope: dict[str, Any], ical_uid: str | None
) -> dict[str, Any]:
    """Fire the company enrichment task for a new booking and stamp its run id. Best-effort —
    a missing domain or a trigger failure never affects the webhook. Sibling of _kick_research;
    the two fire independently so one failing never blocks the other."""
    if not ical_uid:
        return {"triggered": False, "reason": "no ical_uid"}
    fields = normalize.extract(trigger_event, envelope)
    domain = fields.get("domain")
    if not domain:
        return {"triggered": False, "reason": "no domain"}
    run_id = await enrich.trigger_enrich(
        ical_uid=ical_uid, company_name=fields.get("company_name"), domain=domain
    )
    if not run_id:
        return {"triggered": False, "reason": "trigger returned no run id"}
    try:
        async with get_db_connection() as conn:
            await queries.set_enrich_ref(conn, ical_uid, run_id)
            await conn.commit()
    except Exception as exc:  # noqa: BLE001 — the run is created; stamping is best-effort
        logger.warning("enrich ref stamp failed for %s: %s", ical_uid, exc)
    return {"triggered": True, "run_id": run_id}


async def _kick_materialize(ical_uid: str | None) -> dict[str, Any]:
    """Fire the booking → CRM deal materialization task for a new booking. Best-effort —
    a trigger failure never affects the webhook (booking + raw are already durable). Sibling of
    _kick_research / _kick_enrich; fires independently so one failing never blocks the others."""
    if not ical_uid:
        return {"triggered": False, "reason": "no ical_uid"}
    run_id = await materialize.trigger_materialize(ical_uid=ical_uid)
    if not run_id:
        return {"triggered": False, "reason": "trigger returned no run id"}
    return {"triggered": True, "run_id": run_id}


async def _normalize(conn, trigger_event: str, envelope: dict[str, Any], raw_id: str) -> dict[str, Any]:
    """Drain one raw event into corex.bookings, anchored on the stable iCalUID. Caller owns
    the commit/rollback."""
    kind = normalize.event_kind(trigger_event)

    # created + rescheduled are ONE path: upsert on ical_uid. A reschedule advances the same
    # row's live identifiers + time (uid/bookingId roll; ical_uid does not).
    if kind in ("created", "rescheduled"):
        fields = normalize.extract(trigger_event, envelope)
        ical_uid = fields.get("ical_uid")
        if not ical_uid:
            return {"action": "skipped_no_ical_uid"}
        await queries.upsert_booking(conn, fields=fields, source_raw_event_id=raw_id)
        await queries.mark_raw_processed(conn, raw_id, _PROCESSED_BY)
        return {"action": kind, "ical_uid": ical_uid, "cal_event_uid": fields.get("cal_event_uid")}

    if kind == "cancelled":
        ical_uid = (envelope.get("payload") or {}).get("iCalUID")
        if not ical_uid:
            return {"action": "skipped_no_ical_uid"}
        matched = await queries.cancel_booking(conn, ical_uid)
        await queries.mark_raw_processed(conn, raw_id, _PROCESSED_BY)
        return {"action": "cancelled", "ical_uid": ical_uid, "matched": matched}

    return {"action": "raw_only", "kind": kind}
