"""Internal cal.com booking — Trigger.dev-facing (OUTBOUND create).

  POST /internal/cal/book   trigger-secret — Close custom activity → cal.com booking

Called by the ``cal-book`` Trigger.dev task (one run per completed Close booking activity) via
``callHqx``. Gated by ``require_trigger_secret`` (TRIGGER_SHARED_SECRET) — the same ``/internal/*``
contract the deal-materialize + engagement-template-push tasks use.

Idempotent + double-book-safe: the ``ops.cal_booking_runs`` ledger is CLAIMED on
``close_activity_id`` BEFORE the cal.com call (INSERT … ON CONFLICT DO NOTHING). A duplicate delivery
/ re-fire that finds an existing row does NOT create a second booking — it returns the prior state.
The created booking fires cal.com's ``BOOKING_CREATED`` webhook back into ``/webhooks/cal``, where the
existing pipeline takes over (no write-back from here).
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, ConfigDict, Field

from .. import config
from ..cal import book, booking_runs
from ..cal.book import CalBookingError
from ..close import client as close_client
from ..close.client import CloseApiError
from ..db import get_db_connection
from ..trigger_secret import require_trigger_secret

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/cal", tags=["internal"])


class BookRequest(BaseModel):
    # The Trigger task sends camelCase; accept that and the snake_case form.
    model_config = ConfigDict(populate_by_name=True)
    close_activity_id: str = Field(alias="closeActivityId")
    close_lead_id: str | None = Field(default=None, alias="closeLeadId")
    close_contact_id: str | None = Field(default=None, alias="closeContactId")
    custom_activity_type_id: str | None = Field(default=None, alias="customActivityTypeId")


class BookResponse(BaseModel):
    status: str  # success | already_booked | skipped | error
    cal_booking_uid: str | None = None
    cal_booking_id: str | None = None
    ical_uid: str | None = None
    event_type_slug: str | None = None
    reason: str | None = None


@router.post("/book", dependencies=[Depends(require_trigger_secret)])
async def book_calendar(body: BookRequest) -> BookResponse:
    close_activity_id = body.close_activity_id.strip()
    if not close_activity_id:
        raise HTTPException(status_code=400, detail="closeActivityId is required")

    # 1) Idempotency claim — durable BEFORE any cal.com call. A second delivery / re-fire that finds
    #    the row skips (no double-book) and reports the prior state.
    async with get_db_connection() as conn:
        claimed = await booking_runs.claim_run(
            conn,
            close_activity_id=close_activity_id,
            close_lead_id=body.close_lead_id,
            close_contact_id=body.close_contact_id,
        )
        await conn.commit()
        if not claimed:
            existing = await booking_runs.get_run(conn, close_activity_id) or {}
            if existing.get("status") == "success":
                return BookResponse(
                    status="already_booked",
                    cal_booking_uid=existing.get("cal_booking_uid"),
                    cal_booking_id=existing.get("cal_booking_id"),
                    ical_uid=existing.get("ical_uid"),
                    event_type_slug=existing.get("event_type_slug"),
                )
            logger.warning(
                "cal-book skipped activity %s — prior run status=%s (not re-booking)",
                close_activity_id, existing.get("status"),
            )
            return BookResponse(status="skipped", reason=f"prior run status={existing.get('status')}")

    # 2) Resolve + create. Any failure stamps the ledger error and surfaces (the task is maxAttempts=1,
    #    so there is no blind retry that could double-book).
    try:
        if config.cal_api_key() is None:
            raise CalBookingError("CAL_API_KEY not configured")
        if config.close_api_key() is None:
            raise CalBookingError("CLOSE_API_KEY not configured")

        activity = await close_client.get_custom_activity(close_activity_id)
        # Fetch the lead + contact: the contact is the booking attendee (prefer the explicit
        # contact_id — i.e. log the activity ON the contact — else the lead's first contact), and the
        # lead supplies booking fields the rep should not re-type (30min's Company-Name/Website).
        lead = await close_client.get_lead(body.close_lead_id) if body.close_lead_id else {}
        contact: dict = {}
        if body.close_contact_id:
            contact = await close_client.get_contact(body.close_contact_id)
        elif isinstance(lead, dict) and lead.get("contacts"):
            contact = lead["contacts"][0]
        lead = lead if isinstance(lead, dict) else {}
        contact = contact if isinstance(contact, dict) else {}

        parts = book.build_booking_request(
            activity=activity,
            lead=lead,
            contact=contact,
            fallback_name=contact.get("name"),
            fallback_email=close_client.primary_email(contact),
            field_map=config.close_booking_field_map(),
            event_type_id_map=config.cal_event_type_id_map(),
            default_tz=config.cal_default_timezone(),
        )
        created = await book.create_booking(
            event_type_id=parts["event_type_id"],
            start=parts["start"],
            attendee=parts["attendee"],
            booking_fields_responses=parts["booking_fields_responses"],
        )
    except (CalBookingError, CloseApiError) as exc:
        # Stamp the ledger error best-effort — a ledger-write failure must NOT mask the original booking
        # failure (the 502 below is the signal the maxAttempts=1 task surfaces).
        try:
            async with get_db_connection() as conn:
                await booking_runs.mark_error(
                    conn, close_activity_id=close_activity_id, error=str(exc)[:500]
                )
                await conn.commit()
        except Exception as ledger_exc:  # noqa: BLE001 — keep the original booking error as the cause
            logger.warning(
                "cal-book ledger mark_error failed for activity %s: %s", close_activity_id, ledger_exc
            )
        logger.warning("cal-book failed for activity %s: %s", close_activity_id, exc)
        raise HTTPException(status_code=502, detail=f"cal booking failed: {exc}") from exc

    cal_booking_id = str(created["id"]) if created.get("id") is not None else None
    cal_booking_uid = created.get("uid")
    # Best-effort convenience link to corex.bookings; the INBOUND BOOKING_CREATED webhook is the
    # authoritative source of iCalUID, so None here is fine (no silent field-name fallback).
    ical_uid = created.get("iCalUID")

    async with get_db_connection() as conn:
        await booking_runs.mark_success(
            conn,
            close_activity_id=close_activity_id,
            cal_booking_id=cal_booking_id,
            cal_booking_uid=cal_booking_uid,
            ical_uid=ical_uid,
            event_type_slug=parts["event_type_slug"],
        )
        await conn.commit()

    logger.info(
        "cal-book ok: activity=%s slug=%s cal_uid=%s start=%s",
        close_activity_id, parts["event_type_slug"], cal_booking_uid, parts["start"],
    )
    return BookResponse(
        status="success",
        cal_booking_uid=cal_booking_uid,
        cal_booking_id=cal_booking_id,
        ical_uid=ical_uid,
        event_type_slug=parts["event_type_slug"],
    )
