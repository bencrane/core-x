"""cal.com raw landing + corex.bookings normalization — psycopg async.

Two boundaries, two transactions:
  • ``insert_raw_event`` COMMITS immediately — the raw payload survives regardless of
    what happens downstream.
  • ``upsert_booking`` / ``cancel_booking`` / ``mark_raw_processed`` do NOT commit; the
    caller wraps them in one normalize transaction so the booking row and the
    processed flag move atomically (and roll back together on any error).

The booking anchor is ``ical_uid`` (cal.com's ``iCalUID``) — STABLE across the entire
reschedule chain. ``cal_event_uid`` / ``cal_booking_id`` track the CURRENT live booking
and roll on each reschedule, so the upsert keys on ``ical_uid`` and advances the live
identifiers only when the incoming booking is the same-or-newer (monotonic ``bookingId``).
"""
from __future__ import annotations

from typing import Any

from psycopg.types.json import Jsonb


async def insert_raw_event(
    conn,
    *,
    trigger_event: str,
    payload: dict[str, Any],
    cal_event_uid: str | None,
    organizer_email: str | None,
    attendee_emails: list[str],
    event_type_id: int | None,
) -> str:
    """Append the verbatim cal.com envelope to ``public.cal_raw_events`` and COMMIT.
    Returns the new row id (text). The complete payload lives in the ``payload`` jsonb;
    the convenience columns are best-effort and null-safe."""
    async with conn.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO public.cal_raw_events
                (trigger_event, payload, cal_event_uid, organizer_email, attendee_emails, event_type_id)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id::text
            """,
            (
                trigger_event, Jsonb(payload), cal_event_uid, organizer_email,
                Jsonb(attendee_emails), event_type_id,
            ),
        )
        row = await cur.fetchone()
    await conn.commit()
    return row[0]


# Field keys produced by cal.normalize.extract that map 1:1 onto corex.bookings columns.
_BOOKING_FIELD_KEYS = (
    "ical_uid", "cal_event_uid", "cal_booking_id", "event_type_id", "first_name", "last_name",
    "email", "company_name", "domain", "title", "start_time", "end_time", "status", "booked_at",
)


async def upsert_booking(conn, *, fields: dict[str, Any], source_raw_event_id: str) -> None:
    """Idempotent upsert keyed on ``ical_uid`` — one row per logical meeting, covering both
    BOOKING_CREATED and BOOKING_RESCHEDULED.

    The live identifiers (``cal_event_uid``/``cal_booking_id``) and the time are OVERWRITTEN
    from the incoming event; the person/company enrichment is COALESCE'd (never wiped by an
    omitted field); ``booked_at`` keeps the original (first) booking time. The DO-UPDATE is
    guarded so a stale / out-of-order delivery (lower ``cal_booking_id``) cannot regress the
    row — cal.com's ``bookingId`` is monotonic, so newer always wins. Does NOT commit."""
    params: dict[str, Any] = {k: fields.get(k) for k in _BOOKING_FIELD_KEYS}
    params["source_raw_event_id"] = source_raw_event_id
    async with conn.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO corex.bookings AS b
                (ical_uid, cal_event_uid, cal_booking_id, event_type_id, first_name, last_name,
                 email, company_name, domain, title, start_time, end_time, status, booked_at,
                 source_raw_event_id)
            VALUES
                (%(ical_uid)s, %(cal_event_uid)s, %(cal_booking_id)s, %(event_type_id)s,
                 %(first_name)s, %(last_name)s, %(email)s, %(company_name)s, %(domain)s,
                 %(title)s, %(start_time)s, %(end_time)s, %(status)s, %(booked_at)s,
                 %(source_raw_event_id)s::uuid)
            ON CONFLICT (ical_uid) DO UPDATE SET
                cal_event_uid  = EXCLUDED.cal_event_uid,
                cal_booking_id = EXCLUDED.cal_booking_id,
                event_type_id  = COALESCE(EXCLUDED.event_type_id, b.event_type_id),
                first_name     = COALESCE(EXCLUDED.first_name, b.first_name),
                last_name      = COALESCE(EXCLUDED.last_name, b.last_name),
                email          = COALESCE(EXCLUDED.email, b.email),
                company_name   = COALESCE(EXCLUDED.company_name, b.company_name),
                domain         = COALESCE(EXCLUDED.domain, b.domain),
                title          = COALESCE(EXCLUDED.title, b.title),
                start_time     = EXCLUDED.start_time,
                end_time       = EXCLUDED.end_time,
                status         = EXCLUDED.status,
                booked_at      = COALESCE(b.booked_at, EXCLUDED.booked_at),
                source_raw_event_id = COALESCE(b.source_raw_event_id, EXCLUDED.source_raw_event_id),
                updated_at     = now()
            WHERE b.cal_booking_id IS NULL
               OR EXCLUDED.cal_booking_id IS NULL
               OR EXCLUDED.cal_booking_id >= b.cal_booking_id
            """,
            params,
        )


async def cancel_booking(conn, ical_uid: str) -> bool:
    """Mark a booking cancelled by its stable ``ical_uid``. Returns whether a row matched.
    Does NOT commit."""
    async with conn.cursor() as cur:
        await cur.execute(
            "UPDATE corex.bookings SET status='cancelled', updated_at=now() WHERE ical_uid=%s",
            (ical_uid,),
        )
        return cur.rowcount == 1


async def mark_raw_processed(conn, raw_id: str, by: str) -> None:
    """Flag the raw row drained (``processed=true``, ``processed_by=by``). Does NOT commit."""
    async with conn.cursor() as cur:
        await cur.execute(
            "UPDATE public.cal_raw_events SET processed=true, processed_by=%s WHERE id=%s::uuid",
            (by, raw_id),
        )
