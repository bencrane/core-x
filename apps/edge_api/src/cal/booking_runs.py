"""ops.cal_booking_runs — the OUTBOUND cal.com booking ledger (idempotency + terminal state).

One row per Close custom activity that requested a booking, keyed UNIQUE on ``close_activity_id`` —
the double-book guard for the create path. psycopg async; the caller owns the commit. Distinct from
``cal/queries.py`` (the INBOUND corex.bookings normalization).
"""
from __future__ import annotations

from typing import Any


async def claim_run(
    conn,
    *,
    close_activity_id: str,
    close_lead_id: str | None = None,
    close_contact_id: str | None = None,
) -> bool:
    """Insert a ``pending`` run if absent. Returns True iff this call CLAIMED it (first time) — the
    caller proceeds to create the booking only when True; a False means a prior run exists (duplicate
    delivery / re-fire) and the cal.com create is skipped. Does NOT commit."""
    async with conn.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO ops.cal_booking_runs
                (close_activity_id, close_lead_id, close_contact_id, status)
            VALUES (%s, %s, %s, 'pending')
            ON CONFLICT (close_activity_id) DO NOTHING
            RETURNING id
            """,
            (close_activity_id, close_lead_id, close_contact_id),
        )
        row = await cur.fetchone()
    return row is not None


async def get_run(conn, close_activity_id: str) -> dict[str, Any] | None:
    """The current ledger state for an activity, or None when no row exists."""
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT status, event_type_slug, cal_booking_id, cal_booking_uid, ical_uid, error
            FROM ops.cal_booking_runs WHERE close_activity_id=%s
            """,
            (close_activity_id,),
        )
        row = await cur.fetchone()
    if row is None:
        return None
    return {
        "status": row[0],
        "event_type_slug": row[1],
        "cal_booking_id": row[2],
        "cal_booking_uid": row[3],
        "ical_uid": row[4],
        "error": row[5],
    }


async def mark_success(
    conn,
    *,
    close_activity_id: str,
    cal_booking_id: str | None,
    cal_booking_uid: str | None,
    ical_uid: str | None,
    event_type_slug: str | None,
) -> None:
    """Advance the run to ``success`` with the cal.com booking identifiers. Does NOT commit."""
    async with conn.cursor() as cur:
        await cur.execute(
            """
            UPDATE ops.cal_booking_runs
            SET status='success', cal_booking_id=%s, cal_booking_uid=%s, ical_uid=%s,
                event_type_slug=%s, error=NULL, updated_at=now()
            WHERE close_activity_id=%s
            """,
            (cal_booking_id, cal_booking_uid, ical_uid, event_type_slug, close_activity_id),
        )


async def mark_error(conn, *, close_activity_id: str, error: str) -> None:
    """Advance the run to ``error`` with the failure reason. Does NOT commit."""
    async with conn.cursor() as cur:
        await cur.execute(
            """
            UPDATE ops.cal_booking_runs
            SET status='error', error=%s, updated_at=now()
            WHERE close_activity_id=%s
            """,
            (error, close_activity_id),
        )
