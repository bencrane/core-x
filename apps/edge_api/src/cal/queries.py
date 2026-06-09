"""cal.com raw landing — psycopg async, append-only.

ONE job for now: persist the verbatim cal.com webhook envelope to
``public.cal_raw_events`` (the append-only raw SoR) and commit. Normalization into
``corex.bookings`` is a SEPARATE, later step wired against the real captured payload
shape — not modeled here.
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
    the convenience columns are best-effort and null-safe (no shape is required)."""
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
