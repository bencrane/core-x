"""business.close_webhook_events — RAW Close.com webhook capture (append-only, psycopg async)
plus the OFFLINE "now dialing" derivation the Insights tab polls.

The full webhook body is stored verbatim in ``payload`` (the system of record); scalar columns
are best-effort lookup extracts. The current call is DERIVED at read time (no projection table),
mirroring documenso ``read_sign_state``.
"""
from __future__ import annotations

import json
from typing import Any


async def insert_event(
    conn,
    *,
    event_id: str | None,
    object_type: str | None,
    action: str | None,
    close_user_id: str | None,
    close_lead_id: str | None,
    close_contact_id: str | None,
    direction: str | None,
    status: str | None,
    remote_phone: str | None,
    payload: Any,
) -> str:
    """Append one raw Close webhook delivery. Returns the row id. COMMITS."""
    async with conn.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO business.close_webhook_events
                (event_id, object_type, action, close_user_id, close_lead_id,
                 close_contact_id, direction, status, remote_phone, payload)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            RETURNING id::text
            """,
            (event_id, object_type, action, close_user_id, close_lead_id,
             close_contact_id, direction, status, remote_phone, json.dumps(payload)),
        )
        row = await cur.fetchone()
    await conn.commit()
    return row[0]


# A call event older than this is treated as stale — the dialer has moved on / the session ended,
# so the Insights tab clears rather than showing a ghost briefing.
_ACTIVE_WINDOW_SECONDS = 180


async def read_active_call(conn) -> dict[str, Any]:
    """The CURRENT outbound Close call, DERIVED at read time — FULLY OFFLINE, NOT operator-scoped.

    Single-operator reality ("all users are me"): the latest RESOLVING outbound ``activity.call``
    inside the active window IS the current call, whatever platform account is viewing it —
    mirroring Documenso ``/sign-state`` (keyed by content, not operator).

    RESOLUTION is on the DIALED NUMBER first: Close does NOT reliably put lead_id/contact_id on call
    webhooks (and "personal number" bridge legs carry the operator's own number with no lead at all).
    So we match the event's ``remote_phone`` (digits) to ``close_crosswalk.contact_phone`` — the exact
    number we pushed to the contact, i.e. what Close dials — falling back to close_contact_id /
    close_lead_id when those ARE present (softphone calls). The crosswalk match is REQUIRED (INNER
    LATERAL), so unresolved bridge legs are skipped and only a real prospect call surfaces. Returns
    ``{active: false}`` when nothing in the window resolves.
    """
    async with conn.cursor() as cur:
        await cur.execute(
            """
            WITH resolved AS (
                SELECT e.close_lead_id, e.close_contact_id, e.status, e.remote_phone, e.received_at,
                       x.normalized_domain, x.company_name, x.resolved_contact_id
                FROM business.close_webhook_events e
                JOIN LATERAL (
                    SELECT normalized_domain, company_name, resolved_contact_id
                    FROM public.close_crosswalk x
                    WHERE (x.contact_phone IS NOT NULL
                           AND x.contact_phone = regexp_replace(COALESCE(e.remote_phone, ''), '[^0-9]', '', 'g'))
                       OR (e.close_contact_id IS NOT NULL AND x.close_contact_id = e.close_contact_id)
                       OR (e.close_lead_id IS NOT NULL AND x.close_lead_id = e.close_lead_id)
                    LIMIT 1
                ) x ON TRUE
                WHERE e.object_type = 'activity.call'
                  AND e.direction = 'outbound'
                  AND e.received_at > now() - make_interval(secs => %(window)s)
                ORDER BY e.received_at DESC
                LIMIT 1
            )
            SELECT close_lead_id, close_contact_id, status, remote_phone, received_at,
                   normalized_domain, company_name, resolved_contact_id
            FROM resolved
            """,
            {"window": _ACTIVE_WINDOW_SECONDS},
        )
        row = await cur.fetchone()
    if row is None:
        return {"active": False}
    return {
        "active": True,
        "close_lead_id": row[0],
        "close_contact_id": row[1],
        "status": row[2],
        "remote_phone": row[3],
        "started_at": row[4].isoformat() if row[4] else None,
        "normalized_domain": row[5],
        "company_name": row[6],
        "resolved_contact_id": row[7],
    }
