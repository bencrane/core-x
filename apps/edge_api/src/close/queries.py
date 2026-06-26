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


async def read_active_call(conn, *, auth_user_id: str) -> dict[str, Any]:
    """The operator's CURRENT outbound call, DERIVED at read time — FULLY OFFLINE (no Close call).

    Resolves the operator (auth_user_id) → their Close user via business.close_operator_map, takes
    the most recent outbound ``activity.call`` event inside the active window, and joins
    public.close_crosswalk (by contact, falling back to lead) to surface the briefing anchor
    (normalized_domain) + company/contact identity. Returns ``{active: false}`` when idle.
    """
    async with conn.cursor() as cur:
        await cur.execute(
            """
            WITH op AS (
                SELECT close_user_id FROM business.close_operator_map WHERE auth_user_id = %(auth)s
            ),
            latest AS (
                SELECT e.*
                FROM business.close_webhook_events e
                JOIN op ON op.close_user_id = e.close_user_id
                WHERE e.object_type = 'activity.call'
                  AND e.direction = 'outbound'
                  AND e.received_at > now() - make_interval(secs => %(window)s)
                ORDER BY e.received_at DESC
                LIMIT 1
            )
            SELECT
                l.close_lead_id, l.close_contact_id, l.status, l.remote_phone, l.received_at,
                COALESCE(xc.normalized_domain, xl.normalized_domain)  AS normalized_domain,
                COALESCE(xc.company_name,      xl.company_name)       AS company_name,
                COALESCE(xc.resolved_contact_id, xl.resolved_contact_id) AS resolved_contact_id
            FROM latest l
            LEFT JOIN public.close_crosswalk xc ON xc.close_contact_id = l.close_contact_id
            LEFT JOIN LATERAL (
                SELECT normalized_domain, company_name, resolved_contact_id
                FROM public.close_crosswalk WHERE close_lead_id = l.close_lead_id LIMIT 1
            ) xl ON TRUE
            """,
            {"auth": auth_user_id, "window": _ACTIVE_WINDOW_SECONDS},
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
