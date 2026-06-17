"""business.documenso_webhook_events — RAW Documenso webhook capture (append-only, psycopg async).

The full webhook body is stored verbatim in ``payload`` (the system of record). The scalar columns
are best-effort lookup extracts only — never re-derive truth from them, re-read ``payload``.
"""
from __future__ import annotations

import json
from typing import Any


async def insert_event(
    conn,
    *,
    event: str | None,
    envelope_id: str | None,
    external_id: str | None,
    payload: Any,
) -> str:
    """Append one raw webhook delivery. Returns the row id. COMMITS."""
    async with conn.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO business.documenso_webhook_events (event, envelope_id, external_id, payload)
            VALUES (%s, %s, %s, %s::jsonb)
            RETURNING id::text
            """,
            (event, envelope_id, external_id, json.dumps(payload)),
        )
        row = await cur.fetchone()
    await conn.commit()
    return row[0]


# Documenso webhook event names that mean "the document is fully executed" — the terminal,
# all-signers-done state. For the single-signer engagement flow, DOCUMENT_COMPLETED IS the done
# signal. Verified against REAL landed events (business.documenso_webhook_events, 2026-06-17):
# the webhook stores the event verbatim as UPPERCASE_UNDERSCORE (DOCUMENT_SENT / DOCUMENT_OPENED /
# DOCUMENT_SIGNED / DOCUMENT_COMPLETED) — NOT the lowercase-dotted form. Truth derives from the
# presence of a terminal-event ROW, never from a projection.
_TERMINAL_EVENTS = ("DOCUMENT_COMPLETED",)


async def read_sign_state(conn, *, opportunity_id: str, document_id: str) -> dict[str, Any]:
    """Project signing state for an ``(opportunity_id, document_id)`` PAIR at read time from the raw
    webhook rows — FULLY OFFLINE (no projection table, no live Documenso call).

    Security model — the pair MUST be valid (the document belongs to the opportunity), so a guessed
    numeric ``document_id`` with a wrong/missing opportunity handle returns nothing:
      * ``external_id`` (the captured column) = the opportunity's public 8-char handle stamped on the
        envelope at originate (the access capability — 8 hex = 32 bits).
      * ``envelope_id`` (the captured column) = Documenso's NUMERIC document id (e.g. ``"1462137"``),
        the same value the webhook payload carries as ``payload.id`` and the SPA link carries as the
        ``document_id`` segment.
    Verified against REAL landed rows 2026-06-17: the pair
    (``external_id='7bbf1081-…', envelope_id='1462137'``) carries a ``DOCUMENT_COMPLETED`` row.

    Returns ``{signed, latest_event, status, received_at}``:
      * ``signed``       — a terminal (DOCUMENT_COMPLETED) row has landed for this pair.
      * ``latest_event`` — the most recent event name seen (by received_at), or None if no rows.
      * ``status``       — the ``payload->payload->>status`` of the latest row (PENDING/COMPLETED/…),
                           the envelope-level Documenso status carried verbatim in the raw body.
    """
    empty = {"signed": False, "latest_event": None, "status": None, "received_at": None}
    if not opportunity_id or not document_id:
        return empty
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT
              bool_or(event = ANY(%(terminal)s))                         AS signed,
              (array_agg(event ORDER BY received_at DESC))[1]            AS latest_event,
              (array_agg(payload->'payload'->>'status' ORDER BY received_at DESC))[1] AS status,
              max(received_at)                                           AS received_at
            FROM business.documenso_webhook_events
            WHERE external_id = %(opportunity_id)s
              AND envelope_id = %(document_id)s
            """,
            {
                "terminal": list(_TERMINAL_EVENTS),
                "opportunity_id": opportunity_id,
                "document_id": document_id,
            },
        )
        row = await cur.fetchone()
    if row is None or row[3] is None:
        return empty
    return {
        "signed": bool(row[0]),
        "latest_event": row[1],
        "status": row[2],
        "received_at": row[3].isoformat() if row[3] else None,
    }
