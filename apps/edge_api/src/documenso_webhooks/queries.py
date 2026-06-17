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


async def read_envelope_state(conn, envelope_ids: list[str]) -> dict[str, Any]:
    """Project signing state for an envelope AT READ TIME from the raw webhook rows — no projection
    table, no live Documenso.

    ``envelope_ids`` is the set of candidate handles to match against the ``envelope_id`` column. The
    column stores the value the webhook extracted from the payload, which is Documenso's NUMERIC
    document id (e.g. ``"1462137"``) — NOT the prefixed ``envelope_…`` v2 handle the prospect link
    carries. The caller passes every form it can resolve (the numeric id translated from the handle,
    plus the raw handle itself as a defensive fallback) so this match works regardless of which shape
    a given delivery landed under.

    Returns ``{signed, latest_event, status, received_at}``:
      * ``signed``       — a terminal (DOCUMENT_COMPLETED) row has landed for this envelope.
      * ``latest_event`` — the most recent event name seen (by received_at), or None if no rows.
      * ``status``       — the ``payload->payload->>status`` of the latest row (PENDING/COMPLETED/…),
                           the envelope-level Documenso status carried verbatim in the raw body.
    """
    ids = [i for i in {*envelope_ids} if i]
    if not ids:
        return {"signed": False, "latest_event": None, "status": None, "received_at": None}
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT
              bool_or(event = ANY(%(terminal)s))                         AS signed,
              (array_agg(event ORDER BY received_at DESC))[1]            AS latest_event,
              (array_agg(payload->'payload'->>'status' ORDER BY received_at DESC))[1] AS status,
              max(received_at)                                           AS received_at
            FROM business.documenso_webhook_events
            WHERE envelope_id = ANY(%(ids)s)
            """,
            {"terminal": list(_TERMINAL_EVENTS), "ids": ids},
        )
        row = await cur.fetchone()
    if row is None or row[3] is None:
        return {"signed": False, "latest_event": None, "status": None, "received_at": None}
    return {
        "signed": bool(row[0]),
        "latest_event": row[1],
        "status": row[2],
        "received_at": row[3].isoformat() if row[3] else None,
    }
