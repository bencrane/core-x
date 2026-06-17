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
