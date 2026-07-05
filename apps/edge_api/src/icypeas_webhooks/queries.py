"""business.icypeas_webhook_events — RAW Icypeas webhook capture (append-only, psycopg async).

The full webhook body is stored verbatim in ``payload`` (the system of record). The scalar columns
are best-effort lookup extracts only — never re-derive truth from them, re-read ``payload``.
"""
from __future__ import annotations

import json
from typing import Any


async def insert_event(
    conn,
    *,
    kind: str | None,
    item_id: str | None,
    file_id: str | None,
    status: str | None,
    external_id: str | None,
    company_url: str | None,
    signature_ts: str | None,
    payload: Any,
) -> str:
    """Append one raw Icypeas webhook delivery. Returns the row id. COMMITS."""
    async with conn.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO business.icypeas_webhook_events
                (kind, item_id, file_id, status, external_id, company_url, signature_ts, payload)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            RETURNING id::text
            """,
            (kind, item_id, file_id, status, external_id, company_url, signature_ts, json.dumps(payload)),
        )
        row = await cur.fetchone()
    await conn.commit()
    return row[0]
