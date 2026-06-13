"""business.documenso_envelopes — the verbatim Documenso mirror (system of record for "what was
signed"). Concept-agnostic: this module knows only Documenso's shape, nothing of our proposals or
mandate drafts.
"""
from __future__ import annotations

from typing import Any

from psycopg.types.json import Jsonb


async def upsert_envelope(conn, *, snapshot: dict[str, Any], event: Any) -> None:
    """Upsert one Documenso envelope by its id and append the VERBATIM webhook event to ``events``.

    ``snapshot`` is :func:`documenso_client.envelope_snapshot` output (Documenso's keys, verbatim).
    The scalar/structural columns are overwritten with the latest snapshot; ``events`` is append-only
    (the immutable lifecycle stream). Idempotent on the envelope id.
    """
    s = snapshot
    async with conn.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO business.documenso_envelopes
                (envelope_id, secondary_id, external_id, status, title, type,
                 recipients, fields, raw, events)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (envelope_id) DO UPDATE SET
                secondary_id = COALESCE(EXCLUDED.secondary_id, business.documenso_envelopes.secondary_id),
                external_id  = COALESCE(EXCLUDED.external_id,  business.documenso_envelopes.external_id),
                status       = EXCLUDED.status,
                title        = COALESCE(EXCLUDED.title, business.documenso_envelopes.title),
                type         = COALESCE(EXCLUDED.type,  business.documenso_envelopes.type),
                recipients   = EXCLUDED.recipients,
                fields       = EXCLUDED.fields,
                raw          = EXCLUDED.raw,
                events       = business.documenso_envelopes.events || EXCLUDED.events,
                updated_at   = now()
            """,
            (
                s["envelope_id"],
                s.get("secondary_id"),
                s.get("external_id"),
                s.get("status") or "UNKNOWN",
                s.get("title"),
                s.get("type"),
                Jsonb(s.get("recipients") or []),
                Jsonb(s.get("fields") or []),
                Jsonb(s.get("raw") or {}),
                Jsonb([event] if event is not None else []),
            ),
        )
    await conn.commit()
