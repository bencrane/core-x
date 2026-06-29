"""business.documenso_envelopes writes — the envelope-mirror upsert + soft-delete (psycopg async).

VERBATIM CONTRACT: ``documenso_response`` is the FULL get_envelope response stored EXACTLY as Documenso
returns it (Jsonb, no rewrite). ``type``/``status`` are lowercased-only projections of Documenso's own
terms — NEVER remapped. This module writes ONLY business.documenso_envelopes; it MUST NEVER touch
business.documenso_template_configs (operator/app-owned).
"""
from __future__ import annotations

from typing import Any

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb


async def upsert_envelope(
    conn,
    *,
    documenso_id: int,
    envelope_id: str,
    secondary_id: str | None,
    type_: str,
    template_documenso_id: int | None,
    external_id: str | None,
    title: str | None,
    status: str | None,
    documenso_response: dict[str, Any],
) -> None:
    """UPSERT one envelope-mirror row keyed on documenso_id. ``documenso_response`` is stored verbatim
    (Jsonb). ``type``/``status`` are already lowercased by the caller — written as-is. COMMITS.

    ``created_at`` is set on INSERT only (kept on conflict); ``synced_at``/``updated_at`` bump on every
    upsert. ``deleted_at`` is reset to NULL on upsert — a live event resurrects a previously
    soft-deleted row.
    """
    async with conn.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO business.documenso_envelopes
                (documenso_id, envelope_id, secondary_id, type, template_documenso_id,
                 external_id, title, status, documenso_response,
                 deleted_at, synced_at, created_at, updated_at)
            VALUES
                (%(documenso_id)s, %(envelope_id)s, %(secondary_id)s, %(type)s,
                 %(template_documenso_id)s, %(external_id)s, %(title)s, %(status)s,
                 %(documenso_response)s, NULL, now(), now(), now())
            ON CONFLICT (documenso_id) DO UPDATE SET
                envelope_id           = EXCLUDED.envelope_id,
                secondary_id          = EXCLUDED.secondary_id,
                type                  = EXCLUDED.type,
                template_documenso_id = EXCLUDED.template_documenso_id,
                external_id           = EXCLUDED.external_id,
                title                 = EXCLUDED.title,
                status                = EXCLUDED.status,
                documenso_response    = EXCLUDED.documenso_response,
                deleted_at            = NULL,
                synced_at             = now(),
                updated_at            = now()
            """,
            {
                "documenso_id": documenso_id,
                "envelope_id": envelope_id,
                "secondary_id": secondary_id,
                "type": type_,
                "template_documenso_id": template_documenso_id,
                "external_id": external_id,
                "title": title,
                "status": status,
                "documenso_response": Jsonb(documenso_response),
            },
        )
    await conn.commit()


async def soft_delete_envelope(
    conn, *, documenso_id: int, status: str | None
) -> int:
    """Soft-delete the mirror row matching ``documenso_id`` (a *_DELETED event). NO API pull. Sets
    ``deleted_at = now()`` and ``status`` (lowercased, verbatim) when provided. Returns the affected
    row count (0 = no such row, a no-op). COMMITS.
    """
    async with conn.cursor() as cur:
        await cur.execute(
            """
            UPDATE business.documenso_envelopes
               SET deleted_at = now(),
                   status     = COALESCE(%(status)s, status),
                   updated_at = now()
             WHERE documenso_id = %(documenso_id)s
            """,
            {"documenso_id": documenso_id, "status": status},
        )
        affected = cur.rowcount
    await conn.commit()
    return affected


async def list_template_mirror(conn) -> list[dict[str, Any]]:
    """The mirrored TEMPLATE envelopes (non-deleted), newest sync first — the LIST surface.

    Reads STRAIGHT off the verbatim mirror: ``documenso_id``/``title``/``status`` as stored, plus
    field/recipient counts derived from ``documenso_response`` (the full envelope), and ``synced_at``.
    Read-only; touches ONLY business.documenso_envelopes.
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT
                documenso_id,
                title,
                status,
                jsonb_array_length(COALESCE(documenso_response->'fields', '[]'::jsonb))     AS field_count,
                jsonb_array_length(COALESCE(documenso_response->'recipients', '[]'::jsonb)) AS recipient_count,
                synced_at
            FROM business.documenso_envelopes
            WHERE type = 'template' AND deleted_at IS NULL
            ORDER BY synced_at DESC
            """
        )
        return await cur.fetchall()


async def list_template_documenso_ids(conn) -> list[int]:
    """The ``documenso_id`` of every non-deleted TEMPLATE mirror row — the re-grab-all worklist."""
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT documenso_id
            FROM business.documenso_envelopes
            WHERE type = 'template' AND deleted_at IS NULL
            ORDER BY documenso_id
            """
        )
        return [int(r[0]) for r in await cur.fetchall()]
