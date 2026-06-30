"""business.documenso_template_defaults access — the operator-owned MIRROR-template default store.

Records which business.documenso_envelopes template is the operator's Confirm & Originate default.
OPERATOR/app-owned — the projector / on-demand re-grab NEVER touch this (same boundary as
business.documenso_template_document_prefill_configs). The default cannot live on the mirror
(projector-owned, verbatim) nor on the legacy business.documenso_templates registry (mirror-path
templates aren't in it), so it lives here keyed by the mirror's numeric documenso_id.

``list_templates_with_default`` LEFT JOINs the verbatim mirror (it READS the mirror; never writes it)
with this store to surface the picker rows. ``set_default`` clear-then-sets the single default,
validating the target is a live, non-deleted mirror TEMPLATE first.
"""
from __future__ import annotations

from typing import Any

from psycopg.rows import dict_row


async def list_templates_with_default(conn) -> list[dict[str, Any]]:
    """The mirrored TEMPLATE envelopes (non-deleted), newest sync first, each flagged with whether it
    is the operator's default.

    Read-only: reads business.documenso_envelopes (the verbatim mirror) LEFT JOIN the operator-owned
    business.documenso_template_defaults — writes nothing. ``is_default`` is true for at most one row
    (the one-default partial unique index guarantees it).
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT
                e.documenso_id,
                e.title,
                e.status,
                COALESCE(d.is_default, false) AS is_default
            FROM business.documenso_envelopes e
            LEFT JOIN business.documenso_template_defaults d
                   ON d.documenso_id = e.documenso_id AND d.is_default
            WHERE e.type = 'template' AND e.deleted_at IS NULL
            ORDER BY e.synced_at DESC
            """
        )
        return await cur.fetchall()


async def set_default(conn, documenso_id: int) -> bool:
    """Mark ``documenso_id`` as the operator's single Confirm & Originate default for MIRROR templates.

    Validates the target is a LIVE, non-deleted TEMPLATE in business.documenso_envelopes, then CLEARS
    the current default and SETS the new one inside a transaction. Clear-then-set (two statements) keeps
    the partial unique index ``documenso_template_defaults_one_default_uidx`` from being transiently
    violated. Returns False when the target is unknown / deleted / not a template (caller → 404).
    COMMITS.
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT 1
              FROM business.documenso_envelopes
             WHERE documenso_id = %(id)s AND type = 'template' AND deleted_at IS NULL
            """,
            {"id": documenso_id},
        )
        if not await cur.fetchone():
            return False
        async with conn.transaction():
            await cur.execute(
                """
                UPDATE business.documenso_template_defaults
                   SET is_default = false, updated_at = now()
                 WHERE is_default
                """
            )
            await cur.execute(
                """
                INSERT INTO business.documenso_template_defaults (documenso_id, is_default)
                VALUES (%(id)s, true)
                ON CONFLICT (documenso_id)
                DO UPDATE SET is_default = true, updated_at = now()
                """,
                {"id": documenso_id},
            )
    return True
