"""business.engagement_mandate_draft_content writes — psycopg async.

Append a draft stamping (opportunity_id, documenso_template_id). The org is resolved from the
Documenso template; the INSERT...SELECT also validates the template exists (no row → no insert).

The draft is OUR concept; it carries a POINTER (in ``metadata.documenso_envelope_id``) into the
verbatim Documenso mirror (``business.documenso_envelopes``). The draft never duplicates Documenso's
signed-document data — it only references which envelope it spawned and tracks its own lifecycle.
"""
from __future__ import annotations

from uuid import UUID

from psycopg.types.json import Jsonb


async def insert_draft(conn, *, opportunity_id: str, documenso_template_id: str) -> str | None:
    """Insert a draft stamp; returns its id, or None when the documenso template id is unknown."""
    async with conn.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO business.engagement_mandate_draft_content
                 (opportunity_id, documenso_template_id, organization_id)
            SELECT %s::uuid, dt.documenso_template_id, dt.organization_id
              FROM business.documenso_templates dt
             WHERE dt.documenso_template_id = %s
            RETURNING id::text
            """,
            (opportunity_id, documenso_template_id),
        )
        row = await cur.fetchone()
    await conn.commit()
    return row[0] if row else None


async def get_draft(conn, draft_id: str) -> dict | None:
    """Resolve a draft to what confirm needs — its Documenso template id (+ org / opportunity for
    context). Returns None when the id is unknown OR not a well-formed UUID.

    The id is validated in Python BEFORE the query: ``business.engagement_mandate_draft_content.id``
    is ``uuid``, so a truncated/garbage ref (e.g. a stale ``slice(0,8)`` link like ``ab76cee5``) would
    otherwise make ``%s::uuid`` raise ``InvalidTextRepresentation`` — a raw 500 that also leaves the
    pooled connection in an aborted transaction. Guarding here turns it into a clean 404."""
    try:
        UUID(str(draft_id))
    except (ValueError, TypeError, AttributeError):
        return None
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT documenso_template_id, organization_id::text, opportunity_id::text
              FROM business.engagement_mandate_draft_content
             WHERE id = %s::uuid
            """,
            (draft_id,),
        )
        row = await cur.fetchone()
    if not row:
        return None
    return {
        "documenso_template_id": row[0],
        "organization_id": row[1],
        "opportunity_id": row[2],
    }


async def attach_envelope(conn, *, draft_id: str, envelope_id: str, signing_token: str | None) -> None:
    """At confirm: point the draft at the Documenso envelope it just spawned and mark it sent. The
    pointer (``metadata.documenso_envelope_id``) is how OUR draft resolves its row in the verbatim
    Documenso mirror; the signed-document data itself lives only in the mirror."""
    async with conn.cursor() as cur:
        await cur.execute(
            """
            UPDATE business.engagement_mandate_draft_content
               SET status = 'sent',
                   metadata = metadata || %s::jsonb,
                   updated_at = now()
             WHERE id = %s::uuid
            """,
            (Jsonb({"documenso_envelope_id": envelope_id, "documenso_signing_token": signing_token}), draft_id),
        )
    await conn.commit()


async def advance_draft_status(conn, *, draft_id: str, status: str, envelope_id: str | None = None) -> bool:
    """Webhook domain reaction: advance the draft's own lifecycle (our vocabulary —
    sent/opened/signed/completed/rejected/voided, already mapped from the Documenso event) and keep the
    envelope pointer current. The signed data is NOT copied here — it is read from the mirror by
    ``documenso_envelope_id``. Returns True when a draft row matched."""
    patch = {"documenso_envelope_id": envelope_id} if envelope_id else {}
    async with conn.cursor() as cur:
        await cur.execute(
            """
            UPDATE business.engagement_mandate_draft_content
               SET status = %s,
                   metadata = metadata || %s::jsonb,
                   updated_at = now()
             WHERE id = %s::uuid
            RETURNING id
            """,
            (status, Jsonb(patch), draft_id),
        )
        row = await cur.fetchone()
    await conn.commit()
    return row is not None
