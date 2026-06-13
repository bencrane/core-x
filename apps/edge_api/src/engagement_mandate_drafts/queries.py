"""business.engagement_mandate_draft_content writes — psycopg async.

Append a draft stamping (opportunity_id, documenso_template_id). The org is resolved from the
Documenso template; the INSERT...SELECT also validates the template exists (no row → no insert).
"""
from __future__ import annotations

from uuid import UUID


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
