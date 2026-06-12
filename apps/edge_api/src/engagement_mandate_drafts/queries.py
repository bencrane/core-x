"""business.engagement_mandate_draft_content writes — psycopg async.

Append a draft stamping (opportunity_id, documenso_template_id). The org is resolved from the
Documenso template; the INSERT...SELECT also validates the template exists (no row → no insert).
"""
from __future__ import annotations


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
    context). Returns None when the id is unknown or malformed (the cast guards against junk ids)."""
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
