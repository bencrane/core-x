"""business.engagement_mandate_draft_content writes — psycopg async.

Append a draft stamping (opportunity_id, documenso_template_id). The org is resolved from the
Documenso template; the INSERT...SELECT also validates the template exists (no row → no insert).
"""
from __future__ import annotations

import json
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
            SELECT documenso_template_id, organization_id::text, opportunity_id::text, prefill_values
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
        # Operator-entered per-deal values, keyed by Documenso field LABEL (e.g.
        # {"Engagement Fee": "$35,000"}). Confirm passes these through as prefillFields.
        "prefill_values": row[3] or {},
    }


async def get_staged_prefill_values(conn, opportunity_id: str) -> dict:
    """The opportunity's STAGED per-deal values — the latest draft for this opportunity that carries
    NON-EMPTY ``prefill_values`` (the Stage-mandate save). This is the source Confirm & Originate
    pulls from, so values flow from the staging row, NOT the fresh empty draft an "Originate Mandate"
    click mints (``insert_draft`` sets no prefill_values). Returns ``{}`` when nothing is staged.

    Both the staging save and the originate click write to the SAME table; they differ only in that
    the staged row has values. ``ORDER BY updated_at DESC`` takes the most recent save, so the result
    is order-independent (stage-then-originate or originate-then-stage both resolve to the row with
    values). The UUID guard mirrors the other reads — a garbage ref returns ``{}``, not a 500."""
    try:
        UUID(str(opportunity_id))
    except (ValueError, TypeError, AttributeError):
        return {}
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT prefill_values
              FROM business.engagement_mandate_draft_content
             WHERE opportunity_id = %s::uuid
               AND prefill_values IS NOT NULL
               AND prefill_values <> '{}'::jsonb
             ORDER BY updated_at DESC
             LIMIT 1
            """,
            (opportunity_id,),
        )
        row = await cur.fetchone()
    return (row[0] if row else None) or {}


async def get_latest_by_opportunity(conn, opportunity_id: str) -> dict | None:
    """The opportunity's latest staging draft — what the prep page loads to resume editing what was
    staged off-screen. Returns None when the id is not a well-formed UUID or no draft exists yet (the
    UUID guard mirrors get_draft: a garbage ref must 404 cleanly, not abort the pooled transaction)."""
    try:
        UUID(str(opportunity_id))
    except (ValueError, TypeError, AttributeError):
        return None
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT id::text, documenso_template_id, archetype_id::text, prefill_values, status
              FROM business.engagement_mandate_draft_content
             WHERE opportunity_id = %s::uuid
             ORDER BY created_at DESC
             LIMIT 1
            """,
            (opportunity_id,),
        )
        row = await cur.fetchone()
    if not row:
        return None
    return {
        "id": row[0],
        "documenso_template_id": row[1],
        "archetype_id": row[2],
        "prefill_values": row[3] or {},
        "status": row[4],
    }


async def upsert_staging(
    conn, *, opportunity_id: str, documenso_template_id: str, prefill_values: dict
) -> str | None:
    """Create-or-update the opportunity's staging draft: stamp the selected template (plus the org +
    archetype resolved FROM that template — never trusted from the client) and the operator-entered
    per-deal values. Updates the opportunity's latest draft in place if one exists (edit-and-resave),
    else inserts. Returns the draft id, or None when the documenso template id is unknown.

    UUID-guarded like the reads: a non-UUID opportunity ref returns None (→ 404) rather than aborting
    the pooled transaction on ``%s::uuid``."""
    try:
        UUID(str(opportunity_id))
    except (ValueError, TypeError, AttributeError):
        return None
    pv = json.dumps(prefill_values or {})
    async with conn.cursor() as cur:
        # Resolve org + archetype from the template (this also validates it exists).
        await cur.execute(
            """
            SELECT organization_id, archetype_id
              FROM business.documenso_templates
             WHERE documenso_template_id = %s
            """,
            (documenso_template_id,),
        )
        tpl = await cur.fetchone()
        if not tpl:
            return None
        organization_id, archetype_id = tpl[0], tpl[1]
        # Update the opportunity's latest draft in place; if none, insert.
        await cur.execute(
            """
            WITH latest AS (
                SELECT id
                  FROM business.engagement_mandate_draft_content
                 WHERE opportunity_id = %s::uuid
                 ORDER BY created_at DESC
                 LIMIT 1
            )
            UPDATE business.engagement_mandate_draft_content d
               SET documenso_template_id = %s,
                   organization_id        = %s,
                   archetype_id           = %s,
                   prefill_values         = %s::jsonb,
                   updated_at             = now()
              FROM latest
             WHERE d.id = latest.id
            RETURNING d.id::text
            """,
            (opportunity_id, documenso_template_id, organization_id, archetype_id, pv),
        )
        row = await cur.fetchone()
        if not row:
            await cur.execute(
                """
                INSERT INTO business.engagement_mandate_draft_content
                     (opportunity_id, documenso_template_id, organization_id, archetype_id, prefill_values)
                VALUES (%s::uuid, %s, %s, %s, %s::jsonb)
                RETURNING id::text
                """,
                (opportunity_id, documenso_template_id, organization_id, archetype_id, pv),
            )
            row = await cur.fetchone()
    await conn.commit()
    return row[0] if row else None
