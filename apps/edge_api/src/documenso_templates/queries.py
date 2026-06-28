"""``business.documenso_templates`` access — psycopg async.

Backs the Settings → Documenso → Manage Templates table. ``list_for_org`` lists EVERY template for
one operator-org (active AND archived), scoped by email domain (``organizations.metadata->>'domain'``),
mirroring the engagement-mappings picker's org filter. ``set_default`` marks ONE template as the org's
Confirm & Originate default.

Distinct from ``engagement_mappings.list_visible`` (only the VISIBLE, MAPPED, active ones for the
prospect picker) — this is the full management view.
"""
from __future__ import annotations

from psycopg.rows import dict_row

from .models import DocumensoTemplateSummary

_SQL = """
    SELECT dt.documenso_template_id           AS id,
           dt.name                            AS name,
           dt.slug                            AS slug,
           dt.status                          AS status,
           dt.is_default                      AS is_default,
           a.name                             AS archetype_name
      FROM business.documenso_templates dt
      JOIN business.organizations o              ON o.id = dt.organization_id
      LEFT JOIN business.engagement_archetypes a ON a.id = dt.archetype_id
     WHERE lower(o.metadata->>'domain') = lower(%s)
     ORDER BY dt.status, dt.name
"""


async def list_for_org(conn, org_domain: str | None) -> list[DocumensoTemplateSummary]:
    """Every documenso_template for the operator's org domain. Empty domain → no rows."""
    if not org_domain:
        return []
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(_SQL, (org_domain,))
        rows = await cur.fetchall()
    return [DocumensoTemplateSummary(**r) for r in rows]


async def set_default(conn, org_domain: str | None, documenso_template_id: str) -> bool:
    """Mark ONE template as the org's Confirm & Originate default.

    Validates the target is an ACTIVE template in the operator's org, then CLEARS the org's current
    default and SETS the new one inside a transaction. Clear-then-set (two statements) keeps the
    per-org partial unique index ``documenso_templates_one_default_per_org`` from being transiently
    violated. Returns False when the target is unknown / archived / not in the org (caller → 404).
    """
    if not org_domain:
        return False
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT dt.id
              FROM business.documenso_templates dt
              JOIN business.organizations o ON o.id = dt.organization_id
             WHERE dt.documenso_template_id = %(tid)s
               AND lower(o.metadata->>'domain') = lower(%(domain)s)
               AND dt.status = 'active'
            """,
            {"tid": documenso_template_id, "domain": org_domain},
        )
        target = await cur.fetchone()
        if not target:
            return False
        async with conn.transaction():
            await cur.execute(
                """
                UPDATE business.documenso_templates dt
                   SET is_default = false, updated_at = now()
                  FROM business.organizations o
                 WHERE o.id = dt.organization_id
                   AND lower(o.metadata->>'domain') = lower(%(domain)s)
                   AND dt.is_default
                """,
                {"domain": org_domain},
            )
            await cur.execute(
                "UPDATE business.documenso_templates SET is_default = true, updated_at = now() "
                "WHERE id = %(id)s",
                {"id": target["id"]},
            )
    return True
