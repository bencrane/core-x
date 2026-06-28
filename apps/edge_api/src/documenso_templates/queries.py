"""``business.documenso_templates`` read access — psycopg async, read-only.

The Settings → Documenso → Manage Templates table's data source. Lists EVERY template for one
operator-org (active AND archived), scoped to the org by email domain
(``organizations.metadata->>'domain'``), mirroring the engagement-mappings picker's org filter.

Distinct from ``engagement_mappings.list_visible`` (which returns only the VISIBLE, MAPPED, active
ones for the prospect picker) — this is the full management view. No mutation.
"""
from __future__ import annotations

from psycopg.rows import dict_row

from .models import DocumensoTemplateSummary

_SQL = """
    SELECT dt.documenso_template_id           AS id,
           dt.name                            AS name,
           dt.slug                            AS slug,
           dt.status                          AS status,
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
