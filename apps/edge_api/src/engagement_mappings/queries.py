"""business.engagement_documenso_template_mappings read access — psycopg async, read-only.

The Dossier engagement picker's data source. Lists the VISIBLE mappings scoped to one
operator-org (by email domain), resolving each to the underlying content-config slug so the
selected value still drives origination unchanged.

    mapping ──▶ documenso_templates ──▶ global_engagement_content (slug)
              (m.documenso_template_uuid)  (dt.source_config_id)

The org filter is on the MAPPING's organization (its ``metadata->>'domain'``).
"""
from __future__ import annotations

from psycopg.rows import dict_row

from .models import EngagementMappingOption

_SQL = """
    SELECT dt.documenso_template_id            AS id,
           m.name                              AS label,
           a.key                               AS archetype_key,
           a.name                              AS archetype_name,
           a.performance_fee_basis             AS performance_fee_basis,
           COALESCE(dt.recipients->'text_fields', '[]'::jsonb) AS text_fields
      FROM business.engagement_documenso_template_mappings m
      JOIN business.documenso_templates dt       ON dt.id = m.documenso_template_uuid
      JOIN business.organizations o              ON o.id = m.organization_id
      LEFT JOIN business.engagement_archetypes a ON a.id = dt.archetype_id
     WHERE m.is_visible = true
       AND m.status = 'active'
       AND lower(o.metadata->>'domain') = lower(%s)
     ORDER BY a.name NULLS LAST, m.name
"""


async def list_visible(conn, org_domain: str | None) -> list[EngagementMappingOption]:
    """Visible engagement options for the operator's org domain. Empty domain → no options."""
    if not org_domain:
        return []
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(_SQL, (org_domain,))
        rows = await cur.fetchall()
    return [EngagementMappingOption(**r) for r in rows]
