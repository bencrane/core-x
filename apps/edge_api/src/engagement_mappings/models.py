"""Engagement-mapping option — the prospect-facing engagement picker row.

A row in ``business.engagement_documenso_template_mappings`` maps a prospect-facing friendly
name to a Documenso template. The Dossier picker lists the VISIBLE ones for the operator's org.
``id`` is the underlying content-config slug (what a minted proposal stores as ``template_id``),
so origination is unaffected by the relabel; ``label`` is the prospect-facing mapping name.
"""
from __future__ import annotations

from pydantic import BaseModel


class EngagementMappingOption(BaseModel):
    id: str       # the Documenso template id (documenso_templates.documenso_template_id) — the originate value
    label: str    # prospect-facing mapping name
    # Archetype (the economic shape) the mapping's template belongs to — drives how the staging form
    # groups options and which value fields it shows. Nullable: a template may predate archetype tagging.
    archetype_key: str | None = None
    archetype_name: str | None = None
    performance_fee_basis: str | None = None
    # The template's declared Documenso merge fields (recipients.text_fields) — the exact inputs the
    # staging value form renders. Empty when the template declares none.
    text_fields: list[str] = []
