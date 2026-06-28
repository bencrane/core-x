"""Documenso-template summary — a row of ``business.documenso_templates`` for the management table.

The Settings → Documenso → Manage Templates table lists EVERY template for the operator's org
(active AND archived), independent of whether it is mapped into the engagement picker. Read-only.
``id`` is the external Documenso template id (``documenso_template_id``).
"""
from __future__ import annotations

from pydantic import BaseModel


class DocumensoTemplateSummary(BaseModel):
    id: str                              # documenso_templates.documenso_template_id (external Documenso id)
    name: str
    slug: str | None = None
    status: str                          # 'active' | 'archived'
    archetype_name: str | None = None    # nullable: a template may predate archetype tagging
