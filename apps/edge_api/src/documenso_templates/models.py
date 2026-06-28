"""Documenso-template summary + set-default request for the Manage Templates table.

The Settings → Documenso → Manage Templates table lists EVERY template for the operator's org
(active AND archived), independent of whether it is mapped into the engagement picker. ``id`` is the
external Documenso template id (``documenso_template_id``); ``is_default`` marks the org's Confirm &
Originate default (one per org, set via ``SetDefaultRequest``).
"""
from __future__ import annotations

from pydantic import BaseModel


class DocumensoTemplateSummary(BaseModel):
    id: str                              # documenso_templates.documenso_template_id (external Documenso id)
    name: str
    slug: str | None = None
    status: str                          # 'active' | 'archived'
    is_default: bool = False             # the org's Confirm & Originate default (at most one per org)
    archetype_name: str | None = None    # nullable: a template may predate archetype tagging


class SetDefaultRequest(BaseModel):
    """Mark a template as the org's Confirm & Originate default. ``org_domain`` is the operator's
    email domain (the BFF passes it); ``documenso_template_id`` is the target. Only an ACTIVE template
    in the operator's org may be set as default."""

    documenso_template_id: str
    org_domain: str | None = None
