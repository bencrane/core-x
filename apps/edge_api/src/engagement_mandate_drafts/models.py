"""Engagement mandate draft — the direct-to-documenso "Originate Mandate" stamp.

In direct-to-documenso mode, clicking Originate Mandate inserts one row into
``business.engagement_mandate_draft_content`` stamping the opportunity + the Documenso template
id of the selected engagement. No proposal row, no DocRaptor — just the draft pointer.
"""
from __future__ import annotations

from pydantic import BaseModel


class MandateDraftCreate(BaseModel):
    opportunity_id: str
    documenso_template_id: str


class MandateDraftCreated(BaseModel):
    id: str
