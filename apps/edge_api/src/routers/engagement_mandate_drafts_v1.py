"""Engagement mandate drafts — the direct-to-documenso Originate Mandate stamp.

  POST /api/v1/engagement-mandate-drafts   service-token — stamp (opportunity_id, documenso_template_id)

When the operator is in ``direct-to-documenso`` mode, "Originate Mandate" inserts one row into
``business.engagement_mandate_draft_content`` (the gated replacement for the createProposal path).
Service-token gated — the platform-api BFF brokers it with the operator session.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from ..db import get_db_connection
from ..engagement_mandate_drafts import queries
from ..engagement_mandate_drafts.models import MandateDraftCreate, MandateDraftCreated
from ..service_token import require_service_token

router = APIRouter(prefix="/api/v1/engagement-mandate-drafts", tags=["engagement-mandate-drafts"])


@router.post("", dependencies=[Depends(require_service_token)])
async def create_mandate_draft(body: MandateDraftCreate) -> MandateDraftCreated:
    async with get_db_connection() as conn:
        draft_id = await queries.insert_draft(
            conn,
            opportunity_id=body.opportunity_id,
            documenso_template_id=body.documenso_template_id,
        )
    if not draft_id:
        raise HTTPException(status_code=404, detail="documenso template not found")
    return MandateDraftCreated(id=draft_id)
