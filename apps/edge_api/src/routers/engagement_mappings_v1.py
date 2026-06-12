"""Engagement mappings — the Dossier engagement picker's data source.

  GET /api/v1/engagement-mappings   service-token — visible mappings for one operator-org

Lists ``business.engagement_documenso_template_mappings`` where ``is_visible`` is true, scoped
to the operator's org by email domain (``?org_domain=``, passed by the BFF). Each maps a
prospect-facing name to a Documenso template; the returned ``id`` is the underlying content-config
slug, so origination is unaffected. Service-token gated — the platform-api BFF brokers it.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends

from ..db import get_db_connection
from ..engagement_mappings import queries
from ..engagement_mappings.models import EngagementMappingOption
from ..service_token import require_service_token

router = APIRouter(prefix="/api/v1/engagement-mappings", tags=["engagement-mappings"])


@router.get("", dependencies=[Depends(require_service_token)])
async def list_engagement_mappings(org_domain: str | None = None) -> list[EngagementMappingOption]:
    async with get_db_connection() as conn:
        return await queries.list_visible(conn, org_domain)
