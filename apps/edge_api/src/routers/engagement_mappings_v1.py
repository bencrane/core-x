"""Engagement mappings — the Dossier engagement picker's data source.

  GET /api/v1/engagement-mappings   service-token — visible mappings for one operator-org

Lists ``business.engagement_documenso_template_mappings`` where ``is_visible`` is true, scoped
to the operator's org by email domain (``?org_domain=``, passed by the BFF). Each maps a
prospect-facing name to a Documenso template; the returned ``id`` is the Documenso template id.

The staging value form's fields (``text_fields``) are sourced LIVE from the actual Documenso
template — each TEXT field's label, pulled per option — NOT from a stored/denormalized list, so the
form always matches the real template. Best-effort per option: a Documenso miss leaves the row's
stored fallback intact. Service-token gated — the platform-api BFF brokers it.
"""
from __future__ import annotations

import asyncio

from fastapi import APIRouter, Depends

from ..db import get_db_connection
from ..engagement_mappings import queries
from ..engagement_mappings.models import EngagementMappingOption
from ..service_token import require_service_token
from ..services import documenso_client

router = APIRouter(prefix="/api/v1/engagement-mappings", tags=["engagement-mappings"])


@router.get("", dependencies=[Depends(require_service_token)])
async def list_engagement_mappings(org_domain: str | None = None) -> list[EngagementMappingOption]:
    async with get_db_connection() as conn:
        options = await queries.list_visible(conn, org_domain)

    async def _fill_live(opt: EngagementMappingOption) -> None:
        # Override the stored text_fields with the live Documenso template's TEXT-field labels.
        try:
            opt.text_fields = await documenso_client.get_template_text_field_labels(opt.id)
        except documenso_client.DocumensoError:
            pass  # keep whatever the query returned (the stored fallback)

    await asyncio.gather(*(_fill_live(o) for o in options))
    return options
