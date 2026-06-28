"""Documenso templates — the Settings → Documenso → Manage Templates table data source.

  GET /api/v1/documenso-templates   service-token — every template for one operator-org

Lists ``business.documenso_templates`` (active AND archived) scoped to the operator's org by email
domain (``?org_domain=``, passed by the BFF). Read-only. Service-token gated — the platform-api BFF
brokers it. Distinct from ``/api/v1/engagement-mappings`` (which lists only the VISIBLE, MAPPED,
active ones for the prospect picker) and ``/api/v1/documenso-template-fields`` (one template's
fields). Prefix is disjoint from those, so there is no route collision.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends

from ..db import get_db_connection
from ..documenso_templates import queries
from ..documenso_templates.models import DocumensoTemplateSummary
from ..service_token import require_service_token

router = APIRouter(prefix="/api/v1/documenso-templates", tags=["documenso-templates"])


@router.get("", dependencies=[Depends(require_service_token)])
async def list_documenso_templates(
    org_domain: str | None = None,
) -> list[DocumensoTemplateSummary]:
    async with get_db_connection() as conn:
        return await queries.list_for_org(conn, org_domain)
