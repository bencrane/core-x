"""Documenso templates — the Settings → Documenso → Manage Templates table data source.

  GET  /api/v1/documenso-templates           service-token — every template for one operator-org
  POST /api/v1/documenso-templates/default   service-token — mark one template as the org default

Lists ``business.documenso_templates`` (active AND archived) scoped to the operator's org by email
domain (``?org_domain=``, passed by the BFF), and marks ONE as the Confirm & Originate default (one
per org, ACTIVE only). Service-token gated — the platform-api BFF
brokers it. Distinct from ``/api/v1/engagement-mappings`` (which lists only the VISIBLE, MAPPED,
active ones for the prospect picker) and ``/api/v1/documenso-template-fields`` (one template's
fields). Prefix is disjoint from those, so there is no route collision.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from ..db import get_db_connection
from ..documenso_templates import queries
from ..documenso_templates.models import DocumensoTemplateSummary, SetDefaultRequest
from ..service_token import require_service_token

router = APIRouter(prefix="/api/v1/documenso-templates", tags=["documenso-templates"])


@router.get("", dependencies=[Depends(require_service_token)])
async def list_documenso_templates(
    org_domain: str | None = None,
) -> list[DocumensoTemplateSummary]:
    async with get_db_connection() as conn:
        return await queries.list_for_org(conn, org_domain)


@router.post("/default", dependencies=[Depends(require_service_token)])
async def set_default_template(body: SetDefaultRequest) -> dict[str, str]:
    """Mark ``documenso_template_id`` as the operator-org's Confirm & Originate default (one per org).
    404 when the target is unknown, archived, or not in the operator's org."""
    async with get_db_connection() as conn:
        ok = await queries.set_default(conn, body.org_domain, body.documenso_template_id)
    if not ok:
        raise HTTPException(
            status_code=404,
            detail="template not found, not active, or not in your org",
        )
    return {"default": body.documenso_template_id}
