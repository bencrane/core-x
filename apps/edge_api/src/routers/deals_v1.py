"""Deals — the operator Applications / Research list data source.

  GET /api/v1/deals   service-token — operator list (most recent first)

Deals are the first-class pipeline object (``business.deals``, one per account via
``uq_deals_account``), replacing the booking->opportunity projection as the cockpit's list +
Application detail surface. Read-only for now; the booking->deal upsert and deal mutations
land in later phases. Service-token gated — the platform-api BFF brokers it with the operator
session. Each row carries ``last_booking_id`` so the detail page resolves handle -> booking.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from ..db import get_db_connection
from ..deals import queries
from ..deals.models import DealDetails, DealDetailsUpdate, DealSummary, TemplateOption
from ..service_token import require_service_token

router = APIRouter(prefix="/api/v1/deals", tags=["deals"])


@router.get("", dependencies=[Depends(require_service_token)])
async def list_deals(limit: int = 100) -> list[DealSummary]:
    async with get_db_connection() as conn:
        rows = await queries.list_recent(conn, min(max(limit, 1), 500))
    return [DealSummary.from_row(d) for d in rows]


def _details_payload(deal: dict, templates: list[dict]) -> DealDetails:
    return DealDetails(
        deal_id=deal["deal_id"],
        deal_handle=deal["deal_handle"],
        company_name=deal.get("company_name"),
        company_domain=deal.get("company_domain"),
        contacts=deal.get("contacts") or [],
        content=deal.get("content") or {},
        default_template_uuid=deal.get("default_template_uuid"),
        template_origin=deal.get("template_origin") or "default",
        available_templates=[TemplateOption(**t) for t in templates],
    )


# GET /api/v1/deals/{handle}/details — the deal's editable deal_details (contacts + content + the
# attached Documenso template) plus the deal-org's selectable templates for the editor dropdown.
@router.get("/{handle}/details", dependencies=[Depends(require_service_token)])
async def get_deal_details(handle: str) -> DealDetails:
    async with get_db_connection() as conn:
        deal = await queries.get_deal_with_details(conn, handle)
        if deal is None:
            raise HTTPException(status_code=404, detail="deal not found")
        templates = await queries.list_org_templates(conn, deal.get("organization_id"))
    return _details_payload(deal, templates)


# PUT /api/v1/deals/{handle}/details — upsert the deal's deal_details. Returns the canonical merged
# shape (re-read); template_origin is derived server-side, never client-set.
@router.put("/{handle}/details", dependencies=[Depends(require_service_token)])
async def update_deal_details(handle: str, body: DealDetailsUpdate) -> DealDetails:
    async with get_db_connection() as conn:
        deal = await queries.get_deal_with_details(conn, handle)
        if deal is None:
            raise HTTPException(status_code=404, detail="deal not found")
        await queries.upsert_details(
            conn,
            deal_id=deal["deal_id"],
            contacts=body.contacts,
            content=body.content,
            default_template_uuid=body.default_template_uuid,
        )
        fresh = await queries.get_deal_with_details(conn, handle)
        templates = await queries.list_org_templates(conn, fresh.get("organization_id"))
    return _details_payload(fresh, templates)
