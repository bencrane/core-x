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

from .. import config
from ..db import get_db_connection
from ..deals import queries
from ..deals.models import DealDetails, DealDetailsUpdate, DealOriginated, DealSummary, TemplateOption
from ..service_token import require_service_token
from ..services import documenso_client

router = APIRouter(prefix="/api/v1/deals", tags=["deals"])


@router.get("", dependencies=[Depends(require_service_token)])
async def list_deals(limit: int = 100) -> list[DealSummary]:
    async with get_db_connection() as conn:
        rows = await queries.list_recent(conn, min(max(limit, 1), 500))
    return [DealSummary.from_row(d) for d in rows]


def _details_payload(deal: dict, contacts: list[dict], available: list[dict],
                     templates: list[dict]) -> DealDetails:
    return DealDetails(
        deal_id=deal["deal_id"],
        deal_handle=deal["deal_handle"],
        company_name=deal.get("company_name"),
        company_domain=deal.get("company_domain"),
        contacts=contacts,
        available_contacts=available,
        field_values=deal.get("field_values") or {},
        default_template_uuid=deal.get("default_template_uuid"),
        template_origin=deal.get("template_origin") or "default",
        available_templates=[TemplateOption(**t) for t in templates],
    )


async def _assemble_details(conn, deal: dict) -> DealDetails:
    contacts = await queries.get_deal_contacts(conn, deal["deal_id"])
    available = await queries.get_available_contacts(conn, deal.get("account_id"), deal["deal_id"])
    templates = await queries.list_org_templates(conn, deal.get("organization_id"))
    return _details_payload(deal, contacts, available, templates)


# GET /api/v1/deals/{handle}/details — the deal's editable deal_details: its contacts (from the
# deal_contacts junction, person fields read-only), the account's available contacts to add,
# field_values, and the deal-org's selectable Documenso templates.
@router.get("/{handle}/details", dependencies=[Depends(require_service_token)])
async def get_deal_details(handle: str) -> DealDetails:
    async with get_db_connection() as conn:
        deal = await queries.get_deal_with_details(conn, handle)
        if deal is None:
            raise HTTPException(status_code=404, detail="deal not found")
        return await _assemble_details(conn, deal)


# PUT /api/v1/deals/{handle}/details — write field_values + the attached template and reconcile the
# deal_contacts junction (membership + is_signatory). Returns the canonical merged shape (re-read).
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
            field_values=body.field_values,
            default_template_uuid=body.default_template_uuid,
        )
        fresh = await queries.get_deal_with_details(conn, handle)
        return await _assemble_details(conn, fresh)


# POST /api/v1/deals/{handle}/originate — mint a prefilled, PENDING Documenso document for the deal
# from its attached template. Resolves the signatory contact + template off the deal, stamps the
# public deal_handle as the envelope externalId (the prospect-link capability + sign-gate anchor),
# and returns the /p/m/{deal_handle}/{document_id} sign link. No email is sent; payment is Phase 3.
@router.post("/{handle}/originate", dependencies=[Depends(require_service_token)])
async def originate_deal(handle: str) -> DealOriginated:
    async with get_db_connection() as conn:
        inp = await queries.get_deal_originate_inputs(conn, handle)
        if inp is None:
            raise HTTPException(status_code=404, detail="deal not found")
        if not inp["documenso_template_id"]:
            raise HTTPException(status_code=422, detail="deal has no attached template")
        if not inp["recipient_email"]:
            raise HTTPException(status_code=422, detail="deal has no signatory contact with an email")
        template_id = inp["documenso_template_id"]
        prospect_rid = await queries.get_prospect_recipient_id(conn, template_id)
        template_defaults = await queries.get_template_default_field_values(conn, template_id)
        editable_labels = await queries.get_template_editable_field_labels(conn, template_id)
    # Template-level defaults UNDER the deal's per-deal field_values (the deal overrides). Both {} today.
    field_values = {**template_defaults, **(inp["field_values"] or {})}
    try:
        result = await documenso_client.create_document_from_template(
            template_id,
            recipient_email=inp["recipient_email"],
            recipient_name=inp["recipient_name"] or inp["recipient_email"],
            field_values_by_label=field_values,
            external_id=inp["deal_handle"],  # LOCKED: externalId = deal_handle (the public 8-char id)
            title="Engagement Agreement",
            prospect_recipient_id=prospect_rid,
            editable_labels=editable_labels,
        )
    except documenso_client.DocumensoError as e:
        raise HTTPException(status_code=502, detail=f"documenso: {e}") from e
    # The numeric document_id IS the sign-link disambiguator (/p/m/{handle}/{document_id}); a missing
    # one yields a dead "…/None" link. Fail loud here rather than return a successful-looking 200.
    if result.document_id is None:
        raise HTTPException(status_code=502, detail="documenso returned no numeric document id")
    return DealOriginated(
        envelope_id=result.envelope_id,
        document_id=result.document_id,
        deal_handle=inp["deal_handle"],
        signing_token=result.client_token,
        sign_link=f"/p/m/{inp['deal_handle']}/{result.document_id}",
        status="pending",
        documenso_host=config.documenso_api_url(),
    )
