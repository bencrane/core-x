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
from ..deals import create, originate, queries
from ..deals.models import (
    DealCreate,
    DealCreated,
    DealDetails,
    DealDetailsUpdate,
    DealOriginated,
    DealSummary,
    TemplateOption,
)
from ..service_token import require_service_token
from ..services import documenso_client

router = APIRouter(prefix="/api/v1/deals", tags=["deals"])


@router.get("", dependencies=[Depends(require_service_token)])
async def list_deals(limit: int = 100) -> list[DealSummary]:
    async with get_db_connection() as conn:
        rows = await queries.list_recent(conn, min(max(limit, 1), 500))
    return [DealSummary.from_row(d) for d in rows]


# POST /api/v1/deals — the MANUAL deal lane: mint a deal from an operator payload with NO booking
# upstream (the parallel to the Cal.com producer). Same grain (one deal per account) and same upsert
# semantics as deals/materialize.py. Ontology ruling 2026-07-19: signatory-free — a deal is
# company_name (+domain); person fields are optional plain-contact data. The returned deal_handle
# keys the follow-ups (POST /deals/{handle}/agreements → generate).
@router.post("", dependencies=[Depends(require_service_token)])
async def create_deal(body: DealCreate) -> DealCreated:
    company = body.company_name.strip()
    if not company:
        raise HTTPException(status_code=422, detail="company_name is required")
    email = (body.email or "").strip()
    if email and "@" not in email:
        raise HTTPException(status_code=422, detail="email is not a valid email")
    async with get_db_connection() as conn:
        result = await create.create_deal_manual(
            conn,
            company_name=company,
            domain=body.domain,
            first_name=(body.first_name or "").strip() or None,
            last_name=(body.last_name or "").strip() or None,
            email=email or None,
            title=(body.title or "").strip() or None,
        )
        await conn.commit()
    return DealCreated(**result)


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
        template_documenso_id=deal.get("template_documenso_id"),
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
        await queries.upsert_document_config(
            conn,
            deal_id=deal["deal_id"],
            contacts=body.contacts,
            field_values=body.field_values,
            template_documenso_id=body.template_documenso_id,
        )
        fresh = await queries.get_deal_with_details(conn, handle)
        return await _assemble_details(conn, fresh)


# POST /api/v1/deals/{handle}/originate — mint a prefilled, PENDING Documenso document for the deal
# from its attached template. Resolves the signatory contact + template off the deal, stamps the
# public deal_handle as the envelope externalId (the prospect-link capability + sign-gate anchor),
# and returns the /p/m/{deal_handle}/{document_id} sign link. No email is sent; payment is Phase 3.
# DEPRECATED (2026-07-19): superseded by POST /api/v1/agreements/{id}/generate — the Agreement owns
# template/values/signatory/lifecycle now (agreements_v1.py). Do not extend this route.
@router.post("/{handle}/originate", dependencies=[Depends(require_service_token)])
async def originate_deal(handle: str) -> DealOriginated:
    async with get_db_connection() as conn:
        inp = await queries.get_deal_originate_inputs(conn, handle)
    if inp is None:
        raise HTTPException(status_code=404, detail="deal not found")
    if inp["template_documenso_id"] is None:
        raise HTTPException(status_code=422, detail="deal has no attached template")
    if not inp["recipient_email"]:
        raise HTTPException(status_code=422, detail="deal has no signatory contact with an email")
    # Resolve (model B): per template label, value = the deal's field_values[label] (override) ELSE the
    # prefill-config default. read_only config labels LOCK on the derived document; the rest stay editable
    # for the prospect. The prospect recipient is derived from the template's verbatim recipients.
    field_settings = inp["field_settings"] or {}
    field_values = originate.resolve_field_values(field_settings, inp["field_values"] or {})
    locked = originate.locked_labels(field_settings)
    # Fail loud: a read_only operator term with no resolved value can't be locked (Documenso requires
    # read-only fields to carry text) — it would silently ship signer-EDITABLE. Refuse instead so an
    # unset term (e.g. a blank fee) never reaches the prospect as an open field.
    missing_locked = locked - set(field_values)
    if missing_locked:
        raise HTTPException(
            status_code=422,
            detail=(
                "operator-term field(s) marked read-only but with no value: "
                f"{', '.join(sorted(missing_locked))} — set a default in the template prefill config "
                "or a value on the deal before originating"
            ),
        )
    editable_labels = set(field_values) - locked
    prospect_rid = originate.derive_prospect_recipient_id(inp["template_response"])
    try:
        result = await documenso_client.create_document_from_template(
            str(inp["template_documenso_id"]),
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


@router.get("/company-intel", dependencies=[Depends(require_service_token)])
async def company_intel(domain: str) -> dict:
    """Firmographic intel for a domain — currently the verbatim LeadMagic company payload
    (corex.company_leadmagic, fetch-once at booking). Serves the gc Mandate page's
    recognition band. Returns {found: bool, status, fetched_at, payload}."""
    d = (domain or "").strip().lower()
    if not d:
        raise HTTPException(status_code=422, detail="domain required")
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT payload, status, fetched_at FROM corex.company_leadmagic WHERE domain = %s",
                (d,),
            )
            row = await cur.fetchone()
    if not row:
        return {"found": False, "domain": d}
    payload, status, fetched_at = row
    return {"found": True, "domain": d, "status": status,
            "fetched_at": fetched_at.isoformat() if fetched_at else None, "payload": payload}
