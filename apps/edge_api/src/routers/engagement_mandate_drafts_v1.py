"""Engagement mandate drafts — the direct-to-documenso Originate Mandate flow.

  POST /api/v1/engagement-mandate-drafts                          service-token — stamp (opportunity_id, template)
  GET  /api/v1/engagement-mandate-drafts/by-opportunity/{id}      service-token — read the opportunity's staging draft
  PUT  /api/v1/engagement-mandate-drafts/by-opportunity/{id}      service-token — save the opportunity's staging draft
  POST /api/v1/engagement-mandate-drafts/{id}/originate-prefilled service-token — instantiate the prefilled Documenso doc

When the operator is in ``direct-to-documenso`` mode, "Originate Mandate" inserts one row into
``business.engagement_mandate_draft_content`` (the gated replacement for the createProposal path);
"Originate prefilled" then instantiates a signable Documenso document FROM the draft's template via
``/api/v2/template/use`` with the opportunity's per-deal field values prefilled, distributes WITHOUT
email (``PENDING``), and returns the envelope id + numeric document id + signer token. The create
writes are service-token gated (the platform-api BFF brokers them with the operator session); the
prospect reads the document by the ``(opportunity_id, document_id)`` sign-token surface.
"""
from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException

from .. import config
from ..db import get_db_connection
from ..engagement_mandate_drafts import queries
from ..engagement_mandate_drafts.models import (
    EmbedTemplateOriginateRequest,
    MandateDraftCreate,
    MandateDraftCreated,
    MandateEmbedTemplateOriginated,
    MandatePrefilledOriginated,
    MandateStagingDraft,
    MandateStagingUpsert,
)
from ..service_token import require_service_token
from ..services import documenso_client

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/engagement-mandate-drafts", tags=["engagement-mandate-drafts"])


def _pick_direct_recipient_id(recipients: list[dict[str, Any]]) -> int | None:
    """Designate the template recipient the public direct-link signer assumes — the COUNTERPARTY, i.e.
    the recipient that is NOT the Rare Structure provider. Best-effort heuristic (overridable via the
    request body): prefer an explicit participant/client recipient, else the first non-provider, else
    the first recipient. Returns the recipient id, or None when the template has none."""
    if not recipients:
        return None

    def _txt(r: dict[str, Any]) -> str:
        return f"{r.get('email') or ''} {r.get('name') or ''}".lower()

    def _is_provider(r: dict[str, Any]) -> bool:
        t = _txt(r)
        return "provider" in t or "rare structure" in t or "crane" in t

    for r in recipients:
        if "participant" in _txt(r) or "client" in _txt(r):
            return r.get("id")
    for r in recipients:
        if not _is_provider(r):
            return r.get("id")
    return recipients[0].get("id")


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


@router.get("/by-opportunity/{opportunity_id}", dependencies=[Depends(require_service_token)])
async def get_staging_by_opportunity(opportunity_id: str) -> MandateStagingDraft | None:
    """The opportunity's staged mandate — selected template, archetype, and the per-deal values the
    operator entered — for the per-opportunity prep page to resume editing. Null when nothing is
    staged yet (or the opportunity ref is not a UUID)."""
    async with get_db_connection() as conn:
        draft = await queries.get_latest_by_opportunity(conn, opportunity_id)
    return MandateStagingDraft(**draft) if draft else None


@router.put("/by-opportunity/{opportunity_id}", dependencies=[Depends(require_service_token)])
async def upsert_staging_by_opportunity(
    opportunity_id: str, body: MandateStagingUpsert
) -> MandateDraftCreated:
    """Save the prep page: create-or-update the opportunity's staging draft with the selected template
    and the entered per-deal values. The org + archetype are resolved server-side from the template;
    re-saving edits the same draft in place. 404 when the template id is unknown."""
    async with get_db_connection() as conn:
        draft_id = await queries.upsert_staging(
            conn,
            opportunity_id=opportunity_id,
            documenso_template_id=body.documenso_template_id,
            prefill_values=body.prefill_values,
        )
    if not draft_id:
        raise HTTPException(status_code=404, detail="documenso template not found")
    return MandateDraftCreated(id=draft_id)


@router.post(
    "/{draft_id}/originate-prefilled",
    dependencies=[Depends(require_service_token)],
)
async def originate_prefilled(draft_id: str) -> MandatePrefilledOriginated:
    """Template-use lane — the canonical direct-to-documenso originate path.

    Instantiates a Documenso document FROM the draft's template via ``POST /api/v2/template/use`` with
    the opportunity's per-deal ``field_values`` PREFILLED, then distributes with
    ``distributionMethod:NONE`` so it lands ``PENDING`` (signable, no email sent). Values are sourced
    from ``business.opportunity_specific_content`` and the recipient from the opportunity's contact.
    No signature/readOnly handling — the signer fills those.
    """
    async with get_db_connection() as conn:
        draft = await queries.get_draft(conn, draft_id)
        if not draft:
            raise HTTPException(status_code=404, detail="mandate draft not found")
        prefill = await queries.get_opportunity_prefill_and_contact(
            conn, draft["opportunity_id"]
        )
    if not prefill:
        raise HTTPException(
            status_code=404, detail="no opportunity_specific_content for this opportunity"
        )
    if not prefill["recipient_email"]:
        raise HTTPException(
            status_code=422, detail="opportunity contact has no email to set as recipient"
        )
    # The opportunity's PUBLIC 8-char handle (business.opportunities.opportunity_id) — NOT the row
    # UUID. This is the externally-visible opportunity id: stamped as the envelope's externalId and
    # carried in the prospect link, so the signing gate matches it exactly on the read side.
    opportunity_ref = prefill["opportunity_ref"]
    try:
        result = await documenso_client.create_document_from_template(
            draft["documenso_template_id"],
            recipient_email=prefill["recipient_email"],
            recipient_name=prefill["recipient_name"] or prefill["recipient_email"],
            field_values_by_label=prefill["field_values"],
            # Stamp the opportunity's 8-char handle as the envelope's externalId — the durable anchor
            # the Documenso webhook capture carries back, and the prospect-link access capability.
            external_id=opportunity_ref,
            # Prospect-facing title for the derived document — replaces the template's raw filename
            # ("AO_Term_Plain_v1.pdf") in the signer's confirmation modal + the downloaded PDF.
            title="Engagement Agreement",
        )
    except documenso_client.DocumensoError as e:
        raise HTTPException(status_code=502, detail=f"documenso: {e}") from e
    return MandatePrefilledOriginated(
        envelope_id=result.envelope_id,
        document_id=result.document_id,
        # The 8-char handle is the link capability and the externalId stamped on the envelope —
        # surfaced so the SPA can build /p/m/{opportunity_id}/{document_id} directly.
        opportunity_id=opportunity_ref,
        signing_token=result.client_token,
        status="pending",
        documenso_host=config.documenso_api_url(),
    )


@router.post(
    "/{draft_id}/originate-embed-template",
    dependencies=[Depends(require_service_token)],
)
async def originate_embed_template(
    draft_id: str, body: EmbedTemplateOriginateRequest = EmbedTemplateOriginateRequest()
) -> MandateEmbedTemplateOriginated:
    """Embed-template lane — PARALLEL to ``originate-prefilled`` (which is left untouched).

    Enables a Documenso DIRECT LINK on the draft's template and returns its reusable token for the SPA
    to mount ``<EmbedDirectTemplate>``. NO document is created here — the signer self-identifies in the
    embed and Documenso creates the document (source ``TEMPLATE_DIRECT_LINK``) at completion. The
    opportunity's PUBLIC 8-char handle is returned as ``external_id`` for the embed to stamp (and the
    prospect (opportunity_id, document_id) gate, once the completed document id is captured client-side).

    Kept distinct from the document path so the embed-document lane stays available for the
    custom-PDF-as-custom-document case.
    """
    async with get_db_connection() as conn:
        draft = await queries.get_draft(conn, draft_id)
        if not draft:
            raise HTTPException(status_code=404, detail="mandate draft not found")
        opp = await queries.get_opportunity_ref_and_contact(conn, draft["opportunity_id"])
    if not opp:
        raise HTTPException(status_code=404, detail="opportunity not found for this draft")

    template_id = draft["documenso_template_id"]
    try:
        recipients = await documenso_client.get_template_recipients(template_id)
        direct_recipient_id = body.direct_recipient_id or _pick_direct_recipient_id(recipients)
        link = await documenso_client.create_direct_link(
            template_id, direct_recipient_id=direct_recipient_id
        )
    except documenso_client.DocumensoError as e:
        raise HTTPException(status_code=502, detail=f"documenso: {e}") from e
    if not link.token:
        raise HTTPException(status_code=502, detail="documenso direct link returned no token")

    host = config.documenso_api_url()
    logger.info(
        "embed-template originate: draft=%s template=%s direct_recipient=%s token=%s",
        draft_id, template_id, link.direct_template_recipient_id or direct_recipient_id, link.token[:8],
    )
    return MandateEmbedTemplateOriginated(
        direct_token=link.token,
        documenso_host=host,
        embed_url=f"{host}/embed/direct/{link.token}",
        external_id=opp["opportunity_ref"],
        opportunity_id=opp["opportunity_ref"],
        direct_recipient_id=link.direct_template_recipient_id or direct_recipient_id,
        recipient_email=opp["recipient_email"],
        recipient_name=opp["recipient_name"],
        status="ready",
    )
