"""Engagement mandate drafts — the direct-to-documenso Originate Mandate flow.

  POST /api/v1/engagement-mandate-drafts                 service-token — stamp (opportunity_id, template)
  POST /api/v1/engagement-mandate-drafts/{id}/confirm    service-token — instantiate the Documenso doc
  GET  /api/v1/engagement-mandate-drafts/document/{eid}  PUBLIC        — prospect reads the signer token

When the operator is in ``direct-to-documenso`` mode, "Originate Mandate" inserts one row into
``business.engagement_mandate_draft_content`` (the gated replacement for the createProposal path);
"Confirm & originate" then instantiates a signable Documenso document FROM the draft's template (no
DocRaptor render) and returns the envelope id + signer token. The confirm/create writes are
service-token gated (the platform-api BFF brokers them with the operator session); the prospect's
document read is PUBLIC — the envelope id is the capability, exactly like the proposal ``ref``.

STATELESS by design: no envelope/token columns are added to the draft. The envelope id carried in
the prospect link is the only handle back to the document (re-confirm mints a fresh one) — durable
persistence is a clean follow-up, not a prerequisite for the flow.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from .. import config
from ..db import get_db_connection
from ..engagement_mandate_drafts import queries
from ..engagement_mandate_drafts.models import (
    MandateDraftConfirmed,
    MandateDraftCreate,
    MandateDraftCreated,
    MandateDraftDocument,
    MandatePrefilledOriginated,
    MandateStagingDraft,
    MandateStagingUpsert,
)
from ..service_token import require_service_token
from ..services import documenso_client

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


@router.post("/{draft_id}/confirm", dependencies=[Depends(require_service_token)])
async def confirm_mandate_draft(draft_id: str) -> MandateDraftConfirmed:
    """Direct-to-documenso originate: instantiate a signable Documenso document from the draft's
    template and hand back the envelope id + signer token. No PDF render — the template carries the
    document, recipients, and fields; we only instantiate, distribute (no email), and read the token.
    """
    async with get_db_connection() as conn:
        draft = await queries.get_draft(conn, draft_id)
        if not draft:
            raise HTTPException(status_code=404, detail="mandate draft not found")
        # Pull the per-deal values from the opportunity's STAGED draft (the Stage-mandate save),
        # NOT from this freshly-minted confirm draft (which carries no prefill_values of its own).
        prefill_values = await queries.get_staged_prefill_values(conn, draft["opportunity_id"])
    try:
        result = await documenso_client.create_document_from_template_with_custom_pdf(
            draft["documenso_template_id"], external_id=draft_id,
            prefill_values=prefill_values,
        )
    except documenso_client.DocumensoError as e:
        raise HTTPException(status_code=502, detail=f"documenso: {e}") from e
    return MandateDraftConfirmed(
        envelope_id=result.envelope_id,
        signing_token=result.client_token,
        documenso_host=config.documenso_api_url(),
    )


@router.post(
    "/{draft_id}/originate-prefilled",
    dependencies=[Depends(require_service_token)],
)
async def originate_prefilled(draft_id: str) -> MandatePrefilledOriginated:
    """Template-use lane — PARALLEL to ``/confirm``, leaving it untouched.

    Instantiates a Documenso document FROM the draft's template via ``POST /api/v2/template/use`` with
    the opportunity's per-deal ``field_values`` PREFILLED, then distributes with
    ``distributionMethod:NONE`` so it lands ``PENDING`` (signable, no email sent). Unlike ``/confirm``
    (the ``/envelope/use`` lane, which pulls the engagement-mandate-draft ``prefill_values``), this
    lane sources values from ``business.opportunity_specific_content`` and the recipient from the
    opportunity's contact. No signature/readOnly handling — the signer fills those.
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


@router.get("/document/{envelope_id}")
async def read_mandate_draft_document(envelope_id: str) -> MandateDraftDocument:
    """PUBLIC prospect read — the envelope id is the capability (no service token). Re-reads the live
    Documenso envelope for the signer token + status that drive the embedded signer."""
    try:
        token, status = await documenso_client.read_template_document(envelope_id)
    except documenso_client.DocumensoError as e:
        raise HTTPException(status_code=404, detail="document not found") from e
    return MandateDraftDocument(
        signing_token=token,
        documenso_host=config.documenso_api_url(),
        status=status,
    )
