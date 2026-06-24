"""Engagement mandate draft — the direct-to-documenso "Originate Mandate" stamp.

In direct-to-documenso mode, clicking Originate Mandate inserts one row into
``business.engagement_mandate_draft_content`` stamping the opportunity + the Documenso template
id of the selected engagement. No proposal row, no DocRaptor — just the draft pointer.
"""
from __future__ import annotations

from pydantic import BaseModel


class MandateDraftCreate(BaseModel):
    opportunity_id: str
    documenso_template_id: str


class MandateDraftCreated(BaseModel):
    id: str


class MandatePrefilledOriginated(BaseModel):
    """The template-use lane result — a Documenso document instantiated FROM the template via
    ``/template/use`` with the opportunity's field values prefilled, then distributed
    (``distributionMethod:NONE``) so it lands ``PENDING`` (signable, no email sent). ``status`` is
    ``"pending"``.

    The prospect signing link is ``/p/m/{opportunity_id}/{document_id}``: ``opportunity_id`` is the
    opportunity's PUBLIC 8-char handle (``business.opportunities.opportunity_id`` = first 8 of the row
    UUID) stamped as the envelope's ``externalId`` — the access capability — and the numeric
    ``document_id`` is the disambiguator behind it. Both come off the originate, so the SPA builds the
    link directly from this response. ``envelope_id`` (the prefixed handle) is retained for continuity
    but is NOT part of the prospect link anymore."""

    envelope_id: str
    document_id: int | None = None
    opportunity_id: str
    signing_token: str | None = None
    status: str = "pending"
    documenso_host: str


class EmbedTemplateOriginateRequest(BaseModel):
    """Optional body for the embed-template lane. ``direct_recipient_id`` overrides which template
    recipient the public signer assumes; omitted → resolved server-side (the non-Provider recipient)."""

    direct_recipient_id: int | None = None


class MandateEmbedTemplateOriginated(BaseModel):
    """The embed-template lane result — PARALLEL to MandatePrefilledOriginated, NOT a replacement.

    Unlike the prefill lane (which mints a Documenso document NOW and returns a per-document
    ``signing_token``), this enables a DIRECT LINK on the draft's template and returns the template's
    reusable ``direct_token``. The SPA mounts ``<EmbedDirectTemplate token={direct_token}
    host={documenso_host} externalId={external_id} email name>``; the signer self-identifies and
    Documenso creates the document AT completion (source ``TEMPLATE_DIRECT_LINK``). The embed's
    ``onDocumentCompleted`` returns the numeric document id; the existing
    ``/sign-state/{opportunity_id}/{document_id}`` surface then tracks it (gate: externalId ==
    opportunity_id). ``recipient_email``/``recipient_name`` are optional embed prefill (the signer may
    still change them). ``status`` is ``"ready"`` — no document exists until someone signs."""

    direct_token: str
    documenso_host: str
    embed_url: str
    external_id: str
    opportunity_id: str
    direct_recipient_id: int | None = None
    recipient_email: str | None = None
    recipient_name: str | None = None
    status: str = "ready"


class MandateStagingDraft(BaseModel):
    """The opportunity's staging draft as the prep page loads it — the selected template, its
    archetype, and the operator-entered per-deal values. Returned by the by-opportunity read so the
    operator can resume editing what they staged off-screen."""

    id: str
    documenso_template_id: str | None = None
    archetype_id: str | None = None
    prefill_values: dict[str, str] = {}
    status: str | None = None


class MandateStagingUpsert(BaseModel):
    """The prep page's save payload — the selected template + the entered per-deal values. The org
    and archetype are resolved server-side from the template, never trusted from the client."""

    documenso_template_id: str
    prefill_values: dict[str, str] = {}
