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


class MandateSigner(BaseModel):
    """One signer link on the instantiated document — the identity (name/email/role) plus the signing
    token that drives that party's embed. The engagement template carries TWO signers (participant +
    provider), so the BFF renders one link per recipient."""

    name: str | None = None
    email: str | None = None
    role: str | None = None
    signing_token: str | None = None


class MandateDraftConfirmed(BaseModel):
    """The direct-to-documenso originate result — the envelope id (the prospect-link capability)
    plus the signer tokens + host that drive the embedded signers. STATELESS: nothing is persisted on
    the draft, so the envelope id carried in the link is the only handle back to the document.

    ``recipients`` surfaces EVERY signer (participant + provider) with full identity + token — the BFF
    keys off role/email, not order. The legacy ``signing_token`` (the first SIGNER-role recipient's
    token) is retained for back-compat; prefer ``recipients`` for new consumers."""

    envelope_id: str
    signing_token: str | None
    documenso_host: str
    recipients: list[MandateSigner] = []


class MandateDraftDocument(BaseModel):
    """The public prospect read for a confirmed mandate — the signer token + status the embed needs,
    re-read live from Documenso by envelope id."""

    signing_token: str | None
    documenso_host: str
    status: str | None


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
