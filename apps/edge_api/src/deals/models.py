"""Deal data model — the deals projection (``business.deals`` + ``business.deal_details``).

A deal is the first-class pipeline object: one per account (``uq_deals_account``), grounded
in an organization, carrying a public 8-char ``deal_handle``. For the operator list it joins
to its company (``deals.company_name``/``company_domain``), the account's primary contact
(person), and back to ``corex.bookings`` (via ``last_booking_id``) for the booked date.
Read-only here: the list surface serves the operator cockpit (Applications / Research).
"""
from __future__ import annotations

import datetime as _dt

from pydantic import BaseModel


class Deal(BaseModel):
    """A joined deal row (deal + account's primary contact + most-recent booking)."""

    deal_id: str
    deal_handle: str
    status: str
    created_at: _dt.datetime | None = None
    company_name: str | None = None
    domain: str | None = None
    first_name: str | None = None
    last_name: str | None = None
    email: str | None = None
    title: str | None = None
    last_booking_id: str | None = None
    booked_at: _dt.datetime | None = None


class DealSummary(BaseModel):
    """Operator list row for the Applications / Research tabs. Datetimes are ISO strings at
    the projection boundary (the consumer renders them, never computes on them)."""

    deal_id: str
    deal_handle: str
    status: str
    created_at: str | None
    company_name: str | None
    domain: str | None
    first_name: str | None
    last_name: str | None
    email: str | None
    title: str | None
    last_booking_id: str | None
    booked_at: str | None

    @classmethod
    def from_row(cls, d: "Deal") -> "DealSummary":
        return cls(
            deal_id=d.deal_id,
            deal_handle=d.deal_handle,
            status=d.status,
            created_at=d.created_at.isoformat() if d.created_at else None,
            company_name=d.company_name,
            domain=d.domain,
            first_name=d.first_name,
            last_name=d.last_name,
            email=d.email,
            title=d.title,
            last_booking_id=d.last_booking_id,
            booked_at=d.booked_at.isoformat() if d.booked_at else None,
        )


class TemplateOption(BaseModel):
    """A selectable Documenso template for the deal-details editor dropdown, read off the MIRROR.
    ``documenso_id`` is ``business.documenso_envelopes.documenso_id`` — the attach key the editor
    matches and PUTs back (into ``deal_details.default_template_documenso_id``). ``name`` is the
    envelope title. The legacy ``template_uuid``/``documenso_template_id`` fields are retained (empty
    for mirror rows) so the Mandate page's existing contract still resolves until it is repointed."""

    documenso_id: int
    name: str | None = None
    is_default: bool = False
    template_uuid: str = ""
    documenso_template_id: str = ""


class DealDetails(BaseModel):
    """The editable deal_details payload: the deal's contacts + content + attached Documenso template,
    plus the deal-org's selectable templates for the dropdown. ``template_origin`` is derived
    server-side ('default' = the org default, 'operator' = a manual override)."""

    deal_id: str
    deal_handle: str
    company_name: str | None = None
    company_domain: str | None = None
    contacts: list = []            # the deal's contacts (deal_contacts JOIN the person), is_signatory each
    available_contacts: list = []  # account contacts NOT yet on the deal — the add-contact pool
    field_values: dict = {}        # the document's prefilled form-field values (renamed from content)
    default_template_uuid: str | None = None             # LEGACY attach (documenso_templates uuid); kept for Mandate
    default_template_documenso_id: int | None = None     # MIRROR attach — the live key (documenso_envelopes.documenso_id)
    template_origin: str = "default"
    available_templates: list[TemplateOption] = []


class DealDetailsUpdate(BaseModel):
    """PUT body — ``contacts`` is the desired deal_contacts set [{contact_id, is_signatory}];
    ``field_values`` + ``default_template_uuid`` land on deal_details. ``template_origin`` is derived
    server-side; person identity (business.contacts) is not edited here."""

    contacts: list = []
    field_values: dict = {}
    default_template_documenso_id: int | None = None


class DealOriginated(BaseModel):
    """The originate result — a Documenso document instantiated FROM the deal's attached template
    (template-use lane) with the deal's ``field_values`` prefilled, then distributed
    (``distributionMethod:NONE``) so it lands ``PENDING`` (signable, no email sent). The prospect
    signing link is ``/p/m/{deal_handle}/{document_id}``: ``deal_handle`` is the public 8-char handle
    stamped as the envelope's ``externalId`` (the access capability + sign-gate anchor) and the numeric
    ``document_id`` is the disambiguator behind it. ``envelope_id`` (the prefixed handle) is retained
    for continuity; ``signing_token`` is the prospect's client token."""

    envelope_id: str
    document_id: int | None = None
    deal_handle: str
    signing_token: str | None = None
    sign_link: str
    status: str = "pending"
    documenso_host: str
