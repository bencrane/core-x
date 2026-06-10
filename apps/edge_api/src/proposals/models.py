"""Proposal data model — the single structured source of truth.

One ``Proposal`` renders to TWO artifacts: the native React proposal page (the consumer
app reads the public projection) and the legal PDF (the agreement DocRaptor renders and
Documenso seals). Money is carried in integer minor units (cents) end-to-end; display
strings are derived only at the public-projection boundary.
"""
from __future__ import annotations

import datetime as _dt
from typing import Any

from pydantic import BaseModel, Field

# ── Default Success Fee schedule — the fallback when a template carries none. Per-template
# schedules (business.proposal_templates.success_fee_schedule) override this; it is the seed only.
SUCCESS_FEE_TIERS: list[dict[str, str]] = [
    {"tier": "First $1,000,000 of Enterprise Value", "rate": "5.0%"},
    {"tier": "Second $1,000,000 of Enterprise Value", "rate": "4.0%"},
    {"tier": "Third $1,000,000 of Enterprise Value", "rate": "3.0%"},
    {"tier": "Fourth $1,000,000 of Enterprise Value", "rate": "2.0%"},
    {"tier": "All Enterprise Value Exceeding $4,000,000", "rate": "1.5%"},
]

DEFAULT_DURATION_MONTHS = 6
DEFAULT_BILLING_CADENCE = "upfront_in_full"

# The exec-summary blurb shown on the engagement page when the resolved template carries no
# ``exec_summary`` of its own (NULL column, unpublished template, or registry lookup failure).
# Per-template copy (``business.proposal_templates.exec_summary``) overrides this; it is the
# built-in floor so the public projection is never empty.
DEFAULT_EXEC_SUMMARY = (
    "Rare Structure originates and structures off-market deal flow against your investment "
    "mandate. This engagement deploys dedicated sourcing infrastructure on a success-based fee "
    "schedule — the transaction success fee below applies only to capital that closes and funds."
)

ProposalStatus = str  # one of: draft|sent|opened|signed|completed|rejected|voided


def format_usd(cents: int) -> str:
    """Minor-units → display string. Drops the decimal when whole (``$25,000``)."""
    whole, frac = divmod(int(cents), 100)
    return f"${whole:,}" if frac == 0 else f"${whole:,}.{frac:02d}"


def total_cents(monthly_fee_cents: int, duration_months: int) -> int:
    """Full engagement value = monthly fee × duration. DERIVED, never stored — so it can never
    drift from the two inputs the operator actually sets."""
    return int(monthly_fee_cents) * int(duration_months)


def charge_cents(monthly_fee_cents: int, duration_months: int, billing_cadence: str | None) -> int:
    """Amount billed PER INVOICE, by cadence (independent of duration): ``upfront_in_full`` → the
    whole engagement in one charge; ``monthly`` → one month; ``quarterly`` → three months. Unknown
    cadence falls back to upfront-in-full."""
    cadence = (billing_cadence or DEFAULT_BILLING_CADENCE).lower()
    if cadence == "monthly":
        return int(monthly_fee_cents)
    if cadence == "quarterly":
        return int(monthly_fee_cents) * 3
    return total_cents(monthly_fee_cents, duration_months)


class ProposalCreate(BaseModel):
    """The intake-form contract a platform-app BFF POSTs to mint a proposal.

    Pricing is dynamic-with-defaults: every field below may be omitted, in which case it INHERITS
    the selected template's default (``business.proposal_templates``). The proposal stores the
    resolved actuals; the operator can still override any of them per deal. ``total`` is never sent
    or stored — it derives from ``monthly_fee × duration``.
    """

    template_id: str | None = None                       # published-template slug; None → built-in default
    client_name: str = Field(..., min_length=1)          # institutional entity ({{client_name}})
    client_signer_name: str = Field(..., min_length=1)   # the person ({{client_signer_name}})
    client_email: str = Field(..., min_length=3)         # the Documenso recipient
    client_title: str | None = None                      # ({{client_title}})
    effective_date: _dt.date | None = None               # ({{effective_date}}); defaults to today
    # Pricing config — None ⇒ inherit the template default (never empty on the proposal).
    monthly_fee_cents: int | None = Field(default=None, gt=0)       # ({{monthly_fee}})
    duration_months: int | None = Field(default=None, gt=0)         # ({{duration}}) — engagement term
    billing_cadence: str | None = None                             # ({{billing_cadence}}) — when billed
    success_fee_schedule: list[dict[str, str]] | None = None        # tiered %s; None ⇒ template default
    quarterly_total_cents: int | None = Field(default=None, gt=0)   # deprecated; total derives from duration
    rs_signer_name: str | None = None                    # ({{rs_name}}); defaults from config
    created_by: str | None = None


class Proposal(BaseModel):
    """The full persisted row (backend bookkeeping included)."""

    ref: str
    template_id: str
    client_name: str
    client_signer_name: str
    client_title: str | None
    client_email: str
    effective_date: _dt.date
    monthly_fee_cents: int
    duration_months: int = DEFAULT_DURATION_MONTHS
    billing_cadence: str = DEFAULT_BILLING_CADENCE
    success_fee_schedule: list[dict[str, str]] = Field(default_factory=lambda: list(SUCCESS_FEE_TIERS))
    quarterly_total_cents: int
    rs_signer_name: str
    status: ProposalStatus
    documenso_envelope_id: str | None = None
    documenso_client_token: str | None = None
    signed_pdf_url: str | None = None
    field_values: dict[str, Any] = Field(default_factory=dict)
    created_by: str | None = None
    created_at: _dt.datetime | None = None
    sent_at: _dt.datetime | None = None
    opened_at: _dt.datetime | None = None
    signed_at: _dt.datetime | None = None
    completed_at: _dt.datetime | None = None


class ProposalPublic(BaseModel):
    """The public-ref projection the consumer React page renders. Carries the signing token
    (the ref is the bearer credential for it) and display-formatted money."""

    ref: str
    status: ProposalStatus
    template_label: str = "Strategic Origination Mandate"
    # The engagement-page blurb, resolved from the proposal's template (read-time); falls back to
    # DEFAULT_EXEC_SUMMARY when the template carries none.
    exec_summary: str = DEFAULT_EXEC_SUMMARY
    client: dict[str, str | None]
    effective_date: str
    monthly_fee: str
    duration_months: int
    billing_cadence: str
    total: str                                    # = monthly_fee × duration (derived)
    quarterly_total: str                          # back-compat alias of ``total`` (legacy field name)
    success_fee_tiers: list[dict[str, str]]
    signing_token: str | None
    signed_pdf_url: str | None
    created_at: str | None
    # Payment surface (additive; defaults keep the projection valid before any payment exists).
    payment_status: str = "none"
    amount_due: str = ""
    amount_due_cents: int = 0

    @classmethod
    def from_row(
        cls, p: Proposal, *, payment_status: str = "none", exec_summary: str | None = None,
    ) -> "ProposalPublic":
        full = total_cents(p.monthly_fee_cents, p.duration_months)
        charge = charge_cents(p.monthly_fee_cents, p.duration_months, p.billing_cadence)
        return cls(
            ref=p.ref,
            status=p.status,
            # Template-owned copy wins; empty/NULL/lookup-miss collapses to the built-in floor.
            exec_summary=exec_summary or DEFAULT_EXEC_SUMMARY,
            client={
                "name": p.client_name,
                "signer_name": p.client_signer_name,
                "title": p.client_title,
            },
            effective_date=p.effective_date.isoformat(),
            monthly_fee=format_usd(p.monthly_fee_cents),
            duration_months=p.duration_months,
            billing_cadence=p.billing_cadence,
            total=format_usd(full),
            quarterly_total=format_usd(full),
            success_fee_tiers=p.success_fee_schedule or SUCCESS_FEE_TIERS,
            signing_token=p.documenso_client_token,
            signed_pdf_url=p.signed_pdf_url,
            created_at=p.created_at.isoformat() if p.created_at else None,
            payment_status=payment_status,
            # The amount the client will be debited — derived from the persisted proposal content by
            # billing cadence (upfront_in_full → the full engagement total; monthly/quarterly → that
            # slice). Never hardcoded.
            amount_due=format_usd(charge),
            amount_due_cents=int(charge),
        )


class ProposalSummary(BaseModel):
    """Operator list row."""

    ref: str
    client_name: str
    client_signer_name: str
    status: ProposalStatus
    monthly_fee: str
    created_at: str | None
