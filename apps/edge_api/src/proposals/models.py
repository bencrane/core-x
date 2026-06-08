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

# ── The fixed Transaction Success Fee schedule (baked into the agreement body) ────────────
# These do NOT vary per deal — they are surfaced to the proposal page for the headline only.
SUCCESS_FEE_TIERS: list[dict[str, str]] = [
    {"tier": "First $1,000,000 of Enterprise Value", "rate": "5.0%"},
    {"tier": "Second $1,000,000 of Enterprise Value", "rate": "4.0%"},
    {"tier": "Third $1,000,000 of Enterprise Value", "rate": "3.0%"},
    {"tier": "Fourth $1,000,000 of Enterprise Value", "rate": "2.0%"},
    {"tier": "All Enterprise Value Exceeding $4,000,000", "rate": "1.5%"},
]

ProposalStatus = str  # one of: draft|sent|opened|signed|completed|rejected|voided


def format_usd(cents: int) -> str:
    """Minor-units → display string. Drops the decimal when whole (``$25,000``)."""
    whole, frac = divmod(int(cents), 100)
    return f"${whole:,}" if frac == 0 else f"${whole:,}.{frac:02d}"


class ProposalCreate(BaseModel):
    """The intake-form contract a platform-app BFF POSTs to mint a proposal.

    ``quarterly_total_cents`` is optional — when omitted it is computed as 3× the monthly
    infrastructure fee (the agreement invoices every three months in advance).
    """

    client_name: str = Field(..., min_length=1)          # institutional entity (<<clientName>>)
    client_signer_name: str = Field(..., min_length=1)   # the person (<<clientSignerName>>)
    client_email: str = Field(..., min_length=3)         # the Documenso recipient
    client_title: str | None = None                      # (<<clientTitle>>)
    effective_date: _dt.date | None = None               # (<<effectiveDate>>); defaults to today
    monthly_fee_cents: int = Field(..., gt=0)            # (<<monthlyFee>>)
    quarterly_total_cents: int | None = Field(default=None, gt=0)  # (<<quarterlyTotal>>)
    rs_signer_name: str | None = None                    # (<<rsName>>); defaults from config
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
    client: dict[str, str | None]
    effective_date: str
    monthly_fee: str
    quarterly_total: str
    success_fee_tiers: list[dict[str, str]]
    signing_token: str | None
    signed_pdf_url: str | None
    created_at: str | None

    @classmethod
    def from_row(cls, p: Proposal) -> "ProposalPublic":
        return cls(
            ref=p.ref,
            status=p.status,
            client={
                "name": p.client_name,
                "signer_name": p.client_signer_name,
                "title": p.client_title,
            },
            effective_date=p.effective_date.isoformat(),
            monthly_fee=format_usd(p.monthly_fee_cents),
            quarterly_total=format_usd(p.quarterly_total_cents),
            success_fee_tiers=SUCCESS_FEE_TIERS,
            signing_token=p.documenso_client_token,
            signed_pdf_url=p.signed_pdf_url,
            created_at=p.created_at.isoformat() if p.created_at else None,
        )


class ProposalSummary(BaseModel):
    """Operator list row."""

    ref: str
    client_name: str
    client_signer_name: str
    status: ProposalStatus
    monthly_fee: str
    created_at: str | None
