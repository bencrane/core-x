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
