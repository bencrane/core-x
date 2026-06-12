"""Opportunity data model — the CRM opportunity projection (``business.opportunities``).

An opportunity is materialized on a booking (one per ``source_booking_id``) and is the
first-class pipeline object the Pipeline tab lists. It joins to its ``account`` (company)
and ``contact`` (person) for display, and back to ``corex.bookings`` for the booked date.
Read-only here: the list surface serves the operator cockpit.
"""
from __future__ import annotations

import datetime as _dt

from pydantic import BaseModel


class Opportunity(BaseModel):
    """A joined opportunity row (opportunity + account + contact + source booking)."""

    opportunity_id: str
    status: str
    created_at: _dt.datetime | None = None
    company_name: str | None = None
    domain: str | None = None
    first_name: str | None = None
    last_name: str | None = None
    email: str | None = None
    title: str | None = None
    source_booking_id: str | None = None
    booked_at: _dt.datetime | None = None


class OpportunitySummary(BaseModel):
    """Operator list row for the Pipeline tab. Datetimes are ISO strings at the
    projection boundary (the consumer renders them, never computes on them)."""

    opportunity_id: str
    status: str
    created_at: str | None
    company_name: str | None
    domain: str | None
    first_name: str | None
    last_name: str | None
    email: str | None
    title: str | None
    source_booking_id: str | None
    booked_at: str | None

    @classmethod
    def from_row(cls, o: "Opportunity") -> "OpportunitySummary":
        return cls(
            opportunity_id=o.opportunity_id,
            status=o.status,
            created_at=o.created_at.isoformat() if o.created_at else None,
            company_name=o.company_name,
            domain=o.domain,
            first_name=o.first_name,
            last_name=o.last_name,
            email=o.email,
            title=o.title,
            source_booking_id=o.source_booking_id,
            booked_at=o.booked_at.isoformat() if o.booked_at else None,
        )
