"""Booking data model — the cal.com-derived booking projection.

A ``corex.bookings`` row is the normalized form of a cal.com BOOKING_CREATED event.
``domain`` is the canonical resolution key that links a booking to its company
dossier downstream (never email). Read-only for now: the Pipeline tab lists recent
bookings; enrichment/origination land later.
"""
from __future__ import annotations

import datetime as _dt

from pydantic import BaseModel


class Booking(BaseModel):
    """The full persisted ``corex.bookings`` row."""

    booking_id: str
    cal_event_uid: str
    first_name: str | None = None
    last_name: str | None = None
    email: str | None = None
    company_name: str | None = None
    domain: str | None = None
    title: str | None = None
    status: str
    start_time: _dt.datetime | None = None
    created_at: _dt.datetime | None = None


class BookingSummary(BaseModel):
    """Operator list row for the Pipeline tab. Datetimes are ISO strings at the
    projection boundary (the consumer renders them, never computes on them)."""

    booking_id: str
    cal_event_uid: str
    first_name: str | None
    last_name: str | None
    email: str | None
    company_name: str | None
    domain: str | None
    title: str | None
    status: str
    start_time: str | None
    created_at: str | None

    @classmethod
    def from_row(cls, b: Booking) -> "BookingSummary":
        return cls(
            booking_id=b.booking_id,
            cal_event_uid=b.cal_event_uid,
            first_name=b.first_name,
            last_name=b.last_name,
            email=b.email,
            company_name=b.company_name,
            domain=b.domain,
            title=b.title,
            status=b.status,
            start_time=b.start_time.isoformat() if b.start_time else None,
            created_at=b.created_at.isoformat() if b.created_at else None,
        )
