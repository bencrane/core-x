"""Booking data model — the cal.com-derived booking projection.

A ``corex.bookings`` row is the normalized form of a cal.com booking, anchored on the
stable ``ical_uid``. ``domain`` is the canonical resolution key that links a booking to
its company dossier downstream (never email). Read surfaces only for now: the Pipeline
tab lists bookings; the booking-profile page reads one; enrichment/origination land later.
"""
from __future__ import annotations

import datetime as _dt

from pydantic import BaseModel


class Booking(BaseModel):
    """The full persisted ``corex.bookings`` row. The list query selects a lean subset
    (the rest default to None); the detail query selects everything."""

    booking_id: str
    ical_uid: str | None = None
    cal_event_uid: str
    cal_booking_id: int | None = None
    event_type_id: int | None = None
    first_name: str | None = None
    last_name: str | None = None
    email: str | None = None
    company_name: str | None = None
    domain: str | None = None
    title: str | None = None
    status: str
    start_time: _dt.datetime | None = None
    end_time: _dt.datetime | None = None
    booked_at: _dt.datetime | None = None
    created_at: _dt.datetime | None = None
    updated_at: _dt.datetime | None = None


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


class BookingDetail(BaseModel):
    """The booking-profile page projection — the full cal-derived record. Carries the
    system identifiers + the meeting window; enrichment fields layer on later."""

    booking_id: str
    ical_uid: str | None
    cal_event_uid: str
    cal_booking_id: int | None
    event_type_id: int | None
    first_name: str | None
    last_name: str | None
    email: str | None
    company_name: str | None
    domain: str | None
    title: str | None
    status: str
    start_time: str | None
    end_time: str | None
    booked_at: str | None
    created_at: str | None
    updated_at: str | None

    @classmethod
    def from_row(cls, b: Booking) -> "BookingDetail":
        def _iso(v: _dt.datetime | None) -> str | None:
            return v.isoformat() if v else None

        return cls(
            booking_id=b.booking_id,
            ical_uid=b.ical_uid,
            cal_event_uid=b.cal_event_uid,
            cal_booking_id=b.cal_booking_id,
            event_type_id=b.event_type_id,
            first_name=b.first_name,
            last_name=b.last_name,
            email=b.email,
            company_name=b.company_name,
            domain=b.domain,
            title=b.title,
            status=b.status,
            start_time=_iso(b.start_time),
            end_time=_iso(b.end_time),
            booked_at=_iso(b.booked_at),
            created_at=_iso(b.created_at),
            updated_at=_iso(b.updated_at),
        )
