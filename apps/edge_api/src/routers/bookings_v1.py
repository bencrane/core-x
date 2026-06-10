"""Bookings — the Pipeline tab's data source + the booking-profile read.

  GET /api/v1/bookings            service-token — operator list (most recent first)
  GET /api/v1/bookings/{id}       service-token — one booking (the profile page)

Read-only for now: cal.com bookings land in ``corex.bookings`` via the webhook
consumer; these surfaces serve them to the operator cockpit. Enrichment + origination
(the dossier/mandate path) attach in later phases. Service-token gated — the
platform-api BFF brokers it with the operator session.
"""
from __future__ import annotations

import uuid as _uuid

from fastapi import APIRouter, Depends, HTTPException

from ..bookings import queries
from ..bookings.models import BookingDetail, BookingSummary
from ..db import get_db_connection
from ..service_token import require_service_token

router = APIRouter(prefix="/api/v1/bookings", tags=["bookings"])


@router.get("", dependencies=[Depends(require_service_token)])
async def list_bookings(limit: int = 100) -> list[BookingSummary]:
    async with get_db_connection() as conn:
        rows = await queries.list_recent(conn, min(max(limit, 1), 500))
    return [BookingSummary.from_row(b) for b in rows]


@router.get("/{booking_id}", dependencies=[Depends(require_service_token)])
async def get_booking(booking_id: str) -> BookingDetail:
    """One booking by its uuid — the booking-profile page's data source."""
    try:
        _uuid.UUID(booking_id)
    except ValueError:
        raise HTTPException(status_code=404, detail="booking not found")
    async with get_db_connection() as conn:
        booking = await queries.get_by_id(conn, booking_id)
    if booking is None:
        raise HTTPException(status_code=404, detail="booking not found")
    return BookingDetail.from_row(booking)
