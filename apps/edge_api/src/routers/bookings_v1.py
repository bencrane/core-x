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

import logging

from ..bookings import queries
from ..bookings.models import BookingDetail, BookingSummary
from ..company_profiles import queries as snapshot_queries
from ..db import get_db_connection
from ..service_token import require_service_token

logger = logging.getLogger(__name__)

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
        # The dossier content is resolved by domain (never email); None until the company is enriched.
        profile = (
            await queries.get_profile_by_domain(conn, booking.domain) if booking.domain else None
        )
        # The operator's latest saved snapshot wins when present (the page seeds from it). Isolated:
        # any miss (no snapshot, table not yet applied, lookup error) collapses to the seed and
        # NEVER breaks the booking read.
        latest_snapshot = None
        if booking.domain:
            try:
                latest_snapshot = await snapshot_queries.get_latest_by_domain(conn, booking.domain)
            except Exception as exc:  # noqa: BLE001
                logger.warning("snapshot lookup failed for %s (%s); using seed", booking.domain, exc)
                try:
                    await conn.rollback()
                except Exception:
                    pass
    return BookingDetail.from_row(booking, profile, latest_snapshot)
