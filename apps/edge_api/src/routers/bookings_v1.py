"""Bookings — the Pipeline tab's data source.

  GET /api/v1/bookings   service-token — operator list (most recent first)

Read-only for now: cal.com bookings land in ``corex.bookings`` via the webhook
consumer; this surface lists them for the operator cockpit. Enrichment + origination
(the dossier/mandate path) attach in later phases. Service-token gated — the
platform-api BFF brokers it with the operator session.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends

from ..bookings import queries
from ..bookings.models import BookingSummary
from ..db import get_db_connection
from ..service_token import require_service_token

router = APIRouter(prefix="/api/v1/bookings", tags=["bookings"])


@router.get("", dependencies=[Depends(require_service_token)])
async def list_bookings(limit: int = 100) -> list[BookingSummary]:
    async with get_db_connection() as conn:
        rows = await queries.list_recent(conn, min(max(limit, 1), 500))
    return [BookingSummary.from_row(b) for b in rows]
