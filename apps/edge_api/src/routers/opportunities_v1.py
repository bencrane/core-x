"""Opportunities — the Pipeline tab's data source.

  GET /api/v1/opportunities   service-token — operator list (most recent first)

Read-only for now: opportunities are materialized on a booking into
``business.opportunities``; this surface serves them to the operator cockpit. The
opportunity↔deal association and mutations land in later phases. Service-token gated —
the platform-api BFF brokers it with the operator session.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends

from ..db import get_db_connection
from ..opportunities import queries
from ..opportunities.models import OpportunitySummary
from ..service_token import require_service_token

router = APIRouter(prefix="/api/v1/opportunities", tags=["opportunities"])


@router.get("", dependencies=[Depends(require_service_token)])
async def list_opportunities(limit: int = 100) -> list[OpportunitySummary]:
    async with get_db_connection() as conn:
        rows = await queries.list_recent(conn, min(max(limit, 1), 500))
    return [OpportunitySummary.from_row(o) for o in rows]
