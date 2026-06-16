"""Engagement Mandates — the PARALLEL engagement-document pathway's operator surface.

  GET  /api/v1/engagement-mandates/packages          service-token  the price+term dropdown options
  POST /api/v1/engagement-mandates/{opportunity_id}   service-token  lock a package → enqueue the render
  GET  /api/v1/engagement-mandates/{opportunity_id}   service-token  the mandate's current state

Service-token gated (the rare-structure-hq platform-api BFF brokers it with the operator session).
The POST resolves the package SERVER-SIDE (the client only sends a key), records the selection, and
hands off to the ``engagement-doc-render`` Trigger.dev task — which calls back to the internal render
route. Separate from the existing proposal/engagement_proposals surface.
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException

from ..db import get_db_connection
from ..engagement_docs import kickoff, packages, queries, render, service
from ..engagement_docs.models import GenerateMandateRequest
from ..service_token import require_service_token

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1/engagement-mandates",
    tags=["engagement-mandates"],
    dependencies=[Depends(require_service_token)],
)


@router.get("/packages")
def list_packages() -> list[dict]:
    """The preset price+term options for the Applications action control."""
    return packages.options()


@router.post("/{opportunity_id}")
async def generate_mandate(opportunity_id: str, body: GenerateMandateRequest) -> dict:
    """Stage a new DEAL on an opportunity and enqueue its render. INSERTs a fresh deal row
    (status='pending') — an opportunity may have MANY — fires the Trigger.dev task for THAT deal, and
    stamps the run id. The task renders + creates the DRAFT Documenso document, flipping it to
    'rendered'."""
    pkg = packages.get(body.package_key)
    if pkg is None:
        raise HTTPException(status_code=400, detail=f"unknown package_key: {body.package_key!r}")

    async with get_db_connection() as conn:
        deal = await queries.insert_mandate(
            conn, opportunity_id=opportunity_id, package_key=pkg.key,
            term_fee_cents=pkg.term_fee_cents, duration_months=pkg.duration_months,
            slug=service.SLUG, style=render.style_for(service.SLUG), status="pending",
        )
        await conn.commit()
    mandate_id = deal["id"]

    run_id = await kickoff.trigger_render(mandate_id=mandate_id)
    if run_id is None:
        raise HTTPException(status_code=502, detail="failed to enqueue render (deal recorded)")
    async with get_db_connection() as conn:
        await queries.update_mandate(conn, mandate_id=mandate_id, status="pending", trigger_run_id=run_id)
        await conn.commit()

    return {
        "opportunity_id": opportunity_id,
        "mandate_id": mandate_id,
        "status": "pending",
        "package_key": pkg.key,
        "run_id": run_id,
    }


@router.get("/{opportunity_id}")
async def get_mandate(opportunity_id: str) -> dict:
    """The opportunity's LATEST deal (status, package, artifact, Documenso envelope, last error)."""
    async with get_db_connection() as conn:
        mandate = await queries.get_latest_by_opportunity(conn, opportunity_id)
    if mandate is None:
        raise HTTPException(status_code=404, detail="no mandate for this opportunity")
    return mandate
