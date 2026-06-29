"""Documenso envelope MIRROR — LIST the mirrored templates + on-demand RE-GRAB.

  GET  /api/v1/documenso-envelopes/templates          the mirrored TEMPLATE rows (verbatim mirror)
  POST /api/v1/documenso-envelopes/{documenso_id}/resync   re-grab ONE template/envelope, re-mirror it
  POST /api/v1/documenso-envelopes/resync-all         re-grab EVERY mirrored template, sequentially

The LIST reads STRAIGHT off business.documenso_envelopes (the verbatim mirror). The RE-GRAB reuses the
webhook projector's EXACT pull+upsert (``documenso_client.get_envelope`` → ``queries.upsert_envelope``)
— same verbatim contract, no second upsert path. Re-grab NEVER writes
business.documenso_template_configs. Resilient: a per-template Documenso error surfaces as
``{synced: false, error}`` with 200, never a 5xx — a bad template can't poison the batch. Service-token
gated (the platform-api BFF brokers it).
"""
from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from ..db import get_db_connection
from ..documenso_projection import (
    list_template_documenso_ids,
    list_template_mirror,
    resync_template_by_documenso_id,
)
from ..service_token import require_service_token

router = APIRouter(
    prefix="/api/v1/documenso-envelopes",
    tags=["documenso-envelopes"],
    dependencies=[Depends(require_service_token)],
)


class TemplateMirrorRow(BaseModel):
    """One mirrored TEMPLATE envelope, projected off the verbatim mirror row."""

    documenso_id: int
    title: str | None = None
    status: str | None = None
    field_count: int
    recipient_count: int
    synced_at: datetime | None = None


class ResyncResult(BaseModel):
    """The outcome of one re-grab. ``synced`` false carries ``error``; ``field_count`` is present only
    on success (the live envelope's field count)."""

    documenso_id: int
    synced: bool
    field_count: int | None = None
    error: str | None = None


class ResyncAllResult(BaseModel):
    """The batch re-grab outcome: ``requested`` rows walked, ``synced`` succeeded, per-row ``results``."""

    requested: int
    synced: int
    results: list[ResyncResult]


@router.get("/templates")
async def list_templates() -> list[TemplateMirrorRow]:
    """The mirrored TEMPLATE envelopes (non-deleted), newest sync first."""
    async with get_db_connection() as conn:
        rows = await list_template_mirror(conn)
    return [TemplateMirrorRow(**r) for r in rows]


@router.post("/{documenso_id}/resync")
async def resync_template(documenso_id: int) -> ResyncResult:
    """RE-GRAB one template/envelope by numeric ``documenso_id`` and re-mirror it verbatim. A Documenso
    error surfaces in ``error`` with ``synced: false`` (HTTP 200) — the re-grab is resilient by design."""
    result = await resync_template_by_documenso_id(documenso_id)
    return ResyncResult(**result)


@router.post("/resync-all")
async def resync_all_templates() -> ResyncAllResult:
    """RE-GRAB every mirrored TEMPLATE, SEQUENTIALLY (one Documenso pull at a time — no fan-out). One
    failing template can't fail the batch: each per-row outcome is collected verbatim."""
    async with get_db_connection() as conn:
        ids = await list_template_documenso_ids(conn)
    results: list[ResyncResult] = []
    for documenso_id in ids:
        result = await resync_template_by_documenso_id(documenso_id)
        results.append(ResyncResult(**result))
    return ResyncAllResult(
        requested=len(ids),
        synced=sum(1 for r in results if r.synced),
        results=results,
    )
