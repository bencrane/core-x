"""MIRROR-template DEFAULT picker — list mirror templates flagged with the Confirm & Originate default,
and SET the default.

  GET  /api/v1/documenso-template-defaults   service-token — mirror TEMPLATE rows (type='template',
                                                              non-deleted), each flagged is_default
  POST /api/v1/documenso-template-defaults   service-token — mark one mirror template as the default

The LIST reads business.documenso_envelopes (the verbatim mirror) LEFT JOIN the operator-owned
business.documenso_template_defaults — it NEVER writes the mirror. SET-DEFAULT is the SOLE writer of
business.documenso_template_defaults (the projector / on-demand re-grab never touch it). Distinct from
the LEGACY /api/v1/documenso-templates picker (the business.documenso_templates registry): mirror-path
templates like 14503 are NOT in that registry, which is exactly why this surface exists. Service-token
gated — the platform-api BFF brokers it.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from ..db import get_db_connection
from ..documenso_template_defaults import queries
from ..documenso_template_defaults.models import DefaultTemplateRow, SetDefaultRequest
from ..service_token import require_service_token

router = APIRouter(
    prefix="/api/v1/documenso-template-defaults",
    tags=["documenso-template-defaults"],
    dependencies=[Depends(require_service_token)],
)


@router.get("")
async def list_template_defaults() -> list[DefaultTemplateRow]:
    """Every MIRROR template (non-deleted), newest sync first, each flagged with whether it is the
    operator's Confirm & Originate default."""
    async with get_db_connection() as conn:
        rows = await queries.list_templates_with_default(conn)
    return [DefaultTemplateRow(**r) for r in rows]


@router.post("")
async def set_template_default(body: SetDefaultRequest) -> dict[str, int]:
    """Mark ``documenso_id`` as the operator's single Confirm & Originate default. 404 when the target
    is not a live, non-deleted template in the mirror."""
    async with get_db_connection() as conn:
        ok = await queries.set_default(conn, body.documenso_id)
    if not ok:
        raise HTTPException(
            status_code=404,
            detail="template not found in the mirror, or not a live template",
        )
    return {"default": body.documenso_id}
