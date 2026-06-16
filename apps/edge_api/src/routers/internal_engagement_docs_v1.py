"""Internal engagement-document render — Trigger.dev-facing.

  POST /internal/engagement-doc/render   trigger-secret — bind opportunity + package → DocRaptor PDF → R2

Called by the ``engagement-doc-render`` Trigger.dev task via ``callHqx``. Gated by
``require_trigger_secret`` (TRIGGER_SHARED_SECRET) — the same ``/internal/*`` contract as the gtm
pipeline + the opportunity-materialize render. The render result (status='rendered') is committed
before any non-2xx, so a 'failed' mandate persists even when we surface the error to the run.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from ..db import get_db_connection
from ..engagement_docs import service
from ..engagement_docs.models import RenderRequest
from ..trigger_secret import require_trigger_secret

router = APIRouter(prefix="/engagement-doc", tags=["internal"])


@router.post("/render", dependencies=[Depends(require_trigger_secret)])
async def render_mandate(body: RenderRequest) -> dict:
    mandate_id = body.mandate_id.strip()
    if not mandate_id:
        raise HTTPException(status_code=400, detail="mandateId is required")
    async with get_db_connection() as conn:
        result = await service.render_mandate(conn, mandate_id=mandate_id)
        await conn.commit()  # persist 'rendered' OR 'failed' BEFORE surfacing any error
    if result.get("status") == "failed":
        raise HTTPException(status_code=502, detail=result.get("error", "render failed"))
    return result
