"""Internal deal materialization — Trigger.dev-facing.

  POST /internal/deals/materialize   trigger-secret — booking → account+contact+deal

Called by the ``deal-materialize`` Trigger.dev task (one run per new cal booking) via
``callHqx``. Gated by ``require_trigger_secret`` (TRIGGER_SHARED_SECRET) — the same
``/internal/*`` contract the gtm pipeline run-step uses. Idempotent: safe for the task's
retry policy and for cal's duplicate deliveries (the account dedupes on domain and the deal
collapses on ``account_id`` via ``uq_deals_account``). Replaces the retired
``/internal/opportunities/materialize`` producer; this is the seam the DocRaptor render
layers onto later.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, ConfigDict, Field

from ..db import get_db_connection
from ..deals import materialize
from ..trigger_secret import require_trigger_secret

router = APIRouter(prefix="/deals", tags=["internal"])


class MaterializeRequest(BaseModel):
    # The Trigger task sends ``{ "icalUid": ... }``; accept that and the snake_case form.
    model_config = ConfigDict(populate_by_name=True)
    ical_uid: str = Field(alias="icalUid")


@router.post("/materialize", dependencies=[Depends(require_trigger_secret)])
async def materialize_deal(body: MaterializeRequest) -> dict:
    ical_uid = body.ical_uid.strip()
    if not ical_uid:
        raise HTTPException(status_code=400, detail="icalUid is required")
    async with get_db_connection() as conn:
        result = await materialize.materialize_deal_for_booking(conn, ical_uid=ical_uid)
        await conn.commit()
    return result
