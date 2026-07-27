"""Internal equipment-outreach ledger writes — Trigger.dev-facing.

  POST /internal/equipment-outreach/pushes   trigger-secret — record Clay pushes

Called by the ``equipment-outreach-push`` Trigger.dev task after a successful
Clay-webhook delivery, via ``callHqx``. Gated by ``require_trigger_secret`` —
the same ``/internal/*`` contract as deals/cal. Idempotent: ON CONFLICT
(person_key, campaign_id) DO NOTHING, so the task's retry policy and duplicate
deliveries never double-count. catalyst_api's /equipment-audience/select reads
this table as its anti-join; the read gateway never writes it (doctrine).
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from ..db import get_db_connection
from ..trigger_secret import require_trigger_secret

router = APIRouter(prefix="/equipment-outreach", tags=["internal"])

_MAX_ROWS = 5000


class PushRow(BaseModel):
    person_key: str = Field(min_length=1, max_length=512)
    macro_region: str | None = None


class PushesRequest(BaseModel):
    campaign_id: str = Field(min_length=1, max_length=256)
    batch_label: str | None = Field(default=None, max_length=256)
    rows: list[PushRow]


@router.post("/pushes", dependencies=[Depends(require_trigger_secret)])
async def record_pushes(body: PushesRequest) -> dict:
    if not body.rows:
        raise HTTPException(status_code=400, detail="rows must be non-empty")
    if len(body.rows) > _MAX_ROWS:
        raise HTTPException(status_code=400, detail=f"rows capped at {_MAX_ROWS} per call")
    inserted = 0
    async with get_db_connection() as conn:
        for r in body.rows:
            cur = await conn.execute(
                "INSERT INTO ops.equipment_outreach_pushes"
                " (person_key, campaign_id, macro_region, batch_label)"
                " VALUES (%s, %s, %s, %s)"
                " ON CONFLICT (person_key, campaign_id) DO NOTHING",
                (r.person_key, body.campaign_id, r.macro_region, body.batch_label),
            )
            inserted += cur.rowcount or 0
        await conn.commit()
    return {"requested": len(body.rows), "inserted": inserted,
            "duplicates": len(body.rows) - inserted}
