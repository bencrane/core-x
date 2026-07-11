"""combo_work_summary_equipment_needs — raw landing sink for LLM equipment-needs
verdicts at the NAICS x PSC combo grain (UPSERT /land).

Endpoint (mounted at ``/api/v1/combo-work-summary-equipment-needs``, service-token gated):
  POST /land   → land ONE verdict; re-send for the same (naics_code, psc_code, model_id)
                 overwrites (UPSERT)

WIRE CONTRACT::

    {
      "naics_code":  "541712",             # REQUIRED
      "psc_code":    "AC12",               # REQUIRED (uppercased on land)
      "raw_payload": { ... the LLM object, EXACTLY as emitted ... },  # REQUIRED (JSON object)
      "model_id":    "gpt-5.4-nano",       # optional — defaults to gpt-5.4-nano
      "source":      "clay"                # optional — defaults to clay
    }

raw_payload is stored verbatim as jsonb — NO projection, NO comma-splitting of the
``response`` equipment list, NO taxonomy normalization. Unfurling is a downstream,
read-time concern. Grain (naics_code, psc_code, model_id): re-ingest UPSERTs; a
different model_id lands as a DISTINCT row.
"""
from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from psycopg.types.json import Jsonb

from ..db import get_db_connection
from ..service_token import require_service_token

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1/combo-work-summary-equipment-needs",
    tags=["combo-work-summary-equipment-needs"],
)


def _s(v: Any) -> str | None:
    return v.strip() if isinstance(v, str) and v.strip() else None


_UPSERT_SQL = """
INSERT INTO gtm.combo_work_summary_equipment_needs
    (naics_code, psc_code, model_id, source, raw_payload)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (naics_code, psc_code, model_id) DO UPDATE
SET source = EXCLUDED.source,
    raw_payload = EXCLUDED.raw_payload,
    landed_at = now()
RETURNING (xmax = 0) AS inserted, landed_at
"""


@router.post("/land", dependencies=[Depends(require_service_token)])
async def land(body: dict[str, Any]) -> dict[str, Any]:
    """Land ONE combo equipment-needs verdict. Body is
    ``{"naics_code": "...", "psc_code": "...", "raw_payload": {...}, "model_id"?, "source"?}``."""
    rec = body.get("raw_payload")
    if not isinstance(rec, dict):
        raise HTTPException(status_code=422, detail="raw_payload must be a JSON object")

    naics_code = _s(body.get("naics_code"))
    if not naics_code:
        raise HTTPException(status_code=422, detail="naics_code is required (non-empty string)")

    psc_code = _s(body.get("psc_code"))
    if not psc_code:
        raise HTTPException(status_code=422, detail="psc_code is required (non-empty string)")
    psc_code = psc_code.upper()

    model_id = _s(body.get("model_id")) or "gpt-5.4-nano"
    source = _s(body.get("source")) or "clay"

    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                _UPSERT_SQL, (naics_code, psc_code, model_id, source, Jsonb(rec))
            )
            row = await cur.fetchone()
        await conn.commit()

    return {
        "landed": True,
        "inserted": bool(row[0]),
        "updated": not bool(row[0]),
        "naics_code": naics_code,
        "psc_code": psc_code,
        "model_id": model_id,
        "source": source,
        "landed_at": row[1].isoformat() if row[1] else None,
    }
