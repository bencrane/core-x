"""combo_job_to_be_done — raw landing sink for LLM "to: …" job sentences at the
NAICS x PSC combo grain (UPSERT /land).

Endpoint (mounted at ``/api/v1/combo-job-to-be-done``, service-token gated):
  POST /land   → land ONE sentence; re-send for the same (naics_code, psc_code,
                 model_id) overwrites (UPSERT)

WIRE CONTRACT::

    {
      "naics_code":      "336411",                       # REQUIRED
      "psc_code":        "1510",                         # REQUIRED (uppercased on land)
      "output_sentence": "to: build fixed-wing aircraft",# REQUIRED (non-empty string)
      "model_id":        "gpt-5.4",                      # optional — defaults to gpt-5.4
      "source":          "clay"                          # optional — defaults to clay
    }

output_sentence is stored verbatim — NO trimming beyond surrounding whitespace,
NO normalization into the phrase grammar (downstream, read-time concern).
Grain (naics_code, psc_code, model_id): re-ingest UPSERTs; a different model_id
lands as a DISTINCT row (e.g. a later Opus tightening pass accumulates beside
the GPT batch).
"""
from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException

from ..db import get_db_connection
from ..service_token import require_service_token

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1/combo-job-to-be-done",
    tags=["combo-job-to-be-done"],
)


def _s(v: Any) -> str | None:
    return v.strip() if isinstance(v, str) and v.strip() else None


_UPSERT_SQL = """
INSERT INTO gtm.combo_job_to_be_done
    (naics_code, psc_code, model_id, source, output_sentence)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (naics_code, psc_code, model_id) DO UPDATE
SET source = EXCLUDED.source,
    output_sentence = EXCLUDED.output_sentence,
    landed_at = now()
RETURNING (xmax = 0) AS inserted, landed_at
"""


@router.post("/land", dependencies=[Depends(require_service_token)])
async def land(body: dict[str, Any]) -> dict[str, Any]:
    """Land ONE combo job-to-be-done sentence. Body is
    ``{"naics_code": "...", "psc_code": "...", "output_sentence": "to: ...", "model_id"?, "source"?}``."""
    output_sentence = _s(body.get("output_sentence"))
    if not output_sentence:
        raise HTTPException(status_code=422, detail="output_sentence is required (non-empty string)")

    naics_code = _s(body.get("naics_code"))
    if not naics_code:
        raise HTTPException(status_code=422, detail="naics_code is required (non-empty string)")

    psc_code = _s(body.get("psc_code"))
    if not psc_code:
        raise HTTPException(status_code=422, detail="psc_code is required (non-empty string)")
    psc_code = psc_code.upper()

    model_id = _s(body.get("model_id")) or "gpt-5.4"
    source = _s(body.get("source")) or "clay"

    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                _UPSERT_SQL, (naics_code, psc_code, model_id, source, output_sentence)
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
