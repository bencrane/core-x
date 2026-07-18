"""Staffing-agency website research — raw landing surface (append-only, raw-only).

Endpoints (mounted at ``/api/v1/staffing/website-research``, service-token gated):
  POST /land   → land ONE research payload for one UEI, idempotent
  GET  /stats  → row / distinct-uei / distinct-record counts

The research grain for the SAM-matched staffing agencies (outbound CSV
``staffing_agencies_sam_matched_1-500_*.csv``): per-agency website/LinkedIn research —
rolesPlaced, placementModel, workCategories, geographiesServed, clearanceAndFederalIntent,
plus reasoning/confidence/stepsTaken — landed into gtm.staffing_website_research.

WIRE CONTRACT. One record per request; the UEI travels as a TOP-LEVEL field (the research
payload does not carry it) and the payload object rides under ``raw_payload``, verbatim::

    { "uei": "L3EGERSE6LN1", "raw_payload": { "rolesPlaced": "...", ... } }

STORAGE — RAW ONLY, NO EXPLODE. raw_payload (jsonb) is stored EXACTLY as sent. Normalization
(geographiesServed → FIPS, rolesPlaced → SOC/SCA via occupation_alias_lookup) is a downstream
Lance-materializer concern, never this surface's.

GRAIN. PK = record_id = sha256(uei + canonical raw_payload). APPEND-ONLY HISTORY: a
re-research of the same agency hashes differently → new immutable row; a byte-identical
re-send collapses to the same record_id → ``ON CONFLICT DO NOTHING`` (first-write-wins).
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from psycopg.types.json import Jsonb

from ..db import get_db_connection
from ..service_token import require_service_token

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/staffing/website-research", tags=["staffing-website-research"])

_SOURCE = "staffing_website_research"

_UEI_RE = re.compile(r"^[A-Z0-9]{12}$")

_INSERT_SQL = (
    "INSERT INTO gtm.staffing_website_research (record_id, uei, source, raw_payload) "
    "VALUES (%s, %s, %s, %s) "
    "ON CONFLICT (record_id) DO NOTHING"
)

_STATS_SQL = """
    SELECT count(*)                 AS rows,
           count(DISTINCT uei)      AS distinct_ueis,
           count(DISTINCT record_id) AS distinct_records
    FROM gtm.staffing_website_research
"""


def _record_id(uei: str, rec: dict[str, Any]) -> str:
    """sha256 over uei + canonical (sorted-key, tight-separator) JSON. Key-order / whitespace
    variants collapse to one id; any field change — or the same payload under a different
    UEI — yields a new id (append-only history)."""
    canon = json.dumps(rec, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(f"{uei}|{canon}".encode()).hexdigest()


@router.post("/land", dependencies=[Depends(require_service_token)])
async def land(body: dict[str, Any]) -> dict[str, Any]:
    """Land ONE research payload. Body is ``{"uei": "...", "raw_payload": {...}}``.
    Stores raw_payload verbatim — no explode."""
    uei = body.get("uei")
    uei = uei.strip().upper() if isinstance(uei, str) else None
    if not uei or not _UEI_RE.match(uei):
        logger.warning("staffing website-research land rejected: bad uei=%r", body.get("uei"))
        raise HTTPException(status_code=422, detail="unidentifiable: top-level uei (12-char SAM UEI) is required")

    rec = body.get("raw_payload")
    if not isinstance(rec, dict) or not rec:
        raise HTTPException(status_code=422, detail="raw_payload must be a non-empty object")

    record_id = _record_id(uei, rec)
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(_INSERT_SQL, (record_id, uei, _SOURCE, Jsonb(rec)))
            landed = cur.rowcount == 1
        await conn.commit()

    return {
        "landed": landed,                 # False ⇒ this exact (uei, payload) was already present
        "already_present": not landed,
        "record_id": record_id,
        "uei": uei,
    }


@router.get("/stats", dependencies=[Depends(require_service_token)])
async def stats() -> dict[str, Any]:
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(_STATS_SQL)
            r = await cur.fetchone()
    return {"rows": r[0], "distinct_ueis": r[1], "distinct_records": r[2]}
