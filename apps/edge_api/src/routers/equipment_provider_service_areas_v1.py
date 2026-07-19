"""Equipment-provider service-area (geography) research — DOMAIN-keyed raw landing surface.

Endpoints (mounted at ``/api/v1/equipment/service-areas-by-domain``, service-token gated):
  POST /land   → land ONE research payload for one company domain, idempotent
  GET  /stats  → row / distinct-domain / distinct-record counts

The sibling of the UEI-keyed ``/api/v1/equipment/service-areas`` surface for the
beyond-SAM provider plane (outbound roster
``hq/rosters/2026-07-18-equipment-providers-beyond-sam-1785.csv``) — these firms
have no UEI; the company domain is the identity and ``domain_norm`` the canonical
bridge to equipment_provider / equipment_matchmaking / firmographics_blitz.

WIRE CONTRACT. One record per request; the domain travels as a TOP-LEVEL field and
the payload object rides under ``raw_payload``, verbatim::

    { "company_domain": "wyomingrents.com", "raw_payload": { "summary": "...", "serviceAreas": [...] } }

STORAGE — RAW ONLY, NO EXPLODE. raw_payload (jsonb) is stored EXACTLY as sent.
Normalization (serviceAreas → states/centroids/radii) is a downstream
Lance-materializer concern, never this surface's.

GRAIN. PK = record_id = sha256(domain_norm + canonical raw_payload). APPEND-ONLY
HISTORY: a re-research of the same firm hashes differently → new immutable row; a
byte-identical re-send collapses to the same record_id (first-write-wins).
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

router = APIRouter(prefix="/api/v1/equipment/service-areas-by-domain",
                   tags=["equipment-service-areas-by-domain"])

_SOURCE = "equipment_provider_service_areas"

# ── normalization (mirrors equipment_provider_v1 / firmographics_blitz) ──────
_SCHEME_RE = re.compile(r"^https?://", flags=re.I)
_WWW_RE = re.compile(r"^www\.", flags=re.I)
_PATH_RE = re.compile(r"/.*$")
_TRAIL_DOTS_RE = re.compile(r"\.+$")


def _normalize_domain(raw: Any) -> str | None:
    if not isinstance(raw, str):
        return None
    s = raw.strip().lower()
    if not s:
        return None
    s = _SCHEME_RE.sub("", s)
    s = _WWW_RE.sub("", s)
    s = _PATH_RE.sub("", s)
    s = _TRAIL_DOTS_RE.sub("", s)
    return s or None


_INSERT_SQL = (
    "INSERT INTO gtm.equipment_provider_service_areas "
    "(record_id, company_domain, domain_norm, source, raw_payload) "
    "VALUES (%s, %s, %s, %s, %s) "
    "ON CONFLICT (record_id) DO NOTHING"
)

_STATS_SQL = """
    SELECT count(*)                    AS rows,
           count(DISTINCT domain_norm) AS distinct_domains,
           count(DISTINCT record_id)   AS distinct_records
    FROM gtm.equipment_provider_service_areas
"""


def _record_id(domain_norm: str, rec: dict[str, Any]) -> str:
    canon = json.dumps(rec, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(f"{domain_norm}|{canon}".encode()).hexdigest()


@router.post("/land", dependencies=[Depends(require_service_token)])
async def land(body: dict[str, Any]) -> dict[str, Any]:
    """Land ONE research payload. Body is ``{"company_domain": "...", "raw_payload": {...}}``.
    Stores raw_payload verbatim — no explode."""
    company_domain = body.get("company_domain")
    company_domain = company_domain.strip() if isinstance(company_domain, str) else None
    domain_norm = _normalize_domain(company_domain)
    if not company_domain or not domain_norm:
        logger.warning("equipment service-areas-by-domain land rejected: bad domain=%r",
                       body.get("company_domain"))
        raise HTTPException(status_code=422, detail="company_domain is required (non-empty string)")

    rec = body.get("raw_payload")
    if not isinstance(rec, dict) or not rec:
        raise HTTPException(status_code=422, detail="raw_payload must be a non-empty object")

    record_id = _record_id(domain_norm, rec)
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(_INSERT_SQL,
                              (record_id, company_domain, domain_norm, _SOURCE, Jsonb(rec)))
            landed = cur.rowcount == 1
        await conn.commit()

    return {
        "landed": landed,
        "already_present": not landed,
        "record_id": record_id,
        "domain_norm": domain_norm,
    }


@router.get("/stats", dependencies=[Depends(require_service_token)])
async def stats() -> dict[str, Any]:
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(_STATS_SQL)
            r = await cur.fetchone()
    return {"rows": r[0], "distinct_domains": r[1], "distinct_records": r[2]}
