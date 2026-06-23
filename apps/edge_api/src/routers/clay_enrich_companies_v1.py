"""Clay Enrich Company — raw landing surface (append-only, raw-only).

Endpoints (mounted at ``/api/v1/clay/enrich-companies``, service-token gated):
  POST /land   → land ONE Clay enrich-company record (Clay fires one row per request), idempotent
  GET  /stats  → row / distinct-linkedin-url / distinct-record counts

DISTINCT from /api/v1/clay/find-companies. That lands the Clay *Find Companies* discovery grain
into gtm.clay_find_companies (verbatim + exploded typed columns). THIS lands the Clay *Enrich
Company* dossier (a superset payload: org_id / founded / website / specialties / last_refresh /
employee_count / follower_count / locations[].inferred_location) into gtm.clay_enrich_companies.

WIRE CONTRACT. Clay POSTs a single record per request, the object under a ``raw_payload`` key::

    { "raw_payload": { "url": "https://www.linkedin.com/company/bank-ozk", "name": "...", ... } }

A bare object with no ``raw_payload`` wrapper is also accepted, so a Clay misconfig never drops data.

STORAGE — RAW ONLY, NO EXPLODE. raw_payload (jsonb) is stored EXACTLY as sent — the immutable,
drift-proof source of truth. The payload is NOT unfurled into typed columns. The connect key
``company_linkedin_url`` is a STORED generated column over ``raw_payload->>'url'`` (derived by
Postgres, never set here). A record carrying no ``url`` is rejected (422) — LinkedIn URL is the
sole resolution key and a url-less dossier cannot join the lake.

GRAIN. PK = record_id = sha256(canonical raw_payload). APPEND-ONLY HISTORY: a refreshed payload
(new last_refresh / changed fields) hashes differently → new immutable row; a byte-identical
re-send collapses to the same record_id → ``ON CONFLICT DO NOTHING`` (first-write-wins).
"""
from __future__ import annotations

import hashlib
import json
import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from psycopg.types.json import Jsonb

from ..db import get_db_connection
from ..service_token import require_service_token

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/clay/enrich-companies", tags=["clay-enrich-companies"])

_SOURCE = "clay_enrich_companies"

_INSERT_SQL = (
    "INSERT INTO gtm.clay_enrich_companies (record_id, source, raw_payload) "
    "VALUES (%s, %s, %s) "
    "ON CONFLICT (record_id) DO NOTHING"
)

_STATS_SQL = """
    SELECT count(*)                            AS rows,
           count(DISTINCT company_linkedin_url) AS distinct_linkedin_urls,
           count(DISTINCT record_id)            AS distinct_records
    FROM gtm.clay_enrich_companies
"""


def _record_id(rec: dict[str, Any]) -> str:
    """sha256 of the canonical (sorted-key, tight-separator) JSON. Key-order / whitespace variants
    of the same payload collapse to one id; any field change yields a new id (append-only history)."""
    canon = json.dumps(rec, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canon.encode("utf-8")).hexdigest()


def _url(rec: dict[str, Any]) -> str | None:
    v = rec.get("url")
    return v.strip() if isinstance(v, str) and v.strip() else None


@router.post("/land", dependencies=[Depends(require_service_token)])
async def land(body: dict[str, Any]) -> dict[str, Any]:
    """Land ONE Clay enrich-company record. Body is ``{"raw_payload": {...}}`` (one row per request);
    a bare object with no wrapper is also accepted. Stores raw_payload verbatim — no explode."""
    rec = body.get("raw_payload")
    if not isinstance(rec, dict):
        # tolerate a bare Clay object sent without the raw_payload wrapper
        rec = body

    url = _url(rec)
    if url is None:
        # A 422 is a SILENT drop from Clay's side (4xx is not retried). Log so a bulk load is
        # reconcilable against /stats.
        logger.warning(
            "clay enrich-companies land rejected: missing connect key (had_name=%s had_domain=%s)",
            bool(rec.get("name")), bool(rec.get("domain")),
        )
        raise HTTPException(status_code=422, detail="unidentifiable: raw_payload.url (company LinkedIn URL) is required")

    record_id = _record_id(rec)
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(_INSERT_SQL, (record_id, _SOURCE, Jsonb(rec)))
            landed = cur.rowcount == 1
        await conn.commit()

    return {
        "landed": landed,                 # False ⇒ this exact payload was already present
        "already_present": not landed,
        "record_id": record_id,
        "company_linkedin_url": url,
    }


@router.get("/stats", dependencies=[Depends(require_service_token)])
async def stats() -> dict[str, Any]:
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(_STATS_SQL)
            r = await cur.fetchone()
    return {"rows": r[0], "distinct_linkedin_urls": r[1], "distinct_records": r[2]}
