"""Clay Find Companies — raw landing surface (append-only).

Endpoints (mounted at ``/api/v1/clay/find-companies``, service-token gated):
  POST /land   → land ONE Clay find-companies record (Clay fires one row per request), idempotent
  GET  /stats  → row / distinct-linkedin-url / distinct-domain counts

WIRE CONTRACT. Clay POSTs a single record per request, the object under a ``raw_payload`` key::

    { "raw_payload": { "name": "...", "domain": "...", "linkedin_url": "...", ... } }

No ``records`` array — one row at a time. (A bare object with no ``raw_payload``
wrapper is also accepted, so a Clay misconfig never drops data.)

STORAGE. The record is stored TWO ways, both faithful to source:
  1. ``raw_payload`` (jsonb) — the object EXACTLY as sent. Immutable source of truth; drift-proof.
  2. flat typed columns — a LOSSLESS structural projection of the known fields, stored AS-IS.
     This surface does ZERO normalization. ``linkedin_url`` and ``domain`` are stored VERBATIM —
     no scheme/``www.``/sub-domain/case stripping. Bands (``annual_revenue``, ``size``,
     ``total_funding_amount_range_usd``) are kept as-sent. The only transform on any value is a
     surrounding-whitespace trim (``_s``); semantic normalization (URL canonicalization, band →
     numeric, domain → firmographics FK) is a SEPARATE downstream stage, never done here.
     Array/object fields (``industries``, ``structured_locations``, ``derived_datapoints``) are
     projected verbatim into their own jsonb columns; the scalar leaves of ``resolved_domain``,
     the HQ ``structured_locations`` entry, and ``derived_datapoints`` are exploded into typed
     columns for pushdown filtering.

GRAIN: one row per company. PK = ``record_id`` = sha256(company_key), where company_key is the
VERBATIM identity, degrading by source-key strength:
    linkedin_url → domain → linkedin_company_id → clay_company_id
so a record lacking the LinkedIn handle still lands under the next-strongest id, and a record with
NONE of these (unidentifiable) is rejected (422) rather than landed keyless. Because the key is the
raw string, trailing-slash / ``www.`` / case variants of the same LinkedIn URL land as DISTINCT
rows — deduping those is a downstream normalization concern, by design. ``ON CONFLICT DO NOTHING``
makes byte-identical re-sends idempotent (first-write-wins; raw_payload is immutable).
"""
from __future__ import annotations

import hashlib
import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from psycopg.types.json import Jsonb

from ..db import get_db_connection
from ..service_token import require_service_token

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/clay/find-companies", tags=["clay-find-companies"])


def _sha(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _s(v: Any) -> str | None:
    """Verbatim text projection — trims surrounding whitespace only, never interprets."""
    return v.strip() if isinstance(v, str) and v.strip() else None


def _bigint(v: Any) -> int | None:
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, str) and v.strip().lstrip("-").isdigit():
        return int(v.strip())
    return None


def _bool(v: Any) -> bool | None:
    """Verbatim boolean projection — only true JSON booleans; never coerces strings/ints."""
    return v if isinstance(v, bool) else None


def _obj(rec: dict[str, Any], key: str) -> dict[str, Any]:
    o = rec.get(key)
    return o if isinstance(o, dict) else {}


def _jsonb(v: Any) -> Jsonb | None:
    """Project a known array/object field verbatim into its own jsonb column. NULL when the key
    is absent or not a container, so a scalar/typo never lands as a structural column."""
    return Jsonb(v) if isinstance(v, (list, dict)) else None


def _hq_location(rec: dict[str, Any]) -> dict[str, Any]:
    """The headquarters entry of structured_locations[] (is_headquarters=true), else the first
    entry, else {}. A lossless pick — the full array is preserved verbatim in its own jsonb column."""
    locs = rec.get("structured_locations")
    if not isinstance(locs, list):
        return {}
    first: dict[str, Any] = {}
    for loc in locs:
        if not isinstance(loc, dict):
            continue
        if loc.get("is_headquarters") is True:
            return loc
        if not first:
            first = loc
    return first


def _company_key(linkedin_url: str | None, domain: str | None, rec: dict[str, Any]) -> str | None:
    """The company's VERBATIM identity, degrading by source-key strength. No normalization — the
    raw string is the key. Prefix-tagged so a URL, a domain, and a numeric id can never collide.
    None ⇒ unidentifiable record."""
    if linkedin_url:
        return "li:" + linkedin_url
    if domain:
        return "dom:" + domain
    lci = _bigint(rec.get("linkedin_company_id"))
    if lci is not None:
        return "lci:" + str(lci)
    ccid = _bigint(rec.get("clay_company_id"))
    if ccid is not None:
        return "ccid:" + str(ccid)
    return None


# Column order is the single source of truth for the INSERT placeholders below.
_COLS = (
    "record_id", "linkedin_url", "domain", "clay_company_id", "linkedin_company_id",
    "name", "size", "company_type", "industry", "country", "location",
    "description", "annual_revenue", "total_funding_amount_range_usd",
    "domain_resolved", "domain_is_live", "domain_redirects",
    "hq_city", "hq_state", "hq_region", "hq_country_iso", "hq_postal_code",
    "derived_business_stage", "derived_scale_scope", "derived_pattern_tags", "derived_description",
    "industries", "structured_locations", "derived_datapoints",
    "source", "raw_payload",
)
_INSERT_SQL = (
    f"INSERT INTO gtm.clay_find_companies ({', '.join(_COLS)}) "
    f"VALUES ({', '.join(['%s'] * len(_COLS))}) "
    f"ON CONFLICT (record_id) DO NOTHING"
)

_STATS_SQL = """
    SELECT count(*)                       AS rows,
           count(DISTINCT linkedin_url)   AS distinct_linkedin_urls,
           count(DISTINCT domain)         AS distinct_domains
    FROM gtm.clay_find_companies
"""

_SOURCE = "clay_find_companies"


def _to_row(rec: dict[str, Any]) -> tuple | None:
    """Project one Clay record → the full column tuple. None if it carries no resolvable identity
    (no linkedin_url, domain, linkedin_company_id, or clay_company_id)."""
    linkedin_url = _s(rec.get("linkedin_url"))
    domain = _s(rec.get("domain"))
    company_key = _company_key(linkedin_url, domain, rec)
    if company_key is None:
        return None
    rd, hq, dd = _obj(rec, "resolved_domain"), _hq_location(rec), _obj(rec, "derived_datapoints")
    return (
        _sha(company_key), linkedin_url, domain,
        _bigint(rec.get("clay_company_id")), _bigint(rec.get("linkedin_company_id")),
        _s(rec.get("name")), _s(rec.get("size")), _s(rec.get("type")), _s(rec.get("industry")),
        _s(rec.get("country")), _s(rec.get("location")),
        _s(rec.get("description")), _s(rec.get("annual_revenue")),
        _s(rec.get("total_funding_amount_range_usd")),
        _s(rd.get("resolved_domain")), _bool(rd.get("is_live")),
        _bool(rd.get("redirects_to_another_domain")),
        _s(hq.get("city")), _s(hq.get("state")), _s(hq.get("region")),
        _s(hq.get("country_iso")), _s(hq.get("postal_code")),
        _s(dd.get("business_stage")), _s(dd.get("scale_scope")),
        _s(dd.get("pattern_tags")), _s(dd.get("description")),
        _jsonb(rec.get("industries")), _jsonb(rec.get("structured_locations")),
        _jsonb(rec.get("derived_datapoints")),
        _SOURCE, Jsonb(rec),
    )


@router.post("/land", dependencies=[Depends(require_service_token)])
async def land(body: dict[str, Any]) -> dict[str, Any]:
    """Land ONE Clay record. Body is ``{"raw_payload": {...}}`` (one row per request); a bare
    object with no wrapper is also accepted. Stores raw_payload verbatim + exploded columns."""
    rec = body.get("raw_payload")
    if not isinstance(rec, dict):
        # tolerate a bare Clay object sent without the raw_payload wrapper
        rec = body

    row = _to_row(rec)
    if row is None:
        # A 422 is a SILENT drop from Clay's side (4xx is not retried). Log why so a bulk load is
        # reconcilable: count these against /stats to see drops.
        logger.warning(
            "clay find-companies land rejected: unidentifiable record "
            "(had_linkedin_url=%s had_domain=%s had_linkedin_company_id=%s had_clay_company_id=%s "
            "had_name=%s)",
            bool(_s(rec.get("linkedin_url"))), bool(_s(rec.get("domain"))),
            _bigint(rec.get("linkedin_company_id")) is not None,
            _bigint(rec.get("clay_company_id")) is not None, bool(_s(rec.get("name"))),
        )
        raise HTTPException(
            status_code=422,
            detail="unidentifiable: need linkedin_url, domain, linkedin_company_id, or clay_company_id",
        )

    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(_INSERT_SQL, row)
            landed = cur.rowcount == 1
        await conn.commit()

    return {
        "landed": landed,                 # False ⇒ this company was already present
        "already_present": not landed,
        "record_id": row[0],
        "linkedin_url": row[1],
        "domain": row[2],
    }


@router.get("/stats", dependencies=[Depends(require_service_token)])
async def stats() -> dict[str, Any]:
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(_STATS_SQL)
            r = await cur.fetchone()
    return {"rows": r[0], "distinct_linkedin_urls": r[1], "distinct_domains": r[2]}
