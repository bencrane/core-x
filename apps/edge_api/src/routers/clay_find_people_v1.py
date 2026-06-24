"""Clay Find People — raw landing surface (append-only).

Endpoints (mounted at ``/api/v1/clay/find-people``, service-token gated):
  POST /land   → land ONE Clay find-people record (Clay fires one row per request), idempotent
  GET  /stats  → row / distinct-person / distinct-domain counts

WIRE CONTRACT. Clay POSTs a single record per request, the object under a ``raw_payload`` key::

    { "raw_payload": { "url": "...", "name": "...", "domain": "...", ... } }

No ``records`` array — one row at a time. (A bare object with no ``raw_payload``
wrapper is also accepted, so a Clay misconfig never drops data.)

STORAGE. The record is stored TWO ways, both faithful to source:
  1. ``raw_payload`` (jsonb) — the object EXACTLY as sent. Immutable source of truth; drift-proof.
  2. flat typed columns — a LOSSLESS structural projection of the known fields, stored AS-IS.
     NOT normalization: ``matched_job_title`` keeps the verbatim Clay string. Semantic
     normalization (job_title → job_level enum) is a SEPARATE downstream stage, never done here.
The only computed columns are lossless identity keys read server-side from the payload:
    raw_payload.url    → linkedin_url_raw (verbatim; feeds blitz /v2/enrichment/email)
                         → linkedin_url_norm → person_id = sha256(linkedin_url_norm)
    raw_payload.domain → domain_norm (FK → firmographics_blitz.domain_norm)

GRAIN: (person × company). PK = ``sha256(linkedin_url_norm | company_key)`` where company_key
degrades  domain_norm → company_record_id → hash(full payload)  so a missing company never
silently collapses distinct observations. ``ON CONFLICT DO NOTHING`` makes re-sends idempotent.
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

router = APIRouter(prefix="/api/v1/clay/find-people", tags=["clay-find-people"])


# ── Lossless canonicalization (NOT classification) ───────────────────────────
_SCHEME = re.compile(r"^https?://")
_WWW = re.compile(r"^www\.")
_QS = re.compile(r"[?#].*$")
_TRAIL_SLASH = re.compile(r"/+$")
_PATH = re.compile(r"/.*$")
_TRAIL_DOT = re.compile(r"\.+$")


def _norm_linkedin(url: str) -> str:
    s = _SCHEME.sub("", url.strip().lower())
    s = _WWW.sub("", s)
    s = _QS.sub("", s)
    return _TRAIL_SLASH.sub("", s)


def _norm_domain(domain: str) -> str:
    s = _SCHEME.sub("", domain.strip().lower())
    s = _WWW.sub("", s)
    s = _PATH.sub("", s)
    return _TRAIL_DOT.sub("", s)


def _sha(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _s(v: Any) -> str | None:
    """Verbatim text projection — trims whitespace only, never interprets."""
    return v.strip() if isinstance(v, str) and v.strip() else None


def _bigint(v: Any) -> int | None:
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, str) and v.strip().lstrip("-").isdigit():
        return int(v.strip())
    return None


def _obj(rec: dict[str, Any], key: str) -> dict[str, Any]:
    o = rec.get(key)
    return o if isinstance(o, dict) else {}


def _company_fallback(rec: dict[str, Any]) -> str:
    """Company discriminant when domain is absent: prefer Clay's company_record_id, else a
    stable hash of the full payload so every distinct observation stays a distinct row."""
    crid = _s(rec.get("company_record_id"))
    if crid:
        return "crid:" + crid
    return "raw:" + _sha(json.dumps(rec, sort_keys=True, separators=(",", ":")))


# Column order is the single source of truth for the INSERT placeholders below.
_COLS = (
    "record_id", "person_id", "linkedin_url_raw", "linkedin_url_norm", "domain_norm",
    "domain_raw", "full_name", "first_name", "last_name", "location_name", "follower_count",
    "company_table_id", "company_record_id",
    "matched_job_title", "matched_company_name", "matched_start_date",
    "loc_city", "loc_state", "loc_region", "loc_country", "loc_country_iso",
    "latest_experience_title", "latest_experience_company", "latest_experience_start_date",
    "source", "raw_payload",
)
_INSERT_SQL = (
    f"INSERT INTO gtm.clay_find_people ({', '.join(_COLS)}) "
    f"VALUES ({', '.join(['%s'] * len(_COLS))}) "
    f"ON CONFLICT (record_id) DO NOTHING"
)

_STATS_SQL = """
    SELECT count(*)                    AS rows,
           count(DISTINCT person_id)   AS distinct_persons,
           count(DISTINCT domain_norm) AS distinct_domains
    FROM gtm.clay_find_people
"""

_SOURCE = "clay_find_people"

# Person LinkedIn URL → that person's Clay find-people record(s). Keyed on person_id
# (sha256(linkedin_url_norm), indexed) — the SAME derivation the land path writes, so the
# read is parity-exact. Grain is (person × company), so one URL can return several rows
# (the person observed at multiple companies); newest-first, hard-capped.
_BY_LINKEDIN_SQL = """
    SELECT record_id, domain_norm, matched_job_title, matched_company_name,
           landed_at, raw_payload
    FROM gtm.clay_find_people
    WHERE person_id = %s
    ORDER BY landed_at DESC
    LIMIT %s
"""
_BY_LINKEDIN_CAP = 200


def _to_row(rec: dict[str, Any]) -> tuple | None:
    """Project one Clay record → the full column tuple. None if it lacks a usable url."""
    url = rec.get("url")
    if not isinstance(url, str) or not url.strip():
        return None
    li_norm = _norm_linkedin(url)
    if not li_norm:
        return None
    raw_dom = rec.get("domain")
    dom_norm = _norm_domain(raw_dom) if isinstance(raw_dom, str) and raw_dom.strip() else None
    record_id = _sha(f"{li_norm}|{dom_norm or _company_fallback(rec)}")
    me, loc = _obj(rec, "matched_experience"), _obj(rec, "structured_location")
    return (
        record_id, _sha(li_norm), url.strip(), li_norm, dom_norm,
        _s(rec.get("domain")), _s(rec.get("name")), _s(rec.get("first_name")),
        _s(rec.get("last_name")), _s(rec.get("location_name")), _bigint(rec.get("follower_count")),
        _s(rec.get("company_table_id")), _s(rec.get("company_record_id")),
        _s(me.get("job_title")), _s(me.get("company_name")), _s(me.get("start_date")),
        _s(loc.get("city")), _s(loc.get("state")), _s(loc.get("region")),
        _s(loc.get("country")), _s(loc.get("country_iso")),
        _s(rec.get("latest_experience_title")), _s(rec.get("latest_experience_company")),
        _s(rec.get("latest_experience_start_date")),
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
        # A 422 is a SILENT drop from Clay's side (4xx is not retried). Log why — without
        # PII — so a bulk load is reconcilable: count these against /stats to see drops.
        logger.warning(
            "clay find-people land rejected: missing/empty raw_payload.url "
            "(had_domain=%s had_name=%s had_company_record_id=%s)",
            bool(_s(rec.get("domain"))), bool(_s(rec.get("name"))),
            bool(_s(rec.get("company_record_id"))),
        )
        raise HTTPException(status_code=422, detail="missing/empty raw_payload.url")

    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(_INSERT_SQL, row)
            landed = cur.rowcount == 1
        await conn.commit()

    return {
        "landed": landed,                 # False ⇒ this (person, company) was already present
        "already_present": not landed,
        "record_id": row[0],
        "person_id": row[1],
        "domain_norm": row[4],
    }


@router.get("/stats", dependencies=[Depends(require_service_token)])
async def stats() -> dict[str, Any]:
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(_STATS_SQL)
            r = await cur.fetchone()
    return {"rows": r[0], "distinct_persons": r[1], "distinct_domains": r[2]}


@router.post("/by-linkedin", dependencies=[Depends(require_service_token)])
async def by_linkedin(body: dict[str, Any]) -> dict[str, Any]:
    """Person LinkedIn URL (POST body) → that person's ENTIRE Clay find-people payload(s).

    Body is ``{"url": "..."}`` (``linkedin_url`` accepted as an alias) — the value travels in
    the body, never the path/query. The URL is normalized with the SAME ``_norm_linkedin`` the
    land path uses and hashed to ``person_id = sha256(linkedin_url_norm)`` (the indexed
    per-person key), so the lookup is parity-exact with how rows were keyed at landing.

    ALWAYS 200 (an enrichment surface: a no-match is normal data, not an error — a Clay /
    automation row never hard-fails on it). ``found`` is the flag: false when the body carries
    no usable url OR the person has no Clay find-people record; true otherwise, with every
    (person × company) record newest-first, each carrying the verbatim ``raw_payload`` (the
    Clay object EXACTLY as sent) plus its lossless discriminators (domain_norm,
    matched_job_title, matched_company_name, landed_at)."""
    url = _s(body.get("url")) or _s(body.get("linkedin_url"))
    li_norm = _norm_linkedin(url) if url else None
    person_id = _sha(li_norm) if li_norm else None

    records: list[dict[str, Any]] = []
    if person_id is not None:
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(_BY_LINKEDIN_SQL, (person_id, _BY_LINKEDIN_CAP))
                rows = await cur.fetchall()
        records = [
            {
                "record_id": r[0],
                "domain_norm": r[1],
                "matched_job_title": r[2],
                "matched_company_name": r[3],
                "landed_at": r[4].isoformat() if r[4] is not None else None,
                "raw_payload": r[5],        # jsonb → dict, the Clay object verbatim
            }
            for r in rows
        ]
    return {
        "linkedin_url": url,
        "linkedin_url_norm": li_norm,
        "person_id": person_id,
        "found": len(records) > 0,
        "count": len(records),
        "records": records,
    }
