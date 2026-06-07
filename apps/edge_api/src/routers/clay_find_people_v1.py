"""Clay Find People — raw landing surface (append-only).

Endpoints (mounted at ``/api/v1/clay/find-people``, service-token gated):
  POST /land   → land a batch of Clay find-people records, idempotent
  GET  /stats  → row / distinct-person / distinct-domain counts

CONTRACT. Each item in ``records`` is a Clay find-people object. It is stored TWO ways, both
faithful to the source:
  1. ``raw_payload`` (jsonb) — the object EXACTLY as sent, verbatim. Immutable source of truth,
     drift-proof: if Clay adds/removes fields nothing is ever lost.
  2. Flat typed columns — a LOSSLESS structural projection of the known fields (``name`` →
     full_name, ``matched_experience.job_title`` → matched_job_title, …). Values are stored
     AS-IS; this is NOT normalization. Semantic normalization (job_title → job_level enum) is a
     SEPARATE downstream stage and is NOT done here.

The caller does NOT send a separate ``person_linkedin_url`` — the server reads it out of the
payload. The only computed values are lossless identity keys:
    raw_payload.url     → linkedin_url_raw (verbatim; feeds the blitz email finder)
                          → linkedin_url_norm → person_id = sha256(linkedin_url_norm)
    raw_payload.domain  → domain_norm (FK → firmographics_blitz.domain_norm)

GRAIN: (person × company). PK = ``sha256(linkedin_url_norm | <company_key>)`` where company_key
degrades through a fallback chain so a missing company never silently collapses distinct Clay
observations:  domain_norm  →  company_record_id  →  stable hash of the full payload.
``ON CONFLICT DO NOTHING`` makes re-sends idempotent at whatever tier the key resolved to.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
from typing import Any

from fastapi import APIRouter, Depends
from psycopg.types.json import Jsonb
from pydantic import BaseModel, ConfigDict, Field

from ..db import get_db_connection
from ..service_token import require_service_token

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/clay/find-people", tags=["clay-find-people"])


class LandBody(BaseModel):
    """Envelope is strict; each item in ``records`` is a Clay object stored verbatim + exploded."""

    records: list[dict[str, Any]] = Field(min_length=1, max_length=25_000)
    batch_id: str | None = Field(default=None, max_length=200)
    source: str = Field(default="clay_find_people", max_length=100)

    model_config = ConfigDict(extra="forbid")


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
    "source", "batch_id", "raw_payload",
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


def _to_row(rec: dict[str, Any], source: str, batch_id: str | None) -> tuple | None:
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
        source, batch_id, Jsonb(rec),
    )


@router.post("/land", dependencies=[Depends(require_service_token)])
async def land(body: LandBody) -> dict[str, Any]:
    """Land Clay records (verbatim raw_payload + exploded columns). Returns received /
    landed (newly inserted) / already_present (ON CONFLICT dups) / skipped (no usable url)."""
    received = len(body.records)
    skipped: list[dict[str, Any]] = []
    rows: list[tuple] = []
    seen: set[str] = set()

    for i, rec in enumerate(body.records):
        row = _to_row(rec, body.source, body.batch_id)
        if row is None:
            skipped.append({"index": i, "reason": "missing/empty raw_payload.url"})
            continue
        if row[0] in seen:  # collapse byte-identical (person, company) dups within this payload
            continue
        seen.add(row[0])
        rows.append(row)

    landed = 0
    if rows:
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.executemany(_INSERT_SQL, rows)
                landed = cur.rowcount if (cur.rowcount and cur.rowcount > 0) else 0
            await conn.commit()

    return {
        "received": received,
        "accepted": len(rows),
        "landed": landed,
        "already_present": max(len(rows) - landed, 0),
        "skipped": len(skipped),
        "skipped_detail": skipped[:50],
        "batch_id": body.batch_id,
    }


@router.get("/stats", dependencies=[Depends(require_service_token)])
async def stats() -> dict[str, Any]:
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(_STATS_SQL)
            r = await cur.fetchone()
    return {"rows": r[0], "distinct_persons": r[1], "distinct_domains": r[2]}
