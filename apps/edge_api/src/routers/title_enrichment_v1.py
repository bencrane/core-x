"""gtm.title_enrichment — landing surface for job-title enrichment records (append-only).

Endpoints (mounted at ``/api/v1/title-enrichment``, service-token gated):
  POST /land   → land ONE enriched title (one row per request), idempotent on byte-identical resends
  POST /check  → has this title been enriched? returns enriched-yes/no + latest verdict by title_norm
  GET  /stats  → row / distinct-title counts + level / function / confidence coverage rollup

The PERSISTENCE sibling of the stateless ``/api/v1/titles/normalize`` gate (``title_normalize_v1``): that
route compiles a raw scraped title into the strict 6×22 taxonomy in-flight; this surface LANDS an enriched
title so the result is reusable.

WIRE CONTRACT. ONE record per request, FLAT singular fields (no nested raw_payload on the wire). The ONLY
required value is ``raw_job_title``; EVERY other field is OPTIONAL and may be sent null/omitted — a bare
``{"raw_job_title": "..."}`` is a valid landing::

    {
      "raw_job_title":        "VP of Making Things Go",                // required (the single mandatory field)
      "normalized_job_title": "VP, Operations",                       // optional — caller-supplied normalized title
      "normalized_level":     "VP",                                   // optional — taxonomy seniority, verbatim
      "normalized_function":  "Operations",                           // optional — taxonomy function, verbatim
      "confidence":           "high",                                 // optional
      "model":                "claude-haiku-4-5",                     // optional — enricher lineage
      "reasoning":            "...",                                  // optional — free text
      "person_linkedin_url":  "https://www.linkedin.com/in/jane-doe"  // optional — stored verbatim
    }

STORAGE. Dual: jsonb raw_payload (the flat body EXACTLY as sent — immutable SoT; extra keys survive
verbatim) + flat typed columns. title_norm (lower + whitespace-collapsed raw_job_title) is the canonical
title bridge key, computed server-side and indexed. PK record_id = sha256(person_linkedin_url(norm) |
title_norm | normalized_level | normalized_function | confidence | model) — PERSON-grain: two DIFFERENT
people with the SAME normalized title BOTH land (the person is in the key). A re-classification of the same
person appends a DISTINCT historical row; a byte-identical resend is idempotent (ON CONFLICT DO NOTHING).
person_linkedin_url is normalized (lower/scheme/www/trailing-slash) for the KEY ONLY — the stored column
stays verbatim — and is indexed for tie-back. A record with no person_linkedin_url falls back to
title-grain dedup (empty person key). reasoning and normalized_job_title are stored but EXCLUDED from
record_id.
"""
from __future__ import annotations

import hashlib
import logging
import re
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from psycopg.types.json import Jsonb

from ..db import get_db_connection
from ..service_token import require_service_token

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/title-enrichment", tags=["title-enrichment"])

_MAX_TITLE_LEN = 512


def _s(v: Any) -> str | None:
    """Verbatim text projection — trims whitespace only, never interprets."""
    return v.strip() if isinstance(v, str) and v.strip() else None


_WS_RE = re.compile(r"\s+")


def _title_norm(raw: str) -> str:
    """lower + whitespace-collapsed raw job title — the canonical bridge/dedup key. Deterministic and
    caller-reproducible."""
    return _WS_RE.sub(" ", raw.strip().lower())


_SCHEME_RE = re.compile(r"^https?://", re.I)
_WWW_RE = re.compile(r"^www\.", re.I)
_TRAIL_SLASH_RE = re.compile(r"/+$")


def _person_li_norm(url: str | None) -> str:
    """Normalize person_linkedin_url for the identity hash ONLY (the stored column stays verbatim):
    lower → strip scheme → strip leading www. → strip trailing slash. Empty string when absent, so a
    title-only record (no person) falls back to title-grain dedup."""
    if not url:
        return ""
    return _TRAIL_SLASH_RE.sub("", _WWW_RE.sub("", _SCHEME_RE.sub("", url.strip().lower())))


def _sha(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


# Column order is the single source of truth for the INSERT placeholders below.
_COLS = (
    "record_id", "raw_job_title", "title_norm", "normalized_job_title",
    "normalized_level", "normalized_function", "confidence", "model", "reasoning",
    "person_linkedin_url",
    "source", "raw_payload",
)
_INSERT_SQL = (
    f"INSERT INTO gtm.title_enrichment ({', '.join(_COLS)}) "
    f"VALUES ({', '.join(['%s'] * len(_COLS))}) "
    f"ON CONFLICT (record_id) DO NOTHING"
)

_STATS_SQL = """
    SELECT count(*)                                                AS rows,
           count(DISTINCT title_norm)                              AS distinct_titles,
           count(*) FILTER (WHERE normalized_level IS NOT NULL)    AS with_level,
           count(*) FILTER (WHERE normalized_function IS NOT NULL) AS with_function,
           count(*) FILTER (WHERE normalized_level IS NOT NULL
                              AND normalized_function IS NOT NULL)  AS fully_enriched,
           count(*) FILTER (WHERE confidence = 'high')             AS confidence_high,
           count(*) FILTER (WHERE confidence = 'medium')           AS confidence_medium,
           count(*) FILTER (WHERE confidence = 'low')              AS confidence_low
    FROM gtm.title_enrichment
"""

_SOURCE = "title_enrichment"


def _to_row(rec: dict[str, Any]) -> tuple | None:
    """Project one flat enrichment body → the full column tuple. None if the single hard requirement
    fails: raw_job_title must be a non-empty string. Every other field is optional (nullable)."""
    raw_job_title = _s(rec.get("raw_job_title"))
    if not raw_job_title:
        return None

    title_norm = _title_norm(raw_job_title)
    normalized_job_title = _s(rec.get("normalized_job_title"))
    normalized_level = _s(rec.get("normalized_level"))
    normalized_function = _s(rec.get("normalized_function"))
    confidence = _s(rec.get("confidence"))
    model = _s(rec.get("model"))
    reasoning = _s(rec.get("reasoning"))
    person_linkedin_url = _s(rec.get("person_linkedin_url"))

    # PERSON-grain identity: the (normalized) person_linkedin_url is folded into record_id FIRST so two
    # DIFFERENT people with the same normalized title both land. A re-classification of the SAME person
    # still appends history (level/function/confidence/model in the hash); a byte-identical resend is a
    # no-op. A record without a person LinkedIn falls back to title-grain dedup (empty person key).
    record_id = _sha("|".join([
        _person_li_norm(person_linkedin_url),
        title_norm,
        normalized_level or "",
        normalized_function or "",
        confidence or "",
        model or "",
    ]))

    return (
        record_id, raw_job_title, title_norm, normalized_job_title,
        normalized_level, normalized_function, confidence, model, reasoning,
        person_linkedin_url,
        _SOURCE, Jsonb(rec),
    )


@router.post("/land", dependencies=[Depends(require_service_token)])
async def land(body: dict[str, Any]) -> dict[str, Any]:
    """Land ONE enriched title. Required: raw_job_title (non-empty, ≤512 chars). Every other field is
    optional and may be sent null/omitted: normalized_job_title, normalized_level, normalized_function,
    confidence, model, reasoning, person_linkedin_url."""
    raw_input = body.get("raw_job_title")
    if not isinstance(raw_input, str) or not raw_input.strip():
        raise HTTPException(status_code=422, detail="raw_job_title is required (non-empty string)")
    if len(raw_input.strip()) > _MAX_TITLE_LEN:
        raise HTTPException(status_code=422, detail=f"raw_job_title exceeds {_MAX_TITLE_LEN} chars")

    row = _to_row(body)
    if row is None:  # defensive — the guards above already cover the only hard requirement
        raise HTTPException(status_code=422, detail="raw_job_title is required (non-empty string)")

    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(_INSERT_SQL, row)
            landed = cur.rowcount == 1
        await conn.commit()

    return {
        "landed": landed,                 # False ⇒ this exact (person, title, level, function, confidence, model) was already present
        "already_present": not landed,
        "record_id": row[0],
        "raw_job_title": row[1],
        "title_norm": row[2],
        "normalized_job_title": row[3],
        "normalized_level": row[4],
        "normalized_function": row[5],
        "person_linkedin_url": row[9],
    }


@router.post("/check", dependencies=[Depends(require_service_token)])
async def check(body: dict[str, Any]) -> dict[str, Any]:
    """Has this title been enriched? POST body ``{"raw_job_title": "..."}``. Returns ``enriched`` (bool)
    plus the historical row count, most-recent ``landed_at``, and the latest verdict (normalized_level /
    normalized_function / confidence). Title normalized identically to /land."""
    raw_input = body.get("raw_job_title")
    if not isinstance(raw_input, str) or not raw_input.strip():
        raise HTTPException(status_code=422, detail="raw_job_title is required (non-empty string)")
    title_norm = _title_norm(raw_input)

    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT count(*)                 AS n,
                       max(landed_at)           AS most_recent_at,
                       (SELECT normalized_level    FROM gtm.title_enrichment WHERE title_norm = %s ORDER BY landed_at DESC LIMIT 1),
                       (SELECT normalized_function FROM gtm.title_enrichment WHERE title_norm = %s ORDER BY landed_at DESC LIMIT 1),
                       (SELECT confidence          FROM gtm.title_enrichment WHERE title_norm = %s ORDER BY landed_at DESC LIMIT 1)
                FROM gtm.title_enrichment WHERE title_norm = %s
                """,
                (title_norm, title_norm, title_norm, title_norm),
            )
            r = await cur.fetchone()
    return {
        "raw_job_title": raw_input.strip(),
        "title_norm": title_norm,
        "enriched": r[0] > 0,
        "record_count": r[0],
        "most_recent_at": r[1].isoformat() if r[1] else None,
        "latest_normalized_level": r[2],
        "latest_normalized_function": r[3],
        "latest_confidence": r[4],
    }


@router.get("/stats", dependencies=[Depends(require_service_token)])
async def stats() -> dict[str, Any]:
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(_STATS_SQL)
            r = await cur.fetchone()
    return {
        "rows": r[0],
        "distinct_titles": r[1],
        "with_level": r[2],
        "with_function": r[3],
        "fully_enriched": r[4],
        "confidence_high": r[5],
        "confidence_medium": r[6],
        "confidence_low": r[7],
    }
