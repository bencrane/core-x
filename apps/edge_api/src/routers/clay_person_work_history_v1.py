"""Clay Person Work-History — raw landing surface for the FULL profile payload (append-only).

Endpoints (mounted at ``/api/v1/clay/person-work-history``, service-token gated):
  POST /land   → land ONE Clay person profile (Clay fires one row per request), idempotent
  GET  /stats  → row / distinct-person / distinct-profile counts

WIRE CONTRACT. Clay POSTs a single profile per request, the object under a ``raw_payload`` key::

    { "raw_payload": { "url": "...", "experience": [ ... ], "education": [ ... ], ... } }

One profile at a time — no ``records`` array. (A bare object with no ``raw_payload`` wrapper is also
accepted, so a Clay misconfig never drops data.)

STORAGE. The profile is stored as ONE verbatim jsonb blob — NOT exploded. Unlike the sibling
``clay_find_people`` (which projects known scalars into typed columns), this surface keeps the entire
LinkedIn-style profile — the full multi-position ``experience[]`` array, ``education``,
``publications``, ``certifications``, ``volunteering``, ``structured_location`` — untouched in
``raw_payload``. The only computed columns are lossless identity keys read server-side:

    raw_payload.url          → linkedin_url_raw (verbatim) → linkedin_url_norm
                               → person_id = sha256(linkedin_url_norm)   (SAME derivation as
                                 gtm.clay_find_people.person_id — the two tables join on person_id)
    raw_payload.profile_id   → profile_id (bigint)         (stable LinkedIn numeric id)
    raw_payload.last_refresh → last_refresh (timestamptz)  (enrichment recency; latest-per-person)

GRAIN: one row per DISTINCT profile snapshot. PK = ``record_id`` = sha256(canonical_json(raw_payload)).
``ON CONFLICT DO NOTHING`` makes byte-identical resends idempotent (first-write-wins; the blob is
immutable). A re-enrichment of the same person (ANY field changed, including last_refresh) lands as a
DISTINCT snapshot row — append-only history. The current profile is the latest snapshot per person:
``SELECT DISTINCT ON (person_id) ... ORDER BY person_id, last_refresh DESC``.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from psycopg.types.json import Jsonb

from ..db import get_db_connection
from ..service_token import require_service_token

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/clay/person-work-history", tags=["clay-person-work-history"])


# ── Lossless canonicalization (NOT classification) — mirrors clay_find_people ──
_SCHEME = re.compile(r"^https?://")
_WWW = re.compile(r"^www\.")
_QS = re.compile(r"[?#].*$")
_TRAIL_SLASH = re.compile(r"/+$")


def _norm_linkedin(url: str) -> str:
    s = _SCHEME.sub("", url.strip().lower())
    s = _WWW.sub("", s)
    s = _QS.sub("", s)
    return _TRAIL_SLASH.sub("", s)


def _sha(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _canonical_json(obj: Any) -> str:
    """Stable serialization for the content-address PK — key order canonical, no incidental whitespace."""
    return json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def _bigint(v: Any) -> int | None:
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, str) and v.strip().lstrip("-").isdigit():
        return int(v.strip())
    return None


def _parse_last_refresh(v: Any) -> datetime | None:
    """Clay sends ``last_refresh`` as e.g. ``"2026-06-28 00:11:16.476"`` (UTC, no tz). Parse to an
    aware datetime; None if absent/unparseable so a bad timestamp never rejects a land (the verbatim
    value is preserved in raw_payload regardless)."""
    if not isinstance(v, str) or not v.strip():
        return None
    try:
        dt = datetime.fromisoformat(v.strip().replace(" ", "T", 1))
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


# Column order is the single source of truth for the INSERT placeholders below.
_COLS = (
    "record_id", "person_id", "linkedin_url_raw", "linkedin_url_norm",
    "profile_id", "last_refresh", "source", "raw_payload",
)
_INSERT_SQL = (
    f"INSERT INTO gtm.clay_person_work_history ({', '.join(_COLS)}) "
    f"VALUES ({', '.join(['%s'] * len(_COLS))}) "
    f"ON CONFLICT (record_id) DO NOTHING"
)

_STATS_SQL = """
    SELECT count(*)                  AS rows,
           count(DISTINCT person_id) AS distinct_persons,
           count(DISTINCT profile_id) AS distinct_profiles
    FROM gtm.clay_person_work_history
"""

_SOURCE = "clay_person_work_history"


def _to_row(rec: dict[str, Any]) -> tuple | None:
    """Project one Clay profile → the full column tuple. None if it lacks a usable url (the per-person
    identity). The payload itself is stored verbatim — only the identity keys are computed."""
    url = rec.get("url")
    if not isinstance(url, str) or not url.strip():
        return None
    li_norm = _norm_linkedin(url)
    if not li_norm:
        return None
    return (
        _sha(_canonical_json(rec)),   # record_id — content address of the whole profile
        _sha(li_norm),                # person_id — joins gtm.clay_find_people
        url.strip(),                  # linkedin_url_raw (verbatim)
        li_norm,                      # linkedin_url_norm
        _bigint(rec.get("profile_id")),
        _parse_last_refresh(rec.get("last_refresh")),
        _SOURCE,
        Jsonb(rec),                   # raw_payload — EXACTLY as sent, NOT exploded
    )


@router.post("/land", dependencies=[Depends(require_service_token)])
async def land(body: dict[str, Any]) -> dict[str, Any]:
    """Land ONE Clay person profile. Body is ``{"raw_payload": {...}}`` (one row per request); a bare
    object with no wrapper is also accepted. Stores raw_payload verbatim + computed identity keys."""
    rec = body.get("raw_payload")
    if not isinstance(rec, dict):
        # tolerate a bare Clay object sent without the raw_payload wrapper
        rec = body

    row = _to_row(rec)
    if row is None:
        # A 422 is a SILENT drop from Clay's side (4xx is not retried). Log why — without PII — so a
        # bulk load is reconcilable: count these against /stats to see drops.
        logger.warning(
            "clay person-work-history land rejected: missing/empty raw_payload.url "
            "(had_name=%s had_profile_id=%s had_experience=%s)",
            isinstance(rec.get("name"), str) and bool(rec.get("name", "").strip()),
            _bigint(rec.get("profile_id")) is not None,
            isinstance(rec.get("experience"), list),
        )
        raise HTTPException(status_code=422, detail="missing/empty raw_payload.url")

    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(_INSERT_SQL, row)
            landed = cur.rowcount == 1
        await conn.commit()

    return {
        "landed": landed,                 # False ⇒ this exact profile snapshot was already present
        "already_present": not landed,
        "record_id": row[0],
        "person_id": row[1],
        "profile_id": row[4],
    }


@router.get("/stats", dependencies=[Depends(require_service_token)])
async def stats() -> dict[str, Any]:
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(_STATS_SQL)
            r = await cur.fetchone()
    return {"rows": r[0], "distinct_persons": r[1], "distinct_profiles": r[2]}
