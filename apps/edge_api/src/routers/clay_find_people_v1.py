"""Clay Find People — raw landing surface (append-only).

Endpoints (mounted at ``/api/v1/clay/find-people``, service-token gated):
  POST /land   → land a batch of Clay find-people records, verbatim, idempotent
  GET  /stats  → row / distinct-person / distinct-domain counts

CONTRACT. Each item in ``records`` is a Clay find-people object stored EXACTLY as sent into
``gtm.clay_find_people.raw_payload`` (jsonb). NOTHING is exploded. The caller does NOT send a
separate ``person_linkedin_url`` — the server reads it out of the payload itself. The only
derived values are lossless identity keys:

    raw_payload.url     → linkedin_url_raw (verbatim; this is what feeds the blitz email finder)
                          → linkedin_url_norm → person_id = sha256(linkedin_url_norm)
    raw_payload.domain  → domain_norm (FK → firmographics_blitz.domain_norm)

GRAIN: (person × company-record). The same LinkedIn URL can attach to multiple domains, so the
PK = sha256(linkedin_url_norm | domain_norm) keeps every attachment; ``ON CONFLICT DO NOTHING``
makes re-sends idempotent. Title/role normalization + email enrichment are SEPARATE downstream
stages that read this table — never done here.
"""
from __future__ import annotations

import hashlib
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
    """Envelope is strict; each item in ``records`` is a Clay object stored VERBATIM."""

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
    """lower/trim → strip scheme → strip www. → strip query/fragment → strip trailing slash."""
    s = _SCHEME.sub("", url.strip().lower())
    s = _WWW.sub("", s)
    s = _QS.sub("", s)
    s = _TRAIL_SLASH.sub("", s)
    return s


def _norm_domain(domain: str) -> str:
    """Mirror of the firmographics_blitz domain_norm: lower/trim → strip scheme → strip www.
    → strip path → strip trailing dots."""
    s = _SCHEME.sub("", domain.strip().lower())
    s = _WWW.sub("", s)
    s = _PATH.sub("", s)
    s = _TRAIL_DOT.sub("", s)
    return s


def _sha(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


_INSERT_SQL = """
    INSERT INTO gtm.clay_find_people
        (record_id, person_id, linkedin_url_raw, linkedin_url_norm,
         domain_norm, source, batch_id, raw_payload)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (record_id) DO NOTHING
"""

_STATS_SQL = """
    SELECT count(*)                      AS rows,
           count(DISTINCT person_id)     AS distinct_persons,
           count(DISTINCT domain_norm)   AS distinct_domains
    FROM gtm.clay_find_people
"""


@router.post("/land", dependencies=[Depends(require_service_token)])
async def land(body: LandBody) -> dict[str, Any]:
    """Land Clay records verbatim. Returns received / landed (newly inserted) /
    already_present (ON CONFLICT dups) / skipped (records missing a usable url)."""
    received = len(body.records)
    skipped: list[dict[str, Any]] = []
    rows: list[tuple] = []
    seen: set[str] = set()

    for i, rec in enumerate(body.records):
        url = rec.get("url")
        if not isinstance(url, str) or not url.strip():
            skipped.append({"index": i, "reason": "missing raw_payload.url"})
            continue
        li_norm = _norm_linkedin(url)
        if not li_norm:
            skipped.append({"index": i, "reason": "empty linkedin_url after normalization"})
            continue
        raw_dom = rec.get("domain")
        dom_norm = _norm_domain(raw_dom) if isinstance(raw_dom, str) and raw_dom.strip() else None
        record_id = _sha(f"{li_norm}|{dom_norm or ''}")
        if record_id in seen:  # collapse exact (person, domain) dups within this one payload
            continue
        seen.add(record_id)
        rows.append(
            (record_id, _sha(li_norm), url.strip(), li_norm,
             dom_norm, body.source, body.batch_id, Jsonb(rec))
        )

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
