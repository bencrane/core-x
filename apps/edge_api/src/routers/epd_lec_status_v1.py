"""epd_lec_status — raw landing surface for EPD / Buy-Clean / LEC compliance research payloads
(append-only).

Endpoints (mounted at ``/api/v1/epd-lec-status``, service-token gated):
  POST /land   → land ONE research record, idempotent (byte-identical resends are no-ops)
  POST /check  → has the EPD/LEC enrichment been done for this domain? returns count + latest status
  GET  /stats  → row / distinct-company-domain / distinct-domain_norm / status-mix counts

WIRE CONTRACT::

    {
      "company_domain": "dcp-int.com",
      "raw_payload":    { ... the entire research object, EXACTLY as your tool emitted it ... }
    }

PAYLOAD SHAPE (one shape only; verbatim from the EPD/LEC tool)::

    {
      "reasoning":      "...",
      "confidence":     "low"|"medium"|"high",
      "stepsTaken":     ["Visited <url>", ...],
      "epdLecStatus":   "YES"|"NO"|"UNCLEAR"|...,
      "justification":  "..."
    }

STORAGE. Dual: jsonb raw_payload (immutable SoT) + flat projection of {confidence, reasoning,
steps_taken, epd_lec_status, justification}. company_domain stored verbatim; domain_norm is the
canonical bridge to firmographics_blitz (lower/trim → strip scheme → strip www → strip path
→ strip trailing dots). PK = sha256(domain_norm | sha256(canonical_json(raw_payload))) —
byte-identical resends idempotent; different payloads for the same domain land as DISTINCT rows.
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

router = APIRouter(prefix="/api/v1/epd-lec-status", tags=["epd-lec-status"])


# ── normalization (mirrors pipelines/firmographics_blitz/materialize_blitz._normalized_domain) ──
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


def _s(v: Any) -> str | None:
    return v.strip() if isinstance(v, str) and v.strip() else None


def _list_of_str(v: Any) -> list[str] | None:
    if not isinstance(v, list):
        return None
    out = [x.strip() for x in v if isinstance(x, str) and x.strip()]
    return out or None


def _canonical_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def _sha(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


_COLS = (
    "record_id", "company_domain", "domain_norm",
    "confidence", "reasoning", "steps_taken",
    "epd_lec_status", "justification",
    "source", "raw_payload",
)
_INSERT_SQL = (
    f"INSERT INTO gtm.epd_lec_status ({', '.join(_COLS)}) "
    f"VALUES ({', '.join(['%s'] * len(_COLS))}) "
    f"ON CONFLICT (record_id) DO NOTHING"
)

_STATS_SQL = """
    SELECT count(*)                       AS rows,
           count(DISTINCT company_domain) AS distinct_company_domains,
           count(DISTINCT domain_norm)    AS distinct_domain_norms,
           count(*) FILTER (WHERE confidence = 'high')   AS confidence_high,
           count(*) FILTER (WHERE confidence = 'medium') AS confidence_medium,
           count(*) FILTER (WHERE confidence = 'low')    AS confidence_low,
           count(*) FILTER (WHERE epd_lec_status = 'YES')     AS status_yes,
           count(*) FILTER (WHERE epd_lec_status = 'NO')      AS status_no,
           count(*) FILTER (WHERE epd_lec_status NOT IN ('YES','NO') OR epd_lec_status IS NULL) AS status_other
    FROM gtm.epd_lec_status
"""

_SOURCE = "epd_lec_status"


def _to_row(company_domain_raw: str, rec: dict[str, Any]) -> tuple | None:
    company_domain = _s(company_domain_raw)
    if not company_domain:
        return None
    domain_norm = _normalize_domain(company_domain)
    if not domain_norm:
        return None

    steps_taken = _list_of_str(rec.get("stepsTaken"))
    record_id = _sha(domain_norm + "|" + _sha(_canonical_json(rec)))

    def _j(v: Any) -> Jsonb | None:
        return Jsonb(v) if v is not None else None

    return (
        record_id, company_domain, domain_norm,
        _s(rec.get("confidence")), _s(rec.get("reasoning")),
        _j(steps_taken),
        _s(rec.get("epdLecStatus")), _s(rec.get("justification")),
        _SOURCE, Jsonb(rec),
    )


@router.post("/land", dependencies=[Depends(require_service_token)])
async def land(body: dict[str, Any]) -> dict[str, Any]:
    """Land ONE EPD-LEC-status record. Body is ``{"company_domain": "...", "raw_payload": {...}}``."""
    company_domain = body.get("company_domain")
    rec = body.get("raw_payload")
    if not isinstance(rec, dict):
        raise HTTPException(status_code=422, detail="raw_payload must be a JSON object")
    if not isinstance(company_domain, str) or not company_domain.strip():
        raise HTTPException(status_code=422, detail="company_domain is required (non-empty string)")

    row = _to_row(company_domain, rec)
    if row is None:
        logger.warning(
            "epd_lec_status land rejected: unresolvable company_domain (had_input=%s)",
            bool(_s(company_domain)),
        )
        raise HTTPException(
            status_code=422,
            detail="company_domain did not normalize to a usable bridge key",
        )

    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(_INSERT_SQL, row)
            landed = cur.rowcount == 1
        await conn.commit()

    return {
        "landed": landed,
        "already_present": not landed,
        "record_id": row[0],
        "company_domain": row[1],
        "domain_norm": row[2],
        "epd_lec_status": row[6],
    }


@router.post("/check", dependencies=[Depends(require_service_token)])
async def check(body: dict[str, Any]) -> dict[str, Any]:
    """Has the EPD-LEC enrichment been done for this domain? POST body:
    ``{"company_domain": "..."}``. Returns ``enriched`` (bool) plus the record count,
    most-recent ``landed_at``, latest ``confidence``, and latest ``epd_lec_status``.
    Domain normalized identically to /land so both endpoints agree."""
    company_domain = body.get("company_domain")
    if not isinstance(company_domain, str) or not company_domain.strip():
        raise HTTPException(status_code=422, detail="company_domain is required (non-empty string)")
    domain_norm = _normalize_domain(company_domain)
    if not domain_norm:
        raise HTTPException(status_code=422, detail="company_domain did not normalize to a usable bridge key")
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT count(*) AS n,
                       max(landed_at) AS most_recent_at,
                       max(confidence) FILTER (WHERE landed_at = (
                           SELECT max(landed_at) FROM gtm.epd_lec_status WHERE domain_norm = %s
                       )) AS latest_confidence,
                       max(epd_lec_status) FILTER (WHERE landed_at = (
                           SELECT max(landed_at) FROM gtm.epd_lec_status WHERE domain_norm = %s
                       )) AS latest_epd_lec_status
                FROM gtm.epd_lec_status
                WHERE domain_norm = %s
                """,
                (domain_norm, domain_norm, domain_norm),
            )
            r = await cur.fetchone()
    return {
        "company_domain": company_domain,
        "domain_norm": domain_norm,
        "enriched": r[0] > 0,
        "record_count": r[0],
        "most_recent_at": r[1].isoformat() if r[1] else None,
        "latest_confidence": r[2],
        "latest_epd_lec_status": r[3],
    }


@router.get("/stats", dependencies=[Depends(require_service_token)])
async def stats() -> dict[str, Any]:
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(_STATS_SQL)
            r = await cur.fetchone()
    return {
        "rows": r[0],
        "distinct_company_domains": r[1],
        "distinct_domain_norms": r[2],
        "confidence_high": r[3],
        "confidence_medium": r[4],
        "confidence_low": r[5],
        "status_yes": r[6],
        "status_no": r[7],
        "status_other": r[8],
    }
