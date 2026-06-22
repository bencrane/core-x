"""equipment_finance_candidates — flat cohort landing for curated equipment-finance lender candidates.

Endpoints (mounted at ``/api/v1/equipment-finance-candidates``, service-token gated):
  POST /land   → land ONE candidate, idempotent (byte-identical resends are no-ops)
  POST /check  → has this domain or LinkedIn URL been landed?
  GET  /stats  → row / distinct counts + SKIP-verdict rollup

WIRE CONTRACT::

    {
      "company_name":        "Auxilior Capital Partners",
      "company_domain":      "auxcap.com",                                    // optional
      "company_linkedin_url": "https://www.linkedin.com/company/auxilior-...",// optional
      "verdict":             "SKIP"                                            // optional ('SKIP' marks unusable)
    }

At least one of `company_domain` or `company_linkedin_url` must resolve to a usable bridge key.

PK = sha256(domain_norm | linkedin_url_norm | verdict). Different verdicts for the same firm
land as DISTINCT rows (operator-edits are history).
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

router = APIRouter(prefix="/api/v1/equipment-finance-candidates",
                   tags=["equipment-finance-candidates"])


# ── domain normalization (mirrors firmographics_blitz._normalized_domain) ──
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


# ── linkedin url normalization — lower, strip scheme + www, strip trailing /, drop locale subdomain ──
_LI_LOCALE_RE = re.compile(r"^[a-z]{2}\.linkedin\.com/", flags=re.I)


def _normalize_linkedin(raw: Any) -> str | None:
    if not isinstance(raw, str):
        return None
    s = raw.strip().lower()
    if not s:
        return None
    s = _SCHEME_RE.sub("", s)
    s = _WWW_RE.sub("", s)
    # collapse locale subdomain (e.g. ca.linkedin.com → linkedin.com)
    s = _LI_LOCALE_RE.sub("linkedin.com/", s)
    s = s.rstrip("/")
    # only accept if it looks like a real linkedin URL (linkedin.com/company/... or /in/... etc)
    if not s.startswith("linkedin.com/"):
        return None
    return s or None


def _s(v: Any) -> str | None:
    return v.strip() if isinstance(v, str) and v.strip() else None


def _sha(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


_COLS = (
    "record_id", "company_name", "company_domain", "domain_norm",
    "linkedin_url", "linkedin_url_norm", "verdict",
    "source", "raw_payload",
)
_INSERT_SQL = (
    f"INSERT INTO gtm.equipment_finance_candidates ({', '.join(_COLS)}) "
    f"VALUES ({', '.join(['%s'] * len(_COLS))}) "
    f"ON CONFLICT (record_id) DO NOTHING"
)

_STATS_SQL = """
    SELECT count(*)                                AS rows,
           count(DISTINCT domain_norm)             AS distinct_domains,
           count(DISTINCT linkedin_url_norm)       AS distinct_linkedin_urls,
           count(*) FILTER (WHERE verdict = 'SKIP') AS skip_rows,
           count(*) FILTER (WHERE verdict IS NULL)  AS active_rows,
           count(*) FILTER (WHERE domain_norm IS NOT NULL)        AS with_domain,
           count(*) FILTER (WHERE linkedin_url_norm IS NOT NULL)   AS with_linkedin
    FROM gtm.equipment_finance_candidates
"""

_SOURCE = "equipment_finance_candidates"


def _to_row(rec: dict[str, Any]) -> tuple | None:
    company_name = _s(rec.get("company_name"))
    if not company_name:
        return None
    domain_norm = _normalize_domain(rec.get("company_domain"))
    linkedin_url_norm = _normalize_linkedin(rec.get("company_linkedin_url"))
    if not domain_norm and not linkedin_url_norm:
        return None
    verdict = _s(rec.get("verdict"))

    key_seed = "|".join([
        domain_norm or "",
        linkedin_url_norm or "",
        (verdict or "").upper(),
    ])
    record_id = _sha(key_seed)

    return (
        record_id, company_name,
        _s(rec.get("company_domain")), domain_norm,
        _s(rec.get("company_linkedin_url")), linkedin_url_norm,
        verdict, _SOURCE, Jsonb(rec),
    )


@router.post("/land", dependencies=[Depends(require_service_token)])
async def land(body: dict[str, Any]) -> dict[str, Any]:
    """Land ONE candidate. Body fields: company_name (required), company_domain (optional),
    company_linkedin_url (optional), verdict (optional)."""
    row = _to_row(body)
    if row is None:
        if not _s(body.get("company_name")):
            raise HTTPException(status_code=422, detail="company_name is required (non-empty string)")
        raise HTTPException(
            status_code=422,
            detail="need at least one of company_domain or company_linkedin_url that normalizes to a usable bridge key",
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
        "company_name": row[1],
        "domain_norm": row[3],
        "linkedin_url_norm": row[5],
        "verdict": row[6],
    }


@router.post("/check", dependencies=[Depends(require_service_token)])
async def check(body: dict[str, Any]) -> dict[str, Any]:
    """Has this firm been landed? Send `company_domain` and/or `company_linkedin_url`."""
    domain_norm = _normalize_domain(body.get("company_domain"))
    linkedin_url_norm = _normalize_linkedin(body.get("company_linkedin_url"))
    if not domain_norm and not linkedin_url_norm:
        raise HTTPException(
            status_code=422,
            detail="need at least one of company_domain or company_linkedin_url",
        )
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT count(*) AS n,
                       max(landed_at) AS most_recent_at,
                       (SELECT verdict FROM gtm.equipment_finance_candidates
                        WHERE (domain_norm = %s OR linkedin_url_norm = %s)
                        ORDER BY landed_at DESC LIMIT 1) AS latest_verdict,
                       (SELECT company_name FROM gtm.equipment_finance_candidates
                        WHERE (domain_norm = %s OR linkedin_url_norm = %s)
                        ORDER BY landed_at DESC LIMIT 1) AS latest_company_name
                FROM gtm.equipment_finance_candidates
                WHERE (domain_norm = %s OR linkedin_url_norm = %s)
                """,
                (domain_norm, linkedin_url_norm) * 3,
            )
            r = await cur.fetchone()
    return {
        "domain_norm": domain_norm,
        "linkedin_url_norm": linkedin_url_norm,
        "found": r[0] > 0,
        "record_count": r[0],
        "most_recent_at": r[1].isoformat() if r[1] else None,
        "latest_verdict": r[2],
        "latest_company_name": r[3],
    }


@router.get("/stats", dependencies=[Depends(require_service_token)])
async def stats() -> dict[str, Any]:
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(_STATS_SQL)
            r = await cur.fetchone()
    return {
        "rows": r[0],
        "distinct_domains": r[1],
        "distinct_linkedin_urls": r[2],
        "skip_rows": r[3],
        "active_rows": r[4],
        "with_domain": r[5],
        "with_linkedin": r[6],
    }
