"""industries_served — raw landing surface for company-industries research payloads (append-only).

Endpoints (mounted at ``/api/v1/industries-served``, service-token gated):
  POST /land   → land ONE research record, idempotent (byte-identical resends are no-ops)
  GET  /stats  → row / distinct-company-domain / distinct-domain_norm counts

WIRE CONTRACT::

    {
      "company_domain":  "togglerentals.com",   # or "domain" — ALWAYS sent by the caller
      "source":          "capital_providers",   # optional lane tag; default "industries_served"
      "clay_table_url":  "https://app.clay.com/…",  # optional provenance — which Clay table ran it
      "raw_payload":     { ... the entire research object, EXACTLY as your tool emitted it ... }
    }

The domain comes from the BODY only — raw_payload is never consulted for it (its key set is
not guaranteed).

Two payload key shapes project losslessly (the raw jsonb is verbatim either way):
the original ``{industriesServed[], sources[]}`` and the Claygent capital-provider run
``{industries[], source: "url; url", companyName, domain}`` (2026-07-19 shape).

STORAGE. Dual: jsonb raw_payload (immutable SoT) + flat projection of {confidence, reasoning,
sources, steps_taken, industries_served, industries_served_count}. company_domain stored verbatim;
domain_norm is the canonical bridge to firmographics_blitz (lower/trim → strip scheme → strip www
→ strip path → strip trailing dots). PK = sha256(domain_norm | sha256(canonical_json(raw_payload)))
— byte-identical resends idempotent; different payloads for the same domain land as DISTINCT rows.
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

router = APIRouter(prefix="/api/v1/industries-served", tags=["industries-served"])


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
    "confidence", "reasoning", "sources", "steps_taken",
    "industries_served", "industries_served_count",
    "source", "clay_table_url", "raw_payload",
)
_INSERT_SQL = (
    f"INSERT INTO gtm.industries_served ({', '.join(_COLS)}) "
    f"VALUES ({', '.join(['%s'] * len(_COLS))}) "
    f"ON CONFLICT (record_id) DO NOTHING"
)

_STATS_SQL = """
    SELECT count(*)                       AS rows,
           count(DISTINCT company_domain) AS distinct_company_domains,
           count(DISTINCT domain_norm)    AS distinct_domain_norms,
           count(*) FILTER (WHERE confidence = 'high')   AS confidence_high,
           count(*) FILTER (WHERE confidence = 'medium') AS confidence_medium,
           count(*) FILTER (WHERE confidence = 'low')    AS confidence_low
    FROM gtm.industries_served
"""

_SOURCE = "industries_served"
_SOURCE_TAG_RE = re.compile(r"^[a-z0-9_\-]{1,64}$")


def _to_row(company_domain_raw: str, rec: dict[str, Any], source: str = _SOURCE,
            clay_table_url: str | None = None) -> tuple | None:
    company_domain = _s(company_domain_raw)
    if not company_domain:
        return None
    domain_norm = _normalize_domain(company_domain)
    if not domain_norm:
        return None

    # sources: original shape sends sources[] (list); the Claygent capital shape sends
    # source (one "; "-joined string of URLs) — split for the projection, raw keeps verbatim.
    sources = _list_of_str(rec.get("sources"))
    if sources is None and isinstance(rec.get("source"), str):
        sources = _list_of_str([p for p in rec["source"].split(";")])
    steps_taken = _list_of_str(rec.get("stepsTaken"))
    # industries: original shape industriesServed[]; Claygent capital shape industries[].
    industries_served = _list_of_str(rec.get("industriesServed"))
    if industries_served is None:
        industries_served = _list_of_str(rec.get("industries"))
    industries_served_count = len(industries_served) if industries_served else None

    record_id = _sha(domain_norm + "|" + _sha(_canonical_json(rec)))

    def _j(v: Any) -> Jsonb | None:
        return Jsonb(v) if v is not None else None

    return (
        record_id, company_domain, domain_norm,
        _s(rec.get("confidence")), _s(rec.get("reasoning")),
        _j(sources), _j(steps_taken),
        _j(industries_served), industries_served_count,
        source, _s(clay_table_url), Jsonb(rec),
    )


@router.post("/land", dependencies=[Depends(require_service_token)])
async def land(body: dict[str, Any]) -> dict[str, Any]:
    """Land ONE industries-served record. Body is ``{"company_domain"|"domain": "...",
    "raw_payload": {...}}`` (+ optional ``"source"`` lane tag, optional ``"clay_table_url"``).
    The domain comes from the body only — raw_payload is never consulted for it."""
    rec = body.get("raw_payload")
    if not isinstance(rec, dict):
        raise HTTPException(status_code=422, detail="raw_payload must be a JSON object")
    company_domain = body.get("company_domain") or body.get("domain")
    if not isinstance(company_domain, str) or not company_domain.strip():
        raise HTTPException(status_code=422, detail="company_domain (or domain) is required (non-empty string)")

    source = body.get("source", _SOURCE)
    if not isinstance(source, str) or not _SOURCE_TAG_RE.match(source):
        raise HTTPException(status_code=422, detail="source must match ^[a-z0-9_-]{1,64}$")

    clay_table_url = body.get("clay_table_url")
    if clay_table_url is not None and not isinstance(clay_table_url, str):
        raise HTTPException(status_code=422, detail="clay_table_url must be a string when present")

    row = _to_row(company_domain, rec, source=source, clay_table_url=clay_table_url)
    if row is None:
        logger.warning(
            "industries_served land rejected: unresolvable company_domain (had_input=%s)",
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
        "industries_served_count": row[8],
    }


@router.post("/check", dependencies=[Depends(require_service_token)])
async def check(body: dict[str, Any]) -> dict[str, Any]:
    """Has the industries-served enrichment been done for this domain? POST body:
    ``{"company_domain": "..."}``. Returns ``enriched`` (bool) plus the record count,
    most-recent ``landed_at``, and the latest payload's ``industries_served_count`` for
    a quick at-a-glance. Domain normalized identically to /land so both endpoints agree."""
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
                           SELECT max(landed_at) FROM gtm.industries_served WHERE domain_norm = %s
                       )) AS latest_confidence,
                       max(industries_served_count) FILTER (WHERE landed_at = (
                           SELECT max(landed_at) FROM gtm.industries_served WHERE domain_norm = %s
                       )) AS latest_industries_served_count
                FROM gtm.industries_served
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
        "latest_industries_served_count": r[3],
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
    }
