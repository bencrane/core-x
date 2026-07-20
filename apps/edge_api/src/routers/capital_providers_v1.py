"""capital_providers — raw landing surface for the capital-provider classification enrichment
(append-only, /land only).

Endpoint (mounted at ``/api/v1/capital-providers``, service-token gated):
  POST /land   → land ONE capital-provider record, idempotent (byte-identical resends are no-ops)

WIRE CONTRACT::

    {
      "company_name":         "Wagner Equipment Co.",            # optional
      "domain":               "wagnerequipment.com",             # REQUIRED (bridge key)
      "company_linkedin_url": "https://www.linkedin.com/...",    # optional (if known)
      "description":          "Cat dealer serving Colorado...",  # optional (prospeo description)
      "clay_table_url":       "https://app.clay.com/...",        # optional provenance (Clay table)
      "raw_payload":          { ... the LLM capital-provider object, EXACTLY as emitted ... }
    }

PROVENANCE. raw_payload (capitalType / providesCapital / …) is an IN-HOUSE LLM pass, NOT a
prospeo.io vendor field. Stored dual: raw_payload jsonb (immutable SoT — so re-running the same
enrichment on new entities just appends and the full object is always recoverable) + a flat
projection of {capital_provider_type_category(=capitalType), provides_capital, confidence,
reasoning, source_urls, evidence_phrases, steps_taken}. domain stored verbatim; domain_norm is the
canonical bridge. PK = sha256(domain_norm | sha256(canonical_json(raw_payload))) — resends
idempotent; different payloads for the same domain land as DISTINCT rows.
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

router = APIRouter(prefix="/api/v1/capital-providers", tags=["capital-providers"])


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


def _b(v: Any) -> bool | None:
    return v if isinstance(v, bool) else None


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
    "record_id", "company_name", "domain", "domain_norm", "company_linkedin_url", "description",
    "capital_provider_type_category", "provides_capital", "confidence", "reasoning",
    "source_urls", "evidence_phrases", "steps_taken",
    "source", "clay_table_url", "raw_payload",
)
_INSERT_SQL = (
    f"INSERT INTO gtm.company_capital_providers ({', '.join(_COLS)}) "
    f"VALUES ({', '.join(['%s'] * len(_COLS))}) "
    f"ON CONFLICT (record_id) DO NOTHING"
)

_SOURCE = "prospeo_capital_llm"


def _to_row(body: dict[str, Any], rec: dict[str, Any]) -> tuple | None:
    # `domain` is the operator's wire field; accept `company_domain` as an alias for parity with
    # the sibling landing routers.
    domain = _s(body.get("domain") or body.get("company_domain"))
    if not domain:
        return None
    domain_norm = _normalize_domain(domain)
    if not domain_norm:
        return None

    source_urls = _list_of_str(rec.get("sourceUrls"))
    evidence_phrases = _list_of_str(rec.get("evidencePhrases"))
    steps_taken = _list_of_str(rec.get("stepsTaken"))

    record_id = _sha(domain_norm + "|" + _sha(_canonical_json(rec)))

    def _j(v: Any) -> Jsonb | None:
        return Jsonb(v) if v is not None else None

    return (
        record_id, _s(body.get("company_name")), domain, domain_norm,
        _s(body.get("company_linkedin_url")), _s(body.get("description")),
        _s(rec.get("capitalType")), _b(rec.get("providesCapital")), _s(rec.get("confidence")),
        _s(rec.get("reasoning")),
        _j(source_urls), _j(evidence_phrases), _j(steps_taken),
        _SOURCE, _s(body.get("clay_table_url")), Jsonb(rec),
    )


@router.post("/land", dependencies=[Depends(require_service_token)])
async def land(body: dict[str, Any]) -> dict[str, Any]:
    """Land ONE capital-provider record. Body is
    ``{"domain": "...", "raw_payload": {...}, company_name?, company_linkedin_url?, description?,
    clay_table_url?}``. The domain comes from the body only — raw_payload is never consulted."""
    rec = body.get("raw_payload")
    if not isinstance(rec, dict):
        raise HTTPException(status_code=422, detail="raw_payload must be a JSON object")
    if body.get("clay_table_url") is not None and not isinstance(body.get("clay_table_url"), str):
        raise HTTPException(status_code=422, detail="clay_table_url must be a string when present")
    domain_in = body.get("domain") or body.get("company_domain")
    if not isinstance(domain_in, str) or not domain_in.strip():
        raise HTTPException(status_code=422, detail="domain is required (non-empty string)")

    row = _to_row(body, rec)
    if row is None:
        logger.warning(
            "capital_providers land rejected: unresolvable domain (had_input=%s)",
            bool(_s(domain_in)),
        )
        raise HTTPException(
            status_code=422,
            detail="domain did not normalize to a usable bridge key",
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
        "domain": row[2],
        "domain_norm": row[3],
        "capital_provider_type_category": row[6],
        "provides_capital": row[7],
    }
