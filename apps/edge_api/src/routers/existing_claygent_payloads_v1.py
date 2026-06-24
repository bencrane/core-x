"""existing_claygent_payloads — universal raw landing sink for heterogeneous Claygent enrichment
payloads (append-only, /land only).

Endpoint (mounted at ``/api/v1/existing-claygent-payloads``, service-token gated):
  POST /land   → land ONE enrichment payload, idempotent (byte-identical resends are no-ops)

WIRE CONTRACT::

    {
      "domain":                  "fundbox.com",            # REQUIRED
      "enrichment_payload_type": "equipment-finance-one",  # REQUIRED — discriminator (filter on it later)
      "raw_payload":             { ... the Claygent object, EXACTLY as emitted ... }  # REQUIRED (JSON object)
    }

raw_payload is stored verbatim as jsonb — NO projection, NO typed columns, NO coercion. Claygent
emits different shapes per enrichment type; jsonb enforces no schema, so every shape lands
losslessly (including the sources-as-string-vs-array divergence seen across runs) and is sorted out
at READ time by filtering on enrichment_payload_type. PK = sha256(enrichment_payload_type |
domain_norm | sha256(canonical_json(raw_payload))) — resends idempotent; a different payload (or a
different type) for the same domain lands as a DISTINCT row.
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

router = APIRouter(
    prefix="/api/v1/existing-claygent-payloads", tags=["existing-claygent-payloads"]
)


# ── normalization (mirrors capital_providers / firmographics_blitz._normalized_domain) ──
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


def _canonical_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def _sha(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


_COLS = ("record_id", "enrichment_payload_type", "domain", "domain_norm", "raw_payload")
_INSERT_SQL = (
    f"INSERT INTO gtm.existing_claygent_payloads ({', '.join(_COLS)}) "
    f"VALUES ({', '.join(['%s'] * len(_COLS))}) "
    f"ON CONFLICT (record_id) DO NOTHING"
)


@router.post("/land", dependencies=[Depends(require_service_token)])
async def land(body: dict[str, Any]) -> dict[str, Any]:
    """Land ONE Claygent enrichment payload. Body is
    ``{"domain": "...", "enrichment_payload_type": "...", "raw_payload": {...}}``."""
    rec = body.get("raw_payload")
    if not isinstance(rec, dict):
        raise HTTPException(status_code=422, detail="raw_payload must be a JSON object")

    payload_type = _s(body.get("enrichment_payload_type"))
    if not payload_type:
        raise HTTPException(
            status_code=422,
            detail="enrichment_payload_type is required (non-empty string)",
        )

    # `domain` is the operator's wire field; accept `company_domain` as an alias for parity with
    # the sibling landing routers.
    domain = _s(body.get("domain") or body.get("company_domain"))
    if not domain:
        raise HTTPException(status_code=422, detail="domain is required (non-empty string)")
    domain_norm = _normalize_domain(domain)
    if not domain_norm:
        raise HTTPException(
            status_code=422, detail="domain did not normalize to a usable key"
        )

    record_id = _sha(payload_type + "|" + domain_norm + "|" + _sha(_canonical_json(rec)))
    row = (record_id, payload_type, domain, domain_norm, Jsonb(rec))

    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(_INSERT_SQL, row)
            landed = cur.rowcount == 1
        await conn.commit()

    return {
        "landed": landed,
        "already_present": not landed,
        "record_id": record_id,
        "domain": domain,
        "domain_norm": domain_norm,
        "enrichment_payload_type": payload_type,
    }
