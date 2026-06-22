"""equipment_catalog — raw landing surface for company-offerings research payloads (append-only).

Endpoints (mounted at ``/api/v1/equipment-catalog``, service-token gated):
  POST /land   → land ONE research record, idempotent (byte-identical resends are no-ops)
  GET  /stats  → row / distinct-company-domain / distinct-domain_norm / payload_kind counts

WIRE CONTRACT. One record per request::

    {
      "company_domain": "kwipped.com",
      "raw_payload":    { ... the entire research object, EXACTLY as your tool emitted it ... }
    }

STORAGE. Dual storage, both faithful to source:
  1. ``raw_payload`` (jsonb) — the object EXACTLY as sent. Immutable source of truth.
  2. flat typed columns — a LOSSLESS structural projection: common fields (confidence,
     reasoning, steps_taken, sources, evidence) plus per-shape fields (industries_served for
     the Ex-1 shape; provider_modes, categories, equipment_items + derived *_names/*_count for
     the Ex-2 shape). Sparse — null where the inbound shape does not carry the field.
     ``payload_kind`` is the inferred discriminator.

BRIDGE. ``domain_norm`` is computed from ``company_domain`` with the SAME canonical regex chain
firmographics_blitz uses (lower/trim → strip scheme → strip www → strip path → strip trailing
dots). It is the BTREE join key to ``firmographics_blitz.domain_norm`` (the domain-keyed
companies Lance system-of-record). ``company_domain`` is preserved verbatim — variants of the
same canonical domain land as DISTINCT rows by design (research outputs are append-only history).

GRAIN: one row per (domain_norm × canonical-raw_payload). PK = ``record_id`` = sha256(
domain_norm "|" sha256(canonical_json(raw_payload))). Byte-identical resends idempotent via
``ON CONFLICT DO NOTHING``. A different payload for the same domain lands as a NEW row.
A record without a resolvable domain (after normalization) is rejected (422).
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

router = APIRouter(prefix="/api/v1/equipment-catalog", tags=["equipment-catalog"])


# ── normalization (mirrors pipelines/firmographics_blitz/materialize_blitz._normalized_domain) ──
_SCHEME_RE = re.compile(r"^https?://", flags=re.I)
_WWW_RE = re.compile(r"^www\.", flags=re.I)
_PATH_RE = re.compile(r"/.*$")
_TRAIL_DOTS_RE = re.compile(r"\.+$")


def _normalize_domain(raw: Any) -> str | None:
    """Canonical domain bridge — lower/trim → strip scheme → strip www → strip path → strip
    trailing dots. NULL on empty / non-string input. Identical to the chain firmographics_blitz
    uses, so join lossless on domain_norm."""
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


# ── small projection helpers ──
def _s(v: Any) -> str | None:
    """Verbatim text projection — trims surrounding whitespace only, never interprets."""
    return v.strip() if isinstance(v, str) and v.strip() else None


def _list_of_str(v: Any) -> list[str] | None:
    """text[] projection — keeps only the str leaves; None when absent / not a list / empty."""
    if not isinstance(v, list):
        return None
    out = [x.strip() for x in v if isinstance(x, str) and x.strip()]
    return out or None


def _list_of_obj(v: Any) -> list[dict[str, Any]] | None:
    """object[] projection — keeps only the dict leaves; None when absent / not a list / empty."""
    if not isinstance(v, list):
        return None
    out = [x for x in v if isinstance(x, dict)]
    return out or None


def _derive_names(objs: list[dict[str, Any]] | None) -> list[str] | None:
    """Pull the .name leaf from each object, preserving order. None when no usable names."""
    if not objs:
        return None
    names: list[str] = []
    for o in objs:
        n = _s(o.get("name"))
        if n:
            names.append(n)
    return names or None


def _derive_sources(rec: dict[str, Any]) -> list[str] | None:
    """Explicit sources[] when present (Ex-1 shape); else dedup the urls inside evidence[]
    (Ex-2 shape) preserving first-seen order. None if neither source nor evidence carry urls."""
    sources = _list_of_str(rec.get("sources"))
    if sources:
        return sources
    ev = _list_of_obj(rec.get("evidence"))
    if not ev:
        return None
    seen, out = set(), []
    for e in ev:
        u = _s(e.get("url"))
        if u and u not in seen:
            seen.add(u)
            out.append(u)
    return out or None


def _infer_kind(rec: dict[str, Any]) -> str:
    """Discriminate the payload shape from the top-level keys present.
    industries_served — Ex-1 shape (industriesServed[] present, no equipment/categories/modes).
    equipment_offerings — Ex-2 shape (any of providerModes / categories / equipmentItems present).
    mixed — both populated.
    unknown — neither shape's fingerprint is present (still landed; flat columns sparse)."""
    has_is = isinstance(rec.get("industriesServed"), list) and rec["industriesServed"]
    has_eo = any(
        isinstance(rec.get(k), list) and rec[k]
        for k in ("providerModes", "categories", "equipmentItems")
    )
    if has_is and has_eo:
        return "mixed"
    if has_is:
        return "industries_served"
    if has_eo:
        return "equipment_offerings"
    return "unknown"


def _canonical_json(obj: Any) -> str:
    """Order-insensitive JSON dump for hashing — sort_keys so semantically-equal payloads with
    different key orderings hash to the SAME record_id. ensure_ascii=False keeps unicode
    intact; separators kill trailing whitespace differences."""
    return json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def _sha(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


# Column order is the single source of truth for the INSERT placeholders below.
_COLS = (
    "record_id", "company_domain", "domain_norm", "payload_kind",
    "confidence", "reasoning", "steps_taken", "sources", "evidence",
    "industries_served",
    "provider_modes", "categories", "category_names",
    "equipment_items", "equipment_item_names", "equipment_item_count",
    "source", "raw_payload",
)
_INSERT_SQL = (
    f"INSERT INTO gtm.equipment_catalog ({', '.join(_COLS)}) "
    f"VALUES ({', '.join(['%s'] * len(_COLS))}) "
    f"ON CONFLICT (record_id) DO NOTHING"
)

_STATS_SQL = """
    SELECT count(*)                              AS rows,
           count(DISTINCT company_domain)        AS distinct_company_domains,
           count(DISTINCT domain_norm)           AS distinct_domain_norms,
           count(*) FILTER (WHERE payload_kind = 'industries_served')   AS kind_industries_served,
           count(*) FILTER (WHERE payload_kind = 'equipment_offerings') AS kind_equipment_offerings,
           count(*) FILTER (WHERE payload_kind = 'mixed')               AS kind_mixed,
           count(*) FILTER (WHERE payload_kind = 'unknown')             AS kind_unknown
    FROM gtm.equipment_catalog
"""

_SOURCE = "equipment_catalog"


def _to_row(company_domain_raw: str, rec: dict[str, Any]) -> tuple | None:
    """Project (company_domain, raw_payload) → the full column tuple. None when the company_domain
    does not normalize to a usable bridge key (unidentifiable for the join)."""
    company_domain = _s(company_domain_raw)
    if not company_domain:
        return None
    domain_norm = _normalize_domain(company_domain)
    if not domain_norm:
        return None

    payload_kind = _infer_kind(rec)
    steps_taken = _list_of_str(rec.get("stepsTaken"))
    sources = _derive_sources(rec)
    evidence = _list_of_obj(rec.get("evidence"))

    industries_served = _list_of_str(rec.get("industriesServed"))

    provider_modes = _list_of_str(rec.get("providerModes"))
    categories = _list_of_obj(rec.get("categories"))
    category_names = _derive_names(categories)
    equipment_items = _list_of_obj(rec.get("equipmentItems"))
    equipment_item_names = _derive_names(equipment_items)
    equipment_item_count = len(equipment_items) if equipment_items else None

    record_id = _sha(domain_norm + "|" + _sha(_canonical_json(rec)))

    def _j(v: Any) -> Jsonb | None:
        return Jsonb(v) if v is not None else None

    return (
        record_id, company_domain, domain_norm, payload_kind,
        _s(rec.get("confidence")), _s(rec.get("reasoning")),
        _j(steps_taken), _j(sources), _j(evidence),
        _j(industries_served),
        _j(provider_modes), _j(categories), _j(category_names),
        _j(equipment_items), _j(equipment_item_names), equipment_item_count,
        _SOURCE, Jsonb(rec),
    )


@router.post("/land", dependencies=[Depends(require_service_token)])
async def land(body: dict[str, Any]) -> dict[str, Any]:
    """Land ONE research record. Body is ``{"company_domain": "...", "raw_payload": {...}}``.
    Stores raw_payload verbatim + the flat structural projection. Idempotent on (domain_norm,
    canonical raw_payload)."""
    company_domain = body.get("company_domain")
    rec = body.get("raw_payload")
    if not isinstance(rec, dict):
        raise HTTPException(status_code=422, detail="raw_payload must be a JSON object")
    if not isinstance(company_domain, str) or not company_domain.strip():
        raise HTTPException(status_code=422, detail="company_domain is required (non-empty string)")

    row = _to_row(company_domain, rec)
    if row is None:
        logger.warning(
            "equipment_catalog land rejected: unresolvable company_domain (had_input=%s)",
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
        "landed": landed,              # False ⇒ byte-identical payload for this domain was already present
        "already_present": not landed,
        "record_id": row[0],
        "company_domain": row[1],
        "domain_norm": row[2],
        "payload_kind": row[3],
    }


@router.post("/check", dependencies=[Depends(require_service_token)])
async def check(body: dict[str, Any]) -> dict[str, Any]:
    """Has the equipment-catalog enrichment been done for this domain? POST body:
    ``{"company_domain": "..."}``. Returns ``enriched`` (bool) plus the record count,
    most-recent ``landed_at``, latest ``payload_kind`` / ``confidence`` / counts. Domain
    normalized identically to /land so both endpoints agree."""
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
                WITH mr AS (
                    SELECT max(landed_at) AS t FROM gtm.equipment_catalog WHERE domain_norm = %s
                )
                SELECT count(*) AS n,
                       (SELECT t FROM mr) AS most_recent_at,
                       max(payload_kind)         FILTER (WHERE landed_at = (SELECT t FROM mr)) AS latest_payload_kind,
                       max(confidence)           FILTER (WHERE landed_at = (SELECT t FROM mr)) AS latest_confidence,
                       max(equipment_item_count) FILTER (WHERE landed_at = (SELECT t FROM mr)) AS latest_equipment_item_count
                FROM gtm.equipment_catalog
                WHERE domain_norm = %s
                """,
                (domain_norm, domain_norm),
            )
            r = await cur.fetchone()
    return {
        "company_domain": company_domain,
        "domain_norm": domain_norm,
        "enriched": r[0] > 0,
        "record_count": r[0],
        "most_recent_at": r[1].isoformat() if r[1] else None,
        "latest_payload_kind": r[2],
        "latest_confidence": r[3],
        "latest_equipment_item_count": r[4],
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
        "kind_industries_served": r[3],
        "kind_equipment_offerings": r[4],
        "kind_mixed": r[5],
        "kind_unknown": r[6],
    }
