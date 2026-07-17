"""entity-resolve — batch CSV-row resolution for uploaded entity lists (gc-hq).

Contract (operator rulings 2026-07-17): the uploader mandates an exact CSV
format; resolution is EXACT-match only — `uei` verified against SAM
registrants, `domain` matched on the normalized domain across two sources
(SAM registrations + firmographics_blitz). A domain with multiple registrant
matches returns ALL candidates (with ultimate-parent family names from the
Lance `entity_hierarchy` table) for user confirmation; nothing fuzzy, ever.

Endpoint (service-token gated):
  POST /api/v1/resolve/entities
    {"ueis"?: [...], "domains"?: [...], "linkedin_urls"?: [...]}   (≤2000 each)
  → {"ueis": {<uei>: {uei, name, state, active} | null},
     "domains": {<domain>: [{uei, name, state, source, ultimate_parent_name}, ...]},
     "linkedin_urls": {<url>: [candidates, same shape]},
     "elapsed_ms", "artifact"}

LinkedIn path (2026-07-17): company URL → slug → `pdl_slug_lookup` (slug-sorted;
the unsorted probe measured 18.4s) → registrant candidates via the SAM↔PDL
bridge, UNION via the PDL company's non-generic domain against SAM
registrations — both exact hops, one statement.

Execution: one sidecar statement per identifier type (VALUES join — server-side
SQL only, inputs validated against closed shapes, never interpolated as text)
plus an in-process family lookup from `entity_hierarchy` (148k rows, Lance,
lazy-loaded once per process).
"""
from __future__ import annotations

import logging
import os
import re
from typing import Any

import httpx
from fastapi import APIRouter, Depends, HTTPException

from .. import config
from ..service_token import require_service_token

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/resolve", tags=["entity-resolve"])

SIDECAR_URL = os.environ.get("QUERY_SIDECAR_URL", "https://query-sidecar-api.onrender.com")

_UEI_RE = re.compile(r"^[A-Za-z0-9]{12}$")
# bare normalized domain: label(.label)+ — lowercase, no scheme/path/port
_DOMAIN_RE = re.compile(r"^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?(\.[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?)+$")
_MAX_BATCH = 2000
_MAX_CANDIDATES_PER_DOMAIN = 50

# ── entity_hierarchy family map (Lance, 148k rows, lazy singleton) ────────────
_hierarchy: dict[str, str] | None = None


def _family_map() -> dict[str, str]:
    global _hierarchy
    if _hierarchy is None:
        import lance

        ds = lance.dataset(
            "s3://data-sink/active/entity_hierarchy",
            storage_options=config.r2_storage_options(),
        )
        tbl = ds.to_table(columns=["uei", "ultimate_parent_name"])
        _hierarchy = {
            u: p
            for u, p in zip(tbl.column("uei").to_pylist(), tbl.column("ultimate_parent_name").to_pylist())
            if u and p
        }
        logger.info("entity_hierarchy loaded: %d family rows", len(_hierarchy))
    return _hierarchy


_SLUG_RE = re.compile(r"^[a-z0-9][a-z0-9\-_.]{0,99}$")
_LI_URL_RE = re.compile(
    r"^(?:https?://)?(?:[a-z]{2,3}\.)?linkedin\.com/company/([^/?#]+)/?", re.IGNORECASE
)


def normalize_linkedin(raw: str) -> str | None:
    """Company URL (or bare slug) → lowercase slug; None if not exact-parseable."""
    s = raw.strip()
    m = _LI_URL_RE.match(s)
    slug = (m.group(1) if m else s).strip().lower()
    return slug if _SLUG_RE.match(slug) else None


def normalize_domain(raw: str) -> str | None:
    d = raw.strip().lower()
    d = re.sub(r"^[a-z][a-z0-9+.-]*://", "", d)   # scheme
    d = d.split("/")[0].split("?")[0].split(":")[0]
    if d.startswith("www."):
        d = d[4:]
    return d if _DOMAIN_RE.match(d) else None


def _values_clause(items: list[str]) -> str:
    # items are validated against closed regexes above — safe to embed as
    # single-quoted literals (no user text reaches SQL unvalidated).
    return ", ".join(f"('{i}')" for i in items)


async def _sidecar(sql: str, limit: int) -> dict[str, Any]:
    token = os.environ.get("QUERY_SIDECAR_TOKEN")
    if not token:
        raise HTTPException(status_code=503, detail="QUERY_SIDECAR_TOKEN not configured")
    async with httpx.AsyncClient(timeout=60.0) as client:
        r = await client.post(
            f"{SIDECAR_URL}/api/v1/sql",
            headers={"Authorization": f"Bearer {token}"},
            json={"sql": sql, "limit": limit},
        )
    if r.status_code != 200:
        logger.error("sidecar resolve failed: %s %s", r.status_code, r.text[:300])
        raise HTTPException(status_code=502, detail="sidecar query failed")
    return r.json()


@router.post("/entities", dependencies=[Depends(require_service_token)])
async def resolve_entities(body: dict[str, Any]) -> dict[str, Any]:
    raw_ueis = body.get("ueis") or []
    raw_domains = body.get("domains") or []
    raw_linkedin = body.get("linkedin_urls") or []
    if not all(isinstance(x, list) for x in (raw_ueis, raw_domains, raw_linkedin)):
        raise HTTPException(status_code=422, detail="ueis/domains/linkedin_urls must be lists")
    if max(len(raw_ueis), len(raw_domains), len(raw_linkedin)) > _MAX_BATCH:
        raise HTTPException(status_code=422, detail=f"batch limit {_MAX_BATCH} per identifier type")

    ueis = sorted({u.strip().upper() for u in raw_ueis if isinstance(u, str) and _UEI_RE.match(u.strip())})
    norm_map: dict[str, str | None] = {
        d: normalize_domain(d) for d in raw_domains if isinstance(d, str) and d.strip()
    }
    domains = sorted({n for n in norm_map.values() if n})

    li_map: dict[str, str | None] = {
        u: normalize_linkedin(u) for u in raw_linkedin if isinstance(u, str) and u.strip()
    }
    slugs = sorted({s for s in li_map.values() if s})

    out: dict[str, Any] = {
        "ueis": {},
        "domains": {},
        "linkedin_urls": {},
        "elapsed_ms": 0.0,
        "artifact": None,
    }

    if ueis:
        sql = (
            f"WITH u(uei) AS (VALUES {_values_clause(ueis)}) "
            "SELECT u.uei, e.legal_business_name, e.physical_state, e.sam_is_active "
            "FROM u LEFT JOIN gtm_sam_entities e ON e.uei = u.uei"
        )
        payload = await _sidecar(sql, len(ueis) + 10)
        out["elapsed_ms"] += payload.get("elapsed_ms") or 0
        out["artifact"] = payload.get("artifact")
        for uei, name, state, active in payload.get("rows") or []:
            out["ueis"][uei] = (
                {"uei": uei, "name": name, "state": state, "active": bool(active)} if name else None
            )

    if domains:
        # Two sources, one statement: SAM registrations (authoritative) plus
        # firmographics_blitz (LinkedIn-derived domain→uei). DISTINCT on
        # (domain, uei); source label kept for disclosure.
        sql = (
            f"WITH d(domain) AS (VALUES {_values_clause(domains)}), "
            "cand AS ("
            "  SELECT d.domain, e.uei, e.legal_business_name AS name, e.physical_state AS state, 'sam' AS source"
            "  FROM d JOIN gtm_sam_entities e ON e.normalized_domain = d.domain"
            "  UNION"
            "  SELECT d.domain, f.uei, f.company_name AS name, f.hq_state AS state, 'firmographics' AS source"
            "  FROM d JOIN firmographics_blitz f ON f.domain_norm = d.domain"
            "  WHERE f.uei IS NOT NULL AND f.uei <> ''"
            ") "
            "SELECT domain, uei, max(name) AS name, max(state) AS state, "
            "       string_agg(DISTINCT source, '+' ORDER BY source) AS source "
            "FROM cand GROUP BY domain, uei ORDER BY domain, uei"
        )
        payload = await _sidecar(sql, min(len(domains) * _MAX_CANDIDATES_PER_DOMAIN, 50000))
        out["elapsed_ms"] += payload.get("elapsed_ms") or 0
        out["artifact"] = payload.get("artifact") or out["artifact"]
        fam = _family_map()
        by_domain: dict[str, list[dict[str, Any]]] = {d: [] for d in domains}
        for domain, uei, name, state, source in payload.get("rows") or []:
            if len(by_domain.get(domain, [])) >= _MAX_CANDIDATES_PER_DOMAIN:
                continue
            by_domain.setdefault(domain, []).append(
                {
                    "uei": uei,
                    "name": name,
                    "state": state,
                    "source": source,
                    "ultimate_parent_name": fam.get(uei),
                }
            )
        # answer under the CALLER's raw keys so the uploader can map rows back
        for raw, norm in norm_map.items():
            out["domains"][raw] = by_domain.get(norm, []) if norm else []

    if slugs:
        # slug → PDL company (slug-sorted lookup) → registrants via the bridge,
        # UNION via the PDL company's non-generic domain — one statement, both
        # legs DISTINCT (the bridge carries duplicate rows, probe-verified).
        sql = (
            f"WITH s(slug) AS (VALUES {_values_clause(slugs)}), "
            "p AS ("
            "  SELECT s.slug, l.pdl_company_id, l.normalized_domain, l.is_generic_domain"
            "  FROM s JOIN pdl_slug_lookup l ON l.linkedin_slug = s.slug"
            "), cand AS ("
            "  SELECT DISTINCT p.slug, b.uei, e.legal_business_name AS name,"
            "         e.physical_state AS state, 'linkedin_bridge' AS source"
            "  FROM p JOIN bridge_sam_pdl b ON b.pdl_company_id = p.pdl_company_id"
            "  JOIN gtm_sam_entities e ON e.uei = b.uei"
            "  UNION"
            "  SELECT DISTINCT p.slug, e.uei, e.legal_business_name AS name,"
            "         e.physical_state AS state, 'linkedin_domain' AS source"
            "  FROM p JOIN gtm_sam_entities e ON e.normalized_domain = p.normalized_domain"
            "  WHERE coalesce(p.is_generic_domain, TRUE) = FALSE"
            ") "
            "SELECT slug, uei, max(name) AS name, max(state) AS state, "
            "       string_agg(DISTINCT source, '+' ORDER BY source) AS source "
            "FROM cand GROUP BY slug, uei ORDER BY slug, uei"
        )
        payload = await _sidecar(sql, min(len(slugs) * _MAX_CANDIDATES_PER_DOMAIN, 50000))
        out["elapsed_ms"] += payload.get("elapsed_ms") or 0
        out["artifact"] = payload.get("artifact") or out["artifact"]
        fam = _family_map()
        by_slug: dict[str, list[dict[str, Any]]] = {s: [] for s in slugs}
        for slug, uei, name, state, source in payload.get("rows") or []:
            if len(by_slug.get(slug, [])) >= _MAX_CANDIDATES_PER_DOMAIN:
                continue
            by_slug.setdefault(slug, []).append(
                {
                    "uei": uei,
                    "name": name,
                    "state": state,
                    "source": source,
                    "ultimate_parent_name": fam.get(uei),
                }
            )
        for raw, slug in li_map.items():
            out["linkedin_urls"][raw] = by_slug.get(slug, []) if slug else []

    return out
