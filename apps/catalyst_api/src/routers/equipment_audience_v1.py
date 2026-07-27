"""equipment-audience — read surface over the equipment_audience_people mart.

The GTM outreach read-side for the equipment-yard universe (spec:
hq/2026-07-26-equipment-audience-people-audit-and-spec.md). Two endpoints,
both service-token gated, both read-only per the gateway doctrine:

  POST /api/v1/equipment-audience/person
    {"person_key"?, "linkedin"?, "domain"?}
    → enrichment surface (Clay HTTP column / trigger.dev lookup). ALWAYS 200:
      a no-match is normal data ({"found": false}), never an error, so an
      automation row can't fail on it. domain returns every person at the
      company; person_key/linkedin return that person (+ company context).

  POST /api/v1/equipment-audience/select
    {"tiers"?, "macro_regions"?, "demo_regions"?, "title_classes"?,
     "email_status"?, "source_planes"?, "max_people_at_domain"?,
     "campaign_id"?, "exclude_pushed"? (default true), "limit"? (default 500)}
    → the send-batch selector. Anti-joined against ops.equipment_outreach_pushes
      (HQX pg, campaign-scoped when campaign_id given, any-campaign otherwise) so
      a person already pushed is never re-selected. exclude_pushed=true with the
      pg pool down is a 503 — degrading silently would double-push.

The mart is 9k rows and re-baked in place (scripts/demo_bakes/
bake_audience_people.py, mode="overwrite"); it is served from an in-process
TTL cache (default 300 s) — no BTREE needed at this size, staleness bounded
by the TTL. Ledger writes live on edge_api (/internal/equipment-outreach/*);
this gateway only ever reads the ledger for the anti-join.
"""
from __future__ import annotations

import logging
import os
import threading
import time
from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException

from .. import config
from ..db import get_db_connection
from ..service_token import require_service_token

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/equipment-audience", tags=["equipment-audience"])

_MART_URI = "s3://data-sink/active/equipment_audience_people"
_TTL_S = int(os.environ.get("EQUIPMENT_AUDIENCE_TTL_SECONDS", "300"))
_SELECT_LIMIT_DEFAULT = 500
_SELECT_LIMIT_MAX = 5000

_TIERS = {"T1", "T2", "T3", "T4"}
_SOURCE_PLANES = {"domain", "sam", "both"}

# ── mart cache (TTL, stale-while-error) ───────────────────────────────────────
_cache_lock = threading.Lock()
_cache_rows: list[dict[str, Any]] | None = None
_cache_at: float = 0.0
_cache_stamp: str | None = None


def _load_mart() -> list[dict[str, Any]]:
    import lance

    ds = lance.dataset(_MART_URI, storage_options=config.r2_storage_options())
    tbl = ds.to_table()
    cols = tbl.column_names
    data = {c: tbl.column(c).to_pylist() for c in cols}
    n = tbl.num_rows
    return [{c: data[c][i] for c in cols} for i in range(n)]


def _rows() -> list[dict[str, Any]]:
    global _cache_rows, _cache_at, _cache_stamp
    now = time.time()
    if _cache_rows is not None and now - _cache_at < _TTL_S:
        return _cache_rows
    with _cache_lock:
        if _cache_rows is not None and time.time() - _cache_at < _TTL_S:
            return _cache_rows
        try:
            rows = _load_mart()
        except Exception as exc:  # noqa: BLE001 — stale-while-error
            if _cache_rows is not None:
                logger.warning("equipment-audience mart refresh failed (serving stale): %s", exc)
                _cache_at = time.time()  # back off a full TTL before retrying
                return _cache_rows
            raise HTTPException(status_code=503, detail="audience mart unreachable") from exc
        _cache_rows = rows
        _cache_at = time.time()
        _cache_stamp = rows[0].get("materialized_at") if rows else None
        logger.info("equipment-audience mart loaded: %d rows (stamp %s)", len(rows), _cache_stamp)
        return rows


def _norm_domain(raw: str) -> str:
    d = (raw or "").strip().lower()
    for pre in ("https://", "http://"):
        if d.startswith(pre):
            d = d[len(pre):]
    d = d.split("/", 1)[0].split("?", 1)[0].split(":", 1)[0]
    return d[4:] if d.startswith("www.") else d


def _norm_linkedin(raw: str) -> str:
    s = (raw or "").strip().lower()
    for pre in ("https://", "http://"):
        if s.startswith(pre):
            s = s[len(pre):]
    if s.startswith("www."):
        s = s[4:]
    return s.rstrip("/")


def _person_payload(r: dict[str, Any]) -> dict[str, Any]:
    return {k: r.get(k) for k in (
        "person_key", "linkedin_url_norm", "full_name", "first_name", "last_name",
        "title", "priority_tier", "title_class", "dm_class", "domain_norm",
        "company_name", "uei", "macro_region", "demo_region", "industries_topline",
        "equipment_sample", "matched_psc_count", "n_people_at_domain",
        "email", "email_status", "source_plane", "loc_city", "loc_state",
    )}


@router.post("/person", dependencies=[Depends(require_service_token)])
def person_lookup(body: dict[str, Any] = Body(...)) -> dict[str, Any]:
    rows = _rows()
    person_key = (body.get("person_key") or "").strip()
    linkedin = _norm_linkedin(body.get("linkedin") or "")
    domain = _norm_domain(body.get("domain") or "")

    matches: list[dict[str, Any]] = []
    if person_key:
        matches = [r for r in rows if r.get("person_key") == person_key]
    elif linkedin:
        matches = [r for r in rows if r.get("linkedin_url_norm") == linkedin]
    elif domain:
        matches = [r for r in rows if r.get("domain_norm") == domain]
    else:
        raise HTTPException(status_code=422, detail="one of person_key | linkedin | domain is required")

    return {
        "found": bool(matches),
        "people": [_person_payload(r) for r in matches[:100]],
        "count": len(matches),
        "mart_stamp": _cache_stamp,
    }


def apply_segment(rows: list[dict[str, Any]], seg: dict[str, Any]) -> list[dict[str, Any]]:
    """Pure segment filter — closed vocabulary, 422 on off-shape input."""
    def _strlist(key: str, allowed: set[str] | None = None) -> list[str] | None:
        v = seg.get(key)
        if v is None:
            return None
        if not isinstance(v, list) or not all(isinstance(x, str) for x in v):
            raise HTTPException(status_code=422, detail=f"{key} must be a list of strings")
        if allowed is not None:
            bad = [x for x in v if x not in allowed]
            if bad:
                raise HTTPException(status_code=422, detail=f"{key}: unknown values {bad}")
        return v

    tiers = _strlist("tiers", _TIERS)
    macros = _strlist("macro_regions")
    demos = _strlist("demo_regions")
    tclasses = _strlist("title_classes")
    estatus = _strlist("email_status")
    planes = _strlist("source_planes", _SOURCE_PLANES)
    max_pad = seg.get("max_people_at_domain")
    if max_pad is not None and (not isinstance(max_pad, int) or max_pad < 1):
        raise HTTPException(status_code=422, detail="max_people_at_domain must be a positive int")

    out = []
    for r in rows:
        if tiers and r.get("priority_tier") not in tiers:
            continue
        if macros and r.get("macro_region") not in macros:
            continue
        if demos and r.get("demo_region") not in demos:
            continue
        if tclasses and r.get("title_class") not in tclasses:
            continue
        if estatus and r.get("email_status") not in estatus:
            continue
        if planes and r.get("source_plane") not in planes:
            continue
        if max_pad is not None:
            n = r.get("n_people_at_domain")
            if n is None or n > max_pad:
                continue
        out.append(r)
    return out


async def _pushed_keys(campaign_id: str | None) -> set[str]:
    sql = "SELECT person_key FROM ops.equipment_outreach_pushes"
    params: tuple = ()
    if campaign_id:
        sql += " WHERE campaign_id = %s"
        params = (campaign_id,)
    async with get_db_connection() as conn:
        cur = await conn.execute(sql, params)
        return {row[0] for row in await cur.fetchall()}


@router.post("/select", dependencies=[Depends(require_service_token)])
async def select_batch(body: dict[str, Any] = Body(default={})) -> dict[str, Any]:
    rows = _rows()
    matched = apply_segment(rows, body)

    exclude_pushed = body.get("exclude_pushed", True)
    campaign_id = (body.get("campaign_id") or "").strip() or None
    excluded = 0
    if exclude_pushed:
        try:
            pushed = await _pushed_keys(campaign_id)
        except HTTPException:
            raise
        except Exception as exc:  # noqa: BLE001 — never silently double-push
            logger.error("equipment-audience ledger read failed: %s", exc)
            raise HTTPException(
                status_code=503,
                detail="outreach ledger unreachable — refusing to select without the anti-join",
            ) from exc
        before = len(matched)
        matched = [r for r in matched if r.get("person_key") not in pushed]
        excluded = before - len(matched)

    limit = body.get("limit", _SELECT_LIMIT_DEFAULT)
    if not isinstance(limit, int) or limit < 1 or limit > _SELECT_LIMIT_MAX:
        raise HTTPException(status_code=422, detail=f"limit must be 1..{_SELECT_LIMIT_MAX}")
    # deterministic order: tier asc, then domain, then person_key
    matched.sort(key=lambda r: (r.get("priority_tier") or "~", r.get("domain_norm") or "~",
                                r.get("person_key") or ""))
    batch = matched[:limit]
    return {
        "people": [_person_payload(r) for r in batch],
        "meta": {
            "matched": len(matched) + excluded,
            "already_pushed": excluded,
            "returned": len(batch),
            "capped": len(matched) > limit,
            "campaign_id": campaign_id,
            "mart_stamp": _cache_stamp,
        },
    }
