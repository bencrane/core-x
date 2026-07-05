"""Icypeas webhooks — RAW landing (system of record) for company-scrape results.

  POST /webhooks/icypeas/item       webhookUrlItem     — one scraped company per delivery
  POST /webhooks/icypeas/bulk-done  webhookUrlBulkDone — one per finished bulk (stats)

Icypeas delivers a POST body of ``{signature, timestamp, data}``. The signature is
``HMAC-SHA1(ICYPEAS_API_SECRET, lower(url_pathname + timestamp))`` as a hex digest — it covers the
endpoint PATH + timestamp, NOT the body (verified against api-doc.icypeas.com/push-notifs 2026-07-04).
Because the signature does not bind the body, a captured delivery is replayable, so we LOG timestamp
staleness (unit-agnostic) as defense-in-depth — landings are append-only + dedup-at-projection, so a
replay is low-risk and we never reject on a timestamp-unit guess that could drop every real delivery.

Verified body → append-insert the RAW body verbatim into ``business.icypeas_webhook_events``. No
projection here — that is a SEPARATE, revisable step decided against the captured payloads
(Directive 28 raw-first doctrine). Signature-gated; NOT service-token gated (vendor self-authenticates).
"""
from __future__ import annotations

import hashlib
import hmac
import logging
import time
from typing import Any

from fastapi import APIRouter, HTTPException, Request

from .. import config
from ..db import get_db_connection
from ..icypeas_webhooks import queries

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/webhooks/icypeas", tags=["icypeas-webhooks"])

# Log (not reject) deliveries whose signed timestamp is this far from now — see the module docstring.
_TS_TOLERANCE_S = 15 * 60


def _dig(obj: Any, *keys: str) -> Any:
    """First present, non-null key from a dict (defensive across payload-shape variants)."""
    if isinstance(obj, dict):
        for k in keys:
            if obj.get(k) is not None:
                return obj[k]
    return None


def _verify(path: str, timestamp: str, signature: str, secret: str) -> bool:
    """Icypeas scheme: ``HMAC-SHA1(secret, lower(pathname + timestamp))`` hex, constant-time compared.
    ``path`` is the URL pathname Icypeas signed (the registered webhook path)."""
    payload = f"{path}{timestamp}".lower()
    expected = hmac.new(secret.encode(), payload.encode(), hashlib.sha1).hexdigest()
    return hmac.compare_digest(expected, (signature or "").lower())


async def _capture(request: Request, kind: str) -> dict[str, Any]:
    secret = config.icypeas_webhook_secret()
    if secret is None:
        raise HTTPException(status_code=503, detail="ICYPEAS_API_SECRET not configured")

    try:
        body = await request.json()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail="invalid JSON body") from exc
    raw: Any = body if isinstance(body, dict) else {"_raw": body}

    signature = _dig(raw, "signature")
    timestamp = _dig(raw, "timestamp")
    if not signature or timestamp is None:
        raise HTTPException(status_code=401, detail="missing signature/timestamp")

    # Verify against the request path and common variants (trailing slash), robust to proxy rewrites.
    path = request.url.path
    candidates = {path, path.rstrip("/"), path + "/"}
    if not any(_verify(p, str(timestamp), str(signature), secret) for p in candidates):
        raise HTTPException(status_code=401, detail="invalid Icypeas signature")

    # Defense-in-depth replay window (log-only; signature is the authenticity gate). Tolerate a
    # seconds OR milliseconds timestamp so a unit guess never rejects a legitimately-signed delivery.
    try:
        ts = float(timestamp)
        now = time.time()
        skew = min(abs(now - ts), abs(now - ts / 1000.0))
        if skew > _TS_TOLERANCE_S:
            logger.warning("icypeas webhook: timestamp skew %.0fs beyond %ds (kind=%s) — landing anyway",
                           skew, _TS_TOLERANCE_S, kind)
    except (TypeError, ValueError):
        logger.warning("icypeas webhook: unparseable timestamp %r (kind=%s)", timestamp, kind)

    # Best-effort lookup extracts ONLY — `payload` (the full body) is the system of record. Icypeas
    # nests the result under `data`; the item handle is its `_id`, the bulk handle is `file`, and the
    # externalId we stamp at submit IS the requested LinkedIn company URL.
    data = _dig(raw, "data") or {}
    item_id = _dig(data, "_id", "id")
    file_id = _dig(data, "file", "fileId") or _dig(raw, "file")
    status = _dig(data, "status")
    external_id = _dig(data, "externalId") or _dig(_dig(data, "custom") or {}, "externalId")
    company_url = external_id if isinstance(external_id, str) and "linkedin.com" in external_id else (
        _dig(data, "url") or _dig(_dig(data, "results") or {}, "url", "linkedinUrl", "companyUrl")
    )

    async with get_db_connection() as conn:
        event_id = await queries.insert_event(
            conn,
            kind=kind,
            item_id=str(item_id) if item_id is not None else None,
            file_id=str(file_id) if file_id is not None else None,
            status=str(status) if status is not None else None,
            external_id=str(external_id) if external_id is not None else None,
            company_url=str(company_url) if company_url is not None else None,
            signature_ts=str(timestamp),
            payload=raw,
        )
    logger.info("icypeas webhook captured: kind=%s id=%s item=%s file=%s status=%s",
                kind, event_id, item_id, file_id, status)
    return {"ok": True, "id": event_id}


@router.post("/item")
async def icypeas_item(request: Request) -> dict[str, Any]:
    """webhookUrlItem — one scraped company. Verify signature → RAW land → ACK 200."""
    return await _capture(request, kind="scrape_item")


@router.post("/bulk-done")
async def icypeas_bulk_done(request: Request) -> dict[str, Any]:
    """webhookUrlBulkDone — a finished bulk (stats). Verify signature → RAW land → ACK 200."""
    return await _capture(request, kind="bulk_done")
