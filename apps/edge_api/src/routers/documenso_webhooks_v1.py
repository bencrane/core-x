"""Documenso webhook — RAW landing (system of record).

  POST /api/v1/documenso/webhook   X-Documenso-Secret — capture every Documenso event verbatim.

Documenso is repointed here from the legacy ``/api/v1/proposals/webhook`` (same shared
``DOCUMENSO_WEBHOOK_SECRET``). This route verifies the secret and append-inserts the RAW body into
``business.documenso_webhook_events`` — EVERY event type, no filtering, no normalization, no
projection. The legacy proposals webhook is untouched (it simply stops receiving deliveries).

Projection of these raw events into engagement/signing state is a SEPARATE, revisable step decided
against the captured payloads — never inferred ahead of real data.
"""
from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Header, HTTPException, Request

from .. import config
from ..db import get_db_connection
from ..documenso_webhooks import queries
from ..services import documenso_client

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/documenso", tags=["documenso-webhooks"])


def _dig(obj: Any, *keys: str) -> Any:
    """First present, non-null key from a dict (defensive across payload-shape variants)."""
    if isinstance(obj, dict):
        for k in keys:
            if obj.get(k) is not None:
                return obj[k]
    return None


@router.post("/webhook")
async def documenso_webhook(
    request: Request, x_documenso_secret: str | None = Header(default=None)
) -> dict[str, Any]:
    """Capture a Documenso webhook delivery RAW. Secret-gated; append-only; no projection here."""
    if config.documenso_webhook_secret() is None:
        raise HTTPException(status_code=503, detail="webhook secret not configured")
    if not documenso_client.verify_webhook_secret(x_documenso_secret):
        raise HTTPException(status_code=401, detail="invalid webhook secret")

    try:
        body = await request.json()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail="invalid JSON body") from exc
    # Always persist SOMETHING raw, even if the body isn't a JSON object.
    raw: Any = body if isinstance(body, dict) else {"_raw": body}

    # Best-effort lookup extracts ONLY — `payload` (the full body) is the system of record. Documenso
    # nests the entity under `payload`/`data`; the envelope handle is its `id` (prefixed envelope_…),
    # and `externalId` is what we stamp at originate (= opportunity_id).
    event = _dig(raw, "event")
    inner = _dig(raw, "payload", "data") or {}
    envelope_id = _dig(inner, "id", "documentId", "envelopeId") or _dig(raw, "id", "envelopeId")
    external_id = _dig(inner, "externalId") or _dig(raw, "externalId")

    async with get_db_connection() as conn:
        event_id = await queries.insert_event(
            conn,
            event=str(event) if event is not None else None,
            envelope_id=str(envelope_id) if envelope_id is not None else None,
            external_id=str(external_id) if external_id is not None else None,
            payload=raw,
        )
    logger.info("documenso webhook captured: id=%s event=%s envelope=%s", event_id, event, envelope_id)
    return {"ok": True, "id": event_id}
