"""Documenso webhook — authoritative document status advance for engagement mandates.

  POST /webhooks/documenso   X-Documenso-Secret — Documenso document lifecycle tracking

Mirrors the proposals webhook discipline:
  1. Verify the X-Documenso-Secret signature (401 on bad/missing; 503 if secret is unset).
  2. Normalize the event (extracts envelope_id, status).
  3. Find the mandate by envelope_id and update its documenso_status.

Engagement mandate documents are created with signing tokens and NOT distributed
until an explicit send action later. Only status transitions post-distribution are tracked here:
PENDING (sent to participants) → COMPLETED (signed by both parties).

Mounted at ``/webhooks/documenso`` (NOT under ``/api/v1``) — the path Documenso posts to.
"""
from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Header, HTTPException, Request

from ..config import documenso_webhook_secret
from ..db import get_db_connection
from ..documenso_documents import queries as documenso_documents_queries
from ..services import documenso_client
from ..engagement_docs import queries

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/webhooks", tags=["webhooks"])


@router.post("/documenso")
async def documenso_webhook(
    request: Request, x_documenso_secret: str | None = Header(default=None),
) -> dict[str, Any]:
    """Documenso → mandate documenso_status advance. Source of truth for document lifecycle."""
    if documenso_webhook_secret() is None:
        raise HTTPException(status_code=503, detail="webhook secret not configured")
    if not documenso_client.verify_webhook_secret(x_documenso_secret):
        raise HTTPException(status_code=401, detail="invalid webhook secret")

    body = await request.json()
    evt = documenso_client.normalize_event(body)
    if evt.status is None:
        return {"ok": True, "ignored": True, "event": evt.event}

    if not evt.envelope_id:
        return {"ok": True, "ignored": True, "event": evt.event, "reason": "no envelope_id"}

    status = evt.status.upper()  # stored uppercase for consistency

    # Re-read the authoritative envelope payload BEFORE taking a pooled connection (never hold a
    # pooled conn across a network call). Best-effort — a fetch failure degrades to a status-only
    # advance. ``evt.envelope_id`` may be a numeric document id on some payload shapes, in which case
    # the GET 404s and we fall back to the webhook event alone.
    payload: dict[str, Any] | None = None
    try:
        payload = await documenso_client.get_envelope(evt.envelope_id)
    except documenso_client.DocumensoError:
        payload = None

    async with get_db_connection() as conn:
        # AO term-only mandate lane (active-operators) — status advance by envelope_id, unchanged.
        mandate = await queries.update_documenso_status_by_envelope(
            conn, envelope_id=evt.envelope_id, documenso_status=status,
        )
        # RS direct-to-documenso document SoR — matched by the external id we stamped (the draft id)
        # or the envelope id; advances status, appends the event, refreshes payload when we have it.
        document = await documenso_documents_queries.sync_from_webhook(
            conn, match=(evt.external_id or evt.envelope_id), status=status,
            event=body, payload=payload,
        )
        await conn.commit()

    if mandate is None and document is None:
        logger.warning("documenso webhook: no mandate/document for envelope %s", evt.envelope_id)
        return {"ok": True, "ignored": True, "event": evt.event, "reason": "no matching record"}

    logger.info(
        "documenso webhook: %s → %s (mandate=%s document=%s)",
        evt.envelope_id, status,
        mandate["id"] if mandate else None,
        document["envelope_id"] if document else None,
    )
    return {
        "ok": True, "event": evt.event, "status": evt.status, "updated": True,
        "mandate": mandate["id"] if mandate else None,
        "document": document["envelope_id"] if document else None,
    }
