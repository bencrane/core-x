"""Async ENVELOPE projector — mirror a Documenso webhook event into business.documenso_envelopes.

Runs as a FastAPI BackgroundTask AFTER the webhook route returns 200 (ack fast, sync after). The route
already landed the RAW event (the system of record); this pulls the FULL live envelope and upserts a
VERBATIM mirror row. Fire-and-forget: every failure is logged, NEVER raised.

HARD CONTRACT:
  * documenso_response is stored EXACTLY as get_envelope returns it (no rewrite).
  * type/status are lowercased ONLY — Documenso's own terms, NEVER remapped ('CANCELLED' -> 'cancelled',
    never 'voided'). No derived/normalized states.
  * DELETE events soft-delete with NO API pull.
  * NEVER write business.documenso_template_configs.
"""
from __future__ import annotations

import logging
from typing import Any

from ..db import get_db_connection
from ..services import documenso_client
from . import queries

logger = logging.getLogger(__name__)


def _to_int(v: Any) -> int | None:
    """Coerce a numeric id (int or digit-string) to int; None for anything else."""
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, str) and v.strip().lstrip("-").isdigit():
        return int(v)
    return None


def _lower_str(v: Any) -> str:
    """Lowercase a Documenso term VERBATIM (no remap). Empty string for None/missing."""
    return str(v).lower() if v is not None else ""


async def project_envelope_event(event: str, raw_payload: dict) -> None:
    """Mirror one Documenso webhook event into business.documenso_envelopes.

    DELETE events (event ends in ``DELETED``) soft-delete the matching row with NO API pull. Every other
    event pulls the FULL live envelope (GET /api/v2/envelope/{envelopeId}) and upserts it verbatim.

    Resilient: catches DocumensoError and any Exception, logs, and returns — it runs detached as a
    background task, so a raise would have nowhere to surface.
    """
    try:
        inner = raw_payload.get("payload") or raw_payload.get("data") or raw_payload
        if not isinstance(inner, dict):
            logger.info("documenso projector: non-dict inner payload for event=%s — skipping", event)
            return

        documenso_id = _to_int(inner.get("id"))
        envelope_id = inner.get("envelopeId")
        if documenso_id is None or not envelope_id:
            # Legacy events lack envelopeId (or carry a non-numeric id) — nothing to mirror.
            logger.info(
                "documenso projector: missing id/envelopeId (id=%r envelopeId=%r) for event=%s — skipping",
                inner.get("id"), envelope_id, event,
            )
            return
        envelope_id = str(envelope_id)

        if event.upper().endswith("DELETED"):
            # Soft-delete: NO API pull. Status comes off the webhook inner body (verbatim, lowercased).
            status = _lower_str(inner.get("status")) or None
            async with get_db_connection() as conn:
                affected = await queries.soft_delete_envelope(
                    conn, documenso_id=documenso_id, status=status
                )
            logger.info(
                "documenso projector: soft-deleted documenso_id=%s rows=%d (event=%s)",
                documenso_id, affected, event,
            )
            return

        # Non-delete: pull the FULL live envelope and upsert verbatim.
        env = await documenso_client.get_envelope(envelope_id)
        if not isinstance(env, dict):
            logger.warning(
                "documenso projector: get_envelope(%s) returned non-dict for event=%s — skipping",
                envelope_id, event,
            )
            return

        async with get_db_connection() as conn:
            await queries.upsert_envelope(
                conn,
                documenso_id=documenso_id,
                envelope_id=envelope_id,
                secondary_id=(str(env["secondaryId"]) if env.get("secondaryId") is not None else None),
                type_=_lower_str(env.get("type") or env.get("source")),
                template_documenso_id=_to_int(env.get("templateId")),
                external_id=(str(env["externalId"]) if env.get("externalId") is not None else None),
                title=(str(env["title"]) if env.get("title") is not None else None),
                status=_lower_str(env.get("status")) or None,
                documenso_response=env,
            )
        logger.info(
            "documenso projector: upserted envelope documenso_id=%s envelope=%s type=%s status=%s (event=%s)",
            documenso_id, envelope_id, _lower_str(env.get("type") or env.get("source")),
            _lower_str(env.get("status")), event,
        )
    except documenso_client.DocumensoError:
        logger.exception("documenso projector: Documenso API error for event=%s — not retried", event)
    except Exception:  # noqa: BLE001 — fire-and-forget background task; never raise
        logger.exception("documenso projector: unexpected error for event=%s", event)
