"""On-demand RE-GRAB — pull a live Documenso template/envelope and re-mirror it VERBATIM.

The pull side of the envelope mirror, decoupled from the webhook. Resolves an ``envelope_id`` for a
numeric ``documenso_id`` (the mirror row, or a live ``GET /api/v2/template/{id}`` fallback), pulls the
FULL live envelope (``documenso_client.get_envelope``), and upserts via the SAME
``queries.upsert_envelope`` the webhook projector uses — IDENTICAL field extraction and verbatim
contract (``documenso_response`` stored exactly as returned; ``type``/``status`` lowercased ONLY,
NEVER remapped). There is NO second upsert path.

Resilient: a ``DocumensoError`` returns ``{synced: False, error}`` rather than raising — a re-grab of a
deleted/unreachable template degrades cleanly. Writes ONLY gc.documenso_envelopes.
"""
from __future__ import annotations

import logging
from typing import Any

from ..db import get_db_connection
from ..services import documenso_client
from . import queries
from .projector import _lower_str, _to_int

logger = logging.getLogger(__name__)


async def _resolve_envelope_id(documenso_id: int) -> str | None:
    """Resolve the prefixed ``envelope_id`` for a numeric ``documenso_id``.

    Mirror row first (the listed-template case — it already carries the handle); else fall back to the
    live template read ``GET /api/v2/template/{documenso_id}`` → ``envelopeId``. Returns None only when
    neither the mirror nor Documenso yields a handle.
    """
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT envelope_id FROM gc.documenso_envelopes WHERE documenso_id = %s",
                (documenso_id,),
            )
            row = await cur.fetchone()
    if row and row[0]:
        return str(row[0])
    # Fallback: the mirror row is absent — resolve the envelope handle live off the template endpoint.
    async with documenso_client._client() as client:  # noqa: SLF001 — reuse the configured client
        envelope_id = await documenso_client._resolve_template_envelope_id(client, str(documenso_id))
    return str(envelope_id) if envelope_id else None


async def resync_template_by_documenso_id(documenso_id: int) -> dict[str, Any]:
    """RE-GRAB one template/envelope by numeric ``documenso_id`` and re-mirror it VERBATIM.

    Resolves the envelope handle (mirror row, else live ``GET /api/v2/template/{id}``), pulls the FULL
    live envelope, and upserts through ``queries.upsert_envelope`` with the EXACT field extraction the
    webhook projector uses (no duplicate upsert logic, no remap). Returns
    ``{documenso_id, field_count, synced: True}`` on success; on ``DocumensoError`` returns
    ``{documenso_id, synced: False, error}`` (truncated) WITHOUT raising.
    """
    try:
        envelope_id = await _resolve_envelope_id(documenso_id)
        if not envelope_id:
            return {
                "documenso_id": documenso_id,
                "synced": False,
                "error": "no envelope_id (mirror row absent and template/get yielded no envelopeId)",
            }

        env = await documenso_client.get_envelope(envelope_id)
        if not isinstance(env, dict):
            return {
                "documenso_id": documenso_id,
                "synced": False,
                "error": f"get_envelope({envelope_id}) returned non-dict",
            }

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
            "documenso resync: re-mirrored documenso_id=%s envelope=%s type=%s status=%s",
            documenso_id, envelope_id, _lower_str(env.get("type") or env.get("source")),
            _lower_str(env.get("status")),
        )
        return {
            "documenso_id": documenso_id,
            "field_count": len(env.get("fields") or []),
            "synced": True,
        }
    except documenso_client.DocumensoError as e:
        logger.warning("documenso resync: Documenso error for documenso_id=%s: %s", documenso_id, e)
        return {"documenso_id": documenso_id, "synced": False, "error": str(e)[:200]}
