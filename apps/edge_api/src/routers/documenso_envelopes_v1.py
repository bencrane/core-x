"""Documenso envelope MIRROR — LIST the mirrored templates + on-demand RE-GRAB.

  GET  /api/v1/documenso-envelopes/templates          the mirrored TEMPLATE rows (verbatim mirror)
  POST /api/v1/documenso-envelopes/{documenso_id}/resync   re-grab ONE template/envelope, re-mirror it
  POST /api/v1/documenso-envelopes/resync-all         re-grab EVERY mirrored template, sequentially

The LIST reads STRAIGHT off business.documenso_envelopes (the verbatim mirror). The RE-GRAB reuses the
webhook projector's EXACT pull+upsert (``documenso_client.get_envelope`` → ``queries.upsert_envelope``)
— same verbatim contract, no second upsert path. Re-grab NEVER writes
business.documenso_template_configs. Resilient: a per-template Documenso error surfaces as
``{synced: false, error}`` with 200, never a 5xx — a bad template can't poison the batch. Service-token
gated (the platform-api BFF brokers it).
"""
from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from ..db import get_db_connection
from ..documenso_projection import (
    list_template_documenso_ids,
    list_template_mirror,
    resync_template_by_documenso_id,
)
from ..service_token import require_service_token

router = APIRouter(
    prefix="/api/v1/documenso-envelopes",
    tags=["documenso-envelopes"],
    dependencies=[Depends(require_service_token)],
)


class TemplateMirrorRow(BaseModel):
    """One mirrored TEMPLATE envelope, projected off the verbatim mirror row."""

    documenso_id: int
    title: str | None = None
    status: str | None = None
    field_count: int
    recipient_count: int
    synced_at: datetime | None = None


class ResyncResult(BaseModel):
    """The outcome of one re-grab. ``synced`` false carries ``error``; ``field_count`` is present only
    on success (the live envelope's field count)."""

    documenso_id: int
    synced: bool
    field_count: int | None = None
    error: str | None = None


class ResyncAllResult(BaseModel):
    """The batch re-grab outcome: ``requested`` rows walked, ``synced`` succeeded, per-row ``results``."""

    requested: int
    synced: int
    results: list[ResyncResult]


@router.get("/templates")
async def list_templates() -> list[TemplateMirrorRow]:
    """The mirrored TEMPLATE envelopes (non-deleted), newest sync first."""
    async with get_db_connection() as conn:
        rows = await list_template_mirror(conn)
    return [TemplateMirrorRow(**r) for r in rows]


@router.post("/{documenso_id}/resync")
async def resync_template(documenso_id: int) -> ResyncResult:
    """RE-GRAB one template/envelope by numeric ``documenso_id`` and re-mirror it verbatim. A Documenso
    error surfaces in ``error`` with ``synced: false`` (HTTP 200) — the re-grab is resilient by design."""
    result = await resync_template_by_documenso_id(documenso_id)
    return ResyncResult(**result)


@router.post("/resync-all")
async def resync_all_templates() -> ResyncAllResult:
    """RE-GRAB every mirrored TEMPLATE, SEQUENTIALLY (one Documenso pull at a time — no fan-out). One
    failing template can't fail the batch: each per-row outcome is collected verbatim."""
    async with get_db_connection() as conn:
        ids = await list_template_documenso_ids(conn)
    results: list[ResyncResult] = []
    for documenso_id in ids:
        result = await resync_template_by_documenso_id(documenso_id)
        results.append(ResyncResult(**result))
    return ResyncAllResult(
        requested=len(ids),
        synced=sum(1 for r in results if r.synced),
        results=results,
    )


class EnvelopeFieldDetail(BaseModel):
    """One field projected off the mirrored envelope, with its recipient (assignee) resolved."""

    field_type: str
    label: str | None = None
    required: bool | None = None
    template_read_only: bool | None = None  # the TEMPLATE's readOnly flag (lock at mint is config-driven)
    recipient_id: int | None = None
    recipient_email: str | None = None
    recipient_role: str | None = None


@router.get("/{documenso_id}/fields")
async def envelope_fields(documenso_id: int) -> list[EnvelopeFieldDetail]:
    """Per-field detail off the VERBATIM mirror: type, label, required, template readOnly, and the
    recipient the field is assigned to (id/email/role). 404 when the mirror doesn't carry the id."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT documenso_response FROM business.documenso_envelopes "
                "WHERE documenso_id = %s AND deleted_at IS NULL LIMIT 1",
                (documenso_id,),
            )
            row = await cur.fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="envelope not mirrored")
    body = row[0] or {}
    recips: dict[int, dict] = {}
    for r in body.get("recipients") or []:
        if isinstance(r, dict) and r.get("id") is not None:
            try:
                recips[int(r["id"])] = r
            except (TypeError, ValueError):
                continue
    out: list[EnvelopeFieldDetail] = []
    for f in body.get("fields") or []:
        if not isinstance(f, dict):
            continue
        meta = f.get("fieldMeta") or {}
        rid = f.get("recipientId")
        try:
            rid = int(rid) if rid is not None else None
        except (TypeError, ValueError):
            rid = None
        rec = recips.get(rid) if rid is not None else None
        out.append(EnvelopeFieldDetail(
            field_type=str(f.get("type") or ""),
            label=meta.get("label") if isinstance(meta.get("label"), str) else None,
            required=meta.get("required") if isinstance(meta.get("required"), bool) else None,
            template_read_only=meta.get("readOnly") if isinstance(meta.get("readOnly"), bool) else None,
            recipient_id=rid,
            recipient_email=(rec or {}).get("email"),
            recipient_role=(rec or {}).get("role"),
        ))
    # Stable order: assignee, then labelled TEXT/NUMBER alphabetically, then signature/date.
    out.sort(key=lambda x: (x.recipient_id or 0, x.label is None, (x.label or "").lower(), x.field_type))
    return out
