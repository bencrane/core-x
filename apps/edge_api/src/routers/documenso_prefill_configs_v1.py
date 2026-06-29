"""Documenso template-document PREFILL CONFIG editor — the "Manage Documenso Templates" prefill API.

  GET  /api/v1/documenso-template-prefill/{documenso_id}   value fields (off the MIRROR) + saved config
  PUT  /api/v1/documenso-template-prefill/{documenso_id}    upsert the operator-owned field_settings

The editable fields come from the verbatim envelope MIRROR (``business.documenso_envelopes``,
``type='template'``) — the value-bearing TEXT/NUMBER fields with a ``fieldMeta.label``. The saved config
is the OPERATOR-OWNED ``business.documenso_template_document_prefill_configs`` row: ``field_settings``
keyed by field LABEL, each value an ARBITRARY object stored verbatim (Phase 1 sets
``default_document_field_value`` + ``read_only``; Phase 2 adds ``source`` — inner keys are NOT validated
so they pass through). The default lives in OUR config, applied at ORIGINATE later (model B: deal
override ?? default) — nothing is baked onto the Documenso template here. Service-token gated (the
platform-api BFF brokers it). This editor is the SOLE writer of the config table.
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field

from ..db import get_db_connection
from ..documenso_prefill_configs import (
    get_prefill_config,
    get_template_value_fields,
    upsert_prefill_config,
)
from ..service_token import require_service_token

router = APIRouter(
    prefix="/api/v1/documenso-template-prefill",
    tags=["documenso-template-prefill"],
    dependencies=[Depends(require_service_token)],
)


class TemplateValueField(BaseModel):
    """One value-bearing template field, projected off the verbatim envelope mirror."""

    label: str
    type: str                       # TEXT | NUMBER
    required: bool | None = None
    read_only: bool | None = None
    recipient_id: int | None = None


class PrefillConfigResponse(BaseModel):
    """The editor's read payload: the template's value fields + the saved per-label settings."""

    documenso_id: int
    fields: list[TemplateValueField]
    # Keyed by field LABEL; each value is an ARBITRARY object stored verbatim (inner keys not validated).
    field_settings: dict[str, dict[str, Any]] = Field(default_factory=dict)


class PrefillConfigUpdate(BaseModel):
    """The editor's write body — the full per-label settings map to persist verbatim."""

    # Keyed by field LABEL; each value is an ARBITRARY object (Phase 2 keys pass through untouched).
    field_settings: dict[str, dict[str, Any]] = Field(default_factory=dict)


class PrefillConfigSaved(BaseModel):
    """The write result — the saved settings echoed back."""

    documenso_id: int
    field_settings: dict[str, dict[str, Any]] = Field(default_factory=dict)


@router.get("/{documenso_id}")
async def get_prefill(documenso_id: int) -> PrefillConfigResponse:
    """The template's value fields (off the mirror) plus the saved per-label prefill settings."""
    async with get_db_connection() as conn:
        fields = await get_template_value_fields(conn, documenso_id)
        field_settings = await get_prefill_config(conn, documenso_id)
    return PrefillConfigResponse(
        documenso_id=documenso_id,
        fields=[TemplateValueField(**f) for f in fields],
        field_settings=field_settings,
    )


@router.put("/{documenso_id}")
async def put_prefill(documenso_id: int, body: PrefillConfigUpdate) -> PrefillConfigSaved:
    """UPSERT the operator-owned ``field_settings`` for this template; echo the saved map back. Inner
    per-label keys are stored verbatim (not validated) so Phase 2 keys pass through."""
    async with get_db_connection() as conn:
        saved = await upsert_prefill_config(conn, documenso_id, body.field_settings)
    return PrefillConfigSaved(documenso_id=documenso_id, field_settings=saved)
