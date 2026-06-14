"""Documenso template field defaults — the Settings "Documenso Templates" editor's API.

  GET  /api/v1/documenso-template-fields?documenso_template_id=   editable fields + current defaults
  POST /api/v1/documenso-template-fields/defaults                 write defaults onto the template

Reads/writes the ACTUAL Documenso template (a v2 TEMPLATE envelope) via the field endpoints. Setting
a default modifies the template; future documents created from it (``/envelope/use``) inherit it,
while already-created documents are untouched. Service-token gated — the platform-api BFF brokers it.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from ..service_token import require_service_token
from ..services import documenso_client

router = APIRouter(prefix="/api/v1/documenso-template-fields", tags=["documenso-template-fields"])


class TemplateField(BaseModel):
    id: int
    type: str                       # TEXT | NUMBER | DROPDOWN (the default-able types)
    label: str | None = None        # the operator-facing field label
    recipient_id: int | None = None
    page: int | None = None
    default: str | None = None      # the value currently baked into the template, if any


class FieldDefault(BaseModel):
    id: int                         # the Documenso field id
    value: str                      # the default to bake in (written to the type's fieldMeta key)


class SetDefaultsRequest(BaseModel):
    documenso_template_id: str
    defaults: list[FieldDefault]


@router.get("", dependencies=[Depends(require_service_token)])
async def list_template_fields(documenso_template_id: str) -> list[TemplateField]:
    """The editable fields (+ current defaults) of a live Documenso template."""
    try:
        fields = await documenso_client.get_template_fields(documenso_template_id)
    except documenso_client.DocumensoError as e:
        raise HTTPException(status_code=502, detail=f"documenso: {e}") from e
    return [TemplateField(**f) for f in fields]


@router.post("/defaults", dependencies=[Depends(require_service_token)])
async def save_template_field_defaults(body: SetDefaultsRequest) -> list[TemplateField]:
    """Write default values onto the template's fields, then return the refreshed field list."""
    mapping = {d.id: d.value for d in body.defaults}
    try:
        await documenso_client.set_template_field_defaults(body.documenso_template_id, mapping)
        fields = await documenso_client.get_template_fields(body.documenso_template_id)
    except documenso_client.DocumensoError as e:
        raise HTTPException(status_code=502, detail=f"documenso: {e}") from e
    return [TemplateField(**f) for f in fields]
