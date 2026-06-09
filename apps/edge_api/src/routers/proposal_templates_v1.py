"""Proposal-template authoring routes — the engine behind "Settings → Proposal Templates".

  POST /api/v1/proposal-templates/convert        service-token  markdown → HTML + detected {{tokens}}
  POST /api/v1/proposal-templates/preview        service-token  render → DocRaptor → R2 → presigned PDF link
  POST /api/v1/proposal-templates                service-token  create a draft
  GET  /api/v1/proposal-templates                service-token  list (``?published=true`` for the intake picker)
  GET  /api/v1/proposal-templates/{id}           service-token  one template (markdown included)
  PUT  /api/v1/proposal-templates/{id}           service-token  update a draft
  POST /api/v1/proposal-templates/{id}/publish   service-token  name + slug → publishable/selectable

All gated by EDGE_API_SERVICE_TOKEN — the platform-api BFF brokers them with the operator's
session on the front side. The markdown SOURCE is persisted in Postgres; preview PDFs are stashed
in R2 and handed back as a short-lived presigned URL the browser opens directly.
"""
from __future__ import annotations

import logging
import secrets
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from psycopg import errors as pg_errors

from ..db import get_db_connection
from ..proposals import template_queries as q
from ..proposals.template_models import (
    TemplateConvertRequest,
    TemplateConvertResult,
    TemplateDraftCreate,
    TemplateDraftUpdate,
    TemplatePreviewRequest,
    TemplatePreviewResult,
    TemplatePublishRequest,
    TemplateRow,
    TemplateSummary,
)
from ..proposals.template_render import (
    extract_tokens,
    render_template_html,
    substitute_tokens,
)
from ..service_token import require_service_token
from ..services import docraptor_client, r2_client

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1/proposal-templates",
    tags=["proposal-templates"],
    dependencies=[Depends(require_service_token)],
)

_PREVIEW_TTL_SECONDS = 3600


def _manifest(markdown: str, apply_brand: bool) -> list[str]:
    """Tokens detected in the FULLY ASSEMBLED document (body + brand-shell signature block)."""
    return extract_tokens(render_template_html(markdown, apply_brand=apply_brand))


@router.post("/convert")
def convert(body: TemplateConvertRequest) -> TemplateConvertResult:
    """Stateless markdown → branded HTML for the editor's live preview pane."""
    html = render_template_html(body.markdown, apply_brand=body.apply_brand)
    return TemplateConvertResult(html=html, detected_tokens=extract_tokens(html))


@router.post("/preview")
async def preview(body: TemplatePreviewRequest) -> TemplatePreviewResult:
    """Render the current content to a real PDF via DocRaptor, stash it in R2, return a presigned
    link. Stateless — previews UNSAVED content; ``token_values`` fills the {{handlebars}}."""
    html = render_template_html(body.markdown, apply_brand=body.apply_brand)
    rendered = substitute_tokens(html, body.token_values)
    try:
        pdf = await docraptor_client.render_pdf(rendered, name="proposal-template-preview")
    except docraptor_client.DocRaptorError as exc:
        raise HTTPException(status_code=502, detail=f"docraptor: {exc}") from exc
    key = f"{r2_client.PREVIEW_PREFIX}{secrets.token_urlsafe(16)}.pdf"
    try:
        await r2_client.put_pdf(key, pdf)
        url = await r2_client.presigned_get_url(key, expires_seconds=_PREVIEW_TTL_SECONDS)
    except r2_client.R2Error as exc:
        raise HTTPException(status_code=502, detail=f"r2: {exc}") from exc
    return TemplatePreviewResult(
        pdf_url=url, expires_seconds=_PREVIEW_TTL_SECONDS, detected_tokens=extract_tokens(html)
    )


@router.post("", status_code=201)
async def create_draft(body: TemplateDraftCreate) -> TemplateRow:
    tid = q.new_template_id()
    async with get_db_connection() as conn:
        row = await q.create_draft(
            conn,
            id=tid,
            markdown=body.markdown,
            apply_brand=body.apply_brand,
            name=body.name,
            token_manifest=_manifest(body.markdown, body.apply_brand),
            created_by=body.created_by,
        )
    return TemplateRow.from_row(row)


@router.get("")
async def list_templates(published: bool = False) -> list[TemplateSummary]:
    async with get_db_connection() as conn:
        rows = await q.list_all(conn, published_only=published)
    return [TemplateSummary.from_row(r) for r in rows]


@router.get("/{template_id}")
async def get_template(template_id: str) -> TemplateRow:
    async with get_db_connection() as conn:
        row = await q.get(conn, template_id)
    if row is None:
        raise HTTPException(status_code=404, detail="template not found")
    return TemplateRow.from_row(row)


@router.put("/{template_id}")
async def update_draft(template_id: str, body: TemplateDraftUpdate) -> TemplateRow:
    async with get_db_connection() as conn:
        existing = await q.get(conn, template_id)
        if existing is None:
            raise HTTPException(status_code=404, detail="template not found")
        # Recompute the token manifest when the content (markdown or brand wrap) changes.
        manifest: list[str] | None = None
        if body.markdown is not None or body.apply_brand is not None:
            md = body.markdown if body.markdown is not None else existing["markdown"]
            brand = body.apply_brand if body.apply_brand is not None else existing["apply_brand"]
            manifest = _manifest(md, brand)
        row = await q.update_draft(
            conn,
            template_id,
            markdown=body.markdown,
            apply_brand=body.apply_brand,
            name=body.name,
            token_manifest=manifest,
        )
    return TemplateRow.from_row(row)


@router.post("/{template_id}/publish")
async def publish_template(template_id: str, body: TemplatePublishRequest) -> TemplateRow:
    slug = body.slug or q.slugify(body.name)
    async with get_db_connection() as conn:
        existing = await q.get(conn, template_id)
        if existing is None:
            raise HTTPException(status_code=404, detail="template not found")
        try:
            row = await q.publish(
                conn, template_id, name=body.name, slug=slug,
                monthly_fee_cents=body.monthly_fee_cents,
            )
        except pg_errors.UniqueViolation as exc:
            raise HTTPException(
                status_code=409, detail=f"slug '{slug}' is already used by another template"
            ) from exc
    return TemplateRow.from_row(row)
