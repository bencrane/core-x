"""Engagement-template render — Settings "Engagement Templates" → Send to DocRaptor.

  GET  /api/v1/engagement-templates           service-token  selectable (path, archetype, version)
  POST /api/v1/engagement-templates/render    service-token  render plain (default) -> R2 -> presigned PDF URL

STANDALONE from the engagement-doc pathway: its own catalog/assembly/DocRaptor/R2, and NO Documenso.
The operator picks a template, clicks render, and gets a short-lived PDF link; affixing Documenso
fields is done by hand in the editor afterward. Gated by EDGE_API_SERVICE_TOKEN — the platform-api
BFF brokers it with the operator session.
"""
from __future__ import annotations

import asyncio
import logging
import secrets

from fastapi import APIRouter, Depends, HTTPException

from ..engagement_templates import catalog, render, store
from ..engagement_templates.models import RenderRequest, RenderResult, TemplateRef
from ..service_token import require_service_token

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/engagement-templates", tags=["engagement-templates"])

_PDF_TTL_SECONDS = 3600


@router.get("", dependencies=[Depends(require_service_token)])
def list_templates() -> list[TemplateRef]:
    """Selectable templates for the Settings dropdowns (path -> archetype -> version)."""
    return [
        TemplateRef(
            path=e.path,
            archetype=e.archetype,
            version=e.version,
            name=e.name,
            default_style=e.default_style,
            styles_available=list(e.styles_available),
        )
        for e in catalog.list_templates()
    ]


@router.post("/render", dependencies=[Depends(require_service_token)])
async def render_template(body: RenderRequest) -> RenderResult:
    """Render the selected template to a clean PDF (plain style by default) and return a presigned URL.
    Does NOT create anything in Documenso."""
    try:
        content_dir = catalog.resolve(body.path, body.archetype, body.version)
    except catalog.CatalogError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e

    # Assembly does blocking filesystem reads (manifest + html + css) — keep it off the event loop.
    try:
        html, style = await asyncio.to_thread(render.assemble_html, content_dir, body.style)
    except render.StyleError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except render.RenderError as e:
        raise HTTPException(status_code=502, detail=f"render: {e}") from e

    name = f"{body.path}-{body.archetype}-{body.version}-{style}.pdf"
    try:
        pdf = await render.render_pdf(html, name=name)
    except render.RenderConfigError as e:
        raise HTTPException(status_code=503, detail=f"render: {e}") from e
    except render.RenderError as e:
        raise HTTPException(status_code=502, detail=f"render: {e}") from e

    key = f"{store.PREFIX}{body.path}/{body.archetype}/{body.version}/{style}-{secrets.token_hex(8)}.pdf"
    try:
        await store.put_pdf(key, pdf)
        url = await store.presigned_get_url(key, expires_seconds=_PDF_TTL_SECONDS)
    except store.StoreConfigError as e:
        raise HTTPException(status_code=503, detail=f"store: {e}") from e
    except store.StoreError as e:
        raise HTTPException(status_code=502, detail=f"store: {e}") from e

    logger.info(
        "engagement-template render ok: %s/%s/%s style=%s bytes=%d",
        body.path, body.archetype, body.version, style, len(pdf),
    )
    return RenderResult(
        pdf_url=url,
        expires_seconds=_PDF_TTL_SECONDS,
        path=body.path,
        archetype=body.archetype,
        version=body.version,
        style=style,
        pdf_bytes=len(pdf),
    )
