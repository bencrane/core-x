"""Archetype-version render — the operator render surface → Send to DocRaptor.

  GET  /api/v1/archetype-versions           service-token  selectable (path, archetype, version)
  POST /api/v1/archetype-versions/render    service-token  render plain (default) -> R2 -> presigned PDF URL

STANDALONE from the agreement-document pathway: its own catalog/assembly/DocRaptor/R2, and NO Documenso.
The operator picks a template, clicks render, and gets a short-lived PDF link; affixing Documenso
fields is done by hand in the editor afterward. Gated by EDGE_API_SERVICE_TOKEN — the platform-api
BFF brokers it with the operator session.
"""
from __future__ import annotations

import asyncio
import logging
import secrets

from fastapi import APIRouter, Depends, HTTPException

from ..db import get_db_connection
from ..archetype_versions import catalog, push, render, store, values
from ..archetype_versions.models import (
    RenderPushRequest,
    RenderPushResult,
    RenderRequest,
    RenderResult,
    TemplateRef,
)
from ..service_token import require_service_token
from ..services import documenso_client

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/archetype-versions", tags=["archetype-versions"])

_PDF_TTL_SECONDS = 3600


@router.get("", dependencies=[Depends(require_service_token)])
def list_templates() -> list[TemplateRef]:
    """Selectable templates for the Settings dropdowns (path -> archetype -> version)."""
    return [
        TemplateRef(
            brand=e.brand,
            path=e.path,
            archetype=e.archetype,
            version=e.version,
            name=e.name,
            default_style=e.default_style,
            styles_available=list(e.styles_available),
            inputs=list(e.inputs),
        )
        for e in catalog.list_templates()
    ]


@router.post("/render", dependencies=[Depends(require_service_token)])
async def render_template(body: RenderRequest) -> RenderResult:
    """Render the selected template to a clean PDF (plain style by default) and return a presigned URL.
    Does NOT create anything in Documenso."""
    try:
        content_dir = catalog.resolve(body.path, body.archetype, body.version, brand=body.brand)
    except catalog.CatalogError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e

    # Assembly does blocking filesystem reads (manifest + html + css) — keep it off the event loop.
    try:
        html, style = await asyncio.to_thread(render.assemble_html, content_dir, body.style)
    except (render.StyleError, render.MissingTokenError) as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except render.RenderError as e:
        raise HTTPException(status_code=502, detail=f"render: {e}") from e

    name = f"{body.brand}-{body.path}-{body.archetype}-{body.version}-{style}.pdf"
    try:
        pdf = await render.render_pdf(html, name=name)
    except render.RenderConfigError as e:
        raise HTTPException(status_code=503, detail=f"render: {e}") from e
    except render.RenderError as e:
        raise HTTPException(status_code=502, detail=f"render: {e}") from e

    key = f"{store.PREFIX}{body.brand}/{body.path}/{body.archetype}/{body.version}/{style}-{secrets.token_hex(8)}.pdf"
    try:
        await store.put_pdf(key, pdf)
        url = await store.presigned_get_url(key, expires_seconds=_PDF_TTL_SECONDS)
    except store.StoreConfigError as e:
        raise HTTPException(status_code=503, detail=f"store: {e}") from e
    except store.StoreError as e:
        raise HTTPException(status_code=502, detail=f"store: {e}") from e

    logger.info(
        "archetype-version render ok: %s/%s/%s/%s style=%s bytes=%d",
        body.brand, body.path, body.archetype, body.version, style, len(pdf),
    )
    return RenderResult(
        pdf_url=url,
        expires_seconds=_PDF_TTL_SECONDS,
        brand=body.brand,
        path=body.path,
        archetype=body.archetype,
        version=body.version,
        style=style,
        pdf_bytes=len(pdf),
    )


@router.post("/render-push", dependencies=[Depends(require_service_token)])
async def render_push_template(body: RenderPushRequest) -> RenderPushResult:
    """Render the selected template to a PDF and create a Documenso TEMPLATE from the bytes.

    Operator-driven sibling of POST /render (which stops at the presigned PDF). Reuses
    ``push.render_and_push`` — the SAME machinery the Trigger.dev `/internal` lane uses — but gated by
    the operator service token (not the trigger secret). Every terminal state (success | error)
    writes the ops.global_agreement_archetype_version_push_runs ledger, same as the automation lane — the ledger is
    the "Pushed" source of truth for the archetype template-conformance surface. (The former
    "DB-free" contract predates the operator lane becoming the primary push path.)

    A tokenized template (manifest ``inputs``, e.g. prepaid-introductions) carries the operator's
    values in ``body.values``; they are formatted into ``{{token}}`` substitutions and baked into the
    PDF before DocRaptor. Untokenized templates ignore ``values``.
    """
    tokens: dict[str, str] | None = None
    if body.values is not None:
        try:
            tokens = values.prepaid_introduction_tokens(
                amount=body.values.amount,
                introductions=body.values.introductions,
                term_days=body.values.term_days,
            )
        except (ValueError, ArithmeticError) as e:
            raise HTTPException(status_code=400, detail=f"invalid values: {str(e)[:200]}") from e

    async def _record_error(error: str) -> None:
        async with get_db_connection() as conn:
            await push.record_run(
                conn,
                status="error",
                brand=body.brand,
                path=body.path,
                archetype=body.archetype,
                version=body.version,
                style=body.style,
                source_kind=push.REPO_HTML,
                error=error,
            )

    try:
        outcome = await push.render_and_push(
            brand=body.brand,
            path=body.path,
            archetype=body.archetype,
            version=body.version,
            source_kind=push.REPO_HTML,
            style=body.style,
            title=body.name,  # operator-entered NAME -> Documenso template title (blank -> manifest name)
            tokens=tokens,
            external_id=body.external_id,
        )
    except (push.PushError, render.StyleError, render.MissingTokenError) as e:
        # Bad selector / unknown template / unwired source / bad style — operator-fixable.
        await _record_error(str(e))
        raise HTTPException(status_code=400, detail=str(e)[:300]) from e
    except render.RenderConfigError as e:
        await _record_error(str(e))
        raise HTTPException(status_code=503, detail=f"render: {str(e)[:300]}") from e
    except (render.RenderError, documenso_client.DocumensoError) as e:
        await _record_error(str(e))
        raise HTTPException(status_code=502, detail=str(e)[:300]) from e

    async with get_db_connection() as conn:
        await push.record_run(
            conn,
            status="success",
            brand=outcome.brand,
            path=outcome.path,
            archetype=outcome.archetype,
            version=outcome.version,
            style=outcome.style,
            source_kind=outcome.source_kind,
            documenso_template_id=outcome.documenso_template_id,
            documenso_numeric_id=outcome.documenso_numeric_id,
            pdf_r2_key=outcome.pdf_r2_key,
            pdf_bytes=outcome.pdf_bytes,
        )

    logger.info(
        "archetype-version render-push ok: %s/%s/%s/%s style=%s template=%s",
        outcome.brand, outcome.path, outcome.archetype, outcome.version,
        outcome.style, outcome.documenso_template_id,
    )
    return RenderPushResult(
        documenso_template_id=outcome.documenso_template_id,
        documenso_numeric_id=outcome.documenso_numeric_id,
        brand=outcome.brand,
        path=outcome.path,
        archetype=outcome.archetype,
        version=outcome.version,
        style=outcome.style,
        source_kind=outcome.source_kind,
        pdf_bytes=outcome.pdf_bytes,
        pdf_url=outcome.pdf_url,
    )
