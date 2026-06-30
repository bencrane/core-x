"""Internal engagement-template render+push — Trigger.dev-facing.

  POST /internal/engagement-templates/render-push   trigger-secret — content source -> DocRaptor PDF -> Documenso TEMPLATE

Called by the ``engagement-template-push`` Trigger.dev task via ``callHqx``. Gated by
``require_trigger_secret`` (TRIGGER_SHARED_SECRET) — the same ``/internal/*`` contract the
deal-materialize / gtm pipeline tasks use.

CHOOSE-WHERE-TO-PULL: pass a ``registryPath`` (or ``registryId``) to resolve a
``business.global_input_content`` row (brand + source_kind + brand-relative path), OR pass an explicit
``brand`` / ``path`` / ``archetype`` / ``version``. Either way the lane renders the content and creates
a Documenso TEMPLATE, recording a terminal row in ops.engagement_template_push_runs.
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, ConfigDict, Field

from ..db import get_db_connection
from ..engagement_templates import push, registry, render
from ..services import documenso_client
from ..trigger_secret import require_trigger_secret

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/engagement-templates", tags=["internal"])


class RenderPushRequest(BaseModel):
    # The Trigger task sends camelCase; accept both forms.
    model_config = ConfigDict(populate_by_name=True)
    registry_path: str | None = Field(default=None, alias="registryPath")
    registry_id: str | None = Field(default=None, alias="registryId")
    brand: str | None = None
    path: str | None = None
    archetype: str | None = None
    version: str | None = None
    style: str | None = None
    title: str | None = None
    trigger_run_id: str | None = Field(default=None, alias="triggerRunId")


async def _resolve_descriptor(body: RenderPushRequest) -> dict:
    """(brand, path, archetype, version, source_kind, title) from a registry row OR explicit fields."""
    if body.registry_path or body.registry_id:
        async with get_db_connection() as conn:
            row = (
                await registry.get_by_path(conn, body.registry_path)
                if body.registry_path
                else await registry.get_by_id(conn, body.registry_id)
            )
        if row is None:
            raise HTTPException(status_code=404, detail="unknown content-source registry row")
        try:
            path, archetype, version = push.split_registry_path(row["path"])
        except push.PushError as e:
            raise HTTPException(status_code=422, detail=str(e)) from e
        return {
            "brand": row["brand"],
            "path": path,
            "archetype": archetype,
            "version": version,
            "source_kind": row["source_kind"],
            "title": body.title or row.get("name"),
        }

    if not (body.path and body.archetype and body.version):
        raise HTTPException(
            status_code=400,
            detail="provide registryPath/registryId, or all of brand/path/archetype/version",
        )
    return {
        "brand": body.brand or "active-operators",
        "path": body.path,
        "archetype": body.archetype,
        "version": body.version,
        "source_kind": push.REPO_HTML,
        "title": body.title,
    }


@router.post("/render-push", dependencies=[Depends(require_trigger_secret)])
async def render_push(body: RenderPushRequest) -> dict:
    d = await _resolve_descriptor(body)

    try:
        outcome = await push.render_and_push(
            brand=d["brand"],
            path=d["path"],
            archetype=d["archetype"],
            version=d["version"],
            source_kind=d["source_kind"],
            style=body.style,
            title=d["title"],
        )
    except (push.PushError, render.StyleError) as e:
        await _record_error(body, d, str(e))
        raise HTTPException(status_code=400, detail=str(e)[:300]) from e
    except render.RenderConfigError as e:
        await _record_error(body, d, str(e))
        raise HTTPException(status_code=503, detail=f"render: {str(e)[:300]}") from e
    except (render.RenderError, documenso_client.DocumensoError) as e:
        await _record_error(body, d, str(e))
        raise HTTPException(status_code=502, detail=str(e)[:300]) from e

    async with get_db_connection() as conn:
        await push.record_run(
            conn,
            status="success",
            run_id=body.trigger_run_id,
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
        "engagement-template push ok: %s/%s/%s/%s style=%s template=%s",
        outcome.brand, outcome.path, outcome.archetype, outcome.version,
        outcome.style, outcome.documenso_template_id,
    )
    return {
        "status": "success",
        "brand": outcome.brand,
        "path": outcome.path,
        "archetype": outcome.archetype,
        "version": outcome.version,
        "style": outcome.style,
        "source_kind": outcome.source_kind,
        "documenso_template_id": outcome.documenso_template_id,
        "documenso_numeric_id": outcome.documenso_numeric_id,
        "pdf_bytes": outcome.pdf_bytes,
        "pdf_url": outcome.pdf_url,
    }


async def _record_error(body: RenderPushRequest, d: dict, error: str) -> None:
    async with get_db_connection() as conn:
        await push.record_run(
            conn,
            status="error",
            run_id=body.trigger_run_id,
            brand=d["brand"],
            path=d["path"],
            archetype=d["archetype"],
            version=d["version"],
            style=body.style,
            source_kind=d["source_kind"],
            error=error,
        )
