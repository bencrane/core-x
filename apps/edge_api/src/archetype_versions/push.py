"""Render an agreement-content source to PDF and PUSH it to Documenso as a TEMPLATE.

The terminal step of the content-source-selectable lane:

    registry row / explicit selector
        -> resolve content source (repo-html: catalog + assemble_html; db-markdown: not yet wired)
        -> DocRaptor LIVE PDF (render.render_pdf)
        -> [optional] audit copy to R2 (store)
        -> Documenso TEMPLATE create (documenso_client.create_template_from_pdf)

``render_and_push`` is DB-free (pure HTTP/filesystem) and raises a typed error on any failure;
``record_run`` writes the ops.global_agreement_archetype_version_push_runs ledger (fire-and-forget). The caller
(``routers/internal_archetype_versions_v1.py``) owns the resolve→push→ledger orchestration.
"""
from __future__ import annotations

import asyncio
import logging
import secrets
from dataclasses import dataclass

from ..services import documenso_client
from . import catalog, render, store

logger = logging.getLogger(__name__)

# Content kinds the lane can pull from. 'repo-html' is wired; 'db-markdown' is the documented
# extension point (resolve business.global_agreement_content.markdown by slug → branded HTML).
REPO_HTML = "repo-html"
DB_MARKDOWN = "db-markdown"


class PushError(RuntimeError):
    """A render-and-push failure (bad selector, unwired source, render/store/documenso error)."""


@dataclass(frozen=True)
class PushOutcome:
    brand: str
    path: str
    archetype: str
    version: str
    style: str
    source_kind: str
    documenso_template_id: str
    documenso_numeric_id: int | None
    pdf_bytes: int
    pdf_r2_key: str | None
    pdf_url: str | None


def split_registry_path(registry_path: str) -> tuple[str, str, str]:
    """Split a brand-relative registry ``path`` into the catalog's (path, archetype, version) triple.
    ``docraptor-to-documenso-template/capital-origination/v1`` → the three segments."""
    parts = [s for s in registry_path.strip("/").split("/") if s]
    if len(parts) != 3:
        raise PushError(
            f"registry path must be '<family>/<archetype>/<version>' (3 segments), got {registry_path!r}"
        )
    return parts[0], parts[1], parts[2]


async def render_and_push(
    *,
    brand: str,
    path: str,
    archetype: str,
    version: str,
    source_kind: str = REPO_HTML,
    style: str | None = None,
    title: str | None = None,  # operator NAME override for the Documenso template title; blank -> manifest name
    tokens: dict[str, str] | None = None,
    recipients: list[dict[str, str]] | None = None,
    store_copy: bool = True,
    external_id: str | None = None,  # stamped as Documenso externalId (provenance, e.g. the content path)
) -> PushOutcome:
    """Resolve the content source, render it to PDF (substituting any baked-value ``tokens``), and
    create a Documenso TEMPLATE from the bytes.

    Raises ``PushError`` (bad selector / unwired source), ``render.RenderError`` /
    ``render.StyleError`` / ``render.RenderConfigError`` (assembly or DocRaptor), ``store.StoreError``
    (R2, only if ``store_copy``), or ``documenso_client.DocumensoError`` (template create).
    """
    if source_kind != REPO_HTML:
        # db-markdown would resolve business.global_agreement_content.markdown by slug and render via
        # proposals.template_render; not wired here — fail loudly rather than silently mis-render.
        raise PushError(f"source_kind {source_kind!r} not yet wired (only {REPO_HTML!r})")

    try:
        content_dir = catalog.resolve(path, archetype, version, brand=brand)
    except catalog.CatalogError as exc:
        raise PushError(str(exc)) from exc

    # Assembly does blocking filesystem reads (manifest + html + css) — keep it off the event loop.
    html, resolved_style = await asyncio.to_thread(render.assemble_html, content_dir, style, tokens)

    # The operator-entered NAME (``title`` override) becomes the Documenso template title when non-empty;
    # otherwise fall through to the manifest ``name`` (then a brand/path/archetype/version fallback).
    title = title.strip() if title else None
    if not title:
        doc = await asyncio.to_thread(catalog.manifest_doc, content_dir)
        title = str(doc.get("name") or f"{brand} {path} {archetype} {version}")

    name = f"{brand}-{path}-{archetype}-{version}-{resolved_style}.pdf"
    pdf = await render.render_pdf(html, name=name)

    pdf_r2_key: str | None = None
    pdf_url: str | None = None
    if store_copy:
        # An audit copy of the exact bytes pushed to Documenso. Non-fatal: a storage hiccup must not
        # void a successful Documenso push.
        pdf_r2_key = f"{store.PREFIX}push/{brand}/{path}/{archetype}/{version}/{resolved_style}-{secrets.token_hex(8)}.pdf"
        try:
            await store.put_pdf(pdf_r2_key, pdf)
            pdf_url = await store.presigned_get_url(pdf_r2_key)
        except store.StoreError as exc:
            logger.warning("archetype-version push: R2 audit copy failed (non-fatal): %s", exc)
            pdf_r2_key = None

    result = await documenso_client.create_template_from_pdf(
        title=title, pdf=pdf, recipients=recipients, filename=name, external_id=external_id
    )

    return PushOutcome(
        brand=brand,
        path=path,
        archetype=archetype,
        version=version,
        style=resolved_style,
        source_kind=source_kind,
        documenso_template_id=result.template_id,
        documenso_numeric_id=result.numeric_id,
        pdf_bytes=len(pdf),
        pdf_r2_key=pdf_r2_key,
        pdf_url=pdf_url,
    )


async def record_run(
    conn,
    *,
    status: str,
    brand: str,
    path: str,
    archetype: str,
    version: str,
    source_kind: str,
    style: str | None = None,
    run_id: str | None = None,
    documenso_template_id: str | None = None,
    documenso_numeric_id: int | None = None,
    pdf_r2_key: str | None = None,
    pdf_bytes: int | None = None,
    error: str | None = None,
) -> None:
    """Write one terminal-state row to ops.global_agreement_archetype_version_push_runs. Fire-and-forget: a ledger
    failure is logged, never raised — it must not turn a successful push into a 500."""
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO ops.global_agreement_archetype_version_push_runs
                    (run_id, brand, path, archetype, version, style, source_kind, status,
                     documenso_template_id, documenso_numeric_id, pdf_r2_key, pdf_bytes, error)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    run_id, brand, path, archetype, version, style, source_kind, status,
                    documenso_template_id, documenso_numeric_id, pdf_r2_key, pdf_bytes,
                    (error[:2000] if error else None),
                ),
            )
        await conn.commit()
    except Exception:  # noqa: BLE001 — ledger is best-effort
        logger.exception("ops.global_agreement_archetype_version_push_runs write failed (non-fatal)")
