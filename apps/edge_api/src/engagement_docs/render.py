"""Standalone engagement-document render — STATIC HTML → token-bound → DocRaptor PDF.

No proposal-pathway code: its OWN manifest load, its OWN `{{token}}` substitution, its OWN DocRaptor
call (live mode). The legal body, wordmark, and signature block come from the repo-resident content
(apps/edge_api/content/active-operators/docraptor-to-documenso-document-only/global_engagement_content/);
the `__STYLESHEET__` slot is filled per the manifest `plain` flag. `[[...]]` Documenso anchors use a
different grammar and are never touched.
"""
from __future__ import annotations

import html as _html
import json
import os
import pathlib
import re

import httpx

# apps/edge_api/src/engagement_docs/render.py -> parents[2] == apps/edge_api
# Content tree relocated under the active-operators root in #494 (44ef2fc); this lane reads it directly.
_CONTENT_DIR = (
    pathlib.Path(__file__).resolve().parents[2]
    / "content" / "active-operators" / "docraptor-to-documenso-document-only" / "global_engagement_content"
)
_MANIFEST = _CONTENT_DIR / "manifest.json"

_DOCRAPTOR_URL = "https://docraptor.com/docs"
_TIMEOUT = httpx.Timeout(60.0, connect=10.0)
_TOKEN_RE = re.compile(r"\{\{\s*([a-zA-Z0-9_]+)\s*\}\}")


class RenderError(RuntimeError):
    """Content not found, DocRaptor key unset, or a non-2xx DocRaptor response."""


def _doc(slug: str) -> dict:
    manifest = json.loads(_MANIFEST.read_text())
    if slug not in manifest:
        raise RenderError(f"unknown engagement-content document: {slug!r}")
    return manifest[slug]


def style_for(slug: str) -> str:
    """'plain' | 'branded' per the manifest flag."""
    return "plain" if _doc(slug).get("plain", True) else "branded"


def assemble_html(slug: str) -> str:
    """Inject the selected stylesheet into the document's ``__STYLESHEET__`` slot."""
    doc = _doc(slug)
    document = (_CONTENT_DIR / doc["document"]).read_text()
    css = (_CONTENT_DIR / doc["stylesheets"][style_for(slug)]).read_text()
    return document.replace("__STYLESHEET__", css)


def substitute(document_html: str, values: dict[str, str]) -> str:
    """Replace ``{{token}}`` with ``values[token]`` (HTML-escaped). Unknown tokens stay LITERAL so an
    unfilled field is visible, never silently blank. ``[[anchors]]`` are a different grammar → untouched."""
    def repl(m: "re.Match[str]") -> str:
        key = m.group(1)
        return _html.escape(values[key]) if key in values else m.group(0)
    return _TOKEN_RE.sub(repl, document_html)


async def render_pdf(document_html: str, *, name: str) -> bytes:
    """Render to PDF via DocRaptor (live, clean — test output is watermarked). Raises RenderError."""
    api_key = os.environ.get("DOCRAPTOR_API_KEY")
    if not api_key:
        raise RenderError("DOCRAPTOR_API_KEY is not set")
    payload = {
        "test": False,  # LIVE — clean, billed
        "document_type": "pdf",
        "name": name,
        "document_content": document_html,
        "prince_options": {"media": "print", "javascript": False},
    }
    async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
        resp = await client.post(_DOCRAPTOR_URL, json=payload, auth=(api_key, ""))
    if resp.status_code // 100 != 2:
        raise RenderError(f"docraptor {resp.status_code}: {resp.text[:400]}")
    return resp.content
