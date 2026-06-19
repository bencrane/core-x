"""DocRaptor (PrinceXML) HTML → PDF — LIVE mode.

Authored engagement-template HTML (``proposals.template_render``, via ``proposal_templates_v1``) is
rendered to a clean, paged PDF. Always ``test=False`` — DocRaptor's free test output is watermarked,
so a clean document requires a paid production render.

Returns the raw PDF bytes inline (synchronous render).
"""
from __future__ import annotations

import logging

import httpx

from .. import config

logger = logging.getLogger(__name__)

_DOCRAPTOR_URL = "https://docraptor.com/docs"
_TIMEOUT = httpx.Timeout(60.0, connect=10.0)


class DocRaptorError(RuntimeError):
    """A non-2xx response from DocRaptor (auth, quota, or malformed HTML)."""


async def render_pdf(html: str, *, name: str = "agreement.pdf") -> bytes:
    """Render ``html`` to PDF bytes via DocRaptor in live mode.

    Raises ``DocRaptorError`` when the key is unset or the API returns non-2xx.
    """
    api_key = config.docraptor_api_key()
    if not api_key:
        raise DocRaptorError("DOCRAPTOR_API_KEY is not set")

    payload = {
        "test": False,  # LIVE — clean, billed output (test output is watermarked)
        "document_type": "pdf",
        "name": name,
        "document_content": html,
        "prince_options": {
            "media": "print",
            "javascript": False,  # static HTML/CSS agreement — no JS pass needed
        },
    }
    async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
        # DocRaptor authenticates via HTTP Basic with the API key as the username.
        resp = await client.post(_DOCRAPTOR_URL, json=payload, auth=(api_key, ""))

    if resp.status_code // 100 != 2:
        # DocRaptor returns an XML/JSON error body on failure — surface it (truncated) for logs.
        detail = resp.text[:500]
        logger.error("docraptor render failed: %s %s", resp.status_code, detail)
        raise DocRaptorError(f"docraptor {resp.status_code}: {detail}")

    return resp.content
