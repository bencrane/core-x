"""Assemble a template's HTML (inject the chosen stylesheet + substitute any baked-value ``{{tokens}}``)
and render it to PDF via DocRaptor in LIVE mode (clean, billed; DocRaptor's test output is watermarked).

Most templates carry no ``{{tokens}}`` — every dynamic value is reserved blank space the operator fills
as Documenso fields later — so assembly is just the ``__STYLESHEET__`` slot injection. A tokenized
template (e.g. government-contracted prepaid-introductions) additionally has ``{{ name }}`` slots whose
formatted values are passed in via ``tokens`` and substituted here, BEFORE DocRaptor. Any token left
unsubstituted is a hard error — a blank-input bug must never reach a billed render as literal ``{{…}}``.

``assemble_html`` does blocking filesystem reads; call it off the event loop (``asyncio.to_thread``).
"""
from __future__ import annotations

import logging
import pathlib
import re

import httpx

from .. import config
from . import catalog

logger = logging.getLogger(__name__)

_DOCRAPTOR_URL = "https://docraptor.com/docs"
_TIMEOUT = httpx.Timeout(60.0, connect=10.0)
_STYLE_SLOT = "__STYLESHEET__"
_TOKEN_RE = re.compile(r"{{\s*([a-z_]+)\s*}}")


class RenderError(RuntimeError):
    """Unreadable assets, a manifest problem, or a non-2xx DocRaptor response (transient → 502)."""


class RenderConfigError(RenderError):
    """A required render secret is unset (e.g. DOCRAPTOR_API_KEY) → surface as 503, not 502."""


class StyleError(RenderError):
    """The requested style is not one of the manifest's stylesheets → surface as 400 (client error)."""


class MissingTokenError(RenderError):
    """A tokenized template was rendered with a ``{{token}}`` the caller did not supply (or with no
    values at all) → operator-fixable input, so surface as 400, not 502."""


def _within(base: pathlib.Path, rel: str) -> pathlib.Path:
    """Resolve ``rel`` under ``base`` and confirm it does not escape — defense-in-depth so a manifest
    that names ``../../secret`` cannot read outside the template directory."""
    base = base.resolve()
    target = (base / rel).resolve()
    if target != base and base not in target.parents:
        raise RenderError(f"asset path escapes template dir: {rel!r}")
    return target


def _substitute_tokens(html: str, tokens: dict[str, str]) -> str:
    """Replace each ``{{ name }}`` with ``tokens[name]``. Raises ``RenderError`` if the template names
    a token the caller did not supply."""

    def repl(match: re.Match[str]) -> str:
        key = match.group(1)
        if key not in tokens:
            raise MissingTokenError(f"template references unknown token {{{{ {key} }}}}")
        return tokens[key]

    return _TOKEN_RE.sub(repl, html)


def assemble_html(
    content_dir: pathlib.Path,
    style: str | None = None,
    tokens: dict[str, str] | None = None,
) -> tuple[str, str]:
    """Load the manifest, resolve the style (default = the manifest's plain flag), validate it,
    containment-check the asset paths, inject the CSS into the ``__STYLESHEET__`` slot, and substitute
    any baked-value ``{{tokens}}``.

    Returns ``(html, resolved_style)``. Raises ``StyleError`` (bad style) or ``RenderError`` (manifest
    or asset read failure, or a ``{{token}}`` left unsubstituted — e.g. a tokenized template rendered
    without its input values). Blocking I/O — run via ``asyncio.to_thread``.
    """
    try:
        doc = catalog.manifest_doc(content_dir)
    except catalog.CatalogError as exc:
        raise RenderError(f"manifest: {exc}") from exc

    stylesheets = doc.get("stylesheets") or {}
    resolved = style or ("plain" if doc.get("plain", True) else "branded")
    if resolved not in stylesheets:
        raise StyleError(f"unknown style {resolved!r}; available: {sorted(stylesheets)}")

    try:
        document = _within(content_dir, doc["document"]).read_text()
        css = _within(content_dir, stylesheets[resolved]).read_text()
    except (KeyError, OSError) as exc:
        raise RenderError(f"cannot read template assets: {exc}") from exc

    html = document.replace(_STYLE_SLOT, css)
    if tokens:
        html = _substitute_tokens(html, tokens)
    leftover = _TOKEN_RE.search(html)
    if leftover:
        raise MissingTokenError(
            f"template has an unsubstituted token {{{{ {leftover.group(1)} }}}} — missing input values"
        )
    return html, resolved


async def render_pdf(html: str, *, name: str) -> bytes:
    """Render ``html`` to PDF bytes via DocRaptor in live mode. Raises ``RenderConfigError`` when the
    key is unset, ``RenderError`` on a non-2xx response."""
    api_key = config.docraptor_api_key()
    if not api_key:
        raise RenderConfigError("DOCRAPTOR_API_KEY is not set")
    payload = {
        "test": False,  # LIVE — clean, billed output (test output is watermarked)
        "document_type": "pdf",
        "name": name,
        "document_content": html,
        "prince_options": {"media": "print", "javascript": False},
    }
    async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
        # DocRaptor authenticates via HTTP Basic with the API key as the username.
        resp = await client.post(_DOCRAPTOR_URL, json=payload, auth=(api_key, ""))
    if resp.status_code // 100 != 2:
        detail = resp.text[:500]
        logger.error("docraptor render failed: %s %s", resp.status_code, detail)
        raise RenderError(f"docraptor {resp.status_code}: {detail}")
    return resp.content
