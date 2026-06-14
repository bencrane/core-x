#!/usr/bin/env python3
"""End-to-end PREVIEW: Active Operators Strategic Origination Agreement -> PLAIN PDF via DocRaptor.

Self-contained — NO database, NO running server:

    markdown body  ->  plain HTML (white bg, black text, NO brand)  ->  DocRaptor  ->  PDF on disk

Rendered DELIBERATELY UNBRANDED (plain white/black, no letterhead) so Documenso applies its OWN
branding to the uploaded template. The body is the single source of truth:
``apps/edge_api/content/active_operators_strategic_origination.md``.

Run (Doppler injects DOCRAPTOR_API_KEY):
    doppler run --project core-x --config prd -- python scripts/render_ao_preview.py

Env:
    DOCRAPTOR_API_KEY   required (injected by doppler)
    DOCRAPTOR_TEST=1    optional — watermarked + FREE (default is LIVE / clean / billed)
    AO_OUT=<path>       override output path
"""
from __future__ import annotations

import os
import pathlib
import sys

import httpx
from markdown_it import MarkdownIt

_ROOT = pathlib.Path(__file__).resolve().parents[1]
BODY_MD = _ROOT / "apps/edge_api/content/active_operators_strategic_origination.md"
OUT = pathlib.Path(
    os.environ.get(
        "AO_OUT",
        str(pathlib.Path.home() / "Downloads" / "active-operators-strategic-origination.preview.pdf"),
    )
)

# Provider signs as the legal entity. The d/b/a ("doing business as Active Operators") is established
# in the body's party definition, so the signature line stays clean.
PROVIDER_ENTITY = "Rare Structure LLC"
PROVIDER_BY = "Benjamin J. Crane"
PROVIDER_TITLE = "Managing Director"

_md = MarkdownIt("commonmark").enable("table").enable("strikethrough")

# Plain document styling — white background, black text, standard legal typography. No letterhead,
# no brand color: Documenso layers its own branding on top of this.
_STYLE = r"""
  @page { size: Letter; margin: 1in;
    @bottom-center { content: "Page " counter(page); font-family: Helvetica, Arial, sans-serif;
      font-size: 8pt; color: #777; } }
  * { box-sizing: border-box; }
  html, body { background: #ffffff; }
  body { font-family: Georgia, 'Times New Roman', serif; font-size: 11pt; line-height: 1.5;
    color: #111111; margin: 0; }
  h1 { font-family: Georgia, serif; font-size: 17pt; font-weight: 700; margin: 0 0 6pt; color: #111; }
  h2 { font-family: Helvetica, Arial, sans-serif; font-size: 9.5pt; letter-spacing: 0.08em;
    text-transform: uppercase; font-weight: 700; margin: 16pt 0 5pt; color: #111; }
  strong { font-weight: 700; color: #000; }
  p { margin: 0 0 8pt; text-align: justify; }
  ul, ol { margin: 0 0 8pt; padding-left: 18pt; } li { margin: 0 0 4pt; }
  a { color: #111; }
  /* Execution block. Entity name heads each side (above the line); By / Title / Date sit below with
     a fixed-width label column so the VALUES align vertically regardless of label length. */
  .sig-wrap { margin-top: 30pt; page-break-inside: avoid; }
  .sig-grid { width: 100%; border-collapse: separate; border-spacing: 0; }
  .sig-grid td { width: 50%; vertical-align: top; padding-right: 28pt; }
  .sig-party { font-family: Helvetica, Arial, sans-serif; font-size: 8.5pt; letter-spacing: 0.1em;
    text-transform: uppercase; font-weight: 700; color: #111; margin-bottom: 2pt; }
  .sig-entity { font-size: 10.5pt; font-weight: 700; color: #111; margin-bottom: 18pt; }
  .sig-line { border-bottom: 1pt solid #111; height: 30pt; margin-bottom: 4pt; }
  .sig-field { font-family: Helvetica, Arial, sans-serif; font-size: 10pt; color: #111; margin: 0 0 2pt; }
  .sig-field .lab { display: inline-block; width: 40pt; color: #666; }
  .sig-field .val { color: #111; }
  /* Documenso findText anchor — real selectable text, whited out at sign-time. Never hide it. */
  .sig-anchor { font-family: Helvetica, Arial, sans-serif; font-size: 8pt; letter-spacing: 0.06em; color: #888; }
"""

_SHELL = r"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8" /><title>Strategic Origination Agreement</title>
<style>__STYLE__</style></head>
<body>
«BODY»
  <div class="sig-wrap">
    <table class="sig-grid"><tr>
      <td>
        <div class="sig-party">Provider</div>
        <div class="sig-entity">__PROVIDER_ENTITY__</div>
        <div class="sig-line"><span class="sig-anchor">[[PROVIDER_SIGNATURE]]</span></div>
        <div class="sig-field"><span class="lab">By:</span><span class="val">__PROVIDER_BY__</span></div>
        <div class="sig-field"><span class="lab">Title:</span><span class="val">__PROVIDER_TITLE__</span></div>
        <div class="sig-field"><span class="lab">Date:</span><span class="sig-anchor">[[PROVIDER_DATE]]</span></div>
      </td>
      <td>
        <div class="sig-party">Participant</div>
        <div class="sig-entity">{{participant_name}}</div>
        <div class="sig-line"><span class="sig-anchor">[[PARTICIPANT_SIGNATURE]]</span></div>
        <div class="sig-field"><span class="lab">By:</span><span class="val">{{participant_signer_name}}</span></div>
        <div class="sig-field"><span class="lab">Title:</span><span class="val">{{participant_title}}</span></div>
        <div class="sig-field"><span class="lab">Date:</span><span class="sig-anchor">[[PARTICIPANT_DATE]]</span></div>
      </td>
    </tr></table>
  </div>
</body></html>
"""


def build_html() -> str:
    body_html = _md.render(BODY_MD.read_text())
    shell = (
        _SHELL.replace("__STYLE__", _STYLE)
        .replace("__PROVIDER_ENTITY__", PROVIDER_ENTITY)
        .replace("__PROVIDER_BY__", PROVIDER_BY)
        .replace("__PROVIDER_TITLE__", PROVIDER_TITLE)
    )
    return shell.replace("«BODY»", body_html)


def main() -> None:
    api_key = os.environ.get("DOCRAPTOR_API_KEY")
    if not api_key:
        sys.exit(
            "DOCRAPTOR_API_KEY not set. Run:\n"
            "  doppler run --project core-x --config prd -- python scripts/render_ao_preview.py"
        )
    test_mode = os.environ.get("DOCRAPTOR_TEST", "0") != "0"  # default LIVE / clean
    payload = {
        "test": test_mode,
        "document_type": "pdf",
        "name": OUT.name,
        "document_content": build_html(),
        "prince_options": {"media": "print", "javascript": False},
    }
    resp = httpx.post(
        "https://docraptor.com/docs", json=payload, auth=(api_key, ""), timeout=httpx.Timeout(120.0, connect=10.0)
    )
    if resp.status_code // 100 != 2:
        sys.exit(f"docraptor {resp.status_code}: {resp.text[:500]}")
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_bytes(resp.content)
    mode = "TEST/watermarked (free)" if test_mode else "LIVE/clean (billed)"
    print(f"wrote {OUT}  ({len(resp.content):,} bytes)  [{mode}, PLAIN]")


if __name__ == "__main__":
    main()
