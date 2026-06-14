#!/usr/bin/env python3
"""End-to-end PREVIEW: Active Operators Strategic Origination Agreement → PDF via DocRaptor.

Self-contained test harness — NO database, NO running server:

    markdown body  →  branded AO HTML (dark shell + execution block)  →  DocRaptor  →  PDF on disk

This mirrors the real edge_api render path (markdown-it + the dark brand shell + DocRaptor) but
inlines a clean Active Operators shell so you can iterate on the artifact before it is wired into
the live authoring surface. The body is the single source of truth:
``apps/edge_api/content/active_operators_strategic_origination.md``.

Run (Doppler injects DOCRAPTOR_API_KEY):
    doppler run --project core-x --config prd -- python scripts/render_ao_preview.py

Env:
    DOCRAPTOR_API_KEY   required (injected by doppler)
    DOCRAPTOR_TEST=1    default — watermarked + FREE, for fast layout iteration.
    DOCRAPTOR_TEST=0    clean, BILLED production render (the real artifact to upload to Documenso).
    AO_OUT=<path>       override output path (default: ~/Downloads/active-operators-...preview.pdf)
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

# Brand / party identity (Active Operators is the operating brand of the Rare Structure legal entity).
WORDMARK = "ACTIVE OPERATORS"
PROVIDER_ENTITY = "Rare Structure LLC d/b/a Active Operators"  # clean casing — fixes the all-caps "D/B/A"
PROVIDER_BY = "Benjamin J. Crane"
PROVIDER_TITLE = "Managing Director"

_md = MarkdownIt("commonmark").enable("table").enable("strikethrough")

# Dark brand CSS — same identity as the live shell. The provider entity line is NOT uppercased, so
# "d/b/a" reads naturally instead of "D/B/A".
_STYLE = r"""
  @page { size: Letter; margin: 1in 1in 1.1in 1in; background: #0a0e1a;
    @bottom-center { content: "ACTIVE OPERATORS  \2022  Page " counter(page);
      font-family: 'Helvetica Neue', Arial, sans-serif; font-size: 7.5pt; letter-spacing: 0.12em;
      color: #82828c; text-transform: uppercase; } }
  * { box-sizing: border-box; }
  html { background: #0a0e1a; }
  body { font-family: 'Georgia', 'Times New Roman', serif; font-size: 10.5pt; line-height: 1.55;
    color: #e4e4e7; background: #0a0e1a; margin: 0; }
  strong { color: #fafafa; }
  .wordmark { font-family: 'Helvetica Neue', Arial, sans-serif; font-weight: 700;
    letter-spacing: 0.34em; font-size: 12pt; color: #fafafa; }
  h1 { font-family: 'Helvetica Neue', Arial, sans-serif; font-size: 15pt; letter-spacing: 0.06em;
    font-weight: 600; margin: 6pt 0 2pt; color: #fafafa; }
  .rule { border: 0; border-top: 1pt solid #2d3548; margin: 10pt 0 16pt; }
  h2 { font-family: 'Helvetica Neue', Arial, sans-serif; font-size: 9pt; letter-spacing: 0.16em;
    text-transform: uppercase; font-weight: 600; margin: 18pt 0 5pt; color: #7b9fd4; }
  p { margin: 0 0 8pt; text-align: justify; }
  ul, ol { margin: 0 0 8pt; padding-left: 18pt; } li { margin: 0 0 4pt; }
  .sig-wrap { margin-top: 26pt; page-break-inside: avoid; }
  .sig-grid { width: 100%; border-collapse: separate; border-spacing: 0; }
  .sig-grid td { width: 50%; vertical-align: top; padding-right: 24pt; border-bottom: 0; text-align: left; }
  .sig-party { font-family: 'Helvetica Neue', Arial, sans-serif; font-size: 8pt; letter-spacing: 0.14em;
    text-transform: uppercase; color: #82828c; margin-bottom: 2pt; }
  .sig-entity { font-family: Georgia, serif; font-size: 8.5pt; font-style: italic; color: #a1a1aa; margin-bottom: 14pt; }
  .sig-line { border-bottom: 1pt solid #2d3548; height: 30pt; margin-bottom: 3pt; }
  .sig-field { font-size: 8pt; color: #82828c; font-family: 'Helvetica Neue', Arial, sans-serif; }
  .sig-val { font-size: 10pt; color: #e4e4e7; }
  .sig-anchor { font-family: 'Helvetica Neue', Arial, sans-serif; font-size: 8pt; letter-spacing: 0.08em;
    color: #5b6373; line-height: 30pt; padding-left: 4pt; }
"""

# «BODY» injected last. Provider side = Rare Structure d/b/a Active Operators (your signature anchor +
# printed name). Participant side = the counterparty (Documenso fills name/title per deal).
_SHELL = r"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8" /><title>Strategic Origination Agreement</title>
<style>__STYLE__</style></head>
<body>
  <div class="wordmark">__WORDMARK__</div>
  <hr class="rule" />
«BODY»
  <div class="sig-wrap">
    <table class="sig-grid"><tr>
      <td>
        <div class="sig-party">Provider</div>
        <div class="sig-entity">__PROVIDER_ENTITY__</div>
        <div class="sig-line"><span class="sig-anchor">[[PROVIDER_SIGNATURE]]</span></div>
        <div class="sig-field">By: <span class="sig-val">__PROVIDER_BY__</span></div>
        <div class="sig-field">Title: <span class="sig-val">__PROVIDER_TITLE__</span></div>
        <div class="sig-field">Date: <span class="sig-anchor">[[PROVIDER_DATE]]</span></div>
      </td>
      <td>
        <div class="sig-party">Participant</div>
        <div class="sig-entity">{{client_name}}</div>
        <div class="sig-line"><span class="sig-anchor">[[PARTICIPANT_SIGNATURE]]</span></div>
        <div class="sig-field">By: <span class="sig-val">&nbsp;</span></div>
        <div class="sig-field">Name: <span class="sig-val">{{client_signer_name}}</span></div>
        <div class="sig-field">Title: <span class="sig-val">{{client_title}}</span></div>
        <div class="sig-field">Date: <span class="sig-anchor">[[PARTICIPANT_DATE]]</span></div>
      </td>
    </tr></table>
  </div>
</body></html>
"""


def build_html() -> str:
    body_html = _md.render(BODY_MD.read_text())
    shell = (
        _SHELL.replace("__STYLE__", _STYLE)
        .replace("__WORDMARK__", WORDMARK)
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
    test_mode = os.environ.get("DOCRAPTOR_TEST", "1") != "0"
    html_doc = build_html()
    payload = {
        "test": test_mode,
        "document_type": "pdf",
        "name": OUT.name,
        "document_content": html_doc,
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
    print(f"wrote {OUT}  ({len(resp.content):,} bytes)  [{mode}]")


if __name__ == "__main__":
    main()
