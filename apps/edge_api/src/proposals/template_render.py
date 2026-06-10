"""Markdown-authored proposal templates → brand-matched HTML for DocRaptor.

The operator authors only the BODY of an agreement, in markdown (the numbered sections —
the ~85% that is canonical legal text). This module:

  * renders that markdown to HTML (``markdown-it-py``, CommonMark + GFM tables),
  * wraps it in the Rare Structure brand SHELL — the dark identity, the letterhead, and the
    execution/signature block. The client sign + date lines carry the ``[[CLIENT_SIGNATURE]]`` /
    ``[[CLIENT_DATE]]`` anchor markers (``signing_anchors``); Documenso resolves the SIGNATURE +
    DATE field positions from them via ``findText`` at sign-time (see ``documenso_client``), and
  * substitutes ``{{handlebars}}`` merge tokens.

Tokens are OPEN-ENDED: whatever ``{{snake_case}}`` the author writes in the body, PLUS the
shell's own signature tokens (``{{rs_name}}``, ``{{client_signer_name}}`` …). ``extract_tokens``
scans the FULLY ASSEMBLED document, so the authoring UI surfaces shell + body tokens together
and the operator fills each — the editor never needs to know what a token *means*. The ``[[...]]``
anchor markers use a deliberately DIFFERENT bracket grammar, so token extraction/substitution
ignore them and they survive verbatim into the PDF for Documenso's ``findText``.

The built-in Strategic Origination Mandate (``agreement_template._TEMPLATE``) carries the same two
client anchors and remains the fallback; this path serves templates published through the
authoring surface.
"""
from __future__ import annotations

import datetime as _dt
import html
import re

from markdown_it import MarkdownIt

from .models import Proposal, format_usd, total_cents
from .signing_anchors import CLIENT_DATE_ANCHOR, CLIENT_SIGNATURE_ANCHOR

# CommonMark + GFM tables/strikethrough. Linkify is deliberately OFF — bare URLs in legal text
# should stay literal, and it avoids the linkify-it-py dependency. ``html=False`` (the default)
# means raw HTML embedded in the markdown is escaped, not passed through — the operator authors
# prose + tables, never markup, so a stray ``<script>`` can never reach the rendered legal PDF.
_md = MarkdownIt("commonmark").enable("table").enable("strikethrough")

# A merge field: ``{{snake_case}}`` with optional inner whitespace.
_TOKEN_RE = re.compile(r"\{\{\s*([a-zA-Z0-9_]+)\s*\}\}")


def markdown_to_html(markdown_src: str) -> str:
    """Render authored markdown to body HTML."""
    return _md.render(markdown_src or "")


def extract_tokens(text: str) -> list[str]:
    """Unique ``{{tokens}}`` in first-seen order (run over the assembled document)."""
    seen: dict[str, None] = {}
    for m in _TOKEN_RE.finditer(text or ""):
        seen.setdefault(m.group(1), None)
    return list(seen.keys())


def substitute_tokens(html_doc: str, values: dict[str, str], *, escape: bool = True) -> str:
    """Replace every ``{{token}}`` with ``values[token]``.

    Unknown tokens are left LITERAL (``{{foo}}``) on purpose — an unfilled field stays visible
    rather than vanishing silently. Values are HTML-escaped by default (they are plain merge data
    — names, money, dates — injected into HTML).
    """

    def repl(m: re.Match[str]) -> str:
        key = m.group(1)
        if key not in values:
            return m.group(0)
        v = values[key]
        return html.escape(v) if escape else v

    return _TOKEN_RE.sub(repl, html_doc)


def render_branded_document(body_html: str, *, apply_brand: bool = True) -> str:
    """Wrap authored body HTML in the brand shell + execution block.

    The shell's own sentinels (style + the client signature/date anchor markers) are resolved
    FIRST, against the shared ``signing_anchors`` constants; ``«BODY»`` is injected LAST so authored
    body content can never be mistaken for a shell sentinel.
    """
    style = _BRAND_STYLE if apply_brand else _PLAIN_STYLE
    shell = (
        _SHELL.replace("«STYLE»", style)
        .replace("«CLIENT_SIGNATURE_ANCHOR»", CLIENT_SIGNATURE_ANCHOR)
        .replace("«CLIENT_DATE_ANCHOR»", CLIENT_DATE_ANCHOR)
    )
    return shell.replace("«BODY»", body_html)


def render_template_html(markdown_src: str, *, apply_brand: bool = True) -> str:
    """markdown → body HTML → branded, signable document (tokens NOT yet substituted)."""
    return render_branded_document(markdown_to_html(markdown_src), apply_brand=apply_brand)


def _long_date(d: _dt.date) -> str:
    return f"{d:%B} {d.day}, {d:%Y}"


_CADENCE_PHRASE = {
    "upfront_in_full": "in full upon execution",
    "monthly": "monthly in advance",
    "quarterly": "every three (3) months in advance",
}


def success_fee_markdown_table(schedule: list[dict[str, str]]) -> str:
    """The success-fee schedule as a GFM markdown table (tier | rate), built from the proposal's
    STRUCTURED ``success_fee_schedule`` so operator edits to the tiers flow into the rendered PDF.
    Empty schedule → empty string (the token simply disappears)."""
    if not schedule:
        return ""
    rows = [
        "| Total Contract Value Tier | Success Fee Percentage |",
        "| :--- | :--- |",
    ]
    for item in schedule:
        # Collapse ALL interior whitespace (newlines/CR/tabs) to single spaces FIRST — a stray \n or
        # \r in a cell would otherwise break the single-line GFM row and corrupt the legal table —
        # then escape pipes so a literal "|" can't add a column.
        tier = " ".join(str(item.get("tier", "")).split()).replace("|", "\\|")
        rate = " ".join(str(item.get("rate", "")).split()).replace("|", "\\|")
        rows.append(f"| {tier} | {rate} |")
    return "\n".join(rows)


def substitute_markdown_tokens(markdown_src: str, p: Proposal) -> str:
    """Pre-render dynamic BLOCK tokens into the markdown SOURCE (before markdown→HTML) so they
    render as native markdown. Currently just ``{{success_fee_table}}`` → the structured tier
    table; a no-op when the token is absent. Inline scalar tokens (fee/cadence/total/duration) are
    handled later by ``substitute_tokens`` against ``proposal_token_values``."""
    if "{{success_fee_table}}" in markdown_src:
        markdown_src = markdown_src.replace(
            "{{success_fee_table}}", success_fee_markdown_table(p.success_fee_schedule)
        )
    return markdown_src


def proposal_token_values(p: Proposal) -> dict[str, str]:
    """The standard merge values bound from a real Proposal row at generation time. A published
    template's tokens resolve against these (anything outside this set renders literal). Carries
    BOTH ``total`` (engagement value = monthly × duration) and the legacy ``quarterly_total`` alias
    so old and new template bodies both resolve."""
    total = total_cents(p.monthly_fee_cents, p.duration_months)
    return {
        "client_name": p.client_name,
        "client_signer_name": p.client_signer_name,
        "client_title": p.client_title or "",
        "client_email": p.client_email,
        "effective_date": _long_date(p.effective_date),
        "monthly_fee": format_usd(p.monthly_fee_cents),
        "duration": str(p.duration_months),
        "billing_cadence": _CADENCE_PHRASE.get(p.billing_cadence, p.billing_cadence),
        "total": format_usd(total),
        "quarterly_total": format_usd(total),  # legacy alias — old bodies still resolve
        "rs_name": p.rs_signer_name,
    }


# ── Brand shell ───────────────────────────────────────────────────────────────────────────────
# Reuses the Rare Structure dark identity from ``agreement_template._TEMPLATE``. The authored body
# is injected at «BODY»; the execution block (with its {{tokens}}) is appended after it. The client
# signature + date lines carry the [[CLIENT_SIGNATURE]] / [[CLIENT_DATE]] anchor markers (literal
# text, NOT {{merge}} tokens) — Documenso resolves the field positions from them via ``findText``
# at sign-time (see ``services.documenso_client``).

_BRAND_STYLE = r"""
  @page { size: Letter; margin: 1in 1in 1.1in 1in; background: #0a0e1a;
    @bottom-center {
      content: "RARE STRUCTURE LLC  \2022  Page " counter(page);
      font-family: 'Helvetica Neue', Arial, sans-serif; font-size: 7.5pt; letter-spacing: 0.12em;
      color: #82828c; text-transform: uppercase;
    }
  }
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
  h3 { font-family: 'Helvetica Neue', Arial, sans-serif; font-size: 8.5pt; letter-spacing: 0.1em;
    text-transform: uppercase; font-weight: 600; margin: 12pt 0 4pt; color: #a1a1aa; }
  p { margin: 0 0 8pt; text-align: justify; }
  ul, ol { margin: 0 0 8pt; padding-left: 18pt; }
  li { margin: 0 0 4pt; }
  a { color: #7b9fd4; }
  blockquote { margin: 8pt 0; padding-left: 12pt; border-left: 2pt solid #2d3548; color: #a1a1aa; }
  table { width: 100%; border-collapse: collapse; margin: 8pt 0 4pt; font-size: 10pt; }
  th { text-align: left; font-family: 'Helvetica Neue', Arial, sans-serif; font-size: 8pt;
    letter-spacing: 0.12em; text-transform: uppercase; font-weight: 600; color: #82828c;
    border-bottom: 1pt solid #3f4b63; padding: 5pt 6pt; }
  td { padding: 6pt 6pt; border-bottom: 0.5pt solid #1c2333; color: #e4e4e7; }
  td:last-child { text-align: right; font-variant-numeric: tabular-nums; white-space: nowrap;
    color: #fafafa; }
  /* Execution block. The page break is now COSMETIC (keeps the signatures on a clean final page);
     it is NOT load-bearing — Documenso places the SIGNATURE + DATE fields by resolving the
     [[CLIENT_SIGNATURE]] / [[CLIENT_DATE]] anchor markers via ``findText``, so position no longer
     depends on the block sitting at a fixed page coordinate. */
  .sig-wrap { margin-top: 26pt; page-break-inside: avoid; page-break-before: always; }
  .sig-grid { width: 100%; border-collapse: separate; border-spacing: 0; }
  .sig-grid td { width: 50%; vertical-align: top; padding-right: 24pt; border-bottom: 0;
    text-align: left; }
  .sig-party { font-family: 'Helvetica Neue', Arial, sans-serif; font-size: 8pt;
    letter-spacing: 0.14em; text-transform: uppercase; color: #82828c; margin-bottom: 14pt; }
  .sig-line { border-bottom: 1pt solid #2d3548; height: 30pt; margin-bottom: 3pt; }
  .sig-rs { font-family: Georgia, 'Times New Roman', serif; font-style: italic; font-weight: 600;
    font-size: 18pt; line-height: 30pt; padding-left: 4pt; color: #7b9fd4; }
  .sig-field { font-size: 8pt; color: #82828c; font-family: 'Helvetica Neue', Arial, sans-serif; }
  .sig-val { font-size: 10pt; color: #e4e4e7; }
  /* Anchor marker on the client sign/date lines: a subtle placeholder, but REAL selectable text in
     a standard font — Documenso's findText resolves + whites it out at sign-time. Never set to
     display:none / zero-size / a script font, or the PDF text layer loses it. */
  .sig-anchor { font-family: 'Helvetica Neue', Arial, sans-serif; font-size: 8pt;
    letter-spacing: 0.08em; color: #5b6373; line-height: 30pt; padding-left: 4pt; }
"""

# Light/unbranded variant — same class vocabulary (so the body + signature block still render),
# neutral print colors. The .sig-wrap page break is preserved as a COSMETIC choice (not load-
# bearing — Documenso places fields by anchor, see the brand-style note above and documenso_client).
_PLAIN_STYLE = r"""
  @page { size: Letter; margin: 1in 1in 1.1in 1in;
    @bottom-center { content: "Page " counter(page); font-family: Arial, sans-serif;
      font-size: 7.5pt; color: #666; } }
  * { box-sizing: border-box; }
  body { font-family: 'Georgia', 'Times New Roman', serif; font-size: 10.5pt; line-height: 1.55;
    color: #111; margin: 0; }
  strong { color: #000; }
  .wordmark { font-family: Arial, sans-serif; font-weight: 700; letter-spacing: 0.34em;
    font-size: 12pt; color: #000; }
  h1 { font-family: Arial, sans-serif; font-size: 15pt; letter-spacing: 0.06em; font-weight: 600;
    margin: 6pt 0 2pt; color: #000; }
  .rule { border: 0; border-top: 1pt solid #ccc; margin: 10pt 0 16pt; }
  h2 { font-family: Arial, sans-serif; font-size: 9pt; letter-spacing: 0.16em;
    text-transform: uppercase; font-weight: 600; margin: 18pt 0 5pt; color: #333; }
  h3 { font-family: Arial, sans-serif; font-size: 8.5pt; letter-spacing: 0.1em;
    text-transform: uppercase; font-weight: 600; margin: 12pt 0 4pt; color: #555; }
  p { margin: 0 0 8pt; text-align: justify; }
  ul, ol { margin: 0 0 8pt; padding-left: 18pt; }
  li { margin: 0 0 4pt; }
  table { width: 100%; border-collapse: collapse; margin: 8pt 0 4pt; font-size: 10pt; }
  th { text-align: left; font-family: Arial, sans-serif; font-size: 8pt; letter-spacing: 0.12em;
    text-transform: uppercase; font-weight: 600; color: #555; border-bottom: 1pt solid #999;
    padding: 5pt 6pt; }
  td { padding: 6pt 6pt; border-bottom: 0.5pt solid #ddd; }
  td:last-child { text-align: right; font-variant-numeric: tabular-nums; white-space: nowrap; }
  .sig-wrap { margin-top: 26pt; page-break-inside: avoid; page-break-before: always; }
  .sig-grid { width: 100%; border-collapse: separate; border-spacing: 0; }
  .sig-grid td { width: 50%; vertical-align: top; padding-right: 24pt; border-bottom: 0;
    text-align: left; }
  .sig-party { font-family: Arial, sans-serif; font-size: 8pt; letter-spacing: 0.14em;
    text-transform: uppercase; color: #555; margin-bottom: 14pt; }
  .sig-line { border-bottom: 1pt solid #999; height: 30pt; margin-bottom: 3pt; }
  .sig-rs { font-family: Georgia, serif; font-style: italic; font-weight: 600; font-size: 18pt;
    line-height: 30pt; padding-left: 4pt; color: #111; }
  .sig-field { font-size: 8pt; color: #555; font-family: Arial, sans-serif; }
  .sig-val { font-size: 10pt; color: #111; }
  /* Anchor marker — subtle but REAL selectable text in a standard font (Documenso findText). */
  .sig-anchor { font-family: Arial, sans-serif; font-size: 8pt; letter-spacing: 0.08em;
    color: #888; line-height: 30pt; padding-left: 4pt; }
"""

_SHELL = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8" />
<title>Engagement Proposal</title>
<style>«STYLE»</style>
</head>
<body>
  <div class="wordmark">RARE STRUCTURE</div>
  <hr class="rule" />
«BODY»
  <div class="sig-wrap">
    <table class="sig-grid"><tr>
      <td>
        <div class="sig-party">Rare Structure LLC</div>
        <div class="sig-line"><span class="sig-rs">{{rs_name}}</span></div>
        <div class="sig-field">By: <span class="sig-val">{{rs_name}}</span></div>
        <div class="sig-field">Title: <span class="sig-val">Managing Director</span></div>
        <div class="sig-field">Date: <span class="sig-val">{{effective_date}}</span></div>
      </td>
      <td>
        <div class="sig-party">Client / Institutional Partner</div>
        <!-- Documenso resolves the SIGNATURE + DATE field positions from the «CLIENT_SIGNATURE_ANCHOR»
             / «CLIENT_DATE_ANCHOR» markers below via findText, and whites them out at sign-time. -->
        <div class="sig-line"><span class="sig-anchor">«CLIENT_SIGNATURE_ANCHOR»</span></div>
        <div class="sig-field">By: <span class="sig-val">&nbsp;</span></div>
        <div class="sig-field">Name: <span class="sig-val">{{client_signer_name}}</span></div>
        <div class="sig-field">Title: <span class="sig-val">{{client_title}}</span></div>
        <div class="sig-field">Date: <span class="sig-anchor">«CLIENT_DATE_ANCHOR»</span></div>
      </td>
    </tr></table>
  </div>
</body>
</html>
"""
