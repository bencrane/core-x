# Global Engagement Content — HTML form

Repo-resident global engagement content, **as render-ready HTML** — the on-disk parallel to the
`business.global_engagement_content` table, which holds the **markdown** body the operator authors.
Here we do not hold markdown: each document is the **HTML that DocRaptor renders directly**, and it
**holds the wordmark and the execution/signature block inline** (it is not assembled by appending a
signature block at render time).

## Layout

```
global_engagement_content/
  active_operators_term_only.html   # style-agnostic document: wordmark + legal body + signature block
  styles/
    plain.css                       # white page / black text          (SENT TODAY)
    branded.css                     # dark Active Operators identity    (alternate)
  manifest.json                     # per-document metadata + the style flag
```

## Two style options, one body

The document carries a single `<style>__STYLESHEET__</style>` slot. The render step injects **one**
of the two stylesheets — the body (the legal text, wordmark, and signature block) is identical
either way, so the legal content has a single source of truth and never drifts between styles:

| `manifest.plain` | Injected stylesheet | Result |
| --- | --- | --- |
| **`true`** (current) | `styles/plain.css` | White background, black text. **DocRaptor emits a neutral PDF; Documenso layers its own branding on top.** |
| `false` | `styles/branded.css` | Fully self-branded dark Active Operators / Rare Structure PDF (no Documenso branding needed). |

We send **plain** right now (`"plain": true`). Flip the flag to switch — no change to the body.

## Tokens & anchors

- **Merge tokens** are `{{snake_case}}` — bound per deal at generation: `participant_name`,
  `term_fee`, `duration_in_months`, `participant_signer_name`, `participant_title`. The effective
  date is NOT a token — the agreement is "effective as of the date of execution + payment" (static
  legalese in the body). Unfilled tokens render literally (they never silently vanish).
- **Documenso findText anchors** are `[[ANCHOR]]` — a deliberately different bracket grammar so
  token substitution never touches them; they ride into the PDF verbatim and Documenso resolves each
  SIGNATURE/DATE field position from them, then whites them out at sign-time:
  `[[PROVIDER_SIGNATURE]]`, `[[PROVIDER_DATE]]`, `[[PARTICIPANT_SIGNATURE]]`, `[[PARTICIPANT_DATE]]`.

## term_only

This document is the **term-only** archetype: a fixed Engagement Fee for the Initial Term, **no
performance/success fee**. The two performance-fee blocks present in the markdown source
(`apps/edge_api/content/active_operators_strategic_origination.md` — §3.3 Success Fee, §5.2 Tail
Protection) are **absent** here. Section numbers are otherwise unchanged.

## Provenance

Legal text is transcribed faithfully from the approved markdown source
`apps/edge_api/content/active_operators_strategic_origination.md` (term-only selection). The
plain/branded stylesheets and the inline signature block follow the conventions already proven in
`apps/edge_api/src/proposals/agreement_template.py` and `scripts/render_ao_preview.py`.
