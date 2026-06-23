# Rare Structure — Strategic Origination Agreement (Capital Origination)

Render-ready HTML for the Rare Structure Strategic Origination Agreement — the capital/lending
origination variant (capital providers, lenders, financing events). DocRaptor renders this file
directly to a PDF, and that PDF is the source document for a Documenso **template**. The wordmark,
full legal body, and the execution/signature block are **held inline**: the document is complete as
written, not assembled by appending a signature block at render time.

This is the Rare Structure sibling of `content/active-operators/.../term-only/v1`. Pure Rare
Structure — **no `doing business as Active Operators` d/b/a**, and **no Master Terms incorporation**.

## Layout

```
global_engagement_content/
  rare_structure_strategic_origination.html   # style-agnostic document: wordmark + legal body + signature block
  styles/
    plain.css                                 # white page / black text          (sent today)
    branded.css                               # dark Rare Structure identity      (alternate)
  manifest.json                               # per-document metadata, style flag, tokens, anchors
```

## Two style options, one body

The document carries a single `<style>__STYLESHEET__</style>` slot. The render step injects **one**
of the two stylesheets — the body (legal text, wordmark, signature block) is identical either way,
so the legal content has a single source of truth and never drifts between styles:

| `manifest.plain` | Injected stylesheet | Result |
| --- | --- | --- |
| **`true`** (current) | `styles/plain.css` | White background, black text. DocRaptor emits a neutral PDF; Documenso layers its own branding on top. |
| `false` | `styles/branded.css` | Fully self-branded dark Rare Structure PDF. |

Send **plain** right now (`"plain": true`). Flip the flag to switch — no change to the body.

## Token-bearing (differs from the active-operators sibling)

The active-operators sibling is a static body with blank slots — every field is dropped on in the
Documenso editor. **This document is token-bearing**, matching the `render_ao_preview` /
`template_render` convention:

- **`{{merge_tokens}}`** are substituted at use-time (before/at template instantiation):
  - `{{effective_date}}` — opening clause
  - `{{participant_name}}` — opening clause + Participant signature block entity
  - `{{integration_fee_amount}}` — §3.1 Systems Integration Fee
  - `{{success_fee_percentage}}` — §3.2 Success Fee (% of funded amount)
  - `{{payment_window_days}}` — §3.2 payment window
  - `{{participant_signer_name}}` — Participant **By:**
  - `{{participant_title}}` — Participant **Title:**
- **`[[FINDTEXT_ANCHORS]]`** on the signature/date lines are resolved by Documenso's `findText` at
  sign-time, then whited out: `[[PROVIDER_SIGNATURE]]`, `[[PROVIDER_DATE]]`,
  `[[PARTICIPANT_SIGNATURE]]`, `[[PARTICIPANT_DATE]]`. They are **real selectable text** — never set
  `display:none`, zero-size, or a script font, or the PDF text layer loses them.

The `{{...}}` merge grammar and the `[[...]]` anchor grammar are deliberately distinct, so token
substitution never touches the anchors and they survive verbatim into the PDF.

## capital_origination

The capital-origination archetype's financial architecture:

- **§3.1 Systems Integration & Enrichment Fee** — one-time, non-refundable, due on execution.
- **§3.2 Marketing Referral & Success Fee** — percentage of the total funded amount / max facility
  limit of each Financing Event (credit facility, equipment loan, factoring, capital deployment).
- **§3.3 Rolling Client Ownership** — Success Fee applies to all subsequent Financing Events,
  renewals, and line increases with a Provider-introduced entity for **24 months** from introduction.
- **§5.2 Survival & Tail Protection** — Success-Fee obligations survive termination for the 24-month
  rolling-ownership window.

Provider executes as **Rare Structure LLC** (Benjamin J. Crane, Managing Director).
