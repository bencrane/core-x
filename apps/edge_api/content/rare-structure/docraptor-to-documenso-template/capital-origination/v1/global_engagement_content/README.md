# Rare Structure — Strategic Origination Agreement (Capital Origination)

Render-ready HTML for the Rare Structure Strategic Origination Agreement — the capital/lending
origination variant (capital providers, lenders, financing events). DocRaptor renders this file
directly to a PDF, and that PDF is the source document for a Documenso **template** — the signature,
date, and value fields are affixed onto it in the Documenso editor. The wordmark, full legal body,
and the execution/signature block are **held inline**: the document is complete as written, not
assembled by appending a signature block at render time.

This is the Rare Structure sibling of `content/active-operators/.../term-only/v1`. Pure Rare
Structure — **no `doing business as Active Operators` d/b/a**, and **no Master Terms incorporation**.

## Layout

```
global_engagement_content/
  rare_structure_strategic_origination.html   # style-agnostic document: wordmark + legal body + signature block
  styles/
    plain.css                                 # white page / black text          (sent today)
    branded.css                               # dark Rare Structure identity      (alternate)
  manifest.json                               # per-document metadata + the style flag
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

## Fields

The HTML is a **static PDF body** — **no merge tokens, no `[[anchors]]`**. Every dynamic value is
reserved space that a field is dropped onto in the Documenso template editor; nothing is filled into
the HTML before render.

- **Inline blanks** — fixed-width underscore runs placed in the prose so a dropped field sits clear
  of the surrounding text:
  - Effective Date — opening clause (`as of ____`)
  - Participant name — opening clause (`by and between … and ____`)
  - Systems Integration Fee — §3.1 (`$____`)
  - Success Fee percentage — §3.2 (`____%`)
  - Success Fee payment window — §3.2 (`within ____ days`)
- **Signature block** — the Participant column is empty rows ready for fields: entity, signature
  line, **By:**, **Title:**, **Date:**. The Provider column is pre-set (Rare Structure LLC /
  Benjamin J. Crane / Managing Director) with an empty signature line and **Date:**.

## capital_origination

The capital-origination archetype's financial architecture:

- **§3.1 Systems Integration & Enrichment Fee** — one-time, non-refundable, due on execution.
- **§3.2 Marketing Referral & Success Fee** — percentage of the total approved facility limit / total
  committed capital of each Financing Event (credit facility, equipment loan, factoring, capital
  deployment); calculated on the maximum approved credit limit at closing regardless of drawdown;
  absolute and non-refundable once remitted (no clawback / offset / refund).
- **§3.3 Rolling Client Ownership** — Success Fee applies to all subsequent Financing Events,
  renewals, and line increases with a Provider-introduced entity for **24 months** from introduction.
- **§5.2 Survival & Tail Protection** — Success-Fee obligations survive termination for the 24-month
  rolling-ownership window.

Provider executes as **Rare Structure LLC** (Benjamin J. Crane, Managing Director).
