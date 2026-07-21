# Active Operators — Strategic Origination Agreement (Term Only)

Render-ready HTML for the Active Operators Strategic Origination Agreement. DocRaptor renders this
file directly to a PDF, and that PDF is the source document for a Documenso **template** — the
signature, date, and value fields are affixed onto it in the Documenso editor. The wordmark, full
legal body, and the execution/signature block are **held inline**: the document is complete as
written, not assembled by appending a signature block at render time.

## Layout

```
global_agreement_content/
  active_operators_term_only.html   # style-agnostic document: wordmark + legal body + signature block
  styles/
    plain.css                       # white page / black text          (sent today)
    branded.css                     # dark Active Operators identity    (alternate)
  manifest.json                     # per-document metadata + the style flag
```

## Two style options, one body

The document carries a single `<style>__STYLESHEET__</style>` slot. The render step injects **one**
of the two stylesheets — the body (legal text, wordmark, signature block) is identical either way,
so the legal content has a single source of truth and never drifts between styles:

| `manifest.plain` | Injected stylesheet | Result |
| --- | --- | --- |
| **`true`** (current) | `styles/plain.css` | White background, black text. DocRaptor emits a neutral PDF; Documenso layers its own branding on top. |
| `false` | `styles/branded.css` | Fully self-branded dark Active Operators / Rare Structure PDF. |

Send **plain** right now (`"plain": true`). Flip the flag to switch — no change to the body.

## Fields

The HTML is a **static PDF body**. Every dynamic value is reserved space that a field is dropped
onto in the Documenso template editor — nothing is filled into the HTML before render.

- **Inline blanks** — empty fixed-width `<span class="field-slot">` gaps (sized in `ch`) placed in
  the prose so a dropped field sits clear of the surrounding text:
  - Participant name — opening clause (`by and between … and ___`)
  - Engagement Fee — §3.1 (`$xx,xxx`)
  - Term length in days — §5.1
- **Signature block** — the Participant column is empty rows ready for fields: entity, signature
  line, **By:**, **Title:**, **Date:**. The Provider column is pre-set (Rare Structure LLC /
  Benjamin J. Crane / Managing Director) with an empty signature line and **Date:**.

The effective date is not a field — the agreement is effective as of the date of execution and
payment (static legalese in the body).

## term_only

The **term-only** archetype: a fixed Engagement Fee for the Initial Term, with **no
performance/success fee**.
