# Rare Structure (Government-Contracted) — Strategic Origination Agreement (Prepaid Introductions)

Render-ready HTML for the **government-contracted** brand's first template — the **prepaid-introductions**
archetype. DocRaptor renders this file directly to a PDF, and that PDF is the source document for a
Documenso **template**; the signature, date, value, and count fields are affixed onto it in the Documenso
editor (the render+push lane creates the template with NO fields — they are placed by hand afterward).

> **PLACEHOLDER v1.** The HTML body is a clearly-marked scaffold. Replace it with the supplied
> government-contracted content. On intake, convert every handlebar `{{token}}` to an underscore run
> (`_______`) — the render+push lane performs **zero** token substitution, so a stray `{{token}}` would
> render literally into the PDF.

## Brand → Organization

`government-contracted` is its own **brand** (a content-root subtree + a `catalog._ALLOWED_BRANDS` entry)
but **rolls up to the Rare Structure organization** — the first brand whose subtree name differs from its
org (`active-operators` and `rare-structure` are 1:1 brand↔org). Consequences:

- **Provider** executes as **Rare Structure LLC** (Benjamin J. Crane, Managing Director).
- **`branded.css`** is the Rare Structure dark identity (shared with the `rare-structure` brand).
- At DB-registration time, `business.documenso_templates.organization_id` and
  `business.engagement_documenso_template_mappings.organization_id` point at the **Rare Structure** org.

## Archetype — `prepaid_introductions`

A NEW economic shape (no `business.engagement_archetypes` row yet — see Deferred). The Participant
**pre-pays a fixed amount** for a **dedicated allocation of Introductions** over a **primary fulfillment
window (X days)**, at a fixed **per-introduction unit cost (Y)**. Any balance unfulfilled at primary-term
expiration resolves to the Participant's election of a **prorated refund** (Option A) or a **credit
rollover** (Option B, transitioning to a passive-allocation mandate). An "Introduction" is strictly an
outbound email facilitation (§2.2):

```
allocation (Z)  =  prepayment (amount paid)  ÷  price_per_introduction (Y)
```

Economic fill-slots in the body — each a blank underscore run paired with an invisible `[[XX]]` findText
anchor:

| Slot | Anchor | Meaning |
| --- | --- | --- |
| Effective Date | `[[EFD]]` | agreement effective date |
| Participant entity | `[[LEN]]` | Participant legal entity name (the preamble reads "[name] d/b/a [dba]" verbatim from source — the participant's own d/b/a) |
| Prepayment amount | `[[AMT]]` | total prepaid (the non-refundable upfront fee) |
| Allocation count | `[[QTY]]` | number of Introductions (Z) — recurs in §3.1, §3.2, §3.4 |
| Primary term | `[[WIN]]` | fulfillment window in days |
| Per-introduction price | `[[PPI]]` | unit cost (Y) — recurs in §3.1, §3.4 A/B |
| Execution block | `[[VSG]] [[VDT]] [[PSG]] [[PNM]] [[PTL]] [[PDT]]` | Provider/Participant signature, date, name, title |

## Layout

```
global_engagement_content/
  government_contracted_prepaid_introductions.html   # style-agnostic body: wordmark + legal body + signature block
  styles/
    plain.css                                        # white page / black text (sent today)
    branded.css                                      # dark Rare Structure identity (alternate)
  manifest.json                                      # per-document metadata + the style flag
```

## Two style options, one body

The document carries a single `<style>__STYLESHEET__</style>` slot; the render step injects ONE of the
two stylesheets — the body is identical either way. `manifest.plain = true` sends `plain.css` (neutral
white/black; Documenso layers its own branding on top). Flip to `false` for the self-branded Rare
Structure dark PDF.

## Fields & convention

Static PDF body — **no `{{handlebars}}`, no merge tokens.** Every dynamic value is either a visible blank
**underscore run** (where the value will show) or an **invisible `[[XX]]` anchor** (white, 5pt — painted
into the text layer for Documenso `findText`, invisible to the eye). Documenso fields are dropped onto
these slots in the editor; nothing is filled into the HTML before render. Underscore-run widths are
render-dependent — measure with a local DocRaptor preview, do not eyeball.

## Render + Push lane (`engagement-template-push`)

1. **Catalog discovery** — `apps/edge_api/src/engagement_templates/catalog.py` resolves
   `(brand, path, archetype, version)` under
   `apps/edge_api/content/<brand>/<path>/<archetype>/<version>/global_engagement_content/`.
   `_ALLOWED_BRANDS` now includes `government-contracted`.
2. **Render + push** — `apps/edge_api/src/engagement_templates/push.py` assembles HTML + CSS, invokes
   DocRaptor (LIVE), creates a Documenso TEMPLATE via `POST /api/v2/envelope/create` (type=TEMPLATE),
   and records the outcome in `ops.engagement_template_push_runs`.
3. **Endpoint** — `POST /internal/engagement-templates/render-push` (trigger-secret), body
   `{"registryPath":"docraptor-to-documenso-template/prepaid-introductions/v1"}` OR explicit
   `brand=government-contracted` / `path=docraptor-to-documenso-template` /
   `archetype=prepaid-introductions` / `version=v1`. Render-only preview (no Documenso):
   `POST /api/v1/engagement-templates/render` with the SPLIT segments.

## Deferred (DB-registration time — not in this scaffold)

- **`business.engagement_archetypes`** — seed a `prepaid_introductions` archetype row (no
  `performance_fee_basis`: it is prepaid, not performance-based). Seeded archetypes today are `term_only`
  and `term_plus_greater_of`.
- **`business.global_input_content`** — seed a content-source row
  (`path='docraptor-to-documenso-template/prepaid-introductions/v1'`, `brand='government-contracted'`,
  `source_kind='repo-html'`) IF render-push should resolve via `registryPath` (explicit-params push needs
  no row). `global_input_content.brand` has **no** CHECK constraint, so no DDL widening is required.
- **`business.documenso_templates`** + **`business.engagement_documenso_template_mappings`** — after the
  template is pushed and fields are placed, register the template row (`organization_id` → Rare Structure)
  and the operator-dropdown mapping. Runbook:
  `docs/reference/DOCUMENSO_ARCHITECTURE/10-TEMPLATE-ITERATION-RUNBOOK.md`.
