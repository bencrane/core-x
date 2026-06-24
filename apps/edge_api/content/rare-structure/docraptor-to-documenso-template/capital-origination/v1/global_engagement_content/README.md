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

## Origination Lanes (Documenso direct-to-documenso mode)

When an operator is in `render_mode='direct-to-documenso'`, the origination flow uses this template via one of three lanes (selected by `direct_to_documenso_lane` in `operator_settings`):

- **prefill-document-from-template** (DEFAULT): POST `/api/v2/template/use` with opportunity-specific field values prefilled, then distribute (NONE) → PENDING. Returns signing token + document id; signer receives the document immediately (source: PREFILLED_DOCUMENT).
- **embed-template** (NEW): Enable a DIRECT LINK on the template via POST `/api/v2/template/direct/create`, returning a reusable token. Signer self-identifies in the embed; Documenso creates the document AT completion (source: TEMPLATE_DIRECT_LINK).
- **envelope-distribute** (RETIRED): The /envelope/use + .../confirm lane was removed in code; the operator_settings CHECK retains the value so pre-existing rows never violate it, but no live path serves it.

The corresponding edge_api endpoints:
- POST `/api/v1/engagement-mandate-drafts/{draft_id}/originate-prefilled` → MandatePrefilledOriginated {envelope_id, document_id, opportunity_id, signing_token, status, documenso_host}
- POST `/api/v1/engagement-mandate-drafts/{draft_id}/originate-embed-template` → MandateEmbedTemplateOriginated {direct_token, documenso_host, embed_url, external_id, opportunity_id, direct_recipient_id, recipient_email, recipient_name, status}

Verify lane availability in `apps/edge_api/sql/operator_settings.sql` (lines 85-89: CHECK constraint on `direct_to_documenso_lane`).

## Render + Push Lane (engagement-template-push)

The control plane (Trigger.dev task `engagement-template-push`, src/trigger/engagement_template_push.ts) renders this HTML asset to PDF and publishes it as a Documenso TEMPLATE. The lane is orchestrated by:

1. **Content source registry** (`business.global_input_content`, apps/edge_api/sql/global_input_content.sql): Rows carry `brand='rare-structure'` + `path='docraptor-to-documenso-template/capital-origination/v1'` + `source_kind='repo-html'`.
2. **Catalog discovery** (apps/edge_api/src/engagement_templates/catalog.py): Resolves (brand, path, archetype, version) tuples to `apps/edge_api/content/<brand>/<path>/<archetype>/<version>/global_engagement_content/` directories. Allows only `_ALLOWED_BRANDS={'active-operators', 'rare-structure'}`.
3. **Render + push** (apps/edge_api/src/engagement_templates/push.py): Assembles HTML + CSS, invokes DocRaptor to PDF, creates Documenso TEMPLATE via POST `/api/v2/envelope/create` (type=TEMPLATE), records outcome in `ops.engagement_template_push_runs` ledger.
4. **Edge API endpoint** (apps/edge_api/src/routers/internal_engagement_templates_v1.py): POST `/internal/engagement-templates/render-push` (trigger-secret gated) accepts `registryPath` or explicit `brand`/`path`/`archetype`/`version`; returns documenso_template_id + numeric_id.

The ledger table `ops.engagement_template_push_runs` (apps/edge_api/sql/ops_engagement_template_push_runs.sql) records every terminal state (success | error): brand, path, archetype, version, style, source_kind, documenso_template_id, pdf_bytes, pdf_r2_key, error reason.
