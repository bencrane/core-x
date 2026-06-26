# 10 — Engagement Template Iteration Runbook (operator / agent how-to)

> STATUS — **ACTIVE**. This is the OPERATIONAL runbook for iterating on the engagement
> AGREEMENT templates (rare-structure / active-operators): editing the agreement body,
> re-rendering it into a Documenso template, registering it, and wiring per-field
> **prefill + editable/locked** behavior. Architecture depth lives in `03`
> (direct-to-documenso flow), `04` (Documenso integration), `06` (templates layer), `07`
> (data stores). This doc adds the two primitives those predate —
> `recipients->'default_field_values'` and `recipients->'editable_field_labels'` — and the
> end-to-end loop. **Point a fresh agent here first.**

---

## The map — what you actually touch

**Content (the agreement body + layout) — repo files**
`apps/edge_api/content/<brand>/docraptor-to-documenso-template/<archetype>/<version>/global_engagement_content/`
- `<doc>.html` — the agreement body. Every dynamic value is a blank **fill-slot**
  (underscores, or a `.field-slot` span) that a Documenso field is dropped over in the
  editor. `__STYLESHEET__` is the CSS-injection slot.
- `styles/plain.css`, `styles/branded.css` — layout (`plain` is default per manifest `plain:true`).
- `manifest.json` — `{document, stylesheets:{plain,branded}, plain, archetype, name}`.

  e.g. `content/rare-structure/docraptor-to-documenso-template/capital-origination/v2/global_engagement_content/`

**Tables (system of record — HQX Postgres, Doppler `core-x/prd → HQX_DB_URL_POOLED`)**

| table | role |
|---|---|
| `business.global_input_content` | content-source REGISTRY: `path` (brand-relative `<family>/<archetype>/<version>`) + `brand` + `source_kind`. FK target of `documenso_templates.global_input_content_id`. (07) |
| `business.documenso_templates` | one row per Documenso template: `documenso_template_id` (numeric-as-text), `status` (`active`/`archived`), `global_input_content_id`, `archetype_id`, and the `recipients` jsonb (below). UPSTREAM-OWNED — edge_api ALTERs only (06 B.6). |
| `business.engagement_documenso_template_mappings` | the **operator dropdown**: friendly `name`/`slug` → `documenso_template_uuid` (FK → `documenso_templates.id`), `is_visible`. Repoint this to cut a mapping over to a new template. (06 B.4) |
| `business.opportunity_specific_content` | per-deal `field_values` jsonb (label→value). The prefill data source; read-but-never-written here (07). |
| `business.documenso_webhook_events` | RAW Documenso webhook capture; sign-state is derived from it at read time (03, 07). |

**The `documenso_templates.recipients` jsonb — the per-template config edge_api reads at originate**
- `prospect_recipient_id` (int) — the Documenso recipient the prospect binds to.
  **Prefilled fields MUST live on this recipient** (see Traps).
- `default_field_values` (label→value) — template-level **OPERATOR TERMS**: prefilled and **LOCKED** (e.g. `{"amt%":"2"}`).
- `editable_field_labels` (list of label) — labels left **UNLOCKED** after prefill (the prospect's own facts, e.g. `["Full Name","Title","Legal Entity Name"]`). **Unset → lock everything prefilled** (backward-compatible default).
- `text_fields`, `fields` — informational snapshot of the live template. Originate reads the **LIVE** template, not this snapshot, so it is for record only.

---

## The prefill + lock model (the part not in 03/06/07)

At originate (`POST /api/v1/engagement-mandate-drafts/{draft}/originate-prefilled`,
`routers/engagement_mandate_drafts_v1.py`):

1. `field_values = {**default_field_values, **opportunity_specific_content.field_values}`
   — per-deal value overrides the template default.
2. `create_document_from_template` (`services/documenso_client.py`) reads the LIVE template,
   fans each value onto **every field whose `fieldMeta.label` EXACTLY matches the key**,
   mints the doc via `/api/v2/template/use` (binding `prospect_recipient_id`, overriding its
   email/name with the opportunity contact), then **locks every prefilled field EXCEPT** those
   whose label is in `editable_field_labels`.

Two keys, two behaviors — this is the whole mental model:

- **Operator term** (the firm's number, e.g. success-fee %) → `default_field_values` → prefilled + **locked**.
- **Prospect fact** (their name / title / legal entity) → `opportunity_specific_content.field_values` (per deal) **and** add the label to `editable_field_labels` → prefilled + **editable**. The prospect can correct it; their signature adopts it.

The signature itself is never prefillable — only the signer produces it, on Documenso's surface.

---

## The iteration loop

1. **Edit** the content HTML/CSS in the content dir. It is a *proportional* font, so **preview
   locally before pushing** — DocRaptor test mode is free and same-layout:
   assemble `html.replace("__STYLESHEET__", css)` → `POST https://docraptor.com/docs`
   `{test:true, document_type:"pdf", document_content:<html>, prince_options:{media:"print",javascript:false}}`
   (HTTP Basic, `DOCRAPTOR_API_KEY` as username) → read the PDF. **Do not eyeball slot widths; measure.**
2. **Merge** to core-x `main`. edge_api redeploys on Railway (content is baked into the image).
3. **Render + push** once the deploy serves the new content:
   `POST {EDGE_API_BASE_URL}/internal/engagement-templates/render-push`,
   header `Authorization: Bearer {TRIGGER_SHARED_SECRET}`, body
   `{"registryPath":"<family>/<archetype>/<version>"}`.
   → returns a **NEW** Documenso template (new numeric id), placeholder recipients, **no fields**.
   Gate this on a render-only freshness check (`POST /api/v1/engagement-templates/render`,
   `Authorization: Bearer {EDGE_API_SERVICE_TOKEN}`) so you never push stale content.
4. **Place fields** in the Documenso editor over the slots. Set each field's **label** = the
   prefill key (exact). Set **Required** as desired. Leave **Read Only UNCHECKED** — lock is
   applied at originate, not via the editor checkbox.
5. **Register** the new template row in `business.documenso_templates`: `status='active'`,
   `recipients` = `{prospect_recipient_id, default_field_values, editable_field_labels, text_fields, fields}`,
   `archetype_id`, `global_input_content_id`.
6. **Cut over**: repoint the `engagement_documenso_template_mappings` row →
   the new template's `id`; set the old template `status='archived'`.
7. **Per-deal data**: `opportunity_specific_content.field_values` keyed by the field labels.

---

## Traps (each one cost real debugging in build-out)

- **Field-label EXACT match.** `participant_full_name` ≠ `Full Name`. The #1 silent prefill miss — the value is simply dropped, field stays blank.
- **Prefilled fields must be on the participant recipient** (`prospect_recipient_id`). `/template/use` overrides only that recipient and OMITS the provider; Documenso DROPS prefill for omitted recipients, so a value on a *provider* field silently vanishes.
- **Read Only on the template ≠ editability.** Lock is applied on the derived doc at originate; control it via `editable_field_labels`, not the editor checkbox. (A template field also can't be Read Only without static text.)
- **Derived docs drop field labels** → the lock step re-identifies the editable fields by **GEOMETRY** (page + rounded x/y). Keep field positions stable between template and derived.
- **Layout:** `p { text-align: justify }` stretches short lines (e.g. the preamble's `entity, ___ d/b/a` line) edge-to-edge — `.lead { text-align: left }` fixes it. Underscore-slot lengths are render-dependent; measure with a local render.
- **Sign-state is recipient-scoped** (`/sign-state?signer=client|originator`); the operator's countersign link must poll `signer=originator` or it shows "Your signature is recorded" the moment the *prospect* signs (#725).
- **render+push reads the edge_api FILESYSTEM** (the deployed image), NOT your local edit. Always merge + let Railway redeploy before pushing.

---

## Secrets (Doppler `core-x/prd`)
`HQX_DB_URL_POOLED` (Postgres) · `DOCUMENSO_API_KEY` + `DOCUMENSO_API_URL` (templates/fields, header `Authorization: api_…`) · `DOCRAPTOR_API_KEY` (render) · `EDGE_API_SERVICE_TOKEN` (render-only, Bearer) · `TRIGGER_SHARED_SECRET` (render-push, Bearer) · `EDGE_API_BASE_URL` (`https://api.edgeapi.run`).

---

## Current state — snapshot (2026-06-26, will drift)
- Lane: **rare-structure / capital-origination / content `v2`** (`global_input_content` id `69b4e941…`, path `docraptor-to-documenso-template/capital-origination/v2`).
- Dropdown mapping **"Strategic Origination Mandate – Setup + Percentage"** (`7e25123f…`) → template **14396** (active).
- Latest render: **14416** (`envelope_cxeaumctnmmdldbn`) — new `Legal Entity d/b/a DBA` preamble, left-aligned, underscore slots (date 12 / legal 30 / dba 28). **Awaiting field placement**, then register → repoint mapping → archive 14396.
- Field labels: `Full Name`, `Title`, `Legal Entity Name`, `amt%` (+ a DBA field being added). **Editable**: Full Name, Title, Legal Entity Name. **Locked**: `amt%` (`default_field_values={"amt%":"2"}`).
