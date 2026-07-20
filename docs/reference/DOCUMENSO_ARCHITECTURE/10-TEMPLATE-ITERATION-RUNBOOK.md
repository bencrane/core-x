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
- `<doc>.html` — the agreement body. Every dynamic value is a blank **fill-slot** that a
  Documenso field is dropped over in the editor. Two distinct slot styles — **don't mix them
  in one slot**:
  - a **`.field-slot` span** — a ruled CSS line (fixed `min-width`, no glyphs); width is a
    static visual guide that does not grow with content.
  - a **literal underscore run** (`____`) — width = glyph count, so its length is
    render-dependent (measure with a local render, never eyeball).

  `__STYLESHEET__` is the CSS-injection slot.
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
- `text_fields`, `fields` — snapshot of the live template. Originate reads the **LIVE** template, not this snapshot, so it does not drive prefill/lock. But `recipients->'text_fields'` **IS read live by the engagement-picker query** (the operator dropdown's field list) — keep it populated and in sync with the template's labels.

---

## The prefill + lock model (the part not in 03/06/07)

At originate (`POST /api/v1/engagement-mandate-drafts/{draft}/originate-prefilled`,
`routers/engagement_mandate_drafts_v1.py`):

1. `field_values = {**default_field_values, **opportunity_specific_content.field_values}`
   — per-deal value overrides the template default.
2. `create_document_from_template` (`services/documenso_client.py`) reads the LIVE template and,
   for **every** field, resolves its prefill value by `fieldMeta.label`: the **exact key first,
   else the BASE name** (label minus a trailing `_segment`) — so split labels placed in multiple
   spots (`participant_company_one` / `participant_company_two`) both draw from a single
   `participant_company` value. A label sitting on several fields fans out to all of them. It then
   mints the doc via `/api/v2/template/use` (binding `prospect_recipient_id`, overriding its
   email/name with the opportunity contact), and **locks every prefilled field EXCEPT** those
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
   `{"registryPath":"docraptor-to-documenso-template/<archetype>/<version>"}`.
   The first path segment is the **literal** template-family directory
   `docraptor-to-documenso-template` (NOT the business archetype) — three segments, brand-relative.
   → returns a **NEW** Documenso template (new numeric id), placeholder recipients, **no fields**.
   Gate this on a render-only freshness check first — `POST /api/v1/engagement-templates/render`,
   `Authorization: Bearer {EDGE_API_SERVICE_TOKEN}`, body
   `{"brand":"<brand>","path":"docraptor-to-documenso-template","archetype":"<archetype>","version":"<version>"}`
   (render-only takes the segments SPLIT OUT; it has **no** `registryPath` field — that is render-push
   only). It returns a presigned PDF URL and touches **nothing** in Documenso; compare it to your local
   DocRaptor preview to confirm Railway is serving the new content before you push.
4. **Place fields** in the Documenso editor over the slots. Set each field's **label** = the
   prefill key (exact). Set **Required** as desired. Leave **Read Only UNCHECKED** — lock is
   applied at originate, not via the editor checkbox.
5. **Register** the new template row in `business.documenso_templates`: `status='active'`,
   `recipients` = `{prospect_recipient_id, default_field_values, editable_field_labels, text_fields, fields}`,
   `archetype_id`, `global_input_content_id`.
   `prospect_recipient_id` is the Documenso recipient id of the **placeholder Participant** recipient
   on the NEW template — read it from `GET /api/v2/template/{documenso_template_id}` → `recipients[]`
   (the entry whose email is the unset/placeholder one, i.e. the prospect slot, not the provider).
6. **Cut over**: repoint the `engagement_documenso_template_mappings` row →
   the new template's `id` (the uuid PK, `documenso_template_uuid`); set the old template
   `status='archived'`. **Rollback** is the inverse and safe at any time: repoint the mapping back
   to the prior template's `id` and set the prior template `status='active'`. Already-originated
   documents are independent derived envelopes — neither cut-over nor rollback touches them.
7. **Per-deal data**: write `business.opportunity_specific_content.field_values` (label→value) for
   the opportunity. This is the ONLY source originate prefills from (see Traps).
8. **Verify end-to-end** against a test opportunity before declaring the cut-over done. Hit
   `POST /api/v1/engagement-mandate-drafts/{draft}/originate-prefilled` (service-token), open the
   returned envelope in Documenso, and confirm: operator terms (e.g. `amt%`) are **prefilled and
   locked** (read-only); the `editable_field_labels` (e.g. Full Name / Title / Legal Entity Name)
   are **prefilled but still editable**; and SIGNATURE/DATE are open for the signer. A locked field
   that should be editable means its label is missing from `editable_field_labels` (lock/editable
   is matched by field LABEL on the derived document).

---

## Traps (each one cost real debugging in build-out)

- **Field labels must match the value KEY** — exact key, or the BASE name for a `_`-split field (`participant_company_one`/`_two` both draw from `participant_company`). An unrelated name like `participant_full_name` vs `Full Name` matches NEITHER and is silently dropped (the #1 prefill miss — field stays blank). Use the `_`-split form *deliberately* to place one value in multiple spots; don't rely on it for unrelated labels.
- **Originate prefills from `opportunity_specific_content.field_values` ONLY.** The mandate-draft `prefill_values` column (the prep-page staging store) does **NOT** feed `originate-prefilled` — editing the prep page changes nothing at originate unless `opportunity_specific_content` is also written. Two different tables; nothing syncs them.
- **Prefilled fields must be on the participant recipient** (`prospect_recipient_id`). `/template/use` overrides only that recipient and OMITS the provider; Documenso DROPS prefill for omitted recipients, so a value on a *provider* field silently vanishes.
- **Read Only on the template ≠ editability.** Lock is applied on the derived doc at originate; control it via `editable_field_labels`, not the editor checkbox. (A template field also can't be Read Only without static text.)
- **Lock/editable is matched by field LABEL on the derived document** (derived docs preserve `fieldMeta.label` through `/template/use`). Field positions do not affect lock behavior.
- **Layout:** `p { text-align: justify }` stretches short lines (e.g. the preamble's `entity, ___ d/b/a` line) edge-to-edge — `.lead { text-align: left }` fixes it. Underscore-slot lengths are render-dependent; measure with a local render.
- **Sign-state is recipient-scoped** (`/sign-state?signer=client|originator`); the operator's countersign link must poll `signer=originator` or it shows "Your signature is recorded" the moment the *prospect* signs (#725).
- **render+push reads the edge_api FILESYSTEM** (the deployed image), NOT your local edit. Always merge + let Railway redeploy before pushing.

---

## Secrets (Doppler `core-x/prd`)
`HQX_DB_URL_POOLED` (Postgres) · `DOCUMENSO_API_KEY` + `DOCUMENSO_API_URL` (templates/fields, header `Authorization: api_…`) · `DOCRAPTOR_API_KEY` (render) · `EDGE_API_SERVICE_TOKEN` (render-only, Bearer) · `TRIGGER_SHARED_SECRET` (render-push, Bearer) · `EDGE_API_BASE_URL` (`https://api.edgeapi.run`).

---

## Discover current state (run these — never trust a hardcoded id)

Template/mapping/field ids drift constantly; resolve them live against `HQX_DB_URL_POOLED`.

**Active template behind each visible dropdown mapping** — the template a deal originates against today,
and its wired config:
```sql
SELECT m.name                       AS dropdown_label,
       m.is_visible,
       dt.documenso_template_id     AS originate_id,   -- numeric-as-text, the originate value
       dt.id                        AS template_uuid,  -- the mapping FK / repoint target
       dt.status,
       dt.global_input_content_id,
       dt.recipients->>'prospect_recipient_id'         AS prospect_recipient_id,
       dt.recipients->'default_field_values'           AS locked_defaults,
       dt.recipients->'editable_field_labels'          AS editable_labels
  FROM business.engagement_documenso_template_mappings m
  JOIN business.documenso_templates dt ON dt.id = m.documenso_template_uuid
 WHERE m.is_visible AND m.status = 'active' AND dt.status = 'active'
 ORDER BY m.name;
```

**Most recent render-push runs** — find the freshly-pushed template id (and any push error) after step 3:
```sql
SELECT created_at, status, brand, path, archetype, version,
       documenso_template_id, documenso_numeric_id, error
  FROM ops.engagement_template_push_runs
 ORDER BY created_at DESC
 LIMIT 5;
```

**Live field labels on a template** — the source of truth for what to key prefill values by (NOT any
stored snapshot): `GET /api/v2/template/{documenso_template_id}` → `fields[].fieldMeta.label`
(TEXT/NUMBER fields; SIGNATURE/DATE carry no label).
