# 06 — Engagement Docs Lane (REMOVED) & Documenso Templates/Fields/Archetypes Layer

> **STATUS BANNER.** This file covers the **AO (Active Operators) engagement-document origination** machinery in `core-x` edge_api. It once spanned **two render lanes**, but only one survives: (1) the `engagement_docs` subsystem — an older static-HTML → DocRaptor PDF → **DRAFT** two-signer Documenso DOCUMENT pathway — is **REMOVED from current main** (its DDL, module, both routers, Trigger task, and content subtree were pruned wholesale by commit `47e1815`, #531; `git grep` zero matches; superseded by the render+push lane — see Part A); (2) the **Documenso TEMPLATES / fields / defaults / archetypes / mappings** layer plus the successor `engagement_templates` surface (TWO routes: a DocRaptor-only PDF render AND a render+push that CREATES a Documenso TEMPLATE) is **LIVE** (Part B). It pertains to the AO term-only and term+success-fee economic shapes. It does **not** cover the `direct-to-documenso` prefill payment lane (see `05-DIRECT_TO_DOCUMENSO_PAYMENT_E2E`) nor the `engagement-mandate-drafts` staging-draft signing lane.

## Orientation

A fresh agent should hold two distinct mental models. **First**, the `engagement_docs` lane (`apps/edge_api/src/engagement_docs/`) — an older self-contained pathway fired by an operator "Stage" click on a `rare-structure-hq` Applications row (Trigger.dev `engagement-doc-render` → bind opportunity company/signer values + a server-resolved price/term package into repo-resident static AO term-only HTML → DocRaptor PDF → segregated R2 → a DRAFT two-signer Documenso DOCUMENT; ledger `business.ao_engagement_mandates`) — **has been REMOVED from current main** (pruned wholesale by `47e1815`, #531; `git grep ao_engagement_mandates` / `engagement_docs` return zero source matches). Do NOT chase it; see Part A for the removal record and what it was.

**Second**, the Documenso **templates layer** governs how edge_api reads/writes/classifies live Documenso v2 TEMPLATE envelopes and the per-operator content that feeds them: a Settings defaults editor that writes default field values onto a live template; a Dossier engagement picker scoped by operator email-domain; the brand-aware `engagement_templates` surface — a service-token DocRaptor-to-PDF render (NO Documenso) AND a trigger-secret render+push that CREATES a Documenso TEMPLATE (Trigger.dev `engagement-template-push`), keyed off the `business.global_input_content` content-source registry; the `business.engagement_archetypes` economic-shape classifier above `business.documenso_templates`; and a one-shot push script that CREATES the two AO agreement templates in Documenso. These surfaces ARE active in the live flow.

All HTTP surfaces here are **service-token gated** and brokered by the `rare-structure-hq` platform-api BFF (a dumb forwarder across the auth boundary).

---

## Part A — The `engagement_docs` Subsystem (REMOVED)

> **REMOVED from current main.** The entire AO `engagement_docs` "Stage" render lane was pruned wholesale by `refactor(edge_api): prune dead/broken Documenso originate paths` (commit `47e1815`, #531). `git grep ao_engagement_mandates` and `git grep engagement_docs` over current main return **zero source matches** — nothing creates, writes, reads, renders, or routes this lane. Do NOT document it as a live (or "broken-at-HEAD") subsystem. Superseded by the `engagement_templates` render+push lane (`apps/edge_api/src/routers/internal_engagement_templates_v1.py:84`; Part B.7).

The prune deleted, in one commit:

- the DDL `apps/edge_api/sql/ao_engagement_mandates.sql` — the ledger table `business.ao_engagement_mandates`;
- the whole module `apps/edge_api/src/engagement_docs/` — `__init__.py`, `service.py`, `render.py`, `documenso.py`, `queries.py`, `kickoff.py`, `packages.py`, `models.py`, `store.py`;
- the operator router `apps/edge_api/src/routers/engagement_mandates_v1.py` (prefix `/api/v1/engagement-mandates`);
- the internal render route `apps/edge_api/src/routers/internal_engagement_docs_v1.py` (`POST /internal/engagement-doc/render`);
- the monorepo-root Trigger.dev task `src/trigger/engagement_doc_render.ts` (`id: "engagement-doc-render"`);
- its repo-resident content subtree `apps/edge_api/content/active-operators/docraptor-to-documenso-document-only/global_engagement_content/` (manifest, HTML, `styles/`).

**What it was (historical — for archaeology only, not present in any source).** An operator "Stage" click on a `rare-structure-hq` Applications row enqueued the `engagement-doc-render` Trigger.dev task; edge_api bound an opportunity's company/signer values plus a server-resolved price/term package into repo-resident static AO term-only HTML, DocRaptor rendered a plain PDF (LIVE), the bytes landed under a per-deal R2 key, and a **DRAFT** (never-distributed) two-signer Documenso DOCUMENT was created with SIGNATURE/DATE fields placed by `[[anchor]]` `findText`. The ledger `business.ao_engagement_mandates` held one row per deal (many per opportunity), and the document's `externalId` was the OPPORTUNITY id. At its removal the lane was already non-functional at HEAD — a `service.SLUG` `AttributeError` killed the POST before INSERT, and `render.py` read a content directory relocated out from under it by #494 — which is why it was **pruned rather than repaired**. The live successor is the `engagement_templates` lane (Part B): a DocRaptor-only render plus a render+push that CREATES a Documenso TEMPLATE.

---

## Part B — Documenso Templates / Fields / Defaults / Archetypes / Mappings (ACTIVE)

### B.1 Settings defaults editor — `documenso_template_fields_v1.py`

Two service-token-gated routes that read/write **default values directly onto a live Documenso template's fields**:

| Method / path | Behavior | Citation |
|---|---|---|
| `GET /api/v1/documenso-template-fields?documenso_template_id=…` | Returns the template's editable fields + current defaults. | `documenso_template_fields_v1.py:40`, `:41`, `:44` |
| `POST /api/v1/documenso-template-fields/defaults` | Writes defaults onto fields, returns refreshed list. | `documenso_template_fields_v1.py:50`, `:55`, `:56` |

Request body `SetDefaultsRequest {documenso_template_id: str, defaults: list[FieldDefault]}` where `FieldDefault {id: int, value: str}` (`documenso_template_fields_v1.py:30`-`37`). `DocumensoError` → HTTP **502** (detail prefixed `"documenso: "`) on both routes (`:45`-`46`, `:57`-`58`). Response model `TemplateField` carries `id:int`, `type:str` (**TEXT | NUMBER | DROPDOWN** — the default-able types), `label`, `recipient_id`, `page`, `default` (`:21`-`27`).

> Setting a default **modifies the actual Documenso TEMPLATE**: future documents instantiated via `/envelope/use` inherit it; already-created documents are untouched (router docstring `:6`-`7`; client docstring `documenso_client.py:716`, `:720`-`721`). *(This is a documented behavioral assertion in docstrings — not independently confirmed against Documenso's live API.)*

### B.2 The field-read/write client functions — `documenso_client.py`

- `get_template_fields()` (`documenso_client.py:689`): resolves the numeric template id → envelope id (`:696`), GETs `/api/v2/envelope/{envelope_id}` (`:697`), returns only fields whose type is in `_DEFAULT_META_KEY` i.e. **TEXT/NUMBER/DROPDOWN** (`:700`) — SIGNATURE/DATE excluded — each as `{id, type, label, recipient_id, page, default}` (`:702`-`711`).
- `_DEFAULT_META_KEY = {"TEXT": "text", "NUMBER": "value", "DROPDOWN": "defaultValue"}` (`documenso_client.py:677`) — the per-type fieldMeta key holding the default.
- `set_template_field_defaults()` (`documenso_client.py:715`): resolve envelope (`:727`) → read existing fields (`:728`) → **MERGE** each new value into the field's existing `fieldMeta` (so label/type survive: `meta = dict(f.get("fieldMeta") or {}); meta[key] = value`, `:738`-`739`) → POST the **FULL** field record (id, type, recipientId, page, positionX/Y, width, height, merged fieldMeta) to `/api/v2/envelope/field/update-many` (`:740`-`756`) so no property is dropped → returns `len(data)` (`:759`).

### B.3 The envelope-id resolver — `_resolve_template_envelope_id`

`business.documenso_templates` stores only the **numeric** template id; the v2 envelope endpoints 400 on a bare numeric id, so it must be resolved live: `GET /api/v2/template/{id}` (`documenso_client.py:311`).

> **CODE-WINS DISCREPANCY (carried from verification).** The docstring at `documenso_client.py:309` says the resolver returns `.envelopeId`, but the code at `:313` actually does `_dig(resp.json(), "envelopeId", "id")` — it digs `envelopeId` first, **falling back to `id`**. The primary key is `envelopeId`; the docstring's "→ .envelopeId" is imprecise.

Used by exactly four callers: `create_document_from_template_with_custom_pdf` (`:355`), `get_template_text_field_labels` (`:661`), `get_template_fields` (`:696`), `set_template_field_defaults` (`:727`).

### B.4 Dossier engagement picker — `engagement_mappings_v1.py` + `engagement_mappings/`

`GET /api/v1/engagement-mappings?org_domain=…` returns `list[EngagementMappingOption]` for one operator org; service-token gated (`engagement_mappings_v1.py:29`, `:30`).

The SQL (`engagement_mappings/queries.py:18`-`33`) lists rows from `business.engagement_documenso_template_mappings m` where `m.is_visible = true AND m.status = 'active' AND lower(o.metadata->>'domain') = lower(%s)`:

```sql
SELECT dt.documenso_template_id AS id,          -- the numeric Documenso template id (origination uses this)
       m.name                   AS label,
       a.key                    AS archetype_key,
       a.name                   AS archetype_name,
       a.performance_fee_basis  AS performance_fee_basis,
       COALESCE(dt.recipients->'text_fields', '[]'::jsonb) AS text_fields  -- stored FALLBACK
  FROM business.engagement_documenso_template_mappings m
  JOIN business.documenso_templates dt       ON dt.id = m.documenso_template_uuid  -- FK -> surrogate PK
  JOIN business.organizations o              ON o.id = m.organization_id
  LEFT JOIN business.engagement_archetypes a ON a.id = dt.archetype_id
 WHERE m.is_visible = true AND m.status = 'active'
   AND lower(o.metadata->>'domain') = lower(%s)
 ORDER BY a.name NULLS LAST, m.name
```

`queries.py:19`-`32`. An **empty `org_domain` short-circuits to `[]`** (`if not org_domain: return []`, `queries.py:38`).

> **DO-NOT-CONFLATE the two id columns.** The FK `m.documenso_template_uuid` joins to `dt.id` (the **surrogate PK**), but the returned option `id` is `dt.documenso_template_id` (the **numeric Documenso template id** — the value origination uses). The mapping FK was renamed from `documenso_template_id` (uuid) to `documenso_template_uuid` precisely to kill this collision. `queries.py:19`, `:26`.

`EngagementMappingOption` fields: `id`, `label` (=`m.name`), `archetype_key`, `archetype_name`, `performance_fee_basis`, `text_fields` (`engagement_mappings/models.py:13`-`23`; `queries.py:20`-`24`, `:28`).

**LIVE text_fields override:** after the SQL list, the router overrides each option's `text_fields` with `documenso_client.get_template_text_field_labels(opt.id)` (`engagement_mappings_v1.py:37`), fanned out concurrently via `asyncio.gather` (`:41`). A per-option `DocumensoError` is **swallowed** (`except documenso_client.DocumensoError: pass`, `:38`-`39`), leaving the SQL stored fallback (`COALESCE(dt.recipients->'text_fields','[]')`) intact. `get_template_text_field_labels()` returns each TEXT field's `fieldMeta.label` (stripped, de-duplicated, in field order); SIGNATURE/DATE excluded (`documenso_client.py:652`, `:665`-`671`).

### B.5 The archetype classifier — `business.engagement_archetypes`

An archetype is the **economic SHAPE** of an engagement (which pricing variables exist and how they combine) — a TYPE, not a value (`engagement_archetypes.sql:5`-`7`). Hierarchy: `engagement_archetypes (1) ──< documenso_templates (N) ──< engagement_documenso_template_mappings` (`:11`); each `documenso_template` belongs to exactly one archetype (`:8`).

Table DDL (`engagement_archetypes.sql:19`), applied to `HQX_DB_URL_POOLED`, idempotent (`:3`):

| Column | Type / constraint | Citation |
|---|---|---|
| `id` | `uuid PRIMARY KEY DEFAULT gen_random_uuid()` | `:21` |
| `key` | `text NOT NULL UNIQUE` (stable code selector) | `:22` |
| `name` | `text NOT NULL` | `:23` |
| `description` | `text` | `:24` |
| `performance_fee_basis` | `text CHECK IN ('greater_of','lesser_of','sum','percentage_only','flat_only')` | `:27` |
| `created_at` / `updated_at` | `timestamptz NOT NULL DEFAULT now()` | `:29`-`30` |

Seeded with **exactly two** live archetypes via `INSERT ... ON CONFLICT (key) DO NOTHING` (`:34`-`44`):

| key | performance_fee_basis | meaning | Citation |
|---|---|---|---|
| `term_only` | `NULL` | fixed term/retainer, no per-deal performance fee | `:36`-`39` |
| `term_plus_greater_of` | `greater_of` | a term plus a per-deal fee = greater of a percentage or a flat amount | `:40`-`43` |

**ALTER-only on the upstream `documenso_templates`:** `ADD COLUMN IF NOT EXISTS archetype_id uuid` (`:49`); a guarded `DO $$` block adds the `ON DELETE RESTRICT` FK `documenso_templates_archetype_id_fkey` (Postgres lacks `ADD CONSTRAINT IF NOT EXISTS`) (`:50`-`62`); `documenso_templates_archetype_idx` on `(archetype_id)` (`:63`-`64`). **Data-driven backfill** (`:69`-`78`): a template whose `recipients->'text_fields'` overlaps `array['percentage_deal_fee','flat_deal_fee_amount','term_fee']` (the `?|` any-of operator, `:73`) is set to `term_plus_greater_of`, else `term_only`; idempotent (`WHERE dt.archetype_id IS NULL`, `:78`), no hardcoded ids.

### B.6 `business.documenso_templates` & mappings are UPSTREAM-OWNED

There is **no `CREATE TABLE`** for `business.documenso_templates` or `business.engagement_documenso_template_mappings` anywhere in the repo (independently re-verified: `grep -rniE 'create table[^;]*documenso_templates|create table[^;]*engagement_documenso'` returns nothing). edge_api only ALTERs `documenso_templates` (adds `archetype_id`) and SELECTs from both. The SQL itself states "the table predates this file and is not defined here" (`engagement_archetypes.sql:46`). Columns observed in use: `documenso_template_id` (numeric id as text — the originate value), `id` (surrogate PK), `organization_id`, `archetype_id`, `source_config_id` (referenced only in comment/docstring; its column type was not read from a CREATE TABLE), `recipients` (jsonb → `'text_fields'`). Also: `business.engagement_mandate_draft_content` denormalizes `archetype_id` from `documenso_templates` via the same ALTER+FK+index+backfill pattern, matching on the text numeric template id (`engagement_mandate_draft_content.sql:25`, `:35`-`36`, `:43`, `:48`-`51`).

### B.7 The `engagement_templates` lane — TWO distinct routes

`engagement_templates` exposes **two routes that must not be conflated**: a DocRaptor-only PDF render (service-token, operator-facing, NO Documenso) and a render+PUSH that CREATES a live Documenso TEMPLATE (trigger-secret, Trigger.dev-facing).

**(1) DocRaptor-only render — `engagement_templates_v1.py` (service-token).** `GET /api/v1/engagement-templates` lists selectable templates; `POST /api/v1/engagement-templates/render` renders one to a PDF (plain style by default) and returns a **presigned R2 URL** with a 3600s TTL (`_PDF_TTL_SECONDS = 3600`, `engagement_templates_v1.py:27`). Both service-token gated (`:30`, `:47`). This surface **explicitly does NOT touch Documenso** (`:6`, `:49`-`50`) — the operator gets a clean PDF link and affixes Documenso fields by hand afterward.

**(2) Render+PUSH — `internal_engagement_templates_v1.py` (trigger-secret).** `POST /internal/engagement-templates/render-push` (prefix `/engagement-templates` under `/internal`, gated by `require_trigger_secret`, `internal_engagement_templates_v1.py:24`, `:28`, `:84`) renders the content source via DocRaptor **and CREATES a Documenso TEMPLATE** from the bytes, recording a terminal row in `ops.engagement_template_push_runs`. It is called by the Trigger.dev task `engagement-template-push` (`src/trigger/engagement_template_push.ts`, `id: "engagement-template-push"`, `maxDuration: 300`, `retry: { maxAttempts: 1 }`) via `callHqx`. Both routers registered in `main.py` (`apps/edge_api/main.py:51` / `:267` render; `:52` / `:274` render-push under `prefix="/internal"`). **Distinct lanes: render = PDF-only for operator consumption; render-push = template creation for live Documenso.**

The render-push request resolves its content descriptor two ways (`internal_engagement_templates_v1.py:45`-`81`): pass a `registryPath`/`registryId` to look up a `business.global_input_content` row (brand + source_kind + brand-relative path; `engagement_templates/registry.py`), OR pass explicit `brand`/`path`/`archetype`/`version`. Either way `push.split_registry_path()` extracts the `(path, archetype, version)` triple from the registry path string (`push.py:52`-`60`) and `push.render_and_push()` resolves the content, renders LIVE DocRaptor, optionally stores an R2 audit copy, and calls `documenso_client.create_template_from_pdf` (`push.py:63`-`130`). Only `source_kind='repo-html'` is wired; `'db-markdown'` raises `PushError` (`push.py:81`-`84`).

The catalog (`engagement_templates/catalog.py`) discovers any `<brand>/<path>/<archetype>/<version>/global_engagement_content/manifest.json` under `_CONTENT_ROOT = parents[2] / "content"` (`catalog.py:20`), gated by `_ALLOWED_BRANDS = frozenset({"active-operators", "rare-structure"})` (`catalog.py:28`) — `brand` selects the content-root subtree and defaults to `active-operators` so the original three-segment call sites keep working (`catalog.py:99`). Selection segments (brand+path+archetype+version) are validated against `_SAFE_SEG = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")` (`catalog.py:32`, `:102`-`104`) AND the resolved dir is confirmed inside the content root (`if _CONTENT_ROOT not in content_dir.parents: raise`, `catalog.py:109`) — path-traversal defense-in-depth.

Render assembly does **NO token substitution** (the HTML body carries no `{{tokens}}` — every dynamic value is reserved blank space the operator affixes as Documenso fields later); `assemble_html` only injects the chosen stylesheet into the `__STYLESHEET__` slot (`engagement_templates/render.py:1`, `:4`-`5`, `:70`). DocRaptor renders LIVE `"test": False` (`render.py:79`-`80`).

Rendered PDFs are stored under the segregated `engagement_templates/store.py` PREFIX `"engagement-templates/"` (`store.py:18`, kept out of the data-lake SoR `active/` namespace `:17`); edge_api never proxies bytes (browser opens the presigned URL directly). Error mapping in the router:

| Error | HTTP | Citation |
|---|---|---|
| `CatalogError` (unknown template) | 404 | `engagement_templates_v1.py:52`-`53` |
| `StyleError` (bad style) | 400 | `:58`-`59` |
| `RenderError` (assemble) | 502 | `:60`-`61` |
| `RenderConfigError` (missing `DOCRAPTOR_API_KEY`) | 503 | `:66`-`67` |
| `RenderError` (render_pdf transient) | 502 | `:68`-`69` |
| `StoreConfigError` (R2 unconfigured) | 503 | `:75`-`76` |
| `StoreError` (put/sign failure) | 502 | `:77`-`78` |

### B.8 The wired content lanes + their manifests

**TWO** content lanes are selectable by the `engagement_templates` catalog, one per brand subtree (both at the required `<brand>/<path>/<archetype>/<version>/global_engagement_content/manifest.json` depth):

| Brand | Catalog path | Archetype | Manifest evidence |
|---|---|---|---|
| `active-operators` | `docraptor-to-documenso-template/term-only/v1` | `term_only` | seeded `global_input_content.sql:54` |
| `rare-structure` | `docraptor-to-documenso-template/capital-origination/v1` | `capital_origination` | `…/capital-origination/v1/global_engagement_content/manifest.json` |

The rare-structure capital-origination manifest declares slug `rare_structure_strategic_origination`, name "Rare Structure — Strategic Origination Agreement (Capital Origination)", archetype `capital_origination`, document `rare_structure_strategic_origination.html`, stylesheets `{plain, branded}`, `plain: true` (`content/rare-structure/docraptor-to-documenso-template/capital-origination/v1/global_engagement_content/manifest.json`). Both lanes are registered in `business.global_input_content` (`global_input_content.sql:53`-`56`).

The **document-only** content subtree that fed the now-removed `engagement_docs` lane (`apps/edge_api/content/active-operators/docraptor-to-documenso-document-only/global_engagement_content/`) was **deleted with that lane** (`47e1815`, #531) and is no longer on disk — only the two `docraptor-to-documenso-template` subtrees in the table above remain selectable by the `engagement_templates` catalog.

#### B.8.1 The content-source registry — `business.global_input_content`

The render+push lane (B.7) resolves WHERE to pull content from a registry table, NOT from `documenso_templates`. `business.global_input_content` is one row per content asset: `{id, path, name, status, created_at, updated_at}` provisioned upstream, plus the guarded-ALTER source-selection columns `brand` (`'active-operators' | 'rare-structure'`) and `source_kind` (`'repo-html' | 'db-markdown'`, CHECK-constrained) (`global_input_content.sql:21`-`47`). **It does NOT carry `archetype_id`** — archetype is implicit in the `path` string (`<family>/<archetype>/<version>`, brand-relative), which `push.split_registry_path()` parses back into segments (`global_input_content.sql:14`-`16`, `:23`; `push.py:52`-`60`). `source_kind` selects HOW to resolve the row: `repo-html` reads `content/<brand>/<path>/global_engagement_content`; `db-markdown` (documented extension point, not yet wired) would read `business.global_engagement_content` by slug. Seeds the AO term-only and RS capital-origination repo-html assets via `ON CONFLICT (path) DO NOTHING` (`global_input_content.sql:52`-`56`).

### B.9 One-shot template-push script — `scripts/documenso_push_templates.py`

A one-shot operator-run script (repo-root `scripts/`, not `apps/edge_api/scripts/`) that **CREATES the two AO agreement TEMPLATES** in Documenso via the v2 API, one per archetype (`scripts/documenso_push_templates.py:2`, `:38`). Run via `doppler run --project core-x --config prd -- python3 scripts/documenso_push_templates.py` (`:19`); CREATES REAL TEMPLATES (`:21`). Titles (`:39`-`42`):

| Archetype | Title |
|---|---|
| `term_only` | `AO Strategic Origination Agreement — Term Only` |
| `term_plus_greater_of` | `AO Strategic Origination Agreement — Term + Success Fee` |

Per archetype: `build_html` (reused from `render_ao_preview`, `:35`) → DocRaptor PDF (`test: False` live, `:60`) → `POST /api/v2/envelope/create` (`type=TEMPLATE`, recipients `[PARTICIPANT, PROVIDER]`, PDF as multipart `files`, `:112`-`116`) → read recipient ids via `GET /api/v2/envelope/{id}` (`:124`) → `POST /api/v2/envelope/field/create-many` placing every `[[ANCHOR]]` and `{{token}}` by placeholder via `findText` (`matchAll`, no coordinates, `:140`). Field routing (`:76`-`89`): placeholders containing `PARTICIPANT` or starting with `{{participant_` go to the Participant recipient, else Provider; `[[...]]` anchors become SIGNATURE (if `SIGNATURE` present) or DATE; `{{token}}` becomes a TEXT field with `fieldMeta {type:'text', label: titleized token, readOnly: True}` (every token is operator-prefilled at `/template/use`, never signer-typed). Recipients are PLACEHOLDERS overridden per-deal: PROVIDER = `Benjamin J. Crane` / `benjaminjcrane@gmail.com` (`:44`), PARTICIPANT = placeholder `participant@example.com` (`:45`). Field box SIZE (percent of page): SIGNATURE 30×7, DATE 20×4, TEXT 16×3.6 (`:49`-`51`).

### B.10 Distinct-lane callout

The BFF route `engagement-mandates-admin.ts` (in the SEPARATE `rare-structure-hq` repo) historically distinguished the `engagement_docs` lane from `engagement-mandate-drafts` ("Distinct from engagement-mandate-drafts (staging draft → Documenso)", `rare-structure-hq:apps/platform-api/src/routes/engagement-mandates-admin.ts:8`); the edge_api `engagement_docs` lane it forwarded to is now **REMOVED** (`47e1815`, #531), so any surviving cross-repo BFF/SPA wiring points at a deleted backend until reconciled there. The live edge_api lanes are: the `engagement_templates` render + render-push paths (correct brand-aware `content/` root, `catalog.py:20`/`:28`; registered `apps/edge_api/main.py:51`/`:267` and `:52`/`:274`), the `engagement-mandate-drafts` Documenso template-use lane (`main.py:49`/`:251`), and the `engagement-mappings` picker (`main.py:247`).

### B.11 Embed-template lane — direct-link origination (PARALLEL to prefill)

A THIRD `direct-to-documenso` sub-lane sits alongside `prefill-document-from-template` under `render_mode='direct-to-documenso'`: `direct_to_documenso_lane='embed-template'` (`operator_settings.sql:81`-`89`; the lane CHECK now accepts `{envelope-distribute` (RETIRED), `prefill-document-from-template` (DEFAULT), `embed-template}`). It originates via `POST /api/v1/engagement-mandate-drafts/{draft_id}/originate-embed-template` (`engagement_mandate_drafts_v1.py:168`-`221`, service-token gated `:170`), **PARALLEL to** `originate-prefilled` (which is left untouched, `:175`).

**No document is minted here.** The endpoint enables a Documenso DIRECT LINK on the draft's template and returns its reusable token; the signer **self-identifies** in the embed (name/email NOT locked) and Documenso creates the document AT completion (source `TEMPLATE_DIRECT_LINK`, `:179`). Control flow (`:186`-`221`): load draft + opportunity ref/contact → `documenso_client.get_template_recipients(template_id)` (`:196`) → pick the direct recipient (`body.direct_recipient_id` or `_pick_direct_recipient_id`, `:197`; the helper at `:43`) → `documenso_client.create_direct_link(template_id, direct_recipient_id=…)` (`:198`-`200`) → 502 if no token (`:203`-`204`).

Response model `MandateEmbedTemplateOriginated` (`engagement_mandate_drafts/models.py:49`-`70`):

| Field | Meaning | Citation |
|---|---|---|
| `direct_token` | the reusable direct-template token (`EmbedDirectTemplate` prop / `/d/{token}` / iframe `/embed/direct/{token}`) | `models.py:62` |
| `documenso_host` | Documenso API base | `models.py:63` |
| `embed_url` | `f"{host}/embed/direct/{token}"` | `models.py:64`; `engagement_mandate_drafts_v1.py:214` |
| `external_id` | opportunity's PUBLIC 8-char handle (stamped by the embed) | `models.py:65` |
| `opportunity_id` | same 8-char handle | `models.py:66` |
| `direct_recipient_id` | the template recipient the public signer assumes | `models.py:67` |
| `recipient_email` / `recipient_name` | optional embed prefill (signer may change) | `models.py:68`-`69` |
| `status` | `"ready"` — no document exists until someone signs | `models.py:70` |

The Documenso v2 surface (`documenso_client.py`):

- `get_template_recipients(documenso_template_id)` — `GET /api/v2/template/{id}`, returns recipients (id/email/name/role) to designate the direct-link recipient (`:504`-`511`).
- `create_direct_link(documenso_template_id, *, direct_recipient_id=None)` — `POST /api/v2/template/direct/create {templateId, directRecipientId?}`; **idempotent**: an already-enabled link 4xxes, so it falls back to `/template/direct/toggle {enabled:true}` to recover the existing token (`:514`-`539`).
- `toggle_direct_link(documenso_template_id, *, enabled)` — `POST /api/v2/template/direct/toggle` (`:542`-`551`).
- `DirectLinkResult` dataclass `{token, enabled, direct_template_recipient_id, envelope_id, template_id}` (`documenso_client.py:468`-`477`).

The numeric template id `/template/direct/*` requires is extracted by `_template_id_number` (DB stores the numeric id as text; tolerates a prefixed handle, `:480`-`489`). The embed-document path (`create_document_from_template`) is **unchanged** — it binds a specific recipient and mints a document NOW, vs. the direct-link path where Documenso creates the document at signer completion (`documenso_client.py:460`-`465`).

> **Cross-repo (rare-structure-hq, SEPARATE repo).** The SPA mounts `<EmbedDirectTemplate token={direct_token} host={documenso_host} externalId={external_id}>` on a `DirectTemplateSignPage` at route `/p/t/:opportunityId/:directToken?host=`; the signer self-identifies. Not verifiable from this repo — corrected only against the `MandateEmbedTemplateOriginated` contract above.

---

## Status: ACTIVE / CONDITIONAL / DEPRECATED / REMOVED / STUB

| Component | Status | Note |
|---|---|---|
| **AO `engagement_docs` lane** — `engagement-mandates` routes (`/packages`, POST, GET), `POST /internal/engagement-doc/render`, `engagement-doc-render` Trigger task, `service.render_mandate` / `service.SLUG`, `packages.PACKAGES`, `business.ao_engagement_mandates` (+ queries), `participant`/`provider_signing_token` columns | **REMOVED** | Entire lane pruned wholesale — DDL, module, both routers, Trigger task, and content subtree all deleted (`47e1815`, #531; `git grep` zero matches). Superseded by render+push (`internal_engagement_templates_v1.py:84`). |
| `GET /api/v1/documenso-template-fields` + `POST .../defaults` | **ACTIVE** | Settings defaults editor; live template is source of truth. |
| `GET /api/v1/engagement-mappings` | **ACTIVE** | Dossier picker; org-domain scoped; live text_fields override. |
| `GET /api/v1/engagement-templates` + `POST .../render` | **ACTIVE** | Standalone DocRaptor→PDF; NO Documenso; service-token. |
| `POST /internal/engagement-templates/render-push` | **ACTIVE** | DocRaptor PDF → CREATE Documenso TEMPLATE; trigger-secret; called by `engagement-template-push` task. |
| `engagement-template-push` Trigger.dev task | **ACTIVE** | Calls render-push; `maxAttempts: 1` (template create is billable). |
| `ops.engagement_template_push_runs` ledger | **ACTIVE** | One row per render+push (success\|error); fire-and-forget. |
| `POST /api/v1/engagement-mandate-drafts/{draft_id}/originate-embed-template` | **ACTIVE** | Direct-link embed lane; PARALLEL to originate-prefilled; mints NO document. |
| `business.global_input_content` | **ACTIVE (registry)** | Content-source registry (brand + source_kind); no `archetype_id` (implicit in path). |
| `business.engagement_archetypes` (+ ALTER/backfill) | **ACTIVE** | Classifier above `documenso_templates`; 2 seed rows. |
| `business.documenso_templates` / `..._mappings` | **ACTIVE (upstream-owned)** | No `CREATE TABLE` here; only ALTER+SELECT. |
| `scripts/documenso_push_templates.py` | **ACTIVE (one-shot, operator-run)** | Not an HTTP route; CREATES real templates. |
| `docraptor-to-documenso-template/term-only/v1` content lane | **ACTIVE** | AO Term Only; capital-origination/v1 (rare-structure) also active. |
| `docraptor-to-documenso-template/capital-origination/v1` content lane | **ACTIVE** | Rare Structure Capital Origination; seeded in `global_input_content`. |

---

## Traps

1. **`engagement_docs` ≠ `engagement_templates` ≠ `engagement-mandate-drafts`.** Separate lanes with overlapping names. `engagement_docs` (this file, Part A) WAS the static-HTML→DocRaptor→**DRAFT Documenso DOCUMENT** lane — now **REMOVED** (`47e1815`, #531). `engagement_templates` (Part B.7) has **TWO routes**: `/render` (DocRaptor-only PDF, NO Documenso) and `/render-push` (DocRaptor PDF → CREATE Documenso TEMPLATE) — do not assume the family "never touches Documenso." `engagement-mandate-drafts` is a separate Documenso **template-use** staging lane carrying two PARALLEL originate paths: `originate-prefilled` (mints a document now) and `originate-embed-template` (direct link, document created at signer completion; Part B.11).
2. **The `engagement_docs` lane is GONE, not merely broken.** Earlier revisions of this doc described it as "BROKEN at HEAD" by two bugs (`service.SLUG` undefined; `render.py` reading a vacated `content/global_engagement_content`) and flagged stale docstrings and a stale `ao_engagement_mandates.sql` distribution comment. All of that code was **deleted** by `47e1815` (#531), not repaired — do not look for `apps/edge_api/src/engagement_docs/`, `engagement_mandates_v1.py`, `internal_engagement_docs_v1.py`, `ao_engagement_mandates.sql`, or `src/trigger/engagement_doc_render.ts`; `git grep` returns zero.
3. **Two id columns on `documenso_templates`.** The mapping FK joins `dt.id` (surrogate PK); origination uses `dt.documenso_template_id` (numeric Documenso id, stored as text). The returned option `id` is the **numeric** one (`queries.py:19`). Mixing them silently breaks origination.
4. **`_resolve_template_envelope_id` docstring is imprecise.** It says `→ .envelopeId` but the code digs `envelopeId` OR `id` (`documenso_client.py:313`). The numeric DB id 400s on envelope endpoints, so this live resolution step is mandatory.
5. **`documenso_templates` and `engagement_documenso_template_mappings` are UPSTREAM-owned.** There is no `CREATE TABLE` for them in this repo. Do not add one; only ALTER/SELECT. Some columns (e.g. `source_config_id`) appear only in comments/docstrings and were not read from a CREATE TABLE.
6. **The live Trigger.dev task lives at monorepo-root `src/trigger/`, not `apps/edge_api/src/trigger/`.** The render+push task is `src/trigger/engagement_template_push.ts` (`engagement-template-push`); looking for it under edge_api will fail. (The removed `engagement_docs` lane's `engagement_doc_render.ts` lived at the same root before the prune.)
7. **Em-dash vs hyphen in titles.** Template titles use `—` (U+2014), e.g. the push-script titles `AO Strategic Origination Agreement — Term Only` / `— Term + Success Fee` (`scripts/documenso_push_templates.py:39`-`42`). String-matching with ASCII `-` will miss.
