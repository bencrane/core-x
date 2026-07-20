# 11 — Envelope Mirror & Template→Document Prefill Config (architecture + handoff)

> STATUS — **ACTIVE**. This is the reference for the **Documenso envelope MIRROR** and the
> operator-authored **template→document PREFILL CONFIG** — the two new primitives that let a
> template be instantiated into a signable document with its fields prefilled (operator
> defaults, some locked read-only; prospect facts editable; deal-bound values in Phase 2).
> Architecture depth on the surrounding system lives in `03` (direct-to-documenso flow),
> `04` (Documenso integration), `06` (templates layer), `07` (data stores), `10` (template
> iteration runbook). This doc adds what those predate: `business.documenso_envelopes` (the
> event-driven mirror), `business.documenso_template_document_prefill_configs` (the operator
> config), the async projector, the on-demand re-grab, and the operator UI for both.
> **Point a fresh agent at `10` for content iteration; point them here for the mirror+prefill
> machinery.**

---

## Orientation — what this system does and why

When a Documenso template is instantiated into a signable document for a deal, the document's
fields must be **prefilled**: operator-set defaults (some **locked** read-only), prospect facts
(left **editable**), and — Phase 2 — values bound from deal data. Delivering that needs three
things, all built this cycle:

1. A reliable, event-driven **MIRROR** of Documenso template/document state, queryable in our
   own Postgres (`business.documenso_envelopes`) so the editor and the future resolver never
   have to round-trip Documenso to know a template's fields.
2. An operator-authored **PREFILL CONFIG** (`business.documenso_template_document_prefill_configs`)
   that stores, per field label, the **default value** and a **read-only** flag (Phase 2 adds a
   `source` binding key).
3. The **operator UI** for both — a mirror inspector with on-demand re-grab, and a per-field
   default + read-only editor.

What is **NOT** built yet: the originate-time **resolver** that actually consumes the config to
prefill+lock a document. The config is fully settable; nothing reads it at originate. See
**OPEN WORK** — that is the next agent's primary job.

The hard design choice is **model B (resolve-at-read, no copy-on-attach)**: the config holds the
default; `business.deal_details.field_values` holds per-deal overrides; at originate the value is
resolved `override ?? default` so there is no baked copy to drift. Detailed in
[The prefill model (B)](#the-prefill-model-b--resolution-precedence).

---

## Repos & deploy

| repo | path | role |
|---|---|---|
| **core-x** | `/Users/benjamincrane/core-x` | data/compute plane. `apps/edge_api` (FastAPI) owns the Documenso integration, the mirror, the projector, and the prefill-config API. HQX Postgres (Doppler `core-x/prd` → `HQX_DB_URL_POOLED`), schema `business`, is the operational SoR. Deploys on **Railway** (auto-redeploy on merge to `main`). |
| **rare-structure-hq** | `/Users/benjamincrane/rare-structure-hq` | operator app. `apps/platform-app` (React SPA) + `apps/platform-api` (Hono BFF). Consumes edge_api via `EDGE_API_SERVICE_TOKEN`; `EDGE_API_BASE_URL = https://api.edgeapi.run`. |

**Migration mechanism (core-x).** edge_api has **no migration framework**. `apps/edge_api/sql/*.sql`
is the committed DDL; `src/migrate.py` re-applies the whole `sql/` directory on every boot.
Verified mechanics (`apps/edge_api/src/migrate.py:1-60`): files are auto-discovered by **sorted
glob** (filename ascending, `SQL_DIR = …/apps/edge_api/sql`), each file is sent as a **single
script** under one transaction, guarded by a fixed-key `pg_advisory_xact_lock` so concurrent
replica boots don't double-apply. DDL must be idempotent (`CREATE … IF NOT EXISTS`, guarded
`DO` blocks, `ON CONFLICT DO NOTHING`). A newly committed `sql/*.sql` applies on the next deploy
with **zero wiring** — there is no list to keep in sync.

**PRs that built this** (verified via `git log`):

- core-x:
  - `#812` `68911c5` — rename `recipients` → `documenso_response` on `business.documenso_templates`, store full API envelope. **Predates** the mirror work (foundation for storing verbatim responses).
  - `#813` `d81fb0d` — fix: complete the `recipients`→`documenso_response` rename in `sql/engagement_archetypes.sql` (a stale reference was crashing edge_api boot).
  - `#814` `81cbbb0` — `documenso_envelopes` mirror + async projector (`documenso_projection/`) + template-config table + create-template `name` field. Migration `sql/documenso_envelopes.sql`.
  - `#815` `edca138` — on-demand re-grab (`documenso_projection/resync.py`) + mirror list endpoints (`routers/documenso_envelopes_v1.py`).
  - `#816` `d629107` — rename `documenso_template_configs` → `documenso_template_document_prefill_configs` + reshape; `documenso_prefill_configs/` module + `routers/documenso_prefill_configs_v1.py`.
  - `#811` `93c636a` — prepaid-introductions v2 content (field-prefill HTML, underscores + define-once). **This landed before this cycle** (it is in the base log, not a same-session commit) but is the content groundwork the mirror configures.
- rare-structure-hq:
  - `#234` `20e1619` — operator-set `name` on "Create a Documenso Template".
  - `#235` `b1aa655` — "Template Mirror" settings card + page (mirror inspector + on-demand Re-grab).
  - `#236` `9c5f483` — "Manage Documenso Templates" editor (per-field default + read-only); also renamed the legacy "Manage Templates" card → "Set Template as Default".

---

## Tables (verified live against `core-x/prd → HQX_DB_URL_POOLED`)

### `business.documenso_envelopes` — THE MIRROR

A queryable mirror of Documenso envelopes — **templates AND documents**, type-discriminated.
Read-side convenience over `business.documenso_webhook_events` (which remains the raw append-only
SoR). On every non-DELETE webhook event the projector pulls the FULL live envelope
(`GET /api/v2/envelope/{envelope_id}`) and upserts here verbatim.

DDL: `apps/edge_api/sql/documenso_envelopes.sql:37-63`. Verified columns:

| column | type | null | notes |
|---|---|---|---|
| `id` | uuid | NO | surrogate PK (`gen_random_uuid()`) |
| `documenso_id` | bigint | NO | the envelope's **numeric** Documenso id — the UPSERT key. UNIQUE. |
| `envelope_id` | text | NO | the **prefixed** v2 envelope handle (`envelope_…`). UNIQUE. |
| `secondary_id` | text | YES | Documenso `secondaryId` (e.g. `document_<n>` / `template_<n>`) |
| `type` | text | NO | lowercased Documenso `type`/`source`: `'template'` \| `'document'`. **VERBATIM term, never remapped.** |
| `template_documenso_id` | bigint | YES | documents → SOURCE template numeric id; NULL for templates |
| `external_id` | text | YES | documents → Documenso `externalId` (the deal/opportunity handle stamped at originate) |
| `title` | text | YES | envelope title, verbatim |
| `status` | text | YES | lowercased Documenso status, **VERBATIM** — e.g. `'draft'`, `'cancelled'` (NEVER `'voided'`); no derived states |
| `documenso_response` | jsonb | NO | the FULL `GET /api/v2/envelope/{id}` response **exactly as Documenso returns it**. Holds `.fields` and `.recipients`. System of the mirror. |
| `deleted_at` | timestamptz | YES | soft-delete stamp set on a `*_DELETED` event (no API pull on delete) |
| `synced_at` | timestamptz | NO | bumped on every upsert |
| `created_at` | timestamptz | NO | set on INSERT only |
| `updated_at` | timestamptz | NO | bumped on every upsert |

Indexes (`sql/documenso_envelopes.sql:54-63`): UNIQUE on `documenso_id`, UNIQUE on `envelope_id`,
BTREE on `template_documenso_id`, `external_id`, `type`. The load-bearing resolution keys are all
indexed.

**VERBATIM CONTRACT** (enforced in code, stated in the SQL header `:13-17` and projector
`projector.py:7-13`): `documenso_response` is stored with no key rename / no value rewrite / no
snake_casing. `type`/`status` are **lowercased-only** projections of Documenso's own terms,
**never remapped**. Re-derive anything from `documenso_response` if a scalar extract is ever wrong.

### `business.documenso_template_document_prefill_configs` — operator PREFILL CONFIG

Per-template document-prefill config: per field, the default value + read-only flag. **OPERATOR/
app-owned** — the projector and resync **NEVER** write it (asserted in `projector.py:12`,
`queries.py:5-6`, `resync.py:11`). Keyed UNIQUE on `template_documenso_id`.

DDL: `apps/edge_api/sql/documenso_envelopes.sql:71-80`. Verified columns:

| column | type | null | notes |
|---|---|---|---|
| `id` | uuid | NO | surrogate PK |
| `template_documenso_id` | bigint | NO | the source template's numeric `documenso_id`. **UNIQUE.** |
| `field_settings` | jsonb | NO | default `'{}'`. Keyed by **field LABEL**. Each value is an arbitrary object stored verbatim. |
| `created_at` | timestamptz | NO | |
| `updated_at` | timestamptz | NO | bumped on upsert |

`field_settings` shape (Phase 1 + Phase 2 forward-compat):

```jsonc
{
  "<field label>": {
    "default_document_field_value": "<string>",   // Phase 1
    "read_only": true,                            // Phase 1
    "source": "<deal-fact key>"                   // Phase 2 (binds field → deal data); passes through untouched
  }
}
```

Inner keys are **not validated** by the API (`documenso_prefill_configs_v1.py:52-60`,
`queries.py:77-102`) so Phase 2 keys round-trip without an API change. Live state: **1 row,
non-empty** — the operator has already configured at least one template (14503; see the example).

### `business.documenso_webhook_events` — RAW capture (the projector's trigger source)

Raw verbatim capture of EVERY Documenso webhook delivery — the append-only system of record.
DDL: `apps/edge_api/sql/documenso_webhook_events.sql:26-37`. Verified columns: `id` (uuid),
`received_at` (timestamptz), `event` (text), `envelope_id` (text), `external_id` (text),
`payload` (jsonb, NOT NULL — THE raw body). The `event`/`envelope_id`/`external_id` columns are
best-effort lookup extracts, NOT authoritative; re-derive from `payload`. BTREE on `envelope_id`,
`external_id`, `received_at DESC`. No dedup (append-only); redelivery is a projection-time concern.

### `business.documenso_templates` — LEGACY registry (do not confuse with the mirror)

The OLD registry that predates the mirror. One row per priced/registered template
(14087–14439 etc.). Verified columns include `documenso_template_id` (**text**, numeric-as-string),
`organization_id`, `name`, `slug`, `status`, `documenso_response` (jsonb — note: this is the
`recipients`→`documenso_response` rename from #812), `archetype_id`, `global_input_content_id`,
`is_default`. **Does NOT contain new mirror-path templates** — verified: `documenso_id=14503` has
**0 rows** here. Still feeds: the legacy "Documenso Templates" field-defaults editor, the
"Set Template as Default" table, and the Deal Details template dropdown (via
`deals/queries.py:108 list_org_templates`). It also still carries the old originate primitives
inside `documenso_response`: `prospect_recipient_id`, `default_field_values`,
`editable_field_labels` (read in `deals/queries.py:~218-259`).

### `business.deal_details` — per-deal override store (model-B layer)

Verified columns: `deal_id` (uuid, PK), `field_values` (jsonb, NOT NULL — the per-deal OVERRIDE
store), `default_template_uuid` (uuid → `documenso_templates.id`, the attached template),
`template_origin` (text), `created_at`, `updated_at`. Deal facts for Phase-2 binding live
elsewhere: `business.deals.company_name` / `company_domain`; the signatory's full name / title /
email via `business.deal_contacts` → `business.contacts` (`first_name`+`last_name`, `title`,
`email`; signatory selected by `deal_contacts.is_signatory`, see `deals/queries.py:75-96, 186-219`).

---

## Sync flow (event-driven: ack-then-sync)

```
Documenso webhook
   │  POST /api/v1/documenso/webhook   (X-Documenso-Secret)
   ▼
routers/documenso_webhooks_v1.py
   1. verify secret (401 on bad secret, 503 if unconfigured)
   2. append RAW body → business.documenso_webhook_events   (the SoR write)
   3. return 200 {ok, id}                                   ← never waits on Documenso
   4. THEN BackgroundTasks.add_task(project_envelope_event, event, raw)
   ▼
documenso_projection/projector.py  project_envelope_event()
   • DELETE event (name ends in "DELETED") → soft-delete, NO API pull
   • else → GET /api/v2/envelope/{envelope_id}   (the webhook payload LACKS fields[] — must pull)
            → queries.upsert_envelope(...)  VERBATIM
```

Verified detail:

- **Ack-then-sync.** The 200 is returned in the route body (`documenso_webhooks_v1.py:85`) and the
  projector is scheduled via FastAPI `BackgroundTasks` (`:83`) so it runs **after** the response is
  sent. The 200 never blocks on the projector's live Documenso pull.
- **Trigger gate.** The projector is scheduled only when `event is not None` AND the inner payload
  carries a numeric `id` (`documenso_webhooks_v1.py:82`). Legacy events lacking `envelopeId` are
  skipped inside the projector (`projector.py:59-65`) — `envelopeId` is present on all events since
  ~2026-06-18.
- **DELETE events soft-delete with no pull** (`projector.py:68-79` → `queries.soft_delete_envelope`).
  A subsequent live event **resurrects** the row (`upsert_envelope` resets `deleted_at = NULL`,
  `queries.py:47-58`).
- **Resilience.** `project_envelope_event` catches `DocumensoError` and any `Exception`, logs, and
  returns — it runs detached, so a raise would have nowhere to surface (`projector.py:108-111`).
- **One upsert path.** Both the webhook projector and the on-demand re-grab call the SAME
  `queries.upsert_envelope` with IDENTICAL field extraction (`projector.py:90-102`,
  `resync.py:74-86`). There is no second upsert/remap path.

### The critical gap — TEMPLATE_UPDATED does not fire on field placement

**Documenso does NOT fire `TEMPLATE_UPDATED` when a field is placed/auto-saved in the template
editor.** Consequence: after an operator drops or edits fields on a template, **no webhook
arrives**, so the mirror's `documenso_response.fields[]` for that template goes stale. The prefill
editor reads field labels off the mirror, so a stale mirror means the editor shows the wrong field
set.

**Solution: operator-triggered RE-GRAB.** `documenso_projection/resync.py
resync_template_by_documenso_id(documenso_id)` resolves the `envelope_id` (mirror row first, else a
live `GET /api/v2/template/{id}` fallback — `resync.py:26-45`), pulls the FULL live envelope, and
upserts through the same `queries.upsert_envelope` (`resync.py:48-96`). A `DocumensoError` returns
`{synced: False, error}` rather than raising — a re-grab of a deleted/unreachable template degrades
cleanly. Surfaced in the Template Mirror UI as **Re-grab** (one) / **Re-grab all**.

---

## API endpoints

### edge_api (core-x, FastAPI) — service-token gated

| method | path | router | purpose |
|---|---|---|---|
| POST | `/api/v1/documenso/webhook` | `documenso_webhooks_v1.py:40` | raw capture + schedule projector. `X-Documenso-Secret`. |
| GET | `/api/v1/documenso/sign-state/{opportunity_id}/{document_id}` | `documenso_webhooks_v1.py:88` | PUBLIC offline signing-state poll (derived from raw webhook capture; zero Documenso calls) |
| GET | `/api/v1/documenso/sign-token/{opportunity_id}/{document_id}` | `documenso_webhooks_v1.py:121` | PUBLIC one-time embed-token read (pair-gated) |
| GET | `/api/v1/documenso-envelopes/templates` | `documenso_envelopes_v1.py:65` | the mirrored TEMPLATE rows (verbatim mirror, newest sync first) |
| POST | `/api/v1/documenso-envelopes/{documenso_id}/resync` | `documenso_envelopes_v1.py:73` | re-grab ONE template/envelope, re-mirror it |
| POST | `/api/v1/documenso-envelopes/resync-all` | `documenso_envelopes_v1.py:81` | re-grab EVERY mirrored template, SEQUENTIALLY (no fan-out) |
| GET | `/api/v1/documenso-template-prefill/{documenso_id}` | `documenso_prefill_configs_v1.py:70` | template's value fields (off the mirror) + saved `field_settings` |
| PUT | `/api/v1/documenso-template-prefill/{documenso_id}` | `documenso_prefill_configs_v1.py:83` | UPSERT the operator-owned `field_settings` (sole writer of the config table) |
| GET | `/api/v1/documenso-templates` | `documenso_templates_v1.py:25` | LEGACY registry list (the "Set Template as Default" table) |
| POST | `/api/v1/documenso-templates/default` | `documenso_templates_v1.py:33` | LEGACY set-default |

Notes:
- `resync-all` collects per-template outcomes verbatim; one failing template can't fail the batch —
  errors surface as `{synced: false, error}` with HTTP 200 (`documenso_envelopes_v1.py:81-95`).
- The prefill GET's value fields come from `documenso_response->'fields'` filtered to
  `type IN ('TEXT','NUMBER')` AND a non-null `fieldMeta.label` (`documenso_prefill_configs/queries.py:25-54`).
  Each field is returned with `label`, `type`, `required`, `read_only`, `recipient_id` (booleans
  passed through verbatim from `fieldMeta`).

### platform-api (rare-structure-hq, Hono BFF) — `requireUser`, forwards to edge_api with the service token

The BFF route **prefixes differ** from edge_api's — it re-namespaces under `documenso-template-mirror`
and forwards to edge_api's `documenso-envelopes`:

| method | BFF path | forwards to edge_api | source |
|---|---|---|---|
| GET | `/api/v1/documenso-template-mirror` | `GET /api/v1/documenso-envelopes/templates` | `routes/documenso-template-mirror.ts:4`, `lib/edge.ts:886` |
| POST | `/api/v1/documenso-template-mirror/:id/resync` | `POST /api/v1/documenso-envelopes/{id}/resync` | `routes/documenso-template-mirror.ts:42` |
| POST | `/api/v1/documenso-template-mirror/resync-all` | `POST /api/v1/documenso-envelopes/resync-all` | `routes/documenso-template-mirror.ts:55`, `lib/edge.ts:919` |
| GET | `/api/v1/documenso-template-prefill/:id` | `GET /api/v1/documenso-template-prefill/{id}` | `routes/documenso-template-prefill.ts:42` |
| PUT | `/api/v1/documenso-template-prefill/:id` | `PUT /api/v1/documenso-template-prefill/{id}` | `routes/documenso-template-prefill.ts:52` |

Registered in `apps/platform-api/src/index.ts:135,139`. SPA clients:
`apps/platform-app/src/settings/documenso-template-mirror-api.ts` (BASE `/api/v1/documenso-template-mirror`)
and `documenso-template-prefill-api.ts` (BASE `/api/v1/documenso-template-prefill`).

---

## Operator UI surfaces (rare-structure-hq, Settings → Documenso)

Hub page: `apps/platform-app/src/routes/app/SettingsDocumenso.tsx` (route `settings/documenso`,
`App.tsx:239`). Four cards:

| card | route | component | layer | role |
|---|---|---|---|---|
| **Documenso Templates** | `settings/documenso-templates` (`App.tsx:303`) | `DocumensoTemplatesEditor.tsx` | LEGACY | open a template, set default values that **bake onto the template** — every document inherits them (legacy registry) |
| **Set Template as Default** | `settings/documenso/templates` (`App.tsx:247`) | `DocumensoTemplatesManage.tsx` | LEGACY | org-default radio table over the legacy registry (renamed from "Manage Templates" in #236) |
| **Template Mirror** | `settings/documenso-template-mirror` (`App.tsx:255`) | `DocumensoTemplateMirror.tsx` | NEW | mirror inspector (fields/recipients/status/synced_at) + **Re-grab / Re-grab all** |
| **Manage Documenso Templates** | `settings/manage-documenso-templates` (`App.tsx:263`) | `ManageDocumensoTemplates.tsx` | NEW | the **prefill-config editor**: pick a mirror template → per-field **Default value** + **Read-only** toggle → saves `field_settings` |

(The hub card labels and CTAs are verified verbatim in `SettingsDocumenso.tsx:22-49`.)

---

## The prefill model (B) — resolution precedence

Agreed model **B (resolve-at-read, no copy-on-attach)**, to avoid drift between an attached
template's defaults and the document:

- **Config** holds the per-field `default_document_field_value` + `read_only`.
- **`business.deal_details.field_values`** holds per-deal OVERRIDES (keyed by field label).
- **At originate**, per field: `value = deal_details.field_values[label] ?? config.default_document_field_value`
  (explicit override, else live default). There is **no copy-on-attach**, so nothing drifts.
- `read_only` fields are **locked** on the derived document.
- **Phase 2:** a `source` key in `field_settings[label]` binds prospect-fact fields to deal data
  (e.g. Full Name / Title / Company Name → deal facts), resolved at originate.

Stated in the SQL header (`sql/documenso_envelopes.sql:65-70`), the prefill router docstring
(`documenso_prefill_configs_v1.py:8-13`), and the SPA api-client header
(`documenso-template-prefill-api.ts:10-13`).

---

## Key Documenso facts (load-bearing — verified this cycle)

- **Two id schemes, not interchangeable.** `documenso_id` (the numeric payload `id`) vs `envelope_id`
  (the prefixed `envelopeId`) name the same envelope but route to **different endpoints**: envelope
  endpoints want the **prefixed** handle (`GET /api/v2/envelope/{envelope_id}`); document/template
  endpoints want the **numeric** id (`GET /api/v2/document/{n}`, `GET /api/v2/template/{n}`). A numeric
  id 400s on the envelope endpoint; the prefixed handle 400s on the document endpoint
  (`documenso_webhooks_v1.py:128-133`).
- **A document off a template gets its OWN `envelope_id`,** linked back to the source via `templateId`
  → mirrored as `template_documenso_id` (`projector.py:97`).
- **Template events** carry `id` = template id and `templateId` = null. **`TEMPLATE_USED`** is a
  **document event**: `id` = the new DOCUMENT id, `templateId` = the source template.
- **The webhook payload has `recipients` but NOT `fields[]`** — that is why the projector must pull the
  full envelope on every non-delete event (`projector.py:46`, `:82`).
- **`envelopeId` is present on all events since ~2026-06-18.** Earlier events lack it and are skipped
  by the projector (`projector.py:59-65`).
- **Prefill at instantiation** = `POST /api/v2/template/use` with `prefillFields` keyed by **FIELD ID**
  (not label), `type` **lowercased** (`text`/`number`), `value` always a **string** (even NUMBER). A
  label can map to multiple field ids — emit one prefill entry per id. `/template/use` **preserves the
  field's existing `readOnly`**; `recipients[]` is REQUIRED (unlike `/envelope/use`)
  (`documenso_client.py:215-225, 337-372`).
- **`readOnly` is set on the DERIVED document, AFTER prefill,** via
  `POST /api/v2/envelope/field/update-many` — because `readOnly` can't be set at instantiation and a
  TEMPLATE field can't be `readOnly` without static text. A read-only field **must have a value**
  ("read-only must have text"); the prefilled value satisfies that rule. Derived fields carry NEW ids
  but PRESERVE `fieldMeta.label`, so the lock step identifies prefilled fields by a non-empty value
  and decides editable-vs-locked BY LABEL — the prefill config's own keys
  (`documenso_client.py:create_document_from_template`).
- **Reference implementation:** `documenso_client.create_document_from_template`
  (`apps/edge_api/src/services/documenso_client.py:228`) — the canonical
  resolve-template → prefill → lock → distribute(NONE)→PENDING path. The Phase-2 resolver should reuse
  this, feeding it config-resolved values.

---

## The 14503 example (verified live)

The template being configured: `documenso_id` **14503**, `envelope_id` **`envelope_bfddmodflibswvdu`**,
title **"Rare Structure - GC - v3"** (prepaid-introductions), `type=template`, `status=draft`,
**13 fields**, **2 recipients**. Confirmed NOT in the legacy `documenso_templates` registry (0 rows
there) — it is a pure mirror-path template, selectable only once the deal dropdown is repointed
(OPEN WORK #3).

The 13 fields (verified off `documenso_response.fields`):

- **5 signature/date** (no labels): 2 `SIGNATURE` (recipients 2688731, 2688730), 3 `DATE`. Left open
  for the signer.
- **8 `TEXT` value fields** — all on the participant recipient `2688730`, all `required=true`, all
  `read_only=false` **on the template** (the lock is applied per-config on the derived document, not
  the template):

  | label | role | field id |
  |---|---|---|
  | Legal Entity Name | prospect fact (editable) | 12917662 |
  | D/B/A Name | prospect fact (editable) | 12917665 |
  | Full Name | prospect fact (editable) | 12917853 |
  | Title | prospect fact (editable) | 12917945 |
  | PrepaidFee | operator term (default + lock via config) | 12917791 |
  | IntroNum | operator term (default + lock via config) | 12917792 |
  | PricePerIntro | operator term (default + lock via config) | 12917811 |
  | Duration | operator term (default + lock via config) | 12917841 |

**Economic spine:** `PrepaidFee = IntroNum × PricePerIntro` (e.g. $36,000 = 12 × $3,000).
**Currency:** whole-dollar, no-cents for round amounts (house `usd()` convention).

---

## OPEN WORK (the next agent's job — numbered, actionable)

1. **The originate RESOLVER (Phase 2 consumption) — NOT BUILT.** The config is settable but **nothing
   consumes it**. Build the originate-time path: for each template field, resolve
   `value = deal_details.field_values[label] ?? config.default_document_field_value`; prefill via
   `POST /api/v2/template/use` (`prefillFields` keyed by field id, type lowercased, string value); lock
   the `read_only` fields on the derived document via `POST /api/v2/envelope/field/update-many`. **Reuse**
   `documenso_client.create_document_from_template` (`services/documenso_client.py:228`) — it already does
   template-resolve → prefill → lock → distribute(NONE); feed it config-resolved values and the
   `read_only`/`editable_labels` set derived from `field_settings`. This is the load-bearing missing piece.

2. **Deal-source binding (`source` in `field_settings`) — NOT BUILT.** Add the `source` key to the editor
   and resolver: bind Full Name / Title / Company Name → deal facts
   (`deals.company_name`/`company_domain`; signatory full name/title/email via
   `deal_contacts`→`contacts`, see `deals/queries.py:75-96, 186-219`). `field_settings` already passes
   `source` through verbatim (no API/DDL change needed) — wire the editor UI and the resolver to honor it.

3. **Repoint the Deal Details "Documenso Template" dropdown** from the legacy registry to the mirror.
   Today the deal dropdown is fed by `deals/queries.py:108 list_org_templates`, which reads
   `business.documenso_templates` (legacy) — so new mirror-path templates (14503) are **not selectable on
   deals**. Repoint the feeder to `business.documenso_envelopes WHERE type='template' AND deleted_at IS NULL`.
   Watch the join: `deal_details.default_template_uuid` currently FKs `documenso_templates.id` (a uuid),
   whereas the mirror key is `documenso_id` (bigint) — the attachment storage must be reconciled.

4. **Deal-details prefill PREVIEW surface — NOT BUILT.** On a deal, show the **resolved** values
   (default + override + Phase-2 binding) per field, with override inputs that write
   `deal_details.field_values[label]`. Gives the operator a what-will-the-document-say view before
   originate.

5. **Cleanup: orphan/empty tables.** Verified live: `business.documenso_envelopes_orphan` (**1 row**) and
   the superseded `business.documenso_template_configs` (**0 rows**, empty) both still exist. The SQL has a
   note that the old config name is "dropped manually" (`sql/documenso_envelopes.sql:82-84`) — the DROP is
   **not** in the idempotent boot DDL (a destructive DROP must not auto-run on every boot), so it needs a
   one-off manual terminal run. Drop both once confirmed unreferenced.

---

## Secrets / deploy / how to query

**Secrets (Doppler `core-x/prd`):**

| key | value / use |
|---|---|
| `HQX_DB_URL_POOLED` | HQX Postgres (schema `business`) — the operational SoR |
| `DOCUMENSO_API_KEY` | Documenso API bearer |
| `DOCUMENSO_API_URL` | `https://app.documenso.com` |
| `DOCUMENSO_WEBHOOK_SECRET` | gates `POST /api/v1/documenso/webhook` (`X-Documenso-Secret`) |
| `EDGE_API_SERVICE_TOKEN` | the BFF→edge_api service token |
| `EDGE_API_BASE_URL` | `https://api.edgeapi.run` |
| `DOCRAPTOR_API_KEY` | DocRaptor (HTML→PDF, template content lane) |

**Deploy:** edge_api on **Railway**, auto-redeploy on merge to `main`. `apps/edge_api/sql/*.sql` is
applied idempotently at boot by `src/migrate.py` (sorted-glob auto-discovery, advisory-locked,
per-file transaction). A new DDL file needs no wiring — commit it and it applies on the next deploy.

**Read-only query recipe (the schema/data checks in this doc were run this way):**

```bash
doppler run --project core-x --config prd -- python3 - <<'PY'
import os, psycopg
with psycopg.connect(os.environ["HQX_DB_URL_POOLED"]) as conn, conn.cursor() as cur:
    # the mirrored templates
    cur.execute("""
        SELECT documenso_id, envelope_id, title, status,
               jsonb_array_length(COALESCE(documenso_response->'fields','[]'::jsonb))     AS fields,
               jsonb_array_length(COALESCE(documenso_response->'recipients','[]'::jsonb)) AS recips
        FROM business.documenso_envelopes
        WHERE type='template' AND deleted_at IS NULL
        ORDER BY synced_at DESC
    """)
    for r in cur.fetchall():
        print(r)
    # the value fields the prefill editor sees for a template (e.g. 14503)
    cur.execute("""
        SELECT fld->'fieldMeta'->>'label' AS label, fld->>'type' AS type,
               (fld->'fieldMeta'->>'readOnly')::bool AS read_only, fld->>'id' AS field_id
        FROM business.documenso_envelopes env
        CROSS JOIN LATERAL jsonb_array_elements(env.documenso_response->'fields') fld
        WHERE env.documenso_id=14503 AND fld->>'type' IN ('TEXT','NUMBER')
          AND fld->'fieldMeta'->>'label' IS NOT NULL
        ORDER BY label
    """)
    for r in cur.fetchall():
        print(r)
PY
```

**Keep it READ-ONLY** — no INSERT/UPDATE/DROP against prod from a query session.
