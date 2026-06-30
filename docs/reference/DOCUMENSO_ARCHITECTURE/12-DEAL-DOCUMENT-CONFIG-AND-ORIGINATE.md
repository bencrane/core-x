# 12 — Deal Document Config & the Originate Resolver (architecture + handoff)

> STATUS — **ACTIVE, SHIPPED**. This is the reference for the **deal → document-config → originate**
> layer — the piece that doc 11 named as its primary OPEN WORK ("the originate RESOLVER, NOT BUILT").
> It is now built and live. A deal pins a MIRROR template + per-field override values in a new
> append-only satellite (`business.deal_document_configs`), the operator picks a default for mirror
> templates in a new operator store (`business.documenso_template_defaults`), and `POST
> /api/v1/deals/{handle}/originate` is **rewritten as the resolver**: it resolves field values
> (model B), locks operator terms, derives the prospect recipient off the verbatim mirror, and mints a
> PENDING Documenso document via the UNCHANGED engine `create_document_from_template`.
> **Read `11-ENVELOPE-MIRROR-AND-PREFILL-CONFIG.md` first** — it builds the two primitives this layer
> consumes (the mirror `business.documenso_envelopes` and the prefill config
> `business.documenso_template_document_prefill_configs`). This doc is the continuation: the layer ON TOP.

---

## Orientation — what this layer does and why

Doc 11 delivered the mirror and the operator prefill config but stopped short of the consumer: nothing
read the config at originate, the deal still attached templates against the **legacy**
`business.documenso_templates` registry (which does not even contain mirror-path templates like 14503),
and the deal's per-field overrides lived in the 1:1 mutable `business.deal_details` satellite. This cycle
moved the entire deal document path onto the mirror + new operator-owned tables and rewrote originate:

1. **`business.deal_document_configs`** — an APPEND-ONLY satellite pinning, per deal, `{which mirror
   template is attached, the per-field override values}` together, with exactly one `active` row per
   deal. **Supersedes `business.deal_details`.**
2. **`business.documenso_template_defaults`** — the operator's "Confirm & Originate default" for MIRROR
   templates (the legacy registry's `is_default` had nowhere to record a choice for a mirror template).
3. **The originate RESOLVER** (`POST /api/v1/deals/{handle}/originate`, rewritten) — model B resolution
   (`override ?? config default`), operator-term locking, prospect-recipient derivation off the verbatim
   mirror recipients, a fail-loud guard, then a call into the **unchanged** engine
   `documenso_client.create_document_from_template`.

The **3 legacy resolvers were DELETED**. The deal originate path no longer reads
`business.documenso_templates` at all — it sources every input from the new world (deal config + prefill
config + verbatim mirror + `deal_contacts`).

### What a new agent can do immediately

The full originate path is live and verified end-to-end against a real deal: attach a mirror template to
a deal and set its override values in **Deal Details**, then hit **Originate** on the **Mandate** page —
edge_api mints a prefilled, locked, PENDING Documenso document keyed by the deal's 8-char handle and
returns a `/p/m/{handle}/{document_id}` sign link. A new agent can: tune the resolver
(`deals/originate.py` — pure, no I/O), extend the prefill config consumption, build the Phase-2 `source`
binding (auto-fill prospect facts from `deal_contacts`→`contacts` — see OPEN WORK), or retire the now-dead
`deal_details` table. The engine itself (`create_document_from_template`) needs no changes for any of this.

---

## Repos & deploy

| repo | path | role |
|---|---|---|
| **core-x** | `/Users/benjamincrane/core-x` | data/compute plane. `apps/edge_api` (FastAPI) owns the deal document config, the originate resolver, and the engine. HQX Postgres (Doppler `core-x/prd` → `HQX_DB_URL_POOLED`), schema `business`, is the operational SoR. Deploys on **Railway** (auto-redeploy on merge to `main`). |
| **rare-structure-hq** | `/Users/benjamincrane/rare-structure-hq` | operator app. `apps/platform-app` (React SPA) + `apps/platform-api` (Hono BFF). Consumes edge_api via `EDGE_API_SERVICE_TOKEN`; `EDGE_API_BASE_URL = https://api.edgeapi.run`. |

**Migration mechanism (core-x).** Unchanged from doc 11: edge_api has **no migration framework**.
`apps/edge_api/sql/*.sql` is committed DDL; `src/migrate.py` re-applies the whole `sql/` directory on
every boot (sorted-glob auto-discovery, advisory-locked, per-file transaction). DDL must be idempotent.
A newly committed `sql/*.sql` applies on the next deploy with zero wiring. New this cycle:
`sql/deal_document_configs.sql`, `sql/documenso_template_defaults.sql`.

**PRs that built this layer** (verified via `git log`):

- core-x:
  - `#818` `962cef3` — `documenso-template-defaults` — mirror-backed Set-Template-as-Default picker + default store. Migration `sql/documenso_template_defaults.sql`.
  - `#819` `21f5673` — deal template attach reads the envelope mirror, keyed by `documenso_id` (the deal dropdown repointed off the legacy registry onto `documenso_envelopes`).
  - `#820` `b52445c` — `deal_document_configs` — append-only document config replacing the `deal_details` satellite. Migration `sql/deal_document_configs.sql`.
  - `#821` `b9c89c5` — rename the deal contract field `default_template_documenso_id` → `template_documenso_id`; drop dead legacy fields.
  - `#823` `6ca002d` — **the originate RESOLVER** — mint from `deal_document_configs` + prefill config + mirror; retire the 3 legacy resolvers. New `deals/originate.py`, rewritten `get_deal_originate_inputs` + `originate_deal`. (`#822` `32bd9d0`, booking→deal materialization, landed in the same log but is a separate lane — not part of this document layer.)
- rare-structure-hq:
  - `#237` `018c494` — Set-Template-as-Default reads the envelope mirror; hide vestigial Application picker.
  - `#238` `7148441` — Deal Details template dropdown reads the envelope mirror (`documenso_id`).
  - `#239` `2d97dca` — Mandate reads the mirror attach (`documenso_id`), not the legacy uuid.
  - `#240` `0ce377b` — rename deal contract `defaultTemplateDocumensoId` → `templateDocumensoId`; drop dead legacy fields.
  - `#242` `e418cac` — prefill-fields UI on Deal Details — per-deal override values.
  - `#243` `34beb47` — hoist `prefillFields` useMemo above early returns (fix: blank Deal Details page).

---

## Tables (verified live against `core-x/prd → HQX_DB_URL_POOLED`)

### `business.deal_document_configs` — the deal's DOCUMENT CONFIG (supersedes `deal_details`)

APPEND-ONLY satellite, **one `active` row per deal**. Pins `{template_documenso_id, field_values}`
TOGETHER because the override values are keyed by THAT template's field labels and are only meaningful
next to their template. DDL: `apps/edge_api/sql/deal_document_configs.sql`. Verified columns (live):

| column | type | null | default | notes |
|---|---|---|---|---|
| `id` | uuid | NO | `gen_random_uuid()` | surrogate PK |
| `deal_id` | uuid | NO | | FK `business.deals(id)` **ON DELETE CASCADE** — the config dies with its deal |
| `template_documenso_id` | bigint | YES | | attached MIRROR template's numeric `documenso_envelopes.documenso_id`. NULL = none attached. **No FK** to the projector-owned mirror — validated at write. |
| `field_values` | jsonb | NO | `'{}'::jsonb` | OPERATOR OVERRIDES ONLY (label → value). Template defaults + prospect facts are NOT copied in — they resolve at read/originate (model B). |
| `status` | text | NO | `'active'` | `'active'` \| `'archived'` |
| `created_at` | timestamptz | NO | `now()` | |
| `updated_at` | timestamptz | NO | `now()` | bumped on update |

Indexes (verified live, `pg_indexes`):
- `deal_document_configs_pkey` — UNIQUE on `id`.
- `deal_document_configs_deal_id_idx` — BTREE on `deal_id`.
- `deal_document_configs_one_active_per_deal_uidx` — **partial UNIQUE** on `deal_id` `WHERE status = 'active'` — pins at most one active config per deal.

**Write semantics** (`deals/queries.py:upsert_document_config:139-204`):
- **Same template** (active row's `template_documenso_id` == incoming) → UPDATE the active row's
  `field_values` in place.
- **Different (or first) template** → ARCHIVE the active row (`status='archived'`), INSERT a fresh
  `active` row. Archive-before-insert avoids a transient unique violation.
- Concurrency: a `SELECT 1 FROM business.deals WHERE id = %s FOR UPDATE` locks the parent deal for the
  whole transaction so simultaneous template-switch PUTs can't race the archive→insert into a
  `unique_violation` (the loser would otherwise 500). `queries.py:152-156`.
- The same call reconciles the `deal_contacts` junction (drop removed, upsert kept with `is_signatory`).

### `business.documenso_template_defaults` — operator's MIRROR-template default store

The operator's "Confirm & Originate default" for a MIRROR template. The mirror
(`documenso_envelopes`) is projector-owned/verbatim and must never carry an operator flag; the legacy
`documenso_templates` registry does not contain mirror-path templates (14503). So the choice is recorded
HERE, keyed by the mirror's numeric `documenso_id` — the same operator-owned boundary as the prefill
config. DDL: `apps/edge_api/sql/documenso_template_defaults.sql`. Verified columns (live):

| column | type | null | default | notes |
|---|---|---|---|---|
| `id` | uuid | NO | `gen_random_uuid()` | surrogate PK |
| `documenso_id` | bigint | NO | | the MIRROR template's numeric `documenso_id` — the upsert key |
| `is_default` | boolean | NO | `true` | |
| `created_at` | timestamptz | NO | `now()` | |
| `updated_at` | timestamptz | NO | `now()` | |

Indexes (verified live):
- `documenso_template_defaults_pkey` — UNIQUE on `id`.
- `documenso_template_defaults_documenso_id_uidx` — UNIQUE on `documenso_id` (one row per mirror template; the ON CONFLICT upsert target).
- `documenso_template_defaults_one_default_uidx` — **partial UNIQUE** on `is_default` `WHERE is_default` — at most ONE default across all mirror templates (single-operator plane). Set-default is **clear-then-set** so the partial unique index is never transiently violated.

**OWNERSHIP**: OPERATOR/app-owned. The async projector / on-demand re-grab NEVER write this table; the
sole writer is the Set-Template-as-Default picker (`POST /api/v1/documenso-template-defaults`).

### `business.deal_details` — LEGACY satellite (SUPERSEDED, still present, NOT on the deal path)

The old 1:1 mutable satellite that `deal_document_configs` replaces. **Verified live: it still exists
(`to_regclass` non-null) and holds 3 rows.** It is NO LONGER written by the deal document/originate path
— `get_deal_originate_inputs` and `upsert_document_config` read/write `deal_document_configs`, not
`deal_details`. The only remaining `deal_details` references in `apps/edge_api/src/` are stale doc-comment
strings in `deals/models.py:1` and `deals/__init__.py:1` (module headers), not code that reads the table.
**Retire-able** (see OPEN WORK).

---

## End-to-end flow

```
OPERATOR (rare-structure-hq SPA)
  │
  │  Deal Details page (DealDetails.tsx)
  │   • picks a MIRROR template from the dropdown  (fed off documenso_envelopes via list_org_templates)
  │   • types per-field OVERRIDE values            (prospect facts + operator-term overrides)
  ▼
PUT /api/v1/deals/{handle}/details  →  edge_api upsert_document_config
  → business.deal_document_configs   (active row: template_documenso_id + field_values overrides)
  │
  │  Mandate page (Mandate.tsx) → click Originate
  ▼
POST /api/v1/deals/{handle}/originate            routers/deals_v1.py:90 originate_deal
  │
  1) queries.get_deal_originate_inputs(conn, handle)            deals/queries.py:213
  │    reads, by deal_handle:
  │      • cfg.template_documenso_id   (ACTIVE deal_document_configs)
  │      • cfg.field_values            (the deal's OVERRIDES)
  │      • pc.field_settings           (documenso_template_document_prefill_configs — defaults + read_only)
  │      • env.documenso_response      (the VERBATIM mirror template — recipients[], for prospect derivation)
  │      • signatory contact email + name (deal_contacts WHERE is_signatory → contacts)
  │
  2) RESOLVE (model B — deals/originate.py, PURE):
  │      field_values   = resolve_field_values(field_settings, deal.field_values)   # override ?? default
  │      locked         = locked_labels(field_settings)                             # read_only==true
  │      missing_locked = locked − set(field_values)   → 422 if non-empty (FAIL LOUD)
  │      editable_labels = set(field_values) − locked
  │      prospect_rid   = derive_prospect_recipient_id(template_response)           # most-common recipientId on value fields
  │
  3) documenso_client.create_document_from_template(...)   services/documenso_client.py:228  (UNCHANGED)
  │      /api/v2/template/use  (prefillFields keyed by field id, type lowercased, value as string)
  │      → /api/v2/envelope/field/update-many  (lock the read_only/operator-term fields on the DERIVED doc)
  │      → /api/v2/envelope/distribute  distributionMethod:NONE  → PENDING
  │      externalId = deal_handle      (the prospect-link + sign-gate anchor)
  │
  ▼
DealOriginated { envelope_id, document_id, deal_handle, signing_token,
                 sign_link = "/p/m/{deal_handle}/{document_id}", status:"pending" }
  │
  ▼
WEBHOOK (TEMPLATE_USED → document event) → projector → business.documenso_envelopes
   (the minted doc is mirrored: type='document', external_id=deal_handle, template_documenso_id=14503)
```

---

## The resolver internals (`apps/edge_api/src/deals/originate.py` — PURE, no I/O)

Three pure functions over the query result. Cited at `deals/originate.py`:

- **`resolve_field_values(field_settings, deal_field_values)` (`:19-39`)** — model B. Per label, the
  value is the deal's `field_values[label]` (override, if non-empty after coercion) ELSE the config's
  `default_document_field_value`. Values are coerced to **strings** (Documenso prefill wants strings);
  empty-after-coercion labels are skipped. The merge iterates the keyset of BOTH config defaults and deal
  overrides — so a config-default-only label still prefills, and a deal-override-only label still applies.
- **`locked_labels(field_settings)` (`:42-49`)** — the set of labels with `read_only: true` in the
  prefill config. These are LOCKED on the derived document (operator terms the signer can't change);
  everything else prefilled stays editable.
- **`derive_prospect_recipient_id(template_response)` (`:52-78`)** — the prospect binds to the recipient
  the template's labelled TEXT/NUMBER (value) fields are assigned to: the **most-common `recipientId`**
  among them. `None` when undeterminable (caller falls back to the engine's placeholder heuristic).

**The orchestration (`routers/deals_v1.py:originate_deal:90-146`):**
- 404 if the deal is unknown; 422 if `template_documenso_id is None` ("deal has no attached template");
  422 if no signatory email ("deal has no signatory contact with an email").
- `editable_labels = set(field_values) − locked` — passed into the engine so operator-designated editable
  fields are left UNLOCKED on the derived document.
- **Fail-loud guard (`:106-118`):** `missing_locked = locked − set(field_values)`. A `read_only` operator
  term with no resolved value cannot be locked (Documenso requires read-only fields to carry text) and
  would silently ship signer-EDITABLE — so it 422s instead, listing the offending labels. (This is the
  hardening that came out of #823's adversarial review.)
- `external_id = deal_handle` (LOCKED — the public 8-char id is the prospect-link capability + sign-gate
  anchor). `title = "Engagement Agreement"`.
- 502 on `DocumensoError`; 502 if the engine returns no numeric document id (a missing one yields a dead
  `…/None` link — fail loud rather than return a successful-looking 200).
- Returns `sign_link = f"/p/m/{deal_handle}/{document_id}"`.

**The engine is UNCHANGED.** `documenso_client.create_document_from_template` (`services/documenso_client.py:228`)
is exactly the doc-11 resolve-template → prefill → lock → distribute(NONE) → PENDING path. The resolver
feeds it `field_values_by_label`, `editable_labels`, `prospect_recipient_id`, `external_id`, `title`. It
binds the prospect to the recipient id (`:277-308`), fans each label out to every matching field id
(`:310-335`), prefills via `/template/use` (`:342-372`), then locks the non-editable prefilled fields on
the derived document via `/envelope/field/update-many` and distributes NONE.

---

## API endpoints (this layer)

### edge_api (core-x, FastAPI) — service-token gated

| method | path | router | purpose |
|---|---|---|---|
| GET | `/api/v1/documenso-template-defaults` | `documenso_template_defaults_v1.py:36` | every MIRROR template (non-deleted, newest sync first), each flagged `is_default`. Reads `documenso_envelopes` LEFT JOIN the operator default store. |
| POST | `/api/v1/documenso-template-defaults` | `documenso_template_defaults_v1.py:45` | mark one mirror template as the Confirm & Originate default (clear-then-set). 404 if not a live mirror template. SOLE writer of `documenso_template_defaults`. |
| GET | `/api/v1/deals/{handle}/details` | `deals_v1.py:58` | the deal's editable config: contacts (from `deal_contacts`, person fields read-only), `field_values`, the attached `template_documenso_id`, and the selectable templates (off the mirror, via `list_org_templates`). |
| PUT | `/api/v1/deals/{handle}/details` | `deals_v1.py:70` | write `field_values` + the attached `template_documenso_id` and reconcile `deal_contacts`. Goes through `upsert_document_config` (append-only semantics). Returns the merged shape re-read. |
| POST | `/api/v1/deals/{handle}/originate` | `deals_v1.py:90` | **the resolver** — mint a prefilled, locked, PENDING document; returns the `/p/m/{handle}/{document_id}` sign link. |

`list_org_templates` (`deals/queries.py:117-136`) feeds the deal dropdown off the MIRROR:
`SELECT documenso_id, title AS name, is_default FROM business.documenso_envelopes e LEFT JOIN
business.documenso_template_defaults d ON d.documenso_id = e.documenso_id AND d.is_default WHERE e.type =
'template' AND e.deleted_at IS NULL ORDER BY e.synced_at DESC`. `organization_id` is accepted for call
compatibility but the mirror is NOT org-scoped.

### platform-api (rare-structure-hq, Hono BFF) — `requireUser`, forwards with the service token

| method | BFF path | forwards to edge_api | source |
|---|---|---|---|
| GET | `/api/v1/documenso-template-defaults` | `GET /api/v1/documenso-template-defaults` | `routes/documenso-template-defaults.ts` |
| POST | `/api/v1/documenso-template-defaults` | `POST /api/v1/documenso-template-defaults` | `routes/documenso-template-defaults.ts` (validates `documensoId` is an integer) |
| GET | `/api/v1/deals/:handle/details` | `GET /api/v1/deals/{handle}/details` | `routes/deals-admin.ts:83` (snake→camel mapping) |
| PUT | `/api/v1/deals/:handle/details` | `PUT /api/v1/deals/{handle}/details` | `routes/deals-admin.ts:96` (`templateDocumensoId` int or null) |
| POST | `/api/v1/deals/:handle/originate` | `POST /api/v1/deals/{handle}/originate` | `routes/deals-admin.ts:135` (`edgeOriginateDeal`) |

Registered in `apps/platform-api/src/index.ts` (`documensoTemplateDefaultRoutes` at `:145`,
`dealAdminRoutes` brokered to `/api/v1/deals`). The BFF does NO field mapping for the template-defaults
surface — edge_api's snake_case flows straight through; the deals routes map snake→camel.

**Orphaned legacy route, still registered:** `index.ts:129` `app.route("/api/v1/documenso-templates",
documensoTemplateRoutes)` (`routes/documenso-templates-admin.ts`) — the LEGACY registry picker, now
superseded by `documenso-template-defaults`. Still wired but no live UI uses it on this path (see OPEN
WORK).

---

## Operator UI surfaces (rare-structure-hq)

| surface | route (`App.tsx`) | component | role |
|---|---|---|---|
| **Set Template as Default** | `settings/documenso/templates` (`App.tsx:250`) | `DocumensoTemplatesManage.tsx` | the mirror-backed default picker. Lists every mirror template (`type='template'`, non-deleted) via `documenso-template-defaults`; the Default column marks ONE as the operator's Confirm & Originate default. Title: "Set Template as Default"; description: "Every Documenso template from the live mirror. Mark one as the Confirm & Originate default." (`DocumensoTemplatesManage.tsx:86-87`). Distinct from the legacy registry — mirror-path templates like 14503 aren't in it. |
| **Deal Details** | `deals/:handle` (`App.tsx:181`) | `DealDetails.tsx` | the deal config editor. Mirror template dropdown (`templateDocumensoId`) + per-field prefill OVERRIDE inputs. On template select it fetches the template's fields + per-label config defaults (`getPrefillConfig`); fields split into **term fields** (`required && readOnly` — operator terms) and **prospect fields** (the rest) (`DealDetails.tsx:105-124`). Saves `{contacts, fieldValues, templateDocumensoId}` via PUT details. |
| **Mandate** | `m/:handle` (`App.tsx:191`) | `Mandate.tsx` | the originate surface. Reads the mirror attach (`templateDocumensoId`, keyed only by the deal handle), shows the bound signatory ("Prepared for"), blocks Originate if no template is attached, and on Originate calls `originateDeal` → renders the sign link `${origin}${originated.signLink}` (i.e. `/p/m/{handle}/{document_id}`) and the document id/status (`Mandate.tsx:90,100,107,182`). |

SPA api-clients: `apps/platform-app/src/settings/documenso-template-defaults-api.ts`,
`documenso-template-prefill-api.ts` (consumed by Deal Details for the per-label config defaults).

---

## The prefill model (B) — unchanged contract, now CONSUMED

Model B (resolve-at-read, no copy-on-attach), as agreed in doc 11 — but doc 11 only stored the config;
this layer **consumes** it. The override store moved from `deal_details.field_values` to
`deal_document_configs.field_values`:

```
per field LABEL:
   value = deal_document_configs.field_values[label]                         (operator override)
        ?? documenso_template_document_prefill_configs.field_settings[label]
              .default_document_field_value                                  (config default)
   locked  = field_settings[label].read_only == true                         (operator terms LOCK)
   editable = (prefilled labels) − (locked labels)                           (prospect facts stay open)
```

There is no copy-on-attach, so nothing drifts. Stated in `sql/deal_document_configs.sql` header and
`deals/originate.py:1-12`. **Phase 2 (`source` binding) is NOT built** — see OPEN WORK.

---

## Worked example — template 14503 / deal `013ca823` (verified live)

The TEST DEAL (verified live): `business.deals` `deal_handle='013ca823'`, `company_name="Environmental
Logistics"`, `status='draft'`, id `013ca823-a253-47df-ab74-7f7edb3e25d9`.

**Its `deal_document_configs` rows (append-only history, verified live):** 3 rows — 2 `archived`, 1
`active`. The active row (`ee0fd1ed-…`, `template_documenso_id=14503`) carries `field_values`:

```json
{ "IntroNum": "30", "PrepaidFee": "$45,000", "PricePerIntro": "$1500" }
```

These are the operator OVERRIDES. The earlier archived rows show the append-only mechanics: row 1 attached
14503 with empty `{}` overrides → row 2 detached (`template_documenso_id=null`) → row 3 re-attached 14503
WITH the overrides above. Each template switch archived the prior active row and inserted a new one.

**The prefill config for 14503 (verified live, `field_settings`):**

| label | default_document_field_value | read_only |
|---|---|---|
| Legal Entity Name | `""` | false |
| D/B/A Name | `""` | false |
| Full Name | `""` | false |
| Title | `""` | false |
| IntroNum | `12` | **true** |
| PricePerIntro | `$3,000.` | **true** |
| PrepaidFee | `$36,000` | **true** |
| Duration | `90` | **true** |

**Model B resolution at originate** (config default `??` deal override):
- `IntroNum`: config default `12`, deal override `30` → resolves **`30`**, LOCKED.
- `PricePerIntro`: config default `$3,000.`, deal override `$1500` → resolves **`$1500`**, LOCKED.
- `PrepaidFee`: config default `$36,000`, deal override `$45,000` → resolves **`$45,000`**, LOCKED.
- `Duration`: config default `90`, no deal override → resolves **`90`** (the config default — model B's
  fall-through), LOCKED.
- The 4 prospect facts (`Legal Entity Name`, `D/B/A Name`, `Full Name`, `Title`): empty default, no
  override → not prefilled, left EDITABLE for the prospect.

**The minted document (verified live in the mirror).** `business.documenso_envelopes WHERE
external_id='013ca823' AND type='document'` — the cycle minted **3** documents (re-originate runs); the
one this doc's reference call cited:

| documenso_id | envelope_id | title | status | template_documenso_id | external_id |
|---|---|---|---|---|---|
| **1520372** | **envelope_wkaawrhivthkylum** | Engagement Agreement | pending | 14503 | 013ca823 |
| 1520425 | envelope_tucybumhmukyusma | Engagement Agreement | pending | 14503 | 013ca823 |
| 1520428 | envelope_crcnniaulcmctoia | Engagement Agreement | pending | 14503 | 013ca823 |

Its derived fields (verified off the minted docs' `documenso_response.fields`): the 4 operator terms are
`readOnly=true` with the resolved values baked into `fieldMeta.text` — confirmed on the mirror:
`Duration → 90`, `PricePerIntro → $1500`, `PrepaidFee → $45,000`, `IntroNum → 30` — model B confirmed
end-to-end (Duration fell to the config default; the other three took the deal overrides). The 4
prospect-fact TEXT fields (`Legal Entity Name`, `D/B/A Name`, `Full Name`, `Title`) are `readOnly=false`
with empty text — left open for the signer. Recipients: the prospect slot resolved to the signatory
contact (`Eddie Andrus`, `benjamin.crane+eddieandrus@engineereddemand.com`, role SIGNER); the operator
slot (`Provider`) is the fixed second recipient.

**Sign link:** `/p/m/013ca823/1520372`.

(Cross-ref doc 11: on the SOURCE template 14503 every value field is `read_only=false` — the lock is
applied per-config on the DERIVED document, never the template. The minted doc above is the proof.)

---

## OPEN WORK / known limitations

1. **Prospect-recipient single-prospect-template assumption.** `derive_prospect_recipient_id` binds the
   prospect to the **most-common `recipientId`** among the template's labelled value fields. Documented
   verbatim in `deals/originate.py:59-61`:

   > "ASSUMES value fields cluster on ONE prospect recipient (true for the current single-prospect
   > templates). If a template ever puts a LARGER cluster of labelled value fields on a SECOND recipient
   > (e.g. an operator slot added via 'Add Myself'), most-common would bind the wrong slot — persist an
   > explicit prospect recipient id on the config/mirror and read it instead at that point."

   Until then, a multi-prospect / operator-fields-on-a-second-recipient template can mis-bind. Fix:
   persist an explicit prospect recipient id on the config or mirror.

2. **Phase-2 `source` binding — NOT BUILT.** The prefill config's `field_settings[label].source` key
   (binds a field → deal data, e.g. Full Name / Title / Company Name auto-filled from
   `deal_contacts`→`contacts` and `deals.company_name`/`company_domain`) round-trips verbatim through the
   API but **nothing resolves it**. Today the prospect fills those facts manually (they resolve as empty
   defaults → editable fields). Wire `source` into the resolver (`resolve_field_values`) and the Deal
   Details editor to auto-fill prospect facts from deal data. No DDL/API change needed — `field_settings`
   already passes `source` through.

3. **`business.documenso_templates` (legacy registry) cannot be dropped yet.** The deal originate path no
   longer touches it, but it is still referenced by:
   - **`engagement_mappings`** (`engagement_mappings/queries.py:26` joins `business.documenso_templates dt`).
   - The orphaned legacy **`/api/v1/documenso-templates`** route — edge_api `routers/documenso_templates_v1.py` + the BFF `routes/documenso-templates-admin.ts` (still registered at `index.ts:129`).
   - **`scripts/rs_capital_origination_generate.py`** (creates real Documenso templates and writes `documenso_templates` rows — `:199`, `:194`).

   Drop only after these three are migrated/retired.

4. **`business.deal_details` is RETIRE-ABLE.** Superseded by `deal_document_configs`, no longer on the
   deal document/originate path (verified: only stale doc-comment references remain in
   `deals/models.py:1` and `deals/__init__.py:1`). Still holds 3 rows live. A destructive `DROP` must NOT
   go in the idempotent boot DDL — it needs a one-off manual terminal run after confirming nothing else
   reads it. (Same pattern doc 11 noted for `documenso_template_configs`.)

5. **Carry-over from doc 11 (still open):** `business.documenso_envelopes_orphan` (1 row) and the empty
   `business.documenso_template_configs` (0 rows) are still droppable one-offs.

---

## Secrets / deploy / how to query

**Secrets (Doppler `core-x/prd`)** — unchanged from doc 11. Load-bearing for this layer:

| key | value / use |
|---|---|
| `HQX_DB_URL_POOLED` | HQX Postgres (schema `business`) — the operational SoR |
| `DOCUMENSO_API_KEY` | Documenso API bearer (the engine's `/template/use`, `/envelope/field/update-many`, `/envelope/distribute`) |
| `DOCUMENSO_API_URL` | `https://app.documenso.com` (returned as `documenso_host` in the originate response) |
| `EDGE_API_SERVICE_TOKEN` | the BFF→edge_api service token |
| `EDGE_API_BASE_URL` | `https://api.edgeapi.run` |

**Deploy:** edge_api on **Railway**, auto-redeploy on merge to `main`. `apps/edge_api/sql/*.sql` applied
idempotently at boot by `src/migrate.py`. New this cycle: `sql/deal_document_configs.sql`,
`sql/documenso_template_defaults.sql` — applied with zero wiring.

**Read-only query recipe (every live fact in this doc was verified this way):**

```bash
doppler run --project core-x --config prd -- python3 - <<'PY'
import os, json, psycopg
with psycopg.connect(os.environ["HQX_DB_URL_POOLED"]) as conn, conn.cursor() as cur:
    # the test deal's active document config (template + overrides)
    cur.execute("""
        SELECT template_documenso_id, field_values, status, created_at
        FROM business.deal_document_configs
        WHERE deal_id = (SELECT id FROM business.deals WHERE deal_handle='013ca823')
        ORDER BY created_at
    """)
    for r in cur.fetchall():
        print(r)
    # the minted document(s) in the mirror, keyed by deal_handle
    cur.execute("""
        SELECT documenso_id, envelope_id, title, status, template_documenso_id, external_id
        FROM business.documenso_envelopes
        WHERE external_id='013ca823' AND type='document'
        ORDER BY created_at
    """)
    for r in cur.fetchall():
        print(r)
    # the resolved + locked fields on the minted doc (model B proof)
    cur.execute("""
        SELECT fld->'fieldMeta'->>'label' AS label,
               (fld->'fieldMeta'->>'readOnly')::bool AS read_only,
               fld->'fieldMeta'->>'text' AS text_value
        FROM business.documenso_envelopes env
        CROSS JOIN LATERAL jsonb_array_elements(env.documenso_response->'fields') fld
        WHERE env.documenso_id=1520372 AND fld->>'type' IN ('TEXT','NUMBER')
        ORDER BY read_only, label
    """)
    for r in cur.fetchall():
        print(r)
    # the prefill config that drives resolution
    cur.execute("""
        SELECT field_settings
        FROM business.documenso_template_document_prefill_configs
        WHERE template_documenso_id=14503
    """)
    print(json.dumps(cur.fetchone()[0], indent=2))
PY
```

**Keep it READ-ONLY** — no INSERT/UPDATE/DROP against prod from a query session, and do NOT originate
(it mints real Documenso documents).
