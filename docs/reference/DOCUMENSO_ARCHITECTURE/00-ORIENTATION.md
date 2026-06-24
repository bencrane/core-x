# 00 — ORIENTATION: The Documenso / Document / Payment System (read me FIRST)

> **PURPOSE.** Master index for the entire Documenso + document + payment system spanning **core-x
> edge_api** and **rare-structure-hq platform**. Read this in full before touching any sibling file
> (01–09). Its job: in under two minutes, stop a fresh AI agent from operating on a wrong mental model.
> Every claim carries a `path:line` citation that was opened and read. core-x is cited
> `apps/edge_api/...:NN`; platform is cited `rare-structure-hq:apps/...:NN`. Where a code comment and the
> code disagree, **the code wins** (the comment is flagged stale). Where the older repo-root
> `docs/reference/DIRECT_TO_DOCUMENSO_PAYMENT_E2E.md` disagrees with the code, **the code wins**; treat
> that doc as a historical starting point, not ground truth (09 §5).

---

## The one fact that drives everything

**There is no single dispatcher and no single flow. There are MULTIPLE parallel originate / sign / pay
flows, selected by TWO independent `public.operator_settings` columns that are consumed in THREE
different places by THREE different mechanisms.**

| Selector | Values (DB default first) | Consumed where | Mechanism | Cite |
|---|---|---|---|---|
| `render_mode` | `'through-docraptor'`, `'direct-to-documenso'` | proposal **confirm** (`_provision`) | **server-side** branch | `apps/edge_api/sql/operator_settings.sql:41`; `apps/edge_api/src/routers/proposals_v1.py:99` |
| `direct_to_documenso_lane` | `'envelope-distribute'` (RETIRED), `'prefill-document-from-template'` (DEFAULT), `'embed-template'` (NEW) | SPA `MandateDraftShell.confirm()` | **client-side** endpoint pick (never branched server-side) | `apps/edge_api/sql/operator_settings.sql:84-90`; `apps/edge_api/src/operator_settings/models.py:21-23`; `rare-structure-hq:apps/platform-app/src/proposals/MandateDraftShell.tsx:93` |
| `stripe_mode` | `'test'`, `'live'`, `NULL` | document-payment mint + Stripe webhook | **server-side** resolution (orthogonal) | `apps/edge_api/sql/operator_settings.sql:43`; `apps/edge_api/src/routers/document_payments_v1.py:96` |

The single most dangerous misconception: **`render_mode = 'direct-to-documenso'` is a STUB** on the
proposal path (`apps/edge_api/src/routers/proposals_v1.py:99-103`, returns
`(False, "direct-to-documenso pathway not yet wired")`), while the **actual working direct flow is a
SEPARATE lane** (`MandateDraftShell` → `engagement-mandate-drafts`) that branches on
`direct_to_documenso_lane` and **never reads `render_mode`** (0 `render_mode` matches in
`MandateDraftShell.tsx`, 09 §1). They share an enum string and nothing else. Full detail in **09 §1**.

`edge_api` is `FastAPI(title="edge_api", version="0.4.0")` (`apps/edge_api/main.py:146`). Schema is applied
**as code at boot** — `run_migrations()` globs `sql/*.sql` and applies each file in filename order, one
transaction per file under `pg_advisory_xact_lock`; a failure FAILS THE BOOT
(`apps/edge_api/src/migrate.py:64`). There is no migration framework.

---

## Repo topology

```
rare-structure-hq (PLATFORM)                    core-x (DATA / CONTROL PLANE)
┌──────────────────────────┐                    ┌────────────────────────────────────────┐
│ apps/platform-app  (SPA) │  fetch (Bearer or  │ apps/edge_api  (FastAPI)                 │
│   React cockpit +        │  PUBLIC pair/ref)  │   SINGLE WRITER over HQX Postgres        │
│   prospect /p/* pages    │ ─────────────────► │   ONLY caller of Stripe + Documenso      │
│ apps/platform-api (BFF)  │  service-token     │   owns business.* + public.operator_     │
│   Hono — DUMB pass-thru  │ ─────────────────► │   settings; schema applied AS CODE at    │
│ packages/shared (types)  │                    │   boot (sql/*.sql, no migration tool)    │
└──────────────────────────┘                    └────────────────────────────────────────┘
```

**Architecture invariant: `platform-app → platform-api → edge_api`.** The SPA never calls `edge_api`
directly. The BFF is a **dumb pass-through**: it validates the operator's Supabase JWT, attaches
`EDGE_API_SERVICE_TOKEN` on operator surfaces (nothing on PUBLIC prospect surfaces), remaps a couple of
path shapes, and forwards — **no business logic, no DB for these flows**
(`rare-structure-hq:apps/platform-api/src/routes/settings.ts:63`, `:74`;
`rare-structure-hq:apps/platform-api/src/lib/edge.ts:34-35`).

- `edge_api` is the **single writer** over the hq-x control-plane Postgres (`HQX_DB_URL_POOLED`,
  `business.*` + `public.operator_settings`) and the **only caller** of Stripe and Documenso
  (`apps/edge_api/src/migrate.py:64`; 07 Orientation).
- **ONE live exception to the invariant:** the proposal-confirm BFF route reads
  `operator_settings.render_mode` **directly via the Supabase service-role client**, bypassing the
  `edge_api` gateway (`rare-structure-hq:apps/platform-api/src/routes/proposals-admin.ts:132-137`;
  `rare-structure-hq:apps/platform-api/src/lib/db.ts:15`). This is the only surviving direct BFF DB read
  for this domain. The "BFF no longer touches `operator_settings`" claim is scoped to the **settings-tab
  persistence flow only** (08 §settings; 09 §8.2).

---

## Flow selector matrix

`(render_mode, direct_to_documenso_lane)` → which flow runs, which sibling doc to read, and its status.
`stripe_mode` is orthogonal (it does not select a flow; it picks Stripe keys).

| render_mode | direct_to_documenso_lane | Flow that runs | Originate route (edge_api) | Read | Status |
|---|---|---|---|---|---|
| `through-docraptor` (DEFAULT) | (ignored) | DocRaptor PDF → Documenso `/envelope/create` envelope; legacy `/p/:ref` surfaces | `POST /api/v1/proposals/{ref}/confirm` → `_provision` through-docraptor (`apps/edge_api/src/routers/proposals_v1.py:104`) | **02** | **ACTIVE / DEFAULT** |
| `direct-to-documenso` | (proposal-confirm path) | **STUB** — no PDF, no envelope; draft row survives, "not yet wired" | `_provision` direct branch (`apps/edge_api/src/routers/proposals_v1.py:99-103`) | **02**, **09 §1** | **STUB** |
| `direct-to-documenso` | `envelope-distribute` | Documenso `/envelope/use`; `externalId=draft_id`; returned only `envelope_id`; **could not build `/p/m` pair** → dead-ended prospect flow | **RETIRED** — the `/envelope/use` + `.../{id}/confirm` lane was removed in code; the CHECK still accepts the value so a pre-existing row never violates it, but **no live route serves it** (`apps/edge_api/sql/operator_settings.sql:31-34`, `:84-90`; no `/confirm` endpoint in `apps/edge_api/src/routers/engagement_mandate_drafts_v1.py`) | **03 Lane A**, **04 Lane B** | **RETIRED** |
| `direct-to-documenso` | `prefill-document-from-template` (DEFAULT) | Documenso `/template/use`; `externalId=`8-char handle; returns `(opportunity_id, document_id)`; locks fields read-only; builds `/p/m/{opp}/{doc}` — **the canonical live flow** | `POST /api/v1/engagement-mandate-drafts/{draft_id}/originate-prefilled` (`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:113`) | **03 Lane B**, **04 Lane C** | **ACTIVE / CANONICAL / DEFAULT** |
| `direct-to-documenso` | `embed-template` (NEW) | Documenso template DIRECT LINK; `create_direct_link` returns a reusable `direct_token` + `embed_url`; `externalId=`8-char handle; **NO document minted at originate** — the signer self-identifies in the embed and Documenso creates the document (source `TEMPLATE_DIRECT_LINK`) at completion; `status='ready'` | `POST /api/v1/engagement-mandate-drafts/{draft_id}/originate-embed-template` (`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:172`) | **04** | **ACTIVE / NEW** |
| (any) | (any) | Document fee payment (DUAL-RAIL `card`+`us_bank_account`), `(opp,doc)`-keyed | `POST /api/v1/documenso/payment-intent/{opp}/{doc}` (`apps/edge_api/src/routers/document_payments_v1.py:84`) | **05 Lane B** | **ACTIVE** |
| `through-docraptor` | — | Legacy engagement payment (ACH-only `us_bank_account`), proposal-ref-keyed | `POST /api/v1/proposals/{ref}/payment-intent` (`apps/edge_api/src/routers/payments_v1.py:36`) | **05 Lane A** | **ACTIVE** (legacy) |

> Separate, ungated by these selectors: the **AO `engagement_docs`** render pathway (Stage → DocRaptor →
> R2 → **DRAFT** Documenso `DOCUMENT`, never distributed) was previously **BROKEN at HEAD** — it has since
> been **REMOVED**: no `apps/edge_api/src/engagement_docs/` module, no `engagement_mandates_v1.py` router,
> and no `apps/edge_api/sql/ao_engagement_mandates.sql` exist on current main (grep: zero matches). The
> live render lane is now the **render+push** lane (06; `internal_engagement_templates_v1.py`). See **06**.

---

## Which flow am I in?

A decision tree keyed on what you can observe (URL/ref shape, externalId shape, `metadata.kind`).

```
1. What URL is the PROSPECT on?
   /p/:ref           (ref looks like "rs_…")     → LEGACY through-docraptor proposal flow → 02
   /p/m/:opp/:doc    (8-char handle / numeric)   → NEW direct prefill flow → 03 Lane B + 04 Lane C
   /p/t/:opp/:token  (8-char handle / direct token) → NEW embed-template direct-link flow (no doc until signer completes) → 04
   /p/m/.../pay                                   → document payment (dual-rail) → 05 Lane B + 08
   /p/:ref/pay                                    → legacy engagement payment (ACH-only) → 05 Lane A

2. externalId shape (on the Documenso envelope / in documenso_webhook_events.external_id)?
   "rs_" + token                                 → proposal lane (02); new_ref() (apps/edge_api/src/proposals/queries.py:40-42)
   8 hex chars (e.g. "7bbf1081")                 → prefill lane OR embed-template lane; the opportunity PUBLIC handle (03 Lane B; 04). For embed-template the externalId is stamped client-side by the embed, and no document exists until the signer completes.
   a full UUID (= draft_id)                       → envelope-distribute lane (RETIRED) — DEAD-ENDED, no /p/m pair

3. Stripe metadata.kind on the PaymentIntent / webhook event?
   "document"                                    → Lane B document payment → _handle_document_payment
                                                    (apps/edge_api/src/routers/webhooks_stripe.py:73)
   "engagement" (or anything else)               → Lane A legacy proposal payment (05)

4. Component name?
   Mandate* (MandateDraftShell, MandateEditor)   → cockpit AUTHORING (operator-facing) (08; 09 §6)
   Document* (DocumentSignPage, DocumentPaymentPage) → PROSPECT views (08; 09 §6)
   Applications.tsx "Stage" → engagement-mandates → the BROKEN AO engagement_docs lane (06 Part A)
```

---

## Active vs deprecated map

ACTIVE = runs in the live flow. CONDITIONAL = only under a specific mode/lane. DEPRECATED = exists but no
longer receives traffic. STUB = present but not wired.

| Component | Status | Citation |
|---|---|---|
| `public.operator_settings` table + 3 columns + 3 CHECK constraints | **ACTIVE** | `apps/edge_api/sql/operator_settings.sql:39-94` |
| `GET/PUT /api/v1/operator-settings/{auth_user_id}` (edge settings gateway) | **ACTIVE** | `apps/edge_api/src/routers/operator_settings_v1.py:34`, `:43` |
| `_provision` `through-docraptor` branch (PDF → envelope) | **ACTIVE / DEFAULT** | `apps/edge_api/src/routers/proposals_v1.py:104` |
| `_provision` `direct-to-documenso` branch | **STUB** | `apps/edge_api/src/routers/proposals_v1.py:99-103` |
| `envelope-distribute` lane (`/confirm`, `/envelope/use`) | **RETIRED** (no route serves it; CHECK retains the value) | `apps/edge_api/sql/operator_settings.sql:31-34`; no `/confirm` in `apps/edge_api/src/routers/engagement_mandate_drafts_v1.py` |
| `prefill-document-from-template` lane (`/originate-prefilled`, `/template/use`) | **ACTIVE / CANONICAL / DEFAULT** | `apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:113` |
| `embed-template` lane (`/originate-embed-template`, `/template/direct/create`) | **ACTIVE / NEW** | `apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:172` |
| `create_document_from_template` (Lane C, `/template/use`; embed-document, mint now) | **ACTIVE** (canonical originate) | `apps/edge_api/src/services/documenso_client.py:228` |
| `create_template_from_pdf` + `TemplateCreateResult` (`/api/v2/envelope/create` type=TEMPLATE; render+push terminal step) | **ACTIVE** | `apps/edge_api/src/services/documenso_client.py:420`, `:410` |
| `create_direct_link` / `toggle_direct_link` / `get_template_recipients` (`/api/v2/template/direct/{create,toggle}`, `/api/v2/template/{id}`) + `DirectLinkResult` | **ACTIVE / NEW** | `apps/edge_api/src/services/documenso_client.py:514`, `:542`, `:504`, `:469` |
| `POST /internal/engagement-templates/render-push` (trigger-secret) + `push.render_and_push()` | **ACTIVE / NEW** | `apps/edge_api/src/routers/internal_engagement_templates_v1.py:84`; `apps/edge_api/src/engagement_templates/push.py:63` |
| `engagement-template-push` Trigger.dev task (calls render-push via `callHqx`) | **ACTIVE / NEW** | `src/trigger/engagement_template_push.ts:53` |
| `ops.engagement_template_push_runs` ledger (terminal row per push attempt) | **ACTIVE / NEW** | `apps/edge_api/sql/ops_engagement_template_push_runs.sql:12` |
| `business.global_input_content` content-source REGISTRY (`brand` + `source_kind`) | **ACTIVE / NEW** | `apps/edge_api/sql/global_input_content.sql:21`, `:31-32` |
| `POST /api/v1/documenso/webhook` + `business.documenso_webhook_events` (raw landing, SoR) | **ACTIVE** | `apps/edge_api/src/routers/documenso_webhooks_v1.py:39` |
| `POST /api/v1/proposals/webhook` (legacy Documenso projection) | **DEPRECATED** (functional, no traffic) | `apps/edge_api/src/routers/proposals_v1.py:337` |
| `GET /api/v1/documenso/sign-state/{opp}/{doc}` (offline poll) | **ACTIVE** | `apps/edge_api/src/routers/documenso_webhooks_v1.py:76` |
| `GET /api/v1/documenso/sign-token/{opp}/{doc}` (one live read, client-vs-originator) | **ACTIVE** | `apps/edge_api/src/routers/documenso_webhooks_v1.py:105` |
| Document payment mint (DUAL-RAIL `["card","us_bank_account"]`) | **ACTIVE** | `apps/edge_api/src/document_payments/stripe.py:87` |
| Legacy engagement payment mint (ACH-only `["us_bank_account"]`) | **ACTIVE** (legacy) | `apps/edge_api/src/payments/stripe_client.py:70` |
| `POST /webhooks/stripe` (single router, both lanes, multi-secret) | **ACTIVE** | `apps/edge_api/src/routers/webhooks_stripe.py:49` |
| Trigger.dev post-payment fulfillment seams (×2) | **STUB** (intentional) | `apps/edge_api/src/routers/webhooks_stripe.py:107`, `:162` |
| `engagement_docs` AO render lane (`engagement-mandates` POST + `/internal/.../render`) | **REMOVED** (module, router, and `ao_engagement_mandates.sql` all deleted; grep zero matches) | superseded by render+push (`apps/edge_api/src/routers/internal_engagement_templates_v1.py:84`) |
| Documenso template-fields/defaults editor, engagement-mappings picker, engagement-templates render, archetypes | **ACTIVE** | `apps/edge_api/src/routers/documenso_template_fields_v1.py:40`; `engagement_mappings_v1.py:29`; `engagement_templates_v1.py:30`; `engagement_archetypes.sql:19` |
| engagement-template catalog — brand-aware (`active-operators` + `rare-structure`), `<brand>/<path>/<archetype>/<version>/global_engagement_content` | **ACTIVE** (2 brands: `_ALLOWED_BRANDS`) | `apps/edge_api/src/engagement_templates/catalog.py:28`; `apps/edge_api/content/rare-structure/docraptor-to-documenso-template/capital-origination/v1/` |
| `/p/m/:opportunityId/:documentId` `DocumentSignPage` / `.../pay` `DocumentPaymentPage` | **ACTIVE** | `rare-structure-hq:apps/platform-app/src/App.tsx:100`, `:103` |
| `/p/:ref` `SummaryPage` / `/p/:ref/sign` `SignPage` / `/p/:ref/pay` `PaymentPage` | **ACTIVE** (legacy through-docraptor generation) | `rare-structure-hq:apps/platform-app/src/App.tsx:96`, `:105`, `:107` |
| BFF `documensoPublicRoutes` (4 PUBLIC pair routes, dumb pass-through) | **ACTIVE** | `rare-structure-hq:apps/platform-api/src/routes/documenso-public.ts:37`, `:64`, `:90`, `:118` |
| BFF `/api/v1/engagement-mandate-drafts` alias mount of `documensoPublicRoutes` | **CONDITIONAL** (transitional) | `rare-structure-hq:apps/platform-api/src/index.ts:124` |
| BFF `proposals-admin.ts` direct Supabase `operator_settings.render_mode` read | **ACTIVE** (legacy exception to invariant) | `rare-structure-hq:apps/platform-api/src/routes/proposals-admin.ts:132-137` |
| `GET /api/v1/engagement-mandate-drafts/document/{envelope_id}` (envelope-distribute prospect read) | **RETIRED** (route removed with the envelope-distribute lane) | no such route in `apps/edge_api/src/routers/engagement_mandate_drafts_v1.py` |
| `operator_settings` RLS | **DEPRECATED-as-boundary** (ENABLED, not load-bearing) | `apps/edge_api/sql/operator_settings.sql:96-99` |
| `MandateSignPage` (name) | **DEPRECATED** (0 matches; renamed `DocumentSignPage`) | `apps/edge_api/src/routers/documenso_webhooks_v1.py:84` (last surviving reference) |
| `business.mandate_payments` / `business.mandate_payment_events` | **NONEXISTENT** | grep zero matches (07 "Proven-nonexistent"; 09 §3) |

---

## Glossary

- **8-char handle** (`business.opportunities.opportunity_id`): the PUBLIC access capability,
  `GENERATED ALWAYS AS (LEFT(id::text, 8)) STORED`, non-unique BTREE index, 8 hex = 32 bits. It is the
  Documenso `externalId` in the prefill lane and the `{opportunity_id}` segment of `/p/m/...`
  (`apps/edge_api/sql/opportunities_opportunity_id.sql:19-21`).
- **row UUID** (`business.opportunities.id`): the internal PK / FK target — NOT externally visible. It is
  the JOIN target for per-deal content (`opportunity_specific_content.opportunity_id = o.id`), the OPPOSITE
  carrier of `document_payments.opportunity_id`, which carries the handle (07 "two opportunity identifiers";
  `apps/edge_api/src/document_payments/queries.py:47-53`). (`ao_engagement_mandates`, the former row-UUID
  carrier, has been REMOVED — grep zero matches.)
- **envelope_id** (prefixed `envelope_…`): the Documenso v2 envelope handle; accepted by
  `/api/v2/envelope/*` and `/api/v2/template/*` (`apps/edge_api/src/services/documenso_client.py:303-318`);
  **400s on `/api/v2/document/{id}`** (`apps/edge_api/src/services/documenso_client.py:620-629`).
- **document_id** (numeric, e.g. `1462137`): Documenso's numeric document id; required by
  `/api/v2/document/{id}` and the signed-PDF download; **400s on the envelope endpoints**. It is the
  `{document_id}` segment of `/p/m/...` and the unique pin behind the handle
  (`apps/edge_api/src/services/documenso_client.py:620-629`).
  **TRAP:** the `business.documenso_webhook_events.envelope_id` *column* actually holds this NUMERIC
  document id, despite its name (`apps/edge_api/src/documenso_webhooks/queries.py:71`).
- **externalId**: the value stamped on the Documenso envelope at originate. Shapes disambiguate lanes in
  the single raw webhook table: `rs_…` (proposal lane), 8-char handle (prefill lane — stamped server-side
  at originate; AND embed-template lane — stamped client-side by the embed at signer completion), full
  UUID = draft_id (envelope-distribute lane, RETIRED) (04 §8;
  `apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:149` (prefill), `:215` (embed-template)).
- **sign-token vs sign-state**: `GET /api/v1/documenso/sign-token/{opp}/{doc}` makes ONE live Documenso read
  to fetch the embed token (with two-recipient client-vs-originator selection); `GET
  /api/v1/documenso/sign-state/{opp}/{doc}` is FULLY OFFLINE — derives `signed` from raw
  `documenso_webhook_events` rows, zero Documenso calls
  (`apps/edge_api/src/routers/documenso_webhooks_v1.py:105`, `:76`).
- **the lanes** (overloaded "lane"/"template"): `render_mode` lanes = through-docraptor vs direct. Direct
  sub-lanes = `prefill-document-from-template` (DEFAULT, embed-document — mint a Documenso document now)
  vs `embed-template` (NEW — enable a template direct link, document created at signer completion) vs
  `envelope-distribute` (RETIRED). Documenso originate "lanes" in the client = A `/envelope/create`
  (RETIRED), B `/envelope/use` (RETIRED), C `/template/use` (`create_document_from_template`,
  `apps/edge_api/src/services/documenso_client.py:228`), D `/template/direct/create`
  (`create_direct_link`, `:514`) (04). A Documenso **TEMPLATE** (a v2 envelope you instantiate) ≠ an
  **engagement-template** (repo HTML rendered to a standalone PDF, then pushed as a Documenso TEMPLATE
  via the render+push lane) (06).
- **direct-link token** (`embed-template` lane): a reusable, self-identifying signing token enabled on a
  Documenso TEMPLATE via `POST /api/v2/template/direct/create {templateId, directRecipientId?}` →
  `{token, ...}`. ONE value with three representations: the `MandateEmbedTemplateOriginated.direct_token`
  API field, the `<EmbedDirectTemplate token=…>` SPA prop, and the public `/d/{token}` URL / iframe
  `/embed/direct/{token}`. The signer enters their OWN name + email (name/email are NOT locked); Documenso
  creates the document (source `TEMPLATE_DIRECT_LINK`) at completion, so NO document exists at originate
  (`status='ready'`). `externalId` (the opportunity's 8-char handle) is stamped by the embed in
  JavaScript, not at mint (`apps/edge_api/src/services/documenso_client.py:459-465`, `:514-540`;
  `apps/edge_api/src/engagement_mandate_drafts/models.py:49-72`).

---

## File index

| File | Read this when… |
|---|---|
| **01-MODES-AND-LANES.md** | You need the selector layer: `operator_settings` columns/CHECKs, the merge-upsert (COALESCE-of-existing, NOT EXCLUDED) semantics, the three-layer enum lockstep, and the routing table mapping each `(render_mode, lane)` tuple → flow. |
| **02-FLOW-through-docraptor.md** | You're on the `through-docraptor` default proposal flow: create → confirm → `_provision` (DocRaptor PDF → `/envelope/create`, anchor fields by `findText`) → `/p/:ref` sign → webhook status. Also flags the direct-to-documenso STUB inside `_provision`. |
| **03-FLOW-direct-to-documenso.md** | You're on `render_mode='direct-to-documenso'` and need BOTH sub-lanes side-by-side: the divergence table (externalId, prefill source, recipient binding, read-only lock, `(opp,doc)` pair) for `envelope-distribute` vs `prefill-document-from-template`. |
| **04-DOCUMENSO-INTEGRATION.md** | You need the edge_api Documenso v2 client internals: the live originate lanes (C `/template/use` `create_document_from_template`; D `/template/direct/create` `create_direct_link` + `toggle_direct_link` + `get_template_recipients`), the template-create-from-PDF method (`create_template_from_pdf`/`TemplateCreateResult`), the token-extraction helpers, read/download, the raw webhook capture, and the pair-gated sign reads (offline state + live token). Lanes A `/envelope/create` and B `/envelope/use` are RETIRED (methods removed). |
| **05-PAYMENTS.md** | You're on Stripe: document payments (dual-rail, `(opp,doc)`-keyed) vs legacy engagement payments (ACH-only, ref-keyed), the single `/webhooks/stripe` router dispatching by `metadata.kind`, multi-secret verification, and the webhook-only `paid` rule. |
| **06-ENGAGEMENT-DOCS-AND-TEMPLATES.md** | You're touching the AO `engagement_docs` render lane (REMOVED — module/router/`ao_engagement_mandates.sql` deleted) OR the Documenso TEMPLATE layer (defaults editor, mappings picker, archetypes, standalone engagement-templates render) OR the render+PUSH lane: brand-aware catalog (`active-operators` + `rare-structure`), the `business.global_input_content` content-source registry (`brand` + `source_kind`), `push.render_and_push()` (content → DocRaptor → Documenso TEMPLATE via `create_template_from_pdf`), `POST /internal/engagement-templates/render-push` (trigger-secret), the `engagement-template-push` Trigger.dev task, and the `ops.engagement_template_push_runs` ledger. |
| **07-DATA-STORES.md** | You need the persistence map: which table is edge_api-owned vs upstream vs read-only, the two opportunity identifiers, schema-as-code boot apply, and idempotency posture per ledger. |
| **08-FRONTEND-AND-BFF.md** | You're on the platform side: SPA routes, `MandateDraftShell` lane branch, the BFF→edge path remap (`/sign/{opp}/{doc}/{verb}` → `/{verb}/{opp}/{doc}`), the two prospect-completion models (server-poll vs browser-event), and the settings hook. |
| **09-DEPRECATED-STUBS-AND-TRAPS.md** | You need the consolidated hazard list: every stub, deprecated route, stale comment, nonexistent table, and misnomer across both repos. Read before trusting any comment. |

---

## Hard rules for agents (the traps that cause wrong work)

1. **`render_mode='direct-to-documenso'` is a STUB, not the working direct flow.** The stub is
   `proposals_v1._provision` (`apps/edge_api/src/routers/proposals_v1.py:99-103`). The working direct flow
   is `MandateDraftShell` → `engagement-mandate-drafts`, branching only on `directToDocumensoLane`
   (`rare-structure-hq:apps/platform-app/src/proposals/MandateDraftShell.tsx:93`), which never reads
   `render_mode`. Different code paths entirely (09 §1).

2. **The access capability is an 8-char HANDLE, NOT a UUID.** DDL:
   `GENERATED ALWAYS AS (LEFT(id::text, 8)) STORED` (`apps/edge_api/sql/opportunities_opportunity_id.sql:21`).
   A cluster of ~11 comments in BOTH repos call it "the opportunity UUID" — all stale. Within core-x,
   `documenso_client.py` says "UUID" (wrong) while `documenso_webhooks_v1.py` says "8-char handle" (right).
   Trust the code (09 §4).

3. **`paid` / `succeeded` / `paid_at` are WEBHOOK-ONLY on both payment lanes.** The mint writes only
   `requires_payment`; the browser confirm result NEVER sets paid. ACH settles asynchronously
   (`apps/edge_api/src/routers/webhooks_stripe.py:49`; `apps/edge_api/src/document_payments/queries.py:191`;
   05). Do not "optimize" by trusting the SPA confirm result.

4. **Stripe webhook verification is MULTI-SECRET (dual-secret).** `construct_event_any` tries every
   configured secret (`STRIPE_WEBHOOK_SECRET_TEST`, `_LIVE`, bare) because the document lane's mode is
   operator-toggleable at runtime (`apps/edge_api/src/document_payments/stripe.py:172`;
   `apps/edge_api/src/config.py:122-134`). 503 on missing secret, 400 on none-verify — never accept an
   unverified event (05).

5. **Prefill SOURCES differ per lane — the KEYING does NOT.** `envelope-distribute` reads
   `engagement_mandate_draft_content.prefill_values` (the STAGED row, NOT the freshly-minted confirm
   draft); `prefill-document-from-template` reads `opportunity_specific_content.field_values`. BOTH
   source dicts are keyed by field **LABEL**, and BOTH emit Documenso `prefillFields` keyed by field
   **ID** (resolved through the template's label→id map). The real per-lane divergence is (a) the
   source table above, and (b) Lane C fans ONE label out to MANY field ids with a base-name fallback
   (`_prefill_value_for_label`), whereas Lane B maps each label 1:1 with no fallback
   (`apps/edge_api/src/engagement_mandate_drafts/queries.py:84-89`, `:146`;
   `apps/edge_api/src/services/documenso_client.py:362-375` vs `:489-512`; 03 Traps, 04 §4.4).

6. **`envelope-distribute` has NO `(opp,doc)` pair link.** It stamps `external_id=draft_id`, returns only
   `envelope_id`, and the SPA sets `signLink=null` — the pair-gate (`external_id == opportunity_id`) is
   never satisfiable, dead-ending the prospect sign/pay surface
   (`rare-structure-hq:apps/platform-app/src/proposals/MandateDraftShell.tsx:101-105`;
   `apps/edge_api/src/routers/documenso_webhooks_v1.py:135`). The canonical flow REQUIRES the NON-default
   `prefill-document-from-template` lane (09 §7).

7. **Two payment rails — do not assume one.** Document payments are DUAL-RAIL
   `["card","us_bank_account"]` (`apps/edge_api/src/document_payments/stripe.py:87`); legacy engagement
   payments are ACH-only `["us_bank_account"]` (`apps/edge_api/src/payments/stripe_client.py:70`). The
   "ACH-only" prose in the document-payment module/SQL is STALE — CODE WINS (05 Trap 1).

8. **The live Documenso webhook is `/api/v1/documenso/webhook`, NOT `/api/v1/proposals/webhook`.** The
   proposals webhook is DEPRECATED — fully functional (verifies the secret, projects status) but receives
   no traffic; both share `DOCUMENSO_WEBHOOK_SECRET` (`apps/edge_api/src/routers/documenso_webhooks_v1.py:39`;
   `apps/edge_api/src/routers/proposals_v1.py:337`). Sign-state is derived OFFLINE from the raw table, not
   projected (09 §2).

9. **`documenso_webhook_events.envelope_id` holds the NUMERIC document id, and events are stored
   UPPERCASE_UNDERSCORE.** The column name lies; the pair-gate matches `envelope_id = document_id`. Terminal
   event is `DOCUMENT_COMPLETED` (single-element `_TERMINAL_EVENTS`), not lowercase-dotted
   (`apps/edge_api/src/documenso_webhooks/queries.py:41`, `:71`, `:95-96`; 04 Traps; 07 Trap 2).

10. **Two opposite opportunity carriers — joining the wrong one silently returns nothing.**
    `document_payments.opportunity_id` + `documenso_webhook_events.external_id` carry the **8-char handle**;
    `business.opportunities.id` is the **row UUID**. The fee query uses BOTH in one statement — the handle
    in the `WHERE` (`o.opportunity_id = %s`) and the row UUID on the JOIN leg
    (`osc.opportunity_id = o.id`) (`apps/edge_api/src/document_payments/queries.py:47-53`; 07 Trap 1).
    (The former `ao_engagement_mandates` row-UUID carrier has been REMOVED.)

11. **The BFF "no longer touches `operator_settings`" claim is scoped to the settings-tab flow ONLY.**
    `proposals-admin.ts:132-137` still reads `render_mode` from Supabase service-role on the confirm path —
    the one live contradiction of the sole-gateway invariant (08; 09 §8.2).

12. **`direct_to_documenso_lane` is a TEXT enum, never a boolean** (DB CHECK
    `apps/edge_api/sql/operator_settings.sql:84-90`; pydantic Literal
    `apps/edge_api/src/operator_settings/models.py:21-23`). There is no `= true` form (09 §8.3).

13. **`quarterly_total_cents` is NOT quarterly** — it carries `{{total}} = monthly_fee × duration`, a legacy
    name (`apps/edge_api/sql/engagement_proposals.sql:36`). On Lane A the page may DISPLAY a per-invoice
    slice while Stripe debits the WHOLE engagement total for non-`upfront_in_full` cadences (05 Trap 4; 09 §10.1).

14. **Amount is ALWAYS resolved server-side, never from the browser.** Document fee from
    `opportunity_specific_content.field_values['fee_amount']`; legacy from `quarterly_total_cents`
    (`apps/edge_api/sql/document_payments.sql:9-12`; 05; 07).

15. **The AO `engagement_docs` lane has been REMOVED** (it was previously BROKEN at HEAD). The
    `apps/edge_api/src/engagement_docs/` module, the `engagement_mandates_v1.py` router, and
    `apps/edge_api/sql/ao_engagement_mandates.sql` no longer exist on current main (grep: zero matches).
    The live repo-content render lane is the **render+push** lane
    (`apps/edge_api/src/routers/internal_engagement_templates_v1.py:84`;
    `apps/edge_api/src/engagement_templates/push.py:63`; 06).

16. **`MandateSignPage` and `business.mandate_payments`/`mandate_payment_events` do NOT exist.** The real
    prospect component is `DocumentSignPage`; the real payment tables are `business.document_payments` /
    `document_payment_events`. Rule of thumb: operator-facing = `Mandate*`, prospect-facing = `Document*`
    (09 §3, §6). The new operator-facing originate result `MandateEmbedTemplateOriginated` follows the
    rule (returned by the service-token-gated `originate-embed-template`)
    (`apps/edge_api/src/engagement_mandate_drafts/models.py:49-72`).

17. **The `embed-template` lane mints NO document at originate.** `originate-embed-template` enables a
    template DIRECT LINK and returns a reusable `direct_token` + `embed_url` with `status='ready'` — the
    signer self-identifies in the embed (name/email NOT locked) and Documenso creates the document
    (source `TEMPLATE_DIRECT_LINK`) only at signer completion. Do not look for an `envelope_id`/`document_id`
    on this response; the numeric document id arrives client-side via the embed's `onDocumentCompleted`,
    after which the existing `/sign-state/{opp}/{doc}` surface tracks it (gate: `externalId == opportunity_id`)
    (`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:172-213`;
    `apps/edge_api/src/engagement_mandate_drafts/models.py:49-72`;
    `apps/edge_api/src/services/documenso_client.py:514-540`). Cross-repo: `<EmbedDirectTemplate>` /
    `DirectTemplateSignPage` and the `/p/t/:opportunityId/:directToken` route live in `rare-structure-hq`.

---

## Carried-forward unverified items (do NOT upgrade to fact)

- Several Documenso v2 client request/response shapes are `# CALIBRATE`-flagged and were not byte-pinned
  against the live `{base}/api/v2/openapi` spec; the placeholder-field shape, auth, webhook contract, and
  event names ARE confirmed (`apps/edge_api/src/services/documenso_client.py:14-24`; 04 Traps).
- The prefixed-vs-numeric Documenso id 400 behavior is asserted in inline notes, not independently
  exercised against the live API in source verification (04 §3).
- For the proposals/ref (through-docraptor) lane SPECIFICALLY, no projection of
  `business.documenso_webhook_events` back onto `engagement_proposals.status` was found post-repoint; whether
  that status still advances past `'sent'` server-side is UNVERIFIED (02 §6 open question).
- Upstream `CREATE TABLE` DDL for `business.opportunities` / `opportunity_specific_content` / `contacts` /
  `documenso_templates` / `engagement_mandate_draft_content` was not opened; ownership is inferred from the
  absence of DDL in `sql/` plus usage (07 Open/unverified).
- The repo-root `docs/reference/DIRECT_TO_DOCUMENSO_PAYMENT_E2E.md` covers ONLY the direct prefill lane + its
  payment and is stale on the rail (states ACH-only; code is dual-rail) and on some line cites; its central
  "8-char handle, not UUID" claim is correct. The task brief's path `apps/edge_api/../docs/reference/...`
  resolves to `apps/docs/reference/` (wrong) — the file is at `<repo-root>/docs/reference/` (08 carried-forward; 09 §5).
