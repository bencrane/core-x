# 07 — Data Stores: the Table System-of-Record Map

**STATUS:** Persistence-layer reference for the **edge_api HQX Postgres** (`HQX_DB_URL_POOLED`). Spans ALL render_modes and lanes — `through-docraptor`, `direct-to-documenso` (`envelope-distribute` RETIRED, `prefill-document-from-template` DEFAULT, `embed-template` NEW), the engagement-template render+PUSH lane, plus the parallel `ao_engagement_mandates` pathway and the GTM/company ingest tables. This is the cross-cutting ownership map; lane-specific control flow lives in the sibling docs.

## Orientation

This is the table system-of-record map for the `edge_api` FastAPI backend — the single writer over the hq-x control-plane Postgres reached via `HQX_DB_URL_POOLED`, holding `business.*`, `public.operator_settings`, `ops.*`, and `gtm.*`. A fresh agent reading this needs three things: (1) **which tables edge_api defines** (schema-as-code in `apps/edge_api/sql/*.sql`, applied at boot) vs. **which it only writes** (upstream-owned, upserted) vs. **which it only reads**; (2) **the two distinct opportunity identifiers** — the row UUID `business.opportunities.id` (internal PK / FK target) and the generated 8-char handle `business.opportunities.opportunity_id` — and exactly which tables carry which; (3) **the proven-nonexistent tables** (`mandate_payments` / `mandate_payment_events`) so you do not chase ghosts. Ownership is split into four classes: **edge_api** (defines + writes), **upstream** (hq-x migration tool defines; edge_api writes via idempotent upsert OR only reads), **documenso**, and **stripe**. Where a DDL comment disagrees with runtime behavior, the CODE wins and the discrepancy is flagged in **Traps**.

## How the schema is applied (schema-as-code)

DDL is applied at boot, not by a migration framework. `run_migrations()` globs `sql/*.sql` and applies each file whole, in deterministic filename-ascending order, one transaction per file under an xact-scoped advisory lock.

```
main.py lifespan
  └─ run_migrations()                          # apps/edge_api/src/migrate.py:64
       ├─ if not config.db_migrate_on_boot(): return    # EDGE_API_SKIP_DB_MIGRATE → skip
       └─ for path in sorted(SQL_DIR.glob('*.sql')):     # sql_files() migrate.py:59-61
            async with conn.transaction():               # one txn per file (whole-or-nothing)
              cur.execute("SELECT pg_advisory_xact_lock(%s)", (_APPLY_LOCK_KEY,))  # serialize replicas
              cur.execute(sql)                            # NO params → simple-query protocol, runs DO $$..$$
            except Exception: log.exception(...); raise   # FAIL THE BOOT loudly
```

- `sql_files()` returns `sorted(SQL_DIR.glob("*.sql"))` — filename-ascending apply order (`apps/edge_api/src/migrate.py:59-61`).
- `run_migrations()` loops files, each under `conn.transaction()`, taking `pg_advisory_xact_lock` then executing the whole file with NO params (simple-query protocol so multi-statement files incl. `DO $$ … $$` run server-side), re-raising on the first failure to fail the boot (`apps/edge_api/src/migrate.py:64-96`, lock+execute at `:88-91`, except/raise at `:92-94`).
- Invoked from the FastAPI lifespan (`apps/edge_api/main.py:138`).
- Skippable via `EDGE_API_SKIP_DB_MIGRATE` ∈ {1,true,yes,on} → `config.db_migrate_on_boot()` returns False, the boot DDL apply is skipped, and the live schema MAY drift from committed `sql/*.sql` (`apps/edge_api/src/migrate.py:67-72`; `apps/edge_api/src/config.py:215-225`). Default is to apply.

**Implication for this doc:** any table edge_api *defines* has on-disk DDL in `sql/`. Any table referenced but with **no** `CREATE TABLE` in `sql/` is upstream-owned — its full column set is not provable from this repo's DDL alone.

## The two opportunity identifiers (read this before anything else)

`business.opportunities` carries TWO ids. Conflating them is the single biggest trap in this domain.

| Identifier | Type | Role | Generated how | Externally visible? |
|---|---|---|---|---|
| `business.opportunities.id` | `uuid` | Internal PK; the FK target every other table references | upstream insert | No (no longer) |
| `business.opportunities.opportunity_id` | `text` | Public 8-char handle; Documenso `externalId`; the signing-link gate | `GENERATED ALWAYS AS (LEFT(id::text, 8)) STORED` | Yes |

- The handle is DB-generated, zero application logic: `ADD COLUMN IF NOT EXISTS opportunity_id text GENERATED ALWAYS AS (LEFT(id::text, 8)) STORED` (`apps/edge_api/sql/opportunities_opportunity_id.sql:19-21`).
- It is the value stamped as the Documenso envelope's `externalId` at originate and carried in the prospect signing link `/p/m/{opportunity_id}/{document_id}`; "The full row UUID is NO LONGER the externally-visible opportunity id" (`apps/edge_api/sql/opportunities_opportunity_id.sql:7-10`). The originate router sets `external_id=opportunity_ref` and surfaces `opportunity_id` for the link (`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:148-162`).
- Its index is **non-unique by design** (`idx_opportunities_opportunity_id`): a unique constraint on an 8-char derived prefix could astronomically-rarely hard-fail an insert on a first-8 collision, and the signing flow never resolves an opportunity by this column (`document_id` is the unique pin) (`apps/edge_api/sql/opportunities_opportunity_id.sql:23-28`).

### Which tables carry which identifier

| Carrier column | Carries | Resolved as | Citation |
|---|---|---|---|
| `document_payments.opportunity_id` (`text`) | 8-char **HANDLE** | `WHERE o.opportunity_id = %s` | `apps/edge_api/sql/document_payments.sql:18`; `apps/edge_api/src/document_payments/queries.py:53` |
| `documenso_webhook_events.external_id` (`text`) | 8-char **HANDLE** (= `externalId` at originate) | `WHERE external_id = %(opportunity_id)s` | `apps/edge_api/sql/documenso_webhook_events.sql:19`; `apps/edge_api/src/documenso_webhooks/queries.py:95` |
| `ao_engagement_mandates.opportunity_id` (`uuid`) | row **UUID** (by value, no FK) — the OPPOSITE | `%(opportunity_id)s::uuid` / `o.id = %s::uuid` | `apps/edge_api/sql/ao_engagement_mandates.sql:20`; `apps/edge_api/src/engagement_docs/queries.py:38,65` |

The `document_payments` fee-resolution query uses BOTH ids on different join legs of the SAME query: it joins `business.opportunity_specific_content osc ON osc.opportunity_id = o.id` (the **UUID** leg) and filters `WHERE o.opportunity_id = %s` (the **8-char handle** leg) (`apps/edge_api/src/document_payments/queries.py:41-57`, design note in module docstring `:3-5`).

## Ownership classes

| Class | Means | Tables |
|---|---|---|
| **edge_api (defines + writes)** | `CREATE TABLE` in `sql/`; edge_api is the writer | `document_payments`, `document_payment_events`, `documenso_webhook_events`, `engagement_proposals`, `engagement_events`, `ao_engagement_mandates`, `engagement_archetypes`, `global_engagement_content`, `global_input_content`, `engagement_template_push_runs`, `operator_settings`, `map_query_runs`, `company_profiles`, `company_profile_snapshots`, `clay_find_companies`, `clay_find_people` |
| **upstream (hq-x defines; edge_api writes via upsert)** | NO `CREATE TABLE` in `sql/`; edge_api upserts to live partial-unique keys | `business.opportunities`, `business.accounts`, `business.contacts` |
| **upstream (hq-x defines; edge_api READS only)** | NO `CREATE TABLE`, NO write anywhere | `business.opportunity_specific_content` |
| **upstream/shared (ALTER-only by edge_api)** | base predates this repo's DDL; edge_api only ALTER-adds columns + reads/FK-references | `business.engagement_mandate_draft_content`, `business.documenso_templates`, `business.organizations` |
| **documenso / stripe** | external systems of record for their own state | Documenso webhook payloads (raw landed into `documenso_webhook_events.payload`); Stripe events (raw landed into `document_payment_events.payload`) |

Proof that the upstream tables are NOT defined here: `grep -rniE 'create table.*(business\.)?(opportunities|contacts|accounts|opportunity_specific_content|engagement_mandate_draft_content|documenso_templates|organizations)\b' sql/` → **ZERO**. The ownership contract is documented in `apps/edge_api/src/opportunities/materialize.py:10-19` ("`business.accounts` / `business.contacts` / `business.opportunities` are owned by hq-x's own migration tool — edge_api WRITES them … but does NOT define them"), and the upserts "pin to the live partial-unique keys; if that schema drifts, the upsert fails loudly rather than duplicating" (`:13-14`).

---

## edge_api-defined tables (DDL in `sql/`)

### `business.document_payments` — direct-to-documenso engagement-fee record

- PK = `document_id text` (Documenso numeric document id — the unique pin); `opportunity_id text NOT NULL` carries the 8-char handle (the pair capability) (`apps/edge_api/sql/document_payments.sql:16-29`, key columns `:17-18`).
- `amount_cents integer NOT NULL` is the frozen charge in minor units, resolved server-side from `opportunity_specific_content.field_values['fee_amount']` at intent-mint time, NEVER from the browser; `payment_status` is advanced authoritatively ONLY by the Stripe webhook (`apps/edge_api/sql/document_payments.sql:9-12`, `:19`).
- `payment_status` domain (comment + `DEFAULT 'none'`, **no CHECK constraint**): `none | requires_payment | processing | succeeded | failed | canceled` (`apps/edge_api/sql/document_payments.sql:23-24`).
- `rail` (`'card' | 'us_bank_account'`) and `paid_at` are each stamped ONCE by the webhook (COALESCE keeps the first value) (`apps/edge_api/sql/document_payments.sql:25-26`).
- Writers: `upsert_intent` (idempotent on `document_id`; a terminal `succeeded` is NEVER downgraded on re-mint via a `CASE` guard, `apps/edge_api/src/document_payments/queries.py:111-114`); `advance_status` (the webhook; set-once `paid_at`/`rail` via COALESCE at `:191-195`).
- Status advance is monotonic via a SQL rank `CASE` (`none=0, requires_payment=1, processing=2, succeeded=3`) so out-of-order webhook redelivery cannot regress a terminal row (`apps/edge_api/src/document_payments/queries.py:160-164`, applied in the UPDATE WHERE).

### `business.document_payment_events` — append-only Stripe webhook ledger (document lane)

- PK = `id bigint GENERATED ALWAYS AS IDENTITY`; `stripe_event_id text NOT NULL UNIQUE` is the idempotency key — a redelivered event is a no-op (`ON CONFLICT (stripe_event_id) DO NOTHING`); `payload jsonb NOT NULL` is the raw Stripe event SoR (`apps/edge_api/sql/document_payments.sql:42-50`; `record_event_if_new` at `apps/edge_api/src/document_payments/queries.py:144-154`).

### `business.documenso_webhook_events` — RAW Documenso webhook landing

- PK = `id uuid PRIMARY KEY DEFAULT gen_random_uuid()`; `payload jsonb NOT NULL` is "THE raw verbatim webhook body — system of record" (`apps/edge_api/sql/documenso_webhook_events.sql:26-33`).
- `event` / `envelope_id` / `external_id` are **best-effort lookup extracts only, NOT authoritative** — "re-derive anything from `payload` if extraction was wrong" (`apps/edge_api/sql/documenso_webhook_events.sql:15-22`).
- `external_id` carries the opportunity's 8-char handle (= `externalId` stamped at originate); the webhook extract reads `externalId` from the payload (`apps/edge_api/sql/documenso_webhook_events.sql:19`; `apps/edge_api/src/routers/documenso_webhooks_v1.py:56-62`).
- **`envelope_id` runtime value (direct lane): Documenso's NUMERIC document id** (e.g. `"1462137"`), matched against the link's `{document_id}` segment in `read_sign_state` — despite the DDL comment calling it "the envelope handle (payload id/envelopeId)". The query docstring states this explicitly and is verified against real landed rows 2026-06-17 (`apps/edge_api/src/documenso_webhooks/queries.py:69-76`); the projection filters `WHERE … AND envelope_id = %(document_id)s` (`:88-96`). See **Traps**.
- **No table-level dedup** (purely append-only): "No dedup here (append-only): redelivery handling is a projection-time concern" (`apps/edge_api/sql/documenso_webhook_events.sql:22`) — UNLIKE `document_payment_events` and `engagement_events`, which carry UNIQUE idempotency keys.
- Signing state is derived OFFLINE (zero Documenso calls): `signed` = a `DOCUMENT_COMPLETED` row exists for the pair (`external_id = opportunity_id AND envelope_id = document_id`). `_TERMINAL_EVENTS = ("DOCUMENT_COMPLETED",)` (`apps/edge_api/src/documenso_webhooks/queries.py:41`, projection `:88-97`, router "FULLY OFFLINE — ZERO Documenso calls" `apps/edge_api/src/routers/documenso_webhooks_v1.py:81-90`).
- Documenso terminal events are stored verbatim as **UPPERCASE_UNDERSCORE** (`DOCUMENT_SENT` / `DOCUMENT_OPENED` / `DOCUMENT_SIGNED` / `DOCUMENT_COMPLETED`), NOT lowercase-dotted — verified against real landed events 2026-06-17 (`apps/edge_api/src/documenso_webhooks/queries.py:35-41`).

### `business.engagement_proposals` — engagement-agreement / e-signature grain

- PK = `ref text` — an unguessable capability token that is BOTH the primary key and the public-read credential (the URL slug + the bearer; no auth header) (`apps/edge_api/sql/engagement_proposals.sql:8-13`, `:21-23`).
- `status text NOT NULL DEFAULT 'draft' CHECK (status IN ('draft','sent','opened','signed','completed','rejected','voided'))` (`apps/edge_api/sql/engagement_proposals.sql:39-40`).
- Documenso v2 linkage: `documenso_envelope_id`, `documenso_client_token`, `signed_pdf_url`, with a PARTIAL UNIQUE index on `documenso_envelope_id WHERE … IS NOT NULL` so the webhook UPDATE always hits ≤1 row (`apps/edge_api/sql/engagement_proposals.sql:42-44`, index DROP-legacy-then-CREATE-UNIQUE `:61-63`).
- Money in integer minor units: `monthly_fee_cents bigint`, `quarterly_total_cents bigint` ("legacy name; now carries `{{total}}` = monthly_fee × duration"). The 5/4/3/2/1.5% success-fee schedule is FIXED in the agreement body, NOT stored per-row; only `success_fee_schedule jsonb` varies per deal (`apps/edge_api/sql/engagement_proposals.sql:30-36`, header note `:15-17`).
- **`engagement_payments.sql` is ALTER-ONLY against this table** — it adds the Stripe ACH payment columns and creates `engagement_events` (the only `CREATE TABLE` in that file):
  - Columns ADDed: `stripe_customer_id`, `stripe_payment_intent_id`, `payment_status`, `amount_charged_cents`, `payment_currency`, `payment_initiated_at`, `paid_at` (`apps/edge_api/sql/engagement_payments.sql:18-25`).
  - `payment_status` constrained via `engagement_proposals_payment_status_chk CHECK (payment_status IN ('none','requires_payment','processing','succeeded','failed','canceled'))`; partial UNIQUE `engagement_proposals_pi_uidx ON (stripe_payment_intent_id) WHERE … IS NOT NULL` (one proposal per intent) (`apps/edge_api/sql/engagement_payments.sql:28-36`).
  - Amount resolved from `quarterly_total_cents` at intent-creation and snapshotted into `amount_charged_cents` — never from the browser; paid state advances ONLY by the Stripe webhook (ACH settles asynchronously 1-3 business days) (`apps/edge_api/sql/engagement_payments.sql:8-15`).

### `business.engagement_events` — append-only lifecycle ledger (proposal-ref grain)

- PK = `id uuid DEFAULT gen_random_uuid()`; FK `ref → business.engagement_proposals(ref) ON DELETE CASCADE`; `source text NOT NULL CHECK (source IN ('documenso','stripe','system'))`; `event_type` carries provider strings like `'payment_intent.succeeded'`, `'DOCUMENT_COMPLETED'`; partial UNIQUE `engagement_events_idem_uidx ON (source, idempotency_key) WHERE idempotency_key IS NOT NULL` is the webhook-redelivery idempotency guard (`apps/edge_api/sql/engagement_payments.sql:43-56`).

### `business.ao_engagement_mandates` — PARALLEL engagement-document ledger

- PK = `id text` (`mand_…`, minted at stage time); **`opportunity_id uuid NOT NULL` references `business.opportunities(id)` BY VALUE (no FK)** — the OPPOSITE carrier of `document_payments` (`apps/edge_api/sql/ao_engagement_mandates.sql:11-13`, `:19-20`).
- `status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','rendering','rendered','failed'))`; `style text NOT NULL DEFAULT 'plain' CHECK (style IN ('plain','branded'))` (`apps/edge_api/sql/ao_engagement_mandates.sql:26-31`).
- `opportunity_id` is NON-unique-indexed — "MANY deals (documents) per opportunity"; the prior one-per-opportunity unique index is dropped in place (`apps/edge_api/sql/ao_engagement_mandates.sql:52-57`).
- Documenso v2 linkage: `documenso_envelope_id text` (`envelope_…`), `documenso_document_id integer` (numeric `secondaryId`), `participant_signing_token`, `provider_signing_token` — distributed NONE so the signing tokens exist WITHOUT Documenso emailing anyone (`apps/edge_api/sql/ao_engagement_mandates.sql:36-42`, repeated as guarded ALTER ADD `:63-66`).
- This pathway "owns its own state — it never writes `engagement_proposals` or the proposal SoR" (`apps/edge_api/sql/ao_engagement_mandates.sql:9`).

### `business.engagement_archetypes` — economic-shape classifier (ABOVE `documenso_templates`)

- PK = `id uuid DEFAULT gen_random_uuid()`; `key text NOT NULL UNIQUE`; `performance_fee_basis CHECK (… IN ('greater_of','lesser_of','sum','percentage_only','flat_only'))` (`apps/edge_api/sql/engagement_archetypes.sql:19-27`).
- Seeded idempotently with two live archetypes: `'term_only'` (NULL basis) and `'term_plus_greater_of'` (`'greater_of'`), `ON CONFLICT (key) DO NOTHING` (`apps/edge_api/sql/engagement_archetypes.sql:34-44`).
- The same file ALTERs the upstream `business.documenso_templates` ("the table predates this file and is not defined here") to add `archetype_id uuid` with a guarded FK `ON DELETE RESTRICT`, then backfills it data-driven from each template's `recipients->'text_fields'` tokens (`'term_plus_greater_of'` if it carries `percentage_deal_fee`/`flat_deal_fee_amount`/`term_fee`, else `'term_only'`) (`apps/edge_api/sql/engagement_archetypes.sql:46-78`).

### `business.global_engagement_content` — single archetype-agnostic global content body

- PK = `id text` (`tpl_…`); `status text NOT NULL DEFAULT 'draft' CHECK (status IN ('draft','published'))`; `markdown text`; `slug` is the publish-time selector a minted document stores in `business.engagement_proposals.template_id`, with partial UNIQUE `global_engagement_content_slug_uidx ON (slug) WHERE slug IS NOT NULL` (`apps/edge_api/sql/global_engagement_content.sql:80-112`, slug-as-selector note `:19-21`).
- `organization_id uuid REFERENCES business.organizations (id) ON DELETE RESTRICT` is NULLABLE — "the in-app create path does not set it yet" (CONDITIONAL) (`apps/edge_api/sql/global_engagement_content.sql:100-102`).
- **Name lineage** (comment): `proposal_templates → proposal_content_configs → engagement_content → global_engagement_content` (`apps/edge_api/sql/global_engagement_content.sql:15-17`). BUT the rename DO-block only converges TWO predecessors — it renames `engagement_content` OR `proposal_content_configs` in place (preserving data + inbound FKs + re-pointing inherited index/constraint names); `proposal_templates` is documented historical lineage, NOT a branch the block handles (`apps/edge_api/sql/global_engagement_content.sql:30-42`). See **Traps**.

### `business.global_input_content` — content-source REGISTRY for the render+PUSH lane

- PK = `id uuid DEFAULT gen_random_uuid()`; `path text NOT NULL UNIQUE`; `status text NOT NULL DEFAULT 'active'`. One row per repo-resident engagement-content asset (or DB-markdown source) the render+push lane "grabs from"; a row names WHERE to pull, the renderer resolves it to HTML → DocRaptor → Documenso TEMPLATE (`apps/edge_api/sql/global_input_content.sql:1-31`).
- ALTER-adds two source-selection columns on the already-provisioned live table: `brand text NOT NULL DEFAULT 'active-operators'` (`'active-operators' | 'rare-structure'`) and `source_kind text NOT NULL DEFAULT 'repo-html'` constrained by `global_input_content_source_kind_chk CHECK (source_kind IN ('repo-html','db-markdown'))` (`apps/edge_api/sql/global_input_content.sql:33-49`). The table predates this file (hand-provisioned upstream: `id/path/name/status`); this DDL OWNS it going forward (`:6-8`).
- `path` is BRAND-RELATIVE and encodes the catalog segments `<template-family>/<archetype>/<version>` for `repo-html` (e.g. `docraptor-to-documenso-template/term-only/v1`), OR the `business.global_engagement_content` slug for `db-markdown` (`apps/edge_api/sql/global_input_content.sql:12-18`).
- Seeded idempotently (`ON CONFLICT (path) DO NOTHING`) with two `repo-html` assets: AO `docraptor-to-documenso-template/term-only/v1` (term-only) and rare-structure `docraptor-to-documenso-template/capital-origination/v1` (capital-origination) (`apps/edge_api/sql/global_input_content.sql:54-58`).

### `ops.engagement_template_push_runs` — QA ledger for the render+PUSH lane

- PK = `id bigint GENERATED ALWAYS AS IDENTITY`; one row per render+push attempt: which content source was pulled (`brand/path/archetype/version/source_kind`), the `style` (`'plain' | 'branded'`), the `documenso_template_id` + `documenso_numeric_id` minted, an audit `pdf_r2_key`, and `error` on failure. `status text NOT NULL` ∈ `'success' | 'error'`; `run_id` is the Trigger.dev run id (NULL for a direct/manual call) (`apps/edge_api/sql/ops_engagement_template_push_runs.sql:11-30`).
- Written by `apps/edge_api/src/engagement_templates/push.py` on EVERY terminal state, fire-and-forget — a ledger error never blocks or fails the render-push response (`apps/edge_api/sql/ops_engagement_template_push_runs.sql:1-5`).

### `public.operator_settings` — per-operator cockpit config (PUBLIC schema, SAME HQX Postgres)

- PK = `auth_user_id uuid` (the Supabase JWT `sub`); one row per operator (`apps/edge_api/sql/operator_settings.sql:39-45`).
- `render_mode CHECK (… ANY (ARRAY['through-docraptor','direct-to-documenso']))` DEFAULT `'through-docraptor'`; `direct_to_documenso_lane CHECK (… ANY (ARRAY['envelope-distribute','prefill-document-from-template','embed-template']))` DEFAULT `'prefill-document-from-template'` — the lane ONLY applies when `render_mode='direct-to-documenso'`; `stripe_mode CHECK (stripe_mode IS NULL OR … ANY (ARRAY['test','live']))`, NULL = follow the `STRIPE_MODE` env (`apps/edge_api/sql/operator_settings.sql:41-43`, `:71-94`; lane-conditionality + lane-domain `:23-34`, `:60-94`, stripe_mode-NULL `:51-56`).
- **The three direct-to-documenso lanes** (CHECK domain DROP+re-ADDed every apply so new values converge, `apps/edge_api/sql/operator_settings.sql:60-94`):
  - `'envelope-distribute'` — **RETIRED**. The `/envelope/use` + `.../{id}/confirm` lane was removed in code; the CHECK still accepts the value so a pre-existing row never violates it, but no live path serves it (`apps/edge_api/sql/operator_settings.sql:30-34`).
  - `'prefill-document-from-template'` — **DEFAULT** (embed-document). `/api/v2/template/use` with the opportunity's field values prefilled, distribute(NONE) → PENDING (no email) → `POST .../{id}/originate-prefilled` → `create_document_from_template`. A document is minted NOW (`apps/edge_api/sql/operator_settings.sql:24-29`; `apps/edge_api/src/services/documenso_client.py:228`).
  - `'embed-template'` — **NEW** (direct-link). Enables a reusable DIRECT LINK on the draft's template; **NO document minted here** — Documenso creates it at signer completion (source `TEMPLATE_DIRECT_LINK`). `POST .../{id}/originate-embed-template` → `MandateEmbedTemplateOriginated` (`direct_token`, `documenso_host`, `embed_url`, `external_id`, `opportunity_id`, `direct_recipient_id`, `recipient_email`, `recipient_name`, `status="ready"`). PARALLEL to originate-prefilled, not a replacement (`apps/edge_api/sql/operator_settings.sql:31-32`; `apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:169-223`; `apps/edge_api/src/engagement_mandate_drafts/models.py:49-70`). See **The embed-template direct-link lane** below.
- `stripe_mode` is read by the document-payment flow as a SINGLE GLOBAL selection (latest non-null row wins, `ORDER BY updated_at DESC LIMIT 1`) because the single-operator platform's prospect-facing mint has no operator session (`apps/edge_api/src/document_payments/queries.py:64-80`).
- RLS stays ENABLED as defense-in-depth but is "no longer load-bearing": edge_api connects via the pooled application role; the access boundary is the service token + the BFF's upstream Supabase session check (`apps/edge_api/sql/operator_settings.sql:96-99`).
- **Gateway scope:** for the **Settings-tab persistence flow**, the platform-api BFF no longer touches this table directly — edge_api is the gateway (`GET/PUT /api/v1/operator-settings/{auth_user_id}`) (`apps/edge_api/sql/operator_settings.sql:5-13`). This is TRUE FOR THE SETTINGS-PERSISTENCE FLOW ONLY — a separate path (proposal-confirm) STILL reads `operator_settings` directly via Supabase. See **Cross-repo handoffs** and **Traps**.

### `ops.map_query_runs` — QA ledger for the map NL query path

- PK = `id bigint GENERATED ALWAYS AS IDENTITY`; `status text NOT NULL` ∈ `'success' | 'translate_error' | 'execute_error'`; `dataset text NOT NULL` ∈ `'company' | 'winners'`. Written fire-and-forget on every terminal `/ask` state (`apps/edge_api/sql/ops_map_query_runs.sql:10-27`, intent note `:1-8`).

### `business.company_profiles` — domain-keyed dossier

- PK = `domain text` (canonical resolution key, lowercased, "matches `corex.bookings.domain`"); one row per company; `source text NOT NULL DEFAULT 'seed'` (`'seed'` = hand-authored stand-in; later `'parallel'`/`'cal'` enrichment) (`apps/edge_api/sql/company_profiles.sql:17-30`).

### `business.company_profile_snapshots` — append-only Save-Profile history

- PK = `id bigint GENERATED ALWAYS AS IDENTITY`; `domain text NOT NULL` is NOT unique ("many snapshots per domain"). Superset of `company_profiles` — adds the Main Contact (`signer_name`/`title`/`email`) and the per-section `verified jsonb` map. The Dossier loads the LATEST snapshot, else the `company_profiles` seed (`apps/edge_api/sql/company_profile_snapshots.sql:17-38`, superset/latest-load note `:5-13`).

### `gtm.clay_find_companies` — Clay raw company landing (append-only, verbatim)

- PK = `record_id text` (`sha256(company_key)`); columns stored VERBATIM (no scheme/case/normalization), e.g. `size` band, `annual_revenue` band (`apps/edge_api/sql/clay_find_companies.sql:24-40`).

### `gtm.clay_find_people` — Clay raw people landing (append-only, verbatim)

- PK = `record_id text` (`sha256(linkedin_url_norm | company_key)`); `person_id text NOT NULL` = `sha256(linkedin_url_norm)` is the email-rail dedup key (`apps/edge_api/sql/clay_find_people.sql:20-26`).

---

## Upstream tables (NO `CREATE TABLE` in edge_api `sql/`)

### `business.opportunities` — upstream-DEFINED, edge_api-WRITTEN

- Owned by hq-x's migration tool (grep-confirmed: ZERO `CREATE TABLE … opportunities` in `sql/`). The ONLY DDL touching it here is the `ALTER … ADD COLUMN` for the generated `opportunity_id` handle (`apps/edge_api/sql/opportunities_opportunity_id.sql:19-21`).
- edge_api WRITES it: `materialize.py` upserts `INSERT INTO business.opportunities … ON CONFLICT (source_booking_id) …` (idempotent, one opportunity per booking) (`apps/edge_api/src/opportunities/materialize.py:151-159`, key documented `:18`).

### `business.accounts` — upstream-DEFINED, edge_api-WRITTEN

- No edge_api `CREATE TABLE`. Written by `materialize.py`: domain-keyed upsert (`ON CONFLICT (domain) …`) at `apps/edge_api/src/opportunities/materialize.py:83`, and a keyless insert (no domain) at `:96`.

### `business.contacts` — upstream-DEFINED, edge_api-WRITTEN (NOT read-only)

- No edge_api `CREATE TABLE`. Written by `materialize.py` TWICE: account-email upsert (`ON CONFLICT (account_id, lower(email)) …`) at `apps/edge_api/src/opportunities/materialize.py:108`, and a named-no-email backstop insert at `:133`.

### `business.opportunity_specific_content` — the ONLY truly read-but-never-written table here

- READ-ONLY for edge_api: no `CREATE TABLE`, and `grep -rniE '(insert into|update|delete from)\s+(business\.)?opportunity_specific_content'` → ZERO. Read sites: one `FROM` (`apps/edge_api/src/engagement_mandate_drafts/queries.py:150`) and one `JOIN ON osc.opportunity_id = o.id` (`apps/edge_api/src/document_payments/queries.py:51`). Supplies `field_values['fee_amount']` for payment and recipient values for drafts.

### `business.engagement_mandate_draft_content` — upstream base, edge_api ALTER-only

- The base table "predates this file and is not defined here"; edge_api ALTER-adds `prefill_values jsonb NOT NULL DEFAULT '{}'::jsonb` (per-deal operator values keyed by template text_field) and `archetype_id uuid` (FK → `engagement_archetypes` `ON DELETE RESTRICT`). `status` already exists on the base table (default `'draft'`) (`apps/edge_api/sql/engagement_mandate_draft_content.sql:4-13`, `:19-38`). `archetype_id` is backfilled from `documenso_templates` matching `dt.documenso_template_id = d.documenso_template_id` (`:46-52`). Full column set is NOT provable from on-disk DDL (see **Open / unverified**).

### `business.documenso_templates` — pre-existing base, edge_api ALTER-only

- "The table predates this file" (`apps/edge_api/sql/engagement_archetypes.sql:46-48`). edge_api ALTER-adds `archetype_id uuid` (guarded FK `ON DELETE RESTRICT`) and backfills from `recipients.text_fields` tokens (`apps/edge_api/sql/engagement_archetypes.sql:46-78`). Labeled **documenso**-owned on the strength of the "predates" note; whether an earlier edge_api era or the upstream Documenso integration originally created it is NOT provable from current on-disk DDL (see **Open / unverified**).

### `business.organizations` — upstream/shared base, edge_api ALTER-only + reads

- "`business.organizations` is an UPSTREAM/shared table (edge_api only FK-references it)" (`apps/edge_api/sql/organizations_theme.sql:3`). `organizations_theme.sql` is ALTER-only against it: ADDs `theme_config jsonb` (DocRaptor brand shell) and seeds it via `UPDATE … WHERE name IN ('Active Operators','Rare Structure','Engineered Demand')` (`apps/edge_api/sql/organizations_theme.sql:1-22`). Also FK-referenced by `global_engagement_content.organization_id` (`apps/edge_api/sql/global_engagement_content.sql:100-102`). Note: the comment says "only FK-references it" yet the file ALTER-adds + UPDATEs seed rows — both are accurate; the literal comment understates the seed write.

---

## Proven-nonexistent tables

| Table | Status | Proof |
|---|---|---|
| `mandate_payments` | DOES NOT EXIST | `grep -rniE 'mandate_payment(s\|_events)'` over BOTH repos → ZERO matches (re-verifiable); corroborated by a one-time live prd probe (2026-06-17): `information_schema.tables LIKE 'mandate_payment%'` → empty, direct selects → "relation does not exist". (That probe lives in the untracked, repo-external `DIRECT_TO_DOCUMENSO_PAYMENT_E2E.md` — not a repo citation; the grep-zero is self-contained.) |
| `mandate_payment_events` | DOES NOT EXIST | Same proof as above |

The document-payment record IS `business.document_payments` (keyed by Documenso `document_id`); there is no `mandate_payments` table. Do not invent one. (The corroborating prd-probe artifact `DIRECT_TO_DOCUMENSO_PAYMENT_E2E.md` is an untracked file in the operator's main checkout — NOT part of this repo; rely on the self-contained grep-zero proof above.)

---

## Cross-repo handoffs (SPA → BFF → edge_api)

### Operator-settings — Settings-tab persistence

```
platform-app Settings (originationMode.ts)
  GET/PUT ${API_BASE}/api/v1/settings                  # rare-structure-hq:apps/platform-app/src/settings/originationMode.ts:27,42
    → platform-api BFF  app.route("/api/v1/settings")  # rare-structure-hq:apps/platform-api/src/index.ts:110
        settings.ts  DUMB PASS-THROUGH (requireUser, assert JWT sub as auth_user_id)
        edge.ts  fetch ${base()}/api/v1/operator-settings/{authUserId}   # rare-structure-hq:apps/platform-api/src/lib/edge.ts:646,657
          → edge_api  GET/PUT /api/v1/operator-settings/{auth_user_id}
            → public.operator_settings
```

`settings.ts` is explicitly a "DUMB PASS-THROUGH to edge_api's operator-settings gateway … it NO LONGER touches `public.operator_settings` directly" (`rare-structure-hq:apps/platform-api/src/routes/settings.ts:1-15`).

**CAVEAT (do not over-generalize the gateway):** a SEPARATE flow — proposal-confirm — STILL reads `operator_settings.render_mode` directly via Supabase in the BFF: `db().from("operator_settings").select("render_mode").eq("auth_user_id", …)` (`rare-structure-hq:apps/platform-api/src/routes/proposals-admin.ts:132-136`). The "BFF no longer touches the table directly" claim holds ONLY for the settings-persistence flow.

### Document-payment pair capability

```
platform-app  /p/m/{opportunity_id}/{document_id}
  → BFF
    → edge_api  /api/v1/documenso/{sign-state|sign-token|payment-intent|payment}/{opportunity_id}/{document_id}
```

`opportunity_id` is the 8-char generated handle (the access capability); `document_id` is the Documenso numeric pin. NOTE: the E2E reference doc calls the payment intent "ACH us_bank_account" but the code creates a DUAL-RAIL `['card','us_bank_account']` intent — CODE WINS (see **Traps**).

### Opportunity materialization (writes the upstream-owned tables)

```
cal.com webhook → Trigger.dev opportunity-materialize task
  → edge_api POST /internal/opportunities/materialize           # apps/edge_api/src/routers/internal_opportunities_v1.py:3
    → materialize_for_booking(...)                                # apps/edge_api/src/opportunities/materialize.py:47
      → upserts business.accounts / business.contacts / business.opportunities  (upstream-owned)
        idempotently from corex.bookings
```

### The embed-template direct-link lane (`direct_to_documenso_lane='embed-template'`)

```
platform-app MandateDraftShell (3rd dispatch branch; rare-structure-hq, SEPARATE repo)
  → BFF originate-embed-template
    → edge_api  POST /api/v1/engagement-mandate-drafts/{draft_id}/originate-embed-template
        → documenso_client.create_direct_link  POST /api/v2/template/direct/create {templateId, directRecipientId?}
        → MandateEmbedTemplateOriginated {direct_token, documenso_host, embed_url, external_id, opportunity_id, ...}
    SPA mounts EmbedDirectTemplate(token=direct_token, host) at route /p/t/{opportunityId}/{directToken}?host=
      → public /d/{token} / iframe /embed/direct/{token}; SIGNER self-identifies (name+email NOT locked)
        → on completion Documenso CREATES the document (source TEMPLATE_DIRECT_LINK)
          → existing /sign-state/{opportunity_id}/{document_id} surface tracks it (gate: externalId == opportunity_id)
```

- **NO document is minted at originate** — unlike the prefill lane (which mints a document NOW and returns a per-document signing token), embed-template enables a reusable DIRECT LINK on the draft's template and returns the template's reusable `direct_token`; Documenso creates the document AT signer completion (`apps/edge_api/src/engagement_mandate_drafts/models.py:50-60`; endpoint `apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:169-223`, `status="ready"` at `:222`).
- core-x contracts (THIS repo): `documenso_client` gained `DirectLinkResult` + `create_direct_link` / `toggle_direct_link` / `get_template_recipients` (`POST /api/v2/template/direct/{create,toggle}`) and `create_template_from_pdf` + `TemplateCreateResult` (`POST /api/v2/envelope/create` `type=TEMPLATE`); `create_document_from_template` (embed-document) is unchanged (`apps/edge_api/src/services/documenso_client.py:228,410-547`).
- **Documenso direct-link facts (v2 OpenAPI):** `/template/direct/create {templateId, directRecipientId?}` → `{token,...}`; the `token` is BOTH the `EmbedDirectTemplate` prop AND the public `/d/{token}` (iframe `/embed/direct/{token}`). The signer enters their OWN name+email. `typedSignatureEnabled`/`drawSignatureEnabled`/`uploadSignatureEnabled` are document/template-level meta settings. Field types: `SIGNATURE,FREE_SIGNATURE,INITIALS,NAME,EMAIL,DATE,TEXT,NUMBER,RADIO,CHECKBOX,DROPDOWN` (NO dedicated `TITLE` — it's `TEXT`); `prefillFields` supports text/number/radio/checkbox/dropdown/date (NOT name/signature).
- **Frontend (cross-repo, rare-structure-hq — verify there):** `DirectTemplateSignPage` (`EmbedDirectTemplate`; signer self-identifies, name/email NOT locked), route `/p/t/:opportunityId/:directToken` with `?host=`, `MandateDraftShell` 3rd dispatch branch, `SignLink` discriminated union, a shared `DirectToDocumensoLane` literal, the BFF `originate-embed-template` proxy, host threaded via `?host=`. Verifiable from THIS repo only via the `MandateEmbedTemplateOriginated` contract (`apps/edge_api/src/engagement_mandate_drafts/models.py:49-70`); the SPA/BFF source is in the separate repo.

### Engagement-template render+PUSH lane (content source → DocRaptor → Documenso TEMPLATE)

```
Trigger.dev task "engagement-template-push" (src/trigger/engagement_template_push.ts)
  → callHqx  POST /internal/engagement-templates/render-push   (TRIGGER_SHARED_SECRET)
    → edge_api  apps/edge_api/src/routers/internal_engagement_templates_v1.py:84  require_trigger_secret
        → catalog.resolve(brand, path, archetype, version)   # apps/edge_api/src/engagement_templates/catalog.py:99
        → push.py: render HTML → DocRaptor PDF → documenso_client.create_template_from_pdf (Documenso TEMPLATE)
          → ledger: ops.engagement_template_push_runs (fire-and-forget, every terminal state)
```

- The brand-aware catalog resolves `<brand>/<path>/<archetype>/<version>/global_engagement_content/manifest.json` under `content/`; `_ALLOWED_BRANDS = {active-operators, rare-structure}` is enforced as an allowlist so an unvetted directory can never surface as a selectable template; `brand` defaults to `active-operators` so the original three-segment call sites keep working (`apps/edge_api/src/engagement_templates/catalog.py:21-28,99-112`).
- The brand asset tree `content/rare-structure/docraptor-to-documenso-template/capital-origination/v1/global_engagement_content/` carries static-blank HTML (field-slot blanks, NO underscore glyphs; §8.4 Authority; 1.6 leading) plus `styles/plain.css` + `styles/branded.css` and a `manifest.json` (archetype `capital_origination`) (`apps/edge_api/content/rare-structure/docraptor-to-documenso-template/capital-origination/v1/global_engagement_content/manifest.json`). The minted live Documenso template numeric id (`14310`) is a runtime fact, not committed in this repo.
- The internal endpoint is `/internal/*`-gated by `require_trigger_secret` (TRIGGER_SHARED_SECRET) — the same contract opportunity-materialize uses (`apps/edge_api/src/routers/internal_engagement_templates_v1.py:1-6,84-85`).

---

## Payment-rail split (do-not-conflate)

| Lane | Intent `payment_method_types` | Citation |
|---|---|---|
| Document payments (direct-to-documenso fee) | **DUAL-RAIL** `["card", "us_bank_account"]` | `apps/edge_api/src/document_payments/stripe.py:74,87` |
| Engagement (proposal-ref) payments | **ACH-ONLY** `["us_bank_account"]` (card NOT offered) | `apps/edge_api/src/payments/stripe_client.py:1-3,70` |

---

## Status: ACTIVE / CONDITIONAL / DEPRECATED / STUB

**ACTIVE**
- `run_migrations()` boot DDL apply — `apps/edge_api/src/migrate.py:64-96`
- `business.opportunities` (upstream) + generated `opportunity_id` handle — `apps/edge_api/sql/opportunities_opportunity_id.sql:19-28`
- `business.document_payments` + `business.document_payment_events` — `apps/edge_api/sql/document_payments.sql:16-50`
- `business.documenso_webhook_events` (raw landing; offline sign-state) — `apps/edge_api/sql/documenso_webhook_events.sql:26-33`
- `business.engagement_proposals` + ALTERed payment columns + `business.engagement_events` — `apps/edge_api/sql/engagement_proposals.sql:21-65`, `apps/edge_api/sql/engagement_payments.sql:18-56`
- `business.ao_engagement_mandates` (parallel pathway) — `apps/edge_api/sql/ao_engagement_mandates.sql:17-66`
- `business.engagement_archetypes` + `documenso_templates.archetype_id` ALTER/backfill — `apps/edge_api/sql/engagement_archetypes.sql:19-78`
- `business.global_engagement_content` — `apps/edge_api/sql/global_engagement_content.sql:80-112`
- `business.global_input_content` (render+push content-source registry) — `apps/edge_api/sql/global_input_content.sql:21-58`
- `ops.engagement_template_push_runs` (render+push QA ledger) — `apps/edge_api/sql/ops_engagement_template_push_runs.sql:11-30`
- `public.operator_settings` (settings-tab gateway via edge_api; 3-lane domain) — `apps/edge_api/sql/operator_settings.sql:39-94`
- Engagement-template render+PUSH lane (`/internal/engagement-templates/render-push`) — `apps/edge_api/src/routers/internal_engagement_templates_v1.py:84-85`, `apps/edge_api/src/engagement_templates/catalog.py:99-112`
- embed-template direct-link lane (`/{draft_id}/originate-embed-template`) — `apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:169-223`, `apps/edge_api/src/services/documenso_client.py:410-547`
- `ops.map_query_runs`, `business.company_profiles`, `business.company_profile_snapshots`, `gtm.clay_find_companies`, `gtm.clay_find_people`
- Upstream-written: `business.accounts`, `business.contacts` (via `materialize.py`)
- Upstream read-only: `business.opportunity_specific_content`
- Upstream ALTER-only/shared: `business.documenso_templates`, `business.organizations`, `business.engagement_mandate_draft_content`

**CONDITIONAL**
- `operator_settings.direct_to_documenso_lane` — only meaningful when `render_mode='direct-to-documenso'`; DEFAULT now `'prefill-document-from-template'`; domain `{envelope-distribute(retired), prefill-document-from-template, embed-template}` (`apps/edge_api/sql/operator_settings.sql:23-34`, `:60-94`)
- `operator_settings.stripe_mode` override — only when non-NULL; NULL falls back to `STRIPE_MODE` env (`apps/edge_api/sql/operator_settings.sql:51-56`)
- `global_engagement_content.organization_id` — nullable; the in-app create path does not set it yet (`apps/edge_api/sql/global_engagement_content.sql:100-102`)
- `EDGE_API_SKIP_DB_MIGRATE` — when set, the boot DDL apply is skipped (`apps/edge_api/src/migrate.py:67-72`)
- Direct Supabase read of `operator_settings` in the proposal-confirm BFF path — coexists with the edge_api gateway (`rare-structure-hq:apps/platform-api/src/routes/proposals-admin.ts:132-136`)

**DEPRECATED**
- The full row UUID as the externally-visible opportunity id — superseded by the 8-char `opportunity_id` handle (`apps/edge_api/sql/opportunities_opportunity_id.sql:7-10`). The UUID remains the internal PK/FK target.
- `engagement_proposals.quarterly_total_cents` NAME — legacy; now carries `{{total}}` = monthly_fee × duration (`apps/edge_api/sql/engagement_proposals.sql:36`)
- `direct_to_documenso_lane='envelope-distribute'` — RETIRED; the `/envelope/use` + `.../{id}/confirm` lane was removed in code; the CHECK still accepts the value so a pre-existing row never violates it, but no live path serves it (`apps/edge_api/sql/operator_settings.sql:30-34`)

**STUB / nonexistent**
- `mandate_payments`, `mandate_payment_events` — DO NOT EXIST (grep-zero across both repos; see "Proven-nonexistent tables" above)

---

## Traps

1. **Two opportunity ids, opposite carriers.** `document_payments.opportunity_id` and `documenso_webhook_events.external_id` carry the **8-char handle**; `ao_engagement_mandates.opportunity_id` carries the **row UUID** (`uuid`, by value, no FK). Never assume one type. Citations: `apps/edge_api/sql/document_payments.sql:18`, `apps/edge_api/sql/documenso_webhook_events.sql:19`, `apps/edge_api/sql/ao_engagement_mandates.sql:20`.

2. **`documenso_webhook_events.envelope_id` DDL comment LIES (for the direct lane).** The DDL comments it as "the envelope handle (payload id/envelopeId)" (`apps/edge_api/sql/documenso_webhook_events.sql:18`), but the live sign-state projection treats it as Documenso's NUMERIC document id and matches it against `{document_id}` (`apps/edge_api/src/documenso_webhooks/queries.py:69-76,88-96`). The webhook extract digs `id`/`documentId`/`envelopeId` in that order (`apps/edge_api/src/routers/documenso_webhooks_v1.py:61`); the numeric id lands first for the direct lane. Verified numeric for the direct lane only — the `envelope-distribute` lane's stored value was not independently probed.

3. **`document_payments.payment_status` has NO CHECK constraint** — only a comment listing the domain and a `DEFAULT 'none'` (`apps/edge_api/sql/document_payments.sql:23-24`). UNLIKE `engagement_proposals.payment_status`, which IS constrained by `engagement_proposals_payment_status_chk` (`apps/edge_api/sql/engagement_payments.sql:28-31`). Same string domain, different enforcement.

4. **`business.contacts` is NOT read-only.** It is upstream-DEFINED but edge_api-WRITTEN (two `INSERT`s in `materialize.py`, lines 108 and 133). The ONLY truly read-but-never-written table in this domain is `business.opportunity_specific_content`.

5. **`mandate_payments` / `mandate_payment_events` are a phantom.** They do not exist (grep-zero across both repos; corroborated by the 2026-06-17 prd probe). The document-payment record is `business.document_payments`, keyed by Documenso `document_id`.

6. **`global_engagement_content` rename converges only TWO predecessors.** The comment lists four names (`proposal_templates → proposal_content_configs → engagement_content → global_engagement_content`, `:15-17`) but the DO-block only renames `engagement_content` OR `proposal_content_configs` (`:30-42`). `proposal_templates` is historical lineage, not a live branch.

7. **`operator_settings` gateway is flow-scoped.** "BFF no longer touches the table directly" is TRUE for the settings-tab persistence flow ONLY (`settings.ts`/`edge.ts` pass-through). The proposal-confirm path STILL reads it directly via Supabase (`rare-structure-hq:apps/platform-api/src/routes/proposals-admin.ts:132-136`).

8. **Document-payment intent is DUAL-RAIL, not ACH-only.** The E2E reference doc calls it "ACH us_bank_account" but the code creates `["card","us_bank_account"]` (`apps/edge_api/src/document_payments/stripe.py:87`). Engagement (proposal-ref) payments ARE ACH-only (`apps/edge_api/src/payments/stripe_client.py:70`). Code wins; do not conflate the two lanes.

9. **`documenso_webhook_events` has NO table-level dedup.** Redelivery handling is projection-time (`:22`). `document_payment_events` (`stripe_event_id` UNIQUE) and `engagement_events` (`(source, idempotency_key)` UNIQUE) DO dedup at the table. Do not assume webhook idempotency is uniform across the three ledgers.

10. **Documenso events are UPPERCASE_UNDERSCORE, not lowercase-dotted.** `_TERMINAL_EVENTS = ("DOCUMENT_COMPLETED",)`; matching the lowercase-dotted form (`document.completed`) against `event` will silently never fire (`apps/edge_api/src/documenso_webhooks/queries.py:35-41`).

11. **`engagement_mandate_draft_content` and `documenso_templates` ownership is partly unprovable.** Both are ALTERed but not defined on disk; full column sets and true original creators cannot be confirmed from this repo's DDL — carried as open questions, not asserted.

---

## Open / unverified (carried honestly, not upgraded)

- `business.engagement_mandate_draft_content`: no `CREATE TABLE` on disk (grep-confirmed ZERO). Full column set (beyond `opportunity_id`, `documenso_template_id`, `status`, `prefill_values`, `archetype_id`) and true owner (earlier edge_api deploy vs hq-x) are NOT provable from committed DDL.
- `documenso_webhook_events.envelope_id`: verified as the numeric `document_id` for the DIRECT lane only; the `envelope-distribute` lane's stored value was not independently probed.
- `business.documenso_templates`: labeled documenso-owned on the strength of "predates this file"; whether an earlier edge_api era or the upstream Documenso integration originally created it is NOT provable from current DDL.
- The proposal-confirm path's direct Supabase read of `operator_settings.render_mode`: whether intended to persist or residual pre-gateway access is not stated in code.
