# 01 — Modes & Lanes: the selector layer

> **STATUS BANNER.** This file is the **selector layer** for the entire Documenso/origination domain. It pertains to **all three independent selectors** carried by `public.operator_settings`: `render_mode` (`'through-docraptor'` | `'direct-to-documenso'`), `direct_to_documenso_lane` (`'envelope-distribute'` (RETIRED) | `'prefill-document-from-template'` (DEFAULT) | `'embed-template'`, meaningful **only** under `render_mode='direct-to-documenso'`), and `stripe_mode` (`'test'` | `'live'` | `NULL`). It documents the storage table, the dumb-BFF settings pass-through, the per-tuple routing table, and the edge_api router-mount overview. Downstream lane-specific flows are documented in sibling files; this file tells you **which** flow each tuple selects.

## Orientation

A fresh agent landing in this domain needs to answer one question before touching any flow: **given an operator's saved settings, which downstream path actually runs?** That answer lives in three independent toggles persisted in a single Postgres row (`public.operator_settings`), edited through the cockpit Settings tab. `edge_api` (FastAPI, `apps/edge_api/`) is the **sole gateway** to that table for the Settings surface and the single writer over the HQX Postgres; the `platform-api` BFF (Hono) is a **dumb pass-through** for the settings read/write, and the `platform-app` SPA stages and commits the toggles. The critical, non-obvious fact: the selectors are consumed in **different places by different mechanisms** — `render_mode` and `direct_to_documenso_lane` are now **both client-side** in the SPA (neither is branched server-side), and `stripe_mode` is **server-side** at the document-payment mint and Stripe webhook. The server-side `render_mode` consumer that older revisions of this doc described (the `_provision` branch at proposal-confirm) was **deleted** along with the entire `/api/v1/proposals` backend (#531, #533); `render_mode` now only picks which origination *surface* the SPA shows, and its persisted-default `through-docraptor` surface routes to backend routes that no longer exist. This is called out explicitly in **Traps**.

---

## The storage table: `public.operator_settings`

One row per operator, in `public` in the **same HQX Postgres** as `business.*` (`apps/edge_api/sql/operator_settings.sql:5`, `apps/edge_api/sql/operator_settings.sql:15`). The table predates this DDL file (originally created live by the BFF); the DDL is now the schema-as-code system-of-record and converges the live table in place (`apps/edge_api/sql/operator_settings.sql:11`).

### Columns

| Column | Type | Nullable / Default | Meaning | Citation |
|---|---|---|---|---|
| `auth_user_id` | `uuid` | `PRIMARY KEY` | Supabase JWT `sub`. Grain key (one row per operator). | `apps/edge_api/sql/operator_settings.sql:41` |
| `render_mode` | `text` | `NOT NULL DEFAULT 'through-docraptor'` | Originate pathway. **Persisted/validated only** — now a client-side SPA selector with no server-side consumer (see routing table). | `apps/edge_api/sql/operator_settings.sql:42` |
| `direct_to_documenso_lane` | `text` | `NOT NULL DEFAULT 'prefill-document-from-template'` | Sub-selector; meaningful **only** under `render_mode='direct-to-documenso'`. | `apps/edge_api/sql/operator_settings.sql:43`, `apps/edge_api/sql/operator_settings.sql:50` |
| `stripe_mode` | `text` | **NULLABLE**, no default | Document-payment Stripe toggle. `NULL` = "follow the `STRIPE_MODE` env". | `apps/edge_api/sql/operator_settings.sql:44`, `apps/edge_api/sql/operator_settings.sql:52` |
| `updated_at` | `timestamptz` | `NOT NULL DEFAULT now()` | Set to `now()` on upsert. `null` in the GET response only when no row exists (resolved defaults, never persisted). | `apps/edge_api/sql/operator_settings.sql:45`, `apps/edge_api/src/operator_settings/queries.py:32` |

### DB CHECK constraints (the canonical allowed-value record)

The `render_mode` and `stripe_mode` constraints are added **idempotently** inside `DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname=...)` guards, so re-running the DDL does not error on an existing constraint (`apps/edge_api/sql/operator_settings.sql:61`, `apps/edge_api/sql/operator_settings.sql:91`). The `direct_to_documenso_lane` constraint instead uses a **guarded DROP + re-ADD** (`DROP CONSTRAINT IF EXISTS ... ; ADD CONSTRAINT ...`) so a newly-widened value set converges on every apply — a guarded add-if-missing would never widen an existing constraint (`apps/edge_api/sql/operator_settings.sql:81`, `apps/edge_api/sql/operator_settings.sql:83`).

| Constraint | Rule | Citation |
|---|---|---|
| `operator_settings_render_mode_check` | `render_mode = ANY (ARRAY['through-docraptor','direct-to-documenso'])` | `apps/edge_api/sql/operator_settings.sql:68`, `apps/edge_api/sql/operator_settings.sql:69` |
| `operator_settings_direct_to_documenso_lane_check` | `direct_to_documenso_lane = ANY (ARRAY['envelope-distribute','prefill-document-from-template','embed-template'])` | `apps/edge_api/sql/operator_settings.sql:84`–`apps/edge_api/sql/operator_settings.sql:89` |
| `operator_settings_stripe_mode_check` | `stripe_mode IS NULL OR stripe_mode = ANY (ARRAY['test','live'])` | `apps/edge_api/sql/operator_settings.sql:98`, `apps/edge_api/sql/operator_settings.sql:99` |

### RLS

RLS stays **ENABLED** but is **no longer load-bearing**: edge_api reaches the table over its pooled application role; the access boundary is the **service token plus the BFF's upstream Supabase session check**. `anon`/`authenticated` have no grants and no access (`apps/edge_api/sql/operator_settings.sql:106`, rationale comment `apps/edge_api/sql/operator_settings.sql:103`–`:105`).

---

## The edge_api gateway: GET/PUT `/api/v1/operator-settings/{auth_user_id}`

The router is defined with `prefix="/api/v1/operator-settings"`, `dependencies=[Depends(require_service_token)]` (router-level — every route is service-token gated as a group), and exposes `GET /{auth_user_id}` and `PUT /{auth_user_id}` (`apps/edge_api/src/routers/operator_settings_v1.py:27`, `apps/edge_api/src/routers/operator_settings_v1.py:30`, `apps/edge_api/src/routers/operator_settings_v1.py:34`, `apps/edge_api/src/routers/operator_settings_v1.py:43`). Registered in `main.py` as `app.include_router(operator_settings_router)` (`apps/edge_api/main.py:257`).

The path param `auth_user_id` is typed `UUID`, so a malformed id is rejected with **422 at the edge BEFORE any `::uuid` cast** — protecting the pooled connection from an aborted transaction (`apps/edge_api/src/routers/operator_settings_v1.py:35`, docstring at `apps/edge_api/src/routers/operator_settings_v1.py:13`). **Trust model:** the BFF validates the operator's Supabase session upstream and asserts the trusted `auth_user_id`; edge_api TRUSTS it and never re-validates — the same trust model as `agent_runs` (`apps/edge_api/src/routers/operator_settings_v1.py:6`).

### GET semantics — resolved defaults when no row exists

`get_settings` returns the row, or the **resolved defaults** when no row exists, so the BFF always receives a usable pathway (`apps/edge_api/src/operator_settings/queries.py:13`):

```text
if no row for auth_user_id:
    return {
        render_mode:              DEFAULT_RENDER_MODE                # 'through-docraptor'
        direct_to_documenso_lane: DEFAULT_DIRECT_TO_DOCUMENSO_LANE   # 'prefill-document-from-template'
        stripe_mode:              None
        updated_at:               None       # <- null ONLY here (never persisted)
    }
```

(`apps/edge_api/src/operator_settings/queries.py:27`–`apps/edge_api/src/operator_settings/queries.py:33`.) Python-side defaults `DEFAULT_RENDER_MODE='through-docraptor'` and `DEFAULT_DIRECT_TO_DOCUMENSO_LANE='prefill-document-from-template'` are kept in lockstep with the DB column defaults and imported into `queries.py` (`apps/edge_api/src/operator_settings/models.py:33`, `apps/edge_api/src/operator_settings/models.py:34`, `apps/edge_api/src/operator_settings/queries.py:10`).

### PUT semantics — merge-upsert via COALESCE-of-existing (NOT EXCLUDED)

`upsert_settings` is the critical correctness primitive: **an omitted (NULL) field is a no-op, not a reset to default** (`apps/edge_api/src/operator_settings/queries.py:42`).

```text
INSERT INTO public.operator_settings (auth_user_id, render_mode, direct_to_documenso_lane, stripe_mode)
VALUES (
    auth_user_id::uuid,
    COALESCE(render_mode, default_render_mode),                 -- INSERT: param else DB default
    COALESCE(direct_to_documenso_lane, default_lane),           -- INSERT: param else DB default
    stripe_mode                                                 -- INSERT: as-is (nullable)
)
ON CONFLICT (auth_user_id) DO UPDATE SET
    render_mode = COALESCE(param, public.operator_settings.render_mode),               -- existing, NOT EXCLUDED
    direct_to_documenso_lane = COALESCE(param, public.operator_settings.direct_to_documenso_lane),
    stripe_mode = COALESCE(param, public.operator_settings.stripe_mode),
    updated_at = now()
```

Verbatim: INSERT branch at `apps/edge_api/src/operator_settings/queries.py:73`–`apps/edge_api/src/operator_settings/queries.py:75`; the `DO UPDATE` branch references the **bound params and the EXISTING row** (`public.operator_settings.<col>`), **not `EXCLUDED`**, at `apps/edge_api/src/operator_settings/queries.py:78`–`apps/edge_api/src/operator_settings/queries.py:83`; `updated_at = now()` at `apps/edge_api/src/operator_settings/queries.py:84`; explicit `await conn.commit()` before returning at `apps/edge_api/src/operator_settings/queries.py:90`. The docstring explains *why* (EXCLUDED already had the INSERT-branch default applied, which would defeat the merge) at `apps/edge_api/src/operator_settings/queries.py:53`.

### Pydantic models mirror the DB CHECKs

`models.py` declares Literal types so a bad value yields a clean **422 at the edge** instead of reaching a CHECK violation that would abort the pooled transaction (`apps/edge_api/src/operator_settings/models.py:3`):

| Type | Allowed values | Citation |
|---|---|---|
| `RenderMode` | `Literal['through-docraptor','direct-to-documenso']` | `apps/edge_api/src/operator_settings/models.py:15` |
| `DirectToDocumensoLane` | `Literal['envelope-distribute','prefill-document-from-template','embed-template']` | `apps/edge_api/src/operator_settings/models.py:21`–`apps/edge_api/src/operator_settings/models.py:23` |
| `StripeMode` | `Literal['test','live']` | `apps/edge_api/src/operator_settings/models.py:27` |

`OperatorSettingsUpsert` has **all three fields Optional** (`render_mode|None`, `direct_to_documenso_lane|None`, `stripe_mode|None`) — the cockpit save payload merges, so toggling one never clobbers another (class `apps/edge_api/src/operator_settings/models.py:48`, fields `:53`–`:55`).

---

## The dumb-BFF settings pass-through: GET/PUT `/api/v1/settings`

The platform-api BFF exposes `settingsRoutes` mounted at `app.route('/api/v1/settings', settingsRoutes)` in `index.ts` (`rare-structure-hq:apps/platform-api/src/routes/settings.ts:63`, `rare-structure-hq:apps/platform-api/src/routes/settings.ts:74`, `rare-structure-hq:apps/platform-api/src/index.ts:39`, `rare-structure-hq:apps/platform-api/src/index.ts:115`).

This route is a **DUMB pass-through** — it does NOT touch `public.operator_settings` directly (`rare-structure-hq:apps/platform-api/src/routes/settings.ts:9`):

```text
GET /api/v1/settings:
  requireUser  -> validate Supabase JWT, user = c.get('user')
  edgeGetOperatorSettings(user.user_id)          # JWT sub asserted as path id
  -> toOperatorSettings(edge)                     # snake_case -> camelCase
  EdgeError -> HTTP 502

PUT /api/v1/settings:
  requireUser
  validate-if-present each field (clean 400 on bad enum)
  forward ONLY supplied fields to edgePutOperatorSettings(user.user_id, edgeBody)
  -> toOperatorSettings(edge)
  EdgeError -> HTTP 502
```

- `requireUser` verifies the Supabase JWT against the JWKS (issuer + `audience='authenticated'`), extracts `payload.sub` as `user_id`, stashes `{user_id, email}` on the Hono context; a missing bearer or missing sub yields **401** (`rare-structure-hq:apps/platform-api/src/auth.ts:38`, `rare-structure-hq:apps/platform-api/src/auth.ts:43`, `rare-structure-hq:apps/platform-api/src/auth.ts:45`, `rare-structure-hq:apps/platform-api/src/auth.ts:32`).
- The BFF assigns the **validated JWT `sub`** as the `auth_user_id` path id — the operator cannot supply an arbitrary id; it comes from `c.get('user').user_id`, never the request body/query (`rare-structure-hq:apps/platform-api/src/routes/settings.ts:64`, `rare-structure-hq:apps/platform-api/src/routes/settings.ts:66`, `rare-structure-hq:apps/platform-api/src/routes/settings.ts:115`).
- `edgeGetOperatorSettings`/`edgePutOperatorSettings` call edge_api with `serviceHeaders` (`Authorization: Bearer ${EDGE_API_SERVICE_TOKEN}`); the PUT body is snake_case `{render_mode?, direct_to_documenso_lane?, stripe_mode?}` (`rare-structure-hq:apps/platform-api/src/lib/edge.ts:453`, `rare-structure-hq:apps/platform-api/src/lib/edge.ts:461`, `rare-structure-hq:apps/platform-api/src/lib/edge.ts:463`, `rare-structure-hq:apps/platform-api/src/lib/edge.ts:34`).
- Per-field validation (defense-in-depth on top of the edge Literal + DB CHECK) returns a clean **400** on a bad enum and forwards only supplied fields (`rare-structure-hq:apps/platform-api/src/routes/settings.ts:90`, `rare-structure-hq:apps/platform-api/src/routes/settings.ts:96`, `rare-structure-hq:apps/platform-api/src/routes/settings.ts:107`, `rare-structure-hq:apps/platform-api/src/routes/settings.ts:82`).
- `toOperatorSettings` maps snake_case → camelCase and resolves an unset `stripe_mode` (null) to `DEFAULT_STRIPE_MODE='live'` on the wire to the SPA (`rare-structure-hq:apps/platform-api/src/routes/settings.ts:42`, `rare-structure-hq:apps/platform-api/src/routes/settings.ts:56`–`rare-structure-hq:apps/platform-api/src/routes/settings.ts:59`).
- An `EdgeError` is surfaced as **HTTP 502** by both handlers (`rare-structure-hq:apps/platform-api/src/routes/settings.ts:69`, `rare-structure-hq:apps/platform-api/src/routes/settings.ts:118`).

### Shared schema (canonical values + defaults)

| Set | Values | Default | Citation |
|---|---|---|---|
| `RENDER_MODES` | `['through-docraptor','direct-to-documenso']` | `DEFAULT_RENDER_MODE='through-docraptor'` | `rare-structure-hq:packages/shared/src/schemas/settings.ts:13`, `:15` |
| `DIRECT_TO_DOCUMENSO_LANES` | `['envelope-distribute','prefill-document-from-template','embed-template']` | `DEFAULT_DIRECT_TO_DOCUMENSO_LANE='envelope-distribute'` — ⚠️ **DIVERGES** from edge_api's `'prefill-document-from-template'` | `rare-structure-hq:packages/shared/src/schemas/settings.ts:37`–`:41`, `:43` (cf. edge_api `models.py:34`, DB default `sql:43`,`:50`) |
| `STRIPE_MODES` | `['test','live']` | `DEFAULT_STRIPE_MODE='live'` (UI/wire surface) | `rare-structure-hq:packages/shared/src/schemas/settings.ts:53`, `:55` |

> **`DEFAULT_STRIPE_MODE='live'` (shared/wire) ≠ the env default.** `config.stripe_mode()` returns `os.environ.get('STRIPE_MODE','test')` — the **env-level default is `'test'`** (fail-safe so an unset/typo'd mode never accidentally hits live rails). These are different layers, not contradictory (`rare-structure-hq:packages/shared/src/schemas/settings.ts:55`, `apps/edge_api/src/config.py:62`, `apps/edge_api/src/config.py:61`).

---

## The routing table: which (mode, lane) tuple selects which downstream flow

The three selectors are consumed in distinct places: `render_mode` and `direct_to_documenso_lane` are now **both client-side** (SPA), `stripe_mode` is **server-side** (edge_api).

| Selector | Consumed where | Mechanism | Downstream branch | Status |
|---|---|---|---|---|
| `render_mode` | SPA `Mandate.tsx` + `ProspectDossierBoard.tsx` (`useOriginationMode`) | **client-side** branch | `direct-to-documenso` → engagement-mandate-draft lanes (**wired**); `through-docraptor` (DEFAULT) → legacy `/api/v1/proposals/*` path (**backend DELETED** — dangling client calls) | PERSISTED, CLIENT-ONLY (server consumer removed) |
| `direct_to_documenso_lane` | SPA `MandateDraftShell.confirm()` | **client-side** endpoint pick | `prefill-document-from-template` → `originate-prefilled`; `embed-template` → `originate-embed-template`; `envelope-distribute` → `confirm` (RETIRED) | ACTIVE |
| `stripe_mode` | edge_api document-payment mint + Stripe webhook | **server-side** resolution | `resolve_stripe_mode(get_stripe_mode_selection())` picks mode-specific keys | ACTIVE |

### `render_mode` routing (CLIENT-SIDE, in the SPA — the server-side consumer was DELETED)

**There is no server-side branch on `render_mode` anywhere.** The proposal-confirm provision that once consumed it — `proposals_v1.py::_provision` (`through-docraptor` → DocRaptor render + Documenso envelope; `direct-to-documenso` → non-raising stub) and the whole `POST /api/v1/proposals/{ref}/confirm` route — was **deleted**: `refactor(edge_api): prune dead/broken Documenso originate paths` (#531) and `refactor(edge_api): remove legacy through-docraptor proposal + payment backend` (#533). `apps/edge_api/src/routers/proposals_v1.py` and `payments_v1.py` no longer exist; `grep -rn "_provision\|/api/v1/proposals" apps/edge_api/src/routers/` returns only stale comments (`documenso_webhooks_v1.py:5`). On the BFF side the matching route was removed too — `proposals-admin.ts` now serves **only** the published-template picker (`GET /api/v1/proposal-templates`), its header stating "The proposal create/confirm/list/read/send/pay surface (through-docraptor) has been removed" (`rare-structure-hq:apps/platform-api/src/routes/proposals-admin.ts:6`–`:8`, `:23`–`:24`); there is no `edgeConfirmProposal` in `edge.ts`.

`render_mode` now survives in core-x as exactly two inert artifacts: the `operator_settings.render_mode` column (still `NOT NULL DEFAULT 'through-docraptor'`, 2-value CHECK — `apps/edge_api/sql/operator_settings.sql:42`, `:69`) and a nullable, **unconsumed** `render_mode` field on the `ProposalConfirm` model (`apps/edge_api/src/proposals/models.py:103`, comment `:102`). No edge_api code reads either for routing.

**Where it IS consumed: client-side, in the platform-app SPA.** `render_mode` is read via `useOriginationMode()` and branches which origination *surface* the operator gets:

```text
// Mandate.tsx — the /app/m/:ref cockpit page
const { renderMode } = useOriginationMode();                                   // Mandate.tsx:23
const proposalRef = renderMode === "direct-to-documenso" ? undefined : ref;    // :28 (gate the proposal fetch off)
if (renderMode === "direct-to-documenso")
    return <MandateDraftShell draftId={ref} ... />;                            // :36–:37  → the WIRED direct lanes
// else: `ref` is a PROPOSAL ref → <MandateEditor> → useProposalDraft.submit → confirmProposal()

// ProspectDossierBoard.tsx — "Originate"
const { renderMode } = useOriginationMode();                                   // ProspectDossierBoard.tsx:108
if (renderMode === "direct-to-documenso") {                                    // :179
    createMandateDraft(...); navigate(`/app/m/${draftId}`);                    // :183, :189  → mandate-draft lane
} else {
    createProposal(...);                                                       // :192        → legacy proposal path
}
```

So `'direct-to-documenso'` routes to the engagement-mandate-draft lanes (`originate-prefilled` / `originate-embed-template`), which **are** fully wired (see `direct_to_documenso_lane` below). `'through-docraptor'` — the persisted DEFAULT — routes the SPA to the **legacy proposal path** (`createProposal`, `getProposalShell`, `confirmProposal`, `sendProposal` in `rare-structure-hq:apps/platform-app/src/proposals/api.ts:236`, `:241`, `:253`), every one of which now targets a `/api/v1/proposals/*` route that **exists in neither edge_api nor the BFF** — a dangling client surface, not a live flow. **See Traps.**

**Where the DocRaptor RENDER engine actually lives now** (it did NOT move into a `render_mode` branch): the standalone **engagement-template render+push** lane (content source → DocRaptor PDF → Documenso **TEMPLATE**, `POST /internal/engagement-templates/render-push`; `apps/edge_api/src/engagement_templates/render.py:74`, `push.py:7`, `apps/edge_api/src/routers/internal_engagement_templates_v1.py:3`) and the **proposal-template preview** (`POST /api/v1/proposal-templates/preview` → DocRaptor → R2; `apps/edge_api/src/routers/proposal_templates_v1.py:69`–`:76`). Neither is gated by `render_mode`, and neither mints a per-proposal envelope at confirm.

### `direct_to_documenso_lane` routing (client-side, in the SPA)

The lane is decided **purely client-side**. There is **no server-side branch on the lane column anywhere** (verified by grep across `edge_api/src` and `platform-api/src`: the column appears only in the operator_settings storage module in edge_api, and only in `edge.ts` type/body + `settings.ts` gateway in platform-api). The SPA `MandateDraftShell` reads `directToDocumensoLane` via `useOriginationMode` and dispatches across the **two live** lanes (`envelope-distribute` is retired and has **no client caller**):

```text
const { directToDocumensoLane } = useOriginationMode();   // MandateDraftShell.tsx:86
async function confirm() {                                 // :101
  if (directToDocumensoLane === "embed-template") {        // :108
    originateEmbedTemplate(token, draftId);   // -> POST .../{id}/originate-embed-template   (embed-template lane)   :111
  } else {
    originatePrefilled(token, draftId);       // -> POST .../{id}/originate-prefilled        (prefill / embed-document lane — the DEFAULT)   :124
  }
}
// envelope-distribute (.../{id}/confirm) has NO client caller — both the lane dispatch and the edge endpoint were retired.
```

The edge_api lane endpoints are independent handlers, **none reading `operator_settings`** (two live, one retired):
- `POST /{draft_id}/originate-prefilled` (the canonical prefill / embed-document lane) mints a Documenso DOCUMENT now via `create_document_from_template`, distributes `NONE` → `PENDING`, and returns `opportunity_id` + `document_id` (+ `signing_token`) for the `/p/m/{opportunityId}/{documentId}` link — `MandatePrefilledOriginated` (`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:109`–`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:165`).
- `POST /{draft_id}/originate-embed-template` (the embed-template lane, **PARALLEL** to prefill) enables a Documenso DIRECT LINK on the draft's template via `get_template_recipients` + `create_direct_link` and returns the reusable `direct_token` (+ `embed_url`, `external_id`, `direct_recipient_id`, optional `recipient_email`/`recipient_name`) — `MandateEmbedTemplateOriginated`, `status='ready'`. **No document is minted here**: the signer self-identifies in the embed and Documenso creates the document (source `TEMPLATE_DIRECT_LINK`) at completion (`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:168`–`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:221`, model at `apps/edge_api/src/engagement_mandate_drafts/models.py:49`–`apps/edge_api/src/engagement_mandate_drafts/models.py:70`).
- `POST /{draft_id}/confirm` (envelope-distribute) is **RETIRED** — the `/envelope/use` lane was removed in code; the lane value survives only so a pre-existing row never violates the CHECK (`apps/edge_api/sql/operator_settings.sql:31`–`apps/edge_api/sql/operator_settings.sql:34`, `apps/edge_api/sql/operator_settings.sql:80`).

### `stripe_mode` routing (server-side, at the document-payment mint + webhook)

At `POST /api/v1/documenso/payment-intent/{opportunity_id}/{document_id}` the mint resolves the effective mode and mode-specific keys at request time (`apps/edge_api/src/routers/document_payments_v1.py:85`):

```text
mode = config.resolve_stripe_mode(await pay_queries.get_stripe_mode_selection(conn))   # document_payments_v1.py:96
publishable_key = config.stripe_publishable_key_for_mode(mode)                          # :97
secret_key      = config.stripe_secret_key_for_mode(mode)                               # :98
```

The Stripe webhook does the **same resolution** on the document settled-rail path: `config.resolve_stripe_mode(await doc_pay_queries.get_stripe_mode_selection(conn))` (`apps/edge_api/src/routers/webhooks_stripe.py:155`). These are the **only two** consumers of `get_stripe_mode_selection`.

`get_stripe_mode_selection` reads the **GLOBAL** selection (single-operator platform; the prospect-facing mint has no operator session) — latest non-null row wins, else `None` to fall back to the env (`apps/edge_api/src/document_payments/queries.py:64`):

```sql
SELECT stripe_mode FROM public.operator_settings
 WHERE stripe_mode IS NOT NULL
 ORDER BY updated_at DESC
 LIMIT 1
```

(`apps/edge_api/src/document_payments/queries.py:72`–`apps/edge_api/src/document_payments/queries.py:76`.) `resolve_stripe_mode(selection)`: if `selection in ('test','live')` it indirects through `STRIPE_MODE_{TEST,LIVE}` env (falling back to the literal selection); otherwise (`None`) it falls back to `config.stripe_mode()` (the `STRIPE_MODE` env, default `'test'`) (`apps/edge_api/src/config.py:99`, `:102`, `:103`, `:104`).

### `render_mode='through-docraptor'` IGNORES the lane

When `render_mode='through-docraptor'`, `direct_to_documenso_lane` is **ignored** — documented in the DDL ("Ignored when `render_mode = 'through-docraptor'`", `apps/edge_api/sql/operator_settings.sql:35`; sub-selector "ONLY applies when `render_mode = 'direct-to-documenso'`", `apps/edge_api/sql/operator_settings.sql:23`) and in the shared schema (`rare-structure-hq:packages/shared/src/schemas/settings.ts:20`). Because there is **no runtime server branch on the lane at all**, for docraptor it is moot by construction.

---

## Cross-repo handoff path map

| Flow | SPA call | BFF route | edge_api route | terminal |
|---|---|---|---|---|
| Settings **read** | `getSettings` | `GET /api/v1/settings` → `edgeGetOperatorSettings` | `GET /api/v1/operator-settings/{auth_user_id}` | `get_settings` |
| Settings **write** | `useOriginationMode.putSettings` | `PUT /api/v1/settings` → `edgePutOperatorSettings` | `PUT /api/v1/operator-settings/{auth_user_id}` | `upsert_settings` |
| `render_mode` (origination surface) | `Mandate.tsx` / `ProspectDossierBoard.tsx` (**client-side** `useOriginationMode` branch) | — (no BFF render_mode route; `proposals-admin.ts` now serves only `proposal-templates`) | direct-to-documenso → mandate-draft lanes (wired); through-docraptor → `/api/v1/proposals/*` (**removed in edge_api + BFF**) | SPA picks the surface; server consumer deleted (#531/#533) |
| `direct_to_documenso_lane` (mandate originate) | `MandateDraftShell.confirm()` (**client-side lane pick**) | `POST /api/v1/engagement-mandate-drafts/:id/originate-prefilled` **\|** `.../:id/originate-embed-template` **\|** `.../:id/confirm` (RETIRED) | `originate-prefilled` **\|** `originate-embed-template` **\|** `confirm` | lane-specific |
| `stripe_mode` (document payment) | prospect SPA | BFF | `POST /api/v1/documenso/payment-intent/{opp}/{doc}` | `resolve_stripe_mode(get_stripe_mode_selection())` |

The SPA `useOriginationMode` hook stages `render_mode`/`lane`/`stripe_mode` locally (no network) and commits all three in **one PUT** to `/api/v1/settings`; it skips the call entirely under the DEV mock session (`token === 'dev'`) (`rare-structure-hq:apps/platform-app/src/settings/originationMode.ts:94`, `:149`, `:150`, `:154`). The `Settings.tsx` `OriginationModeCard` renders the lane sub-selector **only** when `selected === 'direct-to-documenso'` (`showLaneSelector`), matching the column's conditional semantics; the Stripe-mode selector is **always** shown (`rare-structure-hq:apps/platform-app/src/routes/app/Settings.tsx:156`, `:206`, `:268`).

---

## Wiring / router-mount overview (which router serves which surface)

edge_api is a public FastAPI app (`apps/edge_api/main.py:151`). Routers each declare their own `prefix=` (mostly `/api/v1/...`); a few are mounted with an extra include-time `prefix="/internal"`; webhooks for cal/Stripe live under `/webhooks` (NOT `/api/v1`) and the Documenso webhook under `/api/v1/documenso`. The selector-relevant mounts:

| Surface | Router prefix | Mount / gate | Citation |
|---|---|---|---|
| Settings gateway | `/api/v1/operator-settings` | `include_router(operator_settings_router)`; router-level `require_service_token` | `apps/edge_api/main.py:257`, `apps/edge_api/src/routers/operator_settings_v1.py:28`, `:30` |
| ~~Proposal confirm/originate (`render_mode`)~~ | ~~`/api/v1/proposals`~~ | **REMOVED** — `proposals_router` + `payments_router` (and their `proposals_v1.py`/`payments_v1.py`) deleted; edge_api serves nothing under `/api/v1/proposals` | gone (#531, #533) |
| Proposal-template authoring (DocRaptor preview/publish) | `/api/v1/proposal-templates` | `include_router(proposal_templates_router)`; service-token | `apps/edge_api/main.py:229`, `apps/edge_api/src/routers/proposal_templates_v1.py:47` |
| Mandate-draft lanes (`direct_to_documenso_lane`) | `/api/v1/engagement-mandate-drafts` | `include_router(engagement_mandate_drafts_router)` | `apps/edge_api/main.py:251`, `apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:40` |
| Document payments (`stripe_mode`) | `/api/v1/documenso` | `include_router(document_payments_router)`; **PUBLIC**, keyed by `(opportunity_id, document_id)` | `apps/edge_api/main.py:224`, `apps/edge_api/src/routers/document_payments_v1.py:31` |
| Documenso webhook | `/api/v1/documenso` | `include_router(documenso_webhooks_router)`; `X-Documenso-Secret` | `apps/edge_api/main.py:219`, `apps/edge_api/src/routers/documenso_webhooks_v1.py:27` |
| Stripe webhook (settles `stripe_mode` rail) | `/webhooks` | `include_router(webhooks_stripe_router)`; signature-gated, **NOT** under `/api/v1` | `apps/edge_api/main.py:299`, `apps/edge_api/src/routers/webhooks_stripe.py:36` |

> Note the **shared prefix collision-by-design**: `document_payments_router` and `documenso_webhooks_router` both declare `prefix="/api/v1/documenso"` with non-overlapping suffixes (`/payment-intent/..`, `/payment/..` vs `/webhook`) — `document_payments_v1.py:31`, `documenso_webhooks_v1.py:27`. (The former `payments_router` that layered Stripe ACH onto `/api/v1/proposals` was deleted alongside `proposals_router` in #531/#533; no router declares that prefix anymore.)

On the platform side: `app.route('/api/v1/settings', settingsRoutes)` is the dumb gateway (`rare-structure-hq:apps/platform-api/src/index.ts:115`); `proposals-admin.ts` now exposes **only** the published-template picker (`GET /api/v1/proposal-templates`, mounted at `index.ts:118`), and the mandate-draft admin routes live in `engagement-mandate-drafts-admin.ts`. The proposal create/confirm/send/pay admin routes were removed.

---

## Status: ACTIVE / CONDITIONAL / DEPRECATED / STUB

| Component | Status | Note |
|---|---|---|
| `public.operator_settings` table + all 3 columns + 3 CHECKs | **ACTIVE** | `apps/edge_api/sql/operator_settings.sql:40` |
| `GET/PUT /api/v1/operator-settings/{auth_user_id}` (edge gateway) | **ACTIVE** | `apps/edge_api/src/routers/operator_settings_v1.py:34`, `:43` |
| `GET/PUT /api/v1/settings` (BFF dumb pass-through) | **ACTIVE** | `rare-structure-hq:apps/platform-api/src/routes/settings.ts:63`, `:74` |
| Server-side `render_mode` branch (`_provision` at proposal-confirm) | **REMOVED** | `proposals_v1.py`/`payments_v1.py` deleted (#531, #533); no edge_api route consumes `render_mode` |
| Client-side `render_mode` branch (SPA origination surface) | **ACTIVE** | `direct-to-documenso` → mandate-draft lanes (wired); `through-docraptor` → legacy `/api/v1/proposals/*` (backend removed), `rare-structure-hq:apps/platform-app/src/routes/app/Mandate.tsx:28`, `:36` |
| `operator_settings.render_mode` column + `ProposalConfirm.render_mode` field | **PERSISTED, server-side UNCONSUMED** | column `apps/edge_api/sql/operator_settings.sql:42`; orphan field `apps/edge_api/src/proposals/models.py:103` |
| `direct_to_documenso_lane` client-side routing (`MandateDraftShell`) | **ACTIVE** | prefill + embed-template lanes live; envelope-distribute RETIRED. edge endpoints `originate-prefilled` / `originate-embed-template` at `apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:109`, `:168` |
| `direct_to_documenso_lane` server-side branch | **does NOT exist** | no server reads the lane for routing (grep-confirmed) |
| `stripe_mode` resolution (mint + webhook) | **ACTIVE** | `apps/edge_api/src/routers/document_payments_v1.py:96`, `apps/edge_api/src/routers/webhooks_stripe.py:155` |
| `proposals-admin.ts` direct Supabase read of `render_mode` | **REMOVED** | file now serves only `GET /api/v1/proposal-templates`; no `operator_settings` read, no confirm route, `rare-structure-hq:apps/platform-api/src/routes/proposals-admin.ts:6`–`:8` |
| RLS on `public.operator_settings` | **ACTIVE but not load-bearing** | enabled as defense-in-depth only, `apps/edge_api/sql/operator_settings.sql:106` |

---

## Traps

- **The render_mode-at-confirm flow is GONE in BOTH repos — but the SPA still ships the client stubs.** edge_api's `proposals_v1.py`/`payments_v1.py` (the `_provision` through-docraptor render→envelope path + the Stripe-ACH backend) were deleted (#531, #533); the BFF's `proposals-admin.ts` confirm/create/send/pay surface was removed (it now serves only `GET /api/v1/proposal-templates`, `rare-structure-hq:apps/platform-api/src/routes/proposals-admin.ts:6`–`:8`), and there is no `edgeConfirmProposal` in `edge.ts`. The earlier "BFF reads `operator_settings.render_mode` directly via Supabase service-role" discrepancy **no longer exists** — `lib/db.ts` is still the service-role client (`rare-structure-hq:apps/platform-api/src/lib/db.ts:15`–`:18`) but is no longer used for settings (now only `/api/v1/me`); `/api/v1/settings` is a pure pass-through (the docstrings at `settings.ts:9`, `operator_settings_v1.py:8`, `operator_settings.sql:7` are now fully accurate). **What remains is dead client code:** `rare-structure-hq:apps/platform-app/src/proposals/api.ts:236`,`:241`,`:253` still defines `confirmProposal` / `createProposal` / `getProposalShell` / `sendProposal`, still imported by live components (`useProposalDraft.ts:109`, `ProspectDossierBoard.tsx:192`, `Mandate.tsx:17`), all POSTing to `/api/v1/proposals/*` routes that no longer exist. Under the persisted DEFAULT `render_mode='through-docraptor'` the SPA routes the operator INTO this dead path; only `direct-to-documenso` reaches a wired backend.

- **The lane is NEVER routed server-side.** `direct_to_documenso_lane` is selected **only** in the SPA (`MandateDraftShell.confirm()`). Do not look for an edge_api or BFF branch on the lane column — there isn't one. The lane's three edge endpoints (`/originate-prefilled`, `/originate-embed-template`, `/confirm`) are dumb and independent; the SPA decides which one to call. `prefill-document-from-template` and `embed-template` are both live; `envelope-distribute` (`/confirm`) is **RETIRED** (`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:109`, `:168`; `apps/edge_api/sql/operator_settings.sql:80`).

- **`embed-template` ≠ `prefill-document-from-template` — different Documenso primitives, different "when is a document created".** The prefill lane mints a Documenso DOCUMENT **at originate** (`create_document_from_template`, source TEMPLATE) and hands back a per-document `signing_token` + numeric `document_id`. The embed-template lane mints **nothing at originate** — it enables a reusable DIRECT LINK on the *template* (`create_direct_link`) and returns a `direct_token`; the document is created by Documenso **at signer completion** (source `TEMPLATE_DIRECT_LINK`), and the signer self-identifies (name/email NOT locked). Do not assume a `document_id` exists right after an embed-template originate — there isn't one until someone signs (`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:175`–`:185`, `apps/edge_api/src/engagement_mandate_drafts/models.py:49`–`:70`).

- **`DEFAULT_STRIPE_MODE='live'` (shared) vs `STRIPE_MODE` env default `'test'`.** These are **different layers** and both correct: `'live'` is the UI/wire resolution the BFF applies when the DB value is null (`rare-structure-hq:packages/shared/src/schemas/settings.ts:55`); `'test'` is the env-level fail-safe (`apps/edge_api/src/config.py:62`). Do not "fix" one to match the other.

- **`stripe_mode` is read GLOBALLY, ignoring `auth_user_id`.** The document-payment mint is prospect-facing with no operator session, so `get_stripe_mode_selection` takes the **latest non-null row** as the platform-wide selection (`apps/edge_api/src/document_payments/queries.py:72`–`:80`). Single-operator assumption baked in.

- **`updated_at` is null in the GET response ONLY when no row exists.** That is the resolved-defaults sentinel (`apps/edge_api/src/operator_settings/queries.py:32`); it is `NOT NULL` in the table itself (`apps/edge_api/sql/operator_settings.sql:45`). Do not infer the column is nullable.

- **The upsert merges via `COALESCE(param, EXISTING)`, NOT `EXCLUDED`.** An omitted PUT field is a **no-op**, not a reset to default (`apps/edge_api/src/operator_settings/queries.py:78`–`:83`). `EXCLUDED` appears only in the docstring as a negation, never in SQL. A naive reading that assumes `EXCLUDED` would wrongly conclude omitted fields reset to default.

- **The Settings UI label `direct-to-documenso` = "Prototype pathway (not yet wired)" is now BACKWARDS** (`rare-structure-hq:apps/platform-app/src/routes/app/Settings.tsx:93`). With `_provision` deleted, the `direct-to-documenso` mandate-draft lanes (`originate-prefilled` / `originate-embed-template`) are the **only** end-to-end-wired originate path; `MandateDraftShell.confirm()` dispatches them client-side (`rare-structure-hq:apps/platform-app/src/proposals/MandateDraftShell.tsx:101`, `:108`). The `through-docraptor` path the label implies is "the wired one" is the path whose backend was removed. Treat the label as stale copy, not a description of current capability.

- **Two routers share `prefix="/api/v1/documenso"`** (`document_payments_v1` and `documenso_webhooks_v1`), with non-overlapping suffixes — do not assume a one-router-per-prefix mapping when tracing a path. (The former `payments_v1` that reused `/api/v1/proposals` was deleted in #531/#533; no router declares `/api/v1/proposals` anymore.)
