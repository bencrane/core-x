# 01 — Modes & Lanes: the selector layer

> **STATUS BANNER.** This file is the **selector layer** for the entire Documenso/origination domain. It pertains to **all three independent selectors** carried by `public.operator_settings`: `render_mode` (`'through-docraptor'` | `'direct-to-documenso'`), `direct_to_documenso_lane` (`'envelope-distribute'` | `'prefill-document-from-template'`, meaningful **only** under `render_mode='direct-to-documenso'`), and `stripe_mode` (`'test'` | `'live'` | `NULL`). It documents the storage table, the dumb-BFF settings pass-through, the per-tuple routing table, and the edge_api router-mount overview. Downstream lane-specific flows are documented in sibling files; this file tells you **which** flow each tuple selects.

## Orientation

A fresh agent landing in this domain needs to answer one question before touching any flow: **given an operator's saved settings, which downstream path actually runs?** That answer lives in three independent toggles persisted in a single Postgres row (`public.operator_settings`), edited through the cockpit Settings tab. `edge_api` (FastAPI, `apps/edge_api/`) is the **sole gateway** to that table for the Settings surface and the single writer over the HQX Postgres; the `platform-api` BFF (Hono) is a **dumb pass-through** for the settings read/write, and the `platform-app` SPA stages and commits the toggles. The critical, non-obvious fact: the three selectors are consumed in **three different places by three different mechanisms** — `render_mode` server-side at proposal-confirm (where `direct-to-documenso` is a non-raising **stub**), `direct_to_documenso_lane` **client-side** in the SPA (never branched server-side), and `stripe_mode` server-side at the document-payment mint and Stripe webhook. There is one verified discrepancy with the in-code docstrings (the legacy proposal-confirm BFF still reads `operator_settings` directly via Supabase service-role); it is called out explicitly in **Traps**.

---

## The storage table: `public.operator_settings`

One row per operator, in `public` in the **same HQX Postgres** as `business.*` (`apps/edge_api/sql/operator_settings.sql:5`, `apps/edge_api/sql/operator_settings.sql:15`). The table predates this DDL file (originally created live by the BFF); the DDL is now the schema-as-code system-of-record and converges the live table in place (`apps/edge_api/sql/operator_settings.sql:11`).

### Columns

| Column | Type | Nullable / Default | Meaning | Citation |
|---|---|---|---|---|
| `auth_user_id` | `uuid` | `PRIMARY KEY` | Supabase JWT `sub`. Grain key (one row per operator). | `apps/edge_api/sql/operator_settings.sql:40` |
| `render_mode` | `text` | `NOT NULL DEFAULT 'through-docraptor'` | Top-level originate pathway. | `apps/edge_api/sql/operator_settings.sql:41` |
| `direct_to_documenso_lane` | `text` | `NOT NULL DEFAULT 'envelope-distribute'` | Sub-selector; meaningful **only** under `render_mode='direct-to-documenso'`. | `apps/edge_api/sql/operator_settings.sql:42`, `apps/edge_api/sql/operator_settings.sql:49` |
| `stripe_mode` | `text` | **NULLABLE**, no default | Document-payment Stripe toggle. `NULL` = "follow the `STRIPE_MODE` env". | `apps/edge_api/sql/operator_settings.sql:43`, `apps/edge_api/sql/operator_settings.sql:52` |
| `updated_at` | `timestamptz` | `NOT NULL DEFAULT now()` | Set to `now()` on upsert. `null` in the GET response only when no row exists (resolved defaults, never persisted). | `apps/edge_api/sql/operator_settings.sql:44`, `apps/edge_api/src/operator_settings/queries.py:32` |

### DB CHECK constraints (the canonical allowed-value record)

All three are added **idempotently** inside `DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname=...)` guards, so re-running the DDL does not error on an existing constraint (`apps/edge_api/sql/operator_settings.sql:62`, `apps/edge_api/sql/operator_settings.sql:72`, `apps/edge_api/sql/operator_settings.sql:84`).

| Constraint | Rule | Citation |
|---|---|---|
| `operator_settings_render_mode_check` | `render_mode = ANY (ARRAY['through-docraptor','direct-to-documenso'])` | `apps/edge_api/sql/operator_settings.sql:67`, `apps/edge_api/sql/operator_settings.sql:68` |
| `operator_settings_direct_to_documenso_lane_check` | `direct_to_documenso_lane = ANY (ARRAY['envelope-distribute','prefill-document-from-template'])` | `apps/edge_api/sql/operator_settings.sql:79`, `apps/edge_api/sql/operator_settings.sql:80` |
| `operator_settings_stripe_mode_check` | `stripe_mode IS NULL OR stripe_mode = ANY (ARRAY['test','live'])` | `apps/edge_api/sql/operator_settings.sql:91`, `apps/edge_api/sql/operator_settings.sql:92` |

### RLS

RLS stays **ENABLED** but is **no longer load-bearing**: edge_api reaches the table over its pooled application role; the access boundary is the **service token plus the BFF's upstream Supabase session check**. `anon`/`authenticated` have no grants and no access (`apps/edge_api/sql/operator_settings.sql:99`, `apps/edge_api/sql/operator_settings.sql:98`).

---

## The edge_api gateway: GET/PUT `/api/v1/operator-settings/{auth_user_id}`

The router is defined with `prefix="/api/v1/operator-settings"`, `dependencies=[Depends(require_service_token)]` (router-level — every route is service-token gated as a group), and exposes `GET /{auth_user_id}` and `PUT /{auth_user_id}` (`apps/edge_api/src/routers/operator_settings_v1.py:27`, `apps/edge_api/src/routers/operator_settings_v1.py:30`, `apps/edge_api/src/routers/operator_settings_v1.py:34`, `apps/edge_api/src/routers/operator_settings_v1.py:43`). Registered in `main.py` as `app.include_router(operator_settings_router)` (`apps/edge_api/main.py:226`).

The path param `auth_user_id` is typed `UUID`, so a malformed id is rejected with **422 at the edge BEFORE any `::uuid` cast** — protecting the pooled connection from an aborted transaction (`apps/edge_api/src/routers/operator_settings_v1.py:35`, docstring at `apps/edge_api/src/routers/operator_settings_v1.py:13`). **Trust model:** the BFF validates the operator's Supabase session upstream and asserts the trusted `auth_user_id`; edge_api TRUSTS it and never re-validates — the same trust model as `agent_runs` (`apps/edge_api/src/routers/operator_settings_v1.py:6`).

### GET semantics — resolved defaults when no row exists

`get_settings` returns the row, or the **resolved defaults** when no row exists, so the BFF always receives a usable pathway (`apps/edge_api/src/operator_settings/queries.py:13`):

```text
if no row for auth_user_id:
    return {
        render_mode:              DEFAULT_RENDER_MODE                # 'through-docraptor'
        direct_to_documenso_lane: DEFAULT_DIRECT_TO_DOCUMENSO_LANE   # 'envelope-distribute'
        stripe_mode:              None
        updated_at:               None       # <- null ONLY here (never persisted)
    }
```

(`apps/edge_api/src/operator_settings/queries.py:27`–`apps/edge_api/src/operator_settings/queries.py:33`.) Python-side defaults `DEFAULT_RENDER_MODE='through-docraptor'` and `DEFAULT_DIRECT_TO_DOCUMENSO_LANE='envelope-distribute'` are kept in lockstep with the DB column defaults and imported into `queries.py` (`apps/edge_api/src/operator_settings/models.py:26`, `apps/edge_api/src/operator_settings/models.py:27`, `apps/edge_api/src/operator_settings/queries.py:10`).

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
| `DirectToDocumensoLane` | `Literal['envelope-distribute','prefill-document-from-template']` | `apps/edge_api/src/operator_settings/models.py:16` |
| `StripeMode` | `Literal['test','live']` | `apps/edge_api/src/operator_settings/models.py:20` |

`OperatorSettingsUpsert` has **all three fields Optional** (`render_mode|None`, `direct_to_documenso_lane|None`, `stripe_mode|None`) — the cockpit save payload merges, so toggling one never clobbers another (`apps/edge_api/src/operator_settings/models.py:41`, `apps/edge_api/src/operator_settings/models.py:46`–`apps/edge_api/src/operator_settings/models.py:48`).

---

## The dumb-BFF settings pass-through: GET/PUT `/api/v1/settings`

The platform-api BFF exposes `settingsRoutes` mounted at `app.route('/api/v1/settings', settingsRoutes)` in `index.ts` (`rare-structure-hq:apps/platform-api/src/routes/settings.ts:63`, `rare-structure-hq:apps/platform-api/src/routes/settings.ts:74`, `rare-structure-hq:apps/platform-api/src/index.ts:40`, `rare-structure-hq:apps/platform-api/src/index.ts:110`).

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

- `requireUser` verifies the Supabase JWT against the JWKS (issuer + `audience='authenticated'`), extracts `payload.sub` as `user_id`, stashes `{user_id, email}` on the Hono context; a missing bearer or missing sub yields **401** (`rare-structure-hq:apps/platform-api/src/auth.ts:38`, `rare-structure-hq:apps/platform-api/src/auth.ts:43`, `rare-structure-hq:apps/platform-api/src/auth.ts:50`, `rare-structure-hq:apps/platform-api/src/auth.ts:31`).
- The BFF assigns the **validated JWT `sub`** as the `auth_user_id` path id — the operator cannot supply an arbitrary id; it comes from `c.get('user').user_id`, never the request body/query (`rare-structure-hq:apps/platform-api/src/routes/settings.ts:64`, `rare-structure-hq:apps/platform-api/src/routes/settings.ts:66`, `rare-structure-hq:apps/platform-api/src/routes/settings.ts:115`).
- `edgeGetOperatorSettings`/`edgePutOperatorSettings` call edge_api with `serviceHeaders` (`Authorization: Bearer ${EDGE_API_SERVICE_TOKEN}`); the PUT body is snake_case `{render_mode?, direct_to_documenso_lane?, stripe_mode?}` (`rare-structure-hq:apps/platform-api/src/lib/edge.ts:645`, `rare-structure-hq:apps/platform-api/src/lib/edge.ts:653`, `rare-structure-hq:apps/platform-api/src/lib/edge.ts:655`, `rare-structure-hq:apps/platform-api/src/lib/edge.ts:35`).
- Per-field validation (defense-in-depth on top of the edge Literal + DB CHECK) returns a clean **400** on a bad enum and forwards only supplied fields (`rare-structure-hq:apps/platform-api/src/routes/settings.ts:90`, `rare-structure-hq:apps/platform-api/src/routes/settings.ts:96`, `rare-structure-hq:apps/platform-api/src/routes/settings.ts:107`, `rare-structure-hq:apps/platform-api/src/routes/settings.ts:82`).
- `toOperatorSettings` maps snake_case → camelCase and resolves an unset `stripe_mode` (null) to `DEFAULT_STRIPE_MODE='live'` on the wire to the SPA (`rare-structure-hq:apps/platform-api/src/routes/settings.ts:42`, `rare-structure-hq:apps/platform-api/src/routes/settings.ts:56`–`rare-structure-hq:apps/platform-api/src/routes/settings.ts:59`).
- An `EdgeError` is surfaced as **HTTP 502** by both handlers (`rare-structure-hq:apps/platform-api/src/routes/settings.ts:69`, `rare-structure-hq:apps/platform-api/src/routes/settings.ts:118`).

### Shared schema (canonical values + defaults)

| Set | Values | Default | Citation |
|---|---|---|---|
| `RENDER_MODES` | `['through-docraptor','direct-to-documenso']` | `DEFAULT_RENDER_MODE='through-docraptor'` | `rare-structure-hq:packages/shared/src/schemas/settings.ts:13`, `:15` |
| `DIRECT_TO_DOCUMENSO_LANES` | `['envelope-distribute','prefill-document-from-template']` | `DEFAULT_DIRECT_TO_DOCUMENSO_LANE='envelope-distribute'` | `rare-structure-hq:packages/shared/src/schemas/settings.ts:30`, `:35` |
| `STRIPE_MODES` | `['test','live']` | `DEFAULT_STRIPE_MODE='live'` (UI/wire surface) | `rare-structure-hq:packages/shared/src/schemas/settings.ts:45`, `:47` |

> **`DEFAULT_STRIPE_MODE='live'` (shared/wire) ≠ the env default.** `config.stripe_mode()` returns `os.environ.get('STRIPE_MODE','test')` — the **env-level default is `'test'`** (fail-safe so an unset/typo'd mode never accidentally hits live rails). These are different layers, not contradictory (`rare-structure-hq:packages/shared/src/schemas/settings.ts:47`, `apps/edge_api/src/config.py:62`, `apps/edge_api/src/config.py:61`).

---

## The routing table: which (mode, lane) tuple selects which downstream flow

The three selectors are consumed in **three distinct places by three distinct mechanisms**.

| Selector | Consumed where | Mechanism | Downstream branch | Status |
|---|---|---|---|---|
| `render_mode` | edge_api `POST /api/v1/proposals/{ref}/confirm` → `_provision` | **server-side** branch | `through-docraptor`/None → DocRaptor render + Documenso envelope (**wired**); `direct-to-documenso` → non-raising **STUB** | CONDITIONAL |
| `direct_to_documenso_lane` | SPA `MandateDraftShell.confirm()` | **client-side** endpoint pick | `prefill-document-from-template` → `originate-prefilled`; else `envelope-distribute` → `confirm` | ACTIVE |
| `stripe_mode` | edge_api document-payment mint + Stripe webhook | **server-side** resolution | `resolve_stripe_mode(get_stripe_mode_selection())` picks mode-specific keys | ACTIVE |

### `render_mode` routing (server-side, at proposal-confirm)

`_provision` branches on `render_mode` (`apps/edge_api/src/routers/proposals_v1.py:89`, called at `apps/edge_api/src/routers/proposals_v1.py:243` as `ok, err = await _provision(conn, updated, render_mode=body.render_mode)`):

```text
async def _provision(conn, p, render_mode=None):
    if render_mode == "direct-to-documenso":
        logger.info("... pathway not yet wired")          # proposals_v1.py:102
        return False, "direct-to-documenso pathway not yet wired"   # STUB — proposals_v1.py:103
    try:                                                   # proposals_v1.py:104 — the WIRED branch
        pdf = await docraptor_client.render_pdf(...)       # DocRaptor render
        env = await documenso_client.create_signing_envelope(...)   # Documenso envelope
        ...
```

The `direct-to-documenso` branch is a **non-raising stub** — it logs and returns `(False, 'direct-to-documenso pathway not yet wired')`; the committed draft row survives and is re-provisionable (`apps/edge_api/src/routers/proposals_v1.py:99`–`apps/edge_api/src/routers/proposals_v1.py:103`). Only the `through-docraptor` branch of the **proposal-confirm path** is wired.

**Who resolves `render_mode` for this path:** the proposal-confirm BFF (`proposals-admin.ts`) reads it **server-side, directly from `public.operator_settings`** via the Supabase service-role client — **not** via the edge gateway (`rare-structure-hq:apps/platform-api/src/routes/proposals-admin.ts:132`–`rare-structure-hq:apps/platform-api/src/routes/proposals-admin.ts:137`), absent-row falling back to `DEFAULT_RENDER_MODE`, then forwards `render_mode` into `edgeConfirmProposal` (`rare-structure-hq:apps/platform-api/src/routes/proposals-admin.ts:146`). `lib/db.ts` is the Supabase service-role client (`HQX_SUPABASE_URL` + `HQX_SUPABASE_SERVICE_ROLE_KEY`) that **bypasses RLS** (`rare-structure-hq:apps/platform-api/src/lib/db.ts:15`, `rare-structure-hq:apps/platform-api/src/lib/db.ts:18`, `rare-structure-hq:apps/platform-api/src/lib/db.ts:4`). **See Traps** — this is the one documented discrepancy with the "BFF no longer touches the table" docstrings.

### `direct_to_documenso_lane` routing (client-side, in the SPA)

The lane is decided **purely client-side**. There is **no server-side branch on the lane column anywhere** (verified by grep across `edge_api/src` and `platform-api/src`: the column appears only in the operator_settings storage module in edge_api, and only in `edge.ts` type/body + `settings.ts` gateway in platform-api). The SPA `MandateDraftShell` reads `directToDocumensoLane` via `useOriginationMode` and picks the endpoint:

```text
const { directToDocumensoLane } = useOriginationMode();   // MandateDraftShell.tsx:72
async function confirm() {
  if (directToDocumensoLane === "prefill-document-from-template") {
    originatePrefilled(token, draftId);   // -> POST .../{id}/originate-prefilled   (MandateDraftShell.tsx:93-94)
  } else {
    confirmMandateDraft(token, draftId);  // -> POST .../{id}/confirm  (envelope-distribute)  (MandateDraftShell.tsx:104)
  }
}
```

(`rare-structure-hq:apps/platform-app/src/proposals/MandateDraftShell.tsx:72`, `:93`, `:94`, `:104`.) The two BFF lane endpoints are independent handlers, **neither reading `operator_settings`**: `POST /:id/confirm` → `edgeConfirmMandateDraft` (envelope-distribute lane); `POST /:id/originate-prefilled` → `edgeOriginatePrefilled` (prefill lane, returns `opportunity_id` + `document_id` for the `/p/m/{opportunityId}/{documentId}` link) (`rare-structure-hq:apps/platform-api/src/routes/engagement-mandate-drafts-admin.ts:121`, `:124`, `:142`, `:145`).

### `stripe_mode` routing (server-side, at the document-payment mint + webhook)

At `POST /api/v1/documenso/payment-intent/{opportunity_id}/{document_id}` the mint resolves the effective mode and mode-specific keys at request time (`apps/edge_api/src/routers/document_payments_v1.py:85`):

```text
mode = config.resolve_stripe_mode(await pay_queries.get_stripe_mode_selection(conn))   # document_payments_v1.py:96
publishable_key = config.stripe_publishable_key_for_mode(mode)                          # :97
secret_key      = config.stripe_secret_key_for_mode(mode)                               # :98
```

The Stripe webhook does the **same resolution** on the document settled-rail path: `config.resolve_stripe_mode(await doc_pay_queries.get_stripe_mode_selection(conn))` (`apps/edge_api/src/routers/webhooks_stripe.py:196`). These are the **only two** consumers of `get_stripe_mode_selection`.

`get_stripe_mode_selection` reads the **GLOBAL** selection (single-operator platform; the prospect-facing mint has no operator session) — latest non-null row wins, else `None` to fall back to the env (`apps/edge_api/src/document_payments/queries.py:64`):

```sql
SELECT stripe_mode FROM public.operator_settings
 WHERE stripe_mode IS NOT NULL
 ORDER BY updated_at DESC
 LIMIT 1
```

(`apps/edge_api/src/document_payments/queries.py:72`–`apps/edge_api/src/document_payments/queries.py:80`.) `resolve_stripe_mode(selection)`: if `selection in ('test','live')` it indirects through `STRIPE_MODE_{TEST,LIVE}` env (falling back to the literal selection); otherwise (`None`) it falls back to `config.stripe_mode()` (the `STRIPE_MODE` env, default `'test'`) (`apps/edge_api/src/config.py:99`, `:102`, `:103`, `:104`).

### `render_mode='through-docraptor'` IGNORES the lane

When `render_mode='through-docraptor'`, `direct_to_documenso_lane` is **ignored** — documented in the DDL ("Ignored when `render_mode = 'through-docraptor'`", `apps/edge_api/sql/operator_settings.sql:34`; sub-selector "ONLY applies when `render_mode = 'direct-to-documenso'`", `apps/edge_api/sql/operator_settings.sql:23`) and in the shared schema (`rare-structure-hq:packages/shared/src/schemas/settings.ts:18`). Because there is **no runtime server branch on the lane at all**, for docraptor it is moot by construction.

---

## Cross-repo handoff path map

| Flow | SPA call | BFF route | edge_api route | terminal |
|---|---|---|---|---|
| Settings **read** | `getSettings` | `GET /api/v1/settings` → `edgeGetOperatorSettings` | `GET /api/v1/operator-settings/{auth_user_id}` | `get_settings` |
| Settings **write** | `useOriginationMode.putSettings` | `PUT /api/v1/settings` → `edgePutOperatorSettings` | `PUT /api/v1/operator-settings/{auth_user_id}` | `upsert_settings` |
| `render_mode` (proposal originate) | confirm | `POST /api/v1/proposals/:ref/confirm` (**reads `operator_settings.render_mode` directly via Supabase service-role**) → `edgeConfirmProposal` | `POST /api/v1/proposals/{ref}/confirm` | `_provision` (docraptor wired / direct-to-documenso stub) |
| `direct_to_documenso_lane` (mandate originate) | `MandateDraftShell.confirm()` (**client-side lane pick**) | `POST /api/v1/engagement-mandate-drafts/:id/confirm` **or** `.../:id/originate-prefilled` | `confirm` **or** `originate-prefilled` | lane-specific |
| `stripe_mode` (document payment) | prospect SPA | BFF | `POST /api/v1/documenso/payment-intent/{opp}/{doc}` | `resolve_stripe_mode(get_stripe_mode_selection())` |

The SPA `useOriginationMode` hook stages `render_mode`/`lane`/`stripe_mode` locally (no network) and commits all three in **one PUT** to `/api/v1/settings`; it skips the call entirely under the DEV mock session (`token === 'dev'`) (`rare-structure-hq:apps/platform-app/src/settings/originationMode.ts:94`, `:149`, `:150`, `:154`). The `Settings.tsx` `OriginationModeCard` renders the lane sub-selector **only** when `selected === 'direct-to-documenso'` (`showLaneSelector`), matching the column's conditional semantics; the Stripe-mode selector is **always** shown (`rare-structure-hq:apps/platform-app/src/routes/app/Settings.tsx:151`, `:201`, `:251`).

---

## Wiring / router-mount overview (which router serves which surface)

edge_api is a public FastAPI app (`apps/edge_api/main.py:146`). Routers each declare their own `prefix=` (mostly `/api/v1/...`); a few are mounted with an extra include-time `prefix="/internal"`; webhooks for cal/Stripe live under `/webhooks` (NOT `/api/v1`) and the Documenso webhook under `/api/v1/documenso`. The selector-relevant mounts:

| Surface | Router prefix | Mount / gate | Citation |
|---|---|---|---|
| Settings gateway | `/api/v1/operator-settings` | `include_router(operator_settings_router)`; router-level `require_service_token` | `apps/edge_api/main.py:226`, `apps/edge_api/src/routers/operator_settings_v1.py:28`, `:30` |
| Proposal confirm/originate (`render_mode`) | `/api/v1/proposals` | `include_router(proposals_router)`; service-token create/provision, PUBLIC ref read | `apps/edge_api/main.py:176`, `apps/edge_api/src/routers/proposals_v1.py:52` |
| Mandate-draft lanes (`direct_to_documenso_lane`) | `/api/v1/engagement-mandate-drafts` | `include_router(engagement_mandate_drafts_router)` | `apps/edge_api/main.py:220`, `apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:37` |
| Document payments (`stripe_mode`) | `/api/v1/documenso` | `include_router(document_payments_router)`; **PUBLIC**, keyed by `(opportunity_id, document_id)` | `apps/edge_api/main.py:186`, `apps/edge_api/src/routers/document_payments_v1.py:31` |
| Documenso webhook | `/api/v1/documenso` | `include_router(documenso_webhooks_router)`; `X-Documenso-Secret` | `apps/edge_api/main.py:181`, `apps/edge_api/src/routers/documenso_webhooks_v1.py:27` |
| Stripe webhook (settles `stripe_mode` rail) | `/webhooks` | `include_router(webhooks_stripe_router)`; signature-gated, **NOT** under `/api/v1` | `apps/edge_api/main.py:260`, `apps/edge_api/src/routers/webhooks_stripe.py:38` |

> Note the **shared prefix collision-by-design**: `document_payments_router` and `documenso_webhooks_router` both declare `prefix="/api/v1/documenso"` with non-overlapping suffixes (`/payment-intent/..`, `/payment/..` vs `/webhook`). Likewise `payments_router` reuses `/api/v1/proposals` (same as `proposals_router`), layering Stripe ACH routes onto the proposal-ref namespace (`apps/edge_api/main.py:256`, `apps/edge_api/src/routers/payments_v1.py:28`).

On the platform side: `app.route('/api/v1/settings', settingsRoutes)` is the dumb gateway (`rare-structure-hq:apps/platform-api/src/index.ts:110`); the proposal-confirm and mandate-draft admin routes live in `proposals-admin.ts` and `engagement-mandate-drafts-admin.ts` respectively.

---

## Status: ACTIVE / CONDITIONAL / DEPRECATED / STUB

| Component | Status | Note |
|---|---|---|
| `public.operator_settings` table + all 3 columns + 3 CHECKs | **ACTIVE** | `apps/edge_api/sql/operator_settings.sql:39` |
| `GET/PUT /api/v1/operator-settings/{auth_user_id}` (edge gateway) | **ACTIVE** | `apps/edge_api/src/routers/operator_settings_v1.py:34`, `:43` |
| `GET/PUT /api/v1/settings` (BFF dumb pass-through) | **ACTIVE** | `rare-structure-hq:apps/platform-api/src/routes/settings.ts:63`, `:74` |
| `render_mode='through-docraptor'` branch in `_provision` | **ACTIVE** | the wired proposal-confirm pathway, `apps/edge_api/src/routers/proposals_v1.py:104` |
| `render_mode='direct-to-documenso'` branch in `_provision` | **STUB** | non-raising "not yet wired", `apps/edge_api/src/routers/proposals_v1.py:103` |
| `direct_to_documenso_lane` client-side routing (`MandateDraftShell`) | **ACTIVE** | both lanes live, `rare-structure-hq:apps/platform-app/src/proposals/MandateDraftShell.tsx:93` |
| `direct_to_documenso_lane` server-side branch | **does NOT exist** | no server reads the lane for routing (grep-confirmed) |
| `stripe_mode` resolution (mint + webhook) | **ACTIVE** | `apps/edge_api/src/routers/document_payments_v1.py:96`, `apps/edge_api/src/routers/webhooks_stripe.py:196` |
| `proposals-admin.ts` direct Supabase read of `render_mode` | **ACTIVE (legacy, discrepant)** | reads `operator_settings` out-of-band from the edge gateway, `rare-structure-hq:apps/platform-api/src/routes/proposals-admin.ts:132` |
| RLS on `public.operator_settings` | **ACTIVE but not load-bearing** | enabled as defense-in-depth only, `apps/edge_api/sql/operator_settings.sql:99` |

---

## Traps

- **The "BFF no longer touches `public.operator_settings`" docstrings are scoped, not absolute.** That claim holds **only for the `/api/v1/settings` gateway route** (`rare-structure-hq:apps/platform-api/src/routes/settings.ts:9`; DDL `apps/edge_api/sql/operator_settings.sql:7`; router `apps/edge_api/src/routers/operator_settings_v1.py:8`). The **proposal-confirm path still reads `operator_settings.render_mode` directly** via the Supabase service-role client (`rare-structure-hq:apps/platform-api/src/routes/proposals-admin.ts:132`–`:137`, `lib/db.ts` at `rare-structure-hq:apps/platform-api/src/lib/db.ts:15`). The **code wins**: do not assume the BFF is a pure pass-through for the render_mode-at-confirm flow. (Whether this is intentional legacy or pending migration is **undocumented / unverified** — carry that uncertainty.)

- **The lane is NEVER routed server-side.** `direct_to_documenso_lane` is selected **only** in the SPA (`MandateDraftShell.tsx:93`). Do not look for an edge_api or BFF branch on the lane column — there isn't one. The lane's two BFF endpoints (`/confirm`, `/originate-prefilled`) are dumb and independent; the SPA decides which one to call.

- **`DEFAULT_STRIPE_MODE='live'` (shared) vs `STRIPE_MODE` env default `'test'`.** These are **different layers** and both correct: `'live'` is the UI/wire resolution the BFF applies when the DB value is null (`rare-structure-hq:packages/shared/src/schemas/settings.ts:47`); `'test'` is the env-level fail-safe (`apps/edge_api/src/config.py:62`). Do not "fix" one to match the other.

- **`stripe_mode` is read GLOBALLY, ignoring `auth_user_id`.** The document-payment mint is prospect-facing with no operator session, so `get_stripe_mode_selection` takes the **latest non-null row** as the platform-wide selection (`apps/edge_api/src/document_payments/queries.py:72`–`:80`). Single-operator assumption baked in.

- **`updated_at` is null in the GET response ONLY when no row exists.** That is the resolved-defaults sentinel (`apps/edge_api/src/operator_settings/queries.py:32`); it is `NOT NULL` in the table itself (`apps/edge_api/sql/operator_settings.sql:44`). Do not infer the column is nullable.

- **The upsert merges via `COALESCE(param, EXISTING)`, NOT `EXCLUDED`.** An omitted PUT field is a **no-op**, not a reset to default (`apps/edge_api/src/operator_settings/queries.py:78`–`:83`). `EXCLUDED` appears only in the docstring as a negation, never in SQL. A naive reading that assumes `EXCLUDED` would wrongly conclude omitted fields reset to default.

- **The Settings UI labels `direct-to-documenso` as "Prototype pathway (not yet wired)"** (`rare-structure-hq:apps/platform-app/src/routes/app/Settings.tsx:93`). That copy is accurate for the **proposal-confirm** path only (the `_provision` stub). The **engagement-mandate-drafts** direct lanes (`confirm` / `originate-prefilled`) ARE fully wired (`rare-structure-hq:apps/platform-app/src/proposals/MandateDraftShell.tsx:94`). Do not conflate the two — "not yet wired" does NOT mean the whole direct-to-documenso surface is dead.

- **Two routers share `prefix="/api/v1/documenso"`** (`document_payments_v1` and `documenso_webhooks_v1`), and `payments_v1` reuses `/api/v1/proposals`. Suffixes are non-overlapping by design — do not assume a one-router-per-prefix mapping when tracing a path.
