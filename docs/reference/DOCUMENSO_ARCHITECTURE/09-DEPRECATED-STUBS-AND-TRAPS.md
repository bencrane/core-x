# 09 — Deprecated Paths, Stubs, and Traps

> **STATUS BANNER.** Cross-cutting hazard list for the Documenso + payment surface across BOTH repos (core-x `apps/edge_api/` and `rare-structure-hq` platform). This file is render_mode/lane-agnostic by design: it catalogs the things that mislead — code that EXISTS but does not RUN, routes that were REMOVED but whose enum values/docstrings linger, tables that do not exist at all, stale docstrings, retired naming, and a retired lane value kept only for backwards compat. The single biggest hazard documented here: **`render_mode = 'direct-to-documenso'` is the load-bearing pathway selector, yet NEITHER `render_mode` value now has a live backend** (the through-docraptor proposal backend was REMOVED; `direct-to-documenso` was never wired). The working direct-to-documenso flow is the separate engagement-mandate-drafts lane, which never reads `render_mode`. Where this file and `DIRECT_TO_DOCUMENSO_PAYMENT_E2E.md` disagree, the CODE wins and the discrepancy is noted.

## Orientation

A fresh AI agent reading the Documenso/payment surface will trip on the classes of hazard catalogued below. The originate surface has TWO independent switches that are easy to conflate — `render_mode` (top-level pathway) and `direct_to_documenso_lane` (sub-lane). The legacy through-docraptor proposal backend (`proposals_v1.py`, its `_provision` function, its webhook, and the `render_mode` STUB) was REMOVED in commit `b83e002` (`refactor(edge_api): remove legacy through-docraptor proposal + payment backend`, #533); the `render_mode` enum persists for backwards compat but no longer has a live consumer in edge_api. The actual working "direct to documenso" flow lives in `engagement_mandate_drafts_v1` and branches on `direct_to_documenso_lane` only. That lane is now a THREE-value text enum: `'prefill-document-from-template'` (DEFAULT, canonical embed-document lane), `'embed-template'` (NEW, parallel direct-link lane), and `'envelope-distribute'` (RETIRED — value kept so a pre-existing row never violates the CHECK, but no code path serves it). Only ONE Documenso webhook route now exists (`/api/v1/documenso/webhook`, raw capture); the legacy `/api/v1/proposals/webhook` was removed with `proposals_v1.py`. Several docstrings in the platform repo still call the prospect-link capability an "opportunity UUID" — it is not a UUID, it is an 8-char handle (core-x's own `documenso_client.py` docstrings have since been corrected). And the reference doc `DIRECT_TO_DOCUMENSO_PAYMENT_E2E.md` itself carries stale line cites and one false offender. Trust the code path, not the comments.

---

## 1. The render_mode enum is a LEGACY relic — no live backend behind either value

### 1.1 The through-docraptor proposal backend (and the STUB) were REMOVED

Earlier revisions of this doc described a `proposals_v1._provision` `render_mode == "direct-to-documenso"` STUB that returned `"not yet wired"`, and a live `'through-docraptor'` proposal-confirm path beside it. **Both are gone.** Commit `b83e002` (`refactor(edge_api): remove legacy through-docraptor proposal + payment backend`, #533) deleted `apps/edge_api/src/routers/proposals_v1.py` in its entirety — the `_provision` function, the `render_mode == "direct-to-documenso"` STUB branch, and the `'through-docraptor'` live branch all left with it.

- **Grep-confirmed:** `proposals_v1.py` does not exist (`ls apps/edge_api/src/routers/proposals_v1.py` → no such file), and `grep -rn proposals_v1 apps/edge_api/src` returns ZERO matches.
- `render_mode` is read by NO router in edge_api: `grep -rn render_mode apps/edge_api/src/routers/` matches only the `operator_settings` gateway that persists the enum value, never a path that branches on it.

### 1.2 The `render_mode` enum persists for backwards compat only

The `render_mode` column still exists on `operator_settings` and still carries the same two values, but neither has a live edge_api consumer:

- `'through-docraptor'` (DEFAULT) — the proposal/`_provision` backend that served this value was REMOVED (§1.1). No live path renders a proposal PDF and creates a Documenso envelope from `render_mode`.
- `'direct-to-documenso'` — was always a STUB on the proposal path; that path is now gone too. The working direct-to-documenso flow (the engagement-mandate-drafts lane) does NOT read `render_mode`.

- Enum domain enforced at the DB CHECK (`render_mode = ANY (ARRAY['through-docraptor', 'direct-to-documenso'])`) at `apps/edge_api/sql/operator_settings.sql:69`; column DEFAULT `'through-docraptor'` at `:42`. Mirrored as `RenderMode = Literal["through-docraptor", "direct-to-documenso"]` at `apps/edge_api/src/operator_settings/models.py:15`; `DEFAULT_RENDER_MODE = "through-docraptor"` at `:33`.

### 1.3 The WORKING direct-to-documenso flow (a SEPARATE code path)

The real working flow lives in the engagement-mandate-drafts router and branches ONLY on `direct_to_documenso_lane` — it never reads `render_mode`.

```text
platform-app MandateDraftShell.confirm()
  branches on directToDocumensoLane (NOT render_mode):
    'prefill-document-from-template' -> originatePrefilled (DEFAULT / canonical embed-document lane)
    'embed-template'                 -> originateEmbedTemplate (NEW / direct-link lane)
  -> BFF engagement-mandate-drafts-admin POST /:id/originate-prefilled  (or /originate-embed-template)
  -> edge_api POST /api/v1/engagement-mandate-drafts/{id}/originate-prefilled
       stamps external_id = opportunity 8-char handle   apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:149
  -> returns {opportunity_id, document_id}              ...engagement_mandate_drafts_v1.py:161
  -> SPA builds /p/m/{opp}/{doc}
```

- `originate_prefilled` stamps the opportunity's 8-char handle as `external_id` at `apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:149` and returns it as `opportunity_id` at `:161`.
- `render_mode` is consumed by NO live edge_api path (§1.1).

> **DO NOT CONFLATE.** The `render_mode` enum is a legacy relic with no backend behind either value, while the working direct-to-documenso flow is the engagement-mandate-drafts lane keyed on `direct_to_documenso_lane`. The phrase "direct to documenso" names both an inert enum value and the live lane.

---

## 2. One live Documenso webhook (the legacy proposals webhook was REMOVED)

Documenso posts directly to edge_api (no BFF route exists for the webhook). Earlier revisions of this doc listed TWO routes — a deprecated-but-functional `/api/v1/proposals/webhook` (projection) beside the live `/api/v1/documenso/webhook`. The legacy route lived in `proposals_v1.py`, which was deleted in commit `b83e002` (#533). Only the live raw-capture endpoint remains.

| Route | Status | Behavior | Citations |
|---|---|---|---|
| `POST /api/v1/documenso/webhook` | ACTIVE (system of record) | RAW append-only capture into `business.documenso_webhook_events`; no projection/filtering/normalization | `apps/edge_api/src/routers/documenso_webhooks_v1.py:27` (router prefix), `:39` (route), `:65` (`insert_event`) |

- **Grep-confirmed:** `grep -rn "proposals/webhook" apps/edge_api/src` returns ZERO matches against a route — the only surviving mention is a historical note in the live module's docstring (next bullet).
- The live module docstring still records the migration: Documenso is "repointed here from the legacy `/api/v1/proposals/webhook` (same shared `DOCUMENSO_WEBHOOK_SECRET`)" (`apps/edge_api/src/routers/documenso_webhooks_v1.py:5`) and "The legacy proposals webhook is untouched (it simply stops receiving deliveries)." (`:8`). That second clause is now stale — the legacy webhook is not merely untouched, it was DELETED with `proposals_v1.py`.
- Raw capture (live): `queries.insert_event` at `apps/edge_api/src/routers/documenso_webhooks_v1.py:65`.
- Sign-state is derived OFFLINE at read time from the raw rows — described in `read_sign_state`'s docstring at `apps/edge_api/src/routers/documenso_webhooks_v1.py:83` (true iff a terminal `DOCUMENT_COMPLETED` row has landed for the pair).

> **TRAP.** Do not look for `/api/v1/proposals/webhook` — it no longer exists. The live module docstring's "the legacy proposals webhook is untouched" is itself stale: the route was removed, not merely repointed.

---

## 3. Tables that DO NOT EXIST

There is no `business.mandate_payments` and no `business.mandate_payment_events` anywhere in the repo. **Independently re-grepped:** `grep -rn mandate_payment apps/edge_api/sql apps/edge_api/src` returns ZERO matches.

| Imagined table | Reality | Citation |
|---|---|---|
| `business.mandate_payments` | DOES NOT EXIST → use `business.document_payments` | `apps/edge_api/sql/document_payments.sql:16` |
| `business.mandate_payment_events` | DOES NOT EXIST → use `business.document_payment_events` | `apps/edge_api/sql/document_payments.sql:42` |

The real document-payment tables (created in `apps/edge_api/sql/document_payments.sql`):

- `business.document_payments` — PRIMARY KEY `document_id` (the numeric Documenso doc id, `apps/edge_api/sql/document_payments.sql:17`); `opportunity_id` is the 8-char pair capability (`:18`). `payment_status` is advanced ONLY by the Stripe webhook.
- `business.document_payment_events` — append-only Stripe-event audit + idempotency ledger with `UNIQUE stripe_event_id` (`apps/edge_api/sql/document_payments.sql:46`).

> **TRAP.** The naming pattern `mandate_*` was retired on the prospect-facing surface (see §6). Do not extrapolate it onto the payment tables — those are `document_*`. A query against `business.mandate_payments` will fail; the table never existed.

---

## 4. The "opportunity UUID" stale-comment cluster (BOTH repos)

The prospect-link access capability is the opportunity's **PUBLIC 8-char handle** (`business.opportunities.opportunity_id`, a generated handle) — **NOT a UUID, NOT the row UUID**. Multiple docstrings/comments in both repos incorrectly call it a "UUID". They are stale; the runtime value is the 8-char handle.

### 4.1 Ground truth for the capability value

`originate_prefilled` stamps `external_id = opportunity_ref` where `opportunity_ref = prefill['opportunity_ref']`, explicitly documented as the 8-char handle:

```python
# apps/edge_api/src/routers/engagement_mandate_drafts_v1.py
# The opportunity's PUBLIC 8-char handle (business.opportunities.opportunity_id) — NOT the row  # :137
# UUID. ...
opportunity_ref = prefill["opportunity_ref"]   # :140
...
external_id=opportunity_ref,                    # :149
...
opportunity_id=opportunity_ref,                # :161 (response)
```

The pair gate matches this value: `if (doc.external_id or "") != opportunity_id:` at `apps/edge_api/src/routers/documenso_webhooks_v1.py:135`. The live webhook router uses the CORRECT term — "the opportunity's public 8-char handle" — in `read_sign_state`'s docstring at `apps/edge_api/src/routers/documenso_webhooks_v1.py:87`.

### 4.2 Offender list

**core-x is now CLEAN.** Earlier revisions listed four core-x `documenso_client.py` offenders (the `DocumentReadResult` docstring + field comment, and two `read_document` docstring lines). Those have been corrected: the `DocumentReadResult` docstring/field now read "the opportunity's 8-char handle stamped at originate" (`apps/edge_api/src/services/documenso_client.py:557`, `:562`), and `read_document`'s docstring reads "`externalId` (the opportunity's 8-char handle stamped at originate)" / "a guessed numeric id with a wrong/missing handle never yields a signing surface" (`apps/edge_api/src/services/documenso_client.py:574`, `:576`). No `UUID` reference survives anywhere in `documenso_client.py` (grep-confirmed, 0 matches).

The remaining offenders are all in the **platform repo (`rare-structure-hq`, CROSS-REPO — not verifiable from this repo)**:

| Repo | Location | Stale text | Citation |
|---|---|---|---|
| platform | `edge.ts` `EdgeMandatePrefilledOriginated.opportunity_id` | "The opportunity UUID stamped as the envelope's externalId — the prospect-link capability." | `rare-structure-hq:apps/platform-api/src/lib/edge.ts:422` |
| platform | `edge.ts` `edgeGetSignState` docstring | "a guessed numeric document id with a wrong/missing opportunity UUID returns signed:false." | `rare-structure-hq:apps/platform-api/src/lib/edge.ts:534` |
| platform | `documenso-public.ts` `/sign/:opp/:doc/token` comment | "The opportunity UUID is the capability; ..." / "a guessed document id under a wrong/missing UUID → 404." | `rare-structure-hq:apps/platform-api/src/routes/documenso-public.ts:35-36` |
| platform | `engagement-mandate-drafts-admin.ts` `/:id/originate-prefilled` comment | "The opportunity UUID is the prospect-link capability: `/p/m/{opportunityId}/{documentId}`." | `rare-structure-hq:apps/platform-api/src/routes/engagement-mandate-drafts-admin.ts:150` |
| platform | `proposals/api.ts` `MandatePrefilledOriginated.opportunityId` | "The opportunity UUID (the envelope's externalId) — the unguessable prospect-link capability" | `rare-structure-hq:apps/platform-app/src/proposals/api.ts:93` |
| platform | `proposals/api.ts` `getMandateSignToken` docstring | "The opportunity UUID is the capability; a guessed document id under a wrong UUID → 404 (null)." | `rare-structure-hq:apps/platform-app/src/proposals/api.ts:125` |
| platform | `App.tsx` route comment for `/p/m/:opportunityId/:documentId` | "the opportunity UUID is the unguessable access capability ..." | `rare-structure-hq:apps/platform-app/src/App.tsx:97-98` |

> **NOTE on `edge.ts:534`.** This same stale-UUID line ALSO correctly says "Drives DocumentSignPage's advance" — the NEW component name. One file therefore mixes the stale UUID term with the correct (renamed) component name. Do not treat the whole comment as wrong.

> **DO-NOT-CONFLATE.** Within core-x, BOTH `documenso_client.py` and `documenso_webhooks_v1.py` now say "8-char handle" (right) — the core-x side is clean. The surviving "UUID" misnomers are all in the platform repo (cross-repo, listed above). The runtime value is the 8-char handle; trust that term.

---

## 5. The reference doc's own inaccuracies (`DIRECT_TO_DOCUMENSO_PAYMENT_E2E.md`)

The reference doc flags the stale-UUID comments as a known hazard ("The **runtime value is the 8-char handle**, not the UUID ... Trust the code path, not those comments.") — its CORE point is correct (`/Users/benjamincrane/core-x/docs/reference/DIRECT_TO_DOCUMENSO_PAYMENT_E2E.md:123-124`). But its own offender list and line cites are partly wrong:

- **False offender:** the doc cites `MandateDraftShell.tsx:17` as a stale "opportunity UUID" comment (`/Users/benjamincrane/core-x/docs/reference/DIRECT_TO_DOCUMENSO_PAYMENT_E2E.md:122`). **`MandateDraftShell.tsx` contains ZERO `UUID` references** (independently grep-confirmed, 0 matches). Line 17 is part of the file's header doc-comment about the prospect link, not a UUID comment.
- **Stale internal line cite:** the doc cites `read_document` at `services/documenso_client.py:566-575` (`/Users/benjamincrane/core-x/docs/reference/DIRECT_TO_DOCUMENSO_PAYMENT_E2E.md:121`). `read_document` is actually at `apps/edge_api/src/services/documenso_client.py:569` (its `DocumentReadResult` model at `:555`).
- **Path note:** the reference doc lives at repo-root `docs/reference/`, NOT `apps/docs/reference/` (verified at `/Users/benjamincrane/core-x/docs/reference/DIRECT_TO_DOCUMENSO_PAYMENT_E2E.md:1`). The import-relative path `apps/edge_api/../docs/reference/` in the task framing resolves to `apps/docs/reference/`, which is wrong — the file is at `<repo-root>/docs/reference/`.

> **TRAP.** Treat `DIRECT_TO_DOCUMENSO_PAYMENT_E2E.md` as a starting point, NOT ground truth. Its central claim (8-char handle, not UUID) is correct; its offender list and internal line numbers are stale. Re-verify against current code.

---

## 6. Retired naming: `Mandate*` → `Document*` (partial)

The prospect-facing components were renamed `Mandate*` → `Document*`. **`MandateSignPage` has ZERO matches across the entire platform repo** (independently grep-confirmed: 0 in `apps/` and `packages/`). All 7 `Document*` prospect components resolve to real files.

| Old (retired, 0 matches) | New (active) | Citation |
|---|---|---|
| `MandateSignPage` | `DocumentSignPage` (`/p/m/:opportunityId/:documentId`) | `rare-structure-hq:apps/platform-app/src/routes/p/DocumentSignPage.tsx:2`; imported `App.tsx:51` |
| (prospect payment/sign/frame/summary set) | `DocumentPaymentPage`, `DocumentSignedConfirmation`, `DocumentPaymentForm`, `DocumentPaymentConfirmation`, `DocumentFrame`, `DocumentSummaryScaffold` | `rare-structure-hq:apps/platform-app/src/App.tsx:50` (imports `DocumentPaymentPage`) |

**Survivors (cockpit-side, NOT retired):** two operator-facing authoring components retain the `Mandate` prefix.

- `MandateDraftShell.tsx` — the `/app/m/:ref` cockpit body for a draft (`rare-structure-hq:apps/platform-app/src/proposals/MandateDraftShell.tsx:1-2`).
- `MandateEditor.tsx` — the through-docraptor operator mandate-generator/editor at `/app/m/:ref` (`rare-structure-hq:apps/platform-app/src/proposals/MandateEditor.tsx:1-2`).

> **STALE CROSS-REPO NAME.** edge_api's `read_sign_state` docstring still says "The prospect-facing `MandateSignPage` polls this" (`apps/edge_api/src/routers/documenso_webhooks_v1.py:84`). The component is now `DocumentSignPage` — the name `MandateSignPage` no longer exists in the platform repo. The docstring is the only surviving reference to the retired name.

---

## 7. The lane enum: DEFAULT is canonical; one value is RETIRED; one is NEW

`operator_settings.direct_to_documenso_lane` is a **text enum** (NOT a boolean), now with THREE values, enforced by a DB CHECK constraint and a pydantic `Literal`. The DEFAULT is the canonical embed-document lane. (The DEFAULT was changed from `'envelope-distribute'` to `'prefill-document-from-template'`; earlier revisions of this doc that called `'envelope-distribute'` the default are stale.)

| Value | Status | Behavior | Note |
|---|---|---|---|
| `'prefill-document-from-template'` | **DEFAULT, canonical** | `/api/v2/template/use` (prefilled) → distribute(NONE) → PENDING → `POST .../{id}/originate-prefilled` → `create_document_from_template` | The embed-document lane. Mints a document NOW; required for the `/p/m` prospect link + payment |
| `'embed-template'` | **NEW**, parallel | enable a Documenso DIRECT LINK on the template → `POST .../{id}/originate-embed-template` → returns a reusable `direct_token` for the SPA's `<EmbedDirectTemplate>` | Signer SELF-identifies (name/email not locked); NO document minted until the signer completes (Documenso creates it then, source `TEMPLATE_DIRECT_LINK`) |
| `'envelope-distribute'` | **RETIRED, no handler** | — | The `/envelope/use` + `.../{id}/confirm` lane was REMOVED in code. DB value retained for backwards compat so a pre-existing row never violates the CHECK; no live path serves it |

- Canonical lane documented at `apps/edge_api/sql/operator_settings.sql:26-30` (prefill-document-from-template, DEFAULT); `'envelope-distribute'` documented as RETIRED at `:31-34` ("The /envelope/use + .../{id}/confirm lane was removed in code; the CHECK still accepts the value so a pre-existing row never violates it, but no live path serves it"); `'embed-template'` documented at `:77-79`.
- Column DEFAULT `'prefill-document-from-template'` at `apps/edge_api/sql/operator_settings.sql:43`; `ALTER ADD COLUMN ... DEFAULT 'prefill-document-from-template'` at `:50`; the three-value DB CHECK (`envelope-distribute` / `prefill-document-from-template` / `embed-template`) at `:85-89`.
- Pydantic: `DirectToDocumensoLane = Literal["envelope-distribute", "prefill-document-from-template", "embed-template"]` at `apps/edge_api/src/operator_settings/models.py:21-23`.

### 7.1 The retired lane (`envelope-distribute`) has no handler

Earlier revisions documented a `confirm_mandate_draft` handler (the `envelope-distribute` lane) that stamped `external_id = draft_id` (the draft id, not the opportunity handle) and dead-ended the prospect link via `create_document_from_template_with_custom_pdf`. **That function and the lane it served were REMOVED.** Both `confirm_mandate_draft` and `create_document_from_template_with_custom_pdf` are grep-confirmed absent from the current codebase (0 matches). The `envelope-distribute` enum value survives only as a backwards-compat token in the DB CHECK (`apps/edge_api/sql/operator_settings.sql:31-34`, `:86`); no code path serves it.

The canonical lane is `prefill-document-from-template`, handled by `originate_prefilled`, which stamps the opportunity's 8-char handle as `external_id` (`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:149`) and returns the `(opportunity_id, document_id)` pair (`:156-165`) the SPA needs to build `/p/m/{opp}/{doc}`. The prospect sign-state/sign-token surface pair-gates on `external_id == opportunity_handle` (`apps/edge_api/src/routers/documenso_webhooks_v1.py:135`), which this lane satisfies.

### 7.2 The embed-template lane (NEW, parallel to prefill)

`originate_embed_template` is a SEPARATE handler, left parallel to `originate_prefilled` (which is untouched). It enables a Documenso DIRECT LINK on the draft's template and returns the reusable token for the SPA to mount `<EmbedDirectTemplate>`. **No document is created at originate time** — the signer self-identifies in the embed and Documenso mints the document (source `TEMPLATE_DIRECT_LINK`) at completion.

- Endpoint `POST /api/v1/engagement-mandate-drafts/{draft_id}/originate-embed-template` declared at `apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:168-174`.
- Flow: `get_template_recipients` → pick `direct_recipient_id` → `create_direct_link` (`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:196-200`).
- Response `MandateEmbedTemplateOriginated` (`apps/edge_api/src/engagement_mandate_drafts/models.py:49`) carries `{direct_token, documenso_host, embed_url, external_id, opportunity_id, direct_recipient_id, recipient_email, recipient_name, status}` (`engagement_mandate_drafts_v1.py:211-221`); `embed_url` is `{host}/embed/direct/{token}` (`:214`).
- Documenso client surface (all v2): `get_template_recipients` (`apps/edge_api/src/services/documenso_client.py:504`), `create_direct_link` → `POST /api/v2/template/direct/create {templateId, directRecipientId?}` (`:514`, `:529`; falls back to `/template/direct/toggle {enabled:true}` to return an existing token, `:533`), `toggle_direct_link` → `POST /api/v2/template/direct/toggle` (`:542`), returning `DirectLinkResult` (`:469`).
- The returned `direct_token` is the `<EmbedDirectTemplate>` prop AND the public `/d/{token}` / iframe `/embed/direct/{token}` capability. The signer enters their own name + email; `typedSignatureEnabled` / `drawSignatureEnabled` / `uploadSignatureEnabled` are document/template-level meta settings.

> **TRAP.** The DB default is `'prefill-document-from-template'` (canonical), NOT `'envelope-distribute'` — a fresh operator row lands on the live lane. The `'envelope-distribute'` value still passes the CHECK but has NO handler; selecting it (e.g. via a stale tool that writes it) yields a lane with no originate path. `'embed-template'` is a third, parallel lane that mints no document until the signer completes.

---

## 8. Misleading UI copy and DB-vs-comment discrepancies

### 8.1 Settings card hint undersells a load-bearing mode

The Settings "Direct to Documenso" render-mode card hint says "Skip DocRaptor — go straight to Documenso. Prototype pathway (not yet wired)." (`rare-structure-hq:apps/platform-app/src/routes/app/Settings.tsx:93`; value `'direct-to-documenso'` / label "Direct to Documenso" at `:91-92` — **CROSS-REPO, verify in `rare-structure-hq`**). The hint reads as if `direct-to-documenso` is inert, but selecting this mode is REQUIRED to expose the sub-lane (`direct_to_documenso_lane`) and reach the working engagement-mandate-drafts flow. The reference doc states the live prd row is `render_mode = direct-to-documenso` + `direct_to_documenso_lane = prefill-document-from-template`, `stripe_mode = NULL` (`/Users/benjamincrane/core-x/docs/reference/DIRECT_TO_DOCUMENSO_PAYMENT_E2E.md:73-74`).

> **TRAP.** The "not yet wired" hint is misleading: `render_mode='direct-to-documenso'` itself has no backend (the only branch that ever read it was the removed proposal STUB, §1), yet selecting it is still load-bearing because it is the gate that surfaces the `direct_to_documenso_lane` sub-selector driving the live flow. The hint undersells it.

### 8.2 "BFF no longer touches operator_settings"

The SQL comment asserts "The platform-api BFF NO LONGER touches this table directly" (`apps/edge_api/sql/operator_settings.sql:7`; same in `apps/edge_api/src/operator_settings/__init__.py:9`). The earlier exception to this — a surviving direct Supabase read `db().from("operator_settings").select("render_mode")` in `proposals-admin.ts` — was tied to the proposal confirm path, whose **core-x backend was REMOVED** (`proposals_v1.py`, commit `b83e002`/#533). The BFF `proposals-admin.ts` route in the platform repo (`rare-structure-hq`) was almost certainly removed alongside the backend removal.

> **DISCREPANCY (CROSS-REPO — verify in `rare-structure-hq`).** Re-verify whether `proposals-admin.ts` (and its `render_mode` read) still exists in the platform repo after the core-x proposal-backend removal. If the route is gone, the blanket "BFF no longer touches operator_settings" comment is now fully accurate for the read surface; the prior surviving-read exception no longer applies. Do not assume the exception still holds.

### 8.3 Lane is NOT a boolean

Discovery inventories sometimes describe the lane switch as `direct_to_documenso_lane = true`. It is a THREE-VALUE TEXT ENUM (§7), enforced by DB CHECK (`apps/edge_api/sql/operator_settings.sql:85-89`) and pydantic `Literal` (`apps/edge_api/src/operator_settings/models.py:21`). There is no boolean form.

---

## 9. Stripe fulfillment SEAMs (intentionally not wired — NOT bugs)

`webhooks_stripe` carries a "fulfillment seam" hook on `payment_intent.succeeded`: a deliberate logging-only placeholder for a future Trigger.dev fan-out; the audit row is the record of truth. (Earlier revisions listed a second, "legacy proposal path" seam; that path was removed with the proposal backend — only the module-level note and the `_handle_document_payment` seam remain.)

| Seam | Location | Text |
|---|---|---|
| Module-level note | `apps/edge_api/src/routers/webhooks_stripe.py:15-17` | "SEAM: durable post-payment fulfillment ... left as a marked hook below, intentionally not wired yet." |
| `_handle_document_payment` | `apps/edge_api/src/routers/webhooks_stripe.py:121-122` | "SEAM: durable post-payment fulfillment ... Intentionally not wired — the document_payment_events row is the audit of record" |

> **TRAP.** These are NOT missing-functionality bugs. They are marked hooks. The `engagement_events` / `document_payment_events` audit row is the record of truth; fulfillment fan-out is a deliberate future seam.

---

## 10. Other deprecated fields, misnomers, and a transitional alias

### 10.1 `quarterly_total` is NOT a quarterly figure

| Field | Status | Reality | Citation |
|---|---|---|---|
| `Proposal.quarterly_total_cents` | DEPRECATED | "# deprecated; total derives from duration" | `apps/edge_api/src/proposals/models.py:85` |
| `ProposalPublic.quarterly_total` | back-compat alias | "# back-compat alias of total (legacy field name)" | `apps/edge_api/src/proposals/models.py:152` |
| `template_render` output | legacy alias | `quarterly_total: format_usd(total), # legacy alias` | `apps/edge_api/src/proposals/template_render.py:195` |
| `edge.ts` BFF type | legacy alias | `quarterly_total: string; // legacy alias of total (kept for back-compat)` | `rare-structure-hq:apps/platform-api/src/lib/edge.ts:212` |

> **TRAP.** `quarterly_total` is the FULL engagement total (monthly × duration), NOT a quarterly figure. The name is a legacy alias of `total`.

### 10.2 `create_document_from_template_with_custom_pdf` was REMOVED

Earlier revisions documented `create_document_from_template_with_custom_pdf` (the envelope-distribute lane's Documenso client method, whose `with_custom_pdf` name was a misnomer — it did NO PDF render) and its optional `recipients` override. **That method was removed** along with the envelope-distribute lane and its `confirm_mandate_draft` handler. Grep-confirmed absent: `grep -rn "with_custom_pdf" apps/edge_api/src` → 0 matches; `grep -rn "confirm_mandate_draft" apps/edge_api/src` → 0 matches.

The current Documenso template/document surface in `documenso_client.py` is:

- `create_document_from_template` (the embed-document / prefill lane — mints a document via `/api/v2/template/use`), at `apps/edge_api/src/services/documenso_client.py:228`.
- `create_template_from_pdf` → `POST /api/v2/envelope/create` with `type=TEMPLATE` (the render+push lane, §11), returning `TemplateCreateResult`, at `:420` / `:410`; payload `{"type": "TEMPLATE", ...}` at `:437`.
- `create_direct_link` / `toggle_direct_link` / `get_template_recipients` (the embed-template lane, §7.2), at `:514` / `:542` / `:504`.

> **DO-NOT-CHASE.** `create_document_from_template_with_custom_pdf` no longer exists. If a discovery inventory cites it, it is stale.

### 10.3 Transitional alias mount (will be removed)

`documensoPublicRoutes` is mounted under BOTH `/api/v1/documenso` (`rare-structure-hq:apps/platform-api/src/index.ts:123`) AND the legacy `/api/v1/engagement-mandate-drafts` prefix (`:124`) as a transitional alias so an in-flight SPA bundle on the old path keeps working. The gating comment ("drop that alias once the new bundle is fully live") is at `rare-structure-hq:apps/platform-api/src/index.ts:119-122`; the same note is in `rare-structure-hq:apps/platform-api/src/routes/documenso-public.ts:15-17`.

> **TRAP.** Two URL prefixes resolve to the SAME handlers today. The `/api/v1/engagement-mandate-drafts` prefix is also mounted by `engagementMandateDraftRoutes` (`rare-structure-hq:apps/platform-api/src/index.ts:126`) for the operator-authenticated draft CRUD — the prefix is shared between the public alias and the admin CRUD routes.

---

## 11. The engagement_templates render+PUSH lane (NEW subsystem)

A separate, brand-aware subsystem renders a content source to PDF (via DocRaptor) and PUSHES it to Documenso as a **TEMPLATE** (not a document). This is the supply side that creates the templates the mandate-draft lanes (§7) later consume. It is Trigger.dev-driven and does NOT touch `render_mode` or `direct_to_documenso_lane`.

### 11.1 Pipeline shape

```text
Trigger.dev task "engagement-template-push" (src/trigger/engagement_template_push.ts:53)
  -> callHqx POST /internal/engagement-templates/render-push   (TRIGGER_SHARED_SECRET)
  -> resolve content source (registryPath/registryId OR explicit brand/path/archetype/version)
  -> render.assemble_html (content/<brand>/<path>/<archetype>/<version>/global_engagement_content)
  -> DocRaptor LIVE PDF (render.render_pdf)
  -> documenso_client.create_template_from_pdf  -> POST /api/v2/envelope/create  type=TEMPLATE
  -> record terminal row in ops.engagement_template_push_runs
```

### 11.2 Components

| Component | Location | Role |
|---|---|---|
| `engagement-template-push` Trigger.dev task | `src/trigger/engagement_template_push.ts:52-53` (task id `"engagement-template-push"`) | Calls `/internal/engagement-templates/render-push` via `callHqx` (`:78-79`) |
| `POST /internal/engagement-templates/render-push` | `apps/edge_api/src/routers/internal_engagement_templates_v1.py:84` | Trigger-secret-gated (`require_trigger_secret`, `:24`); router prefix `/engagement-templates` at `:28` |
| `push.render_and_push` | `apps/edge_api/src/engagement_templates/push.py:63` | DB-free render→DocRaptor→Documenso TEMPLATE; raises typed `PushError` (`:34`) |
| brand-aware `catalog` | `apps/edge_api/src/engagement_templates/catalog.py` | Resolves `content/<brand>/<path>/<archetype>/<version>/global_engagement_content`; `_ALLOWED_BRANDS = {'active-operators', 'rare-structure'}` at `:28`; `_DEFAULT_BRAND='active-operators'` at `:21` |
| `documenso_client.create_template_from_pdf` | `apps/edge_api/src/services/documenso_client.py:420` | `POST /api/v2/envelope/create` `type=TEMPLATE` (`:437`, `:440`); returns `TemplateCreateResult` (`:410`) |
| `business.global_input_content` (REGISTRY) | `apps/edge_api/sql/global_input_content.sql` | One row per content source; gained `brand` + `source_kind` columns (`:31-32`); `source_kind ∈ {'repo-html','db-markdown'}` CHECK (`:43-45`) |
| `ops.engagement_template_push_runs` (LEDGER) | `apps/edge_api/sql/ops_engagement_template_push_runs.sql:12` | One row per render+push attempt; `status ∈ {'success','error'}` (`:22`), `brand`/`run_id`/`documenso_template_id`/`documenso_numeric_id` columns |

### 11.3 The content-source registry vs the ledger (do not conflate)

- `business.global_input_content` is a **content-source REGISTRY** — a row names WHERE to pull content from (a brand-relative `path`, a `brand`, and a `source_kind`). It is NOT a payload store. `source_kind='repo-html'` resolves to `content/<brand>/<path>/global_engagement_content`; `source_kind='db-markdown'` resolves `business.global_engagement_content WHERE slug=path` (the db-markdown branch is documented but NOT wired — `push.py:82-83` fails loudly rather than silently mis-rendering). Seeds: AO term-only + rare-structure capital-origination (`apps/edge_api/sql/global_input_content.sql:53-56`).
- `ops.engagement_template_push_runs` is an append-only **LEDGER** of push attempts (success/error), written fire-and-forget by `push.record_run`. It records the resolved selector, the Documenso template handle, and an optional R2 audit copy of the exact pushed bytes.

> **NAME TRAP.** The catalog content subdirectory is `global_engagement_content` (`apps/edge_api/src/engagement_templates/catalog.py:22`), while the registry TABLE is `business.global_input_content`. Two similar names, two different things: the table is the registry of sources; the subdir holds the repo-resident HTML asset. The brand asset tree is `apps/edge_api/content/<brand>/docraptor-to-documenso-template/<archetype>/v1/global_engagement_content` (e.g. `content/rare-structure/docraptor-to-documenso-template/capital-origination/v1`).

---

## Status: ACTIVE / CONDITIONAL / DEPRECATED / STUB

| Component | Owner | Status | Note |
|---|---|---|---|
| `proposals_v1.py` (router, `_provision`, `render_mode` STUB, `'through-docraptor'` branch, `/proposals/webhook`) | edge_api | **REMOVED** | Deleted in commit `b83e002` (#533); 0 grep matches |
| `POST /api/v1/documenso/webhook` | edge_api | ACTIVE | Raw-capture system of record (`documenso_webhooks_v1.py:39`) |
| `originate_prefilled` (prefill-document-from-template) | edge_api | ACTIVE | The DEFAULT, canonical embed-document lane; stamps the opportunity 8-char handle, returns `(opportunity_id, document_id)` (`engagement_mandate_drafts_v1.py:113`) |
| `originate_embed_template` (embed-template) | edge_api | ACTIVE / NEW | Direct-link lane parallel to prefill; creates a Documenso direct link on the template, returns reusable token for `<EmbedDirectTemplate>` (signer self-identifies; document created at completion) (`engagement_mandate_drafts_v1.py:168`) |
| `confirm_mandate_draft` / `create_document_from_template_with_custom_pdf` (envelope-distribute) | edge_api | **REMOVED** | Function + lane deleted; 0 grep matches. `'envelope-distribute'` is a RETIRED DB value with no handler |
| `business.mandate_payments` / `business.mandate_payment_events` | edge_api | **NONEXISTENT** | 0 grep matches; use `business.document_payments` / `business.document_payment_events` |
| `business.document_payments` / `business.document_payment_events` | edge_api | ACTIVE | Real payment tables (`document_payments.sql:16`, `:42`) |
| Stripe fulfillment seam | edge_api | **STUB** | Logging-only "Intentionally not wired" hook on `payment_intent.succeeded` (`webhooks_stripe.py:15-17`, `:121-122`) |
| `quarterly_total_cents` / `quarterly_total` | edge_api | **DEPRECATED** | Legacy alias of `total` (`models.py:85`, `:152`) |
| `render_mode` enum | edge_api | **LEGACY** | `through-docraptor` (DEFAULT) \| `direct-to-documenso`; NEITHER value has a live backend (proposal backend removed; direct-to-documenso never wired). Persists for backwards compat (`operator_settings.sql:42`, `:69`) |
| `direct_to_documenso_lane` enum | edge_api | ACTIVE | `prefill-document-from-template` (DEFAULT, canonical) \| `embed-template` (NEW, parallel) \| `envelope-distribute` (RETIRED, no handler); text enum, not boolean (`operator_settings.sql:43`, `:85-89`) |
| `engagement-template-push` Trigger.dev task | edge_api/trigger | ACTIVE / NEW | Render+push supply lane → `/internal/engagement-templates/render-push` (`engagement_template_push.ts:53`) |
| `POST /internal/engagement-templates/render-push` | edge_api | ACTIVE / NEW | Trigger-secret render→DocRaptor→Documenso TEMPLATE (`internal_engagement_templates_v1.py:84`) |
| `business.global_input_content` (registry) | edge_api | ACTIVE / NEW | Content-source registry; `brand` + `source_kind` columns (`global_input_content.sql:31-32`) |
| `ops.engagement_template_push_runs` (ledger) | edge_api | ACTIVE / NEW | Push-attempt ledger (`ops_engagement_template_push_runs.sql:12`) |
| `MandateSignPage` (name) | platform | **DEPRECATED** | 0 matches; renamed to `DocumentSignPage`; only edge_api docstring still references it (`documenso_webhooks_v1.py:84`) |
| `DocumentSignPage` + 6 `Document*` prospect components | platform | ACTIVE | Renamed prospect surface (`DocumentSignPage.tsx:2`, `App.tsx:50-51`) |
| `MandateDraftShell` / `MandateEditor` | platform | ACTIVE | Cockpit-side `Mandate*` survivors (`MandateDraftShell.tsx:1`, `MandateEditor.tsx:1`) |
| `documensoPublicRoutes` legacy `/api/v1/engagement-mandate-drafts` mount | platform | CONDITIONAL | Transitional alias; drop once new bundle live (`index.ts:124`) |

## Traps

- **`render_mode` is a legacy relic — no backend behind either value.** The proposal backend that read it (`proposals_v1.py`, `_provision`, the `direct-to-documenso` STUB, the `through-docraptor` branch) was REMOVED in `b83e002` (#533). The working flow is the engagement-mandate-drafts lane, which branches on `direct_to_documenso_lane` and never reads `render_mode`. §1.
- **Only one Documenso webhook exists.** `/api/v1/proposals/webhook` was REMOVED with `proposals_v1.py`; the live raw-capture endpoint is `/api/v1/documenso/webhook`. The live module's "legacy proposals webhook is untouched" docstring is itself stale (it was deleted, not repointed). §2.
- **`business.mandate_payments` / `business.mandate_payment_events` DO NOT EXIST** (0 grep matches). The real tables are `business.document_payments` / `business.document_payment_events`. §3.
- **"opportunity UUID" is a lie — but only in the platform repo now.** The value is the PUBLIC 8-char handle (`business.opportunities.opportunity_id`), not a UUID. core-x's `documenso_client.py` has been corrected to "8-char handle"; the surviving misnomers are cross-repo (`rare-structure-hq`). §4.
- **The reference doc is a starting point, not ground truth.** `DIRECT_TO_DOCUMENSO_PAYMENT_E2E.md:122` falsely names `MandateDraftShell.tsx:17` as a stale-UUID comment (that file has 0 UUID matches), and its `read_document` cite `:566-575` is stale (actual `:569`). §5.
- **`MandateSignPage` no longer exists** (0 matches repo-wide); it is `DocumentSignPage`. Only edge_api's `documenso_webhooks_v1.py:84` docstring still names the dead component. §6.
- **The DB default lane is the CANONICAL one.** `direct_to_documenso_lane` defaults to `'prefill-document-from-template'` (the live embed-document lane). `'envelope-distribute'` is RETIRED (value kept for backwards compat; no handler). `'embed-template'` is a NEW parallel direct-link lane. §7.
- **"not yet wired" Settings copy undersells a load-bearing mode.** Selecting `direct-to-documenso` has no backend of its own but is still the gate that surfaces the `direct_to_documenso_lane` sub-selector driving the live flow (cross-repo Settings copy — verify in `rare-structure-hq`). §8.1.
- **"BFF no longer touches operator_settings" — re-verify cross-repo.** The prior surviving `proposals-admin.ts` read was tied to the proposal backend, which was REMOVED in core-x; the BFF route was likely removed too. Verify in `rare-structure-hq`. §8.2.
- **The lane switch is a THREE-VALUE TEXT ENUM, not a boolean.** No `direct_to_documenso_lane = true` form exists. §8.3.
- **Stripe fulfillment seams are intentional, not bugs.** Logging-only "Intentionally not wired" hooks awaiting Trigger.dev. §9.
- **`quarterly_total` is the FULL engagement total**, not a quarterly figure — a legacy alias of `total`. §10.1.
- **`create_document_from_template_with_custom_pdf` was REMOVED** along with the envelope-distribute lane and `confirm_mandate_draft` (0 grep matches). If a discovery inventory cites it, it is stale. §10.2.
- **Two URL prefixes resolve to the same handlers.** `/api/v1/documenso` and the transitional `/api/v1/engagement-mandate-drafts` alias both mount `documensoPublicRoutes`. §10.3.
- **The render+push lane is a SEPARATE subsystem.** `engagement_templates` (Trigger.dev `engagement-template-push` → `/internal/engagement-templates/render-push` → DocRaptor → Documenso TEMPLATE) supplies templates; it does NOT touch `render_mode`/`direct_to_documenso_lane`. The registry table `business.global_input_content` is distinct from the catalog subdir `global_engagement_content`. §11.
- **`embed-template` mints no document until completion.** The NEW `originate_embed_template` lane returns a direct-link token; the signer self-identifies and Documenso creates the document (source `TEMPLATE_DIRECT_LINK`) only at completion. §7.2.
