# 09 — Deprecated Paths, Stubs, and Traps

> **STATUS BANNER.** Cross-cutting hazard list for the Documenso + payment surface across BOTH repos (core-x `apps/edge_api/` and `rare-structure-hq` platform). This file is render_mode/lane-agnostic by design: it catalogs the things that mislead — code that EXISTS but does not RUN, routes that exist but no longer receive traffic, tables that do not exist at all, stale docstrings, retired naming, and the DB-default lane that is thinner than the canonical one. The single biggest hazard documented here: **`render_mode = 'direct-to-documenso'` (a STUB) is NOT the working direct-to-documenso flow** (that is the separate engagement-mandate-drafts lane). Where this file and `DIRECT_TO_DOCUMENSO_PAYMENT_E2E.md` disagree, the CODE wins and the discrepancy is noted.

## Orientation

A fresh AI agent reading the Documenso/payment surface will trip on six classes of hazard, each catalogued below. The originate surface has TWO independent switches that are easy to conflate — `render_mode` (top-level pathway) and `direct_to_documenso_lane` (sub-lane). One value of the first, `'direct-to-documenso'`, hits an unimplemented STUB on the *proposal* confirm path; the actual working "direct to documenso" flow lives in a *different* router (`engagement_mandate_drafts_v1`) that never reads `render_mode` at all. There are two Documenso webhook routes that look interchangeable but are not: one is deprecated-but-functional (projects state), one is live (raw capture). Several docstrings in BOTH repos call the prospect-link capability an "opportunity UUID" — it is not a UUID, it is an 8-char handle. The DB default lane (`'envelope-distribute'`) is a dead end for the prospect signing/payment surface. And the reference doc `DIRECT_TO_DOCUMENSO_PAYMENT_E2E.md` itself carries stale line cites and one false offender. Trust the code path, not the comments.

---

## 1. The render_mode STUB vs. the working direct-to-documenso flow (the #1 trap)

### 1.1 The STUB

`proposals_v1._provision` contains a `render_mode == "direct-to-documenso"` branch that is intentionally unimplemented. It logs and returns a failure tuple **without creating any document**.

```python
# apps/edge_api/src/routers/proposals_v1.py
if render_mode == "direct-to-documenso":              # :99
    # Prototype branch — intentionally unimplemented.  # :100
    logger.info("proposal %s: ... pathway not yet wired", p.ref)  # :102
    return False, "direct-to-documenso pathway not yet wired"     # :103
```

- Branch declared at `apps/edge_api/src/routers/proposals_v1.py:99`; "intentionally unimplemented" comment at `:100`; log at `:102`; return of the not-wired tuple at `:103`.
- The `_provision` docstring documents the two modes: `'through-docraptor' (default / None)` renders PDF then creates the Documenso envelope — labeled "CURRENT behavior" (`apps/edge_api/src/routers/proposals_v1.py:96`); `'direct-to-documenso'` is "the no-DocRaptor pathway — NOT YET WIRED (stub below)" (`apps/edge_api/src/routers/proposals_v1.py:97`).
- The `'through-docraptor'` branch (the live path) renders the PDF via `docraptor_client.render_pdf` and creates the envelope via `documenso_client.create_signing_envelope`, beginning at `apps/edge_api/src/routers/proposals_v1.py:104`.

### 1.2 The STUB is REACHABLE in production (a live foot-gun, not dead code)

The stub is not dead code — it is reachable. The BFF proposals-admin confirm route reads `operator_settings.render_mode` directly from Supabase and forwards it to edge_api:

```text
platform-app (proposal / MandateEditor confirm)
  -> BFF POST /api/v1/proposals/:ref/confirm     rare-structure-hq:apps/platform-api/src/routes/proposals-admin.ts:120
       reads operator_settings.render_mode from Supabase  ...proposals-admin.ts:132-136
       forwards as render_mode                            ...proposals-admin.ts:146
  -> edge_api POST /api/v1/proposals/:ref/confirm
       passes body.render_mode into _provision   apps/edge_api/src/routers/proposals_v1.py:243
  -> _provision(render_mode='direct-to-documenso') -> returns "not yet wired" (provisioned:false)
```

- Confirm route declared at `rare-structure-hq:apps/platform-api/src/routes/proposals-admin.ts:120`.
- Direct Supabase read `db().from("operator_settings").select("render_mode")` at `rare-structure-hq:apps/platform-api/src/routes/proposals-admin.ts:132-134`, keyed `.eq("auth_user_id", ...)` at `:135`.
- `renderMode` resolved (falls back to `DEFAULT_RENDER_MODE` when the row is absent) at `rare-structure-hq:apps/platform-api/src/routes/proposals-admin.ts:137`; forwarded as `render_mode` at `:146`.

**Consequence:** an operator on `render_mode = 'direct-to-documenso'` who originates via the proposal / `MandateEditor` surface (rather than `MandateDraftShell`) gets a silent `"not yet wired"` provision failure rather than being routed to the working flow.

### 1.3 The WORKING direct-to-documenso flow (a SEPARATE code path)

The real working flow lives in the engagement-mandate-drafts router and branches ONLY on `directToDocumensoLane` — it never reads `render_mode`.

```text
platform-app MandateDraftShell.confirm()
  branches: if (directToDocumensoLane === 'prefill-document-from-template')   ...MandateDraftShell.tsx:93
  -> originatePrefilled
  -> BFF engagement-mandate-drafts-admin POST /:id/originate-prefilled
  -> edge_api POST /api/v1/engagement-mandate-drafts/{id}/originate-prefilled
       stamps external_id = opportunity 8-char handle   apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:150
  -> returns {opportunity_id, document_id}              ...engagement_mandate_drafts_v1.py:162
  -> SPA builds /p/m/{opp}/{doc}
```

- `MandateDraftShell` destructures `{ directToDocumensoLane }` at `rare-structure-hq:apps/platform-app/src/proposals/MandateDraftShell.tsx:72`; the lane branch is at `:93`; the else branch (`confirmMandateDraft`) at `:104`.
- **Grep-confirmed:** `MandateDraftShell.tsx` has ZERO `render_mode` / `renderMode` matches (independently re-grepped, 0 results). The working flow does not pass through the `render_mode` stub.
- `render_mode` is consumed only on the proposal confirm path (`apps/edge_api/src/routers/proposals_v1.py:243`).

> **DO NOT CONFLATE.** `render_mode == 'direct-to-documenso'` (the STUB on the proposal path) and the working direct-to-documenso flow (the engagement-mandate-drafts lane) are two different code paths. The phrase "direct to documenso" names both.

---

## 2. Two Documenso webhooks — one deprecated-but-functional, one live

Documenso posts directly to edge_api (no BFF route exists for either webhook). Both routes share `DOCUMENSO_WEBHOOK_SECRET` and use identical secret verification, but they behave differently.

| Route | Status | Behavior | Citations |
|---|---|---|---|
| `POST /api/v1/proposals/webhook` | DEPRECATED (functional, no traffic) | PROJECTS: `normalize_event` → `queries.advance_status` on the proposal row | `apps/edge_api/src/routers/proposals_v1.py:337`, `:348`, `:365` |
| `POST /api/v1/documenso/webhook` | ACTIVE (system of record) | RAW append-only capture into `business.documenso_webhook_events`; no projection/filtering/normalization | `apps/edge_api/src/routers/documenso_webhooks_v1.py:39`, `:65` |

- The live module docstring states Documenso is "repointed here from the legacy `/api/v1/proposals/webhook` (same shared `DOCUMENSO_WEBHOOK_SECRET`)" (`apps/edge_api/src/routers/documenso_webhooks_v1.py:5`) and "The legacy proposals webhook is untouched (it simply stops receiving deliveries)." (`apps/edge_api/src/routers/documenso_webhooks_v1.py:8`).
- Legacy route declared `@router.post("/webhook")` at `apps/edge_api/src/routers/proposals_v1.py:337`; docstring "Documenso → status advance. Source of truth ..." at `:341`.
- Identical secret verification: live guard at `apps/edge_api/src/routers/documenso_webhooks_v1.py:44` + `verify_webhook_secret` at `:46`; legacy guard at `apps/edge_api/src/routers/proposals_v1.py:342` + `verify_webhook_secret` at `:344`.
- Projection (legacy): `normalize_event` at `apps/edge_api/src/routers/proposals_v1.py:348`, `advance_status` at `:365`. Raw capture (live): `queries.insert_event` at `apps/edge_api/src/routers/documenso_webhooks_v1.py:65`.
- Sign-state is derived OFFLINE at read time from the raw rows — described in `read_sign_state`'s docstring at `apps/edge_api/src/routers/documenso_webhooks_v1.py:84` (true iff a terminal `DOCUMENT_COMPLETED` row has landed for the pair).

> **TRAP.** The legacy `/api/v1/proposals/webhook` still verifies the secret and advances proposal status — it is fully functional. It is "dead" only in the sense that Documenso no longer points at it. Do not assume it is broken or unreachable; assume it receives no deliveries.

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
# The opportunity's PUBLIC 8-char handle (business.opportunities.opportunity_id) — NOT the row  # :138
# UUID. ...
opportunity_ref = prefill["opportunity_ref"]   # :141
...
external_id=opportunity_ref,                    # :150
...
opportunity_id=opportunity_ref,                # :162 (response)
```

The pair gate matches this value: `if (doc.external_id or "") != opportunity_id:` at `apps/edge_api/src/routers/documenso_webhooks_v1.py:135`. The live webhook router uses the CORRECT term — "the opportunity's public 8-char handle (the access capability — 8 hex = 32 bits)" — in `read_sign_state`'s docstring at `apps/edge_api/src/routers/documenso_webhooks_v1.py:87-88`.

### 4.2 Offender list (all VERIFIED stale)

| Repo | Location | Stale text | Citation |
|---|---|---|---|
| core-x | `documenso_client` `DocumentReadResult` docstring | "`external_id` is the opportunity UUID stamped at originate" | `apps/edge_api/src/services/documenso_client.py:608` |
| core-x | `documenso_client` field comment | "the opportunity UUID stamped at originate" | `apps/edge_api/src/services/documenso_client.py:613` |
| core-x | `documenso_client.read_document` docstring | "`externalId` (the opportunity UUID stamped at originate)" | `apps/edge_api/src/services/documenso_client.py:625` |
| core-x | `documenso_client.read_document` docstring | "a guessed numeric id with a wrong/missing UUID never yields a signing surface" | `apps/edge_api/src/services/documenso_client.py:628` |
| platform | `edge.ts` `EdgeMandatePrefilledOriginated.opportunity_id` | "The opportunity UUID stamped as the envelope's externalId — the prospect-link capability." | `rare-structure-hq:apps/platform-api/src/lib/edge.ts:422` |
| platform | `edge.ts` `edgeGetSignState` docstring | "a guessed numeric document id with a wrong/missing opportunity UUID returns signed:false." | `rare-structure-hq:apps/platform-api/src/lib/edge.ts:534` |
| platform | `documenso-public.ts` `/sign/:opp/:doc/token` comment | "The opportunity UUID is the capability; ..." / "a guessed document id under a wrong/missing UUID → 404." | `rare-structure-hq:apps/platform-api/src/routes/documenso-public.ts:35-36` |
| platform | `engagement-mandate-drafts-admin.ts` `/:id/originate-prefilled` comment | "The opportunity UUID is the prospect-link capability: `/p/m/{opportunityId}/{documentId}`." | `rare-structure-hq:apps/platform-api/src/routes/engagement-mandate-drafts-admin.ts:150` |
| platform | `proposals/api.ts` `MandatePrefilledOriginated.opportunityId` | "The opportunity UUID (the envelope's externalId) — the unguessable prospect-link capability" | `rare-structure-hq:apps/platform-app/src/proposals/api.ts:93` |
| platform | `proposals/api.ts` `getMandateSignToken` docstring | "The opportunity UUID is the capability; a guessed document id under a wrong UUID → 404 (null)." | `rare-structure-hq:apps/platform-app/src/proposals/api.ts:125` |
| platform | `App.tsx` route comment for `/p/m/:opportunityId/:documentId` | "the opportunity UUID is the unguessable access capability ..." | `rare-structure-hq:apps/platform-app/src/App.tsx:97-98` |

> **NOTE on `edge.ts:534`.** This same stale-UUID line ALSO correctly says "Drives DocumentSignPage's advance" — the NEW component name. One file therefore mixes the stale UUID term with the correct (renamed) component name. Do not treat the whole comment as wrong.

> **DO-NOT-CONFLATE.** Within the same core-x repo, `documenso_client.py` says "UUID" (wrong) while `documenso_webhooks_v1.py` says "8-char handle" (right). Same value, two terms. Trust "8-char handle".

---

## 5. The reference doc's own inaccuracies (`DIRECT_TO_DOCUMENSO_PAYMENT_E2E.md`)

The reference doc flags the stale-UUID comments as a known hazard ("The **runtime value is the 8-char handle**, not the UUID ... Trust the code path, not those comments.") — its CORE point is correct (`/Users/benjamincrane/core-x/docs/reference/DIRECT_TO_DOCUMENSO_PAYMENT_E2E.md:123-124`). But its own offender list and line cites are partly wrong:

- **False offender:** the doc cites `MandateDraftShell.tsx:17` as a stale "opportunity UUID" comment (`/Users/benjamincrane/core-x/docs/reference/DIRECT_TO_DOCUMENSO_PAYMENT_E2E.md:122`). **`MandateDraftShell.tsx` contains ZERO `UUID` references** (independently grep-confirmed, 0 matches). Line 17 is part of the file's header doc-comment about the prospect link, not a UUID comment.
- **Stale internal line cite:** the doc cites `read_document` at `services/documenso_client.py:566-575` (`/Users/benjamincrane/core-x/docs/reference/DIRECT_TO_DOCUMENSO_PAYMENT_E2E.md:121`). `read_document` is actually at `apps/edge_api/src/services/documenso_client.py:620-628`.
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

## 7. DB default lane is THINNER than the canonical one

`operator_settings.direct_to_documenso_lane` is a **text enum** (NOT a boolean), with exactly two values, enforced by a DB CHECK constraint and a pydantic `Literal`. The DEFAULT is the older/thinner lane.

| Value | Status | Behavior | Note |
|---|---|---|---|
| `'envelope-distribute'` | DEFAULT, thinner | `/envelope/use` + distribute → `POST .../{id}/confirm` → `create_document_from_template_with_custom_pdf` | Dead end for the prospect link |
| `'prefill-document-from-template'` | non-default, canonical | `/api/v2/template/use` (prefilled) → distribute(NONE) → PENDING → `POST .../{id}/originate-prefilled` → `create_document_from_template` | Required for the prospect link + payment |

- Lane documented at `apps/edge_api/sql/operator_settings.sql:26` (envelope-distribute, DEFAULT) and `:29` (prefill-document-from-template); "The DEFAULT preserves the existing envelope-distribute behavior" at `:34`.
- Column DEFAULT `'envelope-distribute'` at `apps/edge_api/sql/operator_settings.sql:42`; `ALTER ADD COLUMN ... DEFAULT 'envelope-distribute'` at `:49`; DB CHECK at `:80`.
- Pydantic: `DirectToDocumensoLane = Literal["envelope-distribute", "prefill-document-from-template"]` at `apps/edge_api/src/operator_settings/models.py:16`.

### 7.1 Why the default lane dead-ends the prospect flow

`confirm_mandate_draft` (the envelope-distribute handler) stamps `external_id = draft_id` — the draft id, NOT the opportunity handle — and returns only `{envelope_id, signing_token}`, no opportunity/document pair.

```python
# apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:96
result = await documenso_client.create_document_from_template_with_custom_pdf(
    draft["documenso_template_id"], external_id=draft_id,   # :97  ← draft id, not the opp handle
    prefill_values=prefill_values,
)
```

The SPA confirms this: "The envelope-distribute lane stamps externalId=draftId (not the opportunity pair) and does not return the document id, so it cannot build the `/p/m/{opportunity}/{document}` links." (`rare-structure-hq:apps/platform-app/src/proposals/MandateDraftShell.tsx:102-103`).

Because the prospect sign-state/sign-token surface pair-gates on `external_id == opportunity_handle` (`apps/edge_api/src/routers/documenso_webhooks_v1.py:135`), under the default lane that gate is never satisfiable — the prospect signing/payment surface cannot be reached.

> **TRAP.** The DB default (`'envelope-distribute'`) is NOT the canonical end-to-end lane. The prospect link + payment flow needs the NON-default `'prefill-document-from-template'`. A fresh operator row defaults to the dead-end lane.

---

## 8. Misleading UI copy and DB-vs-comment discrepancies

### 8.1 Settings card hint undersells a load-bearing mode

The Settings "Direct to Documenso" render-mode card hint says "Skip DocRaptor — go straight to Documenso. Prototype pathway (not yet wired)." (`rare-structure-hq:apps/platform-app/src/routes/app/Settings.tsx:93`; value `'direct-to-documenso'` / label "Direct to Documenso" at `:91-92`). This describes the `render_mode` stub — but selecting this mode is REQUIRED to expose the sub-lane and reach the working engagement-mandate-drafts flow. The reference doc states the live prd row is `render_mode = direct-to-documenso` + `direct_to_documenso_lane = prefill-document-from-template`, `stripe_mode = NULL` (`/Users/benjamincrane/core-x/docs/reference/DIRECT_TO_DOCUMENSO_PAYMENT_E2E.md:73-74`).

> **TRAP.** "not yet wired" is true for the proposal-path STUB but FALSE for the mode as a whole: `direct-to-documenso` is load-bearing for the live direct-to-documenso flow. The hint undersells it.

### 8.2 "BFF no longer touches operator_settings" — TRUE except one surviving read

The SQL comment asserts "The platform-api BFF NO LONGER touches this table directly" (`apps/edge_api/sql/operator_settings.sql:7`; same in `apps/edge_api/src/operator_settings/__init__.py:9`). This is TRUE for the settings READ/WRITE surface (`/api/v1/operator-settings/{auth_user_id}` → edge_api) but FALSE for the proposal confirm path: `proposals-admin.ts` still queries Supabase directly.

- Surviving direct read: `db().from("operator_settings").select("render_mode")` at `rare-structure-hq:apps/platform-api/src/routes/proposals-admin.ts:132-134`.

> **DISCREPANCY (do not trust the blanket comment).** Exactly one BFF read of `operator_settings` survives outside the edge_api gateway — the proposals-admin confirm read. (Open question: deliberate exception, or un-migrated remnant.)

### 8.3 Lane is NOT a boolean

Discovery inventories sometimes describe the lane switch as `direct_to_documenso_lane = true`. It is a TWO-VALUE TEXT ENUM (§7), enforced by DB CHECK (`apps/edge_api/sql/operator_settings.sql:80`) and pydantic `Literal` (`apps/edge_api/src/operator_settings/models.py:16`). There is no boolean form.

---

## 9. Stripe fulfillment SEAMs (intentionally not wired — NOT bugs)

`webhooks_stripe` has two "fulfillment seam" hooks on `payment_intent.succeeded`. Both are deliberate logging-only placeholders for a future Trigger.dev fan-out; the audit row is the record of truth.

| Seam | Location | Text |
|---|---|---|
| Module-level | `apps/edge_api/src/routers/webhooks_stripe.py:15-17` | "SEAM: durable post-payment fulfillment ... left as a marked hook below, intentionally not wired yet." |
| Legacy proposal path | `apps/edge_api/src/routers/webhooks_stripe.py:107-108` | "SEAM: hand durable fulfillment to Trigger.dev here ... Intentionally not wired yet" |
| `_handle_document_payment` | `apps/edge_api/src/routers/webhooks_stripe.py:162-163` | "SEAM: durable post-payment fulfillment ... Intentionally not wired" |

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

### 10.2 `create_document_from_template_with_custom_pdf` — `with_custom_pdf` is a misnomer (NOT-A-TRAP clarification)

- The "with_custom_pdf" name is a misnomer for the envelope-distribute lane: the docstring confirms NO PDF render — it uses the template's stored document via `/envelope/use` + `/envelope/distribute` ("no DocRaptor render, no anchor field placement", `apps/edge_api/src/services/documenso_client.py:328`). Function def at `:321`.
- The `recipients` parameter is an OPTIONAL override (stamp the prospect's identity onto the template's placeholder recipient), described at `apps/edge_api/src/services/documenso_client.py:342`, and IS wired/applied when supplied: `if recipients: payload["recipients"] = recipients` at `:350`. It is simply NOT supplied by `confirm_mandate_draft` (which calls with `external_id` + `prefill_values` only, `apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:96-98`).

> **DO-NOT-OVERSTATE.** `recipients` is not a reserved/unused capability — it is a live optional override, just not used by the default confirm flow.

### 10.3 Transitional alias mount (will be removed)

`documensoPublicRoutes` is mounted under BOTH `/api/v1/documenso` (`rare-structure-hq:apps/platform-api/src/index.ts:123`) AND the legacy `/api/v1/engagement-mandate-drafts` prefix (`:124`) as a transitional alias so an in-flight SPA bundle on the old path keeps working. The gating comment ("drop that alias once the new bundle is fully live") is at `rare-structure-hq:apps/platform-api/src/index.ts:119-122`; the same note is in `rare-structure-hq:apps/platform-api/src/routes/documenso-public.ts:15-17`.

> **TRAP.** Two URL prefixes resolve to the SAME handlers today. The `/api/v1/engagement-mandate-drafts` prefix is also mounted by `engagementMandateDraftRoutes` (`rare-structure-hq:apps/platform-api/src/index.ts:126`) for the operator-authenticated draft CRUD — the prefix is shared between the public alias and the admin CRUD routes.

---

## Status: ACTIVE / CONDITIONAL / DEPRECATED / STUB

| Component | Owner | Status | Note |
|---|---|---|---|
| `_provision` `render_mode=='direct-to-documenso'` branch | edge_api | **STUB** | Returns "not yet wired"; no document created (`proposals_v1.py:99-103`) |
| `_provision` `'through-docraptor'` branch | edge_api | ACTIVE | Live PDF→envelope path (`proposals_v1.py:104`) |
| `POST /api/v1/proposals/webhook` | edge_api | **DEPRECATED** | Functional (projects) but receives no Documenso traffic (`proposals_v1.py:337`) |
| `POST /api/v1/documenso/webhook` | edge_api | ACTIVE | Raw-capture system of record (`documenso_webhooks_v1.py:39`) |
| `confirm_mandate_draft` (envelope-distribute) | edge_api | CONDITIONAL | Runs only under `direct_to_documenso_lane='envelope-distribute'` (DB default); dead-ends the prospect link (`engagement_mandate_drafts_v1.py:82`) |
| `originate_prefilled` (prefill-from-template) | edge_api | CONDITIONAL | Runs only under `direct_to_documenso_lane='prefill-document-from-template'`; the canonical lane (`engagement_mandate_drafts_v1.py:113`) |
| `business.mandate_payments` / `business.mandate_payment_events` | edge_api | **NONEXISTENT** | 0 grep matches; use `business.document_payments` / `business.document_payment_events` |
| `business.document_payments` / `business.document_payment_events` | edge_api | ACTIVE | Real payment tables (`document_payments.sql:16`, `:42`) |
| Stripe fulfillment seams (×2) | edge_api | **STUB** | Logging-only "Intentionally not wired" hooks (`webhooks_stripe.py:107`, `:162`) |
| `quarterly_total_cents` / `quarterly_total` | edge_api | **DEPRECATED** | Legacy alias of `total` (`models.py:85`, `:152`) |
| `render_mode` enum | edge_api | ACTIVE | `through-docraptor` (DEFAULT) \| `direct-to-documenso`; affects only the proposal path (`operator_settings.sql:41`) |
| `direct_to_documenso_lane` enum | edge_api | ACTIVE | `envelope-distribute` (DEFAULT) \| `prefill-document-from-template`; text enum, not boolean (`operator_settings.sql:42`) |
| `MandateSignPage` (name) | platform | **DEPRECATED** | 0 matches; renamed to `DocumentSignPage`; only edge_api docstring still references it (`documenso_webhooks_v1.py:84`) |
| `DocumentSignPage` + 6 `Document*` prospect components | platform | ACTIVE | Renamed prospect surface (`DocumentSignPage.tsx:2`, `App.tsx:50-51`) |
| `MandateDraftShell` / `MandateEditor` | platform | ACTIVE | Cockpit-side `Mandate*` survivors (`MandateDraftShell.tsx:1`, `MandateEditor.tsx:1`) |
| `proposals-admin /:ref/confirm` direct Supabase read of `operator_settings` | platform | ACTIVE | Surviving direct BFF read (`proposals-admin.ts:132`) |
| `documensoPublicRoutes` legacy `/api/v1/engagement-mandate-drafts` mount | platform | CONDITIONAL | Transitional alias; drop once new bundle live (`index.ts:124`) |
| `create_document_from_template_with_custom_pdf` | edge_api | ACTIVE | Envelope-distribute impl; "with_custom_pdf" is a misnomer (no PDF render) (`documenso_client.py:321`) |

## Traps

- **render_mode STUB ≠ working direct-to-documenso.** `render_mode='direct-to-documenso'` hits an unimplemented stub on the PROPOSAL path (`proposals_v1.py:99-103`). The working flow is the engagement-mandate-drafts lane, which never reads `render_mode` (`MandateDraftShell.tsx:93`, 0 render_mode matches). §1.
- **The deprecated webhook is functional.** `/api/v1/proposals/webhook` still verifies the secret and advances status — it just gets no traffic (Documenso repointed to `/api/v1/documenso/webhook`). Don't assume it's broken. §2.
- **`business.mandate_payments` / `business.mandate_payment_events` DO NOT EXIST** (0 grep matches). The real tables are `business.document_payments` / `business.document_payment_events`. §3.
- **"opportunity UUID" is a lie in ~11 places** across both repos. The value is the PUBLIC 8-char handle (`business.opportunities.opportunity_id`), not a UUID. Within core-x, `documenso_client.py` says "UUID" (wrong) while `documenso_webhooks_v1.py` says "8-char handle" (right). §4.
- **The reference doc is a starting point, not ground truth.** `DIRECT_TO_DOCUMENSO_PAYMENT_E2E.md:122` falsely names `MandateDraftShell.tsx:17` as a stale-UUID comment (that file has 0 UUID matches), and its `read_document` cite `:566-575` is stale (actual `:620-628`). §5.
- **`MandateSignPage` no longer exists** (0 matches repo-wide); it is `DocumentSignPage`. Only edge_api's `documenso_webhooks_v1.py:84` docstring still names the dead component. §6.
- **The DB default lane is a dead end.** `direct_to_documenso_lane` defaults to `'envelope-distribute'`, which stamps `external_id=draft_id` and cannot build the `/p/m` link. The canonical flow needs `'prefill-document-from-template'`. §7.
- **"not yet wired" Settings copy undersells a load-bearing mode.** Selecting `direct-to-documenso` is required to reach the working flow, despite the "Prototype pathway (not yet wired)" hint. §8.1.
- **"BFF no longer touches operator_settings" is not fully true.** `proposals-admin.ts:132` still reads it directly from Supabase. §8.2.
- **The lane switch is a TEXT ENUM, not a boolean.** No `direct_to_documenso_lane = true` form exists. §8.3.
- **Stripe fulfillment seams are intentional, not bugs.** Logging-only "Intentionally not wired" hooks awaiting Trigger.dev. §9.
- **`quarterly_total` is the FULL engagement total**, not a quarterly figure — a legacy alias of `total`. §10.1.
- **`create_document_from_template_with_custom_pdf` does NO PDF render** despite "with_custom_pdf"; its `recipients` param IS a live optional override (just not used by the default confirm flow). §10.2.
- **Two URL prefixes resolve to the same handlers.** `/api/v1/documenso` and the transitional `/api/v1/engagement-mandate-drafts` alias both mount `documensoPublicRoutes`. §10.3.
