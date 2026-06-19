# 08 — Frontend (platform-app SPA) and the DUMB BFF (platform-api)

> STATUS BANNER — This file documents the **platform-side** of the Documenso flows: the React SPA prospect/operator surfaces, the dumb Hono BFF that brokers to `edge_api`, the shared settings schemas, and the full SPA → BFF → edge_api path map. It spans BOTH render modes: the **legacy `through-docraptor` proposal-ref lane** (`/p/:ref*`) and the **new `direct-to-documenso` pair-keyed lane** (`/p/m/:opportunityId/:documentId*`), and within direct-to-documenso, both lanes (`envelope-distribute` and `prefill-document-from-template`). The edge_api side of every cross-repo seam is documented in the sibling files (04-DOCUMENSO-INTEGRATION, 05-PAYMENTS, 07-DATA-STORES); this file stops at the BFF→edge fetch boundary and names the edge route it targets. Every platform claim carries a `rare-structure-hq:...:NN` citation; every edge claim carries an `apps/edge_api/...:NN` citation.

---

## Orientation

You are a fresh AI agent looking at the two repos that sit IN FRONT OF `edge_api`. `rare-structure-hq` holds the operator cockpit and the public prospect surfaces; `core-x/apps/edge_api` is the single writer over Postgres / Stripe / Documenso behind them. The architecture invariant is strict: **platform-app (React SPA) → platform-api (DUMB Hono BFF) → edge_api**. The BFF carries NO business logic and NO database for these flows — it validates the operator's Supabase JWT, attaches a service token on operator surfaces (and NOTHING on public prospect surfaces), remaps a couple of path shapes, and forwards. There are two generations of prospect surface living side by side: the LEGACY proposal-ref pages (`/p/:ref`, `/p/:ref/sign`, `/p/:ref/pay`) keyed on an opaque proposal `ref`, and the NEW direct-to-documenso pages (`/p/m/:opportunityId/:documentId` and `.../pay`) keyed on the `(opportunityId, documentId)` PAIR. The operator originates a direct-to-documenso mandate from the cockpit page `/app/m/:ref`, which (only under `renderMode === 'direct-to-documenso'`) renders `MandateDraftShell` and branches on `directToDocumensoLane`. Read 00-ORIENTATION and 01-MODES-AND-LANES first if you have not; this file assumes you know what a render mode and a lane are.

---

## Repo + layer boundaries

| Layer | Repo / path | Role |
|---|---|---|
| SPA | `rare-structure-hq:apps/platform-app/` | React + react-router cockpit + public prospect surfaces. Talks ONLY to the BFF (`VITE_API_BASE_URL`). |
| Shared types | `rare-structure-hq:packages/shared/` | The `@rare-structure-hq/shared` package — render-mode/lane/stripe-mode enums consumed by BOTH SPA and BFF. |
| BFF | `rare-structure-hq:apps/platform-api/` | Dumb Hono broker. Validates JWT, attaches service token on operator surfaces, remaps paths, forwards to edge_api. NO DB for these flows. |
| Edge | `core-x:apps/edge_api/` | Single writer over Postgres/Stripe/Documenso. Out of this file's scope except as the named target of each BFF fetch. |

The SPA never calls edge_api directly. The BFF never holds business logic for these flows — `settings.ts` was specifically rewired to STOP touching `public.operator_settings` directly and now forwards to edge_api (`rare-structure-hq:apps/platform-api/src/routes/settings.ts:9`, `rare-structure-hq:apps/platform-api/src/lib/edge.ts:636`).

---

## SPA route table (`App.tsx`)

The whole route tree is declared in `App.tsx` with react-router `<Routes>/<Route>`, wrapped in `<AuthProvider>` (`rare-structure-hq:apps/platform-app/src/App.tsx:85`, `:87`). Only the `/app/*` subtree gates on a session via `<RequireAuth>` wrapping `<AppShell>` (`rare-structure-hq:apps/platform-app/src/App.tsx:116`); every `/p/*` prospect surface stays anonymous.

### Public prospect routes

| Path | Element | Generation | Capability | Citation |
|---|---|---|---|---|
| `/p/:ref` | `<SummaryPage>` | LEGACY | proposal `ref` | `rare-structure-hq:apps/platform-app/src/App.tsx:96` |
| `/p/m/:opportunityId/:documentId` | `<DocumentSignPage>` | NEW (direct-to-documenso) | `(opp, doc)` pair | `rare-structure-hq:apps/platform-app/src/App.tsx:100` |
| `/p/m/:opportunityId/:documentId/pay` | `<DocumentPaymentPage>` | NEW (direct-to-documenso) | `(opp, doc)` pair | `rare-structure-hq:apps/platform-app/src/App.tsx:103` |
| `/p/:ref/sign` | `<SignPage>` | LEGACY | proposal `ref` | `rare-structure-hq:apps/platform-app/src/App.tsx:105` |
| `/p/:ref/pay` | `<PaymentPage>` | LEGACY | proposal `ref` | `rare-structure-hq:apps/platform-app/src/App.tsx:107` |

The static `/p/m/` segment ranks above the dynamic `/p/:ref` so the two generations do not collide — asserted in the adjacent comment (`rare-structure-hq:apps/platform-app/src/App.tsx:97-99`). On the NEW route, the opportunity UUID is the unguessable access capability and the numeric document id is only a disambiguator behind it (same comment block).

### Operator (`/app/*`) routes touching the Documenso flows

| Path (under `/app`) | Element | Gate | Citation |
|---|---|---|---|
| `applications/:opportunityId` | `<ApplicationStaging>` | `<RequireOperator>` | `rare-structure-hq:apps/platform-app/src/App.tsx:157` (path), `:159` (gate), `:160` (element) |
| `m/:ref` | `<Mandate>` | `<RequireOperator>` | `rare-structure-hq:apps/platform-app/src/App.tsx:194` (path), `:196` (gate), `:197` (element) |
| `settings` | `<Settings>` | `<RequireOperator>` | `rare-structure-hq:apps/platform-app/src/App.tsx:210` (path), `:214` (element) |
| `settings/templates` | `<TemplatesTable>` | operator | `rare-structure-hq:apps/platform-app/src/App.tsx:218` (path), `:221` |
| `settings/templates/new` | `<TemplateEditor>` | operator | `rare-structure-hq:apps/platform-app/src/App.tsx:226` → `:230` |
| `settings/templates/:id` | `<TemplateEditor>` | operator | `rare-structure-hq:apps/platform-app/src/App.tsx:234` → `:238` |
| `settings/documenso-templates` | `<DocumensoTemplatesEditor>` | operator | `rare-structure-hq:apps/platform-app/src/App.tsx:242` (path), `:245` |
| `settings/engagement-templates` | `<EngagementTemplatesRender>` | operator | `rare-structure-hq:apps/platform-app/src/App.tsx:250` (path), `:253` |

`/app/m/:ref` is a NATIVE cockpit page (it gets the sidebar from `AppShell`); the operator opens it after originate, but **the client-facing link stays the public `/p/:ref`** (legacy) or the `/p/m/...` pair (direct-to-documenso) — rationale in the comment at `rare-structure-hq:apps/platform-app/src/App.tsx:190-192`.

---

## The operator origination shell (`/app/m/:ref` → `Mandate` → `MandateDraftShell`)

### `Mandate.tsx` — the render-mode branch

`Mandate` renders `<MandateDraftShell draftId={ref} housing="cockpit" />` ONLY when `renderMode === 'direct-to-documenso'` (`rare-structure-hq:apps/platform-app/src/routes/app/Mandate.tsx:36`); otherwise it falls through to the proposal shell and `<MandateEditor key={ref} shell={shell} proposalRef={ref} housing="cockpit" />` (`rare-structure-hq:apps/platform-app/src/routes/app/Mandate.tsx:46`).

> TRAP: In direct-to-documenso mode the `ref` URL segment is an `engagement_mandate_draft` id, **NOT a proposal ref**. To avoid a guaranteed-404 `GET /api/v1/proposals/:ref`, `Mandate` sets `proposalRef=undefined` in direct mode so the proposal-shell fetch is suppressed (`rare-structure-hq:apps/platform-app/src/routes/app/Mandate.tsx:28`, comment at `:24-27`).

### `MandateDraftShell.confirm()` — the lane branch

`MandateDraftShell` is the operator's confirm/originate body. `confirm()` (`rare-structure-hq:apps/platform-app/src/proposals/MandateDraftShell.tsx:87`) branches on `directToDocumensoLane`:

```
confirm():
  guard: if !draftId || status in {submitting, ready} → no-op   # blocks double-originate (line 89)
  if directToDocumensoLane === 'prefill-document-from-template':       # line 93
      res = await originatePrefilled(token, draftId)                   # line 94
      if res.documentId == null: throw                                # line 95-97
      link = { opportunityId: res.opportunityId, documentId: res.documentId }
      saveSignLink(draftId, link)                                      # line 99  → localStorage
      setSignLink(link)                                                # reveals the /p/m share links
  else:                                                                # envelope-distribute, line 101
      # envelope-distribute stamps externalId=draftId (not the opp pair) and returns no document id,
      # so it CANNOT build the /p/m/{opp}/{doc} links  (comment lines 102-103)
      await confirmMandateDraft(token, draftId)                        # line 104
      setSignLink(null)                                                # no shareable pair link
  setStatus('ready')                                                   # line 107
```

Citations: prefill test at `rare-structure-hq:apps/platform-app/src/proposals/MandateDraftShell.tsx:93`; else branch at `:101`; the comment-documented reason envelope-distribute cannot build the pair link at `:102`.

### Share-link surfacing (prefill lane only)

On a successful prefill originate, `MandateDraftShell` surfaces TWO links:

| Button label | URL | Citation |
|---|---|---|
| `Copy prospect link` | `${window.location.origin}/p/m/{opp}/{doc}` | `rare-structure-hq:apps/platform-app/src/proposals/MandateDraftShell.tsx:198`, label `:214` |
| `Copy your link` | the same URL with `?signer=originator` appended | `rare-structure-hq:apps/platform-app/src/proposals/MandateDraftShell.tsx:201`, label `:215` |

### Persistence (re-originate guard)

The originated `{opportunityId, documentId}` pair is persisted to `localStorage` keyed `mandate-signlink:${draftId}` (`rare-structure-hq:apps/platform-app/src/proposals/MandateDraftShell.tsx:41`). On mount it hydrates via `loadSignLink` in a `useMemo` (`:76`) and SEEDS `status='ready'` directly from the persisted pair — `useState<ConfirmStatus>(persisted ? 'ready' : 'idle')` (`rare-structure-hq:apps/platform-app/src/proposals/MandateDraftShell.tsx:77`). Returning to the page therefore re-surfaces both links and blocks an accidental second originate (the `status === 'ready'` arm of the re-originate guard at `:89`).

> NOTE — Two distinct lines, do not conflate: line **77** is the `useState` that seeds `'ready'` on hydration; line **89** is the re-originate GUARD inside `confirm()`. Both are real.

---

## The SPA API client (`proposals/api.ts`)

`api.ts` splits into operator (Bearer-authed) and public (no-auth) calls; the split is documented in the file header (`rare-structure-hq:apps/platform-app/src/proposals/api.ts:1-7`) and again at `:242`. `authHeaders(token)` returns `{ 'Content-Type', Authorization: 'Bearer ${token}' }` (`rare-structure-hq:apps/platform-app/src/proposals/api.ts:25`). All fetches go to `API_BASE` derived from `VITE_API_BASE_URL` (`rare-structure-hq:apps/platform-app/src/proposals/api.ts:23`).

### Operator (Bearer) client functions

| Function | Method + path | Returns | Citation |
|---|---|---|---|
| `originatePrefilled(token, id)` | `POST /api/v1/engagement-mandate-drafts/{id}/originate-prefilled` | `MandatePrefilledOriginated` | `rare-structure-hq:apps/platform-app/src/proposals/api.ts:104` (decl), `:109` (path) |
| `confirmMandateDraft(token, id)` | `POST /api/v1/engagement-mandate-drafts/{id}/confirm` | `MandateDraftConfirmed` | `rare-structure-hq:apps/platform-app/src/proposals/api.ts:78` (decl), `:83` (path) |
| `createMandateDraft(token, …)` | `POST /api/v1/engagement-mandate-drafts` body `{opportunityId, documensoTemplateId}` | `{ id }` | `rare-structure-hq:apps/platform-app/src/proposals/api.ts:57` (decl), `:61` (path), `:67` (return) |

`createMandateDraft`'s ONLY SPA consumer is `ProspectDossierBoard` (import `rare-structure-hq:apps/platform-app/src/proposals/ProspectDossierBoard.tsx:38`, call `:183`) — NOT `MandateDraftShell` (which calls `originatePrefilled` / `confirmMandateDraft`).

`MandatePrefilledOriginated` (interface at `rare-structure-hq:apps/platform-app/src/proposals/api.ts:90`) EXTENDS `MandateDraftConfirmed` and adds `documentId / opportunityId / status`. It inherits `signingToken / documensoHost` from `MandateDraftConfirmed` (`rare-structure-hq:apps/platform-app/src/proposals/api.ts:70-74`).

> TRAP — the SPA type does NOT declare `envelopeId`. `MandateDraftConfirmed` declares `envelopeId, signingToken, documensoHost` (`:71-73`); `MandatePrefilledOriginated` adds `documentId/opportunityId/status` on top. The BFF response DOES include `envelopeId` (`rare-structure-hq:apps/platform-api/src/routes/engagement-mandate-drafts-admin.ts:148`), but the SPA shape consumes `documentId`/`opportunityId` to build the `/p/m` link.

### Public (no-auth) client functions

None of these four pass a `headers` option — there is NO Authorization header (the ref / pair IS the capability).

| Function | Method + path | Naming | Citation (decl / fetch-path) |
|---|---|---|---|
| `getMandateSignToken(opp, doc, signer?)` | `GET /api/v1/documenso/sign/{opp}/{doc}/token` (optional `?signer=originator`) | `Mandate*` (kept) | `rare-structure-hq:apps/platform-app/src/proposals/api.ts:126` / `:134` |
| `getMandateSignState(opp, doc)` | `GET /api/v1/documenso/sign/{opp}/{doc}/state` | `Mandate*` (kept) | `rare-structure-hq:apps/platform-app/src/proposals/api.ts:157` / `:162` |
| `createDocumentPaymentIntent(opp, doc)` | `POST /api/v1/documenso/sign/{opp}/{doc}/payment-intent` | `Document*` (newer) | `rare-structure-hq:apps/platform-app/src/proposals/api.ts:307` / `:312` |
| `getDocumentPaymentState(opp, doc)` | `GET /api/v1/documenso/sign/{opp}/{doc}/payment` | `Document*` (newer) | `rare-structure-hq:apps/platform-app/src/proposals/api.ts:334` / `:339` |

### Legacy proposal-ref payment client functions (public)

| Function | Method + path | Citation |
|---|---|---|
| `createPaymentIntent(ref)` | `POST /api/v1/proposals/{ref}/payment-intent` (no body) | `rare-structure-hq:apps/platform-app/src/proposals/api.ts:267` (decl), `:269` (path) |
| `getPaymentState(ref)` | `GET /api/v1/proposals/{ref}/payment` | `rare-structure-hq:apps/platform-app/src/proposals/api.ts:282` (decl), `:283` (path) |
| `getProposalShell(ref)` | `GET …/proposals/{ref}` shell read (public, no headers) | `rare-structure-hq:apps/platform-app/src/proposals/api.ts:246` |

### `PaymentError` (shared 409 gate)

`PaymentError` is a status-carrying `Error` subclass (`readonly status`) at `rare-structure-hq:apps/platform-app/src/proposals/api.ts:253`. BOTH `createPaymentIntent` (throws at `:274`) and `createDocumentPaymentIntent` (throws at `:315`) raise it so the pay pages can branch on HTTP **409** (the "agreement not signed yet" gate) vs. other failures. Consumed in `PaymentPage.tsx:60` and `DocumentPaymentPage.tsx:133`.

### Naming history — Mandate\* → Document\* (do NOT rename on sight)

The pages/components were renamed to `Document*` (`DocumentSignPage`, `DocumentPaymentPage`, `DocumentSummaryScaffold`), but the **token/state read client fns kept their `Mandate*` names** (`getMandateSignToken` `:126`, `getMandateSignState` `:157`, interfaces `MandateSignToken` `:116`, `MandateSignState` `:141`, `MandatePrefilledOriginated` `:90`). The payment-side client fns are already `Document*`-named (`createDocumentPaymentIntent` `:307`, `getDocumentPaymentState` `:334`). This is a half-applied rename, not a semantic distinction — `getMandateSignToken` serves the `Document*` pages.

---

## The new prospect pages (direct-to-documenso)

### `DocumentSignPage` (`/p/m/:opportunityId/:documentId`)

Reads `(opportunityId, documentId)` from `useParams` (`rare-structure-hq:apps/platform-app/src/routes/p/DocumentSignPage.tsx:45`). It treats `?signer=originator` specially: `asOriginator = searchParams.get('signer') === 'originator'` (`:52`), which seeds `proceed=true` so the operator goes straight into the embed (skipping the summary) (`:55`), and the token fetch passes `'originator'` (`:78`). A bare `/p/m/...` stays the prospect path.

Control flow:

```
on load (useEffect, deps opp/doc/asOriginator):                # line 71
  getMandateSignToken(opp, doc, asOriginator ? 'originator' : undefined)   # line 78 (the ONLY Documenso call)
     → null  ⇒ state='notfound'  → "This mandate link is invalid or has expired."  # lines 81-82 / 195
     → token ⇒ state='ready', render embed

signed poll (useEffect, runs from load — NOT gated on proceed):   # line 101
  every SIGNED_POLL_MS (4000ms, line 42):                          # tick line 104
    s = getMandateSignState(opp, doc)                              # line 107 (offline server truth)
    if s.signed ⇒ setSigned(true); clearInterval                  # lines 108-113

embed completion event:
  onDocumentCompleted ⇒ setFinalizing(true)  ONLY                 # line 238 — display-only veil
    # NEVER advances the page; only the server `signed` poll advances it (comment 235-237)

payment pre-check (one read):
  getDocumentPaymentState(opp, doc)                               # line 132
    if paymentStatus in {succeeded, processing} ⇒ setPaid(true)   # line 134
```

Citations: `SIGNED_POLL_MS=4000` (`rare-structure-hq:apps/platform-app/src/routes/p/DocumentSignPage.tsx:42`); poll tick + `s?.signed → setSigned(true)` (`:107`); `onDocumentCompleted={() => setFinalizing(true)}` (`:238`); payment read (`:132`), `(succeeded||processing) → setPaid(true)` (`:134`).

Branch-specific body:
- `signed && asOriginator` → renders only `<BodyNote>Your signature is recorded. Thank you.</BodyNote>`, NO "Continue to payment" CTA (`rare-structure-hq:apps/platform-app/src/routes/p/DocumentSignPage.tsx:162` test, `:164` body).
- `signed && paid` → read-only "Payment received" summary instead of the CTA (`:173`, inside the branch opened at `:165`).
- `signed && paymentChecked` (prospect, not paid) → renders `DocumentSignedConfirmation` with the "Continue to payment" CTA (`:182-187`).

Embed: `<EmbedSignDocument token={doc.signingToken} host={doc.documensoHost ?? DOCUMENSO_DEFAULT_HOST} lockName={true} cssVars={DOCUMENSO_CSS_VARS} css={DOCUMENSO_EMBED_CSS} />` (`rare-structure-hq:apps/platform-app/src/routes/p/DocumentSignPage.tsx:220-230`). `DOCUMENSO_DEFAULT_HOST` is `https://app.documenso.com` (`rare-structure-hq:apps/platform-app/src/proposals/documensoTheme.ts:121`).

### `DocumentSignedConfirmation` (post-sign view)

Rendered in place inside `DocumentSignPage`'s frame. Its "Continue to payment →" CTA is a `<Link to={`/p/m/${opportunityId}/${documentId}/pay`}>` keyed on the `(opp, doc)` pair, not a proposal ref (`rare-structure-hq:apps/platform-app/src/proposals/DocumentSignedConfirmation.tsx:21` decl, `:53` link, `:56` label).

### `DocumentPaymentPage` (`/p/m/:opportunityId/:documentId/pay`)

```
mint/reuse:
  createDocumentPaymentIntent(opp, doc)                       # line 129
  on 409 PaymentError:                                        # line 133
     /already paid/i  ⇒ kind:'succeeded'                      # line 136
     else             ⇒ kind:'unsigned'

poll (setInterval, 5000ms):                                   # lines 68-97
  getDocumentPaymentState:
     'succeeded'         ⇒ paid                               # lines 71-79
     'failed'|'canceled' ⇒ unavailable                       # lines 80-83
     'processing'        ⇒ keep confirmation up               # lines 84-96
```

Citations: `createDocumentPaymentIntent` (`rare-structure-hq:apps/platform-app/src/routes/p/DocumentPaymentPage.tsx:129`); 409 branch (`:133`); poll delay `}, 5000)` (`:97`).

### `DocumentPaymentForm` + `StagedAchForm` — the DUAL-RAIL document payment

`DocumentPaymentForm` passes `enableCard` to `StagedAchForm` so the minted intent's Stripe `PaymentElement` renders **Card | US bank account** tabs (`rare-structure-hq:apps/platform-app/src/proposals/DocumentPaymentForm.tsx:54`). It also sets `requireConfirm` (`:53`), `submitLabel='Authorize payment'` (`:50`), and `returnUrl = ${origin}/p/m/{opp}/{doc}/pay?status=submitted` (`:49`). The form element is `<StagedAchForm>` at `:47`.

`StagedAchForm` is SHARED by both pay surfaces (type `SettledHint` at `rare-structure-hq:apps/platform-app/src/proposals/StagedAchForm.tsx:55`). In the dual-rail document intent, `confirmPayment` maps the result:

| Stripe `paymentIntent.status` | Emitted `SettledHint` | Citation |
|---|---|---|
| `'succeeded'` | `{ status: 'succeeded', rail: 'card' }` | `rare-structure-hq:apps/platform-app/src/proposals/StagedAchForm.tsx:494`, `:499` |
| otherwise | `{ status: 'processing', rail: 'us_bank_account' }` | `rare-structure-hq:apps/platform-app/src/proposals/StagedAchForm.tsx:500` |

ACH (`us_bank_account`) returns `'processing'` and settles in 1–3 business days (copy at `rare-structure-hq:apps/platform-app/src/proposals/StagedAchForm.tsx:489-490`).

### `DocumentPaymentConfirmation`

Branches on the polled status: `settled = status === 'succeeded'` (`rare-structure-hq:apps/platform-app/src/proposals/DocumentPaymentConfirmation.tsx:35`); headline `{settled ? 'Payment received' : 'Payment initiated'}` (`:53`). `railLabel`: `'card' → 'Card'`, `'us_bank_account' → 'Bank transfer (ACH)'` (`:37`).

---

## The legacy prospect pages (proposal-ref)

### `SummaryPage` (`/p/:ref`)

Loads the proposal shell via `useProposalShell(ref, getProposalShell)` (`rare-structure-hq:apps/platform-app/src/routes/p/SummaryPage.tsx:23`). `signed = initialSigned || shell.status === 'signed' || shell.status === 'paid'` (`:43`), where `justSigned` comes from `location.state` (`:22`). `ExecutionPanel` is ready when `!!shell.signingToken` (`:123`) with CTA `<Link to={`/p/${proposalRef}/sign`}>` (`:142`); the `ExecutedPanel` "Continue to payment" CTA is `<Link to={`/p/${proposalRef}/pay`}>` (`:233`).

### `SignPage` (`/p/:ref/sign`)

Renders `<EmbedSignDocument token={shell.signingToken} … />` (`rare-structure-hq:apps/platform-app/src/routes/p/SignPage.tsx:36-37`) and on `onDocumentCompleted` navigates back to `/p/${ref}` with `state: { justSigned: true }` (`:43`).

> DO-NOT-CONFLATE — advance semantics differ by generation. The legacy `SignPage` advances on the **browser embed event** (`onDocumentCompleted`). The new `DocumentSignPage` deliberately does NOT trust that event; it advances only on the **server `signed` poll** (`rare-structure-hq:apps/platform-app/src/routes/p/DocumentSignPage.tsx:235-238`).

### `PaymentPage` (`/p/:ref/pay`)

Mints via `createPaymentIntent(ref)` (`rare-structure-hq:apps/platform-app/src/routes/p/PaymentPage.tsx:56`), polls `getPaymentState` every 5000ms for `paymentStatus === 'succeeded'` (`:72-79`), and on a 409 `PaymentError` shows the `'unsigned'` note (`:60`). It uses `<StripePaymentSection>` with NO `enableCard` prop (`:129`, props at `:130-133`).

> DO-NOT-CONFLATE — payment rails differ by generation. Legacy `PaymentPage` uses `StripePaymentSection` (ACH-only — inferred from the ABSENCE of `enableCard`; see Traps). New `DocumentPaymentForm` uses `StagedAchForm` WITH `enableCard` → dual-rail (Card + ACH).

---

## Shared engagement-proposal body + chrome

### `DocumentSummaryScaffold` (shared body)

The same engagement-proposal body is rendered by BOTH the operator cockpit (`MandateDraftShell`, Execution box = the operator confirm action, fields editable inline when `canEdit`) AND the prospect entry (`DocumentSignPage`, Execution box = "Proceed to Proposal" CTA, fields read-only). Component at `rare-structure-hq:apps/platform-app/src/proposals/DocumentSummaryScaffold.tsx:48`. Sections: "Prepared for" (`:71-73`), "Strategic Origination Mandate" narrative (`:93`), "Mandate Parameters" (`:103`), "Commercial Terms" (`:132`), "Execution" (`:162`). Shared usage: `MandateDraftShell.tsx:123` (editable) and `DocumentSignPage.tsx:170/202` (read-only).

`MandateSummaryValues` (interface at `rare-structure-hq:apps/platform-app/src/proposals/DocumentSummaryScaffold.tsx:22`) has exactly 10 fields: `preparedFor, targetMarket, keyInflectionPoints, coreCapabilities, regionalActivity, dataInfrastructureFee, accessAllocationPayment, termDuration, billingCadence, total`.

> TRAP — the inline editing in `MandateDraftShell` is an **in-session prototype**: it is NOT persisted and does NOT touch the live document (`rare-structure-hq:apps/platform-app/src/proposals/MandateDraftShell.tsx:8-9`, `:81-82`; `DocumentSummaryScaffold.tsx:13-16`). The prospect side passes NO `values`, so the default `EMPTY_MANDATE_SUMMARY_VALUES` (`:35-46`) renders "—" on every field.

### `DocumentFrame` (shared chrome)

Shared letterhead chrome (utility bar + framed card `{header, body, footer}`) used by every mandate surface (`rare-structure-hq:apps/platform-app/src/proposals/DocumentFrame.tsx:18`). `housing='cockpit'` adopts the AppShell sidebar header band (`min-h-16`) while `housing='standalone'` (default, public `/p/*`) keeps its own `py-4` band — ternary at `:69`. `StatusPill` maps `'paid' → 'Executed'`, `'signed' → 'Signed'`, else `'Awaiting signature'` (`:153`).

---

## The DUMB BFF (platform-api)

### `serviceHeaders()` — the operator-surface gate

`serviceHeaders(json=true)` returns `{ Authorization: 'Bearer ${EDGE_API_SERVICE_TOKEN}' }` (+ `Content-Type` when json) at `rare-structure-hq:apps/platform-api/src/lib/edge.ts:34`. `EDGE_API_URL` and `EDGE_API_SERVICE_TOKEN` are read from `process.env` (`:15`, `:16`). **Presence of `serviceHeaders()` = operator surface; absence = public prospect surface.**

### Public prospect router (`documenso-public.ts`)

Defines the PUBLIC prospect routes — none carry `requireUser` (the file imports only edge helpers + Hono/`HTTPException`, never `requireUser`):

| BFF route | Handler | edge helper | Citation |
|---|---|---|---|
| `GET /sign/:opportunityId/:documentId/token` | sign-token read (supports `?signer=originator`) | `edgeGetSignToken` | `rare-structure-hq:apps/platform-api/src/routes/documenso-public.ts:37` |
| `GET /sign/:opportunityId/:documentId/state` | sign-state poll | `edgeGetSignState` | `rare-structure-hq:apps/platform-api/src/routes/documenso-public.ts:64` |
| `POST /sign/:opportunityId/:documentId/payment-intent` | document payment mint (propagates 409 verbatim) | `edgeCreateDocumentPaymentIntent` | `rare-structure-hq:apps/platform-api/src/routes/documenso-public.ts:90` |
| `GET /sign/:opportunityId/:documentId/payment` | document payment-state poll | `edgeGetDocumentPaymentState` | `rare-structure-hq:apps/platform-api/src/routes/documenso-public.ts:118` |

### The BFF → edge REMAP

The SPA's `/sign/{opp}/{doc}/{verb}` shape is REMAPPED to edge_api's `/{verb}/{opp}/{doc}` shape — the verb moves from a trailing segment to a verb-prefix:

| SPA / BFF path (`…/sign/{opp}/{doc}/X`) | edge helper | edge_api path | Citation (edge fetch path) |
|---|---|---|---|
| `…/token` | `edgeGetSignToken` | `GET /api/v1/documenso/sign-token/{opp}/{doc}` | `rare-structure-hq:apps/platform-api/src/lib/edge.ts:568` |
| `…/state` | `edgeGetSignState` | `GET /api/v1/documenso/sign-state/{opp}/{doc}` | `rare-structure-hq:apps/platform-api/src/lib/edge.ts:541` |
| `…/payment-intent` | `edgeCreateDocumentPaymentIntent` | `POST /api/v1/documenso/payment-intent/{opp}/{doc}` | `rare-structure-hq:apps/platform-api/src/lib/edge.ts:600` |
| `…/payment` | `edgeGetDocumentPaymentState` | `GET /api/v1/documenso/payment/{opp}/{doc}` | `rare-structure-hq:apps/platform-api/src/lib/edge.ts:627` |

All four prospect edge helpers call edge_api with a BARE `fetch` and NO `serviceHeaders()` — confirming PUBLIC pass-through: `edgeGetSignState` (`:540`), `edgeGetSignToken` (`:567`), `edgeCreateDocumentPaymentIntent` (`:599`, passes only `{ method: 'POST' }`), `edgeGetDocumentPaymentState` (`:626`). `edgeGetSignToken`'s `signer` param is typed `'client' | 'originator'`; `?signer=originator` selects the originator's token, default = client/prospect (`rare-structure-hq:apps/platform-api/src/lib/edge.ts:563`, `:566`).

### Operator router (`engagement-mandate-drafts-admin.ts`)

Mounted at `/api/v1/engagement-mandate-drafts` AFTER the public alias (`rare-structure-hq:apps/platform-api/src/index.ts:126`). Operator routes use `requireUser`; only `GET /document/:envelopeId` is public.

| BFF route | Auth | edge helper | Citation |
|---|---|---|---|
| `POST /` (stamp draft) | `requireUser` | — | `rare-structure-hq:apps/platform-api/src/routes/engagement-mandate-drafts-admin.ts:31` |
| `GET /options` | `requireUser` | engagement-option list | `rare-structure-hq:apps/platform-api/src/routes/engagement-mandate-drafts-admin.ts:56` |
| `GET /by-opportunity/:id` | `requireUser` | staging-draft read | `rare-structure-hq:apps/platform-api/src/routes/engagement-mandate-drafts-admin.ts:75` |
| `PUT /by-opportunity/:id` | `requireUser` | staging-draft upsert | `rare-structure-hq:apps/platform-api/src/routes/engagement-mandate-drafts-admin.ts:98` |
| `POST /:id/confirm` | `requireUser` | `edgeConfirmMandateDraft` (`edge.ts:409`) | `rare-structure-hq:apps/platform-api/src/routes/engagement-mandate-drafts-admin.ts:121` |
| `POST /:id/originate-prefilled` | `requireUser` | `edgeOriginatePrefilled` (`edge.ts:438`) | `rare-structure-hq:apps/platform-api/src/routes/engagement-mandate-drafts-admin.ts:142` |
| `GET /document/:envelopeId` | **PUBLIC (no requireUser)** | `edgeGetMandateDraftDocument` (`edge.ts:506`) | `rare-structure-hq:apps/platform-api/src/routes/engagement-mandate-drafts-admin.ts:165` |

The `/:id/originate-prefilled` handler calls `edgeOriginatePrefilled` (`:145`) and maps the edge snake_case response to camelCase `{ envelopeId=res.envelope_id (:148), documentId (:149), opportunityId (:151), signingToken (:152), documensoHost (:153), status (:154) }`. `opportunityId` is documented as the prospect-link capability for `/p/m/{opportunityId}/{documentId}` (comment at `:150`).

`GET /options` resolves the operator org domain from the validated JWT email — `domain = c.get('user').email.split('@')[1]?.toLowerCase()` (`rare-structure-hq:apps/platform-api/src/routes/engagement-mandate-drafts-admin.ts:57`) — and returns `EngagementOption` rows `{ id, label, archetypeKey, archetypeName, performanceFeeBasis, textFields }` (`:60-67`), returning an empty array `{ data: [] }` on any failure (`:69-70`).

### The TRANSITIONAL double-mount

`documenso-public.ts` is mounted at BOTH prefixes so an in-flight SPA bundle on the old prefix keeps working across independent platform-app/platform-api deploys:

```
app.route('/api/v1/documenso', documensoPublicRoutes)                  # primary,   index.ts:123
app.route('/api/v1/engagement-mandate-drafts', documensoPublicRoutes)  # alias,     index.ts:124
app.route('/api/v1/engagement-mandate-drafts', engagementMandateDraftRoutes)  # operator CRUD, AFTER, index.ts:126
```

Citations: `rare-structure-hq:apps/platform-api/src/index.ts:123`, `:124`, `:126`. The "drop the alias once new bundle live" note is at `index.ts:119-122`.

### Operator settings BFF (`settings.ts`) — DUMB pass-through

`GET /` and `PUT /` both use `requireUser`, assert the validated JWT-derived `auth_user_id` (`user.user_id`) on the edge path, and forward via `edgeGetOperatorSettings` / `edgePutOperatorSettings`. The BFF NO LONGER touches `public.operator_settings` directly (the only `operator_settings` token in the file is the doc comment at `rare-structure-hq:apps/platform-api/src/routes/settings.ts:9`).

- `GET /` → `edgeGetOperatorSettings(user.user_id)` (`rare-structure-hq:apps/platform-api/src/routes/settings.ts:63`, `:66`).
- `PUT /` → validates each supplied field against `RENDER_MODES` / `DIRECT_TO_DOCUMENSO_LANES` / `STRIPE_MODES` (`:90-112`) and forwards ONLY supplied fields into `edgeBody` (merge semantics — an omitted field preserves its stored value, note at `:82-83`), then `edgePutOperatorSettings(user.user_id, edgeBody)` (`:74`, `:115`).

The settings/edge boundary is snake_case: `edgeGetOperatorSettings` / `edgePutOperatorSettings` hit `/api/v1/operator-settings/${authUserId}` with `render_mode` / `direct_to_documenso_lane` / `stripe_mode` (`rare-structure-hq:apps/platform-api/src/lib/edge.ts:646` get, `:657` put; `EdgeOperatorSettings` shape at `:638-643`). `toOperatorSettings` maps snake_case → camelCase and defaults an unset (`null`) `stripe_mode` to `'live'` (`rare-structure-hq:apps/platform-api/src/routes/settings.ts:56-59`).

---

## Operator settings: SPA hook + shared enums

### `useOriginationMode` hook

Reads `GET /api/v1/settings` and writes `PUT /api/v1/settings` (BFF) with the Supabase Bearer (`rare-structure-hq:apps/platform-app/src/settings/originationMode.ts:27` get, `:42` put). It manages three INDEPENDENT fields — `renderMode`, `directToDocumensoLane`, `stripeMode` — staged locally via `select` / `selectLane` / `selectStripeMode` (`:125` / `:131` / `:137`) and committed in ONE `PUT` (`:154-158`). It SKIPS the call under the DEV mock session (`token === 'dev'`) in both the load effect (`:94`) and `save()` (`:150`).

### Shared enums (`packages/shared/src/schemas/settings.ts`)

| Enum | String values | Default | Citation |
|---|---|---|---|
| `RenderMode` | `'through-docraptor'`, `'direct-to-documenso'` | `'through-docraptor'` | `rare-structure-hq:packages/shared/src/schemas/settings.ts:10` (type), `:13` (array), `:15` (default) |
| `DirectToDocumensoLane` | `'envelope-distribute'`, `'prefill-document-from-template'` | `'envelope-distribute'` | `rare-structure-hq:packages/shared/src/schemas/settings.ts:27` (type), `:30-33` (array), `:35` (default) |
| `StripeMode` | `'test'`, `'live'` | `'live'` | `rare-structure-hq:packages/shared/src/schemas/settings.ts:42` (type), `:45` (array), `:47` (default) |

- `RenderMode` is resolved SERVER-SIDE by the BFF from `public.operator_settings` (via edge_api) and never trusted from the client at confirm time (schema doc comment `:4-6`).
- `DirectToDocumensoLane` only applies when `renderMode === 'direct-to-documenso'`; it is ignored under `through-docraptor` (schema doc `:17-26`). Endpoint mapping: `'envelope-distribute'` → `.../{id}/confirm`; `'prefill-document-from-template'` → `.../{id}/originate-prefilled` (schema doc `:21-25`).
- `StripeMode` AUGMENTS the `STRIPE_MODE` env so the operator can flip test↔live for document payments from the cockpit without a Doppler change + redeploy; edge_api reads it as the global single-operator selection at document-payment mint (schema doc `:37-40`). **The edge_api-side mint behavior is documented only via this shared-schema comment — out of this repo's citation scope; see 05-PAYMENTS for the edge side.**

### Settings page (`OriginationModeCard`)

The Settings page surfaces the toggles via `<OriginationModeCard>` (`rare-structure-hq:apps/platform-app/src/routes/app/Settings.tsx:134`): the "Originate pathway" cards (Through DocRaptor / Direct to Documenso), a "Direct-to-Documenso lane" sub-selector shown only when `selected === 'direct-to-documenso'` (`showLaneSelector` at `:151`, block `:201-249`), and a "Payments — Stripe mode" (Live/Test) toggle (`:251-297`), all committed by ONE Save button (`:314-326`).

> TRAP — STALE COPY: the "Direct to Documenso" card hint still reads "Prototype pathway (not yet wired)" (`rare-structure-hq:apps/platform-app/src/routes/app/Settings.tsx:93`). This is cosmetic copy and is FALSE — the engagement-mandate-drafts originate lanes ARE wired end-to-end. Do not infer behavior from this hint.

---

## Operator staging page (`ApplicationStaging`)

`ApplicationStaging` loads engagement options, the resumable staging draft, and opportunities in parallel via `Promise.all` (`rare-structure-hq:apps/platform-app/src/routes/app/ApplicationStaging.tsx:59`): `listEngagementOptions` (`:60`), `getStagingDraft` (`:61`), `listOpportunities` (`:62`). The `@/staging/api` module (`DRAFTS = '/api/v1/engagement-mandate-drafts'` at `rare-structure-hq:apps/platform-app/src/staging/api.ts:37`) targets: `listEngagementOptions → GET .../options` (`:41`); `getStagingDraft → GET .../by-opportunity/{id}` (`:52`); `saveStagingDraft → PUT .../by-opportunity/{id}` (`:66`).

---

## Cross-repo path map (SPA → BFF → edge_api)

```
PROSPECT SIGN TOKEN
  DocumentSignPage.getMandateSignToken (api.ts:126/134, PUBLIC, +?signer=originator)
  → BFF GET /api/v1/documenso/sign/:opp/:doc/token (documenso-public.ts:37)
  → edgeGetSignToken (edge.ts:568, NO serviceHeaders)
  → edge_api GET /api/v1/documenso/sign-token/{opp}/{doc}   [one live Documenso read + pair gate]

PROSPECT SIGN STATE
  DocumentSignPage.getMandateSignState (api.ts:157/162, PUBLIC)
  → BFF GET /api/v1/documenso/sign/:opp/:doc/state (documenso-public.ts:64)
  → edgeGetSignState (edge.ts:541, NO serviceHeaders)
  → edge_api GET /api/v1/documenso/sign-state/{opp}/{doc}   [OFFLINE, from webhook capture; DOCUMENT_COMPLETED]

PROSPECT PAYMENT MINT
  DocumentPaymentPage.createDocumentPaymentIntent (api.ts:307/312, PUBLIC)
  → BFF POST /api/v1/documenso/sign/:opp/:doc/payment-intent (documenso-public.ts:90, 409 verbatim)
  → edgeCreateDocumentPaymentIntent (edge.ts:600, NO serviceHeaders)
  → edge_api POST /api/v1/documenso/payment-intent/{opp}/{doc}   [409 until signed; amount from fee_amount; dual-rail]

PROSPECT PAYMENT STATE
  DocumentPaymentPage.getDocumentPaymentState (api.ts:334/339, PUBLIC)
  → BFF GET /api/v1/documenso/sign/:opp/:doc/payment (documenso-public.ts:118)
  → edgeGetDocumentPaymentState (edge.ts:627, NO serviceHeaders)
  → edge_api GET /api/v1/documenso/payment/{opp}/{doc}   [succeeded only via Stripe webhook]

OPERATOR ORIGINATE — prefill lane
  MandateDraftShell.originatePrefilled (api.ts:104, Bearer)
  → BFF POST /api/v1/engagement-mandate-drafts/:id/originate-prefilled (admin.ts:142, requireUser)
  → edgeOriginatePrefilled (edge.ts:438, serviceHeaders)
  → edge_api originate-prefilled → returns (opportunityId, documentId) → SPA builds /p/m/{opp}/{doc}

OPERATOR ORIGINATE — envelope-distribute lane
  MandateDraftShell.confirmMandateDraft (api.ts:78, Bearer)
  → BFF POST /api/v1/engagement-mandate-drafts/:id/confirm (admin.ts:121, requireUser)
  → edgeConfirmMandateDraft (edge.ts:409, serviceHeaders)
  → edge_api confirm   [no pair link]

OPERATOR SETTINGS
  useOriginationMode getSettings/putSettings (originationMode.ts:27/42, Bearer)
  → BFF GET/PUT /api/v1/settings (settings.ts:63/74, requireUser, asserts user.user_id as auth_user_id)
  → edgeGetOperatorSettings/edgePutOperatorSettings (edge.ts:646/657, serviceHeaders)
  → edge_api GET/PUT /api/v1/operator-settings/{auth_user_id}   [owns public.operator_settings]

OPERATOR STAGING
  ApplicationStaging via @/staging/api (listEngagementOptions/getStagingDraft/saveStagingDraft, Bearer)
  → BFF /api/v1/engagement-mandate-drafts/options + /by-opportunity/:opportunityId (requireUser)
  → edge helpers (serviceHeaders) → edge_api staging-draft endpoints

LEGACY PROPOSAL PAYMENT
  PaymentPage.createPaymentIntent/getPaymentState (api.ts:267/282, PUBLIC, ref capability)
  → BFF /api/v1/proposals/:ref/payment-intent + /payment
  → edge_api proposal-ref payment endpoints   [ACH-only via StripePaymentSection, no enableCard]
```

---

## Status: ACTIVE / CONDITIONAL / DEPRECATED / STUB

| Component | Status | Notes |
|---|---|---|
| `/p/m/:opportunityId/:documentId` → `DocumentSignPage` | ACTIVE | NEW direct-to-documenso prospect signing; server-poll advance. `App.tsx:100`. |
| `/p/m/:opportunityId/:documentId/pay` → `DocumentPaymentPage` | ACTIVE | NEW dual-rail (Card+ACH) prospect payment. `App.tsx:103`. |
| `/p/:ref`, `/p/:ref/sign`, `/p/:ref/pay` (Summary/Sign/Payment) | ACTIVE (legacy generation) | Still served; `through-docraptor` proposal-ref lane. Embed-event advance, ACH-only pay. `App.tsx:96/105/107`. |
| `/app/m/:ref` → `Mandate` → `MandateDraftShell` | CONDITIONAL | Shell only renders when `renderMode === 'direct-to-documenso'`; else `MandateEditor`. `Mandate.tsx:36`. |
| `MandateDraftShell` prefill branch (`originatePrefilled`) | CONDITIONAL | Only when `directToDocumensoLane === 'prefill-document-from-template'`. Builds /p/m links. `MandateDraftShell.tsx:93`. |
| `MandateDraftShell` envelope-distribute branch (`confirmMandateDraft`) | CONDITIONAL | The `else` lane; no pair link. `MandateDraftShell.tsx:101`. |
| `DocumentSummaryScaffold` inline editing (operator) | CONDITIONAL / in-session prototype | NOT persisted, does not touch the live document. `MandateDraftShell.tsx:8-9/81-82`. |
| BFF public router `documenso-public.ts` (token/state/payment-intent/payment) | ACTIVE | PUBLIC pass-throughs; no requireUser; no serviceHeaders. `documenso-public.ts:37/64/90/118`. |
| BFF operator router `engagement-mandate-drafts-admin.ts` (confirm/originate/options/by-opportunity/POST `/`) | ACTIVE | `requireUser` + `serviceHeaders`. `admin.ts:31/56/75/98/121/142`. |
| BFF settings router `settings.ts` | ACTIVE | DUMB pass-through; no DB. `settings.ts:63/74`. |
| Transitional alias mount `/api/v1/engagement-mandate-drafts` → `documensoPublicRoutes` | ACTIVE (transitional) | Backwards-compat for in-flight SPA bundles; slated for removal. `index.ts:124`. |
| `getMandateSignToken` / `getMandateSignState` (Mandate\*-named client fns) | ACTIVE | Half-applied rename; serve the `Document*` pages. `api.ts:126/157`. |
| `createMandateDraft` | ACTIVE | Only consumer `ProspectDossierBoard.tsx:183`. `api.ts:57`. |
| `OriginationModeCard` / `useOriginationMode` (renderMode/lane/stripeMode toggles) | ACTIVE | One PUT commits all three. `Settings.tsx:134`, `originationMode.ts`. |
| BFF `GET /api/v1/engagement-mandate-drafts/document/:envelopeId` (+ `edgeGetMandateDraftDocument`) | STUB (no live SPA consumer) | PUBLIC envelope-id read, distinct from the pair reads. grep across `apps/platform-app/src` found NO consumer. `admin.ts:165`, `edge.ts:506`. |
| Settings hint "Prototype pathway (not yet wired)" | DEPRECATED (stale copy) | Cosmetic, FALSE — the lanes are wired. `Settings.tsx:93`. |

---

## Traps

- **`/app/m/:ref` ref is an `engagement_mandate_draft` id in direct mode, NOT a proposal ref.** `Mandate` deliberately suppresses the proposal-shell fetch (`proposalRef=undefined`) to avoid a guaranteed 404. `rare-structure-hq:apps/platform-app/src/routes/app/Mandate.tsx:28`.
- **Half-applied rename Mandate\* → Document\*.** The token/state read CLIENT FUNCTIONS kept their `Mandate*` names (`getMandateSignToken`, `getMandateSignState`, `MandateSignToken`, `MandateSignState`, `MandatePrefilledOriginated`); the PAGES are `Document*`. Do not assume a `Mandate*` fn belongs to a different flow — `getMandateSignToken` serves `DocumentSignPage`. `api.ts:126`.
- **`MandatePrefilledOriginated` (SPA type) does NOT declare `envelopeId`.** It carries `documentId`/`opportunityId`/`status` + inherited `signingToken`/`documensoHost`. The BFF response DOES include `envelopeId` (`admin.ts:148`), but the SPA builds the link from the pair. `api.ts:90`.
- **Advance semantics differ by generation.** Legacy `SignPage` advances on the browser `onDocumentCompleted` event (`SignPage.tsx:43`); new `DocumentSignPage` advances ONLY on the server `signed` poll and uses `onDocumentCompleted` for a display-only "Finalizing…" veil (`DocumentSignPage.tsx:238`). Do not assume the embed event advances the new page.
- **Payment rails differ by generation.** Legacy `PaymentPage` → `StripePaymentSection` (no `enableCard`); new `DocumentPaymentForm` → `StagedAchForm` WITH `enableCard` (dual-rail Card + ACH). `PaymentPage.tsx:129` vs `DocumentPaymentForm.tsx:54`.
- **Inline summary editing is an in-session prototype.** `MandateSummaryValues` edits in `MandateDraftShell` are NOT persisted and never touch the live document; the prospect side passes no `values` so every field renders "—". `MandateDraftShell.tsx:8-9/81-82`.
- **Stale Settings hint.** "Prototype pathway (not yet wired)" under "Direct to Documenso" is FALSE — the lanes are wired end-to-end. `Settings.tsx:93`.
- **`signer=originator` is the operator's own link, not a separate flow.** `getMandateSignToken(..., 'originator')` selects the originator recipient token; `DocumentSignPage` skips the summary and shows "Your signature is recorded. Thank you." with NO payment CTA on completion. `DocumentSignPage.tsx:52/162/164`.
- **The transitional double-mount means the same public router answers two prefixes.** `/api/v1/documenso` (primary) AND `/api/v1/engagement-mandate-drafts` (alias) both resolve to `documensoPublicRoutes`; the operator CRUD router mounts AFTER, on the same `engagement-mandate-drafts` prefix. `index.ts:123/124/126`.
- **Two distinct lines in `MandateDraftShell`:** line 77 seeds `status='ready'` on localStorage hydration; line 89 is the re-originate guard. Citing one for the other inverts cause/effect.

---

## Unverified / carried forward

- **The `core-x` reference doc `DIRECT_TO_DOCUMENSO_PAYMENT_E2E.md` does NOT exist** at the path named in the task brief (`apps/edge_api/../docs/reference/DIRECT_TO_DOCUMENSO_PAYMENT_E2E.md`). A directory listing of `core-x/.../docs/reference/` found no file by that name (closest: `PROPOSAL_EXPERIENCE_REACT_DOCUMENSO_SPEC.md` and this directory's own `03-FLOW-direct-to-documenso.md`). The prior dossier's claim "the platform path map agrees with that reference doc, no discrepancy" therefore remains **UNVERIFIED** — the doc could not be opened because it is absent. The platform-side path map above is independently and fully verified from `rare-structure-hq` code; only the *doc-agreement* assertion is unconfirmable.
- **`edgeGetSignToken`'s `signer: 'client' | 'originator'`** — the SPA only ever passes `'originator'` (default else = client/prospect). Whether edge_api meaningfully distinguishes a literal `'client'` string from the default was NOT traced into edge_api (out of `rare-structure-hq` scope). `rare-structure-hq:apps/platform-api/src/lib/edge.ts:563`.
- **`StripePaymentSection` ACH-only** is INFERRED from the absence of an `enableCard` prop in `PaymentPage`, not read line-by-line in the source of `StripePaymentSection`.
- **`StripeMode` edge-side mint behavior** is asserted only via the shared-schema doc comment (`packages/shared/src/schemas/settings.ts:37-40`); the edge_api read at mint is out of this repo's scope — see 05-PAYMENTS.
- **`GET /api/v1/engagement-mandate-drafts/document/:envelopeId`** (+ `edgeGetMandateDraftDocument`) has NO live SPA consumer (grep negative) — appears to be an older/parallel envelope-id read path superseded by the pair reads.
