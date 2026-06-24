# Documenso Architecture — 05 · Payments

> **STATUS BANNER.** This file is the canonical reference for the **direct-to-documenso document-fee** payment surface (`metadata.kind='document'`, pair-keyed, dual-rail) — the **ONLY active Stripe integration** owned by `edge_api`. It mints intents at `POST /api/v1/documenso/payment-intent/...` and advances state on the single Stripe webhook `POST /webhooks/stripe`, which is the **sole writer** of `paid_at`/`'succeeded'`. **The legacy engagement-proposal payment lane (proposal-`ref`, ACH-only, "through-docraptor") was removed in commit `b83e002` (2026-06-18, "refactor(edge_api): remove legacy through-docraptor proposal + payment backend") — its router (`payments_v1.py`), module (`src/payments/**`), and the proposal-`ref` branch of the webhook are gone; the `business.engagement_proposals`/`engagement_payments` tables were kept (zero data loss). Any references to that lane below are historical only.** Rails are stated **definitively from code** (verified verbatim against worktree commit `e029728`), against several **stale "ACH-only" comments** — see [Traps](#traps).
>
> This doc also covers the two **template-authoring** lanes that feed the document-payment surface but never touch Stripe themselves: the **embed-template** sign lane (`originate-embed-template` → Documenso direct link) and the **render+push** content lane (`/internal/engagement-templates/render-push` → DocRaptor PDF → Documenso TEMPLATE), with the `business.global_input_content` content-source registry.

---

## Orientation

`edge_api` is the SINGLE writer over the HQX Postgres and the ONLY caller of Stripe. There is **ONE** payment lane: the **direct-to-documenso document fee**. It keys off the `(opportunity_id, document_id)` **pair**, is **dual-rail** (`['card','us_bank_account']`), and stores state on `business.document_payments` + the ledger `business.document_payment_events`. The webhook discriminates by `event.data.object.metadata.kind`: `'document'` → `_handle_document_payment`; **anything else is ignored** (`reason='not a document payment'`, `apps/edge_api/src/routers/webhooks_stripe.py:73`) — this is the only `kind` `edge_api` mints. The lane is ACTIVE end-to-end through the cockpit SPA → the dumb platform-api BFF → `edge_api`. A "durable fulfillment" Trigger.dev seam exists on the `succeeded` path but is an intentional STUB — only a log line fires.

> **Removed (historical).** The legacy engagement-proposal payment lane — a PUBLIC proposal-`ref` capability, **ACH-only**, with state on `business.engagement_proposals` and an audit ledger on `business.engagement_events` — was deleted in commit `b83e002` (2026-06-18). The router `payments_v1.py`, the module `src/payments/**`, the proposals router `proposals_v1.py`, and the webhook's `kind != 'document'` fall-through are all gone; `main.py` no longer mounts a proposals/payments router. The `engagement_proposals`/`engagement_payments` tables and the template-authoring surface were kept untouched (zero data loss). Do not look for a second payment lane in this repo — there isn't one.

---

## Route map (the document-fee lane + the shared webhook + the authoring lanes)

| Surface | Method + path | Repo / owner | Auth |
|---|---|---|---|
| Document fee mint | `POST /api/v1/documenso/payment-intent/{opportunity_id}/{document_id}` | `edge_api` `apps/edge_api/src/routers/document_payments_v1.py:84` | PUBLIC (pair = capability) |
| Document fee state poll | `GET /api/v1/documenso/payment/{opportunity_id}/{document_id}` | `edge_api` `apps/edge_api/src/routers/document_payments_v1.py:228` | PUBLIC |
| **Stripe webhook** | `POST /webhooks/stripe` | `edge_api` `apps/edge_api/src/routers/webhooks_stripe.py:47` | Stripe-signed (verify-any-secret) |
| Embed-template originate (sign lane) | `POST /api/v1/engagement-mandate-drafts/{draft_id}/originate-embed-template` | `edge_api` `apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:169` | service-token |
| Render+push (content lane) | `POST /internal/engagement-templates/render-push` | `edge_api` `apps/edge_api/src/routers/internal_engagement_templates_v1.py:84` | trigger-secret |

The webhook router declares `prefix="/webhooks"` (`apps/edge_api/src/routers/webhooks_stripe.py:36`) and the handler is `@router.post("/stripe")` (`:47`; the `async def stripe_webhook` follows at `:48`); it is mounted in `main.py` with no prefix override (`apps/edge_api/main.py:299`), with a comment that it sits **outside `/api/v1`** because that is the path Stripe posts to (`apps/edge_api/main.py:297`–`:298`; docstring `apps/edge_api/src/routers/webhooks_stripe.py:19`). The document-payments router (`prefix="/api/v1/documenso"`, `apps/edge_api/src/routers/document_payments_v1.py:31`) is registered at `apps/edge_api/main.py:224`.

---

## The Stripe webhook — `POST /webhooks/stripe`

### Signature verification (verify-against-ANY-secret)

The webhook verifies via `doc_pay_stripe.construct_event_any(raw, stripe_signature)` (`apps/edge_api/src/routers/webhooks_stripe.py:56`). The document-payment **mode is operator-toggleable at runtime**, so events arrive signed by whichever mode (`test`/`live`) minted the intent — comment at `apps/edge_api/src/routers/webhooks_stripe.py:53`–`:55`.

`construct_event_any` (`apps/edge_api/src/document_payments/stripe.py:172`) reads **all** configured webhook signing secrets via `config.stripe_webhook_secrets()` (`apps/edge_api/src/config.py:122`) — `STRIPE_WEBHOOK_SECRET_TEST` (`config.py:129`), `STRIPE_WEBHOOK_SECRET_LIVE` (`config.py:130`), and the bare `STRIPE_WEBHOOK_SECRET` (`config.py:131`), de-duplicated (`config.py:133`) — and tries each in a loop. It **raises** `StripeError('...is not set')` when none are configured and `StripeError('webhook signature verification failed: ...')` when none verify. It never accepts an unverified event.

The route maps the two failure classes to **distinct HTTP codes**: `503` when the message contains `'not set'` (`webhooks_stripe.py:59`→`:60`), else `400` (`:61`).

### Dispatch by `metadata.kind`

```
event = construct_event_any(raw, sig)            # 503 (no secret) / 400 (bad sig)
obj   = event.data.object
if obj.metadata.kind == 'document':              # webhooks_stripe.py:70
    return _handle_document_payment(...)         # (:71)
# anything without kind='document' is IGNORED — no other payment kind is minted
return {'ok':True,'ignored':True,'event':...,'reason':'not a document payment'}  # :73
```

`_EVENT_TO_STATUS` maps Stripe event types → persisted statuses (`apps/edge_api/src/routers/webhooks_stripe.py:39`):

| Stripe event | Persisted `payment_status` |
|---|---|
| `payment_intent.processing` | `processing` (`:40`) |
| `payment_intent.succeeded` | `succeeded` (`:41`) |
| `payment_intent.payment_failed` | `failed` (`:42`) |
| `payment_intent.canceled` | `canceled` (`:43`) |

Inside `_handle_document_payment`, any other event type yields `status=None` and is ignored as `{'ok':True,'ignored':True,'event':...}` — guard at `webhooks_stripe.py:88`→`:89`.

### Document-payment handler — `_handle_document_payment`

`apps/edge_api/src/routers/webhooks_stripe.py:76`. Audit-first, advance-on-first-sight, single commit:

```
status        = _EVENT_TO_STATUS.get(event_type)  # :83
intent_id     = obj.id                            # :84
document_id   = obj.metadata.document_id          # :86
opportunity_id= obj.metadata.opportunity_id       # :87
if status is None or not document_id: ignore      # :88
paid = status == 'succeeded'                       # :96
# 1) AUDIT — append the verbatim event (idempotent on stripe_event_id)
first_seen = record_event_if_new(...)              # :99
if first_seen:                                      # :108
    rail = _resolve_rail(...)                       # :109
    advance_status(document_id, status, paid, intent_id, rail)   # :110
await conn.commit()                                 # :118  (audit + advance commit TOGETHER)
if paid and first_seen: logger.info('... PAID — fulfillment seam')  # :120-125  (STUB)
```

### Idempotency & monotonicity

| | `document_payments` |
|---|---|
| Idempotency key | `ON CONFLICT (stripe_event_id) DO NOTHING` in `record_event_if_new` (`apps/edge_api/src/document_payments/queries.py:148`); UNIQUE `stripe_event_id` |
| Rank order | `none=0,requires_payment=1,processing=2,succeeded=3` (`apps/edge_api/src/document_payments/queries.py:160`/`:164`) |
| Forward-state guard | `%(rank)s > {rank_case}` (`document_payments/queries.py:187`) |
| Terminal-protect guard | `failed`/`canceled` apply only WHERE `payment_status <> 'succeeded'` (`document_payments/queries.py:185`) |
| `paid_at` set-once | `paid_at = CASE WHEN %(paid)s THEN COALESCE(paid_at, now()) ELSE paid_at END` (`document_payments/queries.py:191`) |

`advance_status` (`apps/edge_api/src/document_payments/queries.py:167`) additionally sets `rail = COALESCE(rail, %(rail)s)` (`:195`) and `stripe_payment_intent_id = COALESCE(%(intent_id)s, stripe_payment_intent_id)` (`:194`).

> **The webhook is the SOLE writer of `'succeeded'`/`paid_at` on document payments.** Negative claim: `business.document_payments` has exactly one UPDATE writer — `advance_status` (`document_payments/queries.py:167`), called only from the webhook at `webhooks_stripe.py:110`; its only INSERT writer is `upsert_intent` (mint), which never writes `'succeeded'`.

---

## The document-fee lane — direct-to-documenso

### Mint — `POST /api/v1/documenso/payment-intent/{opportunity_id}/{document_id}`

PUBLIC (no auth dependency on the handler; the `(opportunity_id, document_id)` pair IS the capability), `response_model=DocumentPaymentInitPublic` (`apps/edge_api/src/routers/document_payments_v1.py:84`/`:86`/`:88`). Control flow:

```
# STRIPE MODE — single-operator global selection
mode = resolve_stripe_mode(get_stripe_mode_selection(conn))            # :96
if secret_key_for_mode(mode) is None or not publishable: 503 'Stripe is not configured'  # :98-99

# SIGNED GATE — offline projection (no live Documenso call)
state = read_sign_state(opportunity_id, document_id)                    # :103
if not state['signed']: 409 'agreement not yet signed'                  # :106-107

# AMOUNT + CONTACT — resolved server-side from the opportunity handle
info = get_fee_and_contact(opportunity_id)                              # :110
if not info: 404 'opportunity not found'                                # :111-112
charge_cents = resolve_fee_cents(info['field_values'])                  # :113
if charge_cents <= 0: 409 'no payable fee_amount for this opportunity'  # :114-115
if not info['recipient_email']: 422 'opportunity contact has no email'  # :116-117

# ALREADY-PAID GUARD
existing = get_payment(document_id) or {}                               # :119
if existing.payment_status == 'succeeded': 409 'already paid'           # :120-121

# REUSE / RETRY  (see below)
...
# MINT — dual-rail, then persist
upsert_intent(..., status='requires_payment', currency='usd')          # :206
return DocumentPaymentInitPublic(payment_status='requires_payment', ...)
```

**Signed gate (offline).** `read_sign_state` (`apps/edge_api/src/documenso_webhooks/queries.py:63`) derives `signed` **FULLY OFFLINE** from raw rows in `business.documenso_webhook_events` — no projection table, no live Documenso call. `signed = bool_or(event = ANY(%(terminal)s))` (`:90`) where `_TERMINAL_EVENTS=('DOCUMENT_COMPLETED',)` (`:41`), matched on `FROM business.documenso_webhook_events` (`:94`) `WHERE external_id = %(opportunity_id)s` (`:95`) `AND envelope_id = %(document_id)s` (`:96`). `external_id` = the 8-char public opportunity handle; `envelope_id` = Documenso's numeric document id (docstring `:69`/`:71`). This also enforces the pair: a document not belonging to the opportunity is not "signed" for it.

**Amount resolution.** `resolve_fee_cents(info['field_values'])` (`apps/edge_api/src/document_payments/amount.py:19`) reads `field_values['fee_amount']` (`FEE_KEY='fee_amount'`, `amount.py:16`; raw extracted `:21`), strips all non-`[0-9.]` chars via `re.sub(r"[^0-9.]","",str(raw))` (`:25` — removes `$`, commas, whitespace, `/month`), parses via `Decimal` (`:29`), and returns `int((dollars*100).quantize(...))` (`:34`). Returns `0` when absent/unparseable/≤0. The charge is **never** taken from the browser.

**Fee + contact.** `get_fee_and_contact(opportunity_id)` (`apps/edge_api/src/document_payments/queries.py:41`) JOINs `business.opportunities o` (`:50`) → `business.opportunity_specific_content osc ON osc.opportunity_id = o.id` (`:51`), `LEFT JOIN business.contacts c ON c.id = o.contact_id` (`:52`), `WHERE o.opportunity_id = %s` (`:53` — the 8-char handle, not the row UUID). `recipient_name` is `NULLIF(TRIM(CONCAT_WS(' ', first_name, last_name)), '')` (`:49`). The three upstream tables are read-only here (upstream-owned; no CREATE TABLE under `apps/edge_api/sql` for them).

**Stripe mode.** `resolve_stripe_mode(get_stripe_mode_selection(conn))` (`document_payments_v1.py:96`). `get_stripe_mode_selection` reads `stripe_mode FROM public.operator_settings WHERE stripe_mode IS NOT NULL ORDER BY updated_at DESC LIMIT 1` (`apps/edge_api/src/document_payments/queries.py:72`–`:75`) — latest non-null row wins; on this single-operator platform that one row carries the platform-wide selection (the prospect mint has no operator session). `resolve_stripe_mode` (`apps/edge_api/src/config.py:99`): for `'test'`/`'live'` it indirects through `STRIPE_MODE_{TEST,LIVE}` (falling back to the literal selection) (`:102`–`:103`); when `None` it falls back to env `STRIPE_MODE` (`:104`, default `'test'` at `config.py:62`). `stripe_secret_key_for_mode` / `stripe_publishable_key_for_mode` resolve `{base}_{LIVE|TEST}` with fallback to bare `{base}` (`config.py:108`–`:109`); the secret is server-side only (`:112`), the publishable key is surfaced to the browser (`:117`).

### Mint — the DEFINITIVE rail (dual-rail, NOT ACH-only)

`create_payment_intent` (`apps/edge_api/src/document_payments/stripe.py:65`) mints with **`payment_method_types=["card", "us_bank_account"]`** (`stripe.py:87`) — verified verbatim. `card` is listed first so the instant rail leads the Stripe Element's tabs (docstring `:74`/`:76`). It also sets:

| Field | Value | Citation |
|---|---|---|
| `currency` | `_CURRENCY` = `'usd'` | `stripe.py:85` (const at `:20`) |
| `payment_method_types` | `["card", "us_bank_account"]` | `stripe.py:87` |
| `setup_future_usage` | `'off_session'` (both rails — stores instrument for later quarterly debit) | `stripe.py:88` |
| `payment_method_options` | `{"us_bank_account": {"verification_method": "automatic"}}` | `stripe.py:89` |
| `description` | `'Rare Structure engagement — document {opportunity_id}/{document_id}'` | `stripe.py:90` |
| `metadata` | `{kind:'document', opportunity_id, document_id}` (routes the webhook) | `stripe.py:91`–`:94` |

> The deleted legacy lane (`src/payments/stripe_client.py`, removed in `b83e002`) was the ACH-only counterpart — `payment_method_types=['us_bank_account']`, `metadata={'ref':ref,'kind':'engagement'}`. That `kind='engagement'` discriminant no longer reaches a handler; the webhook ignores it.

### Mint — reuse, retry, and idempotency

```
if existing_intent and existing_status not in ('failed','canceled'):   # :131  reuse open intent
    intent = retrieve_payment_intent(existing_intent, mode)            # :133
    stale_single_rail = 'card' not in intent.payment_method_types      # :134
    if stale_single_rail and intent.status in _AMOUNT_MUTABLE:         # :135
        cancel_payment_intent(existing_intent, mode)                   # :136  → fall through to fresh dual-rail mint
    else:
        if intent.amount != charge_cents and intent.status in _AMOUNT_MUTABLE:  # :139
            update_payment_intent_amount(...)                          # :140  re-sync amount
        return existing client_secret                                 # :144-153
    except Exception: logger.warning('reuse ... failed; minting a new one'); fall through  # :154/:157
# HARD-FAILURE RETRY
if existing_status in ('failed','canceled') and existing_intent:       # :171
    try: cancel_payment_intent(...)  except: pass  (non-fatal)         # :174-176
```

- `_AMOUNT_MUTABLE = {'requires_payment_method','requires_confirmation','requires_action'}` (`document_payments_v1.py:34`). An intent already `'processing'` (mid-ACH) is **left alone** so the in-flight debit is never disrupted, even though it lacks the card tab (comment `:129`).
- **Reuse is a pure optimization:** any exception during reuse is caught, logged as a warning, and degrades to a fresh mint — never a 500 — because the idempotency key makes the mint return the same intent (`:154`/`:157`).
- **Idempotency key scheme** (`_mint_idempotency_key`, `document_payments_v1.py:66`): a pristine pair (or reuse fall-through on an open intent) → `f'pay_document_{document_id}'` (`:81`); a retry after a HARD FAILURE (failed/canceled **with** an existing intent) → `f'pay_document_{document_id}_retry_{existing_intent}'` (`:79`–`:80`) so a post-failure fee edit does not trigger a Stripe 400 `idempotency_error`.
- On a `StripeError` during `ensure_customer`/`create_payment_intent` the mint raises **502** `'stripe: {exc}'` (`document_payments_v1.py:200`/`:204`).

`ensure_customer` (`apps/edge_api/src/document_payments/stripe.py:38`) reuses an existing Stripe customer id only if it still resolves and is **not deleted** in the current mode (`:45`→`Customer.retrieve` `:47`→`if not getattr(cust,'deleted',False): return existing_id` `:48`); otherwise it mints a fresh customer with `metadata={'source':'edge_api/document'}` (`:57`) and the caller re-persists it (self-heal).

`upsert_intent` (`apps/edge_api/src/document_payments/queries.py:83`) does `INSERT INTO business.document_payments` (`:100`) `... ON CONFLICT (document_id) DO UPDATE` (`:105`) and `COMMITS` (`:127`); a terminal `'succeeded'` is **never** downgraded by a re-mint (`payment_status = CASE WHEN ...='succeeded' THEN keep ELSE EXCLUDED`, `:111`–`:114`).

### State poll — `GET /api/v1/documenso/payment/{opportunity_id}/{document_id}`

PUBLIC, `response_model=DocumentPaymentStatePublic` (`apps/edge_api/src/routers/document_payments_v1.py:228`/`:230`/`:232`). Returns `DocumentPaymentStatePublic(payment_status='none')` **both** before the first mint **and** on a pair mismatch — `pay = get_payment(conn, document_id)` (`:238`); `if pay is None or pay.get('opportunity_id') != opportunity_id` (`:239`) → return `'none'` (`:240`). A guessed document id therefore leaks no info, and the SPA can poll without erroring.

### Rail attribution (cosmetic, set-once)

`_resolve_rail` (`apps/edge_api/src/routers/webhooks_stripe.py:132`):

```
if status == 'processing': return 'us_bank_account'   # :145-146  (call-free; only ACH emits processing)
if status != 'succeeded' or not intent_id: return None # :147
existing = get_payment(conn, document_id)              # :152
if existing and existing.rail: return None             # :153  (already pinned by prior processing — keep it)
return retrieve_settled_rail(intent_id, mode)          # :155-156  (authoritative charge read, ambiguous succeeded only)
```

`retrieve_settled_rail` (`apps/edge_api/src/document_payments/stripe.py:128`) calls `PaymentIntent.retrieve(intent_id, expand=['latest_charge'])` (`:139`) and reads `latest_charge.payment_method_details.type` (`:147`); returns `None` on any failure (`:141`/`:148`) — caller treats `None` as "leave rail unset". Rail is **cosmetic** (used only to tailor paid-state copy) and never raises into the webhook (set once via `COALESCE`, `document_payments/queries.py:195`).

### Tables

`business.document_payments` (`apps/edge_api/sql/document_payments.sql:16`):

| Column | Type | Notes | Cite |
|---|---|---|---|
| `document_id` | text **PRIMARY KEY** | Documenso numeric id (the unique pin) | `:17` |
| `opportunity_id` | text NOT NULL | 8-char handle (the pair capability) | `:18` |
| `amount_cents` | integer NOT NULL | frozen charge in cents | `:19` |
| `currency` | text NOT NULL DEFAULT `'usd'` | | `:20` |
| `stripe_customer_id` | text | | `:21` |
| `stripe_payment_intent_id` | text | | `:22` |
| `payment_status` | text NOT NULL DEFAULT `'none'` | enum below | `:24` |
| `rail` | text | `'card'` \| `'us_bank_account'` — stamped ONCE by the webhook | `:25` |
| `paid_at` | timestamptz | set ONCE by the webhook on succeeded | `:26` |
| `created_at` / `updated_at` | timestamptz | | `:27`/`:28` |

`payment_status` enum (SQL comment, `:23`): `none | requires_payment | processing | succeeded | failed | canceled`. Backfill `ALTER TABLE ... ADD COLUMN IF NOT EXISTS rail text` (idempotent) at `:32`; indexes `idx_document_payments_opportunity_id` (`:35`) and `idx_document_payments_intent_id` (`:37`).

`business.document_payment_events` (`apps/edge_api/sql/document_payments.sql:42`) — append-only Stripe webhook audit + idempotency ledger: `id bigint GENERATED ALWAYS AS IDENTITY PK` (`:43`), `document_id text` (`:44`), `opportunity_id text` (`:45`), `stripe_event_id text NOT NULL UNIQUE` (the idempotency key, `:46`), `event_type text` (`:47`), `payload jsonb NOT NULL` (raw event = SoR, `:48`), `received_at timestamptz` (`:49`); index `idx_document_payment_events_document_id` (`:52`).

`business.documenso_webhook_events` — raw Documenso webhook capture; the signed-gate source (`DOCUMENT_COMPLETED` on the `external_id`/`envelope_id` pair, `apps/edge_api/src/documenso_webhooks/queries.py:94`/`:41`).

Response models: `DocumentPaymentInitPublic` (`apps/edge_api/src/document_payments/models.py:7`) — `client_secret`, `publishable_key`, `amount_cents`, `currency='usd'`, `payment_status`, optional `recipient_name`/`recipient_email`. `DocumentPaymentStatePublic` (`models.py:22`) — `payment_status`, optional `amount_cents`, `currency='usd'`, optional `paid_at`/`rail`. The secret key and customer id never leave the server.

---

## Authoring lanes that feed the document-fee surface

The document-fee mint requires a **signed Documenso document** for the `(opportunity_id, document_id)` pair, and that document is instantiated from a **Documenso TEMPLATE**. Two repo-owned lanes produce those upstream artifacts. Neither touches Stripe; they are documented here because the payment gate depends on their output.

### Embed-template sign lane — `POST /api/v1/engagement-mandate-drafts/{draft_id}/originate-embed-template`

Service-token gated (`Depends(require_service_token)`, `apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:170`); the handler is at `:172`. PARALLEL to `originate-prefilled` (the embed-document lane, which is left untouched, `:175`/`:183`) — it does not replace it. Selected by `operator_settings.direct_to_documenso_lane = 'embed-template'` (the lane domain is `{envelope-distribute (RETIRED), prefill-document-from-template (DEFAULT), embed-template}`, `apps/edge_api/sql/operator_settings.sql:86`–`:88`; the Pydantic `DirectToDocumensoLane` Literal at `apps/edge_api/src/operator_settings/models.py:21`–`:22`).

Flow: it loads the draft + the opportunity's public handle/contact (`:187`/`:190`), reads the template's recipients (`documenso_client.get_template_recipients`, `:196`), picks the direct recipient (`body.direct_recipient_id` override or `_pick_direct_recipient_id`, `:197`), and enables a Documenso DIRECT LINK on the template (`documenso_client.create_direct_link`, `:198`). **No document is minted here** — the signer self-identifies in the embed and Documenso creates the document (source `TEMPLATE_DIRECT_LINK`) at completion. A `DocumensoError` → `502` (`:201`–`:202`); a token-less link → `502` (`:203`–`:204`).

Returns `MandateEmbedTemplateOriginated` (`apps/edge_api/src/engagement_mandate_drafts/models.py:49`): `direct_token`, `documenso_host`, `embed_url` (`{host}/embed/direct/{token}`, `:214`), `external_id`/`opportunity_id` (the public 8-char handle, both `:215`/`:216`), `direct_recipient_id`, `recipient_email`, `recipient_name` (optional embed prefill — the signer may still change them), `status='ready'` (`:211`–`:221`). The prospect signing-state surface then tracks the completed document by the `external_id == opportunity_id` gate; once captured, the `(opportunity_id, document_id)` pair feeds the document-fee mint above.

**Documenso direct-link client** (`apps/edge_api/src/services/documenso_client.py`): `DirectLinkResult` (`:469`), `create_direct_link` (`:514`, `POST /api/v2/template/direct/create {templateId, directRecipientId?}` → `{token,...}`, `:519`/`:529`), `toggle_direct_link` (`:542`, `POST /api/v2/template/direct/toggle`), and `get_template_recipients` (`:504`) are NEW. `create_document_from_template` (the embed-document path, `:228`) is unchanged. The `token` is the `<EmbedDirectTemplate>` prop AND the public `/d/{token}` / iframe `/embed/direct/{token}`; the signer enters their own name + email.

### Render+push content lane — `POST /internal/engagement-templates/render-push`

Trigger-secret gated (`Depends(require_trigger_secret)`, `apps/edge_api/src/routers/internal_engagement_templates_v1.py:84`), mounted under `/internal` (`apps/edge_api/main.py:274`) so the full path is `/internal/engagement-templates/render-push`. Called by the Trigger.dev task `"engagement-template-push"` (cross-repo: `rare-structure-hq:src/trigger/engagement_template_push.ts` — UNVERIFIED here). Renders a content source to a PDF and creates a Documenso TEMPLATE from the bytes:

```
render.assemble_html(content_dir, style)            # push.py:92
render.render_pdf(html)                             # DocRaptor LIVE PDF — push.py:99
store ... ; documenso_client.create_template_from_pdf(...)  # push.py:114
```

`render_and_push` (`apps/edge_api/src/engagement_templates/push.py:63`) is DB-free (pure HTTP/filesystem) and raises typed errors mapped by the router to `400` (`push.PushError`/`render.StyleError`, `:98`–`:100`), `503` (`render.RenderConfigError`, `:101`–`:103`), or `502` (`render.RenderError`/`documenso_client.DocumensoError`, `:104`–`:106`). On success it records `ops.engagement_template_push_runs` (`push.record_run`, `:109`; ledger DDL `apps/edge_api/sql/ops_engagement_template_push_runs.sql:12`) with the resolved `(brand, path, archetype, version, style, source_kind)`, the `documenso_template_id` + numeric id, and the PDF's R2 key.

`create_template_from_pdf` (`apps/edge_api/src/services/documenso_client.py:420`) is NEW — `POST /api/v2/envelope/create` (multipart `payload` JSON + the PDF file) with `type=TEMPLATE` (`:437`/`:440`), returning `TemplateCreateResult` (`:410`).

**Brand-aware catalog.** `apps/edge_api/src/engagement_templates/catalog.py` resolves a template by `content/<brand>/<path>/<archetype>/<version>/global_engagement_content/manifest.json` (`:4`). `_ALLOWED_BRANDS = {active-operators, rare-structure}` (`:28`) is enforced in both `list_templates` (`:65`) and `resolve` (`:99`/`:105`) — an unvetted directory can never surface as selectable. `brand` defaults to `active-operators` (`:21`) so the original three-segment call sites keep working.

**Brand asset tree.** `apps/edge_api/content/rare-structure/docraptor-to-documenso-template/capital-origination/v1/global_engagement_content/` is the rare-structure capital-origination template family (static-blank HTML: field-slot blanks with no underscore glyphs, the §8.4 Authority rep, 1.6 leading, plain + branded styles). It maps to the live Documenso template numeric id `14310` at runtime (a Documenso-side fact, not a repo constant).

### `business.global_input_content` — content-source registry

`apps/edge_api/sql/global_input_content.sql:21`. The registry of which content sources are selectable for the render+push lane. It gained `brand` (`'active-operators' | 'rare-structure'`, `ALTER ... ADD COLUMN ... DEFAULT 'active-operators'`, `:31`) and `source_kind` (`ALTER ... DEFAULT 'repo-html'`, `:32`), the latter guarded by a `CHECK (source_kind IN ('repo-html', 'db-markdown'))` constraint added idempotently (`:43`–`:45`). `path` is the brand-relative `<family>/<archetype>/<version>` for `repo-html` or a slug for `db-markdown` (`:23`). Seeds two rows (`ON CONFLICT` keeps any hand-provisioned row, `:53`–`:55`): the active-operators term-only template (`docraptor-to-documenso-template/term-only/v1`) and the rare-structure capital-origination template (`docraptor-to-documenso-template/capital-origination/v1`).

---

## Cross-repo handoffs (SPA → BFF → edge_api)

> **CROSS-REPO — UNVERIFIED.** The SPA (`platform-app`) and BFF (`platform-api`) live in the SEPARATE `rare-structure-hq` repo, not verifiable from this codebase. The `rare-structure-hq:...` citations below are carried forward from prior audits and may have drifted. The `edge_api` CONTRACT they consume (route shapes + response models) is verified in this repo and is authoritative.

Architecture invariant: `platform-app` → `platform-api` (DUMB BFF, no business logic / no DB for these flows) → `edge_api`. The BFF only remaps field names (snake → camel) and translates error codes.

**MINT.** `createDocumentPaymentIntent` (`rare-structure-hq:apps/platform-app/src/proposals/api.ts:307`, fetch `:312`) → `POST` BFF `/api/v1/documenso/sign/:opportunityId/:documentId/payment-intent` (`rare-structure-hq:apps/platform-api/src/routes/documenso-public.ts:90`, **no `requireUser`** — pair is the capability) → `edgeCreateDocumentPaymentIntent` (`rare-structure-hq:apps/platform-api/src/lib/edge.ts:595`, fetch `:600`) → `edge_api POST /api/v1/documenso/payment-intent/{opp}/{doc}`. The BFF camelCases to `clientSecret`/`publishableKey`/`amountCents`/`currency`/`paymentStatus`/`contactName`/`contactEmail` (`documenso-public.ts:97`–`:103`) and **propagates the edge HTTP status verbatim** (`const status = e.status ?? 502` `:109`; re-throw `:110`; `.status` set on `EdgeError` at `edge.ts:606`) so a `409` reaches the SPA as "sign first".

**STATE POLL.** `getDocumentPaymentState` (`rare-structure-hq:apps/platform-app/src/proposals/api.ts:334`, fetch `:339`) → `GET` BFF `/api/v1/documenso/sign/:opportunityId/:documentId/payment` (`rare-structure-hq:apps/platform-api/src/routes/documenso-public.ts:118`) → `edgeGetDocumentPaymentState` (`rare-structure-hq:apps/platform-api/src/lib/edge.ts:622`, fetch `:627`) → `edge_api GET /api/v1/documenso/payment/{opp}/{doc}`. CamelCases to `paymentStatus`/`amountCents`/`currency`/`paidAt`/`rail` (`documenso-public.ts:125`–`:129`); this state-poll route maps `EdgeError → 502` (`:134`), NOT verbatim.

**Embed-template sign (cross-repo).** The SPA's `MandateDraftShell` carries a THIRD dispatch branch (alongside embed-document + the retired distribute) for `direct_to_documenso_lane='embed-template'`, calling the BFF `originate-embed-template` → `edge_api POST /api/v1/engagement-mandate-drafts/{draft_id}/originate-embed-template`. It mounts `DirectTemplateSignPage` (`EmbedDirectTemplate`) at `/p/t/:opportunityId/:directToken` with `?host=`, threading the `direct_token`/`documenso_host` from the response. The signer self-identifies (name/email NOT locked). `SignLink` is a discriminated union and the `DirectToDocumensoLane` literal is shared. ALL of this is `rare-structure-hq` and UNVERIFIED here; the matching `edge_api` contract is the `MandateEmbedTemplateOriginated` model above.

**BFF dual mount.** `documensoPublicRoutes` is mounted at `/api/v1/documenso` (`rare-structure-hq:apps/platform-api/src/index.ts:123`) AND the legacy transitional alias `/api/v1/engagement-mandate-drafts` (`:124`) so an in-flight SPA bundle calling the old path keeps working across independent deploys. A SEPARATE operator-authed router (`engagementMandateDraftRoutes`) is also mounted at the legacy prefix (`index.ts:126`) — the alias and the real legacy router coexist; do not conflate.

**SPA pay page.** `/p/m/:opportunityId/:documentId/pay` (`rare-structure-hq:apps/platform-app/src/routes/p/DocumentPaymentPage.tsx:2`). ACH settles asynchronously: `confirmPayment` returns `processing`, NOT `succeeded` (`DocumentPaymentPage.tsx:14`); the page polls `getDocumentPaymentState` every **5000ms** (`:97`, `startPolling` `:66`) until terminal. **Server truth only advances the page; the browser confirm result never sets paid** (`:65`). SPA error branching: `if e instanceof PaymentError && e.status===409` (`:133`); `/already paid/i → succeeded` else `unsigned` (`:136`); any other error → `unavailable` (`:138`). The confirm-result `SettledHint` maps `succeeded → rail 'card'` / `processing → rail 'us_bank_account'` as an instant client-side hint (`rare-structure-hq:apps/platform-app/src/proposals/StagedAchForm.tsx:494`/`:498`), refined by the server poll; `enableCard` is passed (`rare-structure-hq:apps/platform-app/src/proposals/DocumentPaymentForm.tsx:54`) so Stripe renders the "Card | US bank account" tabs.

**PAID transition.** Stripe → `edge_api POST /webhooks/stripe` (`webhooks_stripe.py:47`) is the only thing that flips `'succeeded'`/`paid_at`. The SPA learns it only via its state poll.

---

## Schema-drift seatbelt & boot-time apply

`_payments_db()` (`apps/edge_api/src/routers/document_payments_v1.py:50`) wraps every document-payment DB read/write so any of `UndefinedColumn`/`UndefinedTable`/`UndefinedFunction`/`InvalidSchemaName` (`_SCHEMA_DRIFT_ERRORS`, `:42`–`:47`) is converted to **HTTP 503 `'payment is temporarily unavailable'`** instead of a raw 500 (`:58`→`:63`). This is the seatbelt for the PR #518 rail-column outage (comment `:36`–`:41`).

The primary fix is the startup apply: `run_migrations` (`apps/edge_api/src/migrate.py:64`) re-applies every `sql/*.sql` (sorted glob) to `HQX_DB_URL_POOLED` before serving, each in its own transaction guarded by a transaction-scoped `pg_advisory_xact_lock` to serialize concurrent replica boots. Escape hatch: `EDGE_API_SKIP_DB_MIGRATE=1` (`apps/edge_api/src/config.py:233`).

---

## Environment variables

| Var | Role | Cite |
|---|---|---|
| `STRIPE_SECRET_KEY` / `STRIPE_SECRET_KEY_{LIVE,TEST}` | Stripe secret (sk_), server-side only; resolved by explicit mode (`stripe_secret_key_for_mode`) | `apps/edge_api/src/config.py:112`/`:107` |
| `STRIPE_PUBLISHABLE_KEY_{LIVE,TEST}` | publishable (pk_), surfaced to browser in the Init response (`stripe_publishable_key_for_mode`) | `apps/edge_api/src/config.py:117` |
| `STRIPE_WEBHOOK_SECRET_{LIVE,TEST}` + bare | webhook signing secrets; ALL tried by `construct_event_any` | `apps/edge_api/src/config.py:129`/`:130`/`:131` |
| `STRIPE_MODE` / `STRIPE_MODE_{TEST,LIVE}` | default mode (defaults `'test'`) + indirection target for `operator_settings.stripe_mode` | `apps/edge_api/src/config.py:62`/`:103` |
| `EDGE_API_SKIP_DB_MIGRATE` | skip the boot-time `sql/*.sql` apply | `apps/edge_api/src/config.py:233` |

---

## Status: ACTIVE / CONDITIONAL / DEPRECATED / STUB

| Component | Status | Note |
|---|---|---|
| `POST /webhooks/stripe` | **ACTIVE** | sole writer of `'succeeded'`/`paid_at` on document payments (`webhooks_stripe.py:47`) |
| `POST /api/v1/documenso/payment-intent/{opp}/{doc}` | **ACTIVE** | dual-rail mint (`document_payments_v1.py:84`) |
| `GET /api/v1/documenso/payment/{opp}/{doc}` | **ACTIVE** | state poll (`document_payments_v1.py:228`) |
| `_handle_document_payment` | **CONDITIONAL** | runs only when `metadata.kind=='document'` (`webhooks_stripe.py:70`) |
| dual-rail mint `create_payment_intent` | **ACTIVE** | `['card','us_bank_account']` (`stripe.py:87`) |
| `_resolve_rail` / `retrieve_settled_rail` | **ACTIVE** | rail cosmetic, set-once (`webhooks_stripe.py:132`, `stripe.py:128`) |
| signed gate `read_sign_state` | **ACTIVE** | offline `DOCUMENT_COMPLETED` projection (`documenso_webhooks/queries.py:63`) |
| `construct_event_any` (verify-any-secret) | **ACTIVE** | test+live+bare (`stripe.py:172`) |
| `_payments_db` schema-drift 503 seatbelt | **ACTIVE** | (`document_payments_v1.py:50`) |
| `run_migrations` boot-time `sql/*.sql` apply | **ACTIVE** | advisory-locked (`migrate.py:64`) |
| Embed-template `POST .../originate-embed-template` | **ACTIVE** | direct-link sign lane, no document minted here (`engagement_mandate_drafts_v1.py:169`) |
| Render+push `POST /internal/engagement-templates/render-push` | **ACTIVE** | render → DocRaptor → Documenso TEMPLATE (`internal_engagement_templates_v1.py:84`) |
| `business.global_input_content` content registry | **ACTIVE** | `brand` + `source_kind` (`global_input_content.sql:21`) |
| `ops.engagement_template_push_runs` ledger | **ACTIVE** | render+push audit (`ops_engagement_template_push_runs.sql:12`) |
| `direct_to_documenso_lane = 'envelope-distribute'` | **RETIRED** | value retained in CHECK; lane code removed (`operator_settings.sql:80`) |
| BFF legacy alias `/api/v1/engagement-mandate-drafts` | **CONDITIONAL** | transitional, for in-flight bundles (`rare-structure-hq:apps/platform-api/src/index.ts:124`) |
| Post-payment fulfillment seam (Trigger.dev) | **STUB** | only a `logger.info`; nothing fires (`webhooks_stripe.py:120`–`:125`) |
| Legacy proposal-`ref` payment lane (`payments_v1.py`, `src/payments/**`) | **REMOVED** | deleted in `b83e002` (2026-06-18); tables kept |

---

## Traps

- **The legacy proposal-`ref` payment lane is GONE.** Removed in `b83e002` (2026-06-18). Do not look for `payments_v1.py`, `src/payments/**`, `proposals_v1.py`, or a `kind != 'document'` webhook branch — none exist. The webhook IGNORES any event without `metadata.kind='document'` (`webhooks_stripe.py:73`). The `engagement_proposals`/`engagement_payments` tables remain in `sql/` (zero data loss) but have no live payment code reading or writing them.
- **STALE "ACH-only" comments — code is DUAL-RAIL.** The router module docstring (`document_payments_v1.py:3` "mint/reuse ACH intent"), the SQL header (`apps/edge_api/sql/document_payments.sql:1` "Stripe ACH"), the `currency` column comment ("ACH (us_bank_account) is USD-only", `document_payments.sql:20`), and the `webhooks_stripe.py` module docstring (describes the surface as "ACH"/"engagement ACH debits", `:1`–`:19`) all **predate** the dual-rail mint. The behavior is `['card','us_bank_account']` (`stripe.py:87`). Only the prose lags. **The CODE wins.**
- **`processing` is NOT `paid`.** ACH settles asynchronously; `confirmPayment` returns `processing`. Only a `payment_intent.succeeded` webhook flips `'succeeded'`/`paid_at`. The browser confirm result never advances the page (`DocumentPaymentPage.tsx:65`, cross-repo).
- **The state poll returns `'none'` on a pair mismatch, not an error** (`document_payments_v1.py:239`–`:240`) — a `'none'` poll result does not necessarily mean "never minted"; it can mean the document id does not belong to the opportunity. The mint, by contrast, propagates a 409 verbatim through the BFF.
- **`metadata.kind` is the ONLY dispatch discriminant** (`webhooks_stripe.py:70`). An intent minted without `kind='document'` is ignored (`:73`). The document mint always sets it (`stripe.py:92`). There is no longer a fall-through path for any other `kind`.
- **`rail` is cosmetic.** It tailors paid-state copy only; it is stamped once via `COALESCE` and `retrieve_settled_rail` returning `None` is benign. Never gate logic on it.
- **Fulfillment is a STUB.** No provision/receipt/CRM action fires on payment success — only a log line marks the Trigger.dev seam (`webhooks_stripe.py:120`–`:125`). The `document_payment_events` rows are the audit of record.
- **The embed-template lane mints NO document.** `originate-embed-template` only enables a Documenso direct link and returns its token; Documenso creates the document (source `TEMPLATE_DIRECT_LINK`) when the signer completes. Until then the `(opportunity_id, document_id)` pair the payment gate needs does not exist (`engagement_mandate_drafts_v1.py:175`–`:185`).
- **Upstream DDL not in this repo.** `business.opportunities`, `business.opportunity_specific_content`, `business.contacts` are upstream-owned (no `CREATE TABLE` under `apps/edge_api/sql`); their column definitions live outside this tree. The mint joins them read-only.
