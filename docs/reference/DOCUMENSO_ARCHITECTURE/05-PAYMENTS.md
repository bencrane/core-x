# Documenso Architecture — 05 · Payments

> **STATUS BANNER.** This file is the canonical reference for **both Stripe payment surfaces** owned by `edge_api`: (A) the **legacy engagement payment** (`render_mode`/lane = the proposal-`ref` "through-docraptor" path, ACH-only) and (B) the **direct-to-documenso document fee** (lane = `metadata.kind='document'`, pair-keyed, dual-rail). Both share **one** Stripe webhook endpoint, `POST /webhooks/stripe`, which is the **single writer** of `paid_at`/`'succeeded'` on either lane. Rails are stated **definitively from code** (verified verbatim 2026-06-18), against several **stale "ACH-only" comments and a stale E2E reference doc** — see [Traps](#traps).

---

## Orientation

`edge_api` is the SINGLE writer over the HQX Postgres and the ONLY caller of Stripe. Two payment lanes coexist and are deliberately self-contained — they share no table and no code path except the one webhook router. **Lane A (LEGACY engagement payment)** keys off a proposal `ref` (the bearer capability), is **ACH-only** (`payment_method_types=['us_bank_account']`), and stores payment state as `ALTER`-added columns on `business.engagement_proposals` (there is no separate `engagement_payments` table; the Stripe audit/idempotency ledger is `business.engagement_events`). **Lane B (direct-to-documenso document fee)** keys off the `(opportunity_id, document_id)` **pair**, is **dual-rail** (`['card','us_bank_account']`), and stores state on `business.document_payments` + the ledger `business.document_payment_events`. The webhook discriminates by `event.data.object.metadata.kind`: `'document'` → Lane B handler; everything else → the Lane A proposal-`ref` path. Both lanes are ACTIVE end-to-end through the cockpit SPA → the dumb platform-api BFF → `edge_api`. A "durable fulfillment" Trigger.dev seam exists on both `succeeded` paths but is an intentional STUB — only a log line fires.

---

## Route map (both lanes + the shared webhook)

| Surface | Method + path | Repo / owner | Auth | Lane |
|---|---|---|---|---|
| Document fee mint | `POST /api/v1/documenso/payment-intent/{opportunity_id}/{document_id}` | `edge_api` `apps/edge_api/src/routers/document_payments_v1.py:84` | PUBLIC (pair = capability) | B |
| Document fee state poll | `GET /api/v1/documenso/payment/{opportunity_id}/{document_id}` | `edge_api` `apps/edge_api/src/routers/document_payments_v1.py:228` | PUBLIC | B |
| Legacy engagement mint | `POST /api/v1/proposals/{ref}/payment-intent` | `edge_api` `apps/edge_api/src/routers/payments_v1.py:36` | PUBLIC (ref = capability) | A |
| Legacy engagement state poll | `GET /api/v1/proposals/{ref}/payment` | `edge_api` `apps/edge_api/src/routers/payments_v1.py:111` | PUBLIC | A |
| **Stripe webhook (shared)** | `POST /webhooks/stripe` | `edge_api` `apps/edge_api/src/routers/webhooks_stripe.py:49` | Stripe-signed (verify-any-secret) | A + B |

The webhook router declares `prefix="/webhooks"` (`apps/edge_api/src/routers/webhooks_stripe.py:38`) and the handler is `@router.post("/stripe")` (`:49`); it is mounted in `main.py` with no prefix override (`apps/edge_api/main.py:260`), with a comment that it sits **outside `/api/v1`** because that is the path Stripe posts to (`apps/edge_api/main.py:258`; docstring `apps/edge_api/src/routers/webhooks_stripe.py:19`). The document-payments router (`prefix="/api/v1/documenso"`, `apps/edge_api/src/routers/document_payments_v1.py:31`) is registered at `apps/edge_api/main.py:186`; the legacy payments router (`prefix="/api/v1/proposals"`, `apps/edge_api/src/routers/payments_v1.py:28`) at `apps/edge_api/main.py:256`.

---

## The shared Stripe webhook — `POST /webhooks/stripe`

### Signature verification (verify-against-ANY-secret)

The webhook verifies via `doc_pay_stripe.construct_event_any(raw, stripe_signature)` (`apps/edge_api/src/routers/webhooks_stripe.py:58`). The Lane-B document-payment **mode is operator-toggleable at runtime**, so events arrive signed by whichever mode (`test`/`live`) minted the intent; the legacy events verify the same way (shared secrets, one Stripe account) — comment at `apps/edge_api/src/routers/webhooks_stripe.py:55`.

`construct_event_any` (`apps/edge_api/src/document_payments/stripe.py:172`) reads **all** configured webhook signing secrets via `config.stripe_webhook_secrets()` (`apps/edge_api/src/config.py:122`) — `STRIPE_WEBHOOK_SECRET_TEST` (`config.py:129`), `STRIPE_WEBHOOK_SECRET_LIVE` (`config.py:130`), and the bare `STRIPE_WEBHOOK_SECRET` (`config.py:131`), de-duplicated (`config.py:133`) — and tries each in a loop (`stripe.py:186`). It **raises** `StripeError('...is not set')` when none are configured (`stripe.py:180`) and `StripeError('webhook signature verification failed: ...')` when none verify (`stripe.py:189`). It never accepts an unverified event.

The route maps the two failure classes to **distinct HTTP codes**: `503` when the message contains `'not set'` (`webhooks_stripe.py:61`→`:62`), else `400` (`:63`).

### Dispatch by `metadata.kind`

```
event = construct_event_any(raw, sig)            # 503 (no secret) / 400 (bad sig)
obj   = event.data.object
if obj.metadata.kind == 'document':              # webhooks_stripe.py:73
    return _handle_document_payment(...)         # Lane B  (:74)
# else: fall through to the legacy proposal-ref path
ref = obj.metadata.ref                           # Lane A  (:76)
```

`_EVENT_TO_STATUS` maps Stripe event types → persisted statuses (`apps/edge_api/src/routers/webhooks_stripe.py:41`):

| Stripe event | Persisted `payment_status` |
|---|---|
| `payment_intent.processing` | `processing` (`:42`) |
| `payment_intent.succeeded` | `succeeded` (`:43`) |
| `payment_intent.payment_failed` | `failed` (`:44`) |
| `payment_intent.canceled` | `canceled` (`:45`) |

Any other event type yields `status=None` and is ignored as `{'ok':True,'ignored':True,'event':...}` — Lane A guard at `webhooks_stripe.py:79`→`:80`; Lane B guard at `:129`→`:130`.

### Lane B handler — `_handle_document_payment`

`apps/edge_api/src/routers/webhooks_stripe.py:117`. Audit-first, advance-on-first-sight, single commit:

```
status        = _EVENT_TO_STATUS.get(event_type)
document_id   = obj.metadata.document_id         # :127
opportunity_id= obj.metadata.opportunity_id      # :128
if status is None or not document_id: ignore     # :129
paid = status == 'succeeded'                      # :137
# 1) AUDIT — append the verbatim event (idempotent on stripe_event_id)
first_seen = record_event_if_new(...)             # :140
if first_seen:                                     # :149
    rail = _resolve_rail(...)                      # :150
    advance_status(document_id, status, paid, intent_id, rail)   # :151
conn.commit()                                      # :159  (audit + advance commit TOGETHER)
if paid and first_seen: logger.info('... PAID — fulfillment seam')  # :161-166  (STUB)
```

### Lane A handler — legacy proposal-ref path (inline in `stripe_webhook`)

```
ref = obj.metadata.ref                             # webhooks_stripe.py:76
status = _EVENT_TO_STATUS.get(event_type)
if status is None or not intent_id: ignore         # :79
if not ref: ref = pay_queries.ref_for_intent(intent_id)   # :90  (lookup by intent id)
if not ref: ignore reason='no matching proposal'   # :91-92
# 1) AUDIT first (idempotent on the Stripe event id)
first_seen = pay_queries.insert_event(source='stripe', idempotency_key=event_id, ...)  # :95
# 2) ADVANCE — monotonic
paid_at      = now() if status=='succeeded' else None   # :100
amount_cents = obj.amount if status=='succeeded' else None  # :101
applied = pay_queries.advance_payment_status(intent_id, status, paid_at, amount_cents)  # :102
if status=='succeeded' and first_seen and applied: logger.info('... PAID — fulfillment seam')  # :106-109  (STUB)
```

`ref_for_intent` resolves the proposal by `stripe_payment_intent_id` (`apps/edge_api/src/payments/queries.py:44`). NB `insert_event` **commits internally** (`apps/edge_api/src/payments/queries.py:138`), so on Lane A the audit row is durable **before** the advance runs — module docstring `webhooks_stripe.py:8` / `:13`.

### Idempotency & monotonicity (both lanes)

| | Lane A (`engagement_proposals`) | Lane B (`document_payments`) |
|---|---|---|
| Idempotency key | `ON CONFLICT (source, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING` in `insert_event` (`apps/edge_api/src/payments/queries.py:132`); partial UNIQUE index `engagement_events_idem_uidx` (`apps/edge_api/sql/engagement_payments.sql:54`) | `ON CONFLICT (stripe_event_id) DO NOTHING` in `record_event_if_new` (`apps/edge_api/src/document_payments/queries.py:148`); UNIQUE `stripe_event_id` |
| Rank order | `none=0,requires_payment=1,processing=2,succeeded=3` (`apps/edge_api/src/payments/queries.py:78`) | identical ranks (`apps/edge_api/src/document_payments/queries.py:160`/`:164`) |
| Forward-state guard | `%s > {rank_case}` in UPDATE WHERE (`payments/queries.py:107`) | `%(rank)s > {rank_case}` (`document_payments/queries.py:187`) |
| Terminal-protect guard | `failed`/`canceled` apply only WHERE `payment_status <> 'succeeded'` (`payments/queries.py:104`) | same (`document_payments/queries.py:185`) |
| `paid_at` set-once | `paid_at = COALESCE(%s, now())` only when `status=='succeeded'` (`payments/queries.py:97`) | `paid_at = CASE WHEN %(paid)s THEN COALESCE(paid_at, now()) ELSE paid_at END` (`document_payments/queries.py:191`) |

`advance_payment_status` (Lane A, `apps/edge_api/src/payments/queries.py:84`) returns `changed = cur.rowcount == 1` (`:116`); when the succeeded event carries an amount it writes `amount_charged_cents = %s` (`:100`) — note the column is `amount_charged_cents`, NOT `amount_cents`. `advance_status` (Lane B, `apps/edge_api/src/document_payments/queries.py:167`) additionally sets `rail = COALESCE(rail, %(rail)s)` (`:195`) and `stripe_payment_intent_id = COALESCE(%(intent_id)s, stripe_payment_intent_id)` (`:194`).

> **The webhook is the SOLE writer of `'succeeded'`/`paid_at` on both lanes.** Negative claim (re-grepped in the verified dossiers): `business.document_payments` has exactly one UPDATE writer — `advance_status` (`document_payments/queries.py:167`), called only from the webhook at `webhooks_stripe.py:151`; its only INSERT writer is `upsert_intent` (mint), which never writes `'succeeded'`. The `proposals_v1.py` `advance_status` is a **DIFFERENT** module (writes `engagement_proposals` via the proposals path) — do not conflate.

---

## Lane B — Direct-to-documenso document fee

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

> Contrast with **Lane A** (`apps/edge_api/src/payments/stripe_client.py:58`): `payment_method_types=['us_bank_account']` (ACH-only, no card — `:70`), `setup_future_usage='off_session'` (`:71`), nested `payment_method_options={'us_bank_account':{'verification_method':'automatic'}}` (`:72`), `metadata={'ref':ref,'kind':'engagement'}` (`:74`).

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

`_resolve_rail` (`apps/edge_api/src/routers/webhooks_stripe.py:173`):

```
if status == 'processing': return 'us_bank_account'   # :186-187  (call-free; only ACH emits processing)
if status != 'succeeded' or not intent_id: return None # :188
existing = get_payment(conn, document_id)              # :193
if existing and existing.rail: return None             # :194  (already pinned by prior processing — keep it)
return retrieve_settled_rail(intent_id, mode)          # :197  (authoritative charge read, ambiguous succeeded only)
```

`retrieve_settled_rail` (`apps/edge_api/src/document_payments/stripe.py:128`) calls `PaymentIntent.retrieve(intent_id, expand=['latest_charge'])` (`:139`) and reads `latest_charge.payment_method_details.type` (`:147`); returns `None` on any failure (`:141`/`:148`) — caller treats `None` as "leave rail unset". Rail is **cosmetic** (used only to tailor paid-state copy) and never raises into the webhook (set once via `COALESCE`, `document_payments/queries.py:195`).

### Lane B tables

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

## Lane A — Legacy engagement payment (proposal-ref, ACH-only)

**ACTIVE, not deprecated.** Registered in `main.py` (`apps/edge_api/main.py:256`) and wired end-to-end: SPA `/p/:ref/pay` (`PaymentPage`, route at `rare-structure-hq:apps/platform-app/src/App.tsx:107`) → BFF `/api/v1/proposals/:ref/payment-intent` & `/payment` (`rare-structure-hq:apps/platform-api/src/routes/proposals-admin.ts:266`/`:288`) → `edge_api payments_v1.py`.

Two PUBLIC routes under `prefix="/api/v1/proposals"` (the `ref` is the bearer capability): `POST /{ref}/payment-intent` (`apps/edge_api/src/routers/payments_v1.py:36`) and `GET /{ref}/payment` (`:111`).

**Signed gate.** `_PAYABLE_STATUSES = {'signed','completed'}` (`payments_v1.py:31`); `if proposal.status not in _PAYABLE_STATUSES: 409 'agreement not yet signed'` (`:46`–`:47`).

**Amount resolution.** `resolve_charge_cents(proposal)` returns `int(p.quarterly_total_cents)` (`apps/edge_api/src/payments/amount.py:18`/`:20`); `if charge_cents <= 0: 409 'proposal has no payable amount'` (`payments_v1.py:50`–`:51`).

**Mint.** ACH-only (`stripe_client.py:70`); idempotent on `idempotency_key=f'ach_{ref}'` (`payments_v1.py:89`; idempotent-create docstring `stripe_client.py:61`). On mint it attaches the intent (`attach_payment_intent`, `status='requires_payment'`, `payments_v1.py:95`/`:97`) and appends a system `payment_intent.created` event to `business.engagement_events` (`source='system'`, `idempotency_key=created intent id`, `:99`–`:101`). `attach_payment_intent` never overwrites a row already `'succeeded'` (`WHERE ref=%s AND payment_status <> 'succeeded'`, `apps/edge_api/src/payments/queries.py:70`) and COALESCEs `stripe_customer_id`/`payment_initiated_at` set-once (`:63`/`:68`).

**Reuse.** An existing open intent (status not in `failed`/`canceled`) is retrieved and amount re-synced only when status in `_AMOUNT_MUTABLE = {'requires_payment_method','requires_confirmation','requires_action'}` (`payments_v1.py:33`/`:59`/`:62`); reuse failures degrade to a fresh mint, never a 500 (`:73`/`:78`).

**Stripe key resolution (asymmetric vs Lane B).** Lane A uses the **env-driven** getters `config.stripe_publishable_key()` (`payments_v1.py:38`) / `config.stripe_secret_key()` (`:39`, def `config.py:72`), selected by env `STRIPE_MODE` — **NOT** the per-request operator-selectable `resolve_stripe_mode` used by Lane B. `503 'Stripe is not configured'` when either key is absent (`payments_v1.py:40`).

**Lane A state lives on `engagement_proposals`** as ALTER-added columns (no separate payments table); the audit/idempotency ledger is `business.engagement_events`.

---

## Cross-repo handoffs (SPA → BFF → edge_api)

Architecture invariant: `platform-app` → `platform-api` (DUMB BFF, no business logic / no DB for these flows) → `edge_api`. The BFF only remaps field names (snake → camel) and translates error codes.

**Lane B MINT.** `createDocumentPaymentIntent` (`rare-structure-hq:apps/platform-app/src/proposals/api.ts:307`, fetch `:312`) → `POST` BFF `/api/v1/documenso/sign/:opportunityId/:documentId/payment-intent` (`rare-structure-hq:apps/platform-api/src/routes/documenso-public.ts:90`, **no `requireUser`** — pair is the capability) → `edgeCreateDocumentPaymentIntent` (`rare-structure-hq:apps/platform-api/src/lib/edge.ts:595`, fetch `:600`) → `edge_api POST /api/v1/documenso/payment-intent/{opp}/{doc}`. The BFF camelCases to `clientSecret`/`publishableKey`/`amountCents`/`currency`/`paymentStatus`/`contactName`/`contactEmail` (`documenso-public.ts:97`–`:103`) and **propagates the edge HTTP status verbatim** (`const status = e.status ?? 502` `:109`; re-throw `:110`; `.status` set on `EdgeError` at `edge.ts:606`) so a `409` reaches the SPA as "sign first".

**Lane B STATE POLL.** `getDocumentPaymentState` (`rare-structure-hq:apps/platform-app/src/proposals/api.ts:334`, fetch `:339`) → `GET` BFF `/api/v1/documenso/sign/:opportunityId/:documentId/payment` (`rare-structure-hq:apps/platform-api/src/routes/documenso-public.ts:118`) → `edgeGetDocumentPaymentState` (`rare-structure-hq:apps/platform-api/src/lib/edge.ts:622`, fetch `:627`) → `edge_api GET /api/v1/documenso/payment/{opp}/{doc}`. CamelCases to `paymentStatus`/`amountCents`/`currency`/`paidAt`/`rail` (`documenso-public.ts:125`–`:129`); this state-poll route maps `EdgeError → 502` (`:134`), NOT verbatim.

**BFF dual mount.** `documensoPublicRoutes` is mounted at `/api/v1/documenso` (`rare-structure-hq:apps/platform-api/src/index.ts:123`) AND the legacy transitional alias `/api/v1/engagement-mandate-drafts` (`:124`) so an in-flight SPA bundle calling the old path keeps working across independent deploys. A SEPARATE operator-authed router (`engagementMandateDraftRoutes`) is also mounted at the legacy prefix (`index.ts:126`) — the alias and the real legacy router coexist; do not conflate.

**SPA pay page.** `/p/m/:opportunityId/:documentId/pay` (`rare-structure-hq:apps/platform-app/src/routes/p/DocumentPaymentPage.tsx:2`). ACH settles asynchronously: `confirmPayment` returns `processing`, NOT `succeeded` (`DocumentPaymentPage.tsx:14`); the page polls `getDocumentPaymentState` every **5000ms** (`:97`, `startPolling` `:66`) until terminal. **Server truth only advances the page; the browser confirm result never sets paid** (`:65`). SPA error branching: `if e instanceof PaymentError && e.status===409` (`:133`); `/already paid/i → succeeded` else `unsigned` (`:136`); any other error → `unavailable` (`:138`). The confirm-result `SettledHint` maps `succeeded → rail 'card'` / `processing → rail 'us_bank_account'` as an instant client-side hint (`rare-structure-hq:apps/platform-app/src/proposals/StagedAchForm.tsx:494`/`:498`), refined by the server poll; `enableCard` is passed (`rare-structure-hq:apps/platform-app/src/proposals/DocumentPaymentForm.tsx:54`) so Stripe renders the "Card | US bank account" tabs.

**PAID transition (both lanes).** Stripe → `edge_api POST /webhooks/stripe` (`webhooks_stripe.py:49`) is the only thing that flips `'succeeded'`/`paid_at`. The SPA learns it only via its state poll.

---

## Schema-drift seatbelt & boot-time apply

`_payments_db()` (`apps/edge_api/src/routers/document_payments_v1.py:50`) wraps every Lane-B DB read/write so any of `UndefinedColumn`/`UndefinedTable`/`UndefinedFunction`/`InvalidSchemaName` (`_SCHEMA_DRIFT_ERRORS`, `:42`–`:47`) is converted to **HTTP 503 `'payment is temporarily unavailable'`** instead of a raw 500 (`:58`→`:63`). This is the seatbelt for the PR #518 rail-column outage (comment `:36`–`:41`).

The primary fix is the startup apply: `run_migrations` (`apps/edge_api/src/migrate.py:64`) re-applies every `sql/*.sql` (sorted glob, `:61`) to `HQX_DB_URL_POOLED` before serving, each in its own transaction (`:88`) guarded by a transaction-scoped `pg_advisory_xact_lock` (`:90`) to serialize concurrent replica boots. Escape hatch: `EDGE_API_SKIP_DB_MIGRATE=1` (`apps/edge_api/src/config.py:221`).

---

## Environment variables

| Var | Role | Cite |
|---|---|---|
| `STRIPE_SECRET_KEY` / `STRIPE_SECRET_KEY_{LIVE,TEST}` | Stripe secret (sk_), server-side only; Lane B by explicit mode, Lane A bare | `apps/edge_api/src/config.py:112`/`:107` |
| `STRIPE_PUBLISHABLE_KEY_{LIVE,TEST}` | publishable (pk_), surfaced to browser in the Init response | `apps/edge_api/src/config.py:117` |
| `STRIPE_WEBHOOK_SECRET_{LIVE,TEST}` + bare | webhook signing secrets; ALL tried by `construct_event_any` | `apps/edge_api/src/config.py:129`/`:130` |
| `STRIPE_MODE` / `STRIPE_MODE_{TEST,LIVE}` | default mode (defaults `'test'`) + indirection target for `operator_settings.stripe_mode` | `apps/edge_api/src/config.py:62`/`:103` |
| `EDGE_API_SKIP_DB_MIGRATE` | skip the boot-time `sql/*.sql` apply | `apps/edge_api/src/config.py:221` |

---

## Status: ACTIVE / CONDITIONAL / DEPRECATED / STUB

| Component | Status | Note |
|---|---|---|
| `POST /webhooks/stripe` (shared router) | **ACTIVE** | sole writer of `'succeeded'`/`paid_at` on both lanes (`webhooks_stripe.py:49`) |
| Lane B `POST /api/v1/documenso/payment-intent/{opp}/{doc}` | **ACTIVE** | dual-rail mint (`document_payments_v1.py:84`) |
| Lane B `GET /api/v1/documenso/payment/{opp}/{doc}` | **ACTIVE** | state poll (`document_payments_v1.py:228`) |
| Lane B `_handle_document_payment` | **CONDITIONAL** | runs only when `metadata.kind=='document'` (`webhooks_stripe.py:73`) |
| Lane B dual-rail mint `create_payment_intent` | **ACTIVE** | `['card','us_bank_account']` (`stripe.py:87`) |
| Lane B `_resolve_rail` / `retrieve_settled_rail` | **ACTIVE** | rail cosmetic, set-once (`webhooks_stripe.py:173`, `stripe.py:128`) |
| Lane B signed gate `read_sign_state` | **ACTIVE** | offline `DOCUMENT_COMPLETED` projection (`documenso_webhooks/queries.py:63`) |
| Lane A `POST /api/v1/proposals/{ref}/payment-intent` | **ACTIVE** | ACH-only (`payments_v1.py:36`, `stripe_client.py:70`) |
| Lane A `GET /api/v1/proposals/{ref}/payment` | **ACTIVE** | state poll (`payments_v1.py:111`) |
| Lane A inline proposal-ref webhook path | **CONDITIONAL** | runs only when `metadata.kind != 'document'` (`webhooks_stripe.py:76`) |
| `construct_event_any` (verify-any-secret) | **ACTIVE** | test+live+bare (`stripe.py:172`) |
| `_payments_db` schema-drift 503 seatbelt | **ACTIVE** | (`document_payments_v1.py:50`) |
| `run_migrations` boot-time `sql/*.sql` apply | **ACTIVE** | advisory-locked (`migrate.py:64`) |
| BFF legacy alias `/api/v1/engagement-mandate-drafts` | **CONDITIONAL** | transitional, for in-flight bundles (`rare-structure-hq:apps/platform-api/src/index.ts:124`) |
| Post-payment fulfillment seam (Trigger.dev) — both lanes | **STUB** | only a `logger.info`; nothing fires (`webhooks_stripe.py:161`–`:166` Lane B, `:106`–`:109` Lane A) |
| `DIRECT_TO_DOCUMENSO_PAYMENT_E2E.md` reference doc | **DEPRECATED** (as ground truth) | states ACH-only; contradicted by dual-rail code at `stripe.py:87` |

---

## Traps

- **STALE "ACH-only" comments — code is DUAL-RAIL.** The Lane B router module docstring (`document_payments_v1.py:3` "mint/reuse ACH intent"), the SQL header (`apps/edge_api/sql/document_payments.sql:1` "Stripe ACH"), the `currency` column comment ("ACH (us_bank_account) is USD-only", `document_payments.sql:20`), and the `webhooks_stripe.py` module docstring (describes the surface as "ACH"/"engagement ACH debits") all **predate** the dual-rail mint. The behavior is `['card','us_bank_account']` (`stripe.py:87`). Only the prose lags. **The CODE wins.**
- **The E2E reference doc is out of date on the rail.** `docs/reference/DIRECT_TO_DOCUMENSO_PAYMENT_E2E.md` describes Lane B as ACH-only; that is wrong vs current code. Treat it as a historical starting point, not ground truth.
- **Two `advance_status`-family functions — do not conflate.** `document_payments/queries.py:advance_status` writes `business.document_payments` (Lane B, webhook-only). Lane A's webhook advance is `payments/queries.py:advance_payment_status` (`:84`). The `proposals_v1.py`/`proposals.queries` `advance_status` is yet another function writing `business.engagement_proposals` via the proposals module.
- **Two lanes, two amount resolvers, two key-resolution paths.** Lane B `resolve_fee_cents` parses a display string from `field_values['fee_amount']` (`amount.py:19`); Lane A `resolve_charge_cents` returns `quarterly_total_cents` (`payments/amount.py:18`/`:20`). Lane B keys are mode-selected via `resolve_stripe_mode` (operator-toggleable); Lane A keys are bare env via `config.stripe_secret_key()`. Do not assume one path covers both.
- **Lane A displayed-vs-charged divergence (live, code-confirmed).** The legacy mint charges `quarterly_total_cents`, which the proposals router populates with the FULL engagement total `= total_cents(monthly_fee_cents, duration_months)` (`apps/edge_api/src/proposals/models.py:50`). Meanwhile the public projection's `amount_due` is computed by **billing cadence** via `charge_cents` (`models.py:53`: `monthly`→1mo `:59`, `quarterly`→3mo `:61`, else full `:62`). For a `monthly` or `quarterly` cadence proposal, displayed `amount_due` and the actually-charged amount **diverge**. `DEFAULT_BILLING_CADENCE='upfront_in_full'`, so for default proposals charge == full total and there is no gap; the gap is real ONLY for `billing_cadence ∈ {monthly, quarterly}`. The `payments/amount.py` docstring asserting "charge == signed amount by construction" is stale for those cadences.
- **`amount_charged_cents`, not `amount_cents`, on the legacy advance.** `advance_payment_status` writes `amount_charged_cents = %s` (`payments/queries.py:100`) when the succeeded event carries an amount — distinct from the Lane-B `amount_cents` column.
- **`processing` is NOT `paid`.** ACH settles asynchronously; `confirmPayment` returns `processing`. Only a `payment_intent.succeeded` webhook flips `'succeeded'`/`paid_at`. The browser confirm result never advances the page (`DocumentPaymentPage.tsx:65`).
- **The state poll returns `'none'` on a pair mismatch, not an error** (`document_payments_v1.py:239`–`:240`) — a `'none'` poll result does not necessarily mean "never minted"; it can mean the document id does not belong to the opportunity. The mint, by contrast, propagates a 409 verbatim through the BFF.
- **`metadata.kind` is the ONLY dispatch discriminant** (`webhooks_stripe.py:73`). An intent minted without `kind='document'` is routed to the legacy proposal-ref path. Lane B mints always set it (`stripe.py:92`); Lane A mints set `kind='engagement'` (`stripe_client.py:74`) which falls through to the legacy path keyed by the `ref`.
- **`rail` is cosmetic.** It tailors paid-state copy only; it is stamped once via `COALESCE` and `retrieve_settled_rail` returning `None` is benign. Never gate logic on it.
- **Fulfillment is a STUB on BOTH lanes.** No provision/receipt/CRM action fires on payment success — only a log line marks the Trigger.dev seam (`webhooks_stripe.py:161`–`:166` and `:106`–`:109`). The `document_payment_events` / `engagement_events` rows are the audit of record.
- **Upstream DDL not in this repo.** `business.opportunities`, `business.opportunity_specific_content`, `business.contacts` are upstream-owned (no `CREATE TABLE` under `apps/edge_api/sql`); their column definitions live outside this tree. Lane B joins them read-only.
