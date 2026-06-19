# 04 — Documenso Integration (v2 client, webhook capture, sign reads)

> **STATUS BANNER.** This file is the canonical reference for the Documenso v2 e-signature
> integration in `edge_api`: the single client module (`documenso_client.py`), its three origination
> lanes, the three token-extraction helpers, the raw webhook capture, and the pair-gated sign reads
> (offline `sign-state` + live `sign-token`, including two-recipient client-vs-originator selection).
> It is **lane-spanning**: it documents the shared signing core used by ALL render modes/lanes —
> Lane A `through-docraptor` (proposals), Lane B `/envelope/use` (mandate-draft `/confirm`), and
> Lane C `/template/use` (mandate-draft `/originate-prefilled`) — plus the read/webhook/download side
> common to them. Every non-trivial claim carries a `path:line` citation that was opened and read.

## Orientation

`documenso_client.py` is the ONLY edge_api module that talks to Documenso Cloud v2 (Platform tier)
(`apps/edge_api/src/services/documenso_client.py:1`). edge_api is the single writer of Documenso
state in this platform; the SPA and the platform-api BFF never call Documenso directly — they call
edge_api routes, which call this client. There are **three origination lanes** that all converge on
this module, all distributing with `distributionMethod:NONE` (no Documenso-sent email; the consumer
app delivers the link and embeds the recipient signing token). On the read side there are two PUBLIC
prospect-facing reads behind the `/p/m/{opportunity_id}/{document_id}` signing link: an **offline
poll** (`sign-state`, zero Documenso calls, derived from raw webhook rows) and a **single live read**
(`sign-token`, one `GET /api/v2/document/{id}`, pair-gated). Inbound Documenso webhooks land RAW in
`business.documenso_webhook_events` (the system of record) via a secret-gated capture route. A fresh
agent should treat `payload` jsonb as truth and the scalar columns as best-effort lookup keys only.

> **Do-not-conflate primer.** There is a SEPARATE, parallel Documenso integration at
> `apps/edge_api/src/engagement_docs/documenso.py` — its own client, anchors, and recipient/field
> logic, explicitly NOT this module. It creates a DRAFT `DOCUMENT` from a rendered mandate PDF and
> deliberately does NOT distribute (stays DRAFT, no tokens minted)
> (`apps/edge_api/src/engagement_docs/documenso.py:1-18`). Out of scope here; flagged so an agent
> does not merge the two.

---

## 1. Configuration getters (`config.py`)

| Getter | Env var | Default / behavior | Citation |
|---|---|---|---|
| `documenso_api_key()` | `DOCUMENSO_API_KEY` | format `api_...`, server-side only; `None` when unset | `apps/edge_api/src/config.py:29-31` |
| `documenso_api_url()` | `DOCUMENSO_API_URL` | default `https://app.documenso.com`, trailing slash stripped | `apps/edge_api/src/config.py:34-37` |
| `documenso_webhook_secret()` | `DOCUMENSO_WEBHOOK_SECRET` | `None` when unset; both webhook routes 503 before any verify | `apps/edge_api/src/config.py:40-43` |

`documenso_api_url()` carries a hard invariant in its docstring: **the host passed to the embed MUST
match this value — a doc created here cannot be signed against a different instance**
(`apps/edge_api/src/config.py:34-37`). For that reason `documenso_api_url()` is surfaced to callers
as `documenso_host` in the `/confirm` (`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:105`),
`/originate-prefilled` (`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:165`),
`/document/{envelope_id}` (`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:179`), and
`sign-token` (`apps/edge_api/src/routers/documenso_webhooks_v1.py:157`) responses so the SPA mounts
the embed against the matching instance.

---

## 2. Module primitives (`documenso_client.py`)

### 2.1 Transport, auth, errors

- `_TIMEOUT = httpx.Timeout(60.0, connect=10.0)` — 60s read, 10s connect
  (`apps/edge_api/src/services/documenso_client.py:42`).
- `_auth_value()` reads `config.documenso_api_key()`; raises `DocumensoError` if unset; returns the
  key as-is when it starts with `api_`, otherwise prepends `api_` (tolerates a key stored without the
  prefix) (`apps/edge_api/src/services/documenso_client.py:74-79`).
- `_client()` builds an `httpx.AsyncClient` with `base_url=config.documenso_api_url()`,
  header `Authorization=_auth_value()`, and `timeout=_TIMEOUT`
  (`apps/edge_api/src/services/documenso_client.py:82-87`).
- `_raise_for_status(resp, op)` treats any status whose hundreds digit is not 2 (`status_code // 100
  != 2`) as failure: logs and raises `DocumensoError` with the op name, status, and first 500 chars
  of the response text (`apps/edge_api/src/services/documenso_client.py:90-94`).
- `DocumensoError(RuntimeError)` is raised for any non-2xx response or an unconfigured client
  (`apps/edge_api/src/services/documenso_client.py:55-56`, raised at
  `apps/edge_api/src/services/documenso_client.py:76-77` and `90-94`).
- `_dig(obj, *keys)` returns the first present, non-`None` key from a dict — defensive across v2
  response-shape variants (`apps/edge_api/src/services/documenso_client.py:97-103`). NOTE: the
  webhook router defines its OWN `_dig` (`apps/edge_api/src/routers/documenso_webhooks_v1.py:30`);
  functionally equivalent (first present non-null key), just a different module copy.

### 2.2 Dataclasses (all `@dataclass(frozen=True)`)

| Dataclass | Fields | Citation |
|---|---|---|
| `EnvelopeResult` | `envelope_id:str`, `document_id:int\|None` (numeric secondary id, for signed-PDF download), `client_token:str\|None` | `apps/edge_api/src/services/documenso_client.py:59-63` |
| `NormalizedEvent` | `event:str`, `status:str\|None` (mapped internal status, `None` for unknown event), `envelope_id:str\|None`, `external_id:str\|None` | `apps/edge_api/src/services/documenso_client.py:66-71` |
| `DocumentReadResult` | `document_id:int`, `envelope_id:str\|None` (prefixed `envelope_…` handle), `external_id:str\|None`, `status:str\|None`, `signing_token:str\|None` (first SIGNER / fallback), `recipient_tokens:tuple[tuple[str,str],...]=()` | `apps/edge_api/src/services/documenso_client.py:605-617` |

### 2.3 Event-name and field-meta enums

`_EVENT_TO_STATUS` maps six Documenso enum event names to internal statuses
(`apps/edge_api/src/services/documenso_client.py:45-52`):

| Documenso event (verbatim) | Internal status |
|---|---|
| `DOCUMENT_SENT` | `sent` |
| `DOCUMENT_OPENED` | `opened` |
| `DOCUMENT_SIGNED` | `signed` |
| `DOCUMENT_COMPLETED` | `completed` |
| `DOCUMENT_REJECTED` | `rejected` |
| `DOCUMENT_CANCELLED` | `voided` |

`_DEFAULT_META_KEY` maps fillable field types to their `fieldMeta` default key
(`apps/edge_api/src/services/documenso_client.py:675-677`):

| Field type | fieldMeta key |
|---|---|
| `TEXT` | `text` |
| `NUMBER` | `value` |
| `DROPDOWN` | `defaultValue` |

`SIGNATURE`/`DATE` appear in NEITHER enum — they carry no default to set, and no internal status row
is keyed off them in the offline sign-state (only `DOCUMENT_COMPLETED` is terminal; see §6).

### 2.4 Field-size overrides and anchors (Lane A only)

- `_SIGNATURE_FIELD_SIZE = {"width": 32.0, "height": 7.0}` and `_DATE_FIELD_SIZE = {"width": 22.0,
  "height": 4.0}` are field SIZE overrides (percent of page). **Position is NOT set here** — Documenso
  resolves it from the anchor marker via `findText`. Per the inline comment, `width`/`height` are the
  only `ZPlaceholderPositionSchema` fields beyond `placeholder`
  (`apps/edge_api/src/services/documenso_client.py:167-174`).
- `CLIENT_SIGNATURE_ANCHOR` = `'[[CLIENT_SIGNATURE]]'`
  (`apps/edge_api/src/proposals/signing_anchors.py:21`) and `CLIENT_DATE_ANCHOR` = `'[[CLIENT_DATE]]'`
  (`apps/edge_api/src/proposals/signing_anchors.py:24`), imported at
  `apps/edge_api/src/services/documenso_client.py:38`. Used as `findText` placeholder strings in Lane A.

### 2.5 Small helpers

- `_numeric_document_id(env)` parses the legacy numeric document id from `secondaryId` (format
  `document_<n>`) by regex, returning `int` or `None`
  (`apps/edge_api/src/services/documenso_client.py:177-180`).
- `_prefill_value_for_label(label, values)` resolves the prefill value exact-key first, else falls
  back to the BASE name (`label.rsplit('_', 1)[0]`) so a split label like
  `participant_company_one`/`_two` draws from a single `participant_company` value; empty/None yields
  `None` (field stays open) (`apps/edge_api/src/services/documenso_client.py:183-195`).
- `_field_default_value(field)` returns the field's current baked default per its type via
  `_DEFAULT_META_KEY`, or `None` when unset/empty
  (`apps/edge_api/src/services/documenso_client.py:680-686`).

---

## 3. The three token-extraction helpers (WHEN each is used)

This is the most error-prone area for an agent. Three helpers extract recipient tokens from a
Documenso response body; they differ by **selection strategy** and are deliberately not
interchangeable.

| Helper | Selection strategy | Returns | Used by |
|---|---|---|---|
| `_extract_client_token(body, email)` | EMAIL-MATCHED: descends `body` via `envelope`/`document`/`data`, reads `recipients` (key `recipients` or `Recipient`), returns the token of the recipient whose lowercased email == supplied email; falls back to the FIRST recipient (`chosen = chosen or r`) for single-signer envelopes | first matching/fallback `token` (or `signingToken`) | Lane A (`create_signing_envelope`, by `signer_email`), Lane C (`create_document_from_template`, by `recipient_email`), `client_token()` re-read |
| `_extract_signer_token(body)` | ROLE/FIRST: no caller email; selects the recipient whose role upper-cased == `'SIGNER'`, else the first recipient (`recips[0]`) | first SIGNER / first recipient `token` | Lane B (`create_document_from_template_with_custom_pdf` read-back), `read_template_document`, `DocumentReadResult.signing_token` in `read_document` |
| `_recipient_email_tokens(body)` | ALL-PAIRS: returns a tuple of `(email_lowercased, token)` for EVERY recipient that actually carries a token; blank-email recipients keep an empty-string key | `tuple[tuple[str,str],...]` | `read_document` → `DocumentReadResult.recipient_tokens`; the `sign-token` route iterates this to pick client vs originator |

Citations: `_extract_client_token` `apps/edge_api/src/services/documenso_client.py:110-126`;
`_extract_signer_token` `apps/edge_api/src/services/documenso_client.py:129-146`;
`_recipient_email_tokens` `apps/edge_api/src/services/documenso_client.py:149-164`.

> **Why `_extract_signer_token` exists separately:** a document instantiated from a template via
> `/envelope/use` (recipients omitted) carries the template's own recipients, whose email may be
> blank — so there is no caller-supplied email to match on; select by role
> (`apps/edge_api/src/services/documenso_client.py:130-136`).

> **Why Lane C uses `_extract_client_token` not `_extract_signer_token`:** the originator may be a
> SECOND recipient, so "first SIGNER" is ambiguous; Lane C binds the prospect by the email it just
> set and matches on it (`apps/edge_api/src/services/documenso_client.py:545-547`).

---

## 4. The three origination lanes

All three POST `/api/v2/envelope/distribute` with `meta.distributionMethod:NONE` (DRAFT → PENDING,
no email, signing token minted).

### 4.1 Lane A — `create_signing_envelope` (`through-docraptor`, proposals)

`create_signing_envelope(pdf_bytes, *, title, signer_name, signer_email, external_id=None) ->
EnvelopeResult` (`apps/edge_api/src/services/documenso_client.py:198-281`).

```
1. POST /api/v2/envelope/create   (multipart: data 'payload' JSON + files 'files')
     payload: type=DOCUMENT, recipients=[{name,email,role:SIGNER}], distributeDocument:false
2. GET  /api/v2/envelope/{envelope_id}
     -> _extract_client_token(env, signer_email)  (email-matched)
     -> recipient_id, _numeric_document_id(env)
3. POST /api/v2/envelope/field/create-many
     SIGNATURE @ CLIENT_SIGNATURE_ANCHOR (+_SIGNATURE_FIELD_SIZE)
     DATE      @ CLIENT_DATE_ANCHOR      (+_DATE_FIELD_SIZE)   [position via findText]
4. POST /api/v2/envelope/distribute   meta.distributionMethod:NONE
-> EnvelopeResult(envelope_id, document_id, client_token)
```

**Caller:** the proposals provision path (`apps/edge_api/src/routers/proposals_v1.py:99-110`), which
renders a DocRaptor PDF then attaches the envelope, stamping `external_id = p.ref` (the proposal ref;
line 108). This is the ONLY caller, and it sits in the `through-docraptor` branch — the
`direct-to-documenso` render_mode branch is a not-yet-wired STUB that returns before this call
(`apps/edge_api/src/routers/proposals_v1.py:99-110`,
`apps/edge_api/src/services/documenso_client.py:198-201`).

### 4.2 Lane B — `create_document_from_template_with_custom_pdf` (`/envelope/use`, mandate-draft `/confirm`)

`create_document_from_template_with_custom_pdf(documenso_template_id, *, external_id=None,
recipients=None, prefill_values=None) -> EnvelopeResult`
(`apps/edge_api/src/services/documenso_client.py:321-403`).

```
1. _resolve_template_envelope_id(...) -> payload['envelopeId']
1b. if prefill_values:
      GET /api/v2/envelope/{envelopeId}
      build label -> (id, lowercased type) from fields[].fieldMeta.label
      payload['prefillFields'] = [{id, type(lowercased), value}, ...]   (skip unmatched/empty)
2. POST /api/v2/envelope/use   (multipart: files 'payload' = JSON string, NO files part)
     distributeDocument:false; recipients OPTIONAL (override)
3. POST /api/v2/envelope/distribute   meta.distributionMethod:NONE
4. GET  /api/v2/envelope/{envelope_id}
     -> _extract_signer_token(env)  (role/first, NO email)
     -> _numeric_document_id(env)
-> EnvelopeResult(envelope_id, document_id, client_token=signer token)
```

- prefillFields are keyed by FIELD **LABEL** (resolved against the new envelope's fields via
  `fieldMeta.label`), field type lowercased; unmatched/empty labels skipped
  (`apps/edge_api/src/services/documenso_client.py:362-375`).
- `recipients` is an optional override parameter; **the only caller (`/confirm`) does NOT pass it** —
  the `payload['recipients']` branch is present-but-unexercised in the live flow
  (`apps/edge_api/src/services/documenso_client.py:350-351`,
  `apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:96-99`).

**Caller:** `POST /api/v1/engagement-mandate-drafts/{draft_id}/confirm` (service-token gated), with
`external_id=draft_id` and `prefill_values` from `queries.get_staged_prefill_values(opportunity_id)`;
returns `envelope_id` + signer token to the prospect via `MandateDraftConfirmed`
(`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:82-106`).

### 4.3 Lane C — `create_document_from_template` (`/template/use`, mandate-draft `/originate-prefilled`)

`create_document_from_template(documenso_template_id, *, recipient_email, recipient_name,
field_values_by_label=None, external_id=None, title=None) -> EnvelopeResult`
(`apps/edge_api/src/services/documenso_client.py:427-594`).

```
1. GET /api/v2/template/{id}  -> fields[] + recipients[]
   bind prospect to the PLACEHOLDER recipient (first with empty email; else SIGNER; else first)
2. build prefillFields by fanning EACH label to EVERY matching field id
   (type lowercased; value via _prefill_value_for_label; value always a STRING)
3. POST /api/v2/template/use
     templateId(int), recipients=[{id, email, name}] (REQUIRED), distributeDocument:false
     optional override.title (capped 255)
   -> body carries envelopeId (prefixed), id (numeric), recipients[].token
   -> token = _extract_client_token(body, recipient_email)   (NOT first-SIGNER)
4. GET  /api/v2/envelope/{envelope_id}
   POST /api/v2/envelope/field/update-many  readOnly:true on derived prefilled TEXT/NUMBER fields
     (identified by non-empty value — derived fields have NEW ids and NO labels)
5. POST /api/v2/envelope/distribute   meta.distributionMethod:NONE
-> EnvelopeResult(envelope_id, document_id=numeric body.id, client_token)
```

- prefillFields are keyed by FIELD **ID** (not label), type lowercased, value always a STRING; a
  label can map to MULTIPLE field ids (fan-out, one prefill entry per id); SIGNATURE/DATE and labels
  not in `field_values` are skipped (`apps/edge_api/src/services/documenso_client.py:505-512`).
- readOnly lock is applied on the DERIVED document (not the template): a template field can't be
  readOnly without static text, and the prefilled value satisfies Documenso's "read-only must have
  text" rule (`apps/edge_api/src/services/documenso_client.py:549-573`).

**Caller:** `POST /api/v1/engagement-mandate-drafts/{draft_id}/originate-prefilled` (service-token
gated), with recipient email/name and `field_values` from
`queries.get_opportunity_prefill_and_contact`, `external_id = prefill['opportunity_ref']` (the
opportunity's PUBLIC 8-char handle), and `title='Engagement Agreement'`. It returns `opportunity_id`
(the 8-char handle) + `document_id` so the SPA builds `/p/m/{opportunity_id}/{document_id}`
(`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:109-166`; `external_id=opportunity_ref`
at line 150; `title` at 153; handle is explicitly NOT the row UUID per 138-141).

### 4.4 `/envelope/use` vs `/template/use` (lane B vs lane C) — key contract differences

| Aspect | Lane B `/envelope/use` | Lane C `/template/use` |
|---|---|---|
| `recipients` | OPTIONAL (only `envelopeId` required) | REQUIRED (`[{id, email, name}]`) |
| prefill mapping (BOTH emit id-keyed `prefillFields`) | source label → **1** field id (1:1, no fallback) | template label → **N** field ids (fan-out + base-name fallback) |
| token read-back | separate `GET /api/v2/envelope/{id}` then `_extract_signer_token` | straight off the `/template/use` body via `_extract_client_token` |
| readOnly lock | none | yes, on derived doc |
| envelope id input | resolved via `_resolve_template_envelope_id` | `templateId` int passed directly |

Citations: lane B `apps/edge_api/src/services/documenso_client.py:321-403`; lane C
`apps/edge_api/src/services/documenso_client.py:427-594`.

### 4.5 `_resolve_template_envelope_id`

`_resolve_template_envelope_id(client, documenso_template_id) -> str` resolves a numeric Documenso
template id to its prefixed envelope id via `GET /api/v2/template/{id}` reading `.envelopeId`
(fallback `.id`). Required because the numeric id 400s on envelope endpoints, and
`business.documenso_templates` stores only the numeric template id (as text)
(`apps/edge_api/src/services/documenso_client.py:303-318`;
`apps/edge_api/sql/engagement_mandate_draft_content.sql:47-50`).

---

## 5. Read side

### 5.1 `get_envelope` / `client_token`

- `get_envelope(envelope_id) -> dict` makes `GET /api/v2/envelope/{envelope_id}` and returns parsed
  JSON (`apps/edge_api/src/services/documenso_client.py:284-288`).
- `client_token(envelope_id, signer_email) -> str|None` (re)reads the email-matched client token from
  the live envelope via `get_envelope` + `_extract_client_token`
  (`apps/edge_api/src/services/documenso_client.py:291-293`). **No edge_api router caller located** in
  verification — it is a module-surface helper; whether any other module invokes it is unconfirmed.

### 5.2 `read_template_document` (prospect mandate-draft read)

`read_template_document(envelope_id) -> tuple[str|None, str|None]` returns `(_extract_signer_token,
status)` from `get_envelope` — the prefixed envelope id is the capability, no per-recipient email
needed (`apps/edge_api/src/services/documenso_client.py:597-602`). Called by PUBLIC
`GET /api/v1/engagement-mandate-drafts/document/{envelope_id}`, returning signer token + status +
`documenso_host` (`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:169-181`).

### 5.3 `read_document` (sign-token lane)

`read_document(document_id: str) -> DocumentReadResult` makes `GET /api/v2/document/{document_id}`
(NUMERIC id; per the inline note the prefixed `envelope_` handle 400s there, verified 2026-06-17) and
returns `document_id` (resolved from `body.id` else supplied id), `envelope_id`, `external_id`,
`status`, `signing_token` (`_extract_signer_token`), `recipient_tokens` (`_recipient_email_tokens`)
(`apps/edge_api/src/services/documenso_client.py:620-649`). The live `GET` is at
`apps/edge_api/src/services/documenso_client.py:631`; the 400-comment at
`apps/edge_api/src/services/documenso_client.py:626`.

### 5.4 `download_signed_pdf`

`download_signed_pdf(envelope_id) -> bytes` resolves the numeric document id from
`GET /api/v2/envelope/{id}`, then `GET /api/v2/document/{document_id}/download?version=signed`. If
content-type is `application/pdf` or bytes start with `%PDF-` it returns the body; otherwise it reads
`downloadUrl`/`url` from JSON and fetches it with a **BARE httpx client (no Authorization header)** so
the Documenso API key never rides to a third-party host (S3/R2/CDN)
(`apps/edge_api/src/services/documenso_client.py:762-788`). Called by PUBLIC
`GET /api/v1/proposals/{ref}/document` only when `p.status=='completed'` and
`p.documenso_envelope_id` exists (otherwise 409 before the call)
(`apps/edge_api/src/routers/proposals_v1.py:321-334`).

### 5.5 Settings (template defaults editor)

- `get_template_text_field_labels(documenso_template_id) -> list[str]` resolves the envelope id then
  `GET /api/v2/envelope/{id}`, returning each TEXT field's `fieldMeta.label` in field order,
  de-duplicated (SIGNATURE/DATE excluded by the `type != 'TEXT'` continue)
  (`apps/edge_api/src/services/documenso_client.py:652-672`). Called by
  `GET /api/v1/engagement-mappings` to fill each option's `text_fields` live (per-request,
  concurrently via `asyncio.gather`, swallowing `DocumensoError` to keep the stored fallback)
  (`apps/edge_api/src/routers/engagement_mappings_v1.py:34-41`).
- `get_template_fields(documenso_template_id) -> list[dict]` resolves envelope id then
  `GET /api/v2/envelope/{id}`, returning editable fields (type in `_DEFAULT_META_KEY`) as
  `{id, type, label, recipient_id, page, default}`; SIGNATURE/DATE excluded
  (`apps/edge_api/src/services/documenso_client.py:689-712`).
- `set_template_field_defaults(documenso_template_id, defaults: dict[int,str]) -> int` writes default
  values onto the TEMPLATE's fields: resolves envelope id, `GET /api/v2/envelope/{id}`, indexes fields
  by id, writes each value into the right `fieldMeta` key per type (MERGING into existing meta), then
  `POST /api/v2/envelope/field/update-many` sending the FULL field
  (`id, type, recipientId, page, positionX, positionY, width, height, fieldMeta`); returns count
  written (0 if no data) (`apps/edge_api/src/services/documenso_client.py:715-759`).
- These two are the read/write of the documenso-template-fields router:
  `GET /api/v1/documenso-template-fields` lists fields+defaults;
  `POST /api/v1/documenso-template-fields/defaults` writes the `{id:value}` mapping then re-reads
  (`apps/edge_api/src/routers/documenso_template_fields_v1.py:40-59`).

---

## 6. Webhook capture (`documenso_webhooks_v1.py` + `documenso_webhooks/queries.py`)

### 6.1 Router and routes

Router prefix `/api/v1/documenso`, tags `documenso-webhooks`
(`apps/edge_api/src/routers/documenso_webhooks_v1.py:27`). Three routes:

| Route | Method | Auth | Citation |
|---|---|---|---|
| `/api/v1/documenso/webhook` | POST | secret-gated (`X-Documenso-Secret`) | `apps/edge_api/src/routers/documenso_webhooks_v1.py:39` |
| `/api/v1/documenso/sign-state/{opportunity_id}/{document_id}` | GET | PUBLIC (no dependency) | `apps/edge_api/src/routers/documenso_webhooks_v1.py:76` |
| `/api/v1/documenso/sign-token/{opportunity_id}/{document_id}` | GET | PUBLIC (no dependency) | `apps/edge_api/src/routers/documenso_webhooks_v1.py:105` |

### 6.2 RAW capture — `POST /api/v1/documenso/webhook`

```
if config.documenso_webhook_secret() is None: 503     [line 44-45]
if not verify_webhook_secret(x_documenso_secret): 401  [line 46-47]
body = await request.json()  (non-JSON -> 400)          [line 49-52]
raw = body if isinstance(body, dict) else {"_raw": body} [line 54]
event       = _dig(raw, "event")                                              [line 59]
inner       = _dig(raw, "payload", "data") or {}                             [line 60]
envelope_id = _dig(inner, "id","documentId","envelopeId") or _dig(raw,"id","envelopeId") [line 61]
external_id = _dig(inner, "externalId") or _dig(raw, "externalId")            [line 62]
insert_event(conn, event, envelope_id, external_id, payload=raw)             [line 65-71]
return {"ok": True, "id": event_id}
```

The route does **no filtering, no normalization, no projection** — the full `raw` body is the system
of record; the three scalars are best-effort extracts only
(`apps/edge_api/src/routers/documenso_webhooks_v1.py:1-7`, `39-73`). It does **NOT** call
`normalize_event` (contrast the legacy proposals webhook, §8).

`verify_webhook_secret(provided) -> bool` does a constant-time `hmac.compare_digest` of the inbound
`X-Documenso-Secret` against `config.documenso_webhook_secret()`; returns `False` when the secret is
unconfigured (route must refuse) (`apps/edge_api/src/services/documenso_client.py:791-800`).

### 6.3 `insert_event` and the table

`insert_event(conn, *, event, envelope_id, external_id, payload) -> str` runs
`INSERT INTO business.documenso_webhook_events (event, envelope_id, external_id, payload) VALUES
(%s,%s,%s,%s::jsonb) RETURNING id::text`, then `conn.commit()`. Append-only, no `ON CONFLICT`
(no dedup) (`apps/edge_api/src/documenso_webhooks/queries.py:24`, `26`, `31`).

`business.documenso_webhook_events` columns
(`apps/edge_api/sql/documenso_webhook_events.sql:27-32`):

| Column | Type | Role | Citation |
|---|---|---|---|
| `id` | `uuid PK DEFAULT gen_random_uuid()` | row id | `…documenso_webhook_events.sql:27` |
| `received_at` | `timestamptz NOT NULL DEFAULT now()` | capture time; orders latest_event/status | `…documenso_webhook_events.sql:28` |
| `event` | `text` | raw event string, verbatim | `…documenso_webhook_events.sql:29` |
| `envelope_id` | `text` | best-effort extract, **NOT source of truth**; holds the NUMERIC document id (see Trap 1) | `…documenso_webhook_events.sql:30` |
| `external_id` | `text` | best-effort extract; = opportunity 8-char handle (or proposal `rs_` ref) at originate | `…documenso_webhook_events.sql:31` |
| `payload` | `jsonb NOT NULL` | **the raw verbatim body — system of record** | `…documenso_webhook_events.sql:32` |

Three BTREE indexes: `documenso_webhook_events_envelope_idx` on `(envelope_id)`,
`documenso_webhook_events_external_idx` on `(external_id)`, `documenso_webhook_events_received_idx` on
`(received_at DESC)` (`apps/edge_api/sql/documenso_webhook_events.sql:35-37`). DDL is applied to the
hq-x control-plane Postgres (`HQX_DB_URL_POOLED`), idempotent
(`apps/edge_api/sql/documenso_webhook_events.sql:1`, `24`, `26`).

`_TERMINAL_EVENTS = ('DOCUMENT_COMPLETED',)` — a single-element tuple; `DOCUMENT_COMPLETED` is the
all-signers-done signal (`apps/edge_api/src/documenso_webhooks/queries.py:41`). Per a code comment
verified against REAL landed rows 2026-06-17, events land verbatim as **UPPERCASE_UNDERSCORE**
(`DOCUMENT_SENT`/`DOCUMENT_OPENED`/`DOCUMENT_SIGNED`/`DOCUMENT_COMPLETED`), NOT the lowercase-dotted
form (`apps/edge_api/src/documenso_webhooks/queries.py:37-39`).

---

## 7. Sign reads (pair-gated, behind `/p/m/{opportunity_id}/{document_id}`)

### 7.1 `sign-state` — FULLY OFFLINE poll

`GET /api/v1/documenso/sign-state/{opportunity_id}/{document_id}` is PUBLIC and makes **ZERO
Documenso calls**. It returns `{opportunity_id, document_id, signed, latest_event, status}`
(`apps/edge_api/src/routers/documenso_webhooks_v1.py:76`, `96-102`).

`read_sign_state(conn, *, opportunity_id, document_id)` derives state at read time from the raw rows:

```sql
SELECT
  bool_or(event = ANY(%(terminal)s))                                        AS signed,
  (array_agg(event ORDER BY received_at DESC))[1]                           AS latest_event,
  (array_agg(payload->'payload'->>'status' ORDER BY received_at DESC))[1]   AS status,
  max(received_at)                                                          AS received_at
FROM business.documenso_webhook_events
WHERE external_id = %(opportunity_id)s
  AND envelope_id = %(document_id)s
```

(`apps/edge_api/src/documenso_webhooks/queries.py:63`, `90-96`.) It short-circuits to
`{signed:False, latest_event:None, status:None, received_at:None}` when either id is falsy or no rows
matched (`row is None or row[3] is None`)
(`apps/edge_api/src/documenso_webhooks/queries.py:83-85`, `105`).

- `signed` — a terminal `DOCUMENT_COMPLETED` row has landed for the pair.
- `latest_event` — most recent event name by `received_at`.
- `status` — the envelope-level Documenso status carried verbatim in
  `payload->'payload'->>'status'` (PENDING/COMPLETED/…), distinct from `latest_event`
  (`apps/edge_api/src/documenso_webhooks/queries.py:80-81`, `92`).

**Security model:** `opportunity_id` is the opportunity's public 8-char handle (access capability, 8
hex = 32 bits); `document_id` is Documenso's sequential/guessable numeric id, the unique lookup pin,
valid only behind a matching handle. A guessed numeric id with a wrong/missing handle → no matching
rows → `signed:false` (`apps/edge_api/src/routers/documenso_webhooks_v1.py:87-90`, enforced by the
`WHERE` at `apps/edge_api/src/documenso_webhooks/queries.py:95-96`). There is **NO projection
table** — everything is computed at read time; redelivery/dedup is deferred to projection time
(`apps/edge_api/src/documenso_webhooks/queries.py:64-65`,
`apps/edge_api/sql/documenso_webhook_events.sql:22`).

`business.opportunities.opportunity_id` is `GENERATED ALWAYS AS (LEFT(id::text, 8)) STORED` — the
first 8 chars of the row UUID, NON-unique by design, with a BTREE index
`idx_opportunities_opportunity_id` (`apps/edge_api/sql/opportunities_opportunity_id.sql:20-21`, `27`).

### 7.2 `sign-token` — ONE live read, pair-gated, client-vs-originator selection

`GET /api/v1/documenso/sign-token/{opportunity_id}/{document_id}` is PUBLIC, takes
`signer: str = Query("client")`, and makes ONE live read via
`documenso_client.read_document(document_id)`; a `DocumensoError` becomes 404 "document not found"
(`apps/edge_api/src/routers/documenso_webhooks_v1.py:105`, `109`, `128-131`).

```
doc = read_document(document_id)                                  [line 128]
# PAIR GATE
if (doc.external_id or "") != opportunity_id: 404 "document not found"  [line 135-140]
token = doc.signing_token                                          [line 143]
pairs = list(doc.recipient_tokens)                                 [line 144]
if len(pairs) > 1:                                                 [line 145]
    contact   = get_opportunity_contact_email(conn, opportunity_id)  [line 147]
    contact_l = (contact or "").strip().lower()
    client_tok     = next((t for (e,t) in pairs if contact_l and e == contact_l), None)  [line 150]
    originator_tok = next((t for (e,t) in pairs if e != contact_l), None)                [line 151]
    token = (originator_tok if signer == "originator" else client_tok) or token          [line 152]
return {"signing_token": token, "status": doc.status, "documenso_host": documenso_api_url()}  [line 154-158]
```

- **Pair gate:** asserts `(doc.external_id or '') == opportunity_id`; mismatch raises 404 IDENTICAL to
  not-found, so a guessed numeric id leaks nothing about which opportunity it belongs to
  (`apps/edge_api/src/routers/documenso_webhooks_v1.py:135`, `140`).
- **Two-recipient selection:** when `len(pairs) > 1`, the route loads the opportunity contact email;
  CLIENT = recipient whose email == contact email, ORIGINATOR = any recipient whose email != contact
  email; `signer=='originator'` picks `originator_tok` else `client_tok`, falling back to
  `doc.signing_token` (`apps/edge_api/src/routers/documenso_webhooks_v1.py:144-152`).
- **Single-signer:** `len(pairs) <= 1` ignores `signer` and returns `doc.signing_token` (the
  first/only signer token, `_extract_signer_token` output)
  (`apps/edge_api/src/routers/documenso_webhooks_v1.py:143-145`).

`get_opportunity_contact_email(conn, opportunity_id)` resolves the contact email by the 8-char public
handle: `SELECT c.email FROM business.opportunities o LEFT JOIN business.contacts c ON c.id =
o.contact_id WHERE o.opportunity_id = %s LIMIT 1`; returns `None` when handle/contact/email is unknown
(`apps/edge_api/src/documenso_webhooks/queries.py:44`, `51-55`, `60`).

---

## 8. Legacy proposals webhook + `normalize_event` (DEPRECATED for capture)

`normalize_event(body) -> NormalizedEvent` reads the event (raw), folds it to an enum key
(`raw.upper().replace('.', '_')`), maps via `_EVENT_TO_STATUS` (None for unknown), and digs the
payload (`payload`/`data`) for `envelope_id` (first of `id`/`documentId`/`envelopeId`) and
`external_id` (`externalId`). Handles Documenso delivering events as either lowercase-dotted
(`document.completed`) or enum form (`DOCUMENT_COMPLETED`)
(`apps/edge_api/src/services/documenso_client.py:803-818`).

`normalize_event`'s ONLY caller is the legacy `POST /api/v1/proposals/webhook`: it verifies the same
secret, calls `normalize_event`, ignores unmapped events (`evt.status is None` →
`{ok:True, ignored:True, event}`), resolves the proposal by `externalId` (the `rs_` ref) then by
`envelope_id`, and advances `business.engagement_proposals` status — it does NOT write
`business.documenso_webhook_events` (`apps/edge_api/src/routers/proposals_v1.py:337`, `342-371`).
Documenso is **repointed** from this legacy route to `/api/v1/documenso/webhook` (same shared
`DOCUMENSO_WEBHOOK_SECRET`); the legacy route is untouched and simply stops receiving deliveries
(config-level repoint described in comments at
`apps/edge_api/src/routers/documenso_webhooks_v1.py:5-8`, mount at `apps/edge_api/main.py:178-181`).

Both webhook routes independently 503 when `documenso_webhook_secret()` is None before any verify
(`apps/edge_api/src/routers/documenso_webhooks_v1.py:44-47`,
`apps/edge_api/src/routers/proposals_v1.py:342-345`).

### Lane disambiguation in the single capture table

The single repointed `POST /api/v1/documenso/webhook` receives BOTH lanes' deliveries, disambiguated
by the `external_id` SHAPE (the legacy `/proposals/webhook` does NOT write the capture table — it
advances `engagement_proposals` only; the one table receives both lanes because Documenso is
repointed to the single capture route):

| Lane | `external_id` shape | Stamp site |
|---|---|---|
| Proposals (Lane A) | `new_ref()` = `'rs_' + secrets.token_urlsafe(16)` | `apps/edge_api/src/routers/proposals_v1.py:108`; `apps/edge_api/src/proposals/queries.py:40`, `42` |
| Mandate-draft originate (Lane C) | the opportunity's 8-char handle | `apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:141`, `150` |

---

## 9. Cross-repo handoff map (SPA → BFF → edge_api)

The SPA never calls Documenso or edge_api directly; the platform-api BFF (`edge.ts`) proxies. All BFF
aliases below were grepped in `rare-structure-hq:apps/platform-api/src/lib/edge.ts`.

| Flow | BFF (`edge.ts`) | edge_api route → client fn |
|---|---|---|
| Mandate-draft confirm (Lane B) | `rare-structure-hq:apps/platform-api/src/lib/edge.ts:411` | `POST /api/v1/engagement-mandate-drafts/{id}/confirm` → `create_document_from_template_with_custom_pdf` (`…engagement_mandate_drafts_v1.py:82-106`) |
| Mandate-draft originate-prefilled (Lane C) | `rare-structure-hq:apps/platform-api/src/lib/edge.ts:440` | `POST /api/v1/engagement-mandate-drafts/{id}/originate-prefilled` → `create_document_from_template` (`…engagement_mandate_drafts_v1.py:143-166`) |
| Prospect mandate-draft document read | `rare-structure-hq:apps/platform-api/src/lib/edge.ts:510` | `GET /api/v1/engagement-mandate-drafts/document/{envelopeId}` → `read_template_document` (`…engagement_mandate_drafts_v1.py:169-181`) |
| Prospect sign-state poll (offline) | `rare-structure-hq:apps/platform-api/src/lib/edge.ts:541` | `GET /api/v1/documenso/sign-state/{opp}/{doc}` → `read_sign_state` (`…documenso_webhooks_v1.py:76-102`) |
| Prospect sign-token (live, gated) | `rare-structure-hq:apps/platform-api/src/lib/edge.ts:568` | `GET /api/v1/documenso/sign-token/{opp}/{doc}?signer=client\|originator` → `read_document` (`…documenso_webhooks_v1.py:105-158`) |
| Settings template-fields editor | `rare-structure-hq:apps/platform-api/src/lib/edge.ts:471`, `483` | `GET/POST /api/v1/documenso-template-fields[/defaults]` → `get_template_fields` / `set_template_field_defaults` (`…documenso_template_fields_v1.py:40-59`) |
| Documenso → capture | (Documenso → edge_api directly) | `POST /api/v1/documenso/webhook` (raw, `verify_webhook_secret`) and legacy `POST /api/v1/proposals/webhook` (`verify_webhook_secret` + `normalize_event`) (`…documenso_webhooks_v1.py:44-47`, `…proposals_v1.py:342-348`) |

---

## Status: ACTIVE / CONDITIONAL / DEPRECATED / STUB

| Component | Status | Note |
|---|---|---|
| `documenso_client.py` (the v2 client module) | ACTIVE | the single Documenso v2 caller (`…documenso_client.py:1`) |
| `create_signing_envelope` (Lane A) | CONDITIONAL | runs ONLY in the proposals `through-docraptor` branch; the `direct-to-documenso` branch is a STUB that returns before the call (`…proposals_v1.py:99-110`) |
| `create_document_from_template_with_custom_pdf` (Lane B) | ACTIVE | mandate-draft `/confirm` (`…documenso_client.py:321-403`) |
| `create_document_from_template` (Lane C) | ACTIVE | mandate-draft `/originate-prefilled` (`…documenso_client.py:427-594`) |
| `…with_custom_pdf` `recipients` override branch | STUB | present but unexercised; `/confirm` does not pass `recipients` (`…documenso_client.py:350-351`, `…engagement_mandate_drafts_v1.py:96-99`) |
| `read_template_document` | ACTIVE | public mandate-draft read (`…documenso_client.py:597-602`) |
| `read_document` + `DocumentReadResult` | ACTIVE | sign-token lane (`…documenso_client.py:620-649`) |
| `get_envelope` | ACTIVE | used by `client_token` and reads (`…documenso_client.py:284-288`) |
| `client_token` | STUB (no in-repo router caller found) | module-surface re-read helper; live use unconfirmed (`…documenso_client.py:291-293`) |
| `get_template_text_field_labels` | ACTIVE | engagement-mappings live fill (`…documenso_client.py:652-672`) |
| `get_template_fields` / `set_template_field_defaults` | ACTIVE | Settings template-defaults editor (`…documenso_client.py:689-759`) |
| `download_signed_pdf` | CONDITIONAL | only on completed proposal PDF stream (`…documenso_client.py:762-788`) |
| `verify_webhook_secret` | ACTIVE | gates both webhook routes (`…documenso_client.py:791-800`) |
| `normalize_event` | DEPRECATED for capture | used ONLY by legacy proposals webhook; canonical capture route does not call it (`…documenso_client.py:803-818`) |
| `POST /api/v1/documenso/webhook` | ACTIVE | raw capture; sole writer of the table (`…documenso_webhooks_v1.py:39`) |
| `GET /api/v1/documenso/sign-state/...` | ACTIVE | offline poll (`…documenso_webhooks_v1.py:76`) |
| `GET /api/v1/documenso/sign-token/...` | ACTIVE | live gated read (`…documenso_webhooks_v1.py:105`) |
| `POST /api/v1/proposals/webhook` | DEPRECATED | repointed away; advances `engagement_proposals`, NOT the raw table (`…proposals_v1.py:337`) |
| `business.documenso_webhook_events` | ACTIVE | append-only raw capture; system of record (`…documenso_webhook_events.sql:26`) |
| `engagement_docs/documenso.py` | OUT OF SCOPE (separate integration) | DRAFT-only, not distributed (`…engagement_docs/documenso.py:1-18`) |

---

## Traps

1. **`envelope_id` column name vs content.** Despite its name, the
   `business.documenso_webhook_events.envelope_id` column holds the **NUMERIC document id** (e.g.
   `"1462137"`) — the value the SPA link carries as `document_id` and that `read_sign_state` matches
   on `envelope_id = document_id` (`apps/edge_api/src/documenso_webhooks/queries.py:71-73`, `96`). The
   DDL comment at `apps/edge_api/sql/documenso_webhook_events.sql:18` calls it "the envelope handle
   (payload id/envelopeId), prefixed `envelope_…`" — this comment is **misleading vs. the matching
   logic**; the numeric value is what actually lands and is matched. The further sub-claim that this
   value is "always = `payload.id`" is a docstring assertion (`queries.py:71-73`), NOT enforced by the
   extract — the extract digs `inner.id`/`documentId`/`envelopeId` else `raw.id`/`envelopeId`
   (`apps/edge_api/src/routers/documenso_webhooks_v1.py:61`). Treat the column as "the numeric pin the
   link/poll match on," and re-derive from `payload` if extraction was wrong.

2. **`external_id` = 8-char handle, NOT a UUID — despite stale code comments.** `DocumentReadResult`
   and `read_document`'s docstring call `external_id` "the opportunity UUID stamped at originate"
   (`apps/edge_api/src/services/documenso_client.py:609`, `613`, `625`). The actual stamp site for
   Lane C is `external_id = prefill['opportunity_ref']`, explicitly the opportunity's PUBLIC 8-char
   handle, NOT the row UUID (`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:138-141`,
   `150`). The pair gate compares it to the 8-char `opportunity_id` from the link
   (`apps/edge_api/src/routers/documenso_webhooks_v1.py:135`). The "UUID" wording in the client
   docstrings is STALE; the value is the 8-char handle. (The proposals lane stamps `rs_…` instead —
   see §8.)

3. **Three token helpers are NOT interchangeable.** `_extract_client_token` needs a caller email;
   `_extract_signer_token` selects by role with NO email; `_recipient_email_tokens` returns all pairs.
   Lane C uses the email-matched one (not "first SIGNER") precisely because the originator can be a
   second recipient (`apps/edge_api/src/services/documenso_client.py:545-547`). Picking the wrong
   helper on a two-recipient document returns the WRONG signer's token. See §3.

4. **`/envelope/use` vs `/template/use` differ in contract.** recipients optional-vs-required,
   prefillFields by label-vs-id, no-readback-vs-readback. Do not copy a prefill block from Lane B into
   Lane C — the prefill key changes from `fieldMeta.label` to field `id`
   (`apps/edge_api/src/services/documenso_client.py:362-375` vs `505-512`). See §4.4.

5. **Numeric id vs prefixed `envelope_…` handle are not interchangeable across endpoints.** The
   prefixed `envelope_…` id is accepted by `/api/v2/envelope/*` and `/api/v2/template/*`; the NUMERIC
   document id is required by `/api/v2/document/{id}` and `/api/v2/document/{id}/download`. Per inline
   notes, the prefixed handle 400s on the document endpoint and the numeric id 400s on the envelope
   endpoints. These are documented as inline code assertions (verified present), **not independently
   re-tested against the live Documenso API** in source verification
   (`apps/edge_api/src/services/documenso_client.py:303-318`, `620-629`, `762-775`).

6. **Event-string form: the capture path relies on UPPERCASE_UNDERSCORE.** Real rows land verbatim as
   `DOCUMENT_COMPLETED` etc. (`apps/edge_api/src/documenso_webhooks/queries.py:37-39`), and the
   offline `signed` check is an exact-string compare against `_TERMINAL_EVENTS =
   ('DOCUMENT_COMPLETED',)` (`queries.py:41`, `90`). But the DDL comment gives the example as
   `document.completed` (lowercase-dotted) (`apps/edge_api/sql/documenso_webhook_events.sql:17`), and
   `normalize_event` (proposals lane only) tolerates BOTH forms
   (`apps/edge_api/src/services/documenso_client.py:806-810`). The raw-capture sign-state path does
   NOT tolerate the dotted form — if Documenso ever delivers `document.completed` to the capture
   route, `signed` would never flip. Code wins: the capture path assumes UPPERCASE_UNDERSCORE.

7. **`normalize_event` is NOT used by the canonical capture route.** Only the legacy
   `/api/v1/proposals/webhook` calls it (`apps/edge_api/src/routers/proposals_v1.py:348`); the
   canonical `/api/v1/documenso/webhook` persists the raw body and does its own inline `_dig`
   extraction (`apps/edge_api/src/routers/documenso_webhooks_v1.py:49-73`). Do not assume webhook
   capture maps events to internal statuses — it does not.

8. **No projection table for signing state.** `read_sign_state` computes everything at read time from
   `business.documenso_webhook_events`; redelivery/dedup is deferred (append-only table)
   (`apps/edge_api/src/documenso_webhooks/queries.py:64-65`,
   `apps/edge_api/sql/documenso_webhook_events.sql:22`). Do not look for a mirror/state table — there
   isn't one.

9. **CALIBRATION BOUNDARY (carried forward, unverified-against-live-spec).** The module docstring
   confirms the placeholder field shape (`field/create-many` with a `placeholder` string) against
   Documenso v2 source; but the remaining `# CALIBRATE` shapes (envelope/create multipart field names,
   the download operation, response key names) render client-side in the OpenAPI viewer and **could
   not be byte-pinned** — verify against the live Platform spec at `{base}/api/v2/openapi`. Auth, the
   webhook contract, and event names are confirmed and stable
   (`apps/edge_api/src/services/documenso_client.py:14-24`). Carry this status honestly; do not
   upgrade it.

10. **`config.documenso_api_url()` is a hard invariant for the embed.** A document created against one
    Documenso instance cannot be signed against another; the `documenso_host` surfaced in every
    originate/read response MUST match the creating instance
    (`apps/edge_api/src/config.py:34-37`). Do not let the SPA hardcode a different host.
