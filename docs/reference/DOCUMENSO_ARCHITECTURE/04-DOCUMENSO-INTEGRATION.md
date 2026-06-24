# 04 — Documenso Integration (v2 client, webhook capture, sign reads)

> **STATUS BANNER.** This file is the canonical reference for the Documenso v2 e-signature
> integration in `edge_api`: the single client module (`documenso_client.py`), its origination
> lanes, the token-extraction helpers, the raw webhook capture, and the pair-gated sign reads
> (offline `sign-state` + live `sign-token`, including two-recipient client-vs-originator selection).
> It is **lane-spanning**: it documents the shared signing core used by the live origination lanes —
> the **prefilled-document** lane (`/template/use`, mandate-draft `/originate-prefilled`) and the
> **embed-template** lane (direct link, mandate-draft `/originate-embed-template`) — plus the
> **template-creation** render+push lane (`/envelope/create` `type=TEMPLATE`), and the
> read/webhook/download side common to them. Every non-trivial claim carries a `path:line` citation
> that was opened and read.
>
> **REMOVED LANES (do not look for them).** The former Lane A (`create_signing_envelope`,
> `through-docraptor`, proposals) and Lane B (`create_document_from_template_with_custom_pdf`,
> `/envelope/use`, mandate-draft `/confirm`) have been DELETED from the codebase: there is no
> `proposals_v1.py` router and no `/confirm` endpoint, and neither client function exists
> (`apps/edge_api/src/services/documenso_client.py:1` — grep returns nothing for either name). The
> `operator_settings` `envelope-distribute` lane value is RETIRED (kept only so a pre-existing row
> never violates the CHECK; `apps/edge_api/sql/operator_settings.sql:80-89`).

## Orientation

`documenso_client.py` is the ONLY edge_api module that talks to Documenso Cloud v2 (Platform tier)
(`apps/edge_api/src/services/documenso_client.py:1`). edge_api is the single writer of Documenso
state in this platform; the SPA and the platform-api BFF never call Documenso directly — they call
edge_api routes, which call this client. There are **two origination lanes** that converge on this
module: the **prefilled-document** lane (`/template/use` → `distribute(NONE)`, mints the document NOW
with `distributionMethod:NONE` — no Documenso-sent email, the consumer app delivers the link and
embeds the recipient signing token) and the **embed-template** lane (a Documenso DIRECT LINK on the
template; NO document is minted up front — Documenso creates it at signer completion, source
`TEMPLATE_DIRECT_LINK`). A third, non-prospect lane CREATES the templates themselves — the render+push
lane (`/envelope/create` `type=TEMPLATE`), driven by a Trigger.dev task. On the read side there are
two PUBLIC prospect-facing reads behind the `/p/m/{opportunity_id}/{document_id}` signing link: an
**offline poll** (`sign-state`, zero Documenso calls, derived from raw webhook rows) and a **single
live read** (`sign-token`, one `GET /api/v2/document/{id}`, pair-gated). Inbound Documenso webhooks
land RAW in `business.documenso_webhook_events` (the system of record) via a secret-gated capture
route. A fresh agent should treat `payload` jsonb as truth and the scalar columns as best-effort
lookup keys only.

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
as `documenso_host` in the `/originate-prefilled`
(`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:164`), `/originate-embed-template`
(`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:213`), and `sign-token`
(`apps/edge_api/src/routers/documenso_webhooks_v1.py:157`) responses so the SPA mounts the embed
against the matching instance.

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
| `EnvelopeResult` | `envelope_id:str`, `document_id:int\|None` (numeric secondary id, for signed-PDF download), `client_token:str\|None` | `apps/edge_api/src/services/documenso_client.py:45-49` |
| `NormalizedEvent` | `event:str`, `status:str\|None` (mapped internal status, `None` for unknown event), `envelope_id:str\|None`, `external_id:str\|None` | `apps/edge_api/src/services/documenso_client.py:52-57` |
| `TemplateCreateResult` | `template_id:str` (v2 envelope/template handle), `numeric_id:int\|None` (legacy secondary id), `recipients:tuple[dict,...]` (placeholder recipients read back) — render+push lane (§4.3) | `apps/edge_api/src/services/documenso_client.py:409-417` |
| `DirectLinkResult` | `token:str` (reusable direct-template token — embed prop / `/d/{token}`), `enabled:bool`, `direct_template_recipient_id:int\|None`, `envelope_id:str\|None`, `template_id:int\|None` — embed-template lane (§4.2) | `apps/edge_api/src/services/documenso_client.py:468-477` |
| `DocumentReadResult` | `document_id:int`, `envelope_id:str\|None` (prefixed `envelope_…` handle), `external_id:str\|None` (the opportunity's 8-char handle stamped at originate), `status:str\|None`, `signing_token:str\|None` (first SIGNER / fallback), `recipient_tokens:tuple[tuple[str,str],...]=()` | `apps/edge_api/src/services/documenso_client.py:554-566` |

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
(`apps/edge_api/src/services/documenso_client.py:626`):

| Field type | fieldMeta key |
|---|---|
| `TEXT` | `text` |
| `NUMBER` | `value` |
| `DROPDOWN` | `defaultValue` |

`SIGNATURE`/`DATE` appear in NEITHER enum — they carry no default to set, and no internal status row
is keyed off them in the offline sign-state (only `DOCUMENT_COMPLETED` is terminal; see §6).

### 2.4 Small helpers

- `_numeric_document_id(env)` parses the legacy numeric document id from `secondaryId` (format
  `document_<n>`) by regex, returning `int` or `None`
  (`apps/edge_api/src/services/documenso_client.py:153-156`).
- `_prefill_value_for_label(label, values)` resolves the prefill value exact-key first, else falls
  back to the BASE name (`label.rsplit('_', 1)[0]`) so a split label like
  `participant_company_one`/`_two` draws from a single `participant_company` value; empty/None yields
  `None` (field stays open) (`apps/edge_api/src/services/documenso_client.py:159-171`).
- `_str_or_none(v)` coerces a non-`None` value to `str`, else `None`
  (`apps/edge_api/src/services/documenso_client.py:92-93`).
- `_template_id_number(documenso_template_id)` returns the NUMERIC template id the
  `/template/direct/*` endpoints require — passes a bare-numeric id through, else extracts the trailing
  digits from a prefixed handle, raising `DocumensoError` when none
  (`apps/edge_api/src/services/documenso_client.py:480-489`).
- `_field_default_value(field)` returns the field's current baked default per its type via
  `_DEFAULT_META_KEY`, or `None` when unset/empty
  (`apps/edge_api/src/services/documenso_client.py:629-635`).

---

## 3. The token-extraction helpers (WHEN each is used)

This is the most error-prone area for an agent. Three helpers extract recipient tokens from a
Documenso response body; they differ by **selection strategy** and are deliberately not
interchangeable. After the Lane A/B removal only two remain on a LIVE caller path
(`_extract_client_token` and `_recipient_email_tokens`); `_extract_signer_token` survives but is now
exercised only in the document-completion read (`read_document`), not in the prefilled-document
origination path (which uses `_extract_client_token`).

| Helper | Selection strategy | Returns | Used by |
|---|---|---|---|
| `_extract_client_token(body, email)` | EMAIL-MATCHED: descends `body` via `envelope`/`document`/`data`, reads `recipients` (key `recipients` or `Recipient`), returns the token of the recipient whose lowercased email == supplied email; falls back to the FIRST recipient (`chosen = chosen or r`) for single-signer envelopes | first matching/fallback `token` (or `signingToken`) | prefilled-document lane (`create_document_from_template`, by `recipient_email`), `client_token()` re-read |
| `_extract_signer_token(body)` | ROLE/FIRST: no caller email; selects the recipient whose role upper-cased == `'SIGNER'`, else the first recipient (`recips[0]`) | first SIGNER / first recipient `token` | `DocumentReadResult.signing_token` in `read_document` (the sign-token / document-completion read) |
| `_recipient_email_tokens(body)` | ALL-PAIRS: returns a tuple of `(email_lowercased, token)` for EVERY recipient that actually carries a token; blank-email recipients keep an empty-string key | `tuple[tuple[str,str],...]` | `read_document` → `DocumentReadResult.recipient_tokens`; the `sign-token` route iterates this to pick client vs originator |

Citations: `_extract_client_token` `apps/edge_api/src/services/documenso_client.py:96-112`;
`_extract_signer_token` `apps/edge_api/src/services/documenso_client.py:115-132`;
`_recipient_email_tokens` `apps/edge_api/src/services/documenso_client.py:135-150`.

> **Why `_extract_signer_token` exists separately:** a document whose recipients carry a blank email
> (the template-instantiated path) has no caller-supplied email to match on; select by role
> (`SIGNER`), falling back to the first recipient
> (`apps/edge_api/src/services/documenso_client.py:115-132`).

> **Why the prefilled-document lane uses `_extract_client_token` not `_extract_signer_token`:** the
> originator may be a SECOND recipient, so "first SIGNER" is ambiguous; the lane binds the prospect by
> the email it just set and matches on it
> (`apps/edge_api/src/services/documenso_client.py:346-348`).

---

## 4. The origination lanes

Two PROSPECT-facing origination lanes are live, both reachable as parallel endpoints on the
mandate-draft router (`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py`): the
**prefilled-document** lane (§4.1, mints a Documenso DOCUMENT now and `distribute`s with
`meta.distributionMethod:NONE`, DRAFT → PENDING, no email, signing token minted) and the
**embed-template** lane (§4.2, enables a Documenso DIRECT LINK on the template — NO document is minted
up front). A third, NON-prospect lane (§4.3) CREATES the templates from rendered HTML. The former Lane
A (proposals / `through-docraptor`) and Lane B (`/envelope/use` / `/confirm`) are deleted (see the
STATUS BANNER); the `/template/use` lane below is the canonical originate path.

### 4.1 Prefilled-document lane — `create_document_from_template` (`/template/use`, mandate-draft `/originate-prefilled`)

`create_document_from_template(documenso_template_id, *, recipient_email, recipient_name,
field_values_by_label=None, external_id=None, title=None) -> EnvelopeResult`
(`apps/edge_api/src/services/documenso_client.py:228-395`).

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
  not in `field_values` are skipped (`apps/edge_api/src/services/documenso_client.py:301-313`).
- readOnly lock is applied on the DERIVED document (not the template): a template field can't be
  readOnly without static text, and the prefilled value satisfies Documenso's "read-only must have
  text" rule (`apps/edge_api/src/services/documenso_client.py:350-380`).

**Caller:** `POST /api/v1/engagement-mandate-drafts/{draft_id}/originate-prefilled` (service-token
gated), with recipient email/name and `field_values` from
`queries.get_opportunity_prefill_and_contact`, `external_id = prefill['opportunity_ref']` (the
opportunity's PUBLIC 8-char handle), and `title='Engagement Agreement'`. It returns `opportunity_id`
(the 8-char handle) + `document_id` via `MandatePrefilledOriginated` so the SPA builds
`/p/m/{opportunity_id}/{document_id}` (`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:109-165`;
`external_id=opportunity_ref` at line 149; `title` at 152; handle is explicitly NOT the row UUID per
137-139; response model `apps/edge_api/src/engagement_mandate_drafts/models.py:21-39`).

### 4.2 Embed-template lane — `create_direct_link` (`/template/direct/create`, mandate-draft `/originate-embed-template`)

PARALLEL to the prefilled lane (which is left untouched). Instead of minting a document NOW, it
enables a Documenso DIRECT LINK on the draft's template and returns the reusable token. NO document
exists until a signer completes the embed; Documenso then creates the document itself (source
`TEMPLATE_DIRECT_LINK`). The signer SELF-IDENTIFIES — they enter their own name/email (the embed's
`name`/`email` are optional prefill, not locked).

The client surface for this lane (`apps/edge_api/src/services/documenso_client.py:459-551`):

| Fn | Contract | Citation |
|---|---|---|
| `get_template_recipients(documenso_template_id) -> list[dict]` | `GET /api/v2/template/{id}` → recipients (id/email/name/role); used to designate the direct-link recipient | `…documenso_client.py:504-511` |
| `create_direct_link(documenso_template_id, *, direct_recipient_id=None) -> DirectLinkResult` | `POST /api/v2/template/direct/create {templateId, directRecipientId?}` → `{token,…}`; on a 4xx (link already enabled) falls back to `toggle(enabled:true)` to recover the existing token (idempotent) | `…documenso_client.py:514-539` |
| `toggle_direct_link(documenso_template_id, *, enabled) -> DirectLinkResult` | `POST /api/v2/template/direct/toggle {templateId, enabled}` enable/disable | `…documenso_client.py:542-551` |

```
1. GET /api/v2/template/{id}  -> recipients[]  (get_template_recipients)
   direct_recipient_id = body.direct_recipient_id OR _pick_direct_recipient_id(recipients)
     (heuristic: the COUNTERPARTY — prefer participant/client, else first non-provider, else first)
2. POST /api/v2/template/direct/create  {templateId(int), directRecipientId?}
     (4xx => POST /api/v2/template/direct/toggle {enabled:true} to recover the existing token)
   -> DirectLinkResult(token, enabled, direct_template_recipient_id, envelope_id, template_id)
-> embed_url = {documenso_host}/embed/direct/{token}
```

**Caller:** `POST /api/v1/engagement-mandate-drafts/{draft_id}/originate-embed-template` (service-token
gated; optional body `EmbedTemplateOriginateRequest{direct_recipient_id}`,
`apps/edge_api/src/engagement_mandate_drafts/models.py:42-46`). Resolves the draft's
opportunity via `queries.get_opportunity_ref_and_contact`, picks the direct recipient
(`_pick_direct_recipient_id`, `…engagement_mandate_drafts_v1.py:43-64`), enables the link, and returns
`MandateEmbedTemplateOriginated` (`apps/edge_api/src/engagement_mandate_drafts/models.py:49-70`):
`{direct_token, documenso_host, embed_url, external_id, opportunity_id, direct_recipient_id,
recipient_email, recipient_name, status='ready'}`
(`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:168-221`). The same `direct_token` value
is the API token, the `<EmbedDirectTemplate token=…>` prop (cross-repo SPA), the public `/d/{token}`
URL, and the iframe `/embed/direct/{token}`. `external_id`/`opportunity_id` are the opportunity's
PUBLIC 8-char handle the embed stamps so the existing `(opportunity_id, document_id)` sign-state gate
applies once the completed document id surfaces client-side.

### 4.3 Template-creation lane — `create_template_from_pdf` (`/envelope/create` `type=TEMPLATE`, render+push)

This lane CREATES the Documenso templates the two prospect lanes instantiate from. It is NOT
prospect-facing — it is driven by a Trigger.dev task. Flow: a content-source registry row (or an
explicit selector) → catalog-resolved repo HTML → DocRaptor LIVE PDF → Documenso TEMPLATE.

`create_template_from_pdf(*, title, pdf, recipients=None, filename=None) -> TemplateCreateResult`
(`apps/edge_api/src/services/documenso_client.py:420-456`):

```
1. POST /api/v2/envelope/create   (multipart: data 'payload' JSON + files 'files')
     payload: type=TEMPLATE, title, recipients (default two SIGNER placeholders @ example.com)
2. GET  /api/v2/envelope/{id}     -> read placeholder recipients back
-> TemplateCreateResult(template_id, numeric_id=_numeric_document_id(env), recipients)
```

Field placement is NOT done here — the engagement-template HTML is a static, blank body (field-slot
blanks, no underscore glyphs), so signature/value fields are affixed in the Documenso editor
afterward. The default recipients are two SIGNER placeholders (`participant@example.com`,
`provider@example.com`) overridden per-deal at instantiation
(`apps/edge_api/src/services/documenso_client.py:403-406`).

**Orchestration — `engagement_templates.push.render_and_push`**
(`apps/edge_api/src/engagement_templates/push.py:63-130`): resolves the content dir via the
brand-aware catalog (`catalog.resolve(path, archetype, version, brand=…)`,
`apps/edge_api/src/engagement_templates/catalog.py:99-113`), assembles the HTML, renders the PDF via
DocRaptor, optionally stores an audit copy to R2, then calls `create_template_from_pdf`. It returns a
`PushOutcome` (`brand`, `path`, `archetype`, `version`, `style`, `source_kind`,
`documenso_template_id`, `documenso_numeric_id`, `pdf_bytes`, `pdf_r2_key`, `pdf_url`;
`apps/edge_api/src/engagement_templates/push.py:37-49`). Only `source_kind='repo-html'` is wired;
`'db-markdown'` raises `PushError` (`…push.py:81-84`). `record_run` writes one terminal row to
`ops.engagement_template_push_runs` fire-and-forget (`…push.py:133-169`).

**Brands.** The catalog discovers/resolves only `_ALLOWED_BRANDS = {'active-operators',
'rare-structure'}` (`apps/edge_api/src/engagement_templates/catalog.py:28`), each a subtree under
`apps/edge_api/content/<brand>/<path>/<archetype>/<version>/global_engagement_content/`. The
rare-structure capital-origination asset exists at
`apps/edge_api/content/rare-structure/docraptor-to-documenso-template/capital-origination/v1/global_engagement_content`.

**Caller:** `POST /internal/engagement-templates/render-push` (trigger-secret gated, mounted with the
`/internal` prefix at `apps/edge_api/main.py:274`; router prefix `/engagement-templates` at
`apps/edge_api/src/routers/internal_engagement_templates_v1.py:28`, route at `…:84-142`). The request
takes either a `registryPath`/`registryId` (resolves a `business.global_input_content` row → brand +
source_kind + brand-relative path) OR explicit `brand`/`path`/`archetype`/`version`
(`…internal_engagement_templates_v1.py:31-81`). Called by the Trigger.dev task
`engagement-template-push` (`src/trigger/engagement_template_push.ts`).

### 4.4 `_resolve_template_envelope_id`

`_resolve_template_envelope_id(client, documenso_template_id) -> str` resolves a numeric Documenso
template id to its prefixed envelope id via `GET /api/v2/template/{id}` reading `.envelopeId`
(fallback `.id`). Required because the numeric id 400s on envelope endpoints, and
`business.documenso_templates` stores only the numeric template id (as text)
(`apps/edge_api/src/services/documenso_client.py:191-206`;
`apps/edge_api/sql/engagement_mandate_draft_content.sql:47-50`).

### 4.5 Which lane runs — `operator_settings` gating

Which originate pathway "Confirm & Originate" uses is a per-operator config (one row per operator,
keyed by the Supabase `auth_user_id`), read/written ONLY through edge_api
(`GET/PUT /api/v1/operator-settings/{auth_user_id}`, service-token gated) — the BFF no longer touches
the table directly (`apps/edge_api/sql/operator_settings.sql:5-13`). Two independent selectors:

- **`render_mode`** = `Literal['through-docraptor', 'direct-to-documenso']`, DEFAULT
  `through-docraptor` (`apps/edge_api/src/operator_settings/models.py:15`,
  `apps/edge_api/sql/operator_settings.sql:42`, `69`).
- **`direct_to_documenso_lane`** = `Literal['envelope-distribute', 'prefill-document-from-template',
  'embed-template']`, DEFAULT `prefill-document-from-template`; applies ONLY when
  `render_mode='direct-to-documenso'` (`apps/edge_api/src/operator_settings/models.py:21-23`, `34`;
  `apps/edge_api/sql/operator_settings.sql:43`, `50`). The DB CHECK enforces exactly those three
  values (`apps/edge_api/sql/operator_settings.sql:81-89`):
  - `prefill-document-from-template` (DEFAULT) → the §4.1 prefilled-document lane.
  - `embed-template` → the §4.2 embed-template lane.
  - `envelope-distribute` → **RETIRED.** The value is retained so a pre-existing row never violates
    the CHECK, but no live path serves it (the `/envelope/use` + `/confirm` lane was removed in code;
    `apps/edge_api/sql/operator_settings.sql:31-34`, `80`).

The Pydantic `Literal`s mirror the DB CHECK so a bad value 422s at the edge rather than aborting the
pooled transaction on a CHECK violation (`apps/edge_api/src/operator_settings/models.py:1-7`).

---

## 5. Read side

### 5.1 `get_envelope` / `client_token`

- `get_envelope(envelope_id) -> dict` makes `GET /api/v2/envelope/{envelope_id}` and returns parsed
  JSON (`apps/edge_api/src/services/documenso_client.py:174-178`).
- `client_token(envelope_id, signer_email) -> str|None` (re)reads the email-matched client token from
  the live envelope via `get_envelope` + `_extract_client_token`
  (`apps/edge_api/src/services/documenso_client.py:181-183`). **No edge_api router caller located** in
  verification — it is a module-surface helper; whether any other module invokes it is unconfirmed.

### 5.2 `read_document` (sign-token lane)

`read_document(document_id: str) -> DocumentReadResult` makes `GET /api/v2/document/{document_id}`
(NUMERIC id; per the inline note the prefixed `envelope_` handle 400s there, verified 2026-06-17) and
returns `document_id` (resolved from `body.id` else supplied id), `envelope_id`, `external_id`,
`status`, `signing_token` (`_extract_signer_token`), `recipient_tokens` (`_recipient_email_tokens`)
(`apps/edge_api/src/services/documenso_client.py:569-598`). The live `GET` is at
`apps/edge_api/src/services/documenso_client.py:580`; the 400-comment at
`apps/edge_api/src/services/documenso_client.py:573`.

### 5.3 `download_signed_pdf`

`download_signed_pdf(envelope_id) -> bytes` resolves the numeric document id from
`GET /api/v2/envelope/{id}`, then `GET /api/v2/document/{document_id}/download?version=signed`. If
content-type is `application/pdf` or bytes start with `%PDF-` it returns the body; otherwise it reads
`downloadUrl`/`url` from JSON and fetches it with a **BARE httpx client (no Authorization header)** so
the Documenso API key never rides to a third-party host (S3/R2/CDN)
(`apps/edge_api/src/services/documenso_client.py:711-737`). **No edge_api router caller located** —
the former proposals download route (`GET /api/v1/proposals/{ref}/document`) was removed with the
proposals router; the function survives as module surface for the completed-document download.

### 5.4 Settings (template defaults editor)

- `get_template_text_field_labels(documenso_template_id) -> list[str]` resolves the envelope id then
  `GET /api/v2/envelope/{id}`, returning each TEXT field's `fieldMeta.label` in field order,
  de-duplicated (SIGNATURE/DATE excluded by the `type != 'TEXT'` continue)
  (`apps/edge_api/src/services/documenso_client.py:601-621`). Called by
  `GET /api/v1/engagement-mappings` to fill each option's `text_fields` live (per-request,
  concurrently via `asyncio.gather`, swallowing `DocumensoError` to keep the stored fallback)
  (`apps/edge_api/src/routers/engagement_mappings_v1.py:34-41`).
- `get_template_fields(documenso_template_id) -> list[dict]` resolves envelope id then
  `GET /api/v2/envelope/{id}`, returning editable fields (type in `_DEFAULT_META_KEY`) as
  `{id, type, label, recipient_id, page, default}`; SIGNATURE/DATE excluded
  (`apps/edge_api/src/services/documenso_client.py:638-661`).
- `set_template_field_defaults(documenso_template_id, defaults: dict[int,str]) -> int` writes default
  values onto the TEMPLATE's fields: resolves envelope id, `GET /api/v2/envelope/{id}`, indexes fields
  by id, writes each value into the right `fieldMeta` key per type (MERGING into existing meta), then
  `POST /api/v2/envelope/field/update-many` sending the FULL field
  (`id, type, recipientId, page, positionX, positionY, width, height, fieldMeta`); returns count
  written (0 if no data) (`apps/edge_api/src/services/documenso_client.py:664-708`).
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

## 8. `normalize_event` (dead module surface — no live caller)

`normalize_event(body) -> NormalizedEvent` reads the event (raw), folds it to an enum key
(`raw.upper().replace('.', '_')`), maps via `_EVENT_TO_STATUS` (None for unknown), and digs the
payload (`payload`/`data`) for `envelope_id` (first of `id`/`documentId`/`envelopeId`) and
`external_id` (`externalId`). Handles Documenso delivering events as either lowercase-dotted
(`document.completed`) or enum form (`DOCUMENT_COMPLETED`)
(`apps/edge_api/src/services/documenso_client.py:752-767`).

Its ONLY former caller was the legacy `POST /api/v1/proposals/webhook`, which was **removed** with the
proposals router (`proposals_v1.py` no longer exists). `normalize_event` and `NormalizedEvent`
therefore have **no live caller** today — they are dead module surface. The canonical
`POST /api/v1/documenso/webhook` capture route does NOT use `normalize_event`: it persists the raw
body and does its own inline `_dig` extraction (§6.2). The repoint to `/api/v1/documenso/webhook` is
still described in code comments (`apps/edge_api/src/routers/documenso_webhooks_v1.py:5-8`;
`apps/edge_api/sql/documenso_webhook_events.sql:12-13`) as historical context.

### Stamp site for the capture table

The canonical capture route receives every Documenso delivery and stamps `external_id` from the
envelope's `externalId`. The single live originate path stamps the opportunity's PUBLIC 8-char handle:

| Lane | `external_id` shape | Stamp site |
|---|---|---|
| Mandate-draft originate (prefilled / embed-template) | the opportunity's 8-char handle | `apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:149` (prefilled), `215` (embed-template) |

---

## 9. Cross-repo handoff map (SPA → BFF → edge_api)

The SPA never calls Documenso or edge_api directly; the platform-api BFF (`edge.ts`) proxies. The BFF
column lives in the SEPARATE `rare-structure-hq` repo — those alias line numbers cannot be verified
from this repo and are marked **(cross-repo)**; the edge_api column is verified against this repo's
routes. The Trigger.dev row is task-driven (no SPA/BFF path).

| Flow | BFF (`edge.ts`) | edge_api route → client fn |
|---|---|---|
| Mandate-draft originate-prefilled (prefilled-document) | `rare-structure-hq:apps/platform-api/src/lib/edge.ts` (cross-repo) | `POST /api/v1/engagement-mandate-drafts/{id}/originate-prefilled` → `create_document_from_template` (`…engagement_mandate_drafts_v1.py:109-165`) |
| Mandate-draft originate-embed-template (embed-template) | `rare-structure-hq:apps/platform-api/src/lib/edge.ts` (cross-repo) | `POST /api/v1/engagement-mandate-drafts/{id}/originate-embed-template` → `get_template_recipients` + `create_direct_link` (`…engagement_mandate_drafts_v1.py:168-221`) |
| Prospect sign-state poll (offline) | `rare-structure-hq:apps/platform-api/src/lib/edge.ts` (cross-repo) | `GET /api/v1/documenso/sign-state/{opp}/{doc}` → `read_sign_state` (`…documenso_webhooks_v1.py:76-102`) |
| Prospect sign-token (live, gated) | `rare-structure-hq:apps/platform-api/src/lib/edge.ts` (cross-repo) | `GET /api/v1/documenso/sign-token/{opp}/{doc}?signer=client\|originator` → `read_document` (`…documenso_webhooks_v1.py:105-158`) |
| Settings template-fields editor | `rare-structure-hq:apps/platform-api/src/lib/edge.ts` (cross-repo) | `GET/POST /api/v1/documenso-template-fields[/defaults]` → `get_template_fields` / `set_template_field_defaults` (`…documenso_template_fields_v1.py:40-59`) |
| Engagement-template render+push | (Trigger.dev task `engagement-template-push`, not SPA/BFF) | `POST /internal/engagement-templates/render-push` → `push.render_and_push` → `create_template_from_pdf` (`…internal_engagement_templates_v1.py:84-142`) |
| Documenso → capture | (Documenso → edge_api directly) | `POST /api/v1/documenso/webhook` (raw, `verify_webhook_secret`) (`…documenso_webhooks_v1.py:44-47`) |

---

## Status: ACTIVE / CONDITIONAL / DEPRECATED / STUB

| Component | Status | Note |
|---|---|---|
| `documenso_client.py` (the v2 client module) | ACTIVE | the single Documenso v2 caller (`…documenso_client.py:1`) |
| `create_document_from_template` (prefilled-document lane) | ACTIVE | mandate-draft `/originate-prefilled` (`…documenso_client.py:228-395`) |
| `create_direct_link` (embed-template lane) | ACTIVE | mandate-draft `/originate-embed-template`; idempotent via toggle-on fallback (`…documenso_client.py:514-539`) |
| `get_template_recipients` | ACTIVE | designates the direct-link recipient for the embed-template lane (`…documenso_client.py:504-511`) |
| `toggle_direct_link` | ACTIVE (module surface) | enable/disable a template's direct link; recovery path inside `create_direct_link` (`…documenso_client.py:542-551`) |
| `create_template_from_pdf` + `TemplateCreateResult` | ACTIVE | render+push lane terminal step (`…documenso_client.py:420-456`) |
| `read_document` + `DocumentReadResult` | ACTIVE | sign-token lane (`…documenso_client.py:569-598`) |
| `get_envelope` | ACTIVE | used by `client_token` and reads (`…documenso_client.py:174-178`) |
| `client_token` | STUB (no in-repo router caller found) | module-surface re-read helper; live use unconfirmed (`…documenso_client.py:181-183`) |
| `get_template_text_field_labels` | ACTIVE | engagement-mappings live fill (`…documenso_client.py:601-621`) |
| `get_template_fields` / `set_template_field_defaults` | ACTIVE | Settings template-defaults editor (`…documenso_client.py:638-708`) |
| `download_signed_pdf` | STUB (no in-repo router caller found) | completed-document download surface; the proposals caller was removed (`…documenso_client.py:711-737`) |
| `verify_webhook_secret` | ACTIVE | gates the webhook route (`…documenso_client.py:740-749`) |
| `normalize_event` + `NormalizedEvent` | DEAD (no live caller) | the only caller (legacy proposals webhook) was removed; canonical capture route does not call it (`…documenso_client.py:752-767`) |
| `POST /api/v1/documenso/webhook` | ACTIVE | raw capture; sole writer of the table (`…documenso_webhooks_v1.py:39`) |
| `GET /api/v1/documenso/sign-state/...` | ACTIVE | offline poll (`…documenso_webhooks_v1.py:76`) |
| `GET /api/v1/documenso/sign-token/...` | ACTIVE | live gated read (`…documenso_webhooks_v1.py:105`) |
| `POST /internal/engagement-templates/render-push` | ACTIVE | Trigger-secret render+push lane (`…internal_engagement_templates_v1.py:84-142`) |
| `ops.engagement_template_push_runs` | ACTIVE | append-only push-run ledger (`…sql/ops_engagement_template_push_runs.sql:12`) |
| `business.global_input_content` | ACTIVE | content-source registry (brand + source_kind) (`…sql/global_input_content.sql:21`) |
| `business.documenso_webhook_events` | ACTIVE | append-only raw capture; system of record (`…documenso_webhook_events.sql:26`) |
| `operator_settings.direct_to_documenso_lane='envelope-distribute'` | RETIRED | value retained for pre-existing rows; no live path (`…sql/operator_settings.sql:80`) |

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

2. **`external_id` = the opportunity's 8-char handle, NOT a UUID.** The stamp site for the
   prefilled-document lane is `external_id = prefill['opportunity_ref']`, explicitly the opportunity's
   PUBLIC 8-char handle, NOT the row UUID
   (`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:137-139`, `149`); the embed-template
   lane stamps the same handle (`…engagement_mandate_drafts_v1.py:215`). The pair gate compares it to
   the 8-char `opportunity_id` from the link (`apps/edge_api/src/routers/documenso_webhooks_v1.py:135`).
   `DocumentReadResult` and `read_document`'s docstrings now correctly describe it as "the opportunity's
   8-char handle stamped at originate" (`apps/edge_api/src/services/documenso_client.py:558`, `562`,
   `576`) — the prior STALE "UUID" wording has been removed.

3. **The token helpers are NOT interchangeable.** `_extract_client_token` needs a caller email;
   `_extract_signer_token` selects by role with NO email; `_recipient_email_tokens` returns all pairs.
   The prefilled-document lane uses the email-matched one (not "first SIGNER") precisely because the
   originator can be a second recipient
   (`apps/edge_api/src/services/documenso_client.py:346-348`). Picking the wrong helper on a
   two-recipient document returns the WRONG signer's token. See §3.

4. **prefillFields are keyed by FIELD ID, not label, in the live lane.** The prefilled-document lane
   (`/template/use`) emits id-keyed `prefillFields`, type lowercased, value always a STRING, fanning
   each label to EVERY matching field id; SIGNATURE/DATE and unmatched labels are skipped
   (`apps/edge_api/src/services/documenso_client.py:301-313`). (The former `/envelope/use` Lane B,
   which keyed by `fieldMeta.label`, has been removed — do not reintroduce a label-keyed prefill
   pattern.) See §4.1.

5. **Numeric id vs prefixed `envelope_…` handle are not interchangeable across endpoints.** The
   prefixed `envelope_…` id is accepted by `/api/v2/envelope/*` and `/api/v2/template/*`; the NUMERIC
   document id is required by `/api/v2/document/{id}` and `/api/v2/document/{id}/download`. Per inline
   notes, the prefixed handle 400s on the document endpoint and the numeric id 400s on the envelope
   endpoints. These are documented as inline code assertions (verified present), **not independently
   re-tested against the live Documenso API** in source verification
   (`apps/edge_api/src/services/documenso_client.py:191-206`, `569-577`, `711-721`).

6. **Event-string form: the capture path relies on UPPERCASE_UNDERSCORE.** Real rows land verbatim as
   `DOCUMENT_COMPLETED` etc. (`apps/edge_api/src/documenso_webhooks/queries.py:37-39`), and the
   offline `signed` check is an exact-string compare against `_TERMINAL_EVENTS =
   ('DOCUMENT_COMPLETED',)` (`queries.py:41`, `90`). But the DDL comment gives the example as
   `document.completed` (lowercase-dotted) (`apps/edge_api/sql/documenso_webhook_events.sql:17`), and
   the now-caller-less `normalize_event` tolerates BOTH forms
   (`apps/edge_api/src/services/documenso_client.py:755-759`). The raw-capture sign-state path does
   NOT tolerate the dotted form — if Documenso ever delivers `document.completed` to the capture
   route, `signed` would never flip. Code wins: the capture path assumes UPPERCASE_UNDERSCORE.

7. **`normalize_event` is dead and is NOT used by the capture route.** Its only caller (the legacy
   `/api/v1/proposals/webhook`) was removed with the proposals router; nothing calls `normalize_event`
   today. The canonical `/api/v1/documenso/webhook` persists the raw body and does its own inline
   `_dig` extraction (`apps/edge_api/src/routers/documenso_webhooks_v1.py:49-73`). Do not assume
   webhook capture maps events to internal statuses — it does not. See §8.

8. **No projection table for signing state.** `read_sign_state` computes everything at read time from
   `business.documenso_webhook_events`; redelivery/dedup is deferred (append-only table)
   (`apps/edge_api/src/documenso_webhooks/queries.py:64-65`,
   `apps/edge_api/sql/documenso_webhook_events.sql:22`). Do not look for a mirror/state table — there
   isn't one.

9. **CALIBRATION BOUNDARY (carried forward, unverified-against-live-spec).** Per the module docstring,
   the multipart/JSON shapes used here (envelope/create multipart field names, the download operation,
   response key names) render client-side in Documenso's OpenAPI viewer and **could not be byte-pinned**
   at build time — verify against the live Platform spec at `{base}/api/v2/openapi`. Auth, the webhook
   contract, and event names are confirmed and stable
   (`apps/edge_api/src/services/documenso_client.py:7-11`). Carry this status honestly; do not upgrade
   it.

10. **`config.documenso_api_url()` is a hard invariant for the embed.** A document created against one
    Documenso instance cannot be signed against another; the `documenso_host` surfaced in every
    originate/read response MUST match the creating instance
    (`apps/edge_api/src/config.py:34-37`). Do not let the SPA hardcode a different host.
