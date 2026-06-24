# Flow: `direct-to-documenso` (all sub-lanes)

> **STATUS BANNER.** This file documents `operator_settings.render_mode = 'direct-to-documenso'` and its sub-lanes, selected by `operator_settings.direct_to_documenso_lane`. The lane domain is CHECK-constrained to THREE values (`apps/edge_api/sql/operator_settings.sql:84-89`):
> - **`prefill-document-from-template`** — the DB DEFAULT (`apps/edge_api/sql/operator_settings.sql:43`) and CANONICAL, fully-instrumented direct lane (`POST .../{draft_id}/originate-prefilled`, `POST /api/v2/template/use`, prefill from `opportunity_specific_content.field_values`, `externalId = opportunity` 8-char handle, returns the `(opportunity_id, document_id)` pair → builds `/p/m/{opp}/{doc}`). Mints a Documenso document NOW.
> - **`embed-template`** — the NEW direct-link lane (`POST .../{draft_id}/originate-embed-template`, `POST /api/v2/template/direct/create`, enables a reusable direct link on the template, returns its `direct_token` → the SPA mounts `<EmbedDirectTemplate>`). NO document is minted here; the signer self-identifies in the embed and Documenso creates the document (source `TEMPLATE_DIRECT_LINK`) at completion. PARALLEL to the prefill lane.
> - **`envelope-distribute`** — RETIRED. The `/envelope/use` + `.../{draft_id}/confirm` lane was REMOVED from code (no `/confirm` route, no `create_document_from_template_with_custom_pdf` function). The CHECK still accepts the value so a pre-existing operator row never violates it, but NO live path serves it (`apps/edge_api/sql/operator_settings.sql:31-34,80`).
>
> The sibling `through-docraptor` mode is OUT OF SCOPE here. Where this file disagrees with `docs/reference/DIRECT_TO_DOCUMENSO_PAYMENT_E2E.md`, **the code wins** (that doc was not relied upon).

## Orientation

A fresh agent reading this: `direct-to-documenso` is the originate pathway that SKIPS DocRaptor PDF rendering and works straight from a stored Documenso template. There are TWO LIVE sub-lanes under it (a third, `envelope-distribute`, is RETIRED — code path removed); the choice is a per-operator setting, NOT a per-request flag, and the dispatch happens **in the SPA**, not in `edge_api`. Both live sub-lanes live in the SAME router (`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py`). The DEFAULT lane (`prefill-document-from-template`) instantiates a signable Documenso document with `distributionMethod:'NONE'` (no email; the consumer app delivers the signing link) and is the one that wires the prospect-facing `/p/m/...` signing + payment surfaces. The NEW lane (`embed-template`) does NOT mint a document at originate — it enables a Documenso direct link on the template and hands the SPA a reusable token; the signer self-identifies in an embed and Documenso creates the document at completion. The two live lanes differ on almost every other axis — entrypoint route, what is created at originate, recipient binding, prefill, response shape, and the prospect surface. Read the divergence table first; it is the spine of this document.

---

## Divergence Table — live lanes (`prefill-document-from-template` vs `embed-template`)

`envelope-distribute` is RETIRED (no live route; `create_document_from_template_with_custom_pdf` and the `/confirm` route were removed). The two LIVE lanes are below.

| Dimension | `prefill-document-from-template` (DEFAULT, CANONICAL) | `embed-template` (NEW, direct-link) |
|---|---|---|
| Lane status | ACTIVE (DB default) | ACTIVE (conditional — only when lane set explicitly) |
| Setting value | `direct_to_documenso_lane = 'prefill-document-from-template'` (DEFAULT, `apps/edge_api/sql/operator_settings.sql:43`; CHECK `:84-89`) | `direct_to_documenso_lane = 'embed-template'` (CHECK `apps/edge_api/sql/operator_settings.sql:84-89`) |
| edge_api entrypoint | `POST /api/v1/engagement-mandate-drafts/{draft_id}/originate-prefilled` (`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:109`) | `POST /api/v1/engagement-mandate-drafts/{draft_id}/originate-embed-template` (`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:168`) |
| Auth | service-token (`Depends(require_service_token)`) `:111` | service-token (`Depends(require_service_token)`) `:170` |
| Documenso client fn | `create_document_from_template` (`apps/edge_api/src/services/documenso_client.py:228`) | `get_template_recipients` + `create_direct_link` (`apps/edge_api/src/services/documenso_client.py:504,514`) |
| Documenso endpoint | `POST /api/v2/template/use` (`documenso_client.py:336`) | `POST /api/v2/template/direct/create` (`documenso_client.py:529`) |
| Document minted at originate? | YES — a Documenso document instantiated now (`PENDING`) | NO — Documenso creates it at signer completion (source `TEMPLATE_DIRECT_LINK`) (`engagement_mandate_drafts_v1.py:178-181`) |
| Prefill SOURCE | `business.opportunity_specific_content.field_values` via `get_opportunity_prefill_and_contact` (`queries.py:96,115,118`) | NONE at originate — signer fills the template directly (optional embed prefill only) |
| Recipient binding | EXPLICIT — placeholder recipient overridden with opportunity contact email/name (`documenso_client.py:282,320`) | DIRECT-LINK recipient designated on the template (`_pick_direct_recipient_id`, `engagement_mandate_drafts_v1.py:43,197`); signer self-identifies (name/email NOT locked) |
| `externalId` stamped | opportunity PUBLIC 8-char handle `opportunity_id` (`engagement_mandate_drafts_v1.py:149`) | returned as `external_id` for the embed to stamp at completion (`engagement_mandate_drafts_v1.py:215`) |
| readOnly field lock | YES — derived fields locked via `POST /api/v2/envelope/field/update-many` (`documenso_client.py:336+`) | N/A — no document at originate |
| Title override | `override.title = 'Engagement Agreement'` (`engagement_mandate_drafts_v1.py:152`) | N/A |
| Token | per-document `client_token` (email-matched `_extract_client_token`, `documenso_client.py:96`) | reusable template `direct_token` (`DirectLinkResult.token`, `documenso_client.py:469,496`) |
| Response model | `MandatePrefilledOriginated` = `{envelope_id, document_id, opportunity_id, signing_token, status, documenso_host}` (`models.py:21-39`) | `MandateEmbedTemplateOriginated` = `{direct_token, documenso_host, embed_url, external_id, opportunity_id, direct_recipient_id, recipient_email, recipient_name, status}` (`models.py:49-70`) |
| Builds SPA signing link? | YES — `/p/m/{opportunity_id}/{document_id}` (`rare-structure-hq:apps/platform-app/src/proposals/MandateDraftShell.tsx:98-99`) | YES — `/p/t/{opportunityId}/{directToken}?host=` → `DirectTemplateSignPage` / `EmbedDirectTemplate` (cross-repo, rare-structure-hq) |
| Prospect read surface | PUBLIC `GET .../sign-token/{opp}/{doc}` (LIVE, pair-gated) + `GET .../sign-state/{opp}/{doc}` (OFFLINE poll) (`documenso_webhooks_v1.py:105,76`) | embed surface; `embed_url = {host}/embed/direct/{token}` (`engagement_mandate_drafts_v1.py:214`); completion document then tracked via `sign-state` (gate: `externalId == opportunity_id`) (`models.py:57-60`) |
| SPA dispatch branch | `if (directToDocumensoLane === 'prefill-document-from-template')` (`...MandateDraftShell.tsx:93-100`) | 3rd dispatch branch in `MandateDraftShell` (cross-repo, rare-structure-hq) |
| Persists envelope/token? | NO — durable anchor is the webhook capture (`externalId`+numeric doc id) | NO — direct link lives on the template; document born at completion |

---

## Section A — Selection & dispatch

### A.1 The setting (edge_api owns the DB)

`operator_settings.direct_to_documenso_lane` is `text NOT NULL DEFAULT 'prefill-document-from-template'` (`apps/edge_api/sql/operator_settings.sql:43`), CHECK-constrained to `{'envelope-distribute','prefill-document-from-template','embed-template'}` (`apps/edge_api/sql/operator_settings.sql:84-89`) — three values, of which `envelope-distribute` is RETIRED (value retained so a pre-existing row never violates the constraint, `apps/edge_api/sql/operator_settings.sql:80`). It is a SECOND, INDEPENDENT sub-selector that applies **only** when `render_mode = 'direct-to-documenso'`; it is ignored under `render_mode = 'through-docraptor'` (SQL comment `apps/edge_api/sql/operator_settings.sql:23-36`). `render_mode` itself is CHECK-constrained to `{'through-docraptor','direct-to-documenso'}` (`apps/edge_api/sql/operator_settings.sql:69`). The DEFAULT routes new and row-less operators to the live prefill-from-template lane (`apps/edge_api/sql/operator_settings.sql:35-36`).

The lane→endpoint mapping is documented in the SQL comments (`apps/edge_api/sql/operator_settings.sql:26-34,77-80`) but is NOT enforced in SQL — it is enforced by the SPA dispatch (below).

Shared platform types mirror the enum (cross-repo, rare-structure-hq): `RenderMode = 'through-docraptor' | 'direct-to-documenso'` and the `DirectToDocumensoLane` literal, which must now include `'embed-template'` to match the DB constraint at `apps/edge_api/sql/operator_settings.sql:84-89` — i.e. `'envelope-distribute' | 'prefill-document-from-template' | 'embed-template'` (verify against rare-structure-hq separately; this repo owns the canonical CHECK).

### A.2 Dispatch is in the SPA, not edge_api

`MandateDraftShell.confirm()` branches on the operator's lane (cross-repo, `rare-structure-hq:apps/platform-app/src/proposals/MandateDraftShell.tsx`). Three dispatch branches, one per live/retired lane:

```text
async function confirm():
  if directToDocumensoLane === 'prefill-document-from-template':   # DEFAULT
      res = await originatePrefilled(token, draftId)
      if res.documentId == null: throw
      link = { opportunityId: res.opportunityId,
               documentId:    res.documentId }
      saveSignLink(draftId, link); setSignLink(link)               # → /p/m/{opp}/{doc}
  else if directToDocumensoLane === 'embed-template':              # NEW
      res = await originateEmbedTemplate(token, draftId)
      link = { opportunityId: res.opportunityId,
               directToken:   res.directToken,
               host:          res.documensoHost }                  # → /p/t/{opp}/{token}?host=
      saveSignLink(draftId, link); setSignLink(link)
```

`SignLink` is a discriminated union (the `/p/m` document pair vs the `/p/t` direct-template token); the host is threaded through `?host=` on the `/p/t` route so the embed mounts against the right Documenso instance (cross-repo, rare-structure-hq). The retired `envelope-distribute` `else` branch (which called `confirmMandateDraft` and set `setSignLink(null)`) is gone with its route.

---

## Section B — `prefill-document-from-template` sub-lane (DEFAULT, CANONICAL)

> **RETIRED — `envelope-distribute`.** The old DB-default lane (`POST .../{draft_id}/confirm` → `create_document_from_template_with_custom_pdf` → `POST /api/v2/envelope/use`, prefill from the staged `engagement_mandate_draft_content.prefill_values`, `externalId = draft_id`, returning only `MandateDraftConfirmed{envelope_id, signing_token, documenso_host}` with no `(opp, doc)` pair) was REMOVED from code. There is no `/confirm` route in `apps/edge_api/src/routers/engagement_mandate_drafts_v1.py`, no `create_document_from_template_with_custom_pdf` function in `apps/edge_api/src/services/documenso_client.py`, and no `MandateDraftConfirmed`/`MandateDraftDocument` model (`apps/edge_api/src/engagement_mandate_drafts/models.py`). Its public single-handle prospect read (`GET .../document/{envelope_id}` → `read_template_document`) and the `_extract_signer_token` role-based token path are likewise gone. The CHECK constraint still accepts the literal `'envelope-distribute'` so a pre-existing operator row never violates it (`apps/edge_api/sql/operator_settings.sql:80,84-89`), but NO live path serves it. The DEFAULT is now `prefill-document-from-template` (`apps/edge_api/sql/operator_settings.sql:43`).

### B.1 Route: `POST /api/v1/engagement-mandate-drafts/{draft_id}/originate-prefilled`

Service-token gated (`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:109-112`); the canonical template-use originate path (`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:114`). Control flow:

```text
originate_prefilled(draft_id):                                # py:113
  draft = get_draft(conn, draft_id)        # ONLY for draft.documenso_template_id  # py:124
  prefill = get_opportunity_prefill_and_contact(draft.opportunity_id)             # py:127
  if not prefill: 404 "no opportunity_specific_content for this opportunity"       # py:130-132
  if not prefill.recipient_email: 422 "opportunity contact has no email …"         # py:134-136
  opportunity_ref = prefill.opportunity_ref   # the 8-char handle                  # py:141
  result = create_document_from_template(
             draft.documenso_template_id,                      # py:144
             recipient_email = prefill.recipient_email,        # py:145
             recipient_name  = prefill.recipient_name or prefill.recipient_email,  # py:146
             field_values_by_label = prefill.field_values,     # py:147
             external_id = opportunity_ref,   # ← the 8-char handle  # py:150
             title = 'Engagement Agreement')                   # py:153
  return MandatePrefilledOriginated(
           envelope_id   = result.envelope_id,                 # py:158
           document_id   = result.document_id,                 # py:159 (numeric)
           opportunity_id= opportunity_ref,                    # py:162 (8-char handle)
           signing_token = result.client_token,                # py:163
           status        = 'pending',                          # py:164
           documenso_host= config.documenso_api_url())         # py:165
```

`DocumensoError` → `HTTPException(502, "documenso: {e}")` (`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:155-156`). The draft is loaded ONLY for its `documenso_template_id` — the prefill values and recipient do NOT come from the draft (`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:124,127`).

### B.2 Prefill source + recipient: `get_opportunity_prefill_and_contact`

`apps/edge_api/src/engagement_mandate_drafts/queries.py:96`. UUID-guarded (`apps/edge_api/src/engagement_mandate_drafts/queries.py:108-111`). One SELECT/JOIN:

- `osc.field_values` FROM `business.opportunity_specific_content osc` (`apps/edge_api/src/engagement_mandate_drafts/queries.py:115,119`) — label→value per-deal values, returned as `field_values` (`apps/edge_api/src/engagement_mandate_drafts/queries.py:130`).
- recipient from `JOIN business.opportunities o ON o.id = osc.opportunity_id` then `LEFT JOIN business.contacts c ON c.id = o.contact_id`, returning `c.email` and `NULLIF(TRIM(CONCAT_WS(' ', c.first_name, c.last_name)),'')` (`apps/edge_api/src/engagement_mandate_drafts/queries.py:116,117,120,121`).
- `opportunity_ref = o.opportunity_id or str(opportunity_id)[:8]` — never null (`apps/edge_api/src/engagement_mandate_drafts/queries.py:135`).

`None` when no `opportunity_specific_content` row exists or the ref is not a UUID (`apps/edge_api/src/engagement_mandate_drafts/queries.py:127-128`).

### B.3 `externalId` = the public 8-char handle

`opportunities.opportunity_id` is a GENERATED column: `ALTER TABLE business.opportunities ADD COLUMN IF NOT EXISTS opportunity_id text GENERATED ALWAYS AS (LEFT(id::text, 8)) STORED` (`apps/edge_api/sql/opportunities_opportunity_id.sql:19-21`), with a non-unique BTREE index `idx_opportunities_opportunity_id` (`apps/edge_api/sql/opportunities_opportunity_id.sql:27-28`). It is the first 8 chars of the row UUID — the externally-visible opportunity id stamped as `externalId` (`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:147-149`) and carried in the prospect link. `business.opportunities` is upstream-owned; this DDL is ALTER-only.

### B.4 Documenso instantiation: `create_document_from_template`

`apps/edge_api/src/services/documenso_client.py:228`. Steps:

```text
1) GET /api/v2/template/{id} → fields[] + recipients[] (resolved LIVE)  # py:260
2) RECIPIENT BIND: select the PLACEHOLDER recipient (no email set),
   else SIGNER-role, else recips[0]; capture recipient_id              # py:275-282
3) PREFILL FAN-OUT: by_label[label] = [(field id, lowercased type), …] # py:290,297
   - one label may sit on MULTIPLE fields → one prefillFields entry per field id  # py:308-313
   - value resolved by _prefill_value_for_label: exact key first, else BASE name (label.rsplit('_',1)[0])
4) POST /api/v2/template/use  json payload:                            # py:336
   - recipients=[{id, email, name}]  (REQUIRED on /template/use)       # py:319-321
   - override.title = title[:255]  (when title provided)
   - externalId, distributeDocument:false                             # py:322,326
   read body → envelope_id (body.envelopeId), numeric document_id (body.id)
   token = _extract_client_token(body, recipient_email)  # email-matched
5) READONLY LOCK on the DERIVED document:
   GET /api/v2/envelope/{envelope_id} → new_fields
   identify prefilled fields by non-empty value (TEXT→fieldMeta.text, NUMBER→fieldMeta.value)
   POST /api/v2/envelope/field/update-many  readOnly:true
6) POST /api/v2/envelope/distribute  meta.distributionMethod=NONE
return EnvelopeResult(envelope_id, document_id (int|None), client_token=token)
```

A recipient that already carries an email (e.g. the originator added via "Add Myself") is OMITTED from `recipients[]` and keeps its template default — this supports two-recipient documents (`apps/edge_api/src/services/documenso_client.py:264-269`). readOnly cannot be set at instantiation, so the lock MUST be applied on the derived document where prefilled values satisfy Documenso's "read-only must have text" rule; SIGNATURE/DATE carry no value and stay open for the signer (`apps/edge_api/src/services/documenso_client.py:251-255`).

### B.5 Per-signer token extraction

`_extract_client_token(body, email)` (`apps/edge_api/src/services/documenso_client.py:96`) pulls the recipient signing token matched by email (lowercased/trimmed), falling back to the first recipient then `token`/`signingToken`. `_recipient_email_tokens` returns ALL `(email_lowercased, token)` pairs carrying a token so a caller can pick client vs originator on a multi-recipient document.

### B.6 Prospect signing route + reads

The SPA route is `/p/m/:opportunityId/:documentId` → `DocumentSignPage` (`rare-structure-hq:apps/platform-app/src/App.tsx:100`), with the payment variant `/p/m/:opportunityId/:documentId/pay` → `DocumentPaymentPage` (`rare-structure-hq:apps/platform-app/src/App.tsx:103`). Here `opportunityId` is the 8-char handle and `documentId` is the NUMERIC Documenso document id. Two backend reads drive it:

**Live, pair-gated sign-token read** — `GET /api/v1/documenso/sign-token/{opportunity_id}/{document_id}` (`apps/edge_api/src/routers/documenso_webhooks_v1.py:105`), `signer` query param defaults `'client'` (`apps/edge_api/src/routers/documenso_webhooks_v1.py:109`):

```text
doc = read_document(document_id)                  # one live GET /api/v2/document/{numeric}  # py:128
if (doc.external_id or '') != opportunity_id: 404 # PAIR GATE — guessed id leaks nothing     # py:135,140
token = doc.signing_token                         # single-signer fallback                   # py:143
if len(doc.recipient_tokens) > 1:
    contact = get_opportunity_contact_email(opportunity_id)  # py:147
    client_tok     = pair where email == contact            # py:150
    originator_tok = pair where email != contact            # py:151
    token = (originator_tok if signer=='originator' else client_tok) or token  # py:152
```

`read_document(document_id)` reads `GET /api/v2/document/{numeric}` (the prefixed `envelope_…` handle 400s there) and returns `external_id`, `envelope_id`, `status`, `signing_token` (first SIGNER), `recipient_tokens` (all pairs) (`apps/edge_api/src/services/documenso_client.py:569,580`; `DocumentReadResult` `:554-566`).

**Offline sign-state poll** — `GET /api/v1/documenso/sign-state/{opportunity_id}/{document_id}` (`apps/edge_api/src/routers/documenso_webhooks_v1.py:76`) is FULLY OFFLINE (ZERO Documenso calls, `apps/edge_api/src/routers/documenso_webhooks_v1.py:81-83`). `signed` is derived from raw `business.documenso_webhook_events` rows where `external_id = {opportunity_id} AND envelope_id = {document_id}` carry a terminal `DOCUMENT_COMPLETED` event (`apps/edge_api/src/documenso_webhooks/queries.py:41,69,71`). The captured `envelope_id` column holds the NUMERIC document id (`apps/edge_api/src/documenso_webhooks/queries.py:71`).

### B.7 Payment (same prefill source)

The document fee is resolved from the SAME source as the prefill: `opportunity_specific_content.field_values['fee_amount']`, parsed to integer cents by `resolve_fee_cents` (`FEE_KEY = 'fee_amount'`, `apps/edge_api/src/document_payments/amount.py:16,19,21`). `get_fee_and_contact` JOINs `business.opportunity_specific_content osc ON osc.opportunity_id = o.id` and resolves by the 8-char handle `WHERE o.opportunity_id = %s` (`apps/edge_api/src/document_payments/queries.py:41,47,51,53`). The charge therefore equals the signed fee.

---

## Section C — `embed-template` sub-lane (NEW, direct-link)

The `embed-template` lane is PARALLEL to `prefill-document-from-template` (which is left untouched, `apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:175`). The defining difference: NO Documenso document is minted at originate. Instead, edge_api enables a Documenso DIRECT LINK on the draft's template and hands the SPA the reusable `direct_token`; the prospect signs against the embed, self-identifies (enters their own name + email — these are NOT locked), and Documenso creates the document (source `TEMPLATE_DIRECT_LINK`) only AT completion.

### C.1 Route: `POST /api/v1/engagement-mandate-drafts/{draft_id}/originate-embed-template`

Service-token gated (`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:168-171`); accepts an optional `EmbedTemplateOriginateRequest` body whose only field, `direct_recipient_id`, overrides which template recipient the public signer assumes (`apps/edge_api/src/engagement_mandate_drafts/models.py:42-46`). Control flow:

```text
originate_embed_template(draft_id, body):                              # py:172
  draft = get_draft(conn, draft_id)        # ONLY for draft.documenso_template_id  # py:187 (404 if missing)
  opp   = get_opportunity_ref_and_contact(draft.opportunity_id)        # py:190 (no osc row required)
  if not opp: 404 "opportunity not found for this draft"               # py:191-192
  recipients          = get_template_recipients(template_id)           # py:196
  direct_recipient_id = body.direct_recipient_id or _pick_direct_recipient_id(recipients)  # py:197
  link = create_direct_link(template_id, direct_recipient_id=…)        # py:198-200
  if not link.token: 502 "documenso direct link returned no token"     # py:203-204
  return MandateEmbedTemplateOriginated(
           direct_token        = link.token,                           # py:212
           documenso_host      = host,                                 # py:213
           embed_url           = f"{host}/embed/direct/{link.token}",  # py:214
           external_id         = opp.opportunity_ref,                  # py:215 (8-char handle)
           opportunity_id      = opp.opportunity_ref,                  # py:216
           direct_recipient_id = link.direct_template_recipient_id …,  # py:217
           recipient_email     = opp.recipient_email,                  # py:218 (optional embed prefill)
           recipient_name      = opp.recipient_name,                   # py:219
           status              = 'ready')                              # py:220 (no doc until someone signs)
```

`DocumensoError` → `HTTPException(502, "documenso: {e}")` (`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:201-202`). The response model is `MandateEmbedTemplateOriginated` (`apps/edge_api/src/engagement_mandate_drafts/models.py:49-70`); `status` is `"ready"` (NOT `"pending"` — there is no document yet).

### C.2 Opportunity handle + contact: `get_opportunity_ref_and_contact`

`apps/edge_api/src/engagement_mandate_drafts/queries.py:139`. UNLIKE the prefill lane's `get_opportunity_prefill_and_contact`, this does NOT require an `opportunity_specific_content` row — the self-serve embed needs no prefilled per-deal values, only the PUBLIC 8-char handle to stamp as `externalId` plus the contact for optional embed prefill (`apps/edge_api/src/engagement_mandate_drafts/queries.py:140-145`). UUID-guarded (`apps/edge_api/src/engagement_mandate_drafts/queries.py:146-149`). Returns `{"opportunity_ref", "recipient_email", "recipient_name"}` or `None` when the opportunity is unknown / not a UUID (`apps/edge_api/src/engagement_mandate_drafts/queries.py:165-169`).

### C.3 Direct-recipient designation: `_pick_direct_recipient_id`

The public direct-link signer assumes the COUNTERPARTY recipient — the recipient that is NOT the Rare Structure provider. `_pick_direct_recipient_id` (`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:43`) is a best-effort, overridable heuristic: prefer an explicit `participant`/`client` recipient, else the first non-provider (`provider`/`rare structure`/`crane` in email or name), else the first recipient (`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:58-64`). The request body's `direct_recipient_id` wins when supplied (`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:197`).

### C.4 Documenso direct link: `create_direct_link` / `get_template_recipients`

`get_template_recipients` reads the template's recipients (id/email/name/role) via `GET /api/v2/template/{id}` (`apps/edge_api/src/services/documenso_client.py:504`). `create_direct_link` enables the link via `POST /api/v2/template/direct/create {templateId, directRecipientId?}` and returns a `DirectLinkResult` (`apps/edge_api/src/services/documenso_client.py:514,529`; model `:468-477`). It is IDEMPOTENT: if a direct link already exists (`/create` 4xx), it recovers the existing token via `POST /api/v2/template/direct/toggle {enabled:true}` (`apps/edge_api/src/services/documenso_client.py:530-537`); a standalone `toggle_direct_link(enabled)` (`apps/edge_api/src/services/documenso_client.py:542`) enables/disables it. The numeric template id required by `/template/direct/*` is extracted by `_template_id_number` (tolerates a prefixed handle, `apps/edge_api/src/services/documenso_client.py:480-489`).

The `token` value is load-bearing in THREE places at once (`apps/edge_api/src/services/documenso_client.py:459-465`): the API-response token, the `<EmbedDirectTemplate token=…>` prop the SPA mounts, the public `/d/{token}` URL, and the iframe `/embed/direct/{token}`. The SPA route is `/p/t/:opportunityId/:directToken` with `?host=` → `DirectTemplateSignPage` → `EmbedDirectTemplate` (cross-repo, rare-structure-hq); the signer enters their own name + email (NOT locked). At completion the embed's `onDocumentCompleted` surfaces the numeric document id; the existing offline `/sign-state/{opportunity_id}/{document_id}` poll (`apps/edge_api/src/routers/documenso_webhooks_v1.py:76`) then tracks it, gated by `externalId == opportunity_id` (`apps/edge_api/src/engagement_mandate_drafts/models.py:57-60`).

### C.5 No document until completion

No row, no envelope, no `field/update-many` lock, no per-document signing token at originate. The direct link lives on the TEMPLATE; the document is born when the signer completes (source `TEMPLATE_DIRECT_LINK`), so the durable anchor is again the webhook capture (`externalId` = 8-char handle + numeric document id). `typedSignatureEnabled` / `drawSignatureEnabled` / `uploadSignatureEnabled` are document/template-level meta settings on the Documenso side, not flags this lane passes per-request.

---

## Section D — Cross-repo handoffs (SPA → BFF → edge_api)

The architecture invariant: `platform-app` → `platform-api` (dumb BFF) → `edge_api`.

The edge_api routes below are verified from THIS repo; the SPA/BFF columns are cross-repo (rare-structure-hq) and correct only what THIS repo's contracts pin down.

| Operation | SPA call | BFF route | BFF edge client | edge_api route |
|---|---|---|---|---|
| Staging save (all lanes) | — | `PUT .../by-opportunity/:opportunityId` `requireUser` (cross-repo) | `edgeUpsertStagingDraft` (cross-repo) | `PUT .../by-opportunity/{opportunity_id}` (`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:90`) |
| Originate (prefill-document-from-template) | `originatePrefilled` (cross-repo) | `POST /:id/originate-prefilled` `requireUser` (cross-repo) | `edgeOriginatePrefilled` (cross-repo) | `POST .../{draft_id}/originate-prefilled` (`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:109`) |
| Originate (embed-template) | `originateEmbedTemplate` (cross-repo) | `POST /:id/originate-embed-template` `requireUser` (cross-repo) | BFF embed-template edge client (cross-repo) | `POST .../{draft_id}/originate-embed-template` (`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:168`) |
| PUBLIC sign-token (prefill lane) | `getMandateSignToken` (cross-repo) | `GET /sign/:opportunityId/:documentId/token` (cross-repo) | `edgeGetSignToken` (cross-repo) | `GET /api/v1/documenso/sign-token/{opp}/{doc}` (`apps/edge_api/src/routers/documenso_webhooks_v1.py:105`) |

Notes:
- The retired `envelope-distribute` `/confirm` handoff (BFF `POST /:id/confirm`, `edgeConfirmMandateDraft`, edge_api `POST .../{draft_id}/confirm`) and the `GET /document/:envelopeId` prospect read are GONE on the edge_api side — there is no `/confirm` or `/document/{envelope_id}` route in `apps/edge_api/src/routers/engagement_mandate_drafts_v1.py`. Any surviving BFF client is dead (verify + prune in rare-structure-hq separately).
- The embed-template BFF must thread the Documenso `host` through to the SPA `/p/t/...?host=` route so `EmbedDirectTemplate` mounts against the right instance (cross-repo, rare-structure-hq).
- The BFF mounts `documensoPublicRoutes` under BOTH `/api/v1/documenso` AND, transitionally, `/api/v1/engagement-mandate-drafts`, then mounts `engagementMandateDraftRoutes` under `/api/v1/engagement-mandate-drafts` — same prefix, Hono first-match (cross-repo, rare-structure-hq).

---

## Section E — Shared/supporting data elements

| Element | Kind | Owner | Status | Citation |
|---|---|---|---|---|
| `business.engagement_mandate_draft_content` | table | UPSTREAM (hq-x) | ACTIVE | `apps/edge_api/sql/engagement_mandate_draft_content.sql:5` (ALTER-only, NO `CREATE TABLE`) |
| `…draft_content.prefill_values` | column | edge_api | ACTIVE | `apps/edge_api/sql/engagement_mandate_draft_content.sql:20` (`jsonb NOT NULL DEFAULT '{}'`) |
| `…draft_content.archetype_id` | column | edge_api | ACTIVE | `apps/edge_api/sql/engagement_mandate_draft_content.sql:25` (FK `:36` ON DELETE RESTRICT) |
| `…draft_content.status` | column | UPSTREAM | ACTIVE | `apps/edge_api/sql/engagement_mandate_draft_content.sql:13` (default `'draft'`, pre-exists) |
| `business.opportunity_specific_content` | table | UPSTREAM | ACTIVE | read-only from both repos; NO writer/DDL anywhere (`apps/edge_api/src/engagement_mandate_drafts/queries.py:150`) |
| `business.opportunities.opportunity_id` | column | edge_api (ALTER) | ACTIVE | `apps/edge_api/sql/opportunities_opportunity_id.sql:19-21` (generated 8-char handle) |
| `operator_settings.direct_to_documenso_lane` | column | edge_api | ACTIVE | `apps/edge_api/sql/operator_settings.sql:43` (DEFAULT `'prefill-document-from-template'`; CHECK `:84-89`) |
| `business.global_input_content` | table | edge_api | ACTIVE | content-source REGISTRY for the render+push lane; gained `brand` + `source_kind` cols (`apps/edge_api/sql/global_input_content.sql:21,31,32`; CHECK `:43-45`); seeds AO term-only + RS capital-origination `:53-55` |
| `ops.engagement_template_push_runs` | table | edge_api | ACTIVE | render+push QA ledger (one row per attempt, success/error) `apps/edge_api/sql/ops_engagement_template_push_runs.sql:12` |
| `DOCUMENSO_API_URL` | env | edge_api | ACTIVE | `apps/edge_api/src/config.py:34,37` (default `https://app.documenso.com`, trailing `/` stripped) |
| `DOCUMENSO_API_KEY` | env | edge_api | ACTIVE | `apps/edge_api/src/config.py:31` (format `api_…`; unset → `DocumensoError`) |

---

## Section F — Render+push lane (engagement-template provisioning)

Distinct from the originate lanes above: this is how an engagement TEMPLATE is provisioned IN Documenso in the first place (content source → DocRaptor PDF → Documenso TEMPLATE), so the originate lanes have a `documenso_template_id` to point at. It runs on the trigger-secret `/internal/*` contract, NOT the prospect/service-token surface.

### F.1 Brand-aware content catalog

`apps/edge_api/src/engagement_templates/catalog.py` resolves a template under `content/<brand>/<path>/<archetype>/<version>/global_engagement_content/manifest.json` (`apps/edge_api/src/engagement_templates/catalog.py:4,22,99`). `brand` is the first segment, defaulting to `active-operators` so the original three-segment call sites keep working (`apps/edge_api/src/engagement_templates/catalog.py:10,21`); `_ALLOWED_BRANDS = {active-operators, rare-structure}` (`apps/edge_api/src/engagement_templates/catalog.py:28`). The new RS brand asset tree is `content/rare-structure/docraptor-to-documenso-template/capital-origination/v1/global_engagement_content` (static-blank HTML, `plain` + `branded` stylesheets; `manifest.json` slug `rare_structure_strategic_origination`, archetype `capital_origination`). The live Documenso template id for this RS asset is **14310** (live-Documenso fact, not a repo artifact).

### F.2 Content-source registry: `business.global_input_content`

`business.global_input_content` is the content-source REGISTRY (`apps/edge_api/sql/global_input_content.sql:21`). It gained two columns: `brand` (`'active-operators' | 'rare-structure'`, default `active-operators`, `apps/edge_api/sql/global_input_content.sql:31`) and `source_kind` (`'repo-html' | 'db-markdown'`, default `repo-html`, CHECK `apps/edge_api/sql/global_input_content.sql:32,43-45`). `repo-html` resolves under `content/<brand>/…`; `db-markdown` resolves `business.global_engagement_content WHERE slug = path` (`apps/edge_api/sql/global_input_content.sql:11-12`). Seeded with the AO term-only and RS capital-origination repo-html rows (`apps/edge_api/sql/global_input_content.sql:53-55`).

### F.3 Render+push route + Trigger.dev task

`POST /internal/engagement-templates/render-push` (`apps/edge_api/src/routers/internal_engagement_templates_v1.py:84`) is gated by `require_trigger_secret` (TRIGGER_SHARED_SECRET, `apps/edge_api/src/routers/internal_engagement_templates_v1.py:24,84`). It resolves a descriptor either from a registry row (`registryPath`/`registryId` → `business.global_input_content`) or from explicit `brand`/`path`/`archetype`/`version` fields (`apps/edge_api/src/routers/internal_engagement_templates_v1.py:45-81`), then calls `push.render_and_push` (render → DocRaptor → Documenso TEMPLATE via `create_template_from_pdf` → `POST /api/v2/envelope/create type=TEMPLATE`, `apps/edge_api/src/services/documenso_client.py:420,437,440`) and records a terminal row in `ops.engagement_template_push_runs` on EVERY outcome (success or error, `apps/edge_api/src/routers/internal_engagement_templates_v1.py:108-123,145-158`). The route is called by the `engagement-template-push` Trigger.dev task via `callHqx` (`src/trigger/engagement_template_push.ts:53,78-79`).

---

## Status: ACTIVE / CONDITIONAL / DEPRECATED / STUB

- **ACTIVE (DEFAULT) — `prefill-document-from-template` sub-lane** (`POST .../originate-prefilled`): the DB default; routes new and row-less operators here. The canonical, fully-instrumented path (locks fields, builds the `/p/m` pair link, wires payment). `apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:109`; DEFAULT `apps/edge_api/sql/operator_settings.sql:43`.
- **ACTIVE (NEW) — `embed-template` sub-lane** (`POST .../originate-embed-template`): runs when `direct_to_documenso_lane = 'embed-template'`. Enables a Documenso direct link on the template and returns its `direct_token`; NO document at originate — Documenso mints it at signer completion. `apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:168`.
- **ACTIVE — direct-link client fns** (`create_direct_link`, `toggle_direct_link`, `get_template_recipients`): the embed-template Documenso plumbing (`POST /api/v2/template/direct/{create,toggle}`). `apps/edge_api/src/services/documenso_client.py:504,514,542`.
- **ACTIVE — staging routes** (`PUT/GET/POST .../by-opportunity`, `POST ""`): shared prep-page plumbing for the prefill and embed lanes. `apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:90,80,67`.
- **ACTIVE — PUBLIC sign-token + sign-state reads**: the prospect surface (live token for the prefill lane + offline poll, which the embed lane also tracks at completion). `apps/edge_api/src/routers/documenso_webhooks_v1.py:105,76`.
- **ACTIVE — render+push lane** (`POST /internal/engagement-templates/render-push`, trigger-secret): content source → DocRaptor PDF → Documenso TEMPLATE (`create_template_from_pdf`). `apps/edge_api/src/routers/internal_engagement_templates_v1.py:84`; `apps/edge_api/src/services/documenso_client.py:420`.
- **RETIRED — `envelope-distribute` sub-lane**: code path REMOVED. No `/confirm` route, no `create_document_from_template_with_custom_pdf` function, no `GET .../document/{envelope_id}` read, no `MandateDraftConfirmed`/`MandateDraftDocument` models. The CHECK literal is retained so a pre-existing row never violates it (`apps/edge_api/sql/operator_settings.sql:80,84-89`).

---

## Traps (do not be misled)

- **T1 — staged `prefill_values` keying drift (LABEL vs NAME), and it feeds no live originate lane.** The DDL comment at `apps/edge_api/sql/engagement_mandate_draft_content.sql:7-8,18` says `prefill_values` is keyed by the template's text_field NAME; the `get_draft` query comment says LABEL and that "Confirm passes these through" (`apps/edge_api/src/engagement_mandate_drafts/queries.py:59-60`) — but the `/confirm` path it refers to is RETIRED. The two LIVE originate lanes read `business.opportunity_specific_content.field_values`, NOT this staged `prefill_values`. So the staged column is now staging-only (written by `upsert_staging`, `apps/edge_api/src/engagement_mandate_drafts/queries.py:172`); the NAME-vs-LABEL drift in its comments is no longer load-bearing on an originate path. Treat the `queries.py` "Confirm passes these through" wording as stale.

- **T2 — RETIRED lane has no surface at all.** Do not look for `envelope-distribute`'s `/confirm` route, `create_document_from_template_with_custom_pdf` client, `GET .../document/{envelope_id}` read, or `MandateDraftConfirmed`/`MandateDraftDocument` models — all REMOVED from this repo. Any lingering `edgeGetMandateDraftDocument` BFF client or `/p/m/:envelopeId` JSDoc in rare-structure-hq is dead (verify + prune cross-repo).

- **T3 — `externalId` is the 8-char handle, not the full UUID.** The `DocumentReadResult`/`read_document` docstrings now correctly say "the opportunity's 8-char handle stamped at originate" (`apps/edge_api/src/services/documenso_client.py:557,562,574`). The value stamped is `opportunity_ref`, the public 8-char handle (`apps/edge_api/src/engagement_mandate_drafts/queries.py:135`), and the pair-gate compares `external_id` to it (`apps/edge_api/src/routers/documenso_webhooks_v1.py`). Any cross-repo SPA comment still calling it "the opportunity UUID" is stale prose; the route param and gate are correct.

- **T4 — DEFAULT lane IS the canonical lane now.** The DB DEFAULT is `prefill-document-from-template` (`apps/edge_api/sql/operator_settings.sql:43`), which is also the canonical, fully-wired path. `embed-template` is a parallel, conditional lane. `envelope-distribute` is RETIRED. Do not treat the historical "default ≠ canonical" framing as current.

- **T5 — prefill vs embed: document NOW vs document AT completion.** `prefill-document-from-template` uses `POST /api/v2/template/use` (recipients REQUIRED) and mints a document at originate (`apps/edge_api/src/services/documenso_client.py:336`). `embed-template` uses `POST /api/v2/template/direct/create` and mints NO document at originate — Documenso creates it at signer completion (source `TEMPLATE_DIRECT_LINK`, `apps/edge_api/src/services/documenso_client.py:529`). Do not assume an `embed-template` originate produced a signable document id; there is none until someone signs.

- **T6 — numeric template id is NOT an envelope id.** The DB carries only the numeric Documenso template id; it 400s on envelope endpoints. The prefill lane reads the template directly via `GET /api/v2/template/{id}` (`apps/edge_api/src/services/documenso_client.py:260`); the embed lane's `/template/direct/*` calls take the numeric id, extracted by `_template_id_number` (tolerates a prefixed handle, `apps/edge_api/src/services/documenso_client.py:480-489`).

- **T7 — prefill fan-out: every matching field, not 1:1.** The prefill lane fans out a label to EVERY matching field id (`apps/edge_api/src/services/documenso_client.py:290,308-313`) and supports base-name splits (e.g. `participant_company_one`/`_two` drawing from `participant_company`, `apps/edge_api/src/services/documenso_client.py:301-313`). The embed lane does no prefill at originate — the signer fills the template directly.

- **T8 — base-table columns are upstream-owned.** `…draft_content.updated_at`, `created_at`, `organization_id`, `opportunity_id`, `documenso_template_id` are referenced by queries (e.g. `apps/edge_api/src/engagement_mandate_drafts/queries.py:46`) but are NOT added by edge_api DDL (which adds only `prefill_values` and `archetype_id`). They must pre-exist on the upstream base table; drift would fail writes loudly. Base-table type/default definitions are UNVERIFIED (owned by hq-x, not in edge_api DDL).
