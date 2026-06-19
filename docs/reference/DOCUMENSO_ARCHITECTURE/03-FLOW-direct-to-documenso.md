# Flow: `direct-to-documenso` (both sub-lanes)

> **STATUS BANNER.** This file documents `operator_settings.render_mode = 'direct-to-documenso'` and BOTH of its sub-lanes, selected by `operator_settings.direct_to_documenso_lane`:
> - **`envelope-distribute`** — the DB-DEFAULT direct lane (`POST .../{draft_id}/confirm`, `POST /api/v2/envelope/use`, prefill from `engagement_mandate_draft_content.prefill_values`, `externalId = draft_id` (UUID), returns only `envelope_id` → **cannot build a `/p/m` link**).
> - **`prefill-document-from-template`** — the CANONICAL, fully-instrumented direct lane (`POST .../{draft_id}/originate-prefilled`, `POST /api/v2/template/use`, prefill from `opportunity_specific_content.field_values`, `externalId = opportunity` 8-char handle, returns the `(opportunity_id, document_id)` pair → builds `/p/m/{opp}/{doc}`).
>
> The sibling `through-docraptor` mode is OUT OF SCOPE here. Where this file disagrees with `docs/reference/DIRECT_TO_DOCUMENSO_PAYMENT_E2E.md`, **the code wins** (that doc was not relied upon).

## Orientation

A fresh agent reading this: `direct-to-documenso` is the originate pathway that SKIPS DocRaptor PDF rendering and instantiates a signable Documenso document straight from a stored Documenso template. There are TWO mutually-exclusive sub-lanes under it; the choice is a per-operator setting, NOT a per-request flag, and the dispatch happens **in the SPA**, not in `edge_api`. Both sub-lanes live in the SAME router (`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py`) and call the SAME Documenso instance with `distributionMethod:'NONE'` (no email; the consumer app delivers the signing link). The two lanes differ on almost every other axis — entrypoint route, prefill source, recipient binding, `externalId` value, readOnly locking, response shape, and crucially whether the SPA can build a live signing link. The default lane (`envelope-distribute`) is the older path and is structurally **link-incapable**; the newer `prefill-document-from-template` lane is the one that actually wires the prospect-facing `/p/m/...` signing + payment surfaces. Read the divergence table first; it is the spine of this document.

---

## Divergence Table — `envelope-distribute` vs `prefill-document-from-template`

| Dimension | `envelope-distribute` (DEFAULT) | `prefill-document-from-template` (CANONICAL) |
|---|---|---|
| Lane status | ACTIVE (DB default) | CONDITIONAL (only when lane set explicitly) |
| Setting value | `direct_to_documenso_lane = 'envelope-distribute'` (`apps/edge_api/sql/operator_settings.sql:42`, CHECK `:80`) | `direct_to_documenso_lane = 'prefill-document-from-template'` (`apps/edge_api/sql/operator_settings.sql:80`) |
| edge_api entrypoint | `POST /api/v1/engagement-mandate-drafts/{draft_id}/confirm` (`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:82`) | `POST /api/v1/engagement-mandate-drafts/{draft_id}/originate-prefilled` (`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:109`) |
| Auth | service-token (`Depends(require_service_token)`) `:82` | service-token (`Depends(require_service_token)`) `:111` |
| Documenso client fn | `create_document_from_template_with_custom_pdf` (`apps/edge_api/src/services/documenso_client.py:321`) | `create_document_from_template` (`apps/edge_api/src/services/documenso_client.py:427`) |
| Documenso instantiate endpoint | `POST /api/v2/envelope/use` (`documenso_client.py:381`) | `POST /api/v2/template/use` (`documenso_client.py:535`) |
| Prefill SOURCE | `business.engagement_mandate_draft_content.prefill_values` via `get_staged_prefill_values` (`engagement_mandate_draft_content.sql:20`; `queries.py:65`) | `business.opportunity_specific_content.field_values` via `get_opportunity_prefill_and_contact` (`queries.py:127,146,150`) |
| Recipient binding | OMITTED — template's stored recipient defaults (`documenso_client.py:378`; `engagement_mandate_drafts_v1.py:96` passes none) | EXPLICIT — placeholder recipient overridden with opportunity contact email/name (`documenso_client.py:474,518`) |
| `externalId` stamped | `draft_id` (a UUID) (`engagement_mandate_drafts_v1.py:97`) | opportunity PUBLIC 8-char handle `opportunity_id` (`engagement_mandate_drafts_v1.py:150`) |
| readOnly field lock | NONE (no `field/update-many` call) (`documenso_client.py:321-403`) | YES — derived fields locked via `POST /api/v2/envelope/field/update-many` (`documenso_client.py:575`) |
| Prefill label→field map | first field per label (1:1) (`documenso_client.py:367`) | fan-out — every field carrying the label, + base-name split (`documenso_client.py:496,508`) |
| Title override | NONE | `override.title = 'Engagement Agreement'` (`engagement_mandate_drafts_v1.py:153`; `documenso_client.py:533`) |
| Token extraction | role-based first SIGNER `_extract_signer_token` (`documenso_client.py:402,129`) | email-matched `_extract_client_token(body, recipient_email)` (`documenso_client.py:547,110`) |
| Response model | `MandateDraftConfirmed` = `{envelope_id, signing_token, documenso_host}` (`models.py:21-28`) | `MandatePrefilledOriginated` = `{envelope_id, document_id, opportunity_id, signing_token, status, documenso_host}` (`models.py:40-58`) |
| Returns `(opp, doc)` pair? | NO — only `envelope_id` (`engagement_mandate_drafts_v1.py:102-106`) | YES — `opportunity_id` + `document_id` (`engagement_mandate_drafts_v1.py:157-166`) |
| Builds SPA signing link? | NO — `setSignLink(null)` (`rare-structure-hq:apps/platform-app/src/proposals/MandateDraftShell.tsx:105`) | YES — `/p/m/{opportunity_id}/{document_id}` (`rare-structure-hq:apps/platform-app/src/proposals/MandateDraftShell.tsx:98-99`) |
| Prospect read surface | PUBLIC `GET .../document/{envelope_id}` (single handle, LIVE) (`engagement_mandate_drafts_v1.py:169`) | PUBLIC `GET .../sign-token/{opp}/{doc}` (LIVE, pair-gated) + `GET .../sign-state/{opp}/{doc}` (OFFLINE poll) (`documenso_webhooks_v1.py:105,76`) |
| SPA dispatch branch | `else` branch of `MandateDraftShell.confirm` (`...MandateDraftShell.tsx:101-105`) | `if (directToDocumensoLane === 'prefill-document-from-template')` (`...MandateDraftShell.tsx:93-100`) |
| Persists envelope/token? | NO — STATELESS by design (`engagement_mandate_drafts_v1.py:14-16`; `models.py:22-24`) | NO — durable anchor is the webhook capture (`externalId`+numeric doc id) |

---

## Section A — Selection & dispatch

### A.1 The setting (edge_api owns the DB)

`operator_settings.direct_to_documenso_lane` is `text NOT NULL DEFAULT 'envelope-distribute'` (`apps/edge_api/sql/operator_settings.sql:42`), CHECK-constrained to `{'envelope-distribute','prefill-document-from-template'}` (`apps/edge_api/sql/operator_settings.sql:80`). It is a SECOND, INDEPENDENT sub-selector that applies **only** when `render_mode = 'direct-to-documenso'`; it is ignored under `render_mode = 'through-docraptor'` (SQL comment `apps/edge_api/sql/operator_settings.sql:23-34`). `render_mode` itself is CHECK-constrained to `{'through-docraptor','direct-to-documenso'}` (`apps/edge_api/sql/operator_settings.sql:68`).

The lane→endpoint mapping is documented in the SQL comments (`apps/edge_api/sql/operator_settings.sql:26-33`) but is NOT enforced in SQL — it is enforced by the SPA dispatch (below).

Shared platform types mirror the enum: `RenderMode = 'through-docraptor' | 'direct-to-documenso'` (`rare-structure-hq:packages/shared/src/schemas/settings.ts:10`) and `DirectToDocumensoLane = 'envelope-distribute' | 'prefill-document-from-template'` (`rare-structure-hq:packages/shared/src/schemas/settings.ts:27`), with the comment "Only meaningful when renderMode === 'direct-to-documenso'" at `rare-structure-hq:packages/shared/src/schemas/settings.ts:51`.

### A.2 Dispatch is in the SPA, not edge_api

`MandateDraftShell.confirm()` branches on the operator's lane (`rare-structure-hq:apps/platform-app/src/proposals/MandateDraftShell.tsx:93`):

```text
async function confirm():
  if directToDocumensoLane === 'prefill-document-from-template':
      res = await originatePrefilled(token, draftId)          # tsx:94
      if res.documentId == null: throw                        # tsx:95-96
      link = { opportunityId: res.opportunityId,
               documentId:    res.documentId }                 # tsx:98
      saveSignLink(draftId, link); setSignLink(link)          # tsx:99-100
  else:  # envelope-distribute
      # cannot build /p/m/{opp}/{doc}: stamps externalId=draftId,
      # returns no document id  (in-code comment tsx:102-103)
      await confirmMandateDraft(token, draftId)               # tsx:104
      setSignLink(null)                                        # tsx:105
```

The else-branch's in-code comment (`rare-structure-hq:apps/platform-app/src/proposals/MandateDraftShell.tsx:102-103`) states the limitation verbatim: envelope-distribute "stamps externalId=draftId (not the opportunity pair) and does not return the document id, so it cannot build the /p/m/{opportunity}/{document} links."

---

## Section B — `envelope-distribute` sub-lane (DEFAULT)

### B.1 Route: `POST /api/v1/engagement-mandate-drafts/{draft_id}/confirm`

Service-token gated (`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:82`). Control flow:

```text
confirm_mandate_draft(draft_id):                              # py:83
  draft = get_draft(conn, draft_id)                           # py:89  (404 if missing)
  # per-deal values come from the opportunity's STAGED draft,
  # NOT this freshly-minted confirm draft                     # py:92-93
  prefill_values = get_staged_prefill_values(draft.opportunity_id)  # py:94
  result = create_document_from_template_with_custom_pdf(
             draft.documenso_template_id,
             external_id = draft_id,          # ← the UUID     # py:97
             prefill_values = prefill_values) # py:96-99
  return MandateDraftConfirmed(
           envelope_id   = result.envelope_id,                # py:103
           signing_token = result.client_token,               # py:104
           documenso_host= config.documenso_api_url())        # py:105
```

`result.document_id` IS computed by the client (`apps/edge_api/src/services/documenso_client.py:401`) but is DROPPED at the route boundary — `MandateDraftConfirmed` carries no `document_id`/`opportunity_id` (`apps/edge_api/src/engagement_mandate_drafts/models.py:26-28`). This is the structural reason the lane cannot build the `(opp,doc)` pair link.

`DocumensoError` → `HTTPException(502, "documenso: {e}")` (`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:100-101`).

### B.2 Prefill source: STAGED `prefill_values`

`get_staged_prefill_values` (`apps/edge_api/src/engagement_mandate_drafts/queries.py:65`) selects `prefill_values` from the latest draft for the opportunity that has NON-EMPTY values: `WHERE opportunity_id = %s::uuid AND prefill_values IS NOT NULL AND prefill_values <> '{}'::jsonb ORDER BY updated_at DESC LIMIT 1` (`apps/edge_api/src/engagement_mandate_drafts/queries.py:84-88`). It UUID-guards the ref (garbage → `{}`, `apps/edge_api/src/engagement_mandate_drafts/queries.py:76-78`) and returns `{}` when nothing staged (`apps/edge_api/src/engagement_mandate_drafts/queries.py:93`). The `ORDER BY updated_at DESC` makes it order-independent (stage-then-originate and originate-then-stage both resolve to the row carrying values).

`prefill_values` is keyed by Documenso field **LABEL** at runtime (e.g. `{"Engagement Fee": "$35,000"}`, `apps/edge_api/src/engagement_mandate_drafts/queries.py:59-60`). See **Trap T1** — the DDL comment claims keying by field NAME; the code/label keying is authoritative.

### B.3 Documenso instantiation: `create_document_from_template_with_custom_pdf`

`apps/edge_api/src/services/documenso_client.py:321`. Steps:

```text
1) resolve numeric template id → prefixed envelopeId:
   GET /api/v2/template/{id} → .envelopeId   (_resolve_template_envelope_id, py:303,311,313)  # called py:355
2) if prefill_values: build label→(field id, lowercased type) map from the template envelope:
   GET /api/v2/envelope/{envelopeId} → fields[].fieldMeta.label  # py:363,366,368
   prefillFields = [{id, type(lowercased), value}]               # py:369-375
   - first field per label only (lab not in by_label)            # py:367   ← 1:1, no fan-out
   - skip labels with no match or empty value                    # py:372
3) POST /api/v2/envelope/use  (multipart 'payload' JSON part, NO files,
   recipients OMITTED → template's stored recipient defaults)    # py:380-383
4) POST /api/v2/envelope/distribute  meta.distributionMethod=NONE (no email) # py:390-392
5) GET /api/v2/envelope/{envelope_id} → read signer token + numeric document id # py:397
```

`envelope_id` is read off the `/envelope/use` response itself (`apps/edge_api/src/services/documenso_client.py:385`); the subsequent `GET /api/v2/envelope/{envelope_id}` (`apps/edge_api/src/services/documenso_client.py:397`) supplies the signer token and numeric document id. Result = `EnvelopeResult(envelope_id, document_id=_numeric_document_id(env), client_token=_extract_signer_token(env))` (`apps/edge_api/src/services/documenso_client.py:399-403`).

Type values are LOWERCASED (`text`/`number`); an upper-case type 400s (`apps/edge_api/src/services/documenso_client.py:359-361`). This lane does NOT lock fields readOnly and does NOT place SIGNATURE/DATE.

### B.4 Recipients omitted → role-based token

Because `/envelope/use` is called with no `recipients` (`apps/edge_api/src/services/documenso_client.py:350-351`, never set by `/confirm` at `apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:96`), the document instantiates with the template's stored recipient defaults (email may be blank, `apps/edge_api/src/services/documenso_client.py:378`). The signer token is therefore selected by ROLE, not email: `_extract_signer_token` picks the recipient with `str(role).upper() == 'SIGNER'`, falling back to the first recipient (`apps/edge_api/src/services/documenso_client.py:129,142,143`), returning `token`/`signingToken` or `None` (`apps/edge_api/src/services/documenso_client.py:145-146`).

### B.5 Public prospect read (single handle)

`GET /api/v1/engagement-mandate-drafts/document/{envelope_id}` — NO service-token dependency, PUBLIC (`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:169`). It calls `read_template_document(envelope_id)` (`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:174`), which does a LIVE `GET /api/v2/envelope/{envelope_id}` (`apps/edge_api/src/services/documenso_client.py:597,601`) and returns `(_extract_signer_token(env), env.status)` (`apps/edge_api/src/services/documenso_client.py:602`). The route returns `MandateDraftDocument{signing_token, documenso_host, status}` (`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:177-181`; model `apps/edge_api/src/engagement_mandate_drafts/models.py:31-37`). `DocumensoError` → `HTTPException(404, "document not found")` (`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:176`). The `envelope_id` is the only bearer capability, and the read is LIVE on EVERY call.

**No live SPA signing surface consumes this endpoint** (see Trap T2).

### B.6 Stateless by design

The router module docstring states it explicitly: "STATELESS by design: no envelope/token columns are added to the draft. The envelope id carried in the prospect link is the only handle back to the document (re-confirm mints a fresh one)" (`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:14-16`; echoed in `apps/edge_api/src/engagement_mandate_drafts/models.py:22-24`). Re-confirming mints a FRESH document.

---

## Section C — `prefill-document-from-template` sub-lane (CANONICAL)

### C.1 Route: `POST /api/v1/engagement-mandate-drafts/{draft_id}/originate-prefilled`

Service-token gated (`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:109-111`), documented as PARALLEL to `/confirm`, leaving it untouched (`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:114`). Control flow:

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

### C.2 Prefill source + recipient: `get_opportunity_prefill_and_contact`

`apps/edge_api/src/engagement_mandate_drafts/queries.py:127`. UUID-guarded (`apps/edge_api/src/engagement_mandate_drafts/queries.py:139-142`). One SELECT/JOIN:

- `osc.field_values` FROM `business.opportunity_specific_content osc` (`apps/edge_api/src/engagement_mandate_drafts/queries.py:146,150`) — label→value per-deal values, returned as `field_values` (`apps/edge_api/src/engagement_mandate_drafts/queries.py:161`).
- recipient from `JOIN business.opportunities o ON o.id = osc.opportunity_id` then `LEFT JOIN business.contacts c ON c.id = o.contact_id`, returning `c.email` and `NULLIF(TRIM(CONCAT_WS(' ', c.first_name, c.last_name)),'')` (`apps/edge_api/src/engagement_mandate_drafts/queries.py:147,148,151,152`).
- `opportunity_ref = o.opportunity_id or str(opportunity_id)[:8]` — never null (`apps/edge_api/src/engagement_mandate_drafts/queries.py:166`).

`None` when no `opportunity_specific_content` row exists or the ref is not a UUID (`apps/edge_api/src/engagement_mandate_drafts/queries.py:158-159`).

### C.3 `externalId` = the public 8-char handle

`opportunities.opportunity_id` is a GENERATED column: `ALTER TABLE business.opportunities ADD COLUMN IF NOT EXISTS opportunity_id text GENERATED ALWAYS AS (LEFT(id::text, 8)) STORED` (`apps/edge_api/sql/opportunities_opportunity_id.sql:19-21`), with a non-unique BTREE index `idx_opportunities_opportunity_id` (`apps/edge_api/sql/opportunities_opportunity_id.sql:27-28`). It is the first 8 chars of the row UUID — the externally-visible opportunity id stamped as `externalId` (`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:148-150`) and carried in the prospect link. `business.opportunities` is upstream-owned; this DDL is ALTER-only.

### C.4 Documenso instantiation: `create_document_from_template`

`apps/edge_api/src/services/documenso_client.py:427`. Steps:

```text
1) GET /api/v2/template/{id} → fields[] + recipients[] (resolved LIVE)  # py:459
2) RECIPIENT BIND: select the PLACEHOLDER recipient (no email set),
   else SIGNER-role, else recips[0]; capture recipient_id              # py:474-481
3) PREFILL FAN-OUT: by_label[label] = [(field id, lowercased type), …] # py:489,496
   - one label may sit on MULTIPLE fields → one prefillFields entry per field id  # py:507-512
   - value resolved by _prefill_value_for_label: exact key first, else BASE name (label.rsplit('_',1)[0])  # py:508,183,190-191
4) POST /api/v2/template/use  json payload:                            # py:535
   - recipients=[{id, email, name}]  (REQUIRED on /template/use)       # py:518
   - override.title = title[:255]  (when title provided)               # py:533
   - externalId, distributeDocument:false
   read body → envelope_id (body.envelopeId), numeric document_id (body.id)  # py:541-542
   token = _extract_client_token(body, recipient_email)  # email-matched     # py:547
5) READONLY LOCK on the DERIVED document:                              # py:549
   GET /api/v2/envelope/{envelope_id} → new_fields                     # py:555
   identify prefilled fields by non-empty value (TEXT→fieldMeta.text, NUMBER→fieldMeta.value)  # py:562-563
   POST /api/v2/envelope/field/update-many  readOnly:true              # py:566,575-577
6) POST /api/v2/envelope/distribute  meta.distributionMethod=NONE      # py:584-586
return EnvelopeResult(envelope_id, document_id (int|None), client_token=token)  # py:590-593
```

A recipient that already carries an email (e.g. the originator added via "Add Myself") is OMITTED from `recipients[]` and keeps its template default — this supports two-recipient documents (`apps/edge_api/src/services/documenso_client.py:463-468`). readOnly cannot be set at instantiation, so the lock MUST be applied on the derived document where prefilled values satisfy Documenso's "read-only must have text" rule; SIGNATURE/DATE carry no value and stay open for the signer (`apps/edge_api/src/services/documenso_client.py:449-454`).

### C.5 Per-signer token extraction

`_extract_client_token(body, email)` (`apps/edge_api/src/services/documenso_client.py:110`) pulls the recipient signing token matched by email (lowercased/trimmed, `apps/edge_api/src/services/documenso_client.py:121`), falling back to the first recipient (`apps/edge_api/src/services/documenso_client.py:124`) then `token`/`signingToken` (`apps/edge_api/src/services/documenso_client.py:125`). `_recipient_email_tokens` returns ALL `(email_lowercased, token)` pairs carrying a token (`apps/edge_api/src/services/documenso_client.py:149,161-163`) so a caller can pick client vs originator on a multi-recipient document.

### C.6 Prospect signing route + reads

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

`read_document(document_id)` reads `GET /api/v2/document/{numeric}` (the prefixed `envelope_…` handle 400s there) and returns `external_id`, `envelope_id`, `status`, `signing_token` (first SIGNER), `recipient_tokens` (all pairs) (`apps/edge_api/src/services/documenso_client.py:620,631,644,645,647,648`).

**Offline sign-state poll** — `GET /api/v1/documenso/sign-state/{opportunity_id}/{document_id}` (`apps/edge_api/src/routers/documenso_webhooks_v1.py:76`) is FULLY OFFLINE (ZERO Documenso calls, `apps/edge_api/src/routers/documenso_webhooks_v1.py:81-83`). `signed` is derived from raw `business.documenso_webhook_events` rows where `external_id = {opportunity_id} AND envelope_id = {document_id}` carry a terminal `DOCUMENT_COMPLETED` event (`apps/edge_api/src/documenso_webhooks/queries.py:41,69,71`). The captured `envelope_id` column holds the NUMERIC document id (`apps/edge_api/src/documenso_webhooks/queries.py:71`).

### C.7 Payment (same prefill source)

The document fee is resolved from the SAME source as the prefill: `opportunity_specific_content.field_values['fee_amount']`, parsed to integer cents by `resolve_fee_cents` (`FEE_KEY = 'fee_amount'`, `apps/edge_api/src/document_payments/amount.py:16,19,21`). `get_fee_and_contact` JOINs `business.opportunity_specific_content osc ON osc.opportunity_id = o.id` and resolves by the 8-char handle `WHERE o.opportunity_id = %s` (`apps/edge_api/src/document_payments/queries.py:41,47,51,53`). The charge therefore equals the signed fee.

---

## Section D — Cross-repo handoffs (SPA → BFF → edge_api)

The architecture invariant: `platform-app` → `platform-api` (dumb BFF) → `edge_api`.

| Operation | SPA call | BFF route | BFF edge client | edge_api route |
|---|---|---|---|---|
| Staging save (both lanes) | — | `PUT .../by-opportunity/:opportunityId` `requireUser` (`rare-structure-hq:apps/platform-api/src/routes/engagement-mandate-drafts-admin.ts:98`) | `edgeUpsertStagingDraft` (`rare-structure-hq:apps/platform-api/src/lib/edge.ts:693`) | `PUT .../by-opportunity/{opportunity_id}` (`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:63`) |
| Confirm (envelope-distribute) | `confirmMandateDraft` (`rare-structure-hq:apps/platform-app/src/proposals/api.ts:78`) | `POST /:id/confirm` `requireUser` (`rare-structure-hq:apps/platform-api/src/routes/engagement-mandate-drafts-admin.ts:121`) | `edgeConfirmMandateDraft` (`rare-structure-hq:apps/platform-api/src/lib/edge.ts:409`) | `POST .../{draft_id}/confirm` (`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:82`) |
| Originate (prefill-document-from-template) | `originatePrefilled` (`rare-structure-hq:apps/platform-app/src/proposals/api.ts:104`) | `POST /:id/originate-prefilled` `requireUser` (`rare-structure-hq:apps/platform-api/src/routes/engagement-mandate-drafts-admin.ts:142`) | `edgeOriginatePrefilled` (`rare-structure-hq:apps/platform-api/src/lib/edge.ts:438`) | `POST .../{draft_id}/originate-prefilled` (`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:109`) |
| PUBLIC prospect read (envelope-distribute) | (no live SPA consumer — Trap T2) | `GET /document/:envelopeId` NO `requireUser` (`rare-structure-hq:apps/platform-api/src/routes/engagement-mandate-drafts-admin.ts:165`) | `edgeGetMandateDraftDocument` null-on-404 (`rare-structure-hq:apps/platform-api/src/lib/edge.ts:506,512`) | `GET .../document/{envelope_id}` (`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:169`) |
| PUBLIC sign-token (prefill lane) | `getMandateSignToken` (`rare-structure-hq:apps/platform-app/src/proposals/api.ts:126`) | `GET /sign/:opportunityId/:documentId/token` (`rare-structure-hq:apps/platform-api/src/routes/documenso-public.ts:37`) | `edgeGetSignToken` (`rare-structure-hq:apps/platform-api/src/lib/edge.ts:560`) | `GET /api/v1/documenso/sign-token/{opp}/{doc}` (`apps/edge_api/src/routers/documenso_webhooks_v1.py:105`) |

Notes:
- BFF service-token: `edgeConfirmMandateDraft` POSTs `.../confirm` with `serviceHeaders()` (`rare-structure-hq:apps/platform-api/src/lib/edge.ts:411`); `edgeOriginatePrefilled` POSTs with `serviceHeaders()` (`rare-structure-hq:apps/platform-api/src/lib/edge.ts:441`).
- The BFF `/confirm` response surfaces only `{envelopeId, signingToken, documensoHost}` (`rare-structure-hq:apps/platform-api/src/routes/engagement-mandate-drafts-admin.ts:125-130`); the BFF `/originate-prefilled` response carries `{envelopeId, documentId, opportunityId, signingToken, documensoHost, status}` (`rare-structure-hq:apps/platform-api/src/routes/engagement-mandate-drafts-admin.ts:146-155`).
- **BFF path segment differs** from edge_api on the sign-token read: the BFF exposes `/sign/.../token` (`rare-structure-hq:apps/platform-api/src/routes/documenso-public.ts:37`) but `edgeGetSignToken` targets edge_api's `/api/v1/documenso/sign-token/...` (`rare-structure-hq:apps/platform-api/src/lib/edge.ts:568`).
- The BFF mounts `documensoPublicRoutes` under BOTH `/api/v1/documenso` (`rare-structure-hq:apps/platform-api/src/index.ts:123`) AND, transitionally, `/api/v1/engagement-mandate-drafts` (`rare-structure-hq:apps/platform-api/src/index.ts:124`), then mounts `engagementMandateDraftRoutes` under `/api/v1/engagement-mandate-drafts` (`rare-structure-hq:apps/platform-api/src/index.ts:126`) — same prefix, Hono first-match.

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
| `operator_settings.direct_to_documenso_lane` | column | edge_api | ACTIVE | `apps/edge_api/sql/operator_settings.sql:42` |
| `DOCUMENSO_API_URL` | env | edge_api | ACTIVE | `apps/edge_api/src/config.py:37` (default `https://app.documenso.com`, trailing `/` stripped) |
| `DOCUMENSO_API_KEY` | env | edge_api | ACTIVE | `apps/edge_api/src/config.py:29` (format `api_…`; unset → `DocumensoError` `documenso_client.py:76-77`) |

---

## Status: ACTIVE / CONDITIONAL / DEPRECATED / STUB

- **ACTIVE — `envelope-distribute` sub-lane** (`POST .../confirm`): the DB default; runs for any operator who has not switched the lane. `apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:82`.
- **CONDITIONAL — `prefill-document-from-template` sub-lane** (`POST .../originate-prefilled`): runs ONLY when `direct_to_documenso_lane = 'prefill-document-from-template'`. This is the canonical, fully-instrumented path (locks fields, builds the `/p/m` pair link, wires payment). `apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:109`.
- **ACTIVE — staging routes** (`PUT/GET/POST .../by-opportunity`, `POST ""`): shared prep-page plumbing for both lanes. `apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:63,53,40`.
- **ACTIVE — PUBLIC sign-token + sign-state reads**: the prefill-lane prospect surface (live token + offline poll). `apps/edge_api/src/routers/documenso_webhooks_v1.py:105,76`.
- **CONDITIONAL — PUBLIC `GET .../document/{envelope_id}`**: the envelope-distribute prospect read. Endpoint runs, but NO live SPA route consumes it for signing (Trap T2). `apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:169`.
- **DEPRECATED-leaning — `create_document_from_template_with_custom_pdf`**: still the active `/confirm` client, but the older lane; structurally link-incapable. Not the canonical path. `apps/edge_api/src/services/documenso_client.py:321`.
- **STUB / DEAD — `edgeGetMandateDraftDocument` BFF client + `/p/m/:envelopeId` references**: the BFF client exists but no live SPA caller; the only `/p/m/:envelopeId` mention in the SPA is a stale comment. `rare-structure-hq:apps/platform-api/src/lib/edge.ts:506`; stale comment `rare-structure-hq:apps/platform-app/src/proposals/DocumentSummaryScaffold.tsx:8` (per dossier grep).

---

## Traps (do not be misled)

- **T1 — `prefill_values` keying: LABEL, not NAME.** The DDL comment at `apps/edge_api/sql/engagement_mandate_draft_content.sql:9` (per dossier) says `prefill_values` is keyed by the template's text_field NAME. The runtime mapping matches on `fieldMeta.label` (`apps/edge_api/src/services/documenso_client.py:366`; query comment `apps/edge_api/src/engagement_mandate_drafts/queries.py:59-60`). **Code wins: keying is by LABEL.** If a template's field name and label diverge, staged values silently fail to map (UNVERIFIED whether name==label for live engagement templates).

- **T2 — `GET .../document/{envelope_id}` has no live SPA signing consumer.** The endpoint is real and PUBLIC, but the SPA's `DocumentSignPage` lives only at `/p/m/:opp/:doc` (`rare-structure-hq:apps/platform-app/src/App.tsx:100`); there is NO `/p/m/:envelopeId` route. The only `:envelopeId` reference in the SPA is a stale comment (`rare-structure-hq:apps/platform-app/src/proposals/DocumentSummaryScaffold.tsx:8`, per dossier grep) and an unused BFF client (`rare-structure-hq:apps/platform-api/src/lib/edge.ts:506`, whose JSDoc still says `/p/m/:envelopeId`). Do not assume the envelope-distribute lane has a working prospect signing surface.

- **T3 — "UUID" wording is stale; the value is the 8-char handle.** `App.tsx:97-98` comment calls `opportunityId` "the opportunity UUID is the unguessable access capability" (`rare-structure-hq:apps/platform-app/src/App.tsx:97-98`), and the `DocumentReadResult`/`read_document` docstrings call `external_id` "the opportunity UUID stamped at originate" (`apps/edge_api/src/services/documenso_client.py:608-609,626` per dossier). The actual stamped value is the **8-char public handle** (`opportunity_ref`, `apps/edge_api/src/engagement_mandate_drafts/queries.py:166`), NOT the full UUID. The pair-gate compares `external_id` to the 8-char handle (`apps/edge_api/src/routers/documenso_webhooks_v1.py:135`). The route param and gate are correct; only the prose is stale.

- **T4 — DEFAULT lane ≠ canonical lane.** `envelope-distribute` is the DB DEFAULT (`apps/edge_api/sql/operator_settings.sql:42`) but is the OLDER, link-incapable path. The newer, fully-wired path is `prefill-document-from-template`. Do not conflate "default" with "primary/canonical."

- **T5 — same Documenso, different endpoints.** `envelope-distribute` uses `/api/v2/envelope/use` (recipients OPTIONAL); `prefill-document-from-template` uses `/api/v2/template/use` (recipients REQUIRED) (`apps/edge_api/src/services/documenso_client.py:381` vs `:535`). They are sibling Documenso v2 endpoints with different recipient semantics; do not assume one is a wrapper of the other.

- **T6 — numeric template id is NOT an envelope id.** The DB carries only the numeric Documenso template id; it 400s on envelope endpoints. The envelope-distribute lane resolves it live via `GET /api/v2/template/{id} → .envelopeId` on every `/confirm` (`apps/edge_api/src/services/documenso_client.py:303,311,313`). The prefill lane reads the template directly via `GET /api/v2/template/{id}` (`apps/edge_api/src/services/documenso_client.py:459`).

- **T7 — prefill fan-out differs between lanes.** envelope-distribute maps a label to the FIRST field only (`apps/edge_api/src/services/documenso_client.py:367`), so a label split across multiple template fields prefills only ONE — possible under-fill (UNVERIFIED whether envelope-distribute templates have split labels). The prefill lane fans out to EVERY matching field id (`apps/edge_api/src/services/documenso_client.py:496,508-512`) and supports base-name splits (`apps/edge_api/src/services/documenso_client.py:190-191`).

- **T8 — base-table columns are upstream-owned.** `…draft_content.updated_at`, `created_at`, `organization_id`, `opportunity_id`, `documenso_template_id` are referenced by queries (e.g. `apps/edge_api/src/engagement_mandate_drafts/queries.py:87,214`) but are NOT added by edge_api DDL (which adds only `prefill_values` and `archetype_id`). They must pre-exist on the upstream base table; drift would fail writes loudly. Base-table type/default definitions are UNVERIFIED (owned by hq-x, not in edge_api DDL).
