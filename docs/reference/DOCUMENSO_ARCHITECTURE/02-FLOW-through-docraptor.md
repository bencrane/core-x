# Through-DocRaptor Proposals Flow (engagement agreement + e-signature)

> **STATUS BANNER** — This file documents the **`render_mode == 'through-docraptor'`** lane (the DB-default render mode), implemented in `edge_api` `proposals_v1` + `src/proposals/*`. This is the **legacy/default** engagement-agreement origination path: create draft → confirm/originate → DocRaptor PDF render → Documenso v2 envelope by anchor → embedded sign → webhook. The `render_mode == 'direct-to-documenso'` branch inside this same router is a **STUB** (`'direct-to-documenso pathway not yet wired'`); the live direct path lives in the SEPARATE `engagement-mandate-drafts` flow (not covered here).

## Orientation

This is the engagement-agreement + e-signature pathway in `core-x` `edge_api`, keyed on an unguessable capability ref (`rs_…`). An operator (via the `platform-api` BFF) mints a DRAFT proposal, then originates it: confirm stamps the operator's locked-in structured pricing onto the still-draft row, renders a legal PDF via DocRaptor (PrinceXML, LIVE mode), creates a Documenso v2 envelope from that PDF with the Client as the sole `SIGNER`, places `SIGNATURE`/`DATE` fields BY ANCHOR (`[[CLIENT_SIGNATURE]]` / `[[CLIENT_DATE]]` resolved via Documenso `findText`), distributes WITHOUT email, and binds the envelope + signing token to the row (`draft` → `sent`). The prospect reads a PUBLIC projection and signs in the embedded Documenso surface. `render_mode` is resolved server-side by the BFF from `public.operator_settings.render_mode` (default `'through-docraptor'`); the client never supplies it at confirm time. All persistence is on `business.engagement_proposals`; the Stripe ACH payment overlay is additive columns on that same row.

This is ONE of several Documenso lanes. Sibling lanes — the live `direct-to-documenso` path (`engagement-mandate-drafts`) and the document-payment flow — are separate domains. **Critical correction vs prior internal docs:** the in-router webhook `POST /api/v1/proposals/webhook` is now **DEPRECATED** — Documenso is repointed to the raw-capture `POST /api/v1/documenso/webhook`, and the legacy proposals webhook no longer receives deliveries.

---

## 1. The capability ref (`rs_…`)

The `ref` is minted on create by `queries.new_ref()` as `"rs_" + secrets.token_urlsafe(16)` — an unguessable capability token that is simultaneously the **primary key**, the **URL slug**, and the **bearer credential** (`apps/edge_api/src/proposals/queries.py:40`, `apps/edge_api/src/proposals/queries.py:42`). It is the PRIMARY KEY of `business.engagement_proposals` (`apps/edge_api/sql/engagement_proposals.sql:23`). Because the ref IS the credential, the public read and document routes are unauthenticated — possession of the ref is authorization.

---

## 2. Edge_api routes (`proposals_v1`)

The router is mounted with `prefix="/api/v1/proposals"`, `tags=["proposals"]` (`apps/edge_api/src/routers/proposals_v1.py:52`), imported at `apps/edge_api/main.py:59` and included at `apps/edge_api/main.py:176`.

| Method + path | Function | Auth | Status | Purpose |
|---|---|---|---|---|
| `POST /api/v1/proposals` | `create_proposal` | service-token | ACTIVE | Mint a DRAFT; provisioning deferred to `/confirm` (`apps/edge_api/src/routers/proposals_v1.py:142`) |
| `POST /api/v1/proposals/{ref}/confirm` | `confirm_proposal` | service-token | ACTIVE | Originate: stamp pricing → `_provision` (render_mode-aware) (`apps/edge_api/src/routers/proposals_v1.py:203`) |
| `POST /api/v1/proposals/{ref}/provision` | `provision_proposal` | service-token | ACTIVE (no BFF broker) | Re-render PDF + create envelope; through-docraptor only (`apps/edge_api/src/routers/proposals_v1.py:254`) |
| `GET /api/v1/proposals` | `list_proposals` | service-token | ACTIVE | Operator list; limit clamped `1..500` (`apps/edge_api/src/routers/proposals_v1.py:272`) |
| `GET /api/v1/proposals/{ref}` | `get_proposal` | PUBLIC | ACTIVE | Consumer page data source (`apps/edge_api/src/routers/proposals_v1.py:286`) |
| `GET /api/v1/proposals/{ref}/document` | `get_signed_document` | PUBLIC | ACTIVE | Stream sealed PDF when `status=='completed'` (`apps/edge_api/src/routers/proposals_v1.py:321`) |
| `POST /api/v1/proposals/webhook` | `documenso_webhook` | `X-Documenso-Secret` | **DEPRECATED** | Legacy status advance; no longer receives Documenso deliveries (`apps/edge_api/src/routers/proposals_v1.py:337`) |

### 2.1 `create_proposal` — mint DRAFT

Service-token gated; mints a DRAFT and DEFERS provisioning to `/confirm` (`apps/edge_api/src/routers/proposals_v1.py:142`, `apps/edge_api/src/routers/proposals_v1.py:143`). The ref is minted at `apps/edge_api/src/routers/proposals_v1.py:147`. `template_id` defaults to `'strategic_origination_mandate'` when None (`apps/edge_api/src/routers/proposals_v1.py:150`).

Pricing is **dynamic-with-defaults**: each unset field inherits the published template's default via `template_queries.get_published_by_slug` (`apps/edge_api/src/routers/proposals_v1.py:161`), then falls back to `DEFAULT_DURATION_MONTHS = 6` (`apps/edge_api/src/proposals/models.py:25`) and `DEFAULT_BILLING_CADENCE = 'upfront_in_full'` (`apps/edge_api/src/proposals/models.py:26`), applied at `apps/edge_api/src/routers/proposals_v1.py:175`. If no fee can be resolved it raises HTTP 422 `'monthly_fee_cents required (no template default)'` (`apps/edge_api/src/routers/proposals_v1.py:178`, `apps/edge_api/src/routers/proposals_v1.py:179`) — it never persists a null fee.

Returns:
```python
{
  "ref": ref,
  "path": "/proposal/{ref}",
  "status": ...,
  "provisioned": False,
  "provision_error": None,
}
```
(`apps/edge_api/src/routers/proposals_v1.py:194` — return dict spans 194-200.)

### 2.2 `confirm_proposal` — originate (THE through-docraptor entrypoint)

Service-token gated (`apps/edge_api/src/routers/proposals_v1.py:203`). It is the originate entrypoint: stamps locked-in structured values via `update_pricing`, then calls `_provision` with `render_mode` from the body (`apps/edge_api/src/routers/proposals_v1.py:235`, `apps/edge_api/src/routers/proposals_v1.py:243`).

Control flow:
```
confirm_proposal(ref, body):
  p = get_by_ref(ref)
  if p is None                       -> 404 'proposal not found'           (proposals_v1.py:213-214)
  if p.documenso_envelope_id != None -> 409 'already originated'           (proposals_v1.py:215-216)
  # field merge — omitted value keeps the row's current actual:
  monthly_fee_cents = body.monthly_fee_cents or p.monthly_fee_cents        (proposals_v1.py:220)
  duration_months   = body.duration_months  or p.duration_months          (proposals_v1.py:221)
  # success_fee_schedule uses explicit None-check (empty list honored):
  success_fee_schedule = p.success... if body.success... is None else body... (proposals_v1.py:223-225)
  updated = update_pricing(...)      # WHERE status='draft'                (proposals_v1.py:235)
  if updated is None                 -> 409 'already originated'  # concurrent originate (proposals_v1.py:240-242)
  ok, err = _provision(conn, updated, render_mode=body.render_mode)        (proposals_v1.py:243)
  fresh = get_by_ref(ref)
  return {ref, status, provisioned, provision_error, signing_token}        (proposals_v1.py:245-251)
```

The `or` for `monthly_fee_cents`/`duration_months` is safe because both are `gt=0` in the model (`apps/edge_api/src/proposals/models.py:96`, `apps/edge_api/src/proposals/models.py:97`) so a meaningful `0` can never arrive; `success_fee_schedule` deliberately uses a None-check so an explicit empty list (no tiers) is honored (`apps/edge_api/src/routers/proposals_v1.py:223`).

Returns `signing_token` = the `documenso_client_token` read back from the freshly-updated row (`apps/edge_api/src/routers/proposals_v1.py:250`).

> **TWO 409 conditions** — read-time (envelope already bound, `:215`) AND write-time (the row left `draft` between read and write under a concurrent originate, so `update_pricing` returns None, `:240`). Both surface the same `'already originated'` detail.

### 2.3 `provision_proposal` — re-provision / recovery

Service-token gated (`apps/edge_api/src/routers/proposals_v1.py:254`). (Re)renders the PDF + creates the envelope. Returns 404 if unknown (`apps/edge_api/src/routers/proposals_v1.py:259`-260), 409 `'already provisioned'` if `documenso_envelope_id` is already set (`apps/edge_api/src/routers/proposals_v1.py:261`, `apps/edge_api/src/routers/proposals_v1.py:264`), and 502 `'provisioning failed: {err}'` on failure (`apps/edge_api/src/routers/proposals_v1.py:268`). It calls `_provision(conn, p)` **without** a `render_mode` arg — defaulting to None → through-docraptor (`apps/edge_api/src/routers/proposals_v1.py:265`).

> This route has **NO BFF broker**: `edge.ts` exposes only `edgeCreateProposal`/`edgeConfirmProposal`/`edgeGetProposal`/`edgeListProposals`, and `proposals-admin.ts` brokers create/confirm/list/read/send/payment — a re-grep of `apps/platform-api/src/` for `provision` returns only the `provisioned`/`provision_error` FIELD names, zero `/provision` route calls (`rare-structure-hq:apps/platform-api/src/lib/edge.ts:168`, `rare-structure-hq:apps/platform-api/src/lib/edge.ts:182`, `apps/edge_api/src/routers/proposals_v1.py:254`). It is reachable only by a direct service-token caller.

### 2.4 `get_proposal` — PUBLIC consumer read

PUBLIC — no service-token dependency on the decorator; the ref is the bearer credential (`apps/edge_api/src/routers/proposals_v1.py:286`, `apps/edge_api/src/routers/proposals_v1.py:287`). It is the consumer page data source, returning `ProposalPublic.from_row` with `payment_status` and `exec_summary` (`apps/edge_api/src/routers/proposals_v1.py:318`). Both side reads are isolated: the `payment_status` read collapses to `"none"` on any error (`apps/edge_api/src/routers/proposals_v1.py:295`, `apps/edge_api/src/routers/proposals_v1.py:303`) and the `exec_summary` read collapses to a built-in default — neither can break the public read/signing path.

### 2.5 `get_signed_document` — PUBLIC sealed-PDF stream

PUBLIC (`apps/edge_api/src/routers/proposals_v1.py:321`). Streams the sealed PDF (`media_type application/pdf`, inline `filename "{ref}.pdf"`) ONLY when `status == 'completed'` AND `documenso_envelope_id` is set; else 409 `'agreement not yet completed'` (404 if unknown) (`apps/edge_api/src/routers/proposals_v1.py:328`, `apps/edge_api/src/routers/proposals_v1.py:329`). It calls `documenso_client.download_signed_pdf(p.documenso_envelope_id)` (`apps/edge_api/src/routers/proposals_v1.py:330`).

### 2.6 `documenso_webhook` (in-router) — DEPRECATED

The route is service-gated by `X-Documenso-Secret` (constant-time compare via `verify_webhook_secret`), returns 503 if the secret is unconfigured (`apps/edge_api/src/routers/proposals_v1.py:342`) and 401 on mismatch (`apps/edge_api/src/routers/proposals_v1.py:344`, `apps/edge_api/src/routers/proposals_v1.py:345`), normalizes via `documenso_client.normalize_event` (`apps/edge_api/src/routers/proposals_v1.py:348`), resolves the proposal by `externalId` (the ref) first then `get_by_envelope` (`apps/edge_api/src/routers/proposals_v1.py:356`, `apps/edge_api/src/routers/proposals_v1.py:358`), sets `signed_url = /api/v1/proposals/{ref}/document` only when `status == 'completed'` (`apps/edge_api/src/routers/proposals_v1.py:362`), and advances via `queries.advance_status` on the stored envelope id (`apps/edge_api/src/routers/proposals_v1.py:365`).

**The route code is intact and would work if called, but it is DEPRECATED**: Documenso is repointed to `POST /api/v1/documenso/webhook` (same shared `DOCUMENSO_WEBHOOK_SECRET`), and this legacy route "simply stops receiving deliveries" (`apps/edge_api/src/routers/documenso_webhooks_v1.py:5`, `apps/edge_api/src/routers/documenso_webhooks_v1.py:8`). See §6.

---

## 3. `_provision` — render + envelope step

`_provision(conn, p, render_mode)` is **non-raising** — it returns `tuple[bool, str | None]` (`apps/edge_api/src/routers/proposals_v1.py:89`, `apps/edge_api/src/routers/proposals_v1.py:90`).

```
_provision(conn, p, render_mode=None):
  if render_mode == "direct-to-documenso":              # STUB branch
    log "...direct-to-documenso requested — pathway not yet wired"  (proposals_v1.py:102)
    return False, "direct-to-documenso pathway not yet wired"       (proposals_v1.py:103)
  try:
    pdf = docraptor_client.render_pdf(_agreement_html(conn, p), name=p.ref)   (proposals_v1.py:105)
    env = documenso_client.create_signing_envelope(
             pdf, title=_title(p),
             signer_name=p.client_signer_name,
             signer_email=p.client_email,
             external_id=p.ref)                                              (proposals_v1.py:106-109)
    queries.attach_envelope(conn, p.ref, env.envelope_id, env.client_token)  (proposals_v1.py:110)
    return True, None
  except (DocRaptorError, DocumensoError, httpx.HTTPError) as exc:           (proposals_v1.py:112)
    return False, str(exc)   # committed draft survives, re-provisionable     (proposals_v1.py:116)
```

The `except` catches DocRaptor errors, Documenso errors, and `httpx.HTTPError` (including transport timeouts) — returning `(False, str(exc))` non-raising so the committed draft survives and is re-provisionable, never stranding the caller (`apps/edge_api/src/routers/proposals_v1.py:112`, `apps/edge_api/src/routers/proposals_v1.py:115`, `apps/edge_api/src/routers/proposals_v1.py:116`).

> **The `direct-to-documenso` branch is a STUB** — it logs and returns `(False, 'direct-to-documenso pathway not yet wired')` WITHOUT rendering a PDF or creating an envelope (`apps/edge_api/src/routers/proposals_v1.py:99`, `apps/edge_api/src/routers/proposals_v1.py:102`, `apps/edge_api/src/routers/proposals_v1.py:103`). The committed draft survives; no origination occurs. The docstring above it confirms the through-docraptor branch is "CURRENT behavior" while direct-to-documenso is "NOT YET WIRED (stub below)" (`apps/edge_api/src/routers/proposals_v1.py:96`, `apps/edge_api/src/routers/proposals_v1.py:97`).

### 3.1 `_title`

`_title(p) = f"Strategic Origination Mandate — {p.client_name}"` (`apps/edge_api/src/routers/proposals_v1.py:55`, `apps/edge_api/src/routers/proposals_v1.py:56`); passed as the envelope `title` at `apps/edge_api/src/routers/proposals_v1.py:107`.

### 3.2 `_agreement_html` — resolve the agreement body

```
_agreement_html(conn, p):
  tpl = template_queries.get_published_by_slug(conn, p.template_id)   (proposals_v1.py:69)
  except ANY error -> tpl stays None (rollback), fall through to built-in
  if tpl is not None:
    md   = substitute_markdown_tokens(tpl["markdown"], p)   # pre-render BLOCK tokens (proposals_v1.py:79)
    identity = queries.get_org_identity(conn, tpl["organization_id"])
    html = render_template_html(md, identity=identity)      # markdown -> branded HTML (proposals_v1.py:84)
    return substitute_tokens(html, proposal_token_values(p))  # inline scalar tokens   (proposals_v1.py:85)
  return render_agreement_html(p)                            # built-in fallback         (proposals_v1.py:86)
```

If a PUBLISHED template exists for `p.template_id` it pre-renders BLOCK tokens, renders markdown → branded HTML under the org identity, then substitutes inline scalar tokens; on ANY registry error it falls back to the built-in `render_agreement_html(p)` (`apps/edge_api/src/routers/proposals_v1.py:59`, `apps/edge_api/src/routers/proposals_v1.py:69`, `apps/edge_api/src/routers/proposals_v1.py:79`, `apps/edge_api/src/routers/proposals_v1.py:84`, `apps/edge_api/src/routers/proposals_v1.py:86`). The live signing path must never break because of the template registry.

### 3.3 `_field_values` — display merge map

`_field_values` builds the display merge values stamped onto the `field_values` jsonb column (`clientName`, `clientSignerName`, `clientTitle`, `clientEmail`, `effectiveDate` iso, `monthlyFee` formatted, `duration` str, `billingCadence`, `total` formatted, `quarterlyTotal` formatted `= total`, `rsName`) recomputed from structured pricing (`apps/edge_api/src/routers/proposals_v1.py:119`, `apps/edge_api/src/routers/proposals_v1.py:126`, `apps/edge_api/src/routers/proposals_v1.py:137`, `apps/edge_api/src/routers/proposals_v1.py:138`).

---

## 4. DocRaptor render

`docraptor_client.render_pdf` POSTs to `https://docraptor.com/docs` (`apps/edge_api/src/services/docraptor_client.py:19`) with `test=False` (**LIVE**, `apps/edge_api/src/services/docraptor_client.py:37`), `document_type 'pdf'`, `prince_options` media `'print'` and `javascript False` (`apps/edge_api/src/services/docraptor_client.py:41`-43), HTTP Basic auth with the API key as username (`apps/edge_api/src/services/docraptor_client.py:48`). It raises `DocRaptorError` when `DOCRAPTOR_API_KEY` is unset (`apps/edge_api/src/services/docraptor_client.py:33`-34) or on non-2xx (`apps/edge_api/src/services/docraptor_client.py:54`); returns raw PDF bytes (`apps/edge_api/src/services/docraptor_client.py:56`). Timeout 60s / connect 10s (`apps/edge_api/src/services/docraptor_client.py:20`).

---

## 5. Documenso v2 envelope by anchor

`documenso_client.create_signing_envelope` is the through-docraptor envelope creator — 4 steps against Documenso Cloud v2 (`apps/edge_api/src/services/documenso_client.py:198`):

```
1) POST /api/v2/envelope/create   (multipart)                            (documenso_client.py:223)
     payload JSON = {type:"DOCUMENT", title, recipients:[{name,email,role:"SIGNER"}],
                     distributeDocument:false, externalId}                (documenso_client.py:213-220)
     files        = {"files": ("{title}.pdf", pdf_bytes, "application/pdf")}
   -> envelope_id
2) GET /api/v2/envelope/{envelope_id}                                     (documenso_client.py:234)
     token        = _extract_client_token(env, signer_email)             (documenso_client.py:235)
     recipient_id = matched by email (lowercased), else first recipient   (documenso_client.py:238-241)
     document_id  = _numeric_document_id(env)                            (documenso_client.py:245)
3) POST /api/v2/envelope/field/create-many                               (documenso_client.py:264)
     SIGNATURE field: placeholder=CLIENT_SIGNATURE_ANCHOR, **_SIGNATURE_FIELD_SIZE (documenso_client.py:254-255)
     DATE      field: placeholder=CLIENT_DATE_ANCHOR,      **_DATE_FIELD_SIZE      (documenso_client.py:260-261)
4) POST /api/v2/envelope/distribute  meta.distributionMethod="NONE"      (documenso_client.py:271-273)
return EnvelopeResult(envelope_id, document_id, client_token)            (documenso_client.py:277)
```

### 5.1 Anchor placement (not coordinates)

`SIGNATURE`/`DATE` fields are placed BY ANCHOR — a `placeholder` string resolved by `findText` over the PDF, NOT coordinates; Documenso resolves the position and whites the marker out at sign-time (`apps/edge_api/src/services/documenso_client.py:254`, `apps/edge_api/src/services/documenso_client.py:260`). Only the field box SIZE is overridden: `_SIGNATURE_FIELD_SIZE {width:32.0, height:7.0}` (`apps/edge_api/src/services/documenso_client.py:173`), `_DATE_FIELD_SIZE {width:22.0, height:4.0}` (`apps/edge_api/src/services/documenso_client.py:174`).

The anchor sentinels are the literal strings `'[[CLIENT_SIGNATURE]]'` and `'[[CLIENT_DATE]]'`, defined ONCE in `signing_anchors.py` (`apps/edge_api/src/proposals/signing_anchors.py:21`, `apps/edge_api/src/proposals/signing_anchors.py:24`) and imported by BOTH the renderer and the Documenso client (`apps/edge_api/src/services/documenso_client.py:38`, `apps/edge_api/src/proposals/agreement_template.py:27`). The `[[…]]` grammar deliberately differs from the `{{snake_case}}` merge grammar so token substitution ignores the anchors and they survive verbatim into the PDF text layer for `findText`.

### 5.2 Client token extraction

`_extract_client_token` pulls the SIGNER recipient's signing token matched by email (lowercased), falling back to the first recipient on single-signer envelopes (`apps/edge_api/src/services/documenso_client.py:110`, `apps/edge_api/src/services/documenso_client.py:121`, `apps/edge_api/src/services/documenso_client.py:124`); this token is bound to the row via `attach_envelope` and surfaced as `signing_token` (`apps/edge_api/src/services/documenso_client.py:235`).

### 5.3 Auth + base URL

`_auth_value` prefixes the Documenso key with `'api_'` if not already present (`apps/edge_api/src/services/documenso_client.py:74`, `apps/edge_api/src/services/documenso_client.py:79`); `_client` sets `base_url = config.documenso_api_url()` (env `DOCUMENSO_API_URL`, default `https://app.documenso.com`) and the Authorization header (`apps/edge_api/src/services/documenso_client.py:82`, `apps/edge_api/src/config.py:34`, `apps/edge_api/src/config.py:37`). **The embed host MUST match this URL.**

### 5.4 Sealed-PDF download

`download_signed_pdf` resolves the numeric document id from the envelope, GETs `/api/v2/document/{numeric}/download?version=signed`, and returns bytes directly for `application/pdf` or a `%PDF-` magic; otherwise it follows a JSON `downloadUrl` with a BARE `httpx` client (never attaching the Documenso API key to the third-party presigned host) (`apps/edge_api/src/services/documenso_client.py:762`, `apps/edge_api/src/services/documenso_client.py:773`, `apps/edge_api/src/services/documenso_client.py:778`, `apps/edge_api/src/services/documenso_client.py:785`).

---

## 6. Status truth — the webhook repoint (CRITICAL)

**The live status-of-record is the raw-capture webhook, NOT the in-router proposals webhook.**

`POST /api/v1/documenso/webhook` (`documenso_webhooks_v1`, prefix `/api/v1/documenso`) is `X-Documenso-Secret` gated and append-inserts EVERY raw Documenso event into `business.documenso_webhook_events` with NO normalization/projection (`apps/edge_api/src/routers/documenso_webhooks_v1.py:39`, mounted at `apps/edge_api/main.py:181`). Its module docstring states Documenso "is repointed here from the legacy `/api/v1/proposals/webhook` (same shared `DOCUMENSO_WEBHOOK_SECRET`)" and "The legacy proposals webhook is untouched (it simply stops receiving deliveries)" (`apps/edge_api/src/routers/documenso_webhooks_v1.py:5`, `apps/edge_api/src/routers/documenso_webhooks_v1.py:8`).

```
Documenso  --(X-Documenso-Secret)-->  POST /api/v1/documenso/webhook   [ACTIVE, raw capture]
                                          |
                                          +--> append-insert RAW into business.documenso_webhook_events
                                                (every event, no filter/normalize/project)

Documenso  --X--X-->  POST /api/v1/proposals/webhook   [DEPRECATED, code intact, no traffic]
                          |
                          +--> normalize_event -> advance_status   [CONDITIONAL: only reachable here]
```

`normalize_event` (used by the deprecated route) maps Documenso event names → internal status via `_EVENT_TO_STATUS` (`apps/edge_api/src/services/documenso_client.py:45`-52, `apps/edge_api/src/services/documenso_client.py:803`):

| Documenso event | Internal status |
|---|---|
| `DOCUMENT_SENT` | `sent` |
| `DOCUMENT_OPENED` | `opened` |
| `DOCUMENT_SIGNED` | `signed` |
| `DOCUMENT_COMPLETED` | `completed` |
| `DOCUMENT_REJECTED` | `rejected` |
| `DOCUMENT_CANCELLED` | `voided` |

It folds both lowercase-dotted (`document.completed`) and enum forms to the enum key (`apps/edge_api/src/services/documenso_client.py:810`) and reads `envelope_id` from payload `id`/`documentId`/`envelopeId` and `external_id` from `payload.externalId` (`apps/edge_api/src/services/documenso_client.py:816`).

`verify_webhook_secret` does an `hmac.compare_digest` constant-time compare of `X-Documenso-Secret` against `config.documenso_webhook_secret()`, returning False (refuse) when the secret is unconfigured (`apps/edge_api/src/services/documenso_client.py:791`, `apps/edge_api/src/services/documenso_client.py:797`, `apps/edge_api/src/services/documenso_client.py:800`). Used by BOTH webhook routes.

> **OPEN QUESTION (carried forward, UNVERIFIED):** For the proposals/ref (through-docraptor) lane SPECIFICALLY, no code path was found in this pass that projects `business.documenso_webhook_events` back onto `engagement_proposals.status`. So it is UNVERIFIED whether a proposals-ref agreement's `status` still advances past `'sent'` server-side after the repoint, or whether status advance is now resolved only for the direct `(opportunity_id, document_id)` lane via the offline `/sign-state` poll. This needs a follow-up trace.

---

## 7. Persistence — `business.engagement_proposals` + queries

### 7.1 The table

`business.engagement_proposals` is the e-signature grain table CREATEd by edge_api DDL on the HQX control-plane Postgres; idempotent DDL applied at boot (`main.py:138` `run_migrations()` + `config.py:215` `db_migrate_on_boot` default ON) (`apps/edge_api/sql/engagement_proposals.sql:21`). `ref` is PRIMARY KEY (`apps/edge_api/sql/engagement_proposals.sql:23`). `documenso_envelope_id` has a PARTIAL UNIQUE index `engagement_proposals_envelope_uidx WHERE documenso_envelope_id IS NOT NULL` (`apps/edge_api/sql/engagement_proposals.sql:62`, `apps/edge_api/sql/engagement_proposals.sql:63`) — so the webhook UPDATE hits ≤1 row and an accidental rebind fails loudly.

The DDL note records the naming reason: `business.proposals` is ALREADY TAKEN by the DMaaS data-transfer subsystem, hence `business.engagement_proposals` (`apps/edge_api/sql/engagement_proposals.sql:4`, `apps/edge_api/sql/engagement_proposals.sql:5`, `apps/edge_api/sql/engagement_proposals.sql:6`).

Representative columns (`apps/edge_api/sql/engagement_proposals.sql:23`-55): `ref` (PK), `template_id` (default `'strategic_origination_mandate'`), `client_name`, `client_signer_name`, `client_title`, `client_email`, `effective_date`, `monthly_fee_cents` (bigint), `duration_months`, `billing_cadence`, `success_fee_schedule` (jsonb), `quarterly_total_cents` (bigint; legacy name, now carries `{{total}} = monthly*duration` — `apps/edge_api/sql/engagement_proposals.sql:36`), `rs_signer_name`, `status` (CHECK enum, default `'draft'` — `apps/edge_api/sql/engagement_proposals.sql:39`), `documenso_envelope_id` (`apps/edge_api/sql/engagement_proposals.sql:42`), `documenso_client_token`, `signed_pdf_url`, `field_values` (jsonb NOT NULL DEFAULT `'{}'` — `apps/edge_api/sql/engagement_proposals.sql:46`), `created_by`, and the timeline columns `created_at`/`sent_at`/`opened_at`/`signed_at`/`completed_at`/`updated_at` (`apps/edge_api/sql/engagement_proposals.sql:49`).

### 7.2 Status enum

`ProposalStatus` is one of `draft | sent | opened | signed | completed | rejected | voided`, declared in the model (`apps/edge_api/src/proposals/models.py:38`) and CHECK-constrained in the DDL (`apps/edge_api/sql/engagement_proposals.sql:39`, `apps/edge_api/sql/engagement_proposals.sql:40`).

### 7.3 The durability queries

- `queries.insert_proposal` inserts with `status` hardcoded `'draft'` and commits; `success_fee_schedule` and `field_values` stored as `Jsonb` (`apps/edge_api/src/proposals/queries.py:49`, `apps/edge_api/src/proposals/queries.py:70`, `apps/edge_api/src/proposals/queries.py:79`, `apps/edge_api/src/proposals/queries.py:84`).
- `queries.update_pricing` UPDATEs ONLY `WHERE ref=%s AND status='draft'` (the durability gate) and RETURNS the updated row or None on a non-draft row — origination terms become immutable once the envelope is bound (`apps/edge_api/src/proposals/queries.py:88`, `apps/edge_api/src/proposals/queries.py:116`, `apps/edge_api/src/proposals/queries.py:130`).
- `queries.attach_envelope` binds `documenso_envelope_id` + `documenso_client_token` and moves `draft → sent` (CASE), setting `sent_at = COALESCE(sent_at, now())`, ONLY `WHERE documenso_envelope_id IS NULL` (no silent rebind); returns whether one row was bound (`apps/edge_api/src/proposals/queries.py:160`, `apps/edge_api/src/proposals/queries.py:173`, `apps/edge_api/src/proposals/queries.py:176`, `apps/edge_api/src/proposals/queries.py:180`).
- `queries.advance_status` applies a webhook-driven transition atomically/idempotently with the monotonic/terminal guard IN the UPDATE WHERE predicate. Forward-chain (rank `draft0 → sent1 → opened2 → signed3 → completed4`) applies only when strictly ahead (`apps/edge_api/src/proposals/queries.py:214`); terminal (`rejected`/`voided`) applies unless already in `('rejected','voided','completed')` (`apps/edge_api/src/proposals/queries.py:211`, `apps/edge_api/src/proposals/queries.py:212`). Returns whether a row changed (`apps/edge_api/src/proposals/queries.py:222`). **CONDITIONAL** — the SQL logic is correct but is reachable only from the deprecated proposals webhook, so it is off the live status path.
- The `_SELECT_COLS` used by all reads COALESCEs `duration_months → 6`, `billing_cadence → 'upfront_in_full'`, `success_fee_schedule → '[]'::jsonb` so legacy rows read cleanly and the model never sees a NULL (`apps/edge_api/src/proposals/queries.py:28`, `apps/edge_api/src/proposals/queries.py:31`, `apps/edge_api/src/proposals/queries.py:32`, `apps/edge_api/src/proposals/queries.py:33`).
- `queries.get_org_identity` reads `business.organizations` (`name`, `theme_config`) `WHERE id=%s AND deleted_at IS NULL`, returning `{display_name, legal_name (theme.legal_name or name), theme}`; None when not found (`apps/edge_api/src/proposals/queries.py:227`, `apps/edge_api/src/proposals/queries.py:236`, `apps/edge_api/src/proposals/queries.py:246`). `business.organizations` is UPSTREAM-OWNED (read-only here).

---

## 8. Money model

The `Proposal` model carries money in integer cents. `total_cents(monthly_fee_cents, duration_months) = monthly * duration` is DERIVED and never stored (`apps/edge_api/src/proposals/models.py:47`, `apps/edge_api/src/proposals/models.py:50`); `format_usd` drops the decimal when whole (`$25,000`) (`apps/edge_api/src/proposals/models.py:41`, `apps/edge_api/src/proposals/models.py:44`).

`charge_cents(monthly_fee_cents, duration_months, billing_cadence)` computes the PER-INVOICE amount by cadence (`apps/edge_api/src/proposals/models.py:53`):

| `billing_cadence` | Charge |
|---|---|
| `'monthly'` | one month (`apps/edge_api/src/proposals/models.py:58`) |
| `'quarterly'` | three months (`apps/edge_api/src/proposals/models.py:61`) |
| else (incl. `'upfront_in_full'` / unknown) | full engagement total (`apps/edge_api/src/proposals/models.py:62`) |

`ProposalPublic.amount_due`/`amount_due_cents` are derived from this (`apps/edge_api/src/proposals/models.py:167`, `apps/edge_api/src/proposals/models.py:193`).

---

## 9. Renderers (built-in + authored-template)

### 9.1 Built-in Strategic Origination Mandate

`render_agreement_html` is the built-in "Strategic Origination Mandate" — a style-agnostic legal body with a `«STYLE»` injection slot; default `_PLAIN_STYLE` (neutral white/black serif), with `_BRAND_STYLE` (Rare Structure dark identity) available (`apps/edge_api/src/proposals/agreement_template.py:1`, `apps/edge_api/src/proposals/agreement_template.py:34`, `apps/edge_api/src/proposals/agreement_template.py:67`). Merge tokens use a `«TOKEN»` sentinel substituted by `str.replace` (NOT `str.format`) so print-CSS braces are untouched; the `[[CLIENT_SIGNATURE]]` / `[[CLIENT_DATE]]` anchors ride in NOT-escaped (`apps/edge_api/src/proposals/agreement_template.py:52`, `apps/edge_api/src/proposals/agreement_template.py:55`).

The execution block pre-renders RS as the pre-signed party (typed-name italic serif, NOT a script font because DocRaptor/Prince blocks filesystem font access — named script fonts 422) and leaves the CLIENT signature + date lines carrying the anchor markers in `.sig-anchor` (real selectable text in a standard font; never `display:none`/zero-size) (`apps/edge_api/src/proposals/agreement_template.py:107`, `apps/edge_api/src/proposals/agreement_template.py:113`, `apps/edge_api/src/proposals/agreement_template.py:296`, `apps/edge_api/src/proposals/agreement_template.py:309`, `apps/edge_api/src/proposals/agreement_template.py:313`).

### 9.2 Authored-template render

`render_template_html` / `render_branded_document` render operator-authored markdown via `markdown-it-py` (commonmark + table + strikethrough, `html=False` so embedded raw HTML/`<script>` is escaped) into the Rare Structure brand SHELL (`_SHELL`), which appends the execution block with `{{rs_name}}` / `{{client_signer_name}}` / `{{client_title}}` / `{{effective_date}}` tokens and the `«CLIENT_SIGNATURE_ANCHOR»` / `«CLIENT_DATE_ANCHOR»` markers (`apps/edge_api/src/proposals/template_render.py:40`, `apps/edge_api/src/proposals/template_render.py:106`, `apps/edge_api/src/proposals/template_render.py:120`, `apps/edge_api/src/proposals/template_render.py:129`, `apps/edge_api/src/proposals/template_render.py:263`, `apps/edge_api/src/proposals/template_render.py:287`).

`_TOKEN_RE` matches `{{snake_case}}` with optional inner whitespace (`apps/edge_api/src/proposals/template_render.py:43`); `substitute_tokens` leaves UNKNOWN tokens literal (`{{foo}}`) so an unfilled field stays visible, and HTML-escapes values by default (`apps/edge_api/src/proposals/template_render.py:59`, `apps/edge_api/src/proposals/template_render.py:69`); `extract_tokens` runs over the FULLY ASSEMBLED document in first-seen order (`apps/edge_api/src/proposals/template_render.py:51`).

`proposal_token_values` binds a real `Proposal` row to merge values (`client_name`, `client_signer_name`, `client_title`, `client_email`, `effective_date` long-date, `monthly_fee`, `duration`, `billing_cadence` via `_CADENCE_PHRASE`, `total`, `quarterly_total` legacy alias `= total`, `rs_name`) (`apps/edge_api/src/proposals/template_render.py:179`, `apps/edge_api/src/proposals/template_render.py:193`, `apps/edge_api/src/proposals/template_render.py:195`). `substitute_markdown_tokens` pre-renders `{{success_fee_table}}` → a GFM table from the structured `success_fee_schedule` (`apps/edge_api/src/proposals/template_render.py:167`, `apps/edge_api/src/proposals/template_render.py:172`).

### 9.3 The authoring surface (template registry)

`proposal_templates_v1` (prefix `/api/v1/proposal-templates`, all service-token gated) persists markdown to `business.global_engagement_content` and renders previews via DocRaptor → R2 presigned URL (`apps/edge_api/src/routers/proposal_templates_v1.py:46`): `POST /convert` (`:60`), `POST /preview` (TTL 3600s — `:67`, `:52`), `POST ''` (create draft 201 — `:88`), `GET ''` (list; `?published`, `?org_domain`), `GET /{id}`, `PUT /{id}`, `POST /{id}/publish` (name+slug, 409 on slug collision — `:144`, `:156`). `template_queries.get_published_by_slug` (`WHERE slug=%s AND status='published'`) is the slug resolution used by proposal-create + `_agreement_html` (`apps/edge_api/src/proposals/template_queries.py:98`, `apps/edge_api/src/proposals/template_queries.py:103`).

---

## 10. Cross-repo handoffs (SPA → BFF → edge_api)

The architecture invariant: `platform-app` → `platform-api` (dumb BFF) → `edge_api`. `render_mode` is resolved server-side by the BFF at confirm time; the client never supplies it.

| Stage | SPA call | BFF route | edge_api route |
|---|---|---|---|
| Create | `createProposal` (auth Bearer) (`rare-structure-hq:apps/platform-app/src/proposals/api.ts:170`) | `POST /api/v1/proposals` (requireUser) (`rare-structure-hq:apps/platform-api/src/routes/proposals-admin.ts:85`) | `POST /api/v1/proposals` (service-token) (`apps/edge_api/src/routers/proposals_v1.py:142`) |
| Confirm/Originate | `confirmProposal` (409→AlreadyOriginatedError) (`rare-structure-hq:apps/platform-app/src/proposals/api.ts:213`, `:224`) | `POST /api/v1/proposals/:ref/confirm` (requireUser, resolves render_mode) (`rare-structure-hq:apps/platform-api/src/routes/proposals-admin.ts:120`) | `POST /api/v1/proposals/{ref}/confirm` (`apps/edge_api/src/routers/proposals_v1.py:203`) |
| Public read | `getProposalShell` (no auth) (`rare-structure-hq:apps/platform-app/src/proposals/api.ts:245`) | `GET /api/v1/proposals/:ref` (no auth) (`rare-structure-hq:apps/platform-api/src/routes/proposals-admin.ts:185`) | `GET /api/v1/proposals/{ref}` (`apps/edge_api/src/routers/proposals_v1.py:286`) |
| Sign | `EmbedSignDocument` (token + host from shell) (`rare-structure-hq:apps/platform-app/src/routes/p/SignPage.tsx:36`) | — (embed talks to Documenso directly) | — |
| Document | — | — | `GET /api/v1/proposals/{ref}/document` (`apps/edge_api/src/routers/proposals_v1.py:321`) |
| Payment | `createPaymentIntent` / `getPaymentState` | `POST/GET /api/v1/proposals/:ref/payment-intent\|/payment` (`rare-structure-hq:apps/platform-api/src/routes/proposals-admin.ts:266`, `:288`) | proposal-ref payment routes (legacy Stripe ACH `us_bank_account`) |

### 10.1 render_mode resolution (BFF)

At confirm, the BFF reads `operator_settings.render_mode` for the signed-in operator (`auth_user_id`) and falls back to `DEFAULT_RENDER_MODE = 'through-docraptor'` when the row is absent, then forwards it in the edge_api `/confirm` body (`rare-structure-hq:apps/platform-api/src/routes/proposals-admin.ts:132`, `rare-structure-hq:apps/platform-api/src/routes/proposals-admin.ts:135`, `rare-structure-hq:apps/platform-api/src/routes/proposals-admin.ts:137`, `rare-structure-hq:apps/platform-api/src/routes/proposals-admin.ts:146`, `rare-structure-hq:packages/shared/src/schemas/settings.ts:15`).

`RenderMode = 'through-docraptor' | 'direct-to-documenso'` (`rare-structure-hq:packages/shared/src/schemas/settings.ts:10`): `through-docraptor` (default) renders the agreement PDF (DocRaptor) → Documenso envelope; `direct-to-documenso` is the no-DocRaptor pathway "wired separately" (`rare-structure-hq:packages/shared/src/schemas/settings.ts:7`, `rare-structure-hq:packages/shared/src/schemas/settings.ts:8`).

### 10.2 BFF status mapping

`mapStatus` collapses edge_api lifecycle to the shell enum: `paymentStatus 'succeeded' → 'paid'`; `draft → 'created'`; `signed|completed → 'signed'`; everything else (`sent|opened|rejected|voided`) → `'sent'`. `'paid'` is reserved for a settled ACH debit only (`rare-structure-hq:apps/platform-api/src/routes/proposals-admin.ts:63`, `:64`, `:67`, `:72`).

### 10.3 Embedded signing

The prospect signs via the SPA `SignPage` using `@documenso/embed-react` `EmbedSignDocument` with `token = shell.signingToken` and `host = shell.documensoHost` (`DOCUMENSO_APP_URL`, default `https://app.documenso.com`); on completion it navigates back to `/p/{ref}` (`justSigned`) (`rare-structure-hq:apps/platform-app/src/routes/p/SignPage.tsx:10`, `:36`, `:37`, `:43`). **The `onDocumentCompleted` browser callback is a UI nav ONLY** — status truth comes from the Documenso webhook capture, never this callback.

---

## Status: ACTIVE / CONDITIONAL / DEPRECATED / STUB

| Component | Status | Note |
|---|---|---|
| `POST /api/v1/proposals` (`create_proposal`) | ACTIVE | Mint DRAFT (`apps/edge_api/src/routers/proposals_v1.py:142`) |
| `POST /api/v1/proposals/{ref}/confirm` (`confirm_proposal`) | ACTIVE | Originate entrypoint (`apps/edge_api/src/routers/proposals_v1.py:203`) |
| `POST /api/v1/proposals/{ref}/provision` (`provision_proposal`) | ACTIVE | Re-provision; NO BFF broker (`apps/edge_api/src/routers/proposals_v1.py:254`) |
| `GET /api/v1/proposals` (`list_proposals`) | ACTIVE | Operator list (`apps/edge_api/src/routers/proposals_v1.py:272`) |
| `GET /api/v1/proposals/{ref}` (`get_proposal`) | ACTIVE | PUBLIC consumer read (`apps/edge_api/src/routers/proposals_v1.py:286`) |
| `GET /api/v1/proposals/{ref}/document` (`get_signed_document`) | ACTIVE | PUBLIC sealed-PDF stream (`apps/edge_api/src/routers/proposals_v1.py:321`) |
| `_provision` (through-docraptor branch) | ACTIVE | Render + envelope + bind (`apps/edge_api/src/routers/proposals_v1.py:104`) |
| `_provision` (direct-to-documenso branch) | **STUB** | `'direct-to-documenso pathway not yet wired'` (`apps/edge_api/src/routers/proposals_v1.py:99`-103) |
| `docraptor_client.render_pdf` | ACTIVE | LIVE mode `test=False` (`apps/edge_api/src/services/docraptor_client.py:27`) |
| `documenso_client.create_signing_envelope` | ACTIVE | through-docraptor envelope creator (`apps/edge_api/src/services/documenso_client.py:198`) |
| `documenso_client.download_signed_pdf` | ACTIVE | sealed PDF fetch (`apps/edge_api/src/services/documenso_client.py:762`) |
| `POST /api/v1/documenso/webhook` (`documenso_webhooks_v1`) | ACTIVE | Live raw capture, system of record (`apps/edge_api/src/routers/documenso_webhooks_v1.py:39`) |
| `POST /api/v1/proposals/webhook` (in-router) | **DEPRECATED** | Code intact; repointed away, no traffic (`apps/edge_api/src/routers/proposals_v1.py:337`, `apps/edge_api/src/routers/documenso_webhooks_v1.py:5`) |
| `queries.advance_status` | **CONDITIONAL** | Correct logic; reachable only from the deprecated webhook (`apps/edge_api/src/proposals/queries.py:192`) |
| `render_mode == 'direct-to-documenso'` (in proposals_v1) | **STUB** | Live direct path is `engagement-mandate-drafts`, not this branch |
| `business.engagement_proposals` | ACTIVE | E-signature grain + payment overlay (`apps/edge_api/sql/engagement_proposals.sql:21`) |
| `business.documenso_webhook_events` | ACTIVE | Raw Documenso capture, live signing-state SoR (`apps/edge_api/src/routers/documenso_webhooks_v1.py:7`) |
| `proposal_templates_v1` (authoring) | ACTIVE | Markdown registry + preview (`apps/edge_api/src/routers/proposal_templates_v1.py:46`) |
| Status-projection back onto `engagement_proposals.status` (proposals/ref lane, post-repoint) | **UNVERIFIED** | No projection path found this pass — see §6 open question |

---

## Traps

- **STALE COMMENT — the in-router webhook's docstring lies.** `apps/edge_api/src/routers/proposals_v1.py:341` reads `"Documenso → status advance. Source of truth (never the client embed callback)."` This is now FALSE: the route is DEPRECATED, Documenso is repointed to `POST /api/v1/documenso/webhook`, and the legacy route no longer receives deliveries (`apps/edge_api/src/routers/documenso_webhooks_v1.py:5`, `:8`). Do NOT treat the proposals webhook as the live status source. (A prior internal dossier made exactly this mistake.)
- **`direct-to-documenso` is NOT wired in this router.** The `render_mode == 'direct-to-documenso'` branch in `_provision` is a STUB that returns `(False, 'direct-to-documenso pathway not yet wired')` (`apps/edge_api/src/routers/proposals_v1.py:99`-103). The LIVE direct path is the separate `engagement-mandate-drafts` flow (`create_document_from_template` / `create_document_from_template_with_custom_pdf`), selected by `operator_settings.direct_to_documenso_lane`. Do not conflate the two.
- **`/provision` has no BFF caller.** It is a direct service-token-only re-provision/recovery endpoint. The SPA/BFF only broker create/confirm/list/read/send/payment. Searching the BFF for "provision" returns only the `provisioned`/`provision_error` field names.
- **`quarterly_total_cents` is a misleading legacy column name.** It now carries `{{total}} = monthly_fee_cents * duration_months`, NOT a quarterly figure (`apps/edge_api/sql/engagement_proposals.sql:36`, `apps/edge_api/src/proposals/template_render.py:195`). The per-invoice quarterly amount comes from `charge_cents(..., 'quarterly')`, a different computation (`apps/edge_api/src/proposals/models.py:61`).
- **`business.proposals` ≠ `business.engagement_proposals`.** `business.proposals` belongs to the DMaaS data-transfer subsystem; the e-signature table is `business.engagement_proposals` (`apps/edge_api/sql/engagement_proposals.sql:4`-6).
- **Anchor grammar `[[…]]` vs merge grammar `{{…}}` are intentionally different.** `[[CLIENT_SIGNATURE]]` / `[[CLIENT_DATE]]` must survive token substitution untouched and land verbatim in the PDF text layer for Documenso `findText`. Do not "normalize" anchors to `{{…}}` or HTML-escape them — they ride in not-escaped (`apps/edge_api/src/proposals/agreement_template.py:52`).
- **DocRaptor runs LIVE (`test=False`), not test mode** (`apps/edge_api/src/services/docraptor_client.py:37`). Every render bills against the live DocRaptor account.
- **No script fonts in the rendered PDF.** Prince/DocRaptor blocks filesystem font access, so signatures are typed-name italic serif; naming a script font yields a 422 (`apps/edge_api/src/proposals/agreement_template.py:107`-110). Signature/date anchor text is real selectable text, never `display:none`/zero-size, or `findText` can't resolve it (`apps/edge_api/src/proposals/agreement_template.py:113`-117).
- **The embed host must equal `DOCUMENSO_API_URL`.** The token is minted against `config.documenso_api_url()`; the SPA must embed against the SAME host (`shell.documensoHost`, default `https://app.documenso.com`). A mismatch silently breaks signing (`apps/edge_api/src/services/documenso_client.py:82`, `rare-structure-hq:apps/platform-api/src/lib/edge.ts:19`).
- **`onDocumentCompleted` is UI navigation only**, never a status source (`rare-structure-hq:apps/platform-app/src/routes/p/SignPage.tsx:43`). Trust the webhook capture.
- **Sign-token path-shape mismatch (carry-forward note).** The SPA calls `/documenso/sign/{opp}/{doc}/token` while the BFF/edge_api use `/documenso/sign-token/{opp}/{doc}` — different path shapes for the direct-lane embed token (`rare-structure-hq:apps/platform-app/src/proposals/api.ts:134`, `apps/edge_api/src/routers/documenso_webhooks_v1.py:105`). Out of scope for this through-docraptor lane but noted so an agent doesn't assume a single canonical shape.
- **DISCREPANCY vs `DIRECT_TO_DOCUMENSO_PAYMENT_E2E.md`.** That reference covers ONLY the direct prefill lane + its payment. For THIS through-docraptor domain the authoritative behavior is: `through-docraptor` renders PDF → envelope (live), and the `direct-to-documenso` branch inside `proposals_v1._provision` is a non-functional stub (`apps/edge_api/src/routers/proposals_v1.py:96`-103). That markdown file was not opened this pass; the discrepancy is stated against its documented scope, with CODE treated as ground truth.
