# Through-DocRaptor Proposals Flow (engagement agreement + e-signature) — ARCHIVED

> **⚠️ ARCHIVED — REMOVED SYSTEM, HISTORICAL ONLY.** The entire `render_mode == 'through-docraptor'` proposals backend was **DELETED** in commit `b83e002` (`refactor(edge_api): remove legacy through-docraptor proposal + payment backend`, #533). That commit removed `apps/edge_api/src/routers/proposals_v1.py` (create/confirm/provision/ref-read/signed-pdf + the in-router webhook), `apps/edge_api/src/proposals/agreement_template.py`, `apps/edge_api/src/proposals/queries.py`, and `documenso_client.create_signing_envelope`. None of those files/functions exist on current `main` (verified: `git show --stat b83e002`; `proposals_v1.py` returns "No such file"; `grep 'create_signing_envelope' src/services/documenso_client.py` returns nothing). The router is no longer imported or mounted in `apps/edge_api/main.py`. This file is kept as the historical account of the removed lane — every endpoint and helper below is **REMOVED**, NOT active. Do NOT cite this document as a description of live behavior.
>
> **WHERE THE LIVE FLOW IS NOW.** Engagement origination is the **direct-to-documenso** flow in `apps/edge_api/src/routers/engagement_mandate_drafts_v1.py`, with two parallel live originate lanes plus a separate render+push lane:
> - **`prefill-document-from-template`** (DEFAULT) — `POST /api/v1/engagement-mandate-drafts/{draft_id}/originate-prefilled` mints a Documenso document NOW from the draft's template via `/api/v2/template/use`, prefilled with per-deal field values (`engagement_mandate_drafts_v1.py:113`, `documenso_client.create_document_from_template`).
> - **`embed-template`** — `POST /api/v1/engagement-mandate-drafts/{draft_id}/originate-embed-template` enables a Documenso DIRECT LINK on the template and returns a reusable token; NO document is minted until the signer completes (Documenso then creates it, source `TEMPLATE_DIRECT_LINK`) (`engagement_mandate_drafts_v1.py:172`, `documenso_client.create_direct_link`).
> - **render+push** — `POST /internal/engagement-templates/render-push` (trigger-secret) renders an engagement-content source via DocRaptor and PUSHes it to Documenso as a TEMPLATE (`internal_engagement_templates_v1.py:84`, `documenso_client.create_template_from_pdf`).
>
> The lane is selected by `public.operator_settings.direct_to_documenso_lane` (`apps/edge_api/sql/operator_settings.sql:85`). See `01-MODES-AND-LANES.md` and `03-FLOW-direct-to-documenso.md` for the live architecture. §11 below maps the removed surface to its live replacement.

## Orientation (historical)

This WAS the engagement-agreement + e-signature pathway in `core-x` `edge_api`, keyed on an unguessable capability ref (`rs_…`). An operator (via the `platform-api` BFF) minted a DRAFT proposal, then originated it: confirm stamped the operator's locked-in structured pricing onto the still-draft row, rendered a legal PDF via DocRaptor (PrinceXML, LIVE mode), created a Documenso v2 envelope from that PDF with the Client as the sole `SIGNER`, placed `SIGNATURE`/`DATE` fields BY ANCHOR (`[[CLIENT_SIGNATURE]]` / `[[CLIENT_DATE]]` resolved via Documenso `findText`), distributed WITHOUT email, and bound the envelope + signing token to the row (`draft` → `sent`). The prospect read a PUBLIC projection and signed in the embedded Documenso surface. `render_mode` was resolved server-side by the BFF from `public.operator_settings.render_mode`; the client never supplied it at confirm time. Persistence was on `business.engagement_proposals`; the Stripe ACH payment overlay was additive columns on that same row.

**All of that origination backend was removed in `b83e002`.** The `business.engagement_proposals` DDL still exists (`apps/edge_api/sql/engagement_proposals.sql` — data preserved) but is no longer written or read by any `.py` (verified: `grep -rn 'engagement_proposals' apps/edge_api/src/ --include='*.py'` returns nothing). The live engagement workflow uses the `engagement_mandate_drafts` flow described in the banner.

This WAS one of several Documenso lanes. The live `direct-to-documenso` path (`engagement-mandate-drafts`) and the document-payment flow are separate domains. **Note vs prior internal docs:** the in-router webhook `POST /api/v1/proposals/webhook` was not merely deprecated — it was **REMOVED entirely** with the rest of `proposals_v1.py`. The live webhook is the raw-capture `POST /api/v1/documenso/webhook` (`documenso_webhooks_v1.py`), which is still active. See §6.

---

## 1. The capability ref (`rs_…`) — REMOVED

In the removed lane, the `ref` was minted on create as `"rs_" + secrets.token_urlsafe(16)` — an unguessable capability token that was simultaneously the **primary key**, the **URL slug**, and the **bearer credential**. It was the PRIMARY KEY of `business.engagement_proposals` (`apps/edge_api/sql/engagement_proposals.sql:23` — DDL still present, table unused). Because the ref WAS the credential, the public read and document routes were unauthenticated — possession of the ref was authorization. The minting helper lived in `apps/edge_api/src/proposals/queries.py`, **deleted in `b83e002`** (248 lines removed).

The live flow keys on `business.opportunities.opportunity_id` (an 8-char PUBLIC handle), not an `rs_…` ref — see `engagement_mandate_drafts_v1.py:137` and §11.

---

## 2. Edge_api routes (`proposals_v1`) — REMOVED

> **The `proposals_v1` router was deleted in `b83e002` and is no longer imported or mounted.** `apps/edge_api/src/routers/proposals_v1.py` does not exist on current `main`; `grep -n 'proposals_v1' apps/edge_api/main.py` returns nothing. The line references in this section point at the pre-removal file for historical traceability only. Every endpoint below has status **REMOVED**.

| Method + path | Function | Auth | Status | Live replacement |
|---|---|---|---|---|
| `POST /api/v1/proposals` | `create_proposal` | service-token | **REMOVED** | `POST /api/v1/engagement-mandate-drafts` (`engagement_mandate_drafts_v1.py:67`) |
| `POST /api/v1/proposals/{ref}/confirm` | `confirm_proposal` | service-token | **REMOVED** | `POST /api/v1/engagement-mandate-drafts/{draft_id}/originate-prefilled` (`engagement_mandate_drafts_v1.py:113`) |
| `POST /api/v1/proposals/{ref}/provision` | `provision_proposal` | service-token | **REMOVED** | No equivalent — templates are pre-created (render+push) and documents instantiated at originate time |
| `GET /api/v1/proposals` | `list_proposals` | service-token | **REMOVED** | None — proposals are no longer created or listed via this flow |
| `GET /api/v1/proposals/{ref}` | `get_proposal` | PUBLIC | **REMOVED** | Document shell read via the direct-to-documenso `(opportunity_id, document_id)` sign-token surface |
| `GET /api/v1/proposals/{ref}/document` | `get_signed_document` | PUBLIC | **REMOVED** | Signed PDF downloaded from Documenso directly via the platform-api/SPA integration |
| `POST /api/v1/proposals/webhook` | `documenso_webhook` | `X-Documenso-Secret` | **REMOVED** | `POST /api/v1/documenso/webhook` (`documenso_webhooks_v1.py:39`) — still active |

> **§§2.1–2.6 below describe REMOVED code (`b83e002`).** They are retained verbatim as the historical record of how the lane behaved; the cited `proposals_v1.py` line numbers reference the pre-removal file. None of these functions exist on current `main`.

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

### 2.6 `documenso_webhook` (in-router) — REMOVED

In the removed lane, this route was service-gated by `X-Documenso-Secret` (constant-time compare via `verify_webhook_secret`), returned 503 if the secret was unconfigured and 401 on mismatch, normalized via `documenso_client.normalize_event`, resolved the proposal by `externalId` (the ref) first then `get_by_envelope`, set `signed_url = /api/v1/proposals/{ref}/document` only when `status == 'completed'`, and advanced via `queries.advance_status` on the stored envelope id.

**The route was DELETED with the rest of `proposals_v1.py` in `b83e002` — it is REMOVED, not merely deprecated/repointed.** The live webhook is the raw-capture `POST /api/v1/documenso/webhook` (same shared `DOCUMENSO_WEBHOOK_SECRET`), which is still active (`apps/edge_api/src/routers/documenso_webhooks_v1.py:5`, `apps/edge_api/src/routers/documenso_webhooks_v1.py:8`, `apps/edge_api/src/routers/documenso_webhooks_v1.py:39`). See §6.

---

## 3. `_provision` — render + envelope step — REMOVED

> **`_provision`, `_title`, `_agreement_html`, and `_field_values` all lived in `proposals_v1.py`, deleted in `b83e002`.** The through-docraptor envelope creation (`documenso_client.create_signing_envelope`) was removed in the same commit. The live originate path mints documents from pre-created Documenso templates (`create_document_from_template`) or enables a template direct link (`create_direct_link`); there is no in-request DocRaptor-render-then-create-envelope step. §§3.1–3.3 are retained as historical record; the cited line numbers reference the pre-removal file.

`_provision(conn, p, render_mode)` was **non-raising** — it returned `tuple[bool, str | None]` (`apps/edge_api/src/routers/proposals_v1.py:89`, `apps/edge_api/src/routers/proposals_v1.py:90`).

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

> **Historical note (no longer true):** in the removed code, the `direct-to-documenso` branch WAS a stub returning `(False, 'direct-to-documenso pathway not yet wired')`. That stub, and the entire `_provision` function, were deleted in `b83e002`. The `direct-to-documenso` path is now the FULLY LIVE `engagement-mandate-drafts` flow (§11), not a stub.

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

## 5. Documenso v2 envelope by anchor — REMOVED

> **`documenso_client.create_signing_envelope` was deleted in `b83e002`** (verified: `grep -n 'create_signing_envelope' apps/edge_api/src/services/documenso_client.py` returns nothing). The 4-step envelope-by-anchor creation below no longer exists. The signing-anchor sentinels (`signing_anchors.py`) survive in the repo but are no longer wired into an envelope creator on this lane. The LIVE Documenso integration uses template-direct linking and template-use instead — see §11.4 and `04-DOCUMENSO-INTEGRATION.md`. §§5.1–5.4 are historical; cited line numbers reference the pre-removal file (except §5.4 `download_signed_pdf`, which still exists — see note in §5.4).

`documenso_client.create_signing_envelope` WAS the through-docraptor envelope creator — 4 steps against Documenso Cloud v2:

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

`download_signed_pdf` **still exists** (`apps/edge_api/src/services/documenso_client.py:711`) — it survived `b83e002`; only its proposals-lane caller (`get_signed_document`) was removed. It resolves the numeric document id from the envelope, GETs `/api/v2/document/{numeric}/download?version=signed`, and returns bytes directly for `application/pdf` or a `%PDF-` magic; otherwise it follows a JSON `downloadUrl` with a BARE `httpx` client (never attaching the Documenso API key to the third-party presigned host).

---

## 6. Status truth — the webhook (CRITICAL)

**The live status-of-record is the raw-capture webhook. The in-router proposals webhook was REMOVED, not just repointed.**

`POST /api/v1/documenso/webhook` (`documenso_webhooks_v1`, prefix `/api/v1/documenso`) is `X-Documenso-Secret` gated and append-inserts EVERY raw Documenso event into `business.documenso_webhook_events` with NO normalization/projection (`apps/edge_api/src/routers/documenso_webhooks_v1.py:39`, mounted at `apps/edge_api/main.py:219`). It is **still active** and survived `b83e002`. Its module docstring (written before the legacy route's deletion) states Documenso "is repointed here from the legacy `/api/v1/proposals/webhook` (same shared `DOCUMENSO_WEBHOOK_SECRET`)" (`apps/edge_api/src/routers/documenso_webhooks_v1.py:5`, `apps/edge_api/src/routers/documenso_webhooks_v1.py:8`); that legacy route has since been DELETED outright.

```
Documenso  --(X-Documenso-Secret)-->  POST /api/v1/documenso/webhook   [ACTIVE, raw capture]
                                          |
                                          +--> append-insert RAW into business.documenso_webhook_events
                                                (every event, no filter/normalize/project)

POST /api/v1/proposals/webhook   [REMOVED in b83e002 — route, normalize→advance_status, and
                                  proposals/queries.advance_status all deleted with proposals_v1.py]
```

`normalize_event` **still exists** (`apps/edge_api/src/services/documenso_client.py:752`) and maps Documenso event names → internal status via `_EVENT_TO_STATUS`:

| Documenso event | Internal status |
|---|---|
| `DOCUMENT_SENT` | `sent` |
| `DOCUMENT_OPENED` | `opened` |
| `DOCUMENT_SIGNED` | `signed` |
| `DOCUMENT_COMPLETED` | `completed` |
| `DOCUMENT_REJECTED` | `rejected` |
| `DOCUMENT_CANCELLED` | `voided` |

It folds both lowercase-dotted (`document.completed`) and enum forms to the enum key and reads `envelope_id` from payload `id`/`documentId`/`envelopeId` and `external_id` from `payload.externalId`. (`normalize_event` survives but its only former in-tree consumer — the removed proposals webhook — is gone; raw capture does no normalization.)

`verify_webhook_secret` (`apps/edge_api/src/services/documenso_client.py:740`) does an `hmac.compare_digest` constant-time compare of `X-Documenso-Secret` against `config.documenso_webhook_secret()`, returning False (refuse) when the secret is unconfigured. It is used by the live raw-capture webhook route.

> **RESOLVED (was an open question):** With the proposals/ref lane removed (`b83e002`), there is no `engagement_proposals.status` projection path because the table is no longer written. Status truth for the live direct lane is resolved on the `(opportunity_id, document_id)` surface — see `03-FLOW-direct-to-documenso.md`.

---

## 7. Persistence — `business.engagement_proposals` + queries

> **The `business.engagement_proposals` DDL still exists (data preserved) but is no longer actively used** — `apps/edge_api/src/proposals/queries.py` was deleted in `b83e002` (248 lines), and `grep -rn 'engagement_proposals' apps/edge_api/src/ --include='*.py'` returns zero references. The live workflow persists to `business.engagement_mandate_draft_content` + `business.opportunity_specific_content` and originates against Documenso directly — see §11 and `07-DATA-STORES.md`. §7.1 (the table) remains a correct description of the surviving DDL; §7.3 (durability queries) describes REMOVED code.

### 7.1 The table

`business.engagement_proposals` is the e-signature grain table CREATEd by edge_api DDL on the HQX control-plane Postgres; idempotent DDL applied at boot (`main.py:138` `run_migrations()` + `config.py:215` `db_migrate_on_boot` default ON) (`apps/edge_api/sql/engagement_proposals.sql:21`). `ref` is PRIMARY KEY (`apps/edge_api/sql/engagement_proposals.sql:23`). `documenso_envelope_id` has a PARTIAL UNIQUE index `engagement_proposals_envelope_uidx WHERE documenso_envelope_id IS NOT NULL` (`apps/edge_api/sql/engagement_proposals.sql:62`, `apps/edge_api/sql/engagement_proposals.sql:63`) — so the webhook UPDATE hits ≤1 row and an accidental rebind fails loudly.

The DDL note records the naming reason: `business.proposals` is ALREADY TAKEN by the DMaaS data-transfer subsystem, hence `business.engagement_proposals` (`apps/edge_api/sql/engagement_proposals.sql:4`, `apps/edge_api/sql/engagement_proposals.sql:5`, `apps/edge_api/sql/engagement_proposals.sql:6`).

Representative columns (`apps/edge_api/sql/engagement_proposals.sql:23`-55): `ref` (PK), `template_id` (default `'strategic_origination_mandate'`), `client_name`, `client_signer_name`, `client_title`, `client_email`, `effective_date`, `monthly_fee_cents` (bigint), `duration_months`, `billing_cadence`, `success_fee_schedule` (jsonb), `quarterly_total_cents` (bigint; legacy name, now carries `{{total}} = monthly*duration` — `apps/edge_api/sql/engagement_proposals.sql:36`), `rs_signer_name`, `status` (CHECK enum, default `'draft'` — `apps/edge_api/sql/engagement_proposals.sql:39`), `documenso_envelope_id` (`apps/edge_api/sql/engagement_proposals.sql:42`), `documenso_client_token`, `signed_pdf_url`, `field_values` (jsonb NOT NULL DEFAULT `'{}'` — `apps/edge_api/sql/engagement_proposals.sql:46`), `created_by`, and the timeline columns `created_at`/`sent_at`/`opened_at`/`signed_at`/`completed_at`/`updated_at` (`apps/edge_api/sql/engagement_proposals.sql:49`).

### 7.2 Status enum

`ProposalStatus` is one of `draft | sent | opened | signed | completed | rejected | voided`, declared in the model (`apps/edge_api/src/proposals/models.py:38`) and CHECK-constrained in the DDL (`apps/edge_api/sql/engagement_proposals.sql:39`, `apps/edge_api/sql/engagement_proposals.sql:40`).

### 7.3 The durability queries — REMOVED

> All functions below lived in `apps/edge_api/src/proposals/queries.py`, **deleted in `b83e002`**. Retained as historical record of the removed durability model.

- `queries.insert_proposal` inserts with `status` hardcoded `'draft'` and commits; `success_fee_schedule` and `field_values` stored as `Jsonb` (`apps/edge_api/src/proposals/queries.py:49`, `apps/edge_api/src/proposals/queries.py:70`, `apps/edge_api/src/proposals/queries.py:79`, `apps/edge_api/src/proposals/queries.py:84`).
- `queries.update_pricing` UPDATEs ONLY `WHERE ref=%s AND status='draft'` (the durability gate) and RETURNS the updated row or None on a non-draft row — origination terms become immutable once the envelope is bound (`apps/edge_api/src/proposals/queries.py:88`, `apps/edge_api/src/proposals/queries.py:116`, `apps/edge_api/src/proposals/queries.py:130`).
- `queries.attach_envelope` binds `documenso_envelope_id` + `documenso_client_token` and moves `draft → sent` (CASE), setting `sent_at = COALESCE(sent_at, now())`, ONLY `WHERE documenso_envelope_id IS NULL` (no silent rebind); returns whether one row was bound (`apps/edge_api/src/proposals/queries.py:160`, `apps/edge_api/src/proposals/queries.py:173`, `apps/edge_api/src/proposals/queries.py:176`, `apps/edge_api/src/proposals/queries.py:180`).
- `queries.advance_status` applies a webhook-driven transition atomically/idempotently with the monotonic/terminal guard IN the UPDATE WHERE predicate. Forward-chain (rank `draft0 → sent1 → opened2 → signed3 → completed4`) applies only when strictly ahead (`apps/edge_api/src/proposals/queries.py:214`); terminal (`rejected`/`voided`) applies unless already in `('rejected','voided','completed')` (`apps/edge_api/src/proposals/queries.py:211`, `apps/edge_api/src/proposals/queries.py:212`). Returns whether a row changed (`apps/edge_api/src/proposals/queries.py:222`). **REMOVED** — deleted in `b83e002` along with its only caller (the in-router proposals webhook); no status-advance projection exists for this table anymore.
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

> **Split survival:** §9.1 (`render_agreement_html` / `agreement_template.py`) is **REMOVED** — `apps/edge_api/src/proposals/agreement_template.py` was deleted in `b83e002` (319 lines; `grep -rn 'render_agreement_html' apps/edge_api/src` returns nothing). §9.2 (`template_render.py`: `render_template_html`, `render_branded_document`, `substitute_tokens`, `proposal_token_values`, `substitute_markdown_tokens`, `extract_tokens`) and §9.3 (`proposal_templates_v1` + `template_queries.get_published_by_slug`) **still exist** — but with `_agreement_html` and `create_signing_envelope` gone, this authoring surface no longer renders into a signed proposal/envelope; it is markdown storage + DocRaptor PDF preview only.

### 9.1 Built-in Strategic Origination Mandate — REMOVED

`render_agreement_html` WAS the built-in "Strategic Origination Mandate" — a style-agnostic legal body with a `«STYLE»` injection slot; default `_PLAIN_STYLE` (neutral white/black serif), with `_BRAND_STYLE` (Rare Structure dark identity) available (`apps/edge_api/src/proposals/agreement_template.py:1`, `apps/edge_api/src/proposals/agreement_template.py:34`, `apps/edge_api/src/proposals/agreement_template.py:67`). Merge tokens use a `«TOKEN»` sentinel substituted by `str.replace` (NOT `str.format`) so print-CSS braces are untouched; the `[[CLIENT_SIGNATURE]]` / `[[CLIENT_DATE]]` anchors ride in NOT-escaped (`apps/edge_api/src/proposals/agreement_template.py:52`, `apps/edge_api/src/proposals/agreement_template.py:55`).

The execution block pre-renders RS as the pre-signed party (typed-name italic serif, NOT a script font because DocRaptor/Prince blocks filesystem font access — named script fonts 422) and leaves the CLIENT signature + date lines carrying the anchor markers in `.sig-anchor` (real selectable text in a standard font; never `display:none`/zero-size) (`apps/edge_api/src/proposals/agreement_template.py:107`, `apps/edge_api/src/proposals/agreement_template.py:113`, `apps/edge_api/src/proposals/agreement_template.py:296`, `apps/edge_api/src/proposals/agreement_template.py:309`, `apps/edge_api/src/proposals/agreement_template.py:313`).

### 9.2 Authored-template render

`render_template_html` / `render_branded_document` render operator-authored markdown via `markdown-it-py` (commonmark + table + strikethrough, `html=False` so embedded raw HTML/`<script>` is escaped) into the Rare Structure brand SHELL (`_SHELL`), which appends the execution block with `{{rs_name}}` / `{{client_signer_name}}` / `{{client_title}}` / `{{effective_date}}` tokens and the `«CLIENT_SIGNATURE_ANCHOR»` / `«CLIENT_DATE_ANCHOR»` markers (`apps/edge_api/src/proposals/template_render.py:40`, `apps/edge_api/src/proposals/template_render.py:106`, `apps/edge_api/src/proposals/template_render.py:120`, `apps/edge_api/src/proposals/template_render.py:129`, `apps/edge_api/src/proposals/template_render.py:263`, `apps/edge_api/src/proposals/template_render.py:287`).

`_TOKEN_RE` matches `{{snake_case}}` with optional inner whitespace (`apps/edge_api/src/proposals/template_render.py:43`); `substitute_tokens` leaves UNKNOWN tokens literal (`{{foo}}`) so an unfilled field stays visible, and HTML-escapes values by default (`apps/edge_api/src/proposals/template_render.py:59`, `apps/edge_api/src/proposals/template_render.py:69`); `extract_tokens` runs over the FULLY ASSEMBLED document in first-seen order (`apps/edge_api/src/proposals/template_render.py:51`).

`proposal_token_values` binds a real `Proposal` row to merge values (`client_name`, `client_signer_name`, `client_title`, `client_email`, `effective_date` long-date, `monthly_fee`, `duration`, `billing_cadence` via `_CADENCE_PHRASE`, `total`, `quarterly_total` legacy alias `= total`, `rs_name`) (`apps/edge_api/src/proposals/template_render.py:179`, `apps/edge_api/src/proposals/template_render.py:193`, `apps/edge_api/src/proposals/template_render.py:195`). `substitute_markdown_tokens` pre-renders `{{success_fee_table}}` → a GFM table from the structured `success_fee_schedule` (`apps/edge_api/src/proposals/template_render.py:167`, `apps/edge_api/src/proposals/template_render.py:172`).

### 9.3 The authoring surface (template registry)

`proposal_templates_v1` (prefix `/api/v1/proposal-templates`, all service-token gated) persists markdown to `business.global_engagement_content` and renders previews via DocRaptor → R2 presigned URL (`apps/edge_api/src/routers/proposal_templates_v1.py:46`): `POST /convert` (`:60`), `POST /preview` (TTL 3600s — `:67`, `:52`), `POST ''` (create draft 201 — `:88`), `GET ''` (list; `?published`, `?org_domain`), `GET /{id}`, `PUT /{id}`, `POST /{id}/publish` (name+slug, 409 on slug collision — `:144`, `:156`). `template_queries.get_published_by_slug` (`WHERE slug=%s AND status='published'`) is the slug resolution used by proposal-create + `_agreement_html` (`apps/edge_api/src/proposals/template_queries.py:98`, `apps/edge_api/src/proposals/template_queries.py:103`).

---

## 10. Cross-repo handoffs (SPA → BFF → edge_api) — edge_api side REMOVED

> **Every `edge_api` route in this table was removed in `b83e002` (REMOVED, see §2).** The SPA/BFF (the `rare-structure-hq` repo, cross-repo — not verifiable from `core-x`) targeted those routes; whether the SPA/BFF callers still exist or have been repointed to the `engagement-mandate-drafts` lane must be checked in that repo (`08-FRONTEND-AND-BFF.md` covers the live SPA/BFF). The live cross-repo contract is the direct-to-documenso prefill + embed-template lanes — see §11.5. The table is retained as the historical handoff map.

The architecture invariant WAS: `platform-app` → `platform-api` (dumb BFF) → `edge_api`. `render_mode` was resolved server-side by the BFF at confirm time; the client never supplied it.

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

## 11. The live replacement (post-`b83e002`)

This section is the forward map from the removed through-docraptor lane to the live engagement-origination system. It is intentionally a pointer/summary — the full live architecture is in `01-MODES-AND-LANES.md`, `03-FLOW-direct-to-documenso.md`, and `04-DOCUMENSO-INTEGRATION.md`.

### 11.1 Removed → live route map

| Removed (`proposals_v1`, `b83e002`) | Live (`engagement_mandate_drafts_v1`) |
|---|---|
| `POST /api/v1/proposals` (`create_proposal`) | `POST /api/v1/engagement-mandate-drafts` (`create_mandate_draft`, `engagement_mandate_drafts_v1.py:67`) |
| `POST /api/v1/proposals/{ref}/confirm` (`confirm_proposal`) | `POST /api/v1/engagement-mandate-drafts/{draft_id}/originate-prefilled` (`engagement_mandate_drafts_v1.py:113`) — DEFAULT lane |
| `POST /api/v1/proposals/{ref}/provision` (`provision_proposal`) | — (no equivalent; templates pre-created via render+push) |
| `GET /api/v1/proposals/{ref}` (`get_proposal`) | `GET /api/v1/engagement-mandate-drafts/by-opportunity/{opportunity_id}` (`get_staging_by_opportunity`, `engagement_mandate_drafts_v1.py:80`) for staging; signed doc read via the `(opportunity_id, document_id)` sign-token surface |
| `GET /api/v1/proposals/{ref}/document` (`get_signed_document`) | Signed PDF from Documenso directly (`download_signed_pdf` survives) |
| `POST /api/v1/proposals/webhook` (`documenso_webhook`) | `POST /api/v1/documenso/webhook` (`documenso_webhooks_v1.py:39`) |

### 11.2 Lane selection (`operator_settings.direct_to_documenso_lane`)

Under `render_mode == 'direct-to-documenso'`, the sub-lane is a SECOND independent selector `public.operator_settings.direct_to_documenso_lane` (`apps/edge_api/sql/operator_settings.sql:43`), CHECK-constrained to three values (`operator_settings.sql:85`):

| Lane | Status | Behavior |
|---|---|---|
| `prefill-document-from-template` | **DEFAULT** (`operator_settings.sql:43`, `:50`) | `originate-prefilled` → `/api/v2/template/use`, mint a document NOW with per-deal values prefilled |
| `embed-template` | ACTIVE | `originate-embed-template` → Documenso DIRECT LINK on the template; signer self-identifies, document minted by Documenso at completion |
| `envelope-distribute` | **RETIRED** (`operator_settings.sql:31`, `:80`) | `/envelope/use` lane removed in code; the CHECK value is retained so a pre-existing row never violates |

### 11.3 The two live originate lanes

- **`originate-prefilled`** (`engagement_mandate_drafts_v1.py:113`) — reads the draft + the opportunity's prefill/contact, then calls `documenso_client.create_document_from_template(...)` (`/api/v2/template/use`) with `field_values_by_label` prefilled, `external_id` = the opportunity's 8-char PUBLIC handle, title `"Engagement Agreement"`, distributing `NONE` → `PENDING`. Returns `MandatePrefilledOriginated {envelope_id, document_id, opportunity_id, signing_token, status='pending', documenso_host}`.
- **`originate-embed-template`** (`engagement_mandate_drafts_v1.py:172`) — PARALLEL to `originate-prefilled` (which is untouched). Reads the template's recipients (`get_template_recipients`), designates the counterparty direct recipient (`_pick_direct_recipient_id`, or `body.direct_recipient_id`), enables a DIRECT LINK (`create_direct_link`), and returns `MandateEmbedTemplateOriginated {direct_token, documenso_host, embed_url=f"{host}/embed/direct/{token}", external_id, opportunity_id, direct_recipient_id, recipient_email, recipient_name, status='ready'}`. **No document is minted here** — Documenso creates it (source `TEMPLATE_DIRECT_LINK`) when the signer completes.

### 11.4 Documenso client (live methods)

`apps/edge_api/src/services/documenso_client.py` (`create_signing_envelope` removed):

- `create_document_from_template(...)` — `/api/v2/template/use`; embed-document / prefill lane (`documenso_client.py:228`).
- `create_template_from_pdf(*, title, pdf, recipients=None, filename=None) -> TemplateCreateResult` — `/api/v2/envelope/create` with `type=TEMPLATE`; the render+push terminal step (`documenso_client.py:420`). Field placement is NOT done here (the engagement HTML is a static blank body).
- `get_template_recipients(documenso_template_id) -> list[dict]` — `GET /api/v2/template/{id}` (`documenso_client.py:504`).
- `create_direct_link(documenso_template_id, *, direct_recipient_id=None) -> DirectLinkResult` — `POST /api/v2/template/direct/create {templateId, directRecipientId?}`; idempotent (falls back to `/template/direct/toggle {enabled:true}` if the link already exists) (`documenso_client.py:514`).
- `toggle_direct_link(documenso_template_id, *, enabled) -> DirectLinkResult` — `POST /api/v2/template/direct/toggle` (`documenso_client.py:542`).

The direct-link `token` is the single value used three ways: the API-response token, the `EmbedDirectTemplate` `token` prop, and the public `/d/{token}` URL / iframe `/embed/direct/{token}` (`documenso_client.py:459`-465).

### 11.5 The render+PUSH lane (engagement_templates)

A separate, Trigger-facing lane renders a brand-aware engagement-content source to a DocRaptor PDF and pushes it to Documenso as a TEMPLATE (this is how the templates the originate lanes consume get created):

- **Catalog** — `apps/edge_api/src/engagement_templates/catalog.py` resolves `content/<brand>/<path>/<archetype>/<version>/global_engagement_content/manifest.json` under the content root `apps/edge_api/content` (`catalog.py:20`), with a strict brand allowlist `_ALLOWED_BRANDS = {active-operators, rare-structure}` (`catalog.py:28`).
- **Push** — `push.render_and_push(brand, path, archetype, version, source_kind=REPO_HTML, style, title)` (`push.py:63`) resolves the source, renders via DocRaptor LIVE (`render.render_pdf`, `test=False` — `render.py:80`), and creates a Documenso TEMPLATE (`create_template_from_pdf`); only `repo-html` is wired (`db-markdown` raises `PushError`, `push.py:81`-84).
- **Internal route** — `POST /internal/engagement-templates/render-push` (`internal_engagement_templates_v1.py:84`), gated by `require_trigger_secret` (`TRIGGER_SHARED_SECRET`), mounted under `/internal` (`main.py:274`). Accepts either a `registryPath`/`registryId` (resolved against `business.global_input_content`) OR explicit `brand`/`path`/`archetype`/`version`.
- **Trigger task** — `engagement-template-push` (`src/trigger/engagement_template_push.ts:53`) calls the route via `callHqx`.
- **Ledger** — every attempt writes one terminal row to `ops.engagement_template_push_runs` (`apps/edge_api/sql/ops_engagement_template_push_runs.sql:12`): `run_id`, `brand`, `path`, `archetype`, `version`, `style`, `source_kind`, `status` (`success`/`error`), `documenso_template_id`, `documenso_numeric_id`, `pdf_r2_key`, `pdf_bytes`, `error`.

### 11.6 The content-source registry (`business.global_input_content`)

`business.global_input_content` is the CONTENT-SOURCE REGISTRY — one row per repo-resident (or DB-markdown) engagement-content source (`apps/edge_api/sql/global_input_content.sql:21`). It gained two columns (`global_input_content.sql:31`, `:32`):

- `brand` (`'active-operators' | 'rare-structure'`, default `'active-operators'`, indexed `:49`).
- `source_kind` (`'repo-html' | 'db-markdown'`, default `'repo-html'`, CHECK at `:44`) — `repo-html` resolves under `content/<brand>/<path>/global_engagement_content`; `db-markdown` resolves `business.global_engagement_content WHERE slug = path`.

Seeds (`global_input_content.sql:53`-55): the AO term-only source (`docraptor-to-documenso-template/term-only/v1`, brand `active-operators`) and the rare-structure capital-origination source (`docraptor-to-documenso-template/capital-origination/v1`, brand `rare-structure`).

### 11.7 Brand asset tree (rare-structure capital-origination)

`apps/edge_api/content/rare-structure/docraptor-to-documenso-template/capital-origination/v1/global_engagement_content/` holds the static-blank brand asset: `rare_structure_strategic_origination.html` (manifest slug `rare_structure_strategic_origination`, archetype `capital_origination`), `manifest.json`, and `styles/{plain,branded}.css`. The body is a static blank (field-slot blanks, no underscore-glyph fill lines) carrying an `§8.4 Authority` clause; signature/value fields are affixed in the Documenso editor after the TEMPLATE is created (render+push does no field placement — `documenso_client.create_template_from_pdf` docstring, `documenso_client.py:430`-433). The corresponding live Documenso template numeric id is a runtime fact in the Documenso workspace, not committed to this repo.

### 11.8 Frontend (cross-repo — `rare-structure-hq`, NOT verifiable from `core-x`)

The live SPA consumes the two originate lanes. From this repo only the contracts are verifiable; the SPA/BFF surface below is reported per the cross-repo handoff and should be confirmed in `rare-structure-hq` / `08-FRONTEND-AND-BFF.md`:

- `DirectTemplateSignPage` mounts `EmbedDirectTemplate` with the `direct_token` from `originate-embed-template`; the signer self-identifies (name/email NOT locked).
- Route `/p/t/:opportunityId/:directToken` with `?host=` threaded from `documenso_host`.
- `MandateDraftShell` carries a third dispatch branch (prefill / embed-document / embed-template); `SignLink` is a discriminated union; a shared `DirectToDocumensoLane` literal mirrors the edge_api CHECK domain.
- The BFF brokers `originate-embed-template` and threads the Documenso host via `?host=`.

> **Documenso direct-link facts (live v2 API).** `/api/v2/template/direct/create {templateId, directRecipientId?}` → `{token, ...}`; the token is the `EmbedDirectTemplate` prop AND the public `/d/{token}` / iframe `/embed/direct/{token}`. The signer enters their own name + email. `typedSignatureEnabled` / `drawSignatureEnabled` / `uploadSignatureEnabled` are document/template-level meta settings. Field types: `SIGNATURE, FREE_SIGNATURE, INITIALS, NAME, EMAIL, DATE, TEXT, NUMBER, RADIO, CHECKBOX, DROPDOWN` — there is NO dedicated `TITLE` type (a title field is `TEXT`). `prefillFields` supports `text/number/radio/checkbox/dropdown/date` (NOT `name`/`signature`).

---

## Status: ACTIVE / REMOVED / RETIRED

| Component | Status | Note |
|---|---|---|
| `POST /api/v1/proposals` (`create_proposal`) | **REMOVED** | Deleted in `b83e002`; live: `POST /api/v1/engagement-mandate-drafts` |
| `POST /api/v1/proposals/{ref}/confirm` (`confirm_proposal`) | **REMOVED** | Deleted in `b83e002`; live: `originate-prefilled` |
| `POST /api/v1/proposals/{ref}/provision` (`provision_proposal`) | **REMOVED** | Deleted in `b83e002`; no equivalent |
| `GET /api/v1/proposals` (`list_proposals`) | **REMOVED** | Deleted in `b83e002` |
| `GET /api/v1/proposals/{ref}` (`get_proposal`) | **REMOVED** | Deleted in `b83e002` |
| `GET /api/v1/proposals/{ref}/document` (`get_signed_document`) | **REMOVED** | Deleted in `b83e002` |
| `_provision` (through-docraptor + direct-to-documenso branches) | **REMOVED** | Lived in `proposals_v1.py`, deleted in `b83e002` |
| `docraptor_client.render_pdf` | ACTIVE | LIVE mode `test=False`; now driven by the render+push lane (`apps/edge_api/src/services/docraptor_client.py`) |
| `documenso_client.create_signing_envelope` | **REMOVED** | Deleted in `b83e002`; live: `create_direct_link` / `create_document_from_template` / `create_template_from_pdf` |
| `documenso_client.create_document_from_template` | ACTIVE | Prefill/embed-document lane (`apps/edge_api/src/services/documenso_client.py:228`) |
| `documenso_client.create_template_from_pdf` | ACTIVE | render+push terminal step, TEMPLATE create (`apps/edge_api/src/services/documenso_client.py:420`) |
| `documenso_client.create_direct_link` / `toggle_direct_link` / `get_template_recipients` | ACTIVE | embed-template lane (`apps/edge_api/src/services/documenso_client.py:514`, `:542`, `:504`) |
| `documenso_client.download_signed_pdf` | ACTIVE | sealed PDF fetch; survived `b83e002` (`apps/edge_api/src/services/documenso_client.py:711`) |
| `POST /api/v1/documenso/webhook` (`documenso_webhooks_v1`) | ACTIVE | Live raw capture, system of record (`apps/edge_api/src/routers/documenso_webhooks_v1.py:39`) |
| `POST /api/v1/proposals/webhook` (in-router) | **REMOVED** | Deleted in `b83e002` (not merely deprecated); live: `/api/v1/documenso/webhook` |
| `queries.advance_status` (`proposals/queries.py`) | **REMOVED** | Deleted in `b83e002` with the proposals webhook |
| `POST /api/v1/engagement-mandate-drafts/{draft_id}/originate-prefilled` | ACTIVE | DEFAULT direct lane (`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:113`) |
| `POST /api/v1/engagement-mandate-drafts/{draft_id}/originate-embed-template` | ACTIVE | embed-template lane (`apps/edge_api/src/routers/engagement_mandate_drafts_v1.py:172`) |
| `POST /internal/engagement-templates/render-push` | ACTIVE | render+PUSH lane, trigger-secret (`apps/edge_api/src/routers/internal_engagement_templates_v1.py:84`) |
| `business.engagement_proposals` | INACTIVE (DDL present) | Data preserved; no `.py` references (`apps/edge_api/sql/engagement_proposals.sql`) |
| `business.global_input_content` | ACTIVE | Content-source registry (`apps/edge_api/sql/global_input_content.sql:21`) |
| `ops.engagement_template_push_runs` | ACTIVE | render+push ledger (`apps/edge_api/sql/ops_engagement_template_push_runs.sql:12`) |
| `business.documenso_webhook_events` | ACTIVE | Raw Documenso capture, live signing-state SoR (`apps/edge_api/src/routers/documenso_webhooks_v1.py:7`) |
| `proposal_templates_v1` (authoring) | ACTIVE | Markdown storage + DocRaptor preview ONLY; no longer renders into signed proposals (`apps/edge_api/src/routers/proposal_templates_v1.py:47`) |

---

## Traps

- **THE WHOLE LANE IS REMOVED.** Do NOT treat any `proposals_v1` endpoint, `_provision`, `create_signing_envelope`, `render_agreement_html`, or `proposals/queries.py` function as live — all deleted in `b83e002`. The live engagement origination is `engagement-mandate-drafts` (§11). A prior internal dossier treated this lane as active; that is the exact mistake to avoid.
- **`POST /api/v1/proposals/webhook` is REMOVED, not deprecated.** Older docs called it "deprecated, code intact, repointed away." The route was DELETED with `proposals_v1.py`. The live webhook is `POST /api/v1/documenso/webhook` (`documenso_webhooks_v1.py:39`). Status truth is the raw-capture `business.documenso_webhook_events`.
- **`direct-to-documenso` is the LIVE flow, not a stub.** Earlier docs described a `render_mode == 'direct-to-documenso'` STUB branch inside `proposals_v1._provision`. That branch (and its router) no longer exists. The live direct flow is `engagement-mandate-drafts` with three sub-lanes selected by `operator_settings.direct_to_documenso_lane` (`prefill-document-from-template` default, `embed-template`, `envelope-distribute` retired — `apps/edge_api/sql/operator_settings.sql:85`).
- **`business.engagement_proposals` DDL survives but is unused.** The table is not dropped (data preserved), but no `.py` reads or writes it (`grep -rn 'engagement_proposals' apps/edge_api/src/ --include='*.py'` → nothing). Do not assume a row there reflects live state.
- **`business.proposals` ≠ `business.engagement_proposals`.** `business.proposals` belongs to the DMaaS data-transfer subsystem; the (now-unused) e-signature table is `business.engagement_proposals` (`apps/edge_api/sql/engagement_proposals.sql:4`-6).
- **The render+push HTML is a STATIC BLANK body — no anchors, no field placement in-code.** `create_template_from_pdf` does NOT place fields; signature/value fields are affixed in the Documenso editor afterward (`apps/edge_api/src/services/documenso_client.py:430`-433). The legacy `[[CLIENT_SIGNATURE]]`/`[[CLIENT_DATE]]` anchor mechanism belonged to the removed `agreement_template.py`; `signing_anchors.py` survives but is no longer wired into an envelope creator on this lane.
- **DocRaptor still runs LIVE (`test=False`).** Now via the render+push lane (`apps/edge_api/src/engagement_templates/render.py:80`). Every render bills against the live DocRaptor account.
- **Documenso has no `TITLE` field type.** Field types are `SIGNATURE, FREE_SIGNATURE, INITIALS, NAME, EMAIL, DATE, TEXT, NUMBER, RADIO, CHECKBOX, DROPDOWN`; a title is a `TEXT` field. `prefillFields` supports `text/number/radio/checkbox/dropdown/date` — NOT `name`/`signature`. Do not attempt to prefill a signer's name or signature.
- **The embed host must equal `DOCUMENSO_API_URL`.** Tokens are minted against `config.documenso_api_url()`; the SPA must embed against the SAME host. The originate responses now return `documenso_host` explicitly (`engagement_mandate_drafts_v1.py:164`, `:213`) and the SPA threads it via `?host=`. A mismatch silently breaks signing.
- **Cross-repo SPA/BFF claims are NOT verifiable from `core-x`.** §10 and §11.8 describe the `rare-structure-hq` SPA/BFF; confirm those against that repo / `08-FRONTEND-AND-BFF.md` before relying on them.
