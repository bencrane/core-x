# 06 — Engagement Docs Subsystem & Documenso Templates/Fields/Archetypes Layer

> **STATUS BANNER.** This file covers the **AO (Active Operators) engagement-document origination** machinery in `core-x` edge_api, across **two render lanes that must not be conflated**: (1) the `engagement_docs` subsystem — the older static-HTML → DocRaptor PDF → **DRAFT** two-signer Documenso DOCUMENT pathway, **WIRED end-to-end but BROKEN at HEAD by two critical-path bugs** and explicitly "left disconnected pending an explicit repoint" by commit #494; and (2) the **Documenso TEMPLATES / fields / defaults / archetypes / mappings** layer plus the successor `engagement_templates` DocRaptor-only render surface. It pertains to the AO term-only and term+success-fee economic shapes. It does **not** cover the `direct-to-documenso` prefill payment lane (see `05-DIRECT_TO_DOCUMENSO_PAYMENT_E2E`) nor the `engagement-mandate-drafts` staging-draft signing lane.

## Orientation

A fresh agent should hold two distinct mental models. **First**, `engagement_docs` (`apps/edge_api/src/engagement_docs/`) is a self-contained pathway: an operator "Stage" click on a `rare-structure-hq` Applications row → Trigger.dev task `engagement-doc-render` → edge_api binds an opportunity's company/signer values + a server-resolved price/term package into **repo-resident static AO term-only HTML** → DocRaptor renders a plain PDF (LIVE) → stored in a segregated R2 namespace → a **DRAFT** (never distributed) two-signer Documenso DOCUMENT is created with SIGNATURE/DATE fields placed by `[[anchor]]` `findText`. Its ledger is `business.ao_engagement_mandates` (one row = one deal/document; an opportunity may have many). **At HEAD this lane is non-functional**: the POST raises `AttributeError` on `service.SLUG` (undefined) before INSERT, and the render reads a content directory that was relocated out from under it in #494.

**Second**, the Documenso **templates layer** governs how edge_api reads/writes/classifies live Documenso v2 TEMPLATE envelopes and the per-operator content that feeds them: a Settings defaults editor that writes default field values onto a live template; a Dossier engagement picker scoped by operator email-domain; a standalone `engagement_templates` DocRaptor-to-PDF surface (NO Documenso); the `business.engagement_archetypes` economic-shape classifier above `business.documenso_templates`; and a one-shot push script that CREATES the two AO agreement templates in Documenso. These surfaces ARE active in the live flow.

All HTTP surfaces here are **service-token gated** and brokered by the `rare-structure-hq` platform-api BFF (a dumb forwarder across the auth boundary).

---

## Part A — The `engagement_docs` Subsystem (older lane)

### A.1 What it is, and that it is self-described as PARALLEL

The package docstring declares it a pathway **PARALLEL to the proposal/markdown machinery** with its own token substitution, DocRaptor call, R2 namespace, and ledger `business.ao_engagement_mandates`; it explicitly does **not** use the proposal markdown→HTML render, `agreement_template`, `docraptor_client`, or `r2_client` (`apps/edge_api/src/engagement_docs/__init__.py:1`, `:7`, `:8`, `:9`).

> **STALE COMMENT (proof of bug #2).** That same docstring at `apps/edge_api/src/engagement_docs/__init__.py:5` still names the content path `apps/edge_api/content/global_engagement_content/` — a directory that **no longer exists** (relocated by #494; see A.4).

### A.2 Operator surface — router `engagement_mandates_v1.py`

Router prefix `/api/v1/engagement-mandates`, gated by `dependencies=[Depends(require_service_token)]` (`apps/edge_api/src/routers/engagement_mandates_v1.py:26`, `:28`).

| Method / path | Behavior | Citation |
|---|---|---|
| `GET /packages` | Returns `packages.options()` (preset price+term dropdown). | `engagement_mandates_v1.py:32`, `:35` |
| `POST /{opportunity_id}` | Resolve package server-side → INSERT deal (`status='pending'`) → commit → enqueue Trigger.dev render → stamp run id. **BROKEN — see A.3.** | `engagement_mandates_v1.py:38`, `:44`, `:49`, `:54`, `:57`, `:59` |
| `GET /{opportunity_id}` | Returns the opportunity's LATEST deal (`get_latest_by_opportunity`, `ORDER BY created_at DESC LIMIT 1`); 404 if none. | `engagement_mandates_v1.py:73`, `:77`, `:79`; `queries.py:141`, `:146` |

POST control flow (`engagement_mandates_v1.py:44`-`62`):

```
pkg = packages.get(body.package_key)         # 400 if unknown (:46)
deal = queries.insert_mandate(..., slug=service.SLUG, ...)   # <-- AttributeError HERE (:52)
conn.commit()                                # deal persisted (:54)
run_id = kickoff.trigger_render(mandate_id)  # best-effort (:57)
if run_id is None: raise 502                 # "deal recorded" (:59)
queries.update_mandate(status="pending", trigger_run_id=run_id); commit  # (:61)
```

### A.3 CRITICAL BUG #1 — `service.SLUG` is undefined (POST is dead)

`POST /{opportunity_id}` references `service.SLUG` twice — `slug=service.SLUG, style=render.style_for(service.SLUG)` (`engagement_mandates_v1.py:52`). The `engagement_docs.service` module **defines no `SLUG` attribute**: its only module-level assignments are `logger` (`service.py:21`) and `_PROVIDER_NAME = "Benjamin J. Crane"` (`service.py:24`), plus module-level functions/imports. Independently confirmed: `grep -rn "SLUG" apps/edge_api/src/engagement_docs/` returns **nothing**, and the only `service.SLUG` usage in the entire tree is `engagement_mandates_v1.py:52`. The attribute access raises `AttributeError` **before** the deal row is INSERTed, so the entire POST path is non-functional at HEAD. (Intended value is almost certainly `'active_operators_term_only'` — the SQL default at `ao_engagement_mandates.sql:26` and the manifest key.)

### A.4 CRITICAL BUG #2 — `render.py` reads a vacated content directory

`render.py` computes `_CONTENT_DIR = parents[2] / "content" / "global_engagement_content"` → `apps/edge_api/content/global_engagement_content` (`render.py:19`), with `_MANIFEST = _CONTENT_DIR / "manifest.json"` (`:20`). That directory **does not exist at HEAD** (verified `ls`: "No such file or directory"). The content tree was renamed in **#494 (commit 44ef2fc)** into `apps/edge_api/content/active-operators/docraptor-to-documenso-document-only/global_engagement_content/` (verified: that path holds `active_operators_term_only.html`, `manifest.json`, `styles/`). `render.py` was never repointed. So `assemble_html()` reading `_MANIFEST.read_text()` (`render.py:32`, called via `:43`/`:46`) raises `FileNotFoundError`. **Even if `service.SLUG` were fixed, the render task would still fail.** The successor `engagement_templates` uses the correct relocated root (`catalog.py:17`, see B.7), proving the move was intentional and `engagement_docs/render.py` is the stale survivor.

### A.5 The render orchestrator — `service.render_mandate`

`render_mandate(conn, *, mandate_id)` is the orchestrator (`service.py:35`). It **never raises**; failures are recorded on the deal (`status='failed'`) via the internal `_fail` helper (`service.py:48`-`55`) and returned, so the caller commits before surfacing a non-2xx.

```
deal = queries.get_by_id(mandate_id)                 # (:38)  None -> failed
opp  = queries.read_opportunity_for_doc(opp_id)      # (:57)  None -> _fail
values = {participant_name, participant_signer_name,  # (:64-70)
          participant_title, term_fee, duration_in_months}
bound = render.substitute(render.assemble_html(slug), values)  # (:74)
pdf   = render.render_pdf(bound, ...)                 # (:75)  DocRaptor LIVE
key   = engagement-mandates/{opportunity_id}/{mandate_id}.pdf  # (:76)
store.put_pdf(key, pdf); url = presigned_get_url(key) # (:77-78)
if not participant_email: _fail(...)                 # (:84)  Participant must have email
doc = documenso.create_draft_document(... external_id=opportunity_id)  # (:93,:100)
queries.update_mandate(status="rendered", documenso_envelope_id=..., ...)  # (:107)
```

Merge values bound into the document (`service.py:64`-`69`):

| Token | Source | Citation |
|---|---|---|
| `participant_name` | account company name | `service.py:65` |
| `participant_signer_name` | contact first+last | `service.py:66` |
| `participant_title` | contact title | `service.py:67` |
| `term_fee` | `packages.format_usd(term_fee_cents)` | `service.py:68` |
| `duration_in_months` | `str(duration_months)` | `service.py:69` |

The Provider (Rare Structure signatory) is hardcoded `_PROVIDER_NAME = "Benjamin J. Crane"` (`service.py:24`); provider email defaults to `PROVIDER_SIGNER_EMAIL` env or `benjaminjcrane@gmail.com` (`service.py:27`-`28`). Document title: `f"Active Operators — Strategic Origination Agreement — {company or signer or 'Engagement'}"` (`service.py:91`) — note **em-dash** (—) separators, not ASCII hyphens.

### A.6 Render internals — `render.py`

- `substitute()` replaces `{{token}}` with **HTML-escaped** values; the regex `_TOKEN_RE = re.compile(r"\{\{\s*([a-zA-Z0-9_]+)\s*\}\}")` matches `{{token}}` only (`render.py:24`). **Unknown tokens stay LITERAL** (`return ... else m.group(0)`, `render.py:56`) so an unfilled field is visible, never silently blanked; `[[anchors]]` are a different grammar and untouched (`render.py:51`-`57`).
- `render_pdf()` POSTs to `https://docraptor.com/docs` (`render.py:22`) in **LIVE mode** `"test": False` (`render.py:66`), `document_type=pdf`, `prince_options {"media": "print", "javascript": False}` (`render.py:70`). Requires `DOCRAPTOR_API_KEY` (`RenderError` if unset, `render.py:62`-`64`); raises `RenderError` on non-2xx (`render.py:74`-`75`).
- `style_for()` returns `'plain'` if the manifest doc's `plain` flag is truthy (default `True`) else `'branded'` (`render.py:38`-`40`); `assemble_html()` injects the chosen CSS into the document's `__STYLESHEET__` slot (`render.py:43`-`48`).

### A.7 Storage — `store.py`

`put_pdf()` writes the PDF to R2 at key `engagement-mandates/{opportunity_id}/{mandate_id}.pdf` — a **per-deal key** built at `service.py:76` (prefix `MANDATE_PREFIX = "engagement-mandates/"`, `store.py:15`) so sibling deals never overwrite. Bucket is `R2_PROPOSAL_BUCKET` (default `'data-sink'`, `store.py:24`). Its own boto3 client reads `R2_ENDPOINT` / `R2_ACCESS_KEY_ID` / `R2_SECRET_ACCESS_KEY` (`store.py:31`-`33`). `presigned_get_url()` returns a short-lived (default 3600s) GET URL (`store.py:57`).

### A.8 The DRAFT Documenso client — `documenso.py` (standalone)

A standalone Documenso v2 client (own httpx client; auth header `Authorization: api_<key>` formed at `documenso.py:60`, applied at `:68`; base `DOCUMENSO_API_URL` default `https://app.documenso.com` at `:64`). `create_draft_document()` (`documenso.py:97`):

1. `POST /api/v2/envelope/create` (multipart `payload` JSON + PDF as `files`), `type="DOCUMENT"` (`:111`), `externalId=external_id` = **the OPPORTUNITY id** (`:113`), two SIGNER recipients provider+participant (`:115`-`116`), `distributeDocument: False` (`:118`). POST at `:124`; envelope id dug from `id` in the response (`:129`).
2. `GET /api/v2/envelope/{id}` to resolve recipient ids (`:134`).
3. `POST /api/v2/envelope/field/create-many` placing SIGNATURE+DATE per signer by anchor `placeholder` with `matchAll: True` (`:149`; field list `:142`-`147`).
4. **NO distribute** — comment at `:153`; document stays DRAFT, no signing tokens minted.

Anchor placeholders (module constants, the `[[...]]` value carried in the rendered PDF):

| Constant | Value | Citation |
|---|---|---|
| `PROVIDER_SIGNATURE_ANCHOR` | `[[PROVIDER_SIGNATURE]]` | `documenso.py:36` |
| `PROVIDER_DATE_ANCHOR` | `[[PROVIDER_DATE]]` | `documenso.py:37` |
| `PARTICIPANT_SIGNATURE_ANCHOR` | `[[PARTICIPANT_SIGNATURE]]` | `documenso.py:38` |
| `PARTICIPANT_DATE_ANCHOR` | `[[PARTICIPANT_DATE]]` | `documenso.py:39` |

Field box SIZE (percent of page; position comes from `findText`, not coordinates): SIGNATURE `{width: 30.0, height: 7.0}` (`documenso.py:42`), DATE `{width: 20.0, height: 4.0}` (`documenso.py:43`).

> The `externalId` is the **OPPORTUNITY** id (not the deal/mandate id), so many documents can hang off one opportunity (`documenso.py:14`, `:113`; `service.py:100`).

### A.9 Persistence — `queries.py` & the ledger

- `insert_mandate()` mints id `f"mand_{secrets.token_hex(12)}"` (`queries.py:23`) and INSERTs into `business.ao_engagement_mandates` (`queries.py:45`, `:61`). **Caller owns commit** (no commit inside).
- `update_mandate()` UPDATEs **by id**; artifact/Documenso fields are `COALESCE`'d (a later partial update never wipes them, `queries.py:105`) while `status` and `error` are set outright (`queries.py:104`, `:112`). Caller owns commit.
- `read_opportunity_for_doc()` reads `business.opportunities o JOIN business.accounts acc` (`acc.name AS company_name`) `LEFT JOIN business.contacts c` (first/last/title/email) keyed `WHERE o.id = %s::uuid` (`queries.py:27`, `:33`, `:35`, `:37`, `:38`). These tables are **upstream-owned**, referenced by value (no FK from this pathway).

### A.10 The ledger table — `business.ao_engagement_mandates`

The subsystem's own ledger, one row per deal (`apps/edge_api/sql/ao_engagement_mandates.sql:17`):

| Column | Type / constraint | Citation |
|---|---|---|
| `id` | `text PRIMARY KEY` (`mand_…`) | `:19` |
| `opportunity_id` | `uuid NOT NULL` (by value, **no FK**) | `:20` |
| `package_key`, `term_fee_cents`, `duration_months` | locked commercial terms | `:22`-`24` |
| `document_slug` | `text NOT NULL DEFAULT 'active_operators_term_only'` | `:26` |
| `style` | `text DEFAULT 'plain' CHECK IN ('plain','branded')` | `:27`-`28` |
| `status` | `text DEFAULT 'pending' CHECK IN ('pending','rendering','rendered','failed')` | `:30`-`31` |
| `pdf_r2_key` / `pdf_url` / `pdf_bytes` | rendered artifact pointers | `:33`-`35` |
| `documenso_envelope_id` | `text` (envelope_…) | `:39` |
| `documenso_document_id` | `integer` (numeric secondaryId) | `:40` |
| `participant_signing_token` / `provider_signing_token` | `text` — **stay NULL** (no distribute) | `:41`-`42` |
| `field_values` / `trigger_run_id` / `error` | provenance | `:44`-`46` |

**Data-model evolution (one→many):** the old partial-unique index `ao_engagement_mandates_opportunity_uidx` is `DROP`ped in place (`:55`) and replaced by a **non-unique** lookup index `ao_engagement_mandates_opportunity_idx` (`:56`), plus `status` and `created_at DESC` indexes (`:58`, `:59`). Confirmed many-docs-per-opportunity model.

> Code writes `'pending'`, `'rendered'`, `'failed'` — **never `'rendering'`** (it's in the CHECK enum but unused by any code path).

### A.11 Preset packages — `packages.py` (SERVER-OWNED)

The client sends only a key; money is resolved server-side. Pricing is explicitly **PLACEHOLDER** ("set the real numbers before any mandate is sent to a counterparty", `packages.py:7`).

| Key | term_fee | cents | months | Citation |
|---|---|---|---|---|
| `term_3mo` | $7,500 | 750,000 | 3 | `packages.py:25` |
| `term_6mo` | $13,500 | 1,350,000 | 6 | `packages.py:26` |
| `term_12mo` | $24,000 | 2,400,000 | 12 | `packages.py:27` |

`PACKAGES` dict at `packages.py:24`.

### A.12 The Trigger.dev render fan-out

- `kickoff.trigger_render()` enqueues task id `RENDER_TASK = "engagement-doc-render"` (`kickoff.py:12`) with payload `{"mandateId": mandate_id}` (`kickoff.py:22`) and `idempotency_key=f"engagement-doc-render:{mandate_id}"` (`kickoff.py:23`), via the generic `services.trigger_dev_client.trigger_task` (`kickoff.py:8`, `:20`). **Best-effort**: a failure is logged and returns `None`, never raises (`kickoff.py:26`-`27`).
- The task is defined at **monorepo-root** `src/trigger/engagement_doc_render.ts` (NOT under `apps/edge_api`): `task({ id: "engagement-doc-render", maxDuration: 180 })` (`src/trigger/engagement_doc_render.ts:33`, `:34`); it owns zero state and calls `callHqx("/internal/engagement-doc/render", { mandateId })` (`:41`).
- Registered for deploy: `trigger.config.ts` sets `project: 'proj_pakdcffjbeiwcixcoepb'` (`trigger.config.ts:5`) and `dirs: ['./src/trigger']` (`trigger.config.ts:22`).
- `callHqx` (`src/trigger/lib/hqx-client.ts:29`) authenticates with `requireEnv('EDGE_API_BASE_URL')` (`:34`) and `requireEnv('TRIGGER_SHARED_SECRET')` (`:35`), sending `Authorization: Bearer <secret>` (`:46`).

### A.13 The internal render route

`POST /internal/engagement-doc/render` — router prefix `/engagement-doc` (`internal_engagement_docs_v1.py:19`) mounted under `/internal` (`main.py:212`), gated by `require_trigger_secret` (`TRIGGER_SHARED_SECRET`) (`internal_engagement_docs_v1.py:22`). It calls `service.render_mandate` (`:28`), **commits the result** (persisting `'rendered'` OR `'failed'`) BEFORE surfacing any error (`:29`), and raises 502 when `status=='failed'` (`:31`).

### A.14 Cross-repo handoff (engagement_docs lane)

```
SPA stage:   Applications.tsx:269 generateMandate
  -> pipeline/api.ts:74 POST ${API_BASE}/api/v1/engagement-mandates/{opportunityId}
  -> BFF engagement-mandates-admin.ts:37 (requireUser) -> {data} wrap, EdgeError->502
  -> lib/edge.ts:317 edgeGenerateMandate (service token, body {packageKey} :323)
  -> edge_api POST /api/v1/engagement-mandates/{opportunity_id} (engagement_mandates_v1.py:38)
     [[ BROKEN at HEAD: service.SLUG AttributeError before INSERT ]]

SPA packages: Applications.tsx:62 listEngagementPackages
  -> pipeline/api.ts:53 GET .../packages -> BFF admin.ts:27 -> edge.ts:300 -> edge_api :32

SPA poll:    Applications.tsx:283 getMandate (setInterval 2000ms :288, max 15 tries :285)
  -> pipeline/api.ts:97 GET .../{opportunityId} -> BFF admin.ts:51 -> edge.ts:348
     (null on 404 :353) -> edge_api :73

Render fan-out: kickoff.trigger_render (kickoff.py:15) enqueues 'engagement-doc-render'
  -> src/trigger/engagement_doc_render.ts:41 callHqx (Bearer TRIGGER_SHARED_SECRET, hqx-client.ts:46)
  -> edge_api POST /internal/engagement-doc/render (internal_engagement_docs_v1.py:22)
  -> service.render_mandate -> DocRaptor + R2 + DRAFT Documenso DOCUMENT
     [[ BROKEN at HEAD: assemble_html reads vacated content/global_engagement_content -> FileNotFoundError ]]
```

Cross-repo paths: SPA `rare-structure-hq:apps/platform-app/src/routes/app/Applications.tsx:21`-`23` (imports), `:62`, `:264`, `:269`, `:283`, `:285`, `:288`; `rare-structure-hq:apps/platform-app/src/pipeline/api.ts:52`-`57`, `:74`, `:97`, `:102` (`{data}` unwrap); BFF `rare-structure-hq:apps/platform-api/src/index.ts:33`, `:127` (mount), `engagement-mandates-admin.ts:24`-`58`, `lib/edge.ts:300`, `:317`, `:323`, `:348`, `:353`.

### A.15 DRAFT-only by design — no send/distribute exists here

`create_draft_document` never distributes (`distributeDocument: False`, `documenso.py:118`; "NO distribute", `:153`; docstring `:11`), so `participant_signing_token` / `provider_signing_token` (`ao_engagement_mandates.sql:41`-`42`) **remain NULL** after a successful render. `queries.py:14` confirms "Tokens stay NULL until a later 'send' step." **No send/distribute route exists** anywhere in `engagement_docs` or its two routers.

> **CODE-WINS DISCREPANCY.** The SQL comment at `ao_engagement_mandates.sql:37`-`38` aspirationally says the document is distributed with method NONE "so the signing tokens exist WITHOUT Documenso emailing anyone." The **live code does NOT distribute at all** (`documenso.py:118`/`:153`), so tokens are **never minted**. The SQL comment is stale/aspirational — CODE wins.

### A.16 Reachability

The ONLY importers of the `engagement_docs` package inside edge_api are the two routers: `engagement_mandates_v1.py:19` (`from ..engagement_docs import kickoff, packages, queries, render, service`) + `:20` (`models.GenerateMandateRequest`), and `internal_engagement_docs_v1.py:15`-`16` (`service` + `models.RenderRequest`). Both registered in `main.py` (`:48`-`49` imports, `:209`/`:212` includes). No other importers — the subsystem is reachable **only** via those two routes. The package also contains `kickoff.py`, `models.py`, `packages.py`, `store.py` beyond `service`/`render`/`documenso`/`queries`.

---

## Part B — Documenso Templates / Fields / Defaults / Archetypes / Mappings (ACTIVE)

### B.1 Settings defaults editor — `documenso_template_fields_v1.py`

Two service-token-gated routes that read/write **default values directly onto a live Documenso template's fields**:

| Method / path | Behavior | Citation |
|---|---|---|
| `GET /api/v1/documenso-template-fields?documenso_template_id=…` | Returns the template's editable fields + current defaults. | `documenso_template_fields_v1.py:40`, `:41`, `:44` |
| `POST /api/v1/documenso-template-fields/defaults` | Writes defaults onto fields, returns refreshed list. | `documenso_template_fields_v1.py:50`, `:55`, `:56` |

Request body `SetDefaultsRequest {documenso_template_id: str, defaults: list[FieldDefault]}` where `FieldDefault {id: int, value: str}` (`documenso_template_fields_v1.py:30`-`37`). `DocumensoError` → HTTP **502** (detail prefixed `"documenso: "`) on both routes (`:45`-`46`, `:57`-`58`). Response model `TemplateField` carries `id:int`, `type:str` (**TEXT | NUMBER | DROPDOWN** — the default-able types), `label`, `recipient_id`, `page`, `default` (`:21`-`27`).

> Setting a default **modifies the actual Documenso TEMPLATE**: future documents instantiated via `/envelope/use` inherit it; already-created documents are untouched (router docstring `:6`-`7`; client docstring `documenso_client.py:716`, `:720`-`721`). *(This is a documented behavioral assertion in docstrings — not independently confirmed against Documenso's live API.)*

### B.2 The field-read/write client functions — `documenso_client.py`

- `get_template_fields()` (`documenso_client.py:689`): resolves the numeric template id → envelope id (`:696`), GETs `/api/v2/envelope/{envelope_id}` (`:697`), returns only fields whose type is in `_DEFAULT_META_KEY` i.e. **TEXT/NUMBER/DROPDOWN** (`:700`) — SIGNATURE/DATE excluded — each as `{id, type, label, recipient_id, page, default}` (`:702`-`711`).
- `_DEFAULT_META_KEY = {"TEXT": "text", "NUMBER": "value", "DROPDOWN": "defaultValue"}` (`documenso_client.py:677`) — the per-type fieldMeta key holding the default.
- `set_template_field_defaults()` (`documenso_client.py:715`): resolve envelope (`:727`) → read existing fields (`:728`) → **MERGE** each new value into the field's existing `fieldMeta` (so label/type survive: `meta = dict(f.get("fieldMeta") or {}); meta[key] = value`, `:738`-`739`) → POST the **FULL** field record (id, type, recipientId, page, positionX/Y, width, height, merged fieldMeta) to `/api/v2/envelope/field/update-many` (`:740`-`756`) so no property is dropped → returns `len(data)` (`:759`).

### B.3 The envelope-id resolver — `_resolve_template_envelope_id`

`business.documenso_templates` stores only the **numeric** template id; the v2 envelope endpoints 400 on a bare numeric id, so it must be resolved live: `GET /api/v2/template/{id}` (`documenso_client.py:311`).

> **CODE-WINS DISCREPANCY (carried from verification).** The docstring at `documenso_client.py:309` says the resolver returns `.envelopeId`, but the code at `:313` actually does `_dig(resp.json(), "envelopeId", "id")` — it digs `envelopeId` first, **falling back to `id`**. The primary key is `envelopeId`; the docstring's "→ .envelopeId" is imprecise.

Used by exactly four callers: `create_document_from_template_with_custom_pdf` (`:355`), `get_template_text_field_labels` (`:661`), `get_template_fields` (`:696`), `set_template_field_defaults` (`:727`).

### B.4 Dossier engagement picker — `engagement_mappings_v1.py` + `engagement_mappings/`

`GET /api/v1/engagement-mappings?org_domain=…` returns `list[EngagementMappingOption]` for one operator org; service-token gated (`engagement_mappings_v1.py:29`, `:30`).

The SQL (`engagement_mappings/queries.py:18`-`33`) lists rows from `business.engagement_documenso_template_mappings m` where `m.is_visible = true AND m.status = 'active' AND lower(o.metadata->>'domain') = lower(%s)`:

```sql
SELECT dt.documenso_template_id AS id,          -- the numeric Documenso template id (origination uses this)
       m.name                   AS label,
       a.key                    AS archetype_key,
       a.name                   AS archetype_name,
       a.performance_fee_basis  AS performance_fee_basis,
       COALESCE(dt.recipients->'text_fields', '[]'::jsonb) AS text_fields  -- stored FALLBACK
  FROM business.engagement_documenso_template_mappings m
  JOIN business.documenso_templates dt       ON dt.id = m.documenso_template_id   -- FK -> surrogate PK
  JOIN business.organizations o              ON o.id = m.organization_id
  LEFT JOIN business.engagement_archetypes a ON a.id = dt.archetype_id
 WHERE m.is_visible = true AND m.status = 'active'
   AND lower(o.metadata->>'domain') = lower(%s)
 ORDER BY a.name NULLS LAST, m.name
```

`queries.py:19`-`32`. An **empty `org_domain` short-circuits to `[]`** (`if not org_domain: return []`, `queries.py:38`).

> **DO-NOT-CONFLATE the two id columns.** The FK `m.documenso_template_id` joins to `dt.id` (the **surrogate PK**), but the returned option `id` is `dt.documenso_template_id` (the **numeric Documenso template id** — the value origination uses). `queries.py:19`, `:26`.

`EngagementMappingOption` fields: `id`, `label` (=`m.name`), `archetype_key`, `archetype_name`, `performance_fee_basis`, `text_fields` (`engagement_mappings/models.py:13`-`23`; `queries.py:20`-`24`, `:28`).

**LIVE text_fields override:** after the SQL list, the router overrides each option's `text_fields` with `documenso_client.get_template_text_field_labels(opt.id)` (`engagement_mappings_v1.py:37`), fanned out concurrently via `asyncio.gather` (`:41`). A per-option `DocumensoError` is **swallowed** (`except documenso_client.DocumensoError: pass`, `:38`-`39`), leaving the SQL stored fallback (`COALESCE(dt.recipients->'text_fields','[]')`) intact. `get_template_text_field_labels()` returns each TEXT field's `fieldMeta.label` (stripped, de-duplicated, in field order); SIGNATURE/DATE excluded (`documenso_client.py:652`, `:665`-`671`).

### B.5 The archetype classifier — `business.engagement_archetypes`

An archetype is the **economic SHAPE** of an engagement (which pricing variables exist and how they combine) — a TYPE, not a value (`engagement_archetypes.sql:5`-`7`). Hierarchy: `engagement_archetypes (1) ──< documenso_templates (N) ──< engagement_documenso_template_mappings` (`:11`); each `documenso_template` belongs to exactly one archetype (`:8`).

Table DDL (`engagement_archetypes.sql:19`), applied to `HQX_DB_URL_POOLED`, idempotent (`:3`):

| Column | Type / constraint | Citation |
|---|---|---|
| `id` | `uuid PRIMARY KEY DEFAULT gen_random_uuid()` | `:21` |
| `key` | `text NOT NULL UNIQUE` (stable code selector) | `:22` |
| `name` | `text NOT NULL` | `:23` |
| `description` | `text` | `:24` |
| `performance_fee_basis` | `text CHECK IN ('greater_of','lesser_of','sum','percentage_only','flat_only')` | `:27` |
| `created_at` / `updated_at` | `timestamptz NOT NULL DEFAULT now()` | `:29`-`30` |

Seeded with **exactly two** live archetypes via `INSERT ... ON CONFLICT (key) DO NOTHING` (`:34`-`44`):

| key | performance_fee_basis | meaning | Citation |
|---|---|---|---|
| `term_only` | `NULL` | fixed term/retainer, no per-deal performance fee | `:36`-`39` |
| `term_plus_greater_of` | `greater_of` | a term plus a per-deal fee = greater of a percentage or a flat amount | `:40`-`43` |

**ALTER-only on the upstream `documenso_templates`:** `ADD COLUMN IF NOT EXISTS archetype_id uuid` (`:49`); a guarded `DO $$` block adds the `ON DELETE RESTRICT` FK `documenso_templates_archetype_id_fkey` (Postgres lacks `ADD CONSTRAINT IF NOT EXISTS`) (`:50`-`62`); `documenso_templates_archetype_idx` on `(archetype_id)` (`:63`-`64`). **Data-driven backfill** (`:69`-`78`): a template whose `recipients->'text_fields'` overlaps `array['percentage_deal_fee','flat_deal_fee_amount','term_fee']` (the `?|` any-of operator, `:73`) is set to `term_plus_greater_of`, else `term_only`; idempotent (`WHERE dt.archetype_id IS NULL`, `:78`), no hardcoded ids.

### B.6 `business.documenso_templates` & mappings are UPSTREAM-OWNED

There is **no `CREATE TABLE`** for `business.documenso_templates` or `business.engagement_documenso_template_mappings` anywhere in the repo (independently re-verified: `grep -rniE 'create table[^;]*documenso_templates|create table[^;]*engagement_documenso'` returns nothing). edge_api only ALTERs `documenso_templates` (adds `archetype_id`) and SELECTs from both. The SQL itself states "the table predates this file and is not defined here" (`engagement_archetypes.sql:46`). Columns observed in use: `documenso_template_id` (numeric id as text — the originate value), `id` (surrogate PK), `organization_id`, `archetype_id`, `source_config_id` (referenced only in comment/docstring; its column type was not read from a CREATE TABLE), `recipients` (jsonb → `'text_fields'`). Also: `business.engagement_mandate_draft_content` denormalizes `archetype_id` from `documenso_templates` via the same ALTER+FK+index+backfill pattern, matching on the text numeric template id (`engagement_mandate_draft_content.sql:25`, `:35`-`36`, `:43`, `:48`-`51`).

### B.7 Standalone DocRaptor render surface — `engagement_templates_v1.py`

`GET /api/v1/engagement-templates` lists selectable templates; `POST /api/v1/engagement-templates/render` renders one to a PDF (plain style by default) and returns a **presigned R2 URL** with a 3600s TTL (`_PDF_TTL_SECONDS = 3600`, `engagement_templates_v1.py:27`). Both service-token gated (`:30`, `:46`). This surface **explicitly does NOT touch Documenso** (`:6`, `:48`-`49`).

The catalog (`engagement_templates/catalog.py`) discovers any `<path>/<archetype>/<version>/global_engagement_content/manifest.json` under `_AO_ROOT = parents[2] / "content" / "active-operators"` (the **correct relocated root**, `:17`). Selection segments are validated against `_SAFE_SEG = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")` (`:23`) AND the resolved dir is confirmed inside the content root (`if _AO_ROOT not in content_dir.parents: raise`, `:92`) — path-traversal defense-in-depth.

Render assembly does **NO token substitution** (the HTML body carries no `{{tokens}}` — every dynamic value is reserved blank space the operator affixes as Documenso fields later); `assemble_html` only injects the chosen stylesheet into the `__STYLESHEET__` slot (`engagement_templates/render.py:1`, `:4`-`5`, `:70`). DocRaptor renders LIVE `"test": False` (`render.py:79`-`80`).

Rendered PDFs are stored under the segregated `engagement_templates/store.py` PREFIX `"engagement-templates/"` (`store.py:18`, kept out of the data-lake SoR `active/` namespace `:17`); edge_api never proxies bytes (browser opens the presigned URL directly). Error mapping in the router:

| Error | HTTP | Citation |
|---|---|---|
| `CatalogError` (unknown template) | 404 | `engagement_templates_v1.py:52`-`53` |
| `StyleError` (bad style) | 400 | `:58`-`59` |
| `RenderError` (assemble) | 502 | `:60`-`61` |
| `RenderConfigError` (missing `DOCRAPTOR_API_KEY`) | 503 | `:66`-`67` |
| `RenderError` (render_pdf transient) | 502 | `:68`-`69` |
| `StoreConfigError` (R2 unconfigured) | 503 | `:75`-`76` |
| `StoreError` (put/sign failure) | 502 | `:77`-`78` |

### B.8 The only wired content lane + its manifest

Only **one** content lane is selectable by the `engagement_templates` catalog: `docraptor-to-documenso-template/term-only/v1/global_engagement_content` (it sits at the required `<path>/<archetype>/<version>/global_engagement_content/manifest.json` depth). *(Per the verified dossier, this is the only lane wired into the render catalog of the three lanes in the `content/active-operators` tree.)* The relocated **document-only** content (consumed by the broken `engagement_docs` lane) lives at `apps/edge_api/content/active-operators/docraptor-to-documenso-document-only/global_engagement_content/manifest.json` and declares slug `active_operators_term_only`, name "Active Operators — Strategic Origination Agreement (Term Only)", archetype `term_only`, document `active_operators_term_only.html`, stylesheets `{plain, branded}`, `plain: true`, signing_anchors the four `[[...]]` placeholders (`manifest.json:2`-`24`).

### B.9 One-shot template-push script — `scripts/documenso_push_templates.py`

A one-shot operator-run script (repo-root `scripts/`, not `apps/edge_api/scripts/`) that **CREATES the two AO agreement TEMPLATES** in Documenso via the v2 API, one per archetype (`scripts/documenso_push_templates.py:2`, `:38`). Run via `doppler run --project core-x --config prd -- python3 scripts/documenso_push_templates.py` (`:19`); CREATES REAL TEMPLATES (`:21`). Titles (`:39`-`42`):

| Archetype | Title |
|---|---|
| `term_only` | `AO Strategic Origination Agreement — Term Only` |
| `term_plus_greater_of` | `AO Strategic Origination Agreement — Term + Success Fee` |

Per archetype: `build_html` (reused from `render_ao_preview`, `:35`) → DocRaptor PDF (`test: False` live, `:60`) → `POST /api/v2/envelope/create` (`type=TEMPLATE`, recipients `[PARTICIPANT, PROVIDER]`, PDF as multipart `files`, `:112`-`116`) → read recipient ids via `GET /api/v2/envelope/{id}` (`:124`) → `POST /api/v2/envelope/field/create-many` placing every `[[ANCHOR]]` and `{{token}}` by placeholder via `findText` (`matchAll`, no coordinates, `:140`). Field routing (`:76`-`89`): placeholders containing `PARTICIPANT` or starting with `{{participant_` go to the Participant recipient, else Provider; `[[...]]` anchors become SIGNATURE (if `SIGNATURE` present) or DATE; `{{token}}` becomes a TEXT field with `fieldMeta {type:'text', label: titleized token, readOnly: True}` (every token is operator-prefilled at `/template/use`, never signer-typed). Recipients are PLACEHOLDERS overridden per-deal: PROVIDER = `Benjamin J. Crane` / `benjaminjcrane@gmail.com` (`:44`), PARTICIPANT = placeholder `participant@example.com` (`:45`). Field box SIZE (percent of page): SIGNATURE 30×7, DATE 20×4, TEXT 16×3.6 (`:49`-`51`).

### B.10 Distinct-lane callout

The BFF route `engagement-mandates-admin.ts` header explicitly distinguishes the `engagement_docs` lane from `engagement-mandate-drafts` ("Distinct from engagement-mandate-drafts (staging draft → Documenso)", `rare-structure-hq:apps/platform-api/src/routes/engagement-mandates-admin.ts:8`). Three sibling lanes coexist alongside `engagement_docs`: the `engagement_templates` render path (#494, correct `content/active-operators` paths, `catalog.py:1`/`:17`; registered `main.py:53`/`:236`), the `engagement-mandate-drafts` Documenso template-use lane (`main.py:51`/`:220`), and the `engagement-mappings` picker (`main.py:50`/`:216`).

---

## Status: ACTIVE / CONDITIONAL / DEPRECATED / STUB

| Component | Status | Note |
|---|---|---|
| `GET /api/v1/engagement-mandates/packages` | **ACTIVE** | Returns preset package options. |
| `POST /api/v1/engagement-mandates/{opportunity_id}` | **DEPRECATED (broken)** | `service.SLUG` `AttributeError` before INSERT — dead at HEAD. Wired but #494 "left disconnected pending an explicit repoint." |
| `GET /api/v1/engagement-mandates/{opportunity_id}` | **ACTIVE** | Reads latest deal; functional independent of the broken POST. |
| `POST /internal/engagement-doc/render` | **DEPRECATED (broken)** | `assemble_html` reads vacated `content/global_engagement_content` → `FileNotFoundError`. |
| `engagement-doc-render` Trigger.dev task | **ACTIVE (deployed) / fans into broken render** | Task is registered/reachable; the edge_api endpoint it calls fails. |
| `service.render_mandate` | **DEPRECATED (broken)** | Orchestrator; never raises but always lands `'failed'` at HEAD. |
| `service.SLUG` | **STUB / BUG** | Referenced, never defined. Intended `'active_operators_term_only'`. |
| `business.ao_engagement_mandates` (+ queries) | **ACTIVE (schema)** | Table/DDL/queries valid; write path is blocked upstream. |
| `packages.PACKAGES` | **ACTIVE** | Server-owned, PLACEHOLDER pricing. |
| `participant/provider_signing_token` columns | **STUB** | Never written — no send/distribute action exists. |
| `GET /api/v1/documenso-template-fields` + `POST .../defaults` | **ACTIVE** | Settings defaults editor; live template is source of truth. |
| `GET /api/v1/engagement-mappings` | **ACTIVE** | Dossier picker; org-domain scoped; live text_fields override. |
| `GET /api/v1/engagement-templates` + `POST .../render` | **ACTIVE** | Standalone DocRaptor→PDF; NO Documenso. |
| `business.engagement_archetypes` (+ ALTER/backfill) | **ACTIVE** | Classifier above `documenso_templates`; 2 seed rows. |
| `business.documenso_templates` / `..._mappings` | **ACTIVE (upstream-owned)** | No `CREATE TABLE` here; only ALTER+SELECT. |
| `scripts/documenso_push_templates.py` | **ACTIVE (one-shot, operator-run)** | Not an HTTP route; CREATES real templates. |
| `docraptor-to-documenso-template/term-only/v1` content lane | **ACTIVE (only wired catalog lane)** | Per dossier, the sole lane at the required manifest depth. |

---

## Traps

1. **`engagement_docs` ≠ `engagement_templates` ≠ `engagement-mandate-drafts`.** Three separate lanes with overlapping names. `engagement_docs` (this file, Part A) is the BROKEN static-HTML→DocRaptor→**DRAFT Documenso DOCUMENT** lane. `engagement_templates` (Part B.7) is a DocRaptor-only render that **never touches Documenso**. `engagement-mandate-drafts` is a separate Documenso **template-use** staging lane (out of scope here).
2. **Two HEAD bugs make the whole `engagement_docs` write path dead.** `service.SLUG` is undefined (POST dies before INSERT) and `render.py:19` reads the vacated `content/global_engagement_content` (render dies on `FileNotFoundError`). Do not assume the staging action works end-to-end. Prod may be running a pre-bug deploy — that was **not** verified; this doc reflects HEAD source.
3. **Stale docstrings name a directory that no longer exists.** `engagement_docs/__init__.py:5`, `render.py:5`, and `ao_engagement_mandates.sql:6` all reference `apps/edge_api/content/global_engagement_content/` — it was moved by #494. The live content is under `content/active-operators/docraptor-to-documenso-document-only/global_engagement_content/`.
4. **SQL comment lies about distribution.** `ao_engagement_mandates.sql:37`-`38` says distribution mints signing tokens "without emailing." The code never distributes (`documenso.py:118` `distributeDocument: False`). Tokens are never minted; the columns stay NULL. CODE wins.
5. **Two id columns on `documenso_templates`.** The mapping FK joins `dt.id` (surrogate PK); origination uses `dt.documenso_template_id` (numeric Documenso id, stored as text). The returned option `id` is the **numeric** one (`queries.py:19`). Mixing them silently breaks origination.
6. **The Documenso `externalId` is the OPPORTUNITY id, not the deal/mandate id.** Many documents legitimately share one `externalId`. Webhooks must resolve the exact document by envelope id, not by `externalId`.
7. **`_resolve_template_envelope_id` docstring is imprecise.** It says `→ .envelopeId` but the code digs `envelopeId` OR `id` (`documenso_client.py:313`). The numeric DB id 400s on envelope endpoints, so this live resolution step is mandatory.
8. **Pricing in `packages.py` is PLACEHOLDER.** `term_3mo`/`term_6mo`/`term_12mo` ($7.5k/$13.5k/$24k) are explicitly flagged to replace before any mandate is sent to a counterparty (`packages.py:7`).
9. **`'rendering'` status is declared but never written.** The enum allows it; code only writes `'pending'`/`'rendered'`/`'failed'`.
10. **`documenso_templates` and `engagement_documenso_template_mappings` are UPSTREAM-owned.** There is no `CREATE TABLE` for them in this repo. Do not add one; only ALTER/SELECT. Some columns (e.g. `source_config_id`) appear only in comments/docstrings and were not read from a CREATE TABLE.
11. **The Trigger.dev task lives at monorepo-root `src/trigger/`, not `apps/edge_api/src/trigger/`.** Looking for it under edge_api will fail.
12. **Em-dash vs hyphen in titles.** Document/template titles use `—` (U+2014), e.g. `Active Operators — Strategic Origination Agreement — {company}`. String-matching with ASCII `-` will miss.
