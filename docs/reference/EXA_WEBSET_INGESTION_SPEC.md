# Exa Webset Ingestion Engine — Architectural Specification

**Status: RATIFIED & BUILT (Directive 22 sign-off, 2026-06-02). §11 locked — per-run ceiling 5,000 credits · month_cap 100,000 · contact enrichment globally forbidden · HARD_RESULT_CAP 1,000 · `webset_membership` shipped. Implemented: [`pipelines/exa_websets/ingest.py`](../../pipelines/exa_websets/ingest.py) · [`src/trigger/exa_websets.ts`](../../src/trigger/exa_websets.ts) · [`pipelines/exa_websets/ops_exa_webset_runs.sql`](../../pipelines/exa_websets/ops_exa_webset_runs.sql).**
**Exa API surface verified against `exa.ai/docs` + `exa-labs/openapi-spec` as of 2026-06-02.**

**Live validation (2026-06-03):** Tier B (`/search` harvest) validated end-to-end — 5/5 candidates landed in `discovered_websets` ($0.007), JIT dedup + BTREE indexes confirmed. **Tier A (Websets API) is blocked pending a Pro plan** (`POST /websets/v0/websets` → `401` on this account). Credit-safety hardening from the first runs: Trigger `retry.maxAttempts=1` (no blind auto-retry on a credit-spending task) and the credit reservation is released on pre-create failure (`exa_webset_id is None` → nothing accrued). Correct Websets base path is `…/websets/v0`, not root `/v0`.

**Operating default (2026-06-03):** `tier` now defaults to **`harvest`** (Tier B / `/search`) — the working path on the current plan. Tier A (Websets precision) code is intact but **disabled** behind `EXA_TIER_A_ENABLED=false`; requesting `tier:"precision"` while disabled returns a clean `rejected`. Flip the flag (no code change) after a Pro upgrade.

This document is the canonical contract for harvesting high-precision, custom industry websets
(e.g. *OSHA Defense Law Firms*, *Maritime Logistics Providers*) from Exa.ai into Gen-3 Lance. It
slots into the existing planes: Trigger.dev v4 control ([`04_trigger_orchestration.md`](04_trigger_orchestration.md)),
Modal compute ([`03_modal_compute.md`](03_modal_compute.md)), DuckDB transform ([`01_duckdb_processing.md`](01_duckdb_processing.md)),
LanceDB system-of-record ([`02_lancedb_storage.md`](02_lancedb_storage.md)). It adds **no new plane and no new
architectural primitive** — one worker, one Trigger task, two Lance datasets, two `ops.*` tables.

---

## 0. Decisions baked in (rule, don't re-litigate at build time)

| # | Decision | Rationale |
|---|---|---|
| D1 | **Websets API (`POST /websets/v0/websets`) is the primary discovery engine.** Not `/search`. | `/search` and `/findSimilar` hard-cap at **100 results, no pagination/cursor**. The directive's own example `max_results_limit: 500` is *physically impossible* on the raw endpoints. Websets is async, unbounded by count, and verifies each item against criteria. |
| D2 | `/findSimilar` + `/search` are the **complementary cheap-harvest path** (Tier B), used only for seed-URL look-alike expansion and sub-100 sweeps where verification is deferred. | Dollar-priced (~$0.007–0.013/result) vs Websets credit-priced (~$0.045/verified item). Cheap top-of-funnel; no native verification. |
| D3 | **Exa enrichments default to EMPTY (`enrichments: []`).** Downstream enrichment (Clay, firmographics_blitz, our own warehouse) is authoritative. | Enrichments are the dominant credit drain (+2/row, **+5/contact datapoint**). We already own firmographic + contact enrichment. Paying Exa for it is double-spend. Opt-in only, behind a separate sub-ceiling (§4). |
| D4 | **Trigger.dev owns the wait; Modal does short compute bursts.** The worker creates the webset, polls to `idle`, ingests, and fires one callback — bounded by Modal `maxDuration`. No inbound Exa webhook in v1. | Mirrors the existing dispatch→callback pattern 1:1 (§2). Cadence/waiting is a control-plane concern per [`03_modal_compute.md`](03_modal_compute.md) ("workers expose zero endpoints"). The clay-style push endpoint is the documented scale-out for monitors (§2.4), not the v1 path. |
| D5 | Warehouse dedup is **JIT against `s3://data-sink/active/companies/` on `normalized_domain`**, never against Exa-side state. New domains → `s3://data-sink/active/discovered_websets/`. | The directive. Our warehouse is the system of record; Exa's `exclude`/Imports is a *cost* lever (§4.3), not the dedup authority. |
| D6 | `source_platform = 'exa-websets'`. | Extends the existing GTM lineage convention (`exa-all`, `prospeo-parallel.ai`, `sfnet`). |

**Ratified extension (D3 sign-off — SHIPPED):** the `webset_membership` edge dataset (§6.2) records that an
*already-known* company matches an industry webset, so a known company can be stamped with its specific niche
(e.g. "OSHA Defense Firm") without re-inserting into `companies`. New domains still route to `discovered_websets`.

---

## 1. Endpoint selection

### 1.1 The fork, decided

| Capability | `/search` + `/findSimilar` (raw) | **Websets `/websets/v0/websets` (chosen, D1)** |
|---|---|---|
| Max results / call | **100 hard cap, no cursor** | Unbounded (`search.count`, async) |
| Satisfies `max_results_limit: 500`? | **No** | **Yes** |
| Per-item criteria verification | None (relevance score only) | **Yes** — each item carries an evaluation against natural-language criteria |
| Execution model | Synchronous, ≤ a few sec | Async, seconds→minutes, event-driven |
| Entity typing | `category` hint + `entities[]` best-effort | First-class `entity.type` (`company`/`person`/…) |
| Origin tagging | none (client-side only) | native `externalId` + `metadata` |
| Warehouse-aware suppression | `excludeDomains` (≤1200) | `search.exclude` + Imports (arbitrary scale) |
| Pricing unit | **dollars**, tiered by result count | **credits** (10/result + enrichment) |
| Cost / 500 verified cos. | n/a (can't reach 500) | ~5,000 credits ≈ **$22.45** (Pro rate, no enrichment) |

### 1.2 Tier A — Precision Webset (default)

`POST https://api.exa.ai/websets/v0/websets` — verified request contract:

```jsonc
{
  "title": "OSHA Defense Law Firms 2026",          // optional, ≤5000 chars
  "search": {
    "query": "Top law firms specializing in OSHA defense and workplace safety compliance",
    "count": 500,                                    // == clamped max_results_limit
    "entity": { "type": "company" },                 // company|person|article|research_paper|custom
    "criteria": [                                     // optional; drives verification precision
      { "description": "Firm actively practices OSHA / workplace-safety defense litigation" },
      { "description": "U.S.-based law firm, not a consultancy or staffing agency" }
    ],
    "exclude": [ { "source": "import", "id": "<known-domains-import-id>" } ]  // §4.3, optional
  },
  "enrichments": [],                                  // D3 — empty by default
  "externalId": "exa-webset-<run_id>",               // idempotency key == our run_id
  "metadata": {                                       // string values ≤1000 chars
    "webset_label": "osha_defense_firms_2026",
    "webset_identifier": "osha_defense_firms",
    "callback_url": "<trigger_waitpoint_token_url>"  // recovery breadcrumb only
  }
}
```

Response `201` (fields we persist in **bold**):

```jsonc
{
  "id": "ws_abc123", "object": "webset",
  "status": "running",                               // idle|pending|running|paused
  "externalId": "exa-webset-<run_id>",
  "searches":   [ { "id": "...", "status": "running",  "criteria": [ { "description": "...", "successRate": 0 } ] } ],
  "enrichments":[ ],
  "dashboardUrl": "...", "createdAt": "...", "updatedAt": "..."
}
```

Then:
- `GET /websets/v0/websets/{id}` → poll `status` until `idle` (SDK: `exa.websets.wait_until_idle(id)`).
- `GET /websets/v0/websets/{id}/items` → **cursor-paginated** full item pull (SDK: `exa.websets.items.list(webset_id, cursor=…)`).

**Item shape** (per-item, the payload we capture in full):

```jsonc
{
  "id": "witem_...", "object": "webset_item", "websetId": "ws_abc123",
  "source": "search", "sourceId": "...",
  "properties": {
    "type": "company", "url": "https://acme-law.com", "description": "…", "content": "…",
    "company": { "name": "Acme Defense LLP", "location": "…", "employees": 42,
                 "industry": "…", "about": "…", "logoUrl": "…" }   // sub-object keyed BY ENTITY TYPE
  },
  "evaluations": [ { "criterion": "…", "reasoning": "…", "satisfied": "yes|no|unclear", "references": [] } ],
  "enrichments": [ { "enrichmentId": "…", "status": "completed", "format": "url",
                     "result": ["…"], "reasoning": "…", "references": [] } ],
  "createdAt": "…", "updatedAt": "…"
}
```
> Pinned (2026-06-03) against `exa-labs/openapi-spec · exa-websets-spec.yaml` (`WebsetItem`): the verdict field
> is **`satisfied` ∈ `"yes"|"no"|"unclear"`** (JSON strings), the entity sub-object is keyed by type
> (`company`/`person`/`researchPaper`/…), and the company name is at **`properties.company.name`** (NOT
> `properties.name`). List envelope is `{data, hasMore, nextCursor}`; poll terminal is webset `status=="idle"`.
> The Tier A worker parses this via `_websets_item_to_record`, fully decoupled from the Tier B `/search` shape
> (Directive 22-B).

### 1.3 Tier B — Seed expansion / bulk harvest (complementary, D2)

- **Seed look-alikes:** `POST /findSimilar` per seed URL — `{ url, numResults≤100, excludeSourceDomain:true, category:"company", excludeDomains:[…known…] }`. One call per seed; the cheap way to turn a known-target seed list into a candidate domain cloud.
- **Query sweep:** `POST /search` — `{ query, type:"auto", category:"company", numResults≤100, contents:{ summary:{ query, schema } } }` (content minimal, §4.4).
- Tier B output is **candidate domains only** → either ingested low-confidence (`verification_status='unverified'`) **or** promoted: fed back as a Webset `search.scope`/criteria for verification. Tier B never writes a `verified` row.

### 1.4 Contents payload strategy (Tier B + any direct `/contents` hydration)

GTM relevance per credit, in priority order:
1. `summary` with a **structured `schema`** (JSON-schema'd extraction of the GTM fields we want: industry, HQ, employee band, services) — one summary credit/page, returns typed JSON, no parsing.
2. `text` with `verbosity:"compact"` + tight `maxCharacters` **only if** the summary schema is insufficient.
3. **Never** request `text` + `highlights` + `summary` together — each is a separate per-page charge.
4. `maxAgeHours` set high (e.g. `720`) to prefer cache and avoid live-crawl latency/premium; `0` only when freshness is mandatory. `livecrawl` is deprecated — do not write it.

---

## 2. Engine topology

```
Anthropic Managed Agent
        │  trigger_task("exa-webset-ingest", { webset_identifier, search_prompt, max_results_limit, … })
        ▼
Trigger.dev v4  src/trigger/exa_websets.ts          ← control plane: schema-validates, owns retries + the wait
        │  1. wait.createToken({ timeout, tags })
        │  2. POST $MODAL_DISPATCHER_URL  { app_name:"exa-webset-pipelines",
        │        function_name:"ingest_exa_webset", kwargs:<validated payload>, trigger_callback_url:token.url }
        │  3. await wait.forToken(token.id)          ← suspends; no compute burned while Exa works
        ▼
core/modal_dispatcher.py  → spawn() fire-and-forget → 202
        ▼
Modal worker  pipelines/exa_websets/ingest.py  (exa-py + duckdb + lancedb + pyarrow + psycopg)
        │  a. budget pre-flight  (ops.exa_credit_ledger; reject if over ceiling — §4)
        │  b. POST /websets/v0/websets (externalId=run_id; idempotent re-create guard)
        │  c. poll GET /websets/v0/websets/{id} → idle   (bounded by maxDuration; partial-persist on timeout)
        │  d. GET /websets/v0/websets/{id}/items  (cursor) → capture FULL raw payload → R2 landing (ZSTD parquet)
        │  e. DuckDB: normalize domain, JIT LEFT JOIN vs active/companies → {new, known}  (§5)
        │  f. lance.write_dataset → active/discovered_websets (+ webset_membership)  (§6)
        │  g. create_scalar_index BTREE on resolution keys
        │  h. write ops.exa_webset_runs + debit ops.exa_credit_ledger
        │  i. POST trigger_callback_url  { status, requested, returned, new, known, credits_spent }
        ▼
Trigger resumes → returns summary to the Managed Agent
```

### 2.1 Reused, not rebuilt
- **Dispatcher:** [`core/modal_dispatcher.py`](../../core/modal_dispatcher.py) `DispatchRequest{app_name, function_name, kwargs, trigger_callback_url}`. Unchanged.
- **Trigger pattern:** `wait.createToken` → `fetch(MODAL_DISPATCHER_URL, {Modal-Key, Modal-Secret})` → `wait.forToken` — identical to [`src/trigger/gtm_companies_people.ts`](../../src/trigger/gtm_companies_people.ts).
- **Worker image:** the canonical data-engineering image ([`03_modal_compute.md`](03_modal_compute.md) §2). Exa is called over **raw `requests`** (already in the image) — not `exa-py` — so retries, rate-governance, and `costDollars` capture stay fully under our control.

### 2.2 New artifacts
| Artifact | Path |
|---|---|
| Modal worker | `pipelines/exa_websets/ingest.py` (app `exa-webset-pipelines`, fn `ingest_exa_webset`) |
| Trigger task | `src/trigger/exa_websets.ts` (id `exa-webset-ingest`) |
| Modal secret | `exa-api` → `EXA_API_KEY` (Doppler-sourced, §8) |
| Lance datasets | `s3://data-sink/active/discovered_websets/`, `…/webset_membership/` |
| ops tables | `ops.exa_webset_runs`, `ops.exa_credit_ledger` (hqx-postgres) |

### 2.3 Bounded-poll + partial-persist (the maxDuration guard)
The worker polls to `idle` within a hard wall-clock budget (`POLL_BUDGET_S`, default 3000 < Modal `maxDuration` 3600). On budget exhaustion it **persists whatever items exist**, marks `ops.exa_webset_runs.status='timeout_partial'`, stores `exa_webset_id`, and fires the callback. A follow-up `resume_ingest` dispatch re-pulls items for the same `exa_webset_id` (idempotent on `exa_item_id`) — no credits re-spent (read-only `items.list`).

### 2.4 Scale-out (documented, NOT v1)
For monitor-style or very large (>~2k item) websets that exceed the poll budget routinely, switch to the **clay-style push endpoint** ([`pipelines/gtm/clay_industries_endpoint.py`](../../pipelines/gtm/clay_industries_endpoint.py) precedent): a single `@modal.fastapi_endpoint(label="exa-webset")` consuming Exa's `webset.idle` webhook (HMAC-signature-verified, `requires_proxy_auth=False`), running phases (d)–(i). Events available: `webset.created`, `webset.search.created`, `webset.search.completed`, `webset.item.created`, `webset.item.enriched`, `webset.idle`. Deferred until v1 demonstrates a need.

---

## 3. Exa API contract reference (verified — builder need not re-research)

Base URLs: core endpoints at `https://api.exa.ai` (e.g. `/search`); the **Websets API is namespaced at `https://api.exa.ai/websets/v0`** — NOT root `/v0` (a root POST 404s). Auth header `x-api-key: $EXA_API_KEY`. **The Websets API requires a Pro plan** — lower tiers return `401` on `/websets/*` ("Upgrade to a Pro plan to get access"); `/search` + `/findSimilar` work on the free/standard tier.

| Endpoint | Method | Cap / note | Rate limit |
|---|---|---|---|
| `/websets/v0/websets` | POST | async; `search.count` unbounded | control-plane, low volume |
| `/websets/v0/websets/{id}` | GET | status poll | — |
| `/websets/v0/websets/{id}/items` | GET | **cursor** pagination | — |
| `/websets/v0/webhooks` | POST | scale-out only (§2.4) | — |
| `/search` | POST | `numResults` **1–100**, no cursor | **10 QPS** |
| `/findSimilar` | POST | `numResults` **1–100** | 10 QPS (shared) |
| `/contents` | POST | `urls` **≤100**/call | **100 QPS** |

**`/search` + `/findSimilar` enums (OpenAPI-pinned):**
- `type`: `neural` · `fast` · `auto`(default) · `deep` · `deep-reasoning` · `instant`
- `category`: `company` · `people` · `news` · `pdf` · `github` · `personal site` · `research paper` · `financial report`
- `contents`: `text{maxCharacters 1–10000, includeHtmlTags, verbosity:compact|standard|full, includeSections/excludeSections}`, `highlights{query, maxCharacters}`, `summary{query, schema}`, `extras{links, imageLinks}`, `maxAgeHours -1..720`, `subpages 0–100`.
- Response root: `requestId`, `results[]`, **`costDollars{ total, breakDown[], perRequestPrices{neuralSearch_1_25_results, neuralSearch_26_100_results, …}, perPagePrices{contentText, contentHighlight, contentSummary} }`** — read `costDollars.total` for exact post-hoc spend reconciliation (§4.5).
- `/contents` returns `statuses[]{ id, status:success|error, error{ tag, httpStatusCode } }`; error tags: `CRAWL_NOT_FOUND`, `CRAWL_TIMEOUT`, `CRAWL_LIVECRAWL_TIMEOUT`, `SOURCE_NOT_AVAILABLE`, `UNSUPPORTED_URL`, `CRAWL_UNKNOWN_ERROR`.

**Websets create** — `search{query*, count, entity{type}, criteria[{description}], maxPeoplePerCompany, recall, exclude[{source:import|webset, id}], scope[…]}`, `enrichments[{description*, format:text|date|number|options|email|phone|url, options, metadata}]`, `externalId`, `metadata`. Statuses: webset `idle|pending|running|paused`; search `created|pending|running|completed|canceled`; enrichment `pending|canceled|completed`.

---

## 4. Cost-control guardrails & rate governance ("credit-protection logic")

Exa bills two incompatible units. The middleware tracks **both** and converts to dollars for a single budget ceiling.

| Operation | Unit | Verified rate |
|---|---|---|
| Webset result (discovery + verification) | credit | **10 / result** |
| Webset enrichment (text/number/date/url/options) | credit | **+2 / row** |
| Webset contact datapoint (email / phone) | credit | **+5 / datapoint** |
| Credit → USD (Pro plan) | — | $449 / 100,000 = **$0.00449 / credit** |
| `/search` neural | USD | $7 / 1k req (≤10 res); tiered 1–25 / 26–100 (`perRequestPrices`) |
| `/contents` per page (text \| highlights \| summary, each) | USD | $1 / 1k pages |
| `/findSimilar` | USD | same as `/search` |
| Free tier | — | 1,000 requests / month |

### 4.1 Pre-flight ceiling gate (hard reject before any spend)
```
clamped_count   = min(max_results_limit, HARD_RESULT_CAP)            # HARD_RESULT_CAP = 1000 (D4)
text_enrich     = #non-contact enrichments                          # 0 by default; every email/phone stripped (D2)
projected_cred  = clamped_count * (10 + 2*text_enrich)              # contact credits never accrue (D2)
projected_usd   = projected_cred * 0.00449
effective_cap   = min(max_credits, PER_RUN_CREDIT_CEILING)          # PER_RUN_CREDIT_CEILING = 5000 (D1, hard)
REJECT if  projected_cred > effective_cap            (per-run ceiling — payload may lower it, never raise past 5000)
REJECT if  projected_cred > ledger.month_remaining   (ops.exa_credit_ledger; month_cap = 100_000, D1)
```
**Binding interaction:** at the ratified 5,000 ceiling with zero enrichment, the credit gate binds at **500 results**
(5000 ÷ 10) — a 1,000-result request is *rejected* until `PER_RUN_CREDIT_CEILING` is raised in code. This is the
intended runaway protection for initial testing. `dry_run:true` returns the estimate and exits **without creating
the webset or reserving credits** — the Managed Agent's safe pre-check.

### 4.2 Monthly budget ledger — `ops.exa_credit_ledger`
Single source of truth for spend. The worker (1) reserves `projected_cred` at create, (2) reconciles to **actual** at completion (Websets actual = items_returned × per-item rate; raw actual = `Σ costDollars.total`). A Doppler flag `EXA_ENGINE_ENABLED=false` is a global kill switch checked before step (b). Monthly cap = **100,000 credits** (D1, ratified); the hard per-run ceiling is **5,000 credits**.

### 4.3 Warehouse-aware suppression (credit lever, not dedup authority)
Optional, when expected overlap with `companies` is high: pre-upload the active-domain set as an Exa **Import** and pass its id in `search.exclude` so already-known domains are not returned — **Exa charges per result returned, so suppressed domains cost zero**. Tier B equivalent: pass known domains in `excludeDomains` (≤1200). Toggle: `exclude_known_domains` (default `true`). This trims credits; it does **not** replace the JIT dedup in §5.

### 4.4 Enrichment & content minimalism (the dominant lever — D3)
`enrichments:[]` by default. Contact enrichment (5 credits/datapoint) is **forbidden unconditionally** (D2 ratified): the worker strips every `email`/`phone` enrichment format before create — contacts come from Blitz-API, never Exa. Content options follow §1.4 (summary-schema first, never stack text+highlights+summary).

### 4.5 Rate governance & concurrency
- Token-bucket limiter: **10 QPS** for `/search`+`/findSimilar` (shared bucket), **100 QPS** for `/contents`. Websets control-plane calls are low-volume; poll `GET /websets/v0/websets/{id}` on a **fixed backoff cadence** (e.g. 5s→15s→30s, cap 30s), never a tight loop.
- **429** body `{"error":"rate limit exceeded"}` → exponential backoff w/ full jitter, honor `Retry-After` if present, max 5 retries then surface a partial-run failure to the callback.
- Modal concurrency: one webset = one worker invocation. Batch fan-out (many websets) is governed by **Trigger.dev queue concurrency**, not by spawning unbounded Modal functions. Higher Exa limits via `sales@exa.ai` (enterprise) — out of scope for v1.

---

## 5. Delta processing & warehouse ingestion

1. **Capture raw (full fidelity, transport layer).** Every `items.list` page is written verbatim as ZSTD parquet to
   `s3://data-sink/landing/exa_websets/exa_webset_id=<id>/run_id=<run>/part-*.parquet`. This is the immutable
   payload the directive demands; Lance is derived from it and re-derivable.
2. **Normalize domain.** Extract the registrable domain from `properties.url` using the *same* `normalized_domain`
   derivation the companies pipeline uses ([`pipelines/gtm/companies_people_bulk.py`](../../pipelines/gtm/companies_people_bulk.py)). The join key must be byte-identical to the anchor, or dedup silently fails.
3. **JIT dedup (DuckDB, in-worker).** Read the active companies Lance and LEFT JOIN candidates on the anchor:
   ```sql
   -- candidates := exa items (one row per normalized_domain, DISTINCT)
   SELECT c.normalized_domain,
          (k.normalized_domain IS NOT NULL) AS is_known
   FROM candidates c
   LEFT JOIN lance_scan('s3://data-sink/active/companies/') k   -- read-only attach
          ON c.normalized_domain = k.normalized_domain;
   ```
   Partition → `new` (`is_known=false`) and `known` (`is_known=true`). DuckDB does 100% of projection / cast /
   DISTINCT per [`01_duckdb_processing.md`](01_duckdb_processing.md); `_enforce_schema` guards the contract before write.
4. **Route `new` → `discovered_websets`** (§6.1), tagged with full webset origin + verification.
5. **Route `known` → `webset_membership`** (§6.2, severable) — the industry-match signal, no re-insert into `companies`.
6. **Never write to `companies`.** Promotion of a verified `discovered_websets` row into the canonical
   `companies` spine is a *separate, deliberate* downstream step — out of scope for this engine.

---

## 6. Lance data-structure mapping

Storage: LanceDB on R2, `data_storage_version` matching the companies pipeline constant (currently **`"2.1"`**),
`mode="overwrite"` on full re-materialization / `"append"` on incremental, `max_rows_per_file=1_048_576`,
`storage_options=` the standard R2 block ([`02_lancedb_storage.md`](02_lancedb_storage.md)). All columns `VARCHAR`
unless noted — mirrors the `companies` string-typed convention. Indexes via `ds.create_scalar_index(col, index_type="BTREE")`.

### 6.1 `s3://data-sink/active/discovered_websets/` — NEW companies (the directive's staging target)

| Lance column | Type | Source (Exa item) | Index | Notes |
|---|---|---|---|---|
| `discovered_domain` | VARCHAR | `properties.url` → normalized | **BTREE** | resolution anchor; byte-identical to `companies.normalized_domain` |
| `company_name` | VARCHAR | Tier A `properties.<type>.name` · Tier B result title | | e.g. `properties.company.name` |
| `company_url` | VARCHAR | `properties.url` | | original URL |
| `exa_item_id` | VARCHAR | `id` | **BTREE** | idempotency key on re-pull/resume |
| `exa_webset_id` | VARCHAR | `websetId` | **BTREE** | Exa-native collection id |
| `webset_label` | VARCHAR | `<identifier>_<YYYY>` | | **the directive's `webset_id: 'osha_defense_firms_2026'` origin flag** |
| `webset_identifier` | VARCHAR | payload slug | | `osha_defense_firms` |
| `search_prompt` | VARCHAR | payload | | the query that produced this cohort |
| `entity_type` | VARCHAR | `properties.type` | | `company` |
| `verification_status` | VARCHAR | derived from `evaluations[].satisfied` | | `verified` (all `="yes"`) \| `partial` (some) \| `unverified` (none / Tier B) |
| `verification_json` | VARCHAR(JSON) | `evaluations[]` | | per-criterion reasoning + satisfied — full fidelity |
| `match_criteria_json` | VARCHAR(JSON) | `search.criteria` | | criteria the cohort was verified against |
| `description` | VARCHAR | `properties.description` / summary | | GTM blurb |
| `enrichment_json` | VARCHAR(JSON) | `enrichments[]` | | null unless enrichments opted-in (D3) |
| `linkedin_url` | VARCHAR | person `properties.url` when it is a LinkedIn URL | | company items carry none |
| `source_platform` | VARCHAR | const | | `'exa-websets'` (D6) |
| `raw_payload_uri` | VARCHAR | R2 path | | pointer to the landing parquet (§5.1) |
| `raw_item_json` | VARCHAR(JSON) | full item | | complete Exa item dumped verbatim — guarantees full fidelity in the SoR even if the landing write is skipped |
| `exa_webset_run_id` | VARCHAR | our `run_id` | | joins `ops.exa_webset_runs` |
| `discovered_at` | VARCHAR | item `createdAt` / run ts | | ISO-8601 |
| `snapshot_date` | VARCHAR | run date | | partition/recency |

### 6.2 `s3://data-sink/active/webset_membership/` — known-company industry edges (D3 — shipped)

| Lance column | Type | Index | Notes |
|---|---|---|---|
| `normalized_domain` | VARCHAR | **BTREE** | FK to `companies.normalized_domain` (NOT NULL) |
| `exa_webset_id` | VARCHAR | **BTREE** | |
| `webset_label` | VARCHAR | | industry tag, e.g. `osha_defense_firms_2026` |
| `exa_item_id` | VARCHAR | | idempotency key (NOT NULL) |
| `verification_status` | VARCHAR | | |
| `verification_json` | VARCHAR(JSON) | | |
| `raw_item_json` | VARCHAR(JSON) | | full item verbatim |
| `exa_webset_run_id` | VARCHAR | | |
| `discovered_at` | VARCHAR | | |
| `source_platform` | VARCHAR | | `'exa-websets'` |

---

## 7. Trigger.dev schema contract (Managed-Agent-facing)

The Managed Agent invokes `exa-webset-ingest`. The directive's minimal example is valid as-is; everything else has a safe default.

```jsonc
// VALID minimal call (directive example) — everything else defaulted:
{ "webset_identifier": "osha_defense_firms",
  "search_prompt": "Top law firms specializing in OSHA defense and workplace safety compliance",
  "max_results_limit": 500 }
```

Formal JSON Schema (validated in `src/trigger/exa_websets.ts` before dispatch — bad input never reaches Modal or Exa):

```jsonc
{
  "type": "object",
  "required": ["webset_identifier", "search_prompt"],
  "additionalProperties": false,
  "properties": {
    "webset_identifier": { "type": "string", "pattern": "^[a-z0-9_]{3,64}$" },   // slug → externalId + label base
    "search_prompt":     { "type": "string", "minLength": 8, "maxLength": 5000 },  // → search.query
    "max_results_limit": { "type": "integer", "minimum": 1, "maximum": 1000, "default": 100 }, // → search.count (clamped HARD_RESULT_CAP)
    "entity_type":       { "enum": ["company","person"], "default": "company" },   // → search.entity.type
    "criteria":          { "type": "array", "items": { "type": "string", "maxLength": 1000 }, "maxItems": 10, "default": [] },
    "tier":              { "enum": ["precision","harvest"], "default": "harvest" }, // DEFAULT harvest (Tier B); precision (Tier A/Websets) is Pro-gated, disabled via EXA_TIER_A_ENABLED
    "seed_urls":         { "type": "array", "items": { "type": "string", "format": "uri" }, "maxItems": 50, "default": [] }, // Tier B findSimilar
    "enrichments":       { "type": "array", "items": { "type": "object" }, "default": [] }, // empty default; email/phone formats stripped (D2)
    "exclude_known_domains":    { "type": "boolean", "default": true },            // §4.3 cost lever
    "max_credits":       { "type": "integer", "minimum": 0, "default": 5000 },     // per-run; clamped to the 5000 ceiling worker-side (D1)
    "dry_run":           { "type": "boolean", "default": false }                   // estimate only, no spend
  }
}
```

**Server-side clamps (worker, non-negotiable):** `count = min(max_results_limit, 1000)` (D4); `max_credits` clamped to the **5,000** per-run ceiling (D1 — payload may lower, never raise); `webset_label = "<webset_identifier>_<YYYY>"`; `externalId = "exa-webset-<run_id>"`; **every `email`/`phone` enrichment stripped unconditionally** (D2). The Managed Agent cannot override the credit ledger, the per-run ceiling, or the kill switch.

**Callback payload (worker → Trigger waitpoint):**
```jsonc
{ "status": "success|timeout_partial|rejected|failed",
  "exa_webset_id": "ws_abc123", "run_id": "...",
  "requested": 500, "returned": 487, "new": 412, "known": 75,
  "credits_estimated": 5000, "credits_actual": 4870, "usd_actual": 21.87,
  "discovered_websets_uri": "s3://data-sink/active/discovered_websets/",
  "rejected_reason": null }
```

---

## 8. Secrets, config, modules

- **Modal secret `exa-api`** → `EXA_API_KEY` + two flags: `EXA_ENGINE_ENABLED` (global kill switch, default `true`) and `EXA_TIER_A_ENABLED` (Websets precision gate, default `false`). Sourced from Doppler `core-x/prd` → `modal secret create`. Worker: `secrets=[Secret.from_name("r2-credentials"), Secret.from_name("hqx-postgres"), Secret.from_name("exa-api")]`. **To enable Tier A after a Pro upgrade:** `modal secret create exa-api --force EXA_API_KEY=… EXA_ENGINE_ENABLED=true EXA_TIER_A_ENABLED=true` (no code change).
- **No new Trigger env vars.** The Exa key lives Modal-side only; Trigger keeps `MODAL_DISPATCHER_URL`/`MODAL_KEY`/`MODAL_SECRET` (existing `syncEnvVars`). The key never transits the control plane.
- **Worker image** = canonical data image **+ `exa-py>=1.0`**.
- **New files:** `pipelines/exa_websets/ingest.py`, `src/trigger/exa_websets.ts`.

---

## 9. `ops.*` state (hqx-postgres, `HQX_DB_URL_POOLED`)

```sql
CREATE TABLE IF NOT EXISTS ops.exa_webset_runs (
  run_id            text PRIMARY KEY,
  exa_webset_id     text,
  webset_identifier text NOT NULL,
  webset_label      text NOT NULL,
  search_prompt     text NOT NULL,
  tier              text NOT NULL DEFAULT 'precision',
  status            text NOT NULL,             -- pending|running|success|timeout_partial|rejected|failed
  requested         integer, returned integer, new_count integer, known_count integer,
  credits_estimated integer, credits_actual integer, usd_actual numeric(12,4),
  rejected_reason   text,
  started_at        timestamptz DEFAULT now(), finished_at timestamptz
);

CREATE TABLE IF NOT EXISTS ops.exa_credit_ledger (
  month            date NOT NULL,              -- first-of-month bucket
  credits_reserved bigint NOT NULL DEFAULT 0,
  credits_actual   bigint NOT NULL DEFAULT 0,
  month_cap        bigint NOT NULL,            -- operator-set ceiling (§11)
  PRIMARY KEY (month)
);
```
`month_remaining = month_cap - max(credits_reserved, credits_actual)`. Reserve at create, reconcile at finish.

---

## 10. Failure modes & idempotency

| Mode | Handling |
|---|---|
| Re-invocation (same run) | `externalId = run_id`; check-then-create — never double-create a webset |
| Item re-pull / resume | dedup on `exa_item_id` (BTREE) — read-only `items.list`, zero credits re-spent |
| Webset slower than poll budget | partial-persist + `status='timeout_partial'` + resume dispatch (§2.3) |
| 429 / rate limit | jittered exponential backoff, honor `Retry-After`, max 5 retries → partial-failure callback |
| Budget breach | reject at pre-flight (`422`, `status='rejected'`), zero spend |
| Contents crawl error | per-URL `statuses[].error.tag` recorded; row kept w/o content, not dropped |
| Kill switch | `EXA_ENGINE_ENABLED=false` → reject before any Exa call |

---

## 11. Ratified decisions (Directive 22 sign-off, 2026-06-02)

| # | Ruling | Implemented as |
|---|---|---|
| D1 | Cost governance: `month_cap = 100,000`; **per-run ceiling restricted to 5,000 credits** to protect against runaway queries during initial testing. | `MONTH_CREDIT_CAP = 100_000`, `PER_RUN_CREDIT_CEILING = 5_000` (hard; payload `max_credits` clamped down only). Binds at 500 results/run. |
| D2 | Contact enrichment **forbidden globally** — contacts come from Blitz-API; Exa is strictly domain/company discovery. | Worker strips every `email`/`phone` enrichment format before create; no opt-in flag exists. |
| D3 | Build the `webset_membership` edge so known companies can be stamped with their niche. | `s3://data-sink/active/webset_membership/` (§6.2), BTREE on `normalized_domain` + `exa_webset_id`. |
| D4 | Hard cap **1,000 results** max per webset run. | `HARD_RESULT_CAP = 1_000` (the credit ceiling binds first at 500). |

**Operational constraint (operator-owned):** no automated/first test run. The task is manual-invoke only (no cron); the operator triggers the first webset to observe exact credit consumption.

---

## 12. Out of scope (explicit)

Promotion of `discovered_websets` → canonical `companies`; the §2.4 push-webhook; multi-webset monitor scheduling;
people-grain (`maxPeoplePerCompany`) ingestion. Each is a follow-on directive, not this engine.
