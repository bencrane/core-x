# Exa Webset Ingestion Engine — Architectural Specification

**Status: BLUEPRINT — pending approval. No production code until this contract is ratified (Directive 22 mandate).**
**Exa API surface verified against `exa.ai/docs` + `exa-labs/openapi-spec` as of 2026-06-02.**

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
| D1 | **Websets API (`POST /v0/websets`) is the primary discovery engine.** Not `/search`. | `/search` and `/findSimilar` hard-cap at **100 results, no pagination/cursor**. The directive's own example `max_results_limit: 500` is *physically impossible* on the raw endpoints. Websets is async, unbounded by count, and verifies each item against criteria. |
| D2 | `/findSimilar` + `/search` are the **complementary cheap-harvest path** (Tier B), used only for seed-URL look-alike expansion and sub-100 sweeps where verification is deferred. | Dollar-priced (~$0.007–0.013/result) vs Websets credit-priced (~$0.045/verified item). Cheap top-of-funnel; no native verification. |
| D3 | **Exa enrichments default to EMPTY (`enrichments: []`).** Downstream enrichment (Clay, firmographics_blitz, our own warehouse) is authoritative. | Enrichments are the dominant credit drain (+2/row, **+5/contact datapoint**). We already own firmographic + contact enrichment. Paying Exa for it is double-spend. Opt-in only, behind a separate sub-ceiling (§4). |
| D4 | **Trigger.dev owns the wait; Modal does short compute bursts.** The worker creates the webset, polls to `idle`, ingests, and fires one callback — bounded by Modal `maxDuration`. No inbound Exa webhook in v1. | Mirrors the existing dispatch→callback pattern 1:1 (§2). Cadence/waiting is a control-plane concern per [`03_modal_compute.md`](03_modal_compute.md) ("workers expose zero endpoints"). The clay-style push endpoint is the documented scale-out for monitors (§2.4), not the v1 path. |
| D5 | Warehouse dedup is **JIT against `s3://data-sink/active/companies/` on `normalized_domain`**, never against Exa-side state. New domains → `s3://data-sink/active/discovered_websets/`. | The directive. Our warehouse is the system of record; Exa's `exclude`/Imports is a *cost* lever (§4.3), not the dedup authority. |
| D6 | `source_platform = 'exa-websets'`. | Extends the existing GTM lineage convention (`exa-all`, `prospeo-parallel.ai`, `sfnet`). |

**Severable opinionated extension (approve or cut at the gate):** the `webset_membership` edge dataset (§6.2) —
records that an *already-known* company matches an industry webset. The directive only requires routing *new*
domains to `discovered_websets`; membership captures the GTM signal for the known overlap. Low cost, high
composition value, but strictly beyond the literal mandate.

---

## 1. Endpoint selection

### 1.1 The fork, decided

| Capability | `/search` + `/findSimilar` (raw) | **Websets `/v0/websets` (chosen, D1)** |
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

`POST https://api.exa.ai/v0/websets` — verified request contract:

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
- `GET /v0/websets/{id}` → poll `status` until `idle` (SDK: `exa.websets.wait_until_idle(id)`).
- `GET /v0/websets/{id}/items` → **cursor-paginated** full item pull (SDK: `exa.websets.items.list(webset_id, cursor=…)`).

**Item shape** (per-item, the payload we capture in full):

```jsonc
{
  "id": "witem_...", "websetId": "ws_abc123", "source": "search",
  "properties": { "type": "company", "url": "https://acme-law.com", "name": "Acme Defense LLP",
                  "description": "…", "entity": { /* company firmographics Exa resolved */ } },
  "evaluations": [ { "criterion": "…", "reasoning": "…", "satisfied": "match|unclear|no" } ],
  "enrichments": [ /* present only if enrichments requested */ ],
  "createdAt": "…"
}
```
> Exact `evaluations[]` key names (`satisfied` enum, `confidence`) are the one field family not pinned from
> static docs — confirm against a live `items.list` response during build and freeze here. Everything else is
> contract-verified.

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
        │  b. POST /v0/websets (externalId=run_id; idempotent re-create guard)
        │  c. poll GET /v0/websets/{id} → idle   (bounded by maxDuration; partial-persist on timeout)
        │  d. GET /v0/websets/{id}/items  (cursor) → capture FULL raw payload → R2 landing (ZSTD parquet)
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
- **Worker image:** the canonical data-engineering image ([`03_modal_compute.md`](03_modal_compute.md) §2) **+ `exa-py>=1.0`**.

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

Base URL `https://api.exa.ai`. Auth header `x-api-key: $EXA_API_KEY`.

| Endpoint | Method | Cap / note | Rate limit |
|---|---|---|---|
| `/v0/websets` | POST | async; `search.count` unbounded | control-plane, low volume |
| `/v0/websets/{id}` | GET | status poll | — |
| `/v0/websets/{id}/items` | GET | **cursor** pagination | — |
| `/v0/websets/webhooks` | POST | scale-out only (§2.4) | — |
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
clamped_count   = min(max_results_limit, HARD_RESULT_CAP)            # HARD_RESULT_CAP = 1000
contact_pts     = (#email enrich) + (#phone enrich)                  # 0 by default (D3)
text_enrich     = #non-contact enrichments                          # 0 by default (D3)
projected_cred  = clamped_count * (10 + 2*text_enrich + 5*contact_pts)
projected_usd   = projected_cred * 0.00449
REJECT (422) if  projected_cred > max_credits            (per-run ceiling, payload-overridable, default 25_000)
REJECT (422) if  projected_cred > ledger.month_remaining (ops.exa_credit_ledger)
REJECT (422) if  contact_pts > 0 AND not payload.allow_contact_enrichment  (separate explicit opt-in)
```
`dry_run:true` returns the estimate and exits **without creating the webset** — the Managed Agent's safe pre-check.

### 4.2 Monthly budget ledger — `ops.exa_credit_ledger`
Single source of truth for spend. The worker (1) reserves `projected_cred` at create, (2) reconciles to **actual** at completion (Websets actual = items_returned × per-item rate; raw actual = `Σ costDollars.total`). A Doppler flag `EXA_ENGINE_ENABLED=false` is a global kill switch checked before step (b). Monthly cap default = plan allotment (100,000 credits) — **operator sets the real number at approval (§11)**.

### 4.3 Warehouse-aware suppression (credit lever, not dedup authority)
Optional, when expected overlap with `companies` is high: pre-upload the active-domain set as an Exa **Import** and pass its id in `search.exclude` so already-known domains are not returned — **Exa charges per result returned, so suppressed domains cost zero**. Tier B equivalent: pass known domains in `excludeDomains` (≤1200). Toggle: `exclude_known_domains` (default `true`). This trims credits; it does **not** replace the JIT dedup in §5.

### 4.4 Enrichment & content minimalism (the dominant lever — D3)
`enrichments:[]` by default. Contact enrichment (5 credits/datapoint) is the single most expensive option and is **forbidden unless** `allow_contact_enrichment:true` *and* under its own sub-ceiling. Content options follow §1.4 (summary-schema first, never stack text+highlights+summary).

### 4.5 Rate governance & concurrency
- Token-bucket limiter: **10 QPS** for `/search`+`/findSimilar` (shared bucket), **100 QPS** for `/contents`. Websets control-plane calls are low-volume; poll `GET /v0/websets/{id}` on a **fixed backoff cadence** (e.g. 5s→15s→30s, cap 30s), never a tight loop.
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
| `company_name` | VARCHAR | `properties.name` | | |
| `company_url` | VARCHAR | `properties.url` | | original URL |
| `exa_item_id` | VARCHAR | `id` | **BTREE** | idempotency key on re-pull/resume |
| `exa_webset_id` | VARCHAR | `websetId` | **BTREE** | Exa-native collection id |
| `webset_label` | VARCHAR | `metadata.webset_label` | | **the directive's `webset_id: 'osha_defense_firms_2026'` origin flag** |
| `webset_identifier` | VARCHAR | payload slug | | `osha_defense_firms` |
| `search_prompt` | VARCHAR | payload | | the query that produced this cohort |
| `entity_type` | VARCHAR | `properties.type` | | `company` |
| `verification_status` | VARCHAR | derived from `evaluations[]` | | `verified` (all criteria match) \| `partial` \| `unverified` (Tier B) |
| `verification_json` | VARCHAR(JSON) | `evaluations[]` | | per-criterion reasoning + satisfied — full fidelity |
| `match_criteria_json` | VARCHAR(JSON) | `search.criteria` | | criteria the cohort was verified against |
| `description` | VARCHAR | `properties.description` / summary | | GTM blurb |
| `enrichment_json` | VARCHAR(JSON) | `enrichments[]` | | null unless enrichments opted-in (D3) |
| `linkedin_url` | VARCHAR | `properties.entity` if present | | best-effort |
| `source_platform` | VARCHAR | const | | `'exa-websets'` (D6) |
| `raw_payload_uri` | VARCHAR | R2 path | | pointer to §5.1 landing parquet |
| `exa_webset_run_id` | VARCHAR | our `run_id` | | joins `ops.exa_webset_runs` |
| `discovered_at` | VARCHAR | item `createdAt` / run ts | | ISO-8601 |
| `snapshot_date` | VARCHAR | run date | | partition/recency |

### 6.2 `s3://data-sink/active/webset_membership/` — known-company industry edges (severable, §0)

| Lance column | Type | Index | Notes |
|---|---|---|---|
| `normalized_domain` | VARCHAR | **BTREE** | FK to `companies.normalized_domain` |
| `exa_webset_id` | VARCHAR | **BTREE** | |
| `webset_label` | VARCHAR | | industry tag, e.g. `osha_defense_firms_2026` |
| `exa_item_id` | VARCHAR | | |
| `verification_status` | VARCHAR | | |
| `verification_json` | VARCHAR(JSON) | | |
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
    "tier":              { "enum": ["precision","harvest"], "default": "precision" }, // precision=Websets, harvest=Tier B
    "seed_urls":         { "type": "array", "items": { "type": "string", "format": "uri" }, "maxItems": 50, "default": [] }, // Tier B findSimilar
    "enrichments":       { "type": "array", "items": { "type": "object" }, "default": [] }, // D3: empty
    "allow_contact_enrichment": { "type": "boolean", "default": false },           // 5-credit gate (§4.1)
    "exclude_known_domains":    { "type": "boolean", "default": true },            // §4.3 cost lever
    "max_credits":       { "type": "integer", "minimum": 0, "default": 25000 },    // per-run ceiling
    "dry_run":           { "type": "boolean", "default": false }                   // estimate only, no spend
  }
}
```

**Server-side clamps (worker, non-negotiable):** `count = min(max_results_limit, 1000)`; `webset_label = "<webset_identifier>_<YYYY>"`; `externalId = "exa-webset-<run_id>"`; contact enrichment stripped unless `allow_contact_enrichment`. The Managed Agent cannot override the credit ledger or the kill switch.

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

- **Modal secret `exa-api`** → `EXA_API_KEY` (Doppler `hq-x/prd` → `modal secret create`). Worker: `secrets=[Secret.from_name("r2-credentials"), Secret.from_name("hqx-postgres"), Secret.from_name("exa-api")]`.
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

## 11. Decisions required at approval (operator input — cannot default safely)

1. **Exa plan tier & monthly credit cap** (`ops.exa_credit_ledger.month_cap`). Pro is $449/mo / 100,000 credits ≈ 10k verified items/mo at zero enrichment. Set the real ceiling and the per-run default (`max_credits`, drafted at 25,000).
2. **Contact enrichment policy** — keep globally forbidden (D3), or allow opt-in with a contact sub-ceiling? (5 credits/datapoint is the steepest cost.)
3. **`webset_membership` (§6.2)** — ship it (capture known-company industry signal) or cut to the literal directive (new-domains-only)?
4. **`HARD_RESULT_CAP`** — drafted at 1,000 (2× the directive's 500 example). Raise/lower?

---

## 12. Out of scope (explicit)

Promotion of `discovered_websets` → canonical `companies`; the §2.4 push-webhook; multi-webset monitor scheduling;
people-grain (`maxPeoplePerCompany`) ingestion. Each is a follow-on directive, not this engine.
