# Directive 24 — Parallel.ai Enrichment Capability (Build Blueprint)

**Status:** v4 — three workflows BUILT (code complete + statically verified; not yet deployed) · **Repo:** `core-x` · **Date:** 2026-06-04
**Spawned by:** `~/Desktop/hq/reports/2026-06-02-directive-24-parallel-ai-ecosystem-diagnostic.md`
**Adversarial review folded in:** `docs/reference/DIRECTIVE_24_PARALLEL_ENRICHMENT_ASSESSMENT.md` (verdict: ship-with-changes; all findings verified against live code/R2/trigger/Parallel and accepted).
**Repo correction:** the diagnostic places the work in `hq-x`/`data-engine-x`; the actual rails (gtm-mcp, corex, `src/trigger`, `core/modal_dispatcher.py`, the R2→DuckDB→Lance factory) all live in **`core-x`**. Build here.

### Revision log (v4 — three separate workflows, BUILT)
- **One tool with a `mode` flag is REJECTED.** The capability is now **three separate
  `trigger.dev → Modal → Parallel` workflows**, each with its own purpose-named gtm-mcp launch
  tool, its own Trigger task, and its own Modal worker entrypoint + Modal app. No shared `mode`
  dial. The §5 "two pipelines" and §6 single-`launch_parallel_task_enrich` framings are
  superseded by §§5–6 below.
  | Workflow | gtm-mcp tool | Trigger task | Modal app / worker | Parallel API | output_schema | Landing |
  |---|---|---|---|---|---|---|
  | **Enrichment** | `enrich_companies` | `parallel-enrich` | `parallel-enrich` / `pipelines/parallel/enrich.py` | Task API + **Task Groups** | `{"type":"json","json_schema":…}` | `s3://data-sink/active/enrichment/<spec>/` keyed `company_id` (BTREE), `merge_insert` |
  | **Deep Research** | `deep_research` | `parallel-deep-research` | `parallel-deep-research` / `pipelines/parallel/deep_research.py` | Task API, blocking `/result` | `{"type":"text"}` / `{"type":"auto"}` | `s3://data-sink/active/parallel_research/` — **grain** `per_entity`→key `company_id`, `topic`→key `run_id` |
  | **Web Search** | `web_search` | `parallel-search` | `parallel-search` / `pipelines/parallel/search.py` | **Search API** `POST /v1/search` (sync) | n/a | inline excerpts; optional `persist=true` → `s3://data-sink/active/parallel_search/` |
- **Shared HTTP client factored to `core/parallel_client.py`** (auth `x-api-key`, base
  `https://api.parallel.ai`, retry/backoff on 429/5xx, the create→result/group + search helpers,
  the `output_schema` envelopes). Shipped to every worker container via
  `Image.add_local_python_source("core.parallel_client")` — the proven `core.name_norm` idiom. The
  three Trigger tasks and three Modal entrypoints stay SEPARATE; only the client is shared.
- **Distinct Modal app names** (`parallel-enrich` / `parallel-deep-research` / `parallel-search`)
  so the three workers deploy independently and each resolves under its own `app_name` in the
  Universal Dispatcher (one shared app name would let the last `modal deploy` clobber the others'
  function registry).
- **Tier ceilings enforced both at the launch tool and worker:** enrichment caps at `core`;
  deep research supports up to `pro` and **REJECTS `ultra`** with a clear message that ultra needs
  the per-run webhook path (deferred per §0/§9). Web search has no tier (sync Search API).
- **`refresh_catalog()` + `catalog.invalidate()` built** — registry refresh **and** the
  `catalog.py` `_lance_cache`/`_schema_cache` clear (§4.2), both registered on gtm-mcp.
- **Ledgers built:** `ops.parallel_specs` (spec registry, all three workflows), `ops.parallel_runs`
  (one row per run; `workflow` column discriminates), `ops.parallel_review` (enrichment confidence
  gate). Each worker self-applies the DDL before its terminal write.
- **Cost governance per launch:** idempotencyKey `f"{spec_id}:{audience_id}:{run_kind}"`, a hard
  `max_runs` clamp enforced in the Trigger task BEFORE dispatch, `test_limit` (default 3 inline).
- **Status:** code complete + statically verified (`tsc --noEmit` clean on the 3 tasks;
  `py_compile` clean on all workers + tools). Not yet deployed; the `parallel-api` Modal secret
  must be minted from Doppler `core-x/prd` before `modal deploy`.

### Revision log (v2 — from adversarial review)
- **Topology corrected (v3) — driven by Parallel's own contract, not another vendor's pipeline.** Earlier drafts reasoned from Exa's poll-to-idle behavior; **Exa is a different provider** and its async model has no bearing on Parallel. Parallel's wait is fully specified in §0: server-side blocking `GET /result?timeout` (single) or group `is_active==false` poll / SSE (batch). The trigger→dispatcher→Modal→Lance→callback *plumbing* is the vendor-agnostic fleet shape (Exa/icypeas/shovels all ride it); none of their wait semantics transfer. base/core settle in seconds-to-minutes → bounded Modal burst; only `ultra` (hours) needs Parallel's per-run webhook (§0) → token (deferred; v1 caps at `core`).
- **Authoritative Task API contract added (§0)** — first-hand from the live OpenAPI, replacing secondhand summaries. Forces: `output_schema` wrapped `{type:"json",json_schema:{…}}`; batch via `default_task_spec` (≤1,000 runs/POST); terminal = group `is_active==false`; `confidence` nullable/processor-dependent (conservative land-and-flag is correct).
- **`companies` has no `company_website`** (live manifest: `company_id, company_name, normalized_domain, company_linkedin_url, source_platform`). Input passes `normalized_domain` **bare** as `company_website` — no scheme (verified live: the enrichment doc's example uses `www.un.org`; input is free-form, input_schema types the field plain `string`).
- **`refresh_catalog()` must bust two more caches** (`catalog._lance_cache`, `catalog._schema_cache`) — registry refresh alone is insufficient.
- **`PARALLEL_API_KEY` already exists in Doppler `core-x/prd`** — provisioning step removed; only the Modal secret `parallel-api` is missing.
- Added: idempotency keys, hard cost ceiling, per-company partial-failure handling, array/nested→Lance mapping, ultra timeout-chain, per-run (not group) webhook scope. Nits: `ultra` ≈20 fields; Clay attribution softened.

---

## 0. Parallel Task API contract (authoritative — live OpenAPI `components.schemas`, fetched 2026-06-04)

Auth: header `x-api-key`. Base `https://api.parallel.ai`.

**Create** — `POST /v1/tasks/runs`, body `TaskRunInput` → **202** `TaskRun {run_id, interaction_id, status:"queued", is_active}`.
- `input` (sent every run) — string **or** JSON object (e.g. `{"company_name":"Stripe","website":"stripe.com"}` — arbitrary field names, bare domain, no scheme).
- `processor` (sent every run) — **free string**, not an enum (value = tier name).
- `task_spec` (optional; omit → auto). `TaskSpec.output_schema` (required within task_spec) is one of:
  - `{"type":"json","json_schema":{…JSON-Schema subset…}}`  ← enrichment
  - `{"type":"text","description":"…"}` or a bare string  ← long-form report
  - `{"type":"auto"}`
  A bare JSON-Schema object is **NOT** valid — it must be wrapped under `{"type":"json","json_schema":…}`.
- optional: `metadata` (str/int/num/bool; key≤16 / val≤512), `source_policy` (`include_domains`/`exclude_domains` ≤200 combined, `after_date` YYYY-MM-DD), `advanced_settings` (e.g. `location`), `webhook`, `enable_events` (default true for pro+), `previous_interaction_id`, `mcp_servers`.

**Result** — `GET /v1/tasks/runs/{run_id}/result?timeout=600` — "blocks until the run is completed." `timeout` (int, default 600s) = how long the **server holds the connection**. **200** → `TaskRunResult {run, output}`; **408** → still active, re-call; **404** → failed/not found. Waiting is a blocking GET, not a busy loop; base/core usually returns 200 on the first call.
- `output` (oneOf, discriminated by `type`): `TaskRunJsonOutput {type:"json", content:{…your schema…}, basis:[FieldBasis], mcp_tool_calls?}` (required: basis, type, content) | `TaskRunTextOutput`.

**Basis** — `FieldBasis {field, reasoning` (both required)`, citations:[Citation{url(req), title?, excerpts?}], confidence}`. `confidence` is **nullable**, `low|medium|high`, "Only certain processors provide confidence levels." `excerpts` likewise processor-dependent. Per-list-element basis requires header `parallel-beta: field-basis-2025-11-25`.

**Batch (Task Groups)** — `POST /v1/tasks/groups {metadata?}` → `task_group_id`. `POST /v1/tasks/groups/{id}/runs {default_task_spec?, inputs:[TaskRunInput]}` — **≤1,000 runs/POST**; `default_task_spec` sets one output_schema for the batch (per-run overrides). Completion = `GET /v1/tasks/groups/{id}` until **`is_active==false`**, then `status.task_run_status_counts.{completed,failed}`. Result stream: `GET /v1/tasks/groups/{id}/runs?include_output=true` (SSE, `last_event_id` resumable).

**Webhook** — `TaskRunInput.webhook {url, event_types:["task_run.status"]}` — **per-run only** (each input in a group may carry one; no group-level webhook). Delivers run **status**, not the result → then GET `/result`.

**Status** — `queued · action_required · running · completed · failed · cancelling · cancelled`. Active = {queued, running, cancelling}; terminal = {completed, failed, cancelled}.

---

## 1. Objective

Make the GTM machine a **producer of its own intelligence**, not just a consumer of what's already in the lake. The gtm-agent selects the exact entities it cares about and commissions Parallel to research them into typed, cited facts that land in Lance as a **durable, compounding, queryable attribute layer** — so segmentation and messaging run on knowledge the system generated on purpose.

The deliverable is **not** a one-off enrichment. It is a **reusable scaffold**: a parameterized `trigger.dev → Modal → Parallel` pipeline the gtm-agent drives repeatedly with different entity sets and enrichment specs.

**Frame (locked):** enrichment is an **entity-level asset** (Frame A). It lands in Lance keyed by the entity's resolution key and compounds — enrich an entity once, every future audience joins it for free. The campaign-scoped `corex.lead.research` bundle (Frame B) becomes a *projection* of that layer, never the source.

**Scope of v1:** the `companies` Lance dataset — live: **758 rows, `company_id` PK-grade (758 distinct), `normalized_domain` BTREE present (`normalized_domain_idx`), 0.7% null**. **Not** SAM/UEI entities yet — same rails pointed at a UEI-keyed cohort, deferred.

---

## 2. The interaction loop

```
1. Operator ↔ gtm-agent  →  select a set from `companies`
      ("insurance providers", "won govt contracts", source = X …)  → a resolved audience
2. Operator: "enrich these via Parallel."
      gtm-agent gathers the SPEC conversationally — fields, mode (Task|Search), processor —
      persists it, then produces the INPUT trigger.dev needs (audience ref + spec).
3. Test gate:  "send 3 through"  →  small run, rows returned INLINE for inspection.
4. Full release  →  results land LINKED to each company record, in Lance, queryable.
5. Operator ↔ gtm-agent  →  build a segmented audience by filtering enriched fields.
6. Segmentation drives the MESSAGE.
```

The gtm-agent is the **input producer + trigger**, never the executor. It may use Parallel's MCP servers to *explore* while shaping a spec, but production runs go REST-behind-trigger.dev (MCP cannot carry a custom `output_schema`, Basis, or Task Groups).

---

## 3. Architecture (mapped to core-x, topology corrected)

Three separate workflows share one shape (gtm-mcp tool → Trigger task → Universal Dispatcher
→ Modal worker → flat callback) and one shared HTTP client (`core/parallel_client.py`).

```
gtm-agent ──calls──▶ gtm-mcp tools                apps/gtm_mcp/src/tools/parallel.py   (NEW; pattern = hydration.py)
   define_enrichment_spec(name, processor, output_schema) → ops.parallel_specs
   enrich_companies(spec_id, audience_id, test_limit=3, max_runs?, max_usd?)   (idempotencyKey + hard cap)
   deep_research(objective, grain, audience_id?, processor=base, ...)
   web_search(objective?, search_queries?, mode=advanced, persist=false)
   refresh_catalog()                              ← get_registry(refresh=True) + catalog.invalidate()
        │ POST https://api.trigger.dev/api/v1/tasks/{parallel-enrich|parallel-deep-research|parallel-search}/trigger
        ▼                                            (Bearer TRIGGER_SECRET_KEY)
trigger.dev tasks                                 src/trigger/parallel_{enrich,deep_research,search}.ts (NEW; pattern = exa_websets.ts)
   validate + clamp (HARD pre-dispatch cost cap) ; wait.createToken({timeout, idempotencyKey})
   POST core/modal_dispatcher.py {app_name, function_name, kwargs, trigger_callback_url=token.url}
   await wait.forToken(token)                      ← zero-compute suspend (the proven repo idiom)
        ▼
Modal workers (distinct apps)                     pipelines/parallel/{enrich,deep_research,search}.py (NEW; shared client via add_local_python_source)

 ① parallel-enrich / enrich_companies   ── Task API + Task Groups
     resolve audience → companies (corex.audience.source_sql; result_key='company_id')
     input/company {company_name, company_website = normalized_domain}  (bare host, NO scheme); null-domain → name-only, counted
     POST /v1/tasks/groups + /runs (default_task_spec = ONE {type:json,json_schema}; ≤1000/POST)
     wait §0: poll GET /v1/tasks/groups/{id} until is_active==false → task_run_status_counts → GET /runs?include_output=true
     DuckDB project content+basis → typed cols + <field>__confidence + _basis → merge_insert("company_id")
     s3://data-sink/active/enrichment/<spec>/ · BTREE company_id (+ normalized_domain)
     CONFIDENCE GATE: high → trust ; null/low/medium → ops.parallel_review (value still lands)
     smoke path: POST /v1/tasks/runs + blocking GET /result?timeout=600 (smoke_one)

 ② parallel-deep-research / deep_research ── Task API, blocking GET /result?timeout=600 looped (408-aware)
     grain=topic → one report keyed run_id ; grain=per_entity → one report per company_id
     output_schema {type:text}|{type:auto} ; REJECTS ultra (deferred per-run-webhook path)
     s3://data-sink/active/parallel_research/  {report_md, basis, objective, processor, ...}

 ③ parallel-search / web_search          ── Search API POST /v1/search (SYNCHRONOUS; no run lifecycle, no basis)
     {objective, search_queries ≤5, mode:advanced, source_policy?} → ranked excerpts INLINE in the callback
     optional persist=true → s3://data-sink/active/parallel_search/ (keyed run_id)

  all three: ops.parallel_runs ledger (workflow column) ; flat-JSON callback → waitpoint token
        ▼
   gtm-mcp refresh_catalog()  (registry + catalog caches)  →  new dataset queryable by execute_audience_query
   (optional) project enrichment/research to corex.lead.research via attach_research at enroll time
```

**Topology — driven by Parallel's contract (§0), not by another vendor's pipeline.** How Parallel waits is specified by Parallel, full stop — Exa and icypeas are *different providers* and their async models do not transfer. Single entity = a server-side blocking `GET /result?timeout=600` (the server holds the connection; base/core return on the first call — not a client poll loop). Batch = create group → add runs → consume `GET /groups/{id}/runs?include_output=true` (SSE) or poll `GET /groups/{id}` until **`is_active==false`**. The trigger→dispatcher→Modal→Lance→callback *plumbing* is the vendor-agnostic fleet shape, and a Modal worker holding a bounded wait on an async vendor is already operational across the fleet — so the plumbing is proven; only the wait *mechanism* is Parallel-specific. base/core settle in seconds-to-minutes → bounded Modal burst. The **ultra** family (up to ~2h) is the only case that pushes past a comfortable hold; it needs Parallel's **per-run** webhook (§0) → trigger token, with the chain **waitpoint `timeout` > Modal fn `timeout` > wait duration** holding. Therefore:

- **v1 caps at `core`** (≤5min) — Modal-internal poll, proven, simplest.
- **`ultra` family requires per-run waitpoint tokens** fed to Parallel's **per-run** webhook (groups have **no** group-level webhook — `task_api/webhooks`: single `task_run.status` event, per run). This is *required* for ultra, not a v2 nicety. Scope it as one token per run + an HMAC-verifying ingress when ultra is actually needed.

---

## 4. Data model & landing

### 4.1 Per-spec Lance datasets (system of record)

One dataset **per spec**, namespaced: `s3://data-sink/active/enrichment/<spec_name>/` (e.g. `enrichment/equipment_profile_v1`).

- **Auto-discovered** — `database.discover_datasets()` walks `active/` to `_MAX_DEPTH=2`, registering any prefix whose children include `_versions` (`database.py:226-257`). Verified live: `active/enrichment/` does not exist yet; the walk will register `active/enrichment/<spec>/` as name `enrichment/<spec>`, **double-quoted** in SQL: `FROM "enrichment/equipment_profile_v1"` (passes `assert_read_only`; resolves in `referenced_datasets`).
- **Keyed `company_id`** (BTREE); `normalized_domain` carried + BTREE for human joins.
- **Columns = the spec's `output_schema` fields**, plus `<field>__confidence` (from Basis), `_basis` (full citations+reasoning), `spec_id`, `run_id`, `processor`, `enriched_at`.
- **Type mapping (must be explicit in the worker):** scalar → typed column; `array<T>` → Lance `LIST<T>`; nested object / `_basis` → `STRUCT` or JSON (precedent: `ca_cslb` `LIST<STRUCT>` projection). `<field>__confidence` is matched from `basis[i].confidence` by `field` name; for **per-array-element** confidence the worker must send header `parallel-beta: field-basis-2025-11-25` (else basis is one entry for the whole array).
- **Drift tolerance:** if a processor returns fewer/renamed fields than the schema, the DuckDB projection **null-fills missing keys** — never throws mid-batch.
- **Lifecycle = per spec, not per run.** First run `mode="create"`; later runs `merge_insert("company_id")` (refresh existing, add new). **Partial failure:** land completed rows, persist failed `company_id`s (`ops.parallel_runs.failed` + optional per-company ledger); a re-run resolves only missing/failed keys (audience SQL ⋈ existing spec dataset, anti-join).
- **Schema change ⇒ new spec version** (`equipment_profile_v2` → new dataset). The decisive reason per-spec beats one wide evolving table.

### 4.2 Catalog-refresh (MUST build — registry alone is insufficient)

`database.get_registry()` is a lazy singleton (`database.py:297-306`); nothing calls refresh today. **Verified gap:** `get_registry(refresh=True)` surfaces a new dataset on the *clean* path, **but** does NOT clear `catalog.py`'s module caches (`_lance_cache`, `_schema_cache`, `catalog.py:30-31`). Consequences: a `describe_dataset` call *before* the dataset lands memoizes `None` permanently (survives refresh → restart-only); `list_datasets` shows the spec with **no `column_count`** (manifest cache is the stale `active/catalog.json` — live: `dataset_count:103`, generated `2026-06-01`).

**Fix:** `refresh_catalog()` calls `get_registry(refresh=True)` **and** a new `catalog.invalidate()` that clears `_lance_cache` + `_schema_cache`. Keep an opportunistic refresh-on-miss in `execute_audience_query` as the safety net.

### 4.3 Substrate Split (honored)

- **Lance (R2):** enriched values + `_basis` + confidence — analytical/entity data.
- **Postgres `ops.*` (hq-x):** spec registry, run ledger, review *queue* — operational state only. No Postgres MV of analytical data.

```sql
-- pipelines/parallel/ops_parallel_specs.sql  (one registry, all three workflows)
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.parallel_specs (
    spec_id       text PRIMARY KEY,              -- equipment_profile_v1
    workflow      text NOT NULL CHECK (workflow IN ('enrich','deep_research','search')),
    processor     text NOT NULL,                 -- enrich: lite|base|core ; research: lite|base|core|pro (ultra deferred)
    output_schema jsonb,                          -- JSON Schema = Lance column contract (enrich); NULL for research/search
    objective     text,                          -- deep_research / search prompt
    grain         text,                          -- deep_research: 'per_entity' | 'topic'
    dataset_uri   text NOT NULL,
    result_key    text NOT NULL DEFAULT 'company_id',
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now()
);
-- pipelines/parallel/ops_parallel_runs.sql   (mirrors ops_cslb_runs.sql; one ledger, all workflows)
CREATE TABLE IF NOT EXISTS ops.parallel_runs (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    spec_id text NOT NULL,
    workflow text NOT NULL CHECK (workflow IN ('enrich','deep_research','search')),
    run_kind text NOT NULL CHECK (run_kind IN ('test','full')),
    group_id text, audience_id uuid, idempotency_key text,
    requested bigint, skipped_no_domain bigint, completed bigint, failed bigint,
    failed_company_ids jsonb NOT NULL DEFAULT '[]',
    processor text, cost_estimate numeric, cost_cap numeric,
    status text NOT NULL, error text,
    started_at timestamptz, completed_at timestamptz, recorded_at timestamptz NOT NULL DEFAULT now()
);
-- pipelines/parallel/ops_parallel_review.sql  (gate queue; values land in Lance regardless)
CREATE TABLE IF NOT EXISTS ops.parallel_review (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    spec_id text NOT NULL, run_id text, company_id text NOT NULL,
    field text NOT NULL, confidence text, resolved boolean NOT NULL DEFAULT false,
    recorded_at timestamptz NOT NULL DEFAULT now()
);
```

---

## 5. The three workflows (separate, per the directive — one `mode` flag was rejected)

Each workflow is an independent `trigger.dev → Modal → Parallel` lane with its own
purpose-named gtm-mcp tool, its own Trigger task, and its own Modal worker entrypoint + Modal
app. The common Parallel HTTP client (`core/parallel_client.py`) is the only shared code,
shipped to each worker container via `Image.add_local_python_source("core.parallel_client")`.

### 5.1 Enrichment — `parallel-enrich` (segmentation workhorse) — BUILT

- **gtm-mcp tool** `enrich_companies(spec_id, audience_id, processor?, test_limit=3, max_runs?, max_usd?)`.
- **trigger task** `src/trigger/parallel_enrich.ts` (id `parallel-enrich`): validate + clamp (HARD pre-dispatch `max_runs` cap, ≤1000); `wait.createToken({timeout, idempotencyKey})`; POST the Universal Dispatcher `{app_name:"parallel-enrich", function_name:"enrich_companies", kwargs, trigger_callback_url}`; `await wait.forToken`. idempotencyKey `f"{spec_id}:{audience_id}:{run_kind}"`; billing task → `retry:{maxAttempts:1}`.
- **Modal worker** `pipelines/parallel/enrich.py` (app `parallel-enrich`; `secrets=[parallel-api, r2-credentials, hqx-postgres]`): resolve `audience_id` → companies from `corex.audience.source_sql` (worker re-runs the SQL over the live Lance plane, JIT-registering only the named datasets); set `company_website = normalized_domain` (bare host, no scheme); null-domain rows → `company_name` only, counted; `POST /v1/tasks/groups` then `/runs` with **`default_task_spec`** (one wrapped `{type:"json",json_schema:…}`, ≤1,000 inputs/POST; `company_id` round-trips on each input's `metadata`); wait §0: poll `GET /v1/tasks/groups/{id}` until **`is_active==false`**, then `task_run_status_counts`, then `GET /runs?include_output=true`; DuckDB → typed columns + `<field>__confidence` (matched from Basis by field; `parallel-beta: field-basis-2025-11-25` for per-array-element basis) + `_basis`; drift null-fills missing/renamed keys (never throws mid-batch); arrays/nested → JSON column; `lance merge_insert("company_id")` → `s3://data-sink/active/enrichment/<spec>/`; BTREE `company_id` (+ `normalized_domain`); CONFIDENCE GATE (null/low/medium → `ops.parallel_review`, value still lands); land completed, persist failed `company_id`s; `ops.parallel_runs`; flat-JSON callback. Single-entity **smoke path** `smoke_one`: `POST /v1/tasks/runs` + blocking `GET /result?timeout=600`. Tier ceiling = `core`.

### 5.2 Deep Research — `parallel-deep-research` (cited report) — BUILT

- **gtm-mcp tool** `deep_research(objective, grain="topic", audience_id?, spec_id?, processor="base", output_type="text", test_limit=3, max_runs?, max_usd?)`.
- **trigger task** `src/trigger/parallel_deep_research.ts` (id `parallel-deep-research`): same durable-callback shape; **rejects `ultra` up front**; clamps per_entity `max_runs`.
- **Modal worker** `pipelines/parallel/deep_research.py` (app `parallel-deep-research`): `output_schema={"type":"text"}` (or `{"type":"auto"}`); wait = blocking `GET /v1/tasks/runs/{id}/result?timeout=600` **looped, 408-aware**, within the fn timeout. **Landing (decided):** Lance `s3://data-sink/active/parallel_research/` with a `grain` param — `per_entity` → one row per `company_id` `{company_id, normalized_domain, report_md, basis, objective, processor, spec_id, run_id, grain, created_at}`; `topic` → one row keyed `run_id` `{run_id, objective, report_md, basis, processor, ..., grain, created_at}`. One dataset, two grains: same artifact + same consumer; `merge_insert` on the grain key; BTREE on `company_id` and `run_id`. **v1 supports up to `pro`; `ultra` REJECTED** (per-run webhook path deferred per §0/§9).

### 5.3 Web Search — `parallel-search` (evidence-gathering, synchronous) — BUILT

- **gtm-mcp tool** `web_search(objective?, search_queries?, mode="advanced", source_policy?, persist=false, spec_id?)`.
- **trigger task** `src/trigger/parallel_search.ts` (id `parallel-search`): same trigger→dispatcher→Modal→callback shape even though the call is synchronous, so the surface is uniform.
- **Modal worker** `pipelines/parallel/search.py` (app `parallel-search`): `POST /v1/search {objective, search_queries ≤5, mode:"advanced", source_policy?}` — SYNCHRONOUS, no run lifecycle, no groups, no basis, no poll (§0). Returns ranked excerpts **INLINE** in the flat callback (capped at 50 for sanity); **landing (decided):** optional `persist=true` → `s3://data-sink/active/parallel_search/` (one row per excerpt, keyed `run_id`, BTREE); default inline-only. `ops.parallel_runs` (workflow `search`).

- **Auth (all three):** `x-api-key: $PARALLEL_API_KEY`, base `https://api.parallel.ai` (`core/parallel_client.py`). Endpoints per §0 (live OpenAPI).
- **Batch:** ≤1,000 runs/POST (live group-api); the enrich worker chunks at 1000.

---

## 6. gtm-mcp surface (BUILT — `apps/gtm_mcp/src/tools/parallel.py`, registered in `main.py`)

Control-plane signal only (each tool fires a Trigger run; writes no dataset, calls no Parallel directly). Trigger payloads are **camelCase** (matching the TS tasks). All five are registered on the FastMCP server (verified: `enrich_companies`, `deep_research`, `web_search`, `define_enrichment_spec`, `refresh_catalog`).

- `define_enrichment_spec(name, processor, output_schema) → spec_id` — persists to `ops.parallel_specs` (workflow `enrich`), computes `dataset_uri`. Agent authors `output_schema` (pure JSON-Schema data columns) with the operator. **Maximize fields** — `core ≈10`, `pro ≈20`. The wire wrapping (`{type:"json",json_schema:…}`) is applied at dispatch.
- `enrich_companies(spec_id, audience_id, processor=None, test_limit=3, max_runs=None, max_usd=None)` — validates the audience is a `company_id` audience + the spec exists; `test_limit=3` → sample, rows land + inspectable; `0` → full; **hard pre-dispatch cost cap** (`max_runs`) enforced in the Trigger task. Emits the idempotencyKey.
- `deep_research(objective, grain="topic", audience_id=None, spec_id=None, processor="base", output_type="text", test_limit=3, max_runs=None, max_usd=None)` — `grain="per_entity"` requires a `company_id` audience; **`ultra` raises**.
- `web_search(objective=None, search_queries=None, mode="advanced", source_policy=None, persist=False, spec_id=None)` — synchronous; excerpts returned inline in the run output.
- `refresh_catalog()` — `get_registry(refresh=True)` **+** `catalog.invalidate()` (clears `_lance_cache`/`_schema_cache`) so a just-landed `enrichment/<spec>` or `parallel_research` dataset is fully visible (name + columns).

Selection source = `corex.audience` (`define_audience` stores `source_sql` + `result_key`). Agent saves the set as an audience, references `audience_id`; the worker re-runs the SQL to resolve `company_id`s (no large lists through the trigger payload).

---

## 7. Spec & schema design

`output_schema` = **pure data columns**. Provenance is NOT inlined — Parallel's native **Basis** sidecar carries citations/reasoning/confidence per field. (Calibration figures from the diagnostic; the live basis guide states **all** processors return a confidence rating — so do not assert lite/base omit it. Default behavior: land the value, gate on confidence, flag missing/low in `ops.parallel_review`.)

> **On the equipment JSONs (`~/Desktop/hq/parallel-data-equipment/`):** the Claygent cost fingerprint (`totalCostToAIProvider`, `forcedToFinishEarlyBecauseOfCost`) is present in **three of six** files (`isEquipmentSeller-status`, `equipment-financing-status`, `equipment-seller-extract`); `enriched-company.json` is PDL. The two the proof spec is built from (`industries-served`, `equipment-financing-classification`) carry only `sources`/`evidence`/`reasoning`/`stepsTaken` — **ambiguous, not decisively Clay**. The "Clay → Parallel migration" thesis holds where the cost-truncation failure mode is visible; the blanket attribution was too strong. Either way: re-author as pure-data `output_schema` + Basis.

**Proof spec — `equipment_profile_v1`** (processor `core`):

```json
{
  "type": "object",
  "properties": {
    "is_equipment_seller":      { "type": "boolean", "description": "True iff the company DIRECTLY SELLS/resells physical equipment, not financing/leasing it." },
    "financing_classification": { "type": "string", "enum": ["bank_or_credit_union","independent_lender","captive_lessor","broker","not_a_financier"], "description": "Ownership/structure, disambiguated across sources (a 'division of <Bank>' = bank_or_credit_union)." },
    "industries_served":        { "type": "array", "items": { "type": "string" }, "description": "Distinct end-customer industry sectors served, verbatim from the company's own site where stated." }
  },
  "required": ["is_equipment_seller","financing_classification","industries_served"],
  "additionalProperties": false
}
```

**Wire form:** sent as `task_spec.output_schema = {"type":"json","json_schema": <the object above>}` (a bare JSON-Schema object is rejected); for a batch, set it once as the group `default_task_spec`.

Input per company: `{company_name, company_website}` where `company_website` is the **bare `normalized_domain`** — no scheme (verified live; the quickstart example uses `{"company_name":"Stripe","website":"stripe.com"}`). The operator's "100 surety bond companies → regional geo / insurance lines written" is a second spec (`surety_profile_v1`).

---

## 8. Secrets & config

- **`PARALLEL_API_KEY` already exists in Doppler `core-x/prd`** (verified) — no provisioning needed. **Mint the Modal secret `parallel-api` from it** (genuinely absent; name matches the `<vendor>-api` convention, e.g. `exa-api`).
- **`TRIGGER_SECRET_KEY`**: `hydration.py:69` reads `TRIGGER_SECRET_KEY`, but Doppler `core-x/prd` exposes `TRIGGER_SHARED_SECRET`. **Confirm the gtm-mcp service env actually sets `TRIGGER_SECRET_KEY`** (i.e. that hydration works today) before assuming the new tools inherit it.
- R2 / Postgres unchanged (`r2-credentials`, `hqx-postgres`, `HQX_DB_URL_POOLED` — all present in `core-x/prd`).
- If the trigger task itself calls Parallel, add `PARALLEL_API_KEY` to `trigger.config.ts` `syncEnvVars`. (In the corrected topology the *worker* calls Parallel, so this may be unnecessary — the worker reads the Modal secret.)

---

## 9. Build phases (dependency-ordered) — status

DONE (code complete + statically verified):
1. **Shared client.** `core/parallel_client.py` (auth, retry/backoff, Task/Group/Search helpers, output_schema envelopes).
2. **Ledgers.** `ops_parallel_specs.sql`, `ops_parallel_runs.sql`, `ops_parallel_review.sql`; each worker self-applies the DDL.
3. **Enrichment workflow.** `parallel_enrich.ts` + `pipelines/parallel/enrich.py` (audience resolve → website-derive + null-skip → group create/add → §0 group poll → fetch → DuckDB typed cols + Basis confidence + drift null-fill + array→JSON → `merge_insert("company_id")` → BTREE → confidence gate → partial-failure → ledger → callback; `smoke_one` single-run path).
4. **Deep Research workflow.** `parallel_deep_research.ts` + `pipelines/parallel/deep_research.py` (blocking `/result` loop; `grain` per_entity|topic landing; ultra rejected).
5. **Web Search workflow.** `parallel_search.ts` + `pipelines/parallel/search.py` (sync `/v1/search`; inline excerpts; optional persist).
6. **Catalog refresh.** `catalog.invalidate()` + `refresh_catalog()` tool (§4.2).
7. **gtm-mcp tools.** `apps/gtm_mcp/src/tools/parallel.py`: `define_enrichment_spec`, `enrich_companies`, `deep_research`, `web_search`, `refresh_catalog`; registered in `main.py`.

REMAINING TO GO LIVE (no prod mutations were performed):
0. **Mint Modal secret `parallel-api`** from Doppler `core-x/prd` (key `PARALLEL_API_KEY`) — genuinely absent; referenced by all three workers.
8. **Confirm `TRIGGER_SECRET_KEY`** on the gtm-mcp service env (Doppler holds `TRIGGER_SHARED_SECRET`; same prerequisite as `hydration.py`).
9. **Deploy:** `modal deploy pipelines/parallel/enrich.py` (+ `deep_research.py`, `search.py`); `modal run pipelines/parallel/enrich.py::init_ops` (creates the three ops tables); deploy the Trigger project (`npm run trigger:deploy` via `doppler run`).
10. **First spec + smoke.** `equipment_profile_v1`; `enrich_companies(..., test_limit=3)` → inspect inline rows + Basis; full run; verify dataset, BTREE, `refresh_catalog`, queryability. Then the segmentation proof (`companies ⋈ "enrichment/…"` on `company_id`).

**Deferred (flagged, not silently dropped):** the **per-run** webhook ingress (required before any `ultra`-tier enrich or deep-research spec — groups have no group webhook); automated/scheduled runs; SAM/UEI cohort; Parallel Search **MCP** bolt-on for interactive lookups; the optional opportunistic refresh-on-miss in `execute_audience_query` (the explicit `refresh_catalog()` covers the post-run case).

---

## 10. Risks & gotchas

- **Catalog refresh (§4.2)** — registry refresh alone leaves `catalog.py` caches stale; must clear both. The one thing that silently breaks "dynamic per-spec."
- **Topology bound chain** — `ultra` exceeds exa's 1h ceilings; v1 caps at `core`; ultra needs per-run webhook tokens. Don't ship an ultra spec on the poll path.
- **Group completion** — authoritative terminal = group `is_active==false` (then read `task_run_status_counts.{completed,failed}`). One cheap live test still warranted to confirm latency, not semantics.
- **Partial failure / re-run** — land completed, persist failed ids, anti-join on re-run; never re-bill completed rows.
- **Idempotency + cost cap** — both enforced at the launch tool; a double-fire or an oversized audience must not silently bill.
- **Array/nested mapping + drift** — explicit `LIST`/`STRUCT` rules + null-fill; per-element basis needs the beta header.
- **Null-domain companies** — send `company_name`-only (still a valid anchor), count them; never dispatch a fully-empty input that still bills.
- **Confidence coverage** — the OpenAPI (authoritative) says `confidence` is **nullable** and "Only certain processors provide confidence levels"; the docs-guide "all processors" line is contradicted by the schema. Keep land-and-flag; treat null/low as the review trigger.
- **Cost governance** — default `base`/`core`; the `test_limit=3` gate + hard cap are the spend guards.

---

## 11. Open decisions (operator)

1. **Spec authoring locus** — agent-authored in chat (assumed) vs a curated spec library.
2. **Search landing** — persist excerpts vs inline-only (v1: persist + inline for test).
3. **`lead.research` projection** — wire `attach_research` now vs defer until segmentation is proven (plan defers; Frame A first).

---

## 12. Build-time confirmations (UNVERIFIED — check before/at coding)

- **`POST /v1/tasks/runs` exact body nesting** (`task_spec.output_schema={type:"json", json_schema:{…}}`, top-level `processor`, `input`) — confirm against `public-openapi.json` `components.schemas` (create-run ref page 404'd at review).
- **Rate limits** — 2,000/min create + any per-group ceiling beyond the confirmed 1,000 runs/POST.
- **Group terminal semantics** — no live Parallel call was made (would bill); confirm with one cheap test run at build.
- **`TRIGGER_SECRET_KEY` on the gtm-mcp service** — Doppler has `TRIGGER_SHARED_SECRET`; confirm the service aliases it.
- **`-fast` / exact $/1k per tier** — pricing page not re-fetched; figures inherited from the diagnostic.
