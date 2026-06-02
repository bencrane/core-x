# Atomic Enrichment — Blitz-API Firmographic Workflows (Directive 23)

> **Status:** SPEC — for review. **No production code is written until this design
> contract is signed off** (Directive 23 mandate). This document is the contract.
>
> - **Verified against:** live BlitzAPI OpenAPI v2 (`blitz-api` docs MCP, 2026-06-02),
>   [`docs/reference/BLITZ_API_CANONICAL_REFERENCE.md`](BLITZ_API_CANONICAL_REFERENCE.md),
>   [`ARCHITECTURE.md`](../../ARCHITECTURE.md),
>   [`docs/reference/04_trigger_orchestration.md`](04_trigger_orchestration.md),
>   [`docs/reference/FIRMOGRAPHICS_BLITZ_MATERIALIZATION_PLAN.md`](FIRMOGRAPHICS_BLITZ_MATERIALIZATION_PLAN.md).
> - **Reference implementations mirrored:** [`src/trigger/sam_opps_bulk.ts`](../../src/trigger/sam_opps_bulk.ts),
>   [`core/modal_dispatcher.py`](../../core/modal_dispatcher.py),
>   [`pipelines/firmographics_blitz/materialize_blitz.py`](../../pipelines/firmographics_blitz/materialize_blitz.py),
>   [`apps/gtm_mcp/src/tools/audience.py`](../../apps/gtm_mcp/src/tools/audience.py).
> - **Scope:** payload schemas for the 3 entry paths · the shared rate-limit queue ·
>   JIT skip pseudo-code. Recon & blueprint only.

---

## 0. Executive summary — what this changes

Three atomic, rate-governed firmographic workflows over the Blitz-API, wired through
the existing core-x control/compute/data planes. The decisive recon finding: **the
output sink already exists.** The `firmographics-blitz` materializer
([`pipelines/firmographics_blitz/materialize_blitz.py`](../../pipelines/firmographics_blitz/materialize_blitz.py))
already projects `ops.task_runs` rows tagged `blitz_firmo_direct` and
`modal_hydrate_firmo_cascade` into the `firmographics_blitz` Lance system-of-record
(165,884 completed rows as of 2026-06-02). **Those two task_types *are* Workflows B
and C.** Directive 23 formalizes them as first-class core-x feeds, adds the new
Workflow A (domain resolution), and introduces the one piece of net-new
infrastructure the repo does not have: **a shared, centralized rate-limit queue.**

**Minimal-blast-radius principle** — the design adds exactly one new cross-cutting
component and one new external secret; everything else reuses proven patterns:

| Component | Status | Rationale |
|---|---|---|
| `core/blitz_gateway.py` — single-container 5-RPS priority egress | **NET-NEW** | No shared rate limiter exists anywhere in the repo (confirmed by grep). The 5 RPS ceiling is the scarce global resource; it gets one authoritative gatekeeper. |
| `blitz-api` Modal secret (the `x-api-key`) | **NET-NEW** | A new external API credential. Held by the gateway **only** — workers never see it. |
| `pipelines/enrichment_blitz/` — planner + A/B/C workers | **NET-NEW** | A new feed domain under `pipelines/`, per [`ARCHITECTURE.md` §3](../../ARCHITECTURE.md). |
| `src/trigger/enrichment_blitz.ts` — orchestrator tasks + queues | **NET-NEW** | One Trigger task surface per [§7 of `04_trigger_orchestration.md`](04_trigger_orchestration.md). |
| `ops.enrichment_blitz_runs` table | **NET-NEW** | Terminal run-state ledger, mirrors `ops.firmographics_blitz_runs`. |
| `ops.task_runs` event-log contract (task_types `blitz_firmo_direct` / `modal_hydrate_firmo_cascade`) | **REUSED, UNCHANGED** | Workflows B/C write the *exact* JSONB shape the existing materializer reads. Zero change to the 165k-row pipeline. |
| `firmographics-blitz` materializer → Lance system-of-record | **REUSED, UNCHANGED** | Rolls the event log into `firmographics_blitz` on its cadence. The live workflows are pure event-log *writers*; the materializer stays the only Lance *writer*. **No new Lance write pattern is introduced.** |
| `companies` + `firmographics_blitz` Lance datasets | **READ-ONLY (JIT skip)** | The warehouse guardrail reads these (BTREE pushdown) to decide what to skip. |

The Universal Dispatcher ([`core/modal_dispatcher.py`](../../core/modal_dispatcher.py))
and the Trigger durable-callback pattern are reused verbatim — **zero new web
endpoints, zero new dispatcher secrets.**

---

## 1. Prior art & ground truth

### 1a. The event log already carries B and C

`ops.task_runs` (hq-x control-plane Postgres, `HQX_DB_URL_POOLED`) is the firmographic
event log. The existing materializer reads exactly two task_types and unifies their
payloads:

```
blitz_firmo_direct          → result_payload.blitz_payload.{found, company{…}}      (Workflow B)
modal_hydrate_firmo_cascade → result_payload.blitz_data.{found, company{…}}         (Workflow C)
                              (+ top-level uei, domain, linkedin_url, status)
```

Unified by `COALESCE(result_payload->'blitz_payload', result_payload->'blitz_data')`.
**Directive 23 preserves these names and shapes byte-for-byte** so the materializer
keeps working untouched. Workflow A earns a third, new task_type
(`blitz_domain_resolve`) that the firmo materializer deliberately ignores (A produces
a resolution, not firmographics).

### 1b. The Blitz `company{}` object == the firmo grain

`POST /v2/enrichment/company` (Workflow B's endpoint) returns precisely the
`company{}` object the materializer already flattens into the 24-column
`firmographics_blitz` schema (`name`, `website`, `industry`, `size`→`employee_size_band`,
`employees_on_linkedin`, `type`→`company_type`, `founded_year`, `followers`,
`specialties`, `about`, `hq{city,state,region,continent}`, `linkedin_url`,
`linkedin_id`). A Workflow-B result is, by construction, a valid `blitz_firmo_direct`
row. (Confirmed: OpenAPI v2 example and the materializer projection align field-for-field.)

### 1c. The warehouse the JIT skip reads

| Dataset | URI | Anchor (BTREE) | Skip-relevant columns |
|---|---|---|---|
| `companies` | `s3://data-sink/active/companies/` | `normalized_domain` | `company_linkedin_url` (nullable) |
| `firmographics_blitz` | `s3://data-sink/active/firmographics_blitz/` | `domain_norm` | `linkedin_url`, `source_updated_at`, `materialized_at` |
| `ops.task_runs` | hq-x Postgres | `(task_type, domain)` | `status`, `result_payload.found`, `updated_at` |

Point-lookups push the predicate into the Lance BTREE for sub-100 ms answers
(`lance.dataset(uri, so).scanner(filter=…, columns=…).to_table()` —
[`audience.py:65`](../../apps/gtm_mcp/src/tools/audience.py)).

### 1d. Negative finding — no rate limiter exists

There is **no** shared rate-limiting queue, token bucket, semaphore, `p-limit`,
`Bottleneck`, or governed-concurrency primitive anywhere in core-x. Existing workers
self-govern with hard per-worker call ceilings (e.g. the OSHA sniper's
`MAX_API_CALLS=5`). Three workflows sharing one set of Blitz keys at a hard 5 RPS
**cannot** be governed per-worker — the ceiling is global. §3 designs it.

### 1e. Blitz-API hard facts (verified)

- **Base:** `https://api.blitz-api.ai` · **Auth:** `x-api-key` header.
- **Rate limit:** **5 RPS on all plans.** Exact value is `max_requests_per_seconds`
  from `GET /v2/account/key-info` — read it, don't hardcode. 429 on exceed; **wait
  ≥60 s before retry** after a server-side 429. Client-side limiting must prevent it.
- **Cost:** paid plans are **flat-rate unlimited** (no per-request fee); credits are
  metered only on the free trial. ⇒ the throttle protects the **RPS ceiling and
  end-to-end latency**, not a credit balance (on paid). JIT skip still matters: it
  removes calls from a 5-RPS-bottlenecked pipe, directly shortening wall-clock.
- **Recommended client timeout:** 10 s for enrichment endpoints.

---

## 2. The three workflows — contracts

Priority classes (used by the rate gateway, §3):

| Class | Assigned to | Intent |
|---|---|---|
| `HIGH` | Standalone Workflow B; interactive single-company calls from the GTM agent | Latency-sensitive; must preempt bulk |
| `NORMAL` | Workflow C cascades (both internal hops) | Atomic per-company hydration |
| `LOW` | Bulk Workflow A resolution batches | Throughput; yields to HIGH/NORMAL, never fully starved |

### 2A. Workflow A — Domain Resolution (`domain → company_linkedin_url`)

```
domain ──[JIT: companies.company_linkedin_url? firmographics_blitz.linkedin_url? neg-cache?]──┐
         (miss) ──▶ blitz-gateway ──▶ POST /v2/enrichment/domain-to-linkedin ──▶ {found, url} │
              └──▶ ops.task_runs(task_type=blitz_domain_resolve) ──▶ (materializer ignores)    │
         (hit)  ──▶ skip API; emit resolved url from warehouse ◀────────────────────────────────┘
```

- **Endpoint:** `POST /v2/enrichment/domain-to-linkedin`. **Errors:** 402 (trial
  credits), 422 (bad input), 500, plus 429 (handled by gateway).
- **Output:** the resolved URL is recorded to `ops.task_runs(blitz_domain_resolve)`.
  Workflow A does **not** write Lance (see §5). Its result becomes JIT-visible via the
  event log immediately and via `firmographics_blitz`/`companies` once a downstream B
  enriches the LinkedIn URL.
- **`found:false` boundary:** domain does not map to a LinkedIn page (private / very
  small / different public domain). Recorded as `found:false` → negative-cached
  (§4) so the same domain is not re-hammered within `NEG_TTL_DAYS`.

#### A — payload schemas (all five layers)

```jsonc
// (1) Trigger task input  — src/trigger/enrichment_blitz.ts : resolveDomains
{
  "cohort": { "inline": ["openai.com", "stripe.com"] },   // OR {"r2_key":"staging/<batch>.parquet"} for large cohorts
  "priority": "low",                                       // default LOW for A
  "batch_label": "gtm-cohort-2026-06-02"                   // optional, for run lineage + tags
}

// (2) Universal Dispatcher body  — POST {MODAL_DISPATCHER_URL}  (proxy-auth headers Modal-Key/Modal-Secret)
{
  "app_name": "enrichment-blitz",
  "function_name": "run_resolve_domains",
  "kwargs": { "cohort_ref": "...", "priority": "low", "batch_label": "..." },
  "trigger_callback_url": "https://api.trigger.dev/api/v1/waitpoints/tokens/{id}/callback/{hash}"
}

// (3) Blitz request / response  — POST /v2/enrichment/domain-to-linkedin  (issued BY THE GATEWAY)
//     request:
{ "domain": "openai.com" }
//     response 200:
{ "found": true, "company_linkedin_url": "https://www.linkedin.com/company/openai" }
//     response 200 (miss):
{ "found": false }

// (4) ops.task_runs row written by the worker  (per company)
{
  "run_id": "<uuid>", "task_type": "blitz_domain_resolve", "status": "completed",
  "domain": "openai.com",
  "result_payload": { "found": true, "company_linkedin_url": "https://www.linkedin.com/company/openai" },
  "updated_at": "2026-06-02T17:00:00Z"
}

// (5) Trigger terminal callback  (RAW body — no {data} wrapper; whole body becomes result.output)
{ "status": "success", "workflow": "A", "feed": "enrichment_blitz",
  "requested": 100, "skipped_warehouse": 40, "skipped_neg_cache": 6,
  "api_calls": 54, "resolved": 49, "unresolved": 5, "failed": 0 }
```

### 2B. Workflow B — LinkedIn Enrichment (`company_linkedin_url → enriched_firmo`)

```
company_linkedin_url ──[JIT: firmographics_blitz fresh? neg-cache?]──┐
         (miss) ──▶ blitz-gateway ──▶ POST /v2/enrichment/company ──▶ {found, company{…}}
              └──▶ ops.task_runs(task_type=blitz_firmo_direct) ──▶ firmographics-blitz materializer ──▶ Lance
         (hit)  ──▶ skip API
```

- **Endpoint:** `POST /v2/enrichment/company`. **Errors:** 401, 404, 429.
- **Output:** the full `company{}` profile is recorded to
  `ops.task_runs(blitz_firmo_direct)` under `result_payload.blitz_payload` — the exact
  shape the materializer reads. **Default priority HIGH** (the directive's
  "high-priority Workflow B job").
- **State checks:** `found:true` required for a firmo row; `found:false` →
  negative-cached. Firmo freshness gate: skip if `firmographics_blitz.source_updated_at`
  for the resolved `domain_norm` is within `FIRMO_TTL_DAYS`.

#### B — payload schemas (layers that differ from A)

```jsonc
// (1) Trigger task input  — enrichmentBlitz : enrichLinkedIn
{ "cohort": { "inline": ["https://www.linkedin.com/company/openai"] }, "priority": "high" }

// (3) Blitz request / response  — POST /v2/enrichment/company
//     request:
{ "company_linkedin_url": "https://www.linkedin.com/company/openai" }
//     response 200 (company{} = the firmo grain; 14 keys + hq{8}):
{ "found": true, "company": {
    "linkedin_url": "...", "linkedin_id": 108037802, "name": "...", "about": "...",
    "specialties": null, "industry": "Technology; Information and Internet",
    "type": "Privately Held", "size": "1-10", "employees_on_linkedin": 3,
    "followers": 6, "founded_year": null, "domain": "blitz-api.ai", "website": "https://blitz-api.ai",
    "hq": { "city": "Paris", "state": null, "postcode": null, "country_code": "FR",
            "country_name": "France", "region": null, "continent": null, "street": null } } }

// (4) ops.task_runs row  — MUST nest under blitz_payload to feed the materializer
{ "run_id": "<uuid>", "task_type": "blitz_firmo_direct", "status": "completed",
  "domain": "blitz-api.ai",                                  // top-level anchor (materializer reads this)
  "uei": null,
  "result_payload": { "blitz_payload": { "found": true, "company": { /* …as above… */ } } },
  "updated_at": "2026-06-02T17:00:00Z" }

// (5) terminal callback
{ "status": "success", "workflow": "B", "feed": "enrichment_blitz",
  "requested": 50, "skipped_warehouse": 8, "api_calls": 42, "enriched": 41, "not_found": 1, "failed": 0 }
```

### 2C. Workflow C — Full Cascade (`domain → company_linkedin_url → enriched_firmo`)

```
domain ─[JIT plan]─┬─ DONE         (fresh firmo present)                    ─▶ skip both hops
                   ├─ NEEDS_B      (linkedin_url known, firmo stale/absent)  ─▶ hop B only   ← the directive's "40 of 100"
                   ├─ NEEDS_A_THEN_B (no linkedin_url)                       ─▶ hop A, then conditionally hop B
                   └─ NEG_CACHED   (recent found:false)                      ─▶ skip
   hop A: gateway ▶ /domain-to-linkedin ▶ ops.task_runs(blitz_domain_resolve) ; if found ⇒ promote to NEEDS_B
   hop B: gateway ▶ /enrichment/company  ▶ ops.task_runs(modal_hydrate_firmo_cascade) ▶ materializer ▶ Lance
```

- **Unified transaction frame:** C is one Modal worker invocation per chunk. It runs
  the JIT plan once, executes hop A for the `NEEDS_A_THEN_B` partition, **re-checks
  state on the returned URL** (promote/skip), then executes hop B for everything that
  now has a URL. Per-company state is carried in-memory through the chunk; the worker
  emits one `ops.enrichment_blitz_runs` row + one terminal callback for the chunk.
- **task_type:** the firmo-producing hop writes `modal_hydrate_firmo_cascade` (under
  `result_payload.blitz_data`, + top-level `uei`/`domain`/`linkedin_url`/`status` —
  the exact existing cascade shape). The resolution hop writes `blitz_domain_resolve`.
- **Default priority NORMAL.** Both internal hops inherit the cascade's class so a
  cascade is not starved behind a bulk-A batch (LOW) yet yields to interactive B (HIGH).
- **Error boundaries:** hop A `found:false` ⇒ company terminates `unresolved` (no hop
  B, negative-cached). hop B `found:false` ⇒ `not_found`. Any 5xx/429 after the
  gateway's retries ⇒ that company marked `failed` (the chunk continues; failures are
  per-company, not per-chunk).

#### C — payload schemas (layers that differ)

```jsonc
// (1) Trigger task input  — enrichmentBlitz : cascade
{ "cohort": { "r2_key": "staging/gtm-cohort-2026-06-02.parquet" }, "priority": "normal",
  "firmo_ttl_days": 180, "neg_ttl_days": 30 }

// (4) ops.task_runs row for the firmo hop  — nest under blitz_data (cascade shape)
{ "run_id": "<uuid>", "task_type": "modal_hydrate_firmo_cascade", "status": "completed",
  "domain": "openai.com", "uei": null, "linkedin_url": "https://www.linkedin.com/company/openai",
  "result_payload": { "status": "ok",
                      "blitz_data": { "found": true, "company": { /* company{} */ } } },
  "updated_at": "2026-06-02T17:00:00Z" }

// (5) terminal callback
{ "status": "success", "workflow": "C", "feed": "enrichment_blitz", "requested": 100,
  "skipped_done": 22, "skipped_neg_cache": 6, "routed_needs_b": 40, "routed_a_then_b": 32,
  "api_calls_a": 32, "api_calls_b": 70, "resolved": 28, "enriched": 66, "unresolved": 4, "failed": 0 }
```

> **Note — `result.output` is signal, not data.** Per the data-plane law
> ([§7 of `04_trigger_orchestration.md`](04_trigger_orchestration.md)), every terminal
> callback is terminal *metadata* (counts), never firmo rows. The cohort travels by
> reference (R2 key) for large batches; firmo lands in `ops.task_runs` → Lance, never
> through Trigger.

---

## 3. Shared rate-limiting queue architecture

### 3.1 The problem statement

Three workflows × N concurrent chunk-workers × one shared Blitz key set, hard-capped at
**5 RPS globally**, with the requirement that *a bulk Workflow-A batch must not starve a
high-priority Workflow-B job.* A per-worker limiter cannot enforce a global ceiling
(Modal scales workers horizontally; 8 workers each "limiting to 5 RPS" = 40 RPS = 429
storms). The scarce resource is global, so the governor must be global.

### 3.2 Design — one authoritative egress: `core/blitz_gateway.py`

A dedicated Modal app `blitz-gateway`, the **single point through which every Blitz HTTP
request in the fleet flows.** This is the exact philosophical parallel to the Universal
Dispatcher: one cross-cutting concern (there, fleet ingress; here, rate-bound Blitz
egress) localized in one `core/` Modal app.

```
   A-workers ─┐
   B-workers ─┼─▶  modal.Function.from_name("blitz-gateway","blitz_call").remote(...)
   C-workers ─┘                         │
                                        ▼
        ┌──────────────  blitz-gateway  (max_containers=1, @modal.concurrent)  ──────────────┐
        │  one process ⇒ one authoritative token bucket                                       │
        │                                                                                     │
        │   inbound .remote() inputs ──▶ enqueue into PRIORITY LANES {HIGH, NORMAL, LOW}      │
        │                                                                                     │
        │   token emitter coroutine: releases ≤5 grants / rolling 1s  (deque-of-monotonic     │
        │       timestamps — verbatim from the Blitz docs base client, async-adapted)         │
        │   on each grant: serve HIGH ▶ NORMAL ▶ LOW, with a reserved FLOOR for the lower      │
        │       lanes so bulk never fully starves (LOW_LANE_FLOOR = 1 grant of every 5)        │
        │                                                                                     │
        │   per grant: issue the HTTP request (holds the only x-api-key), apply 429→sleep≥60s, │
        │       402/401/404 → terminal, 5xx → exp-backoff retry (≤3), 10s timeout              │
        │   return {found, data|null, http_status, ok} to the calling worker                  │
        └─────────────────────────────────────────────────────────────────────────────────────┘
```

**Why single-container is correct, not a liability:**
- The bucket is globally authoritative *for free* — one process, no distributed CAS,
  no clock-skew races, no coordination store. True 5 RPS, provably.
- The gateway does **no heavy compute** — it relays rate-bound HTTP. One container
  trivially sustains 5 RPS; the "throughput ceiling" of a single container *is* the
  intended ceiling.
- The Blitz `x-api-key` lives in exactly one place (the gateway's `blitz-api` secret).
  Workers never hold it — minimal credential blast radius.
- 429/backoff and the `key-info` health gate are centralized — one retry policy, one
  observability surface, not re-implemented per workflow.
- **SPOF is hollow here:** Modal auto-restarts the container; in-flight `.remote()`
  inputs are retried by Modal; on cold start the bucket simply resets to a fresh
  (never-exceeding) 5-RPS budget. A few seconds of cold-start latency is immaterial to
  a batch plane that is, by definition, 5-RPS-bound.

### 3.3 Two-tier throttle (answers Mandate #2 directly)

| Tier | Mechanism | Governs | Tuning |
|---|---|---|---|
| **Fine (hard)** | gateway token bucket | Exact global ≤5 RPS across A+B+C | `max_requests_per_seconds` read live from `/v2/account/key-info` at gateway boot |
| **Coarse** | Trigger.dev queue `concurrencyLimit` per workflow | # of chunk-workers alive at once (Modal container budget + gateway input-queue depth) | ≈ bandwidth-delay product: 5 RPS × ~1 s latency ⇒ keep ~5–10 chunk-workers in flight; enough to saturate the bucket, not so many that hundreds idle |

The Trigger queue is **not** the RPS governor (concurrency ≠ rate — 5 concurrent ×
200 ms = 25 RPS). It is the container-budget governor. The gateway bucket is the only
thing that enforces 5 RPS. Both are required; neither substitutes for the other.

### 3.4 Priority & anti-starvation (the exact rule)

- **HIGH preempts:** a sudden Workflow-B job is served at the next token grant. Worst-case
  B latency = one in-flight lower-lane grant ≈ ≤200 ms. This satisfies "running a batch
  of A jobs does not block a high-priority B job" — B never waits behind A's backlog,
  only behind at most one already-granted token.
- **LOW is reserved, not starved:** when LOW is backlogged, it is guaranteed
  `LOW_LANE_FLOOR` (default 1) grant out of every 5, so a continuous HIGH/NORMAL stream
  cannot indefinitely freeze a bulk-A batch.
- NORMAL (cascades) sits between: served after HIGH, before LOW, with its own floor.
- All knobs (`LOW_LANE_FLOOR`, lane weights) are gateway constants, tuned without
  touching workers.

### 3.5 Gateway interface (pseudo-code — NOT production code)

```python
# core/blitz_gateway.py   (Modal app "blitz-gateway")   — PSEUDO-CODE, for review only
image = debian_slim(py3.12).pip_install("requests>=2.32", "fastapi[standard]>=0.115")
app = modal.App("blitz-gateway", image=image)

ENDPOINTS = {                       # logical name → (path, priority_default)
    "resolve": "/v2/enrichment/domain-to-linkedin",
    "company": "/v2/enrichment/company",
}
LOW_LANE_FLOOR = 1                  # ≥1 LOW grant per 5 when LOW backlogged

@app.function(secrets=[modal.Secret.from_name("blitz-api")],
              max_containers=1,                       # ← the whole design rests on this
              timeout=60*60)
@modal.concurrent(max_inputs=256)                     # many concurrent .remote() into one container
async def blitz_call(endpoint: str, payload: dict, priority: str = "normal",
                     idem_key: str | None = None) -> dict:
    await _LANES[priority].put(_Job(endpoint, payload))      # enqueue into HIGH/NORMAL/LOW
    job = await _await_my_grant(priority)                    # emitter hands this job a token
    return _http_with_retries(endpoint, payload)             # holds the only x-api-key; 429→sleep≥60s

# background emitter (started on container boot): one loop, ≤5 grants / rolling 1s,
# serve HIGH▶NORMAL▶LOW honoring LOW_LANE_FLOOR. Bucket = deque[monotonic ts] (Blitz docs algo).

# caller side, inside an enrichment worker:
#   GW = modal.Function.from_name("blitz-gateway", "blitz_call")
#   r  = GW.remote(endpoint="company", payload={"company_linkedin_url": url}, priority="high")
#   if r["ok"] and r["data"]["found"]: write_firmo_row(r["data"]["company"])
```

### 3.6 Alternative considered (flagged for sign-off, §7-D2)

**Distributed token bucket** in a shared store (Modal `Dict` or a Postgres GCRA row),
workers scaling horizontally and each acquiring a token before its own HTTP call. At 5
RPS global the coordination load is trivial (≤5 acquisitions/s), and it removes the
single-container SPOF — **but** it adds per-call coordination correctness (CAS retries,
refill races, clock skew), spreads the Blitz key to every worker, and decentralizes
429 handling. Rejected as the primary for "no fragile abstractions," retained as the
documented fallback if the single-container gateway ever proves a real bottleneck
(it cannot, below 5 RPS).

---

## 4. JIT skip logic — the warehouse guardrail

**Principle:** never spend a 5-RPS-bound API call on data the warehouse already holds.
Run **once per chunk, batched**, before any gateway call. The directive's worked
example — *Workflow C for 100 companies, 40 already have `company_linkedin_url` → route
those 40 straight to Workflow B* — is the `NEEDS_B` partition below.

### 4.1 Three batched reads, then an in-memory partition

```python
# pipelines/enrichment_blitz/planner.py   — PSEUDO-CODE, for review only
#
# Inputs : cohort = [raw domain | linkedin_url], workflow ∈ {A,B,C}, FIRMO_TTL_DAYS, NEG_TTL_DAYS
# Output : routing plan {DONE, NEEDS_B, NEEDS_A_THEN_B, NEG_CACHED} + the resolved keys

def plan(cohort, workflow, firmo_ttl_days=180, neg_ttl_days=30):
    norms = { raw: normalize_domain(raw) for raw in cohort }          # fleet macro (audience.py:_normalize_domain)
    keys  = sorted({ n for n in norms.values() if n })
    in_list = ",".join("'" + k.replace("'", "''") + "'" for k in keys)

    # READ 1 — companies: is company_linkedin_url already populated?  (Lance BTREE pushdown, sub-100ms)
    have_url = {}
    for r in lance("companies").scanner(
            filter=f"normalized_domain IN ({in_list})",
            columns=["normalized_domain", "company_linkedin_url"]).to_table().to_pylist():
        if r["company_linkedin_url"]:
            have_url[r["normalized_domain"]] = r["company_linkedin_url"]

    # READ 2 — firmographics_blitz: is firmo present AND fresh?  (Lance BTREE pushdown)
    fresh_firmo, known_url = set(), dict(have_url)
    cutoff = now_utc() - days(firmo_ttl_days)
    for r in lance("firmographics_blitz").scanner(
            filter=f"domain_norm IN ({in_list})",
            columns=["domain_norm", "linkedin_url", "source_updated_at"]).to_table().to_pylist():
        if r["linkedin_url"]:
            known_url.setdefault(r["domain_norm"], r["linkedin_url"])
        if r["source_updated_at"] and r["source_updated_at"] >= cutoff:
            fresh_firmo.add(r["domain_norm"])

    # READ 3 — ops.task_runs: recent attempts → in-flight successes (not yet in Lance) + negative cache
    #   SELECT domain, task_type, result_payload->>'found' AS found, max(updated_at) ...
    #   WHERE domain IN (keys) AND updated_at >= now() - NEG_TTL_DAYS GROUP BY ...
    neg_cached, recent_url, recent_firmo = load_recent_attempts(keys, neg_ttl_days)
    known_url.update(recent_url); fresh_firmo |= recent_firmo

    # PARTITION (per company)
    plan = {"DONE": [], "NEEDS_B": [], "NEEDS_A_THEN_B": [], "NEG_CACHED": []}
    for raw, n in norms.items():
        if not n:                              plan["NEG_CACHED"].append(raw); continue   # unnormalizable
        if n in fresh_firmo:                   plan["DONE"].append(raw)                    # firmo already fresh
        elif n in known_url:                   plan["NEEDS_B"].append((raw, known_url[n])) # ← the "40 of 100"
        elif n in neg_cached:                  plan["NEG_CACHED"].append(raw)              # recent found:false
        else:                                  plan["NEEDS_A_THEN_B"].append(raw)          # cold → full cascade
    return plan
```

### 4.2 How each workflow consumes the plan

| Workflow | Acts on | Skips |
|---|---|---|
| **A** (`resolve`) | companies whose `domain_norm` is **not** in `known_url` and not `NEG_CACHED` | already-resolved domains + negative-cached |
| **B** (`enrich`) | linkedin_urls whose `domain_norm` is **not** in `fresh_firmo` | already-fresh firmo + negative-cached |
| **C** (`cascade`) | `NEEDS_A_THEN_B` → hop A then hop B; `NEEDS_B` → hop B only | `DONE` + `NEG_CACHED` |

### 4.3 Freshness & negative caching (threshold protection)

- **`FIRMO_TTL_DAYS` (default 180):** firmo is "fresh" if `source_updated_at` (the age
  of the enrichment, not the snapshot build) is within the window. Older ⇒ re-enrich.
- **`NEG_TTL_DAYS` (default 30):** a `found:false` from A or B is cached so the same
  dead domain/URL is not re-attempted every batch — this is the single biggest
  protector of the 5-RPS budget against repeated cohorts.
- **In-flight dedup:** READ 3 surfaces successes already in `ops.task_runs` but not yet
  rolled into Lance by the materializer, closing the materialization-lag gap so a domain
  enriched 5 minutes ago in a sibling batch is not re-called.
- **Within-batch dedup:** the planner normalizes the cohort to `domain_norm` and
  de-duplicates before partitioning, so a cohort listing `OpenAI.com` and
  `https://openai.com/` consumes one call, not two.

### 4.4 Why the planner runs in Modal (not Trigger)

Lance reads require Python + R2 credentials (the read-gateway pattern in
[`apps/gtm_mcp/src/database.py`](../../apps/gtm_mcp/src/database.py)); Trigger.dev tasks
are Node and carry no data-plane access. The planner is therefore the **first step
inside the Modal worker**, and only counts flow back to Trigger — consistent with the
data-plane law.

---

## 5. Data sinks, freshness & the one Lance decision

### 5.1 Primary design (recommended): live workflows are pure event-log writers

- A/B/C write results to **`ops.task_runs`** (preserving the task_type/JSONB contract,
  §1a/§2) + a per-chunk **`ops.enrichment_blitz_runs`** state row.
- The existing **`firmographics-blitz` materializer** rolls B/C rows into the
  `firmographics_blitz` Lance system-of-record on its cadence — **unchanged.** It stays
  the only writer of that dataset.
- **Consequence: Directive 23 introduces NO new Lance write pattern.** The fleet's
  overwrite-materialization model is untouched. The JIT skip's materialization-lag is
  fully covered by READ 3 (the live event log).

### 5.2 The Lance write decision (flagged for sign-off, §7-D1)

The directive language ("appends … directly into our active Lance layers", "updates the
company row in Lance") could be read as requiring **immediate** incremental Lance writes
(`lance.merge_insert(on="domain_norm")…`) from the live workflows for read-after-write.
That would introduce incremental Lance upsert — a pattern the batch-overwrite fleet has
never used. **Recommendation: do NOT** add it initially; rely on §5.1 (event log +
existing materializer + READ-3 lag coverage). If a confirmed product requirement needs
sub-cadence read-after-write in Lance, add a `merge_insert` fast-path mirror as a
follow-up — but that is a deliberate new fleet pattern requiring its own sign-off, not a
default. **No mutation of the `companies` dataset** either way: it is overwrite-materialized
from the gtm Postgres spine and a live write would be clobbered on its next refresh.

### 5.3 `ops.enrichment_blitz_runs` (mirror `ops.firmographics_blitz_runs`)

```sql
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.enrichment_blitz_runs (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed           text        NOT NULL,        -- 'enrichment_blitz'
    workflow       text        NOT NULL,        -- 'A' | 'B' | 'C'
    batch_label    text,
    priority       text        NOT NULL,        -- 'high' | 'normal' | 'low'
    requested      bigint      NOT NULL DEFAULT 0,
    skipped        bigint      NOT NULL DEFAULT 0,   -- warehouse + neg-cache
    api_calls      bigint      NOT NULL DEFAULT 0,   -- actual gateway calls (5-RPS budget spent)
    succeeded      bigint      NOT NULL DEFAULT 0,
    not_found      bigint      NOT NULL DEFAULT 0,
    failed         bigint      NOT NULL DEFAULT 0,
    status         text        NOT NULL,             -- 'success' | 'error'
    error          text,
    started_at     timestamptz,
    completed_at   timestamptz,
    recorded_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS enrichment_blitz_runs_feed_idx        ON ops.enrichment_blitz_runs (feed);
CREATE INDEX IF NOT EXISTS enrichment_blitz_runs_workflow_idx    ON ops.enrichment_blitz_runs (workflow);
CREATE INDEX IF NOT EXISTS enrichment_blitz_runs_recorded_at_idx ON ops.enrichment_blitz_runs (recorded_at DESC);
```

---

## 6. Build manifest (after sign-off) & control-plane checklist

| # | Artifact | Pattern mirrored |
|---|---|---|
| 1 | `core/blitz_gateway.py` (Modal app `blitz-gateway`) | new infra; philosophy of `core/modal_dispatcher.py` |
| 2 | `blitz-api` Modal secret (`BLITZ_API_KEY`) | held by the gateway only |
| 3 | `pipelines/enrichment_blitz/planner.py` | Lance reads — `apps/gtm_mcp/src/database.py` + `audience.py` |
| 4 | `pipelines/enrichment_blitz/workers.py` (`run_resolve_domains`, `run_enrich_linkedin`, `run_cascade`) | `pipelines/firmographics_blitz/materialize_blitz.py` (secrets, callback, ops write) |
| 5 | `pipelines/enrichment_blitz/ops_enrichment_blitz_runs.sql` | `ops.firmographics_blitz_runs` DDL |
| 6 | `src/trigger/enrichment_blitz.ts` (3 tasks + per-workflow queues, `batch.trigger` fan-out) | `src/trigger/sam_opps_bulk.ts` (token → dispatch → forToken) |
| 7 | `ARCHITECTURE.md` + this file: register the feed | house convention |

Per [§7 of `04_trigger_orchestration.md`](04_trigger_orchestration.md): **zero new web
endpoints, zero new dispatcher secrets.** Deploy: `modal deploy core/blitz_gateway.py`,
`modal deploy pipelines/enrichment_blitz/workers.py`,
`doppler run -- npx trigger.dev@4.4.4 deploy`.

---

## 7. Key design decisions (opinionated; flagged for sign-off)

1. **D1 — Live workflows write the event log, not Lance.** Reuse `ops.task_runs` +
   the existing materializer; introduce no incremental Lance upsert. JIT lag covered by
   READ 3. *Recommend yes.* (Alternative: `merge_insert` fast-path — defer to a separate
   sign-off only if read-after-write in Lance is a confirmed requirement.)
2. **D2 — Rate governor = single-container Modal `blitz-gateway`** (not a distributed
   bucket, not Trigger queue concurrency). One authoritative 5-RPS bucket, one Blitz key
   holder, in-process priority. *Recommend yes.*
3. **D3 — Preserve task_type names** `blitz_firmo_direct` (B) / `modal_hydrate_firmo_cascade`
   (C); add `blitz_domain_resolve` (A). Keeps the materializer untouched. *Recommend yes.*
4. **D4 — Priority defaults:** B=HIGH, C=NORMAL, A=LOW, with `LOW_LANE_FLOOR=1/5`.
   *Confirm the floor + whether interactive single-company calls from the GTM agent
   should always be HIGH regardless of workflow.*
5. **D5 — Cohort transport:** inline list for ≤~1k, R2 Parquet reference above that;
   chunk size default 50/worker; Trigger queue `concurrencyLimit` default ~8.
   *Confirm thresholds.*
6. **D6 — Freshness/negative TTLs:** `FIRMO_TTL_DAYS=180`, `NEG_TTL_DAYS=30`. *Confirm.*

## 8. Open items — sign-off checklist

- [ ] **D1** — event-log-only writes vs add `merge_insert` Lance fast-path?
- [ ] **D2** — single-container gateway approved as the rate governor?
- [ ] **D4** — priority defaults + `LOW_LANE_FLOOR` + GTM-agent-call override?
- [ ] **D5** — chunk size (50), Trigger `concurrencyLimit` (8), inline/R2 cohort cutoff (~1k)?
- [ ] **D6** — `FIRMO_TTL_DAYS` (180) / `NEG_TTL_DAYS` (30)?
- [ ] Confirm the live `max_requests_per_seconds` from `/v2/account/key-info` is **5**
      for the production key, and the active plan unlocks `domain-to-linkedin` +
      `enrichment/company` (Unlimited Leads tier or higher).
- [ ] Confirm the Blitz production `x-api-key` provisioning path (Doppler → `blitz-api`
      Modal secret).

**On sign-off:** build items §6.1–§6.7 in order (gateway + secret first, then workers,
then the Trigger surface), verify against this contract, ship per the standard git
lifecycle.
