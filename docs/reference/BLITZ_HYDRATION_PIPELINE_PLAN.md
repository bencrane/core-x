# Blitz Contact Hydration — Waterfall ICP, Gateway-Routed (Directive 20-Final)

**Status:** BUILT & MERGED. Decoupled, **Waterfall-only**, **gateway-routed**.
Supersedes the Directive-20 dual-mode draft (Employee Finder dropped per the
course correction — the upstream GTM agent generates title cascades dynamically,
so Waterfall ICP is the undisputed default ABM pipeline).
**Verified against live systems:** 2026-06-02 (Blitz docs MCP; deployed
`blitz-gateway` `key-info` → `Agency - Enterprise`, unlimited, 5 RPS; repo at
Directive-23 `main`).

---

## 0. What this is

An asynchronous pipeline that hydrates a saved campaign audience with **executive
contact data**. The interactive GTM agent fires it through one gtm-mcp tool; it
resolves the audience's companies from the Lance plane, finds the best
decision-maker(s) per account via **BlitzAPI Waterfall ICP**, enriches verified
work emails (and US phones), and lands the raw payloads to R2.

Two non-negotiable properties from the course correction:

1. **Waterfall-only.** No `persona_mode`, no Employee Finder. The Trigger payload
   accepts the **native Waterfall `cascade` array**, populated dynamically by the
   upstream Anthropic GTM agent.
2. **Centralized egress.** The worker holds **no Blitz key** and makes **no direct
   `api.blitz-api.ai` calls**. Every Blitz request — bridge, Waterfall, email,
   phone — is routed through the single-container `core/blitz_gateway.py`
   (`blitz-gateway`, Directive 23), which owns the only `BLITZAPI_API_KEY` and the
   authoritative global ≤5-RPS priority token bucket. Interactive hydration rides
   the **HIGH** lane to preempt background bulk sweeps.

---

## 1. Endpoint = Waterfall ICP (settled)

`POST /v2/search/waterfall-icp-keyword` — "the single best decision-maker at a known
company via a priority cascade." Our input is a **resolved company list** (the
audience) + a **per-account title cascade** the LLM generates — exactly the ABM /
Account-Breakthrough case Blitz routes to Waterfall. The cascade is the seniority
fallback: tier 1 (C-level) → tier 2 (VP) → tier 3 (Director), `max_results` filling
top-down for "the best possible seniority mix." Employee Finder (facet enumeration)
and Find People (multi-company) are out of scope.

**Live caps (docs "Waterfall Logic," used by the contract):** `max_results` **≤25**,
cascade **≤8 tiers**. (The in-repo `BLITZ_API_CANONICAL_REFERENCE.md` says 1–100;
the live page says ≤25 — the contract clamps to the lower live bound.)

**Bridge:** Waterfall keys on `company_linkedin_url`, not a domain. We hold that
bridge internally — `companies.company_linkedin_url` (BTREE on `normalized_domain`)
and `firmographics_blitz.linkedin_url` (BTREE on `domain_norm`). The worker recovers
the URL by LEFT JOIN; only misses fall back to the gateway `domain-to-linkedin` hop.

**Plan tier (resolved):** deployed `blitz-gateway` `key-info` → plan **Agency -
Enterprise**, `remaining_credits: unlimited`, 5 RPS, `allowed_apis` includes
`/search/waterfall-icp-keyword`, `/enrichment/domain-to-linkedin`,
`/enrichment/email`, **and** `/enrichment/phone`. Every hop is unlocked. The worker
still preflights `key-info` and degrades a hop to off (never crashes) if a future
key loses access.

---

## 2. Architecture — control/compute split + gateway egress

```
GTM Agent ──(MCP: launch_contact_hydration)──▶ gtm-mcp tool (apps/gtm_mcp/src/tools/hydration.py)
    │  validate (campaign_id, audience_name) ∈ ops.campaign_audiences
    │  POST https://api.trigger.dev/api/v1/tasks/blitz-contact-hydration-waterfall/trigger  (Bearer TRIGGER_SECRET_KEY)
    ▼
Trigger task  "blitz-contact-hydration-waterfall"  (src/trigger/blitz_hydration_waterfall.ts — event task, NO cron)
    │  mint waitpoint token (2h) → POST Universal Dispatcher → suspend on wait.forToken
    ▼
Universal Dispatcher (core/modal_dispatcher.py, unchanged) ── spawn ──▶
Modal worker  "blitz-hydration-waterfall"  (pipelines/gtm/blitz_hydration_waterfall.py — endpoint-less, NO Blitz key)
    │  1. gateway key-info preflight (degrade plan-gated enrichments)
    │  2. psycopg READ ops.campaign_audiences → source_query
    │  3. resolve audience via the canonical gtm-mcp resolver (apps/gtm_mcp/src/database.query):
    │       source_query → DISTINCT normalized_domain  ⋈ companies/firmographics_blitz → company_linkedin_url
    │  4. per company, via blitz-gateway (HIGH lane):
    │       [resolve domain→linkedin if no local URL] → waterfall-icp-keyword(cascade) → per person [email] [phone(US)]
    │  5. land raw contacts → R2 ZSTD Parquet  (s3://dex-raw-landing-zone/blitz_contacts/campaign_id=…/run=…/part-*.parquet)
    │  6. ops.blitz_hydration_runs (psycopg) + RAW flat callback to the waitpoint url
    ▼
core/blitz_gateway.py  "blitz-gateway"  (Directive 23, single-container, owns BLITZAPI_API_KEY + the ≤5-RPS bucket)
```

**Control/compute law honored.** Trigger carries only signals
(`{campaignId, audienceName?, cascade, …}`) — never company lists or contact rows.
The worker owns all data work (audience read, DuckDB resolution, Blitz fan-out via
the gateway, R2 landing, ops state). The dispatcher payload is tiny regardless of
audience size.

**Decoupling honored.** The worker's secrets are `r2-credentials` + `hqx-postgres`
**only** — the `blitz-api` secret is deliberately absent. Egress is 100% the
gateway's concern; the worker calls `modal.Function.from_name("blitz-gateway",
"blitz_call")` with `priority="high"`. The gateway is the sole RPS governor — the
worker implements no rate logic.

---

## 3. The parameterization contract (LLM-populated)

The Trigger payload (and the `launch_contact_hydration` tool args):

```jsonc
{
  "campaign_id":   "summer-2026-fintech-abm",   // REQUIRED — matches ops.campaign_audiences.campaign_id
  "audience_name": "fintech-series-b",           // OPTIONAL — default: the campaign's most-recently-updated audience
  "cascade": [                                    // REQUIRED — the NATIVE Waterfall cascade (≤8 tiers), LLM-generated
    { "include_title": ["CMO", "Chief Marketing Officer"], "exclude_title": ["assistant", "intern"],
      "location": ["US", "GB"], "include_headline_search": false },
    { "include_title": ["VP Marketing", "Head of Marketing"], "location": ["WORLD"], "include_headline_search": true }
  ],
  "max_results_per_company": 5,                   // OPTIONAL 1–25, default 5
  "enrich_email": true,                           // OPTIONAL, default true  (degrades if plan-gated)
  "enrich_phone": false,                          // OPTIONAL, default false (US-only at the API)
  "priority": "high"                              // OPTIONAL gateway lane: high|normal|low, default high
}
```

`cascade` passes **straight through** to Blitz (the worker injects
`company_linkedin_url` per company). The directive's persona vocabulary —
seniority, department, title keywords — is compiled by the LLM into ordered
`include_title` tiers + `exclude_title` + `location`; the agent is fully capable of
this, which is precisely why dual-mode was rejected.

The `audience` itself is defined separately, at save time, via the existing
`save_campaign_audience(campaign_id, audience_name, source_query, parameters)` tool.
**Audience contract:** `source_query` MUST project a `normalized_domain` column (the
anchor). The worker recovers `company_linkedin_url` itself (Lance bridge + gateway
fallback); the query need not expose a LinkedIn column.

---

## 4. Rate, scale, durability

- **The gateway is the hard ≤5-RPS governor** (single container, token bucket,
  priority lanes). The worker fans out serial `blitz_call` invocations at HIGH
  priority; HIGH preempts background bulk-A at the next token, and the gateway
  reserves a LOW floor so bulk is throttled, never starved.
- **Per-company call shape:** `[1 resolve if bridge-miss] + 1 waterfall + ≤max_results×(email + phone(US))`.
  At `max_results=5`, both enrichments: ~11 gateway calls/company.
- **One worker invocation streams the whole audience** (the gateway is the
  bottleneck, so fan-out wouldn't help and is far simpler). Worker `timeout=1h`;
  Trigger token `timeout=2h` (suspended wait is free). **Scale path** for very large
  audiences (≳ a few thousand companies, wall-clock → maxDuration): chunk the
  audience and `batch.trigger` a child run per chunk. Single-shot is v1.
- **Durability:** raw contacts land to R2 Parquet on success; `ops.blitz_hydration_runs`
  records the counts (`companies_in`, `linkedin_local`/`api`/`unresolved`,
  `waterfall_calls`, `contacts_found`, `emails_found`, `phones_found`,
  `gateway_calls`, `email_gated`/`phone_gated`, `landing_uri`, `status`).

---

## 5. Data landing & state (single-mode output)

- **R2 transport landing (raw payloads):** ZSTD Parquet to
  `s3://dex-raw-landing-zone/blitz_contacts/campaign_id=<cid>/run=<run_root>/part-0000.parquet`
  (bucket/prefix overridable). One row per (company, matched person): the Waterfall
  `icp` tier / `ranking` / `what_matched`, the flattened Person, email/phone
  enrichment, `raw_person` JSON for full fidelity, and provenance. Parquet is
  transport only.
- **Terminal state:** `ops.blitz_hydration_runs` (DDL: `pipelines/gtm/ops_blitz_hydration_runs.sql`,
  mirrored verbatim in the worker, applied idempotently before each write).
- **Callback:** flat metadata only — `{status, feed, campaign_id, audience_name,
  companies_in, contacts_found, emails_found, phones_found, gateway_calls,
  landing_uri, error}`. The worker re-raises on failure so the Modal call is marked
  failed; the Trigger run inspects `result.output.status`.

> Promotion of these raw contacts into the **Lance** system-of-record (a
> `campaign_contacts` dataset / `people` augmentation with indexes) is the natural
> **next directive** — explicitly out of scope here (this pipeline ends at "raw
> payloads landed").

---

## 6. Shipped artifacts

| File | Role |
|---|---|
| `pipelines/gtm/blitz_hydration_waterfall.py` | Modal worker `blitz-hydration-waterfall`, fn `hydrate_campaign_waterfall`. No Blitz key; gateway-routed. Reuses the gtm-mcp resolver for the audience. Entrypoints: `init_ops`, `verify_resolver_run` (safe smoke), `verify_run`. |
| `pipelines/gtm/ops_blitz_hydration_runs.sql` | `ops.blitz_hydration_runs` DDL (canonical sibling). |
| `src/trigger/blitz_hydration_waterfall.ts` | Event task `blitz-contact-hydration-waterfall` (mirror `enrichment_blitz.ts`). |
| `apps/gtm_mcp/src/tools/hydration.py` | `launch_contact_hydration` tool + `register`. Fires the Trigger task (stdlib HTTP). |
| `apps/gtm_mcp/main.py` | mounts `hydration.register(mcp)`. |

**One architectural note:** the worker imports `apps/gtm_mcp/src/database` (via Modal
`add_local_python_source("apps")`) to resolve the audience byte-identically to how
the agent authored its `source_query` — the same read-only SQL guard + dataset
registry + hq-x attach. This is the one `apps/`→`pipelines/` import in the fleet,
justified because forking a security-sensitive SQL guard is worse than a read-only
reuse. A future refactor could lift `database.py` into `core/` to remove the
boundary crossing.

---

## 7. Provisioning & deploy

| Item | State | Action |
|---|---|---|
| `blitz-gateway` (egress + `BLITZAPI_API_KEY`) | ✅ deployed (Directive 23) | none |
| Worker secrets `r2-credentials` + `hqx-postgres` | ✅ exist (fleet) | none — worker mounts them |
| `ops.blitz_hydration_runs` | created by | `modal run pipelines/gtm/blitz_hydration_waterfall.py::init_ops` |
| Modal worker | deploy | `modal deploy pipelines/gtm/blitz_hydration_waterfall.py` |
| Trigger task | deploy | `doppler run -- npx trigger.dev@4.4.4 deploy` |
| `TRIGGER_SECRET_KEY` on gtm-mcp (Render) | **NEW — required for the launch tool** | set the Trigger prod `tr_…` key in the gtm-mcp service env, then redeploy gtm-mcp |

Safe post-deploy smoke (no Blitz spend, no DB write):
`modal run pipelines/gtm/blitz_hydration_waterfall.py::verify_resolver_run` — confirms
the reused resolver imports in-image, R2 discovery works, `companies` /
`firmographics_blitz` resolve, and the gateway `key-info` is reachable.

---

## 8. Verification performed

- `python -m py_compile` clean on the worker, the tool, and `main.py`.
- Cross-layer name consistency: Trigger `id` = `blitz-contact-hydration-waterfall`
  = tool `TASK_ID`; dispatcher `app_name`/`function_name` = worker
  `modal.App`/fn name.
- Live `blitz-gateway` `key-info` → plan/RPS/allowed-apis (§1) — confirms every hop
  unlocked and the 5-RPS ceiling.
