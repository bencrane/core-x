# gtm-mcp Recall Architecture — Adversarial Diagnostic

> Produced by an 8-agent adversarial workflow (4 evidence lenses → 3 adversarial critics → synthesis),
> grounded in live R2 measurement and the `ops.*_runs` ledgers. The key adversarial finding — the
> month-rollover staleness via `_latest_snapshot` pins — was verified against the code
> (`_uri`→`_latest_snapshot` caches `feed→snap` in `_snapshot_cache` and reads `database.get_registry()`
> with no refresh; `_ds_cache` keyed on the static URI). Remediation landed in the same PR as this doc.

## 1. Verdict

The per-call-fresh-open contract is a **genuine architectural flaw**, and it is **gateway-wide, not
provider_360-scoped**. `database.py` conflates the durability-first system of record (Lance on R2,
fresh-on-write) with the agent-facing serving tier: its freshness contract (`database.py:55-59`) opens
every dataset fresh per tool call, which reloads the Lance scalar index from R2 on every request.
Measurement shows the index-load is **~78% of a cold point lookup** and makes a single-row BTREE lookup
**~7-20x slower** than the same lookup on a warm-resident handle (cold 1.5-3.5s vs warm 0.14-0.29s WAN),
while the load-bearing datasets mutate **monthly** (ledger: each published exactly once) — a continuous
per-request tax against a once-a-month event. The (now-reverted) `provider360.py` `_versions`-fingerprint
cache was the right *direction* for point lookups but was **mis-scoped (provider360-local), over-engineered
(a per-call S3 LIST on every hit), and carried a fatal month-rollover staleness bug** — it was replaced,
not shipped. The honest fix is a process-resident handle cache lifted into `database.py` with coarse TTL
invalidation; it lands point lookups in the interactive budget but does **nothing** for the cohort-scan
data-fetch, which is a separate problem.

## 2. The flaw, quantified

**Latency decomposition** (provider_360 BTREE(npi) point lookup; laptop→R2 WAN, upper bound — Render is
co-located so absolutes compress, relative structure holds):

| Component | Cost | % of cold lookup | Cacheable by warm handle? |
|---|---|---|---|
| Manifest open (single GET) | 372-441 ms (~418 ms) | ~14% | No (cheap, paid once per open) |
| **Scalar-index load from R2** | **~1.2-2.8 s** | **~78%** | **Yes — this is the entire flaw** |
| Data fetch + scan | ~150-250 ms | ~8% | No (irreducible) |
| **Cold open + lookup TOTAL** | **1.5-3.5 s** (med ~1.78-3.06s) | 100% | — |
| Warm same-handle lookup | 140-290 ms | — | (index already resident) |

**Cold vs warm, measured (WAN):**

| Path | Latency |
|---|---|
| Fresh-per-call point lookup (provider_360) | 1706 / 2270 / 4099 / 1939 ms |
| Warm-handle point lookup (index resident) | 206 / 245 / 206 / 209 / 228 ms |
| Fresh handle opened *after* a prior handle primed the same URI | still 1521 ms first scan |
| companies (audience.py, **uncached**) cold / warm | 830-933 / 90-95 ms |
| awards (audience.py, **uncached**) cold / warm | 1050-2402 / 73-106 ms |
| Server-side warm (live Render) get_provider_360_profile / roster | 1482 / 981 ms |

**Load-bearing fact:** pylance does **not** share the resident scalar index across `LanceDataset`
handles. A brand-new `lance.dataset()` to the identical URI, opened immediately after a prior handle
primed it in-process, still pays the full ~1521 ms index-load on its first scan. So per-call-fresh-open
genuinely re-reads the BTREE from R2 every call, and **only object reuse** avoids it. This also refutes the
misattribution hypotheses: the point-lookup path never touches DuckDB; the hqx Postgres ATTACH (740 ms)
and httpfs install (32 ms) are lazy singletons warmed once at boot (`main.py:148`), per-call cursor cost
0.01 ms.

**Actual mutation cadence (ops ledgers, Postgres):**

| Dataset | Successful publishes (all history) | Cadence | Write-burst |
|---|---|---|---|
| provider_360 (9,551,447 rows) | **1** (snapshot 2026-06) | snapshot-partitioned monthly | 14 `_versions/` manifests in one 15-sec burst, byte-static since |
| practice_group_360 (253,740 rows) | **1** (snapshot 2026-06) | snapshot-partitioned monthly | 5 manifests, 4-sec burst |
| companies / people / awards (flat, in-place) | 4 / (enrichment 87 runs/4d) / 7-16 versions | **days** | spans multiple days |

**Cost/benefit mismatch:** the contract validates freshness on potentially thousands of tool calls
between rebuilds to guard against a mutation that fires monthly for the entity-360 datasets and
every-few-days for the flat datasets. The docstring's stated performance (`database.py:58-59`,
"point-lookup path stays sub-100 ms") is **refuted** on the path it describes by 15-35x. **Nuance:** the
"overwrite in place" rationale is *literally true* for companies/people/awards (8-16 versions across
days) — the freshness need is real there, just over-served by a per-call check. It is *false-by-overkill*
for the snapshot-partitioned entity-360 datasets.

## 3. The real use case

The gateway is consumed by **one interactive Anthropic Managed Agent** (gtm-agent, wired as the `polaris`
MCP server via `config.py:198-209` + `services/managed_agents.py` → SSE `/v1/sessions`). The access shape
is **interactive / sequential / exploratory**: a reason→tool→observe→reason loop emitting many small tool
calls per task, not batch.

The tools are **explicitly designed to chain** (`provider360.py`; `README.md`): a `find_*` cohort call
returns up to `_RESULT_CAP=500` NPIs/groups, then the agent point-looks-up `get_provider_360_profile` /
`get_practice_group_roster` on selected results. This is exactly the loop where per-call latency
**compounds linearly**:

- At measured 1.5-4.1 s/point-lookup + 2.3-4.2 s/cohort scan, a single 8-12 call investigative task
  spends **~15-40 s purely in gateway I/O**, on top of the agent's own per-turn think time.
- Internal corex fan-out is *already* batched (`corex.py` `_resolve_contacts` collapses companies→contacts
  into one `IN(...)` query), so the multiplication risk lives in the **per-call open cost**, not loop fan-out.

**Latency budget an interactive agent tool surface should hold:** point lookups **sub-100-200 ms/call**
(warm-handle reuse achieves it — ~205 ms WAN, low tens of ms co-located). The directive's <20 ms target is
reachable for point lookups **only co-located + warm**, and is **not reachable for cohort scans by any
handle cache** (the R2 columnar data-fetch is irreducible).

## 4. Critique of the current state

### (a) The existing `database.py` freshness contract — REPLACE the policy

The "open fresh per call" policy (`open_dataset()`, `_register_datasets()`) is the root cause. Wrong on
three counts:

1. **It pays the dominant cost on every call.** The index-load it forces is ~78% of point-lookup wall
   time and is pure waste — the BTREE did not change.
2. **Its stated mechanism would not even work via Lance's version integer.** `_publish_full_swap`
   (`materialize.py` `_del_prefix` then file-by-file recopy) rewrites the identical deterministic commit
   sequence (1 `create` + N `create_scalar_index`) into the same prefix, so provider_360 re-lands on
   **version 13** every rebuild. A naive `ds.version` check reads 13 before and after and **silently misses
   the rebuild**. Content-fingerprinting via `_versions/` ETags is the only safe republish signal — and R2
   ETag == md5(manifest content) was verified for all 14 single-part manifests.
3. **It is uniform where cadence is not.** A per-call guard applied identically to monthly snapshots and
   days-cadence flat datasets is a configuration error masquerading as a safety contract.

### (b) The reverted `_versions`-fingerprint cache — REPLACED (did not ship)

Verdict: **correct on result-correctness and concurrency, fatally flawed on the freshness contract it
claimed, and mis-scoped.** Failure modes the critics found:

- **FATAL — month-rollover permanent staleness.** When `snapshot=2026-07` publishes at a *new prefix*,
  three process-lifetime pins keep the running gateway serving 2026-06 **indefinitely until restart**:
  (a) `_snapshot_cache` caches `feed→'2026-06'`, never re-listed; (b) `_latest_snapshot` reads
  `get_registry()` with **no refresh** (the singleton refreshes only on `refresh=True`); (c) `_ds_cache`
  keys on the byte-static **old-month** URI, whose fingerprint the new publish never touches → eternal
  cache HIT. `refresh_catalog` cleared neither cache. The "degrade toward freshness, never stale" promise
  was **false** — it degraded toward stale, permanently, at exactly the monthly mutation cadence.
- **Over-engineered — per-call S3 LIST floor.** `_versions_fingerprint` ran on **every** call before the
  cache check (70-157 ms WAN), ~doubling the warm floor. A network round-trip on every request to detect a
  once-a-month event.
- **LIST-failure latency cliff.** A bare `except` returned `None` on any R2 throttle/5xx, silently
  reverting that call to a fresh uncached ~1.5-3 s open with no metric.
- **Cold-start thundering herd.** `lance.dataset()` opened outside the lock; concurrent first-callers each
  paid a redundant ~2 s cold open. (Self-correcting, cost-only.)
- **Scope gap.** Covered only `provider360._open`; `audience.py` and `execute_audience_query` still paid
  full freight.

**What survived (kept as design ideas):** the same-snapshot ETag defense is robust (Lance regenerates
random fragment/manifest UUIDs every rebuild → new ETags → correct miss); 90 concurrent scanners off one
shared handle returned **0 errors, 0 wrong rows** (handle reuse is concurrency-safe); the cache was
memory-bounded. The *resident-handle* mechanism is sound — the *per-call-LIST invalidation* and the
*snapshot-resolution pinning* are what changed.

## 5. Ranked remediations

Organizing principle: **separate the system of record from the serving tier.** The SoR stays Lance/R2
(durability-first, fresh-on-write). The serving tier is an in-process warm mirror with invalidation matched
to the *actual* mutation cadence — not a per-request re-validation of an immutable artifact.

| # | Fix | Mechanism | Expected latency | Freshness | Render/scaling | Complexity | Blast radius |
|---|---|---|---|---|---|---|---|
| **1** | **Process-resident handle cache in `database.py` + coarse TTL** | module `{uri→(deadline, ds)}` in `open_dataset()`/`_register_datasets()`; index stays resident | point → **tens of ms co-located**; cohort → true scan time | TTL-bounded (≤30min) | fits 2 GB (index residency ~174 MB) | **Low** | isolated to open path; **generalizes gateway-wide** |
| **2** | TTL'd registry + drop snapshot pins | `get_registry` self-refreshes; `_latest_snapshot` re-resolves | same as #1 | picks up new month within TTL | same | Low | **fixes the rollover bug — prerequisite for #1 correctness** |
| 3 | Hardened fingerprint cache | ETag check at most once per TTL, not per call | tens of ms | per-TTL fresh | same | Medium | provider360-local unless lifted |
| 4 | Precomputed cohort leaderboards | materialize top-N per (specialty×state×rank_by) at publish | cohorts → **<100 ms, path to <20 ms** | refreshed at publish | small derived tables | High | **only** path to fast cohorts — DEFER |
| 5 | Local-NVMe / in-RAM materialization | copy 4.8 GB to instance | sub-ms | re-copy on publish | **INFEASIBLE** at 2 GB / no persistent disk | High | forfeits stateless scaling — **REJECT** |
| 6 | Postgres serving mirror | monthly COPY 9.5M rows into indexed PG | sub-100 ms points | refresh on publish | duplicates SoR | High | redundant with #1 — DEFER |

**DO-NOW: #1 + #2 + the cohort-scan concurrency cap.** **LATER: #4** (only if warm cohort latency proves
insufficient). **REJECT: #5.** **DEFER: #6.**

### Implementation (shipped with this doc)

- **`database.py`**: `_cached_dataset(uri)` — URI-keyed, TTL-gated (`GTM_HANDLE_TTL_S`, default 1800s) handle
  cache; `open_dataset()` and `_register_datasets()` route through it, so the win is **gateway-wide** (every
  tool: audience point lookups, `execute_audience_query`, ops, provider360). On a cache miss the dataset's
  `_versions/` prefix is listed **twice and required equal** before the handle is trusted — a guard against
  the non-atomic `_publish_full_swap` delete-then-recopy window (a mid-swap partial is served fresh,
  uncached, never adopted). `get_registry()` gains a TTL self-refresh (`GTM_REGISTRY_TTL_S`) so a new
  `snapshot=YYYY-MM` prefix is discovered without a restart.
- **`provider360.py`**: the `_snapshot_cache` permanent pin is dropped (re-resolves from the TTL'd
  registry), `_open()` routes through `database.open_dataset_uri()`, and a `threading.Semaphore`
  (`GTM_MAX_CONCURRENT_COHORT_SCANS`, default 2) caps concurrent cohort `to_table()` materializations — the
  OOM guard (measured: 6 concurrent broad cohorts → 2333 MB, past the 2048 MB Render Standard limit). Point
  lookups are NOT capped, so they stay responsive under cohort load.

**Why TTL over the per-call fingerprint LIST:** the monthly/days cadence tolerates a ≤30-min staleness
window trivially; the per-call LIST is a permanent network dependency on the hot path defending against an
event that fires monthly. TTL gets ~95% of the win at ~5% of the complexity and removes the LIST-failure
latency cliff. The ETag stability check is kept **only** as the miss-time swap guard, not as a per-request
gate.

## 6. Recommended decision (shipped)

Ship **#1 + #2 + cohort cap**: a process-resident, TTL-invalidated handle cache lifted into `database.py`,
with snapshot re-resolution via a TTL'd registry, a stable-LIST guard against the mid-swap window, and a
concurrency cap on cohort scans. Replace — do not ship — the provider360-local `_versions` shim. This is the
only path that (a) eliminates the dominant index-load tax for the **whole gateway**, (b) actually picks up a
new monthly snapshot (the shim did not — fatal bug), (c) matches the measured days-to-monthly cadence, and
(d) fits the 2 GB single-instance box. Cohort scans (the worst latencies) are explicitly **out of scope** —
they need precomputed leaderboards (#4), deferred until the warm tier proves insufficient, because no handle
cache can touch the irreducible R2 columnar data-fetch.

## 7. Open risks / what to watch

- **Co-located absolutes unmeasured.** All numbers are laptop→R2 WAN (upper bound). The relative
  decomposition holds regardless of distance, but whether warm point lookups clear <20 ms vs land at low
  tens of ms co-located must be confirmed from inside Render.
- **Multi-instance future.** `stateless_http=True` preserves scaling to >1 instance. A process-local cache
  is per-instance (each cold-starts its own handles, each deploy re-cold-starts). Correct (each instance
  independently invalidates) but warm benefit is not shared. Acceptable for a read-mostly reference plane.
- **Memory OOM under concurrent cohorts.** 6 concurrent broad cohorts → 2333 MB (laptop-measured); the
  cohort cap is non-optional. The exact co-located threshold needs a load test; cohort scan memory appeared
  **~386-500 MB unreclaimed** per scan (RSS high-water) — monitor steady-state RSS in production.
- **Non-atomic publish swap.** Until `_publish_full_swap` writes-new-prefix-then-flips-pointer, every
  invalidation scheme inherits the delete-then-recopy window; the stable-LIST guard is the interim
  mitigation, not a fix.
- **Per-dataset TTL.** Flat GTM datasets mutate every few days, entity-360 monthly. A single global TTL is a
  compromise; a per-dataset TTL (short for in-place feeds, long for snapshots) is more correct and is the
  one real config-surface follow-up.
- **`people` ↔ `enrichment_blitz` mapping unconfirmed.** `enrichment_blitz` shows 87 runs/4 days; if the
  gateway's `people` registry entry maps to that feed, people lookups mutate far more often than monthly and
  warrant a shorter TTL — confirm the R2 URI behind the `people` entry before tuning.

## 8. Ledger-poll exact invalidation (deferred follow-up)

The TTL bounds staleness to ≤30 min. An *exact* invalidation reads `MAX(recorded_at)` from
`ops.provider_360_runs` / sibling `*_runs` (one indexed Postgres read per TTL window via the already-attached
`hqx` connection); on a bump, evict the affected URIs and `get_registry(refresh=True)`. This is the only
invalidation correct across both the version-integer reset and the month boundary, at the cost of one cheap
ledger read per window rather than one S3 LIST per request. Wire it once `_publish_full_swap` is atomic
(write-new-prefix → flip-pointer), which removes the swap-window race entirely and is the precondition for
any push-based invalidation.
