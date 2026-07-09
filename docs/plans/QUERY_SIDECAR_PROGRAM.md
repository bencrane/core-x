# Query-Sidecar Program — End-to-End Record (Phases 0–5)

Executed 2026-07-08/09, autonomously, end-to-end. The platform's `/phrase`/market analytical lane now executes against a sorted DuckDB artifact instead of scanning Lance over R2 — **measured in production: 205.4 s → 1.7 s** on the operator's flagship shape.

## What exists now

| Component | Location | State |
|---|---|---|
| Frozen mart manifest (evidence-based) | [SIDECAR_PHASE0_MART_MANIFEST.md](SIDECAR_PHASE0_MART_MANIFEST.md) | Merged #1087 |
| Export builder (Modal app `query-sidecar`) | [pipelines/query_sidecar/build_query_sidecar.py](../../pipelines/query_sidecar/build_query_sidecar.py) | Merged #1088/#1089 · `modal deploy`-ed (dispatcher-spawnable) |
| Artifact (v3-class, Tiers A+B+C+D) | `s3://data-sink/query-sidecar/` + `LATEST.json` | 40 tables · ~708M rows · 23.8 GiB · 40/40 parity · <10 min rebuild |
| Benchmark + Tier C verdicts | [QUERY_SIDECAR_PHASE2_BENCHMARK.md](QUERY_SIDECAR_PHASE2_BENCHMARK.md) | 14/14 parity; every family ≤134 ms warm |
| Serving endpoint (Render, Ohio, standard + 50 GB disk) | `https://query-sidecar-api.onrender.com` · [apps/query_sidecar_api/](../../apps/query_sidecar_api/) | Live; read-only SQL, bearer `QUERY_SIDECAR_TOKEN` (Doppler `core-x/prd`) |
| Sidecar executor in catalyst (the flip) | [apps/catalyst_api/src/sidecar_executor.py](../../apps/catalyst_api/src/sidecar_executor.py) + `market_store` shims | Merged #1092 · **live in production** (`QUERY_SIDECAR_EXECUTE=on`, Doppler) |
| Parity gate (rerunnable) | [scripts/parity_sidecar_phrase.py](../../scripts/parity_sidecar_phrase.py) | 13/13 identical totals (14th refuses at compile on both backends — vocabulary artifact) |
| Refresh loop | builder `_notify_refresh()` → `POST /api/v1/refresh` | Live; Modal secret `query-sidecar` carries the bearer |
| Scheduled rebuild (Trigger v4) | [src/trigger/query_sidecar_rebuild.ts](../../src/trigger/query_sidecar_rebuild.ts) | **Code shipped; deploy blocked** — see Open items |

## Measured results (live production, end-to-end through catalyst)

| Phrase | Before (Lance lane) | After (sidecar lane) |
|---|--:|--:|
| companies with awards expiring within 90 days | 205.4 s | **1.7 s** |
| awards over $5m expiring within 365 days | 31.3 s | **0.5 s** |
| companies over $10m that primed in 236220 | 5.0 s | **1.9 s** |
| two-lane flagship (expiring-180d ∩ code-G-mod-90d) | ~70 s-class | **6.6 s** |

Same-container ceilings (Phase 2, warm NVMe): 4.7–134 ms per family. Production adds catalyst→Render round-trips per plan leg and Render network-SSD cold pages; latencies improve as the disk cache warms. Levers if p99 ever matters: larger Render plan / `DUCKDB_MEMORY_LIMIT`, or co-locating the artifact on instance NVMe.

## Execution semantics (what changed, what didn't)

- **Unchanged:** the phrase grammar/compiler, all consumer repos, the 5 legacy map decoders (edge_api map-ask), gtm_mcp, the Lance SoR and every pipeline. Validation semantics identical (`MapCompileError` re-raises).
- **Changed:** `market_store`'s three executors try the sidecar first (flag-gated). Tiering: entity grain, prime_awards rows+collapse, transactions **collapse** → sidecar; transactions **row** queries stay on Lance (`NotServable`) because `gtm_txn_events_slim` lacks the 16-col row projection.
- **Better than before:** sidecar collapse aggregation is exact — never 500k scan-capped (`scan_capped:false` always).
- **Fallback proven:** any sidecar failure (incl. hydrating 503) logs and falls through to the Lance path — demonstrated live with a bogus URL returning the correct total via Lance.

## Runbook

| Action | Command |
|---|---|
| Rebuild + publish + serving refresh | `modal run pipelines/query_sidecar/build_query_sidecar.py::run` |
| Smoke build (Tier A → smoke/, no pointer) | `…::smoke` |
| Hot-swap serving to LATEST (no redeploy) | `POST https://query-sidecar-api.onrender.com/api/v1/refresh` (bearer) |
| Kill switch (instant, no deploy needed at next boot; live within one catalyst redeploy) | `doppler secrets set -p core-x -c prd QUERY_SIDECAR_EXECUTE=off` + redeploy catalyst |
| Parity re-check | `doppler run -p core-x -c prd -- env QUERY_SIDECAR_URL=… PYTHONPATH=. python3 scripts/parity_sidecar_phrase.py` |
| Ledger | `ops.query_sidecar_runs` (one row per build, success and failure) |

## Open items (operator decisions)

1. **Trigger schedule limit:** the account is at **26/10 schedules**; deploying `query-sidecar-rebuild` (daily 11:00 UTC) aborts at the final step. Raise the plan limit or prune dead schedules (candidates per the ops ledger: `cal_booking`, `close_push`, `fmcsa_pdl_domain_spine`, `icypeas_mv`, `map_query` — all stale), then any routine `npx trigger.dev@4.4.6 deploy` registers it. Until then the rebuild is operator-initiated (runbook above).
2. **Legacy-consumer migration** (out of scope by directive): the excluded superseded marts (`contractor_award_summary`, `capability_profile`, `entity_profile_gold`, 5 map decoders) still serve their old routes; successors are named in the Phase 0 manifest. `/healthz` of catalyst still anchors on a stale mart.
3. **Tier C leftover:** `gtm_subaward_recipient_code_evidence` (92M) stays out of the artifact until a workload touches it.

## PR trail

#1087 (Phase 0 manifest) → #1088 (Phase 1 builder + artifact) → #1089 (Phase 2 benchmark + Tier C promotion) → #1090 (Phase 3 service) → #1091 (background hydration fix) → #1092 (Phase 4 executor + flip) → this doc + Trigger task + refresh hook (Phase 5).
