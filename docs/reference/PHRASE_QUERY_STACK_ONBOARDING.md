# Phrase / Query Stack — Agent Onboarding (platform-app ⇄ DuckDB/Lance)

One page, top-down: how an analytical question typed in the platform-app becomes SQL
against the query-sidecar artifact, and how that artifact relates to the Lance system
of record. Everything referenced lives on `core-x` main. Current: compiler `phrase.v5`,
artifact 71 tables / ~1.23B rows.

## 0. The stack in one diagram

```
platform-app (BFF)
   │  POST $CATALYST_API_URL/api/v1/market/phrase   {"phrase": "..."}   bearer CATALYST_API_TOKEN
   ▼
catalyst-api (Railway service, auto-deploys from core-x main)          apps/catalyst_api/
   │  phrase_compiler.py: lex → bind (closed vocabulary) → grain-resolve → emit plan
   │  execute_plan: collapse lane(s) → UEI-set intersect → entity hydration
   ▼
market_store executors — SIDECAR-FIRST, Lance fallback                  sidecar_executor.py
   │  POST https://query-sidecar-api.onrender.com/api/v1/sql            bearer QUERY_SIDECAR_TOKEN
   ▼
query-sidecar-api (Render, Ohio, 200 GB disk)                           apps/query_sidecar_api/
   │  ONE read-only DuckDB file, physically sorted tables, per-request cursors,
   │  LATEST-poll convergence, require_artifact pin (409 on mismatch)
   ▼
query_sidecar_<stamp>.duckdb  ←  blue-green publish  ←  Modal builder   pipelines/query_sidecar/
   ▲                                                    build_query_sidecar.py (MANIFEST = truth)
   │  parity-gated export (exact row counts vs pinned Lance versions)
Lance SoR  s3://data-sink/active/  (write-side system of record; 600+ datasets)
```

Two doctrines make this safe to build on:
- **Deterministic compiler**: closed grammar, closed vocabularies, zero LLM. Every token
  binds or the phrase refuses (422) naming the token. Same phrase + vocabulary + `today`
  → same plan, always. Vocabulary changes are reviewed code PRs (COMPILER_VERSION bumps,
  pinned tests).
- **Snapshot artifact**: the sidecar is a versioned, immutable, read-only copy. A failed
  build publishes NOTHING (parity gate → pointer swap ordering); serving hot-swaps
  blue-green with zero downtime. Freshness = the artifact stamp (`/healthz`), not live.

## 1. Request lifecycle — one worked example

Phrase: `construction companies that just received new funding > $500k`

1. **Bind**: `companies`→entity subject · `construction`→frozen NAICS-23 set ·
   `just`→90-day window · `new funding`→action_type C · `> $500k`→money ≥ 500,000.
2. **Emit** (2-step pipeline): step 1 = transaction-grain collapse
   (`filters: naics in [...], action_type = C, action_date <= 90d`,
   `amt_thresholds: [{op: ">=", value: 500000}]`) → step 2 = entity hydration
   (`uei in <collapse survivors>`).
3. **Execute**: the collapse runs on the sidecar (`gtm_txn_events_slim`, uei-sorted),
   returns per-firm `amt_total`/`match_ct`; the threshold filters the Σ$; entity rows
   hydrate from `gtm_sam_entities`/rollup.
4. **Respond**: hydrated companies, **ordered by fresh money**, each row carrying
   `event_amt_total` + `event_match_ct`; `meta.bindings` disclosed token-by-token;
   `meta.compilerVersion: phrase.v5`.

## 2. Semantics that trip people up (learn these before writing phrases)

| Clause | Means | Not |
|---|---|---|
| `over $5m` / `> $5m` **with an event** | per-firm Σ$ of the matched events in the window | per-action floor; firm size |
| `$` on `actions ...` subject | per-action floor (`federal_action_obligation`) | firm sum |
| `lifetime over $5m` | explicit firm-size floor (`prime_obl_lifetime`) | anything windowed |
| bare `$` on companies, no event/window | **refuses**, names both fixes | silent lifetime default (dead since v5) |
| `construction` / sector words | NAICS of the **award actions** (event lane) or demonstrated lanes (`primed in`) | SAM registration `primary_naics` — unreachable from phrases |
| `in VA` | firm **HQ state** (entity step) | place of performance (SQL lane: `pop_state` on `txn_events_combo`) |
| `navy`, `army`, `usace` | **refuse** (sub-tier; phrase reaches toptier only: `dod`, `gsa`…) | the SQL lane serves sub/funding agency fine |
| `to repair bridges` / `build runways` | PSC in-list from the generated work-language vocabulary (2,562 noun aliases; PSC literals equally valid) | fuzzy matching |
| `or` | merges values on the SAME axis only | cross-axis OR / NOT (refuse) |

Grain tiering: entity, prime_award (awards/orders/vehicles), transaction — all families
execute on the sidecar; any sidecar failure falls through to the Lance path (logged).

## 3. The serving contract (for anything that queries directly)

```bash
TOKEN=$(doppler secrets get QUERY_SIDECAR_TOKEN -p core-x -c prd --plain)
curl -s -X POST https://query-sidecar-api.onrender.com/api/v1/sql \
  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"sql": "SELECT ...", "limit": 1000, "require_artifact": "<stamp-to-pin>"}'
```
ONE statement (SELECT/WITH/DESCRIBE/SHOW), 120 s timeout, responses carry
`elapsed_ms, artifact, instance`. Multi-statement analyses that must reconcile to the
dollar pin `require_artifact` and treat 409 as "snapshot moved — re-run the batch".
Box realities: 2 threads / 1.5 GB DuckDB — filter on sort keys; never put an 83M-row
table on the build side of a join (use `gtm_position_orders`, pre-prune to UEI sets).

## 4. Reading order (deep dives)

| # | Read | Gets you |
|---|---|---|
| 1 | [QUERY_SIDECAR_AGENT_GUIDE.md](QUERY_SIDECAR_AGENT_GUIDE.md) | THE map: 71-table catalog, patterns, sidecar-vs-Lance rule, §7 gap reporting |
| 2 | [../plans/QUERY_SIDECAR_PROGRAM.md](../plans/QUERY_SIDECAR_PROGRAM.md) | system record, phases, runbook, measured results |
| 3 | [../../apps/catalyst_api/src/phrase_compiler.py](../../apps/catalyst_api/src/phrase_compiler.py) docstring + [tests](../../apps/catalyst_api/tests/test_phrase_compiler.py) | the grammar spec + 57 pinned examples |
| 4 | [../../apps/catalyst_api/src/sidecar_executor.py](../../apps/catalyst_api/src/sidecar_executor.py) | execution tiering, NotServable fallback |
| 5 | [../../apps/catalyst_api/README.md](../../apps/catalyst_api/README.md) | the Railway service + how platform-app consumes it |
| 6 | [../../pipelines/query_sidecar/build_query_sidecar.py](../../pipelines/query_sidecar/build_query_sidecar.py) | MANIFEST + build/promotion doctrine |
| 7 | [../sidecar_gaps/README.md](../sidecar_gaps/README.md) + processed/ | the demand loop + worked dispositions |

Agent-runtime equivalents: `sidecar-query` skill (query routing), `sidecar-gaps` skill
(compile reports / run the build cycle). Build-lane role directive:
`~/Desktop/hq/directives/2026-07-09-query-sidecar-build-agent.md`.

## 5. The demand loop (how the artifact grows — applies to YOU)

Answer a data question WITHOUT the sidecar (Lance scan, degraded answer)? Write a gap
entry (guide §7 schema) to `docs/sidecar_gaps/SIDECAR_GAP_REPORT_<date>-<slug>.md` — a
NEW dated file, never appended to an archived one. The build cycle gates every entry
(promote / routing fix / correctly-on-Lance), runs a mandatory adjacency sweep (one
build ships the complete thought — structural growth is demand-gated, column adds on a
paid-for build ride free), rebuilds parity-gated, and archives with a Disposition.
Silent fallbacks are lost demand.

## 6. Operational quick reference

| Thing | Where |
|---|---|
| Rebuild artifact (median ~32 min, observed 22–42, zero-downtime) | `/sidecar-build` skill — `modal deploy` then `modal.Function.from_name("query-sidecar","build").spawn(...)`; NEVER `modal run …::run` (client-tethered) |
| Serving health / current stamp | `GET https://query-sidecar-api.onrender.com/healthz` (no auth) |
| Build ledger | `ops.query_sidecar_runs` (HQX Postgres) |
| Phrase endpoint auth | Doppler `core-x/prd`: `CATALYST_API_BASE_URL`, `CATALYST_API_TOKEN` |
| Sidecar SQL auth | Doppler `core-x/prd`: `QUERY_SIDECAR_TOKEN` |
| Kill switch (executor flip back to Lance) | `QUERY_SIDECAR_EXECUTE=off` (Doppler) + catalyst redeploy |
| gtm-mcp fast lane | `sidecar_sql` / `sidecar_tables` tools |

Compiler version history, one line each: v2 active/expiring/two-lane · v3 work-language
(`to <verb> <noun>` → PSC) · v4 event-money = per-firm Σ$ + hydrated rows carry
`event_amt_total`, Σ$-ordered · v5 comparator symbols, `new funding`, explicit
`lifetime $` (bare no-window money refuses).
