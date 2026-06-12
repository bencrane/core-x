# GovCon Market Map — Canonical End-to-End Handoff

**Status date:** 2026-06-11 · **Author:** verification pass against live disk + deployed services + R2 datasets.
**Audience:** a capable AI agent with zero prior context. Read this top-to-bottom and you can operate, debug, and extend the whole system.

> **Verification posture.** Every quantitative claim below was re-checked against the live R2 Lance sink, the deployed endpoints, and the source files on disk on 2026-06-11. Confirmed numbers are marked **[V]**. Where the original briefing was wrong, the correction is marked **[CORRECTED]**. Assumptions and unverifiable items are marked **[ASSUMED]**. Do not trust prose — trust the dataset and the file.

> **Reconciliation (2026-06-11 · adversarial re-verify).** An independent adversarial pass re-probed every `[V]` claim against live disk, deployed services, R2, and git. All quantitative and live-state claims held exactly (incl. live `/ask` → 7126 features, zero drift). **Two risks below were already CLOSED by PRs that landed immediately after this handoff (#423): R-01 by [#425](https://github.com/bencrane/core-x/pull/425) (`22f1510` — `addr_hash_sql` single-sourced) and R-02 by [#424](https://github.com/bencrane/core-x/pull/424) (`850c17f` — edge↔catalyst enum+type parity tests).** Those are updated inline below: §7-8 is retired, §7-2's "fix R-02 first" precondition is satisfied, and the edge test count is corrected 6→8. Newly-closed items are tagged **[RESOLVED]**.

---

## 1. TL;DR / Status

A **natural-language market map**: a public cockpit search box compiles a plain-English sentence into a constrained filter (one forced-tool LLM call), executes that filter as a Lance scanner predicate over pre-geocoded serving tables, and returns companies/winners as GeoJSON for the cockpit to render.

### Request flow (text diagram)

```
 Cockpit search box  (platform-app /demo, MapQuery.nl)
        │  runQuery → runAsk(nl, dataset)  →  askMap()
        ▼
 BFF  platform-api   GET /api/v1/federal/ask?q=&dataset=        [rare-structure-hq]
        │  lib/edge.ts askMarket() — service-token proxy, flattens GeoJSON→rows(+lat/lon)
        ▼
 edge_api  POST /api/v1/map/{dataset}/ask   (TRANSLATE)         [core-x, api.edgeapi.run]
        │  1 forced-tool Anthropic Messages call (emit_filter) → {title, filters}
        │  memo on (normalized_q, decoder_version, model) — caches the FILTER, not GeoJSON
        ▼
 catalyst_api  POST /api/v1/map/{dataset}/query   (EXECUTE)     [core-x, api.catalystdev.run]
        │  compile_map_filter() → Lance scanner predicate (no DuckDB, no LLM, no SQL engine)
        │  appends "latitude IS NOT NULL"; 20k row cap
        ▼
 Lance serving table (R2)  →  GeoJSON FeatureCollection  →  back up the chain
```

**Canned ⌘K commands take a DIFFERENT path** (the warm in-memory federal snapshot in the BFF). Only the **free-text box** uses the live `/ask` path above. See §5.

### One-line status per layer

| Layer | Status | Note |
|---|---|---|
| `geocode_xwalk` (address→coords) | **DONE [V]** | 224,836 rows, all geocoded |
| `usaspending_winners_map_serving` | **DONE [V]** | 40,191 rows (38,435 prime / 1,756 sub), 35,306 coords |
| `firmographics_company_map_serving` | **DONE [V]** | 243,842 rows, 213,949 coords, 139,918 federal |
| catalyst_api EXECUTE | **DONE [V]** | deployed, 401 without token, 19 tests |
| edge_api TRANSLATE | **DONE [V]** | deployed, live `/ask` returns correct filter+7126 features |
| BFF `/ask` proxy | **DONE [V]** | `federal.ts` `/ask` → `askMarket` |
| Cockpit free-text wiring + Table view | **DONE [V]** | `runAsk` reachable via `runQuery` |
| **Geo-dot rendering (map view)** | **PENDING** | live entities have no `x`/`y`; lat/lon dropped before `Company` (§6 R-07) |
| Boot **schema/index** contract check | **PARTIAL** | reachability done; column/index assertion NOT done (§7-5) |
| Public `/ask` rate-limit / auth | **PENDING** | unauthenticated, one LLM call per hit (§6 R-04) |
| `winners` dataset exposed in box | **PARTIAL** | plumbed end-to-end; UI defaults to `company`, no winners affordance (§7-7) |

---

## 2. Architecture & Locked Decisions

**Layers:** (1) data plane — Modal/manual ingest → DuckDB compute → Lance on R2; (2) serving tables — denormalized, pre-geocoded read models with scalar indices; (3) EXECUTE tier (`catalyst_api`) — deterministic filter→Lance→GeoJSON; (4) TRANSLATE tier (`edge_api`) — the single LLM touchpoint; (5) BFF (`platform-api`) — thin service-token proxy; (6) cockpit (`platform-app`).

**Locked decisions (do not relitigate without cause):**

- **No DuckDB in the read path.** DuckDB is a *build-time* compute engine (it materializes the serving tables). At read time, catalyst_api uses **only the Lance scanner** with native BITMAP/BTREE index pushdown. `apps/catalyst_api/requirements.txt` has **no duckdb dependency** [V] — `fastapi`, `pylance`, `pyarrow` only. This keeps the read path sub-second and dependency-light.
- **catalyst = EXECUTE, edge = TRANSLATE (split).** The LLM (untrusted, can hallucinate) is isolated in edge_api and can only emit a constrained `{title, filters}` object. catalyst_api is the **security boundary**: its decoder allowlist (`map_decoders.py`) is the authoritative gate — column names come ONLY from `FieldSpec.column`, never the caller; values are type-validated and SQL-escaped. Even a hallucinated field is rejected at EXECUTE. This separation means the public LLM surface can never reach Lance with an arbitrary predicate.
- **`gtm_mcp` / gtm-agent are OUT of scope.** They are the operator console (a different, authenticated surface). The map `/ask` path never touches them — confirmed in `map_ask_v1.py`, `catalyst_client.py` (explicit "never touches gtm_mcp" comments) [V]. Do not wire the map through the agent loop.
- **Address-keyed, accretive crosswalk.** `geocode_xwalk` is keyed by `addr_hash = md5(normalized street|city|state|zip5)`, not by entity/vintage. Geocode each distinct address **once**, read it from any surface forever. Writes are `merge_insert(addr_hash).when_not_matched_insert_all()` — a known address is never re-geocoded [V, geocode_xwalk.py:306-307]. Rationale: addresses are stable and shared across awards/SAM/SoS; entity-keyed geocoding would re-pay the Census cost on every vintage.

---

## 3. Data Plane

All datasets live under `s3://data-sink/active/` (Lance, written directly to R2 — no catalog). Row counts below are **live-verified 2026-06-11 [V]**.

### Serving / crosswalk datasets

| Dataset (URI) | Grain | Rows [V] | Coords [V] | Indices | Builder | Ledger |
|---|---|---|---|---|---|---|
| `geocode_xwalk/` | 1/`addr_hash` | **224,836** | 224,836 (100%) | BTREE `addr_hash` | `pipelines/usaspending/geocode_xwalk.py` | `ops.geocode_xwalk_runs` |
| `usaspending_winners_map_serving/` | 1/(`winner_uei`,`winner_type`) | **40,191** (38,435 prime + 1,756 sub) | 35,306 (87.8%) | BTREE `winner_uei`,`addr_hash` · BITMAP `naics2`,`state`,`winner_type` | `pipelines/serving/materialize_winners_map.py` | `ops.winners_map_serving_runs` |
| `firmographics_company_map_serving/` | 1/`uei` | **243,842** (139,918 federal) | 213,949 (87.7%) | BTREE `uei`,`addr_hash`,`domain_norm`,`primary_naics` · BITMAP `naics2`,`industry`,`employee_size_band`,`company_type`,`physical_address_state`,`has_federal_awards` | `pipelines/serving/materialize_company_map.py` | `ops.company_map_serving_runs` |

> **Note:** `geocode_xwalk` stores **only successfully-geocoded** rows (skip-on-fail, no centroid fallback — geocode_xwalk.py:295-303). The "100% coords" is a tautology, not a quality signal: ~12% of *winner/company* rows have no coord because their address never matched (or the addr_hash never reached the crosswalk worklist), not because the crosswalk has nulls.

**`geocode_xwalk` builder facts [V/CORRECTED]:**
- Census Bulk Geocoder (`geocoding.geo.census.gov/.../addressbatch`, no key). **`CENSUS_BATCH` default = 10000** [CORRECTED — briefing said "1000–2500"]. Benchmark `Public_AR_Current` [CORRECTED — not a date-pinned vintage]. 3 retries, skip-on-fail per batch.
- Two worklist sources, both feeding the SAME accretive dataset: `build` (awards — recipient + subawardee addresses, rolling `GEOCODE_WINDOW_DAYS=90`) and `build_blitz` (firmographics_blitz `domain_norm` → `sam_master_domains.normalized_domain` → UEIs → `entity_profile_gold.physical_address_*`).
- `mode="create"` only on a genuinely absent dataset (errors loudly otherwise — never overwrites); the only write touching an existing dataset is `merge_insert` (non-destructive) [V].
- The 224,836 total >> the "~42k 90-day seed" because the all-time/blitz backfill has been run [V].

### Source feeds + caveats

| Feed (URI) | Rows [ASSUMED from briefing] | Caveat |
|---|---|---|
| `usaspending_api_fresh/contract_prime_txn/` | ~1.52M | **Rolling ~100-day `last_modified_date` window + 90d backfill — NOT a complete year.** A winner with no recent transaction is invisible. **This is the single biggest data-completeness limit (R-06).** |
| `usaspending_api_fresh/contract_subaward/` | ~199,901 | Same rolling-window limit; subawards are sparse (only 1,756 made it into the winners serving table). |
| `firmographics_blitz/` | ~235,426 | 1/`domain_norm`. The company-map universe is gated by this — only companies reachable from a blitz domain appear. |
| `sam_master_domains/` | ~709,546 | domain→UEI link; **domain-only match (~86% parity per builder docstring), blitz `uei` ignored.** |
| `entity_profile_gold/` | ~1.54M | 1/`uei`. **Address-only, no native coords** — must route through `geocode_xwalk`. |

> The prime/sub feed row counts and the firmographics/SAM counts above were **not** re-probed this pass (heavy); they are carried from the briefing as **[ASSUMED]**. The three *serving* tables they produce WERE re-probed and match exactly.

### The `addr_hash` single-source join key ([RESOLVED] — was R-01)

`addr_hash_sql()` is **single-sourced** in `pipelines/_shared/addr_hash.py` and imported by all three builders (`geocode_xwalk.py`, `materialize_winners_map.py`, `materialize_company_map.py`). The prior 3-file verbatim duplication was extracted by **PR [#425](https://github.com/bencrane/core-x/pull/425) (`22f1510`)**, which also added a parity unit test (`pipelines/_shared/tests/test_addr_hash_parity.py`) pinning byte-identical output across import sites. The canonical expression:

```
md5(
  upper(regexp_replace(trim(coalesce(<street>,'')),'\s+',' ','g')) || '|' ||
  upper(trim(coalesce(<city>,'')))  || '|' ||
  upper(trim(coalesce(<state>,''))) || '|' ||
  substr(regexp_replace(coalesce(<zip>,''),'[^0-9]','','g'),1,5)
)
```

Drift is now structurally impossible: one definition, guarded by a test. (Historically the hazard was real — a one-byte drift between the three copies would have made the coord LEFT JOIN silently produce all-null coords for the affected serving table: no error, just an empty map. That failure mode, formerly the doc's #1 HIGH risk R-01, is closed by #425.)

---

## 4. Read Path

### 4.1 catalyst_api — EXECUTE (`apps/catalyst_api`)

- **Route:** `POST /api/v1/map/{dataset}/query` (`dataset` ∈ `winners` | `company`). Body: `{title?, filters:[{field,op,value}], limit?}` (`MapQueryRequest`, `models.py:40`). `title` is echo-through and ignored by EXECUTE.
- **Decoder** (`src/map_decoders.py`): `FieldSpec(column, type, ops, enum?, index?)` + `Decoder(dataset_key, version, geometry, properties, fields, synonyms)`. Versions: **`winners.v1`**, **`company.v1`**. This is the load-bearing security artifact — the field allowlist, per-field op allowlist, and types. **Bump `version` on any field/enum/synonym change** (busts the edge memo).
- **Compile + safety** (`src/lance_store.py` `compile_map_filter`):
  - Column from `FieldSpec.column` only (never interpolated from caller).
  - Per-field type validation in `_map_coerce` (a bool is NOT an int; strings go through `_sql_str` quote-doubling — the only place a caller string is escaped).
  - Ops: `=`, `>=`, `<=`, `in`→`IN(...)`, `between`→`a >= lo AND a <= hi`. Bools unquoted Arrow literals. Multi-clause AND.
  - Appends `<lat_col> IS NOT NULL` so the scan + row cap cover only **plottable** rows → `meta.returned` == feature count.
  - `MapCompileError` → **422**; unknown dataset → 404.
- **Cap:** `MAP_HARD_ROW_CAP = 20_000`. Scans `cap+1` to detect over-cap; `meta.capped` flags truncation.
- **Auth:** `require_operator` validates `Authorization: Bearer <CATALYST_API_TOKEN>` in constant time. **Fail-closed:** an unset token in a deployed env (Railway) refuses to boot (`config.auth_required`). Local dev warns + allows.
- **Tests:** `tests/test_map_query.py` — **19 tests [V]** including injection-escape, enum-violation, type-mismatch rejections, axis-order, null-coord drop.
- **Deployed:** `https://api.catalystdev.run` [V — root + `/healthz` 200; map query returns **401** without token].

### 4.2 edge_api — TRANSLATE (`apps/edge_api`)

- **Route:** `POST /api/v1/map/{dataset}/ask` (`src/routers/map_ask_v1.py`). Body `{q}`. Service-token gated (`require_service_token`).
- **LLM call** (`src/services/anthropic_messages.py`): one forced-tool `/v1/messages` call, `tool_choice` pins `emit_filter`, `max_tokens=512`, `timeout=15s`, `retries=1` (only on 5xx/transport). Uses `ANTHROPIC_API_KEY` (distinct from the managed-agents key). System block sent with `cache_control: ephemeral` (prompt-cached).
- **Decoder mirror** (`src/map_decoders.py`): hand-mirrored prompt-facing subset (field names, types, ops, enums, synonyms). Distinct internal Lance columns are **not** exposed (`latitude`, `total_active_obligations`, `physical_address_state`, …); the prompt only ever sees public field names — which happen to coincide with the column string for the handful of fields the operator named identically (`naics2`, `primary_naics`, `total_obligation`, `award_count`). The security boundary does **not** rest on prompt opacity regardless: EXECUTE sources columns solely from `FieldSpec.column` (§4.1), so even a leaked column name buys an attacker nothing. `render_decoder_prompt()` builds the cached system block; `build_emit_filter_tool()` builds the enum-bounded tool schema.
- **Memo** (`map_ask_v1.py:34`): `_MEMO[(normalized_q, decoder_version, model)] = filter_object`. **Stores the filter, never GeoJSON** — every hit re-executes against live Lance. Process-local, cleared on deploy. A decoder version bump busts the key.
- **Model:** `MAP_COMPILER_MODEL`, default **`claude-opus-4-7`** [V — env not set in `core-x/prd`, so default is live]. Operator chose Opus over Haiku for translation quality (low-volume box).
- **Forwarder** (`src/services/catalyst_client.py`): POSTs the filter to `CATALYST_API_BASE_URL/api/v1/map/{dataset}/query` with `Bearer CATALYST_API_TOKEN`, returns the envelope verbatim plus `query: <filter>`.
- **Tests:** `tests/test_map_ask.py` — **8 tests [V]**: version parity, field-set parity, ops-subset, synonym validity, enum-bounded tool schema, no-columns-in-prompt, **plus per-field type parity (`test_edge_field_types_match_catalyst`) and enum-value parity (`test_edge_field_enums_match_catalyst`)** added by PR [#424](https://github.com/bencrane/core-x/pull/424) (`850c17f`). The former R-02 gap (enum/type *values* not asserted) is **[RESOLVED]** — `set(edge_enum) == set(cat_enum)` and per-field type now fail the test on any drift.
- **Deployed:** `https://api.edgeapi.run` [V — root 200]. **Live end-to-end [V]:** `/ask` "construction companies that have won federal awards" → model emitted `{naics2='23', has_federal_awards=true}` → **7126 features, capped=false** (exact match to construction+federal plottable count probed directly). Latency ~4.6s cold (first translation); memo makes repeats fast.

### 4.3 Deployment + required secrets

Both services deploy via `CMD ["doppler","run","--","python","-m","apps.<svc>.main"]`, EXPOSE 8080 [V]. Railway, GitHub-synced (push to `main` redeploys). (edge_api's Dockerfile also documents a Render fallback path.)

| Secret | Project/config | Used by | Verified present [V] |
|---|---|---|---|
| `CATALYST_API_TOKEN` | `core-x/prd` | catalyst (gate) + edge (presents) + BFF (`COREX_SERVICE_TOKEN` mirror) | ✅ |
| `CATALYST_API_BASE_URL` | `core-x/prd` | edge → catalyst | ✅ (`https://api.catalystdev.run`) |
| `ANTHROPIC_API_KEY` | `core-x/prd` | edge LLM call | ✅ |
| `EDGE_API_SERVICE_TOKEN` | `core-x/prd` + `hq-rare-structure-hq` | BFF → edge | ✅ |
| `MAP_COMPILER_MODEL` | `core-x/prd` | edge model override | ❌ NOT set → defaults `claude-opus-4-7` |
| `CATALYST_REQUIRE_AUTH` | `core-x/prd` | catalyst fail-closed override | ❌ NOT set → auth on via `RAILWAY_ENVIRONMENT` |
| `R2_ACCESS_KEY_ID` / `R2_SECRET_ACCESS_KEY` / `R2_ENDPOINT` (or `R2_ACCOUNT_ID`) | `core-x/prd` | catalyst Lance reads | (fleet convention) |
| `EDGE_API_URL` | `hq-rare-structure-hq` | BFF → edge base URL | (BFF env) |

---

## 5. Frontend (rare-structure-hq)

### 5.1 BFF (`apps/platform-api`)
- `src/lib/edge.ts` `askMarket(dataset, q)`: POSTs `/api/v1/map/{dataset}/ask` with the service token, flattens the GeoJSON FeatureCollection → `rows` (properties + `lat`/`lon` from `geometry.coordinates`), `total`, `capped`, `query`. **`AskMarketRow` carries `lat`/`lon` [V].**
- `src/routes/federal.ts` `GET /api/v1/federal/ask?q=&dataset=` → `askMarket`. **Unauthenticated** (no `requireUser`) — the cockpit `/map` is public.

### 5.2 The warm-snapshot vs live-`/ask` DUALITY (document this carefully)
`federal.ts` mounts **two different data sources** under `/api/v1/federal/*`:
- **Warm, in-memory:** `/spend-by-industry`, `/spend-by-state`, `/spend-by-agency`, `/entities`, `/entity/:uei` all read the boot-loaded `federal-store` snapshot (precomputed by core-x's `materialize_federal_charts`, PR #404). **No Lance, no DuckDB, no core-x round-trip at request time** — the hard invariant for the public surface. Canned ⌘K commands hit these.
- **Live:** `/ask` is the ONLY federal route that round-trips to core-x (edge → catalyst → Lance). The free-text box hits this.

These are **separate data sources** — the warm snapshot and the live serving tables can drift apart in vintage. A user toggling between a canned command and a free-text query is comparing two different materializations.

### 5.3 Cockpit (`apps/platform-app/src/demo`)
- `federalApi.ts` `askMap(nl, dataset)` → BFF `/ask`. `AskMarketRow` type carries `lat`/`lon` [V].
- `data.ts`:
  - `MapQuery.nl` (types.ts) → `runQuery` (data.ts:1411) → if `q.nl` set, `runAsk(nl, dataset ?? "company")` (data.ts:1413).
  - `runAsk` → `askMap` → `dedupeByName(rows.map(askRowToCompany))`.
  - `askRowToCompany` (data.ts:1233): maps a row → `Company`. **CRITICAL GAP [V]: it reads name/uei/naics/city/state/obligations but does NOT read `r.lat`/`r.lon` and does NOT set `x`/`y`.** The real coordinates reach the cockpit but are **discarded at this boundary** — so the geo-dot layer has nothing to plot even though the data is present one function-call away.
  - `dedupeByName` (data.ts:1269): groups by normalized name, keeps the largest-obligation entity's identity, **sums `totalAwarded` across the group**, records `relatedEntities = group.length`. Headline `total` = distinct-company count post-collapse; raw UEI match count rides in `fullUniverse`.
- `components/CommandPalette.tsx` ("Ask the market" row), `ResultsTable.tsx` (full-surface table — the default view), `MapView.tsx` (Map/Table toggle), `DemoApp.tsx` (`resultView`, default **Table**), `TerminalChrome.tsx` `ViewToggle`.
- **Geo-dots do NOT render [V]:** `MapView.tsx:91` plots only `c.x != null && c.y != null`; live entities have neither. The us-geo viewBox (`us-geo.ts`) is an **offline-projected Albers-USA composite (lower-48 + AK/HI insets), 1000×590**, plain SVG path strings — **no runtime projection**. The 6 historical catalyst points were lon/lat-projected offline.

---

## 6. Critical Assessment

### 6.1 Verified vs assumed

| Claim | Status |
|---|---|
| geocode_xwalk 224,836 / winners 40,191 (38,435+1,756) / company 243,842 | **VERIFIED [V]** (live probe) |
| coords: winners 35,306 / company 213,949 / company federal 139,918 | **VERIFIED [V]** |
| construction+federal plottable = 7126; live `/ask` returns 7126 | **VERIFIED [V]** (live end-to-end) |
| catalyst deployed, 401 without token; edge deployed | **VERIFIED [V]** |
| `addr_hash_sql` single-sourced (`pipelines/_shared/addr_hash.py`); 3 builders import it | **VERIFIED [V]** — was 3-file dup, extracted by #425 |
| 19 catalyst + 8 edge map tests | **VERIFIED [V]** — edge was 6; #424 added type+enum parity |
| All PRs MERGED (#413,#414,#416,#417,#419,#421,#423,#424,#425 core-x; #98,#99 hq) | **VERIFIED [V]** |
| Secrets in `core-x/prd` | **VERIFIED [V]** (4 present; 2 intentionally unset) |
| model `claude-opus-4-7` | **VERIFIED [V]** (default; env unset) |
| Census batch 1000–2500 | **CORRECTED → 10000 [V]** |
| Source feed row counts (prime 1.52M, sub 199,901, etc.) | **ASSUMED** (not re-probed) |
| Census geocoder match-rate quality | **ASSUMED** (no per-run match-rate audited) |

### 6.2 Risks & tech debt

| ID | Risk | Sev | Failure mode | Mitigation / measure |
|---|---|---|---|---|
| **R-01** | ~~`addr_hash` 3-file parity~~ **[RESOLVED]** | ~~HIGH~~ | Was: a one-byte drift in any copy → coord LEFT JOIN all-null → blank map, no error. | **Closed by PR [#425](https://github.com/bencrane/core-x/pull/425) (`22f1510`):** `addr_hash_sql` single-sourced in `pipelines/_shared/addr_hash.py`, imported by all three builders; parity unit-test (`test_addr_hash_parity.py`) pins byte-identical output on a fixture address. |
| **R-02** | ~~edge↔catalyst decoder drift~~ **[RESOLVED]** | ~~MED~~ | Was: parity test checked field-set/version/ops-subset but not enum values or types → silent capability loss, or EXECUTE 422s a "valid" suggestion. | **Closed by PR [#424](https://github.com/bencrane/core-x/pull/424) (`850c17f`):** `test_edge_field_types_match_catalyst` + `test_edge_field_enums_match_catalyst` now assert per-field type and `set(edge_enum) == set(cat_enum)`; any enum/type mismatch fails the test. |
| **R-03** | dedup award-summing semantics | **MED** | `dedupeByName` sums `total_active_obligations` across same-name UEIs. Two genuinely distinct companies sharing a name get merged + summed (overstated headline); if the serving value were ever a pre-rolled-up figure, double-count. Grouping by name (not UEI parent) is a heuristic. | Group by a real corporate-parent key if/when available; otherwise label the row "(N entities)" and show the sum is a group total. Measure: spot-check 10 collapsed groups against SAM parent relationships. |
| **R-04** | public unauthenticated `/ask` LLM cost/abuse | **HIGH** | BFF `/ask` and edge `/ask` are both public; **every** call is one Opus round-trip. No rate-limit (only a code comment). A scraper or accidental loop runs up Anthropic spend; memo only helps *repeated identical* queries. | Add per-IP rate-limit at the BFF `/ask` (Hono middleware) + a global token-bucket. Measure: load-test confirms >N req/min/IP → 429; Anthropic dashboard cost flat under flood. |
| **R-05** | Census geocoder reliability | **MED** | Batch geocode is skip-on-fail; a sustained Census outage silently defers addresses (coords stay null → fewer dots). No alerting on match-rate regression. | Record per-run match-rate in `ops.geocode_xwalk_runs`; alert if a run's match-rate drops below a threshold. Measure: ledger row carries `geocoded/worklist` ratio; an alert fires on regression. |
| **R-06** | feed completeness (rolling window) | **HIGH** | Prime feed is a rolling ~100-day `last_modified_date` window, not a full year. A real federal winner with no recent transaction is **absent** from the winners map. Users will read the map as comprehensive; it is a recency slice. | Either widen to 365d (§7-7) or label the map "active in the last ~100 days." Measure: a known long-tail winner appears/doesn't per the documented window. |
| **R-07** | geo-dot layer unrealized + lat/lon dropped | **HIGH** (headline) | Two-part: (a) no runtime lon/lat→viewBox projection; (b) `askRowToCompany` discards `lat`/`lon` so even with a projection there's nothing to project. Map view shows zero dots for live queries. | §7-1. Measure: live "construction federal" query renders thousands of dots inside US state outlines, AK/HI included. |
| **R-08** | two-data-source UI (warm vs live) | **MED** | Canned commands (warm snapshot) and free-text (`/ask` live) can show inconsistent numbers for "the same" query; users can't tell which source they're seeing. | Stamp each result with its source + vintage; ideally converge both on the live serving tables. Measure: same logical query via both paths reconciles, or the UI labels the divergence. |
| **R-09** | boot contract check is reachability-only | **MED** | `probe_surfaces()` opens manifests (`count_rows`) but does NOT assert decoder columns/indices exist. A renamed column or dropped BITMAP index → EXECUTE 5xx or silent full-scan, not caught at boot. | §7-5 (Plan §6.4). Measure: boot fails loud if any decoder column or declared index is missing from the live schema. |
| **R-10** | memo is process-local + unbounded | **LOW** | `_MEMO` is a plain dict, never evicted, lost on deploy. Unbounded growth under high query diversity (small risk given low volume); no cross-instance sharing. | Bound with an LRU; acceptable as-is at current volume. Measure: memory flat under a large distinct-query set. |

---

## 7. Roadmap (prioritized)

### 7-1 · Geo-dot projection (the headline) — **HIGH**
**Goal:** live `/ask` results render as dots on the cockpit map.
**Mechanism (two parts):**
1. **Thread coordinates through:** in `apps/platform-app/src/demo/data.ts` `askRowToCompany`, read `r.lat`/`r.lon` and carry them onto `Company` (add `lat?`/`lon?` to the `Company` type in `types.ts`). They're already on `AskMarketRow` (federalApi.ts:75) — currently dropped.
2. **Project lon/lat → the 1000×590 Albers-USA viewBox.** The us-geo geometry was built with an **offline Albers-USA composite incl. AK/HI insets**, so a naive `geoAlbersUsa()` will not align to those insets out of the box. Two options:
   - Reconstruct the exact projection used to generate `us-geo.ts` (d3-geo `geoAlbersUsa().scale(s).translate([tx,ty])` or `.fitSize([1000,590], statesFeatureCollection)`) and project at runtime. Verify by overplotting a known city (e.g. DC ≈ near the mid-Atlantic) and confirming it lands inside the DC/MD outline.
   - Or precompute `x`/`y` in the serving table build (project lat/lon at materialize time) and ship pixel coords on the row — keeps the cockpit projection-free but couples the table to one viewBox.
**Recommended:** runtime `geoAlbersUsa().fitSize([1000,590], <states FeatureCollection>)` reusing the *same* feature set `us-geo.ts` was fitted from, so AK/HI insets coincide.
**Measure of done:** "construction companies that have won federal awards" in Map view renders ~7,126 dots, all inside US state polygons, AK/HI included; clicking a dot opens that company's profile.

### 7-2 · Decoder vocabulary enum hints (`industry`, `employee_size_band`, `company_type`) — **MED**
**Goal:** the model emits valid `industry`/size/type values instead of guessing freeform strings.
**Live cardinality [V]:** `employee_size_band` = **8** distinct (`1-10`, `11-50`, `51-200`, `201-500`, `501-1000`, `1001-5000`, …) → **clean enum**. `company_type` = **10** distinct (`Self-Owned`, `Privately Held`, `Partnership`, `Nonprofit`, `Public Company`, …) → **clean enum**. `industry` = **463** distinct → too many to enum; instead add the top ~30 as `desc` hints or a curated synonym table, leave the field open-valued.
**Mechanism:** pull distinct values (`SELECT DISTINCT` via a Lance scanner + `pyarrow.compute.unique`), add `enum=(...)` to the `FieldSpec` in **both** `apps/catalyst_api/src/map_decoders.py` and `apps/edge_api/src/map_decoders.py`, **bump `company.v1` → `company.v2`** (busts the memo). The enum-parity guard this depends on is **already in place** ([RESOLVED] R-02, PR #424) — an enum added to one decoder but not the other now fails `test_edge_field_enums_match_catalyst`.
**Measure of done:** a size/type query ("companies with 51-200 employees") compiles to the exact enum value and returns rows; an off-enum value 422s.

### 7-3 · Rate-limit / auth the public `/ask` — **HIGH**
**Goal:** cap LLM cost and abuse on the unauthenticated `/ask`.
**Mechanism:** add a Hono rate-limit middleware on `federalRoutes.get("/ask")` in `apps/platform-api/src/routes/federal.ts` (per-IP token bucket) and a global ceiling; optionally a lightweight CAPTCHA/origin check. Consider a short-TTL response cache keyed on the normalized query to absorb bursts.
**Measure of done:** load test → >N req/min/IP returns 429; Anthropic spend stays flat under a flood.

### 7-4 · ZIP-centroid fallback for the unmatchable geocode tail — **MED**
**Goal:** recover the ~12% of serving rows with no coord (address didn't rooftop-match).
**Mechanism:** in `geocode_xwalk.py`, on a Census no-match, fall back to a ZIP5-centroid lookup (a static ZIP→lat/lon table) with a `match_type='zip_centroid'` marker so the UI can render those dots at lower confidence. Keep rooftop matches authoritative.
**Measure of done:** company-map coord coverage rises from 87.7% toward ~99%; `match_type` distinguishes centroid dots; no rooftop coord is overwritten.

### 7-5 · catalyst boot schema/index contract check (Plan §6.4) — **MED**
**Goal:** fail loud at boot if a decoder column or declared index is missing from the live Lance schema (R-09).
**Mechanism:** extend `probe_surfaces()` (or add `verify_decoder_contract()`) in `apps/catalyst_api/src/lance_store.py`: for each decoder, open the dataset, assert every `FieldSpec.column` + geometry column exists in `ds.schema`, and every declared BTREE/BITMAP index exists in `ds.list_indices()`. Call it from `main.py` `lifespan` and surface on `/healthz`. Today `lifespan` only does `count_rows()` reachability.
**Measure of done:** booting against a dataset missing a decoder column or index logs a loud error / fails the deploy; `/healthz` reports per-decoder contract status.

### 7-6 · Cadence / scheduling — **MED**
**Goal:** keep geocode, serving tables, and the warm federal snapshot fresh on a schedule.
**Mechanism:** wire the rebuild commands (§8) into the orchestrator (Modal/Trigger.dev/cron) in dependency order: `geocode_xwalk build`+`build_blitz` → `materialize_winners_map build` + `materialize_company_map build` → `materialize_federal_charts` (warm snapshot, redeploy BFF to reload). Record each in its `ops.*_runs` ledger (already wired).
**Measure of done:** a scheduled run advances all `ops.*_runs` ledgers with `status='success'`; the cockpit reflects new data without manual steps.

### 7-7 · Expose `winners` in the box + 365d refresh + place-of-performance — **MED**
**Goal:** let users query winners (currently `company`-only default) and widen the recency window.
**Mechanism:** (a) add a dataset toggle in the cockpit (`MapQuery.dataset`, plumbed end-to-end already — `runAsk(nl, dataset)` accepts `winners`); (b) set `WINNERS_WINDOW_DAYS=365` and rebuild; (c) add a place-of-performance geocode layer (winners are recipient-address-keyed today; PoP is a distinct location dimension — would need a PoP address → addr_hash join).
**Measure of done:** a winners query ("prime construction between 150k and 500k") returns from the box; window covers 365d; PoP layer toggles independently of recipient location.

### 7-8 · `addr_hash` shared helper (kill R-01) — **✅ DONE (PR [#425](https://github.com/bencrane/core-x/pull/425))**
**Shipped:** `addr_hash_sql` (+ `_zip5_sql`) extracted to `pipelines/_shared/addr_hash.py`; all three builders import it; `pipelines/_shared/tests/test_addr_hash_parity.py` asserts a fixture address hashes identically through every import site. The function now exists in exactly one file. R-01 closed — no further action.

---

## 8. Operational Runbook

### Rebuild commands (run from the core-x repo root)

**geocode_xwalk** (accretive — safe to re-run):
```bash
doppler run -p core-x -c prd -- uv run --no-project \
  --with 'pylance>=7' --with 'pyarrow>=17' --with 'duckdb>=1.5,<2' \
  --with 'requests>=2.32' --with 'psycopg[binary]>=3.2' \
  python3 pipelines/usaspending/geocode_xwalk.py <init_ops|build|build_blitz|verify> [window_days]
```

**winners serving table** (derived, overwrite):
```bash
doppler run -p core-x -c prd -- uv run --no-project \
  --with 'pylance>=7' --with 'pyarrow>=17' --with 'duckdb>=1.5,<2' \
  --with 'psycopg[binary]>=3.2' \
  python3 pipelines/serving/materialize_winners_map.py <init_ops|build|verify|demo> [window_days]
```

**company serving table** (derived, overwrite):
```bash
doppler run -p core-x -c prd -- uv run --no-project \
  --with 'pylance>=7' --with 'pyarrow>=17' --with 'duckdb>=1.5,<2' \
  --with 'psycopg[binary]>=3.2' \
  python3 pipelines/serving/materialize_company_map.py <init_ops|build|verify|demo>
```

Order for a full refresh: `geocode_xwalk build` (+`build_blitz`) → `materialize_winners_map build` + `materialize_company_map build`. The warm snapshot (`pipelines/serving/materialize_federal_charts.py`, PR #404) is a separate rebuild that the BFF reloads on redeploy.

> **Local probe gotcha [V]:** running the `uv --with pylance` one-liner from **inside the core-x repo root** can collide with the repo `.venv` (an "OSError: version must be an integer" on `lance.dataset`). Run ad-hoc probes from a neutral cwd (e.g. `/tmp`) or rely on the repo `.venv` directly.

### Redeploy
- **core-x services (catalyst_api, edge_api):** push to `main` → Railway GitHub-sync redeploys; secrets injected via `doppler run` in the Dockerfile CMD. An empty `chore: trigger Railway redeploy` commit forces it (see `d90b455`).
- **rare-structure-hq (BFF + cockpit):** push to `main` → its deploy reloads, including the warm `federal-store` snapshot at boot.

### Ops ledgers (HQX Postgres, `HQX_DB_URL_POOLED`)
- `ops.geocode_xwalk_runs`, `ops.winners_map_serving_runs`, `ops.company_map_serving_runs` — one row per terminal run with `status`, row counts, coord rate. DDL: `pipelines/usaspending/ops_geocode_xwalk_runs.sql`, `pipelines/serving/ops_{winners,company}_map_serving_runs.sql`. Each builder's `init_ops` command creates its table.

### Verify each layer
```bash
# Datasets (run from /tmp to avoid repo .venv collision):
doppler run -p core-x -c prd -- uv run --no-project --with 'pylance>=7' python3 -c '
import os,lance
so={"aws_access_key_id":os.environ["R2_ACCESS_KEY_ID"],"aws_secret_access_key":os.environ["R2_SECRET_ACCESS_KEY"],
    "endpoint":os.environ.get("R2_ENDPOINT") or f"https://{os.environ[\"R2_ACCOUNT_ID\"]}.r2.cloudflarestorage.com","region":"auto"}
for n in ["geocode_xwalk","usaspending_winners_map_serving","firmographics_company_map_serving"]:
    print(n, lance.dataset(f"s3://data-sink/active/{n}/",storage_options=so).count_rows())'

# catalyst (deployed): expect 401 without token, 200 with
curl -s -o /dev/null -w "%{http_code}\n" -X POST https://api.catalystdev.run/api/v1/map/company/query \
  -H 'content-type: application/json' -d '{"filters":[]}'

# edge end-to-end (needs the service token):
TOKEN=$(doppler secrets get EDGE_API_SERVICE_TOKEN -p core-x -c prd --plain)
curl -s -X POST https://api.edgeapi.run/api/v1/map/company/ask \
  -H "authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"q":"construction companies that have won federal awards"}' | python3 -m json.tool | head -30
```
Expected live answers (2026-06-11): catalyst no-auth → **401**; edge `/ask` → `query.filters` = `naics2='23'` + `has_federal_awards=true`, `meta.returned` = **7126**, `capped=false`.

---

## 9. File & PR Index

### Core-x — data plane
| Path | Role |
|---|---|
| `pipelines/_shared/addr_hash.py` | **`addr_hash_sql` single source of truth** (imported by all 3 builders); `tests/test_addr_hash_parity.py` pins byte-identity (#425) |
| `pipelines/usaspending/geocode_xwalk.py` | address→coords crosswalk (accretive); imports `addr_hash_sql` from `_shared` |
| `pipelines/serving/materialize_winners_map.py` | winners serving table |
| `pipelines/serving/materialize_company_map.py` | company serving table |
| `pipelines/serving/materialize_federal_charts.py` | warm federal snapshot (BFF in-memory source) |
| `pipelines/usaspending/ops_geocode_xwalk_runs.sql` · `pipelines/serving/ops_{winners,company}_map_serving_runs.sql` | ledger DDLs |

### Core-x — read path
| Path | Role |
|---|---|
| `apps/catalyst_api/main.py` | EXECUTE route + boot lifespan (reachability probes) |
| `apps/catalyst_api/src/map_decoders.py` | **authoritative** decoder allowlist (`winners.v1`, `company.v1`) |
| `apps/catalyst_api/src/lance_store.py` | `compile_map_filter`, `map_query`, `to_geojson`, `probe_surfaces` |
| `apps/catalyst_api/src/models.py` | `MapQueryRequest`/`MapFilterClause` wire contract |
| `apps/catalyst_api/src/config.py` | URIs, R2 creds, token, fail-closed auth |
| `apps/catalyst_api/tests/test_map_query.py` | 19 EXECUTE tests |
| `apps/edge_api/src/routers/map_ask_v1.py` | TRANSLATE route + memo |
| `apps/edge_api/src/map_decoders.py` | prompt-facing decoder mirror |
| `apps/edge_api/src/services/anthropic_messages.py` | forced-tool Messages call |
| `apps/edge_api/src/services/catalyst_client.py` | edge → catalyst forwarder |
| `apps/edge_api/src/config.py` | model, catalyst URL, keys |
| `apps/edge_api/tests/test_map_ask.py` | 8 parity tests (incl. per-field type + enum-value parity, #424) |

### rare-structure-hq — frontend
| Path | Role |
|---|---|
| `apps/platform-api/src/lib/edge.ts` | `askMarket` proxy (flattens GeoJSON → rows + lat/lon) |
| `apps/platform-api/src/routes/federal.ts` | `/ask` (live) + warm-snapshot routes |
| `apps/platform-api/src/lib/federal-store.ts` | warm in-memory snapshot |
| `apps/platform-app/src/demo/federalApi.ts` | `askMap`; `AskMarketRow` (carries lat/lon) |
| `apps/platform-app/src/demo/data.ts` | `runAsk`, `askRowToCompany` (DROPS lat/lon), `dedupeByName` |
| `apps/platform-app/src/demo/types.ts` | `MapQuery.nl`, `Company` (`x?`/`y?` absent for live) |
| `apps/platform-app/src/demo/MapView.tsx` | Map/Table toggle; plots only x/y-bearing rows |
| `apps/platform-app/src/demo/ResultsTable.tsx` | full-surface table (default view) |
| `apps/platform-app/src/demo/us-geo.ts` | offline Albers-USA composite, 1000×590 |

### Plan docs
- `docs/plans/MAP_READ_PATH_CATALYST_API_PLAN.md` (§6.4 = the schema/index contract check spec for 7-5)
- `docs/plans/NL_QUERY_MAP_COMPILER_STRATEGY.md`

### PRs (all MERGED [V])
core-x: **#413** geocode_xwalk + winners serving · **#414** geocode blitz_sam source + hardening · **#416** company serving · **#419** catalyst EXECUTE · **#421** edge TRANSLATE · **#417** plan doc · **#423** this handoff · **#424** edge type+enum parity (closes R-02) · **#425** `addr_hash` single-source (closes R-01) · `d90b455` deploy-trigger.
rare-structure-hq: **#98** NL wiring · **#99** table + Map/Table toggle.
