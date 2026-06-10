# NL-Query → Map: Compiler-Endpoint Strategy (and what we are *not* doing)

> **Audience:** the agent currently building the `query → map` surface through `edge_api`'s
> managed-agent path (`apps/edge_api/src/services/managed_agents.py`,
> `apps/edge_api/src/_hqx/app/services/anthropic_managed_agents.py`).
> **Decision:** the natural-language map search is served by a **thin stateless compiler endpoint**
> (`NL → constrained filter → DuckDB-over-Lance → results`) in a new `ask-api` service — **not** by an
> autonomous managed-agent loop. This document is the rationale and the build spec.
>
> **Read this first — what is and isn't being rejected.** We are **not** rejecting MCP-the-protocol.
> A single narrow MCP tool can be called *deterministically by a non-agent client* and return one
> structured result with no looping (the repo's own `gtm_mcp` proves it: `search_company_by_domain`,
> `lookup_awards_by_uei` are narrow typed tools, and `federal.py` notes "BFF can call it directly, not
> only via MCP"). What we are rejecting is putting an **autonomous agent runtime** — one that *selects*
> tools, *loops*, and *interprets/summarizes* results with prose latitude — under a map filter. The
> distinction is load-bearing; most of this doc is about it.
>
> **Nothing you've built is thrown away.** The managed-agent/MCP path you have becomes the open-ended
> **interrogation** surface (§4). This migration *adds* the filter surface beside it; it does not
> replace your work.

---

## 0. TL;DR

- The map's "Show me all X that Y" box is a **deterministic filter-and-render** interaction. Build it as
  **one HTTP route** that (1) calls an LLM **once** to translate the sentence into a constrained filter,
  (2) compiles that filter to parameterized SQL itself, (3) runs it in-process on DuckDB over a pre-baked
  Lance serving table, (4) returns rows/GeoJSON.
- **The lede for you specifically:** to drive a map from a browser you must build a plain HTTP
  translate-and-call endpoint **either way** — a browser speaks HTTP/JSON, not MCP, and not "agent." The
  compiler endpoint *is* that endpoint. The autonomous-agent path is that same endpoint **plus** a
  reasoning loop you don't need for a filter. So B is **less** to build and run, not more.
- We keep an LLM (the translate step). We remove the **agent loop** — the tool selection, the
  multi-turn looping, the result interpretation. The model is put **on rails** for one translation.
- The reliability/safety wins (determinism, no-SQL-from-the-model, testability) come from the **output
  contract + statelessness**, which a narrow MCP tool could adopt too. We choose the HTTP compiler
  because the **primary consumer is a browser**, we want an **LLM-free canned-toggle path**, and we want
  to **iterate the decoder fast** without redeploying a shared substrate. The trade vs. "add one narrow
  tool to the existing `gtm_mcp` server" is real and acknowledged in §4 — it is not one-sided.

---

## 1. What we are building (and the ground truth it stands on)

A live map for a sales motion to **hazard-remediation** buyers: "we are tracking all these
opportunities." Points are MSHA mine sites carrying remediation signals (silica/quartz overexposure,
S&S violations, accidents/fatalities, abandoned status), filterable by a natural-language search box.

The data plane is **already built and indexed on disk** (verified live, R2 + HQX, 2026-06-10). This
surface needs **no new ingestion and no entity resolution.**

- **19 MSHA Lance datasets** under `s3://data-sink/active/` (~14.74M rows), typed, BTREE/BITMAP-indexed,
  grain-clean. (Reconciles with the on-disk state diagnostic: 17 raw/extension datasets ≈ 14.60M rows +
  `msha_site_master` 91,803 + `msha_contractor_master` ~44.6K.)
- **`msha_mines`** (91,803) carries `LATITUDE`/`LONGITUDE` (DOUBLE), `STATE`, `ZIP_CD`.
- **`msha_site_master`** (91,803; 1 row per `MINE_ID`; 11 scalar indices) already pre-bakes the signal
  columns the map needs. **Real column names** (from
  `pipelines/ingest_msha/materialize_msha_site_master.py`): `silica_overexposure` (= max `QUARTZ_PCT` >
  5), `multi_controller_flag`, `multi_operator_flag`, `CURRENT_MINE_STATUS`, `COAL_METAL_IND`, `STATE`,
  `violation_count`, `ss_count`, `ss_count_since_2025` (≥ 2025-01-01), `order_count`,
  `proposed_penalty_sum` (gross **proposed**, not unpaid), `last_violation_dt`, `accident_count`,
  `fatality_count` (= `DEGREE_INJURY_CD = '01'`), `last_accident_dt`. **This is the seed of the serving
  table.** (Counts are **all-time** except the one `*_since_2025` window — see §5.1/§8.)

**Demo sizing (ad-hoc live probe, 2026-06-10 — methodology stated so it's reproducible, not a committed
cohort):** plottable = `msha_mines` with `LATITUDE`/`LONGITUDE` inside the US bounding box = **46,772**
(raw LAT/LON fill is ~52% per `MSHA_DATA_PROFILING_REPORT`; 46,772 ≈ 51% is the stricter *valid-US-coords*
subset that actually plots). Signal cohorts were computed with **probe-time** date filters on the real
indexed `VIOLATION_ISSUE_DT` / `ACCIDENT_DT` columns (a "since 2021" filter chosen for the probe — **not**
a stored window): silica ∩ geo ≈ 3,019; ≥1 S&S since 2021 ∩ geo ≈ 9,001; ≥1 accident since 2021 ∩ geo ≈
4,476; **union ≈ 12,139**. Status mix (matches the diagnostic): 69,196 abandoned + 8,853
abandoned-and-sealed vs 6,634 active (abandoned-dominant is a *feature* for AML-remediation framing).
**The serving table defines the canonical windows; today the only materialized window is
`ss_count_since_2025`.** Treat the "since 2021" cohort numbers as illustrative sizing, not as queryable
columns.

**Parked, out of scope:** the MSHA→corporate-spine legal-entity bridge. No MSHA→SoS bridge
(`bridge_msha_to_sos`) exists on disk (confirmed: grep-clean; both MSHA schema/state diagnostics record
the deliberate Directive-29 no-bridge stance and list "build operator/controller entity bridge" as
un-built). Note `entity_profile_gold` **does** exist — but it is the govcon (SAM×USAspending) gold mirror
(`apps/catalyst_api/src/config.py` `ENTITY_PROFILE_GOLD_URI`), **unrelated to MSHA**; cite it as the
*pattern* a future bridge would follow, not as a missing MSHA artifact. The bridge becomes relevant only
when the pitch shifts to "these are *your existing accounts*." Do not build it for this surface.

---

## 2. The two patterns, named precisely

Both run **DuckDB-over-Lance** as the engine. The difference is **how much latitude the model has** and
**who is driving.**

### A. Autonomous agent loop (what you have now, in `edge_api`)
An LLM **agent** reads the sentence, **selects** which tool(s) to call, the server runs them, results are
**fed back into the model**, the model **interprets** them and may call again, then emits a response. The
agent drives; it may use MCP as the transport, but the loop — not MCP — is the defining property. This is
the right shape for open-ended reasoning. It is the wrong shape for a filter.

### B. Compiler endpoint (the chosen path)
A single stateless HTTP route calls the LLM **once** with a curated decoder and **forces a structured
output** (a filter object — never prose, never SQL). The server **validates and compiles** that filter to
**parameterized** SQL against an allowlist, runs it in-process on DuckDB, returns rows/GeoJSON. The model
is a **translator on rails**, invoked as a subroutine — it does not drive, loop, or see results.

> **A narrow MCP tool is a third, valid shape** — one purpose-built `filter_mines(field, op, value)` tool
> called directly (one round-trip, no loop) is *behaviorally identical* to B; it just reaches the engine
> through MCP transport instead of an HTTP route. §4 covers when that's the better pick. The thing we
> reject is specifically **A** — the loop with prose latitude — not "MCP."

> **Directional correction that matters:** in B, *your route calls the LLM*. The LLM does **not** call
> your routes. One inbound request, one redraw. In A, the model orchestrates multiple tool round-trips.

---

## 3. The argument: why B for this surface

### 3.1 Surface & cost — you build the endpoint either way (the real lede)
A browser fetching map points speaks HTTP/JSON. It is not an MCP client and not an agent. So **a plain
translate-and-call endpoint must exist regardless** of which pattern you pick. B *is* that endpoint. A
adds an autonomous reasoning loop **on top of** the endpoint you'd build anyway. For a filter, that loop
buys nothing — so B is the smaller build and the smaller thing to operate, not "extra work over MCP."

### 3.2 Determinism, safety, testability — from the *contract*, not from avoiding MCP
Be precise about where these wins come from, because a skeptic who knows MCP will (correctly) reject the
lazy version of this claim:

- **Determinism** comes from forcing **enum-bounded structured output** and never letting the model emit
  prose or SQL. An agent loop *with free prose output* drifts (re-phrases, adds conditions you didn't ask
  for, summarizes instead of returning rows, takes a different path on re-run). A model *constrained to a
  structured filter* — whether in B or in a narrow MCP tool — does not. B simply makes that constraint the
  **entire** interaction.
- **Safety**: the model never emits SQL. It selects `field`/`op` from a closed enum and supplies a
  `value`; the server compiles to a **parameterized** query touching only the one serving table. A
  *general* run-query tool (like the repo's `execute_audience_query` escape hatch) widens the boundary; a
  *narrow parameterized* tool (like the repo's `search_company_by_domain`) does not. **B is safer than a
  broad tool, equal to a narrow one** — the win is removing SQL emission, not avoiding MCP.
- **Testability/cacheability**: `text → filter` is a pure function — unit-testable and cacheable. The
  untestability people associate with agents is a property of **stateful multi-turn** loops, not of a
  one-shot constrained call (B or a narrow tool both have it).

### 3.3 Latency — structural, not a vibe
The claim that survives scrutiny is **structural round-trip count**: B is exactly **one** model
round-trip by construction; an autonomous loop is **N ≥ 2** (decide-to-call, then interpret-result, often
more). The multiplier follows from the loop regardless of model speed. The seconds below are
**estimates/targets, not measurements** (external-API latency; measure on your endpoint): Haiku-class
translate ≈ **0.6–1.2s typical, ~0.5s best-case** on a warm prompt cache; the in-memory DuckDB filter is
sub-millisecond; canned toggles (no LLM) are sub-100ms. A *single non-looping tool call* (B or a narrow
MCP tool) has the same one-round-trip cost — so latency argues against **the loop**, not against MCP.

### 3.4 Why the HTTP compiler (not a narrow MCP tool) for *this* surface
Given §3.2 makes B and a narrow tool behaviorally equal, the tiebreakers are concrete: (a) the **primary
consumer is a browser** that wants plain HTTP/JSON; (b) we want an **LLM-free canned-toggle path** on the
same surface (instant, no model) which is natural in an HTTP route and awkward through an agent/MCP
client; (c) we want to **iterate the decoder rapidly** (prompt + synonym map) without redeploying the
shared `gtm_mcp` substrate. These, not any deficiency of MCP, are why `ask-api` wins here. §4 states the
honest counter-case.

---

## 4. When a narrow MCP tool is the better call (the honest counter-scenario)

Not one-sided. Two real points in MCP's favor for this exact surface:

1. **Reuse the substrate you already run.** `gtm_mcp` is a live, maintained MCP server with read-only
   gating (`assert_read_only`) and dataset scoping (`referenced_datasets`) already solved. Adding **one
   narrow `filter_mines` tool** there reuses all of that auth/gating/deploy machinery — potentially
   **less new infrastructure** than standing up a fresh `ask-api` Railway service. We still choose
   `ask-api` for the §3.4 reasons (browser-primary, LLM-free toggles, fast decoder iteration), **but the
   trade is real.**
2. **One implementation, two consumers.** Factor the filter logic as a single callable; expose it **as**
   one MCP tool *and* call it directly from the browser shim. Then the §4 interrogation agent and the map
   both hit the **same** filter implementation — avoiding two divergent query paths. (Your `gtm_mcp`
   already does exactly this dual-use: tools are mounted for MCP **and** "BFF can call it directly.")
   Build the compiler's core as a plain function so this option stays open even if `ask-api` owns the
   HTTP front door.

MCP/agent remains the right tool for genuinely **agentic** interrogation — *"find clusters of distressed
mines, then tell me which operators recur, then draft outreach."* That needs tool selection, looping, and
interpretation — the things B deliberately removes. Keep your `edge_api` managed-agent path for that.

---

## 5. The build (concrete)

### 5.1 The serving table — one flat, pre-baked, offline-built Lance dataset
A **single denormalized read model**, one row per mine, every filterable signal already a column. **Not**
a sidecar joined at read time — a flat table the query hits with **zero joins**.

- **Build it offline as a pipeline step** (NOT in the request path, NOT at `ask-api` startup-from-raw).
  Follow `pipelines/ingest_msha/materialize_msha_site_master.py` as the exact template: join
  `msha_site_master` (the pre-baked flags/counts above) + `msha_mines` (lat/lon/state/zip) + any added
  rollups, cast/DISTINCT, `lance.write_dataset(..., mode="overwrite")`. Output to a named dataset, e.g.
  **`s3://data-sink/active/msha_map_serving/`**. Index `STATE`/`COAL_METAL_IND`/`CURRENT_MINE_STATUS`
  (BITMAP) and `MINE_ID` (BTREE).
- **`ask-api` READS the finished Lance dataset** at startup and pins it in memory. It does not build it.
- **Use the real column names** (§1). If you want windows other than `ss_count_since_2025` (e.g. a
  fatality or accident window), **add them to this build** — do not assume they exist; today only all-time
  counts + `ss_count_since_2025` are materialized.
- **Property:** derived and **disposable**. The raw MSHA tables stay the system of record; this is a
  rebuildable overwrite-materialization. Reshape freely.

### 5.2 The decoder — the critical artifact (everything else is plumbing)
Curated, not auto-dumped. Three parts:

- **Field allowlist** — the ~12 serving-table fields with types and allowed values (`commodity ∈ {C,M}`
  from `COAL_METAL_IND`; `status ∈ {Active, Abandoned, Abandoned and Sealed, ...}`; `silica_overexposure`
  bool; the `*_count` ints; `proposed_penalty_sum` numeric).
- **Synonym/semantics map** (system prompt) — literal `term → {field, op, value}` rows the model copies:
  - `"fatality" / "death"` → `{field:"fatality_count", op:">=", value:1}`
  - `"S&S" / "significant and substantial"` → `{field:"ss_count", op:">=", value:1}`
  - `"silica" / "overexposure"` → `{field:"silica_overexposure", op:"=", value:true}`
  - `"abandoned"` → `{field:"status", op:"=", value:"Abandoned"}`
  - `"coal"` → `{field:"commodity", op:"=", value:"C"}`
  This is **the single biggest reliability lever** — bad term-mapping is where naive NL demos fail.
- **Output contract** — force the structured object via **Anthropic tool-use with `tool_choice` pinning a
  single `emit_filter` tool**, so the model **cannot** return prose or SQL. Schema:

  ```json
  {"title": "string",
   "filters": [{"field": "<enum of allowlisted fields>",
                "op": "<enum: =, >=, <=, in, between>",
                "value": "<scalar | array>"}]}
  ```

- **Scale note:** one serving table ⇒ the whole decoder fits one small prompt; nothing to route. Dynamic
  schema-fetching (pick the table first, then load its fields) is real but only needed at **many** tables.
  Pre-baking one cohort collapses it.

### 5.3 The `/ask` flow (one route, four in-process steps)
1. **Translate** — one LLM call (Haiku-class) with the cached decoder system block; `tool_choice` forces
   `emit_filter`. Reuse the existing Anthropic client/Doppler-key pattern in
   `apps/edge_api/src/_hqx/app/services/anthropic_managed_agents.py`.
2. **Compile (the security-critical step)** — for each filter, look up `field` and `op` in the allowlist;
   they map to a **hardcoded column name + operator template**. **Only `value` is bound as a query
   parameter — never string-formatted/concatenated into SQL.** Any off-allowlist `field` *or* `op` →
   reject the whole request. The model's output can never become SQL text.
3. **Execute** — run on **DuckDB embedded in the process** against the in-memory serving table
   (12k–47k rows). Sub-millisecond; no R2 round-trip on the hot path.
4. **Shape & return** — rows → JSON/GeoJSON; the page swaps its data source and redraws.

### 5.4 Where it runs — a new `ask-api` Railway service
- "The server" is a Railway-deployed FastAPI process holding DuckDB + the warm serving table + `/ask`.
- **Do not add `/ask` to `edge_api`** (where your managed-agent code lives) **or to `catalyst-api`** (the
  stable point-lookup API). The NL endpoint brings an outbound LLM dependency, an in-memory pinned cohort,
  and rapid decoder redeploys — keep that off both existing services (blast-radius containment).
- **Stand up `ask-api`** on the recipe `catalyst-api` already documents (Dockerfile, Doppler `core-x/prd`,
  co-located near R2). It reads the **same** Lance SoR — no data copied.
- **Share code, not process, and don't touch the stable services:** extract the Lance/R2/DuckDB read
  helpers from `apps/catalyst_api/src/lance_store.py` (or `apps/gtm_mcp/src/database.py`) into a shared
  module (e.g. `apps/_shared/` or `libs/`); `ask-api` imports it. **Do not modify `catalyst-api` or
  `gtm_mcp` behavior** beyond the mechanical extraction.

### 5.5 The hybrid that makes the demo feel instant
- **Canned toggles** (state, signal, status) emit the **same structured filter object** the decoder would
  produce — built client-side or by a thin server map — and POST it **directly to the compile+execute
  path (steps 2–4), skipping step 1 entirely**. A canned toggle **never sends natural language to the
  LLM**, so it's genuinely instant (sub-100ms) and fully deterministic.
- **Free-typed sentences** take the full `/ask` path (~0.6–1.2s).
- **Memoize** free-typed results keyed on **`hash(normalized_sentence, decoder_version, model_id)`**
  (`normalized` = lowercased + whitespace-collapsed). Cache is process-local and cleared on deploy, so a
  decoder change auto-invalidates old mappings. **Re-compile the filter each request even on a memo hit**
  (cache the *filter*, re-run steps 2–4) so an allowlist/schema change can never serve a stale column.

---

## 6. Latency budget
| Step | Cost | Notes |
|---|---|---|
| LLM translate | **~0.6–1.2s** (est., ~0.5s warm-cache best case) | Haiku-class + cached decoder prompt; the only real latency. **Estimate, measure on your endpoint.** |
| Compile filter → SQL | ~0ms | pure server code |
| DuckDB filter | **sub-ms** | in-memory serving table, 12k–47k rows |
| Shape + network back | tens of ms | GeoJSON serialize + one HTTP hop |
| **Canned toggle (no LLM)** | **<100ms** | structured filter straight to compile+execute |

Structural claim (the durable one): **B is one model round-trip; an autonomous loop is N ≥ 2.** Model
tier is the dominant lever — use **Haiku** for mechanical translation; cache the static decoder; keep
output tiny.

---

## 7. Migration for the agent on the `edge_api` managed-agent path
**This is additive, not a teardown.** Your managed-agent code stays as the §4 interrogation surface.

1. **Keep** the DuckDB-over-Lance read layer and the read-only gate — correct and reused.
2. **Build the serving table** (§5.1) as an offline pipeline step → `s3://data-sink/active/msha_map_serving/`.
3. **Stand up `ask-api`** (§5.4) — do **not** extend `edge_api` with `/ask`.
4. **Implement `/ask`** (§5.3): one forced-`emit_filter` LLM call + allowlist compile + in-memory execute.
5. **Put the effort in the decoder** (§5.2): field allowlist + synonym map + output schema. The rest is
   plumbing you already have.
6. **Add the hybrid** (§5.5): canned toggles bypass the LLM (structured filter direct to compile); memoize
   free-typed by the composite key.
7. **Retain `edge_api`'s managed-agent path** for §4 interrogation — and consider factoring the filter as
   a shared callable so it can *also* be one `gtm_mcp` tool (§4.2), one implementation feeding both
   surfaces.

---

## 8. Open items (genuinely deferrable — not build blockers)
- **Signal windows.** The serving build must decide each rollup's window. Today only `ss_count_since_2025`
  is windowed; everything else is all-time. If the map needs "last N years" for fatalities/accidents/S&S,
  add those columns to §5.1's build — do not reference windows that don't exist.
- **`silica_overexposure` semantics.** Confirm the `QUARTZ_PCT > 5` definition is the threshold you want
  to pitch on; it's already a clean bool, so no normalization needed.
- **Geo-fill messaging.** Agree the on-screen claim: "mines we can geolocate" (~51% / 46,772), not "every
  mine."
- (Model id, Anthropic client wiring, and prompt-cache are **not** open items — they're §5.3 step 1; reuse
  the `edge_api` Anthropic helper and name Haiku.)

---

## 9. One-line summary
**The map's NL box is a filter, not a conversation — so compile the sentence to a constrained, enum-bounded
filter with one on-rails LLM call and run it in-process on a pre-baked serving table; keep your existing
managed-agent path for the separate open-ended interrogation surface. We reject the autonomous agent loop
under a filter, not MCP-the-protocol — a narrow `filter_mines` tool would be equally valid, and we factor
the filter so it can become one later.**
