# NL-Query → Map: Compiler Route Spec

The map's natural-language search box is a **deterministic filter-and-render** interaction, not a
conversation. It is served by a compiler route in `edge_api`: one forced LLM call translates the
sentence into a constrained filter object, `edge_api` compiles that filter to parameterized SQL, runs
DuckDB in-process over a pre-baked Lance serving table, and returns rows/GeoJSON. The map never calls
an agent.

The pattern is general — it serves any `(serving table, decoder config)` pair. MSHA is instance #1
because its data plane is already built and indexed.

---

## 1. Data ground truth (verified on disk, R2 + HQX, 2026-06-10)

This surface needs no new ingestion and no entity resolution.

- **19 MSHA Lance datasets** under `s3://data-sink/active/` (~14.74M rows), typed, BTREE/BITMAP-indexed.
- **`msha_mines`** (91,803 rows) carries `LATITUDE`/`LONGITUDE` (DOUBLE), `STATE`, `ZIP_CD`.
- **`msha_site_master`** (91,803 rows, 1 per `MINE_ID`, 11 scalar indices) pre-bakes the signal columns.
  Real column names (from `pipelines/ingest_msha/materialize_msha_site_master.py`):
  `silica_overexposure` (= max `QUARTZ_PCT` > 5), `multi_controller_flag`, `multi_operator_flag`,
  `CURRENT_MINE_STATUS`, `COAL_METAL_IND`, `STATE`, `violation_count`, `ss_count`,
  `ss_count_since_2025`, `order_count`, `proposed_penalty_sum` (gross proposed), `last_violation_dt`,
  `accident_count`, `fatality_count` (= `DEGREE_INJURY_CD = '01'`), `last_accident_dt`. Counts are
  all-time except `ss_count_since_2025`.
- **~46.8K plottable**: `msha_mines` rows with valid US-bounding-box `LATITUDE`/`LONGITUDE`.

The MSHA→corporate-spine legal-entity bridge is out of scope; no `bridge_msha_to_sos` exists on disk.
`entity_profile_gold` is the govcon (SAM×USAspending) mirror, unrelated to MSHA.

---

## 2. The serving table

One flat, denormalized read model — one row per mine, every filterable signal already a column, zero
joins at read time.

- **Built offline as a pipeline step**, templated on
  `pipelines/ingest_msha/materialize_msha_site_master.py`: join `msha_site_master` (flags/counts) +
  `msha_mines` (lat/lon/state/zip) + any added rollups, cast/DISTINCT,
  `lance.write_dataset(..., mode="overwrite")` to a named dataset, e.g.
  `s3://data-sink/active/msha_map_serving/`. Index `STATE`/`COAL_METAL_IND`/`CURRENT_MINE_STATUS`
  (BITMAP) and `MINE_ID` (BTREE).
- **`edge_api` reads the finished dataset at startup** and pins it in memory. It does not build it.
- Derived and disposable. Raw MSHA stays the system of record; rebuild via overwrite freely.
- Only `ss_count_since_2025` is windowed today. New windows (fatality/accident/S&S over last N years)
  are added to this build, not assumed.

---

## 3. The decoder

The load-bearing artifact. Three parts.

**Field allowlist** — the ~12 serving-table fields with types and allowed values:
`commodity ∈ {C,M}` (from `COAL_METAL_IND`); `status ∈ {Active, Abandoned, Abandoned and Sealed, ...}`;
`silica_overexposure` bool; the `*_count` ints; `proposed_penalty_sum` numeric.

**Synonym/semantics map** (system prompt) — literal `term → {field, op, value}` rows the model copies:

| term | filter |
|---|---|
| `"fatality"` / `"death"` | `{field:"fatality_count", op:">=", value:1}` |
| `"S&S"` / `"significant and substantial"` | `{field:"ss_count", op:">=", value:1}` |
| `"silica"` / `"overexposure"` | `{field:"silica_overexposure", op:"=", value:true}` |
| `"abandoned"` | `{field:"status", op:"=", value:"Abandoned"}` |
| `"coal"` | `{field:"commodity", op:"=", value:"C"}` |

**Output contract** — Anthropic tool-use with `tool_choice` pinning a single `emit_filter` tool. Field
and op are enum-bounded; value is a scalar or array. The model cannot emit prose or SQL.

```json
{"title": "string",
 "filters": [{"field": "<enum of allowlisted fields>",
              "op": "<enum: =, >=, <=, in, between>",
              "value": "<scalar | array>"}]}
```

One serving table ⇒ the whole decoder fits one small cached prompt; nothing to route.

---

## 4. The `/ask` route (one route, four in-process steps)

1. **Translate** — one Messages API call (Haiku-class) with the cached decoder system block;
   `tool_choice` forces `emit_filter`. `edge_api` calls the Messages API over `httpx` with the
   Doppler-backed key — the existing pattern in
   `apps/edge_api/src/_hqx/app/services/anthropic_managed_agents.py`.
2. **Compile** — for each filter, look up `field` and `op` in the allowlist; they map to a hardcoded
   column name + operator template. Only `value` is bound as a query parameter, never formatted into
   SQL text. Any off-allowlist `field` or `op` rejects the request. The model's output never becomes
   SQL.
3. **Execute** — run DuckDB in-process against the in-memory serving table (~46.8K rows). Sub-ms; no
   R2 round-trip on the hot path.
4. **Shape & return** — rows → JSON/GeoJSON; the page swaps its data source and redraws.

---

## 5. Where it runs

The compiler is a **route in `edge_api`** — the service the request path already flows through and the
service that already holds the LLM dependency. There is no `ask-api` service.

Single-ingress request flow:

```
platform-app → platform-api → edge-api
  → one Anthropic Messages API call (tool_choice forces emit_filter → filter object)
  → edge_api compiles the filter to parameterized SQL
  → edge_api runs DuckDB in-process on the pre-baked Lance serving table
  → rows/GeoJSON back up the chain
```

Nothing bypasses `edge_api`. The BFF does not call `gtm_mcp` or Lance directly.

The DuckDB-over-Lance engine lives only in `apps/gtm_mcp/src/database.py`. `edge_api` gains the read
capability by importing a shared Lance/DuckDB helper extracted from there (with `assert_read_only`).
`catalyst_api` has no DuckDB.

The gtm-agent is a separate hosted Anthropic Managed Agent driven by a client `edge_api` holds; this
compiler route is independent of it.

---

## 6. Canned/free hybrid

- **Canned toggles** (state, signal, status) emit the same `emit_filter` filter object directly and
  POST it to the compile+execute path (steps 2–4), skipping the LLM entirely. Instant (sub-100ms),
  deterministic.
- **Free-typed sentences** take the full route — one model round-trip.
- **Memoize** free-typed results keyed on `(normalized_sentence, decoder_version, model_id)`
  (`normalized` = lowercased + whitespace-collapsed). Cache is process-local, cleared on deploy.
  Re-run compile+execute (steps 2–4) on every memo hit so an allowlist/schema change can never serve a
  stale column.

**Latency**: one model round-trip (Haiku-class, cached decoder prompt) for free text; sub-ms in-memory
DuckDB filter; canned toggles sub-100ms.
