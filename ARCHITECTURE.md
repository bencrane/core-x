# Architecture — core-x

Source of truth for core-x, the Gen-3 data & compute plane. Every new
data-ingest / compute worker follows these patterns; deviations require updating
this file first.

core-x is a clean room. There is **no Iceberg, no Polaris, no `modal.Cron`, and
no Gen-2 FastAPI/Railway *ingest* application.** The ingest/compute stack is
exactly four layers: a Trigger.dev v4 control plane, one proxy-authed Modal
dispatcher, domain-grouped Modal compute workers, and a DuckDB → LanceDB v2.0 →
R2 data plane. A thin **read-only gateway layer** (`apps/`) sits *on top of* the
committed data plane — it serves the Lance system-of-record to consumers (e.g.
the `gtm-mcp` MCP server) and reintroduces none of the retired patterns.

There is one reference implementation today — SAM.gov Contract Opportunities
(active) — and it is the template every other feed is built against.

| Layer | File | Modal app |
|---|---|---|
| Control plane | [`src/trigger/sam_opps_bulk.ts`](src/trigger/sam_opps_bulk.ts) | — |
| Router | [`core/modal_dispatcher.py`](core/modal_dispatcher.py) | `universal-dispatcher` |
| Compute worker | [`pipelines/sam_gov/sam_opps_bulk.py`](pipelines/sam_gov/sam_opps_bulk.py) | `sam-gov-pipelines` |

## 1. Control plane — Trigger.dev v4, durable callbacks

- Cadence is owned **exclusively** by Trigger.dev v4. Tasks live in
  `src/trigger/` (`trigger.config.ts` pins `dirs: ["./src/trigger"]`) and the
  schedule is declared in-code via `schedules.task({ cron })`.
- Durable HTTP callback via **waitpoint tokens**. The task `wait.createToken()`
  mints a pre-signed callback `url` (the `callbackHash` embedded in the URL is
  the auth — no API key), POSTs the dispatcher with that url as
  `trigger_callback_url`, then suspends on `wait.forToken(token.id)`. While
  suspended the run is checkpointed: zero compute, immune to HTTP timeouts.
- **API note:** the methods are `wait.createToken()` + `wait.forToken()`
  (completed via the token's pre-signed URL). There is **no `wait.forRequest()`**
  in the Trigger.dev v4 API — do not write it.
- **`modal.Cron` is strictly forbidden.** No worker carries an embedded cron;
  cadence belongs to Trigger v4, full stop.

## 2. Router — the Universal Dispatcher

- `core/modal_dispatcher.py`, Modal app `universal-dispatcher`. It is the
  **only** Modal app exposing a web endpoint for routing compute, and that endpoint is
  **proxy-authenticated** (`requires_proxy_auth=True`; `Modal-Key` /
  `Modal-Secret`). One endpoint for the entire fleet, `MODAL_DISPATCHER_URL`,
  forever. One separate push-ingestion endpoint exists for an external producer — §6.
- A **stateless router.** It receives the Trigger payload
  `{app_name, function_name, kwargs, trigger_callback_url}`, resolves the target
  via `modal.Function.from_name(app_name, function_name)`, `spawn()`s it
  fire-and-forget, and returns `202`. It holds no connection and owns no state.
- A new feed = a new worker + a one-line Trigger task. **Zero new endpoints,
  zero new secrets.** This is what kills per-feed env-var bloat.

## 3. Compute layer — domain-grouped Modal workers

- Modal apps are grouped **strictly by domain**:
  `app = modal.App("sam-gov-pipelines")`. Workers live under
  `pipelines/<domain>/` — e.g. `pipelines/sam_gov/`. **All new compute workers
  MUST be placed in a domain-specific subdirectory under `pipelines/`.**
- Workers **do not expose web endpoints** — one exception, a push-ingestion
  endpoint for an external producer, is documented in §6. Otherwise they are
  reachable only by the dispatcher's `spawn()` (or `modal run` for manual ops)
  and receive `trigger_callback_url` as a kwarg.

## 4. Data plane — DuckDB → LanceDB v2.0 → R2

- **100% DuckDB for transformation.** `read_csv(..., all_varchar=true)` on
  ingest, `TRY_CAST` for every type coercion, all projection / filter / shaping
  in SQL. Python does I/O only (stream the source to `/tmp`, hand the bytes to
  DuckDB).
- Output is **LanceDB v2.0** written directly to **Cloudflare R2**:
  `lance.write_dataset(s3://<bucket>/<path>/, data_storage_version="2.0")`.
  Lance is the system of record; every load-bearing resolution key gets a
  `BTREE` scalar index.
- Parquet, where used, is **transport only.** **No Iceberg. No Polaris.** The
  worker writes Lance to R2 with no catalog round-trip.

## 5. State management — the worker owns terminal state

- On terminal state (success **or** failure), the Modal compute worker, in
  order:
  1. writes the run row to the Postgres **`ops.*`** tables via **psycopg** — the
     compute that knows the true outcome owns the state row; and
  2. **immediately** POSTs `{status, ...}` back to the Trigger **wait-token
     callback URL**, waking the suspended run.
- Trigger.dev therefore owns true end-to-end success/failure state. **No
  polling, no heartbeat.** (SAM.gov writes `ops.sam_opps_canonical_runs`.)

## 6. Push-ingestion endpoints — external producers

One worker exposes its own web endpoint. It receives POSTs initiated by an
external producer (Clay) — it is not Trigger-scheduled and not dispatcher-spawned:

| File | Modal app | Endpoint | Auth |
|---|---|---|---|
| [`pipelines/gtm/clay_industries_endpoint.py`](pipelines/gtm/clay_industries_endpoint.py) | `gtm-clay-ingest` | `POST https://bencrane--clay-industries.modal.run` | `requires_proxy_auth=True` (`Modal-Key` / `Modal-Secret`) |

- Decorator: `@modal.fastapi_endpoint(method="POST", requires_proxy_auth=True, label="clay-industries")`.
- Each request inserts one row into HQX Postgres `public.company_industry_payloads`
  (`normalized_domain text`, `source_platform text`, `raw_payload jsonb`,
  `ingested_at timestamptz`) via psycopg; `raw_payload` is bound with `Jsonb`.
- Secret: `hqx-postgres` (`HQX_DB_URL_POOLED`). No new secret.
- It writes no Lance, holds no schedule, and is not invoked by the dispatcher.

## Forbidden / retired — do not reintroduce

- **Iceberg** tables and the **Polaris** REST catalog. The Gen-3 data plane
  writes Lance to R2 directly and needs neither.
- **`modal.Cron`** embedded in workers — cadence belongs to Trigger v4.
- **Per-feed Modal web endpoints** for dispatcher-routed compute — the Universal
  Dispatcher is the only one. Push-ingestion endpoints, where an external producer
  initiates the request, are documented in §6.

**Adding a feed:** a new feed earns a `src/trigger/<feed>.ts` task, a
domain-grouped worker under `pipelines/<domain>/`, and an `ops.*` runs table for
its terminal state — then it is wired through the same dispatcher by name. Zero
new endpoints, zero new secrets.

## Maintenance workers (reindex / cross-domain campaigns)

A worker that mutates *already-committed* datasets (rather than ingesting a feed)
is a maintenance worker. When it spans domains — e.g. a physical-indexing campaign
over the federal-spend + mortgage + crosswalk spines —
it lives under the closest cross-source domain (`pipelines/resolution/`), not
duplicated per feed. Reference: `pipelines/resolution/federal_spine_index_campaign.py`.

Building `BTREE` scalar indices on R2 Lance datasets — the two-tier rule:

- **Default — direct-R2, in place.** `lance.dataset(uri, storage_options=so)
  .create_scalar_index(col, "BTREE")` appends index files straight to R2. Read
  the column via range GETs, sort in-RAM (`LANCE_BYPASS_SPILLING=true`, 32–64 GiB),
  no scratch disk. This is the dominant fleet pattern and the first choice.
- **Giants — Volume-staged, append-only.** Once a single scalar-index
  `page_data.lance` is large enough (empirically ~100M+ rows on a load-bearing
  column), a direct-R2 write trips R2's "all non-trailing parts must have the same
  length" rule (`400 InvalidPart`) — object_store's adaptive multipart escalates
  part size mid-upload and R2 (unlike S3) rejects it. Lance exposes no part-size
  knob. Fix: stage the dataset to a **Modal Volume** (network storage — does NOT
  push the worker onto preemptible spot capacity the way a large `ephemeral_disk`
  request does), build the index on the local copy (local FS has no multipart
  rule), then upload **only the new files** (`_indices/<uuid>/`, the new
  `_versions/<n>.manifest`, `_transactions/*.txn`) via boto3 (uniform parts). Never
  wipe or re-upload data files. **Do not** reach for a large `ephemeral_disk`
  override — that is what forced the USAspending giant ingest onto spot capacity.

## Application layer (`apps/`) — read-only gateways

`apps/` holds long-running services that *read* the committed Gen-3
system-of-record and expose it to consumers. They are categorically distinct from
`pipelines/` (Modal workers that *ingest / materialize / mutate* the data plane):
a gateway never writes a dataset. They host **no** Gen-2 patterns — no Iceberg /
Polaris, no per-feed data-plane endpoints, no embedded cron — and read R2 with the
same credentials the workers use (`R2_ACCESS_KEY_ID` / `R2_SECRET_ACCESS_KEY` /
`R2_ENDPOINT`).

Reference: [`apps/gtm_mcp/`](apps/gtm_mcp/) — the unified **GTM MCP gateway**
(Render Web Service, Ohio). One `FastMCP` server (`mcp.server.fastmcp`) over the
SSE transport, exposing two access shapes over the same R2 sink: **Lance `BTREE`
index pushdown** for sub-100 ms point-lookups (`companies` / `people` / `awards`),
and **raw DuckDB ANSI SQL** (`execute_audience_query`) for cross-layer audience
segments. Name-matching reuses the canonical `core.name_norm` blocking key as a
DuckDB SQL literal — the gateway never re-implements a spine rule. Run from the
repo root: `python -m apps.gtm_mcp.main` (package `gtm_mcp` is underscored to be a
valid module path; the Render service is named `gtm-mcp`).
