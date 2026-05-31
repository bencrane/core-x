# Architecture — core-x

Source of truth for core-x, the Gen-3 data & compute plane. Every new
data-ingest / compute worker follows these patterns; deviations require updating
this file first.

core-x is a clean room. There is **no FastAPI / Railway application layer, no
Iceberg, no Polaris, and no `modal.Cron`.** The stack is exactly four layers: a
Trigger.dev v4 control plane, one proxy-authed Modal dispatcher, domain-grouped
Modal compute workers, and a DuckDB → LanceDB v2.0 → R2 data plane.

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
  **only** Modal app exposing a web endpoint, and that endpoint is
  **proxy-authenticated** (`requires_proxy_auth=True`; `Modal-Key` /
  `Modal-Secret`). One endpoint for the entire fleet, `MODAL_DISPATCHER_URL`,
  forever.
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
- Workers **do not expose web endpoints.** They are reachable only by the
  dispatcher's `spawn()` (or `modal run` for manual ops) and receive
  `trigger_callback_url` as a kwarg.

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

## Forbidden / retired — do not reintroduce

- **Iceberg** tables and the **Polaris** REST catalog. The Gen-3 data plane
  writes Lance to R2 directly and needs neither.
- **`modal.Cron`** embedded in workers — cadence belongs to Trigger v4.
- **Per-feed Modal web endpoints** — the Universal Dispatcher is the only one.

**Adding a feed:** a new feed earns a `src/trigger/<feed>.ts` task, a
domain-grouped worker under `pipelines/<domain>/`, and an `ops.*` runs table for
its terminal state — then it is wired through the same dispatcher by name. Zero
new endpoints, zero new secrets.
