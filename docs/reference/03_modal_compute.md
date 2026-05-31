# 03 · Modal Compute — The Serverless Execution Plane

The execution plane is where compute physically runs. It is a clean room. Modal hosts exactly two kinds of object: the **single proxy-authed Universal Dispatcher** ([`core/modal_dispatcher.py`](../../core/modal_dispatcher.py)) and a set of **domain-grouped, endpoint-less compute workers** under [`pipelines/<domain>/`](../../pipelines/). Nothing else lives here. Cadence, retries, and durable state belong to the Trigger.dev v4 control plane ([`04_trigger_orchestration.md`](04_trigger_orchestration.md)); transformation belongs to DuckDB ([`01_duckdb_processing.md`](01_duckdb_processing.md)); the system of record is LanceDB v2.0 on R2 ([`02_lancedb_storage.md`](02_lancedb_storage.md)). Modal is the muscle, not the brain.

This document is loaded verbatim as system truth. Every decorator, parameter, and import below is current as of Modal 1.x. Renamed and deprecated APIs are called out explicitly — write the current name, never the old one.

---

## 1. Role of the Execution Plane — A Clean Room

Modal in core-x runs compute and nothing else. The following are **forbidden** on the execution plane:

- **NO FastAPI / Railway application layer.** core-x has no long-lived web application. The only HTTP surface Modal exposes is the one dispatcher endpoint (§3). Workers expose **zero** endpoints.
- **NO `modal.Cron`.** Schedule lives **exclusively** in Trigger.dev v4 via `schedules.task({ cron })`. A worker that embeds `modal.Cron` is a defect — delete it. Cadence is owned by [`04_trigger_orchestration.md`](04_trigger_orchestration.md), full stop.
- **NO per-feed endpoints, NO per-feed secrets.** A new feed adds a worker function and a one-line Trigger task. It adds no new endpoint and no new proxy-auth pair. This is what kills env-var bloat.
- **NO Iceberg, NO Polaris.** The data plane writes Lance directly to R2 with no catalog round-trip. The execution plane never touches a REST catalog.

The contract (mirrors [`ARCHITECTURE.md`](../../ARCHITECTURE.md) §2–3): Trigger POSTs the dispatcher → the dispatcher `spawn()`s the named worker fire-and-forget and returns `202` → the worker streams source → DuckDB → Arrow → Lance, writes terminal state to `ops.*`, and POSTs the Trigger waitpoint callback. Modal holds no schedule, no connection, and no durable state.

### `modal.Cron` — forbidden, do not write it

`modal.Cron` (and `@app.function(schedule=...)`) is a real Modal API, but it is **banned in core-x**. Cadence is owned solely by Trigger.dev v4 (`schedules.task({ cron: { pattern, timezone } })`). A worker must never carry an embedded schedule. The real mechanism that satisfies "run this daily" is the Trigger schedule task in [`04_trigger_orchestration.md`](04_trigger_orchestration.md), which POSTs the dispatcher — not a Modal cron.

---

## 2. The Canonical `modal.Image` for Data Engineering

Every worker pins an image with `modal.Image.debian_slim(python_version="3.12")` and installs the data-plane toolchain. This is the exact base used by both the dispatcher and the SAM.gov worker.

```python
import modal

# Domain worker image — DuckDB → Arrow → Lance toolchain.
# Lower bounds, not exact pins — freeze with `modal shell` once validated.
image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.1",
    "lancedb>=0.15",
    "pylance>=0.19",        # provides `import lance`; lancedb does not re-export it
    "pyarrow>=17",
    "requests>=2.32",
    "psycopg[binary]>=3.2",  # terminal-state write to ops.*
)
```

```python
# Dispatcher image — fastapi_endpoint REQUIRES the 'standard' extra.
image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "fastapi[standard]>=0.115",
    "pydantic>=2",
)
```

`debian_slim(python_version=None, force_build=False)` is the canonical base. `python_version="3.12"` is mandatory — never let it default to the builder's Python.

### Verified builder-method reference

All builder methods are chainable and return a new `Image`.

| Method | Signature (current) | Use |
|---|---|---|
| `.pip_install` | `.pip_install(*packages, find_links=None, index_url=None, extra_index_url=None, pre=False, extra_options="", force_build=False, env=None, secrets=None, gpu=None)` | Install Python packages. The canonical data-engineering entrypoint. |
| `.uv_pip_install` | `.uv_pip_install(*packages, requirements=None, find_links=None, index_url=None, extra_index_url=None, pre=False, extra_options="", force_build=False, uv_version=None, env=None, secrets=None, gpu=None)` | uv-backed installer — faster resolver, drop-in for `.pip_install`. |
| `.pip_install_from_requirements` | `.pip_install_from_requirements(requirements_txt, find_links=None, *, index_url=None, extra_index_url=None, pre=False, extra_options="", force_build=False, env=None, secrets=None, gpu=None)` | Install from a `requirements.txt`. |
| `.apt_install` | `.apt_install(*packages, force_build=False, env=None, secrets=None, gpu=None)` | Install system (Debian) packages. |
| `.run_commands` | `.run_commands(*commands, env=None, secrets=None, volumes=None, gpu=None, force_build=False)` | Run arbitrary shell at build time. |
| `.env` | `.env(vars: dict[str, str])` | Set build/runtime env vars baked into the image. |

`uv_pip_install` is current and available; either it or `.pip_install` is acceptable for the data toolchain. A faster build of the worker image:

```python
image = modal.Image.debian_slim(python_version="3.12").uv_pip_install(
    "duckdb>=1.1", "lancedb>=0.15", "pylance>=0.19",
    "pyarrow>=17", "requests>=2.32", "psycopg[binary]>=3.2",
)
```

### Adding local code — the current canonical way

To make first-party core-x code importable inside the container, use `add_local_python_source`. For data/config files, use `add_local_dir` / `add_local_file`.

| Current method | Signature | Replaces (deprecated) |
|---|---|---|
| `image.add_local_python_source` | `add_local_python_source(*modules, copy=False, ignore=NON_PYTHON_FILES)` | `modal.Mount.from_local_python_packages` |
| `image.add_local_dir` | `add_local_dir(local_path, remote_path, *, copy=False, ignore=[])` | `Image.copy_local_dir` |
| `image.add_local_file` | `add_local_file(local_path, remote_path, *, copy=False)` | `Image.copy_local_file` |

```python
# Make the shared core-x package importable inside the worker container.
image = (
    modal.Image.debian_slim(python_version="3.12")
    .pip_install("duckdb>=1.1", "pylance>=0.19", "pyarrow>=17")
    .add_local_python_source("pipelines")     # importable by module name
    .add_local_dir("sql", "/root/sql")         # transform SQL files, etc.
)
```

Default behavior mounts at container start — fast, no rebuild. Pass `copy=True` **only** when a later build step (`run_commands`) must see the files.

### `modal.Mount` and `copy_local_*` — do not write them

`modal.Mount`, `Image.copy_local_dir`, and `Image.copy_local_file` are **deprecated** and removed from the recommended path in Modal 1.0. They must never appear in core-x. The real mechanism is the `add_local_*` family above. The migration is mechanical:

| Deprecated — never write | Current — write this |
|---|---|
| `modal.Mount.from_local_dir("data")` | `image.add_local_dir("data", "/root/data")` |
| `modal.Mount.from_local_python_packages("pipelines")` | `image.add_local_python_source("pipelines")` |
| `image.copy_local_file("x.sql", "/root/x.sql")` | `image.add_local_file("x.sql", "/root/x.sql")` |

---

## 3. The Universal Dispatcher — The Only Proxy-Authed Endpoint

[`core/modal_dispatcher.py`](../../core/modal_dispatcher.py), Modal app `universal-dispatcher`, is the **only** Modal app in the fleet that exposes a web endpoint, and that endpoint is **proxy-authenticated**. One endpoint (`MODAL_DISPATCHER_URL`), one proxy-auth pair (`MODAL_KEY` / `MODAL_SECRET`), for the entire fleet, forever.

It is a stateless router. It receives the Trigger payload `{app_name, function_name, kwargs, trigger_callback_url}`, resolves the target via `modal.Function.from_name(app_name, function_name)`, `spawn()`s it fire-and-forget, and returns `202`. It holds no connection and owns no state.

Mirror this file exactly:

```python
"""Universal Dispatcher — the single proxy-authed Modal entrypoint for the fleet.

ONE endpoint for every feed. Trigger.dev POSTs
``{app_name, function_name, kwargs, trigger_callback_url}``; the dispatcher
resolves the target Modal function by name, ``spawn()``s it (fire-and-forget,
the HTTP connection is never held open for the job), and returns 202 Accepted.
The spawned worker runs in the background and POSTs its terminal metadata to
``trigger_callback_url`` to wake the suspended Trigger run.

    modal deploy core/modal_dispatcher.py
"""

from __future__ import annotations

import modal
from pydantic import BaseModel

app = modal.App("universal-dispatcher")

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "fastapi[standard]>=0.115",
    "pydantic>=2",
)


class DispatchRequest(BaseModel):
    app_name: str
    function_name: str
    kwargs: dict = {}
    trigger_callback_url: str


@app.function(image=image)
@modal.fastapi_endpoint(method="POST", requires_proxy_auth=True, label="dispatch")
def dispatch(req: DispatchRequest):
    """Spawn the named worker and return immediately.

    NOTE on the spawn signature: the directive writes
    ``.spawn(kwargs, trigger_callback_url=...)``, but passing the dict
    positionally would bind it to the worker's first parameter and collide
    with the explicit ``trigger_callback_url`` keyword. The correct form
    spreads kwargs as keyword args — the worker receives its feed-specific
    parameters plus ``trigger_callback_url``.
    """
    from fastapi.responses import JSONResponse

    fn = modal.Function.from_name(req.app_name, req.function_name)
    call = fn.spawn(**req.kwargs, trigger_callback_url=req.trigger_callback_url)

    # 202 Accepted — work acknowledged, result delivered later via the callback.
    return JSONResponse(
        status_code=202,
        content={
            "accepted": True,
            "call_id": call.object_id,
            "app": req.app_name,
            "function": req.function_name,
        },
    )
```

### The endpoint decorator

`@modal.fastapi_endpoint` is the current decorator. Its keyword-only signature:

```python
@modal.fastapi_endpoint(
    *,
    method: str = "GET",
    label: Optional[str] = None,
    custom_domains: Optional[Iterable[str]] = None,
    docs: bool = False,
    requires_proxy_auth: bool = False,
)
```

| Param | Dispatcher value | Meaning |
|---|---|---|
| `method` | `"POST"` | HTTP verb. |
| `requires_proxy_auth` | `True` | Enforce `Modal-Key` / `Modal-Secret` headers; reject missing/invalid with `401`. |
| `label` | `"dispatch"` | Stable URL label → `https://<workspace>--dispatch.modal.run`. |
| `docs` | (omit) | `docs=True` would serve interactive OpenAPI/Swagger — not used. |

**Decorator order is load-bearing.** `@app.function` MUST be the **outermost** decorator with `@modal.fastapi_endpoint` stacked directly under it on the same function. Reversing them fails. `fastapi_endpoint` requires the `fastapi[standard]` extra in the image (a bare `fastapi` install fails at serve time) — the dispatcher image installs it.

### Cross-app invocation — `from_name` + `spawn`

| API | Signature | Behavior |
|---|---|---|
| `modal.Function.from_name` | `from_name(app_name, name, *, environment_name=None, client=None) -> Function` | Lazy handle to a **deployed** function. Second positional is the function `name`. |
| `fn.spawn` | `fn.spawn(*args, **kwargs) -> FunctionCall` | Fire-and-forget. Enqueues the call and returns immediately; does **not** hold the connection open for the job. |
| `fn.remote` | `fn.remote(*args, **kwargs) -> ReturnType` | Synchronous, **blocks** until the function returns. Used only by `local_entrypoint`. |
| `FunctionCall.object_id` | `-> str` | The `fc-...` call id, returned to Trigger as `call_id`. |
| `FunctionCall.from_id` | `from_id(function_call_id, client=None) -> FunctionCall` | Reconstruct a handle elsewhere; `.get(timeout=None, *, index=0)` retrieves the result. |

`spawn` is exactly the dispatcher's requirement: the HTTP request returns `202` without keeping a socket open for a 30-minute job. Use `spawn`, never `remote`, in the dispatcher.

### `spawn` arg-binding gotcha (verbatim from the source file)

Passing the kwargs dict **positionally** binds the whole dict to the worker's first parameter and collides with the explicit `trigger_callback_url` keyword. **Spread it.**

```python
# WRONG — binds the dict to the worker's first positional param.
call = fn.spawn(req.kwargs, trigger_callback_url=req.trigger_callback_url)

# CORRECT — spread kwargs; worker gets its feed params + trigger_callback_url.
call = fn.spawn(**req.kwargs, trigger_callback_url=req.trigger_callback_url)
```

### `from_name` resolves only DEPLOYED apps

`modal.Function.from_name` resolves a function **only** in a `modal deploy`-ed app. If the target worker was only `modal run` (ephemeral), the lookup fails and the dispatcher cannot spawn it. A worker MUST be `modal deploy`-ed before it is reachable. (`Function.lookup` is the deprecated predecessor of `from_name` — do not write `lookup`.)

### `@modal.web_endpoint` — renamed, do not write it

`@modal.web_endpoint` is **deprecated**. It was renamed to `@modal.fastapi_endpoint` in Modal 1.0 to make the FastAPI dependency explicit. The old name warns/breaks on current clients. Write `@modal.fastapi_endpoint` everywhere — never `@modal.web_endpoint`.

### Proxy auth — `Modal-Key` / `Modal-Secret`

A Proxy Auth Token is a `(Token ID, Token Secret)` pair minted in the **Modal dashboard → Settings → Proxy Auth Tokens** — **not** via CLI. The caller sends two headers: `Modal-Key` carries the Token ID (`wk-...`); `Modal-Secret` carries the Token Secret (`ws-...`). `requires_proxy_auth=True` enforces this; missing/invalid credentials get `401`. The dispatcher is the **only** proxy-authed surface; Trigger.dev holds the `wk-` / `ws-` pair and is the only caller.

This is **distinct** from `modal token new` CLI auth, which authenticates the SDK to the platform — not HTTP callers. Mixing them yields `401`s.

```bash
# Token pair minted in the Modal dashboard: Settings → Proxy Auth Tokens.
# Modal-Key carries the Token ID (wk-...); Modal-Secret carries the Token Secret (ws-...).
export MODAL_KEY=wk-xxxxxxxxxxxx
export MODAL_SECRET=ws-xxxxxxxxxxxx

curl -X POST \
  -H "Modal-Key: $MODAL_KEY" \
  -H "Modal-Secret: $MODAL_SECRET" \
  -H "Content-Type: application/json" \
  -d '{"app_name":"sam-gov-pipelines","function_name":"ingest_sam_opps_bulk","kwargs":{"mode":"overwrite"},"trigger_callback_url":"https://api.trigger.dev/.../waitpoint/token/..."}' \
  https://<workspace>--dispatch.modal.run
```

In production the caller is never `curl` — it is the Trigger v4 task, which mints `trigger_callback_url` from a waitpoint token (`wait.createToken().url`) and sends the same two headers. See [`04_trigger_orchestration.md`](04_trigger_orchestration.md).

---

## 4. Domain-Grouped Compute Workers

Modal apps are grouped **strictly by domain**: `app = modal.App("sam-gov-pipelines")`. Workers live under `pipelines/<domain>/` — e.g. [`pipelines/sam_gov/sam_opps_bulk.py`](../../pipelines/sam_gov/sam_opps_bulk.py). **All new compute workers MUST be placed in a domain-specific subdirectory under `pipelines/`.** Workers **never** expose a web endpoint. They are reachable only by the dispatcher's `spawn()` (or `modal run` for manual ops) and receive `trigger_callback_url` as a kwarg.

The SAM.gov worker is the reference every feed is built against. Its resource config:

```python
app = modal.App("sam-gov-pipelines", image=image)


@app.function(
    secrets=[
        modal.Secret.from_name("r2-credentials"),
        modal.Secret.from_name("hqx-postgres"),
    ],
    timeout=60 * 30,            # 30-min ceiling for the bulk job
    memory=8192,
    cpu=4.0,
)
def ingest_sam_opps_bulk(
    trigger_callback_url: str | None = None,
    csv_url: str | None = None,
    mode: str = "overwrite",
) -> dict:
    ...
```

### `@app.function` resource parameters

| Param | Type | Worker value | Meaning |
|---|---|---|---|
| `image` | `Image` | set on `App` | The pinned image from §2. |
| `secrets` | `Collection[Secret]` | `[Secret.from_name("r2-credentials"), Secret.from_name("hqx-postgres")]` | Injected as env vars at runtime. |
| `timeout` | `int` (seconds) | `60 * 30` | Max execution + startup. |
| `memory` | `int \| tuple[int,int]` (MiB) | `8192` | Request (or `(request, limit)`). Size to peak Arrow table — see §5. |
| `cpu` | `float \| tuple[float,float]` | `4.0` | Fractional cores. |
| `gpu` | `str \| list[str]` | (omit) | Not used in the data plane. |

`modal.Secret.from_name(name, *, environment_name=None, required_keys=[])` resolves a named secret created in the Modal dashboard. The SAM.gov worker attaches `r2-credentials` (R2 object-store creds → `_r2_storage_options()`) and `hqx-postgres` (`HQX_DB_URL_POOLED` → `ops.*` write).

### Autoscaling — current parameter names only

The worker omits autoscaling, so it **scales to zero** — correct for a spawn-only worker. To tune, use the **current** names. The old names were **removed** in Modal 1.0 (not merely deprecated) and **raise** if passed.

| Current (write this) | Removed — never write | Meaning |
|---|---|---|
| `min_containers` | `keep_warm` | Warm-pool floor. |
| `max_containers` | `concurrency_limit` | Ceiling on concurrent containers. |
| `scaledown_window` | `container_idle_timeout` | Seconds an idle container survives before scaledown. |
| `buffer_containers` | `_experimental_buffer_containers` | Extra idle headroom under load. |

```python
@app.function(
    secrets=[modal.Secret.from_name("r2-credentials")],
    timeout=60 * 30,
    memory=8192,
    cpu=4.0,
    min_containers=0,          # NOT keep_warm
    max_containers=4,          # NOT concurrency_limit
    scaledown_window=60,       # NOT container_idle_timeout
)
def ingest_sam_opps_bulk(...): ...
```

### Manual ops — `@app.local_entrypoint`

`@app.local_entrypoint()` marks the function that runs **locally** (on the operator's machine, not in a container) when the file is `modal run`. It orchestrates remote calls via `.remote()`. Keep it to orchestration — never heavy compute. The reference worker:

```python
@app.local_entrypoint()
def main(mode: str = "overwrite") -> None:
    # Manual run: no callback URL (callback is skipped); ops.* write still
    # fires if the hqx-postgres secret is attached.
    print(ingest_sam_opps_bulk.remote(trigger_callback_url=None, mode=mode))
```

The manual path passes `trigger_callback_url=None` — there is no Trigger run to wake. The terminal-state write to `ops.*` still fires. The full terminal contract (Postgres `ops.*` write via psycopg + waitpoint callback POST) is owned by the worker; see [`ARCHITECTURE.md`](../../ARCHITECTURE.md) §5 and [`04_trigger_orchestration.md`](04_trigger_orchestration.md).

---

## 5. The Flat-RAM Law — Arrow Is the Only In-Memory Interchange

**Apache Arrow is the only permitted in-memory interchange between DuckDB and Lance.** DuckDB's `Result.to_arrow_table()` streams column-chunk `RecordBatch`es across the Arrow C Data Interface into a `pyarrow.Table` — no per-row Python objects, no dict-of-dicts. `lance.write_dataset` accepts that `pyarrow.Table` directly, so the columnar buffers flow straight into the writer. Peak RSS is dominated by the single Arrow table, not Python object overhead. The container memory profile stays **flat** regardless of dataset shape.

### pandas is forbidden in the DuckDB → Lance chain

**NEVER** call `.df()`, `.fetchdf()`, or any pandas conversion in the transform path. pandas forces a row/block-managed copy, inflates RSS, and breaks the flat columnar profile. Likewise **NEVER** materialize nested-dict intermediates or write per-row Python loops over the result. The only legal chain is `to_arrow_table()` / `to_arrow_reader()` → `lance.write_dataset` (the deprecated `fetch_arrow_table()` / `fetch_record_batch()` still run but are on the removal track — see [`01_duckdb_processing.md`](01_duckdb_processing.md) §4).

The reference worker's flat-RAM core (canonical shape; the on-disk worker still calls the deprecated `.fetch_arrow_table()` — see [`01_duckdb_processing.md`](01_duckdb_processing.md)):

```python
import duckdb
import lance

con = duckdb.connect(":memory:")
try:
    con.execute("PRAGMA threads=4;")
    # Zero-copy: DuckDB streams RecordBatches over the Arrow C Data Interface
    # into a pyarrow.Table. No pandas, no per-row Python, no nested dicts.
    arrow_table = con.execute(sql).to_arrow_table()
finally:
    con.close()

# Arrow buffers flow straight into Lance — no intermediate DataFrame.
lance.write_dataset(
    arrow_table,
    LANCE_BASE_URI,                       # s3://sam-gov-opps/active/ (R2)
    mode=mode,                            # "overwrite" canonical for daily snapshot
    data_storage_version="2.0",           # repo pins "2.0"; "2.1" is the current default — see 02
    storage_options=_r2_storage_options(),
)
```

R2 storage options are sourced from the `r2-credentials` Modal secret (AWS-style creds + explicit `endpoint` and `region="auto"`), exactly as [`pipelines/sam_gov/sam_opps_bulk.py`](../../pipelines/sam_gov/sam_opps_bulk.py) does:

```python
def _r2_storage_options() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID in the Modal secret.")
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": endpoint,
        "region": "auto",
    }
```

`data_storage_version` MUST be pinned explicitly — never rely on the SDK default. The repo pins `"2.0"`; the current Lance default is `"2.1"` (one generation newer, fully read/write-interoperable). The `"2.0"`-vs-`"2.1"` reconciliation, Lance internals, and indexing are owned by [`02_lancedb_storage.md`](02_lancedb_storage.md).

### Sizing `memory=` and the streaming variant

`to_arrow_table()` materializes the **entire** result in RAM as one `pyarrow.Table`. Size `memory=` (MiB) to roughly **peak Arrow table size × a safety factor** (DuckDB scan buffers + writer staging). The reference worker pins `memory=8192`, which holds the full SAM.gov active extract.

For results near or over container RAM, switch to `to_arrow_reader(batch_size=...)` — a `pyarrow.RecordBatchReader` that streams batches lazily. Peak RSS becomes one batch, so `memory=` can be pinned far below the full table:

```python
import duckdb
import lance

con = duckdb.connect(":memory:")
con.execute("PRAGMA threads=4;")

# RecordBatchReader streams columnar batches over the Arrow C Data Interface —
# peak RSS is one batch, not the whole result. pandas never enters.
reader = con.execute(sql).to_arrow_reader(batch_size=131_072)

lance.write_dataset(
    reader,                       # pyarrow.RecordBatchReader
    "s3://sam-gov-opps/active/",
    schema=reader.schema,         # REQUIRED when data is a batch iterator/reader
    mode="overwrite",
    data_storage_version="2.0",
    max_rows_per_file=1_000_000,
    storage_options=_r2_storage_options(),
)
```

`schema=` is **mandatory** with a `RecordBatchReader` / batch iterator — the writer cannot infer it from a streaming source. For a single in-memory `pyarrow.Table` the schema is inferred and may be omitted.

| Result size vs container RAM | Read method | `memory=` | `schema=` on `write_dataset` |
|---|---|---|---|
| Comfortably fits | `con.execute(sql).to_arrow_table()` | ≈ peak table × safety | omit (inferred) |
| Near or over RAM | `con.execute(sql).to_arrow_reader(batch_size=...)` | pin low (per-batch RSS) | **required** (`reader.schema`) |

---

## 6. Lifecycle — `modal deploy` and `modal run`

| Command | App lifetime | Effect |
|---|---|---|
| `modal deploy <file.py>` | **Persistent** | Creates a persistent app, prerequisite for `Function.from_name`. Required for the dispatcher and **every** worker the dispatcher spawns. |
| `modal run <file.py>[::entrypoint] [--arg val]` | **Ephemeral** | One-off; runs `@app.local_entrypoint` **locally**. Manual ops only — the dispatcher cannot resolve an ephemeral app. |

```bash
# Persist the single dispatcher endpoint (MODAL_DISPATCHER_URL).
modal deploy core/modal_dispatcher.py

# Persist a worker so the dispatcher can resolve + spawn it by name.
modal deploy pipelines/sam_gov/sam_opps_bulk.py

# Manual run (local_entrypoint, no callback URL) — ad-hoc ops / validation.
modal run pipelines/sam_gov/sam_opps_bulk.py

# Manual run with an arg, mapped to the local_entrypoint's `mode` param.
modal run pipelines/sam_gov/sam_opps_bulk.py --mode overwrite
```

A worker reachable in production MUST be `modal deploy`-ed — `modal run` alone leaves it ephemeral and unresolvable, and the dispatcher's `from_name` will fail. `modal serve` gives a hot-reloading ephemeral dev deployment (its label gets a `-dev` suffix); it is a dev convenience, never the production path.

**Adding a feed** (mirrors [`ARCHITECTURE.md`](../../ARCHITECTURE.md)): a domain-grouped worker under `pipelines/<domain>/`, `modal deploy`-ed; a one-line `src/trigger/<feed>.ts` task ([`04_trigger_orchestration.md`](04_trigger_orchestration.md)); and an `ops.*` runs table for terminal state. It is then wired through the same dispatcher **by name** — zero new endpoints, zero new secrets.
