# 04 · Trigger.dev v4 — The Event & Control Plane

Source of truth for the core-x control plane. Trigger.dev v4 owns cadence and
durable orchestration for the entire fleet. Every feed's schedule, every
external-event suspension, and every real-time read of run state flows through
the patterns in this document. Deviations require updating
[`ARCHITECTURE.md`](../../ARCHITECTURE.md) first.

This file is loaded VERBATIM as system context. Every API name, parameter,
import path, and decorator below is exact per the verified SDK surface
(`@trigger.dev/sdk` `^4.4.6`, CLI `trigger.dev@4.4.4`). A wrong API name here
makes every downstream agent write broken code. Mirror these snippets exactly.

Sibling references:
[`01_duckdb_processing.md`](01_duckdb_processing.md) ·
[`02_lancedb_storage.md`](02_lancedb_storage.md) ·
[`03_modal_compute.md`](03_modal_compute.md)

Canonical implementations mirrored throughout:
[`src/trigger/sam_opps_bulk.ts`](../../src/trigger/sam_opps_bulk.ts),
[`trigger.config.ts`](../../trigger.config.ts),
[`core/modal_dispatcher.py`](../../core/modal_dispatcher.py),
[`pipelines/sam_gov/sam_opps_bulk.py`](../../pipelines/sam_gov/sam_opps_bulk.py).

---

## 1. Role of the control plane

Trigger.dev v4 owns cadence **exclusively**. It is the only system in core-x
permitted to decide when work runs. Every other layer is reactive: the Universal
Dispatcher routes on demand, the Modal workers compute on demand, the data plane
writes on demand. Time is Trigger's concern alone.

- **Cadence is declared in-code**, never in a control panel and never in a
  worker. A feed's schedule lives in its `src/trigger/<feed>.ts` task via
  `schedules.task({ cron })`. The cron string is committed to the repo; it is
  reviewable, diffable, and version-controlled like any other code.
- **Tasks live in `src/trigger/`.** [`trigger.config.ts`](../../trigger.config.ts)
  pins `dirs: ["./src/trigger"]`; the deploy bundler discovers exactly the tasks
  in that directory. A task placed anywhere else is invisible to the platform.
- **`modal.Cron` is strictly forbidden.** No Modal worker carries an embedded
  schedule. A worker that schedules itself has stolen cadence from the control
  plane and fractured the single source of timing truth. Cadence belongs to
  Trigger v4, full stop. See the forbidden list in
  [`ARCHITECTURE.md` §1](../../ARCHITECTURE.md).

> ### Does not exist — do not write
> **`modal.Cron`** is not part of core-x. Modal supports it; core-x bans it.
> If a feed needs a schedule, it earns a `schedules.task` in `src/trigger/`,
> never a `modal.Cron` decorator on the worker. Scheduling on the worker is the
> single most common architectural regression — reject it on sight.

The contract between control plane and compute is one POST to the Universal
Dispatcher (§2 of [`ARCHITECTURE.md`](../../ARCHITECTURE.md),
[`core/modal_dispatcher.py`](../../core/modal_dispatcher.py)) carrying
`{ app_name, function_name, kwargs, trigger_callback_url }`. The dispatcher
`spawn()`s the named worker fire-and-forget and returns `202`. Trigger never
holds the HTTP connection open for the job — it suspends on a waitpoint and is
woken by the worker's terminal callback (§4).

---

## 2. Project configuration

Two import surfaces exist and they are not interchangeable. Memorize the split.

| Symbol | Import path | Used in |
|---|---|---|
| `defineConfig` | `@trigger.dev/sdk` | [`trigger.config.ts`](../../trigger.config.ts) |
| `syncEnvVars` | `@trigger.dev/build/extensions/core` | [`trigger.config.ts`](../../trigger.config.ts) |
| `schedules`, `wait`, `logger`, `task`, `tasks`, `batch`, `auth`, `runs` | `@trigger.dev/sdk` | every file in `src/trigger/` |
| `useRealtimeRun`, `useRealtimeRunsWithTag`, `useRealtimeBatch` | `@trigger.dev/react-hooks` | external Command Center client (§6) |

> ### Migrate off the deprecated import path
> The current [`trigger.config.ts`](../../trigger.config.ts) imports `defineConfig`
> from **`@trigger.dev/sdk/v3`**. In v4 that subpath is **officially deprecated**
> — the docs state verbatim *"This still works, but will be removed in a future
> version."* The v4-canonical source is **`@trigger.dev/sdk`** (NO `/v3`). It
> compiles today but is on the removal track. New code MUST import `defineConfig`
> from `@trigger.dev/sdk`. The `/v3` subpath is forbidden in anything new.

### 2.1 `trigger.config.ts` shape

`defineConfig` accepts `project`, `runtime`, `logLevel`, `maxDuration`,
`retries`, `dirs`, and `build`. Every key below is valid.

```typescript
// trigger.config.ts — v4-canonical (defineConfig from @trigger.dev/sdk, NOT /v3)
import { defineConfig } from "@trigger.dev/sdk";
import { syncEnvVars } from "@trigger.dev/build/extensions/core";

export default defineConfig({
  project: "proj_pakdcffjbeiwcixcoepb",
  runtime: "node",
  logLevel: "log",
  // Max COMPUTE seconds a task may run. Time suspended on a waitpoint or
  // wait.for/until does NOT count against this (see §4, §5). Override per-task.
  maxDuration: 3600,
  retries: {
    enabledInDev: true,
    default: {
      maxAttempts: 3,
      minTimeoutInMs: 1000,
      maxTimeoutInMs: 10000,
      factor: 2,
      randomize: true,
    },
  },
  dirs: ["./src/trigger"],
  build: {
    // Forward the Universal Dispatcher's proxy-auth credentials into the
    // deployed environment at deploy time. Read from the deploy-time process
    // env; NEVER committed to the repo.
    extensions: [
      syncEnvVars(() =>
        ["MODAL_DISPATCHER_URL", "MODAL_KEY", "MODAL_SECRET"]
          .filter((k) => process.env[k])
          .map((k) => ({ name: k, value: process.env[k] as string })),
      ),
    ],
  },
});
```

| Key | Meaning |
|---|---|
| `project` | Trigger project ref. Immutable per project. |
| `runtime` | `"node"`. The container runtime for tasks. |
| `logLevel` | `"log"`. Verbosity of the platform logger. |
| `maxDuration` | Compute-seconds ceiling. NOT wall-clock. Suspended time is free. |
| `retries.enabledInDev` | Retries fire in the dev environment too. |
| `retries.default` | `maxAttempts`, `minTimeoutInMs`, `maxTimeoutInMs`, `factor`, `randomize` — the exponential-backoff envelope applied to every task unless overridden. |
| `dirs` | `["./src/trigger"]`. The only directory scanned for tasks. |
| `build.extensions` | Build-time hooks. Holds `syncEnvVars`. |

### 2.2 `syncEnvVars` — credential forwarding at deploy time

The `syncEnvVars` build extension imports from
`@trigger.dev/build/extensions/core` and is placed inside `build.extensions`. It
forwards the three Universal-Dispatcher secrets — `MODAL_DISPATCHER_URL`,
`MODAL_KEY`, `MODAL_SECRET` — from the deploy-time process env into the deployed
Trigger environment. This is the entire reason a feed needs **zero new secrets**:
the proxy-auth pair is forwarded once, and every task reaches the dispatcher with
it.

Credentials are supplied at deploy time from Doppler, never committed:

```bash
# Deploy with the dispatcher proxy-auth pair injected from Doppler.
doppler run -- npx trigger.dev@4.4.4 deploy
```

The `filter((k) => process.env[k])` guard means a missing var is silently
skipped rather than forwarded as `undefined` — deploy from a shell where all
three are present (the `doppler run --` wrapper guarantees this).

---

## 3. Scheduled tasks — `schedules.task`

A feed's cadence is one declarative `schedules.task`. The signature is exact:

```typescript
schedules.task({
  id: string,
  cron: string | { pattern: string; timezone: string; environments?: string[] },
  maxDuration?: number,
  ttl?: string,
  run: async (payload, { ctx }) => any,
});
```

| Field | Rule |
|---|---|
| `id` | Stable unique task id, e.g. `"sam-opps-bulk-dispatcher"`. The handle the platform schedules and the Command Center subscribes to. |
| `cron` | A bare 5-field string is interpreted as **UTC**. The object form `{ pattern, timezone }` pins an IANA timezone explicitly — **use the object form** to remove all ambiguity. |
| `maxDuration` | Per-task compute-seconds override. Does NOT bound suspended waitpoint time (§4). |
| `run` | `run(payload, { ctx })` — **`ctx` is nested inside the second argument**, not the second argument itself. |

> ### The `run` signature is `run(payload, { ctx })`
> Destructuring as `run(payload, ctx)` and then reading `ctx.run.id` is **wrong**
> — `ctx` is a property of the second argument. The canonical task uses
> `run: async (_payload, { ctx })` and reads `ctx.run.id`. Mirror it exactly.

The scheduled payload carries `{ timestamp: Date, lastTimestamp?: Date,
timezone: string, scheduleId: string, externalId?: string, upcoming: Date[] }`.
The SAM.gov dispatcher ignores it (`_payload`) because its trigger is purely the
clock.

This is the **declarative** form — cadence committed in code. The imperative
alternative (`schedules.create()` at runtime) is not used in core-x; schedules
are code, reviewed in PRs, not created out-of-band.

```typescript
// Cadence declared in-code. Daily 12:00 UTC. timezone pinned via object form.
export const samOppsBulkDispatcher = schedules.task({
  id: "sam-opps-bulk-dispatcher",
  cron: { pattern: "0 12 * * *", timezone: "UTC" },
  maxDuration: 3900,
  run: async (_payload, { ctx }) => {
    /* mint token → dispatch → suspend → resume — see §4 */
  },
});
```

> A bare cron string carries no timezone and defaults to UTC. The object form
> `{ pattern, timezone: "UTC" }` makes that explicit and unambiguous — the
> canonical task pins it deliberately. Always pin the timezone.

---

## 4. Durable async workflow via waitpoint tokens

This is the core pattern of the entire control plane. A scheduled task fires the
Universal Dispatcher, then **suspends** on a waitpoint token until the Modal
worker POSTs its terminal result back. While suspended the run is checkpointed:
it consumes **zero compute** and is **immune to HTTP timeouts**. The Modal job
may run for thirty minutes; the Trigger run holds no connection for any of it.

Mirror [`src/trigger/sam_opps_bulk.ts`](../../src/trigger/sam_opps_bulk.ts) exactly.

### 4.1 The four-step lifecycle

1. **Mint** a callback token with `wait.createToken({ timeout, tags })`. It
   returns `{ id, url, isCached, publicAccessToken }`. The `url` is a pre-signed
   HTTP callback of the form
   `https://api.trigger.dev/api/v1/waitpoints/tokens/{id}/callback/{callbackHash}`.
   The `callbackHash` embedded in the path **is the auth** — completing it needs
   **no API key**.
2. **Dispatch.** POST the Universal Dispatcher with the worker coordinates and
   `trigger_callback_url: token.url`.
3. **Suspend** on `await wait.forToken<T>(token.id)`. The run checkpoints. Zero
   compute until the callback arrives or the token times out.
4. **Resume.** The Modal worker's POST body becomes `result.output`. Inspect it
   and resolve or fail the run.

### 4.2 `wait.createToken` return surface

| Field | Type | Meaning |
|---|---|---|
| `id` | `string` (`"waitpoint_…"`) | Token id, passed to `wait.forToken`. |
| `url` | `string` | Pre-signed callback URL. Hand this to the dispatcher as `trigger_callback_url`. The whole POST body to it becomes the run output. |
| `isCached` | `boolean` | Idempotency-hit flag (when `idempotencyKey` is used). |
| `publicAccessToken` | `string` | Waitpoint-scoped JWT for browser-side completion. |

`wait.createToken` accepts `{ timeout?, tags?, idempotencyKey?,
idempotencyKeyTTL? }`. **`timeout` defaults to `"10m"` if omitted** — for a long
Modal job that is far too short. The canonical task sets `timeout: "1h"`
explicitly. The suspended wait costs no compute, so a generous timeout is cheap;
but if the worker dies silently, the run only fails after the token timeout
elapses. Set it deliberately.

### 4.3 `wait.forToken` result surface

`wait.forToken<T>(tokenId)` returns `{ ok: boolean, output?: T, error?: Error }`
and exposes `.unwrap()` to throw on timeout.

> **The only failure mode of `wait.forToken` is a TIMEOUT.** `result.ok === false`
> means the token timed out before any callback arrived — `result.error` is that
> timeout. A successful HTTP callback that carries a business **error payload**
> still returns `ok: true` with that payload in `output`. **`ok: true` does NOT
> mean business-success.** Always inspect `result.output.status` yourself. The
> canonical task does both checks.

### 4.4 Canonical task — `src/trigger/sam_opps_bulk.ts`

```typescript
import { schedules, wait, logger } from "@trigger.dev/sdk";

// The body Modal POSTs to the waitpoint url becomes this run's output.
interface IngestCallback {
  status: "success" | "error";
  rows: number;
  feed: string;
}

export const samOppsBulkDispatcher = schedules.task({
  id: "sam-opps-bulk-dispatcher",
  cron: { pattern: "0 12 * * *", timezone: "UTC" },
  // Generous cap; the durable wait itself consumes no compute while suspended.
  maxDuration: 3900,
  run: async (_payload, { ctx }) => {
    // 1) Mint the durable callback token. token.url is a pre-signed HTTP
    //    callback — the callbackHash in the URL authenticates; no API key.
    const token = await wait.createToken({
      timeout: "1h",
      tags: ["sam-opps-active", "modal-dispatch"],
    });

    // 2) Fire the Universal Dispatcher and return immediately (202). The
    //    worker runs in Modal; this task does not hold the connection.
    const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Modal-Key": requireEnv("MODAL_KEY"),
        "Modal-Secret": requireEnv("MODAL_SECRET"),
      },
      body: JSON.stringify({
        app_name: "sam-gov-pipelines",
        function_name: "ingest_sam_opps_bulk",
        kwargs: {},
        trigger_callback_url: token.url,
      }),
    });

    if (!res.ok) {
      const body = await res.text();
      throw new Error(`dispatcher ${res.status}: ${body.slice(0, 500)}`);
    }

    logger.info("Dispatched to Modal; suspending on waitpoint", {
      tokenId: token.id,
      triggerRunId: ctx.run.id,
    });

    // 3) Suspend until Modal POSTs the callback url. 4) Resolve from it.
    const result = await wait.forToken<IngestCallback>(token.id);

    // result.ok === false ONLY on token timeout (no callback arrived).
    if (!result.ok) {
      throw new Error(`SAM opps ingest timed out before Modal callback (token ${token.id})`);
    }
    // ok:true still carries the worker's payload — inspect business status.
    if (result.output.status !== "success") {
      throw new Error(`SAM opps ingest failed in Modal: ${JSON.stringify(result.output)}`);
    }

    logger.info("SAM opps ingest complete", { ...result.output });
    return result.output;
  },
});

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required env var: ${name}`);
  }
  return value;
}
```

### 4.5 The Modal-side completion contract — RAW body, no envelope

The pre-signed callback `url` (`token.url`) consumes a **RAW JSON body** — the
**entire body becomes `result.output`**. The worker
([`pipelines/sam_gov/sam_opps_bulk.py`](../../pipelines/sam_gov/sam_opps_bulk.py))
POSTs the terminal `{ status, rows, feed }` dict directly. Mirror its
`_post_callback`:

```python
import requests

# trigger_callback_url is token.url, passed in by the dispatcher task as a kwarg.
# POST the RAW terminal payload. The whole body becomes result.output in
# wait.forToken. NO API key (the callbackHash in the url path is the auth) and
# NO { "data": ... } wrapper. Completing an already-completed token is a no-op
# returning success — retries are safe.
def _post_callback(url: str | None, payload: dict, attempts: int = 3) -> None:
    if not url:
        print("No trigger_callback_url (manual run); skipping callback.")
        return
    import time

    for i in range(attempts):
        try:
            resp = requests.post(url, json=payload, timeout=30)
            if resp.status_code < 300:
                print(f"Callback delivered: {payload}")
                return
        except Exception as exc:  # noqa: BLE001
            print(f"Callback attempt {i + 1} failed: {exc}")
        time.sleep(2 * (i + 1))
    print(f"WARN: callback delivery failed after {attempts} attempts → {url}")
```

> ### Body-shape law — RAW for `token.url`, `{ data }` only for the management endpoint
> Two distinct completion paths exist and they take **different body shapes**:
> - The **pre-signed callback url** (`token.url`, used by Modal) takes the
>   **RAW** body — the whole body IS the output.
> - The **generic management endpoint** `POST /waitpoints/tokens/{id}/complete`
>   wraps it as `{ "data": { … } }`.
>
> Modal POSTs to `token.url`, so it MUST send the raw `IngestCallback` object
> with **NO `{ data }` wrapper**. Wrapping it would make `result.output` equal
> `{ data: {…} }`, the `result.output.status` check would read `undefined`, and
> the run would always fail. This is the single most dangerous mismatch in the
> pattern.

### 4.6 SDK-side completion — `wait.completeToken`

When completing a token from inside TypeScript task code (not from Modal),
`wait.completeToken(tokenOrId, data)` exists; the **second arg is the raw output
object directly**:

```typescript
import { wait } from "@trigger.dev/sdk";
// SDK completion: raw data as the 2nd arg (the SDK adds no wrapper for you here).
await wait.completeToken(token.id, { status: "success", rows: 12_843, feed: "sam_opps_active" });
```

> ### Does not exist — do not write
> **`wait.forRequest()` is not part of the Trigger.dev v4 API.** It does not
> exist in the SDK surface. The durable HTTP-callback pattern is
> **`wait.createToken`** (mint the url) + **`wait.forToken`** (suspend), exactly
> as the repo does. Any agent reaching for `wait.forRequest` is hallucinating —
> the real mechanism is the two-method token flow above. See
> [`ARCHITECTURE.md` §1](../../ARCHITECTURE.md), which names this nonexistent API
> explicitly.

---

## 5. Durable delays — `wait.for` and `wait.until`

When a flow must pause for a fixed interval or until an absolute instant, the two
real durable-delay primitives are `wait.for` and `wait.until`. Both checkpoint
the run when the wait exceeds ~5 seconds; while suspended the run consumes **no
compute**. This is the official cost-reduction guidance — a long pause is free.

```typescript
await wait.for({ minutes: 5 });                          // duration object
await wait.for({ seconds: 30 }, { idempotencyKey: "k" }); // optional 2nd arg
await wait.until({ date: new Date(ts), throwIfInThePast: true });
```

| Primitive | Signature | Notes |
|---|---|---|
| `wait.for` | `wait.for({ seconds \| minutes \| hours \| days \| weeks \| months \| years }, { idempotencyKey?, idempotencyKeyTTL? })` | First arg is a **duration object**; any of the seven keys. Second arg is optional idempotency options. |
| `wait.until` | `wait.until({ date: Date, throwIfInThePast?: boolean, idempotencyKey?, idempotencyKeyTTL? })` | **`date` is a KEY inside the options object**, not a positional argument. |

> **`wait.until` takes a single options object** — `wait.until({ date })`, never
> `wait.until(new Date(...))`. Passing a bare `Date` is wrong.

### 5.1 Durable polling / countdown loop

A loop that polls an external state between durable waits is a **plain JS loop**
calling `wait.for` / `wait.until`. Each wait > 5 s checkpoints the run, so even a
sixty-iteration poll costs nothing while suspended.

```typescript
import { task, wait, logger } from "@trigger.dev/sdk";

// Durable poll loop for a feed whose readiness must be observed externally.
// Each wait checkpoints the run -> zero compute while suspended.
export const samFeedReadinessPoll = task({
  id: "sam-feed-readiness-poll",
  run: async (payload: { deadline: string }) => {
    // Option A — fixed-interval poll until the upstream snapshot is published.
    for (let i = 0; i < 60; i++) {
      const status = await checkSamSnapshotPublished();
      if (status.published) {
        logger.info("SAM snapshot published; proceeding", { attempt: i });
        return status;
      }
      await wait.for({ minutes: 1 });
    }
    // Option B — sleep precisely until an absolute instant, then check once.
    await wait.until({ date: new Date(payload.deadline), throwIfInThePast: true });
    return await checkSamSnapshotPublished();
  },
});

async function checkSamSnapshotPublished(): Promise<{ published: boolean }> {
  // probe SAM.gov fileextractservices availability — read-only, no transform.
  return { published: false };
}
```

> ### Does not exist — do not write
> **"Countdown delayer loop" is NOT a named Trigger.dev primitive.** There is no
> such API in the SDK or docs. The intent is satisfied by a plain loop over
> `wait.for` / `wait.until`, as above. Do not reference "countdown delayer" as
> though it were a callable.
>
> **Prefer waitpoint tokens over poll loops for external-event-driven
> resumption.** When completion is signalled by an external worker — the SAM.gov
> dispatcher pattern (§4) — use `wait.forToken`, not a poll loop. The worker
> POSTs the callback the instant it finishes; the run resumes immediately with
> zero wasted iterations. The docs explicitly recommend waitpoints / `wait.for`
> / `wait.until` / `triggerAndWait` **instead of** polling. A poll loop is the
> fallback only when nothing external can signal completion.

### 5.2 `maxDuration` does not bound a wait

`maxDuration` is a **compute-seconds** cap. Time spent suspended on a waitpoint,
`wait.for`, or `wait.until` does **not** count against it — waits > 5 s
checkpoint and free the compute. So `maxDuration: 3900` does **not** bound the
1-hour waitpoint window in §4; the **token timeout** does. Never reach for a
larger `maxDuration` to widen a waitpoint window — raise the token `timeout`
instead.

---

## 6. Zero-server real-time client tracking

The external Command Center reads run, tag, and batch state **with no app server
in the read loop**. The backend mints a read-scoped public token once and hands
it to the browser; the browser subscribes directly to Trigger's Realtime API.

### 6.1 Backend — mint a scoped public token

`auth.createPublicToken` (from `@trigger.dev/sdk`) takes `{ scopes?: { read?: {
runs?: string[], tasks?: string[], tags?: string[], batch?: string } },
expirationTime?: string }`. `read.runs` / `tasks` / `tags` take **arrays**;
`read.batch` takes a **single** batch-id string.

```typescript
// backend (Next.js route / server action): mint a read-scoped public token.
import { auth, runs } from "@trigger.dev/sdk";

export async function mintCommandCenterToken(runId: string, batchId?: string) {
  return auth.createPublicToken({
    scopes: {
      read: {
        runs: [runId],
        tags: ["sam-opps-active"],
        ...(batchId ? { batch: batchId } : {}),
      },
    },
    expirationTime: "1h",
  });
}

// Optional pure-backend tail (no client at all): an async iterator over the run.
export async function tailRun(runId: string) {
  for await (const run of runs.subscribeToRun(runId)) {
    if (run.status === "COMPLETED" || run.status === "FAILED") return run;
  }
}
```

### 6.2 Backend subscription iterators

From `@trigger.dev/sdk`, each returns an async iterator consumed with
`for await`, yielding the run object on every status / metadata / tag change.
Type params are supported, e.g. `runs.subscribeToRun<typeof samOppsBulkDispatcher>(id)`.

| Function | Subscribes to |
|---|---|
| `runs.subscribeToRun(runId)` | one run |
| `runs.subscribeToRunsWithTag(tag)` | every run carrying a tag, e.g. `"sam-opps-active"` |
| `runs.subscribeToBatch(batchId)` | every run in a batch |

### 6.3 Client — React hooks

All three hooks import from `@trigger.dev/react-hooks`. Pass the public token as
`accessToken` to every hook for client-side auth.

| Hook | Returns |
|---|---|
| `useRealtimeRun(runId, { accessToken })` | `{ run, error }` |
| `useRealtimeRunsWithTag(tag, { accessToken })` | `{ runs, error }` |
| `useRealtimeBatch(batchId, { accessToken })` | `{ runs, error }` |

```typescript
// client (React): subscribe with the handed-off token. No app server in the loop.
"use client";
import {
  useRealtimeRun,
  useRealtimeRunsWithTag,
  useRealtimeBatch,
} from "@trigger.dev/react-hooks";

export function RunPanel({ runId, accessToken }: { runId: string; accessToken: string }) {
  const { run, error } = useRealtimeRun(runId, { accessToken });
  return error ? <p>{error.message}</p> : <p>{run?.status}</p>;
}

export function FeedPanel({ accessToken }: { accessToken: string }) {
  const { runs } = useRealtimeRunsWithTag("sam-opps-active", { accessToken });
  return <ul>{runs.map((r) => <li key={r.id}>{r.id}: {r.status}</li>)}</ul>;
}

export function BatchPanel({ batchId, accessToken }: { batchId: string; accessToken: string }) {
  const { runs } = useRealtimeBatch(batchId, { accessToken });
  return <p>{runs.filter((r) => r.status === "COMPLETED").length}/{runs.length} done</p>;
}
```

### 6.4 Fanning out — `batch.trigger`

`batch.trigger` (from `@trigger.dev/sdk`) triggers multiple runs of **different**
tasks and returns a result carrying `batchId`. Feed that `batchId` to
`useRealtimeBatch` / `runs.subscribeToBatch` to track the whole fan-out as one
unit.

```typescript
import { batch } from "@trigger.dev/sdk";
// Fan out across feeds; the returned batchId is the Command Center's handle.
const handle = await batch.trigger<typeof samOppsBulkDispatcher>([
  { id: "sam-opps-bulk-dispatcher", payload: {} },
]);
// handle.batchId -> useRealtimeBatch(handle.batchId, { accessToken })
```

> The `batch.trigger` payload cap is version-gated: **1,000 payloads in SDK
> 4.3.1+**, 500 before. `package.json` pins `@trigger.dev/sdk` `^4.4.6`, so the
> 1,000 ceiling applies. A pin below 4.3.1 would silently hit the lower limit.

> ### Public Access Token vs trigger token — read vs write
> **Reading** run state from the browser uses **Public Access Tokens** scoped
> with `read: { runs | tags | batch }`, as above. **Triggering** a task from the
> browser requires a single-use **trigger token** — a more restricted credential,
> NOT a Public Access Token. Do not conflate the two. The Command Center is a
> read surface: a read-scoped Public Access Token is the right credential, and no
> app server sits in the read loop.

---

## 7. Adding a feed — the control-plane checklist

A new feed earns exactly one new Trigger task and zero new platform plumbing:

1. **Task** — add `src/trigger/<feed>.ts`. A `schedules.task` (declarative cron,
   timezone pinned) that mints `wait.createToken`, POSTs the Universal Dispatcher
   with `trigger_callback_url: token.url`, and suspends on `wait.forToken`.
   Mirror [`src/trigger/sam_opps_bulk.ts`](../../src/trigger/sam_opps_bulk.ts).
2. **Worker** — a domain-grouped Modal worker under `pipelines/<domain>/` that
   accepts `trigger_callback_url` and POSTs the **raw** terminal payload to it
   (§4.5). See [`03_modal_compute.md`](03_modal_compute.md).
3. **State** — an `ops.*` runs table for terminal state, written by the worker
   via psycopg before the callback (§5 of [`ARCHITECTURE.md`](../../ARCHITECTURE.md)).
4. **Deploy** — `doppler run -- npx trigger.dev@4.4.4 deploy`. `syncEnvVars`
   forwards the existing dispatcher proxy-auth pair; the feed is wired through
   the same dispatcher by name.

**Zero new endpoints, zero new secrets.** The data plane the worker writes —
DuckDB → Apache Arrow → LanceDB v2.0 on R2 — is governed by
[`01_duckdb_processing.md`](01_duckdb_processing.md) and
[`02_lancedb_storage.md`](02_lancedb_storage.md).

> ### Data-plane law that the control plane must never violate
> Trigger tasks carry no data payloads through the transform path. **Apache
> Arrow is the only in-memory interchange** between DuckDB and Lance in the
> worker. **pandas is forbidden.** **Heavy nested-dict intermediates are
> forbidden.** A Trigger task's `result.output` is terminal metadata
> (`{ status, rows, feed }`) — never a data row, never a frame. The control
> plane moves signals; the data plane moves columns. See
> [`01_duckdb_processing.md`](01_duckdb_processing.md).
