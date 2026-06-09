# USAspending API FRESH (Prime) — Daily APPEND Worker + Trigger.dev/Modal Dispatch — BUILD PLAN

**Audience:** executor agent. **Repo:** `core-x`. **Status:** ready to execute.
**Mandate:** add the missing **append** (top-up) path for the prime contract+IDV cache and wire it to
**Trigger.dev → Universal Dispatcher → Modal**. The path **MUST append, never overwrite.** Mirror the
*subaward* append semantics; mirror the *ffata_exec_comp* Modal/Trigger execution model.

---

## 0. TL;DR

The prime fresh table `s3://data-sink/active/usaspending_api_fresh/contract_prime_txn/` (1.4M rows,
accumulating) has **only** a first-create `run_backfill` worker, whose sole write mode is
`mode="overwrite"`. There is **no append/daily worker** — so there is no safe way to top it up.
A literal "10-day backfill" today either refuses (table exists) or, if forced, **overwrites 1.4M rows
with a 10-day slice = data loss.**

This plan adds `run_daily` (append-only) to the existing Modal app and a one-file Trigger task that
dispatches it through the existing Universal Dispatcher. No new endpoints, no new secrets, no ledger
migration. The subaward sibling already proves every piece — we are porting its append semantics onto
prime's Modal substrate.

---

## 1. The bones we already have (do not rebuild)

| Piece | Location | Reuse as-is? |
|---|---|---|
| Modal app `usaspending-api-fresh` | `pipelines/usaspending/usaspending_api_fresh.py:79` | yes |
| `_fetch_window(ws, we)` (async bulk_download, keyless, 429→fresh-IP retry) | `usaspending_api_fresh.py:118` | yes |
| `_write(csv_glob, mode, so)` — **already supports `mode="append"`** | `usaspending_api_fresh.py:184` | yes |
| `_window(days)` trailing window | `usaspending_api_fresh.py:105` | yes |
| `_dataset_exists(so)` | `usaspending_api_fresh.py:96` | yes |
| `_record_run(... run_mode, write_mode ...)` → `ops.usaspending_api_fresh_runs` | `usaspending_api_fresh.py:230` | yes (no change) |
| Ops ledger DDL (comment already lists `'backfill' \| 'daily'`) | `pipelines/usaspending/ops_usaspending_api_fresh_runs.sql` | yes (no migration) |
| `run_backfill` (overwrite, first-create) | `usaspending_api_fresh.py:268` | **leave untouched** |
| Universal Dispatcher (`Function.from_name(app, fn).spawn(**kwargs, trigger_callback_url=)`) | `core/modal_dispatcher.py` | yes |
| Append-semantics reference (`daily()`, `_optimize_indices`) | `usaspending_api_subaward_fresh.py:367,301` | port the shape |
| Modal worker + callback + Trigger task reference | `pipelines/usaspending/ffata_exec_comp.py` + `src/trigger/ffata_exec_comp.ts` | clone the shape |

**The only gap:** an append worker + its Trigger task. Everything else exists.

---

## 2. Design decisions (locked)

1. **Append, never overwrite.** `run_daily` calls `_write(csv_glob, "append", so)` exclusively. It must
   never call `run_backfill` and never pass `mode="overwrite"`. The overwrite path stays create-only.
2. **Requires the table to exist.** `run_daily` raises if `_dataset_exists(so)` is false — daily only
   appends; first-create remains `backfill`'s job. (Mirrors subaward `daily()`
   `usaspending_api_subaward_fresh.py:370`.)
3. **No chunking for prime.** Unlike subaward (slow `elasticsearch_sub_awards` backend → must chunk),
   prime uses the fast `prime_awards` backend; a 10-day window completes in minutes as a single shot.
   Reuse `_fetch_window` directly — do **not** port subaward's chunk/zombie machinery.
4. **Index correctness on append.** After a non-zero append, call `_optimize_indices(so)` so the BTREE
   scalar indices extend over the new fragments — otherwise pushdown silently misses appended rows.
5. **Execution model = Modal + Trigger, not local uv-run.** Mirror subaward's *append + index + ledger*
   semantics, but run on Modal (fresh-IP-per-retry defeats USAspending's F5/IP throttle —
   `DIRECTIVE_33_USASPENDING_DAILY_DELTA_PORT.md:99`) and dispatch via Trigger.dev like `ffata_exec_comp`.
6. **Ledger: no migration.** `_record_run(run_mode="daily", write_mode="append", ...)` already targets
   `ops.usaspending_api_fresh_runs` with the right columns.
7. **Idempotent / duplicate-safe.** Overlapping recent windows re-pull rows we already hold — intentional
   (publish lag), harmless, reconciled by a downstream mirror, never here.

---

## 3. Work item 1 — `pipelines/usaspending/usaspending_api_fresh.py`

All additions are **new functions** plus one backward-compatible signature extension. `run_backfill`,
`_fetch_window`, `_write`, `verify` are **not** modified.

### 3a. Add a callback poster (clone of `ffata_exec_comp._post_callback`)

```python
def _post_callback(url, payload, attempts: int = 3) -> None:
    """POST terminal metadata to the Trigger.dev waitpoint. No-op on manual runs."""
    if not url:
        print("No trigger_callback_url (manual run); skipping callback.", flush=True)
        return
    import time
    import requests
    for i in range(attempts):
        try:
            resp = requests.post(url, json=payload, timeout=30)
            if resp.status_code < 300:
                print(f"Callback delivered: {payload}", flush=True)
                return
        except Exception as exc:  # noqa: BLE001
            print(f"Callback attempt {i + 1} failed: {exc}", flush=True)
        time.sleep(2 * (i + 1))
    print(f"WARN: callback delivery failed after {attempts} attempts → {url}", flush=True)
```

### 3b. Extend `_build_indices` with a `rebuild` flag + add `_optimize_indices` (port from subaward)

Replace the existing `_build_indices` (`:211`) with the `rebuild`-aware version and add `_optimize_indices`:

```python
def _build_indices(so, rebuild: bool = False) -> list[str]:
    import lance
    ds = lance.dataset(FRESH_URI, storage_options=so)
    present = set(ds.schema.names)
    built = []
    for col in INDEX_COLS:
        if col not in present:
            print(f"  SKIP (absent) {col}", flush=True)
            continue
        try:
            ds.create_scalar_index(col, index_type="BTREE", replace=True) if rebuild \
                else ds.create_scalar_index(col, index_type="BTREE")
        except TypeError:  # older lance has no `replace=`
            ds.create_scalar_index(col, index_type="BTREE")
        built.append(col)
        print(f"  BTREE {col}", flush=True)
    return built


def _optimize_indices(so) -> None:
    """Extend existing BTREE indices over newly appended fragments (cheap incremental
    train). Fall back to a full rebuild only if the lance build lacks optimize_indices."""
    import lance
    ds = lance.dataset(FRESH_URI, storage_options=so)
    try:
        ds.optimize.optimize_indices()
        print("optimize_indices: extended over appended fragments", flush=True)
    except Exception as e:  # noqa: BLE001
        print(f"optimize_indices unavailable ({e}); rebuilding", flush=True)
        _build_indices(so, rebuild=True)
```

> `run_backfill` calls `_build_indices(so)` with no args — the new default `rebuild=False` keeps it
> identical. No behavioral change to the create path.

### 3c. Add the append worker `run_daily` (mirrors `run_backfill`, append semantics)

```python
@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 200,
    memory=32768,            # identical to run_backfill; a 10-day window is far smaller — fine to trim later
    cpu=4.0,
    retries=modal.Retries(max_retries=5, backoff_coefficient=2.0, initial_delay=30.0),  # fresh IP per retry
)
def run_daily(days: int = 7, trigger_callback_url: str | None = None) -> dict:
    """Trailing-window APPEND top-up. Pulls the past `days` of last_modified_date contract+IDV
    transactions and APPENDS them (mode='append' — NEVER overwrites). Requires the table to exist.
    Overlapping recent days are re-pulled on purpose (publish lag) → harmless duplicate rows,
    reconciled downstream. optimize_indices extends BTREE over the new fragments. POSTs terminal
    metadata to trigger_callback_url (Trigger.dev waitpoint) on success."""
    import datetime as dt

    started_at = dt.datetime.now(dt.timezone.utc)
    so = _r2_storage_options()
    if not _dataset_exists(so):
        raise RuntimeError(f"{FRESH_URI} does not exist — run backfill first (daily only appends).")

    ws, we = _window(days)
    print(f"[fresh-daily] last_modified_date ∈ [{ws} … {we}]  ({days}d APPEND, award_types={AWARD_TYPES})",
          flush=True)

    status, error, rows, cols, total, built = "error", None, 0, 0, 0, []
    api_calls = 0
    try:
        csv_glob, api_calls = _fetch_window(ws, we)
        rows, cols = _write(csv_glob, "append", so)
        if rows == 0:
            # Soft-block tell: a "finished" job over a 10-day window with 0 rows is the F5/IP-throttle
            # signature, not a normal quiet day. Surface it loudly; do NOT hard-fail an append.
            print("WARN: 0 rows appended for a multi-day window — possible soft block / silent throttle.",
                  flush=True)
        else:
            _optimize_indices(so)
        import lance
        total = lance.dataset(FRESH_URI, storage_options=so).count_rows()
        status = "success"
        _post_callback(trigger_callback_url,
                       {"status": status, "rows": int(rows), "feed": FEED, "dataset_uri": FRESH_URI,
                        "window_start": ws.isoformat(), "window_end": we.isoformat(),
                        "table_rows_after": int(total), "run_mode": "daily"})
    except BaseException as exc:  # noqa: BLE001
        error = f"{type(exc).__name__}: {exc}" if str(exc) else f"{type(exc).__name__} (no message)"
        status = "error"
        raise
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run(run_mode="daily", window_start=ws, window_end=we, rows_written=int(rows),
                    columns=int(cols), table_rows_after=int(total), api_calls=int(api_calls),
                    write_mode="append", indices_built=built, status=status, error=error,
                    started_at=started_at, completed_at=completed_at)

    return {"feed": FEED, "run_mode": "daily", "window_start": ws.isoformat(),
            "window_end": we.isoformat(), "rows_written": int(rows), "columns": int(cols),
            "table_rows_after": int(total), "api_calls": int(api_calls), "status": status}
```

> `indices_built` stays `[]` on daily rows: optimize *extends*, it does not *build* — recording it empty
> keeps the ledger's `indices_built` column meaning "columns indexed at create time" honest. (Subaward
> records `INDEX_COLS` here; prefer accuracy.)
>
> Callback fires **only on success**, inside `try`. On error the worker re-raises so `modal.Retries`
> recycles the container (fresh IP); a Trigger run that never gets a callback fails on token timeout —
> the intended behavior, do not post an error callback mid-retry.

### 3d. Add the local entrypoint `daily`

```python
@app.local_entrypoint()
def daily(days: int = 7) -> None:
    """Trailing-window APPEND top-up NOW (mode=append, never overwrites). 10-day window:
        modal run --detach pipelines/usaspending/usaspending_api_fresh.py::daily 10
    Launched locally, EXECUTES ON MODAL (fresh-IP-per-retry beats USAspending's IP throttle)."""
    import json
    print(json.dumps(run_daily.remote(days=days), indent=2, default=str))
```

### 3e. Update the module docstring usage block (`:33`)

Add under the existing `modal run …::backfill` line:

```
    modal run --detach pipelines/usaspending/usaspending_api_fresh.py::daily 10   # past 10d → APPEND
```

---

## 4. Work item 2 — `src/trigger/usaspending_api_fresh.ts` (new file)

Clone of `src/trigger/ffata_exec_comp.ts`, but a **manually-triggerable `task`** carrying a `days`
payload (so the same task serves the test, ad-hoc top-ups, and — wrapped in a schedule later — the daily
cadence). `app_name`/`function_name` must match the Modal app exactly.

```ts
import { task, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — USAspending API FRESH (prime contract+IDV) daily APPEND top-up.
 *
 * Mints a Trigger.dev v4 waitpoint, POSTs the Universal Dispatcher to spawn the
 * `run_daily` Modal worker (append-only; never overwrites), suspends on
 * `wait.forToken` (checkpointed, zero compute), and resumes on the worker's flat
 * terminal callback. Manually triggerable with `{ days }`; default 7.
 */
interface FreshDailyCallback {
  status: "success" | "error";
  rows: number;
  feed: string;
  dataset_uri?: string;
  window_start?: string;
  window_end?: string;
  table_rows_after?: number;
  run_mode?: string;
}

export const usaspendingApiFreshDaily = task({
  id: "usaspending-api-fresh-daily",
  maxDuration: 3900, // suspended wait consumes no compute; token timeout bounds the window
  run: async (payload: { days?: number }, { ctx }) => {
    const days = payload?.days ?? 7;

    const token = await wait.createToken({
      timeout: "1h", // prime_awards backend is fast; a 10-day window finishes in minutes
      tags: ["usaspending-api-fresh", "modal-dispatch"],
    });

    const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Modal-Key": requireEnv("MODAL_KEY"),
        "Modal-Secret": requireEnv("MODAL_SECRET"),
      },
      body: JSON.stringify({
        app_name: "usaspending-api-fresh",
        function_name: "run_daily",
        kwargs: { days },
        trigger_callback_url: token.url,
      }),
    });
    if (!res.ok) {
      const body = await res.text();
      throw new Error(`dispatcher ${res.status}: ${body.slice(0, 500)}`);
    }

    logger.info("Dispatched usaspending_api_fresh daily → Modal; suspending on waitpoint", {
      tokenId: token.id, days, triggerRunId: ctx.run.id,
    });

    const result = await wait.forToken<FreshDailyCallback>(token.id);
    if (!result.ok) {
      throw new Error(`usaspending_api_fresh daily timed out before Modal callback (token ${token.id})`);
    }
    if (result.output.status !== "success") {
      throw new Error(`usaspending_api_fresh daily failed in Modal: ${JSON.stringify(result.output)}`);
    }

    logger.info("usaspending_api_fresh daily append complete", { ...result.output });
    return result.output;
  },
});

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}
```

**Env (already provisioned fleet-wide; do not add new secrets):** `MODAL_DISPATCHER_URL`, `MODAL_KEY`,
`MODAL_SECRET` in the Trigger.dev dashboard env (same vars `ffata_exec_comp.ts` / `overture_places.ts`
already use).

**Optional daily cadence (add only after the test passes):** a thin `schedules.task` that triggers
`usaspendingApiFreshDaily` with `{ days: 7 }` on cron `0 16 * * *` UTC (stagger before ffata's 17:00).
Skip if the operator prefers manual triggering.

---

## 5. Ops ledger — verify only (no migration)

`run_daily` writes to `ops.usaspending_api_fresh_runs` via the existing `_record_run`. The table is
created by the idempotent DDL; ensure it exists once:

```bash
modal run pipelines/usaspending/usaspending_api_fresh.py::init_ops
```

Expected daily row after a run: `run_mode='daily'`, `write_mode='append'`, `status='success'`,
`table_rows_after` strictly greater than the pre-run count.

---

## 6. Execution & verification

### Phase 1 — Modal direct (cheap IP test; no Trigger involved)

This is the empirical settle on the IP-throttle question at minimum blast radius.

```bash
# 1. Deploy the app so the dispatcher can resolve run_daily by name (and to run locally).
doppler run -p core-x -c prd -- modal deploy pipelines/usaspending/usaspending_api_fresh.py

# 2. Ensure the ledger table exists (idempotent).
doppler run -p core-x -c prd -- modal run pipelines/usaspending/usaspending_api_fresh.py::init_ops

# 3. 10-day APPEND top-up. --detach so the async pull survives client disconnect.
doppler run -p core-x -c prd -- modal run --detach \
  pipelines/usaspending/usaspending_api_fresh.py::daily 10

# 4. Independent read-back: rows grew, last_modified frontier advanced.
doppler run -p core-x -c prd -- modal run pipelines/usaspending/usaspending_api_fresh.py::verify_table
```

**Pass criteria (Phase 1):**
- Worker logs `wrote (mode=append): N rows` with `N > 0`, then `optimize_indices: extended …`.
- `verify_table` shows `rows` increased from 1,406,045 and `max_last_modified` advanced toward today.
- Ledger row present:
  ```bash
  doppler run -p core-x -c prd -- psql "$HQX_DB_URL_POOLED" -c \
    "SELECT run_mode,write_mode,window_start,window_end,rows_written,table_rows_after,status,executed_at \
     FROM ops.usaspending_api_fresh_runs ORDER BY recorded_at DESC LIMIT 3;"
  ```
- Re-run the HWM pulse and confirm the prime delta shrank:
  ```bash
  doppler run -p core-x -c prd -- uv run --no-project --with 'pylance>=7' --with 'duckdb>=1.5,<2' \
    --with 'pyarrow>=17' python3 scripts/usaspending_hwm_pulse.py
  ```

**Soft-block watch:** a `finished` job that lands **0 rows** over a 10-day window is the F5/IP-throttle
signature (not a 429). If Phase 1 returns 0 rows, the IP path is being soft-throttled — stop and report;
the residential-IP fallback in Appendix A becomes the primary surface.

### Phase 2 — Trigger.dev end-to-end (only if Phase 1 is clean)

```bash
# Deploy Trigger tasks from a CLEAN main checkout (deploys the whole project — not from a dirty worktree).
npx trigger.dev@latest deploy
```

Trigger the task with a 10-day payload (dashboard “Test” with `{ "days": 10 }`, or the Trigger API/MCP
`trigger_task` for `usaspending-api-fresh-daily`). **Expected:** run suspends on the waitpoint → dispatcher
returns 202 → Modal `run_daily` executes → worker POSTs the callback → run resumes with
`output.status === "success"`. Confirm a second `run_mode='daily'` ledger row.

---

## 7. Guardrails / failure modes

| Risk | Control |
|---|---|
| **Overwrite of the accumulating table** | `run_daily` only ever passes `mode="append"`; never touches `run_backfill`. The overwrite path stays guarded + create-only. |
| Running daily before the table exists | `_dataset_exists` guard → hard raise. |
| Appended rows invisible to pushdown | `_optimize_indices(so)` after every non-zero append. |
| USAspending IP throttle (F5 BotDefense) | `modal.Retries(max_retries=5, …)` → fresh container = fresh egress IP per retry. Keep the decorator. |
| Silent soft block | 0-row multi-day window logs a WARN; Phase-1 gate inspects row count, not just job status. |
| Duplicate rows on overlap | Intentional + harmless (publish lag); reconciled by the downstream mirror, never here. |
| Trigger run hangs on worker crash | Callback only on success; failed retries exhaust → token timeout fails the run cleanly. |

---

## 8. Git lifecycle (executor owns end-to-end)

```bash
git checkout -b feat/usaspending-api-fresh-prime-daily-append
# … apply Work items 1 + 2 …
git add pipelines/usaspending/usaspending_api_fresh.py src/trigger/usaspending_api_fresh.ts \
        docs/plans/USASPENDING_API_FRESH_PRIME_DAILY_APPEND_BUILD_PLAN.md
git commit -m "feat(usaspending): prime API fresh daily APPEND worker + Trigger/Modal dispatch (never overwrites)"
git push -u origin HEAD
gh pr create --fill
# Self-verify Phase 1 green, then:
gh pr merge <num> --squash --delete-branch
```

Then **pull into the operator's primary checkout** (`/Users/benjamincrane/core-x`, not the worktree) and
`git log -1 --oneline` to confirm disk truth. Deploy the Modal app (§6 step 1) and, if Phase 2 is in
scope, `trigger.dev deploy` from that clean checkout.

---

## 9. Acceptance criteria

- [ ] `run_daily` appends (never overwrites); refuses if the table is absent.
- [ ] BTREE indices extended over appended fragments (`optimize_indices`).
- [ ] `ops.usaspending_api_fresh_runs` gains a `run_mode='daily', write_mode='append', status='success'` row.
- [ ] `table_rows_after` > pre-run count; prime `max_last_modified` advanced; HWM delta shrank.
- [ ] Trigger task `usaspending-api-fresh-daily` dispatches `run_daily` via the Universal Dispatcher and
      resumes from the worker callback (Phase 2).
- [ ] `run_backfill` and the overwrite path are byte-for-byte unchanged.
- [ ] PR merged; operator's primary checkout pulled to the merge commit.

---

## Appendix A — Optional residential-IP fallback (build ONLY if Phase 1 soft-blocks)

If Modal egress is throttled, add a true in-process CLI to `usaspending_api_fresh.py` mirroring
`usaspending_api_subaward_fresh.py:432` — an argv `main()` (`daily`/`backfill`/`verify`/`init_ops`) that
runs the fetch→`_write(append)`→`_optimize_indices`→`_record_run` chain locally (operator's IP) under
`doppler run -- uv run --no-project … python3 …py daily 10`. Same worker logic, same ledger row, no Modal,
no Trigger. This is the subaward execution model applied to prime; it is the reliable fallback precisely
because it runs on a residential IP the federal endpoint does not throttle.
```
