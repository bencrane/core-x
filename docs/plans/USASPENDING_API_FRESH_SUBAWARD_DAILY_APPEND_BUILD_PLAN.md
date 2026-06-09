# USAspending API FRESH (Subaward) — Daily APPEND on Trigger.dev/Modal — BUILD PLAN

**Audience:** executor agent. **Repo:** `core-x`. **Status:** ready to execute.
**Mandate:** give the procurement-subaward cache a **Trigger.dev → Universal Dispatcher → Modal**
daily APPEND surface, **8-day** trailing window. The path **MUST append, never overwrite.** This is a
**PORT**, not a rewrite: the chunked append engine already exists and works *locally* — wrap it in the
prime-proven Modal/Trigger scaffolding and **keep the local `uv-run` path as the fallback.**

> Sibling of `USASPENDING_API_FRESH_PRIME_DAILY_APPEND_BUILD_PLAN.md` (prime, shipped + verified live:
> +89,878 rows, no Modal-IP throttle). Read that first — this plan reuses its Modal/Trigger pattern.

---

## 0. TL;DR

`pipelines/usaspending/usaspending_api_subaward_fresh.py` is a self-contained **`uv-run` CLI** (no Modal)
with a **working append path**: `daily()` → `_run_chunks` → `_combine_write("append")` → `_optimize_indices`
→ `_record_run`. The append engine, the chunking, the zombie-escape sub-splitting, the gap tracking, and
the ledger are all **already built and proven**. The only thing missing is a **Modal worker + Trigger task**
so it can run server-side, durably, on the same dispatcher as prime.

Unlike prime (one fast `prime_awards` shot), subaward runs on the **slow `elasticsearch_sub_awards`
backend** — the documented reason it was kept local. So the Modal port is genuinely new ground: P1 must
confirm the slow chunked job completes within the Modal timeout and that the backend doesn't throttle
Modal's egress IP. **The local `uv-run` path is the proven fallback and must not be removed.**

> **Fragmentation (handled separately):** the chunked *fetch* does NOT fragment Lance — `_combine_write`
> assembles all chunks into one Arrow table and does one `write_dataset` (the 90-day backfill landed exactly
> 1 fragment, verified live). The daily *append* adds +1 small fragment/run, which is closed by a separate
> weekly compaction worker — see `USASPENDING_FRESH_COMPACTION_BUILD_PLAN.md`. Do **not** add compaction to
> `run_daily` (blast-radius containment). Keep the append worker's `_optimize_indices` as-is.

---

## 1. The bones we already have (do not rebuild)

| Piece | Location | Reuse |
|---|---|---|
| Chunked submit/poll/zombie-split/gap orchestrator `_run_chunks` | `usaspending_api_subaward_fresh.py:189` | **verbatim** |
| `_submit` / `_status` / `_download` (keyless, 429-aware) | `:125`–`:158` | verbatim |
| `_chunk_windows` / `_zombie_budget` / `_split_window` | `:161`–`:186` | verbatim |
| `_combine_write(workdir, mode, so)` — **already supports `"append"`** | `:266` | verbatim |
| `_build_indices(so, rebuild)` / `_optimize_indices(so)` | `:282` / `:301` | verbatim |
| `_record_run(... run_mode, write_mode ...)` → `ops.usaspending_api_subaward_fresh_runs` | `:310` | verbatim (no change) |
| Working **append** command `daily(days, chunk_days)` | `:367` | refactor → shared core |
| `_r2_so()` / `_dataset_exists()` | `:104` / `:115` | verbatim |
| Ops ledger DDL (already models `'backfill' \| 'daily'`) | `ops_usaspending_api_subaward_fresh_runs.sql` | verbatim (no migration) |
| Local CLI `main()` argv dispatch + `__main__` | `:432` / `:449` | **keep** (fallback surface) |
| Modal/Trigger scaffolding reference (proven) | `usaspending_api_fresh.py` + `src/trigger/usaspending_api_fresh.ts` | clone |

**The only gap:** a Modal app wrapper (`run_daily` worker) + a Trigger task. The data engine is done.

---

## 2. Design decisions (locked)

1. **Append, never overwrite.** The Modal worker calls `_daily_core` → `_combine_write(SCRATCH, "append", so)`
   exclusively. It must never touch `backfill()` / `_combine_write(..., "overwrite", ...)`.
2. **Dual-mode single file.** Add the Modal app to the existing file; **keep** `main()`/`daily()`/`backfill()`/
   `verify()`/`init_ops()` for the residential-IP `uv-run` fallback. Add `import modal` at top → the local
   `uv-run` command must add `--with 'modal>=0.66'` (the module now imports modal; the decorators only
   *define* objects, they never connect, so `main()` still runs fully locally).
3. **One implementation, two surfaces.** Extract `daily()`'s body into `_daily_core(days, chunk_days,
   trigger_callback_url=None)`. Local `daily()` becomes a thin wrapper; the Modal `run_daily` calls the same
   core + posts the waitpoint callback. No duplicated orchestration.
4. **8-day window.** `run_daily(days=8, ...)` and the Trigger payload default to **8** (operator buffer; the
   pulse showed subaward 4 days behind). The local `daily()` default (`DEFAULT_DAILY_DAYS=14`) is left as-is.
5. **Chunk size for the slow backend.** Default the Modal daily path to `chunk_days=4` (an 8-day window →
   2 completable chunks under `MAX_INFLIGHT`); the existing zombie sub-splitting remains the safety net for
   any dense chunk.
6. **Timeout sized for the slow backend.** `elasticsearch_sub_awards` is ~100–300× slower than prime. Set the
   Modal worker `timeout=3*3600` (3h) and the Trigger waitpoint token to **"3h"** (vs prime's 1h). The durable
   wait consumes zero compute while suspended; the token timeout — not `maxDuration` — bounds the window.
7. **`retries=0` on the Modal worker (deliberate).** `_run_chunks` is internally resilient (per-chunk 429
   backoff, one resubmit on failed/expired, zombie-split, `RUN_CAP_SECONDS`). A *function-level* retry
   re-runs the entire multi-chunk orchestration and — because `_combine_write` appends once at the very end —
   a retry that fires **after** a successful append would **double-append** the window. Prefer `retries=0` and
   lean on internal resilience. If P1 shows the backend hard-throttles Modal's IP at submit (a fresh-IP retry
   would help), revisit with an idempotency guard — do **not** naively copy prime's `retries=5`.
8. **Ledger: no migration.** `_record_run(run_mode="daily", write_mode="append", ...)` already targets
   `ops.usaspending_api_subaward_fresh_runs`.
9. **Keep the proven fallback.** The local `uv-run daily` path is the guaranteed-working surface for the slow
   backend (residential IP). It stays. If Modal proves unreliable for this backend, the fallback is production.

---

## 3. Work item 1 — `pipelines/usaspending/usaspending_api_subaward_fresh.py`

Additive + one refactor. `_run_chunks`, `_combine_write`, `_optimize_indices`, `_record_run`, `backfill`,
`verify`, `init_ops`, `main` are **not** behavior-changed.

### 3a. Add the Modal app scaffolding (after the `import requests` line, top of file)

```python
import modal

# Modal image mirrors the prime app: verbatim-CSV → DuckDB → Lance(append) → ops via psycopg.
image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2", "lancedb>=0.15", "pylance>=7", "pyarrow>=17",
    "requests>=2.32", "psycopg[binary]>=3.2",
)
app = modal.App("usaspending-api-subaward-fresh", image=image)
```

> The existing top-level imports (`datetime`, `os`, `shutil`, `sys`, `time`, `zipfile`, `requests`) are stdlib
> or in the image; `duckdb`/`lance`/`psycopg` stay function-level (run inside the container). `if __name__ ==
> "__main__": main()` does **not** fire on Modal import (module name ≠ `__main__`).

### 3b. Add the callback poster (clone of prime `_post_callback`)

```python
def _post_callback(url, payload, attempts: int = 3) -> None:
    """POST terminal metadata to the Trigger.dev waitpoint. No-op on manual/local runs."""
    if not url:
        log("No trigger_callback_url (manual run); skipping callback.")
        return
    for i in range(attempts):
        try:
            resp = requests.post(url, json=payload, timeout=30)
            if resp.status_code < 300:
                log(f"Callback delivered: {payload}")
                return
        except Exception as exc:  # noqa: BLE001
            log(f"Callback attempt {i + 1} failed: {exc}")
        time.sleep(2 * (i + 1))
    log(f"WARN: callback delivery failed after {attempts} attempts → {url}")
```

### 3c. Extract `daily()`'s body into `_daily_core`; make `daily()` a thin wrapper

```python
def _daily_core(days, chunk_days, trigger_callback_url=None) -> dict:
    """Trailing-window chunked APPEND (mode='append' — NEVER overwrites). Requires the table to exist.
    Internal zombie-split + gap tracking handle incompletable windows. Returns a flat metrics dict and
    (on success) POSTs it to trigger_callback_url. This is the single implementation behind both the
    local `daily()` CLI and the Modal `run_daily` worker."""
    started = dt.datetime.now(dt.timezone.utc)
    so = _r2_so()
    if not _dataset_exists(so):
        raise RuntimeError(f"{FRESH_URI} does not exist — run backfill first (daily only appends).")
    we = dt.datetime.now(dt.timezone.utc).date()
    ws = we - dt.timedelta(days=days)
    windows = _chunk_windows(ws, we, chunk_days)
    log(f"[daily] last_modified [{ws}..{we}] {days}d → {len(windows)} chunks of {chunk_days}d (append)")
    status, error, rows, cols, total = "error", None, 0, 0, 0
    polls = 0
    try:
        n, polls, gaps = _run_chunks(windows, SCRATCH)
        if n == 0:
            raise RuntimeError("no chunk finished")
        rows, cols = _combine_write(SCRATCH, "append", so)
        if rows > 0:
            _optimize_indices(so)
        else:
            # Soft-block tell on the slow backend: finished chunks but 0 rows across a multi-day window.
            log("WARN: 0 rows appended for a multi-day window — possible soft block / silent throttle.")
        import lance
        total = lance.dataset(FRESH_URI, storage_options=so).count_rows()
        status = "success"
        error = ("GAPS: " + " ".join(gaps)) if gaps else None
        log(f"DONE appended={rows:,} table_total={total:,} gaps={gaps or 'none'}")
        _post_callback(trigger_callback_url,
                       {"status": status, "rows": int(rows), "feed": FEED, "dataset_uri": FRESH_URI,
                        "window_start": ws.isoformat(), "window_end": we.isoformat(),
                        "table_rows_after": int(total), "run_mode": "daily", "gaps": gaps or None})
    finally:
        _record_run(run_mode="daily", ws=ws, we=we, rows=rows, cols=cols, total=total,
                    polls=polls, write_mode="append", indices=INDEX_COLS, status=status,
                    error=error, started=started, completed=dt.datetime.now(dt.timezone.utc))
    return {"feed": FEED, "run_mode": "daily", "window_start": ws.isoformat(),
            "window_end": we.isoformat(), "rows_written": int(rows), "columns": int(cols),
            "table_rows_after": int(total), "status": status, "gaps": gaps or None}


def daily(days=DEFAULT_DAILY_DAYS, chunk_days=DEFAULT_CHUNK_DAYS):
    """Local (residential-IP) APPEND path — unchanged CLI surface; delegates to the shared core."""
    _daily_core(days, chunk_days)
```

> Behavior parity with the current `daily()`: ledger always written in `finally`; gaps recorded as a
> non-fatal `error="GAPS: …"` on an otherwise-`success` row; a hard failure (e.g. `no chunk finished`)
> propagates after the ledger write. Only addition: the callback (success-only) and the 0-row WARN.

### 3d. Add the Modal worker + local entrypoint

```python
@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=3 * 3600,        # slow elasticsearch_sub_awards backend; internal RUN_CAP_SECONDS still bounds the run
    memory=32768,
    cpu=4.0,
    retries=0,               # see Design Decision 7 — whole-run retry would double-append post-_combine_write
)
def run_daily(days: int = 8, chunk_days: int = 4, trigger_callback_url: str | None = None) -> dict:
    """APPEND top-up on Modal (server-side, durable). Wraps the chunked _daily_core. 8-day default
    (operator buffer). NEVER overwrites. POSTs terminal metadata to trigger_callback_url on success."""
    return _daily_core(days=days, chunk_days=chunk_days, trigger_callback_url=trigger_callback_url)


@app.local_entrypoint()
def daily_modal(days: int = 8, chunk_days: int = 4) -> None:
    """Drive the Modal run_daily worker for manual testing (8-day APPEND):
        modal run --detach pipelines/usaspending/usaspending_api_subaward_fresh.py::daily_modal --days 8
    Launched locally, EXECUTES ON MODAL."""
    import json
    print(json.dumps(run_daily.remote(days=days, chunk_days=chunk_days), indent=2, default=str))
```

> Name the entrypoint `daily_modal` (not `daily`) — a `@app.local_entrypoint` named `daily` would shadow the
> plain `daily()` CLI function. Optionally add Modal `init_ops`/`verify` entrypoints mirroring prime, but they
> are not required: the ledger table already exists and the local `init_ops`/`verify` still run via `uv-run`.

### 3e. Update the local `uv-run` invocation (module docstring + runbook)

The module now imports `modal`, so the residential-IP path adds `--with 'modal>=0.66'`:

```
doppler run -p core-x -c prd -- uv run --no-project \
  --with 'pylance>=7' --with 'pyarrow>=17' --with 'duckdb>=1.5,<2' \
  --with 'requests>=2.32' --with 'psycopg[binary]>=3.2' --with 'modal>=0.66' \
  python3 pipelines/usaspending/usaspending_api_subaward_fresh.py daily 8 4
```

---

## 4. Work item 2 — `src/trigger/usaspending_api_subaward_fresh.ts` (new file)

Clone of `src/trigger/usaspending_api_fresh.ts` with the subaward app/function/window and a **longer token**
(slow backend). `app_name`/`function_name` must match the Modal app exactly.

```ts
import { task, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — USAspending API FRESH (procurement SUBAWARD) daily APPEND top-up.
 *
 * Mints a Trigger.dev v4 waitpoint, POSTs the Universal Dispatcher to spawn the
 * `run_daily` Modal worker (append-only; NEVER overwrites), suspends on
 * `wait.forToken`, resumes on the worker's flat callback. Manually triggerable with
 * `{ days }`; default 8. The subaward export runs on the slow elasticsearch_sub_awards
 * backend, so the token timeout is 3h (vs prime's 1h).
 */
interface SubawardDailyCallback {
  status: "success" | "error";
  rows: number;
  feed: string;
  dataset_uri?: string;
  window_start?: string;
  window_end?: string;
  table_rows_after?: number;
  run_mode?: string;
  gaps?: string[] | null;
}

export const usaspendingApiSubawardFreshDaily = task({
  id: "usaspending-api-subaward-fresh-daily",
  maxDuration: 3900, // suspended wait is free; the 3h token below bounds the window
  run: async (payload: { days?: number; chunkDays?: number }, { ctx }) => {
    const days = payload?.days ?? 8;
    const chunkDays = payload?.chunkDays ?? 4;

    const token = await wait.createToken({
      timeout: "3h", // elasticsearch_sub_awards is ~100–300× slower than the prime backend
      tags: ["usaspending-api-subaward-fresh", "modal-dispatch"],
    });

    const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Modal-Key": requireEnv("MODAL_KEY"),
        "Modal-Secret": requireEnv("MODAL_SECRET"),
      },
      body: JSON.stringify({
        app_name: "usaspending-api-subaward-fresh",
        function_name: "run_daily",
        kwargs: { days, chunk_days: chunkDays },
        trigger_callback_url: token.url,
      }),
    });
    if (!res.ok) {
      const body = await res.text();
      throw new Error(`dispatcher ${res.status}: ${body.slice(0, 500)}`);
    }

    logger.info("Dispatched usaspending_api_subaward_fresh daily → Modal; suspending on waitpoint", {
      tokenId: token.id,
      days,
      chunkDays,
      triggerRunId: ctx.run.id,
    });

    const result = await wait.forToken<SubawardDailyCallback>(token.id);
    if (!result.ok) {
      throw new Error(
        `usaspending_api_subaward_fresh daily timed out before Modal callback (token ${token.id})`,
      );
    }
    if (result.output.status !== "success") {
      throw new Error(
        `usaspending_api_subaward_fresh daily failed in Modal: ${JSON.stringify(result.output)}`,
      );
    }

    logger.info("usaspending_api_subaward_fresh daily append complete", { ...result.output });
    return result.output;
  },
});

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}
```

**Env:** `MODAL_DISPATCHER_URL`, `MODAL_KEY`, `MODAL_SECRET` — already provisioned fleet-wide; no new secrets.

---

## 5. Ops ledger — verify only (no migration)

The subaward table already exists (~199,901 rows) and `ops.usaspending_api_subaward_fresh_runs` already
receives `daily`/`append` rows from the local path. Optional idempotent re-apply via `uv-run … init_ops`.
Expected daily row: `run_mode='daily'`, `write_mode='append'`, `status='success'` (or `success` with
`error="GAPS: …"` if any window poisoned), `table_rows_after` > pre-run.

---

## 6. Execution & verification

> Baseline (pulse): subaward = **199,901 rows**, `max(subaward_sam_report_last_modified_date)` = **2026-06-05**,
> delta 4 days. The append frontier column is `subaward_sam_report_last_modified_date` (the API windows on
> `date_type=last_modified_date`; there is no plain `last_modified_date` column).

### Phase 1 — Modal direct (validates the slow backend on Modal; minimal blast radius)

```bash
doppler run -p core-x -c prd -- modal deploy pipelines/usaspending/usaspending_api_subaward_fresh.py
# table + ledger already exist; (re)apply DDL only if desired, via the local uv-run init_ops.
doppler run -p core-x -c prd -- modal run --detach \
  pipelines/usaspending/usaspending_api_subaward_fresh.py::daily_modal --days 8 --chunk-days 4
```

**Watch for (Phase 1):**
- Chunk logs: `submitted [ws..we]`, `FINISHED rows=… sec=…`, any `ZOMBIE … split` / `GAP`.
- Does it **finish inside the 3h Modal timeout**? (the core slow-backend question)
- `wrote (mode=append): N rows` with `N > 0` then `optimize_indices: extended …`.
- **0-row tell:** finished chunks but 0 rows over 8 days ⇒ likely soft block / silent throttle — stop, report,
  the local `uv-run` path stays production.

**Pass criteria:** rows grew from 199,901; `max(subaward_sam_report_last_modified_date)` advanced past
2026-06-05; ledger `daily/append/success` row written; re-run the HWM pulse and confirm the subaward delta
shrank from 4d:

```bash
doppler run -p core-x -c prd -- modal run pipelines/usaspending/usaspending_api_subaward_fresh.py::daily_modal --days 8   # (or local verify)
doppler run -p core-x -c prd -- uv run --no-project --with 'pylance>=7' --with 'duckdb>=1.5,<2' \
  --with 'pyarrow>=17' python3 scripts/usaspending_hwm_pulse.py
```

### Phase 2 — Trigger.dev end-to-end (only if Phase 1 is clean)

`trigger.dev deploy` is **manual + fleet-wide** here (redeploys all ~55 tasks; no CI). Deploy from a clean
`main` checkout and get the operator's go before firing it:

```bash
doppler run -p core-x -c prd -- npm run trigger:deploy
# then trigger usaspending-api-subaward-fresh-daily with {"days": 2} (minimal dup) to prove the round-trip.
```

---

## 7. Guardrails / failure modes

| Risk | Control |
|---|---|
| **Overwrite of the accumulating table** | `_daily_core` only passes `mode="append"`; never calls `backfill()`/overwrite. |
| Running daily before the table exists | `_dataset_exists` guard → hard raise. |
| Slow backend exceeds the worker window | `timeout=3h`; internal `RUN_CAP_SECONDS`/`CHUNK_CAP_SECONDS` + zombie-split bound the run and convert stuck windows to GAPs (recorded, non-fatal). |
| Whole-run retry double-appends | `retries=0` on the Modal worker (append happens once at the end; a retry post-write would duplicate). |
| Backend soft-throttles Modal IP | 0-row WARN + Phase-1 gate inspects row count; **local `uv-run` fallback (residential IP) stays production.** |
| Appended rows invisible to pushdown | `_optimize_indices(so)` after every non-zero append. |
| Duplicate rows on overlap | Intentional + harmless (FFATA/FSRS lag); reconciled downstream, never here. |
| Breaking the proven local path | Refactor is behavior-preserving; `main()`/`daily()` keep working (add `--with modal`). |

---

## 8. Git lifecycle (executor owns end-to-end)

```bash
git checkout -b feat/usaspending-subaward-daily-append-modal origin/main
# … apply Work items 1 + 2 …
python3 -m py_compile pipelines/usaspending/usaspending_api_subaward_fresh.py
git add pipelines/usaspending/usaspending_api_subaward_fresh.py src/trigger/usaspending_api_subaward_fresh.ts
git commit -m "feat(usaspending): subaward API-fresh daily APPEND on Trigger/Modal (port; never overwrites)"
git push -u origin HEAD && gh pr create --fill --base main
# Self-verify Phase 1 green, then:
gh pr merge <num> --squash --delete-branch
```

Then **fast-forward the operator-facing `main` checkout** (the `main` worktree, not a feature-branch
checkout) and `git log -1 --oneline`. Deploy the Modal app (§6 P1 step 1); run `trigger:deploy` only with the
operator's go.

---

## 9. Acceptance criteria

- [ ] `run_daily` appends (never overwrites); refuses if the table is absent; `retries=0`.
- [ ] `_daily_core` is the single implementation; local `daily()`/`main()` still run (`--with modal`).
- [ ] BTREE indices extended over appended fragments (`optimize_indices`).
- [ ] `ops.usaspending_api_subaward_fresh_runs` gains a `daily`/`append`/`success` row; `table_rows_after` grew.
- [ ] `max(subaward_sam_report_last_modified_date)` advanced; HWM delta shrank from 4d.
- [ ] Slow-backend run completed inside the Modal timeout (record the wall-clock for tuning).
- [ ] Trigger task dispatches `run_daily` via the Universal Dispatcher and resumes from the callback (Phase 2).
- [ ] `backfill`/overwrite path and the local CLI fallback unchanged.
- [ ] PR merged; `main` checkout fast-forwarded.

---

## Appendix A — the fallback is already production

If the Modal port proves unreliable for the slow backend (timeouts, IP soft-blocks), the residential-IP
`uv-run` path is the proven surface and needs nothing new:

```bash
doppler run -p core-x -c prd -- uv run --no-project \
  --with 'pylance>=7' --with 'pyarrow>=17' --with 'duckdb>=1.5,<2' \
  --with 'requests>=2.32' --with 'psycopg[binary]>=3.2' --with 'modal>=0.66' \
  python3 pipelines/usaspending/usaspending_api_subaward_fresh.py daily 8 4
```

Same engine, same `daily`/`append` ledger row, on your IP. The Modal/Trigger surface is the durability
upgrade — not a replacement for a path that already works.
