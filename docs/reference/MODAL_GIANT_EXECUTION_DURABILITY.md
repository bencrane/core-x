# Durable execution of long-running (giant) Modal jobs

**Purpose:** stop tying multi-hour jobs to an ephemeral session. A job that outlives the
interaction that launched it MUST be decoupled from that session and tracked by a durable
sentinel — never by a held process.

---

## 1. The incident (2026-06-28)

The full 107M-row `usaspending_fpds_canonical_txn` `build_fn` was launched as a **session-bound,
no-detach `modal run`** parked in a background slot:

```
modal run pipelines/usaspending/usaspending_fpds_canonical_modal.py::build   # (backgrounded)
```

It was **killed ~13 minutes in, mid-merge** (`_merge_tail_sql`, the `bulk_latest` 107M collapse).
The Modal log shows `KeyboardInterrupt → Query interrupted`: the local `modal run` **client**
received a termination signal, and because the run was `no-detach`, interrupting the client
interrupted the remote function. The kill was coincident with an MCP server disconnect/reconnect —
i.e. a **session/environment disruption reaped the long-lived background process**, not a code fault.

Damage: **none.** Publish is the last step (`build()` writes local Lance → boto3-publish *after*
the merge), so nothing was written to the prod URI; the table is overwrite-safe regardless. An
`ops.usaspending_fpds_canonical_runs` row was logged with `status='error'` — the expected audit
trail. No orphaned Modal app (no-detach → the kill stopped it cleanly).

## 2. Root cause

**The job's lifecycle was coupled to an ephemeral client process.** A `no-detach` `modal run`
(or a long-running background local-CLI process) lives and dies with the launching session/slot.
A multi-hour job's runtime **exceeds the lifetime of any single watched interaction**, so a routine
session reap, MCP hiccup, or background-process eviction kills it. Watching it does not make it
durable — it makes it fragile.

Contributing miss: the d.8 "**no `--detach`**" rule was applied too broadly. That rule exists to
prevent **append-mode double-writes on auto-retry** (the archive ingest disaster). It does **not**
apply to an **overwrite + `retries=0`** job, where detach is the correct, safe tool.

## 3. The principle (durable rule)

> **Any job expected to run longer than a single watched interaction (≳ a few minutes) MUST be
> decoupled from the session — a detached Modal run or a deployed app — and its completion tracked
> via a durable sentinel (ops-ledger row / published artifact), NOT by holding a process.**

A held foreground/background process is a *convenience for live logs*, never the thing that keeps
the job alive.

## 4. Detach-safety decision rule

| Job semantics | Detach safe? | Why |
|---|---|---|
| **overwrite / fully idempotent + `retries=0`** | **YES** | A re-run replaces wholesale; no auto-retry, so no double-write. Detach cannot cause the append disaster. |
| append-mode (new fragments) **with** auto-retries | NO | A retried detached function re-appends (the archive double-append). Fix idempotency / set `retries=0` **first**, then detach. |
| append-mode + `retries=0`, stamp-idempotent | YES (with the stamp guard) | The stamp skip-guard prevents the double-append even on a manual re-run. |

`usaspending_fpds_canonical_txn` is **overwrite + `retries=0`** → **detach is safe and correct.**

## 5. The fix — how we run the canonical giant from now on

### 5a. Immediate, zero new code — detached + ledger-tracked

```bash
cd <worktree>

# one-time ops DDL (idempotent; _record_run also self-bootstraps)
doppler run -p core-x -c prd -- python3 -m pipelines.usaspending.usaspending_fpds_canonical init_ops

# BUILD — DETACHED. Returns immediately; the job runs server-side, session-independent.
modal run --detach pipelines/usaspending/usaspending_fpds_canonical_modal.py::build

# INDEX — DETACHED, only AFTER build's ledger row is status='success' (see tracking below).
modal run --detach pipelines/usaspending/usaspending_fpds_canonical_modal.py::index

# VERIFY — read-back assertions.
modal run pipelines/usaspending/usaspending_fpds_canonical_modal.py::verify
```

- `--detach` decouples the run from the launching session: a reap/disconnect can no longer
  interrupt it. Safe here because overwrite + `retries=0` (§4).
- Still **killable**: `modal app stop <app-id>`. Still **observable**: Modal dashboard, or
  `modal app logs <app-id>`.

### 5b. Track completion out-of-band (the durable sentinel)

Do **not** hold a process waiting. Poll the durable signals:

```sql
-- completion sentinel: the latest ledger row flips to 'success' (or 'error' with a message)
SELECT status, rows_out, fresh_only_tail, deletes_tombstoned, max_action_date,
       error_message, completed_at
FROM ops.usaspending_fpds_canonical_runs
ORDER BY recorded_at DESC LIMIT 1;
```

Plus the R2 publish (`usaspending_fpds_canonical_txn/` prefix populated) and the Modal dashboard.
Gate `index` on `status='success'` + the metric envelope (`rows_out≈107.2M`, `pk_unique`,
`max_action_date='2026-06-26'`, `fresh_only_tail≈523K`).

### 5c. Recommended hardening (small code add) — one detached call, server-side chain

Add an orchestrator `@app.function` to the Modal wrapper so the whole sequence is **one** detached
job, fully session-independent, with each phase gated internally and written to the ledger:

```python
@app.function(image=image, secrets=[...], volumes={VOL_MOUNT: vol},
              memory=131072, cpu=16.0, timeout=60*60*12, retries=0)
def run_all(target_uri: str | None = None) -> dict:
    b = build_fn.local(target_uri=target_uri)          # merge → publish (fail-closed PK gate inside)
    v1 = verify_fn.local(target_uri=target_uri)        # data sanity BEFORE indexing
    assert v1["pk_unique"] and v1["rows_out"] > 100_000_000
    i = index_fn.local(target_uri=target_uri)          # Volume-staged append-only index
    v2 = verify_fn.local(target_uri=target_uri)        # final read-back
    return {"build": b, "verify_pre": v1, "index": i, "verify_post": v2}
```

```bash
modal run --detach pipelines/usaspending/usaspending_fpds_canonical_modal.py::run_all
```

One launch, one ledger completion to watch, survives the session entirely. Keep the individual
`build_fn`/`index_fn`/`verify_fn` for blast-radius re-runs of a single phase.

### 5d. Recurring rebuild (after each FRESH advance) — deploy + schedule

For the durable cadence, do not `modal run` at all — **deploy** and let Modal's scheduler invoke it:

```bash
modal deploy pipelines/usaspending/usaspending_fpds_canonical_modal.py
```

Attach a `modal.Cron`/schedule to `run_all` (e.g. weekly, lagging the BULK snapshot + FRESH daily
append). A deployed scheduled function has **no client dependency at all** — it cannot be reaped by
any session. Full overwrite per run (the canonical is a reconciled read-model; never incremental).

## 6. Checklist before launching any giant Modal job

- [ ] Job is overwrite/idempotent and `retries=0` → detach is safe (else fix idempotency first).
- [ ] Launch with `modal run --detach` (or a deployed/scheduled function) — **never** a session-bound `modal run` or a long background local-CLI process.
- [ ] A durable completion sentinel exists (ops-ledger row and/or published artifact). Do not track by holding a process.
- [ ] Kill switch known (`modal app stop <id>`); logs reachable (`modal app logs <id>` / dashboard).
- [ ] Sized for the work (memory/Volume/timeout) and using a `modal.Volume`, not `ephemeral_disk` (spot-preemption trap).
- [ ] Phases gated on each other's success (build → verify → index → verify), ideally chained in one detached `run_all`.

## 7. General takeaway for the fleet

Watching a job is for visibility, not durability. **Durability comes from decoupling the job from
the session and recording a sentinel.** This applies to every long Modal job, heavy index rebuild,
and multi-hour materialization in this repo — not just the FPDS canonical.
