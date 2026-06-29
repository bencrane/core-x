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

**Spill placement (corrected):** spill + stage live on the **standard 512 GiB container local
disk** (`/tmp`) — **NOT a `modal.Volume`, NOT `ephemeral_disk`.** The merge's ~100-180 GiB DuckDB
spill and the ~50-90 GiB local Lance stage both fit the default disk. A network-backed Volume
background-commits a high-churn spill dir every few seconds (slow → risks the 8 h build timeout),
and the proven giant `usaspending_bulk.py` ran on standard `/tmp` with NO Volume. The
`ephemeral_disk` "spot-preemption trap" is **project lore** (bulk.py comments), **not
Modal-documented**, and is **moot** because the 512 GiB default suffices without requesting
`ephemeral_disk` at all.

## 5. The fix — how we run the canonical giant from now on

### 5a. Immediate, zero new code — detached + two-source-tracked

Drive via the **local entrypoints** (`::smoke`/`::build`/`::index`/`::verify`), never the bare
`::*_fn` function targets — the entrypoints coerce `--since ""` → `None` (a bare `::build_fn` with
`--since ""` would inject `action_date >= DATE ''` → SQL error). Run the three `--detach` phases
**sequentially**, gating each on the prior's two-source completion (§5b).

```bash
cd <worktree>

# ── 0. one-time ops DDL — MANDATORY pre-step (do NOT rely on _record_run's self-bootstrap:
#       two concurrent first-run CREATEs can deadlock; pre-create the table once via doppler) ──
doppler run -p core-x -c prd -- \
  python3 -m pipelines.usaspending.usaspending_fpds_canonical init_ops

# ── 0a. CHEAP smoke gate — MANDATORY (foreground, seconds, pennies) before committing the box ──
modal run pipelines/usaspending/usaspending_fpds_canonical_modal.py::smoke
#   require: {"status": "ok", "column_spec_ok": true, "r2_endpoint_present": true}

# ── 1. BUILD — DETACHED. Capture the app id (printed to stdout/stderr) for liveness polling. ──
modal run --detach pipelines/usaspending/usaspending_fpds_canonical_modal.py::build \
  2>&1 | tee /tmp/fpds_build_launch.log
APP_ID=$(grep -oE 'ap-[A-Za-z0-9]+' /tmp/fpds_build_launch.log | head -1); echo "APP_ID=$APP_ID"

# ── 2. COMPLETION DETECTION — two-source AND (see §5b). Only on PASS proceed to INDEX. ──

# ── 3. INDEX — DETACHED, ONLY after step 2 PASS. Capture its app id too. ──
modal run --detach pipelines/usaspending/usaspending_fpds_canonical_modal.py::index \
  2>&1 | tee /tmp/fpds_index_launch.log
IDX_APP_ID=$(grep -oE 'ap-[A-Za-z0-9]+' /tmp/fpds_index_launch.log | head -1)
modal app list | grep "$IDX_APP_ID"        # poll to 'stopped'
modal app logs "$IDX_APP_ID" --tail 200    # require BTREE/BITMAP ✓ lines + n_uploaded>0

# ── 4. VERIFY — foreground (1 h box); this is the index-corruption gate (HIGH-2). ──
#       "index launched" is NON-TERMINAL until this post-index verify passes — a half-uploaded
#       index set surfaces HERE as a verify failure, not silent corruption.
modal run pipelines/usaspending/usaspending_fpds_canonical_modal.py::verify

# ── kill switch (any phase) — the detached app survives client exit; this reclaims the box ──
modal app stop "$APP_ID"
```

- `--detach` decouples the run from the launching session: a reap/disconnect can no longer
  interrupt it. Safe here because overwrite + `retries=0` (§4).
- Still **killable**: `modal app stop <app-id>`. Still **observable**: Modal dashboard, or
  `modal app logs <app-id>`.
- Before any manual launch, check `modal app list` for a live `usaspending-fpds-canonical` app
  and ensure the §5d schedule is paused — `max_containers=1` guards within the app, but two
  separate ephemeral `modal run` apps are not covered.

### 5b. Track completion out-of-band — the two-source AND sentinel

Do **not** poll the ledger row as the *sole* signal. The ledger row is written only in `build()`'s
`finally:`, which an **OOM SIGKILL (or spot reap) SKIPS** — a killed run writes **NO row**, so a
ledger-only poller waits forever on the previous run's row (or reads a STALE prior `success` and
arms `index` against an unpublished dataset). Decide completion on **Modal app state AND a fresh
ledger row**:

```bash
# (a) Modal job state — authoritative for OOM/SIGKILL/timeout/reap (the ledger cannot see these):
modal app list | grep "$APP_ID"            # State: running → keep polling; stopped → check (b)
modal app logs "$APP_ID" --tail 200        # the merge DONE line OR an OOM/timeout banner
```

```sql
-- (b) ledger row — success confirmation + metric envelope (NOT the sole sentinel):
SELECT status, rows_out, fresh_only_tail, deletes_tombstoned, max_action_date,
       error_message, started_at, completed_at, recorded_at,
       CASE WHEN status='running' AND now()-started_at > interval '9 hours'
            THEN 'STUCK_PRESUMED_KILLED' ELSE status END AS effective_status
FROM ops.usaspending_fpds_canonical_runs
ORDER BY recorded_at DESC LIMIT 1;
```

**Completion DECISION TABLE:**

| Modal app state | Ledger row | Verdict |
|---|---|---|
| `stopped` | fresh `status='success'` + `rows_out≈107.2M` + `max_action_date='2026-06-26'` | **PASS** → arm `index` |
| `stopped` | fresh `status='error'` | **FAIL** — read `error_message`; prod untouched (publish is the last step), re-launch |
| `stopped` | **NO** fresh row (or `effective_status=STUCK_PRESUMED_KILLED`) | **OOM/REAP FAIL** — inspect `modal app logs`; prod untouched; re-launch |
| `running` | (any) | **keep polling** — never `index` yet |

- The build's **return dict is log-only under `--detach`** (`--write-result` is str/bytes only),
  so the metric envelope is gated on the **ledger columns** (`rows_out`, `fresh_only_tail`,
  `deletes_tombstoned`, `max_action_date`), never on the unrecoverable return value.
- If `effective_status=STUCK_PRESUMED_KILLED`, `modal app stop "$APP_ID"` to reclaim the box and
  investigate before any re-launch.
- The `started_at`/`effective_status` columns above assume the RECOMMENDED `status='running'`
  start-row follow-on (see §6); until that lands, "app stopped + NO fresh row" is the OOM/reap
  signal and `STUCK_PRESUMED_KILLED` will not appear — the Modal app state remains authoritative.

### 5c. Optional hardening (small code add, AFTER the first clean run) — server-side `.remote()` chain

> **Skip `run_all` for the first prod run** — run the three `--detach` phases sequentially (§5a),
> gating each on the prior's two-source completion. The chain orchestrator is a convenience to add
> AFTER the first clean manual run, not before it. It does **not** exist in the wrapper today.

If a one-call chain is later wanted, it MUST use `.remote()`, not `.local()`. `.local()` runs each
callee's body **in the caller's container**, ignoring each function's own `memory=`/`timeout=`/
`secrets=` config — so the 96GB-DuckDB merge, the in-RAM 107M BTREE sort, and two full-materialize
verifies would all run in ONE wrong-sized container with the WRONG Secrets (the module's import-time
env reads fall back to `memory_limit=8GB`, `temp_directory=/tmp`, → the merge dies). `.remote()`
dispatches each phase into ITS OWN container with ITS OWN sizing/secrets and gates on the RESULTS:

```python
@app.function(image=image, secrets=[modal.Secret.from_name("hqx-postgres")],
              timeout=60 * 60 * 13, retries=0)   # thin coordinator (4 GiB is plenty); no volumes=
def run_all(since: str | None = None, target_uri: str | None = None) -> dict:
    # .remote() → each phase runs in ITS OWN container with ITS OWN memory/timeout/secrets.
    b  = build_fn.remote(since=since, target_uri=target_uri)      # 128 GiB box, build_env
    assert b.get("status") == "success" and b.get("pk_unique") and b["rows_out"] > 100_000_000, b
    v1 = verify_fn.remote(target_uri=target_uri)                  # 32 GiB box, verify_env
    assert v1["pk_unique"] and v1["rows_out"] > 100_000_000, v1
    i  = index_fn.remote(target_uri=target_uri)                   # 48 GiB box, index_env
    assert i.get("status") == "ok" and i.get("indices_built"), i
    v2 = verify_fn.remote(target_uri=target_uri)
    assert v2["indices"], v2
    return {"build": b, "verify_pre": v1, "index": i, "verify_post": v2}
```

```bash
modal run --detach pipelines/usaspending/usaspending_fpds_canonical_modal.py::run_all
```

The dispatcher is a thin coordinator; each `.remote()` child is a tracked Modal function-call that
survives independently — on a dispatcher reap the children's app/ledger state remain the sentinel.
Keep the individual `build_fn`/`index_fn`/`verify_fn` for blast-radius re-runs of a single phase.

### 5d. Recurring rebuild (after each FRESH advance) — deploy + schedule

For the durable cadence, do not `modal run` at all — **deploy** and let Modal's scheduler invoke it:

```bash
modal deploy pipelines/usaspending/usaspending_fpds_canonical_modal.py
```

Attach a `modal.Cron`/schedule to a chain orchestrator (the `.remote()` `run_all` from §5c, once
added — it does not exist today) (e.g. weekly, lagging the BULK snapshot + FRESH daily append). A
deployed scheduled function has **no client dependency at all** — it cannot be reaped by any
session. Full overwrite per run (the canonical is a reconciled read-model; never incremental).

**Mutual exclusion:** the schedule and any manual `--detach` run must NEVER both be armed —
`max_containers=1` guards within the app, but a cron firing while a manual run is in flight is two
launches on the same R2 prefix. Pause the schedule before any manual launch, and check
`modal app list` for a live `usaspending-fpds-canonical` app first.

## 6. Checklist before launching any giant Modal job

Delta vs the original checklist (corrections applied this pass):

| Item | Original said | Corrected |
|---|---|---|
| Sentinel | "ops-ledger row and/or published artifact" | **WRONG as sole signal.** Sentinel = **Modal app state AND a fresh `status='success'` ledger row** (§5b decision table). The build's return dict is **log-only** under `--detach`. |
| Sizing | "using a `modal.Volume`, not `ephemeral_disk` (spot-trap)" | **REVERSED.** Spill + stage on the **standard 512 GiB local disk** (`/tmp`) — no Volume, no `ephemeral_disk`. Volume is slow for high-churn spill (risks the 8 h timeout); `ephemeral_disk`'s spot-trap is bulk.py lore, not Modal-documented, and moot (512 GiB suffices). |
| Orchestrator | "ideally chained in one detached `run_all`" (`.local()`) | `.local()` = one wrong-sized, wrong-Secrets container → defeats the split. Use `.remote()` if chaining; otherwise run the three `--detach` phases sequentially. **Skip `run_all` for the first prod run.** |
| Double-launch | (absent) | **ADD:** `max_containers=1` on `build_fn` + `index_fn`. Pause the §5d schedule before any manual run; check `modal app list` for a live app first. |
| DuckDB mem | (implicit 64GB) | **ADD:** `FPDS_CANONICAL_DUCKDB_MEM=96GB` on the 128 GiB box (less spill → finishes in time). |
| Smoke gate | (absent) | **ADD:** `modal run …::smoke` as a MANDATORY first step; gate on `status: ok`. |
| init_ops | "idempotent; also self-bootstraps" | **ADD:** run `init_ops` once via doppler as a **MANDATORY** pre-step (not either/or) so the table pre-exists before any concurrent self-bootstrap. |
| Index integrity | (only "verify after") | **ADD explicitly:** treat "index launched" as NON-TERMINAL until the **post-index verify passes** — that gate catches a partial-upload (HIGH-2). |
| App id | (absent) | **ADD:** capture the `ap-…` id from the detached launch (`tee` + `grep`) — the handle for `modal app logs`/`modal app stop`/liveness polling. |

Corrected checklist:

- [ ] Job is overwrite/idempotent and `retries=0` → detach is safe (else fix idempotency first).
- [ ] `init_ops` applied once via doppler (MANDATORY pre-step) — never rely on the self-bootstrap under concurrency.
- [ ] `::smoke` run and `status: ok` (MANDATORY) before committing the giant box.
- [ ] Launch with `modal run --detach` (or a deployed/scheduled function) — **never** a session-bound `modal run` or a long background local-CLI process. Capture the `ap-…` id (`tee` + `grep`).
- [ ] Completion decided by **Modal app state AND a fresh `status='success'` ledger row** (§5b table) — never the ledger alone (OOM/reap writes no row). Return dict is log-only under `--detach`.
- [ ] Kill switch known (`modal app stop <id>`); logs reachable (`modal app logs <id>` / dashboard).
- [ ] Sized for the work — spill + stage on the **standard 512 GiB local disk** (no Volume, no `ephemeral_disk`); `FPDS_CANONICAL_DUCKDB_MEM=96GB` on the 128 GiB box.
- [ ] Double-launch guarded: `max_containers=1` on `build_fn`/`index_fn`; §5d schedule paused before any manual run; `modal app list` checked for a live app first.
- [ ] Phases gated on each other's success (build → verify → index → verify); "index launched" is NON-TERMINAL until the post-index verify passes (HIGH-2). Run the three `--detach` phases sequentially for the first prod run (skip `run_all`).

**RECOMMENDED low-risk follow-on (deferred this pass):** add a `status='running'` start-row +
in-progress ledger guard to the shipped module's `_record_run` path (a small additive change:
allow `status IN ('running','success','error')`, insert the row at `build()` start, upsert-by-id
to its terminal state in the `finally`). It makes "no fresh row" unambiguous (a stuck `'running'`
row older than the timeout = presumed killed) and powers the `effective_status` /
`STUCK_PRESUMED_KILLED` column in §5b. **Deferred** to keep the sample-validated merge module
(`usaspending_fpds_canonical.py`) untouched this pass — the two-source completion gate already
makes OOM/reap detectable (app stopped + no fresh row) without it.

## 7. General takeaway for the fleet

Watching a job is for visibility, not durability. **Durability comes from decoupling the job from
the session and recording a sentinel.** This applies to every long Modal job, heavy index rebuild,
and multi-hour materialization in this repo — not just the FPDS canonical.
