# Adversarial remediation — MODAL_GIANT_EXECUTION_DURABILITY.md

Reviewer mandate: assume the durability runbook is flawed; find every way the REAL 107M-row
run fails, hangs, double-writes, OOMs without a trace, or goes unobservable; specify surgical
fixes. READ-ONLY on infra — nothing was launched, stopped, or mutated.

All Modal claims below were verified against the **installed CLI (modal 1.4.1)** and the
**modal.com/docs** pages cited inline, cross-checked against the proven giant
`pipelines/usaspending/usaspending_bulk.py`. Nothing here is asserted from memory.

---

## 0. Verdict

**Not runnable as-described without correcting two BLOCKERs.** The plan's core mechanics
(`modal run --detach`, `retries=0`, overwrite idempotency) are sound, and the §5a immediate
path is *almost* right. But:

- **The single biggest risk is the completion sentinel itself (BLOCKER-1).** The runbook tells
  the operator to track completion by polling the `ops.*_runs` ledger row, which is written
  ONLY in `build()`'s `finally:`. The worst failure modes — **OOM (SIGKILL) and any
  infra/spot reap** — bypass Python entirely, so `finally:` never runs and **NO ledger row is
  ever written.** A poller watching "latest row flips to success/error" then waits **forever**
  on the previous run's row (or on nothing). The sentinel is silently absent in exactly the
  scenario that motivated this runbook. The authoritative completion signal for a detached run
  is the **Modal function-call / app state** (`modal app logs <id>`, dashboard), with the
  ledger as a *secondary* success confirmation — the runbook has this backwards.

- **The §5c `run_all` orchestrator is wrong as written (BLOCKER-2):** `build_fn.local()` /
  `index_fn.local()` do **not** dispatch new containers. `.local()` runs the callee's body
  **in the calling container**, ignoring each function's own `memory=`/`timeout=`/`secrets=`
  config. So `run_all` would run the 64 GB-DuckDB merge, the in-RAM 107M BTREE sort, and two
  full-materialize verifies **all in one container** governed solely by `run_all`'s own knobs —
  and, fatally, **with the wrong Secrets**: `run_all` as sketched injects no `build_env`/
  `index_env`/`verify_env`, so the shipped module's import-time env reads fall back to
  `memory_limit=8GB`, `temp_directory=/tmp`, `SCRATCH=/tmp/...` and the merge dies.

Secondary but real: the Volume is the **wrong tool for spill** (network-backed, and unnecessary
— containers already get a 512 GiB local disk by default), there is **no double-launch guard**,
and `index_fn` has a **partial-upload corruption window** on a mid-upload kill.

---

## 1. What was VERIFIED (grounding)

| Claim | Verified source | Result |
|---|---|---|
| Modal client version | `modal --version` | `modal client version: 1.4.1` |
| `--detach` exists, frees the shell | `modal run --help`; docs/reference/cli/run | `-d, --detach  Don't stop the app if the local process dies or disconnects.` Returns immediately; remote survives client exit. |
| `--write-result` only takes str/bytes | `modal run --help` | `-w, --write-result TEXT  Write return value (which must be str or bytes) to this local path.` A **dict return value is NOT recoverable** from a detached run. |
| `.local()` runs in the CALLER's container | docs/reference/modal.Function; docs/guide/apps | `.local()` executes the body in the caller's environment/process — **no new container, callee resource config ignored.** `.remote()` dispatches a new container with the callee's own config. |
| OOM = SIGKILL (no `finally`) | docs/guide/resources | "Modal containers can have a hard memory limit which will 'Out of Memory' (OOM) kill containers which attempt to exceed the limit." OOM kill = SIGKILL → Python `finally:`/`except:` do **not** run. |
| Timeout = catchable `FunctionTimeoutError` | docs/guide/timeouts | "a timeout in a Function will produce a `modal.exception.FunctionTimeoutError` which you may catch." It is a `BaseException` → `build()`'s `except BaseException` + `finally` **do** run → an 'error' ledger row IS written on timeout. |
| Volume auto-commits without `.commit()` | docs/guide/volumes | "Modal Volumes run background commits: every few seconds while your Function … executes, the contents of attached Volumes will be committed without your application code calling `.commit`." + "A final snapshot and commit is also automatically performed on container shutdown." |
| Volume mounts latest committed state at start; no auto-reload after | docs/guide/volumes | "At container creation time the latest state of an attached Volume is mounted." Later external commits need `.reload()`. |
| Volume concurrency = last-write-wins | docs/guide/volumes | "Last write wins in case of concurrent modification of the same file — any data the last writer didn't have when committing changes will be lost!" |
| Default container disk = 512 GiB | docs/guide/resources | "a per-container disk quota that defaults to 512 GiB." (i.e. spill does NOT need a Volume) |
| `ephemeral_disk` max + billing | docs/guide/resources | "The maximum disk size is 3.0 TiB." "Disk requests are billed by increasing the memory request at a 20:1 ratio." **The docs do NOT state that a large `ephemeral_disk` forces spot/preemptible capacity** — that claim lives only in `usaspending_bulk.py` comments; treat it as project lore, not Modal-documented fact. |
| `modal app list` shows running/recent apps; `modal app logs <id>` / `<name>` | `modal app --help`, `modal app logs --help`, `modal app list` (read-only) | `logs` accepts an app-id (`ap-…`) for ephemeral runs, or a name for deployed apps. `modal app list` lists deployed/running/recently-stopped with App ID — the way to find a detached run's `ap-…`. |
| `fpds-canonical-vol` exists; currently clean | `modal volume list`, `modal volume ls fpds-canonical-vol` | Volume present (created 2026-06-28 18:49 EDT, same day as the reaped run); `ls` returned empty → no residue right now. |
| `bulk.py` giant path uses NO Volume | `usaspending_bulk.py` L744-755, L838-852 | The 43 GiB-gz giant ingest used `ephemeral_disk=512*1024` then the preemption-safe replacement streamed via httpfs to **standard `/tmp`** — **never a `modal.Volume`.** The wrapper's "Volume = bulk.py giant lesson" is a misreading. |
| `_record_run` writes ONLY terminal rows | `usaspending_fpds_canonical.py` L609-635, L782-792; `ops_*.sql` | Ledger has `status text NOT NULL -- 'success' | 'error'` only. **No 'running' / in-progress row is ever written at build start.** |

---

## 2. Severity-ranked defects

### BLOCKER-1 — The completion sentinel does not exist on the failures that matter (OOM / reap)

**Location:** runbook §5b ("Track completion out-of-band … Poll the durable signals" → the
`ORDER BY recorded_at DESC LIMIT 1` query) and §6 checklist line "A durable completion sentinel
exists (ops-ledger row…)". Backed by `usaspending_fpds_canonical.py` `build()` L778-792
(`except BaseException` + `finally: _record_run(...)`) and `_record_run` L609-635.

**Why the real run hangs / goes unobserved:**
- The ledger row is written in `build()`'s `finally:`. `finally:` runs for a normal exception
  and for `KeyboardInterrupt` (the 13-min reap that prompted this runbook — that is why an
  'error' row appeared that time) and, per docs/guide/timeouts, **also for a Modal timeout**
  (`FunctionTimeoutError` is a catchable `BaseException`). Those are the *recoverable* cases.
- But an **OOM kill is a SIGKILL** (docs/guide/resources). SIGKILL cannot be caught and runs no
  Python — `finally:` never executes, **no ledger row is written.** Same for a host/spot reap.
- The runbook's sentinel is "latest ledger row flips to success/error." On an OOM/reap there is
  no new row, so the *previous* run's row remains "latest" — a poller (and the gate that arms
  `index_fn`) either waits forever or, worse, reads a STALE prior 'success' and **launches
  `index` against a half-written / unpublished dataset.** Note publish is the last build step,
  so an OOM mid-merge leaves prod untouched — but the *observer* cannot tell that from the
  ledger alone.
- Compounding: even on the happy path, the build's **return-value dict is unrecoverable** under
  `--detach` (`--write-result` is str/bytes only), so the metric envelope the runbook wants to
  gate on (`rows_out≈107.2M`, `pk_unique`, …) is only in logs + the ledger row. If the ledger
  row is the sentinel and it can be silently absent, there is no envelope to gate on.

**Surgical fix — make Modal's own job state the authoritative sentinel; keep the ledger as a
secondary success confirmation, and add a start-row + heartbeat so absence is detectable:**

1. **Authoritative liveness/terminal signal = Modal app state, not the ledger.** Capture the
   `ap-…` id at launch and poll Modal, which records SIGKILL/OOM/timeout as a terminal app state
   the ledger cannot:
   ```bash
   # capture the app id the detached run prints (stdout/stderr) into a file
   modal run --detach pipelines/usaspending/usaspending_fpds_canonical_modal.py::build \
     2>&1 | tee /tmp/fpds_build_launch.log
   APP_ID=$(grep -oE 'ap-[A-Za-z0-9]+' /tmp/fpds_build_launch.log | head -1)
   # liveness/terminal state (read-only):
   modal app list | grep "$APP_ID"          # State column: running / stopped / …
   modal app logs "$APP_ID" --tail 200      # last lines incl. the OOM/timeout banner
   ```
   Success = Modal app reaches `stopped` with a clean final log AND the ledger shows a fresh
   `status='success'` row whose `recorded_at` is newer than launch. **Failure (OOM/reap) = Modal
   app stopped with NO matching fresh ledger row** — this is the detectable signal the runbook is
   missing. Document this two-source AND, not the ledger alone.

2. **Add a 'running' start-row + heartbeat (small code add, recommended).** So "no fresh row"
   stops being ambiguous. In `build()`, BEFORE the work, insert a `status='running'` row
   (`started_at=now()`), and in the `finally` UPDATE the same row to its terminal state. Then:
   - OOM/reap → the row is stuck at `'running'` with a stale `started_at` → a poller that sees
     `'running'` older than (timeout + slack) declares **failed**, never hangs.
   - This needs a schema delta: allow `status IN ('running','success','error')` (drop the
     implicit two-value contract; the column is already free-text `text NOT NULL`) and make
     `_record_run` do upsert-by-id. Keep `error_message` NULL while running.
   ```sql
   -- detection that covers success AND silent OOM/reap:
   SELECT status, rows_out, pk_unique_proxy := (rows_out IS NOT NULL),
          max_action_date, error_message, started_at, completed_at, recorded_at,
          CASE WHEN status='running' AND now() - started_at > interval '9 hours'
               THEN 'STUCK_PRESUMED_KILLED' ELSE status END AS effective_status
   FROM ops.usaspending_fpds_canonical_runs
   ORDER BY recorded_at DESC LIMIT 1;
   ```

3. **Persist the metrics dict as a side effect, not as the return value.** The dict is already
   captured in the ledger columns (`rows_out`, `fresh_only_tail`, `deletes_tombstoned`,
   `max_action_date`) — so gate `index` on those ledger columns, NOT on the unrecoverable return
   dict. Document that the return dict is log-only under `--detach`.

---

### BLOCKER-2 — `run_all` (§5c) uses `.local()` → one oversized container, wrong Secrets, no blast-radius split

**Location:** runbook §5c, the `run_all` sketch (`b = build_fn.local(...)`, `i = index_fn.local(...)`).

**Why it fails the real run:**
- **`.local()` is in-process, not a new container** (verified: docs/reference/modal.Function,
  docs/guide/apps). So `build_fn.local()` executes `build_fn`'s body inside `run_all`'s
  container. `build_fn`'s decorator config — `memory=131072`, `cpu=16.0`, `timeout=8h`,
  `secrets=[r2, hqx, build_env]`, `volumes=` — is **entirely ignored.** Same for `index_fn`
  (which is supposed to run with `index_env` + `LANCE_BYPASS_SPILLING=true`) and `verify_fn`
  (`verify_env`).
- **Wrong/absent Secrets = the merge dies on import.** The §5c `run_all` decorator lists only
  `secrets=[...]` (placeholder) and does NOT inject `build_env`/`index_env`/`verify_env`. The
  shipped module reads `FPDS_CANONICAL_SCRATCH` / `DUCK_TMP` / `DUCK_MEM` / `DUCK_THREADS` at
  **import time** (`usaspending_fpds_canonical.py` L85-88). With those env vars absent the
  module resolves to `SCRATCH=/tmp/fpds_canonical_stage`, `DUCK_MEM=8GB`, `DUCK_TMP=/tmp/...`,
  `DUCK_THREADS=4`. An 8 GB DuckDB memory_limit on the 107M `bulk_latest` collapse spills
  catastrophically to a single `/tmp` dir and almost certainly OOMs or errors. Worse, because
  `index_fn`'s body is reached via `.local()` in the SAME long-lived process, **`LANCE_BYPASS_SPILLING`
  must be set before the FIRST `import lance` anywhere in the container** — but `build_fn`'s body
  already imported lance under build semantics, and the index step's required env never arrived.
- **Defeats the blast-radius split** the wrapper docstring and runbook §6 explicitly want
  (build → verify → index → verify as separate invocations). One container failing at the index
  step takes the whole 12 h job down; you cannot re-run just `index` from where it died.
- **12 h timeout realism:** build alone is budgeted at 8 h; chaining build+verify+index+verify
  serially under one 12 h cap leaves ~4 h for a full 107M re-materialize verify (×2) + the
  in-RAM BTREE sort of 16 columns. On the network-Volume spill (see BLOCKER-3) that is tight to
  the point of likely timeout.

**Surgical fix — if a one-call chain is wanted, make it a `.spawn()`/`.remote()` server-side
orchestrator that dispatches the real, individually-sized functions and gates on their RESULTS,
not `.local()`:**
```python
@app.function(image=image, secrets=[modal.Secret.from_name("hqx-postgres")],
              timeout=60 * 60 * 13, retries=0)   # generous wall-clock for the dispatcher ONLY
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
- `run_all` itself is a thin coordinator (4 GiB is plenty) — its `memory=131072` in §5c is wasted
  spend; each `.remote()` child gets the right box.
- The dispatcher container can still be reaped, but each `.remote()` child is a tracked Modal
  function-call that survives independently; on dispatcher reap the children's ledger/app state
  remain the sentinel.
- **Simplest correct option (RECOMMENDED for the first prod run): skip `run_all` entirely.** Run
  the three `--detach` invocations sequentially from §5a, gating each on the prior's verified
  ledger row + Modal app state (procedure in §3). The chain orchestrator is a convenience to add
  AFTER the first clean manual run, not before it.

---

### BLOCKER-3 — Volume-backed DuckDB spill is the wrong tool, slower, and unnecessary; the bulk.py "lesson" is misread

**Location:** wrapper L139-167 (`vol`, `build_env` `FPDS_CANONICAL_DUCKDB_TEMP_DIR=/vol/.../duckdb_spill`),
L237-264 (`build_fn` 128 GiB box, spill on Volume), wrapper docstring L74-76 ("modal.Volume …
NEVER ephemeral_disk … usaspending_bulk.py giant lesson"); runbook §6 checklist "using a
`modal.Volume`, not `ephemeral_disk` (spot-preemption trap)".

**Why it degrades / risks the real run:**
- **Containers already get 512 GiB of local disk by default** (verified: docs/guide/resources,
  "a per-container disk quota that defaults to 512 GiB"). The merge's ~100-180 GiB DuckDB spill
  fits on the standard local `/tmp` with no Volume and no `ephemeral_disk` request at all.
- **Volumes are network-backed.** docs/guide/volumes describes background commits "every few
  seconds" of the *entire* attached Volume contents — i.e. a high-churn DuckDB spill directory on
  a Volume is being continuously snapshotted/committed over the network. Spill is exactly the
  random-read/write, write-heavy, throwaway workload Volumes are worst at. At network-Volume
  spill speed the 107M `bulk_latest` collapse is at material risk of NOT finishing inside the 8 h
  `build_fn` timeout — directly answering review axis 6.
- **The bulk.py precedent says the OPPOSITE of the wrapper's claim.** bulk.py's giant ingest
  (`usaspending_bulk.py` L744-755) used `ephemeral_disk=512*1024` and DuckDB spill on `/tmp`
  (L806 `PRAGMA temp_directory='{SCRATCH_DIR}/duckdb_spill'`, `SCRATCH_DIR="/tmp"`). The
  "preemption-safe" replacement (L838-852, `ingest_stream_table`) **dropped `ephemeral_disk` and
  used the standard container disk** — it NEVER introduced a `modal.Volume`. The wrapper cites
  this as the reason to use a Volume; it is in fact the reason to use **standard `/tmp`**.
- The `ephemeral_disk` "spot-preemption trap" is real project lore from bulk.py but is **not in
  the Modal resources doc** — the doc only states the 20:1 memory-billing ratio and the 3 TiB cap.
  The fix below avoids `ephemeral_disk` anyway (512 GiB default suffices), so the trap, real or
  not, does not apply.

**Surgical fix — point spill + stage at the container's standard local disk; drop the Volume for
`build_fn` and `verify_fn`:**
```python
# build_env: spill + stage on the standard 512 GiB local disk, NOT the network Volume.
build_env = modal.Secret.from_dict({
    "FPDS_CANONICAL_SCRATCH": "/tmp/fpds_canonical/stage",
    "FPDS_CANONICAL_DUCKDB_TEMP_DIR": "/tmp/fpds_canonical/duckdb_spill",
    "FPDS_CANONICAL_DUCKDB_MEM": "96GB",   # raise from 64GB: less spill on a 128 GiB box (see B-6)
    "FPDS_CANONICAL_DUCKDB_THREADS": "8",
    "LANCE_BYPASS_SPILLING": "true",
})
# build_fn: drop volumes={...}; the merge needs no cross-run persistence (publish is to R2).
@app.function(image=image, secrets=[r2, hqx, build_env],
              memory=131072, cpu=16.0, timeout=60*60*8, retries=0)   # no volumes=
def build_fn(...): ...
```
- `index_fn` legitimately needs a sizable local staging area to download the ~50-80 GiB published
  dataset; the **512 GiB standard disk covers that too** → drop its Volume as well and stage under
  `/tmp/fpds_canonical/idx_stage`. (If you ever exceed 512 GiB, request `ephemeral_disk`, not a
  Volume — the workload is scratch, not shared state.)
- Removing the Volume also eliminates BLOCKER-4's residue/collision surface entirely.

---

### BLOCKER-4 — Shared scratch Volume → cross-run residue + last-write-wins collision

**Location:** wrapper L139-142 (one `vol` shared by `build_fn` + `index_fn`), L246-263
(`build_fn` `os.makedirs(..., exist_ok=True)` then relies on the module's `rmtree` of SCRATCH;
spill dir `rmtree` in `finally`), L310-312 (`index_fn` `rmtree(local_ds)` at start).

**Why it can corrupt / mislead (only relevant if BLOCKER-3's Volume is kept):**
- A new container "mounts the latest committed state of the Volume at creation" (docs/guide/volumes).
  Because the Volume **background-commits every few seconds**, a container that is reaped mid-run
  leaves its partial `duckdb_spill/` and `stage/canonical_lance/` **committed and visible** to the
  next run. The wrapper's `finally: shutil.rmtree(duckdb_spill)` does NOT run on a SIGKILL/reap
  (same root cause as BLOCKER-1), so residue accrues exactly when you least expect it.
- `build_fn` pre-creates dirs with `exist_ok=True` but only `build()` (the module) rmtrees
  `SCRATCH`; the **spill dir is rmtree'd only in the wrapper `finally`** → on a reap it is not
  cleaned, and a subsequent run's DuckDB writes into a dir that may contain a prior run's spill
  pages. DuckDB temp files are run-scoped, so this is mostly wasted space, but it defeats the
  "steady-state occupancy ~0" claim and can exhaust the Volume.
- **Two concurrent `build_fn` runs** (see BLOCKER-5) both target `stage/canonical_lance` and
  `duckdb_spill` on the SAME Volume → "last write wins … any data the last writer didn't have …
  will be lost" (docs/guide/volumes). Interleaved Lance-stage writes from two builds = a corrupt
  local dataset that then gets published.

**Surgical fix:**
- Primary: adopt BLOCKER-3 (no Volume → no shared committed scratch).
- If a Volume is retained for any reason, namespace every run's scratch by a unique token and
  rmtree at START (not only in `finally`), so a reap-orphaned tree is never reused:
  ```python
  run_tok = os.environ.get("MODAL_TASK_ID", "local")  # unique per container
  scratch = f"{VOL_MOUNT}/stage-{run_tok}"
  ```
  and add a periodic GC function that prunes scratch dirs older than the max timeout.

---

### HIGH-1 — No double-launch guard: two builds → interleaved DeleteObjects + uploads on the same R2 prefix

**Location:** runbook §5a (manual `modal run --detach …::build`), §5d (deploy + cron) overlapping
a manual run; publish path `usaspending_fpds_canonical.py` `_publish_local_to_r2` L564-586
(wipe-then-upload to `CANONICAL_URI` prefix).

**Why it corrupts:** `retries=0` stops *auto*-retries, but nothing stops a second `modal run
--detach …::build` (operator re-launch, or a §5d cron firing while a manual run is in flight).
Two builds both run `_publish_local_to_r2` against the same prefix: build A's `delete_objects`
batch can land between build B's upload and its manifest write, leaving a prefix that is neither
A's nor B's — a Lance dataset whose `_versions/latest_version_hint.json` points at a manifest
whose data files were deleted by the other run. There is no lock, no `concurrency_limit`, no
in-progress ledger check.

**Surgical fix (defense in depth):**
1. **`max_containers=1` on `build_fn`** (and on `index_fn`) — Modal will not run two
   simultaneously *within the same app deployment*. (Does not stop two separate `modal run`
   ephemeral apps; hence #2.)
2. **In-progress ledger guard** (rides on BLOCKER-1's 'running' row): at the top of `build()`,
   `SELECT 1 FROM ops.…_runs WHERE status='running' AND now()-started_at < interval '8 hours'`;
   if found, abort with a clear "build already in progress" error.
3. **Operationally:** the §5d cron and any manual run must never both be armed. Document that the
   schedule is paused before any manual `--detach` build, and that `modal app list` is checked for
   a live `usaspending-fpds-canonical` app before launching.

---

### HIGH-2 — `index_fn` partial-upload corruption window on a mid-upload kill

**Location:** wrapper `index_fn` L411-431 (ordered upload loop), L428-431 (hint uploaded LAST).

**Why it corrupts:** The ordering (indices/transactions → manifests → hint LAST) is correct and
careful, and `retries=0` prevents a re-append. BUT if the container is **OOM/timeout/reap-killed
between "manifests uploaded" and "hint uploaded"**, R2 holds new `_indices/**` + new
`_versions/*.manifest` but the hint still points at the OLD manifest → readers are fine (old,
un-indexed, but consistent). The genuinely dangerous window is a kill **between completing some
`_indices/**`/`_transactions/**` and finishing all `*.manifest`**: a manifest may already be
present referencing index payload that did not finish uploading, OR the hint gets written
(it does not, since it is last) — net: a *new manifest exists on R2 referencing a possibly
incomplete `_indices` payload*. A reader that picks that manifest (e.g. via `_transactions` log
scan rather than the hint) can hit a dangling index file.

**Severity is HIGH not BLOCKER** because: (a) the hint-last ordering means the default read path
still resolves to the old good version, and (b) data files are never touched. But it is a real
unobserved-corruption path under exactly the kill modes this runbook exists to handle.

**Surgical fix:**
- After `index_fn` completes, **re-run `verify_fn`** (the runbook already gates on this) AND have
  `verify()` assert the index set is non-empty and the row count matches — a half-uploaded index
  set will surface as a verify failure, not silent corruption. Make the §3 procedure treat
  "index launched" as non-terminal until the post-index verify passes (the runbook says this in
  §6 but the §5a immediate path does not enforce it — wire the gate explicitly).
- Optionally, upload index payload to a **staging key prefix**, then atomically flip via a single
  hint write only after all payload+manifests are confirmed present (closes the window fully).
  Given hint-last already protects the default path, the post-index verify gate is the
  proportionate fix; the staging-prefix flip is the belt-and-suspenders upgrade.

---

### MEDIUM-1 — `memory_limit=64GB` on a 128 GiB box spills more than necessary

**Location:** wrapper `build_env` L149 (`FPDS_CANONICAL_DUCKDB_MEM=64GB`), `build_fn` `memory=131072`.

**Why it matters:** On a 128 GiB container, capping DuckDB at 64 GB forces ~half of available RAM
unused while pushing the `bulk_latest` window-collapse to spill earlier and harder — and (pre
BLOCKER-3 fix) that spill is on a slow network Volume. bulk.py's giant ingests ran DuckDB with
`PRAGMA threads=8` and let the engine use the box; the FPDS merge has more headroom available
than it is taking.

**Surgical fix:** Raise `FPDS_CANONICAL_DUCKDB_MEM` to `96GB` (leaving ~32 GiB for Arrow drain +
the Lance write reader + OS page cache on the 128 GiB box). Combined with BLOCKER-3 (local-disk
spill), this is the single biggest lever on whether the merge finishes inside 8 h. Keep
`max_bytes_per_file`/`max_rows_per_file` as-is.

---

### MEDIUM-2 — `--since=""` empty-string passthrough is correct, but the bare-target-form launch in the runbook bypasses the local entrypoints' None-coercion

**Location:** runbook §5a launches `…::build` (local_entrypoint, which coerces `since or None`),
but the wrapper docstring RUN SEQUENCE (L35) and §0 of this review's reproduction launch
`…::build_fn` (the raw function). The local entrypoint `build` (wrapper L533-541) maps
`since=""` → `None`; the raw `build_fn` takes `since: str | None = None` and a bare
`modal run …::build_fn` with no `--since` passes `None` correctly too.

**Why it matters (minor):** Mixing `::build` (entrypoint) and `::build_fn` (function) in different
docs invites an operator to pass `--since ""` to the raw function, which would inject
`action_date >= DATE ''` → SQL error. Low blast radius (errors loud, pre-publish) but avoidable.

**Surgical fix:** Standardize the runbook on the **local entrypoints** (`::build`, `::index`,
`::verify`, `::run_all` once added) everywhere — they exist precisely to coerce args and are the
"explicit, documented drivers" (wrapper L512-514). Never document the bare `::build_fn` form for
prod.

---

### MEDIUM-3 — `init_ops` race + ledger self-bootstrap under concurrent first run

**Location:** runbook §5a step 1 (`init_ops` via doppler) vs `_record_run` self-bootstrap
(`usaspending_fpds_canonical.py` L621-623, `to_regclass` then create-if-missing).

**Why it matters (minor):** If `init_ops` is skipped and two functions both first hit
`_record_run`, both may see `to_regclass IS NULL` and both run the DDL. The DDL is
`CREATE … IF NOT EXISTS`, so it is idempotent and safe — but two concurrent `CREATE SCHEMA/TABLE`
can deadlock or error on some Postgres configs. With BLOCKER-1's start-row, the very first thing
each build does is write the ledger, raising the concurrency odds.

**Surgical fix:** Keep §5a's explicit `init_ops` as a **mandatory pre-step**, not "either/or".
Run it once via doppler before any build so the table always pre-exists and the self-bootstrap
path is never the creator under concurrency.

---

### LOW-1 — Image ships under `--detach` fine, but verify the smoke gate is actually run

**Location:** wrapper L120-135 (image build with `add_local_python_source` + `add_local_file`),
runbook §5a (no smoke step) vs wrapper RUN SEQUENCE step 0a (`smoke_fn`).

**Why it matters (minor):** `--detach` does not change image building — the image is built/loaded
at app-create time on the client before detaching, so packaging errors surface immediately, not
silently mid-run. But the runbook §5a **omits the cheap `smoke` gate** that the wrapper docstring
mandates as step 0a. Skipping it risks discovering a packaging/secret/import fault only after
committing the 128 GiB box.

**Surgical fix:** Add `modal run …::smoke` as the first line of the §5a procedure (it is
foreground, ~pennies, ~seconds) and gate everything on its `status: ok`.

### LOW-2 — Cost blowup if a detached build hangs unnoticed

**Location:** runbook §5d (deploy + cron), §5b (poll, don't hold).

**Why it matters:** A 128 GiB / 16-CPU box for up to 8 h is expensive; a detached run that
silently OOM-loops (it cannot — `retries=0`) or a cron that overlaps (HIGH-1) multiplies spend.
With `retries=0` the single-run cost is bounded, but an unobserved `'running'`-stuck container
(BLOCKER-1) bills until the timeout.

**Surgical fix:** The BLOCKER-1 'running'-row + `modal app list` liveness check caps this — a
stuck run is detected within the poll interval and `modal app stop <id>` reclaims the box. Add an
explicit "if `effective_status=STUCK_PRESUMED_KILLED`, `modal app stop` and investigate" line.

---

## 3. Corrected, copy-pasteable run procedure (success AND OOM/timeout detection)

> Assumes BLOCKER-3 applied (spill on local `/tmp`, no Volume) OR, if not yet applied, the run
> still works but slower and with the residue caveat. Detection below works regardless.

```bash
cd /Users/benjamincrane/core-x/.claude/worktrees/objective-brahmagupta-c83ef5

# ── 0. one-time ops DDL (mandatory pre-step; do NOT rely on self-bootstrap) ──
doppler run -p core-x -c prd -- \
  python3 -m pipelines.usaspending.usaspending_fpds_canonical init_ops

# ── 0a. cheap packaging/secrets/import gate (foreground, seconds, pennies) ──
modal run pipelines/usaspending/usaspending_fpds_canonical_modal.py::smoke
#   require: {"status": "ok", "column_spec_ok": true, "r2_endpoint_present": true}

# ── 1. BUILD — detached. Capture the app id (it is printed to stdout/stderr). ──
modal run --detach pipelines/usaspending/usaspending_fpds_canonical_modal.py::build \
  2>&1 | tee /tmp/fpds_build_launch.log
APP_ID=$(grep -oE 'ap-[A-Za-z0-9]+' /tmp/fpds_build_launch.log | head -1); echo "APP_ID=$APP_ID"

# ── 2. COMPLETION DETECTION (two-source AND — covers OOM/reap, not just clean exit) ──
#   (a) Modal job state — authoritative for OOM/SIGKILL/timeout/reap:
modal app list | grep "$APP_ID"            # State: running → keep polling; stopped → check (b)+(c)
modal app logs "$APP_ID" --tail 200        # look for the merge DONE line OR an OOM/timeout banner
#   (b) ledger row — success confirmation + metric envelope (NOT the sole sentinel):
doppler run -p core-x -c prd -- psql "$HQX_DB_URL_POOLED" -c "
  SELECT status, rows_out, fresh_only_tail, deletes_tombstoned, max_action_date,
         error_message, started_at, completed_at, recorded_at,
         CASE WHEN status='running' AND now()-started_at > interval '9 hours'
              THEN 'STUCK_PRESUMED_KILLED' ELSE status END AS effective_status
  FROM ops.usaspending_fpds_canonical_runs ORDER BY recorded_at DESC LIMIT 1;"
#   DECISION TABLE:
#     app=stopped + ledger status='success' + rows_out≈107.2M + max_action_date='2026-06-26' → PASS → step 3
#     app=stopped + ledger status='error'                                                    → FAIL (read error_message; prod untouched, publish is last)
#     app=stopped + NO fresh ledger row (or effective_status=STUCK_PRESUMED_KILLED)          → OOM/REAP → FAIL: inspect `modal app logs`, prod untouched, re-launch step 1
#     app=running                                                                            → keep polling; never `index` yet

# ── 3. INDEX — detached, ONLY after step 2 PASS ──
modal run --detach pipelines/usaspending/usaspending_fpds_canonical_modal.py::index \
  2>&1 | tee /tmp/fpds_index_launch.log
IDX_APP_ID=$(grep -oE 'ap-[A-Za-z0-9]+' /tmp/fpds_index_launch.log | head -1)
modal app list | grep "$IDX_APP_ID"        # poll to 'stopped'
modal app logs "$IDX_APP_ID" --tail 200    # require BTREE/BITMAP ✓ lines + n_uploaded>0

# ── 4. VERIFY — foreground (1 h box); this is the index-corruption gate (HIGH-2) ──
modal run pipelines/usaspending/usaspending_fpds_canonical_modal.py::verify
#   require: pk_unique=true, rows_out≈107.2M, built_at_distinct=1, indices non-empty,
#            canonical_source_distribution sane. A half-uploaded index surfaces HERE.

# ── kill switch (any phase) ──
modal app stop "$APP_ID"     # detached app survives client exit; this is how you reclaim the box
```

**Why this is the fix:** completion is decided by **Modal app state AND a fresh ledger row**, so
an OOM/reap (no ledger row) is an explicit FAIL state, not an infinite wait. `index` is armed only
on a verified `success`. The post-index `verify` closes HIGH-2's partial-upload window. The return
dict is never relied on (it is unrecoverable under `--detach`).

---

## 4. Pre-launch checklist — delta vs runbook §6

Runbook §6 items kept (good): overwrite+`retries=0`; launch detached/deployed; kill switch known;
phases gated build→verify→index→verify.

**Corrected / added:**

| § | Runbook §6 says | Correction / addition |
|---|---|---|
| Sentinel | "durable completion sentinel exists (ops-ledger row…)" | **WRONG as sole signal.** Sentinel = **Modal app state AND a fresh ledger row**. Add a `status='running'` start-row + stuck-detection so OOM/reap (no terminal row) is detectable, never an infinite wait. The build's return dict is **log-only** under `--detach`. |
| Sizing | "using a `modal.Volume`, not `ephemeral_disk` (spot-preemption trap)" | **REVERSED.** Spill belongs on the **standard 512 GiB local disk** (`/tmp`), not a network Volume (slow for high-churn spill; risks the 8 h timeout). `ephemeral_disk`'s spot-trap is bulk.py lore, not Modal-documented; the 512 GiB default makes it moot. bulk.py's giant path used **no Volume**. |
| Orchestrator | "ideally chained in one detached `run_all`" (`.local()`) | `.local()` runs in-process in ONE wrong-sized, wrong-Secrets container → defeats the split. Use `.remote()` if chaining; otherwise run the three `--detach` phases sequentially. **Skip `run_all` for the first prod run.** |
| Double-launch | (absent) | **ADD:** `max_containers=1` on build/index + an in-progress ledger guard. Pause the §5d cron before any manual run; check `modal app list` for a live `usaspending-fpds-canonical` first. |
| DuckDB mem | (implicit 64GB) | **ADD:** raise `FPDS_CANONICAL_DUCKDB_MEM` to `96GB` on the 128 GiB box (less spill → finishes in time). |
| Smoke gate | (absent from §5a) | **ADD:** `modal run …::smoke` as a mandatory first step; gate on `status: ok`. |
| init_ops | "either local OR remote" | **ADD:** run `init_ops` once via doppler as a **mandatory** pre-step (not either/or) so the ledger table pre-exists before any concurrent self-bootstrap. |
| Index integrity | (covered only by "verify after") | **ADD explicitly:** treat "index launched" as non-terminal until the **post-index verify passes** — that gate is what catches a partial-upload (HIGH-2). |
| App id | (absent) | **ADD:** capture the `ap-…` id from the detached launch (`tee` + `grep`) — it is the handle for `modal app logs`/`modal app stop` and for liveness polling. |

---

## 5. One-line bottom line

The detach decision is correct; the **observability and orchestration are not**. Fix the sentinel
(Modal app state + a 'running' start-row, not the terminal-only ledger), move spill off the Volume
onto the 512 GiB local disk, and replace the `.local()` `run_all` with sequential `--detach`
phases (or a `.remote()` dispatcher) — then the 107M run is durable, observable on every failure
mode, and uncorruptible by a double-launch.
