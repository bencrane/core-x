# USAspending FRESH caches — Fragment Compaction Worker (prime + subaward) — BUILD PLAN

**Audience:** executor agent. **Repo:** `core-x`. **Status:** ready to execute.
**Mandate:** keep the two append-only fresh caches from accumulating small Lance fragments under a daily
APPEND cadence. Add a **separate, threshold-gated, weekly Trigger.dev → Modal compaction worker** covering
**BOTH** tables. Isolated from the daily append (blast-radius containment). **Retrofits prime, which already
shipped its daily-append worker (#358) without compaction.**

> Companion to `USASPENDING_API_FRESH_PRIME_DAILY_APPEND_BUILD_PLAN.md` (shipped) and
> `USASPENDING_API_FRESH_SUBAWARD_DAILY_APPEND_BUILD_PLAN.md` (planned). Those add the APPEND path; this
> closes the fragmentation gap both leave open. Grounded in a live read-only fragment probe (2026-06-09).

---

## 0. TL;DR

Each daily `lance.write_dataset(mode="append")` writes **+1 small fragment**; `optimize_indices` (already
called) extends indices but **does not compact data fragments**. Live state today: **prime 7 fragments,
subaward 2** — the leading edge, caught at append run #1. Without compaction both reach **~370 fragments in a
year** → linear point-lookup amplification. Fix: a weekly `schedules.task` that dispatches a Modal
`run_compaction` worker per table; the worker no-ops unless `get_fragments() > 16`, else runs
`optimize_indices` → `compact_files` → `cleanup_old_versions`, asserts the row count is unchanged, and writes
a `run_mode='compaction'` ledger row. The chunked backfill design is **not** the problem (it produces one
fragment); do not touch it.

---

## 1. Evidence (live probe, read-only)

| | Prime `contract_prime_txn` | Subaward `contract_subaward` |
|---|--:|--:|
| rows | 1,495,923 | 199,901 |
| fragments (`len(get_fragments())`) | 7 | 2 |
| fragment row counts | `[250000×5, 156045, 89878]` | `[147252, 52649]` |
| versions | 12 | 12 |
| BTREE scalar indices | 9 | 9 |

- The 90-day chunked subaward backfill → **1 fragment** (147,252 rows, single combined `write_dataset`). The
  "many fragments needing re-consolidation" memory is the **`award_search` in-place-merge subsystem**, not
  these caches.
- Prime's 7th and subaward's 2nd fragments are each the **first daily APPEND** — confirms +1 fragment/run.
- `optimize_indices` ≠ compaction: it only folds new rows into existing indices (verified: the `optimize_indices`
  commit left fragment count unchanged). `compact_files` is the data-fragment operation, and **nothing calls it**.
- Prior art in-repo to mirror: `pipelines/gleif/ingest.py:443-460` (`compact_files()` before indexing);
  `pipelines/sam_gov/sam_attachment_extract_90day.py:1602-1606` (`compact_files(target_rows_per_fragment=…)` +
  `cleanup_old_versions()`); `pipelines/osha/osha_sniper.py:503` (`cleanup_old_versions(retain_versions=30)`);
  `docs/plans/medicare_ingestion_plan.md:173-178` ("compaction isolated from appends and from index builds").

---

## 2. Design decisions (locked)

1. **Separate worker, not inline.** Compaction is a full fragment rewrite; the daily append is light. Coupling
   them (compact-every-run, or conditional inside `run_daily`) violates blast-radius containment — a compaction
   failure (R2 multipart, OOM) would fail the append and stall the HWM. Compaction lives in its own Modal
   function and its own Trigger schedule. (Rejected: inline every-run, inline-conditional, write-strategy
   changes — a daily delta is far under `max_rows_per_file=250_000`, so a larger cap still yields 1 fragment/run.)
2. **Threshold-gated, idempotent.** The worker no-ops unless `len(get_fragments()) > MIN_FRAGMENTS` (default
   **16** — the good-topology band; GLEIF's cliff was between 6 and 34). Re-running below threshold returns
   `{"compacted": false}`. A `force` param overrides the gate (for the P1 test, since both tables are below 16 today).
3. **Order: `optimize_indices` → `compact_files` → `cleanup_old_versions`.** `optimize_indices` folds any
   un-indexed appended tail into the index FIRST; `compact_files` then rewrites data fragments and **remaps the
   BTREE indices in-place** (`defer_index_remap=False`, the default — **no `create_scalar_index` rebuild
   needed**); `cleanup_old_versions` bounds manifest/version growth. These are orthogonal — do not skip the
   leading `optimize_indices`, do not add a trailing index rebuild.
   - **P1 MUST verify this empirically** (see §4). The in-place-remap behavior is the design basis from API
     introspection; the forced-compaction test confirms indices stay valid before we trust it in the schedule.
4. **`compact_files(target_rows_per_fragment=250_000)`, `compaction_mode` default (`reencode`).** Do NOT use
   `try_binary_copy` (int32 cumulative-offset trap on wide VARCHAR — `sam_attachment_extract_90day.py:256`).
   At 0.2–1.5M rows a full rewrite is seconds–minutes.
5. **`cleanup_old_versions` — mirror the OSHA call exactly** (`osha_sniper.py:503`: `cleanup_old_versions(retain_versions=30)`),
   best-effort (try/except, non-fatal). Keeps ~a month of rollback. **NEVER pass `delete_unverified=True`**
   (corrupts a dataset with a concurrent writer).
6. **Row count is invariant — assert it.** Compaction must not change row count; if `count_rows()` differs
   pre/post, raise and fail the run (a hard correctness tripwire).
7. **`retries=0`.** A heavy rewrite should not auto-retry mid-flight; the next weekly schedule re-drives it.
   (The threshold gate makes a re-run cheap.)
8. **Isolated cadence.** Weekly cron offset clear of the daily append window (~16:00–17:00 UTC observed; the
   `entity_profile_gold` chain keys off the daily pull). Recommend **Sunday 09:00 UTC**. Cadence lives in Trigger
   (`schedules.task`) — `modal.Cron` is forbidden (`ARCHITECTURE.md:28,37,114`).
9. **Both tables, one design.** Prime and subaward are structurally identical (one `write_dataset(append)`/run,
   250k cap, `optimize_indices`-only steady state). Ship a `run_compaction` worker in each pipeline + one Trigger
   task that dispatches both. **Prime retrofit is in-scope, not optional.**

---

## 3. Work items

### WI1 — `run_compaction` Modal worker, in BOTH pipelines

Add to `pipelines/usaspending/usaspending_api_fresh.py` (prime) and to
`pipelines/usaspending/usaspending_api_subaward_fresh.py` (subaward — after its Modal port from the subaward
append plan lands; reuse that file's `app`, `image`, `_r2_so`/`_r2_storage_options`, `_post_callback`,
`_record_run`, `FRESH_URI`, `FEED`). Shape (prime; adapt helper names for subaward):

```python
@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 60,         # full small-fragment rewrite at this scale = seconds–minutes
    memory=32768, cpu=4.0,
    retries=0,               # heavy rewrite; re-driven by the next weekly schedule, not auto-retried
)
def run_compaction(min_fragments: int = 16, target_rows_per_fragment: int = 250_000,
                   force: bool = False, trigger_callback_url: str | None = None) -> dict:
    """Threshold-gated fragment compaction (no-op unless get_fragments() > min_fragments, or force).
    optimize_indices (fold un-indexed tail) → compact_files (rewrites + REMAPS BTREE in-place) →
    cleanup_old_versions. Row count is invariant (asserted). Isolated from the daily append."""
    import datetime as dt
    import lance

    started_at = dt.datetime.now(dt.timezone.utc)
    so = _r2_storage_options()
    ds = lance.dataset(FRESH_URI, storage_options=so)
    n0, rows0 = len(ds.get_fragments()), ds.count_rows()
    if n0 <= min_fragments and not force:
        print(f"[compaction] {n0} fragments ≤ {min_fragments}; no-op.", flush=True)
        _post_callback(trigger_callback_url, {"status": "success", "run_mode": "compaction", "feed": FEED,
                       "compacted": False, "fragments_before": n0, "fragments_after": n0, "table_rows_after": rows0})
        return {"status": "success", "compacted": False, "fragments_before": n0, "fragments_after": n0}

    status, error, n1, files_removed = "error", None, n0, None
    try:
        ds.optimize.optimize_indices()                                              # fold un-indexed tail FIRST
        m = ds.optimize.compact_files(target_rows_per_fragment=target_rows_per_fragment)  # remaps BTREE in-place
        files_removed = getattr(m, "files_removed", None)
        try:
            ds.cleanup_old_versions(retain_versions=30)                             # mirror osha_sniper.py:503
        except Exception as e:  # noqa: BLE001 — best-effort; never delete_unverified=True
            print(f"WARN cleanup_old_versions: {e}", flush=True)
        ds2 = lance.dataset(FRESH_URI, storage_options=so)
        n1, rows1 = len(ds2.get_fragments()), ds2.count_rows()
        if rows1 != rows0:
            raise RuntimeError(f"ROW COUNT CHANGED by compaction: {rows0} → {rows1} — abort.")
        status = "success"
        print(f"[compaction] fragments {n0} → {n1}; rows={rows1} (invariant); files_removed={files_removed}", flush=True)
        _post_callback(trigger_callback_url, {"status": status, "run_mode": "compaction", "feed": FEED,
                       "compacted": True, "fragments_before": n0, "fragments_after": n1, "table_rows_after": rows1})
    except BaseException as exc:  # noqa: BLE001
        error = f"{type(exc).__name__}: {exc}"; status = "error"; raise
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        # repurpose window_* as the run date; rows_written=0; write_mode='compact'; stash frag delta in error.
        _record_run(run_mode="compaction", window_start=started_at.date(), window_end=completed_at.date(),
                    rows_written=0, columns=0, table_rows_after=int(rows0), api_calls=0, write_mode="compact",
                    indices_built=[], status=status, error=(error or f"frags {n0}->{n1} removed={files_removed}"),
                    started_at=started_at, completed_at=completed_at)
    return {"status": status, "compacted": True, "fragments_before": n0, "fragments_after": n1}


@app.local_entrypoint()
def compact(min_fragments: int = 16, force: bool = False) -> None:
    """Manual compaction (P1): modal run …::compact --force true   (force, since both tables are < 16 today)."""
    import json
    print(json.dumps(run_compaction.remote(min_fragments=min_fragments, force=force), indent=2, default=str))
```

> **Subaward adaptation:** that pipeline's `_record_run` has the `(run_mode, ws, we, rows, cols, total, polls,
> write_mode, indices, status, error, started, completed)` signature — map accordingly (`ws=started.date()`,
> `we=completed.date()`, `rows=0`, `total=rows0`, `write_mode="compact"`, `indices=[]`). Use `_r2_so()` not
> `_r2_storage_options()`. The subaward Modal `app`/`image`/`_post_callback` come from its append plan — sequence
> this AFTER that lands, or fold both into one subaward PR.

### WI2 — Trigger task `src/trigger/usaspending_fresh_compaction.ts` (new)

One **`schedules.task`**, weekly, dispatching BOTH workers sequentially through the Universal Dispatcher (two
waitpoints). Clone the dispatch/waitpoint shape from `src/trigger/usaspending_api_fresh.ts`.

```ts
import { schedules, wait, logger } from "@trigger.dev/sdk";

interface CompactionCallback {
  status: "success" | "error"; run_mode: "compaction"; feed: string;
  compacted: boolean; fragments_before: number; fragments_after: number; table_rows_after?: number;
}

export const usaspendingFreshCompaction = schedules.task({
  id: "usaspending-fresh-compaction",
  cron: { pattern: "0 9 * * 0", timezone: "UTC" },   // Sun 09:00 UTC — clear of the 16–17:00 append window
  maxDuration: 5400,
  run: async (_payload, { ctx }) => {
    const targets = [
      { app_name: "usaspending-api-fresh",          feed: "prime" },
      { app_name: "usaspending-api-subaward-fresh", feed: "subaward" },
    ];
    const out: CompactionCallback[] = [];
    for (const t of targets) {
      const token = await wait.createToken({ timeout: "1h", tags: ["usaspending-fresh-compaction", t.feed] });
      const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
        method: "POST",
        headers: { "Content-Type": "application/json",
                   "Modal-Key": requireEnv("MODAL_KEY"), "Modal-Secret": requireEnv("MODAL_SECRET") },
        body: JSON.stringify({ app_name: t.app_name, function_name: "run_compaction",
                               kwargs: {}, trigger_callback_url: token.url }),
      });
      if (!res.ok) throw new Error(`dispatcher ${res.status} (${t.feed}): ${(await res.text()).slice(0, 300)}`);
      const r = await wait.forToken<CompactionCallback>(token.id);
      if (!r.ok) throw new Error(`${t.feed} compaction timed out (token ${token.id})`);
      if (r.output.status !== "success") throw new Error(`${t.feed} compaction failed: ${JSON.stringify(r.output)}`);
      logger.info(`${t.feed} compaction`, { ...r.output });
      out.push(r.output);
    }
    return { compactions: out };
  },
});

function requireEnv(name: string): string {
  const v = process.env[name]; if (!v) throw new Error(`Missing required env var: ${name}`); return v;
}
```

Sequential (not parallel) so the two heavy rewrites never run concurrently. No new secrets.

### WI3 — Ledger (`ops.usaspending_api_fresh_runs`, `…_subaward_fresh_runs`)

Zero-migration path: `_record_run(run_mode="compaction", write_mode="compact", …)` already fits the existing
columns (frag delta stashed in `error_message`). **Optional clean ALTER** (idempotent) if you want first-class
fields:
```sql
ALTER TABLE ops.usaspending_api_fresh_runs          ADD COLUMN IF NOT EXISTS fragments_before int,
                                                    ADD COLUMN IF NOT EXISTS fragments_after  int;
ALTER TABLE ops.usaspending_api_subaward_fresh_runs ADD COLUMN IF NOT EXISTS fragments_before int,
                                                    ADD COLUMN IF NOT EXISTS fragments_after  int;
```

---

## 4. Execution & verification

### Phase 1 — Modal direct, FORCED (both tables are < 16 fragments today, so the gate no-ops without `--force`)

```bash
doppler run -p core-x -c prd -- modal deploy pipelines/usaspending/usaspending_api_fresh.py
doppler run -p core-x -c prd -- modal run pipelines/usaspending/usaspending_api_fresh.py::compact --force true
doppler run -p core-x -c prd -- modal run pipelines/usaspending/usaspending_api_fresh.py::verify_table
# repeat for the subaward pipeline once its Modal port + run_compaction land
```

**Pass criteria (the index-remap tripwire is the important one):**
- `fragments_before → fragments_after` **drops** (prime 7 → ~6; the two sub-250k tail fragments coalesce).
- `verify_table` shows **row count unchanged**, the **9 BTREE indices still present**, and `max_last_modified`
  unchanged — i.e. compaction preserved data + indices.
- **Index validity check (must pass):** a pushdown query returns correct rows post-compaction, proving the
  in-place remap held — e.g. count a known `recipient_uei` (or `last_modified_date >= '2026-06-01'`) before and
  after; counts must match. If indices are stale, STOP — the design's no-rebuild assumption is wrong and the
  worker must add a `create_scalar_index(..., replace=True)` pass.
- `run_mode='compaction'` ledger row written.

### Phase 2 — Trigger schedule (only if Phase 1 + index check are clean)

`npm run trigger:deploy` (manual, fleet-wide — operator go required). Then trigger `usaspending-fresh-compaction`
once from the dashboard to prove the round-trip (it will likely no-op at < 16 fragments — pass `force` via a
temporary manual run, or lower `MIN_FRAGMENTS` for the test). Confirm the weekly cron is registered.

---

## 5. Guardrails / failure modes

| Risk | Control |
|---|---|
| Heavy rewrite corrupts/stalls the daily append | Separate worker + separate weekly schedule, offset from the append window. Never share an invocation. |
| Compaction silently drops/dupes rows | Hard assert `count_rows()` unchanged; raise + fail on mismatch. |
| Stale indices after compaction | `defer_index_remap=False` remaps in-place; **P1 index-validity check gates trust**; trailing `create_scalar_index(replace=True)` only if P1 fails. |
| `cleanup_old_versions` destroys rollback | `retain_versions=30` (≈1 month); best-effort try/except; **never `delete_unverified=True`**. |
| Concurrent writer during cleanup | Schedule isolation guarantees the compaction worker is the only writer in its window. |
| int32 offset trap on wide VARCHAR | `compaction_mode` default (`reencode`); never `try_binary_copy`. |
| Needless churn | Threshold gate (`> 16`) → no-op most weeks; idempotent. |

---

## 6. Git lifecycle + acceptance

```bash
git checkout -b feat/usaspending-fresh-compaction origin/main
# WI1 prime now; WI1 subaward + WI2 + WI3 after/with the subaward append PR
python3 -m py_compile pipelines/usaspending/usaspending_api_fresh.py
git add -A && git commit -m "feat(usaspending): fresh-cache fragment compaction worker (prime+subaward, weekly, threshold-gated)"
git push -u origin HEAD && gh pr create --fill --base main
# P1 (forced) + index-validity check green, then:
gh pr merge <num> --squash --delete-branch
```
Then fast-forward the operator-facing `main` checkout; deploy the Modal app(s); run `trigger:deploy` only with operator go.

**Acceptance:**
- [ ] `run_compaction` in both pipelines; threshold-gated; `retries=0`; row-count assertion present.
- [ ] Order `optimize_indices` → `compact_files(target_rows_per_fragment=250_000)` → `cleanup_old_versions(retain_versions=30)`.
- [ ] P1 forced run drops fragment count, preserves rows + indices, and the **pushdown index-validity check passes**.
- [ ] `run_mode='compaction'` ledger rows for both tables.
- [ ] Weekly `schedules.task` dispatches both workers sequentially, offset from the append window.
- [ ] Prime retrofit included (not subaward-only). Daily-append workers unchanged.
```
