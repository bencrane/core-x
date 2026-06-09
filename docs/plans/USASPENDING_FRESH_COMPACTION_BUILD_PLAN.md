# USAspending FRESH caches — Fragment Compaction Worker — BUILD PLAN

**Audience:** executor agent. **Repo:** `core-x`. **Status:** ready to execute (Phase A — prime).
**Mandate:** keep the append-only fresh caches from accumulating small Lance fragments under a daily APPEND
cadence. Add a **separate, threshold-gated, weekly Trigger.dev → Modal compaction worker.** Isolated from the
daily append (blast-radius containment). **Ships prime first (already Modal); subaward follows its Modal port.**

> **Revised per adversarial review (2026-06-09).** Both make-or-break claims were **empirically verified on
> throwaway `/tmp` Lance datasets (pylance 7.0.0)**: (1) `compact_files` **remaps BTREE scalar indices in-place**
> — post-compaction `explain_plan` shows `ScalarIndexQuery@…(BTree)`, filtered counts match ground truth, **no
> rebuild needed**; (2) `cleanup_old_versions(retain_versions=30)` is a **valid, non-throwing** call. The review
> also caught a blocker (wrong compaction target param), a false concurrency premise (survivable), and a
> sequencing gap (subaward blocked on its port) — all folded in below.

> **⚠ Execution finding (2026-06-09 — SUPERSEDES adversarial F1).** Running compaction against the LIVE R2
> prime table surfaced a constraint the `/tmp` tests could not: the **Lance-default (~1M-row) compaction
> target** writes a file big enough that `object_store` escalates its multipart part size mid-upload, which
> **R2 rejects — `400 InvalidPart`, "all non-trailing parts must have the same length"** (`ARCHITECTURE.md:140`).
> So **`target_rows_per_fragment=250_000` is MANDATORY** (the adversary's "use the Lance default" holds only on
> local FS). 250k is the proven R2-safe append fragment size AND still **bounds the count to ~total_rows/250k**
> (~6 for prime) because it re-merges the small daily fragments each pass. **Verified on live R2 prime:**
> `compact_files(250k)` → 7→6, rows invariant, BTREE indices remapped in-place (filtered count 126,392 ==
> full-scan). The broad "commit-conflict → no-op" catch was also **removed** — it masked this IO error; genuine
> errors now propagate (`status=error`, re-driven by the next weekly schedule).

> Companion to `USASPENDING_API_FRESH_PRIME_DAILY_APPEND_BUILD_PLAN.md` (shipped #358) and
> `USASPENDING_API_FRESH_SUBAWARD_DAILY_APPEND_BUILD_PLAN.md` (planned). Those add the APPEND path; this closes
> the fragmentation gap both leave open.

---

## 0. TL;DR

Each daily `lance.write_dataset(mode="append")` writes **+1 small fragment**; `optimize_indices` (already
called by the append) extends indices but **does not compact data fragments**. Live state: **prime 7, subaward
2** — the leading edge, caught at append run #1. Without compaction both reach **~370 fragments/year** → linear
point-lookup amplification (GLEIF: 143ms@34 vs 69ms@6 frags). Fix: a weekly `schedules.task` dispatching a Modal
`run_compaction` worker that no-ops unless `get_fragments() > 12`, else runs `optimize_indices` → `compact_files`
(`target_rows_per_fragment=250_000` — R2-mandatory, see banner) → `cleanup_old_versions`, asserts row count
unchanged, and ledgers `run_mode='compaction'`.
**Phase A ships prime now; Phase B adds subaward after its Modal port lands** (a one-line `targets` extension).
The chunked backfill is NOT the problem (one combined `write_dataset` → one fragment); do not touch it. **Dedup
is out of scope** (real exact-dup is ~2.6–4.6%, not a bottleneck — see §2.10).

---

## 1. Evidence (live read-only probe + adversarial /tmp tests)

| | Prime `contract_prime_txn` | Subaward `contract_subaward` |
|---|--:|--:|
| rows | 1,495,923 | 199,901 |
| fragments | 7 | 2 |
| fragment row counts | `[250000×5, 156045, 89878]` | `[147252, 52649]` |
| compact @ target=250k (tested) | 7 → **6** (only sub-250k tails merge) | 2 → 2 |
| compact @ Lance default (tested) | 7 → **2** | 2 → 1 |
| BTREE indices | 9 | 9 |

- 90-day chunked subaward backfill → **1 fragment** (single combined `write_dataset`). The "many fragments
  needing re-consolidation" memory is the **`award_search` in-place-merge subsystem**, not these caches.
- `optimize_indices` ≠ compaction (verified: leaves fragment count unchanged). Nothing calls `compact_files`.
- Prior art to mirror: `pipelines/gleif/ingest.py:443-460`, `pipelines/sam_gov/sam_attachment_extract_90day.py:1602-1606`,
  `pipelines/osha/osha_sniper.py:503`, `docs/plans/medicare_ingestion_plan.md:173-178`.

---

## 2. Design decisions (locked; ✎ = changed by adversarial review)

1. **Separate worker, not inline.** Compaction is a full fragment rewrite; the append is light. Coupling them
   violates blast-radius containment. Own Modal function, own weekly schedule.
2. **Threshold gate `> 12`, idempotent.** No-op unless `len(get_fragments()) > MIN_FRAGMENTS` (default **12**).
   With the R2-mandatory 250k target (decision 4) the post-compaction floor is ~total_rows/250k (**~6 for prime**),
   so at +7 appends/week this fires ~weekly and **bounds the table to ≤ ~13 fragments** — well below the
   ~34-fragment latency cliff. A `force` param overrides the gate (P1 test). Re-running below threshold returns
   `{"compacted": false}`.
3. **Order: `optimize_indices` → `compact_files` → `cleanup_old_versions`.** `compact_files` **remaps the BTREE
   indices in-place** (`defer_index_remap=False`, default — **EMPIRICALLY VERIFIED, no rebuild**). The leading
   `optimize_indices` is a **cheap idempotent guard** — the daily append already folds the tail, so it is
   normally a near-no-op; keep it as defense against a partial last append, but it is not load-bearing.
4. **`compact_files(target_rows_per_fragment=250_000)` — MANDATORY on R2 (see ⚠ banner).** The Lance default
   (~1M) writes a file large enough that object_store escalates its multipart part size mid-upload → R2 `400
   InvalidPart`. 250k is the proven R2-safe append fragment size; it leaves already-full 250k fragments alone
   (optimal) and re-merges the small daily fragments, bounding the count to ~total_rows/250k. **Verified live:
   prime 7→6, rows invariant, indices remapped.** `compaction_mode` default (`reencode`); never `try_binary_copy`
   (int32 offset trap). The append path keeps `max_rows_per_file=250_000` unchanged.
5. **`cleanup_old_versions(retain_versions=30)` — verified valid; mirror `osha_sniper.py:503`.** Best-effort
   (try/except, non-fatal). `retain_versions` is a **count cap** (the last 30 versions ≈ 30 daily-append +
   compaction commits), NOT an age cap. **Never pass `delete_unverified=True`.**
6. **Row count invariant — assert it.** If `count_rows()` differs pre/post, raise and fail (hard tripwire).
7. **`retries=0`.** Compaction is **commit-atomic** (verified: SIGKILL mid-`compact_files` left the dataset
   readable, row-preserved — Lance only publishes a version on commit). A failed run is re-driven by the next
   weekly schedule; the threshold gate makes the re-run cheap. No auto-retry mid-rewrite.
8. **Concurrency: errors propagate + re-drive, NO masking.** The live ledger shows a prime append committed at
   **20:48 UTC** (manual) — a fixed "16–17:00 window" does not exist, so a cron offset cannot *guarantee*
   non-overlap. Empirically Lance's optimistic-concurrency resolver is safe (a concurrent append merged into the
   compaction commit, no lost write); worst case is a **commit conflict that fails the run** → `status=error`,
   re-driven by the next weekly schedule. Do NOT add a broad substring catch for "conflict/commit/version" — it
   **masks real IO errors** (during execution such a catch swallowed the R2 `InvalidPart`, hiding the blocker).
   Correctness rests on commit-atomicity + re-drive, not on the schedule offset.
9. ✎ **Phase the rollout. Prime ships now (already Modal); subaward is blocked on its Modal port.** The two are
   structurally identical; the Trigger task loops a `targets` array, so adding subaward is a one-line extension
   once `usaspending_api_subaward_fresh.py` becomes a Modal app (its append plan).
10. ✎ **Dedup is OUT OF SCOPE.** The tables carry intentional re-pull duplicates, but measured exact-dup is only
    **prime ~4.6% / subaward ~2.6%** (the headline "37%" was a non-grain `subaward_number` artifact — the true
    grain `(prime_award_unique_key, subaward_number, sam_last_modified)` is ~4.5%). Fragment count, not the ~3%
    dup tax, is the point-lookup bottleneck. The pipeline docstrings promise a downstream reconcile mirror that
    **does not exist** — if single-row point-lookups are ever needed, build that mirror as a SEPARATE plan; it is
    not this worker's job.

---

## 3. Work item 1 — `run_compaction` Modal worker

**Phase A:** add to `pipelines/usaspending/usaspending_api_fresh.py` (prime — already Modal; reuse `_r2_storage_options`,
`_post_callback`, `_record_run`, `FRESH_URI`, `FEED`).

```python
@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 60, memory=32768, cpu=4.0,
    retries=0,        # commit-atomic; re-driven by the next weekly schedule, never auto-retried mid-rewrite
)
def run_compaction(min_fragments: int = 12, target_rows_per_fragment: int = 250_000,
                   force: bool = False, trigger_callback_url: str | None = None) -> dict:
    """Threshold-gated fragment compaction. No-op unless get_fragments() > min_fragments (or force).
    optimize_indices (cheap guard) → compact_files → cleanup_old_versions. Row count invariant (asserted).
    target_rows_per_fragment=250_000 is MANDATORY on R2: the Lance default (~1M) writes a file large enough
    that object_store escalates its multipart part size mid-upload, which R2 rejects (400 InvalidPart). 250k
    keeps each rewritten fragment at the R2-safe append size AND bounds the count to ~total_rows/250k.
    compact_files remaps the BTREE indices in-place (verified on live R2)."""
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

    status, error, n1, rows1, files_removed = "error", None, n0, rows0, None
    try:
        ds.optimize.optimize_indices()                                   # cheap idempotent guard (append already folds tail)
        m = ds.optimize.compact_files(target_rows_per_fragment=target_rows_per_fragment)  # 250k = R2-safe; remaps BTREE in-place
        files_removed = getattr(m, "files_removed", None)
        try:
            ds.cleanup_old_versions(retain_versions=30)                  # count cap; best-effort; never delete_unverified=True
        except Exception as e:  # noqa: BLE001
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
        _record_run(run_mode="compaction", window_start=started_at.date(), window_end=completed_at.date(),
                    rows_written=0, columns=0, table_rows_after=int(rows1), api_calls=0, write_mode="compact",
                    indices_built=[], status=status, error=error,   # error stays None on success
                    started_at=started_at, completed_at=completed_at)
    return {"status": status, "compacted": True, "fragments_before": n0, "fragments_after": n1}


@app.local_entrypoint()
def compact(min_fragments: int = 12, force: bool = False) -> None:
    """Manual compaction (P1):
        modal run …::compact --force true            # force, ignore the gate
        modal run …::compact --min-fragments 3       # exercise the GATED fire path"""
    import json
    print(json.dumps(run_compaction.remote(min_fragments=min_fragments, force=force), indent=2, default=str))
```

**Phase B (subaward — after its Modal port lands):** same worker in `usaspending_api_subaward_fresh.py`, using
that file's `_r2_so()` (not `_r2_storage_options`). Its `_record_run` signature differs — map:
`run_mode="compaction", ws=started_at.date(), we=completed_at.date(), rows=0, cols=0, total=int(rows1),
polls=0, write_mode="compact", indices=[], status=…, error=…(None on success), started=started_at,
completed=completed_at`. **Pass `polls=0` explicitly** (required positional, no compaction analog).

> Ledger-hygiene fixes baked in: `error` stays `None` on success (no frag-delta in `error_message` — the delta
> rides the `print` + Trigger callback); `table_rows_after=int(rows1)` (post, not pre); `rows1` hoisted so the
> `finally` records the correct value even on the assert path.

---

## 4. Work item 2 — Trigger task `src/trigger/usaspending_fresh_compaction.ts` (new)

Weekly `schedules.task`, dispatching each target sequentially (so the heavy rewrites never overlap). **Phase A =
prime only.**

```ts
import { schedules, wait, logger } from "@trigger.dev/sdk";

interface CompactionCallback {
  status: "success" | "error"; run_mode: "compaction"; feed: string;
  compacted: boolean; reason?: string; fragments_before: number; fragments_after: number; table_rows_after?: number;
}

export const usaspendingFreshCompaction = schedules.task({
  id: "usaspending-fresh-compaction",
  cron: { pattern: "0 9 * * 0", timezone: "UTC" },   // Sun 09:00 UTC — reduces (does not guarantee) append overlap
  maxDuration: 5400,
  run: async (_payload, { ctx }) => {
    const targets = [
      { app_name: "usaspending-api-fresh", feed: "prime" },
      // Phase B (after the subaward Modal port lands), add:
      // { app_name: "usaspending-api-subaward-fresh", feed: "subaward" },
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

No new secrets. Phase B = uncomment one `targets` line.

## 5. Ledger — verify only (no migration)

`_record_run(run_mode="compaction", write_mode="compact", error=None_on_success, …)` fits the existing columns;
the fragment delta rides the `print` + callback, so **no `error_message` pollution** and **no ALTER needed**.
(Optional first-class fields remain available: idempotent `ALTER TABLE … ADD COLUMN IF NOT EXISTS fragments_before/after int`,
but only if you also thread two params through `_record_run` — not required.)

---

## 6. Execution & verification (Phase A — prime)

```bash
doppler run -p core-x -c prd -- modal deploy pipelines/usaspending/usaspending_api_fresh.py
# gate NO-OP (≤ 12 frags → no-op):
doppler run -p core-x -c prd -- modal run pipelines/usaspending/usaspending_api_fresh.py::compact
# gated FIRE (lower the gate below the current frag count to exercise compact+merge on R2):
doppler run -p core-x -c prd -- modal run pipelines/usaspending/usaspending_api_fresh.py::compact --min-fragments 3
doppler run -p core-x -c prd -- modal run pipelines/usaspending/usaspending_api_fresh.py::verify_table
```

**Pass criteria (the index-validity check is the gate that matters) — ALL VERIFIED on live R2 prime:**
- `compact_files(target_rows_per_fragment=250_000)` succeeds with **no `400 InvalidPart`** (the Lance default
  fails on R2 — see ⚠ banner). A real merge ran on the deployed Modal worker: **8 → 7** fragments, `status=success`.
- **Row count unchanged** across compaction (asserted in-worker; cross-check via `verify_table`).
- **Index-validity (passed):** a pushdown range query post-compaction returned the SAME count as a full scan
  (149,276 == 149,276), `uses_scalar_index=true`, 9 BTREE indices intact — the in-place remap held on live R2.
  If a future run shows a mismatch, STOP — add a trailing `create_scalar_index(..., replace=True)`; otherwise do not.
- `run_mode='compaction'` ledger rows present, `error_message` NULL on success, `table_rows_after` = post count.

### Phase 2 — Trigger schedule (only if Phase A is clean)

`npm run trigger:deploy` (manual, fleet-wide — operator go required). Trigger `usaspending-fresh-compaction`
once to prove the round-trip. Confirm the weekly cron registers.

---

## 7. Guardrails / failure modes

| Risk | Control |
|---|---|
| Heavy rewrite corrupts/stalls the append | Separate worker + weekly off-peak schedule. A coincident-append commit conflict fails the run loudly → re-driven next week (concurrency is survivable, not corruption — verified; no masking). |
| Crash mid-compaction | **Commit-atomic** — verified: SIGKILL left the dataset readable, rows preserved. `retries=0` safe. |
| Compaction drops/dupes rows | Hard assert `count_rows()` unchanged; raise on mismatch. |
| Stale indices after compaction | `compact_files` remaps in-place (verified); P1 pushdown check gates trust; `create_scalar_index(replace=True)` only if P1 fails. |
| `cleanup_old_versions` destroys rollback | `retain_versions=30` (count cap); best-effort; never `delete_unverified=True`. |
| Direct-R2 compaction → `400 InvalidPart` | `target_rows_per_fragment=250_000` (R2-safe append size); the Lance default ~1M trips R2's uniform-multipart-part rule (verified live, see ⚠ banner). |
| Needless churn | `> 12` gate → no-op most days; idempotent. |

---

## 8. Git lifecycle + acceptance

```bash
git checkout -b feat/usaspending-fresh-compaction-prime origin/main
# Phase A: WI1 prime run_compaction + local entrypoint; WI2 Trigger task (prime-only targets)
python3 -m py_compile pipelines/usaspending/usaspending_api_fresh.py
git add -A && git commit -m "feat(usaspending): prime fresh-cache fragment compaction worker (weekly, threshold-gated)"
git push -u origin HEAD && gh pr create --fill --base main
# P1 (force + gated) + index-validity check green, then:
gh pr merge <num> --squash --delete-branch
```
Then fast-forward the operator-facing `main` checkout; deploy the Modal app; `trigger:deploy` only with operator go.

**Acceptance (Phase A):**
- [x] `run_compaction` in prime; gate `> 12`; `retries=0`; row-count assertion; errors propagate (no masking).
- [x] `compact_files(target_rows_per_fragment=250_000)` (**R2-mandatory**, not the Lance default); order optimize_indices→compact_files→cleanup_old_versions.
- [x] Gate no-op **and** gated-fire run both pass; a real R2 merge ran on Modal (8→7); rows + indices preserved; **pushdown index-validity check passed** (149,276 == full-scan). *(prime verified live 2026-06-09)*
- [x] `run_mode='compaction'` ledger row with `error_message` NULL on success; `table_rows_after` = post count. *(verified)*
- [ ] Weekly `schedules.task` (prime target); daily-append worker unchanged.
- [ ] **Phase B (subaward)** explicitly deferred to after the subaward Modal port; documented as a one-line `targets` extension.
```
