# Fix Plan — cal.com booking → Parallel deep-research

**Two bugs:** (1) Trigger waitpoint-key collision; (2) `run_id` join mismatch (booking ↔ report).
**Status:** Proposed, not yet executed. Handoff-ready for a fresh agent.
**Produced by:** an adversarial planning pass, verified against live code + production state (Trigger traces, Lance datasets, Postgres) on 2026-06-10.

Repo: `/Users/benjamincrane/core-x` @ `main`. All claims verified against code at that time.

---

## A. Bug verification

### Bug 1 — waitpoint collision — **CONFIRMED (with one correction to the stated mechanism)**

**Code path:**
- `apps/edge_api/src/cal/research.py:60-70` builds the booking payload with `objective/grain/processor/outputType/domain/company_name` — **no `idempotencyKey` field**. It calls `trigger_task(RESEARCH_TASK, payload, idempotency_key=f"research:{ical_uid}")`.
- `apps/edge_api/src/services/trigger_dev_client.py:128-150`: that `idempotency_key` is placed in **`options.idempotencyKey`** (line 141) — the Trigger **run-enqueue** dedup layer, a *different* layer from the payload.
- `src/trigger/parallel_deep_research.ts:101-102`: inside the task, `idempotencyKey = payload?.idempotencyKey ?? ${specId||"research"}:${audienceId||"topic"}:${runKind}`. For a booking (no `specId`, `grain="topic"` → `audienceId=""`, `runKind="full"`), this resolves to the **shared constant `research:topic:full`**.
- `src/trigger/parallel_deep_research.ts:121-125`: `wait.createToken({ timeout:"1h", idempotencyKey, ... })` mints the waitpoint with that shared key. **No `idempotencyKeyTTL` is passed → Trigger default of 30 days.**

**Trigger semantics (Trigger docs, `management/waitpoints/create`):** the waitpoint idempotency key is **project-scoped, not run-scoped**. Passing the same key twice before expiry returns the original token; if it's already completed, `wait.forToken()` continues immediately (`isCached: true`).

**Production proof (Trigger run traces, env=prod, task `parallel-deep-research`):**
| Run | When | `wait.forToken()` dur | Output `run_id` | Note |
|---|---|---|---|---|
| `run_cmq7g59xg445r0uojli08jp5q` (origin) | 06-10 02:25 | **3.5 min** (real research) | itself | first use of `research:topic:full` |
| `run_cmq8c1qk76o4f0omtax09d1hb` (**real booking**, ERS Corp) | 06-10 17:18 | **220 ms** | **`run_cmq7g59…`** | returned the origin's stale output |
| `run_cmq8cr0ve70870on7r8acpnc0` (manual base-test) | 06-10 17:38 | **217 ms** | **`run_cmq7g59…`** | same stale output |

Three distinct runs, one of them the actual ERS booking, all resolved on the origin's 15-hour-old completed token and returned its report. `corex.bookings` row `3jE43dyt9…` is stamped `research_run_id=run_cmq8c1qk…` but that run's *output* was the origin's brief.

**Correction to the stated mechanism:** the collision does **NOT** skip Modal. In `parallel_deep_research.ts`, `wait.createToken` (line 121) runs first, then the Modal `fetch` dispatch (line 128) fires **unconditionally** — `isCached` is never checked. Proof: `ops.parallel_runs` has **distinct `group_id`** rows for all three runs and `parallel_research` has 4 landed `trun_…` rows, i.e. Modal ran and wrote a ledger row each time. So the real failure is: **a redundant Modal run IS dispatched (wasted Parallel spend + an orphan Lance row), but the Trigger task resolves instantly on the first run's cached token and returns the first booking's report; the fresh run's callback hits an already-completed token and is silently discarded.** User-visible effect matches (2nd+ bookings "complete" with booking #1's report), but the cost/orphan-row dimension is worse than first stated, and the fix rationale must account for the unconditional dispatch.

### Bug 2 — run_id join mismatch — **CONFIRMED exactly as stated**

- `apps/edge_api/src/cal/research.py:74` returns `run.get("id")` = the **Trigger run id** (`run_…`); `webhooks_cal.py:136` → `queries.set_research_refs` → `queries.py:140-148` stamps it into `corex.bookings.research_run_id` (column type `text`, nullable).
- `pipelines/parallel/deep_research.py:571-572`: topic path does `run = create_task_run(...)`, `rid = run.get("run_id")` = the **Parallel run id** (`trun_…`, from `core/parallel_client.py:159-182`).
- `deep_research.py:581` writes the topic row with `"run_id": rid` (Parallel id). `_write_research` keys topic `merge_insert` on column `run_id` → the `trun_…` value. `_write_raw` keys `parallel_research_raw` on `run_id` = same `trun_…`.

**Production proof (Lance on R2, read via pylance):**
- `parallel_research` (4 rows): every `run_id` is `trun_…`, incl. `trun_16b51781c2464cbca988a66e16af3939`. `company_id=None` (topic).
- `parallel_research_raw` (2 rows): both keyed `trun_…`. **No Trigger-id column exists.**
- `corex.bookings.research_run_id = run_cmq8c1qk…` (a `run_…`).

`run_…` (bookings) ∩ `trun_…` (parallel_research) = ∅. **The join is impossible.**

Corroborating: `_record_run` (`deep_research.py:526`) sets `group_id = run_id` (the worker's **Trigger-id** param). `ops.parallel_runs.group_id` for the ERS booking run = `run_cmq8c1qk…` — this DOES match the booking, but it points at the *ledger row*, not the report content (Lance, keyed by `trun_…`).

---

## B. Blast-radius findings

1. **No live consumer joins `bookings.research_run_id` to `parallel_research`.** `apps/edge_api/src/bookings/queries.py` `_SELECT_DETAIL_COLS` does not even select `research_run_id`; the dossier read is described in docstrings but **not yet implemented**. **Bug 2 has no currently-broken reader — the fix establishes a correct join for the dossier reader that lands next. No consumer code changes in lockstep.**

2. **gtm-mcp trigger path (`apps/gtm_mcp/src/tools/parallel.py`):** `deep_research()` (lines 265-349) ALWAYS sets `payload["idempotencyKey"] = f"{sid or 'research'}:{aid or 'topic'}:{run_kind}"` (line 334). A gtm-mcp **topic** run with **no `spec_id`** → **`research:topic:full` — the identical collision string as the booking path.** gtm-mcp topic-no-spec runs already collide with each other AND with bookings. Topic runs *with* a `spec_id` are keyed `{spec}:topic:full` (unique). **Implication:** the robust Bug-1 fix is **in the TS task** (fixes booking + gtm-mcp + future callers at once).

3. **gtm-mcp read path:** reads `parallel_research` generically (DuckDB attach by dataset name; no hardcoded `run_id` join). Changing the topic `run_id` column is safe for gtm-mcp; keeping the Parallel id in a side column preserves traceability.

4. **per_entity grain is correctly keyed and must NOT be touched.** per_entity rows key on `company_id` (`deep_research.py:342, 614`). One Trigger run fans out to N companies → N Parallel runs; there is no 1:1 Trigger-id↔row, so keying per_entity by Trigger id would **collapse N companies into one row**. The Bug-2 fix is **topic-only**. This asymmetry is the central safety constraint.

5. **Existing Lance rows** (4 in `parallel_research`, 2 in `parallel_research_raw`, all `trun_…`) are test/manual rows, not load-bearing. All-string explicit pyarrow schema + `merge_insert` → **adding a column triggers Lance schema evolution on the next write** (verify, don't assume — risk R3). **Do not backfill;** validate end-to-end with a fresh booking. Optional: re-key the one ERS row for a live join demo (step 7).

6. **`enrich`/`search` tasks:** `parallel_enrich.ts:97` keys `${specId}:${audienceId}:${runKind}` (unique by construction). `parallel_search.ts:70` keys `${specId||"search"}:${ctx.run.id}` — **already run-unique; this is the reference pattern for the fix.** Neither is on the booking path.

7. **`idempotencyKeyTTL`:** the run-enqueue key (edge_api `options.idempotencyKey`) and the waitpoint key (TS task) are independent layers with independent 30-day default TTLs. The enqueue key is already unique per booking; only the waitpoint layer collides. The fix changes **only** the waitpoint key.

---

## C. Execution plan (handoff-ready)

**Success criteria:**
- (G1) Two distinct bookings each produce their **own** Parallel research run and report row — no cached short-circuit.
- (G2) A `corex.bookings` row is **joinable** to its own topic report row in `parallel_research`.
- (G3) gtm-mcp `deep_research` (topic + per_entity), `enrich`, `search` still function; per_entity rows still key on `company_id`; existing rows still readable.

**Read first:** `src/trigger/parallel_deep_research.ts`, `pipelines/parallel/deep_research.py`, `apps/edge_api/src/cal/research.py`, `apps/gtm_mcp/src/tools/parallel.py`, `core/parallel_client.py`. The fix touches **3 files**.

### Change 1 — TS task: make the waitpoint key run-unique (fixes Bug 1 for ALL callers)

`src/trigger/parallel_deep_research.ts`, line 101-102. Append `ctx.run.id` to the fallback (mirrors `parallel_search.ts:70`):
```ts
const idempotencyKey =
  payload?.idempotencyKey ?? `${specId || "research"}:${audienceId || "topic"}:${runKind}:${ctx.run.id}`;
```
**Rationale:** the waitpoint must be unique per run. Fixes bookings (no `payload.idempotencyKey`) + gtm-mcp topic-no-spec + future callers in one line, without perturbing the run-enqueue dedup (different layer) or callers that pass `payload.idempotencyKey`. **Do NOT strip `payload?.idempotencyKey`** — gtm-mcp relies on it for intentional same-spec waitpoint dedup; the `??` only changes the fallback.

### Change 2 — edge_api: unique waitpoint key per booking (defense in depth + deploy-order independence)

`apps/edge_api/src/cal/research.py`, payload dict (lines 60-68). Add:
```python
    "idempotencyKey": f"research:{ical_uid}",   # ← unique waitpoint key per booking
```
**Rationale:** `ical_uid` is the stable per-booking anchor. Makes the waitpoint unique per booking **even on the currently-deployed worker**, so edge_api is correct independent of the TS deploy order. Same string as the enqueue key — fine, the two are different layers and both should be per-booking. Redundant-but-harmless once Change 1 ships.

### Change 3 — worker: make topic rows joinable to the booking (fixes Bug 2)

`pipelines/parallel/deep_research.py`. **Design:** key the **topic** `parallel_research` row by the **Trigger run id** (the worker's `run_id` param, which the booking stamps); carry the **Parallel id (`trun_…`) in a new side column `parallel_run_id`**. Mirror in `parallel_research_raw`. **per_entity keying UNCHANGED (`company_id`).**

**3a.** Add the column to both schemas:
```python
_RESEARCH_COLUMNS = (
    "run_id", "company_id", "normalized_domain", "objective", "report_md",
    "basis", "processor", "spec_id", "grain", "created_at", "parallel_run_id",  # ← ADD
)
_RAW_COLUMNS = ("run_id", "raw_result", "processor", "grain", "created_at", "parallel_run_id")  # ← ADD
```

**3b.** Topic write — `run_id` = Trigger id (worker `run_id` param), `parallel_run_id` = `rid`:
```python
    st, report_md, basis, raw_res = _await_report(rid)
    if raw_res is not None:
        raw_rows.append({"run_id": run_id, "raw_result": raw_res, "processor": proc,   # run_id = Trigger id
                         "grain": grain, "created_at": created, "parallel_run_id": rid})
    if st == "completed" and report_md:
        rows.append({"run_id": run_id, "company_id": None, "normalized_domain": None,   # run_id = Trigger id
                     "objective": objective, "report_md": report_md,
                     "basis": _json_dump(basis), "processor": proc, "spec_id": spec,
                     "grain": grain, "created_at": created, "parallel_run_id": rid})    # Parallel id side col
    else:
        counts["failed"] = 1
        failed_ids.append(rid)
```
(`run_id` here is the worker param = the Trigger id the booking stamped. `rid` is the Parallel id.)

**3c.** per_entity write — **keying unchanged** (`run_id=rid`, merge on `company_id`); just populate `parallel_run_id=rid` for symmetry:
```python
    if raw_res is not None:
        raw_rows.append({"run_id": rid, "raw_result": raw_res, "processor": proc,
                         "grain": grain, "created_at": created, "parallel_run_id": rid})
    if st == "completed" and report_md:
        rows.append({"run_id": rid, "company_id": c["company_id"],
                     "normalized_domain": c.get("normalized_domain"),
                     "objective": objective, "report_md": report_md,
                     "basis": _json_dump(basis), "processor": proc, "spec_id": spec,
                     "grain": grain, "created_at": created, "parallel_run_id": rid})
```
`_write_research`'s `key = "company_id" if grain=="per_entity" else "run_id"` (line 342) is **unchanged**. `_write_raw` merges on `run_id` for all grains — for per_entity `run_id=rid` (unique per company, unchanged); for topic `run_id=`Trigger id (one `trun_` per topic run, correct). **No `_write_raw` merge-key change needed.**

**3d.** BTREE indexing — add `parallel_run_id` to the indexed columns in `_write_research` (line ~372: `for col in ("company_id", "run_id", "parallel_run_id"):`) and optionally `_write_raw`.

**Why this over alternatives** (see D): the worker already has the Trigger id (zero new plumbing, no mapping table); edge_api stamps the Trigger id and never sees the Parallel id, so keying topic by Trigger id makes `bookings.research_run_id = parallel_research.run_id` a **direct equi-join**; `parallel_run_id` preserves the Parallel id for ops; per_entity untouched.

### Deploy order

1. **Branch** `fix/research-waitpoint-and-join` from `main`.
2. **Apply all 3 changes.** No DB DDL (`parallel_run_id` is a Lance column via schema evolution; `research_run_id` already exists).
3. **Commit.**
4. **Deploy the Trigger task:** `doppler run -p core-x -c prd -- npx trigger.dev deploy` (the `syncEnvVars` build extension needs MODAL_* + edge envs from Doppler at deploy time — `trigger.config.ts:30-58`). Confirm a new version > `20260609.1`.
5. **Deploy the Modal worker:** `doppler run -p core-x -c prd -- modal deploy pipelines/parallel/deep_research.py`. Confirm `parallel-deep-research` redeployed.
6. **Merge + propagate edge_api:** `gh pr merge <num> --squash --delete-branch`; edge_api auto-deploys to Railway (`https://api.edgeapi.run`) on merge. Then in `/Users/benjamincrane/core-x`: `git fetch && git pull` (or `git reset --hard origin/main` if FF blocked — diagnose first) and verify `git log -1 --oneline`.
7. **(Optional) Re-key the one ERS demo row** to prove a live join before the next booking: rewrite row `trun_16b51781c2464cbca988a66e16af3939` (the ERS row) with `run_id=run_cmq8c1qk76o4f0omtax09d1hb` + `parallel_run_id=trun_16b51781c2464cbca988a66e16af3939` via a pylance `merge_insert`. Only if a live demo is needed; otherwise skip (no consumer depends on it).

### Verification

**Pre-req:** `CAL_WEBHOOK_SECRET` for signing replays (`doppler run -p core-x -c prd -- printenv CAL_WEBHOOK_SECRET`).

**G1 — two distinct bookings each get their own run:**
1. Build two `BOOKING_CREATED` cal payloads with **distinct `iCalUID`** (e.g. `test-fix-A@Cal.com`, `test-fix-B@Cal.com`) and distinct `responses["Company-Website"]` domains (a `domain` is required — `webhooks_cal.py:117` returns early without one). Use a captured ERS envelope from `public.cal_raw_events` as the shape template.
2. For each: `X-Cal-Signature-256 = hex(HMAC_SHA256(secret, raw_body))` (`signature.py:21`), POST raw to `https://api.edgeapi.run/webhooks/cal`.
3. Assert webhook response `research.triggered == true`; capture two distinct `research.run_id`.
4. In Trigger (`mcp__trigger__get_run_details`, env=prod) for **each** run: `wait.forToken()` duration is **minutes, not ~220 ms**, AND Output `run_id` **equals that run's own id** (pre-fix both returned `run_cmq7g59…`).
5. `ops.parallel_runs`: two new rows, **distinct `idempotency_key`** (now `…:run_…`, not bare `research:topic:full`), distinct `group_id`.

**G2 — booking joins its own report:**
6. Read `parallel_research` (pylance): new topic rows have `run_id` = `run_…` (Trigger) and `parallel_run_id` = `trun_…`.
7. For booking A: `SELECT research_run_id FROM corex.bookings WHERE ical_uid='test-fix-A@Cal.com';` → look up that `run_…` in `parallel_research.run_id` (pylance) → assert exactly 1 row, correct company, `parallel_run_id` populated. Repeat for B.
8. **Cross-contamination check:** booking A's joined `report_md` ≠ booking B's (pre-fix identical — both the origin's brief).

**Cost guard:** processor is `lite` (~$0.0001/run); two test runs negligible. Let them complete so rows land.

---

## D. Adversarial review

**Risks / failure modes:**

- **R1 — Deploy ordering / version skew.** TS (Change 1) and worker (Change 3) are independent. TS-only → collisions stop but topic rows still `trun_…`-keyed (G2 fails). Worker-only → joinable but output still strands on the cached token. **Deploy worker + TS together (steps 4-5); edge_api (Change 2) may lag (it only hardens Bug 1).** Verify the live Trigger version bumped past `20260609.1` and Modal redeployed.
- **R2 — Edge_api alone is insufficient for Bug 1.** Skipping Change 1 leaves gtm-mcp topic-no-spec colliding on `research:topic:full`. **Change 1 is primary; ship both.**
- **R3 — Lance schema evolution on `parallel_run_id`.** Next `merge_insert` writes 11 cols against a 10-col dataset. Lance supports column-add on merge (existing rows → NULL) but **verify on the first post-deploy write** (step 6). If it raises on an older Lance, fall back to `lance.add_columns`/recreate (only 4 disposable rows). Check the image's `pylance` version (`lancedb>=0.15`, `pylance>=7`, lines 108-110).
- **R4 — Overwrite trap.** `_write_research` only creates when genuinely absent; a `merge_insert` failure RAISES (a prior bug clobbered the dataset via overwrite — comment lines 311-312). Ensure the new column does NOT reintroduce an overwrite fallback; the `exists=True` merge branch must run and a schema error must propagate.
- **R5 — Trigger-id keying is correct for topic (1:1), WRONG for per_entity (1:N).** Keying per_entity by Trigger id would collapse N companies into one row. **Touch ONLY the topic branch's `run_id` value.** Do not "simplify" by unifying both grains.
- **R6 — idempotency TTL/retries.** Task is `retry: {maxAttempts: 1}` — no retries, so the run-unique `ctx.run.id` waitpoint key can't be reused by a retry; if retries were ever enabled, `ctx.run.id` is stable per run, so a retry correctly resumes the same waitpoint. Default 30-day TTL is now harmless (keys are unique per run).
- **R7 — Dispatch count unchanged.** Post-fix `createToken` returns a fresh token, dispatch fires once, `forToken` actually waits. The 3 pre-fix orphan `trun_…` rows remain as harmless test data.
- **R8 — `ops.parallel_runs.idempotency_key` now longer/unique.** `text`, no uniqueness constraint; `group_id` (Trigger id) remains the ledger↔booking join key, unaffected.

**Alternatives rejected (Bug 2):**
- **(a) Store both ids; dossier joins on a Parallel-id column edge_api learns.** Rejected — **edge_api never sees the `trun_` id** (minted inside Modal after edge_api returned; the callback goes to the Trigger waitpoint, not edge_api). Would need new worker→edge_api plumbing for zero benefit.
- **(b) Mapping table `ops.research_run_map`.** Rejected — adds a Postgres write on the hot path + a second read hop for every dossier read when a direct equi-join suffices.
- **(c) Stamp the Parallel id in edge_api.** Rejected — impossible; the `trun_` doesn't exist when `set_research_refs` runs.
- **(d) Join via `ops.parallel_runs.group_id` → Lance by `trun_`.** Rejected as primary — ledger row has no report content; forces a two-hop join and still needs a `trun_`↔report key the ledger doesn't carry. (`group_id` linkage left intact as a secondary ops correlation.)

**Open questions for the operator:**
1. **gtm-mcp topic-no-spec dedup:** Change 1 removes `research:topic:full` dedup for topic-no-spec. Assumed undesired (it's the bug); confirm no workflow relies on it.
2. **Backfill the one ERS demo row (step 7):** skip (default) or perform (live join demo before next booking)? Recommended: skip; validate with a fresh test booking.
3. **`parallel_enrich.ts:97` same-class latent collision:** a 2nd enrich of the same spec+audience+kind within 30 days short-circuits — may be *desired* (idempotent re-run) or a footgun. Out of scope; flag for a separate decision.

**Files (absolute):**
- Edit: `/Users/benjamincrane/core-x/src/trigger/parallel_deep_research.ts` (Change 1, line ~101)
- Edit: `/Users/benjamincrane/core-x/apps/edge_api/src/cal/research.py` (Change 2, lines 60-68)
- Edit: `/Users/benjamincrane/core-x/pipelines/parallel/deep_research.py` (Change 3, lines ~313-320, ~569-588, ~610-618, ~372-380, ~419-423)
- Reference (no edit): `src/trigger/parallel_search.ts` (correct pattern), `apps/gtm_mcp/src/tools/parallel.py` (blast radius), `core/parallel_client.py`, `apps/edge_api/src/services/trigger_dev_client.py`, `apps/edge_api/src/cal/signature.py` (replay signing).
