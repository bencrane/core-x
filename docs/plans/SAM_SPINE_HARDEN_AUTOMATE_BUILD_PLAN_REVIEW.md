# Adversarial Review — SAM Resolution Spine Harden + Automate Build Plan

**Reviewed:** `docs/plans/SAM_SPINE_HARDEN_AUTOMATE_BUILD_PLAN.md`
**Reviewer mandate:** verification-driven pre-execution review. Every load-bearing claim checked against the actual worker code, the live `ops.*` ledger (`HQX_DB_URL_POOLED`), the trigger sources, and `ARCHITECTURE.md`. Items not locally verifiable (dataset-internal counts, Lance/pylance behavior, Trigger-runtime semantics) are flagged **verify-in-execution**.

---

## 1. Verdict

The plan is **directionally correct and largely well-founded**: harden-before-automate is the right sequencing, the per-family Δ-guard / floor-qualified-baseline / population-probe / rollback-wraps-index patterns are *already proven in shipped `sam_pocs.py`* (I verified each against that file), and the headline cold-seek fix (WS-B §5.1) is a real, correctly-diagnosed bug sitting at `sam_normalized_entities.py:420` exactly as claimed. The live ledger confirms the plan's floors are sanely placed below reality. WS-A (harden `sam_master`) and WS-B (upgrade `sam_normalized`) are executable with the fixes below.

**However, WS-C as specified will not run.** The orchestrator dispatches `build_sam_master` through the Universal Dispatcher, which calls `fn.spawn(**kwargs, trigger_callback_url=...)` — but `build_sam_master`'s **first required positional argument is `sql: dict`**, a bundle of SQL strings generated *locally* on the operator's machine from a field map that is **not mounted into the container**. The dispatcher has no way to supply it. This is an architectural mismatch the plan never addresses (it treats `sam_master` as dispatcher-ready like `sam_pocs`/`sam_normalized`, which generate their SQL *inside* the container). WS-C cannot be built as written. Additionally, the plan's own freshness-gate design has a **stale-sidecar trap** (master-current ⇒ both skip, even when normalized independently failed) that it half-acknowledges in a directive but does not fix, and the **new `dataset_uri` column on `ops.sam_master_runs` silently disables every Δ-guard on the first hardened runs** because all pre-existing ledger rows are NULL-scoped out of the baseline query.

These are fixable without redesign, but they are load-bearing. The plan is not ready to execute WS-C, and WS-A needs a baseline-bootstrap decision before its Δ-guards mean anything.

**Viability rating: 7/10** — strong hardening core (WS-A/B), but WS-C has a build-breaking gap and two correctness gaps that must be closed before the automation workstream is touched.

**Severity counts:** 3 BLOCKER · 4 MAJOR · 5 MINOR

---

## 2. What's sound (verified, keep)

- **The cold-seek bug is real and correctly located.** `sam_normalized_entities.py:420` reads `if hit < 1 or seek_ms > 2000: raise RuntimeError(...)` — verbatim as the plan claims. The fix mirrors the already-shipped corrected gate-17 in `sam_pocs.py:605-607` (`if hit < 1: raise ...` then WARN-only on `seek_ms`). Confirmed `sam_pocs` ships the correct pattern. **This is the single highest-value line in the cycle and the fix is exactly right.**
- **`ops.sam_master_runs` has no `dataset_uri` column.** Verified via `\d ops.sam_master_runs` — columns are `id, feed, sam_label, entities_rows, contacts_rows, domains_rows, distinct_uei, status, error, started_at, completed_at, recorded_at`. The plan's premise (add it) is correct. `ops.sam_normalized_entities_runs` **already has** `dataset_uri` (NOT NULL) and `sam_extract_label` — verified; the plan correctly says "already has one, scope on it."
- **`sam_master` has no rollback, only the entities row-floor + uniqueness + non-empty-satellite gates** (`sam_master.py:300-305`), and writes the 3 datasets in a bare loop (`sam_master.py:318-329`) with no `v_before` capture. Verified exactly as described.
- **`sam_master` lacks `trigger_callback_url`.** Signature is `build_sam_master(sql: dict, dry_run: bool = False)` (line 249) — no callback param, no `_post_callback`. The plan's L16/§4.7 premise is correct.
- **`sam_normalized`'s write+index sit OUTSIDE the rollback try.** `write_dataset` at lines 382-387 and `create_scalar_index` at 390-395 run *before* the `try:` at line 398 that owns `restore(v_before)`. A mid-index failure leaks an under-indexed dataset exactly as the plan claims. Fix (§5.5) is correct.
- **Hardcoded `KIPPER_UEI = "DD1BCRF2QQG8"`** at `sam_normalized_entities.py:78`, used as a hard gate at lines 407-410. `sam_master` also spot-checks the same UEI (lines 309-314) but only in `dry_run`. Plan's L8 premise correct.
- **Absolute cardinality targets** at `sam_normalized_entities.py:72-73` (`NORM_DISTINCT_TARGET = 1_466_764`, `BASE_DISTINCT_TARGET = 1_450_598`), gated ±5% at line 239. Live ledger shows the dataset is *currently exactly* 1,466,764 / 1,450,598 — so the absolute target is pinned to today's value and will age out as the plan says.
- **`source_file` IS in the build scan** (`sam_master.py:279`: `columns=["uei","extract_label","source_file","pipe_fields"]`) and is projected (`sam_master.py:157,187`). So the L10 deterministic-dedup tiebreak (`source_file DESC`) is implementable. Confirmed `sam_pocs.py:260` already uses `source_file DESC NULLS LAST` as its final tiebreak — proven pattern.
- **The dispatcher spreads `**kwargs`** (`modal_dispatcher.py:53`: `fn.spawn(**req.kwargs, trigger_callback_url=req.trigger_callback_url)`), so `kwargs:{skip_if_current:true}` binds correctly *to a worker that declares that param*. Verified.
- **`queue: { concurrencyLimit: N }` is valid Trigger v4** — in use at `blitz_email_finder.ts:145` and `enrichment_blitz.ts:131,146,161`. SDK pinned `^4.4.6` (v4). (Caveat in Finding #8.)
- **Live floors check out.** `ops.sam_master_runs` latest success: `entities_rows=1,541,566`, `contacts_rows=4,373,319`, `domains_rows=709,546`, `distinct_uei=1,541,566`. Plan floors `CONTACTS_FLOOR 3.5M` (~20% headroom), `DOMAINS_FLOOR 500k` (~30% headroom), `BASELINE_MIN_ENTITIES 1.45M` (~6% below live), `NORM_FLOOR 1.3M` (~11% below live 1.467M distinct) are all sanely placed.
- **An `entity_registrations` ingest Trigger task exists** — `src/trigger/entity_registrations_backfill.ts` — but it is a manual `task()` (no cron, "Trigger manually"), so the plan's daily-freshness-gated floor (vs chaining off it) is the correct call. See Finding #11.

---

## 3. Findings (ranked)

### 1. [BLOCKER] WS-C cannot dispatch `build_sam_master` — the container has no `sql`

**Problem.** `build_sam_master(sql: dict, dry_run: bool = False)` (`sam_master.py:249`) takes a **required positional `sql`** — a dict of generated SQL strings (`proj`/`latest`/`tenure`/`entities`/`contacts`/`domains`). That dict is built **locally in the entrypoint** (`sam_master.py:352`: `sql = build_sql(PUBLIC_FIELD_MAP, DATE_POSITIONS)`) from `pipelines.sam_gov.reference.sam_v2_public_field_map`, imported only after `sys.path.insert(...)` on the operator's machine (lines 347-350). The container image adds **only** `core.ops_alert` (`sam_master.py:81`: `.add_local_python_source("core.ops_alert")`) — the field map is **not mounted**. WS-C (§6.1) dispatches via the Universal Dispatcher, which calls `fn.spawn(**req.kwargs, trigger_callback_url=...)` (`modal_dispatcher.py:53`) and passes **no `sql`**. Result: `TypeError: build_sam_master() missing 1 required positional argument: 'sql'` on the first orchestrated fire. `sam_pocs`/`sam_normalized` are dispatcher-compatible precisely because they generate their SQL *inside the container* (`build_pocs_sql()` / `build_normalized_entities_sql()` are pure, self-contained) — `sam_master` is structurally different and the plan treats them as equivalent.

**Why it matters.** WS-C is the entire automation workstream. It is unbuildable as specified; this would surface only at the first live orchestrator fire (or a `modal run` of the deployed dispatched path), after WS-A/B are merged and deployed — the worst time to discover it.

**Fix.** Move SQL generation **into the container** for the dispatched path. Either:
- (preferred) `add_local_python_source("pipelines.sam_gov.reference.sam_v2_public_field_map")` (and the `pipelines.sam_gov.reference` package `__init__`) to the `sam_master` image, and have `build_sam_master` build its own SQL internally when `sql is None`:
  ```python
  image = (modal.Image.debian_slim(...).pip_install(...)
           .env({"LANCE_BYPASS_SPILLING": "true"})
           .add_local_python_source("core.ops_alert")
           .add_local_python_source("pipelines.sam_gov.reference.sam_v2_public_field_map"))
  ...
  def build_sam_master(sql: dict | None = None, dry_run: bool = False,
                       dest_prefix: str | None = None, skip_if_current: bool = True,
                       trigger_callback_url: str | None = None) -> dict:
      if sql is None:
          from pipelines.sam_gov.reference.sam_v2_public_field_map import DATE_POSITIONS, PUBLIC_FIELD_MAP
          sql = build_sql(PUBLIC_FIELD_MAP, DATE_POSITIONS)
  ```
  The entrypoint keeps passing `sql=` (mount is harmless); the dispatcher omits it and the container self-generates. This is the only change that makes `sam_master` a first-class dispatcher citizen.
- (alternative, rejected) keep `sql` required and have the orchestrator carry the SQL dict in `kwargs` — infeasible: the SQL is multi-kilobyte generated text, the Trigger task has no access to the field map, and it would couple the control plane to the projection internals. Do not do this.

**Add to acceptance:** "The *deployed* `build_sam_master` (dispatcher path, no local mount) builds its own SQL and runs green via `modal run pipelines/sam_gov/sam_master.py` AND a dispatcher-spawned invocation — the local-entrypoint pass is not sufficient evidence."

---

### 2. [BLOCKER] New `dataset_uri` column ⇒ `baseline=None` on every hardened `sam_master` run until backfill — all Δ-guards silently skip

**Problem.** The plan adds `dataset_uri` to `ops.sam_master_runs` (§4.4) and makes the baseline query `WHERE status='success' AND dataset_uri = <prod_uri> AND entities_rows >= BASELINE_MIN_ENTITIES`. But the column is added by `ALTER TABLE ... ADD COLUMN IF NOT EXISTS dataset_uri text` — so **every pre-existing row gets `dataset_uri = NULL`**. Verified: `ops.sam_master_runs` has exactly **3 rows total, 1 with `status='success'`** (`sam_label=20260503`, `entities_rows=1,541,566`), and that row's `dataset_uri` will be NULL after the migration. The URI-scoped baseline query returns **zero rows ⇒ `baseline=None` ⇒ gates 5-7 (the per-family Δ-guards) skip** (the plan's own §4.3 says "skip if no floor-qualified prior"). The half-collapse defense (L5, the entire reason per-family Δ exists) is **inert on the first hardened prod run, and stays inert until a second clean hardened run writes a URI-stamped baseline.** This is the exact failure the `sam_pocs` hardening hit — and `sam_pocs` got away with it only because it had accumulated URI-stamped success rows before the URI scoping mattered (the ledger shows `sam_pocs` rows already carry `dataset_uri`).

**Why it matters.** The first hardened `sam_master` prod rebuild (a WS-A hard gate, §7D) runs with **no Δ-guard** — precisely the run where you most want it, because it is the first run of newly-rewritten gate/dedup/threading code. A projection regression that halves `contacts_rows` would pass (floors are 20-30% below live, so a ~50% collapse to ~2.18M would still clear `CONTACTS_FLOOR 3.5M`? No — but a 25-30% partial collapse to ~3.1M *would* clear nothing... actually 3.1M < 3.5M floor catches it; but a *15-20%* erosion to ~3.6M clears the floor and, with no Δ baseline, is **undetected**). The Δ-guard's job is exactly to catch sub-floor-but-anomalous drift, and it is off.

**Fix.** Backfill the existing success row's `dataset_uri` as part of the same migration, so the baseline query immediately sees a qualified prior:
```sql
ALTER TABLE ops.sam_master_runs ADD COLUMN IF NOT EXISTS dataset_uri text;
UPDATE ops.sam_master_runs
   SET dataset_uri = 's3://data-sink/active/sam_master_entities/'
 WHERE dataset_uri IS NULL AND feed = 'sam_master';
```
Run this **before** the first hardened prod rebuild (it is safe: the only existing rows are genuine prod runs at that URI). Add to §7D: "Confirm `_prior_success_baseline(entities_uri)` returns the backfilled 1,541,566-row prior **before** the first hardened prod build — i.e. the Δ-guards are armed, not skipped." Note this is purely a `sam_master` problem; `sam_normalized` already carries `dataset_uri` on its one success row (verified), so its first hardened run *will* have a baseline.

---

### 3. [BLOCKER] `skip_if_current` step-2 gate leaves the sidecar permanently stale when master is current but normalized is not

**Problem.** WS-C §6.1 step 2 runs `sam_normalized` **"only if master rebuilt"** (callback `status==="skipped"` on master ⇒ "spine current" ⇒ `return`, no normalized run). The plan's directive C self-identifies the scenario but the plan body does not fix it: if `sam_master` is current (skipped) while `sam_normalized` is independently stale — e.g. a prior normalized run failed/rolled back, or normalized was never run after a manual master rebuild — the orchestrator skips **both** and the sidecar **stays stale forever** (every subsequent daily fire re-skips on master-current). The live ledger makes this concrete: `sam_master` last succeeded `2026-06-04`, `sam_normalized` last succeeded `2026-06-06` — they are decoupled in time and have failed independently before (the `sam_pocs` ledger shows interleaved `error` rows; the same class of independent failure applies here).

**Why it matters.** The spine's *consumer* surface is `sam_normalized_entities` (crosswalks resolve against it, per the lineage diagram). A stale sidecar with a fresh master is the worst silent state — the system *looks* current (master advanced) but every name→UEI bridge resolves against old keys. This defeats the plan's own stated objective ("no longer held together by hand" / "silent-staleness class closed").

**Fix.** Decouple the two staleness checks. The orchestrator must evaluate `sam_normalized`'s own freshness independently of whether master rebuilt:
- Step 1: dispatch `build_sam_master` with `skip_if_current:true`. On `skipped` → master is current; **do not return** — fall through to step 2.
- Step 2 (**always**, not "only if master rebuilt"): dispatch `build_sam_normalized_entities` with `skip_if_current:true`. Its own `skip_if_current` (WS-B §5.6: compare source `sam_master_entities` max `sam_extract_label` vs the sidecar's current label) correctly no-ops when the sidecar already matches the master, and rebuilds when it lags. On master `success` (rebuilt), step 2 will see a label mismatch and rebuild; on master `skipped` (current), step 2 self-skips *iff* the sidecar already matches — and rebuilds if it does not.

This makes the chain idempotent **and** self-healing: a stale sidecar is caught on the next daily fire regardless of master's state. The cost is one cheap extra dispatch + label compare per day when both are current — negligible, and the whole point of self-skipping workers. (This also closes the directive-E recovery gap: if the orchestrator crashes between master and normalized, the next daily fire's unconditional step 2 recovers the sidecar.) Update §6.1, the §9 "Idempotent + self-skipping" line, and acceptance item 8 accordingly.

---

### 4. [MAJOR] `skip_if_current` label comparison is unreliable — two label *formats* for the same snapshot

**Problem.** WS-A §4.7 / WS-B §5.6 gate the skip on *equality* of a freshly-resolved `entity_registrations` label against the stored `sam_extract_label`. But the two values are produced by **different code paths with different normalizations**, and SAM ships **two label formats for the same logical month**:
- `entity_registrations_bulk._classify` (line 156-158) emits `extract_label` as either the V2 numeric form (`20260503`) or the legacy form (`2026_MAY`), depending on the source filename.
- `sam_master.py:186` stores `sam_extract_label = l.extract_label` — the **raw label of the dedup-winning row** (whatever survived `ORDER BY last_update_date DESC, initial_registration_date DESC, {snap_key} DESC`).
- `sam_master.py:271-273` separately computes its "latest label" as `ORDER BY _snap_key_sql() DESC LIMIT 1` — the **numeric-normalized** max, which can resolve to a row whose raw label is `2026_MAY` even when a `20260503`-labeled row exists.

The `ops.sam_pocs_runs` ledger **proves the collision empirically**: consecutive successful runs over the same underlying data recorded `sam_label='2026_MAY'` (2026-06-06 16:34) and `sam_label='20260503'` (2026-06-06 16:59). Raw-string equality of `2026_MAY` vs `20260503` is **false** even though they denote the same snapshot. A naive `skip_if_current` will therefore (a) **fail to skip** when the two paths disagree on format (wasteful but safe), or worse (b) **wrongly skip** if the stored label happens to equal a resolved label that is *not actually the latest* by snap-key order.

**Why it matters.** A `skip_if_current` that flaps between skip and rebuild on no real change is merely wasteful; one that *wrongly skips* (case b) reintroduces the staleness the plan exists to kill. The comparison must be over the **canonical snap-key**, not the raw label.

**Fix.** Compare the **normalized snap-key**, not raw labels. Both workers already have `_snap_key_sql`. In `skip_if_current`:
```python
# current target label → snap-key
cur = con.execute(f"SELECT {_snap_key_sql('extract_label')} FROM (SELECT ? AS extract_label)",
                  [stored_sam_extract_label]).fetchone()[0]
latest = con.execute(f"SELECT max({_snap_key_sql('extract_label')}) FROM lbl ...").fetchone()[0]
if cur == latest: return {"status": "skipped", ...}
```
i.e. resolve *both* sides through `_snap_key_sql` and compare the BIGINT keys. This is format-agnostic and matches the dedup ordering the worker already trusts. Spell this out in §4.7 / §5.6 — the current wording ("read the current `sam_master_entities` `sam_extract_label` (one value)" and compare) reads as a raw-string compare and will be implemented as one.

---

### 5. [MAJOR] `sam_master` 3-dataset rollback: net-new datasets cannot roll back, and a partial-family commit can persist undetected

**Problem.** §4.6's rollback captures `v_before[name]` per dataset and on failure restores each whose `v_before is not None`. Two gaps:
1. **Net-new dataset (`v_before=None`) is silently un-rollback-able.** If dataset N writes fine, N+1 is genuinely net-new (no prior version), and N+1's write/index *fails*, then N+1 has `v_before=None` and is **not** restored — it stays as a partially-written/under-indexed live dataset, while N is rolled back. The family is left **mixed**: N at old version, N+1 a fresh-but-broken overwrite. The plan's §9 claim "A partial-family state is never left committed" is **false in the net-new case.** (For the spine today this is moot — all 3 datasets exist, verified by the live ledger's non-zero counts — so on prod it degenerates to the all-have-`v_before` case. But on the **scratch prefix** (§7B/7C), the datasets *are* net-new on build #1, and §7C-2 explicitly injects a contacts-index failure on scratch — where `v_before=None` for all three. The mid-index rollback test (§7C-2) as written **will not restore anything** because scratch build #1 has no prior version, so the test cannot prove what it claims.)
2. **Order dependency unverified.** The build materializes entities→contacts→domains, then writes in that order. The plan asserts atomicity but does not confirm contacts/domains do not *depend on entities being written first*. Verified from the code (`sam_master.py:191-210`): contacts and domains are built from the in-memory `latest` CTE, **not** from the written entities dataset — so there is no read-after-write dependency, and write order is safe. (Good — but state this explicitly so a future edit doesn't introduce one.)

**Why it matters.** Gap (1) makes the §7C-2 negative test a no-op on a fresh scratch prefix (it proves nothing), and the plan would record a green checkmark for an untested rollback path. On prod the path *is* exercised, but the test that's supposed to validate it runs in the one environment where it's inert.

**Fix.**
- For the §7C-2 mid-index rollback test, **pre-seed the scratch datasets** with one clean scratch build (#1 from §7B), *then* inject the bogus contacts index on build #2 — so all three have a real `v_before` and the restore is genuinely exercised. State in §7C-2: "run after at least one clean scratch build so `v_before` is non-None for all three."
- For the net-new partial-family case, make it **loud, not silent**: in the rollback handler, if any dataset has `v_before is None` AND was written this run, raise a distinct error naming the orphaned net-new datasets to inspect/drop (mirror `sam_pocs.py:617`'s net-new branch, but for the family). Net-new partial commits should never pass silently.
- Add one sentence to §4.6/§9: "contacts/domains are derived from the in-memory `latest` relation, not the written entities dataset — write order carries no read-after-write dependency."

---

### 6. [MAJOR] Population probe must be guaranteed present in *both* entities and contacts — the most-contacts UEI is not

**Problem.** §4.2 derives `probe_uei` as "a uei known to have ≥1 contact row, e.g. most-contacts uei," and §4.6 round-trips it across **both** entities and contacts (asserting `hit>=1` in each). The "most-contacts uei" is guaranteed in *contacts* but the plan does not prove it is in *entities*. It almost certainly is (every contact's uei comes from the same `latest` CTE that feeds entities), but "almost certainly" is not a gate guarantee — and the `sam_pocs` probe (`sam_pocs.py:338-341`) deliberately derives its probe from `WHERE uei IS NOT NULL AND name_key IS NOT NULL` (i.e. from the *output* table) precisely to guarantee presence. The `sam_master` probe should be derived from the **intersection**, not from contacts alone.

**Why it matters.** If the probe UEI is somehow absent from entities (e.g. a future change where contacts retains a uei entities drops), the post-write gate false-fails and rolls back a good 3-dataset build — the exact L1/L8 class of false-positive this plan is trying to eliminate, reintroduced via an under-specified probe.

**Fix.** Derive the probe from a uei present in **both** materialized tables before any write, e.g. in the same DuckDB connection:
```sql
SELECT c.uei FROM (SELECT uei, count(*) n FROM contacts GROUP BY uei) c
JOIN (SELECT DISTINCT uei FROM entities) e USING (uei)
ORDER BY c.n DESC, c.uei LIMIT 1
```
Emit `probe_uei` from this intersection (deterministic via the `uei` tiebreak). State in §4.2 that the probe is the intersection's most-contacts uei, not contacts-only.

---

### 7. [MAJOR] Dropping `sam_normalized`'s absolute cardinality target — the coarse floor must catch a *base*-key collapse too

**Problem.** WS-B §5.3 replaces gate 5's absolute `NORM_DISTINCT_TARGET`/`BASE_DISTINCT_TARGET` (±5%) with a coarse floor on `distinct_normalized_name` (`NORM_FLOOR 1.3M`) **plus** a Δ-guard on `distinct_normalized_name`. But the current gate 5 (`sam_normalized_entities.py:239-242`) checks **two** cardinalities — `distinct_normalized_name` AND `distinct_legal_name_base`. The plan's replacement only names a floor + Δ for `distinct_normalized_name`; `distinct_legal_name_base` (currently 1,450,598, the suffix-peeled key) loses both its absolute check **and** is not given a Δ-guard or floor. A normalization regression that corrupts *only* the suffix-peeling (`legal_name_base`) while leaving `normalized_legal_name` intact would pass every replacement gate. The plan's L6 decision table says "per-family Δ-guards: `rows`, `distinct_normalized_name`" — it **omits `distinct_legal_name_base`** entirely, regressing coverage vs the absolute target it removes.

**Why it matters.** `legal_name_base` is a load-bearing blocking key (it's BTREE-indexed, line 61, and consumed by suffix-tolerant bridge joins). Losing its regression check is a real coverage loss the Δ-guard "doesn't replace" — exactly the directive-D concern.

**Fix.** Carry `distinct_legal_name_base` in the per-family Δ-guard set and give it its own coarse floor. The ledger already records `distinct_legal_name_base` (verified column present), so the baseline query can return it:
- baseline returns `{rows_written, distinct_normalized_name, distinct_legal_name_base}`;
- gate: `distinct_legal_name_base >= BASE_FLOOR` (e.g. 1_280_000, ~12% below live 1.45M) **and** within ±`DELTA_GUARD` of the floor-qualified prior.
Update §5.3 and the L6 row to include `distinct_legal_name_base`.

---

### 8. [MINOR] `schedules.task` + `queue:{concurrencyLimit:1}` combo is unprecedented in this repo — verify it actually serializes scheduled fires

**Problem.** `queue:{concurrencyLimit:N}` is valid v4 and in use — but **only on plain `task()`** (`blitz_email_finder.ts`, `enrichment_blitz.ts`). **No existing `schedules.task` in the repo carries a `queue`** (verified across all 25 `schedules.task` definitions). The plan's `schedules.task({ cron, queue:{concurrencyLimit:1} })` is type-valid but unproven here, and the directive-E question — *can two daily fires overlap if one is suspended on a waitpoint while the next starts?* — is not answered by code I can read. Trigger's concurrency accounting around checkpointed/suspended waitpoint runs is a runtime semantic: a run suspended on `wait.forToken` may or may not still occupy a concurrency slot.

**Why it matters.** If a suspended run *releases* its slot, `concurrencyLimit:1` does **not** prevent overlap: fire N suspends on the master waitpoint (slot freed), fire N+1 starts and dispatches `sam_master` again while N's `sam_master` is still running. Two concurrent `mode="overwrite"` rebuilds of the same dataset is the overlap hazard the plan claims concurrency-1 closes.

**Fix.** Treat concurrency-1 as **necessary but not proven sufficient**. Either (a) confirm via Trigger docs/MCP that a waitpoint-suspended run retains its concurrency slot (if so, document it inline citing the source), or (b) add a belt-and-suspenders **in-flight ledger guard**: the orchestrator's first action queries `ops.sam_master_runs` / `ops.sam_normalized_entities_runs` for a `started_at` within the last N hours with no `completed_at`, and bails if one is in flight. Given daily cadence and overwrite semantics, (b) is cheap insurance. At minimum, add an acceptance line: "verified (doc-cited or empirically) that `concurrencyLimit:1` serializes *across the waitpoint suspension*, not just at task entry." Flag **verify-in-execution**.

---

### 9. [MINOR] `primary_naics` numeric-frac 0.95 floor — `primary_naics` is plausibly low-fill; a fill-skewed denominator could false-trip or false-pass

**Problem.** §4.3 gate 8 asserts `naics_numeric_frac >= 0.95` over **non-null** `primary_naics`. `primary_naics` is lifted positionally in `sam_master`'s frozen field map (the plan cites "pos 33"; I confirmed it's one of the scalar positional lifts via `_scalar_expr`, but the exact position and its real null/blank rate are **not locally verifiable** — `pylance`/`duckdb` aren't importable here and the dataset internals are in Lance). The gate computes the fraction over *non-null* values, so legit nulls don't skew it (good) — but if `primary_naics` is heavily blank (e.g. many SAM entities have no primary NAICS), the denominator is small and a positional-offset that shifts a *different* numeric field into the `primary_naics` slot would still read ~100% numeric and **pass** the gate while the column is silently wrong. Conversely, if SAM occasionally emits NAICS with a trailing alpha or a range, the numeric-only regex could dip below 0.95 on clean data and false-trip. `sam_pocs` uses the analogous `zip_numeric_frac >= 0.95` and `name_alpha_frac >= 0.95` (`sam_pocs.py:413-416`) — proven to work for *zip5* and *first_name*, which are high-fill — so the pattern is sound, but `primary_naics`' fill profile is the unverified variable.

**Why it matters.** A content gate calibrated on an unverified fill rate either provides false comfort (high-blank column, offset undetected) or false-fails on legit data. The plan's §4.3 note ("content floors a few points below observed") is the right instinct but presupposes you've *observed* the real fraction.

**Fix.** Make the §7B scratch build **emit and log `naics_numeric_frac` and the `primary_naics` non-null rate**, and set the floor a few points below the *observed scratch* value (the plan says to do this for content floors — make it explicit for NAICS specifically). If the non-null rate is low (say <60%), prefer a content check on a **higher-fill** positional column as the primary offset-defense (e.g. `legal_business_name` alpha-frac, which §4.3 gate 9 already includes and which is near-100% fill) and treat NAICS-numeric as secondary/observational. Flag the 0.95 NAICS floor **verify-in-execution** against the scratch fraction before prod.

---

### 10. [MINOR] Per-family Δ on `contacts_rows`/`domains_rows` partially co-moves with `entities_rows` — confirm independent half-collapse coverage

**Problem.** Directive D asks whether `contacts_rows`/`domains_rows` Δ-guards are independent enough from `entities_rows` to catch a real half-collapse. From the code: `contacts` (≤6 POC rows/uei) and `domains` (entity_url-derived) are both built from the **same `latest` CTE** as entities — so a collapse in the *source* (`latest`) moves all three together, and the entities Δ-guard would catch that. The per-family guards add value specifically for a regression in the **per-family projection logic** (e.g. the POC unpivot at `sam_master.py:191-197` drops blocks, or the domain regex at 199-210 over-filters) that leaves entities intact. That is real, independent coverage (it's the L5 v2-half-collapse class). But the guard is only as good as the baseline — see Finding #2: with `baseline=None` on the first hardened runs, **none** of the per-family guards fire.

**Why it matters.** The per-family guards are genuinely independent for projection-logic regressions (the case they're designed for), so the design is sound — but Finding #2 (baseline backfill) is a prerequisite for any of them to be active, and this finding underscores that #2 is not optional polish.

**Fix.** No change to the guard design — it's correct. This finding is a dependency note: **#2 must land for #10's coverage to exist.** Add a unit-test fixture (§7A already plans "a contacts-half-collapse caught by the per-family Δ") and assert it fires *given a baseline* — and a second fixture proving it correctly **skips** when `baseline=None` (so the skip path is tested, not just assumed).

---

### 11. [MINOR] Chaining off the manual `entity_registrations_backfill` is correctly rejected — but say *why* in the plan, and reconsider the slot

**Problem.** Directive F asks whether to chain off an `entity_registrations` ingest task. One exists: `src/trigger/entity_registrations_backfill.ts` — but it's a manual `task()` (no cron; doc says "Trigger manually (dashboard Test, or MCP)"), and `entity_registrations_bulk.py` is "a BOUNDED backfill, not a daily feed (no Trigger cron)." So there is **no recurring completion event to chain off** — the plan's daily-freshness-gated floor is the right call, matching the documented `crosswalk_sam_usaspending.ts` rationale ("None of those upstreams is on a daily cron today, so there is no completion event to chain off"). The plan's §6 mentions this only as an optional "If an `entity_registrations` ingest Trigger task already exists" aside — it should state definitively that the task exists but is manual, so the daily floor is primary and an *optional* completion-callback hook off the backfill can be added later (mirroring the gleif/fmcsa time-offset pattern). Separately: the proposed `0 18 * * *` UTC slot **collides** with `contractor_award_summary` (also `0 18 * * *` UTC, verified) — both dispatch to the same Modal account.

**Why it matters.** Minor, but the plan's hedged wording ("If ... already exists ... MAY additionally be invoked") suggests uncertainty about something that is verifiable and verified. And a shared cron-minute with another heavy dispatcher is a small avoidable load spike.

**Fix.** (1) In §6, replace the conditional with the fact: "`entity_registrations_backfill` exists but is a manual, cron-less backfill task — no recurring completion event — so the daily freshness-gated schedule is the sole driver; an optional completion-callback hook off the backfill is a future enhancement." (2) Move the orchestrator off the 18:00 collision — e.g. `30 18 * * *` (after `contractor_award_summary`'s window, still after the 12:00 SAM drop + 16:30 `sam_pocs`).

---

### 12. [MINOR] `OPS_DDL` `ALTER` for `dataset_uri` must also update the canonical `.sql` file, or the two drift

**Problem.** The plan adds `dataset_uri` to `sam_master`'s `OPS_DDL` constant (§4.4). But `ops.sam_master_runs` has **no canonical `.sql` sidecar file** (only `ops_sam_normalized_entities_runs.sql` and `ops_sam_pocs_runs.sql` exist — verified; the `sam_master` DDL lives *only* inline at `sam_master.py:85-102`). The other two workers' `.sql` files carry a "CANONICAL COPY ... Keep in sync" banner. Adding `dataset_uri` only to the inline `OPS_DDL` is internally consistent for `sam_master` (no sidecar to drift from), but the plan should either (a) create `pipelines/sam_gov/ops_sam_master_runs.sql` to match the fleet convention, or (b) explicitly note `sam_master` has no canonical sidecar so the inline DDL is authoritative. Also: `CREATE TABLE IF NOT EXISTS` will **not** add the column to the already-existing table — the plan correctly uses a separate `ALTER ... ADD COLUMN IF NOT EXISTS`, but that `ALTER` must run in `_record_run`'s `cur.execute(OPS_DDL)` path (i.e. append the `ALTER` to the `OPS_DDL` string, not just the `CREATE`), or deployed runs never migrate the live table.

**Why it matters.** If the `ALTER` lives somewhere the build doesn't execute on every run (e.g. only in an `init_ops` the orchestrator never calls — note `sam_master` has **no `init_ops` function**, verified), the column never gets added in prod and `_record_run` throws on the unknown column. `sam_master`'s `_record_run` calls `cur.execute(OPS_DDL)` (line 227) — so the `ALTER` must be inside `OPS_DDL`.

**Fix.** Put the `ALTER TABLE ops.sam_master_runs ADD COLUMN IF NOT EXISTS dataset_uri text;` **inside** the `OPS_DDL` string (after the `CREATE TABLE`), so every `_record_run` idempotently ensures the column. Add the Finding-#2 `UPDATE` backfill as a one-time op (run via `psql` or a tiny `init_ops`-style function, since `sam_master` lacks `init_ops`). Optionally create the canonical `ops_sam_master_runs.sql` to match fleet convention.

---

## 4. Strategic recommendations

1. **Re-sequence WS-C's prerequisites.** Finding #1 (container `sql`) is a `sam_master`-code change — it belongs in **WS-A**, not deferred to WS-C, because WS-A's acceptance already requires a *deployed, prod-verified* `sam_master`, and "dispatcher-spawnable" is part of what WS-C's gate ("both workers deployed & green") must mean. Fold the `add_local_python_source` + internal-SQL change into WS-A and add a WS-A acceptance line that the **dispatched** path (not just the local entrypoint) runs green. Otherwise WS-A passes, WS-B passes, and WS-C dies on its first fire with a `sam_master` defect that should have been caught two PRs earlier.

2. **Make `skip_if_current` snap-key-based fleet-wide.** Finding #4's raw-label-vs-snap-key hazard is not unique to this plan — any future worker that gates on `sam_extract_label` equality inherits it. Since `_snap_key_sql` is *already* duplicated across `sam_master`/`sam_pocs` (and §10/next-cycle plans to consolidate it into `pipelines/sam_gov/reference/sam_labels.py`), consider pulling that consolidation **forward** into WS-A — the `skip_if_current` correctness depends on snap-key normalization, so the shared module earns its place now rather than next cycle. (Weigh against scope discipline; if deferred, at minimum both workers must snap-key-normalize both sides of the compare.)

3. **The residual-risk framing undersells the 3-dataset window — but the plan's mitigation is the right *next*-cycle call.** §0/§2 accept `mode="overwrite"` with the window "longer for `sam_master`'s 3 datasets." Correct, and the rollback bounds it. But note the window is not just *longer* — it is **3× the failure surface** (three independent write+index sequences, each of which can fail mid-flight), and during the window the three datasets are **not mutually consistent** (entities overwritten, contacts not yet). Any consumer reading the spine *during* a rebuild can observe a torn family. The plan correctly defers staging/atomic-promote to next cycle (§11.1) and that is the right boundary — but the residual-risk note should say "torn-family reads are possible during the rebuild window" explicitly, not just "longer window," so the accepted risk is honestly stated.

4. **Add a smoke-level consumer assertion to §7D.** After the prod cutover, the *point* of the spine is that `crosswalk_sos_sam` / name→UEI bridges resolve against it. A one-line read-back (e.g. resolve a known name through the fresh `sam_normalized_entities` BTREE) as the final §7D step proves the surface is actually usable post-rebuild, not just that the gates passed. Cheap, high-signal, closes the loop the objective opens ("the surface every crosswalk loads onto").

5. **Harden-before-automate is correctly sequenced — keep it absolute.** The plan's "WS-C lands only after A and B are merged, deployed, and prod-verified" is the single most important strategic call and it is right. Do not let the WS-C blockers tempt a "ship C with a TODO" shortcut; an automated un-hardened (or un-dispatchable) spine is strictly worse than the current manual-but-safe state.

---

## 5. Amended acceptance criteria (delta to plan §10)

Add / modify the following (numbered to extend §10):

- **[A — replaces "callback plumbing" sub-bullet]** `build_sam_master` accepts `dest_prefix`, `skip_if_current`, `trigger_callback_url` **and builds its own SQL inside the container** (`sql=None` path + `add_local_python_source` of the field map). Proven by a **dispatcher-spawned** (not local-entrypoint) green run. *(Finding #1)*
- **[A — new]** Before the first hardened prod build, `ops.sam_master_runs.dataset_uri` is added **and the pre-existing success row is backfilled** to the prod URI; `_prior_success_baseline()` returns that 1,541,566-row prior (Δ-guards armed, not skipped). Verified by query. *(Finding #2)*
- **[A — new]** The `dataset_uri ALTER` lives **inside `OPS_DDL`** (runs on every `_record_run`); a deployed run on a fresh column does not throw. *(Finding #12)*
- **[A — new]** `probe_uei` is derived from the **entities∩contacts** intersection (most-contacts uei present in both), not contacts-only. *(Finding #6)*
- **[A — modified §7C-2]** The mid-index rollback test runs **after ≥1 clean scratch build** so `v_before` is non-None for all three; the restore is genuinely exercised (versions unchanged post-failure). A net-new partial-family failure raises a distinct loud error naming orphaned datasets. *(Finding #5)*
- **[A — new]** `naics_numeric_frac` and `primary_naics` non-null rate are logged from the scratch build; the 0.95 floor is set below the *observed* fraction (or NAICS-numeric is demoted to observational if fill is low, with `legal_business_name` alpha-frac as the primary offset-defense). *(Finding #9)*
- **[B — modified]** Per-family Δ-guard + coarse floor cover **`distinct_legal_name_base`** in addition to `rows` and `distinct_normalized_name`; the retired absolute targets lose no coverage. *(Finding #7)*
- **[B — new]** Unit tests assert the per-family Δ both **fires given a baseline** and **correctly skips when `baseline=None`** (the first-hardened-run path is tested, not assumed). *(Findings #2, #10)*
- **[B/A — new]** `skip_if_current` compares **snap-key-normalized** values on both sides (not raw `sam_extract_label` strings); a `2026_MAY`-vs-`20260503` same-snapshot pair correctly resolves to "current." *(Finding #4)*
- **[C — replaces item 8 / §6.1 step 2]** The orchestrator runs `sam_normalized`'s `skip_if_current` dispatch **unconditionally** (not only when master rebuilt); a current-master + stale-sidecar state is detected and healed on the next daily fire. Demonstrated: force the sidecar stale while master is current → orchestrator rebuilds the sidecar. *(Finding #3)*
- **[C — new]** Either doc-cited or empirically confirmed that `concurrencyLimit:1` serializes **across waitpoint suspension** (a suspended run retains its slot); else an in-flight ledger guard is added. *(Finding #8)*
- **[C — new]** Orchestrator cron does **not** collide with `contractor_award_summary` (`0 18 * * *`); use e.g. `30 18 * * *`. *(Finding #11)*
- **[D — new, §7D]** Post-cutover, a known name resolves through the fresh `sam_normalized_entities` BTREE (consumer-surface smoke check). *(Strategic #4)*

---

*Verification basis: `sam_master.py`, `sam_normalized_entities.py`, `sam_pocs.py`, `entity_registrations_bulk.py`, `modal_dispatcher.py`, `sam_pocs.ts`, `crosswalk_sam_usaspending.ts`, `entity_registrations_backfill.ts`, `trigger.config.ts`, `ARCHITECTURE.md`, both `ops_sam_*_runs.sql`, and live `ops.sam_master_runs` / `ops.sam_normalized_entities_runs` / `ops.sam_pocs_runs` rows (read-only, `HQX_DB_URL_POOLED`, 2026-06-06). Dataset-internal counts and Trigger-runtime concurrency semantics were not locally executable (`pylance`/`duckdb` not importable; no Trigger runtime) and are flagged verify-in-execution where they bear on a finding.*
