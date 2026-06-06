# SAM POCs Hardening — Adversarial Pre-Execution Review

**Reviewer role:** Principal Data Engineer, adversarial gate.
**Artifact under review:** [`docs/plans/SAM_POCS_HARDENING_BUILD_PLAN.md`](SAM_POCS_HARDENING_BUILD_PLAN.md)
**Target:** [`pipelines/sam_gov/sam_pocs.py`](../../pipelines/sam_gov/sam_pocs.py) · **Reference:** [`pipelines/sam_gov/sam_normalized_entities.py`](../../pipelines/sam_gov/sam_normalized_entities.py)
**Ledger evidence:** live `ops.sam_pocs_runs`, queried read-only during this review (full history is only 7 rows; reproduced inline below).

---

## 1. Verdict

The plan is well-researched, correctly identifies a real and present danger (an unattended daily destructive overwrite with no floor and no rollback), and ports a *proven* sibling pattern — most external-API assumptions are de-risked because `sam_normalized_entities.py` already ships them. The signature is preserved, the no-mount call for `_snap_key_sql` is correct, WS-C's `source_file` premise is verified to exist in the source, and WS-B's target label matches the rest of the SAM fleet. **But the plan's central safety claim is wrong on the one failure mode it most needs to catch, and its own validation harness will silently poison the production ledger.** The live ledger contains the smoking gun: a real 6,389,167-row partial build (label `20260503`, distinct_uei 888,361) that *recorded `success`* on 2026-06-02 — and that exact partial **passes the Δ-guard the plan calls "the sensitive check," passes the row floor, and passes gate 3.** It is caught only by the distinct_uei/distinct_cage floors, which the plan frames as secondary. Separately, the plan runs its full negative-path harness against a scratch URI but `_record_run` is hardwired to `feed='sam_pocs'` and the plan's own `_prior_success_rows()` query does not filter by `dataset_uri` — so scratch runs write `success` rows to the prod ledger and become the next cron's baseline. Both are fixable with surgical changes already scoped below; neither requires re-architecting.

**Viability rating: Blockers must be fixed first — then ship.** The architecture (port the sibling pattern) is right. Three blockers and four majors gate the merge; once addressed, this is a strong, in-scope hardening.

**Findings by severity:** 3 BLOCKER · 4 MAJOR · 5 MINOR.

---

## 2. What's sound (keep)

- **Problem statement is real and now ledger-proven.** The 2026-06-02 02:08 row is a literal `status='success'` with `rows_written=0` / all-zero metrics. Fail-silent is not hypothetical here; it has already happened in production. Porting the gate+rollback pattern is the correct response.
- **Reuse of the proven sibling.** `assert_pre_write_gates`/`gate()` closure, `_prior_success_rows()`, `v_before` capture, post-write `restore()`, the `idx_names` set-comprehension with the dict/attr fallback, the 2 s R2-RTT seek ceiling, and the `KIPPER_UEI` round-trip are all *already running* in `sam_normalized_entities.py`. Treat them as de-risked. The only genuine deltas are POC-grain-specific (gate semantics, two families, the KIPPER-has-a-POC assumption) — flagged below.
- **Signature preservation is correct and already satisfied.** Verified end-to-end: `src/trigger/sam_pocs.ts:51` sends `kwargs: {}`; `core/modal_dispatcher.py:53` calls `fn.spawn(**req.kwargs, trigger_callback_url=...)`. `build_sam_pocs(trigger_callback_url=None)` is invoked with exactly one kwarg. The plan adds no required kwarg. No blocker here.
- **`_snap_key_sql` no-mount call is correct.** `sam_pocs.py` imports nothing repo-local (no `core.*`); a module-level copy ships in the container image automatically. Unlike `sam_normalized_entities` (which imports `core.name_norm` and therefore needs `add_local_python_source`), `sam_pocs` needs no mount for WS-B. The plan states this correctly. (The §10.1 follow-up that *extracts* `_snap_key_sql` to a shared module WILL need the mount — the plan flags that too.)
- **WS-C premise verified.** `source_file` is written into `entity_registrations` by the bulk loader (`entity_registrations_bulk.py:259`, `'{lit(key)}' AS source_file`). Adding it to the scan and the QUALIFY tiebreak is legitimate and cheap.
- **WS-B target verified.** `sam_master_entities` and `sam_normalized_entities` both sit at `sam_label/sam_extract_label = '20260503'` with 1,541,566 rows (live ledger). Stamping `sam_pocs` `'20260503'` genuinely aligns the fleet; the current `'2026_MAY'` is the lexical-max bug.
- **`import time` placement is fine.** The function already imports `time` only inside `_post_callback`; adding a function-level `import time` to `build_sam_pocs` mirrors `sam_normalized_entities.py:348` exactly. Non-issue.
- **Restore semantics are proven.** `lance.dataset(uri, version=N).restore()` is the same call the sibling ships. Lance `restore()` makes the old manifest the new latest version; because indices are referenced by the manifest, restoring the manifest restores the index set atomically (addressed in finding #11 with the one caveat that matters).

---

## 3. Findings (ranked)

Live ledger (the whole table — 7 rows) referenced throughout:

```
recorded_at                    | status  | sam_label | rows_written | distinct_uei | distinct_cage | poc_rows_v2 | poc_rows_legacy | dataset_uri
2026-06-05 16:33:47 success | 2026_MAY | 8065116 | 1540966 | 1167572 | 4372870 | 3692246 | active/sam_pocs/
2026-06-04 16:38:44 success | 2026_MAY | 8065165 | 1540965 | 1167572 | 4372895 | 3692270 | active/sam_pocs/
2026-06-03 16:39:16 success | 2026_MAY | 8065160 | 1540965 | 1167572 | 4372868 | 3692292 | active/sam_pocs/
2026-06-02 16:33:39 success | 2026_MAY | 8065167 | 1540966 | 1167572 | 4372901 | 3692266 | active/sam_pocs/
2026-06-02 03:04:01 success | 2026_MAY | 8065079 | 1540966 | 1167572 | 4372826 | 3692253 | active/sam_pocs/
2026-06-02 02:19:26 success | 20260503 | 6389167 |  888361 | 1167571 | 2696876 | 3692291 | active/sam_pocs/   <-- 30% partial, recorded success
2026-06-02 02:08:36 success | 20260503 |       0 |       0 |       0 |       0 |       0 | active/sam_pocs/   <-- zero build, recorded success
```

---

### [BLOCKER] 1 — The Δ-guard does NOT catch the historically-observed partial; the plan mis-frames it as the primary defense

*Problem.* Plan §4.1 comment: *"the Δ-guard is the sensitive check that auto-tracks month-over-month growth."* §5 gate table leans on it. But run the real numbers against the real partial:

- Prior success = 8,065,116. Δ-guard ±25% lower bound = **6,048,837**.
- The observed partial = **6,389,167**, which is **20.78%** below prior → **inside ±25% → gate 8 PASSES.**
- Row floor 6.0M → 6.39M ≥ 6.0M → **gate 1 PASSES.**
- Gate 3 (`poc_rows_v2 > 0 ∧ poc_rows_legacy > 0`) → v2=2,696,876, legacy=3,692,291 → both > 0 → **gate 3 PASSES.**

The partial is caught **only** by gate 2 (`distinct_uei 888,361 ≥ 1,300,000` → FAIL) and gate 4 (`distinct_cage`). The Δ-guard, the row floor, and the family gate would all wave it through. The plan's stated primary defense is the wrong one for the exact failure class on record.

*Why it matters.* This is the single most likely real regression (a v2-half classification break — the same class the docstring and gate-3 comment fear), and the plan's mental model of "floors are coarse, Δ-guard is sensitive" is inverted for it. If a future edit ever weakens gate 2 (e.g. someone widens `DISTINCT_UEI_FLOOR` headroom, or the floor is removed as "redundant with the Δ-guard"), the partial ships. The blast radius is the GTM/MCP gateway silently losing ~650K distinct entities' POCs for a day.

*Fix.* Keep all floors as first-class (do **not** let anyone treat the Δ-guard as a superset), and add a **per-family Δ-guard** so a half-collapse is caught by the *sensitive* check, not just the coarse floor. Concretely, persist and gate `poc_rows_v2` and `distinct_uei` against prior success too:

```python
# in _prior_success_rows(): return a dict, not a scalar
cur.execute("SELECT rows_written, distinct_uei, poc_rows_v2, poc_rows_legacy "
            "FROM ops.sam_pocs_runs WHERE status='success' AND rows_written IS NOT NULL "
            "AND rows_written >= %s "                       # floor-guarded baseline (see #2)
            "AND dataset_uri = %s "                          # prod-only baseline   (see #3)
            "ORDER BY recorded_at DESC LIMIT 1", (POCS_ROW_FLOOR, DATASET_URI))
```
```python
# new gate 8b in assert_pre_write_gates, only when prior is present:
gate(_within(metrics["poc_rows_v2"], prior["poc_rows_v2"], DELTA_GUARD),
     f"8b v2 Δ-guard: {metrics['poc_rows_v2']:,} within ±{DELTA_GUARD:.0%} of prior {prior['poc_rows_v2']:,}")
gate(_within(metrics["distinct_uei"], prior["distinct_uei"], DELTA_GUARD),
     f"8c uei Δ-guard: {metrics['distinct_uei']:,} within ±{DELTA_GUARD:.0%} of prior {prior['distinct_uei']:,}")
```
With this, the 6.39M partial fails 8b (v2 2.70M vs 4.37M = −38%) **and** 8c (uei 888K vs 1.54M = −42%) — caught by the sensitive guard exactly as the plan intends. Reuse the sibling's `_within` helper (the plan already lists it as borrowable, §3).

---

### [BLOCKER] 2 — The Δ-guard locks in a degraded baseline: a *good* recovery build is rolled back

*Problem.* The plan's `_prior_success_rows()` (§4.7) selects the latest `status='success'` row unconditionally. The 2026-06-02 partial recorded `success`. Therefore, after any partial-that-passed (or after the hardened build's *own* gate-2 abort still leaves the prior partial as the latest success), the next baseline = 6,389,167. The real recovery build is 8,065,116 = **+26.23% → exceeds +25% → gate 8 FAILS → the GOOD build is aborted/rolled back.** The guard has trapped the feed in the degraded state and now actively rejects the fix. (The zero-row row is benign by luck: `rows_written=0` → `prior_rows` falsy → `if prior_rows:` false → Δ-guard *skipped*. The partial is not benign — it is a hard lockout.)

*Why it matters.* This converts a one-day outage into a multi-day one and makes the recovery path require a human to manually delete/ignore the poisoned ledger row — the opposite of "fail safe, unattended." It is a direct consequence of using `status='success'` as the baseline predicate without a floor.

*Fix.* Floor-guard the baseline selection so a known-degraded success row can never become the comparison point: add `AND rows_written >= POCS_ROW_FLOOR` to the `_prior_success_rows()` query (shown in #1's fix). The floor (6.0M) is below every healthy build (~8.06M) and above the partial only if the partial cleared it — so set the baseline floor slightly higher than `POCS_ROW_FLOOR` if you want the partial excluded as a baseline even though it cleared the abort floor. Cleanest: introduce `BASELINE_MIN_ROWS = 7_000_000` (clearly-healthy) for the baseline query only, distinct from the abort floor. That makes the baseline immune to any sub-7M degraded success.

---

### [BLOCKER] 3 — Scratch validation pollutes the production ledger and corrupts the next cron's Δ-guard baseline

*Problem.* The plan's entire negative-path proof (§6A, §6B-1, §6B-2) runs `modal run … sam_pocs.py` with `SAM_POCS_LANCE_URI=$SCRATCH`. But `_record_run` writes `feed=FEED` (`'sam_pocs'`, a module constant) and `dataset_uri=DATASET_URI`. The URI override flows into `dataset_uri`, but `feed` stays `'sam_pocs'`, and the plan's `_prior_success_rows()` query filters on `feed`/`status` only — **no `dataset_uri` predicate**. So:
  - 6A (scratch happy path) inserts a `success` row → it is the newest success → next prod cron's Δ-guard baseline is a *scratch* number.
  - 6B-2 (post-write rollback test) still reaches `status='success'`? No — it raises, so it records `error`. Fine. But 6A and any clean scratch rerun record `success`.
  - Worse: if scratch source ≠ prod source size, or scratch is run on a different day's `entity_registrations`, the baseline drifts and a *real* prod build can trip the Δ-guard against a scratch row.

This is precisely the Directive-D leak. The plan asserts blast-radius containment via the URI override but never isolates the *ledger*, which is global.

*Why it matters.* Validation is supposed to be the safety net; here it reaches into prod state. A reviewer running 6A the evening before the 16:30 cron can silently arm a false-FAIL (or false-PASS) for the next unattended run.

*Fix (two parts, both required):*
1. **Tag scratch runs distinctly.** Derive `feed` from the URI so scratch never shares the prod feed label:
   ```python
   FEED = "sam_pocs" if DATASET_URI == "s3://data-sink/active/sam_pocs/" else "sam_pocs_scratch"
   ```
   (or read an explicit `SAM_POCS_FEED` env override). This keeps the ledger's `feed` column honest and is the durable fix.
2. **Filter the baseline by `dataset_uri`** (already shown in #1's query: `AND dataset_uri = %s`, bound to `DATASET_URI`). Belt-and-suspenders: even if `feed` tagging is missed, a prod build only ever baselines off prod-URI successes.

Add an acceptance line: *after the scratch harness, `SELECT count(*) FROM ops.sam_pocs_runs WHERE dataset_uri LIKE '%scratch%'` is the only place scratch rows appear, and the prod-URI baseline query returns the pre-existing prod success.*

---

### [MAJOR] 4 — Gate 11 (KIPPER must have a POC) can false-FAIL and roll back every good build

*Problem.* Gate 11 asserts `KIPPER_UEI` resolves to ≥1 POC row carrying a `name_key`. `KIPPER_UEI = "DD1BCRF2QQG8"` is proven to exist as an *entity* (`sam_master.py:306` round-trips it as an entity) — but **POCs are a different population**. `sam_pocs`'s terminal `WHERE first_name IS NOT NULL OR last_name IS NOT NULL` drops every empty slot; an entity with all six POC name-slots blank produces **zero** POC rows and zero `name_key`. If KIPPER is such an entity (or its POC slots get scrubbed in a future SAM extract), gate 11 raises on a *correct* build and triggers `restore()` — rolling back 8M good rows daily. This cannot be verified locally (`pylance`/`duckdb` not importable per the brief), so it is a live risk, not a confirmed pass.

*Why it matters.* A post-write gate that false-FAILs doesn't just abort — it *rolls back*, and on a daily unattended feed it would silently revert every run to the pre-cutover version until someone notices the dataset is frozen in time.

*Fix.* Do not pin the round-trip to a single hardcoded UEI for the POC layer. Either (a) **verify-in-execution first**: during 6A, confirm `KIPPER_UEI` yields ≥1 POC with a non-null `name_key`; if it does not, pick a different anchor that does and is stable; **or** (b) make gate 11 *population-based* rather than *identity-based* — assert that the dataset round-trips *some* high-fill anchor, e.g.:
```python
# pick the most-populated uei as the probe — guaranteed to have a POC by construction
top = ds.scanner(columns=["uei"], filter="uei IS NOT NULL", limit=1,
                 ).to_table()  # or precompute argmax(count) in _materialize and pass it through
```
Cleanest: have `_materialize` emit `probe_uei` (a UEI known to have ≥1 POC, e.g. `arg_max(uei, poc_count)` or simply the first non-null-`name_key` row) into `metrics`, and gate the round-trip on *that* — it is non-null by construction, eliminating the false-FAIL entirely while still proving the index works. Keep KIPPER as a secondary informational log, not a hard gate.

---

### [MAJOR] 5 — Gate 7 (`distinct_poc_type == 6`) false-FAILs whenever a slot legitimately empties fleet-wide

*Problem.* Gate 7 hard-asserts exactly 6 distinct `poc_type` values and zero NULLs. `poc_type` is a CASE over `slot_no ∈ {1..6}` (`build_pocs_sql`, `poc_type_case`), so `null_poc_type == 0` is structurally guaranteed (every slot 1–6 maps to a label) — that half is a tautology and harmless. But `distinct_poc_type == 6` is **not** guaranteed: it requires that *at least one entity in the entire universe* populates each of the six slots after the empty-slot `WHERE` filter. Slots 2/3/4/6 are the optional alternates (docstring: "near-always populated" applies only to 1 & 5). If a SAM extract ever ships with, say, zero populated `past_performance_alt` slots across all 1.5M entities, `distinct_poc_type` = 5 and gate 7 false-FAILs → rollback.

*Why it matters.* Same rollback blast radius as #4, triggered by a benign distributional shift rather than a bug. Low probability today, but it is an *equality* gate on an *optional* population — fragile by construction.

*Fix.* Split the assertion: keep the cheap tautology check (`null_poc_type == 0`) as a hard gate (it proves the CASE didn't regress), but downgrade the slot-count check to assert the **mandatory** slots are present and merely *log* the alternates:
```python
gate(metrics["null_poc_type"] == 0, "7a no null poc_type")
gate({"government_business", "electronic_business"}.issubset(present_poc_types),
     "7b mandatory slots present")   # present_poc_types from a GROUP BY in _materialize
# alternates: informational only — checks.append(f"INFO slots present: {sorted(present_poc_types)}")
```
This still catches the failure that matters (a mandatory slot vanishing ⇒ real corruption) without rolling back on an empty optional slot. Requires `_materialize` to surface the set of present `poc_type`s (one extra cheap aggregate, same scan).

---

### [MAJOR] 6 — `name_present_frac ≥ 0.999` (gate 5) is a near-tautology and provides almost no protection; the plan over-credits it

*Problem.* `name_key = upper(full_name)`, `full_name = trim(concat_ws(' ', first, middle, last))`, and the terminal `WHERE first_name IS NOT NULL OR last_name IS NOT NULL` guarantees at least one of first/last is present on every emitted row. Therefore `full_name` is non-empty and `name_key` non-null on ~100% of rows **by construction** — gate 5 can essentially never fire. The plan acknowledges this ("≈1.0 by construction," "defense-in-depth") but then lists it in §5 as a real gate. It is dead weight, and more importantly it gives false comfort: a reviewer may believe "name fill is gated" when in fact *name correctness* is not gated at all (see #7).

*Why it matters.* Not a correctness bug — a **false sense of coverage**. The directive-C concern (positional-offset regression yielding wrong-but-non-null names) sails straight past gate 5: a shifted offset still produces non-null strings, so fill stays ~1.0.

*Fix.* Keep gate 5 as a cheap invariant tripwire (it catches a future SQL edit that breaks the WHERE), but **do not count it toward content safety**, and add the missing content-plausibility check described in #7. Re-label it in §5 as "invariant tripwire (not a content check)."

---

### [MAJOR] 7 — No gate asserts name *content* plausibility; a positional-offset regression (the exact failure the docstring fears) passes every count/fill/slot gate

*Problem.* This is directive C, and it is a genuine gap. A `pipe_fields` base-offset error (`b` computed as 47 vs 45, or a stride miscount) shifts which cell maps to `first_name`/`last_name` — yielding fully-populated, non-null, plausibly-shaped rows with the *wrong* values (e.g. a ZIP in `first_name`, a state code in `last_name`). Every existing/planned gate passes: row count unchanged, distinct_uei unchanged, both families nonzero, name fill ~1.0, six slots present, Δ-guard unmoved (counts identical — only *content* shifted). The plan's only nod to this is the `verify_sam_pocs` `sample` (§verify) — but that's a 6-row eyeball, not a gate, and runs *after* the write, gating nothing. The Δ-guard is explicitly **not** a defense here (the directive's own hypothesis), confirmed: a pure offset shift moves no counts.

*Why it matters.* This is the highest-severity *silent corruption* class for this feed and the one its docstring is most worried about. It would push millions of malformed contact names into the GTM gateway with zero gate firing and a green ledger.

*Fix.* Add a cheap, content-shape gate computed in the same `_materialize` aggregate (no extra scan). Names and address/zip fields have disjoint character profiles; assert the populations don't bleed:
```python
# fraction of name cells that look like names (alpha-dominant), and the inverse for zip5
name_alpha = count(*) FILTER (WHERE first_name IS NOT NULL
                AND regexp_matches(first_name, '[A-Za-z]')) / nullif(first_name_present,0)
zip_numeric = count(*) FILTER (WHERE zip5 IS NOT NULL
                AND regexp_matches(zip5, '^[0-9]{3,5}$')) / nullif(zip5_present,0)
```
Gate `name_alpha ≥ 0.95` and `zip_numeric ≥ 0.95` (baseline both from a clean 6A run, then set the floor a few points below observed). A positional shift inverts these (zip digits land in `first_name`, names land in `zip5`) → the gate fires *before the write*. This is the only proposed check that actually closes the directive-C hole, and it is pure-SQL, single-scan, and unit-testable in the same `assert_pre_write_gates`.

---

### [MINOR] 8 — `tests/test_sam_pocs_gates.py` location is unverified; no `tests/` dir exists in the repo

*Problem.* §6C and §7 step 1 reference `tests/test_sam_pocs_gates.py`, but there is no `tests/` directory at the repo root (verified). The plan hedges with "(or repo's test location)" but does not name the convention.

*Why it matters.* `git add tests/test_sam_pocs_gates.py` (§7.1) fails if the path is wrong; the unit-test acceptance criterion (§9) then can't be satisfied as written, stalling the merge on a triviality.

*Fix.* Before writing the test, run `git ls-files '*test*'` to find where this fleet keeps tests (or confirm there is no harness and the test must establish `tests/`). If none exists, the plan should explicitly state it is creating `tests/` and that there is no runner wired — and either add a one-line `pytest` invocation to the DoD or downgrade the unit test to a `python -m pytest tests/test_sam_pocs_gates.py` smoke step the executor runs locally. Make the path concrete.

---

### [MINOR] 9 — Gate 9 (`committed == materialized`) is the only line truly guaranteed to hold, yet is positioned as a corruption catch; the real silent-truncation risk is upstream

*Problem.* Gate 9 compares Lance `count_rows()` to `metrics["rows"]`. The Arrow table written *is* the table counted; barring a Lance write bug, these always match. It catches a Lance-side write/commit fault (worth having) but does **not** catch a 30% silent truncation that happened *during materialization* — that truncation already lives in `metrics["rows"]`, so committed == materialized still passes. The directive's "is the floor low enough that a 30% silent truncation passes?" — answer: a 30% drop from 8.06M = 5.65M, which is **below** the 6.0M floor → gate 1 catches it. Good. But a 25% drop = 6.05M ≈ at the floor and inside the (current scalar) Δ-guard → marginal. The per-family Δ-guard from #1 is what de-risks the 20–25% band.

*Why it matters.* Mostly framing, but the marginal 20–25% truncation band is real and only #1's fix closes it. Worth stating plainly so no one believes gate 9 is a truncation defense.

*Fix.* No code change beyond #1. Re-document gate 9 in §5 as "write-integrity (Lance commit) check," not a content/truncation check, so the coverage map is honest.

---

### [MINOR] 10 — `name_key` seek probe escaping is correct, but the probe value can be empty/degenerate and make gate 12 meaningless

*Problem.* Gate 12 picks `probe = next(r["name_key"] for r in kip if r["name_key"])` and `.replace("'", "''")`. The escaping is correct (mirrors the sibling). But it is sourced from the KIPPER rows; if #4's risk materializes (KIPPER has no POC), the `next(...)` raises `StopIteration` *inside the post-write try*, which is caught as `gate_exc` → rollback. So #4 also takes out gate 12. Independently: if the probe `name_key` is a very common value, the seek returns many rows and the timing is still valid; if it's degenerate (single char), still fine. The escaping itself is not the risk — the *source* of the probe is (same root cause as #4).

*Why it matters.* Compounds #4; not independently severe.

*Fix.* Resolve via #4 (population-based probe). Once the probe UEI is guaranteed to have a POC with a non-null `name_key`, gate 12's probe is always well-formed. Optionally guard the generator with a default to convert a `StopIteration` into an explicit gate message rather than an opaque rollback.

---

### [MINOR] 11 — `restore()` reverts index state, but the failure modes of `restore()` itself and of partial `create_scalar_index` are unhandled

*Problem.* Three sub-cases the plan asserts away:
  1. *Does `restore()` revert indices?* Yes — Lance indices are manifest-referenced; restoring version N restores N's manifest and thus N's index set atomically. The plan's claim is correct. **Keep.**
  2. *What if `restore()` itself throws?* The plan's block calls `restore()` inside the `except gate_exc` handler and then `raise`. If `restore()` throws (transient R2), the original `gate_exc` is lost and the dataset is left on the bad version with no second attempt. No retry, no preservation of the rollback target in the ledger.
  3. *Partial `create_scalar_index`* (some BTREEs built, then OOM/throw on the high-cardinality `name_key`): the exception propagates out of the write block into the outer `except exc` → ledger records `error` → **but no rollback runs**, because the failure happened *before* the post-write try. The half-indexed overwrite is now live (the data is the new data; only some indices exist). Gate 10 would have caught missing indices *if reached*, but an exception during indexing never reaches the post-write gates.

*Why it matters.* (3) is the real one: an indexing OOM leaves a fully-overwritten-but-under-indexed prod dataset live, recorded as `error`, with the GTM gateway now doing full scans on `name_key`. The plan's exposure-window claim ("bounded to gate 9–12 eval") under-counts this path.

*Fix.* Move `v_before` capture and the rollback responsibility to wrap **the write *and* the indexing**, not just the post-write gates. Restructure so any exception after `write_dataset` (including mid-index) triggers the same `restore(v_before)`:
```python
try:
    lance.write_dataset(...); ds = lance.dataset(...)
    for col in BTREE_INDEXES: ds.create_scalar_index(col, "BTREE")
    for col in BITMAP_INDEXES: ds.create_scalar_index(col, "BITMAP")
    # ... post-write gates 9-12 ...
except Exception as gate_exc:
    if v_before is not None:
        try:
            lance.dataset(DATASET_URI, storage_options=so, version=v_before).restore()
        except Exception as restore_exc:
            raise RuntimeError(f"ROLLBACK FAILED to v{v_before}: {restore_exc}; original: {gate_exc}")
        raise RuntimeError(f"write/index/gate failed → rolled back to v{v_before}: {gate_exc}")
    raise RuntimeError(f"failed on net-new dataset (inspect/drop {DATASET_URI}): {gate_exc}")
```
This closes the mid-index hole and makes a failed rollback loud instead of silent. (The sibling has the same latent gap; this is a genuine improvement over the reference, not just parity — worth noting in the PR.)

---

### [MINOR] 12 — `EXPECTED_POC_TYPES`/`distinct_poc_type` exposure window and the GTM consumer overlap (directive E)

*Problem.* The exposure window for the GTM/MCP gateway between `write_dataset` (overwrite commits new version, becomes default) and the post-write gate eval is the index-build + gate duration — for `sam_pocs` that includes building **four** BTREEs (uei, cage_code, name_key, last_name) over ~8M rows plus two BITMAPs. `name_key`/`last_name` are high-cardinality string BTREEs; on R2 this is not "seconds." During that window the gateway reads a committed-but-not-yet-validated (and not-yet-fully-indexed) dataset. The plan's §8 "exposure window bounded to gate 9–12 (seconds)" undercounts the index-build time, which is the dominant term.

*Why it matters.* For a daily feed this is tolerable (the plan's risk acceptance is reasonable), but the stated bound is wrong and the *indexing* portion is exactly when point-lookups degrade. Honesty in §8 matters because it informs the strategic call in #13.

*Fix.* Either (a) correct §8 to "exposure = write-commit → all indices built → gates pass; on the order of minutes for the high-cardinality BTREEs, acceptable for a daily feed," or (b) adopt the staging/promote architecture (#13) to drop the destructive window to zero. At minimum, fix the documentation so the window isn't undersold.

---

## 4. Strategic recommendations

**S1 — Move to validated staging + atomic promote; eliminate the destructive window (directive F).** The strongest structural improvement, and it is *cheaper* than it looks because Lance gives it nearly for free. Today the sequence is `overwrite prod → index prod → gate prod → maybe rollback`. The fail-safe version writes to a **staging URI**, indexes and gates there, and only on PASS promotes. With Lance, "promote" need not be a physical copy: write the new version to a staging dataset, gate it, then the cheapest correct swap is to write to the *same* prod dataset only after the staging gates pass (i.e. materialize once, gate the Arrow table pre-write — which the plan already does — *and* gate a staging-written copy, then do the prod overwrite last with high confidence). The pre-write gates already catch the materialization-stage failures (#1, #7, truncation); the residual risk the post-write gates cover is *Lance write/index integrity*, which is low. **Net recommendation:** the pre-write gate suite (with #1 and #7 added) captures ~80% of the safety at zero exposure window; the post-write gates + rollback are lower marginal value but cheap to keep. Do **not** block this PR on staging/promote — but flag it as the next cycle, because rollback-after-overwrite is strictly inferior to never-overwrite-a-bad-build, and the directive's instinct here is correct.

**S2 — There is NO alerting; the feed is "fail-safe" but "fail-silent-to-humans" (directive F).** Every finding protects the *data*. None pages a *human*. On a rollback or `error`, the Trigger task throws (`sam_pocs.ts:69`), which surfaces in the Trigger dashboard — but nobody is watching a dashboard at 16:30 UTC. A degraded-but-passing build (e.g. a 1.35M distinct_uei drop that clears gate 2's 1.3M floor but is clearly anomalous) emits **nothing**. Add a cheap alert: on `status='error'` *or* on any Δ-guard line that lands in the outer half of tolerance (a "warn band"), POST a Telegram/Slack/webhook from `_record_run` or have a tiny Trigger consumer watch `ops.sam_pocs_runs`. This is higher-leverage than another gate: the gates protect the dataset; alerting protects the *operator's awareness* that protection fired. Cheapest possible version: a 3-line `requests.post(ALERT_WEBHOOK, ...)` in the `if status != 'success'` branch.

**S3 — Is hardening the right #1 priority?** Yes. This is the *only* daily, unattended, destructive-overwrite SAM feed (the masters/sidecars are `[MANUAL]`), and the ledger proves it has already silently shipped a zero-row build and a 30%-partial build to a GTM-serving dataset. The blast radius (live gateway) and the demonstrated recurrence make this correctly P0 over the other flagged cycles (`sam_master_contacts` retirement, freshness automation, person-bridge). Keep the priority.

**S4 — Re-baseline policy (plan §10.2) should be promoted, not deferred.** The absolute floors (6.0M / 1.3M / 0.9M) are set ~25%/16%/23% below today's live values. SAM grows monthly; in ~6–9 months the live values rise and the *headroom shrinks asymmetrically* — but more pressingly, a *shrink* event (SAM purges inactive registrations, which happens) could legitimately drop the universe toward a floor and cause a false-FAIL on a correct build. The per-family Δ-guard (#1) auto-tracks growth/shrink within ±25% and is the right primary; the absolute floors should be explicitly framed as "catastrophic-collapse catchers, re-baselined on any ±20% universe shift," and a `SELECT max(rows_written)` sanity line added to the runbook. Minor, but it prevents the guard from aging into a liability.

---

## 5. Amended acceptance criteria (delta to the plan's DoD / §9)

Add / change the following (everything else in §1 and §9 stands):

1. **(was DoD #3 / gate 8)** Δ-guard is **per-family**: gates assert `rows_written`, `poc_rows_v2`, and `distinct_uei` each within ±25% of the prior *floor-qualified, prod-URI* success. The 6,389,167 partial (label `20260503`, ledger 2026-06-02) must FAIL the new build's pre-write gates in a regression fixture. *(closes #1)*

2. **(new)** `_prior_success_rows()` selects the baseline with `AND rows_written >= BASELINE_MIN_ROWS AND dataset_uri = DATASET_URI`. Unit test: a seeded ledger containing the 6.39M partial as the latest success does **not** become the baseline; the prior healthy 8.06M row does. *(closes #2, #3)*

3. **(new)** Scratch runs are ledger-isolated: `feed` resolves to `sam_pocs_scratch` for any non-prod URI. Acceptance: after the full §6 harness, `SELECT DISTINCT feed, dataset_uri FROM ops.sam_pocs_runs WHERE recorded_at > <harness_start>` shows scratch rows only under `feed='sam_pocs_scratch'`, and the prod baseline query is unaffected. *(closes #3)*

4. **(amends gate 11)** The post-write round-trip gate uses a probe UEI **proven (in 6A) to have ≥1 POC with non-null `name_key`** — not an unverified hardcoded constant. If `KIPPER_UEI` is confirmed to have a POC in 6A, it may stay; otherwise `_materialize` emits a population-derived `probe_uei`. Document the 6A confirmation as evidence. *(closes #4, #10)*

5. **(amends gate 7)** Slot integrity asserts `null_poc_type == 0` (hard) and the two **mandatory** slots present (hard); the four alternates are logged, not gated. *(closes #5)*

6. **(new — content gate)** A pre-write **name-plausibility** gate is added and unit-tested: `name_alpha_frac ≥ floor` and `zip_numeric_frac ≥ floor`, baselined from a clean 6A run. The unit test includes a synthetic "offset-shifted" metrics row (names numeric, zip alpha) that MUST raise. This is the directive-C defense and is **required** for DoD. *(closes #6, #7)*

7. **(amends rollback)** `v_before` capture and `restore()` wrap the **write + indexing + post-write gates** (not just the gates); a `restore()` failure raises a distinct, loud `ROLLBACK FAILED` error preserving the original cause; a mid-index exception triggers rollback. Negative test 6B adds a third case: force a failure *between* index builds and assert rollback to `v_before`. *(closes #11)*

8. **(amends §8 docs)** The exposure-window statement is corrected to include index-build time (minutes for the high-cardinality BTREEs), and the pre-cutover prod version number is recorded in the PR body *before* 6D. *(closes #12)*

9. **(new — alerting, may be a fast-follow but must be tracked)** A rollback or `error` terminal state emits a human-visible alert (webhook/Telegram/Slack). If deferred, it is filed as an explicit, linked follow-up — not dropped. *(S2)*

10. **(test path)** The unit-test file path is confirmed against the repo's existing test convention (`git ls-files '*test*'`) before §7.1; if `tests/` is being created fresh, the DoD states the runner invocation. *(closes #8)*

---

### Net

Port the sibling pattern — that decision is right and most of the borrowed machinery is genuinely de-risked by `sam_normalized_entities` already running it. But the plan as written would (a) wave through the precise partial-build regression its ledger already recorded, (b) trap the feed in a degraded baseline that rejects the recovery, (c) write validation noise into the prod ledger, and (d) leave the docstring's own positional-offset fear ungated. Fixes for all four are surgical, pure-SQL/single-scan where they touch compute, and unit-testable. Land the three blockers + the four majors, keep the post-write rollback (with #11's wrap fix as a real improvement over the reference), and tee up staging/promote (S1) and alerting (S2) as the next cycle.
