# Adversarial Review — `SOS_NORMALIZED_MASTER_REMEDIATION_PLAN.md`

**Reviewer posture:** adversarial principal data-engineering review. Every claim was
cross-checked against the live R2 system-of-record (read-only probes via `/tmp/sosdiag_venv` +
Doppler `core-x/prd`) and against `origin/main` source (`git show origin/main:<path>`).

---

## Provenance & reconciliation (read this first)

This review was reconciled across a **one-merge version skew** and corrected accordingly:

- **Live data probes** ran against **post-#182 R2** (master `v9`, FL `v10`).
- **Initial on-disk code reads** came from this worktree at **commit `1524a1f` (PR #181) —
  exactly one merge behind production.** **PR #182** (`dd5409b`, *"fix(sos): re-materialize
  stale normalized_legal_name + add legal_name_base & FL agent BTREE"*) merged after it.
- **`git show --stat dd5409b` confirms #182 changed ONLY `pipelines/fl_sos/sunbiz.py`** (plus
  the diagnostic doc). The master re-materialization in #182 was a **Modal `run_normalize`
  job — it writes R2 and produces NO git diff.** That is the entire source of the skew:
  post-#182 *data* (R2 = v9) vs pre-#182 *code* (worktree = #181 files).

All findings below are stated against **post-#182 `origin/main`** (re-verified, not the stale
worktree). The two findings that were artifacts of the skew (S1, S6/ex-M3) are corrected in place.

- **Cascade correction (parent-agent reconciliation pass).** The reviewer's first cascade pass
  concluded the bridge was "key-idempotent, no gap." A parent verification pass **disproved that**
  against the master version timeline (`m.versions()`) + the bridge join code
  (`materialize_epa.py:688/807`) + a sizing scan. **M4 below is the corrected finding: there is a
  real conjunction/dash recall gap.** The mechanism the first pass inferred ("the master already
  carried new-rule keys when the bridge built") is false — the master was provably v4/old-macro
  on the bridge's build date.

---

## Verdict

**VERIFIED COMPLETE IN PRODUCTION — the plan is SUPERSEDED-BY-EXECUTION. It worked.**

The plan was **accurate when authored** (the master was genuinely v4-stale: an old-macro
`normalized_legal_name`, no `legal_name_base` column, an unindexed FL `registered_agent_name`).
It was then **executed** — an operator-started agent produced **#182**, which re-materialized
the master and committed the FL index edit. Live ground truth confirms both defects are closed:
master is **v9, 17,926,543 rows, 12 cols including `legal_name_base` (BTREE-trained,
0 unindexed)**, CA/NY/FL exact-transform drift is **0.000%**, and `fl_sos_corporations` is
**v10 with a trained `registered_agent_name` BTREE**. **Do not re-run Task 1** — the plan's own
§1.3 pre-flight gate ("STOP if version > 4") correctly enforces this; an in-place overwrite now
would reproduce an already-correct SoR for zero delta while incurring real risk (M2, S3).

The **forward value of this review** is three things #182 did *not* address:
1. **M2 / S2 — pipeline hardening:** `_build_master_indexes` still `WARN`-continues on an index
   miss (confirmed on post-#182 main), so the *next* master rebuild has a partial-failure window
   that can leave the live SoR with data and no/partial BTREE.
2. **M4 — cascade (corrected):** #182 rebuilt the master but touched no downstream consumer.
   The master keys flipped **v4(old)→v9(new) at 2026-06-05 23:40**, *after* the committed
   `epa_to_sos_bridge` (and likely other consumers) built — so those consumers exact-joined the
   **old-macro** master and carry a real **conjunction/dash recall gap** (≈**1,438,606** SoS
   entities, 8.03%, were under-matchable at v4). **Should-do:** re-run `build_bridge` (and any
   consumer materialized before the flip) against v9 to recover the missing `&`/dash matches;
   existing matches stay valid (no breakage). Plus a standing cascade rule for future rebuilds.
3. **S4 / S5 — verifier:** commit it to the repo, run it in-region, and add row-count + CO-scrub
   + join-loss assertions it currently lacks.

Risk of the *plan as a forward artifact*: low, provided Task 1 is not re-run. The remediation
itself is done and correct.

---

## Must-fix (blocking) — for the NEXT rebuild, not a re-run of this one

### M2. `normalize()` commits the overwrite, THEN builds indexes in the same call; an index miss `WARN`s instead of raising — a crash in between leaves the live master with data and NO/partial BTREE. *(VALID — confirmed on post-#182 main.)*
**What's wrong.** `git show origin/main:pipelines/sos_normalized/normalize.py` confirms the
defect is live: `_build_master_indexes`'s docstring literally reads *"An index miss must not
fail the build,"* and each `create_scalar_index` is wrapped `try/except: print(WARN…)`. Trace
`normalize()` (L519–531):

```python
lance.write_dataset(result, MASTER_URI, mode="overwrite", ...)   # data committed here (v_new)
del result
built = _build_master_indexes(so)                                 # indexes built AFTER, may WARN-skip
status = "success"
```

Two degraded end-states are reachable, **neither raises** in a way the plan's gate catches:
1. **Container dies (OOM/timeout) after the data commit, before/within index builds** → the
   live master is the new version **with no scalar indexes** (or a partial subset). Every
   `normalized_legal_name` / `legal_name_base` lookup silently degrades from `ScalarIndexQuery`
   (1 row) to a **17.9M-row full scan** — strictly worse than the state being "fixed." An
   OOM-killed container never reaches `finally`, so even the `_record_run("error")` audit row is
   not written.
2. **Indexes individually `WARN`-fail but the function returns `success`** with a `built` list
   missing entries.

**Why it bites.** A degraded-but-not-broken SoR: data present and queryable (nothing 500s), it
just full-scans. The plan's "Lance is versioned" rollback only helps *after* you notice, and
notice is hard with no error. This is the highest-severity operational risk for any future
rebuild.

**Fix (S2 / Patch D).** Make `_build_master_indexes` **raise** on any miss (collect failures,
`raise` if non-empty) so `status="success"` can never coexist with a missing index. Add to the
runbook: "if the container dies or `indexes` omits any of the four, the master is index-degraded
— immediately `modal run …::reindex` (the existing reindex-only path) before any reader relies
on it." The plan never names `reindex()` (L548) as the partial-failure remedy; it should.

---

### M4. #182 rebuilt the master but touched NO downstream consumer — re-verified against the version timeline + join code: there IS a real conjunction/dash recall gap, because consumers exact-joined the OLD-macro master before the v9 flip. A `build_bridge` re-run recovers it.
**What I re-verified (read-only, two independent angles).** Consumers that compute
`core.name_norm` and exact-join the master key (grep-confirmed): `epa_to_sos_bridge`
(`pipelines/ingest_epa/materialize_epa.py`), `fl_federal_tax_liens`, `sam_normalized_entities`,
`recon_ca_ucc_sos`, `credit_spine_normalize_index`, `gleif`, `crosswalk_hmda_gleif`. #182's diff
touched none of them.

**(1) The master keys flipped AFTER the consumers built.**
```
sos_normalized_master:  v1–v4 @ 2026-06-01 19:47–19:49  (original, OLD-macro: '&' dropped, dash glued)
                        v5–v9 @ 2026-06-05 23:38–23:40  (the #182 rebuild, NEW-macro: '&'→' AND ', dash→space)
epa_to_sos_bridge:      v3    @ 2026-06-02 23:04         (built 3 days BEFORE the flip → against v4/OLD-macro)
```
The diagnostic independently confirmed v4 was old-macro (8% drift). So on 2026-06-02 the master
the bridge joined was **old-macro** — the keys were *not* yet fixed.

**(2) The bridge join is coupled to the master's STORED key (not self-normalized on both sides).**
`materialize_epa.py` computes the EPA side with the current macro (`nn = name_norm("raw_name")`,
L763) and joins it against the master's **stored** `normalized_legal_name`
(`SELECT normalized_legal_name AS nln … FROM sos_rdr`, L807; tiers `exact_name` /
`exact_name_state` / `base_name_zip` at L688 / L684 / L680). At v4 the SoS side was old-macro, so
an EPA `"X AND Y"` (from `"X & Y"`) could **not** exact-match a v4 SoS `"X Y"` (& dropped). Those
EPA facilities were under-matched and **never written to the bridge.**

**Why the 5,994 ` AND ` bridge rows are NOT evidence of "no gap"** (this is exactly where the
first pass went wrong): they are entities whose **raw SoS name spells "AND" as a word** —
old-macro preserves the word "AND", so they matched at v4 under *both* macros. Likewise the 2,782
`&`-in-`epa_matched_name` rows matched SoS entities spelled with the word "AND". The genuine gap
is EPA facilities matching SoS entities whose name uses the **`&` symbol or a dash** — those are
**invisible from the bridge** because the unmatched rows aren't stored. The bridge being 100%
matched is tautological: it only *stores* matches.

**Size of the under-matchable SoS population (live v9 scan):**
```
SoS entities whose raw name contains '&'   : 996,337 rows  (934,640 distinct keys)
              … or an inter-word dash      : +455,396 rows
total affected                             : 1,438,606 rows  (8.03% of the master)  ← matches the diagnostic's 8.04%
```
Every one of these had a key that *differed* between old-macro (v4) and new-macro (v9). A
current-macro joiner against v4 missed all of them. The recoverable bridge matches are the
EPA∩(these entities) subset — unsized without the re-run, but the SoS-side population is ~1.44M
entities, so the gap is **real and material, not zero.** The bridge's existing 356,899 matches
remain valid (`sos_original_entity_id` is stable across rebuilds), so this is recall recovery,
not corruption repair.

**Conclusion (corrected).** The cascade is **not** "key-idempotent / freshness-only." The bridge
— and any consumer materialized before **2026-06-05 23:40** — exact-joined the old-macro master
and therefore carries a conjunction/dash **recall gap**. Severity: **should-do, not urgent** (no
breakage; existing matches valid; the exact recovered-match count is known only after the re-run).

**What's worth doing (prioritized):**
1. **Should-do — re-run `build_bridge` against v9** to recover the `&`/dash EPA→SoS matches the
   v4 keys couldn't satisfy (this is recall recovery, **not** mere freshness). Diff `by_tier` /
   `by_confidence` before/after to quantify the recovered matches. Apply the same to any other
   consumer whose output predates 2026-06-05 23:40 (check each consumer's manifest timestamp).
2. **Standing cascade rule** (documentation): "A change to the master's `normalized_legal_name` /
   `legal_name_base` **values** requires re-materializing every consumer that *stores* a join
   result — `epa_to_sos_bridge` is a frozen materialization, not a live view — *after* the master
   commits, in dependency order, then re-verify confidence/tier distributions." #182 shipped the
   master without this step, which is exactly why the gap exists now.
3. **Cleanup:** `materialize_epa.py` L801 hardcodes *"the LIVE sos_normalized_master does not
   persist a legal_name_base column"* — now **false** (v9 persists it). The code still works (it
   re-derives the same value via the same macro), so no break, but the stale comment misleads and
   the spine-side re-derivation is now redundant (it could read the master's column directly).

---

## Should-fix (strong)

### S1. *(RESOLVED by #182 — corrected from "open.")* FL `registered_agent_name` is durable on main; the plan's `reindex` step was correctly never needed.
My initial read showed `INDEX_PLAN["master"]["btree"]` as a 2-element list
`["document_number","corporate_name"]` — **because that read came from the pre-#182 worktree
(#181).** Post-#182 ground truth:

```
git show origin/main:pipelines/fl_sos/sunbiz.py  →  L142:
"master": {"btree": ["document_number", "corporate_name", "registered_agent_name"], ...}
```

The durability edit **is committed on main** (the next FL re-ingest will rebuild the agent
index from `INDEX_PLAN`, so it survives an overwrite), and the live FL v10 already carries the
trained index. **No action.** The plan's instruction to run `modal run …::reindex --target
master` (§3.2) was unnecessary blast radius (it rebuilds all four healthy FL indexes with
`replace=True`) and is moot now — the edit alone, already merged, is the correct and complete
fix. Net: Task 2 is **fully done, code + data.**

### S2. The in-place overwrite pattern is the wrong rollout shape for a 17.9M-row SoR — shadow-build + atomic swap eliminates the M2 window (for the NEXT rebuild).
The pattern the plan endorses (in-place `mode="overwrite"` then index in the same call) is the
root cause of M2. For any future genuine rebuild: write to a **side dataset**
(`s3://data-sink/active/sos_normalized_master__build/`), build + **fully verify** all indexes
there, then atomically repoint — `SOS_NORMALIZED_MASTER_URI` already exists as the indirection
point (`normalize.py` L51), flip it only after verification. The live master is then **never** in
a degraded-index state, and rollback is "don't flip the pointer" rather than "restore a manifest
after bad data is already serving." Pair with making `_build_master_indexes` raise (Patch D).

### S3. Concurrency: nothing guards a scheduled/dispatcher `sos-normalized` run firing mid-rebuild.
`normalize()` is reachable from the Universal Dispatcher and any scheduled trigger. Two writers
doing `mode="overwrite"` on the same `MASTER_URI` can interleave a data commit from run A with an
index build from run B against a different data version. Lance commits are atomic per-operation
but there is **no lease/advisory lock.** For any real run: (a) confirm no scheduled
`sos-normalized` job is enabled, and (b) state that the manual `run_normalize` must be the sole
writer for its duration. Low probability, high-confusion blast radius; cheap to gate.

### S4. Commit the verifier to the repo and run it in-region.
The §7 verifier streams **two full 17.9M-row column scans** from a laptop-side, non-in-region
client across R2. An in-region Modal `verify` function already exists (`normalize.py` L555) and
should be **extended** to carry these assertions and run next to the data. A verifier this
load-bearing must be **committed** (e.g. `pipelines/sos_normalized/verify_remediation.py`), not
pasted into `/tmp` per the appendix — a `/tmp` script is unversioned and silently drifts from
`core.name_norm`. Pin the verifier to the **same** DuckDB the image resolves (image is
`duckdb>=1.5,<2`; verifier venv got 1.5.3) so a regex-engine difference can't make the drift
comparison disagree with what the pipeline wrote.

### S5. The verifier doesn't check the three things that matter most: row-count conservation, the CO scrub (positively), and the headline join-loss metric.
- **Row-count conservation** v(before)→v(after): a rebuild could silently drop/duplicate rows
  (e.g. a partially-loaded spine) and every drift check would still pass at 0% on the survivors.
  Add `assert after.count_rows() == before.count_rows()` (or an explained delta) and assert
  per-state counts match `per_state` from the run JSON.
- **CO scrub, verified not exempted (Patch C, see S6).**
- **The actual deliverable** the diagnostic leads with — distinct current-macro keys with no
  stored match (`1,367,567 → ~0`): the verifier checks per-row drift but never re-computes the
  **distinct-key join-loss** that is the stated business impact. Add the `diag6` join-loss query
  and assert it collapses to 0 for CA/NY/FL.

### S6. *(Reframed from M3 — corrected for v4-vs-v9 version confusion.)* The verifier should VERIFY CO positively against the scrubbed macro, not exempt it. The diagnostic was correct for v4 and anticipated v9.
**Correction first.** My initial draft accused the diagnostic of being "materially wrong /
over-generalized" about CO. **That was a v4-vs-v9 confusion and is withdrawn.** The diagnostic
probed **v4**; I probed **v9**. The reconciliation:
- The diagnostic's `diag6` measured `stored != old_norm(raw) = 0` across **all** 17.9M rows
  **including CO** — which *proves* **v4 CO had no scrub** (if it had, CO would show residual
  > 0 against the unscrubbed old rule). Correct for v4.
- The **CO scrub was applied in the #182 rebuild (→ v9).** On the live v9 I measured `CO stored
  != name_norm(scrub(raw)) = 0` (exact) and `CO stored != name_norm(raw) = 1,812,865` (59.3%) —
  i.e. v9 CO now == `name_norm(scrub(raw))`, exactly as expected.
- The diagnostic **§3.4 explicitly predicted this** ("CO may retain a residual from the
  documented pre-norm status-decoration scrub"). So the diagnostic was right for v4 and
  anticipated v9 CO. No accusation stands.

**The genuinely-good fix remains.** Because v9 CO stored == `name_norm(scrub(raw))` but the
verifier compares against `name_norm(raw)` (raw `source_entity_name`, intentionally unscrubbed —
`normalize.py` L391 stores `source_entity_name = _raw(name)`), CO will *always* show ~59%
"drift," so the verifier's blind exemption **cannot distinguish "scrub working" from "CO
normalization broken."** Replace the exemption with a positive check against the **scrubbed**
macro (Patch C), so CO is *verified*, not waved through — and so the verifier imports the scrub
from the pipeline rather than re-defining norm logic.

---

## Nits / optional

- **N1. `as_of` stamp is hardcoded stale.** `AS_OF_DEFAULT = "2026-05-31"` (L47); for any future
  rebuild pass `--as-of $(date +%F)` (the entrypoint accepts it) or the audit/provenance stamp
  misrepresents when the data was materialized.
- **N2. `modal deploy` for the scheduled path.** The plan correctly notes `modal run` builds the
  image ephemerally so `deploy` isn't needed for the manual path. But if a scheduled path exists
  (S3), the *deployed* image is what the schedule runs and may be older than disk — state that
  anything shipping to the scheduled path requires `modal deploy`.
- **N3. Plan ↔ diagnostic figures are now stale-by-version (correct observation).** The
  diagnostic cites master `v4` / FL `v5`; live is master `v9` / FL `v10`. The §0/§2.2 row and
  version numbers (8.036% drift, 1,440,646, "11 cols") describe the *pre-#182* world and read as
  authoritative-but-wrong now. Add a one-line "executed by #182; figures below are the
  pre-remediation baseline" banner to the plan, or re-baseline every figure to v9.
- **N4. Verifier `idx_map` calls `stats.index_stats(name)` once per field** — harmless for
  single-field indexes, sloppy if a composite is ever added.
- **N5. The `&`/dash `LIMIT 1` sample checks** prove the rule fired on one row, not uniformity;
  the drift check already covers uniformity, so §4's "sampled names normalize with ` AND `"
  oversells what one row demonstrates.

---

## What the plan got right (do not touch)

- **The whole remediation was correct and it shipped.** The defect analysis (stale `&`/dash key,
  missing `legal_name_base`, unindexed FL agent) was accurate at authoring time, and #182 closed
  it. CA/NY/FL drift 0.000%, `legal_name_base` BTREE-trained, FL agent BTREE live — all verified.
- **The `legal_name_base` alias-reuse projection is correct and is a proven fleet pattern.**
  `legal_name_base("normalized_legal_name")` referencing the SELECT-list alias (so the name
  chain evaluates once) is mirrored in `sam_normalized_entities.py` L153–154 and validated by
  `core/name_norm_check.py`. No double-evaluation bug.
- **"Use `run_normalize`, not `reindex`, to add a *column*" is correct** — `reindex()` only
  rebuilds indexes and cannot project `legal_name_base`.
- **The pre-flight currency gates (§1.3 "STOP if version > 4", §1.4 spine-currency) are the
  right idea** — and §1.3 is exactly what makes re-running this plan safe-by-construction.
- **The drift-by-state verifier using the *imported* `core.name_norm`** (not a re-inlined copy)
  is the correct way to guarantee the check matches what the pipeline wrote — for CA/NY/FL (CO
  needs S6/Patch C).
- **`LANCE_BYPASS_SPILLING` + 32 GiB rationale is accurate** for the high-card BTREE sorts; the
  OOM contingency (bump to 49152) is the right lever for a future rebuild.

---

## Concrete patch set (forward-looking: next rebuild + the open cascade rule)

### Patch A — annotate Task 1 as executed; keep §1.3 as the re-run guard (plan §0/§2)
Add a banner at the top of the plan and replace the §2.1 execution with a verify-only gate:
```text
> EXECUTED 2026-06-05 by PR #182 (Modal run_normalize, no git diff). Master is v9, FL is v10,
> both defects closed. The figures in §0/§2.2 are the PRE-remediation baseline. DO NOT re-run
> Task 1 — §1.3's "STOP if version > 4" gate enforces this. To confirm current health, run the
> verifier (§7) in --phase after; expect ALL CHECKS PASSED.
```

### Patch B — Task 2 is fully done; drop the reindex step (plan §3)
```text
> DONE by #182: INDEX_PLAN["master"]["btree"] now includes registered_agent_name (durable on
> main); live fl_sos_corporations v10 carries the trained BTREE. The §3.2 `reindex --target
> master` step was unnecessary (rebuilds all four healthy indexes) and is moot. No action.
```

### Patch C — verify CO against the SCRUBBED macro instead of exempting it (plan §7 verifier)
Replace the CO branch in the drift loop:
```python
# BEFORE
if PHASE == "after" and st == "CO":
    print(f"    (CO residual {pct:.3f}% is the expected pre-norm status-decoration scrub — informational)")
```
```python
# AFTER  (import _co_status_scrub from pipelines/sos_normalized/normalize.py — don't redefine norm logic)
if PHASE == "after" and st == "CO":
    co_expr = _name_norm(_co_status_scrub("source_entity_name"))
    co_bad = con.execute(
        f"SELECT count(*) FROM m WHERE source_state='CO' "
        f"AND normalized_legal_name IS DISTINCT FROM {co_expr}").fetchone()[0]
    check("CO drift ~0 (vs SCRUBBED current macro)", co_bad == 0, f"{co_bad} mismatches")
```
Add row-count + distinct-key join-loss assertions (S5):
```python
jl = con.execute(f"""
  WITH cur AS (SELECT DISTINCT {expr} k FROM m WHERE source_state IN ('CA','NY','FL') AND {expr} IS NOT NULL),
       sto AS (SELECT DISTINCT normalized_legal_name k FROM m WHERE normalized_legal_name IS NOT NULL)
  SELECT count(*) FROM cur c WHERE NOT EXISTS (SELECT 1 FROM sto s WHERE s.k=c.k)""").fetchone()[0]
check("CA/NY/FL distinct-key join-loss ~0", jl == 0, f"{jl} keys missing from BTREE")
```

### Patch D — make index misses fail + shadow-build (next rebuild; plan §2/§6, fixes M2)
```python
# pipelines/sos_normalized/normalize.py _build_master_indexes — collect + raise instead of WARN-continue
failures = []
for col in MASTER_BTREE_INDEXES:
    try:
        ds.create_scalar_index(col, index_type="BTREE", replace=True); built.append(f"BTREE:{col}")
    except Exception as exc:
        failures.append(f"BTREE:{col}: {exc}")
# ... bitmap loop similarly ...
if failures:
    raise RuntimeError(f"index build incomplete (data committed, indexes degraded): {failures}")
```
And for the next genuine rebuild, write to `…/sos_normalized_master__build/`, verify all four
indexes there, then flip `SOS_NORMALIZED_MASTER_URI` — so the live master is never degraded.

### Patch E — recover the cascade recall gap NOW + document the standing rule (plan §6, fixes M4)
**Action (recall recovery, not freshness):** re-run `build_bridge` (epa_to_sos_bridge) against
the v9 master to recover the `&`/dash EPA→SoS matches the old-macro v4 keys couldn't satisfy;
diff `by_tier`/`by_confidence` before/after to quantify. Audit every consumer in the inventory
whose committed output predates **2026-06-05 23:40** (the key flip) and re-run those built against
the old master. Then add a **Consumer Inventory + Cascade** section with the standing rule: "A
master **value** change (normalization-rule change) requires re-materializing every consumer that
*stores* a join result — `epa_to_sos_bridge` (`build_bridge`) and the resolution crosswalks — in
dependency order *after* the master commits, then re-verify their confidence/tier distributions.
A freshness-only reload (same rules) is the lighter case." Also fix the stale
`materialize_epa.py` L801 comment (master *does* now persist `legal_name_base`).

### Patch F — commit the verifier + stamp as_of (plan §1.2 / §5)
Move §7 to `pipelines/sos_normalized/verify_remediation.py`, version it, have it
`from core.name_norm import name_norm` + import `_co_status_scrub` (don't redefine norm logic);
prefer extending the in-region Modal `verify` (L555). For any real rebuild: `run_normalize
--as-of $(date +%F)`.
