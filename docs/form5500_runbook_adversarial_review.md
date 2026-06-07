# Adversarial Review — Form 5500 Orphan Materialization Runbook

**Reviewer mode:** adversarial, read-only. No SoR mutation, no git mutation, no runbook/pipeline edit.
**Target:** `docs/form5500_orphan_materialization_runbook.md`
**Cross-checked against:** `pipelines/form5500/ingest_form5500.py`, `pipelines/form5500/diagnose_post_ingest.py`, `docs/form5500_ingestion_reconciliation.md`, `docs/form5500_relational_diagnostic.md`, live R2 (`s3://data-sink/{active,landing}/`), local lake `~/core-x-lake/active/`.
**Date:** 2026-06-07

---

## Verdict

**NOT safe to execute end-to-end as-written.** Phases A and B (the actual SoR materialization) are sound — the pipeline mechanics are empirically verified to work, the preflight gates are correct, the clean-publish ordering is safe, and the row/col/index claims for the four new datasets are consistent with the pipeline's runtime behavior. **The single most dangerous flaw is in Phase C (§5): the verification script is guaranteed to crash on every run.** It calls `con.sql(...).df()` (line 146) but omits `pandas` from its inline `dependencies` (line 127); under that exact dependency set DuckDB raises `InvalidInputException: 'pandas' is required for this operation`. Because §5 is the gate that proves the cross-table joins the entire patch exists to unlock, an operator following the runbook publishes 27,996 rows to the system of record and then hits a hard crash on the one step that validates the join graph — producing a false "the relational verification is broken" signal *after* an irreversible-by-default SoR write, with no indication that the data itself is fine. A second, quieter defect compounds it: the runbook's Definition of Done and the reconciliation doc both claim the `(ACK_ID, FORM_ID)` carrier↔broker join is "BTREE on both sides," but the runbook never re-publishes `sch_a_broker`, so the broker side ships **without** the FORM_ID index the patch added — the DoD asserts a state the runbook does not create.

Both are surgical to fix and neither touches Phases A/B. After the §5 dependency fix and the broker re-index addition, the runbook is safe.

---

## Findings

### [CRITICAL] §5 line 127+146 — Phase C verification script crashes 100% of runs (missing `pandas` dependency)

**Location:** `docs/form5500_orphan_materialization_runbook.md:127` (PEP-723 deps) and `:146` (`.df()` call).

**What's wrong:** The heredoc declares `# dependencies = ["duckdb>=1.5,<2", "pylance>=7", "pyarrow>=17"]`. Line 146 is:
```python
heads  = con.sql("SELECT ACK_ID FROM main UNION SELECT ACK_ID FROM sf").df()
```
DuckDB's `.df()` materializes to a pandas DataFrame and requires pandas at runtime. It is absent from the dependency set, so `uv run` resolves an environment without pandas and the call raises.

**Evidence (empirically verified):** Ran the exact §5 dependency set with both calls:
```
to_arrow_table(): 2 rows -> OK
.df(): CRASH -> InvalidInputException: Invalid Input Error: 'pandas' is required for this operation
```
Also reproduced inside the real plumbing against live R2 datasets (`main`, `sf`, `sch_a_broker` as a carrier stand-in): every other operation in §5 — `lance.dataset(uri, storage_options=so).to_table(columns=[...])`, the `SEMI JOIN`, the `LEFT JOIN ... USING(ACK_ID)` — executed cleanly; the run died precisely at the `.df()` line. The `.df()` is the **only** broken mechanic in the script.

**Exact remediation:** Replace `.df()` with `.to_arrow_table()` (no new dependency; `register()` accepts an Arrow table identically). Change line 146 from:
```python
heads  = con.sql("SELECT ACK_ID FROM main UNION SELECT ACK_ID FROM sf").df()
```
to:
```python
heads  = con.sql("SELECT ACK_ID FROM main UNION SELECT ACK_ID FROM sf").to_arrow_table()
```
(Verified working: `heads` then resolves to a 218,477-row Arrow table and `con.register("heads", heads)` + the `LEFT JOIN ... USING(ACK_ID)` orphan count run to completion.) Alternatively add `"pandas>=2"` to line 127, but `.to_arrow_table()` is the lighter, dependency-free fix and matches the all-Arrow idiom the pipeline itself uses (`to_arrow_reader`, `to_table`).

---

### [MAJOR] DoD line 15 + §3.4 reconciliation — runbook never re-publishes `sch_a_broker`, so the FORM_ID index it claims on "both sides" never lands

**Location:** Runbook DoD `:15` ("the `(ACK_ID, FORM_ID)` carrier↔broker composite join resolves"), Phase A `:75-76`, Phase B `:105-107`; corroborated against `ingest_form5500.py:120` and `docs/form5500_ingestion_reconciliation.md:46,167`.

**What's wrong:** PR #300 backfilled `FORM_ID` into `sch_a_broker`'s `biz_keys` (`ingest_form5500.py:120`: `{"stem": "F_SCH_A_PART1", "name": "sch_a_broker", "biz_keys": ["FORM_ID"], ...}`), and the reconciliation doc states the composite join is "now BTREE both sides" (`:167`) and "backfilled by the patch" (`:46`). But the runbook's Phase A and Phase B commands both run `--only sch_a_carrier,sch_c_indirect,sch_c_eligible,sch_c_terminated` — **`sch_a_broker` is not in the list.** The patch only changed the in-code registry; the index is materialized at ingest time via `ds.create_scalar_index`. Without re-running `--only sch_a_broker`, the deployed broker dataset keeps its v1 index set.

**Evidence (empirically verified):** Read `list_indices()` on the live R2 `form5500_sch_a_broker`:
```
DEPLOYED R2 sch_a_broker indices:
   ACK_ID_idx: type=BTree fields=['ACK_ID']
   -> FORM_ID indexed on deployed broker? NO (v1 publish, pre-patch)
```
So after this runbook completes, the carrier head carries `BTREE(FORM_ID)` but the broker detail does not — the composite join is BTREE-pushed on one side only, directly contradicting DoD `:15` and reconciliation `:167`. (Grain is otherwise consistent: broker has 19,993 distinct `(ACK_ID, FORM_ID)` composites, carrier head F_SCH_A = 23,648 ≥ 19,993, so the 1:N head→detail relationship holds and every broker composite can resolve to a carrier head.)

**Exact remediation:** Add `sch_a_broker` to both publish commands so the backfilled index actually lands. Change Phase A (`:75-76`) and Phase B (`:105-107`) `--only` to:
```
--only sch_a_carrier,sch_a_broker,sch_c_indirect,sch_c_eligible,sch_c_terminated
```
and update the SoR-count gate (`:11`, `:94`) and the local-count gate (`:12`, `:114`) — re-publishing `sch_a_broker` does **not** add a prefix (it overwrites an existing one), so the count stays **11**, but the row-total in the Phase A expected tail (`:84`) rises by 34,358 to `62,354 rows` across 5 datasets with 8 BTREE indexes. If the operator prefers to keep blast radius to the four net-new datasets, the alternative surgical fix is to **strike the "both sides" / "now BTREE both sides" language** from DoD `:15` and downgrade it to "the carrier side carries `BTREE(FORM_ID)`; the broker-side FORM_ID index is a separate follow-up publish." Either is acceptable; silently shipping a DoD that asserts an unmet state is not. Re-publishing is the stronger choice because the patch's stated intent (recon `:46`) was to index both sides.

---

### [MAJOR] §3 line 84 — Phase A "expected tail" row total is correct for the 4-dataset run but the header is a hard-coded literal that becomes a false gate on re-stage

**Location:** Runbook `:84`: `**Result:** ✅ PASS — 4 datasets · 27,996 rows · 7 BTREE indexes · …`

**What's wrong:** `27,996` = 23,648 + 2,656 + 1,611 + 81 and `7 BTREE` = 4 (carrier: ACK_ID+FORM_ID+SCH_A_EIN+SCH_A_PLAN_NUM) + 1 + 1 + 1 (the three Sch C tables, ACK_ID each). Those are arithmetically self-consistent for the 2026-06-06 vintage. The risk is that the runbook presents this exact string as "the gate" (`:81` "Expected tail (the gate)"). §1's note (`:41`) correctly says the authoritative gate is the pipeline's internal `row_match`, not the absolute number — but §3's framing contradicts §1 by elevating a vintage-specific literal to gate status. On a re-staged weekly file the counts shift, the pipeline still PASSes (because `row_match` holds), yet an operator matching the literal tail will read a false FAIL.

**Evidence (reasoned + partially verified):** The real gate in code is `render_report`'s `all_ok` (`ingest_form5500.py:479`), computed from `ack_ok and row_match and lz_ok` per dataset — never from an absolute row count. The dry-run I executed for `sch_c_terminated` rendered `✅ PASS — 1 datasets · 81 rows · 1 BTREE` with no dependence on a pre-declared total. So the literal is descriptive, not enforced by the pipeline; the runbook's `:81` framing is what makes it a hazard.

**Exact remediation:** Reword `:81-85` to make the gate the header *token* plus exit code, not the row literal. Replace:
```
**Expected tail (the gate):**
```
**Result:** ✅ PASS — 4 datasets · 27,996 rows · 7 BTREE indexes · …
```
**Gate:** process exit `0` **and** report header `✅ PASS`.
```
with:
```
**Expected tail (reference — 2026-06-06 vintage):**
```
**Result:** ✅ PASS — 4 datasets · 27,996 rows · 7 BTREE indexes · …
```
**Gate:** process exit `0` **and** the report header begins `**Result:** ✅ PASS`. The `27,996` / `7 BTREE` figures are the reference vintage; the binding pass condition is the pipeline's per-dataset `row_match` (landed == parsed), which holds across re-stages even when the absolute counts move.
```

---

### [MINOR] §5 line 130 comment + §0 DoD line 16 — the `<1%` orphan threshold and `main ∪ sf` superset are directionally right but `∪ sf` is inert for Schedule A; the threshold should be documented as "expected ~0"

**Location:** Runbook `:148` (orphan join), `:154` (`[PASS if < 1%]`), `:15`/`:16` (DoD relational sanity).

**What's wrong:** Not a defect, but the threshold is looser than the data warrants and the `∪ sf` term is nearly inert, which could mask a real regression. Schedule A is a welfare/insurance schedule that large plans (F_5500) file; F_5500_SF small plans generally do not file Schedule A.

**Evidence (empirically verified):** Using the deployed broker (`F_SCH_A_PART1`, the detail sharing the carrier's exact `ACK_ID` head-binding) as the orphan-probe subject against `main ∪ sf`:
```
heads (main UNION sf): 218,477 distinct ACK_ID
broker rows                         : 34,358
broker ACK_ID orphan vs main UNION sf: 0  (0.00%)
broker rows binding to MAIN (F_5500): 34,358  (100.00%)
broker rows binding to SF (F_5500_SF): 0  (0.00%)
```
So for Schedule A the true orphan rate is **0%**, binding is **100% to MAIN, 0% to SF**. The `∪ sf` adds 199k irrelevant head keys but resolves zero Schedule A rows. `<1%` would silently pass even if, say, 0.9% of carrier rows (≈213 rows) failed to bind — a real referential break that the empirical baseline says should never happen.

**Exact remediation:** Tighten the comment and keep `∪ sf` (harmless, and defensible as "any filing head") but annotate the expectation. Change `:154`:
```python
print(f"carrier ACK_ID orphan vs main∪sf     : {orphan:,}  ({orphan/rows*100:.2f}%)   [PASS if < 1%]")
```
to:
```python
print(f"carrier ACK_ID orphan vs main∪sf     : {orphan:,}  ({orphan/rows*100:.2f}%)   [PASS if < 1%; empirically ~0 — Sch A binds to F_5500, not SF]")
```
The `<1%` ceiling is an acceptable guard against a partial/torn `main` slice; do not tighten it to exactly 0 (a future re-stage where `main` and Schedule A are cut from different snapshot moments could legitimately strand a handful of rows — see the existing `sch_c_provider` orphan note in `diagnose_post_ingest.py`). The fix is documentation of the expected value, not a stricter gate.

---

### [MINOR] reconciliation doc §3.3 line 135 + Appendix line 215 — "45 NUMERIC / 45 TEXT" for F_SCH_A is wrong (actual 36/54); cosmetic, no behavior impact

**Location:** `docs/form5500_ingestion_reconciliation.md:135` ("**45 numeric / 45 string** + 3 provenance") and `:215` ("90 cols, 45 TEXT / 45 NUMERIC").

**What's wrong:** The F_SCH_A EFAST2 layout has 90 fields split **36 NUMERIC / 54 TEXT**, not 45/45. This is a supporting-doc inaccuracy, not a runbook defect, and it does not change landed output because the pipeline computes types at runtime from the live layout + precedence rules.

**Evidence (empirically verified):** Fetched the live layout `https://askebsa.dol.gov/FOIA%20Files/2025/Latest/F_SCH_A_2025_Latest_layout.txt` (HTTP 200): 90 fields, 36 NUMERIC / 54 TEXT. FORM_ID = NUMERIC (correctly overridden to VARCHAR by FORCE_STRING); INS_CARRIER_EIN, INS_CARRIER_NAIC_CODE, SCH_A_EIN, SCH_A_PLAN_NUM all = TEXT. Note the landed numeric column count will *also* not be 45: after FORCE_STRING pins FORM_ID (NUMERIC→VARCHAR) plus the `_AMT`/`_CNT` name rules, the resolved numeric count is whatever the precedence yields — the runbook's `93` landed-col total (90 + 3 provenance) is correct, but any "45 numeric" sub-claim is not.

**Exact remediation:** This is outside the runbook, in a supporting doc, so it is informational for this review. If corrected: change `:135` to "(`F_SCH_A`, 90 src cols → 93 landed; **36 NUMERIC / 54 TEXT** in the DOL layout, resolved by the precedence rules — FORM_ID pinned VARCHAR) + 3 provenance" and `:215` to "90 cols, 54 TEXT / 36 NUMERIC". The runbook's own row/col figures (`93`, `27,996`) need no change.

---

### [MINOR] §8 line 197-208 — append block is correct for diagnose's dict shape, but it re-asserts the `sch_a_broker` FORM_ID expectation that won't exist unless [MAJOR #2] is fixed

**Location:** Runbook `:197-208`; `diagnose_post_ingest.py:67` (anchor), `:82-83` (existing broker spec).

**What's wrong:** The §8 block adds `sch_a_carrier` with `"exp_btree": ["ACK_ID", "FORM_ID", "SCH_A_EIN", "SCH_A_PLAN_NUM"]` — correct, and it uses the `key_cols`/`lz_cols` keys that match diagnose's schema (not the ingest `biz_keys`/`lz_col` shape), so the append is structurally valid. The latent issue: the existing `sch_a_broker` entry in `diagnose_post_ingest.py:82-83` still declares `"exp_btree": ["ACK_ID"]`. If [MAJOR #2] is fixed by re-publishing broker with the FORM_ID index, the diagnostic will then under-report (it won't expect/verify the new broker FORM_ID index). If [MAJOR #2] is *not* fixed, the diagnostic is consistent but the system is missing the index the patch promised.

**Evidence (verified):** Read `diagnose_post_ingest.py:67` (`DATASETS = [`, anchor correct) and `:82-83` (broker `exp_btree: ["ACK_ID"]`).

**Exact remediation:** When applying §8, also bump the existing broker spec `exp_btree` if [MAJOR #2]'s re-publish is adopted. Change `diagnose_post_ingest.py:82-83` from `"exp_btree": ["ACK_ID"]` to `"exp_btree": ["ACK_ID", "FORM_ID"]` for `sch_a_broker`. This keeps the diagnostic's expected-index plan in lockstep with what the patch+runbook actually materialize. (§8 is labeled "Optional," so this is conditional on adopting §8 at all.)

---

### [STRATEGIC] Ledger-guard the materialization to match the rest of the fleet (`ops.*_runs`)

**Location:** Runbook `:5` (idempotency model), whole-runbook.

**Observation:** The runbook's idempotency story ("`mode=overwrite` + clean-publish", "byte-identical re-runs") is *almost* right but overstated: `build_projection` emits `now() AS ingested_at` (`ingest_form5500.py:309`), so the `ingested_at` provenance column is **non-deterministic** — two runs produce datasets that differ in that column and therefore are **not** byte-identical, and the R2 size-census verify compares sizes (which match) not bytes (which differ in the timestamp). The self-healing and overwrite-safety claims hold; the "byte-identical" claim (`:5`) is literally false. Separately, the global instruction set and the rest of the fleet gate ingests on `ops.*_runs`; this pipeline is ledgerless, so there is no durable record that the materialization ran, no run_id, and no way to detect a half-applied multi-dataset run except by re-listing R2.

**Recommendation:** (a) Soften `:5` to "a retry reproduces an **equivalent** dataset (identical rows/types/indices; the `ingested_at` provenance column is per-run wall-clock and is the only intended difference) and self-heals a partial failure." (b) Consider wiring an `ops.form5500_runs` ledger row (started/finished/row-count/manifest-vintage) so the materialization is observable post-hoc like the rest of the fleet — out of scope for this execution, but it closes the "did this actually run, and against which vintage?" gap that the current ledgerless design leaves open. The vintage point is real: the runbook's numbers are pinned to the 2026-06-06 stage, and nothing records which vintage produced the SoR state.

---

### [STRATEGIC] Promote the §5 verification to a committed, reusable script

**Location:** Runbook `:124-162` (the `/tmp` heredoc).

**Observation:** §5 is a single-use `/tmp/verify_carrier_recon.py` heredoc — exactly the artifact that shipped with the `.df()` bug *because it was never executed* (it cannot have been: it crashes deterministically). A `/tmp` heredoc is untested, unversioned, and not re-runnable after the shell exits. The fleet pattern (`diagnose_post_ingest.py`) is a committed, idempotent, read-only verifier.

**Recommendation:** After fixing [CRITICAL], commit the §5 logic as `pipelines/form5500/verify_carrier_recon.py` (PEP-723, read-only, parameterized on `--root s3://data-sink/active | ~/core-x-lake/active`) and have the runbook invoke the committed script instead of heredoc-ing it. This makes the relational gate testable, reviewable, and re-runnable — and prevents the next "written but never executed" verifier from reaching an operator. Cross-reference: this is the same class of gap the [CRITICAL] finding exploited.

---

## Fix-before-execute checklist (CRITICAL + MAJOR only)

1. **[CRITICAL]** §5 line 146 — replace `.df()` with `.to_arrow_table()` (or add `"pandas>=2"` to line 127). Without this, Phase C crashes on every run *after* the SoR write. **Verified fix.**
2. **[MAJOR]** Phase A/B `--only` (lines 76, 107) — add `sch_a_broker` so the patch's backfilled `BTREE(FORM_ID)` actually lands on the broker side; OR strike the "BTREE on both sides" claim from DoD line 15. The deployed broker currently has `BTREE(ACK_ID)` only. **Verified gap.**
3. **[MAJOR]** §3 lines 81-85 — demote the `27,996 rows / 7 BTREE` literal from "the gate" to "reference (this vintage)"; bind the gate to exit `0` + `✅ PASS` token + the pipeline's internal `row_match`, per §1's own (correct) framing. Prevents a false FAIL on re-stage.

(If Phase A/B re-publish `sch_a_broker` per #2: the Phase A expected-tail total becomes `5 datasets · 62,354 rows · 8 BTREE`; the SoR/local prefix counts stay **11** — broker overwrites, not adds.)

---

## Empirically verified vs. reasoned

**Empirically verified (ran a read-only probe or local `/tmp` dry-run):**
- §5 `.df()` crashes under the exact §5 dependency set; `.to_arrow_table()` works — reproduced twice (synthetic + against live R2 `main`/`sf`/`sch_a_broker`).
- `lance.dataset(uri, storage_options={aws_access_key_id, aws_secret_access_key, endpoint, region})` `.to_table(columns=[...])` opens R2 datasets correctly with the runbook's exact `storage_options` keys (34,358-row read succeeded). DuckDB `SEMI JOIN` and `LEFT JOIN ... USING(ACK_ID)` both parse and execute.
- Deployed R2 `form5500_sch_a_broker` carries **only** `BTree(ACK_ID)` — no FORM_ID index (`list_indices()`).
- Schedule A orphan reality: broker is 0% orphan, 100% bound to F_5500 (MAIN), 0% to F_5500_SF — `∪ sf` is inert for Schedule A; true orphan ≈ 0.
- Live state: R2 `active/` has exactly 7 `form5500_*` prefixes; `landing/form-5500/` has all 4 target zips at the stated sizes (1,260,618 / 25,249 / 71,097 / 6,639 bytes) and 41 total zips; manifest has 41 rows with valid `secondary_url` layout links for all 4 targets.
- Git HEAD = `0c69ce2` (PR #300), `sch_a_carrier` present in pipeline, 7 local `.lance` dirs.
- Full pipeline end-to-end works: local `/tmp` dry-run of `sch_c_terminated` → `✅ PASS`, 81 rows, 19→22 cols (+3 provenance), ACK_ID `string`, BTREE built, report renders.
- F_SCH_A live layout = 90 fields, **36 NUMERIC / 54 TEXT** (not 45/45); FORM_ID = NUMERIC (FORCE_STRING-overridden), the four key fields = TEXT.
- Broker grain: 34,358 rows, 19,993 distinct `(ACK_ID, FORM_ID)`; carrier head 23,648 ≥ 19,993 (1:N head→detail consistent).
- `diagnose_post_ingest.py:67` anchor = `DATASETS = [` (§8 append target correct); broker spec at `:82-83` still `exp_btree: ["ACK_ID"]`.

**Reasoned (not executed — would require publishing `sch_a_carrier` to the SoR, which is out of scope):**
- The exact `INS_CARRIER_EIN LIKE '0%'` count (`≈2,823`) — inherited from the merged reconciliation dry-run; FORCE_STRING membership of `INS_CARRIER_EIN` (`ingest_form5500.py:101`) is confirmed, so leading-zero retention is structurally guaranteed > 0, but the precise 2,823 was not re-measured.
- The carrier→broker `>95%` resolution PASS — reasoned from grain (carrier composites ⊇ broker composites makes ~100% the expected value, so `>95%` is safe but loose); not executed against a live carrier table.
- The "byte-identical re-run" claim is false by inspection of `now() AS ingested_at` (`:309`); not separately measured because the reasoning is dispositive.
- `aws s3 rm --recursive` rollback (§7) correctly removes a net-new prefix and `sch_a_broker` is correctly absent from the rollback list (it is not net-new) — read, not executed (would mutate the SoR).
