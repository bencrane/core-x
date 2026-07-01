# USAspending FPDS Canonical — Critical Plan Review

> **STATUS UPDATE (post-implementation).** The two highest-impact findings below are now **RESOLVED
> in shipped code** and are marked SUPERSEDED inline — do NOT re-apply their line-anchored fixes
> (the line anchors point at a merge body that no longer exists; re-applying would REGRESS):
> - **P0-1 (monthly corrections discarded) — SUPERSEDED / LANDED.** The anti-join survivor-universe
>   design was replaced by the two-tier per-key argmax reconciliation (`bulk_base = BULK⊕MONTHLY`,
>   then flat 3-way `core_winner`). MONTHLY now competes on every shared key; `canonical_source ∈
>   {fresh,bulk,monthly}`; `monthly_corrections_applied` is a gated (>0) metric.
> - **P1-3 (`bl_probe` 107M duplicate) — SUPERSEDED / LANDED.** `bl_probe` was deleted; the core
>   resolution and enrichment joins reference `bulk_latest` directly on its PARTITION key.
> - **P0-4 (monotonic tombstone accumulation) — CONFIRMED, now the locked R5+R6 requirement.** See
>   the re-anchored P0-4 below: R6 scopes `delete_keys` to the latest `archive_snapshot_stamp`; R5
>   consumes the (previously dead) `delta_lmt` to reinstate strictly-newer 'D' keys. Ground truth:
>   **39 strictly-newer 'D' keys survive** (the 39-key floor).
> - **Enrichment expansion LANDED.** 12 MONTHLY-unique cols (TAS/federal-account funding +
>   officer-comp name/amount) added to `COLUMN_SPEC` as pg-preferred `COALESCE(pg, monthly)`, sourced
>   from a separate enrichment-populatedness dedup (`monthly_enrich_latest`). Gains: TAS/federal
>   +330,370 keys, officer-comp +507,542 keys.
> - Consumer-repoint findings (P0-5, P0-6, P1-8) and the cadence/control-plane findings (P0-2, P0-3)
>   remain OPEN as a separate follow-up track — the current change set is the reconciliation +
>   enrichment + delete-gate core, not the repoint or the Trigger.dev cadence.

Review of the build plan (`docs/plans/USASPENDING_FPDS_CANONICAL_BUILD_PLAN.md`), the worker
(`pipelines/usaspending/usaspending_fpds_canonical.py`), the ops DDL
(`pipelines/usaspending/ops_usaspending_fpds_canonical_runs.sql`), and the sole downstream consumer
(`pipelines/serving/materialize_active_awards.py`). Findings are line-verified against code and cross-checked
against bounded live R2 probes of all four prime feeds.

---

## 1. Executive verdict

The merge CORE is sound and buildable as-is: keys are proven byte-for-byte, the `s()`/`kbulk()` normalization
is applied uniformly across every projection and anti-join (the §3.7 disjointness proof holds), BULK is
confirmed PK-unique (exact spill DISTINCT = 107,250,527 = rowcount, zero dups), the FAIL-CLOSED PK gate raises
before publish, and the tombstone leg is correctly `--since`-immune. A full build will produce a correct,
PK-unique table.

**The single biggest gap is the archive-correction routing decision (P0, F1/L2-1).** The 2026-06-06 monthly CSV
(source #2 in the operator's mental model) is included precisely because it post-dates BULK's 2026-04-23
pg-dump — yet the plan anti-joins the archive leg against the FULL BULK key universe, so all **46,234 keys the
archive carries with strictly-newer mtime than BULK are silently discarded**. The archive contributes ONLY its
219,418 archive-only keys as a backfill; its actual corrective payload never lands. The plan's own §5 row-5 and
§7 label archive as a "corrections" source, directly contradicting the §3 design that discards those corrections.

Second-order but blocking for the stated migration goal: repointing `materialize_active_awards.py` at the
canonical **hard-breaks** — 76 of its 114 columns are absent from `COLUMN_SPEC`, including the two columns that
gate the entire product thesis (`period_of_performance_potential_end_date` → active/done boundary;
`subcontracting_plan_code` → the 2.8x subcontracting-obligation completeness gain). The build ships a table that
cannot serve its named consumer.

Three §7 verify centerlines are stale against live truth and will RED a correct build (`rows_out` off by ~981K,
`fresh_only_tail` off by ~189K, `deletes_tombstoned` hint off by ~2.8x). And there is **no control-plane cadence**
(no Trigger.dev task) plus a **monotonic tombstone-accumulation trap** that only manifests on the 2nd monthly
cycle. None of these block a one-shot manual build; all block the "reconciled monthly SoR" the plan advertises.

**Buildable as-is:** yes for a correct one-shot table. **Fit-for-purpose as-is:** no — it discards the monthly
corrections it exists to capture, cannot serve its consumer, and has no safe recurring cadence.

---

## 2. THE EXPLICIT 3-SOURCE RECONCILIATION LADDER

The operator's 3-source model maps cleanly onto the plan's 4 feeds: **source #2 (the 2026-06-06 monthly BULK CSV)
= archive_full + archive_delta together** — one physical drop (both stamped `20260606`), split into a data table
and a deletion ledger. The plan's "4th feed" is not a 4th source; it is the delete-ledger half of source #2.

### Precedence ladder as IMPLEMENTED (top wins)

| Rank | Rule | Source of value |
|---|---|---|
| 1 | **DELETE** — key in the 656 delta-'D' set is removed post-merge (553 effective) | archive_delta 'D' (tombstone anti-join) |
| 2 | **FRESH wins on tie-or-newer** — `MAX(last_modified_date)`, tie → FRESH | FRESH volatile-core |
| 3 | **BULK wins when strictly-newer mtime** (BLOCKER-1) | BULK volatile-core |
| 4 | **archive_full — ONLY for archive-only keys** (anti-joined vs full BULK + FRESH) | archive_full volatile-core |
| — | **Enrichment (27 BULK-only cols) ALWAYS from `bulk_latest`** regardless of core winner (BLOCKER-2) | BULK enrichment |

### Per-source role + shared-key rule + monthly-CSV propagation

| Source | Role | Exact precedence rule for a SHARED key | How its corrections + deletes propagate |
|---|---|---|---|
| **BULK pg-dump** (107,250,527 rows) | volatile-core body + uniform enrichment source + BULK-only survivor body | Owns the PK universe. Wins core only when its mtime is strictly newer than FRESH (rank 3). Enrichment always sourced from here. | Its 2026-04-23 snapshot is the frozen back-catalog (to 1962). Corrections land on the next BULK re-dump. |
| **MONTHLY-CSV** = archive_full (2,975,677) + archive_delta (3,060,070) | archive_full → survivor-body for **archive-only keys ONLY**; archive_delta → **tombstone anti-join ONLY** | archive_full is anti-joined vs the FULL BULK universe AND fresh_keys → it NEVER competes for a shared key. archive_delta 'D' removes any surviving key. | **CORRECTIONS: DISCARDED.** The ~3.05M non-'D' archive rows are ingested into `archive_proj`, then all but the 219,418 archive-only keys are anti-joined out. The 46,234 keys carrying strictly-newer mtime than BULK's April snapshot are silently dropped. **DELETES: applied** — 553 of 656 'D' keys hit a survivor and are removed. |
| **FRESH API prime** (1,986,682 rows → 1,706,525 keys after dedup) | volatile-core precedence winner on tie/newer + FRESH-only survivor tail | Wins core on tie or when FRESH mtime ≥ BULK (rank 2). Supplies `recipient_zip_4_code` (BULK lacks it). | Rolling ~4-month `last_modified` window (2026-03-09..2026-06-26) — structurally cannot re-touch old keys, so old-key freshness depends entirely on BULK re-dumps + the (discarded) archive corrections. Contributes 711,814 FRESH-only keys. |

### DIRECT ANSWER TO THE OPERATOR'S CENTRAL CONCERN

**Does the plan ingest the ~3.05M non-'D' archive correction rows, or discard them?**
It **ingests then discards** them. `arch_survivors = archive_proj ANTI JOIN fresh_keys ANTI JOIN bulk_keys_full`
(pipeline L466-474) drops every archive key that exists in BULK or FRESH. `arch_final` (L489-495) pulls only
enrichment from BULK and never enters the `b_wins` precedence probe (which is FRESH-exclusive, L481-487).
**Net: only the 656 'D' tombstones and 219,418 archive-only keys are used from the entire 2026-06-06 monthly drop.
The 46,234 post-April corrections to BULK-owned keys — the exact reason source #2 exists — are thrown away.**
This is a deliberate §3 design choice, but it contradicts the plan's own §5/§7 "corrections" language and the
operator's inclusion rationale. **This is the decision that must be made explicitly before any monthly cadence ships.**

---

## 3. Findings by severity

### P0

**P0-1 — Monthly-CSV corrections to BULK/FRESH-owned keys are silently discarded (F1 / L2-1 / F3-cadence). — SUPERSEDED / RESOLVED IN SHIPPED CODE.**
The fix below ("corrections must land") was implemented, but NOT as a separate 3-way anti-join fold —
the entire survivor-universe design was replaced by the two-tier per-key argmax reconciliation (see
BUILDPLAN §3). `arch_survivors`/`bulk_only`/`bl_probe` no longer exist. MONTHLY (`monthly_latest`)
competes on every shared key; the flat `core_winner` window resolves `argmax(last_modified_date)` with
precedence FRESH>MONTHLY>BULK; `canonical_source ∈ {fresh,bulk,monthly}`. The historical defect
description is retained below for provenance ONLY — its line anchors are dead.
Defect (historical): `arch_survivors` (pipeline L466-474) anti-joins archive against the full BULK universe; the precedence
probe (`_b_wins_replace_block`, L370-384; `b_wins` at L374) is FRESH-vs-BULK only — archive_full has no precedence
entry. Live probe: 46,234 shared keys carry strictly-newer archive mtime than BULK's 2026-04-23 snapshot (0
older); all discarded. Plan §5 row-5 (L69) and §7 (L279) call archive a "corrections" source, which it is not.
Fix — force the operator's decision:
- **If corrections must land** (aligned with source-#2's inclusion rationale): build `arch_latest` (per-key collapse
  mirroring `bulk_latest`); extend the volatile-core winner to a 3-way `MAX(last_modified_date)` across
  {fresh_latest, bulk_latest, arch_latest} with locked tie-break **FRESH > archive > BULK**; NULL-parsed archive
  mtime treated as older (mirrors BLOCKER-1). **Critical:** do NOT simply drop the `ANTI JOIN bulk_keys_full` —
  that lets a BULK∩archive key emit rows on both the bulk/fresh legs AND `arch_final`, violating the §3.7
  three-disjoint-universe invariant and tripping the FAIL-CLOSED PK gate (L735). Fold archive's volatile-core into
  the single per-key precedence resolution, not a separate `arch_final` row. `rows_out` is UNCHANGED (~108.18M —
  those keys already count as BULK; only their values change).
- **If archive-only-backfill is intended:** strike the "corrections" language at plan §2 L69 and §7 L279, add a
  comment at pipeline L466 stating archive corrections to BULK-owned keys are intentionally not applied.

**P0-2 — No Trigger.dev task / control-plane cadence; the plan defines cadence nowhere (freshness F1).**
Defect: no `src/trigger/usaspending_fpds_canonical.ts` exists; the modal runner's only cadence statement is prose;
plan §4 lists manual subcommands, §9 has no scheduling row. build→index→verify ordering and fail-on-timeout are
operator-carried; a build without a follow-up index leaves prod correct-but-unindexed.
Fix: add `src/trigger/usaspending_fpds_canonical.ts` modeled on `govcon_prime_trajectories.ts` (waitpoint →
Universal Dispatcher → Modal callback), chaining `build_fn → index_fn → verify_fn`. Wrap as `schedules.task({ cron })`
fired a bounded lag after the archive drop lands. **Concurrency guard is mandatory** (see P0-3).

**P0-3 — Recurring cadence introduces an R2-prefix wipe-then-publish race with no cross-app mutex (cadence-safety).**
Defect: `max_containers=1` is per-Modal-app; a Trigger.dev-scheduled fire is a separate ephemeral `modal run` app
not covered by it. If a scheduled build overruns its interval, the next fire can `DeleteObjects` + full re-upload
the SAME R2 prefix (build L568-586) concurrently — prefix corruption the one-shot manual model never had.
Fix: any `schedules.task` for P0-2 MUST set a Trigger.dev `concurrencyKey` / `maxConcurrentRuns=1`, OR the
dispatcher must refuse a launch when `modal app list` shows a live build.

**P0-4 — Tombstone set accumulates monotonically across appended monthly snapshots (data-loss under cadence). — CONFIRMED; both remedies now IMPLEMENTED as the locked R5+R6 gates.**
This is the confirmed R5/R6 defect. BOTH remedies the fix line offered were taken (they are
complementary, not alternatives):
- **R6 (scope to latest snapshot):** `delete_keys` is scoped to `max(archive_snapshot_stamp)` in the
  delta-'D' set; `archive_snapshot_stamp` was added to `delta_scan_cols`, gated by `delta_has_stamp`.
- **R5 (reinstatement gate):** the previously-DEAD `delta_lmt` (see P2-1) is now CONSUMED — a 'D'
  tombstone is honored only when the reconciled-winner mtime ≤ `delta_lmt`; a strictly-newer non-'D'
  row REINSTATES the key. Tombstone-minus-reinstatement is ONE coupled final-state op applied to
  `resolved` (post fresh overlay). **Ground truth: 92/656 'D' keys are live in `monthly_full`; 39 are
  strictly-newer → the 39-key floor SURVIVES** (must be PRESENT in `canonical_out`). "Delete-wins-
  always" is no longer the semantics.
Defect (historical): the delta scanner filters ONLY by `correction_delete_ind='D'` (L716-718) with NO `archive_snapshot_stamp`
predicate; `delete_keys` (L506-511) unions all 'D' keys across every appended monthly snapshot (archive_delta is
append-only and stamped). A key deleted in an old month stays tombstoned forever even if a later BULK/FRESH mtime
or a newer snapshot legitimately reinstates it — and `reinstatement_candidates` is LOG-only.

**P0-5 — Repoint of `materialize_active_awards.py` hard-breaks: `period_of_performance_potential_end_date` absent
from `COLUMN_SPEC`, corrupting the active/done membership boundary (L6-01).**
Defect: consumer `_SRC_COLS` L100 requests `period_of_performance_potential_end_date`; `_assemble` L253
`TRY_CAST(... AS DATE) AS pop_potential_end` feeds `active_potential` (L274), `has_option_tail` (L275-276),
`pop_unknown` (L277), and the membership WHERE (L335-337, an explicit 3-way OR). COLUMN_SPEC carries
`period_of_performance_current_end_date` (L132-134) but NO potential-end entry (verified by full enumeration).
Repoint fails at DuckDB name-binding; without it, unexercised-option awards the consumer docstring mandates be
INCLUDED are silently dropped.
Fix: add `period_of_performance_potential_end_date` to COLUMN_SPEC as a `group='core'` column
(bulk_expr pass-through date32; feed_expr `TRY_CAST(s(...) AS DATE)`) as a hard precondition of any repoint.

**P0-6 — The advertised 2.8x `has_subcontracting_plan` completeness gain is unreachable: `subcontracting_plan_code`
not in `COLUMN_SPEC` (L6-02).**
Defect: consumer L309 derives `has_subcontracting_plan` from `upper(trim(subcontracting_plan_code)) IN
('C','D','E','F','G','H')`; both `subcontracting_plan` and `subcontracting_plan_code` are in `_SRC_COLS` L117 and
BITMAP-indexed (L83-84). Neither appears in COLUMN_SPEC. A repointed consumer computes `has_subcontracting_plan=FALSE`
for 100% of rows. BULK carries `subcontracting_plan` natively (the source of the 59,381 BULK-only active-with-subplan
upside), so the crosswalk is a direct add.
Fix: promote `subcontracting_plan` + `subcontracting_plan_code` into COLUMN_SPEC as `group='core'` before the
repoint milestone.

### P1

**P1-1 — Three (four) §7 verify centerlines are stale against live truth → false-fail / false-confidence (F3 / L2-3).**
Defect: plan §7 L269 `rows_out ≈107,200,000 ±0.5M` (the derivation string OMITS the 219,418 archive-only term AND
uses `fresh_only_tail≈523K`); L275 `fresh_only_tail ≈523,000 ±50K`; L273 `deletes_tombstoned "close to 198+"`; L279
`canonical_source archive "small (FY2026 corrections only)"`. Live: `rows_out` = 108,181,206 (BULK 107,250,527 +
FRESH-only 711,814 + archive-only 219,418 − 553 tombstoned); `fresh_only_tail` = 711,814; `deletes_tombstoned` = 553;
canonical_source: bulk-only ≈ 106.3M, fresh-owned survivors ≈ 1.7M, archive_full = 219,418 archive-only backfill
(NOT corrections).
Fix: update §7 to `rows_out ≈108,181,000 ±0.5M`, `fresh_only_tail ≈711,800 ±30K`, `deletes_tombstoned ≈553`,
canonical_source `bulk ≈106.3M ≫ fresh-owned ≈1.7M ≫ archive_full ≈219K backfill`. Record `bulk_self_dup=0` as
PROVEN (§7 L268 "never proven" caveat resolves). Re-derive `rows_out` from disjoint key-universe arithmetic
explicitly including archive-only. `rows_out` is INVARIANT under the P0-1 fix. Re-baseline AFTER the P0-1 decision.

**P1-2 — §7 verify gates are the advertised acceptance harness but `verify()` implements almost none (F2).**
Defect: `verify()` (L823-870) computes only `rows_out`, `pk_unique`/`pk_dupes`, `null_pk_rows`, `max_action_date`,
`built_at_distinct`, `canonical_source_distribution`, `fresh_rows_with_enrichment`, `columns`, `indices`. §7 (L266-279)
specifies eight more absent from read-back: `delta_d_keys`, `reinstatement_candidates`, `true_tail_null_enrichment`
(GATED), `bulkside_null_enrichment`, and `spot_join_20_keys` entirely; `deletes_tombstoned` exists only in `build()`
(L744-746), not in the independent read-back. `spot_join_20_keys` — the ONLY check that would catch
tombstones-not-applied or archive-only-keys-missing — is absent.
Fix: implement in `verify()`: (a) `delta_d_keys` + `spot_join_20_keys` as membership assertions on the PUBLISHED
table (hardcoded key list: pre-2020 BULK key present, 2026-06 FRESH-only key present, known FRESH∩'D' key ABSENT,
archive-only key present, non-NULL enrichment on FRESH-sourced); (b) `true_tail_null_enrichment` as a real
fail-closed gate; (c) `reinstatement_candidates` as a logged count.

**P1-3 — `bl_probe` is a full ~107M-row verbatim duplicate of `bulk_latest` (L4-01). — SUPERSEDED / RESOLVED IN SHIPPED CODE.**
`bl_probe` was deleted. The core resolution joins against `bulk_latest` directly on
`contract_transaction_unique_key` (its PARTITION key); the enrichment fill LEFT JOINs `bulk_latest`
(pg) and `monthly_enrich_latest` (monthly) on the same key. No second full-width materialization
remains. Historical defect retained for provenance only.
Defect (historical): L478-479 `CREATE TEMP TABLE bl_probe AS SELECT *, contract_transaction_unique_key AS k FROM bulk_latest`
— a second full-width materialization whose only delta is an aliased duplicate of the partition key. Directly
contradicts plan §3 BLOCKER-1 (L112-113: "a cheap probe into the already-built `bulk_latest`, NOT a second 109M pass").
Fix: delete the `bl_probe` CREATE. In `fresh_final` (L487) and `arch_final` (L495) join `bulk_latest b ON
f.contract_transaction_unique_key = b.contract_transaction_unique_key`. Update the `b_wins` predicate (L374) from
`b.k IS NOT NULL` to `b.contract_transaction_unique_key IS NOT NULL`. `bulk_latest` already carries the key as its
PARTITION BY column (L450) — zero new materialization.

**P1-4 — Eight concurrent ~107M-row TEMP TABLEs never dropped mid-script → 350-520 GB spill on the full build (L4-02).**
Defect: DuckDB TEMP TABLEs persist until `con.close()` (L761); the merge tail holds a chain of full-universe tables
concurrently; plan §5 sizes only RAM, gives no scratch-volume floor; §6 sets `temp_directory` but names no capacity.
Fix (ordering is load-bearing — naive DROPs break downstream metric reads): after P1-3 eliminates `bl_probe` the
graph shrinks by one copy for free. Only `fresh_final`, `arch_final`, `bulk_final`, `arch_survivors`, `bulk_only`,
`fresh_keys` are safe to DROP after `merged` is built (L503); `merged` after L747. `bulk_proj` cannot drop until
the L726 count runs; `fresh_latest`/`bulk_keys_full`/`merged`/`delete_keys` must survive until the L741-747 metric
queries. Thread DROPs through `build()` around those reads, NOT as a pure-SQL edit inside `_merge_tail_sql()`
(that would raise "table does not exist"). Add a startup scratch guard: assert
`shutil.disk_usage(DUCK_TMP).free >= FPDS_CANONICAL_MIN_SCRATCH_GB*1024**3` at `build()` entry; pin
`FPDS_CANONICAL_DUCKDB_TEMP_DIR` to a sized volume in the plan §5 box-routing table.

**P1-5 — `verify()` re-materializes the entire ~108M-row published canonical into a TEMP TABLE on the 8GB sample
default memory_limit (out-of-core, verifier-added).**
Defect: L840 `CREATE TEMP TABLE c AS SELECT * FROM c_src` fully materializes the read-back scanner because the six
downstream queries (L842-854) multi-scan it. Runs under `_duck()` with `DUCK_MEM` defaulting to 8GB (L86) and the
same unsized `/tmp` (L87). The full-build run command (plan §4 L192) invokes `verify` with no
`FPDS_CANONICAL_DUCKDB_MEM` override (unlike build's `=96GB`). Heavy spill on an 8GB limit.
Fix: compute all six aggregates in ONE pass over the `c_src` reader (drop the `CREATE TEMP TABLE c` + multi-scan),
OR set `FPDS_CANONICAL_DUCKDB_MEM` on the verify run command and document a verify-side memory/scratch requirement.

**P1-6 — `pop_zip5 → primary_place_of_performance_zip_4` lossy map: a 5-digit value masquerades as a zip4 field
with no width guard (L3-03).**
Defect: pipeline L207-209 maps BULK `s(pop_zip5)` directly into `primary_place_of_performance_zip_4` with no
truncation/width-tag/gate; documented only in §2 correction #3 prose. Contrast the recipient side (L164-165): honest
NULL on BULK + 5-digit preserved as separate enrichment `recipient_location_zip5` (L261-262).
Fix (adopt symmetric recipient pattern): route BULK `pop_zip5` to a dedicated enrich column `pop_zip5`, set the
canonical `primary_place_of_performance_zip_4` `bulk_expr=None` (honest NULL on BULK), let FRESH/archive supply the
true zip4. If rejected, at minimum add a §7 gate asserting length distribution of `primary_place_of_performance_zip_4`
by `canonical_source`. Note: this column is not currently indexed (BTREE_COLS/BITMAP_COLS L291-295), so the hazard
is latent-at-parse, not active-at-index today.

**P1-7 — Ops ledger omits per-leg precedence-wins + decision-relevant diagnostics; `indices_built` hard-coded None (F4).**
Defect: `ops_..._runs.sql` L10-20 records no per-leg precedence wins, no `reinstatement_candidates`, no
`canonical_source` distribution. `build()` passes `indices_built=None` (L790); `index()` (L796-820) and the Modal
`index_fn` never call `_record_run` — no ledger row ever records that indexing happened. Under recurring cadence the
ledger is the only durable audit trail; a staleness regression would be invisible run-over-run.
Fix: add bigint columns `bulk_precedence_wins`, `fresh_precedence_wins`, `reinstatement_candidates`,
`archive_contributed` to the DDL; compute in `build()` (the `b_wins` CASE and reinstatement mtime check already exist
as SQL shapes) and pass through `_record_run`. Wire `index_fn`/`index()` to `_record_run` (or a lightweight ledger
UPDATE) so `indices_built` is populated and the build-without-index footgun is auditable.

**P1-8 — 76 of 114 consumer columns missing (the socio / code-desc / permalink / parent-uei tail) — repoint
prerequisites, plus a parent-uei NAMING mismatch (L6-03).**
Defect: the 23 `SOCIO_FLAGS` (consumer L63-74), `ordering_period_end_date` (L100), `usaspending_permalink` (L140),
the code/description pairs — all consumed, all absent from COLUMN_SPEC. `recipient_parent_uei`/`recipient_parent_name`
data EXISTS in COLUMN_SPEC but under DIFFERENT canonical names (`parent_uei` L227, `parent_recipient_name` L231), so
the consumer's references still fail to bind. Correction to prior framing: the consumer's socio idiom (L247
`COALESCE(lower(trim(c)) IN ('t','true','y','yes','1'), FALSE)`) already tolerates BOTH BULK's `'t'` and FRESH's
`'y'/'1'`, so socio normalization is a cleanliness/index choice, NOT a correctness gate — the ONLY hard break is
missing/misnamed columns.
Fix: sequence the §8 v2 widening as a hard prerequisite of the repoint; expose
`recipient_parent_uei`/`recipient_parent_name` under the consumer's canonical names (alias the existing enrich
columns); land the socio flags as `group='core'` with dual feed/bulk exprs (feed_expr `s(flag)`, bulk_expr a
bool→VARCHAR cast, e.g. `CASE WHEN woman_owned_business THEN 't' ELSE 'f' END`); add a verify gate asserting
non-NULL socio rate is high on BOTH `canonical_source='bulk'` AND `='fresh'` rows. Gate repoint on a column-superset
assertion over `_SRC_COLS + SOCIO_FLAGS`.

**P1-9 — FRESH `contract_subaward` correctly stays a separate table — folding it in is a PK-category error (L6-04).**
This is a POV confirmation, not a code defect (the plan is correct by omission). See §4.

### P2

**P2-1 — `delete_keys.delta_lmt` computed but never consumed — dead scaffolding (F4-lens1). — SUPERSEDED / RESOLVED (stronger than the fix asked).**
`delta_lmt` is now CONSUMED by the R5 reinstatement gate (P0-4), and not merely as a LOG signal —
it GATES survival: `WHERE d.k IS NULL OR resolved.last_modified_date > d.delta_lmt`. The original
"do NOT gate (delete-wins-always is locked)" guidance is OBSOLETE — the locked semantics changed to
tombstone-minus-reinstatement. Historical defect retained for provenance.

**P2-2 — `canonical_out` and `bulk_final` are redundant full-width copies of the two largest tables (L4-03).**
`canonical_out` (L518-519) is a self-admitted "defensive" `SELECT {canon_cols} FROM canonical`; `bulk_final` (L497)
is a bare `SELECT * FROM bulk_only` (no rename at all). Fix: stream directly from `canonical` at L755 (keep the
explicit `canon_cols` list in the streaming SELECT for column-order guarantee), delete the `canonical_out` CREATE;
reference `bulk_only` directly in the merged UNION (L502), delete the `bulk_final` CREATE. Both are terminal
producers — safe to eliminate outright (P1-4's DROP-ordering caveat does not apply).

**P2-3 — `LANCE_BYPASS_SPILLING` RAM cost on the index sort is unbudgeted; no startup RAM guard (L4-04).**
`index()` already runs standalone (build's con closed at L761) — restate "must run standalone" as a documented HARD
RULE in §5, not a change. Bypass-spill makes the ~107M PK BTREE sort in-RAM and NON-resumable (a killed sort leaves
no external-merge spill). Fix: add a §5 RAM-floor row (index requires ≥64GB free RAM); add a startup RAM guard at
`index()` top (L796) raising below `FPDS_CANONICAL_INDEX_MIN_RAM_GB` (default 64) before the first
`create_scalar_index` (L807) — converts silent mid-index OOM into fail-fast.

**P2-4 — `--since` cannot express go-forward refresh; monthly cost is O(total history), not O(monthly delta) (F5).**
Full-overwrite is a CORRECTNESS requirement (whole-universe precedence is incompatible with naive append), not a
defect. Fix: state the cost explicitly in §5 (every monthly run re-scans ~107M BULK, re-collapses `bulk_latest`,
re-sorts the ~107M PK BTREE, re-uploads 50-90 GiB). Size cadence accordingly (monthly is fine; never more frequent
than the archive drop). Name the two-tier split (frozen historical partition + hot recent partition) as an explicit
v2, not a silent assumption.

**P2-5 — §8 under-scopes the widening as "BULK-only append"; a dual-sourced core column touches 3 projections +
the precedence CASE (L3-04).** §8 L293-294 ("append columns, re-index — no rewrite") is true for Lance storage but
glosses that a dual-sourced core column requires editing `feed_expr`, setting `group='core'` (else the feed side
emits `_typed_null` at L363-365 and enrichment routes b-only at L381-384), AND adding it to the `b_wins` block
(L377-379). Fix: make §8 explicit about the two buckets' differing code cost — (i) BULK-only enrichment = rpt.*
append, cheapest; (ii) dual-sourced core (potential_end, ordering_period_end, potential_total_value_of_award, socio
flags) = feed_expr + bulk_expr + `group='core'` + `b_wins` inclusion — the real cost.

**P2-6 — Consumer type-shift is SAFE; the ONLY repoint blocker is missing NAMES, not typed-column idioms
(schema-typing, verifier-added).** `materialize_active_awards.py` wraps every date/double in `TRY_CAST(... AS
DATE/DOUBLE)` (L232-233, L251-255, L271, L288-293, L313); `TRY_CAST(date32 AS DATE)` / `TRY_CAST(double AS DOUBLE)`
are no-ops. So for THIS consumer the plan §2 "typed-column footgun" does not bite — the sole break is the 76 missing
column names (P0-5, P0-6, P1-8). Fix: state this explicitly in §2/§8 so the perceived risk (type idioms) is not
mistaken for the real one (missing columns).

**P2-7 — `current_total_value_award → current_total_value_of_award` is a NON-1:1 crosswalk not called out in
§2/§3 (schema-typing, verifier-added).** COLUMN_SPEC L141-143 crosswalks BULK rpt `current_total_value_award` (no
`of_`) via `bulk_expr TRY_CAST(s(...) AS DOUBLE)`. Code is correct but the non-identity rename is uncommented, unlike
`construction_wage_rate_req` (L186-188). Fix: list this rename in the §3 crosswalk description for auditability.

**P2-8 — `canonical_source` / award-grain selection semantics shift silently on repoint (downstream-contract,
verifier-added).** The consumer collapses txn→award at L230-234 `ORDER BY action_date DESC, last_modified_date DESC,
...`. On the FRESH-only feed today "latest txn per award" is FRESH-latest; on the canonical it becomes the reconciled
(possibly BULK-corrected) winner. This is the DESIRED behavior but an unvalidated semantic change. Fix: add a repoint
regression test comparing award-grain row selection FRESH-only vs canonical on a sample of shared awards, confirming
the delta is exactly the BLOCKER-1 corrections and not an unintended tiebreak flip. This subsumes the
`modification_number` VARCHAR-ordering watch item (canonical primaries on `last_modified_date` L433/L451; consumer
primaries on `action_date`).

---

## 4. SUBAWARD POV — SEPARATE, not folded

**Recommendation: keep subawards a SEPARATE canonical.** Confirmed by code: the pipeline docstring (L1-9) scopes
exactly four PRIME feeds; COLUMN_SPEC carries zero subaward columns; the PK is `contract_transaction_unique_key` with
a fail-closed uniqueness gate (L731-739). A subaward is a CHILD of a prime award (many subawards per prime txn), so
folding subaward rows in would either trip the L735 PK gate or force a lossy 1:1 collapse — a category error. The
operator's "assumed separate thing" holds; the plan is correct to carry no subaward columns.

### Parallel subaward-canonical spec (separate task, separate PK grain)

| Dimension | Spec |
|---|---|
| **Sources** | BULK `usaspending/subaward_search` (9,801,723 rows / 210 cols) as the body ⊕ FRESH `usaspending_api_fresh/contract_subaward` (199,901 rows / 118 cols) as the freshness overlay |
| **PK / grain** | Subaward grain. BULK PK `broker_subaward_id` (exact 1:1 with rows). FRESH PK `subaward_sam_report_id` (exact 1:1). **No shared PK across feeds** → reconcile on the prime link + subaward number, not a single key |
| **Precedence** | `MAX(last_modified)` tie-break FRESH, mirroring the prime ladder; BULK `broker_updated_at` to 2026-04-24, FRESH `last_modified` to 2026-06-05 |
| **Contract-only scope** | FRESH is contract-only (folds against BULK sub-CONTRACT rows: BULK grants 7,158,222 / contracts 2,643,501) |
| **Why the coverage gap makes it urgent** | FRESH contract-subaward coverage is only **7.56%** of the BULK contract-subaward universe (90.49-97.65% contained), so FRESH alone is a thin recent slice. The reconciliation argument is STRONGER here than for prime: without BULK as the body, ~92% of contract subawards are invisible. Span 2001-2026 confirms it is not a recent-window residue |

This belongs in a separate build, not a merge into the PK-grained prime table.

---

## 5. Downstream migration — repoint `materialize_active_awards.py` at the canonical

`materialize_active_awards.py` is the SOLE FRESH prime-txn consumer (`PRIMETXN_URI` L48, reads 114 cols). It cannot
be repointed as-built: 76 of 114 columns are absent from COLUMN_SPEC.

### Steps (each is a hard precondition of the repoint)

1. **Land the consumer-required columns into COLUMN_SPEC as `group='core'`** (blocks on P0-5, P0-6, P1-8):
   `period_of_performance_potential_end_date`, `ordering_period_end_date`, `subcontracting_plan`,
   `subcontracting_plan_code`, `potential_total_value_of_award`, `total_dollars_obligated`,
   `base_and_exercised_options_value`, `number_of_offers_received`, `usaspending_permalink`, the 23 socio flags, and
   the `*_code` half-pairs.
2. **Expose `recipient_parent_uei`/`recipient_parent_name`** under the consumer's canonical names (alias the existing
   `parent_uei`/`parent_recipient_name` enrich columns — data already present, only misnamed).
3. **Add a schema-superset acceptance test** asserting the canonical schema covers `_SRC_COLS + SOCIO_FLAGS` — the
   repoint precondition becomes enumerable and testable, not implied. Expand plan §8 (or a new §8b
   "consumer-repoint prerequisites") to list every missing name explicitly; "land §8" as written is NECESSARY BUT
   NOT SUFFICIENT.
4. **Add the award-grain selection-equivalence regression test** (P2-8): FRESH-only vs canonical row selection on a
   sample of shared awards; the delta must be exactly the BLOCKER-1 corrections.
5. **Repoint `PRIMETXN_URI` L48**; drop no VARCHAR-massaging idioms — for THIS consumer the type shift is a no-op
   (P2-6), the only change is that the 76 formerly-missing columns now resolve.

### Compatibility risks
- **Type shift is SAFE** (P2-6): every consumer date/double is `TRY_CAST`-wrapped → no-op on native types.
- **Semantic shift is real but desired** (P2-8): award-grain "latest txn" becomes the reconciled winner; gate on the
  equivalence test.
- **Socio idiom already bi-representational** (P1-8): no normalization correctness gate.

### Quantified completeness gain (the reason to migrate)
- Active awards: **365,572 (BULK-backed canonical) vs 171,257 (FRESH-only today)** — 2.13x.
- Active awards carrying a subcontracting plan: **79,088 (BULK) vs 28,044 (FRESH-only)** — 2.82x, i.e. **+59,381
  additional still-active awards with a subcontracting plan** on the single measurable GTM signal the table exists to
  unlock. **Unreachable until `subcontracting_plan_code` lands (P0-6).** Caveat: BULK lacks `potential_end`, so its
  active-award count is a lower bound on that dimension until the FRESH/archive potential_end lands (P0-5).

---

## 6. Recommended build sequencing (dependency-ordered)

1. **DECIDE P0-1 (archive-correction routing).** Everything downstream (rows_out centerline, canonical_source
   distribution, ledger `archive_contributed`) depends on whether archive enters the precedence probe. Nothing else
   is re-baselineable until this is chosen. If "corrections must land," implement the 3-way `MAX(last_modified_date)`
   with the PK-disjointness-preserving fold (NOT a naive anti-join drop).
2. **P1-3 (delete `bl_probe`) + P2-2 (delete `canonical_out`/`bulk_final`) + P1-4 (free-as-you-go DROPs + scratch
   guard).** Out-of-core hygiene — do before any full build; independent of P0-1's outcome.
3. **P1-1 re-baseline §7 numbers** from the disjoint key-universe arithmetic, AFTER P0-1. Then **P1-2 wire the
   missing verify gates** (spot_join, delta_d_keys, true_tail, reinstatement) + **P2-1 wire `delta_lmt`**.
4. **P0-4 (scope tombstone to latest snapshot / gate reinstatement)** — required before any 2nd monthly cycle.
5. **P1-7 (ledger columns + index `_record_run`) + P2-3 (index RAM guard) + P1-5 (verify memory).**
6. **P0-2 (Trigger.dev task chaining build→index→verify) + P0-3 (concurrency guard).** Cadence lands only after the
   correctness (P0-1, P0-4) and audit (P1-7) work is in.
7. **Consumer-repoint track (parallelizable after step 1):** P0-5 + P0-6 + P1-8 land the columns; then the
   schema-superset test (§5 step 3), the award-grain equivalence test (§5 step 4, P2-8), P1-6 (pop_zip5), and finally
   repoint `PRIMETXN_URI`.
8. **Subaward canonical (§4)** — fully independent, separate task, any time.
9. **Documentation-only:** P2-4 (cadence cost in §5), P2-5 (§8 two-bucket cost), P2-6 (type-shift-is-safe note),
   P2-7 (crosswalk rename note).
