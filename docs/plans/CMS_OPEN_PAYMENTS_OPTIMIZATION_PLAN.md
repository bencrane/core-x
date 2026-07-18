# CMS Open Payments — Optimization Plan

**Status:** Optional polish on three **healthy** datasets. No mandatory tier. Gate future work on operator review.
**Supersedes:** `docs/plans/CMS_OPEN_PAYMENTS_REMEDIATION_PLAN.md` (**DELETED** — see Appendix A for why; it was built on a dead premise and cited code that no longer exists).
**Built on:** `docs/analysis/cms_open_payments_structural_diagnostic.md` (current live state, 2026-06-06).
**Scope:** `pipelines/cms_open_payments/ingest.py` is the only file with potential logic changes. **Zero data mutation is required by this plan; every item is optional.**
**Credentials:** all R2 / Postgres access via `doppler run --project core-x --config prd -- <cmd>` (`R2_ENDPOINT`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `HQX_DB_URL_POOLED`). Modal worker uses secrets `r2-credentials` + `hqx-postgres`. Never persist secret values.

---

## 0. Posture — why this plan has no P0

The CMS Open Payments data plane is **structurally complete and query-ready.** Measured live (full detail in the diagnostic):

| | general | research | ownership |
|---|---|---|---|
| Rows | 82,290,893 | 5,936,454 | 27,480 |
| Tombstones | 0 | 0 | 0 |
| Indices (all cover 100% of rows) | 10/10 | 10/10 | 8/8 |
| Format / version | v2.1 / 17 | v2.1 / 17 | v2.1 / 15 |
| Structural verdict | **HEALTHY** | HEALTHY | HEALTHY |

The two things that *were* P0 in the prior plan are **done**:

1. **Publish hardening — SHIPPED (PR #213 `3858f07` + PR #224 `4cbc7ee`).** The destructive `_replace_r2_prefix` (wipe-then-reupload) is gone from this worker. It is replaced by `_publish_full_swap` (stage → verify == local → swap, manifest-LAST) and `_publish_incremental` (append-only, never wipe), each gated by `_verify_published` (reopen fresh from R2; assert rows + full index plan + a BTREE point-probe before recording success). The corpse-producing failure mode is structurally eliminated.
2. **General restore — DONE.** Re-ingested from CMS this session; run #101 `refresh_all=success`, `publish`+`verify` both `success` @ 82,290,893 rows. R2 holds a clean, complete object set with no orphans and no `__staging` debris.

**Everything below is optional logical hygiene on healthy datasets.** None of it blocks a query. None of it is a storage reclamation (Lance v2.1 already compresses the ballast to ~0 on disk). Sequence by blast radius; ship any subset, or none. **The single decision that gates the column-dropping items is the architectural fork in §1 — resolve it first.**

---

## 1. The one decision that must precede any column work — the vintage fork

`payment_publication_date` (1 distinct value, `2026-01-23`), `program_year`, `source_file`, `source_url`, `ingested_at` are **dead weight only under the current single-snapshot `overwrite` model.** The worker is scheduled **quarterly** (`src/trigger/cms_open_payments.ts`) to catch CMS's annual republish + rolling late submissions. Two mutually exclusive models:

- **Model A — single current vintage (what the code does today).** Each `refresh_all` is a from-scratch rebuild that replaces the whole family with the latest CMS publication. Then `payment_publication_date` is genuinely constant and demotable to dataset metadata; `program_year` is redundant with `payment_year`. Column drops in §3 are safe.
- **Model B — vintage accumulation.** If the SoR is meant to retain historical CMS publications (so a consumer can see what 2022 looked like as published in 2024 vs 2026), then `payment_publication_date` becomes the **vintage discriminator** and MUST stay (and likely wants a BITMAP). Column drops that touch it are wrong.

**The current code implements Model A** (`refresh_all` does `shutil.rmtree(local_ds)` then a clean rebuild + `_publish_full_swap`). Unless the operator wants Model B, proceed as Model A. **Do not drop `payment_publication_date` without an explicit Model-A confirmation.** This fork is a one-line operator decision; it costs nothing to leave both columns in place (Lance compresses the constant to ~0), so **the safe default is: keep them, change nothing, and only drop under an explicit Model-A ruling.**

---

## 2. Engineering decisions (baked in — including what is deliberately NOT done)

**D1 — Publish is already correct. No change.** `_publish_full_swap` / `_publish_incremental` / `_verify_published` are the hardened, non-destructive, verified publish. The prior plan's entire "build `_publish_dataset`, delete `_replace_r2_prefix`" spec is obsolete (Appendix A, item 1).

**D2 — Giants stage on `/tmp`, not a Modal Volume. No change.** Proven this session: a Volume's FUSE rejects Lance's commit `rename()` with `EPERM` (ledger ids 76/77/89/90). `/tmp` (overlay) accepts it and held the full 82.29M-row write. The prior plan's D2 ("Volume-staged with per-year checkpointing") is infeasible for a `write_dataset` rebuild (Appendix A, item 2). Spot-preemption recovery is `retries=3` + the idempotent, read-back-verified publish — already in place.

**D3 — `ephemeral_disk=524288` is the floor, not a knob. No change.** Modal hard-rejects requests below 512 GiB at deploy. The prior plan's "lower to 128 GiB" is infeasible (Appendix A, item 3).

**D4 — Keep the dynamic full-fidelity projection. Do NOT hardcode-drop dead columns.** The per-file dynamic projection (`_projection`) is a deliberate robustness invariant — it survives any CMS schema change and auto-adopts new columns/years. Dead columns cost ~0 on disk (Lance crushes nulls), and CMS may populate PI/drug groups in future years. A static drop-list would silently discard future real data for no benefit. If dead-column pruning is ever wanted, do it **dynamically** — "drop columns that are 100% null across all landed years, computed at publish time" — never a hand-enumerated list. (Default: do nothing; the 256/95 column counts are a legibility nit, not a cost.)

**D5 — Do NOT recast types for storage** (`record_id`→int64, categoricals→dictionary). Lance v2.1 already dictionary/RLE-encodes physically (2.99×–5.19× measured). The query win is the BITMAP indices, which already exist. `record_id` stays VARCHAR for consistency with the all-VARCHAR leading-zero-safety posture (NPIs especially must never be numeric). A marginal column narrowing is not worth cross-consumer ripple. (`record_id`→int64 remains a *flagged* option in §3 only because it is the one type with no leading-zero risk; it is opt-in, not recommended.)

**D6 — Do NOT cluster/`ORDER BY` the resolution key by default.** A BTREE point lookup already resolves to exact row addresses (measured — see the pushdown table in the diagnostic §4.2); the multi-fragment spread is because an NPI genuinely recurs across year-fragments (those rows must be read regardless). Global clustering would force a full external re-sort of the 82M-row giant on every quarterly refresh to buy negligible fragment-pruning at 83 fragments. The append-per-year topology is correct for this access pattern. Listed in §3 as an *experiment to measure first*, not a prescription.

**D7 — Sentinel-nulling is conditional on the measured probe.** The prior plan's D5 hinged on "1.2M `'N/A'` rows in `associated_*_pdi_*`". That claim was made against the **corpse sample** and must be re-validated against the restored data (the diagnostic §4.2 sentinel probe). Apply sentinel-nulling **only if** the probe shows a material literal-sentinel count, and **only** on the device/drug-ID columns — never a global `nullif(x,'N/A')` (it would corrupt legitimate free-text fields like `contextual_information` / `name_of_drug_or_biological_…`).

**D8 — Temporal + geography hygiene are the highest-value optional items** (they fix measured data-truth defects: the `0002-11-30` floor in *both* general and research, and the 60–69-NDV state pollution). Field-level, row-preserving, no schema change.

**D9 — Blast-radius isolation preserved.** Any rebuild is per-family through the existing hardened publish; the prior version stays live + readable until the new manifest commits and the read-back gate passes. A failed rebuild leaves the previous good version live.

---

## 3. Optimization items (sequenced by blast radius; each optional, each isolated)

All transform edits land in `_projection` / `_build_sql` in `pipelines/cms_open_payments/ingest.py` and take effect on the next per-family rebuild (`backfill --only-family <f>` → re-ingest → reindex → `_publish_full_swap` → read-back verify). All are **row-preserving** (never drop a row — a dirty field does not invalidate a real payment record, and dropping rows would break the `payment_year` idempotent-replace contract).

### Tier A — index parity (cheapest; index-only, no data rewrite, no transform edit)
- **A1 (optional): add BITMAP `form_of_payment_or_transfer_of_value` to research** for cross-family parity (general indexes it; 5 NDV, ideal BITMAP fit). Edit: append the column to `FAMILIES["research"]["bitmap"]` **and** bump `EXPECTED_INDEX_COUNT["research"]` 10 → 11 (the import-time assertion enforces this). Ship via `reindex_family research` — touches only `_indices/` + manifest, never data fragments.
  - **Blast radius:** research only; index-only; +1 small index file. **Rollback:** remove the column + revert the constant + `reindex_family research`.

### Tier B — temporal + geography hygiene (highest data-truth value; field-level, row-preserving)
Requires a per-family **re-ingest** rebuild (the fix is in the projection, so it applies as CSVs are re-read).
- **B1 — sanitize `date_of_payment`** (general + research; the `0002-11-30` floor is confirmed in both). Wrap the existing cast so implausible dates (`< DATE '2013-01-01'`, Open Payments inception) become NULL rather than a poison zone-map floor:
  ```python
  # in _projection, for alias == "date_of_payment":
  cast = "TRY_CAST(TRY_STRPTIME(nullif(trim({q}),''),'%m/%d/%Y') AS DATE)".format(q=q)
  expr = f"CASE WHEN ({cast}) >= DATE '2013-01-01' THEN ({cast}) END"
  ```
- **B2 — normalize geography** (`recipient_state`, `*_license_state_code*`, `state_of_travel`): `upper(trim(...))`, map full state names → USPS via a small static dict in SQL `CASE`, leave already-valid codes, NULL non-US. Collapses 60–69 → ~51 clean codes (+ military `AA`/`AE`/`AP` retained as valid). Pure SQL chain in the projection; no row drops.
  - **Blast radius (B1+B2):** per-family rebuild; data fragments rewritten (fresh lineage) but published via the verified swap → prior version stays live until verify passes. **Rollback:** `git revert` the projection edit + `backfill --only-family <f>` off the prior transform.

### Tier C — sentinel nulling (RESOLVED by the §4.2 probe — **C1 fires**)
- **C1 — null the device-PDI sentinel.** The §4.2 probe (now measured against the restored 82.29M-row general) **resolves the condition**: **27,346,208 literal `'N/A'` rows (33.2% of the table)** on *every* `associated_device_or_medical_supply_pdi_*` slot, and **0** on `associated_drug_or_biological_ndc_*`. So C1 fires, scoped to the device-PDI group; the NDC prefix stays in the list for robustness but is a **measured no-op**. Targeted prefix match only:
  ```python
  _SENTINEL_PREFIXES = ("associated_device_or_medical_supply_pdi_",
                        "associated_drug_or_biological_ndc_")
  # for an alias matching a sentinel prefix:
  expr = "nullif(nullif(nullif(trim({q}),''),'N/A'),'NA')".format(q=q)
  ```
  **Never** apply globally (would corrupt `contextual_information`, `name_of_drug_or_biological_…`, free-text). The prior plan's "~1.2M" sentinel figure was a corpse-sample undercount by ~23× (actual: 27.35M); the prior D5's inclusion of the NDC group was also wrong (it carries 0 sentinels). The device-PDI nulling is the only real change here.
  - **Blast radius:** general (+research if present); field-level; folds into the B-tier rebuild for free. **Rollback:** revert the projection edit + rebuild.

### Tier D — schema legibility (lowest value; gated on the §1 fork; default = do nothing)
- **D1 (Model A only): dynamic dead-column drop.** Only after an explicit Model-A ruling (§1). Implement as a **publish-time dynamic** "drop columns 100% null across all landed years" (NOT a static list — D4). Demote `payment_publication_date` to metadata; drop `program_year` (keep typed `payment_year`); drop `delay_in_publication_indicator` (1 value). Win is scan width + schema legibility only (~0 disk). **Default recommendation: do nothing** — the column ballast is free and the dynamic-drop machinery is new surface area for a legibility-only gain.
  - **Blast radius:** per-family rebuild; schema narrows (breaking for any consumer that `SELECT`s a dropped column). **Rollback:** revert + rebuild restores the full schema.

### Tier E — clustering experiment (do NOT ship blind; measure first)
- **E1 (experiment, not a prescription): resolution-key clustering.** `ORDER BY covered_recipient_npi` (general) / `principal_investigator_1_npi` (research) before write tightens per-fragment NPI zone maps so by-NPI lookups *might* prune whole `.lance` files. **But** measured pushdown already resolves NPI to exact row addresses; the only gain is fragment-skipping, and the cost is a full external re-sort of the 82M-row giant on **every** quarterly refresh (it fights the append-per-year topology). **Action:** if pursued at all, first run a one-off A/B on a staged copy (bytes_read with vs without clustering for a representative NPI lookup); ship only if the fragment-pruning gain is material. Default: **do not pursue** — D6.

---

## 4. Sequencing & change map

```
(nothing is mandatory)
Tier A  (index parity)         ── reindex_family research; index-only, instantly reversible
Tier B  (date + geo hygiene)   ── per-family rebuild; highest data-truth value
Tier C  (sentinel null)        ── ONLY if §4.2 probe warrants; folds into Tier B rebuild
§1 fork resolved (Model A?)    ── operator decision; gates Tier D
Tier D  (dynamic dead-col drop)── Model A only; legibility-only; default skip
Tier E  (clustering)           ── measure first; default skip
```

| File | Change | Tier |
|---|---|---|
| `pipelines/cms_open_payments/ingest.py` | append `form_of_payment_…` to `FAMILIES["research"]["bitmap"]`; bump `EXPECTED_INDEX_COUNT["research"]` → 11 | A |
| `pipelines/cms_open_payments/ingest.py` | `_projection`: `date_of_payment` `>= 2013-01-01` guard; `recipient_state` / license-state / travel-state USPS normalization | B |
| `pipelines/cms_open_payments/ingest.py` | `_projection`: sentinel `nullif` on `_pdi_*` / `_ndc_*` prefixes (conditional) | C |
| `pipelines/cms_open_payments/ingest.py` | publish-time dynamic 100%-null drop; demote `payment_publication_date` (Model A only) | D |
| `pipelines/cms_open_payments/ingest.py` | `ORDER BY` resolution key pre-write (experiment) | E |

No changes outside `pipelines/cms_open_payments/`. `ops.cms_open_payments_runs` schema is unchanged. Tiers A–C/E are model-agnostic; only Tier D depends on the §1 fork.

---

## 5. Global rollback & safety

- Every tier is code-only in the transform/registry; `git revert` the edit + (for B/C/D/E) `backfill --only-family <f>` restores prior behavior.
- The **publish is no-wipe + manifest-LAST + verify-gated**, so at no point does a partial failure produce an unreadable dataset — the prior good version stays live until the new manifest commits and `_verify_published` passes. This invariant (the fix that ended the corpse era) holds across all tiers.
- Per-family isolation: a rebuild of one family never touches another's prefix.

---

## 6. Explicitly out of scope (rejected with rationale)

- **Re-restore / re-ingest general** — it is whole (82,290,893 rows, 10/10 indices, verified). Nothing to fix.
- **Publish hardening / `_publish_dataset` / delete `_replace_r2_prefix`** — already shipped under different names (Appendix A.1).
- **Modal-Volume giant staging / per-year Volume resume** — infeasible (`EPERM` on commit rename; Appendix A.2/A.4).
- **Lower `ephemeral_disk` below 512 GiB** — hard-rejected by Modal (Appendix A.3).
- **`LANCE_MEM_POOL_SIZE` on the current config** — inert while `LANCE_BYPASS_SPILLING` is present; setting it now is cargo-cult (diagnostic §4.1, Appendix A.6). It is the correct lever **only** on a future spill-to-disk index path.
- **Compaction** — 0 tombstones, optimal fragment topology on all three.
- **Type recasts for storage / global `nullif('N/A')` / hardcoded dead-column lists** — D4/D5/D7.
- **Static resolution-key clustering** — D6/E1 (measure first; negligible expected gain vs full quarterly re-sort).

---

## Appendix A — Why the prior remediation plan was retired (adversarial teardown)

The retired plan (`docs/plans/CMS_OPEN_PAYMENTS_REMEDIATION_PLAN.md`, **now deleted**) was authored against pre-PR-#213 code and a since-falsified diagnostic. Every claim below is verified against the shipped `pipelines/cms_open_payments/ingest.py` on `main` (HEAD `2cb4538`), the git history, the live R2 state, and the `ops.cms_open_payments_runs` ledger. Offending text is quoted. The operator's instruction was explicit: the stale plan "is only going to confuse any future agents… it should not 'amend' stale data" — hence deletion, with this teardown preserved so the death is documented.

**Confirmed/refuted against the assignment's ammunition (items 1–6):**

1. **CONFIRMED — the central Phase-0 fix was already shipped; the whole §3 is moot.** The plan's §3.3 prescribes building a new `_publish_dataset` to "replace `_replace_r2_prefix`", and §7's change map lists "delete `_replace_r2_prefix`; repoint 3 callers." **This work shipped in PR #213 (commit `3858f07`)** under different names: `_publish_full_swap` (stage→verify==local→swap, manifest-LAST) and `_publish_incremental` (append-only, never wipe), gated by `_verify_published` (PR #224, `4cbc7ee`). In the current worker, `_replace_r2_prefix` **exists only as a word in a comment** (`ingest.py:461`, "The retired `_replace_r2_prefix`…") — the function is gone. The plan's `_upload_file_with_retry` / `_remote_index` / `_publish_dataset` / `_verify_published` specs all describe already-shipped behavior. **Every dead line citation in §3.5/§7:** `ingest.py:431`, `:436–451`, `:444`, `:925`, `:959`, `:1011`, `:1067`, `:1088`, `:848` — none maps to the code it claims (the file is now 1554 lines with an entirely different publish section). A future executor following §3 would re-implement, under a third name, a primitive that already exists — pure waste and a merge hazard.

2. **CONFIRMED — D2 "Volume-staged" is infeasible for a `write_dataset` rebuild.** The plan's D2: *"Stage general's local dataset on a **Modal Volume** (`modal.Volume`), not large `ephemeral_disk`… the Volume persists across container restarts so `resume=True` skips already-landed years."* **Measured false this session.** Lance's dataset commit performs an atomic `rename()`; a Modal Volume's FUSE layer rejects it with `EPERM` — the ledger carries the exact error at ids **76, 77, 89, 90**: `LanceError(IO): … Unable to rename file: Operation not permitted (os error 1), …/lance-table/src/…`. Giants stage on the `/tmp` overlay (which accepts the rename and held the full 82.29M-row write); `/dev/shm` (tmpfs, rename-safe) is too small at 16 GiB. The plan inverts the actual constraint — it routes the giant to the one filesystem that **cannot** hold it.

3. **CONFIRMED — "lower `ephemeral_disk` 524288 → 131072 (128 GiB)" is infeasible.** The plan's §3.6/D2: *"Lower `refresh_all` `ephemeral_disk` from 524288 → e.g. 131072 (128 GiB, DuckDB CSV spill only)."* **Modal's `ephemeral_disk` floor is 512 GiB** — a request below 524288 is hard-rejected at deploy. The repo's own history proves it: commit `d711bdb` (PR #22) is titled *"raise ephemeral_disk to Modal's 512 GiB floor."* 512 GiB is the **minimum**, not a tunable ceiling. The prescription would fail `modal deploy` outright.

4. **CONFIRMED — per-year Volume RESUME (D2/O3) is dead with the Volume.** The plan's O3: *"general backfill runs Volume-staged; a mid-run kill + re-run with `resume=True` completes without re-downloading already-landed years"*, and §3.5 adds a `resume` param + `vol.commit()` per year. Since the Volume itself is infeasible (item 2), the entire resume mechanism it depends on is dead. The shipped reality: spot-preemption recovery is `retries=3` (re-run from scratch on a fresh container) + the idempotent, read-back-verified publish — no Volume, no per-year checkpoint, no `resume` flag.

5. **CONFIRMED — §4.3 acceptance references a `_versions != 0` / `--clobber-broken` flow that does not exist.** The plan's §4.1 pre-flight gates on *"If `_versions != 0` → STOP"* and §4.2 wires *"a `--clobber-broken` local-entrypoint flag wired to allow_clobber on the general publish."* The shipped `_publish_full_swap` **deletes-at-swap unconditionally** (after staging + verifying the replacement) — there is no `allow_clobber`, no `--clobber-broken` flag, no `_versions == 0` guard anywhere in `ingest.py`. The restore that actually happened used plain `backfill --only-family general` (no clobber flag); `refresh_all` already `rmtree`s and rebuilds from scratch. The plan's entire D3 "guarded clobber" decision and its §4.2 command block describe a mechanism that was never built and is unnecessary (the swap is inherently safe).

6. **CONFIRMED the variable is REAL; REFUTED the prior diagnostic's "phantom" claim — and the assignment's instinct to flag it is right.** The companion diagnostic (`docs/analysis/cms_open_payments_structural_diagnostic.md` §4.1, prior version) asserted: *"`LANCE_MEM_POOL_SIZE` is not a knob this stack uses… no fleet worker sets it… Do not introduce a phantom variable."* **Both factual claims are false.** (a) It is a **real Lance/DataFusion variable** — parsed as raw bytes via `s.parse::<u64>()`, sizing the `FairSpillPool` working set on the index-build external sort (raises the ~100 MB/partition default that crashed `lance-format/lance#2650`). `docs/analysis/pdl_companies_structural_diagnostic.md` §4.2 states plainly: *"it is a real Lance variable."* (b) A **fleet worker does set it**: `pipelines/ingest_epa/materialize_epa_history.py` sets `LANCE_MEM_POOL_SIZE = str(24 * 1024**3)` on its `index_image` for the 422M-row spilled BTREE build. **However**, the *operational conclusion* (don't set it on CMS now) is correct **for a different reason**: the CMS image sets `LANCE_BYPASS_SPILLING=true`, and because Lance keys that var on **presence not value**, the `FairSpillPool` is never instantiated — so `LANCE_MEM_POOL_SIZE` is **inert under the current config**. Setting it today would be cargo-cult. It becomes the correct lever **only** if general's BTREE build is moved off the in-RAM bypass to the spill path (see diagnostic §4.1). The new diagnostic states this precisely; the retired one was wrong on the facts even where it reached a defensible end-state.

**Additional defects found (beyond items 1–6):**

7. **The companion diagnostic's foundational premise is dead.** It opens: *"`cms_general_payments` — STRUCTURALLY BROKEN (P0). It is not a dataset; it is a partial-publish corpse: 71 orphaned `data/*.lance` fragments (14.98 GiB), and zero `_versions/` manifests."* General is now **82,290,893 rows, 83 fragments, 10/10 indices, version 17, 0 tombstones** (measured live). Any agent reading the old diagnostic would act on a corpse that no longer exists — the precise confusion the operator wants eliminated.

8. **The "1.2M `'N/A'` sentinel rows" claim (plan D5 / diagnostic §3A) was derived from the corpse sample, not the live data, and is unverified for the restored dataset.** The plan makes sentinel-nulling a Phase-2 deliverable on this basis. The figure came from *"a 1.8M-row sample of its orphaned fragments."* It must be re-measured against the restored 82.29M-row general before any sentinel logic is prescribed (the new diagnostic §4.2 does exactly this). Building a code change on a corpse-sample statistic is unsound.

9. **Stale telemetry throughout the plan.** Plan §0.3: research *"5,936,454 rows… indices cover 100% of fragments (research 10/10→8/8…)"* and the column counts (*"general 95… research 256"*) were correct, but the plan's "rows lost in publish = 12,239,319" / "70,051,574 rows on R2" framing for general is now entirely obsolete (0 lost, 82.29M present). The plan's whole "Current state" §0 is a snapshot of a moment that has been overwritten.

10. **`EXPECTED_INDEX_COUNT` is already shipped — the plan presents it as new work.** Plan §3.4: *"Add `EXPECTED_INDEX_COUNT = {"general": 10, "research": 10, "ownership": 8}`."* This constant **already exists** in `ingest.py` (with an import-time assertion that it tracks the `FAMILIES` registry). Another already-done item dressed as a task.

11. **Plan §3.7 / §4 test+restore choreography is retrospective.** The plan's Phase-0 acceptance tests ("kill-test on ownership", "integration on ownership") and Phase-1 restore were the *process that already ran* to produce the current healthy state (the ledger shows the ownership publish/verify cycles at ids 72/73, 86/87, 109/110 and the general restore at 92–100). Presenting them as forward work would cause a future agent to needlessly re-run a completed restore against a healthy dataset — at best wasteful, at worst a gratuitous 20 GiB rebuild.

**Net:** the retired plan's two P0 objectives are complete; its central code spec targets a deleted function via dead line numbers; three of its baked-in engineering decisions (D2 Volume, D2 ephemeral lowering, D3 guarded clobber) are infeasible or nonexistent against shipped reality; and its companion diagnostic profiled a corpse that no longer exists. It is not salvageable by amendment — it is a snapshot of a problem that has been solved by different means. **Deleted.** The only forward-looking content it held (date-floor sanitization, geography normalization, optional index parity, the dead-column/clustering rejections) is carried forward — re-validated against live data — in this plan's §3.

---

### Appendix B — quick reference
- Diagnostic (current state): `docs/analysis/cms_open_payments_structural_diagnostic.md`
- Worker: `pipelines/cms_open_payments/ingest.py` · Ledger DDL: `pipelines/cms_open_payments/ops_cms_open_payments_runs.sql`
- Hardened publish: `_publish_full_swap` / `_publish_incremental` / `_verify_published` (PR #213 `3858f07`, PR #224 `4cbc7ee`)
- Giant staging constraint: `/tmp` overlay (Volume rejects Lance commit rename, `EPERM`); `ephemeral_disk` floor 512 GiB
- Registry index plan (source of `EXPECTED_INDEX_COUNT`): `FAMILIES` dict, `ingest.py`
- Fleet rules: `ARCHITECTURE.md` (two-tier index rule); `docs/reference/0{1,2}_*`
- `LANCE_MEM_POOL_SIZE` precedent (spill path): `pipelines/ingest_epa/materialize_epa_history.py`; `docs/analysis/pdl_companies_structural_diagnostic.md` §4.2
