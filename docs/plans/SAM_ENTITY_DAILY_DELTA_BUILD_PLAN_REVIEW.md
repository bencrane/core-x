# Adversarial Review — SAM Entity Daily Delta Build Plan

**Reviews:** `docs/plans/SAM_ENTITY_DAILY_DELTA_BUILD_PLAN.md` (merged to `main`, commit `da3491f`, PR #249)
**Reviewer stance:** guilty-until-proven-innocent; every finding carries `file:line`, a quoted doc, a command result, or a query result. Suspicions that could not be proven are labelled **UNVERIFIED / hypothesis**.
**Review date:** 2026-06-06 (~23:00 UTC). SAM `SAM_API_KEY` quota was exhausted (429) this day — **no live entity-extract API call was spent**; the data surface was verified against GSA docs + no-key/invalid-key routing probes only.

---

## Executive verdict

**REWORK** — ship Phase 1 (daily landing) after schema fixes; **Phase 2 (merge into `sam_master_entities`) as written is incorrect and will silently corrupt the master.**

The daily-landing track (Phase 1) is sound in shape and has a real template. The reconciliation track (Phase 2) is built on a false structural premise: `sam_master_entities` carries **five cross-snapshot aggregate columns** that a single-daily-row `merge_insert(...).when_matched_update_all()` cannot reproduce — and the Lance merge primitive the plan specifies performs a **full-row replace**, not a column-wise merge. Applied as written it would reset tenure/activeness for the ~97% of UEIs that have history. Several supporting claims (new provenance columns, the "mirror USASpending two-track" template, the daily-table shape) are also wrong or unverified against the live plane.

### The 5 headline findings

1. **[CRITICAL] Phase-2 merge corrupts 5 aggregate columns for ~97% of the master.** `is_active, first_seen_label, last_seen_label, snapshot_count, ever_inactive` are GROUP-BY-UEI aggregates over *all* snapshots (`sam_master.py:199-206`). `when_matched_update_all` "removes the target row and adds the source row" (lance 7.0.0 docstring) — full replace, no per-column preservation. Live: **1,494,385 / 1,541,566 rows (96.9%) have `snapshot_count > 1`** (avg 7.45, max 14) — all would be reset toward 1 as their UEIs receive daily updates.
2. **[CRITICAL] `is_active` is not derivable from a daily row at all.** It is `bool_or(extract_label = {LATEST monthly label} AND sam_extract_code = 'A')` (`sam_master.py:204`). A daily row's `extract_label` never equals the monthly `{LATEST}` literal, so a re-projected daily row computes `is_active=false` for *everyone*. Proof it is a true aggregate (not winning-row `sam_extract_code='A'`): live query shows **20,998 rows where `sam_extract_code='A'` but `is_active=false`**. The plan's deactivation story (§4.2 reconciliation, "latest-wins flips is_active") is therefore false.
3. **[HIGH] The provenance columns the plan stamps do not exist and are never added.** Plan §4.7 / Phase 2 "stamp `source='daily'`, `valid_as_of`". Live `sam_master_entities` has **68 columns; `source` and `valid_as_of` are both absent** (verified read from R2). Lance `merge_insert` requires the source Arrow schema to match the target; introducing unknown columns fails unless the dataset is first migrated. No DDL/migration step is in the plan.
4. **[HIGH] The cited template overstates what exists.** Plan §2/§4: "the USASpending two-track already exists — `usaspending-daily-delta` Trigger schedule … Mirror its structure." **No such Trigger schedule exists** (`grep -rln` over `src/` returns nothing; `usaspending_bulk.ts` has zero `cron`). The only USASpending daily artifact is the Modal worker `pipelines/usaspending/usaspending_api_landing.py`, which is *explicitly a pure landing with no merge, no dedup, no SoR mutation* (its docstring, lines 8-13). So the **reconciliation half of the "two-track" has no precedent in USASpending** — the part the plan leans on as proven is the part that does not exist.
5. **[HIGH] A PUBLIC daily entity extract may not exist.** GSA docs enumerate daily files as `SAM_FOUO_DAILY_V2_*` and `SAM_SENSITIVE_DAILY_V3_*` only — **no `SAM_PUBLIC_..._DAILY_*` is listed**. The plan assumes a public daily sibling (§3.3, §6 Phase 0) on a same-family-naming inference. This is the single biggest feasibility unknown and must be the first Phase-0 gate; if only FOUO/SENSITIVE dailies exist, the entire "PUBLIC daily" premise (and the current `SI-NONFED` key's access to it) collapses.

### Findings by severity

| Severity | Count | IDs |
|---|---|---|
| Critical | 2 | F1, F2 |
| High | 5 | F3, F4, F5, F6, F7 |
| Medium | 5 | F8, F9, F10, F11, F12 |
| Low | 3 | F13, F14, F15 |

---

## VERIFIED findings

### F1 — [CRITICAL] `merge_insert.when_matched_update_all` full-replaces the 5 aggregate columns; ~97% blast radius
**Target:** Plan §4.3 (daily fast path), Phase 2 merge block (plan lines 111-118).
**Evidence:**
- Aggregate columns are computed GROUP BY uei over every snapshot in `proj` (the whole `entity_registrations` scan):
  `sam_master.py:199-206` —
  ```
  arg_min(extract_label,_snap) AS first_seen_label,
  arg_max(extract_label,_snap) AS last_seen_label,
  count(DISTINCT extract_label) AS snapshot_count,
  bool_or(sam_extract_code IS DISTINCT FROM 'A') AS ever_inactive,
  bool_or(extract_label = {LATEST} AND sam_extract_code='A') AS is_active
  ```
  These join onto the winning row at `sam_master.py:211-213` (`t.is_active, t.first_seen_label, t.last_seen_label, t.snapshot_count, t.ever_inactive`).
- Lance 7.0.0 semantics (installed major; `sam_normalized_entities.py:123`/`sam_pocs.py` pin `pylance>=7`):
  `when_matched_update_all` docstring — *"The rows from the target table will be removed and the rows from the source table will be added."* The optional `condition` only **gates whether** the row updates; it cannot **combine** columns.
- Live blast radius (R2 read of `sam_master_entities`, 1,541,566 rows): `snapshot_count > 1` for **1,494,385 rows (96.9%)**, avg 7.45, max 14. Every one of these is wrong the moment its UEI appears in a daily delta and the upsert fires.
**Why it's a problem:** A matched daily upsert deletes the historically-correct row and inserts a row whose 5 aggregates were derived from one snapshot → `snapshot_count` collapses to 1, `first_seen_label`/`last_seen_label` get clobbered to the daily date, `ever_inactive` loses history. The master silently degrades with every daily run; the existing pre-write gates do NOT catch it (they run inside the monthly `build_sam_master`, never on the merge path).
**Remediation (concrete):** Do **not** use `when_matched_update_all` against `sam_master_entities`. Two correct options:
- **(Preferred) Tenure columns are monthly-rebuild-only.** Split the master into (a) scalar winning-row columns the daily path may touch and (b) the 5 aggregates, which only `build_sam_master` (full union scan) may write. On the daily fast path, drive the merge with an **explicit column list** via `when_matched_update_columns([...scalars only...])` is *not* available in this Lance (builder exposes only `when_matched_update_all`) — so instead build the source Arrow table by **reading the matched target rows back, recomputing the 5 aggregates by folding the daily row into the existing values** (`snapshot_count = target.snapshot_count + (daily_label not already counted)`, `last_seen_label = max(...)`, `ever_inactive = target.ever_inactive OR (daily code≠'A')`, `is_active` recomputed only against the true monthly LATEST — see F2), then `when_matched_update_all` on that fully-correct source row. i.e. the merge source must be the *combined* row, not the raw daily row.
- **(Simplest, lowest-risk) Drop the daily fast path entirely; make the daily delta a freshness *input* to the monthly-style rebuild.** Land daily to its own table (Phase 1), and on each daily fire run `build_sam_master` reading `(entity_registrations ∪ sam_entity_daily)` with `skip_if_current` relaxed to a label/row check. `build_sam_master` already computes all 68 columns correctly from a union; the only cost is the 128 GB scan the plan is trying to avoid. If that cost is unacceptable, the combined-row approach above is mandatory.

### F2 — [CRITICAL] `is_active` cannot be produced from a daily row; deactivation logic in the plan is false
**Target:** Plan §4.2 ("deactivated/expired entities … latest-wins flips is_active"), §7 success criterion ("a UEI present in the daily shows the daily's last_update_date … latest-wins proven").
**Evidence:** `sam_master.py:204` — `is_active = bool_or(extract_label = {LATEST} AND sam_extract_code='A')`, where `{LATEST}` is substituted with the latest **monthly** v2 label (`sam_master.py:449-450`, `_lit` from the `lbl` scan at 416-420). Live disproof that `is_active ≡ winning-row sam_extract_code='A'`: **20,998 rows have `sam_extract_code='A'` yet `is_active=false`** (R2 query). Those are UEIs whose latest *row* is active but who were not present-and-active in the exact latest monthly snapshot.
**Why it's a problem:** If the daily worker re-projects via `build_sql`, `{LATEST}` becomes the *daily* label → for a merged daily row `is_active` would reflect the daily snapshot, which is a *different definition* than every monthly-derived row in the table → the column becomes semantically mixed and the `is_active` BITMAP in `sam_normalized_entities` (built off this column, `sam_normalized_entities.py:69`) inherits the inconsistency. "Latest-wins flips is_active" is simply not how the column is computed — there is no `is_active` flip; it is recomputed from a monthly-anchored predicate.
**Remediation:** Define `is_active` semantics explicitly for the two-track world. Recommended: keep `is_active` anchored to the **latest monthly** snapshot (rebuild-only), and add a **separate** daily-sourced column, e.g. `daily_extract_code` / `is_active_daily` = `sam_extract_code='A'` on the most recent daily row, so surety GTM gets fresh active/deactivated signal without redefining the monthly-anchored `is_active`. Deactivation detection for the surety feed reads `sam_extract_code != 'A'` (or `'E'`/expiry) on the daily row directly — not a master flip.

### F3 — [HIGH] `source` / `valid_as_of` columns do not exist and no migration adds them
**Target:** Plan §4.7, Phase 2 ("Stamp `source='daily'`, `valid_as_of`").
**Evidence:** R2 read of `sam_master_entities` → **68 columns**, full list captured in the verification log; `source` absent, `valid_as_of` absent. Lance `merge_insert` requires source schema ⊆ target schema; a source Arrow table carrying `source`/`valid_as_of` against a target lacking them errors at execute.
**Why it's a problem:** The plan's auditability mechanism is unbuildable as written; an executor following it hits a schema-mismatch failure.
**Remediation:** Add a one-time schema migration before Phase 2: rebuild `sam_master_entities` once via `build_sam_master` with the two columns added to `entity_select` (`sam_master.py:210-214`) defaulted to `source='bulk'`, `valid_as_of = sam_extract_label`. Lance overwrite (the master's write mode, `sam_master.py:513`) handles the column add cleanly. Only then can the daily merge stamp them.

### F4 — [HIGH] "USASpending two-track already exists / mirror its structure" — the reconciliation half does not exist
**Target:** Plan §2 (bullet "Template to clone"), §4.
**Evidence:** `grep -rln "usaspending-api-landing|usaspending-daily|daily-delta|api_landing" src/` → **no matches**. `usaspending_bulk.ts` contains no `cron`. `pipelines/usaspending/usaspending_api_landing.py:8-13`: *"This is NOT a delta and computes NO delta … No merge into any SoR, no scalar index on any SoR, no watermark/state."*
**Why it's a problem:** The plan presents its hardest, unproven component (merge into a faithful master with aggregates) as a clone of an existing pattern. It is not; the existing pattern deliberately *avoids* merging into a SoR. This is the same class of error the plan was burned by twice (asserting an unverified channel as fact).
**Remediation:** Re-anchor the template citations. The **correct** in-repo precedents for delta `merge_insert` are `pipelines/uspto_tm/ingest.py:870-872` and `pipelines/edgar/ingest.py:806-808` — but cite them with the caveat that **their target tables have no cross-snapshot aggregate columns**, which is exactly why `when_matched_update_all` is safe there and unsafe for `sam_master_entities` (F1). For the daily *landing* task, the accurate template is `usaspending_api_landing.py` (landing only) + `src/trigger/sam_opps_bulk.ts` (the Trigger cron/dispatch shape, `cron: "0 12 * * *"`, dispatch via `app_name`/`function_name`).

### F5 — [HIGH] Public DAILY entity extract existence is unverified; GSA lists only FOUO/SENSITIVE dailies
**Target:** Plan §3.1, §3.3 ("daily sibling expected `SAM_PUBLIC_..._DAILY_V2_YYYYMMDD.ZIP`"), §5 ("Forward daily feed … fits the 10/day cap … No blocker").
**Evidence:** GSA Open SAM Entity Extracts API page (fetched 2026-06-06) enumerates daily ASCII files as `SAM_FOUO_DAILY_V2_YYYYMMDD.ZIP` and `SAM_SENSITIVE_DAILY_V3_YYYYMMDD.ZIP` (+ exclusion dailies). The documented daily **example request uses `sensitivity=FOUO`** (`…&fileType=ENTITY&sensitivity=FOUO&frequency=DAILY&date=04/07/2022`). No `SAM_PUBLIC` daily entity file is listed; monthly lists `SAM_PUBLIC_MONTHLY_V2`.
**Why it's a problem:** "Forward feed = no blocker" assumes a PUBLIC daily exists and is reachable by the `SI-NONFED` key. If daily entity extracts are FOUO/SENSITIVE-only, the public path the whole plan rests on may not exist; FOUO/SENSITIVE require entitlement (and SENSITIVE requires POST + Basic Auth per the docs).
**Remediation:** Make Phase-0 gate #1 a single authenticated call **after quota reset**: `fileType=ENTITY&sensitivity=PUBLIC&frequency=DAILY&date=<recent Tue–Sat>`. Record HTTP status + filename. If 404/empty, pivot: either (a) obtain FOUO entitlement on the SAM account, or (b) accept monthly-only freshness. Do not build Phase 1 until this returns a real ZIP. (This is in the plan's Phase 0, but it is buried as one bullet among six; it is THE gate and should block all build.)

### F6 — [HIGH] Daily-table shape omits columns the master/POC scans require → schema drift
**Target:** Plan §4.2 ("Daily table = raw SoR shape … `uei, extract_label, source_file, pipe_fields, format_family` + a `pull_date`").
**Evidence:** Live `entity_registrations` schema (R2 read) = **18 columns**: `uei, duns, cage_code, registration_status, purpose_of_registration, registration_date, expiration_date, last_update_date, activation_date, legal_business_name, dba_name, pipe_fields, field_count, format_family, source_encoding, extract_label, source_file, ingested_at`. The master scans `["uei","extract_label","source_file","pipe_fields"]` filtered on `format_family='v2'` (`sam_master.py:443-445`); `build_sql` width logic and `sam_pocs.build_pocs_sql` both key on **`field_count`** (`sam_pocs.py:243-244`, 312-314). The plan's 6-column daily shape omits `field_count` and the projected scalars.
**Why it's a problem:** If a daily-rebuild union or POC rebuild ever reads the daily table, missing `field_count`/`format_family` parity breaks the width classifier and the POC base-position CASE. The two raw tables must be schema-compatible to be unionable (the plan's own §4.3 "re-baseline reading (monthly ∪ daily)" depends on it).
**Remediation:** The daily table must emit the **same 18-column schema** as `entity_registrations` (reuse `entity_registrations_bulk._build_sql` verbatim, which already produces all 18 incl. `field_count`, `format_family`, `source_encoding`, `ingested_at`), plus `pull_date`. Do not hand-author a reduced 6-column shape.

### F7 — [HIGH] Latest-wins tiebreak chain is not replicable by `merge_insert(on="uei")`
**Target:** Plan §4.3 merge `condition="source.last_update_date >= target.last_update_date"`.
**Evidence:** The master's dedup is a 4-key ORDER, not a single comparison: `sam_master.py:191-194` —
`row_number() OVER (PARTITION BY uei ORDER BY last_update_date DESC NULLS LAST, initial_registration_date DESC NULLS LAST, {snap_key} DESC, source_file DESC NULLS LAST)`. `merge_insert`'s `condition` is a single boolean predicate; it cannot express the `initial_registration_date → snap_key → source_file` cascade. `last_update_date` IS a column on the target (verified — date32), so the predicate is at least runnable, but it is incomplete.
**Why it's a problem:** On `last_update_date` **ties** (common — many SAM rows share an update date) and on **NULL** `last_update_date` (`>=` is false when either side is NULL → a NULL-dated daily row never wins, and a daily update to a NULL-dated existing row never applies), the merge diverges from what a full rebuild produces → the daily fast path and the monthly re-baseline disagree, violating the plan's own "both paths consistent" claim (§4.3) and the §7 re-baseline gate.
**Remediation:** Pre-collapse the daily delta to one row per UEI using the **exact** `latest_sql` ordering before merge (the plan already projects via `build_sql`; reuse its `latest` CTE on the daily input). For the merge predicate, replicate the full tiebreak as a compound condition: `source.last_update_date > target.last_update_date OR (source.last_update_date = target.last_update_date AND source.initial_registration_date > target.initial_registration_date) OR (... AND source.source_file > target.source_file)`, and handle NULLs explicitly with `coalesce(last_update_date, DATE '1900-01-01')`. Add a parity test (§7) that diffs a daily-merge result vs a full union-rebuild on a fixed fixture and asserts zero row-level divergence.

---

## VERIFIED findings — Medium

### F8 — [MEDIUM] `sam_master_contacts` is not derived from the master; the plan's Phase-2 step is misdirected
**Target:** Plan §1 goal 3 ("derived `sam_master_contacts`"), Phase 2 ("Re-derive affected UEIs into `sam_normalized_entities` and `sam_master_contacts` (merge_insert into those too)").
**Evidence:** `sam_pocs.py:69` reads `SAM_SRC_URI = entity_registrations` (the **raw** SoR), not `sam_master_entities`; its grain is 1 row per (entity, POC slot) unpivoted from `pipe_fields` (`sam_pocs.py:236-288`). `sam_master.py` produces `sam_master_contacts` from the same raw scan in the monthly build (`sam_master.py:218-224`), keyed `uei` but **multi-row per uei** (≤6). There is no `sam_master_contacts` that is a 1:1 derivative of `sam_master_entities`.
**Why it's a problem:** "merge_insert into `sam_master_contacts` keyed on uei" is wrong — uei is not unique there (up to 6 rows/uei), so an `on="uei"` upsert would delete 5 of 6 POC rows per matched uei. And `sam_pocs` doesn't read the master at all, so "re-derive affected UEIs" from the master is impossible for the POC layer.
**Remediation:** For contacts, the daily path must merge on the **composite POC key** (`uei, poc_type`) or simply rebuild the affected UEIs' POC rows from the daily `pipe_fields` via `build_pocs_sql`. Better: keep `sam_master_contacts`/`sam_pocs` on the monthly cadence (they are POC-stable; new-registrant POCs are a smaller value-add than the entity signal) and scope Phase 2 to `sam_master_entities` + `sam_normalized_entities` only.

### F9 — [MEDIUM] `sam_normalized_entities` hard gate `rows == src_count` will fight a growing master
**Target:** Plan §1 goal 3, Phase 2 ("Re-derive … `sam_normalized_entities`").
**Evidence:** `sam_normalized_entities.py:253-255` gates `rows == src_count` and `distinct_uei == rows` against a full re-scan of `sam_master_entities` (`SRC_URI`, line 48). It is an **overwrite** rebuild (line 451), and `skip_if_current` compares snap-keys on `sam_extract_label` (lines 405-422).
**Why it's a problem:** (a) The sidecar's `skip_if_current` keys on `max(sam_extract_label)`; daily merges that change row *content* but not the max monthly label will **not** advance the snap-key → the sidecar self-skips and goes stale relative to daily-updated master rows (its self-healing assumption in `sam_spine_refresh.ts:10-16` breaks for daily deltas). (b) A `merge_insert` "into the sidecar" as the plan suggests is incompatible with its overwrite+full-gate design.
**Remediation:** Either (a) advance a daily-aware watermark the sidecar can see (e.g. add `valid_as_of`/`max(last_update_date)` to the sidecar's skip check), forcing a sidecar rebuild after any daily merge; or (b) since the sidecar is a cheap 8-column overwrite (its own docstring), just **always rebuild it after a daily merge** — drop the merge-into-sidecar idea entirely. Do not `merge_insert` the sidecar.

### F10 — [MEDIUM] Idempotency of daily landing is asserted, not designed
**Target:** Plan §4.5 (rolling 4-day window, "dedup on (uei,last_update_date)"), §7 ("re-running the same window is idempotent").
**Evidence:** Plan §6 Phase 1 lands via `lance.write_dataset(..., mode="append")` ("append to `sam_entity_daily`"). Append is **not** idempotent: re-running an overlapping window re-appends the same (uei, label) rows. The plan's mitigation is "dedup the rolling window before append (or rely on the master merge)" — but in-window dedup does not prevent **cross-run** duplication (yesterday's run already appended date D; today's window includes D again).
**Why it's a problem:** The daily table accumulates duplicate (uei, extract_label) rows run-over-run; the §7 idempotency gate fails; downstream union-rebuild double-counts unless every consumer re-dedups.
**Remediation:** Make the daily table **`merge_insert(on=["uei","extract_label"])`** (it is its own table — full-row replace is correct here, no aggregates) instead of blind append; or partition the landing by `pull_date`/`extract_label` and write each date's slice with `mode="overwrite"` scoped to that partition (the USASpending landing uses `pull_date=YYYY-MM-DD` prefixes for exactly this, `usaspending_api_landing.py:27`). A ledger label-check (`ops.sam_entity_daily_runs`) should also short-circuit a re-pull of an already-landed date.

### F11 — [MEDIUM] No merge-path gates or rollback; the master's safety net is bypassed
**Target:** Plan §6 Phase 2, §3 Phase 3 ("Port the gate discipline … to … the merge step").
**Evidence:** All correctness gates (`assert_pre_write_gates`, uniqueness `distinct_uei == entities_rows`, write-integrity, index-presence, rollback-to-`v_before`) live **inside `build_sam_master`** (`sam_master.py:250-286`, 498-551) and run only on the overwrite path. `merge_insert` writes a new Lance version with **none** of these. Phase 3 defers porting them — i.e. the first daily merges run ungated.
**Why it's a problem:** The plan calls the uniqueness gate "sacred" (§8) yet the merge path has no uniqueness check, no row-floor, no Δ-guard, no rollback. A bad daily projection (e.g. width misclassification → garbage uei) writes straight to the prod master with no abort.
**Remediation:** Wrap the merge in the same guard skeleton: capture `v_before = ds.version`; after `execute`, assert `count_rows() == count_distinct(uei)` (re-run the master's uniqueness invariant), assert row delta within a sane band, assert indices still present (`merge_insert` can drop/degrade scalar indices — the plan's own §6 "weekly compaction/re-index" acknowledges this), and `lance.dataset(uri, version=v_before).restore()` on any failure. This is non-negotiable for Phase 2, not Phase 3.

### F12 — [MEDIUM] `merge_insert` index degradation + tombstones are real and unscheduled for the daily cadence
**Target:** Plan §6 Phase 2 maintenance bullet ("weekly `optimize.compact_files()` + index re-optimize").
**Evidence:** `merge_insert` builder exposes `use_index` and the master relies on `uei/primary_naics/cage_code` BTREEs (live indices confirmed: `uei_idx, primary_naics_idx, cage_code_idx`). `sam_normalized_entities.py` and `uspto_tm/ingest.py:872` call `_optimize_indices`/`_optimize_indices` after merges precisely because merge writes new fragments that fall outside the existing index until re-optimized.
**Why it's a problem:** A *weekly* compaction against a *daily* merge means up to 7 days of unindexed fragments → degraded `uei` lookups (the master's primary access path) and growing tombstones between compactions. The monthly overwrite "resets this for free" (plan §6) only once a month.
**Remediation:** Re-optimize indices **after every daily merge** (cheap relative to the merge), as `uspto_tm` does (`_optimize_indices` immediately post-merge, `ingest.py:873`). Reserve `compact_files()` for weekly. Add a post-merge gate asserting the BTREEs still cover the new rows (probe a known-present daily uei via the index, mirroring `sam_master.py:530-532`).

---

## VERIFIED findings — Low

### F13 — [LOW] Daily Trigger cron at 12:00 UTC is tight against the 7 AM ET (≈11:00–12:00 UTC) publish + lag
**Evidence:** GSA: daily files "produced every Tuesday-Saturday … after 7 AM Eastern." `sam_opps_bulk.ts:29` fires `0 12 * * *` UTC. 7 AM ET = 11:00 UTC (EDT) but "after 7 AM" + SAM's own 1–2 day publish lag (plan §4.5) means a 12:00 UTC fire often races same-day availability.
**Why it's a problem:** Same-day misses are silent if the worker treats "file not yet present" as success.
**Remediation:** Fire later (e.g. 13:00–14:00 UTC) and/or rely on the rolling 4-day lookback (plan §4.5) so a same-day miss is backfilled next run; explicitly log+alert (not silently skip) when an expected Tue–Sat date is absent.

### F14 — [LOW] "New registrant" trigger field is left open; both candidate fields have failure modes
**Target:** Plan §6 Phase 0 / §9 open decision 2 (`registrationDate` vs `activationDate`).
**Evidence:** Available date columns on the row: `initial_registration_date` (pos 8), `activation_date` (pos 11), `last_update_date` (pos 10) — all present in `entity_registrations` and the master. The daily delta is "new/updated/deactivated/expired since the previous day" (GSA) — so presence in a daily file ≠ new registration (could be any update).
**Why it's a problem:** Filtering "new" by `extract_label`-membership over-counts (includes updates). Filtering by `initial_registration_date` within window risks late-landing registrations whose `initial_registration_date` predates the window. False-positive/negative modes are real.
**Remediation:** Define "new" as `initial_registration_date` within the lookback window **AND** the UEI absent from the prior monthly master snapshot (anti-join on `sam_master_entities.uei`). This is robust to the daily file's mixed update semantics. Surface `activation_date` separately as the "biddable" signal. Block Phase-1 ship on the operator's pick (plan already flags this — keep it blocking).

### F15 — [LOW] Backfill rate-limit math is plausible but rests on the unverified daily-PUBLIC + retention assumptions
**Target:** Plan §3.7 ("~24 daily files"), §5 (backfill levers).
**Evidence:** Plan's own §3.5: "no daily retention window is documented — you cannot discover which dailies are available except by requesting specific dates (each costs quota)." The 24-file estimate assumes every Tue–Sat since 2026-05-03 is fetchable; retention is explicitly unknown.
**Why it's a problem:** If SAM retains only N recent dailies, the backfill silently has holes; the plan correctly defaults to "monthly gap-close" but the 1,000/day role-upgrade lever is sold as a clean unblock when retention may cap it anyway.
**Remediation:** Keep "ingest the 2026-06-07 monthly to close the gap" as the default (plan §5 already does). Treat daily backfill as best-effort only; before pursuing the role upgrade for backfill, spend exactly one quota call to probe a 21-day-old date (plan's optional Phase-0 step) and confirm retention.

---

## Plan claims that VERIFIED as ACCURATE (credit where due)

- **§3.4 open no-key download is DEAD.** Live probe: `https://www.sam.gov/SAM/extractfiledownload?...` → **HTTP 301** redirect into `sam.gov:443/.../error`-class portal. Consistent with the plan's "301→302→error.jsf". Do not reintroduce data.gov links.
- **§3.4/§3.5 data-services endpoint is key-gated and our key is `SI-NONFED` 10/day.** The earlier 429 (cited in the brief) proves the endpoint is live and the key authenticates; no-key and invalid-key probes both return 404 (routing is key-mediated), which is consistent with a gated endpoint. Rate-limit tiers in §3.4 match GSA's published table exactly (non-fed+role 1,000/day; fed-system 10,000/day).
- **§3.1/§3.2 cadence + contents.** GSA verbatim: dailies "produced every Tuesday-Saturday … after 7 AM Eastern"; "incremental files that contain new/updated/deactivated/expired entities"; monthly "all active entities and entities expired in the last 6 months." All accurate.
- **§2 master shape.** Live `ops.sam_master_runs`: prod `sam_master` success, label `20260503`, entities 1,541,566 = distinct_uei, contacts 4,373,319, domains 709,546. Matches the plan's stated counts.
- **§2 sam-spine-refresh deploy/cron state.** Verified: PR #228 (orchestrator **with** cron) deployed as `v20260606.1` at **20:00:54 UTC**; PR #233 (cron removal, source-level) merged at **20:43 UTC** — *after* the deploy. So the **deployed bundle does still carry the declarative cron**, held inert only by the runtime `active=false` override, exactly as the plan states. (Note the live consequence the plan doesn't: redeploying current `main` now would *remove* the cron at the bundle level — fine, but the master refresh would then have no schedule until the event-driven replacement lands.)
- **§4 parser reuse is structurally sound** *given F6* — `entity_registrations_bulk._build_sql` (`entity_registrations_bulk.py:194-262`) is width-driven (142⇒v2), not filename-driven, so a daily file in the v2 142-wide layout parses unchanged. The plan's "confirm width matches in Phase 0" caveat is correct.

---

## Verification log (auditable)

Environment: cwd `/Users/benjamincrane/core-x/.claude/worktrees/keen-buck-1edf00` (worktree of `core-x`). Local `lance`/`duckdb` absent → created `/tmp/lvenv` venv (`pylance 7.0.0`, `pyarrow 24.0.0`, `boto3`, `duckdb 1.x`). R2/Postgres accessed via `doppler run -p core-x -c prd`. Current time at review: ~22:57–23:00 UTC 2026-06-06.

| # | Check | Method | Result |
|---|---|---|---|
| 1 | Plan read in full | `Read` | 171 lines; §1-§10 captured |
| 2 | `sam_master_entities` column production | `Read sam_master.py:170-240` | scalars (winning-row) at 210-214; aggregates `is_active/first_seen/last_seen/snapshot_count/ever_inactive` at 199-206 (GROUP BY uei) |
| 3 | `is_active` definition | `sam_master.py:204` | `bool_or(extract_label={LATEST monthly} AND sam_extract_code='A')` |
| 4 | latest-row tiebreak | `sam_master.py:191-194` | 4-key ORDER: last_update_date, initial_registration_date, snap_key, source_file |
| 5 | Live master schema + indices | `doppler … lance.dataset(...).schema` | **68 cols**; `source`/`valid_as_of` ABSENT; `last_update_date`/`is_active`/`snapshot_count`/`initial_registration_date`/`sam_extract_code` PRESENT; indices `uei_idx, primary_naics_idx, cage_code_idx` |
| 6 | Aggregate blast radius | `doppler … duckdb` over master | total 1,541,566; `snapshot_count>1`: **1,494,385 (96.9%)**; max_snap 14; avg 7.45; `code='A'`: 803,541; `is_active`: 782,543; `code='A' & !is_active`: **20,998**; `is_active & code!='A'`: 0 |
| 7 | `merge_insert` semantics | `/tmp/lvenv … inspect MergeInsertBuilder` | builder methods incl. `when_matched_update_all(condition)`, `use_index`, `execute`; docstring: matched rows "removed … and the rows from the source table will be added" (full replace); condition uses `source.`/`target.` prefixes |
| 8 | Raw `entity_registrations` schema | `doppler … lance.dataset` | **18 cols** incl. `field_count`, `format_family`, `source_encoding`, `ingested_at`; 19,299,314 rows |
| 9 | `sam_pocs` source + keying | `Read sam_pocs.py:69,236-288,312-314` | reads raw `entity_registrations`; ≤6 rows/uei; keys on `field_count`/`format_family` |
| 10 | `sam_normalized_entities` gates | `Read sam_normalized_entities.py:48,69,253-255,405-422,451` | overwrite; `rows==src_count` & `distinct_uei==rows`; skip on `max(sam_extract_label)` snap-key |
| 11 | ops.sam_master_runs | `psql` | prod success label 20260503, 1,541,566=distinct_uei |
| 12 | USASpending template existence | `grep -rln src/`, `Read usaspending_api_landing.py:8-13` | **no daily-delta Trigger schedule**; api_landing is landing-only (no merge/SoR) |
| 13 | Trigger prod deploys | `mcp__trigger__list_deploys` | latest `v20260606.1` DEPLOYED 20:00:54 UTC (#228, cron-present orchestrator) |
| 14 | Cron-removal vs deploy timing | `git log --format=%ci -- sam_spine_refresh.ts` | #233 (cron removal) `2026-06-06 16:43 -0400` = 20:43 UTC → after the 20:00 UTC deploy |
| 15 | merge precedents | `grep merge_insert`, `Read uspto_tm/ingest.py:795-875`, `edgar/ingest.py:806` | `when_matched_update_all().when_not_matched_insert_all()`; targets have NO aggregate columns (winning-row only) |
| 16 | GSA daily/format/rate/SFTP/open-path | `WebFetch open.gsa.gov/api/sam-entity-extracts-api/` | dailies Tue–Sat after 7 AM ET; incremental new/updated/deactivated/expired; daily files listed FOUO_DAILY_V2 / SENSITIVE_DAILY_V3 only (no PUBLIC daily); rate tiers match §3.4; SFTP **not mentioned**; no open no-key path |
| 17 | Open download path liveness | `curl …extractfiledownload` | HTTP 301 → sam.gov portal (DEAD, matches plan) |
| 18 | extracts endpoint routing | `curl` no-key + invalid-key | both HTTP 404 (gated; inconclusive for liveness — endpoint liveness rests on the prior 429) |
| 19 | sam_opps_bulk Trigger pattern | `grep src/trigger/sam_opps_bulk.ts` | `schedules.task`, `cron: "0 12 * * *"` UTC, dispatch by app/function — valid Phase-1 template |

**Quota discipline:** No call to the live `data-services/v1/extracts` endpoint with the real `SAM_API_KEY` was made (quota exhausted; resets 00:00 UTC). All endpoint probes used no-key or an obviously-invalid key, which cannot consume the `SI-NONFED` entity quota.

---

## What "executable after remediation" looks like

1. **Phase 0 first, blocking:** authenticated PUBLIC daily probe (F5) → if absent, decide FOUO-entitlement vs monthly-only before any code.
2. **Phase 1 landing:** new worker emits the **full 18-col `entity_registrations` schema + `pull_date`** (F6), lands via **`merge_insert(on=["uei","extract_label"])`** for idempotency (F10), ledgers `ops.sam_entity_daily_runs`, Trigger task cloned from `sam_opps_bulk.ts` firing ≥13:00 UTC (F13). Surety feed = `initial_registration_date` ∈ window ∧ anti-join vs master ∧ relevant NAICS/PSC (F14). **Revenue-capable here.**
3. **Schema migration (pre-Phase-2):** rebuild master once with `source`/`valid_as_of` added (F3).
4. **Phase 2 merge — corrected:** build the merge **source as the combined row** (recompute the 5 aggregates by folding the daily row into the read-back target values; `is_active` stays monthly-anchored, add a separate daily-active column) (F1, F2); replicate the full tiebreak chain in the condition with NULL handling (F7); wrap in the master's gate+rollback skeleton (F11); re-optimize indices every run (F12); scope to entities + sidecar-rebuild only, leave POCs monthly (F8, F9).
5. **Re-baseline:** keep `build_sam_master` reading `(entity_registrations ∪ sam_entity_daily)` as the drift-free backstop and the parity oracle for a daily-merge-vs-rebuild diff test (F1, F7).
