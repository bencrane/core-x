# CANONICAL PLAN — sidecar build cycle: pricing-flow + award-key companions + person channels

**Status:** ready to execute · **Written:** 2026-07-21 · **Authority:** operator-directed
**Executor:** any cold agent. Read this top-to-bottom, then execute phases in order.
**Prerequisite reads:** `pipelines/query_sidecar/build_query_sidecar.py` module docstring
(build doctrine), `docs/sidecar_gaps/PRICING_FLOW_MART_HANDOFF.md`,
`docs/sidecar_gaps/2026-07-21-award-key-probes.md` (incl. addendum),
`docs/sidecar_gaps/2026-07-21-firm-contact-channels.md`.

---

## 0. Context (why this build, what failed before)

Serving artifact at plan time: `query_sidecar_20260721T020734Z` (107 tables). Three prior
build attempts died (ops.query_sidecar_runs ids 40/42/43, all `error: Query interrupted`,
at 6/18/2 min). Diagnosis (2026-07-21 session): NOT container preemption and NOT a mart
bug — the builds were launched as client-tethered `modal run` (the `run` local_entrypoint
BLOCKS on `build.remote()` for the whole ~40-min build; the agent's local process died →
Modal tore the app down → DuckDB raised the interrupt the builder's handler recorded).
Successful precedents: runs 38/39/41 (22–42 min). Ephemeral-app logs age out, which is why
the failures couldn't be autopsied — fixed in this plan by (a) spawn-on-deployed launch and
(b) ledger instrumentation.

## 1. Scope (the committed bundle — one build)

| Item | What | Code state |
|---|---|---|
| A | `gtm_entity_pricing_flow` (uei-grain pricing/labor FLOW) | **Already on main** (#1273, manifest + dispatch + fixtures green; built clean once in run 42 before that run died). ZERO new code. |
| B | Award-key point-read companions (kills the 13–27s `/award` drawer read; demand: every Explore award-dot click, on-camera green dots) | New manifest entries, mostly pure sort copies — §3 |
| C | `gtm_person_channels` (uei-sorted person contact channels for the firm drawer) | New small mart — §4. Gate-check PASSED 2026-07-21: `gtm_sam_person_contactability` already carries `uei, email, phone, person_linkedin_url_norm` (26 cols, 152k rows); join to `gtm_sam_people` is pure equality on `sam_person_id`. |
| D | Ledger instrumentation (launch mode + function-call id into `ops.query_sidecar_runs`) | Two-line builder patch — §5 |

**Parked (do NOT build):** lender-book bridge `ucc_lender_filings` (next cycle; scope block
already in its report), outlay spine (blocked on upstream reconcile — demo uses frozen
constants), novation/`reason_for_modification`.

## 2. Phase 0 — prep

1. Fresh worktree/branch off `origin/main` (never reuse a shared worktree).
2. `python3 -c "import sys; sys.path.insert(0,'pipelines/query_sidecar'); import build_query_sidecar as b; b._preflight()"` — must pass BEFORE any edit (baseline) and after every edit.
3. Confirm item A present: `grep -n pricing_flow pipelines/query_sidecar/build_query_sidecar.py` → manifest entry at ~line 291 + dispatch branch. If absent, STOP — wrong base.

## 3. Phase 1 — award-key companions (item B)

The measured residuals of the `/award` profile read (post-#1299 uei-pruning):
state-row anchor probe **4.8s** (table sorted `current_end_date`), centroid probe **0.9s**,
txn ledger probes ~0.65s. Target: all ms-class via award-key-sorted copies.

Add to MANIFEST (Tier C, AFTER their source tables — order matters, `from_table` copies
must follow their source's build):

```python
# award-key point-read companions (gap 2026-07-21-award-key-probes):
# every Explore award-dot click reads these; sorted by the probe key.
{"ds": "txn_events_combo", "tier": "C", "dest": "txn_events_combo_by_award",
 "sort": ["award_key", "action_date"], "from_table": "txn_events_combo"},
{"ds": "usaspending_fpds_canonical_txn", "tier": "C",
 "dest": "txn_rows_by_award", "sort": ["contract_award_unique_key", "action_date"],
 "cols": [  # identical column list to the existing txn_rows entry — copy it verbatim
     "contract_transaction_unique_key", "contract_award_unique_key", "award_id_piid",
     "recipient_uei", "recipient_name", "action_date", "action_type_code",
     "action_type_description", "subcontracting_plan", "subcontracting_plan_desc",
     "federal_action_obligation", "base_and_all_options_value", "naics_code",
     "product_or_service_code", "awarding_agency_code", "awarding_agency_name",
     "type_of_contract_pricing_code", "type_of_contract_pric_desc"]},
{"ds": "usaspending_fpds_prime_award_state", "tier": "C",
 "dest": "prime_award_state_by_key", "sort": ["contract_award_unique_key"],
 "from_table": "usaspending_fpds_prime_award_state"},
{"ds": "usaspending_award_pop_centroids", "tier": "C",
 "dest": "award_pop_centroids_by_key", "sort": ["generated_unique_award_id"]},
```

Implementation notes (verify, don't assume):
- `txn_events_combo_by_geo` (~line 256) is the proven `from_table` + `sort` copy pattern —
  no special dispatch flag. Confirm the generic `_build_one` branch handles `from_table`
  with no flag; it does for by_geo — mirror exactly.
- `prime_award_state_by_key`: full-width copy of the 83M-row state table (43 cols). If
  build cost is a concern, slim with `cols` to the anchor columns the `/award` SELECT uses
  (see `award_profile` in `apps/catalyst_api/src/routers/market_slice_v1.py` ~line 773) —
  BUT then run the adjacency sweep on the drop list: any column another consumer probes by
  key must stay. Full-width is the safe default; disk is cheap, a rebuild is not.
- ADJACENCY SWEEP (mandatory, write results into the disposition BEFORE building):
  next-questions for an award tear-sheet — vocab names (already served via
  `fpds_code_vocab`), parent/vehicle fields (in state full-width ✓), sub-out
  (`award_subout_rollup` — check its sort; if not award-key-sorted, add
  `{"ds": "award_subout_rollup", "dest": "award_subout_rollup_by_key", "sort": ["award_key"], "from_table": "award_subout_rollup"}`
  after verifying its key column name).
- Fixture-test THROUGH THE DISPATCH PATH; EXPLAIN any join (none expected — these are
  copies); `_preflight()` after edits.

## 4. Phase 1b — `gtm_person_channels` (item C)

One small local mart (Tier D, after `gtm_sam_people` + `gtm_sam_person_contactability`):

```python
{"ds": "gtm_sam_people", "tier": "D", "dest": "gtm_person_channels",
 "sort": ["uei"], "from_table": "gtm_sam_people", "person_channels": True,
 "aggregate": True},
```

New dispatch branch + SQL constant (equality join ONLY — the nested-loop doctrine):

```sql
CREATE TABLE gtm_person_channels AS
SELECT p.uei, p.sam_person_id, p.display_name, p.first_name, p.last_name,
       p.best_title, p.is_govt_poc, p.is_ebiz_poc, p.n_sources,
       c.email, c.email_verification_status, c.phone, c.phone_status, c.phone_type,
       c.person_linkedin_url_norm, c.linkedin_match_score
FROM gtm_sam_people p
LEFT JOIN gtm_sam_person_contactability c ON c.sam_person_id = p.sam_person_id
ORDER BY p.uei, p.sam_person_id
```

(2.3M rows ⋈ 152k — trivial. LEFT JOIN keeps parity row-preserving vs `gtm_sam_people`;
if the builder's parity gate compares against the `ds` count, this is exact-parity — set
`aggregate: True` ONLY if the dispatch requires it for local marts; check how
`gtm_entity_fy_won` declares it and mirror.) EXPLAIN-gate the join at fixture time
(assert no NESTED_LOOP node). Add a dispatch branch — `_preflight` will hold you to it.

## 5. Phase 1c — ledger instrumentation (item D)

In the builder's run-record insert (~line 2198–2207 function `record` /
`init_schema` area): add columns `launch_mode text` and `function_call_id text`
(`ALTER TABLE ... ADD COLUMN IF NOT EXISTS` in `init_schema`), populate from env
`MODAL_LAUNCH_MODE` / `MODAL_FUNCTION_CALL_ID` if present else null. The spawn wrapper
(§7) sets them via the function's own context (`modal.current_function_call_id()` if
available in the pinned Modal version — verify; else pass as a build() kwarg from the
spawn call). Keep it two-line simple; this is instrumentation, not architecture.

## 6. Phase 2 — ship the builder change

House lifecycle: commit by explicit path → push → PR against main → squash-merge →
pull `~/core-x` → `git log -1 --oneline` verify.

## 7. Phase 3 — launch (spawn-on-deployed; NEVER client-tethered `modal run`)

```bash
cd ~/core-x   # the pulled, merged main — NOT a worktree with uncommitted edits
modal deploy pipelines/query_sidecar/build_query_sidecar.py   # refresh deployed app "query-sidecar"
python3 - << 'EOF'
import modal
fn = modal.Function.from_name("query-sidecar", "build")
fc = fn.spawn(tiers="A,B,C,D", publish=True, smoke=False, trigger_callback_url=None)
print("FUNCTION_CALL_ID:", fc.object_id)
EOF
```

RECORD the printed `fc-…` id immediately (paste it into the disposition draft). The call
runs server-side under the deployed app — no client tether at any phase; the launching
session may end freely.

## 8. Phase 4 — monitor (poll, never attach)

- `modal app logs query-sidecar` (deployed-app logs persist — this is the autopsy fix).
- Poll: `modal.FunctionCall.from_id("<fc-id>").get(timeout=0)` → raises TimeoutError while
  running; returns the result dict when done.
- `curl -s https://query-sidecar-api.onrender.com/healthz` — stamp flips on publish.
- Healthy precedent: 22–42 min total. If a step exceeds ~3× its precedent AND the container
  loadavg ≈ 0 (`modal container list` + exec `cat /proc/loadavg`) → hung: py-spy dump
  (`--native`) BEFORE killing (`modal.FunctionCall.from_id(id).cancel()`). A dead build
  publishes nothing (blue-green + parity) — serving is never at risk.
- Terminal state lands in `ops.query_sidecar_runs` either way.

## 9. Phase 5 — verify + measure (a shape not measured is not done)

Against serving (bearer `QUERY_SIDECAR_TOKEN`, `POST /api/v1/sql`):
1. `/healthz` → new stamp, table count 107 + (4 or 5 companions) + 1 pricing_flow + 1 person_channels.
2. `SELECT COUNT(*) FROM gtm_entity_pricing_flow` → expect ~162,872 (run-42 precedent).
3. `SELECT COUNT(*) FROM gtm_person_channels` → ~2.3M; spot-check one known uei returns email/phone rows.
4. Timing before→after for the gap shapes (record in disposition):
   - `SELECT * FROM prime_award_state_by_key WHERE contract_award_unique_key = 'CONT_AWD_N0001922F2503_9700_N0001919G0029_9700'` — expect ms-class (was 4.8s on the end-date-sorted table).
   - FY-sum probe on `txn_events_combo_by_award WHERE award_key = '<same>'` — ms-class.
   - Centroid probe on `award_pop_centroids_by_key` — ms-class (was 0.9s).

## 10. Phase 6 — consume (separate PRs, after serving verifies)

1. **catalyst** (`apps/catalyst_api/src/routers/market_slice_v1.py`, `award_profile`):
   point the anchor read at `prime_award_state_by_key`, ledger probes at
   `txn_events_combo_by_award` / `txn_rows_by_award` (drop the `uei_leg` pruning trick —
   keep the regex guard), centroid at `award_pop_centroids_by_key`. Update
   `test_award_profile` fixtures. Catalyst deploys from main (~90–120s Railway).
2. **catalyst** `/market-slice/firm`: point `contacts` at `gtm_person_channels`
   (email/phone/linkedin now real); update the POC disclosure string.
3. **gc-hq-new**: revert `marketSliceAward` timeout 90s → 30s
   (`apps/platform-api/src/catalyst.ts`); firm drawer people section renders
   email/phone/linkedin. Measure a COLD award-dot click end-to-end (browse binary) —
   record the number.

## 11. Phase 7 — disposition + archive (one core-x PR)

- Append Disposition tables to `2026-07-21-award-key-probes.md`,
  `2026-07-21-firm-contact-channels.md`, `SIDECAR_GAP_REPORT_2026-07-20-capitalization-triggers.md`
  (entries folded by pricing_flow), and the capital-video report's Entry 2: verdict, what
  shipped, measured before→after, adjacency riders + rationale, parked candidates.
- Update `docs/reference/QUERY_SIDECAR_AGENT_GUIDE.md` catalog IN THE SAME PR: new table
  rows (grain/sort/cols), §4 proven pattern per new capability, table count.
- `git mv` the processed reports to `docs/sidecar_gaps/processed/`; archive
  `PRICING_FLOW_MART_HANDOFF.md` (its §5–§6 doc artifacts apply now).
- Full lifecycle; pull `~/core-x`; verify.

## 12. Failure handling

- Build error → read `modal app logs query-sidecar` + the ledger row (now carries
  launch_mode/fc-id). Diagnose before relaunching; theories are free, builds are not.
- Parity failure on a new companion → the copy SQL diverged from its source; fix, re-run.
  Serving keeps the prior artifact throughout.
- If `from_table`+`cols` combination turns out unsupported by the generic dispatch for
  `prime_award_state_by_key` slim variant: ship full-width (supported) — do NOT invent a
  new dispatch flag for a width optimization.
