# [RESOLVED 2026-07-22] HANDOFF — `gtm_entity_pricing_flow` mart

> **STATUS: SHIPPED + SERVING.** The mart is live in `query_sidecar_20260722T023655Z` (and forward),
> 163k rows, sorted uei. It built clean under the spawn-on-deployed launch pattern (the client-tether
> failures that blocked it — runs 40/42/43, `Query interrupted` — were diagnosed as non-detached
> `modal run` dying with the local client, NOT container preemption; fixed by
> `modal deploy` + `Function.spawn`). Catalog row added to the agent guide §3. This handoff is
> retained for history; no action remains. Original content below.

---

# HANDOFF — `gtm_entity_pricing_flow` mart: landed in code, NOT yet served

**Status (2026-07-21):** the mart **code is merged to `main`** (PR #1273, squash commit
`6c6d2196`) and is in the operator checkout on disk. It is **not in the live sidecar
artifact** — no full build has completed since it landed (two attempts were killed by Modal
container preemption). This doc is the single pointer for (a) bundling the mart into a later
sidecar rebuild, and (b) investigating the real build-reliability / architecture issues.

Point a new agent HERE first. Everything load-bearing is inlined below (the scratchpad drafts
this was assembled from are session-ephemeral and may be gone).

---

## 1. TL;DR

- **What:** a new query-sidecar mart `gtm_entity_pricing_flow` (1/uei) — the trailing-window
  pricing/labor **FLOW** complement to `gtm_entity_pricing_mix`'s active **STOCK**. Folds
  sidecar-gap entries 2 (FFP→cost/T&M **transition**) and 3 (SCA/DBA **labor exposure**) into
  one ms-class entity-grain read.
- **Where the code is:** `pipelines/query_sidecar/build_query_sidecar.py` on `main` @ `6c6d2196`.
- **Proven:** fixture-tested + `_preflight` green + **built clean in a real Modal run**
  (`162,872 rows, 2.7s, parity=OK`) before that run was preempted on an unrelated later mart.
- **Blocked on:** one full sidecar build surviving its ~25-min window. Serving is safe
  throughout (blue-green + parity gate → a failed build publishes nothing).
- **To finish:** run a build, verify, then apply the two doc artifacts in §5–§6 and archive the
  gap report. NO code change to the mart is needed.

---

## 2. Coordinates (every file/id that matters)

| Thing | Location |
|---|---|
| Mart SQL builder | `build_query_sidecar.py` → `def _entity_pricing_flow_sql(e12,e24,e48)` |
| Manifest entry | same file, MANIFEST → `{"dest":"gtm_entity_pricing_flow","pricing_flow":True,...}` (right after `gtm_entity_fy_won`) |
| Dispatch branch | same file, `_build_one` → `elif spec.get("pricing_flow"):` (computes the max(action_date) watermark, inlines window bounds) |
| Merge | PR #1273, squash `6c6d2196` → `main` |
| Demand origin (gap report) | `docs/sidecar_gaps/SIDECAR_GAP_REPORT_2026-07-21-capitalization-triggers.md` (entries 2+3; entry 1 = win-then-borrow, DEFERRED) |
| Strategy context (why the mart exists) | `docs/reference/CAPITALIZATION_TRIGGERS_RECON_2026-07-20.md` (§8.3 originally proposed it) |
| Guide (where the catalog row goes, once served) | `docs/reference/QUERY_SIDECAR_AGENT_GUIDE.md` (insert after the `gtm_entity_pricing_mix` row) |
| Build entrypoint | ~~`modal run --detach …::run`~~ SUPERSEDED (client-tethered; killed 8 builds) — spawn on the deployed app per the `/sidecar-build` skill |
| Build fn config | `@app.function(memory=131_072 (128 GiB), cpu=8.0, timeout=2h, max_containers=1)`, `SET memory_limit='96GB'`, spill NVMe. **No `retries=`.** |
| Run ledger | Postgres `ops.query_sidecar_runs` (terminal state per run, success AND failure) |

---

## 3. What the mart is (columns)

1/uei, local off `txn_events_combo`, sorted `uei`. Windows anchored to the data's
`max(action_date)` at build time (FPDS-lag watermark, NOT current_date). recent24 = last 24mo;
prior24 = the 24–48mo window; recent12 = last 12mo. Share numerators `coalesce`→0 (0.0 = window
had activity, none of that class; NULL = no activity in the window). Pure GROUP BY, no join.

- **Denominators:** `obl_total_recent24/prior24/recent12`, `action_ct_recent24/prior24`.
- **Pricing-class FLOW (transition core):** `obl_{fixed,cost,tm_lh,other}_{recent24,prior24}` +
  `cost_share_recent24`, `cost_share_prior24`, `cost_tm_share_recent24`, `cost_tm_share_prior24`,
  `fixed_share_recent24`. Class map = `gtm_entity_pricing_mix` (fixed A,B,J,K,L,M · cost R,S,T,U,V
  · tm_lh Y,Z · else other).
- **Labor exposure (entry 3):** `obl_labor_covered_recent24/prior24`, `labor_covered_share_recent24`
  (`labor_standards_code='Y'` = SCA/DBA applies).
- **Adjacency riders (same scan):** `obl_unfinanced_recent24`+`unfinanced_share_recent24`;
  `obl_new_award_recent24`+`new_award_share_recent24` (new-work vs mods, `action_type_code IS NULL`);
  `n_agencies_recent24`, `n_states_recent24` (recent-window buyer/geo breadth — the recent
  multi-state signal); `obl_small_co_recent24` (`co_business_size='S'`).

⚠ **Verify on first serve:** the clean run produced **162,872 rows** (distinct UEIs with combo
activity in the last 48mo). That felt low vs `gtm_entity_pricing_mix` (766,803 UEIs over all award
history) — plausible (recent-active subset) but confirm the count + window semantics against a
known firm before trusting it downstream.

---

## 4. What happened with the builds (the situation)

| Attempt | Modal app | Outcome |
|---|---|---|
| 1 | `ap-jEI72vEzMT4hUQMTmXwJ9p` | **my mart built clean** (162,872 rows, 2.7s, parity=OK) at ~mart 35, then container preempted ~mart 53 (`gtm_prime_demand_events`, an 11.3M generic copy). `RuntimeError: Query interrupted / Runner terminated.` |
| 2 | `ap-7TND9h35kFCxwqyAaUNMnK` | preempted earlier, ~mart 15 (`gtm_txn_events_slim`, the 108M base). Same `Runner terminated.` |

Both died to **container termination at *different* marts** — the signature of **spot
preemption / infra reclamation**, not a deterministic code/OOM bug (128 GiB cap, 12h timeout rule
those out). **My mart is not implicated** (it built clean, and both failures are on other marts).
Serving stayed on the prior good artifact `query_sidecar_20260721T020734Z` (107 tables) the entire
time. Per operator instruction, **no further retries were fired.**

---

## 5. To finish shipping (bundle with any later sidecar rebuild)

The mart is already in the manifest on `main`, so **any** full build that completes will serve it.
After a build publishes (new `/healthz` artifact stamp; table count 107→108):

1. **Verify:** `DESCRIBE gtm_entity_pricing_flow`; confirm ~163k rows + a real firm sample; parity=OK.
2. **Measure before→after** (reproduce with the sidecar SQL endpoint):

```sql
-- ENTRY 2 (transition) — BEFORE: ~4.1s over txn_events_combo → 56 firms
WITH e AS (SELECT uei,
  sum(obligation) FILTER (WHERE action_date>=DATE '2024-07-01') all_recent,
  sum(obligation) FILTER (WHERE pricing_code IN ('R','S','T','U','V') AND action_date>=DATE '2024-07-01') cost_recent,
  sum(obligation) FILTER (WHERE action_date>=DATE '2022-07-01' AND action_date<DATE '2024-07-01') all_prior,
  sum(obligation) FILTER (WHERE pricing_code IN ('R','S','T','U','V') AND action_date>=DATE '2022-07-01' AND action_date<DATE '2024-07-01') cost_prior
  FROM txn_events_combo WHERE action_date>=DATE '2022-07-01' AND pricing_code IS NOT NULL GROUP BY 1)
SELECT count(*) FILTER (WHERE all_recent BETWEEN 5e6 AND 100e6 AND all_prior>1e6
  AND coalesce(cost_prior,0)<=0.10*all_prior AND coalesce(cost_recent,0)>=0.30*all_recent) FROM e;
-- AFTER: ms-class, same count target ↓ (window is 24/24 anchored to max(action_date); expect ≈56)
SELECT count(*) FROM gtm_entity_pricing_flow
WHERE obl_total_recent24 BETWEEN 5e6 AND 100e6 AND obl_total_prior24 > 1e6
  AND coalesce(cost_share_prior24,0) <= 0.10 AND cost_share_recent24 >= 0.30;

-- ENTRY 3 (labor) — BEFORE: ~3.3s → 3,681 firms
SELECT count(*) FILTER (WHERE o BETWEEN 5e6 AND 100e6) FROM
  (SELECT uei, sum(obligation) o FROM txn_events_combo
   WHERE labor_standards_code='Y' AND action_date>=DATE '2024-07-01' GROUP BY 1) t;
-- AFTER: ms-class
SELECT count(*) FROM gtm_entity_pricing_flow WHERE obl_labor_covered_recent24 BETWEEN 5e6 AND 100e6;
```
Note: before/after counts won't match to the row (before uses a fixed `2024-07-01` cut; the mart
uses a 24/24 split anchored to `max(action_date)`). Confirm they're in the same ballpark.

3. **Apply the guide-catalog entry** (§6) to `QUERY_SIDECAR_AGENT_GUIDE.md`.
4. **Append the disposition** (§7) into the gap report, then
   `git mv docs/sidecar_gaps/SIDECAR_GAP_REPORT_2026-07-21-capitalization-triggers.md docs/sidecar_gaps/processed/`.
5. One PR (guide + gap-report disposition + git mv) → squash-merge → pull the operator checkout.

---

## 6. Guide-catalog entry (paste into `QUERY_SIDECAR_AGENT_GUIDE.md`, once served)

**§3 row (insert directly after the `gtm_entity_pricing_mix` row):**

```
| `gtm_entity_pricing_flow` | 1/uei · ~163k | uei | **The trailing-window FLOW** (2026-07-21, sidecar-gaps entries 2+3) — velocity/transition complement to `gtm_entity_pricing_mix`'s active STOCK. recent-24mo vs prior-24mo obligations by pricing class: `obl_{fixed,cost,tm_lh,other}_{recent24,prior24}` + `cost_share_recent24`/`cost_share_prior24`/`cost_tm_share_*`/`fixed_share_recent24`. **FFP→cost/T&M transition** = prior fixed-dominant → recent materially cost/tm (replaces a ~4s combo scan). **SCA/DBA labor exposure**: `obl_labor_covered_recent24/prior24` + `labor_covered_share_recent24`. Riders (same scan): `obl_unfinanced_recent24`+`unfinanced_share_recent24`, `obl_new_award_recent24`+`new_award_share_recent24` (new-work vs mods), `n_agencies_recent24`/`n_states_recent24` (recent buyer/geo breadth — the recent multi-state signal), `obl_small_co_recent24`, `obl_total_recent12/recent24/prior24`. Windows anchored to max(action_date) at build; share numerators coalesce 0 (NULL = no window activity). Local off txn_events_combo, uei-sorted |
```

**§4 proven patterns (add near the pricing-terms block):** the two AFTER queries in §5, plus the
velocity **routing-fix** (no table needed):
```sql
-- whole-universe velocity is ALREADY servable off the behavior rollup's trailing windows:
SELECT uei FROM gtm_entity_behavior_rollup
WHERE prime_obl_12mo >= 3 * NULLIF((prime_obl_36mo - prime_obl_12mo)/2, 0);  -- >=3x recent vs prior-24 annualized
```

---

## 7. Disposition + build-scope block (append to the gap report on archive)

**Promoted (structural):** `gtm_entity_pricing_flow` — folds entries 2+3; pure GROUP BY off
`txn_events_combo`, EXPLAIN-clean (no join), fixture-verified.

**Rides as adjacency (same single scan):** full pricing-class split both windows; `cost_share_*`
/`cost_tm_share_*`/`fixed_share_recent24`; labor recent+prior+share; `unfinanced_*`; `new_award_*`
(mobilization vs mods); `n_agencies_recent24`/`n_states_recent24` (serves the recent-window
multi-state latent gap); `obl_small_co_recent24`; `obl_total_recent12`.

**Routing fixes (no build):** whole-universe monthly velocity → `gtm_entity_behavior_rollup`
`prime_obl_12/24/36mo` (guide §4 note). Recent-window multi-state → now served by
`n_states_recent24`.

**Parked (structural, deferred — do NOT cram into an unrelated build):**
- **Entry 1 win-then-borrow.** Its award↔UCC pairing is a `BETWEEN` interval join (violates the
  builder's pure-equality join doctrine) and its highest-value component (fresh-money-in-90d "open
  window") is query-time-inherent — baking it freezes staleness. Correct design: a pure-equality
  `gtm_win_then_borrow` propensity leg (historical paired-count + last-award/last-filing dates) with
  the open-window overlay as a cheap uei-pruned `gtm_txn_events_slim` read. CA/CO-coverage-bound.
- Month-grain per-firm pricing/labor sparkline series — entity-grain fixed windows chosen
  deliberately; promote a `uei × month` rollup only if per-firm sparklines become a page section.

**Measured deltas:** _(fill on serve — before: transition ~4.1s/56 firms, labor ~3.3s/3,681 firms;
after: expect ms-class)._

---

## 8. Investigation brief — the REAL issues (for a diagnose-and-fix agent)

Two independent problems surfaced. Neither is the mart.

### 8a. Build reliability — repeated container preemption
Two full builds died with `RuntimeError: Query interrupted / Runner terminated.` at different marts.
Diagnose:
- **Ledger:** query `ops.query_sidecar_runs` (via `doppler run -p core-x -c prd -- sh -c 'psql "$HQX_DB_URL_POOLED" -c "SELECT recorded_at,status,error_message FROM ops.query_sidecar_runs ORDER BY recorded_at DESC LIMIT 10"'`) for the recorded terminal reason.
- **Modal:** `modal app logs <app-id>` for the last two runs; look for OOMKilled vs spot-reclaim vs
  ENOSPC. Check if these ran on preemptible/spot capacity.
- **ENOSPC lead:** there is a branch `fix/sidecar-preclean-enospc` (seen in `git worktree list`) —
  the build streams 100M+ row tables to the container's ephemeral NVMe (`SET temp_directory=.../spill`,
  the growing `.duckdb` file). If the scratch disk is undersized, a large sort/spill can kill the
  container. Confirm whether these two failures were disk-driven (attempt 2 died on the 108M
  `gtm_txn_events_slim` build — a heavy write) or genuinely spot-reclaimed.
- **Cheap resilience win:** the build `@app.function` has **no `retries=`**. Adding `retries=1–2`
  would self-heal transient preemption (the build is idempotent — fresh file each run). Low-risk,
  high-value; but confirm the failure class first (retries won't fix a deterministic OOM/ENOSPC).

### 8b. Architecture — full rebuild for every change (the expensive root cause)
The build produces a **brand-new monolithic `.duckdb` from scratch every run** (`duckdb.connect(new
timestamped path)` → re-materialize ALL ~107 marts from the Lance SoR → upload → swap LATEST
pointer). There is **no incremental path**. Adding this **163k-row** mart re-crunches **~600M+ rows**
across the giants (txn_events_slim 108M, txn_events_combo 108M, award_state 83M, inferred codes
263M+160M, txn_rows 108M) — ~25 min — and exposes the *entire* run to preemption for a trivial change.

Proposed structural fix to evaluate: **incremental publish** — copy the current published `.duckdb`,
`ATTACH` it, build ONLY the changed/new mart(s), republish (~1 min, near-zero preemption window).
Tradeoff to decide: the other tables would sit at a slightly older Lance snapshot than the new mart
until the next full rebuild — usually fine, but it breaks the "one consistent pinned snapshot"
invariant the current full-rebuild guarantees. Worth a design note before implementing.

---

## 9. What is already durable (do not redo)
- Mart **code**: `main` @ `6c6d2196` (#1273).
- Strategy recon + original gap report: `main` @ `cf9ef38` (#1269).
- This handoff: (committed alongside — see git log).
Nothing is running or consuming compute. The live sidecar is intact on `query_sidecar_20260721T020734Z`.
