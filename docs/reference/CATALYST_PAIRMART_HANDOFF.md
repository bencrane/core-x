# HANDOFF — Catalyst per-UEI precompute (pair-mart pivot) + Track 1 rollups

**Written:** 2026-07-08 · **For:** the session taking over end-to-end.
**Status:** in-progress
**Repo:** `/Users/benjamincrane/core-x` (branch `main`). **You own the full git lifecycle** (commit → push → PR → merge → pull operator checkout → `git log -1` verify). Operator does not merge for you.

## 0. Read these first (in order)
1. `docs/reference/SUB_UNIVERSE_BLOB_SCHEMA_AND_NODE_GRAMMAR.md` — the frozen contract. **§0 amendment (in the unlanded worktree, see §3) declares the blob DEAD and pair-mart the replacement.** The on-main version still describes the blob; the worktree version is authoritative.
2. `apps/catalyst_api/src/sub_universe_store.py` — shipped `sub_universe.v3` recipe (Definition C `target_combo_farmout`, membership rule). Stable, merged.
3. `docs/reference/CATALYST_FIVE_INPUT_MODEL_ADDENDUM.md` — the follow-on spec (five-input model, families, Cycle C spec).
4. This file.

## 1. The architecture (operator-ratified 2026-07-08 — do not relitigate)
- **Per-UEI BLOB is dead.** It denormalized shared node-grain facts into every overlapping target's payload (v1 136MB; multi-TB at fleet scale); the two-tier rescue degraded exact-day time windows to monthly buckets. Blob code (`sub_universe_full.py`, `sub_universe_serve.py`) landed in main via #1071 and is **superseded but NOT deleted** — leave it; do not extend it.
- **Replacement = relational pair-grain precompute**, two Lance datasets, served by the standard executor pattern (pinned target lane + intersects):
  - `gtm_sub_universe_pairs` — (target_uei × node_uei) pair scalars; BTREE `target_uei` + `node_uei`.
  - `gtm_sub_universe_targets` — 1 row/target: `target_analytics` JSON (pre-call Acts 1–3); BTREE `uei`.
  - Node-grain facts (award-state, demand events, entity, portfolio) are **NOT** stored per pair — they serve at query time from already-indexed node-grain marts. This is what fixes the build cost that killed the 335-target blob batch (measured: one indexed 16K-row award-state pull = 11.8s; hydrating that per node per target = the lost hour).
- **Build model:** operator-triggered on-demand per target. NO cron, NO 57K batch, NO nightly. The outbound list is the build input. Mart grows monotonically. `as_of` stamped/disclosed.
- **Doctrine (non-negotiable):** null ≠ zero; no scoring; truncations flagged; no query-time raw-spine access (refuse, never fall through); measure, never assert.

## 2. What is MERGED on main (done)
- #1068 sub_universe.v3 (Definition C). #1069 freeze doc. #1070 `gtm_sub_profiles` (105K rows, peer dims). #1071 blob v2 (now superseded).
- **#1072 Cycle B — Track 1 rollup marts, all live + indexed:**
  - `gtm_txn_events_slim` 107.9M (grammar-column projection of the 108M txn spine)
  - `gtm_txn_recipient_month_rollup` 34.1M (event-lane collapse killer)
  - `gtm_award_recipient_rollup` 6.3M + `gtm_award_expiry_months` 221K (award-lane)
  - **Marts only — routing NOT flipped.** `market_store.py` still hits the spines. Flipping routing to these rollups is unstarted work (see §5).

## 3. What is BUILT but NOT LANDED — Cycle A (your first job)
- **Worktree:** `/Users/benjamincrane/core-x/.claude/worktrees/agent-a9c28c941891940cb`, branch `worktree-agent-a9c28c941891940cb`, commit **`d6f06b1`**. **Not pushed** (prior agent hit an account cap before landing).
- **Contents:** `apps/catalyst_api/src/sub_universe_pairs.py` (`build_target(uei)`), `scripts/build_sub_universe_target.py` (`--uei` / `--ueis-file`), `test_sub_universe_pairs.py`, config URIs, freeze-doc §0 amendment. **18/18 hermetic tests pass** (verified: `cd` into the worktree, `/Users/benjamincrane/core-x/.venv/bin/python -m pytest apps/catalyst_api/tests/test_sub_universe_pairs.py apps/catalyst_api/tests/test_sub_universe.py -q`).
- **Live datasets already written by the single gate build:** `gtm_sub_universe_pairs` = 10,000 rows (2 targets), `gtm_sub_universe_targets` = 2 rows.

### ⚠️ The 20-target live gate is INCOMPLETE — measured facts (2026-07-08)
A 20-target run got through **1/20** before the invoking session's 10-min Bash timeout killed it (exit 255 — harness timeout, NOT a code failure). Measured from that one target (`F98TZC6J5XV1`, a heavy pick):
- **The builder WORKS end-to-end:** `766.68s, pairs=5,000 (TRUNCATED at store MAX_LIMIT), disc=1,718/undisc=3,282, wrote pairs_v7 (+5000, total 10,000) targets_v5 (total 2)` — delete-then-append semantics verified live.
- **The cost problem:** `base=627s` — `build_target` reuses `execute_sub_universe`, which hydrates demand-event summaries + geo for ALL 5,000 page nodes (chunked scans over the 11.3M demand mart). That is node-grain hydration the pair pivot was supposed to skip. `geo_hydrate=96s` on top.
- **Your gate job:** (1) OPTIMIZE FIRST — have the pair builder compute membership/ranking from the store's caches directly (or add a store flag skipping demand/geo page hydration); target cost should fall to seconds-to-~1min. (2) Re-run 20 genuinely mid-size targets (the previous picker sorted by edge count DESC and grabbed monsters — pick median-edge subs) with a generous timeout, in background. (3) Cross-check ONE node's `tcf_farmout_60mo` byte-equal vs `gtm_prime_farmout_combo_lanes`. (4) Mega-universes: cap or exclude, disclosed.

### Then land Cycle A
From the worktree: `git push -u origin worktree-agent-a9c28c941891940cb` → `gh pr create --repo bencrane/core-x --base main` → rebase on origin/main if `config.py` conflicts (keep BOTH sides' URI additions — Cycle B added rollup URIs) → `gh pr merge --squash --delete-branch` → `git -C /Users/benjamincrane/core-x pull` → `git log -1 --oneline`.

## 4. Verify the whole board before moving on
Confirm all marts live: `gtm_txn_events_slim, gtm_txn_recipient_month_rollup, gtm_award_recipient_rollup, gtm_award_expiry_months, gtm_sub_universe_pairs, gtm_sub_universe_targets` — all should return rows + indices (Lance point checks with the standard R2 storage_options pattern; prefix live commands with `doppler run --project core-x --config prd --`).

## 5. What remains (priority order — the operator's stated critical path is the pre-call PAGE)
1. **Cycle C — the grammar + serving executor** (`sub_universe_node`). NOT started. **ON HOLD until operator green-light.** Build to the spec in `CATALYST_FIVE_INPUT_MODEL_ADDENDUM.md` §3 (three result surfaces over five inputs), NOT to the original open-grammar v1.
2. **Pre-call brief endpoint** — the revenue-motion deliverable. `GET /brief/{token}` on `apps/catalyst_api` → resolve token→uei → point-lookup `gtm_sub_universe_targets` → server-side Jinja render of `~/Desktop/hq/design-artifacts/pre-call/sub_universe_precall_confirm.v29.html` (currently a static mock with hardcoded values). Token (not bare UEI) in URL — enumerable UEIs leak prospects' briefs. Route sits outside `CATALYST_API_TOKEN`. Staleness = last operator build (`as_of` disclosed). Trust gate: every rendered figure traces to public record; verify one real prospect end-to-end before any send.
3. **Track 1 routing flip** — point `market_store.py` collapse steps at the merged rollups (§2), residual union for partial-month/expiry windows, equivalence-gate vs spine on a frozen snapshot (headline fixture: the 35s "companies that funded construction last quarter"), then delete spine paths from the request path. LAST — macro querying serves exploration, not the page motion.

## 6. Traps
- `config.py` is the parallel-work collision point — always keep both sides on rebase.
- The blob-era `sub_universe_full.py`/`_serve.py` are dead weight but referenced by the freeze doc's superseded sections — don't get pulled into extending them.
- Don't build-on-demand INSIDE a cold laptop process for latency numbers — the API service holds the 6h-TTL caches warm; measure there or the winners-index scan (~8-15s) dominates every single-target build.
- Mega-universe targets (resellers: Carahsoft/CDW/Dell Federal — top ~30 by node count) are the only ones that blow up build cost, contribute ~1.5% of total rows, and are not demo targets. Cap or exclude; don't let one hang the batch.
