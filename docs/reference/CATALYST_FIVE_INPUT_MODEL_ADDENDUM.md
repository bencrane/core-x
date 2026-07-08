# ADDENDUM — The Five-Input Universe Model, Capability Families, and the Cycle C Spec

**Written:** 2026-07-08
**Predecessor:** docs/reference/CATALYST_PAIRMART_HANDOFF.md
**Status:** in-progress
**For:** the agent executing the handoff (currently: re-running the 20-target gate, landing Cycle A, verifying the six marts).
**Standing instruction:** Cycle C remains ON HOLD until the operator explicitly green-lights it. This document is C's spec plus the concrete work-queue deltas to execute around it. Everything here is operator-ratified 2026-07-08; do not relitigate.

---

## 1. The Five-Input Model (supersedes "one node list + open filters" as the presentation/query model)

The per-UEI universe is composed from **five named inputs, each with baked default assumptions** derived from the target's own record, each tunable live without any global rebuild. The goal on record: *decrease granular filtering, increase sharp point-of-view, keep tuning cheap.*

| # | Input | Baked default (derived per target) | Tunable knobs |
|---|---|---|---|
| 1 | **Geographic Focus** | Target's evidenced footprint: PoP geos as a sub ∪ PoP geos as a prime — at **state AND county** grain (see §4.2) | add/remove states/counties; radius later (ZCTA centroids exist) |
| 2 | **Core Capabilities** | Top-X (start X=3) **capability families** by frequency + $ weighting over demonstrated combos; exact combos retained underneath for drilldown | X; family vs exact-combo altitude; include/exclude a family |
| 3 | **Lookalike Primes Audience** | Membership = shipped lookalike-winner rule (win the anchors' work — combo/family level, NEVER literal same-award/vehicle). Tier B = ∩ farms out the target's combos/families (shipped Definition C facts) | tier A vs B; monetary floor/ceiling (dimension, §1 rule); event-posture filters |
| 4 | **Lookalike Subs Audience** | Subs sharing ≥3 of the target's lanes (family or exact grain) ∧ ≥1 state ∧ deal-band overlap (= shipped peer rule, `gtm_sub_profiles`) | shared-lane threshold; grain; monetary floor (dimension) |
| 5 | **Deal Economics** | Target's demonstrated band: p20–p80 + median chunk | band bounds; median comparator |

**Composition rule (binding):** inputs 1, 2, 5 and all monetary/time knobs are **filters over ONE universe** whose membership is input 3's rule (widest tier, dim-never-delete). They scale/scope members; they never define parallel universes. Input 4 is a second audience surface over the same target scopes, not a second membership rule for primes.

**Monetary is a dimension pattern, not an axis** (operator-ratified): "money of the primes" = floor/ceiling knobs on axis 3 (`matched_prime_obl_60mo`, `tcf_farmout_60mo`, lifetime $ via `gtm_entity_behavior_rollup`); "money of the subs" = floor knobs on axis 4 (capture, sub $ windows); chunk-size economics = input 5. Money-of-whom is only defined relative to an audience. All monetary facts are existing columns — **zero new build** for this section.

**Facet-vs-membership rulings (binding):**
- Declared NAICS/PSC (SAM) of anchors/lookalikes: **facet/display only**, never membership (declared ≠ demonstrated).
- Vehicle co-holding (literal same parent IDV): **per-node annotation only**, never membership (recall collapses; mega-vehicles invert the signal).

## 2. Capability Families — definition and placement

**Definition:** `family_key = NAICS[:4] + 'x' + PSC[:1]`. Example: 541330×R425 and 541380×R408 → `5413xR` ("Engineering/Architectural Services × Professional Support"). Pure string truncation over keys that already exist everywhere — no new source data. One tiny new reference artifact: **family titles** (family_key → display name, low hundreds of rows; derive from `naics_reference` 4-digit titles + PSC category letters; bake as a small Lance table or inline title columns).

**Why (quantified):** (a) fixes Definition C sparsity — farm-out evidence is 37,569 lanes / 5,460 primes at 6-digit; family grain densifies it so "buys work in my family" flags far more confirmed-adjacent buyers; (b) implements "group the near-misses in" — an R-support firm's unwon R-codes roll into the same family with zero inference machinery; (c) top-3 families cover most of a target's $ (concentration rises at coarser grain); (d) vocabulary shrinks ~50× (low hundreds of populated families vs ~10K+ active combos) — smaller bind tables for C, nameable filter chips.

**Placement — columns, NOT a query-time join. Two phases:**
- **Phase F1 (do with/immediately after landing Cycle A; no mart rebuilds):** inside `build_target()` — roll matched combos up to families in the same pass (dict aggregation). Bake into **pair rows**: `family_matched_obl_60mo`, `family_tcf_farmout_60mo` (JSON or per-top-family columns; keep it queryable). Bake into **targets row**: the target's demonstrated families, top-X by frequency+$, with per-family totals. Because writes are delete+append per target, re-tuning family grain later rebuilds only already-built targets — the cheap-tuning property, by construction.
- **Phase F2 (each mart's NEXT rebuild, operator-triggered — do not rebuild marts just for this):** add `family_key` column + BTREE to `gtm_prime_farmout_combo_lanes`, `gtm_prime_combo_lanes`, `gtm_sub_combo_lanes`, `gtm_txn_events_slim`. Family filters become indexed exact-matches instead of prefix→IN-list expansion.

**Boundary (binding):** families coarsen inputs 2/3/4; they do NOT replace exact combos. **Deal economics (chunk medians/bands) stay computed at true combo grain** and aggregate up — never compute a median across a family's heterogeneous combos as if it were a deal-size fact. Exact combos remain underneath for drilldown.

## 3. Cycle C spec (HOLD until operator green-light — build to THIS, not the original open-grammar v1)

**Query model:** `For UEI <target> → show me all <surface> that <predicates>` with three result surfaces:

| Surface | Returns | Sales meaning | Substrate |
|---|---|---|---|
| **S1 Lookalike primes that…** | prime nodes | "the best buyers for you" | pair mart (pinned `target_uei`) ∩ node-grain marts |
| **S2 Lookalike subs that…** | peer subs | "your sharpest competitors" | `gtm_sub_combo_lanes` + `gtm_sub_profiles` (+ baked capture in targets row) |
| **S3 Awards/flow that…** | awards/actions in the target's families | "where the money is going without you (incl. active today)" | `gtm_txn_events_slim`, `usaspending_fpds_prime_award_state`, baked pool block |

Cross-cutting dimensions on all three: time (exact-day, indexed dates), geo (input 1), deal economics (input 5), monetary knobs (§1).

**Execution pattern (this is the whole executor):** S1 = indexed pair-mart scan pinned to `target_uei` + pushdown on pair columns, INTERSECT uei-sets from node-grain lanes (`gtm_txn_events_slim` for event/time/agency predicates — exact-day, full history; `gtm_sam_entities` for qualifiers; `gtm_entity_behavior_rollup` for overall-$ floors). S2 = indexed scans over sub-side marts scoped by target lanes/states/band. S3 = Track 1 marts scoped by the target's families/geo — the two tracks converging on one serving pattern. Same lex→bind→refuse discipline as `phrase.v2`; **v1 is an input-parameter compiler** (five inputs + knobs + surface selector), the free-text phrase layer stacks on later against the same executor.

**Substrate readiness (verified 2026-07-08):** every S1/S2 predicate family is servable once Cycle A lands; S3 is servable now with one gap: `usaspending_fpds_prime_award_state` has NO `naics_code`/`product_or_service_code` BTREEs (audit-verified absent) → §4.1.

## 4. Concrete work-queue deltas (execution list, in order)

**4.0 — unchanged from the handoff:** finish the 20-target gate (with the builder perf fix: stop reusing `execute_sub_universe`'s full page hydration — compute membership/ranking from caches; measured 627s/766s per heavy target says this is mandatory, not optional), land Cycle A, verify the six marts.

**4.1 — award-state BTREEs (minutes, do immediately after landing A):** `create_scalar_index` BTREE on `naics_code` and `product_or_service_code` on `usaspending_fpds_prime_award_state`. Unblocks S3 award lists by family/combo.

**4.2 — PoP rollup mart, STATE + COUNTY grain (operator-directed: county or equivalent, not just state).** New mart `gtm_prime_pop_lanes`: grain (`uei` × `pop_state` × `pop_county_fips`), columns: `pop_county_name`, `n_actions_24mo/60mo`, `obligation_24mo/60mo`, `last_action_date`. Source: `usaspending_fpds_canonical_txn` (columns verified present: `primary_place_of_performance_state_code`, `pop_county_fips`, `pop_county_name`; note `gtm_txn_events_slim` does NOT carry geo — source the spine directly, one batch pass, house builder pattern + `--verify`). BTREEs: `uei`, `pop_state`, `pop_county_fips`. This closes the geo dimension on S1/S3 (prime win-side work-site). Target-side county footprint needs NO new mart — the subaward spine carries `place_of_perform_county_*` and `sub_place_of_perform_county_*`; extend `build_target()` to bake the target's state+county footprint into the targets row (input 1's default).
Radius note: zip5→ZCTA centroid sidecars already exist (`usaspending_award_pop_centroids`, `zcta_zip_centroids`) — distance is a later knob, not this build.

**4.3 — Family Phase F1 (§2) + family titles reference.** In `build_target()` + targets row. Ship with or right after A; it's aggregation over data the builder already touches.

**4.4 — freeze-doc amendment:** append the five-input model (§1), family definition + placement (§2), the three-surface C spec (§3), and the facet-vs-membership rulings to `docs/reference/SUB_UNIVERSE_BLOB_SCHEMA_AND_NODE_GRAMMAR.md`. The grammar enumeration remains the contract; this narrows v1 scope and adds the family altitude.

**4.5 — Cycle C: build to §3 WHEN the operator green-lights.** Not before.

**4.6 — Family Phase F2:** ride each mart's next operator-triggered rebuild. Do not initiate rebuilds for it.

## 5. Unchanged rulings (for continuity — do not reopen)
Blob dead; pair mart is the per-UEI precompute; membership = lookalike-winner rule, dim-never-delete; null ≠ zero everywhere; no scoring; operator-triggered rebuilds only; no query-time raw-spine access (refuse, never fall through); mega-universe targets (resellers) capped/excluded with disclosure; pre-call page renders from the targets row (bake acceptable, drift accepted by operator); Track 1 routing flip remains queued LAST.
