# Sub-Universe Blob Schema ≙ `sub_universe_node` Grammar Enumeration

**Date frozen:** 2026-07-07 · **Owner surface:** `apps/catalyst_api` · **Status:** Phase 2 freeze artifact of the per-UEI precompute build.
**Doctrine:** the grammar is the contract; every client (on-call console, pre-call brief page, anything later) is a consumer of the same queries. The blob schema is DERIVED from this enumeration — a field exists in the blob because a predicate needs it, or it is consciously refused. Never the reverse. Same discipline as `phrase.v2` (grain derived from grammar; refuse, never fall through).

This document is two views of ONE artifact: (§2) the vocabulary axes the `sub_universe_node` grammar serves, each mapped to blob fact → upstream source; (§3) the blob container schema those facts freeze into. A vocabulary change and a schema change are the same PR.

---

## 0. v3 AMENDMENT — the per-UEI BLOB is DEAD (operator-ratified 2026-07-08)

> **This section supersedes §1–§3's blob container.** The vocabulary axes (§2) and the
> `sub_universe_node` grammar are **unchanged** — only the storage/serve substrate changes.
> §1–§3 are retained verbatim below as the frozen record of the (now-superseded) blob era;
> nothing there is deleted. Where §1–§3 say "blob", read §0.

**Why the blob died (reasons of record):**

1. **Denormalization at fleet scale.** A per-target blob copies shared node-grain facts
   (award-state, demand events, entity, win portfolio) into *every* overlapping target's
   payload. v1 hit **136 MB**; even the v2 two-tier rescue is multi-TB across 57K targets
   with overlapping universes.
2. **The two-tier rescue degraded the time axis.** To hold the size budget, v2 collapsed
   exact-day event grain into **monthly buckets** — arbitrary N-day windows became
   month-sum approximations. Exact-day windows are a product requirement; monthly buckets
   die here.
3. **Bespoke serve path.** `sub_universe_serve` duplicated executor machinery (fetch +
   in-memory filter + drilldown) that the standard pinned-target-lane / set-intersect
   executor already provides.

**Replacement: relational pair-grain precompute.** The blob container is replaced by
**two Lance datasets**, served later by the standard executor pattern (NOT this doc's scope):

### `gtm_sub_universe_pairs` — (target_uei × node_uei) pair-specific scalars
- **Grain:** one row per (target_uei, node_uei) — i.e. per node of the target's
  `sub_universe.v3` universe. **BTREE on `target_uei` AND `node_uei`** (both load-bearing
  resolution keys).
- **Carries ONLY pair-specific scalars** — facts that depend on the (target, node) pair and
  cannot be read from a node-grain mart: `matched_prime_obl_60mo`, `matched_farmout_60mo`
  (null when undisclosed), `n_matched_combos`, Definition-C `tcf_farmout_60mo` /
  `tcf_n_combos` (null when no evidence at the target's combos), teaming scalars,
  `band_fit` (node median chunk across the node's matched target-combo farm-out lanes vs
  the *target's* p20–p80 band → `node_median_chunk_60mo` + `band_overlap`), and a compact
  `matched_via_json` (top-5, `matched_via_truncated` flag). Node HQ geo
  (latitude/longitude/geo_precision) is the ONE per-node hydration retained inline (chunked
  `gtm_entity_geo` pull; its cost is measured).
- **Recipe:** `sub_universe_pairs.v2` (v1 → v2 at §0.1: uncapped build_mode
  membership + `family_matched_obl_60mo` / `family_tcf_farmout_60mo` pair columns
  + `demonstrated_families` and `scopes.pop_counties` on the targets row).

### `gtm_sub_universe_targets` — one row per target
- **Grain:** one row per target UEI. **BTREE on `uei`**.
- **Carries:** the `target_analytics` JSON (the pre-call brief, Acts 1–3 — reused verbatim
  from the blob-era computation: pool over lanes ∩ states ∩ 24mo, named peers, percentiles
  with p25/p75, lane trends with min-n=5 nulls) + target scalars (`n_nodes`,
  `n_disclosed`, `n_undisclosed`, `as_of`, `recipe`).

**Node-grain facts are NOT stored per pair.** Award-state, demand events (raw, EXACT-DAY),
entity enrichment, and win portfolio serve at **query time** from the already-indexed
node-grain marts (`gtm_prime_demand_events`, `usaspending_fpds_prime_award_state`,
`gtm_sam_entities`, `gtm_prime_combo_lanes` — all BTREE `uei`). They live once, node-grain,
never copied per overlapping target. **This restores exact-day time windows** — the raw
event rows are read at query time, so any N-day / calendar / fiscal window is exact.
Monthly buckets are dead. (The one measured node-grain hydration cost that killed the blob
batch — an indexed 16K-row award-state pull at **11.8s** — is precisely what moves to query
time here, paid once per node, never precomputed per target.)

**Build model — operator-triggered, on-demand, per target.** No batch over 57K, no cron,
no schedule (operator doctrine 2026-07-08). `build_target(uei)` is invoked per target; the
two marts **grow monotonically** (rebuild-per-target: delete this target's rows —
`target_uei = 'X'` on pairs, the target row on targets — then append the fresh set). The
proving-batch / fleet-wide sweep is retired. `as_of` stamped and disclosed.

**The blob-era files are SUPERSEDED, not deleted.** `sub_universe_full.py` /
`sub_universe_serve.py` / `build_sub_universe_blobs.py` / the `gtm_sub_universe_blobs`
dataset remain on disk as the frozen record. New work targets `sub_universe_pairs.py` +
`build_sub_universe_target.py` + the two datasets above.

**§2 vocabulary + the node grammar are unchanged** — every axis still serves; a scalar
axis reads the pair row, a node-grain axis reads the node-grain mart at query time. A
vocabulary change and a schema change are still the same PR.

---

## 0.1 v3.1 AMENDMENT — five-input model, capability families, uncapped membership (operator-ratified 2026-07-08)

> Extends §0. Full input-model text: `docs/reference/CATALYST_FIVE_INPUT_MODEL_ADDENDUM.md`
> (§1 the model, §1.1–1.2 agency/bases/buildings rulings, §2 families, §3 the three-surface
> Cycle C spec — **C build remains ON HOLD until explicit operator green-light**). This
> section carries the binding substrate rulings.

**1. Five inputs, one universe.** The per-UEI universe presents as five named inputs
(geographic focus, core capabilities, lookalike primes, lookalike subs, deal economics),
each with baked per-target defaults, each tunable live without global rebuild. Inputs
1/2/5 and all monetary/time knobs are **filters over ONE universe** whose membership is
the lookalike-winner rule (widest tier, dim-never-delete). Money and time never add
members. Declared SAM codes: facet/display only, never membership. Vehicle co-holding
(same parent IDV): per-node annotation only, never membership.

**2. Membership is materialized IN FULL — the serving cap never touches the mart.**
`build_target()` writes a pair row for **every** member of the lookalike-winner universe.
`MAX_LIMIT` (5000) is a serving/page parameter only. Rationale of record: the build-time
rank cut (disclosed-first ordering) deleted precisely the undisclosed frontier — on the
2026-07-08 live gate every mid-size target truncated at exactly 5000 and **100% of the cut
rows were undisclosed** — violating dim-never-delete at the substrate and silently emptying
the two knobs (undisclosed tier, family widening) the model exists to serve. The only
build-time truncation is the mega-universe guard (`BUILD_NODE_CAP`, reseller-class targets
per §0 doctrine), always with `nodes_truncated=True` — disclosed, never silent.

**3. Capability families — definition (CORRECTS addendum §2 as first written).**
`family_key = NAICS[:4] + 'x' + psc_family(PSC)` where `psc_family = PSC[0]` when the
first char is a letter (services `R…`/`K…`/`M…`/`S…`, R&D `A…`), else `PSC[:2]` (products:
the 2-digit FSC **group**). One-digit product truncation is wrong — it collapses `1410`
guided missiles / `1510` aircraft / `1903` ships into family "1". Examples:
`541330×R425 → 5413xR`; `336411×1510 → 3364x15`.
- **F1 placement:** family rollups ride the pair build — pair-row JSON dict columns
  `family_matched_obl_60mo` / `family_tcf_farmout_60mo`, plus the target row's
  demonstrated families (top-X by frequency + $, titles inline). No mart rebuilds.
- **F2 placement:** `family_key` column + BTREE on the combo marts at their next
  operator-triggered rebuild — never initiate a rebuild for F2 alone.
- **Null doctrine at family grain:** `family_tcf_farmout_60mo` sums **disclosed** lanes at
  the target's combos only; a family with no disclosed lane is ABSENT from the dict (null
  semantics), never 0. Negative farm-out (net de-obligation — observed live 2026-07-08)
  passes through unclamped.
- **Deal economics stay at true combo grain** and aggregate up. Family-grain medians over
  heterogeneous combos are refused — they would blur the one number the deal-fit story
  depends on.

**4. Execution environment.** Per-target builds execute adjacent to R2 (the deployed API
service path), never as serial laptop batches (retired 2026-07-08: ~95% of a measured
12.5-min/target laptop build was residential-link RTT, cache warmth immaterial). The
trigger script is unchanged; the host moves.

**5. Substrate deltas recorded (2026-07-08).** `usaspending_fpds_prime_award_state`
(82.9M rows) gained `naics_code` + `product_or_service_code` BTREEs (dataset versions
23–24) — unblocks S3 award/flow filtering by combo/family. Agency axis: §2.5.1's REFUSAL
is **superseded** (addendum §1.1) — agency serves from `gtm_txn_events_slim` and the
Cycle B rollups (all carry `awarding_agency_code`, indexed); sub-toptier names still
refuse pending a reviewed alias table.

---

## 1. Container

> **v2 AMENDMENT (2026-07-07, Phase 3b/4 — implemented & gated). SUPERSEDED by §0
> (2026-07-08).** The blob is now
> **two-tier** to hold the single-digit-MB / sub-second-load budget at the high-fan-out
> tail (v1's flat "raw event grain for every node" hit 136 MB; v2 probe = **8.80 MB**,
> ~27 ms parse). Changes below are marked **[v2]**; the grammar is otherwise unchanged.

- **One HOT blob per target UEI**, Lance dataset on R2 (`s3://data-sink/active/gtm_sub_universe_blobs/`), BTREE `uei`. Fetch once per call session (indexed point lookup); ALL universe-filter and target-analytics queries execute in-memory over the fetched payload.
- **[v2] Node tiering.** MATERIAL nodes (disclosed sub-buyers, ranked by `matched_prime_obl_60mo`, cap **1500**) carry full hot hydration: entity, award-state, `matched_via` (capped **5**), and per-node **monthly event buckets**. The undisclosed tail rides as lean **STUBS** (membership + ranking scalars + the base recipe's cheap demand summary — `needs_subs_now_total`, `by_action_type`, counts). The blob is still **total** (every paged member is present); tiering governs hydration depth, not membership.
- **[v2] Event grain → monthly buckets.** The hot blob carries per-node **monthly** buckets keyed by `(action_type, subcontracting_plan, set_aside)` with counts + `first`/`needs` flags (~18× smaller than raw rows). Every in-horizon time / plan / set-aside / action-type predicate serves from these in-memory. Raw event rows are **not** stored in the blob.
- **[v2] Row-exact drilldown — the §1 serving carve-out.** On node drilldown ONLY, the serve path makes exactly **one additional indexed PRECOMPUTE fetch**: raw event grain from `gtm_prime_demand_events` (BTREE `uei`) + win_portfolio from `gtm_prime_combo_lanes` (BTREE `uei`), filtered in-memory to the target's matched combos (so rows reconcile with the node's buckets — verified exact). These are marts (precompute), never the raw spine: **no query-time raw-spine access; refuse, never fall through** both stand, and **beyond-horizon windows still REFUSE** with the horizon named. No separate events sidecar dataset exists — it would duplicate ~106 MB/target across overlapping universes (multi-TB). See `apps/catalyst_api/src/sub_universe_serve.py`.
- **Two payload sections**, one build:
  - `universe` — the tiered node map (the `sub_universe.v3` recipe with paging/quota/limit stripped: those are presentation, the blob is total).
  - `target_analytics` — the pre-call brief facts (Acts 1–3 of `~/Desktop/hq/design-artifacts/pre-call/DATA_CONTRACT.md`, corrected per §4).
- **Refresh = full rebuild** (nightly/weekly batch). No incremental, no invalidation. Schema evolution rides the refresh: change recipe, next batch rebuilds the world. `as_of` stamped and disclosed; "last 30 days" means 30 days back from query `today` over events as fresh as `as_of`.
- **Batch input:** 5y-active subawardees (`edge_count_5y > 0` in `gtm_prime_sub_pairs`): **56,672** live (probed 2026-07-07). Lifetime ceiling 105,159. Proving batch ~350.
- **Null doctrine:** unknown ≠ zero, everywhere. Absent facts are null with basis disclosable. Culls dim, never delete.

## 2. Vocabulary axes → blob fact → source

Status legend: **SHIPPED** (in `sub_universe.v3` today) · **WIDEN** (projection widen of an existing scan) · **NEW** (net-new fact at blob build) · **REFUSED** (not served; refusal names the reason).

### 2.1 Rebound from `phrase.v2` (machinery ports: lex → longest-match bind → refuse; money/code/year literal parsers; sector aliases; event phrases; plan phrases; reserved-boolean discipline)

| Axis | Predicate examples | Blob fact | Source | Status |
|---|---|---|---|---|
| Combos (NAICS/PSC/sectors) — matched | "primes matched in 541330", "engineering lanes" | `node.matched_via[]` / `node.gate_facts{}` | `gtm_prime_combo_lanes` | SHIPPED |
| Combos — full win portfolio | "what else do they win", "primes also winning construction" | `node.win_portfolio[]` (naics, psc, prime_obl_60mo; capped, truncation flagged) | same winners scan, wider projection | WIDEN |
| Events (action types) | "funded", "exercised an option", "terminated" | `node.demand_events.events[]` (event grain, §2.3) | `gtm_prime_demand_events` | WIDEN (grain change) |
| Subcontracting plans | "with a subcontracting plan", "plan attached" | `events[].subcontracting_plan` | same | WIDEN |
| Set-aside / competition | "SB set-aside", "full and open" | `events[].type_of_set_aside_code`, `extent_competed` | same | WIDEN |
| $ literals | "farm-out ≥ $1M", "chunks ≥ $250K" | all $ facts below | — | SHIPPED |
| Entity qualifiers | "sam-active", "dsbs" | `node.entity{}` (§2.4) | `gtm_sam_entities` | NEW (enrichment join) |
| HQ state | "HQ in Texas" | `node.entity.physical_state` (+ lat/lon SHIPPED) | `gtm_sam_entities` / `gtm_entity_geo` | NEW / SHIPPED |
| Time windows | "within N days" (any N), "in 2025", "in fy26", "since march" | `events[].action_date` — computed against `today` at query time | event-grain rows | WIDEN |

### 2.2 Universe-native axes (no `phrase.v2` counterpart)

| Axis | Predicate examples | Blob fact | Source | Status |
|---|---|---|---|---|
| Farm-out at MY combos (Definition C) | "buys my work", "farms out 561730" | `node.target_combo_farmout` (total, per-combo $, median/p75 chunk, n subs, last date; null = no evidence) | `gtm_prime_farmout_combo_lanes` keyed to target's demonstrated combos | **SHIPPED (v3, PR #1068)** |
| Disclosed tier | "disclosed buyers only" / "show the frontier" | `node.disclosed_sub_buyer` + null-pattern | — | SHIPPED |
| Teaming | "3+ repeat partners", "deep bench" | `node.teaming{}` | `gtm_prime_sub_pairs` | SHIPPED |
| Vehicles | "on vehicle <piid>", "has vehicles" | `node.vehicles[]` | `gtm_prime_vehicle_lanes` | SHIPPED |
| Deal-band fit | "farm-out chunk in my band", "chunk overlaps my sizes" | node chunk facts (`median/p75_chunk_60mo`) × `target.deal_band` | farm-out mart + target edges | SHIPPED + NEW (band) |
| Target-relative distance | "wins work within 50 miles of me" | `node.pop{}`: per-node PoP-state $ rollup + `work_usd_within_25/50/100mi` of target HQ + nearest-site distance | events/awards → `usaspending_award_pop_centroids` (zip5→ZCTA point) × target HQ at build | NEW |
| Needs-subs-now | "first actions, plan attached, no subs yet" | derivable from event grain (`is_first_action` ∧ plan ∈ C–H ∧ ¬`has_disclosed_subs`) | event rows | WIDEN |
| Lane trends | "lanes where chunks are growing" | `target.lane_trends[]` (§2.5) + per-lane slope stat | target edges + subaward spine | NEW |

### 2.3 Event grain — THE time-axis decision (decided 2026-07-07; **[v2]** amended same day)

**v1 (superseded):** carry raw demand events at event grain in the blob, capped 500/node. This hit 136 MB at the high-fan-out tail (§1) — raw rows are the wrong thing to store per node.

**[v2] as implemented & gated:**
- **Material nodes** carry per-node **monthly buckets** keyed `(action_type, subcontracting_plan, set_aside)` with `{n, obl, first, needs}`. Every in-horizon time / plan / set-aside / action-type predicate computes over these in-memory — arbitrary N-day windows resolve by summing covered months; calendar/fiscal years are exact. Buckets aggregate the FULL restricted event set (uncapped, ~18× smaller than raw rows).
- **Stub nodes** carry the base recipe's `demand_summary` (`needs_subs_now_total`, `by_action_type`, `n_events_24mo`, `n_plan_added_Y`, `n_terminations_EFX`) — enough for coarse frontier prospecting (needs-subs-now, action mix, recency).
- **Raw rows** (`{action_date, action_type_code, naics_code, psc_code, obligation_delta, is_first_action, has_disclosed_subs, subcontracting_plan, type_of_set_aside_code, extent_competed, idv_type_code, award_key}`) are fetched on **row-exact drilldown** from `gtm_prime_demand_events` (BTREE uei), filtered to the target's combos (§1 carve-out) — reconciles exactly with the node's buckets.

**Depth horizon = the mart's ~24mo window; queries past it REFUSE with the horizon named** — the drilldown is never a horizon escape hatch.

### 2.4 Entity enrichment block (NEW, per node + target)

From `gtm_sam_entities` (BTREE `uei`): `name, cage, physical_city, physical_state, physical_zip, sam_is_active, business_types, primary_naics, naics_codes[], psc_codes[], in_dsbs`. Serves qualifier + declared-code + HQ predicates and the pre-call identity header.

### 2.5 Forced decisions (resolved at this freeze)

1. **Agency axis: REFUSED v1.** `gtm_prime_demand_events` carries no agency columns (probed 2026-07-07). Serving "DoD primes" requires an agency column at the events-mart rebuild — queued as a mart change, vocabulary lands with it. Refusal message names this.
2. **Award-state axis (active/expiring per node): NEW at build.** Per-node `{n_active_awards, n_expiring_180d, next_expiry_date}` from `usaspending_fpds_prime_award_state` scanned at build (build-time spine access is the sanctioned carve-out). Vehicle ceiling year for the target rides the same scan (IDV self-row: `award_id_piid = parent_piid`, `potential_end_date`).
3. **Time depth: 24mo event grain** (§2.3). Longer demand history is a mart-window parameter, not blob architecture.

### 2.6 Pinned parameters

| Parameter | Value | Basis |
|---|---|---|
| Deal band | p20–p80 + median of target's subaward amounts | pre-call contract §1 |
| Trend bucket min-n | 5 (reuse `MVS_MIN_N`) | "a 2-edge median is not a trend"; null + reason below it |
| Trend buckets | calendar year (quarterly only if n supports) | noise floor |
| Peer set | ≥3 shared lanes ∧ ≥1 shared PoP state ∧ deal-band overlap | pre-call contract §5 |
| Pool | subaward spine: target lanes ∩ target PoP states ∩ trailing 24mo | contract §4; "comparable primes" = all placing primes v1 |
| Fragmentation buckets | top5 / next20 / rest | v27 design |
| Deal-fit histogram bins | 12 log-spaced edges $25K→$2M, edges IN payload | v27 fix |
| **[v2]** Node tiering | material = disclosed sub-buyers, ranked by `matched_prime_obl_60mo`, cap 1500; undisclosed tail = lean stubs | single-digit-MB budget at the fan-out tail |
| **[v2]** Event grain (hot) | per-node MONTHLY buckets (material); base demand summary (stub) | ~18× smaller than raw; in-memory time predicates |
| **[v2]** `matched_via` hot cap | 5 per material node (full combo set preserved in `gate_facts` keys) | blob size |
| **[v2]** Drilldown raw cap | 500 events / 50 portfolio per node — served from marts, NOT stored | drilldown payload |
| Blob size budget | single-digit MB per target (v2 probe **8.80 MB**) | fetch-once latency class |

## 3. Blob schema (JSON sketch — authoritative field list)

```
{
  "uei", "as_of", "recipe",                       // [v2] sub_universe_blob.v2 (bakes sub_universe.v3)
  "universe": {
    "nodes": [                                    // [v2] TIERED — every node has `tier`
      // MATERIAL (disclosed, ranked by matched_prime_obl_60mo, cap 1500):
      { uei, name, tier:"material", entity{...§2.4}, latitude, longitude, geo_precision,
        disclosed_sub_buyer, matched_farmout_60mo, matched_prime_obl_60mo,
        n_matched_combos, matched_via[<=5], matched_via_truncated, gate_facts{},
        target_combo_farmout{...v3},                        // Definition C
        teaming{}, vehicles[],
        award_state{ n_active_awards, n_expiring_180d, next_expiry_date },
        pop{...} (v1 deferral, null),                       // vs TARGET HQ
        demand_events{ summary{}, grain:"month", detail_in_mart:"gtm_prime_demand_events",
          buckets{ "YYYY-MM": {n, obl, at{code:ct}, plan{code:ct}, sa{code:ct}, first, needs} } } },
      // STUB (undisclosed tail — membership + ranking + cheap summary):
      { uei, name, tier:"stub", disclosed_sub_buyer, matched_farmout_60mo,
        matched_prime_obl_60mo, n_matched_combos, gate_facts{},   // combo keys preserve membership
        demand_summary{ n_events_24mo, by_action_type{}, needs_subs_now_total,
                        n_plan_added_Y, n_terminations_EFX } }
    ],
    "n_material", "n_disclosed", "n_undisclosed", "n_total", "nodes_truncated"  // full counts
  },
  // [v2] win_portfolio + raw event rows are NOT stored — row-exact drilldown via
  //      sub_universe_serve.fetch_node_detail reads gtm_prime_demand_events /
  //      gtm_prime_combo_lanes (BTREE uei), filtered to the target's combos.
  "target_analytics": {                                     // pre-call brief, Acts 1–3
    "entity": { identity + header stats + trajectory_5yr_pct },
    "scopes": { lanes[], performance_states[], deal_band{low,high,median}, window_months },
    "current_performance": { customer_composition[], top_buyer_pct, top_two_pct,
        capabilities[], geography{states[], buyer_hq[]},
        dependencies{ top_buyer, top_lane, vehicle_exposure{..., ceiling_year} },
        lane_trends[{combo, own_series[{bucket,n,total_usd,median_chunk}],
                     market_series[...], median_slope_pct_yr}] },
    "adjacent_market": { pool{ total, prime_count, largest_single_share_pct,
        fragmentation[], entity_capture, named_primes[] },   // NAMED, ranked, top-N
        placement[], deal_fit{ placed_median, entity_median, within_band_pct,
        distribution[], bin_edges[] } },
    "field": { comparable_set{ count, definition, named_peers[],  // NAMED, ranked
        set_capture, median_peer_capture, entity_capture, pool_total, others_capture },
        percentiles[{dimension, entity_value, peer_median, p25, p75, percentile,
                     higher_is_better}], converting[] }
  },
  "meta": { sources[], doctrine, timings }
}
```

## 4. Corrections to the pre-call DATA_CONTRACT (binding)

1. Consumer scripts named in the contract (`build_sub_universe_page_modal.py`, `build_gtm_sub_universe_page.py`) **do not exist** — the page renders from this blob's `target_analytics` section; serving path is the blob store.
2. Payload additions the v27 design proves: **named pool primes**, **named ranked peers**, **histogram bin edges**, **p25/p75 per percentile dimension**.
3. §6.1 recipient resolution is already solved (`subawardee_uei` resolved on `usaspending_subaward_canonical`).

## 5. Build dependencies this freeze creates

| Deliverable | Phase | Status / Needs |
|---|---|---|
| `gtm_sub_combo_lanes` mart (sub_uei × naics × psc: $, n, chunk percentiles + per-sub states/band) | 3a | **DONE** — powers peer set + Act 3 |
| `build_blob(uei)` emitting this schema (`sub_universe_full`) | 3b | **[v2] DONE & gated** — two-tier hot blob (8.80 MB probe, buckets reconcile exact); `award_key`↔`piid` + IDV self-row verified |
| `gtm_sub_universe_blobs` dataset (BTREE uei) + `sub_universe_serve` (fetch_blob / fetch_node_detail) | 4 | **code DONE; proving batch running** — drilldown reads gtm_prime_demand_events / gtm_prime_combo_lanes (BTREE uei) |
| Predicate engine executing §2 vocabulary | 5 | frozen field map (this doc) — scalar axes over hot nodes; time/plan/set-aside over monthly buckets; row-exact via drilldown |
| Agency column on `gtm_prime_demand_events` | ~~queued~~ | **SUPERSEDED (§0.1.5)** — agency serves from `gtm_txn_events_slim` + Cycle B rollups (#1072) |
| `naics_code`/`product_or_service_code` BTREEs on `usaspending_fpds_prime_award_state` | v3.1 | **DONE 2026-07-08** (versions 23–24) — S3 flow surface unblocked |
| **[v3] `gtm_sub_universe_pairs` + `gtm_sub_universe_targets`** (§0) — pair-grain precompute replacing the blob; `sub_universe_pairs.build_target(uei)` + `build_sub_universe_target.py` | v3 | **DONE** — operator-triggered per-target, monotonic grow; node-grain facts serve at query time from indexed marts (exact-day windows restored) |
| Standard executor serve path (pinned target lane + set intersects over the two v3 marts) | v3-serve | NOT this scope — replaces `sub_universe_serve` |
