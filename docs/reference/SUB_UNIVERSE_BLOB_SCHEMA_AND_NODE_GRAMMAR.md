# Sub-Universe Blob Schema ≙ `sub_universe_node` Grammar Enumeration

**Date frozen:** 2026-07-07 · **Owner surface:** `apps/catalyst_api` · **Status:** Phase 2 freeze artifact of the per-UEI precompute build.
**Doctrine:** the grammar is the contract; every client (on-call console, pre-call brief page, anything later) is a consumer of the same queries. The blob schema is DERIVED from this enumeration — a field exists in the blob because a predicate needs it, or it is consciously refused. Never the reverse. Same discipline as `phrase.v2` (grain derived from grammar; refuse, never fall through).

This document is two views of ONE artifact: (§2) the vocabulary axes the `sub_universe_node` grammar serves, each mapped to blob fact → upstream source; (§3) the blob container schema those facts freeze into. A vocabulary change and a schema change are the same PR.

---

## 1. Container

- **One blob per target UEI**, Lance dataset on R2 (`s3://data-sink/active/gtm_sub_universe_blobs/`), BTREE `uei`. Fetch once per call session (indexed point lookup); ALL queries execute in-memory over the fetched payload. No query-time access to any raw spine, ever.
- **Two payload sections**, one build:
  - `universe` — the node map (the `sub_universe.v3` recipe with paging/quota/limit stripped: those are presentation, the blob is total).
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

### 2.3 Event grain — THE time-axis decision (decided 2026-07-07)

The blob carries the node's demand events at **event grain**, restricted to the node's matched combos, capped per node (cap disclosed via `events_truncated`): `{action_date, action_type_code, naics_code, psc_code, obligation_delta, is_first_action, has_disclosed_subs, subcontracting_plan, type_of_set_aside_code, extent_competed, idv_type_code, award_key}`. Summaries (`by_action_type`, `needs_subs_now`) remain as baked conveniences but every time predicate computes over the rows — arbitrary N-day windows, calendar/fiscal years. **Depth horizon = the mart's ~24mo window; queries past it REFUSE with the horizon named.**

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
| Event rows per node cap | 500 (truncation flagged) | blob size budget |
| Blob size budget | single-digit MB per target | fetch-once latency class |

## 3. Blob schema (JSON sketch — authoritative field list)

```
{
  "uei", "as_of", "recipe",                       // sub_universe_blob.v1 (bakes sub_universe.v3)
  "universe": {
    "nodes": [ { uei, name, entity{...§2.4}, latitude, longitude, geo_precision,
        disclosed_sub_buyer, matched_farmout_60mo, matched_prime_obl_60mo,
        n_matched_combos, matched_via[], matched_via_truncated, gate_facts{},
        target_combo_farmout{...v3},                        // Definition C
        win_portfolio[], win_portfolio_truncated,
        teaming{}, vehicles[],
        award_state{ n_active_awards, n_expiring_180d, next_expiry_date },
        pop{ states[{state, work_usd_24mo}], work_usd_within_25mi, _50mi, _100mi,
             nearest_site_mi },                              // vs TARGET HQ
        demand_events{ events[...§2.3], events_truncated, summary{} } } ],
    "n_disclosed", "n_undisclosed"                          // full counts, no paging
  },
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

| Deliverable | Phase | Needs |
|---|---|---|
| `gtm_sub_combo_lanes` mart (sub_uei × naics × psc: $, n, chunk percentiles + per-sub states/band) | 3a | subaward spine; powers peer set + Act 3 |
| `execute_sub_universe_full(uei)` emitting this schema | 3b | v3 store + this doc; verify `award_key`↔`prime_award_piid` join + IDV self-row index |
| Predicate engine executing §2 vocabulary | 5 | frozen field map (this doc) |
| Agency column on `gtm_prime_demand_events` | queued | events mart rebuild (unblocks §2.5.1) |
