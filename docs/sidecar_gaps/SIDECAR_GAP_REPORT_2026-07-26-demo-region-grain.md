# Sidecar Gap Report — 2026-07-26 — demo region-grain aggregates

- **Date:** 2026-07-26
- **Sidecar artifact:** `query-sidecar/query_sidecar_20260724T044059Z.duckdb` (built 2026-07-24T04:40:59Z, 126 tables)
- **Session topic:** Equipment-yard GTM demo — macro-region video flow (gc-hq-new ⌘B walk).
  Operator-articulated demand: "too much computing / lag on the actual demo" — the demo-bake
  scripts recompute region-grain aggregates from raw transaction marts on every iteration.

---

## Entry 1 — Macro-region firms stats (won-$500K+, median award, book growth, first-time winners)

1. **Intent** — For each of the 14 macro regions (and 7 drill regions): how many firms won
   ≥$500K in-region FY23–25; median award size (≥$250K); FY23→FY25 book growth; firms whose
   first in-region action was FY25.
2. **Why not the sidecar** — **wrong grain / missing sort (too slow unpruned).** The sidecar
   serves `txn_events_combo` / `gtm_txn_events_slim` / `award_geo_state` at transaction/award
   grain; no region-grain rollup exists. Multi-state `pop_state IN (…)` aggregation over the
   full mart per region, per metric.
3. **What I ran instead** — `scripts/demo_bakes/bake_drill_demo.py`: per region, a 4-subquery
   aggregate over a CTE of `txn_events_combo` (cols: uei, award_key, obligation, action_date)
   filtered by `pop_state IN (…)`; originally an `award_geo_state → gtm_txn_events_slim` join
   (award-level PoP semantics), which **408s server-side on large macros**.
4. **Cost** — join form: HTTP 408 (>130s) on the first large macro after 8 regions (~25 min
   elapsed). Combo form: ~1–3 min per large macro; 21 regions per bake run. Rows scanned:
   full mart per region per query; returned: 1 row.
5. **Recurrence** — **Recurring.** Every demo-flow iteration, every region added, every data
   refresh triggers a full rebake; 14 macro-region videos are planned.

## Entry 2 — Macro-region active-book rollup (active $, firms, per-NAICS flow-down input)

1. **Intent** — Per region: active-now obligations, distinct firms holding them, and per-NAICS
   sums (input to equipment flow-down weighting).
2. **Why not the sidecar** — **wrong grain.** `award_geo_state` is award×state; no per-region
   (or per-state pre-aggregated) active rollup with NAICS breakdown.
3. **What I ran instead** — per region: `award_geo_state` GROUP BY award_key → per-NAICS sums
   (cols: award_key, uei, naics_code, obligated, is_terminated, current_end_date), plus a
   second `count(DISTINCT uei)` pass over the same filter.
4. **Cost** — tens of seconds per region × 21 regions × 2 passes.
5. **Recurrence** — **Recurring** (same trigger set as Entry 1; also the live "active in your
   region" card on every call/video regeneration).

## Entry 3 — Region FY23–25 obligations by NAICS (share + flow-down ratio)

1. **Intent** — Per region: FY23–25 obligations by NAICS (drives national-share → OBBA uplift
   and factor-weighted equipment ratio).
2. **Why not the sidecar** — **wrong grain.** Same transaction-grain marts; no
   region×NAICS×FY rollup.
3. **What I ran instead** — per region: `SELECT naics_code, sum(obligation) FROM
   txn_events_combo WHERE pop_state IN (…) AND action_date BETWEEN … GROUP BY 1`.
4. **Cost** — ~30–90s per large region; 21 regions per bake.
5. **Recurrence** — **Recurring.**

## Entry 4 — National 5-yr obligations by NAICS (industry-shape pie)

1. **Intent** — FY21–25 national obligations by NAICS6 → KLEMS 8-sector rollup (the video's
   "$3.65T → its shape" card).
2. **Why not the sidecar** — **wrong grain.** Full-history national GROUP BY over
   `txn_events_combo` at query time; no NAICS×FY national rollup.
3. **What I ran instead** — `bake_industry_shape.py` (extended): `SELECT naics_code,
   sum(obligation) FROM txn_events_combo WHERE action_date BETWEEN DATE '2020-10-01' AND
   DATE '2025-09-30' GROUP BY 1`.
4. **Cost** — single national scan (not yet run this session; the active-book variant of the
   same shape ran in prior cycles at ~1 min). 408 risk at the 130s client timeout.
5. **Recurrence** — **Recurring** (every shape-card regeneration; sibling of Entry 3 —
   the same rollup at national grain).

## Entry 5 — Archetype work-order picks (per region × 3 archetypes)

1. **Intent** — Per region: top real award $25–250M (fallback ≥$5M) for each of 3 fixed
   NAICS×PSC archetypes.
2. **Why not the sidecar** — **wrong grain / too slow unpruned** — per-(region×archetype)
   top-N over `txn_events_combo` with NAICS×PSC IN-list + PoP filter; 3 queries × 21 regions,
   with a second fallback query when the band is empty.
3. **What I ran instead** — the two-tier top-N queries in `bake_drill_demo.py` (cols:
   award_key, obligation, uei, pop_city_name, pop_state, recipient_state).
4. **Cost** — ~10–30s per query; up to 126 queries per bake run.
5. **Recurrence** — **Recurring.**

---

## Ranking (recurrence × cost)

1. **Entry 1** — highest: per-region multi-metric firm stats; already caused a failed bake
   (408) and dominates wall time.
2. **Entry 3 / Entry 4** — same underlying shape (region|national × NAICS × FY obligation
   rollup); high recurrence, moderate-high cost each.
3. **Entry 2** — active rollup; moderate cost, high recurrence (live demo card).
4. **Entry 5** — many small queries; cost is aggregate, not per-query.

Demand only — no proposed solutions.

---

# Disposition — `/sidecar-gaps` Mode 2, 2026-07-26

Gated against serving artifact `query_sidecar_20260724T044059Z` (126 tables, /healthz-verified).

## Verification pass (probe-before-believe)

| Report claim | Probe verdict |
|---|---|
| E1 current form = `txn_events_combo` CTE filtered `pop_state IN (…)` | **Corrected.** `bake_drill_demo.py:61-69` on disk still runs the `award_geo_state → gtm_txn_events_slim` award-PoP join form; the combo form was an in-session substitute, never committed. Both PoP semantics are therefore live and the promoted set must serve **both** (see `pop_award_fy`). |
| E4 `bake_industry_shape.py` scans `txn_events_combo` FY21–25 | **Corrected.** On-disk version reads `combo_award_active_state` (active-book variant). The FY21–25 national NAICS scan is a *planned* extension — report says so honestly ("not yet run this session"). Promoted on the strength of E3, whose national twin it is. |
| "no region-grain rollup exists" | **Confirmed.** Nearest existing: `pop_place_fy` (place × FY, no NAICS/PSC), `combo_award_active_state` (combo grain, no geo). Neither answers a region cut. |
| Region definitions on Lance | **Confirmed + extended.** `reference/demo_region_catalog` (3,222 rows) and `reference/state_region_county_map` (1,398) exist; the **14 macro regions are NOT on Lance** — hardcoded as Python dicts in `bake_company_regions.py` and `bake_macro_region_econ.py`. Macro regions are state unions, so a place-grain atom composes them without a catalog. |

## Gate

| Entry | Verdict |
|---|---|
| E1 firm stats | **Promote** — structural, demand-evidenced (recurring × costly, one 408'd bake). |
| E2 active-book rollup | **Promote** — structural. |
| E3 region × NAICS × FY | **Promote** — structural. |
| E4 national × NAICS × FY | **Promote** — rides E3's mart as the no-geo-predicate case; zero extra structure. |
| E5 archetype picks | **Promote** — structural, shares E1's award-grain mart. |

## Build scope block

**The promote is deliberately NOT the 21 baked region rows the demand implies.** Regions here
are unions of places — macro regions are state unions, drill regions are county-FIPS sets, and
every new deal adds a region. Baking region rows buys one rebuild per region. The atoms below
compose **any** region (macro, drill, state, county, CBSA, a region invented on a call) with no
rebuild. Grain carries `pop_state` **and** `pop_county_fips` because 18.1M of 108M actions
(16.8%) have no county — county alone would silently drop them from state-scoped regions.

### Ships from demand (structural)

| Mart | Grain | Probed rows | Serves |
|---|---|---|---|
| `pop_combo_fy` | pop_state × pop_county_fips × naics × **psc** × fy | 10.83M | E3, E4 |
| `pop_entity_fy` | pop_state × pop_county_fips × uei × fy | 7.36M | E1 (firms ≥$500K, growth, first-time winners) |
| `pop_award_fy` | pop_state × pop_county_fips × naics × psc × award_key × fy, awards ≥$100K | 10.49M | E1 (median award), E5 (archetype top-N) |
| `award_geo_active` | award-grain live book, place-sorted | 0.26M | E2 |

Floor rationale (`pop_award_fy`): unfloored, this grain is **100.2M rows** (probe-measured — no
compression at all vs the 108M fact). The ≥$100K award floor cuts the probe side to 16.9M
actions → 10.49M cells and sits an order of magnitude below both metrics that read it (median
over awards ≥$250K; archetype picks ≥$5M), so both stay **exact**.

### Rides as adjacency (free — same GROUP BY / same join)

| Rider | Rationale |
|---|---|
| **`psc_code` in `pop_combo_fy`'s grain** | **Operator-directed 2026-07-26.** Work categories and archetypes are NAICS×PSC-defined; category-scoped cuts must answer post-build. Probed cost: 3.73M → 10.83M rows. |
| `award_pop_state` / `award_pop_county_fips` on `pop_award_fy` | One mart answers under **both** PoP semantics the bakes use — transaction-level (each action where it happened) and award-level (`award_geo_state` per-field `arg_max`, what the firms/econ bakes key on today). Avoids a second mart. |
| `hq_state` + `is_nonlocal` on `pop_entity_fy` | Local-vs-non-local firm split; also serves `bake_macro_region_econ`'s outside-HQ % directly. Same 1:1 uei leg `_AWARD_GEO_STATE_SQL` already uses. |
| `first_action_date` / `last_action_date` | E1's first-time-winner metric needs the min; max is free on the same scan. |
| `obl_set_aside` on both rollups | "Set-aside share of region spend" / "firms that actually win set-aside work" — precedent `gtm_entity_fy_won`. |
| `n_actions`, `award_ct` | The immediate "how much activity / how many awards" follow-up. |
| `pop_city_name`, `recipient_state`, `award_topology`, `awarding_agency_code`, `type_of_set_aside_code` on `pop_award_fy` | E5's display fields plus "who bought it / is it a vehicle order". |
| `demo_region_catalog`, `state_region_county_map`, `equipment_flowdown_factors` (Tier D) | Region membership + flow-down weights become an in-sidecar join instead of a Lance round-trip and a client-assembled 300-county `IN`-list. 4,680 rows total. |

### Next-question simulation (each answerable post-build, or parked)

- *Same stats at state / national grain* → sum the atom's places. ✓
- *Name the firms* → `pop_entity_fy.uei` ⋈ `gtm_sam_entities`. ✓
- *Set-aside split; local vs non-local* → riders above. ✓
- *Trend FY21→FY25* → `fy` retained for all years (1962–2026). ✓
- *Next 5 archetype picks, not just top 1; what was the work* → `pop_award_fy` ⋈ `award_descriptions`. ✓
- *What's expiring in this region in 180 days; which agency* → `award_geo_active`. ✓
- *Agency dimension on `pop_combo_fy`* → **parked**, ×3–5 row multiplier (→40M+). Structural, needs demand.
- *NAICS→KLEMS collapse as a join* → **parked**; the prefix-fallback + PATCH logic lives in `_shared.klems_mapping()` and `bea_bls_klems` is already warm.
- *Median / top-N over awards below $100K* → **parked** (100.2M rows unfloored).

### Stays gated

- Everything in PLATE's parked ledger.
- **`sbir-phase-ladder`** — PLATE listed it ready-on-trigger "if the build touches
  `txn_events_combo`". The trigger technically fires (every build rebuilds that fact), but it is
  **not adjacent to the region-grain thought** and this build is demo-blocking. Deferred to keep
  blast radius on the demo path; recorded as next cycle's lead candidate.

## Builder change

`pipelines/query_sidecar/build_query_sidecar.py` — 4 manifest specs (Tier C) + 3 reference specs
(Tier D) + 4 SQL constants + 4 dispatch branches.

- `_preflight()` green (flag dispatch + declared ordering + tier closure).
- `python3 -m pytest pipelines/query_sidecar/test_fixture_explain.py -q` → **3 passed**.
- Join plans EXPLAIN'd through the dispatch path: `pop_entity_fy` → `HASH_JOIN (RIGHT)`,
  `pop_award_fy` → `HASH_JOIN (INNER)`. No `NESTED_LOOP` / `CROSS_PRODUCT`. The `pop_award_fy`
  $-floor is a build-side gate inside the `keys` CTE, never in the `ON` clause.
- Fixture bug fixed in passing: `life_to_date_obligated` was typed `DATE` in `FIXTURE_SCHEMAS`
  (2 occurrences); it is `DOUBLE` live. The mistype had never been exercised because no prior
  mart compared that column numerically.
- Parity: all four are reducing → `aggregate: True` (≥50% floor of the previous artifact's
  count, `>0` on first build).
