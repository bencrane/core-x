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
