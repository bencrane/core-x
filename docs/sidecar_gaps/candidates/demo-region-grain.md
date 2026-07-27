# demo-region-grain

**Status:** `promoted` — built 2026-07-26, artifact `query_sidecar_20260726T231318Z` (133 tables)

## Capability

Precomputed region-grain (macro-region + drill-region + national) rollups of the GTM
transaction/award marts serving the demo-bake layer: per-region firm stats (firms winning
≥$500K FY23–25, median award, FY23→FY25 book growth, first-time winners), active-book
rollup (active $, distinct firms, per-NAICS sums), FY-obligations × NAICS rollup
(national-share / OBBA-uplift / equipment flow-down inputs), and archetype top-award picks.
Replaces per-region full-mart scans in the bake scripts with warm keyed lookups.

## Evidence trail

- 2026-07-26 — operator-articulated demand: "too much computing / lag on the actual demo" —
  the demo-bake scripts recompute region-grain aggregates from raw transaction marts on
  every iteration; 14 macro-region videos planned.
- 2026-07-26 — [SIDECAR_GAP_REPORT_2026-07-26-demo-region-grain.md](../SIDECAR_GAP_REPORT_2026-07-26-demo-region-grain.md)
  Entry 1 (ranked #1): per-region multi-metric firm stats (≥$500K firms, median award, book
  growth, first-time winners) — 4-subquery aggregate over `txn_events_combo` per region;
  the award-level join form **408s server-side on large macros** (>130 s after 8 regions,
  ~25 min elapsed); combo form ~1–3 min per large macro × 21 regions. Recurring on every
  demo iteration / region add / data refresh.
- 2026-07-26 — same report, Entry 2 (ranked #4): per-region active-book rollup (active $,
  firms, per-NAICS flow-down input) — `award_geo_state` GROUP BY per region, two passes;
  tens of seconds × 21 regions × 2. Recurring; also the live "active in your region" card.
- 2026-07-26 — same report, Entry 3 (ranked #2): region FY23–25 obligations by NAICS
  (share + flow-down ratio) — full-mart GROUP BY per region; ~30–90 s per large region × 21.
  Recurring.
- 2026-07-26 — same report, Entry 4 (ranked #2, sibling of Entry 3): national FY21–25
  obligations by NAICS6 → KLEMS 8-sector rollup (the "$3.65T → its shape" card) — single
  national scan, ~1 min class, 408 risk at the 130 s client timeout. Recurring per
  shape-card regeneration.
- 2026-07-26 — same report, Entry 5 (ranked #4): archetype work-order picks — two-tier
  top-N per (region × 3 archetypes), up to 126 queries per bake at ~10–30 s each. Recurring.
- 2026-07-23 — [processed/SIDECAR_GAP_REPORT_2026-07-23-demo-narrative-decomposition.md](../processed/SIDECAR_GAP_REPORT_2026-07-23-demo-narrative-decomposition.md)
  Entry 1 recurrence note: "region-scoped versions of the same distinct-places shape are
  coming for Arc 3" — the region-grain demand was flagged before this report existed.
- 2026-07-23 — same report, Entry 2 recurrence note: "the Arc-3 region cut re-asks it
  [non-local share] at radius grain … per prospect" — the geographic thesis re-asked at
  region scope on every bake. (Award-grain substrate shipped 2026-07-24 as
  `award_geo_state`; the region-grain rollup on top of it did not.)

## Proposed shape

- `demo_region_firm_stats` — region×metric rollup keyed by region label: firms_500k_fy23_25,
  median_award, book_growth_fy23_fy25, first_time_winners_fy25, plus the Entry-2 active-book
  columns (active_obligated, active_firms). ~21 rows.
- `demo_region_naics_fy` — region × NAICS × FY obligation rollup (Entry 3). The national
  NAICS×FY rollup (Entry 4) rides as the `region='US'` row-set of the same table (or a
  separate `demo_national_naics_fy` if the FY window differs — Entry 4 wants FY21–25).
  ~21 regions × ~1,200 NAICS6 × 5 FY ≈ 130K rows upper bound.
- `demo_region_archetype_awards` — per (region × archetype × tier) top award (Entry 5):
  21 × 3 × 2 ≈ 126 rows.
- Region definitions live in `reference/macro_region_catalog` + `demo_region_catalog`
  (already on Lance) — the build derives `pop_state IN (…)` membership from them, no
  hardcoded state lists.
- Projected artifact delta: **trivially small** — 21 regions × small metric sets;
  <1M rows, <100 MiB total including sorts.

## Adjacency candidates

- State-grain rollup for all 51 states — macro regions are state unions, so the same build
  pass emits the state grain for free; any future region definition composes from it
  without a rebuild.
- Per-region KLEMS 8-sector rollups (the Entry-4 NAICS6→KLEMS collapse, materialized at
  region grain rather than recomputed client-side).
- `equipment_flowdown`-weighted columns on the region×NAICS rollup (the flow-down ratio is
  currently composed in the bake script from the Entry 2/3 outputs).

## Status

`promoted` — shipped 2026-07-26 as four place-grain ATOMS, not the pre-baked region rows
this dossier originally proposed. Regions here are unions of places and every deal adds one,
so baked rows would have bought a rebuild per region; the atoms compose any region for free.

- `pop_combo_fy` (9,145,055) — place x naics x **psc** x fy. PSC in the grain is an
  operator-directed rider (2026-07-26): work categories are NAICS x PSC-defined.
- `pop_entity_fy` (8,995,497) — place x uei x fy, unfloored, + hq_state/is_nonlocal.
- `pop_award_fy` (8,911,133) — place x combo x award x fy, awards >=$100K, carrying BOTH
  transaction-level and award-level PoP.
- `award_geo_active` (263,488) — the live book, place-sorted.
- Tier D riders: `demo_region_catalog`, `state_region_county_map`,
  `equipment_flowdown_factors`.

Delta +0.72 GiB; the four marts cost 45.7 s of a 64.8-min build.

Outcome: the demo-bake region loops went **584.5 s -> 6.8 s of server time (86x)**; the two
bakes now run end-to-end in 42.5 s and 11.7 s. Disposition + measured per-metric deltas:
[processed/SIDECAR_GAP_REPORT_2026-07-26-demo-region-grain.md](../processed/SIDECAR_GAP_REPORT_2026-07-26-demo-region-grain.md).

Adjacency candidates NOT taken (still open): agency dimension on the NAICS rollup (x3-5 row
multiplier), NAICS->KLEMS collapse as a materialized join table, award grain below $100K.
