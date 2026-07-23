# Sidecar Gap Report — 2026-07-22 — sector geo substrate (award-grain county membership)

- **Date:** 2026-07-22
- **Sidecar artifact:** `query-sidecar/query_sidecar_20260722T032457Z.duckdb` (113 tables)
- **Session topic:** Equipment-provider GTM — sector files (desk coverage units defined as
  county-FIPS sets, e.g. Hampton Roads Sector = 13 VA FIPS). Every sector-file page must cut
  on ONE membership definition; operator-ruled 2026-07-22.

## Gap 1 — No county at award grain (sector membership impossible for award-state cuts)

1. **Intent** — "Active awards / obligated / current value / end-of-term IN THE SECTOR",
   where sector = county_fips set. Same membership substrate as transaction-grain pages
   (`txn_events_combo_by_geo.pop_county_fips`).
2. **Why not the sidecar** — `missing column`. `usaspending_fpds_prime_award_state` has no
   place-of-performance geo; `award_pop_centroids_by_key` has `zip5, latitude, longitude,
   state_code` but **no `pop_county_fips`**, and no zip→county crosswalk table exists warm.
   County-exact award-grain sector cuts are unexpressible.
3. **What I ran instead** — 45-mile haversine envelope from the sector core
   (36.85, −76.35) over `award_pop_centroids_by_key` ⋈ `usaspending_fpds_prime_award_state`
   (172 awards · 138 firms · $3.52B obligated · $4.13B current value). Envelope ≠ county set;
   the two page families (FY inflows = county-exact, active = envelope) are on different
   substrates — operator-flagged as unacceptable.
4. **Cost** — ~4.3 s per query (centroids⋈state join + trig per row); recurring on every
   sector-file build/refresh and every "active in sector" question, for every sector.
5. **Recurrence** — recurring and structural: the sector grid (~20 planned sectors) makes
   county membership THE canonical geo cut for the whole equipment-provider program.
   **Ask:** add `pop_county_fips` to `award_pop_centroids_by_key` (derivable upstream from
   the canonical txn PoP county the by_geo mart already carries — latest-txn-per-award), or
   ship a `zip_county_xwalk` reference table.
