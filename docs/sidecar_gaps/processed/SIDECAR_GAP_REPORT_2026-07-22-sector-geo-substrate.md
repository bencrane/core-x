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

---

## Disposition (sidecar-gaps Mode 2, 2026-07-24 — artifact `query_sidecar_20260724T044059Z`, ledger id 46)

Probe-verified before build (every claimed column tested against live Lance + serving).

| # | Verdict | What shipped |
|---|---|---|
| Gap 1 | **Routing fix + Promote (correctness)** | The report's premise — "county-exact award-grain sector cuts are unexpressible" — is **REFUTED**. They were expressible the whole time and fast: a `DISTINCT award_key` semi-join off the geo-sorted `txn_events_combo_by_geo` into `usaspending_fpds_prime_award_state` returned the 13-FIPS Hampton Roads sector in 740 ms (2,912 active awards / 1,116 firms). The 45-mile haversine envelope the report shipped instead returned **172 awards / 138 firms — a 94% under-count** and 6× slower; any sector file built on it is invalid. That correction ships to `QUERY_SIDECAR_AGENT_GUIDE.md` §4 (below), independent of any build. Separately **promoted** `award_geo_state` (1/award · 82.87M, EXACT parity, ZERO R2 read — from_table off award_state, PoP derived by per-field `arg_max` over `txn_events_combo`) so the same cut is now a single predicate: Hampton Roads active = **2,860 awards / 1,103 firms / $35.4B obligated / $49.2B current value in 59 ms**. County reference authorities `census_county_adjacency` (the honest neighbor-set replacement for the haversine envelope), `national_county2020`, `census_county_gazetteer_2023`, `census_county_cbsa_2023` shipped alongside. |

**Honest-partial disclosure (on the record):** county fill at award grain tops out at ~62% on the active universe (source-bounded by `pop_county_fips` fill, worse on IDVs/vehicles). The mart makes the sector number honest, not complete; full coverage needs a separate vehicle-backfill cycle (parked). Merged in PR #1337; artifact 68.37 → 73.45 GiB (+5.08, +13 marts across this + the demo-narrative + novation reports); build 36.8 min.
