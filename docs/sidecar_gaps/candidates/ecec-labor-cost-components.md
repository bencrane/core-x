# ecec-labor-cost-components

**Status:** `promoted` — query_sidecar_20260724T044059Z (2026-07-24, ledger id 46, PR #1337). bls_ecec_costs (627,050, EXACT) + bls_ecec_burden (321, EXACT), plain copies. Health-insurance share 7.3% now 18.6ms warm (was ~6min credentialed Lance-direct). Series key already decoded; sweep added nothing (SELECT * is maximal). bls_oews_2025 stays parked (no staffing demand). Mandatory consumer predicates (area/datatype/year+period/hierarchy level) documented in AGENT_GUIDE §3.

## Capability

BLS ECEC compensation-component decomposition warm — wages/benefits/healthcare share of
total compensation by ownership × industry × occupation (the demo's "7.3% → ≈$75B
healthcare" class of numbers), without leaving the sidecar for credentialed Lance access.

## Evidence trail

- 2026-07-14 — [processed/SIDECAR_GAP_REPORT_2026-07-14-labor-pricing-entry-hop.md](../processed/SIDECAR_GAP_REPORT_2026-07-14-labor-pricing-entry-hop.md)
  Disposition: `bls_ecec_costs` (627k) / `bls_ecec_burden` (321) parked structural-gated,
  no demand — calibration detail behind the composed `naics_labor_share` scalar.
- 2026-07-23 — [SIDECAR_GAP_REPORT_2026-07-23-demo-narrative-decomposition.md](../SIDECAR_GAP_REPORT_2026-07-23-demo-narrative-decomposition.md)
  Entry 3 (ranked #3): the component split demanded; ~6 min wall across five attempts,
  credentialed pylance + local DuckDB + doppler R2 creds for 27 result rows; recurring —
  every demo bake cites the split; per-industry burden components are the next asks and
  hit the same wall.

## Proposed shape

- Tier-D generic copies: `bls_ecec_costs` (627,050 rows) sorted by series-family key, plus
  sibling `bls_ecec_burden` (321 rows).
- Projected artifact delta: **~0.05 GiB** (negligible).

## Adjacency candidates

- `bls_oews_2025` (413k, staffing-pattern detail) — same parked family, still no card
  shape; see [bls-oews-staffing-patterns.md](bls-oews-staffing-patterns.md). Take only if a
  staffing question lands by gating time.
- Series-id decode columns (ownership/industry/occupation/datatype split out of the CM
  series key) so consumers don't re-derive the `CMU2__0000000000P`-style filters.

## Notes

Two dated demand points on the identical table = the recurrence bar the park asked for.
Cheapest open candidate on the plate.
