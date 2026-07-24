# award-grain-geo-spine

**Status:** `open`

## Capability

One award-grain geography spine: per award key — PoP `zip5`, `pop_county_fips`, `pop_state`,
holder HQ state, obligated/current value, and window dates — so county-exact sector cuts,
distinct-places-of-work counts, and PoP-vs-HQ (non-local / import-ratio) shares all run warm
without re-paying 30M-row-class joins. Merges three independently-reported asks that are the
same substrate.

## Evidence trail

- 2026-07-22 — [SIDECAR_GAP_REPORT_2026-07-22-sector-geo-substrate.md](../SIDECAR_GAP_REPORT_2026-07-22-sector-geo-substrate.md)
  Gap 1: no county at award grain; sector (= county_fips set) cuts unexpressible;
  45-mile haversine envelope workaround ~4.3 s/query, operator-flagged unacceptable
  (envelope ≠ county set; two page families on different substrates). Recurs on every
  sector-file build across the ~20-sector grid.
- 2026-07-23 — [SIDECAR_GAP_REPORT_2026-07-23-demo-narrative-decomposition.md](../SIDECAR_GAP_REPORT_2026-07-23-demo-narrative-decomposition.md)
  Entry 1 (ranked #2): distinct PoP zip5s over FY21–25 — twice OOMed the serving instance;
  shipped as a bound ("20,000+"), question still UNANSWERED.
- 2026-07-23 — same report, Entry 2 (ranked #1): non-local share of active-award dollars
  (award ⋈ PoP ⋈ HQ 3-way join re-paid per question; ~25 s per bake; historical variant is
  Entry 1's OOM). The demo's geographic thesis; re-asked per prospect/region/bake.

## Proposed shape

- New award-grain mart (~30M rows): `award_key, uei, zip5, pop_county_fips, pop_state,
  hq_state, obligated, current_value, first_action_date, last_action_date, active_flag` —
  sorted `pop_county_fips` (sector cuts) with the county derived upstream
  (latest-txn-per-award, as `txn_events_combo_by_geo` already does at txn grain).
- Alternative minimal form: add `pop_county_fips` to `award_pop_centroids_by_key` + ship a
  `zip_county_xwalk` reference table (small), plus an `hq_state` column ride.
- Projected artifact delta: **~1–2 GiB** (one ~30M-row narrow mart incl. sort).

## Adjacency candidates

- `pop_congressional_district` if present in source (same latest-txn derivation).
- County/state name references (zip_county_xwalk carries names for free).
- Radius-grain Arc-3 cuts (150/300 mi) ride the centroids already served — no extra shape.

## Notes

Sub-shapes stay reconciled here: sector cuts want county-sorted; distinct-places wants
window dates; import-ratio wants pop_state × hq_state. One mart serves all three.
