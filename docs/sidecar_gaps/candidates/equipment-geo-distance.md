# equipment-geo-distance

**Status:** `parked` — stays a query-time haversine; no demand since parking

## Capability

"Within N miles of a yard/shop" geo-distance join as a materialized surface.

## Evidence trail

- 2026-07-11 — [processed/SIDECAR_GAP_REPORT_2026-07-11-equipment-needs-combo.md](../processed/SIDECAR_GAP_REPORT_2026-07-11-equipment-needs-combo.md)
  disposition: parked, no demand this session — query-time haversine over
  `usaspending_award_pop_centroids` ⋈ shop geo suffices.

## Proposed shape

None warranted; would be per-anchor-set, which doesn't materialize well.

## Adjacency candidates

The sector-grid county substrate ([award-grain-geo-spine.md](award-grain-geo-spine.md))
partially subsumes this — county membership replaces radius for the sector program.

## Notes

Re-check after the geo spine ships; likely absorbable.
