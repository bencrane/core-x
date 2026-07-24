# entity-month-velocity

**Status:** `parked` — annual + fixed-window grains covered the asked questions; scope-drift risk

## Capability

uei × month obligation rollup beyond the construction lanes: all-NAICS monthly velocity,
per-firm pricing/labor sparkline series, capital-card and sub-side lane variants.

## Evidence trail

- 2026-07-19 — [processed/SIDECAR_GAP_REPORT_2026-07-19-growth-lane-months.md](../processed/SIDECAR_GAP_REPORT_2026-07-19-growth-lane-months.md)
  disposition: capital-card lane parked (scope-drift risk vs pg/overlay card definitions,
  no demand); sub-side lane parked (different substrate).
- 2026-07-20 — [processed/SIDECAR_GAP_REPORT_2026-07-20-capitalization-triggers.md](../processed/SIDECAR_GAP_REPORT_2026-07-20-capitalization-triggers.md)
  "served acceptably via proxy — latent grain gap only" (all-NAICS monthly recent-vs-prior
  velocity absent; only `gtm_construction_lane_months` exists).
- 2026-07-21/22 — [processed/PRICING_FLOW_MART_HANDOFF.md](../processed/PRICING_FLOW_MART_HANDOFF.md)
  §7: "promote a `uei × month` rollup only if per-firm sparklines become a page section."

## Proposed shape

uei × month rollup(s); construction analogue was 535k rows — all-NAICS version
single-digit-M rows. Delta: ~0.1–0.3 GiB.

## Adjacency candidates

Action-type split columns (novation/termination counts per month) if the month fact is
built — overlaps [novation-mod-reason.md](novation-mod-reason.md)'s events lane.

## Notes

Three adjacent parks = a real latent grain, but the stated promotion trigger (sparklines as
a page section) has not fired.
