# labor-demand-poc-marts

**Status:** `parked` — no gap entry has exercised either table; people-layer overlap

## Capability

Solicitation-linked labor-category demand (`govcon_labor_demand`, 20.6k rows) and uei-keyed
staffing POCs (`sam_labor_poc_people`, 29.5k rows) warm.

## Evidence trail

- 2026-07-11 — [processed/SIDECAR_GAP_REPORT_2026-07-11-labor-occupation-grain.md](../processed/SIDECAR_GAP_REPORT_2026-07-11-labor-occupation-grain.md)
  disposition: parked structural-gated, no demand yet.
- 2026-07-14 — [processed/SIDECAR_GAP_REPORT_2026-07-14-labor-pricing-entry-hop.md](../processed/SIDECAR_GAP_REPORT_2026-07-14-labor-pricing-entry-hop.md)
  disposition: explicit re-park, "unchanged from gap-pass-6 parking."

## Proposed shape

Two Tier-D generic copies, ~50k rows total. Delta: negligible (<0.05 GiB).

## Adjacency candidates

None distinct — `sam_labor_poc_people` overlaps `gtm_sam_people`/`sam_pocs`; a promotion
must state the distinct demand shape first.

## Notes

Two parks with zero demand between them. Promote only when a solicitation-demand question
recurs in a report.
