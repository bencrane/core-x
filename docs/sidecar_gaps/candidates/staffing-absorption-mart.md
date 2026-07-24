# staffing-absorption-mart

**Status:** `parked` — methodology freeze pending (wage weighting, window choice, FTE formula)

## Capability

Materialized staffing-absorption mart (implied FTE vs headcount vs reported farm-out) at
uei×naics×psc — currently served by view `v_staffing_absorption` at 9.2 s.

## Evidence trail

- 2026-07-15/16 — [processed/SIDECAR_GAP_REPORT_2026-07-15-action-types-pricing-staffing.md](../processed/SIDECAR_GAP_REPORT_2026-07-15-action-types-pricing-staffing.md)
  Entry 4 disposition: promoted as VIEW; mart gated — "baking a still-moving formula into a
  materialized mart is the signature-precedent anti-pattern." The 9.2 s view time is itself
  the demand evidence for the mart once methodology freezes.

## Proposed shape

Materialize the view post-freeze; grain uei×naics×psc, single-digit-M rows. Delta:
~0.1–0.5 GiB.

## Adjacency candidates

Whatever wage-reference vintage columns the frozen formula pins.

## Notes

Promotion trigger is a methodology decision, not new demand.
