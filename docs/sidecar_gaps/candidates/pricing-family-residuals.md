# pricing-family-residuals

**Status:** `parked` — no demand since parking

## Capability

The pricing/financing follow-on set: financing trend-over-time (month fact carries no
financing code), small-determined × financing cross, subcontracting-plan split
(latest_plan domain unprobed), financing code 'F' decode, and lifetime-window
pricing-class dollar splits on `gtm_entity_pricing_mix`.

## Evidence trail

- 2026-07-17 — [processed/SIDECAR_GAP_REPORT_2026-07-17-pricing-financing-combos.md](../processed/SIDECAR_GAP_REPORT_2026-07-17-pricing-financing-combos.md)
  build-scope: four parked structural-gated follow-ons.
- 2026-07-16 — [processed/SIDECAR_GAP_REPORT_2026-07-16-billing-latency.md](../processed/SIDECAR_GAP_REPORT_2026-07-16-billing-latency.md)
  "considered and NOT taken": lifetime pricing-class splits, structural-gated if demanded.

## Proposed shape

Mostly column adds on existing month/entity facts — rides a committed rebuild when any
pricing question recurs. Delta: ~0.1–0.5 GiB aggregate.

## Adjacency candidates

If the month fact grows a financing code, take the small-determined cross in the same pass.

## Notes

Cheap riders awaiting any recurrence in the pricing lane.
