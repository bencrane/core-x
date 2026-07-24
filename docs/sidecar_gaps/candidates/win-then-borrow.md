# win-then-borrow

**Status:** `parked` — interval-join doctrine violation + freshness-inherent core + UCC coverage bound

## Capability

Entity-grain win-then-borrow propensity: firms with fresh federal money (≤90d), a history of
borrowing within 90d of winning, and no lien since last award — "the most surgical
proprietary origination signal."

## Evidence trail

- 2026-07-20 — [processed/SIDECAR_GAP_REPORT_2026-07-20-capitalization-triggers.md](../processed/SIDECAR_GAP_REPORT_2026-07-20-capitalization-triggers.md)
  Entry 2 + footer: ranked #1, top demand, 5,571 ms workaround. 2026-07-22 disposition
  folded Entries 1+3 into `gtm_entity_pricing_flow`; Entry 2 explicitly NOT folded
  (ship-partial).
- 2026-07-21/22 — [processed/PRICING_FLOW_MART_HANDOFF.md](../processed/PRICING_FLOW_MART_HANDOFF.md)
  §7 parked: "structural, deferred — do NOT cram into an unrelated build."

## Proposed shape

Pure-equality `gtm_win_then_borrow` propensity leg (historical paired-count,
last-award/last-filing dates); the open-window overlay stays a query-time uei-pruned
`gtm_txn_events_slim` read (baking it freezes staleness). Award↔UCC pairing is a BETWEEN
interval join — needs a CASE-derived equality key design before it can enter the builder.
Delta: small (entity grain, sub-1M rows).

## Adjacency candidates

Lender identity on the paired leg (rides `ucc_lender_filings`); CA/CO coverage flag column.

## Notes

Also gated by [ucc-state-corpus-expansion.md](ucc-state-corpus-expansion.md) — signal is
CA/CO-only today. Two dated demand points; promote when the equality-key design is written.
