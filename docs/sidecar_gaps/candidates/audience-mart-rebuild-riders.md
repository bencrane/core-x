# audience-mart-rebuild-riders

**Status:** `parked` — blocked on an upstream audience-mart rebuild, not a sidecar rebuild

## Capability

`total_amt_36mo` (combined sub+prime 36-month window; needs `sub_amt_36mo` upstream) and
`cage_code` on `gtm_audience_entities`; plus the considered-and-parked named-signal event
table (terminations/novations as first-class signals).

## Evidence trail

- 2026-07-15 — [processed/SIDECAR_GAP_REPORT_2026-07-15-audience-spec-parity.md](../processed/SIDECAR_GAP_REPORT_2026-07-15-audience-spec-parity.md)
  disposition: 36mo window PARKED (absent from source mart); cage_code tracked with it;
  named-signal table parked (month rollup serves via action_type codes).

## Proposed shape

Upstream `gtm_audience_entities` Lance rebuild adds the columns; sidecar copy then rides
any rebuild free. Delta: negligible.

## Adjacency candidates

The named-signal event lane overlaps [novation-mod-reason.md](novation-mod-reason.md) — if
that promotes, revisit whether the audience mart wants a signal column.

## Notes

Sequence: upstream mart rebuild first; nothing for a sidecar scope block until then.
