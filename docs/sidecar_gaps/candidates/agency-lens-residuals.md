# agency-lens-residuals

**Status:** `parked` — measured-acceptable / session-variant

## Capability

(a) Third `txn_events_combo` copy sorted agency-first; (b) recipient-grain position rollup
(position state per PSC context).

## Evidence trail

- 2026-07-09 — [processed/SIDECAR_GAP_REPORT_2026-07-09-agency-lens-v8-v12.md](../processed/SIDECAR_GAP_REPORT_2026-07-09-agency-lens-v8-v12.md)
  disposition residuals: agency-anchored seed scan 1.6 s warm post-build = acceptable;
  position rollup gated because "rings vary per session."

## Proposed shape

A third combo-fact sort copy is ~5–8 GiB class (the combo fact is the biggest family in
the artifact) — high bar. The rollup is small but definitionally unstable.

## Adjacency candidates

n/a.

## Notes

No recurrence since 2026-07-09; the 1.6 s measurement stands until a report says otherwise.
