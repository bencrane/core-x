# txn-description-per-action

**Status:** `parked` — ≈2× artifact growth for the txn corpus, no workload yet

## Capability

Per-ACTION `transaction_description` text warm (award-grain base descriptions already
served via `award_descriptions`).

## Evidence trail

- 2026-07-09 — [processed/SIDECAR_GAP_REPORT_2026-07-09-oncall-market-brief.md](../processed/SIDECAR_GAP_REPORT_2026-07-09-oncall-market-brief.md)
  disposition 3a: stays gated (≈2× growth, no workload yet).

## Proposed shape

Description column on the txn grain — multi-GiB (heaviest single parked item). Delta:
~5–10 GiB class.

## Adjacency candidates

n/a.

## Notes

The one parked candidate whose promotion would materially move the disk-headroom math —
re-check the ~183 GiB wedge before ever promoting.
