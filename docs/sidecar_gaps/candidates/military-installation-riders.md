# military-installation-riders

**Status:** `parked` — flags not landed upstream; proximity already 7.8 ms at query time

## Capability

(a) `isFirrmaSite`/`isCui` compliance flags on the installations table; (b) zip3/centroid →
nearest-installation proximity crosswalk.

## Evidence trail

- 2026-07-17 — [processed/SIDECAR_GAP_REPORT_2026-07-17-military-installations.md](../processed/SIDECAR_GAP_REPORT_2026-07-17-military-installations.md)
  disposition: parked structural-gated; proximity measured 7.8 ms warm — "no derived table
  justified."

## Proposed shape

Flags: upstream ingest columns first, then ride free. Crosswalk: small derived table,
negligible delta.

## Adjacency candidates

n/a.

## Notes

Flags are an ingest gap, not a rebuild gap.
