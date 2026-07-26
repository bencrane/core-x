# award-key-point-reads

**Status:** `promoted` — query_sidecar_20260722T032457Z (2026-07-22, 113 tables, PRs #1304/#1305/#1308)

## Capability

Ms-class award-key point-reads for the full award profile (drawer/tear-sheet): anchor state
row, FY ledger, recent actions, PoP centroid — award-key-sorted companions replacing
uei-pruned or full-scan probes on tables sorted by other columns.

## Evidence trail

- 2026-07-21 — [processed/2026-07-21-award-key-probes.md](../processed/2026-07-21-award-key-probes.md)
  original gap + addendum: /award end-to-end ~13 s post-#1299; residual award-KEY probes 4.8 s
  (state row) and 0.9 s (centroid) on tables sorted for other pruning axes.
- 2026-07-21 — [SIDECAR_GAP_REPORT_2026-07-21-capital-video-surfaces.md](../SIDECAR_GAP_REPORT_2026-07-21-capital-video-surfaces.md)
  Entry 2 (ranked #2): fresh, demo-critical demand — green-dot award clicks on camera; 26.9 s
  cold for a Lockheed-class award, one demo-path 500; converted the parked gap from
  "promote on demand evidence" to demanded. Report's 2026-07-22 disposition: PROMOTED + shipped.
- 2026-07-22 disposition ([processed/2026-07-21-award-key-probes.md](../processed/2026-07-21-award-key-probes.md)):
  shipped `prime_award_state_by_key`, `txn_events_combo_by_award`, `txn_rows_by_award`,
  `award_pop_centroids_by_key` with the `award_key_pfx = substr(key,10,12)` leading sort
  (string zone-maps truncate at 8 bytes; full-key sort alone pruned zero). Measured: anchor
  8,619 → 32 ms; FY ledger 11,516 → 29 ms; /award end-to-end 13–27 s → 0.81 s. Consumed by
  catalyst `/market-slice/award` (#1308); BFF timeout reverted 90 s → 30 s (gc-hq-new #101).

## Proposed shape

Shipped as-built: four award-key-pfx-sorted companions (see disposition table). Probe contract:
`award_key_pfx = substr('<key>',10,12) AND <fullkey> = '<key>'`.

## Adjacency candidates

`award_subout_rollup` was adversarially reviewed and correctly NOT companioned (197k-row
aggregate, already cheap). No open riders.

## Notes

Dossier created 2026-07-26 (reconciliation): the capability shipped pre-dossier-layer and was
absent from candidates/ — PLATE's "Just shipped" table covers only the 2026-07-24 cycle, and
award-grain-geo-spine's shipped notes (award_geo_state) do not include these companions.
Closed demand; nothing open.
