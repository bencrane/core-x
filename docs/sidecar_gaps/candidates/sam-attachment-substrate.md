# sam-attachment-substrate

**Status:** `parked` — GB-scale, freshness-coupled to in-flight pipeline stages, one session of demand

## Capability

Award→solicitation→attachment/PDF/extracted-text coverage reads warm (opps universe,
`sam_opps_attachment_manifest*`, `sam_attachment_files`, `sam_attachment_extraction`).

## Evidence trail

- 2026-07-12/13 — [processed/SIDECAR_GAP_REPORT_2026-07-12-entity-inflection-liability.md](../processed/SIDECAR_GAP_REPORT_2026-07-12-entity-inflection-liability.md)
  Gap A, ranked #1 in-report (2.84M-row universe + 9 manifest shards per read);
  disposition: parked structural-gated, "re-evaluate on recurrence." Join keys already
  served via `award_descriptions`.

## Proposed shape

Manifest/ledger tables as Tier-D copies (GB-scale); extraction-state freshness makes a
snapshot partially wrong by construction. Delta: multi-GiB.

## Adjacency candidates

n/a until the freshness coupling is resolved (extraction stages complete).

## Notes

No recurrence since 2026-07-13. Leave parked.
