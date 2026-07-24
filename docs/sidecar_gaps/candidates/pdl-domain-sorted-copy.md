# pdl-domain-sorted-copy

**Status:** `parked` — gated until felt; domain-anchored matching is a seconds-class scan

## Capability

Second copy of `pdl_normalized_companies` sorted by domain, for domain-anchored matching.

## Evidence trail

- 2026-07-10 — [processed/SIDECAR_GAP_REPORT_2026-07-10-funding-tab-pdl-match.md](../processed/SIDECAR_GAP_REPORT_2026-07-10-funding-tab-pdl-match.md)
  disposition sweep rationale: gated until felt.

## Proposed shape

Sort copy of the PDL normalized table. Delta: ~1–2 GiB.

## Adjacency candidates

n/a.

## Notes

No recurrence since 2026-07-10; `gtm_sam_entities.normalized_domain` also now serves
uei↔domain (2026-07-20 routing note), which absorbed part of the original need.
