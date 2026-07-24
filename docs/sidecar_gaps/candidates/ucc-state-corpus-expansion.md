# ucc-state-corpus-expansion

**Status:** `parked` — ingest-scale work, not a sidecar promotion

## Capability

State UCC corpora beyond CA/CO, completing lender-book and win-then-borrow coverage.

## Evidence trail

- 2026-07-16 — [processed/SIDECAR_GAP_REPORT_2026-07-16-ucc-full-corpus.md](../processed/SIDECAR_GAP_REPORT_2026-07-16-ucc-full-corpus.md)
  disposition: "Other states — no source ingested beyond CA/CO."
- 2026-07-17 — [processed/SIDECAR_GAP_REPORT_2026-07-17-market-composition.md](../processed/SIDECAR_GAP_REPORT_2026-07-17-market-composition.md)
  disposition: "ingestion work (new state corpora), not a sidecar promotion."
- 2026-07-20 — [processed/SIDECAR_GAP_REPORT_2026-07-20-capitalization-triggers.md](../processed/SIDECAR_GAP_REPORT_2026-07-20-capitalization-triggers.md)
  Entry 2 note: win-then-borrow "is also CA/CO-only — a UCC-ingest coverage gap."

## Proposed shape

Data-factory ingest directive per new state (upstream of the sidecar entirely); the sidecar
copies then grow `ucc_filings_all`/`ucc_lender_filings` on the next rebuild automatically.

## Adjacency candidates

n/a — upstream ingest decision.

## Notes

Three dated touches. This gates two other candidates; belongs on the operator's ingest
roadmap, not in a rebuild scope block.
