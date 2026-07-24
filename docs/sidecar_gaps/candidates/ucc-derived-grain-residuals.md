# ucc-derived-grain-residuals

**Status:** `parked` — derivable at query time; promote on demonstrated cost

## Capability

The UCC derived-grain family left parked after `ucc_lender_filings` shipped:
(a) `ucc_debtors_all` debtor-grain rollup; (b) debtor-key-sorted copy of the lender bridge
(co-lender / competitor overlap); (c) `secured_parties` + `collateral_text` blobs on the
exploded grain; (d) filing-key sort copy of `ucc_filings_all`.

## Evidence trail

- 2026-07-16 — [processed/SIDECAR_GAP_REPORT_2026-07-16-ucc-full-corpus.md](../processed/SIDECAR_GAP_REPORT_2026-07-16-ucc-full-corpus.md)
  disposition: debtor rollup + lender×debtor edge parked ("derivable at query time").
  The lender half promoted next day as `ucc_lender_filings`.
- 2026-07-17/18 — [processed/SIDECAR_GAP_REPORT_2026-07-17-lender-book-bridge.md](../processed/SIDECAR_GAP_REPORT_2026-07-17-lender-book-bridge.md)
  build-scope + disposition: blobs (~1 GB, "promote a collateral-text path only on
  demonstrated recurrence of 'against what' at lender grain"); filing-key copy ("not worth
  a third copy of the corpus"); debtor-key bridge copy ("only if competitor-overlap becomes
  a page section").

## Proposed shape

Per-item as stated; blob adds ~1 GiB, sort copies ~0.5–1 GiB each.

## Adjacency candidates

If ANY of these promotes, the whole family should be re-swept in one pass — same corpus,
same joins.

## Notes

Promotion triggers are explicit page-section/recurrence conditions; none has fired.
