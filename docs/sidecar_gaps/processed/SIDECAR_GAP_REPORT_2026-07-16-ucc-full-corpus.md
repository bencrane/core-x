# SIDECAR GAP REPORT — 2026-07-16 — ucc-full-corpus

- **Date:** 2026-07-16
- **Artifact stamp:** `query-sidecar/query_sidecar_20260716T034525Z.duckdb` (96 tables)
- **Session topic:** CA/CO UCC lender segmentation → full-corpus (non-SAM-constrained) demand

## Entry 1

1. **Intent** — "Of the non-bank UCC lenders, what does their full book look like — not
   just SAM-registered debtors? The SAM constraint is artificial for lender-side analysis."
   (Operator directive: build the sidecar surface that is NOT SAM-constrained.)
2. **Why not the sidecar** — `missing table` + `wrong grain`: `sam_ucc_filings` /
   `sam_ucc_lenders` are the SAM INTERSECTION by construction (crosswalk_ucc_sos ⋈
   crosswalk_sos_sam inner-joined at the source). Non-SAM debtors — the majority of every
   lender's book — are invisible; lender `filings`/`sam_firms` counts systematically
   understate book size and hide the federal-exposure ratio.
3. **What I ran instead** — nothing served the shape; answered qualitatively from
   `sam_ucc_lenders` (21,686 lenders, SAM-only counts) with an explicit caveat.
4. **Cost** — the analytical question (lender total book, SAM share, non-SAM debtor
   discovery) was unanswerable. Raw sources on Lance: `ca_ucc/{debtors,filings,
   secured_parties}`, `ucc_co_debtors`, `co_ucc_transactions`, `ucc_co_secured_parties`,
   `ucc_co_collateral`.
5. **Recurrence** — recurring: every lender-side GTM/EFC question hits this; the
   operator directive makes it standing demand.

## Disposition (build cycle 2026-07-16)

**Verdict:** PROMOTE (structural, operator-directed). Two new Lance datasets + two
manifest entries; one parity-gated rebuild.

### Build scope block

**Ships from demand:**
- `ucc_filings_all` — grain (ucc_state, filing_id, debtor_key); FULL CA/CO corpus, org
  AND individual debtors; `uei`/`sos_entity_key`/`in_sam` as NULLABLE enrichment via
  LEFT JOIN through the same canonical crosswalks. Built in
  `pipelines/resolution/sam_ucc_debtor_overlap.py::filings_all`.
- `ucc_lenders_all` — lender grain over the full corpus; same class brackets
  (FDIC/NCUA authorities + masks + in_efc); adds `total_firms` (all debtors) alongside
  `sam_firms` → federal-exposure share per lender. Built in `::lenders_all`.

**Adjacency riders (one line each):**
- `debtor_name`, `debtor_name_norm`, `debtor_city`, `debtor_state`, `debtor_zip` —
  "who/where is this non-SAM debtor" is the immediate next question; free from the
  debtor scan already performed.
- `is_org` — individual vs org debtor split; free CASE off source columns.
- `in_sam` — boolean form of the enrichment for cheap filtering/grouping.
- `total_firms` + retained `sam_firms` on lenders — the ratio is the point of the cycle.

**Parked (structural-gated, not shipped):**
- Debtor-grain rollup over the full corpus (a `ucc_debtors_all` uei-free analogue of
  sam_ucc_debtor_overlap) — derivable from `ucc_filings_all` by GROUP BY debtor_key at
  query time; promote only if that aggregation shows up as recurring cost.
- Lender × debtor edge table — same: expressible as a query over filings_all
  (unnest secured_parties); promote on demonstrated cost.
- Other states — no source ingested beyond CA/CO.

**Next-question simulation:** "top lenders by total book" ✓ (lenders_all);
"non-SAM debtors of lender X" ✓ (filings_all LIKE on secured_parties + in_sam=false);
"geo split of a lender's book" ✓ (debtor_city/state/zip); "org vs individual" ✓
(is_org); "trend of filings by year" ✓ (first_filing_date); "which of these debtors
could register in SAM" — needs firmographics beyond this corpus, correctly-on-Lance.

### Measured results

- `ucc_filings_all` (Lance): **7,711,737 rows** (CA 5,743,105 / CO 1,968,632; 82,445
  distinct uei enriched; 2,630,742 active financings; 0 orphans; all 5 pre-write gates
  PASS). vs sam_ucc_filings 376,451 → 20× corpus expansion.
- `ucc_lenders_all` (Lance): **135,153 lenders** (115,349 non_bank; 53,082 with active
  book; 156 in_efc; CA 73,762 / CO 72,299 with firms). vs sam_ucc_lenders 21,686 → 6.2×.
- Sidecar rebuild: artifact `query_sidecar_20260717T020529Z.duckdb`, 98 tables
  (96 + the 2 promoted), 49.05 GiB, parity=OK on all marts, published + hot-swapped.
