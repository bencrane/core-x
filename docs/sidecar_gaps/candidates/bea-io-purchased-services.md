# bea-io-purchased-services

**Status:** `promoted` (reduced) — query_sidecar_20260724T044059Z (2026-07-24, ledger id 46, PR #1337). 4 small tables: bea_bls_klems (52,808) + bea_contingent_labor_intake (3,067) + bea_naics_concordance (499, Tier A) + bea_io_use_summary_annual (206,172, added by sweep for 'of what?'). All EXACT parity. KLEMS service-share (5415) 25.6% in 10ms. CORRECTION: dossier's bea_io_use_detail (~369k) was the wrong table (159k, 9yr stale) — parked. QCEW-scale members stay gated.

## Capability

Industry cost-structure decomposition warm — purchased-services / contingent-labor /
materials-energy-services shares per industry from the BEA IO Use family and the
industry-cost-structure batch, for the demo's remaining narrative slices (M/E/S, equipment
spend rate, IT, contingent labor).

## Evidence trail

- 2026-07-15 — [processed/SIDECAR_GAP_REPORT_2026-07-15-action-types-pricing-staffing.md](../processed/SIDECAR_GAP_REPORT_2026-07-15-action-types-pricing-staffing.md)
  Entry 5: BEA I-O purchased-services share — "correctly-absent, ingest-scale, demand
  unproven" at the time (no upstream data).
- 2026-07-23 — upstream RESOLVED: BEA IO Use ingests + industry-cost-structure batch landed
  on main (#1324/#1325/#1326; 14 new Lance datasets incl. KLEMS, fixed assets, QCEW 86.3M,
  TFP, IO use detail/summary, SUT concordance, contingent-labor intake).
- 2026-07-23 — [SIDECAR_GAP_REPORT_2026-07-23-demo-narrative-decomposition.md](../SIDECAR_GAP_REPORT_2026-07-23-demo-narrative-decomposition.md)
  report-level context note: demo's remaining slices will draw on these; "off-sidecar asks
  will appear in the next report."

## Proposed shape

- NOT the raw 86.3M-row QCEW — start with the small IO-use/derived-share tables actually
  cited (IO use detail ~369k rows, concordances, ECEC-class small tables).
- Projected artifact delta: **~0.1–0.5 GiB** for the small-table set; QCEW-scale members
  stay gated until a specific shape is demanded.

## Adjacency candidates

Whichever industry-code crosswalks (NAICS↔IO commodity) the first promoted table joins
through — take them in the same pass.

## Notes

Structural growth needs demand evidence: the 07-15 entry + the announced demo draw justify
the SMALL tables only. Let the next gap report name the exact shapes before promoting
anything QCEW-scale.
