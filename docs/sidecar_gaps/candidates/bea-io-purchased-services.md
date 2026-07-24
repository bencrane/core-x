# bea-io-purchased-services

**Status:** `open` (weak — one dated demand entry + imminent demo draw; upstream now landed)

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
