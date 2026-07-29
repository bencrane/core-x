# Run record — CBO Measure Canonicalization

**Stream:** `cbo_measure_taxonomy` · **Dataset:** `s3://data-sink/active/cbo_measure_taxonomy/`
**Derived from:** `active/cbo_key_budget_economic_data/` (12.18M cells, 13 products, vintages 2000–2026)
**Method:** LLM classification (118 batches) → independent verify pass → deterministic dedup + land.
**Date:** 2026-07-29 UTC

## Result

- **4,782** distinct `row_label` strings classified → canonical measure + family crosswalk, one row per label.
- **13** `measure_family` values (full taxonomy enum populated).
- BTREE scalar indexes on `row_label`, `measure_family`.
- Ledger: `ops.federal_appropriations_ingest_runs`, stream `cbo_measure_taxonomy`, status `completed`.

## §5 validation gates (all passed)

| gate | threshold | actual |
|---|---|---|
| classified labels | > 3,000 | 4,782 |
| distinct families | ≥ 8 | 13 |
| `row_label` unique + non-null | 4,782 unique / 0 null | pass |
| JOIN coverage over data-row cells | > 95% | **99.998%** (2,003,916 / 2,003,955) |
| BTREE `row_label`, `measure_family` | required | built |
| separators (`___`, `____`) → `non_measure` | required | pass |

## Per-family counts

| measure_family | labels |
|---|---|
| outlay | 3,110 |
| revenue | 421 |
| non_measure | 229 |
| other | 222 |
| deficit_surplus | 196 |
| demographic | 102 |
| income | 101 |
| gdp_output | 94 |
| trust_fund | 90 |
| labor | 84 |
| debt | 60 |
| prices | 46 |
| interest_rates | 27 |

## Notes

- `outlay` dominates because the OMB budget-account identifiers (`NNN-NNNN-N-N-NNN`) that appear
  as row labels in the CBO baseline tables are spending accounts.
- The highest-occurrence bare labels (`Total`, `Subtotal`, `Other`, `Percentage change`,
  `Percentage of GDP`) land in `other` with `is_total` set where applicable — they span too many
  sheet contexts to pin to a single family, which is the honest crosswalk value. Units are NOT
  encoded here; they live in the sheet name on the fact table (`(GDP)` = % of GDP vs `$B`).
- Artifacts of record: `docs/reference/data/cbo_measure_classifications.json` (verified LLM output),
  `docs/reference/data/cbo_labels.json` (work-list + occurrences).
