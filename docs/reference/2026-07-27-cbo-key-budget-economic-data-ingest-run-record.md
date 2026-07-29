# Run record — CBO Key Budget & Economic Data ingest (keyless landing-drop route)

**Cycle:** 2026-07-27 · **Module:** `pipelines/reference/cbo_landing_ingest.py`
**Ledger:** `ops.federal_appropriations_ingest_runs` (stream `cbo_key_budget_economic_data`)
**Dataset:** `s3://data-sink/active/cbo_key_budget_economic_data/` · **Raw archive:** `s3://data-sink/active/cbo_raw/`

Lands CBO's Key Budget & Economic Data corpus — the legislation/scoring context layer the plane
otherwise lacks — for reports, data-viz, and market-trend insight publishing.

## Keyless by design (the route decision)

`cbo.gov` **hard-blocks automated clients** (Akamai edge 403 from the first request; verified
2026-07-27 — bare UA *and* full browser UA, three URLs incl. a direct `.xlsx`). So there is **no
automated fetch and no API key** in this ingest:

- **USAspending precedent:** we hit USAspending keyless by design; same philosophy here.
- **Route used:** the operator hand-downloaded the CBO workbooks in a browser (a real browser is
  not the blocked automated client) and dropped them into `landing/cbo/<product>/`. Those bytes
  were copied **as-is** into `active/cbo_raw/<product>/` (durable archive), and the melt reads
  `active/cbo_raw/` — zero cbo.gov, zero GovInfo, zero `api.data.gov` key.
- **GovInfo note:** `GOVINFO_API_KEY` is now in Doppler as a fallback for the cost-estimate
  route, but it is **not needed** for this projection corpus. For future automated *refreshes*
  the keyless path is CBO's public GitHub, not the keyed GovInfo API.
- Verified: `SAM_API_KEY` is an `api.data.gov` key but GovInfo **rejects it** (`401`,
  its own key registry) — so `api.data.gov` keys are NOT universal across GovInfo.

## Corpus & result

Operator dropped **347 files / 158.6 MB** across 13 products (`.xlsx` 247, legacy `.xls` 49,
`.zip` 51 — the zips carry economic-data **CSVs**, not workbooks). Melted **346/347 files →
12,184,395 rows**.

| Product | rows |
|---|---:|
| demographic-projections | 7,848,062 |
| spending-projections | 2,248,889 |
| historical-data-and-economic-projections | 1,009,898 |
| economic-projections | 389,187 |
| 10-year-budget-projections | 182,192 |
| long-term-budget-projections | 129,189 |
| historical-budget-data | 102,290 |
| potential-gdp-and-underlying-inputs | 87,001 |
| revenue-projections-by-category | 73,114 |
| estimates-of-automatic-stabilizers | 40,667 |
| long-term-economic-projections | 29,553 |
| tax-parameters | 26,701 |
| 10-year-trust-fund-projections | 17,652 |

## Melt design (lossless cell-grain + navigation tags)

CBO workbooks are a `Contents` sheet + data tables (banner rows, then a row-label × year-column
grid; **units are encoded in the sheet name** — `(GDP)` = % of GDP vs `$B` — so units are NOT
normalized). One landed row per non-empty cell, tagged: `product, vintage_year, vintage_month,
source_file, sheet, row_num, col_num, row_label, col_year, is_projection, value_str, value_num`.
Filtering `row_label IS NOT NULL AND col_year IS NOT NULL` yields a clean
`(product, vintage, sheet, measure, year, value)` time series for viz; raw cells remain for
anything the tagging misses. Deterministic openpyxl/xlrd/csv parse — **no LLM**.

Verified tag ranges: `vintage_year` 2000–2026, `col_year` 1901–2099, 0 null vintages.

## Known limitations (recorded)

- **1 file skipped** — `51118-2009-01-budgetprojections.xls`: xlrd cannot parse its NAME-formula
  workbook globals (`Token 0x2d (AreaN)`). Its raw bytes are preserved in `active/cbo_raw/`;
  re-drop a converted `.xlsx` to recover that single 2009 vintage.
- **BTREE only on `product`** — the `vintage_year`/`col_year` scalar-index builds hit a Lance
  external-sort memory-pool cap at 12.2M rows (`Resources exhausted`, ~22 MB pool; the string
  `product` index builds fine; compaction 22→13 fragments did not help). For a cell-grain
  ANALYTICAL table queried by DuckDB scan, `product` is the load-bearing partition key and
  `vintage_year`/`col_year` filter fine by scan (<1s at this scale). Not load-bearing here.

## Provenance / catalog

Registered in `ops.data_source_catalog` (`cbo_key_budget_economic_data`, `ON CONFLICT DO NOTHING`).
Ledger row written (`status='completed'`, `disposition='ok'`; `failed=1` file noted in `notes`).
