# Labor-Share-of-Revenue Stack — Census SUSB + BEA Value-Added + BLS ECEC

**Status:** live · **Ingested:** 2026-07-11 UTC · **Module:** [`pipelines/reference/labor_share_ingest.py`](../../pipelines/reference/labor_share_ingest.py)
**Directive:** `~/Desktop/hq/directives/2026-07-11-labor-share-of-revenue-stack-ingest.md`

## Why this exists — the composition identity it feeds

For a prime award's NAICS+PSC we already know *which* labor categories fulfill it and their
within-labor mix (`naics_psc_labor_profile_categories.pct_of_industry` × `a_median`), but not
what fraction of the award dollar is labor at all. This stack supplies that scalar and its
calibration, closing:

```
expected labor $ by category = award_$ × labor_share × category_mix
                                          └── this stack ──┘   └─ already live ─┘
```

- **labor_share** (payroll ÷ receipts per NAICS) ← Census SUSB
- **cross-check + coarse-grain fallback** (compensation ÷ gross output per industry) ← BEA
- **burden multiplier** (total comp ÷ wages, converts payroll share → fully-loaded labor-cost share) ← BLS ECEC

The composed `naics_labor_share` dim (share × mix join, NAICS↔BEA concordance, pass-through
discounting) is a **follow-on** directive — out of scope here.

## Datasets (Gen-3 Lance SoR, `s3://data-sink/active/`, Pattern A direct hydration)

| Dataset | Grain | Rows | Derived column |
|---|---|---:|---|
| `census_susb_naics_payroll_receipts` | naics × size_class | 32,200 | `payroll_share = annual_payroll_k / receipts_k` |
| `bea_industry_value_added` | table × industry_line × component × year | 14,272 | `comp_share_of_output = CoE / gross_output` (285 derived rows) |
| `bls_ecec_costs` | series_id × year × period | 1,200 | — |
| `bls_ecec_burden` | ownership × industry_group | 24 | `burden_multiplier = total_comp / wages_salaries` |

All Lance v2.1, `mode="overwrite"` (idempotent). BTREE on join keys (`naics`; `bea_industry_line`
+ `industry_name`; `series_id`; `industry_group`), BITMAP on low-cardinality categoricals.

## Sources (all keyless, verified live 2026-07-11)

1. **Census SUSB 2022** — static xlsx `www2.census.gov/.../us_6digitnaics_rcptsize_2022.xlsx`
   (browser UA). Sheet `US 6-digit NAICS`, header row 3, data row 4+. Payroll & Receipts in
   $1,000s; receipts exist only for Economic-Census years, so `ref_year=2022` is hardcoded.
   **The Census API is now key-gated (302→missing_key.html) — the static path is the only route.**
2. **BEA GDP-by-Industry** — static zip `apps.bea.gov/industry/Release/ZIP/GdpByInd.zip`.
   Two annual dollar tables selected after sheet inspection:
   - `ValueAdded.xlsx::TVA113-A` "Components of Value Added by Industry" [$M, 1997–2024] —
     per-industry header = value_added total; 3 sub-rows = compensation_of_employees /
     taxes_on_production_less_subsidies / gross_operating_surplus.
   - `GrossOutput.xlsx::TGO105-A` "Gross Output by Industry" [$M, 1997–2025] — the denominator.
   The `io-annual/IOUse_*Summary.xlsx` path is dead (returns text/html) — avoided.
3. **BLS ECEC** — Public Data API v2 POST `api.bls.gov/publicAPI/v2/timeseries/data/` (keyless,
   25 series/query). `download.bls.gov` flat files 403 even with a browser UA — API is the only
   route. Series ID format (authoritative, `bls.gov/ecec/factsheets/ecec-series-id-guide.htm`):
   `CM·U·<owner:1>·<estimate:2>·<industry:4>·<occupation:3>·<subcell:3>·<datatype:1>`.

## Key formulas & verified constants

- **SUSB payroll_share** = `annual_payroll_k / receipts_k` (NULL-safe; receipts 0/blank → NULL).
  Economy total (`NAICS='--'`, `01: Total`): payroll_k=8,965,035,263, receipts_k=50,848,996,830
  → **0.176307**.
- **BEA comp_share_of_output** = `CoE(industry, year) / GrossOutput(industry, year)`, joined by
  trimmed industry_name (single economy-wide alias: value-added "Gross domestic product" ↔ gross
  output "All industries"), latest 3 common years (2022–2024, CoE is published through 2024).
  Economy 2024: 15,049,121 / 50,736,556 = **0.296613**.
- **ECEC burden_multiplier** = `total_comp / wages_salaries` per (ownership, industry_group) at
  the latest common quarter. Economy-private (all industries): **1.4294**. Anchor
  `CMU2010000000000D` (private, total comp, all industries) 2025 Q04 = **46.15** (exact).

## Validation gate results (directive §8 — all pass)

| Check | Bound | Value |
|---|---|---|
| SUSB row count | 32,000–32,500 | 32,200 (= 32,203 − 2 banner − 1 header; **zero dropped**) |
| SUSB economy payroll_k / receipts_k | exact | 8,965,035,263 / 50,848,996,830 |
| SUSB economy payroll_share | 0.1763 ±0.0005 | 0.176307 |
| SUSB sector-54 payroll_share | [0.30, 0.50] | 0.392757 |
| SUSB distinct 6-digit NAICS | ≥ 900 | 970 |
| BEA CoE / GOS / Taxes present | all | all present |
| BEA economy comp_share_of_output (latest) | [0.25, 0.40] | 0.296613 |
| BEA per-industry share | (0, 1), never > 1 | min 0.009004 / max 0.644585 |
| BEA industry lines / year | ≥ 60 | 97 |
| BEA latest year | ≥ 2023 | 2025 |
| ECEC series succeeded | all REQUEST_SUCCEEDED | 48/48 (0 dropped) |
| ECEC anchor 2025 Q04 | 46.15 ±0.5 | 46.15 |
| ECEC every burden_multiplier | [1.15, 1.65] | 1.2352 – 1.581 |
| ECEC economy-private multiplier | [1.35, 1.50] | 1.4294 |
| ops.labor_share_runs | 4 rows `status='success'` | ✓ |

## Design notes / deliberate deviations

- **BEA `bea_industry_code` lands NULL.** The GDPbyIndustry *summary* workbooks expose only Line
  + Name (no alphanumeric industry code column), so `bea_industry_line` is the stable per-table
  join key and is BTREE-indexed alongside `industry_name`. The NAICS↔BEA concordance is the
  downstream composition step, deliberately out of scope.
- **BEA raw rows keep all years (1997+)**, not just recent — raw-stays-lossless. This puts the
  dataset at ~14.3K rows vs the directive's ~4–8K estimate; still trivially small. Derived
  `comp_share` rows are limited to the latest 3 common years per the directive.
- **ECEC industry set** = the 24 published major industry groups (private ownership), each for
  total compensation (01) and wages & salaries (02) → 48 candidate series, all landed. Detailed
  4-digit-NAICS subcells (e.g. Aircraft manufacturing, Nursing care) are excluded by design.

## Ledger

`ops.labor_share_runs` (HQX, `HQX_DB_URL_POOLED`) — one terminal-state row per dataset per run;
an audit-write failure warns, never masks a good load.

## Re-run

```bash
doppler run -p core-x -c prd -- uv run --with pylance --with pyarrow --with duckdb \
  --with requests --with boto3 --with openpyxl --with 'psycopg[binary]' \
  python -m pipelines.reference.labor_share_ingest --stream all --smoke   # throwaway URIs first
... python -m pipelines.reference.labor_share_ingest --stream all                  # full overwrite
```
