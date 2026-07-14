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

The composed `naics_labor_share` dim is now **live** (2026-07-14) — see the section below.

## Datasets (Gen-3 Lance SoR, `s3://data-sink/active/`, Pattern A direct hydration)

| Dataset | Grain | Rows | Derived column |
|---|---|---:|---|
| `census_susb_naics_payroll_receipts` | naics × size_class | 32,200 | `payroll_share = annual_payroll_k / receipts_k` |
| `bea_industry_value_added` | table × industry_line × component × year | 14,272 | `comp_share_of_output = CoE / gross_output` (285 derived rows) |
| `bls_ecec_costs` | series_id × year × period | 627,050 | — (full universe; see ECEC section below) |
| `bls_ecec_burden` | ownership × industry_group × occupation_group × subcell | 321 | `burden_multiplier = total_comp / wages_salaries` |
| `naics_labor_share` | 1/6-digit naics | 1,133 | `loaded_labor_share = payroll_share × burden_multiplier` (the composed dim; see below) |

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
3. **BLS ECEC — full universe (2026-07-11 rebuild)** — the CM time-series flat-file snapshot at
   `s3://data-sink/landing/bls/time-series/cm/` (operator-landed, byte-exact vs `download.bls.gov`,
   which TLS-blocks programmatic clients). `cm.series.txt` (7,998 series, 15 cols) joined to
   `cm.data.1.AllData.txt` (627,050 observations, full history from 2004); all 8 dim codes decoded
   via the `cm.*` mapping files. Module
   [`pipelines/reference/ecec_full_universe.py`](../../pipelines/reference/ecec_full_universe.py)
   — supersedes the original 48-series/2020+ API slice from `labor_share_ingest --stream ecec`.
   Series ID format (authoritative, `bls.gov/ecec/factsheets/ecec-series-id-guide.htm`):
   `CM·U·<owner:1>·<estimate:2>·<industry:6>·<occupation:6>·<subcell:2>·<datatype:1>` (area 5-digit
   in the catalog columns). Owners: civilian 824 / private 6,642 / state-local 532 series;
   28 estimate components (total comp, wages, health, retirement, leave, …).

## Key formulas & verified constants

- **SUSB payroll_share** = `annual_payroll_k / receipts_k` (NULL-safe; receipts 0/blank → NULL).
  Economy total (`NAICS='--'`, `01: Total`): payroll_k=8,965,035,263, receipts_k=50,848,996,830
  → **0.176307**.
- **BEA comp_share_of_output** = `CoE(industry, year) / GrossOutput(industry, year)`, joined by
  trimmed industry_name (single economy-wide alias: value-added "Gross domestic product" ↔ gross
  output "All industries"), latest 3 common years (2022–2024, CoE is published through 2024).
  Economy 2024: 15,049,121 / 50,736,556 = **0.296613**.
- **ECEC burden_multiplier** = `total_comp / wages_salaries` (estimate 01/02, datatype D, area
  99999 national, NSA) per (ownership × industry_group × occupation_group × subcell) at the
  latest common quarter. Economy-private (all industries × all occ × all workers): **1.429448**
  @ 2026 Q01 (matches prior slice's 1.4294). Anchor `CMU2010000000000D` 2025 Q04 = **46.15**,
  2026 Q01 = **46.60** (exact).

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
| ops.labor_share_runs | rows `status='success'` | ✓ |

### ECEC full-universe rebuild gates (2026-07-11 directive — all pass, hard-fail in module)

| Check | Bound | Value |
|---|---|---|
| cm.series parse | exactly 7,998 series, 15 cols, 0 dropped | 7,998 / 15 / 0 |
| AllData join | every obs series_id ∈ catalog; joined == data rows | 627,050 == 627,050, 0 orphans |
| Dim resolution | 0 unresolved codes across 8 mapping files | 0 |
| Anchor `CMU2010000000000D` | 2025 Q04 == 46.15, 2026 Q01 == 46.60 exact | 46.15 / 46.60 |
| Economy-private multiplier | [1.35, 1.50] and ±0.01 of 1.4294 @ 2026 Q01 | 1.429448 @ 2026 Q01 |
| All burden rows | [1.05, 1.90] | 1.1668 – 1.7709 (321 rows) |
| Costs row count | ≥ 400,000 | 627,050 |
| Ledger | success rows both datasets | ✓ |

Non-numeric observation values: 5,257 (footnote-suppressed) → `value` NULL, verbatim string kept
in additive `value_raw`.

## Design notes / deliberate deviations

- **BEA `bea_industry_code` lands NULL in `bea_industry_value_added`.** The GDPbyIndustry
  *summary* workbooks expose only Line + Name (no alphanumeric industry code column), so
  `bea_industry_line` is the stable per-table join key, BTREE-indexed alongside `industry_name`.
  The BEA↔NAICS bridge is landed as a **separate concordance asset** (see below) rather than
  back-filled in place — no spine mutation, bridge pattern.
- **BEA raw rows keep all years (1997+)**, not just recent — raw-stays-lossless. This puts the
  dataset at ~14.3K rows vs the directive's ~4–8K estimate; still trivially small. Derived
  `comp_share` rows are limited to the latest 3 common years per the directive.
- **ECEC = full published universe** (2026-07-11 rebuild): all 7,998 series, all owners, all
  estimate components, detailed industries/occupations, size/union/region subcells, full history.
  Prior-slice columns kept name/type-compatible; codes AND decoded texts both land (raw lossless).
  Burden restricted to datatype D · area 99999 · NSA; the prior 24 private-industry combos
  reappear within tolerance (economy-private 1.4294 exact match). `cm.aspect.txt` (RSEs) and any
  seasonal-adjustment handling beyond landing the code verbatim are out of scope.

## The composed dim (`naics_labor_share`) — live 2026-07-14

Module [`pipelines/reference/materialize_naics_labor_share.py`](../../pipelines/reference/materialize_naics_labor_share.py),
SoR `s3://data-sink/active/naics_labor_share/`. One row per 6-digit NAICS — universe =
combo-layer NAICS (`naics_psc_labor_dim`, 853) ∪ SUSB 6-digit (970) = **1,133 rows**. Closes
the identity with a single join:

```
expected labor $ by category = award_$ × loaded_labor_share × (pct_of_industry / 100)
                                         └── this dim ──┘      └ naics_psc_labor_profile_categories ┘
```

⚠ `naics_psc_labor_profile_categories.pct_of_industry` is in **percent** (e.g. 21.08), not a
fraction — divide by 100 in the composition. Verified: $10M on 541512×D307 → loaded share
0.5162 → Software Developers (mix 21.08%) ≈ $1.09M expected labor $.

- **`payroll_share`** — SUSB '01: Total' row at the most specific NAICS level available,
  walk 6→5→4→3→sector (ranges 31-33/44-45/48-49 handled); `payroll_share_naics` +
  `payroll_share_level` carry provenance. Level distribution: 970 at 6-digit, 65/17/13 at
  5/4/3, 43 at sector, 25 at level 0 (sector 92 public administration — structurally absent
  from SUSB → economy share 0.176307, flagged).
- **`burden_multiplier`** — the ECEC private × all-occupations × all-workers cell matched via
  a deterministic NAICS→CES-supersector map (`336411` exact; `6231xx`→nursing care;
  31-33→manufacturing; 44-45→retail; 55→prof & business services; …); `burden_match_level`
  ∈ detail (36) / sector (1,014) / supersector (58: ag+mining → goods-producing) / economy
  (25). ECEC industry codes recovered from `bls_ecec_costs` (the burden table carries only
  group text).
- **`loaded_labor_share = payroll_share × burden_multiplier`** — the one scalar per NAICS.
  Median 0.270; anchors: 236220 → 0.0979 × 1.4415 = 0.141; 541512 → 0.3528 × 1.4629 = 0.516;
  561720 → 0.4157 × 1.3425 = 0.558; sector-92 fallback = 0.1763 × 1.4294 = 0.2520.
- **`bea_comp_share_of_output`** (+ `bea_summary_code`/`bea_summary_desc`/`bea_share_year`)
  — value-added-basis cross-check bound via `bea_naics_concordance` longest-prefix →
  summary-line name → latest derived year (2024); coverage 1,038/1,133 (91.6%). Never
  composed into the scalar (different denominator basis).
- **Pass-through hazard, kept verbatim:** `payroll_share` can exceed 1 where receipts are
  pass-through-heavy — 3 rows: 551114 holding companies (2.51), 561330 PEOs (1.51), 493110
  warehousing (1.05). Award-level pass-through discounting is query-time work against
  `award_subout_rollup`, deliberately not baked into the dim.
- **Indexes:** BTREE `naics_code`; BITMAP `payroll_share_level`, `burden_match_level`,
  `in_combo_layer`. Gates (all hard-fail in module): economy anchors exact (0.176307 /
  1.429448), row count = universe, share coverage 100%, BEA coverage ≥90%, sector-54 median
  in [0.30, 0.55] (0.3668), burden band [1.05, 1.90]. Ledger: `ops.labor_share_runs`.

Re-run:
```bash
doppler run -p core-x -c prd -- uv run --with pylance --with pyarrow --with duckdb \
  --with boto3 --with 'psycopg[binary]' \
  python -m pipelines.reference.materialize_naics_labor_share --smoke   # then full (no flag)
```

## BEA↔NAICS concordance (`bea_naics_concordance`)

Resolves the BEA `bea_industry_code` NULL and bridges BEA GDP-by-Industry lines onto the NAICS
grain. Module [`pipelines/reference/materialize_bea_naics_concordance.py`](../../pipelines/reference/materialize_bea_naics_concordance.py),
SoR `s3://data-sink/active/bea_naics_concordance/`.

- **Source (keyless static):** BEA "Industry and Commodity Codes and NAICS Concordance"
  `www.bea.gov/sites/default/files/2023-10/BEA-Industry-and-Commodity-Codes-and-NAICS-Concordance.xlsx`,
  sheet `NAICS Codes`. Each row maps one 2017 NAICS code up through BEA's five levels —
  Sector (21) → **Summary (71)** → Underlying Summary (138) → Detail (402) → GO Detail (414).
- **Landed:** 499 rows · 471 distinct NAICS (levels 2–6) · 73 summary codes · 406 detail codes ·
  23 sectors. `naics_code_clean` (digits-only) + `naics_level` are derived; the `*` multi-I-O
  marker is preserved verbatim and flagged in `naics_multi_io`. BTREE `naics_code_clean`,
  `bea_summary_code`; BITMAP `bea_sector_code`, `naics_level`.
- **How it resolves the join:** `bea_industry_value_added.industry_name` → `bea_summary_desc`
  (or `bea_u_summary_desc`/`bea_detail_desc`) → `bea_summary_code` (the BEA industry code); and
  `naics_code` is the NAICS-grain bridge (a 6-digit award NAICS rolls up by prefix, e.g.
  `5415`→541511–541519). **81/102 landed BEA names bind directly — i.e. 100 % of the mappable
  industries;** the 21 that don't are non-NAICS aggregates by construction (economy totals like
  `Gross domestic product`/`Private industries`, government rows like `National defense`/`State
  and local`, and special composites like the ICT-producing aggregate).

## Ledger

`ops.labor_share_runs` (HQX, `HQX_DB_URL_POOLED`) — one terminal-state row per dataset per run
(incl. `bea_naics_concordance`); an audit-write failure warns, never masks a good load.

## Re-run

```bash
doppler run -p core-x -c prd -- uv run --with pylance --with pyarrow --with duckdb \
  --with requests --with boto3 --with openpyxl --with 'psycopg[binary]' \
  python -m pipelines.reference.labor_share_ingest --stream susb   # or bea (ecec stream superseded)

# ECEC full universe (costs + burden) — reads the CM flat files from R2 landing:
doppler run -p core-x -c prd -- uv run --with pylance --with pyarrow --with boto3 \
  --with 'psycopg[binary]' python -m pipelines.reference.ecec_full_universe --smoke  # then full (no flag)
```
