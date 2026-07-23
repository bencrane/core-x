# Industry Cost-Structure Batch — run record

Directive: `hq/directives/2026-07-23-industry-cost-structure-batch-ingest.md` (operator-authorized
2026-07-23, executed same day). Module: `pipelines/reference/industry_cost_structure_ingest.py`.
Ledger: HQX `ops.industry_cost_structure_runs`. Predecessor stack: labor-share-of-revenue
(PR #1118–#1120, `docs/reference/LABOR_SHARE_OF_REVENUE_STACK.md`).

Purpose: close the "where does the industry dollar go" decomposition beyond labor — K/L/E/M/S
input recipe, equipment capex flow + capital stock by asset type, filed income-statement cost
lines, firm demographics, county-grain wages, TFP/MFP.

## Datasets landed (all `s3://data-sink/active/`)

| Dataset | Grain | Source | Rows | Years |
|---|---|---|---|---|
| `bea_bls_klems` | measure-sheet × industry × year | BEA-BLS integrated production account xlsx (1997–2024) | 52,808 | 1997–2024 |
| `bea_fixed_assets_detail` | industry × asset × year × measure | BEA FA `detailnonres_stk1.xlsx` + `detailnonres_inv1.xlsx` | 1,641,024 | 1901/1925–2024 |
| `census_aces_capex` | CELL (file × sheet × row × col) | ACES tables 1998–2022, every xls/xlsx incl. OLD- variants | 332,483 | 1998–2022 |
| `irs_soi_corp_industry` | CELL (file × sheet × row × col) | IRS `{yy}co{NN}ccr.xlsx`, tables 01–26 probed (13–14 land/yr) | 125,027 | TY2014–2022 |
| `census_bds_firm_dynamics` | variant × cell × year (all-varchar, DuckDB union-by-name) | BDS 2023 release, ALL 133 variant CSVs incl. geo crosses | **PENDING** — 123/133 CSVs parked in `landing/census/bds/`; 11 blocked by a Census WAF rule (all egress IPs incl. Modal); stream resumes from landing on retry | 1978–2023 |
| `bls_qcew_annual` | area × own × industry × agglvl × size × year (38 cols typed) | QCEW annual singlefiles 2001–2024 (all 24 via operator landing drops) | 86,295,978 | 2001–2024 |
| `bls_mfp_major_sector` | MachineReadable long form | landed catalog: major-sectors TFP (+historical) | 14,274 | 1948→ |
| `bls_tfp_detailed_industries` | MachineReadable long form | landed catalog: detailed-industry TFP KLEMS + mfg/transport | 570,468 | 1987→ |
| `bls_productivity_tables` | MachineReadable long form (union) | remaining landed catalog w/ MR sheets (16 files) | 2,855,571 | varies |
| `bls_productivity_cells` | CELL grain | remaining landed catalog w/o MR sheets (6 files) | 10,265,795 | varies |

## Decisions of record

- **ASM dropped at Stage 1** — no keyless expense-detail files exist on www2.census.gov
  (`asm/tables/{2018–2023}/` carries only a robotics workbook). The Census API is key-gated
  (predecessor lesson) and out of scope. Gap: manufacturing expense detail (materials, fuels,
  electricity) has no landed source; revisit only if a keyless publication appears.
- **Cell grain for ACES + SOI** — presentation workbooks with multi-row headers and per-year
  layout drift; a schema'd melt would be speculation (L44). Cell grain is lossless and
  deterministic; semantic extraction is downstream composition work.
- **BLS web is agent-blocked (403, Akamai)** — the productivity catalog was hand-landed by the
  operator at `s3://data-sink/landing/bls/productivity/` (27 files, 2026-07-23). MachineReadable
  sheets (present in all modern BLS productivity workbooks) made structured landing trivial.
- **QCEW zips are parked verbatim** in `s3://data-sink/landing/bls/qcew/` as they are fetched
  (operator convention: landing holds raw artifacts; active holds the SoR).
- **Landing reads disable AWS response-checksum validation** — Cyberduck multipart uploads to
  R2 carry composite checksums that AWS SDKs ≥2025 reject as full-object mismatches.
- **Gate deviations from the directive** (recorded per §8): ACES gate is presence/volume-based
  (cell grain has no semantic equipment/structures columns); BEA FA gate is span+asset-presence
  (summing hierarchical asset rows would double-count).

## Per-stream columns + samples

TBD — filled after full runs.

## Gate values

TBD — filled after full runs; also pasted into the PR body.

## Follow-on menu (present in landing, structurally landed, not yet composed)

- Labor productivity detailed industries / major sectors / **by state and region** (pairs with
  QCEW county grain), hours-employment, TFP capital details (IPP / info-processing), rental
  prices of capital, industry contributions to growth, dispersion, retail margins, government
  productivity, historical SIC series.
- Composition work (explicitly out of scope this cycle): NAICS↔BEA/KLEMS concordance joins,
  the demo "$2.3T pie" pack section, sidecar promotion of klems / bea_fa / soi.
