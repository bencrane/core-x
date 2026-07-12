# VA Veteran Demand-Side Cluster

County-grain veteran-population + disability-compensation reference datasets, keyed on 5-char
county FIPS + state. The demand denominator for GTM market pages ranking where clinician-staffing
demand (VA C&P exam work — NAICS `621111` × PSC `Q403`) outruns local medical-labor supply. The
exam contracts book to a few national primes (QTC / OptumServe / Loyal Source / VES) at their ops
hubs and carry no meaningful place-of-performance geography; actual demand tracks where veterans
live. Join to `txn_events_combo_by_geo.pop_county_fips`, `gtm_entity_geo`, and SAM `physical_state`.

**Module:** `pipelines/reference/va_veteran_demand.py` · **Source:** data.va.gov (Socrata JSON API, public, no key) · **Ledger:** `ops.labor_share_runs` (reused)

## Datasets (`s3://data-sink/active/`)

| Dataset | Rows | Grain | Notes |
|---|--:|---|---|
| `va_vetpop_county` | 781,200 | fips × snapshot_year × age_group × sex | VetPop2023 (`jrjd-qghv`). 31 projection years FY2023→FY2053; age_group ∈ {17 to 44, 45 to 64, 65 to 84, 85 and older}. |
| `va_vetpop_county_total` | 97,650 | fips × snapshot_year | Rollup — `veterans_total` = Σ over age/sex. The plain denominator; filter `snapshot_year=2023` for the current base. |
| `va_disability_comp_county` | 15,656 | fiscal_year × fips | Disability Compensation Recipients by County, FY 2019/2021/2023/2024/2025. `recipients` + SCD-rating severity bands (`scd_0_20`…`scd_100`) + age + sex (nullable — richer in later FYs). |

Indexes: BTREE `fips` (+ `state` on vetpop detail) on all; BITMAP on `age_group,sex,snapshot_year` / `snapshot_year,state` / `fiscal_year,state` respectively.

## Run record (2026-07-12)

- VetPop gates: fips all 5-char (0 bad); 3,150 distinct counties; **FY2023 national veterans = 18,266,748** (in band 16–20M); rollup FY2023 sum == detail sum exactly.
- Disability: 5 fips-native FYs landed (2019: 3,131 · 2021: 3,130 · 2023: 3,128 · 2024: 3,132 · 2025: 3,135 rows); 0 null-recipient; 0 malformed fips; **8 unmappable rows kept with null fips** (VA "Unknown" / "Other Foreign Countries" catch-all buckets — retained so recipient totals stay complete).
- Ledger: `ops.labor_share_runs` success rows for `va_vetpop_county` + `va_disability_comp_county`.

## Scope decisions (probe-driven, 2026-07-12)

- **FY2020 disability (`6263-7mn5`) excluded** — degenerate schema: no `fips_code`, counties = "Unknown". Would have forced null-fips on the whole year.
- **PACT Act dashboard — not landed.** Not a clean file on data.va.gov (search returns unrelated datasets; it's a rendered dashboard). The disability-comp-recipients-by-county series **is** the county-grain, PACT-Act-driven exam-demand proxy and supersedes the need.
- **GDX (VA expenditure geography) — deferred.** The data.va.gov GDX JSON resources (`qhqa-74yq`, `2hnn-8vkt`) return a single collapsed `geographic_distribution_of` column — malformed via the API. Revisit only if a clean source surfaces.
- **VA facility locations — out of scope.** VA Facilities API is keyed (HTTP 401); `federal_sites_lance` already covers site inventory.

## Sidecar promotion — disposition (2026-07-12, operator-directed)

`va_vetpop_county_total` + `va_disability_comp_county` **promoted** to the query-sidecar (build stamp `20260712T224718Z`, 83→85 tables). Both are plain sorted copies (tier D): `va_vetpop_county_total` sorted `fips, snapshot_year`; `va_disability_comp_county` sorted `fips, fiscal_year`. Guide catalog + §4 pattern (g) updated in the same PR.

- **Adjacency sweep (shipped as free riders via `SELECT *`):** disability SCD-severity bands (`scd_0_20`…`scd_100`), age bands, and sex — the analyst's next-question columns (severity → re-exam intensity; age → exam propensity); and vetpop's full **31 projection years** (FY2023→FY2053) so the per-county veteran *trend* is one statement, not a rebuild.
- **Parked structural:** `va_vetpop_county` (781k age×sex×year population **detail**) stays Lance-only — disability already carries age/sex at county grain for the demand signal; no demand for population-by-age structure, and it is 8× the row weight recurring every rebuild.
- **State-abbr note:** VA `state` is full-name; the canonical join is county `fips` (matches `txn_events_combo_by_geo.pop_county_fips`). 2-letter state derives via `substr(fips,1,2)` → `sam_county_fips_crosswalk` (served) — not bloating the VA tables.
- **Measured:** the veteran-density × disability-recipients × severity cross (§4 pattern g) serves in **~6 ms** on the warm endpoint, vs. a full pylance/DuckDB Lance read (creds + engine spin-up, seconds) pre-promotion.
