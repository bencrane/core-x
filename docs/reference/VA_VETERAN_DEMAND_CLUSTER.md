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

## Sidecar-promotion note

Small (all three < 800K rows) and they cross directly to the GTM geo marts (`txn_events_combo_by_geo`, `gtm_entity_geo`) on `fips`/`state`. Warranted candidates for query-sidecar promotion if per-geography market-page builds query them repeatedly — `va_vetpop_county_total` (denominator) and `va_disability_comp_county` (demand signal) are the two to promote; the full age/sex projection detail can stay Lance-only.
