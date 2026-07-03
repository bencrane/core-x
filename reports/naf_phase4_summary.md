# NAF Phase 4 — Reference Sidecar + County→FIPS Crosswalk

Closes the NAF wage-schedule pipeline: lands the OPM policy definitions and makes the priced SoR
(`naf_wage_rates`) composable with the SCA/OEWS wage stack and the FPDS contract spine.

## `naf_manual_docs` — 22 docs
The OPM Federal Wage System **Nonappropriated-Fund Operating Manual** (the policy / definitions
layer). Fetched from opm.gov → `s3://data-sink/landing/naf/manuals/` + Lance
`s3://data-sink/active/naf_manual_docs/` with per-document extracted text (pypdfium2).
- 11 subchapters + 11 appendices; 1.41 MB; **every doc text-extracted** (fail-closed coverage gate).
- Highest-value: `subchapter6` (NA/NL/NS job-grading), `subchapter4` (schedule layout), `appendixh`
  (canonical sample schedule the parser mirrors), `subchapter5`/`appendixc`/`appendixd` (wage-area
  geography), `appendixv` (agency special schedules).
- Columns: doc_name, category, source_url, r2_key, byte_len, sha256, page_count, text, text_char_len, fetched_at.
- Validator-role sidecar (mirrors `dol_sca_occupations`): OPM sets NAF policy; DoD DCPAS publishes the
  priced schedules `naf_wage_rates` parses.

## `naf_wage_area_county_fips` — 769 rows
Resolves each NAF wage-area county (from `naf_wage_area_geography`) to a canonical 5-digit Census FIPS
off `national_county2020` — the SAME gazetteer authority + `_nz()` name normalization the SCA
crosswalk uses (imported, not re-implemented).
- Grain: 1 row per (wage_area, naf_area, state, county). Indexes: BTREE(county_fips, wage_area, naf_area) + BITMAP(scope, match_method).
- Resolution: 733 exact + 3 alias + 12 territory + 6 collision (city/county) = **754 county-scoped**; 15 unmapped.
- **99.46% of stated (CONUS) counties resolved**; PR municipios + Guam + American Samoa resolved via
  a unique-match territory pass. Unmapped tail (15) = genuinely-defunct areas (Valdez-Cordova split
  2019; Bedford City) + Pacific atolls with no county FIPS + a few upstream geography-parse artifacts
  — **none guessed** (asymmetric-collision-safe, fail-closed).
- Independent-city collisions correct: VA Richmond city (51760) vs Fairfax County (51059); MD
  Baltimore city (24510) vs Baltimore County (24005); MO St. Louis city (29510).

## Composability achieved — the Phase-4 goal
`county_fips` == `usaspending_fpds_canonical_txn.pop_county_fips` == `sca_wd_county_rollup.county_fips`:
- **99% (493/496)** of NAF counties carry SCA wage-determination coverage → `naf_wage_rates` ↔
  `sca_wd_rates` joinable on county.
- FPDS place-of-performance county (`pop_county_fips`) joins directly to NAF wage areas.
- Full chain: **FPDS txn → pop_county_fips → naf_wage_area_county_fips → wage_area → naf_wage_rates**
  (NA/NL/NS/AS/PS priced matrix) → benchmarkable against `sca_wd_rates` / `soc_state_wage`.

## The complete NAF pipeline (Phases 0–4)
Census (#913) → Fetch (#916) → Parse+Materialize (#924) → **Sidecar+Crosswalk (this)**.

| Dataset | Rows | Role |
|---|---|---|
| `naf_wage_rates` | 1,670,700 | priced NA/NL/NS + AS/PS matrix (SoR) |
| `naf_nf_payband_ranges` | 52,818 | NF-level pay ranges |
| `naf_nf_payband_survey` | 86,838 | survey job wage points |
| `naf_wage_area_geography` | 19,791 | wage-area county/installation defs |
| `naf_wage_area_county_fips` | 769 | wage_area → Census FIPS (FPDS-spine bridge) |
| `naf_manual_docs` | 22 | OPM Operating Manual (policy definitions) |
