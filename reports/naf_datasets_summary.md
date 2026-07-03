# NAF Wage-Schedule Datasets — Phase 2/3 Materialization

Parsed the 29,326 landed PDFs (Phase 1) into four append-only-snapshot Lance datasets under
`s3://data-sink/active/`. 27,449 PDFs yielded structured content; 1,877 parsed empty — almost all
the **"NAF Special Wage Rate Ranges"** occupation-memo format (a distinct document type the NA/NL/NS
matrix parser correctly skips), plus a handful of image-only PDFs with no text layer.

Route: `schedule_type → parser → dataset`
- CT, Special → `_parse_matrix_naflns` → `naf_wage_rates`
- AS → `_parse_matrix_as` → `naf_wage_rates`
- NF, PBS → `_parse_payband_range` → `naf_nf_payband_ranges`
- PBPR → `_parse_survey` → `naf_nf_payband_survey`
- RSB → `_parse_geography` → `naf_wage_area_geography`

## Datasets

### `naf_wage_rates` — 1,670,700 rows
The priced grade×step matrix — the core SoR. Unifies three schedule families on one grain.
- **Grain**: `(naf_area, source_pdf_filename, series, grade, step)`
- **Families**: CT (regular NA/NL/NS Crafts & Trades), Special (special-rate NA/NL/NS, $15/hr-adjusted amendments), AS (AS/PS Administrative Support / Patron Services)
- **series** ∈ {NA, NL, NS, AS, PS} · **rate_type** ∈ {regular, special} · **schedule_family** ∈ {CT, special, AS}
- Columns: wage_area, naf_area, schedule_number, series, grade, step, hourly_rate, rate_type, schedule_family, footnote, subject, effective_date, issue_date, source_pdf_filename, ingest_ts
- Indexes: BTREE(wage_area, naf_area, schedule_number) · BITMAP(series, grade, step, rate_type, schedule_family)

### `naf_nf_payband_ranges` — 52,818 rows
NF-level pay ranges (min/max annual + hourly) from `-NF` payband schedules.
- **Grain**: `(naf_area, source_pdf_filename, nf_level)`
- Columns: wage_area, naf_area, schedule_number, nf_level, min_annual, min_hourly, max_annual, max_hourly, effective_date, source_pdf_filename, ingest_ts
- Indexes: BTREE(wage_area, naf_area, schedule_number) · BITMAP(nf_level)

### `naf_nf_payband_survey` — 86,838 rows
Survey job wage points (matches / average / high / low per NF level) from `-PR` pay reports.
- **Grain**: `(naf_area, source_pdf_filename, nf_level, survey_job_title, average)`
- Columns: wage_area, naf_area, schedule_number, nf_level, survey_job_title, matches, average, high, low, effective_date, issue_date, source_pdf_filename, ingest_ts
- Indexes: BTREE(wage_area, naf_area, schedule_number) · BITMAP(nf_level)

### `naf_wage_area_geography` — 19,791 rows
Wage-area county + installation definitions from `-ScheduleBack` ("Schedule Back") PDFs. The
explicit county lists per wage area feed the Phase-4 county→FIPS crosswalk directly.
- **Grain**: `(naf_area, source_pdf_filename, row_kind, agency, installation, county, state)`
- **row_kind** ∈ {installation, county}
- Columns: wage_area, naf_area, schedule_number, row_kind, agency, installation, county, state, applicable_schedule, source_pdf_filename, ingest_ts
- Indexes: BTREE(wage_area, naf_area, state) · BITMAP(row_kind)

## Verification
- **Fail-closed materialize verify**: grain-unique across all four; 12 hand-verified anchors matched (area 101 sched 45 NA-1-1 = 16.23, NS-19-5 = 45.00; area 004 sched 011 AS-1-1 = 5.23, PS-1-1 = 4.75; plus payband/survey/geography anchors). Identical duplicate rows (2-page repeated tables, cross-listed area shares) collapsed; conflicting-value dupes hard-fail.
- **Adversarial independent verification** (agents re-extract sampled values from source PDFs by eye, no project parsers): value fidelity perfect — 72/72 + 48/48 + 40/40 exact matches, zero dupes, clean domains. It surfaced a systematic **completeness** defect the value-anchors could not: the matrix parser silently dropped grade rows whose step values wrapped/wide-spaced across pdftotext lines (802 AS PDFs missing grades 1–4; 6 CT PDFs missing top grades), and geography glued schedule-column tokens onto 261 installation names. All three fixed (spacing-independent 12-token parsing for AS, wrapped-line stitching for CT, schedule-column strip for geography) and re-materialized — **recovering +45,015 rows**. Post-fix completeness: AS 2,370/2,370 PDFs at the full 70 rows; CT 5,169/5,171 at 245 (2 legitimate partials); 0 glued geography names. A final independent re-verification surfaced two more defects — a geography multi-agency mis-grouping (2nd+ agency headers stored as installations) and a payband `effective_date` boilerplate leak on 324 rows (0.61%); both fixed and re-materialized (scoped). **Final state: all four datasets independently verified — value fidelity exact (wage_rates 36/36, ranges 36/36, survey 40/40), completeness confirmed, agency grouping and effective dates clean.**

## Notes / latent work
- **Special = real rate data**, not authorization artifacts: `-AUTH` special-rate schedules are legitimate NA/NL/NS matrices folded into `naf_wage_rates` (rate_type=special). The separate "NAF Special Wage Rate **Ranges**" occupation-memo format (~1,300 PDFs) is a distinct 5th dataset (occupation-specific ranges) — deferred.
- Area codes that share a physical schedule (e.g. `034` and its remainder area `034R`) each retain their own rate rows, distinguished by `naf_area`.
- 1,005 source PDFs were `missing` (enumerated versions with no published PDF); 3 `failed` (asterisk-filename redirect loops) — recorded in the Phase-1 fetch manifest.
