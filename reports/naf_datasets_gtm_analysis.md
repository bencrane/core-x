# NAF Wage-Schedule Datasets — Composition & GTM Analysis

**Scope.** Seven Lance datasets under `s3://data-sink/active/` derived from the DoD DCPAS
Nonappropriated-Fund (NAF) wage-schedule corpus (29,326 landed PDFs, Phases 0–5), plus the join surface
they open against the existing wage/geo/contract stack. **Phase 5 (2026-07-03)** normalized the schedule
dates to true `DATE32` and materialized the unified `view_county_wage_arbitrage_benchmark` — both folded
into this document. Every count, schema, and match-rate was pulled live from the Lance SoR via
DuckDB-over-Lance (2026-07-02 for Phases 0–4; 2026-07-03 for Phase 5); the join numbers are measured,
not asserted.

**System of record.** LanceDB on R2 (`s3://data-sink/active/`). No catalog layer; datasets addressed by
URI. Access pattern used throughout:

```bash
PYTHONPATH=$(pwd) doppler run --project core-x --config prd -- uv run --quiet \
  --with pylance --with pyarrow --with boto3 --with 'duckdb>=1.1' python3 - <<'PY'
import lance
from pipelines.bls.ingest import _storage_options
so=_storage_options()
ds=lance.dataset('s3://data-sink/active/naf_wage_rates/', storage_options=so)
print(ds.count_rows(), [f.name for f in ds.schema])
PY
```

---

## 1. Executive summary

What landed is a **priced, geo-resolvable, DoD labor-cost reference plane**. The core asset
(`naf_wage_rates`, 1,670,700 rows) is the full DoD NAF grade×step hourly-wage matrix — the
appropriated-equivalent of a locality wage table for the ~non-GS blue-collar and admin-support workforce
on and around military installations. Five supporting datasets frame it: NF-level pay-band ranges and
survey wage points (the white-collar NAF side), the wage-area→county→installation geography, a
county→Census-FIPS crosswalk bridge, and the OPM policy manual.

The crosswalk is the load-bearing piece. It resolves NAF wage areas to **5-digit Census FIPS**, the same
key already carried by `usaspending_fpds_canonical_txn.pop_county_fips`, `sca_wd_county_rollup.county_fips`,
and `national_county2020.county_fips`. That single shared key wires the NAF rate matrix into the federal
contract spine and the SCA/OEWS wage benchmarks.

**Verified addressable surface (measured live over 108,181,354 FPDS rows):**

| Metric | Value | Basis |
|---|---|---|
| Distinct NAF counties (county-scoped FIPS) | **502** | `naf_wage_area_county_fips`, scope=`county` |
| NAF counties also carrying SCA coverage | **499 / 502 (99.4%)** | join to `sca_wd_county_rollup` |
| FPDS txns with a non-null `pop_county_fips` | 89,864,404 | full scan |
| **FPDS txns landing in a NAF-covered county** | **66,571,845** | 74.1% of geolocated txns |
| **FPDS obligations landing in NAF counties** | **$8.53 trillion** | 80.4% of geolocated $ ($10.6T) |
| …of which DoD-awarded | **40,752,295 txns / $5.60 trillion** | `awarding_toptier_agency_abbreviation='DOD'` |
| …flagged SCA-applicable (`labor_standards_code='Y'`) | 5,016,262 txns / $1.85 trillion | service-contract labor standards |
| Counties with BOTH NAF + SCA coverage | 499, capturing **$8.52T** of NAF-county FPDS $ | triple-join |

The top-line unlock: for four out of five federal-contract dollars that can be geolocated, there is now a
DoD-published, grade-resolved wage floor for the place of performance, benchmarkable against the SCA wage
determination and the OEWS state wage for the same county/occupation. That is a wage-arbitrage and
labor-cost-targeting surface that did not exist in the stack before this landing.

**Phase 5 realized it directly.** The schedule dates are now true `DATE32` (chronologically-correct
"latest schedule" selection), and the three-way benchmark is materialized as
`view_county_wage_arbitrage_benchmark` (502 counties; §2.7) — the six raw datasets collapse into one
queryable "what does labor cost, three ways, where this contract performs" table.

---

## 2. Dataset-by-dataset detail

All six confirmed present and non-empty. Schemas below are the **live** field lists (a few differ from
earlier planning notes — flagged inline).

### 2.1 `naf_wage_rates` — 1,670,700 rows (the priced core SoR)

The DoD NAF grade×step hourly-wage matrix. Unifies three schedule families on one grain.

| Field | Type | Notes |
|---|---|---|
| `wage_area` | string | NAF wage-area code, zero-padded 3-char (e.g. `034`). Primary geo key. |
| `naf_area` | string | Area code; equals `wage_area` except for remainder areas (e.g. `034R`) that share a physical schedule but retain distinct rows. |
| `schedule_number` | int | Schedule/version identifier within an area. |
| `series` | string | `NA` (non-supervisory), `NL` (leader), `NS` (supervisor) — Crafts & Trades; `AS`/`PS` — Administrative Support / Patron Services. |
| `grade` | int | Job grade. NA/NL: 1–15; NS: 1–19; AS/PS: 1–7 (verified). |
| `step` | int | Within-grade step, 1–5 (verified, all series). |
| `hourly_rate` | double | Priced rate. Live range $4.75–$76.53, median ≈ $16.72. |
| `rate_type` | string | `regular` \| `special`. |
| `schedule_family` | string | `CT` (Crafts & Trades) \| `special` (special-rate amendments) \| `AS`. |
| `footnote`, `subject` | string | Free text; `subject` names the wage area ("…for the Lauderdale, Mississippi…"). |
| `effective_date`, `issue_date` | string | Free-text as printed on the PDF (retained lossless). |
| `effective_date_parsed`, `issue_date_parsed` | date32 | **Phase 5** — parsed to real DATE (99.94% / 99.99% coverage). Use these for recency/point-in-time; `BTREE(effective_date_parsed)` indexed. |
| `source_pdf_filename` | string | Provenance to a single DCPAS PDF. |
| `ingest_ts` | timestamp | Materialization time. |

- **Grain:** `(naf_area, source_pdf_filename, series, grade, step)` — grain-unique, fail-closed verified.
- **Indexes:** BTREE(`wage_area`,`naf_area`,`schedule_number`,`effective_date_parsed`) · BITMAP(`series`,`grade`,`step`,`rate_type`,`schedule_family`).
- **Live family × series distribution:**

  | family | series | rate_type | rows | areas |
  |---|---|---|---|---|
  | CT | NS | regular | 491,230 | 170 |
  | CT | NL | regular | 387,825 | 170 |
  | CT | NA | regular | 387,825 | 170 |
  | special | NS | special | 91,190 | 94 |
  | AS | PS | regular | 82,950 | 150 |
  | AS | AS | regular | 82,950 | 150 |
  | special | NL | special | 73,365 | 94 |
  | special | NA | special | 73,365 | 94 |

- **Provenance:** DCPAS `wageandsalary.dcpas.osd.mil` → landed R2 PDFs → matrix parsers
  (`_parse_matrix_naflns.py`, `_parse_matrix_as.py`) → materialize. Independently adversarially verified
  (value fidelity exact on sampled anchors; completeness defects found and fixed, +45,015 rows recovered).
- **Coverage:** 170 wage areas for CT; up to 49 versions of history per area (deepest: area 002).
- **Phase 5:** `effective_date_parsed`/`issue_date_parsed` (DATE32) now enable chronologically-correct
  latest-version selection; the raw strings are retained lossless. (Lexical sort of the *raw* string is
  still wrong — key off the parsed column; see 6.1.)

### 2.2 `naf_nf_payband_ranges` — 52,818 rows (white-collar NF pay bands)

NF-level (non-craft, salaried-equivalent) pay ranges from `-NF` payband schedules.

| Field | Type | Notes |
|---|---|---|
| `wage_area`,`naf_area`,`schedule_number` | | area keys (as above) |
| `nf_level` | int | NF pay-band level (1–5 observed). |
| `min_annual`,`max_annual` | double | Band salary floor/ceiling. |
| `min_hourly`,`max_hourly` | double | Hourly-equivalent floor/ceiling. |
| `effective_date` | string | Free-text (e.g. `04 Jul 2026`). |
| `source_pdf_filename`,`ingest_ts` | | provenance |

- **Grain:** `(naf_area, source_pdf_filename, nf_level)`. Indexes BTREE(area) · BITMAP(`nf_level`).
- **Semantics:** the pay-*range* rail (min/max), distinct from the discrete grade×step matrix. Same wage
  area supplies multiple vintages; `min_hourly` floors often equal the prevailing federal minimum ($7.25).

### 2.3 `naf_nf_payband_survey` — 86,838 rows (NF survey wage points)

Market-survey wage observations behind the NF pay bands, from `-PR` pay reports.

| Field | Type | Notes |
|---|---|---|
| `wage_area`,`naf_area`,`schedule_number`,`nf_level` | | keys |
| `survey_job_title` | string | e.g. `SALES ASSOCIATE`, `CASHIER-CHECKER`. |
| `matches` | int | Number of surveyed incumbents behind the point. |
| `average`,`high`,`low` | double | Surveyed hourly wage stats. |
| `effective_date`,`issue_date` | string | free-text |
| `source_pdf_filename`,`ingest_ts` | | provenance |

- **Grain (composite key verified unique 86,838/86,838):** `(wage_area, nf_level, survey_job_title, average, source_pdf_filename)`.
- **Semantics:** this is the only dataset carrying **named private-sector-comparable job titles** with
  sample sizes — the bridge from NAF pay bands to real labor-market survey wages. `matches` acts as a
  confidence/volume weight (values up to ~9,200 for high-volume retail titles).

### 2.4 `naf_wage_area_geography` — 19,791 rows (area definitions)

The county + installation definitions per wage area, from `-ScheduleBack` PDFs.

| Field | Type | Notes |
|---|---|---|
| `wage_area`,`naf_area`,`schedule_number` | | keys |
| `row_kind` | string | `installation` \| `county`. |
| `agency` | string | Owning agency (AAFES, Dept of the Army/Air Force/Navy, etc.). |
| `installation` | string | Base name (installation rows). |
| `county`,`state` | string | County+state (county rows; NULL on installation rows). |
| `applicable_schedule` | string | Cross-listed schedule reference. |
| `source_pdf_filename`,`ingest_ts` | | provenance |

- **Grain:** `(naf_area, source_pdf_filename, row_kind, agency, installation, county, state)`.
- **Semantics:** dual-purpose. `row_kind='county'` rows feed the FIPS crosswalk; `row_kind='installation'`
  rows are the **installation-name → wage-area** map (e.g. WA034 covers Dobbins Air Reserve Base,
  Anniston Army Depot, Fort McClellan) — the physical-base intelligence layer.
- **Caveat:** county/state are NULL on installation rows by design; installation rows are the geo you use
  to reach base names, county rows the geo you use to reach FIPS.

### 2.5 `naf_wage_area_county_fips` — 769 rows (the crosswalk bridge)

The load-bearing composability layer. Resolves each distinct NAF wage-area county to a canonical 5-digit
Census FIPS off `national_county2020`, using the **same gazetteer + name-normalization the SCA crosswalk
uses** (imported, not re-implemented).

| Field | Type | Notes |
|---|---|---|
| `wage_area`,`naf_area` | string | area keys |
| `state`,`county` | string | as stated in NAF geography (mixed bare/suffixed/independent-city forms) |
| `county_fips` | string | **5-digit Census FIPS — the shared join key.** Nullable (unmapped rows). |
| `county_name_census` | string | canonical census name |
| `class_fp` | string | Census class (H% county-equivalent, C% independent city) |
| `scope` | string | `county` (754 rows) \| `unmapped` (15 rows) |
| `match_method` | string | `exact` (733) \| `territory` (12) \| `collision_city` (4) \| `alias` (3) \| `collision_county` (2) \| `unmapped` (15) |
| `source`,`ingested_at` | | `naf_wage_area_geography+national_county2020` |

- **Grain:** 1 row per `(wage_area, naf_area, state, county)`. Indexes BTREE(`county_fips`,`wage_area`,`naf_area`) · BITMAP(`scope`,`match_method`).
- **Live resolution:** 754 county-scoped rows → **502 distinct FIPS across 119 wage areas**; 15 unmapped.
  99.46% of stated CONUS counties resolved.
- **Fail-closed:** territory rows (state NULL — Pacific atolls) are routed to `unmapped`, not guessed.
  Independent-city collisions handled correctly (VA Richmond city 51760 vs Fairfax County 51059; MD
  Baltimore city 24510 vs county 24005; MO St. Louis city 29510).
- **NOTE (schema drift vs planning brief):** live columns are
  `…county_fips, county_name_census, class_fp, scope, match_method, source, ingested_at`. There is **no
  `county_name` field** distinct from `county`, and `match_method` values are
  `exact|territory|collision_city|collision_county|alias|unmapped` (the brief's `scope=county|unmapped`
  is the `scope` field). Documented here so downstream joins reference real column names.

### 2.6 `naf_manual_docs` — 22 rows (OPM policy sidecar)

The OPM Federal Wage System NAF Operating Manual (policy/definitions layer). Validator-role sidecar,
mirroring `dol_sca_occupations`: OPM sets NAF policy, DoD DCPAS publishes the priced schedules.

| Field | Type | Notes |
|---|---|---|
| `doc_name`,`category` | string | 11 subchapters + 11 appendices |
| `source_url` | string | `opm.gov/policy-data-oversight/pay-leave/pay-systems/federal-wage-system/…` |
| `r2_key`,`byte_len`,`sha256`,`page_count` | | blob provenance |
| `text`,`text_char_len` | string/int | full extracted text (fail-closed: every doc extracted) |
| `fetched_at` | timestamp | |

- Highest-value docs for downstream logic: `subchapter6` (NA/NL/NS job-grading), `subchapter4`
  (schedule layout), `appendixh` (canonical sample schedule the parser mirrors), `subchapter5`/`appendixc`/`appendixd`
  (wage-area geography), `appendixv` (agency special schedules).
- Use: a text/RAG surface for defining what a series/grade *means* and how areas are constituted — the
  semantic dictionary for the numeric datasets above.

### 2.7 `view_county_wage_arbitrage_benchmark` — 502 rows (Phase 5 — the product surface)

The materialized three-way benchmark: NAF vs SCA vs OEWS wages aligned on `county_fips`, one row per
county. Unlock A1 turned from a join recipe into a queryable table.

| Field | Type | Notes |
|---|---|---|
| `county_fips` | string | 5-digit Census FIPS — grain (1:1). |
| `state`,`naf_wage_area`,`naf_area` | string | canonical NAF wage area for the county. |
| `naf_schedule_number` / `naf_effective_date` | int / date32 | the **chronologically latest** CT-regular schedule (via `effective_date_parsed`). |
| `naf_na_min/max`,`naf_nl_min/max`,`naf_ns_min/max` | double | NAF hourly bands per series. |
| `naf_na5_min/max`,`naf_na10_min/max` | double | NA grade-5 / grade-10 anchors. |
| `sca_canonical_wd_id`,`sca_occ_count`,`sca_min/max/median_hourly` | | canonical SCA WD occupation-rate band. |
| `oews_all_occ_h_median`,`oews_all_occ_h_p25/p75`,`oews_trades_h_median` | double | OEWS state market wage (All-Occupations + SOC 47/49/51 trades band). |
| `has_naf`,`has_sca`,`has_oews` | bool | coverage flags. |
| `source`,`ingested_at` | | provenance. |

- **Grain:** 1 row per `county_fips`. Indexes BTREE(`county_fips`,`naf_wage_area`) · BITMAP(`state`,`has_sca`,`has_oews`).
- **Coverage:** 100% NAF, 99.4% SCA, 97.6% OEWS; **488 counties triple-covered**.
- **Verified (Cobb County GA, 13067):** area 034 / schedule 13 / effective **2025-08-16** (true latest of
  24 schedules) — NAF CT NA-5 $15.29–17.85, NA-10 $20.78–24.25; SCA canonical WD 1977-0193 median $23.91;
  OEWS trades $22.80, all-occ $24.79. Independently adversarially verified (15 counties traced to source,
  0 mismatch; grain 1:1; all 502 FIPS valid).
- **Semantics:** NAF = latest CT-regular schedule; SCA = the county's canonical WD median across its
  occupations; OEWS = the state trades/all-occ bands. Occupation-specific alignment (per-occupation, not
  aggregate) awaits the NAF↔SCA↔SOC concordance (§7).

---

## 3. Relationship & join map

### 3.1 ASCII composition diagram

```
                         national_county2020  (3,235 rows, Census 2020 gazetteer)
                         county_fips ── canonical 5-digit FIPS authority
                                 ▲
                                 │ name-normalized resolve (_nz)
                                 │
  naf_wage_area_geography ──►  naf_wage_area_county_fips  ◄── SAME authority ──► sam_wd_county_fips_xwalk
   (row_kind='county')          769 rows / 502 FIPS                                (3,338 rows)
   (row_kind='installation')    wage_area ↔ county_fips                                   │
        │                            │        │                                          │
        │ installation names         │        │ county_fips                              ▼
        ▼                            │        └──────────────►  sca_wd_county_rollup (3,224)
  [BASE / INSTALLATION              │                            county_fips → active_sca_wd_ids
   INTELLIGENCE]                    │ wage_area                          │ wd_id
                                    ▼                                    ▼
                          naf_wage_rates (1,670,700)            sca_wd_rates (371,408)
                          wage_area,series,grade,step,$         wd_id,occupation_code,$
                          naf_nf_payband_ranges (52,818)                 ▲
                          naf_nf_payband_survey (86,838)                 │ occupation ≈ SOC
                                    ▲                                    │
                                    │ (semantic defs)          soc_state_wage (35,223)   state OEWS $
                          naf_manual_docs (22)                 soc_priced_skilled (830)  national OEWS+ONET
                                                                        ▲
              usaspending_fpds_canonical_txn (108,181,354) ────────────┘  (via NAICS/PSC → labor profile)
                pop_county_fips ─────────► THE SPINE. 66.6M txns / $8.53T land in NAF counties.
                naics_code, product_or_service_code, awarding_agency, labor_standards_code
```

### 3.2 Join-key table (every key measured live)

| From → To | Join key | Cardinality | **Verified match** |
|---|---|---|---|
| `naf_wage_area_county_fips` → `national_county2020` | `county_fips` | N:1 | 502/502 distinct NAF FIPS exist in gazetteer (crosswalk built off it) |
| `naf_wage_area_county_fips` → `naf_wage_rates` | `wage_area` | 1:N | 119 crosswalk areas; all resolvable to rate rows |
| `naf_wage_area_county_fips` → `sca_wd_county_rollup` | `county_fips` | 1:1 | **499/502 NAF counties (99.4%) carry SCA coverage** |
| `sca_wd_county_rollup` → `sca_wd_rates` | `canonical_wd_id`/`active_sca_wd_ids` → `wd_id` | 1:N | direct; 371,408 priced occupation rows across WDs |
| `usaspending_fpds_canonical_txn` → `naf_wage_area_county_fips` | `pop_county_fips` = `county_fips` | N:1 | **66,571,845 txns / $8.53T match a NAF county (74% of geolocated txns, 80% of $)** |
| `usaspending_fpds_canonical_txn` → `sca_wd_county_rollup` | `pop_county_fips` = `county_fips` | N:1 | overlaps NAF in 499 counties; $8.52T triple-covered |
| `sca_wd_rates` → `soc_state_wage` | `occupation_code` ≈ SOC (mapping req'd) | N:1 (per state) | SCA SCADD occ codes; OEWS keyed by SOC + `prim_state` |
| `naf_wage_rates` ↔ `sca_wd_rates` | via `county_fips` (geo), series≈occupation (semantic) | many:many | geo join exact; occupation mapping is analytical, not keyed |
| `usaspending_fpds_canonical_txn` → `naics_psc_labor_profile` | `naics_code`+`product_or_service_code` | N:1 | labor-play classification per NAICS×PSC (14,112 combos) |
| `naf_wage_area_geography` → `naf_wage_rates` | `wage_area` (installation→area→rates) | 1:N | installation name → wage area → full rate matrix |

**Key composition fact — VERIFIED:**
`naf_wage_area_county_fips.county_fips` == `national_county2020.county_fips` ==
`usaspending_fpds_canonical_txn.pop_county_fips` == `sca_wd_county_rollup.county_fips`, all 5-digit Census
FIPS. The chain **FPDS txn → pop_county_fips → naf_wage_area_county_fips → wage_area → naf_wage_rates**
resolves a contract's place-of-performance county to its DoD NAF priced wage schedule, and the parallel
branch resolves the same county to its SCA WD and OEWS state wage — enabling three-way wage benchmarking on
the same geography.

---

## 4. GTM unlocks (verifiable)

Each unlock below states the join path and quotes a **real query result** run live against the SoR. All
are reproducible with the access pattern in §0.

### Theme A — Labor-cost benchmarking / wage arbitrage

**A1. Three-way county wage benchmark (NAF vs SCA vs OEWS) for any place of performance.**
Path: `pop_county_fips` → `naf_wage_area_county_fips.wage_area` → `naf_wage_rates`; parallel
`county_fips` → `sca_wd_county_rollup` → `sca_wd_rates`; parallel `soc_state_wage` by state+SOC.
*Verified answer — Cobb County GA (FIPS 13067 → NAF wage area 034), latest schedule `034-013-CT.pdf`
(effective 16 August 2025):*

| Benchmark | Role | Rate |
|---|---|---|
| NAF CT **NA-5** (non-supervisory grade 5, step span) | DoD blue-collar floor | **$15.29–$17.85/hr** |
| NAF CT **NA-10** | mid-grade | $20.78–$24.25/hr |
| NAF CT **NS-10** (supervisor) | | $24.95–$29.11/hr |
| SCA **General Maintenance Worker** (occ 23370, WD 2015-4471) | service-contract floor | $23.43/hr |
| SCA **Janitor** (occ 11150) | | $16.93/hr |
| SCA **Laborer** (occ 23470) | | $18.13/hr |
| OEWS GA **49-9071** (Maint & Repair Workers) | market median / p10–p90 | $22.92 median ($16.31–$34.85) |
| OEWS GA **37-2011** (Janitors) | | $16.50 median ($12.55–$21.34) |

Reading: for a maintenance-labor scope performed in Cobb County, the DoD NAF floor (NA-grade), the SCA
service-contract floor, and the OEWS market wage are all now co-resolvable on one county — the exact
inputs to a wage-arbitrage / bid-cost model.

**A2. Rank counties by NAF-vs-market wage spread.** Path: aggregate `naf_wage_rates` NA-grade rate per
`wage_area`, join crosswalk → `county_fips`, join `soc_state_wage` on state. The spread (OEWS median −
NAF floor) surfaces where DoD-pegged labor is under/over market — arbitrage geographies. All inputs
verified present; the median NAF `hourly_rate` is $16.72 vs OEWS blue-collar medians in the high-teens to
low-$20s, so the spread is real and computable at national scale.

**A3. NAF pay-band vs private survey wage gap for white-collar/retail roles.** Path:
`naf_nf_payband_ranges` (min/max hourly by NF level) vs `naf_nf_payband_survey` (surveyed average/low/high
by named job title), joined on `(wage_area, nf_level)`. *Verified:* NAF WA034 NF-1 band floors at
$7.25/hr with a ceiling range $21.04–$22.92; survey CASHIER-CHECKER points (WA130) average $9.19–$11.59
across vintages on 8,700–9,200 matches — the band-vs-survey gap is directly measurable.

### Theme B — Federal-contract place-of-performance targeting

**B1. Total DoD contract dollars performed in NAF-priced counties.** Path: FPDS `pop_county_fips` →
crosswalk, filter `awarding_toptier_agency_abbreviation='DOD'`. *Verified: 40,752,295 DoD txns /
**$5.60 trillion** land in the 502 NAF counties.* This is the addressable DoD labor-adjacent contract
base with a native wage floor attached.

**B2. Service-contract (SCA-flagged) dollars in NAF counties.** Path: same, filter
`labor_standards_code='Y'`. *Verified: 5,016,262 txns / **$1.85 trillion** of SCA-labor-standards
contracts perform in NAF-covered counties* — the subset where a published wage floor is legally load-bearing
to bid costing.

**B3. Total geolocated contract surface with a wage floor.** *Verified: 66,571,845 txns / **$8.53T**
(all agencies) land in NAF counties — 74% of geolocated FPDS txns, 80% of geolocated obligations.* Every
one now carries a resolvable NAF grade×step matrix for its place of performance.

**B4. Contract-vertical labor intensity overlay.** Path: FPDS `naics_code`+`product_or_service_code` →
`naics_psc_labor_profile` (`is_labor_play`, `work_summary`, `n_awards`, `total_dollars_obligated`), then
county → NAF/SCA wage. Isolates the labor-play contract combos (14,112 classified) and prices their
place-of-performance labor. Both sides verified present and joinable on NAICS×PSC.

### Theme C — Installation & geography intelligence

**C1. Installation → wage area → full rate matrix.** Path: `naf_wage_area_geography`
(`row_kind='installation'`) → `wage_area` → `naf_wage_rates`. *Verified:* WA034 installations include
Dobbins Air Reserve Base, Anniston Army Depot, Fort McClellan, Camp F.D. Merrill, Navy Lake Site
Allatoona — each inherits the WA034 NA/NL/NS/AS/PS matrix. Answers "what is the blue-collar wage schedule
at base X."

**C2. Multi-area counties (wage-schedule overlap zones).** Path: crosswalk grouped by `county_fips`.
*Verified:* counties spanning two wage areas exist (e.g. Jefferson County AL 01073 → areas 104+105;
Alameda CA 06001 → 056+059; Kanawha WV 54003 → 124+088) — flags where two DoD wage regimes co-apply and a
contract could be priced under either.

**C3. Agency footprint per area.** Path: `naf_wage_area_geography.agency`. Distinguishes AAFES vs
Army/Air Force/Navy NAF employers per installation — segmentation for who actually employs against a given
schedule.

### Theme D — Temporal / effective-date analysis

**D1. Wage-schedule history depth per area.** Path: `naf_wage_rates` grouped by `wage_area`,
`source_pdf_filename`. *Verified:* up to 49 schedule versions per area (deepest area 002); WA034 alone
carries schedules from `034-008-CT` (effective 1996) through `034-013-CT` (effective 16 August 2025) — a
~30-year NAF wage time series per area.

**D2. Point-in-time rate lookup — DONE (Phase 5).** `effective_date_parsed` (DATE32) is now materialized
at **99.94%** coverage with `BTREE(effective_date_parsed)` for pushdown. Latest-schedule selection is now
chronologically correct — *proven:* for area 034 the parsed-date max is `2025-08-16`, where lexical string
max would wrongly return "9 August 2008". `view_county_wage_arbitrage_benchmark` (§2.7) uses this to pick
each county's current schedule. The residual ~0.06% are faithfully-transcribed source-PDF typos (e.g.
"30 Mar 3013" for 2013), retained lossless and not affecting the recency key.

**D3. Real-wage escalation per grade.** Path: same-area NA-1 across vintages. *Verified:* WA034 NA-1
step-1 rose from $5.38 (2021 schedule) to $10.89 (2025 schedule `034-013`), NA-15 top step to $30.49 —
a per-grade escalation curve is computable for any area with multi-vintage history.

### Theme E — Occupation / skill mapping

**E1. NAF series/grade ↔ SCA occupation ↔ SOC benchmarking surface.** NAF `series`+`grade` is analogous
to SCA `occupation_code` and OEWS `soc_code` (all wage-benchmarking surfaces). Path: crosswalk unifies the
geography; a series/grade↔occupation↔SOC concordance (analytical, not yet keyed) unifies the occupation
axis. `naf_manual_docs` subchapter6 supplies the NA/NL/NS job-grading definitions to build that
concordance. *Status: geography join exact and verified; occupation concordance is the recommended next
build (see §7).*

**E2. Skilled-trade wage triangulation.** Path: SCA `sca_wd_rates` named trades (Electrician, Plumber,
Machinist — verified present in WD 2015-4471 at $24–$38/hr) ↔ NAF NA high-grades ↔ `soc_priced_skilled`
(830 SOCs with ONET descriptions + BLS employment projections). Gives a skilled-trade wage + supply/demand
(openings, separations) view per trade.

---

## 5. Verified addressable-surface summary (the numbers to quote)

| Question | Answer (measured) |
|---|---|
| Distinct NAF counties | 502 (across 119 wage areas) |
| NAF counties w/ SCA coverage | 499 (99.4%) |
| FPDS txns in NAF counties | 66,571,845 (74% of geolocated) |
| FPDS $ in NAF counties | $8.53T (80% of geolocated $10.6T) |
| DoD FPDS $ in NAF counties | $5.60T / 40.75M txns |
| SCA-flagged FPDS $ in NAF counties | $1.85T / 5.02M txns |
| Triple-covered (NAF+SCA) counties | 499, capturing $8.52T |
| Point-in-time date parseability (Phase 5) | **99.94%** of `effective_date` → DATE32 |
| Arbitrage benchmark counties (Phase 5) | 502 (488 triple-covered NAF+SCA+OEWS) |

---

## 6. Limitations & data-quality caveats

1. **~~`effective_date`/`issue_date` are free-text strings~~ — RESOLVED (Phase 5).** True `DATE32`
   `effective_date_parsed`/`issue_date_parsed` are now materialized (99.94%+ coverage, BTREE-indexed); the
   raw strings are retained lossless. Residual note: ~1,120 rows (0.07%) carry faithfully-transcribed
   source-PDF typo years (e.g. "30 Mar 3013" for 2013) — at min/max extremes, not polluting the recency
   key; a consumer can filter year `< 1985 or > 2027`.
2. **15 unmapped crosswalk counties** (`scope='unmapped'`): defunct areas (Valdez-Cordova split 2019,
   Bedford City), Pacific atolls with no county FIPS, and a few upstream geography-parse artifacts. Not
   guessed (fail-closed) — but 15 wage-area coverages have no FIPS bridge.
3. **NAF series/grade ↔ SCA occupation ↔ SOC is not a keyed join.** It is an analytical concordance to be
   built. The geography join (`county_fips`) is exact; the occupation axis is currently semantic
   ("NA-grade ≈ blue-collar laborer ≈ SOC 49/47/37 families").
4. **`naf_wage_area_geography` installation rows have NULL county/state** by design — installation→FIPS
   requires going installation→wage_area→county rows→FIPS, not a direct field.
5. **Remainder areas (`034R`)** share a physical schedule but retain distinct `naf_area` rows; naive
   `wage_area`-only aggregation can double-count. Group on `naf_area` when precision matters.
6. **Deferred 5th dataset:** the "NAF Special Wage Rate **Ranges**" occupation-memo format (~1,300 PDFs
   the matrix parser correctly skips) is not yet materialized — occupation-specific ranges are absent.
7. **Payband survey has multiple vintages per title** (not duplicates — composite grain is unique
   86,838/86,838); consumers must pick a vintage, not sum across them.
8. **FPDS `pop_county_fips` is null on ~17% of txns** (18.3M of 108.2M have null pop FIPS); those cannot
   be geo-benchmarked. The 66.6M NAF-county figure is against the 89.9M geolocated base.

---

## 7. Next-priority recommendations (ranked by leverage)

**✅ DONE — Phase 5 (landed, PR #927):**
- ~~Normalize `effective_date`/`issue_date` → real DATE + point-in-time view.~~ `effective_date_parsed`
  DATE32 materialized on the three rate datasets (99.94%+), BTREE-indexed; the lexical-recency footgun is
  fixed and independently verified.
- ~~Build the unified wage-arbitrage benchmark view.~~ `view_county_wage_arbitrage_benchmark` materialized
  (502 counties; §2.7) — NAF vs SCA vs OEWS on `county_fips`, adversarially verified (0 mismatch).

**1. Materialize the deferred "Special Wage Rate Ranges" occupation-memo dataset (~1,300 PDFs).**
The one known coverage gap in the NAF corpus. A distinct occupation-range parser (not the grade×step
matrix parser) yields occupation-specific ranges — the missing granular layer between the grade matrix and
named occupations, and a direct feeder for the concordance below.

**2. Build the NAF series/grade ↔ SCA occupation_code ↔ SOC concordance.**
Converts the strongest semantic relationship (E1/E2) into a keyed join, making the occupation axis of the
arbitrage view (§2.7) per-occupation rather than aggregate-only. Seed from `naf_manual_docs` subchapter6
(NA/NL/NS job-grading definitions) mapped to SCA SCADD titles and SOC families.

**3. Resolve the 15 unmapped crosswalk counties + normalize remainder areas.**
Closes fail-closed gaps: hand-map the defunct/split counties (Valdez-Cordova → its 2019 successors,
Bedford City → Bedford County VA), and add an explicit `is_remainder` flag so `034R`-style rows don't
silently double-count in `wage_area` aggregations.

**4. Add an FPDS-driven NAF demand overlay.**
Materialize `view_county_wage_arbitrage_benchmark` joined against FPDS obligations per county (the $8.53T
surface) as a ranked targeting table — "counties by DoD service-contract $ × NAF wage floor × SCA/OEWS
spread" — the direct input to a place-of-performance targeting motion (B1–B4).

---

*All figures verified live against `s3://data-sink/active/` via DuckDB-over-Lance — Phases 0–4 on
2026-07-02, Phase 5 (date normalization + arbitrage view) on 2026-07-03. FPDS aggregates computed over the
full 108,181,354-row `usaspending_fpds_canonical_txn`. Sample benchmark: Cobb County GA (FIPS 13067) →
NAF wage area 034, schedule 13 (effective 2025-08-16, chronologically latest of 24).*
