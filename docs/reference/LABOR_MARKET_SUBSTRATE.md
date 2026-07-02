# Labor-Market Substrate & GovCon Staffing Bridge — Canonical Reference

**Status:** authoritative · **Built:** 2026-07-01 (single session) · **Plane:** Gen-3 (LanceDB on R2)

This document is the get-up-to-speed reference for the labor-market data substrate landed in
one session: **BLS** (occupational supply/demand/wages), **DOL SCA** (service-contract labor
taxonomy), **SAM.gov Wage Determinations** (locality-priced labor rates), and **O*NET** (the SOC
semantic layer). It covers every dataset, its provenance and keys, the pipeline modules, the
join spine that ties them to prime-award NAICS+PSC, the known gotchas, and what remains to build.

---

## 1. Why this exists — the business thesis

The operator sells to **staffing firms**: connect them with companies that just **won federal
contracts** and therefore have **imminent labor needs** the staffing firm can fulfill. The data
job is to turn a prime award's coarse codes into a concrete, priced labor demand:

> **what was bought** (PSC + NAICS on a prime award, at a place of performance)
> → **who the winner must hire** (occupations / SCA labor categories)
> → **at what locality-specific rate** (SCA/DBA wage determination).

Every dataset below is a layer in that pipeline.

---

## 2. Architecture / access (the Gen-3 plane)

- **System of record:** LanceDB datasets under `s3://data-sink/active/`, written directly to
  Cloudflare **R2** (no catalog). Addressed by URI. Bucket `data-sink`, account
  `e957a626a3a06d48d8e75c60c67d0e74`, endpoint `https://<account>.r2.cloudflarestorage.com`.
- **Transport-only raw:** operator drops land in `s3://data-sink/landing/<domain>/`; ephemeral.
- **Compute:** DuckDB reads ephemeral raw → projects/casts → streams to Lance. Net-new datasets
  pin `data_storage_version="2.1"`, `mode="overwrite"`, `max_rows_per_file=1_048_576`,
  `max_bytes_per_file=90 GiB`.
- **Indexing:** BTREE on load-bearing resolution keys, BITMAP on low-cardinality categoricals.
- **Credentials:** Doppler project `core-x`, config `prd` — `R2_ACCESS_KEY_ID`,
  `R2_SECRET_ACCESS_KEY`, `R2_ENDPOINT`, `HQX_DB_URL_POOLED` (Postgres ops ledgers), `SAM_API_KEY`
  (present but **not needed** for the SAM frontend crawl). Local `rclone` remote `r2:` also works.
- **Run pattern:** `doppler run -p core-x -c prd -- python3 -m pipelines.<domain>.ingest ...`
  (add `uv run --with pylance --with pyarrow --with requests --with 'psycopg[binary]' [--with pypdfium2]`
  for PDF/scrape deps). All modules reuse plumbing from **`pipelines/bls/ingest.py`**:
  `_storage_options`, `_s3_client`, `_build_indexes`, `DATA_STORAGE_VERSION`, `MAX_ROWS_PER_FILE`,
  `MAX_BYTES_PER_FILE` (and `_record_run` for BLS specifically).
- **Read a dataset:**
  ```python
  import lance, os
  so={"aws_access_key_id":os.environ["R2_ACCESS_KEY_ID"],
      "aws_secret_access_key":os.environ["R2_SECRET_ACCESS_KEY"],
      "endpoint":os.environ["R2_ENDPOINT"],"region":"auto"}
  d=lance.dataset("s3://data-sink/active/<name>", storage_options=so)
  ```

---

## 3. Dataset catalog (all under `s3://data-sink/active/`)

### 3.1 BLS — occupational supply, demand, wages

| dataset | rows × cols | grain / keys | source |
|---|---|---|---|
| `bls_oews_2025` | 413,527 × 35 | industry×occupation staffing pattern **+ current wages**; BTREE `area`,`occ_code`,`naics`; BITMAP `area_type`,`i_group`,`own_code`,`o_group`,`prim_state` | OEWS May-2025 `oesm25all.zip → all_data_M_2025.xlsx` |
| `bls_ep_industry_occupation_matrix_2024_2034` | 113,473 × 20 | **projected** industry×occupation (2024→34), one row per `(industry_code, occupation_code)`; BTREE `industry_code`,`occupation_code`,`naics_code` | scraped `data.bls.gov/projections/nationalMatrix` (423 industries) |
| `bls_employment_projections_2024_2034` | 1,113 × ~19 | occupation projections **+ worker characteristics** (median wage, education, experience, training, openings); BTREE `occupation_code`; BITMAP `occupation_type` | `occupation.xlsx` Table 1.2 (**overwrote** the original thin TE1000 CSV) |
| `bls_ep_occupation_separations_openings_2024_2034` | 1,113 × ~17 | separations/openings per occupation; BTREE `occupation_code`; BITMAP `occupation_type` | `occupation.xlsx` Table 1.10 |
| `bls_ep_occupation_utilization_factors_2024_2034` | 998 × ~8 | utilization factors, occupation×industry; BTREE `occupation_code`,`industry_code` | `occupation.xlsx` Table 1.12 |
| `bls_ep_major_group_employment_2024_2034` | 23 | major-group rollup; BTREE `occupation_code` | `occupation.xlsx` Table 1.1 |
| `bls_ep_stem_occupations_2024_2034` | 3 | STEM aggregates | `occupation.xlsx` Table 1.11 |
| `bls_ep_occupation_rankings_2024_2034` | 124 | fastest growing/declining lists, `ranking_list`+`rank`; BTREE `occupation_code`; BITMAP `ranking_list` | `occupation.xlsx` Tables 1.3–1.6 |

**OEWS deliberately-not-ingested (documented):** the 4 redundant part-zips (`oesm25nat/st/ma/in4`)
— `all_data` is the **exact superset** (413,527 = Σ parts, reconciled by `AREA_TYPE`/`I_GROUP`/
`OWN_CODE`); `file_descriptions.xlsx` (BLS column readme).
**occupation.xlsx not-ingested:** Index (TOC); Table 1.8 (by-occupation matrix index = transpose
of the industry matrix already landed); Table 1.9 (industry index = the scrape seed).

### 3.2 DOL SCA — federal service-contract labor taxonomy

| dataset | rows | grain / keys | source |
|---|---|---|---|
| `dol_sca_occupations` | 502 | structured SCA `occupation_code → title → definition`, `family_code`, `entry_type` (family/occupational_base/occupation); BTREE `occupation_code`,`family_code`; BITMAP `entry_type` | SCA Directory of Occupations (5th ed.) PDF |
| `dol_sca_directory_occupations` | 139 | verbatim per-page text (lossless); BTREE `page_no` | ↑ same PDF |
| `dol_sca_directory_occupations_blob` | 1 | raw PDF bytes + sha256; BTREE `sha256` | ↑ same PDF |

Parse gate: 502 entries, 25 families, 0 orphans, 99.31% char coverage (fail-closed).

### 3.3 SAM.gov Wage Determinations — locality-priced labor rates

| dataset | rows | grain / keys | source |
|---|---|---|---|
| `sam_wage_determinations` | 10,055 | every **active** WD (SCA 1,521 / DBA 4,236 / CBA 4,298), `_id` grain; BTREE `_id`,`full_reference_number`,`short_reference_number`; BITMAP `type_code`,`is_active`,`is_latest`,`revision_number` | `sam.gov/api/prod/sgs/v1/search?index=wd` (key-less frontend) |
| `sam_wd_county_coverage` | 33,156 | exploded `(wd_id, state_code, county_code, county_name)` — 57 states/territories; BTREE `wd_id`,`state_code`,`county_code` | ↑ same search records |
| `sam_wd_rate_documents` | 5,757 | the plaintext **rate register** (labor-category → wage/fringe) per SCA/DBA WD; `document` verbatim + sha256; BTREE `wd_id`,`full_reference_number`; BITMAP `wd_type`,`active`,`revision_number` | `sam.gov/api/prod/wdol/v1/wd/{fullReferenceNumber}/{revisionNumber}` |
| `sam_wd_cba_pointers` | 4,298 | one **pointer** per active CBA WD (§4(c)) — employer × union × agency × locality × effective dates, **NO rate table**; 45% (1,940) carry contractor/union/agency, rest skeletal (vintages 2003–2026); BTREE `wd_id`,`cba_number`,`organization_id`; BITMAP `status`,`latest`,`archived` | `sam.gov/api/prod/wdol/v1/cba/{_id}` (key-less frontend) |
| `sam_wd_cba_coverage` | 4,270 | exploded `(wd_id, state_name, county_name, city, zip)` — 56 states; full state **NAMES** (not USPS codes / SAM county codes); BTREE `wd_id`,`cba_number` | ↑ same CBA records |

A §4(c) CBA WD has **no WHD rate register** — it is a *pointer* that cites the governing union contract.
`/wdol/v1/cba/{_id}` is **key-less** (verified 200; the earlier "X-Auth-Token gated" note was wrong) but
carries only employer × union × agency × locality × effective-dates — **no wage table**. SAM.gov hosts no
copy of the CBA (every `/cba/{_id}/{attachments,files,document,download}` sub-path 404s). The wage/fringe
schedule lives in the **external union contract**, resolved from independent corpora — DOL **OLMS CBA
File** (primary; private+public 1,000+-employee units), **OPM** NAF CBAs (federal-sector; the NAF slice is
wage-bearing), **Cornell ILR** (historical) — matched on the pointer's `(contractor, union, locality,
dates)`. Structured `(occupation, rate, fringe)` extraction from those documents is a **pending** downstream
dataset.

### 3.4 O*NET 30.3 — the SOC semantic layer (45 datasets, 1,104,314 rows)

All named `onet_<snake_table>`. Every 8-digit `o_net_soc_code` gets a derived 6-digit `soc_code`
companion (strip `.NN` suffix) for the OEWS/SCA join; `related_o_net_soc_code`→`related_soc_code` too.

**Occupation-keyed (join on `o_net_soc_code` / `soc_code`):** `onet_occupation_data` (1,016 — SOC→title+description, the spine), `onet_job_titles` (57,543 — alternate titles), `onet_sample_of_reported_titles` (7,953), `onet_task_statements` (18,796), `onet_task_ratings` (161,559), `onet_knowledge` (59,004), `onet_abilities` (92,976), `onet_essential_skills` (17,880), `onet_transferable_skills` (44,700), `onet_software_skills` (31,821), `onet_work_activities` (73,308), `onet_work_context` (297,676), `onet_work_styles` (37,422), `onet_education` (11,100), `onet_training_and_experience` (26,025), `onet_career_interest_types` (8,307), `onet_specific_interest_areas` (73,062), `onet_emerging_tasks` (328), `onet_interests_illustrative_activities` (188), `onet_interests_illustrative_occupations` (186), `onet_occupation_level_metadata` (32,202), `onet_related_occupations` (18,460 — both sides SOC-derived), `onet_survey_booklet_locations` (211).

**Element/task crosswalks (keyed on `*_element_id` / `task_id`):** `onet_abilities_to_work_activities` (381), `onet_abilities_to_work_context` (139), `onet_essential_skills_to_work_activities` (110), `onet_essential_skills_to_work_context` (39), `onet_transferable_skills_to_work_activities` (122), `onet_transferable_skills_to_work_context` (57), `onet_work_styles_to_work_activities` (303), `onet_work_styles_to_work_context` (266), `onet_gwas_to_iwas_to_dwas` (2,087), `onet_gwas_to_iwas` (332), `onet_tasks_to_dwas` (23,850), `onet_specific_interest_areas_to_career_interest_types` (53).

**Reference/lookup:** `onet_content_model_reference` (3,006), `onet_scales_reference` (32), `onet_job_zone_reference` (4), `onet_job_zones` (923), `onet_level_scale_anchors` (483), `onet_education_categories` (12), `onet_task_categories` (7), `onet_work_context_categories` (281), `onet_training_and_experience_categories` (29), `onet_career_interest_type_keywords` (75).

### 3.5 Pre-existing, referenced (not built this session)

- `usaspending_api_catalog` — 176 rows, one per USASpending API endpoint (the endpoint catalog).
  **No SAM.gov equivalent exists** (verified). Sibling `usaspending_api_fresh` is a probe companion.
- **Prime awards** (USASpending / FPDS canonical datasets, e.g. `usaspending_*`) — the **input**:
  carry NAICS + PSC + place-of-performance county. This is what feeds the whole bridge.

---

## 4. The join spine (how it all connects)

```
prime award (usaspending/FPDS): NAICS + PSC + place-of-performance COUNTY (FIPS)
  │
  ├─ NAICS ──► bls_oews_2025            (current occupations + wages)
  │           bls_ep_industry_occupation_matrix_2024_2034 (projected occ mix)
  │
  ├─ SOC (occ_code / soc_code) ──► bls_oews / bls_ep_* ⇄ onet_* (tasks/skills/titles)
  │                                 ⇄ dol_sca_occupations   [SCA↔SOC bridged via O*NET/LLM]
  │
  ├─ PSC ──► service-vs-product gate (first char) + (via LLM) labor categories
  │
  └─ county FIPS ──► sam_wd_county_coverage ──► sam_wd_rate_documents  (locality SCA/DBA rates)
                     sam_wd_cba_pointers ──► [external union contract]  (§4(c) CBA-covered labor; wages off-platform)
```

- **NAICS**: 6-digit on prime awards; OEWS carries it at 2/3/4/5/6-digit (`i_group`); EP matrix
  `naics_code` maps its EP `industry_code` to 2022 NAICS (415/423 industries; `TE*` aggregates have none).
- **SOC**: OEWS `occ_code` (6-digit) = O*NET derived `soc_code` = the SCA-bridge target. O*NET→OEWS
  direct join coverage **94.35%** (818/867 O*NET SOCs); 49 residuals are 100% explained (military
  SOCs OEWS doesn't survey + O*NET-finer SOC splits) — **not** a key defect.
- **SCA ↔ SOC**: no clean key join (SCA uses its own 5-digit codes). Bridged semantically via O*NET
  alternate titles + an LLM, or a **one-time crosswalk to build** (see §8).
- **County geography**: `sam_wd_county_coverage.county_code` is a **SAM-internal code, NOT a Census
  county FIPS** (values fall in a ~14343–20125 range and are non-unique across states) — do **not**
  join it to the spine's `pop_county_fips` (a true 5-digit Census FIPS = 2-digit state + 3-digit
  county). A naive `county_code = pop_county_fips` equality fabricates ~150 spurious county matches
  (~$83B of garbage obligation). Only `sam_wd_county_coverage.state_code` binds cleanly (to
  `primary_place_of_performance_state_code`). To bind SAM WD county geography to the spine, a
  `(state, county_name) → Census FIPS` crosswalk against the Census `national_county2020` gazetteer
  is **required**; an alias table must close the ~131-pair name-normalization residual (DE KALB,
  St. Clair, Miami-Dade/DADE, St. Johns, AK census-area renames). The bound FIPS then reaches the
  actual priced labor rate in `sam_wd_rate_documents`.

---

## 5. The layered labor-profile model (the product logic)

- **L0 — labor gate (deterministic):** PSC first char → service (labor-heavy) vs product (skip).
- **L1 — industry staffing pattern (join):** NAICS → OEWS ranked occupations + median wages.
- **L2 — contract labor categories (LLM):** NAICS+PSC(+title) → narrowed SOC + SCA categories,
  grounded on O*NET candidates + the 502 SCA codes (RAG; the model *selects*, never free-hallucinates).
- **L3 — quantified demand (join+math):** award value ÷ loaded rates × PoP → FTE/headcount by
  category; overlay EP growth (`bls_ep_industry_occupation_matrix`) for hot roles.
- **L4 — staffing pitch (LLM synthesis):** per winning company → labor-demand profile + outreach.

**Efficient architecture (pending build):** key the LLM step on **distinct (NAICS × PSC) combos**
(thousands), not per-award (millions). Materialize once → **`naics_psc_labor_profile`** (BTREE
`(naics, psc)`), then every award/company joins to it for free. This is the highest-leverage next build.

---

## 6. Pipeline modules

| module | builds | notes |
|---|---|---|
| `pipelines/bls/ingest.py` | `bls_oews_2025`, (orig) EP CSV | registry (`oews` xlsx-in-zip, `employment_projections` csv); **exports the shared R2/index/ledger plumbing** everything else reuses |
| `pipelines/bls/ep_industry_occupation_matrix.py` | `bls_ep_industry_occupation_matrix_2024_2034` | scrapes 423 industries; seeded from landed `occupation.xlsx` Table 1.9; fail-closed on any industry |
| `pipelines/bls/ep_occupation_workbook.py` | the 5 remaining `bls_ep_*` + EP upgrade | Tables 1.1/1.2/1.10/1.11/1.12 + 1.3–1.6 |
| `pipelines/dol/ingest.py` | `dol_sca_*` (3 sinks) | pypdfium2 text; coverage-gated structured parse |
| `pipelines/sam_gov/sam_wd_manifest.py` | `sam_wage_determinations`, `sam_wd_county_coverage`, `sam_wd_rate_documents`, `sam_wd_cba_pointers`, `sam_wd_cba_coverage` | `--run` manifest, `--rates` SCA/DBA rate stage, `--cba` CBA pointer stage; key-less frontend crawl |
| `pipelines/onet/ingest.py` | all 45 `onet_*` | **registry-free auto-discovery** of the zip; robust to O*NET version |

**ops ledgers (Postgres, `HQX_DB_URL_POOLED`):** `ops.bls_runs`, `ops.dol_runs`, `ops.onet_runs`,
`ops.sam_wage_determination_runs`. Apply with `--init-state` on the BLS/DOL/O*NET modules.

---

## 7. Domain gotchas / decisions another agent must know

**OEWS** — `all_data` is the complete superset; part-zips are redundant. Values stored verbatim
VARCHAR; suppression markers (`*` not-released, `#` topcode) preserved (coercing would null them).

**EP CSV / workbook** — the original `IND_TE1000.csv` had Excel formula-escaped codes (`=""11-1011""`
→ de-artifact to `11-1011`) and thousands-commas. Table 1.2 **topcodes** the top-19 wages as
`>=$239,200` → stored **verbatim string** (numeric coercion silently nulls them = data left behind).

**EP industry-occupation matrix** — `occupation.xlsx` Table 1.9 is a **426-row industry index**, NOT
the matrix; the cells live behind per-industry links `data.bls.gov/projections/nationalMatrix?queryParams=<code>&ioType=i`.
Pages are **server-rendered** (parse the inline HTML table with `pandas.read_html`; the "download"
button just serializes it client-side). 423 industries fetched; **TE1000 slice = 1,113** is the
self-check (reproduces the standalone EP feed). `bls.gov/emp/...` blocks datacenter curl UAs (403);
`data.bls.gov/projections` does not.

**SAM.gov WD — key-less frontend crawl** (modeled on `pipelines/sam_gov/sam_attachment_manifest.py`):
- The `api.sam.gov` (api.data.gov) gateway caps non-federal keys at a tiny daily quota — useless for
  a catalog crawl. The **public website backend `sam.gov/api/prod/*`** serves the same data **key-less,
  no quota**. Requires a **residential IP** (this runner's host; WAF blocks datacenter IPs) and headers:
  browser UA, `Accept: application/json, text/plain, */*` (a strict `application/json` **406s** the
  hal+json), `Origin: https://sam.gov`, `Referer: https://sam.gov/wage-determination`.
- **Offset ceiling:** the search index is Elasticsearch capped at `maxAllowedRecords=10000` —
  `(page*size)+size > 10000` → HTTP 400. Since active = 10,055 > 10,000, naive paging **silently drops
  the last 55 SCA WDs**. Workaround (proven gap-0): 3 single-page `q=SCA|DBA|CBA` slices (each <10k),
  union + **dedup on `_id`** (the universal key — `fullReferenceNumber` is NULL for all CBA), **fail-closed
  reconciliation** against live `is_active=true totalElements`. No sort/`search_after` cursor exists.
- **Rate endpoint:** `sam.gov/api/prod/wdol/v1/wd/{fullReferenceNumber}/{revisionNumber}` → hal+json
  `document` (the rate register text). `revisionNumber` is a **required** path segment. SCA+DBA only.
- **CBA endpoint:** `sam.gov/api/prod/wdol/v1/cba/{_id}` → hal+json **pointer** (employer, union, agency,
  locality, effective dates) — **key-less** (verified 200; the earlier "X-Auth-Token gated" note was
  wrong). A §4(c) CBA WD has **no** rate register; SAM.gov hosts no CBA copy (every
  `/cba/{_id}/{attachments,files,document,download}` sub-path 404s). Harvested by `--cba`.
- Location has **3 incompatible shapes** in the search record (SCA `states[].counties.{include/exclude}`,
  DBA singular `state`, CBA flat `states[].counties[]`); the county explode branches on `type.code`.

**O*NET** — 45 Excel tables; `o_net_soc_code` (8-digit `11-1011.00`) → `soc_code` (6-digit `11-1011`).
Crosswalk tables key on `<domain>_element_id` (e.g. `abilities_element_id`, `gwa_element_id`) — the
index detection must catch `*_id` columns, not just literal `element_id`. **Correction to an earlier
false caveat:** O*NET 30.3 is **exactly 45 tables in every format** (Excel = text = SQL). Tables like
`tools_used`/`technology_skills`/`dwa_reference`/`iwa_reference`/`unspsc_reference`/`alternate_titles`/
`riasec_keywords` **do not exist in 30.3** — they were retired (Tools & Technology, discontinued ~2020)
or renamed (`alternate_titles`→`job_titles`, `riasec_keywords`→`career_interest_type_keywords`,
`technology_skills`→`software_skills`). Nothing is missing; the Excel workbook is the complete DB.

---

## 8. Landing zone state (`s3://data-sink/landing/`)

Created this session: `bls/{employment-projections,oews}`, `dol/`, `sam-gov-labor/` (**unused** —
SAM WDs came via the frontend crawl, not a drop), `o-net/` (holds `db_30_3_excel.zip`).
Discussed but **not** created: `gsa-calc/`, `dol-oflc/`.
Folders in object storage exist only as prefixes; created via zero-byte `.keep` markers.

---

## 9. Provenance (merged PRs, all squash-merged to `main`)

| PR | commit | scope |
|---|---|---|
| [#840](https://github.com/bencrane/core-x/pull/840) | `f013baa` | BLS OEWS + EP (orig) + DOL SCA (3 sinks) |
| [#846](https://github.com/bencrane/core-x/pull/846) | `3daed0e` | BLS EP industry-occupation matrix (423-industry scrape) |
| [#849](https://github.com/bencrane/core-x/pull/849) | `7d303ee` | `occupation.xlsx` remaining tables + EP feed upgrade |
| [#853](https://github.com/bencrane/core-x/pull/853) | `a49eb5c` | SAM WD catalog + county coverage + rate documents |
| [#857](https://github.com/bencrane/core-x/pull/857) | `40c7951` | O*NET 30.3 (45 tables) |

> Note on #853: a **concurrent agent** built and merged `sam_wd_manifest.py` in parallel; an
> independent build in this session converged **byte-identical** to it, so it was reconciled to
> #853 rather than opening a duplicate PR.

---

## 10. Pending / highest-leverage next builds

1. **`naics_psc_labor_profile` (L2 materialization)** — the combo→labor-category cache keyed on
   distinct (NAICS × PSC); the join hub every downstream layer hangs off. *Highest leverage.*
2. **Structured WD rate parsing** — `sam_wd_rate_documents.document` (fixed-width text) →
   `(wd_id, occupation/labor_category, wage, fringe)` rows. Turns the register text into join-able rates.
3. **SCA ↔ SOC crosswalk** — build **once** from O*NET titles (`onet_job_titles`) + SCA definitions
   (`dol_sca_occupations`); reuse as a static Lance crosswalk (no authoritative public one exists).
4. **PSC → labor-category seed map** — small derived map to ground the LLM instead of bare codes.
5. Optional feeds discussed, not built: GSA CALC (API **dead** — data would need GSA schedule price
   lists), DOL OFLC (H-1B LCA/PERM — bulk XLSX, employer-level demand overlay), BLS EP is complete.

---

## 11. One-line operating conventions

- Ingests are **fail-closed** and **idempotent** (`mode="overwrite"`); "no data left behind" is
  enforced by per-source row-count reconciliation, not assumed.
- Raw stays transport-only; the Lance dataset is the record. Verbatim-VARCHAR fidelity by default;
  type only where unambiguous and lossless.
- Every load-bearing key gets a hard BTREE index. Deliberately-not-ingested items are **documented**
  (with the redundancy proof), never silently dropped.
- Git: commit → push → PR → **self-merge** (`gh pr merge --squash --delete-branch`) → pull into the
  operator's `main` checkout. "Merged" ≠ done; done = the change is on disk in `main`.
