# Staffing Agencies & Their People — Data Asset Inventory (dex archive)

**Date:** 2026-06-07 · **Status:** reference · **Repo:** bencrane/core-x
**Scope:** what staffing-agency company + people data exists in the archived legacy **dex-db**, where it lives, how to read it, what each table contains, and the materialized table assembled from it.

---

## 0. Summary

- **Materialized 2026-06-07:** the 24,398 vertical-tagged staffing agencies are a Lance table at **`s3://data-sink/active/staffing_agencies`**. One row per agency: `company_name`, `domain`, `website`, `company_linkedin_url`, `linkedin_slug`, the 11-vertical `industries_served`, a unified `employee_band`, and clay `revenue_range`. Each firmographic field carries provenance (`firmo_source` = `in_clay` 16,312 · `pdl` 7,242 · `none` 844). Indices: `BTREE(domain_norm, company_linkedin_url)`, `BITMAP(employee_band, firmo_source)`. Schema and read recipe in §9. CSV mirror: `~/Desktop/hq/exports/staffing_agencies_24398_2026-06-07.csv`.
- The source data behind it is in the cold archive tier as Parquet (`archive/dex/entities/`), a snapshot dumped **2026-06-02** — the raw material §1–§8 describe.
- Distinct staffing-agency domains: **~45,515** (deduped union across 3 source tables).
- Structured "industries served" classification exists for **24,398** of them (11-vertical controlled vocabulary).
- Contacts exist for ~10k of the 45.5k domains; for those, ~30.5k contacts with ~100% email coverage and a 1,000-row outreach-priority ranking (§6).
- The three company tables overlap but are mostly disjoint (measured on domain and LinkedIn keys, §5.3); none is a subset of another.
- Distinct from the live `gtm.clay_find_people` Postgres ingest (the active edge-api stream) — different system, grain, and purpose (§8).

---

## 1. Provenance & location

The legacy **dex-db** (Polaris-backed Postgres, deprecated) was exported to R2 on 2026-06-02 as Parquet, one file per table, preserving its schema namespaces:

```
s3://data-sink/archive/dex/
  ├── bulk_ingest/
  ├── enrichment/
  ├── entities/      ← the application schema; all staffing data is here
  ├── gex/
  ├── ops/
  └── public/
```

`archive/dex/entities/` = **141 tables, 874.6 MB**, dumped 2026-06-02 19:26–19:37 UTC, one `<table>.parquet` per table (no multipart).

| Property | Value |
|---|---|
| Storage | Cloudflare R2, bucket `data-sink`, endpoint `e957a626a3a06d48d8e75c60c67d0e74.r2.cloudflarestorage.com` |
| Tier | `archive/` (cold). Not `active/` (the live Gen-3 Lance SoR). |
| Format | Parquet (not Lance). No scalar indices. Read-only snapshot. |
| Credentials | Doppler `core-x/prd` → `R2_ACCESS_KEY_ID` / `R2_SECRET_ACCESS_KEY` / `R2_ENDPOINT` (or `R2_ACCOUNT_ID`) |

### 1.1 How to read it (DuckDB-over-R2)

```sql
INSTALL httpfs; LOAD httpfs;
CREATE SECRET r2 (
  TYPE S3, PROVIDER credential_chain,
  ENDPOINT 'e957a626a3a06d48d8e75c60c67d0e74.r2.cloudflarestorage.com',
  URL_STYLE 'path', REGION 'us-east-1'
);
-- read any table:
SELECT * FROM read_parquet('s3://data-sink/archive/dex/entities/clay_find_companies.parquet') LIMIT 10;
```

Run with R2 creds exported to the AWS env so `credential_chain` resolves them:
```bash
doppler run -p core-x -c prd -- bash -c '
  export AWS_ACCESS_KEY_ID="$R2_ACCESS_KEY_ID" AWS_SECRET_ACCESS_KEY="$R2_SECRET_ACCESS_KEY"
  duckdb -f your_query.sql'
```
(The `lance` python lib is not installed in the default env; not required for this archive — it is Parquet. `aws s3 ls`, `boto3`, `pyarrow`, `duckdb` are available.)

---

## 2. Inferred lineage

The table topology forms a pipeline:

```
clay_find_companies         ── company records + LinkedIn/Clay enrichment (firmographic fields here)
        │
        ├─► target_companies ── the "target" list
        │        └─► demand_company_target_verticals ── 11-vertical "industries served" classification
        │
clay_find_people            ── people records at companies (carry job titles)
        └─► target_people   ── resolved + emailed contacts
                 ├─► target_people_emails        ── email per person (~100% coverage)
                 └─► target_people_outreach_tiers ── 1,000 rows, tiered

company_entities / person_entities ── identity-resolution tables; enrichment fields empty (§5.2)
```

These tables were populated at different scales and were not fully reconciled — the partial overlaps in §5.3 follow from that.

---

## 3. Headline counts (by grain)

"How many staffing agencies?" depends on the table and on whether the count is **rows** or **distinct domains**. Reconciled to normalized domain:

| Source | Table | Raw count | **Distinct domains** |
|---|---|---|---|
| clay enrichment table | `clay_find_companies` WHERE `industry='Staffing and Recruiting'` | 43,382 rows | **37,300** |
| vertical-classified | `target_companies` ∩ `demand_company_target_verticals` | 24,398 companies | **24,398** |
| identity master | `company_entities` (staffing) | 2,838 rows | **2,694** |
| deduped union of all three | — | — | **45,515** |

Counts by stage: ~45.5k domains → 24.4k vertical-classified → ~10k with any contact → ~6.7k with emailed contacts → 1,000 tiered.

Domain-overlap distribution across the 3 sources: **28,200** domains appear in 1 source, **15,753** in 2, **1,562** in all 3.

---

## 4. Industries-served classification

`demand_company_target_verticals` — **36,665 tags across 24,398 companies, avg 1.5 verticals each**, controlled vocabulary of **11 verticals**:

| vertical | companies | | vertical | companies |
|---|--:|---|---|--:|
| logistics_and_supply_chain | 8,703 | | construction | 2,349 |
| healthcare_clinical | 6,632 | | facilities_services | 1,981 |
| light_industrial_and_manufacturing | 5,342 | | it_staffing | 1,007 |
| engineering | 4,936 | | skilled_trades | 909 |
| accounting | 3,678 | | aerospace_and_defense | 814 |
| | | | trucking | 314 |

Join: `demand_company_target_verticals.target_company_id → target_companies.id`. Example: `Spherion` → {accounting, aerospace_and_defense, engineering, healthcare_clinical, it_staffing, light_industrial, logistics}.

Narrative specialty text also in `clay_find_companies.description` (free text, e.g. "Nurse Staffing Agency", "tech staffing… Software Development") and `clay_find_companies.raw_payload` (JSON `industries[]` + `derived_datapoints`).

---

## 5. Company tables — reference

### 5.1 `clay_find_companies`
- **495,482 rows** total; **43,382** staffing (37,300 domains).
- Grain: ~1 row per Clay company record (some duplicate domains).
- Columns: `id`(uuid), `domain`, `name`, `linkedin_url`, `linkedin_company_id`, `industry`, `size`, `country`, `location`, `annual_revenue`, `company_type`, `description`, `clay_company_id`, `total_funding_amount_range_usd`, `raw_payload`(json), `source_provider`, `source_table`, `ingested_at`, `updated_at`.
- Firmographic fields: `size`, `annual_revenue`, `description`, `linkedin_company_id`, `industry`.

### 5.2 `company_entities`
- **45,679 rows** total (39,101 with a domain); **2,838** staffing (2,694 domains).
- Enrichment columns are empty in this dump: `employee_count` 0.1% populated, `enrichment_confidence` 0.0%, `icp_fit_verdict` 0.0% (every row NULL).
- Populated columns: `org_id`, `entity_id`, `company_id`, `canonical_domain`, `canonical_name`, `industry`, `linkedin_url`, `company_linkedin_id`, `record_version`, `source_providers[]`.

### 5.3 `target_companies` + verticals
- `target_companies`: **2,679,234 rows** (target list). Columns: `id`(uuid), `company_name`, `domain`, `linkedin_url`, `entity_role`, `phone`, `mailing_address`, `source`, timestamps. No industry column — classification comes from the verticals table.
- The **24,398** vertical-tagged subset is the staffing-classified set.

Vertical-tagged ↔ `company_entities` overlap, measured on domain and LinkedIn slug (both keys ~100% populated):

| Match key | vertical-tagged (of 24,398) also in `company_entities` |
|---|--:|
| normalized domain | 1,597 |
| LinkedIn company slug | 1,516 |
| domain OR LinkedIn | 1,602 |

Adding LinkedIn matching changes overlap by +5. The two sets share ~1,600 entities; **22,796** vertical-tagged agencies are absent from the master; **~1,092** master agencies are un-tagged. Domain and LinkedIn agree to within 5 rows across the 24k companies.

---

## 6. People / contacts — reference

Coverage: ~6,725 agencies have `target_people`; ~7,389 have `clay_find_people`; these overlap, so ~10k distinct agencies have ≥1 person; ~35k of the 45.5k domains have none in this archive.

### 6.1 `target_people`
- **30,578 rows**; **29,563 (96.7%)** at vertical-tagged staffing agencies, across **6,725 agencies** (~4.4 people/agency).
- Columns: `id`(uuid), `full_name`, `first_name`, `last_name`, `company_name`, `company_domain`, `person_linkedin_url`, `business_concept`(free text), `clay_find_person_id`(→ `clay_find_people.id`), `target_company_id`(→ `target_companies.id`), timestamps.
- No structured job-title column; carries free-text `business_concept`. Structured titles are in `person_entities` and `clay_find_people`.

### 6.2 `target_people_emails`
- **30,574 rows** (≈ 1 per target person). Columns: `id`, `target_person_id`(→ `target_people.id`), `email`, `email_type`, `source`, `created_at`.

### 6.3 `target_people_outreach_tiers`
- **1,000 rows**. Columns: `id`, `target_person_id`, `tier`(smallint), `rationale`, `model`, `prompt_version`, `input_title`, `input_employee_range`, `input_company_name`, `tiered_at`. `input_title` is sparsely populated.

### 6.4 `person_entities`
- **2,116 rows**. Columns: `org_id`, `entity_id`, `company_id`, `full_name`, `first/last_name`, `linkedin_url`, `title`, `seniority`, `department`, `work_email`, `email_status`, `phone_e164`, `contact_confidence`, timestamps.
- Carries structured `title`, `seniority`, `department`, `work_email`, `phone_e164`.

### 6.5 `clay_find_people` (legacy dex)
- **66,618 rows**; **17,604** at staffing-industry companies across **7,389** companies.
- Columns: `id`(uuid), `linkedin_url`, `name`, `first/last_name`, `domain`, `location_name`, `location_country_iso`, `company_table_id`, `company_record_id`, `matched_experience`, `latest_experience_title`, `latest_experience_company`, `latest_experience_start_date`, `raw_payload`, `clay_find_company_id`(→ `clay_find_companies.id`), timestamps.
- `target_people.clay_find_person_id` points back into it. Carries `latest_experience_title`.

---

## 7. Join graph (entity-relationship)

```
demand_company_target_verticals.target_company_id ──► target_companies.id ◄── target_people.target_company_id
                                                                                      │ .id
                                                                          ┌───────────┴───────────┐
                                                       target_people_emails.target_person_id   target_people_outreach_tiers.target_person_id
                                                                                      │
                                                       target_people.clay_find_person_id ──► clay_find_people.id
                                                                                                        │ .clay_find_company_id
                                                                                                        ▼
                                                                                          clay_find_companies.id
company_entities  ── join to the above by normalized domain or LinkedIn slug (no shared surrogate key)
person_entities   ── join by company_id → company_entities.company_id, or by domain
```

Join keys: normalized `domain` (primary), LinkedIn company slug (`regexp_extract(url,'linkedin.com/company/([^/?#]+)')`, secondary). Surrogate UUIDs (`target_company_id`, `clay_find_company_id`) join only *within* their own table family — they do not bridge `clay_find_*` ↔ `target_*` ↔ `company_entities`.

---

## 8. Data caveats

1. **Archive, not live.** §1–§8 describe frozen Parquet in `archive/dex/entities/` (2026-06-02): not indexed, not appended, not the SoR.
2. **Distinct from `gtm.clay_find_people`.** A separate edge-api → Postgres stream lands people (board members / general contacts at arbitrary companies) into `gtm.clay_find_people`. That cohort is unrelated to these staffing agencies — different table, system, and grain.
3. **`company_entities` enrichment fields are empty** (§5.2); firmographic fields are in `clay_find_companies`.
4. **The 3 company tables are mostly disjoint** (§5.3); none nests in another.
5. **Structured job titles** are present only in `person_entities` (2,116 rows) and `clay_find_people.latest_experience_title`; `target_people` carries free-text `business_concept`.
6. **Email values are from the 2026-06-02 snapshot**; freshness is not current. The dex schema includes `millionverifier_*` tables (prior verification runs).
7. **~35k of the 45.5k domains have no associated people** in this archive.

---

## 9. Materialized table

`s3://data-sink/active/staffing_agencies` — Lance, **24,398 rows**, built 2026-06-07. One row per vertical-tagged staffing agency.

Schema (16 cols): `target_company_id · company_name · domain · website · domain_norm · company_linkedin_url · linkedin_slug · industries_served · employee_band · employee_band_source · revenue_range · revenue_source · country · year_founded · pdl_company_id · firmo_source`.

Indices: `BTREE(domain_norm)`, `BTREE(company_linkedin_url)`, `BITMAP(employee_band)`, `BITMAP(firmo_source)`.

Provenance: `firmo_source` = `in_clay` (16,312) · `pdl` (7,242) · `none` (844); `revenue_range` is clay-sourced (15,099 populated, `revenue_source='clay'`); `employee_band` unifies clay `size` and PDL `employee_size_range` (`employee_band_source` records which).

```python
import lance
ds = lance.dataset("s3://data-sink/active/staffing_agencies", storage_options=R2_SO)  # R2_SO per §1.1
ds.scanner(filter="employee_band IN ('1-10','11-50') AND firmo_source='in_clay'").to_table()
```

Build: assembled via DuckDB-over-R2 (dex archive source) joined to `active/pdl_normalized_companies` (BTREE on `normalized_domain` / `linkedin_slug`), streamed to Lance via Arrow.

**Not included in this table:**
- People/contacts. Source tables: `target_people` (+ `target_people_emails`, `target_people_outreach_tiers`), `clay_find_people` (titles); join by `person_linkedin_url`. Contacts exist for ~10k of the 24,398 (§6).
- Firmographics for the 844 `firmo_source='none'` rows and the ~846 `employee_band='unknown'` rows (no clay or PDL match). These rows carry `domain` and `company_linkedin_url`.
- Email values (company-grain table; emails are in `target_people_emails`).

---

## 10. Source queries

Counts in this document were produced via DuckDB-over-R2 against the Parquet archive (recipe in §1.1) and a Lance match against `active/pdl_normalized_companies`. Reproduce by reading the relevant `s3://data-sink/archive/dex/entities/<table>.parquet`, filtering `industry` for staffing (`ILIKE '%staff%' OR '%recruit%' OR '%employment%' OR '%human resource%' OR '%executive search%'`), or joining through `demand_company_target_verticals`.
