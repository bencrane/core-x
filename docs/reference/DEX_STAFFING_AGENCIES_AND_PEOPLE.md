# Staffing Agencies & Their People — Data Asset Inventory (dex archive)

**Date:** 2026-06-07 · **Status:** canonical reference / handoff · **Repo:** bencrane/core-x
**Scope:** what staffing-agency company + people data exists in the archived legacy **dex-db**, where it lives, how to read it, what each table actually is, and how to assemble it into a usable master.

---

## 0. TL;DR (read this first)

- ✅ **MATERIALIZED (2026-06-07) — START HERE:** the 24,398 vertical-tagged staffing agencies are now a live, indexed Lance table at **`s3://data-sink/active/staffing_agencies`**. One row per agency: identity (`company_name`, `domain`, `website`, `company_linkedin_url`, `linkedin_slug`), the 11-vertical `industries_served`, a unified `employee_band`, and clay `revenue_range` — every firmographic field carries explicit provenance (`firmo_source` ∈ {`in_clay` 16,312 · `pdl` 7,242 · `none` 844}). Indices: `BTREE(domain_norm, company_linkedin_url)`, `BITMAP(employee_band, firmo_source)`. **Query this, don't re-derive from the archive.** See §9. CSV mirror: `~/Desktop/hq/exports/staffing_agencies_24398_2026-06-07.csv`.
- The *source* data behind it lives **only in the cold archive tier** as **Parquet** (`archive/dex/entities/`), a frozen snapshot dumped **2026-06-02** — that's the raw material §1–§8 describe.
- Canonical company count: **~45,515 distinct staffing-agency domains** (deduped union across 3 source tables).
- Structured **"industries served"** classification exists for **24,398** of them (11-vertical controlled vocabulary).
- **People/contacts exist for a minority** of the universe (~10k of 45.5k domains have ≥1 person). Where they exist they are rich: ~30.5k contacts with **~100% email coverage** and a 1,000-row outreach-priority ranking.
- The three company tables are **overlapping but mostly-disjoint workflows, NOT a clean funnel** — validated on both domain and LinkedIn keys (see §5.3). Do not assume one is a subset of another.
- ⚠️ **This is NOT the live `gtm.clay_find_people` Postgres ingest** (the active millions-of-people edge-api stream). Different system, different grain, different purpose. Do not conflate. See §8.

---

## 1. Provenance & location

The legacy **dex-db** (Polaris-backed Postgres, now deprecated) was exported wholesale to R2 on 2026-06-02 as Parquet, one file per table, preserving its schema namespaces:

```
s3://data-sink/archive/dex/
  ├── bulk_ingest/
  ├── enrichment/
  ├── entities/      ← the application schema; ALL staffing data is here
  ├── gex/
  ├── ops/
  └── public/
```

`archive/dex/entities/` = **141 tables, 874.6 MB**, dumped 2026-06-02 19:26–19:37 UTC, one `<table>.parquet` per table (no multipart).

| Property | Value |
|---|---|
| Storage | Cloudflare R2, bucket `data-sink`, endpoint `e957a626a3a06d48d8e75c60c67d0e74.r2.cloudflarestorage.com` |
| Tier | `archive/` — **cold**. NOT `active/` (the live Gen-3 Lance SoR). |
| Format | **Parquet** (not Lance). **No scalar indices.** Read-only frozen snapshot. |
| Credentials | Doppler `core-x/prd` → `R2_ACCESS_KEY_ID` / `R2_SECRET_ACCESS_KEY` / `R2_ENDPOINT` (or `R2_ACCOUNT_ID`) |

### 1.1 How to read it (DuckDB-over-R2, copy-paste)

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

Run with R2 creds exported to the AWS env so `credential_chain` finds them:
```bash
doppler run -p core-x -c prd -- bash -c '
  export AWS_ACCESS_KEY_ID="$R2_ACCESS_KEY_ID" AWS_SECRET_ACCESS_KEY="$R2_SECRET_ACCESS_KEY"
  duckdb -f your_query.sql'
```
(The `lance` python lib is NOT installed in the default env; not needed — this archive is Parquet. `aws s3 ls`, `boto3`, `pyarrow`, `duckdb` 1.5.2 are available.)

---

## 2. What this asset is (inferred lineage)

The dex-db was a **staffing-agency GTM/prospecting engine**. The table topology reads as a pipeline:

```
clay_find_companies         ── raw company discovery + LinkedIn/Clay enrichment (firmographics live here)
        │
        ├─► target_companies ── the working "target" list (staffing agencies being prospected)
        │        └─► demand_company_target_verticals ── 11-vertical "industries served" classification
        │
clay_find_people            ── raw people discovery at companies (carries job titles)
        └─► target_people   ── resolved + emailed contacts at the target agencies
                 ├─► target_people_emails        ── email per person (~100% coverage)
                 └─► target_people_outreach_tiers ── top-1,000 prioritized for outreach

company_entities / person_entities ── canonical identity-resolution masters (golden records),
                                       but LARGELY UNENRICHED (see §5.2)
```

These ran at **different scales and were never fully reconciled** — hence the partial overlaps in §5.3.

---

## 3. Headline numbers (with grain — this is where it's easy to get confused)

"How many staffing agencies?" depends on which table and whether you count **rows** or **distinct domains**. Reconciled to normalized domain:

| Lens | Source table | Raw count | **Distinct domains** |
|---|---|---|---|
| Broadest — every enriched staffing company | `clay_find_companies` WHERE `industry='Staffing and Recruiting'` | 43,382 rows | **37,300** |
| Classified — has "industries served" | `target_companies` ∩ `demand_company_target_verticals` | 24,398 companies | **24,398** |
| Identity master (thin/unenriched) | `company_entities` (staffing) | 2,838 rows | **2,694** |
| **CANONICAL — deduped union of all three** | — | — | **45,515** |

**Funnel for picking an altitude:** ~45.5k known → 24.4k vertical-classified → ~10k with any contact → ~6.7k with emailed contacts → 1,000 tiered for outreach.

Domain-overlap distribution across the 3 sources: **28,200** domains appear in only 1 source, **15,753** in 2, **1,562** in all 3.

---

## 4. Industries served / specialty classification (the differentiated asset)

`demand_company_target_verticals` — **36,665 tags across 24,398 companies, avg 1.5 verticals each**, controlled vocabulary of **11 staffing verticals**:

| vertical | companies | | vertical | companies |
|---|--:|---|---|--:|
| logistics_and_supply_chain | 8,703 | | construction | 2,349 |
| healthcare_clinical | 6,632 | | facilities_services | 1,981 |
| light_industrial_and_manufacturing | 5,342 | | it_staffing | 1,007 |
| engineering | 4,936 | | skilled_trades | 909 |
| accounting | 3,678 | | aerospace_and_defense | 814 |
| | | | trucking | 314 |

Join: `demand_company_target_verticals.target_company_id → target_companies.id`. Verified example: **Spherion** → {accounting, aerospace_and_defense, engineering, healthcare_clinical, it_staffing, light_industrial, logistics}.

Secondary/narrative specialty signal also lives in `clay_find_companies.description` (free text, e.g. "Nurse Staffing Agency", "tech staffing… Software Development") and `clay_find_companies.raw_payload` (JSON `industries[]` + `derived_datapoints`).

---

## 5. Company tables — reference

### 5.1 `clay_find_companies` — the firmographic layer (richest)
- **495,482 rows** total; **43,382** staffing (37,300 domains).
- Grain: ~1 row per Clay company record (some dup domains).
- Columns: `id`(uuid), `domain`, `name`, `linkedin_url`, `linkedin_company_id`, `industry`, `size`, `country`, `location`, `annual_revenue`, `company_type`, `description`, `clay_company_id`, `total_funding_amount_range_usd`, `raw_payload`(json), `source_provider`, `source_table`, `ingested_at`, `updated_at`.
- **Use for:** size, revenue, description, LinkedIn id, industry label. This is where company firmographics actually are.

### 5.2 `company_entities` — identity skeleton (⚠️ UNENRICHED)
- **45,679 rows** total (39,101 with a domain); **2,838** staffing (2,694 domains).
- Columns exist for enrichment but are **empty in this dump**: `employee_count` **0.1%** populated, `enrichment_confidence` **0.0%**, `icp_fit_verdict` **0.0%** (every row NULL).
- Populated: `org_id`, `entity_id`, `company_id`, `canonical_domain`, `canonical_name`, `industry`, `linkedin_url`, `company_linkedin_id`, `record_version`, `source_providers[]`.
- **Use for:** canonical identity/dedup only. **Do NOT treat as an enriched master** — the firmographics are in `clay_find_companies`, not here.

### 5.3 `target_companies` + verticals — the classified working set
- `target_companies`: **2,679,234 rows** (raw target list). Columns: `id`(uuid), `company_name`, `domain`, `linkedin_url`, `entity_role`, `phone`, `mailing_address`, `source`, timestamps. **No industry column** — classification comes from the verticals table.
- The **24,398** vertical-tagged subset is the staffing-classified set.

**Overlap is real, not a join artifact.** Re-measured the vertical-tagged↔master overlap on domain AND LinkedIn slug (both keys ~100% populated):

| Match key | vertical-tagged (of 24,398) also in `company_entities` |
|---|--:|
| normalized domain | 1,597 |
| LinkedIn company slug | 1,516 |
| **domain OR LinkedIn** | **1,602** |

Adding LinkedIn matching moved overlap by **+5**. Conclusion: the two sets genuinely share only ~1,600 entities — **22,796** vertical-tagged agencies are truly absent from the master; **~1,092** master agencies are truly un-tagged. (Good news: **domain is a reliable merge key** here — it agrees with LinkedIn to within 5 rows across 24k companies.)

---

## 6. People / contacts — reference

**Coverage caveat:** contacts exist for a **minority** of the 45.5k-domain universe. ~6,725 agencies have `target_people`; ~7,389 have `clay_find_people`; these overlap, so on the order of **~10k distinct agencies have ≥1 person — the other ~35k have none** in this archive.

### 6.1 `target_people` — resolved, emailed contacts (best for outreach)
- **30,578 rows**; **29,563 (96.7%)** are at vertical-tagged staffing agencies, across **6,725 agencies** (~4.4 people/agency).
- Columns: `id`(uuid), `full_name`, `first_name`, `last_name`, `company_name`, `company_domain`, `person_linkedin_url`, `business_concept`(free text), `clay_find_person_id`(→ `clay_find_people.id`), `target_company_id`(→ `target_companies.id`), timestamps.
- ⚠️ **No structured job title** — only free-text `business_concept`. Titles must come from `person_entities` or `clay_find_people` (see below).

### 6.2 `target_people_emails` — ~100% email coverage
- **30,574 rows** (≈ 1 per target person). Columns: `id`, `target_person_id`(→ `target_people.id`), `email`, `email_type`, `source`, `created_at`.

### 6.3 `target_people_outreach_tiers` — prioritized top 1,000
- **1,000 rows**. Columns: `id`, `target_person_id`, `tier`(smallint), `rationale`, `model`, `prompt_version`, `input_title`, `input_employee_range`, `input_company_name`, `tiered_at`. (`input_title` is sparsely populated.)

### 6.4 `person_entities` — enriched person master (small, but has titles + phone)
- **2,116 rows**. Columns: `org_id`, `entity_id`, `company_id`, `full_name`, `first/last_name`, `linkedin_url`, **`title`**, **`seniority`**, **`department`**, **`work_email`**, `email_status`, **`phone_e164`**, `contact_confidence`, timestamps.
- **Use for:** the only structured title/seniority/department/phone signal at clean grain (but tiny coverage).

### 6.5 `clay_find_people` (legacy dex) — raw people, carries titles
- **66,618 rows**; **17,604** at staffing-industry companies across **7,389** staffing cos.
- Columns: `id`(uuid), `linkedin_url`, `name`, `first/last_name`, `domain`, `location_name`, `location_country_iso`, `company_table_id`, `company_record_id`, `matched_experience`, **`latest_experience_title`**, `latest_experience_company`, `latest_experience_start_date`, `raw_payload`, `clay_find_company_id`(→ `clay_find_companies.id`), timestamps.
- Upstream raw layer; `target_people.clay_find_person_id` points back into it. **Use for:** titles + LinkedIn at scale (broader than `target_people`).

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

**Merge keys that work:** normalized `domain` (primary, reliable), LinkedIn company slug (`regexp_extract(url,'linkedin.com/company/([^/?#]+)')`, secondary). Surrogate UUIDs (`target_company_id`, `clay_find_company_id`) only join *within* their own table family — they do NOT bridge `clay_find_*` ↔ `target_*` ↔ `company_entities`.

---

## 8. ⚠️ Critical gotchas

1. **Archive ≠ live.** All of the above is frozen Parquet in `archive/dex/entities/` (2026-06-02). It is not indexed, not appended, not the SoR.
2. **NOT the live `gtm.clay_find_people` ingest.** A separate, active edge-api → Postgres stream is landing **millions of people** (board members / general contacts at arbitrary companies) into `gtm.clay_find_people`. That cohort is unrelated to these staffing agencies. Different table, different system, different purpose.
3. **`company_entities` is unenriched** — identity skeleton only. Firmographics are in `clay_find_companies`.
4. **The 3 company tables are mostly-disjoint workflows**, not a funnel (§5.3). Union them; don't assume nesting.
5. **Titles are thin** on `target_people` (free-text `business_concept`); structured titles only in `person_entities` (2,116) and `clay_find_people.latest_experience_title`.
6. **Emails decay.** This snapshot is months-stale relative to any send. Re-verify before outreach — the same dex schema historically used `millionverifier_*` tables for exactly this.
7. **Most agencies have no people** (~35k of 45.5k domains). Contact coverage is a known gap, not a bug.

---

## 9. Materialized master + remaining work

**✅ BUILT — `s3://data-sink/active/staffing_agencies`** (Lance, **24,398 rows**, 2026-06-07). One row per vertical-tagged staffing agency; the canonical company layer. Query it directly — do **not** re-derive from the archive.

Schema (16 cols): `target_company_id · company_name · domain · website · domain_norm · company_linkedin_url · linkedin_slug · industries_served · employee_band · employee_band_source · revenue_range · revenue_source · country · year_founded · pdl_company_id · firmo_source`.
Indices: `BTREE(domain_norm)`, `BTREE(company_linkedin_url)`, `BITMAP(employee_band)`, `BITMAP(firmo_source)`.
Provenance: `firmo_source` = `in_clay` (16,312) · `pdl` (7,242) · `none` (844); `revenue_range` is clay-sourced (15,099 populated, `revenue_source='clay'`); `employee_band` unifies clay `size` ⊕ PDL `employee_size_range` (`employee_band_source` says which).

```python
import lance
ds = lance.dataset("s3://data-sink/active/staffing_agencies", storage_options=R2_SO)  # R2_SO per §1.1
ds.scanner(filter="employee_band IN ('1-10','11-50') AND firmo_source='in_clay'").to_table()
```

**Remaining work (NOT yet built):**
1. **Contacts layer** — attach `target_people` (+ `target_people_emails`, `outreach_tiers`) and `clay_find_people` (titles) to these agencies; dedup people by `person_linkedin_url`. Only ~10k of 24,398 have any contact today (§6).
2. **Firmographic gaps** — the 844 `firmo_source='none'` (and ~846 `employee_band='unknown'`) agencies need enrichment. They already carry domain + company LinkedIn URL → feed **Blitz Workflow B** (`enrichment-blitz-enrich-linkedin`), which lands firmographics in the `firmographics_blitz` Lance SoR.
3. **Re-verify emails** before any send (archive contacts are stale since 2026-06-02).

**Build provenance:** assembled via DuckDB-over-R2 (dex archive source) ⊕ the live `active/pdl_normalized_companies` match (BTREE on `normalized_domain`/`linkedin_slug`), streamed to Lance via Arrow.

---

## 10. Source queries

All numbers in this doc were produced via DuckDB-over-R2 against the Parquet archive (recipe in §1.1). Reproduce/extend by reading the relevant `s3://data-sink/archive/dex/entities/<table>.parquet` and filtering `industry` for staffing (`ILIKE '%staff%' OR '%recruit%' OR '%employment%' OR '%human resource%' OR '%executive search%'`) or joining through `demand_company_target_verticals`.
