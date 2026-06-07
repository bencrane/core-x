# CMS Medicare Archive — Layout, Grain & Schema-Drift Diagnostic

**Status:** reconnaissance only — zero ingest, zero extraction, zero rows written to Lance.
**Source of record probed:** `s3://data-sink/landing/cms/medicare-datasets/` (Cloudflare R2, Gen-3 landing tier).
**Probe:** [`scripts/recon_medicare_archive.py`](../scripts/recon_medicare_archive.py) — reproducible via
`doppler run -p core-x -c prd -- python3 scripts/recon_medicare_archive.py > /tmp/medicare_recon.json`.
**Committed evidence:** the full per-member schema / grain / drift ground truth this report rests on is checked in at
[`docs/reference/medicare_archive_recon_evidence.md`](reference/medicare_archive_recon_evidence.md) (distilled from
the probe output by `scripts/recon_medicare_evidence.py`). **No claim here depends on an out-of-repo file** — the
`/tmp` path above is only the raw intermediate the generator consumes.
**Method (anti-OOM, strict):** each ZIP's central directory was read from the object **tail** via a handful of
HTTP Range GETs (`_S3RangeReader` → stdlib `zipfile`); each data member was characterised by
**stream-decompressing only its leading deflate blocks** — capped at 50 rows / 1 MiB decompressed, whichever
came first. **No object was downloaded in full; no member was decompressed in full; `pandas.read_csv` was
never invoked.** The full **89.78 GB logical corpus** was mapped while transferring on the order of a few
hundred MB of prefix bytes. Single-pass head sampling — row-level claims (summary rows, NPI validity) describe
the **first 50 rows** of each file and are stated as such; the recommended ingest-time guards do not rely on them.

> **Post-review correction (Opus-4.8 architectural review, verified against the committed recon evidence).** Several §3/§4 claims
> below were corrected: **DME C1/C2 have additive drift** (72→97, 68→93 at 2017) — not "stable"; **A1 56-col is a
> name-subset of 81-col but NOT a positional prefix** (cols inserted at index 55, displacing `Bene_Avg_Risk_Scre`);
> **R1 Provider Enrollment is a five-table relational archive**, not one flat file; **QPP drift is severe**
> (92→165→204→212); money totals need **`DECIMAL(18,2)`**; **`program_year` indexes as `BITMAP`**. The review also
> **disproved the NPPES→EIN→Form 5500 bridge** (NPPES redacts EIN to a constant `<UNAVAIL>` sentinel). The forward
> ingest specification is [`docs/plans/medicare_ingestion_plan.md`](plans/medicare_ingestion_plan.md).

---

## §1 — Inventory Summary

| Metric | Value |
|---|---|
| Objects under prefix | **36** (35 `.zip` + 1 standalone `.pdf`) |
| Compressed footprint | **17.03 GB** |
| Uncompressed (logical) footprint | **89.78 GB** (≈ **5.3×** expansion) |
| Data members (`.csv`) across all zips | **118** |
| Documentation / dictionary members (`.pdf` / `.xlsx`) | **398** |
| Dialect (every data file) | comma-delimited, RFC-4180 quoting, **UTF-8 (BOM)** |
| ZIP64 | not required (no single member ≥ 4 GB compressed; largest uncompressed member 4.06 GB) |

### 1.1 Dataset families (8 distinct datasets + reference)

The prefix is **not one dataset** — it is the CMS provider-disclosure suite. Eight payload families plus
reference/crosswalk assets, each with its own grain and vocabulary:

| # | Family / sub-grain | Hub key | Years | Zips | Cadence |
|---|---|---|---|---|---|
| **A1** | Physician & Other Practitioners — **by Provider** | `Rndrng_NPI` | **2013–2024** (12) | 1 bundle | annual files, one bundle |
| **A2** | Physician & Other Practitioners — **by Provider and Service** | `Rndrng_NPI` | **2017–2024** (8) | 8 annual | one zip / year |
| **A3** | Physician & Other Practitioners — by Geography and Service | *(none — geo)* | 2013–2024 (12) | 1 bundle | annual files, one bundle |
| **B1** | Part D Prescribers — **by Provider** | `Prscrbr_NPI` | **2013–2020** (8) | 1 bundle | annual files, one bundle |
| **B2** | Part D Prescribers — **by Provider and Drug** | `Prscrbr_NPI` | **2013–2024** (13) | 13 annual | one zip / year |
| **B3** | Part D Prescribers — by Geography and Drug | *(none — geo)* | 2014–2024 (11) | 1 bundle | annual files, one bundle |
| **C1** | DME — **by Referring Provider** | `Rfrg_NPI` | 2014–2023 (10) | 1 bundle | annual files, one bundle |
| **C2** | DME — **by Supplier** | `Suplr_NPI` | 2014–2023 (10) | 1 bundle | annual files, one bundle |
| **C3** | DME — **by Supplier and Service** | `Suplr_NPI` | 2014–2023 (10) | 1 bundle | annual files, one bundle |
| **C4** | DME — by Geography and Service | *(none — geo)* | 2017–2023 (7) | 1 bundle | annual files, one bundle |
| **R1** | FFS Public Provider Enrollment | `NPI` | snapshot 2026-04-01 | 1 | point-in-time |
| **R2** | Quality Payment Program (MIPS) Experience | `npi` | 2017–2024 (8) | 1 bundle | annual files, one bundle |
| **R3** | Restructured BETOS (RBCS) taxonomy | *(HCPCS crosswalk)* | RY2025 | 1 | reference |
| **R4** | CMS Program Statistics (Physician/NPP/Supplier) | *(workbook)* | — | 2 (v1,v2) | reference (xlsx-only) |
| **R5** | ACO REACH Financial & Quality Results | *(ACO-level)* | 2021–2023 | 1 | reference |

> **Directive target.** The commercial signals in the directive — **Total Submitted Charges, Total Medicare
> Payment Amount, Total Services, Specialty, ZIP** — exist natively, as *totals*, **only in family A1
> (Physician & Other Practitioners — by Provider)**, which is also the only series spanning the full decade+
> (2013–2024). **A1 is the reconciled-mirror target.** A2 is its finer NPI×HCPCS companion (financials there are
> *averages*, not totals — see §4.2). All other families are distinct mirrors, catalogued here but **not** unioned
> into A1.

### 1.2 Per-zip physical inventory

`u` = uncompressed. Grain column is the **authoritative** read (see §2; corrects the probe's count-column false positives).

| Zip | Family | Year(s) | Comp MB | u GB | Members (data/doc) | Hub | Grain |
|---|---|---|---:|---:|---|---|---|
| `…by Provider and Service` ×8 | A2 | 2017–2024 | 477–505 | 2.99–3.25 | 1 / 3–5 | `Rndrng_NPI` | NPI × HCPCS × POS |
| `Medicare …by Provider.zip` | A1 | 2013–2024 | 1 618 | 5.14 | 12 / 36 | `Rndrng_NPI` | **NPI (per year)** |
| `…by Geography and Service.zip` | A3 | 2013–2024 | 159 | 0.51 | 12 / 41 | — | Geo × HCPCS × POS |
| `2013-2020 …Part D …by Provider.zip` | B1 | 2013–2020 | 1 360 | 3.94 | 8 / 32 | `Prscrbr_NPI` | **NPI (per year)** |
| `…Part D …by Provider and Drug` ×13 | B2 | 2013–2024 | 590–716 | 3.38–4.06 | 1 / 4 | `Prscrbr_NPI` | NPI × (Brnd,Gnrc) |
| `…Part D …by Geography and Drug.zip` | B3 | 2014–2024 | 61 | 0.16 | 11 / 39 | — | Geo × (Brnd,Gnrc) |
| `…DME …by Referring Provider.zip` | C1 | 2014–2023 | 501 | 1.62 | 10 / 30 | `Rfrg_NPI` | **NPI (per year)** |
| `…DME …by Supplier.zip` | C2 | 2014–2023 | 120 | 0.36 | 10 / 30 | `Suplr_NPI` | **NPI (per year)** |
| `…DME …by Supplier and Service.zip` | C3 | 2014–2023 | 313 | 2.38 | 10 / 20 | `Suplr_NPI` | NPI × HCPCS |
| `…DME …by Geography and Service.zip` | C4 | 2017–2023 | 15 | 0.07 | 7 / 14 | — | Geo × HCPCS |
| `…Public Provider Enrollment.zip` | R1 | 2026-04-01 | 131 | 0.52 | 5 / 3 | `NPI` | NPI enrollment (crosswalk) |
| `Quality Payment Program Experience.zip` | R2 | 2017–2024 | 528 | 3.01 | 8 / 16 | `npi` | NPI (MIPS, per year) |
| `Restructured BETOS …System.zip` | R3 | RY2025 | 1 | <0.01 | 1 / 2 | — | HCPCS taxonomy (crosswalk) |
| `CMS Program Statistics …v1/v2.zip` | R4 | — | 5 / 4 | <0.01 | 0 / 22–20 | — | xlsx workbooks only |
| `ACO REACH …Results.zip` | R5 | 2021–2023 | 1 | <0.01 | 3 / 6 | — | ACO-level |

### 1.3 Duplicates & anomalies (dedup before ingest)

- **Exact duplicate:** `2013-2013-Medicare Part D Prescribers - by Provider and Drug-2.zip` is byte-identical in
  size (589.8 MB) to `…-by Provider and Drug.zip` and resolves to the same `(family, sub-grain, year)`.
  **Keep one; drop the `-2`.** (The `-2` suffix on the 2021/2022/2023 Part-D-by-drug zips is **not** a duplicate —
  those are the *only* copy for those years; the suffix is a download artifact.)
- **Versioned reference:** Program Statistics ships as `-v1` and `-v2` of the same workbook set — treat **v2** as
  canonical, v1 as superseded.
- **Part D bundle overlap:** the **B1 bundle (2013–2020)** and the **B2 annual files (2013–2024)** are *different
  grains* (provider-level totals vs provider×drug detail) — not duplicates; both are kept, in separate mirrors.
- **Nested template zip:** A2 year folders contain a tiny `MUP_PHY_Ryy_P04_V10_Dyy_Prov_Svc.zip` (a CMS naming
  *template* placeholder, classified `other`). Ignore it.
- **Standalone:** `CMS_AMA_CPT_license_agreement.pdf` (19 KB) — the AMA CPT license, not data.

### 1.4 Documentation / dictionary payloads (per year folder)

Every payload zip co-packages its glossary. Distinguishing primary from documentation is unambiguous by size and
extension (data CSV is 300 MB–4 GB uncompressed; docs are < 1 MB):

- **Data dictionary (column definitions):** `*_DD_*.pdf` — e.g. `MUP_PHY_RY25_20250312_DD_PRV_SVC_508.pdf`,
  `MUP_DPR_RY25_20250401_DD_PRV_508.pdf`. The 2013–2016 physician block ships `DD_Prvdr_2013_2016_508.pdf`
  (a distinct, narrower dictionary — consistent with the 56-col variant in §3).
- **Methodology:** `*_Methodology_508.pdf`. **Technical specs:** `*_Technical Specifications.pdf` (A2).
- **High-level / drug-level summary aggregates:** `*_HLSum*.xlsx` / `*_DLSum*.xlsx` (Part D), `*_HCCs_*.xlsx`
  (physician HCC supplements). These are **pre-aggregated** workbooks — **not** to be confused with row-level data.

---

## §2 — The Grain

> **Definitive statement.** There is **no single grain** across the prefix. There are **four distinct grains**,
> and conflating them is the primary corruption risk. Within the directive's target (family A1):
> **one row = one rendering NPI, for one calendar year** (the year is encoded by the file's folder, not a column).

| Grain class | Families | One row = | Code/aggregate axis | Empirical evidence (head-50) |
|---|---|---|---|---|
| **Provider-level annual** | A1, B1, C1, C2, R2, R1 | **1 NPI × 1 year** | none (HCPCS appears only as *counts*, e.g. `Tot_HCPCS_Cds`) | NPI 1:1 across sampled rows |
| **Provider × service detail** | A2, C3 | **1 NPI × 1 HCPCS × 1 place-of-service × 1 year** | `HCPCS_Cd` | NPI repeats (4–7 distinct NPIs / 50 rows) |
| **Provider × drug detail** | B2 | **1 NPI × 1 brand/generic drug × 1 year** | `Brnd_Name` + `Gnrc_Name` | NPI repeats across sample |
| **Geographic aggregate** | A3, B3, B4/C4 | **1 geography × 1 code × 1 year**; geo ∈ {National, State, County} | `*_Geo_Lvl` | **no NPI column at all** |

**Probe-heuristic correction.** The recon's grain guesser flagged A1/C1/C2 as "NPI × code" because it
pattern-matched `HCPCS` inside the **count** columns `Tot_HCPCS_Cds` / `Tot_Suplr_HCPCS_Cds`. Those are scalar
counts of distinct codes billed by the provider that year — **not** a row-multiplying axis. Authoritative grain for
A1/B1/C1/C2 is **provider-level annual** (confirmed: distinct-NPI == row-count across every sampled head).

### 2.1 The hub-key drift — the single most important cross-family finding

The NPI hub column is named **six different ways** across the suite. Any reconciliation must alias all to one
canonical `npi`:

| Column as published | Families |
|---|---|
| `Rndrng_NPI` | A1, A2 (physician, "rendering") |
| `Prscrbr_NPI` | B1, B2 (Part D, "prescriber") |
| `Rfrg_NPI` | C1 (DME, "referring") |
| `Suplr_NPI` | C2, C3 (DME, "supplier") |
| `NPI` | R1 (enrollment) |
| `npi` (lowercase, space-laden header) | R2 (QPP) |
| *(absent)* | A3, B3, C4 (geography — different grain) |

### 2.2 Summary / "National aggregation" rows — located and contained

The directive's hazard is **real but localized**, and not where the framing assumed:

- **The NPI-keyed payload files (A1, A2, B1, B2, C1, C2, C3) contain no aggregate rows in-sample** — every sampled
  NPI is a valid 10-digit identifier; zero blank/placeholder/"Total"/"National" rows in the first 50 rows of any of them.
- **The aggregate data lives in physically separate files** — the **by-Geography** families (A3, B3, C4), where the
  first column is `*_Geo_Lvl` and the literal value **`National`** is a *first-class geography level* (alongside
  `State` and `County`), e.g. `['National','','National','Drugs Administered Through DME']`. These files have **no
  NPI column** — they can never be NPI-joined and must never be unioned into an NPI mirror.

The corruption guard is therefore **grain separation**, plus defense-in-depth filters at ingest (§4.3). Because the
sample is head-only, the ingest still enforces an NPI-shape predicate rather than trusting the sample.

---

## §3 — Schema Drift Matrix

Two independent drift axes: **(3.1) cross-family name divergence** (large — the same concept is named differently
per family) and **(3.2) within-series year-over-year drift** (small — CMS re-published the entire history under one
current dictionary, so names are stable within a series and only *column count* moves).

### 3.1 Cross-family signal matrix (directive's required fields × family)

Exact published column names for each commercial signal. `Avg_*` = per-service average; `Tot_*` = provider-year total.

| Signal | A1 by-Provider | A2 by-Prov+Service | B1/B2 Part D | C1 DME-Referring | C2/C3 DME-Supplier |
|---|---|---|---|---|---|
| **NPI (hub)** | `Rndrng_NPI` | `Rndrng_NPI` | `Prscrbr_NPI` | `Rfrg_NPI` | `Suplr_NPI` |
| **Last/Org name** | `Rndrng_Prvdr_Last_Org_Name` | `Rndrng_Prvdr_Last_Org_Name` | `Prscrbr_Last_Org_Name` | `Rfrg_Prvdr_Last_Name_Org` | `Suplr_Prvdr_Last_Name_Org` |
| **First name** | `Rndrng_Prvdr_First_Name` | `Rndrng_Prvdr_First_Name` | `Prscrbr_First_Name` | `Rfrg_Prvdr_First_Name` | `Suplr_Prvdr_First_Name` |
| **Entity (org/indiv)** | `Rndrng_Prvdr_Ent_Cd` | `Rndrng_Prvdr_Ent_Cd` | `Prscrbr_Ent_Cd` (B1 only) | `Rfrg_Prvdr_Ent_Cd` | `Suplr_Prvdr_Ent_Cd` |
| **Specialty / type** | `Rndrng_Prvdr_Type` | `Rndrng_Prvdr_Type` | `Prscrbr_Type` | `Rfrg_Prvdr_Spclty_Desc` | `Suplr_Prvdr_Spclty_Desc` |
| **ZIP** | `Rndrng_Prvdr_Zip5` | `Rndrng_Prvdr_Zip5` | `Prscrbr_zip5` *(lc)* / — (B2) | `Rfrg_Prvdr_Zip5` | `Suplr_Prvdr_Zip5` |
| **State** | `Rndrng_Prvdr_State_Abrvtn` | `Rndrng_Prvdr_State_Abrvtn` | `Prscrbr_State_Abrvtn` | `Rfrg_Prvdr_State_Abrvtn` | `Suplr_Prvdr_State_Abrvtn` |
| **Total submitted charges** | **`Tot_Sbmtd_Chrg`** | `Avg_Sbmtd_Chrg` ⚠ avg | — (drug cost, not charges) | `Suplr_Sbmtd_Chrgs` | `Suplr_Sbmtd_Chrgs` (C2) / `Avg_Suplr_Sbmtd_Chrg` (C3) |
| **Total Medicare payment** | **`Tot_Mdcr_Pymt_Amt`** | `Avg_Mdcr_Pymt_Amt` ⚠ avg | — | `Suplr_Mdcr_Pymt_Amt` | `Suplr_Mdcr_Pymt_Amt` / `Avg_Suplr_Mdcr_Pymt_Amt` |
| **Total Medicare allowed** | `Tot_Mdcr_Alowd_Amt` | `Avg_Mdcr_Alowd_Amt` ⚠ avg | — | `Suplr_Mdcr_Alowd_Amt` | `Suplr_Mdcr_Alowd_Amt` / `Avg_…` |
| **Total services** | **`Tot_Srvcs`** | `Tot_Srvcs` | — | `Tot_Suplr_Srvcs` | `Tot_Suplr_Srvcs` |
| **Total claims** | — (n/a) | — | **`Tot_Clms`** | `Tot_Suplr_Clms` | `Tot_Suplr_Clms` |
| **Total beneficiaries** | `Tot_Benes` | `Tot_Benes` | `Tot_Benes` | `Tot_Suplr_Benes` | `Tot_Suplr_Benes` |
| **Drug cost (Part D)** | — | — | **`Tot_Drug_Cst`** | — | — |
| **Code axis** | `Tot_HCPCS_Cds` (count) | `HCPCS_Cd` | `Brnd_Name`,`Gnrc_Name` | (counts) | `HCPCS_Cd` (C3) |

⚠ **A2 financials are averages, not totals** — derive a line total as `Avg_* × Tot_Srvcs` (CMS rounds the averages,
so this reconstructs CMS-published totals only approximately). The directive's *totals* are native to **A1**.

### 3.2 Year-over-year drift, per series

| Series | Years | Col count | Drift verdict |
|---|---|---|---|
| **A1 — by Provider** | 2013–2016 | **56** | **ADDITIVE drift →** baseline |
| **A1 — by Provider** | 2017–2024 | **81** | **+25 `Bene_CC_*` chronic-condition columns** (see 3.3) |
| **A2 — by Provider and Service** | 2017–2024 | **28** | **STABLE** — byte-identical header every year |
| **B2 — by Provider and Drug** | 2013–2024 | **22** | **STABLE** — identical every year |
| **B1 — by Provider** | 2013–2020 | **84** | **STABLE** — identical every year |
| **A3 — by Geography and Service** | 2013–2024 | **15** | **STABLE** |
| **C1 — DME by Referring Provider** | 2014–2023 | **72 → 97** | **ADDITIVE** at 2017 (+25 `Bene_CC_*`, same block as A1) |
| **C2 — DME by Supplier** | 2014–2023 | **68 → 93** | **ADDITIVE** at 2017 (+25 `Bene_CC_*`) |
| **C3 — DME by Supplier and Service** | 2014–2023 | **32** | **STABLE** |
| **C4 — DME by Geography and Service** | 2017–2023 | **18** | **STABLE** |
| **R2 — QPP** | 2017–2024 | **92 → 165 → 204 → 212** | **SEVERE additive drift** (92 for 2017–2021; 165 in 2022; 204 in 2023; 212 in 2024) — superset-reconcile by name |

**No legacy-rename drift exists in this archive.** This is the decisive correction to the directive's premise:
CMS **re-published the entire historical series (back to 2013) under the unified current data dictionary**
(every file stamped `RY25`/`RY26` release-year). The classic legacy PUF column churn — `npi` →
`average_Medicare_payment_amt` → `Tot_Mdcr_Pymt_Amt` — is **absent**: 2013 already uses `Rndrng_NPI` /
`Tot_Mdcr_Pymt_Amt`. Drift in this corpus is **additive columns only**, never renames.

### 3.3 A1 (mirror target) — the additive delta at 2017

2013–2016 (56 cols) is a **column-name subset** of 2017–2024 (81 cols) — but **NOT a positional prefix**
(`h81[:56] != h56`): the first positional divergence is at **index 55**, where the 25 columns are *inserted*,
displacing `Bene_Avg_Risk_Scre` to the tail. **Reconciliation must be name-keyed, never positional** — a positional
union would misfile `Bene_Avg_Risk_Scre` under a chronic-condition column. The 25 added columns are inserted between
`Bene_Ndual_Cnt` and `Bene_Avg_Risk_Scre`:

- **11 behavioral-health prevalence** `Bene_CC_BH_*_Pct`: ADHD_OthCD, Alcohol_Drug, Tobacco, Alz_NonAlzdem,
  Anxiety, Bipolar, Mood, Depress, PD, PTSD, Schizo_OthPsy.
- **14 physical-health prevalence** `Bene_CC_PH_*_Pct`: Asthma, Afib, Cancer6, CKD, COPD, Diabetes, HF_NonIHD,
  Hyperlipidemia, Hypertension, IschemicHeart, Osteoporosis, Parkinson, Arthritis, Stroke_TIA.

**All directive signals (NPI, name, specialty, ZIP, `Tot_Sbmtd_Chrg`, `Tot_Mdcr_Pymt_Amt`, `Tot_Srvcs`) are present
and identically named in *both* the 56- and 81-col variants.** The 2013–2016 rows simply carry NULL for the 25
chronic-condition columns under a superset schema.

---

## §4 — Ingestion Pre-requisites

Canonical transforms the future ingest **must** execute. Compute is DuckDB (out-of-core, `all_varchar` read);
the system of record is **append-only Lance** under `s3://data-sink/active/` (one dataset per grain; **never** union
across grains). One CSV staged to local NVMe at a time; member bytes random-accessed from R2 by ZIP local-header
offset (the `_member_to_gz` pattern) — never extract a full zip to disk.

### 4.1 Mirror topology (one Lance dataset per grain — do NOT union)

| Lance dataset (`active/…`) | Source | Grain | Decade+? |
|---|---|---|---|
| **`cms_physician_provider`** ⭐ | **A1** | NPI × year | **2013–2024 ✓ (directive mirror)** |
| `cms_physician_provider_service` | A2 | NPI × HCPCS × POS × year | 2017–2024 |
| `cms_partd_provider` | B1 | NPI × year | 2013–2020 |
| `cms_partd_provider_drug` | B2 | NPI × drug × year | 2013–2024 |
| `cms_dme_referring_provider` | C1 | NPI × year | 2014–2023 |
| `cms_dme_supplier` / `…_supplier_service` | C2 / C3 | NPI × year / × HCPCS | 2014–2023 |
| `cms_*_geography` (3) | A3/B3/C4 | geo × code × year | — |
| `cms_qpp_experience` | R2 | NPI × year | 2017–2024 |
| `cms_provider_enrollment` *(head)* | R1·Enrollment | `ENRLMT_ID` (1/NPI) | — |
| `cms_provider_enrollment_reassignment` | R1·Reassignment | reassign↔receive `ENRLMT_ID` pair (affiliation graph) | — |
| `cms_provider_enrollment_practice` | R1·Practice_Location | `ENRLMT_ID` × location | — |
| `cms_provider_enrollment_specialty` | R1·Secondary_Specialty | `ENRLMT_ID` × specialty | — |
| `cms_provider_enrollment_npi` | R1·Additional_NPIs | `ENRLMT_ID` × NPI | — |
| `ref_rbcs_taxonomy` *(crosswalk)* | R3 | HCPCS | — |

### 4.2 Per-row transforms (the reconciled mirror — `cms_physician_provider`)

1. **Stamp the year.** The year is **not a column** — it is the parent folder (`…/by Provider/2019/…`). Parse it
   from the member path and inject `program_year SMALLINT` as the first projected column. **Without this the
   appended fragments are indistinguishable across years.**
2. **Alias the hub key to canonical `npi VARCHAR`** (`Rndrng_NPI → npi`). **Keep NPI as VARCHAR** — never cast to
   integer (10-digit identifier; numeric cast risks leading-zero / precision loss). Trim whitespace.
3. **snake_case every column** to the unified dictionary names; preserve full fidelity (every source column kept).
4. **Schema-superset reconciliation.** Project against the **81-col superset**; for 2013–2016 (56-col) the 25
   `Bene_CC_*` columns are **absent → emit NULL** (typed), so all years share one Arrow schema before append.
   Lance append requires a stable schema — enforce column order + type from the superset, not per-file.
5. **Typed casts** (rest stay trimmed VARCHAR):
   - Money — `Tot_Sbmtd_Chrg`, `Tot_Mdcr_Alowd_Amt`, `Tot_Mdcr_Pymt_Amt`, `Tot_Mdcr_Stdzd_Amt`, and the
     `Drug_*`/`Med_*` parallels, DME `Suplr_*_Amt`, Part D `Tot_Drug_Cst` → **`DECIMAL(18,2)`** (strip `$`/`,`).
     **Not `DECIMAL(14,2)`** — a large org/lab/dialysis NPI's annual *submitted* charges (gross, pre-adjudication)
     can approach/exceed 10¹²; a `TRY_CAST` overflow silently NULLs exactly the largest, most commercially
     interesting providers. (A2's per-service **averages** are bounded → `DECIMAL(14,2)` is fine there.)
   - Counts — `Tot_Benes`, `Tot_Srvcs`, `Tot_HCPCS_Cds`, all `Bene_*_Cnt` → **`BIGINT`**.
   - Rates/scores — `Bene_CC_*_Pct`, `Bene_Avg_Risk_Scre`, `Bene_Avg_Age` → **`DOUBLE`**.
   - No native date columns in A1 (temporal grain is `program_year`).
6. **Suppression sentinels.** CMS suppresses small cells (the `*_Sprsn_Ind` flags + blanked metrics). Treat
   blank/`*` metric values as **NULL**, and **retain the `_Sprsn_Ind` flag columns** — do not silently coerce
   suppressed-to-zero (zero ≠ suppressed).
7. **A2 only** — financials are averages: carry `Avg_*` as published; if a line total is needed downstream, derive
   `Avg_* × Tot_Srvcs` and label it explicitly as reconstructed.

### 4.3 Grain-integrity & anti-corruption guards

1. **Never union across grains** — A1/A2/B/C/geo are separate Lance datasets. A single mirror = a single grain.
2. **Exclude geography families from any NPI mirror** entirely (they have no NPI). If geography is ingested to its
   own dataset, **either keep `Geo_Lvl` as a partition dimension or filter `Geo_Lvl <> 'National'`** depending on
   whether national roll-ups are wanted — never blend levels into one undisaggregated total.
3. **NPI-shape predicate at ingest** (defense-in-depth, independent of the head sample):
   `WHERE npi ~ '^[0-9]{10}$'` — drops any stray non-NPI/aggregate row anywhere in the file body.
4. **Dedup before append:** drop the `-2` 2013 Part-D-by-drug zip; use Program Statistics **v2**; the B1 bundle and
   B2 annuals are distinct grains (no dedup between them).
5. **Idempotency:** guard appends with an ops ledger keyed on `(dataset, program_year, source_object_etag)` so a
   re-run of a year is a no-op — mirrors the `ops.cms_open_payments_runs` pattern. One year = one append unit;
   blast-radius isolation between years.

### 4.4 Index plan (build once, after all years land — heavy external sort, isolate from appends)

| Dataset | `BTREE` (high-cardinality / resolution) | `BITMAP` (low-cardinality / filter) |
|---|---|---|
| `cms_physician_provider` | `npi` | `program_year`, `Rndrng_Prvdr_Type`, `Rndrng_Prvdr_State_Abrvtn`, `Rndrng_Prvdr_Ent_Cd` |
| `cms_physician_provider_service` | `npi`, `HCPCS_Cd` | `program_year`, `Place_Of_Srvc`, `Rndrng_Prvdr_State_Abrvtn` |
| `cms_partd_provider_drug` | `npi` | `program_year`, `Prscrbr_Type`, `Prscrbr_State_Abrvtn` |

`npi` (and `HCPCS_Cd`/`enrlmt_id` where present) is the load-bearing resolution key → hard `BTREE`;
**`program_year` is low-cardinality (≤13 distinct) → `BITMAP`, not BTREE** (year-filter pushdown is served strictly
better by a bitmap at that NDV). Build indexes only after the per-(dataset, year) candidate-key proof
(`GROUP BY npi … HAVING count(*)>1` = 0) passes — an unexpected duplicate key changes the index semantics.
Configure DuckDB
`temp_directory` to local NVMe and a bounded `memory_limit` for the sort; run index builds in a process isolated
from the append path.

### 4.5 Crosswalk / enrichment assets (ingest as references, not payloads)

- **R1 Provider Enrollment** → `cms_provider_enrollment`: `NPI ↔ PECOS_ASCT_CNTL_ID ↔ ENRLMT_ID` plus
  `PROVIDER_TYPE_*`, name, org, state, `MULTIPLE_NPI_FLAG`. The bridge from disclosure NPIs to PECOS enrollment IDs.
- **R3 RBCS/BETOS** → `ref_rbcs_taxonomy`: `HCPCS_Cd → RBCS category / subcategory / family` (+ validity dates).
  Enriches every HCPCS-grain mirror (A2, C3) with clinical service categories.
- **R4 Program Statistics** is **xlsx-only (zero CSV members)** — out of the DuckDB CSV path; if needed, convert
  the relevant sheet to CSV/Parquet first. Treat as low-priority reference.

---

### Appendix — reproduce

```bash
doppler run -p core-x -c prd -- python3 scripts/recon_medicare_archive.py            # full prefix
doppler run -p core-x -c prd -- python3 scripts/recon_medicare_archive.py "by provider and service"  # one family
```

Required env (Doppler `core-x/prd`): `R2_ENDPOINT` (or `R2_ACCOUNT_ID`), `R2_ACCESS_KEY_ID`,
`R2_SECRET_ACCESS_KEY`. Output: JSON to stdout, human progress to stderr. Read-only end-to-end.
