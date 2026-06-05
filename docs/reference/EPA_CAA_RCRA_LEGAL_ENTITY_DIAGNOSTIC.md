# EPA CAA (Air) & RCRA (Hazardous Waste) Legal Entities — Orphaned-Name Diagnostic

Probe of the two remaining high-value EPA regulatory archives — `ICIS-AIR_downloads.zip` (Clean Air
Act) and `rcra_downloads.zip` (RCRAInfo hazardous waste) — to determine whether their primary
legal/operating **corporate names** were orphaned in landing the same way the NPDES permit /
enforcement-defendant names were (see `EPA_LEGAL_ENTITY_MATERIALIZATION_PLAN.md`, PR #132). Verdict:
**both are, RCRA totally and CAA nearly so.**

**Provenance / attestation.** Every figure is a **live, read-only read of R2**. Harness: `boto3`
central-directory random-access — a **1 MB partial-deflate header scan** of every CSV member to
locate the name table (no full extraction), then full `_member_to_gz` extract of the two name nodes;
`duckdb 1.5.3` `read_csv(all_varchar=true, parallel=false)` for counts/fill/grain; `pylance 7.0.0`
direct read of the **live** `epa_program_links`, `epa_pipeline_caa`, `epa_pipeline_rcra` (Arrow schema
+ `count_rows` + projected scans). Reached under `doppler run -p core-x -c prd`. **No DDL, no Lance
write, no migration executed.** Landing archives written **2026-06-03 00:43–00:49 UTC**; audit date
**2026-06-05**.

---

## 0. Headline posture

| Program | Name table (in raw ZIP) | Name col / fill | Active dataset | Name in active SoR? | Verdict |
|---|---|---|---|---|---|
| **CAA (Air)** | `ICIS-AIR_downloads.zip::ICIS-AIR_FACILITIES.csv` | `FACILITY_NAME` **100.00%** (279,211) | `epa_pipeline_caa` | ⚠️ **`AIR_NAME` present on a 19,687-entity enforcement slice only** | **Orphaned for 92.6% of the air universe.** 245,817 / 265,489 air `REGISTRY_ID`s have **no** name node in active Lance. |
| **RCRA (HazWaste)** | `rcra_downloads.zip::RCRA_FACILITIES.csv` | `FACILITY_NAME` **100.00%** (1,597,673) | `epa_pipeline_rcra` | 🛑 **NONE — zero `*_NAME` columns** | **Totally orphaned.** All 1,597,673 handler names (→1,476,583 `REGISTRY_ID`s) live only in the raw ZIP. |

Both bind cleanly to the universal `REGISTRY_ID` hub — **CAA directly (inline), RCRA via the already-materialized `epa_program_links`** — so remediation is unblocked.

---

## 1. CAA — `ICIS-AIR_FACILITIES.csv` (the air name node)

**Sole name-bearing table in `ICIS-AIR_downloads.zip`.** The other 9 members
(`ICIS-AIR_PROGRAMS`, `_POLLUTANTS`, `_PROGRAM_SUBPARTS`, `_FCES_PCES`, `_STACK_TESTS`,
`_TITLEV_CERTS`, `_FORMAL_ACTIONS`, `_INFORMAL_ACTIONS`, `_VIOLATION_HISTORY`) carry **no name** —
all key on `PGM_SYS_ID` (+`ACTIVITY_ID`).

- **Rows: 279,211 · Columns: 19 · Grain: `PGM_SYS_ID` UNIQUE** (max 1 row/id) — one row per air facility.
- **Name:** `FACILITY_NAME` **279,211 / 100.00%**.
- **Hub key:** **`REGISTRY_ID` is INLINE — 278,944 / 99.90%**, distinct **265,489**. No `program_links` hop needed.

| Column | Fill | Role |
|---|--:|---|
| `PGM_SYS_ID` | 100.00% | **ICIS-AIR program id (PK) → all ICIS-AIR_* program/enforcement tables** · BTREE |
| `REGISTRY_ID` | 99.90% | **universal hub key — inline, direct** · BTREE |
| `FACILITY_NAME` | 100.00% | **CAA legal/operating name — payload** |
| `STREET_ADDRESS`,`CITY`,`COUNTY_NAME`,`STATE`,`ZIP_CODE`,`EPA_REGION` | 100.00% | facility address (no geocode in this table) |
| `NAICS_CODES` / `SIC_CODES` | 100.00% / 76.84% | industry |
| `FACILITY_TYPE_CODE` | 83.09% | facility class |
| `AIR_POLLUTANT_CLASS_CODE`/`_DESC` | 94.49% | major/minor/synthetic-minor class · BITMAP |
| `AIR_OPERATING_STATUS_CODE`/`_DESC` | 95.53% | operating/permanently-closed · BITMAP |
| `CURRENT_HPV` | 100.00% | high-priority-violator flag · BITMAP |
| `LOCAL_CONTROL_REGION_CODE`/`_NAME` | 4.18% | local district |

**Bind path — DIRECT (no join):**
```sql
-- REGISTRY_ID is a column of the row; PGM_SYS_ID ties out to the air program/enforcement tables.
SELECT nullif(trim(REGISTRY_ID),'')   AS REGISTRY_ID,     -- inline hub key
       nullif(trim(FACILITY_NAME),'') AS FACILITY_NAME,   -- CAA operating name
       nullif(trim(PGM_SYS_ID),'')    AS PGM_SYS_ID,      -- ICIS-AIR id
       STREET_ADDRESS, CITY, COUNTY_NAME, STATE, ZIP_CODE, EPA_REGION, NAICS_CODES, …
FROM   "ICIS-AIR_FACILITIES.csv"
WHERE  nullif(trim(REGISTRY_ID),'') IS NOT NULL;          -- 278,944 rows (drops 267 / 0.10%)
```
Corroboration: `epa_program_links` carries `PGM_SYS_ACRNM='AIR'` with **279,103** links — matches the
279,211-facility universe, confirming `PGM_SYS_ID` is the AIR program id. The inline `REGISTRY_ID` is
the primary (and simpler) bind.

### 1.1 Active coverage — `epa_pipeline_caa` (live schema)

- **66,655 rows · 35 cols · `*_NAME` present: `AIR_NAME` (100% fill) · distinct `REGISTRY_ID`: 19,687.**
- Built from `pipeline_caa_downloads.zip::PIPELINE_CAA_00_COMPLETE.csv` — the **CAA enforcement/compliance
  pipeline** (eval→violation→action timeline), **not** the facility master. `AIR_NAME` is denormalized
  onto each event row but spans **only 19,687 distinct facilities**.
- **Coverage gap: 245,817 of 265,489 air `REGISTRY_ID`s (92.6%) are absent from `epa_pipeline_caa`** —
  i.e. 92.6% of the air-permitted universe's legal names exist **only** in the raw ZIP.

> **Definitive (CAA):** the operating name is **not** absent outright, but it is present for only the
> **7.4% enforcement-pipeline slice**. The full air-facility legal-name node
> (`ICIS-AIR_FACILITIES.FACILITY_NAME`, 279,211 rows, `REGISTRY_ID` inline) is **orphaned**.

---

## 2. RCRA — `RCRA_FACILITIES.csv` (the hazardous-waste name node)

**Sole name-bearing table in `rcra_downloads.zip`.** The other 5 members (`RCRA_ENFORCEMENTS`,
`_EVALUATIONS`, `_VIOLATIONS`, `_VIOSNC_HISTORY`, `_NAICS`) carry **no name** — all key on
`ID_NUMBER`+`ACTIVITY_LOCATION`.

- **Rows: 1,597,673 · Columns: 15 · Grain: `ID_NUMBER` UNIQUE** (max 1 row/id; distinct
  (`ID_NUMBER`,`ACTIVITY_LOCATION`) = row count) — one row per EPA hazardous-waste handler.
- **Name:** `FACILITY_NAME` **1,597,673 / 100.00%**.
- **Hub key: NOT inline.** Keyed on **`ID_NUMBER`** (EPA Handler / RCRA ID, e.g. `TXD000…`).

| Column | Fill | Role |
|---|--:|---|
| `ID_NUMBER` | 100.00% | **EPA Handler ID (PK) → `epa_program_links`(RCRAINFO) & all RCRA_* tables** · BTREE |
| `FACILITY_NAME` | 100.00% | **RCRA legal/operating name — payload** |
| `ACTIVITY_LOCATION` | 100.00% | state-activity qualifier (part of native key) |
| `STREET_ADDRESS`/`CITY_NAME`/`STATE_CODE`/`ZIP_CODE` | 99.92 / 99.95 / 100.0 / 99.94% | handler address |
| `LATITUDE83`/`LONGITUDE83` | 93.32% | geocode (present, unlike the air table) |
| `FED_WASTE_GENERATOR` | 99.43% | LQG/SQG/VSQG generator class · BITMAP |
| `TRANSPORTER`,`ACTIVE_SITE`,`OPERATING_TSDF` | ~100% | role / status flags · BITMAP |
| `FULL_ENFORCEMENT`,`HREPORT_UNIVERSE_RECORD` | 100.00% | universe flags |

**Bind path — VIA `epa_program_links` (RCRAINFO), already materialized:**
```sql
-- ID_NUMBER == program_links.PGM_SYS_ID for PGM_SYS_ACRNM='RCRAINFO'; link is 1:1 → no fan-out.
SELECT pl.REGISTRY_ID                  AS REGISTRY_ID,    -- hub key (resolved)
       nullif(trim(r.FACILITY_NAME),'')AS FACILITY_NAME, -- RCRA handler name
       nullif(trim(r.ID_NUMBER),'')    AS HANDLER_ID,     -- EPA Handler / RCRA ID
       r.STREET_ADDRESS, r.CITY_NAME, r.STATE_CODE, r.ZIP_CODE, r.LATITUDE83, r.LONGITUDE83, …
FROM   "RCRA_FACILITIES.csv" r
JOIN  (SELECT DISTINCT PGM_SYS_ID, REGISTRY_ID
       FROM epa_program_links WHERE PGM_SYS_ACRNM='RCRAINFO') pl
  ON   pl.PGM_SYS_ID = r.ID_NUMBER;                       -- 1,578,504 rows
```

**Measured resolution (live `epa_program_links`):**

| Metric | Value |
|---|--:|
| RCRAINFO links (distinct `PGM_SYS_ID` = distinct (pgm,rid)) | 1,578,620 (1:1 — clean, no fan-out) |
| RCRA handler rows → resolvable `REGISTRY_ID` | **1,578,504 / 98.80%** |
| Distinct `REGISTRY_ID` reached | **1,476,583** |
| Named RCRA rows with `REGISTRY_ID` | 1,578,504 (name is 100%) |
| Handlers with no RCRAINFO link (orphan, INNER drops) | ~19,169 (1.20%) |

### 2.1 Active coverage — `epa_pipeline_rcra` (live schema)

- **456,773 rows · 30 cols · `*_NAME` present: `NONE` · distinct `REGISTRY_ID`: 90,169.**
- Columns are entirely ISN / eval / violation / enforcement / `CASE_ID` / `REGISTRY_ID` / `SOURCE_ID` /
  penalties / dates — built from `pipeline_rcra_downloads.zip::PIPELINE_RCRA_00_COMPLETE.csv`. **No
  handler name field exists anywhere in the dataset.**

> **Definitive (RCRA):** the handler legal name is **entirely absent** from the active SoR. Even the
> 90,169-`REGISTRY_ID` enforcement subset in `epa_pipeline_rcra` is nameless. All 1,597,673 handler
> names — binding to 1,476,583 `REGISTRY_ID`s — exist **only** inside `rcra_downloads.zip`.

---

## 3. Consolidated structural finding

| | CAA | RCRA |
|---|---|---|
| Name table (raw ZIP) | `ICIS-AIR_FACILITIES.csv` | `RCRA_FACILITIES.csv` |
| Name column / fill | `FACILITY_NAME` / 100.00% | `FACILITY_NAME` / 100.00% |
| Source rows | 279,211 | 1,597,673 |
| Native PK | `PGM_SYS_ID` (unique) | `ID_NUMBER` (unique) |
| `REGISTRY_ID` bind | **inline, direct** (99.90%) | **via `epa_program_links` RCRAINFO** (98.80%) |
| Distinct `REGISTRY_ID` reachable | 265,489 | 1,476,583 |
| Active dataset | `epa_pipeline_caa` | `epa_pipeline_rcra` |
| Name in active SoR | `AIR_NAME`, **7.4% slice only** | **NONE** |
| Names orphaned | **245,817 REGISTRY_IDs (92.6%)** | **all (100%)** |

The pattern is identical to the NPDES gap (PR #132): the **flattened compliance "pipeline" datasets**
(`epa_pipeline_caa`/`_rcra`) were materialized, but the **facility/handler master tables that carry the
legal name** (`ICIS-AIR_FACILITIES`, `RCRA_FACILITIES`) were left in landing. Remediation = two net-new
`REGISTRY_ID`-keyed name nodes, exactly parallel to `epa_permits` / `epa_defendants`:

- **`epa_air_facilities`** ← `ICIS-AIR_FACILITIES` (direct `REGISTRY_ID`); BTREE `REGISTRY_ID`,`PGM_SYS_ID`;
  BITMAP `STATE`,`AIR_OPERATING_STATUS_CODE`,`CURRENT_HPV`,`AIR_POLLUTANT_CLASS_CODE`. ~278,944 rows.
- **`epa_rcra_handlers`** ← `RCRA_FACILITIES` ⟕ `epa_program_links`(RCRAINFO); BTREE `REGISTRY_ID`,`HANDLER_ID`;
  BITMAP `STATE_CODE`,`ACTIVE_SITE`,`OPERATING_TSDF`,`FED_WASTE_GENERATOR`. ~1,578,504 rows.

**Materialization pipeline intentionally not written** — diagnostic + structural join paths only, per directive.
