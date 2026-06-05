# EPA Legal Entities (Permits & Defendants) — Materialization Diagnostic & Build Plan

Remediation spec for an architectural gap in the EPA data plane: the high-value corporate
names **`PERMIT_NAME`** (ICIS-NPDES permittees) and **`DEFENDANT_NAME`** (federal enforcement
defendants) — and their facility addresses — were never materialized into the active LanceDB
SoR. They exist only inside the raw ZIP archives under `s3://data-sink/landing/epa/`, where they
are read **transiently** inside `build_bridge` (`pipelines/ingest_epa/materialize_epa.py`) and
dropped. Downstream entity-resolution bridges cannot random-access them.

This document delivers **Phase 1 (live diagnostic)** and **Phase 2 (schema + build plan)** only.
The Python pipeline is intentionally **not** written here — it is gated on review of this plan.

**Provenance / attestation.** Every figure below is a **live, read-only read of R2**, not a
recon estimate. Harness: `boto3` central-directory random-access extract of single ZIP members
(the `_S3RangeReader` / `_member_to_gz` raw-deflate→gzip technique lifted verbatim from
`materialize_epa.py`; `/tmp` held only the **compressed** member, never the decompressed CSV),
then `duckdb 1.5.3` `read_csv(all_varchar=true, parallel=false)` for row counts, schemas, full
per-column fill, and the join cardinality probes. R2 endpoint reached under
`doppler run -p core-x -c prd`. **No DDL, no Lance write, no migration was executed.**
As-of: landing archives written **2026-06-03 00:39–00:47 UTC**; audit date **2026-06-05**.

---

## 0. Headline posture

| Verdict | Detail |
|---|---|
| **Gap confirmed (both ends)** | 🛑 `active/epa_permits/` and `active/epa_defendants/` **do not exist** in R2 (live `list_objects_v2`, `KeyCount=0`). The four source CSVs are present only inside `npdes_downloads.zip` / `case_downloads.zip`. The sibling event tables they must bind (`epa_npdes_dmrs`, `epa_case_enforcements`) **are** live. |
| **Source integrity** | ✅ All four members extracted clean. `PERMIT_NAME` 97.22% fill, `DEFENDANT_NAME` ~100%, both facility `REGISTRY_ID` sources ≥99.5%. |
| **Permit→REGISTRY_ID join** | ✅ **Clean 1:1.** `ICIS_FACILITIES.NPDES_ID` is unique (max 1 row/NPDES_ID, 0 dupes). **99.53%** of permit rows resolve to a non-null `REGISTRY_ID`. |
| **Defendant→REGISTRY_ID join** | ⚠️ **Many-to-many** (`ACTIVITY_ID` carries ≤852 defendants and ≤860 facilities). No defendant→facility FK exists in ECHO — the join is a per-case cartesian. Contained in practice (311,594 rows), but grain must be declared, not assumed. |
| **What this unlocks** | These are the missing **identity/name nodes**: `epa_npdes_dmrs` (67.6 M rows) keys on `EXTERNAL_PERMIT_NMBR` but carries no name and no `REGISTRY_ID`; `epa_case_enforcements` keys on `ACTIVITY_ID` but reaches `REGISTRY_ID` only through the un-materialized `CASE_FACILITIES`. Both gaps close once these two datasets land. |
| **Independent corroboration** | `docs/reference/EPA_PPP_MAPPING_BLUEPRINT.md` (audited 2026-06-05) §1.1 + Rec #5 names this exact gap: *"`CASE_FACILITIES` … is not materialized as a standing dataset (it is read transiently inside the bridge build)."* |

---

# PHASE 1 — Diagnostic & Verification

## 1.1 Landing footprint (live `boto3` listing)

Both archives present under `s3://data-sink/landing/epa/`:

| Archive | Size (zip) | Written (UTC) | Target members (uncompressed / compressed) |
|---|--:|---|---|
| `npdes_downloads.zip` | 343.6 MB | 2026-06-03 00:39:27 | `ICIS_PERMITS.csv` 369.4 / 97.0 MB · `ICIS_FACILITIES.csv` 187.4 / 68.8 MB |
| `case_downloads.zip` | 79.3 MB | 2026-06-03 00:46:55 | `CASE_DEFENDANTS.csv` 11.8 / 3.4 MB · `CASE_FACILITIES.csv` 25.3 / 9.7 MB |

Member names verified against each ZIP's central directory (15 members in `npdes_downloads.zip`,
22 in `case_downloads.zip`); all four targets present and `ZIP_DEFLATED`.

## 1.2 `ICIS_PERMITS` — permit master (permittee legal name)

**Rows: 1,694,646 · Columns: 28.** Carries `PERMIT_NAME`; carries **no** address and **no**
`REGISTRY_ID` of its own (address/registry come from `ICIS_FACILITIES`).

- **Grain:** one row per **(permit, version)**. `EXTERNAL_PERMIT_NMBR` distinct = **1,194,023**
  → **500,623 rows are prior permit versions** (`VERSION_NMBR`, 100% filled). `ACTIVITY_ID`
  100% filled (per-version surrogate).
- **Name fill:** `PERMIT_NAME` **1,647,485 / 97.22%**.

| Column | Fill | Role / type |
|---|--:|---|
| `EXTERNAL_PERMIT_NMBR` | 100.00% | **NPDES permit id — join key to `ICIS_FACILITIES.NPDES_ID` & `epa_npdes_dmrs`** (txt) |
| `ACTIVITY_ID` | 100.00% | per-version activity id (txt) |
| `VERSION_NMBR` | 100.00% | permit version (int) |
| `PERMIT_NAME` | 97.22% | **permittee legal name — payload** (txt) |
| `PERMIT_TYPE_CODE` | 100.00% | type (txt · BITMAP) |
| `PERMIT_STATUS_CODE` | 97.26% | status (txt · BITMAP) |
| `MAJOR_MINOR_STATUS_FLAG` | 96.43% | major/minor (txt · BITMAP) |
| `FACILITY_TYPE_INDICATOR` | 97.09% | POTW/non-POTW etc. (txt) |
| `AGENCY_TYPE_CODE` / `ISSUING_AGENCY` | 100.0% / 39.87% | issuing authority (txt) |
| `ORIGINAL_ISSUE_DATE`,`ISSUE_DATE`,`EFFECTIVE_DATE`,`EXPIRATION_DATE` | ~96.67% | lifecycle (date) |
| `RETIREMENT_DATE`,`TERMINATION_DATE` | 27.78% / 36.73% | lifecycle (date) |
| `TOTAL_DESIGN_FLOW_NMBR`,`ACTUAL_AVERAGE_FLOW_NMBR` | 21.13% / 6.40% | flow (num) |
| `MASTER_EXTERNAL_PERMIT_NMBR` | 80.77% | general-permit master (txt) |
| *(+ `RNC_TRACKING_FLAG`, `DMR_NON_RECEIPT_FLAG`, `PERMIT_COMP_STATUS_FLAG`, `STATE_WATER_BODY[_NAME]`, `EDMR_AUTHORIZATION_FLAG`, `TMDL_INTERFACE_FLAG`, `PRETREATMENT_INDICATOR_CODE`, `RAD_WBD_HUC12S`)* | varies | metadata (txt/flag) |

## 1.3 `ICIS_FACILITIES` — permit facility (REGISTRY_ID + address)

**Rows: 1,192,755 · Columns: 14.** This is the address + `REGISTRY_ID` carrier for permits.

- **Grain: `NPDES_ID` is UNIQUE** — max facility rows per `NPDES_ID` = **1**, zero `NPDES_ID`
  with >1 row. The permit→facility join is therefore **clean 1:1, no fan-out.**
- **`FACILITY_UIN` (≡ `REGISTRY_ID`) fill: 1,187,698 / 99.58%**, distinct = **1,013,316**.

| Column | Fill | Role / type |
|---|--:|---|
| `NPDES_ID` | 100.00% | **PK; join key ← `ICIS_PERMITS.EXTERNAL_PERMIT_NMBR`** (txt) |
| `FACILITY_UIN` | 99.58% | **≡ `REGISTRY_ID` — universal hub key** (txt) |
| `ICIS_FACILITY_INTEREST_ID` | 100.00% | ICIS surrogate (txt) |
| `FACILITY_NAME` | 100.00% | site name (txt) |
| `LOCATION_ADDRESS` | 100.00% | street (txt) |
| `SUPPLEMENTAL_ADDRESS_TEXT` | 10.49% | street line 2 (txt) |
| `CITY` | 99.87% | city (txt) |
| `STATE_CODE` | 100.00% | state (txt · BITMAP) |
| `ZIP` | 100.00% | ZIP (txt) |
| `COUNTY_CODE` | 58.06% | county FIPS (txt) |
| `GEOCODE_LATITUDE` / `GEOCODE_LONGITUDE` | 93.01% | geocode (num) |
| `FACILITY_TYPE_CODE` | 64.15% | facility class (txt) |
| `IMPAIRED_WATERS` | 23.09% | 303(d) flag (txt) |

## 1.4 `CASE_DEFENDANTS` — enforcement defendant (legal name)

**Rows: 200,159 · Columns: 5.** Carries `DEFENDANT_NAME`; no address, no `REGISTRY_ID`
(both come from `CASE_FACILITIES`).

- **Grain:** one row per **(case-activity, defendant)**. `ACTIVITY_ID` distinct = **134,439**;
  **max 852 defendants per `ACTIVITY_ID`** (multi-defendant federal cases).
- `DEFENDANT_NAME` **200,155 / ~100%** · `ACTIVITY_ID`, `CASE_NUMBER`,
  `NAMED_IN_COMPLAINT_FLAG`, `NAMED_IN_SETTLEMENT_FLAG` all **100%**.

## 1.5 `CASE_FACILITIES` — case facility (REGISTRY_ID + address)

**Rows: 202,509 · Columns: 10.** Address + `REGISTRY_ID` carrier for defendants.

- **Grain:** one row per **(case-activity, facility)**. `ACTIVITY_ID` distinct = **134,056**;
  **max 860 facilities per `ACTIVITY_ID`.**
- `REGISTRY_ID` **201,705 / 99.60%**, distinct = **113,414**.

| Column | Fill | Role / type |
|---|--:|---|
| `ACTIVITY_ID` | 100.00% | **join key ← `CASE_DEFENDANTS.ACTIVITY_ID`; → `epa_case_enforcements`** (txt) |
| `CASE_NUMBER` | 100.00% | case number (txt) |
| `REGISTRY_ID` | 99.60% | **universal hub key — direct, no UIN alias** (txt) |
| `FACILITY_NAME` | 100.00% | site name (txt) |
| `LOCATION_ADDRESS` | 98.95% | street (txt) |
| `CITY` | 99.20% | city (txt) |
| `STATE_CODE` | 99.88% | state (txt · BITMAP) |
| `ZIP` | 99.04% | ZIP (txt) |
| `PRIMARY_SIC_CODE` | 58.56% | industry SIC (txt) |
| `PRIMARY_NAICS_CODE` | 39.78% | industry NAICS (txt) |

## 1.6 Join verification (measured, not assumed)

### Permits — `ICIS_PERMITS.EXTERNAL_PERMIT_NMBR = ICIS_FACILITIES.NPDES_ID`, `REGISTRY_ID = FACILITY_UIN`

| Metric | Value |
|---|--:|
| Permit rows total | 1,694,646 |
| Permit rows with a matching facility | **1,691,765 (99.83%)** |
| Permit rows → non-null `REGISTRY_ID` | **1,686,705 (99.53%)** |
| Named permit rows → non-null `REGISTRY_ID` | **1,640,139** |
| Distinct `REGISTRY_ID` reached | **1,013,316** |
| Facility fan-out | **none** (`NPDES_ID` unique) |

→ **`epa_permits` materializable rows ≈ 1,686,705** (INNER, `REGISTRY_ID` non-null). `REGISTRY_ID`
is the **resolution key, not a unique PK** — 1.69 M permit-version rows collapse onto 1.01 M
distinct entities (multiple permits/versions per facility).

### Defendants — `CASE_DEFENDANTS.ACTIVITY_ID = CASE_FACILITIES.ACTIVITY_ID`, `REGISTRY_ID` direct

| Metric | Value |
|---|--:|
| Joined rows (defendant × facility per activity) | 311,594 |
| Joined rows → non-null `REGISTRY_ID` | **310,522** |
| Distinct `REGISTRY_ID` reached | **113,217** |
| Distinct (`REGISTRY_ID`, `DEFENDANT_NAME`) pairs | 235,702 |
| Defendant `ACTIVITY_ID`s with **no** facility row (lose `REGISTRY_ID`) | **961** |

→ **`epa_defendants` materializable rows ≈ 310,522** at **(ACTIVITY_ID, REGISTRY_ID, DEFENDANT_NAME)**
grain. ⚠️ **Cartesian caveat:** ECHO carries no defendant→facility foreign key; in a multi-defendant
*and* multi-facility case the join pairs every defendant with every facility. The blow-up is bounded
(310,522 vs ~200 K inputs — most activities are 1×1), but the dataset asserts *candidate* entity edges,
not adjudicated defendant-operated-this-site facts. Carry `ACTIVITY_ID` + `CASE_NUMBER` so downstream
can collapse/weight.

---

# PHASE 2 — Materialization Build Plan

Two **net-new** Lance datasets in `s3://data-sink/active/`, Lance `data_storage_version="2.1"`
(matches the existing 11-dataset EPA family), `max_rows_per_file=1,048,576`. Build pattern is the
fleet default already proven in `materialize_epa.py`: random-access ZIP member → `.csv.gz` → DuckDB
`read_csv(all_varchar=true, parallel=false)` transform → stream to Lance → `create_scalar_index`
direct-to-R2. Both fit cleanly as **two additional join-based specs + a small two-source builder**
in the existing `epa-pipelines` Modal app (the four members are already extracted inside
`build_bridge`; this persists the join instead of dropping it).

## 2.1 `epa_permits`

- **Source:** `npdes_downloads.zip::ICIS_PERMITS.csv` ⟕ `npdes_downloads.zip::ICIS_FACILITIES.csv`
- **Grain:** one row per permit-version (`EXTERNAL_PERMIT_NMBR`, `VERSION_NMBR`); facility side 1:1.
- **Join:** `INNER JOIN … ON f.NPDES_ID = p.EXTERNAL_PERMIT_NMBR` with `f.FACILITY_UIN IS NOT NULL`
  (requires a resolvable `REGISTRY_ID`; drops ~7,941 orphan permit rows / 0.47%).
- **Expected rows:** ~1,686,705 · **distinct `REGISTRY_ID`:** ~1,013,316.

```sql
-- epa_permits  (facility cols namespaced FAC_* to avoid collision; permit side native)
SELECT
    nullif(trim(f.FACILITY_UIN),'')              AS REGISTRY_ID,         -- PK / hub  · BTREE
    nullif(trim(p.PERMIT_NAME),'')               AS PERMIT_NAME,         -- payload
    nullif(trim(p.EXTERNAL_PERMIT_NMBR),'')      AS EXTERNAL_PERMIT_NMBR,-- → epa_npdes_dmrs · BTREE
    nullif(trim(f.NPDES_ID),'')                  AS NPDES_ID,            -- → npdes violation tables · BTREE
    TRY_CAST(nullif(trim(p.VERSION_NMBR),'') AS BIGINT) AS VERSION_NMBR,
    nullif(trim(p.ACTIVITY_ID),'')               AS ACTIVITY_ID,
    nullif(trim(p.PERMIT_TYPE_CODE),'')          AS PERMIT_TYPE_CODE,    -- BITMAP
    nullif(trim(p.PERMIT_STATUS_CODE),'')        AS PERMIT_STATUS_CODE,  -- BITMAP
    nullif(trim(p.MAJOR_MINOR_STATUS_FLAG),'')   AS MAJOR_MINOR_STATUS_FLAG, -- BITMAP
    nullif(trim(p.FACILITY_TYPE_INDICATOR),'')   AS FACILITY_TYPE_INDICATOR,
    nullif(trim(p.AGENCY_TYPE_CODE),'')          AS AGENCY_TYPE_CODE,
    nullif(trim(p.ISSUING_AGENCY),'')            AS ISSUING_AGENCY,
    nullif(trim(p.MASTER_EXTERNAL_PERMIT_NMBR),'') AS MASTER_EXTERNAL_PERMIT_NMBR,
    CAST(try_strptime(nullif(trim(p.ORIGINAL_ISSUE_DATE),''),'%m/%d/%Y') AS DATE) AS ORIGINAL_ISSUE_DATE,
    CAST(try_strptime(nullif(trim(p.ISSUE_DATE),''),      '%m/%d/%Y') AS DATE) AS ISSUE_DATE,
    CAST(try_strptime(nullif(trim(p.EFFECTIVE_DATE),''),  '%m/%d/%Y') AS DATE) AS EFFECTIVE_DATE,
    CAST(try_strptime(nullif(trim(p.EXPIRATION_DATE),''), '%m/%d/%Y') AS DATE) AS EXPIRATION_DATE,
    CAST(try_strptime(nullif(trim(p.TERMINATION_DATE),''),'%m/%d/%Y') AS DATE) AS TERMINATION_DATE,
    CAST(try_strptime(nullif(trim(p.RETIREMENT_DATE),''), '%m/%d/%Y') AS DATE) AS RETIREMENT_DATE,
    TRY_CAST(nullif(trim(p.TOTAL_DESIGN_FLOW_NMBR),'')   AS DOUBLE) AS TOTAL_DESIGN_FLOW_NMBR,
    TRY_CAST(nullif(trim(p.ACTUAL_AVERAGE_FLOW_NMBR),'') AS DOUBLE) AS ACTUAL_AVERAGE_FLOW_NMBR,
    -- permit facility address (namespaced)
    nullif(trim(f.FACILITY_NAME),'')             AS FAC_NAME,
    nullif(trim(f.LOCATION_ADDRESS),'')          AS FAC_LOCATION_ADDRESS,
    nullif(trim(f.SUPPLEMENTAL_ADDRESS_TEXT),'') AS FAC_SUPPLEMENTAL_ADDRESS,
    nullif(trim(f.CITY),'')                      AS FAC_CITY,
    nullif(trim(f.STATE_CODE),'')                AS FAC_STATE_CODE,      -- BITMAP
    nullif(trim(f.ZIP),'')                       AS FAC_ZIP,
    nullif(trim(f.COUNTY_CODE),'')               AS FAC_COUNTY_CODE,
    TRY_CAST(nullif(trim(f.GEOCODE_LATITUDE),'')  AS DOUBLE) AS FAC_LATITUDE,
    TRY_CAST(nullif(trim(f.GEOCODE_LONGITUDE),'') AS DOUBLE) AS FAC_LONGITUDE
FROM icis_permits p
JOIN icis_facilities f ON f.NPDES_ID = p.EXTERNAL_PERMIT_NMBR
WHERE nullif(trim(f.FACILITY_UIN),'') IS NOT NULL;
```

**Indexing — `epa_permits`**
- **BTREE:** `REGISTRY_ID` *(required)*, `EXTERNAL_PERMIT_NMBR`, `NPDES_ID`
- **BITMAP:** `PERMIT_TYPE_CODE`, `PERMIT_STATUS_CODE`, `MAJOR_MINOR_STATUS_FLAG`, `FAC_STATE_CODE`

## 2.2 `epa_defendants`

- **Source:** `case_downloads.zip::CASE_DEFENDANTS.csv` ⟕ `case_downloads.zip::CASE_FACILITIES.csv`
- **Grain:** one row per (`ACTIVITY_ID`, `REGISTRY_ID`, `DEFENDANT_NAME`); `SELECT DISTINCT` to
  collapse exact duplicate edges.
- **Join:** `INNER JOIN … ON cf.ACTIVITY_ID = cd.ACTIVITY_ID` with `cf.REGISTRY_ID IS NOT NULL`.
- **Expected rows:** ~310,522 · **distinct `REGISTRY_ID`:** ~113,217 · **distinct (registry,name):** ~235,702.

```sql
-- epa_defendants  (case facility cols namespaced FAC_*; case/defendant native)
SELECT DISTINCT
    nullif(trim(cf.REGISTRY_ID),'')          AS REGISTRY_ID,        -- PK / hub  · BTREE
    nullif(trim(cd.DEFENDANT_NAME),'')       AS DEFENDANT_NAME,     -- payload
    nullif(trim(cd.ACTIVITY_ID),'')          AS ACTIVITY_ID,        -- → epa_case_enforcements/milestones · BTREE
    nullif(trim(cd.CASE_NUMBER),'')          AS CASE_NUMBER,        -- BTREE
    nullif(trim(cd.NAMED_IN_COMPLAINT_FLAG),'')  AS NAMED_IN_COMPLAINT_FLAG,  -- BITMAP
    nullif(trim(cd.NAMED_IN_SETTLEMENT_FLAG),'') AS NAMED_IN_SETTLEMENT_FLAG, -- BITMAP
    -- case facility address (namespaced)
    nullif(trim(cf.FACILITY_NAME),'')        AS FAC_NAME,
    nullif(trim(cf.LOCATION_ADDRESS),'')     AS FAC_LOCATION_ADDRESS,
    nullif(trim(cf.CITY),'')                 AS FAC_CITY,
    nullif(trim(cf.STATE_CODE),'')           AS FAC_STATE_CODE,     -- BITMAP
    nullif(trim(cf.ZIP),'')                  AS FAC_ZIP,
    nullif(trim(cf.PRIMARY_SIC_CODE),'')     AS FAC_PRIMARY_SIC_CODE,
    nullif(trim(cf.PRIMARY_NAICS_CODE),'')   AS FAC_PRIMARY_NAICS_CODE
FROM case_defendants cd
JOIN case_facilities cf ON cf.ACTIVITY_ID = cd.ACTIVITY_ID
WHERE nullif(trim(cf.REGISTRY_ID),'') IS NOT NULL;
```

**Indexing — `epa_defendants`**
- **BTREE:** `REGISTRY_ID` *(required)*, `ACTIVITY_ID`, `CASE_NUMBER`
- **BITMAP:** `FAC_STATE_CODE`, `NAMED_IN_SETTLEMENT_FLAG`

> **Case metadata is not duplicated.** `epa_case_enforcements` (135,053 rows, BTREE `ACTIVITY_ID`
> / `CASE_NUMBER`) already carries `CASE_NAME`, penalties, dates, `DOJ_DOCKET_NMBR`. `epa_defendants`
> carries the two join keys so the caption/penalty layer attaches by index — no column copy.

## 2.3 Where it slots (no new endpoints, no new secrets)

- **Two new entries in `build_specs()` semantics**, but join-based (two members each) rather than
  single-member — modeled on the existing `build_bridge` extraction (which already lands all four).
  Reuse `_member_to_gz`, `_read()` (`parallel=false`), `_new_con`, `_build_indexes`, `_record_run`.
- **Ops ledger:** writes `ops.epa_ingest_runs` rows (`dataset='epa_permits'|'epa_defendants'`) —
  existing table, no schema change.
- **Orchestration:** dispatcher-resolvable in `epa-pipelines`; manual `modal run … ::one
  --name epa_permits`. Control-plane callback unchanged.
- **Storage:** `data_storage_version="2.1"`, direct-R2 BTREE/BITMAP (both sets far below the
  ~100 M-row Volume-staging threshold).

## 2.4 Decisions for review (taste calls, not blockers)

1. **Permit version grain.** Plan keeps **all 1.69 M permit-version rows** (lossless; BTREE on
   `REGISTRY_ID` returns every permit/version for an entity). Alternative: dedupe to latest version
   per `EXTERNAL_PERMIT_NMBR` (→ ~1.19 M rows) via `row_number() OVER (PARTITION BY
   EXTERNAL_PERMIT_NMBR ORDER BY VERSION_NMBR DESC)=1`. **Recommend lossless + an `IS_LATEST_VERSION`
   boolean** so both reads are one filter away.
2. **Orphan handling.** Plan uses **INNER** (require non-null `REGISTRY_ID`) — drops 0.47% of permits
   and 961 facility-less defendant activities that carry no hub key and serve no resolution purpose.
   Alternative LEFT JOIN retains them with null `REGISTRY_ID` (un-indexable). **Recommend INNER.**
3. **Defendant cartesian.** Plan persists candidate (defendant × same-case facility) edges with
   `ACTIVITY_ID` retained. If a stricter single-edge grain is wanted, add a downstream collapse to
   distinct (`REGISTRY_ID`, `DEFENDANT_NAME`) (~235,702 rows) — but that discards which case asserted
   the edge. **Recommend keeping `ACTIVITY_ID` grain.**

## 2.5 Post-build verification gate (read-back, before "done")

- `lance.dataset(uri).count_rows()` within ±0 of the join projection (permits ≈1,686,705;
  defendants ≈310,522).
- `REGISTRY_ID` BTREE present and **`null_count = 0`** on both sets.
- Point-lookup proof: a known `REGISTRY_ID` returns its permits in `epa_permits` and its defendant
  rows in `epa_defendants`; `EXTERNAL_PERMIT_NMBR`/`ACTIVITY_ID` index-join to `epa_npdes_dmrs` /
  `epa_case_enforcements` resolves.
- `ops.epa_ingest_runs` shows both datasets `status=success` with `indexes_built` populated.
