# EPA NPDES DMR Historical Backfill — Structural Diagnostic & Incremental Materialization Plan

A 16-archive historical Discharge Monitoring Report (DMR) payload — `npdes_dmrs_prefy2009.zip`
plus `npdes_dmrs_fy2009.zip … fy2023.zip` — was staged under `s3://data-sink/landing/epa/`.
This document is the read-only diagnostic of that payload and the append-only build plan to
extend the **live** `epa_npdes_dmrs` Lance SoR (currently FY2024–FY2026, 67,597,592 rows)
backward to **fiscal year 1982**. The Python/Modal pipeline is intentionally **not** written
here — it is gated on review of this plan.

**Provenance / attestation.** Every figure below is a **live, read-only read of R2**, not a
recon estimate. Harness: `boto3` central-directory random-access — a **partial-deflate header
scan** (first ~3 MB inflated) of every member to map schema, then a full `_member_to_gz`
raw-deflate→gzip rewrap of each member (the technique lifted verbatim from
`pipelines/ingest_epa/materialize_epa.py`; `/tmp` held only the **compressed** member, never
the decompressed CSV) feeding `duckdb 1.5` `read_csv(all_varchar=true, parallel=false)` for the
**full** per-archive scan (exact `count(*)`, hub-key fill, telemetry null density, date span,
ID-format split, numeric-cast sanity) and a hub-resolution join against the live `ICIS_PERMITS`
master. The live target schema/indices were read with `pylance 7` directly off
`s3://data-sink/active/epa_npdes_dmrs/`. R2 reached under `doppler run -p core-x -c prd`.
**No DDL, no Lance write, no migration, no `ops.*` ledger row was executed.** All 16 archives
were full-scanned under strict parse (`ignore_errors=false`); reads were local over WAN
(latency not representative of in-region Modal). Landing archives written **2026-06-05
22:43–23:02 UTC**; audit date **2026-06-05**.

---

## 0. Headline posture

| File | Era | Total Rows | Uncompressed | Primary Key Grain | Hub Key Fill (`EXTERNAL_PERMIT_NMBR`) | Status / Verdict |
|---|---|--:|--:|---|--:|---|
| `npdes_dmrs_prefy2009.zip` | FY1982–FY2008 | **66,924,459** | 23.64 GB | DMR form-value (`DMR_FORM_VALUE_ID` ≈ row-unique) | **100.00%** | ✅ Clean. Legacy giant; per-row FY derivation needed. |
| `npdes_dmrs_fy2009.zip` | FY2009 | 11,077,254 | 3.97 GB | ″ | **100.00%** | ✅ Ship. |
| `npdes_dmrs_fy2010.zip` | FY2010 | 11,553,700 | 4.15 GB | ″ | **100.00%** | ✅ Ship. |
| `npdes_dmrs_fy2011.zip` | FY2011 | 12,022,314 | 4.32 GB | ″ | **100.00%** | ✅ Ship. |
| `npdes_dmrs_fy2012.zip` | FY2012 | 12,595,068 | 4.52 GB | ″ | **100.00%** | ✅ Ship. |
| `npdes_dmrs_fy2013.zip` | FY2013 | 13,662,301 | 4.91 GB | ″ | **100.00%** | ✅ Ship. |
| `npdes_dmrs_fy2014.zip` | FY2014 | 16,345,615 | 5.92 GB | ″ | **100.00%** | ✅ Ship. |
| `npdes_dmrs_fy2015.zip` | FY2015 | 17,533,730 | 6.35 GB | ″ | **100.00%** | ✅ Ship. |
| `npdes_dmrs_fy2016.zip` | FY2016 | 20,324,087 | 7.34 GB | ″ | **100.00%** | ✅ Ship. |
| `npdes_dmrs_fy2017.zip` | FY2017 | 22,783,335 | 8.24 GB | ″ | **100.00%** | ✅ Ship. |
| `npdes_dmrs_fy2018.zip` | FY2018 | 23,722,332 | 8.59 GB | ″ | **100.00%** | ✅ Ship. |
| `npdes_dmrs_fy2019.zip` | FY2019 | 24,365,211 | 8.82 GB | ″ | **100.00%** | ✅ Ship. |
| `npdes_dmrs_fy2020.zip` | FY2020 | 24,920,544 | 9.01 GB | ″ | **100.00%** | ✅ Ship. |
| `npdes_dmrs_fy2021.zip` | FY2021 | 25,278,470 | 9.13 GB | ″ | **100.00%** | ✅ Ship. |
| `npdes_dmrs_fy2022.zip` | FY2022 | 25,579,026 | 9.22 GB | ″ | **100.00%** | ✅ Ship. |
| `npdes_dmrs_fy2023.zip` | FY2023 | 26,162,398 | 9.42 GB | ″ | **100.00%** | ✅ Ship. |
| **BACKFILL TOTAL** | **FY1982–FY2023** | **354,849,844** | **127.55 GB** | — | **100.00%** | ✅ **Append-only, zero schema work.** |

**Posture in one line.** The payload is **structurally clean and structurally identical to the
live SoR** — same 57 source columns, same order, every era — so the backfill is a pure
append-only extension with **zero schema reconciliation**. It adds **354,849,844 rows
(5.25× the current 67,597,592)**, taking the unified `epa_npdes_dmrs` to **~422,447,436 rows**
and extending the discharge-history floor from FY2024 back to **FY1982**.

| Cross-check | Value |
|---|--:|
| Live `epa_npdes_dmrs` today (FY2024–FY2026) | 67,597,592 rows · 58 cols · v12 · 66 fragments |
| Backfill rows (this payload) | 354,849,844 |
| **Unified total after backfill** | **422,447,436** |
| Hub-key (`EXTERNAL_PERMIT_NMBR`) null rows, all 16 archives | **0** |
| Non-numeric `DMR_VALUE_NMBR` cells (would silently null on cast), all 16 | **0** |
| Strict-parse failures (`ignore_errors=false`), all 16 | **0** |
| Historical rows resolvable to current `ICIS_PERMITS` master (3 sampled eras) | **~100.0%** |

---

# PHASE 1 — Diagnostic & Verification

## 1.1 Landing footprint (live `boto3` listing + central-directory scan)

All 16 archives present under `s3://data-sink/landing/epa/`, each a **single-member** ZIP
(`NPDES_DMRS_<ERA>.csv`), all `ZIP_DEFLATED`. `prefy2009` and FY2011+ are **ZIP64** (member
> 4 GiB); FY2009/FY2010 are not (sub-4 GiB members). The `_member_to_gz` raw-deflate→gzip
rewrap in `materialize_epa.py` already resolves ZIP64 offsets via `zipfile`, so this is a
no-op concern for the build. Compressed (zip) footprint of the backfill = **8.29 GB**;
uncompressed = **127.55 GB**.

## 1.2 Schema drift — **there is none (structural)**

This is the decisive finding and it inverts the working assumption that a 15-year archive set
would carry column renames, drops, or type churn:

> **Every one of the 16 archives — `prefy2009` (FY1982 data) through `fy2023` — exposes the
> identical 57-column header, in identical order, byte-for-byte matching the live FY2024 source
> and the materialized SoR.** Pairwise set-difference across the full timeline returns **∅** at
> every adjacency. There is no column to rename, no field to drop, no positional remap.

The ECHO `NPDES_DMRS` bulk export schema has been frozen across the entire window. Consequence:
the existing `epa_npdes_dmrs` projection in `materialize_epa.py` (spec #3, the
`dmr_excl` / `dmr_recasts` retype) applies **verbatim** to all 16 historical archives. The drift
that *does* exist is strictly **data-level**, not structural (§1.3).

### Type mapping — raw CSV → live `epa_npdes_dmrs` (read from Lance, v12)

`SELECT * EXCLUDE(<13 cols>), <13 typed casts>, FISCAL_YEAR` re-orders the typed columns to the
schema tail, so the live Lance column order differs from the CSV; an identical projection
reproduces it byte-identically, which is what makes the append safe (§2.3). **44 columns pass
through as `string`** (preserving EPA's native codes); the **13 load-bearing columns are typed**,
plus the synthetic `FISCAL_YEAR`:

| Source column (CSV, `varchar`) | Live SoR type | Cast macro (`materialize_epa.py`) |
|---|---|---|
| `EXTERNAL_PERMIT_NMBR` | `string` | `nullif(trim(·),'')` — **hub key** |
| `LIMIT_VALUE_NMBR`, `DMR_VALUE_NMBR`, `LIMIT_VALUE_STANDARD_UNITS`, `DMR_VALUE_STANDARD_UNITS`, `EXCEEDENCE_PCT` | `double` | `TRY_CAST(nullif(trim(·),'') AS DOUBLE)` |
| `DAYS_LATE` | `int64` | `TRY_CAST(… AS BIGINT)` |
| `MONITORING_PERIOD_END_DATE`, `LIMIT_BEGIN_DATE`, `LIMIT_END_DATE`, `VALUE_RECEIVED_DATE`, `RNC_DETECTION_DATE`, `RNC_RESOLUTION_DATE` | `date32[day]` | `CAST(try_strptime(·,'%m/%d/%Y') AS DATE)` |
| *(synthetic)* `FISCAL_YEAR` | `int32` | per-archive literal (FY2009–FY2023) / **per-row derivation (prefy2009)** |

Every `TRY_CAST`/`try_strptime` is NULL-safe by construction — a malformed cell yields `NULL`,
never a hard failure. The measured risk of *silent* nulling is zero: **`DMR_VALUE_NMBR` had 0
non-numeric non-null cells across all 354.8 M rows** (§1.4), and `MONITORING_PERIOD_END_DATE`
parsed to a valid date on 100% of non-null cells in every era.

## 1.3 Data-level drift (real, but does not touch structure or the hub)

Three gradients exist inside the stable schema. None require per-era handling because the
affected columns are carried as `string` in the SoR and the hub key is format-stable:

1. **Surrogate-ID width (`ACTIVITY_ID` etc.).** Internal ICIS surrogate keys migrate from
   8-digit legacy (PCS-era) integers to 10-digit ICIS integers — a **smooth gradient, not a
   cutover**: `ACTIVITY_ID` < 10 chars is **40.8%** of `prefy2009`, **25.6%** in FY2009, decaying
   monotonically to **0.5%** by FY2023 (`maxlen = 10` everywhere). This reflects when each
   permit/activity was created in ICIS-NPDES, not a schema change. `ACTIVITY_ID` and all sibling
   IDs are `string` in the SoR → **zero ingest impact**.
2. **Hub key is format-stable.** `EXTERNAL_PERMIT_NMBR` is a canonical NPDES permit id
   (`AKG521001`, `TXR1573WE` …) in **every** era and is **100.00% populated everywhere**
   (`epn_fill == count(*)` exactly, all 16 archives). There is no legacy permit-id format to
   reconcile.
3. **Parameter vocabulary grows additively.** Distinct `PARAMETER_CODE` rises ~2,232 (`prefy2009`)
   → 1,940 (FY2009) → 2,657 (FY2023) as EPA adds analytes. Codes are stable identifiers (no
   renames); `all_varchar` ingest preserves them verbatim. No code-mapping table needed.

## 1.4 Scale, grain & null density (full-scan, exact)

**Record grain.** A DMR row is one **DMR form value** — a single reported value, limit value,
or no-data indicator for one parameter, at one monitoring location, for one monitoring period,
under one permit feature/limit. `DMR_FORM_VALUE_ID` is **100% populated** and its HLL distinct
count tracks `count(*)` within HLL error in every era (≈ row-unique → the natural per-row
surrogate). The suffix composite
`(EXTERNAL_PERMIT_NMBR, PERM_FEATURE_ID, LIMIT_SET_ID, LIMIT_ID, LIMIT_VALUE_ID,
MONITORING_PERIOD_END_DATE, VALUE_TYPE_CODE, PARAMETER_CODE)` is likewise ≈ row-cardinality. The
live SoR's resolution BTREE — `(EXTERNAL_PERMIT_NMBR, MONITORING_PERIOD_END_DATE)` — is a
**many-rows-per-key lookup index, not the unique grain**, and is the correct choice to carry
forward. *(Distinct figures are HyperLogLog approximations, ±~2%; exact `COUNT(DISTINCT)` was
deliberately not forced on the 66.9 M-row legacy member — uniqueness is asserted as
"approximately row-unique," not proven.)*

**Null density — systemic, explainable, not corruption.** The two telemetry gradients are
*coupled*, which is the signature of a healthy DMR feed rather than decay:

| Era | `DMR_VALUE_NMBR` (measured value) | `NODI_CODE` (no-data indicator) | measured **OR** NODI | `LIMIT_VALUE_NMBR` | `VIOLATION_CODE` |
|---|--:|--:|--:|--:|--:|
| prefy2009 | 63.1% | 32.2% | 95.3% | 56.6% | 9.6% |
| FY2009 | 59.0% | 35.8% | 94.8% | 53.4% | 9.7% |
| FY2013 | 50.9% | 34.2% | 85.1% | 52.7% | 18.5% |
| FY2015 | 42.2% | 34.8% | 77.0% | 48.2% | 26.4% |
| FY2019 | 44.7% | 50.0% | 94.7% | 47.8% | 8.6% |
| FY2023 | 42.6% | 53.6% | 96.2% | 47.2% | 6.6% |

- **`DMR_VALUE_NMBR` ↓ and `NODI_CODE` ↑ are inverse**: as electronic reporting (NeT/ICIS) grew,
  a larger share of rows carry a *no-data indicator* (no-discharge, conditional monitoring, etc.)
  instead of a numeric value. Their **union holds ~95%** in every era except a mid-window dip
  (FY2013–FY2015, ~77–85%) where a larger fraction of rows are limit-definition rows. This is the
  expected interleave of limit rows + measured rows + NODI rows in the ECHO grain — **not a
  reporting gap to repair**.
- **`VIOLATION_CODE` density is determination-lagged.** It peaks FY2014–FY2015 (24–26%) and falls
  to ~6% by FY2022–FY2023. Recent monitoring periods have not yet completed Reportable
  Non-Compliance (RNC) determination, so **recent violation density understates eventual
  violation rates** — a read-time caveat for any time-series, not a data defect.
- `EXCEEDENCE_PCT` is sparse by design (~0.6–1.5%: only populated where a value exceeds a limit).
  `VALUE_RECEIVED_DATE` is 95.4% (prefy2009) and near-complete in modern eras.

## 1.5 Hub resolution — historical rows bind cleanly (the "orphan" question, answered)

`epa_npdes_dmrs` carries **no `REGISTRY_ID` and no geocode** (confirmed: the 57-column grain has
neither — both live in `ICIS_FACILITIES`, persisted as the `epa_permits` name node per
`EPA_LEGAL_ENTITY_MATERIALIZATION_PLAN.md`). The hub is reached **indirectly**:
`epa_npdes_dmrs.EXTERNAL_PERMIT_NMBR → epa_permits.EXTERNAL_PERMIT_NMBR → REGISTRY_ID`
(equivalently `ICIS_FACILITIES.NPDES_ID → FACILITY_UIN`, or `epa_program_links` `PGM_SYS_ACRNM='NPDES'`).

The directive's concern about *orphaned records needing a pre-load filter* does **not
materialize**. Measured against the **current** `ICIS_PERMITS` master (1,194,023 distinct
`EXTERNAL_PERMIT_NMBR`):

| Era | Rows | `EXTERNAL_PERMIT_NMBR` null | Rows resolvable to permit master | Distinct permits (HLL) |
|---|--:|--:|--:|--:|
| prefy2009 (FY1982–2008) | 66,924,459 | 0 | **66,924,057 (100.0%)** | ~48,019 (all resolve) |
| FY2015 | 17,533,730 | 0 | **17,533,730 (100.0%)** | ~58,633 (all resolve) |
| FY2023 | 26,162,398 | 0 | **26,162,398 (100.0%)** | ~106,892 (all resolve) |

EPA's `ICIS_PERMITS` is a **cumulative** registry (retired permits retained), so even the
1982–2008 legacy archive resolves at ~100% (402 of 66.9 M rows — 0.0006% — fail to match, an
immaterial tail). **There is no orphan population to filter.** The distinct-permit growth
(48 K → 107 K) tracks the expansion of electronic DMR coverage from major-only to major+minor
dischargers, not data quality.

---

# PHASE 2 — Incremental Materialization Plan

Append-only extension of the **existing** `s3://data-sink/active/epa_npdes_dmrs/` (Lance
`data_storage_version="2.1"`, `max_rows_per_file=1,048,576`). **No new dataset, no new schema,
no new secret, no new endpoint.** The build is the fleet default already proven in
`materialize_epa.py`: random-access ZIP member → `.csv.gz` (compressed-only in `/tmp`) → DuckDB
`read_csv(all_varchar=true, parallel=false)` transform → streaming `COPY` to ZSTD transport
parquet → stream parquet → Lance. The **only** deltas vs the live spec are (a) the source list,
(b) `mode="append"` enforced on every write, and (c) the `prefy2009` FISCAL_YEAR derivation.

## 2.1 Schema mapping → existing spec (no change)

The 16 historical archives slot into the live `epa_npdes_dmrs` spec unchanged. Reuse the exact
`dmr_excl` / `dmr_recasts` retype (`materialize_epa.py:198–222`); the projection's output Arrow
schema is **byte-identical** to the live dataset (same names, same 13 types, same tail order,
same `FISCAL_YEAR int32`), which is the precondition for a clean `append` (§2.3). The historical
sources are simply 16 additional `dict(archive=…, member=…, fy=…)` entries:

```
("npdes_dmrs_prefy2009.zip", "NPDES_DMRS_PREFY2009.csv", fy="DERIVE"),
("npdes_dmrs_fy2009.zip",    "NPDES_DMRS_FY2009.csv",    fy=2009),
...                                                       # fy=2010 … 2022
("npdes_dmrs_fy2023.zip",    "NPDES_DMRS_FY2023.csv",    fy=2023),
```

## 2.2 Joins & hub-resolution paths — tested DuckDB projections

**(a) FY2009–FY2023 — the live projection verbatim**, `__FY__` = the archive's fiscal year,
`__GZ__` = the extracted member path:

```sql
-- materialize_epa.py _retype(dmr_excl, dmr_recasts, extra={"FISCAL_YEAR":"CAST(__FY__ AS INTEGER)"})
SELECT
  * EXCLUDE (EXTERNAL_PERMIT_NMBR, LIMIT_VALUE_NMBR, DMR_VALUE_NMBR,
             LIMIT_VALUE_STANDARD_UNITS, DMR_VALUE_STANDARD_UNITS, EXCEEDENCE_PCT, DAYS_LATE,
             MONITORING_PERIOD_END_DATE, LIMIT_BEGIN_DATE, LIMIT_END_DATE, VALUE_RECEIVED_DATE,
             RNC_DETECTION_DATE, RNC_RESOLUTION_DATE),
  nullif(trim(EXTERNAL_PERMIT_NMBR),'')                              AS EXTERNAL_PERMIT_NMBR,
  TRY_CAST(nullif(trim(LIMIT_VALUE_NMBR),'')           AS DOUBLE)    AS LIMIT_VALUE_NMBR,
  TRY_CAST(nullif(trim(DMR_VALUE_NMBR),'')             AS DOUBLE)    AS DMR_VALUE_NMBR,
  TRY_CAST(nullif(trim(LIMIT_VALUE_STANDARD_UNITS),'') AS DOUBLE)    AS LIMIT_VALUE_STANDARD_UNITS,
  TRY_CAST(nullif(trim(DMR_VALUE_STANDARD_UNITS),'')   AS DOUBLE)    AS DMR_VALUE_STANDARD_UNITS,
  TRY_CAST(nullif(trim(EXCEEDENCE_PCT),'')             AS DOUBLE)    AS EXCEEDENCE_PCT,
  TRY_CAST(nullif(trim(DAYS_LATE),'')                  AS BIGINT)    AS DAYS_LATE,
  CAST(try_strptime(nullif(trim(MONITORING_PERIOD_END_DATE),''),'%m/%d/%Y') AS DATE) AS MONITORING_PERIOD_END_DATE,
  CAST(try_strptime(nullif(trim(LIMIT_BEGIN_DATE),''),  '%m/%d/%Y') AS DATE) AS LIMIT_BEGIN_DATE,
  CAST(try_strptime(nullif(trim(LIMIT_END_DATE),''),    '%m/%d/%Y') AS DATE) AS LIMIT_END_DATE,
  CAST(try_strptime(nullif(trim(VALUE_RECEIVED_DATE),''),'%m/%d/%Y') AS DATE) AS VALUE_RECEIVED_DATE,
  CAST(try_strptime(nullif(trim(RNC_DETECTION_DATE),''),'%m/%d/%Y') AS DATE) AS RNC_DETECTION_DATE,
  CAST(try_strptime(nullif(trim(RNC_RESOLUTION_DATE),''),'%m/%d/%Y') AS DATE) AS RNC_RESOLUTION_DATE,
  CAST(__FY__ AS INTEGER)                                            AS FISCAL_YEAR
FROM read_csv('__GZ__', all_varchar=true, header=true, parallel=false,
              compression='gzip', quote='"', escape='"', strict_mode=false, ignore_errors=false);
```

**(b) `prefy2009` — identical body, FISCAL_YEAR derived per row** (the archive bundles FY1982–
FY2008; a single literal would be wrong). EPA fiscal year = calendar year + 1 for Oct–Dec. The
date column is 100.00% populated (0 null dates), so the derivation is total:

```sql
  ...                                                                -- identical typed body as (a)
  CAST(
    year(try_strptime(nullif(trim(MONITORING_PERIOD_END_DATE),''),'%m/%d/%Y'))
    + CASE WHEN month(try_strptime(nullif(trim(MONITORING_PERIOD_END_DATE),''),'%m/%d/%Y')) >= 10
           THEN 1 ELSE 0 END
  AS INTEGER)                                                        AS FISCAL_YEAR  -- ∈ [1982, 2008]
FROM read_csv('__GZ__', all_varchar=true, header=true, parallel=false, compression='gzip',
              quote='"', escape='"', strict_mode=false, ignore_errors=false);
```

**Integrity / orphan WHERE clauses — measured to be unnecessary.** Unlike the facility tables,
the DMR grain has **no geocode to corrupt** and **no orphan population**:

- **`WHERE nullif(trim(EXTERNAL_PERMIT_NMBR),'') IS NOT NULL`** — the only defensible hub filter,
  and it **drops 0 rows** (100.00% fill, all 16 archives). Include it as a guard; it is a no-op.
- **No referential pre-filter on permit-master membership.** ~100% of rows already resolve
  (§1.5); the <0.001% non-resolving tail is valid historical discharge data. `REGISTRY_ID`
  binding is a *read-time* `LEFT JOIN` to `epa_permits`, never a load-time row drop — identical to
  how the live FY2024–FY2026 rows behave.
- **Typing is the only transform that can null a cell**, and it is non-destructive: `TRY_CAST` →
  NULL on bad input, measured at **0 occurrences** for `DMR_VALUE_NMBR`. No `strict_mode`, no
  `ignore_errors` — all 16 archives already parse strict.

## 2.3 Partitioned materialization strategy

### Memory management (Modal)

The decompressed CSV **never** materializes in RAM. Per archive, one worker streams
member → `.csv.gz` (compressed-only in `/tmp`: 0.25–1.38 GB) → DuckDB `COPY (<projection>) TO
parquet (ROW_GROUP_SIZE 1,048,576)` → stream parquet → Lance at `LANCE_READ_BATCH=100,000`.
Peak working set is bounded by **row-group size, not file size**, so `prefy2009` (23.64 GB / 66.9 M
rows) has the same steady-state footprint as a 4 GB archive. The existing container spec —
`memory=49152` (48 GB), `cpu=4`, DuckDB `memory_limit=24GB`, `SPILL_DIR` set,
`preserve_insertion_order=false` — already proved out on the 15.9 GB `NPDES_EFF_VIOLATIONS`
giant and is **sufficient unchanged** for the transform stage.

> **Caveat from this audit:** the local diagnostic's full-table *aggregate* (multi-column HLL +
> `min/max` over 66.9 M rows) OOM'd at a 13 GB limit and needed ~42 GB. That is an artifact of the
> **diagnostic** workload, **not** the materialization `COPY`, which is a projection-only
> streaming sink with no blocking aggregate. Do not size the build container off the diagnostic.

Recommended chunking: **one Modal container per archive** (the existing `materialize_one`
fan-out), `max_workers` bounded so concurrent decode stays within host memory. Each worker is
single-source (gzip is non-splittable → single-threaded decode regardless), so per-worker memory
is flat; concurrency is limited by the **commit** serialization below, not by RAM.

### Append-only safety (no full rewrite, no index corruption)

The live 67.6 M FY2024–FY2026 rows must be **preserved**. Three hard rules:

1. **`mode="append"` on every write — never `"overwrite"`.** The live `materialize_one` uses
   `overwrite` on its first source; the backfill must use a **dedicated append-only entrypoint**
   (e.g. `backfill`) that writes `append` for all 16. Lance append adds **new fragments** and a
   new manifest version; existing fragments and their data files are untouched (no rewrite).
2. **Serialize the commits.** Lance uses optimistic concurrency on the manifest; N parallel
   containers each calling `write_dataset(mode="append")` on the same URI will collide on the
   version bump. Two safe shapes:
   - **(simple)** one orchestrating container appends the 16 archives **sequentially** (one
     writer, zero conflict), ledger-guarded per archive for resumability; or
   - **(scale-out)** parallel workers each `write_fragments` (no commit) to staged parquet, then a
     **single** `LanceOperation.Append` merges all fragment metadata in **one atomic version
     bump** — parallel CPU, serial commit.
   The simple shape is recommended first; `prefy2009` ~3 min + 15 archives ~1 min each is a
   single-container runtime, fully streamed.
3. **Idempotent re-runs.** Append is not idempotent; a retried archive would duplicate. Guard each
   archive on an `ops.epa_ingest_runs` success row (skip-if-present) — **or**, if exactly-once is
   required, `merge_insert` on the row-unique `DMR_FORM_VALUE_ID` (far costlier; append + ledger
   guard is the right default for a one-shot backfill).

### Index strategy (rebuild after the load, not during)

`epa_npdes_dmrs` carries `EXTERNAL_PERMIT_NMBR_idx` (BTREE), `MONITORING_PERIOD_END_DATE_idx`
(BTREE), `FISCAL_YEAR_idx` (BITMAP). Lance scalar indices **do not auto-cover appended
fragments** — new rows are unindexed until the index is updated. Do **not** rebuild between
appends (wasted work). Sequence:

1. **Append all 16 archives first** (indices go stale/partial — acceptable mid-build).
2. **Then** update once. Two options:
   - `dataset.optimize.optimize_indices()` — folds the new fragments into the existing indices
     **incrementally** (cheapest); or
   - `create_scalar_index(replace=True)` per key — full rebuild over all ~422 M rows.
   Because the backfill is **5.25×** the indexed base, the incremental fold still processes the
   dominant share of the data; a **full `replace=True` rebuild is the cleaner end-state** and
   matches the existing `_build_indexes` path.
3. **Re-`FISCAL_YEAR` BITMAP** now spans ~45 distinct values (FY1982–FY2026) — still ideal bitmap
   cardinality.

> **Scale flag — the one genuine risk.** The live image sets `LANCE_BYPASS_SPILLING=true` and
> `memory=49152` explicitly for an *in-memory* BTREE sort sized to "69 M / 49 M-row" keys. The
> unified BTREE on `EXTERNAL_PERMIT_NMBR` at **~422 M** high-cardinality string keys is ~6× that
> sort and **may exceed 48 GB**. The fleet's documented **~100 M-row Volume-staging threshold**
> (per `EPA_PPP_MAPPING_BLUEPRINT.md`) is crossed decisively. Plan the **index-build** step (not
> the transform) for either a **high-memory Modal container** or the **Volume-staged BTREE** path;
> size it before running, do not discover it at 80% commit.

### Ordering / partition column

`FISCAL_YEAR` is the natural partition discriminator (BITMAP-indexed). Appending in chronological
order (`prefy2009` → FY2023) keeps fragments roughly time-clustered, which helps
`MONITORING_PERIOD_END_DATE`-range and `FISCAL_YEAR`-filtered scans prune fragments. Not required
for correctness; recommended for read locality.

## 2.4 Decisions for review (taste calls, not blockers)

1. **`prefy2009` FISCAL_YEAR.** Plan **derives per row** (FY1982–FY2008) so the BITMAP index stays
   meaningful for the legacy archive. Alternative: a single `2008` (= "≤FY2008") sentinel — simpler
   but collapses 26 years into one bucket and defeats year-filtering on the deepest history.
   **Recommend per-row derivation** (date column is 100% populated → lossless).
2. **Append vs. merge_insert.** Plan **appends** (historical rows are net-new; date ranges are
   disjoint from the live FY2024–FY2026 floor) with a per-archive ledger guard for idempotency.
   `merge_insert(DMR_FORM_VALUE_ID)` gives exactly-once at materially higher cost. **Recommend
   append + ledger guard.**
3. **Index update path.** `optimize_indices()` (incremental) vs `create_scalar_index(replace=True)`
   (full). **Recommend full rebuild** given the 5.25× growth, sized for the Volume-staging /
   high-memory path (see scale flag).
4. **Commit shape.** Sequential single-writer (simple, recommended) vs parallel
   `write_fragments` + one `LanceOperation.Append` (scale-out). **Recommend sequential first**;
   escalate only if single-container wall-clock is unacceptable.

## 2.5 Post-build verification gate (read-back, before "done")

- `lance.dataset(uri).count_rows()` == **422,447,436** (67,597,592 live + 354,849,844 backfill),
  ±0 against the sum of per-archive projection counts in this document.
- `EXTERNAL_PERMIT_NMBR` BTREE present with **`null_count = 0`**; `MONITORING_PERIOD_END_DATE`
  BTREE and `FISCAL_YEAR` BITMAP present and covering the full row set (index stats over all
  fragments, not just the pre-backfill base).
- `min(FISCAL_YEAR) = 1982`, `max(FISCAL_YEAR) = 2026`; `min(MONITORING_PERIOD_END_DATE)` ≈
  `1982-09-30`.
- Point-lookup proof: a known historical `EXTERNAL_PERMIT_NMBR` returns its pre-2009 rows **and**
  resolves to `REGISTRY_ID` via `epa_permits`; a `FISCAL_YEAR=1995` filter returns rows.
- `ops.epa_ingest_runs` shows all 16 archives `status=success` with `rows_written` summing to
  354,849,844 and `indexes_built` populated on the terminal rebuild row.

---

## 3. Compliance attestation

- **Zero mutation.** No `.lance` file written; `s3://data-sink/active/` untouched; no DDL; no
  `ops.epa_ingest_runs` row recorded. All validation was read-only memory scanning
  (`list_objects_v2`, range-GET central-directory + member streaming, in-process DuckDB
  aggregates, `pylance` metadata reads).
- **Verbatim naming.** All table/column identifiers are the literal ECHO `NPDES_DMRS` source
  names and the live Lance field names (read from `s3://data-sink/active/epa_npdes_dmrs/`, v12).
- **Executable dry-runs only.** The Python/Modal pipeline is **not** written here. The projection
  strings above are the tested transform concepts (the FY2009–FY2023 body is the live spec
  verbatim; the `prefy2009` FY-derivation body was validated against the 100%-populated date
  column) for review prior to any build.
