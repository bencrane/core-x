# MSHA Legal-Entity & Schema-Mapping Diagnostic

Read-only reconnaissance of the **MSHA (Mine Safety & Health Administration) landing
archive vs. the live Lance system of record**, scoped to one question: *did the
materialization drop legal corporate identities, operating histories, or controller /
contractor names while landing event data?* — the architectural gap pattern surfaced in
the EPA CAA/RCRA pipeline (`EPA_CAA_RCRA_LEGAL_ENTITY_DIAGNOSTIC.md`).

- **Landing interrogated:** `s3://data-sink/landing/msha/` — 20 `*.zip` + `.keep` (live
  `boto3` listing).
- **Active SoR interrogated:** `s3://data-sink/active/msha_mines`,
  `…/msha_corporate_history`, `…/msha_enforcement_ledger` (live `pylance 7.0.0` schema +
  `count_rows(filter=…)`).
- **Evidence harness (non-mutating, zero writes):** range-GET + raw-DEFLATE header/sample
  scan of every landing archive (the 114 MB `Violations` giant is **never** pulled whole —
  only its first 6 MB compressed); `boto3` object inventory; `pylance` committed-schema
  read + indexed-key fill rates; full-file distinct-contractor extraction on the two small
  `ContractorProd` archives.
- **As-of:** probe run **2026-06-05**; active datasets written 2026-06-03 01:32–01:35 UTC
  (ingest run id=1); source current through **2026-05-28** (max `VIOLATION_ISSUE_DT`).
- **Attestation:** every figure below is a live read of the committed datasets or the
  landing bytes — not a recon estimate. **Zero data-plane mutation**: no `.lance` writes,
  no DDL, no `ops.*` rows.

---

## 0. Headline verdict

| Question | Verdict |
|---|---|
| **Coverage gap** | 🛑 **15 of 20 landing archives have ZERO active representation.** Only 5 source files (→ 3 datasets) are materialized: `Mines`+`AddressofRecord`→`msha_mines`, `ControllerOperatorHistory`→`msha_corporate_history`, `Violations`+`AssessedViolations`→`msha_enforcement_ledger`. |
| **Operator / controller names** | ✅ **Present & indexed.** Operator legal names (`BUSINESS_NAME`, `CURRENT_OPERATOR_NAME`, `OPERATOR_NAME`, `VIOLATOR_NAME`) and controller identities (`CURRENT_CONTROLLER_NAME`, `CONTROLLER_NAME`) survived into all three datasets. The operating-history SCD landed intact (168,809 rows). |
| **Contractor corporate names** | 🛑 **Orphaned.** The dedicated `CONTRACTOR_NAME` registry exists in exactly two archives — `ContractorProdQuarterly` + `ContractorProdYearly` — **both un-ingested**. In active, a contractor's name is reachable **only** as `VIOLATOR_NAME` on the 6.69% violation slice. **38,653** distinct named contractors live in landing; only **21,966 (56.8%)** surface anywhere in active; **22,614 (58.5%) are entirely absent.** |
| **Schema verbatim-fidelity** | ✅ **Native names preserved verbatim** (UPPERCASE, native punctuation). Zero snake_case transforms, zero ID renames. Only mutations: 2 synthetic lowercase provenance cols (`source_file`, `ingested_at`) + collision-namespacing prefixes (`ADDR_`×4, `ASMT_`×9). **No `normalized_legal_name` / `name_norm` injected** — the opposite posture from EPA (Directive-29 no-bridge guardrail). |
| **External keys** | 🛑 No EIN/DUNS/UEI/CAGE/NAICS/domain anywhere in landing **or** active. Resolution is name + state + mailing-address only. |
| **Multi-era drift** | ✅ **None structural.** Full-snapshot overwrite ⇒ one Arrow schema for all 3.08 M rows; no column can vanish or retype across eras. Data-level fill of name keys is era-stable (`VIOLATOR_NAME`/`MINE_NAME` ~100% from <2000 through 2026). |

---

## PHASE 1 — Landing archive schema map (live header + sample scan)

### 1.1 The two name namespaces

MSHA carries identity in **two disjoint namespaces**, neither bearing any key to our
universe (no EIN/DUNS/UEI/NAICS/domain — confirmed across all 20 headers):

```
PHYSICAL ASSET                          CORPORATE IDENTITY (alpha-prefixed VARCHAR ids)
MINE_ID (7-char zero-padded)            CONTROLLER_ID  parent/ultimate liable   (e.g. 0041044, M00024)
  spine of 16 of 20 files               OPERATOR_ID    site operator            (e.g. L13586)
                                        CONTRACTOR_ID  third-party contractor   (e.g. 1AD, MPH)
                                        VIOLATOR_ID    = OPERATOR_ID or CONTRACTOR_ID per VIOLATOR_TYPE_CD
```

`ControllerOperatorHistory` is the **only** file binding the corporate namespace to the
physical asset over time (SCD with start/end dates).

### 1.2 Directive-named archives — verbatim columns & the entity-name carriers

| Archive | Member | Cols | Primary key(s) | Legal/operating-name fields (verbatim) | Active? |
|---|---|--:|---|---|:--:|
| `Mines.zip` | `Mines.txt` | 59 | **`MINE_ID`** | `CURRENT_MINE_NAME`, `CURRENT_CONTROLLER_NAME`(+`_ID`), `CURRENT_OPERATOR_NAME`(+`_ID`) | ✅ |
| `AddressofRecord.zip` | `AddressOfRecord.txt` | 20 | **`MINE_ID`** (1:1) | `BUSINESS_NAME` (operator mailing legal name), `MINE_NAME`, `CONTACT_TITLE` (title only) | ✅ |
| `ControllerOperatorHistory.zip` | `…History.txt` | 13 | **(`CONTROLLER_ID`,`OPERATOR_ID`,`MINE_ID`,`CONTROLLER_START_DT`)** | `CONTROLLER_NAME`, `OPERATOR_NAME`, `MINE_NAME` + `CONTROLLER_TYPE`(COMPANY\|PERSON) | ✅ |
| `ContractorProdQuarterly.zip` | `…Quarterly.txt` | 12 | **`CONTRACTOR_ID`**·period | **`CONTRACTOR_NAME`** + `AVG_EMPLOYEE_CNT`, `HOURS_WORKED`, `COAL_PRODUCTION` | 🛑 |
| `ContractorProdYearly.zip` | `…Yearly.txt` | 10 | **`CONTRACTOR_ID`**·yr | **`CONTRACTOR_NAME`** + `AVG_EMPLOYEE_CNT`, `ANNUAL_HOURS`, `ANNUAL_COAL_PRODUCTION` | 🛑 |
| `Violations.zip` | `Violations.txt` | 61 | **`VIOLATION_NO`** (event `EVENT_NO`) | `CONTROLLER_NAME`, `VIOLATOR_NAME`(+`VIOLATOR_TYPE_CD`), `MINE_NAME` + `CONTRACTOR_ID` (no name) | ✅ |
| `Accidents.zip` | `Accidents.txt` | 57 | `DOCUMENT_NO` | `CONTROLLER_NAME`, `OPERATOR_NAME` (denormalized) + `CONTRACTOR_ID` (no name) | 🛑 |

**Live samples proving the name fields carry real corporate identities:**

```
ContractorProdYearly : "1AD"|"O K Combs Trucking"|...   "1AF"|"Ac & S Inc"|...
ControllerOperator…  : "0040459"|"Cassia County-ID"|...|"COMPANY"|...|"Cassia County Roads"   (OPERATOR_NAME)
Mines                : "0100003"|...|"0041044"|"Lhoist Group"|"L13586"|"Lhoist North America of Alabama, LLC"
Violations           : ...|"0041044"|"Lhoist Group"|"L13586"|"Lhoist North America of Alabama, LLC"|"Operator"|...
Accidents            : "0100009"|"M00024"|"Legacy Vulcan Corp (Form:Vulcan Materials Co)"|"L16168"|"Vulcan Construction Materials, LLC"
```

> **Denormalization finding:** `Violations` and `Accidents` are **name-denormalized** — they
> carry `CONTROLLER_NAME` / `OPERATOR_NAME` / `VIOLATOR_NAME` inline, **not** bare IDs. They
> do **not** require a link back to the master files to recover an entity name (though
> `CONTRACTOR_ID` is name-less in both — the contractor name must come from `ContractorProd*`).
> `CONTROLLER_NAME` embeds prior-name lineage: `Legacy Vulcan Corp (Form:Vulcan Materials Co)`.

### 1.3 Contractor-name carriers — the syndicate target

`CONTRACTOR_NAME` (the third-party heavy-machinery / mobilization contractor's corporate
name) appears as a **dedicated verbatim field in exactly two archives**, both orphaned:

| Carrier of contractor corporate name | Field | Status |
|---|---|:--:|
| `ContractorProdQuarterly` | **`CONTRACTOR_NAME`** (keyed `CONTRACTOR_ID`) | 🛑 un-ingested |
| `ContractorProdYearly` | **`CONTRACTOR_NAME`** (keyed `CONTRACTOR_ID`) | 🛑 un-ingested |
| `Violations` / `AssessedViolations` | `VIOLATOR_NAME` **only when** `VIOLATOR_TYPE_CD='Contractor'` | ✅ active (event slice) |
| `Accidents`, `*Samples`, `AreaSamples` | `CONTRACTOR_ID` **only — no name** | 🛑 un-ingested |

Every other CONTRACTOR_ID-bearing file (`Accidents`, `NoiseSamples`, `PersonalHealthSamples`,
`AreaSamples`) carries the **id with no name** — they cannot recover a contractor identity
without the orphaned `ContractorProd*` registry or the violation slice.

---

## PHASE 2 — Verification & posture report

### 2.1 The Coverage Gap — 15 of 20 archives unrepresented

| Status | Count | Archives |
|---|--:|---|
| **Materialized** | 5 | `Mines`, `AddressofRecord`, `ControllerOperatorHistory`, `Violations`, `AssessedViolations` |
| **Orphaned (zero active rows)** | 15 | `Accidents`, `Inspections`, `ContestedViolations`, `CivilPenaltyDocketsDecisions`, `Conferences`, `OrdersIssued`, **`ContractorProdQuarterly`**, **`ContractorProdYearly`**, `MinesProdQuarterly`, `MinesProdYearly`, `CoalDustSamples`, `PersonalHealthSamples`, `NoiseSamples`, `QuartzSamples`, `AreaSamples` |

The active SoR is an **identity + enforcement spine only**. Absent classes: Part-50
injury/fatality (`Accidents`, 273 K), litigation (`ContestedViolations` 448 K,
`CivilPenaltyDocketsDecisions` 479 K, `Conferences` 162 K), site-visit cadence
(`Inspections` 1.15 M), production/employment firmographics (`MinesProd*` 3.37 M,
`ContractorProd*` 1.63 M), and IH/exposure sampling (`CoalDust`/`Quartz`/`Noise`/
`PersonalHealth`/`Area`, ~3.75 M). The two `ContractorProd*` files are the **only**
firmographic substrate that names the contractor universe.

### 2.2 The Orphaned-Name Verdict

**Operator & controller identities — NOT orphaned.** Live fill in active:

| Identity | Active column(s) | Fill |
|---|---|--:|
| Operator legal name | `BUSINESS_NAME` (mines) / `CURRENT_OPERATOR_NAME` (mines) / `OPERATOR_NAME` (corp_history) / `VIOLATOR_NAME` (ledger) | 98.9% / 100% / 100% / 99.99% |
| Controller (parent) name | `CURRENT_CONTROLLER_NAME` (mines) / `CONTROLLER_NAME` (corp_history, ledger) | 98.9% / 100% / 93.2% |
| Operating-history lineage | `msha_corporate_history` SCD | 168,809 rows intact |

**Contractor corporate names — ORPHANED, quantified.** The contractor name is **heavily
restricted to a minor event slice** exactly as hypothesized:

| Measure | Value |
|---|--:|
| Distinct named contractors in landing registry (`ContractorProd*` union) | **38,653** |
| Distinct `CONTRACTOR_ID` surfacing in active ledger | **21,966** (56.8%) |
| Active contractor-name rows (`VIOLATOR_TYPE_CD='Contractor'` ∧ `VIOLATOR_NAME` not null) | **205,773** = **6.69%** of the 3.08 M ledger |
| Contractors in registry with **zero** active representation | **22,614 (58.5%)** |
| Contractor firmographics (`HOURS_WORKED`, `AVG_EMPLOYEE_CNT`, production) in active | **0 — entirely absent** |

> 🛑 A contractor that performed work but was never cited (58.5% of the named universe) has
> **no name, no id, and no firmographic footprint** in the SoR. Even the cited 41% are
> present only as a violation-event string (`VIOLATOR_NAME`), never as a first-class
> contractor entity with activity history. The dedicated `CONTRACTOR_NAME` registry is fully
> trapped in landing.

### 2.3 Schema & Naming Verbatim Audit

**Verdict: verbatim-native, minimally mutated.** The materializer (`_q()` double-quotes
each source identifier; the projection aliases `AS "NATIVE_NAME"`) preserved MSHA's exact
column names — UPPERCASE, native underscores, no casing/punctuation drift. Live confirmation
across all 215 committed columns:

| Mutation class | `msha_mines` | `msha_corporate_history` | `msha_enforcement_ledger` |
|---|---|---|---|
| Native verbatim columns | 74 | 13 | 109 |
| Synthetic provenance (lowercase) | `source_file`, `ingested_at` | `source_file`, `ingested_at` | `source_file`, `ingested_at` |
| Collision-namespace prefixes | `ADDR_`×4 (`ADDR_NEAREST_TOWN`, `ADDR_STATE`, `ADDR_PRIMARY_SIC_CD`, `ADDR_COAL_METAL_IND`) | none | `ASMT_`×9 (`ASMT_MINE_ID`, `ASMT_VIOLATOR_ID`, `ASMT_VIOLATOR_NAME`, …) |
| Semantic renames (e.g. ID→key) | **none** | **none** | **none** |
| Injected normalized-name column | **none** | **none** | **none** |

- The only field-level alteration is **right-side collision namespacing** on the two joins:
  `Mines ⟕ AddressofRecord` prefixes the 4 overlapping `AddressofRecord` columns with
  `ADDR_`; `Violations ⟕ AssessedViolations` prefixes the 9 overlapping `AssessedViolations`
  columns with `ASMT_`. The join key is dropped right-side. Left/spine columns keep native
  names unchanged.
- **No `normalized_legal_name`, `name_norm`, or `legal_name_base`** column exists in any
  MSHA dataset — a deliberate Directive-29 no-bridge stance. **This is the inverse of the
  EPA pipeline**, which injected `normalized_legal_name (core.name_norm)` and namespaced
  `ID_NUMBER → RCRA_ID`. MSHA is landed in isolation on native keys; name resolution to
  `sos_normalized_master` is an unbuilt downstream step.

### 2.4 Data-Level Drift Check (multi-era)

**Structural drift is impossible by construction:** each dataset is a single
`mode="overwrite"` commit → one Arrow schema governs all rows (3.08 M for the ledger). No
Hive/range partitioning, no per-era fragments with independent schemas. A column cannot
appear, vanish, or change type between a 1994 row and a 2026 row.

**Data-level era fill** on the enforcement ledger (the only multi-decade set) confirms the
name keys are era-stable, not silently dropping on legacy rows:

| Era (`VIOLATION_ISSUE_DT`) | Rows | `VIOLATOR_NAME` | `CONTROLLER_NAME` | `MINE_NAME` | `VIOLATOR_ID` |
|---|--:|--:|--:|--:|--:|
| `< 2000` | 43 | 100% | 81.4% | 100% | 100% |
| `2010–2014` | 700,321 | 100% | 93.1% | 100% | 100% |
| `≥ 2026` | 33,969 | 99.6% | 93.2% | 100% | 99.6% |

`CONTROLLER_NAME`'s ~93% is a **fill characteristic** (un-rolled ultimate parent; 207,697
nulls dataset-wide), uniform across eras — not drift. Effective ledger coverage begins
~2000 (only 43 pre-2000 rows); max date 2026-05-28. No type or column variance detected.

### 2.5 Structural join paths (active SoR + recovery routes)

```
msha_mines (91,803)        ──MINE_ID──►  msha_enforcement_ledger (3,076,347)
  MINE_ID (BTREE)                          MINE_ID, VIOLATOR_ID, CONTROLLER_ID, VIOLATION_NO,
  CURRENT_CONTROLLER_ID (BTREE)            EVENT_NO, ASSESS_CASE_NO (BTREE) · VIOLATION_ISSUE_DT,
  CURRENT_OPERATOR_ID (BTREE)              PROPOSED_PENALTY_AMT (BTREE) · SIG_SUB, CIT_ORD_SAFE,
       │                                   VIOLATOR_TYPE_CD, COAL_METAL_IND (BITMAP)
       │ CURRENT_CONTROLLER_ID / CURRENT_OPERATOR_ID
       ▼
msha_corporate_history (168,809)   ◄──VIOLATOR_ID / CONTROLLER_ID──  (site-operator attribution of a violation
  CONTROLLER_ID, OPERATOR_ID, MINE_ID (BTREE)                          routes through here — OPERATOR_ID is NOT
  CONTROLLER_TYPE, COAL_METAL_IND (BITMAP)                             on the ledger)

CONTRACTOR NAME RECOVERY (today, lossy):  led.VIOLATOR_ID  where VIOLATOR_TYPE_CD='Contractor'  → VIOLATOR_NAME   (21,966 of 38,653)
CONTRACTOR NAME RECOVERY (full, blocked): ContractorProd*.CONTRACTOR_ID → CONTRACTOR_NAME                          (orphaned in landing)
BRIDGE TO UNIVERSE (unbuilt):             {OPERATOR_NAME|CONTROLLER_NAME} → core.name_norm → sos_normalized_master  (name+state, no native key)
```

- **`OPERATOR_ID` is absent from the enforcement ledger** — violations attribute via
  `VIOLATOR_ID`(+`VIOLATOR_TYPE_CD`) and `CONTROLLER_ID`. Site-operator rollup must hop
  `VIOLATOR_ID → msha_corporate_history`.
- **44.6% of `msha_corporate_history` rows are `CONTROLLER_TYPE='PERSON'`** (sole
  proprietors) — filter to `COMPANY` before any B2B name resolution.

---

## 3. Remediation targets (consolidated, not built here)

| # | Gap | Sev | Action |
|---|---|---|---|
| 1 | Contractor name registry orphaned | 🛑 | Materialize `ContractorProd{Quarterly,Yearly}` → `msha_contractors` (BTREE `CONTRACTOR_ID`); recovers 38,653 named contractors + firmographics (58.5% currently invisible). |
| 2 | Part-50 injury feed absent | 🛑 | Materialize `Accidents` (273 K; denormalized `CONTROLLER_NAME`/`OPERATOR_NAME`, keyed `DOCUMENT_NO`). |
| 3 | No bridge to our universe | 🛑 | Build operator/controller entity bridge: `name_norm(OPERATOR_NAME\|CONTROLLER_NAME)` → `sos_normalized_master`, state-blocked, `AddressOfRecord` ZIP tiebreak. |
| 4 | Litigation / firmographic / IH feeds absent | ◽ | `ContestedViolations`, `CivilPenaltyDocketsDecisions`, `Conferences`, `MinesProd*`, `Inspections`, `*Samples` staged but un-ingested. |

---

## 4. Appendix — live evidence

- **Active datasets (pylance):** `msha_mines` 91,803×80 (6 idx) · `msha_corporate_history`
  168,809×15 (5 idx) · `msha_enforcement_ledger` 3,076,347×120 (12 idx). 23 scalar indices
  total, all committed.
- **Landing (boto3):** 20 archives, ~590 MB compressed. Materialized members:
  `Mines.txt`(59) `AddressOfRecord.txt`(20) `ControllerOperatorHistory.txt`(13)
  `Violations.txt`(61) `AssessedViolations.txt`(58).
- **Wire format (all archives):** single-member ZIP/DEFLATE, pipe-delimited, unquoted
  header, quote-wrapped values with **unescaped interior quotes** (`quote=''` mandatory),
  **Windows-1252** (transcode→UTF-8 before DuckDB). `CoalDustSamples` / `OrdersIssued`
  deviate (bare-numeric / `sep=|` Excel-report header on line 4).
- **Harness:** `/tmp/msha_probe_landing.py`, `/tmp/msha_probe_active.py`,
  `/tmp/msha_probe_contractor.py` (read-only; Doppler-injected R2 creds).
