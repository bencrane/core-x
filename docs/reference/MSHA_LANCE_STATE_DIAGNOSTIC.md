# MSHA Lance/R2 — Schema & State Diagnostic

Clinical state diagnostic of the **live R2-backed Lance system of record** for MSHA (Mine
Safety & Health Administration) data. Companion to `MSHA_DATA_PROFILING_REPORT.md` (that
file profiles the *raw landing zone*, Directive 26; **this file profiles what actually
landed in the active SoR**, Directive 29 output).

- **Targets interrogated (Gen-3 SoR, `s3://data-sink/active/`):** `msha_mines`,
  `msha_corporate_history`, `msha_enforcement_ledger`.
- **Live evidence harness (non-mutating, zero writes):** `boto3` R2 object listing
  (physical footprint) · `psql ops.msha_ingest_runs` (terminal ingest ledger) · Modal
  `verify_datasets` read-back · `pylance 7.0.0` direct read of the committed Arrow schema +
  `count_rows(filter=…)` fill rates + `pyarrow.compute` distributions/sums.
- **As-of:** datasets written **2026-06-03 01:32–01:35 UTC** (single ingest run, id=1);
  source data current through **2026-05-28** (max `VIOLATION_ISSUE_DT`).
- **Attestation:** every figure below is a live read of the committed datasets, not a
  recon estimate.

---

## 0. Headline posture

| Verdict | Detail |
|---|---|
| **Ingest health** | ✅ One clean run (id=1), `status=success`, all 3 datasets `grain_ok=true`, **all 23 scalar indices `ok=true`**, zero row drop (lance_rows == spine_rows on every set). |
| **Coverage** | ⚠️ **3 of 20 source archives materialized** (5 source files). Identity + enforcement spine is live; **injury/Part-50, litigation, production/firmographic, and IH/exposure feeds are staged in landing but ABSENT from the SoR**. |
| **Entity keys** | ✅ `MINE_ID` 100% on all sets. ⚠️ `CONTROLLER_ID` only **93.25%** on the enforcement ledger (207,697 violations un-rolled to ultimate parent). |
| **Contact** | ⚠️ Postal locus (city/state/ZIP) ~99%, **street only 65.7%**, **CONTACT_TITLE is title-only (no person name)**, and **zero phone / email / website** anywhere. |
| **External linkage** | 🛑 **No EIN/DUNS/UEI/CAGE/NAICS/domain in any column.** MSHA universe is name+address-only AND currently landed in isolation — **no bridge column to `sos_normalized_master` / `companies` / PPP exists yet**. |
| **Distress signal** | ✅ 3.08 M citations, **$1.81 B proposed / $1.27 B paid**, **812,609 S&S (26.8%)**, **89,319 withdrawal/closure Orders**, gravity points on 2.42 M, current to within ~6 days. |
| **Nesting** | ✅ **Fully flat scalar — 0 STRUCT/LIST/MAP fields** across all 215 columns. |

---

## 1. Technical state — physical footprint, grain, indexing

### 1.1 Footprint & partitioning (live `boto3` listing of R2)

| Dataset | URI (`s3://data-sink/active/…`) | Rows | Cols | Objects | Total size | Data frags | Index dirs | Lance versions |
|---|---|--:|--:|--:|--:|--:|--:|--:|
| `msha_mines` | `msha_mines/` | **91,803** | 80 | 24 | 33.0 MiB | 1 (30.2 MiB) | 6 | 7 |
| `msha_corporate_history` | `msha_corporate_history/` | **168,809** | 15 | 21 | 15.0 MiB | 1 (10.8 MiB) | 5 | 6 |
| `msha_enforcement_ledger` | `msha_enforcement_ledger/` | **3,076,347** | 120 | 49 | 893.7 MiB | 3 (≤254.6 MiB) | 12 | 13 |

- **Partitioning:** flat single-level `data/` directory per dataset (no Hive/range
  partitioning) — fleet default for direct-R2 sets under the Giants threshold. Fragment
  count tracks `max_rows_per_file=1,048,576`: mines/corp = 1 fragment, enforcement = 3
  (3.08 M rows ÷ 1.05 M).
- **Version count = 1 overwrite + N index commits** (mines 1+6, corp 1+5, enforcement
  1+12) → **no rewrite churn, no `_deletions/`** (no row-level deletes on any set).
- **Total active footprint:** ~942 MiB across 94 objects.

### 1.2 Terminal ingest ledger (`ops.msha_ingest_runs`, id=1)

```
status=success · rows_total=3,336,959 · bytes_downloaded=232 MB · 144 s
2026-06-03 01:32:32Z → 01:34:56Z
msha_mines              spine 91,803  == lance 91,803  grain_ok=true  80 cols  (Mines.zip ⟕ AddressofRecord.zip)
msha_corporate_history  spine 168,809 == lance 168,809 grain_ok=true  15 cols  (ControllerOperatorHistory.zip)
msha_enforcement_ledger spine 3,076,347 == lance 3,076,347 grain_ok=true 120 cols (Violations.zip ⟕ AssessedViolations.zip)
indexes: 23/23 ok=true
```

### 1.3 Type handling — STRUCT / MAP / LIST verification

**Result: zero nested types.** The source is flat pipe-delimited text; the DuckDB
projection casts every field to a primitive, so the committed Arrow schema is 100% flat
scalar. Type histogram across all 215 columns:

| Arrow type | `msha_mines` | `msha_corporate_history` | `msha_enforcement_ledger` |
|---|--:|--:|--:|
| `string` | 72 | 10 | 65 |
| `date32[day]` | 4 | 4 | 23 |
| `int32` | 1 | 0 | 17 |
| `double` | 2 | 0 | 14 |
| `timestamp[us, tz=UTC]` | 1 (`ingested_at`) | 1 | 1 |
| **STRUCT / LIST / MAP** | **0** | **0** | **0** |

All IDs are `string` (leading-zero / alpha-prefix safety: `MINE_ID` 7-char zero-padded;
`VIOLATOR_ID`/`CONTROLLER_ID` alpha-prefixed). Money/geo/hours → `double`; counts/points →
`int32`; every event date → `date32`. Right-side join collisions are namespaced (`ADDR_*`
in mines, `ASMT_*` in enforcement); the join key is dropped right-side.

### 1.4 Index / query readiness — **23 scalar indices, all committed & verified**

| Dataset | BTREE (resolution / range) | BITMAP (categorical) |
|---|---|---|
| `msha_mines` | `MINE_ID`, `CURRENT_CONTROLLER_ID`, `CURRENT_OPERATOR_ID` | `COAL_METAL_IND`, `STATE`, `CURRENT_MINE_STATUS` |
| `msha_corporate_history` | `CONTROLLER_ID`, `OPERATOR_ID`, `MINE_ID` | `CONTROLLER_TYPE`, `COAL_METAL_IND` |
| `msha_enforcement_ledger` | `MINE_ID`, `VIOLATOR_ID`, `VIOLATION_NO`, `CONTROLLER_ID`, `EVENT_NO`, `ASSESS_CASE_NO`, `VIOLATION_ISSUE_DT`, `PROPOSED_PENALTY_AMT` | `SIG_SUB`, `CIT_ORD_SAFE`, `VIOLATOR_TYPE_CD`, `COAL_METAL_IND` |

Every entity key the directive named — **Mine ID, Operator ID, Controller ID** — is
BTREE-indexed where it exists. Severity/temporal range-scan columns (`VIOLATION_ISSUE_DT`,
`PROPOSED_PENALTY_AMT`) and high-selectivity distress flags (`SIG_SUB`, `CIT_ORD_SAFE`) are
indexed → point-lookup and range/equality filters are index-backed today.

---

## 2. Entity keys & resolution map

### 2.1 Key inventory & fill (live)

| Entity key | Dataset(s) | Type | Fill | Null |
|---|---|---|--:|--:|
| **`MINE_ID`** (physical asset, universal spine) | all 3 | string PK | **100.00%** all sets | 0 |
| `CURRENT_OPERATOR_ID` | mines | string | 98.96% | 951 |
| `CURRENT_CONTROLLER_ID` | mines | string | 98.91% | 1,005 |
| `OPERATOR_ID` | corp_history | string | 100.00% | 0 |
| `CONTROLLER_ID` | corp_history | string | 100.00% | 0 |
| `CONTROLLER_ID` | enforcement | string | **93.25%** | **207,697** |
| `VIOLATOR_ID` (operator **or** contractor) | enforcement | string | 99.99% | 182 |
| `CONTRACTOR_ID` | enforcement | string | 6.69% (contractor rows only) | — |
| `VIOLATION_NO` / `EVENT_NO` | enforcement | string | 100.00% | 0 |
| `ASSESS_CASE_NO` | enforcement | string | 97.81% | 67,497 |
| `DOCKET_NO` (litigation) | enforcement | string | 6.25% | — |

> ⚠️ **`OPERATOR_ID` is NOT present in the enforcement ledger.** `Violations` attributes
> via `VIOLATOR_ID` (+`VIOLATOR_TYPE_CD` ∈ {Operator, Contractor}) and `CONTROLLER_ID`,
> not `OPERATOR_ID`. Site-operator attribution of a violation must route
> `VIOLATOR_ID → ControllerOperatorHistory`.

### 2.2 Identity cardinality (live distinct counts)

| Quantity | Value |
|---|--:|
| Distinct `MINE_ID` (mines master) | 91,803 |
| Distinct `MINE_ID` covered by corporate history | 90,699 (**1,104 mines have no controller-history row**) |
| Distinct `CONTROLLER_ID` (history) | **54,120** |
| Distinct `OPERATOR_ID` (history) | **67,787** |
| `ControllerOperatorHistory` SCD rows | 168,809 — `COMPANY` **93,545 (55.4%)** / `PERSON` **75,264 (44.6%)** |
| Current controller links (`CONTROLLER_END_DT IS NULL`) | 131,464 (77.88%) |
| Current operator links (`OPERATOR_END_DT IS NULL`) | 115,057 (68.16%) |

> ⚠️ **44.6% of corporate-history rows are `CONTROLLER_TYPE='PERSON'`** (sole proprietors)
> — structurally un-joinable to any B2B company table. Filter to `COMPANY` before name
> resolution.

### 2.3 External-key void (confirmed against live committed schema)

Scanned all 215 committed columns across the 3 datasets: **no EIN / FEIN / Tax ID, no
DUNS / UEI / CAGE / SAM registration, no NAICS, no domain / website / email / phone.** The
only sector code is legacy **SIC** (`PRIMARY_SIC_CD`, 99.67% / `ADDR_PRIMARY_SIC_CD`,
99.35%). Resolution is **name + state + mailing address only** — identical to the CA UCC
name-indexed reality.

> 🛑 **Linkage gap #1 (structural):** per Directive 29, the MSHA universe is landed in
> **isolation on native keys** — there is **no `normalized_legal_name` / `name_norm` /
> bridge column** to `sos_normalized_master`, `companies`, PPP, or SBA in any of the three
> datasets. Today the MSHA universe is fully queryable **on its own keys** but is **not
> joined to the rest of the GTM graph.** Bridging (the recon's `name_norm` →
> `sos_normalized_master` recipe, blocked by state, with `AddressOfRecord` ZIP as
> geo-tiebreak) is the unbuilt downstream step.

---

## 3. Contact vectors & gaps

All contact data lives in `msha_mines` (the `Mines ⟕ AddressOfRecord` join). Live fill:

| Contact vector | Column | Fill | Null | Note |
|---|---|--:|--:|---|
| Operator legal name (mailing) | `BUSINESS_NAME` | 98.91% | 1,002 | best org-name vector |
| Current operator name | `CURRENT_OPERATOR_NAME` | 100.00% | 0 | denormalized current |
| Current controller (parent) name | `CURRENT_CONTROLLER_NAME` | 98.91% | 1,005 | strip `"(Form:…)"` lineage |
| Mailing city | `CITY` | 98.98% | 940 | |
| Mailing state | `STATE_ABBR` / `ADDR_STATE` | 98.96% | 958 | |
| Mailing ZIP | `ZIP_CD` | 98.95% | 963 | |
| **Street address** | `STREET` | **65.70%** | **31,484** | ⚠️ deliverable street for ⅔ only |
| PO Box | `PO_BOX` | 30.88% | 63,453 | partial fallback for missing street |
| Contact **title** | `CONTACT_TITLE` | 85.94% | 12,904 | ⚠️ **title only — no person name** |
| Mine geocode | `LATITUDE`/`LONGITUDE` | 51.71% | 44,334 | mine site coords, not corporate HQ |
| Headcount | `NO_EMPLOYEES` | 56.73% | 39,719 | firmographic size proxy |

> ⚠️ **Contact gap (critical for outreach):** there is **no person-name field** (only a
> job title), and **no phone, email, or website in any MSHA column.** Direct outreach is
> impossible from the SoR alone — MSHA yields {firm name + mailing address (~99% city/state/ZIP,
> 66% street)}. Person/email/phone require downstream enrichment (the fleet's
> blitz / icypeas / parallel pipelines), keyed on the resolved firm name + address.

---

## 4. Distress & emergency indicator audit

### 4.1 Current mine status (`msha_mines.CURRENT_MINE_STATUS`, 100% filled)

| Status | Count | % | Cohort |
|---|--:|--:|---|
| Abandoned | 69,196 | 75.37% | dead |
| Abandoned and Sealed | 8,853 | 9.64% | dead |
| Active | 6,634 | 7.23% | **live** |
| Intermittent | 5,648 | 6.15% | **live** |
| Temporarily Idled | 765 | 0.83% | **live (distress-adjacent)** |
| NonProducing | 373 | 0.41% | live |
| New Mine | 334 | 0.36% | live |

**≈ 13,754 live mines (14.98%)** vs 78,049 abandoned (85.02%). `Temporarily Idled` +
`NonProducing` are the operational-distress micro-cohorts. Sector split: **Metal/Nonmetal
56,081 (61.1%) · Coal 35,722 (38.9%).**

### 4.2 Enforcement distress (`msha_enforcement_ledger`, 3,076,347 citations)

| Signal | Live value |
|---|---|
| **Significant & Substantial** (`SIG_SUB`) | **Y = 812,609 (26.8% of populated)**, N = 2,215,633 (fill 98.44%) |
| **Citation / Order / Safeguard** (`CIT_ORD_SAFE`) | Citation 2,978,912 (96.83%) · **Order 89,319 (2.90%)** · Safeguard 8,105 (0.26%) |
| **Elevated enforcement** (`SECTION_OF_ACT`, sparse **by design** — 0.56% fill flags the special-action tail) | **§107(a) imminent-danger = 818** · §103(k) control order = 1,421 · §104(b) failure-to-abate = 426 · §104(d)(1)+(2) unwarrantable-failure = 129 |
| **Violator type** (`VIOLATOR_TYPE_CD`) | Operator 2,870,559 (93.3%) · Contractor 205,788 (6.7%) |
| **Proposed penalty $** (`PROPOSED_PENALTY[_AMT]`) | **Σ $1,813,039,881** |
| **Paid penalty $** (`PAID_PROPOSED_PENALTY_AMT`) | **Σ $1,266,703,932** (collection ratio **69.9%**) |
| **Gravity / negligence scoring** | `PENALTY_POINTS` 78.77% fill; `GRAVITY_{PERSONS,INJURY,LIKELIHOOD}_POINTS`, `NEGLIGENCE_POINTS`, `LIKELIHOOD`, `INJ_ILLNESS`, `NEGLIGENCE` all present (~98% on the violation-side ordinals) |
| **Repeat / POV proxies** | `VIOLATOR_VIOLATION_CNT`, `VIOLATOR_REPEATED_VIOL_CNT`, `EXCESSIVE_HISTORY_IND` (97.81% fill) |
| **Contest flag** | `CONTESTED_IND` 100% · `DOCKET_NO` 6.25% (litigated subset) |

> **Pattern of Violations (POV):** no native MSHA POV-status column exists in the SoR. POV
> must be **derived** from S&S rate + `VIOLATOR_REPEATED_VIOL_CNT` + `EXCESSIVE_HISTORY_IND`
> + gravity density. The building blocks are present and indexed; the determination is not
> ingested.

### 4.3 Recency — real-time distress-trigger viability

| Window | Violations issued | Assessments issued |
|---|--:|--:|
| ≥ 2024-01-01 | 211,817 (6.89%) | 195,824 |
| ≥ 2025-01-01 | 117,989 (3.84%) | — |
| YTD 2026 | 33,969 (1.10%) | — |
| Full range | 1994-09-09 → **2026-05-28** | 1994-09-09 → 2026-05-05 |

Data current to within **~6 days** of ingest (snapshot 2026-06-03, data through
2026-05-28). Viable as a near-real-time enforcement-trigger feed (e.g. *new S&S Order on a
live operator* → GTM event) on refresh.

### 4.4 Distress signals NOT in the SoR (staged in landing, not materialized)

The directive explicitly asked for **Accident / Injury / Illness (Part 50)** records.
**These are not in the active SoR.** Only the *violation-level* injury proxy
(`INJ_ILLNESS`, `NO_AFFECTED`, gravity points) is present. 15 of 20 source archives
(~11 M+ rows) remain in `s3://data-sink/landing/msha/` un-ingested:

| Un-ingested feed | Rows (recon) | Distress relevance |
|---|--:|---|
| `Accidents` | 273,065 | 🛑 **Part-50 injury/fatality** — directive-named, absent |
| `ContestedViolations` | 448,158 | litigation / contest onset |
| `CivilPenaltyDocketsDecisions` | 479,439 | docket outcomes (settle/vacate) |
| `Conferences` | 161,623 | pre-penalty conference |
| `Inspections` | 1,147,232 | site-visit cadence |
| `MinesProd{Quarterly,Yearly}` | 2,714,840 / 657,546 | production/employment firmographics |
| `ContractorProd{Quarterly,Yearly}` | 1,350,534 / 280,142 | contractor activity |
| `CoalDust / PersonalHealth / Noise / Quartz / Area Samples` | 2,985,614 / 310,908 / 274,645 / 167,238 / 8,368 | IH / respirable-dust / silica exposure |
| `OrdersIssued` (107a report) | ~3,829 | redundant with `CIT_ORD_SAFE='Order'` |

---

## 5. Critical gaps — consolidated

| # | Gap | Severity | Detail / remediation |
|---|---|---|---|
| 1 | **No bridge to our universe** | 🛑 High | MSHA landed in isolation; no `name_norm`/`normalized_legal_name` column. Build the operator/controller entity bridge (`name_norm` → `sos_normalized_master`, state-blocked, ZIP tiebreak). |
| 2 | **Part-50 Accident/Injury feed absent** | 🛑 High | `Accidents` (273,065) not materialized; only violation-level injury proxy exists. Directive-named distress vector missing. |
| 3 | **No direct contact channel** | ⚠️ Med-High | Zero phone/email/website; `CONTACT_TITLE` is title-only (no name). Outreach requires enrichment on {firm name + address}. |
| 4 | **`STREET` 65.7% fill** | ⚠️ Med | ⅓ of mines lack a deliverable street (PO Box covers part); city/state/ZIP ~99%. |
| 5 | **`CONTROLLER_ID` 93.25% on enforcement** | ⚠️ Med | 207,697 violations cannot roll up to ultimate parent; `VIOLATOR_ID`/`MINE_ID` attribution intact (99.99%/100%). |
| 6 | **44.6% of controller rows are PERSON** | ⚠️ Med | Sole proprietors — unbridgeable to B2B tables; filter `COMPANY` before resolution. |
| 7 | **Litigation/production/IH feeds absent** | ◽ Low-Med | Contest, docket, conference, prod, and exposure samples staged but not ingested. |

---

## 6. Appendix — committed column lists (compact)

**`msha_mines`** (80) — keys/contact/status subset: `MINE_ID`*, `CURRENT_MINE_NAME`,
`CURRENT_MINE_STATUS`, `CURRENT_STATUS_DT`, `CURRENT_CONTROLLER_ID`*,
`CURRENT_CONTROLLER_NAME`, `CURRENT_OPERATOR_ID`*, `CURRENT_OPERATOR_NAME`, `COAL_METAL_IND`†,
`STATE`†, `BUSINESS_NAME`, `CONTACT_TITLE`, `STREET`, `PO_BOX`, `CITY`, `STATE_ABBR`,
`ZIP_CD`, `ADDR_STATE`, `PRIMARY_SIC_CD`, `NO_EMPLOYEES`, `LATITUDE`, `LONGITUDE`, … (+ mining
ops attrs) … `source_file`, `ingested_at`.

**`msha_corporate_history`** (15): `CONTROLLER_ID`*, `CONTROLLER_NAME`,
`CONTROLLER_START_DT`, `CONTROLLER_END_DT`, `CONTROLLER_TYPE`†, `COAL_METAL_IND`†,
`MINE_ID`*, `MINE_NAME`, `MINE_STATUS`, `OPERATOR_ID`*, `OPERATOR_NAME`, `OPERATOR_START_DT`,
`OPERATOR_END_DT`, `source_file`, `ingested_at`.

**`msha_enforcement_ledger`** (120) — keys/signal subset: `EVENT_NO`*, `VIOLATION_NO`*,
`VIOLATION_ISSUE_DT`*, `MINE_ID`*, `MINE_NAME`, `VIOLATOR_ID`*, `VIOLATOR_NAME`,
`VIOLATOR_TYPE_CD`†, `CONTROLLER_ID`*, `CONTROLLER_NAME`, `CONTRACTOR_ID`, `COAL_METAL_IND`†,
`SIG_SUB`†, `CIT_ORD_SAFE`†, `SECTION_OF_ACT`, `LIKELIHOOD`, `INJ_ILLNESS`, `NO_AFFECTED`,
`NEGLIGENCE`, `PROPOSED_PENALTY`, `AMOUNT_DUE`, `AMOUNT_PAID`, `DOCKET_NO`, `CONTESTED_IND`,
`ASSESS_CASE_NO`*, `PROPOSED_PENALTY_AMT`*, `PAID_PROPOSED_PENALTY_AMT`, `PENALTY_POINTS`,
`GRAVITY_{PERSONS,INJURY,LIKELIHOOD}_POINTS`, `NEGLIGENCE_POINTS`,
`VIOLATOR_REPEATED_VIOL_CNT`, `EXCESSIVE_HISTORY_IND`, `SIZE_OF_CONTROLLING_ENTITY`,
`ASMT_*` (namespaced assessment-side dups), … `source_file`, `ingested_at`.

`*` = BTREE-indexed · `†` = BITMAP-indexed.
