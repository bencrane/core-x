# EPA Unmaterialized Planes — Permits, Co-Permittees, Owner/Operator & the Enforcement/Defendant Matrix

Read-only inventory + schema diagnostic of the EPA data plane, scoped to three planes the active
`REGISTRY_ID` hub does **not** carry: **(1)** permit-level legal entity names, co-permittees, and
owner/operator history; **(2)** the formal enforcement / civil-administrative / criminal-judicial
action matrix; **(3)** the defendant-name layer and how (if at all) it links to corporate identity.
**Strictly read-only — zero data-plane mutation. No `.lance` write, no DDL, no pipeline alteration.**

**Provenance / attestation.** Every figure is a **live, read-only read of R2** under
`doppler run -p core-x -c prd`. Harness: `boto3` central-directory random-access over each landing
ZIP (member manifest + per-member raw-deflate→gzip rewrap — `/tmp` holds only the **compressed**
member, the `_member_to_gz` technique lifted verbatim from `materialize_epa.py`); `duckdb 1.5.x`
`read_csv(all_varchar=true, parallel=false)` for row counts, full per-column fill, and distinct
cardinalities; `pylance 7.x` `count_rows` + `list_indices` for the active SoR and
`epa_program_links` routing scan. **No Lance write, no DDL, no migration executed.** Landing
archives written **2026-06-03** (DMR history bundles **2026-06-05**); audit date **2026-06-05**.

Prior attested recon this extends (not re-litigated here): `EPA_LEGAL_ENTITY_MATERIALIZATION_PLAN.md`
(permit/defendant **names** → planned `epa_permits`/`epa_defendants`), `EPA_CAA_RCRA_LEGAL_ENTITY_DIAGNOSTIC.md`
(air/RCRA facility names → `epa_air_facilities`/`epa_rcra_handlers`, now **built**), `GLEIF_EPA_ENTITY_BRIDGE_DIAGNOSTIC.md`.

---

## 0. Headline posture

| Plane | Carrier in landing zone | Materialized? | Verdict |
|---|---|---|---|
| **Permittee legal name** | `npdes_downloads.zip::ICIS_PERMITS.PERMIT_NAME` | 🛑 **No** — planned `epa_permits`, live `KeyCount=0` | Orphaned (transient-only in `build_bridge`). |
| **Co-permittees (general-permit umbrella)** | `npdes_master_general_permits.zip::ICIS_MASTER_GENERAL_PERMITS` + `ICIS_PERMITS.MASTER_EXTERNAL_PERMIT_NMBR` | 🛑 **No** — neither master table nor covered-permit node exists | Umbrella↔covered relation fully unmaterialized. |
| **Facility owner/operator history** | **none staged** | 🛑 **No carrier** | **Un-landed, not merely unmaterialized.** No owner/operator entity+date table exists in any EPA bundle in R2. |
| **Federal enforcement — defendant name** | `case_downloads.zip::CASE_DEFENDANTS` | 🛑 **No** — planned `epa_defendants`, live `KeyCount=0` | **Flat UPPERCASE string**; no address, no corporate id. |
| **Civil-admin / civil-judicial / criminal classifier** | `case_downloads.zip::CASE_ENFORCEMENT_TYPE` (121 codes) | 🛑 **No** | **Criminal cases cannot be isolated from the active SoR.** |
| **Program-level enforcement actions (NPDES/Air/RCRA)** | 6 untouched `*_FORMAL/INFORMAL/ENFORCEMENT*` members | 🛑 **No** (only the federal `CASE_ENFORCEMENTS` header is active) | 6.7 M+ action rows orphaned; most directly `REGISTRY_ID`-bindable. |

The hub itself is intact: `epa_program_links` (**4,360,148** links → **3,385,406** distinct `REGISTRY_ID`)
routes every program key below back to the universal hub. The gap is exclusively in the **name +
enforcement payload tables** left in landing.

---

## 1. Active SoR baseline (live `pylance`, 2026-06-05)

13 `epa_*` datasets exist under `s3://data-sink/active/`. This is the materialized universe the deltas below are measured against.

| Dataset | Rows | Cols | Indices (BTREE/BITMAP) |
|---|--:|--:|---|
| `epa_facilities` | 3,240,591 | 10 | `REGISTRY_ID`, `FAC_STATE` |
| `epa_program_links` | 4,360,148 | 13 | `REGISTRY_ID`, `PGM_SYS_ID`, `PGM_SYS_ACRNM` |
| `epa_npdes_dmrs` | 320,506,998 | 58 | `EXTERNAL_PERMIT_NMBR`, `MONITORING_PERIOD_END_DATE`, `FISCAL_YEAR` |
| `epa_npdes_qncr_history` | 7,866,031 | 8 | `NPDES_ID`, `YEARQTR` |
| `epa_npdes_eff_violations` | 46,361,587 | 43 | `NPDES_ID`, `MONITORING_PERIOD_END_DATE` |
| `epa_case_enforcements` | 135,053 | 25 | `ACTIVITY_ID`, `CASE_NUMBER` |
| `epa_case_milestones` | 508,088 | 5 | `ACTIVITY_ID`, `ACTUAL_DATE` |
| `epa_pipeline_caa` | 66,655 | 35 | `REGISTRY_ID`, `SOURCE_ID`, `FOUND_VIOLATION` |
| `epa_pipeline_rcra` | 456,773 | 30 | `REGISTRY_ID`, `SOURCE_ID`, `FOUND_VIOLATION` |
| `epa_aim_triggering_events` | 5,375 | 18 | `NPDES_ID`, `ACTIVE_EXCEPTION` |
| `epa_to_sos_bridge` | 356,903 | 10 | `REGISTRY_ID`, `normalized_legal_name` |
| `epa_air_facilities` | 278,944 | 20 | `REGISTRY_ID`, `PGM_SYS_ID`, `normalized_facility_name`, +4 BITMAP |
| `epa_rcra_handlers` | 1,578,504 | 17 | `REGISTRY_ID`, `RCRA_ID`, `normalized_facility_name`, +4 BITMAP |

**Absent (live `list_objects_v2` `KeyCount=0`, probed explicitly):** `epa_permits`, `epa_defendants`,
`epa_owner_operators`, `epa_co_permittees`, `epa_npdes_formal_actions`, `epa_npdes_informal_actions`,
`epa_criminal_cases`.

**Hub routing — `epa_program_links.PGM_SYS_ACRNM` → `REGISTRY_ID` (live):**

| ACRNM | links | distinct `PGM_SYS_ID` | distinct `REGISTRY_ID` | Binds which orphan |
|---|--:|--:|--:|---|
| `RCRAINFO` | 1,578,620 | 1,578,620 | 1,476,648 | `RCRA_ENFORCEMENTS.ID_NUMBER` |
| `NPDES` | 1,193,249 | 1,193,249 | 1,016,966 | NPDES enforcement `NPDES_ID` |
| `AIR` | 279,103 | 279,103 | 265,643 | `ICIS-AIR_*_ACTIONS.PGM_SYS_ID` |
| `ICIS`,`SFDW`,`EIS`,`TRIS`,`CEDRI`,`TSCA`,`RMP`,`SEMS`,`E-GGRT`,`CAMDBS` | … | … | … | other media |

⚠️ **No `AFS` acronym exists in `epa_program_links`.** The legacy Air Facility System key (`AFS_ID`)
has **no route to `REGISTRY_ID`** — AFS is superseded by the `AIR` (ICIS-AIR) program. Relevant to §2.3.

---

## 2. The Unmaterialized Permit Space

### 2.1 Permittee legal name — `ICIS_PERMITS` (planned `epa_permits`, not built)

Confirmed `KeyCount=0`. Full grain/fill/bind in `EPA_LEGAL_ENTITY_MATERIALIZATION_PLAN.md`; summary:
**1,694,646** permit-version rows · `PERMIT_NAME` **97.22%** → **1,013,316** distinct `REGISTRY_ID`.
Bind: `ICIS_PERMITS.EXTERNAL_PERMIT_NMBR = ICIS_FACILITIES.NPDES_ID`, `REGISTRY_ID = ICIS_FACILITIES.FACILITY_UIN`
(clean 1:1, 99.53%); second path via `program_links(NPDES)`.

### 2.2 Co-permittees — the general-permit umbrella relation (fully unmaterialized)

EPA does not store a flat "co-permittee" name array. Co-permittees are modeled as **covered permits
hanging off a master general permit**. Both ends are orphaned:

| Source (landing) | Rows | Key | Role | `REGISTRY_ID` bind |
|---|--:|---|---|---|
| `npdes_master_general_permits.zip::ICIS_MASTER_GENERAL_PERMITS` | **2,823** (27 cols) | `EXTERNAL_PERMIT_NMBR` (1,232 distinct), `ACTIVITY_ID` | the **umbrella master** general permits; `PERMIT_NAME` 80.77% | via `ICIS_FACILITIES`/`program_links(NPDES)` |
| `npdes_downloads.zip::ICIS_PERMITS.MASTER_EXTERNAL_PERMIT_NMBR` | (80.77% of 1.69 M permit rows) | `MASTER_EXTERNAL_PERMIT_NMBR` → master's `EXTERNAL_PERMIT_NMBR` | the **covered permittees** under each master | (inherits permit bind) |
| `npdes_downloads.zip::NPDES_PERM_COMPONENTS` | **757,387** (3 cols) | `EXTERNAL_PERMIT_NMBR` (749,784 distinct) | permit **components** (9 `COMPONENT_TYPE` classes) | (inherits permit bind) |

**Join key to surface co-permittees:** `ICIS_MASTER_GENERAL_PERMITS.EXTERNAL_PERMIT_NMBR`
⟵ `ICIS_PERMITS.MASTER_EXTERNAL_PERMIT_NMBR`; each covered permit then carries its own `PERMIT_NAME`
+ `REGISTRY_ID`. Nothing on either side is in the active SoR today.

### 2.3 Owner/operator history — **no source carrier in the landing zone**

Probed every plausible carrier. **None holds a distinct owner-vs-operator entity name or an
ownership-change history.** This is an *un-landed* gap, categorically different from the orphaned-name
gaps above (which have a staged carrier).

| Probed carrier | Rows | What it actually holds | Owner/Operator? |
|---|--:|---|---|
| `afs_downloads.zip::AFS_FACILITIES` | 236,734 (21 cols) | `PLANT_NAME` (site label), `PLANT_ID`/`AFS_ID`, address, SIC/NAICS, status | 🛑 None — no owner/operator col, **no `REGISTRY_ID`, and `AFS_ID` is not in `program_links`** |
| `rcra_downloads.zip::RCRA_FACILITIES` | 1,597,673 | `FACILITY_NAME` only (→ now `epa_rcra_handlers`) | 🛑 None (no `RCRA_OWNER_OPERATORS` member is staged) |
| `npdes_downloads.zip::ICIS_PERMITS` | 1,694,646 | `PERMIT_NAME` per **(permit, version)** — 500,623 prior-version rows | ⚠️ **Permittee-of-record version history only** (temporal proxy, not owner/operator) |

> **Definitive:** the only realizable "history" axis from staged data is the **permittee-of-record
> chain** in `ICIS_PERMITS` (via `VERSION_NMBR` / lifecycle dates) once `epa_permits` lands. A true
> owner/operator change log would require ingesting EPA's **RCRAInfo Handler owner/operator extract**
> or the **ICIS permit-operator** detail — neither is present in `s3://data-sink/landing/epa/`.

---

## 3. The Enforcement & Defendant Matrix

### 3.1 Federal case system — ICIS-FEA (`case_downloads.zip`, 22 members, 2 materialized)

Only `CASE_ENFORCEMENTS` (→ `epa_case_enforcements`, 135,053) and `CASE_MILESTONES`
(→ `epa_case_milestones`, 508,088) are active. The active case **header** carries a coarse
`ACTIVITY_TYPE` (2 values), `ENF_OUTCOME_CODE` (30), `DOJ_DOCKET_NMBR` (2.71% — the judicial slice),
and `TOTAL_PENALTY_*`. **The legal classification, the defendant names, the conclusions/settlements,
the penalties detail, and the statutory citations are all in unmaterialized siblings:**

| Member (orphaned) | Rows | Key(s) | Payload / why it matters |
|---|--:|---|---|
| `CASE_DEFENDANTS` | **200,159** | `ACTIVITY_ID`+`CASE_NUMBER` | **`DEFENDANT_NAME` 100% (160,484 distinct)** — the defendant legal-entity names. → planned `epa_defendants`. |
| `CASE_FACILITIES` | **202,509** | `ACTIVITY_ID` | `REGISTRY_ID` 99.60% (113,414 distinct) + address — the **only** corporate-id/geo carrier for a case. |
| **`CASE_ENFORCEMENT_TYPE`** | **143,406** | `ACTIVITY_ID`+`CASE_NUMBER` | **`ENF_TYPE_CODE`/`_DESC` = 121 distinct** — the civil-administrative / civil-judicial / **criminal** classifier. |
| `CASE_ENFORCEMENT_CONCLUSIONS` | **126,160** | `ACTIVITY_ID`,`ENF_CONCLUSION_ID` | settlement lodged/entered dates, `PRIMARY_LAW`, `FED_PENALTY_ASSESSED_AMT`, conclusion captions. |
| `CASE_PENALTIES` | **123,490** | `ACTIVITY_ID`+`CASE_NUMBER` | federal/state penalty, SEP, cost-recovery, collected amounts (per case). |
| `CASE_LAW_SECTIONS` | **177,603** | `ACTIVITY_ID` | `STATUTE_CODE` (14) + `LAW_SECTION_CODE` (346) — the **statutory cites that flag criminal sections**. |
| `CASE_ENFORCEMENT_CONCLUSION_{COMPLYING_ACTIONS, DOLLARS, FACILITIES, POLLUTANTS, SEP}` | (5 children) | `ENF_CONCLUSION_ID` | per-conclusion complying actions, $ , facilities, pollutants, supplemental-environmental-projects. |
| `CASE_VIOLATIONS`, `CASE_POLLUTANTS`, `CASE_PRIORITIES`, `CASE_PROGRAMS`, `CASE_REGIONAL_DOCKETS`, `CASE_RELATED_ACTIVITIES`, `CASE_RELIEF_SOUGHT`, `CASE_ENFORCEMENT_CONCLUSION_*`, `EPA_INFORMAL_ENFORCEMENT_ACTIONS`, `ICIS_FEC_EPA_INSPECTIONS` | (10 more) | `ACTIVITY_ID` / `ENF_CONCLUSION_ID` | the remainder of the snowflake (violations, statutory program, relief sought, inspections). |

**Defendant identity — flat string, no hard id.** `CASE_DEFENDANTS` columns are exactly
`ACTIVITY_ID, CASE_NUMBER, DEFENDANT_NAME, NAMED_IN_COMPLAINT_FLAG, NAMED_IN_SETTLEMENT_FLAG`. **No
address, no `REGISTRY_ID`, no EIN/DUNS/CORPID.** Corporate address + `REGISTRY_ID` exist only on the
**separate** `CASE_FACILITIES` table, reachable solely through a **per-`ACTIVITY_ID` cartesian** (ECHO
carries no defendant→facility FK; a case with *d* defendants × *f* facilities yields *d×f* candidate
edges). Defendant→hub is therefore a *candidate* edge, not an adjudicated fact.

**Criminal isolation is impossible from the active SoR today.** The active `epa_case_enforcements`
header's `ACTIVITY_TYPE` is only 2-way (judicial/administrative). Distinguishing **criminal** from
civil-judicial requires `CASE_ENFORCEMENT_TYPE.ENF_TYPE_CODE` (121-way) and/or the criminal statute
sections in `CASE_LAW_SECTIONS` — both unmaterialized.

### 3.2 Program-level enforcement actions (per-medium, all unmaterialized)

Distinct from the federal case system: these are the routine NPDES/Air/RCRA formal + informal
actions, keyed on the program id (directly hub-bindable). **6.7 M+ action rows, zero materialized.**

| Member (archive) | Rows | Join key → `REGISTRY_ID` | Action-type cardinality / payload |
|---|--:|---|---|
| `NPDES_INFORMAL_ENFORCEMENT_ACTIONS` (`npdes_downloads.zip`) | **821,977** | **`REGISTRY_ID` inline 100%** (112,418 distinct) + `NPDES_ID` | `ENF_TYPE_CODE` 36 — directly bindable, no join. |
| `NPDES_FORMAL_ENFORCEMENT_ACTIONS` (`npdes_downloads.zip`) | **111,816** | `NPDES_ID` → `ICIS_FACILITIES`/`program_links(NPDES)`; `ACTIVITY_ID` | `ENF_TYPE_CODE` 47 + `FED_PENALTY_ASSESSED_AMT`/`STATE_LOCAL_PENALTY_AMT`. |
| `NPDES_VIOLATION_ENFORCEMENTS` (`npdes_downloads.zip`) | **4,910,356** | `ACTIVITY_ID` (←formal/informal `ENF_IDENTIFIER`); `NPDES_VIOLATION_ID` | violation↔enforcement bridge; `VIOLATION_CODE` 321. |
| `RCRA_ENFORCEMENTS` (`rcra_downloads.zip`) | **382,172** | `ID_NUMBER` → `program_links(RCRAINFO)` (136,462 distinct handlers) | `ENFORCEMENT_TYPE` 447 + `PMP/FMP/FSC/SCR_AMOUNT` (incl. criminal referral types). |
| `ICIS-AIR_FORMAL_ACTIONS` (`ICIS-AIR_downloads.zip`) | **105,656** | `PGM_SYS_ID` → `program_links(AIR)` (37,216 distinct) | `ENF_TYPE_CODE` 47 + `PENALTY_AMOUNT`. |
| `ICIS-AIR_INFORMAL_ACTIONS` (`ICIS-AIR_downloads.zip`) | **336,410** | `PGM_SYS_ID` → `program_links(AIR)` (60,315 distinct) | `ENF_TYPE_CODE` 19. |

---

## 4. Consolidated delta — exact files / rows / keys excluded from `active/`

| # | Plane | Landing file (archive::member) | Rows | Native key | → `REGISTRY_ID` path |
|---|---|---|--:|---|---|
| 1 | permittee name | `npdes_downloads.zip::ICIS_PERMITS` | 1,694,646 | `EXTERNAL_PERMIT_NMBR`,`VERSION_NMBR` | `ICIS_FACILITIES.FACILITY_UIN` (1:1, 99.53%) |
| 2 | co-permittee master | `npdes_master_general_permits.zip::ICIS_MASTER_GENERAL_PERMITS` | 2,823 | `EXTERNAL_PERMIT_NMBR` | via `ICIS_FACILITIES` / `program_links(NPDES)` |
| 3 | permit components | `npdes_downloads.zip::NPDES_PERM_COMPONENTS` | 757,387 | `EXTERNAL_PERMIT_NMBR` | inherits permit bind |
| 4 | owner/operator hist. | **— none staged —** | 0 | — | **un-landed** (proxy: `ICIS_PERMITS` version chain) |
| 5 | defendant name | `case_downloads.zip::CASE_DEFENDANTS` | 200,159 | `ACTIVITY_ID`,`CASE_NUMBER` | `CASE_FACILITIES.REGISTRY_ID` (cartesian) |
| 6 | case facility/geo | `case_downloads.zip::CASE_FACILITIES` | 202,509 | `ACTIVITY_ID` | `REGISTRY_ID` direct (99.60%) |
| 7 | civil/criminal type | `case_downloads.zip::CASE_ENFORCEMENT_TYPE` | 143,406 | `ACTIVITY_ID`,`CASE_NUMBER` | via `CASE_FACILITIES` |
| 8 | conclusions/settlement | `case_downloads.zip::CASE_ENFORCEMENT_CONCLUSIONS` | 126,160 | `ACTIVITY_ID`,`ENF_CONCLUSION_ID` | via `CASE_FACILITIES` |
| 9 | penalties detail | `case_downloads.zip::CASE_PENALTIES` | 123,490 | `ACTIVITY_ID`,`CASE_NUMBER` | via `CASE_FACILITIES` |
| 10 | statutory cites | `case_downloads.zip::CASE_LAW_SECTIONS` | 177,603 | `ACTIVITY_ID` | via `CASE_FACILITIES` |
| 11 | NPDES informal enf. | `npdes_downloads.zip::NPDES_INFORMAL_ENFORCEMENT_ACTIONS` | 821,977 | `REGISTRY_ID` inline + `NPDES_ID` | **direct (inline)** |
| 12 | NPDES formal enf. | `npdes_downloads.zip::NPDES_FORMAL_ENFORCEMENT_ACTIONS` | 111,816 | `NPDES_ID`,`ACTIVITY_ID` | `program_links(NPDES)` |
| 13 | NPDES viol↔enf | `npdes_downloads.zip::NPDES_VIOLATION_ENFORCEMENTS` | 4,910,356 | `ACTIVITY_ID`,`NPDES_VIOLATION_ID` | via formal/informal `ENF_IDENTIFIER` |
| 14 | RCRA enf. | `rcra_downloads.zip::RCRA_ENFORCEMENTS` | 382,172 | `ID_NUMBER` | `program_links(RCRAINFO)` |
| 15 | Air formal enf. | `ICIS-AIR_downloads.zip::ICIS-AIR_FORMAL_ACTIONS` | 105,656 | `PGM_SYS_ID` | `program_links(AIR)` |
| 16 | Air informal enf. | `ICIS-AIR_downloads.zip::ICIS-AIR_INFORMAL_ACTIONS` | 336,410 | `PGM_SYS_ID` | `program_links(AIR)` |
| + | case snowflake remainder | `case_downloads.zip::` 13 more members (`CASE_VIOLATIONS`, `CASE_RELIEF_SOUGHT`, `CASE_PROGRAMS`, 5× `…_CONCLUSION_*`, `EPA_INFORMAL_ENFORCEMENT_ACTIONS`, `ICIS_FEC_EPA_INSPECTIONS`, …) | — | `ACTIVITY_ID`/`ENF_CONCLUSION_ID` | via `CASE_FACILITIES` |

**Orphaned enforcement rows currently invisible to the hub:** **1,758,031** program-level
enforcement *actions* (NPDES informal 821,977 + NPDES formal 111,816 + RCRA 382,172 + Air formal
105,656 + Air informal 336,410) · **143,406** federal-case civil/criminal classifications
(`CASE_ENFORCEMENT_TYPE`) · **200,159** defendant-name rows · **4,910,356** violation↔enforcement
edges. Every one binds to `REGISTRY_ID` by the path in column 6 above.

---

## 5. Format hurdles (what makes each plane hard to land)

- **Federal case layer = highly-normalized snowflake.** 22 relational members fan out from
  `ACTIVITY_ID`/`ENF_CONCLUSION_ID`. Bringing "the defendant + their civil/criminal classification +
  settlement + penalty + statute" into one row is a **5-table multi-stage join**
  (`CASE_DEFENDANTS ⋈ CASE_FACILITIES ⋈ CASE_ENFORCEMENT_TYPE ⋈ CASE_ENFORCEMENT_CONCLUSIONS ⋈ CASE_LAW_SECTIONS`),
  not a passthrough.
- **Defendant→hub is cartesian, not FK.** No defendant→facility key exists; the `REGISTRY_ID` edge is
  a per-case `ACTIVITY_ID` cross-product. Grain must be **declared** (carry `ACTIVITY_ID`+`CASE_NUMBER`),
  not assumed — a materialization that drops them asserts adjudicated facts it cannot support.
- **No corporate hard identifier anywhere.** Defendant/permittee names are flat **UPPERCASE** strings
  with no EIN/DUNS/CORPID. Cross-walking to a legal entity requires the canonical `core.name_norm`
  blocking key (the `epa_to_sos_bridge` pattern) or the cartesian `CASE_FACILITIES.REGISTRY_ID`.
- **Program-level actions are the easy win.** NPDES/Air/RCRA formal+informal tables are **flat** and
  key directly on `NPDES_ID`/`PGM_SYS_ID`/`ID_NUMBER`; `NPDES_INFORMAL_ENFORCEMENT_ACTIONS` even
  carries `REGISTRY_ID` **inline (100%)** — a zero-join materialization.
- **Owner/operator history has no source.** Cannot be materialized from staged data at all; needs a
  new upstream pull (RCRAInfo Handler owner/operator extract or ICIS permit-operator history).
- **Legacy `afs_downloads.zip` is a dead end.** No owner/operator payload, and `AFS_ID` has no
  `program_links` route — superseded by the `AIR` (ICIS-AIR) program. Do not build against it.
- **CRLF + embedded newlines** in case free-text (`ENF_SUMMARY_TEXT`, `ENF_TYPE_DESC`) require
  `read_csv(parallel=false)` — already the fleet default in `materialize_epa.py::_read()`.

---

## 6. Hub topology — where each orphan attaches

```
REGISTRY_ID ─┬─ epa_facilities / epa_program_links / epa_air_facilities / epa_rcra_handlers   [LIVE]
             │
             ├─ ICIS_PERMITS.PERMIT_NAME ............... permittee name      via FACILITY_UIN   [orphan → epa_permits]
             │     └─ MASTER_EXTERNAL_PERMIT_NMBR ...... co-permittee umbrella → ICIS_MASTER_GENERAL_PERMITS  [orphan]
             │     └─ NPDES_PERM_COMPONENTS ............ permit components                       [orphan]
             │
             ├─ NPDES_INFORMAL_ENFORCEMENT_ACTIONS ..... REGISTRY_ID INLINE (direct)            [orphan]
             ├─ NPDES_FORMAL_ENFORCEMENT_ACTIONS ....... NPDES_ID  → program_links(NPDES)       [orphan]
             │     └─ NPDES_VIOLATION_ENFORCEMENTS ..... ACTIVITY_ID (enf) ↔ NPDES_VIOLATION_ID [orphan]
             ├─ RCRA_ENFORCEMENTS ...................... ID_NUMBER → program_links(RCRAINFO)    [orphan]
             ├─ ICIS-AIR_FORMAL/INFORMAL_ACTIONS ....... PGM_SYS_ID → program_links(AIR)        [orphan]
             │
             └─ CASE_FACILITIES.REGISTRY_ID  (ACTIVITY_ID hub for the federal case snowflake)   [orphan]
                   ├─ CASE_DEFENDANTS .................. defendant name (flat; cartesian)       [orphan → epa_defendants]
                   ├─ CASE_ENFORCEMENT_TYPE ............ civil / criminal classifier (121)      [orphan]
                   ├─ CASE_ENFORCEMENT_CONCLUSIONS ..... settlements / outcomes                 [orphan]
                   ├─ CASE_PENALTIES ................... penalty detail                         [orphan]
                   └─ CASE_LAW_SECTIONS ................ statute cites (criminal sections)       [orphan]

   owner/operator history  ──  ✗ no carrier staged (un-landed)
   AFS_ID                  ──  ✗ no program_links route (legacy dead-end)
```

**Materialization intentionally not written — diagnostic + structural join paths only, per directive.**
