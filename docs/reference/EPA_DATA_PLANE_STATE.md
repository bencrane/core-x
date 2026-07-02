# EPA Data Plane — Canonical State (READ THIS FIRST)

> **⚠️ CANONICAL — verified live against R2 (`s3://data-sink/active/`) on 2026-07-02** via
> `doppler run -p core-x -c prd` + `pylance 7` / `boto3`. This is the authoritative "what is
> actually built" map. The other `EPA_*.md` docs in this directory are DIAGNOSTICS and BUILD
> PLANS written before/during the build — several describe now-built datasets as
> "planned / unmaterialized / KeyCount=0 / gap." **When a diagnostic conflicts with this file,
> this file wins.** Every figure below is a live read, not a copy of a doc's own claim.

The EPA plane is BUILT across three layers. Do not re-diagnose these as gaps.

---

## Layer 1 — Source datasets (~124 `epa_*` prefixes)

The raw EPA mirror: ECHO, FRS, ICIS-NPDES, ICIS-Air, RCRAInfo, SDWA, the federal CASE
(ICIS-FEA) enforcement snowflake, AFS (legacy air), CSO/sewer-overflow, ATTAINS, and the
per-program compliance "pipeline" tables. Built by `pipelines/ingest_epa/materialize_epa.py`
(Modal app `epa-pipelines`), storage version 2.1.

Load-bearing source datasets (live row counts):

| Dataset | Rows | Key(s) | Role |
|---|--:|---|---|
| `epa_facilities` | 3,240,591 | `REGISTRY_ID` (BTREE), `FAC_STATE` | FRS facility master — site name + geo. The apex hub. |
| `epa_program_links` | 4,360,148 | `REGISTRY_ID`, `PGM_SYS_ID`, `PGM_SYS_ACRNM` (all BTREE) | Universal crosswalk `REGISTRY_ID ↔ program key` across 13 programs. 3,385,406 distinct RID. |
| `epa_npdes_dmrs` | **422,447,436** | `EXTERNAL_PERMIT_NMBR`, `MONITORING_PERIOD_END_DATE` (BTREE); `FISCAL_YEAR` (BITMAP) | Raw NPDES discharge trove, **FY1982→FY2026** (backfilled from 67.6M — see backfill EXECUTION doc). |
| `epa_npdes_eff_violations` | 46,361,587 | `NPDES_ID`, `MONITORING_PERIOD_END_DATE` | Effluent violations. |
| `epa_npdes_qncr_history` | 7,866,031 | `NPDES_ID`, `YEARQTR` | Quarterly non-compliance. |
| `epa_case_enforcements` | 135,053 | `ACTIVITY_ID`, `CASE_NUMBER` | Federal case header (caption, penalties, DOJ docket). |
| `epa_case_facilities` | ~202,509 | `ACTIVITY_ID` | Case→`REGISTRY_ID` edge (now also distilled into `crosswalk_epa_registry_enforcement`). |
| `epa_air_facilities` | 278,944 | `REGISTRY_ID`, `PGM_SYS_ID` (BTREE); `normalized_facility_name` | CAA air-facility name node (built from the CAA/RCRA diagnostic remediation). |
| `epa_rcra_handlers` | 1,578,504 | `REGISTRY_ID`, `RCRA_ID` (BTREE); `normalized_facility_name` | RCRA handler name node (built from same remediation). |

> The `epa_*` prefix count includes a handful of non-dataset helpers (e.g. `epa_pipeline_rcra_read_me`)
> and singular/plural duplicates from source-file naming; treat ~124 as "the source mirror," not
> a curated count. The datasets above + the legal-entity layer are the load-bearing ones.

---

## Layer 2 — Facility spine (BUILT, `REGISTRY_ID`-keyed, identifiers-only)

Built by `pipelines/ingest_epa/materialize_epa_spine.py` (Modal app `epa-spine-pipelines`).
Deterministic, zero fuzzy matching. Executed end-to-end 2026-06-10; all artifacts confirmed
live 2026-07-02. See `EPA_UNIFIED_SPINE_PLAN.md` (§AS-BUILT) for gates.

| Artifact | Rows × Cols | Key | For |
|---|--:|---|---|
| `spine_epa_facility` | 3,240,591 × 30 | `registry_id` (BTREE), `fac_name`, `fac_zip5`; BITMAP `fac_state`, program-presence flags | Canonical facility dimension — identity + geo + NAICS/SIC + `has_{npdes,rcra,sdwa,air,enforcement}` + ECHO compliance/penalty headline. |
| `spine_epa_facility_360` | 3,240,591 × 76 | `registry_id` (BTREE) + BITMAPs | Serving capstone — spine LEFT JOIN every rollup. One point-read returns full multi-media posture. |
| `crosswalk_epa_registry_program` | 4,360,148 | BTREE `REGISTRY_ID`, `PGM_SYS_ID`; BITMAP `PGM_SYS_ACRNM` | Universal RID↔program-key superset. |
| `crosswalk_epa_registry_npdes` | 1,193,249 | BTREE `REGISTRY_ID`, `NPDES_ID` | NPDES key resolution. |
| `crosswalk_epa_registry_rcra` | 1,578,620 | BTREE `REGISTRY_ID`, `ID_NUMBER` | RCRA key resolution. |
| `crosswalk_epa_registry_sdwa` | 676,905 | BTREE `REGISTRY_ID`, `PWSID` | SDWA key resolution. |
| `crosswalk_epa_registry_air` | 279,103 | BTREE `REGISTRY_ID`, `PGM_SYS_ID` | Air key resolution. |
| `crosswalk_epa_registry_enforcement` | 161,173 | BTREE `REGISTRY_ID`, `ACTIVITY_ID` | Case→RID edge (closes the old "CASE_FACILITIES not standing" gap). |
| `rollup_epa_npdes` | 672,942 | BTREE `registry_id`; BITMAP `has_dmr_exceedance`, `npdes_compliance_tier` | Per-facility NPDES compliance rollup (rides the 422M DMR). |
| `rollup_epa_rcra` | 301,396 | BTREE `registry_id`; BITMAP `rcra_snc_flag`, `has_rcra_violation` | Per-facility RCRA rollup. |
| `rollup_epa_sdwa` | 431,742 | BTREE `registry_id`; BITMAP `has_health_based_violation`, `pws_type` | Per-facility drinking-water rollup. |
| `rollup_epa_air` | 34,689 | BTREE `registry_id`; BITMAP `caa_hpv_flag`, `has_air_violation` | Per-facility air rollup. |
| `rollup_epa_enforcement` | 55,393 | BTREE `registry_id`; BITMAP `has_federal_case`, `has_penalty` | Per-facility federal-enforcement rollup (facility-attributed penalties). |

Every rollup is a strict subset of `spine_epa_facility.registry_id` (0 orphans). Keys are
stored as strings (never int — leading-zero / precision safety).

---

## Layer 3 — Legal-entity / name + resolution layer (BUILT)

The corporate-name nodes and the entity-resolution bridge. Built via
`materialize_epa.py::build_bridge` + `materialize_epa_gtm.py`. See
`EPA_LEGAL_ENTITY_MATERIALIZATION_PLAN.md` and `EPA_NPDES_GTM_COMPLIANCE_LAYER.md`.

| Dataset | Rows | Key columns | Names / payload |
|---|--:|---|---|
| `epa_permits` | 1,686,705 | `REGISTRY_ID`, `EXTERNAL_PERMIT_NMBR`, `NPDES_ID`, `ACTIVITY_ID` (BTREE); `normalized_legal_name` (BTREE) | `PERMIT_NAME` (~683,453 distinct), `normalized_legal_name` (~620,223 distinct), permit meta + lifecycle + `FAC_*` geo. 1,013,316 distinct `REGISTRY_ID`. |
| `epa_case_defendants` | 200,159 | `ACTIVITY_ID`, `CASE_NUMBER` | `DEFENDANT_NAME` (160,484 distinct) + complaint/settlement flags. Raw CASE_DEFENDANTS grain. |
| `epa_entity_compliance` | 142,933 | `REGISTRY_ID`, `normalized_legal_name` (BTREE); BITMAP `FAC_STATE`, `violation_tier`, `is_active` | Per-company NPDES footprint: `entity_name`, geo, `n_permits`, `n_dmr_rows`, `n_violations`, `n_exceedances`, `violation_tier`, `is_active`. The GTM-actionable grain. |
| `epa_permit_compliance` | 156,014 | `EXTERNAL_PERMIT_NMBR`, `REGISTRY_ID`, `normalized_legal_name` | Per-reporting-permit compliance resume (DMR rolled up + entity attached). |
| `epa_permit_parameter_compliance` | 1,884,617 | `EXTERNAL_PERMIT_NMBR`, `REGISTRY_ID`; BITMAP `PARAMETER_CODE`, `has_exceedances` | Per-pollutant resume — parameter-specific targeting. |
| `epa_to_sos_bridge` | 406,191 | `REGISTRY_ID`, `normalized_legal_name` (BTREE) | EPA `REGISTRY_ID` → Secretary-of-State `sos_company_id` (177,413 distinct). `epa_matched_name`, `match_tier`, `confidence`, `sos_source_state`. |

`normalized_legal_name` is the byte-identical `core.name_norm` blocking key used fleet-wide —
these tables join to `companies`, `people`, `sos_normalized_master`, PPP, SAM crosswalks, etc.
with no drift.

---

## Entity resolution: Secretary-of-State, NOT SAM.gov (verified)

The EPA entity-resolution target is **`sos_company_id` + `normalized_legal_name`** via
`epa_to_sos_bridge`. **There is NO `UEI` / `DUNS` / `LEI` / `CAGE` column on any EPA entity
dataset** — column-scanned live on 2026-07-02 across `epa_permits`, `epa_case_defendants`,
`epa_entity_compliance`, `epa_to_sos_bridge`, `spine_epa_facility`, `spine_epa_facility_360`
(all returned zero identity-column hits). Consequences:

- EPA is **not** directly linked to the SAM.gov federal-contractor identity graph (which keys on
  `uei`). Any EPA→SAM join is transitive and unverified — it would hop EPA name → SoS →
  (name/UEI crosswalk) → SAM, a multi-step fuzzy path, not a hard-ID join.
- No GLEIF×EPA (`lei ↔ REGISTRY_ID`) bridge dataset exists in R2. The GLEIF recon
  (`GLEIF_EPA_ENTITY_BRIDGE_DIAGNOSTIC.md`) is a design, not a built artifact.
- The only live external entity link from EPA is to SoS.

---

## Coverage

`epa_to_sos_bridge` resolves **406,191** distinct `REGISTRY_ID` (177,413 distinct
`sos_company_id`). Against the `spine_epa_facility` universe of 3,240,591 facilities, that is
**~12.5%** of facilities bridged to a Secretary-of-State entity. The bridge is
SoS-coverage-bounded and public-entity-filtered — it is a floor for name resolution, not the
population ceiling. Direct-name matching off `epa_permits` / raw EPA names (bypassing the SoS
bottleneck) reaches a wider set (see `EPA_PPP_MAPPING_BLUEPRINT.md` §4 for the PPP-overlap
measurement of that ceiling).

---

## Open follow-up

- **Spine-refresh preemption loop.** The `epa-spine-pipelines` Modal app (the periodic REFRESH
  of the already-built Layer-2 spine) was observed preemption-looping on the Phase-3 NPDES
  rollup — the step that scans the 422M-row `epa_npdes_dmrs`. `create_scalar_index` / the heavy
  aggregate has no checkpoint, so a preemption restarts from zero. Durable fix: non-preemptible
  workers OR checkpoint-resume OR chunk the DMR aggregate into preemption-safe windows (mirror
  the R2-safe local-NVMe spill + detached pattern proven in the DMR backfill EXECUTION doc).
  The built spine artifacts are intact and current; only the automated refresh is affected.

---

## Doc map (provenance — these are historical diagnostics/plans, superseded where noted)

| Doc | What it was | Current status |
|---|---|---|
| `EPA_LEGAL_ENTITY_MATERIALIZATION_PLAN.md` | Build plan for permits/defendants name nodes | EXECUTED — `epa_permits`, `epa_case_defendants` live. |
| `EPA_UNMATERIALIZED_PERMIT_ENFORCEMENT_DIAGNOSTIC.md` | "Unmaterialized planes" inventory (13-dataset baseline) | Partially superseded — permits/defendants now built; owner-op / co-permittee / case-snowflake siblings still unbuilt. |
| `EPA_CAA_RCRA_LEGAL_ENTITY_DIAGNOSTIC.md` | Orphaned air/RCRA name diagnostic | EXECUTED — `epa_air_facilities`, `epa_rcra_handlers` live. |
| `EPA_UNIFIED_SPINE_PLAN.md` | Facility spine build plan | BUILT (carries its own AS-BUILT); refresh preemption is the open item. |
| `EPA_NPDES_GTM_COMPLIANCE_LAYER.md` | GTM compliance layer over the DMR trove | Accurate/current (verified 2026-06-06). |
| `EPA_NPDES_DMR_HISTORICAL_BACKFILL_PLAN.md` / `_EXECUTION.md` | 422M-row DMR backfill | EXECUTED & VERIFIED. |
| `EPA_PPP_MAPPING_BLUEPRINT.md` | EPA↔PPP name-bridge spec | Methodology current; EPA inventory figures superseded; `crosswalk_ppp_epa.py` still unbuilt. |
| `GLEIF_EPA_ENTITY_BRIDGE_DIAGNOSTIC.md` | GLEIF/LEI × EPA recon | Design only — no bridge built; name nodes it wanted now exist. |
