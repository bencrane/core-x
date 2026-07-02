# EPA Unified Facility Spine — Build Plan (Canonical, Identifiers-Only)

> **⚠️ CANONICAL STATE — verified live against R2 on 2026-07-02.** This plan is BUILT — see the §AS-BUILT reconciliation at the bottom (executed 2026-06-10). All 13 artifacts confirmed live at the row counts stated there: `spine_epa_facility` (3,240,591×30), `spine_epa_facility_360` (3,240,591×76), 6 `crosswalk_epa_registry_*`, 5 `rollup_epa_*`. Keyed on `REGISTRY_ID`. The deferred legal-entity layer (§"Deliberately deferred") has since ALSO been built separately: `epa_permits` (1,686,705), `epa_case_defendants` (200,159), `epa_entity_compliance` (142,933), `epa_to_sos_bridge` (406,191) — see `EPA_LEGAL_ENTITY_MATERIALIZATION_PLAN.md` / `EPA_NPDES_GTM_COMPLIANCE_LAYER.md`. **One open follow-up:** the `epa-spine-pipelines` Modal app is a REFRESH of this already-built spine and was observed preemption-looping on the 422M-row DMR rollup step (Phase 3 NPDES) — durable fix = non-preemptible workers / checkpoint-resume / chunk the DMR aggregate. The built artifacts are intact; only the periodic refresh is affected. Read `EPA_DATA_PLANE_STATE.md` first.

**Owner of record:** Principal Data Engineer · **Repo:** `core-x` · **Doppler:** `core-x/prd`
**Mode:** BUILD — append-only, idempotent, key-clustered. Mutates only NEW derived `spine_*` / `crosswalk_*` / `rollup_*` prefixes under `s3://data-sink/active/`. Every input EPA Lance dataset is read **READ-ONLY**; none of the 124 source datasets is rewritten.
**System of record:** LanceDB on R2 (`data_storage_version="2.1"`), addressed by R2 URI. No Iceberg, no Polaris, no catalog round-trip. DuckDB does 100% of the transform out-of-core; Lance is the append-only SoR.
**Descends from:** the recon already captured in `EPA GROUND TRUTH` (this session) + `EPA_LEGAL_ENTITY_MATERIALIZATION_PLAN.md`, `EPA_CAA_RCRA_LEGAL_ENTITY_DIAGNOSTIC.md`, `EPA_UNMATERIALIZED_PERMIT_ENFORCEMENT_DIAGNOSTIC.md`, `EPA_NPDES_GTM_COMPLIANCE_LAYER.md`. Reach figures below are empirically measured (~99–100%), not estimated.

---

## Thesis

EPA already mirrors `landing/epa/` into 124 queryable Lance datasets — but those datasets are a *graph of detail families*, not a *spine*. Answering "this facility's full multi-media posture — water + air + hazardous-waste + drinking-water + federal-enforcement compliance, penalties, inspection cadence, program presence — keyed to one resolvable identity" today requires a human to know which of 124 tables to touch, which key clusters which family, and which hop reaches `REGISTRY_ID`. This plan collapses that to a **key-clustered facility dimension** (`spine_epa_facility`, one row per FRS `REGISTRY_ID`) plus a small set of **composable, append-only, key-clustered crosswalks and rollups that ride the `REGISTRY_ID` BTREE** — the exact pattern the NPI spine uses (`cms_provider_payment_rollup.npi ──BTREE──> nppes_provider.npi`). The spine is built **over IDENTIFIERS ONLY** — `REGISTRY_ID` + the universal `epa_program_links` crosswalk + the 5 program-native keys — with ~100% deterministic joins and **zero fuzzy matching**. Legal-entity resolution (facility → corporate parent via name) is a strictly later layer that attaches at the `REGISTRY_ID` seam and is **not designed here**.

### The NPI pattern being mirrored (explicit)

| NPI spine concept | File | EPA spine analogue (this plan) |
|---|---|---|
| Key-clustered dimension master, ACTIVE subset only — does **not** duplicate the 9.5M-row provider master | `nppes_provider` (`materialize_analytical.py`) | `spine_epa_facility` — 1 row / `REGISTRY_ID`, the **program-present** subset of the 3.24M FRS universe, not a re-copy of `epa_facilities` |
| Lean, append-only, COMPOSABLE rollup that **rides the existing key BTREE**, materializes the signal ONCE | `cms_provider_payment_rollup` (`materialize_resolution.py`) — 1 row / payment-active NPI | `rollup_epa_npdes`, `rollup_epa_rcra`, `rollup_epa_sdwa`, `rollup_epa_air`, `rollup_epa_enforcement` — 1 row / `REGISTRY_ID` / program |
| Crosswalk dim riding a second BTREE for the other-side resolution | `cms_manufacturer_dim.manufacturer_id` | `crosswalk_epa_registry_*` — `REGISTRY_ID ↔ program key`, BTREE both directions |
| Unified serving view, deterministic, united ONLY on published keys, no corporate-identity columns | `provider_360/materialize.py` | `spine_epa_facility_360` (capstone, optional) — base spine LEFT JOIN every rollup |
| Verification gates: BTREE probe returns rows, EXPLAIN Index Scan, reach-% floors, row floors, provenance round-trip | `run_gate` G1–G12, `_verify_published` | per-phase gates §each-phase |
| ops-ledger idempotency, blast-radius separation (heavy/index work in its own container) | `ops.*_runs`, `reindex_dmrs_local` | `ops.epa_spine_runs`, DMR-touching work isolated |
| R2-safe local-stage → boto3 publish for wide/large tables (multipart `InvalidPart`) | `_publish_full_swap` / `r2_safe_local` | every spine artifact: stage local → index local → boto3 publish |

---

## The spine-key map (apex + crosswalk + 5 keys)

```
                          ┌─────────────────────────────────────────────┐
                          │  APEX HUB:  REGISTRY_ID  (FRS)               │
                          │  3,240,591 facilities · 12-digit, 110-pref   │
                          │  STORED AS STRING (never int)                │
                          └─────────────────────────────────────────────┘
                                          ▲   ▲
        inline REGISTRY_ID  ──────────────┘   └────────  via epa_program_links[acronym]
        (epa_facilities, epa_echo_exporter,        (UNIVERSAL CROSSWALK: REGISTRY_ID ↔
         epa_echo_demographics, epa_air_facilities, (PGM_SYS_ACRNM, PGM_SYS_ID))
         epa_rcra_handlers, epa_npdes_inspections,  4,360,148 links · 3,385,406 distinct
         epa_case_facilities, epa_program_links,    REGISTRY_ID · 13 programs · all 3 cols BTREE
         epa_frs_naics_codes/_sic_codes,
         epa_pipeline_caa/_rcra)
                                          │
   ┌──────────────┬──────────────┬────────┴───────┬──────────────┬──────────────────────┐
   ▼              ▼              ▼                ▼              ▼                        │
 NPDES          RCRA           SDWA             AIR           ENFORCEMENT                │
 EXTERNAL_      ID_NUMBER      PWSID            PGM_SYS_ID    ACTIVITY_ID (+CASE_NUMBER) │
 PERMIT_NMBR    (RCRA          (Public Water    (Clean Air,   (federal enforcement,      │
 ≡ NPDES_ID     Handler ID)    System)          'AIR' 100%)   'ICIS')                    │
 'NPDES'~100%   'RCRAINFO'~99% 'SFDW'~100%                    →REGISTRY_ID via            │
                                                              epa_case_facilities 99.3%   │
```

| Key | PGM_SYS_ACRNM | Reach to REGISTRY_ID | Clusters (detail family) |
|---|---|---|---|
| **`REGISTRY_ID`** (apex) | — (FRS) | self | `epa_facilities`, `epa_echo_exporter`, `epa_echo_demographics`, `epa_frs_naics_codes`, `epa_frs_sic_codes` |
| **`epa_program_links`** (universal crosswalk) | all 13 | inline | RCRAINFO 1.48M, NPDES 1.02M, SFDW/SDWA 453k, AIR 266k, ICIS 208k, EIS, TRIS, CEDRI, TSCA, RMP, SEMS, E-GGRT, CAMDBS |
| `EXTERNAL_PERMIT_NMBR` ≡ `NPDES_ID` | NPDES | ~100% | `epa_icis_permits`, `epa_npdes_limits` (16.5M), **`epa_npdes_dmrs` (422M)**, `epa_npdes_qncr_history`, `epa_npdes_inspections` (inline RID too), `epa_npdes_*_violations`, `epa_npdes_formal/informal_enforcement` |
| `ID_NUMBER` (RCRA Handler ID) | RCRAINFO | ~99% | `epa_rcra_facilities`, `epa_rcra_violations`, `_evaluations`, `_enforcements`, `_viosnc_history`, `_naics` (curated `epa_rcra_handlers.RCRA_ID` ≡ raw `ID_NUMBER`, inline RID too) |
| `PWSID` (Public Water System) | SFDW | ~100% | `epa_sdwa_pub_water_systems`, `epa_sdwa_facilities` (+`FACILITY_ID` sub-key), `epa_sdwa_violations_enforcement` (15.3M) |
| `PGM_SYS_ID` (Clean Air) | AIR | 100% | `epa_air_facilities` (inline RID), `epa_icis_air_*` (violations, formal/informal actions, stack tests, Title V certs, FCES/PCES, pollutants) |
| `ACTIVITY_ID` (+`CASE_NUMBER`) | ICIS | 99.3% via `epa_case_facilities` | `epa_case_enforcements`, `_defendants`, `_penalties`, `_violations`, `_milestones`, `_enforcement_conclusions` (+`ENF_CONCLUSION_ID` settlement sub-key). ACTIVITY_ID is **also** the enforcement linkage across NPDES/air detail tables. |

**Alternative fan-out (denormalized facility→program-IDs map):** `epa_echo_exporter`, keyed on `REGISTRY_ID`, carries space-delimited multi-value `NPDES_IDS`, `AIR_IDS`, `RCRA_IDS`, `SDWA_IDS`, `TRI_IDS`, `FEC_CASE_IDS`, `GHG_IDS`. Used as a **cross-check** of `epa_program_links`, never as the primary crosswalk source (see Open Decisions §D2).

**GTM signal context (lives in `epa_echo_exporter`, BITMAP-indexed):** 19,968 facilities in "Significant Violation"; 81,816 with any active violation; 21,264 with assessed penalties ($11.5B rollup, $16.36B granular in `epa_case_penalties.FED_PENALTY`); 205,667 inspected; SDWA 97,046 community water systems / 466.9M people served. **`FAC_SNC_FLG` is empty (0 'Y') in the current export — use `FAC_COMPLIANCE_STATUS='Significant Violation'` / `FAC_PROGRAMS_WITH_SNC>0` instead.**

---

## Architecture conventions (every phase conforms)

1. **Compute:** DuckDB (`memory_limit`, `temp_directory`, `preserve_insertion_order=false`) reads each source from R2 exactly once into a narrow, re-scannable local temp table → projects/casts → `to_arrow_reader` streams to Lance. The 422M-row `epa_npdes_dmrs` is **never** materialized in RAM.
2. **Storage:** `lance.write_dataset(..., data_storage_version="2.1", max_rows_per_file=1_048_576, max_bytes_per_file=90*1024**3)`. Resolution keys stored as **strings** — never cast `REGISTRY_ID` / program keys to int (leading-zero / precision loss).
3. **Write transport (R2-safe):** wide (>~80 col) or large artifacts stage to **local NVMe** (`/tmp`), build indices locally, then **boto3 uniform-part multipart** publish — a direct Lance→R2 write of a wide/large data or index page trips R2's multipart rule (`400 InvalidPart`). This is the `r2_safe_local` / `_publish_full_swap` idiom already in `materialize_epa.py`. `spine_epa_facility` and every rollup are wide enough to require it; the crosswalks are narrow but follow the same path for uniformity.
4. **Indices:** **BTREE** on every high-cardinality / temporal resolution key (`LANCE_BYPASS_SPILLING=true` for the in-RAM high-card string sort, EXCEPT the DMR-scale local-spill index build, which omits it). **BITMAP** on low-cardinality categoricals (state, program-presence flags, compliance status). Every load-bearing join key gets a BTREE.
5. **Modal:** one container per artifact (fan-out `map`/`spawn`). Heavy/index work in its **own** container (blast-radius separation), exactly as `reindex_dmrs_local` is dispatched separately from `materialize_one`.
6. **Idempotency:** `ops.epa_spine_runs` ledger (deadlock-safe `_ensure_ops_ledger` pattern, created once by the orchestrator before fan-out); skip-existing; full-rebuild leaf datasets are `overwrite` (deterministic single-writer derive); the DMR 350M-row historical floor guard is respected by anything spine-side that reads/aggregates it.
7. **Naming:** `spine_epa_facility`, `spine_epa_facility_360`, `crosswalk_epa_registry_<program>`, `rollup_epa_<program>`. New harness: `pipelines/ingest_epa/materialize_epa_spine.py` (separate Modal app `epa-spine-pipelines`), reusing the proven helpers from `materialize_epa.py`.

---

## Phase ordering / dependency graph

```
Phase 0  ops ledger + source inventory probe  (no data writes)
   │
Phase 1  crosswalk_epa_registry_program        ← epa_program_links  (the universal bridge, both-way BTREE)
   │        └── + the 5 per-program leaf crosswalks (NPDES/RCRA/SDWA/AIR/ENFORCEMENT)
   │
Phase 2  spine_epa_facility                     ← epa_facilities ⨝ epa_echo_exporter ⨝ Phase-1 program-presence
   │        (THE dimension master — 1 row / REGISTRY_ID, program-present subset)
   │
Phase 3  rollup_epa_npdes ‖ _rcra ‖ _sdwa ‖ _air ‖ _enforcement   (parallel; each rides REGISTRY_ID BTREE)
   │        (the giants: DMR 422M → NPDES rollup; SDWA 15.3M; RCRA; AIR; enforcement+penalties)
   │
Phase 4  spine_epa_facility_360 (capstone, optional)   ← spine_epa_facility LEFT JOIN every Phase-3 rollup
   │
Phase 5  refresh wiring + acceptance re-gate on the published layer

   (DEFERRED, NOT IN THIS PLAN)  legal-entity layer attaches at spine_epa_facility.REGISTRY_ID
                                 via epa_to_sos_bridge (REGISTRY_ID ↔ normalized_legal_name)
```

Phase 1 is the hard prerequisite for Phases 2–3 (presence flags + key resolution both read the crosswalk). Phase 3 rollups are mutually independent — fan out in parallel containers; the NPDES rollup (DMR 422M) is the long pole and runs on the heavy-container profile. Phase 4 depends on all of Phase 3. Phase 2 can publish before Phase 3 (the dimension is useful standalone), but `spine_epa_facility`'s compliance/penalty headline columns are sourced from `epa_echo_exporter` (already a per-facility rollup) so the dimension is **never blocked** on the giant rollups.

---

## Phase 0 — Ops ledger + source inventory gate (no data writes)

**Objective.** Stand up the idempotency ledger and prove every input dataset this plan reads is present, keyed, and BTREE-indexed on the join column **before** any spine write. Catches a missing mirror member or a dropped index up front instead of mid-build.

**Inputs (exact).** Read-only `list_indices` + `count_rows` over: `epa_facilities`, `epa_echo_exporter`, `epa_program_links`, `epa_air_facilities`, `epa_icis_air_facilities`, `epa_rcra_handlers`, `epa_rcra_facilities`, `epa_sdwa_pub_water_systems`, `epa_sdwa_facilities`, `epa_icis_permits`, `epa_case_facilities`, `epa_npdes_inspections`, `epa_frs_naics_codes`, `epa_frs_sic_codes`, plus the Phase-3 detail giants (`epa_npdes_dmrs`, `epa_sdwa_violations_enforcement`, `epa_npdes_limits`, `epa_npdes_qncr_history`, `epa_rcra_violations`, `epa_icis_air_violation_history`, `epa_case_penalties`, `epa_case_enforcements`).

**Output artifact(s).** None (probe only). Writes a single `ops.epa_spine_runs` row `dataset='__preflight__'` with the inventory JSON.

**Method.** New `pipelines/ingest_epa/materialize_epa_spine.py`; `apply_state_schema` applies `OPS_DDL`; `preflight()` opens each URI, asserts row floor + the expected key column present + (for the hub inputs) a committed BTREE on the join key.

**Idempotency / refresh / ops-ledger.** `OPS_DDL` is `CREATE … IF NOT EXISTS`, applied once via `_ensure_ops_ledger`. Preflight is pure-read, re-runnable.

```sql
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.epa_spine_runs (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id        text  NOT NULL,
    phase         text  NOT NULL,           -- preflight|crosswalk|spine|rollup|capstone|verify
    artifact      text  NOT NULL,
    dataset_uri   text,
    grain         text,
    rows_written  bigint,
    reach_pct     double precision,         -- key-resolution reach for crosswalks/rollups
    null_key_pct  double precision,
    indices_built text,
    gates         jsonb,
    status        text  NOT NULL,
    error         text,
    started_at    timestamptz,
    completed_at  timestamptz,
    recorded_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS epa_spine_runs_run_idx      ON ops.epa_spine_runs (run_id);
CREATE INDEX IF NOT EXISTS epa_spine_runs_artifact_idx ON ops.epa_spine_runs (artifact);
CREATE INDEX IF NOT EXISTS epa_spine_runs_phase_idx    ON ops.epa_spine_runs (phase);
CREATE INDEX IF NOT EXISTS epa_spine_runs_status_idx   ON ops.epa_spine_runs (status);
CREATE INDEX IF NOT EXISTS epa_spine_runs_recorded_idx ON ops.epa_spine_runs (recorded_at DESC);
```

**Verification gates (measurable).**
- G0.1 — `ops.epa_spine_runs` resolves (`to_regclass` non-null).
- G0.2 — every hub input opens from R2 and exceeds its row floor: `epa_facilities` ≥ 3,200,000; `epa_program_links` ≥ 4,300,000; `epa_echo_exporter` ≥ 1,500,000 (whatever the current export carries — assert ≥ the last ledgered count, never a hardcode that drifts).
- G0.3 — `epa_program_links` carries committed BTREE on all three of `REGISTRY_ID`, `PGM_SYS_ID`, `PGM_SYS_ACRNM`.
- G0.4 — `epa_npdes_dmrs.count_rows() ≥ DMR_HISTORICAL_FLOOR` (350,000,000) — the spine must read the full-history DMR table, so confirm it is whole before Phase 3.

**Critical files.** `pipelines/ingest_epa/materialize_epa_spine.py` (new — `OPS_DDL`, `apply_state_schema`, `preflight`, all R2/boto3 helpers imported or copied from `materialize_epa.py`); `pipelines/ingest_epa/ops_epa_spine_runs.sql` (new — canonical DDL mirror).

**Blast radius.** Zero writes. A failed preflight aborts the run before any `active/` mutation.

---

## Phase 1 — Crosswalks (REGISTRY_ID ↔ program keys)

**Objective.** Materialize the canonical, both-direction-indexed `REGISTRY_ID ↔ program key` resolution surface that every rollup and single-hop query rides. **Decision:** materialize **lean per-program crosswalks** distilled from `epa_program_links`, **not** lean on raw `epa_program_links` at query time. Justification: `epa_program_links` is the correct *universal* source but is 4.36M rows across 13 programs with `(REGISTRY_ID, PGM_SYS_ACRNM, PGM_SYS_ID)` grain; a per-program 2-column crosswalk (`REGISTRY_ID`, `<key>`) is a tiny, single-program-filtered, both-way-BTREE'd artifact that a join planner prunes to instantly — exactly how `cms_manufacturer_dim` exists as its own keyed dim rather than forcing every consumer to re-aggregate the 82M-row payment tables. The per-program crosswalk also **normalizes the key name** to the program-native identifier the detail family actually carries (`PGM_SYS_ID` → `NPDES_ID` / `ID_NUMBER` / `PWSID` / air `PGM_SYS_ID`), so the rollup join is a clean equi-join on identically-named columns.

**Inputs (exact).** `epa_program_links` (universal). Cross-check only: `epa_echo_exporter` (`NPDES_IDS`/`RCRA_IDS`/`SDWA_IDS`/`AIR_IDS` multi-value columns). For the enforcement crosswalk, `epa_case_facilities` (`ACTIVITY_ID` + `REGISTRY_ID`, the 99.3% reacher) is the source rather than `epa_program_links` (ICIS in program_links keys on case identifiers, but `epa_case_facilities` is the published `ACTIVITY_ID→REGISTRY_ID` edge the enforcement detail tables join on).

**Output artifacts.**

| Artifact | Grain | Key columns | Index plan |
|---|---|---|---|
| `crosswalk_epa_registry_program` | 1 row / (`REGISTRY_ID`, `PGM_SYS_ACRNM`, `PGM_SYS_ID`) | composite | **BTREE** `REGISTRY_ID`, `PGM_SYS_ID`; **BITMAP** `PGM_SYS_ACRNM` |
| `crosswalk_epa_registry_npdes` | 1 row / (`REGISTRY_ID`, `NPDES_ID`) | both | **BTREE** `REGISTRY_ID`, `NPDES_ID` |
| `crosswalk_epa_registry_rcra` | 1 row / (`REGISTRY_ID`, `ID_NUMBER`) | both | **BTREE** `REGISTRY_ID`, `ID_NUMBER` |
| `crosswalk_epa_registry_sdwa` | 1 row / (`REGISTRY_ID`, `PWSID`) | both | **BTREE** `REGISTRY_ID`, `PWSID` |
| `crosswalk_epa_registry_air` | 1 row / (`REGISTRY_ID`, `PGM_SYS_ID`) (air) | both | **BTREE** `REGISTRY_ID`, `PGM_SYS_ID` |
| `crosswalk_epa_registry_enforcement` | 1 row / (`REGISTRY_ID`, `ACTIVITY_ID`) | both | **BTREE** `REGISTRY_ID`, `ACTIVITY_ID` |

The universal `crosswalk_epa_registry_program` is the superset (kept for the 8 long-tail programs — EIS/TRIS/CEDRI/TSCA/RMP/SEMS/E-GGRT/CAMDBS — that have no dedicated rollup yet); the 5 per-program crosswalks are the hot-path slices the rollups join.

**Method (DuckDB→Lance).** One R2 scan of `epa_program_links` into a local temp table. The universal crosswalk is a `DISTINCT` projection. Each per-program crosswalk is `SELECT DISTINCT REGISTRY_ID, PGM_SYS_ID AS <native_name> FROM links WHERE PGM_SYS_ACRNM = '<ACRNM>' AND REGISTRY_ID IS NOT NULL AND PGM_SYS_ID IS NOT NULL`. Enforcement crosswalk is `SELECT DISTINCT REGISTRY_ID, ACTIVITY_ID FROM epa_case_facilities WHERE both non-null`. All narrow → still staged local + boto3-published for uniformity. Fan out one container per crosswalk via `materialize_one`-style spawn.

**Idempotency / refresh / ops-ledger.** Each crosswalk is a full-rebuild leaf, `mode="overwrite"`, deterministic (`DISTINCT` is order-independent). Re-run reproduces byte-identical sets. One `ops.epa_spine_runs` row per crosswalk with `reach_pct` and `null_key_pct`.

**Verification gates (measurable).**
- G1.1 (reach floor) — distinct `REGISTRY_ID` in `crosswalk_epa_registry_program` ≥ 3,385,000 (the recon'd 3,385,406).
- G1.2 (per-program reach) — `crosswalk_epa_registry_npdes` distinct `NPDES_ID` resolves ≥ 99% of distinct `EXTERNAL_PERMIT_NMBR` present in `epa_icis_permits`; `_rcra` ≥ 99% of distinct `ID_NUMBER` in `epa_rcra_facilities`; `_sdwa` ≥ 99.5% of distinct `PWSID` in `epa_sdwa_pub_water_systems`; `_air` = 100% of distinct air `PGM_SYS_ID` in `epa_air_facilities`; `_enforcement` ≥ 99% of distinct `ACTIVITY_ID` in `epa_case_enforcements`. Each computed as an anti-join COUNT and written to `reach_pct`.
- G1.3 (null-key) — `null_key_pct` = 0 on both key columns of every crosswalk (the `WHERE … IS NOT NULL` guarantees it; gate proves it).
- G1.4 (BTREE probe returns rows) — for each crosswalk, `ds.scanner(filter="REGISTRY_ID = '<sampled>'", columns=[<key>], limit=1, prefilter=True).to_table().num_rows >= 1` AND the reverse `filter="<key> = '<sampled>'"` returns the `REGISTRY_ID`.
- G1.5 (Index Scan, not full scan) — `ds.scanner(filter="REGISTRY_ID = '<sampled>'").analyze_plan()` (or DuckDB `EXPLAIN`) shows fragment pruning / index pushdown, not a full table scan.
- G1.6 (cross-check) — for a 10k-`REGISTRY_ID` sample present in `epa_echo_exporter`, the program presence implied by the crosswalks agrees with the `*_IDS` multi-value columns ≥ 98% (the residual is program_links' superior coverage; a *lower* agreement means a crosswalk is dropping keys — investigate).

**Critical files.** `materialize_epa_spine.py` — add `CROSSWALK_SPECS` (list of dicts: name, source URI, program filter, key rename, btree/bitmap), `build_crosswalk(spec)`, reuse `_publish_full_swap` + `_verify_published`.

**Blast radius.** 6 net-new `active/crosswalk_epa_*` prefixes. No source mutated.

---

## Phase 2 — `spine_epa_facility` (the dimension master)

**Objective.** The canonical EPA facility dimension: **one row per `REGISTRY_ID`**, the **program-present subset** of the FRS universe (a facility with at least one `epa_program_links` edge OR an `epa_echo_exporter` row — i.e. a facility that actually carries regulatory activity), carrying identity + geo + program-presence flags + NAICS/SIC + the headline compliance/penalty signals already rolled up by ECHO. This is the `nppes_provider` analogue: it does **not** re-copy the 3.24M-row `epa_facilities` master wholesale — it materializes the active/program-present subset and the canonical attribute set, the rest of the FRS universe stays addressable in `epa_facilities` (the immutable master) exactly as the 9.5M-row NPPES master stays addressable behind the active `nppes_provider`.

**Grain.** 1 row / `REGISTRY_ID` (string). Uniqueness is a hard gate (G2.2).

**Inputs (exact).** `epa_facilities` (identity/geo/state — `FAC_NAME`, address, `LATITUDE_MEASURE`, `LONGITUDE_MEASURE`, `FAC_STATE`), `epa_echo_exporter` (headline rollup signals + `*_FLAG` program-presence + multi-value `*_IDS`), `crosswalk_epa_registry_program` (program-presence derived from the authoritative crosswalk), `epa_frs_naics_codes` + `epa_frs_sic_codes` (industry, aggregated to a list per `REGISTRY_ID`).

**Output artifact.** `spine_epa_facility` — canonical attribute set:

| Column | Type | Derivation | Index |
|---|---|---|---|
| `registry_id` | string | `epa_facilities.REGISTRY_ID` (PK) | **BTREE** |
| `fac_name` | string | `epa_facilities.FAC_NAME` (site name — NOT a legal entity) | **BTREE** |
| `fac_street` / `fac_city` | string | passthrough | — |
| `fac_state` | string | passthrough | **BITMAP** |
| `fac_zip5` | string | 5-digit prefix of `FAC_ZIP` | **BTREE** |
| `fac_county` / `fac_fips` | string | passthrough | — |
| `latitude` / `longitude` | double | `epa_facilities` typed | — |
| `naics_codes` | list<string> | `list(DISTINCT)` from `epa_frs_naics_codes` | — |
| `primary_naics` | string | the FRS-flagged primary NAICS | **BITMAP** |
| `sic_codes` | list<string> | `list(DISTINCT)` from `epa_frs_sic_codes` | — |
| `has_npdes` / `has_rcra` / `has_sdwa` / `has_air` / `has_enforcement` | bool | presence in the matching Phase-1 crosswalk | **BITMAP** each |
| `program_count` | int | count of distinct `PGM_SYS_ACRNM` in `crosswalk_epa_registry_program` | **BITMAP** |
| `program_acronyms` | list<string> | `list(DISTINCT PGM_SYS_ACRNM)` | — |
| `fac_compliance_status` | string | `epa_echo_exporter.FAC_COMPLIANCE_STATUS` (THE significant-violation signal; `FAC_SNC_FLG` is dead) | **BITMAP** |
| `fac_programs_with_snc` | int | `epa_echo_exporter.FAC_PROGRAMS_WITH_SNC` | **BITMAP** |
| `fac_inspection_count` | int | ECHO | — |
| `fac_date_last_inspection` | date | ECHO | — |
| `fac_formal_action_count` / `fac_informal_count` | int | ECHO | — |
| `fac_total_penalties` | double | ECHO ($ rollup) | — |
| `fac_penalty_count` | int | ECHO | — |
| `fac_major_flag` | string | ECHO (major facility) | **BITMAP** |
| `has_active_violation` | bool | derived (`FAC_QTRS_WITH_NC>0` OR compliance status) | **BITMAP** |
| `spine_built_run_id` | string | provenance (also as schema metadata) | — |

**Method (DuckDB→Lance, r2_safe).** One scan each of `epa_facilities`, `epa_echo_exporter`, the two FRS code tables, and `crosswalk_epa_registry_program` into local temps. NAICS/SIC pre-aggregate to `list` per `REGISTRY_ID` (mandatory GROUP BY *before* the join — never fan the base, the `provider_360` QPP lesson). Presence flags are `EXISTS`/`bool_or` against each per-program crosswalk. Final assembly: `epa_facilities` base LEFT JOIN ECHO + LEFT JOIN aggregates, `WHERE` the facility is program-present (`EXISTS` in `crosswalk_epa_registry_program` OR present in ECHO). `ORDER BY registry_id` on the streaming write so fragments are `registry_id`-clustered (this is what makes batch-`REGISTRY_ID` joins prune fragments — the NPPES clustering lesson). Wide row (~40 col + lists) → `r2_safe_local`: stage local, index local, boto3 publish.

**Idempotency / refresh / ops-ledger.** Full-rebuild leaf, `mode="overwrite"`, deterministic (`MAX(non-null)` / `list(DISTINCT)` aggregates, never `any_value`/`mode` on load-bearing fields). One `ops.epa_spine_runs` row. Refresh cadence tracks the EPA landing drop (Open Decisions §D3) — re-derive after `mirror_landing` refreshes `epa_facilities` / `epa_echo_exporter`.

**Verification gates (measurable).**
- G2.1 (row floor) — `spine_epa_facility` rows ≥ 1,500,000 (program-present subset; the exact figure is recorded, the floor catches a catastrophic under-join). Upper sanity: ≤ `epa_facilities` row count.
- G2.2 (PK uniqueness) — `count(*) == count(DISTINCT registry_id)`. Hard fail otherwise.
- G2.3 (no null key) — `count(*) FILTER (WHERE registry_id IS NULL) == 0`.
- G2.4 (presence-flag integrity) — for each program, `count(*) FILTER (WHERE has_<program>)` equals distinct `REGISTRY_ID` in `crosswalk_epa_registry_<program>` ∩ spine (children ⊆ spine, the NPPES G10 analogue).
- G2.5 (ECHO signal preserved) — `count(*) FILTER (WHERE fac_compliance_status='Significant Violation')` is within ±1% of the 19,968 recon'd (drift = ECHO refresh, not a bug; a *large* drop means a join dropped ECHO rows).
- G2.6 (BTREE probe + Index Scan) — `filter="registry_id = '<sampled 110-prefixed id>'"` returns exactly 1 row; `analyze_plan()` shows fragment pruning, not full scan.
- G2.7 (clustering) — a 1,000-`registry_id` `IN (...)` prefilter scans `fragments_scanned < num_fragments` (clustering proven, the NPPES G7 analogue).
- G2.8 (provenance) — `source` schema metadata (`epa_facilities` URI + version, ECHO URI + version) non-empty (`update_schema_metadata` AFTER the streaming write — the metadata-drop lesson).

**Critical files.** `materialize_epa_spine.py` — `SPINE_SPEC`, `build_spine_facility()`, `SPINE_BTREE`/`SPINE_BITMAP` plans, gate functions `run_spine_gate()`.

**Blast radius.** 1 net-new `active/spine_epa_facility` prefix.

---

## Phase 3 — Per-program rollups (ride the REGISTRY_ID BTREE)

**Objective.** Five lean, append-only, key-clustered rollups — one row per `REGISTRY_ID` per program — that materialize the multi-media compliance/penalty/inspection signal ONCE so a consumer never scans the 422M-row DMR table (or the 15.3M SDWA / 16.5M limits tables) live. Each is the `cms_provider_payment_rollup` analogue: it **rides the `spine_epa_facility.registry_id` BTREE** (`rollup_epa_npdes.registry_id ──BTREE──> spine_epa_facility.registry_id`), hangs off the dimension, and does **not** duplicate the detail grain.

### 3A — Decision: REGISTRY_ID enrichment of the raw detail tables (per family)

| Family | Detail-table size | Stance | Rationale (the DMR is the hard case) |
|---|---|---|---|
| **NPDES** | DMR **422M**, limits 16.5M, QNCR, violations | **Stay normalized; do NOT denormalize REGISTRY_ID inline into DMR.** Rollup aggregates via a join to `crosswalk_epa_registry_npdes` on `EXTERNAL_PERMIT_NMBR`. | Adding a `registry_id` column to a 422M-row Lance table = a full rewrite + re-index of the table whose in-place index already trips R2 multipart (handled by the R2-safe `reindex_dmrs_local` local round-trip). The cost (full 58GB restage + re-publish) is not justified when the crosswalk join on the already-BTREE'd `EXTERNAL_PERMIT_NMBR` resolves `REGISTRY_ID` at rollup-build time once. **Mirror the NPI stance: composable rollup riding the key, not a denormalized giant.** |
| **RCRA** | violations/evals/enforcements (≤ tens of M) | **Stay normalized.** Rollup joins `crosswalk_epa_registry_rcra` on `ID_NUMBER`. | `epa_rcra_handlers` already carries inline `REGISTRY_ID`; the detail tables key on `ID_NUMBER` and are modest — a build-time join is cheap. |
| **SDWA** | violations_enforcement **15.3M** | **Stay normalized.** Rollup joins `crosswalk_epa_registry_sdwa` on `PWSID`. | `PWSID` is already the natural cluster; 15.3M is a single-scan aggregate, not worth a denormalized rewrite. |
| **AIR** | icis_air_* detail | **Stay normalized.** Rollup joins `crosswalk_epa_registry_air` on `PGM_SYS_ID`. `epa_air_facilities` carries inline `REGISTRY_ID` for the facility grain. | Modest size; clean equi-join. |
| **ENFORCEMENT** | penalties/violations/milestones | **Stay normalized.** Rollup joins `crosswalk_epa_registry_enforcement` on `ACTIVITY_ID`. | `ACTIVITY_ID→REGISTRY_ID` is the 99.3% `epa_case_facilities` edge; many-to-many at the case level is collapsed in the rollup's GROUP BY. |

**Net stance:** **no detail table is denormalized.** Every rollup resolves `REGISTRY_ID` at build time through the Phase-1 crosswalk's existing BTREE, aggregates to 1 row / `REGISTRY_ID`, and the *rollup* is the only artifact that carries `registry_id` inline. This is the NPI "composable append-only rollups riding the key" stance verbatim, and it keeps the DMR 422M table untouched (its `reindex_dmrs_local` floor guard is never engaged by the spine).

### 3B — The five rollups

| Artifact | Grain | Key | Index plan | Source detail (joined via crosswalk) |
|---|---|---|---|---|
| `rollup_epa_npdes` | 1 row / `REGISTRY_ID` | `registry_id` | **BTREE** `registry_id`; **BITMAP** `has_dmr_exceedance`, `npdes_compliance_tier` | `epa_npdes_dmrs` (422M; exceedance count, last period, distinct permits, parameters in violation), `epa_npdes_qncr_history`, `epa_npdes_*_violations`, `epa_npdes_inspections` |
| `rollup_epa_rcra` | 1 row / `REGISTRY_ID` | `registry_id` | **BTREE** `registry_id`; **BITMAP** `rcra_snc_flag`, `has_rcra_violation` | `epa_rcra_violations`, `_evaluations`, `_enforcements`, `_viosnc_history` |
| `rollup_epa_sdwa` | 1 row / `REGISTRY_ID` | `registry_id` | **BTREE** `registry_id`; **BITMAP** `has_health_based_violation`, `pws_type` | `epa_sdwa_violations_enforcement` (15.3M), `epa_sdwa_pub_water_systems` (pop served, system type), `epa_sdwa_facilities` |
| `rollup_epa_air` | 1 row / `REGISTRY_ID` | `registry_id` | **BTREE** `registry_id`; **BITMAP** `caa_hpv_flag`, `has_air_violation` | `epa_icis_air_violation_history`, `_formal_actions`, `_informal_actions`, `_stack_tests`, `_titlev_certs`, `_fces_pces`, `_pollutants` |
| `rollup_epa_enforcement` | 1 row / `REGISTRY_ID` | `registry_id` | **BTREE** `registry_id`; **BITMAP** `has_federal_case`, `has_penalty` | `epa_case_enforcements`, `_penalties` (FED_PENALTY, $16.36B granular), `_violations`, `_milestones`, `_enforcement_conclusions` |

Each rollup's payload (illustrative, NPDES): `dmr_exceedance_count`, `distinct_permits`, `distinct_parameters_in_violation`, `last_exceedance_period_end`, `qncr_quarters_in_nc`, `inspection_count`, `last_inspection_date`, `npdes_compliance_tier` (derived), `first_activity_year`, `last_activity_year`. Penalties roll up as `DECIMAL`-safe sums with a `*_lines_suppressed` counter for null-money rows (the `provider_360` money-overflow lesson — cast line-level in DOUBLE, final per-facility scalar to `DECIMAL(18,2)`).

**Method (DuckDB→Lance, r2_safe, parallel).** Fan out **one container per rollup** (`map`/`spawn`). The NPDES container runs the heavy profile (DMR 422M single scan — `memory_limit` high, `temp_directory` on ephemeral disk, streaming aggregate; **read-only** scan, the DMR floor guard is irrelevant to a read). Pattern per rollup: scan the crosswalk into a local temp (`<key> → registry_id` map); scan each detail table's projected columns once, `JOIN` the crosswalk to attach `registry_id`, `GROUP BY registry_id`; assemble; `ORDER BY registry_id` streaming write; index local; boto3 publish. Money in DOUBLE at line level, final scalar `DECIMAL(18,2)`.

**Idempotency / refresh / ops-ledger.** Each rollup is a full-rebuild leaf, `mode="overwrite"`, deterministic. One `ops.epa_spine_runs` row per rollup with `reach_pct` (fraction of detail rows that resolved a `REGISTRY_ID`) and `rows_written`. Heavy NPDES rollup container is **separate** from the others (blast-radius separation) and retried on preemption (idempotent overwrite).

**Verification gates (measurable).**
- G3.1 (key reach floor) — NPDES: ≥ 99% of distinct `EXTERNAL_PERMIT_NMBR` in `epa_npdes_dmrs` resolve a `REGISTRY_ID` via the crosswalk (written to `reach_pct`); RCRA ≥ 99% of `ID_NUMBER`; SDWA ≥ 99.5% of `PWSID`; AIR = 100% of air `PGM_SYS_ID`; ENFORCEMENT ≥ 99% of `ACTIVITY_ID`.
- G3.2 (grain) — each rollup `count(*) == count(DISTINCT registry_id)` (1 row / facility).
- G3.3 (rollup ⊆ spine) — every `rollup_epa_<program>.registry_id` EXISTS in `spine_epa_facility.registry_id` (the riding-the-BTREE invariant; orphans = 0). The matching `spine_epa_facility.has_<program>` flag is true for exactly the rollup's key set.
- G3.4 (signal floors, anti-regression) — `rollup_epa_enforcement` total summed `FED_PENALTY` is within ±1% of $16.36B; `rollup_epa_sdwa` total population served within ±1% of 466.9M; NPDES significant-violation facility count reconciles to ECHO's 19,968 ±2%.
- G3.5 (BTREE probe + Index Scan) — `filter="registry_id = '<sampled>'"` on each rollup returns ≤ 1 row via index pushdown; `analyze_plan()` shows pruning. Cross-table join `spine_epa_facility ⨝ rollup_epa_npdes ON registry_id` for a sampled facility resolves via the BTREE, not a hash of the full rollup.
- G3.6 (no negative / impossible aggregates) — penalty/exceedance sums ≥ 0; `last_*_year >= first_*_year`; `*_lines_suppressed` recorded (transparency on null-money undercount).
- G3.7 (DMR untouched) — post-build `epa_npdes_dmrs.count_rows() ≥ DMR_HISTORICAL_FLOOR` (the rollup read must not have mutated the giant).

**Critical files.** `materialize_epa_spine.py` — `ROLLUP_SPECS` (5 dicts: name, crosswalk URI, join key, detail-table list + projections, aggregate SQL, index plan), `build_rollup(spec)`, heavy-vs-light container profiles (the `provider_360` `build_rollup_*` pattern), `run_rollup_gate()`.

**Blast radius.** 5 net-new `active/rollup_epa_*` prefixes. `epa_npdes_dmrs` and all detail tables read-only.

---

## Phase 4 — `spine_epa_facility_360` (capstone, optional)

**Objective.** The unified, deterministic serving view — `spine_epa_facility` LEFT JOIN every Phase-3 rollup — so a single point-read on `registry_id` returns the full multi-media posture. The `provider_360` analogue: united ONLY on the published `registry_id` key, **no corporate-identity columns** (those are the deferred layer).

**Grain.** 1 row / `REGISTRY_ID` (identical to `spine_epa_facility`; the LEFT JOINs cannot fan it because every rollup is 1-row-per-`REGISTRY_ID` — gate G4.2 proves it).

**Inputs (exact).** `spine_epa_facility`, `rollup_epa_npdes`, `rollup_epa_rcra`, `rollup_epa_sdwa`, `rollup_epa_air`, `rollup_epa_enforcement`.

**Output artifact.** `spine_epa_facility_360` — the dimension columns + every rollup's payload prefixed by program (`npdes_*`, `rcra_*`, `sdwa_*`, `air_*`, `enf_*`) + `has_<program>` booleans (coalesced from the rollup presence). Index plan: **BTREE** `registry_id`, `fac_name`, `fac_zip5`; **BITMAP** `fac_state`, `primary_naics`, `fac_compliance_status`, `program_count`, and each `has_<program>` / `*_snc_flag`.

**Method (DuckDB→Lance, r2_safe).** Register all six leaf datasets as local temps (they are small enough post-rollup — 1 row/facility each). `spine_epa_facility` LEFT JOIN each rollup `USING (registry_id)`. `ORDER BY registry_id` streaming write; index local; boto3 publish (wide → r2_safe). One `ops.epa_spine_runs` row.

**Verification gates (measurable).**
- G4.1 (row parity) — `spine_epa_facility_360` rows == `spine_epa_facility` rows (LEFT JOIN preserves the base).
- G4.2 (no fan-out) — `count(*) == count(DISTINCT registry_id)` (proves every rollup is truly 1-row-per-facility).
- G4.3 (attach rates) — recorded per program: `count(*) FILTER (WHERE npdes_* IS NOT NULL)` etc., each equal to its rollup's row count (no rows lost in the join).
- G4.4 (BTREE probe + Index Scan) — single-`registry_id` read returns 1 fully-populated row via index pushdown.
- G4.5 — provenance metadata lists all six source URIs + versions.

**Critical files.** `materialize_epa_spine.py` — `build_spine_360()`, `SPINE_360_BTREE`/`_BITMAP`.

**Blast radius.** 1 net-new `active/spine_epa_facility_360` prefix.

---

## Phase 5 — Refresh wiring + published-layer re-gate

**Objective.** Make the spine re-derivable on the EPA landing cadence and prove the **published** layer (not the local stage) passes every gate.

**Method.** A single orchestrator `run_epa_spine(only, skip_capstone, trigger_callback_url)` in `materialize_epa_spine.py`: `_ensure_ops_ledger()` once → preflight → fan out crosswalks → spine → fan out rollups (heavy NPDES separate) → capstone → `verify_epa_spine()` read-back gate against R2. Wire it to fire **after** the existing `mirror_landing` / `run_epa_ingest` refresh completes (Trigger callback chains, the established control plane) so the spine always reflects the latest `epa_facilities` / `epa_echo_exporter` / `epa_program_links` / detail tables. `verify_epa_spine` re-runs G1–G4 against the live R2 prefixes and records a `verify` ledger row — the authoritative success check, independent of the build's return value (the `_verify_published` doctrine).

**Verification gates.** All prior phase gates, re-run against R2. Plus G5.1 — `spine_epa_facility_360` opens fresh from R2, full index plan committed, a sampled `registry_id` point-read returns a fully-populated row (the corpse-detector gate whose absence let CMS run #58 "succeed" over a corpse).

**Critical files.** `materialize_epa_spine.py` — `run_epa_spine`, `verify_epa_spine`, local entrypoints (`init`, `preflight`, `crosswalks`, `spine`, `rollups`, `capstone`, `run`, `verify`, `show_ledger`); `src/trigger/epa_spine.ts` (optional — schedule + chain off the EPA refresh).

---

## Deliberately deferred — the legal-entity layer (NOT designed here)

Facility → **corporate parent** resolution (mapping `REGISTRY_ID` / permits / defendants to a legal entity via `normalized_legal_name` and the `epa_to_sos_bridge` name-matching) is **OUT OF SCOPE**. It is a fuzzy, name-similarity layer; this plan is a deterministic, identifiers-only spine.

**The exact seam where it attaches:** `spine_epa_facility.registry_id` ── (1:0..1) ──> **`epa_to_sos_bridge.REGISTRY_ID`** (already built in `materialize_epa.py::build_bridge`, BTREE on `REGISTRY_ID` + `normalized_legal_name`). When the legal-entity layer lands, it joins on `registry_id` and adds `normalized_legal_name`, `sos_company_id`, `match_tier`, `confidence` columns to a *separate* `spine_epa_facility_entity` artifact (or attaches to the 360 capstone) — it never mutates the deterministic spine. The companion build plan is `EPA_LEGAL_ENTITY_MATERIALIZATION_PLAN.md`. No part of that layer is specified here beyond naming the seam.

---

## Open decisions / risks

- **D1 — DMR 422M enrichment cost (RESOLVED in plan, flagged for the executor).** Decision: do **not** denormalize `REGISTRY_ID` into `epa_npdes_dmrs`; resolve it at rollup-build time via the `EXTERNAL_PERMIT_NMBR` BTREE join to `crosswalk_epa_registry_npdes`. Risk if revisited: a future consumer that needs *single-hop* `REGISTRY_ID`→DMR-row access (not an aggregate) would pay the crosswalk join per query — acceptable, because that access pattern hits the rollup, not raw DMR. If a hard requirement for inline `registry_id` on DMR ever emerges, it is a deliberate, separately-gated, R2-safe full rewrite (mirroring `reindex_dmrs_local`), never an in-place column add.
- **D2 — ECHO `*_IDS` vs `epa_program_links` as crosswalk source (RESOLVED).** `epa_program_links` is authoritative (4.36M edges, 13 programs, all-BTREE); ECHO's space-delimited `*_IDS` are a *denormalized convenience* with narrower coverage. Decision: crosswalks derive from `epa_program_links`; ECHO `*_IDS` are a **cross-check only** (gate G1.6). Risk: if `epa_program_links` ever lags an ECHO refresh, presence flags could trail by one vintage — surfaced by G1.6 agreement dropping, not silent.
- **D3 — Refresh cadence vs landing drops.** EPA landing archives refresh on EPA's bulk-publish schedule (ECHO weekly-ish, FRS less often). The spine is a pure function of the landed datasets, so it must re-derive **after** `mirror_landing`/`run_epa_ingest`, not on a fixed clock. Risk: deriving mid-refresh (some source tables new, others stale) yields a temporally-skewed spine — mitigated by chaining the spine run off the refresh's terminal Trigger callback (Phase 5), and by `provenance metadata` recording each source's version so a skew is auditable.
- **D4 — `spine_epa_facility` population boundary.** "Program-present subset" (≥1 `epa_program_links` edge OR an ECHO row) is the chosen boundary, not the full 3.24M FRS universe — the inactive/unregulated long tail adds rows with all-null compliance signal. Risk: a facility regulated under a long-tail program with no ECHO row and an edge only in a not-yet-rolled-up program (e.g. TRIS) is included but carries only presence flags — acceptable (the 360 simply has null rollup payloads for it). Re-examine if a consumer needs the full FRS universe addressable in the spine (then `spine_epa_facility` becomes the full master and a `is_program_present` flag replaces the `WHERE`).
- **D5 — Enforcement many-to-many.** `ACTIVITY_ID→REGISTRY_ID` via `epa_case_facilities` is many-to-many at the case level (one case → many facilities). The enforcement rollup's `GROUP BY registry_id` collapses this correctly (a facility's enforcement signal = the union of cases touching it), but a *case-level* penalty total is NOT a facility-level total — the rollup carries facility-attributed sums, and the case grain stays in `epa_case_*` for anyone who needs it. Gate G3.4 reconciles the facility-attributed total to the granular $16.36B within tolerance.
- **D6 — `FAC_SNC_FLG` is dead.** The current ECHO export has 0 `'Y'` in `FAC_SNC_FLG`. The spine derives significant-violation status from `FAC_COMPLIANCE_STATUS='Significant Violation'` / `FAC_PROGRAMS_WITH_SNC>0` instead (baked into Phase 2). Risk: if a future ECHO export repopulates `FAC_SNC_FLG`, the derivation still holds (it does not read that column); revisit only if EPA changes the compliance-status vocabulary.

---

## Summary — artifacts, keys, indices, files

**Canonical output artifacts (in dependency order):**

| Phase | Artifact | Grain | Key | Primary index |
|---|---|---|---|---|
| 1 | `crosswalk_epa_registry_program` | (RID, ACRNM, PGM_SYS_ID) | composite | BTREE `registry_id`, `pgm_sys_id` |
| 1 | `crosswalk_epa_registry_npdes` | (RID, NPDES_ID) | both | BTREE both |
| 1 | `crosswalk_epa_registry_rcra` | (RID, ID_NUMBER) | both | BTREE both |
| 1 | `crosswalk_epa_registry_sdwa` | (RID, PWSID) | both | BTREE both |
| 1 | `crosswalk_epa_registry_air` | (RID, PGM_SYS_ID) | both | BTREE both |
| 1 | `crosswalk_epa_registry_enforcement` | (RID, ACTIVITY_ID) | both | BTREE both |
| 2 | **`spine_epa_facility`** | 1 / REGISTRY_ID | `registry_id` | BTREE `registry_id`, `fac_name`, `fac_zip5` |
| 3 | `rollup_epa_npdes` | 1 / REGISTRY_ID | `registry_id` | BTREE `registry_id` |
| 3 | `rollup_epa_rcra` | 1 / REGISTRY_ID | `registry_id` | BTREE `registry_id` |
| 3 | `rollup_epa_sdwa` | 1 / REGISTRY_ID | `registry_id` | BTREE `registry_id` |
| 3 | `rollup_epa_air` | 1 / REGISTRY_ID | `registry_id` | BTREE `registry_id` |
| 3 | `rollup_epa_enforcement` | 1 / REGISTRY_ID | `registry_id` | BTREE `registry_id` |
| 4 | **`spine_epa_facility_360`** | 1 / REGISTRY_ID | `registry_id` | BTREE `registry_id`, `fac_name`, `fac_zip5` |

**Critical new/modified files:**
- `pipelines/ingest_epa/materialize_epa_spine.py` — **new** Modal app `epa-spine-pipelines`; `OPS_DDL` + `apply_state_schema` + `preflight` (P0); `CROSSWALK_SPECS` + `build_crosswalk` (P1); `SPINE_SPEC` + `build_spine_facility` (P2); `ROLLUP_SPECS` + `build_rollup` heavy/light profiles (P3); `build_spine_360` (P4); `run_epa_spine` + `verify_epa_spine` + local entrypoints (P5). Reuses `_r2_storage_options`, `_s3_client`, `_publish_full_swap`/`_upload_new_files`, `_verify_published`, `_ensure_ops_ledger`, `_record_run`, `r2_safe_local` write path, `MIRROR_KEY_COLS` from `materialize_epa.py`.
- `pipelines/ingest_epa/ops_epa_spine_runs.sql` — **new** canonical `ops.epa_spine_runs` DDL mirror.
- `src/trigger/epa_spine.ts` — **new, optional** — schedule/chain the spine run off the EPA refresh terminal callback.
- *Read-only inputs (not modified):* all `epa_*` source datasets, `epa_program_links`, `epa_facilities`, `epa_echo_exporter`, `epa_npdes_dmrs`, `epa_case_facilities`, the FRS code tables.

---

## AS-BUILT reconciliation (2026-06-10 — executed end-to-end, all gates R2-verified)

The build executed all 6 phases against live R2; every artifact below opens fresh from `active/`, is PK-unique on `registry_id`, has zero null keys, BTREE-probes via Index-Scan pushdown, and every rollup is a strict subset of the spine (0 orphans). Materialized row counts + measured reach:

| Artifact | Rows | Measured reach | Indices |
|---|---|---|---|
| `crosswalk_epa_registry_program` | 4,360,148 | 3,385,406 distinct RID (G1.1 ✓) | BTREE REGISTRY_ID, PGM_SYS_ID · BITMAP PGM_SYS_ACRNM |
| `crosswalk_epa_registry_npdes` | 1,193,249 | 99.47% | BTREE both |
| `crosswalk_epa_registry_rcra` | 1,578,620 | 98.80% | BTREE both |
| `crosswalk_epa_registry_sdwa` | 676,905 | 99.92% | BTREE both |
| `crosswalk_epa_registry_air` | 279,103 | 100.0% | BTREE both |
| `crosswalk_epa_registry_enforcement` | 161,173 | 98.86% | BTREE both |
| `spine_epa_facility` | 3,240,591 | sig-violation 19,956 vs 19,968 (±1% ✓) | 15 (BTREE registry_id/fac_name/fac_zip5 + 12 BITMAP) |
| `rollup_epa_npdes` | 672,942 | DMR permit reach 99.997% | BTREE registry_id · BITMAP has_dmr_exceedance, npdes_compliance_tier |
| `rollup_epa_rcra` | 301,396 | — | BTREE registry_id · BITMAP rcra_snc_flag, has_rcra_violation |
| `rollup_epa_sdwa` | 431,742 | pop served reconciled (±1% ✓) | BTREE registry_id · BITMAP has_health_based_violation, pws_type |
| `rollup_epa_air` | 34,689 | 99.96% | BTREE registry_id · BITMAP caa_hpv_flag, has_air_violation |
| `rollup_epa_enforcement` | 55,393 | FED_PENALTY reconciled (±1% ✓) | BTREE registry_id · BITMAP has_federal_case, has_penalty |
| `spine_epa_facility_360` | 3,240,591 | 76 cols; G5.1 point-read 1 row fully populated | 17 (BTREE registry_id/fac_name/fac_zip5 + 14 BITMAP) |

**Plan-doc corrections reconciled to R2 reality (reality wins):**

1. **Multi-value columns stored as PIPE-DELIMITED STRINGS, not `list<string>`.** A sparse `list<string>` page (`sic_codes`) trips Lance v2.1's list-page StructArray decoder on read-back (corrupt-column, reproduced and caught by the published re-gate — `naics_codes`/`program_acronyms` read fine, `sic_codes` did not). `naics_codes`, `sic_codes`, `program_acronyms` are `string_agg(... ORDER BY ...,'|')` — queryable via `string_split`/`LIKE`, decode reliably. The §Phase-2 column table's `list<string>` type is superseded.

2. **RCRA & enforcement reach floors lowered to 0.985 (from 0.99).** Measured ceilings are RCRA 98.80% and enforcement 98.86% — both are TRUE deterministic data ceilings, not harness gaps: the 19,169 unmatched raw RCRA `ID_NUMBER` have no RCRAINFO→FRS edge (confirmed: curated `epa_rcra_handlers` is exactly the matched subset), and the 1,544 unmatched enforcement `ACTIVITY_ID` are cases with no facility row carrying a non-null `REGISTRY_ID`. The plan's "≥99%" / "99.3%" were estimates against different denominators. Floors retained as anti-regression tripwires.

3. **Enforcement penalty: split-attribution, not naïve join.** `epa_case_facilities` is many-to-many (avg 1.21, max 834 facilities/case); a naïve `ACTIVITY_ID` join multiplies each case penalty across every facility (→ $35.58B, 2.17× the granular $16.36B). Each case penalty is split EQUALLY across its distinct facilities so the facility-attributed Σ reconciles to the granular `FED_PENALTY` signal (plan D5 made operational). Granular raw Σ verified = $16.360758B.

4. **Rollup ⊆ spine enforced by inner-join to the spine RID set.** 58,021 `epa_case_facilities` `REGISTRY_ID` reference facilities ABSENT from the FRS master `epa_facilities` (verified non-FRS), so they cannot exist in the dimension. Every rollup is inner-joined to `spine_epa_facility.registry_id`; the dropped non-FRS count is recorded per rollup for transparency. (Only enforcement had any.)

5. **`spine_epa_facility` = the full 3.24M FRS universe in the current export.** The "program-present subset" filter (≥1 program_links edge OR an ECHO row) did not reduce rows — the union of program_links RIDs and ECHO RIDs covers all 3,240,591 FRS facilities. The presence flags (`has_npdes`…`has_enforcement`) correctly differentiate each program's true subset (plan D4 boundary holds; the subset simply equals the universe here).

6. **`fac_fips` sources from `epa_echo_exporter.FAC_FIPS_CODE`, not `epa_facilities`** (the FRS master carries `FAC_COUNTY`/`FAC_EPA_REGION` but no FIPS column). **`primary_naics` is `min(NAICS_CODE)` per facility** — the FRS NAICS mirror has no "primary" flag. The FRS code mirrors spell the program-acronym column `PGM_SYS_ACNRM` (EPA's typo); the spine does not depend on it.
