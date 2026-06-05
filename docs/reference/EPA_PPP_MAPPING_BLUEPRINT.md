# EPA Multi-Media Compliance × SBA/PPP — Entity Unification Blueprint

Read-only data-forensics audit over `s3://data-sink/active/`. Pairs the EPA ECHO/FRS
compliance layer (11 datasets) to the SBA Paycheck Protection Program credit spine (`ppp`)
to determine whether — and how — the two can be united into one queryable entity surface.
This document is the structural specification for a future
`pipelines/resolution/crosswalk_ppp_epa.py` Pattern-B bridge.

**Provenance.** Every figure below was read **directly from R2** (`s3://data-sink/active/`):
objects listed with boto3, each dataset opened with `lance.dataset(uri, storage_options=…)`
under `doppler run -p core-x -c prd`. **The data-factory catalog was not used.** Lance
schemas, row counts, and index manifests are dataset metadata; all population, join, and
match figures are live DuckDB scans over the committed columns (`pylance 7`, `duckdb 1.5`,
boto3 → R2 endpoint `…e957…r2.cloudflarestorage.com`). Audit date: **2026-06-05**. Reads
were local over WAN; latency is not representative of in-region Modal execution and is
excluded.

Pipeline sources: `pipelines/ingest_epa/materialize_epa.py`,
`pipelines/sba_ppp/ppp_loans_bulk.py`,
`pipelines/resolution/credit_spine_normalize_index.py`, `core/name_norm.py`.

---

## 0. The four questions, answered

| Question | Verdict | Evidence (measured) |
|---|---|---|
| **Shared government IDs?** | **None.** No EIN / UEI / DUNS / CAGE / LEI / TIN on **either** side. | Identity-column scan across all 86 active datasets: `uei`/`duns`/`cage`/`lei` appear only in the SAM & GLEIF families; `registry_id`/`npdes_id`/`pgm_sys_id` only in `epa_*`; `loan_number` only in `ppp`. `naics_code` is **not** shared (PPP has it, EPA does not). |
| **Legal names?** | **Yes — both already carry the identical canonical key.** | `ppp.normalized_legal_name` populated 11,468,117 / 11,468,210 = **100.0%**, BTREE-indexed. EPA legal names live in `epa_to_sos_bridge.normalized_legal_name` (356,903 rows) and as raw `FAC_NAME`/`PRIMARY_NAME`/permit/defendant strings. Both produced by the **byte-identical `core.name_norm`** macro. |
| **Officers listed?** | **Not in EPA, not in PPP** — reachable only after name-resolution. | Source layers: `ca_sos_principals` (18.67M), `ca_sos_agents` (8.56M), `co_sos` agents (3.06M), `ny_sos` (`ceo_name`+`registered_agent_name`, 4.22M), `fl_sos_corporations` (`registered_agent_name`+`raw_officer_block`, 1.26M), `ffata_exec_comp` (`officer_name`, 29.6k), `edgar_form_4` (`reporting_owners`). Verified: 638,593 PPP names resolve to a CA SoS entity; **638,592 (≈100%) have ≥1 listed principal.** |
| **POCs?** | **Not in EPA, not in PPP** — reachable only via a name→UEI hop. | `sam_pocs` (8.07M) and `sam_master_contacts` (4.37M) are keyed on `uei`, which neither EPA nor PPP carries. `crosswalk_sam_usaspending` (`normalized_legal_name`+`uei`) is the name→UEI bridge that makes SAM POCs reachable by name. `people` (GTM contacts, 7.7k) is keyed on `company_id`/domain. |

**Bottom line.** EPA and PPP **cannot be joined on any hard identifier.** The only viable
union vector is `core.name_norm(legal_name) + zip5`. It works: the measured high-confidence
EPA↔PPP overlap is **60,980–73,380 distinct name+zip5 entities** (by EPA source column),
with **162,664** name-only matches as the upper bound. This is the same methodology the
sibling `uspto_sba_ppp_mapping_blueprint.md` prescribes: *no shared identifier → normalized
name (primary) + geography (corroborating), behind an explicit false-positive tiering.*

---

## 1. EPA Lance inventory (11 datasets — exhaustive)

All under `s3://data-sink/active/`. Built by `pipelines/ingest_epa/materialize_epa.py`
(Modal app `epa-pipelines`). Storage version 2.1.

| Dataset | Rows | Cols | Entity identity it carries | Names | Geo | BTREE / BITMAP |
|---|--:|--:|---|---|---|---|
| `epa_facilities` | 3,240,591 | 10 | **`REGISTRY_ID`** (FRS hub) | `FAC_NAME` (site) | `FAC_STREET/CITY/STATE/ZIP/COUNTY`, lat/long | REGISTRY_ID / FAC_STATE |
| `epa_program_links` | 4,360,148 | 13 | **`REGISTRY_ID` ↔ `PGM_SYS_ID`** (+`PGM_SYS_ACRNM`) | `PRIMARY_NAME` (entity) | `LOCATION_ADDRESS/CITY_NAME/STATE_CODE/POSTAL_CODE`, FIPS | REGISTRY_ID, PGM_SYS_ID / PGM_SYS_ACRNM |
| `epa_npdes_dmrs` | 67,597,592 | 58 | `EXTERNAL_PERMIT_NMBR` (NPDES), `ACTIVITY_ID` | — | — | EXTERNAL_PERMIT_NMBR, MONITORING_PERIOD_END_DATE / FISCAL_YEAR |
| `epa_npdes_eff_violations` | 46,361,587 | 43 | `NPDES_ID`, `ACTIVITY_ID` | — | — | NPDES_ID, MONITORING_PERIOD_END_DATE |
| `epa_npdes_qncr_history` | 7,866,031 | 8 | `NPDES_ID` | — | — | NPDES_ID, YEARQTR |
| `epa_aim_triggering_events` | 5,375 | 18 | `NPDES_ID` | `FACILITY_NAME` | `FACILITY_ADDRESS_1/2/CITY/STATE/ZIPCODE` | NPDES_ID / ACTIVE_EXCEPTION |
| `epa_case_enforcements` | 135,053 | 25 | `ACTIVITY_ID`, `CASE_NUMBER`, `DOJ_DOCKET_NMBR` | `CASE_NAME` (caption) | `STATE_CODE` | ACTIVITY_ID, CASE_NUMBER |
| `epa_case_milestones` | 508,088 | 5 | `ACTIVITY_ID`, `CASE_NUMBER` | — | — | ACTIVITY_ID, ACTUAL_DATE |
| `epa_pipeline_caa` | 66,655 | 35 | **`REGISTRY_ID`**, `SOURCE_ID` | `AIR_NAME` | — | REGISTRY_ID, SOURCE_ID / FOUND_VIOLATION |
| `epa_pipeline_rcra` | 456,773 | 30 | **`REGISTRY_ID`**, `SOURCE_ID`, `CASE_ID` | — | `*_ACTIVITY_LOCATION` | REGISTRY_ID, SOURCE_ID / FOUND_VIOLATION |
| `epa_to_sos_bridge` | 356,903 | 10 | **`REGISTRY_ID` → `sos_company_id`** | `epa_matched_name`, `normalized_legal_name` | — | REGISTRY_ID, normalized_legal_name |

### 1.1 EPA-internal join graph (verified)

- **`REGISTRY_ID` is the universal hub.** All **3,240,591** distinct facility REGISTRY_IDs
  are present in `program_links` (100% intersect; program_links spans 3,385,406 distinct).
- **`program_links` crosswalks REGISTRY_ID to 13 program systems** (`PGM_SYS_ACRNM`, with
  per-program link counts):

  | Program | Links | Distinct REGISTRY_ID |
  |---|--:|--:|
  | RCRAINFO | 1,578,620 | 1,476,648 |
  | NPDES | 1,193,249 | 1,016,966 |
  | SFDW (Safe Drinking Water) | 676,905 | 452,826 |
  | AIR | 279,103 | 265,643 |
  | ICIS | 229,937 | 207,792 |
  | EIS | 211,205 | 199,029 |
  | TRIS (Toxic Release) | 74,871 | 73,819 |
  | CEDRI | 37,445 | 35,959 |
  | TSCA | 27,020 | 18,203 |
  | RMP (Risk Management) | 21,883 | 21,594 |
  | SEMS (Superfund) | 16,188 | 16,064 |
  | E-GGRT (Greenhouse Gas) | 11,540 | 11,125 |
  | CAMDBS | 2,182 | 2,136 |

- **NPDES chain verified.** `program_links` rows with `PGM_SYS_ACRNM='NPDES'` →
  `PGM_SYS_ID` matches the `NPDES_ID` in the violation tables almost perfectly:
  `qncr_history` 683,047 / 683,077 (**99.996%**), `eff_violations` 125,641 / 125,642
  (**99.999%**), `aim_triggering_events` 395 / 395 (**100%**). Sample `PGM_SYS_ID`:
  `TXR1573WE`, `UTRH97311`, `LAG535001`, `TXG111499` — canonical NPDES permit IDs.
- **Consequence:** one resolved `REGISTRY_ID` pulls the entity's full compliance footprint —
  facilities + program_links + CAA + RCRA (REGISTRY_ID inline), plus NPDES discharge
  (`dmrs` via `EXTERNAL_PERMIT_NMBR`) and NPDES violations (`eff_violations`, `qncr`, `aim`
  via `NPDES_ID = program_links(NPDES).PGM_SYS_ID`).
- **Gap:** `epa_case_enforcements` / `epa_case_milestones` key on `ACTIVITY_ID` / `CASE_NUMBER`
  and link to `REGISTRY_ID` only through `CASE_FACILITIES`, which is **not** materialized as a
  standing dataset (it is read transiently inside the bridge build).

**EPA carries zero officers, zero POCs, and no EIN/UEI/DUNS/LEI** — confirmed across all 11
schemas.

## 2. PPP Lance inventory (1 dataset — exhaustive)

`s3://data-sink/active/ppp/` — **11,468,210 rows, 59 columns**, point-in-time FOIA snapshot
`2024-09-30`. Built by `pipelines/sba_ppp/ppp_loans_bulk.py` (Modal app `sba-ppp-pipelines`).

- **Identity:** `loan_number` (SBA loan id, BTREE), `naics_code` (6-digit industry, BTREE).
  **No entity government ID.**
- **Legal name:** `borrower_name` → **`normalized_legal_name`** (BTREE, 100.0% populated).
  Also `franchise_name`, `servicing_lender_name`, `originating_lender`.
- **Geography:** `borrower_address/city/state/zip` (`borrower_state` 100.0% populated),
  `project_city/county/state/zip`, `congressional_district`, and derived **`zip_code`**
  (zip5, BTREE, 100.0% populated).
- **Loan / demographics:** `loan_status`, amounts (`initial_approval_amount`,
  `current_approval_amount`, `forgiveness_amount`, proceeds breakdown), `date_approved`,
  `jobs_reported`, `business_type`, `race`, `ethnicity`, `gender`, `veteran`, `non_profit`.
- **Officers / POCs:** **none** — PPP FOIA never published owner or contact identity.

**Critical (and absent from the pipeline source).** The live `ppp` carries **two appended
columns not written by `ppp_loans_bulk.py`** — `normalized_legal_name` and `zip_code`, each
BTREE-indexed. They were added in place by the maintenance worker
`pipelines/resolution/credit_spine_normalize_index.py` (Task E), which applies the
**byte-identical `core.name_norm`** macro that produces `sos_normalized_master.normalized_legal_name`,
plus a `zip5` key, to PPP / `sba_7a` / `sba_504`. **PPP is therefore already on the
fleet-wide blocking key.** Verified samples: `"SUMTER COATINGS, INC."` →
`SUMTER COATINGS INC`; `"BOYER CHILDREN'S CLINIC"` → `BOYER CHILDRENS CLINIC`;
`"PLEASANT PLACES, INC."` → `PLEASANT PLACES INC`.

## 3. The canonical join key

`core/name_norm.py` is the single source of truth. `name_norm(expr)`:
UPPER → `&`→` AND ` → dash/en-dash/em-dash → space → strip every non-`[A-Z0-9 space]` →
collapse whitespace → trim → NULL if emptied. `legal_name_base(expr)` additionally peels
trailing `LLC|INC|CORP|CO|LTD|PLC` for suffix-tolerant blocking. It is imported by
`sos_normalized/normalize.py` (the master spine), the credit-spine campaign (PPP/7a/504),
and every `crosswalk_*` / bridge — so the key cannot drift across layers.

**`name_norm(legal_name) + zip5` is the fleet blocking key.** PPP carries it natively
(BTREE on `normalized_legal_name` and `zip_code`). EPA carries it only in
`epa_to_sos_bridge`; the EPA base tables carry raw names that must be normalized in-build.

## 4. Measured EPA↔PPP overlap

PPP blocking sets (live): **8,101,665** distinct names, **9,341,067** distinct (name, zip5).

### 4.1 Route A — via `epa_to_sos_bridge` (a FLOOR)

Bounded by SoS coverage and excludes public/government entities (the bridge's `PUBLIC_RE`
filter). Confidence split: high 134,233 / medium 129,759 / low 92,911. Name source:
facility 182,369 / permit 156,386 / defendant 18,148. 156,309 distinct names; 154,813
distinct `sos_company_id`.

- Match a PPP borrower by **name: 53,281 (34.1%)**; by **name+zip5: 25,969**; distinct
  `sos_company_id` matched: 54,038.

### 4.2 Route B — EPA raw names normalized DIRECTLY (the CEILING)

Bypasses the SoS bottleneck; normalizes EPA names in-place with `core.name_norm`.

| EPA source column | distinct names | match PPP by NAME | match PPP by NAME+ZIP5 |
|---|--:|--:|--:|
| `epa_facilities.FAC_NAME` | 2,715,678 | 139,786 (5.1%) | **60,980** |
| `epa_program_links.PRIMARY_NAME` | 3,364,722 | 160,265 (4.8%) | **73,380** |
| **union of both** | 3,467,794 | **162,664** | — |

The low *percentages* are denominator dilution — EPA facility names are dominated by
municipal/utility/site labels that never borrowed PPP. The **absolute** name+zip5 counts
(61k–73k) are the defensible high-confidence population, ≈3× Route A. Verified example
name+zip5 matches: `CAHABA VENEER INC` (35042), `SHAFER VINEYARDS` (94558),
`INTRA AEROSPACE LLC` (91730), `ROADSTAR TRUCKING INC` (94544), `MEZA PALLETS INC` (92335)
— real industrial SMBs that both handle regulated material and took PPP.

### 4.3 Route C — both sides → SoS spine (officer-enrichment overlay, not the hub)

`sos_normalized_master` = 17,926,543 rows, 16,822,318 distinct names. PPP names present in
SoS: **1,017,162 (12.6%)**; by name+zip5: **505,081**. Lower because the SoS spine is
multi-state (CA / CO / FL / NY observed), not all 50 — so SoS is the wrong *primary* hub for
EPA↔PPP, but the right place to attach officers/agents where coverage exists.

## 5. Officer / POC reachability (verified path + cardinality)

Because neither EPA nor PPP has a hard entity ID, officers/POCs attach **only after
name-resolution**:

- **Officers / agents (SoS route).** EPA/PPP `normalized_legal_name` → `sos_normalized_master`
  → `original_entity_id` → `ca_sos_principals` (`first/middle/last_name`, `position_type`),
  `ca_sos_agents`, `co_sos` agent fields, `ny_sos` (`ceo_name`, `registered_agent_name`),
  `fl_sos_corporations` (`registered_agent_name`, `raw_officer_block`). **Verified density:**
  of 638,593 PPP names resolving to a CA SoS entity, **638,592 (≈100%)** have ≥1 principal.
- **POCs / exec-comp / awards (SAM route).** EPA/PPP `normalized_legal_name` →
  `crosswalk_sam_usaspending` (`normalized_legal_name`→`uei`) → `sam_pocs` (8.07M),
  `sam_master_contacts` (4.37M), `ffata_exec_comp` (`officer_name`), `contractor_award_summary`.
  Adds a second fuzzy hop (name→UEI) since neither EPA nor PPP carries `uei`. Yield not yet
  measured — quantify before relying on it.

## 6. Recommendations (concrete, ranked)

1. **Build `pipelines/resolution/crosswalk_ppp_epa.py` (Pattern-B bridge) on
   `normalized_legal_name + zip5`, off EPA *raw* names — not off `epa_to_sos_bridge`.**
   Routing through the existing bridge discards ~3× of the overlap (53k vs 162k name matches)
   and all public-entity coverage. Normalize EPA names in-build with `core.name_norm`,
   prioritizing **permit → defendant → facility/PRIMARY_NAME** (the order `epa_to_sos_bridge`
   already uses, because ICIS `PERMIT_NAME` and `CASE_DEFENDANTS` are true legal entities
   while `FAC_NAME` is a site label). The PPP side is already BTREE-indexed on the key.

2. **Emit precision-ordered tiers; persist `match_tier` on every row** (lift from
   `uspto_sba_ppp_mapping_blueprint.md` §2.5): **T1** name+zip5 (auto-accept, ~61–73k),
   **T2** name+state (accept for org-type names), **T3** name-only org-only (candidate).
   **Reject** individual/personal-name name-only matches and any name appearing in ≥5
   states — PPP's sole-proprietor long tail makes `MICHAEL WILLIAMS`-class homonyms the
   dominant false-positive source. **Geography is mandatory for PPP** (raw name-match state
   agreement is only ~24%).

3. **Carry resolution keys on each bridge row** — `loan_number`, `REGISTRY_ID`,
   `sos_company_id`, `naics_code`, `confidence` — so it slots into the resolution graph.
   With `REGISTRY_ID` attached, each matched loan inherits the entity's full EPA compliance
   footprint through the verified internal join graph (§1.1).

4. **Treat SoS as the officer-enrichment overlay, not the EPA↔PPP hub.** After the direct
   name bridge, left-join resolved entities to `sos_normalized_master` →
   `ca_sos_principals` / `co_sos` / `ny_sos` / `fl_sos_corporations` for officers/agents
   (~100% dense where SoS covers the state). Add the
   `crosswalk_sam_usaspending`→`uei`→`sam_pocs` hop for POCs where a SAM registration exists.

5. **Close the EPA enforcement gap** if cases matter: materialize `CASE_FACILITIES`
   (`REGISTRY_ID` ↔ `ACTIVITY_ID`) as a standing dataset so `epa_case_enforcements` /
   `epa_case_milestones` attach to the REGISTRY_ID hub without a transient bridge-build read.

## 7. Caveats (verified, not assumed)

- **No hard-ID join exists** — this is irreducibly fuzzy resolution. Treat every match as
  probabilistic and tier it.
- **`epa_to_sos_bridge` is SoS-coverage-bounded and public-entity-filtered** — do not use it
  as the EPA↔PPP denominator. It is a floor, not the population.
- **PPP `borrower_state` / `zip_code` are 100% populated** (the 8-row head sample that showed
  null states is a first-fragment ordering artifact — the aggregate `count(*)` filter is
  authoritative).
- **The 162,664 name-only figure is an upper bound** requiring the tier/dedup discipline in
  §6.2. The **60,980–73,380 name+zip5** figures are the shippable high-confidence population
  (dedup across the two EPA name sources to consolidate to one entity grain).
- **EPA `FAC_NAME` is a site name, not always a legal entity** — prefer permit/defendant names
  for precision; use facility/PRIMARY_NAME for recall behind a geographic confirmer.

---

### Appendix — datasets referenced (live row counts, 2026-06-05)

`epa_facilities` 3,240,591 · `epa_program_links` 4,360,148 · `epa_npdes_dmrs` 67,597,592 ·
`epa_npdes_eff_violations` 46,361,587 · `epa_npdes_qncr_history` 7,866,031 ·
`epa_aim_triggering_events` 5,375 · `epa_case_enforcements` 135,053 ·
`epa_case_milestones` 508,088 · `epa_pipeline_caa` 66,655 · `epa_pipeline_rcra` 456,773 ·
`epa_to_sos_bridge` 356,903 · `ppp` 11,468,210 · `sos_normalized_master` 17,926,543 ·
`ca_sos_principals` 18,670,722 · `ca_sos_agents` 8,560,095 · `sam_pocs` 8,065,116 ·
`sam_master_contacts` 4,373,319 · `ffata_exec_comp` 29,601 ·
`crosswalk_sam_usaspending` 1,028,144 · `sba_7a` 1,947,098 · `sba_504` 227,404.
