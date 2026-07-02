# GLEIF × EPA Entity Bridge — Read-Only Reconnaissance

> **⚠️ CANONICAL STATE — verified live against R2 on 2026-07-02.** Two of this recon's premises are now stale. (1) The EPA legal-owner names it calls "not materialized" ARE built: **`epa_permits`** = **1,686,705 rows** (`PERMIT_NAME`, `normalized_legal_name`) and the defendant names as **`epa_case_defendants`** = **200,159 rows** (`DEFENDANT_NAME`). (2) `epa_to_sos_bridge` is now **406,191 rows** (was 356,903). GLEIF drifted: `gleif_l1_entities` = **3,360,382 rows**, `gleif_l2_relationships` = **478,018 rows**. **STILL TRUE and load-bearing:** NO GLEIF×EPA bridge dataset exists in R2 — there is no built `lei ↔ REGISTRY_ID` crosswalk of any kind. The only live EPA→external entity link is `epa_to_sos_bridge` (EPA `REGISTRY_ID` → Secretary-of-State `sos_company_id` + `normalized_legal_name`); no EPA entity dataset carries UEI/DUNS/LEI/CAGE (column-scanned live), so any EPA→GLEIF/LEI or EPA→SAM link remains an unbuilt, transitive, name-matched proposition. Read `EPA_DATA_PLANE_STATE.md` first.

Schema diagnostic mapping the vectors to crosswalk global GLEIF Level-1/Level-2 corporate
data to the EPA regulatory network (link domestic EPA liabilities → global corporate parents
via the LEI). **Strictly read-only: no Lance write, no DDL, no data-plane mutation.**

**Provenance.**
- **GLEIF** — *live* read of the **original golden-copy source** (not the lossy active Lance
  projection): discovery API `goldencopy.gleif.org/api/v2/golden-copies/publishes` + record-faithful
  `api.gleif.org/api/v1` sampling. Publish **2026-06-05 16:00 UTC**.
- **EPA** — schema from the on-`main` materializers (`pipelines/ingest_epa/*.py`); null densities
  from two attested **live read-only R2 audits dated 2026-06-05**: `EPA_LEGAL_ENTITY_MATERIALIZATION_PLAN.md`
  and `EPA_PPP_MAPPING_BLUEPRINT.md`.

---

## 0. Headline posture

| Verdict | Detail |
|---|---|
| **GLEIF active projection is lossy** | `gleif_l1_entities` materializes **8 of ~40** source fields. It **drops ZIP (`postalCode`), the entire `headquartersAddress`, and `otherNames[]`** — three of the highest-value binding vectors. A geo/alias-aware bridge **cannot** be built from the active dataset as it stands; it must read the **original golden copy** or the active L1 must be re-ingested wider. |
| **EPA legal-owner names are not materialized** | The true owner names **`PERMIT_NAME`** (NPDES permittee) and **`DEFENDANT_NAME`** (enforcement defendant) exist only inside landing ZIPs, read transiently in `build_bridge` and dropped. Planned `epa_permits`/`epa_defendants` **do not exist in R2** (live `KeyCount=0`). Materialized EPA name columns (`PRIMARY_NAME`, `FAC_NAME`, `normalized_facility_name`) are **site labels, not legal entities** (asserted verbatim in the EPA code). <br>**[SUPERSEDED 2026-07-02: `PERMIT_NAME` is now a standing, `REGISTRY_ID`-keyed column in `epa_permits` (1,686,705 rows); `DEFENDANT_NAME` is now standing in `epa_case_defendants` (200,159 rows). Both carry `normalized_legal_name` (permits) / are name-resolvable. The precision tier this recon said was blocked is now available.]** |
| **No shared hard identifier** | GLEIF carries **no EIN, no DUNS, no domestic CORPID**. EPA carries no LEI. The only structured cross-ref is GLEIF's `(registeredAt, registeredAs)` registry coordinate → routable to EPA **only** indirectly through the SoS spine. The realizable bridge is **Name + Geo**, exactly as the existing `epa_to_sos_bridge` already does. |
| **Bridge template already exists** | `epa_to_sos_bridge` (356,903 rows) resolves EPA names → `sos_normalized_master` via the canonical `core.name_norm` key + a 3-tier cascade. A GLEIF bridge is the **same Pattern-B build** with GLEIF L1 swapped for the SoS spine. |

---

## 1. Target Acquisition — the GLEIF master (original golden copy)

### 1.1 Datasets (publish 2026-06-05 16:00 UTC)

| Level | Golden-copy node | Records | Zip | Active Lance dataset | BTREE |
|---|---|--:|--:|---|---|
| **L1** | `lei2` · LEIRecord | **3,331,673** | 888 MB | `s3://data-sink/active/gleif_l1_entities/` | `lei` |
| **L2** | `rr` · RelationshipRecord | **475,003** | 32 MB | `s3://data-sink/active/gleif_l2_relationships/` | `lei`, `parent_lei` |
| L2 | `repex` · ReportingException | 6,150,399 | 67 MB | *(not ingested)* | — |

Daily full-snapshot overwrite (already 1 row/LEI). `repex` = the "why no parent reported"
companion to `rr`; the active pipeline ingests L1 + L2 only.

### 1.2 Original L1 record surface vs. active projection

Active L1 keeps **8 data columns** (+ `source_file`, `publish_date`, `ingested_at`). Everything
marked **DROPPED** is present in the original and load-bearing for this crosswalk.

| Original field (LEI-CDF) | In active? | Maps to / role for the bridge |
|---|---|---|
| `lei` | ✅ `lei` | join anchor |
| `entity.legalName.name` | ✅ `legal_name` | **primary name-match string** (raw, mixed-case) |
| `entity.legalAddress.city/region/country` | ✅ `legal_address_{city,region,country}` | `region` = ISO-3166-2 (`US-DE`); `country`=`US` |
| `entity.legalAddress.`**`postalCode`** | 🛑 **DROPPED** | **ZIP — the geo confirmer.** 100% fill (US sample) |
| `entity.`**`headquartersAddress`**`.{city,region,postalCode,…}` | 🛑 **DROPPED** | **Operating-location geo.** 100% present; **≠ legalAddress in 58% of US entities** |
| `entity.`**`otherNames[]`**` {name,type}` | 🛑 **DROPPED** | alias array — `PREVIOUS_LEGAL_NAME`, `TRADING_OR_OPERATING_NAME` (recall lever) |
| `entity.transliteratedOtherNames[]`, `entity.otherAddresses[]` | 🛑 DROPPED | secondary recall |
| `entity.registeredAt.id` | ✅ `registration_authority_id` | **RA code** (registry namespace) |
| `entity.registeredAs` | ✅ `registration_authority_entity_id` | **local registry number** (SoS file # / SEC CIK) |
| `entity.legalForm.id`, `entity.jurisdiction`, `entity.category` | 🛑 DROPPED | ELF code, ISO jurisdiction, fund/branch class |
| `entity.status` | ✅ `entity_status` | `ACTIVE`/`INACTIVE` |
| `registration.status` (ISSUED/LAPSED/RETIRED) | 🛑 DROPPED | LEI-registration validity (active keeps only entity status) |
| `registration.{managingLou,corroborationLevel,…}` | 🛑 DROPPED | provenance/trust |

### 1.3 Domestic cross-reference arrays (the "bypass string-matching" question)

| Vector | Source-native? | Verdict |
|---|---|---|
| `registeredAt.id` + `registeredAs` | **Golden-copy native** (in active) | **Only usable structured cross-ref.** Observed in-sample: `RA000602`→Delaware Div. of Corporations (dominant for US LLC/Inc), `RA000627`→NV, `RA000608`→IL, `RA000637`→TX; `RA999999` = no-registry sentinel (funds/trusts → `registeredAs` null). `registeredAs` = the **state SoS file number / SEC CIK**. |
| `ocid` (OpenCorporates), `spglobal[]`, `bic`, `mic`, `qcc` | API-layer enrichment only | 🛑 **Null for 100% of sampled US records; NOT in the golden copy XML.** Unreliable bridge surface. |
| **EIN / DUNS / domestic CORPID** | — | 🛑 **Absent from GLEIF entirely.** No direct identifier bypass to EPA exists. |

**Consequence:** the registry coordinate bypasses string-matching only **GLEIF → SoS** (it is a
state-registry number, which EPA does not carry). GLEIF → EPA remains a Name+Geo problem.

### 1.4 Original L2 RelationshipRecord vs. active projection

Active keeps **4 columns**: `lei` (startNode = child), `parent_lei` (endNode = parent),
`relationship_type`, `relationship_status`. Original additionally carries (all **DROPPED**):
`relationship.periods[]` (`ACCOUNTING_PERIOD`/`RELATIONSHIP_PERIOD`/`DOCUMENT_FILING_PERIOD`),
`registration.{status,corroborationLevel,managingLou}`, `validFrom`/`validTo`. Edge types:
`IS_DIRECTLY_CONSOLIDATED_BY` (direct parent), `IS_ULTIMATELY_CONSOLIDATED_BY` (ultimate parent),
plus branch/fund variants. The 4 active columns suffice for parent rollup; the drop costs
point-in-time validity and corroboration filtering.

---

## 2. Target Mapping — the EPA owners

### 2.1 Owner-name carriers (legal entity, not facility)

| EPA name string | Materialized on `main`? | Dataset | Null density | Grain / note |
|---|---|---|--:|---|
| **`PERMIT_NAME`** (NPDES permittee) | 🛑 **No** (planned `epa_permits`) | read transiently in `build_bridge` | **2.78% null** (97.22% fill) | true legal entity; ~1.69M permit-versions → ~1.01M distinct `REGISTRY_ID` |
| **`DEFENDANT_NAME`** (enforcement defendant) | 🛑 **No** (planned `epa_defendants`) | read transiently | **~0% null** (~100%) | true legal entity; ~310k rows → ~113k distinct `REGISTRY_ID` |
| `PRIMARY_NAME` (FRS program reg.) | ✅ Yes | `epa_program_links` (4,360,148 rows) | **4.8% null** | **site label, not always a legal owner** |
| `PRIMARY_NAME` / `FAC_NAME` (FRS facility) | ✅ Yes | `epa_facilities` | low | site label |
| `normalized_facility_name` | ✅ Yes | `epa_air_facilities`, `epa_rcra_handlers` | low | `core.name_norm(FACILITY_NAME)` — **site label** (code's own caveat) |
| `normalized_legal_name` | ✅ Yes | `epa_to_sos_bridge` (356,903 rows) | — | already-resolved commercial subset only |

### 2.2 Owner-geo carriers — **facility location, not owner mailing address**

ICIS_PERMITS carries **no address of its own**; geo comes from the joined **facility** row.
EPA permit/defendant records therefore expose a **site address, never an owner mailing address.**

| Dataset | State col | ZIP col | Fill (state / ZIP) | `REGISTRY_ID` fill |
|---|---|---|--:|--:|
| `epa_permits` (planned) ← ICIS_FACILITIES | `FAC_STATE_CODE` | `FAC_ZIP` | 100% / 100% | 99.58% |
| `epa_defendants` (planned) ← CASE_FACILITIES | `FAC_STATE_CODE` | `FAC_ZIP` | 99.88% / 99.04% | 99.60% |
| `epa_program_links` (live) ← FRS_PROGRAM_LINKS | `STATE_CODE` | `POSTAL_CODE` | high | hub key |

### 2.3 REGISTRY_ID topology (the hub the directive isolated)

```
REGISTRY_ID  ─┬─ epa_facilities            (FRS_FACILITIES; site name + geo)        BTREE registry_id
              ├─ epa_program_links         (REGISTRY_ID↔PGM_SYS_ID; PRIMARY_NAME+geo) BTREE registry_id,pgm_sys_id
              ├─ epa_permits   (PLANNED)   (PERMIT_NAME    + FAC_* geo)             BTREE registry_id,ext_permit,npdes_id
              ├─ epa_defendants(PLANNED)   (DEFENDANT_NAME + FAC_* geo)             BTREE registry_id,activity_id
              └─ epa_to_sos_bridge         (REGISTRY_ID → sos_company_id)           BTREE registry_id,normalized_legal_name
```

---

## 3. Matching topology

### 3.1 Casing & cleanliness — raw exact match is **not** viable

| Side | Casing | Punctuation | Example |
|---|---|---|---|
| GLEIF `legal_name` | **mixed** | retained | `Casey Fork Solar, LLC` · `Mountain Holding, Inc.` |
| EPA `PERMIT_NAME`/`PRIMARY_NAME` | **UPPER** (ECHO/ICIS) | retained | `CASEY FORK SOLAR LLC` |

Casing + punctuation diverge → **a raw string equality join is mathematically dead** beyond a
trivial all-caps subset. The canonical **`core.name_norm`** (UPPER → `&`→`AND` → dash→space →
strip non-`[A-Z0-9 ]` → collapse ws) **is required on both sides**. Post-normalization, exact
equality on `normalized_legal_name` is the fleet-proven key (identical across `sos_normalized_master`,
PPP, SAM, and `epa_to_sos_bridge` — zero drift). Fleet blocking key = **`name_norm(legal_name) + zip5`**.

### 3.2 Geo caveat (decisive)

GLEIF address is **corporate** (legalAddress is frequently the **Delaware registered agent**, e.g.
`850 New Burton Road, Dover DE 19904`; headquartersAddress is the corporate HQ). EPA geo is the
**regulated site**. A multi-site operator's HQ ZIP ≠ its facility ZIP → **ZIP-blocking systematically
loses multi-site operators.** Use ZIP as a **precision confirmer**, state-level as the **recall**
blocker, and prefer GLEIF **`headquartersAddress`** over `legalAddress` for any geo tier.

### 3.3 Structural join paths

1. **Primary — Name+Geo cascade (mirror `build_bridge`).** Output grain: `REGISTRY_ID ↔ lei`.
   - `gleif`: `nln = core.name_norm(legal_name)`; `lnb = core.legal_name_base(nln)`; `zip5 = left(headquartersAddress.postalCode,5)`; `state = region`.
   - `epa`: same macros over owner names, priority **`PERMIT_NAME` → `DEFENDANT_NAME` → `PRIMARY_NAME`/`FAC_NAME`** (precision→recall), tracked in `epa_name_source`.
   - Tiers: **(A)** exact `nln` · **(A+)** `nln` + `state` · **(B)** `legal_name_base` + `zip5`.
2. **Registry-ID bypass (high-precision, partial).** `GLEIF(registeredAt=RA000602/DE, registeredAs=file#)`
   → DE SoS entity in `sos_normalized_master` → `epa_to_sos_bridge (sos_company_id → REGISTRY_ID)`.
   Skips string-matching for the subset where `registeredAs` already resolves to a bridged SoS entity.
   **Not** a direct EPA join (EPA carries no SoS file number).
3. **L2 parent rollup.** Once `lei ↔ REGISTRY_ID` exists: `gleif_l2_relationships`
   (`lei → parent_lei`, `IS_ULTIMATELY_CONSOLIDATED_BY`) rolls each domestic EPA liability up to the
   global ultimate parent — the directive's end goal.

---

## 4. Blockers before a bridge can be built

1. **Re-ingest GLEIF L1 wider** (or read the golden copy directly in-build) to expose
   `legalAddress.postalCode`, the full `headquartersAddress`, and `otherNames[]`. Without ZIP + HQ
   in the active dataset, geo-confirmation tiers (A+/B) cannot run off `gleif_l1_entities`.
2. **Materialize `epa_permits` / `epa_defendants`** so `PERMIT_NAME`/`DEFENDANT_NAME` become standing,
   random-access, `REGISTRY_ID`-keyed owner nodes (the precision tier). Today they are transient-only.
   > **[SUPERSEDED 2026-07-02: DONE. `epa_permits` (1,686,705) and `epa_case_defendants` (200,159) are built. This blocker is cleared — the precision-tier name nodes exist.]**
3. Until (2), a GLEIF bridge can run **only** against `epa_program_links.PRIMARY_NAME` (site label,
   lower precision) or the already-resolved `epa_to_sos_bridge.normalized_legal_name` (commercial
   subset, recall-capped).

> **[NET STATE 2026-07-02: blocker (2) is cleared; blocker (1) — re-ingest GLEIF L1 wider for ZIP/HQ/otherNames — remains unverified. Regardless, NO GLEIF×EPA bridge has been built yet; this remains a design, not a live dataset.]**
