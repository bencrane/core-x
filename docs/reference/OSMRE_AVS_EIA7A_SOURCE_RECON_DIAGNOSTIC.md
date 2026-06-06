# OSMRE AVS + EIA Form-7A — Coal-Sector Source Reconnaissance

Read-only acquisition recon for two prospective coal-sector entity sources: the **OSMRE
Applicant/Violator System (AVS)** "Organizational Family Tree" and the **EIA Form-7A**
(Annual Survey of Coal Production & Preparation). Both directives assume the data is *in
the lake*. **It is not.** Neither source has been ingested — no `active/` dataset, no
`landing/` raw drop, no pipeline, no env var, no catalog entry. This file records the
attested absence and specifies the **live MSHA/EPA bridge target** each source would bind
to once acquired — i.e. answers the "join keys to link parents to our MSHA/EPA" question
from the *target* side, the only side that exists today.

- **Probed (live R2, `s3://data-sink/`):** full `active/` prefix list (95 datasets +
  `catalog.json`), full `landing/` prefix list (25 prefixes), recursive token sweep of
  `landing/**`, retired Gen-2 `dex-raw-landing-zone/`, and a repo-wide source grep.
- **Live bridge-target reads (`pylance`):** `msha_mines`, `msha_corporate_history`,
  `msha_contractors`, `epa_facilities` — `count_rows()` + schema only.
- **As-of:** probe **2026-06-05**; `active/catalog.json` generated 2026-06-01 (predates
  the MSHA/EPA materializations — stale for counts, still authoritative for *absence*).
- **Attestation:** every figure is a live read. **Zero data-plane mutation** — no `.lance`
  writes, no DDL, no indexes, no `ops.*` rows. Doppler-injected R2 creds (`core-x/prd`);
  `/tmp/bridge_target_probe.py`, ephemeral `uv` venv (`pylance>=7`/`pyarrow`).

---

## 0. Headline verdict

| # | Finding | Verdict |
|---|---------|---------|
| 1 | **OSMRE AVS is NOT in the lake.** No `avs_*`, `oft_*`, `osmre_*`, `entity_relationship*` in `active/` or `landing/`; no raw drop; no pipeline/env/catalog reference. | 🛑 absent — not acquired |
| 2 | **EIA Form-7A is NOT in the lake.** No `eia_*`/`f7a_*`/`coal_production_survey*`; the directive's `landing/eia/` prefix **does not exist** (no `eia/` under landing at all). | 🛑 absent — not acquired |
| 3 | **`sba_7a` / `sba-7a-504` is a lexical false-positive, NOT EIA-7A.** It is the SBA 7(a) loan program (`active/sba_7a`, `landing/sba-7a-504/`). Unrelated to the EIA coal survey. | ⚠️ disambiguated |
| 4 | **The only coal-sector data in the lake is the MSHA family** — `msha_mines`, `msha_corporate_history`, `msha_contractors`, `msha_enforcement_ledger`, `msha_accidents`. This is the de-facto coal OFT today. | ✅ live |
| 5 | **MSHA carries the corporate tree but NO hard corporate xref.** `msha_corporate_history` is a Controller→Operator→Mine SCD. Zero EIN/DUNS/UEI/CAGE/LEI/NAICS across all MSHA cols (SIC only). Any AVS bind to MSHA is **name+state+ZIP**, never key-deterministic. | ✅ confirmed |
| 6 | **EIA-7A would be the *one* coal source with a deterministic MSHA join** — it reports the **MSHA mine ID**, which is MSHA's `MINE_ID` (BTREE). `eia_7a.MSHA_ID → msha_mines.MINE_ID` is a hard-key crosswalk, no string-normalization on the MSHA side. | ✅ path mapped |
| 7 | **EPA bridge target = `epa_facilities` (3.24 M).** Hard key is `REGISTRY_ID` (EPA FRS) — EPA-internal, not a corporate registry; no EIN/DUNS. An AVS→EPA bind without an FRS column is **name+geo**. | ✅ confirmed |

---

## 1. Probe scope & evidence (what was searched, all empty)

| Surface | Command | Result |
|---------|---------|--------|
| `active/` dataset list | `aws s3 ls .../active/` | 95 prefixes — **no** avs/oft/osmre/eia/f7a |
| `landing/` prefix list | `aws s3 ls .../landing/` | 25 prefixes — **no** eia/osmre/avs/oft |
| directive subpaths | `ls landing/{eia,osmre,avs,oft}/` | all empty (prefixes do not exist) |
| `landing/**` recursive | `ls --recursive landing/ \| grep -iE 'avs\|osmre\|oft\|eia\|f7a\|coal.?prod\|applicant.?violator'` | **0 matches** |
| retired Gen-2 | `aws s3 ls dex-raw-landing-zone/ \| grep …` | **0 matches** |
| catalog manifest | `active/catalog.json` (103-dataset, domain-nested) | **0** AVS/EIA/OSMRE entries |
| repo source | `grep -rniE 'osmre\|avs_\|oft_\|eia.?7a\|f7a_\|coal_production_survey\|applicant.?violator'` | **0 matches** |

`7a`-token resolves exclusively to `landing/sba-7a-504/` (SBA loans, `asof=260331`) + one
coincidental `exa_webset` id substring — neither is EIA.

---

## 2. OSMRE AVS — status: **absent**; would-be bridge target

**Lake status:** nothing to read. The directive's hypothesized columns (`APPLICANT_NAME`,
`PERMITTEE_NAME`, `OPERATOR_NAME`, `PARENT_COMPANY_NAME`, parent/sequence keys) **cannot be
confirmed** — there is no export in the lake. Exact column matrix resolves only on ingest.

**Upstream profile** *(external/public — for acquisition planning, NOT a lake read):* AVS is
documented to publish (a) applicant/operator/permittee entities with mailing addresses, (b)
internal **AVS entity IDs**, and (c) the **"owns/controls / is owned-controlled-by"**
ownership-and-control linkage that constitutes the OFT — an *internal* parent↔subsidiary
graph keyed on AVS's own entity IDs, plus **SMCRA permit numbers** (state-issued).

**Binding to the live universe — the structural truth:** AVS shares **no hard key** with
MSHA (SMCRA permit ≠ MSHA `MINE_ID`) or EPA (no FRS). Therefore AVS's value is its
**internal** ownership tree (resolve parent↔sub once on AVS entity IDs *before* bridging),
and its bind to MSHA/EPA is **name+state+ZIP string resolution** — identical mechanics to
the GLEIF→MSHA spine (see `GLEIF_MSHA_ENTITY_BRIDGE_DIAGNOSTIC.md` §2–4: `core.name_norm`,
two-pass blocking, modal-state + ZIP-candidate-set tiebreak). AVS would *enrich* the coal
OFT (SMCRA-side ownership MSHA lacks), not replace MSHA's controller/operator SCD.

---

## 3. EIA Form-7A — status: **absent**; would-be MSHA crosswalk

**Lake status:** nothing to read. Hypothesized `MSHA_ID` / company-name / address /
producer-type / `PARENT_COMPANY` columns **cannot be confirmed** — no export in the lake.

**Upstream profile** *(external/public — for acquisition planning, NOT a lake read):*
Form-7A respondents (coal operators >50k tons/yr) are documented to report, per mine, the
**MSHA mine identification number**, the legal operating company name + mailing address, a
**producer/operator type** (independent / operating subsidiary / contractor), and the
**parent/controlling company** name when a subsidiary/contractor status is flagged.

**Binding to the live universe — why this one is special:** the reported MSHA mine ID **is**
MSHA's `MINE_ID` (7-char zero-padded VARCHAR, BTREE-indexed on `msha_mines`). That makes
EIA-7A the **only** coal source offering a *deterministic, hard-key* MSHA join:

```
eia_7a.MSHA_ID  ──(hard key)──►  msha_mines.MINE_ID  ──►  CURRENT_CONTROLLER_ID / CURRENT_OPERATOR_ID
                                                          └─►  msha_corporate_history (Controller→Operator SCD)
```

No name-normalization on the MSHA side. EIA-7A would resolve the geographic tiebreakers MSHA
lacks for **contractors** (which are geo-orphaned in `msha_contractors`, §4) and supply a
**parent-company** string MSHA's flat SCD does not carry above the controller tier.

---

## 4. Live bridge-target spec (the join surface that exists today)

### MSHA — `msha_corporate_history` (168,809 rows) — the coal OFT
`CONTROLLER_ID` · `CONTROLLER_NAME` · `CONTROLLER_START_DT` · `CONTROLLER_END_DT` ·
`CONTROLLER_TYPE` · `COAL_METAL_IND` · `MINE_ID` · `MINE_NAME` · `MINE_STATUS` ·
`OPERATOR_ID` · `OPERATOR_NAME` · `OPERATOR_START_DT` · `OPERATOR_END_DT`.
→ **Two-tier hierarchy:** Controller (ultimate parent) → Operator (local operating entity) →
Mine. SCD via START/END dates. **No geo, no corporate xref.**

### MSHA — `msha_mines` (91,803 rows, 80 cols) — geo carrier
Keys: `MINE_ID`* (PK) · `CURRENT_CONTROLLER_ID`* + `CURRENT_CONTROLLER_NAME` ·
`CURRENT_OPERATOR_ID`* + `CURRENT_OPERATOR_NAME`. Geo (denormalized on row):
`STATE_ABBR` · `ADDR_STATE` · `ZIP_CD` · `CITY` · `STREET` · `PO_BOX` · `FIPS_STATE_CD` ·
`FIPS_CNTY_CD` · `LATITUDE`/`LONGITUDE`. Firmographic: `PRIMARY_SIC_CD`/`PRIMARY_SIC`
(SIC — **not** NAICS), `BUSINESS_NAME`, `NO_EMPLOYEES`. (`*` = BTREE.)

### MSHA — `msha_contractors` (1,630,676 rows) — geo-orphaned
`CONTRACTOR_ID`* · `CONTRACTOR_NAME` · production/hours/employee stats. **No geo, no xref.**
→ this is the tier EIA-7A's address columns would repair.

### EPA — `epa_facilities` (3,240,591 rows) — EPA bridge target
`REGISTRY_ID` (FRS hard key) · `FAC_NAME` · `FAC_STREET` · `FAC_CITY` · `FAC_STATE` ·
`FAC_ZIP` · `FAC_COUNTY` · `FAC_EPA_REGION` · `LATITUDE_MEASURE`/`LONGITUDE_MEASURE`.
→ downstream EPA↔SoS resolution already exists as `active/epa_to_sos_bridge`.

---

## 5. Cross-reference reality (the directives' EIN/DUNS premise)

Both directives ask whether a structured corporate xref (EIN, D&B/DUNS, state registration)
can **bypass string-normalization**. On the **resolution targets that exist**, the answer is
**no**:

| Target | Hard corporate xref present? |
|--------|------------------------------|
| `msha_mines` (80 cols) | **NONE** — no EIN/DUNS/UEI/CAGE/LEI/NAICS (SIC only) |
| `msha_corporate_history` | **NONE** |
| `msha_contractors` | **NONE** |
| `epa_facilities` | `REGISTRY_ID` = EPA FRS (agency-internal), **not** a corporate registry; no EIN/DUNS |

Implication: even if an acquired AVS/EIA-7A export carried EIN/DUNS, it could not key-join to
MSHA — MSHA is **name+state+ZIP only**. The single deterministic exception is **EIA-7A ↔
MSHA on `MINE_ID`** (§3), which is an operational mine key, not a corporate xref. Corporate
xref (EIN/DUNS/LEI) for coal entities is reachable only by first bridging MSHA→GLEIF/SoS via
the existing name-norm spine.

---

## 6. Acquisition path (not built here — read-only recon only)

To make either source real, mirror the MSHA ingest pattern (`pipelines/ingest_msha/`,
profiled in `MSHA_DATA_PROFILING_REPORT.md`): land raw to `s3://data-sink/landing/{osmre_avs,eia_7a}/`,
DuckDB transform (all-VARCHAR read → typed projection, IDs stay VARCHAR), write native Lance
to `s3://data-sink/active/{avs_*,eia_7a}/`, BTREE the resolution keys (`MINE_ID` for EIA-7A;
AVS entity ID for AVS). Then a `crosswalk_eia7a_msha` (hard-key) and a name-norm
`crosswalk_avs_msha` (mirroring `crosswalk_hmda_gleif.py`). **None of this is executed in
this recon.**

---

## 7. Evidence appendix

- **`active/`** 95 prefixes incl. `msha_{mines,corporate_history,contractors,enforcement_ledger,accidents}`,
  `epa_*` (incl. `epa_facilities`, `epa_to_sos_bridge`), `sba_7a` (SBA, not EIA). No avs/oft/osmre/eia/f7a.
- **`landing/`** 25 prefixes incl. `msha/`, `sba-7a-504/`. No `eia/`/`osmre/`/`avs/`/`oft/`.
  Recursive `landing/**` token sweep: **0** AVS/EIA matches. Retired `dex-raw-landing-zone/`: **0**.
- **`active/catalog.json`** (gen 2026-06-01, 103 datasets, domains: bridge/ca/cms/edgar/
  gleif/sba/sec/… — no msha/epa domain ⇒ catalog predates those materializations): **0** AVS/EIA entries.
- **Repo grep** (`*.py`/`*.ts`/`*.sql`/`*.md`): **0** matches for any target token.
- **Live bridge-target reads (2026-06-05):** `msha_mines` 91,803 (80 cols, xref NONE) ·
  `msha_corporate_history` 168,809 (15 cols) · `msha_contractors` 1,630,676 (18 cols) ·
  `epa_facilities` 3,240,591 (10 cols, xref = `REGISTRY_ID`/FRS only).
- **Harness:** `/tmp/bridge_target_probe.py` (read-only; `pylance>=7`/`pyarrow`, ephemeral
  `uv` venv). Companions: `MSHA_DATA_PROFILING_REPORT.md`, `GLEIF_MSHA_ENTITY_BRIDGE_DIAGNOSTIC.md`.
