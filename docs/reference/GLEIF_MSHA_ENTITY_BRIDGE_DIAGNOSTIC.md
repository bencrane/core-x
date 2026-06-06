# GLEIF → MSHA Entity-Bridge Reconnaissance

Read-only schema diagnostic mapping the join vectors to crosswalk the **GLEIF LEI golden
copy** against the **MSHA (Mine Safety & Health Administration)** entity universe. MSHA is
structurally hostile: **no EIN/DUNS/UEI/CAGE/NAICS/domain anywhere** — resolution is
**name + state + mailing-ZIP only** (confirmed across all 215 committed MSHA columns in
`MSHA_LANCE_STATE_DIAGNOSTIC.md`). This file maps how to string-bind the global LEI master
to MSHA's native registries.

- **Live targets (`s3://data-sink/active/`):** `gleif_l1_entities`, `gleif_l2_relationships`,
  `msha_corporate_history`, `msha_mines`, `msha_contractors` — `pylance 7.0.0` schema read +
  `count_rows(filter=)` + bounded `scanner` samples.
- **Original GLEIF master interrogated:** `goldencopy.gleif.org/api/v2/golden-copies/publishes`
  (discovery API, read-only) — the upstream golden copy the local Lance is projected from.
- **As-of:** probe **2026-06-05**; GLEIF publish `2026-06-05 16:00`; MSHA SoR written 2026-06-03.
- **Attestation:** every figure is a live read. **Zero data-plane mutation** — no `.lance`
  writes, no DDL, no indexes, no `ops.*` rows. Probe deps in an ephemeral venv;
  `/tmp/gleif_msha_bridge_probe.py`, Doppler-injected R2 creds (`core-x/prd`).

---

## 0. Headline verdict

| # | Finding | Verdict |
|---|---|---|
| 1 | **GLEIF ZIP tiebreaker is BLOCKED — the local dataset is a lossy projection.** The original golden copy (CDF `Entity/LegalAddress`) carries **PostalCode**, but `gleif/ingest.py::_extract_l1` lands only City/Region/Country. `gleif_l1_entities` has **no postal/ZIP column**. | 🛑 ZIP-bind needs re-ingest |
| 2 | **State binding works today.** GLEIF `legal_address_region` = ISO-3166-2 (`US-TX`); strip `US-` → 2-letter ↔ MSHA `STATE_ABBR`/`ADDR_STATE`. | ✅ |
| 3 | **MSHA controller/operator geo must be JOINED in.** `msha_corporate_history` has **zero** geographic columns; State/ZIP live only on `msha_mines`, reached via `MINE_ID`. | ✅ path mapped |
| 4 | **Directive assumption wrong: `ADDR_ZIP` does not exist.** The mailing-ZIP column is **`ZIP_CD`** (98.95% fill). `ADDR_STATE` exists; `ADDR_ZIP`/`ZIP`/`ADDR_ZIP_CD` do **not**. | ⚠️ corrected |
| 5 | **Sole-prop filter confirmed exactly.** `CONTROLLER_TYPE`: COMPANY **93,545 (55.41%)** / PERSON **75,264 (44.59%)**, 0 null. Filter `WHERE CONTROLLER_TYPE='COMPANY'`. | ✅ |
| 6 | **Contractors are geo-orphaned.** `msha_contractors` (1.63 M rows) has **no state/ZIP/FIPS/city** — a contractor ZIP tiebreaker is **unrecoverable** from its own registry (mine-geo proxy only via the 6.69% cited ledger slice). | 🛑 name-only |
| 7 | **Names are mixed-case, not uppercase** (`Lhoist Group`, `O K Combs Trucking`). The directive's "uppercase" premise is moot — canonical `name_norm` `UPPER()`s both sides. | ⚠️ corrected |

---

## 1. Target schemas (live)

### GLEIF — `gleif_l1_entities` (3,330,881 rows · BTREE `lei`)
`lei` · **`legal_name`** · **`legal_address_city`** · **`legal_address_region`** ·
`legal_address_country` · `registration_authority_id` · `registration_authority_entity_id` ·
`entity_status` · `source_file` · `publish_date` · `ingested_at`.

> **The projection gap.** Original golden-copy L1 = 3,331,673 records (csv/json/xml). The CDF
> `Entity/LegalAddress` node carries `FirstAddressLine, City, **Region**, Country, **PostalCode**`
> (+ a parallel `HeadquartersAddress` node, `LegalJurisdiction`, `LegalForm`). `_extract_l1`
> kept **City/Region/Country only** → **PostalCode, HeadquartersAddress, LegalJurisdiction are
> dropped.** State-bind is satisfiable now (`Region`); **ZIP-bind requires re-ingest** (add
> `legal_address_postalcode`, ideally `headquarters_address_postalcode`+`legal_jurisdiction`,
> to `_extract_l1`/`_l1_schema` — the original source already carries them).

`gleif_l2_relationships` (475,125 rows · BTREE `lei`,`parent_lei`): `lei`(child) · `parent_lei` ·
`relationship_type` · `relationship_status`. → roll a matched MSHA entity to its **ultimate
LEI parent** once on the LEI.

### MSHA — `msha_corporate_history` (168,809 rows) — **no geo**
`CONTROLLER_ID`* · `CONTROLLER_NAME` · `CONTROLLER_START_DT` · `CONTROLLER_END_DT` ·
`CONTROLLER_TYPE`† · `COAL_METAL_IND`† · `MINE_ID`* · `MINE_NAME` · `MINE_STATUS` ·
`OPERATOR_ID`* · `OPERATOR_NAME` · `OPERATOR_START_DT` · `OPERATOR_END_DT`.
(`*`=BTREE, `†`=BITMAP.) **The only path to geo is `MINE_ID` → `msha_mines`.**

### MSHA — `msha_mines` (91,803 rows) — **the geo carrier**
Geo columns (live fill): **`STATE`** 100% (operational, BITMAP) · **`STATE_ABBR`** 98.96%
(mailing 2-letter) · **`ADDR_STATE`** 98.96% (mailing) · **`ZIP_CD`** 98.95% (mailing ZIP) ·
`POSTAL_CD` (operational) · `FIPS_STATE_CD` · `CITY` 98.98% · `STREET` 65.70% · `PO_BOX` 30.88%.
Entity keys: **`CURRENT_CONTROLLER_ID`*** (98.91%) + `CURRENT_CONTROLLER_NAME` · **`CURRENT_OPERATOR_ID`***
(98.96%) + `CURRENT_OPERATOR_NAME` · `MINE_ID`* (100%).
→ **current** controller/operator carry State+ZIP **denormalized on the same row** (no join).
**Use mailing `STATE_ABBR`+`ZIP_CD`** (operator address-of-record = corporate locus) over `STATE`
(mine's physical site) for corporate matching.

### MSHA — `msha_contractors` (1,630,676 rows) — **geo-orphaned**
`CONTRACTOR_ID`* · **`CONTRACTOR_NAME`** · `SUBUNIT_CD`† · firmographics (`AVG_EMPLOYEE_CNT`,
`HOURS_WORKED`, `COAL_PRODUCTION`, `ANNUAL_*`) · `COAL_METAL_IND`†. **No geographic column of
any kind.** ZIP/State tiebreaker is not recoverable from this set.

---

## 2. SQL traversal — attach State + ZIP to an MSHA entity

```sql
-- ════ PATH A · CURRENT controller/operator → geo  (NO JOIN; denormalized on msha_mines) ════
-- Fastest. CURRENT_*_ID are BTREE-indexed. Grain = mine → aggregate to entity (multi-mine fan-out).
SELECT  CURRENT_CONTROLLER_ID                         AS entity_id,
        any_value(CURRENT_CONTROLLER_NAME)           AS msha_name,
        mode(STATE_ABBR)                             AS state2,     -- modal mailing state
        array_agg(DISTINCT left(ZIP_CD,5))           AS zip5_set,   -- ZIP candidate set
        count(*)                                     AS mine_count
FROM    msha_mines
WHERE   CURRENT_CONTROLLER_ID IS NOT NULL
GROUP BY 1;                              -- swap CURRENT_CONTROLLER_* → CURRENT_OPERATOR_* for operators

-- ════ PATH B · FULL operating history → geo  (ONE JOIN on MINE_ID; covers ended links) ════
-- corp_history has the entity↔mine SCD but NO geography; msha_mines supplies it via MINE_ID.
-- Both join keys BTREE-indexed. NOTE: join is on MINE_ID — msha_mines has no bare OPERATOR_ID.
SELECT  ch.OPERATOR_ID                                AS entity_id,   -- or ch.CONTROLLER_ID
        any_value(ch.OPERATOR_NAME)                  AS msha_name,
        mode(m.STATE_ABBR)                           AS state2,
        array_agg(DISTINCT left(m.ZIP_CD,5))         AS zip5_set,
        count(DISTINCT ch.MINE_ID)                   AS mine_count
FROM    msha_corporate_history ch
JOIN    msha_mines             m  ON m.MINE_ID = ch.MINE_ID
WHERE   ch.CONTROLLER_TYPE = 'COMPANY'               -- §3 sole-prop filter (controller-side only)
GROUP BY 1;

-- ════ CONTRACTORS · no native geo — mine-geo PROXY via the cited ledger slice only ════
-- Recovers geo for ≤21,966 of 38,653 named contractors (6.69% ledger). 58.5% remain geo-less.
SELECT  l.VIOLATOR_ID                                AS contractor_id,
        mode(m.STATE_ABBR)                           AS state2,
        array_agg(DISTINCT left(m.ZIP_CD,5))         AS zip5_set
FROM    msha_enforcement_ledger l
JOIN    msha_mines              m  ON m.MINE_ID = l.MINE_ID
WHERE   l.VIOLATOR_TYPE_CD = 'Contractor'
GROUP BY 1;     -- contractor NAME comes from msha_contractors.CONTRACTOR_NAME (CONTRACTOR_ID join)
```

**Fan-out caveat:** an entity controls/operates N mines → N (state,ZIP) rows. Bind on **modal
state**; treat **ZIP as a candidate set** (a GLEIF ZIP that hits any member confirms the match).

---

## 3. Sole-proprietor filter (B2B isolation)

```sql
WHERE CONTROLLER_TYPE = 'COMPANY'   -- drops 75,264 PERSON rows (44.59%); keeps 93,545 (55.41%)
```
`CONTROLLER_TYPE` is BITMAP-indexed (free filter). **Caveats:** (a) it types the **controller
only** — `OPERATOR_*` has no parallel discriminator, so operator-name resolution has no sole-prop
gate (rely on GLEIF naturally failing individuals + the name-shape pre-clean below). (b) The flag
is imperfect — `'Sherrill Pat'` lands as `COMPANY`; GLEIF non-match will reject these downstream.

---

## 4. String-normalization strategy

**Spine:** the canonical `core.name_norm.name_norm(expr)` (the fleet blocking key used by
`sos_normalized_master` and `crosswalk_hmda_gleif`): `UPPER` → `&`→` AND ` → `[-–—]`→space →
strip `[^A-Z0-9 ]` → collapse whitespace → trim → NULL-if-empty. Apply the **same** macro to
both sides so MSHA keys are byte-identical to the existing GLEIF/SoS blocking key.

**GLEIF side** (clean): `name_norm(legal_name)`; `state2 = substr(legal_address_region,4)` where
`region LIKE 'US-%'`; prefer `entity_status='ACTIVE'`; constrain `legal_address_country='US'`.

**MSHA side — pre-clean BEFORE `name_norm` (3 native quirks GLEIF lacks):**
1. **Lineage parenthetical** — `regexp_replace(name,'\s*\(Form:[^)]*\)','')` → strips
   `Legacy Vulcan Corp (Form:Vulcan Materials Co)` → `Legacy Vulcan Corp`.
2. **Trailing state suffix** — `regexp_replace(name,'-([A-Z]{2})$','')` on municipal/controller
   names → `Cassia County-ID`→`Cassia County`, `Town of Conesville-NY`→`Town of Conesville`
   (else `name_norm` leaves a stray `ID`/`NY` token). The stripped code is a free **fallback state**.
3. **`et al`** — strip trailing ` et al` (`Triminco Inc et al`).

**Two-pass blocking:**
- **Pass 1 (tight):** `(name_norm, state2)` exact. ZIP confirms when GLEIF ZIP exists (post-re-ingest).
- **Pass 2 (drift):** `core.name_norm.legal_name_base(...)` (peels trailing `LLC|INC|CORP|CO|LTD|PLC`)
  blocked on `(legal_name_base, state2)` → recovers `PACIFIC TRUCKING` vs `PACIFIC TRUCKING LLC`.
- **ZIP tiebreaker** (`left(ZIP_CD,5)` ∈ GLEIF `zip5`): **one-sided until GLEIF re-ingest** —
  available on MSHA today, absent on GLEIF (finding #1). State is the only live geo discriminant.

---

## 5. End-to-end bridge recipe (when built — not built here)

```
GLEIF L1 (US, ACTIVE)  ──name_norm(legal_name) + region→state2 [+ postalcode after re-ingest]──┐
                                                                                                ├─ block (name,state2)
MSHA entity  ── WHERE CONTROLLER_TYPE='COMPANY' ─ pre-clean ─ name_norm ─ PATH A/B geo (state2,zip5_set) ─┘
                                                                                                │
   matched LEI ──► gleif_l2_relationships (lei→parent_lei) ──► ultimate corporate parent
```
Output a `crosswalk_msha_gleif` bridge (BTREE `lei` + `normalized_legal_name`), mirroring
`crosswalk_hmda_gleif.py`. **Prereq for ZIP-grade precision:** re-ingest `gleif_l1_entities`
with `legal_address_postalcode`.

---

## 6. Appendix — live evidence

- **GLEIF L1** 3,330,881 rows, BTREE `lei` only; **0 postal columns**; region samples all `US-XX`
  (`US-MA`,`US-DE`,`US-PA`); city casing inconsistent (`BOSTON`/`Wilmington` → `UPPER` any city match).
- **GLEIF original** (discovery API): L1 3,331,673 · L2 475,003 · repex 6,150,399; csv/json/xml full files.
- **`msha_corporate_history`** 168,809 — COMPANY 55.41% / PERSON 44.59%; no geo columns.
- **`msha_mines`** 91,803 — geo family present: `STATE,STATE_ABBR,ADDR_STATE,ZIP_CD,POSTAL_CD,`
  `FIPS_STATE_CD,CITY,STREET,PO_BOX`; `ADDR_ZIP`/`ZIP`/`ADDR_ZIP_CD` **absent**. Lhoist Group
  (`0041044`) on 3 mines (AL · 35040/35035/35236) → fan-out confirmed.
- **`msha_contractors`** 1,630,676 — **0 geo columns**; BTREE `CONTRACTOR_ID`.
- **Harness:** `/tmp/gleif_msha_bridge_probe.py` (read-only; pylance 7.0.0 / duckdb 1.5.3 /
  pyarrow 24.0.0; ephemeral `uv` venv). Companions: `MSHA_LANCE_STATE_DIAGNOSTIC.md`,
  `MSHA_LEGAL_ENTITY_SCHEMA_DIAGNOSTIC.md`.
