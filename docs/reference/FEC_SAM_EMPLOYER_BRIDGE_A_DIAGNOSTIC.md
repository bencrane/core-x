# FEC → SAM.gov Employer→UEI Bridge (A) + Entity-Master Canonicalization

Read-only diagnostic resolving **which SAM entity master is canonical** and what it takes to wire
**FEC into the existing entity graph** via the donor's `employer` (org), not the donor's `name`
(person). Companion to [`FEC_SAM_PERSONNEL_BRIDGE_DIAGNOSTIC.md`](FEC_SAM_PERSONNEL_BRIDGE_DIAGNOSTIC.md)
(Bridge B, person↔person).

> **⚠️ CORRECTION (2026-06-05) — the §3 geo-join semantics were wrong.** This doc originally asserted
> ZIP is a *hard tiebreaker* because "both sides are the corporate address." **False.** FEC
> `state`/`zip_code` are the **contributor's residence**, not the employer's address — the exact
> home-vs-corp locus this doc's own companion (Bridge B) identified correctly, then this doc
> contradicted. The adversarial review
> [`SAM_NORMALIZED_ENTITIES_BUILD_PLAN_REVIEW.md`](../plans/SAM_NORMALIZED_ENTITIES_BUILD_PLAN_REVIEW.md)
> §B2 probed it: **32.67% of true name-matches have donor-home ≠ employer-HQ state** (55.5% for
> national employers). **For the employer bridge, geo is a confirmatory SCORE, never a hard JOIN
> predicate.** The probe *facts* in this doc (schemas, fills, distinct counts) are verified and
> unaffected — the error was an interpretive design claim carried over by analogy from the FEC×MSHA
> org bridge. The FEC bridge itself is **deferred / out of scope** (`fec_individual_contributions` is
> being rebuilt); this note preserves correctness, it does not authorize the bridge build.

- **As-of:** probe **2026-06-05**, `s3://data-sink/active/`. **Zero data-plane mutation** — pylance
  schema read + DuckDB aggregate scans; ephemeral `uv` env; Doppler `core-x/prd`; `core.name_norm`
  imported (blocking key byte-identical to the fleet). Harness: `/tmp/probe_master.py`.

---

## 0. Why A, not B (the wiring argument)

The whole core-x fleet is an **entity graph keyed on `uei` + the `core.name_norm` legal-entity
blocking key** — `crosswalk_sam_usaspending`, `contractor_award_summary`, `sam_pocs`,
`ffata_exec_comp`, GLEIF/EPA bridges all hang off UEI / normalized legal name.

| | **Bridge A — `employer` → UEI** | Bridge B — `name` → POC/officer |
|---|---|---|
| Left key | `name_norm(fec.employer)` (org) | `name_norm(fec.name)` (person) |
| Right target | SAM **entity** (`legal_business_name` → `uei`) | SAM **person** (`sam_pocs`/`ffata`) |
| Resolves to | a **UEI** → donor inherits the entire pre-built graph (awards, NAICS/PSC, POCs, exec-comp, hierarchy) | a person row only (donor↔POC, no graph) |
| Precedent | **proven** — identical shape to the FEC×MSHA org recon + `crosswalk_sam_usaspending` | **greenfield** — no person spine, homonym-heavy |
| State now | **not built** | **not built** (recon only) |

A is the high-leverage link: one normalized-name join lands FEC on the UEI spine and everything
downstream comes free. B is the literal ask but isolated. **Neither exists yet.** A's right side **is
the entity master** — hence the canonicalization question below.

---

## 1. Canonical entity master — VERDICT

**`sam_master_entities` (1,541,566) is canonical/newer. `sam_entity_master` (782,543) is the
outdated predecessor — still live in R2 (cutover never completed).**

| Evidence | `sam_master_entities` (GOLDEN) | `sam_entity_master` (THIN v3) |
|---|---|---|
| Rows = distinct UEI | **1,541,566** | 782,543 |
| Scope | all-time v2 universe, latest-row-per-uei | latest snapshot, **active-only** |
| `is_active` split | **782,543 True** / 759,023 False | n/a (all `registration_status='A'`) |
| Latest extract label | **`20260503`** (daily) | `2026_MAY` (monthly) |
| max `last_update_date` | **2026-05-03** | 2026-05-02 |
| Columns | **69** (full public dict + `is_active` + tenure + parsed arrays) | 17 |
| Indices | BTREE `uei`,`primary_naics`,`cage_code` | BTREE `uei`,`primary_naics` |
| Builder | `pipelines/sam_gov/sam_master.py` (current) | `pipelines/sam_gov/sam_entity_master.py` (**superseded**, plan §6 marks for removal) |

**The clincher:** the golden's `is_active=True` count (**782,543**) equals the thin master's entire
row count, exactly. The thin dataset *is* the active slice of the golden — the golden is a strict
superset (+759,023 historical/expired entities) on a fresher extract. No production/FEC consumer
hard-depends on either (the only non-builder reference to the thin one is
`COREX_GTM_CONTROL_SURFACE.md`, which should be repointed). **Use `sam_master_entities`.**

---

## 2. Bridge-A join surface — the gap

`employer` is RAW on FEC (no normalized column; `name_norm` on the fly — same as MSHA recon). The
right side needs **normalized legal name + geo + UEI** in one place. No single dataset has all three:

| Surface | Entities | normalized-name key | geo (state/zip) | UEI |
|---|---|---|---|---|
| **`sam_master_entities`** | 1.54M (1.48M name∧geo) | ❌ **absent** — compute `name_norm(legal_business_name)` on the fly | ✅ 96.2% / 98.2% | ✅ |
| `crosswalk_sam_usaspending` | 1.03M UEI | ✅ **BTREE** `normalized_legal_name` — but **51.6% fill** (503,719 distinct), recipient-anchored | ❌ none | ✅ |
| `sam_entity_master` (thin) | 782k active | ❌ absent | ✅ | ✅ |
| `sos_normalized_master` | SoS only (no SAM) | ✅ BTREE | ✅ zip | ❌ **no UEI** |

- The **golden master has geo+UEI but no normalized blocking key/BTREE** → an `employer`-name join
  is a 1.54M full-scan `name_norm` per probe, not an index lookup.
- The **crosswalk has the BTREE but only 51.6% name coverage and no geo** → can't run the
  State/ZIP false-positive shield, and misses ~half the registry.

**Minimal fix to make A index-ready (recommended, EPA precedent — commit `86cbef6`):** add
`normalized_legal_name = name_norm(legal_business_name)` and `legal_name_base` as materialized
columns + **BTREE** on `sam_master_entities` (rebuild). One dataset then carries normalized name +
geo + UEI + firmographics = the ideal FEC-employer landing surface. *(This is a data-plane build, out
of scope for this read-only recon — teed up as the next action.)*

---

## 3. Join path (once §2 fix lands)

```sql
-- LEFT · FEC employer → canonical key + geo (RAW employer, name_norm on the fly; sentinels dropped)
WITH fec AS (
  SELECT sub_id, name_norm("employer") AS emp_key, upper(state) AS state2, left(zip_code,5) AS zip5,
         transaction_amt, transaction_dt, cycle_year
  FROM fec_individual_contributions
  WHERE entity_tp='IND' AND employer IS NOT NULL AND state IS NOT NULL
    AND name_norm("employer") NOT IN ('RETIRED','SELF EMPLOYED','SELF','NONE','NOT EMPLOYED',
        'NA','INFORMATION REQUESTED','HOMEMAKER','UNEMPLOYED','REQUESTED')
)
-- RIGHT · sam_master_entities with the new BTREE normalized_legal_name (+ legal_name_base pass-2)
SELECT f.sub_id, e.uei, e.cage_code, e.legal_business_name, e.primary_naics, e.is_active,
       f.transaction_amt, f.transaction_dt,
       (f.zip5 = left(e.physical_address_zip_postal_code,5)) AS zip_confirms
FROM   fec f
JOIN   sam_master_entities e
  ON   e.normalized_legal_name = f.emp_key                       -- BTREE exact (after §2)
 AND   e.physical_address_province_or_state = f.state2;          -- ⚠️ WRONG (review §B2): geo SCORES, not gates — see correction below
-- e.uei → joins crosswalk_sam_usaspending, contractor_award_summary, sam_pocs, ffata_exec_comp, …
-- Pass-2 drift: legal_name_base(emp_key) ↔ e.legal_name_base recovers LLC/INC suffix variance.
```

> **⚠️ CORRECTED — do not use the §3 join as written.** The original claim here ("ZIP is a *hard
> tiebreaker*, both sides corporate address") is **false**: FEC `state`/`zip_code` are the donor's
> **residence**, not the employer's. Review §B2 probed **32.67%** donor-home ≠ employer-HQ (55.5% for
> national employers), so the `= f.state2` predicate above is a false-negative gate — **geo must score,
> not filter.** Review §B1/§B3 further correct it: block on `legal_name_base` first (free-text donors
> omit LLC/INC), treat `normalized_legal_name` as the precision tier, and resolve name→uei multiplicity
> (max 2,184 uei/name) with a candidate set + confidence — never a silent fan-out. The corrected join
> belongs to the **deferred FEC-bridge plan**, not here.

---

## 4. Addressable universe (A)

- **Left (FEC):** 282,923,196 rows; `employer` fill 94.97%; **≈6,199,428 distinct `name_norm`
  employer keys**; bridge-eligible (employer∧state∧zip) ≈ **268.4M rows / 94.9%**. Caveat: `employer`
  is **38-char truncated** (prefix-pass needed for len=38) — see the MSHA recon §4.
- **Right (`sam_master_entities`):** **1,466,764 distinct `name_norm(legal_business_name)`** keys;
  **1,479,205 (95.96%) entities carry name∧state∧zip** (legal_business_name is full, **not
  truncated** — clean exact-join target). 782,543 currently active; 759,023 historical (still valid
  past-employer matches).
- **Match engine:** deterministic `name_norm` + `legal_name_base` + State/ZIP — **no fuzzy primitive
  exists in the fleet** (consistent with every prior bridge).

---

## 5. Next actions (not done here — read-only recon)

1. **Make A index-ready:** rebuild `sam_master_entities` with `normalized_legal_name` +
   `legal_name_base` materialized columns + BTREE (EPA-style, `86cbef6`). The one blocker for an
   index-speed FEC employer→UEI join with geo.
2. **Retire the thin master:** delete `s3://data-sink/active/sam_entity_master/` and repoint
   `COREX_GTM_CONTROL_SURFACE.md` to the golden (cutover the plan deferred).
3. **Then build the FEC×SAM employer crosswalk** (`crosswalk_fec_sam_employer`) on §3.

---

## Appendix — live evidence (probe 2026-06-05)

- **`sam_master_entities`:** 1,541,566 rows = distinct uei · is_active T/F 782,543/759,023 ·
  sam_extract_code A/E 803,541/738,025 · latest label `20260503` (876,399) · max last_update
  2026-05-03 · legal_business_name 100% · phys_state 96.17% (1,482,557) · phys_zip 98.2%
  (1,513,782) · **distinct name_norm(legal) 1,466,764** · name∧state∧zip 1,479,205 ·
  **`normalized_legal_name` column ABSENT**.
- **`sam_entity_master`:** 782,543 rows = distinct uei · registration_status all `A` · label all
  `2026_MAY` · max last_update 2026-05-02 · phys_state 96.72% · phys_zip5 98.49% · distinct
  name_norm(legal) 747,562 · 17 cols.
- **`crosswalk_sam_usaspending`:** 1,028,144 rows = distinct uei · `normalized_legal_name` BTREE,
  fill **51.58%** (530,359), distinct 503,719 · **no geo columns**.
- **`sos_normalized_master`:** SoS sources only (ny/ca/fl/co), BTREE `normalized_legal_name` +
  `legal_name_base` + `zip_code`, **no `uei`** → not a FEC→SAM UEI route.
- **Harness:** `/tmp/probe_master.py`; pylance 7.0.0 / duckdb 1.5.x / pyarrow ≥17; ephemeral `uv`
  env; Doppler `core-x/prd`. **Zero mutation.**
