# DSBS × SAM.gov Hybrid Certification MV — Implementation Plan

**Status:** AWAITING AUTHORIZATION. No builds, DDL, indexes, or datasets created. Read-only probes only.
**Target:** `s3://data-sink/active/govcon_sub_certifications_mv/` (Lance v2.1, serving tier).
**Author basis:** live probes of `sba_dsbs_certified_firms`, `sam_master_entities`, `govcon_subawardee_profiles`, `govcon_sub_targeting`, `sam_business_type_code_dict`, the serving-MV code idiom, and the HQX `ops.enrichment_cohort_runs` ledger — 2026-06-20.

---

## 0. Executive decisions (read first)

| # | Decision | Rationale (from live probe) |
|---|---|---|
| D1 | **Adjudicated cert truth is sourced ONLY from the DSBS `certs` JSON column**, not the `certDateExit_*`/`active_*_boolean` scalars. | `certDateExit_*` exists for only **4 of 9** programs (8a, 8aJV, SDVOSB, VOSB). EDWOSB/HUBZone/WOSB have **no exit-date scalar at all**. `certs` is 100% populated and carries `{name, status, active, entranceDate, exitDate}` for every program. |
| D2 | **"Currently certified" ≡ `certs[].status == 'Active'` AND (`exitDate IS NULL` OR `exitDate >= CURRENT_DATE`).** Do NOT equate `active_*_boolean=true` with certified. | `active_wosb_boolean=true` includes **2,058 Pending** applications; `active_edwosb_boolean=true` includes **1,458 Pending**. Plus a 21-row expired-but-active leak. Gating on `status='Active'` excludes both. |
| D3 | **Two strictly disjoint cert namespaces.** `cert_*` (adjudicated, DSBS-owned) and `sam_self_*` (self-certified, SAM-owned). The SAM namespace is filtered to designations with **no DSBS-adjudicated equivalent**. | Prevents double-counting/conflation. DSBS owns {8a, hubzone, wosb, edwosb, vosb, sdvosb}; SAM contributes only {minority-owned, self-cert SDB, woman-owned(general), veteran-owned(general)}. |
| D4 | **MV universe = `D ∪ P ∪ T` = 90,210 distinct UEIs**, 1 row per UEI. Greenfield (DSBS-only) = **64,760**, not the ~58k in the directive. | `T ⊆ P` (targeting adds 0 new UEIs); `\|D ∩ (P∪T)\| = 2,474`; `67,234 − 2,474 = 64,760`. The ~58k estimate undercounts by ~6,760 — flagged, see §3. |
| D5 | **Zero firmographics_blitz join.** `email`, `phone`, `contact_person` pulled natively from DSBS; subawardee POC fields carried as fallback for the 22,976 sub-only firms that have no DSBS record. | Per directive. DSBS native coverage: email 87.7%, phone 86.5%, contact_person 97.1%. Keeps the MV self-contained and rebuildable from 4 Lance sources + 1 dict. |
| D6 | **`govcon_sub_targeting` must be `DISTINCT`/aggregated to UEI grain before any join.** | It is edge-grain (PK = `contract_award_unique_key, candidate_sub_uei`); a naive UEI join fans out up to **821× per firm**. |
| D7 | **Build as a frozen-schema, snapshot-overwrite serving worker** (the `materialize_sub_targeting` / `build_subawardee_capability_profiles` discipline), not a read-model. Add a cross-namespace integrity assertion to `verify`. | This is a citeable GTM sink; it deserves `assert_schema`, deterministic aggregation, and a content-hash zero-delta DoD. |
| D8 | **Ledger remediation is a backfill + a code-hardening patch + a latent-bug fix**, not a "missing insert." | The DSBS cohort builders DO call `_record_run`; it swallowed an exception (likely `hqx-postgres` secret unresolved). A latent bug also writes `firms_gap` into the `firms_with_linkedin` column. |

**Naming note:** house serving datasets are `govcon_sub_*` with **no `_mv` suffix** (they are Lance datasets, not Postgres MVs). The directive's name `govcon_sub_certifications_mv` is honored verbatim, but be aware it breaks the `_mv`-less convention and may read as a Postgres MV to someone scanning `active/`. Recommend either keeping the directive name or renaming to `govcon_sub_certifications` for convention parity. **Operator's call.**

---

## 1. The Hybrid Certification rules engine

### 1.1 Namespace model — the anti-conflation contract

There are exactly two certification namespaces, and a designation belongs to **one and only one**:

```
ADJUDICATED  (cert_*)            owner = DSBS        truth = certs[].status='Active'
  8a · hubzone · wosb · edwosb · vosb · sdvosb   (+ 8a_jv, low volume)

SELF-CERTIFIED  (sam_self_*)     owner = SAM        truth = business_types ∩ dict(sba_administered=false, no-DSBS-equivalent)
  minority_owned (23) · self_cert_sdb (27) · woman_owned/general (A2) · veteran_owned/general (A5)
```

**The override is structural, not procedural.** `cert_*` columns are populated **exclusively** from the DSBS `certs` array. A firm absent from DSBS has no `certs` row → every `cert_*` is `false`/`NULL`, **regardless of what SAM claims**. SAM's adjudicated-program self-reps (`8W` WOSB, `QF` SDVOSB, `8C` WOSB-JV) and even SAM's *SBA-administered* codes (`A9`, `A6`, `XX`, `A0`, `JT`) are **never read into `cert_*`**. DSBS presence is necessary and sufficient for adjudicated truth. That is the entire override — no precedence CASE, no coalesce, no tie-break. The two namespaces are computed from disjoint sources and merged additively.

### 1.2 The SAM self-cert filter — exact rule (dict-driven)

`sam_business_type_code_dict` (12 rows, permanent, BTREE on `code`/`namespace`/`designation_key`) is the rules table. The self-cert namespace is:

```sql
-- candidate self-cert codes
SELECT code, designation_key
FROM sam_business_type_code_dict
WHERE sba_administered = false                       -- the "checked-the-box" half (7 codes)
  AND designation_key NOT IN (                        -- minus designations a DSBS program adjudicates
        'women_owned_small_business',                 -- 8W → owned by cert_wosb
        'joint_venture_women_owned_small_business',   -- 8C → owned by cert_wosb (JV)
        'service_disabled_veteran_owned_business'     -- QF → owned by cert_sdvosb
      );
-- RESULT (retained): 23 minority_owned_business · 27 self_certified_small_disadvantaged_business
--                    A2 woman_owned_business · A5 veteran_owned_business
```

**Why `women_owned_small_business` (8W) is excluded but `woman_owned_business` (A2) is retained:** they are different `designation_key`s. `8W`/`A9` = the WOSB *program* (small + NAICS-eligible + ownership), adjudicated as `cert_wosb`. `A2` = general woman-ownership, which the SBA does not adjudicate — a firm can be `sam_self_woman_owned=true` without being `cert_wosb=true`, and that is a **legitimate, non-conflated** distinction. Same logic for `A5` veteran-owned (general) vs `cert_vosb` (VA-adjudicated VOSB). This is the deliberate decision in D3 — if the operator wants the stricter reading (drop A2/A5 too, keep only 23/27), it is a one-line change to the exclusion set; flagged as **open decision O1** in §6.

**Self-documenting hardening (recommended):** add a derived `mv_disposition` column to `sam_business_type_code_dict` via its existing seed builder (`pipelines/serving/seed_sam_business_type_code_dict.py`) so the engine is fully data-driven and auditable rather than relying on an inline `NOT IN` list:

```
mv_disposition ∈ {'adjudicated_dsbs', 'self_cert_retained', 'ignored'}
  A6,A0,XX,JT,A9  → 'adjudicated_dsbs'   (sba_administered=true)
  8W,8C,QF        → 'adjudicated_dsbs'   (self-rep of a DSBS program — DSBS owns it)
  23,27,A2,A5     → 'self_cert_retained'
```
Then the MV filter becomes `WHERE mv_disposition = 'self_cert_retained'`. This keeps the namespace boundary in the reference table where it is reviewable, not buried in build SQL.

### 1.3 Adjudicated derivation from `certs` (exact SQL)

`certs` is a JSON-encoded array string, 100% populated. Display names map to program keys:
`'8(a)'→8a · '8(a) JV'→8a_jv · 'HUBZone'→hubzone · 'WOSB'→wosb · 'EDWOSB'→edwosb · 'VOSB'→vosb · 'SDVOSB'→sdvosb`.

```sql
-- one row per (uei, program); explode the certs array
WITH dsbs_exploded AS (
  SELECT
    d.uei,
    json_extract_string(c, '$.name')                       AS prog_name,
    json_extract_string(c, '$.status')                     AS prog_status,
    TRY_CAST(json_extract_string(c, '$.exitDate') AS DATE) AS exit_date
  FROM dsbs d,
       UNNEST(from_json(d.certs, 'JSON[]')) AS t(c)
),
dsbs_active AS (                          -- D2 gate: Active + not expired
  SELECT uei, prog_name, exit_date
  FROM dsbs_exploded
  WHERE prog_status = 'Active'
    AND (exit_date IS NULL OR exit_date >= CURRENT_DATE)
),
dsbs_pivot AS (
  SELECT
    uei,
    bool_or(prog_name = '8(a)')    AS cert_8a,
    bool_or(prog_name = '8(a) JV') AS cert_8a_jv,
    bool_or(prog_name = 'HUBZone') AS cert_hubzone,
    bool_or(prog_name = 'WOSB')    AS cert_wosb,
    bool_or(prog_name = 'EDWOSB')  AS cert_edwosb,
    bool_or(prog_name = 'VOSB')    AS cert_vosb,
    bool_or(prog_name = 'SDVOSB')  AS cert_sdvosb,
    max(exit_date) FILTER (WHERE prog_name = '8(a)')    AS cert_8a_exit_date,
    max(exit_date) FILTER (WHERE prog_name = 'HUBZone') AS cert_hubzone_exit_date,
    max(exit_date) FILTER (WHERE prog_name = 'WOSB')    AS cert_wosb_exit_date,
    max(exit_date) FILTER (WHERE prog_name = 'EDWOSB')  AS cert_edwosb_exit_date,
    max(exit_date) FILTER (WHERE prog_name = 'VOSB')    AS cert_vosb_exit_date,
    max(exit_date) FILTER (WHERE prog_name = 'SDVOSB')  AS cert_sdvosb_exit_date,
    min(exit_date) FILTER (WHERE exit_date >= CURRENT_DATE) AS next_cert_expiration_date,
    count(DISTINCT prog_name)                            AS cert_count,
    string_agg(DISTINCT CASE prog_name
        WHEN '8(a)' THEN '8a' WHEN '8(a) JV' THEN '8a_jv' WHEN 'HUBZone' THEN 'hubzone'
        ELSE lower(prog_name) END, '|' ORDER BY 1)       AS cert_programs
  FROM dsbs_active
  GROUP BY uei
)
```
`cert_any = cert_count > 0`. `next_cert_expiration_date` (BTREE) powers an expiring-soon GTM motion (§5). JV programs `vosb_jv`/`sdvosb_jv` are **structurally empty** in this snapshot (0 rows, names never appear in `certs`) — schema-reserve `cert_8a_jv` only; omit the empty two.

### 1.4 Self-cert derivation from SAM (exact SQL)

```sql
WITH sam_selfcert AS (
  SELECT
    e.uei,
    bool_or(dk.designation_key = 'minority_owned_business')                       AS sam_self_minority_owned,
    bool_or(dk.designation_key = 'self_certified_small_disadvantaged_business')   AS sam_self_sdb,
    bool_or(dk.designation_key = 'woman_owned_business')                          AS sam_self_woman_owned,
    bool_or(dk.designation_key = 'veteran_owned_business')                        AS sam_self_veteran_owned,
    string_agg(DISTINCT dk.code, '|' ORDER BY 1)                                  AS sam_self_codes,
    bool_or(e.is_active)                                                          AS sam_registration_active
  FROM sam_master_entities e,
       UNNEST(e.business_types) AS t(bt)
  JOIN sam_business_type_code_dict dk
    ON dk.namespace = 'business_types' AND dk.code = bt
   AND dk.sba_administered = false
   AND dk.designation_key NOT IN ('women_owned_small_business',
                                  'joint_venture_women_owned_small_business',
                                  'service_disabled_veteran_owned_business')
  GROUP BY e.uei
)
```
`sam_self_any = minority OR sdb OR woman OR veteran`. `e.is_active` collapses to one value per UEI (entities is 1-row-per-UEI), so `bool_or` is a safe grain-preserver. `sam_present` (uei in `sam_master_entities` at all) is computed from a separate `LEFT JOIN` flag, independent of self-cert.

### 1.5 No-double-count guarantee (assert in `verify`)

By construction `cert_*` and `sam_self_*` share **zero** designations. The `verify` step asserts the invariant so a future dict edit cannot silently reintroduce conflation:

```sql
-- must return 0 rows: no firm may carry an adjudicated program in its self-cert namespace
SELECT count(*) AS conflation_violations
FROM govcon_sub_certifications_mv
WHERE (sam_self_woman_owned   AND cert_wosb)      -- only flagged if A2/general were ever mismapped to WOSB
   OR (sam_self_veteran_owned AND cert_vosb AND false);  -- intentionally inert; template for stricter checks
-- Primary integrity check: assert sam_self_codes ∩ {8W,8C,QF,A9,A6,XX,A0,JT} = ∅ for every row.
```
The load-bearing assertion is the second comment: **no retained `sam_self_codes` value may be an adjudicated code**. That proves the namespaces never overlap.

---

## 2. The Target MV — `govcon_sub_certifications_mv`

### 2.1 Universe construction (the UNION) + exact join conditions

Spine = distinct UEI over the three populations, then LEFT JOIN each enrichment at **UEI grain** (every source pre-collapsed to 1 row/UEI first):

```sql
-- (a) UEI spine — the 90,210-row population
WITH spine AS (
  SELECT uei FROM dsbs                                   -- D : 67,234
  UNION
  SELECT sub_uei AS uei FROM subawardee_profiles         -- P : 25,450  (already 1/UEI)
  UNION
  SELECT DISTINCT candidate_sub_uei AS uei FROM sub_targeting   -- T : 14,610 (DISTINCT collapses 165,974 edges; T ⊆ P)
),
-- (b) targeting pre-aggregated to UEI grain (D6 — avoid 821× fan-out; D anomaly: sentinel date)
t_agg AS (
  SELECT candidate_sub_uei AS uei,
         count(*)                                   AS n_targeting_edges,
         max(last_subaward_action_date)
           FILTER (WHERE last_subaward_action_date < DATE '2100-01-01') AS last_targeting_action_date
  FROM sub_targeting
  GROUP BY candidate_sub_uei
)
SELECT
  s.uei,
  -- membership flags
  (dc.uei IS NOT NULL)                              AS in_dsbs,
  (p.sub_uei IS NOT NULL)                           AS is_subawardee,
  (t.uei IS NOT NULL)                               AS is_targeting_candidate,
  (dc.uei IS NOT NULL AND p.sub_uei IS NULL AND t.uei IS NULL) AS is_greenfield,
  -- adjudicated namespace (from §1.3)
  coalesce(dp.cert_8a,false) AS cert_8a, dp.cert_8a_exit_date, /* … all cert_* … */
  -- self-cert namespace (from §1.4)
  coalesce(sc.sam_self_minority_owned,false) AS sam_self_minority_owned, /* … */
  coalesce(sc.sam_registration_active,false) AS sam_registration_active,
  (sm.uei IS NOT NULL)                              AS sam_present,
  -- decoupled contact payload (D5): DSBS native, POC fallback
  d.email, d.phone, d.contact_person, d.website,
  p.poc_full_name, p.poc_title,
  CASE WHEN d.email IS NOT NULL OR d.phone IS NOT NULL THEN 'dsbs'
       WHEN p.poc_full_name IS NOT NULL THEN 'subaward_poc' ELSE 'none' END AS contact_source,
  -- geo/firmographic (DSBS first, profile fallback)
  coalesce(d.state, p.hq_state)     AS state,
  coalesce(d.city,  p.hq_city)      AS city,
  d.zipcode, d.county, d.naics_primary,
  CASE WHEN d.state IS NOT NULL THEN 'dsbs' WHEN p.hq_state IS NOT NULL THEN 'subaward' ELSE NULL END AS geo_source,
  -- subaward footprint (from P/T)
  (p.sub_uei IS NOT NULL)           AS has_subaward_history,
  p.n_subawards, p.total_subaward_amount, p.top_subaward_description,
  t.n_targeting_edges
FROM spine s
LEFT JOIN dsbs               d  ON d.uei  = s.uei            -- 1:1
LEFT JOIN dsbs_pivot         dp ON dp.uei = s.uei            -- 1:1 (active certs only)
LEFT JOIN (SELECT DISTINCT uei FROM dsbs) dc ON dc.uei = s.uei
LEFT JOIN subawardee_profiles p ON p.sub_uei = s.uei        -- 1:1 (clean PK)
LEFT JOIN t_agg              t  ON t.uei  = s.uei            -- 1:1 (pre-aggregated)
LEFT JOIN sam_selfcert       sc ON sc.uei = s.uei           -- 1:1 (pre-aggregated)
LEFT JOIN (SELECT DISTINCT uei FROM sam_master_entities) sm ON sm.uei = s.uei
```

**Join-condition invariants (all verified against live grain):**
- Every join is UEI = UEI, 1:1, after pre-aggregation. `dsbs` (1/UEI), `subawardee_profiles` (1/UEI on `sub_uei`), `sam_master_entities` (1/UEI). `sub_targeting` and `sam_selfcert` are GROUP BY'd to 1/UEI before the join.
- **UEI is the only join key in the entire system** — no `cage_code`/`duns`/`ein` exists on the govcon side. Firms that don't share a UEI cannot be linked (e.g., the 500 DSBS firms absent from SAM stay unlinked on the SAM side; acceptable).
- `universe_tier` derived: `greenfield_dsbs` (in_dsbs ∧ ¬is_subawardee) · `dsbs_and_sub` (in_dsbs ∧ is_subawardee) · `sub_only` (¬in_dsbs).
- **Grain guard before write:** `assert rows == (SELECT count(*) FROM spine)` and `assert count(*) == count(DISTINCT uei)`.

### 2.2 Universe arithmetic (live)

| Tier | UEIs | Cert payload | Contact source |
|---|---:|---|---|
| Greenfield DSBS-only (`is_greenfield`) | **64,760** | full adjudicated certs | DSBS native |
| DSBS ∩ subawardee (`in_dsbs ∧ is_subawardee`) | **2,474** | full adjudicated certs + subaward footprint | DSBS native |
| Sub-only (`¬in_dsbs`) | **22,976** | **no `cert_*`** (not adjudicated); SAM self-cert only | subaward POC fallback |
| **Total MV rows** | **90,210** | | |

Greenfield SAM resolvability: 99.2% present in SAM, 97.1% self-certify, **74.7% SAM-active** → ~16,353 certified firms are SAM-inactive (carry `sam_registration_active=false` as a deliverability flag).

### 2.3 Proposed schema & column list (1 row / UEI)

| column | type | index | source |
|---|---|---|---|
| `uei` | string | **BTREE** | spine |
| `legal_business_name` | string | — | DSBS ▸ P.sub_name |
| `universe_tier` | string | BITMAP | derived |
| `in_dsbs` `is_subawardee` `is_targeting_candidate` `is_greenfield` | bool | BITMAP | membership |
| `cert_any` | bool | BITMAP | §1.3 |
| `cert_count` | int32 | — | §1.3 |
| `cert_programs` | string | BITMAP | §1.3 (pipe list) |
| `cert_8a` `cert_hubzone` `cert_wosb` `cert_edwosb` `cert_vosb` `cert_sdvosb` `cert_8a_jv` | bool | BITMAP | §1.3 |
| `cert_8a_exit_date` `cert_hubzone_exit_date` `cert_wosb_exit_date` `cert_edwosb_exit_date` `cert_vosb_exit_date` `cert_sdvosb_exit_date` | date32 | **BTREE** | §1.3 |
| `next_cert_expiration_date` | date32 | **BTREE** | §1.3 (min future exit) |
| `cert_lifecycle` | string | BITMAP | derived: `expiring_90d` / `active` / `none` |
| `sam_self_any` `sam_self_minority_owned` `sam_self_sdb` `sam_self_woman_owned` `sam_self_veteran_owned` | bool | BITMAP | §1.4 |
| `sam_self_codes` | string | — | §1.4 (audit) |
| `sam_registration_active` `sam_present` | bool | BITMAP | SAM |
| `email` `phone` `contact_person` `website` | string | — | **DSBS native** (D5) |
| `poc_full_name` `poc_title` | string | — | P fallback |
| `contact_source` | string | BITMAP | derived |
| `state` | string | BITMAP | DSBS ▸ P.hq_state |
| `city` `county` | string | — | DSBS ▸ P |
| `zipcode` | string | **BTREE** | DSBS |
| `naics_primary` | string | **BTREE** | DSBS |
| `geo_source` | string | BITMAP | derived |
| `has_subaward_history` | bool | BITMAP | P |
| `n_subawards` `n_targeting_edges` | int32 | — | P / T |
| `total_subaward_amount` | double | BTREE (opt) | P |
| `top_subaward_description` | string | — | P |
| `dsbs_snapshot` | string | — | provenance (DSBS source_version) |
| `built_at` | timestamp[us,tz=UTC] | — | run stamp |

**Indexing strategy (zero-join frontend filtering).** Rule, consistent with the fleet: resolution keys + every range-filterable date → **BTREE**; every boolean / enum / state / low-cardinality category → **BITMAP**; lists/free-text → unindexed.
- **BTREE (≈11):** `uei`, `naics_primary`, `zipcode`, `next_cert_expiration_date`, and the 6 `cert_<prog>_exit_date` columns (drives "expires before X" range scans), optionally `total_subaward_amount`.
- **BITMAP (≈26):** `state`, `universe_tier`, `cert_programs`, `cert_lifecycle`, `contact_source`, `geo_source`, the 4 membership flags, `cert_any` + 7 `cert_<prog>` bools, the 5 `sam_self_*` bools, `sam_registration_active`, `sam_present`, `has_subaward_history`. This lets the frontend AND/OR-compose any set-aside × geo × NAICS × lifecycle filter with bitmap intersection and **no join**.

`cert_programs` cardinality is the distinct pipe-combinations (~hundreds, per the DSBS `cert_programs` distribution) — well within BITMAP's sweet spot.

---

## 3. Schema anomalies detected (must be handled, not worked around)

| # | Anomaly | Location | Handling in this plan |
|---|---|---|---|
| A1 | `certDateExit_*` scalar exists for only 4/9 programs; **absent for EDWOSB, HUBZone, WOSB** | DSBS | Source all expiration from `certs[].exitDate` (§1.3). Never read the scalars. |
| A2 | `active_*_boolean=true` includes **2,058 Pending WOSB + 1,458 Pending EDWOSB** | DSBS | Gate on `certs[].status='Active'` (D2), not the boolean. |
| A3 | `certPending_*` bool undercounts `certStatus='Pending'` by ~1.5% | DSBS | Ignore `certPending_*`; `status` from `certs` is authoritative. |
| A4 | `SDVOSB JV` / `VOSB JV` columns 100% empty; names never appear in `certs` | DSBS | Schema-reserve only `cert_8a_jv`; omit the two empty JV programs. |
| A5 | 21-row expired-but-active leak (`active=true` ∧ `exitDate < today`) | DSBS | `exitDate >= CURRENT_DATE` clause (D2) removes all 21. |
| A6 | `govcon_sub_targeting` is **edge-grain**, 821× UEI fan-out | T | `GROUP BY candidate_sub_uei` before join (D6, §2.1). |
| A7 | Far-future **sentinel `2106-12-01`** in `last_subaward_action_date` (T) and `teaming_last_action_date` (P) | P, T | Filter `< DATE '2100-01-01'` in every date aggregation touching these. |
| A8 | `T ⊆ P` — targeting contributes **0 net-new UEIs** | P/T | Universe is `P ∪ greenfield`; T only sets `is_targeting_candidate` + edge counts. |
| A9 | **Greenfield = 64,760, not ~58k** (directive estimate off by ~6,760) | universe | Plan sizes to 64,760 / total 90,210. Flag to operator. |
| A10 | `req_clearance_level_max` is BITMAP-indexed but 92.4% NULL (in P) | P | Not consumed by the MV; noted for P's own hygiene, out of scope here. |
| A11 | **No `cage_code`/`duns` join key** anywhere on the govcon side | all | UEI-only joins; unlinkable firms stay unlinked (acceptable, §2.1). |

---

## 4. Ledger remediation — `ops.enrichment_cohort_runs`

**Root cause (corrected):** the DSBS cohort builders **do** call `_record_run` (`cohort_sba_dsbs_certified.py:366`, `cohort_sba_dsbs_certified_gap_domains.py:364`). The insert failed and was **swallowed** by a broad `except` that only prints `WARN` (`cohort_sba_dsbs_certified.py:318-319`). The cohort Parquet had already published to R2 (publish runs before the PG write in the `finally`), so the symptom is "published cohort, no provenance row." Most likely the `hqx-postgres` secret wasn't resolved in the new Modal apps' build worker.

### 4.1 Backfill (run once against HQX) — 2 rows

`#573` chunk files are an operational **re-slice** of the already-built `#572` cohort (6 files written within ~1s, summing to the 23,973 parent) via the **unwired** `_publish_chunks` (`cohort_sba_dsbs_certified_gap_domains.py:251-278`, never called by `_run`). They are not a distinct cohort build → **no separate ledger row**; per-chunk enrollment belongs in `ops.enrichment_blitz_runs`, not the cohort-build ledger.

```sql
-- id (IDENTITY) and recorded_at (DEFAULT now()) auto-generate; column order per ops_enrichment_cohort_runs.sql
INSERT INTO ops.enrichment_cohort_runs
    (feed, cohort_name, firms_total, firms_with_domain, firms_pdl_matched,
     firms_with_linkedin, distinct_urls, r2_key, column_name, status, error,
     started_at, completed_at)
VALUES
  ('enrichment_cohort_sba_dsbs',                                   -- #571 LinkedIn → Workflow B
   'sba_dsbs_certified_firms_linkedin',
   67234, 52698, 28422, 26502, 26502,
   'cohorts/enrichment_blitz/sba_dsbs_certified_firms_linkedin.parquet',
   'company_linkedin_url', 'success', NULL,
   TIMESTAMPTZ '2026-06-20 09:52:27+00', TIMESTAMPTZ '2026-06-20 09:52:27+00'),
  ('enrichment_cohort_sba_dsbs_gap',                               -- #572 gap domains → Workflow A
   'sba_dsbs_certified_firms_gap_domains',
   67234, 52698, 28422, 0, 23973,                                  -- firms_with_linkedin = 0 by construction
   'cohorts/enrichment_blitz/sba_dsbs_certified_firms_gap_domains.parquet',
   'normalized_domain', 'success', NULL,
   TIMESTAMPTZ '2026-06-20 10:10:38+00', TIMESTAMPTZ '2026-06-20 10:10:38+00');
```
`firms_pdl_matched=28422` is the firms-with-a-PDL-hit count (DDL semantics), mirroring the live `equipment_rental_firms_cascade_domains` precedent (`firms_pdl_matched=3322`, `firms_with_linkedin=0`, `distinct_urls=1654`). `firms_gap` has **no column** — it is not persisted.

### 4.2 Code hardening (so a silent failure can't recur)

In both DSBS builders, make `_record_run` return a bool, log failures to **stderr**, and surface `ledger_written` in the Trigger callback so the orchestrator can alert:

```python
def _record_run(...) -> bool:
    try:
        conn = _open_conn(_hqx_dsn())
        try:
            cur = conn.cursor(); cur.execute(OPS_DDL)
            cur.execute("INSERT INTO ops.enrichment_cohort_runs (...) VALUES (%s, …)", (...))
        finally:
            conn.close()
        return True
    except Exception as exc:  # noqa: BLE001 — audit must not mask the build outcome
        import sys
        print(f"ERROR: ops.enrichment_cohort_runs write FAILED (cohort published to R2 "
              f"but UNJOURNALED): {exc}", file=sys.stderr)
        return False
# … in _run finally: ledger_written = _record_run(...); include "ledger_written": ledger_written in callback
```

### 4.3 Latent bug fix (independent of the silent failure)

`cohort_sba_dsbs_certified_gap_domains.py:313` passes `stats.get("firms_gap", 0)` (=24,276) into the **7th positional value**, which the column list binds to **`firms_with_linkedin`**. A successful gap run would write `firms_with_linkedin = 24276` — semantically wrong (a gap firm has no LinkedIn). Fix: pass literal `0` there, mirroring `cohort_equipment_rental_cascade.py:280`. The backfill in §4.1 already encodes the correct `0`.

### 4.4 `sam_business_type_code_dict` — already permanent, no action

Live: 12 rows, 11 cols, **3 BTREE** (`code`, `namespace`, `designation_key`), 4 Lance versions, builder `pipelines/serving/seed_sam_business_type_code_dict.py` (idempotent overwrite, row-count gate, `--verify`), consumers wired (`materialize_subawardee_designations.py`, `materialize_equipment_rental_construction_match.py`), documented (`docs/reference/SUBAWARDEE_DESIGNATIONS.md`). It is correctly **not** ledgered (hand-curated reference seed; provenance is in-band via `confidence`/`source`/`source_version`/`built_at` + Lance version history). **The only recommended change is additive:** extend the seed with the `mv_disposition` column (§1.2) so the namespace boundary is reviewable in the reference table. This is the directive's "materialization of the permanent reference table" requirement — it already exists; this plan consumes it and proposes one optional column.

---

## 5. Strategic value beyond the directive

1. **Recertification-outreach engine.** `next_cert_expiration_date` (BTREE) + `cert_lifecycle='expiring_90d'` turns the MV into a time-phased GTM trigger: every firm whose adjudicated cert lapses in the next quarter is a high-intent outreach target (recert assistance, teaming before status loss). This is the single highest-leverage column the directive didn't ask for.
2. **Greenfield is the asset being bought.** 64,760 federally-certified firms with **no current subaward footprint** = net-new TAM. `is_greenfield` + `cert_any` + `state`/`naics_primary` is the prospecting cut. Size it explicitly so the operator sees the 64,760 (not 58k).
3. **Deliverability as a first-class filter.** ~16,353 certified firms are SAM-inactive. `sam_registration_active=false` + `contact_source` lets campaigns suppress stale-registration firms or route them to a re-verification lane instead of burning sends.
4. **"Self-certified but uncertified" wedge.** Sub-only + `sam_self_woman_owned`/`veteran_owned`/`sdb` with **no `cert_*`** = firms claiming a status they haven't formally certified — a productizable "get-certified" funnel, and the inverse of the adjudicated cohort. The MV exposes both sides natively.
5. **Audit-grade integrity gate.** The cross-namespace assertion (§1.5) in `verify` makes conflation a build-breaking error, not a silent data smell — the dict can evolve without risking double-counting.
6. **Decoupling pays compounding interest.** Because contact is DSBS-native and certs are `certs`-native, the MV rebuilds from 4 Lance datasets + 1 dict with zero dependency on the enrichment-blitz funnel — it stays correct even if firmographics_blitz is mid-rebuild.

---

## 6. End-to-end build sequence (on authorization)

> Blast-radius order: ledger remediation is independent and ships first; the MV is additive (new dataset, no mutation of sources).

1. **Ledger remediation (independent).** Run §4.1 backfill; apply §4.2 hardening + §4.3 latent-bug fix to the two DSBS cohort builders (and, for parity, the two rental builders). Verify 4 rows now in `ops.enrichment_cohort_runs` (2 rental + 2 DSBS).
2. **Dict enhancement (optional, additive).** Add `mv_disposition` to `seed_sam_business_type_code_dict.py`; re-seed (idempotent overwrite, 12 rows, +1 col, re-index).
3. **Frozen schema.** Add `SUB_CERTIFICATIONS_MV_SCHEMA` + URI to `pipelines/sam_gov/govcon_gtm_schemas.py` (the §2.3 column list, exact Arrow types).
4. **Materializer.** Author `pipelines/serving/materialize_sub_certifications.py` from the house skeleton: `_r2_so` → `_duck` (`PRAGMA threads=1` for zero-delta determinism, `memory_limit`/`temp_directory`) → materialize each source with `.scanner(columns=[...]).to_table()` (never `.to_reader()`) → the §1.3/§1.4/§2.1 CTEs → `.to_arrow_table()`.
5. **Guards before write:** empty-output (`rows>0`), grain (`rows == count(DISTINCT uei) == |spine|`), `assert_schema`-equality, cross-namespace integrity (§1.5).
6. **Write + index:** `lance.write_dataset(mode="overwrite", data_storage_version="2.1", storage_options=so)` → `create_scalar_index(..., replace=True)` over the §2.3 BTREE/BITMAP lists → `assert_schema(URI, schema, so)` post-write.
7. **Ledger:** `ops.sub_certifications_mv_serving_runs` (inline `OPS_DDL`, lazy-create via `to_regclass`, `Jsonb` metrics, written in `finally:` on success AND failure, best-effort).
8. **Modal-dual entrypoints** (`r2-credentials` + `hqx-postgres` secrets; `build`/`verify`); `verify --content-hash` excluding `built_at` for a zero-delta DoD.
9. **Rebuild cadence:** snapshot-overwrite after each DSBS refresh and each SAM monthly extract. Recency lives in the data (`next_cert_expiration_date`, `built_at`, `dsbs_snapshot`), not in cadence.

### Open decisions for the operator (O-list)
- **O1 — SAM self-cert breadth.** Retain `woman_owned`(A2)/`veteran_owned`(A5) as distinct general-ownership self-reps (this plan's default, D3), or restrict the SAM namespace to only `minority_owned`(23)/`self_cert_sdb`(27) per the directive's two examples? One-line exclusion-set change.
- **O2 — Dataset name.** Keep `govcon_sub_certifications_mv` (directive) or rename to `govcon_sub_certifications` for `_mv`-less convention parity.
- **O3 — `mv_disposition` column.** Add to the dict (data-driven, auditable namespace boundary) or keep the boundary as an inline `NOT IN` list in the build SQL.
- **O4 — `total_subaward_amount` BTREE.** Index for range filtering ("subs > $X"), or leave unindexed if the frontend only filters on certs/geo/NAICS.

---

*Probes that grounded this plan (read-only, 2026-06-20): `sba_dsbs_certified_firms` cert-temporal surface; `govcon_subawardee_profiles`/`govcon_sub_targeting` grain+indices; serving-MV code idiom; `ops.enrichment_cohort_runs` + cohort builders; live UEI-set algebra across D/P/T/S. Findings: `/tmp/wf_findings/{A,B,C,D,E}_*.md`.*
