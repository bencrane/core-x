# FEC → MSHA Entity-Bridge Reconnaissance

Read-only schema diagnostic mapping the join vectors to crosswalk **FEC itemized individual
contributions** (the donor's self-reported `EMPLOYER`) against the **MSHA (Mine Safety & Health
Administration)** operator / controller / contractor universe. The objective: trace individual
political capital back to industrial mining entities by normalizing noisy, self-reported donor
employer strings and binding them to MSHA's native name registries on **Name + State + ZIP5**.

- **Live targets (`s3://data-sink/active/`):** `fec_individual_contributions`,
  `msha_corporate_history`, `msha_mines`, `msha_contractors`, `msha_enforcement_ledger` —
  `pylance 7.0.0` schema read + `count_rows(filter=)` pushdown + ONE full-set DuckDB column
  scan (employer/state/zip_code/cycle_year) for cardinality.
- **As-of:** probe **2026-06-05**. FEC SoR = 282,923,196 rows, cycles **1980–2026 (24)**.
  MSHA SoR written 2026-06-03 (schemas re-confirmed live this probe, unchanged).
- **Attestation:** every figure is a live read. **Zero data-plane mutation** — no `.lance`
  writes, no DDL, no indexes, no `ops.*` rows. Probe deps in an ephemeral `uv` venv
  (`/tmp/fec_msha_probe_meta.py`, `/tmp/fec_bridge_probe_agg.py`); Doppler-injected R2 creds
  (`core-x/prd`).
- **Companion:** [`GLEIF_MSHA_ENTITY_BRIDGE_DIAGNOSTIC.md`](GLEIF_MSHA_ENTITY_BRIDGE_DIAGNOSTIC.md)
  fully maps the MSHA right side (geo traversal, sole-prop filter, fan-out). This file reuses that
  treatment and focuses the new probe on the FEC left side.

---

## 0. Headline verdict

| # | Finding | Verdict |
|---|---|---|
| 1 | **FEC master is materialized: `fec_individual_contributions`** — 282,923,196 rows, 24 cycles (1980–2026), donor fields `name/city/state/zip_code/employer/occupation`. BTREE on raw `employer`; BITMAP on `state`,`entity_tp`,`cycle_year`. | ✅ exists |
| 2 | **`EMPLOYER` is strictly RAW.** Ingest applies only `nullif(trim())` — no `name_norm` at rest, **no `normalized_legal_name` column.** Source-uppercased, but `&`/punctuation/`LLC`/`INC`/parentheticals all intact. Bridge must compute `name_norm(employer)` **on the fly**. | ⚠️ raw |
| 3 | **EMPLOYER null density 5.03%** (14,217,620 null). Geo near-complete: **STATE null 0.12%**, **ZIP_CODE null 0.18%**. Bridge-eligible (employer ∧ state ∧ zip all present) ≈ **94.9% / 268.4 M rows**. | ✅ dense |
| 4 | **38-char truncation CONFIRMED live.** `max(length(employer))=38`, **0 rows > 38**, 1,738,948 rows (0.65% of filled) sit exactly at 38 — sliced mid-token (`CHEVRON UPSTREAM (A DIVISION OF CHEVRO`). Pure name_norm equality MISSES truncated multi-word entities → **prefix / base matching required.** | ⚠️ structural |
| 5 | **ZIP_CODE is majority ZIP+4.** 54.95% are 9-digit, 44.85% are 5-digit. Raw equality to MSHA `ZIP_CD` (5-digit) fails on the majority → **bridge MUST `left(zip_code,5)`.** | ⚠️ corrected |
| 6 | **STATE is clean USPS 2-letter** (99.88% `len=2`; 274 distinct incl. territories/APO/foreign) → joins MSHA `STATE_ABBR`/`ADDR_STATE` directly. | ✅ |
| 7 | **MSHA `ADDR_ZIP` does NOT exist** (directive assumption wrong). Mailing ZIP is **`ZIP_CD`** on `msha_mines`. Entity registries (`msha_corporate_history`, `msha_contractors`) are **geo-orphaned** → geo via `MINE_ID`/`CURRENT_*_ID` (operators) or `msha_enforcement_ledger` (contractors). | ⚠️ corrected |
| 8 | **No fuzzy-match primitive exists in the codebase.** grep for trigram/Levenshtein/Jaro/Jaccard/Damerau across all `.py`/`.sql` → **zero** implementations; `sam_fmcsa_domain_spine.py` states "No fuzzy matching, no probabilistic scoring." Fleet is **strictly deterministic** `core.name_norm` + `legal_name_base`. | ✅ deterministic |
| 9 | **name_norm collapses cardinality 25.8%** (8,353,661 → 6,199,428 distinct keys); `legal_name_base` suffix-peel reaches 27.1% (6,089,757). | ✅ |

---

## 1. Target schemas (live)

### FEC — `fec_individual_contributions` (282,923,196 rows)
21 source cols + `cycle_year`(int16) + `source_file` + `ingested_at`. Resolution-relevant:
`name`*·`city`·**`state`**†·**`zip_code`**·**`employer`**\*·**`occupation`**·`entity_tp`†·`cmte_id`\*·
`other_id`*·`transaction_amt`(dec14,2)·`transaction_dt`*·`sub_id`*·`cycle_year`†.
(`*`=BTREE, `†`=BITMAP.) **`employer` carries a BTREE — but on the RAW string**, so it does NOT
accelerate a `name_norm`'d join (the normalized key is a different value). All donor strings are
`VARCHAR` (no normalized column).

### MSHA — right side (live-confirmed; full treatment in the companion doc)
- **`msha_corporate_history`** (168,809) — `CONTROLLER_NAME`, `OPERATOR_NAME`, `CONTROLLER_TYPE`†
  (COMPANY 55.41% / PERSON 44.59%), `MINE_ID`*. **No geo.**
- **`msha_mines`** (91,803) — the **geo carrier**: `STATE_ABBR`, `ADDR_STATE`, **`ZIP_CD`**,
  `POSTAL_CD`, `CITY`, `STREET`; entity keys `CURRENT_CONTROLLER_ID/NAME`*, `CURRENT_OPERATOR_ID/NAME`*
  (State+ZIP **denormalized on the same row**). `ADDR_ZIP`/`ZIP` absent.
- **`msha_contractors`** (1,630,676) — `CONTRACTOR_NAME`, `CONTRACTOR_ID`*. **No geo** → mine-geo
  proxy only via `msha_enforcement_ledger` (`VIOLATOR_ID`/`VIOLATOR_NAME`/`VIOLATOR_TYPE_CD` + `MINE_ID`).

---

## 2. The precise projection — FEC left side (the string-normalization bridge)

The headline deliverable. `employer` is stored RAW, so the normalized blocking key is computed
inline from **`core.name_norm.name_norm('employer')`** (THE canonical fleet key — byte-identical to
`sos_normalized_master`, GLEIF, EPA). Geo is reshaped to MSHA's grain (`state2`, `zip5`).

```sql
-- ════ FEC donor → normalized employer entity key  (LEFT side; computed on the fly) ════
SELECT
    f.sub_id,                                     -- FEC txn id (BTREE) — provenance / dedup
    f.name                              AS donor_name,
    f.employer                          AS employer_raw,            -- ≤38 chars, truncated
    -- canonical core.name_norm: UPPER → &→' AND ' → [-–—]→space → strip [^A-Z0-9 ] → collapse ws.
    -- Import in the worker: `from core.name_norm import name_norm, legal_name_base`.
    nullif(trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
        upper(CAST(f.employer AS VARCHAR)),
        '&', ' AND ', 'g'), '[-\x{2013}\x{2014}]+', ' ', 'g'),
        '[^A-Z0-9 ]+', '', 'g'), '\s+', ' ', 'g')), '')  AS employer_norm,
    upper(f.state)                      AS state2,                  -- 2-letter USPS (99.88% len=2)
    left(f.zip_code, 5)                 AS zip5,                    -- TRUNCATE ZIP+4 → ZIP5 (54.95% are 9-digit)
    f.occupation, f.transaction_amt, f.transaction_dt, f.cycle_year, f.cmte_id
FROM fec_individual_contributions f
WHERE f.entity_tp = 'IND'                                          -- individuals (96.50%, BITMAP-free)
  AND f.employer  IS NOT NULL                                      -- drop 5.03% null employer
  AND f.state     IS NOT NULL                                      -- drop 0.12% null state
  -- FEC non-employer SENTINELS — self-report artifacts that never match MSHA; drop pre-block:
  AND name_norm(f.employer) NOT IN (
        'RETIRED','SELF EMPLOYED','SELF','NONE','NOT EMPLOYED','NA',
        'INFORMATION REQUESTED','HOMEMAKER','UNEMPLOYED','REQUESTED');
-- Pass-2 drift key (recovers "PACIFIC TRUCKING" vs "PACIFIC TRUCKING LLC"):
--   legal_name_base(name_norm('employer'))  →  peels trailing LLC|INC|CORP|CO|LTD|PLC
```

**Truncation mitigation (finding #4).** FEC `employer` is hard-capped at 38 chars and sliced
mid-token, so it is always the **shorter** side. Block exact on `(employer_norm, state2)` first;
for FEC rows at `length(employer)=38`, add a **prefix pass** — `msha_name_norm LIKE employer_norm || '%'`
within the same `state2` — to recover untruncated MSHA operator names whose head matches the slice.

---

## 3. SQL join path — attach geo to the MSHA entity, then bind

```sql
-- RIGHT side · operators/controllers → geo  (Path A: denormalized on msha_mines, NO join) --
WITH msha_entity AS (
  SELECT  CURRENT_CONTROLLER_ID                          AS entity_id,
          name_norm(any_value(CURRENT_CONTROLLER_NAME))  AS entity_norm,   -- core.name_norm, SAME macro
          mode(STATE_ABBR)                               AS state2,        -- modal mailing state
          array_agg(DISTINCT left(ZIP_CD, 5))            AS zip5_set       -- ZIP candidate set
  FROM    msha_mines
  WHERE   CURRENT_CONTROLLER_ID IS NOT NULL
  GROUP BY 1                                                               -- swap → CURRENT_OPERATOR_* for operators
)
-- BIND · deterministic inner join on the byte-identical blocking key + state, ZIP confirms --
SELECT  d.sub_id, d.donor_name, d.employer_raw, e.entity_id, e.entity_norm,
        d.transaction_amt, d.transaction_dt,
        (d.zip5 = ANY(e.zip5_set))                       AS zip_confirms
FROM    fec_left d                                                         -- §2 projection
JOIN    msha_entity e
       ON  e.entity_norm = d.employer_norm                                 -- Pass-1 exact
       AND e.state2      = d.state2;                                       -- State tiebreaker (live geo discriminant)
-- Contractors: NAME from msha_contractors.CONTRACTOR_NAME; geo only via msha_enforcement_ledger
-- (VIOLATOR_ID→CONTRACTOR_ID, MINE_ID→msha_mines). See companion doc §2 for the contractor proxy.
```

**Fan-out caveat** (from companion): one entity → N mines → N (state,ZIP) rows; bind on **modal
state**, treat **ZIP as a candidate set** (a FEC `zip5` hitting any member confirms the match).

---

## 4. Matching topology & data anomalies

| Axis | FEC (left) | MSHA (right) | Bridge action |
|---|---|---|---|
| Entity name | `employer` RAW, ≤38c, truncated, noisy | `*_NAME` mixed-case, untruncated | `name_norm` BOTH; prefix-pass for len=38 |
| Suffix drift | `LLC`/`INC`/`CORP` present | present | `legal_name_base` Pass-2 |
| State | `state` 2-letter USPS 99.88% | `STATE_ABBR`/`ADDR_STATE` 2-letter | exact `state2` join |
| ZIP | `zip_code` 45% ZIP5 / 55% ZIP+4 | `ZIP_CD` 5-digit | `left(zip_code,5)` ↔ `left(ZIP_CD,5)` |
| Match engine | — | — | **deterministic only — no fuzzy macro exists** |

- **Fuzzy matching (directive Q3): NONE in the codebase.** The fleet resolves on the deterministic
  `core.name_norm` blocking key + `legal_name_base` suffix peel — no trigram/Levenshtein/Jaro/Jaccard
  macro anywhere. DuckDB ships `jaro_winkler_similarity`/`levenshtein`/`jaccard` as native scalars
  **if** a fuzzy fallback is ever authorized, but that would be net-new and unprecedented here; v1 is
  deterministic Name+State+ZIP5.
- **Non-employer sentinels.** Self-reported free text yields junk employers (`RETIRED`,
  `SELF-EMPLOYED`, `NONE`, `INFORMATION REQUESTED`, and literal noise — observed
  `THIS IS A PERSONAL DONATION AND HAS NO…`). These never match MSHA and inflate the distinct space;
  the §2 stop-list drops them pre-block.
- **Truncation collisions.** Three distinct 38-char Chevron variants
  (`CHEVRON TECHNICAL CENTER (A CHEVRON U.`, `CHEVRON UPSTREAM (A DIVISION OF CHEVRO`,
  `CHEVRON U.S.A. INC. - CORPORATE AFFAIR`) all denote one parent — name_norm + base + prefix-pass,
  not raw equality, recover them.

---

## 5. Appendix — live evidence (probe 2026-06-05)

- **Fill / null (exact, pushdown):** name 99.98% · city 99.92% · **state 99.88% (332,845 null)** ·
  **zip_code 99.82% (497,749 null)** · **employer 94.97% (14,217,620 null)** · occupation 92.66% ·
  `entity_tp='IND'` 96.50% (273,008,463). Co-fill employer∧state 94.93%, employer∧zip 94.88%.
- **Cardinality / truncation (full-set scan):** employer DISTINCT raw ≈8,353,661 → name_norm
  ≈6,199,428 (−25.8%) → legal_name_base ≈6,089,757 (−27.1%). `length(employer)` max=38 / avg=12.2;
  **len==38: 1,738,948 (0.65%); len>38: 0.**
- **Geo shape:** state distinct=274, len==2 = 99.88%. zip_code len5=126,880,137 (44.85%) /
  len9=155,454,089 (54.95%) / other=91,221 / non-numeric=55,778.
- **Samples (cycle 2026):** `FENWICK & WEST LLP`→`FENWICK AND WEST LLP`;
  `AMERICAN AIRLINES, INC.`→`AMERICAN AIRLINES INC`; `TD BANK N.A.`→`TD BANK NA`;
  `CHEVRON U.S.A. INC. - CORPORATE AFFAIR`→`CHEVRON USA INC CORPORATE AFFAIR`. Geo: `TX|77389`
  beside `TX|773897885` (5- vs 9-digit confirmed).
- **Harness:** `/tmp/fec_msha_probe_meta.py` (schema/count/indices) + `/tmp/fec_bridge_probe_agg.py`
  (pushdown fills + full-set HLL distinct + bounded 2026 sample). Read-only; pylance 7.0.0 /
  duckdb 1.5.x / pyarrow ≥17; ephemeral `uv` venv; Doppler `core-x/prd`. **Zero mutation.**
