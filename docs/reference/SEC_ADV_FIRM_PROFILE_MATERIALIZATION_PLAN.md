# SEC ADV Firm-Profile Materialization Plan

**Target:** `sec_adv_firm_profile` — a typed, fully-indexed RIA firm-profile serving dataset
recovered from the 252-column Form ADV Part 1A base record currently locked inside
`sec_adv_part1.raw_filing` (JSON, all-varchar).

**Status:** spec — execution-ready. One build cycle, fully described below.

---

## 0. Problem & objective

`sec_adv_part1` (the RIA registry, `s3://data-sink/active/sec_adv_part1/`, 36,846 firms) preserves
the **entire** Form ADV Part 1A base row losslessly in a `raw_filing` JSON column, but the typed
spine projects only **two** firm-economics fields (`regulatory_aum`, `large_fund_adviser_flag`).
Every other dimension an operator wants to segment on — employee headcount, discretionary vs
non-discretionary AUM, account counts, client-type/asset-base mix, advisory-activity specialties,
compensation model, private-fund activity, disciplinary history — exists in the data but is
**recoverable only by full-scan JSON extraction**, with zero index pushdown.

**Objective:** materialize a derived Lance dataset, **1 row per CRD**, that promotes the
high-value Item 1/5/6/7/9/11 fields to **typed, BTREE/BITMAP-indexed columns**, so the entire
RIA universe is filterable at query speed and joinable against the broader corporate spine and
GTM datasets.

**Scale framing (honest):** the *build* is trivial-scale — 36,846 rows of JSON extraction fits
in RAM and completes in well under a minute on a single worker. "At scale" here is a **query**
property, not a build property: it is delivered entirely by the **scalar-index plan** (§5), which
makes range predicates (AUM, headcount) and categorical/boolean predicates (client type, specialty,
fee model) index-accelerated across the firm universe and any downstream JOIN. No out-of-core
machinery is warranted for the build; spend the engineering on the index plan and the field
dictionary, which are the load-bearing artifacts.

---

## 1. Source contract (read side)

| Property | Value |
|---|---|
| Source dataset | `s3://data-sink/active/sec_adv_part1/` (Lance v2.1) |
| Grain | 1 row / `crd_number` (source already deduped to latest filing) |
| Rows | 36,846 total — **IA 26,963**, **ERA 9,883** |
| Payload | `raw_filing` = `to_json(raw)` over the full base CSV row; keys are SEC item codes (`5A`, `5F2c`, `5D1b`, `5G7`, `11A1`, …) |
| Typed passthroughs (no JSON needed) | `crd_number, sec_number, lei, legal_name, primary_business_name, business_address_* , large_fund_adviser_flag, regulatory_aum, date_submitted, form_version, filer_type, snapshot_date` |
| Read idiom | `con.register("adv", lance.dataset(SRC_URI, storage_options=so))` then `json_extract(raw_filing, '$."5F2a"')` |

**Schema heterogeneity gate:** IA base ≈ 243 cols, ERA base ≈ 111 cols. IA-only fields (Item 5
economics) are **absent** from the ERA payload. Every extraction must `TRY_CAST(nullif(...))` so an
absent key resolves to typed **NULL** (not 0) — preserving the "not reported" vs "reported zero"
distinction. ERA rows therefore carry NULL across the Item-5 economics block by design.

---

## 2. Target dataset (write side)

| Property | Value |
|---|---|
| Dataset | `sec_adv_firm_profile` |
| URI | `s3://data-sink/active/sec_adv_firm_profile/` (env-overridable `SEC_ADV_FIRM_PROFILE_URI`) |
| Format | Lance `data_storage_version="2.1"` |
| Grain | **1 row / `crd_number`** (inherits source grain) |
| Write mode | **snapshot-overwrite** — derived, rebuildable, a pure function of the source snapshot |
| Module | `pipelines/serving/materialize_sec_adv_firm_profile.py` |
| Ledger | `ops.sec_adv_firm_profile_runs` (DDL: `pipelines/serving/ops_sec_adv_firm_profile_runs.sql`) |

Snapshot-overwrite is the correct semantic: the source is itself a monthly full-overwrite snapshot,
so the derived view is a deterministic re-projection — re-running yields byte-identical logical
output. No incremental merge, no append ledger dedup needed; idempotency is structural.

`raw_filing` is **not** duplicated into the profile (it stays addressable in the source by
`crd_number`) — keeps the serving dataset lean and the BITMAP scans dense.

---

## 3. Field dictionary — item code → typed column

The load-bearing spec. Codified as an ordered field plan in the module (mirrors the
`PART1_FIELDS` idiom in `pipelines/sec_adv/ingest.py`). Fill % = observed IA population.

### 3.1 Identity & registration (Item 1 — typed passthrough + selected JSON)
| Column | Source | Type | Notes |
|---|---|---|---|
| `crd_number` | typed | str | resolution PK |
| `lei` | typed (`1P`) | str | GLEIF; only ~15% populated |
| `sec_number` | typed (`1D`) | str | 801-/802- |
| `legal_name`, `primary_business_name` | typed (`1A`,`1B1`) | str | |
| `business_address_city/state/country/postal` | typed | str | |
| `website` | `1I` | str | firm web presence (100%) |
| `phone` | `1F3` | str | principal office phone |
| `fiscal_year_end` | `1M` | str | |
| `native_cik` | `1N-CIK` | str | **0% populated** — see §13 |

### 3.2 Size / headcount (Item 5A–5B)
| Column | Code | Fill | Type |
|---|---|---|---|
| `total_employees` | `5A` | 100% | bigint |
| `advisory_employees` | `5B1` | 100% | bigint |
| `registered_reps` | `5B2` | 100% | bigint |
| `iar_count` | `5B3` | 100% | bigint |
| `insurance_agents` | `5B4` | 100% | bigint |

### 3.3 AUM & accounts (Item 5F2)
| Column | Code | Fill | Type |
|---|---|---|---|
| `discretionary_aum` | `5F2a` | 94% | bigint |
| `non_discretionary_aum` | `5F2b` | 94% | bigint |
| `total_regulatory_aum` | `5F2c` | 94% | bigint (reconcile vs typed `regulatory_aum`) |
| `discretionary_accounts` | `5F2d` | 94% | bigint |
| `non_discretionary_accounts` | `5F2e` | 94% | bigint |
| `total_accounts` | `5F2f` | 94% | bigint |
| `num_clients` | `5C1` | 100% | bigint |

### 3.4 Clientele / asset base (Item 5D — 14 client types a–n)
For each type emit `n_clients_<t>` (`5D1{x}`) + `aum_<t>` (`5D3{x}`) + boolean `serves_<t>`.
(Range bucket `5D2{x}` is dropped — `5D1`/`5D3` are the analytic fields.)

| Suffix | `<t>` | Label |
|---|---|---|
| a | `individuals` | Individuals (non-HNW) |
| b | `hnw_individuals` | High-net-worth individuals |
| c | `banks` | Banking/thrift institutions |
| d | `investment_companies` | Registered investment companies |
| e | `bdc` | Business development companies |
| f | `pooled_vehicles` | Pooled vehicles (ex-RIC/BDC) |
| g | `pension` | Pension & profit-sharing plans |
| h | `charities` | Charitable organizations |
| i | `state_muni` | State/municipal government |
| j | `other_advisers` | Other investment advisers |
| k | `insurance_co` | Insurance companies |
| l | `sovereign_wealth` | Sovereign wealth / foreign official |
| m | `corporations` | Corporations / other businesses |
| n | `other` | Other |

### 3.5 Specialties / advisory activities (Item 5G — booleans)
`act_financial_planning` 5G1 · `act_pm_individuals` 5G2 · `act_pm_investment_companies` 5G3 ·
`act_pm_pooled` 5G4 · `act_pm_institutional` 5G5 · `act_pension_consulting` 5G6 ·
`act_adviser_selection` 5G7 · `act_newsletters` 5G8 · `act_ratings` 5G9 · `act_market_timing` 5G10 ·
`act_seminars` 5G11 · `act_other` 5G12. (All 100% fill.)

### 3.6 Compensation model (Item 5E — booleans)
`comp_pct_aum` 5E1 · `comp_hourly` 5E2 · `comp_subscription` 5E3 · `comp_fixed` 5E4 ·
`comp_commissions` 5E5 · `comp_performance` 5E6 · `comp_other` 5E7. (All 100% fill.)

### 3.7 Business model & risk
| Column | Source | Type |
|---|---|---|
| `advises_private_funds` | `7B` (Y) | bool |
| `has_wrap_program` | `5H` > 0 | bool |
| `has_smas` | `5I1` (Y) | bool |
| `has_custody` | `9A1a`/`9B1a` (Y) | bool |
| `is_broker_dealer` | `6A1`/`5B2`>0 | bool |
| `any_disciplinary` | OR over all `11*` (Y) | bool |

---

## 4. Derived enrichments (computed in-SQL, indexed)

- **`aum_band`** (bitmap) from `total_regulatory_aum`: `lt_25m, 25m_100m, 100m_500m, 500m_1b, 1b_10b, 10b_50b, gte_50b, unreported`.
- **`employee_band`** (bitmap) from `total_employees`: `1_5, 6_10, 11_50, 51_250, gt_250`.
- **`pct_discretionary`** = `discretionary_aum / nullif(total_regulatory_aum,0)` (numeric).
- **`serves_<t>`** = `coalesce(n_clients_<t>,0) > 0 OR coalesce(aum_<t>,0) > 0` (14 bitmaps).
- **`primary_client_type`** (bitmap) = `argmax(aum_<t>)` across the 14 types — single-label segment key.
- **`any_disciplinary`** (bitmap) = OR of every Item-11 flag = 'Y' — high-value clean/flagged filter.
- **`is_ria` / `is_era`** from `filer_type`.

All boolean derivations use `upper(trim(x)) = 'Y'`; all numerics `TRY_CAST(nullif(trim(x),'') AS BIGINT)`.

---

## 5. Index plan — the "queryable at scale" payload

`create_scalar_index` per the serving idiom (BTREE high-cardinality / numeric-range;
BITMAP low-cardinality categorical + every boolean). Index miss = warn, never fatal.

**BTREE** (resolution keys + numeric range pushdown):
```
crd_number, lei, total_regulatory_aum, discretionary_aum, non_discretionary_aum,
total_employees, advisory_employees, num_clients, total_accounts
```

**BITMAP** (categorical + ~45 booleans):
```
filer_type, business_address_state, aum_band, employee_band, primary_client_type,
large_fund_adviser_flag,
serves_{individuals,hnw_individuals,banks,investment_companies,bdc,pooled_vehicles,
        pension,charities,state_muni,other_advisers,insurance_co,sovereign_wealth,
        corporations,other},
act_{financial_planning,pm_individuals,pm_investment_companies,pm_pooled,pm_institutional,
     pension_consulting,adviser_selection,newsletters,ratings,market_timing,seminars,other},
comp_{pct_aum,hourly,subscription,fixed,commissions,performance,other},
advises_private_funds, has_wrap_program, has_smas, has_custody, any_disciplinary
```

This is what converts the operator's segmentation questions into index scans, e.g.:
> *RIAs with > $1B discretionary AUM, serving pension plans, charging performance fees, clean disciplinary record*
```sql
SELECT crd_number, legal_name, discretionary_aum
FROM sec_adv_firm_profile
WHERE discretionary_aum > 1e9      -- BTREE range
  AND serves_pension               -- BITMAP
  AND comp_performance             -- BITMAP
  AND NOT any_disciplinary;        -- BITMAP
```

---

## 6. Cycle execution (one build, phase by phase)

| Phase | Action | Guard |
|---|---|---|
| **A. Precondition** | Read `ops.sec_adv_runs` latest `part1` row; capture source `snapshot_date` + `rows_written`. | Abort if absent or `status<>'success'`. |
| **B. Extract+transform** | `register` source Lance; run the `json_extract` typed-projection SQL → Arrow table. | DuckDB does 100% of the work. |
| **C. Pre-write gates** | grain, floor, reconciliation (§9). | Hard `assert` — abort before any write. |
| **D. Write** | `lance.write_dataset(tbl, URI, mode="overwrite", data_storage_version="2.1")`. | |
| **E. Index** | BTREE then BITMAP, `replace=True` (idempotent). | Miss → warn. |
| **F. Post-write integrity** | `ds.count_rows() == rows`; `list_indices()` ⊇ planned set. | Hard `assert`. |
| **G. Ledger** | `_record_run` terminal row → `ops.sec_adv_firm_profile_runs`. | Best-effort; never masks the build. |
| **H. Callback** (scheduled mode) | POST flat-JSON terminal body to the Trigger waitpoint url. | |

Local invocation:
```bash
doppler run --project core-x --config prd -- python pipelines/serving/materialize_sec_adv_firm_profile.py
doppler run --project core-x --config prd -- python pipelines/serving/materialize_sec_adv_firm_profile.py --verify
```

---

## 7. Idempotency, ledger, blast-radius

- **Idempotent by construction:** overwrite of a pure projection — re-run = same logical output.
- **Ledger:** one row per run in `ops.sec_adv_firm_profile_runs` (feed, rows_written, fill metrics,
  indices_built, status, error, timings) — mirrors `ops.company_map_serving_runs`.
- **Blast-radius containment:** the materializer is a **separate** module, **separate** Lance
  dataset, and **separate** ledger from the source ingest. A projection bug can only corrupt the
  derived view; `sec_adv_part1` (the SoR) is read-only here and untouched. Rebuild is one command.

---

## 8. Resource configuration

```python
con.execute("PRAGMA threads=4")
con.execute("SET memory_limit='4GB'")          # 36k rows — generous headroom
con.execute("SET temp_directory='/tmp/_adv_spill'")
# index build: env LANCE_BYPASS_SPILLING=true (in-memory scalar-index sort; cheap at this scale)
```
Modal sizing (scheduled mode): `memory=4096, cpu=2.0, timeout=600`. The build is I/O-light
(one Lance read, one Lance write, ~54 small index builds).

---

## 9. Verification harness (definition of done)

Encoded in `verify()` + the pre-write gates. A cycle is **done** only when all pass:

1. **Grain:** `count(*) == count(DISTINCT crd_number)`.
2. **Completeness:** `rows == source row count` (≈ 36,846); IA subset ≈ 26,963.
3. **Fill-rate sanity (IA):** `total_employees` ~100%, `discretionary_aum` ~94%, `act_*`/`comp_*` ~100% — matches the source probe.
4. **AUM reconciliation:** `sum(total_regulatory_aum)` within 0.1% of `sum(regulatory_aum)` in source (the `5F2c` ↔ typed-spine check); per-row `discretionary_aum + non_discretionary_aum ≈ total_regulatory_aum` for ≥ 99% of IA rows.
5. **Spot-check (golden row) — MetLife Investment Management, LLC:**
   `discretionary_aum=496,466,586,170 · total_employees=925 · serves_insurance_co=TRUE · aum_insurance_co=412,729,719,207 · act_pm_institutional=TRUE · comp_performance=TRUE · advises_private_funds=TRUE`.
6. **Index presence:** every column in §5 returns an index from `list_indices()`.
7. **Pushdown proof:** `EXPLAIN` of the §5 example query shows scalar-index/`ScalarIndexQuery` nodes, not a full scan.

---

## 10. Control plane (cadence)

The profile is a deterministic function of `sec_adv_part1`, so it must rebuild **after** each monthly
source refresh. Chain it into the existing monthly task rather than a free-running schedule:

- Extend `src/trigger/sec_adv_monthly.ts`: after the `part1` child reports `success`, dispatch a
  third child — the `materialize_sec_adv_firm_profile` Modal function — under its own waitpoint
  token, and fold its terminal callback into the run summary. ADV-W is unaffected.
- Modal: add `materialize_sec_adv_firm_profile` as a function in the `sec-adv-pipelines` app (or a
  new `sec-adv-serving` app), spawned by the Universal Dispatcher, writing its own ledger row +
  callback exactly like `ingest_dataset`.
- Ordering guarantee: the MV build only fires on `part1` success, so it can never project a
  half-written source snapshot.

---

## 11. Edge cases & data-quality gates

- **ERA filers (9,883):** Item-5 economics absent → typed NULL across that block (gate #3 asserts
  the IA/ERA split, not a global fill floor).
- **"Not reported" vs "zero":** `TRY_CAST(nullif(trim(x),''))` everywhere — absent ⇒ NULL, never 0.
- **Key spellings with spaces/suffixes:** `'5D1n Other'`, `'5E7-Other'`, `'1F1-State'`, `'1N-CIK'`
  — extracted with exact JSON-path quoting; resolved against the live 252-key union.
- **5D sparsity (24–80%):** expected; `serves_<t>` derives FALSE on absent, not NULL.
- **AUM outliers:** mega-custodians can report > $1T RAUM; columns are `bigint` (no overflow);
  `aum_band` top bucket is `gte_50b`.
- **Boolean normalization:** `upper(trim()) = 'Y'`; blanks ⇒ FALSE.
- **LEI (~15%) / native CIK (0%):** kept nullable; never gated on.

---

## 12. Deliverables (files this cycle ships)

1. `docs/reference/SEC_ADV_FIRM_PROFILE_MATERIALIZATION_PLAN.md` — this spec.
2. `pipelines/serving/ops_sec_adv_firm_profile_runs.sql` — ledger DDL (idempotent).
3. `pipelines/serving/materialize_sec_adv_firm_profile.py` — worker: `_build_sql()` (json_extract
   projection + derived bands/flags), `build()` (gates → overwrite → index → integrity → ledger),
   `verify()`, dual local/Modal entrypoints.
4. `src/trigger/sec_adv_monthly.ts` — extended to chain the MV build after `part1`.

---

## 13. Follow-on (explicitly out of this cycle)

- **CRD↔CIK↔LEI corporate bridge.** The native ADV CIK field (`1N-CIK`) is **0% populated** and LEI
  only ~15% — so an RIA↔EDGAR join **cannot** be lifted from ADV. It requires an external
  entity-resolution build (name + address + LEI-where-present matching) against `edgar_cik_map`.
  Tracked in `FININST_REGISTRY_CANONICAL_DESIGNATION_RECON.md`; a separate materialization.
- **ADV Part 2 brochure (narrative).** Investment philosophy, strategy prose, written fee schedules,
  **account minimums**, qualitative "stated preferences" — a separate filing, not in the Part 1 bulk
  base file. Distinct ingest (different source + extraction).
- **Schedule D child tables.** Per-private-fund detail (`7B.1`), branch offices, direct/indirect
  owners, control persons — live in the `adv-filing-data-*-part2.zip` Schedule-D bundle the current
  resolver deliberately excludes. Distinct ingest if/when fund-level granularity is needed.
