# SEC ADV Firm-Profile Materialization Plan

**Target:** `sec_adv_firm_profile` — a typed, fully-indexed RIA firm-profile serving dataset
recovered from the 252-column Form ADV Part 1A base record currently locked inside
`sec_adv_part1.raw_filing` (JSON, all-varchar).

**Status:** spec — execution-ready, **v2 (adversarial-review-hardened)**. Every field-level
derivation and verification gate below was empirically validated against the live source; see the
**Revision log** at the end for what changed from v1 and why.

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
| `has_website` | `1I` | bool | `1I` is the Y/N "do you have a website" flag (100%), NOT a URL (URLs live in Schedule D, not ingested) |
| `phone` | `1F3` | str | principal-office phone (verified: distinct phone strings) — not indexed |

> Execution-time correction: v2 §3.1 listed `website`(str), `fiscal_year_end`(1M) and `native_cik`(1N-CIK).
> Live sampling showed `1I` is Y/N (→ `has_website` bool), `1M` is an unrelated Y/N (dropped), and
> `1N-CIK` is 0% populated (dropped — see §13).

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
For each type emit `n_clients_<t>` (`5D1{x}`, count) + `aum_<t>` (`5D3{x}`, RAUM) + boolean `serves_<t>`.
`5D2{x}` is the SEC's own **Y/N "do you serve this client type" checkbox** (present for suffixes
a,b,c,g–n; absent for d,e,f) — it is the cleanest `serves_<t>` source and **drives the boolean** (§4),
not a range bucket and not dropped.

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
| `has_wrap_program` | `5H` non-null & ≠ `'0'` — **5H is a range-bucket STRING** (`'0'`,`'1-10'`,…,`'More than 500'`), never numeric | bool |
| `has_smas` | `5I1` (Y) | bool |
| `has_custody` | `9A1a`/`9B1a` (Y) | bool |
| `is_broker_dealer` | `6A1`='Y' OR `registered_reps`>0 — **6A1 is Y/N, not a count** (reuse the typed `registered_reps`) | bool |
| `any_disciplinary` | OR over all `11*` (Y) | bool |

### 3.8 Provenance (carried on every row; not indexed)
| Column | Source | Notes |
|---|---|---|
| `source_snapshot_date` | source `snapshot_date` | which ADV snapshot this profile was projected from |
| `built_at` | `now()` | materialization timestamp — self-describing lineage for downstream JOINs |

---

## 4. Derived enrichments (computed in-SQL, indexed)

- **`aum_band`** (bitmap) from `total_regulatory_aum`, **NULL-led, half-open intervals**:
  `'unreported'` (NULL), `'0'` (reported-zero — kept distinct from unreported, same not-reported-vs-zero
  principle), then `lt_25m, 25m_100m, 100m_500m, 500m_1b, 1b_10b, 10b_50b, gte_50b` (each `>= lo AND < hi`).
- **`employee_band` / `client_count_band`** (bitmaps), **NULL-led, MIXED-MODE**: Items 5A/5C1 are
  reported as an exact integer by large filers but as an SEC **range-bucket string** by small filers
  (`'1-5'`, `'251-500'`, `'More than 1000'`). The band absorbs BOTH into one vocabulary, so its labels
  **MUST equal the raw SEC bucket strings verbatim** (underscore labels like `1_5` would split each
  bucket in two — silently wrong segmentation). `employee_band ∈ {unreported, 0, 1-5, 6-10, 11-50,
  51-250, 251-500, 501-1000, more_than_1000}`; `client_count_band ∈ {unreported, 0, 1-10, 11-25,
  26-100, 101-250, 251-500, 501-1000, more_than_1000}`. Exact-int filers are bucketed by the matching
  SEC boundaries; range-string filers pass through (normalizing only `'More than 1000' → more_than_1000`).
  The exact integer is also kept in `total_employees`/`num_clients` (NULL where the filer used a bucket,
  ~93% of IA); the band is ~100% populated.
- **`pct_discretionary`** = `discretionary_aum / nullif(total_regulatory_aum,0)` (numeric).
- **`serves_<t>`** (14 bitmaps) — **prefer the SEC checkbox where it exists**:
  a,b,c,g–n → `upper(trim("5D2{x}"))='Y' OR coalesce(n_clients_<t>,0)>0 OR coalesce(aum_<t>,0)>0`;
  d,e,f (no `5D2`) → `coalesce(n_clients_<t>,0)>0 OR coalesce(aum_<t>,0)>0`.
- **`primary_client_type`** (bitmap) — single-label segment key. **NOT `argmax`** (a row aggregate,
  invalid across columns). Compute horizontally: `greatest(coalesce(aum_a,0),…,coalesce(aum_n,0))`
  + an **ordered CASE cascade** (fixed priority list ⇒ deterministic tie-break), emitting a real
  `'none'` value when the max is 0 — **≈51% of IA rows have all-zero 5D3**, so `'none'` keeps them
  queryable rather than NULL. Document the priority order in the field dictionary.
- **`economics_reported`** (bitmap) = `total_regulatory_aum IS NOT NULL` — separates IA-reported from
  ERA-exempt/unreported at query time (the entire ERA cohort + ~110 IA carry NULL economics).
- **`any_disciplinary`** (bitmap) = OR of every Item-11 flag = 'Y' — high-value clean/flagged filter.
- **`is_ria` / `is_era`** (bitmaps) from `filer_type` — `is_era` indexed so the 9,883-row exempt
  cohort is a one-predicate filter, not a string scan.

All boolean derivations use `upper(trim(x)) = 'Y'` (tokens verified clean `Y`/`N`/JSON-null ⇒
blanks/NULL ⇒ FALSE); all numerics `TRY_CAST(nullif(trim(x),'') AS BIGINT)`.

---

## 5. Index plan — the "queryable at scale" payload

`create_scalar_index` per the serving idiom (BTREE high-cardinality / numeric-range;
BITMAP low-cardinality categorical + every boolean). Index miss = warn, never fatal.

**BTREE** (resolution keys + numeric range pushdown):
```
crd_number, lei, total_regulatory_aum, discretionary_aum, non_discretionary_aum,
total_employees, advisory_employees, num_clients, total_accounts
```

**BITMAP** (categorical + ~48 booleans):
```
filer_type, is_era, economics_reported, business_address_state, aum_band, employee_band,
primary_client_type, large_fund_adviser_flag,
serves_{individuals,hnw_individuals,banks,investment_companies,bdc,pooled_vehicles,
        pension,charities,state_muni,other_advisers,insurance_co,sovereign_wealth,
        corporations,other},
act_{financial_planning,pm_individuals,pm_investment_companies,pm_pooled,pm_institutional,
     pension_consulting,adviser_selection,newsletters,ratings,market_timing,seminars,other},
comp_{pct_aum,hourly,subscription,fixed,commissions,performance,other},
advises_private_funds, has_wrap_program, has_smas, has_custody, any_disciplinary
```
`lei` is BTREE for parity with the source ingest (equality-only at this scale — immaterial; kept, not forced).

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
| **C. Pre-write gates** | grain (1/CRD); **per-population floor** (IA ≥ 25k AND total ≥ 34k); within-row AUM identity; non-degenerate-column checks (§9). | Hard `assert` — abort before any write. |
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

1. **Grain:** `count(*) == count(DISTINCT crd_number)`, 0 null CRDs.
2. **Completeness (per-population floor):** `rows == source row count`; **`ia_rows > 25_000` AND
   `total_rows > 34_000`** (tied to the 26,963 IA / 36,846 total baseline). A single global `>30000`
   floor is wrong — IA-only is 26,963 (< 30k), so it both false-fails an ERA regression and false-passes
   a partial IA load hidden behind ERA rows.
3. **Fill-rate sanity (IA), with tolerance:** `total_employees ≥ 99%` (≈110 IA nulls — not strict 100%),
   `discretionary_aum ≥ 93%`, `act_*`/`comp_*` ≥ 99% — matches the source probe.
4. **AUM identity (the real projection check):** `count(*) FILTER (WHERE disc+nondisc <> total) == 0`
   over IA rows with all three present (verified 100% exact). This — not source-equality — is what
   catches a broken AUM projection. The `5F2c ↔ regulatory_aum` byte-equality is a **tautology** (both
   derive from the same extraction); keep it only as a labeled *passthrough-integrity* check, never as
   projection validation.
5. **Non-degenerate columns (anti-silent-wrong):** assert each derived family has a plausible TRUE rate,
   so a wrong key-path / mis-typed banded-string field that yields an **all-FALSE** column fails the
   build — e.g. `comp_pct_aum` TRUE > 50% of IA, `has_wrap_program` TRUE > 0, `serves_hnw_individuals`
   TRUE > 0, `act_pm_institutional` TRUE > 0, `any_disciplinary` TRUE > 0. A uniformly FALSE/NULL column
   is a failure.
6. **Spot-check (golden row) — MetLife Investment Management, LLC:**
   `discretionary_aum=496,466,586,170 · total_employees=925 · serves_insurance_co=TRUE · aum_insurance_co=412,729,719,207 · act_pm_institutional=TRUE · comp_performance=TRUE · advises_private_funds=TRUE`.
7. **Index presence:** every column in §5 returns an index from `list_indices()`.
8. **Pushdown proof (via the Lance planner, not DuckDB):** DuckDB `EXPLAIN` only shows `ARROW_SCAN`
   with an opaque pushed-down filter — it never surfaces Lance's index node, so a DuckDB-EXPLAIN gate
   false-fails on a correctly-indexed dataset. Assert on the Lance scanner instead:
   `ds.scanner(filter="discretionary_aum > 1e9 AND comp_performance AND advises_private_funds",
   columns=["crd_number"]).explain_plan(True)` contains `ScalarIndexQuery` (or `MaterializeIndex`).

---

## 10. Control plane (cadence)

The profile is a deterministic function of `sec_adv_part1`, so it must rebuild **after** each monthly
source refresh. Chain it into the existing monthly task rather than a free-running schedule:

- Extend `src/trigger/sec_adv_monthly.ts`: after the `part1` child reports `success`, dispatch a
  third child — the `materialize_sec_adv_firm_profile` Modal function — under its own waitpoint token.
  **Failure isolation:** an MV-build failure must NOT fail the monthly run's terminal status — the
  `part1`/`advw` SoR writes are already committed and untouched. Record an MV failure as a
  degraded/partial outcome in the run summary (and page), never as a hard run failure or rollback.
  ADV-W is unaffected.
- Modal: add `materialize_sec_adv_firm_profile` as a function in the `sec-adv-pipelines` app (or a
  new `sec-adv-serving` app), spawned by the Universal Dispatcher, writing its own ledger row +
  callback exactly like `ingest_dataset`.
- Ordering guarantee: the MV build only fires on `part1` success, so it can never project a
  half-written source snapshot.

---

## 11. Edge cases & data-quality gates

- **ERA filers (9,883):** Item-5 economics absent → typed NULL across that block. Surfaced at query
  time via `economics_reported` / `is_era`; the floor gate is **per-population** (IA vs total), not a
  single global number.
- **"Not reported" vs "zero":** `TRY_CAST(nullif(trim(x),''))` everywhere — absent ⇒ NULL, never 0.
- **Banded-string fields are NOT numeric:** `5H` (wrap) is a range bucket (`'0'`,`'1-10'`,…,
  `'More than 500'`) → boolean via non-null & ≠`'0'`, never `> 0`. `5D2{x}` is a Y/N checkbox, not a
  count. `6A1` is Y/N, not a count. Mis-typing any of these silently yields an all-FALSE column —
  caught by gate #5.
- **Key spellings with spaces/suffixes:** `'5D1n Other'`, `'5E7-Other'`, `'1F1-State'`, `'1N-CIK'`
  — extracted with exact JSON-path quoting (`json_extract(raw_filing, '$."5D1n Other"')`); verified
  working against the live 252-key union.
- **5D sparsity (24–80%):** expected; `serves_<t>` derives FALSE on absent, not NULL.
- **AUM outliers:** mega-custodians can report > $1T RAUM; columns are `bigint` (no overflow);
  `aum_band` top bucket is `gte_50b`.
- **Boolean normalization:** `upper(trim()) = 'Y'`; tokens verified clean `Y`/`N`/null; blanks ⇒ FALSE.
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

---

## Revision log

**v2 — adversarial-review-hardened.** An independent Opus 4.8 review re-probed the live source and
found six P0 correctness traps + four worthwhile improvements; all confirmed empirically and folded in.

| # | Sev | Was (v1) | Now (v2) | Why |
|---|---|---|---|---|
| 1 | P0 | `has_wrap_program = 5H > 0` | non-null & ≠`'0'` | `5H` is a range-bucket **string** (`'0'`…`'More than 500'`); `>0` collapses every banded firm to FALSE |
| 2 | P0 | `primary_client_type = argmax(aum_<t>)` | `greatest()` + ordered CASE cascade + `'none'` sentinel | `argmax` is a row aggregate (won't compile across columns); ~51% of IA have all-zero 5D3 → was undefined |
| 3 | P0 | `is_broker_dealer = 6A1/5B2>0` | `6A1='Y' OR registered_reps>0` | `6A1` is Y/N, not a count |
| 4 | P0 | floor `rows > 30000` | per-population: IA > 25k AND total > 34k | IA-only is 26,963 (< 30k) — global floor false-fails/false-passes |
| 5 | P0 | verify: DuckDB `EXPLAIN` shows `ScalarIndexQuery` | Lance `scanner(...).explain_plan(True)` | DuckDB only shows `ARROW_SCAN`; Lance's index node never surfaces there → gate could never pass |
| 6 | P0 | verify: `sum(5F2c)` vs `sum(regulatory_aum)` | within-row `disc+nondisc=total` identity | source-equality is a tautology (byte-identical) — validated nothing; identity is the real check |
| 7 | P1 | — | `economics_reported` + indexed `is_era` | separate IA-reported from ERA-exempt NULLs at query time |
| 8 | P1 | — | `source_snapshot_date` + `built_at` columns | self-describing lineage / snapshot alignment for JOINs |
| 9 | P1 | bands silent on NULL/0 | NULL-led, half-open `aum_band`/`employee_band` (+`unreported`,`0`) | avoid silent bucket fall-through |
| 10 | P1 | MV failure folded into monthly run status | MV failure non-fatal (degraded + page) | a derived-view bug must not fail the already-committed SoR ingest |
| 11 | P2 | `5D2` called a "range bucket", dropped | `5D2` is the Y/N **serves-checkbox**; now the primary `serves_<t>` source | factual correction; salvages the cleanest signal |
| 12 | gate | — | non-degenerate-column gate (#5) | catches an all-FALSE column from a wrong key-path / mis-typed banded field |

Empirically verified during review: grain (36,846 = distinct, 0 null); `5F2a+5F2b=5F2c` 100% exact;
AUM numbers parse clean (0 commas/decimals/negatives); tokens are clean `Y`/`N`/null; the 54-index
build runs in <1s; JSON-path quoting works for all space/suffix keys; MetLife golden row exact.

**v2.1 — execution-hardened** (discovered while implementing + a second adversarial code-review pass):

| Sev | Change | Why |
|---|---|---|
| P0 | `5A`/`5B1`/`5C1` are **mixed-mode** (exact int OR SEC range-bucket string) | a pure-bigint `total_employees` is only ~93% filled (would fail fill-gate); added `employee_band`/`client_count_band` that absorb both modes |
| P0 | band labels = **raw SEC bucket strings** (hyphenated, e.g. `1-5`,`more_than_1000`) | underscore labels would split each bucket across the int- and string-derived paths |
| P1 | `1I` → `has_website` bool (not URL); dropped `fiscal_year_end`(1M, unrelated Y/N) + `native_cik`(0%) | v2 §3.1 mislabeled these; live sampling corrected them |
| P1 | non-degenerate gate numerator IA-scoped (`FILTER(WHERE col AND is_ria)`) | cross-population numerator read >100% and could mask an all-FALSE IA column |
| P1 | `verify()` now **enforces** (accumulates failures, raises) | v2 verify only printed → a corrupt build returned exit 0; §9 calls it definition-of-done |
| P1 | MV chain decoupled from advw (fires on **part1 success only**) | §10 intent; an advw-only failure must not suppress the (valid) part1-derived rebuild |
| P0 | every derived boolean wrapped `coalesce(…, FALSE)`; new **boolean-NULL gate** (build + verify) | `NULL = 'Y'` is NULL under SQL three-valued logic → NULLs leaked into 33 boolean cols (serves_*/act_*/comp_*/has_smas/has_custody/any_disciplinary), silently breaking `WHERE NOT col` for 9,883–15,227 rows. Spec §4 mandates blanks⇒FALSE. Caught by **independent post-build verification** (reconstructing from raw), NOT by self-verify — which is why the gate now asserts zero boolean NULLs. |
