# USASpending Subsystem — Adversarial Review & Rebuild Plan

**Author:** Principal Data Engineer (adversarial review). **Mode:** READ-ONLY investigation + plan. No DDL, no `write_dataset`, no migrations executed.
**Date:** 2026-06-06. **Plane:** core-x Gen-3 (Parquet/CSV transport → DuckDB compute → LanceDB SoR on R2).
**Verdict up front:** The operator's redesign is **mostly correct and already half-built**. The "API Lance table" he wants *exists and works* (`usaspending_api_landing.py`). The thing he calls "an unworkable mess" (`usaspending_daily_delta.py` — merge API rows into the bulk schema at ingest time) is **dead, has never once succeeded, and should be abandoned.** The real work is: (a) finish the resolving mirror the API-landing module was explicitly built to feed, (b) close two concrete index gaps (zero index on the 454M-row table; zero temporal/monetary index anywhere), and (c) re-point gtm-mcp off the wrong-grain `awards` alias. Details, evidence, and sequencing below.

---

## 0. Evidence ledger — what was probed and what it returned

All probes run via `doppler run --project core-x --config prd` against `HQX_DB_URL_DIRECT` (Postgres `ops`) and R2 (`s3://data-sink`). The `lance` python lib is NOT installed locally, so authoritative **active-index enumeration** and **live row counts** require a Modal-side check — flagged inline as **[MODAL-CHECK]**. Crucially, I found a Postgres ledger that records index-build outcomes authoritatively (`ops.federal_spine_index_runs` with `built`/`skipped`/`failed` arrays), so most index truth did NOT require the Lance lib.

| # | Claim under test | Verdict | Evidence |
|---|---|---|---|
| 1 | 51 bulk tables live in Lance; all 51 succeeded; the 26 "errors" include retry artifacts; award_search id 72 = 78,373,286 rows | **CONFIRMED** | `ops.usaspending_table_runs`: 78 rows total, 52 success / 26 error. `count(DISTINCT table_name) WHERE status='success'` = **51**. Tables with an error AND no success row = **0**. award_search id 72 = success, 78,373,286 rows; id 78 = trailing post-success error (row_count 0). |
| 2 | Index coverage gap; silent-skip of mismatched column names | **CONFIRMED + REFRAMED** | The disk `_indices/` proxy is *corroborated* by the authoritative ledger `ops.federal_spine_index_runs`. See §3 for the per-dataset built/skipped truth. The silent-skip is real and is in code (`federal_spine_index_campaign.py:404-410`). |
| 3 | Daily delta is dead; 2 rows both error; rows_upserted=0; no watermark | **CONFIRMED** | `ops.usaspending_award_search_delta_runs`: exactly **2 rows**, both `status=error`, both `run_mode=cold_start`, `rows_upserted=0`, `error_message` **empty/NULL**, latest feed_date 2026-06-03. `max(feed_date) WHERE status='success'` = **NULL** (no watermark ever set). award_search `_versions/` shows **4 manifests** (bulk create + 3 index commits) and **no merge commit** — the merge never landed. |
| 4 | gtm-mcp `awards` alias is wrong grain; only analytical path is hand-SQL; capped at 1000 | **CONFIRMED** | `database.py:81` `ALIASES = {"awards": "contractor_award_summary"}`. `audience.py:113` `lookup_awards_by_uei` pushes down to `awards.recipient_uei` (per-UEI lifetime rollup). `database.py:101` `MAX_QUERY_ROWS = 1000`. `award_search` (catalog) = **154 columns**, has `naics_code`, `action_date DATE`, `recipient_uei`, `total_obligation`, `award_amount`. |
| 5 | Two ingest schemas (bulk dump vs API); map every path | **CONFIRMED + EXTENDED** | Full path map in §1. The API-landing path is a **working Lance-native landing tier** at `s3://data-sink/usaspending_api_landings/` — this IS the operator's "API Lance table." A separate **monthly bulk-CSV ingest does NOT exist** (see §1, refuting that hypothesis). |

**Raw probe outputs that anchor the plan:**

- `ops.usaspending_award_search_delta_runs` (both rows): `run_mode=cold_start`, `window_start=2026-05-06`, `window_end=2026-06-02`/`2026-06-03`, `rows_upserted=0`, `status=error`, `error_message=` (empty), `raw_landing_uri=s3://dex-raw-landing-zone/usaspending/award_search/api-delta/date=…/bulk_download_awards.parquet`. **The raw parquet exists** (50.8 MB on both dates) — so fetch + raw-land succeeded; the **transform/merge phase failed**.
- `ops.usaspending_award_search_api_landing_runs` id 1: `pull_date=2026-06-04`, `rows_landed=583776`, `columns_landed=297`, `api_calls=166`, `status=success`, `landing_uri=s3://data-sink/usaspending_api_landings/award_search/pull_date=2026-06-04/`. **The API path works.**
- `ops.crosswalk_sam_usaspending_runs`: **daily success** (ids 2–6), last 2026-06-06, `rows_written=1,028,144`, `matched_any=530,359`. The SAM↔USAspending crosswalk runs healthy at 16:00 UTC daily (dispatched by `sam_spine_refresh` precedent / its own cron).
- `ops.federal_spine_index_runs` (latest success per usaspending dataset):
  | dataset | rows_indexed | built | skipped |
  |---|---|---|---|
  | award_search | 78,373,286 | recipient_uei (id6) + parent_uei, naics_code (id8) | recipient_uei(already-indexed) |
  | recipient_lookup | 17,754,022 | uei, parent_uei | — |
  | recipient_profile | 18,275,944 | uei, parent_uei | — |
  | subaward_search | 9,801,723 | awardee_or_recipient_uei, ultimate_parent_uei, sub_awardee_or_recipient_uei, sub_ultimate_parent_uei, naics, sub_naics | — |
  | transaction_search_fabs | 128,784,183 | recipient_uei, parent_uei, naics_code, cage_code | — |
  | transaction_search_fpds | 107,250,527 | recipient_uei, parent_uei, naics_code, cage_code | — |
  | **financial_accounts_by_awards (454,215,610)** | **— (NO ROW)** | **NONE** | **NONE** |
- R2 confirmations: `s3://data-sink/active/usaspending/financial_accounts_by_awards/_indices/` is **EMPTY**; `…/award_search/_indices/` = **3 dirs**; data sizes — award_search **39.8 GiB** (75 frags), financial_accounts_by_awards **43.2 GiB** (434 frags), transaction_search_fabs **57.5 GiB** (123 frags). catalog.json `dataset_count: 103`; `usaspending_api_landings` is **NOT in catalog.json** (lives outside `active/`, so gtm-mcp can't see it).

---

## 1. Complete ingest-path map (Finding #5)

There are **four** distinct USAspending code paths in `pipelines/usaspending/`. Source / schema / target / cadence / last-success:

| Path | Source | Schema | Write target (R2) | Grain | Cadence / trigger | Last success |
|---|---|---|---|---|---|---|
| **`usaspending_bulk.py`** | USAspending.gov **full Postgres pg_dump** (`.dat.gz` COPY members), vintage snapshot **2026-05-06**, TOC ids 5846–5969 | Native Postgres `rpt`/`public`/`int` schema (snake_case), derived at runtime from the dump TOC | `s3://data-sink/active/usaspending/<table>/` ×51 | per-table (award_search = award grain, 78.4M) | **Manual / dispatcher** (`src/trigger/usaspending_bulk.ts`); not on a cron. Rare, heavy (161 GiB). | 2026-06-01 (ingest), 51 tables |
| **`usaspending_api_landing.py`** | USAspending **REST** `POST /bulk_download/awards/` (async CSV), rolling **45-day** `last_modified_date` window | Verbatim API CSV, `all_varchar`, **no projection / no dedup** (297 cols) | `s3://data-sink/usaspending_api_landings/award_search/pull_date=YYYY-MM-DD/` (Lance, dated, immutable, 250K-row frags) | award grain, append-only by pull_date | Dispatcher / manual (`::run` trailing 45d). **No cron wired yet** in `src/trigger/` (no `usaspending_api_landing.ts`). | **2026-06-04**, 583,776 rows, 297 cols |
| **`usaspending_daily_delta.py`** | USAspending REST — cold_start `bulk_download/awards`; steady_state `search/spending_by_award` | Projected + TRY_CAST into `rpt.award_search` types; `merge_insert` on `generated_unique_award_id` **into the bulk SoR** | `s3://data-sink/active/usaspending/award_search/` (mutates SoR) | award grain (collapsed on max `last_modified_date`) | `src/trigger/usaspending_daily_delta.ts` cron **`0 11 * * *`** | **NEVER** (2 error runs; no watermark) |
| **`contractor_award_summary.py`** | Reads `usaspending/award_search` (+ subaward) from Lance | Derived per-`recipient_uei` **lifetime rollup** | `s3://data-sink/active/contractor_award_summary/` | **one row per UEI** (no naics, no action_date) | `src/trigger/contractor_award_summary.ts` cron `0 18 * * *` | (see `ops.contractor_award_summary_runs`) |

Plus `ffata_exec_comp.py` (FFATA executive-comp sidecar) and `usaspending_api_catalog.py` (validates live `spending_by_award` field names against the API — a guard, not an ingest).

**Refutation of the "monthly bulk-CSV ingest" hypothesis:** No such path exists. There is no monthly bulk-CSV ingest module, no `*_runs` ledger for one, and no cron for one. The three real ingests are: (1) the heavy **pg_dump bulk** (manual, monthly-ish), (2) the **45-day rolling API landing** (Lance-native, working), (3) the **dead daily delta**. The operator's memory of "a monthly bulk-CSV ingest" is most likely a conflation of the pg_dump bulk with the API landing's bulk_download CSV mechanism.

---

## 2. Root cause of the dead delta + go/no-go (Finding #3)

### 2.1 What actually happened
Both runs: `run_mode=cold_start`, fetched the wide window `[2026-05-06 → yesterday]` via async `bulk_download/awards`, **landed the raw parquet successfully** (50.8 MB present in R2 on both dates), then **failed in the terminal TRANSFORM+MERGE phase** with `status=error` and an **empty `error_message`**.

### 2.2 Why `status=error` and not `stalled`
The code distinguishes two terminal failure modes (`usaspending_daily_delta.py:710-732`):
- **0-row window** → raises `USASpendingDataLagException` → recorded `status=stalled`.
- **any other exception** → caught at line 730 → `status=error`, `error = f"{type(exc).__name__}: {exc}"`.

The ledger says **error**, so it was NOT a 0-row lag stall — a real exception was thrown in `_build_delta_award_grain` or `_merge_delta`. Given a 50 MB raw parquet, the window was non-empty, so `grain > 0` and the lag guard was bypassed. **The failure is in the merge.**

### 2.3 The merge-phase fault
`_merge_delta` (lines 580–592) casts **every** projected delta column to the live award_search field by name:
```python
fields = [target_schema.field(name) for name in delta.column_names]  # KeyError ⇒ bad map
delta = delta.cast(pa.schema(fields))
```
The cold projection (`_COLD_PROJECTION`, lines 398–419) emits 20 aliases including `action_date`, `total_obligation`, `naics_code`. All 20 **exist** in award_search's 154-col schema (verified against catalog.json). So a plain `target_schema.field(name)` KeyError is **not** the obvious cause for those 20 — which deepens the mystery and points at one of:
- **(a) A type-cast failure** inside `delta.cast(...)`: the projection TRY_CASTs CSV strings to DOUBLE/DATE/TIMESTAMP, but the **final cast to the live award_search Arrow field types** can still fail if a column's inferred Arrow type from DuckDB→Arrow disagrees with the committed Lance field type (e.g. `date32` vs `timestamp[us]`, `double` vs `decimal128`). This raises a bare exception whose `str()` can be terse — and notably, the merge runs `_optimize_indices` after; a partial commit + optimize failure is plausible.
- **(b) The R2 multipart `InvalidPart` 400** — the SAME failure that forced `transaction_search_fabs`/`hmda_lar` onto the staged index path (`federal_spine_index_campaign.py:485-510`). A `merge_insert` that rewrites award_search fragments and then `optimize_indices` over a 78M-row dataset writes large index pages to R2; the object-store writer's mid-upload multipart escalation hits R2's "all non-trailing parts must have the same length" rule. This is a **proven, deterministic** failure mode on this exact plane for large in-place Lance writes to R2.
- **(c) The empty `error_message`** itself indicates the recorded error string was falsy at write time. The current code always sets a non-empty `error` on the `except` path, so EITHER the rows were written by a **prior code version** (a Gen-2/early-port artifact, before the message was populated) OR the exception's `str()` genuinely rendered empty. Given the file's maturity, (c) most likely means **these 2 ledger rows predate the current file** and the on-disk code was revised after they were written — i.e. the ledger is stale evidence of an earlier, since-edited implementation.

### 2.4 [MODAL-CHECK] required to pin the exact exception
The ledger cannot tell us more (empty message). To get the authoritative cause without mutating anything, run the **dry-run plan path** (fetch + count + build grain, **merge nothing**):
```
modal run pipelines/usaspending/usaspending_daily_delta.py::run \
  --mode cold_start --window-start 2026-05-06 --window-end 2026-06-05 --dry-run
```
`plan_award_search_delta` calls `_run_delta(..., dry_run=True)` which executes `_build_delta_award_grain` (the projection + grain collapse) but skips `_merge_delta`. If the dry-run **succeeds with grain > 0**, the fault is isolated to `_merge_delta` (cast or R2-multipart → option (a)/(b)). If the dry-run **fails**, the fault is in the projection/CSV read. This is the single decisive next probe and it is **non-mutating**.

### 2.5 Go / No-Go: **ABANDON the in-SoR daily delta.** 
Decisive recommendation: **kill `usaspending_daily_delta.py` and its cron (`usaspending_daily_delta.ts`, `0 11 * * *`).** Reasons:
1. It is the exact architecture the operator (correctly) calls unworkable: jamming a thin API projection into a 154-col bulk schema and mutating the SoR in place.
2. `merge_insert` + `optimize_indices` **in-place on a 78M-row R2 Lance dataset** repeatedly trips R2's multipart rule (the documented reason the index campaign needed a staged path). Doing this **daily** is fragile by construction.
3. `when_matched_update_all()` over a column subset is a **standing corruption hazard**: it relies on the empirically-observed lance 7.0.0 behavior that non-source columns are preserved. That is an undocumented invariant to bet the SoR on, daily, forever.
4. The replacement already exists: the **API landing tier** captures the same freshness (45-day rolling `last_modified_date` window) **without touching the SoR**, and a downstream materialized mirror (the operator's model) resolves bulk ∪ API at the read/MV layer — exactly Gen-2's proven union-at-read pattern, upgraded.

**Salvage value retained:** the F5 BotDefense fetch logic, the async bulk_download poller, and the projection maps are reusable — but as inputs to the **API-landing → mirror** flow, not as an in-SoR merge.

---

## 3. Authoritative index-coverage truth + silent-skip confirmation (Finding #2)

### 3.1 The silent-skip mechanism IS in the code (confirmed)
`federal_spine_index_campaign.py:403-411`:
```python
for col in plan:
    if col not in present:               # not in committed Lance schema
        skipped.append(f"{col}(absent)")
        continue
    if col in already:                   # already has a committed index
        skipped.append(f"{col}(already-indexed)")
        continue
    ... build BTREE ...
```
The plan is **runtime-gated against the committed schema** (line 391 `present = set(ds.schema.names)`), so a planned name that doesn't match the on-disk column is silently skipped (recorded, never fatal — `:269-273` docstring confirms). **However**, the worry from the session findings (that many planned columns were silently dropped) is **NOT** what happened here: the `skipped` arrays in the ledger contain only `recipient_uei(already-indexed)` and `recipient_uei(absent)` — both correct, intentional skips, not name-drift casualties. The INDEX_PLAN (`:274-286`) is deliberately **narrow** (resolution keys only: UEI/NAICS/CAGE variants), so few columns were ever planned. The "13 planned vs 4 on disk" framing conflated `usaspending_bulk.py`'s aspirational `INDEX_PLAN` comment with the campaign's actual executed plan.

### 3.2 The authoritative way to enumerate ACTIVE indexed columns
Two sources, in order of authority:
1. **The ledger** `ops.federal_spine_index_runs` — the `built[]`/`skipped[]`/`failed` arrays are the executed truth (used in §0). This is queryable from the shell and is the recommended operational source of record.
2. **[MODAL-CHECK]** for live manifest truth (catches manual index ops not in the ledger, and confirms indices survived merges): the campaign already ships a verify sweep —
   ```
   modal run pipelines/resolution/federal_spine_index_campaign.py::verify
   ```
   which calls `list_indices()` per dataset (`_list_committed_indices`, `:158-179`, handles the `list_indices`/`list_indexes` return-shape drift). Run this to get the manifest-level active index list per dataset. The disk `_indices/` object count is **not** authoritative (can include superseded dirs) — use `list_indices()`.

### 3.3 The two REAL index gaps (the load-bearing findings)
1. **`financial_accounts_by_awards` (454,215,610 rows, 43.2 GiB, 434 fragments) has ZERO scalar indices** and is **not in INDEX_PLAN at all.** Any lookup is a full 454M-row scan. This is the single largest table in the subsystem and is completely unindexed.
2. **No temporal or monetary index exists on ANY usaspending table.** Every built index is a UEI/NAICS/CAGE resolution key. There is **no BTREE on `action_date`, `last_modified_date`, or `award_amount`/`total_obligation`** on award_search. The analytical query the operator cares about ("aerospace contracts > $150k in last 90 days") filters on `naics_code` (indexed), **`action_date` (NOT indexed)**, and **`award_amount` (NOT indexed)** — so it degrades to a 78M-row scan with only the naics predicate pushed down. This is *the* reason the system feels "slow and basically unusable" for analytical questions.

---

## 4. Target end-state architecture

### 4.1 Endorsement of the operator's model — with one correction
The operator's three-part model:
> "Bulk data gets appended to the massive bulk datasets. The API pulls go to their own API Lance table. Materialized Lance mirrors are produced against both that resolve and de-duplicate."

**ENDORSED**, with these specifics and one correction:
- **Bulk SoR**: KEEP as-is (51 datasets under `active/usaspending/`). Correction: it is **NOT currently append-friendly** — `usaspending_bulk.py` writes `mode="create"`/overwrite from a full pg_dump. "Append to the massive bulk datasets" is not how the bulk path works and **should not become how it works** — a monthly full-dump overwrite is cleaner than incremental appends to a 78M-row base (no fragment sprawl, deterministic). The freshness *between* monthly dumps is the mirror's job, not the bulk's.
- **API Lance table**: **ALREADY EXISTS** (`usaspending_api_landing.py` → `usaspending_api_landings/award_search/pull_date=…/`). Native 297-col API schema, no forced conformance to the dump schema — exactly right. Only gap: no cron, and it's invisible to gtm-mcp (outside `active/`).
- **Resolving/dedup mirror**: this is the **new build**. It is the correct place to resolve bulk ∪ API.

### 4.2 Dataset topology (text diagram)

```
SOURCES                         SYSTEM OF RECORD (active/, immutable per ingest)        SERVED / ANALYTICAL
─────────────────────────       ──────────────────────────────────────────────        ─────────────────────────────

USAspending.gov pg_dump  ──►  s3://data-sink/active/usaspending/<table>/  (×51)
(monthly, manual)              ├─ award_search/            78.4M  award grain  ───┐
                               ├─ transaction_search_fpds/ 107M   txn grain       │
                               ├─ transaction_search_fabs/ 128.8M txn grain       │
                               ├─ subaward_search/         9.8M                    │
                               ├─ financial_accounts_by_awards/ 454M  ◄── NO INDEX │
                               ├─ recipient_lookup/ recipient_profile/             │
                               └─ … 45 reference tables                            │
                                                                                   ├──►  s3://data-sink/active/usaspending_mirror/award/
USAspending REST API     ──►  s3://data-sink/usaspending_api_landings/            │      (NEW — resolved award-grain mirror)
bulk_download/awards          award_search/pull_date=YYYY-MM-DD/  ────────────────┘      • dedup key: generated_unique_award_id
(45-day rolling, daily)       297-col native, append-only, NO dedup                      • precedence: max(last_modified_date)
                                                                                         • union(bulk award_search ∪ API landings)
                                                                                         • BTREE: gen_uniq_award_id, recipient_uei,
                                                                                           naics_code, action_date  | $ via filter
                                                                                         • BITMAP: award_type/category
                                                                                                    │
                                                                                                    ▼
                                                              contractor_award_summary/  ◄── (KEEP) per-UEI lifetime rollup
                                                              (rebuild FROM the mirror, not raw award_search)
                                                                                                    │
                                                                                                    ▼
                                                                            gtm-mcp: typed analytical award tool
                                                                            (pushdown on naics+action_date+$ to mirror)
```

### 4.3 What grain(s) of mirror are actually needed — decisive answer
- **Award-grain mirror: YES, build it.** `usaspending_mirror/award/`. This is the grain every GTM question needs (industry + amount + recency + recipient). It is the union of bulk `award_search` (78.4M) and the API landings, deduped to one row per `generated_unique_award_id`.
- **Transaction-grain mirror: NO, do not build.** 107M + 128.8M = 236M rows across fpds+fabs. The API landing is award-grain (`bulk_download/awards`), so there is no fresh transaction feed to union — a transaction mirror would just be a re-indexed copy of the bulk with no freshness benefit. Index the bulk transaction tables in place instead (already done: 4 BTREEs each).
- **Recipient-grain mirror: NO.** `contractor_award_summary` already serves recipient-grain; rebuild it *from the award mirror* so it inherits freshness.

### 4.4 Materialized vs query-time UNION vs periodic compaction — decisive answer
**Materialize the award mirror; do NOT do query-time UNION.** Tradeoff quantified:

| Option | Storage | Freshness lag | Query cost | Index maintainability |
|---|---|---|---|---|
| **Query-time UNION view** (bulk ∪ API at read) | 0 extra | 0 (always live) | **High** — every analytical query scans 78.4M bulk + N×583K API frags AND dedups at runtime; no index on the deduped result; the 1000-row cap truncates pre-dedup | **Poor** — can't index a view; pushdown only on each base's own indices, then a runtime QUALIFY |
| **Materialized mirror** (rebuild daily) | +~40 GiB (one award-grain copy) | ≤24h (daily rebuild) | **Low** — single indexed dataset; naics+action_date+$ pushdown; dedup already resolved | **Good** — real BTREE/BITMAP on the materialized columns |
| **Periodic compaction** (merge API into bulk dataset) | 0 extra | ≤24h | Low | **Fragile** — this IS the dead delta's in-place-merge approach; R2 multipart hazard |

The materialized mirror wins decisively for an interactive GTM analytical surface: the whole point is **fast, indexed** industry/amount/recency filtering, which a runtime UNION cannot give. The +40 GiB is negligible on R2. The ≤24h lag is acceptable (and beats today's ~30-day staleness by a mile; the underlying API data itself lags 7+ days at the warehouse, so sub-daily mirror refresh buys nothing real).

---

## 5. The resolution + dedup contract for the award mirror (concrete DuckDB shape)

**Inputs:**
- Bulk base: `s3://data-sink/active/usaspending/award_search/` (78.4M, award grain, 154 cols, vintage 2026-05-06).
- API fresh: all `s3://data-sink/usaspending_api_landings/award_search/pull_date=*/` fragments (297-col native, multiple overlapping pull_dates).

**Dedup key:** `generated_unique_award_id` (the API's `generated_internal_id`; present in both, VARCHAR, the merge key the delta already chose). Confirmed present in both schemas.

**Precedence rule:** an award row is taken from whichever source has the **greatest `last_modified_date`**; ties broken **API-wins** (the API is the fresher feed by construction for the recent window). For columns the API does not supply, fall back to the bulk row's value (the API is a thin ~20-col projection; the bulk carries all 154). This is a **column-coalesce on a row-precedence backbone**, not a blind row replacement — which is exactly the corruption trap the in-SoR delta risked, now done safely in a fresh dataset.

**Late-arriving / corrected records:** handled for free. The API landing re-pulls a **45-day rolling `last_modified_date` window daily**, so a corrected award re-enters the landings with a newer `last_modified_date`; the next mirror rebuild picks it up via the `max(last_modified_date)` precedence. DoD's deliberate ~90-day FPDS publish delay also lands as a fresh `last_modified_date` event when it finally publishes — the rolling window + max-precedence captures it. (If a correction lands >45 days after its prior modification AND the monthly bulk hasn't refreshed, it can be missed — see Red-Team §10.)

**DuckDB shape (illustrative — the build worker's core, not prose):**
```sql
-- Stage A: collapse the API landings to one row per award (newest pull wins),
-- projecting the API's ~20 authoritative columns to the bulk's snake_case names.
CREATE TEMP TABLE api_award AS
SELECT * FROM (
  SELECT
    nullif(trim(generated_internal_id),'')          AS generated_unique_award_id,
    nullif(trim("Recipient UEI"),'')                AS recipient_uei,
    nullif(trim("Recipient Name"),'')               AS recipient_name,
    TRY_CAST("Award Amount" AS DOUBLE)              AS award_amount,
    TRY_CAST("Award Amount" AS DOUBLE)              AS total_obligation,
    nullif(trim(naics_code),'')                     AS naics_code,
    TRY_CAST(action_date AS DATE)                    AS action_date,
    TRY_CAST(last_modified_date AS TIMESTAMP)       AS last_modified_date,
    ...                                              -- the rest of the ~20 API cols
    'api'                                            AS _src
  FROM read_lance('s3://data-sink/usaspending_api_landings/award_search/pull_date=*/')  -- via lance scanner → arrow → duckdb
)
QUALIFY row_number() OVER (PARTITION BY generated_unique_award_id
                          ORDER BY last_modified_date DESC NULLS LAST) = 1;

-- Stage B: per-key precedence (API row wins iff its last_modified_date >= bulk's;
-- else keep bulk). Coalesce API-supplied cols over the bulk row for the winning key.
CREATE TEMP TABLE mirror_award AS
WITH bulk AS (SELECT *, 'bulk' AS _src FROM read_lance('s3://data-sink/active/usaspending/award_search/')),
joined AS (
  SELECT
    COALESCE(b.generated_unique_award_id, a.generated_unique_award_id) AS generated_unique_award_id,
    -- precedence flag: does the API carry a newer modification?
    (a.generated_unique_award_id IS NOT NULL
       AND (b.last_modified_date IS NULL OR a.last_modified_date >= b.last_modified_date)) AS api_wins,
    -- coalesce each load-bearing column: API value when api_wins, else bulk
    CASE WHEN <api_wins> THEN COALESCE(a.recipient_uei, b.recipient_uei) ELSE b.recipient_uei END AS recipient_uei,
    CASE WHEN <api_wins> THEN COALESCE(a.award_amount, b.award_amount)   ELSE b.award_amount   END AS award_amount,
    CASE WHEN <api_wins> THEN COALESCE(a.action_date, b.action_date)     ELSE b.action_date     END AS action_date,
    GREATEST(b.last_modified_date, a.last_modified_date)                 AS last_modified_date,
    ... -- all 154 bulk cols carried; API-absent cols always come from bulk
  FROM bulk b FULL OUTER JOIN api_award a USING (generated_unique_award_id)
)
SELECT * EXCLUDE (api_wins) FROM joined;
-- then lance.write_dataset(mirror_award -> active/usaspending_mirror/award/, mode='overwrite'); build indices (§6).
```
Notes: `read_lance(...)` is shorthand for the worker opening each Lance dataset via the lance scanner → Arrow → DuckDB `register` (the established core-x pattern; DuckDB does not read Lance natively). The mirror carries the **full 154-col bulk schema** so it is a drop-in superset of `award_search`; API-only freshness updates the ~20 columns that matter for GTM.

---

## 6. Index strategy per load-bearing table (exact columns, type, build mechanism)

**BTREE** = high-cardinality / range (equality + `>`/`<`/`BETWEEN`). **BITMAP** = low-cardinality categorical. Build mechanism = `federal_spine_index_campaign.py` (the proven worker: 64 GiB, `LANCE_BYPASS_SPILLING=true`, `cpu=8`, sequential per-dataset, per-column retry, ledgered to `ops.federal_spine_index_runs`, blast-radius-isolated from ingest). Over-R2-multipart-threshold tables use its **`::run_staged`** path (stage to `/tmp` local FS, build, publish — the documented fix for the 400 InvalidPart).

| Table | Rows | Add BTREE | Add BITMAP | Build path | Notes |
|---|---|---|---|---|---|
| **`usaspending_mirror/award/`** (NEW) | ~78.4M | `generated_unique_award_id`, `recipient_uei`, `naics_code`, **`action_date`**, **`last_modified_date`** | `type` / `category` (award_type, ~10 values), `awarding_toptier_agency_code` | direct-R2 (proven OK at 78M for award_search) | **`action_date` BTREE is the headline fix** — enables the recency predicate pushdown. Monetary `award_amount` left to filter-eval (BTREE on a continuous DOUBLE helps range scans; build it only if [MODAL-CHECK] shows the scan is still hot — see §10). |
| **`financial_accounts_by_awards`** | 454M | `award_id` (join key to award_search), `parent_award_id` | `disaster_emergency_fund_code` (if present, low-card) | **`::run_staged` MANDATORY** | 43.2 GiB / 434 frags. 454M-row BTREE sort needs the staged path; direct-R2 will hit InvalidPart. **`memory=65536` may be insufficient** for an in-RAM 454M-row sort even with `LANCE_BYPASS_SPILLING` — see §10; likely needs `temp_directory` NVMe spill ON for this one (override the bypass) and ~120 GiB `/tmp`. |
| `award_search` (bulk base) | 78.4M | **`action_date`** (add) | — | direct-R2 | Add `action_date` so the bulk base is queryable on recency even before the mirror exists (lets gtm-mcp fall back to the base). recipient_uei/parent_uei/naics_code already built. |
| `transaction_search_fpds` | 107M | (have 4) — add `action_date` | — | direct-R2 | Already has recipient_uei/parent_uei/naics_code/cage_code. |
| `transaction_search_fabs` | 128.8M | (have 4) — add `action_date` | — | **`::run_staged`** (over threshold, proven) | |
| `recipient_lookup` / `recipient_profile` | 17.8M / 18.3M | (have uei, parent_uei) | — | direct-R2 | Adequate. |
| `subaward_search` | 9.8M | (have 6) | — | direct-R2 | Adequate. |

**Spill / OOM sizing for the giants (decisive):**
- The campaign's default is `memory=65536` (64 GiB) + `LANCE_BYPASS_SPILLING=true`, which runs the BTREE sort fully in RAM. This is sized for the 128.8M-row column (`:362`).
- **454M rows is ~3.5× that.** A single 454M-row VARCHAR column sort in RAM at 64 GiB will **OOM**. For `financial_accounts_by_awards`: **do NOT bypass spilling**; instead set `temp_directory` to a real NVMe path with ≥120 GiB free (the staged path's `/tmp` overlay), let Lance's external sorter spill, and raise `memory` to 96–128 GiB if the spot/on-demand class allows. This is the one table where the campaign's standing config must be overridden. [MODAL-CHECK] the container disk free space before the run.
- award_search mirror (~78.4M) and the txn tables (107M/128.8M) are within the proven envelope (the campaign already built these grains).

**Post-build coverage verification:** after each build, (1) confirm the `ops.federal_spine_index_runs` row shows the column in `built[]` and `failed=[]`, and (2) **[MODAL-CHECK]** `federal_spine_index_campaign.py::verify` (`list_indices()` sweep) to confirm the index is committed in the live manifest and survived. For the mirror specifically, also confirm an indexed predicate query plans as a pushdown, not a scan.

---

## 7. Ingestion + freshness design (cadence, watermarks, self-healing, SLA)

### 7.1 The SAM spine refresh template (the explicit pattern to copy)
`src/trigger/sam_spine_refresh.ts` is the reference. How it works (cite):
- **Daily cron** `30 18 * * *`, `queue.concurrencyLimit=1`, `maxDuration` set to comfortably exceed the sum of waitpoint timeouts.
- **Freshness-gated workers**: each Modal worker takes `skip_if_current=true` and **self-skips** when its upstream label hasn't advanced (a no-change day = two cheap label checks, no compute).
- **Unconditional dispatch of the dependent step**: step 2 (`build_sam_normalized_entities`) is dispatched **every** run regardless of whether step 1 rebuilt. Its own `skip_if_current` no-ops when current. Running it unconditionally is **what makes the chain self-healing** — a current-upstream + stale-downstream state (from a prior partial failure or crash between steps) is caught and fixed on the next daily fire instead of frozen forever.
- **Durable waitpoint callbacks**: mint token → `POST MODAL_DISPATCHER_URL` with `{app_name, function_name, kwargs, trigger_callback_url}` + Modal-Key/Secret → `wait.forToken` (checkpointed, releases the concurrency slot while suspended) → resume on the worker's flat-JSON terminal callback.
- **Overlap precluded by timeout math**, not locks: waitpoint timeouts cap wall-time under the 24h interval, so run N finishes before run N+1.

### 7.2 The USAspending refresh orchestrator (new — modeled on the above)
A new `src/trigger/usaspending_spine_refresh.ts`, daily, dispatching two steps:
1. **`land_award_search_window`** (`usaspending_api_landing.py`, already deployable) — pull the trailing 45-day window → append a new `pull_date=` Lance fragment. Idempotent by pull_date (re-running a date overwrites that partition). **Freshness gate**: skip if today's pull_date already landed successfully (`ops.usaspending_award_search_api_landing_runs`).
2. **`build_award_mirror`** (NEW worker) — UNCONDITIONALLY dispatched (self-healing): rebuild `usaspending_mirror/award/` from bulk ∪ all API landings, then build/optimize its indices. Its own `skip_if_current` no-ops if the newest landing pull_date and the bulk vintage both already match the mirror's recorded inputs.

Optionally a step 3 `build_contractor_award_summary` (rebuild the rollup from the mirror) — or leave it on its existing 18:00 cron reading the mirror.

**Cadence placement:** land at ~09:00 UTC (after USAspending's ~05:00 ETL), mirror-rebuild right after, so the crosswalk (16:00) and contractor_award_summary (18:00) consume a fresh mirror the same day. De-collide from the SAM 18:30 slot.

### 7.3 Watermarks / ledgers per stage
- **API landing**: `ops.usaspending_award_search_api_landing_runs` (exists). Watermark = `max(pull_date) WHERE status='success'`. It's a fixed rolling window, so the "watermark" is just the last successful pull date for the freshness gate.
- **Mirror build (NEW ledger)**: `ops.usaspending_award_mirror_runs` — one row per rebuild: `bulk_vintage` (snapshot_date the bulk award_search was built from), `latest_api_pull_date`, `rows_written`, `rows_from_api`, `rows_from_bulk_only`, `indices_built`, `status`, `error`, timestamps. The freshness gate compares `(bulk_vintage, latest_api_pull_date)` against the last successful row.
- **Index campaign**: `ops.federal_spine_index_runs` (exists) — already the index SoR.
- **Bulk**: `ops.usaspending_table_runs` (exists).
- **DECOMMISSION**: `ops.usaspending_award_search_delta_runs` is retired with the delta (keep the table for history; stop writing).

### 7.4 Freshness SLA + staleness detection/surfacing
- **SLA**: the served award mirror is **≤ 36h stale** relative to the USAspending API (one daily landing + one daily mirror rebuild + slack). Underlying warehouse lag (7+ days, 90 days for DoD) is upstream and unbeatable — surface it, don't pretend to fix it.
- **Detection**: a lightweight daily check (can ride the orchestrator's tail or a separate cron) asserts `max(pull_date)` in the landing ledger ≥ today−2 and the mirror ledger's `latest_api_pull_date` ≥ today−2; on breach, log error + (optional) alert. This is the gap the dead delta never had — it failed silently with an empty error message for days.
- **Surfacing to consumers**: the mirror dataset carries a `_mirror_built_at` / `_latest_api_pull_date` column (or a sidecar `active/usaspending_mirror/_meta.json`) so gtm-mcp can read and return freshness with every analytical answer (§8).

---

## 8. gtm-mcp changes

1. **Add a typed, indexed analytical award tool** — `search_awards(naics_code=None, naics_prefix=None, min_amount=None, since_date=None, recipient_uei=None, agency=None, limit=...)` in `apps/gtm_mcp/src/tools/audience.py`. It builds a Lance `scanner(filter=..., columns=[...])` against `active/usaspending_mirror/award/`, pushing `naics_code` (BTREE), `action_date >= since_date` (BTREE — the new index), and `recipient_uei` (BTREE) predicates down to the indices. This is the missing typed path for "aerospace contracts > $150k in last 90 days." Returns rows + a `freshness` block.
2. **Re-point the analytical grain off the `awards` alias.** Keep `awards`→`contractor_award_summary` for the **lifetime-rollup point-lookup** (`lookup_awards_by_uei`), but add an explicit **`award_mirror`** registry name → `active/usaspending_mirror/award/` and make it the default for any per-award / industry / recency / amount question. Update the `execute_audience_query` docstring and `list_datasets`/`describe_dataset` so agents are steered to `"usaspending_mirror/award"` for award-grain analytics and told plainly that `awards` is a per-UEI lifetime rollup (no naics, no action_date) — the current docstring's silence on grain is the trap.
3. **Register the mirror (and optionally the API landings) in the catalog.** The mirror lives under `active/`, so it will appear in catalog.json automatically once written. The raw API landings (outside `active/`) stay invisible by design — analytics read the mirror, not the raw landings.
4. **Surface freshness/staleness.** Every analytical award tool returns `{"as_of": <mirror_built_at>, "latest_api_pull_date": …, "bulk_vintage": …, "warehouse_lag_note": "USAspending warehouse lags 7+ days (90 for DoD prime detail)"}`. The calling agent can then caveat recency claims honestly.
5. **Handle the 1000-row cap (`MAX_QUERY_ROWS=1000`).** For the typed `search_awards` tool, (a) make `limit` explicit and default to a sane page (e.g. 200), (b) return `truncated: true` plus the total matched count (a cheap `count_rows(filter=...)` against the index), and (c) for genuine bulk export, return a continuation cursor (order by `action_date`/`generated_unique_award_id`, page on the indexed key) instead of silently clipping at 1000. The raw `execute_audience_query` keeps its 1000 cap as a guardrail but the typed tool gives agents a paginated, total-aware path.

---

## 9. Sequencing (ordered phases, dependencies, blast radius)

**Reversible / low-risk steps are front-loaded; heavy/irreversible writes are gated behind verification. Do the FIRST item first because it is the single highest-value, lowest-risk fix and it unblocks the analytical use case immediately.**

### Phase 0 — Diagnose + stop the bleeding (no data writes)
- **0a. [DO THIS FIRST] [MODAL-CHECK]** run the delta dry-run (`::run --dry-run --mode cold_start`) to pin the exact merge failure (§2.4), and run `federal_spine_index_campaign.py::verify` to capture the live index manifest baseline. *Blast radius: none (read-only).* **Justification for "first":** it is non-mutating, it resolves the one open root-cause question, and it produces the index baseline every later phase verifies against.
- **0b.** Disable the dead daily-delta cron (`usaspending_daily_delta.ts`, `0 11 * * *`) so it stops writing error rows and stops the (small) risk of a partial in-SoR merge. *Blast radius: removes a broken scheduled job; no data change.* Reversible.

### Phase 1 — Index the existing bulk tables (the indices-first question, answered)
**Decisive answer to "indices first vs wait for the topology to change": BUILD THE TWO MISSING INDEX CLASSES NOW — they are NOT wasted by the mirror.**
- `action_date` BTREE on the **bulk `award_search`** and the **mirror** are both needed; the bulk-base index lets gtm-mcp query recency *before* the mirror exists (a working fallback during the build-out) and is the exact column the mirror will also carry. Index work on the bulk transaction tables (`action_date`) is permanent regardless of topology — the mirror doesn't replace the transaction tables.
- **The one thing NOT to do prematurely:** don't over-index speculative columns on award_search that the mirror will supersede. Limit Phase 1 to `action_date` (the proven-hot gap) + the **`financial_accounts_by_awards` baseline indices** (which no future topology removes — it's a bulk-only fact table).
- **1a.** Add `action_date` BTREE to `award_search` (direct-R2), `transaction_search_fpds` (direct-R2), `transaction_search_fabs` (`::run_staged`). *Blast radius: in-place index add on SoR; appends should be quiesced during the build (the campaign is designed to run isolated from ingest). Idempotent (replace=True), ledgered.*
- **1b.** Build `financial_accounts_by_awards` indices via `::run_staged` with NVMe-spill override (§6). *Blast radius: heaviest single build (454M rows); isolate from everything; [MODAL-CHECK] disk free first.* This is the most failure-prone step — run it alone, verify, before proceeding.

### Phase 2 — Wire the API landing to a cron (freshness capture)
- **2a.** Create `src/trigger/usaspending_api_landing.ts` (or fold into the spine orchestrator) on a daily cron, dispatching `land_award_search_window` (45-day rolling). *Blast radius: writes only to the dated landing tier (immutable, outside `active/`); cannot harm the SoR.* Fully reversible.

### Phase 3 — Build the resolving mirror (the new SoR-served dataset)
- **3a.** Implement the mirror build worker (`pipelines/usaspending/usaspending_award_mirror.py`) per §5, with its `ops.usaspending_award_mirror_runs` ledger and `skip_if_current`. *Blast radius: writes a NEW dataset `active/usaspending_mirror/award/`; touches nothing existing.* First run is `mode="overwrite"` of a brand-new path — safe.
- **3b.** Build the mirror's indices (§6) via the campaign (add the mirror to `DATASETS`/`INDEX_PLAN`). *Blast radius: new dataset only.*
- **3c.** Add `build_award_mirror` (unconditional, self-healing) as step 2 of the spine orchestrator.

### Phase 4 — Re-point consumers
- **4a.** gtm-mcp: add `search_awards`, register `award_mirror`, fix docstrings/grain trap, freshness surfacing, pagination (§8). *Blast radius: gtm-mcp read path only; `awards` point-lookup unchanged.* Reversible.
- **4b.** Rebuild `contractor_award_summary` from the mirror (so the rollup inherits freshness). *Blast radius: existing rollup dataset; idempotent overwrite.*

### Phase 5 — Decommission
- **5a.** Delete `usaspending_daily_delta.py` + `usaspending_daily_delta.ts`; stop writing `ops.usaspending_award_search_delta_runs` (retain table for history). *Blast radius: removes dead code; the mirror has fully replaced its purpose.*

**Dependency graph:** 0 → 1 (independent of 2/3, do in parallel) ; 2 → 3 (mirror needs landings) ; 3 → 4 ; 4 → 5. Phase 1 and Phase 2 are independent and can proceed concurrently.

---

## 10. Red-team of this plan

**Failure modes / what would make this wrong:**
1. **The dry-run (0a) might succeed**, proving the dead delta's fault was purely transient R2 multipart and not a design flaw. Even so, the abandon decision **holds** — daily in-place merge+optimize on a 78M-row R2 Lance dataset is fragile by construction (the index campaign needed a staged path for exactly this class of write). A "salvageable" delta is still the wrong architecture vs the mirror.
2. **454M-row index build OOM/spill (the biggest execution risk).** `financial_accounts_by_awards` at 454M rows will OOM the campaign's default in-RAM-sort config (`LANCE_BYPASS_SPILLING=true`, 64 GiB). Mitigation: override to NVMe spill + ≥120 GiB `/tmp` + higher memory, and **[MODAL-CHECK] container disk free before the run**. If even staged+spill fails, fall back to indexing only the single most load-bearing column (`award_id`) and accept a scan on the rest. This step is the one most likely to need iteration.
3. **R2 multipart `InvalidPart` on the mirror's index build.** The mirror is ~78.4M rows (within the direct-R2-proven envelope for award_search), so direct-R2 *should* work — but if a high-cardinality column's index pages exceed the threshold, route that column to `::run_staged`. The campaign's per-column failure capture makes this non-fatal and visible.
4. **Mirror dedup correctness — the precedence edge cases.** (a) An award present ONLY in the API (genuinely new since the bulk vintage) inserts with API columns set and the 130+ bulk-only columns NULL — acceptable (same as the delta's insert path) but means the mirror has partial rows for very recent awards; document it. (b) `last_modified_date` ties or NULLs: the `>=` + API-wins rule handles ties; NULL API `last_modified_date` must NOT win over a non-NULL bulk value (the `NULLS LAST` + the `b.last_modified_date IS NULL OR a >= b` guard handles this — verify in the build). (c) A correction landing >45 days after its prior modification, with no intervening monthly bulk, escapes both the rolling window and the stale bulk — a true (rare) freshness hole; surfaced by the warehouse-lag note, fully closed only by the next monthly bulk.
5. **`read_lance` in DuckDB.** DuckDB cannot read Lance natively; the worker must open each Lance dataset via the lance scanner → Arrow → DuckDB `register`/`from_arrow`. For the 78.4M-row bulk award_search FULL OUTER JOIN, this is a large in-memory Arrow materialization — size the worker memory + use DuckDB `temp_directory` NVMe spill, or stream by `generated_unique_award_id` hash buckets if it doesn't fit. **OOM risk on the join**, not just the index.
6. **Idempotency hazards.** The mirror build must be `mode="overwrite"` of `usaspending_mirror/award/` (deterministic full rebuild) — NOT an incremental merge (that reintroduces the in-place R2 merge hazard). A crash mid-write leaves a partial dataset; the `skip_if_current` gate + overwrite-on-rebuild self-heals on the next fire (the SAM template's exact property). The index build must use `replace=True` (the campaign already does) so a re-run is idempotent.
7. **Quiescing appends during index builds.** Phase 1 indexes the live SoR in place. If a bulk re-ingest or any writer touches those datasets during the build, the manifest version races. The plan relies on **scheduling discipline** (the same defense the SAM/award_search writers use — there is no single-dataset commit lock in core-x). Operator must ensure no bulk ingest runs during Phase 1.

**Open decisions that need the operator's judgment (taste/strategy, not derivable from code):**
- **A. Monetary index on `award_amount`?** A BTREE on a continuous DOUBLE helps range scans but bloats the index and rarely beats a post-naics/action_date filter-eval. Recommendation: **defer** — build it only if [MODAL-CHECK] shows the `naics+action_date`-pushed query is still scan-bound on the amount predicate. Operator's call on whether amount-range is a hot enough filter to pay the index cost.
- **B. Mirror refresh cadence.** Daily is recommended (matches the API landing + the warehouse's own lag makes sub-daily pointless). If the operator wants intraday GTM freshness for a specific campaign, that's a strategy call — but it buys nothing against a 7-day warehouse lag.
- **C. Keep `contractor_award_summary` at all?** If the new typed `search_awards` + a lifetime-rollup query over the mirror covers the per-UEI lookup, the standalone rollup dataset could be retired. Recommendation: **keep** for the sub-100ms point-lookup, but that's an operator preference on maintaining two datasets vs one.
- **D. Monthly bulk automation.** The pg_dump bulk is currently manual. Automating it (a monthly self-healing orchestrator, SAM-template-style) is a separate workstream; the mirror reduces its urgency (freshness now comes from the API path) but the bulk is still the source of the 130+ non-API columns. Operator's call on whether to automate the monthly dump now or after the mirror lands.
- **E. Transaction-grain freshness.** This plan deliberately does NOT freshen the 236M-row transaction tables (no award-grain API feed exists for them). If GTM needs fresh *transaction*-level data, that requires a new `spending_by_transaction` ingest — a scope expansion the operator must explicitly authorize.

---

### Appendix — exact verification commands used (reproducible, read-only)
```
# Ledgers
doppler run --project core-x --config prd -- bash -c 'psql "$HQX_DB_URL_DIRECT" -P pager=off -c "SELECT count(DISTINCT table_name) FROM ops.usaspending_table_runs WHERE status='\''success'\''"'
doppler run --project core-x --config prd -- bash -c 'psql "$HQX_DB_URL_DIRECT" -P pager=off -x -c "SELECT * FROM ops.usaspending_award_search_delta_runs ORDER BY id"'
doppler run --project core-x --config prd -- bash -c 'psql "$HQX_DB_URL_DIRECT" -P pager=off -c "SELECT DISTINCT ON (dataset) dataset, built, skipped FROM ops.federal_spine_index_runs WHERE dataset LIKE '\''usaspending/%'\'' ORDER BY dataset, id DESC"'
# R2
doppler run --project core-x --config prd -- bash -c 'AWS_ACCESS_KEY_ID=$R2_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY=$R2_SECRET_ACCESS_KEY AWS_DEFAULT_REGION=auto aws s3 ls s3://data-sink/active/usaspending/financial_accounts_by_awards/_indices/ --endpoint-url $R2_ENDPOINT'   # EMPTY
doppler run --project core-x --config prd -- bash -c 'AWS_ACCESS_KEY_ID=$R2_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY=$R2_SECRET_ACCESS_KEY AWS_DEFAULT_REGION=auto aws s3 cp s3://data-sink/active/catalog.json - --endpoint-url $R2_ENDPOINT'   # award_search=154 cols, action_date DATE
# Modal-side (NOT runnable locally — lance lib absent)
modal run pipelines/usaspending/usaspending_daily_delta.py::run --mode cold_start --window-start 2026-05-06 --window-end 2026-06-05 --dry-run   # pin merge fault
modal run pipelines/resolution/federal_spine_index_campaign.py::verify   # live index manifest sweep
```
