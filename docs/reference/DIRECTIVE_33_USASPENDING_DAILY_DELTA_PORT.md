# Directive 33 — USAspending Daily Delta: Legacy Investigation & Gen-3 Port Spec

**Status:** investigation + architecture spec (no implementation yet, per directive).
**Scope:** close the ~6-week staleness gap between the monthly 175 GB bulk dump and "today" by porting the Gen-2 daily-delta API ingest into core-x (Gen-3).
**Target dataset:** `s3://data-sink/active/usaspending/award_search/` (Lance, system of record).

---

## 0. TL;DR — three corrections to the directive's stated assumptions

The directive guessed the legacy design from memory. The actual Gen-2 code diverges on three load-bearing points; the port must be built on the real mechanics, not the guess.

| Directive assumed | Legacy reality (verified in code) |
|---|---|
| Endpoint `…/spending_by_award/` | Row-level delta used **`/api/v2/search/spending_by_transaction/`**; the file-level drip used **`/api/v2/bulk_download/awards/`**. `spending_by_award` was **not** used. |
| Window on `action_date` / `updated_at` | Window is on **`date_type: "last_modified_date"`** — and there is an explicit in-code rationale: USAspending lags 7+ days between a contract action and warehouse landing, so an `action_date` daily window returns ≈0 rows. |
| A watermark spanning "last bulk dump → today" | No watermark/state table existed. The window was a **rolling 1-day slice** (`yesterday UTC`), run daily; the gap closes by running every day, not by one wide range. |
| UPSERT on `award_id` at ingest | **No ingest-time upsert.** Daily rows were **appended to a disjoint location**; de-dup was **downstream** at the DuckDB read layer on `(generated_internal_id, mod, max(last_modified_date))`. |

**Headline opportunity for Gen-3:** Lance v2.1 in core-x supports `merge_insert`, which Gen-2 lacked. The port can do what Gen-2 could not — a true **UPSERT keyed on `generated_unique_award_id` directly into the bulk `award_search` dataset** — eliminating the downstream union/dedup tax. This is the recommended path and is exactly what the directive asks for.

---

## 1. Mandate 1 — Where the legacy code lives & what it called

**Repo:** `hq-all/apps/data-engine-x/` (Gen-2 / dex-db archive). Three complementary daily mechanisms + one historical backfill.

| File | Role | Endpoint | Output |
|---|---|---|---|
| [`scripts/run_usaspending_api_daily_ingest.py`](../../../hq-all/apps/data-engine-x/scripts/run_usaspending_api_daily_ingest.py) | **Primary row-level delta** → Parquet | `POST /api/v2/search/spending_by_transaction/` | `s3://dex-raw-landing-zone/usaspending/contracts/api-delta/date={YYYY-MM-DD}/data.parquet` |
| [`scripts/run_usaspending_api_daily_contracts_lance_ingest.py`](../../../hq-all/apps/data-engine-x/scripts/run_usaspending_api_daily_contracts_lance_ingest.py) | 2-stage delta → **Lance append** (library) | Stage 1 `spending_by_transaction`; Stage 2 `GET /api/v2/awards/{id}/` | `s3://dex-raw-landing-zone/polaris-warehouse/usaspending/contracts_lance_api` (NEW dataset, never the bulk one) |
| [`scripts/run_usaspending_daily_ingest.py`](../../../hq-all/apps/data-engine-x/scripts/run_usaspending_daily_ingest.py) | Async **bulk-download drip** → Parquet (heavier, higher fidelity) | `POST /api/v2/bulk_download/awards/` + poll `/status/` | `s3://dex-raw-landing-zone/usaspending/contracts/...` |
| [`scripts/run_usaspending_backfill_r2_ingest.py`](../../../hq-all/apps/data-engine-x/scripts/run_usaspending_backfill_r2_ingest.py) | Historical FY2008–2024 from public award_data_archive (NOT a delta) | `files.usaspending.gov/award_data_archive/` | `s3://dex-raw-landing-zone/usaspending/{stream}/year=YYYY/data.parquet` |

**Orchestrators (Modal, later migrated to Trigger.dev 2026-05-29):**
- `modal/usaspending_api_daily_app.py` — cron `0 6 * * *` UTC (row-level delta).
- `modal/usaspending_api_daily_contracts_lance_app.py` — cron `0 8 * * *` UTC (Lance, Stage-2 fan-out via `modal.Function.map`, batch 25).
- `modal/usaspending_daily_app.py` — bulk-download drip.

### 1a. Exact payload — `spending_by_transaction` (row-level delta)
`run_usaspending_api_daily_ingest.py:280-301`:
```python
time_period = [{
    "start_date": target_date.isoformat(),
    "end_date":   target_date.isoformat(),
    "date_type":  "last_modified_date",   # NOT action_date
}]
payload = {
    "filters": {
        "award_type_codes": ["A", "B", "C", "D"],   # prime contracts
        "time_period": time_period,
    },
    "fields": REQUESTED_FIELDS,          # snake_case projection (11 cols)
    "page":  page,
    "limit": 100,                        # USAspending hard cap
    "sort":  "Action Date",
    "order": "desc",
}
```
`target_date` defaults to **yesterday UTC** (`run_usaspending_api_daily_ingest.py:579-581`). The API auto-returns `internal_id` + `generated_internal_id` regardless of `fields`.

### 1b. Exact payload — `bulk_download/awards` (file drip)
`run_usaspending_daily_ingest.py:120-135`, with the **canonical window rationale** in the docstring (`:110-118`):
```python
payload = {
    "filters": {
        "prime_award_types": ["A", "B", "C", "D"],
        "date_type": "last_modified_date",   # 7+ day warehouse lag → action_date yields 0 rows/day
        "date_range": {"start_date": feed_date.isoformat(), "end_date": feed_date.isoformat()},
    },
    "file_format": "csv",
}
```
Then poll `/api/v2/bulk_download/status/` every 15 s, 60 min ceiling, until `status=="finished"` → `file_url` (`:147-172`). The `date_range` accepts a **multi-day span** — this is the natural cold-start tool for a wide catch-up.

### 1c. Pagination (both search paths, identical)
`run_usaspending_api_daily_ingest.py:288-320`, `run_usaspending_api_daily_contracts_lance_ingest.py:346-374`:
```python
page = 1; api_calls = 0
while api_calls < max_api_calls:        # ceiling: 500 (parquet) / 1000 (lance)
    body = post(payload | {"page": page, "limit": 100})
    yield body["results"]
    if not body.get("page_metadata", {}).get("hasNext"):   # no `total` key anymore
        break
    page += 1
```
**F5 BotDefense lesson (critical, must port):** USAspending throttles by source IP. In-script long backoff does **not** recover — a fresh egress IP does. Gen-2 fails fast per attempt and pushes retry to the orchestrator: `modal.Retries(max_retries=5, backoff_coefficient=2.0, initial_delay=30.0)` → each retry = a fresh Modal container = fresh IP (`run_usaspending_api_daily_ingest.py:200-219`). 429s (which *do* recover in seconds) get one in-script retry-after sleep.

---

## 2. Mandate 2 — Reconciliation logic

**There was none at ingest time.** Confirmed across all three daily files:

- Row-level delta and bulk drip write to **disjoint R2 prefixes** that coexist with the monthly archive (`run_usaspending_api_daily_ingest.py:10-22`).
- The Lance path explicitly chose **"Option A": a new dataset `contracts_lance_api`**, leaving the 15.5M-row bulk `contracts_lance` untouched; the read service unions both at query time (`run_usaspending_api_daily_contracts_lance_ingest.py:14-19`, `588-590`). `assemble_rows` states *"No dedupe — every row appended as-is"* (`:430`).
- **De-dup is downstream**, last-writer-wins on the natural transaction key: `(generated_internal_id, mod, max(last_modified_date))` (`run_usaspending_api_daily_ingest.py:14-22`). The API-delta wins for the recent window because it carries the newer `last_modified_date`.

So the legacy answer to *"did API rows overwrite bulk rows?"*: **no physical overwrite** — logical dedup at read, API-delta superseding bulk for overlapping keys.

---

## 3. Mandate 3 — Gen-3 port spec (core-x)

### 3.1 Target state (verified)
- **Bulk pipeline** [`pipelines/usaspending/usaspending_bulk.py`](../../pipelines/usaspending/usaspending_bulk.py): ingests the 161 GB pg_dump ZIP (snapshot `2026-05-06`) → per-table Lance at `s3://data-sink/active/usaspending/<table>/`, `mode="create"`, `data_storage_version="2.1"`, written LOCAL then published to R2 via boto3 (Lance's object-store writer trips R2's uniform-multipart rule) (`:656-708`).
- **`award_search`** = dump_id `5967`, schema `rpt`, **award-grain** (one row per award). Index plan (`:171-176`): BTREE on `award_id, generated_unique_award_id, recipient_uei, recipient_hash, action_date, …, piid, fain, uri`; BITMAP on `type, category, awarding_toptier_agency_code`. Schema is **derived at runtime from the pg_dump TOC**, so the Lance columns are the full Postgres `rpt.award_search` set (snake_case), **not** API display names.
- **R2 storage options** (authoritative key names — differ from Gen-2's `aws_endpoint`/`aws_region`) (`:212-225`):
  ```python
  {"aws_access_key_id", "aws_secret_access_key", "endpoint", "region": "auto"}
  ```
- **Orchestration**: Trigger.dev v4 → Universal Modal Dispatcher → Modal worker → durable waitpoint callback. Daily tasks use `schedules.task({ cron: { pattern, timezone } })` (e.g. `crosswalk_hmda_gleif.ts:42-46` `0 8 * * *`). The bulk task ([`src/trigger/usaspending_bulk.ts`](../../src/trigger/usaspending_bulk.ts)) is the dispatch+waitpoint reference: mint token → `POST MODAL_DISPATCHER_URL` with `{app_name, function_name, kwargs, trigger_callback_url}` + `Modal-Key`/`Modal-Secret` headers → `wait.forToken`. Callback body is **flat JSON, no `{data:…}` envelope**.
- **Merge idiom already in core-x** ([`pipelines/uspto_tm/ingest.py:863-874`](../../pipelines/uspto_tm/ingest.py), also `edgar`, `shovels`, `osha`):
  ```python
  if not _dataset_exists(uri, so):
      _write_dataset(deduped, uri, mode="overwrite", so=so); _create_indexes(...)
  else:
      ds = lance.dataset(uri, storage_options=so)
      ds.merge_insert(mk).when_matched_update_all().when_not_matched_insert_all().execute(deduped)
      _optimize_indices(uri, so)
  ```
- **Commit-conflict constraint**: core-x has **no commit-lock helper** (verified). Concurrency is managed by *scheduling discipline* — `gleif_daily.ts` notes distinct datasets run in parallel precisely because *"there is no single-dataset commit-conflict constraint"* for them. For a single shared dataset there **is** one: the daily-delta writer must **never co-run with the bulk writer against `award_search`**, and must be a single writer per run.

### 3.2 Recommended architecture

**Merge the daily delta directly into `award_search` via `merge_insert` on `generated_unique_award_id`.** This satisfies the directive verbatim ("merge into the existing dataset without corrupting the bulk footprint") and is strictly better than Gen-2's union-at-read.

**Endpoint choice follows target grain — this is the key design decision.** Gen-2 used `spending_by_transaction` because its target was transaction-grain. The Gen-3 target `award_search` is **award-grain**, so the correct steady-state endpoint is **`/api/v2/search/spending_by_award/`** (one row per award), keyed on `generated_internal_id` (`CONT_AWD_…`) → maps to `award_search.generated_unique_award_id`. (The directive's `spending_by_award` instinct is right *for this target*, even though Gen-2's transaction pipeline used a different endpoint.)

**Data flow honoring the Gen-3 architecture reality (Parquet = transport, DuckDB = compute, Lance = SoR):**
```
spending_by_award (last_modified_date window, paginate limit=100)
  → write raw pages as ZSTD Parquet to s3://dex-raw-landing-zone/usaspending/award_search/api-delta/date=…/  (ephemeral transport)
  → DuckDB reads the Parquet, projects + TRY_CASTs into the rpt.award_search column set
  → Lance merge_insert into s3://data-sink/active/usaspending/award_search/  (system of record)
```

**THE corruption risk — and the mitigation.** `when_matched_update_all()` replaces the *entire* matched row with the incoming batch's columns. The API returns far fewer columns than `award_search` holds; a naive `update_all` would **NULL out every bulk-only column** on matched awards. Mitigations, in order of preference:
1. **Column-scoped update** — restrict the merge to the API-derived columns only (Lance `when_matched_update` with an explicit column set / update expression), leaving bulk-only columns intact. Preferred; no read-back.
2. **Full-schema projection with coalesce** — build the incoming batch against the *complete* `award_search` schema, back-filling non-API columns from the current row (read-modify-write on matched keys only). Daily volume is small (~14K rows/day median, Gen-2), so this is cheap if (1) proves awkward on the installed Lance version.

Either way: insert-path (`when_not_matched_insert_all`) for genuinely new awards must still supply every NOT-NULL/indexed column or pad with NULL.

**Watermark via the ops ledger (improves on Gen-2's stateless design and answers the directive's "last bulk → today" framing):**
- Source of truth = `max(feed_date)` of successful runs in a new `ops.usaspending_award_search_delta_runs` table.
- **Cold start:** window = `SNAPSHOT_DATE (2026-05-06) → yesterday`. Use the **`bulk_download/awards`** endpoint with that wide `last_modified_date` `date_range` (one async server-side CSV job) → R2 → DuckDB → `merge_insert`. Closes the entire 6-week gap in one shot. Mirrors Gen-2's `run_usaspending_daily_ingest.py`.
- **Steady state:** window = `last_success + 1 day → yesterday` (normally a single day). Use **`spending_by_award`** paginated. Mirrors Gen-2's `run_usaspending_api_daily_ingest.py`.

**Orchestration wiring:**
- New `src/trigger/usaspending_daily_delta.ts` = `schedules.task` id `usaspending-daily-delta`, **cron `0 11 * * *` UTC**. Rationale, all artifact-derived: after USAspending's ~05:00 UTC nightly ETL (Gen-2 precedent ran 06:00/08:00), and **before** the existing `award_search` consumers — `crosswalk_sam_usaspending.ts` (`0 16`) and `contractor_award_summary.ts` (`0 18`). Same dispatch+waitpoint shape as `usaspending_bulk.ts`.
- New Modal worker fn (e.g. `ingest_award_search_delta`) in the `usaspending-bulk` app (reuse `_r2_storage_options`, `_s3_client`, `_duck_configure_r2`, `_record_run`). Carry the **F5 fresh-container retry policy verbatim**: `modal.Retries(max_retries=5, backoff_coefficient=2.0, initial_delay=30.0)`.
- New ledger `pipelines/usaspending/ops_usaspending_award_search_delta_runs.sql`, modeled on [`ops_usaspending_table_runs.sql`](../../pipelines/usaspending/ops_usaspending_table_runs.sql): one row per run with `feed_date`/`window_start`/`window_end`, `rows_upserted`, `api_calls`, `status`, `error`, timestamps.

### 3.3 Implementation checklist (dependency-ordered)
1. **Validate the live `spending_by_award` field list** against the API's valid-fields (per the Gen-2 `usaspending-api-canonical-schemas` precedent — invalid `fields`/`sort` return `400`). This is the largest unknown: Gen-2 only ever pinned the `spending_by_transaction` field list.
2. **Build the response → `rpt.award_search` column map** (API display names → snake_case Postgres columns + types). Confirm the merge key mapping `generated_internal_id` → `generated_unique_award_id`. This is the core build task and the main fidelity risk.
3. Worker: paginate (limit=100, `hasNext` loop, `max_api_calls` ceiling) → land ZSTD Parquet to `dex-raw-landing-zone` → DuckDB project/cast → `merge_insert` (column-scoped) into `award_search` → `_optimize_indices`.
4. Cold-start branch via `bulk_download/awards` wide `date_range`.
5. `ops.usaspending_award_search_delta_runs` DDL + `_record_run` writes; derive watermark from it.
6. `schedules.task` + Modal retry policy + waitpoint callback.
7. Guardrail: refuse to run if a `usaspending-bulk` ingest of `award_search` is in flight (commit-conflict avoidance).

### 3.4 Open decisions for the operator
- **Merge-into-`award_search` (recommended) vs. sidecar `award_search_api_delta` + read-union (Gen-2-faithful, zero corruption risk).** The directive points at merge; sidecar is the conservative fallback if the column-map fidelity (step 2) proves lossy.
- **Award-grain `spending_by_award` (recommended) vs. transaction-grain `spending_by_transaction` + roll-up.** Award-grain aligns to the target and the merge key; transaction-grain needs an extra aggregation before merge.
- Whether to also keep a verbatim **raw-payload sidecar** (the recent Directive 28/110 "persist raw payloads verbatim" pattern) alongside the typed merge.

---

## 4. File reference index
**Legacy (hq-all/apps/data-engine-x):**
- `scripts/run_usaspending_api_daily_ingest.py` — payload `:280-301`, pagination `:288-320`, F5 retry `:200-219`, yesterday default `:579-581`, R2 key `:593-595`.
- `scripts/run_usaspending_api_daily_contracts_lance_ingest.py` — search `:327-374`, Variant-E schema `:177-274`, `merge`-less append `:581-659`, BTREE `:649-651`, new-dataset choice `:14-19,92-95`.
- `scripts/run_usaspending_daily_ingest.py` — bulk_download payload `:120-135`, window rationale `:110-118`, poll `:147-172`.
- `scripts/run_usaspending_backfill_r2_ingest.py` — archive backfill `:1-99`.

**Gen-3 (core-x):**
- `pipelines/usaspending/usaspending_bulk.py` — registry `:98-152`, index plan `:159-195`, storage opts `:212-225`, DuckDB-R2 `:246-262`, ingest/write `:656-731`.
- `src/trigger/usaspending_bulk.ts` — dispatch `:122-158`, await `:163-184`.
- `pipelines/uspto_tm/ingest.py` — `merge_insert` idiom `:863-874`.
- `src/trigger/crosswalk_sam_usaspending.ts:42-46`, `contractor_award_summary.ts:30-32` — downstream `award_search` consumers (scheduling constraint).
- `pipelines/usaspending/ops_usaspending_table_runs.sql` — ledger template.
