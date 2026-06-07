# USAspending 90-Day Rolling Ingest — Reconnaissance Diagnostic

**Mode:** READ-ONLY recon. No ingestion code written, no DDL, no `write_dataset`, no migration.
**Date:** 2026-06-06 · **Plane:** core-x Gen-3 (CSV/Parquet transport → DuckDB compute → LanceDB SoR on R2).
**Tooling:** `pylance≥7 / pyarrow≥17 / duckdb≥1.5` via `doppler run -p core-x -c prd`, against the live R2 SoR and the `ops.*` Postgres ledgers.
**Purpose:** establish the exact technical path for a 90-day-rolling / daily-delta USAspending ingest **before** any pipeline code is written, sourced from the live API-endpoint catalog and the live repo footprint — not from memory.

---

## 0. Verdict summary (read first)

| Question | Verdict |
|---|---|
| **Best endpoint for a rolling delta pull?** | **`POST /api/v2/bulk_download/awards/`** — the only date-bounded contract endpoint that is *uncapped* (async server-side CSV) **and** documents `date_type: last_modified_date` in its own contract. Every paginated alternative truncates at this volume (§1.3, §4). |
| **Exact temporal-filter payload?** | A `last_modified_date` `date_range` over the trailing window + `prime_award_types:["A","B","C","D"]`. Verbatim JSON in §2.1. **`last_modified_date`, not `action_date`** — the warehouse lags 7+ days (90 for DoD), so an `action_date` daily window returns ≈0 rows (§2.3). |
| **Is the ingest code already there?** | **Partially — the API tier already exists and works.** `usaspending_api_landing.py` wraps exactly this endpoint and landed **583,776 rows** on 2026-06-04. The dead in-SoR `usaspending_daily_delta.py` is **already removed** from the checkout. Full inventory §3. |
| **Rate limits / pagination to respect?** | No API key, **no numeric rate limit** — throttling is **per source IP** (F5 BotDefense). `bulk_download/awards` is uncapped but async (submit → poll `status` ≤60 min → download ZIP). The paginated/`download` endpoints carry hard caps (10K and 500K) that forbid their use here. Full matrix §4. |
| **⚠️ Does a 7-day lookback actually catch delayed reporting?** | **Only if runs never slip.** A *fixed* 7-day window with **no watermark** loses any record stamped during a >7-day cron outage. The deployed pipeline uses **45 days** for exactly this robustness. Reconcile before narrowing to 7 (§2.4, §5). |

---

## 1. Catalog interrogation — the date-bounded endpoint surface

### 1.1 The catalog (system of record for the API contract surface)

`s3://data-sink/active/usaspending_api_catalog/` — **Lance, BTREE on `endpoint_path`, 176 rows, 18 columns**, built by [`usaspending_api_catalog.py`](../pipelines/usaspending/usaspending_api_catalog.py) from the upstream `usaspending_api/api_contracts/contracts/` API-Blueprint files (the contracts the API is *built from*, not a docs scrape).

Columns: `endpoint_path, uri_template, methods, api_version, group_name, title, request_example, request_parameters, response_example, response_schema, contract_md, contract_path, github_url, source_repo, fetched_at, response_source, response_http_status, response_sampled_at`.

Group census (176): `search 26 · references 24 · agency 21 · disaster 20 · autocomplete 19 · download 11 · awards 8 · federal_accounts 8 · recipient 8 · idvs 7 · reporting 7 · v2 6 · bulk_download 4 · …`.

### 1.2 Candidate endpoints for a date-bounded / delta / award-detail pull

Pulled directly from the catalog. The temporal lever is the **Time Period Object** (`date_type` ∈ `action_date` (default) · `date_signed` · `last_modified_date` · `new_awards_only`).

| Endpoint | Method | Group | Temporal filter | Cap / mechanic | Verdict for rolling delta |
|---|---|---|---|---|---|
| **`/api/v2/bulk_download/awards/`** | POST | `bulk_download` | `date_type` + `date_range{start,end}` — **`last_modified_date` documented in-contract** | **Uncapped**; async CSV job + `bulk_download/status` poll | ✅ **USE THIS** |
| `/api/v2/search/spending_by_award/` | POST | `search` | `time_period[{start,end}]` (no `date_type` in example) | Paginated `limit`/`page`; **10,000-record total ceiling** | ❌ truncates (§4) |
| `/api/v2/search/spending_by_transaction/` | POST | `search` | `time_period[]` | Paginated `limit` (≤100)/`page`; **10,000 ceiling** | ❌ truncates |
| `/api/v2/download/transactions/` | POST | `download` | `date_type` (Transaction Time Period Obj) | **Hard cap `MAX_DOWNLOAD_LIMIT=500,000`**, top-N truncation; **bundles subawards** | ❌ cap + bloat |
| `/api/v2/download/awards/` | POST | `download` | `date_type` | Hard cap 500,000 | ❌ cap |
| `/api/v2/download/count/` | POST | `download` | `time_period[]` | Returns `maximum_limit:500000`, `is_over_limit` bool | ◾ optional pre-flight only |
| `/api/v2/bulk_download/status` | GET | `bulk_download` | `{?file_name}` | async job poll | ◾ companion to the chosen endpoint |
| `/api/v2/awards/{award_id}/` | GET | `awards` | response carries `last_modified_date`, `date_signed` | one award/call | ◾ Stage-2 enrichment only |

**Temporal-lever census** — endpoints whose *own* contract markdown references a delta lever (the catalog ingests only `…/contracts/`, so the shared `search_filters.md` enum is not inlined — `last_modified_date` is therefore under-counted for the `download/*` endpoints, but is **explicit** for `bulk_download/awards`):

```
POST  /api/v2/bulk_download/awards/        levers=[action_date, date_type, last_modified_date]   ← only uncapped + last_modified_date
POST  /api/v2/download/awards/             levers=[action_date, date_type]
POST  /api/v2/download/transactions/       levers=[action_date, date_type]
GET   /api/v2/awards/{award_id}/           levers=[date_signed, last_modified_date]   (response fields, not a filter)
POST  /api/v2/federal_accounts/{code}/program_activities/total   levers=[date_type]   (unrelated — agency reporting)
```

### 1.3 Selected endpoint — `bulk_download/awards` (and why every alternative fails)

`bulk_download/awards` is the **only** endpoint that simultaneously satisfies all three requirements of a rolling delta at federal volume:

1. **Date-bounded on the warehouse-modification stamp.** Its contract documents `date_type: last_modified_date` directly (§1.2). This is the lever that catches late-landing and re-modified records.
2. **Uncapped.** Its contract carries **no `limit` field** and no row-cap prose (verified — the only `limit`-class line in the contract is "used by the Custom Award Data Download page"). Prod proof: **583,776 rows** landed for a single 45-day window — already **above** the 500K ceiling that `download/transactions` would have silently truncated to.
3. **Asynchronous server-side generation.** Submit → poll `bulk_download/status{?file_name}` → download a ZIP of CSV member(s). No client-side pagination, no 10K page-depth wall.

The paginated `search/spending_by_*` endpoints hard-stop at **10,000 records** (`page × limit`); a 7-day prime-contract window is ≈156K transactions / ≈77K awards (§2.2) → **truncated to 10K → unusable**. The `download/*` endpoints cap at **500,000** and `download/transactions` additionally bundles unwanted subaward files. `bulk_download/awards` is the unambiguous choice and is already the one the deployed landing tier uses.

**Documented request schema (catalog `request_example`, verbatim):** keys `filters{ prime_award_types[], date_type, date_range{start_date,end_date}, agencies[] }, file_format`. Full `prime_award_types` enum offered by the contract: `A, B, C, D, IDV_A, IDV_B, IDV_B_A, IDV_B_B, IDV_B_C, IDV_C, IDV_D, IDV_E` (contracts/IDVs) + `02–11, -1` (assistance). Contract permalink: <https://github.com/fedspendingtransparency/usaspending-api/blob/master/usaspending_api/api_contracts/contracts/v2/bulk_download/awards.md>.

---

## 2. The exact temporal-filter payload (7-day rolling lookback)

### 2.1 Submit payload — `POST https://api.usaspending.gov/api/v2/bulk_download/awards/`

Parameterized (`{window_start} = today − 7d`, `{window_end} = today`, both inclusive, UTC):

```json
{
  "filters": {
    "prime_award_types": ["A", "B", "C", "D"],
    "date_type": "last_modified_date",
    "date_range": {
      "start_date": "{window_start}",
      "end_date":   "{window_end}"
    }
  },
  "file_format": "csv"
}
```

Concrete, as-of the diagnostic date (2026-06-06):

```json
{
  "filters": {
    "prime_award_types": ["A", "B", "C", "D"],
    "date_type": "last_modified_date",
    "date_range": { "start_date": "2026-05-30", "end_date": "2026-06-06" }
  },
  "file_format": "csv"
}
```

**Field rationale (each key traces to a requirement):**
- `prime_award_types: ["A","B","C","D"]` — **contracts only** (A=definitive, B=purchase order, C=delivery order, D=BPA call). Excludes IDV vehicles and all assistance. This is "contract records." To include IDVs add the `IDV_*` codes; for assistance add `02–11`.
- `date_type: "last_modified_date"` — windows on the **warehouse modification stamp**, which is set on **both insert and update**. This single lever is what "isolates only newly modified or created contract records." *(Created-only, excluding modifications, is the separate `date_type:"new_awards_only"` member of the Time Period Object — recommend `last_modified_date` per the directive's "modified **or** created"; `new_awards_only` is the alternative if pure net-new is later wanted, pending a live field-accept check since it is not literally enumerated in this endpoint's contract.)*
- `date_range` — inclusive `[start,end]` on `last_modified_date`. The trailing 7-day lookback.
- `file_format: "csv"` — the landed verbatim form (read all-VARCHAR downstream).
- **No `limit` key** — the endpoint is uncapped; supplying one is unnecessary and there is no pagination cursor to respect.

### 2.2 The async retrieval mechanic (this is not a single request)

`bulk_download/awards` is a **job-submit + poll + download** sequence — the future script must implement all three:

```
1. POST  /api/v2/bulk_download/awards/   (payload above)
         → 200 → { file_name, file_url, status_url }   (429 ⇒ throttled, see §4.3)
2. GET   /api/v2/bulk_download/status?file_name={file_name}     every 15s
         → poll until status == "finished"   (status ∈ ready|running|finished|failed)
         → hard ceiling: 60 min, then treat as failure (retry on a fresh IP)
3. GET   {file_url}   → stream the ZIP → extract *.csv member(s)
```

**Volume sizing for the 7-day window** (so the cap analysis is grounded): a live 3-day `last_modified_date` transaction count returned **66,969** (≈22.3K/day) → **7-day ≈ 156K transactions ≈ 77K distinct awards**. Comfortably handled by the uncapped async job; fatal to any 10K-ceiling paginated endpoint.

### 2.3 Why `last_modified_date`, never `action_date` (the load-bearing correctness point)

USAspending's warehouse lags the real contract action by **7+ days** (civilian) and DoD/FPDS publishes prime detail on a **deliberate ~90-day delay**. Consequences:

- An **`action_date`** daily window asks "what was *actioned* yesterday" → the warehouse hasn't landed it yet → **≈0 rows/day**. This is the documented reason the legacy and current pipelines both reject `action_date` for the delta.
- A **`last_modified_date`** window asks "what did the *warehouse stamp* in the last N days" → captures new awards, corrections to old awards, and — critically — a 90-day-late DoD action **on the day it finally publishes** (its `last_modified_date` is the landing date, not the 90-day-old `action_date`). This is precisely "newly modified or created."

### 2.4 ⚠️ 7-day window adequacy — the caveat the directive must absorb

`last_modified_date` semantics mean a 7-day lookback **is** sufficient to catch even 90-day-late DoD publishes — *provided the pull runs at least once every 7 days*. The exposure is **operational, not temporal**:

- The deployed landing tier uses a **fixed rolling window with NO watermark** (§3). A fixed 7-day window that misses >7 consecutive daily runs **permanently loses** every record stamped in the un-covered gap — there is no state to back-fill from.
- The deployed window is **45 days** precisely for this robustness: it tolerates ~38 days of consecutive missed runs before data loss, at the cost of re-pulling overlap daily (harmless — the dedup is downstream).

**Two ways to honor the 7-day directive without the fragility:**
1. **Keep a fixed window, widen the safety margin** (the deployed 45-day choice). A 7-day lookback is the *minimum*; 45 days is the *robust* setting. Re-pull overlap is free.
2. **Watermark-drive the window**: `window_start = min(today − 7d, last_successful_window_end − 1d)`; `window_end = today`. Steady state = 7 days; auto-widens to close any gap after an outage. Requires promoting the audit ledger to a watermark (it is explicitly *not* one today — §3).

Recommendation: **option 2** if 7-day is a hard requirement (gives the narrow steady-state window the directive wants *and* gap-safety); otherwise leave the deployed 45-day fixed window, which already over-covers a 7-day lookback.

### 2.5 Optional pre-flight — `POST /api/v2/download/count/`

Not required (the target endpoint is uncapped), but a cheap guard to assert the window is non-empty / sized as expected before committing the async job:

```json
{ "filters": { "prime_award_types": ["A","B","C","D"],
               "date_type": "last_modified_date",
               "date_range": { "start_date": "{window_start}", "end_date": "{window_end}" } },
  "spending_level": "awards" }
```
→ returns `calculated_count`, `maximum_limit: 500000`, `is_over_limit` (bool).

---

## 3. Codebase footprint — existing USAspending ingestion inventory

Five live modules under [`pipelines/usaspending/`](../pipelines/usaspending/). **The dead in-SoR daily-delta (`usaspending_daily_delta.py` + `.ts` + its ledger DDL) has already been removed** from this checkout — `find` returns no match; `DIRECTIVE_33` is marked SUPERSEDED/ABANDONED.

### 3.1 Ingest / build modules

| Module | Role | Endpoint / source | Write target (R2) | Grain | Cadence |
|---|---|---|---|---|---|
| [`usaspending_api_landing.py`](../pipelines/usaspending/usaspending_api_landing.py) | **The API delta tier (this directive's foundation)** | `POST bulk_download/awards`, **45-day** rolling `last_modified_date` | `s3://data-sink/usaspending_api_landings/award_search/pull_date=YYYY-MM-DD/` (Lance, dated, immutable, verbatim 297-col, 250K-row frags, **no dedup**) | transaction (1.17 txn/award) | **Manual / dispatcher — NO cron wired** |
| [`usaspending_bulk.py`](../pipelines/usaspending/usaspending_bulk.py) | Monthly heavy base | USAspending **pg_dump** (161 GiB), snapshot `2026-05-06` | `s3://data-sink/active/usaspending/<table>/` ×51 | per-table (award_search = award grain) | Manual ([`usaspending_bulk.ts`](../src/trigger/usaspending_bulk.ts)) |
| [`usaspending_api_catalog.py`](../pipelines/usaspending/usaspending_api_catalog.py) | API contract catalog (guard, not ingest) | GitHub `api_contracts/contracts/` tarball | `s3://data-sink/active/usaspending_api_catalog/` (176 rows) | one row/endpoint | Manual |
| [`contractor_award_summary.py`](../pipelines/usaspending/contractor_award_summary.py) | Per-UEI lifetime rollup | reads `usaspending/award_search` (+subaward) | `s3://data-sink/active/contractor_award_summary/` | one row/UEI | [`contractor_award_summary.ts`](../src/trigger/contractor_award_summary.ts) cron **`0 18 * * *`** |
| [`ffata_exec_comp.py`](../pipelines/usaspending/ffata_exec_comp.py) | FFATA exec-comp sidecar | FFATA | derived | — | [`ffata_exec_comp.ts`](../src/trigger/ffata_exec_comp.ts) |

Plus the SAM↔USAspending crosswalk consumer: [`crosswalk_sam_usaspending.ts`](../src/trigger/crosswalk_sam_usaspending.ts) cron **`0 16 * * *`** (reads `award_search`).

### 3.2 What is actually running today (cron-active vs manual)

| Job | Trigger | Cadence | Hits the API? |
|---|---|---|---|
| `crosswalk_sam_usaspending` | `crosswalk_sam_usaspending.ts` | `0 16 * * *` UTC | No (reads Lance) |
| `contractor_award_summary` | `contractor_award_summary.ts` | `0 18 * * *` UTC | No (reads Lance) |
| `ffata_exec_comp` | `ffata_exec_comp.ts` | (see file) | FFATA only |
| **`usaspending_api_landing`** | — | **none — manual `modal run` only** | **Yes — `bulk_download/awards`** |
| `usaspending_bulk` | `usaspending_bulk.ts` | manual/dispatcher | No (pg_dump) |

**The single most important footprint fact:** the API delta endpoint the directive targets is **already wrapped, prod-hardened, and proven** (`usaspending_api_landing.py`), but **has no cron** — it has been run manually exactly once at scale. Wiring a daily cron on this module *is* the 90-day-rolling/daily-delta build; no new API client is required.

### 3.3 Active data frontier (live ledgers, as-of 2026-06-06)

| Tier | Ledger | Latest state |
|---|---|---|
| Bulk SoR `award_search` | `ops.usaspending_table_runs` | snapshot **2026-05-06**, **78,373,286 rows**, success **2026-06-01** |
| API landing | `ops.usaspending_award_search_api_landing_runs` | `pull_date=`**2026-06-04**, window `2026-04-20 → 2026-06-04` (45d), **583,776 rows × 297 cols**, 166 polls, success |

The landing ledger is **audit-only, explicitly NOT a watermark** ([DDL comment](../pipelines/usaspending/ops_usaspending_award_search_api_landing_runs.sql)): "the landing window is a fixed rolling `[today − WINDOW_DAYS … today]`; runs overlap by design." This is the exact gap §2.4 flags for a fixed 7-day window.

---

## 4. API rate limits & pagination boundaries the ingestion script must respect

### 4.1 Per-endpoint cap matrix

| Endpoint | Auth | Cap / pagination | Behavior at the boundary |
|---|---|---|---|
| **`bulk_download/awards`** (chosen) | none | **Uncapped.** Async server-side CSV. No `limit`, no cursor. | Whole window in one job; ZIP may contain multiple CSV members. |
| `bulk_download/status` | none | GET `{?file_name}`; poll | `status ∈ ready\|running\|finished\|failed`. |
| `search/spending_by_award` | none | `limit` ≤ 100/page, `page`, `page_metadata.hasNext`; **10,000-record total ceiling** | Past 10K → blocked/truncated. **Disqualifies it here.** |
| `search/spending_by_transaction` | none | `limit` ≤ 100/page; **10,000 ceiling** | Same. |
| `download/transactions` | none | **`MAX_DOWNLOAD_LIMIT = 500,000`**, Django top-N slice (no OFFSET) | `limit > 500000` → **HTTP 422**; default omitted → **silently truncated to 500K**. |
| `download/awards` | none | 500,000 hard cap | Same. |
| `download/count` | none | n/a (returns the cap) | `maximum_limit: 500000`, `is_over_limit` bool. |

### 4.2 Pagination the script must implement

`bulk_download/awards` has **no row pagination**. The control loop is the **async poll**, not a page cursor:
- Poll `bulk_download/status` every **15 s**, ceiling **60 min**, until `finished` (then `failed` → retry).
- Download the ZIP; iterate **all** `*.csv` members (a wide window splits into several).
- On the R2 write side, land at **≤ 250,000 rows/fragment** (`LANDING_MAX_ROWS_PER_FILE`) so each Lance data fragment stays under the size at which the object-store writer escalates its multipart part size mid-upload — which **R2 rejects (400 `InvalidPart`)**. This is a hard, proven constraint on this plane, not a tuning knob.

### 4.3 Rate limiting — there is no numeric limit; it is IP-based (F5 BotDefense)

USAspending requires **no API key** and publishes **no per-key rate limit**. Throttling is enforced **by source IP** via F5 BotDefense:
- A persistent **HTTP 429** does **not** recover via in-script backoff — only a **fresh egress IP** clears it.
- The deployed pattern (carry it verbatim into any new worker): a persistent 429 **raises out of the fetch phase** so `modal.Retries(max_retries=5, backoff_coefficient=2.0, initial_delay=30.0)` recycles the container = **fresh Modal IP**. Transient 429s with a `Retry-After` get one in-script sleep.
- Politeness spacing for any *synchronous* catalog/sampling calls: ~1.2 s between requests (the catalog builder's `SAMPLE_DELAY_SECONDS`). The async `bulk_download` job needs only the 15 s status-poll spacing.

### 4.4 Single-writer / scheduling discipline (not a rate limit, but a hard operational bound)

core-x has **no commit-lock helper**. The landing tier writes to an **immutable, dated, isolated prefix** (`usaspending_api_landings/award_search/pull_date=…/`), so it can never race the bulk SoR or the crosswalk — re-running a `pull_date` overwrites that one partition idempotently. Any future cron must preserve this isolation: **never** point the delta writer at `active/usaspending/award_search/` (that was the dead delta's fatal design). De-collide the cron slot from the bulk ingest and from the 16:00/18:00 consumers; land at ~09:00 UTC (after USAspending's ~05:00 ETL, before the consumers).

---

## 5. Reconciliation — 7-day directive vs 45-day deployed vs 90-day strategy

| Window | Where it comes from | What it buys | Risk |
|---|---|---|---|
| **7-day lookback** (directive §3) | this directive | Minimal daily re-pull volume (~156K txns) | Fixed + no watermark → **data loss on any >7-day outage** (§2.4) |
| **45-day rolling** (deployed) | `usaspending_api_landing.py` `WINDOW_DAYS=45` | Tolerates ~38-day outage; re-pull overlap free | Larger daily volume (583K txns) — harmless (downstream dedup) |
| **90-day strategy** (directive header) | the overall horizon | Coverage/retention target of the rolling system | Not a per-pull window — it is the dedup-mirror's horizon, not the API call's |

The three are **not** in conflict once separated: the **90-day** is the *strategy horizon* (how far back the resolved/deduped served view stays fresh), the **7-day** is the *daily pull window*, and **45-day** is the *deployed pull window* that already over-satisfies a 7-day lookback. The only real decision the directive forces is **§2.4**: narrow the deployed window 45→7 (and then add a watermark to stay gap-safe), or leave 45 and treat "7-day lookback" as already-covered. **Do not ship a fixed 7-day window without a watermark** — that is strictly more fragile than what is already deployed.

The downstream dedup/resolve layer (the `usaspending_mirror/award` mirror, planned in [`USASPENDING_SUBSYSTEM_REBUILD_PLAN.md`](plans/USASPENDING_SUBSYSTEM_REBUILD_PLAN.md)) is what turns the overlapping daily landings into a clean 90-day-fresh served surface, keyed on `generated_unique_award_id` with `max(last_modified_date)` precedence. The delta pull's only job is to **land the window verbatim**; correctness of the 90-day picture is the mirror's job, not the API call's.

---

## 6. Evidence appendix (reproducible, read-only)

```bash
# Catalog interrogation (176 rows; delta-endpoint extract; temporal-lever census)
doppler run -p core-x -c prd -- uv run --no-project \
  --with 'pylance>=7' --with 'pyarrow>=17' --with 'duckdb>=1.5' \
  python3 /tmp/usaspending_catalog_probe.py

# Full bulk_download/awards + download/count contract dump (request schema, cap prose)
doppler run -p core-x -c prd -- uv run --no-project \
  --with 'pylance>=7' --with 'pyarrow>=17' python3 /tmp/usaspending_bulkdl_detail.py

# Live ledgers (frontier + bulk vintage)
doppler run -p core-x -c prd -- bash -c \
  'psql "$HQX_DB_URL_DIRECT" -c "SELECT pull_date,window_start,window_end,rows_landed,columns_landed,status
     FROM ops.usaspending_award_search_api_landing_runs ORDER BY id DESC LIMIT 5;"'
doppler run -p core-x -c prd -- bash -c \
  'psql "$HQX_DB_URL_DIRECT" -c "SELECT snapshot_date,table_name,rows_processed,status,completed_at::date
     FROM ops.usaspending_table_runs WHERE table_name='\''award_search'\'' AND status='\''success'\'' ORDER BY id DESC LIMIT 2;"'
```

**Cross-references:** [`USASPENDING_SUBSYSTEM_REBUILD_PLAN.md`](plans/USASPENDING_SUBSYSTEM_REBUILD_PLAN.md) (target mirror architecture), [`USASPENDING_DOWNLOAD_TRANSACTIONS_GATE_PROBE.md`](reference/USASPENDING_DOWNLOAD_TRANSACTIONS_GATE_PROBE.md) (500K-cap + endpoint-redirect proof), [`DIRECTIVE_33_USASPENDING_DAILY_DELTA_PORT.md`](reference/DIRECTIVE_33_USASPENDING_DAILY_DELTA_PORT.md) (ABANDONED in-SoR delta — do not implement).

**Recon complete. No ingestion code written. The path is: cron the existing `usaspending_api_landing.py` (`bulk_download/awards`, `last_modified_date`), choose the window per §2.4, and resolve to 90-day-fresh via the planned mirror.**
