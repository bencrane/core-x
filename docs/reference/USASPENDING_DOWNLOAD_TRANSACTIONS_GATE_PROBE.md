# USAspending `download/transactions` — API Gate Probe (verification before the 55-day transaction-grain build)

**Date:** 2026-06-06 · **Status:** VERIFIED — gates answered; **endpoint redirect recommended before any build**
**Scope:** validate the `download/transactions` API contract against (1) the local Lance API catalog and (2) a live 3-day probe, per the gate directive. No pipeline code written; no Modal deploy. This is the pre-build gate artifact.

---

## 0. TL;DR

| Gate | Question | Verdict | Evidence |
|---|---|---|---|
| **1 — `date_type`** | Does `download/transactions` accept `last_modified_date`? | **YES — accepted.** Live POST → `HTTP 200`; server round-trips `date_type:"last_modified_date"` in the `download_request` echo. Documented in the **Transaction Search Time Period Object** (`action_date` default, `date_signed`, `last_modified_date`, `new_awards_only`). | live 200; `download/count` 200; contract `search_filters.md` |
| **2 — `limit`** | Hard cap on ZIP rows, or pagination hint? Does 55-day need chunking? | **HARD CAP — not pagination.** `limit` defaults to **and** is ceilinged at `MAX_DOWNLOAD_LIMIT = 500,000`; applied as a Django slice `source_query[:limit]` → SQL `LIMIT` (top-N, no offset/cursor). `limit=600000` → **`HTTP 422` "above max '500000'"**. Live 3-day count **66,969** → **55-day ≈ 1.23M ≈ 2.5× the cap → silent truncation. Temporal chunking would be mandatory.** | live count; live 422; `settings.py:22`; `download_generation.py:100,658` |
| **3 — unique key** | Exact unique-transaction-ID column in the CSV? | **`contract_transaction_unique_key`** (FPDS) and **`assistance_transaction_unique_key`** (FABS), both **column #0** of their prime-transaction CSVs. The directive's candidate names are **raw DB** columns, **absent** from the download CSV (see §4). | live CSV headers |
| **★ Redirect** | Is `download/transactions` the right endpoint at all? | **NO. Use `bulk_download/awards`.** It is **uncapped** (prod landed **583,776 rows / 45-day window** — already > the 500K download cap), already emits **transaction grain** (`contract_transaction_unique_key` PK, 1.17 modifications/award), supports `last_modified_date`, and is **already wrapped** by the prod-hardened `usaspending_api_landing.py`. `download/transactions` also bundles unwanted **subaward** files. | prod ledger; live grain check; `bulk_download/awards.md` |

**Bottom line:** all three gates pass for `download/transactions`, but the probe surfaced that **the contract side of the "55-day transaction-grain landing pipeline" already exists and runs** (`usaspending_api_landing.py` via the uncapped `bulk_download/awards`). Building on `download/transactions` would be a regression (500K cap → forced chunking; subaward bloat). The real build is a **two-line extension** of the existing landing (window 45→55d; add FABS assistance award types), not a new endpoint.

---

## 1. Catalog Lookup Step — what the local Lance catalog holds

**Table:** `s3://data-sink/active/usaspending_api_catalog/` (Lance, BTREE on `endpoint_path`) — built by [`usaspending_api_catalog.py`](../../pipelines/usaspending/usaspending_api_catalog.py) from the upstream `usaspending_api/api_contracts/contracts/` API-Blueprint files. **176 rows**, one per endpoint. Columns: `endpoint_path, uri_template, methods, api_version, group_name, title, request_example, request_parameters, response_example, response_schema, contract_md, contract_path, github_url, source_repo, fetched_at, response_source, response_http_status, response_sampled_at`.

**Target record — `/api/v2/download/transactions/`:**

| Field | Value |
|---|---|
| `endpoint_path` | `/api/v2/download/transactions/` |
| `methods` | `POST` |
| `title` / `group` | `Award Download` / `download` |
| `contract_path` | `usaspending_api/api_contracts/contracts/v2/download/transactions.md` |
| `github_url` | https://github.com/fedspendingtransparency/usaspending-api/blob/master/usaspending_api/api_contracts/contracts/v2/download/transactions.md |
| `response_source` | `contract` (literal example present) |

**Documented request payload** (`contract_md`): `{ columns?: string[], filters: Filters (required), file_format?: csv|tsv|pstxt, limit?: number }`. The `filters.time_period[]` entry for transaction downloads is typed as **`TransactionSearchTimePeriodObject`**.

**Catalog limitation found (relevant to Gate 1):** the `date_type` enum is **not inlined** in the catalog. The catalog ingests only files under `…/api_contracts/contracts/`; the time-period object definitions live one level up in `…/api_contracts/search_filters.md`, which is **outside** the ingested set (`rows whose contract_path mentions search_filters = 0`). The catalog gives the **reference** (`TransactionSearchTimePeriodObject`); the enum was resolved from the canonical `search_filters.md` and confirmed live. *(Optional hardening: extend the catalog builder's `CONTRACTS_MARKER` to also ingest `search_filters.md` so the date_type enum is queryable in-catalog.)*

---

## 2. Gate 1 — `date_type: last_modified_date` support

**Contract (canonical `search_filters.md` → Transaction Search Time Period Object) — `date_type` members:**
`action_date` (default) · `date_signed` (mapped to `award_date_signed` for transactions) · **`last_modified_date`** · `new_awards_only`.

**Live (3-day window `2026-06-01 … 2026-06-03`):**
- `POST /api/v2/download/transactions/` with `time_period:[{…,"date_type":"last_modified_date"}]` → **`HTTP 200`**. Echoed `download_request.filters.time_period = [{"date_type":"last_modified_date","start_date":"2026-06-01","end_date":"2026-06-03"}]` — accepted and **not** silently rewritten to `action_date`.
- `POST /api/v2/download/count/` (same window, `spending_level:"transactions"`) → `HTTP 200`, `calculated_transaction_count: 66969`.

**Verdict: PASS.** `last_modified_date` is a first-class `date_type` for transaction downloads. (This matches the existing award pipeline, which already windows on `last_modified_date` for the documented reason: USAspending lags ~7+ days between action and warehouse landing, and DoD/FPDS publishes on a ~90-day delay — an `action_date` window misses late-landing records.)

---

## 3. Gate 2 — `limit` ceiling & truncation behavior

**Mechanics (upstream source):**
- `settings.py:22` → `MAX_DOWNLOAD_LIMIT = 500000`.
- `download/v2/request_validations.py` → every download validator declares `limit` with `{"max": MAX_DOWNLOAD_LIMIT, "default": MAX_DOWNLOAD_LIMIT}`. **Omit `limit` → it is injected as 500,000.**
- `download/filestreaming/download_generation.py:100` → `if limit > MAX_DOWNLOAD_LIMIT: raise` ("Unable to process this download because it includes more than the current limit of 500000 records").
- `download_generation.py:658-659` → `if limit: source_query = source_query[:limit]` — a Django QuerySet slice → SQL `LIMIT`. **Top-N truncation; there is no `OFFSET`/cursor.**

**Live confirmation:**
- `maximum_transaction_limit = 500000` (returned by `download/count`).
- `limit = 600000` → **`HTTP 422`: `Field 'limit' value '600000' is above max '500000'`** — the ceiling **rejects**, it does not clamp.
- 3-day `last_modified_date` transaction count = **66,969** → **55-day ≈ 1,227,765 rows ≈ 2.5×** the 500K cap.

**Verdict: `limit` is a HARD CAP on total rows in the generated ZIP — NOT a pagination hint.** There is no mechanism to page past it. A window whose natural result exceeds 500K is **silently truncated to the first 500,000 rows** (no error, no truncation flag in the status — `total_rows` reports the capped 500K, not the true count) when relying on the default.

**Consequence for a 55-day window on `download/transactions`:** **temporal chunking would be mandatory** — minimum 3 chunks (~18 days each) at average volume, but volume is spiky (DoD ~90-day FPDS publishes land as `last_modified` bursts; FY/quarter boundaries spike), so a safe fixed cadence is **7-day chunks (~156K–190K rows, ~2.7× headroom)**, each pre-flighted with `POST /download/count` asserting `rows_gt_limit == false` before committing the async job. **— But see §5: the correct fix is to not use `download/transactions` at all.**

*(Note: in the `download/count` response, prefer the generic `calculated_count` / `maximum_limit` / `rows_gt_limit`; the `*_transaction_*` variants are flagged deprecated in the live `messages`.)*

---

## 4. Gate 3 — transaction unique key extraction

The live 3-day ZIP (`SubawardsAndPrimeTransactions_…zip`) contained **4 CSVs** — `download/transactions` bundles prime transactions **and subawards**, split by award category:

| CSV member | cols | unique-key column (col #0) | sample value |
|---|---|---|---|
| `Contracts_PrimeTransactions` | **297** | **`contract_transaction_unique_key`** | `1100_4732_11316020F0003OMB_0_GS00Q17NSD3006_0` |
| `Assistance_PrimeTransactions` | 112 | **`assistance_transaction_unique_key`** | `12C3_NR208C30XXXXC005_-NONE-_10.905_-NONE-` |
| `Contracts_Subawards` | 118 | `prime_award_unique_key` (subaward grain — not wanted) | — |
| `Assistance_Subawards` | 113 | `prime_award_unique_key` (subaward grain — not wanted) | — |

**Reconciliation with the directive's candidate names (these are raw DB columns, NOT present in the download CSV):**

| Directive candidate | What it is | Surfaces in download CSV as |
|---|---|---|
| `detached_award_proc_unique` | FPDS (contract) DB transaction PK | **`contract_transaction_unique_key`** |
| `afa_generated_unique` | FABS (assistance) DB transaction PK | **`assistance_transaction_unique_key`** |
| `transaction_unique_id` | internal `transaction_search` PK | **not emitted by the download endpoint** |

**Verdict.** The downstream dedup key is **split by award category** (the two keys are disjoint — a transaction row is contract *or* assistance). A unified transaction mirror keys on **`COALESCE(contract_transaction_unique_key, assistance_transaction_unique_key)`**. The contract key is a **proven, fully-unique PK at scale**: in the latest prod landing, **583,776 rows = 583,776 distinct `contract_transaction_unique_key`** (§5). Assistance-key uniqueness-at-scale is to be confirmed when FABS is added (verified present + populated in the live probe).

---

## 5. ★ Architectural finding — use `bulk_download/awards`, not `download/transactions`

The probe also evaluated the endpoint the existing prod pipeline uses. Three grounded facts redirect the build:

**(a) `bulk_download/awards` is UNCAPPED — verified in prod.**
Latest `ops.usaspending_award_search_api_landing_runs` success: **583,776 rows landed for a 45-day `last_modified_date` window** — already **above** the 500K cap that `download/transactions` enforces. The contract carries **no `limit` field**. (`bulk_download/awards.md`; ledger.)

**(b) `bulk_download/awards` already emits TRANSACTION grain.**
Its output is the `…_PrimeTransactions` file set (one row per FPDS modification / FABS action). Grain check on the latest landing `s3://data-sink/usaspending_api_landings/award_search/pull_date=2026-06-04/`:
`rows = 583,776 · distinct contract_transaction_unique_key = 583,776 (unique PK ✓) · distinct contract_award_unique_key = 497,168 · rows/award = 1.17` → **transaction grain, not award summary.** The 297-column schema is **identical** to the live `Contracts_PrimeTransactions` CSV from the `download/transactions` probe — same data, no 500K cap, no subaward bloat.

**(c) The infra is already prod-hardened.** [`usaspending_api_landing.py`](../../pipelines/usaspending/usaspending_api_landing.py) wraps `bulk_download/awards` with: F5-BotDefense fresh-container retry (`modal.Retries(5, ×2, 30s)` so a persistent 429 recycles the egress IP), R2-multipart-safe fragment sizing (`max_rows_per_file=250_000`), and the `ops.*` audit ledger.

**Therefore the "55-day transaction-grain landing pipeline" for the contract side already exists.** The genuine deltas to reach the directive's target:
1. `WINDOW_DAYS` 45 → 55 (one constant).
2. Add **FABS assistance** award type codes (`02,03,04,05,06,…`) to `PRIME_CONTRACT_TYPES` for full transaction coverage (currently contracts-only `A,B,C,D`) — and split the landing by category (the FABS schema differs: 112 vs 297 cols), keyed `COALESCE(contract_transaction_unique_key, assistance_transaction_unique_key)`.

`download/transactions` offers **no benefit** here and **two penalties**: the 500K cap (forces chunking) and subaward-file bloat. The legacy Gen-2 transaction pipeline's `limit:100` hard cap (see `DIRECTIVE_33`) was the **`spending_by_transaction`** paginated-search endpoint (10K total ceiling) — a different, also-unsuitable path; the file-drip correctly used `bulk_download/awards`.

---

## 6. Reconciliation note — API-landing grain (flag for the rebuild plan)

[`USASPENDING_SUBSYSTEM_REBUILD_PLAN.md`](../plans/USASPENDING_SUBSYSTEM_REBUILD_PLAN.md) states (§ "Transaction-grain mirror: NO"): *"The API landing is award-grain (`bulk_download/awards`), so there is no fresh transaction feed to union."* **Empirically the API landing is transaction grain** (1.17 rows/award; `contract_transaction_unique_key` is the unique PK; `…_PrimeTransactions` schema). Two distinct datasets share the `award_search` name and should not be conflated:

| Dataset | Source | Grain | Rows |
|---|---|---|---|
| `active/usaspending/award_search/` (bulk SoR) | monthly 175 GB `rpt.award_search` pg_dump | **award** (1 row/award) | 78.4M |
| `usaspending_api_landings/award_search/` (API landing) | `bulk_download/awards` PrimeTransactions | **transaction** (1.17 rows/award) | ~584K/45d |

The premise "no fresh transaction feed exists" does not hold for **prime contracts** — a fresh prime-contract-transaction feed is already landing daily-able. Operator's call whether this reopens the transaction-mirror decision; flagged here with evidence, not actioned.

---

## 7. Evidence appendix (reproducible)

- Catalog query: `doppler run -p core-x -c prd -- uv run --no-project --with pylance --with pyarrow --with duckdb python3 /tmp/usaspending_catalog_query.py`
- Live gate probe (requests-only): `uv run --no-project --with requests python3 /tmp/usaspending_gate_probe.py`
  - count 200 → `calculated_transaction_count=66969`, `maximum_transaction_limit=500000`
  - download/transactions 200 (last_modified_date) → ZIP `22,589 B`, 4 CSV members, `total_columns=640` (sum across files), prime-transaction keys at col #0
  - `limit=600000` → `422 Field 'limit' value '600000' is above max '500000'`
- Prod ledger + grain: `ops.usaspending_award_search_api_landing_runs` latest = `583,776 rows / 45d / success`; landing grain `rows=distinct_txn_key=583,776`, `rows/award=1.17`.
- Upstream source pins: `settings.py:22` (`MAX_DOWNLOAD_LIMIT=500000`); `download_generation.py:100,658-659` (raise + slice); `search_filters.md` Transaction Search Time Period Object.

**Gate directive satisfied. Do not implement on `download/transactions`; the build is an extension of `usaspending_api_landing.py` (`bulk_download/awards`).**
