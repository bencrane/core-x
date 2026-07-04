# USAspending Awards API — Endpoints, Payloads & the Grain Trap

> **Verified 2026-07-03** against the live Lance API catalog (`s3://data-sink/active/usaspending_api_catalog/`, 176 rows · 1/endpoint) **+ live USAspending API calls** (`https://api.usaspending.gov`, public, no auth). Every request/response payload below is either quoted from the upstream API-Blueprint contract (via the catalog) or captured from a live call this session. Async-job `file_name`s are live artifacts of this session's submissions.

**Purpose.** Resolve the recurring "the *awards* endpoint returns *transaction* rows" confusion, prove it is correct-by-design, and enumerate the actual award-grain API surface with the payloads each endpoint returns. This is the endpoint/grain companion to [`FPDS_CANONICAL_FIELD_DICTIONARY.md`](./FPDS_CANONICAL_FIELD_DICTIONARY.md) (the txn-spine column contract) and [`00_ACTIVE_SINK_CATALOG.md`](./00_ACTIVE_SINK_CATALOG.md) (the SoR inventory).

**Provenance.** Endpoint facts derive from the Lance catalog built by [`pipelines/usaspending/usaspending_api_catalog.py`](../../pipelines/usaspending/usaspending_api_catalog.py) — one row per endpoint, parsed from `github.com/fedspendingtransparency/usaspending-api` `usaspending_api/api_contracts/`. The affected pipeline is [`pipelines/usaspending/usaspending_api_fresh.py`](../../pipelines/usaspending/usaspending_api_fresh.py).

---

## 1. TL;DR — the issue

- `pipelines/usaspending/usaspending_api_fresh.py` POSTs to `bulk_download/awards/` yet lands `contract_prime_txn`, which is **TRANSACTION**-grained. An "awards" endpoint emitting transaction rows *looks* like a grain bug.
- **It is not a bug.** In USAspending's API, `awards` in `bulk_download/awards` is the **data-domain namespace** (the prime-award domain; sibling `bulk_download/accounts` is the account domain), **not a row-grain promise**.
- `bulk_download/awards` is an **async file-generation job**: `POST` returns a `file_url`; you poll `download/status`; you receive a ZIP. **Grain is a property of which CSV member of the ZIP you parse**, not of the endpoint.
- Live-confirmed this session: `bulk_download/awards` with `prime_award_types` produces a ZIP named `All_PrimeTransactions_*.zip` containing **one** member, `All_Contracts_PrimeTransactions_*.csv` — **297 columns, transaction grain**. There is **no** `PrimeAwardSummaries` member in that download. The fresh leg reading it therefore *correctly* gets transaction grain.
- **Correction to the standing recommendation:** the award-summary member (`*_PrimeAwardSummaries_*`) is **not** produced by `bulk_download/awards`. It is produced by the row-level **`download/awards`** endpoint (ZIP `PrimeAwardSummariesAndSubawards_*.zip`) and by `download/search` with `spending_level:["awards"]`. Building the award spine's fresh leg means **switching endpoint**, not merely extracting a different member from the same download (§9).
- Grain is controlled by three distinct mechanisms depending on the endpoint: the modern `spending_level` enum, the legacy `subawards` boolean, and — for the download-job family — the ZIP member you extract (§5).

---

## 2. Root cause: domain ≠ grain

USAspending's URL path segment after `bulk_download/` / `download/` names the **filter/data domain**, not the output granularity:

| path | domain it selects | it does NOT mean |
|---|---|---|
| `bulk_download/awards` | prime-award universe (contracts + assistance + IDV + loans), filtered by `prime_award_types` | "returns one row per award" |
| `bulk_download/accounts` | federal-account/File-A-B-C universe | — |
| `download/awards` | prime-award universe, row-level custom download | "returns transactions" |
| `download/transactions` | transaction universe, row-level custom download | — |

`bulk_download/awards` is an **asynchronous job endpoint**. The POST does not return data — it returns a job handle:

```json
{ "status_url": "...download/status?file_name=All_PrimeTransactions_...zip",
  "file_name":  "All_PrimeTransactions_...zip",
  "file_url":   "https://files.usaspending.gov/generated_downloads/All_PrimeTransactions_...zip",
  "download_request": { "download_types": ["prime_awards"], "request_type": "award", ... } }
```
*(contract response example — [awards.md](https://github.com/fedspendingtransparency/usaspending-api/blob/master/usaspending_api/api_contracts/contracts/v2/bulk_download/awards.md))*

You then poll `download/status?file_name=…` until `status:"finished"`, download the ZIP at `file_url`, and unzip. **The grain lives in the ZIP member.** The request payload for `bulk_download/awards` carries **no grain parameter at all** — no `spending_level`, no member selector. The member that lands is a fixed property of the endpoint: `bulk_download/awards` → `*_PrimeTransactions_*` (txn grain).

That is why an "awards" job legitimately yields transaction rows: the *award domain* filter, materialized at *transaction* grain.

---

## 3. What actually happens in `usaspending_api_fresh.py`

Step-by-step trace (file:line = [`pipelines/usaspending/usaspending_api_fresh.py`](../../pipelines/usaspending/usaspending_api_fresh.py)):

| step | code | detail |
|---|---|---|
| 1. target | `BULK_DL_URL` — **:53** | `https://api.usaspending.gov/api/v2/bulk_download/awards/` |
| 2. award types | `AWARD_TYPES` — **:56–57** | `A,B,C,D` + `IDV_A…IDV_E` (prime contracts + IDV vehicles) |
| 3. build payload | `_fetch_window` — **:128–136** | `filters.prime_award_types`, `date_type:"last_modified_date"`, `date_range`, `file_format:"csv"` — **no grain field** |
| 4. submit | **:137** | `POST` → `{status_url, file_url, file_name}` |
| 5. poll | **:147–165** | `GET status_url` every `BULK_POLL_SECONDS=15` (**:59**) until `status=="finished"`; ceiling `BULK_POLL_CEILING_SECONDS` (**:61**) |
| 6. download + unzip | **:170–179** | stream `file_url` → `awards.zip`; extract only `*.csv` members |
| 7. read member | `_write` — **:197–200** | DuckDB `read_csv(all_varchar=true)` over the extracted CSV(s) |
| 8. land verbatim | **:204–206** | `lance.write_dataset(...)` → `s3://data-sink/active/usaspending_api_fresh/contract_prime_txn/`, columns unrenamed |

The exact request body (payload) — `_fetch_window`, **:128–136**:

```json
{ "filters": {
    "prime_award_types": ["A","B","C","D","IDV_A","IDV_B","IDV_B_A","IDV_B_B","IDV_B_C","IDV_C","IDV_D","IDV_E"],
    "date_type": "last_modified_date",
    "date_range": { "start_date": "<window_start>", "end_date": "<window_end>" } },
  "file_format": "csv" }
```

The docstring already asserts the invariant (**:28–29**): *"A/B/C/D + IDV_* land in a SINGLE 297-col `Contracts_PrimeTransactions` member (verified live), so one pull, one schema, one table."* This session re-verified it live (§8.2).

**Verdict: CORRECT, not a defect.** This feed is the **FRESH leg of the TRANSACTION spine** (`usaspending_fpds_canonical_txn`, per [`FPDS_CANONICAL_FIELD_DICTIONARY.md`](./FPDS_CANONICAL_FIELD_DICTIONARY.md) §1 upstream feeds). It *wants* transaction grain; `bulk_download/awards` on `last_modified_date` is the right uncapped source for it. The `usaspending_api_fresh.py` docstring (**:24–26**) states the rationale: the paginated search endpoints cap at 10k, `last_modified_date` captures late-landing/re-modified records. `bulk_download/awards` is uncapped and async, so it is the correct fresh-leg source **for transactions**.

---

## 4. USAspending grain-control vocabulary — the three mechanisms

Grain is never inferred from the path. It is set by one of three mechanisms, and **which mechanism applies is a property of the endpoint**:

| mechanism | type | values | endpoints it governs | example |
|---|---|---|---|---|
| **`spending_level`** | enum (modern discriminator) | `awards` · `transactions` · `subawards` | `search/spending_by_award`, `search/spending_by_award_count`, `search/spending_by_category/*`, `download/count`, `download/search` | `"spending_level": "awards"` |
| **`subawards`** | boolean (legacy) | `false` → award rows · `true` → subaward rows | `search/spending_by_award`, `search/spending_by_award_count` | `"subawards": false` |
| **ZIP member** | file selection | `*_PrimeAwardSummaries_*` (award) · `*_PrimeTransactions_*` (txn) · `*_Subawards_*` (subaward) | `bulk_download/awards`, `download/awards`, `download/transactions`, `download/search` | extract `*_PrimeAwardSummaries_*.csv` for award grain |

Notes, all evidence-anchored:

- **`spending_level` and `subawards` interact.** On `spending_by_award_count`, the contract states: *"`subawards` … Defaulted to False unless `spending_level` is set to `subawards`, then the default is True."* — [spending_by_award_count.md](https://github.com/fedspendingtransparency/usaspending-api/blob/master/usaspending_api/api_contracts/contracts/v2/search/spending_by_award_count.md). The enum is the modern control; the boolean is the legacy control kept in sync.
- **`spending_level` enum is confirmed live in responses.** `spending_by_award_count` returned `"spending_level":"awards"`; the same endpoint family with `subawards:true` returned `"spending_level":"subawards"` (§8.1).
- **`download/count` documents all three levels** and defaults to `transactions` when unset: *"The spending_level provided by the user or the default value of transactions."* — [count.md](https://github.com/fedspendingtransparency/usaspending-api/blob/master/usaspending_api/api_contracts/contracts/v2/download/count.md).
- **`download/search` takes an array** of levels: `"spending_level": ["awards","transactions","subawards"]` — [download/search.md](https://github.com/fedspendingtransparency/usaspending-api/blob/master/usaspending_api/api_contracts/contracts/v2/download/search.md) — and its ZIP is named `PrimeAwardsTransactionsAndSubawards_*.zip` (all three members).
- **The download-job family has NO `spending_level` on `bulk_download/awards`.** For `bulk_download/awards` the *only* grain lever is the member name, and only the transactions member is produced (§8.2). For `download/awards` the member set is award-summary + subaward (§8.3).

---

## 5. The award-data API endpoints — surface map

Every endpoint in the catalog touching the award/subaward/transaction domain. `returns` distinguishes **sync-JSON** (data in the HTTP response) from **async-file** (job handle → poll → ZIP). Grain and grain-control per row.

| endpoint | method | returns | grain of rows | cap / limit | grain control |
|---|---|---|---|---|---|
| `search/spending_by_award` | POST | sync-JSON (paged) | **award** (`subawards:false`) or **subaward** (`subawards:true`) | ~10k (100 pages × 100) | `subawards` bool / `spending_level` |
| `search/spending_by_award_count` | POST | sync-JSON | tallies by award category | n/a (counts) | `subawards` bool / `spending_level` |
| `search/new_awards_over_time` | POST | sync-JSON | **award** counts, time-bucketed | n/a | `group` (fiscal_year/quarter/month) |
| `bulk_download/awards` | POST | **async-file** | **transaction** (only `*_PrimeTransactions_*` member) | uncapped (async) | ZIP member (fixed: txn) |
| `download/awards` | POST | **async-file** | **award summary** + **subaward** (`*_PrimeAwardSummaries_*` + `*_Subawards_*`) | 500,000 rows (`limit` in `download_request`) | ZIP member; `prime_and_sub_award_types` |
| `download/transactions` | POST | **async-file** | **transaction** + **subaward** (`*_PrimeTransactions_*` + `*_Subawards_*`) | 500,000 rows | ZIP member |
| `download/search` | POST | **async-file** | **award / txn / subaward** (per `spending_level` array) | 500,000 rows | `spending_level` array + member |
| `download/count` | POST | sync-JSON | count for a `spending_level` | n/a | `spending_level` enum |
| `awards/{award_id}` | GET | sync-JSON | **one award** (full detail) | 1 | path id (`generated_unique_award_id`) |
| `awards/last_updated` | GET | sync-JSON | scalar freshness date | 1 | none |
| `awards/funding` | POST | sync-JSON (paged) | funding rows for one award | `limit` | `award_id` |
| `awards/funding_rollup` | POST | sync-JSON | rollup scalars for one award | 1 | `award_id` |
| `awards/accounts` | POST | sync-JSON (paged) | federal accounts for one award | `limit` | `award_id` |
| `awards/count/transaction/{award_id}` | GET | sync-JSON | count of txns under one award | 1 | path id |
| `awards/count/subaward/{award_id}` | GET | sync-JSON | count of subawards under one award | 1 | path id |
| `awards/count/federal_account/{award_id}` | GET | sync-JSON | count of fed accounts under one award | 1 | path id |
| `idvs/awards` | POST | sync-JSON (paged) | **award** (child/grandchild orders under an IDV) | `limit` | `type` (child_awards / child_idvs / …) |
| `idvs/amounts/{award_id}` | GET | sync-JSON | rollup scalars for one IDV | 1 | path id |
| `agency/{toptier_code}/awards` | GET | sync-JSON | agency award **aggregates** (obligations, txn_count) | 1 | `agency_type`, `award_type_codes` |
| `recipient/state/awards/{fips}` | GET | sync-JSON | per-state award-category aggregates | 1 array | `year` |
| `subawards` (`/api/v2/subawards/`) | POST | sync-JSON (paged) | **subaward** for one prime award | `limit` | `award_id` |
| `references/award_types` | GET | sync-JSON | the award-type-code dictionary | 1 | none |

### Per-endpoint payload detail (the data-pull ones)

#### `bulk_download/awards` — POST — async-file, transaction grain
**Request** (contract example — trimmed; the live feed uses the smaller body in §3):
```json
{ "filters": {
    "prime_award_types": ["A","B","C","D","IDV_A","IDV_B","IDV_B_A","IDV_B_B","IDV_B_C","IDV_C","IDV_D","IDV_E","02","03","04","05","10","06","07","08","09","11","-1"],
    "date_type": "action_date",
    "date_range": { "start_date": "2019-10-01", "end_date": "2020-09-30" },
    "agencies": [{ "type":"funding","tier":"subtier","name":"Animal and Plant Health Inspection Service","toptier_name":"Department of Agriculture" }] },
  "file_format": "csv" }
```
**Response** (job handle): `status_url`, `file_name` = `All_PrimeTransactions_<ts>.zip`, `file_url`, and `download_request.download_types:["prime_awards"]`, `request_type:"award"`. **ZIP member (live §8.2): `All_Contracts_PrimeTransactions_<ts>.csv` — 297 cols, txn grain, and it is the only member.** No `spending_level` field exists on this endpoint.

#### `download/awards` — POST — async-file, award-summary + subaward grain
**Request** (contract example):
```json
{ "filters": {
    "agencies": [{ "type":"awarding","tier":"toptier","name":"Department of Agriculture" }],
    "keywords": ["Defense"] },
  "columns": ["assistance_award_unique_key","award_id_fain","award_id_uri","sai_number","total_funding_amount"] }
```
The server normalizes `filters` into `prime_and_sub_award_types:{prime_awards:[…],sub_awards:[…]}` and echoes `download_request.download_types:["awards","sub_awards"]`, `request_type:"award"`, `limit:500000`. **Response `file_name` = `PrimeAwardSummariesAndSubawards_<ts>.zip`** — confirmed live twice this session (§8.3). Members: `*_PrimeAwardSummaries_*` (**award grain**) + `*_Subawards_*`. This is the endpoint whose award-summary member is the award-spine fresh-leg source (§9).

#### `download/transactions` — POST — async-file, transaction + subaward grain
**Request**: `{"filters":{"keywords":["Defense"]},"columns":["assistance_transaction_unique_key","award_id_fain","modification_number","award_id_uri","sai_number"]}`. **`file_name` = `PrimeTransactionsAndSubawards_<ts>.zip`**; members `*_PrimeTransactions_*` + `*_Subawards_*`. (Contract title is confusingly "Award Download" — same domain≠grain trap: the *transaction* download lives under the award domain.)

#### `search/spending_by_award` — POST — sync-JSON, award ⇄ subaward
**Request** (award grain):
```json
{ "subawards": false, "limit": 10, "page": 1,
  "filters": { "award_type_codes": ["A","B","C"], "time_period": [{"start_date":"2018-10-01","end_date":"2019-09-30"}] },
  "fields": ["Award ID","Recipient Name","Start Date","End Date","Award Amount","Awarding Agency","Awarding Sub Agency","Contract Award Type","Award Type","Funding Agency","Funding Sub Agency"] }
```
**Response**: `{"spending_level":"awards","limit":10,"results":[ {…one row per award…} ]}`. Each row carries an award-level `generated_internal_id` (e.g. `CONT_AWD_ZZ65_9700_W91RUS11A0007_9700`) and numeric `internal_id`. Flip `subawards:true` → `"spending_level":"subawards"` and rows become subawards keyed by `Sub-Award ID` with `prime_award_generated_internal_id` back-references (live §8.1). **Cap: 100 pages of ≤100 = ~10k rows** — the reason the fresh feed uses the uncapped async download instead.

#### `search/spending_by_award_count` — POST — sync-JSON, category tallies
**Request**: `{"filters":{"keywords":["…"]}}` (a filter is required). **Response**: `{"results":{"contracts":N,"direct_payments":0,"grants":0,"idvs":0,"loans":0,"other":0},"spending_level":"awards"}`. Live (§8.1): `contracts:791560` for a one-month A–D filter.

#### `awards/{award_id}` — GET — sync-JSON, one award (full detail)
**Request**: path param `award_id` = a `generated_unique_award_id` (e.g. `CONT_AWD_H907_9700_SPE2DX16D1500_9700`). **Response** (live §8.4): the single-award object keyed by `generated_unique_award_id`, with `total_obligation`, `subaward_count`, `base_and_all_options`, `parent_award`, and — the award's rollup of its transactions — `latest_transaction_contract_data` (NAICS/PSC/set-aside/competition). This is the canonical single-**award** read model.

#### `awards/last_updated` — GET — sync-JSON, freshness scalar
**Response** (live §8.4): `{"last_updated":"07/03/2026"}` — the warehouse's award-data high-water date. Useful as a fresh-leg watermark sanity check.

#### `idvs/awards` — POST — sync-JSON, award grain (IDV hierarchy)
**Request**: `{"award_id":"CONT_IDV_TMHQ10C0040_2044","type":"child_awards","limit":10,"page":1,"sort":"period_of_performance_start_date","order":"desc"}`. **Response** (live): `results[]` of child orders, each an **award** with `generated_unique_award_id`, `obligated_amount`, `piid`, `period_of_performance_*`. Grain toggle is `type` ∈ {child_awards, child_idvs, grandchild_awards}.

#### `references/award_types` — GET — the code dictionary
**Response**: the full award-type-code map — `contracts:{A:"BPA Call",B:"Purchase Order",C:"Delivery Order",D:"Definitive Contract"}`, `idvs:{IDV_A…IDV_E}`, `grants`, `loans`, `direct_payments`, `other_financial_assistance`. This is the authoritative source for the `prime_award_types` / `award_type_codes` enum used by every endpoint above.

---

## 6. Contrast matrix — the same endpoint, three grains

The download-job family makes the domain≠grain point sharpest: **the row grain is chosen by endpoint + member, not by the `awards`/`transactions` word in the path.**

| you want | endpoint | live `file_name` (this session or contract) | member to extract | grain |
|---|---|---|---|---|
| **transactions** (fresh feed uses this) | `bulk_download/awards` | `All_PrimeTransactions_*.zip` (live) | `*_Contracts_PrimeTransactions_*` (297 col) | transaction |
| **award summaries** | `download/awards` | `PrimeAwardSummariesAndSubawards_*.zip` (live) | `*_PrimeAwardSummaries_*` | **award** |
| **transactions (custom cols)** | `download/transactions` | `PrimeTransactionsAndSubawards_*.zip` (contract) | `*_PrimeTransactions_*` | transaction |
| **all three at once** | `download/search` | `PrimeAwardsTransactionsAndSubawards_*.zip` (contract) | member per `spending_level` | award / txn / subaward |
| **subawards** | any of the above | (the `*_Subawards_*` member) | `*_Subawards_*` | subaward |

Sync alternatives for award grain (no ZIP, but capped):

| you want | endpoint | control | cap |
|---|---|---|---|
| award rows (paged JSON) | `search/spending_by_award` | `subawards:false` / `spending_level:awards` | ~10k rows |
| one award, full detail | `awards/{award_id}` | path id | 1 |
| award counts by category | `search/spending_by_award_count` | filter | n/a |
| IDV child/grandchild awards | `idvs/awards` | `type` | paged |

**Cap asymmetry is the operational driver:** `spending_by_award` truncates at ~10k, so it cannot backfill or daily-refresh a 78M-row award spine. `bulk_download/awards` and `download/awards` are uncapped async — the only viable bulk/fresh sources. That is exactly why the txn fresh leg chose `bulk_download/awards` over `spending_by_award`, and why the award fresh leg must choose `download/awards` for the same reason (§9).

---

## 7. Three-grain platform mapping

Aligns the API grain surface to the platform's three canonical spines (per the standing architecture):

| grain | API bulk source | API fresh source | canonical Lance spine | key | status |
|---|---|---|---|---|---|
| **transaction** | pg-dump `transaction_search_fpds` | `bulk_download/awards` → `*_PrimeTransactions_*` | `usaspending_fpds_canonical_txn` | `contract_transaction_unique_key` | **BUILT** (108M rows) |
| **award** | pg-dump `award_search` (78.64M, authoritative rollup) | `download/awards` → `*_PrimeAwardSummaries_*` **(recommended, §9)** | `usaspending_award_canonical` | `contract_award_unique_key` / `generated_unique_award_id` | **NOT BUILT** |
| **subaward** | pg-dump `subaward_search` | `download/awards`/`download/transactions` → `*_Subawards_*`, or `bulk_download/awards`* | `usaspending_subaward_canonical` | (prime, `subaward_number`) | **BUILT** (258-col full-width OBT, #953) |

\* the fresh subaward feed `usaspending_api_fresh/contract_subaward` exists (0.2M, 118c).

---

## 8. Live-verified payloads & returns (2026-07-03 / job artifacts 2026-07-04 UTC)

### 8.1 `spending_by_award` — award grain vs subaward grain (sync, one endpoint, the grain flip)

**Award leg** — `POST search/spending_by_award` with `subawards:false`, `award_type_codes:["A","B","C","D"]`, `time_period 2025-05-15…05-16`, `recipient_search_text:["Lockheed Martin"]` → **HTTP 200, 5 rows, 5 distinct `Award ID`s (one row per award)**. First row:
```json
{ "internal_id": 350351578, "Award ID": "W91CRB10C0182", "Recipient Name": "LOCKHEED MARTIN CORPORATION",
  "Award Amount": 8179129.33, "Awarding Agency": "Department of Defense", "Contract Award Type": "DEFINITIVE CONTRACT",
  "generated_internal_id": "CONT_AWD_W91CRB10C0182_9700_-NONE-_-NONE-", "awarding_agency_id": 1173, "agency_slug": "department-of-defense" }
```
All five `Award ID`s distinct: `W91CRB10C0182, W9132V25P0006, W912CH25F0174, W912CH25F0151, W911S024C0004`. The presence of `generated_internal_id` / `internal_id` at the row level is the award-grain signature.

**Subaward leg** — same endpoint, `subawards:true`, `fields` = subaward columns → **HTTP 200, response header `"spending_level":"subawards"`**, rows are subawards:
```json
{ "internal_id": "200155", "prime_award_internal_id": 292026551, "Sub-Award ID": "200155",
  "Sub-Awardee Name": "KIEWIT POWER CONSTRUCTORS CO", "Sub-Award Amount": 1154736350.0, "Sub-Award Date": "2025-05-30",
  "Prime Award ID": "89233018CNR000004", "Prime Recipient Name": "FLUOR MARINE PROPULSION, LLC",
  "Awarding Agency": "Department of Energy", "prime_award_generated_internal_id": "CONT_AWD_89233018CNR000004_8900_-NONE-_-NONE-" }
```
Same endpoint, same filter, `subawards` boolean flips grain from award → subaward, and the response's own `spending_level` field reports `awards` vs `subawards`. This is the cleanest single-endpoint proof of the mechanism.

**Count** — `POST search/spending_by_award_count`, `subawards:false`, A–D, `2025-05`:
```json
{ "results": { "contracts": 791560, "direct_payments": 0, "grants": 0, "idvs": 0, "loans": 0, "other": 0 },
  "spending_level": "awards" }
```

> Note: broad `spending_by_award` award-leg queries with a server-side `sort` returned **HTTP 504** twice this session (the award-leg sort is slow under load); dropping the sort and narrowing with a recipient keyword returned 200. Operationally this reconfirms the ~10k-cap endpoint is unfit for bulk award loads.

### 8.2 `bulk_download/awards` — live ZIP member enumeration (the core proof)

`POST bulk_download/awards` with `prime_award_types:[A,B,C,D,IDV_A…IDV_E]`, `date_type:"last_modified_date"`, tight 1-day `date_range 2026-06-20…06-20`, `file_format:"csv"`:

- **Job `file_name`: `All_PrimeTransactions_2026-07-04_H02M08S45570540.zip`** (the endpoint names its own output *PrimeTransactions*).
- Poll 1 → `status:"finished"`, `total_rows: 0` (no contracts were last-modified that specific day — an empty but structurally complete download).
- **ZIP members: `['All_Contracts_PrimeTransactions_2026-07-04_H02M08S46_1.csv']`** — a single member.
- **Column count: 297.** First 15 headers: `contract_transaction_unique_key, contract_award_unique_key, award_id_piid, modification_number, transaction_number, parent_award_agency_id, parent_award_agency_name, parent_award_id_piid, parent_award_modification_number, federal_action_obligation, total_dollars_obligated, total_outlayed_amount_for_overall_award, base_and_exercised_options_value, current_total_value_of_award, base_and_all_options_value`.

**Conclusion:** `bulk_download/awards` produces exactly one CSV member, at **transaction** grain, 297 columns, matching the `usaspending_api_fresh.py:28–29` and `:55` docstring claims. There is **no** `PrimeAwardSummaries` member in a `bulk_download/awards` download. The `contract_transaction_unique_key` first-column is the transaction PK — definitive txn grain.

### 8.3 `download/awards` — award-summary member naming (live file_name; width PENDING)

`POST download/awards` with `prime_and_sub_award_types.prime_awards:[A,B,C,D]`, `date_range`, `recipient_search_text` — submitted successfully (**HTTP 200**) on multiple attempts. **Live job `file_name`s returned by the endpoint:**
- `PrimeAwardSummariesAndSubawards_2026-07-04_H02M10S37386003.zip`
- `PrimeAwardSummariesAndSubawards_2026-07-04_H02M14S37312557.zip`

This directly confirms — from the live endpoint, not just the contract — that `download/awards` materializes the **PrimeAwardSummaries** (award-grain) + **Subawards** members. The contract corroborates: `download_types:["awards","sub_awards"]`, `request_type:"award"` ([download/awards.md](https://github.com/fedspendingtransparency/usaspending-api/blob/master/usaspending_api/api_contracts/contracts/v2/download/awards.md)).

> **PENDING LIVE CONFIRMATION — the physical PrimeAwardSummaries column count.** The row-level `download/awards` custom-award queue was backlogged this session: submissions succeeded and were correctly named, but the jobs stayed `status:"running"` past an 8-min then a 12-min poll ceiling (one earlier attempt returned `status:"failed"`), so the CSV header was not extracted. The **member NAME and grain are live-confirmed** (job `file_name`); the exact **award-summary column width is not** and rests on the catalog contract + USAspending download structure. **To reproduce:** re-run `scratchpad/probe_download_awards3.py` (tiniest filter: single day + one recipient) off-peak, or raise the ceiling; enumerate the `*_PrimeAwardSummaries_*.csv` header. Expect the award-summary member to be materially wider than the 297-col transactions member (award rollups carry executive-comp, PoP-current, and aggregate-value columns absent at txn grain).

### 8.4 `awards/{award_id}` + `awards/last_updated` — award-detail and freshness (live)

`GET awards/350351578` (the DoD/Lockheed award from §8.1) → **HTTP 200**, single-award payload keyed by `generated_unique_award_id`:
```json
{ "id": 350351578, "generated_unique_award_id": "CONT_AWD_W91CRB10C0182_9700_-NONE-_-NONE-", "piid": "W91CRB10C0182",
  "category": "contract", "type": "D", "type_description": "DEFINITIVE CONTRACT", "total_obligation": 8179129.33,
  "recipient": { "recipient_uei": "G5TLLN5B23K3", "recipient_name": "LOCKHEED MARTIN CORPORATION", … } }
```
Top-level keys (award rollup shape): `id, generated_unique_award_id, piid, category, type, type_description, description, total_obligation, subaward_count, total_subaward_amount, date_signed, base_exercised_options, base_and_all_options, total_account_outlay, total_account_obligation, account_outlays_by_defc, account_obligations_by_defc, parent_award, latest_transaction_contract_data, funding_agency, awarding_agency, period_of_performance, recipient, executive_details, place_of_performance, psc_hierarchy, naics_hierarchy, total_outlay`. Note `subaward_count` and `latest_transaction_contract_data` — the award **rolls up** its subawards and transactions, the defining property of award grain.

`GET awards/last_updated` → **HTTP 200** `{"last_updated":"07/03/2026"}`.

---

## 9. Recommendation — the award-spine fresh leg

The award spine (`usaspending_award_canonical`, not yet built) needs the same two-leg pattern as the txn and subaward spines: **BULK** (authoritative, historical) ⊕ **FRESH** (uncapped, late-landing overlay).

- **BULK leg — authoritative award rollup:** `s3://data-sink/active/usaspending/award_search/` (78.64M rows, 154 cols, AWARD grain — contracts + assistance + IDV + loans). This is the pg-derived award-grain SoR, analogous to `transaction_search_fpds` for the txn spine.
- **FRESH leg — uncapped award-summary overlay:** reuse the **async download-job machinery** already in `usaspending_api_fresh.py` (`_fetch_window` submit/poll/unzip → `_write` verbatim → Lance append-only), changing the **endpoint and member**, not just the member:

  | change | txn fresh leg (current) | award fresh leg (recommended) |
  |---|---|---|
  | endpoint | `bulk_download/awards` | **`download/awards`** (or `download/search` with `spending_level:["awards"]`) |
  | request filter shape | `filters.prime_award_types` | `filters.prime_and_sub_award_types.prime_awards` + `sub_awards:[]` |
  | ZIP `file_name` | `All_PrimeTransactions_*.zip` | `PrimeAwardSummariesAndSubawards_*.zip` |
  | member to extract | `*_Contracts_PrimeTransactions_*` (297c) | **`*_PrimeAwardSummaries_*`** |
  | grain landed | transaction | **award** |
  | PK / index | `contract_transaction_unique_key` | `contract_award_unique_key` / `generated_unique_award_id` |

  > **This corrects the standing recommendation** that the award fresh leg is a one-line member swap on `bulk_download/awards`. It is not: `bulk_download/awards` emits **only** the transactions member (§8.2, live). The award-summary member comes from `download/awards` / `download/search` (§8.3). The reusable asset is the async submit/poll/unzip/verbatim-write *machinery*, not the endpoint.

- **Freshness anchoring:** use `date_type:"action_date"` (award-summary downloads support it) or a last-modified filter if available, and watermark against `GET awards/last_updated` (§8.4). Keep the fresh leg append-only and reconcile duplicates downstream on the argmax of a modified date, exactly as the txn fresh leg does (`usaspending_api_fresh.py:14–22`).
- **Composition:** `usaspending_award_canonical` = `award_search` (bulk, authoritative) ⊕ fresh `PrimeAwardSummaries` overlay, keyed on `contract_award_unique_key`, one surviving row per key — mirroring the txn spine's BULK⊕FRESH⊕MONTHLY reconciliation in [`FPDS_CANONICAL_FIELD_DICTIONARY.md`](./FPDS_CANONICAL_FIELD_DICTIONARY.md) §1.
- **Cap rationale (unchanged):** `spending_by_award` (award leg) caps at ~10k rows and 504s under load (§8.1), so it is unusable for bulk/fresh award loads; the async `download/awards` job is uncapped (500k-row `download_request.limit` per job, chunk the window to stay under it).

---

## 10. Evidence appendix

### 10.1 Catalog provenance
- Lance catalog: `s3://data-sink/active/usaspending_api_catalog/` — **176 rows, 18 cols, 1/endpoint**, BTREE on `endpoint_path`. Built by [`pipelines/usaspending/usaspending_api_catalog.py`](../../pipelines/usaspending/usaspending_api_catalog.py) from the upstream API-Blueprint contracts (`usaspending_api/api_contracts/contracts/`). Verified 2026-07-03.

### 10.2 Live probe artifacts (scratchpad, this session)
| probe | endpoint | key result |
|---|---|---|
| `probe_bulk_members.py` | `bulk_download/awards` | job `All_PrimeTransactions_2026-07-04_H02M08S45570540.zip`; 1 member `All_Contracts_PrimeTransactions_*.csv`; **297 cols**; PK col `contract_transaction_unique_key` |
| `probe_spending_by_award.py` | `spending_by_award` (×2) + `_count` | award leg `spending_level:awards`; subaward leg `spending_level:subawards`; count `contracts:791560` |
| `probe_award_grain_multi.py` | `spending_by_award` award, `awards/{id}`, `awards/last_updated` | 5 distinct `Award ID`s (1/award); `awards/{id}` 200 keyed by `generated_unique_award_id`; `last_updated:07/03/2026` |
| `probe_download_awards*.py` | `download/awards` | job `PrimeAwardSummariesAndSubawards_2026-07-04_*.zip` (naming confirmed live); member width PENDING (queue backlog) |

### 10.3 Code citations
- `pipelines/usaspending/usaspending_api_fresh.py` — docstring **:3–8, :14–29**; `BULK_DL_URL` **:53**; `AWARD_TYPES` **:56–57**; `DATE_TYPE` **:58**; `_fetch_window` payload **:128–136**; submit **:137**; poll loop **:147–165**; unzip **:170–179**; verbatim write **:204–206**; `FRESH_URI` **:48–51**.
- `pipelines/usaspending/usaspending_api_catalog.py` — contract source **:35–37**; parse **:129–186**; live-sample **:220–285**.

### 10.4 Upstream contract permalinks (github.com/fedspendingtransparency/usaspending-api, `blob/master`)
| endpoint | contract |
|---|---|
| `bulk_download/awards` | `usaspending_api/api_contracts/contracts/v2/bulk_download/awards.md` |
| `download/awards` | `.../contracts/v2/download/awards.md` |
| `download/transactions` | `.../contracts/v2/download/transactions.md` |
| `download/search` | `.../contracts/v2/download/search.md` |
| `download/count` | `.../contracts/v2/download/count.md` |
| `search/spending_by_award` | `.../contracts/v2/search/spending_by_award.md` |
| `search/spending_by_award_count` | `.../contracts/v2/search/spending_by_award_count.md` |
| `awards/{award_id}` | `.../contracts/v2/awards/award_id.md` |
| `awards/last_updated` | `.../contracts/v2/awards/last_updated.md` |
| `idvs/awards` | `.../contracts/v2/idvs/awards.md` |
| `references/award_types` | `.../contracts/v2/references/award_types.md` |

### 10.5 Member-naming reference (from contracts + live job names)
| endpoint | ZIP `file_name` pattern | members | grain(s) |
|---|---|---|---|
| `bulk_download/awards` | `All_PrimeTransactions_*.zip` | `*_Contracts_PrimeTransactions_*` (only) | txn |
| `download/awards` | `PrimeAwardSummariesAndSubawards_*.zip` | `*_PrimeAwardSummaries_*` + `*_Subawards_*` | award + subaward |
| `download/transactions` | `PrimeTransactionsAndSubawards_*.zip` | `*_PrimeTransactions_*` + `*_Subawards_*` | txn + subaward |
| `download/search` | `PrimeAwardsTransactionsAndSubawards_*.zip` | member per `spending_level` | award + txn + subaward |
