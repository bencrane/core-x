# Directive: Federal Appropriations & Budget Authority — OMB Public Budget Database + USAspending Budgetary Resources (R2/Lance, core-x)

**Status:** ready for executor
**Created:** 2026-07-27 UTC
**Type:** Ingest — the *appropriated* side of the federal dollar, which the plane currently lacks entirely. Today every number in the system is an **obligation** (money committed on a contract, `txn_events_combo` / `pop_*_fy`). Nothing measures **budget authority** (money Congress made available). This directive lands both, at account grain and agency grain, so a demo card can put appropriated and obligated side by side — and so the OBBA `$785B` constant stops being an authored dial.
**Initiated by:** human (operator: "Produce a directive that I can give to another agent to ingest etc. the historical appropriations datasets etc")
**Predecessor:** `/Users/benjamincrane/Desktop/hq/directives/2026-07-23-industry-cost-structure-batch-ingest.md` (prior cycle; this directive lives in-repo at `docs/plans/`)

---

## 🚀 Executor kickoff (read this first if picking up cold)

1. **Repo = `core-x`.** Gen-3 Lance ingest: ephemeral download → parse → `lance.write_dataset` to `s3://data-sink/active/<name>/`. Raw is transport-only; Lance is the system of record; no catalog layer.
2. **No sibling in flight for these datasets.** Your module is `pipelines/reference/federal_appropriations_ingest.py`; datasets (§4) are disjoint from `labor_share_ingest.py`, `bea_io_use_ingest.py`, and `industry_cost_structure_ingest.py`. Do not touch those modules. Branch: `claude/federal-appropriations-ingest`.
3. **Upstreams VERIFIED LIVE 2026-07-27** — endpoints, workbook dimensions, full column header list, sample rows, and API response shapes are in `### Evidence`. Do not re-discover them. One route is verified-DEAD (§2.6) — do not retry it.
4. **Worktree discipline (L0):** fresh branch off `main`; run modules from the checkout root containing your files (`python -m …` resolves against cwd).
5. **Secrets (L1):** `doppler run -p core-x -c prd --` injects `R2_*` + `HQX_DB_URL_POOLED`. Run pattern:
   `doppler run -p core-x -c prd -- uv run --with pylance --with pyarrow --with duckdb --with openpyxl --with requests --with boto3 --with 'psycopg[binary]' python -m pipelines.reference.federal_appropriations_ingest --stream <name>`
6. **Fleet plumbing — reuse verbatim (the SKELETON only):** model on `pipelines/reference/industry_cost_structure_ingest.py` (multi-stream argparse, per-stream Lance datasets, `--smoke`, fail-closed gates, ledger). Reuse `_build_indexes(uri, btree, bitmap, so)` (pass `bitmap=[]` + the storage-options dict) / `_storage_options` / `DATA_STORAGE_VERSION` / `MAX_ROWS_PER_FILE` / `MAX_BYTES_PER_FILE` from `pipelines/bls/ingest.py`. **⚠ The rate governor does NOT exist in either predecessor** (grep-confirmed: no token bucket, warm-up, circuit breaker, or path-checkpoint anywhere in the fleet). It MUST be written new as a shared helper `pipelines/_lib/rate_governor.py`, landed WITH a unit test asserting (a) sustained rate ≤2 req/s over any 10 s window, (b) a synthetic `403` trips the breaker and the second trip returns `disposition='throttled'`, (c) the path-checkpoint round-trips across a process restart. All three sibling directives import it. "Only the parsers differ" is FALSE — the governor is net-new and safety-critical.
7. **Zero LLM.** Deterministic xlsx + JSON parses only.
8. **Git lifecycle end-to-end:** commit by explicit path (never `git add -A`) → push → PR → self-merge (`gh pr merge --squash --delete-branch`) after gates pass → `git -C /Users/benjamincrane/core-x pull` → `git log -1 --oneline`. Merged ≠ done.

## ⚠ RATE DISCIPLINE (binding — read before writing the fetch loop)

**Assume the host will cut you off without warning.** These publishers return no
rate-limit headers — no `X-RateLimit-*`, no `Retry-After`. There is no warning shot: a
host either tolerates you or goes straight to a block page. This program has already been
hard-`403`'d at the edge by `bls.gov` and `cbo.gov`; those blocks are IP-scoped and can
persist for hours. A block does not just fail the run — it can cost access to the source
for the rest of the day, and it is not undoable by retrying.

**Binding limits. Do not raise them without an operator ruling. Do not "test" them.**

1. **Concurrency ≤ 3 workers. Sustained rate ≤ 2 req/s aggregate**, enforced by a token
   bucket, not `sleep()` between calls.
2. **Warm-up ramp.** First 100 requests at **1 req/s, single worker**. Ramp to the ceiling
   only after 100 consecutive clean `200`s. Any non-200 resets the counter.
3. **Circuit breaker — halt, never grind.** 3 consecutive non-200s, *or* any single `403`
   or `429`: stop all workers immediately, sleep **300 s**, resume at warm-up settings. A
   **second** trip in the same run: **halt the run**, write the ledger row (`status='failed'`, `disposition='throttled'`), flush the checkpoint, surface to the operator. Retrying into a
   wall is what converts a soft throttle into a persistent IP block.
4. **Honor `Retry-After`** if it appears, over every other setting here.
5. **Checkpoint every 200 completed files.** A block must cost only the in-flight batch,
   never the crawl. Re-runs resume from the checkpoint and re-fetch nothing cached.
6. **User-Agent — one honest descriptive UA by default** (`core-x-data-factory/1.0 (federal reference-data ingest; contact: <operator email>)`). Some hosts require a browser UA merely to serve a file (whitehouse.gov 403s a bare client, §2.1) — setting one *there* is a baseline, not evasion; set it per-host. The prohibition is on *cycling* UAs to bypass an active 403 (the cbo.gov case). Per-host: whitehouse.gov → browser UA; api.usaspending.gov → descriptive UA.
7. **One agent per host, ever.** These directives are parallel-safe *because* they touch
   disjoint hosts. Never run two agents, two shells, or two `--stream` invocations against
   the same host concurrently — that silently doubles the rate the host sees.
8. **Never probe a host to discover its limit.** Do not burst, do not benchmark, do not
   ramp "just to see." The limits above are the contract; observed headroom is not
   permission. Wall-clock is not the constraint — an unattended 30K-file crawl at 2 req/s
   finishes in ~4.2 hours, which is a fraction of the cost of a block.

## [GLOBAL: THE DATA FACTORY PROTOCOL]

- **Lifecycle stages 2–3**; Stage-1 verification pre-done for every stream and embedded in `### Evidence`.
- **Pattern A (direct hydration):** static xlsx + keyless JSON API → transform → Lance SoR. No intermediate storage.
- **Raw stays lossless:** agency/bureau/account/subfunction codes and names land verbatim as strings (leading zeros are load-bearing — `001`, `012`, `097`); derived reconciliations are ADDITIONAL datasets, never replacements. Negative values are real (receipt accounts) — never `abs()`, never drop.
- **Estimates are flagged, never silently mixed with actuals.** The OMB workbook carries actuals through FY2025 and *estimates* for FY2026–FY2031 in identical-looking columns. Every landed row carries `is_estimate BOOL`. Getting this wrong silently converts a projection into a reported fact.
- **Source ingest invariant:** bulk-statistical/reference → Lance SoR only.
- **F3 hook:** predecessor path verified at write time (`test -f` passed 2026-07-27).

## [MISSION: FEDERAL APPROPRIATIONS R2 INGEST]

### 0. Why this matters (operator's words)

The demo narrates a five-year obligation history ($3.65T, FY21–25) and an OBBA-driven uplift ahead. The uplift number (`$785B`) and its `0.40` ramp are **authored constants** — nothing in the plane measures money Congress appropriated, only money agencies obligated. That asymmetry is a real hole: it means the system can say "this much was spent" but cannot say "this much was made available and has not been spent yet" — which is precisely the gap the go-to-market thesis rests on. Landing budget authority closes it, and makes an appropriated-vs-obligated card sourceable rather than asserted.

### 1. Objective

Land ten Lance datasets under `s3://data-sink/active/` from two verified static workbooks and one keyless JSON API, each with BTREE indexes on its resolution keys, plus one derived FY reconciliation table, one ledger table, one PR. Volumes: OMB budget authority ~250–290K rows; OMB outlays ~250–290K; USAspending agency-FY ~1–2K; USAspending federal-account-FY ~20–25K; DEFC registry ~52; derived FY reconciliation ~60–70.

### 2. Source-specific facts the executor MUST internalize

1. **OMB Public Budget Database — budget authority (`--stream omb_budauth`).** URL (verified 200, `…spreadsheetml.sheet`, 1,610,953 bytes): `https://www.whitehouse.gov/wp-content/uploads/2026/04/budauth_fy2027.xlsx`. **Browser UA header required** (whitehouse.gov 403s a bare client). One sheet `Sheet1`, **5,129 rows × 69 columns**. Row 1 is the header. Columns 1–12 are dimensions: `Agency Code, Agency Name, Bureau Code, Bureau Name, Account Code, Account Name, Treasury Agency Code, CGAC Agency Code, Subfunction Code, Subfunction Title, BEA Category, On- or Off- Budget`. **Columns 13–69 are years**, verbatim header labels: `1976, TQ, 1977, 1978, … , 2031` (57 columns). `TQ` = the 1976 *transition quarter* (the Jul–Sep 1976 stub from the fiscal-year shift) — it is NOT a year. Land it with a **sentinel that cannot collide with FY1976**: `year = -1976, is_transition_quarter=true` (and `period_label='TQ1976'`). Landing it as `year=1976` would make any `SUM … GROUP BY year` conflate a 3-month stub into the 12-month FY1976 total. Every rollup (incl. §4 stream 10) filters `is_transition_quarter=false`. **Actuals vs estimates is vintage-relative and MUST be derived from the resolved filename, never hardcoded.** Parse `budauth_fy{YYYY}.xlsx` → `budget_year = YYYY` (the verified file is `budauth_fy2027.xlsx` → `budget_year=2027`). Set `is_estimate = (year >= budget_year - 1)` and record `budget_year` in the ledger. For the FY2027 vintage that makes FY2026–FY2031 estimates and FY1976–FY2025 actuals — but on the next release (§2.3 discovers it automatically) FY2026 becomes an actual, and the derived rule follows it. Hardcoding `year >= 2026` would silently label a reported fact as a projection on the very next run — the single most damaging error this directive can make. Values are **$ millions**, may be negative (receipt/offsetting accounts — real, keep), and may be blank (account did not exist that year — skip the cell, do not zero-fill). `Account Code` is blank on some aggregate receipt rows (see Evidence r2/r3) — land those with `account_code=NULL` and `row_kind='aggregate_receipt'` vs `'account'`; do not drop them.
2. **OMB Public Budget Database — outlays (`--stream omb_outlays`).** URL (verified 200, 2,144,756 bytes): `https://www.whitehouse.gov/wp-content/uploads/2026/04/outlays_fy2027.xlsx`. Same publisher, same expected architecture. **Its internals are the ONE thing not pre-verified** — a `HEAD` returns `Content-Length: 0` while `GET` returns the full 2.1 MB body, so it was fetched but not opened. **L44 DISCOVERY REQUIRED:** open it, print `sheetnames`, `max_row`/`max_column`, and row 1 verbatim BEFORE parsing; hard-fail on shape drift rather than assuming the budauth layout. If (and only if) the header row matches budauth's 12 dimension columns + year columns, reuse the same melt.
3. **These URLs are release-dated and WILL move.** `…/uploads/2026/04/…` is the FY2027 budget release (April 2026). The FY2028 release will publish under a new `uploads/YYYY/MM/` path with a new filename. Do **not** hardcode a guessed future path. Discover the current pair by fetching `https://www.whitehouse.gov/omb/information-resources/budget/supplemental-materials/` (verified 200) and regex-ing `href="[^"]*(budauth|outlays)_fy\d{4}\.xlsx"` — that page is what surfaced the verified URLs. Record the resolved URL + byte size per stream in the ledger row.
4. **USAspending agency budgetary resources (`--stream usa_agency`).** Two keyless endpoints, both verified 200 JSON:
   - `https://api.usaspending.gov/api/v2/references/toptier_agencies/` → `results[]`, **111 agencies**, keys: `toptier_code, agency_name, abbreviation, agency_id, agency_slug, active_fy, active_fq, budget_authority_amount, current_total_budget_authority_amount, obligated_amount, outlay_amount, percentage_of_total_budget_authority, congressional_justification_url`. This is the agency roster — land it as-is and use `toptier_code` to drive the per-agency sweep.
   - `https://api.usaspending.gov/api/v2/agency/{toptier_code}/budgetary_resources/` → per agency, `agency_data_by_year[]` with `fiscal_year, agency_budgetary_resources, agency_total_obligated, agency_total_outlayed, total_budgetary_resources` (government-wide denominator) plus a nested `agency_obligation_by_period[]` (`period`, `obligated` — fiscal *month* 2–12, the intra-year obligation curve). Verified live on `012`: FY2026 `$476.7B` resources / `$168.2B` obligated / `$165.7B` outlayed, back through FY2025 and earlier. **Flatten `agency_obligation_by_period` into its own dataset** (§4 stream 5) rather than nesting — it is the monthly pacing series and is independently useful. Sweep all 111 codes at the **RATE DISCIPLINE** ceiling (≤2 req/s aggregate, ≤3 workers — that section governs, no per-stream exception); retry 5xx with backoff; a `429` is not retryable — it trips the circuit breaker; a per-agency failure is logged and skipped, never fatal.
5. **USAspending account balances — the bulk ZIP route (`--stream usa_account_balances`). THIS IS THE PRIMARY APPROPRIATIONS FEED.** `POST https://api.usaspending.gov/api/v2/download/accounts/` mints a real ZIP asynchronously; it is the exact call the site's Download Center GUI makes, and it is fully scriptable. **Verified end-to-end 2026-07-27:**
   - `POST` body: `{"account_level":"federal_account","filters":{"fy":2025,"quarter":4,"submission_types":["account_balances"]},"file_format":"csv"}` → 200, returns `status_url`, `file_name`, `file_url`.
   - Poll `status_url` until `status:"finished"` (verified: finished on the **first** poll; `total_size` 138.461 KB, `total_rows` 1959, `total_columns` 22).
   - `GET file_url` → 200, 138,461 bytes, one CSV inside (`FY2025P01-P12_All_FA_AccountBalances_*.csv`, 727,415 bytes uncompressed).
   - **Verified column list (22, L44):** `owning_agency_name, reporting_agency_name, submission_period, federal_account_symbol, federal_account_name, agency_identifier_name, budget_function, budget_subfunction, budget_authority_unobligated_balance_brought_forward, adjustments_to_unobligated_balance_brought_forward_cpe, budget_authority_appropriated_amount, borrowing_authority_amount, contract_authority_amount, spending_authority_from_offsetting_collections_amount, total_other_budgetary_resources_amount, total_budgetary_resources, obligations_incurred, deobligations_or_recoveries_or_refunds_from_prior_year, unobligated_balance, gross_outlay_amount, status`.
   - **`budget_authority_appropriated_amount` is the appropriated figure this whole directive exists to obtain**, and `total_budgetary_resources` / `obligations_incurred` / `unobligated_balance` / `gross_outlay_amount` sit beside it on the same row. One CSV per FY delivers the entire appropriated-vs-obligated comparison at federal-account grain. **Unobligated balance is the "appropriated but not yet spent" measure** — the single most demo-relevant column in this directive.
   - Sweep `fy` from **2017 → the current FY**, `quarter: 4` for closed years and the latest available quarter for the open year (probe `https://api.usaspending.gov/api/v2/references/submission_periods/` for what is published). Also mint `account_level:"treasury_account"` for the same years — same endpoint, finer grain (TAS), landed as a second dataset.
   - Be polite: mint one job at a time, poll at ~10s intervals, cap at ~40 polls per job before logging a timeout and moving on. Cache the downloaded ZIPs in the session scratchpad so a re-run does not re-mint.
   - **This route supersedes paginating `/api/v2/federal_accounts/`.** That endpoint (verified 200, `count: 2261` for FY2025, `{account_id, account_number, account_name, agency_identifier, managing_agency, managing_agency_acronym, budgetary_resources}`) carries only `budgetary_resources` — no appropriated, no obligated, no unobligated. Land it ONLY as a small roster/crosswalk (`account_number` ↔ `account_id` ↔ managing agency), not as the resource feed.

6. **DEFC registry (`--stream usa_defc`) — and the OBBA finding the operator needs.** `https://api.usaspending.gov/api/v2/references/def_codes/` → verified 200, **52 codes**. DEFC (Disaster Emergency Fund Code) is how USAspending tags dollars to a specific supplemental appropriations act: `N` = CARES Act (P.L. 116-136), `V` = ARPA (P.L. 117-2), **`Z` = Infrastructure Investment and Jobs Act (P.L. 117-58)**, `AAL/AAM/AAN/AAO/AAP` = the P.L. 119-4 / 119-74 / 119-75 / 119-86 acts of the current Congress. **VERIFIED NEGATIVE FINDING — record this prominently in the cycle report: there is NO DEFC for OBBA (P.L. 119-21).** DEFC only tags dollars carrying an emergency / disaster / wildfire-suppression designation; OBBA is a reconciliation act and its appropriations are not so designated. **Therefore OBBA-attributable dollars cannot be isolated by tag on USAspending.** The only defensible routes to an OBBA number are (a) year-over-year deltas in OMB budget authority at account grain across the FY2026/FY2027 releases, or (b) the FY2027 Budget's own scoring tables — both of which are *derivations*, not a tagged feed. Do not manufacture an OBBA total in this directive; land the registry, land the evidence, and state the limitation. **Side benefit worth capturing:** IIJA (`Z`) *is* fully taggable, which makes it a real, sourceable analogue for "what happens to obligations after a large infrastructure act passes."
7. **Gov-wide totals (`--stream usa_total_resources`).** `https://api.usaspending.gov/api/v2/references/total_budgetary_resources/` → verified 200, **118 rows**, `{fiscal_year, fiscal_period, total_budgetary_resources}` (e.g. FY2026 P9 = `$16.047T`). One call, no pagination — the government-wide denominator by FY × period. Land verbatim.

8. **Route corrections — one dead, one live, one deliberately skipped.**
   - **DEAD, do not retry:** `POST /api/v2/bulk_download/accounts/` → **404**. `GET https://files.usaspending.gov/` root → 404. (An earlier probe of this cycle mistook the 404 for "no bulk route exists" — that was wrong; see the live route immediately below.)
   - **LIVE, and it is the primary feed:** `POST /api/v2/download/accounts/` (§2.5). The path segment is `download`, not `bulk_download`. `POST /api/v2/bulk_download/list_agencies/ {"type":"account_agencies"}` → 200 (roster only, 10,124 B) — useful for the CFO-agency list, nothing more.
   - **SKIPPED by choice:** `https://files.usaspending.gov/database_download/` publishes the **entire USAspending dataset as a PostgreSQL archive, FY2001→present** (per the operator-supplied DCAT catalog, `~/Downloads/USAspending-data-catalog.json`, dataset [5]). It is hundreds of GB and duplicates the award/transaction data the plane already holds. **Out of scope** — the §2.5 per-FY account ZIPs are ~140 KB each and carry every column this directive needs. Do not download the archive.
   - **Catalog of record:** the operator supplied `USAspending-data-catalog.json` (DCAT-US, 7 datasets: Award Data Archive, Custom Award Data, Custom Account Data, DABS Submissions, FABS Submissions, Database Downloads, API). Copy it to `docs/reference/data/usaspending-data-catalog.json` in the same PR as the provenance record for these endpoint choices.

### 3. Data Extraction

One module: `pipelines/reference/federal_appropriations_ingest.py`, `--stream {omb_budauth, omb_outlays, usa_account_balances, usa_agency, usa_agency_periods, usa_total_resources, usa_account_roster, usa_defc, derived, all}` + `--smoke` (throwaway URIs; first 200 xlsx rows / first 3 agencies / first page only). **Stream→dataset map (§4 numbers):** `omb_budauth`→#1 · `omb_outlays`→#2 · `usa_account_balances`→#3 AND #4 (federal + treasury, one mint each) · `usa_agency`→#5 · `usa_agency_periods`→#6 · `usa_total_resources`→#7 · `usa_account_roster`→#8 · `usa_defc`→#9 · `derived`→#10 (last; depends on #1,#3). Lift the multi-stream skeleton, fetch helper, fail-closed gate pattern, and ledger writes from `industry_cost_structure_ingest.py`. **The rate governor is net-new (see Kickoff.6) — it is NOT in the predecessor; do not assume it.** Every network call in this module routes through `pipelines/_lib/rate_governor.py`.

- **xlsx:** `openpyxl` read-only. Melt wide→long: one row per (dimension tuple, year-column) where the cell is non-blank. Preserve every dimension column verbatim as a string.
- **API:** `requests` with backoff; land the raw JSON field names as columns (snake_case as returned) — no renaming beyond flattening.
- Every dataset gets `source` (resolved URL), `ingested_at`, and BTREE indexes on its §4 keys.

### 4. Required output streams

| # | Lance dataset | Grain (1 row =) | est. rows | BTREE keys |
|---|---|---|---:|---|
| 1 | `active/omb_budget_authority/` | account × year | ~250–290K | `agency_code`, `account_code`, `year` |
| 2 | `active/omb_outlays/` | account × year | ~250–290K | `agency_code`, `account_code`, `year` |
| 3 | `active/usaspending_federal_account_balances/` | federal account × fiscal_year | ~18–25K | `federal_account_symbol`, `fiscal_year` |
| 4 | `active/usaspending_treasury_account_balances/` | treasury account (TAS) × fiscal_year | ~90–150K | `treasury_account_symbol`, `fiscal_year` |
| 5 | `active/usaspending_agency_fy/` | toptier agency × fiscal_year | ~1–2K | `toptier_code`, `fiscal_year` |
| 6 | `active/usaspending_agency_period_obligations/` | toptier agency × fiscal_year × period | ~10–15K | `toptier_code`, `fiscal_year`, `period` |
| 7 | `active/usaspending_total_budgetary_resources/` | fiscal_year × fiscal_period | ~118 | `fiscal_year`, `fiscal_period` |
| 8 | `active/usaspending_federal_account_roster/` | federal account | ~2.3K | `account_number` |
| 9 | `active/usaspending_def_codes/` | DEFC code | ~52 | `code` |
| 10 | `active/approp_vs_obligated_fy/` (derived) | fiscal_year | ~60–70 | `fiscal_year` |

**Column specs (load-bearing ones — types are the contract):**

- **#1 & #2 (`omb_budget_authority`, `omb_outlays`):** `agency_code STR`, `agency_name STR`, `bureau_code STR`, `bureau_name STR`, `account_code STR NULLABLE`, `account_name STR`, `treasury_agency_code STR NULLABLE`, `cgac_agency_code STR NULLABLE`, `subfunction_code STR`, `subfunction_title STR`, `bea_category STR` (Mandatory/Discretionary/Net interest), `budget_status STR` (On-/Off-budget), `year I32`, `is_transition_quarter BOOL`, `is_estimate BOOL`, `budget_year I32` (vintage parsed from filename), `value_musd F64`, `row_kind STR` ('account'|'aggregate_receipt'), `source STR`, `ingested_at TS`.
- **3 (`usaspending_agency_fy`):** `toptier_code STR`, `agency_name STR`, `abbreviation STR NULLABLE`, `agency_slug STR`, `fiscal_year I32`, `agency_budgetary_resources F64`, `agency_total_obligated F64`, `agency_total_outlayed F64`, `total_budgetary_resources F64` (gov-wide denominator), `source`, `ingested_at`.
- **#3 & #4 (`usaspending_federal_account_balances`, `usaspending_treasury_account_balances`):** land **all 22 verified CSV columns verbatim** (§2.5) — do not subset, do not rename. Add `fiscal_year I32`, `fiscal_period I32` (parsed from `submission_period`, e.g. `FY2025P12`), `account_level STR` ('federal'|'treasury'), `source STR`, `ingested_at TS`. Money columns cast to `F64`; **negatives are real** (deobligations, offsetting collections) — never `abs()`. The treasury-account file carries additional TAS-identifying columns beyond the federal-account file's 22 — land whatever it returns, lossless, and record the observed column list in the run record.
- **#5 (`usaspending_agency_fy`):** `toptier_code STR`, `agency_name STR`, `abbreviation STR NULLABLE`, `agency_slug STR`, `fiscal_year I32`, `agency_budgetary_resources F64`, `agency_total_obligated F64`, `agency_total_outlayed F64`, `total_budgetary_resources F64` (gov-wide denominator), `source`, `ingested_at`.
- **#6 (`usaspending_agency_period_obligations`):** `toptier_code STR`, `fiscal_year I32`, `period I32`, `obligated F64`, `source`, `ingested_at`.
- **#7 (`usaspending_total_budgetary_resources`):** `fiscal_year I32`, `fiscal_period I32`, `total_budgetary_resources F64`, `source`, `ingested_at`.
- **#8 (`usaspending_federal_account_roster`):** `account_number STR`, `agency_identifier STR`, `account_code STR`, `account_name STR`, `account_id I64`, `managing_agency STR`, `managing_agency_acronym STR NULLABLE`, `source`, `ingested_at`. Roster/crosswalk only (from `/federal_accounts/`, which has no appropriated/obligated columns) — NOT the resource feed (#3).
- **#9 (`usaspending_def_codes`):** every field the endpoint returns, verbatim (`code`, `public_law`, `title`, `urls`, `disaster`, …), plus `source`, `ingested_at`.
- **10 (`approp_vs_obligated_fy`, derived):** `fiscal_year I32`, then the OMB side split so a receipts-netting error can't hide: `omb_gross_ba_musd F64` (Σ of positive `row_kind='account'` rows, `is_transition_quarter=false`), `omb_offsetting_receipts_musd F64` (Σ of the negative receipt rows), `omb_net_ba_musd F64` (total), `omb_discretionary_ba_musd F64` (the `BEA Category='Discretionary'` cut), `omb_outlays_musd F64`; then the USAspending side from stream 3: `usa_budget_authority_appropriated_usd F64`, `usa_total_budgetary_resources_usd F64`, `usa_obligations_incurred_usd F64`, `usa_unobligated_balance_usd F64`, `usa_gross_outlay_usd F64`; then `is_estimate BOOL`, `coverage_note STR`, `source`, `ingested_at`. **Never sum OMB `value_musd` blind** — that yields budget authority *net of offsetting receipts across all categories*, which is NOT "money appropriated" and is NOT comparable to USAspending's gross `budget_authority_appropriated_amount`. `coverage_note` MUST state: USAspending's appropriated column is a gross-discretionary basis and compares to `omb_gross_ba_musd` / `omb_discretionary_ba_musd`, never `omb_net_ba_musd`. **`usa_unobligated_balance_usd` is the appropriated-but-unspent measure the demo wants.** The two systems do not tie out and are not meant to — land both, label both, reconcile neither. **These two systems do not tie out and are not supposed to** — OMB is $ millions on a budget-account basis including receipts and off-budget accounts; USAspending is $ actual on a TAS/DABS-submission basis covering only agencies that submit. Land both, label both, and write the delta into `coverage_note`. **Do not reconcile them to zero; do not pick a winner.**

### 5. R2 Layout

`s3://data-sink/active/<dataset>/` — one Lance dataset per §4 row, full deterministic rebuild (overwrite), no appends.

### 6. Migration / audit ledger

`ops.federal_appropriations_ingest_runs` in HQX (`HQX_DB_URL_POOLED`), lifted from the ledger shape in `industry_cost_structure_ingest.py`: `run_id`, `stream`, `resolved_url`, `source_bytes`, `rows_written`, `datasets` (jsonb), `started_at`, `finished_at`, `status`, `disposition`, `notes`. **`status` obeys canonical L4 — CHECK `IN ('running','completed','failed')` and nothing else** (a wider CHECK breaks idempotent re-runs). The throttle/block/partial vocabulary rides a separate free-text `disposition` column (`{'ok','throttled','blocked','partial','none'}`), never the CHECK'd `status`. One row per stream per run; `IF NOT EXISTS` (L3).

### 7. Downstream wiring — DEFERRED

Nothing downstream is wired in this cycle. Lance is the SoR; read gateways, sidecar promotion, and demo bakes each consume these datasets in their own cycles (see `## Out of scope`).

### 8. Validation Gate

Fail-closed. Hard-fail the stream (and the run) on any of:

- **omb_budauth:** header row does not match the 12 verified dimension labels in order → fail. Year-column count `!= 57` → fail (schema drift; surface, do not adapt silently). Rows written `< 200,000` or `> 400,000` → fail. Distinct `year` values must include 1976 and 2031. `is_estimate=true` must cover exactly years `>= (budget_year - 1)` where `budget_year` is parsed from the resolved workbook filename; `is_estimate=false` for all earlier years. (Do NOT hardcode 2026 — that breaks on the next vintage, which §2.3 is built to fetch.) `omb_gross_ba_musd` for FY2025 must be positive and within a defensible discretionary+mandatory band; `omb_offsetting_receipts_musd` must be **non-zero** (a zero proves the negative receipt rows were dropped, not summed). The old blind `$4.0T–$12.0T` band is removed — it was wide enough to swallow a multi-trillion receipts-netting error.
- **omb_outlays:** same gates after the §2.2 discovery step confirms the layout; if the layout differs, fail with the observed header printed — do not improvise a parser.
- **usa_agency:** `< 100` agencies → fail (verified: 111). Every landed `agency_budgetary_resources` must be non-null. FY2025 gov-wide `total_budgetary_resources` must be `> $10T` (verified sample showed `$15.82T` for FY2026).
- **usa_account_balances:** the FY2025 federal-account file must carry **exactly the 22 verified column names in order** (§2.5) → fail on drift, printing the observed header. Row count for FY2025 federal-account level must be within `1,700–2,300` (verified: 1,959). Every FY swept must produce a `finished` job → a stream that times out on a mint is logged and retried once, then fails the run. `budget_authority_appropriated_amount` must be non-null on `> 90%` of rows.
- **usa_total_resources:** `< 100` rows → fail (verified: 118).
- **usa_account_balances back-sweep (FY2017–2024):** each swept FY must land `>= 1,000` federal-account rows and exactly the 22-column header → fail the year on either (an empty/malformed early-year ZIP must not land silently; only FY2025 was content-verified at authoring).
- **usa_defc:** `!= 52` codes → **warn, not fail** (the registry legitimately grows), but the run record must state the new count and diff. Code `Z` (IIJA) must be present → fail if absent.
- **derived:** every `fiscal_year` present in the USAspending columns must also be present in the OMB columns → fail on a one-sided year.
- Smoke run (`--smoke`) must pass end-to-end to throwaway URIs before the full run.

### Evidence

Captured 2026-07-27 UTC. Probes run per L43/L44.

```
=== A) USAspending agency budgetary_resources ===
$ curl -sS "https://api.usaspending.gov/api/v2/agency/012/budgetary_resources/"
HTTP 200 ct=application/json bytes=5158
{"toptier_code":"012","agency_data_by_year":[{"fiscal_year":2026,
 "agency_budgetary_resources":476747723527.36,"agency_total_obligated":168205121918.92,
 "agency_total_outlayed":165721840971.65,"total_budgetary_resources":15823226897068.67,
 "agency_obligation_by_period":[{"period":2,"obligated":3284372873.82},
  {"period":3,"obligated":28901819476.97},{"period":4,"obligated":89239870217.54},
  {"period":5,"obligated":108007965059.2},{"period":6,"obligated":124034779614.36},
  {"period":7,"obligated":147512794326.58},{"period":8,"obligated":168205121918.92}]},
 {"fiscal_year":2025,"agency_budgetary_resources":463595143994.27,
  "agency_total_obligated":276630012349.95,"agency_total_outlayed":253236497823.79, …

=== B) USAspending federal_accounts (POST, fy=2025) ===
HTTP 200 ct=application/json bytes=1067
{"count":2261,"limit":3,"page":1,"fy":"2025","next":2,"hasNext":true,"results":[
 {"account_id":4616,"account_number":"028-8006","account_name":"Federal Old-Age and
  Survivors Insurance Trust Fund - Treasury Managed, Social Security Administration",
  "budgetary_resources":1433955632446.69,"agency_identifier":"028",
  "managing_agency":"Social Security Administration","managing_agency_acronym":"SSA"},
 {"account_id":4324,"account_number":"020-0550","account_name":"Interest on the Public
  Debt (Indefinite), Bureau of the Fiscal Service, Treasury", … }]}

=== E) DEF codes — supplemental-act tagging ===
HTTP 200 bytes=12479 — TOTAL DEFC: 52
  all P.L. 119-* present:
   AAL | Emergency P.L. 119-4  | Full-Year Continuing Appropriations and Extensions Act, 2025
   AAM | Wildfire Suppression P.L. 119-74 | Commerce, Justice, Science; Energy and Water…
   AAN | Emergency P.L. 119-75 | Consolidated Appropriations Act, 2026
   AAO | Disaster  P.L. 119-75 | Consolidated Appropriations Act, 2026
   AAP | Disaster  P.L. 119-86 | Homeland Security and Further Additional Continuing Approp…
  search for 'beautiful' | '119-21' | 'reconcil'  ->  ZERO HITS
  (reference points present: N=CARES P.L.116-136, V=ARPA P.L.117-2, Z=IIJA P.L.117-58)

=== F) toptier agency list ===
HTTP 200 bytes=52730 — agencies: 111
keys: ['abbreviation','active_fq','active_fy','agency_id','agency_name','agency_slug',
 'budget_authority_amount','congressional_justification_url',
 'current_total_budget_authority_amount','obligated_amount','outlay_amount',
 'percentage_of_total_budget_authority','toptier_code']

=== G) account download — the CORRECT route (VERIFIED END-TO-END) ===
POST https://api.usaspending.gov/api/v2/download/accounts/
body {"account_level":"federal_account","filters":{"fy":2025,"quarter":4,
      "submission_types":["account_balances"]},"file_format":"csv"}
 -> HTTP 200, 672 B
 {"status_url":"…/api/v2/download/status?file_name=FY2025P01-P12_All_FA_AccountBalances_…zip",
  "file_name":"FY2025P01-P12_All_FA_AccountBalances_2026-07-27_H20M43S37362903.zip",
  "file_url":"https://files.usaspending.gov/generated_downloads/FY2025P01-P12_All_FA_…zip"}
poll 1 -> status=finished  total_size=138.461 (KB)  total_rows=1959  total_columns=22
GET file_url -> HTTP 200, 138,461 bytes
unzip -l -> FY2025P01-P12_All_FA_AccountBalances_2026-07-27_H20M43S37_1.csv  (727,415 B)

L44 header (22 cols, verbatim, in order):
 owning_agency_name, reporting_agency_name, submission_period, federal_account_symbol,
 federal_account_name, agency_identifier_name, budget_function, budget_subfunction,
 budget_authority_unobligated_balance_brought_forward,
 adjustments_to_unobligated_balance_brought_forward_cpe,
 budget_authority_appropriated_amount, borrowing_authority_amount, contract_authority_amount,
 spending_authority_from_offsetting_collections_amount, total_other_budgetary_resources_amount,
 total_budgetary_resources, obligations_incurred,
 deobligations_or_recoveries_or_refunds_from_prior_year, unobligated_balance,
 gross_outlay_amount, status
sample r1: Government Accountability Office | FY2025P12 | 005-0107 | Salaries and Expenses…
  budget_authority_appropriated_amount=821,894,000.00  total_budgetary_resources=1,057,313,328.14
  obligations_incurred=943,770,259.66  unobligated_balance=113,543,068.48
  gross_outlay_amount=928,445,525.56
sample r2: The Judicial Branch | FY2025P12 | 010-0930 | Court Security…
  appropriated=625,967,000.00  resources=701,476,888.19  obligations=662,344,993.97
  unobligated=39,131,894.22  outlay=676,273,578.24

=== G2) routes that are NOT the answer ===
POST https://api.usaspending.gov/api/v2/bulk_download/accounts/ -> HTTP 404  (wrong path segment)
GET  https://files.usaspending.gov/                             -> HTTP 404
GET  https://files.usaspending.gov/database_download/           -> 200 HTML landing page
     (full USAspending PostgreSQL archive FY2001→present — hundreds of GB, OUT OF SCOPE)
POST /api/v2/bulk_download/list_agencies/ {"type":"account_agencies"} -> 200, 10,124 B (roster)

=== H) gov-wide total budgetary resources ===
GET https://api.usaspending.gov/api/v2/references/total_budgetary_resources/ -> 200, 10,161 B
rows: 118 — {'fiscal_year':2026,'fiscal_period':9,'total_budgetary_resources':16047087458201.36}
             {'fiscal_year':2026,'fiscal_period':8,'total_budgetary_resources':15823226897068.67}

=== I) OMB Public Budget Database — discovery ===
$ curl -sSL "https://www.whitehouse.gov/omb/information-resources/budget/supplemental-materials/" | grep -oiE 'href="[^"]*(budauth|outlay)[^"]*"'
 href="https://www.whitehouse.gov/wp-content/uploads/2026/04/budauth_fy2027.xlsx"
 href="https://www.whitehouse.gov/wp-content/uploads/2026/04/outlays_fy2027.xlsx"
(guessed .zip paths under uploads/2026/02 and uploads/2026/03 both 404 — the .xlsx pair above is the live release)

=== L44: budauth_fy2027.xlsx content verification ===
GET -> HTTP 200 ct=application/vnd.openxmlformats-officedocument.spreadsheetml.sheet
      bytes=1610953
sheets: ['Sheet1']   dims: 5129 rows x 69 cols
r1: ['Agency Code','Agency Name','Bureau Code','Bureau Name','Account Code','Account Name',
     'Treasury Agency Code','CGAC Agency Code','Subfunction Code','Subfunction Title',
     'BEA Category','On- or Off- Budget','1976','TQ', …]
r2: ['001','Legislative Branch','00','Legislative Branch','','Receipts, Central fisc…',
     '','','803','Central fiscal operati…','Mandatory','On-budget','-287','-132']
r3: ['001','Legislative Branch','00','Legislative Branch','','Receipts, Central fisc…',
     '','','908','Other interest','Net interest','On-budget','-30','-17']
r4: ['001','Legislative Branch','00','Legislative Branch','241400','Charges for services…',
     '','','803','Central fiscal operati…','Mandatory','On-budget','-28','-31']
year columns (cols 13..69, 57 total):
 ['1976','TQ','1977','1978','1979','1980','1981','1982','1983','1984','1985','1986','1987',
  '1988','1989','1990','1991','1992','1993','1994','1995','1996','1997','1998','1999','2000',
  '2001','2002','2003','2004','2005','2006','2007','2008','2009','2010','2011','2012','2013',
  '2014','2015','2016','2017','2018','2019','2020','2021','2022','2023','2024','2025','2026',
  '2027','2028','2029','2030','2031']
NOTE: negative values in r2/r3 are receipt accounts — real, not errors.
NOTE: blank Account Code on r2/r3 — aggregate receipt rows, land with row_kind='aggregate_receipt'.

=== outlays_fy2027.xlsx ===
HEAD -> HTTP 200 but Content-Length: 0  (misleading; do not trust HEAD on this host)
GET  -> HTTP 200 bytes=2144756   <-- fetched, NOT opened. §2.2 L44 discovery is REQUIRED.
```

### Execution Command

```bash
cd /Users/benjamincrane/core-x   # (or your worktree checkout root)
git checkout -b claude/federal-appropriations-ingest

# 1. smoke every stream to throwaway URIs
doppler run -p core-x -c prd -- uv run --with pylance --with pyarrow --with duckdb \
  --with openpyxl --with requests --with boto3 --with 'psycopg[binary]' \
  python -m pipelines.reference.federal_appropriations_ingest --stream all --smoke

# 2. full run
doppler run -p core-x -c prd -- uv run --with pylance --with pyarrow --with duckdb \
  --with openpyxl --with requests --with boto3 --with 'psycopg[binary]' \
  python -m pipelines.reference.federal_appropriations_ingest --stream all
```

## Surfaces

| Atom | Path |
|---|---|
| Migration | `migrations/…_ops_federal_appropriations_ingest_runs.sql` (IF NOT EXISTS) |
| Code | `pipelines/reference/federal_appropriations_ingest.py` (new) |
| Lance datasets | `s3://data-sink/active/{omb_budget_authority,omb_outlays,usaspending_federal_account_balances,usaspending_treasury_account_balances,usaspending_agency_fy,usaspending_agency_period_obligations,usaspending_total_budgetary_resources,usaspending_federal_account_roster,usaspending_def_codes,approp_vs_obligated_fy}/` |
| Provenance | `docs/reference/data/usaspending-data-catalog.json` (operator-supplied DCAT catalog, §2.8) |
| Ledger | `ops.federal_appropriations_ingest_runs` (HQX) |
| Run record | `docs/reference/` run-record note incl. the OBBA/DEFC negative finding (§2.6) |

## Lessons learned (cite, don't re-explain)

Canonical: `~/Desktop/hq/inventory/DATA-FACTORY-LESSONS.md` (`-LESSONS-LEARNED.md` is FROZEN — do not read it).
- **L0** worktree discipline · **L1** Doppler shell expansion.
- **L3 / L4** — migration timestamp at write-time; audit-ledger CHECK enum is the 3 values `('running','completed','failed')` (§6).
- **L56** — probe live column count/headers before parsing: budauth is pre-verified in `### Evidence`; **outlays is NOT** (HEAD returns `Content-Length: 0`; GET the full body — never scope from HEAD) and carries a mandatory in-run discovery step (§2.2).
- **L54** — no `LIST<VARCHAR>` in Lance 1.5.x; multi-value columns are pipe-joined VARCHAR.
- **L60** — register the new source(s) in `ops.data_source_catalog` (`ON CONFLICT (source_slug) DO NOTHING`).

## Out of scope (don't do these)

- **Sidecar promotion.** These datasets do not enter the query-sidecar in this directive. If demand exists, it goes through the `sidecar-gaps` → `sidecar-build` cycle, gated separately.
- **Demo-bake wiring.** Do not touch `scripts/demo_bakes/` or any gc-hq-new TS artifact. Producing an appropriated-vs-obligated card is a separate cycle that consumes these datasets.
- **Manufacturing an OBBA total.** Per §2.6, no tagged feed exists. Land the DEFC registry and the evidence; do not derive, estimate, or assert an OBBA figure anywhere in this cycle.
- **Reconciling OMB to USAspending.** Different bases, different coverage. Land both, label both (§4 stream 7).
- **The bulk File-A download route** (§2.7, verified 404).
- Audience/cohort marts and cross-source bridges over these datasets.

## Iteration budget

Two workbook parses (~4 MB total), ~111 + ~25 paginated API calls, ~550K rows written across 7 datasets. Small. The only real unknowns are the outlays layout (§2.2) and the FY sweep depth on `/federal_accounts/` (§2.5) — both bounded by explicit discovery steps. Single-session, single PR.

## Definition of done

- [ ] Source(s) registered in `ops.data_source_catalog` (L60, `ON CONFLICT DO NOTHING`).
- [ ] Migration applied (`ops.federal_appropriations_ingest_runs` exists, `IF NOT EXISTS`).
- [ ] `--smoke` passed end-to-end on every stream to throwaway URIs.
- [ ] §2.2 outlays L44 discovery run; observed sheetnames/dims/header row recorded in the run record.
- [ ] DCAT catalog copied to `docs/reference/data/usaspending-data-catalog.json`.
- [ ] §2.3 URL discovery implemented (regex off the supplemental-materials page), resolved URLs + byte sizes recorded in the ledger.
- [ ] Full run completed for all 10 datasets; every §8 gate passed.
- [ ] `ds.count_rows()` verified per dataset against the §4 estimates; deviations explained in the run record.
- [ ] BTREE indexes built on every §4 key.
- [ ] R2 listing verified under `s3://data-sink/active/` for all 10 prefixes.
- [ ] Ledger rows present, one per stream, `status='ok'`.
- [ ] **OBBA/DEFC negative finding (§2.6) written into the run record and the cycle report, verbatim, including the IIJA (`Z`) analogue note.**
- [ ] PR opened and self-merged (`gh pr merge --squash --delete-branch`) per L39.
- [ ] `git -C /Users/benjamincrane/core-x pull` && `git log -1 --oneline` confirms the merge on disk.
- [ ] Cycle report written.

## Execution log (executor fills in)

- [ ] Branch created
- [ ] Module written
- [ ] Migration applied
- [ ] Smoke passed
- [ ] omb_budauth landed
- [ ] outlays discovery run + omb_outlays landed
- [ ] usa_agency landed
- [ ] usa_account_balances landed (federal + treasury levels)
- [ ] usa_total_resources landed
- [ ] usa_federal_account_roster landed
- [ ] usa_agency_period_obligations landed
- [ ] usa_defc landed
- [ ] derived approp_vs_obligated_fy landed
- [ ] Gates passed
- [ ] PR merged
- [ ] Operator checkout pulled + verified

## Final result (executor fills in)

- Per-dataset row counts:
- Resolved source URLs + byte sizes:
- Outlays workbook observed layout:
- FY range swept on `/download/accounts/` (+ per-FY row counts):
- DEFC count observed (vs 52 baseline) + diff:
- Wall-clock:
- Failures skipped (agency codes, rows) + samples:
- PR:
- Cycle report path:
