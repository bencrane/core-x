# OMB Apportionment ingest — run record

generated_at: 2026-07-28T04:27:30.078612+00:00
index_link_count: 30372  per_fy: {'2022': 6006, '2023': 6287, '2024': 6539, '2025': 6160, '2026': 5380}
files_fetched: 30372  files_failed: 0
rows: files=30372 lines=515841 footnotes=68719
amount_cols: ['approved_amount']  primary: approved_amount
line_kind mapping: 1000–1999→budgetary_resource, 6000–6999→application_of_resource, else→marker (validated by SF-132 identity gate)
discovery coverage: docs=30372 agencies=104 fys=['2022', '2023', '2024', '2025', '2026']
iteration gate: 28789/28789 filenames encode iteration and all match payload
indexes: {'files': ['BTREE:fiscal_year', 'BTREE:tafs', 'BTREE:iteration', 'BITMAP:agency_code'], 'lines': ['BTREE:fiscal_year', 'BTREE:tafs', 'BTREE:iteration', 'BTREE:line_number', 'BITMAP:line_kind'], 'footnotes': ['BTREE:fiscal_year', 'BTREE:tafs', 'BTREE:iteration']}

## ScheduleData key union (fill / numeric of non-null)
| key | non_null_rows | numeric_rows | numeric_pct |
|---|---:|---:|---:|
| budget_agency_title | 515841 | 0 | 0.0% |
| budget_bureau_title | 515841 | 0 | 0.0% |
| account_title | 515841 | 0 | 0.0% |
| cgac_agency | 515841 | 515841 | 100.0% |
| cgac_acct | 515841 | 502318 | 97.4% |
| schedule_iteration | 515841 | 515841 | 100.0% |
| tafs_iteration_id | 515841 | 515841 | 100.0% |
| line_number | 515841 | 406127 | 78.7% |
| approved_amount | 515841 | 515841 | 100.0% |
| line_description | 515820 | 0 | 0.0% |
| begin_poa | 271698 | 271698 | 100.0% |
| end_poa | 271698 | 271698 | 100.0% |
| availability_type_code | 244143 | 0 | 0.0% |
| line_split | 212653 | 73274 | 34.5% |
| footnote_number | 73876 | 0 | 0.0% |
| allocation_agency_code | 18697 | 18697 | 100.0% |
| allocation_subacct | 2482 | 2482 | 100.0% |

## FootnoteData key union
- footnote_number: 68719
- footnote_text: 68719

## FundsProvidedBy — 4241 distinct values across 30372 files
**P.L. 119-21 (OBBA) present: YES — ['Funds Provided by Public Law 115-334,118-158,119-21', 'Funds Provided by Public Law 115-334, 118-158, and 119-21', 'Funds Provided by P.L. 113-79 Sec. 10007, P.L. 119-21 139 STAT 110 Sec. 10606, 7 U.S. Code §7721', 'Funds Provided by Public Law 113-79, 115-334 and 119-21', 'Funds Provided by Public Law 113-79, 115-334, 118-22, 119-21, and 7 U.S. Code 1416', 'Funds provided by Public Law 115-334 & 119-21', 'Funds Provided by Public Law 113-79, 115-334, and 119-21', 'Funds provided by Public Law 119-21', 'Funds provided by Public Law 115-334, 119-4, and 119-21', 'Funds Provided by Public Law 119-21, 139 STAT 110, Section 10606(c)', 'Funds Provided by Public Law 115-334, 118-42, 119-4 and 119-21', 'Funds Provided by Public Law 115-334, 118-158, 119-4 and 119-21', 'Funds Provided by Public Law 108-358, 115-334, 118-22, and 119-21', 'Funds provided by Public Law 115-334, 118-42, 119-4, 119-21 and Estimated Carryover', 'Funds Provided by Public Law 115-334, 118-22,118-158, and 119-21', 'Funds Provided by Public Law 119-21, H.R. 5371', 'Funds Provided by Public Law 115-334, 119-21, 119-37, and Actual Carryover', 'Funds Provided by Public Law 115-334, 118-158, 119-4, 119-21 and 119-37', 'Funds provided by Public Law  119-21 and 119-37', 'Funds provided by Public Law 119-21 and 119-37', 'Funds Provided by Public Law 113-79, 115-334, 118-22, 119-21', 'Funds Provided by Public Law 119-21', 'Funds Provided by Public Law 116-260; 119-21 (ED 26-023)', 'Funds Provided by Public Law 119-21 (OB3) and Public Law 101-508 (TEPSLF) (ED Log Number 26-053)', 'Funds Provided by Public Law 116-260; 119-21 (ED 26-102)', 'Funds Provided by Public Law 116-260; 119-21; 119-75 (ED 26-137)', 'Funds Provided by Public Law 116-260; 119-21; 119-75 (ED 26-190)', 'Funds provided by PL 119-21', 'Funds provided by PL 119-4 and PL 119-21', 'Funds provided by PL 111-5, PL 117-169, and PL 119-21', 'Funds Provided by Public Law 109-171, 115-271, 117-328, 117-159, 118-42, and 119-21', 'Funds Provided by Public Law 118-47, 119-4, 119-21, 114-254', 'Funds Provided by Public Law 109-171, 115-271, 117-328, 117-159, 118-42, 119-21, and 119-75', 'Funds Provided by Public Law Section 100015 of P.L. 119-21', 'Funds Provided by Public Laws 118-158, 119-4, and 119-21', 'Funds Provided by Public Law 107-107, 108-447, 119-21', 'Funds Provided by Public Law 116-6, 116-93, 116-260, 119-21', 'Funds Provided by Public Law 119-4, 119-21, 999-999, 118-47', 'Funds Provided by Public Law 119-4 & 119-21', 'Funds Provided by P.L. 119-21', 'Funds Provided by Public Law 119-21, 119-75', 'Funds Provided by Public Law 119-74 and 119-21', 'Funds Provided by Public Law 111-145, 113-235, 116-94, 117-103, 119-21', 'Funds Provided by Public Laws 116-136 and 119-21', 'Funds Provided by Public Law 115-334, 118-158 and 119-21', 'Funds Provided by Public Law 115-334, 118-22, and 119-21', 'Funds Provided by Public Law 118-158 & 119-21', 'Funds provided by Public Law 118-22, 119-4 and 119-21', 'Funds Provided by Public Law by 113-79, 118-22 , 119-4, and 119-21', 'Funds Provided by Public Laws 115-334 and PL 119-21, 139 STAT 107-108 Sec. 10601(f)', 'Funds Provided by Public Law 113-79, 115-334, 118-22, 118-158, 119-4, and 119-21', 'Funds Provided by Public Law 115-334, 119-21', 'Funds provided by Public Law 113-79, 115-334 & 119-21', 'Funds Provided by Public Laws 7 U.S.C. 2257, 101-508, 115-334, 119-4, and 119-21', 'Funds provided by FY 2025 Carryover Balances, Est. Recoveries, P.L. 119-21', 'Funds Provided by Public Law 101-508, 110-84,119-21 (25-310)', 'Funds provided by PL 117-169 & PL 119-21', 'Funds provided by PL 117-169 and PL 119-21', 'Funds provided by PL 117-169 & 119-21', 'Funds provided by PL 117-169 and 119-21', 'Funds provided by 117-169 and 119-21', 'Funds Provided by Public Laws 119-4 and 119-21', 'Funds Provided by Public Law 107-107, 108-447, 117-139, 119-21', 'Funds Provided by Public Law 117-169, 119-21']**

Top 30 by file count:
| count | FundsProvidedBy |
|---:|---|
| 1543 | 'Funds provided by Public Law 117-328' |
| 1522 | 'Funds provided by Public Law 117-103' |
| 1365 | 'Funds Provided by Public Law 119-4' |
| 771 | 'Funds provided by Public Law N/A' |
| 724 | 'Funds Provided by Public Law Various' |
| 705 | 'Funds provided by Public Law 116-260' |
| 693 | 'Funds Provided by Public Law 118-47' |
| 686 | 'Funds provided by Public Law' |
| 648 | 'Funds Provided by Public Law' |
| 575 | 'Funds Provided by Public Law 117-328' |
| 537 | 'Funds Provided by Public Law N/A' |
| 522 | 'Funds Provided by Public Law 119-75' |
| 508 | 'Funds Provided by Public Law 000-000' |
| 493 | 'Funds provided by Public Law 118-47' |
| 456 | 'Funds provided by Public Law Various' |
| 439 | 'Funds Provided by Public Law 118-42' |
| 404 | 'Funds Provided by Public Law 119-21' |
| 367 | 'Funds provided by Public Law 000-000' |
| 345 | 'Funds Provided by Public Law (Various)' |
| 308 | 'Funds provided by Public Law (Various)' |
| 299 | 'Funds provided by Public Law 118-42' |
| 292 | 'Funds provided by Public Law NA' |
| 288 | 'Funds Provided by Public Law 117-103' |
| 235 | 'Funds Provided by Public Law 117-58' |
| 234 | 'Funds provided by Public Law 117-58' |
| 202 | 'Funds provided by Public Law 117-2' |
| 201 | 'Funds provided by PL 117-328' |
| 185 | 'Funds Provided by Public Law 119-74' |
| 178 | 'Funds provided by PL 118-42' |
| 178 | 'Funds Provided by Public Law NA' |

---

## Executor notes & decisions (2026-07-28)

### 1. OBBA (P.L. 119-21) IS tagged — the headline finding
`FundsProvidedBy` references **Public Law 119-21** (the One Big Beautiful Bill Act) on
**404 documents** where it is the sole/primary citation, plus ~65 distinct value-strings that
cite it in combination with other public laws (e.g. `"Public Law 119-21 (OB3) and Public Law
101-508 (TEPSLF)"`, `"Section 100015 of P.L. 119-21"`, `"P.L. 119-21 139 STAT 110 Sec.
10606"`). **This is the first OBBA-tagged feed landed in this program.** The sibling
appropriations directive established that USAspending's DEFC does not tag OBBA; OMB
apportionment's `FundsProvidedBy` is therefore the live public-law attribution surface. Per
§2.5 this reports the finding and the full distinct-value distribution — it does NOT assert an
OBBA dollar total (out of scope; requires the TAFS↔federal-account crosswalk).

### 2. Schedule-lines gate recalibrated (1,000,000 → exact completeness + 400k floor)
The directive estimated 50–130 lines/file (~1.5–4M total) and set a 1M floor "(implies a parse
that dropped rows)", explicitly deferring the count to in-run confirmation. **Confirmed in-run:
~17 lines/file → 515,841 lines across 30,372 docs.** The parse is provably complete, not
truncated: `lines_written == sd_rows` exactly, every `files` row has `n_lines > 0`, and the
SF-132 identity Σ(budgetary)==Σ(application) holds within $1 for **all 30,372 documents** (0
mismatches). The 1M floor was calibrated to a high estimate; replaced by the exact completeness
equality (the precise encoding of the gate's stated intent) + a 400k coarse sanity floor.

### 3. Filename grain is payload-first (filenames are not uniform)
~88% of filenames use the clean `TAFS=…_Iteration=N_…` shape; EPA files omit TAFS/Iteration and
Treasury uses `Account=…`. Canonical grain therefore comes from the payload: `iteration` from
`ScheduleData.Iteration` (100% fill), `tafs` from the filename when present else reconstructed
from the dominant `(CgacAgency, availability, CgacAcct)`. The §8 iteration gate validates the
FILENAME iteration against the payload wherever the filename encodes one: **28,789/28,789 match**.

### 4. Shared rate governor — converged on the canonical `pipelines/_lib/rate_governor.py`
The sibling appropriations cycle (PR #1358) landed the shared governor first. This module was
rewired onto that canonical governor (`RateGovernor(host=…)` + `gov.get()` + `ThrottleHalt`),
driven single-threaded (at ≤2 req/s over ~8 KB files one worker saturates the ceiling). The
crawl's resume ledger is an R2 object (`landing/omb_apportionment/cache/_checkpoint.json`) rather
than the governor's local-file checkpoint, because the directive forbids a session-local
checkpoint for the 30K-file crawl. The full crawl (30,372 files, 0 failed, 0 breaker trips) and a
merged-code re-run both pass every gate.

### 5. Index count is not strictly monotonic
Baseline 30,443 (2026-07-27 probe) → 30,368 (crawl) → 30,372 (re-run). OMB adds and removes files
continuously; the gate uses a 25,000 floor + required-FYs-present (2022–2026) rather than an exact
baseline, which correctly tolerates this drift.
