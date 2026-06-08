# 90-Day Attachment Substrate — Filename Taxonomy & Routing Sizing

**Date:** 2026-06-08. **Purpose:** size the pre-extraction routing gates for the 90-day prime-winners
attachment cache (`active/sam_attachment_files_90day/`, 126,901 downloaded files / 213.72 GB) by
profiling the `file_name` column. Read-only. All counts measured against the live Lance ledger;
`content_length` equals `size_downloaded` to the byte per tier. Recommendations are labeled as such;
everything else is measured.

> **TL;DR:** Filename routing can confidently classify **at most ~25%** of the cache, and that quarter
> is **30–46% contaminated** by substring false-positives. **74.8% of files (130.7 GB) are
> generically named** and cannot be classified by filename at all. One proposed "drop" token (`%rep%`)
> would silently discard real drawings and engineering reports. **Content extraction is unavoidable for
> the majority; filename tiers are a prioritization signal, not a safe drop filter.**

---

## 1. Taxonomy as specified (substring `ILIKE`, tiers evaluated in order)

Tier rules (mutually exclusive, first match wins): **T1 Scope** = `%SOW% %PWS% %SOO% %SPEC% %DRAWING%
%STATEMENT OF WORK% %TECH%` · **T2 Pricing** = `%SCA% %DBA% %WAGE% %WD% %RATE%` · **T3 Boilerplate** =
`%SF1449% %SF30% %SF33% %PPQ% %CDRL% %REP% %CERT% %CLAUSE% %PAST PERFORMANCE%` · **T4 Generic** = the rest.

| Tier | Files | % of total | Size (GB) | Text-extractable (pdf/docx/doc/txt) |
|---|--:|--:|--:|--:|
| **T4 — Generic / blind spot** | 94,897 | **74.8%** | 130.73 | 84,197 |
| **T1 — Explicit Scope** | 15,184 | 12.0% | 61.82 | 14,560 |
| **T3 — Boilerplate (proposed drop)** | 10,172 | 8.0% | 12.49 | 9,730 |
| **T2 — Pricing** | 6,648 | 5.2% | 8.68 | 6,414 |
| **Total** | 126,901 | 100% | 213.72 | 114,901 |

**Tier-4 random sample (15):**
```
B01  75H70124Q00024.pdf
Combined Synopsis Solicitation 1333ND24QNB030502.docx
J-0200000-13 ELINs Amend 0005.xlsx
Solicitation AMENDMENT  W911N221R0004-0002 conformed copy.pdf
SELF-PERFORMED CALCULATIONS.pdf
Physical Data_OR NPS CRLA 2018(1).zip
Attachment 5 - Price List - Greenville.pdf
W9123823R0004-0001.pdf
SP700019R1001-0011.pdf
Amendment 2 QuTI Response to Questions.pdf
Amendment_A001.pdf
FY26 Q3 KOSHER BREAKDOWN.pdf
N4008421B0092-Amendment 0001.pdf
Attachment 3_Questions and Answers.pdf
SPRDL122R0128-0002.pdf
```
Tier 4 is dominated by bare solicitation/award numbers, amendments, Q&A, combined-synopsis docs, and
price lists — no filename rule classifies these.

---

## 2. Contamination — the tier counts above are inflated by substring matching

Each tier's tokens match substrings, not words. Measured split of each tier into its clean driver vs.
the rows that matched **only** via a dirty substring:

| Tier | Clean driver | Dirty-substring-only | Contamination |
|---|--:|--:|--:|
| T1 Scope | 10,388 (`sow/pws/soo/drawing/statement of work`) | **4,796** (`spec`/`tech`) | **31.6%** |
| T2 Pricing | 3,589 (`wage/dba`) | **3,059** (`sca`/`wd`/`rate`) | **46.0%** |
| T3 Boilerplate | 6,355 (`sf1449/sf30/sf33/ppq/cdrl/clause/past performance`) | **3,817** (`rep`/`cert`) | **37.5%** |

**`%SPEC%` matches "in­SPEC­tion / in­SPEC­tor / SpectRE"** — live T1 false positives:
```
Exterior Cleaning Inspection Sheet .xlsx
Exhibit 3 Inspection form.pdf
2020.03.30 sf30 SpectRE RFP 80HQTR20R0011- Amendment 4.pdf
```

**`%RATE%` matches "corpo­RATE / st­RATE­gic / demonst­RATE­d / c­RATE­r / calib­RATE"; `%SCA%` matches
"SCAN / scanning / TES­CAN / counter­SCArp"** — live T2 false positives:
```
Attachment A- Corporate Experience Questionnaire.pdf
Attachment 1 Strategic Plan Engagement Support.pdf
Attachment 24 - Demonstrated Prior Experience.docx
crla201801_Crater Lake No Fly Zone.pdf
D.19 Document Scanning Policy 02.pdf
W911KF-21-Q-0018 DRK TESCAN Sole Source Solicitation
```
(Genuine `SCA_MI_…` / `SCA_CA_…` wage determinations do exist underneath the noise.)

**`%REP%` is the dangerous one — it is in the proposed DROP tier and matches "REP­lace / REP­ort /
REP­air / REP­resentative"** — live T3 false positives that are actually scope:
```
Attachment 6 - MAHG221069 Replace Fire Alarms Multiple_DWG_BLDG   (a drawing)
Attachment 23 - Asbestos Survey and Lead Paint Report.pdf
OR DOT 18(1) Barnard Bridge Hydraulic Report.pdf
Cable Repair and Test.xlsx
```
**Routing T3 to `/dev/null` would silently discard real drawings and engineering reports.**

---

## 3. Corrected, defensible numbers (token-boundary, not substring)

- **High-confidence Scope: 11,195 files / 44.9 GB.** Token-boundary regex over
  `sow|pws|soo|statement of work|scope of work|performance work statement|statement of objectives|specification|drawings?`
  — keeps `specification`/`drawings` as words, excludes `inspection`. This is the defensible "gold" count
  (vs. the inflated loose-T1 15,184).
- **Safely droppable by filename: ~6,355 files (~5%)** — clean explicit-form tokens only, **excluding**
  the `rep`/`cert` substring trap.
- **Unclassifiable by filename: 94,897 (74.8%, 130.7 GB)** + the contaminated remainder.

---

## 4. Implication & recommended routing (recommendation, not measured)

Filename routing covers ~25% of the corpus and that quarter carries 30–46% substring noise; 75% is
generic. A "route by filename, extract only the gold, drop the rest" pipeline therefore saves little
and is unsafe (it discards real scope via `%rep%`, misroutes inspections as scope, and leaves the
majority unclassified).

**Recommended:** treat the filename tiers as a *prioritization* signal, not a hard filter.
1. **Extract the 11,195 high-confidence scope first** (44.9 GB) — fastest path to GTM value.
2. **Then extract across all ~114,901 text-extractable files**, classifying on the **first-page
   content header** (`PERFORMANCE WORK STATEMENT`, `STATEMENT OF WORK`, `WAGE DETERMINATION`, `SF 1449`),
   which is where ground truth lives — this is the only way to recover scope hidden in `Attachment 1.pdf`
   / `Solicitation.pdf`.
3. **Drop only the ~6,355 confidently-boilerplate** files up front; never drop on `%rep%`/`%cert%`.

---

## 5. Verification basis (2026-06-08)

- Source: `s3://data-sink/active/sam_attachment_files_90day/` (Lance), filtered `status='downloaded'`
  (126,901 rows). Tiering via ordered `CASE` of `lower(file_name) LIKE` (directive spec); contamination
  splits via `count(*) filter(...)`; high-confidence scope via `regexp_matches` with a non-letter token
  boundary; Tier-4 and FP samples via `ORDER BY random() LIMIT`.
- Size columns: `content_length` and `size_downloaded` agree to the byte per tier (sum = 213.72 GB);
  reported as one figure.
- All percentages computed over 126,901. False-positive example filenames are verbatim from the ledger.
