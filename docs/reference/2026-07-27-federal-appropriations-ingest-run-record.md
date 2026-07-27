# Run record — Federal Appropriations & Budget Authority ingest (OMB + USAspending)

**Cycle:** 2026-07-27 · **Module:** `pipelines/reference/federal_appropriations_ingest.py`
**Directive:** `docs/plans/2026-07-27-FEDERAL_APPROPRIATIONS_INGEST_DIRECTIVE.md`
**Ledger:** `ops.federal_appropriations_ingest_runs` (HQX) · **Branch:** `claude/federal-appropriations-ingest-a45f11`

Lands the *appropriated* side of the federal dollar (budget authority + outlays + File-A
account balances), which the plane previously lacked — every prior number was an obligation.
Ten Lance datasets under `s3://data-sink/active/` + one derived FY reconciliation.

---

## ⚠ OBBA / DEFC negative finding (record verbatim — §2.6)

**VERIFIED NEGATIVE FINDING: there is NO DEFC (Disaster Emergency Fund Code) for OBBA
(the One Big Beautiful Bill Act, P.L. 119-21).** DEFC only tags dollars carrying an
emergency / disaster / wildfire-suppression designation; OBBA is a reconciliation act and its
appropriations are not so designated. **Therefore OBBA-attributable dollars cannot be isolated
by tag on USAspending.** Confirmed live 2026-07-27: the `/references/def_codes/` registry
returns 52 codes; a search for `beautiful` / `119-21` / `reconcil` returns ZERO hits. All
`P.L. 119-*` codes present are the FY2025/FY2026 appropriations/CR acts (AAL 119-4, AAM 119-74,
AAN/AAO 119-75, AAP 119-86), none of which is OBBA.

The only defensible routes to an OBBA number are (a) year-over-year deltas in OMB budget
authority at account grain across the FY2026/FY2027 releases, or (b) the FY2027 Budget's own
scoring tables — both **derivations, not a tagged feed.** No OBBA total was manufactured in
this cycle.

**Side benefit (record it):** IIJA (DEFC `Z`, P.L. 117-58) *is* fully taggable on USAspending,
which makes it a real, sourceable analogue for "what happens to obligations after a large
infrastructure act passes." (`usaspending_def_codes` gate hard-fails if `Z` is absent.)

---

## In-run discoveries (things the directive could not pre-verify)

1. **UNIT CORRECTION — OMB detail files are in THOUSANDS of dollars, not millions.** The
   directive (§2.1) stated "Values are $ millions." That is wrong for the OMB Public Budget
   Database *detail* files (`budauth_fy2027.xlsx` / `outlays_fy2027.xlsx`). Verified against
   ground truth: Social Security OASI FY2025 = `1,430,282,000` in the sheet = $1.43T (i.e.
   thousands), FY2025 net BA = $7.60T, discretionary = $1.89T. The melt converts thousands →
   millions (`value_musd = raw / 1000`) so `value_musd` is truthfully $millions and the derived
   reconciliation (`omb_*_musd × 1e6 ≈ USAspending $usd`) is meaningful. Leaving raw thousands
   under a `_musd` name would have injected a **1000× error** into every downstream figure —
   exactly the "silently converts a projection into a fact"-class error the directive warns of.
   Cross-check: OMB gross BA $9.48T ≈ USAspending appropriated $9.54T (~1%).

2. **The OUTLAYS workbook has a DIFFERENT layout than budauth (§2.2 L44 discovery).**
   - `budauth_fy2027.xlsx`: `Sheet1`, 5,129 × 69 — **12** dimension columns + **57** year columns
     (1976..2031 + TQ). Matches the directive's verified layout.
   - `outlays_fy2027.xlsx`: `Sheet1`, 5,761 × 84 — **13** dimension columns (an extra
     `Grant/non-grant split` between `BEA Category` and `On- or Off- Budget`) + **71** year
     columns (**1962**..2031 + TQ). The directive assumed it matched budauth; it does not.
   - Resolution: the melt is layout-adaptive **by column name** (detects the year block as the
     first 4-digit-year / `TQ` column; maps every known dimension by name; lands the extra
     `Grant/non-grant split` losslessly in `grant_split`, NULL for budauth). This is the same
     wide-melt format, not an improvised parser. Hard-fails only if a REQUIRED dimension name
     is missing.

3. **File-A federal header — the directive transcribed 21 names but the file carries 22.** The
   real header ends `… gross_outlay_amount, status_of_budgetary_resources_total,
   last_modified_date` — the directive listed column 21 as `status` (wrong name) and omitted
   `last_modified_date`. Verified live 2026-07-27. The gate asserts the 21 verified names as an
   in-order prefix and tolerates exactly one trailing provenance column, printing the observed
   header on any drift. Treasury File-A carries 32 columns (TAS superset) — landed lossless.

4. **Throttle event — the rate governor halted correctly.** During the first full run the
   treasury File-A back-sweep tripped api.usaspending.gov's throttle; the circuit breaker
   tripped a second time and the run **halted with `disposition='throttled'`** rather than
   grinding into a persistent IP block (the governor's designed behavior). The run was resumed
   after a cool-down; the deterministic ZIP cache + warm-up ramp made the resume light and gentle.

5. **`ops.data_source_catalog` did not exist in HQX.** The L60 canonical catalog was absent
   from the HQX (Supabase) database. Created `IF NOT EXISTS` from the 16-column contract and
   registered both sources (`ON CONFLICT DO NOTHING`). The `data_source_catalog_status` VIEW was
   NOT recreated — no template migration exists in this checkout and downstream wiring is out of
   scope for this cycle.

---

## Sources resolved

| Source | Resolved URL | Bytes |
|---|---|---|
| OMB budget authority | `https://www.whitehouse.gov/wp-content/uploads/2026/04/budauth_fy2027.xlsx` | 1,610,953 |
| OMB outlays | `https://www.whitehouse.gov/wp-content/uploads/2026/04/outlays_fy2027.xlsx` | 2,144,756 |
| USAspending API | `https://api.usaspending.gov/api/v2/*` (keyless JSON + File-A download) | — |

URLs discovered off the supplemental-materials page (§2.3), regex `(budauth|outlays)_fy\d{4}\.xlsx`.

## FY range swept on `/download/accounts/`

FY2017→FY2026 (quarter 4 for closed years FY2017–FY2025; FY2026 at the latest available quarter,
P3, from `submission_periods`). Federal per-FY rows: 2017=1,937 · 2018=1,914 · 2019=1,881 ·
2020=1,918 · 2021=1,949 · 2022=1,928 · 2023=1,949 · 2024=1,965 · **2025=1,959** (matches the
directive's verified count exactly) · 2026=1,902.

## DEFC

52 codes (baseline 52; no drift). `Z` (IIJA) present. No OBBA code (see negative finding above).

---

## Per-dataset row counts (final run — `run_id=bd62b23d-68e0-4cbe-8d4f-89a62bbeb827`, all `status=completed`, `disposition=ok`)

| # | Dataset | Rows | §4 estimate | Note |
|---|---|---:|---|---|
| 1 | `omb_budget_authority` | 292,296 | 250–290K | +2K over — 5,128 accounts × 57 year-cols, dense fill |
| 2 | `omb_outlays` | 408,960 | 250–290K | **higher than est**: outlays has **71** year-cols (1962–2031), not 57 — the directive estimated the budauth layout |
| 3 | `usaspending_federal_account_balances` | 19,302 | 18–25K | ✓ (FY2025=1,959, matches Evidence exactly) |
| 4 | `usaspending_treasury_account_balances` | 82,080 | 90–150K | −8K under — 10 FYs × ~7.4–8.7K TAS rows; estimate was loose |
| 5 | `usaspending_agency_fy` | 1,022 | 1–2K | ✓ |
| 6 | `usaspending_agency_period_obligations` | 7,124 | 10–15K | under — agencies report ~2–8 populated fiscal periods; not all FYs carry period detail |
| 7 | `usaspending_total_budgetary_resources` | 118 | ~118 | ✓ exact |
| 8 | `usaspending_federal_account_roster` | 2,261 | ~2.3K | ✓ (matches Evidence `count:2261`) |
| 9 | `usaspending_def_codes` | 52 | ~52 | ✓ (Z present, no OBBA) |
| 10 | `approp_vs_obligated_fy` (derived) | 56 | 60–70 | 56 = distinct FYs 1976–2031 (TQ excluded); 10 `coverage='both'` years (FY2017–2026) |

All deviations are data-shape realities (cross-validated against ground truth), not parse errors.

## Gate results (all PASSED)

- **omb_budauth**: 12 dims in order · 57 year-cols · 292,296 rows ∈ [200K,400K] · years incl 1976 & 2031 ·
  `is_estimate` = years ≥ (budget_year−1)=2026 exactly · FY2025 gross BA $9.48T (band) · offsetting −$1.88T ≠ 0.
- **omb_outlays**: 13 dims (incl `grant_split`) · 71 year-cols (≥57) · 408,960 rows ∈ [150K,600K] · FY2025 gross outlays > 0.
- **usa_agency**: 111 agencies (≥100) · all resources non-null · FY2025 gov-wide total > $10T.
- **usa_account_balances (federal)**: 22-col header verified in order (`…status_of_budgetary_resources_total,
  last_modified_date`) · FY2025 1,959 ∈ [1700,2300] · back-sweep FY2017–24 each ≥1,000 + header · FY2025 appropriated 100% non-null (>90%).
- **usa_total_resources**: 118 ≥ 100. **usa_defc**: `Z` present; count 52 (no drift).
- **derived**: every USAspending FY present in OMB (no one-sided year); FY2025 OMB gross $9.48T ≈ USAspending appropriated $9.54T (~1%, different bases — the two are labeled, not reconciled).

## Reconciliation snapshot (FY2025)

| Measure | OMB ($M) | USAspending (actual $) |
|---|---:|---:|
| Gross BA / Appropriated | 9,484,053 ($9.48T) | $9.541T |
| Net BA | 7,601,776 ($7.60T) | — |
| Discretionary gross | 1,890,967 ($1.89T) | — |
| Offsetting receipts | −1,882,277 (−$1.88T) | — |
| Outlays / Gross outlay | 7,011,105 ($7.01T) | $9.854T |
| Obligations | — | $10.217T |
| **Unobligated (appropriated-but-unspent)** | — | **$2.883T** |

The two systems are on different bases (OMB budget-account incl. receipts + off-budget; USAspending
TAS/DABS submissions) and are **labeled, not reconciled** (`coverage_note`). They land within ~1% on
the gross/appropriated line, which validates the unit correction.

## Ledger provenance

`ops.federal_appropriations_ingest_runs` retains the full journey, including the earlier run
`e011aa55…` whose `usa_account_balances:treasury` row carries `status='failed', disposition='throttled'`
— the circuit-breaker halt (governor working as designed). Definitive run `bd62b23d…` has all 10 streams
`completed`/`ok`.

## Wall-clock

Definitive run streams: budauth 9s · outlays 10s · agency 4s (cached) · periods 6s · File-A federal 6s
(cached) · File-A treasury 39s (3 new mints) · total 4s · roster 102s (23 pages) · defc 3s · derived 6s.
(First run ~18 min before the throttle halt; ~20 min cool-down before the gentle warm-up resume.)
