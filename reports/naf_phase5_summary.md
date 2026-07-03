# NAF Phase 5 — Date Normalization & Unified Wage-Arbitrage Benchmark

Two derived layers that turn the NAF SoR into a queryable GTM product surface: (1) true DATE columns
enabling chronologically-correct point-in-time queries, and (2) a county-grain benchmark aligning NAF
vs SCA vs OEWS wages on the shared Census FIPS key.

## Date normalization
`effective_date` / `issue_date` were verbatim PDF strings in mixed formats — lexical sort gave WRONG
recency (the string "9 August 2008" sorts *above* "16 August 2025"). `pipelines/naf/normalize_dates.py`
adds true `DATE32` columns **non-destructively** (original string columns preserved byte-for-byte;
parsed columns appended, dataset overwritten, indexes rebuilt):

| Dataset | effective_date_parsed | issue_date_parsed |
|---|---|---|
| `naf_wage_rates` | 99.94% | 99.99% |
| `naf_nf_payband_ranges` | 100% | — |
| `naf_nf_payband_survey` | 99.97% | 99.97% |

The parser strips DoD boilerplate (`"…beginning on or after <date>."`), signature-block bleed
(`"<date> Wage and Salary Branch"`), footnote markers (`"<date>**"`), and handles DD-Month-YYYY /
Month-DD-YYYY / DD/MM/YYYY / DD/Mon/YYYY + punctuation variants. `BTREE(effective_date_parsed)` added
for point-in-time pushdown; fail-closed ≥99.5% coverage gate.

## `view_county_wage_arbitrage_benchmark` — 502 rows (county_fips grain)
Aligns three independent wage regimes on the shared 5-digit Census FIPS:
- **NAF** — the chronologically LATEST CT-regular schedule for the county's wage area (via
  `effective_date_parsed`) → NA/NL/NS hourly bands + NA-5 / NA-10 grade anchors.
- **SCA** — the county's canonical wage determination (`sca_wd_county_rollup` → `sca_wd_rates`) →
  occupation-rate band (min/max/median).
- **OEWS** — state market wage (`soc_state_wage`) → All-Occupations median + a blue-collar trades band
  (SOC 47/49/51 families, the market analog to NAF Crafts & Trades).

Coverage: **100% NAF, 99.4% SCA, 97.6% OEWS; 488 counties triple-covered.** Indexes
BTREE(county_fips, naf_wage_area) BITMAP(state, has_sca, has_oews).

## Validation — Cobb County GA (FIPS 13067)
Chronologically-correct latest-schedule retrieval confirmed:

| Field | Value |
|---|---|
| NAF wage area / schedule / effective | 034 / 13 / **2025-08-16** (true latest of 24 CT schedules) |
| NAF CT NA-5 (grade 5, steps 1–5) | $15.29 – $17.85 /hr |
| NAF CT NA-10 (grade 10) | $20.78 – $24.25 /hr |
| SCA canonical WD → median | 1977-0193 → $23.91 /hr |
| OEWS trades / all-occ median (GA) | $22.80 / $24.79 /hr |

**Recency proof:** lexical string sort of `effective_date` returns "9 August 2008"; the `DATE32`
`effective_date_parsed` sort correctly returns 2025-08-16 — the exact bug this phase resolves.

**Data-quality note (lossless):** ~1,120 rows (0.07%) carry a faithfully-transcribed source-PDF typo
year (e.g. "30 Mar 3013" for 2013, "16 March 1902" for 2002 — the raw PDF literally prints these;
sibling date columns confirm the true year). These sit at min/max extremes but do **not** pollute the
`effective_date_parsed` recency key the arbitrage view uses (independently verified: the only forward
`effective_date_parsed` values are legitimate "4 July 2026" schedules). Left as transcribed; a consumer
can filter implausible years (< 1985 or > 2027).

## The chain, now temporally correct
`FPDS txn → pop_county_fips → view_county_wage_arbitrage_benchmark → {NAF, SCA, OEWS} wage floors`,
evaluated at the current (latest-effective) NAF schedule.
