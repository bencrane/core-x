# OLMS ↔ SAM §4(c) Pointer Join — Gap-3a Measurement

**Status:** measured 2026-07-11 · **Verdict below governs the Gap-3b (PDF wage-extraction) spend decision.**
Companion to `LABOR_MARKET_SUBSTRATE.md` §3.3.1 and `LABOR_x_GOVCON_CROSSWALK_GTM.md`.

## What was measured

Deterministic match (zero LLM, zero fuzzy scoring at runtime) of
`sam_wd_cba_pointers` (4,298 §4(c) WDs) against `olms_cba_index` (4,849 OLMS OPDR
filings) on: normalized employer name (accent-fold, legal-suffix strip,
parenthetical strip; exact-equality first, then ≥75% token containment) ×
state compatibility (`sam_wd_cba_coverage.state_name` vs OLMS `location`) ×
freshness (OLMS `exp_date`).

## Results

| population | n | matched any-tier | high-confidence + fresh |
|---|--:|--:|--:|
| all pointers | 4,298 | 456 (10.6%) | 38 |
| named pointers | 1,940 | 456 (23.5%) | 38 |
| **2020s pointers (all named — the ones that price current contracts)** | **1,633** | **419 (25.7%)** | **38** |

2020s tier detail: T1 exact-name + state + fresh doc (exp ≥2022) **38** ·
T2 exact + state, stale doc 23 · T3 exact name only 209 · T4 token-containment 149 ·
unmatched 1,214. Distinct OLMS documents matched: **65** (37 fresh).

## Why the ceiling is structural (not a matching-quality problem)

1. **55% of pointers are skeletal** — 2,358/4,298 carry no employer name at all
   (2003–2019 vintages). Nothing joins a nameless pointer.
2. **Corpus-population mismatch** — OLMS OPDR files 1,000+-employee bargaining
   units (legacy industrial: paper mills, autos, telecoms). SAM §4(c) service
   contractors are predominantly small local units that never meet the OLMS
   filing threshold. 58% of the OLMS corpus (2,834/4,849) expired before 2015;
   only 721 filings expire ≥2024.
3. T3/T4 matches (358) bind the right *employer* to the wrong *era/locality* —
   a 2009-expired industrial CBA does not price a 2025 service contract.

## Gap-3b spend implication

- **Full-corpus extraction (4,843 PDFs, 14.4 GB) is NOT justified**: ~99% of the
  corpus never joins a pointer that prices a current contract. LLM extraction
  over ~3 MB/PDF average is a large spend for ~38 usable joins.
- **Matched-docs-only extraction (65 PDFs, 37 fresh) is negligible spend** and
  captures everything the join can currently reach.
- Higher-leverage alternative for §4(c) wage floors: the **OPM NAF CBA corpus**
  (federal-sector, wage-bearing, cited in LABOR_MARKET_SUBSTRATE §3.3 as
  pending) — its population actually overlaps federal service contracting.

## Reproduction

Normalization + blocking logic in this doc's measurement session; inputs are the
three Lance datasets (`sam_wd_cba_pointers`, `sam_wd_cba_coverage`,
`olms_cba_index`) as of their 2026-07-01/02 crawls. If a pointer re-crawl lands
more named/2020s records, re-run the measurement before revisiting the verdict.
