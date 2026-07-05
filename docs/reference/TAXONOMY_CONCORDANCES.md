# Taxonomy concordances — `naics_concordance` + `psc_successor_map` (Layer 0)

Two reference dims beside `dec_code_domain_ref`: the official old→new mappings for the two award
taxonomies. FPDS transactions carry codes frozen at action time; these dims are how historical
codes resolve to current vintages instead of fragmenting pooled NAICS×PSC analyses.

**Why they exist (measured, L1 spine 2026-07-05):** 553 of 1,734 distinct L1 NAICS codes are not
in the single-vintage (2022) `naics_reference` — 15.36M txns (14.8% of coded). 1,130 retired PSC
codes carry 10.4M txns. Both recodes run *concurrently* in the spine for years (D3xx still ~40K
txns in 2025; `541712` still ~9K).

## `naics_concordance`

| | |
|---|---|
| URI | `s3://data-sink/active/naics_concordance/` (Lance v2.1, overwrite) |
| Grain | 1 row per `(from_vintage, from_code, to_code)` directed revision edge — 4,603 rows |
| Source | Census "Full Concordance" workbooks: 2002→2007, 2007→2012, 2012→2017, 2017→2022 (census.gov/naics/concordances; the reverse 2022→2017 workbook is the same edges transposed, not ingested) |
| Cols | from/to vintage, code, title (verbatim, split rows describe the piece that moved), `relation` (`identical`\|`one_to_one`\|`split`\|`merge`\|`complex`), provenance |
| Indices | BTREE `from_code`, `to_code` · BITMAP `from_vintage`, `to_vintage`, `relation` |
| Loader | [`pipelines/reference/materialize_naics_concordance.py`](../../pipelines/reference/materialize_naics_concordance.py) `<smoke|build|verify> [--source-dir D]` |

Fail-closed gates: per-file row floor (≥900), ≤5% unparseable rows, 6-digit regex, grain
uniqueness, and revision sentinels verified against the Census files themselves —
`541711(2012)→{541713,541714}`, `541712(2012)→{541713,541715}` (NOT 541714 — that descends from
541711 only), `517311(2017)→{517111}`, `336111(2017)→{336110}`, `111110` stable on every link.

**Resolution to 2022 = transitive closure over the chain** (recursive join on
`from_code = prior.to_code`, stop at codes ∈ `naics_reference`). Measured coverage of the L1
legacy population: **364/553 codes, 96.6% of legacy txns (14.83M/15.36M)**. Known residual: 1997-
vintage codes (`233320`, `421xxx`, `235xxx`, `5133xx`, …) — the chain starts at 2002. Closing it
= ingesting the Census 1997→2002 concordance as a fifth `FILES` entry; deferred until a consumer
needs pre-2002-coded rows.

## `psc_successor_map`

| | |
|---|---|
| URI | `s3://data-sink/active/psc_successor_map/` (Lance v2.1, overwrite) |
| Grain | 1 row per `(old_psc, new_psc)` edge — 2,986 rows, 2,803 distinct old codes; `new_psc` NULL = ended-no-successor (`----` in the manual, 15 edges) |
| Source | October 2020 PSC Manual PDF (acquisition.gov), **Appendix 7 — "PSC Crosswalk from Previous Version of Manual"**. GSA publishes no machine-readable successor file; this appendix is the only official carrier. Two physical tables, column order flipped: pages ~312–313 `(New, Old)` = segment `release_oct2020` (58 edges); pages ~314+ `(Previous, Current)` = segment `historical_cumulative` (2,928 edges, incl. the 721 R&D codes and pre-2020 changes). Parser normalizes both to old→new; release edge wins on collision. |
| Cols | `old_psc`, `new_psc`, `rationale` (verbatim), `mapping_segment`, `is_terminal`, `is_self`, `is_split`, `is_merge`, `page_number`, provenance, `manual_effective_date='2020-10-30'` |
| Indices | BTREE `old_psc`, `new_psc` · BITMAP `mapping_segment`, `is_terminal`, `is_split`, `is_merge` |
| Loader | [`pipelines/reference/materialize_psc_successor_map.py`](../../pipelines/reference/materialize_psc_successor_map.py) `<smoke|build|verify> [--source F.pdf]` |

Fail-closed gates: ≥2,500 distinct old codes, exactly **721 self-mapped `A*` codes** (the
documented R&D carry-forward set — the manual end-dated them and reissued the same IDs with
budget-function-aligned names, so Appendix 7 records them as self-edges with rationale "Expanded
name for clarity"), 2020 IT sentinels (`D301→DC01`, `D302→DA01`, `D303→DH01`, `D305→DB10`,
`D306→DD01`), ≥99% membership of both sides in `psc_reference`, grain uniqueness.

**Consumer policy — self-edges:** the cumulative table carries both `D302→D302` (a pre-2020
revision-in-place) and `D302→DA01` (the 2020 recode). To relabel a *retired* code, filter
`is_self = false` (or resolve WHEN via `psc_reference` start/end dates — lifespans stay there;
this dim holds only FROM→TO). Measured coverage: **1,032/1,130 retired spine PSCs, 99.3% of
retired-code txns (10.33M/10.40M)**. Residual: catch-alls GSA never mapped (`8999`, `9998`,
`8900`, `AT90`, …).

## Composition (the Layer-0 set)

`dec_code_domain_ref` (code→meaning) · `naics_concordance` (NAICS old→new) · `psc_successor_map`
(PSC old→new) · `naics_reference` / `psc_reference` (current taxonomy + PSC lifespans). Derived
artifacts (e.g. vintage-normalized combo columns on any edge/fact base) resolve through these at
build or query time and never re-embed guessed mappings.
