# BEA Input-Output Use Tables — Contingent-Labor Intake per Industry

**Status:** live · **Ingested:** 2026-07-23 UTC · **Module:** [`pipelines/reference/bea_io_use_ingest.py`](../../pipelines/reference/bea_io_use_ingest.py)
**Directive:** `~/Desktop/hq/directives/2026-07-11-bea-io-use-contingent-labor-ingest.md`
**Predecessor:** [`LABOR_SHARE_OF_REVENUE_STACK.md`](LABOR_SHARE_OF_REVENUE_STACK.md)

## Why this exists — the blind leg of the sourcing decomposition

An award dollar's labor is fulfilled three ways: **self-performed** (the prime's own W-2s),
**subcontracted** (visible in FSRS), and **contingent** (staffing-agency / temp / PEO labor).
The third leg is structurally invisible in federal reporting: a prime's payment to a staffing
agency is a *vendor purchase*, not a subaward, so it never crosses the FSRS reporting threshold
and never appears in FPDS. Optum isn't W-2-ing the nurses, and no federal feed says so.

The BEA use table is the only public, economy-wide measurement of that channel. It answers, for
every industry: **how many dollars of "Employment services" (NAICS 5613) does this industry
consume as a purchased input?** Landing the full matrix yields every *other* purchased-input
intensity for free (professional services, facilities, IT, transport…), plus the workbook's own
BEA↔NAICS concordance.

```
award_$ × labor_share × category_mix          ← labor-share stack (live)
                      + contingent intake     ← THIS: emp-svcs input ÷ industry output
```

## Datasets (Gen-3 Lance SoR, `s3://data-sink/active/`, Pattern A direct hydration)

| Dataset | Grain | Rows | Notes |
|---|---|---:|---|
| `bea_io_use_detail` | year × commodity × column | 159,538 | 402×402 benchmark matrix, 3 years |
| `bea_io_use_summary_annual` | year × commodity × column | 206,172 | 71-industry grain, 1997–2023 |
| `bea_sut_naics_concordance` | BEA code (4 levels) | 643 | the detail workbook's own `NAICS Codes` sheet |
| `bea_contingent_labor_intake` | industry × year | 3,067 | derived: 1,150 detail + 1,917 summary-proxy |

All Lance v2.1, `mode="overwrite"` (idempotent full-snapshot replace). Indexes: BTREE
`commodity_code`+`industry_code` (matrices), `bea_code` (concordance), `industry_code`
(derived); BITMAP `year`/`row_kind`/`col_kind`/`grain` (matrices), `bea_level` (concordance),
`grain`/`year` (derived). 17 indexes total.

## Source (one keyless static zip, verified live 2026-07-23)

`https://apps.bea.gov/industry/iTables%20Static%20Files/AllTablesSUP.zip` — 20,439,248 B,
`application/x-zip-compressed`, browser UA, 15 members. Byte-identical to the 2026-07-11 Stage-1
probe. The module asserts the byte length before parsing: a vintage change invalidates the
pre-verified geometry and must re-probe rather than silently reshape.

**Two of the 15 members are in scope.** CxC/IxC/IxI total-requirements and Supply tables are not.

| Member | Bytes | Role |
|---|---:|---|
| `Use_SUT_Framework_2017_DET.xlsx` | 2,121,639 | detail matrix + `NAICS Codes` concordance |
| `Use_Tables_Supply-Use_Framework_1997-2023_Summary.xlsx` | 1,119,194 | annual matrix |

**Dead paths (do not use):** the BEA **API** is key-gated and no key exists in Doppler
`core-x/prd`; `apps.bea.gov/industry/xls/io-annual/IOUse_*_Summary.xlsx` serves `text/html`.

## Workbook geometry (probed live; asserted fail-closed at every parse)

The two workbooks **do not share a header layout** — this is the single most important
structural fact in the module, and the one thing the directive flagged as un-pre-verified:

| | detail (`*_DET`) | summary (`*_Summary`) |
|---|---|---|
| industry **names** row | 5 | **7** |
| industry **codes** row | 6 | **6** |
| first data row | 7 | **8** |
| sheets | `2007`, `2012`, `2017` | one per year `1997`…`2023` (27) |
| shape | 436 × 427 | 90 × 94 |
| columns | 402 industry + `T001` + 19 final-demand + `T019` | 71 industry + `T001` + 19 final-demand + `T019` |
| rows | 402 commodity + 9 total/VA | 73 commodity + 10 total/VA |
| "no flow" encoding | **blank** (71% of cells) | **`...`** (zero blanks) |

Both are $Millions. Column A = commodity code, column B = commodity description.

Every parse asserts the column census (industry / final_demand / total) and the row census
(commodity / total_or_va) against these verified counts and **raises before writing** on any
mismatch. A reshaped upstream workbook fails the run; it never lands a silently-reshaped matrix.

### Grain asymmetry is structural, not a defect

Detail grain exists **only for the 3 benchmark years** (2007/2012/2017) — BEA does not produce a
402-industry use table annually. The annual series is Summary grain, and **at Summary grain BEA
dissolves employment services (5613) into the whole administrative-and-support aggregate (561)**.
So:

- `grain='detail_561300'` — a *measurement* of the staffing channel, 3 years.
- `grain='summary_561_proxy'` — a *proxy*: 561 is all of administrative & support services, of
  which employment services is one part. Use it for trend shape, never for level. It runs
  structurally higher than the detail measure (max 0.1745 vs 0.0967).

## `...` is "not applicable", NOT a disclosure suppression

The directive anticipated `...` as a suppression marker. It is not, and the workbook proves it:

- The summary matrix has **zero blank cells** — every one of its 7,636 cells per year is either
  numeric or the literal `...`. It is the summary workbook's encoding of exactly the "no flow"
  that the detail workbook writes as a blank.
- It publishes **explicit zeros too** (2,346 of them), so `...` ≠ 0 by BEA's own hand.
- BEA's legend, on the same workbook's `NAICS Codes` sheet, reads: *"n.a. Not applicable."*
- BEA does not suppress in the national IO accounts at 71-industry grain — there is no
  establishment-level disclosure risk to suppress for.

**How it lands:** those cells get `value_musd = NULL` with the marker preserved verbatim in
`value_marker`. The `suppressed` boolean is carried per the directive's §4 column contract and
is exactly `value_marker IS NOT NULL` (verified true on read-back). Read it as *"cell carried a
non-numeric published marker"*, not *"BEA withheld this number"*. **Filter
`value_musd IS NOT NULL` for actual flows in either stream.**

## Fidelity (raw stays lossless)

- Codes and names land verbatim; numeric-looking codes render without a float artifact
  (`111200`, never `111200.0`).
- **Published zeros are real observations** and land as `0.0` — never conflated with absence.
- Detail blanks are not landed (nothing to land); summary `...` markers are (see above).
- `row_kind` / `col_kind` are ADDITIONAL classifier columns, never a write-time filter. The
  `T*`/`V*` rows are the denominators every intensity ratio needs — total intermediate inputs
  (`T005`), compensation of employees (`V001`/`V00100`), gross operating surplus, value added,
  **total industry output (`T018`)** — and the `F*` columns are final demand (PCE, private
  fixed investment, exports, federal defense/nondefense, state & local).
- BEA's trailing `"Note.  Detail may not add to total due to rounding."` line is dropped by the
  code-**and**-description row filter; it would otherwise parse as a commodity code.

## Validation Gate — measured values (directive §8)

Every bound below was recomputed **independently off R2** after the load, not reused from the
ingest process.

| § | Gate | Bound | Measured | |
|---|---|---|---|---|
| 8.1 | detail years | 2007/2012/2017 | all 3 | ✅ |
| 8.1 | industries purchasing `561300` | ≥ 50 /yr | 378 · 387 · 385 | ✅ |
| 8.1 | 2017 economy-wide emp-svcs intermediate | [150k, 500k] $M | **$382,817M** | ✅ |
| 8.1 | detail cells per year | see deviation ↓ | 53,115 · 53,201 · 53,222 | ✅ |
| 8.2 | summary years | 27 (1997–2023) | 27, contiguous | ✅ |
| 8.2 | code-set drift vs 2017 | ± 2 | **0** (fully stable) | ✅ |
| 8.2 | summary cells per year | full rectangle 83×92 | 7,636 every year | ✅ |
| 8.3 | concordance rows | ≥ 380 | 643 | ✅ |
| 8.3 | `561300` present | yes | `detail` level → NAICS `5613` | ✅ |
| 8.4 | derived detail rows | (#buyers) × 3 yr | 1,150 (378+387+385) | ✅ |
| 8.4 | `intake_share_of_output` detail | (0, 0.15] | (0.000002, **0.0967**] | ✅ |
| 8.4 | `intake_share_of_output` summary proxy | see deviation ↓ | (0.000090, **0.1745**] | ✅ |
| 8.4 | staffing-heavy in 2017 top decile | ≥ 1 group | **all 3** (below) | ✅ |
| 8.5 | reconciliation `T005 / Σ commodity` | ≥ 0.98 | detail **0.9960**–1.0126 (n=1,200)<br>summary **0.9994**–1.0006 (n=1,917) | ✅ |
| 8.6 | `ops.labor_share_runs` status | `success` ×4 | 4 rows, `success` | ✅ |

### Three deviations from the directive's stated bounds — each a corrected estimate

1. **§8.1 "per-year non-null cells ≥ 60,000" is unreachable and was a density estimate.** The
   real detail matrix is 29.1% dense (53,115 / 53,201 / 53,222). A use matrix *is* sparse — most
   industries buy most commodities not at all. The gate is implemented as a ≥ 50,000 floor
   (a >6% loss vs verified trips it). **The real completeness proof is §8.5**, which passes with
   a worst case of 0.9960 against a 0.98 tolerance — BEA's own note says detail may not add to
   total due to rounding, and the residual is exactly that rounding.
2. **The cell-count floor is a detail-stream bound; §8.2 sets none for summary.** Because the
   summary matrix has zero blanks it lands as a full rectangle, so it gets a strictly stronger
   gate: **exact equality** to 83 × 92 = 7,636 cells, every year, all 27.
3. **§8.4's (0, 0.15] holds at detail grain but not for the summary proxy** — 561 is the entire
   administrative-and-support aggregate, so it structurally runs higher (max 0.1745, pipeline
   transportation 1998). Detail keeps the directive's bound as a hard gate; the proxy gets its
   own documented (0, 0.20].

### 2017 detail — top of the intake distribution

Employment services itself tops the list (staffing firms buy staffing labor), then software and
telecom. The three named staffing-heavy groups all land in the top decile (n=38):

| Industry | intake / output | emp-svcs input |
|---|---:|---:|
| `561300` Employment services | 9.33% | $35,992M |
| `541511` Custom computer programming services | 8.64% | $13,193M |
| `517110` Wired telecommunications carriers | 6.44% | $21,297M |
| `561400` Business support services | 4.46% | $3,546M |
| `561200` Facilities support services | 4.04% | $1,193M |
| `493000` Warehousing and storage | 3.46% | $4,276M |
| `621600` Home health care services | 3.17% | $2,856M |
| `485000` Transit and ground passenger transportation | 2.85% | $2,200M |
| `622000` **Hospitals** | 2.80% | **$23,600M** |
| `623A00` Nursing and community care facilities | 2.59% | $4,819M |

Economy-wide employment-services intermediate purchases: **$176.4B (2007) → $245.8B (2012) →
$382.8B (2017)**. The 561-proxy annual series runs $284.5B (1997) → $1,164.9B (2023).

## Dataset-name deviation: `bea_sut_naics_concordance`, not `bea_naics_concordance`

Directive §4 names the concordance stream `bea_naics_concordance`. **That name was already live**
from the predecessor labor-share stack and writing to it with `mode="overwrite"` would have
clobbered a richer dataset — violating the directive's own §2 constraint that the two modules'
datasets stay disjoint. This stream therefore lands at `bea_sut_naics_concordance`.

The two are **complements, not substitutes** — different sources, different grains, different
join keys:

| | `bea_naics_concordance` (predecessor) | `bea_sut_naics_concordance` (this) |
|---|---|---|
| source | standalone `BEA-Industry-and-Commodity-Codes-and-NAICS-Concordance.xlsx` | the `NAICS Codes` sheet **inside** `Use_SUT_Framework_2017_DET.xlsx` |
| shape | one row per **NAICS code**, all 5 BEA levels across the row (incl. GO Detail) | one row per **BEA code**, at the level it occupies (hierarchical outline) |
| rows | 499 | 643 (23 sector · 73 summary · 141 u.summary · 406 detail) |
| answers | "what BEA levels does this NAICS roll into?" | "what is this code in the use matrix, and which NAICS does it cover?" |

Use `bea_sut_naics_concordance` to resolve any code appearing in **this module's matrices** —
its `bea_code` values are exactly the row/column codes landed here, at every level. Note
`bea_code` is **not unique**: a parent with a single child repeats its code across levels (e.g.
`211` at both summary and u.summary), so join on `(bea_level, bea_code)`. 406 rows carry
`naics_ranges`; verbatim BEA range syntax (`11113-6, 11119`), never re-parsed. 18 carry notes.

**Predecessor caveat status:** the labor-share directive's open concordance item was already
retired on 2026-07-11 by `materialize_bea_naics_concordance.py` — the NULL `bea_industry_code`
in `bea_industry_value_added` resolves through `bea_summary_code`, and that dataset is
**verified untouched by this run** (499 rows, 19 columns, v5). This module adds the BEA-code-keyed
complement the use matrices need.

## Ledger

`ops.labor_share_runs` (HQX, `HQX_DB_URL_POOLED`) — shared with the predecessor stack,
append-only, one terminal-state row per Lance dataset per run, with full coverage JSON. An
audit-write failure warns and never masks a good load.

## Re-run

```bash
doppler run -p core-x -c prd -- uv run --with pylance --with pyarrow --with openpyxl \
  --with requests --with boto3 --with 'psycopg[binary]' \
  python -m pipelines.reference.bea_io_use_ingest --stream all --smoke
```

Then drop `--smoke` for the full load. `--stream {detail,summary,concordance,derived,all}`; the
zip is downloaded once per process and cached in scratch across runs.

## Out of scope (deliberate)

CxC/IxC/IxI total-requirements tables, Supply tables, the Sector-grain use tables, KLEMS (landed
separately by `industry_cost_structure_ingest.py`), the composed sourcing dim onto
`naics_psc_labor_dim` (follow-on), sidecar promotion, any LLM use.
