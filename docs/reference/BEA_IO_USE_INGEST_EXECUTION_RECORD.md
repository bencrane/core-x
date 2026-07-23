# BEA IO Use Ingest — Execution Record

**Executed:** 2026-07-23 UTC · **PR:** [#1325](https://github.com/bencrane/core-x/pull/1325) · **Merge commit:** `48bc559`
**Directive:** `~/Desktop/hq/directives/2026-07-11-bea-io-use-contingent-labor-ingest.md` (closed out at hq `29bb8c3`)
**Module:** [`pipelines/reference/bea_io_use_ingest.py`](../../pipelines/reference/bea_io_use_ingest.py)
**Dataset run record:** [`BEA_IO_USE_CONTINGENT_LABOR.md`](BEA_IO_USE_CONTINGENT_LABOR.md)

> This is the **execution** record — what was probed, what the directive got wrong, the
> dataset-name collision and how it was avoided, and the verification chain. The dataset
> record above is the consumer-facing document (grain, columns, how to query). Read that one
> to *use* the data; read this one to understand *how it got there* and what to trust.

---

## 1. What was asked

Land four Lance datasets from one static BEA zip: the detail use matrix (3 benchmark years),
the summary-annual use matrix (1997–2023), the in-workbook BEA↔NAICS concordance, and a derived
contingent-labor-intake table. One PR, full git lifecycle, zero LLM, deterministic xlsx parses.

**Why it matters:** an award dollar's labor is self-performed, subcontracted, or **contingent**.
The third leg is structurally invisible to federal reporting — a prime's payment to a staffing
agency is a vendor purchase, not a subaward, so it never crosses the FSRS threshold and never
appears in FPDS. The BEA use table is the only public, economy-wide measurement of that channel.

## 2. What landed

| Dataset | Grain | Rows | Indexes |
|---|---|---:|---:|
| `bea_io_use_detail` | year × commodity × column | 159,538 | 6 |
| `bea_io_use_summary_annual` | year × commodity × column | 206,172 | 6 |
| `bea_sut_naics_concordance` | BEA code (4 levels) | 643 | 2 |
| `bea_contingent_labor_intake` | industry × year | 3,067 | 3 |
| | **TOTAL** | **369,420** | **17** |

Lance v2.1 at `s3://data-sink/active/`, `mode="overwrite"`. Ledger `ops.labor_share_runs`:
4 rows, all `status='success'`.

**Headline result.** Economy-wide employment-services intermediate purchases: **$176.4B (2007)
→ $245.8B (2012) → $382.8B (2017)**. In 2017 hospitals bought **$23.6B** of staffing labor
(2.80% of output); warehousing (3.46%), home health (3.17%), transit (2.85%), and nursing
facilities (2.59%) all sit in the top decile. None of that spend is visible in FPDS or FSRS.

---

## 3. ⚠️ The collision: `bea_naics_concordance` was already live

**This is the single most consequential finding of the execution.** It was caught in the first
two minutes, before any code was written, and it would have destroyed a live dataset.

### 3.1 What the directive said

§4 stream 3 specified the dataset name `bea_naics_concordance`, with `mode="overwrite"` (§4:
*"All Lance v2.1, `mode="overwrite"`"*) and a thin schema:

```
bea_code, bea_title, naics_ranges (verbatim), sheet_source, ingested_at
```

§2 of the same directive asserted the constraint the write would have broken:

> Still do NOT touch `pipelines/reference/labor_share_ingest.py` **or its datasets**; your
> module is `pipelines/reference/bea_io_use_ingest.py`, **datasets disjoint (§4)**.

### 3.2 What was actually on disk

A pre-flight grep of every `active/` URI in the repo — run before opening a single workbook —
returned `active/bea_naics_concordance` as an **existing, live dataset**:

```
$ grep -rn "active/bea_..." --include="*.py" pipelines/ apps/ scripts/ | grep -o "active/[a-z0-9_]*" | sort -u
active/bea_bls_klems
active/bea_fixed_assets_detail
active/bea_industry_value_added
active/bea_naics_concordance        ← the collision
```

Owned by `pipelines/reference/materialize_bea_naics_concordance.py`, part of the **predecessor
labor-share stack**, landed 2026-07-11. Verified live: **499 rows, 19 columns, version 5, 4
indexes.** Built from a **different source** with a **different grain**.

| | `bea_naics_concordance` (predecessor, live) | what §4 would have written over it |
|---|---|---|
| source | standalone `BEA-Industry-and-Commodity-Codes-and-NAICS-Concordance.xlsx` | the `NAICS Codes` sheet inside `Use_SUT_Framework_2017_DET.xlsx` |
| grain | one row per **NAICS code**, all 5 BEA levels across the row (incl. GO Detail) | one row per **BEA code**, hierarchical outline |
| columns | 19 (`naics_code_clean`, `bea_summary_code`, `naics_multi_io`, …) | 5 |
| rows | 499 | 643 |
| indexes | BTREE `naics_code_clean` + `bea_summary_code`, BITMAP `bea_sector_code` + `naics_level` | none of those columns exist |

### 3.3 The blast radius if it had been executed as written

`lance.write_dataset(..., mode="overwrite")` is a **full-snapshot replace**, not a merge. Writing
the §4 schema to that URI would have:

1. **Destroyed all 19 columns** of a live dataset, replacing them with 5 unrelated ones.
2. **Broken the predecessor's documented join path.** `LABOR_SHARE_OF_REVENUE_STACK.md` records
   that `bea_industry_value_added.industry_name → bea_summary_desc → bea_summary_code` is what
   resolves the NULL `bea_industry_code` — *"81/102 landed BEA names bind directly — i.e. 100% of
   the mappable industries."* Every one of those columns would have vanished.
3. **Broken the composed `naics_labor_share` dim**, which the predecessor doc records as live
   since 2026-07-14 and bound *"via `bea_naics_concordance` longest-prefix"* — the `naics_code`
   /`naics_code_clean` bridge would have ceased to exist.
4. **Dropped 4 committed scalar indexes** on columns that no longer existed.
5. Been **silent**. `mode="overwrite"` does not warn on a schema change; the run would have
   printed `status=success` and written a `success` row to the shared ledger. The damage would
   have surfaced later as a downstream join returning zero rows.

### 3.4 How it was avoided

**The directive's stated *intent* (disjoint datasets) was treated as governing, and its stated
*name* as the error.** The directive was written 2026-07-11; the predecessor landed its own
concordance asset on that same date. The author believed §4's names were disjoint — they were
not. Obeying the literal name would have violated the directive's own §2 constraint.

Resolution: the stream lands at **`bea_sut_naics_concordance`** — "SUT" for the Supply-Use
Tables workbook it comes from. Nothing else changed; the parse, the gates, and the raw-lossless
contract are as specified.

The two datasets are **complements, not substitutes**, and both are needed:

- `bea_naics_concordance` answers *"what BEA levels does this NAICS code roll into?"* — the
  NAICS-grain bridge the labor-share stack joins through.
- `bea_sut_naics_concordance` answers *"what is this code in the use matrix, and which NAICS does
  it cover?"* — its `bea_code` values **are** exactly the row and column codes landed in
  `bea_io_use_detail` / `bea_io_use_summary_annual`, at all four levels (23 sector / 73 summary /
  141 u.summary / 406 detail). Nothing else in the plane resolves a use-matrix code.

**Join caveat:** `bea_code` is **not unique**. BEA's outline repeats a code across levels when a
parent has a single child (e.g. `211` appears at both summary and u.summary; 59 codes repeat,
582 distinct across 643 rows). **Join on `(bea_level, bea_code)`.**

### 3.5 Verification the predecessor survived

Read back from R2 **after** the full load, in the same script that verified the new datasets:

```
bea_naics_concordance  (PREDECESSOR — must be untouched)
  rows=499 v=5 cols=19 indexes=4
  columns: ['naics_code', 'naics_code_clean', 'naics_level', 'naics_multi_io',
            'naics_description', 'bea_sector_code', 'bea_sector_desc', 'bea_summary_code',
            'bea_summary_desc', 'bea_u_summary_code', 'bea_u_summary_desc', 'bea_detail_code',
            'bea_detail_desc', 'bea_go_detail_code', 'bea_go_detail_desc', 'notes', 'source',
            'source_url', 'ingested_at']
```

Identical to its 2026-07-11 landed state — same row count, same 19 columns, **still at version
5** (an overwrite would have bumped it). Untouched.

### 3.6 The generalizable rule

> **Before writing any `mode="overwrite"` dataset, grep the repo for the URI.** A directive's
> dataset names are an *estimate of the namespace at authoring time*, not a reservation. Where a
> directive's literal instruction and its stated invariant conflict, the invariant governs — and
> the deviation gets named in the module docstring, the PR body, the run record, and the
> directive closeout, never buried.

---

## 4. Method: probe before writing a line of parser

Four read-only probes ran against the live workbooks before the module existed. Every geometry
constant in the module is a measured value, not an assumption — which is why the implementation
took one smoke and one full run, with no rescope.

| Probe | Established |
|---|---|
| 1 | Sheet inventory, header layout of both workbooks, first 14 rows × 8 cols of every sheet |
| 2 | Row/column tails (the `T*`/`V*`/`F*` blocks), the `561300` and `561` target rows, cell vocabulary, cross-year code-set stability |
| 3 | Concordance outline structure, **all §8 gate arithmetic precomputed**, derived-table shape, per-year reconciliation ratios |
| 4 | Concordance footnote-row census, `...` semantics, row-filter safety, summary outliers |

Then a **local dry run with the R2/ledger writers stubbed** exercised all four streams end to
end — catching one real bug (below) with zero R2 writes — before the smoke run touched
throwaway URIs and the full run touched `active/`.

### 4.1 Verified geometry

|  | detail (`*_DET`) | summary (`*_Summary`) |
|---|---|---|
| industry **names** row | 5 | **7** |
| industry **codes** row | 6 | **6** |
| first data row | 7 | **8** |
| sheets | `2007`, `2012`, `2017` | one per year `1997`…`2023` (27) |
| shape | 436 × 427 | 90 × 94 |
| columns | 402 industry + `T001` + 19 final-demand + `T019` | 71 industry + `T001` + 19 final-demand + `T019` |
| rows | 402 commodity + 9 total/VA | 73 commodity + 10 total/VA |
| "no flow" encoding | **blank** (71% of cells) | **`...`** (zero blanks) |
| target row | r348 `561300 Employment services` | r62 `561 Administrative and support services` |

Source zip: 20,439,248 B, `sha256 fe60d189…12de3`, 15 members, 21,847,119 B uncompressed —
byte-identical to the 2026-07-11 Stage-1 probe. The module asserts the byte length before
parsing; a vintage change invalidates the pre-verified geometry and must re-probe rather than
silently reshape.

---

## 5. Six directive facts that the live data contradicted

Each was corrected against measurement and named in the module docstring, PR body, run record,
and directive closeout. **None was silently satisfied.**

### 5.1 §2.4 — the summary workbook's header order is inverted

The directive predicted the summary workbook repeats the detail's *"banner-rows-then-code-row
pattern"* and flagged it as the one un-pre-verified internal. It does not repeat it: detail is
**names row 5 / codes row 6**, summary is **codes row 6 / names row 7**.

Parsing summary on the detail assumption would have read the *code* row as names and the *name*
row as codes across all 27 sheets — landing 206,172 rows in which every industry name was its
own code and vice versa. Structurally valid, entirely wrong, and invisible to a row-count check.

### 5.2 §2.6 — `...` is "Not applicable", not a suppression

The directive said to treat `...` as a suppression: *"land NULL with `suppressed=true`."* The
workbook disproves it three ways:

- The summary matrix has **zero blank cells** — all 7,636 cells per year are numeric or `...`.
  It is the summary encoding of exactly the "no flow" the detail workbook writes as blank.
- It publishes **2,346 explicit zeros** alongside `...`, so `...` ≠ 0 by BEA's own hand.
- BEA's legend, on the same workbook's `NAICS Codes` sheet, reads **"n.a. Not applicable."**
  And BEA does not suppress in the national IO accounts at 71-industry grain — there is no
  establishment-level disclosure risk to suppress for.

**Resolution:** those cells land with `value_musd = NULL` and the marker preserved verbatim in
`value_marker`. The `suppressed` boolean is carried per the §4 column contract and is **exactly**
`value_marker IS NOT NULL` (asserted true on read-back). It is documented everywhere as *"cell
carried a non-numeric published marker"*, never as *"BEA withheld this number"* — otherwise a
future analyst asking "what does BEA suppress?" gets **84,198 false positives**.

### 5.3 §8.1 — the ≥60,000 cells/year floor is unreachable

Measured: **53,115 / 53,201 / 53,222**. A use matrix *is* sparse — 29.1% dense, because most
industries buy most commodities not at all. The 60,000 figure was a density estimate, not a
measurement.

Implemented as a **≥50,000 floor** (a >6% loss vs verified trips it). The real completeness
proof is §8.5 reconciliation, which passes at worst **0.9960** against a 0.98 tolerance — and
BEA's own trailing note says *"Detail may not add to total due to rounding"*, which is exactly
the residual.

### 5.4 §8.2 — summary got a *stronger* gate than asked for

The cell-count floor is a §8.1 **detail** bound; §8.2 sets no cell floor for summary at all.
Because the summary matrix is blank-free it lands as a full rectangle, so it received exact
equality to **83 × 92 = 7,636 cells, every year, all 27** — a strictly stronger completeness
assertion than any floor.

### 5.5 §8.4 — the (0, 0.15] share bound fails on the summary proxy

Detail max: **0.0967** ✅. Summary proxy max: **0.1745** (pipeline transportation, 1998) ✗.

Not a defect. At summary grain BEA dissolves employment services (5613) into the **entire**
administrative-and-support aggregate (561), so the proxy measures a strictly larger basket and
structurally runs higher. Detail keeps the directive's bound as a hard gate; the proxy gets a
documented **(0, 0.20]**, and the dataset flags it `grain='summary_561_proxy'` — trend shape
only, never level.

### 5.6 §1/§4 — row-count estimates

Estimated ~200–500K detail and ~100–150K summary; actual **159,538** and **206,172**. Detail
came in low because the matrix is sparser than assumed; summary came in high because the `...`
markers are landed per §2.6. Both are fully explained by 5.2 and 5.3.

---

## 6. Bugs caught before they shipped

| Stage | Bug | Consequence avoided |
|---|---|---|
| pre-code grep | `bea_naics_concordance` name collision | silent destruction of a live 19-column dataset (§3) |
| probe 1 | inverted summary header order | 206,172 rows with names and codes transposed (§5.1) |
| probe 4 | `...` misread as suppression | 84,198 rows mislabeled as withheld data (§5.2) |
| probe 4 | BEA's trailing `"Note.  Detail may not add to total due to rounding."` line parses as a commodity code | a junk commodity row in all 3 detail years — dropped by requiring **both** a code and a description |
| local dry run | cells-per-year floor wrongly applied to the summary stream | a hard gate failure on a correct parse (fixed by moving the bound into each spec) |
| ruff | unclosed file handle on the cached-zip read path | handle leak; fixed and re-smoked |

---

## 7. Fail-closed design

The module refuses to land a silently-reshaped matrix. Before any write:

- **Byte-length assert** on the source zip — a vintage change raises rather than parsing on
  stale geometry.
- **Column census assert** per sheet: industry / final_demand / total counts must equal the
  verified shape (402/19/2 detail, 71/19/2 summary).
- **Row census assert** per sheet: commodity / total_or_va must equal 402/9 (detail), 73/10
  (summary).
- **Gate functions raise before `_write_lance`**, never after — a failed gate leaves `active/`
  untouched and writes an `error` row to the ledger.

Row and column *kinds* are additional classifier columns, never a write-time filter: the
`T*`/`V*` rows (total intermediate inputs `T005`, compensation of employees, gross operating
surplus, **total industry output `T018`**) are the denominators every intensity ratio needs, and
the `F*` columns are final demand. Published zeros land as `0.0` — never conflated with absence.

---

## 8. Validation Gate §8 — measured

**Every value recomputed independently off R2 after the load**, in a separate script that shares
no state with the ingest process.

| § | Gate | Bound | Measured | |
|---|---|---|---|---|
| 8.1 | detail years | 2007/2012/2017 | all 3 | ✅ |
| 8.1 | industries purchasing `561300` | ≥ 50 /yr | 378 · 387 · 385 | ✅ |
| 8.1 | 2017 economy-wide emp-svcs intermediate | [150k, 500k] $M | **$382,817M** | ✅ |
| 8.1 | detail cells per year | ≥ 50,000 (§5.3) | 53,115 · 53,201 · 53,222 | ✅ |
| 8.2 | summary years | 27 (1997–2023) | 27, contiguous | ✅ |
| 8.2 | code-set drift vs 2017 | ± 2 | **0** (fully stable) | ✅ |
| 8.2 | summary cells per year | exact 83×92 (§5.4) | 7,636 every year | ✅ |
| 8.3 | concordance rows | ≥ 380 | 643 | ✅ |
| 8.3 | `561300` present | yes | `detail` level → NAICS `5613` | ✅ |
| 8.4 | derived detail rows | (#buyers) × 3 yr | 1,150 (378+387+385) | ✅ |
| 8.4 | `intake_share_of_output` detail | (0, 0.15] | (0.000002, **0.0967**] | ✅ |
| 8.4 | `intake_share_of_output` summary proxy | (0, 0.20] (§5.5) | (0.000090, **0.1745**] | ✅ |
| 8.4 | staffing-heavy in 2017 top decile | ≥ 1 group | **all 3** | ✅ |
| 8.5 | reconciliation `T005 / Σ commodity` | ≥ 0.98 | detail **0.9960**–1.0126 (n=1,200)<br>summary **0.9994**–1.0006 (n=1,917) | ✅ |
| 8.6 | ledger status | `success` ×4 | 4 rows `success` | ✅ |

### 2017 detail — top of the intake distribution

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

Employment services tops its own list — staffing firms buy staffing labor. All three
directive-named staffing-heavy groups (administrative/support, hospitals/health,
transportation/warehousing) land in the top decile (n=38).

---

## 9. Verification chain

1. **Pre-flight namespace grep** → collision found before any code (§3).
2. **Four read-only probes** against live workbooks → every geometry constant measured (§4).
3. **Local dry run, writers stubbed** → all 4 streams, all gates, zero R2 writes.
4. **`--stream all --smoke`** → throwaway `_smoke_` URIs, 17 indexes built, prefixes deleted.
5. **`--stream all`** → full load to `active/`.
6. **Independent read-back off R2** → row counts, schemas, index counts, every §8 bound
   recomputed from landed rows, the `suppressed ≡ value_marker IS NOT NULL` invariant, ledger
   rows, and the predecessor dataset's survival.
7. **Post-edit re-smoke** after the final lint fix.
8. **Operator checkout pulled and verified** — `~/core-x` on `main` at `48bc559`, both files on
   disk, in sync with `origin/main`.

---

## 10. Surfaces

| Atom | Path / key |
|---|---|
| module | `pipelines/reference/bea_io_use_ingest.py` |
| datasets | `s3://data-sink/active/{bea_io_use_detail, bea_io_use_summary_annual, bea_sut_naics_concordance, bea_contingent_labor_intake}/` |
| ledger | `ops.labor_share_runs` (shared with the predecessor stack, append-only) |
| dataset run record | `docs/reference/BEA_IO_USE_CONTINGENT_LABOR.md` |
| execution record | `docs/reference/BEA_IO_USE_INGEST_EXECUTION_RECORD.md` (this file) |
| predecessor | `docs/reference/LABOR_SHARE_OF_REVENUE_STACK.md` |
| directive | `~/Desktop/hq/directives/2026-07-11-bea-io-use-contingent-labor-ingest.md` (closed, hq `29bb8c3`) |

### Re-run

```bash
doppler run -p core-x -c prd -- uv run --with pylance --with pyarrow --with openpyxl \
  --with requests --with boto3 --with 'psycopg[binary]' \
  python -m pipelines.reference.bea_io_use_ingest --stream all --smoke
```

Drop `--smoke` for the full load. `--stream {detail,summary,concordance,derived,all}`; the zip
is downloaded once per process and cached in scratch across runs.

---

## 11. Out of scope (deliberate)

CxC/IxC/IxI total-requirements tables, Supply tables, Sector-grain use tables, KLEMS (landed
separately by `industry_cost_structure_ingest.py`), the composed sourcing dim onto
`naics_psc_labor_dim` (follow-on now unblocked — both this and the labor-share stack are live),
query-sidecar promotion, any LLM use, any BEA API key.

## 12. Carry-forward for the next directive

1. **Grep the URI namespace before writing any `mode="overwrite"` dataset.** Directive dataset
   names are an authoring-time estimate, not a reservation (§3.6).
2. **Where a directive's literal instruction contradicts its own stated invariant, the invariant
   governs** — and the deviation is named in the docstring, PR, run record, and closeout.
3. **Probe every workbook internal the directive did not pre-verify, and re-probe the ones it
   did.** §2.4 was honest that summary was unverified; it was also wrong about what to expect
   there (§5.1). Two of the four pre-"verified" facts (§2.6 marker semantics, §8.1 cell density)
   were also wrong.
4. **A directive's numeric gate bounds are estimates until measured.** Compute every bound during
   the probe phase, before writing the module — then a failing gate means a real defect, not a
   bad constant.
5. **Name a flag for what it is.** `suppressed` was kept for contract compatibility but is
   documented as "carried a marker" and paired with a verbatim `value_marker`, because a
   misnamed boolean is a trap that outlives everyone who remembers the workaround.
