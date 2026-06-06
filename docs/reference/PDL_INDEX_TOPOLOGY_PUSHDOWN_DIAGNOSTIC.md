# PDL — Index Topology & Predicate-Pushdown Diagnostic

Read-only, mathematically-grounded interrogation of the **live R2-backed Lance system of
record** for the People Data Labs (PDL) universe — the exact index manifest committed to the
dataset, the trained-row truth of every index, the flat-vs-nested storage of each resolution
key, and an empirical query-planner trace proving how the engine executes raw vs.
normalization-macro predicates against the high-cardinality name/geo keys. Direct follow-up to
`FEC_INDEX_TOPOLOGY_PUSHDOWN_DIAGNOSTIC.md` and `MSHA_INDEX_TOPOLOGY_PUSHDOWN_DIAGNOSTIC.md`:
the directive's question is **does PDL suffer the FEC commit-order dead-BTREE failure, the MSHA
missing-index-on-high-card-strings failure, or a nested/array blinding of the planner?** —
answered here, all three, with live evidence.

- **Target interrogated (Gen-3 SoR, `s3://data-sink/active/`):** `pdl_companies` (Lance
  **v11**, **35,446,771 rows**, 12 cols, **34 fragments**, 73 files / 7.18 GB).
  > The directive named `s3://data-sink/active/pdl_person/` and person-level keys
  > (`work_email`, `full_name`). **No such dataset exists, and no person-level PDL data is
  > live.** The PDL system of record is the **Free Company *firmographic* Dataset**
  > (`pdl_companies`) — company-level, not person-level. `work_email`/`full_name` have no
  > analogue; the resolution keys that *do* exist are `company_name`, `linkedin_url`,
  > `domain`, `locality`, `region`, `country` (the company-level keys the directive also
  > named). The near-empty `active/people/` (0.00 GB) is a resolution spine, **not** a PDL
  > person dump. This is the same directive-misnomer pattern flagged for MSHA's
  > `msha_operator_history`.
- **Live evidence harness (non-mutating, zero writes):** `pylance 7.0.0` / `pyarrow 24.0.0` /
  `duckdb 1.5.x` direct reads — `dataset.list_indices()` (manifest) ·
  `dataset.stats.index_stats()` (type + trained-row truth) ·
  `LanceScanner.explain_plan(verbose=True)` (physical plan) · `LanceScanner.analyze_plan()`
  (EXPLAIN-ANALYZE: real `rows_scanned`/`bytes_read`/`fragments_scanned`) ·
  `count_rows(filter=…)` · `pyarrow`+DuckDB for cardinality/fill. DuckDB `EXPLAIN` over the
  Lance Arrow stream for corroboration. The macro under test is imported **verbatim** from
  `core.name_norm.name_norm` — byte-identical to every resolution spine.
- **As-of:** probed **2026-06-05** against the committed dataset (single manual-drop snapshot:
  `snapshot_date = 2026-05-31`, `ingested_at = 2026-05-31 19:52:52-04`). The committed R2
  dataset was staged read-only to local disk via boto3 (download-only, the operator's own
  `reindex` staging path) for clean wall-clock isolation; the index manifest and stats are
  byte-identical to R2 (the publish is a boto3 mirror). **No DDL, no index build, no `.lance`
  write, no delete, no `optimize_indices`** — every figure is a live read of the committed
  dataset.
- **Attestation:** the figures below are the physical plans the Lance/DataFusion engine
  emitted and executed against the committed fragments, not a recon estimate.

---

## 0. Headline posture

| Verdict | Detail |
|---|---|
| **Does PDL replicate FEC's dead-BTREE / commit-order failure?** | ✅ **NO. Zero dead indices.** All **10** committed scalar indices report `num_indexed_rows == 35,446,771`, `num_unindexed_rows == 0` — **100% trained.** PDL's `mode="overwrite"`-then-`create_scalar_index` lifecycle (over the complete row set, on local disk, then boto3-published) is the structural inverse of FEC's index-at-v1–6-then-append-24-cycles. The commit-order failure **is not present.** |
| **Does PDL replicate MSHA's missing-index-on-high-card-strings failure?** | ✅ **NO — and this is the inverse of MSHA.** Where MSHA left every human-readable name/geo key unindexed, PDL carries a **trained BTREE on `company_name`, `linkedin_url`, `domain`, `locality`** and **trained BITMAPs on `industry`, `country`, `region`, `employee_size_range`**. **Every resolution key the directive named is indexed and trained.** The only two unindexed columns are `snapshot_date` / `ingested_at` (ingest provenance, never join keys). |
| **Are nested/array columns blinding the planner?** | ✅ **NO.** All **12 columns are flat scalar** (`string` / `int32` / `date32` / `timestamp`). There is **no `List`, `Struct`, or `Map`** anywhere in the schema — no resolution key is trapped inside an array. (The directive's nested-key worry is a *person*-schema hazard; the live company schema is flat.) |
| **Are the indexed keys actually used?** | ✅ **Proven live.** Raw point lookups on `company_name`/`linkedin_url`/`domain`/`locality` (BTree), `country`/`industry` (Bitmap), and a `year_founded` **range** each emit a `ScalarIndexQuery` node with `refine_filter=--` and read **only the matched rows** (e.g. `company_name='x'`: `rows_scanned=524`; `linkedin_url`: `rows_scanned=1`, 1 of 34 fragments — the index pruned the other 33 entirely). The index spine is healthy and exercised end-to-end. |
| **Test A — raw `company_name = 'x'`** (BTree) | ✅ **Indexed.** `ScalarIndexQuery@company_name_idx(BTree)`, `refine_filter=--`, **`rows_scanned=524`** → 524 returned, `bytes_read=3.27 MB`, **wall ≈ 9–23 ms.** |
| **Test B — `name_norm(company_name) = 'X'`** | 🛑 **NOT indexed. Full scan: `rows_scanned=35,446,771`** (all 34 fragments) → 549 returned, `bytes_read=623 MB`, **wall ≈ 15.9 s.** DataFusion lowers the macro (`VARCHAR→Utf8`, `trim→btrim`) and binds it as a per-row `refine_filter`; **no `ScalarIndexQuery`.** |
| **Differential (A vs B)** | **67,646× rows scanned** (524 → 35,446,771), **190× bytes** (3.27 MB → 623 MB), **≈1,767× wall** (9 ms → 15,901 ms). The macro discards a *trained* BTREE and full-scans the table. |
| **Does the macro break a *trained* index on an identical result set?** | ✅ **Proven independently** on the trained `country` Bitmap: raw `country='united states'` → `ScalarIndexQuery@country_idx(Bitmap)` (`rows_scanned≈9.01 M`, matched only, 475 ms); `name_norm(country)='UNITED STATES'` → **no index**, `rows_scanned=35,446,771` (full, 7.82 s) — **same 9,014,429-row answer, 16.5× the wall.** `func(col)=lit` is structurally non-indexable. |

**Bottom line:** PDL passes **all three** audits the directive posed — **no dead-BTREE/commit-order
defect, no missing index on the high-card resolution keys, no nested-array trap.** It is the
*structural opposite* of both prior findings: where FEC had dead indices and MSHA had no name
indices, PDL has a **fully-trained index on every resolution key** plus a perfect
`pdl_company_id` primary key (35,446,771 distinct = rows). The single empirical caveat is the
universal one: **the `name_norm()` macro in a `WHERE` clause is non-indexable** — and PDL is the
cleanest dataset in the fleet to prove it, because here the macro demonstrably *bypasses a live,
trained BTREE* (67,646×), not merely a missing one. Remediation is therefore **neither retrain
nor add-index** (both already done) — it is the single read-pattern rule plus, only if a future
bridge needs normalized *name* blocking, a materialized `company_name_norm` column. The existing
`bridge_sam_pdl` already follows this pattern correctly on `normalized_domain` (§5).

---

## 1. Index manifest — exact, from `list_indices()` + `stats.index_stats()`

**10** scalar indices committed; every one **fully trained** — `indexed == total (35,446,771)`,
`unindexed == 0`. (Contrast FEC, where 6 of 14 reported `indexed=0`.)

| Index | Type | Field | `num_indexed_rows` | `num_unindexed_rows` | Idx ver | State |
|---|---|---|--:|--:|--:|---|
| `pdl_company_id_idx` | **BTree** | `pdl_company_id` | 35,446,771 | 0 | 1 | ✅ trained |
| `company_name_idx` | **BTree** | `company_name` | 35,446,771 | 0 | 2 | ✅ trained |
| `linkedin_url_idx` | **BTree** | `linkedin_url` | 35,446,771 | 0 | 3 | ✅ trained |
| `domain_idx` | **BTree** | `domain` | 35,446,771 | 0 | 4 | ✅ trained |
| `locality_idx` | **BTree** | `locality` | 35,446,771 | 0 | 5 | ✅ trained |
| `year_founded_idx` | **BTree** | `year_founded` | 35,446,771 | 0 | 6 | ✅ trained |
| `industry_idx` | **Bitmap** | `industry` | 35,446,771 | 0 | 7 | ✅ trained |
| `country_idx` | **Bitmap** | `country` | 35,446,771 | 0 | 8 | ✅ trained |
| `region_idx` | **Bitmap** | `region` | 35,446,771 | 0 | 9 | ✅ trained |
| `employee_size_range_idx` | **Bitmap** | `employee_size_range` | 35,446,771 | 0 | 10 | ✅ trained |

**FEC-trap scan (`indexed_rows == 0`): NONE.** The manifest **matches the declared
`PDL_BTREE_INDEXES` + `PDL_BITMAP_INDEXES` plan** in
`pipelines/pdl_companies/free_company_dataset.py` exactly — every intended index is present
*and* trained. There is no committed-but-untrained index.

### 1.1 Resolution-key audit — flat scalar, none array-trapped

Full Arrow schema (12 fields). The index-bearing kind is in brackets; **every field is flat —
no `List`/`Struct`/`Map`.**

| Column | Arrow type | Storage | Index |
|---|---|---|---|
| `pdl_company_id` | `string` | flat | **BTree** (PK: 35,446,771 distinct = rows) |
| `company_name` | `string` | flat | **BTree** |
| `domain` | `string` | flat | **BTree** |
| `linkedin_url` | `string` | flat | **BTree** |
| `locality` | `string` | flat | **BTree** |
| `year_founded` | `int32` | flat | **BTree** (range) |
| `industry` | `string` | flat | **Bitmap** |
| `country` | `string` | flat | **Bitmap** |
| `region` | `string` | flat | **Bitmap** |
| `employee_size_range` | `string` | flat | **Bitmap** |
| `snapshot_date` | `date32[day]` | flat | — (provenance) |
| `ingested_at` | `timestamp[us, tz]` | flat | — (provenance) |

The index-type split matches the live cardinality (sampled top values):

| Key | Densest values (row counts) | ~Distinct | Index choice |
|---|---|--:|---|
| `company_name` | `x` (524), `closed` (405), `test` (319) | near-unique (junk sentinels are the *modes*; tail is PK-grade) | BTree ✓ |
| `locality` | `london` (847,667), `paris` (282,276), `new york` (207,123) | ~282 K | BTree ✓ |
| `region` | `england` (2,758,748), `california` (1,320,017), `texas` (747,076) | ~4,176 | Bitmap ✓ |
| `country` | `united states` (9,014,429), `united kingdom` (3,137,077), `france` (2,090,673) | 263 | Bitmap ✓ |
| `industry` | `construction` (1,433,587), `information technology and services` (1,389,268), `retail` (1,240,271) | 152 | Bitmap ✓ |

High-card keys (`company_name`/`linkedin_url`/`domain`/`locality`) → **BTree**; low-card
categoricals (`industry`/`country`/`region`/`employee_size_range`) → **Bitmap**. The design is
cardinality-correct.

---

## 2. Why PDL is immune to the FEC failure — build-order forensics

The FEC defect was a **lifecycle** defect, not an index-type defect. PDL's lifecycle is the
structural inverse — the same reason MSHA was immune, with one extra wrinkle (local build +
boto3 publish):

| | FEC `fec_individual_contributions` | PDL `pdl_companies` |
|---|---|---|
| Write mode | `append` — 24 per-cycle `delete`+`append` pairs | **`mode="overwrite"`** — one full-snapshot write |
| When indices were built | BTREEs at index-version **1–6** (dataset near-empty), bitmaps later | **All 10 indices built *after* the single overwrite**, against the complete 35.4 M-row set |
| Rows present at BTREE build time | ≈0 (pre-backfill) | **100% of rows already landed** |
| Lance auto-folds appends into a scalar index? | No → 282.9 M rows landed after v6 stayed invisible to the BTREEs | N/A — nothing is appended after the index build |
| Lance version structure | 72 versions (heavy append churn) | **v11 = 1 data overwrite (v1) + 10 index commits (v2–v11)**; no `_deletions/`, no rewrite churn |

In `free_company_dataset.py::ingest_pdl_companies`, the local `lance.write_dataset(...,
mode="overwrite")` completes and the committed row count is verified (`count_rows()` on the
stream path) **before** `_create_indexes(LOCAL_DATASET)` runs
(`free_company_dataset.py:474–478`). Every `create_scalar_index` therefore trains over the
finished 35.4 M-row dataset. The 34 fragments are exactly `ceil(35,446,771 / 1,048,576)` —
the `max_rows_per_file` default, no shattering. The dataset is then published to R2 by a
**boto3 byte-mirror** (`_replace_r2_prefix`, wipe + uniform-part upload) that copies the
already-built `_indices/` directory verbatim — it creates **no** new Lance versions and strands
**no** rows. PDL cannot exhibit FEC's "indexed when there was nothing to index" pathology, and
the publish path cannot un-train what was built locally. **Live proof:** all 10 index dirs are
physically present on R2 and all 10 report `indexed=35,446,771, unindexed=0` (§1).

---

## 3. Query-planner diagnostic — physical plans

Sampled real, live values off the committed column for each test. `company_name='x'` is the
densest exact value (524 rows); `name_norm('x')='X'`, which also catches case/whitespace
variants (549 rows) — a deliberate **near-same-result-set** raw-vs-macro contrast on the
directive's primary key.

### 3.1 Primary test — `company_name` (the directive's high-card resolution key)

**Test A — raw `company_name = 'x'`** — `explain_plan`:

```
LanceRead: uri=…/pdl_companies/data, projection=[pdl_company_id, company_name],
           num_fragments=34, row_id=false,
           full_filter=company_name = Utf8("x"), refine_filter=--
  ScalarIndexQuery: query=[company_name = x]@company_name_idx(BTree)
```
`analyze_plan`: `rows_scanned=524` (matched only), `output_rows=524`, `bytes_read=3.27 MB`,
`fragments_scanned=34`, index `search_time≈3.07 ms`, **wall ≈ 9–23 ms.** `refine_filter` empty —
the BTree resolves the row addresses directly.

**Test B — `name_norm(company_name) = 'X'`** — `explain_plan` (macro as DataFusion lowered it;
`VARCHAR→Utf8`, `trim→btrim`):

```
ProjectionExec → Take(columns="company_name, _rowid, (pdl_company_id)")
  → LanceRead: projection=[company_name], num_fragments=34, row_id=true,
      full_filter = refine_filter =
        nullif(btrim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
          upper(CAST(company_name AS Utf8)),"&"," AND ","g"),
          "[-\x{2013}\x{2014}]+"," ","g"),"[^A-Z0-9 ]+","","g"),"\s+"," ","g")),"") = Utf8("X")
```
`analyze_plan`: `rows_scanned=35,446,771`, `fragments_scanned=34`, `output_rows=549`,
`bytes_read=623 MB`, **wall ≈ 15,809–15,901 ms.** **No `ScalarIndexQuery`** — per-row
`refine_filter` over the full scan, plus a `Take` to re-fetch `pdl_company_id` by row-id.
DuckDB `EXPLAIN` over the same Arrow stream agrees — the macro is a post-scan `FILTER`, never a
pushdown:

```
PROJECTION ← FILTER(CASE WHEN trim(regexp_replace(…upper(company_name)…))=''
                         THEN NULL ELSE trim(regexp_replace(…)) END = 'X')
           ← ARROW_SCAN                                    -- full company_name stream
```

| Probe | Predicate | `ScalarIndexQuery`? | `rows_scanned` | Returned | `bytes_read` | Wall |
|---|---|---|--:|--:|--:|--:|
| **A** raw name | `company_name = 'x'` | ✅ `@company_name_idx(BTree)` | **524** | 524 | 3.27 MB | ~9 ms |
| **B** macro name | `name_norm(company_name) = 'X'` | 🛑 no | **35,446,771** | 549 | 623 MB | ~15,901 ms |

The trained BTree reads **524 rows**; the macro discards it and reads the **entire
35,446,771-row table** — a **67,646× rows-scanned penalty**, **190× bytes**, **≈1,767× wall**,
purely from wrapping the indexed column in a function. The 524-vs-549 returned-row gap is the
macro doing real normalization work (folding case/whitespace variants of `x`) — which is exactly
why the answer is **materialize the normalized key**, not abandon normalization (§4).

### 3.2 Positive control — the macro breaks a *trained* index on an identical result set (`country` Bitmap)

`country` is a directive-named geo key that *is* indexed (Bitmap), giving a clean
index→no-index contrast on a single column with an **identical answer**. Sampled
`country='united states'` (9,014,429 rows; `name_norm('united states')='UNITED STATES'`):

| Probe | Predicate | Plan | Index used | `rows_scanned` | Returned | Wall |
|---|---|---|---|--:|--:|--:|
| **P4** raw | `country = 'united states'` | `ScalarIndexQuery=[…]@country_idx(Bitmap)`, `refine_filter=--` | ✅ **yes** | **9,014,429** (matched) | 9,014,429 | 475 ms |
| **P4m** macro | `name_norm(country) = 'UNITED STATES'` | `LanceRead`, `refine_filter=<macro>`, no index node | 🛑 **no** | **35,446,771** (full) | 9,014,429 | 7,823 ms |

Identical column, identical trained index, **identical 9,014,429-row answer**: the raw predicate
resolves through `ScalarIndexQuery` and reads only the 9 M matched rows (pruning 26 M); wrapping
it in `name_norm()` discards the index and forces a full 35,446,771-row scan — **16.5× the wall.**
Even at this low selectivity (25% of the table), the index avoids reading 26 M rows. This is the
empirical proof the directive asked for that read-time normalization macros blind the planner —
shown here on a *live, trained* index (the dead-FEC-BTREE confound that muddied this contrast for
FEC does not exist; PDL's indices are all live).

### 3.3 Index-utilization controls — every declared index, exercised live

| Probe | Predicate | `ScalarIndexQuery`? | Frags | `rows_scanned` | Returned | Wall |
|---|---|---|--:|--:|--:|--:|
| **P1** | `linkedin_url = '…/legoconsult'` (BTree, ~unique) | ✅ `@linkedin_url_idx(BTree)` | **1/34** | **1** | 1 | 19 ms |
| **P2** | `domain = 'legoc.se'` (BTree) | ✅ `@domain_idx(BTree)` | **1/34** | **1** | 1 | 491 ms |
| **P3** | `locality = 'london'` (BTree, high-card geo) | ✅ `@locality_idx(BTree)` | 34/34 | **847,667** | 847,667 | 538 ms |
| **P5** | `industry = 'construction'` (Bitmap) | ✅ `@industry_idx(Bitmap)` | 34/34 | **1,433,587** | 1,433,587 | 163 ms |
| **P6** | `year_founded ∈ [2015,2016]` (BTree **range**) | ✅ `@year_founded_idx(BTree)` | 34/34 | **1,145,296** | 1,145,296 | 866 ms |

For every indexed predicate, `rows_scanned ≈ returned` — the index reads **only matched rows**
and (for the high-selectivity keys `linkedin_url`/`domain`) prunes **33 of 34 fragments**. The
BTree services **range** predicates (`year_founded`) as well as equality. There is no full scan
anywhere except the two macro-wrapped probes (B, P4m).

---

## 4. Structural verdict

| Question (directive) | Answer (measured) |
|---|---|
| Does PDL suffer FEC's commit-order / dead-BTREE failure? | **No.** All 10 indices `indexed==total (35,446,771), unindexed==0`. Overwrite-then-index lifecycle (§2) makes it structurally impossible; boto3 publish mirrors the trained `_indices/` verbatim. |
| Does PDL suffer MSHA's missing-index-on-high-card-strings failure? | **No — the inverse.** `company_name`/`linkedin_url`/`domain`/`locality` carry trained **BTrees**; `industry`/`country`/`region`/`employee_size_range` trained **Bitmaps**. Every resolution key is indexed; only `snapshot_date`/`ingested_at` (provenance) are not. |
| Are nested/array columns blinding the planner? | **No.** All 12 columns flat scalar; zero `List`/`Struct`/`Map`. No key is array-trapped. |
| Exact rows scanned, Test A (raw `company_name`)? | **524** (index) → 524 returned. |
| Exact rows scanned, Test B (macro `company_name`)? | **35,446,771** (full table) → 549 returned. |
| Differential A vs B? | **67,646×** rows scanned · **190×** bytes (3.27 MB → 623 MB) · **≈1,767×** wall (9 ms → 15,901 ms). |
| Does the macro bypass an index / force a full scan? | **Yes — structurally.** `func(col)=lit` is non-indexable; proven on the trained `country` Bitmap (raw → `ScalarIndexQuery` @9.01 M matched, 475 ms; macro → full scan @35.4 M, 7.82 s, identical answer → **16.5×**). DuckDB `EXPLAIN` agrees (post-scan `FILTER`). |
| Differential, indexed key vs macro (same result set)? | **16.5× wall, full-table vs bitmap** (`country`, 9,014,429-row answer either way). |

### 4.1 Architectural remediation

PDL needs **neither** of the two fixes the prior datasets did. There is **no dead index to
retrain** (`optimize_indices()` / `replace=True` rebuilds are unnecessary — the spine is fully
trained) and **no missing BTREE to declare** (every high-card resolution key already carries a
trained index). PDL is the structural opposite of both FEC and MSHA. The remediation is a single
**read-pattern rule**, plus one **conditional** materialization:

1. **Never call `name_norm()` (or any function) on an indexed column in a `WHERE`/join
   predicate against `pdl_companies`.** Lance binds scalar indices to *columns*, not
   *expressions*; `name_norm(col)=lit` is non-indexable and full-scans the 35.4 M-row table
   (proven: **67,646×** on `company_name`, **16.5×** on `country`). For exact lookups and joins
   on raw values, **query the column directly** — `WHERE company_name = 'X'`,
   `WHERE linkedin_url = '…'`, `WHERE domain = '…'` — which already resolves through the trained
   index (`ScalarIndexQuery`, §3.1/§3.3). This is the default and is already optimal; no code
   change is required to *use* it.

2. **Only if a future bridge must block on a *normalized company name*** (case/whitespace/punct
   folding, as Test B does) — materialize the key, do not call the macro at read time. Compute a
   persisted `company_name_norm` column **once at write time** via `core.name_norm.name_norm`,
   build a **BTREE** on it, and issue:

   ```sql
   WHERE company_name_norm = 'X'          -- binds the BTREE → ScalarIndexQuery
   --  NOT  WHERE name_norm(company_name) = 'X'   -- func(col) → 35.4 M-row full scan
   ```

   This is the shipped credit-spine / `normalized_domain` pattern and is **already in use
   downstream** — see §5. It is *not* needed for the current SAM↔PDL resolution path (which
   blocks on `normalized_domain`, already materialized + indexed), so this step is gated on a
   future name-blocking bridge, not on anything live today.

The normalization macro belongs at **WRITE** time (materialize the key), never at **READ** time
(in the predicate) — and every key that anchors the PDL universe today is already correctly
indexed and trained.

---

## 5. Downstream check — `bridge_sam_pdl` already follows the correct pattern

The live SAM↔PDL identity product `active/bridge_sam_pdl` (Lance **v5**, **801,831 rows**, 1
fragment) does **not** exhibit the read-time-macro anti-pattern — it resolves on a **materialized,
indexed normalized key**:

| Index | Type | Field | `indexed` | `unindexed` |
|---|---|---|--:|--:|
| `normalized_domain_idx` | **BTree** | `normalized_domain` | 801,831 | 0 |
| `uei_idx` | **BTree** | `uei` | 801,831 | 0 |
| `duns_idx` | **BTree** | `duns` | 801,831 | 0 |
| `pdl_company_id_idx` | **BTree** | `pdl_company_id` | 801,831 | 0 |

The blocking key is the **persisted `normalized_domain` column** (not `name_norm(domain)` in a
`WHERE`), BTREE-indexed and 100% trained — built/indexed by
`pipelines/resolution/federal_spine_index_campaign.py` (`EXACT_TARGETS ⊇ {normalized_domain}`,
staged-copy `create_scalar_index(..., "BTREE", replace=True)` to dodge the same R2 multipart
escalation the PDL ingest documents). `pdl_company_id` carries a BTree so the bridge joins back
to `pdl_companies` on its perfect PK. **The remediation pattern in §4.1(2) is therefore already
proven in-fleet** — replicate it for *name* blocking only if and when a name-based bridge is
built.

---

## 6. Reproduction (read-only)

```
# pylance 7.0.0 / pyarrow 24 / duckdb 1.5.x / boto3; R2 creds via Doppler core-x/prd
doppler run -p core-x -c prd -- uv run --no-project \
  --with 'pylance>=7' --with 'pyarrow>=17' --with 'duckdb>=1.5,<2' --with boto3 \
  python3 pdl_probe.py
```

The probe calls only `lance.dataset()` (direct R2 read for metadata; boto3 download-only stage
for wall-clock isolation), `list_indices()`, `stats.index_stats()`,
`scanner().explain_plan()/analyze_plan()/count_rows()`, `to_table()` (cardinality/fill), and a
lazy DuckDB `EXPLAIN` over the Arrow stream. The macro is imported verbatim from
`core.name_norm.name_norm`. **Zero mutation:** no `write_dataset`, no `create_scalar_index`, no
`delete`, no `optimize_indices`, no R2 write.
