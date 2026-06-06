# MSHA — Index Topology & Predicate-Pushdown Diagnostic

Read-only, mathematically-grounded interrogation of the **live R2-backed Lance system of
record** for the MSHA (Mine Safety & Health Administration) universe — the exact index
manifest committed to each dataset, the trained-row truth of every index, and an empirical
query-planner trace proving how the engine executes raw vs. normalization-macro predicates
against the high-cardinality name/geo resolution keys. Direct follow-up to
`FEC_INDEX_TOPOLOGY_PUSHDOWN_DIAGNOSTIC.md`: the directive's question is **does MSHA suffer
the same commit-order dead-BTREE failure FEC did?** — answered here with live evidence.

- **Targets interrogated (Gen-3 SoR, `s3://data-sink/active/`):** `msha_mines` (Lance **v7**,
  **91,803 rows**, 80 cols, 1 frag), `msha_corporate_history` (Lance **v6**, **168,809
  rows**, 15 cols, 1 frag), `msha_enforcement_ledger` (Lance **v13**, **3,076,347 rows**,
  120 cols, 3 frags).
  > The directive named `s3://data-sink/active/msha_operator_history/`. **No such dataset
  > exists.** The controller↔operator SCD the directive means is `msha_corporate_history`
  > (`ControllerOperatorHistory.zip`); it is the primary pushdown target below.
- **Live evidence harness (non-mutating, zero writes):** `pylance 7.0.0` direct reads —
  `dataset.list_indices()` (manifest) · `dataset.stats.index_stats()` (type + trained-row
  truth) · `LanceScanner.explain_plan(verbose=True)` (physical plan) ·
  `LanceScanner.analyze_plan()` (EXPLAIN-ANALYZE: real `rows_scanned`/`bytes_read`) ·
  `count_rows(filter=…)` · `pyarrow.compute` for cardinality/fill. DuckDB 1.5.3 `EXPLAIN`
  over the Lance Arrow stream for corroboration. The macro under test is imported verbatim
  from `core.name_norm.name_norm` — byte-identical to every resolution spine.
- **As-of:** probed 2026-06-05 against the committed datasets (single ingest run id=1,
  written 2026-06-03 01:32–01:35 UTC). **No DDL, no index build, no `.lance` write, no
  delete** — every figure is a live read of the committed dataset.
- **Attestation:** the figures below are the physical plans the Lance/DataFusion engine
  emitted and executed against the live fragments on R2, not a recon estimate.

---

## 0. Headline posture

| Verdict | Detail |
|---|---|
| **Does MSHA replicate FEC's dead-BTREE failure?** | ✅ **NO. Zero dead indices.** All **23** committed scalar indices across the 3 datasets report `num_indexed_rows == total`, `num_unindexed_rows == 0` — **100% trained.** MSHA's `mode="overwrite"`-then-`create_scalar_index` lifecycle is the structural inverse of FEC's index-at-v1–6-then-append-24-cycles, so newly-landed rows were never stranded outside a pre-existing index. The commit-order optimization failure **is not present here.** |
| **Are the indexed keys actually used?** | ✅ **Proven live.** Raw point lookups on `CONTROLLER_ID` (BTree), `MINE_ID` (BTree), `STATE` (Bitmap) each emit a `ScalarIndexQuery` node with `refine_filter=--` and read **only the matched rows** (e.g. `MINE_ID` lookup on the 3.08 M ledger: `rows_scanned=871`, 1 of 3 fragments — the index pruned the other two entirely). The index spine is healthy. |
| **But — are the resolution keys the directive named indexed?** | 🛑 **The IDs are; the NAMES and ZIP are NOT.** `operator_name`, `controller_name`, `business_name`, `violator_name`, and `ZIP_CD` carry **no scalar index on any dataset.** Only the surrogate `*_ID` BTREEs and the low-card BITMAP categoricals exist. A point-lookup or join on a *name* or *ZIP* full-scans today — not because an index is dead, but because **none was ever declared.** |
| **Test A — raw `controller_name = '…'`** (corp_history) | 🛑 **NOT indexed. Full scan: `fragments_scanned=1/1`, `rows_scanned=168,809`** to return 1,364 (319 KB, 166 ms). No `ScalarIndexQuery` — `full_filter`+`refine_filter` over the whole table. |
| **Test B — `name_norm(controller_name) = '…'`** | 🛑 **NOT indexed. Full scan: `rows_scanned=168,809`** to return 1,364 (249 ms). Macro parses (DataFusion lowers `trim`→`btrim`, `VARCHAR`→`Utf8`) but binds as a per-row `refine_filter`. |
| **Differential (A vs B)** | **0 rows.** Both scan the entire table — the raw name predicate is *already* a full scan (no index), so the macro cannot *increase* rows scanned. Its only added cost is regex CPU (166 → 249 ms, ~1.5×). |
| **Does the macro break a *trained* index?** | ✅ **Proven independently** on the trained `STATE` Bitmap: raw `STATE='KY'` → `ScalarIndexQuery@STATE_idx(Bitmap)` (`rows_scanned=14,078`, matched only); `name_norm(STATE)='KY'` → no index, `rows_scanned=91,803` (full). `func(col)=lit` is structurally non-indexable. |

**Bottom line:** MSHA passes the FEC audit — **every committed index is trained and used; there is no dead-BTREE / commit-order defect.** The directive's hypothesis does not hold here. What the probe *does* surface is a different, orthogonal gap: the **high-cardinality human-readable resolution keys (operator/controller/business/violator name, ZIP) are unindexed**, so the exact lookups a future entity-bridge will issue full-scan. And the `name_norm()` macro is non-indexable by construction — but for MSHA this is doubly moot today: there is no name index to bypass, and Directive-29 explicitly forbids a `normalized_legal_name` column, so the macro is not in the MSHA write path at all.

---

## 1. Index manifest — exact, from `list_indices()` + `stats.index_stats()`

23 scalar indices committed across the 3 datasets. Every one is **fully trained** —
`indexed == total`, `unindexed == 0`. (Contrast FEC, where 6 of 14 reported `indexed=0`.)

### 1.1 `msha_mines` — Lance v7, 91,803 rows (6 indices, all trained)

| Index | Type | Field | `num_indexed_rows` | `num_unindexed_rows` | State |
|---|---|---|--:|--:|---|
| `MINE_ID_idx` | **BTree** | `MINE_ID` | 91,803 | 0 | ✅ trained |
| `CURRENT_CONTROLLER_ID_idx` | **BTree** | `CURRENT_CONTROLLER_ID` | 91,803 | 0 | ✅ trained |
| `CURRENT_OPERATOR_ID_idx` | **BTree** | `CURRENT_OPERATOR_ID` | 91,803 | 0 | ✅ trained |
| `COAL_METAL_IND_idx` | **Bitmap** | `COAL_METAL_IND` | 91,803 | 0 | ✅ trained |
| `STATE_idx` | **Bitmap** | `STATE` | 91,803 | 0 | ✅ trained |
| `CURRENT_MINE_STATUS_idx` | **Bitmap** | `CURRENT_MINE_STATUS` | 91,803 | 0 | ✅ trained |

### 1.2 `msha_corporate_history` — Lance v6, 168,809 rows (5 indices, all trained)

| Index | Type | Field | `num_indexed_rows` | `num_unindexed_rows` | State |
|---|---|---|--:|--:|---|
| `CONTROLLER_ID_idx` | **BTree** | `CONTROLLER_ID` | 168,809 | 0 | ✅ trained |
| `OPERATOR_ID_idx` | **BTree** | `OPERATOR_ID` | 168,809 | 0 | ✅ trained |
| `MINE_ID_idx` | **BTree** | `MINE_ID` | 168,809 | 0 | ✅ trained |
| `CONTROLLER_TYPE_idx` | **Bitmap** | `CONTROLLER_TYPE` | 168,809 | 0 | ✅ trained |
| `COAL_METAL_IND_idx` | **Bitmap** | `COAL_METAL_IND` | 168,809 | 0 | ✅ trained |

### 1.3 `msha_enforcement_ledger` — Lance v13, 3,076,347 rows (12 indices, all trained)

| Index | Type | Field | `num_indexed_rows` | `num_unindexed_rows` | State |
|---|---|---|--:|--:|---|
| `MINE_ID_idx` | **BTree** | `MINE_ID` | 3,076,347 | 0 | ✅ trained |
| `VIOLATOR_ID_idx` | **BTree** | `VIOLATOR_ID` | 3,076,347 | 0 | ✅ trained |
| `VIOLATION_NO_idx` | **BTree** | `VIOLATION_NO` | 3,076,347 | 0 | ✅ trained |
| `CONTROLLER_ID_idx` | **BTree** | `CONTROLLER_ID` | 3,076,347 | 0 | ✅ trained |
| `EVENT_NO_idx` | **BTree** | `EVENT_NO` | 3,076,347 | 0 | ✅ trained |
| `ASSESS_CASE_NO_idx` | **BTree** | `ASSESS_CASE_NO` | 3,076,347 | 0 | ✅ trained |
| `VIOLATION_ISSUE_DT_idx` | **BTree** | `VIOLATION_ISSUE_DT` | 3,076,347 | 0 | ✅ trained |
| `PROPOSED_PENALTY_AMT_idx` | **BTree** | `PROPOSED_PENALTY_AMT` | 3,076,347 | 0 | ✅ trained |
| `SIG_SUB_idx` | **Bitmap** | `SIG_SUB` | 3,076,347 | 0 | ✅ trained |
| `CIT_ORD_SAFE_idx` | **Bitmap** | `CIT_ORD_SAFE` | 3,076,347 | 0 | ✅ trained |
| `VIOLATOR_TYPE_CD_idx` | **Bitmap** | `VIOLATOR_TYPE_CD` | 3,076,347 | 0 | ✅ trained |
| `COAL_METAL_IND_idx` | **Bitmap** | `COAL_METAL_IND` | 3,076,347 | 0 | ✅ trained |

The manifest **matches the declared plan** in `pipelines/ingest_msha/materialize_msha.py`
(`INDEX_PLAN`) exactly — every intended index is present *and* trained. There is no
committed-but-untrained index anywhere in the MSHA universe.

---

## 2. Why MSHA is immune to the FEC failure — build-order forensics

The FEC defect was a **lifecycle** defect, not an index-type defect. MSHA's lifecycle is the
structural inverse, which is why the same code path that left FEC's BTREEs dead leaves
MSHA's fully trained:

| | FEC `fec_individual_contributions` | MSHA (all 3 datasets) |
|---|---|---|
| Write mode | `append` — 24 per-cycle `delete`+`append` pairs | **`mode="overwrite"`** — one full-snapshot write |
| When indices were built | BTREEs at index-version **1–6** (dataset near-empty), bitmaps later at v64–71 | **All indices built *after* the single overwrite**, against the complete row set |
| Rows present at BTREE build time | ≈0 (pre-backfill) | **100% of rows already landed** |
| Lance auto-folds appends into a scalar index? | No → 282.9 M rows landed *after* v6 stayed invisible to the BTREEs | N/A — nothing is appended after the index build |
| Lance version structure | 72 versions (heavy append churn) | mines **1 overwrite + 6 index commits = v7**; corp **1 + 5 = v6**; enforcement **1 + 12 = v13** |

In `materialize_msha.py::_materialize_one`, `_write_lance(...)` (overwrite) completes and the
committed row count is verified against the spine **before** `_create_indexes(...)` runs
(`materialize_msha.py:587–599`). Every `create_scalar_index` therefore trains over the
finished dataset. The version arithmetic confirms it: each dataset is exactly *one* data
commit plus *one index commit per declared index*, with **no `_deletions/` and no rewrite
churn**. MSHA cannot exhibit FEC's "indexed when there was nothing to index" pathology.

---

## 3. Query-planner diagnostic — physical plans

### 3.1 Primary test — `msha_corporate_history`, `controller_name` (the directive's key)

Sampled a real, high-frequency value off the live column: `CONTROLLER_NAME = 'CRH PLC'`
(1,364 rows; `name_norm` → `'CRH PLC'`, already normalized). That controller maps 1:1 to
`CONTROLLER_ID = 'M06183'` (also 1,364 rows), giving a clean **same-result-set** ID-vs-name
contrast.

**Test A — raw `CONTROLLER_NAME = 'CRH PLC'`** — `explain_plan`:

```
LanceRead: uri=active/msha_corporate_history/data, projection=[CONTROLLER_NAME],
           num_fragments=1, row_id=true,
           full_filter=CONTROLLER_NAME = Utf8("CRH PLC"),
           refine_filter=CONTROLLER_NAME = Utf8("CRH PLC")
```
`analyze_plan`: `fragments_scanned=1, rows_scanned=168,809, output_rows=1,364,
bytes_read=319.4 KB, wall≈166 ms`. **No `ScalarIndexQuery`** — full-table refine_filter.
Read amplification: 168,809 ÷ 1,364 ≈ **124×**.

**Test B — `name_norm(CONTROLLER_NAME) = 'CRH PLC'`** — `explain_plan` (macro as DataFusion
lowered it; `trim`→`btrim`, `VARCHAR`→`Utf8`):

```
LanceRead: projection=[CONTROLLER_NAME], num_fragments=1, row_id=true,
  refine_filter = nullif(btrim(regexp_replace(regexp_replace(regexp_replace(
    regexp_replace(upper(CAST(CONTROLLER_NAME AS Utf8)),"&"," AND ","g"),
    "[-\x{2013}\x{2014}]+"," ","g"),"[^A-Z0-9 ]+","","g"),"\s+"," ","g")),"")
    = Utf8("CRH PLC")
```
`analyze_plan`: `fragments_scanned=1, rows_scanned=168,809, output_rows=1,364, wall≈249 ms`.
**No `ScalarIndexQuery`** — per-row `refine_filter` over the full scan. DuckDB 1.5.3 `EXPLAIN`
over the same stream agrees — the macro is a post-scan `FILTER`, never a pushdown:

```
PROJECTION ← FILTER(CASE WHEN trim(regexp_replace(…upper(CONTROLLER_NAME)…))=''
                         THEN NULL ELSE trim(regexp_replace(…)) END = 'CRH PLC')
           ← ARROW_SCAN (Projections: CONTROLLER_ID, CONTROLLER_NAME)   -- full stream
```

**Control P1 — raw `CONTROLLER_ID = 'M06183'` (trained BTree, identical result set)** —
`explain_plan`:

```
LanceRead: projection=[CONTROLLER_ID, CONTROLLER_NAME, OPERATOR_NAME], num_fragments=1,
           full_filter=CONTROLLER_ID = Utf8("M06183"), refine_filter=--
  ScalarIndexQuery: query=[CONTROLLER_ID = M06183]@CONTROLLER_ID_idx(BTree)
```
`analyze_plan`: `rows_scanned=1,364` (matched only), `bytes_read=25.4 KB`, index
`search_time≈282 µs`. `refine_filter` empty — the BTree resolves the row addresses directly.

| Probe | Predicate | `ScalarIndexQuery`? | `rows_scanned` | Returned |
|---|---|---|--:|--:|
| **A** raw name | `CONTROLLER_NAME = 'CRH PLC'` | 🛑 no | **168,809** | 1,364 |
| **B** macro name | `name_norm(CONTROLLER_NAME) = 'CRH PLC'` | 🛑 no | **168,809** | 1,364 |
| **P1** raw id | `CONTROLLER_ID = 'M06183'` | ✅ `@CONTROLLER_ID_idx(BTree)` | **1,364** | 1,364 |

The ID and the name return the **identical 1,364 rows**; the indexed ID reads 1,364 rows,
the unindexed name reads 168,809 — a **123.8× rows-scanned penalty** purely from the absence
of a name index. A vs B differ by **0 rows** (both full scans).

### 3.2 Positive control — the macro DOES break a *trained* index (`msha_mines.STATE` Bitmap)

`STATE` is the one directive-named key that *is* indexed (Bitmap), so it supplies the clean
index→no-index contrast on a single column. Sampled `STATE = 'KY'` (14,078 rows;
`name_norm('KY')='KY'`):

| Probe | Predicate | Plan | Index used | `rows_scanned` |
|---|---|---|---|--:|
| A2 raw | `STATE = 'KY'` | `ScalarIndexQuery=[STATE = KY]@STATE_idx(Bitmap)`, `refine_filter=--` | ✅ **yes** | **14,078** (matched) |
| B2 macro | `name_norm(STATE) = 'KY'` | `LanceRead num_fragments=1`, `refine_filter=<macro>`, no index node | 🛑 **no** | **91,803** (full) |

Identical column, identical trained index: the raw predicate resolves through
`ScalarIndexQuery`; wrapping it in `name_norm()` discards the index and forces a full
91,803-row scan — **6.5× more rows.** This is the empirical proof the directive asked for that
read-time normalization macros blind the planner. (The dead-FEC-BTREE confound that prevented
this contrast on `employer` does not exist here — MSHA's indices are all live, so the macro's
index-suppression is shown cleanly.)

### 3.3 The unindexed-ZIP control — `msha_mines.ZIP_CD`

`ZIP_CD = '41501'` (793 rows), raw string, no macro: `explain_plan` shows
`full_filter=ZIP_CD = Utf8("41501")`, `refine_filter=ZIP_CD = Utf8("41501")`, **no
`ScalarIndexQuery`**; `analyze_plan` `rows_scanned=91,803` (full) → 793 returned. ZIP is
**unindexed → 115.8× read amplification** on a geographic point lookup.

### 3.4 Scale demonstration — `msha_enforcement_ledger` (3.08 M rows, 3 fragments)

| Probe | Predicate | `ScalarIndexQuery`? | Frags | `rows_scanned` | Returned | Wall |
|---|---|---|--:|--:|--:|--:|
| **E-A** raw id | `MINE_ID = '0100003'` | ✅ `@MINE_ID_idx(BTree)` | **1/3** | **871** | 871 | — |
| **E-B** raw name | `CONTROLLER_NAME = 'CONSOL Energy Inc'` | 🛑 no | 3/3 | **3,076,347** | 87,838 | 1.40 s |
| **E-C** macro name | `name_norm(CONTROLLER_NAME) = 'CONSOL ENERGY INC'` | 🛑 no | 3/3 | **3,076,347** | 87,838 | 1.14 s |

The trained `MINE_ID` BTree prunes **2 of 3 fragments** and reads **871 rows**; the unindexed
`CONTROLLER_NAME` reads the **entire 3,076,347-row table** across all 3 fragments — a
**3,532× rows-scanned gap** between an indexed key and an unindexed one on the same dataset.
E-B vs E-C differ by **0 rows** (the macro neither helps nor hurts row count on an
already-unindexed column; at this scale both are I/O-bound, so the regex CPU is hidden).

---

## 4. Structural verdict

| Question (directive) | Answer (measured) |
|---|---|
| Does MSHA suffer FEC's commit-order / dead-BTREE failure? | **No.** All 23 indices `indexed==total, unindexed==0`. Overwrite-then-index lifecycle (§2) makes it structurally impossible. |
| Are the high-cardinality columns trained? | **The ones that are indexed, yes (100%):** all `*_ID` BTREEs, all categorical BITMAPs. **But the high-card *name* keys are not indexed at all** — `operator_name` (53,404 / 68,863 distinct), `controller_name` (40,693 / 59,406 / 19,373), `business_name` (53,296), `violator_name` (47,149), and `ZIP_CD` (15,916) carry **no index** on any dataset. "Trained" is N/A — there is nothing to train. |
| Exact rows scanned, Test A (raw name)? | **168,809** (corp_history, full table) → 1,364 returned. On the 3.08 M ledger: **3,076,347.** |
| Exact rows scanned, Test B (macro name)? | **168,809** (corp_history) → 1,364. On the ledger: **3,076,347.** |
| Differential A vs B (rows)? | **0** — both full scans (no name index to bypass either way). Macro adds ~1.5× regex CPU on the small set; hidden under I/O at ledger scale. |
| Does the macro bypass an index / force a full scan? | **Yes — structurally.** `func(col)=lit` is non-indexable; proven on the trained `STATE` Bitmap (raw → `ScalarIndexQuery` @14,078 rows; macro → full scan @91,803). |
| Differential, indexed key vs unindexed key (same result set)? | **123.8×** (`CONTROLLER_ID` 1,364 vs `CONTROLLER_NAME` 168,809, corp_history); **3,532×** (`MINE_ID` 871 vs `CONTROLLER_NAME` 3.08 M, ledger). |

### 4.1 Architectural remediation

MSHA does **not** need FEC's fix. There is no dead index to retrain — `optimize_indices()` /
`replace=True` rebuilds are unnecessary; the spine is healthy. The remediation is **additive
coverage for the unindexed resolution keys**, and it splits by use case:

1. **Raw-name / ZIP point lookups before bridging — add the missing BTREEs.** If exact
   lookups or joins on `operator_name` / `controller_name` / `business_name` /
   `violator_name` / `ZIP_CD` are needed against the MSHA universe on its own keys, extend
   `INDEX_PLAN` with **BTREE** indexes on those columns (their cardinalities — 15.9 K–68.9 K
   distinct — mandate BTREE, never BITMAP) and re-run `reindex`. Each is a single
   `create_scalar_index(col, "BTREE")`; the overwrite-then-index lifecycle guarantees they
   train fully. This converts the 124×–3,532× full-scan penalties above into `ScalarIndexQuery`
   point reads.

2. **Normalized cross-universe resolution — materialize the key, never call the macro in
   `WHERE`.** Lance binds scalar indices to *columns*, not *expressions*; `name_norm(col)=lit`
   is non-indexable (§3.2). When the operator/controller entity bridge is built (the unbuilt
   downstream step flagged in `MSHA_LANCE_STATE_DIAGNOSTIC.md` §5), compute persisted
   `*_name_norm` columns **once at write time** via `core.name_norm.name_norm`, build a
   **BTREE on each**, and issue:

   ```sql
   WHERE controller_name_norm = 'CRH PLC'      -- binds the BTREE → ScalarIndexQuery
   --  NOT  WHERE name_norm(controller_name) = 'CRH PLC'   -- func(col) → full scan
   ```

   This is the shipped credit-spine pattern
   (`pipelines/resolution/credit_spine_normalize_index.py` → `normalized_legal_name` + BTREE).
   Note Directive-29 currently forbids a `normalized_legal_name` column in the isolated MSHA
   sets — so this step is gated on the bridging directive that lifts that guardrail, not on
   this read-only probe.

The normalization macro belongs at **WRITE** time (materialize the key), never at **READ**
time (in the predicate) — and the IDs that anchor the MSHA universe are already correctly
indexed and trained.

---

## 5. Reproduction (read-only)

```
# pylance 7.0.0 / duckdb 1.5.3 / pyarrow 24 / boto3; R2 creds via Doppler core-x/prd
doppler run --project core-x --config prd -- python msha_probe.py
```

The probe calls only `lance.dataset()`, `list_indices()`, `stats.index_stats()`,
`scanner().explain_plan()/analyze_plan()/count_rows()`, `pyarrow.compute` (cardinality/fill),
and a lazy DuckDB `EXPLAIN` over the Arrow stream. The macro is imported verbatim from
`core.name_norm.name_norm`. **Zero mutation:** no `write_dataset`, no `create_scalar_index`,
no `delete`, no `optimize_indices`.
