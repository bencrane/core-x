# SBA 7(a) / 504 / PPP — Index Topology & Predicate-Pushdown Diagnostic

Read-only, mathematically-grounded interrogation of the **live R2-backed Lance system of
record** for the three SBA borrower datasets — the exact index manifest committed to each
dataset, the trained-row truth of every index, and an empirical query-planner trace proving
how the engine executes raw vs. normalization-macro vs. flat-materialized predicates against
the high-cardinality borrower-name and geo resolution keys.

- **Targets interrogated (Gen-3 SoR):**
  - `s3://data-sink/active/sba_7a/`  — Lance **v27**, **1,947,098 rows**, 2 fragments, 49 cols.
  - `s3://data-sink/active/sba_504/` — Lance **v35**, **227,404 rows**, 1 fragment, 46 cols.
  - `s3://data-sink/active/ppp/`     — Lance **v37**, **11,468,210 rows**, 13 fragments, 58 cols.
  - The directive's guessed `active/sba_ppp/` **does not exist** — the canonical PPP path is
    `active/ppp/`. The three programs are **distinct datasets**; there is no consolidated SBA table.
- **Live evidence harness (non-mutating, zero writes):** `pylance 7.0.0` direct reads —
  `dataset.list_indices()` (manifest) · `dataset.stats.index_stats()` (BTREE/BITMAP +
  trained-row truth) · `LanceScanner.explain_plan(verbose=True)` (physical plan) ·
  `LanceScanner.analyze_plan()` (EXPLAIN-ANALYZE, real `rows_scanned`) · `count_rows(filter=…)`.
  DuckDB 1.5.3 for normalized-column fidelity + cardinality. The macro under test is imported
  verbatim from `core.name_norm.name_norm` (the single-source-of-truth blocking-key rule).
- **As-of:** probed 2026-06-05 against committed Lance v27 / v35 / v37. **No DDL, no index
  build, no `.lance` write, no delete** — every figure is a live read of the committed dataset.
- **Attestation:** the headline below is not a recon estimate. It is the physical plan the
  Lance/DataFusion engine emitted and executed against the live fragments on R2.

---

## 0. Headline posture — the SBA topology is the FEC remediation, already shipped

The FEC diagnostic (`FEC_INDEX_TOPOLOGY_PUSHDOWN_DIAGNOSTIC.md`) found two failures and
prescribed two fixes: **(1)** retrain the dead BTREEs, **(2)** materialize the normalized key
as a stored, indexed column and query it directly. **The SBA datasets have both in place.**

| Verdict | Detail |
|---|---|
| **FEC Trap (committed-but-untrained indices)** | ✅ **ABSENT on all three.** Every one of the **41 committed indices** (15 + 15 + 11) reports `num_unindexed_rows = 0` and `num_indexed_rows = full table cardinality`. Zero dead indices. No `replace=True` retrain required. |
| **Loan identifiers** | ✅ `sba_surrogate_id` (7a/504, minted PK) and `loan_number` (PPP) are **BTree, fully trained, and emit `ScalarIndexQuery`.** |
| **Normalized name key materialized?** | ✅ **Yes — already done (Directive-29 is shipped).** `normalized_legal_name` is a **stored BTREE-indexed column** on all three, **byte-identical to `core.name_norm(raw_name)`** (0 mismatches / 5,000-row samples ×3). The "materialize a flat `borrower_name_norm` column" remediation exists and is named `normalized_legal_name`. |
| **Normalized geo key materialized?** | ✅ **Yes.** `zip_code` is a **stored BTREE-indexed** 5-digit geo key on all three (verbatim where the source is already 5-digit; ZIP+4→left-5 on PPP). |
| **MSHA Trap (missing index on high-card resolution string)** | ⚠️ **Partial, secondary keys only.** Raw `borr_street`/`borrower_address` (86–90% distinct), `borr_city`/`borrower_city`, and the raw ZIP columns are **unindexed** on all three; **raw `borrower_name` is unindexed on PPP** (99.96% distinct). The *canonical normalized path* is fully indexed; the gaps are on raw/secondary access paths. |
| **Test B — `name_norm(name)` in the WHERE clause** | 🛑 **Never indexed — structurally.** `func(col)=lit` discards the scalar index and forces a full `refine_filter` scan on every dataset. |
| **Test C — flat `normalized_legal_name = '…'`** | ✅ **`ScalarIndexQuery@normalized_legal_name_idx(BTree)` on all three.** Same logical predicate as B, served as a point lookup. |
| **Differential B → C (rows scanned)** | **7a: 1,947,098 → 3.   504: 227,404 → 1.   PPP: 11,468,210 → 1.** On PPP that is an **11,468,210× read-amplification collapse** and **~62× wall-clock** (5.86 s → 0.095 s) even over a non-in-region S3 link. |

**Bottom line:** the SBA datasets do **not** exhibit the FEC Trap (no dead indices) and only
partially exhibit the MSHA Trap (raw secondary keys unindexed, but the normalized name + geo
keys are materialized and BTREE-indexed). The single actionable defect is **at read time, not
write time**: entity-resolution queries must target the flat `normalized_legal_name` /
`zip_code` columns, never `name_norm(col)` wrappers (Test B) — and on PPP, never raw
`borrower_name` (which is itself unindexed). No retrain, no new normalized column required.

---

## 1. Index manifest — exact, from `dataset.list_indices()` + `stats.index_stats()`

Every index below reports `num_unindexed_rows = 0` and `num_indexed_rows = total table
cardinality` → **HEALTHY / fully trained**. (Contrast FEC, whose 6 BTREEs were `indexed_rows=0`.)

### 1.1 `sba_7a` — 15 indices, all BTree, all `indexed_rows = 1,947,098`

| Field | Index | Type | Indexed rows | Unindexed | State |
|---|---|---|--:|--:|---|
| `sba_surrogate_id` | `sba_surrogate_id_idx` | BTree | 1,947,098 | 0 | ✅ PK |
| `borr_name` | `borr_name_idx` | BTree | 1,947,098 | 0 | ✅ |
| `normalized_legal_name` | `normalized_legal_name_idx` | BTree | 1,947,098 | 0 | ✅ canonical name key |
| `zip_code` | `zip_code_idx` | BTree | 1,947,098 | 0 | ✅ canonical geo key |
| `borr_state` | `borr_state_idx` | BTree | 1,947,098 | 0 | ✅ |
| `project_state` · `naics_code` · `approval_fy` · `loan_status` · `location_id` · `congressional_district` · `business_type` · `sba_district_office` · `bank_name` · `bank_fdic_number` | *(10 more)* | BTree | 1,947,098 | 0 | ✅ |

### 1.2 `sba_504` — 15 indices, all BTree, all `indexed_rows = 227,404`

Identical core set to 7(a), with the program-specific lender keys `cdc_name_idx` and
`third_party_lender_name_idx` replacing the 7(a) `bank_*` pair. `sba_surrogate_id`,
`borr_name`, `normalized_legal_name`, `zip_code`, `borr_state` all BTree / 227,404 / 0.

### 1.3 `ppp` — 11 indices, all `indexed_rows = 11,468,210`

| Field | Index | Type | Indexed rows | Unindexed | State |
|---|---|---|--:|--:|---|
| `loan_number` | `loan_number_idx` | **BTree** | 11,468,210 | 0 | ✅ loan PK |
| `normalized_legal_name` | `normalized_legal_name_idx` | **BTree** | 11,468,210 | 0 | ✅ canonical name key |
| `zip_code` | `zip_code_idx` | **BTree** | 11,468,210 | 0 | ✅ canonical geo key |
| `naics_code` · `servicing_lender_location_id` · `originating_lender_location_id` | *(3)* | **BTree** | 11,468,210 | 0 | ✅ |
| `borrower_state` · `project_state` · `business_type` · `processing_method` · `loan_status` | *(5)* | **Bitmap** | 11,468,210 | 0 | ✅ low-card categoricals |

> Three-way confirmation that the index machinery is healthy (not a pylance reporting quirk):
> `index_stats` reports full `indexed_rows`, `explain_plan` emits `ScalarIndexQuery`, and
> `analyze_plan` reads only the matched rows — all consistent. (FEC's BTREEs failed all three.)

---

## 2. Resolution-key audit — cardinality vs. index coverage (the MSHA-Trap analysis)

Distinct counts are **exact** for 7(a)/504, **approx (HLL)** for the 11.47 M-row PPP. A key is
a genuine high-cardinality resolution string (MSHA-Trap candidate) when distinct ≈ row count.

### 2.1 `sba_7a` (1,947,098 rows)

| Key | Distinct | % rows | Indexed? | Verdict |
|---|--:|--:|---|---|
| `borr_name` | 1,575,154 | 80.9% | ✅ `borr_name_idx` BTree | indexed |
| `normalized_legal_name` | 1,509,760 | 77.5% | ✅ `normalized_legal_name_idx` BTree | **canonical name key — indexed** |
| `borr_street` | 1,679,088 | **86.2%** | 🛑 none | **MSHA gap** (highest-card key, unindexed) |
| `borr_city` | 45,763 | 2.35% | 🛑 none | unindexed (moderate-card) |
| `borr_zip` | 36,966 | 1.90% | 🛑 none | unindexed raw — **values identical to `zip_code`** |
| `zip_code` | 36,966 | 1.90% | ✅ `zip_code_idx` BTree | **canonical geo key — indexed** |

### 2.2 `sba_504` (227,404 rows)

| Key | Distinct | % rows | Indexed? | Verdict |
|---|--:|--:|---|---|
| `borr_name` | 210,693 | 92.7% | ✅ BTree | indexed |
| `normalized_legal_name` | 206,773 | 90.9% | ✅ BTree | **canonical name key — indexed** |
| `borr_street` | 220,539 | **97.0%** | 🛑 none | **MSHA gap** |
| `borr_city` | 19,885 | 8.7% | 🛑 none | unindexed |
| `borr_zip` | 19,136 | 8.4% | 🛑 none | unindexed raw — identical to `zip_code` |
| `zip_code` | 19,136 | 8.4% | ✅ BTree | **canonical geo key — indexed** |

### 2.3 `ppp` (11,468,210 rows)

| Key | Distinct (HLL) | % rows | Indexed? | Verdict |
|---|--:|--:|---|---|
| `borrower_name` | 11,464,067 | **99.96%** | 🛑 none | **MSHA gap — raw name is unindexed on PPP** (near-unique) |
| `normalized_legal_name` | 9,726,405 | 84.8% | ✅ BTree | **only indexed name path on PPP** |
| `borrower_address` | 10,280,080 | 89.6% | 🛑 none | **MSHA gap** |
| `borrower_city` | 77,038 | 0.67% | 🛑 none | unindexed |
| `borrower_zip` | 6,493,401 | **56.6%** | 🛑 none | unindexed raw (ZIP+4 noise) |
| `zip_code` | 40,886 | 0.36% | ✅ BTree | **canonical geo key** — ZIP+4 collapsed to clean 5-digit |
| `borrower_state` | low | — | ✅ Bitmap | indexed |
| `loan_number` | ~unique | — | ✅ BTree | loan PK indexed |

> **`normalized_legal_name` fidelity (DuckDB, 5,000-row samples):** `normalized_legal_name`
> `IS DISTINCT FROM core.name_norm(raw_name)` → **0 mismatches on all three.** The stored
> column is the canonical blocking key, not a stale copy.
> **`zip_code` derivation:** 7(a)/504 `zip_code == borr_zip` verbatim (source already 5-digit);
> PPP `zip_code == left-5(borrower_zip)` (0 mismatches vs left-5), collapsing 6.49 M ZIP+4
> values into 40,886 geo buckets.

---

## 3. Query-planner diagnostic — physical plans (A / B / C)

For each dataset a real `(raw_name, normalized_legal_name)` pair was sampled off the live
column. **A** = raw name; **B** = the canonical `name_norm()` macro wrapping the raw column;
**C** = the flat materialized `normalized_legal_name` column. `analyze_plan()` executed; figures
are real. (Caveat: wall-clock is a single-shot read from a non-in-region client — the
deterministic, location-independent proof is `rows_scanned`.)

| Dataset | Test | Predicate | `ScalarIndexQuery`? | Index used | `rows_scanned` | matched | wall |
|---|---|---|---|---|--:|--:|--:|
| **sba_7a** | A | `borr_name = 'Galaide Professional Services, Inc.'` | ✅ | `borr_name_idx(BTree)` | **3** | 3 | 0.596 s |
| | B | `name_norm(borr_name) = 'GALAIDE…INC'` | 🛑 | — | **1,947,098** | 3 | 1.741 s |
| | C | `normalized_legal_name = 'GALAIDE…INC'` | ✅ | `normalized_legal_name_idx(BTree)` | **3** | 3 | 0.178 s |
| **sba_504** | A | `borr_name = 'SUPERIOR PROCESSING A CALIFORN'` | ✅ | `borr_name_idx(BTree)` | **1** | 1 | 0.282 s |
| | B | `name_norm(borr_name) = '…'` | 🛑 | — | **227,404** | 1 | 0.492 s |
| | C | `normalized_legal_name = '…'` | ✅ | `normalized_legal_name_idx(BTree)` | **1** | 1 | 0.147 s |
| **ppp** | A | `borrower_name = 'SUMTER COATINGS, INC.'` | 🛑 | — *(raw name unindexed)* | **11,468,210** | 1 | 2.797 s |
| | B | `name_norm(borrower_name) = 'SUMTER COATINGS INC'` | 🛑 | — | **11,468,210** | 1 | 5.860 s |
| | C | `normalized_legal_name = 'SUMTER COATINGS INC'` | ✅ | `normalized_legal_name_idx(BTree)` | **1** | 1 | 0.095 s |

### 3.1 Indexed path (7a Test C) — `analyze_plan()` excerpt

```
ScalarIndexQuery: query=[normalized_legal_name = GALAIDE PROFESSIONAL SERVICES INC]
                  @normalized_legal_name_idx(BTree), elapsed=354µs
LanceRead: full_filter=normalized_legal_name = Utf8("GALAIDE PROFESSIONAL SERVICES INC"),
           rows_scanned=3, output_rows=3        ← reads only the matched rows
```

### 3.2 Macro path (7a Test B) — `analyze_plan()` excerpt

```
LanceRead: num_fragments=2, row_id=true,
  full_filter=nullif(btrim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
    upper(CAST(borr_name AS Utf8)),'&',' AND ','g'),'[-\x{2013}\x{2014}]+',' ','g'),
    '[^A-Z0-9 ]+','','g'),'\s+',' ','g')),'') = Utf8("GALAIDE PROFESSIONAL SERVICES INC"),
  rows_scanned=1,947,098, output_rows=3        ← full column scan, per-row refine
```

The macro **parses** in Lance's native filter (DataFusion accepts the 4-arg `regexp_replace`,
`upper`, `nullif`, `btrim`) but binds to **no** scalar index — a function-wrapped column is
structurally non-indexable. Same logical result as Test C (3 rows), **649,033× more rows read.**

### 3.3 PPP — the flat column is the *only* indexed name path

On PPP both the raw-name lookup (A) **and** the macro (B) full-scan all 11,468,210 rows
(`borrower_name` carries no scalar index); only the flat `normalized_legal_name` (C) resolves
through `ScalarIndexQuery`, reading **1** row. **A vs C: 11,468,210 → 1 rows, 2.797 s → 0.095 s
(~29×). B vs C: 11,468,210 → 1, 5.860 s → 0.095 s (~62×).**

> **Harness footgun (operator-relevant):** Lance's DataFusion filter dialect treats
> **double-quoted identifiers as string literals**. `"borr_name" = 'X'` silently constant-folds
> to `Boolean(false)` → full scan returning 0 rows — which would make a naive pushdown probe
> wrongly conclude "indexes are never used." **All Lance scanner filters must use bare
> identifiers** (`borr_name = 'X'`). This was the failure mode in the first two harness passes;
> the figures above are from the corrected bare-identifier predicates.

---

## 4. Structural verdict

| Question (directive) | Answer (measured) |
|---|---|
| Any index with `indexed_rows = 0` (FEC Trap)? | **No — zero across all 41 indices.** Every index `indexed_rows = full cardinality`, `unindexed_rows = 0`. |
| Do borrower-name / geo keys have trained BTREEs? | **Normalized keys: yes** — `normalized_legal_name_idx` + `zip_code_idx` BTree, fully trained, on all three. **Raw keys: partial** — `borr_name` BTree on 7a/504; **raw `borrower_name` unindexed on PPP**; `*_street`/`*_address`, `*_city`, raw `*_zip` unindexed everywhere. |
| Loan identifiers indexed? | **Yes** — `sba_surrogate_id` (7a/504) + `loan_number` (PPP), BTree, trained, `ScalarIndexQuery`. |
| Test A vs B vs C — which emits `ScalarIndexQuery`? | **A** indexed on 7a/504 (raw `borr_name`), **not** on PPP. **B** never (macro). **C** always (flat normalized column). |
| Exact rows-scanned differential (B → C)? | **7a 1,947,098 → 3 · 504 227,404 → 1 · PPP 11,468,210 → 1.** Wall: 9.8× / 3.3× / 61.7×. |

### 4.1 Precise architectural remediation

1. **Retrain dead indices (`replace=True`)?** — **Not required.** There are no dead indices.
   The SBA workers' `create_scalar_index(..., replace=True)` pass (`sba_foia/ingest.py::_create_indexes`,
   `sba_ppp/ppp_loans_bulk.py::build_ppp_indexes`) ran against the **completed** datasets; all
   index versions post-date the final writes, so every fragment is folded in.
2. **Add a flat `borrower_name_norm` column (Directive-29)?** — **Not required; already shipped.**
   The stored, BTREE-indexed `normalized_legal_name` (== canonical `core.name_norm`, 0 drift) and
   `zip_code` (5-digit geo) are the materialized keys, present and trained on all three.
3. **The one actionable fix is at READ time — route resolution through the flat columns.**
   ```sql
   WHERE normalized_legal_name = 'SUMTER COATINGS INC'   -- binds BTREE → ScalarIndexQuery (1 row)
   --  NOT  WHERE name_norm(borrower_name) = 'SUMTER COATINGS INC'   -- func(col) → full scan (11.47 M)
   --  NOT  WHERE borrower_name = 'SUMTER COATINGS, INC.'            -- PPP raw name unindexed → full scan
   ```
   Geo joins must likewise target `zip_code`, never raw `borrower_zip` (ZIP+4, unindexed, 6.49 M distinct).
4. **Optional BTREE additions — only if the raw/secondary access path is actually exercised**
   (no DDL authorized by this read-only probe; listed for the write-side owner):
   - **`ppp.borrower_name`** — iff un-normalized exact-name lookups are needed on PPP (today a
     full 11.47 M-row scan). High-card sort; the worker image already sets `LANCE_BYPASS_SPILLING`.
   - **`{7a,504,ppp}.borrower_city` / `borrower_address`** — iff city/street blocking is used.
   - Raw `*_zip` BTREEs are **redundant** (7a/504: identical to indexed `zip_code`; PPP: the
     indexed 5-digit `zip_code` is the correct geo key, not the ZIP+4 raw).

**No Directive-29 override is needed:** the flat normalized borrower-name column it would
authorize already exists, is canonical, and is fully indexed. The remaining work is query
routing (use the materialized columns) — not data-plane mutation.

---

## 5. Reproduction (read-only)

```
# pylance 7.0.0 / duckdb 1.5.3 / pyarrow 24; R2 creds via Doppler core-x/prd
doppler run --project core-x --config prd -- python diag.py    # §1 manifest, §2 audit+fidelity
doppler run --project core-x --config prd -- python diag3.py   # §3 A/B/C pushdown (BARE identifiers)
doppler run --project core-x --config prd -- python card.py    # §2 cardinality (exact 7a/504, HLL ppp)
```

Every script calls only `lance.dataset()`, `list_indices()`, `stats.index_stats()`,
`scanner().explain_plan()/analyze_plan()/count_rows()`, bounded column scans, and lazy DuckDB
over the Arrow stream. **Zero mutation:** no `write_dataset`, no `create_scalar_index`, no
`add_columns`, no `delete`, no `.restore`.
