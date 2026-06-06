# GLEIF — Index Topology & Predicate-Pushdown Diagnostic

Read-only, mathematically-grounded interrogation of the **live R2-backed Lance system of
record** for the GLEIF (Global Legal Entity Identifier Foundation) universe — the exact
index manifest committed to each dataset, the trained-row truth of every index, and an
empirical query-planner trace proving how the engine executes raw vs. normalization-macro
predicates against the high-cardinality corporate-name / geo resolution keys. Direct
follow-up to `FEC_INDEX_TOPOLOGY_PUSHDOWN_DIAGNOSTIC.md` (the dead-BTREE trap) and
`MSHA_INDEX_TOPOLOGY_PUSHDOWN_DIAGNOSTIC.md` (the missing-name-index gap): the directive's
question is **does GLEIF suffer FEC's commit-order dead-BTREE failure, or MSHA's missing
high-cardinality-name-index failure?** — answered here with live evidence.

- **Targets interrogated (Gen-3 SoR, `s3://data-sink/active/`):** `gleif_l1_entities`
  (Lance **v175**, **3,330,881 rows**, 11 cols, **34 fragments**) — the Level-1 LEI entity
  master; `gleif_l2_relationships` (Lance **v35**, **475,125 rows**, 7 cols, **5 fragments**)
  — the Level-2 parent/child edge table.
  > The directive named `s3://data-sink/active/gleif_golden_copy/`. **No such dataset
  > exists.** The active projection of the GLEIF golden copy is split across the two
  > datasets above (`pipelines/gleif/ingest.py` → `gleif_l1_entities` / `gleif_l2_relationships`);
  > both are interrogated below.
- **Live evidence harness (non-mutating, zero writes):** `pylance 7.0.0` direct reads —
  `dataset.list_indices()` (manifest) · `dataset.stats.index_stats()` (type + trained-row
  truth) · `LanceScanner.explain_plan(verbose=True)` (physical plan) ·
  `LanceScanner.analyze_plan()` (EXPLAIN-ANALYZE: real `rows_scanned`/`bytes_read`) ·
  `count_rows(filter=…)` · `pyarrow.compute` for cardinality. DuckDB 1.5.3 `EXPLAIN` over
  the Lance Arrow stream for corroboration. The macro under test is imported verbatim from
  `core.name_norm.name_norm` — byte-identical to every resolution spine.
- **As-of:** probed **2026-06-05** against the committed datasets. **No DDL, no index build,
  no `.lance` write, no delete, no `optimize_indices`** — every figure is a live read of the
  committed dataset. Probe deps in an ephemeral venv; `/tmp/gleif_probe.py` +
  `/tmp/gleif_poscontrol.py`, Doppler-injected R2 creds (`core-x/prd`).
- **Attestation:** the figures below are the physical plans the Lance/DataFusion engine
  emitted and executed against the live fragments on R2, not a recon estimate.

---

## 0. Headline posture

| Verdict | Detail |
|---|---|
| **Does GLEIF replicate FEC's dead-BTREE failure?** | ✅ **NO. Zero dead indices.** All **3** committed scalar indices (L1 `lei`; L2 `lei`, `parent_lei`) report `num_indexed_rows == total`, `num_unindexed_rows == 0` — **100% trained.** GLEIF's daily-overwrite-then-`create_scalar_index(replace=True)` lifecycle (§2) builds the index *after* every append in the cycle, over the complete row set, so no row is ever stranded outside the index. The commit-order optimization failure **is not present here.** |
| **Are the indexed keys actually used?** | ✅ **Proven live.** Raw point lookups on L1 `lei` and L2 `lei` / `parent_lei` each emit a `ScalarIndexQuery@…_idx(BTree)` node with `refine_filter=--` and read **only the matched rows** (CRH plc `lei` on the 3.33 M L1: `fragments_scanned=1/34`, `rows_scanned=1`; the index pruned the other 33 fragments entirely). The deterministic-key spine is healthy. |
| **But — are the resolution keys the directive named indexed?** | 🛑 **The LEIs are; the NAME and GEO keys are NOT.** `legal_name`, `legal_address_region`, and `legal_address_country` carry **no scalar index** on `gleif_l1_entities`. Only the `lei` (L1, L2) and `parent_lei` (L2) BTREEs exist. A point-lookup or join on a *legal name*, *region*, or *country* full-scans all 3.33 M rows today — not because an index is dead, but because **none was ever declared.** This is the **MSHA failure mode, not the FEC one.** |
| **Is `Ultimate_Parent_LEI` materialized?** | 🛑 **No flattened column.** L2 carries only the direct edge (`lei`=child, `parent_lei`=parent); ultimate-vs-direct is encoded in `relationship_type` (`IS_ULTIMATELY_CONSOLIDATED_BY` = **129,980** edges, `IS_DIRECTLY_CONSOLIDATED_BY` = **124,207**). Ultimate-parent bridging is still indexed — `WHERE lei=? AND relationship_type='IS_ULTIMATELY_CONSOLIDATED_BY'` rides the `lei` BTREE — but there is no single-hop `ultimate_parent_lei` column on L1. |
| **Test A — raw `legal_name = '…'`** (L1) | 🛑 **NOT indexed. Full scan: `fragments_scanned=34/34`, `rows_scanned=3,330,881`, `bytes_read=73.12 MB`** to return 1 (CRH plc). No `ScalarIndexQuery` — `full_filter`+`refine_filter` over the whole table. analyze wall ≈ 5.88 s. |
| **Test B — `name_norm(legal_name) = '…'`** | 🛑 **NOT indexed. Full scan: `rows_scanned=3,330,881`, `bytes_read=73.12 MB`** to return 1. Macro parses (DataFusion lowers `trim`→`btrim`, `VARCHAR`→`Utf8`) but binds as a per-row `refine_filter`. analyze wall ≈ 5.45 s. |
| **Differential (A vs B)** | **0 rows.** Both scan the entire table — the raw name predicate is *already* a full scan (no index), so the macro cannot *increase* rows scanned. At 3.33 M rows the single-column read is I/O-bound on R2 (~73 MB), so the regex CPU is hidden (A vs B wall within noise; contrast FEC's 3× CPU blow-up at 283 M rows). |
| **Does the macro break a *trained* index?** | ✅ **Proven independently** on the trained L1 `lei` BTREE: raw `lei='549300MIDJNNTH068E74'` → `ScalarIndexQuery@lei_idx(BTree)` (`rows_scanned=1`); `name_norm(lei)='549300MIDJNNTH068E74'` → no index node, `rows_scanned=3,330,881` (full). `func(col)=lit` is structurally non-indexable. |

**Bottom line:** GLEIF passes the FEC audit — **every committed index is trained and used; there is no dead-BTREE / commit-order defect.** The deterministic identifier spine (`lei` on both levels, `parent_lei` for reverse traversal) is fully indexed and sub-second. What the probe surfaces is the **MSHA-class gap**: the high-cardinality human-readable resolution key the directive cares about for multinational bridging — `legal_name` (≈3.33 M near-distinct) — and the geo tiebreakers `legal_address_region` / `legal_address_country` are **unindexed**, so the exact lookups an entity-bridge issues full-scan. And `name_norm()` in the `WHERE` clause is non-indexable by construction. The remediation is the Directive-29 materialized-`legal_name_norm`-column pattern, not a retrain.

---

## 1. Index manifest — exact, from `list_indices()` + `stats.index_stats()`

3 scalar indices committed across the 2 datasets. Every one is **fully trained** —
`indexed == total`, `unindexed == 0`. (Contrast FEC, where 6 of 14 reported `indexed=0`.)

### 1.1 `gleif_l1_entities` — Lance v175, 3,330,881 rows, 34 fragments (1 index, trained)

| Index | Type | Field | Idx ver | `num_indexed_rows` | `num_unindexed_rows` | State |
|---|---|---|--:|--:|--:|---|
| `lei_idx` | **BTree** | `lei` | 174 | 3,330,881 | 0 | ✅ trained |

`fragment_ids` on `lei_idx` = `{0, 133…165}` — **all 34 current data fragments**, confirming
the index was (re)built against the live fragment set, not a stale pre-append subset.

### 1.2 `gleif_l2_relationships` — Lance v35, 475,125 rows, 5 fragments (2 indices, trained)

| Index | Type | Field | Idx ver | `num_indexed_rows` | `num_unindexed_rows` | State |
|---|---|---|--:|--:|--:|---|
| `lei_idx` | **BTree** | `lei` (StartNode/child) | 33 | 475,125 | 0 | ✅ trained |
| `parent_lei_idx` | **BTree** | `parent_lei` (EndNode/parent) | 34 | 475,125 | 0 | ✅ trained |

The manifest **matches the declared plan** in `pipelines/gleif/ingest.py` (`DATASETS[*]["btree"]`)
exactly — L1 `["lei"]`, L2 `["lei", "parent_lei"]`, every intended index present *and*
trained. There is no committed-but-untrained index anywhere in the GLEIF universe.

### 1.3 Corporate resolution-key audit (the directive's keys)

| Directive key | Active column | Dataset | Indexed? | Trained? |
|---|---|---|---|---|
| **LEI** (deterministic id) | `lei` | L1 **+** L2 | ✅ **BTREE** | ✅ 100% |
| **Entity_LegalName** | `legal_name` | L1 | 🛑 **none** | — |
| **Entity_LegalAddress_Region** | `legal_address_region` (ISO-3166-2, `US-MA`) | L1 | 🛑 **none** | — |
| **Entity_LegalAddress_Country** | `legal_address_country` (ISO-3166-1, `US`) | L1 | 🛑 **none** | — |
| **Parent_LEI** | `parent_lei` | L2 | ✅ **BTREE** | ✅ 100% |
| **Ultimate_Parent_LEI** | *not materialized* — encoded in `relationship_type` | L2 | n/a | — |

`relationship_type` distribution (L2, 475,125 total): `IS_FUND-MANAGED_BY` 146,246 ·
`IS_ULTIMATELY_CONSOLIDATED_BY` **129,980** · `IS_DIRECTLY_CONSOLIDATED_BY` **124,207** ·
`IS_SUBFUND_OF` 71,434 · `IS_INTERNATIONAL_BRANCH_OF` 1,911 · `IS_FEEDER_TO` 1,347. Note the
active L1 projection is **lossy** (per `GLEIF_MSHA_ENTITY_BRIDGE_DIAGNOSTIC.md`): it drops
`postalCode`/ZIP, `headquartersAddress`, and `otherNames[]` — so `legal_name` + region +
country are the *only* binding vectors materialized; a ZIP tiebreaker needs a wider re-ingest.

---

## 2. Why GLEIF is immune to the FEC failure — build-order forensics

The FEC defect was a **lifecycle** defect, not an index-type defect: BTREEs were built at
Lance index-versions 1–6 when the table was near-empty, then 24 append cycles landed
282.9 M rows *after* the index — and Lance does not auto-fold appended rows into an existing
scalar index, so they stayed `unindexed`. GLEIF's lifecycle forecloses this:

| | FEC `fec_individual_contributions` | GLEIF (L1 + L2) |
|---|---|---|
| Write shape | `append` — 24 per-cycle `delete`+`append` pairs | **daily `overwrite` (first 100k-row batch) + intra-run `append` (remaining batches)**, then re-snapshot next day |
| When indices are built | BTREEs at index-version 1–6 (table near-empty), never re-optimized | **`_create_indexes(replace=True)` runs *after* `_stream_to_lance` finishes all appends** (`ingest.py:508→513`), against the complete row set, **every cycle** |
| Rows present at index-build time | ≈0 (pre-backfill) | **100% of the day's rows already landed** |
| Lance folds later appends into the index? | No → 282.9 M post-v6 rows stranded | N/A — the index is the **last** op of the run; nothing is appended after it |
| Live evidence | 6 BTREEs `indexed=0` | **all 3 BTREEs `indexed==total`; `lei_idx.fragment_ids` covers all 34 fragments** |

The daily full-snapshot model (`ingest.py` docstring: "today's dataset always equals today's
published universe") means each run **overwrites** then **re-indexes with `replace=True`** —
the index is rebuilt fresh over the complete daily snapshot. The version arithmetic confirms
churn without index rot: L1 is at data-version **v175** (≈many daily snapshots) yet
`lei_idx` is at index-version **174** and reports `indexed=3,330,881 == total`. The index
tracks the data.

> **Risk that did *not* materialize.** `ingest.py:385–399` makes an index-build failure
> **non-fatal** (logged `WARN`, never raised), and `ingest.py:108–113` forces
> `LANCE_BYPASS_SPILLING=true` precisely because the 3.3 M-row high-cardinality `lei` BTREE
> sort can OOM. A silent index miss would have produced exactly the FEC symptom (data
> committed, index absent/stale). The live read proves it did **not** happen — the spilling
> mitigation held and the `lei` BTREE is current and complete at v175. This is the specific
> failure the directive asked to rule out; it is ruled out empirically, not assumed.

---

## 3. Query-planner diagnostic — physical plans

### 3.1 Primary test — `gleif_l1_entities`, `legal_name` (the directive's key)

The directive's `'CRH PLC'` returns **0 rows** — GLEIF stores the *registered* legal name.
The live value is `legal_name = 'CRH PUBLIC LIMITED COMPANY'` (1 row; `lei =
'549300MIDJNNTH068E74'`, CRH plc's real LEI), giving a clean **same-result-set** id-vs-name
contrast. `name_norm('CRH PUBLIC LIMITED COMPANY')` = `'CRH PUBLIC LIMITED COMPANY'`
(already normalized).

**Test A — raw `legal_name = 'CRH PUBLIC LIMITED COMPANY'`** — `explain_plan`:

```
LanceRead: uri=active/gleif_l1_entities/data, projection=[legal_name], num_fragments=34,
           row_id=true, full_filter=legal_name = Utf8("CRH PUBLIC LIMITED COMPANY"),
           refine_filter=legal_name = Utf8("CRH PUBLIC LIMITED COMPANY")
```
`analyze_plan`: `fragments_scanned=34, rows_scanned=3,330,881, output_rows=1,
bytes_read=73.12 MB, wall≈5.88 s` (`count_rows` 4.49 s). **No `ScalarIndexQuery`** —
full-table refine_filter. Read amplification: 3,330,881 ÷ 1 = **3,330,881×**.

**Test B — `name_norm(legal_name) = 'CRH PUBLIC LIMITED COMPANY'`** — `explain_plan` (macro
as DataFusion lowered it; `trim`→`btrim`, `VARCHAR`→`Utf8`):

```
LanceRead: projection=[legal_name], num_fragments=34, row_id=true,
  refine_filter = nullif(btrim(regexp_replace(regexp_replace(regexp_replace(
    regexp_replace(upper(CAST(legal_name AS Utf8)),"&"," AND ","g"),
    "[-\x{2013}\x{2014}]+"," ","g"),"[^A-Z0-9 ]+","","g"),"\s+"," ","g")),"")
    = Utf8("CRH PUBLIC LIMITED COMPANY")
```
`analyze_plan`: `fragments_scanned=34, rows_scanned=3,330,881, output_rows=1,
bytes_read=73.12 MB, wall≈5.45 s` (`count_rows` 3.85 s). **No `ScalarIndexQuery`** — per-row
`refine_filter` over the full scan. DuckDB 1.5.3 `EXPLAIN` over the same stream agrees — the
macro is a post-scan `FILTER`, never a pushdown:

```
PROJECTION ← FILTER(CASE WHEN trim(regexp_replace(…upper(legal_name)…))=''
                         THEN NULL ELSE trim(regexp_replace(…)) END = 'CRH PUBLIC LIMITED COMPANY')
           ← ARROW_SCAN (Projections: lei, legal_name)   -- full stream
```

**Control P1 — raw `lei = '549300MIDJNNTH068E74'` (trained BTree, identical result set)** —
`explain_plan`:

```
LanceRead: projection=[lei, legal_name], num_fragments=34,
           full_filter=lei = Utf8("549300MIDJNNTH068E74"), refine_filter=--
  ScalarIndexQuery: query=[lei = 549300MIDJNNTH068E74]@lei_idx(BTree)
```
`analyze_plan`: `fragments_scanned=1/34`, `rows_scanned=1` (matched only),
`bytes_read=5.14 KB`; warm `count_rows` 1 ms (cold-index first-touch adds ~1.0 s index-page
fetch from R2 — `search_time`). `refine_filter` empty — the BTree resolves the row address
directly.

| Probe | Predicate | `ScalarIndexQuery`? | Frags | `rows_scanned` | `bytes_read` | Returned |
|---|---|---|--:|--:|--:|--:|
| **A** raw name | `legal_name = 'CRH PUBLIC LIMITED COMPANY'` | 🛑 no | 34/34 | **3,330,881** | 73.12 MB | 1 |
| **B** macro name | `name_norm(legal_name) = '…'` | 🛑 no | 34/34 | **3,330,881** | 73.12 MB | 1 |
| **P1** raw id | `lei = '549300MIDJNNTH068E74'` | ✅ `@lei_idx(BTree)` | **1/34** | **1** | 5.14 KB | 1 |

The id and the name return the **identical 1 row** (CRH plc); the indexed id reads 1 row /
5.14 KB, the unindexed name reads 3,330,881 rows / 73.12 MB — a **3,330,881× rows-scanned**
(≈**14,225× bytes**) penalty purely from the absence of a name index. A vs B differ by
**0 rows** (both full scans).

### 3.2 Positive control — the macro DOES break a *trained* index (`gleif_l1_entities.lei` BTree)

`lei` is the one trained scalar index on L1, so it supplies the clean index→no-index
contrast on a single column (LEIs are alnum-uppercase, so `name_norm(lei)` ≡ `lei` and the
result set is unchanged — isolating the *index suppression*, not a value change):

| Probe | Predicate | Plan | Index used | Frags | `rows_scanned` |
|---|---|---|---|--:|--:|
| raw | `lei = '549300MIDJNNTH068E74'` | `ScalarIndexQuery=[lei=…]@lei_idx(BTree)`, `refine_filter=--` | ✅ **yes** | 1/34 | **1** (matched) |
| macro | `name_norm(lei) = '549300MIDJNNTH068E74'` | `LanceRead num_fragments=34`, `refine_filter=<macro>`, no index node | 🛑 **no** | 34/34 | **3,330,881** (full) |

Identical column, identical trained BTree, identical 1-row result: the raw predicate resolves
through `ScalarIndexQuery`; wrapping it in `name_norm()` discards the index and forces a full
3,330,881-row scan — **3,330,881× more rows.** This is the empirical proof the directive asked
for that read-time normalization macros blind the planner. (Unlike FEC, where the dead
`employer` BTREE confounded this contrast, GLEIF's `lei` index is live — so the macro's
index-suppression is shown cleanly.)

### 3.3 Geo controls — `legal_address_region` / `legal_address_country` (unindexed)

| Probe | Predicate | `ScalarIndexQuery`? | `rows_scanned` | Returned | Wall |
|---|---|---|--:|--:|--:|
| region | `legal_address_region = 'US-MA'` | 🛑 no | **3,330,881** | 13,742 | 0.99 s |
| country | `legal_address_country = 'US'` | 🛑 no | **3,330,881** | 352,637 | 1.59 s |

Both geo keys full-scan all 34 fragments — no index node. (Their cardinality is low —
country especially — so if filtered *alone* they are **Bitmap** candidates; but in a bridge
they are post-name-block *tiebreakers*, where the correct shape is a BTREE on the materialized
name key + a cheap refine on region/country, not a standalone geo index.)

### 3.4 L2 relationship spine — both BTREEs used (parent + reverse traversal)

| Probe | Predicate | `ScalarIndexQuery`? | Frags | `rows_scanned` | Returned |
|---|---|---|--:|--:|--:|
| child anchor | `lei = '001GPB6A9XPE8XJICC14'` | ✅ `@lei_idx(BTree)` | 1/5 | **2** | 2 |
| reverse trav. | `parent_lei = '5493001Z012YSB2A0K51'` | ✅ `@parent_lei_idx(BTree)` | — | **803** | 803 |

"children-of(LEI)" (`parent_lei` lookup) is as instant as "parent-of(LEI)" (`lei` lookup) —
both ride a trained BTree. Ultimate-parent resolution rides the same `lei` index plus a
`relationship_type='IS_ULTIMATELY_CONSOLIDATED_BY'` refine over the ≤handful of matched edges.

---

## 4. Structural verdict

| Question (directive) | Answer (measured) |
|---|---|
| Does GLEIF suffer FEC's commit-order / dead-BTREE failure? | **No.** All 3 indices `indexed==total, unindexed==0`. Overwrite-then-`replace=True`-index lifecycle (§2) makes it structurally impossible; the non-fatal-index-failure risk did not fire (proven, not assumed). |
| Are the high-cardinality keys trained? | **The indexed ones, yes (100%):** `lei` (L1 + L2), `parent_lei` (L2). **But the high-card *name* key and the geo keys are not indexed at all** — `legal_name` (≈3.33 M near-distinct), `legal_address_region`, `legal_address_country` carry **no index**. "Trained" is N/A — there is nothing to train. This is the **MSHA gap, not the FEC trap.** |
| Is `Ultimate_Parent_LEI` materialized? | **No.** No flattened column; encoded as `relationship_type='IS_ULTIMATELY_CONSOLIDATED_BY'` (129,980 edges) on L2, traversable via the `lei` BTree. |
| Exact rows scanned, Test A (raw name)? | **3,330,881** (full L1) → 1 returned, 73.12 MB. |
| Exact rows scanned, Test B (macro name)? | **3,330,881** → 1 returned, 73.12 MB. |
| Differential A vs B (rows)? | **0** — both full scans (no name index to bypass either way). I/O-bound on the 73 MB column read; regex CPU hidden at this scale (unlike FEC's 3× at 283 M). |
| Does the macro bypass an index / force a full scan? | **Yes — structurally.** `func(col)=lit` is non-indexable; proven on the trained `lei` BTree (raw → `ScalarIndexQuery` @1 row; macro → full scan @3,330,881). |
| Differential, indexed key vs unindexed key (same result set)? | **3,330,881×** rows (`lei` 1 vs `legal_name` 3,330,881), ≈**14,225×** bytes (5.14 KB vs 73.12 MB). |

### 4.1 Architectural remediation

GLEIF does **not** need FEC's fix. There is no dead index to retrain —
`optimize_indices()` / `replace=True` rebuilds are unnecessary; the spine is healthy. The
remediation is **additive coverage for the unindexed resolution keys**, gated by use case:

1. **Sub-second multinational name bridging — materialize `legal_name_norm` + BTREE; never
   call the macro in `WHERE`.** Lance binds scalar indices to *columns*, not *expressions*;
   `name_norm(legal_name)=lit` is non-indexable (§3.2). To bridge a domestic operator (e.g.
   an MSHA controller) to its global LEI parent in sub-second time, extend
   `pipelines/gleif/ingest.py::_extract_l1` / `_l1_schema` to compute a persisted
   `legal_name_norm` column **once at write time** via `core.name_norm.name_norm`, add it to
   `DATASETS["l1"]["btree"]`, and issue:

   ```sql
   WHERE legal_name_norm = 'CRH PUBLIC LIMITED COMPANY'  -- binds the BTREE → ScalarIndexQuery
   --  NOT  WHERE name_norm(legal_name) = '…'            -- func(col) → 3.33 M-row full scan
   ```

   This is the shipped credit-spine pattern (`pipelines/resolution/credit_spine_normalize_index.py`
   → `normalized_legal_name` + BTREE). Cardinality (~3.33 M near-distinct) **mandates BTREE,
   never BITMAP.** It converts the 3,330,881× full-scan penalty above into a `ScalarIndexQuery`
   point read. **This is the Directive-29 override the directive references** — authorize a
   `legal_name_norm` column on the GLEIF L1 set.

2. **Geo tiebreakers — BTREE the same materialized name key; refine on region/country.** The
   geo columns are low/mid cardinality (country `US`=352,637; region `US-MA`=13,742), so the
   bridge blocks on `legal_name_norm` (BTREE → ScalarIndexQuery) and applies
   `legal_address_region` / `legal_address_country` as a cheap post-index refine over the
   handful of name-matched rows — no standalone geo index needed. (Add **Bitmap** indexes on
   country/region only if standalone analytical geo filtering becomes a hot path.) Note the
   ZIP tiebreaker remains **blocked** until L1 is re-ingested wider — the active projection
   drops `postalCode` (`GLEIF_MSHA_ENTITY_BRIDGE_DIAGNOSTIC.md` §0).

3. **Ultimate-parent bridging — already indexed; optionally flatten for single-hop joins.**
   `WHERE lei=? AND relationship_type='IS_ULTIMATELY_CONSOLIDATED_BY'` rides the trained `lei`
   BTree today (sub-second). If single-hop `entity → ultimate_parent` joins become hot,
   materialize a flattened `ultimate_parent_lei` column on L1 (computed from the L2 ultimate
   edges) + BTREE — additive, not corrective.

The normalization macro belongs at **WRITE** time (materialize the key), never at **READ**
time (in the predicate) — and the LEIs that anchor the GLEIF universe (`lei`, `parent_lei`)
are already correctly indexed and trained.

---

## 5. Reproduction (read-only)

```
# pylance 7.0.0 / duckdb 1.5.3 / pyarrow 24 / boto3; R2 creds via Doppler core-x/prd
REPO_ROOT="$PWD" doppler run --project core-x --config prd -- python /tmp/gleif_probe.py
REPO_ROOT="$PWD" doppler run --project core-x --config prd -- python /tmp/gleif_poscontrol.py
```

The probe calls only `lance.dataset()`, `list_indices()`, `stats.index_stats()`,
`scanner().explain_plan()/analyze_plan()/count_rows()`, `pyarrow.compute` (cardinality), and
a lazy DuckDB `EXPLAIN` over the Arrow stream. The macro is imported verbatim from
`core.name_norm.name_norm`. **Zero mutation:** no `write_dataset`, no `create_scalar_index`,
no `delete`, no `optimize_indices`.
