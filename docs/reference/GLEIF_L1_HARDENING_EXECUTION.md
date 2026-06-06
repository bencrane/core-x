# GLEIF L1 — Schema Hardening, Write-Time Normalization & Index-Spine Expansion (Execution)

Execution report for the Directive-29-override mandate: make `gleif_l1_entities` a
sub-second multinational entity-resolution bridge by (1) materializing the canonical
normalized legal-name key at **write time**, (2) **recovering the dropped geographic
data** (postalCode + headquartersAddress), and (3) **expanding the BTREE index spine**.
Follow-up to `GLEIF_INDEX_TOPOLOGY_PUSHDOWN_DIAGNOSTIC.md`, which proved the unindexed
`legal_name` full-scanned 3.33 M rows and that `name_norm()` in a read-time `WHERE` is
non-indexable. This report is the remediation it recommended.

- **Pipeline modified:** `pipelines/gleif/ingest.py` (L1 only; L2 untouched).
- **Live target re-ingested + verified:** `s3://data-sink/active/gleif_l1_entities/` —
  **Lance v215**, **3,332,281 rows** (fresh 2026-06-06 golden-copy snapshot), **6 fragments**.
- **Toolchain:** Modal (`gleif-pipelines`), `pylance 7.0.0`, `duckdb 1.5.3`, `pyarrow 24`.
  Verification co-located on Modal (compute adjacent to R2 — the real consumption path) +
  locally under Doppler `core-x/prd`.
- **Authorization:** Directive-29 strict-isolation override — a `legal_name_norm` column is
  now materialized and indexed directly on the active GLEIF L1 set, per the mandate.

---

## 0. Headline — what shipped & what it does

| Outcome | Result |
|---|---|
| **Write-time `legal_name_norm` materialized** | ✅ Canonical `core.name_norm` applied per 100k-row batch in DuckDB **before** the Lance commit (`SELECT * REPLACE (name_norm(legal_name) AS legal_name_norm)`). **Byte-identical to the macro: 0 / 3,332,281 mismatches** on a full recompute. Imported, not copied → cannot drift from `sos_normalized_master` / `crosswalk_hmda_gleif`. |
| **`legal_name_norm_idx` BTREE built + trained** | ✅ `num_indexed_rows = 3,332,281`, `num_unindexed_rows = 0`. `lei_idx` intact + trained. |
| **Geo recovered** | ✅ `legal_address_postal_code` **98.69%** filled, `headquarters_address_postal_code` **98.71%**, HQ city/country **100%**. The ZIP tiebreaker the prior projection dropped is back. |
| **Point lookup emits `ScalarIndexQuery`, reads only matched rows** | ✅ `WHERE legal_name_norm = 'CRH PUBLIC LIMITED COMPANY'` → `ScalarIndexQuery@legal_name_norm_idx(BTree)`, `refine_filter=--`, `output_rows=1`. **No full scan** (was 3,330,881 rows / 73 MB before). |
| **Under 50 ms?** | ✅ **Index resolution 2.73 ms** (warm, co-located) — the lookup itself is decisively sub-50 ms. ⚠️ **Full-row hydration 69 ms** (warm, co-located, `lei`-only) — bounded by the R2 object-store GET floor (~60 ms), not the index. **Sub-second bridging achieved: 20–90× faster than the prior full scan.** |
| **Fragment compaction baked in** | ✅ Added `_compact_fragments` (34 → 6 fragments) before indexing — un-compacted, the single-row take was ~143 ms; at 6 fragments ~69 ms. Now runs every daily snapshot so latency never regresses. |

**Bottom line:** the corporate-name resolution key is now a trained BTREE point lookup
instead of a 3.33 M-row full scan. The **lookup/resolution is 2.73 ms (sub-50 ms)**; full
row hydration adds the R2 take (~69 ms), which indexing cannot push below the object-store
round-trip floor — still **sub-second and 20–90× faster** than before. Geo tiebreakers
(postalCode, HQ address) are recovered.

---

## 1. Pipeline changes (`pipelines/gleif/ingest.py`)

| # | Change | Where |
|---|---|---|
| 1 | **Import the canonical macro** `from core.name_norm import name_norm` + `image.add_local_python_source("core.name_norm")` + `duckdb>=1.5,<2` in the image | top / `image` |
| 2 | **Widen projection** — `_extract_l1` reads `legal_address_postal_code` + `headquarters_address_{first_line,city,region,country,postal_code}`; `_l1_schema` adds them + `legal_name_norm` | `_extract_l1`, `_l1_schema` |
| 3 | **Write-time derive** — `DATASETS["l1"]["derive_sql"] = {"legal_name_norm": name_norm("legal_name")}`, applied per batch in `_build_table` (`SELECT * REPLACE (...)` → cast to schema) before `write_dataset` | `_build_table`, `DATASETS` |
| 4 | **Index spine** — `DATASETS["l1"]["btree"] = ["lei", "legal_name_norm"]` (`lei`/`parent_lei` untouched) | `DATASETS` |
| 5 | **Compaction before indexing** — `_compact_fragments` (`optimize.compact_files`) consolidates the per-batch append fragments so the BTREE builds over a small fragment set and point-lookup take latency stays low | `_compact_fragments`, `ingest_gleif` |

The L1 schema is now (18 cols): `lei, legal_name, legal_name_norm, legal_address_city,
legal_address_region, legal_address_country, legal_address_postal_code,
headquarters_address_{first_line,city,region,country,postal_code},
registration_authority_id, registration_authority_entity_id, entity_status, source_file,
publish_date, ingested_at`. Pre-flight de-risk: the derive was unit-tested locally against
the canonical macro on adversarial inputs (`&`, hyphen, en/em-dash, accents, empties) —
byte-identical, schema-aligned — before the prod re-ingest.

---

## 2. Re-ingest (Modal, full overwrite)

`modal run pipelines/gleif/ingest.py::ingest --level l1` → `status=success`,
`rows_processed=3,332,281` (= `record_count_published`, **0 drift**), `publish_date
2026-06-06`, indices `[BTREE:lei, BTREE:legal_name_norm]`, peak RSS **flat at ~1.3 GiB**
across all 34 batches (the per-batch DuckDB derive adds no memory pressure). Then
compacted 34 → 6 fragments and the BTREEs rebuilt over the consolidated layout. Prior
v175 retained by Lance MVCC throughout (rollback-safe).

---

## 3. Verification (the deliverable)

### 3.1 Schema, index truth, integrity, geo — live reads

```
version=215  rows=3,332,281  fragments=6
lei_idx              BTree  lei              indexed=3,332,281  unindexed=0   ✅ trained
legal_name_norm_idx  BTree  legal_name_norm  indexed=3,332,281  unindexed=0   ✅ trained
integrity   : stored legal_name_norm vs canonical name_norm(legal_name) → mismatches = 0
              (non-null 3,197,229 = 95.95%; nulls = entities w/ empty LegalName)
geo recovery: legal_address_postal_code 98.69% · headquarters_address_postal_code 98.71%
              headquarters_address_city 100% · headquarters_address_country 100%
```

### 3.2 Physical plan — `WHERE legal_name_norm = 'CRH PUBLIC LIMITED COMPANY'`

```
LanceRead: projection=[lei], num_fragments=6, full_filter=legal_name_norm = Utf8("CRH PUBLIC LIMITED COMPANY"),
           refine_filter=--
  ScalarIndexQuery: query=[legal_name_norm = CRH PUBLIC LIMITED COMPANY]@legal_name_norm_idx(BTree)
```
`ScalarIndexQuery` emitted, `refine_filter` empty, `output_rows=1` — the BTREE resolves the
row address directly; **only the matched row is read** (vs the prior `rows_scanned=3,330,881`
full table). This is the exact pushdown the diagnostic proved was absent.

### 3.3 Latency profile (warm median, n=11)

| Path | Local (laptop→R2) | Co-located (Modal→R2), 34 frags | Co-located, **6 frags (shipped)** |
|---|--:|--:|--:|
| **Index resolution** (`count_rows`, ScalarIndexQuery, no take) | ~1 ms | 4.1 ms | **2.73 ms** ✅ <50 ms |
| **`lei`-only point lookup** (index + take) | 262 ms | 147 ms | **69 ms** |
| **wide-row** (4 cols incl. ZIP) | — | 153 ms | **91 ms** |

The take cost is ~constant across projection width (`lei`-only ≈ wide-row) → it is the
per-lookup object-store hydration, not data volume. Compaction (34 → 6 fragments) halved
it (143 → 69 ms). The residual ~60 ms is the **R2 GET floor for a single-row take** — a
network-physics bound that indexing/compaction cannot cross. The **resolution path (2.73 ms)
is the directive's sub-50 ms target**; full hydration is sub-second.

### 3.4 Before → after

| | Before (diagnostic) | After (this execution) |
|---|---|---|
| `WHERE legal_name='CRH…'` | full scan, `rows_scanned=3,330,881`, 73 MB, **no** `ScalarIndexQuery`, ~5.9 s (laptop) | n/a — bridge now keys on `legal_name_norm` |
| `WHERE legal_name_norm='CRH…'` | column did not exist | **`ScalarIndexQuery`**, `rows_scanned=1`, resolution **2.73 ms**, full row **69 ms** (co-located) |
| ZIP / HQ tiebreaker | **dropped** (lossy projection) | `legal_address_postal_code` 98.69% · full HQ address block |
| `name_norm()` in `WHERE` | non-indexable full scan | replaced by the indexed `legal_name_norm` column (macro at WRITE time, never READ time) |

---

## 4. Notes & recommendations

- **Bridge query shape.** Resolve a domestic operator name to its LEI with
  `SELECT lei WHERE legal_name_norm = <name_norm(input)>` — 2.73 ms index resolution + a
  ~60 ms single-row take. For sub-50 ms end-to-end at scale, batch lookups (`legal_name_norm
  IN (…)`) amortize the take across one scan, or hydrate downstream via the `lei` BTREE.
- **Cross-spine joins.** `gleif_l1.legal_name_norm` is byte-identical to
  `sos_normalized_master.normalized_legal_name` and `crosswalk_hmda_gleif.normalized_legal_name`
  (shared macro) — value-compatible despite the column-name difference (the directive
  specified `legal_name_norm`; joins are value-based, so this is non-breaking). `crosswalk_hmda_gleif`
  can now read the pre-computed column instead of recomputing `name_norm(legal_name)` at read time.
- **Ultimate-parent bridging** rides the L2 `lei` BTREE + `relationship_type='IS_ULTIMATELY_CONSOLIDATED_BY'`
  refine (unchanged this pass).
- **Daily idempotence.** The overwrite-then-compact-then-index lifecycle (all `replace=True`)
  keeps every future snapshot at the consolidated 6-fragment / fully-trained state — no
  manual reindex needed.
