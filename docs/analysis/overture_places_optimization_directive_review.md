# Overture Places Optimization Directive — Adversarial Review

**Reviewer stance:** adversarial. Default: the plan is flawed until proven otherwise. Every verdict below carries reproducible, executable evidence against the installed stack (pylance 7.0.0, duckdb 1.5.3, pyarrow 24.0.0) and the live SoR (read-only).
**Date:** 2026-06-06 · **Scope:** `docs/analysis/overture_places_optimization_directive.md` (THE PLAN), `docs/analysis/overture_places_structural_diagnostic.md`, `pipelines/overture_maps/places.py`.
**SoR mutation:** none. All R2 access was read-only. No Modal worker run.

---

## v1.1 Re-Verification Addendum (2026-06-06) — Original BLOCK is **CLEARED**

The directive was revised to v1.1 to remediate the BLOCKER/MAJOR findings. Re-verified the five remediation-checklist items (1–5) adversarially against the on-disk v1.1 doc, same harness/stack. **All five PASS.** One **minor prose regression** found (non-blocking). Overall: the original **BLOCK is CLEARED**; the directive is now **SHIP WITH ONE MINOR DOC FIX** (Phase-3 line + the already-known deferred minors F6/F8/F9/F10).

| Item | v1.0 sev | v1.1 status | One-line evidence |
|---|---|---|---|
| **1 / F1** bbox recipe | BLOCKER | **PASS** | §9.3 is now the exact lon/lat predicate; no corner-derived `hilbert BETWEEN`. Live western slice (lon[-115,-100]): lon/lat predicate == brute force (42,550==42,550, id-set equal), 5/9 frags pruned; rejected corner-range lost rows. §4 + §3 impact table no longer present the corner-range as valid. |
| **2 / F5** acceptance criterion | MINOR (gate) | **PASS** | §7 now asserts bbox "identical to a brute-force lon/lat scan (count and id-set equality, not just non-empty)"; `hilbert BETWEEN` scoped as "cell-range lookup, *not* a bbox." |
| **3 / F2** streaming metadata | MAJOR | **PASS** | Embedded `optimize.py` stream path = `write_dataset(rdr, schema=rdr.schema, …)` then `update_schema_metadata(dict(metadata))`. Verified: all 7 metadata keys persist **through the index build** and the `_verify_local` gate passes (old `schema=`-kwarg path persisted `[]`). `places.py` 5.3d carries the identical fix. |
| **4 / F3** goal↔design | MAJOR | **PASS** | §1.2/§3/D3 reconciled: the win is attributed to *dropping the lon/lat BTREEs* (removing the 38.9 s AND-intersect), not the sort; "bbox correctness is independent of the sort" stated explicitly; zero "kill the bbox" claims; D3 carries the measured 2/16 vs 10/16 trade-off. |
| **5 / F4** idempotency | MAJOR | **PASS** | `AlreadyV2` class + guard checks **columns** (`hilbert` present, `country`/`release_tag` absent) **before** any read/transform; raises → handled into `{"mode":"noop","already_v2":True,"mutated":False}`. Verified: fires on a v2-shaped dataset (clean no-op), silent on v1 (proceeds). Replaces the v1.0 `ValueError: No field named country` crash. |

**Regression found (1, minor):**
- **§6 Phase 3, line 751** still reads *"a bbox-via-hilbert-range returns in well under a second."* This is a v1.0 leftover that contradicts the corrected §9.3 (do **not** use a hilbert range for bbox) and §7 (use the lon/lat predicate). It instructs the executor to validate via the recipe the directive now forbids. **Not** code-breaking and **not** gate-breaking (the §7 acceptance criterion is authoritative and correct), but it should be fixed for internal consistency. **Fix:** change to *"…and a bbox via the §9.3 lon/lat predicate returns rows identical to a brute-force scan and prunes fragments."*

**No other regressions:** both embedded files `py_compile` clean; markdown fences balanced (20 markers, even); the materialize happy-path still passes the full gate on real data; `update_schema_metadata` emits 0 deprecation warnings (F7 folded in). The deferred minors (F6 category NDV 2,019 vs cited 1,574; F8 weak post-publish `region='CA'>0`; F9 `/tmp` vs `/mnt/nvme`; F10 Modal-import deploy confirmation) are unchanged and correctly acknowledged in the v1.1 revision banner.

**Re-verification commands (this round):** `reverify_f1.py`, `reverify_f1_exact.py` (live, read-only — F1), `reverify_f2.py` (F2 stream metadata), `reverify_f4.py` (F4 guard), `ast_check.py` (AST wiring of `optimize.py`), `reverify_regression.py` (live materialize gate + deprecation sweep), all under `/tmp/ovt_review/` on `/tmp/overture_diag/venv/bin/python`.

---

## Headline Verdict (v1.0, superseded by the addendum above): **BLOCK**

The structural decisions (Hilbert key, constant demotion, region normalization, confidence recast, type gate, prefix-collision safety) are largely sound and independently verified. But the directive ships **one mathematically wrong consumer recipe**, **one non-functional write path that the gate will reject**, and **one internal goal↔design contradiction** — each load-bearing. Two of these (the bbox recipe and the streaming-metadata loss) are *verified broken*, not hypothetical. Ship is blocked until the three BLOCKER/MAJOR items are remediated. None of the defects can corrupt the SoR (the gate is fail-safe), so the risk is "publishes a dataset that silently returns wrong query results" + "fallback path aborts the migration," not "destroys data."

---

## Severity-Rated Findings

| # | Severity | Status | Finding | Item |
|---|---|---|---|---|
| F1 | **BLOCKER** | verified broken | §9 bbox recipe (`hilbert BETWEEN corner_min AND corner_max` + lon/lat residual) drops 0.5–27% of in-bbox rows as **false negatives**. A residual filter cannot recover them. | 1 |
| F2 | **MAJOR** | verified broken | Streaming-fallback write path **silently loses all schema metadata** (`schema=` kwarg ignored for a `RecordBatchReader` source). `_verify_local` then **always fails** on the stream path → migration aborts. Same flaw in the `places.py` edit (5.3d). | 4 |
| F3 | **MAJOR** | verified inconsistency | Goal↔design contradiction: headline goal is killing the 38.9 s **bbox**, but D3 sorts **region-primary**, which scatters a multi-state bbox across more fragments than hilbert-primary (10/16 vs 7/16 unprunable, measured). The §3 "bbox → sub-second" projection is not delivered by the chosen sort. | 6 |
| F4 | **MAJOR** | verified risk | No idempotency guard. Re-running `dryrun`/`apply` against an already-v2 SoR raises `ValueError: No field named country` inside `_transform_and_build`. Fail-safe (pre-mutation) but contradicts the docstring's "Idempotent" claim and the §2/§7 retry-after-publish-failure narrative. | 2 |
| F5 | MINOR | verified | §7 acceptance criterion tests the **wrong recipe**: it asserts `hilbert BETWEEN … uses ScalarIndexQuery@hilbert_idx`, locking in the broken F1 path as a success gate. | 1, 7 |
| F6 | MINOR | verified | Stale cardinality: both docs cite `category` NDV **1,574** (HLL); exact live value is **2,019** (+28%). Decision (BITMAP) still correct; the number is wrong. | 10 |
| F7 | MINOR | verified | `replace_schema_metadata` (used in the materialize path on `pa.Table` — OK there) is **deprecated** on `lance.Dataset` in pylance 7.0 (`update_schema_metadata(metadata, replace=True)`). Relevant to the F2 fix. | 7 |
| F8 | MINOR | verified | Post-publish verify is weak: `region='CA' > 0` only. A correct migration merges all CA-variants to exactly **1,692,826**; the gate would pass even on a badly under-counted republish. | 9 |
| F9 | NIT | unverified (deploy) | `/mnt/nvme` (diagnostic §5-F.3) vs `/tmp` (worker `temp_directory`/`SCRATCH_DIR`) inconsistency. Cosmetic *iff* Modal's `ephemeral_disk` backs `/tmp` — confirm at deploy. | 8 |
| F10 | NIT | unverified (deploy) | Modal API surface (`add_local_python_source`, `Secret.from_name`, cross-module `_transform` import in-container) cannot be tested locally; the plan's own hedge (automount / inline fallback) is the right mitigation but must be confirmed at deploy. | 7 |

---

## Methodology (what was verified, and how)

Environment confirmed:
```
$ /tmp/overture_diag/venv/bin/python -c "import lance,duckdb,pyarrow; print(lance.__version__, duckdb.__version__, pyarrow.__version__)"
7.0.0 1.5.3 24.0.0
```
Live SoR read-only confirmation:
```
$ doppler run -- python test_live_claims.py
row_count: 16273123 (claim 16,273,123)   # EXACT
schema fields: 13 cols (id..ingested_at)  # matches diagnostic
indices: id_idx BTree / longitude_idx BTree / latitude_idx BTree / name_idx BTree
         / postcode_idx BTree / locality_idx BTree / region_idx Bitmap   # 6 BTREE + 1 BITMAP, matches
```
All throwaway Lance datasets were built under `/tmp/ovt_review/`. The embedded `_transform.py` / `optimize.py` in the directive were diffed against `/tmp/overture_diag/` copies and found byte-identical (only markdown fences differ), so tests against the `/tmp` copies are valid for the directive.

---

## Detailed Findings

### F1 — BLOCKER — §9 bbox recipe is mathematically wrong (drops valid rows)

**Claim under test (§9.3, §4 verified-primitives, §7 acceptance):** translate a bbox to `WHERE hilbert BETWEEN :h_lo AND :h_hi AND <lon/lat residual>`, where `h_lo/h_hi = min/max ST_Hilbert over the bbox corners (+ edge samples)`. The doc asserts "the Hilbert range is a superset; the refine guarantees exactness."

**Why it is wrong (first principles):** a space-filling curve enters and exits any axis-aligned bbox region *many* times. The Hilbert index of the in-bbox points is **not** a single contiguous interval, and its extrema do **not** occur at the four corners — they occur at interior/edge points. So `[corner_min, corner_max]` is *not* a superset of the in-bbox Hilbert values. A residual lon/lat filter only removes false positives; it can never recover a row the `BETWEEN` already excluded. **False negatives are unrecoverable.**

**Evidence — superset test over a dense interior grid (the diagnostic's own bbox):**
```
$ python test_hilbert_bbox.py
CORNER-only hilbert range: [1254358960, 1258050815]
CORNER+EDGE hilbert range: [1254271588, 1258159851]
GROUND TRUTH over 40401 interior grid points:
  TRUE  hilbert range: [1254271535, 1258159851]
  interior points OUTSIDE corner-range: 2354  (5.83%)  <-- FALSE NEGATIVES
  interior points OUTSIDE corner+edge : 1  (0.00%)      <-- still 1 (true min < edge min)
  corner range covers true range? False
  edge   range covers true range? False
```

**Evidence — false-negative survey across 6 representative bboxes:**
```
$ python test_hilbert_bbox2.py
WY-ish [-111,-104]x[41,45]   FN= 2354 ( 5.83%)
CA  [-124,-114]x[32,42]      FN=  194 ( 0.48%)
TX  [-106,-93]x[26,36]       FN=  648 ( 1.60%)
NYC [-74.3,-73.7]x[40.5,40.9] FN= 6861 (16.98%)
small [-104.9,-104.7]x[41.1,41.2] FN= 2044 ( 5.06%)
multistate [-120,-100]x[35,45] FN=10923 (27.04%)
bboxes with >0 false negatives: 6 / 6
```

**Evidence — end-to-end on a real Lance dataset (synthetic US points, sorted by hilbert, BTREE built):**
```
$ python test_correct_recipe.py
ground-truth in-bbox rows (exact):              4019
Recipe A  hilbert BETWEEN + lon/lat residual:   3801   (MISSING 218 = 5.42%)
Recipe B  lon/lat predicate only (no hilbert):  4019   (MISSING 0)
```

**Remediation (concrete).** Two correct options; ship **Option 1** as the default consumer recipe and reserve Option 2 for hot paths:

- **Option 1 — drop the `hilbert BETWEEN`, filter on lon/lat directly.** Proven exact, and it still prunes fragments via the sort's zone-maps:
  ```
  $ python test_fix_recipe.py
  truth=9631 OPTION1(lon/lat only)=9631 exact=True
  fragments touched by bbox on hilbert-sorted data: 1/10
  ```
  Replace §9.3 with:
  ```sql
  -- bbox: lon/lat predicate is EXACT; fragment zone-maps prune .lance files.
  SELECT * FROM places
  WHERE longitude BETWEEN :min_lon AND :max_lon
    AND latitude  BETWEEN :min_lat AND :max_lat;
  ```
  (Note: with the lon/lat BTREEs dropped per D4, this is a `LanceRead` + `refine_filter`, pruned at the fragment level by zone-maps — verified plan in `test_plans.py`.)

- **Option 2 — index-assisted, still exact.** Tile the bbox into the *set* of contiguous Hilbert intervals that cover it (compute per-row decomposition of the bbox in Hilbert space, merge adjacent cells into runs), then `hilbert IN (range1) OR hilbert IN (range2) … AND <lon/lat residual>`. This is a true superset (per-cell, not per-corner) and uses the BTREE. More code; only worth it if Option 1's fragment pruning proves insufficient. **Do not** ship a single corner-derived range.

**Also fix §4 and §7:** §4's "verified primitives" lists the corner-range behavior as validated — it is not. And §7's acceptance criterion (`hilbert BETWEEN … uses ScalarIndexQuery`) must be removed/replaced (see F5).

---

### F2 — MAJOR — streaming fallback silently loses schema metadata → gate always fails

**Claim under test (§5.2 stream path; §5.3d for `places.py`):**
```python
rdr = con.execute(sql).to_arrow_reader(STREAM_BATCH_ROWS)
schema = rdr.schema.with_metadata({...metadata...})
lance.write_dataset(rdr, LOCAL_OUT, schema=schema, mode="overwrite", ...)
```
The intent: persist provenance metadata even on the OOM fallback path.

**What actually happens:** when the source is a `RecordBatchReader`, Lance takes the data schema from the **reader** (whose `.schema.metadata is None`), and the separately-passed `schema=` kwarg's metadata is dropped. The written dataset has **empty** schema metadata, so `_verify_local`'s `expect_meta.issubset(set(meta))` check fails and the whole migration raises `LOCAL VERIFY FAILED`.

**Evidence:**
```
$ python test_stream_meta.py
reader.schema.metadata BEFORE with_metadata: None
schema(with_metadata).metadata keys: ['country','hilbert_bounds','ingested_at','release_tag',
                                       'schema_version','snapshot_date','sort_order']
PERSISTED ds.schema.metadata: {}
expect_meta subset of persisted? False
VERDICT: stream path LOSES metadata -> _verify_local gate FAILS on stream fallback
```
Contrast — the **materialize** path (which uses `pa.Table.replace_schema_metadata`) persists correctly (verified in `test_typegate.py` and the real-data E2E `test_e2e_gate2.py`: `metadata: ['country','hilbert_bounds','ingested_at','release_tag','schema_version','snapshot_date','sort_order']`).

**Impact:** the fallback exists precisely to survive an OOM at scale. As written, if materialize OOMs, the stream path runs, the gate rejects it, and the migration aborts (fail-safe — no SoR mutation, since this is pre-publish). The safety net is non-functional.

**Remediation (concrete) — both fixes verified working in `test_stream_meta_fix.py`:**

- **Preferred (FIX-B, version-aligned):** write the stream, then set metadata on the dataset:
  ```python
  lance.write_dataset(rdr, LOCAL_OUT, schema=rdr.schema, mode="overwrite", ...)
  ds_out = lance.dataset(LOCAL_OUT)
  ds_out.update_schema_metadata({k: v for k, v in metadata.items()})  # str:str ok
  ```
  (`update_schema_metadata` is the non-deprecated pylance-7 API — see F7. Verified persists: `['country','ingested_at','release_tag','schema_version','snapshot_date']`.)

- **Alternative (FIX-A):** rebuild the reader so its *own* schema carries the metadata, and drop the `schema=` kwarg:
  ```python
  schema_md = rdr.schema.with_metadata({k.encode(): v.encode() for k, v in metadata.items()})
  rdr2 = pa.RecordBatchReader.from_batches(schema_md, (b for b in rdr))
  lance.write_dataset(rdr2, LOCAL_OUT, mode="overwrite", ...)
  ```

Apply the same fix to the `places.py` edit (5.3d) streaming branch — it has the identical bug.

---

### F3 — MAJOR — goal↔design contradiction: region-primary sort undercuts the bbox win it claims

**Claim under test:** §1.2 / §3 headline = "physically sort by `(region, hilbert)` so fragment zone-maps prune — killing the measured 38.9 s bbox," and the §3 projected-impact table lists `bbox query: 38.9 s → sub-second`. D3 separately justifies region-**primary** because "by-state filtering is the dominant access pattern."

**The contradiction:** a region-primary sort orders fragments by USPS code *alphabetically*. Alphabetical region order ≠ geographic adjacency (CA, CO, CT are alphabetical neighbors; CA, NV, OR, WA are geographic neighbors). A multi-state bbox therefore spans many non-contiguous region blocks, defeating fragment pruning — the very thing the headline promises.

**Evidence — realistic state distributions, 16-fragment layout, west-coast multi-state bbox:**
```
$ python test_sort_coherence2.py
WEST-COAST bbox lon[-125,-114] lat[32,49] (CA/NV/OR/WA — geo-adjacent, alpha-scattered):
  region,hilbert: 10/16 frags unprunable
  hilbert       :  7/16 frags unprunable
  hilbert prunes 3 MORE fragments
by-state region='CA' equality (frags containing CA):
  region,hilbert: 2/16   |   hilbert: 5/16
```
So the trade is real and quantified: **region-primary helps by-state (2 vs 5 fragments) but hurts multi-state bbox (10 vs 7 unprunable)** — and the headline goal is the bbox.

The plan *acknowledges* this in D3's parenthetical ("If pure cross-state bbox later dominates, flip to `ORDER BY hilbert`"), but then the §3 impact table still claims bbox → sub-second from the region-primary layout. Both cannot be true.

**Remediation (pick one and make the docs consistent):**
- If by-state genuinely dominates (the D3 premise): **keep `(region, hilbert)`**, but **delete the "kill the 38.9 s bbox" framing** from §1.2/§3 and restate the headline win as "by-state pruning + a correct (Option-1) bbox path that no longer needs the two-BTREE intersect." The bbox improves (no 2-BTREE intersect; some zone-map pruning) but is *not* the optimization target.
- If the bbox is the real target: **sort `ORDER BY hilbert`** (single key), accept that by-state then touches ~5/16 fragments but is still served by the `region` BITMAP. Update D3 + the metadata `sort_order` tag.
- Either way: the §3 projected-impact table must stop claiming a sub-second bbox *from a region-primary sort* without the corrected recipe and the matching sort.

---

### F4 — MAJOR — no idempotency guard; re-run crashes on an already-v2 SoR

**Claim under test:** `optimize.py` docstring: "Idempotent (overwrite)"; §2 safety contract and §7 imply a re-run after a partial failure ("publish succeeded but ledger/verify failed and was retried") is safe.

**What actually happens:** `optimize_overture_places()` calls `_transform_and_build()` **unconditionally** (before the `if not apply` branch). That function reads provenance via `src.scanner(columns=["country","release_tag","snapshot_date","ingested_at"], limit=1)`. On a dataset that is *already* v2, those four columns no longer exist → hard raise.

**Evidence (replayed the optimize.py read steps against a synthetic already-v2 dataset):**
```
$ python test_idempotency.py
already-v2 schema: ['id','longitude','latitude','hilbert','region','locality','postcode','name','category','confidence']
-- STEP 1: read provenance columns that no longer exist --
  RAISES: ValueError :: No field named country. ... lance-datafusion/src/projection.rs:356
-- STEP 2: read SOURCE_COLUMNS --
  SOURCE_COLUMNS read OK
  projection OK, rows: 5 hilbert type: uint32 confidence type: float
```

**Severity rationale:** the crash is in the **build** phase, before `_backup_r2_prefix`/`_wipe_prefix`/`_upload_dir`, so **the SoR is never mutated** — this is fail-safe, hence MAJOR not BLOCKER. But: (a) it contradicts the "Idempotent" claim; (b) the failure mode is a cryptic Rust panic string, not an operator-legible "already v2, nothing to do"; (c) it also breaks `dryrun` on a migrated dataset (same unconditional call); (d) the one realistic retry scenario the plan names — publish succeeded, verify failed, **auto-restore also failed** (the `CRITICAL: rollback FAILED` branch) — leaves the SoR in v2 state, and a manual re-run then dies here instead of being a clean no-op.

**Remediation (concrete) — add a schema probe at the top of `_transform_and_build`:**
```python
src = lance.dataset(DATASET_URI, storage_options=so)
src_field_names = {f.name for f in src.schema}
if "hilbert" in src_field_names and not {"country", "release_tag"}.issubset(src_field_names):
    sv = (src.schema.metadata or {}).get(b"schema_version", b"").decode()
    raise SystemExit(
        f"SoR already appears to be {sv or 'v2'} (has 'hilbert', missing provenance columns). "
        "Migration is a no-op; nothing to do. Restore from a backup prefix if a re-migration is intended."
    )
```
Use a distinct, non-error exit (or a `{'mode':'noop','already_v2':True}` return) so a retry harness can treat "already migrated" as success, not failure. Drop the unqualified "Idempotent" word from the docstring; replace with "Re-run-safe (aborts pre-mutation if already v2)."

---

### F5 — MINOR — acceptance criterion enshrines the broken bbox recipe

§7 includes: *"A representative bbox (translated to a hilbert range + residual lon/lat refine — see §9) returns the correct rows in < 1 s"* and *"`hilbert BETWEEN …` uses `ScalarIndexQuery@hilbert_idx(BTree)`."* Per F1, that recipe returns *incorrect* (under-counted) rows. The criterion would pass on speed while the result is wrong, and it locks the defect in as a "success."

**Remediation:** replace with: *"A representative bbox via the §9 lon/lat predicate (Option 1) returns rows **identical to a brute-force lon/lat scan** (exactness assertion, not just non-empty) and touches < N fragments."* Keep a separate criterion that the `hilbert` BTREE pushes down for a *point/range on hilbert directly* (e.g. nearest-cell lookups), which is the legitimate use of that index.

---

### F6 — MINOR — stale `category` cardinality (1,574 vs exact 2,019) in both docs

```
$ doppler run -- python test_live2.py
ndv_region=131 (claim 131)   ndv_category=2019 (claim ~1574)
ndv_country=1 ndv_release=1 ndv_snapshot=1 ndv_ingested=1 (all claim 1)
confidence range=[0.0092, 1.0]
```
`category` exact NDV is **2,019**, not 1,574 (the HLL estimate the diagnostic carried). Cited in diagnostic §1.2/§3/§4.1 and directive D5. The BITMAP decision is unaffected (2,019 is still well inside roaring-bitmap range), but the number is wrong in four places. **Remediation:** correct to 2,019 (exact); note it remains comfortably within BITMAP range. (`region` 131 exact and all four constants cardinality-1 are confirmed correct.)

---

### F7 — MINOR — `replace_schema_metadata` deprecated on `lance.Dataset` in pylance 7.0

```
$ python test_stream_meta_fix.py
DeprecationWarning: replace_schema_metadata is deprecated. Use update_schema_metadata(metadata, replace=True) instead.
```
The materialize path uses `pa.Table.replace_schema_metadata` (a **pyarrow** method — fine, not deprecated). But the F2 fix must use the **dataset** setter; use `update_schema_metadata(...)`, not `Dataset.replace_schema_metadata(...)`, to avoid the deprecation. Noted so the F2 remediation lands on the supported API.

---

### F8 — MINOR — post-publish verify is too weak (`region='CA' > 0`)

`optimize_overture_places` post-publish check: `n_region = pub.scanner(filter="region='CA'").to_table().num_rows; ok = ... and n_region > 0`. A republish that silently dropped 90% of rows but kept one CA row would pass. The correct migration merges all CA-variants to a known exact count:
```
$ python test_postpublish.py
normalized CA aggregation: [('CA', 1692826)]   # 1,692,767 + ca28 + Calif19 + Ca10 + California2
all CA-variants merge to CA: 1692826 == 1692826? True
```
**Remediation:** strengthen to assert the *exact* merged CA count (and reuse the row-count/distinct-id assertions already in `_verify_local`):
```python
n_ca = pub.scanner(filter="region = 'CA'", columns=["id"]).to_table().num_rows
ok = (pub_rows == report["src_rows"]
      and set(OPTIMIZED_BTREE_INDEXES + OPTIMIZED_BITMAP_INDEXES).issubset(pub_idx)
      and n_ca == 1_692_826)   # pin the post-normalization CA count
```
(If pinning a magic number is undesirable, assert `n_ca` equals the value computed from the source in `_transform_and_build` and threaded through `report`.)

---

### F9 — NIT — `/mnt/nvme` (diagnostic) vs `/tmp` (worker) temp_directory

Diagnostic §5-F.3 prescribes `temp_directory='/mnt/nvme/duckdb_spill'`; both `places.py` and `optimize.py` use `/tmp/...`. On Modal, `ephemeral_disk=524288` (512 GiB) is mounted and `/tmp` typically lands on it, so this is cosmetic — **but unverified locally**. Confirmed adjacent facts: DuckDB **auto-creates** the spill subdir if the parent exists (so `temp_directory='/tmp/overture_opt/duckdb_spill'` after `os.makedirs(SCRATCH_DIR)` is fine):
```
$ python test_tempdir.py
parent exists=True spill subdir exists=False (mirrors optimize.py)
forced-spill sort completed rows: 20000000
spill subdir created by duckdb? True
```
**Remediation:** align the docs (either point the worker at the Modal ephemeral mount explicitly, or update the diagnostic to say `/tmp` on Modal). Confirm at deploy that `/tmp` is on the ephemeral disk, not the small container root.

---

### F10 — NIT — Modal-specific APIs unverifiable locally

`image.add_local_python_source("pipelines")`, `modal.Secret.from_name(...)`, and the in-container `from pipelines.overture_maps._transform import …` cannot be exercised in the venv. The directive already hedges (Modal automount default; inline `_transform` constants as fallback), which is the correct mitigation. **Remediation:** keep the hedge; at deploy, run `dryrun` first (it exercises the import + read + build + gate with **no** mutation) and confirm the import resolves before `apply`.

---

## What's Correct (verified — do NOT change)

These were independently verified with executable evidence and are sound:

1. **Hilbert key validity (D2).** `ST_Hilbert(lon::DOUBLE, lat::DOUBLE, ST_Extent(ST_MakeEnvelope(-180,-90,180,90)))` is callable on duckdb 1.5.3 and maps **all 16,273,123 live coordinates** with zero NULLs, zero overflow, max 4,286,724,467 < uint32 max (`test_hilbert_allcoords.py`). `ST_GeoHash` confirmed **absent** ("Scalar Function ... does not exist"); `ST_QuadKey` confirmed Mercator (silently returns a clamped value `111111111101` at lat −89.91 rather than erroring) — the rejection of QuadKey is justified (`test_geohash_call.py`).
2. **Type gate (§5.2 `_verify_local`) — all 10 type strings are exactly right.** Building a dataset the way `optimize.py` does yields `id:string, longitude:double, latitude:double, hilbert:uint32, region:string, locality:string, postcode:string, name:string, category:string, confidence:float` — `fields == expect_fields → True` (`test_typegate.py`). `CAST(... AS UINTEGER) → uint32`, `CAST(confidence AS FLOAT) → float32`, both accept BTREE + range pushdown (`ScalarIndexQuery` present).
3. **confidence double→float32 (D7) is lossless** at Overture precision (max Δ = 9.5e-9; live range [0.0092, 1.0]).
4. **region normalization (D9) is correct and complete for the live snapshot.** All 15 live full-name variants map correctly; all CA-variants merge to CA=1,692,826; only **29 rows** are NULLed and **every one is genuinely non-US** (Canada/Mexico/UK/Bangladesh/Australia/Spain/Seychelles/Micronesia/garbage). No legit US full-name is silently dropped (`test_region_norm.py`, `test_postpublish.py`).
5. **Materialize happy path passes the full gate on real data.** A 300k-row real sample through `projection_sql` → `replace_schema_metadata` → `write_dataset` → `_build_indexes` → `_verify_local` passes: 7 indices, schema match, metadata persisted, row-preservation + distinct-id hold (`test_e2e_gate2.py`).
6. **Backup prefix is collision-free (D-rollback).** `active/overture_places__bak_<rel>_<ts>/` does **not** start with `active/overture_places/` (discriminator `__bak` vs `/`), so `list_objects_v2(Prefix="active/overture_places/")` and `_wipe_prefix` will not touch the backup (`test_prefix.py`).
7. **pylance/duckdb/pyarrow API surface is valid** for the installed versions: `scanner(filter=,columns=,limit=)`, `to_reader()`, `to_arrow_reader(batch)`, `explain_plan(True)`, `list_indices()` (returns `list[dict]` with a `fields` key — `_index_names` works), `create_scalar_index(col,"BTREE"/"BITMAP",replace=True)`, `pa.Table.replace_schema_metadata`, `pa.Schema.with_metadata` (`test_pylance_api*.py`).
8. **`optimize.py`'s Lance storage_options keys are correct** (`aws_endpoint`/`aws_region`/`aws_virtual_hosted_style_request`) — Lance reads the live SoR with them (`test_storage_opts.py`). The `places.py` `endpoint`/`region` keys are for boto3 only and are fine.
9. **Resource budget is adequate at this scale.** v2 decoded ≈ 1.86 GB; 32 GiB container with `memory_limit=24GB` spilling to 512 GiB ephemeral disk; sequential in-memory BTREE trains (largest = `id` ≈ 0.59 GB raw). Materialize peak fits comfortably (`budget.py`).
10. **Diagnostic telemetry is accurate** where spot-checked: row count 16,273,123 (exact), region NDV 131 (exact), all 4 constants cardinality-1, index:data ratio 1.18× (1.18336, exact), the two-BTREE bbox plan and its **42,550-row** result (exact match; cost 26.5 s this run vs 38.9 s measured — same pathological order of magnitude over cold R2). `confidence` range, lon/lat outliers (lat −89.913), and the 28× DuckDB-handoff penalty structure all corroborated.

---

## Prioritized Remediation Checklist

1. **[BLOCKER] Fix the §9 bbox recipe (F1).** Replace the `hilbert BETWEEN corner_min/max` recipe with the exact lon/lat-predicate recipe (Option 1). Remove the corner-range "verified primitive" from §4. Re-word §9.3.
2. **[BLOCKER-gate] Replace the §7 bbox acceptance criterion (F5).** Assert bbox result **equals** a brute-force lon/lat scan (exactness), not "uses `hilbert BETWEEN` ScalarIndexQuery."
3. **[MAJOR] Fix the streaming-fallback metadata loss (F2)** in `optimize.py` *and* the `places.py` 5.3d edit — write then `update_schema_metadata(...)` (or rebuild the reader with a metadata-bearing schema). Verify a stream-path build passes `_verify_local`.
4. **[MAJOR] Resolve the goal↔design contradiction (F3).** Either keep `(region, hilbert)` and drop the "kill the bbox" headline, or sort `ORDER BY hilbert` and update D3 + `sort_order` metadata. Make §1/§3/§7 internally consistent; do not claim a sub-second bbox from a region-primary sort without the corrected recipe.
5. **[MAJOR] Add an idempotency guard (F4).** Probe the source schema at the top of `_transform_and_build`; treat already-v2 as a clean no-op (distinct return / non-error exit). Drop the unqualified "Idempotent" claim.
6. **[MINOR] Strengthen the post-publish verify (F8)** to assert the exact merged CA count (or the source-derived count threaded through `report`), plus the existing row/distinct-id checks.
7. **[MINOR] Correct the `category` NDV (F6)** to 2,019 (exact) in both docs; note it remains within BITMAP range.
8. **[MINOR] Use `update_schema_metadata` (F7)** in the F2 fix to avoid the pylance-7 deprecation.
9. **[NIT] Align `/mnt/nvme` vs `/tmp` (F9)** in the docs; confirm `/tmp` is on the Modal ephemeral disk at deploy.
10. **[NIT] Confirm the Modal import surface (F10)** by running `dryrun` (no mutation) before `apply`.

**Do not ship `apply` until items 1–5 are remediated and a `dryrun` (which now must also exercise the stream-path metadata fix and the idempotency guard) passes against the live SoR.**
