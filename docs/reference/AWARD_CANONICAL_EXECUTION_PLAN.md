# USASpending Award Canonical — Execution Plan

> Produced 2026-07-04 by an Opus 4.8 multi-agent workflow (3 lens-drafts → synthesize →
> adversarial critique → finalize). All row/overlap counts are LIVE-probed against
> `s3://data-sink/active/`. The adversarial pass corrected four numbers from the initial
> brief (parent_award scope footprint, fresh overlap under is_fpds scope, disk sizing,
> is_fpds NULL bucket) — see the "Ground-truth facts" table and P0/P1 confirmations.

## ⚠️ SCOPE CORRECTION (2026-07-04 — supersedes every `is_fpds` reference below)

A post-plan live probe settled the exact anomaly the critique's P1-5/P1-7 flagged. `is_fpds = TRUE`
is the **WRONG** contract-scope filter and MUST be `generated_unique_award_id LIKE 'CONT%'`. Proof:

- `is_fpds` is **NULL on 263,371 real contract awards** (CONT-prefix); it is never FALSE for a
  contract and never TRUE for a non-contract → a nullable proxy for the CONT prefix, not a scope key.
- FRESH keys found in bulk: **84,341 (85.6%) under `CONT%`** vs **15,859 (16.1%) under `is_fpds=TRUE`**.
  `is_fpds=TRUE` drops the 263,371 NULL-is_fpds contract rows, so 68,482 fresh awards whose bulk
  counterpart exists would falsely read as fresh-only tail on the flip → reconcile corruption.

**Substitute throughout:** scope = `generated_unique_award_id LIKE 'CONT%'`; spine rows **30,683,126**
(not 30,419,755); FRESH overlap **84,341 / 85.6%** (not 15,859 / 16.1%); `fresh_only_tail` **14,169**
(not 82,651). The STEP-7 "263K NULL-is_fpds exclusion" note is **VOID** — those are in-scope contracts.
All live-driven gates (floor = 0.9×live bulk scope; tail gate; parent semi-join against `bulk_latest`)
self-correct once `CONTRACT_FILTER` changes — only the documented expectations above change.

---

Every load-bearing number in the critique is confirmed live:

- **P0-1 CONFIRMED**: `parent_award` = 987,705 rows (not "98K"), only **32,341 in-scope (0.106% of the 30.4M spine, 3.3% of parent)**. Scanning/`.to_table()`ing the full ~988K dataset as a peer leg is wrong — the 10 rollup columns land NULL on 99.89% of output rows. Semi-join filter to the ~32K in-scope keys is the fix.
- **P0-2 CONFIRMED**: `parent_award_id` is NULL on 954,338/987,705 = **96.6%** of rows. The synthesized plan's `parent_award_matched = ... ON parent_award_id = contract_award_unique_key` self-join measures garbage.
- **P0-4 CONFIRMED**: shipped FPDS `.py` runs 192 GiB + 1.5 TiB `ephemeral_disk` because "512 GiB floor was exhausted by the 392-col stage-2 merge spill." The plan's ~440 cols is wider. Asserting 512 GiB suffices is the losing side.
- **P1-5 CONFIRMED**: FRESH containment is **16.1%** (15,859 overlap, 82,651 tail), not "same value space."
- **P1-7 CONFIRMED**: is_fpds NULL bucket = **263,371**, silently dropped by `is_fpds = TRUE`.
- **P1-8 CONFIRMED**: BULK is already 1:1 on gua in scope (30,419,755 rows = distinct gua, 0 null) → BULK collapse is defensive-only, `dedup_collapsed` ≈ 0 expected.

All valid critique items fold in. Below is the final plan.

---

# EXECUTION PLAN — `usaspending_award_canonical` (FINAL)

Single dependency-ordered build of a typed, `generated_unique_award_id`-grained v2.1 Lance read-model reconciling BULK `award_search` (is_fpds scope, 30,419,755 rows) ⊕ FRESH `contract_prime_award` (286 cols) ⊕ a semi-join-scoped `parent_award` enrich leg. Structural template = `usaspending_subaward_canonical.py` (declarative `COLUMN_SPEC`, flat argmax, overwrite, fail-closed gates). Execution mechanics = `usaspending_fpds_canonical.py` / `_modal.py` (single-pass reader, inlined collapse, free-as-you-go DROP, local-stage boto3 publish, `/tmp`-staged append-only index, two-source completion sentinel).

All row/overlap counts are LIVE (probed 2026-07-04, `s3://data-sink/active/`). See STEP 0.

## Ground-truth facts (locked; do not re-litigate)

| Fact | Value | Source |
|---|---|---|
| is_fpds domain | TRUE=30,419,755 · FALSE=47,953,531 · **NULL=263,371** | live |
| BULK gua in is_fpds scope | 30,419,755 rows = 30,419,755 distinct = 0 null → **already 1:1** | live |
| PK carrier | BULK `generated_unique_award_id` (no `contract_award_unique_key`); FRESH `contract_award_unique_key` (no `generated_unique_award_id`) | live |
| FRESH distinct keys | 98,510 · overlap-with-BULK **15,859 (16.1%)** · **fresh_only_tail 82,651** | live |
| parent_award | **987,705 rows = 987,705 distinct gua, 0 null** (1:1 on gua) | live |
| parent_award in is_fpds scope | **32,341 (0.106% of spine, 3.3% of parent)** | live |
| parent_award `parent_award_id` NULL | **954,338 / 987,705 = 96.6%** | live |
| no `correction_delete_ind` in `award_search` | confirmed (154 cols) | live |

## Files to create (absolute)

1. `/Users/benjamincrane/core-x/pipelines/usaspending/ops_usaspending_award_canonical_runs.sql`
2. `/Users/benjamincrane/core-x/pipelines/usaspending/usaspending_award_canonical.py` — shipped module (`COLUMN_SPEC`, generators, stages, `build`/`index`/`verify`/`init_ops`/`_record_run`/`refresh`, CLI).
3. `/Users/benjamincrane/core-x/pipelines/usaspending/usaspending_award_canonical_modal.py` — GIANT wrapper.
4. (authoring-time, NOT shipped) `…/scratchpad/align_award_canonical.py` — DEC-alignment generator emitting draft `COLUMN_SPEC`. Runs once; never imported by `build()`.
5. (follow-on, later cycle) `/Users/benjamincrane/core-x/docs/reference/AWARD_CANONICAL_FIELD_DICTIONARY.md`.

---

## STEP 0 — Pin the live facts (authoring gate, before any code)

Re-run the overlap probe (`…/scratchpad/probe_overlap.py`, already written) and record its output verbatim into the module docstring as the reconciliation-probe block (mirrors subaward L20-45). These six numbers drive floor math, the parent semi-join, and the verify gates. If any drifts materially at authoring, re-derive the floor and tail expectations. **P1-5 pre-lock check:** join the 15,859 overlapping keys on `(piid ⊕ awarding agency ⊕ action_date)` and confirm they describe the SAME award (not a coincidental `CONT_*` format collision). Record containment % + tail as measured facts in the docstring — not "same value space."

## STEP 1 — Prerequisite: FRESH leg landed + verified

`contract_prime_award` at `s3://data-sink/active/usaspending_api_fresh/contract_prime_award/` (286 cols, live-confirmed). Blocks the FRESH half only; the BULK-only first landing (STEP 12) does not depend on it. Land per `AWARD_API_PULL_HANDOFF.md` if not merged.

## STEP 2 — Lock `COLUMN_SPEC` (authoring-time DEC alignment)

Run `align_award_canonical.py` against live datasets to emit the draft spec (SCHEMA-draft two-hop: `award_search.column_name → search_schema_dictionary.dec_element → DEC.element → DEC.dl_award_element`; canonical vocabulary = FRESH PAS names aligned on `dl_award_element`). PR #952's `db_element` correction is FPDS-transaction-scoped; does NOT apply. Then a human selection pass: (i) promote keys/core out of buckets; (ii) fix DuckDB types; (iii) resolve collisions; (iv) tag parent rows `"src":"parent"`.

**Provisional until the generator runs and its output is diffed against both live schemas** (per P2-11): every bucket count (dual-bridge ~22 · BULK-unique enrich ~128 · FRESH-unique enrich ~254 · parent net-new 10 · keys ~7 · prov 2) is PROJECTED, not settled. `len(COLUMN_SPEC)` is locked to an EXACT integer at the end of this step and baked into `smoke_fn`, DDL `columns`, and verify #3 (per P2-10 — no `~440` placeholder survives into code).

**`COLUMN_SPEC` row shape** (subaward-identical + one optional field):
```python
{"canonical": <FRESH PAS name>, "duck_type": <DATE|TIMESTAMP|DOUBLE|BIGINT|BOOLEAN|VARCHAR>,
 "group": <"key"|"core"|"enrich"|"prov">,
 "bulk_expr": <award_search expr, DEC-crosswalked | None>,
 "feed_expr": <contract_prime_award expr | None>,
 "src": "parent"}   # ONLY on the 10 parent_award rows; absent elsewhere
```
Macros: `s(x)=nullif(nullif(trim(x),''),'-NONE-')`. FRESH `TRY_CAST(s(x) AS T)`. Mod-frontier `TRY_CAST(replace(s(x),'+00','') AS TIMESTAMP)` — **NO strptime**. BULK native-typed (matview) → bare column refs; FRESH all-VARCHAR → always wrapped.

**Groups:** `key` (hand-aligned synthesized keys) · `core` (DUAL — both legs non-None) · `enrich` (SINGLE-source — exactly one leg None → typed NULL on the other, OR `src="parent"` with both None) · `prov` (`canonical_source`, `built_at` — both None).

**Locked keys / core anchors:**
```python
{"canonical":"generated_unique_award_id","duck_type":"VARCHAR","group":"key",
 "bulk_expr":"s(generated_unique_award_id)","feed_expr":"s(contract_award_unique_key)"},   # THE PK
{"canonical":"contract_award_unique_key","duck_type":"VARCHAR","group":"key",
 "bulk_expr":"s(generated_unique_award_id)","feed_expr":"s(contract_award_unique_key)"},   # co-key (== PK value; cross-spine join to FPDS)
{"canonical":"last_modified_date","duck_type":"TIMESTAMP","group":"core",
 "bulk_expr":"last_modified_date","feed_expr":"TRY_CAST(replace(s(last_modified_date),'+00','') AS TIMESTAMP)"},  # argmax driver
```

**DUAL core anchors (type-agreement audited in the authoring pass — P2-12):** each DUAL core's BULK-native type MUST exactly equal the FRESH `TRY_CAST` target, or `_assert_collapse_schema_identity` hard-fails build 1.
- `total_obligation` core: BULK native `DOUBLE` `total_obligation` ⊕ FRESH `TRY_CAST(s(total_obligated_amount) AS DOUBLE)`.
- `base_and_all_options_value` core: BULK native `DOUBLE` ⊕ FRESH `TRY_CAST(s(current_total_value_of_award) AS DOUBLE)`.
- `last_modified_date`: BULK native `TIMESTAMP` ⊕ FRESH `TRY_CAST(replace(...,'+00','') AS TIMESTAMP)`.

**Sentinel clamps** (value-level, row always survives; never touch `last_modified_date`):
- amounts (both legs): `CASE WHEN abs(<expr>) <= 1e12 THEN <expr> END` — emits identical `DOUBLE` on BULK-native and FRESH-`TRY_CAST` (audited per P2-12).
- action dates: `CASE WHEN <date> BETWEEN DATE '1776-01-01' AND CURRENT_DATE THEN <date> END`.

**parent-descriptor columns (FRESH-only enrich, `bulk_expr=None`):** `parent_award_type_code`, `parent_award_type`, `parent_award_single_or_multiple_code`, `parent_award_single_or_multiple`, `parent_award_agency_id`, `parent_award_agency_name`, `multiple_or_single_award_idv_code`. `parent_award_id_piid` is DUAL (BULK `parent_award_piid` ⊕ FRESH `parent_award_id_piid`). These are DISTINCT from the `parent_award`-dataset leg below.

**parent_award-dataset leg (`src:"parent"`, both exprs None):** 10 net-new IDV aggregates — `direct_idv_count`, `direct_contract_count`, `direct_total_obligation`, `direct_base_and_all_options_value`, `direct_base_exercised_options_val`, `rollup_idv_count`, `rollup_contract_count`, `rollup_total_obligation`, `rollup_base_and_all_options_value`, `rollup_base_exercised_options_val`. `parent_award_id` FK is **NOT projected into `canonical_out`** (96.6% NULL, drives nothing downstream; per P0-2). Join key `generated_unique_award_id` only.

**Module constants** (FPDS giant path):
```python
os.environ.setdefault("LANCE_BYPASS_SPILLING", "true")   # BEFORE import lance
BULK_URI=f"{ACTIVE}/usaspending/award_search/"; FRESH_URI=f"{ACTIVE}/usaspending_api_fresh/contract_prime_award/"
PARENT_URI=f"{ACTIVE}/usaspending/parent_award/"; CANONICAL_URI=f"{ACTIVE}/usaspending_award_canonical/"
CONTRACT_FILTER="generated_unique_award_id LIKE 'CONT%'"    # BULK scope → 30,683,126 (is_fpds=TRUE was WRONG: NULL on 263,371 real contracts + craters fresh reconcile — see SCOPE CORRECTION)
DATA_STORAGE_VERSION="2.1"; MAX_ROWS_PER_FILE=1_048_576; MAX_BYTES_PER_FILE=90*1024**3
PK_COL="generated_unique_award_id"
SCRATCH=os.environ.get("AWARD_CANONICAL_SCRATCH","/tmp/award_canonical_stage")
DUCK_MEM=os.environ.get("AWARD_CANONICAL_DUCKDB_MEM","8GB")
DUCK_TMP=os.environ.get("AWARD_CANONICAL_DUCKDB_TEMP_DIR","/tmp/award_canonical_duckdb")
DUCK_THREADS=int(os.environ.get("AWARD_CANONICAL_DUCKDB_THREADS","4"))
```

## STEP 3 — Generators (all program-derived from `COLUMN_SPEC`)

Verbatim from subaward, extended for the third leg + `include_fresh` toggle:
- `_canon_order()`, `_cols(group)`, `_typed_null(c)` — unchanged.
- `_source_cols("bulk"|"feed"|"parent")` — regex-parse distinct raw cols per leg. `"parent"` case = the 10 net-new + join key `generated_unique_award_id`. Presence-filtered against `ds.schema.names` at build time.
- `_proj_select(side, built_at_iso, include_fresh)` — per-source projection in canonical order; the `include_fresh` gate lives HERE:
  ```python
  if side == "feed":
      expr = c["feed_expr"] if (include_fresh and c["feed_expr"] is not None) else _typed_null(c)
  elif side == "bulk":
      expr = c["bulk_expr"] if c["bulk_expr"] is not None else _typed_null(c)
  ```
  `prov`: `canonical_source` → typed-NULL placeholder; `built_at` → `TIMESTAMP '{built_at_iso}'`.
- `_enrich_replace_block()` — THREE-leg routing:
  ```python
  for c in _cols("enrich"):
      if c.get("src") == "parent":      leg = "p"
      elif c["bulk_expr"] is not None:  leg = "b"
      else:                             leg = "f"
      parts.append(f"    {leg}.{c['canonical']} AS {c['canonical']}")
  ```

## STEP 4 — Reconcile SQL: Stage-1 collapse (per-source argmax, projections INLINED)

BULK projection inlined into `bulk_latest` (30.4M `bulk_proj` never materialized). Each collapse is `SELECT * EXCLUDE(rn)` over a projection-shaped inner → byte-identical name/type/order → asserted by `_assert_collapse_schema_identity` before the union.

```sql
CREATE MACRO s(x) AS nullif(nullif(trim(x), ''), '-NONE-');

-- BULK collapse (inlined; ONE 30.4M scan). NOTE: BULK is already 1:1 on gua in is_fpds scope
-- (live: 30,419,755 rows = distinct gua = 0 null) → this window is DEFENSIVE-ONLY; rn>1 never fires
-- and dedup from the BULK leg is expected ≈ 0 (do not flag near-zero BULK dedup as a bug — P1-8).
CREATE TEMP TABLE bulk_latest AS
SELECT * EXCLUDE (rn) FROM (
  SELECT *, row_number() OVER (
            PARTITION BY generated_unique_award_id
            ORDER BY last_modified_date DESC NULLS LAST,
                     (recipient_uei IS NULL) ASC,
                     award_id DESC NULLS LAST) AS rn     -- stable BIGINT surrogate (award_search PK, non-null/unique in scope)
  FROM ( SELECT {bulk_proj} FROM bulk_r )
  WHERE generated_unique_award_id IS NOT NULL
) WHERE rn = 1;

-- FRESH collapse (inlined; download re-pulls duplicate keys across windows → keep latest)
CREATE TEMP TABLE fresh_latest AS
SELECT * EXCLUDE (rn) FROM (
  SELECT *, row_number() OVER (
            PARTITION BY generated_unique_award_id
            ORDER BY last_modified_date DESC NULLS LAST,
                     award_latest_action_date DESC NULLS LAST,
                     generated_unique_award_id DESC NULLS LAST) AS rn
  FROM ( SELECT {fresh_proj} FROM fresh_r )
  WHERE generated_unique_award_id IS NOT NULL
) WHERE rn = 1;

-- parent_award leg — SEMI-JOIN SCOPED to the contract spine (P0-1). Materialize ONLY the ~32,341
-- in-scope rows, NOT the full 987,705-row dataset (96.7% is grant/assistance IDV rollups that never
-- join). parent_award is already 1:1 on gua (live) → no collapse; the WHERE prunes the dead 96.7%.
CREATE TEMP TABLE parent_latest AS
SELECT {parent_proj} FROM parent_r
WHERE generated_unique_award_id IS NOT NULL
  AND generated_unique_award_id IN (SELECT generated_unique_award_id FROM bulk_latest);

-- early metric captures (1-row each)
CREATE TEMP TABLE m_rows_in_bulk   AS SELECT count(*) AS c FROM bulk_latest;
CREATE TEMP TABLE m_rows_in_fresh  AS SELECT count(*) AS c FROM fresh_latest;
CREATE TEMP TABLE m_rows_in_parent AS SELECT count(*) AS c FROM parent_latest;   -- expected ≈ 32,341 (post-filter)
CREATE TEMP TABLE m_fresh_only_tail AS
  SELECT count(*) AS c FROM fresh_latest f ANTI JOIN bulk_latest b
    ON f.generated_unique_award_id = b.generated_unique_award_id;                 -- expected ≈ 82,651 under ON
```

`{parent_proj}` projects the 10 net-new cols typed + `generated_unique_award_id` as join key (NO `parent_award_id`). Not a collapse core; participates only in the stage-2 enrich LEFT JOIN.

**`include_fresh` toggle** — data-driven emptiness, NOT a second SQL path:
```python
def _stage1_sql(built_at_iso, include_fresh=True) -> str:
    parts=[_MACROS, _bulk_collapse(built_at_iso)]
    parts.append(_fresh_collapse(built_at_iso) if include_fresh else _fresh_collapse_empty(built_at_iso))
    parts.append(_parent_leg())
    parts += [_m_rows_in_bulk, _m_rows_in_fresh, _m_rows_in_parent, _m_fresh_only_tail]
    return "\n".join(parts)
```
`_fresh_collapse_empty` projects the SAME canonical column list/order/type (every `feed_expr` → `CAST(NULL AS <duck_type>)`) with `WHERE 1=0` → `fresh_latest` empty but schema-identical. `fresh_r` scanner NOT opened when `include_fresh=False`. `core_union` degenerates to BULK-only; every award resolves `canonical_source='bulk'`.

## STEP 5 — Reconcile SQL: Stage-2 flat argmax + 3-leg enrich (free-as-you-go DROP)

```sql
CREATE TEMP TABLE core_union AS
SELECT CAST('fresh' AS VARCHAR) AS src, CAST(1 AS INTEGER) AS source_rank, f.* FROM fresh_latest f
UNION ALL BY NAME
SELECT CAST('bulk'  AS VARCHAR) AS src, CAST(2 AS INTEGER) AS source_rank, b.* FROM bulk_latest b;

-- fresh_latest LIVES ON (enrich); do NOT drop yet
CREATE TEMP TABLE core_winner AS
SELECT * EXCLUDE (rn, source_rank) FROM (
  SELECT *, row_number() OVER (
            PARTITION BY generated_unique_award_id
            ORDER BY last_modified_date DESC NULLS LAST,
                     source_rank ASC,
                     generated_unique_award_id DESC NULLS LAST) AS rn
  FROM core_union
) WHERE rn = 1;

CREATE TEMP TABLE m_merged AS SELECT count(*) AS c FROM core_winner;
DROP TABLE core_union;

-- 3-leg enrich: BULK-only (b), FRESH-only (f), parent_award (p). All joins to gua-unique collapses → no fan-out.
CREATE TEMP TABLE resolved AS
SELECT
  w.* EXCLUDE (src, canonical_source) REPLACE (
{enrich_block}
  ),
  w.src AS canonical_source
FROM core_winner w
LEFT JOIN bulk_latest   b ON w.generated_unique_award_id = b.generated_unique_award_id
LEFT JOIN fresh_latest  f ON w.generated_unique_award_id = f.generated_unique_award_id
LEFT JOIN parent_latest p ON w.generated_unique_award_id = p.generated_unique_award_id;

DROP TABLE core_winner; DROP TABLE bulk_latest; DROP TABLE fresh_latest; DROP TABLE parent_latest;

CREATE TEMP TABLE canonical_out AS SELECT {canon_cols} FROM resolved;
DROP TABLE resolved;
```

`build()` runs stage-1, then `_assert_collapse_schema_identity(con)` (`bulk_latest` vs `fresh_latest` — `parent_latest` excluded, not a union member), then stage-2. **No delete/tombstone leg** (`award_search` has no `correction_delete_ind` — live-confirmed; column dropped from DDL).

## STEP 6 — DuckDB / write / publish config (GIANT)

- `_duck()`: `:memory:`, `PRAGMA threads`, `memory_limit`, `temp_directory` on `/tmp`, `SET preserve_insertion_order=false`.
- **BULK → `.to_reader()` single-pass** (registered `bulk_r`, consumed once by inlined collapse; `rows_in_bulk` from `m_rows_in_bulk`). **FRESH + parent → `.to_table()`** (FRESH ~98K distinct; parent post-filter ~32K — both small, re-scannable for enrich + exact metrics). **Correction to prior sizing note:** the full ~988K parent is `.to_table()`d into `parent_r` (small — 11 narrow cols, tens of MB); the semi-join scope-reduction to ~32K happens in the stage-1 SQL (`_parent_leg`), NOT at scan time (the scanner has no filter — the `bulk_latest` keys do not exist until stage-1 runs).
- `is_fpds = TRUE` pushed into the **BULK scanner filter** only, never the SQL body. `--since` appends `AND last_modified_date >= TIMESTAMP '{since}'` (BULK, naive) and `last_modified_date >= '{since}'` (FRESH, lexical ISO). `is_fpds` scanned, not a `COLUMN_SPEC` output.
- **Write:** LOCAL `lance.write_dataset(reader, local_ds, mode="overwrite", data_storage_version="2.1", max_rows_per_file=MAX_ROWS_PER_FILE, max_bytes_per_file=MAX_BYTES_PER_FILE)` (no `storage_options` → local FS). `con.close(); con=None`.
- **RSS reclaim before index sort:** `del reader; gc.collect(); ctypes.CDLL("libc.so.6").malloc_trim(0); shutil.rmtree(DUCK_TMP)`.
- **Fold:** `_build_indices_local(local_ds)` INTO local_ds, then single `_publish_local_to_r2(_s3(), target_uri, local_ds)` — data + indices land atomically in one wipe+upload. A failed index raises before `_s3()` → R2 SoR untouched.
- `_publish_local_to_r2` verbatim from template: `DeleteObjects` prior prefix in ≤1000-key batches, then `os.walk` upload every fragment + `_indices/`+`_versions/`+`_transactions/`. `_s3()` with `retries={max_attempts:10}`, `request_checksum_calculation="when_required"`, `response_checksum_validation="when_required"`.

## STEP 7 — Gates (both raise BEFORE `write_dataset`)

`build()` pre-initializes every metric to `0`/`None`/`False` so a mid-merge crash still writes a coherent `status='error'` ledger row.

```python
rows_out, pk_distinct = con.execute(
    f"SELECT count(*), count(DISTINCT {PK_COL}) FROM canonical_out").fetchone()

# (1) FAIL-CLOSED PK-uniqueness (single-column; structural row_number()=1)
if rows_out != pk_distinct:
    raise RuntimeError(f"PK gate FAILED: count(*)={rows_out:,} != distinct {PK_COL}={pk_distinct:,} …")

# (2) FAIL-CLOSED rows_out FLOOR (full-universe only; relative to live BULK-scope this run)
if since is None:
    bulk_scope_rows = con.execute("SELECT c FROM m_rows_in_bulk").fetchone()[0]
    rows_out_floor = int(bulk_scope_rows * 0.90)
    if rows_out < rows_out_floor:
        raise RuntimeError(f"rows_out FLOOR FAILED: rows_out={rows_out:,} < floor={rows_out_floor:,} …")

# (3) FAIL-CLOSED tail-entered gate — ON only (P1-9): the flip MUST grow the table by the FRESH tail.
if since is None and include_fresh:
    if rows_out <= bulk_scope_rows:
        raise RuntimeError(f"tail gate FAILED: include_fresh=TRUE but rows_out={rows_out:,} "
                           f"<= bulk_scope={bulk_scope_rows:,} (fresh_only_tail did not enter) …")
```

Floor tracks the live snapshot (never a hard literal); `since is None` guards it so `--since` samples are exempt. Gate (3) closes the hole where FRESH collapses to only its 15,859-key overlap and verify #7 (`fresh_won>0`) still passes while all 82,651 tail awards are silently lost.

**is_fpds NULL note (P1-7):** `is_fpds = TRUE` excludes the 263,371 NULL-`is_fpds` rows (correct SQL — NULLs are non-matches). Spot-check a NULL-is_fpds sample's `type`/`piid` in the authoring pass to confirm they are genuinely assistance/loan (non-FPDS); document the 263K exclusion in the docstring. No code change if confirmed non-contract.

**Ledger metrics captured:** `m_rows_in_bulk`, `m_rows_in_fresh`, `m_rows_in_parent`, `m_fresh_only_tail`, `m_merged`; `dedup_collapsed = rows_in_bulk + rows_in_fresh - null_key_dropped - merged` (BULK leg expected ≈ 0 per P1-8); `bulk_only_body` via ANTI JOIN; `fresh_corrections_applied = canonical_out SEMI JOIN bulk_latest WHERE canonical_source='fresh'` (captured pre-DROP or from a retained metric table); **`parent_award_matched = count(*) FROM canonical_out WHERE <a parent-rollup col, e.g. rollup_total_obligation> IS NOT NULL`** (P0-2 — rows that actually received a parent enrich; NOT the nonsensical `parent_award_id = contract_award_unique_key` self-join); `max_last_modified_date = max(last_modified_date) WHERE last_modified_date <= now()`; `max_action_date` clamped `<= CURRENT_DATE`.

## STEP 8 — Ops ledger DDL (`ops_usaspending_award_canonical_runs.sql`)

Subaward DDL shape + net-new columns; **`deletes_tombstoned` DROPPED** (no `correction_delete_ind`).

```sql
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.usaspending_award_canonical_runs (
    id                        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed                      text        NOT NULL,   -- 'usaspending_award_canonical'
    include_fresh             boolean,                -- reconcile-later switch: FALSE=BULK-only
    rows_in_bulk              bigint,                 -- award_search rows scanned (is_fpds scope)
    rows_in_fresh             bigint,                 -- contract_prime_award rows (0 when include_fresh=FALSE)
    rows_in_parent_award      bigint,                 -- parent_award rows AFTER semi-join filter (≈32,341)
    parent_award_matched      bigint,                 -- canonical_out rows that received a parent-rollup enrich
    rows_out                  bigint,
    rows_out_floor            bigint,
    rows_out_floor_ok         boolean,
    dedup_collapsed           bigint,                 -- BULK leg ≈0 (already 1:1); driven by FRESH re-pull dups
    fresh_only_tail           bigint,
    bulk_only_body            bigint,
    fresh_corrections_applied bigint,
    null_key_dropped          bigint,
    max_last_modified_date    timestamp,
    max_action_date           date,
    columns                   integer,
    write_mode                text,                   -- 'overwrite'
    indices_built             text,
    status                    text        NOT NULL,   -- 'success' | 'error'
    error_message             text,
    started_at                timestamptz,
    completed_at              timestamptz,
    recorded_at               timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE ops.usaspending_award_canonical_runs ADD COLUMN IF NOT EXISTS include_fresh boolean;
ALTER TABLE ops.usaspending_award_canonical_runs ADD COLUMN IF NOT EXISTS rows_in_parent_award bigint;
ALTER TABLE ops.usaspending_award_canonical_runs ADD COLUMN IF NOT EXISTS parent_award_matched bigint;
ALTER TABLE ops.usaspending_award_canonical_runs ADD COLUMN IF NOT EXISTS rows_out_floor bigint;
ALTER TABLE ops.usaspending_award_canonical_runs ADD COLUMN IF NOT EXISTS rows_out_floor_ok boolean;
ALTER TABLE ops.usaspending_award_canonical_runs ADD COLUMN IF NOT EXISTS max_last_modified_date timestamp;
CREATE INDEX IF NOT EXISTS usaspending_award_canonical_runs_status_idx ON ops.usaspending_award_canonical_runs (status);
CREATE INDEX IF NOT EXISTS usaspending_award_canonical_runs_recorded_at_idx ON ops.usaspending_award_canonical_runs (recorded_at DESC);
CREATE INDEX IF NOT EXISTS usaspending_award_canonical_runs_max_lastmod_idx ON ops.usaspending_award_canonical_runs (max_last_modified_date DESC);
```

**`_record_run` contract** (subaward L1048-1075 shape, extended kwargs): called from `build()`'s `finally:`, once, every terminal state; `status!='success' ⇒ error NEVER NULL` (`(error or "")[:2000] or None`); self-bootstrap guard present (`to_regclass` → exec `.sql`) but NOT relied on — `init_ops` mandatory pre-step (concurrent first CREATEs deadlock); whole body wrapped `except Exception → log("WARN")` so audit failure never masks the build. `build()`'s row carries `indices_built=NULL` on the giant path (the standalone `index_fn` writes the populated `indices_built` row). **GIANT caveat:** OOM SIGKILL skips `finally:` → NO ledger row; ledger is not the sole completion oracle.

## STEP 9 — Index subcommands

**BTREE_COLS** (presence-filtered): `generated_unique_award_id`, `contract_award_unique_key`, `recipient_uei`, `recipient_hash`, `last_modified_date`, `action_date`, `period_of_performance_current_end_date`, `total_obligation`, `award_id_piid`, `parent_award_id_piid`, `naics_code`, `product_or_service_code`.
**BITMAP_COLS:** `type`, `type_of_set_aside_code`, `extent_competed`, `awarding_agency_code`, `awarding_sub_agency_code`, `funding_agency_code`, `recipient_state_code`, `primary_place_of_performance_state_code`, `award_type_code`, `parent_award_type_code`, `multiple_or_single_award_idv_code`, `canonical_source`.

- **Shipped `index()`** (module): direct-R2, `replace=True` with `TypeError` fallback, presence-filter, BTREE then BITMAP in listed order — sample/dev only.
- **Giant `index_fn`** (modal): FPDS verbatim — `_download_r2_to_local` mirror → `_build_indices_local` (in-RAM sort, `LANCE_BYPASS_SPILLING=true`) → append-only delta publish (before/after key-set diff, upload `_indices/`+`_versions/`+`_transactions/` ONLY, force-re-upload `_versions/latest_version_hint.json` LAST). TWO FAIL-CLOSED gates before any byte write: (1) no `prefix+"data/"…*.lance` in diff; (2) every new key in the `{_indices/,_versions/,_transactions/}` whitelist. `_gc_orphan_indices` prunes superseded UUIDs + aborts dangling MPUs.

## STEP 10 — Modal wrapper config (`usaspending_award_canonical_modal.py`)

Imports `build`/`verify`/`init_ops`/`COLUMN_SPEC`/`BTREE_COLS`/`BITMAP_COLS`/`_s3`/`_r2_so`/`CANONICAL_URI` from the shipped module (ONE merge definition). `add_local_python_source("pipelines")` + `add_local_file("ops_usaspending_award_canonical_runs.sql")`. Env Secrets injected BEFORE the in-body import.

**Sizing corrected against the SHIPPED FPDS reference, not the wrapper docstring (P0-3, P0-4).** The shipped FPDS `.py` runs `memory=196_608` (192 GiB) + `ephemeral_disk=1_572_864` (1.5 TiB) because "the 512 GiB floor was exhausted by the 392-col stage-2 merge spill (run id=9, No space left on device)". This build is **wider (~440 cols)**; the spill footprint scales with column width, not row count. Do NOT down-extrapolate from 30.4M rows to 64 GiB / 512 GiB — that reproduces the exact ENOSPC that already fired. Column width drives the materialize footprint (`bulk_latest` + `core_union` [UNION doubles the BULK core] + `core_winner` co-resident through the enrich JOIN).

```
image: debian_slim(3.12).pip_install("duckdb>=1.5,<2","lancedb>=0.15","pylance>=7","pyarrow>=17","boto3>=1.35","psycopg[binary]>=3.2")
app:   modal.App("usaspending-award-canonical", image=image)
SCRATCH_ROOT="/tmp/award_canonical"

  knob                            build_fn                 index_fn                 verify_fn         smoke_fn
  container memory                131072 (128 GiB)         49152 (48 GiB)           32768 (32 GiB)    —
  ephemeral_disk                  1_048_576 (1 TiB)        1_048_576 (1 TiB)        —                 —
  container cpu                   16.0                     8.0                      4.0               —
  timeout                         6h                       3h                       1h                120
  retries                         0                        0                        0                 0
  max_containers                  1                        1                        —                 —
  AWARD_CANONICAL_DUCKDB_MEM      88GB                     —                        24GB              —
  AWARD_CANONICAL_DUCKDB_THREADS  16                       —                        4                 —
  AWARD_CANONICAL_SCRATCH         $ROOT/stage              $ROOT/idx_stage          —                 —
  AWARD_CANONICAL_DUCKDB_TEMP_DIR $ROOT/duckdb_spill       —                        $ROOT/verify_spill —
  LANCE_BYPASS_SPILLING           true                     true (REQUIRED)          (setdefault)      —

secrets (every fn): from_name("r2-credentials"), from_name("hqx-postgres") + per-fn env Secret
local entrypoints: ::smoke ::build ::index ::verify ::init_ops_main
```

**`ephemeral_disk=1 TiB` provisioned defensively (P0-4), not asserted-away.** Rationale: at ~440 cols, three wide 30.4M-row DuckDB intermediates co-reside; the 392-col precedent already exhausted 512 GiB. 1 TiB is above the failed 512 GiB ceiling and below FPDS's 1.5 TiB (this is 0.28× the FPDS row count but ~1.12× the width). **STEP 12 measures the actual sample-build spill+stage footprint before the full run; if the sample extrapolation exceeds 1 TiB, raise to 1.5 TiB to match FPDS.** Do not ship the full build until the sample confirms the disk budget.

**verify_fn at 32 GiB / 24GB DUCK_MEM** (matches FPDS verify exactly — FPDS documents the 8GB module-default "spills hard and crashes" on a full materialize; verify does `CREATE TEMP TABLE c AS SELECT * FROM c_src`, a full 30.4M × ~440-col VARCHAR-heavy materialize, wider than FPDS's 75-col verify — 32 GiB is the floor, not a down-size).

**build_fn coercions in the local entrypoint (P2-13):** `--since ""→None`; `--include-fresh` parsed via `v.lower() in ("1","true","yes","on")` — **NOT `bool(v)`** (`bool("false")` is `True`; naive coercion inverts intent). A bare `""` → default `True`. `smoke_fn` asserts `len(COLUMN_SPEC)==<locked integer>` (baked, no placeholder — P2-10), ops `.sql` shipped+readable, `_r2_so()` creds present — plus the **cross-toggle schema-identity assertion** (P0-6): build the stage-1 schema under BOTH `include_fresh=True` and `False`, `DESCRIBE fresh_latest` each, assert names+types byte-identical. This is the only guarantee that the reconcile-later flip is schema-safe; the per-build `_assert_collapse_schema_identity` runs within one build, not across the toggle. `retries=0` everywhere.

## STEP 11 — `verify()` read-back + definition-of-done

GIANT: materializes (`CREATE TEMP TABLE c AS SELECT * FROM c_src`). Independent scanner → DuckDB, never trusts `build()` counts. **Fail-closed checks** (each `failures.append` → `verdict=fail`):

1. PK-unique: `count(*) == count(DISTINCT generated_unique_award_id)`.
2. rows_out floor: `count(*) >= rows_out_floor` (from same run's ledger / passed in).
3. cols present: `len(ds.schema.names) == len(COLUMN_SPEC)` (exact locked integer).
4. **index presence by SUBSTRING** over `ds.list_indices()`: assert substring `generated_unique_award_id` in the rendered blob using `i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i))` (subaward-correct form, NOT the award-fresh regressed bare `getattr`). NEVER exact-string match.
5. last_modified frontier: `max(last_modified_date) WHERE <= now()` non-NULL and recent.
6. canonical_source domain: `count(*) WHERE canonical_source IS NULL OR NOT IN ('fresh','bulk') == 0`.
7. fresh-won ⇔ include_fresh: `include_fresh=TRUE` → `count(canonical_source='fresh') > 0`; `FALSE` → `== 0`.
8. **tail-grew ⇔ include_fresh (P1-9)**: `include_fresh=TRUE` → `count(*) > bulk_scope_rows` (the 82,651-key tail must have entered; #7 alone passes even if only the 15,859 overlap survived). `FALSE` → `count(*) == bulk_scope_rows`.
9. null_key: `count(*) WHERE generated_unique_award_id IS NULL == 0`.
10. built_at single literal: `count(DISTINCT built_at) == 1`.
11. **Reported (not gated):** `parent_award_matched / rows_in_parent_award` coverage ratio. **Expected ≈ 32,341 matched out of ~32,341 in-scope parent rows** post-semi-join (near-1.0 now that the leg is scope-filtered; a low ratio signals a join-key defect, not the old data-shape noise).

**Definition-of-done** (all true): (1) dataset exists, `count_rows() > 0`; (2) `verify()` `pass=True`; (3) fresh `status='success'` ledger row with `write_mode='overwrite'`, `rows_out > 0`, `rows_out_floor_ok=TRUE`, `indices_built` non-empty (from `index_fn` row), `include_fresh` recording mode run; (4) **completion sentinel (two-source AND):** `modal app list` shows build app **stopped** AND fresh ledger row `status='success'` — app-stopped + NO fresh row ⇒ OOM/reap, re-run; (5) `max_last_modified_date` sane (≥ ~2026-06-05 BULK frontier; strictly newer when `include_fresh=TRUE`).

## STEP 12 — First landing (BULK-only) — dependency-ordered invocation

```bash
# init_ops (MANDATORY, once, idempotent)
doppler run -p core-x -c prd -- python3 -m pipelines.usaspending.usaspending_award_canonical init_ops

# SAMPLE build+verify (--since slice → _sample prefix; proves the merge AND measures the spill+stage footprint)
doppler run -p core-x -c prd -- uv run --no-project --with 'pylance>=7' --with 'pyarrow>=17' \
  --with 'duckdb>=1.5,<2' --with 'psycopg[binary]>=3.2' \
  python3 -m pipelines.usaspending.usaspending_award_canonical build --since 2026-06-01 \
    --target-uri s3://data-sink/active/_sample/usaspending_award_canonical_sample/
python3 -m pipelines.usaspending.usaspending_award_canonical verify \
    --target-uri s3://data-sink/active/_sample/usaspending_award_canonical_sample/
# → extrapolate sample spill+stage bytes to full 30.4M; if > 1 TiB, raise build_fn/index_fn ephemeral_disk to 1.5 TiB BEFORE the giant.

# GIANT — cheap smoke gate FIRST (asserts col count, .sql, creds, AND cross-toggle schema identity)
modal run pipelines/usaspending/usaspending_award_canonical_modal.py::smoke

# BULK-only first landing (FRESH-independent — lands the 30.4M spine without waiting on the fresh flip)
modal run --detach pipelines/usaspending/usaspending_award_canonical_modal.py::build --include-fresh false
modal run pipelines/usaspending/usaspending_award_canonical_modal.py::index
# verify MUST assert the BULK-only (FALSE) direction — pass --include-fresh false (or omit: verify_fn
# infers the mode from the latest status='success' ledger row). Bare ::verify defaulting to TRUE would
# false-fail gate #7 on a correctly-built BULK-only dataset.
modal run pipelines/usaspending/usaspending_award_canonical_modal.py::verify --include-fresh false
# → confirm completion sentinel + definition-of-done
```
Coerce `--since ""→None` and `--include-fresh` via explicit `.lower() in {...}` parse in the Modal local entrypoints.

## STEP 13 — Reconcile-later flip (once FRESH landed + verified)

**"Reconcile-later" = flip `include_fresh` ON and re-run the overwrite build.** No `COLUMN_SPEC` edit, no `ALTER`, no schema migration.
```bash
modal run --detach pipelines/usaspending/usaspending_award_canonical_modal.py::build   # default include_fresh=True
modal run pipelines/usaspending/usaspending_award_canonical_modal.py::index
modal run pipelines/usaspending/usaspending_award_canonical_modal.py::verify
```
**What changes (ONLY these):** populatedness of FRESH-unique + dual columns; `canonical_source` flips uniform `'bulk'` → real `{fresh,bulk}` mix; `fresh_corrections_applied` > 0; the **82,651-key `fresh_only_tail` enters → `rows_out` grows ≈ +82,651** (gated by build gate (3) + verify #8); `max_last_modified_date` advances toward today; ledger records `include_fresh=TRUE`, non-zero `rows_in_fresh`.
**Invariant (safety rails):** schema (column set/order/types) IDENTICAL — FRESH-unique cols are typed-NULL placeholders under BULK-only, so Arrow/Lance schema is byte-identical either way (PROVEN, not asserted, by the smoke cross-toggle `DESCRIBE fresh_latest` check — P0-6); `len(COLUMN_SPEC)` invariant (verify #3 both times); PK grain one-per-`generated_unique_award_id`; `rows_out >= rows_out_floor`; `built_at` single literal; same target URI + index plan. Downstream consumers bind before FRESH lands and see columns populate on the flip — no migration, no re-point. Structural freeze via `mode="overwrite"` + program-generated schema: build 2 cannot drift from build 1.

## STEP 14 — Ship

Commit all three files, PR against `main` directly (no stack), self-merge `gh pr merge <num> --squash --delete-branch`, pull into the operator's `main` checkout, verify `git log -1 --oneline`.

## NON-GOALS (hard boundaries)

- **No Trigger.dev schedule/cron.** `refresh()` + waitpoint callback wired for parity, no job/deploy/cadence. Rebuild is operator-initiated `build→index→verify`.
- **No delete/tombstone leg.** `award_search` has no `correction_delete_ind` (live-confirmed). No monthly-CSV feed, no archive snapshot leg.
- **`financial_accounts_by_awards` EXCLUDED** (account grain, many-rows-per-award). Only the award-summary account *strings* already in the FRESH PAS columns are carried.
- **`parent_award` `parent_award_id` FK NOT carried** into `canonical_out` (96.6% NULL; drives nothing). Only the 10 rollup aggregates, scope-filtered to the ~32,341 in-spine parents.
- **No incremental patching.** Full overwrite per run.
- **Follow-on (later cycle):** `docs/reference/AWARD_CANONICAL_FIELD_DICTIONARY.md`, generated from `COLUMN_SPEC` after the fresh flip.

## Critique items rejected / down-graded (with reason)

- None rejected. All P0–P2 items are valid and folded in. Two are recorded as authoring-pass tasks rather than code changes: **P1-7** (263K NULL-is_fpds exclusion — spot-check + document, no code change if confirmed non-contract) and **P1-8** (BULK collapse is defensive; `dedup_collapsed`≈0 is documented-expected, not a bug — no code change, only a docstring note so an operator does not misread near-zero dedup as failure).

## Grounding files (absolute)
- Template (COLUMN_SPEC shape, generators, `_record_run` L1048, `_assert_collapse_schema_identity` L1034, `build()` metric-init, `verify()` index-substring form, gates): `/Users/benjamincrane/core-x/pipelines/usaspending/usaspending_subaward_canonical.py`
- Giant reconcile (single-pass reader, inlined collapse, free-as-you-go DROP, RSS reclaim, local-stage index) + **sizing ground truth (L1545-1546: 192 GiB + 1.5 TiB ephemeral_disk, "512 GiB floor exhausted at 392 cols")**: `/Users/benjamincrane/core-x/pipelines/usaspending/usaspending_fpds_canonical.py`
- Giant modal harness (build/index/verify split, append-only `index_fn` + dual FAIL-CLOSED gates + hint-last ordering, completion sentinel; note its docstring's no-ephemeral_disk claim is CONTRADICTED by the shipped `.py` above — trust the shipped decorator): `/Users/benjamincrane/core-x/pipelines/usaspending/usaspending_fpds_canonical_modal.py`
- Ops DDL shape + forward-fill idiom: `/Users/benjamincrane/core-x/pipelines/usaspending/ops_usaspending_subaward_canonical_runs.sql`
- FRESH leg + verify/index-substring caveats + PK/recon-key facts: `/Users/benjamincrane/core-x/docs/reference/AWARD_API_PULL_HANDOFF.md`
- DEC two-hop resolution: `/Users/benjamincrane/core-x/docs/reference/DATA_DICTIONARY_MAP.md`; endpoint/grain: `/Users/benjamincrane/core-x/docs/reference/USASPENDING_AWARDS_API_ENDPOINTS_AND_GRAIN.md`
- Live overlap probe (parent_award grain/scope, is_fpds domain, FRESH containment — all counts in this plan): `/private/tmp/claude-501/-Users-benjamincrane-core-x/abf5dbc8-629c-4290-875e-e3e664dc0249/scratchpad/probe_overlap.py`