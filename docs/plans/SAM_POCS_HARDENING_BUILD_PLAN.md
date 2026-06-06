# SAM POCs Hardening — Build Plan

**Status:** READY-TO-EXECUTE · **Type:** guardrail hardening (no new dataset, no schema change)
**Target file:** [`pipelines/sam_gov/sam_pocs.py`](../../pipelines/sam_gov/sam_pocs.py)
**Reference implementation to mirror:** [`pipelines/sam_gov/sam_normalized_entities.py`](../../pipelines/sam_gov/sam_normalized_entities.py)
**Baselines captured:** live `ops.sam_pocs_runs` ledger, 2026-06-05 success row.
**One-line objective:** Make `sam_pocs` — the only daily, unattended, destructive-overwrite SAM feed — fail safe instead of fail silent, stamp its provenance correctly, and rebuild deterministically.

---

## 0. Read this first (cold-start orientation)

You are executing against the **core-x** Gen-3 data plane. Architecture invariants (see [`ARCHITECTURE.md`](../../ARCHITECTURE.md)):

- **System of record:** LanceDB v2.x written **directly to Cloudflare R2** under `s3://data-sink/active/<dataset>/`. No catalog. Addressed by R2 URI.
- **Transform:** 100% DuckDB, out-of-core, `temp_directory` spill. Python does I/O only.
- **Control plane:** Trigger.dev v4 owns cadence (`src/trigger/*.ts`), spawns Modal workers through the one proxy-authed Universal Dispatcher. **The cron spawns the _deployed_ Modal function by name** — code on `main` does nothing until `modal deploy` publishes it.
- **State:** every worker writes a terminal-state row to a Postgres `ops.<feed>_runs` ledger (`HQX_DB_URL_POOLED`).
- **Indexing:** every load-bearing resolution key gets a `BTREE`; low-cardinality categoricals get `BITMAP`.

### The SAM lineage you are operating inside

```
entity_registrations  (Lance, ~19.3M rows, stacked monthly snapshots; real fields trapped in pipe_fields[])
   │
   ├─ sam_master.py ───────► sam_master_entities   1,541,566  (1/uei, faithful 142-field dict)   [MANUAL]
   │                         ├► sam_master_contacts  4,373,319  (≤6/uei POC unpivot, v2-only)
   │                         └► sam_master_domains     709,546
   │   sam_normalized_entities.py ◄── reads sam_master_entities
   │                         └► sam_normalized_entities  1,541,566  (1/uei, name_norm keys)        [MANUAL]
   │                                  └──► consumed by pipelines/resolution/crosswalk_sos_sam.py
   │
   └─ sam_pocs.py ─────────► sam_pocs        8,065,116  (1 per entity×slot, v2+legacy)   [DAILY 16:30 UTC]  ← THIS PLAN
```

`sam_pocs` is the human/POC layer: it positionally unpivots the six POC blocks from `pipe_fields` into one row per (entity, populated slot), covering both v2 (uei-keyed) and the 120-wide legacy tail (cage-keyed). It is served to the GTM/MCP gateway. **Operator mandate — ZERO-ALTERATION NAME POLICY:** human name strings are never parsed/split; SAM's discrete first/middle/last fields are copied through with whitespace hygiene only. This plan does **not** touch that policy.

### The problem this plan fixes

`build_sam_pocs` today: `materialize → write(mode="overwrite") → reindex → mark success`. There is **no pre-write floor gate and no rollback**. A degenerate or partial source read at 16:30 — with nobody watching — silently overwrites 8.07M committed rows and records `success`. The sibling `sam_normalized_entities` already proves the correct pattern (pre-write gates 1-7, post-write gates 8-10, restore-to-`v_before`, Δ-guard from the ledger). This plan ports that pattern onto `sam_pocs`, adapted to its long/multi-family grain, and folds in two same-function soundness fixes (provenance label, deterministic dedup).

---

## 1. Objective & Operational Definition of Done

The cycle is **done** when **all** of the following are true and demonstrated with evidence:

1. **Fail-safe write.** A pre-write gate failure aborts `build_sam_pocs` **before** any R2 write; the live `sam_pocs` dataset version is unchanged. A post-write gate failure triggers `restore()` to the pre-write version. Proven on a **scratch URI** (§6B), not prod.
2. **Pure gate function** `assert_pre_write_gates(metrics, prior_rows)` exists, is importable without Modal/R2/Postgres, and has a unit test covering the pass path and every individual gate's failure path (§6C).
3. **Ledger Δ-guard** wired: `build`/`plan` read the latest `status='success'` `rows_written` from `ops.sam_pocs_runs` and gate the new run within ±25%.
4. **Provenance corrected (WS-B):** a fresh build stamps `sam_label = '20260503'` (snap-key ordering), **not** `'2026_MAY'` (lexical max). Matches `sam_master_entities` / `sam_normalized_entities`.
5. **Deterministic rebuild (WS-C):** two consecutive scratch builds over the same source produce **identical** `rows_written` and `distinct_uei` (no ±50 jitter).
6. **Signature preserved:** `build_sam_pocs(trigger_callback_url: str | None = None)` is unchanged — no new required kwargs (the dispatcher spawns it with `kwargs={}`). Verified against [`src/trigger/sam_pocs.ts`](../../src/trigger/sam_pocs.ts).
7. **Deployed:** `modal deploy pipelines/sam_gov/sam_pocs.py` published, so the next 16:30 UTC cron runs the hardened function. Verified via `modal app list` / deploy timestamp.
8. **Green on prod once:** one manual `build_sam_pocs` against the real active URI completes `success`, all gates PASS, `ops.sam_pocs_runs` records it, `verify_sam_pocs` read-back matches.
9. **Landed:** PR merged to `main`, branch deleted, plan + code on `main`, operator checkout fetched (§7).

---

## 2. Scope

### In scope — ONE PR, three workstreams (same function surface, each independently gated & revertable)

| WS | Pri | Change | Blast radius |
|----|-----|--------|--------------|
| **A** | **P0** | Pre-write gates (1-8) + post-write gates (9-12) + `v_before` rollback + ledger Δ-guard | The hardening. Non-negotiable. |
| **B** | P1 | Provenance label via snap-key ordering (replace lexical `max(extract_label)`) | Provenance-only; no row content change |
| **C** | P2 | Deterministic dedup tiebreak in `build_pocs_sql` QUALIFY | Row-selection determinism; output stabilizes |

WS-A is mandatory. WS-B and WS-C are specified in full and land in the same PR (they live in the same `_materialize`/`build_pocs_sql` surface WS-A already rewrites — touching it once is surgical). If WS-A validation surfaces any risk, WS-B/WS-C may split to an immediate fast-follow PR; default is all-in-one.

### Explicitly OUT of scope (do not touch — each is its own cycle)

- **`sam_master_contacts` retirement** — it is redundant with `sam_pocs`'s v2 half and has no consumer, but retiring it needs a separate consumer-confirmation cycle. Leave it.
- **Gen-A `sam_entity_master.py`** dead-code deletion — separate hygiene PR.
- **Master→sidecar freshness automation** (Trigger tasks for `sam_master` / `sam_normalized_entities`) — separate control-plane cycle.
- **`sam_normalized_pocs` / `core.person_name_norm`** — deferred; requires a committed person-bridge consumer (the `FEC_SAM_PERSONNEL_BRIDGE` diagnostic) and a canonical, conservative person-name key first. Do **not** start it here.
- **ZERO-ALTERATION NAME POLICY** — untouched. No name parsing/splitting is added.
- **Schema, grain, index set, memory envelope, image deps** — unchanged.

---

## 3. Reference map — what to copy from `sam_normalized_entities.py`

| Borrow | Source (sam_normalized_entities.py) | Adapt for sam_pocs |
|--------|--------------------------------------|--------------------|
| `assert_pre_write_gates(...)` shape + `gate()` closure | lines ~220-250 | new gate semantics (long grain, two families) |
| `_within(value, target, tol)` helper | ~216 | reuse verbatim if cardinality gates added (optional) |
| `_prior_success_rows()` | ~266-287 | swap table → `ops.sam_pocs_runs` |
| Post-write gates + `restore()` rollback block | ~395-428 | swap probes → POC columns/indices |
| `v_before` capture | ~373-377 | verbatim |
| Gate-aware `plan_*` dry-run | ~513-528 | extend existing `plan_sam_pocs` |
| Snap-key SQL `_snap_key_sql` | [`sam_master.py`](../../pipelines/sam_gov/sam_master.py) lines ~130-136 | copy module-level into sam_pocs (WS-B) |

---

## 4. Detailed change set (file: `pipelines/sam_gov/sam_pocs.py`)

> The blocks below are the **target implementation**. Reconcile against the current file; preserve all existing imports, the R2 helper, the image/app definitions, `_post_callback`, `init_ops`, and `verify_sam_pocs` unless named here.

### 4.1 New module-level constants (near existing `BTREE_INDEXES`)

```python
# ── §gate constants (baselined from ops.sam_pocs_runs success, 2026-06-05) ──
# Floors are catastrophic-collapse catchers (set well below live); the Δ-guard
# is the sensitive check that auto-tracks month-over-month growth. Re-baseline
# floors only if the SAM universe shifts >20% or annually. Live @ baseline:
#   rows 8,065,116 · distinct_uei 1,540,966 · distinct_cage 1,167,572
#   poc_rows_v2 4,372,870 · poc_rows_legacy 3,692,246
POCS_ROW_FLOOR       = 6_000_000     # hard "obviously broken" floor (live ~8.06M)
DISTINCT_UEI_FLOOR   = 1_300_000     # v2 entities with ≥1 POC (live ~1.54M)
DISTINCT_CAGE_FLOOR  =   900_000     # legacy cage entities with ≥1 POC (live ~1.17M)
NAME_FILL_MIN        = 0.999         # name_key non-null fraction (≈1.0 by construction)
DELTA_GUARD          = 0.25          # ±25% rows_written vs prior success
EXPECTED_POC_TYPES   = 6             # the six slot labels, no NULLs
SEEK_CEILING_MS      = 2000          # R2 RTT-tolerant point-seek ceiling (mirror sibling)
KIPPER_UEI           = "DD1BCRF2QQG8"  # fleet-canonical round-trip probe
```

### 4.2 WS-B — snap-key label helper (module level)

Copy `_snap_key_sql` **verbatim** from `sam_master.py` (the `JAN..DEC → 01..12` CASE that normalizes both `^[0-9]{8}$` and `YYYY_MMM` labels to a numeric sort key). Add a one-line provenance comment:

```python
# Canonical SAM extract-label sort key — verbatim copy of sam_master.py:_snap_key_sql.
# NOTE: one accepted duplication (frozen calendar map, zero divergence risk). A
# follow-up consolidates both into pipelines/sam_gov/reference/sam_labels.py and
# imports it in both workers (tracked separately — see §10).
def _snap_key_sql(col: str = "extract_label") -> str:
    months = (...)   # identical to sam_master.py
    return (...)
```

### 4.3 WS-C — deterministic dedup (in `build_pocs_sql`)

1. Add `source_file` to the scan (it exists in `entity_registrations`; one varchar col, negligible memory). Update `_materialize`'s `reg` scanner `columns=[...]` to include `"source_file"`, and surface it in the `extracted` CTE.
2. Extend the `keyed` CTE `QUALIFY` ordering so a uei/cage partition resolves to exactly one row regardless of date ties (a uei is unique within a single `source_file`):

```sql
QUALIFY row_number() OVER (
    PARTITION BY coalesce(uei, 'CAGE:' || cage_code)
    ORDER BY last_update_date  DESC NULLS LAST,
             registration_date DESC NULLS LAST,
             extract_label     DESC NULLS LAST,   -- latest snapshot wins on date-tie
             source_file       DESC NULLS LAST    -- final deterministic tiebreak
) = 1
```

### 4.4 WS-B — corrected label resolution (in `_materialize`)

Replace the lexical-max label computation:

```python
# BEFORE (lexical max → '2026_MAY', mis-orders YYYYMMDD vs YYYY_MMM):
sam_label = con.execute("SELECT max(extract_label) FROM lbl").fetchone()[0]

# AFTER (snap-key ordering → '20260503', matches sam_master/sam_normalized):
sam_label = con.execute(
    f"SELECT extract_label FROM lbl ORDER BY {_snap_key_sql()} DESC LIMIT 1"
).fetchone()[0]
```

### 4.5 WS-A — expanded metrics (in `_materialize`)

Extend the single metrics aggregate so the gates have everything they need from one pass (no extra scan). Add to the existing `SELECT`:

```python
rows, d_uei, d_cage, v2_rows, lg_rows, name_present, unkeyed, d_poc_type, null_poc_type = con.execute("""
    SELECT count(*),
           count(DISTINCT uei),
           count(DISTINCT cage_code) FILTER (WHERE uei IS NULL),
           count(*) FILTER (WHERE source_family = 'v2'),
           count(*) FILTER (WHERE source_family = 'legacy_v1'),
           count(*) FILTER (WHERE name_key IS NOT NULL),
           count(*) FILTER (WHERE uei IS NULL AND cage_code IS NULL),
           count(DISTINCT poc_type),
           count(*) FILTER (WHERE poc_type IS NULL)
    FROM pocs
""").fetchone()
metrics = {
    "rows": int(rows), "distinct_uei": int(d_uei), "distinct_cage": int(d_cage),
    "poc_rows_v2": int(v2_rows), "poc_rows_legacy": int(lg_rows),
    "name_present_frac": (name_present / rows) if rows else 0.0,
    "unkeyed_rows": int(unkeyed),
    "distinct_poc_type": int(d_poc_type), "null_poc_type": int(null_poc_type),
}
```
> The `name_present_frac` / `unkeyed_rows` / `*_poc_type` fields are **defense-in-depth** — the SQL already enforces these invariants via its `WHERE`/`keyed` clauses; the gates assert a future SQL edit didn't silently break them. The ledger insert keeps writing only its existing columns (do **not** change `ops.sam_pocs_runs` schema).

### 4.6 WS-A — the pure gate function (new, module level)

```python
def assert_pre_write_gates(metrics: dict, prior_rows: int | None) -> list[str]:
    """Gates 1-8 on in-memory metrics. Raises RuntimeError on first hard failure;
    returns the human-readable check log on success. Pure — no R2/Modal/PG."""
    rows = metrics["rows"]
    checks: list[str] = []

    def gate(ok: bool, label: str) -> None:
        checks.append(f"{'PASS' if ok else 'FAIL'}  {label}")
        if not ok:
            raise RuntimeError(f"PRE-WRITE GATE FAILED → {label}\n" + "\n".join(checks))

    gate(rows >= POCS_ROW_FLOOR, f"1 row floor: {rows:,} >= {POCS_ROW_FLOOR:,}")
    gate(metrics["distinct_uei"] >= DISTINCT_UEI_FLOOR,
         f"2 v2 uei floor: {metrics['distinct_uei']:,} >= {DISTINCT_UEI_FLOOR:,}")
    gate(metrics["poc_rows_v2"] > 0 and metrics["poc_rows_legacy"] > 0,
         f"3 both families non-empty: v2={metrics['poc_rows_v2']:,} legacy={metrics['poc_rows_legacy']:,}")
    gate(metrics["distinct_cage"] >= DISTINCT_CAGE_FLOOR,
         f"4 legacy cage floor: {metrics['distinct_cage']:,} >= {DISTINCT_CAGE_FLOOR:,}")
    gate(metrics["name_present_frac"] >= NAME_FILL_MIN,
         f"5 name fill: {metrics['name_present_frac']:.4%} >= {NAME_FILL_MIN:.2%}")
    gate(metrics["unkeyed_rows"] == 0,
         f"6 every row keyed (uei or cage): unkeyed={metrics['unkeyed_rows']}")
    gate(metrics["distinct_poc_type"] == EXPECTED_POC_TYPES and metrics["null_poc_type"] == 0,
         f"7 slot integrity: poc_types={metrics['distinct_poc_type']}/{EXPECTED_POC_TYPES} "
         f"null={metrics['null_poc_type']}")
    if prior_rows:
        gate(abs(rows - prior_rows) <= prior_rows * DELTA_GUARD,
             f"8 Δ-guard: {rows:,} within ±{DELTA_GUARD:.0%} of prior {prior_rows:,}")
    else:
        checks.append("SKIP  8 Δ-guard: no prior success")
    return checks
```

### 4.7 WS-A — `_prior_success_rows()` (new; mirror sibling, swap table)

```python
def _prior_success_rows() -> int | None:
    conn = _pg_connect()
    if conn is None:
        return None
    try:
        with conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
            cur.execute("SELECT rows_written FROM ops.sam_pocs_runs "
                        "WHERE status='success' AND rows_written IS NOT NULL "
                        "ORDER BY recorded_at DESC LIMIT 1")
            row = cur.fetchone()
            return int(row[0]) if row and row[0] is not None else None
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: prior-rows lookup failed: {exc}")
        return None
    finally:
        conn.close()
```

### 4.8 WS-A — wire gates into `build_sam_pocs` (signature UNCHANGED)

Restructure the `try` body of `build_sam_pocs(trigger_callback_url=None)` to:

```python
con = _new_con()
try:
    table, metrics, sam_label = _materialize(con)
finally:
    con.close()
prior_rows = _prior_success_rows()
print(f"Built sam_pocs: {metrics} sam_label={sam_label}")

# ── PRE-WRITE GATES (abort before any R2 write) ──
for line in assert_pre_write_gates(metrics, prior_rows):
    print("  ", line)

# ── capture rollback target (sam_pocs is live, so this resolves) ──
try:
    v_before = lance.dataset(DATASET_URI, storage_options=so).version
except Exception:
    v_before = None
print(f"v_before = {v_before}")

# ── write + index (existing logic) ──
lance.write_dataset(table, DATASET_URI, mode="overwrite",
                    data_storage_version=DATA_STORAGE_VERSION,
                    max_rows_per_file=MAX_ROWS_PER_FILE,
                    max_bytes_per_file=MAX_BYTES_PER_FILE, storage_options=so)
ds = lance.dataset(DATASET_URI, storage_options=so)
for col in BTREE_INDEXES:
    ds.create_scalar_index(col, index_type="BTREE");  print(f"  BTREE ✓ {col}")
for col in BITMAP_INDEXES:
    ds.create_scalar_index(col, index_type="BITMAP"); print(f"  BITMAP ✓ {col}")

# ── POST-WRITE GATES 9-12; restore-to-v_before on failure ──
try:
    ds = lance.dataset(DATASET_URI, storage_options=so)
    committed = ds.count_rows()
    if committed != metrics["rows"]:
        raise RuntimeError(f"gate 9 committed {committed:,} != materialized {metrics['rows']:,}")
    idx_names = {(i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i)))
                 for i in ds.list_indices()}
    expect_idx = {f"{c}_idx" for c in BTREE_INDEXES + BITMAP_INDEXES}
    if not expect_idx.issubset(idx_names):
        raise RuntimeError(f"gate 10 indices: missing {sorted(expect_idx - idx_names)} (have {sorted(idx_names)})")
    kip = ds.scanner(columns=["uei", "name_key"],
                     filter=f"uei = '{KIPPER_UEI}'").to_table().to_pylist()
    if not (len(kip) >= 1 and any(r["name_key"] for r in kip)):
        raise RuntimeError(f"gate 11 KIPPER round-trip: {KIPPER_UEI} → {len(kip)} rows / no name_key")
    probe = next(r["name_key"] for r in kip if r["name_key"]).replace("'", "''")
    t0 = time.monotonic()
    hit = ds.scanner(columns=["uei"], filter=f"name_key = '{probe}'").to_table().num_rows
    seek_ms = (time.monotonic() - t0) * 1000
    if hit < 1 or seek_ms > SEEK_CEILING_MS:
        raise RuntimeError(f"gate 12 point-lookup: {hit} rows in {seek_ms:.0f}ms (>{SEEK_CEILING_MS} ⇒ no index)")
    print(f"post-write gates PASS — committed={committed:,} idx={sorted(idx_names)} "
          f"KIPPER seek={seek_ms:.0f}ms ({hit} rows)")
except Exception as gate_exc:  # noqa: BLE001
    if v_before is not None:
        lance.dataset(DATASET_URI, storage_options=so, version=v_before).restore()
        raise RuntimeError(f"post-write gate failed → rolled back to v{v_before}: {gate_exc}")
    raise RuntimeError(f"post-write gate failed on net-new dataset (inspect/drop {DATASET_URI}): {gate_exc}")

status = "success"
```
Keep the existing `finally:` block (ledger `_record_run` + `_post_callback`) and the trailing `if status != "success": raise`. Add `import time` to the function's imports (alongside `datetime`, `lance`).

### 4.9 WS-A — make the dry-run honest (`plan_sam_pocs`)

`plan_sam_pocs` must run the **same** gates and write nothing — so a dry-run truthfully predicts a real build:

```python
@app.function(secrets=[modal.Secret.from_name("r2-credentials"),
                       modal.Secret.from_name("hqx-postgres")],
              timeout=60*60, memory=32768, cpu=8.0)
def plan_sam_pocs() -> dict:
    os.makedirs(SPILL_DIR, exist_ok=True)
    con = _new_con()
    try:
        _table, metrics, sam_label = _materialize(con)
    finally:
        con.close()
    prior_rows = _prior_success_rows()
    checks = assert_pre_write_gates(metrics, prior_rows)   # raises on fail
    return {"sam_label": sam_label, "prior_rows": prior_rows, "gates": checks, **metrics}
```
> Note: `plan_sam_pocs` gains the `hqx-postgres` secret (for the Δ-guard read). Its memory/timeout already match.

---

## 5. Gate specification (authoritative)

| # | Phase | Assertion | Constant | On failure |
|---|-------|-----------|----------|------------|
| 1 | pre | `rows ≥ floor` | `POCS_ROW_FLOOR` 6.0M | abort, no write |
| 2 | pre | `distinct_uei ≥ floor` | `DISTINCT_UEI_FLOOR` 1.3M | abort |
| 3 | pre | `poc_rows_v2 > 0 ∧ poc_rows_legacy > 0` | — | abort (catches a family-classification regression like the #55 bug) |
| 4 | pre | `distinct_cage ≥ floor` | `DISTINCT_CAGE_FLOOR` 0.9M | abort |
| 5 | pre | `name_key` non-null fraction ≥ floor | `NAME_FILL_MIN` 0.999 | abort (invariant guard) |
| 6 | pre | `unkeyed_rows == 0` | — | abort (invariant guard) |
| 7 | pre | `distinct_poc_type == 6 ∧ null_poc_type == 0` | `EXPECTED_POC_TYPES` | abort (slot integrity) |
| 8 | pre | `rows` within ±25% of prior success | `DELTA_GUARD` | abort (skipped if no prior) |
| 9 | post | committed row count == materialized rows | — | **restore `v_before`** |
| 10 | post | all 6 indices present (`*_idx`) | — | restore |
| 11 | post | `KIPPER_UEI` resolves to ≥1 POC with `name_key` | `KIPPER_UEI` | restore |
| 12 | post | indexed point-seek on `name_key` ≤ ceiling | `SEEK_CEILING_MS` | restore |

---

## 6. Validation harness (blast-radius-contained — prove on scratch BEFORE prod)

`DATASET_URI` is read from `os.environ.get("SAM_POCS_LANCE_URI", "s3://data-sink/active/sam_pocs/")`. Use that override to run the entire harness against a **scratch** URI so prod `sam_pocs` is never at risk during validation.

```
SCRATCH = s3://data-sink/scratch/sam_pocs_hardening/
```

### 6A. Happy path (scratch)
```
SAM_POCS_LANCE_URI=$SCRATCH  modal run pipelines/sam_gov/sam_pocs.py --dry-run
SAM_POCS_LANCE_URI=$SCRATCH  modal run pipelines/sam_gov/sam_pocs.py
SAM_POCS_LANCE_URI=$SCRATCH  modal run pipelines/sam_gov/sam_pocs.py::verify_sam_pocs
```
**Expect:** dry-run prints gates 1-8 all `PASS`, writes nothing. Build prints gates 1-8 PASS → write → 6 indices → gates 9-12 PASS → `success`. `sam_label == '20260503'` (WS-B). `verify` read-back: rows ≈ 8.0M, 6 `poc_type`s, both families present, 6 indices.

### 6B. Negative paths (scratch — prove the guard actually guards)
1. **Pre-write abort, no overwrite:** temporarily set `POCS_ROW_FLOOR` above live (e.g. `99_000_000`) → run build → **must** raise `PRE-WRITE GATE FAILED → 1 row floor`, and `lance.dataset($SCRATCH).version` is **unchanged** from before the run. Revert the constant.
2. **Post-write rollback:** after a clean 6A build on scratch, temporarily add a bogus name to `expect_idx` (e.g. inject `"bogus_idx"`) → run build → gate 10 fails → log shows `rolled back to v{N}` → `lance.dataset($SCRATCH).version` returns to the pre-write version and row content is intact. Revert.
3. Record both outcomes (the raised error string + the unchanged/restored version number) as evidence in the PR body.

### 6C. Pure unit test (no R2/Modal/PG) — `tests/test_sam_pocs_gates.py` (or repo's test location)
Import `assert_pre_write_gates` and assert:
- a baseline `metrics` dict (live values) + `prior_rows=8_065_116` → returns 8 `PASS` lines, no raise.
- each gate's failure in isolation (row floor, uei floor, a zero family, cage floor, name fill < min, `unkeyed_rows=1`, `distinct_poc_type=5`, Δ-guard at ±26%) → raises `RuntimeError` whose message names that gate.
- `prior_rows=None` → gate 8 line is `SKIP`, no raise.

### 6D. Prod cutover (only after 6A-6C are green)
```
modal deploy pipelines/sam_gov/sam_pocs.py                 # REQUIRED — cron runs the deployed fn
modal run    pipelines/sam_gov/sam_pocs.py                 # one manual hardened build on real active URI
modal run    pipelines/sam_gov/sam_pocs.py::verify_sam_pocs
```
Then confirm the live ledger:
```sql
-- via: doppler run -- psql "$HQX_DB_URL_POOLED" -c "..."
SELECT recorded_at, status, sam_label, rows_written, distinct_uei, poc_rows_v2, poc_rows_legacy
FROM ops.sam_pocs_runs ORDER BY recorded_at DESC LIMIT 2;
```
**Expect:** newest row `status='success'`, `sam_label='20260503'`, `rows_written` within ±25% of the prior. **Determinism (WS-C):** the manual build's `rows_written`/`distinct_uei` equal the next 16:30 cron run's (same source) — no jitter.

---

## 7. Landing (git lifecycle — own it end to end)

Work happens on the current worktree branch.
1. `git add docs/plans/SAM_POCS_HARDENING_BUILD_PLAN.md pipelines/sam_gov/sam_pocs.py tests/test_sam_pocs_gates.py`
2. Commit:
   ```
   feat(sam_pocs): fail-safe hardening — pre/post-write gates, rollback, Δ-guard,
   snap-key provenance label, deterministic dedup

   sam_pocs was the only daily unattended SAM feed doing a destructive overwrite
   with no floor and no rollback. Ports the proven sam_normalized_entities gate
   pattern (1-8 pre-write, 9-12 post-write, restore-to-v_before) adapted to the
   long v2+legacy POC grain. WS-B fixes provenance (lexical max '2026_MAY' →
   snap-key '20260503'). WS-C adds a deterministic dedup tiebreak (bit-stable
   rebuilds). Signature unchanged; modal deploy required for the cron path.
   ```
   (close per repo convention)
3. `git push -u origin <branch>` → open PR against `main` with §6 evidence in the body.
4. **Self-verify** (§6A-6C green + §6D green) → `gh pr merge <num> --squash --delete-branch`.
5. `modal deploy pipelines/sam_gov/sam_pocs.py` (if not already deployed in 6D) — **the cron is not hardened until this lands.**
6. Operator checkout reconciliation: `git -C /Users/benjamincrane/core-x fetch origin`. **Do not** switch/reset that checkout — it is on `feat/corex-gtm-control-surface` (active WIP). Report that `main` now carries the change; the operator folds it in on their next rebase.
7. Verify: `git -C /Users/benjamincrane/core-x log origin/main -1 --oneline` shows the squashed commit.

---

## 8. Safety, invariants & abort procedure

- **Overwrite exposure window** is bounded to gate-9-12 evaluation (seconds); `restore()` is an atomic Lance version swap. Acceptable for a daily feed; the prior good version is always recoverable via time-travel.
- **Hard invariant — signature:** `build_sam_pocs(trigger_callback_url=None)` must keep zero required kwargs (dispatcher spawns `kwargs={}`). If you must pass anything new, give it a default. Re-confirm against `src/trigger/sam_pocs.ts` before merge.
- **No schema/ledger change.** `ops.sam_pocs_runs` columns are unchanged; new metrics are gate-only.
- **Memory envelope unchanged** (24GB / 8 threads / `LANCE_BYPASS_SPILLING`); `source_file` adds one varchar to the scan — negligible.
- **Idempotent.** Overwrite + reindex each run; reruns converge (and, post-WS-C, are bit-identical).
- **If the cycle itself goes wrong in prod:** restore the active dataset to its pre-cutover version —
  ```python
  import lance
  d = lance.dataset("s3://data-sink/active/sam_pocs/", storage_options=so)
  print(d.versions())                      # find the last pre-cutover version N
  lance.dataset("s3://data-sink/active/sam_pocs/", storage_options=so, version=N).restore()
  ```
  Record the pre-cutover version number **before** running 6D so this target is known.

---

## 9. Acceptance checklist (gate the PR on this)

- [ ] WS-A: `assert_pre_write_gates` + `_prior_success_rows` + post-write/rollback wired into `build_sam_pocs`; `plan_sam_pocs` runs the same gates.
- [ ] WS-B: fresh build stamps `sam_label='20260503'`.
- [ ] WS-C: two consecutive scratch builds → identical `rows_written` & `distinct_uei`.
- [ ] 6A green (scratch happy path); 6B-1 proves no-overwrite-on-pre-gate-fail; 6B-2 proves restore-on-post-gate-fail.
- [ ] 6C unit test green (pass path + every failure path).
- [ ] Signature unchanged; verified vs `src/trigger/sam_pocs.ts`.
- [ ] 6D green on real active URI; ledger shows `success` + corrected label.
- [ ] `modal deploy` published; `modal app list` confirms fresh deploy timestamp.
- [ ] PR merged `--squash --delete-branch`; operator checkout fetched; `origin/main` log verified.

---

## 10. Flagged follow-ups (do NOT do in this PR — tee up next cycles)

1. **Single-source `_snap_key_sql`** → extract to `pipelines/sam_gov/reference/sam_labels.py`, import in both `sam_master.py` and `sam_pocs.py` (removes the one accepted duplication from §4.2). Mirror `sam_normalized`'s `add_local_python_source` pattern for the container mount; regression-gate that `sam_master` still stamps `20260503`.
2. **Re-baseline policy for absolute floors** (here and in `sam_normalized_entities`) — convert collapse-floors to relative-to-source or add a scheduled re-baseline; Δ-guard already auto-tracks growth.
3. **`sam_master_contacts` retirement** — confirm zero consumers, then drop (redundant with `sam_pocs` v2 half).
4. **Freshness automation** — Trigger tasks for `sam_master` + `sam_normalized_entities` so the monthly drop refreshes the resolution surface without a manual two-step.
5. **`core.person_name_norm` + `sam_normalized_pocs`** — only once a person-bridge consumer (FEC↔SAM personnel) is committed.
