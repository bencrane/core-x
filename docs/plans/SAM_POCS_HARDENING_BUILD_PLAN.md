# SAM POCs Hardening — Build Plan

**Status:** READY-TO-EXECUTE · **Type:** guardrail hardening (no new dataset, no schema change)
**Target:** [`pipelines/sam_gov/sam_pocs.py`](../../pipelines/sam_gov/sam_pocs.py)
**Reference pattern:** [`pipelines/sam_gov/sam_normalized_entities.py`](../../pipelines/sam_gov/sam_normalized_entities.py) (ships the gate+rollback shape this plan extends)
**Baselines:** live `ops.sam_pocs_runs`, healthy runs 2026-06-02…05.
**Objective:** Make `sam_pocs` — the only daily, unattended, destructive-overwrite SAM feed — fail safe (never overwrite good data with bad), fail loud (alert a human when it does), stamp provenance correctly, and rebuild deterministically.

---

## 0. Orientation (cold-start — read before touching anything)

You are in **core-x**, the Gen-3 data plane. Invariants ([`ARCHITECTURE.md`](../../ARCHITECTURE.md)):
- **SoR:** LanceDB v2.x written **directly to R2** under `s3://data-sink/active/<dataset>/`. No catalog; addressed by URI.
- **Transform:** 100% DuckDB, out-of-core, `temp_directory` spill. Python does I/O only.
- **Control plane:** Trigger.dev v4 owns cadence; it calls the proxy-authed Universal Dispatcher, which `spawn()`s the **deployed** Modal function by name. **Code on `main` is inert until `modal deploy` publishes it.**
- **State:** every worker writes a terminal-state row to `ops.<feed>_runs` (Postgres, `HQX_DB_URL_POOLED`).
- **Indexing:** load-bearing resolution keys → `BTREE`; low-cardinality categoricals → `BITMAP`.

### Lineage

```
entity_registrations (Lance, ~19.3M rows, stacked monthly snapshots; real fields in pipe_fields[])
   └─ sam_pocs.py ─► sam_pocs   ~8.07M rows (1 per entity×populated POC slot, v2+legacy)   [DAILY 16:30 UTC]
```

`sam_pocs` positionally unpivots the six POC blocks from `pipe_fields` into one row per (entity, populated slot): v2 records key on `uei` (base offset 47), the 120-wide legacy tail keys on `cage_code` (base offset 45). It is served to the GTM/MCP gateway. Cron: [`src/trigger/sam_pocs.ts`](../../src/trigger/sam_pocs.ts) `30 16 * * *` → dispatcher → `app_name="sam-gov-pocs-pipelines", function_name="build_sam_pocs", kwargs={}`.

**ZERO-ALTERATION NAME POLICY (operator mandate):** human names are never parsed/split. SAM's discrete first/middle/last fields are copied through with whitespace hygiene only; `full_name` is a lossless concat; `name_key = upper(full_name)` is an added, non-authoritative accelerator. **This plan does not touch that policy.**

### Why this is P0 — the threat is ledger-proven, not hypothetical

`build_sam_pocs` today does `materialize → write(mode="overwrite") → reindex → mark success`, with **no pre-write floor and no rollback**. `ops.sam_pocs_runs` records two real failures that shipped to the live dataset:

| recorded_at | status | rows_written | distinct_uei | poc_rows_v2 | meaning |
|---|---|---|---|---|---|
| 2026-06-02 02:08 | **success** | **0** | 0 | 0 | empty build — overwrote the GTM dataset to zero |
| 2026-06-02 02:19 | **success** | **6,389,167** | 888,361 | 2,696,876 | ~38% v2 short — partial, recorded clean |

Healthy runs sit at ~8.07M rows / ~1.541M uei / ~1.168M cage / ~4.373M v2 / ~3.692M legacy. The fix is to gate hard **before** the write (where protection costs zero exposure window), keep an integrity gate + rollback **after** the write, and alert a human on any terminal failure.

---

## 1. Objective & Operational Definition of Done

Done when **all** are true and demonstrated with evidence:

1. **Fail-safe.** A pre-write gate failure aborts before any R2 write (live version unchanged). A failure during write, indexing, **or** post-write gates restores the dataset to the pre-write version. Proven on a **scratch URI**, never prod (§6).
2. **The known partial is caught.** A regression fixture feeding the historical 6,389,167-row / 888,361-uei / 2,696,876-v2 metrics against a healthy baseline **fails** the pre-write gates (unit test, §6C).
3. **Baseline can't ratchet or be poisoned.** The Δ-guard baseline is the latest **prod-URI** success that cleared `BASELINE_MIN_ROWS`; a degraded success never becomes the baseline, and scratch/validation runs never touch the prod baseline (unit + integration, §6).
4. **Silent corruption is gated.** A positional-offset name regression (names where digits belong, zip where letters belong) **fails** the pre-write content gates (unit test with an offset-shifted fixture, §6C).
5. **Fail-loud.** A terminal `error`/rollback emits a human-visible alert when `OPS_ALERT_WEBHOOK` is configured; absence of the env var degrades to a no-op (never blocks the build).
6. **Provenance corrected.** A fresh build stamps `sam_label='20260503'` (snap-key ordering), matching `sam_master_entities`/`sam_normalized_entities` — not the lexical-max `'2026_MAY'`.
7. **Deterministic.** Two consecutive scratch builds over the same source produce **identical** `rows_written` and `distinct_uei`.
8. **Signature preserved.** `build_sam_pocs(trigger_callback_url: str | None = None)` — no new required kwargs (dispatcher spawns `kwargs={}`). Verified vs `src/trigger/sam_pocs.ts` + `core/modal_dispatcher.py`.
9. **Deployed + green once.** `modal deploy` published; one manual prod build completes `success`, all gates PASS, ledger + `verify_sam_pocs` read-back match.
10. **Landed.** PR merged to `main`, branch deleted, operator checkout fetched (§7).

---

## 2. Scope

### In scope — ONE PR, four workstreams (same function surface; each independently revertable)

| WS | Pri | Change |
|----|-----|--------|
| **A** | **P0** | Pre-write gate suite (floors + **per-family** Δ-guards + invariants + **name-content plausibility**); post-write integrity/index/round-trip gates; **write+index+gate-wrapped** rollback; floor-qualified, URI-scoped, **scratch-isolated** ledger baseline |
| **B** | P1 | Provenance label via snap-key ordering |
| **C** | P1 | Deterministic dedup (add `source_file`; extend QUALIFY tiebreak) |
| **D** | P1 | Minimal env-gated failure alert (`OPS_ALERT_WEBHOOK`, no-op if unset) |

### Residual risk accepted this cycle (stated, not hidden)

`sam_pocs` keeps `mode="overwrite"`. The destructive window is the interval between the overwrite commit and the post-write gates passing — dominated by building four BTREEs (incl. high-cardinality `name_key`, `last_name`) over ~8M rows on R2, i.e. **minutes, not seconds**. During it the gateway reads committed-but-unvalidated, possibly under-indexed data; on gate failure it is rolled back. This is acceptable for a daily feed and is the explicit trade for shipping protection now. **Eliminating the window** (staging + atomic promote) is the named next cycle (§10), deliberately not co-mingled with this gate-hardening PR.

### Out of scope (each its own cycle)

- `sam_master_contacts` retirement (redundant with `sam_pocs`'s v2 half, no consumer).
- Gen-A `sam_entity_master.py` dead-code deletion.
- Master→sidecar freshness automation (Trigger tasks for the manual masters).
- `sam_normalized_pocs` / `core.person_name_norm` (needs a committed person-bridge consumer first).
- ZERO-ALTERATION NAME POLICY, schema, grain, index set, memory envelope, image deps — unchanged.

---

## 3. Design — the shape this plan stands behind

**Protection lives pre-write.** The materialized Arrow table + a single metrics aggregate are gated before any overwrite, at zero exposure cost. Post-write gates only catch Lance write/index integrity faults and prove the index serves point-lookups; on any post-write (or mid-index) failure, rollback restores the prior version.

**The Δ-guard is per-family and baseline-disciplined.** A half-collapse (e.g. the v2 classification breaking) moves `poc_rows_v2` and `distinct_uei` hard but can leave total `rows` within a coarse band — so each family is guarded independently. The baseline is the latest **prod-URI** success that cleared `BASELINE_MIN_ROWS`, so a degraded success can neither become the comparison point (no ratchet) nor be supplied by a scratch run (no poison).

**Counts don't prove correctness.** A positional-offset bug yields fully-populated, non-null, wrong-valued rows that pass every count/fill gate. Names and zips have disjoint character profiles; a content gate asserts `first_name` is alpha-dominant and `zip5` is numeric — a shift inverts both and fails before the write. This is the one check that closes the silent-corruption class the worker's own docstring fears.

**Rollback wraps write + indexing + gates.** A mid-index OOM must not leave a fully-overwritten, under-indexed dataset live; the rollback guard spans the entire mutating region and makes a failed `restore()` loud.

---

## 4. Change set — `pipelines/sam_gov/sam_pocs.py`

> Target implementation; reconcile against the live file. Preserve existing imports, `_r2_storage_options`, image/app, `_post_callback`, `init_ops`, `verify_sam_pocs`, `OPS_DDL`, and the ledger insert (its columns are unchanged) unless named here.

### 4.1 Constants + scratch-isolated feed tag (module level)

```python
_PROD_URI = "s3://data-sink/active/sam_pocs/"
DATASET_URI = os.environ.get("SAM_POCS_LANCE_URI", _PROD_URI)
# Ledger feed tag: prod URI → 'sam_pocs'; ANY override (scratch/validation) → 'sam_pocs_scratch'
# so validation runs never poison the prod baseline or pollute prod metrics.
FEED = "sam_pocs" if DATASET_URI == _PROD_URI else "sam_pocs_scratch"

# ── gate constants (baselined from ops.sam_pocs_runs healthy runs 2026-06-02..05) ──
# Floors = catastrophic-collapse catchers (well below live). The per-family Δ-guards are
# the sensitive regression check and auto-track month-over-month drift. Re-baseline the
# absolute floors on any sustained ±20% shift in the SAM universe (SAM also *purges*
# inactive registrations — a legitimate shrink must not false-trip a floor).
POCS_ROW_FLOOR      = 6_000_000   # abort floor (live ~8.07M)
DISTINCT_UEI_FLOOR  = 1_300_000   # v2 entities w/ ≥1 POC (live ~1.541M)
DISTINCT_CAGE_FLOOR =   900_000   # legacy cage entities w/ ≥1 POC (live ~1.168M)
BASELINE_MIN_ROWS   = 7_000_000   # a success must clear this to qualify as a Δ baseline
DELTA_GUARD         = 0.25        # ±25% per-family vs prior healthy success
NAME_FILL_MIN       = 0.999       # name_key fill — invariant tripwire (NOT a content check)
NAME_ALPHA_MIN      = 0.95        # first_name alpha-char fraction — confirm vs 6A, set a few pts below
ZIP_NUMERIC_MIN     = 0.95        # zip5 numeric fraction      — confirm vs 6A, set a few pts below
MANDATORY_SLOTS     = ("government_business", "electronic_business")  # the always-populated pair
SEEK_CEILING_MS     = 2000        # R2-RTT-tolerant indexed point-seek ceiling
```

### 4.2 WS-B — snap-key label helper (module level)

Copy `_snap_key_sql` **verbatim** from [`sam_master.py`](../../pipelines/sam_gov/sam_master.py) (`JAN..DEC → 01..12` CASE normalizing both `^[0-9]{8}$` and `YYYY_MMM` to a numeric sort key). `sam_pocs` imports nothing repo-local, so a module-level copy ships in the container with no mount.

```python
# Canonical SAM extract-label sort key — verbatim from sam_master.py:_snap_key_sql.
# One accepted duplication (frozen calendar map, zero divergence risk); §10 consolidates.
def _snap_key_sql(col: str = "extract_label") -> str:
    months = (...); return (...)   # identical to sam_master.py
```

### 4.3 WS-C — deterministic dedup (`build_pocs_sql` + scan)

- Add `"source_file"` to the build scan `columns=[...]` in `_materialize` (one varchar; negligible).
- Carry `source_file` through the `extracted` CTE `SELECT`, and extend the `keyed` CTE QUALIFY so a partition resolves to exactly one row (a uei is unique within a single `source_file`):

```sql
QUALIFY row_number() OVER (
    PARTITION BY coalesce(uei, 'CAGE:' || cage_code)
    ORDER BY last_update_date  DESC NULLS LAST,
             registration_date DESC NULLS LAST,
             extract_label     DESC NULLS LAST,   -- latest snapshot wins on date-tie
             source_file       DESC NULLS LAST    -- final deterministic tiebreak
) = 1
```

### 4.4 WS-B — corrected label (in `_materialize`)

```python
sam_label = con.execute(
    f"SELECT extract_label FROM lbl ORDER BY {_snap_key_sql()} DESC LIMIT 1"
).fetchone()[0]
```

### 4.5 WS-A — single-pass metrics (in `_materialize`)

Replace the metrics aggregate so one scan of the `pocs` temp table yields everything gates 1–13 need; add a tiny probe query and the present-slot set. Return the existing 3-tuple `(table, metrics, sam_label)` with the richer dict (extra keys are build-internal; the ledger insert ignores them).

```python
(rows, d_uei, d_cage, v2, lg, name_present, unkeyed, null_pt,
 fn_present, fn_alpha, zip_present, zip_num, present_types) = con.execute("""
    SELECT count(*),
           count(DISTINCT uei),
           count(DISTINCT cage_code) FILTER (WHERE uei IS NULL),
           count(*) FILTER (WHERE source_family='v2'),
           count(*) FILTER (WHERE source_family='legacy_v1'),
           count(*) FILTER (WHERE name_key IS NOT NULL),
           count(*) FILTER (WHERE uei IS NULL AND cage_code IS NULL),
           count(*) FILTER (WHERE poc_type IS NULL),
           count(*) FILTER (WHERE first_name IS NOT NULL),
           count(*) FILTER (WHERE first_name IS NOT NULL AND regexp_matches(first_name,'[A-Za-z]')),
           count(*) FILTER (WHERE zip5 IS NOT NULL),
           count(*) FILTER (WHERE zip5 IS NOT NULL AND regexp_matches(zip5,'^[0-9]{3,5}$')),
           array_agg(DISTINCT poc_type)
    FROM pocs
""").fetchone()
# probe: a uei guaranteed to have a POC with a non-null name_key (most POC rows; uei tiebreak → stable)
probe_uei = con.execute("""
    SELECT uei FROM pocs WHERE uei IS NOT NULL AND name_key IS NOT NULL
    GROUP BY uei ORDER BY count(*) DESC, uei LIMIT 1
""").fetchone()
metrics = {
    "rows": int(rows), "distinct_uei": int(d_uei), "distinct_cage": int(d_cage),
    "poc_rows_v2": int(v2), "poc_rows_legacy": int(lg),
    "name_present_frac": (name_present / rows) if rows else 0.0,
    "unkeyed_rows": int(unkeyed), "null_poc_type": int(null_pt),
    "name_alpha_frac": (fn_alpha / fn_present) if fn_present else 0.0,
    "zip_numeric_frac": (zip_num / zip_present) if zip_present else 0.0,
    "present_poc_types": set(present_types or []),
    "probe_uei": probe_uei[0] if probe_uei else None,
}
```

### 4.6 WS-A — pure gate function (new, module level)

```python
def _within(value: int, target: int, tol: float) -> bool:
    return abs(value - target) <= target * tol

def assert_pre_write_gates(metrics: dict, baseline: dict | None) -> list[str]:
    """Gates 1-13 on in-memory metrics. Raises on first hard failure; returns the check
    log on success. Pure — no R2/Modal/PG; this is the unit-tested core of the safety net."""
    rows = metrics["rows"]; checks: list[str] = []
    def gate(ok: bool, label: str) -> None:
        checks.append(f"{'PASS' if ok else 'FAIL'}  {label}")
        if not ok:
            raise RuntimeError(f"PRE-WRITE GATE FAILED → {label}\n" + "\n".join(checks))

    # — floors (catastrophic-collapse catchers) —
    gate(rows >= POCS_ROW_FLOOR, f"1 row floor: {rows:,} >= {POCS_ROW_FLOOR:,}")
    gate(metrics["distinct_uei"] >= DISTINCT_UEI_FLOOR, f"2 uei floor: {metrics['distinct_uei']:,} >= {DISTINCT_UEI_FLOOR:,}")
    gate(metrics["distinct_cage"] >= DISTINCT_CAGE_FLOOR, f"3 cage floor: {metrics['distinct_cage']:,} >= {DISTINCT_CAGE_FLOOR:,}")
    gate(metrics["poc_rows_v2"] > 0 and metrics["poc_rows_legacy"] > 0,
         f"4 both families: v2={metrics['poc_rows_v2']:,} legacy={metrics['poc_rows_legacy']:,}")
    # — per-family Δ-guards (the sensitive regression check) —
    if baseline:
        gate(_within(rows, baseline["rows_written"], DELTA_GUARD),
             f"5 rows Δ: {rows:,} ~ ±{DELTA_GUARD:.0%} of {baseline['rows_written']:,}")
        gate(_within(metrics["poc_rows_v2"], baseline["poc_rows_v2"], DELTA_GUARD),
             f"6 v2 Δ: {metrics['poc_rows_v2']:,} ~ ±{DELTA_GUARD:.0%} of {baseline['poc_rows_v2']:,}")
        gate(_within(metrics["distinct_uei"], baseline["distinct_uei"], DELTA_GUARD),
             f"7 uei Δ: {metrics['distinct_uei']:,} ~ ±{DELTA_GUARD:.0%} of {baseline['distinct_uei']:,}")
    else:
        checks.append("SKIP  5-7 Δ-guards: no floor-qualified prior success")
    # — structural invariants (defense-in-depth on the SQL) —
    gate(metrics["unkeyed_rows"] == 0, f"8 every row keyed (uei|cage): unkeyed={metrics['unkeyed_rows']}")
    gate(metrics["null_poc_type"] == 0, f"9 no null poc_type: {metrics['null_poc_type']}")
    gate(set(MANDATORY_SLOTS).issubset(metrics["present_poc_types"]),
         f"10 mandatory slots present: have {sorted(metrics['present_poc_types'])}")
    gate(metrics["name_present_frac"] >= NAME_FILL_MIN,
         f"11 name-fill tripwire: {metrics['name_present_frac']:.4%} >= {NAME_FILL_MIN:.2%}")
    # — content plausibility (positional-offset defense) —
    gate(metrics["name_alpha_frac"] >= NAME_ALPHA_MIN,
         f"12 name-alpha: {metrics['name_alpha_frac']:.4%} >= {NAME_ALPHA_MIN:.0%}")
    gate(metrics["zip_numeric_frac"] >= ZIP_NUMERIC_MIN,
         f"13 zip-numeric: {metrics['zip_numeric_frac']:.4%} >= {ZIP_NUMERIC_MIN:.0%}")
    return checks
```

### 4.7 WS-A — floor-qualified, URI-scoped baseline (new)

```python
def _prior_success_baseline() -> dict | None:
    """Latest prod-URI success that cleared BASELINE_MIN_ROWS — the per-family Δ baseline.
    Floor-qualified (a degraded success can't become the baseline → no ratchet) and
    dataset_uri-scoped (scratch runs can't poison it)."""
    conn = _pg_connect()
    if conn is None:
        return None
    try:
        with conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
            cur.execute(
                "SELECT rows_written, distinct_uei, poc_rows_v2, poc_rows_legacy "
                "FROM ops.sam_pocs_runs "
                "WHERE status='success' AND dataset_uri = %s AND rows_written >= %s "
                "ORDER BY recorded_at DESC LIMIT 1",
                (DATASET_URI, BASELINE_MIN_ROWS),
            )
            r = cur.fetchone()
            return None if not r else {
                "rows_written": int(r[0]), "distinct_uei": int(r[1]),
                "poc_rows_v2": int(r[2]), "poc_rows_legacy": int(r[3])}
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: baseline lookup failed: {exc}")
        return None
    finally:
        conn.close()
```

### 4.8 WS-D — minimal failure alert (new)

```python
def _alert(msg: str) -> None:
    """Best-effort human alert on terminal failure/rollback. No-op if OPS_ALERT_WEBHOOK
    unset; never raises (must not mask or block the build)."""
    url = os.environ.get("OPS_ALERT_WEBHOOK")
    if not url:
        return
    try:
        import requests
        requests.post(url, json={"text": f"[sam_pocs] {msg}"}, timeout=10)
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: alert POST failed: {exc}")
```

### 4.9 WS-A/D — `build_sam_pocs` body (signature UNCHANGED)

Restructure the `try`; keep the existing `finally` (`_record_run` + `_post_callback`) and trailing `if status != "success": raise`. Add `import time`. Add `OPS_ALERT_WEBHOOK` to the function's Modal secret (or a new `ops-alerts` secret).

```python
con = _new_con()
try:
    table, metrics, sam_label = _materialize(con)
finally:
    con.close()
baseline = _prior_success_baseline()
print(f"materialized: { {k: v for k, v in metrics.items() if k != 'present_poc_types'} } "
      f"sam_label={sam_label} baseline={baseline}")

# ── PRE-WRITE GATES (abort before any R2 write) ──
for line in assert_pre_write_gates(metrics, baseline):
    print("  ", line)

# ── rollback target (sam_pocs is live → resolves; None only on a net-new URI) ──
try:
    v_before = lance.dataset(DATASET_URI, storage_options=so).version
except Exception:
    v_before = None
print(f"v_before = {v_before}")

# ── write + index + post-write gates, ALL under one rollback guard ──
try:
    lance.write_dataset(table, DATASET_URI, mode="overwrite",
                        data_storage_version=DATA_STORAGE_VERSION,
                        max_rows_per_file=MAX_ROWS_PER_FILE,
                        max_bytes_per_file=MAX_BYTES_PER_FILE, storage_options=so)
    ds = lance.dataset(DATASET_URI, storage_options=so)
    for col in BTREE_INDEXES:
        ds.create_scalar_index(col, index_type="BTREE");  print(f"  BTREE ✓ {col}")
    for col in BITMAP_INDEXES:
        ds.create_scalar_index(col, index_type="BITMAP"); print(f"  BITMAP ✓ {col}")

    ds = lance.dataset(DATASET_URI, storage_options=so)
    committed = ds.count_rows()
    if committed != metrics["rows"]:
        raise RuntimeError(f"gate 14 write-integrity: committed {committed:,} != materialized {metrics['rows']:,}")
    idx = {(i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i))) for i in ds.list_indices()}
    expect = {f"{c}_idx" for c in BTREE_INDEXES + BITMAP_INDEXES}
    if not expect.issubset(idx):
        raise RuntimeError(f"gate 15 indices: missing {sorted(expect - idx)} (have {sorted(idx)})")
    pr = metrics["probe_uei"]
    rt = ds.scanner(columns=["uei", "name_key"], filter=f"uei = '{pr}'").to_table().to_pylist()
    probe_name = next((r["name_key"] for r in rt if r["name_key"]), None)
    if not probe_name:
        raise RuntimeError(f"gate 16 round-trip: probe {pr} → {len(rt)} rows / no name_key")
    t0 = time.monotonic()
    hit = ds.scanner(columns=["uei"], filter=f"name_key = '{probe_name.replace(chr(39), chr(39)*2)}'").to_table().num_rows
    seek_ms = (time.monotonic() - t0) * 1000
    if hit < 1 or seek_ms > SEEK_CEILING_MS:
        raise RuntimeError(f"gate 17 point-seek: {hit} rows in {seek_ms:.0f}ms (>{SEEK_CEILING_MS} ⇒ no index)")
    print(f"post-write gates PASS — committed={committed:,} idx={sorted(idx)} probe={pr} seek={seek_ms:.0f}ms")
except Exception as werr:  # noqa: BLE001
    if v_before is not None:
        try:
            lance.dataset(DATASET_URI, storage_options=so, version=v_before).restore()
        except Exception as rerr:  # noqa: BLE001
            raise RuntimeError(f"ROLLBACK FAILED to v{v_before}: {rerr}; original: {werr}")
        raise RuntimeError(f"write/index/gate failed → rolled back to v{v_before}: {werr}")
    raise RuntimeError(f"failed on net-new dataset (inspect/drop {DATASET_URI}): {werr}")

status = "success"
```

In the `finally`, after `_record_run(...)`, alert on non-success:

```python
if status != "success":
    _alert(f"{FEED} build {status}: {str(error)[:300]}")
```

### 4.10 WS-A — honest dry-run (`plan_sam_pocs`)

Run the **same** gates with the **same** baseline; write nothing. Add the `hqx-postgres` secret (for the baseline read).

```python
@app.function(secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
              timeout=60*60, memory=32768, cpu=8.0)
def plan_sam_pocs() -> dict:
    os.makedirs(SPILL_DIR, exist_ok=True)
    con = _new_con()
    try:
        _t, metrics, sam_label = _materialize(con)
    finally:
        con.close()
    baseline = _prior_success_baseline()
    checks = assert_pre_write_gates(metrics, baseline)
    out = {k: v for k, v in metrics.items() if k != "present_poc_types"}
    return {"sam_label": sam_label, "baseline": baseline, "gates": checks, **out}
```

---

## 5. Gate spec (authoritative)

| # | Phase | Assertion | Constant | On fail |
|---|-------|-----------|----------|---------|
| 1 | pre | `rows ≥ floor` | `POCS_ROW_FLOOR` | abort, no write |
| 2 | pre | `distinct_uei ≥ floor` | `DISTINCT_UEI_FLOOR` | abort |
| 3 | pre | `distinct_cage ≥ floor` | `DISTINCT_CAGE_FLOOR` | abort |
| 4 | pre | both families `> 0` | — | abort |
| 5 | pre | `rows` within ±25% of baseline | `DELTA_GUARD` | abort (skip if no qualified prior) |
| 6 | pre | `poc_rows_v2` within ±25% of baseline | `DELTA_GUARD` | abort — **catches the v2-half collapse** |
| 7 | pre | `distinct_uei` within ±25% of baseline | `DELTA_GUARD` | abort |
| 8 | pre | `unkeyed_rows == 0` | — | abort (invariant) |
| 9 | pre | `null_poc_type == 0` | — | abort (invariant) |
| 10 | pre | mandatory slots ⊆ present | `MANDATORY_SLOTS` | abort (optional slots logged, not gated) |
| 11 | pre | `name_key` fill ≥ floor | `NAME_FILL_MIN` | abort (invariant tripwire, **not** content) |
| 12 | pre | `first_name` alpha-frac ≥ floor | `NAME_ALPHA_MIN` | abort — **positional-offset defense** |
| 13 | pre | `zip5` numeric-frac ≥ floor | `ZIP_NUMERIC_MIN` | abort — **positional-offset defense** |
| 14 | post | committed count == materialized | — | **restore `v_before`** |
| 15 | post | all 6 `*_idx` present | — | restore |
| 16 | post | population probe round-trips to a POC w/ `name_key` | — | restore |
| 17 | post | indexed point-seek ≤ ceiling | `SEEK_CEILING_MS` | restore |

The historical 6.39M partial fails **2** (uei 888k < 1.3M), **6** (v2 −38%), **7** (uei −42%). The 0-row build fails **1, 2, 3, 4**. An offset shift fails **12 & 13**.

---

## 6. Validation harness (prove on scratch BEFORE prod)

Scratch is ledger-isolated automatically: any non-prod `SAM_POCS_LANCE_URI` sets `FEED='sam_pocs_scratch'` and `_prior_success_baseline` scopes to the scratch URI.

```
SCRATCH = s3://data-sink/scratch/sam_pocs_hardening/
```

### 6A. Happy path (scratch)
```
SAM_POCS_LANCE_URI=$SCRATCH modal run pipelines/sam_gov/sam_pocs.py --dry-run
SAM_POCS_LANCE_URI=$SCRATCH modal run pipelines/sam_gov/sam_pocs.py
SAM_POCS_LANCE_URI=$SCRATCH modal run pipelines/sam_gov/sam_pocs.py::verify_sam_pocs
```
Expect: gates 1-13 PASS (dry-run writes nothing); build → write → 6 indices → gates 14-17 PASS → `success`; `sam_label='20260503'`. **Record the observed `name_alpha_frac`/`zip_numeric_frac`** and, if needed, lower `NAME_ALPHA_MIN`/`ZIP_NUMERIC_MIN` a few points below observed before prod.

### 6B. Negative paths (scratch — prove the guard guards)
1. **Pre-write abort:** set `POCS_ROW_FLOOR=99_000_000`, run build → raises `PRE-WRITE GATE FAILED → 1 row floor`; `lance.dataset($SCRATCH).version` **unchanged**. Revert.
2. **Mid-index rollback:** after a clean 6A, inject `"bogus_idx"` into `expect` (gate 15), run → rolls back; log shows `rolled back to v{N}`; `$SCRATCH` version returns to pre-write. Revert.
3. Capture both (raised string + unchanged/restored version) as PR evidence.

### 6C. Pure unit test — `pipelines/sam_gov/tests/test_sam_pocs_gates.py`
(Create `pipelines/sam_gov/tests/__init__.py`; mirror `apps/catalyst_api/tests/`. Runner: `python -m pytest pipelines/sam_gov/tests/test_sam_pocs_gates.py`.) Import `assert_pre_write_gates` + constants. Assert:
- healthy metrics + healthy baseline → 13 lines, no raise.
- **the historical partial** `{rows 6_389_167, distinct_uei 888_361, poc_rows_v2 2_696_876, distinct_cage 1_167_571, poc_rows_legacy 3_692_291, …}` vs healthy baseline → **raises** (names gate 2 or 6).
- **offset-shift** fixture (`name_alpha_frac 0.08`, `zip_numeric_frac 0.05`, all counts healthy) → **raises** at gate 12.
- each remaining gate's isolated failure (cage floor, a zero family, `unkeyed_rows=1`, `null_poc_type=1`, missing mandatory slot, rows Δ at ±26%) → raises naming that gate.
- `baseline=None` → gates 5-7 `SKIP`, no raise.

### 6D. Baseline isolation (integration, scratch)
After 6A/6B, assert: `SELECT DISTINCT feed, dataset_uri FROM ops.sam_pocs_runs WHERE recorded_at > <start>` shows scratch rows **only** under `feed='sam_pocs_scratch'`; and a prod-scoped `_prior_success_baseline()` still returns the pre-existing healthy prod success (unchanged by the scratch runs).

### 6E. Prod cutover (only after 6A-6D green)
```
modal deploy pipelines/sam_gov/sam_pocs.py     # REQUIRED — cron runs the DEPLOYED fn
modal run    pipelines/sam_gov/sam_pocs.py     # one manual hardened prod build
modal run    pipelines/sam_gov/sam_pocs.py::verify_sam_pocs
```
**Record the pre-cutover prod version** (`lance.dataset(_PROD_URI).version`) in the PR body first. Then confirm the ledger newest row: `status='success'`, `sam_label='20260503'`, all families within ±25%. **Determinism:** the manual build's `rows_written`/`distinct_uei` equal the next 16:30 cron run's.

---

## 7. Landing (own the lifecycle)

1. `git add pipelines/sam_gov/sam_pocs.py pipelines/sam_gov/tests/ docs/plans/SAM_POCS_HARDENING_BUILD_PLAN.md`
2. Commit (`feat(sam_pocs): fail-safe hardening — per-family Δ-guard, content gates, wrapped rollback, snap-key label, deterministic dedup, failure alert`; close per repo convention).
3. `git push -u origin <branch>`; open PR vs `main` with §6 evidence (incl. pre-cutover version + 6B outcomes).
4. Self-verify (6A-6E green) → `gh pr merge <num> --squash --delete-branch`.
5. `modal deploy pipelines/sam_gov/sam_pocs.py` if not already done in 6E — **the cron is not hardened until this lands.**
6. `git -C /Users/benjamincrane/core-x fetch origin`. Do **not** switch/reset it (it is on `feat/corex-gtm-control-surface`, active WIP); report `main` carries the change.
7. Verify: `git -C /Users/benjamincrane/core-x log origin/main -1 --oneline`.

---

## 8. Safety, invariants, abort

- **Where protection lives:** pre-write gates (1-13) at zero exposure window are the primary defense; post-write gates (14-17) + rollback cover Lance write/index integrity.
- **Exposure window (honest):** overwrite-commit → all indices built → gates pass — **minutes**, dominated by the high-cardinality `name_key`/`last_name` BTREE builds; tolerable for a daily feed, eliminated next cycle (§10).
- **Rollback spans write + indexing + gates;** a failed `restore()` raises a distinct `ROLLBACK FAILED` preserving the original cause.
- **Hard invariant — signature:** `build_sam_pocs(trigger_callback_url=None)`, zero required kwargs (dispatcher spawns `kwargs={}`). Re-confirm vs `src/trigger/sam_pocs.ts` before merge.
- **No schema/ledger-column change;** new metrics are gate-only. Memory envelope unchanged.
- **Idempotent + deterministic** (post-WS-C, reruns are bit-identical).
- **If prod cutover goes wrong:** restore to the recorded pre-cutover version —
  ```python
  import lance
  lance.dataset("s3://data-sink/active/sam_pocs/", storage_options=so, version=N).restore()
  ```

---

## 9. Acceptance checklist

- [ ] Pre-write gates 1-13 + post-write 14-17 wired; `plan_sam_pocs` runs the same gates/baseline.
- [ ] Baseline floor-qualified (`BASELINE_MIN_ROWS`) **and** `dataset_uri`-scoped; scratch tagged `sam_pocs_scratch`.
- [ ] Unit test green: healthy pass; historical partial raises; offset-shift raises; each gate's failure raises; `baseline=None` skips Δ.
- [ ] 6B-1 proves no-overwrite-on-pre-fail; 6B-2 proves restore-on-mid-index-fail.
- [ ] 6D proves scratch did not touch the prod baseline.
- [ ] WS-B: build stamps `20260503`. WS-C: two scratch builds identical. WS-D: a forced failure POSTs an alert when `OPS_ALERT_WEBHOOK` set, no-ops when unset.
- [ ] Signature unchanged (verified vs trigger + dispatcher).
- [ ] 6E green on prod; ledger shows `success` + `20260503`; pre-cutover version recorded.
- [ ] `modal deploy` published (`modal app list` fresh timestamp).
- [ ] PR merged `--squash --delete-branch`; operator checkout fetched; `origin/main` verified.

---

## 10. Next cycles (do NOT do here)

1. **Staging + atomic promote** — write to a staging URI, gate there, promote on PASS; eliminates the §8 destructive window. The natural successor once these gates are proven.
2. **Wire the alert channel** — point `OPS_ALERT_WEBHOOK` at the real Telegram/Slack endpoint (operator config); add a warn-band alert (Δ in the outer half of tolerance) so degraded-but-passing builds still surface.
3. **Single-source `_snap_key_sql`** → `pipelines/sam_gov/reference/sam_labels.py`, imported by `sam_master` + `sam_pocs` (needs the `add_local_python_source` mount; regression-gate `sam_master` still stamps `20260503`).
4. **Promote the re-baseline policy** — schedule a floor review on any ±20% universe shift; the per-family Δ-guard already auto-tracks growth/shrink.
5. **Adjacent SAM cleanups** — `sam_master_contacts` retirement, Gen-A `sam_entity_master` deletion, master→sidecar freshness automation.
