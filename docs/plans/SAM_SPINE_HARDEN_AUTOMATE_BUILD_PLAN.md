# SAM Resolution Spine — Harden + Automate (Build Plan)

**Status:** READY-TO-EXECUTE · **Type:** guardrail hardening + control-plane automation (no schema change)
**Targets:** [`pipelines/sam_gov/sam_master.py`](../../pipelines/sam_gov/sam_master.py) · [`pipelines/sam_gov/sam_normalized_entities.py`](../../pipelines/sam_gov/sam_normalized_entities.py) · new `src/trigger/sam_spine_refresh.ts`
**Proven template:** [`pipelines/sam_gov/sam_pocs.py`](../../pipelines/sam_gov/sam_pocs.py) (the fail-safe pattern this plan generalizes) · [`src/trigger/sam_pocs.ts`](../../src/trigger/sam_pocs.ts) (the Trigger/dispatcher/waitpoint pattern)
**Objective:** Make the name→UEI resolution spine — `sam_master_entities` (+ `sam_master_contacts`, `sam_master_domains`) → `sam_normalized_entities` — **fail-safe** (gate-before-write + rollback) and **automatically refreshed** when `entity_registrations` advances, so the surface every crosswalk loads onto is no longer held together by hand.

---

## 0. Orientation (cold-start — read before touching anything)

You are in **core-x**, the Gen-3 data plane (see [`ARCHITECTURE.md`](../../ARCHITECTURE.md)). Invariants: LanceDB on R2 (`s3://data-sink/active/<dataset>/`, no catalog); 100% DuckDB transform; Trigger.dev v4 owns cadence and spawns deployed Modal functions through the one proxy-authed Universal Dispatcher (waitpoint-token durable callback); every worker writes a terminal-state row to `ops.<feed>_runs` (Postgres `HQX_DB_URL_POOLED`); load-bearing keys get `BTREE`, low-card categoricals `BITMAP`. **Code on `main` is inert until `modal deploy` publishes it.**

### The spine lineage

```
entity_registrations  (Lance, ~19.3M rows, stacked monthly snapshots; real fields in pipe_fields[])
   └─ sam_master.py ──► sam_master_entities   ~1.541M  (1/uei, faithful 142-field dict)        [MANUAL]
                        ├► sam_master_contacts ~4.373M  (≤6/uei POC unpivot, v2-only)
                        └► sam_master_domains  ~0.710M  (normalized_domain → uei)
                              │
        sam_normalized_entities.py ◄──────────┘ reads sam_master_entities
                        └► sam_normalized_entities  ~1.541M  (1/uei, core.name_norm blocking keys)  [MANUAL]
                              └──► consumed by pipelines/resolution/crosswalk_sos_sam.py (and every name→UEI bridge)
```

### Current state (what is and isn't true today)

| | rollback | per-family Δ-guard | floor-qualified URI-scoped baseline | content gate | deterministic dedup | dataset_uri threaded | ops-alerts | Trigger task |
|---|---|---|---|---|---|---|---|---|
| `sam_pocs` (reference) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ (#206) | ✅ daily |
| **`sam_master`** | ❌ none | ❌ | ❌ | ❌ | ❌ (date-tie nondeterministic) | ❌ | ✅ (#206) | ❌ |
| **`sam_normalized`** | ⚠️ partial (write/index outside the guard) | ❌ single scalar | ❌ | ❌ | n/a (1:1 passthrough) | ❌ | ✅ (#206) | ❌ |

`sam_master` has only an entities row-floor check ([`sam_master.py:300`](../../pipelines/sam_gov/sam_master.py)) and writes 3 datasets in a loop ([`sam_master.py:318`](../../pipelines/sam_gov/sam_master.py)) with **no rollback, no Δ-guard, no content gate, no callback plumbing**. `sam_normalized` has gates 1-10 but ships the **cold-seek false-positive** (`if hit < 1 or seek_ms > 2000: raise`, [`sam_normalized_entities.py:420`](../../pipelines/sam_gov/sam_normalized_entities.py)), a **hardcoded KIPPER probe**, **absolute cardinality targets**, a **single scalar Δ-guard**, and **write/index outside the rollback try**.

### Why this is the load-bearing cycle

The spine is the platform's name→UEI surface; `crosswalk_sos_sam` and every bridge resolve against it. Today both workers are **manually refreshed with no automated propagation** from `entity_registrations` — the same silent-staleness class closed for the POC layer, on the heavier surface. Automating them while `sam_master` has no rollback and `sam_normalized` carries the cold-seek bug would convert a manual-but-safe state into an **unattended-and-unsafe** one. So: **harden first, automate last.**

---

## 1. Objective & Operational Definition of Done

Done when **all** are true and demonstrated:

1. **`sam_master` fail-safe.** Pre-write gates abort before any of the 3 writes; a failure during write/index/post-gates restores **all three** datasets to their pre-write versions. Proven on a scratch prefix.
2. **`sam_normalized` fail-safe and cold-seek-immune.** Gate 10 no longer gates on seek latency; a forced prod-scale cold seek does **not** roll back a good build; write+index+gates are under one rollback guard.
3. **No false-fail probes.** Both workers derive the round-trip probe from materialized data (no hardcoded UEI).
4. **Baseline discipline.** Both use a floor-qualified, `dataset_uri`-scoped per-family Δ-guard; a degraded success can neither ratchet the baseline nor be supplied by a scratch run (scratch tags `<feed>_scratch`).
5. **Content gates.** A positional-offset regression (NAICS where a name belongs, etc.) fails a pre-write content gate in both workers.
6. **Deterministic `sam_master`.** Two consecutive scratch builds over a fixed source produce identical `entities_rows`/`contacts_rows`/`domains_rows`/`distinct_uei`.
7. **Scratch isolation actually isolates.** Validation runs target a scratch prefix via a threaded `dest_prefix`/`dataset_uri` param (not a container env var) and leave prod datasets and the prod baseline untouched.
8. **Automated refresh.** A Trigger orchestrator runs on cadence, rebuilds the spine **only when `entity_registrations` has advanced**, runs `sam_master` then (on a real rebuild) `sam_normalized`, and no-ops when current. Both workers self-skip when current.
9. **Alerting verified live.** A forced failure in each worker delivers a Telegram alert (in-container `OPS_ALERT_WEBHOOK` present).
10. **Deployed + green once.** All workers `modal deploy`-ed; one manual prod rebuild of each is green; ledgers show `success`; the orchestrator dry-run chains correctly.
11. **Landed + reconciled.** Each PR merged to `main` (linear from `origin/main`); `modal deploy` published; operator checkout `/Users/benjamincrane/core-x` (on `main`) fast-forwarded on disk; `git log -1` verified.

---

## 2. Scope — one cycle, three workstreams, **strictly sequenced** (one PR each, in order)

| WS | PR | Change | Gate to next |
|----|----|--------|--------------|
| **A** | 1 | Harden `sam_master`: pre-write gates (floors + per-family Δ + content + invariants), post-write gates (correctness, **not** timing) + 3-dataset rollback, floor-qualified URI-scoped scratch-isolated baseline, deterministic dedup, `dest_prefix` threading, `skip_if_current`, callback plumbing | scratch + prod rebuild green |
| **B** | 2 | Upgrade `sam_normalized`: **fix gate-10 cold-seek**, population probe, per-family floor-qualified URI-scoped baseline (replace absolute targets), content gate, rollback wraps write+index, `dataset_uri` threading, `skip_if_current` | scratch + prod rebuild green |
| **C** | 3 | Automate: `src/trigger/sam_spine_refresh.ts` orchestrator (freshness-gated, concurrency 1) chaining master→normalized via dispatcher + waitpoints | both workers deployed & green first |

**WS-C lands only after A and B are merged, deployed, and prod-verified.** Automating an un-hardened worker is the one thing this plan must not do.

### Out of scope
`sam_master_contacts` retirement; Gen-A `sam_entity_master.py` deletion; staging/atomic-promote (the residual overwrite window stays, bounded by rollback); warn-band alerts; non-SAM feed alerting; `core.person_name_norm`/`sam_normalized_pocs`. The single-source `_snap_key_sql` consolidation (still duplicated across `sam_master`/`sam_pocs`) is deferred.

### Residual risk accepted
`mode="overwrite"` retained on all spine datasets. The destructive window (overwrite → index → gate, minutes — dominated by high-cardinality BTREE builds, and **longer for `sam_master`'s 3 datasets**) is bounded by the rollback. Staging/promote eliminates it next cycle.

---

## 3. Learnings → concrete decisions (baked in from the `sam_pocs` execution)

Every row is a thing that actually bit us or that the diagnostic proved. Each maps to a specific decision here.

| # | Learning (observed) | Decision in this plan | Applies to |
|---|---|---|---|
| L1 | **Cold-seek timing false-positive.** Gate 17 false-failed the `sam_pocs` prod cutover at **4344ms** (scratch was 1.1-1.4s) and rolled back a good build. | Post-write index proof = **(a)** index present in `list_indices` **+ (b)** round-trip returns the known-present probe (`hit ≥ 1`). **Never gate on seek latency** — log it (WARN above a threshold). | A (new post-write gates); **B (delete `seek_ms > 2000` at line 420 — same bug)** |
| L2 | **`dataset_uri` is read at module import *in the container*; a locally-set env never reaches it** — so scratch redirection silently wrote prod, and `_record_run` stamped the prod URI onto scratch runs. | Thread the write target **explicitly** as a function param (local entrypoint → param → remote). `sam_master`: `dest_prefix` → derive the 3 URIs. `sam_normalized`: `dataset_uri`. `_record_run`/baseline/feed all use the **effective** URI. Never read `SAM_*_LANCE_URI` in the build body. | A, B |
| L3 | **Baseline ratchet.** A degraded `success` became the Δ baseline and rejected the genuine recovery (+26% > ±25%). | Baseline query is **floor-qualified**: `WHERE status='success' AND rows_written >= BASELINE_MIN_ROWS` (a clearly-healthy floor, **above** the abort floor). | A, B |
| L4 | **Scratch poisons prod baseline** (no URI scoping; feed tag not URI-derived). | Feed tag derived from the effective URI (`<feed>` for prod, `<feed>_scratch` otherwise); baseline query scoped `AND dataset_uri = <prod_uri>`. | A, B |
| L5 | **Scalar Δ-guard misses a half-collapse** (the 6.39M v2-half partial passed a total-rows ±25% guard). | **Per-family Δ-guards.** `sam_master`: `entities_rows`, `contacts_rows`, `domains_rows`, `distinct_uei`. `sam_normalized`: `rows`, `distinct_normalized_name`. | A, B |
| L6 | **Absolute cardinality targets drift** (`NORM_DISTINCT_TARGET = 1,466,764 ±5%` ages out as SAM grows/shrinks). | Replace absolute targets with **relative-to-prior** per-family Δ-guard (auto-tracks). Keep an absolute value only as a coarse collapse **floor**. | B (gate 5 at line 239) |
| L7 | **Count/fill gates miss positional-offset corruption** (shifted `pipe_fields` index → wrong-but-non-null values pass every count gate). | **Content-plausibility gate.** `sam_master` lifts NAICS/cage/name positionally → assert `primary_naics` numeric-frac and `legal_business_name` alpha-frac. `sam_normalized` → `normalized_legal_name` alpha-frac. | A, B |
| L8 | **Hardcoded probe (KIPPER) can false-fail** if that UEI lacks the queried row (different population). | **Population-derived probe** emitted from `_materialize` (`arg_max(uei, …)` or first non-null-key row). Drop the hardcoded `KIPPER_UEI` hard gate. | A, B (replaces lines 78/408-410) |
| L9 | **Rollback didn't wrap indexing** (mid-index OOM leaves a fully-overwritten, under-indexed dataset live, recorded `error`, no rollback). | Rollback guard wraps **write + indexing + post-write gates**. `sam_master`: capture `v_before` for **all 3** datasets; on any failure restore **all 3**; a failed `restore()` raises a distinct loud error. | A; **B (move write/index at 391-394 inside the gate-try)** |
| L10 | **Nondeterministic dedup tiebreak** → ±50-row jitter across identical-source rebuilds. | Add a deterministic final tiebreak (`source_file DESC`) to `sam_master`'s latest-per-uei `ORDER BY`. | A |
| L11 | **#203 silent secret.** Workers called `alert(msg)` but the `ops-alerts` secret was never attached → silent no-op. | Any worker/function that calls `alert(msg)` **attaches `modal.Secret.from_name("ops-alerts")`**, and we **prove in-container** `OPS_ALERT_WEBHOOK` is present before declaring alerting live. (Both build fns already attach it via #206 — keep; the staleness path inherits it.) | A, B, C |
| L12 | **Orphan-history squash conflicts** (#196, #205 add/add). | Each PR **branches fresh from `origin/main`**; one workstream per PR; linear history; never carry a prior orphan commit. | A, B, C |
| L13 | **`modal deploy` required** for the cron/dispatcher path (manual `modal run` uses the local mount). | Deploy each worker after merge; the orchestrator spawns the **deployed** `build_sam_master`/`build_sam_normalized_entities`. | A, B, C |
| L14 | **merged ≠ done.** | After each merge, fast-forward `/Users/benjamincrane/core-x` (now on `main`) on disk; verify `git log -1`. | A, B, C |
| L15 | **Prod-scale surfaces what scratch hides** (the 4344ms seek only appeared on prod). | A manual **prod** rebuild of each worker is a hard gate before WS-C; scratch is necessary but not sufficient for latency/scale-sensitive behavior. | A, B |
| L16 | **Trigger waitpoint/dispatcher pattern** (`sam_pocs.ts`). | The orchestrator reuses the proxy-authed dispatcher + `wait.createToken()`/`wait.forToken()`. `sam_master` **gains callback plumbing** (`trigger_callback_url` param + `_post_callback`), which it currently lacks; `sam_normalized` already has it. | A (add), C |

---

## 4. WS-A — Harden `sam_master` (the heaviest: 3 datasets, zero current rollback)

### 4.1 Threading & feed (L2, L4)
- Add `dest_prefix: str | None = None` to `build_sam_master`. `prefix = dest_prefix or "s3://data-sink/active/"`. Derive `entities_uri = prefix + "sam_master_entities/"`, `contacts_uri`, `domains_uri`. `feed = "sam_master" if prefix == "s3://data-sink/active/" else "sam_master_scratch"`.
- Local entrypoint reads `os.environ.get("SAM_MASTER_DEST_PREFIX")` and passes it as `dest_prefix` (None → prod). `_record_run` and the baseline query use `feed` + `entities_uri`.

### 4.2 Single-pass metrics (extend the existing count, add content + probe) (L7, L8)
In the build, after materializing `entities`/`contacts`/`domains` Arrow tables, compute on `entities` (one pass; the satellites are already counted):
```python
# over the entities Arrow table via DuckDB or pyarrow compute:
metrics = {
  "entities_rows": entities.num_rows, "contacts_rows": contacts.num_rows,
  "domains_rows": domains.num_rows, "distinct_uei": <distinct uei>,
  # content plausibility (positional-offset defense):
  "naics_numeric_frac": <frac primary_naics matching ^[0-9]{2,6}$ over non-null primary_naics>,
  "name_alpha_frac":    <frac legal_business_name matching [A-Za-z] over non-null>,
  # probe: a uei present in entities AND contacts (drives the post-write round-trip):
  "probe_uei": <a uei known to have ≥1 contact row, e.g. most-contacts uei>,
}
```
> Compute these in the same DuckDB connection that builds the tables (cheap aggregates over the in-memory relations), not a re-scan of R2.

### 4.3 Pre-write gates (new `assert_pre_write_gates(metrics, baseline)`) (L3, L5, L7)
Pure function, unit-tested. Raises before any write:
1. `entities_rows >= ENTITIES_ROW_FLOOR` (1_400_000, exists)
2. `distinct_uei == entities_rows` (uniqueness — exists)
3. `contacts_rows >= CONTACTS_FLOOR` (3_500_000) · 4. `domains_rows >= DOMAINS_FLOOR` (500_000)
5-7. **per-family Δ** (skip if no floor-qualified prior): `entities_rows`, `contacts_rows`, `domains_rows` each within ±`DELTA_GUARD` (0.25) of the prior `sam_master_runs` success
8. `naics_numeric_frac >= NAICS_NUMERIC_MIN` (0.95) · 9. `name_alpha_frac >= NAME_ALPHA_MIN` (0.95)
> Floors/fractions: baseline from the first clean scratch run (§7), set floors ~25% below live (`contacts ~4.37M`, `domains ~0.71M`), content floors a few points below observed.

### 4.4 Floor-qualified, URI-scoped baseline (L3, L4)
```python
def _prior_success_baseline(entities_uri: str) -> dict | None:
    # SELECT entities_rows, contacts_rows, domains_rows, distinct_uei
    # FROM ops.sam_master_runs
    # WHERE status='success' AND <dataset_uri scoping> AND entities_rows >= BASELINE_MIN_ENTITIES (1_450_000)
    # ORDER BY recorded_at DESC LIMIT 1
```
> **`ops.sam_master_runs` has no `dataset_uri` column today.** Add it (idempotent `ALTER TABLE ... ADD COLUMN IF NOT EXISTS dataset_uri text`) in the worker's `OPS_DDL`, and write `entities_uri` into it. Scope the baseline by it. (This is the L4 fix; without the column, scratch runs poison the prod baseline exactly as they did for `sam_pocs` pre-fix.)

### 4.5 Deterministic dedup (L10)
In the latest-per-uei window (`sam_master.py` `latest` CTE), extend the `ORDER BY` with `, source_file DESC NULLS LAST` after the existing `last_update_date`/`initial_registration_date`/snap-key keys (a uei is unique within one `source_file`). Confirm `source_file` is in the build scan.

### 4.6 Write + index + post-write gates under ONE 3-dataset rollback guard (L1, L9)
```python
# capture pre-write versions for ALL THREE (None for net-new)
v_before = {}
for name, uri in (("entities", entities_uri), ("contacts", contacts_uri), ("domains", domains_uri)):
    try: v_before[name] = lance.dataset(uri, storage_options=so).version
    except Exception: v_before[name] = None

try:
    for table, uri, btree in ((entities, entities_uri, ENTITIES_BTREE), (contacts, contacts_uri, CONTACTS_BTREE), (domains, domains_uri, DOMAINS_BTREE)):
        lance.write_dataset(table, uri, mode="overwrite", data_storage_version=DATA_STORAGE_VERSION,
                            max_rows_per_file=MAX_ROWS_PER_FILE, storage_options=so)
        d = lance.dataset(uri, storage_options=so)
        for col in btree:
            if col in set(d.schema.names): d.create_scalar_index(col, index_type="BTREE")
    # post-write gates (correctness, NOT timing — L1):
    ent = lance.dataset(entities_uri, storage_options=so)
    if ent.count_rows() != metrics["entities_rows"]: raise RuntimeError("gate: entities write-integrity")
    if not {f"{c}_idx" for c in ENTITIES_BTREE}.issubset({_name(i) for i in ent.list_indices()}): raise RuntimeError("gate: entities indices")
    # round-trip the population probe across entities + contacts; assert hit>=1 (log seek, do not gate it):
    pr = metrics["probe_uei"]
    if ent.scanner(columns=["uei"], filter=f"uei='{pr}'").to_table().num_rows < 1: raise RuntimeError("gate: entities probe")
    con_ds = lance.dataset(contacts_uri, storage_options=so)
    if con_ds.scanner(columns=["uei"], filter=f"uei='{pr}'").to_table().num_rows < 1: raise RuntimeError("gate: contacts probe")
except Exception as werr:
    failures = []
    for name, uri in (("entities", entities_uri), ("contacts", contacts_uri), ("domains", domains_uri)):
        if v_before[name] is not None:
            try: lance.dataset(uri, storage_options=so, version=v_before[name]).restore()
            except Exception as rerr: failures.append(f"{name}->v{v_before[name]}: {rerr}")
    if failures: raise RuntimeError(f"ROLLBACK FAILED: {failures}; original: {werr}")
    raise RuntimeError(f"write/index/gate failed → rolled back all datasets: {werr}")
```
> Pre-write gates already ran on the in-memory tables, so most corruption aborts at zero exposure. The 3-dataset rollback covers the genuine Lance write/index integrity faults across the family.

### 4.7 `skip_if_current` + callback plumbing (L16, automation prep)
- Add `skip_if_current: bool = True`. Cheaply resolve the latest `entity_registrations` v2 label (the worker already does this) **and** read the current `sam_master_entities` `sam_extract_label` (one value). If equal → return `{"status": "skipped", "label": ...}` **before** the expensive materialize. (Net-new / missing target → not current → proceed.)
- Add `trigger_callback_url: str | None = None` + a `_post_callback(url, payload)` (mirror `sam_pocs`/`sam_normalized`) and POST `{status, label, entities_rows, ...}` on terminal state. `sam_master` lacks this today; the orchestrator needs it.

---

## 5. WS-B — Upgrade `sam_normalized` (fix the cold-seek bug + baseline discipline)

> `sam_normalized` is a 1:1 passthrough of `sam_master_entities` (no dedup), and already has gates 1-10, `_within`, `_prior_success_rows`, callback plumbing. This is a **targeted upgrade**, not a rewrite.

### 5.1 Fix gate 10 — the cold-seek false-positive (L1) — **the highest-priority line in this cycle**
[`sam_normalized_entities.py:420`](../../pipelines/sam_gov/sam_normalized_entities.py):
```python
# BEFORE — false-fails + rolls back on a slow cold R2 seek (the sam_pocs prod bug):
if hit < 1 or seek_ms > 2000:
    raise RuntimeError(f"gate 10 point-lookup: {hit} rows in {seek_ms:.0f}ms (>2000ms ⇒ no index)")
# AFTER — correctness only; latency logged, not gated:
if hit < 1:
    raise RuntimeError("gate 10 name index: lookup of a known-present name returned 0 rows")
_slow = "" if seek_ms <= SEEK_WARN_MS else f"  [WARN cold seek >{SEEK_WARN_MS}ms]"
```

### 5.2 Population-derived probe (L8) — replace hardcoded KIPPER
Drop `KIPPER_UEI` (line 78) as a hard gate. `_materialize` emits `probe_uei` (a uei with a non-null `normalized_legal_name`, e.g. first such row); gates 9-10 round-trip **that**. (Keep KIPPER only as an optional informational log if desired.)

### 5.3 Per-family, floor-qualified, URI-scoped baseline (L3, L4, L5, L6)
- `_prior_success_rows` → `_prior_success_baseline(dataset_uri)` returning `{rows_written, distinct_normalized_name}`, with `AND dataset_uri = %s AND rows_written >= BASELINE_MIN_ROWS` (1_450_000). (`ops.sam_normalized_entities_runs` already has `dataset_uri` — scope on it.)
- Replace gate 5's **absolute** `NORM_DISTINCT_TARGET` / `BASE_DISTINCT_TARGET` ±5% with: a coarse **floor** (`distinct_normalized_name >= NORM_FLOOR` 1_300_000) **plus** a per-family **Δ-guard** on `distinct_normalized_name` (±25% vs the floor-qualified baseline). Gate 7 stays as the `rows` Δ-guard. This auto-tracks growth/shrink instead of aging out.

### 5.4 Content gate (L7)
Add `normalized_legal_name` alpha-frac ≥ `NAME_ALPHA_MIN` (0.95) to the pre-write gates (a positional/normalization regression that produced non-alpha keys fails before write).

### 5.5 Rollback wraps write + indexing (L9)
Move the `write_dataset` + `create_scalar_index` block (lines ~380-394) **inside** the same `try` that runs gates 8-10 and calls `restore(v_before)`; on a failed `restore()` raise a distinct loud error. (Today write/index sit before the rollback try — a mid-index failure leaks an under-indexed dataset.)

### 5.6 Threading + skip_if_current (L2, automation prep)
- Thread `dataset_uri: str | None = None` (effective `uri = dataset_uri or DATASET_URI`); `feed` derived (`sam_normalized_entities` / `_scratch`); `_record_run`/baseline use the effective URI. Local entrypoint passes `os.environ.get("SAM_NORMALIZED_ENTITIES_URI")`.
- Add `skip_if_current: bool = True`: compare the source `sam_master_entities` max `sam_extract_label` to this dataset's current `sam_extract_label`; equal → `{"status":"skipped"}` before materialize.

---

## 6. WS-C — Automate the refresh (Trigger orchestrator; freshness-gated)

**Design (strong engineering decision):** idempotent **self-skipping workers** + a **thin freshness-gated orchestrator**, not a hard monthly cron. The orchestrator runs daily (cheap when nothing changed) and self-adjusts to whenever the SAM drop actually lands.

### 6.1 `src/trigger/sam_spine_refresh.ts`
- `schedules.task({ cron: { pattern: "0 18 * * *", timezone: "UTC" }, queue: { concurrencyLimit: 1 }, ... })` — daily, **after** `sam_pocs` (16:30) and the drop window; concurrency 1 so a long rebuild never overlaps the next fire.
- Step 1: dispatch `build_sam_master` with `kwargs:{ skip_if_current: true }` + a fresh waitpoint token; `await wait.forToken(...)`.
  - callback `status==="skipped"` → log "spine current" and **return** (no normalized run).
  - `status!=="success"` → `throw` (Trigger surfaces failure; the worker already Telegram-alerts).
- Step 2 (only if master rebuilt): dispatch `build_sam_normalized_entities` with `kwargs:{ skip_if_current: true }` + waitpoint; `await`. `skipped`/`success` → done; else `throw`.
- Payload/secrets identical to `sam_pocs.ts` (`MODAL_DISPATCHER_URL`, `MODAL_KEY`/`MODAL_SECRET`). `dirs: ["./src/trigger"]` already picks it up.

> The dispatcher resolves `modal.Function.from_name("sam-gov-master-pipelines","build_sam_master")` and `("sam-gov-normalized-entities-pipelines","build_sam_normalized_entities")` — both must be **deployed** (WS-A/B) before this task is enabled. The `skip_if_current` self-skip is the idempotency guard; concurrency 1 is the overlap guard. No separate staleness function or ledger lock needed.

> **If an `entity_registrations` ingest Trigger task already exists**, the orchestrator MAY additionally be invoked from its completion callback for event-driven freshness — but the daily freshness-gated schedule stays as the robust floor.

---

## 7. Validation harness (scratch isolation that actually isolates — L2)

Scratch prefix: `s3://data-sink/scratch/sam_spine/`. Because URIs are **threaded** (WS-A `dest_prefix`, WS-B `dataset_uri`), the redirect reaches the container and the feed tags `*_scratch`; the prod baseline query (scoped to the prod URI) is never touched.

### 7A. Unit tests (pure, no R2) — `pipelines/sam_gov/tests/test_sam_master_gates.py`, extend `test_sam_pocs_gates.py` patterns
- `sam_master.assert_pre_write_gates`: healthy pass; a contacts-half-collapse caught by the **per-family** Δ (not the entities floor); an offset-shift fixture (`naics_numeric_frac`/`name_alpha_frac` low) raises; `baseline=None` skips Δ.
- `sam_normalized.assert_pre_write_gates`: healthy pass; `distinct_normalized_name` collapse caught by the per-family Δ (not the absolute target); non-alpha keys raise content gate; baseline floor-qualification (a sub-floor success is **not** the baseline).

### 7B. Scratch builds (Modal, prefix-threaded)
```
SAM_MASTER_DEST_PREFIX=s3://data-sink/scratch/sam_spine/  modal run pipelines/sam_gov/sam_master.py            # build #1
SAM_MASTER_DEST_PREFIX=s3://data-sink/scratch/sam_spine/  modal run pipelines/sam_gov/sam_master.py            # build #2 → determinism (identical 4 counts)
SAM_NORMALIZED_ENTITIES_URI=s3://data-sink/scratch/sam_spine/sam_normalized_entities/  modal run pipelines/sam_gov/sam_normalized_entities.py
```
Expect: gates pass; `sam_master` 3 datasets written+indexed; post-write probe round-trips; **build #2 counts == build #1** (determinism); feed rows tagged `*_scratch`. Capture `naics_numeric_frac`/`name_alpha_frac` and confirm the 0.95 floors.

### 7C. Negative paths (scratch — prove the guard guards)
1. **`sam_normalized` cold-seek no longer false-fails:** there is no longer a path that rolls back on latency — assert by code review + a build whose logged `seek_ms` exceeds `SEEK_WARN_MS` still returns `success`.
2. **`sam_master` mid-index rollback:** inject a bogus expected index on the **contacts** dataset → build → all 3 datasets restore to `v_before` (versions unchanged), terminal `error` recorded under `sam_master_scratch`. Revert.
3. **Baseline isolation:** after the scratch builds, the prod-scoped baseline query for each worker still returns the pre-existing **prod** success (scratch rows live only under `*_scratch`).

### 7D. Prod cutover (only after 7A-7C green; the L15 hard gate)
Record each dataset's pre-cutover version; `modal deploy` then one manual prod rebuild of `sam_master`, then `sam_normalized`; confirm ledgers `success`, labels advance together, families within range, **and `sam_normalized`'s logged prod seek (which exposed the 4344ms bug) no longer rolls back**.

### 7E. Alerting (L11) + automation dry-run (L13)
- Force one failure in each worker (high floor) → confirm in-container `OPS_ALERT_WEBHOOK` present and Telegram delivers.
- With both deployed, trigger the orchestrator once manually: on a current spine it logs "spine current" and no-ops; force-stale (or `skip_if_current:false`) → it chains master→normalized to green.

---

## 8. Landing (per-PR, linear from `origin/main` — L12, L13, L14)

For **each** workstream PR, in order A → B → C:
1. `git fetch origin && git checkout -b claude/<name> origin/main` (fresh, no orphan commit).
2. Commit (close per repo convention) → push → PR vs `main` with §7 evidence → self-verify → `gh pr merge --squash --delete-branch`.
3. `modal deploy` the touched worker(s) (the cron/dispatcher path is inert until deployed).
4. `git -C /Users/benjamincrane/core-x pull --ff-only` (it is on `main`) → verify `git log -1 --oneline` shows the merge on disk.
5. Only after A and B are deployed + prod-verified, land C and **enable** the orchestrator.

---

## 9. Safety, invariants, abort

- **Protection lives pre-write** (gates on in-memory tables, zero exposure). Post-write gates + rollback cover Lance write/index integrity only. **No gate asserts seek latency** (L1).
- **`sam_master` atomicity:** all 3 Arrow tables materialize and pass pre-write gates before any write; rollback restores all 3. A partial-family state is never left committed.
- **Idempotent + self-skipping:** `skip_if_current` makes daily orchestration cheap; `mode="overwrite"` + deterministic dedup make reruns bit-stable.
- **Signatures:** new params are all optional with prod-safe defaults (`dest_prefix=None`, `dataset_uri=None`, `skip_if_current=True`, `trigger_callback_url=None`) — the dispatcher path (`kwargs` it sets) stays valid.
- **Abort:** each dataset is recoverable via Lance time-travel; record pre-cutover versions before 7D. The orchestrator is disabled until A+B are proven.

---

## 10. Acceptance checklist
- [ ] `sam_master`: pre-write gates (floors + per-family Δ + content + uniqueness), 3-dataset rollback wrapping write+index+gates, floor-qualified URI-scoped baseline (with new `dataset_uri` ledger column), deterministic dedup, `dest_prefix` threading, `skip_if_current`, callback plumbing, population probe.
- [ ] `sam_normalized`: **gate-10 cold-seek removed**, population probe, per-family floor-qualified URI-scoped baseline (absolute targets retired to floors), content gate, rollback wraps write+index, `dataset_uri` threading, `skip_if_current`.
- [ ] Unit tests green for both (incl. half-collapse + offset-shift + baseline-floor fixtures).
- [ ] Scratch: determinism (`sam_master` build#1==#2); mid-index rollback restores all 3; cold-seek build still `success`; baseline isolation (prod untouched, `*_scratch` tagged).
- [ ] Prod rebuild of each green; labels advanced; `sam_normalized` prod seek logged-not-gated.
- [ ] Both deployed; forced failure → Telegram alert (in-container webhook present).
- [ ] Orchestrator: no-ops when current; chains master→normalized when stale; concurrency 1.
- [ ] All 3 PRs linear from `origin/main`, squash-merged, branches deleted; operator checkout fast-forwarded on disk; `git log -1` verified.

---

## 11. Next cycles (do NOT do here)
1. **Staging + atomic promote** for the spine (eliminate the overwrite window; `sam_master`'s 3-dataset window is the largest in the fleet).
2. **Warn-band alerts** (Δ in the outer half of tolerance) + **non-SAM feed alerting** (each needs the `core.ops_alert` + `ops-alerts` pattern).
3. **Single-source `_snap_key_sql`** → `pipelines/sam_gov/reference/sam_labels.py`, imported by `sam_master`/`sam_pocs` (with the `add_local_python_source` mount).
4. **`sam_master_contacts` retirement** (redundant with `sam_pocs` v2 half) and **Gen-A `sam_entity_master.py` deletion**.
5. **`core.person_name_norm` + `sam_normalized_pocs`** once a committed person-bridge consumer exists.
