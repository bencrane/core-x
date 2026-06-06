# SAM Resolution Spine — Harden + Automate (Build Plan)

**Status:** READY-TO-EXECUTE · **Type:** guardrail hardening + dispatcher-readiness + control-plane automation (no dataset schema change)
**Targets:** [`pipelines/sam_gov/sam_master.py`](../../pipelines/sam_gov/sam_master.py) · [`pipelines/sam_gov/sam_normalized_entities.py`](../../pipelines/sam_gov/sam_normalized_entities.py) · new [`pipelines/sam_gov/reference/sam_labels.py`](../../pipelines/sam_gov/reference/) · new `src/trigger/sam_spine_refresh.ts`
**Proven template:** [`pipelines/sam_gov/sam_pocs.py`](../../pipelines/sam_gov/sam_pocs.py) (the fail-safe pattern this generalizes) · [`src/trigger/sam_pocs.ts`](../../src/trigger/sam_pocs.ts) (Trigger/dispatcher/waitpoint)
**Objective:** Make the name→UEI resolution spine — `sam_master_entities` (+ `sam_master_contacts`, `sam_master_domains`) → `sam_normalized_entities` — **fail-safe** (gate-before-write + rollback), **dispatcher-spawnable**, and **automatically refreshed** when `entity_registrations` advances, so the surface every crosswalk loads onto is no longer held together by hand.

---

## 0. Orientation (cold-start — read before touching anything)

**core-x** Gen-3 plane ([`ARCHITECTURE.md`](../../ARCHITECTURE.md)): LanceDB on R2 (`s3://data-sink/active/<dataset>/`, no catalog); 100% DuckDB transform; Trigger.dev v4 owns cadence and spawns **deployed** Modal functions through the one proxy-authed Universal Dispatcher, which calls `fn.spawn(**kwargs, trigger_callback_url=...)` ([`core/modal_dispatcher.py:53`](../../core/modal_dispatcher.py)) — **so a dispatched function must build everything it needs from `kwargs` + in-container state; it cannot receive locally-constructed arguments.** Every worker writes a terminal row to `ops.<feed>_runs`; load-bearing keys get `BTREE`, low-card `BITMAP`. **Code on `main` is inert until `modal deploy`.**

### Lineage
```
entity_registrations  (Lance, ~19.3M rows, stacked monthly snapshots; real fields in pipe_fields[])
   └─ sam_master.py ──► sam_master_entities   ~1.541M  (1/uei, faithful 142-field dict)        [MANUAL]
                        ├► sam_master_contacts ~4.373M  (≤6/uei POC unpivot, v2-only)
                        └► sam_master_domains  ~0.710M  (normalized_domain → uei)
                              │
        sam_normalized_entities.py ◄──────────┘ reads sam_master_entities
                        └► sam_normalized_entities  ~1.541M  (1/uei, core.name_norm keys)  [MANUAL]
                              └──► consumed by pipelines/resolution/crosswalk_sos_sam.py (and every name→UEI bridge)
```

### Current state (verified against code + live `ops.*` ledger, 2026-06-06)

| | rollback | per-family Δ | floor-qualified URI-scoped baseline | content gate | deterministic dedup | dispatcher-ready | `dataset_uri` ledger col | callback | Trigger task |
|---|---|---|---|---|---|---|---|---|---|
| `sam_pocs` (reference) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ daily |
| **`sam_master`** | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ **needs `sql` built locally** | ❌ (add) | ❌ | ❌ |
| **`sam_normalized`** | ⚠️ write/index outside the guard | ❌ scalar only | ❌ | ❌ | n/a (1:1) | ✅ | ✅ already | ✅ | ❌ |

Load-bearing facts (verified): `sam_master` has only the entities row-floor + uniqueness + non-empty-satellite gates ([`sam_master.py:300`](../../pipelines/sam_gov/sam_master.py)), writes 3 datasets in a bare loop ([`:318`](../../pipelines/sam_gov/sam_master.py)) with no `v_before`, **takes a required `sql: dict` built locally in the entrypoint** ([`:249`](../../pipelines/sam_gov/sam_master.py), [`:352`](../../pipelines/sam_gov/sam_master.py)) from a field map **not mounted in the container** (image adds only `core.ops_alert`, [`:81`](../../pipelines/sam_gov/sam_master.py)), has **no `trigger_callback_url`**, **no `init_ops`**, and **no canonical `.sql` sidecar** (DDL inline only). `sam_normalized` ships the cold-seek false-positive at [`:420`](../../pipelines/sam_gov/sam_normalized_entities.py) (`if hit < 1 or seek_ms > 2000: raise`), a hardcoded `KIPPER_UEI` ([`:78`](../../pipelines/sam_gov/sam_normalized_entities.py)), absolute cardinality targets ([`:72-73`](../../pipelines/sam_gov/sam_normalized_entities.py), pinned to today's exact 1,466,764 / 1,450,598), a single scalar Δ-guard, and write/index **outside** the rollback try ([`:382-395`](../../pipelines/sam_gov/sam_normalized_entities.py)). `ops.sam_master_runs` has **no `dataset_uri` column and exactly 1 success row**; `ops.sam_normalized_entities_runs` already has `dataset_uri`. `entity_registrations_backfill.ts` exists but is a **manual, cron-less** task (no recurring completion event). Live values: entities 1,541,566 · contacts 4,373,319 · domains 709,546 · norm-distinct 1,466,764 · base-distinct 1,450,598.

### Why this is the load-bearing cycle, and the order
The spine is the platform's name→UEI surface; `crosswalk_sos_sam` and every bridge resolve against `sam_normalized_entities`. Both workers are manually refreshed with no automated propagation — the silent-staleness class closed for the POC layer, on the heavier surface. **Harden + make dispatcher-ready first, automate last.** Automating an un-hardened or un-dispatchable spine is strictly worse than the current manual-but-safe state.

---

## 1. Operational Definition of Done

1. **`sam_master` fail-safe + dispatcher-ready.** Pre-write gates abort before any of the 3 writes; failure during write/index/post-gates restores **all three** to their pre-write versions (and a net-new partial-family failure raises loud, never silent). It **self-generates its SQL in-container** (`sql=None` path) and runs green **when spawned by the dispatcher**, not only via the local entrypoint.
2. **`sam_normalized` fail-safe + cold-seek-immune.** Gate 10 no longer gates on seek latency; a forced prod-scale cold seek does **not** roll back a good build; write+index+gates are under one rollback guard.
3. **No false-fail probes.** Both workers derive the round-trip probe from materialized data; `sam_master`'s probe is a uei present in **both** entities and contacts.
4. **Baseline armed, not skipped.** Both use a floor-qualified, `dataset_uri`-scoped per-family Δ-guard. `ops.sam_master_runs` gains `dataset_uri` **and the existing success row is backfilled**, so the **first** hardened prod rebuild has a live baseline (not `None`).
5. **Content gates.** A positional-offset regression fails a pre-write content gate in both workers (floors calibrated to the observed scratch fraction).
6. **Coverage preserved.** Retiring `sam_normalized`'s absolute targets loses no coverage: `rows`, `distinct_normalized_name`, **and `distinct_legal_name_base`** each get a floor + per-family Δ.
7. **Deterministic `sam_master`.** Two consecutive scratch builds over a fixed source produce identical `entities_rows`/`contacts_rows`/`domains_rows`/`distinct_uei`.
8. **Scratch isolation actually isolates.** Validation targets a scratch prefix via a **threaded** `dest_prefix`/`dataset_uri` param (never a container env var); prod datasets and the prod baseline are untouched (scratch tags `*_scratch`).
9. **`skip_if_current` is snap-key-correct.** Both workers (and the orchestrator) gate skips on `snap_key_sql`-**normalized** labels — a `2026_MAY`-vs-`20260503` same-snapshot pair resolves to "current."
10. **Automated, self-healing refresh.** The orchestrator runs on cadence, rebuilds the spine only when stale, and dispatches `sam_normalized` **unconditionally** (so a current-master + stale-sidecar state self-heals). It cannot overlap a prior in-flight rebuild.
11. **Alerting verified live in-container** for both workers (`OPS_ALERT_WEBHOOK` present; Telegram delivers on a forced failure).
12. **Deployed + green once + consumer-verified.** All workers `modal deploy`-ed; one manual prod rebuild of each green via the **dispatched** path; a known name resolves through the fresh `sam_normalized_entities` BTREE.
13. **Landed + reconciled.** Each PR linear from `origin/main`; deployed; operator checkout `/Users/benjamincrane/core-x` (on `main`) fast-forwarded on disk; `git log -1` verified.

---

## 2. Scope — one cycle, three workstreams, **strictly sequenced** (one PR each)

| WS | PR | Change | Gate to next |
|----|----|--------|--------------|
| **A** | 1 | Harden `sam_master` **and make it dispatcher-ready**: in-container SQL (`sql=None` + field-map mount), pre-write gates (floors + per-family Δ + content + uniqueness), 3-dataset rollback (net-new failures loud), floor-qualified URI-scoped baseline (+ `dataset_uri` column **with backfill**, `ALTER` inside `OPS_DDL`), deterministic dedup, `dest_prefix` threading, snap-key `skip_if_current`, callback plumbing, intersection probe. Shared `sam_labels.py` introduced. | **dispatcher-spawned** prod rebuild green |
| **B** | 2 | Upgrade `sam_normalized`: **fix gate-10 cold-seek**, population probe, per-family floor-qualified URI-scoped baseline over `rows`+`distinct_normalized_name`+**`distinct_legal_name_base`** (retire absolutes), content gate, rollback wraps write+index, `dataset_uri` threading, snap-key `skip_if_current` (imports `sam_labels`) | prod rebuild green + consumer smoke check |
| **C** | 3 | Automate: `src/trigger/sam_spine_refresh.ts` orchestrator — freshness-gated, `concurrencyLimit:1` **plus** an in-flight ledger guard, daily `30 18 * * *` (de-collided), chaining master→normalized via dispatcher+waitpoints, **normalized dispatched unconditionally** | — |

**WS-C lands only after A and B are merged, deployed, and prod-verified via the dispatched path.**

### Out of scope
`sam_master_contacts` retirement; Gen-A `sam_entity_master.py` deletion; staging/atomic-promote (residual overwrite window stays); warn-band alerts; non-SAM feed alerting; `core.person_name_norm`/`sam_normalized_pocs`. `sam_pocs` keeps its inline `_snap_key_sql` copy (migrate to the new shared `sam_labels.py` next cycle — not re-touched here).

### Residual risk accepted
`mode="overwrite"` retained. During a `sam_master` rebuild the three datasets are **not mutually consistent** (entities overwritten, contacts/domains not yet) — a consumer reading mid-rebuild can observe a **torn family**; the window is ~minutes and bounded by rollback, but torn-family reads are possible and accepted this cycle. Staging/atomic-promote eliminates it next cycle (§11).

---

## 3. Durable engineering decisions (every one earned from the `sam_pocs` execution or this plan's adversarial review)

| # | Principle (why) | Decision | Applies |
|---|---|---|---|
| D1 | **Never gate on seek latency** — a cold R2 first-seek hit 4344ms on the `sam_pocs` prod cutover (1.1-1.4s on scratch) and rolled back a good build. | Post-write index proof = index present in `list_indices` + round-trip returns the known-present probe (`hit ≥ 1`). Seek latency **logged** (WARN), never gated. | A (new); **B (delete `seek_ms > 2000` at :420)** |
| D2 | **A dispatched function gets only `kwargs` + in-container state** — it cannot receive locally-built arguments. | `sam_master` self-generates SQL in-container (`sql=None` → build from a **mounted** field map). Dispatcher-readiness is a **WS-A** deliverable, gated by a dispatcher-spawned green run. | A |
| D3 | **`dataset_uri` is read at module import in the container; a local env never reaches it.** | Thread the write target as a param (entrypoint→param→remote): `sam_master` `dest_prefix`; `sam_normalized` `dataset_uri`. `_record_run`/baseline/feed use the **effective** URI. | A, B |
| D4 | **A new URI-scoped baseline column NULL-scopes out all pre-migration rows** → Δ-guards silently skip on the first (riskiest) hardened run. | The `dataset_uri` migration **backfills** the existing success row to the prod URI; the `ALTER` lives **inside `OPS_DDL`** (runs every `_record_run`; `sam_master` has no `init_ops`). | A |
| D5 | **Baseline ratchet** — a degraded success became the baseline and rejected recovery. | Baseline query floor-qualified: `... AND <count> >= BASELINE_MIN ...` (above the abort floor). | A, B |
| D6 | **Scratch poisons prod baseline** without URI scoping + feed tagging. | Feed tag derived from the effective URI (`<feed>` / `<feed>_scratch`); baseline scoped `AND dataset_uri = <prod>`. | A, B |
| D7 | **Scalar Δ-guard misses a half-collapse.** | Per-family Δ. `sam_master`: entities/contacts/domains rows + distinct_uei. `sam_normalized`: rows + **distinct_normalized_name + distinct_legal_name_base** (no coverage lost vs the retired absolutes). | A, B |
| D8 | **Count/fill gates miss positional-offset corruption.** | Content-plausibility gates on positionally-lifted fields, floors set **below the observed scratch fraction** (log it first; demote NAICS-numeric to observational if `primary_naics` fill is low and rely on `legal_business_name` alpha-frac). | A, B |
| D9 | **Hardcoded probe can false-fail.** | Population-derived probe from materialized data. `sam_master`: a uei in the **entities∩contacts** intersection. | A, B |
| D10 | **Rollback must wrap indexing** (a mid-index OOM leaves under-indexed data live). | Rollback guard wraps write + index + post-gates. `sam_master`: capture `v_before` for **all 3**; restore all 3; a **net-new** partial-family failure raises a distinct loud error (never silent). | A; B (move write/index inside the try) |
| D11 | **Nondeterministic dedup jitter.** | `source_file DESC` final tiebreak in `sam_master`'s latest-per-uei `ORDER BY` (`source_file` is already scanned). | A |
| D12 | **Label equality is unreliable** — SAM ships two formats for one snapshot (`2026_MAY` vs `20260503`, observed in the `sam_pocs` ledger). | All `skip_if_current` / freshness comparisons normalize **both sides** through `snap_key_sql`. Introduce shared `pipelines/sam_gov/reference/sam_labels.py`; `sam_master` + `sam_normalized` import it. | A, B, C |
| D13 | **Self-staleness trap** — running normalized "only if master rebuilt" freezes a current-master + stale-sidecar state forever. | Orchestrator dispatches `sam_normalized` **unconditionally** (it self-skips); self-healing + crash-recovery. | C |
| D14 | **`concurrencyLimit:1` on `schedules.task` is unprecedented here**; a waitpoint-suspended run may release its slot. | Keep `concurrencyLimit:1` **and** add an in-flight ledger guard (bail if a started-not-completed run exists). Acceptance: confirm (doc-cited or empirical) the suspension semantics. | C |
| D15 | **#203 silent secret** — `alert()` without the attached secret no-ops silently. | Any `alert()` caller attaches `modal.Secret.from_name("ops-alerts")` and we **prove in-container** `OPS_ALERT_WEBHOOK` present. (Both build fns already attach it via #206.) | A, B |
| D16 | **Orphan-history squash conflicts** (#196, #205). | Each PR branches fresh from `origin/main`; one workstream per PR; linear. | A, B, C |
| D17 | **`modal deploy` required; merged ≠ done.** | Deploy each worker after merge; the orchestrator spawns **deployed** functions; fast-forward `/Users/benjamincrane/core-x` on disk after each merge. | A, B, C |
| D18 | **Prod-scale surfaces what scratch hides** (the 4344ms seek only on prod). | A manual **prod** rebuild via the **dispatched** path is a hard gate before WS-C; close with a consumer-surface smoke read. | A, B |

---

## 4. WS-A — Harden `sam_master` + make it dispatcher-ready (heaviest: 3 datasets, zero current rollback)

### 4.1 Shared label module (D12) — introduce first
Create `pipelines/sam_gov/reference/sam_labels.py` exporting `snap_key_sql(col="extract_label") -> str` (the `JAN..DEC → 01..12` normalizer, lifted verbatim from `sam_master.py`'s current `_snap_key_sql`). Ensure `pipelines/sam_gov/reference/__init__.py` exists so the package mounts. `sam_master` imports `from pipelines.sam_gov.reference.sam_labels import snap_key_sql` and deletes its inline copy. (`sam_pocs` keeps its inline copy this cycle — §11.)

### 4.2 Dispatcher-readiness (D2) — the blocker fix, in WS-A
- Mount the field map **and** labels into the image:
  ```python
  image = (modal.Image.debian_slim(python_version="3.12").pip_install(...)
           .env({"LANCE_BYPASS_SPILLING": "true"})
           .add_local_python_source("core.ops_alert")
           .add_local_python_source("pipelines.sam_gov.reference.sam_v2_public_field_map")
           .add_local_python_source("pipelines.sam_gov.reference.sam_labels"))
  ```
- `build_sam_master(sql: dict | None = None, dry_run: bool = False, dest_prefix: str | None = None, skip_if_current: bool = True, trigger_callback_url: str | None = None)`. When `sql is None`, build it in-container:
  ```python
  if sql is None:
      from pipelines.sam_gov.reference.sam_v2_public_field_map import DATE_POSITIONS, PUBLIC_FIELD_MAP
      sql = build_sql(PUBLIC_FIELD_MAP, DATE_POSITIONS)
  ```
- The local entrypoint keeps passing `sql=` (harmless); the dispatcher omits it and the container self-generates. **Acceptance: a dispatcher-spawned invocation runs green — the local-entrypoint pass is not sufficient evidence.**

### 4.3 Threading & feed (D3, D6)
`prefix = dest_prefix or "s3://data-sink/active/"`; derive `entities_uri`/`contacts_uri`/`domains_uri`; `feed = "sam_master" if prefix == "s3://data-sink/active/" else "sam_master_scratch"`. Local entrypoint reads `os.environ.get("SAM_MASTER_DEST_PREFIX")` → `dest_prefix`. `_record_run`/baseline use `feed` + `entities_uri`.

### 4.4 `skip_if_current`, snap-key-normalized (D12) + callback (D2 prep)
Cheap pre-check before the expensive materialize: resolve `latest = max(snap_key_sql(extract_label))` over `entity_registrations` v2 (the worker already scans this) and `cur = snap_key_sql(<sam_master_entities current sam_extract_label>)`; if `cur == latest` → return `{"status":"skipped","label":...}`. Net-new/missing target → not current → proceed. Add `trigger_callback_url` + `_post_callback(url, payload)` (mirror `sam_pocs`/`sam_normalized`), POST `{status, label, entities_rows, contacts_rows, domains_rows, distinct_uei}` on terminal state.

### 4.5 Single-pass metrics + content + intersection probe (D8, D9)
In the build's DuckDB connection (over the in-memory `latest`/`entities`/`contacts` relations — no R2 re-scan):
```python
metrics = {
  "entities_rows": ..., "contacts_rows": ..., "domains_rows": ..., "distinct_uei": ...,
  "naics_numeric_frac": <numeric ^[0-9]{2,6}$ over NON-NULL primary_naics>,   # log + calibrate (D8)
  "name_alpha_frac":    <[A-Za-z] over NON-NULL legal_business_name>,          # high-fill primary defense
  "primary_naics_fill": <non-null primary_naics / entities_rows>,              # logged, informs the NAICS floor
}
# probe present in BOTH entities and contacts (D9):
#   SELECT c.uei FROM (SELECT uei,count(*) n FROM contacts GROUP BY uei) c
#   JOIN (SELECT DISTINCT uei FROM entities) e USING (uei) ORDER BY c.n DESC, c.uei LIMIT 1
metrics["probe_uei"] = <that uei>
```

### 4.6 Pre-write gates — pure `assert_pre_write_gates(metrics, baseline)` (D5, D7, D8) — unit-tested
1 `entities_rows >= ENTITIES_ROW_FLOOR` (1_400_000) · 2 `distinct_uei == entities_rows` · 3 `contacts_rows >= CONTACTS_FLOOR` (3_500_000) · 4 `domains_rows >= DOMAINS_FLOOR` (500_000) · 5-7 **per-family Δ** (skip iff `baseline is None`): entities/contacts/domains rows each within ±`DELTA_GUARD` (0.25) of the floor-qualified prior · 8 `name_alpha_frac >= NAME_ALPHA_MIN` (0.95) · 9 `naics_numeric_frac >= NAICS_NUMERIC_MIN` (**set below observed scratch fraction; demote to observational if `primary_naics_fill` < 0.6**, D8). Floors calibrated from the first clean scratch run (§7B).

### 4.7 Floor-qualified, URI-scoped baseline + migration with backfill (D4, D5, D6)
```python
def _prior_success_baseline(entities_uri: str) -> dict | None:
    # SELECT entities_rows, contacts_rows, domains_rows, distinct_uei FROM ops.sam_master_runs
    # WHERE status='success' AND dataset_uri = %s AND entities_rows >= BASELINE_MIN_ENTITIES (1_450_000)
    # ORDER BY recorded_at DESC LIMIT 1   -- params: (entities_uri, ...)
```
`OPS_DDL` (runs in every `_record_run` — `sam_master` has no `init_ops`) gains, after the `CREATE TABLE`:
```sql
ALTER TABLE ops.sam_master_runs ADD COLUMN IF NOT EXISTS dataset_uri text;
```
`_record_run` writes `entities_uri` into `dataset_uri`. **One-time backfill** (run via `psql` before the first hardened prod build) so the existing success row qualifies as a baseline:
```sql
UPDATE ops.sam_master_runs SET dataset_uri = 's3://data-sink/active/sam_master_entities/'
 WHERE dataset_uri IS NULL AND feed = 'sam_master';
```
Optionally create canonical `pipelines/sam_gov/ops_sam_master_runs.sql` to match fleet convention (the other two feeds have sidecars).

### 4.8 Deterministic dedup (D11)
In the `latest` CTE `ORDER BY`, append `, source_file DESC NULLS LAST` after the existing `last_update_date`/`initial_registration_date`/snap-key keys.

### 4.9 Write + index + post-write gates under ONE 3-dataset rollback guard (D1, D10)
```python
v_before = {}
for name, uri in (("entities", entities_uri), ("contacts", contacts_uri), ("domains", domains_uri)):
    try: v_before[name] = lance.dataset(uri, storage_options=so).version
    except Exception: v_before[name] = None
written = []
try:
    for table, uri, btree, name in ((entities, entities_uri, ENTITIES_BTREE, "entities"),
                                    (contacts, contacts_uri, CONTACTS_BTREE, "contacts"),
                                    (domains, domains_uri, DOMAINS_BTREE, "domains")):
        lance.write_dataset(table, uri, mode="overwrite", data_storage_version=DATA_STORAGE_VERSION,
                            max_rows_per_file=MAX_ROWS_PER_FILE, storage_options=so)
        written.append(name)
        d = lance.dataset(uri, storage_options=so)
        for col in btree:
            if col in set(d.schema.names): d.create_scalar_index(col, index_type="BTREE")
    # post-write gates (correctness, NOT timing — D1):
    ent = lance.dataset(entities_uri, storage_options=so)
    if ent.count_rows() != metrics["entities_rows"]: raise RuntimeError("gate: entities write-integrity")
    if not {f"{c}_idx" for c in ENTITIES_BTREE}.issubset({_name(i) for i in ent.list_indices()}): raise RuntimeError("gate: entities indices")
    pr = metrics["probe_uei"]                              # present in BOTH (D9)
    if ent.scanner(columns=["uei"], filter=f"uei='{pr}'").to_table().num_rows < 1: raise RuntimeError("gate: entities probe")
    con_ds = lance.dataset(contacts_uri, storage_options=so)
    if con_ds.scanner(columns=["uei"], filter=f"uei='{pr}'").to_table().num_rows < 1: raise RuntimeError("gate: contacts probe")
except Exception as werr:
    restored, orphaned = [], []
    for name, uri in (("entities", entities_uri), ("contacts", contacts_uri), ("domains", domains_uri)):
        if v_before[name] is not None:
            try: lance.dataset(uri, storage_options=so, version=v_before[name]).restore(); restored.append(name)
            except Exception as rerr: raise RuntimeError(f"ROLLBACK FAILED {name}->v{v_before[name]}: {rerr}; original: {werr}")
        elif name in written:
            orphaned.append(name)                          # net-new + written + failed → cannot roll back
    if orphaned: raise RuntimeError(f"NET-NEW partial-family failure: inspect/drop {orphaned}; restored {restored}; original: {werr}")
    raise RuntimeError(f"write/index/gate failed → rolled back {restored}: {werr}")
```
> Pre-write gates ran on the in-memory tables, so corruption aborts at zero exposure. **`contacts`/`domains` are derived from the in-memory `latest` relation, not the written `entities` dataset — write order carries no read-after-write dependency** (state this so a future edit doesn't introduce one). Net-new partial failures are loud, never silent.

---

## 5. WS-B — Upgrade `sam_normalized` (targeted, not a rewrite)

### 5.1 Fix gate 10 — the cold-seek false-positive (D1) — highest-priority line in the cycle
[`sam_normalized_entities.py:420`](../../pipelines/sam_gov/sam_normalized_entities.py):
```python
# BEFORE: if hit < 1 or seek_ms > 2000: raise RuntimeError(... ">2000ms ⇒ no index")
# AFTER:
if hit < 1:
    raise RuntimeError("gate 10 name index: lookup of a known-present name returned 0 rows")
_slow = "" if seek_ms <= SEEK_WARN_MS else f"  [WARN cold seek >{SEEK_WARN_MS}ms]"   # logged, not gated
```

### 5.2 Population probe (D9) — replace hardcoded KIPPER
Drop `KIPPER_UEI` (line 78) as a hard gate; `_materialize` emits `probe_uei` (a uei with non-null `normalized_legal_name`); gates 9-10 round-trip that.

### 5.3 Per-family, floor-qualified, URI-scoped baseline — covering `distinct_legal_name_base` (D5, D6, D7)
- `_prior_success_rows` → `_prior_success_baseline(dataset_uri)` returning `{rows_written, distinct_normalized_name, distinct_legal_name_base}` (all already ledger columns), with `AND dataset_uri = %s AND rows_written >= BASELINE_MIN_ROWS` (1_450_000). (`ops.sam_normalized_entities_runs` already has `dataset_uri` + one URI-stamped success row → baseline armed on the first hardened run.)
- Replace the absolute `NORM_DISTINCT_TARGET`/`BASE_DISTINCT_TARGET` (±5%, lines 239-242) with **coarse floors** (`NORM_FLOOR` 1_300_000, `BASE_FLOOR` 1_280_000) **plus per-family Δ** on `rows`, `distinct_normalized_name`, **and `distinct_legal_name_base`** (±25% vs the floor-qualified prior). No coverage lost.

### 5.4 Content gate (D8)
Add `normalized_legal_name` alpha-frac ≥ `NAME_ALPHA_MIN` (0.95) to the pre-write gates.

### 5.5 Rollback wraps write + indexing (D10)
Move `write_dataset` + `create_scalar_index` (lines 382-395) **inside** the `try` that runs gates 8-10 and `restore(v_before)`; a failed `restore()` raises a distinct loud error.

### 5.6 Threading + snap-key skip_if_current (D3, D12)
Thread `dataset_uri` (effective `uri = dataset_uri or DATASET_URI`); `feed` derived; `_record_run`/baseline use it; entrypoint passes `os.environ.get("SAM_NORMALIZED_ENTITIES_URI")`. Add `skip_if_current: bool = True`: `from pipelines.sam_gov.reference.sam_labels import snap_key_sql` (+ mount), compare `snap_key_sql`(source `sam_master_entities` max label) vs `snap_key_sql`(this dataset's current label); equal → `{"status":"skipped"}`.

---

## 6. WS-C — Automate (Trigger orchestrator; freshness-gated; self-healing)

**`src/trigger/sam_spine_refresh.ts`** — `schedules.task({ cron:{ pattern:"30 18 * * *", timezone:"UTC" }, queue:{ concurrencyLimit:1 }, ... })`. `30 18` avoids the `0 18` collision with `contractor_award_summary`; after the SAM drop, `sam_pocs` (16:30), and `0 18`.

1. **In-flight guard (D14):** first action — query `ops.sam_master_runs`/`ops.sam_normalized_entities_runs` (via a tiny dispatched check, or a Trigger-side Postgres call if available) for a `started_at` within the last N hours with no `completed_at`; if one is in flight, log + exit. (Belt-and-suspenders beyond `concurrencyLimit:1`, whose across-suspension semantics are unverified — D14.)
2. **Step 1 — master:** dispatch `build_sam_master` `kwargs:{ skip_if_current:true }` + waitpoint; `await`. `status!=="success" && !=="skipped"` → `throw` (worker already Telegram-alerts). Do **not** early-return on `skipped`.
3. **Step 2 — normalized (UNCONDITIONAL, D13):** always dispatch `build_sam_normalized_entities` `kwargs:{ skip_if_current:true }` + waitpoint; `await`. It self-skips when the sidecar already matches the master label, rebuilds when it lags. `skipped`/`success` → done; else `throw`. This heals a current-master + stale-sidecar state and recovers from an orchestrator crash between steps.

> `entity_registrations_backfill.ts` exists but is a **manual, cron-less** backfill — no recurring completion event — so the daily freshness-gated schedule is the **sole** driver; an optional completion-callback hook off the backfill is a future enhancement, not this cycle. Both build fns are dispatcher-resolvable after WS-A/B deploy; `kwargs` bind via `fn.spawn(**kwargs, ...)` (verified). `concurrencyLimit` is valid v4 (in use on plain `task()`), unprecedented on `schedules.task` here → the in-flight guard covers the gap.

---

## 7. Validation harness (scratch isolation that actually isolates — D3)

Scratch prefix: `s3://data-sink/scratch/sam_spine/`. URIs are **threaded** (`dest_prefix`/`dataset_uri`), so the redirect reaches the container, feeds tag `*_scratch`, and the prod baseline (scoped to the prod URI) is untouched.

### 7A. Unit tests (pure, no R2) — `pipelines/sam_gov/tests/test_sam_master_gates.py` (+ extend `test_sam_pocs_gates.py` patterns)
- `sam_master`: healthy pass; **contacts-half-collapse caught by the per-family Δ given a baseline**, and a fixture proving the Δ **correctly skips when `baseline is None`** (the first-hardened-run path is tested, not assumed); offset-shift (`naics_numeric_frac`/`name_alpha_frac` low) raises; floors.
- `sam_normalized`: healthy pass; `distinct_normalized_name` **and `distinct_legal_name_base`** collapses each caught by their per-family Δ; non-alpha keys raise; baseline floor-qualification (a sub-floor success is not the baseline).

### 7B. Scratch builds (Modal, prefix-threaded)
```
SAM_MASTER_DEST_PREFIX=s3://data-sink/scratch/sam_spine/  modal run pipelines/sam_gov/sam_master.py     # build #1 (net-new)
SAM_MASTER_DEST_PREFIX=s3://data-sink/scratch/sam_spine/  modal run pipelines/sam_gov/sam_master.py     # build #2 → determinism (identical 4 counts)
SAM_NORMALIZED_ENTITIES_URI=s3://data-sink/scratch/sam_spine/sam_normalized_entities/  modal run pipelines/sam_gov/sam_normalized_entities.py
```
Expect: gates pass; 3 datasets written+indexed; intersection probe round-trips in entities AND contacts; **build #2 counts == build #1**; feeds tagged `*_scratch`. **Log `naics_numeric_frac` + `primary_naics_fill`** and set the NAICS floor below observed (or demote per D8).

### 7C. Negative paths (scratch — prove the guard guards)
1. **`sam_normalized` cold-seek no longer false-fails:** a build whose logged `seek_ms` exceeds `SEEK_WARN_MS` still returns `success` (no rollback path on latency).
2. **`sam_master` mid-index rollback — run AFTER ≥1 clean scratch build** (so `v_before` is non-None for all three): inject a bogus expected index on **contacts** → all 3 restore to `v_before` (versions revert), terminal `error` under `sam_master_scratch`. Separately, a **net-new** (fresh-prefix) injected failure must raise the distinct "NET-NEW partial-family" loud error (not silent). Revert.
3. **Baseline isolation:** after the scratch builds, each worker's prod-scoped baseline query still returns the pre-existing **prod** success (scratch rows live only under `*_scratch`).

### 7D. Prod cutover (only after 7A-7C green; D18)
- **Backfill `ops.sam_master_runs.dataset_uri` first** (§4.7 `UPDATE`); confirm `_prior_success_baseline()` returns the 1,541,566-row prior — **Δ-guards armed, not skipped.**
- Record each dataset's pre-cutover version. `modal deploy` all touched workers. Run one prod rebuild of `sam_master` **via the dispatched path** (not just the local entrypoint — D2), then `sam_normalized`. Confirm ledgers `success`, labels advance together, families within range, `sam_normalized`'s logged prod seek does **not** roll back.
- **Consumer smoke check:** resolve a known business name through the fresh `sam_normalized_entities` `normalized_legal_name` BTREE → returns its uei. The surface is usable, not just gate-green.

### 7E. Alerting (D15) + automation dry-run (D13)
- Force one failure in each worker (high floor) → confirm in-container `OPS_ALERT_WEBHOOK` present + Telegram delivers.
- Orchestrator manual run: current spine → logs "current" and no-ops (master skipped, normalized skipped). **Force the sidecar stale while master is current** → orchestrator's unconditional step 2 rebuilds the sidecar (proves D13 self-heal).

---

## 8. Landing (per-PR, linear from `origin/main` — D16, D17)
For each WS PR, in order A → B → C: `git fetch origin && git checkout -b claude/<name> origin/main` (fresh, no orphan) → commit → push → PR vs `main` with §7 evidence → self-verify → `gh pr merge --squash --delete-branch` → `modal deploy` the touched worker(s) → `git -C /Users/benjamincrane/core-x pull --ff-only` (it is on `main`) → verify `git log -1`. WS-C lands and enables the orchestrator **only after** A and B are deployed and **dispatcher-path** prod-verified.

---

## 9. Safety, invariants, abort
- **Protection lives pre-write** (in-memory gates, zero exposure). Post-write gates + rollback cover Lance write/index integrity only. **No gate asserts seek latency** (D1).
- **`sam_master` atomicity:** all 3 tables materialize + pass pre-write gates before any write; rollback restores all 3; **net-new partial failures raise loud** (not silently committed). Mid-rebuild, the family is momentarily torn (entities ahead of contacts/domains) — accepted this cycle (§2 residual risk).
- **Signatures:** all new params optional with prod-safe defaults (`sql=None`, `dest_prefix=None`, `dataset_uri=None`, `skip_if_current=True`, `trigger_callback_url=None`) — the dispatcher path stays valid.
- **Abort:** each dataset recoverable via Lance time-travel; record pre-cutover versions before 7D. The orchestrator is disabled until A+B are proven on the dispatched path.

---

## 10. Acceptance checklist
- [ ] `sam_master`: in-container SQL (`sql=None` + field-map mount) — **dispatcher-spawned** prod run green; pre-write gates (floors + per-family Δ + content + uniqueness); 3-dataset rollback wrapping write+index+gates with **loud net-new partial failure**; floor-qualified URI-scoped baseline; `dataset_uri` column added via `ALTER` **inside `OPS_DDL`** **and existing success row backfilled** (baseline armed); deterministic dedup; `dest_prefix` threading; snap-key `skip_if_current`; callback plumbing; **entities∩contacts** probe.
- [ ] `sam_normalized`: **gate-10 cold-seek removed**; population probe; per-family floor-qualified URI-scoped baseline over `rows`+`distinct_normalized_name`+**`distinct_legal_name_base`** (absolutes retired, no coverage lost); content gate; rollback wraps write+index; `dataset_uri` threading; snap-key `skip_if_current`.
- [ ] Shared `pipelines/sam_gov/reference/sam_labels.py`; both spine workers import it; `reference/__init__.py` present + mounted.
- [ ] Unit tests green: `sam_master` half-collapse-with-baseline + **skip-when-baseline-None**; offset-shift; `sam_normalized` norm- and base-distinct collapses + content + baseline floor.
- [ ] Scratch: determinism (master #1==#2); mid-index rollback **after a clean build** restores all 3; net-new partial failure raises loud; cold-seek build still `success`; baseline isolation (prod untouched, `*_scratch` tagged); NAICS frac logged + floor calibrated.
- [ ] Prod: `dataset_uri` backfilled + baseline confirmed armed; **dispatched-path** rebuild of each green; labels advanced; consumer smoke read resolves a known name.
- [ ] Both deployed; forced failure → Telegram (in-container webhook present).
- [ ] Orchestrator: no-ops when current; **unconditional** normalized dispatch heals a stale sidecar; in-flight guard + `concurrencyLimit:1`; cron `30 18` (no collision).
- [ ] All 3 PRs linear from `origin/main`, squash-merged, branches deleted; operator checkout fast-forwarded on disk; `git log -1` verified.

---

## 11. Next cycles (do NOT do here)
1. **Staging + atomic promote** for the spine — eliminates the overwrite/torn-family window (`sam_master`'s 3-dataset window is the largest in the fleet).
2. **Migrate `sam_pocs` to the shared `sam_labels.py`** (it keeps an inline `_snap_key_sql` copy this cycle) — completes the single-source consolidation.
3. **Warn-band alerts** (Δ in the outer half of tolerance) + **non-SAM feed alerting** (each needs the `core.ops_alert` + `ops-alerts` pattern).
4. **Optional `entity_registrations_backfill` completion-callback hook** → event-driven spine refresh (the daily freshness gate stays as the floor).
5. **`sam_master_contacts` retirement** + **Gen-A `sam_entity_master.py` deletion**.
6. **`core.person_name_norm` + `sam_normalized_pocs`** once a committed person-bridge consumer exists.
