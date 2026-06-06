# `sam_normalized_pocs` — Person-Layer Sidecar Build Plan

Plan of record for the **derived person-name blocking-key sidecar** built off the verbatim human layer
`sam_pocs`. Immediately executable: a fresh agent follows §12 top to bottom. This is the person analog
of [`sam_normalized_entities`](../../pipelines/sam_gov/sam_normalized_entities.py) and ships the same
gate + rollback + ops-ledger shape; the **data-plane logic is person-specific and is NOT cloned** (§2).

**Status:** ready to execute. **Type:** net-new derived Lance dataset + new shared primitive
(`core.person_name_norm`) + Modal worker + Trigger task. **Reference pattern (control plane, copy):**
[`pipelines/sam_gov/sam_normalized_entities.py`](../../pipelines/sam_gov/sam_normalized_entities.py).
**Reasoning + live cardinality:**
[`FEC_SAM_PERSONNEL_BRIDGE_DIAGNOSTIC.md`](../reference/FEC_SAM_PERSONNEL_BRIDGE_DIAGNOSTIC.md)
(probe 2026-06-05 — every figure below is a live read from that diagnostic).

> **What this builds:** the SAM-side **person resolution + audience spine** only. It reads `sam_pocs`,
> never FEC. It is the consumer-agnostic right-side surface for (a) **GTM/audience** construction
> (contact POCs at SAM/USAspending entities — §13) and (b) a **walled-off FEC personnel bridge**
> (intelligence only, never a contact source — §13). Neither consumer is built here.

---

## 1. Objective

Materialize a thin, super-indexed sidecar that answers one question at index speed: **"which normalized
person + geo is this POC, and which entity (UEI/CAGE) do they sit under?"** `person_key` is computed
**once, here**, and BTREE-indexed — so a downstream consumer pays a point-lookup, not a repeated
8.07M-row full-scan recompute of the `name_norm` regex (which is exactly what the personnel diagnostic
had to hand-inline because no stored key exists today).

**Success =** the dataset exists in R2, BTREE-indexed on `person_key`, validated 1:1 against live
`sam_pocs`, with the person key built from the **shared** `core.person_name_norm` primitive (never
re-inlined), `uei`/`cage_code` provenance load-bearing for the entity traversal, and a round-trip
point-lookup passing.

---

## 2. Why a sidecar — and why person ≠ company (the part that is NOT cloned)

**Why a sidecar, not columns on `sam_pocs`** (identical reasoning to the entity sidecar):
- **Blast radius.** The person-key policy is *volatile* (honorific sets, generational handling,
  particle handling will evolve). Each tweak rebuilds **only this narrow projection** — never the
  8.07M-row verbatim `sam_pocs` build with its 17-gate `pipe_fields` unpivot.
- **Contract purity.** `sam_pocs` is the **ZERO-ALTERATION verbatim SoR**. Derived/mutable keys are
  quarantined here so the verbatim dataset stays 100% authoritative. The verbatim parts are **not**
  duplicated as the system of record — they live once in `sam_pocs`; this is a projection/index over it.
- **Indexability.** Lance BTREEs a **stored column**, not an expression. The canonical key must be
  materialized to be a point-seek instead of a per-query scan.

**Why the logic is person-specific (the cloned-from-entities trap):** the entity gate suite encodes
*company-name* physics. Three invert for people — the executing agent MUST implement these, not copy:

| Entity assumption | Person reality | This plan |
|---|---|---|
| `legal_name_base` **peels & discards** the trailing token (LLC/CO) for recall | A trailing **generational** token (JR/SR/II–V) is a father/son **discriminator** | Generational suffix is peeled OUT of `person_key` (recall: FEC's suffix-less `SMITH JOHN` still blocks SAM's `SMITH JOHN JR`) but **PRESERVED in its own column** (precision: a consumer may require it). Honorifics/credentials (DR/MD/PHD/ESQ) are noise → stripped. |
| Gate 3 `distinct_uei == rows` (key ~unique) | `distinct person_key / rows ≈ 0.26` by design; the same person recurs across entities/slots | **Gate 3 deleted.** A degenerate key is normal. The sensitive check moves to the addressable **`(person_key, state2, zip5)` triple** count. |
| `name_norm(name)` + geo ≈ unique resolver | `JOHN SMITH` + metro zip5 = several humans | The sidecar **blocks, does not resolve.** Disambiguation (employer-agreement via the entity sidecar, amount, temporal) lives in the consumer (§13). |

---

## 3. Inputs & output

| | Value |
|---|---|
| **Source (read-only)** | `s3://data-sink/active/sam_pocs/` — **8,065,116 rows** · 1/(entity, populated POC slot) · BTREE(uei, cage_code, name_key, last_name) |
| **Output** | `s3://data-sink/active/sam_normalized_pocs/` (net-new) |
| **Grain** | **1 row per `sam_pocs` row** (pure 1:1 passthrough — lossless; preserves the `person → uei → employer` fan-out the FEC bridge needs). NOT 1/person; a downstream `DISTINCT ON` collapses to person/person-in-role for audiences (§13). |
| **Est. rows** | ~8,065,116 |
| **Lance version** | `data_storage_version="2.1"` (net-new pin; `02_lancedb_storage.md` §2.3), `max_rows_per_file=1048576`, `max_bytes_per_file=90*1024**3` |

The worker reads **only** `sam_pocs`. It never touches `entity_registrations`, `sam_master_entities`,
or any FEC dataset. **No filtering** — foreign POCs are retained with nullable geo (lossless); the
`country='USA'` filter is a *consumer* concern applied at query time, not baked into the spine.

Live shape (diagnostic, probe 2026-06-05): `first_name` 100% · `last_name` 99.96% · `middle_name`
24.47% · `state` 98.72% · `zip5` 99.06% · `uei` 54.22% (v2) · `cage_code` 95.51% (legacy tail) ·
name ∧ state ∧ zip5 = **7,910,584 (98.08%)** · distinct `name_norm(LAST FIRST)` **2,119,414** ·
distinct `(name_norm, state, zip5)` **2,868,249** · name_norm collapse on human names **−0.15%**.

---

## 4. Output schema (exact)

Scan these `sam_pocs` columns (projection pushdown): `uei`, `cage_code`, `poc_type`, `source_family`,
`first_name`, `middle_name`, `last_name`, `full_name`, `state`, `zip5`, `country`, `sam_extract_label`.

| Column | Type | Derivation | Index |
|---|---|---|---|
| `person_key` | string | `person_name_norm.person_key(last_name, first_name)` — `name_norm(LAST_core FIRST_core)`, no middle, no generational | **BTREE** (primary blocking key) |
| `surname_initial_key` | string | `person_name_norm.surname_initial_key(last_name, first_name)` — `name_norm(LAST_core + first-initial)` | **BTREE** (recall fallback — FEC initials vs SAM full names) |
| `last_name_norm` | string | `name_norm(last_name_core)` | **BTREE** (surname-only blocking) |
| `first_initial` | string | `left(first_core, 1)` (normalized) | — (confirmatory) |
| `middle_norm` | string | `name_norm(middle_name)` | — (confirmatory tiebreak; 24% fill) |
| `generational_suffix` | string | `person_name_norm.generational_suffix(...)` — JR/SR/II–V, extracted & **preserved** | — (precision tiebreak; **never** in `person_key`) |
| `uei` | string | passthrough `nullif(trim(uei),'')` | **BTREE** (entity traversal → employer) |
| `cage_code` | string | passthrough | **BTREE** (legacy-tail entity traversal) |
| `poc_type` | string | passthrough | **BITMAP** (slot filter) |
| `source_family` | string | passthrough (`v2` / `legacy_v1`) | **BITMAP** (family filter) |
| `country` | string | `nullif(trim(country),'')` | **BITMAP** (US-only consumer filter) |
| `state2` | string | `upper(nullif(trim(state),''))` | — (geo, inline tiebreak) |
| `zip5` | string | `left(nullif(trim(zip5),''),5)` | — (geo, inline tiebreak) |
| `first_name` `middle_name` `last_name` `full_name` | string | verbatim passthrough | — (human/audit; SoR copy) |
| `sam_extract_label` | string | passthrough (provenance + snap-key) | — |
| `source_dataset` | string | const `'sam_pocs'` | — |

`state2` / `zip5` are **denormalized onto the row on purpose** (block on name + tiebreak on geo in the
*same* query — a hydration join mid-resolution defeats the index). Column names are chosen
**union-compatible** with `sam_master_contacts` / `ffata_exec_comp` so a future person spine is a literal
`UNION ALL`.

**Minimum viable index set** (if trimming): `person_key`, `uei`, `cage_code`. Recommended adds
`surname_initial_key`, `last_name_norm` BTREE + the three BITMAPs (cheap, fleet convention). `person_key`
is the high-cardinality string BTREE (~2.12M distinct) — `LANCE_BYPASS_SPILLING=true` keeps its sort
in-memory (lance#2650).

**Two measured properties a consumer must plan around:**
- **`person_key` is many-to-many** — non-unique by design (homonyms + multi-entity POCs). The sidecar
  makes **no single-row-join promise**; a consumer resolves candidate sets + disambiguates (§13).
- **Generational suffix is carried, not keyed** — same posture as middle: present it for a precision
  tiebreak; never require it in the primary block (kills recall against suffix-less sources).

---

## 5. New shared primitive — `core/person_name_norm.py` (exact)

Pure DuckDB-SQL string builders, **composes `core.name_norm`** (never re-inlines the regex), imports
nothing else — safe to mount into any image. The person analog of `core.name_norm`. Author it, then
**unit-test the regexes** (§11) before first build.

```python
"""Canonical person-name blocking-key SQL builders — the person analog of core.name_norm.

Operates on ALREADY-SPLIT parts (first/middle/last). It does NOT parse opaque strings — SAM
delivers discrete parts, so no name-splitting library is used (operator ZERO-ALTERATION policy;
investigation confirmed nameparser/probablepeople would only inject role-misassignment error here).

Person ≠ company on the tail (see SAM_NORMALIZED_POCS_BUILD_PLAN §2):
  core.name_norm.legal_name_base PEELS and DISCARDS the trailing token for recall;
  here a trailing GENERATIONAL token (JR/SR/II–V) is peeled out of the primary key for recall
  but PRESERVED in its own column for precision. Honorifics/credentials are noise → stripped.
  Middle is dropped from the key (24% fill, inconsistent) and carried as a confirmatory tiebreak.
"""
from __future__ import annotations

from core.name_norm import name_norm

_GEN = "JR|SR|II|III|IV|V"
_HONORIFIC = "DR|MR|MRS|MS|MISS|PROF|SIR|HON|REV"
_CREDENTIAL = "MD|PHD|ESQ|CPA|JD|RN|DDS|DO|DVM|PE|PMP|MBA"


def _upper(expr: str) -> str:
    return f"upper(CAST({expr} AS VARCHAR))"


def _last_core(last: str) -> str:
    """Surname (UPPER) with a trailing credential then a trailing generational token peeled —
    whole-token, end-anchored (' JR' strips; 'JRINKINS' does not). NULL out if emptied."""
    u = _upper(last)
    u = f"regexp_replace({u}, ' +({_CREDENTIAL})\\.?$', '', 'g')"
    u = f"regexp_replace({u}, ' +({_GEN})\\.?$', '', 'g')"
    return f"nullif(trim({u}), '')"


def _first_core(first: str) -> str:
    """Given name (UPPER) with a leading honorific peeled."""
    u = _upper(first)
    return f"nullif(trim(regexp_replace({u}, '^({_HONORIFIC})\\.?\\s+', '', 'g')), '')"


def person_key(last: str, first: str) -> str:
    """Primary blocking key — name_norm(LAST_core FIRST_core). No middle, no generational.
    Surname-anchored: NULL when _last_core is empty (a key with no surname is not a person key)."""
    return (
        f"CASE WHEN {_last_core(last)} IS NULL THEN NULL ELSE "
        f"{name_norm(f'concat_ws(chr(32), {_last_core(last)}, {_first_core(first)})')} END"
    )


def surname_initial_key(last: str, first: str) -> str:
    """Recall key — name_norm(LAST_core + first initial). Bridges FEC initials ↔ SAM full names."""
    return (
        f"CASE WHEN {_last_core(last)} IS NULL THEN NULL ELSE "
        f"{name_norm(f'concat_ws(chr(32), {_last_core(last)}, left({_first_core(first)}, 1))')} END"
    )


def generational_suffix(last: str, first: str) -> str:
    """The trailing generational token (JR/SR/II–V), extracted & PRESERVED — never enters person_key."""
    src = f"concat_ws(chr(32), {_upper(last)}, {_upper(first)})"
    return f"nullif(upper(regexp_extract({src}, ' ({_GEN})\\.?$', 1)), '')"
```

`middle_norm` and `last_name_norm` use `core.name_norm` directly (`name_norm("middle_name")`,
`name_norm(_last_core("last_name"))`). **Finalization note for the agent:** confirm the whole-token
end-anchored peel against the §11 unit tests (esp. surnames `MAY`, `VI`, `JR`-as-whole-surname) before
the first build; the regex set above is the contract, the exact escaping is yours to land.

---

## 6. The transform (exact SQL)

Pure DuckDB, clean-room. Reads a `src` relation (the scanned `sam_pocs`). 1:1 passthrough — no dedup,
no filter (lossless).

```python
from core.name_norm import name_norm
from core.person_name_norm import person_key, surname_initial_key, generational_suffix

def build_normalized_pocs_sql() -> str:
    return f"""
    SELECT
        {person_key("last_name", "first_name")}            AS person_key,
        {surname_initial_key("last_name", "first_name")}   AS surname_initial_key,
        {name_norm("last_name")}                           AS last_name_norm,
        left({name_norm("first_name")}, 1)                 AS first_initial,
        {name_norm("middle_name")}                         AS middle_norm,
        {generational_suffix("last_name", "first_name")}   AS generational_suffix,
        nullif(trim(uei), '')                              AS uei,
        nullif(trim(cage_code), '')                        AS cage_code,
        poc_type,
        source_family,
        nullif(trim(country), '')                          AS country,
        upper(nullif(trim(state), ''))                     AS state2,
        left(nullif(trim(zip5), ''), 5)                    AS zip5,
        nullif(trim(first_name), '')                       AS first_name,
        nullif(trim(middle_name), '')                      AS middle_name,
        nullif(trim(last_name), '')                        AS last_name,
        nullif(trim(full_name), '')                        AS full_name,
        sam_extract_label,
        'sam_pocs'                                         AS source_dataset
    FROM src
    """
```

DuckDB envelope: `memory_limit='16GB'`, `threads=8`, `temp_directory='/tmp/duckdb_spill'`,
`preserve_insertion_order=false`. Image env `LANCE_BYPASS_SPILLING=true` keeps the `person_key` BTREE
(~2.12M distinct) in-memory. The projection is narrow (12 source cols, no `pipe_fields` unnest) — the
cost is the high-cardinality string-key sort, not the scan.

---

## 7. Worker file (structure — copy the control plane, swap the data plane)

Create **`pipelines/sam_gov/sam_normalized_pocs.py`** (Modal app `sam-gov-normalized-pocs-pipelines`).
**Copy the control-plane skeleton verbatim from
[`sam_normalized_entities.py`](../../pipelines/sam_gov/sam_normalized_entities.py)** — it already has the
exact shape needed: `skip_if_current` snap-key guard, single rollback guard, ops ledger, callback,
dry-run. Swap only the data-plane deltas.

- **Image:** `debian_slim("3.12").pip_install("duckdb>=1.5,<2","lancedb>=0.15","pylance>=7","pyarrow>=17",
  "requests>=2.32","psycopg[binary]>=3.2").env({"LANCE_BYPASS_SPILLING":"true"})
  .add_local_python_source("core.name_norm","core.person_name_norm","core.ops_alert",
  "pipelines.sam_gov.reference.sam_labels")`  ← **both** norm modules + the shared snap-key.
- **Constants:** `SRC_URI="s3://data-sink/active/sam_pocs/"`,
  `_PROD_URI="s3://data-sink/active/sam_normalized_pocs/"`,
  `DATASET_URI=os.environ.get("SAM_NORMALIZED_POCS_URI",_PROD_URI)`,
  `_feed_for(uri)` → `"sam_normalized_pocs"` / `"sam_normalized_pocs_scratch"`,
  `BTREE_INDEXES=["person_key","surname_initial_key","last_name_norm","uei","cage_code"]`,
  `BITMAP_INDEXES=["poc_type","source_family","country"]`,
  `DUCKDB_MEMORY_LIMIT="16GB"`, gate constants per §8.
- **Functions** (mirror `sam_normalized_entities.py` names):
  - `build_sam_normalized_pocs(trigger_callback_url=None, dataset_uri=None, skip_if_current=True)` —
    secrets `r2-credentials` + `hqx-postgres` + `ops-alerts`; `skip_if_current` snap-key compare on
    `sam_extract_label` (import `snap_key_sql` from `reference.sam_labels`) → materialize → **pre-write
    gates on the Arrow table (§8)** → capture `v_before` → write → index → **post-write gates,
    `restore(v_before)` on failure** → `_record_run` → `_post_callback` → re-raise on failure.
  - `verify_sam_normalized_pocs(dataset_uri=None)` — read-back from R2: rows / distinct person_key /
    distinct `(person_key,state2,zip5)` over `country='USA'` / by poc_type / by source_family / indices /
    6-row sample / the §8 observability (generational-in-key leak count; homonym ratio).
  - `plan_sam_normalized_pocs(dataset_uri=None)` — materialize + gates, **write nothing** (dry-run).
  - `init_ops()` — apply §9 DDL.
  - `@app.local_entrypoint() build(dry_run=False)` — dry-run → `plan_*`; else `build_*` then `verify_*`,
    reading `SAM_NORMALIZED_POCS_URI` for scratch override (manual run uses `skip_if_current=False`).
- `_r2_storage_options()`, `_new_con()`, `_pg_connect()`, `_prior_success_baseline()`, `_record_run()`,
  `_post_callback()` — copy verbatim from `sam_normalized_entities.py`, swapping column/feed names.

---

## 8. Validation gates (no ship without all green)

**Gate timing.** Pre-write gates compute on the in-memory Arrow table and **MUST hard-fail BEFORE
`write_dataset` overwrites the live dataset**. Post-write gates run under the rollback guard: capture
`v_before = lance.dataset(uri).version` before the write; on any post-write failure
`lance.dataset(uri, version=v_before).restore()` then re-raise (clone the proven pattern in
`sam_normalized_entities.py:450-494`). First build → `v_before` is `None`; the guard protects every
subsequent rebuild.

Baselines are live (diagnostic 2026-06-05). Floors sit **below** the ±25% Δ-band so the per-family Δ is
the binding sensitive check; floors are catastrophic-collapse catchers only.

**Pre-write (on the Arrow table, hard-fail before overwrite):**
1. **Row floor** — `rows ≥ 7,500,000`.
2. **1:1 passthrough** — `rows == count(sam_pocs)` (lossless; no dedup, no filter).
3. **`person_key` fill tripwire** — non-null `≥ 99.9%` (only the 0.04% null-surname rows null out).
4. **Addressable-triple floor** — `distinct (person_key, state2, zip5)` where `country='USA'`
   `≥ 2,500,000` (live 2,868,249).
5. **`person_key` distinct floor** — `≥ 1,800,000` (live 2,119,414).
6. **Homonym-band sanity** — `distinct person_key / rows ∈ [0.15, 0.40]` (live 0.263) — catches both
   over-collapse (regex ate the name) and fragmentation (regex failed to normalize).
7. **Geo co-fill** — rows with `person_key ∧ state2 ∧ zip5 ≥ 95%` (live 98.08%).
8. **Name-alpha** (positional-offset defense) — `person_key` alpha-char fraction `≥ 0.95`.
9. **Generational preservation** (person-only structural gate) — **zero** `person_key` values end in
   ` JR`/` SR`/` II`/` III`/` IV`/` V` (proves the peel happened), AND `generational_suffix` is non-null
   on `> 0` rows (proves preservation, not silent drop).
10. **Slot-fanout bound** — `max` POC rows per entity `≤ 6` over
    `partition by coalesce(uei, 'CAGE:'||cage_code)` (the structural slot ceiling; catches a unpivot/
    join fan-out bug inherited from a bad source read).
11. **Δ-guards** (vs prior `ops.sam_normalized_pocs_runs` success ≥ `BASELINE_MIN_ROWS=7,800,000`):
    `±25%` on `rows`, `distinct_person_key`, and `distinct_person_geo_triple`. Skip with a logged
    `SKIP` line when there is no floor-qualified prior (first build).

**Post-write (then restore-on-failure per above):**
12. **Write-integrity** — committed `count_rows() == materialized rows`.
13. **Indices present** — all §4 BTREE + BITMAP in the committed manifest.
14. **Round-trip** — a probe `(person_key, state2, zip5)` known-present in the Arrow table returns
    `≥ 1` row carrying the expected `uei`/`cage_code`.
15. **Point-lookup smoke** — a `WHERE person_key = '<known>'` seek returns `≥ 1` row. **Latency logged,
    NOT gated** (a cold R2 first-seek of a freshly-written index is slow and not representative — the
    entity sidecar learned this; gate correctness only, WARN above `SEEK_WARN_MS=2000`).

**Observability (non-failing, logged by `verify_*`):** homonym ratio; count of distinct persons vs POC
occurrences (the audience-collapse factor); generational-suffix fill.

The pre-write gate function (`assert_pre_write_gates(metrics, src_count, baseline) -> list[str]`) is
**pure** (no R2/Modal/PG) and unit-tested in §11.

---

## 9. ops ledger DDL

Create `pipelines/sam_gov/ops_sam_normalized_pocs_runs.sql` (canonical copy) + mirror it verbatim as the
worker's `OPS_DDL`. Same contract as `ops_sam_normalized_entities_runs.sql`.

```sql
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.sam_normalized_pocs_runs (
    id                         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed                       text NOT NULL,        -- 'sam_normalized_pocs'
    dataset_uri                text NOT NULL,        -- s3://data-sink/active/sam_normalized_pocs/
    source_uri                 text,                 -- s3://data-sink/active/sam_pocs/
    sam_extract_label          text,                 -- provenance carried from sam_pocs
    rows_written               bigint,
    distinct_person_key        bigint,
    distinct_person_geo_triple bigint,               -- distinct (person_key,state2,zip5) WHERE country='USA'
    distinct_uei               bigint,
    distinct_cage              bigint,
    status                     text NOT NULL,        -- 'success' | 'error' | 'skipped'
    error                      text,
    started_at                 timestamptz,
    completed_at               timestamptz,
    recorded_at                timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS sam_normalized_pocs_runs_feed_idx        ON ops.sam_normalized_pocs_runs (feed);
CREATE INDEX IF NOT EXISTS sam_normalized_pocs_runs_status_idx      ON ops.sam_normalized_pocs_runs (status);
CREATE INDEX IF NOT EXISTS sam_normalized_pocs_runs_recorded_at_idx ON ops.sam_normalized_pocs_runs (recorded_at DESC);
```

---

## 10. Control plane (Trigger task + dispatcher)

Create **`src/trigger/sam_normalized_pocs.ts`** — copy [`src/trigger/sam_pocs.ts`](../../src/trigger/sam_pocs.ts)
verbatim and swap: `id: "sam-normalized-pocs"`, `app_name: "sam-gov-normalized-pocs-pipelines"`,
`function_name: "build_sam_normalized_pocs"`, callback interface fields
(`distinct_person_key`, `distinct_person_geo_triple`).

**Dependency:** upstream = `sam_pocs` (daily 16:30 UTC, `maxDuration` 65 min → worst-case finish ~17:35).
The sidecar must rebuild **after** `sam_pocs` so it reflects the current vintage.
- **Interim cron:** `0 18 * * *` UTC (margin after `sam_pocs` worst-case), with `skip_if_current=True`
  guarding against a stale or duplicate run (snap-key on `sam_extract_label`).
- **Durable form (preferred next cycle):** chain off `sam_pocs`'s success callback rather than a fixed
  cron — eliminates the race. Wire only after the compute is proven by `modal run`.

No new endpoint, no new secret — the Universal Dispatcher
([`core/modal_dispatcher.py`](../../core/modal_dispatcher.py)) resolves `build_sam_normalized_pocs` by
name and spreads `kwargs` + `trigger_callback_url`.

---

## 11. Tests

Create **`pipelines/sam_gov/tests/test_sam_normalized_pocs_gates.py`** — pure gate unit tests, mirror
[`test_sam_normalized_gates.py`](../../pipelines/sam_gov/tests/test_sam_normalized_gates.py). Cover, at
minimum, one raising test per gate 1–11 plus:
- **person-key construction** (import `core.person_name_norm` builders, run them through an in-memory
  DuckDB on a fixture of ~20 hand-labeled names) asserting:
  - `SMITH JR` / `SMITH` → same `person_key` (`SMITH ...`), distinct `generational_suffix` (`JR` / NULL);
  - `BAILEY, C.E.` shape → `surname_initial_key` = `BAILEY C` matches SAM `BAILEY CHARLES`;
  - honorific/credential stripped (`DR JANE FOX MD` → key `FOX JANE`);
  - whole-token-only peel: surnames `MAY`, `VI`, `JRINKINS`, and `JR`-as-whole-surname are **not**
    mis-peeled (the last → null-surname → null `person_key`, retained row);
  - the homonym-band and generational-preservation gates fire on synthetic violations.

Run: `python -m pytest pipelines/sam_gov/tests/test_sam_normalized_pocs_gates.py -q`.

---

## 12. Execution checklist (do this in order)

```bash
# 0. Branch off main (worktree-aware; never commit on a shared branch)
git checkout -b claude/sam-normalized-pocs origin/main

# 1. Author the files:
#    core/person_name_norm.py                                  (§5 — compose name_norm, do NOT re-inline)
#    pipelines/sam_gov/sam_normalized_pocs.py                  (§6/§7 worker)
#    pipelines/sam_gov/ops_sam_normalized_pocs_runs.sql        (§9 DDL)
#    src/trigger/sam_normalized_pocs.ts                        (§10)
#    pipelines/sam_gov/tests/test_sam_normalized_pocs_gates.py (§11)

# 2. Unit-test the primitive + gates FIRST (pure; no cloud) — finalize the §5 regexes against §11.
python -m pytest pipelines/sam_gov/tests/test_sam_normalized_pocs_gates.py -q

# 3. Create the ops table
modal run pipelines/sam_gov/sam_normalized_pocs.py::init_ops

# 4. DRY-RUN gate — counts only, zero writes. Assert §8 gates 1–11 against the printout.
modal run pipelines/sam_gov/sam_normalized_pocs.py --dry-run

# 5. BUILD + index + verify (the authorized data-plane write of this plan)
modal run pipelines/sam_gov/sam_normalized_pocs.py
#    → confirm verify_* prints all §4 indices, the round-trip (gate 14), point-lookup (gate 15),
#      and the §8 observability (homonym ratio; distinct-persons-vs-occurrences; generational fill).

# 6. (optional) Deploy dispatcher-resolvable + register the Trigger task
modal deploy pipelines/sam_gov/sam_normalized_pocs.py

# 7. Ship — commit, push, PR, MERGE YOURSELF, pull into the operator main checkout, verify
git add core/person_name_norm.py pipelines/sam_gov/sam_normalized_pocs.py \
        pipelines/sam_gov/ops_sam_normalized_pocs_runs.sql \
        pipelines/sam_gov/tests/test_sam_normalized_pocs_gates.py \
        src/trigger/sam_normalized_pocs.ts
git commit -m "feat(sam): sam_normalized_pocs — derived person-key resolution + audience spine"
git push -u origin claude/sam-normalized-pocs
gh pr create --base main --fill && gh pr merge --squash --delete-branch
# then in the operator main checkout: git fetch && git pull --ff-only && git log -1 --oneline
```

A gate failure at step 4 or 5 **stops the ship** — diagnose, do not force past a floor (SAM also purges
registrations; a legitimate shrink must be confirmed before re-baselining, never floored through).

---

## 13. What the sidecar exposes (each consumer defines its own contract — NOT here)

The sidecar is **consumer-agnostic**. It exposes, indexed and inline: the person blocking keys
(`person_key`, `surname_initial_key`, `last_name_norm`, all BTREE), the confirmatory tiebreaks
(`first_initial`, `middle_norm`, `generational_suffix`), inline geo (`state2`, `zip5`), and
`uei`/`cage_code`/`poc_type`/`source_family`/`country` for traversal, filtering, and provenance.

**The two committed consumers — built in their own plans, never here. The boundary is load-bearing:**

- **GTM / audiences → CONTACT (✅).** Collapse occurrences to the targetable grain
  (`DISTINCT ON (person_key, uei)` = human-in-role, recommended for govcon B2B), scope by **USAspending**
  award activity via `uei → crosswalk_sam_usaspending` (clean commercial entity data), enrich
  `employer-domain → email` (`icypeas`/`blitz` gateways — SAM POCs carry identity + employer + postal but
  **no email/phone**, so reachability is a downstream hop on top of this spine), push to `emailbison`.
- **FEC personnel bridge → INTELLIGENCE ONLY (⛔ never a contact source).** A separate
  `pipelines/resolution/crosswalk_fec_sam_pocs.py` artifact, right-side = this sidecar, joining
  `person_key + state2 + zip5`. Used to back into employer and validate via **employer-agreement**
  (`name_norm(fec.employer) = sam_normalized_entities.normalized_legal_name` reached through `uei` — the
  two normalized surfaces interlock on `uei`, and the entity key disambiguates the person homonym). It is
  **never** joined-then-contacted: a blocking match is homonym-prone, and FEC contributor data carries
  statutory use restrictions (52 U.S.C. §30111 / 11 CFR 104.15) against commercial solicitation. Both
  reasons point the same way.

The one promise the sidecar makes: a `person_key` lookup is a BTREE point-seek returning the candidate
POC **set** with geo + entity provenance attached. It does **not** promise a single row per person.

---

## 14. Out of scope

- Any change to `sam_pocs`, `entity_registrations`, `sam_master_entities` (faithful mirrors stay faithful).
  The **ZERO-ALTERATION NAME POLICY** is unchanged — verbatim parts remain SoR in `sam_pocs`; this sidecar
  is additive/derived and never splits or mutates a name.
- **The GTM audience builder** (collapse + USAspending scoping + email enrichment + `emailbison`) — its
  own plan; this builds only the spine it consumes.
- **The FEC personnel bridge** (`crosswalk_fec_sam_pocs`) and ALL FEC-specific match/rank/geo-score logic
  — its own plan, built against the rebuilt `fec_individual_contributions`; intelligence-only (§13).
- **`ffata_exec_comp` / `sam_master_contacts` union** into this spine — later; `ffata_exec_comp`'s opaque
  `FIRST [MID] LAST` officer string needs a sorted-token key (the only place a probabilistic parser is even
  a candidate — `probablepeople`, MIT, output quarantined to a derived column), not the SAM split-parts path.
- **`person_key` ↔ entity-sidecar materialized join** — consumers join `uei` at query time; no pre-joined
  table is built here.
```
