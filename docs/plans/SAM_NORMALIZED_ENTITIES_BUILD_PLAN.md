# `sam_normalized_entities` — Sidecar Build Plan

Plan of record for the **derived normalized-name → UEI resolution sidecar** built off the faithful
golden mirror `sam_master_entities`. Immediately executable: an agent follows §9 top to bottom.

**Status:** approved (operator greenlit 2026-06-05). **Type:** net-new derived Lance dataset + Modal
worker. **Reasoning:** [`FEC_SAM_EMPLOYER_BRIDGE_A_DIAGNOSTIC.md`](../reference/FEC_SAM_EMPLOYER_BRIDGE_A_DIAGNOSTIC.md)
(why employer→UEI is the high-leverage FEC wire-in; why the golden master lacks a normalized key).

---

## 1. Objective

Materialize a thin, super-indexed sidecar that answers one question at index speed: **"what UEI is
this legal-entity name?"** It is the reusable right-side surface for every `name → UEI` bridge (FEC
`employer` first; SoS / GLEIF / HMDA later).

`name_norm(legal_business_name)` is computed **once, here**, and BTREE-indexed — so downstream bridges
pay a point-lookup, not a 1.54M-row full-scan recompute.

**Success =** the dataset exists in R2, BTREE-indexed, validated against the live source, with the
blocking key **byte-identical** to `sos_normalized_master` / `crosswalk_sam_usaspending` (same
`core.name_norm` macro), and a KIPPER round-trip passing.

---

## 2. Why a sidecar (not columns on the mirror)

Decided and locked — full argument in the diagnostic. One-paragraph recap:

`sam_master_entities` is a **faithful mirror** (silver layer): exactly what SAM publishes, deduped,
zero resolution-opinion columns. `name_norm` is an **evolving policy** (the `core.name_norm` module
exists *because* the rule changed and silently broke joins). Putting it in the mirror (a) couples a
mutable key policy to the SoR schema, (b) forces a full 69-col golden rebuild on every macro change,
(c) risks the mirror's multi-output atomic publish, and (d) scatters the key across N schemas instead
of building a unifiable spine. The sidecar isolates all four. The mirror stays the **single full-data
SoR**; the sidecar is a projection/index over it — full data is **not** duplicated. This is the
medallion gold/serving layer + single-source-of-truth-with-projections pattern.

---

## 3. Inputs & output

| | Value |
|---|---|
| **Source (read-only)** | `s3://data-sink/active/sam_master_entities/` — 1,541,566 rows · 1/uei · BTREE(uei, primary_naics, cage_code) |
| **Output** | `s3://data-sink/active/sam_normalized_entities/` (net-new) |
| **Grain** | 1 row / `uei` (pure passthrough — source `uei` is already unique; no dedup) |
| **Est. rows** | ~1,541,566 |
| **Lance version** | `data_storage_version="2.1"` (net-new pin; `02_lancedb_storage.md` §2.3), `max_rows_per_file=1048576` |

The worker reads **only** the mirror. It never touches `sam_master_entities`, `sam_pocs`, or
`entity_registrations`.

---

## 4. Output schema (exact)

Scan these source columns (projection pushdown): `uei`, `legal_business_name`, `cage_code`,
`physical_address_province_or_state`, `physical_address_zip_postal_code`, `is_active`,
`primary_naics`, `sam_extract_label`.

| Column | Type | Derivation | Index |
|---|---|---|---|
| `uei` | string | `nullif(trim(uei),'')` | **BTREE** (spine key) |
| `normalized_legal_name` | string | `name_norm(legal_business_name)` — canonical macro | **BTREE** (primary blocking key) |
| `legal_name_base` | string | `legal_name_base(normalized_legal_name)` — peels LLC/INC/CORP/CO/LTD/PLC | **BTREE** (suffix-drift key) |
| `legal_business_name` | string | verbatim copy (`nullif(trim(...),'')`) | — (human/audit) |
| `cage_code` | string | `nullif(trim(cage_code),'')` | **BTREE** (defense-tail secondary) |
| `physical_state` | string | `nullif(trim(physical_address_province_or_state),'')` | — (geo tiebreak, inline) |
| `physical_zip5` | string | `left(nullif(trim(physical_address_zip_postal_code),''),5)` | — (geo tiebreak, inline) |
| `primary_naics` | string | `nullif(trim(primary_naics),'')` | **BTREE** (sector-scoped resolution) |
| `is_active` | bool | passthrough | **BITMAP** (active-only filter) |
| `sam_extract_label` | string | passthrough (provenance) | — |
| `source_dataset` | string | const `'sam_master_entities'` (provenance) | — |

`physical_state` / `physical_zip5` are **denormalized onto the row on purpose**: resolution blocks on
the name and tiebreaks on state/ZIP in the *same* query, so geo must be inline — a hydration join
mid-resolution would defeat the index. They are not indexed (evaluated against the small per-name
candidate set, not seeked).

**Minimum viable index set** (if trimming): `normalized_legal_name`, `legal_name_base`, `uei`. The
rest (`cage_code`, `primary_naics` BTREE; `is_active` BITMAP) are recommended, cheap, and match the
fleet convention.

---

## 5. The transform (exact SQL)

Pure DuckDB, clean-room. `name_norm` / `legal_name_base` are imported from `core.name_norm` (the
single key definition — **never re-inline the regex**). The `legal_name_base` call references the
`normalized_legal_name` **alias**; DuckDB resolves SELECT-list aliases left-to-right (precedent:
`pipelines/sos_normalized/normalize.py:389-390`).

```python
from core.name_norm import name_norm, legal_name_base

def build_normalized_entities_sql() -> str:
    """Project sam_master_entities → normalized resolution sidecar. Reads a `src` relation
    (the scanned mirror). 1 row/uei passthrough; uei is already unique in source."""
    return f"""
    SELECT
        nullif(trim(uei), '')                                       AS uei,
        {name_norm("legal_business_name")}                          AS normalized_legal_name,
        {legal_name_base("normalized_legal_name")}                  AS legal_name_base,
        nullif(trim(legal_business_name), '')                       AS legal_business_name,
        nullif(trim(cage_code), '')                                 AS cage_code,
        nullif(trim(physical_address_province_or_state), '')        AS physical_state,
        left(nullif(trim(physical_address_zip_postal_code), ''), 5) AS physical_zip5,
        nullif(trim(primary_naics), '')                             AS primary_naics,
        is_active,
        sam_extract_label,
        'sam_master_entities'                                       AS source_dataset
    FROM src
    WHERE nullif(trim(uei), '') IS NOT NULL
    """
```

The build registers the source reader, runs this into a temp table, materializes the Arrow table,
then `lance.write_dataset(... mode="overwrite", data_storage_version="2.1", storage_options=so)` →
`create_scalar_index` per §4. `LANCE_BYPASS_SPILLING=true` (image env) keeps the high-cardinality
`normalized_legal_name` BTREE (1.47M distinct) in-memory (lance#2650). DuckDB: `memory_limit='12GB'`,
`threads=8`, `temp_directory='/tmp/duckdb_spill'`.

---

## 6. Worker file (structure — copy the skeleton, swap the deltas)

Create **`pipelines/sam_gov/sam_normalized_entities.py`** (Modal app `sam-gov-normalized-entities-pipelines`).
Closest skeletons to clone: `pipelines/sam_gov/sam_entity_master.py` (build/ops/dry-run shape) +
`pipelines/resolution/crosswalk_sam_usaspending.py` (the `core.name_norm` import + image).

- **Image:** `debian_slim(python_version="3.12").pip_install("duckdb>=1.5,<2","lancedb>=0.15",
  "pylance>=7","pyarrow>=17","requests>=2.32","psycopg[binary]>=3.2")
  .env({"LANCE_BYPASS_SPILLING":"true"}).add_local_python_source("core.name_norm")`
- **Constants:** `SRC_URI="s3://data-sink/active/sam_master_entities/"`,
  `DATASET_URI=os.environ.get("SAM_NORMALIZED_ENTITIES_URI","s3://data-sink/active/sam_normalized_entities/")`,
  `FEED="sam_normalized_entities"`, `ROW_FLOOR=1_400_000`,
  `BTREE_INDEXES=["uei","normalized_legal_name","legal_name_base","cage_code","primary_naics"]`,
  `BITMAP_INDEXES=["is_active"]`.
- **Functions** (mirror `sam_pocs.py`):
  - `build_sam_normalized_entities(trigger_callback_url=None)` — secrets `r2-credentials` +
    `hqx-postgres`; materialize → floor/uniqueness asserts (§7) → write → index → `_record_run` →
    `_post_callback`; re-raise on failure.
  - `verify_sam_normalized_entities()` — read-back: open from R2, report rows / distinct uei /
    distinct normalized_legal_name / indices / a 6-row sample.
  - `plan_sam_normalized_entities()` — materialize + count, **write nothing** (dry-run gate).
  - `init_ops()` — apply §8 DDL.
  - `@app.local_entrypoint() build(dry_run=False)` — dry-run → `plan_*`; else `build_*` then `verify_*`.
- `_r2_storage_options()`, `_new_con()`, `_pg_connect()`, `_record_run()`, `_post_callback()` — copy
  verbatim from `sam_pocs.py`.

---

## 7. Validation gates (no ship without all green)

Run on the dry-run first (counts only), then re-assert in the build before publish:

1. **Row floor** — `rows ≥ 1,400,000`.
2. **1:1 passthrough** — `rows == count(sam_master_entities)` (only uei-null drop; source uei is 100%).
3. **UEI uniqueness** — `count(distinct uei) == count(*)`.
4. **Key fill** — `normalized_legal_name` non-null `≥ 99.9%` (only all-punctuation names null out).
5. **Cardinality sanity** — `distinct normalized_legal_name` within ±5% of **1,466,764** (probe).
6. **Geo co-fill** — rows with `normalized_legal_name ∧ physical_state ∧ physical_zip5 ≥ 95%`
   (probe: 95.96%).
7. **Δ-guard** — `±25%` row delta vs the prior `ops.sam_normalized_entities_runs` success.
8. **Indices present** — all of §4's BTREE + BITMAP in the committed manifest.
9. **KIPPER round-trip** — `uei='DD1BCRF2QQG8'` → exactly 1 row, `normalized_legal_name` non-null.
10. **Point-lookup smoke** — a `WHERE normalized_legal_name = '<known>'` seek returns < 100 ms.

---

## 8. ops ledger DDL

Create `pipelines/sam_gov/ops_sam_normalized_entities_runs.sql` (canonical copy) + mirror it verbatim
as the worker's `OPS_DDL`. Same contract as `ops_sam_pocs_runs.sql`.

```sql
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.sam_normalized_entities_runs (
    id                       bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed                     text NOT NULL,        -- 'sam_normalized_entities'
    dataset_uri              text NOT NULL,        -- s3://data-sink/active/sam_normalized_entities/
    source_uri               text,                 -- s3://data-sink/active/sam_master_entities/
    sam_extract_label        text,                 -- provenance carried from the mirror
    rows_written             bigint,
    distinct_uei             bigint,
    distinct_normalized_name bigint,
    status                   text NOT NULL,        -- 'success' | 'error'
    error                    text,
    started_at               timestamptz,
    completed_at             timestamptz,
    recorded_at              timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS sam_normalized_entities_runs_feed_idx        ON ops.sam_normalized_entities_runs (feed);
CREATE INDEX IF NOT EXISTS sam_normalized_entities_runs_status_idx      ON ops.sam_normalized_entities_runs (status);
CREATE INDEX IF NOT EXISTS sam_normalized_entities_runs_recorded_at_idx ON ops.sam_normalized_entities_runs (recorded_at DESC);
```

---

## 9. Execution checklist (do this in order)

```bash
# 0. Branch off main (worktree-aware; never commit on a shared branch)
git checkout -b claude/sam-normalized-entities origin/main

# 1. Author the two files (§5/§6 worker, §8 ops DDL). Import core.name_norm — do NOT re-inline.

# 2. Create the ops table
modal run pipelines/sam_gov/sam_normalized_entities.py::init_ops

# 3. DRY-RUN gate — counts only, zero writes. Assert §7 gates 1–6 by hand against the printout.
modal run pipelines/sam_gov/sam_normalized_entities.py --dry-run

# 4. BUILD + index + verify (this is the authorized data-plane write of this plan)
modal run pipelines/sam_gov/sam_normalized_entities.py
#    → confirm verify_* prints all §4 indices, KIPPER round-trip (gate 9), point-lookup (gate 10).

# 5. (optional) Deploy dispatcher-resolvable
modal deploy pipelines/sam_gov/sam_normalized_entities.py

# 6. Ship — commit, push, PR, MERGE YOURSELF, pull into the main worktree, verify
git add pipelines/sam_gov/sam_normalized_entities.py pipelines/sam_gov/ops_sam_normalized_entities_runs.sql
git commit -m "feat(sam): sam_normalized_entities — derived name_norm→UEI resolution sidecar"
git push -u origin claude/sam-normalized-entities
gh pr create --base main --fill && gh pr merge --squash --delete-branch
# then in the operator main checkout: git fetch && git merge --ff-only origin/main && git log -1 --oneline
```

A gate failure at step 3 or 4 **stops the ship** — diagnose, do not force past a floor.

---

## 10. Consumer contract (how the sidecar is used — not built here)

The first consumer, `crosswalk_fec_sam_employer` (separate, downstream), joins it thus:

```sql
SELECT f.sub_id, e.uei, e.legal_business_name, e.is_active, e.primary_naics
FROM   fec_left f                                   -- name_norm(employer)+state2+zip5, sentinels dropped
JOIN   sam_normalized_entities e
  ON   e.normalized_legal_name = f.emp_key          -- BTREE exact (Pass-1)
 AND   e.physical_state        = f.state2;          -- inline geo discriminant
-- Pass-2 drift: e.legal_name_base = legal_name_base(f.emp_key).  zip5 = confirmatory boost.
-- e.uei → joins crosswalk_sam_usaspending, contractor_award_summary, sam_pocs, ffata_exec_comp.
```

Hydration consumers still read `sam_master_entities` by `uei` (already BTREE'd). Full data lives once.

---

## 11. Refresh / orchestration

- **Dependency:** upstream = `sam_master_entities` (`sam_master.py`). The sidecar must rebuild **after**
  the mirror so it reflects the current vintage.
- **Now (interim):** manual `modal run` after each master build.
- **Phase 2 (control plane):** chain a Trigger v4 durable step on `sam_master`'s success callback (or
  register in the Universal Dispatcher keyed to fire on master completion), mirroring `sam_pocs.py`'s
  callback pattern. Wire only after the compute is proven by `modal run`.

---

## 12. Out of scope

- Any change to `sam_master_entities`, `sam_pocs`, `entity_registrations` (faithful mirrors stay faithful).
- `crosswalk_fec_sam_employer` (the FEC bridge — separate downstream build, §10 is its contract only).
- The **POC sidecar** (`sam_normalized_pocs` off `sam_pocs`: `person_key=name_norm(last+first)` BTREE,
  sorted-token key for FFATA order-flip, geo already inline) — same pattern, its own plan, secondary
  (greenfield/homonym-heavy per the personnel diagnostic).
- Union of `sam_normalized_entities` + `sos_normalized_master` into a cross-source name spine — later.
- Retiring the outdated thin `sam_entity_master` — tracked separately.
```
