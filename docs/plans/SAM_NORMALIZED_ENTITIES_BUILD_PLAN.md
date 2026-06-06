# `sam_normalized_entities` — Sidecar Build Plan

Plan of record for the **derived normalized-name → UEI resolution sidecar** built off the faithful
golden mirror `sam_master_entities`. Immediately executable: an agent follows §9 top to bottom.

**Status:** approved (operator greenlit 2026-06-05); **re-scoped 2026-06-05 — FEC de-scoped, sidecar
is consumer-agnostic.** **Type:** net-new derived Lance dataset + Modal worker. **Reasoning:**
[`FEC_SAM_EMPLOYER_BRIDGE_A_DIAGNOSTIC.md`](../reference/FEC_SAM_EMPLOYER_BRIDGE_A_DIAGNOSTIC.md)
(entity-master canonicalization; why the golden master lacks a normalized key — note its FEC-bridge
geo claim is superseded by the review §B2, and the bridge is deferred).

> **Scope cut (2026-06-05).** This builds the **SAM-side resolution sidecar only** — it has **zero FEC
> dependency** (reads `sam_master_entities`, never `fec_individual_contributions`, which is being
> rebuilt). The FEC employer bridge and all FEC-specific match logic are **out of scope** (§12).
> Amendments from [`SAM_NORMALIZED_ENTITIES_BUILD_PLAN_REVIEW.md`](SAM_NORMALIZED_ENTITIES_BUILD_PLAN_REVIEW.md)
> adopted here are the **SAM-intrinsic** ones (probed against `sam_master_entities`, stable across the
> FEC rebuild): **B4** (column names), **B5** (publish safety), **B6** (non-redundancy), **B8** (`CO`
> over-peel surfaced), **B9** (cardinality floor), and **B3's multiplicity *fact*** as a documented
> property. The FEC-derived findings (**B1** key-ranking, **B2** geo-as-score, **B3** handling, **B7**
> active-filter) are vintage-dependent *consumer* logic — deferred to the FEC-bridge plan.

---

## 1. Objective

Materialize a thin, super-indexed sidecar that answers one question at index speed: **"what UEI is
this legal-entity name?"** It is the **consumer-agnostic** reusable right-side surface for any
`name → UEI` bridge (SoS / GLEIF / HMDA / a future FEC `employer` bridge) — the sidecar makes no
assumption about which consumer comes first or how that consumer ranks, gates, or disambiguates.

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

**Not redundant with `crosswalk_sam_usaspending`** (review §B6): that dataset's `normalized_legal_name`
BTREE is **51.6% fill**, recipient-anchored (1.03M USAspending-spend UEIs), and **geo-less**; this
sidecar is **100% fill** over the full 1.54M registrant universe (incl. 759,023 historical) with inline
geo — the only surface that carries geo + the historical tail a name→UEI match needs.

---

## 3. Inputs & output

| | Value |
|---|---|
| **Source (read-only)** | `s3://data-sink/active/sam_master_entities/` — 1,541,566 rows · 1/uei · BTREE(uei, primary_naics, cage_code) |
| **Output** | `s3://data-sink/active/sam_normalized_entities/` (net-new) |
| **Grain** | 1 row / `uei` (pure passthrough — source `uei` is already unique; no dedup). NOTE: 1/uei does **not** mean 1/name — the name axis is non-unique (§4). |
| **Est. rows** | ~1,541,566 |
| **Lance version** | `data_storage_version="2.1"` (net-new pin; `02_lancedb_storage.md` §2.3), `max_rows_per_file=1048576` |

The worker reads **only** the mirror. It never touches `sam_master_entities`, `sam_pocs`, or
`entity_registrations`. It has no FEC input.

---

## 4. Output schema (exact)

Scan these source columns (projection pushdown): `uei`, `legal_business_name`, `cage_code`,
`physical_address_province_or_state`, `physical_address_zip_postal_code`, `is_active`,
`primary_naics`, `sam_extract_label`.

| Column | Type | Derivation | Index |
|---|---|---|---|
| `uei` | string | `nullif(trim(uei),'')` | **BTREE** (spine key) |
| `normalized_legal_name` | string | `name_norm(legal_business_name)` — canonical macro | **BTREE** (exact blocking key) |
| `legal_name_base` | string | `legal_name_base(normalized_legal_name)` — peels LLC/INC/CORP/CO/LTD/PLC | **BTREE** (suffix-peeled blocking key) |
| `legal_business_name` | string | verbatim copy (`nullif(trim(...),'')`) | — (human/audit) |
| `cage_code` | string | `nullif(trim(cage_code),'')` | **BTREE** (defense-tail secondary) |
| `source_state` | string | `nullif(trim(physical_address_province_or_state),'')` | — (geo tiebreak, inline) |
| `zip_code` | string | `left(nullif(trim(physical_address_zip_postal_code),''),5)` | — (geo tiebreak, inline) |
| `primary_naics` | string | `nullif(trim(primary_naics),'')` | **BTREE** (sector-scoped resolution) |
| `is_active` | bool | passthrough | **BITMAP** (active-only filter) |
| `sam_extract_label` | string | passthrough (provenance) | — |
| `source_dataset` | string | const `'sam_master_entities'` (provenance) | — |

`source_state` / `zip_code` are **denormalized onto the row on purpose**: a consumer blocks on the
name and tiebreaks on state/ZIP in the *same* query, so geo must be inline — a hydration join
mid-resolution would defeat the index. They are not indexed (evaluated against the small per-name
candidate set, not seeked). **Column names match `sos_normalized_master`** (`source_state`/`zip_code`,
review §B4) so the deferred cross-source union (§12) is a literal `UNION ALL`, not a rename migration.

**Both name keys are exposed, neither is privileged.** `normalized_legal_name` (exact) and
`legal_name_base` (suffix-peeled) are both BTREE'd; *which one a consumer blocks on first* is the
consumer's call, not the sidecar's. Two measured properties a consumer must plan around:
- **`name → uei` is many-to-one** (review §B3): 37,598 `normalized_legal_name` (2.56%) map to >1 uei;
  112,400 uei (7.29%) sit under a non-unique name; max fan-out **2,184** (`THE SHERWIN WILLIAMS
  COMPANY` — branch registrations). `legal_name_base` fans out more (3.43%). The sidecar is 1/uei
  (clean on the uei axis); **the join axis (name) is not unique** — consumers resolve candidate sets;
  the sidecar makes no single-row-join promise.
- **`legal_name_base` over-peels a bare trailing `CO`** on 11,147 rows / 10,619 names (review §B8,
  e.g. `WOODMANS BREWING CO → WOODMANS BREWING`) — measured, accepted (the shared macro is not touched
  here), surfaced by the verify step (§7).

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
        nullif(trim(physical_address_province_or_state), '')        AS source_state,
        left(nullif(trim(physical_address_zip_postal_code), ''), 5) AS zip_code,
        nullif(trim(primary_naics), '')                             AS primary_naics,
        is_active,
        sam_extract_label,
        'sam_master_entities'                                       AS source_dataset
    FROM src
    WHERE nullif(trim(uei), '') IS NOT NULL
    """
```

The build registers the source reader, runs this into a temp table, materializes the Arrow table,
**asserts the pre-write gates on that Arrow table (§7)**, then
`lance.write_dataset(... mode="overwrite", data_storage_version="2.1", storage_options=so)` →
`create_scalar_index` per §4. `LANCE_BYPASS_SPILLING=true` (image env) keeps the high-cardinality
`normalized_legal_name` BTREE (1.47M distinct) in-memory (lance#2650). DuckDB: `memory_limit='12GB'`,
`threads=8`, `temp_directory='/tmp/duckdb_spill'`.

---

## 6. Worker file (structure — copy the skeleton, swap the deltas)

Create **`pipelines/sam_gov/sam_normalized_entities.py`** (Modal app `sam-gov-normalized-entities-pipelines`).
Closest skeletons to clone: `pipelines/sam_gov/sam_entity_master.py` (build/ops/dry-run shape) +
`pipelines/resolution/crosswalk_sam_usaspending.py` (the `core.name_norm` import + image + the
`version=v_before).restore()` rollback at lines 520-598).

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
    `hqx-postgres`; materialize → **pre-write gate asserts on the Arrow table (§7, gates 1–7)** →
    capture `v_before` → write → index → **post-write gates 8–10, `restore(v_before)` on failure** →
    `_record_run` → `_post_callback`; re-raise on failure.
  - `verify_sam_normalized_entities()` — read-back: open from R2, report rows / distinct uei /
    distinct normalized_legal_name / **distinct legal_name_base** / indices / a 6-row sample, and the
    §7 observability counts (legal_name_base multi-uei collision rate; bare-` CO`-peel count).
  - `plan_sam_normalized_entities()` — materialize + count + run gates 1–7, **write nothing** (dry-run).
  - `init_ops()` — apply §8 DDL.
  - `@app.local_entrypoint() build(dry_run=False)` — dry-run → `plan_*`; else `build_*` then `verify_*`.
- `_r2_storage_options()`, `_new_con()`, `_pg_connect()`, `_record_run()`, `_post_callback()` — copy
  verbatim from `sam_pocs.py`.

---

## 7. Validation gates (no ship without all green)

**Gate timing (review §B5).** Gates 1–7 are computable on the in-memory Arrow table and **MUST
hard-fail BEFORE `write_dataset` overwrites the live dataset** — the dry-run proves counts, but the
build re-proves them in-memory, never trusting the dry-run. Gates 8–10 are post-write; on any
post-write failure, **roll back**: capture `v_before = lance.dataset(uri).version` before the write
and on failure `lance.dataset(uri, version=v_before).restore()` then re-raise (clone the proven
pattern at `crosswalk_sam_usaspending.py:520-598`). For the first build `v_before` is empty; the guard
protects every *subsequent* rebuild — when a `name_norm` regression actually bites.

**Pre-write (on the Arrow table, hard-fail before overwrite):**
1. **Row floor** — `rows ≥ 1,400,000`.
2. **1:1 passthrough** — `rows == count(sam_master_entities)` (only uei-null drop; source uei is 100%).
3. **UEI uniqueness** — `count(distinct uei) == count(*)`.
4. **Key fill** — `normalized_legal_name` non-null `≥ 99.9%` (only all-punctuation names null out).
5. **Cardinality sanity** — `distinct normalized_legal_name` within ±5% of **1,466,764**, AND
   `distinct legal_name_base` within ±5% of **1,450,598** (review §B9 — floors *both* keys so a
   peel-set regression is caught).
6. **Geo co-fill** — rows with `normalized_legal_name ∧ source_state ∧ zip_code ≥ 95%` (probe: 95.96%).
7. **Δ-guard** — `±25%` row delta vs the prior `ops.sam_normalized_entities_runs` success.

**Post-write (then restore-on-failure per above):**
8. **Indices present** — all of §4's BTREE + BITMAP in the committed manifest.
9. **KIPPER round-trip** — `uei='DD1BCRF2QQG8'` → exactly 1 row, `normalized_legal_name` non-null.
10. **Point-lookup smoke** — a `WHERE normalized_legal_name = '<known>'` seek returns < 100 ms.

**Observability (non-failing, logged by `verify_*`, review §B8):** the `legal_name_base` multi-uei
collision rate and the count of bare-` CO`-peeled rows (~11,147) — so the peel trade stays visible.

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
    distinct_legal_name_base bigint,               -- review §B9 floor
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
#    Wire the pre-write Arrow-table gates (§7 gates 1–7) and the v_before restore() guard.

# 2. Create the ops table
modal run pipelines/sam_gov/sam_normalized_entities.py::init_ops

# 3. DRY-RUN gate — counts only, zero writes. Assert §7 gates 1–7 against the printout.
modal run pipelines/sam_gov/sam_normalized_entities.py --dry-run

# 4. BUILD + index + verify (this is the authorized data-plane write of this plan)
modal run pipelines/sam_gov/sam_normalized_entities.py
#    → confirm verify_* prints all §4 indices, KIPPER round-trip (gate 9), point-lookup (gate 10),
#      and the §7 observability counts (legal_name_base collision rate; CO-peel count).

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

## 10. What the sidecar exposes (each consumer defines its own contract — NOT here)

The sidecar is **consumer-agnostic**. It exposes, indexed and inline:
- two name blocking keys — `normalized_legal_name` (exact) and `legal_name_base` (suffix-peeled), both BTREE;
- geo — `source_state`, `zip_code` (inline, for tiebreaking without a hydration join);
- `is_active`, `primary_naics`, `cage_code`, `sam_extract_label` for filtering / provenance.

**How a bridge uses these is the bridge's plan, not this one.** Deferred to each downstream consumer
(and, for FEC, to a bridge built against the *rebuilt* `fec_individual_contributions`):
- **which key to block on** — free-text inputs that omit `LLC`/`INC` favor `legal_name_base`; exact
  registry inputs favor `normalized_legal_name` (review §B1);
- **how to use geo** — a confirmatory **score**, *not* a hard equality, whenever the two sides'
  addresses are different loci (e.g. a person's residence vs an entity HQ) — review §B2;
- **how to resolve `name → uei` multiplicity** — the join axis is non-unique (§4); a consumer MUST
  emit a candidate set + confidence / a deterministic canonical pick, **never a silent fan-out**
  (review §B3).

The one promise the sidecar makes: a name lookup is a BTREE point-seek returning the candidate UEI
**set** with geo attached. It does **not** promise a single row per name. Hydration consumers read
`sam_master_entities` by `uei` (already BTREE'd) — full data lives once.

---

## 11. Refresh / orchestration

- **Dependency:** upstream = `sam_master_entities` (`sam_master.py`). The sidecar must rebuild **after**
  the mirror so it reflects the current vintage. (No FEC dependency — the FEC rebuild is irrelevant to
  this build.)
- **Now (interim):** manual `modal run` after each master build.
- **Phase 2 (control plane):** chain a Trigger v4 durable step on `sam_master`'s success callback (or
  register in the Universal Dispatcher keyed to fire on master completion), mirroring `sam_pocs.py`'s
  callback pattern. Wire only after the compute is proven by `modal run`.

---

## 12. Out of scope

- Any change to `sam_master_entities`, `sam_pocs`, `entity_registrations` (faithful mirrors stay faithful).
- **The FEC employer bridge (`crosswalk_fec_sam_employer`) and ALL FEC-specific match logic** —
  key-ranking (review §B1), geo scoring (§B2), fan-out handling (§B3), active-window filtering (§B7).
  Its own future plan, built against the **rebuilt** `fec_individual_contributions`. The review's
  FEC-derived figures (match rates, home-vs-HQ %) are that-vintage-specific and are **not** carried
  into this SAM-side plan.
- The **POC sidecar** (`sam_normalized_pocs` off `sam_pocs`: `person_key=name_norm(last+first)` BTREE,
  sorted-token key for FFATA order-flip, geo already inline) — same pattern, its own plan, secondary
  (greenfield/homonym-heavy per the personnel diagnostic).
- Union of `sam_normalized_entities` + `sos_normalized_master` into a cross-source name spine — later;
  after the §B4 rename it is a literal `UNION ALL` (a name-spine with nullable source-specific columns,
  discriminated by `source_dataset`).
- Retiring the outdated thin `sam_entity_master` — tracked separately.
```
