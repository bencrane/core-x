# `crosswalk_sos_sam` — Build Plan (SoS → SAM name → UEI)

Plan of record for the **first real consumer of `sam_normalized_entities`**: a name-keyed crosswalk
that attaches a federal **UEI** to every Secretary-of-State registrant that is also a SAM entity.
Immediately executable: an agent follows §9 top to bottom.

**Status:** drafted 2026-06-05. **Type:** net-new derived Lance crosswalk + Modal worker.
**Reasoning:** the `name → UEI` sidecar ([`SAM_NORMALIZED_ENTITIES_BUILD_PLAN.md`](SAM_NORMALIZED_ENTITIES_BUILD_PLAN.md),
built + live) is a dormant surface until something joins it. SoS is the **unblocked** first consumer —
`sos_normalized_master` already carries the byte-identical `core.name_norm` blocking key, it has **zero
FEC dependency** (the FEC bridge stays deferred), and it extends the entity graph: SoS state
registration ↔ federal UEI ↔ awards / POCs / exec-comp (the GTM control-surface path). This build also
**exercises** the sidecar under a real join, validating the multiplicity + geo handling the sidecar's
adversarial review only theorized.

> **Every figure below is a live read-only probe (2026-06-05).** Harnesses `/tmp/sos_sam_meta.py`,
> `/tmp/sos_sam_sizing.py`. Two schema traps were caught by probing and are designed around, not
> assumed (§0).

---

## 0. Live sizing + the two traps (probed, not assumed)

**Inputs (live):**
- `sos_normalized_master` — **17,926,543 rows / 17,843,033 entities / 16,822,318 distinct names**.
  BTREE `normalized_legal_name`, `source_state`, `zip_code`. **No `legal_name_base` column** (compute
  on the fly for the base pass). Entity key = (`source_state`, `original_entity_id`).
- `sam_normalized_entities` — 1,541,566 rows / 1/uei. BTREE `normalized_legal_name`, `legal_name_base`,
  `uei`, `cage_code`, `primary_naics`; BITMAP `is_active`.

**Match sizing (Σ-of-products, no pair materialization):**

| Block key | shared keys | SoS rows covered | SAM UEIs reached | candidate pairs |
|---|---|---|---|---|
| `normalized_legal_name` (exact) | 306,413 | 427,279 (2.38% of SoS) | **339,555** | 502,102 |
| `legal_name_base` (suffix-peeled) | 351,796 | 623,147 (3.48% of SoS) | **400,325** | 775,517 |

The 2–3% "of SoS" is the wrong denominator to fixate on — SoS is 17.8M mostly-tiny local LLCs. The
load-bearing number is **~26% of the entire SAM federal universe (400,325 of 1.54M UEIs) ties to a
state registration.** Output is bounded at **≤ ~775k pairs** — no cartesian blow-up.

**Trap 1 — geo locus (proven).** `sos_normalized_master` has *two* geo signals: `source_state` = the
registration **jurisdiction**; `state`/`zip_code` = the entity's **physical** address. SAM's
`source_state` is the **physical** HQ. So the correct like-for-like confirm is `sos.state ↔
sam.source_state`. On 218,195 clean 1:1 name matches:

| Comparison | agreement | verdict |
|---|---|---|
| `sos.state` (physical) == `sam.source_state` (physical) | **73.03%** | ✅ correct locus |
| `sos.zip_code` == `sam.zip_code` | 56.41% | confirmatory |
| `sos.source_state` (**jurisdiction**) == `sam.source_state` | 64.06% | ❌ wrong locus, measurably worse |

**Trap 2 — geo scores, never gates.** Even the *correct* locus agrees only 73% — 27% of clean,
unambiguous name matches have a differing physical state (registered-agent addresses, multi-site
firms, relocations). A hard state-equality JOIN predicate would silently delete a quarter of true
matches (the sidecar review's B2 lesson, re-confirmed on this dataset). **Geo → a `match_tier` score,
not a filter.**

---

## 1. Objective

Materialize `crosswalk_sos_sam`: one row per matched **(SoS entity, SAM UEI)** candidate, scored by a
name+geo precision ladder, with a deterministic **canonical pick** per SoS entity. Resolves
"what federal UEI is this state-registered company?" at index speed, and exposes the full candidate
set for audit.

**Success =** the dataset exists in R2, BTREE-indexed; exactly one `is_canonical` row per matched SoS
entity; coverage within ±10% of the probed reach (≈400k UEIs, ≈623k SoS entities); a tier-1 round-trip
passing.

---

## 2. Inputs & output

| | Value |
|---|---|
| **Left (read-only)** | `s3://data-sink/active/sos_normalized_master/` — 17.93M rows |
| **Right (read-only)** | `s3://data-sink/active/sam_normalized_entities/` — 1.54M rows |
| **Output** | `s3://data-sink/active/crosswalk_sos_sam/` (net-new) |
| **Grain** | 1 row per matched (SoS entity, UEI) candidate pair; `is_canonical` marks the best UEI per SoS entity |
| **Est. rows** | ~775k (base-superset join; ~623k canonical + non-canonical candidates) |
| **Lance** | `data_storage_version="2.1"`, `max_rows_per_file=1048576` |

Reads only the two normalized layers. Never touches `sos_normalized_master`, `sam_normalized_entities`,
or any mirror. No FEC input.

---

## 3. Output schema (exact)

| Column | Type | Source |
|---|---|---|
| `sos_entity_key` | string | `sos_source_state ‖ ':' ‖ original_entity_id` — **BTREE** (reverse lookup) |
| `sos_source_state` | string | SoS jurisdiction |
| `sos_original_entity_id` | string | SoS entity id (unique within jurisdiction) |
| `sos_normalized_legal_name` | string | the join key — **BTREE** (audit) |
| `sos_source_entity_name` | string | SoS verbatim name (audit) |
| `sos_state` | string | SoS **physical** state |
| `sos_zip_code` | string | SoS **physical** zip5 |
| `sos_entity_status` | string | ACTIVE / TERMINATED / FORFEITED / … |
| `uei` | string | **the resolved SAM key** — **BTREE** (forward lookup) |
| `sam_normalized_legal_name` | string | SAM blocking key (audit) |
| `sam_legal_business_name` | string | SAM verbatim name (audit) |
| `sam_source_state` | string | SAM **physical** state |
| `sam_zip_code` | string | SAM **physical** zip5 |
| `sam_is_active` | bool | SAM active flag |
| `sam_primary_naics` | string | sector |
| `match_key` | string | `'normalized_legal_name'` \| `'legal_name_base'` — **BITMAP** |
| `match_tier` | int8 | 1=exact+state · 2=exact · 3=base+state · 4=base — **BITMAP** |
| `match_confidence` | string | `high`(1) / `medium_high`(2) / `medium`(3) / `low`(4) (ergonomic label) |
| `state_confirms` | bool | `sos_state == sam_source_state` (physical↔physical) |
| `zip_confirms` | bool | `sos_zip_code == sam_zip_code` |
| `is_canonical` | bool | best UEI per `sos_entity_key` — **BITMAP** |
| `source_dataset` | string | const `'crosswalk_sos_sam'` (provenance) |

Indexes: BTREE `uei`, `sos_entity_key`, `sos_normalized_legal_name`; BITMAP `match_tier`,
`is_canonical`, `match_key`.

---

## 4. Match design (the heart)

**Key ladder — registry↔registry favors EXACT (the inverse of the FEC employer call).** Both sides are
clean registry legal names that carry the corporate form, so the exact `normalized_legal_name` is the
high-precision key; `legal_name_base` is the recall fallback for suffix drift (`INC` vs `INCORPORATED`,
`CO` vs `COMPANY`). This is deliberately the **opposite** of the sidecar review's B1 (which favored
`legal_name_base` for *free-text* FEC employer strings) — context decides, not cargo-cult.

Implementation: **one join on `legal_name_base`** (the superset — equal-normalized ⟹ equal-base, so it
captures every exact match plus the suffix-drift recall), then label `match_key='normalized_legal_name'`
when the normalized names are also equal. SoS `legal_name_base` is computed on the fly (no column);
SAM's is the indexed column.

**Geo — a score on the correct locus (§0 traps).** `state_confirms = sos_state == sam_source_state`
(physical↔physical, the 73% signal), `zip_confirms = sos_zip_code == sam_zip_code`. Both feed
`match_tier`; **neither gates membership.** `sos.source_state` (jurisdiction) is NOT used for the
confirm.

**Multiplicity — candidate set + canonical pick (no silent fan-out, the B3 lesson).** Names are
non-unique on **both** sides. Emit every matched pair; mark exactly one `is_canonical` per
`sos_entity_key` by the deterministic ranking
`match_tier ASC, state_confirms DESC, zip_confirms DESC, sam_is_active DESC, uei ASC`. A consumer that
wants a single UEI filters `is_canonical`; one that wants the full set reads all rows.

---

## 5. The transform (exact SQL)

`legal_name_base` imported from `core.name_norm` (computes the SoS-side base; never re-inlined).

```python
from core.name_norm import legal_name_base

def build_crosswalk_sql() -> str:
    """Join sos (1 row/entity, base computed) to sam on legal_name_base; label, score, rank.
    Reads `sos_src` and `sam_src` relations (the scanned layers)."""
    return f"""
    WITH sos AS (
        SELECT
            source_state || ':' || original_entity_id        AS sos_entity_key,
            source_state                                     AS sos_source_state,
            original_entity_id                               AS sos_original_entity_id,
            normalized_legal_name                            AS sos_normalized_legal_name,
            source_entity_name                               AS sos_source_entity_name,
            state                                            AS sos_state,
            left(nullif(trim(zip_code), ''), 5)              AS sos_zip_code,
            entity_status                                    AS sos_entity_status,
            {legal_name_base("normalized_legal_name")}       AS sos_legal_name_base
        FROM sos_src
        WHERE normalized_legal_name IS NOT NULL
        QUALIFY row_number() OVER (
            PARTITION BY source_state, original_entity_id
            ORDER BY snapshot_date DESC NULLS LAST) = 1          -- 1 row/entity (latest snapshot)
    ),
    pairs AS (
        SELECT
            s.sos_entity_key, s.sos_source_state, s.sos_original_entity_id,
            s.sos_normalized_legal_name, s.sos_source_entity_name, s.sos_state,
            s.sos_zip_code, s.sos_entity_status,
            m.uei, m.normalized_legal_name AS sam_normalized_legal_name,
            m.legal_business_name AS sam_legal_business_name,
            m.source_state AS sam_source_state, m.zip_code AS sam_zip_code,
            m.is_active AS sam_is_active, m.primary_naics AS sam_primary_naics,
            (m.normalized_legal_name = s.sos_normalized_legal_name) AS exact_name,
            (upper(trim(s.sos_state)) = upper(trim(m.source_state))) AS state_confirms,
            (s.sos_zip_code = left(nullif(trim(m.zip_code), ''), 5)) AS zip_confirms
        FROM sos s
        JOIN sam_src m ON m.legal_name_base = s.sos_legal_name_base   -- base superset
    ),
    scored AS (
        SELECT *,
            CASE WHEN exact_name THEN 'normalized_legal_name' ELSE 'legal_name_base' END AS match_key,
            CASE WHEN exact_name AND state_confirms THEN 1
                 WHEN exact_name                    THEN 2
                 WHEN state_confirms                THEN 3
                 ELSE 4 END                                       AS match_tier
        FROM pairs
    )
    SELECT
        * EXCLUDE (exact_name),
        CASE match_tier WHEN 1 THEN 'high' WHEN 2 THEN 'medium_high'
                        WHEN 3 THEN 'medium' ELSE 'low' END        AS match_confidence,
        (row_number() OVER (
            PARTITION BY sos_entity_key
            ORDER BY match_tier ASC, state_confirms DESC, zip_confirms DESC,
                     sam_is_active DESC, uei ASC) = 1)             AS is_canonical,
        'crosswalk_sos_sam'                                        AS source_dataset
    FROM scored
    """
```

`sos_src` is scanned with columns `source_state, original_entity_id, normalized_legal_name,
source_entity_name, state, zip_code, entity_status, snapshot_date`; `sam_src` with
`uei, normalized_legal_name, legal_name_base, legal_business_name, source_state, zip_code, is_active,
primary_naics`. DuckDB `memory_limit='24GB'`, `threads=8`, spill `/tmp/duckdb_spill`;
`LANCE_BYPASS_SPILLING=true` for the index sort.

---

## 6. Worker file (structure — clone the skeleton)

Create **`pipelines/resolution/crosswalk_sos_sam.py`** (Modal app `resolution-sos-sam-pipelines`).
Closest skeleton: `pipelines/resolution/crosswalk_hmda_gleif.py` (name-based crosswalk: `core.name_norm`
import, two-source DuckDB join, ops ledger with match-rate, dry-run, Trigger callback) +
`crosswalk_sam_usaspending.py` for the `version=v_before).restore()` rollback (lines 520-598).

- **Image:** `debian_slim(3.12).pip_install("duckdb>=1.5,<2","lancedb>=0.15","pylance>=7","pyarrow>=17",
  "requests>=2.32","psycopg[binary]>=3.2").env({"LANCE_BYPASS_SPILLING":"true"})
  .add_local_python_source("core.name_norm")`
- **Constants:** `SOS_URI`, `SAM_URI`, `DATASET_URI=…/crosswalk_sos_sam/`, `FEED="crosswalk_sos_sam"`,
  `BTREE_INDEXES=["uei","sos_entity_key","sos_normalized_legal_name"]`,
  `BITMAP_INDEXES=["match_tier","is_canonical","match_key"]`, `ROW_FLOOR=500_000`.
  Modal `memory=49152, cpu=8.0` (the 17.9M-row SoS scan is the heavy input).
- **Functions** (mirror `crosswalk_hmda_gleif.py`): `build_crosswalk` (gates → `v_before` → write →
  index → post-write gates → restore-on-failure → ops + callback), `verify_crosswalk` (read-back +
  tier/coverage distribution), `plan_crosswalk` (dry-run: materialize + gates, no write), `init_ops`,
  `@app.local_entrypoint() build(dry_run=False)`.
- `_r2_storage_options` / `_new_con` / `_pg_connect` / `_record_run` / `_post_callback` — copy verbatim.

---

## 7. Validation gates (no ship without all green)

**Pre-write (on the Arrow table, hard-fail before overwrite):**
1. **Row floor** — `rows ≥ 500,000`.
2. **Canonical uniqueness** — `count(*) FILTER (is_canonical) == count(DISTINCT sos_entity_key)`
   (exactly one canonical UEI per matched SoS entity).
3. **No orphans** — `count(*) FILTER (uei IS NULL OR sos_entity_key IS NULL) == 0` (inner join invariant).
4. **Coverage** — `distinct uei` within ±10% of **400,325**; `distinct sos_entity_key` within ±10% of
   **623,147** (probe baselines).
5. **Geo-score sanity** — `state_confirms` rate among tier∈{1,3}-eligible exact+base matches ≈ **73%**
   (±5pp vs probe); guards against a locus regression (e.g. someone wiring `sos.source_state`).
6. **Tier monotonicity** — every row has `match_tier ∈ {1,2,3,4}` and `match_key` consistent with it.
7. **Δ-guard** — `±25%` row delta vs the prior `ops.crosswalk_sos_sam_runs` success (skip if first).

**Post-write (then `restore(v_before)` on failure):**
8. **Indices present** — all §3 BTREE + BITMAP in the manifest.
9. **Tier-1 round-trip** — pick any `match_tier=1, is_canonical` row; assert `uei` resolves in
   `sam_normalized_entities` and `sos_entity_key` resolves in `sos_normalized_master`.
10. **Point-lookup** — a `WHERE uei = '<known>'` seek returns its SoS candidates (BTREE seek; <2s R2
    ceiling, warm target <100ms — same remote-RTT caveat as the sidecar's gate 10).

**Observability (non-failing, `verify_*`):** rows by `match_tier`; canonical rows by `match_confidence`;
`state_confirms`/`zip_confirms` rates; distinct UEIs reached; the top fan-out names (max SoS-entities
per UEI and vice-versa).

---

## 8. ops ledger DDL

Create `pipelines/resolution/ops_crosswalk_sos_sam_runs.sql` (canonical copy) + mirror as the worker's
`OPS_DDL`. Same contract as `ops_crosswalk_hmda_gleif_runs.sql`, with crosswalk metrics:

```sql
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.crosswalk_sos_sam_runs (
    id                  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed                text NOT NULL,         -- 'crosswalk_sos_sam'
    dataset_uri         text NOT NULL,
    rows_written        bigint,                -- all candidate pairs
    canonical_rows      bigint,                -- == distinct matched SoS entities
    distinct_uei        bigint,                -- federal entities reached
    distinct_sos_entity bigint,                -- SoS entities matched
    tier1_rows          bigint,                -- exact-name + state-confirm
    status              text NOT NULL,
    error               text,
    started_at          timestamptz,
    completed_at        timestamptz,
    recorded_at         timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS crosswalk_sos_sam_runs_feed_idx        ON ops.crosswalk_sos_sam_runs (feed);
CREATE INDEX IF NOT EXISTS crosswalk_sos_sam_runs_status_idx      ON ops.crosswalk_sos_sam_runs (status);
CREATE INDEX IF NOT EXISTS crosswalk_sos_sam_runs_recorded_at_idx ON ops.crosswalk_sos_sam_runs (recorded_at DESC);
```

---

## 9. Execution checklist (do this in order)

```bash
git checkout -b claude/crosswalk-sos-sam origin/main
# 1. Author pipelines/resolution/crosswalk_sos_sam.py (§5/§6) + ops_crosswalk_sos_sam_runs.sql (§8).
#    Import core.name_norm — do NOT re-inline. Wire pre-write gates + v_before restore() guard.
# 2. PRE-FLIGHT (read-only): run the §5 SQL on a LIMIT 5000 SoS sample × full SAM to confirm the
#    join compiles, the 22 output columns land, and tier/canonical logic is sane. (Catch before Modal.)
modal run pipelines/resolution/crosswalk_sos_sam.py::init_ops
modal run pipelines/resolution/crosswalk_sos_sam.py --dry-run   # gates 1-7, no write
modal run pipelines/resolution/crosswalk_sos_sam.py             # build + index + verify
modal deploy pipelines/resolution/crosswalk_sos_sam.py
# 3. Independent read-back verification from R2 (counts, indices, tier distribution, a tier-1 round-trip).
# 4. Ship — commit, push, PR, MERGE, pull into the main worktree, verify (git log -1 --oneline).
```

A gate failure stops the ship — diagnose, do not force past a floor.

---

## 10. Consumer contract

```sql
-- "What federal UEI is this state-registered company?" — resolved, one row:
SELECT uei, sam_legal_business_name, match_tier, match_confidence, state_confirms
FROM   crosswalk_sos_sam
WHERE  sos_entity_key = 'CA:1945648' AND is_canonical;          -- BTREE seek

-- "Which state registrations back this federal entity?" — the reverse, full candidate set:
SELECT sos_source_state, sos_source_entity_name, match_tier
FROM   crosswalk_sos_sam WHERE uei = 'DD1BCRF2QQG8';            -- BTREE seek
```
Downstream, `uei` joins `crosswalk_sam_usaspending`, `contractor_award_summary`, `sam_pocs`,
`ffata_exec_comp`. Consumers threshold on `match_tier` (1 = highest precision) and filter `is_canonical`
for a single resolution. The crosswalk makes **no** silent single-row promise — it is an explicit,
scored candidate set.

---

## 11. Refresh / orchestration

- **Dependencies:** rebuild when **either** `sos_normalized_master` **or** `sam_normalized_entities`
  reships. No FEC dependency.
- **Now:** manual `modal run` after either upstream rebuild.
- **Phase 2:** chain a Trigger durable step on both upstreams' success callbacks (mirror
  `crosswalk_hmda_gleif.py`). Wire only after the compute is proven by `modal run`.

---

## 12. Out of scope

- Any change to `sos_normalized_master`, `sam_normalized_entities`, or the mirrors (read-only inputs).
- **Fuzzy matching.** Deterministic `core.name_norm` exact + base only — no trigram/Levenshtein (fleet
  policy). A future fuzzy tier, if ever authorized, is net-new.
- The **FEC employer bridge** — still deferred (rebuilt FEC).
- The `sos_normalized_master` ∪ `sam_normalized_entities` name-spine union — separate; note it must
  **reconcile the `source_state` semantic mismatch** (SoS = jurisdiction, SAM = physical; §0 trap 1)
  before a literal `UNION ALL`, e.g. by renaming SoS's `source_state` → `jurisdiction_state` and
  promoting its physical `state` → `source_state`.
- GLEIF→SAM, HMDA→SAM, and other name→UEI consumers — same pattern, their own plans.
```
