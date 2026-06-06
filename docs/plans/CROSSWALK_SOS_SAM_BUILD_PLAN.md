# `crosswalk_sos_sam` — Build Plan (SoS → SAM name → UEI)

Plan of record for the **first real consumer of `sam_normalized_entities`**: a name-keyed crosswalk
that attaches a federal **UEI** to every Secretary-of-State registrant that is also a SAM entity.
Immediately executable: an agent follows §9 top to bottom.

**Status: v9-canonical — supersedes the v4 figures (revised 2026-06-06).** Originally drafted +
adversarially reviewed 2026-06-05 against a **v4-stale** `sos_normalized_master`. The SoS spine has
since been re-materialized **v4 → v9** (key flip 2026-06-05 23:40;
[`SOS_NORMALIZED_MASTER_REMEDIATION_PLAN.md`](SOS_NORMALIZED_MASTER_REMEDIATION_PLAN.md),
[`SOS_INDEX_TOPOLOGY_PUSHDOWN_DIAGNOSTIC.md` §6](../reference/SOS_INDEX_TOPOLOGY_PUSHDOWN_DIAGNOSTIC.md#6-remediation-applied--2026-06-06)).
Two consequences this revision folds in: (1) every match figure was originally probed across a
**macro mismatch** (SAM's current-macro keys vs SoS's *old*-macro `normalized_legal_name`), silently
dropping every `&`/hyphen SoS entity — the v4 numbers were **undercounts**; (2) `legal_name_base` is
now a **materialized, BTREE-indexed column** on `sos_normalized_master`, so the base join is an index
lookup on **both** sides (the v4 "compute base on the fly on the SoS side" mechanics are obsolete).
**The build MUST read v9** ([cascade rule / Patch E](../reference/SOS_INDEX_TOPOLOGY_PUSHDOWN_DIAGNOSTIC.md):
"resolution crosswalks must build against v9"). All figures below are re-probed live at v9; the folded
adversarial-review findings (C1–C9) carry **v9** values, not the stale v4 ones. **Type:** net-new
derived Lance crosswalk + Modal worker.

**Reasoning:** the `name → UEI` sidecar ([`SAM_NORMALIZED_ENTITIES_BUILD_PLAN.md`](SAM_NORMALIZED_ENTITIES_BUILD_PLAN.md),
built + live) is a dormant surface until something joins it. SoS is the **unblocked** first consumer —
`sos_normalized_master` carries the byte-identical `core.name_norm` blocking key (now v9-current on
both `normalized_legal_name` and `legal_name_base`), it has **zero FEC dependency** (the FEC bridge
stays deferred), and it extends the entity graph: SoS state registration ↔ federal UEI ↔ awards /
POCs / exec-comp (the GTM control-surface path). This build also **exercises** the sidecar under a real
join, validating the multiplicity + geo handling the sidecar's adversarial review only theorized.

> **Every figure below is a live read-only v9 probe (2026-06-06).** Harnesses `/tmp/sos_sam_v9_canon.py`,
> `/tmp/sos_sam_v9_supp.py` (post-dedup-to-entity, materialized `legal_name_base` column on both
> sides; the materialized SoS column was verified byte-equal to `legal_name_base(normalized_legal_name)`
> on a 500k sample — 0 mismatches — then used directly). v4 lineage:
> `/tmp/sos_sam_review.py`, `/tmp/sos_sam_review2.py`. Two schema traps were caught by probing and are
> designed around, not assumed (§0). The v4→v9 figure deltas are tabulated in §13.

---

## 0. Live sizing + the two traps (probed at v9, not assumed)

**Inputs (live, v9):**
- `sos_normalized_master` — **v9 · 17,926,543 rows · 12 cols · 17,843,028 entities (post-dedup,
  latest-snapshot) · 16,508,638 distinct names**. BTREE `normalized_legal_name`, **`legal_name_base`**,
  `zip_code` + BITMAP `source_state`. **`legal_name_base` is now a materialized, BTREE-indexed
  column** (verified byte-equal to `legal_name_base(normalized_legal_name)` on a 500k sample, 0
  mismatches) — the base pass is an index lookup, not an on-the-fly recompute. Entity key =
  (`source_state`, `original_entity_id`).
- `sam_normalized_entities` — v7 · 1,541,566 rows · 1/uei. BTREE `normalized_legal_name`,
  `legal_name_base`, `uei`, `cage_code`, `primary_naics`; BITMAP `is_active`. Verbatim name =
  `legal_business_name`; recency vintage = `sam_extract_label`.

**Match sizing (Σ-of-products, post-dedup-to-entity, no pair materialization; both sides keyed on the
materialized columns):**

| Block key | shared keys | SoS entities covered | SAM UEIs reached | candidate pairs |
|---|---|---|---|---|
| `normalized_legal_name` (exact) | 341,949 | 502,486 (2.82% of SoS) | **381,176** | 600,681 |
| `legal_name_base` (suffix-peeled, the join superset) | 388,338 | 742,795 (4.16% of SoS) | **443,473** | 941,838 |

The 3–4% "of SoS" is the wrong denominator to fixate on — SoS is 17.8M mostly-tiny local LLCs. The
load-bearing number is **~28.8% of the entire SAM federal universe (443,473 of 1.54M UEIs) ties to a
state registration.** Output is bounded at **≤ ~942k pairs** — no cartesian blow-up. (These are
*higher* than the v4 figures because v4 was probed across the old-macro mismatch; see the `&`/dash
recovery below and §13.)

**`&`/dash recovery (the v9 correction).** The v4 SoS spine stored `normalized_legal_name` from an
*older* `core/name_norm` that dropped `&` and glued hyphens; v9 uses the current macro
(`&` → ` AND `, dash → space). At v4 every conjunction-named (`X & Y`) and hyphenated (`COCA-COLA`)
SoS entity blocked to a different key than SAM's current-macro key, so they were **silently invisible**
to the match. **33,376 SoS entities** carry a `&`/dash fingerprint and now exact-match a SAM UEI that
were undetectable at v4 — they are the bulk of the +UEI / +pair lift in the table above.

**Trap 1 — geo locus (proven, re-verified at v9).** `sos_normalized_master` has *two* geo signals:
`source_state` = the registration **jurisdiction**; `state`/`zip_code` = the entity's **physical**
address. SAM's `source_state` is the **physical** HQ. So the correct like-for-like confirm is
`sos.state ↔ sam.source_state`. On **237,212 clean 1:1 name matches** (one SoS row AND one SAM UEI per
name) at v9:

| Comparison | agreement | verdict |
|---|---|---|
| `sos.state` (physical) == `sam.source_state` (physical) | **73.91%** | ✅ correct locus |
| `sos.zip_code` == `sam.zip_code` | 57.27% | confirmatory |
| `sos.source_state` (**jurisdiction**) == `sam.source_state` | 64.36% | ❌ wrong locus, measurably worse |

> **This 73.91% is the CLEAN-1:1-EXACT subset only — NOT a gate-able full-join metric.** The build
> never materializes a 1:1-restricted geo rate; the full-join `state_confirms` rates are far lower
> (§7 Gate 5). Do **not** re-import this number into a gate threshold (the v4 plan's original Gate 5
> did exactly that and hard-failed every run — see §7 / the C1 fix).

**Trap 2 — geo scores, never gates.** Even the *correct* locus agrees only ~74% — ~26% of clean,
unambiguous name matches have a differing physical state (registered-agent addresses, multi-site
firms, relocations). A hard state-equality JOIN predicate would silently delete a quarter of true
matches (the sidecar review's B2 lesson, re-confirmed at v9). **Geo → a `match_tier` score, not a
filter.**

---

## 1. Objective

Materialize `crosswalk_sos_sam`: one row per matched **(SoS entity, SAM UEI)** candidate, scored by a
name+geo precision ladder, with a deterministic **canonical pick** per SoS entity that has a
*trustworthy* (tiers 1–4) match. Resolves "what federal UEI is this state-registered company?" at
index speed, and exposes the full candidate set for audit.

**Success =** the dataset exists in R2, BTREE-indexed; exactly one `is_canonical` row per SoS entity
with a tier-1–4 match (tier-5 base-no-geo is recall, never canonical — C2); coverage within ±10% of
the v9 probed reach (≈**443k** UEIs all-tier union, ≈**743k** SoS entities; is_canonical reach ≈**349k**
UEIs); a tier-1 round-trip passing.

---

## 2. Inputs & output

| | Value |
|---|---|
| **Left (read-only)** | `s3://data-sink/active/sos_normalized_master/` — **v9**, 17.93M rows |
| **Right (read-only)** | `s3://data-sink/active/sam_normalized_entities/` — **v7**, 1.54M rows |
| **Output** | `s3://data-sink/active/crosswalk_sos_sam/` (net-new) |
| **Grain** | 1 row per matched (SoS entity, UEI) candidate pair; `is_canonical` marks the best UEI per SoS entity over tiers 1–4 |
| **Est. rows** | **~942k** (base-superset join; ~527k `is_canonical` + the tier-5 recall tail + non-canonical candidates) |
| **Lance** | `data_storage_version="2.1"`, `max_rows_per_file=1048576` |

Reads only the two normalized layers. Never touches `sos_normalized_master`, `sam_normalized_entities`,
or any mirror. No FEC input.

**Not redundant with any existing crosswalk (C9 — ENDORSE).** `crosswalk_hmda_gleif` keys on **`lei`,
not `uei`** (`BTREE_INDEXES = ["lei", …]`; zero `uei` references) — a GLEIF-LEI spine, a different
identifier. `crosswalk_sam_usaspending` is USAspending-recipient-anchored with no SoS side. A repo grep
of `pipelines/resolution/` confirms **no existing dataset joins SoS to UEI** — `crosswalk_sos_sam` is
the only SoS-registration → federal-UEI path in the fleet.

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
| `sos_entity_status` | string | ACTIVE / GOOD STANDING / TERMINATED / FORFEITED / … |
| `status_is_active` | bool | `sos_entity_status IN ('ACTIVE','GOOD STANDING')` (C5 — canonical tiebreak + audit) |
| `uei` | string | **the resolved SAM key** — **BTREE** (forward lookup) |
| `sam_normalized_legal_name` | string | SAM blocking key (audit) |
| `sam_legal_business_name` | string | SAM verbatim name (audit) |
| `sam_source_state` | string | SAM **physical** state |
| `sam_zip_code` | string | SAM **physical** zip5 |
| `sam_is_active` | bool | SAM active flag |
| `sam_primary_naics` | string | sector |
| `sam_extract_label` | string | SAM extract vintage (C4 — recency tiebreak + audit of which vintage the canonical pick came from) |
| `match_key` | string | `'normalized_legal_name'` \| `'legal_name_base'` — **BITMAP** |
| `match_tier` | int8 | **1**=exact+state+zip · **2**=exact+state · **3**=exact · **4**=base+state · **5**=base (C3) — **BITMAP** |
| `match_confidence` | string | `high`(1) / `medium_high`(2) / `medium`(3) / `low`(4) / `unsafe`(5) (ergonomic label; tier-5 `unsafe` is recall-only) |
| `state_confirms` | bool | `sos_state == sam_source_state` (physical↔physical) |
| `zip_confirms` | bool | `sos_zip_code == sam_zip_code` |
| `is_canonical` | bool | best UEI per `sos_entity_key` **over tiers 1–4 only** — tier 5 (base-no-geo) is recall, never canonical (C2) — **BITMAP** |
| `source_dataset` | string | const `'crosswalk_sos_sam'` (provenance) |

Indexes: BTREE `uei`, `sos_entity_key`, `sos_normalized_legal_name`; BITMAP `match_tier`,
`is_canonical`, `match_key`. (24 columns.)

---

## 4. Match design (the heart)

**Key ladder — registry↔registry favors EXACT (the inverse of the FEC employer call).** Both sides are
clean registry legal names that carry the corporate form, so the exact `normalized_legal_name` is the
high-precision key; `legal_name_base` is the recall fallback for suffix drift (`INC` vs `INCORPORATED`,
`CO` vs `COMPANY`). This is deliberately the **opposite** of the sidecar review's B1 (which favored
`legal_name_base` for *free-text* FEC employer strings) — context decides, not cargo-cult.

Implementation: **one join on the materialized `legal_name_base` column on BOTH sides** —
`sos.legal_name_base` (BTREE, new at v9) ⨝ `sam.legal_name_base` (BTREE). This is the superset
(equal-normalized ⟹ equal-base, so it captures every exact match plus the suffix-drift recall); then
label `match_key='normalized_legal_name'` when the normalized names are also equal. **No on-the-fly
base recompute on either side** — the v4 "compute `legal_name_base` on the SoS side in the SELECT"
mechanic is obsolete now that the column is materialized and indexed. The superset claim is **verified
at v9**: of 502,486 exact-match SoS entities, **0** are missing from the base-superset output at entity
grain — every exact match is retained (an exact match could only be dropped if its name peeled entirely
to a NULL base, which does not occur for real SoS registrants).

**Geo — a score on the correct locus, promoted INTO the tier (§0 traps + C3).** `state_confirms =
sos_state == sam_source_state` (physical↔physical, the ~74% clean-1:1 signal), `zip_confirms =
sos_zip_code == sam_zip_code`. Both feed `match_tier`; **neither gates membership.** `sos.source_state`
(jurisdiction) is NOT used for the confirm. **Zip is the strongest disambiguator and now defines
tier 1**: adding zip to exact+state cuts the multi-UEI collision rate from **1.73% → 0.79%** (a 2.2×
precision lift, v9-measured) — so the tier a consumer thresholds on is `exact + state + zip`, not
`exact + state`. The 5-tier ladder:

| tier | predicate | `match_confidence` | canonical? |
|---|---|---|---|
| 1 | exact name + state + zip | `high` | yes |
| 2 | exact name + state (zip differs/null) | `medium_high` | yes |
| 3 | exact name (no state) | `medium` | yes |
| 4 | base + state | `low` | yes |
| 5 | base (no geo) | `unsafe` | **no — recall only (C2)** |

**Why tier 5 is never canonical (C2).** Base-ONLY pairs (matched on peeled base, not exact name)
confirm physical state at **9.31%** (v9, geo-evaluable) versus exact-name pairs at **47.91%** — they
are predominantly *different companies sharing an over-peeled base* (the sidecar review's B8 over-peel:
`CO` truncation, `INC`/`INCORPORATED` asymmetry). At v9 tier 5 is **33.2% of output pairs** and would
otherwise supply the canonical UEI for **216,119 SoS entities (29.1%)** — nearly a third of the
crosswalk's "answers" resting on the weakest, geo-unconfirmed signal. It stays emitted for recall/audit
but is excluded from `is_canonical`.

**Multiplicity — candidate set + canonical pick (no silent fan-out, the B3 lesson).** Names are
non-unique on **both** sides. Emit every matched pair; mark exactly one `is_canonical` per
`sos_entity_key` *that has a tier-1–4 match* by the deterministic ranking
`match_tier ASC, state_confirms DESC, zip_confirms DESC, status_is_active DESC, sam_is_active DESC,
sam_extract_label DESC, uei ASC`. The `status_is_active` term (C5) prefers a live SoS registration over
a TERMINATED/FORFEITED one when an entity has both; `sam_extract_label DESC` (C4 recency) breaks
branch/franchise ties to the most-recently-seen UEI before falling through to alphabetical `uei ASC`.
At v9 the canonical pick still falls to `uei ASC` alone for **4.00%** of entities (multi-location names
like `THE SHERWIN WILLIAMS COMPANY` → 123 UEIs in OH); the recency tiebreak resolves **11,016** of
those to a non-arbitrary pick. A consumer that wants a single UEI filters `is_canonical`; one that
wants the full set (all branches) reads all rows.

---

## 5. The transform (exact SQL)

The join is on the **materialized `legal_name_base` columns on both sides** — `core.name_norm` is NOT
re-inlined to recompute a base in the SELECT (the v4 mechanic; obsolete at v9). The builder needs no
`core.name_norm` import at all for the base join; the SoS column is already the canonical v9 value and
was verified byte-equal to the macro (§0).

```python
def build_crosswalk_sql() -> str:
    """Join sos (1 row/entity, materialized v9 legal_name_base) to sam on the materialized
    legal_name_base column both sides; label, score (5-tier), rank (recency+status), mark canonical
    over tiers 1-4. Reads `sos_src` and `sam_src` relations (the scanned layers)."""
    return """
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
            (entity_status IN ('ACTIVE','GOOD STANDING'))    AS status_is_active,
            legal_name_base                                  AS sos_legal_name_base  -- materialized v9 column
        FROM sos_src
        WHERE normalized_legal_name IS NOT NULL
          AND nullif(trim(source_state), '') IS NOT NULL          -- key components must exist —
          AND nullif(trim(original_entity_id), '') IS NOT NULL    -- a null sos_entity_key is unusable (Gate 3)
        QUALIFY row_number() OVER (
            PARTITION BY source_state, original_entity_id
            ORDER BY snapshot_date DESC NULLS LAST) = 1          -- 1 row/entity (latest snapshot)
    ),
    pairs AS (
        SELECT
            s.sos_entity_key, s.sos_source_state, s.sos_original_entity_id,
            s.sos_normalized_legal_name, s.sos_source_entity_name, s.sos_state,
            s.sos_zip_code, s.sos_entity_status, s.status_is_active,
            m.uei, m.normalized_legal_name AS sam_normalized_legal_name,
            m.legal_business_name AS sam_legal_business_name,
            m.source_state AS sam_source_state, m.zip_code AS sam_zip_code,
            m.is_active AS sam_is_active, m.primary_naics AS sam_primary_naics,
            m.sam_extract_label,
            (m.normalized_legal_name = s.sos_normalized_legal_name) AS exact_name,
            (upper(trim(s.sos_state)) = upper(trim(m.source_state))) AS state_confirms,
            (s.sos_zip_code IS NOT NULL
             AND s.sos_zip_code = left(nullif(trim(m.zip_code), ''), 5)) AS zip_confirms
        FROM sos s
        JOIN sam_src m ON m.legal_name_base = s.sos_legal_name_base   -- base superset, both BTREE columns
    ),
    scored AS (
        SELECT *,
            CASE WHEN exact_name THEN 'normalized_legal_name' ELSE 'legal_name_base' END AS match_key,
            CASE WHEN exact_name AND state_confirms AND zip_confirms THEN 1
                 WHEN exact_name AND state_confirms                  THEN 2
                 WHEN exact_name                                     THEN 3
                 WHEN state_confirms                                 THEN 4
                 ELSE 5 END                                       AS match_tier
        FROM pairs
    ),
    ranked AS (
        SELECT *,
            row_number() OVER (
                PARTITION BY sos_entity_key
                ORDER BY match_tier ASC, state_confirms DESC, zip_confirms DESC,
                         status_is_active DESC, sam_is_active DESC,
                         sam_extract_label DESC, uei ASC)             AS rn
        FROM scored
    )
    SELECT
        * EXCLUDE (exact_name, rn),
        CASE match_tier WHEN 1 THEN 'high' WHEN 2 THEN 'medium_high'
                        WHEN 3 THEN 'medium' WHEN 4 THEN 'low'
                        ELSE 'unsafe' END                          AS match_confidence,
        (rn = 1 AND match_tier <= 4)                               AS is_canonical,  -- tier 5 never canonical (C2)
        'crosswalk_sos_sam'                                        AS source_dataset
    FROM ranked
    """
```

`sos_src` is scanned with columns `source_state, original_entity_id, normalized_legal_name,
legal_name_base, source_entity_name, state, zip_code, entity_status, snapshot_date` (note
`legal_name_base` is now a scanned **column**, not recomputed); `sam_src` with `uei,
normalized_legal_name, legal_name_base, legal_business_name, source_state, zip_code, is_active,
primary_naics, sam_extract_label`. DuckDB `memory_limit='24GB'`, `threads=8`, spill `/tmp/duckdb_spill`;
`LANCE_BYPASS_SPILLING=true` for the index sort.

---

## 6. Worker file (structure — clone the skeleton)

Create **`pipelines/resolution/crosswalk_sos_sam.py`** (Modal app `resolution-sos-sam-pipelines`).
Closest skeleton: `pipelines/resolution/crosswalk_hmda_gleif.py` (name-based crosswalk: two-source
DuckDB join, ops ledger with match-rate, dry-run, Trigger callback) + `crosswalk_sam_usaspending.py`
for the `version=v_before).restore()` rollback (lines 520-598).

- **Image:** `debian_slim(3.12).pip_install("duckdb>=1.5,<2","lancedb>=0.15","pylance>=7","pyarrow>=17",
  "requests>=2.32","psycopg[binary]>=3.2").env({"LANCE_BYPASS_SPILLING":"true"})
  .add_local_python_source("core.name_norm")`. **Note:** the base join reads the *materialized* v9
  `legal_name_base` columns on both sides, so the transform SQL needs **no** `core.name_norm` call.
  Keep `add_local_python_source("core.name_norm")` for skeleton parity and any future macro-side
  diagnostic, but the worker is no longer functionally coupled to the macro — the SoS column is the
  canonical v9 value (verified byte-equal, §0). Do NOT re-inline a base recompute.
- **Constants:** `SOS_URI` (v9), `SAM_URI` (v7), `DATASET_URI=…/crosswalk_sos_sam/`,
  `FEED="crosswalk_sos_sam"`, `BTREE_INDEXES=["uei","sos_entity_key","sos_normalized_legal_name"]`,
  `BITMAP_INDEXES=["match_tier","is_canonical","match_key"]`, `ROW_FLOOR=500_000`.
  Modal `memory=49152, cpu=8.0` (the 17.9M-row SoS scan is the heavy input). The 5-tier ladder, the
  amended canonical `ORDER BY`, and the gate baselines below are v9 figures.
- **Functions** (mirror `crosswalk_hmda_gleif.py`): `build_crosswalk` (gates → `v_before` → write →
  index → post-write gates → restore-on-failure → ops + callback), `verify_crosswalk` (read-back +
  tier/coverage distribution), `plan_crosswalk` (dry-run: materialize + gates, no write), `init_ops`,
  `@app.local_entrypoint() build(dry_run=False)`.
- `_r2_storage_options` / `_new_con` / `_pg_connect` / `_record_run` / `_post_callback` — copy verbatim.

---

## 7. Validation gates (no ship without all green)

All baselines below are **v9 live** (2026-06-06; §0, §13). Tolerances are deliberately loose enough to
survive a normal upstream refresh, tight enough to catch a structural break.

**Pre-write (on the Arrow table, hard-fail before overwrite):**
1. **Row floor** — `rows ≥ 500,000` (v9 ~942k; comfortable margin).
2. **Canonical uniqueness** — `count(*) FILTER (is_canonical) == count(DISTINCT sos_entity_key FILTER
   (match_tier <= 4))`. Exactly one `is_canonical` row per SoS entity *that has a tier-1–4 match*.
   (Tier-5-only entities — ~216k at v9 — legitimately have **no** canonical row; do NOT assert
   `is_canonical` count == all distinct entities.)
3. **No orphans** — `count(*) FILTER (uei IS NULL OR sos_entity_key IS NULL) == 0` (inner-join invariant).
4. **Coverage** — `distinct uei` within ±10% of **443,473** (the all-tier union — the *recall ceiling*,
   not the trustworthy reach); `distinct sos_entity_key` within ±10% of **742,795** (v9 post-dedup
   baselines). The trustworthy (tier-1–4 `is_canonical`) UEI reach is **349,365**; surfaced as
   observability, not gated.
5. **Locus-regression guard (REPLACES the v4 73% gate — C1).** Compute on the Arrow table
   `count(*) FILTER (state_confirms AND exact_name) / count(*) FILTER (exact_name)` and require it in
   **`[0.41, 0.51]`** (v9 baseline **46.03%**, ±5pp). The v4 plan asserted ≈73% over a clean-1:1-exact
   subset the build never computes — it hard-failed every run; the real full-join exact-name rate is
   ~46%. This band still catches a locus break: wiring `sos.source_state` (jurisdiction) instead of
   `sos.state` (physical) drops the rate measurably (the §0 clean-1:1 jurisdiction agreement is 64.36%
   vs 73.91% physical; the full-join gap is wider). **Belt-and-suspenders:** also floor `tier1_rows ≥
   120,000` (v9 tier-1 = exact+state+zip = **207,283** pairs; a wrong locus collapses it).
5b. **Tier-5 discipline (C2).** `count(*) FILTER (is_canonical AND match_tier = 5) == 0` (tier 5 must
    never win canonical) AND `tier-5 pair share ≤ 40%` of total (recall sanity; v9 = **33.2%**).
6. **Tier monotonicity** — every row has `match_tier ∈ {1,2,3,4,5}`, `match_key` consistent
   (`'normalized_legal_name'` ⟺ tier ≤ 3, `'legal_name_base'` ⟺ tier ∈ {4,5}), and `match_confidence`
   the correct label for its tier.
7. **Δ-guard** — `±25%` row delta vs the prior `ops.crosswalk_sos_sam_runs` success (skip if first).

**Post-write (then `restore(v_before)` on failure):**
8. **Indices present** — all §3 BTREE + BITMAP in the manifest.
9. **Tier-1 round-trip** — pick any `match_tier=1, is_canonical` row; assert `uei` resolves in
   `sam_normalized_entities` and `sos_entity_key` resolves in `sos_normalized_master`.
10. **Point-lookup** — a `WHERE uei = '<known>'` seek returns its SoS candidates (BTREE seek; <2s R2
    ceiling, warm target <100ms — same remote-RTT caveat as the sidecar's gate 10).

**Observability (non-failing, `verify_*`):** rows by `match_tier` (the 5-tier split); canonical rows by
`match_confidence`; `state_confirms`/`zip_confirms` rates; **tier-1 (exact+state+zip) distinct UEIs ≈
183,312 / entities ≈ 205,322** (the *trustworthy* reach, alongside the 443,473 all-tier recall
ceiling); `is_canonical` picks resting on `status_is_active=FALSE` (v9 **35.5%** — non-trivial, the C5
rationale); count of canonical picks decided by `uei ASC` alone (v9 **4.00%**, trending lower after the
recency tiebreak resolves 11,016); top fan-out names (max SoS-entities per UEI and vice-versa, e.g.
`THE SHERWIN WILLIAMS COMPANY` → 123 UEIs).

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
    rows_written        bigint,                -- all candidate pairs (v9 ~942k)
    canonical_rows      bigint,                -- is_canonical rows == distinct SoS entities with a tier-1..4 match (v9 ~527k)
    distinct_uei        bigint,                -- federal entities reached, all-tier union (v9 ~443k)
    distinct_sos_entity bigint,                -- SoS entities matched, all-tier (v9 ~743k)
    tier1_rows          bigint,                -- exact + state + zip (the new high-precision tier-1; v9 ~207k)
    tier5_rows          bigint,                -- base + no-geo (the unsafe recall tier, never canonical; v9 ~313k)
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
#    Join the MATERIALIZED v9 legal_name_base columns both sides — do NOT recompute the base via
#    core.name_norm. Wire the pre-write gates (incl. the C1-fixed Gate 5 + the C2 Gate 5b) + v_before
#    restore() guard. Read SOS_URI at v9, SAM_URI at v7.
# 2. PRE-FLIGHT (read-only): run the §5 SQL on a LIMIT 5000 SoS sample × full SAM to confirm the
#    join compiles, the 24 output columns land, and the 5-tier/canonical logic is sane. (Catch before Modal.)
modal run pipelines/resolution/crosswalk_sos_sam.py::init_ops
modal run pipelines/resolution/crosswalk_sos_sam.py --dry-run   # gates 1-7 + 5b, no write
modal run pipelines/resolution/crosswalk_sos_sam.py             # build + index + verify
modal deploy pipelines/resolution/crosswalk_sos_sam.py
# 3. Independent read-back verification from R2 (counts, indices, 5-tier distribution, a tier-1 round-trip).
# 4. Ship — commit, push, PR, MERGE, pull into the main worktree, verify (git log -1 --oneline).
```

A gate failure stops the ship — diagnose, do not force past a floor. (The v4 plan's Gate 5 was itself
the failure mode: it asserted a 73% rate the build never computes. That is fixed here — Gate 5 now
floors the real ~46% exact-name `state_confirms` rate; see §7 / C1.)

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
`ffata_exec_comp`. Consumers threshold on `match_tier` (1 = exact+state+zip, highest precision; ≤2 for
exact+state; ≤4 for any geo-confirmed or exact link; **tier 5 = base-no-geo is recall-only, ~9% physical
state agreement — do not treat as a resolved link**) and filter `is_canonical` for a single resolution.
The crosswalk makes **no** silent single-row promise — it is an explicit, scored candidate set.

**Tier-1 `high` is name+geo precision, NOT a uniqueness guarantee (C7).** ~1.7% of exact+state SoS
entities (and 0.79% even after the tier-1 zip confirm) legitimately fan out to >1 UEI — multi-location
franchises and defense primes with many registered subsidiaries under one legal name in one state
(e.g. `THE SHERWIN WILLIAMS COMPANY` → 123 UEIs, `KNICKERBOCKER DIALYSIS INC` → 75, `LEIDOS INC` → 41).
For these, the `is_canonical` row is *a representative UEI for this legal name in this geo*, not "the one
true UEI." A consumer that needs all branches reads the full candidate set; `is_canonical` is the
single-pick convenience. (Same honesty the sidecar review's B3 demanded of the FEC contract.)

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
- **NAICS as a precision/tiebreak signal** — *reasoned, not measured.* The output carries
  `sam_primary_naics`, but `sos_normalized_master` has no sector field to compare against, so a NAICS
  confirm needs a third source. Whether NAICS agreement would further cut the 0.79% tier-1 collision
  rate is **not probed** — a plausible Phase-2 lever, not a blocker.
- GLEIF→SAM, HMDA→SAM, and other name→UEI consumers — same pattern, their own plans.
  `crosswalk_hmda_gleif` is **not** one of them: it keys on **LEI, not UEI** (§2, C9), a different
  spine — no overlap with this build.

---

## 13. v9 re-baseline (this revision)

This plan was authored + adversarially reviewed against a **v4-stale** `sos_normalized_master`, then
the SoS spine was re-materialized **v4 → v9** (current `core.name_norm`: `&` → ` AND `, dash → space;
plus the now-materialized BTREE `legal_name_base` column). Every gate/tier/coverage figure was re-probed
live at v9 (2026-06-06; harnesses `/tmp/sos_sam_v9_canon.py`, `/tmp/sos_sam_v9_supp.py`). What moved:

**Two structural changes from v4.**
1. **Macro-mismatch corrected.** v4 numbers were probed across SAM's current-macro keys vs SoS's
   *old*-macro `normalized_legal_name`, silently dropping every `&`/hyphen SoS entity → v4 was an
   **undercount**. **33,376 SoS entities** with a `&`/dash fingerprint now exact-match a SAM UEI that
   were invisible at v4 — the bulk of the +UEI / +pair lift.
2. **`legal_name_base` materialized on the SoS side.** v4 had no column (computed on the fly in the
   SELECT). v9 carries a BTREE-indexed `legal_name_base` column (verified byte-equal to the macro on a
   500k sample, 0 mismatches). The base join is now an index lookup on **both** sides; the §4/§5
   "compute base on the SoS side" mechanic is removed and the worker no longer calls the macro.

**Figure deltas (v4 → v9).**

| Figure | v4 (stale) | v9 (live) | Used in |
|---|---|---|---|
| Exact-name candidate pairs (post-dedup) | 500,233 | **600,681** | §0/§2 |
| Base-superset candidate pairs (≈ output rows) | 771,932 | **941,838** | §0/§2/§7·G1 |
| Distinct UEIs reached (all-tier union) | 400,325 | **443,473** | §7·G4 |
| Distinct SoS entities matched (all-tier) | 620,317 | **742,795** | §7·G4 |
| `is_canonical` rows (tiers 1–4) | — | **526,676** | §7·G2, §8 |
| Distinct UEIs reached by `is_canonical` (trustworthy) | — | **349,365** | §7·G4 obs |
| **Gate-5 exact-name `state_confirms` rate (C1)** | 48.38% | **46.03%** → band **[0.41, 0.51]** | §7·G5 |
| §0 geo locus — physical / jurisdiction (clean 1:1) | 73.03 / 64.06 | **73.91 / 64.36** | §0 traps |
| Base-only state-confirm rate, geo-evaluable (C2) | 9.86% | **9.31%** | §4, §3 |
| exact+state collision rate (C3) | 1.72% | **1.73%** | §4 |
| exact+state+zip collision rate — new tier-1 (C3) | 0.79% | **0.79%** | §4, §7·G5 |
| New tier-1 (exact+state+zip): pairs / entities / UEIs | — | **207,283 / 205,322 / 183,312** | §7 obs, §8 |
| Tier-5 (base-no-geo) share of output pairs (C2) | ~32% | **33.2%** | §7·G5b, §8 |
| Tier-5-only SoS entities (no canonical row) | — | **216,119** | §1, §7·G2 |
| Canonical decided by `uei ASC` alone (C4) | 20,479 / 3.30% | **29,698 / 4.00%** | §4, §7 obs |
| …recency-breakable (≥2 `sam_extract_label`) (C4) | 7,554 | **11,016** | §4 |
| `is_canonical` on non-active SoS registration (C5) | ~30k tier-1 | **187,187 / 35.5%** | §7 obs |

**Adversarial-review C-findings — all folded at v9.** C1 (Blocker): the 73% Gate 5 is **replaced** by
the real exact-name `state_confirms` band [0.41, 0.51] (v9 46.03%) + a `tier1_rows ≥ 120,000` floor —
the gate no longer fails closed. C2 (Major): the base-no-geo tier is now **tier 5**, `match_confidence
= 'unsafe'`, **excluded from `is_canonical`** (gate: tier-5 canonical == 0). C3 (Major): **zip promoted
into the tier ladder** — tier 1 = exact+state+zip (the 0.79% collision tier). C4 (Major):
`sam_extract_label DESC` (recency) inserted before `uei ASC` in the canonical `ORDER BY`; the label is
carried into the schema. C5 (Minor): `status_is_active` derived + inserted into the canonical ranking
after geo. C6/C8 (Minor/Nit): coverage baselines updated to v9 post-dedup; tier-1 distinct-UEI surfaced
as observability. C7 (Minor): tier-1 `high` documented as name+geo precision, not uniqueness (§10).
C9 (Nit): non-redundancy (LEI ≠ UEI) stated in §2.

**What the v4 review / re-baseline missed, found at v9.**
- **The base tier *splits* under the new 5-tier ladder.** The review's C2 "tier-4 = base, no geo = 32%
  of output" is, in the amended ladder, precisely **tier 5** (33.2%). The new **tier 4 (base + state)**
  is a *small, geo-confirmed* slice — **28,504 pairs (3.0%)** — and IS canonical-eligible. The C2 demotion
  correctly targets only the geo-unconfirmed base tier; conflating "base" with "noise" wholesale would
  have wrongly discarded the 3% base+state recall that does confirm geo.
- **C4 arbitrariness grew, not shrank, at v9** (3.30% → **4.00%**; 20,479 → 29,698) — the `&`/dash
  recovery pulled in more multi-branch franchise names (the exact entities that collide). The recency
  tiebreak is therefore *more* load-bearing at v9, not less; it resolves 11,016 of the 29,698.
- **C5 non-active share is far larger than the review's tier-1-only count implies.** Across all
  `is_canonical` picks it is **35.5% (187,187)**, not the ~30k the review cited for tier-1 alone — the
  `status_is_active` tiebreak matters across the whole canonical set, confirming the C5 fix belongs in
  the global `ORDER BY`, not a tier-1 patch.
- **Canonical UEI reach (349,365) is materially below the all-tier union (443,473).** ~94k UEIs are
  reached *only* through tier-5 (base-no-geo) candidates and so are never the canonical answer for any
  entity — the recall ceiling overstates the trustworthy federal reach by ~21%. Gate 4 floors the union
  (recall) but the plan now surfaces the canonical reach separately so the two are never conflated (the
  C6 spirit, extended).
