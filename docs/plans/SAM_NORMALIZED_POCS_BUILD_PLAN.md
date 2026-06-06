# `sam_normalized_pocs` — Person-Layer Sidecar Build Plan (v2, remediated)

Plan of record for the **derived person-name blocking-key sidecar** built off the verbatim human layer
`sam_pocs`. Immediately executable: a fresh agent follows §12 top to bottom. The person analog of
[`sam_normalized_entities`](../../pipelines/sam_gov/sam_normalized_entities.py) — it ships the same
control-plane shape (gate + rollback + ops-ledger + dispatcher); the **data-plane logic is
person-specific and is NOT cloned** (§2).

**Status:** ready to execute. **Type:** net-new derived Lance dataset + new shared primitive
(`core.person_name_norm`) + Modal worker + Trigger task.
**v2 changelog:** every BLOCKER/CRITICAL/HIGH from the adversarial review
([`SAM_NORMALIZED_POCS_BUILD_PLAN_REVIEW.md`](SAM_NORMALIZED_POCS_BUILD_PLAN_REVIEW.md)) is remediated
here — the §5 generational mechanism is rebuilt and **empirically re-verified in DuckDB** against the
review's adversarial fixture (26 rows, 23 claim-checks all PASS; §5/§11), §8 floors are re-set below the
Δ-band, the memory envelope matches the 8M-row `sam_pocs` precedent, and `skip_if_current` gains a
content fingerprint. **Live cardinality:**
[`FEC_SAM_PERSONNEL_BRIDGE_DIAGNOSTIC.md`](../reference/FEC_SAM_PERSONNEL_BRIDGE_DIAGNOSTIC.md)
(probe 2026-06-05) — but note (§3/§8) the plan's `person_key` is NOT the diagnostic's join key, so its
distinct counts are **measured in the dry-run**, not lifted from the diagnostic.

> **What this builds:** the SAM-side **person resolution + audience spine** only. It reads `sam_pocs`,
> never FEC. Consumer-agnostic right-side surface for (a) **GTM/audience** (contact POCs at SAM/USAspending
> entities — §13) and (b) a **walled-off FEC personnel bridge** (intelligence only, never a contact
> source — §13). Neither consumer is built here.

---

## 1. Objective

Materialize a thin, super-indexed sidecar that answers one question at index speed: **"which normalized
person + geo is this POC, and which entity (UEI/CAGE) do they sit under?"** `person_key` is computed
**once, here**, and BTREE-indexed — so a downstream consumer pays a point-lookup, not a repeated
8.07M-row full-scan recompute of the `name_norm` regex (which is exactly what the personnel diagnostic
had to hand-inline because no stored key exists today).

**Success =** the dataset exists in R2, BTREE-indexed on `person_key`, validated 1:1 against live
`sam_pocs`, with the person key built from the **shared** `core.person_name_norm` primitive (never
re-inlined), `uei`/`cage_code` provenance load-bearing for the entity traversal, the §11 fixture suite
green, and a round-trip point-lookup passing.

---

## 2. Why a sidecar — and why person ≠ company (the part that is NOT cloned)

**Why a sidecar, not columns on `sam_pocs`** (identical reasoning to the entity sidecar):
- **Blast radius.** The person-key policy is *volatile* (honorific/credential sets, generational
  handling, particle handling evolve). Each tweak rebuilds **only this narrow projection** — never the
  8.07M-row verbatim `sam_pocs` build with its 17-gate `pipe_fields` unpivot.
- **Contract purity.** `sam_pocs` is the **ZERO-ALTERATION verbatim SoR** (`sam_pocs.py:36-42`). Derived/
  mutable keys are quarantined here; the verbatim parts live once in `sam_pocs`, not duplicated as SoR.
- **Indexability.** Lance BTREEs a **stored column**, not an expression. The canonical key must be
  materialized to be a point-seek instead of a per-query scan.

**Why the logic is person-specific (the cloned-from-entities trap, corrected in v2):** the entity gate
suite encodes *company-name* physics. Three invert for people — implemented here, not copied:

| Entity assumption | Person reality | This plan (v2) |
|---|---|---|
| `legal_name_base` **peels & discards** the trailing token (LLC/CO) for recall | A trailing **generational** token (JR/SR/II–V/2ND–4TH) is a father/son **discriminator** | The gen token is peeled out of `person_key` **from whichever field carries it** (recall: FEC's suffix-less `SMITH JOHN` blocks SAM's `SMITH JOHN JR` whether `JR` rode `last_name` or `first_name`) and **PRESERVED in `generational_suffix`** (precision). Honorifics/credentials are noise → stripped. Verified §5/§11. |
| Gate 3 `distinct_uei == rows` (key ~unique) | `distinct person_key / rows` is low by design; the same person recurs | **Gate 3 deleted.** The sensitive check is the addressable **`(person_key, state2, zip5)` triple** count, **measured in the dry-run** (§8 — the diagnostic's figures are a *different* key, §3). |
| `name_norm(name)` + geo ≈ unique resolver | `JOHN SMITH` + metro zip5 = several humans | The sidecar **blocks, does not resolve.** Disambiguation (employer-agreement via the entity sidecar on `uei`, amount, temporal) lives in the consumer (§13). |

---

## 3. Inputs & output

| | Value |
|---|---|
| **Source (read-only)** | `s3://data-sink/active/sam_pocs/` — **8,065,116 rows** · 1/(entity, populated POC slot) · BTREE(uei, cage_code, name_key, last_name) |
| **Output** | `s3://data-sink/active/sam_normalized_pocs/` (net-new) |
| **Grain** | **1 row per `sam_pocs` row** (pure 1:1 passthrough — lossless; preserves the `person → uei → employer` fan-out). NOT 1/person; a downstream `DISTINCT ON` collapses to person/person-in-role for audiences (§13). |
| **Est. rows** | ~8,065,116 |
| **Lance version** | `data_storage_version="2.1"`, `max_rows_per_file=1048576`, `max_bytes_per_file=90*1024**3` |

The worker reads **only** `sam_pocs`. No filtering — foreign POCs are retained with nullable geo
(lossless); the `country='USA'` filter is a *consumer* concern at query time.

**Key-vs-diagnostic caveat (review P5).** The diagnostic's headline figures are computed from keys that
are **not** this plan's `person_key`: `2,119,414` = `name_norm(LAST FIRST)` (no peel, diagnostic line
190); `2,868,249` = `name_norm(last,first,**middle**)` triples (line 91). This plan's `person_key`
**drops middle AND peels generational/honorific tokens**, so it merges `SMITH JR`+`SMITH` and
`DR JANE`+`JANE` — its distinct count is **below** 2.12M, and its no-middle triple count is **below**
2.868M. Therefore §8's distinct floors are **measured in the dry-run** (§12 step 4), never lifted from
the diagnostic. The diagnostic remains valid for: row count (8,065,116), per-row geo fill (state 98.72%,
zip5 99.06%, co-present 98.08%), `uei` 54.22% / `cage_code` 95.51%, and the −0.15% human-name collapse.

---

## 4. Output schema (exact)

Scan these `sam_pocs` columns (projection pushdown): `uei`, `cage_code`, `poc_type`, `source_family`,
`first_name`, `middle_name`, `last_name`, `full_name`, `state`, `zip5`, `country`, `sam_extract_label`.
(All verified present in the `sam_pocs` output — `sam_pocs.py:198-288`.)

| Column | Type | Derivation (all from `core.person_name_norm`, §5) | Index |
|---|---|---|---|
| `person_key` | string | `person_key(last_name, first_name)` — `name_norm(LAST_core FIRST_core)`, middle dropped, gen/honorific/credential peeled from **either** field, NULL when surname empties | **BTREE** (primary blocking key) |
| `surname_initial_key` | string | `surname_initial_key(last_name, first_name)` — `name_norm(LAST_core + first-initial)` | **BTREE** (recall fallback) |
| `last_name_norm` | string | `last_name_norm(last_name)` = `name_norm(_last_core(last_name))` — **same surname token as inside `person_key`** | **BTREE** (surname-only blocking) |
| `first_initial` | string | `first_initial(first_name)` = `left(_first_core(first_name), 1)` — **honorific-stripped, agrees with `surname_initial_key`** (review P6) | — (confirmatory) |
| `middle_norm` | string | `name_norm(middle_name)` (24% fill) | — (confirmatory) |
| `generational_suffix` | string | `generational_suffix(last_name, first_name)` — full trailing gen run from whichever field, dot-stripped; **preserved, never in `person_key`** | — (precision tiebreak) |
| `uei` | string | passthrough `nullif(trim(uei),'')` | **BTREE** (entity traversal → employer) |
| `cage_code` | string | passthrough | **BTREE** (legacy-tail traversal) |
| `poc_type` | string | passthrough | **BITMAP** |
| `source_family` | string | passthrough (`v2`/`legacy_v1`) | **BITMAP** |
| `country` | string | `nullif(trim(country),'')` | **BITMAP** (US-only consumer filter) |
| `state2` | string | `upper(nullif(trim(state),''))` | — (geo, inline) |
| `zip5` | string | `left(nullif(trim(zip5),''),5)` | — (geo, inline) |
| `first_name` `middle_name` `last_name` `full_name` | string | verbatim passthrough (SoR copy) | — |
| `sam_extract_label` | string | passthrough (provenance + snap-key) | — |
| `source_dataset` | string | const `'sam_pocs'` | — |

`state2`/`zip5` are denormalized on-row on purpose (block on name + tiebreak on geo in one query). Column
names follow the `state2`/`zip5` convention; a **future union** with `sam_master_contacts`/`ffata_exec_comp`
is **not** literal — those carry `middle_initial`/`state_or_province` and (File-E) an unsplit `officer_name`
with no geo, so a union needs an explicit alias projection (review P13).

**Index build order / spilling (review P7).** Build the high-cardinality string BTREEs first
(`person_key`, `surname_initial_key` — most likely to OOM, fail fast), then `last_name_norm`, `uei`,
`cage_code`, then the BITMAPs. `LANCE_BYPASS_SPILLING=true` is image-global and forces every index sort
in-memory; it is genuinely needed only for `person_key`/`surname_initial_key`. **Minimum viable set** (if
trimming): `person_key`, `uei`, `cage_code`.

**Two measured properties a consumer must plan around:** `person_key` is **many-to-many** (homonyms +
multi-entity POCs) — no single-row-join promise; generational suffix is **carried, not keyed** (require it
as a precision tiebreak, never in the primary block).

---

## 5. New shared primitive — `core/person_name_norm.py` (exact, verified)

Pure DuckDB-SQL string builders that **compose `core.name_norm`** (never re-inline the regex). The person
analog of `core.name_norm`. **These builders are reproduced verbatim from the v2 verification harness; all
23 §11 claim-checks pass in DuckDB 1.5 over the 26-row adversarial fixture** — author them exactly as
below, then re-run §11 before the first build.

```python
"""Canonical person-name blocking-key SQL builders — the person analog of core.name_norm.

Operates on ALREADY-SPLIT parts (first/middle/last). Does NOT parse opaque strings — SAM delivers
discrete parts, so no name-splitting library is used (ZERO-ALTERATION policy; investigation confirmed
nameparser/probablepeople would only inject role-misassignment error here).

Person ≠ company on the tail: core.name_norm.legal_name_base PEELS and DISCARDS the trailing token for
recall; here a trailing GENERATIONAL token (JR/SR/II–V/2ND–4TH) is peeled out of the primary key — from
WHICHEVER field (last or first) carried it — for recall, but PRESERVED in generational_suffix for
precision. Honorifics (lead) and credentials (trail) are noise → stripped. Middle is dropped from the key
(24% fill, inconsistent) and carried as a confirmatory tiebreak.
"""
from __future__ import annotations

from core.name_norm import name_norm

# III before II so the longest terminal token wins at the $ anchor; ordinals included (review P10).
_GEN = "JR|SR|III|II|IV|V|2ND|3RD|4TH"
_HONORIFIC = "DR|MR|MRS|MS|MISS|PROF|SIR|HON|REV"
_CREDENTIAL = "MD|PHD|ESQ|CPA|JD|RN|DDS|DO|DVM|PE|PMP|MBA"

# A trailing RUN of whole-tokens, each preceded by a space OR start-of-field, with an optional dot.
_NOISE_TAIL = f"(( |^)({_GEN}|{_CREDENTIAL})\\.?)+$"   # gen + credentials (surname-field peel)
_GEN_RUN = f"(( |^)({_GEN})\\.?)+$"                    # gen only (given-field peel + suffix capture)


def _upper(e: str) -> str:
    return f"upper(CAST({e} AS VARCHAR))"


def _last_core(last: str) -> str:
    """Surname (UPPER) with a trailing run of generational + credential tokens peeled. Whole-token,
    end-anchored: ' JR'/' MD'/'JR SR' strip; 'JRINKINS'/'MAY'/'VI' do not. NULL if emptied (a bare 'JR'
    surname → NULL → a key with no surname is not a person key)."""
    return f"nullif(trim(regexp_replace({_upper(last)}, '{_NOISE_TAIL}', '', 'g')), '')"


def _first_core(first: str) -> str:
    """Given name (UPPER) with a leading honorific peeled (followed by a space OR end-of-field, so a bare
    'DR' clears while 'DRAKE' is untouched) and a trailing generational run peeled ('JOHN JR'→'JOHN')."""
    u = f"regexp_replace({_upper(first)}, '^({_HONORIFIC})\\.?(\\s+|$)', '', 'g')"
    u = f"regexp_replace({u}, '{_GEN_RUN}', '', 'g')"
    return f"nullif(trim({u}), '')"


def person_key(last: str, first: str) -> str:
    """Primary blocking key — name_norm(LAST_core FIRST_core). No middle; no generational (peeled from
    BOTH fields, so it is invariant that the key never ends in a gen token). Surname-anchored: NULL when
    _last_core is empty."""
    lc, fc = _last_core(last), _first_core(first)
    return f"CASE WHEN {lc} IS NULL THEN NULL ELSE {name_norm(f'concat_ws(chr(32), {lc}, {fc})')} END"


def first_initial(first: str) -> str:
    """Honorific-stripped first initial — derives from the SAME _first_core as surname_initial_key, so
    the two agree on every row (review P6)."""
    return f"left({_first_core(first)}, 1)"


def surname_initial_key(last: str, first: str) -> str:
    """Recall key — name_norm(LAST_core + first initial). Bridges FEC initials ('BAILEY, C.E.') ↔ SAM full
    given names."""
    lc, fc = _last_core(last), _first_core(first)
    return f"CASE WHEN {lc} IS NULL THEN NULL ELSE {name_norm(f'concat_ws(chr(32), {lc}, left({fc}, 1))')} END"


def generational_suffix(last: str, first: str) -> str:
    """The full trailing generational RUN (e.g. 'JR', 'JR SR'), from whichever field carries it (surname
    wins), dot-stripped. PRESERVED — never enters person_key."""
    gl = f"regexp_replace(trim(regexp_extract({_upper(last)},  '{_GEN_RUN}', 0)), '\\.', '', 'g')"
    gf = f"regexp_replace(trim(regexp_extract({_upper(first)}, '{_GEN_RUN}', 0)), '\\.', '', 'g')"
    return f"nullif(coalesce(nullif({gl}, ''), nullif({gf}, '')), '')"


def last_name_norm(last: str) -> str:
    """Surname-only blocking key — name_norm over the SAME _last_core token used inside person_key."""
    return name_norm(_last_core(last))
```

`middle_norm` uses `name_norm("middle_name")` directly. **No re-inlining of the `name_norm` regex
anywhere.** Verified outputs (literal DuckDB) for the load-bearing cases:

| `first` | `last` | `person_key` | `gen` | `first_initial` | note |
|---|---|---|---|---|---|
| `JOHN` | `SMITH JR` | `SMITH JOHN` | `JR` | `J` | canonical (suffix in last) |
| `JOHN JR` | `SMITH` | `SMITH JOHN` | `JR` | `J` | suffix in **first** field — still merged |
| `DR JANE` | `FOX` | `FOX JANE` | ·NULL· | `J` | honorific stripped |
| `BILL` | `SMITH JR SR` | `SMITH BILL` | `JR SR` | `B` | double-gen run preserved |
| `JOHN` | `JR` | ·NULL· | `JR` | `J` | surname-only-suffix → null person_key |
| `JOHN` | `JRINKINS` | `JRINKINS JOHN` | ·NULL· | `J` | no false peel |

---

## 6. The transform (exact SQL)

Pure DuckDB, clean-room. Reads a `src` relation (the scanned `sam_pocs`). 1:1 passthrough — no dedup, no
filter (lossless).

```python
from core.name_norm import name_norm
from core.person_name_norm import (
    person_key, surname_initial_key, first_initial, generational_suffix, last_name_norm,
)

def build_normalized_pocs_sql() -> str:
    return f"""
    SELECT
        {person_key("last_name", "first_name")}            AS person_key,
        {surname_initial_key("last_name", "first_name")}   AS surname_initial_key,
        {last_name_norm("last_name")}                      AS last_name_norm,
        {first_initial("first_name")}                      AS first_initial,
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

DuckDB envelope (review P7): `memory_limit='24GB'` (matches the 8M-row `sam_pocs` precedent
`sam_pocs.py:97`), `threads=8`, `temp_directory='/tmp/duckdb_spill'`, `preserve_insertion_order=false`.
Image env `LANCE_BYPASS_SPILLING=true`. The projection is narrow (12 source cols, no `pipe_fields` unnest)
— the binding cost is the index-build phase (§4 build order), not the scan.

---

## 7. Worker file (structure — copy the control plane, swap the data plane)

Create **`pipelines/sam_gov/sam_normalized_pocs.py`** (Modal app `sam-gov-normalized-pocs-pipelines`).
**Copy the control-plane skeleton verbatim from
[`sam_normalized_entities.py`](../../pipelines/sam_gov/sam_normalized_entities.py)** — its rollback guard
(`:449-494`), `skip_if_current`, ops ledger, callback, and dry-run are exactly the needed shape. Swap only
the data-plane deltas.

- **Image:** `debian_slim("3.12").pip_install("duckdb>=1.5,<2","lancedb>=0.15","pylance>=7","pyarrow>=17",
  "requests>=2.32","psycopg[binary]>=3.2").env({"LANCE_BYPASS_SPILLING":"true"})
  .add_local_python_source("core.name_norm","core.person_name_norm","core.ops_alert",
  "pipelines.sam_gov.reference.sam_labels")`  ← both norm modules + the shared snap-key.
- **Constants:** `SRC_URI="s3://data-sink/active/sam_pocs/"`,
  `_PROD_URI="s3://data-sink/active/sam_normalized_pocs/"`,
  `DATASET_URI=os.environ.get("SAM_NORMALIZED_POCS_URI",_PROD_URI)`, `_feed_for(uri)`,
  `BTREE_INDEXES=["person_key","surname_initial_key","last_name_norm","uei","cage_code"]` (built in this
  order — high-card first), `BITMAP_INDEXES=["poc_type","source_family","country"]`,
  `DUCKDB_MEMORY_LIMIT="24GB"`, gate constants per §8.
- **Functions** (mirror `sam_normalized_entities.py`):
  - `build_sam_normalized_pocs(trigger_callback_url=None, dataset_uri=None, skip_if_current=True)` —
    secrets `r2-credentials`+`hqx-postgres`+`ops-alerts`; **`skip_if_current` with a content fingerprint**
    (review P9): no-op only when source `(max snap_key(sam_extract_label), count_rows())` equals the
    sidecar's last-success `(sam_label, rows_written)` in `ops.sam_normalized_pocs_runs` — a within-label
    `sam_pocs` content change (different row count) forces a rebuild → materialize → **pre-write gates on
    the Arrow table (§8)** → capture `v_before` → write → **index in §4 order** → **post-write gates,
    `restore(v_before)` on failure** → `_record_run` → `_post_callback` → re-raise on failure.
  - `verify_sam_normalized_pocs(dataset_uri=None)` — read-back: rows / distinct person_key / distinct
    `(person_key,state2,zip5)` where `country='USA'` / homonym ratio / `generational_suffix` fill /
    **count of person_key ending in a gen token (must be 0)** / by poc_type / by source_family / indices /
    6-row sample.
  - `plan_sam_normalized_pocs(dataset_uri=None)` — materialize + gates, **write nothing**. **Prints the
    measured `distinct_person_key`, the `country='USA'` triple, the homonym ratio, and the `person_key`
    alpha-fraction** so §8 floors can be calibrated from real numbers (review P5/P14).
  - `init_ops()` — apply §9 DDL.
  - `@app.local_entrypoint() build(dry_run=False)` — dry-run → `plan_*`; else `build_*` (manual run uses
    `skip_if_current=False`, per `sam_normalized_entities.py:613-615`) then `verify_*`.
- `_r2_storage_options()`, `_new_con()`, `_pg_connect()`, `_prior_success_baseline()`, `_record_run()`,
  `_post_callback()` — copy from `sam_normalized_entities.py`, swapping column/feed names.

---

## 8. Validation gates (no ship without all green)

**Gate timing.** Pre-write gates compute on the in-memory Arrow table and **MUST hard-fail BEFORE
`write_dataset`**. Post-write gates run under the rollback guard: capture
`v_before = lance.dataset(uri).version` before the write; on any post-write failure
`lance.dataset(uri, version=v_before).restore()` then re-raise (clone `sam_normalized_entities.py:449-494`).
First build → `v_before=None`.

**Floor calibration (review P4/P5 — do this, do not skip).** The distinct-key floors below are
**placeholders**. The §12 step-4 dry-run prints the *measured* `distinct_person_key`, the `country='USA'`
triple, and the ratio for the ACTUAL peeled key. Before the first build, set each **sensitive** floor to
`floor(measured × 0.70)` and confirm `floor < measured × 0.75` (so the ±25% Δ-guard — not the floor — is
the binding check on a shrink). The row floor stays a loose catastrophe catcher.

```python
ROW_FLOOR          = 6_000_000   # catastrophe only (matches sam_pocs POCS_ROW_FLOOR); 25% drop is alarming
PK_DISTINCT_FLOOR  = 1_500_000   # PLACEHOLDER → set floor(measured_distinct_person_key × 0.70) from dry-run
ADDR_TRIPLE_FLOOR  = 2_000_000   # PLACEHOLDER → set floor(measured_us_triple × 0.70) from dry-run
BASELINE_MIN_ROWS  = 6_000_000   # == ROW_FLOOR: any floor-passing success can re-baseline (no purge lockout)
DELTA_GUARD        = 0.25
PK_FILL_MIN        = 0.999        # person_key non-null (only the ~0.04% null-surname rows null out)
NAME_ALPHA_MIN     = 0.95        # person_key alpha-fraction — confirm on the PEELED key in the dry-run (P14);
                                 # if it lands ~0.93, set 0.90 with the logged actual
GEO_COFILL_MIN     = 0.95        # person_key ∧ state2 ∧ zip5 (live 98.08%)
HOMONYM_BAND       = (0.10, 0.40) # distinct_person_key / rows — wide; the anchor is the dry-run measure, not 0.263
```

**Pre-write (hard-fail before overwrite):**
1. **Row floor** — `rows ≥ ROW_FLOOR`.
2. **1:1 passthrough** — `rows == count(sam_pocs)` (lossless; no dedup, no filter).
3. **`person_key` fill** — non-null `≥ PK_FILL_MIN`.
4. **Addressable-triple floor** — `distinct (person_key, state2, zip5)` where `country='USA'` `≥ ADDR_TRIPLE_FLOOR` (calibrated).
5. **`person_key` distinct floor** — `≥ PK_DISTINCT_FLOOR` (calibrated).
6. **Homonym-band sanity** — `distinct person_key / rows ∈ HOMONYM_BAND` — catches over-collapse (regex ate the name) and fragmentation (regex failed).
7. **Geo co-fill** — rows with `person_key ∧ state2 ∧ zip5 ≥ GEO_COFILL_MIN`.
8. **Name-alpha** (positional-offset defense) — `person_key` alpha-char fraction `≥ NAME_ALPHA_MIN` (threshold confirmed on the peeled key in the dry-run, P14).
9. **Generational invariant** (person-only; now true by construction after the v2 dual-field peel) —
   **zero** `person_key` end in a gen token (`JR`/`SR`/`II`/`III`/`IV`/`V`/`2ND`/`3RD`/`4TH`), AND
   `generational_suffix` is non-null on `> 0` rows (the extractor fires; prevalence is observability).
10. **Slot-fanout bound** — `max` rows per entity `≤ 6` over `partition by coalesce(uei,'CAGE:'||cage_code)` (structural slot ceiling).
11. **Δ-guards** (vs prior `ops.sam_normalized_pocs_runs` success ≥ `BASELINE_MIN_ROWS`): `±DELTA_GUARD` on `rows`, `distinct_person_key`, `distinct_person_geo_triple`. `SKIP` line when no floor-qualified prior.

**Post-write (then restore-on-failure):**
12. **Write-integrity** — committed `count_rows() == materialized rows`.
13. **Indices present** — all §4 BTREE + BITMAP in the committed manifest.
14. **Round-trip** — a probe `(person_key, state2, zip5)` known-present in the Arrow table returns `≥ 1` row carrying the expected `uei`/`cage_code`.
15. **Point-lookup smoke** — `WHERE person_key = '<known>'` returns `≥ 1` row. **Latency logged, NOT gated** (cold R2 first-seek; WARN above `SEEK_WARN_MS=2000`).

The pre-write gate function `assert_pre_write_gates(metrics, src_count, baseline) -> list[str]` is **pure**
(no R2/Modal/PG) and unit-tested (§11).

---

## 9. ops ledger DDL

Create `pipelines/sam_gov/ops_sam_normalized_pocs_runs.sql` (canonical copy) + mirror verbatim as the
worker's `OPS_DDL`. **`OPS_DDL` is idempotent and self-applied** by `_prior_success_baseline`/`_record_run`
(verified `sam_normalized_entities.py:304,330`), so §12 step 3 is hygiene, not a hard prerequisite (P12).

```sql
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.sam_normalized_pocs_runs (
    id                         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed                       text NOT NULL,        -- 'sam_normalized_pocs'
    dataset_uri                text NOT NULL,        -- s3://data-sink/active/sam_normalized_pocs/
    source_uri                 text,                 -- s3://data-sink/active/sam_pocs/
    sam_extract_label          text,                 -- provenance carried from sam_pocs (skip_if_current key part 1)
    rows_written               bigint,               --                                  (skip_if_current key part 2)
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
and swap: `id:"sam-normalized-pocs"`, `app_name:"sam-gov-normalized-pocs-pipelines"`,
`function_name:"build_sam_normalized_pocs"`, callback fields (`distinct_person_key`,
`distinct_person_geo_triple`).

**Dependency:** upstream = `sam_pocs` (daily 16:30 UTC, `maxDuration:3900`s → worst-case finish ~17:35).
**Preferred (review P11): chain off `sam_pocs`'s success callback**, not a fixed cron — it removes the race
and is the durable form. The dispatcher path is sound (empty-kwargs cron + defaulted `skip_if_current=True`
spreads correctly, `modal_dispatcher.py:53`), but note **no entities Trigger task is deployed**, so the
cron orchestration is unproven in the precedent — **smoke-test the dispatcher path once with `modal run`
before relying on cron.** If an interim cron ships first, use `30 18 * * *` UTC (55-min margin, not 25) and
add an ops alert when `skip_if_current` skips **two consecutive days** (proxy for an upstream that never
finished). The content-fingerprint `skip_if_current` (§7) also covers within-label corrections; the manual
`modal run` path uses `skip_if_current=False` to force a rebuild after an out-of-band `entity_registrations`
re-ingest.

No new endpoint/secret — the Universal Dispatcher resolves `build_sam_normalized_pocs` by name.

---

## 11. Tests

Create **`pipelines/sam_gov/tests/test_sam_normalized_pocs_gates.py`** — mirror
[`test_sam_normalized_gates.py`](../../pipelines/sam_gov/tests/test_sam_normalized_gates.py). Two suites:

**(A) Gate suite** — one raising test per gate 1–11 (pure `assert_pre_write_gates`), including: 1:1
passthrough mismatch, person_key-fill below floor, addressable-triple below floor, homonym-band violation
(both directions), generational-invariant violation (a synthetic `person_key` ending in a gen token must
raise gate 9), slot-fanout > 6, and the Δ-guards skipping with no baseline.

**(B) Primitive suite** — import the `core.person_name_norm` builders, run them through in-memory DuckDB on
the **26-row v2 fixture**, and assert the verified outputs (these all PASS as of v2 — they are the contract):
- `(last='SMITH JR', first='JOHN')` → `person_key='SMITH JOHN'`, `generational_suffix='JR'`.
- `(last='SMITH', first='JOHN JR')` → `person_key='SMITH JOHN'`, `gen='JR'` (**suffix in the first field**).
- **zero** fixture rows have a `person_key` ending in a gen token (gate-9 invariant, layout-independent).
- `(last='FOX', first='DR JANE')` → `person_key='FOX JANE'`, `first_initial='J'`, and `first_initial` ==
  the initial inside `surname_initial_key` ('FOX J').
- `(last='JR', first='JOHN')` → `person_key` NULL (surname-only-suffix), row retained.
- `(last='SMITH III')` → `gen='III'` (not `II`); `(last='BAILEY 3RD')` → `gen='3RD'`;
  `(last='SMITH JR SR')` → `gen='JR SR'`, `person_key='SMITH BILL'`.
- no false peel: `MAY`/`VI`/`MAYO`/`JRINKINS` retain their surname; `DRAKE` (honorific-prefix) not stripped.

Run: `python -m pytest pipelines/sam_gov/tests/test_sam_normalized_pocs_gates.py -q`. **Both suites must be
green on first authoring** (§12 step 2) — they are reproduced from the verified harness, so they pass
against the §5 builders as written.

---

## 12. Execution checklist (do this in order)

```bash
# 0. Branch off main (worktree-aware; never commit on a shared branch)
git checkout -b claude/sam-normalized-pocs origin/main

# 1. Author:
#    core/person_name_norm.py                                  (§5 — verbatim; compose name_norm, do NOT re-inline)
#    pipelines/sam_gov/sam_normalized_pocs.py                  (§6/§7 worker; 24GB; index order; fingerprint skip)
#    pipelines/sam_gov/ops_sam_normalized_pocs_runs.sql        (§9 DDL)
#    src/trigger/sam_normalized_pocs.ts                        (§10)
#    pipelines/sam_gov/tests/test_sam_normalized_pocs_gates.py (§11 — gate suite + primitive suite)

# 2. Unit-test FIRST (pure; no cloud). BOTH suites must be green (they are reproduced from the verified harness).
python -m pytest pipelines/sam_gov/tests/test_sam_normalized_pocs_gates.py -q

# 3. Create the ops table (optional hygiene — OPS_DDL is idempotent + self-applied; a perms hiccup here is non-fatal)
modal run pipelines/sam_gov/sam_normalized_pocs.py::init_ops

# 4. DRY-RUN — counts only, zero writes. READ the printed distinct_person_key / country='USA' triple / ratio /
#    person_key alpha-fraction, then CALIBRATE §8: PK_DISTINCT_FLOOR=floor(measured×0.70),
#    ADDR_TRIPLE_FLOOR=floor(measured×0.70); confirm each < measured×0.75; confirm alpha clears NAME_ALPHA_MIN.
modal run pipelines/sam_gov/sam_normalized_pocs.py --dry-run

# 5. BUILD + index + verify (the authorized data-plane write). Confirm verify_* prints all §4 indices,
#    round-trip (gate 14), point-lookup (gate 15), zero person_key ending in a gen token, and the observability.
modal run pipelines/sam_gov/sam_normalized_pocs.py

# 6. (optional) Deploy dispatcher-resolvable + smoke-test the dispatcher path before wiring cron/callback
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

A gate failure at step 4 or 5 **stops the ship** — diagnose, do not force past a floor. SAM purges
registrations; a legitimate shrink is confirmed (not floored-through), and because `BASELINE_MIN_ROWS ==
ROW_FLOOR`, a confirmed purge re-baselines instead of locking out the Δ-check.

---

## 13. What the sidecar exposes (each consumer defines its own contract — NOT here)

Consumer-agnostic. Exposes, indexed and inline: the person blocking keys (`person_key`,
`surname_initial_key`, `last_name_norm`, all BTREE), confirmatory tiebreaks (`first_initial`, `middle_norm`,
`generational_suffix`), inline geo (`state2`, `zip5`), and `uei`/`cage_code`/`poc_type`/`source_family`/
`country` for traversal/filtering/provenance.

**The two committed consumers — built in their own plans, never here. The boundary is load-bearing:**

- **GTM / audiences → CONTACT (✅).** Collapse occurrences to the targetable grain
  (`DISTINCT ON (person_key, uei)` = human-in-role, recommended for govcon B2B), scope by **USAspending**
  award activity via `uei → crosswalk_sam_usaspending` (clean commercial entity data), enrich
  `employer-domain → email` (`icypeas`/`blitz` — SAM POCs carry identity + employer + postal but **no
  email/phone**, so reachability is a downstream hop), push to `emailbison`.
- **FEC personnel bridge → INTELLIGENCE ONLY (⛔ never a contact source).** A separate
  `pipelines/resolution/crosswalk_fec_sam_pocs.py`, right-side = this sidecar, joining `person_key + state2
  + zip5`. Used to back into employer and validate via **employer-agreement** (`name_norm(fec.employer) =
  sam_normalized_entities.normalized_legal_name` via `uei` — the two normalized surfaces interlock on `uei`;
  the entity key disambiguates the person homonym). **Never** joined-then-contacted: a blocking match is
  homonym-prone, and FEC contributor data carries statutory use restrictions (52 U.S.C. §30111 / 11 CFR
  104.15) against commercial solicitation.

The one promise: a `person_key` lookup is a BTREE point-seek returning the candidate POC **set** with geo +
entity provenance. It does **not** promise a single row per person.

---

## 14. Out of scope

- Any change to `sam_pocs`, `entity_registrations`, `sam_master_entities` (faithful mirrors stay faithful).
  ZERO-ALTERATION is unchanged — verbatim parts remain SoR in `sam_pocs`; this sidecar is additive/derived
  and never splits or mutates a name.
- **The GTM audience builder** (collapse + USAspending scoping + email enrichment + `emailbison`) — own plan.
- **The FEC personnel bridge** (`crosswalk_fec_sam_pocs`) and ALL FEC-specific match/rank/geo logic — own
  plan, built against the rebuilt `fec_individual_contributions`; intelligence-only (§13).
- **`ffata_exec_comp` / `sam_master_contacts` union** — later; needs an explicit alias projection (those
  carry `middle_initial`/`state_or_province`, and File-E an unsplit `officer_name` + no geo, review P13), and
  `ffata_exec_comp`'s opaque `FIRST [MID] LAST` officer string needs a sorted-token key (the only place a
  probabilistic parser is even a candidate — `probablepeople`, MIT, output quarantined to a derived column).
- **A pre-joined `person_key`↔entity table** — consumers join `uei` at query time; not built here.
```
