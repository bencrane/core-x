# FPDS L1 Canonical Spine → 392-col OBT — Canonical End-to-End Execution Plan

**Status:** authored plan, execution-grade. A different agent MUST be able to execute the entire
392-column OBT spine expansion from this document alone, with zero reliance on the authoring
conversation.

**Target dataset:** `usaspending_fpds_canonical_txn`
(`s3://data-sink/active/usaspending_fpds_canonical_txn/`, LanceDB on R2, the SoR read model).

**Builder (single authored artifact + generators):**
- `pipelines/usaspending/usaspending_fpds_canonical.py` — builder + `COLUMN_SPEC` + merge SQL + inline Modal `.spawn()` harness + `index()` + `verify()`
- `pipelines/usaspending/usaspending_fpds_canonical_modal.py` — split Modal wrapper (`smoke_fn`/build/index/verify) — **NOT driven by this runbook** (see Phase D §0)
- `pipelines/usaspending/gen_fpds_canonical_dictionary.py` — dictionary generator (`--probe` fail-closed)
- `pipelines/usaspending/fpds_field_definitions.json` — 378-entry sidecar (== live BULK, 0 drift)
- `pipelines/catalog/schema_catalog.py` — machine-readable schema catalog
- `pipelines/usaspending/_obt_ground_truth/` — **committed** R2-live-probed ground truth (261-add
  snippet + JSON, spec_current, live BULK/FRESH schemas, not-carried enum, meta). Self-contained
  inputs; no ephemeral scratchpad dependency. See its `README.md`.

All file line numbers below are against the **on-disk main checkout `/Users/benjamincrane/core-x` @ `5ddd960`**.

---

## 1. Objective & Scope

### 1.1 The build

Expand `COLUMN_SPEC` in `pipelines/usaspending/usaspending_fpds_canonical.py` from its **current 131
columns** to a **true One-Big-Table (OBT) of 392 columns** that carries **all 378 BULK-dictionary
columns** plus the retained non-BULK columns already on the spine, and rebuild + reindex + verify the
live `usaspending_fpds_canonical_txn` dataset at the new width.

### 1.2 Final width math (verified, on-disk reconstruction)

```
current COLUMN_SPEC entries (main @ 5ddd960)        = 131   (COLUMN_SPEC lines 133–430)
  of which BULK-native columns already referenced    = 117   (distinct BULK cols in existing bulk_expr)
BULK universe (sidecar == live BULK, 0 drift)        = 378
not-carried BULK cols = 378 − 117                    = 261   ← the ADDS
────────────────────────────────────────────────────────────
final OBT width = 131 + 261                          = 392
```

The 261 adds are **entirely additive** — 0 name collisions with the existing 131 (verified). The 14
retained non-BULK columns (spine-native keys/prov/derived + the 12 monthly-unique placeholders) are a
subset of the existing 131 and are preserved verbatim.

### 1.3 Hard scope constraint (non-negotiable)

This build reconciles **exactly TWO sources**:

| Source | URI | Live-probed width |
|---|---|---|
| **BULK** | `s3://data-sink/active/usaspending/transaction_search_fpds/` | 378 cols |
| **FRESH** | `s3://data-sink/active/usaspending_api_fresh/contract_prime_txn/` | 297 cols |

- The **MONTHLY / archive CSV feed is OUT OF SCOPE**. A parallel agent owns monthly re-integration.
- The current **3-source merge (BULK + FRESH + MONTHLY + DELTA tombstones)** is **REDUCED to a
  2-source merge (BULK + FRESH)** — Phase B removes `archive_proj` / `monthly_latest` /
  `monthly_enrich_latest` / `bulk_base` (Tier-1 pg⊕monthly) / DELTA scanner / tombstone /
  reinstatement.
- `canonical_source` provenance becomes ∈ **{fresh, bulk}** (two-way), tie precedence **FRESH > BULK**.
- The **12 monthly-unique enrichment columns** (`treasury_accounts_funding_this_award`,
  `federal_accounts_funding_this_award`, `highly_compensated_officer_1..5_name` / `_amount`) have no
  source in a BULK+FRESH build; they **remain in the schema as typed-NULL placeholders**
  (`feed_expr=None` AND `bulk_expr=None` → `CAST(NULL AS <type>)`) so the 392-wide schema is a stable
  forward-compatible superset the monthly agent can later populate. **Do NOT drop them.**

> **Cross-agent conflict note (documented, resolved by mandate):** Phase-1 LENS-3 verification found
> that all 12 monthly-unique cols DO exist and are ~17–32% populated in live FRESH. Under the retired
> 3-source merge FRESH's copy was masked by MONTHLY precedence, so the "monthly-unique" label is
> literally stale. The operator's HARD SCOPE CONSTRAINT explicitly **overrides** this: the 12 are
> reserved for the parallel monthly agent and MUST be typed-NULL in this build. This is a **deliberate
> data-suppression choice**, not "no source exists." Execute `feed_expr=None`, and leave the inline
> comment (Phase B §3) so the suppression is not silently lost.
>
> A subsequent executability red-team raised this as a "blocker — do NOT null the 12, they are
> FRESH-sourced." **That finding is REJECTED for this build.** It is correct on the fact (FRESH has
> them) but wrong on the decision: the operator's non-negotiable HARD SCOPE CONSTRAINT mandates the
> 12 remain typed-NULL placeholders reserved for the parallel monthly agent. Keeping FRESH's copy
> would (a) partially populate columns the monthly agent is contracted to own — a split-brain source
> for the same 12 cols across two builds — and (b) break the "these 12 are the coordination contract"
> invariant in §B.9. The suppression is intentional and stays. Do not re-litigate; if the operator
> later rescinds the constraint, the monthly agent flips `feed_expr` back on (§B.9) — that is the
> designed reversal path, not this build.

---

## 2. Invariants & Single Source of Truth

### 2.1 COLUMN_SPEC is the ONLY authored artifact

`COLUMN_SPEC` (a `list[dict]`) in `pipelines/usaspending/usaspending_fpds_canonical.py` is the ONLY
hand-authored artifact. **Everything downstream is program-derived from it:**

- each per-source projection leg (`_proj_select`)
- the per-source collapse windows (`bulk_latest`, `fresh_latest`)
- the 2-way merge window (`core_union` → `core_winner`)
- the enrichment REPLACE block (`_enrich_replace_block`)
- the final locked column order (`canonical_out` projection)
- the BTREE / BITMAP index lists' presence-filtering
- the generated field dictionary

Each entry:

```python
{"canonical": <output col name>,
 "duck_type": VARCHAR | BOOLEAN | BIGINT | DOUBLE | DATE | TIMESTAMP,
 "group":     "key" | "core" | "enrich" | "prov",
 "bulk_expr": <BULK projection expr or None>,
 "feed_expr": <FRESH projection expr or None>}
```

Macros (defined in the builder):
- `s(x)` = `nullif(nullif(trim(x),''),'-NONE-')`
- `kbulk(a,b)` = `s(COALESCE(s(a),s(b)))`

### 2.2 Nothing is hand-typed downstream

No magic column count, no per-column index list, no dictionary type map may be hand-edited to
"match" the spec except the ones this plan names explicitly (the smoke-gate width constant and the
`DUCK2ARROW`/`_STOP` BOOLEAN vocabulary entries — both are one-time vocabulary/contract bumps, not
per-column data). If a downstream artifact disagrees with `COLUMN_SPEC`, `COLUMN_SPEC` wins and the
downstream is regenerated.

### 2.3 Load-bearing runtime invariants (must hold end-to-end)

1. **PK uniqueness:** `count(*) == count(DISTINCT contract_transaction_unique_key)` on `canonical_out`
   (fail-closed gate, builder ~L1210–1221).
2. **Collapse schema identity:** `bulk_latest` and `fresh_latest` have identical `(name, type)`
   sequences (both iterate the same `COLUMN_SPEC` in the same order) — gate at ~L1115–1131.
3. **canonical_source domain ⊆ {fresh, bulk}**, tie → FRESH (`source_rank` FRESH=1 < BULK=2).
4. **Publish is last + all-or-nothing:** indices are built locally, then a single
   `_publish_local_to_r2` wipe-then-upload. Any pre-publish failure leaves prod byte-untouched.
5. **Scanner presence-filter** (`if c in bulk_present` / `fresh_present`) is what restricts the 261
   BULK-native adds to the BULK leg only (their `feed_expr=None` → `CAST(NULL AS <type>)` on FRESH,
   never scanned from FRESH) AND neutralizes phantom parser tokens. Do not remove or reorder it.

---

## 3. Ground Truth & Preconditions

### 3.1 Live-probed schemas (authoritative, R2 live-probed 2026-07-03)

| Artifact | Fact |
|---|---|
| BULK live schema | 378 cols; **== the committed sidecar `fpds_field_definitions.json` with 0 drift** (backed by committed live-verification #880, 2026-07-02) |
| FRESH live schema | 297 cols, all VARCHAR (`contract_prime_txn`) |
| CANONICAL (current) | 131 cols, live |

### 3.2 The 261 adds — reconstruction is verified, DO NOT re-type

> **Provenance / durability (RESOLVED):** the `*.json` ground-truth artifacts referenced in the
> original mandate (`proposed_additions.json`, `not_carried_enum.json`, `spec_current.json`,
> `bulk_live_schema.json`, `fresh_live_schema.json`, `proposed_additions_snippet.py`,
> `spec_meta.json`) were originally authored only in a worktree that was deleted mid-session. They
> have since been **recovered and committed into the repo** at
> `pipelines/usaspending/_obt_ground_truth/` — a fresh agent has direct on-disk access, no reliance on
> any ephemeral scratchpad or the authoring conversation. Three independent representations of the same
> 261-add set now agree exactly and are all committed:
>   1. `pipelines/usaspending/_obt_ground_truth/proposed_additions.json` (+ `_snippet.py`) — the
>      original live-R2-probed generation (378 BULK − 117 referenced = 261).
>   2. This plan's Appendix §12 — the full 261-row table (byte-verified identical to (1): same
>      canonicals, duck_types, and bulk_exprs; 0 diffs).
>   3. The deterministic regenerator in Phase A §A.1 — re-derives (1)/(2) from `COLUMN_SPEC` (131) +
>      the 378-entry sidecar and writes `docs/reference/fpds_obt_261_additions.json` + `_snippet.py`.
>
> Use (1) or (2) to paste; run (3) as a fail-closed cross-check (it must print `count: 261`, the exact
> histogram, and `collisions: []`). Do **not** eyeball-retype from any single source.

Deterministic derivation rule (all 261 are `group="enrich"`, `feed_expr=None`, BULK-native):

| Live arrow type | duck_type | bulk_expr | count |
|---|---|---|---|
| `string` | VARCHAR | `s(<col>)` | 143 |
| `bool` | BOOLEAN | `<col>` (bare native) | 84 |
| `int64` | BIGINT | `<col>` (bare) | 13 |
| `double` | DOUBLE | `<col>` (bare) | 11 |
| `timestamp[us]` | TIMESTAMP | `<col>` (bare) | 6 |
| `date32[day]` | DATE | `<col>` (bare) | 4 |
| `timestamp[us, tz=Etc/UTC]` | TIMESTAMP | `CAST(<col> AS TIMESTAMP)` (strip tz) | 1 (`ingested_at` only) |

**Reconstruction results (independently reproduced):**
- not-carried count = **261** (378 BULK − 117 already-referenced)
- histogram = **143 VARCHAR, 84 BOOLEAN, 13 BIGINT, 11 DOUBLE, 6 TIMESTAMP, 4 DATE** ✓ (exact match)
- sole tz col = `ingested_at` → `CAST(ingested_at AS TIMESTAMP)` ✓
- name collisions with existing 131 = **0** ✓
- final width = 131 + 261 = **392** ✓

Paste-ready snippet of all 261 entries: regenerate deterministically (Phase A §A.1) or use the
Appendix (§12). Do **not** eyeball-retype.

### 3.3 The branch

Work from a fresh worktree/branch cut from `main @ 5ddd960` (or later `main`). The pre-generating
worktree `claude/optimistic-albattani-a59b7d` was deleted with zero commits — do not attempt to
resurrect it. **Commit the derivation artifacts** (§A.1) so they survive worktree teardown.

### 3.4 Pre-existing latent blockers already present on main (will HARD-FAIL an unmodified run)

| # | Blocker | File · line | Effect | Fixed in |
|---|---|---|---|---|
| B1 | smoke-gate asserts `len(COLUMN_SPEC) == 75` | `_modal.py` L219, L237 | any `::smoke` fails against the current 131-col spec AND against 392 | Phase C |
| B2 | `verify()` gate `monthly_corrections_applied <= 0 → fail` | builder ~L1421–1423 | in a 2-source build this is always 0 → hard-fails every verify | Phase B §6b |
| B3 | two Modal apps share name `usaspending-fpds-canonical` | builder L~1469, `_modal.py` L~160 | `max_containers=1` does not span them; concurrent launch = double-write hazard | Phase D §0.2 (procedure, never launch both) |

`B1`/`B2` MUST be resolved **before** launching the giant.

---

## 4. Phase A — COLUMN_SPEC expansion (131 → 392) + BOOLEAN type-vocabulary

**File:** `pipelines/usaspending/usaspending_fpds_canonical.py`.

### A.1 Regenerate the 261 entries deterministically (do NOT hand-type)

The paste-ready 261 entries **already exist committed** at
`pipelines/usaspending/_obt_ground_truth/proposed_additions_snippet.py` (261 entries) — that is the
canonical paste source. The step below **re-derives the same set from first principles** as a
fail-closed cross-check and to produce the `docs/reference/`-scoped copies the dictionary/doc tooling
expects. It is a verification gate, not a prerequisite for having the entries. Run it in the worktree
and **commit the outputs** so the derivation is durable and reproducible; if its output disagrees with
the committed `_obt_ground_truth/proposed_additions_snippet.py`, STOP — the sidecar drifted from the
originally-probed live BULK and R2 must be re-probed before proceeding.

```bash
cd /Users/benjamincrane/core-x   # or the active worktree
python3 - <<'PY'
import re, json
src = open("pipelines/usaspending/usaspending_fpds_canonical.py").read()
m = re.search(r'COLUMN_SPEC\s*(?::[^=]*)?=\s*\[', src)
start = m.end()-1; depth=0; i=start; end=None
while i < len(src):
    ch=src[i]
    if ch=='[': depth+=1
    elif ch==']':
        depth-=1
        if depth==0: end=i; break
    i+=1
block=src[start:end+1]
side = json.load(open("pipelines/usaspending/fpds_field_definitions.json"))  # {col:{definition,type}}
bulk_cols=set(side); assert len(bulk_cols)==378
existing=set(re.findall(r'"canonical"\s*:\s*"([^"]+)"', block)); assert len(existing)==131
STOP={"s","kbulk","TRY_CAST","COALESCE","AS","DOUBLE","BIGINT","DATE","TIMESTAMP","VARCHAR",
      "BOOLEAN","INTEGER","replace","upper","substr","CAST","lower","trim","nullif","None","True","False"}
# split entries, collect BULK cols referenced by existing bulk_expr
entries=[]; d=0; buf=""; inobj=False
for c in block:
    if c=='{': d+=1; inobj=True
    if inobj: buf+=c
    if c=='}':
        d-=1
        if d==0 and inobj: entries.append(buf); buf=""; inobj=False
referenced=set()
for e in entries:
    mm=re.search(r'"bulk_expr"\s*:\s*(.*?)(?:,\s*"feed_expr"|,\s*"group"|\})', e, re.S)
    if not mm: continue
    v=mm.group(1).strip()
    if v=="None": continue
    v=v.strip('f').strip('"').strip("'")
    for tok in re.findall(r'[A-Za-z_][A-Za-z0-9_]*', v):
        if tok not in STOP and tok in bulk_cols: referenced.add(tok)
assert len(referenced)==117, len(referenced)
not_carried=sorted(bulk_cols-referenced); assert len(not_carried)==261, len(not_carried)
def duck(a):
    a=a.strip()
    return {"string":"VARCHAR","bool":"BOOLEAN","int64":"BIGINT","double":"DOUBLE",
            "date32[day]":"DATE","date32":"DATE"}.get(a,"TIMESTAMP" if a.startswith("timestamp") else "??"+a)
adds=[]; snippet=[]
for c in not_carried:
    at=side[c]["type"]; dt=duck(c and at)
    if dt=="VARCHAR": bexpr=f"s({c})"
    elif at.startswith("timestamp") and "tz" in at: bexpr=f"CAST({c} AS TIMESTAMP)"
    else: bexpr=c
    adds.append({"canonical":c,"duck_type":dt,"group":"enrich","bulk_expr":bexpr,"feed_expr":None})
    snippet.append('    {"canonical": "%s", "duck_type": "%s", "group": "enrich", "bulk_expr": "%s", "feed_expr": None},'%(c,dt,bexpr))
from collections import Counter
print("count:", len(adds), "hist:", dict(Counter(a["duck_type"] for a in adds)))
print("collisions:", sorted(set(not_carried)&existing))
json.dump(adds, open("docs/reference/fpds_obt_261_additions.json","w"), indent=1)
open("docs/reference/fpds_obt_261_additions_snippet.py","w").write("\n".join(snippet)+"\n")
print("WROTE docs/reference/fpds_obt_261_additions.json + _snippet.py")
PY
git add docs/reference/fpds_obt_261_additions.json docs/reference/fpds_obt_261_additions_snippet.py
```

**Gate:** the script MUST print `count: 261`, `hist: {'VARCHAR': 143, 'BOOLEAN': 84, 'BIGINT': 13,
'DOUBLE': 11, 'TIMESTAMP': 6, 'DATE': 4}` (order may vary), and `collisions: []`. If any differs,
STOP — the sidecar drifted from live BULK; re-probe R2 before proceeding.

### A.2 Insert placement — EXACT, so column ORDER stays deterministic and prov stays last

`COLUMN_SPEC` today (lines 133–430) is **NOT** strictly grouped. Verified group sequence by index
(0-based into the 131-entry list): `key [0–1]` → `core [2–46]` → `enrich [47–85]` (the 12
monthly-unique are here, at indices 74–85) → `core [86–128]` → **prov [129–130]
(`canonical_source`, `built_at`) LAST**. So the last group before prov is **`core`**
(`number_of_actions` at index 128), NOT `enrich`; the last `enrich` entry
(`highly_compensated_officer_5_amount`) is at index 85 with 43 `core` entries after it. Do **not**
look for a clean enrich→prov boundary — there is none.

**Insert all 261 entries (from `fpds_obt_261_additions_snippet.py`) as a single contiguous block
immediately BEFORE the two prov entries** — i.e. after index 128 (the last `core` entry,
`number_of_actions`), before `canonical_source`. The insertion anchor is defined purely by the prov
block, not by any group boundary. This:
- keeps prov strictly last (required — the projection re-derives `canonical_source` via `w.src`; its
  position anchors the `EXCLUDE (src, canonical_source)` logic),
- makes the final live column order = `[existing 129 non-prov, in their current key/core/enrich/core
  order] + [261 adds] + [canonical_source, built_at]` = 392, fully deterministic and prov-last.

The 261 adds are all `group="enrich"`, but they land at the **tail of the list** (just before prov),
not adjacent to the existing enrich block — the `group` field is metadata for the dictionary/collapse
derivation and does not require contiguous list placement. Column ORDER = list order verbatim, so the
only invariant that matters here is "insert immediately before prov."

Wrap the inserted block with a banner comment:

```python
    # ── OBT expansion: 261 BULK-native "documented but not carried" columns ──────────
    # All group="enrich", feed_expr=None (BULK-only pg enrichment), native typing.
    # Generated deterministically from live BULK (378) − already-referenced (117) = 261.
    # See docs/reference/fpds_obt_261_additions.json (committed derivation artifact).
    <261 entries from fpds_obt_261_additions_snippet.py>
    # ── end OBT expansion ────────────────────────────────────────────────────────────
```

### A.3 Edge cases inside the 261 (already encoded by A.1's rule — confirm, do not re-decide)

- **`ingested_at` (tz-strip):** live `timestamp[us, tz=Etc/UTC]` → `bulk_expr = CAST(ingested_at AS
  TIMESTAMP)`, `duck_type = TIMESTAMP`. Sole tz column. All other 5 timestamps are bare `<col>`.
- **84 native BOOLEANs:** bare `<col>` (NOT wrapped in `s()` — `trim()` on a non-VARCHAR errors).
  Requires the BOOLEAN type-vocabulary plumbing (§A.4).
- **56 dual-source cols (also in live FRESH):** KEEP as `group="enrich"`, `feed_expr=None`
  (BULK-native). Zero promotions to `core`. These are entity/award attributes fixed at award time
  (recipient/socioeconomic booleans, static contract descriptors), not per-transaction volatile
  financials. BULK is the authoritative fuller pg dump; the spine already carries the reconciled
  canonical vocabulary (e.g. `women_owned_small_business`) as `core` with both legs. Verified: no
  financial/temporal volatile col in the 261 requires FRESH reconciliation.
- **12 monthly-unique cols → typed-NULL:** these are NOT in the 261 (they are existing entries at
  spec lines ~309–332). Their `bulk_expr` is already `None`; Phase B §3 sets their `feed_expr=None`.
  Handled in Phase B, not here.

### A.4 BOOLEAN type-vocabulary plumbing (REQUIRED — 84 new BOOLEANs enter the vocabulary)

The type vocabulary was 5 types (VARCHAR/BIGINT/DOUBLE/DATE/TIMESTAMP). BOOLEAN is new. Trace of every
`duck_type` consumer and the required action:

| Consumer | File · line | Action |
|---|---|---|
| `DUCK2ARROW` map | `gen_fpds_canonical_dictionary.py` L35–36 | **ADD** `"BOOLEAN": "bool"` (REQUIRED) |
| `_typed_null()` → `CAST(NULL AS BOOLEAN)` | builder L454–455 | none (valid DuckDB; string-interpolates verbatim; → arrow `bool`) |
| `_bulk_source_cols` token filter | builder L468–469 | HARDEN (§A.5, P2) |
| `_feed_source_cols` token filter | builder L486–487 | HARDEN (§A.5, P2) |
| `_proj_select()` | builder L496–520 | none (type-agnostic) |
| dictionary `_STOP` set | `gen_fpds_canonical_dictionary.py` L38–39 | ADD `"BOOLEAN"` (low priority) |
| `_assert_collapse_schema_identity` | builder L1115–1129 | none (compares live DESCRIBE tuples) |
| `verify()` | builder L1351–1442 | none (type-agnostic) |

**A.4a — `DUCK2ARROW` (REQUIRED).** `gen_fpds_canonical_dictionary.py` L35–36:

```python
# BEFORE
DUCK2ARROW = {"DATE": "date32[day]", "TIMESTAMP": "timestamp[us]", "DOUBLE": "double",
              "BIGINT": "int64", "VARCHAR": "string"}
# AFTER
DUCK2ARROW = {"DATE": "date32[day]", "TIMESTAMP": "timestamp[us]", "DOUBLE": "double",
              "BIGINT": "int64", "VARCHAR": "string", "BOOLEAN": "bool"}
```

Why mandatory: `col_table()` (L129) falls back to `DUCK2ARROW.get(c["duck_type"], c["duck_type"].lower())`
in the code-only (non-`--probe`) path. Without the entry the 84 BOOLEANs render as `boolean`
(`.lower()` fallback), disagreeing with Lance's actual `bool`. The `--probe` fail-closed cross-check
(L160–167) compares names/order/indices only — it does not gate on this — so the emitted table must
be correct on its own.

**A.4b — `_STOP` set (low priority, consistency).** `gen_fpds_canonical_dictionary.py` L38–39: add
`"BOOLEAN"`. The 84 bare-bool exprs contain no `BOOLEAN` token, so omission does not corrupt them;
add for future-proofing any `CAST(x AS BOOLEAN)`.

**A.4c — `_typed_null` → `CAST(NULL AS BOOLEAN)` (CONFIRMED valid, no change).** DuckDB BOOLEAN is
first-class; `CAST(NULL AS BOOLEAN)` → arrow `bool` on drain → Lance persists `bool`, matching
`DUCK2ARROW['BOOLEAN']='bool'`. This path is hit for the FRESH leg of every BULK-native BOOLEAN (all
84 have `feed_expr=None`): bare `<bool_col>` from BULK (arrow `bool`) vs `CAST(NULL AS BOOLEAN)` (arrow
`bool`) → **schema-identity gate passes**.

### A.5 Token-filter hardening (P2 hygiene — ship in the same commit, do NOT gate the build on it)

Latent leak already live in the shipped 131-col spec: the `women_owned_small_business` family
(~L335–346) emits `upper(substr(CAST(... AS VARCHAR),1,1))`, injecting `upper`/`substr`/`CAST` tokens
into `bulk_expr`/`feed_expr`. None is in either builder filter set. Harmless today because both
scanner lists are **presence-filtered** (`if c in bulk_present` etc., L1172–1174) — phantom tokens are
not real schema names and get dropped. Harden anyway for correctness hygiene:

```python
# builder L468–469  _bulk_source_cols
if tok in ("s", "kbulk", "TRY_CAST", "CAST", "COALESCE", "AS", "DOUBLE", "BIGINT",
           "DATE", "TIMESTAMP", "VARCHAR", "INTEGER", "replace", "upper", "substr", "lower"):
# builder L486–487  _feed_source_cols
if tok in ("s", "TRY_CAST", "CAST", "AS", "DOUBLE", "BIGINT", "DATE", "TIMESTAMP",
           "VARCHAR", "INTEGER", "replace", "upper", "substr", "lower"):
```

`BOOLEAN` is deliberately NOT added (no expr wraps a value in a `BOOLEAN` keyword; bools are bare).
The presence-filter remains the actual safety net — do not remove it.

---

## 5. Phase B — Merge reduction 3-source → 2-source (BULK + FRESH)

**File:** `pipelines/usaspending/usaspending_fpds_canonical.py`. Apply the change-list top-to-bottom.
Invariant preserved: `canonical_source ∈ {fresh, bulk}`, precedence **FRESH(1) > BULK(2)**, tie →
FRESH. Every MONTHLY / archive / DELTA leg is excised.

> **Ordering vs Phase A:** Phase B edits SQL-generation functions + `build()` + `verify()` — it does
> NOT touch the 261 inserted rows. The one shared row-level edit (the 12 monthly-unique `feed_expr →
> None`, §B.3) is done as part of Phase B because it is a merge-semantics decision. Phase A and Phase B
> are otherwise orthogonal.

### B.0 CTEs / TEMP tables / views to REMOVE (in `_stage1_sql` L548–645 and `_stage2_sql` L651–851)

| # | Artifact | Defined at | Role deleted |
|---|---|---|---|
| 1 | `archive_proj` TEMP | L566–569 | MONTHLY projection |
| 2 | `monthly_latest` TEMP | L619–628 | MONTHLY core collapse |
| 3 | `bulk_keys` TEMP | L631–632 | narrow key set, only fed `m_monthly_corr` (now dead) |
| 4 | `m_rows_in_archive` metric | L636 | MONTHLY rowcount |
| 5 | `bulk_base` TEMP VIEW (Tier-1) | L703–714 | pg⊕monthly reconcile — identity in 2-source |
| 6 | `monthly_enrich_latest` TEMP | L762–773 | MONTHLY enrichment dedup (`m` leg) |
| 7 | `delete_keys` TEMP | L813–819 | DELTA tombstone key set |
| 8 | `m_monthly_corr` metric | L843–846 | monthly-corrections-applied |
| 9 | `m_deletes` metric | L837–840 | tombstoned count |

Plus the `DROP`s referencing removed tables: `DROP VIEW bulk_base` (L730), `DROP TABLE monthly_latest`
(L732), `DROP TABLE archive_proj` (L776), `DROP TABLE monthly_enrich_latest` (L804), `DROP TABLE
delete_keys` (L849), `DROP TABLE bulk_keys` (L850).

**Retained (identity-critical):** `fresh_latest` (L572–586), `bulk_latest` (L592–607), `core_union`
(reduced to 2 arms), `core_winner` (L740–748), `resolved` (L790–798, monthly join stripped),
`canonical_out` (L828–832, reinstatement predicate stripped); metrics `m_rows_in_fresh` (L635),
`m_rows_in_bulk` (L641), `m_fresh_only_tail` (L642–644, repointed), `m_merged` (L751).

### B.1 `_stage1_sql` (L548–645)

- **B.1a** delete `arch_proj = _proj_select("feed", built_at_iso)` (L563); keep `bulk_proj` (L561) +
  `fresh_proj` (L562).
- **B.1b** delete `archive_proj` CREATE (L565–569).
- **B.1c** delete `monthly_latest` CREATE block incl. §3.4 banner (L609–628).
- **B.1d** delete `bulk_keys` (L630–632).
- **B.1e** delete `m_rows_in_archive` (L636).
- **B.1f** **repoint `m_fresh_only_tail` (L642–644)** — it currently ANTI-JOINs `fresh_latest` against
  `bulk_keys`; point it at `bulk_latest` directly:
  ```sql
  CREATE TEMP TABLE m_fresh_only_tail AS
  SELECT count(*) AS c FROM fresh_latest f
  ANTI JOIN bulk_latest b ON f.contract_transaction_unique_key = b.contract_transaction_unique_key;
  ```
- **B.1g** keep `m_rows_in_bulk` (L641).

Net stage-1 output: `fresh_latest`, `bulk_latest`, `m_rows_in_fresh`, `m_rows_in_bulk`,
`m_fresh_only_tail` (5 tables, was 8 + `arch_proj`).

### B.2 `_enrich_replace_block` (L523–545) — drop the monthly COALESCE leg

Replace the body (L538–545) with an unconditional pg leg:

```python
    parts = []
    for c in _cols("enrich"):
        col = c["canonical"]
        parts.append(f"    b.{col} AS {col}")
    return ",\n".join(parts)
```

Update the docstring (L523–537): delete the "Variant C" monthly-unique branch; state all enrich cols
now project `b.<col>` (pg-only), and the 12 monthly-unique cols are typed-NULL placeholders carried on
the BULK leg pending the monthly agent.

### B.3 The 12 monthly-unique cols — `feed_expr → None` (CRITICAL, HARD-SCOPE-MANDATED)

The 12 cols at spec lines ~309–332 already have `bulk_expr=None`. After B.2, their enrich REPLACE emits
`b.<col>` = `bulk_latest.<col>` = `CAST(NULL AS <type>)` (because `bulk_expr is None` → typed NULL in
`_proj_select` L516) — correct. **BUT** their `feed_expr` is currently a live `s(...)` /
`TRY_CAST(...)`, which feeds the FRESH leg — so FRESH rows would carry real values that the enrich
REPLACE then overwrites with NULL. That is inconsistent.

**Set `feed_expr=None` on all 12 rows** (lines ~310, 312, 314, 316, 318, 320, 322, 324, 326, 328, 330,
332 — the `*_name`/`*_amount`/`*_funding_this_award` entries). This makes them typed-NULL on BOTH legs
(clean forward-compatible placeholder) and removes them from `_feed_source_cols()` so the FRESH scanner
stops reading those 12 columns.

Leave the existing comment block (L304–308) plus a hand-off marker (see §B.9) noting the FRESH source
exists but is intentionally deferred to the monthly agent.

### B.4 `_stage2_sql` (L651–851)

- **B.4a** drop the `delta_has_stamp: bool = True` param (L651). New signature `def _stage2_sql() -> str:`.
- **B.4b** delete Tier-1 `bulk_base` VIEW incl. §3.4b banner (L696–714).
- **B.4c** `core_union` → 2-arm:
  ```sql
  CREATE TEMP TABLE core_union AS
  SELECT CAST('fresh' AS VARCHAR) AS src, CAST(1 AS INTEGER) AS source_rank, f.* FROM fresh_latest f
  UNION ALL BY NAME
  SELECT CAST('bulk'  AS VARCHAR) AS src, CAST(2 AS INTEGER) AS source_rank, b.* FROM bulk_latest b;
  ```
  `source_rank` FRESH=1 < BULK=2 → `core_winner`'s `ORDER BY ... source_rank ASC` already yields tie →
  FRESH. No change to `core_winner`.
- **B.4d** DROP boundary (L726–732) → `DROP TABLE fresh_latest;` only. Delete `DROP VIEW bulk_base`
  (L730), `DROP TABLE monthly_latest` (L732). `bulk_latest` lives on (enrichment source).
- **B.4e** `core_winner` (L740–748) unchanged; `m_merged` (L751) unchanged; `DROP TABLE core_union`
  (L752) unchanged.
- **B.4f** delete `monthly_enrich_latest` (L754–773) + its `DROP TABLE archive_proj` boundary
  (L775–776).
- **B.4g** `resolved` (L790–798) — drop the monthly LEFT JOIN (L798). Result:
  ```sql
  CREATE TEMP TABLE resolved AS
  SELECT
    w.* EXCLUDE (src, canonical_source) REPLACE (
  {enrich_block}
    ),
    w.src AS canonical_source
  FROM core_winner w
  LEFT JOIN bulk_latest b ON w.contract_transaction_unique_key = b.contract_transaction_unique_key;
  ```
- **B.4h** DROP boundary (L800–805) → drop `core_winner` (L803) + `bulk_latest` (L805); delete
  `DROP TABLE monthly_enrich_latest` (L804).
- **B.4i** tombstone/reinstatement → passthrough. Delete `delete_keys` CTE (L807–819) + R5/R6 banners.
  Replace `canonical_out` (L828–832):
  ```sql
  CREATE TEMP TABLE canonical_out AS
  SELECT {canon_cols} FROM resolved;
  ```
- **B.4j** late-metric block (L834–846) → delete `m_deletes` + `m_monthly_corr` + banner (whole block).
- **B.4k** final DROPs (L848–851) → `DROP TABLE resolved;` only; delete `DROP TABLE delete_keys`
  (L849) + `DROP TABLE bulk_keys` (L850).
- **B.4l** rewrite `_stage2_sql` docstring (L652–685): 2-source flow (`fresh_latest`/`bulk_latest` →
  `core_union` 2-arm → `core_winner` argmax mtime tie→FRESH → `resolved` pg-enrich + `w.src` →
  `canonical_out` locked projection, no tombstone). Delete all TWO-TIER / bulk_base / MONTHLY /
  tombstone / reinstatement prose.

### B.5 `_build_merge_sql` (L854–865)

`_stage2_sql()` is now called with no args (the call at L865 already passes nothing else) — trim the
`--since` delta comment (L859–860) from "delta scanner" → "the two data scanners."

### B.6 `_assert_collapse_schema_identity` (L1115–1131)

Change the iterate tuple (L1122) `("bulk_latest", "fresh_latest", "monthly_latest")` →
`("bulk_latest", "fresh_latest")` (`monthly_latest` no longer exists → `DESCRIBE` would raise). Both
survivors iterate the same `COLUMN_SPEC` in the same order at 392 cols → gate passes.

### B.7 `build()` (L1134–1307)

- **B.7a** delete `arch_ds = lance.dataset(ARCHIVE_FULL_URI...)` + `delta_ds = lance.dataset(
  ARCHIVE_DELTA_URI...)` (L1163–1164).
- **B.7b** delete `arch_present` + `delta_present` (L1169–1170).
- **B.7c** delete `arch_scan_cols` (L1174), `delta_scan_cols` (L1177–1178), `delta_has_stamp`
  (L1179). Keep `bulk_scan_cols` (L1172) + `fresh_scan_cols` (L1173).
- **B.7d** delete `archive_r` registration (L1189–1190) + `archive_delta_D` registration
  (L1191–1193). Keep `bulk_r` (L1184–1185) + `fresh_r` (L1187–1188).
- **B.7e** stage-2 call (L1201): `_stage2_sql(delta_has_stamp=delta_has_stamp)` → `_stage2_sql()`.
- **B.7f** metric reads: delete `rows_in_archive_full` read (L1207), `deletes_tombstoned` read
  (L1226), `monthly_corrections_applied` read (L1232). `dedup_collapsed` (L1231): drop archive term →
  `int(rows_in_bulk + rows_in_fresh - merged_rows)`. `fresh_only_tail` (L1225) stays (its
  `m_fresh_only_tail` now ANTI-JOINs `bulk_latest`).
- **B.7g** keep initializers `rows_in_archive_full = 0` / `deletes_tombstoned = 0` /
  `monthly_corrections_applied = 0` (L1144–1147) — they become permanent-0 sentinels (never
  reassigned), preserving the ops-row schema.
- **B.7h** log line (L1235–1238): trim to `core_winner / rows_out / fresh_only_tail / max_action_date`.
- **B.7i** `metrics` dict (L1280–1289): leave the three keys (ops-schema stability); values serialize
  as 0.

### B.8 Ops-ledger + `verify()`

- **B.8a `_record_run` (L1075–1104): NO code change.** `rows_in_archive_full` /
  `deletes_tombstoned` / `monthly_corrections_applied` remain in the `ops.
  usaspending_fpds_canonical_runs` DDL (`OPS_SQL_FILE` L110) and the INSERT column list. `build()`
  passes 0 for all three. Do NOT alter the DDL or the INSERT — the ops table is shared with historical
  rows and the parallel monthly agent.
- **B.8b `verify()` — delete the false-failing monthly gate (L1421–1423)** and its metric
  computation (L1403–1405, `monthly_corrections_applied = con.execute("... canonical_source =
  'monthly'")`). In a 2-source build this is always 0 → hard-fails every verify (blocker B2).
- **B.8c `verify()` — tighten canonical_source domain (L1406–1409, L1418–1420) to {fresh, bulk}:**
  ```python
  bad_source_domain = con.execute(
      "SELECT count(*) FROM c WHERE canonical_source IS NULL "
      "OR canonical_source NOT IN ('fresh','bulk')").fetchone()[0]
  ```
  Update the failure string `∉ {{fresh,bulk,monthly}}` → `∉ {{fresh,bulk}}`.
- **B.8d `verify()` output dict (L1425–1441):** remove the `monthly_corrections_applied` key +
  computation. Keep `canonical_source_distribution` (L1433, shows only fresh/bulk). Update docstring
  (L1351–1373): drop MONTHLY / tombstone / reinstatement / `monthly_corrections_applied > 0` from the
  GATES list; state domain ⊆ {fresh, bulk}. Rewrite INV-7 (L1371–1373): the surviving cross-source
  gate is domain ⊆ {fresh, bulk}.

### B.9 Coordination note — DELETE the monthly legs outright (do NOT flag-guard)

The monthly agent owns re-integration and will re-author `archive_proj` / `monthly_latest` /
`monthly_enrich_latest` / tombstone against the renamed, corrected upstream (the tracked rename
`usaspending_archive_*_fpds` → a `monthly_*` dataset). A dead `if ENABLE_MONTHLY:` branch is stale
scaffolding they must delete anyway, and a live-but-off flag is a latent 3-source path that can
silently violate the 2-source scope. Clean deletion shrinks this file's monthly footprint to zero →
the monthly agent's re-add is a self-contained additive diff against a known-clean 2-source base
(smallest possible conflict). **The 12 typed-NULL placeholders in `COLUMN_SPEC` ARE the coordination
contract** — the monthly agent flips those `feed_expr` back on (or points them at the renamed source)
and re-adds their collapse + COALESCE leg; the schema stays 392-wide throughout.

Leave a one-line hand-off marker at the top of `_enrich_replace_block` (after §B.2) and at the
`core_union` 2-arm site (§B.4c):

```
# 2-source (BULK+FRESH) build. MONTHLY re-integration is owned by a parallel agent;
# the 12 monthly-unique enrich cols (COLUMN_SPEC ~lines 309-332) are typed-NULL
# placeholders (feed_expr=None AND bulk_expr=None) reserved for that re-add. Do NOT
# resurrect archive_proj/monthly_latest/tombstone here — re-author against the renamed
# monthly upstream when it lands.
```

### B.10 Module header + banners (L1–71) — factual correctness

- L3–9: "THREE FPDS transaction feeds" → "TWO"; delete `ARCH_F` (L8) + `ARCH_D` (L9); `~78 typed
  columns` → 392.
- L15–42 (MERGE block): strip MONTHLY / TIER-1 / bulk_base / tombstone / reinstatement / DELTA prose.
  Keep: s()/kbulk() sentinel, per-source collapse (fresh_latest/bulk_latest), single 2-way window
  (source_rank FRESH<BULK tie→FRESH), pg enrichment fill, canonical_source ∈ {fresh, bulk}, fail-closed
  PK gate.
- L51–52 (`--since` note): "THREE DATA scanners" → "TWO"; delete "NEVER the delta scanner".

### B.11 Out-of-scope-but-adjacent (do NOT touch)

- `ARCHIVE_FULL_URI` / `ARCHIVE_DELTA_URI` (L96–97), `DELTA_STAMP_COL` (L648): leave defined (cheap,
  harmless; the monthly agent re-consumes `ARCHIVE_*`). Removing adds churn + a merge-collision risk on
  the constant block.
- Modal `build_fn`/`build_spawn` sizing (L1474–1503): unchanged (handled in Phase D).
- `OPS_SQL_FILE` DDL: untouched (§B.8a).

### B.12 Gates that hold unchanged at 392 cols / 2 legs (confirm, no edit)

- PK-uniqueness fail-closed gate (L1210–1221): structural `count(*)` vs
  `count(DISTINCT contract_transaction_unique_key)` on `canonical_out` — one survivor per key
  regardless of column/source count. Dropping the reinstatement WHERE (§B.4i) cannot add dups
  (`canonical_out` = `resolved` 1:1, `resolved` 1:1 with `core_winner`).
- `verify()` INV-1/INV-4 read-back gate (L1413–1415): structural, source-count-independent.

---

## 6. Phase C — Index plan + smoke-gate bump

**Files:** `usaspending_fpds_canonical.py`, `usaspending_fpds_canonical_modal.py`.

### C.1 Index topology — UNCHANGED (11 BTREE + 7 BITMAP = 18)

`usaspending_fpds_canonical.py` L438–442:
- **11 BTREE:** `contract_transaction_unique_key`, `contract_award_unique_key`, `recipient_uei`,
  `action_date`, `last_modified_date`, `naics_code`, `product_or_service_code`,
  `federal_action_obligation`, `recipient_hash`, `award_id_piid`, `pop_county_fips`.
- **7 BITMAP:** `action_date_fiscal_year`, `type_of_set_aside_code`, `awarding_agency_code`,
  `award_type_code`, `idv_type_code`, `canonical_source`, `subcontracting_plan`.

**Add ZERO of the 84 new BOOLEANs to `BITMAP_COLS`.** Rationale: on a 107M-row universe these
recipient/entity-classification flags are overwhelmingly skewed to a single value — a BITMAP on a
near-constant column buys almost nothing for the common `= false` filter and only helps the rare-true
probe, which is not a known spine pushdown pattern (these are downstream serving-layer filters). Each
`create_scalar_index` commits one manifest + a RAM/disk pass over a 107M column; 84 extra BITMAPs
materially lengthen the index stage for negligible value. If a specific serving query later filters hot
on one bool, add that single column surgically, backed by a query pattern. **`BTREE_COLS`/`BITMAP_COLS`
stay exactly as-is (L438–442 unchanged).**

### C.2 Index mechanics hold at 392 (confirm, no edit)

- Presence-filter (`[c for c in BTREE_COLS if c in present]` / `BITMAP_COLS`) in
  `_build_indices_local()` (builder L1038, L1045) and wrapper `index_fn` (`_modal.py` L365, L372) —
  all 18 indexed cols are in the 131-col core set → all 18 still build at 392; the 261 adds are never
  indexed → correctly skipped.
- Append-only `index_fn` R2 key-set diff, the two FAIL-CLOSED gates (no `data/*.lance` in diff;
  positive `_indices/`/`_versions/`/`_transactions/` whitelist), the `latest_version_hint.json`-last
  ordering (`_modal.py` L388–470), and the `>= 1 manifest` lower-bound (L462–470) are all
  column-count-independent.
- **Cosmetic:** `_modal.py` L321, L458 say "the 16-column plan emits ~16 new manifests" — the plan is
  **18** (11+7), stale even at 131. Fix comments to "18-column"/"18". The assertion is a `>=1` lower
  bound, so not a functional bug. Low priority.

### C.3 Smoke-gate width bump (REQUIRED / BLOCKER B1)

`usaspending_fpds_canonical_modal.py`:

| Line | Change |
|---|---|
| L219 | `assert n_cols == 75, ...` → `assert n_cols == 392, f"COLUMN_SPEC has {n_cols} entries, expected 392"` |
| L237 | `"column_spec_ok": n_cols == 75,` → `"column_spec_ok": n_cols == 392,` |
| L208, L217 | comment "75" → "392" |

This assertion is already broken against the live 131-col spec (frozen at an early 75-col draft). It is
step 0a, the wrapper's mandatory blocking gate. **This runbook does NOT drive the wrapper's `::smoke`
(Phase D §0), so this fix is for hygiene + anyone who runs the wrapper by habit — but it MUST land in
the expansion commit.** Flag it as a pre-existing latent bug in the commit message.

Optional anti-drift form (if the operator prefers expansion-tolerant): assert the internal invariant
`len(COLUMN_SPEC) == len(set canonicals)` (uniqueness) + `>= 392` (floor). Default to exact `== 392`
per the "locked column contract" framing.

---

## 7. Phase D — Modal `.spawn()` execution runbook

**Method (mandated, verified against the prior 131-col build 48h ago #877):** launch via
`modal run --detach ...usaspending_fpds_canonical.py --cmd build_spawn` (the main-module inline
`.spawn()` harness). Completion tracked by the **TWO-SOURCE AND sentinel** — Modal app state AND a
fresh ops-ledger `status='success'` row with `columns=392` — **NEVER the ledger alone** (an OOM/reap
writes no row). retries=0, overwrite-idempotent, local-materialize → boto3 uniform-part publish,
`/tmp` spill.

### D.0 Pre-flight — two blocking facts

- **D.0.1** The inline harness has NO `smoke_fn` (grep: `smoke` appears only in the wrapper). Phase-1
  smoke below is a hand-rolled foreground doppler check, NOT the wrapper's `::smoke`. Do NOT run
  `..._modal.py::smoke` (its `== 75` assertion — even after the §C.3 bump the wrapper's split model
  conflicts with the folded inline build; leave the wrapper alone).
- **D.0.2** Two Modal apps share the name `usaspending-fpds-canonical` (builder L~1469, `_modal.py`
  L~160). `max_containers=1` is per-app-object and does NOT span the two. **Never launch both
  harnesses concurrently. This runbook uses exactly ONE (the inline).** Before every launch, `modal app
  list | grep usaspending-fpds-canonical` MUST show zero live apps.

### D.1 Sizing — use the inline harness box, NOT the wrapper's

| Knob | Inline `build_fn` (builder L1474–1475) — **USE** | Wrapper `build_fn` (do NOT use) |
|---|---|---|
| memory | **196608 (192 GiB)** | 131072 (128 GiB) |
| ephemeral_disk | **524288 (512 GiB explicit)** | none (`/tmp` 512 GiB default) |
| cpu | 16.0 | 16.0 |
| timeout | **12 h** | 8 h |
| DuckDB mem | **160GB (image `.env`, L1463)** | 96GB |
| DuckDB threads | **16 (L1464)** | 8 |
| index | **FOLDED into build (L1267–1276)** | separate `index_fn` |

Reasons: (1) mandate + precedent (#877 used inline `build_spawn`; `build_spawn`/`index_spawn` exist
ONLY in the inline harness at `modal_main` L1497/L1506; the wrapper has no `.spawn()` entrypoint); (2)
the fold is the safer atomic unit — inline `build()` writes data → builds all 18 indices locally →
**one** `_publish_local_to_r2`, so prod is never published-but-unindexed and there is no re-download to
index; (3) the 3×-wider projection needs the bigger box (§D.1.1).

**Do NOT downshift to the wrapper's 128 GiB.**

#### D.1.1 The 3× width risk to the 512 GiB `/tmp` ceiling

The 131-col build spilled ~100–180 GiB (246 GiB pre-inlining). The dominant spill is the single-pass
`bulk_r` 107M per-key window-collapse, whose footprint scales with projected row width. 131 → 392 cols
widens the BULK projection ~3× (261 new BULK-native cols; the 84 booleans + typed numerics are narrow,
so byte-width grows less than 3× while row-count is unchanged at 107M).

- DuckDB spill (`/tmp/fpds_canonical_duckdb`): ~180 GiB × up-to-3× → worst case ~300–450 GiB, bounded
  by `memory_limit=160GB` (more DuckDB mem ⇒ less spill).
- Local Lance stage (`/tmp/fpds_canonical_stage/canonical_lance`): 107M × 392 cols, ~150–270 GiB
  (was ~50–90 GiB).
- Both share the same 512 GiB ephemeral disk. **Mitigation already in code (do not re-engineer):**
  `build()` L1265 `shutil.rmtree(DUCK_TMP)` drops the spill BEFORE `_build_indices_local`, and L1257–
  1264 `malloc_trim(0)` returns RSS. So spill and index-write do NOT co-peak. The genuine co-peak is
  **DuckDB spill + Lance stage during `write_dataset` (L1243)** — the number the SAMPLE (D.2 Phase 2)
  measures.
- **The one sizing knob the sample gates:** if the sample's measured `/tmp` co-peak projects the full
  run > ~450 GiB, raise `ephemeral_disk` (builder L1475) `524288` → `786432` (768 GiB) BEFORE the
  giant. Keep `FPDS_CANONICAL_DUCKDB_MEM=160GB` (more DuckDB mem = less spill = lower disk peak).

### D.2 Phased command sequence (literal; run from the worktree with Doppler-injected R2 + Postgres)

**Phase 0 — init_ops (MANDATORY, once).**
```bash
cd /Users/benjamincrane/core-x   # or the active worktree
doppler run -p core-x -c prd -- \
  python3 -m pipelines.usaspending.usaspending_fpds_canonical init_ops
# capture the BASELINE latest ledger row (to distinguish a fresh success from a stale prior one):
doppler run -p core-x -c prd -- python3 - <<'PY'
import os, psycopg
with psycopg.connect(os.environ["HQX_DB_URL_POOLED"]) as c, c.cursor() as cur:
    cur.execute("SELECT id, status, rows_out, columns, max_action_date, recorded_at "
                "FROM ops.usaspending_fpds_canonical_runs ORDER BY recorded_at DESC LIMIT 1")
    print("BASELINE_LEDGER_ROW:", cur.fetchone())
PY
```
Record `BASELINE_LEDGER_ROW.id` — the giant's success row must have a strictly greater id AND
`columns=392`.

**Phase 1 — smoke gate (foreground, seconds/pennies; hand-rolled, NOT the wrapper `::smoke`).**
```bash
doppler run -p core-x -c prd -- python3 - <<'PY'
from pipelines.usaspending import usaspending_fpds_canonical as f
n = len(f.COLUMN_SPEC)
assert n == 392, f"COLUMN_SPEC={n}, expected 392"
assert len(f.BTREE_COLS) == 11 and len(f.BITMAP_COLS) == 7, (len(f.BTREE_COLS), len(f.BITMAP_COLS))
so = f._r2_so(); assert so.get("endpoint") and so.get("aws_access_key_id"), "R2 creds missing"
print({"column_spec": n, "btree": len(f.BTREE_COLS), "bitmap": len(f.BITMAP_COLS),
       "r2_ok": True, "status": "ok"})
PY
```
Require `column_spec: 392` and `status: ok`. On failure STOP — the expansion PR is incomplete; do NOT
launch the giant.

Optionally validate the 2-source merge SQL string (pure, no R2):
```bash
doppler run -p core-x -c prd -- \
  python3 -m pipelines.usaspending.usaspending_fpds_canonical print_merge_sql | head -60
# eyeball: NO archive_proj / monthly_latest / archive_delta_D / tombstone fragments
```

**Phase 2 — SAMPLE build (GATES the giant; throwaway `_sample` URI, prod never touched).**
```bash
modal run --detach pipelines/usaspending/usaspending_fpds_canonical.py \
  --cmd build_spawn \
  --since 2025-10-01 \
  --target-uri s3://data-sink/active/_sample/usaspending_fpds_canonical_txn_sample/ \
  2>&1 | tee /tmp/fpds_sample_launch.log
SAMPLE_APP_ID=$(grep -oE 'ap-[A-Za-z0-9]+' /tmp/fpds_sample_launch.log | head -1)
echo "SAMPLE_APP_ID=$SAMPLE_APP_ID"
modal app logs "$SAMPLE_APP_ID" --tail 200   # expect: "indexing LOCAL", BTREE ✓×11, BITMAP ✓×7, "published … files", "DONE"
```
**Sample gate — proceed to the giant ONLY when:** the sample published cleanly, `indices_built` length
= 18 in the ledger row, and the observed `/tmp` peak leaves margin under 512 GiB. If the peak projects
the full run > ~450 GiB, raise `ephemeral_disk` (L1475) → `786432` and re-sample. The
`--since 2025-10-01` slice exercises the widest per-key collapse at ~1–2% of rows — spill-per-row ×
107M/slice-rows is the extrapolation basis.

**Phase 3 — FULL BUILD (detached `.spawn()` giant; NO `--since`, NO `--target-uri` → defaults to
`CANONICAL_URI`).**
```bash
modal run --detach pipelines/usaspending/usaspending_fpds_canonical.py \
  --cmd build_spawn \
  2>&1 | tee /tmp/fpds_build_launch.log
APP_ID=$(grep -oE 'ap-[A-Za-z0-9]+' /tmp/fpds_build_launch.log | head -1)
CALL_ID=$(grep -oE '"call_id": *"[^"]+"' /tmp/fpds_build_launch.log | head -1)
echo "APP_ID=$APP_ID  $CALL_ID"   # persist both — handles for logs / stop / liveness
```
`build_spawn` submits `build_fn.spawn(...)` and the client exits in seconds; `--detach` keeps the app +
call alive independent of this session.

**Phase 4 — completion detection (TWO-SOURCE AND sentinel — never the ledger alone).**
```bash
# (a) Modal app state — authoritative for OOM/timeout/reap:
modal app list | grep "$APP_ID"          # running → keep polling; stopped → check (b)
modal app logs "$APP_ID" --tail 200      # "DONE → …" OR an OOM/timeout banner
# (b) ledger row — success confirmation + metric envelope (NOT the sole sentinel):
doppler run -p core-x -c prd -- python3 - <<'PY'
import os, psycopg
with psycopg.connect(os.environ["HQX_DB_URL_POOLED"]) as c, c.cursor() as cur:
    cur.execute("SELECT id, status, rows_out, columns, max_action_date, "
                "indices_built, error_message, started_at, completed_at, recorded_at "
                "FROM ops.usaspending_fpds_canonical_runs ORDER BY recorded_at DESC LIMIT 1")
    print(cur.fetchone())
PY
```

| Modal app state | Ledger row | Verdict |
|---|---|---|
| `stopped` | new `id` > baseline, `status='success'`, `columns=392`, `rows_out≈107M`, `indices_built` has 18 | **PASS** → published AND indexed (fold). Skip Phase 5; go to Phase 6. |
| `stopped` | new `id`, `status='error'` | **FAIL** — read `error_message`; prod untouched (publish is last, all-or-nothing L1267–1276); fix; re-launch Phase 3. |
| `stopped` | NO new row (id == baseline) | **OOM/REAP FAIL** — inspect logs; prod untouched; `modal app stop "$APP_ID"`; investigate disk peak (§D.1.1); bump `ephemeral_disk`; re-launch. |
| `running` | (any) | **keep polling** — never advance. |

The ledger has no `status='running'` start-row, so "app stopped + no new row" is the sole OOM/reap
signal; Modal app state is authoritative for that case.

**Phase 5 — INDEX (conditional; SKIP on a clean folded PASS).** Inline `build()` already builds all 18
indices + publishes atomically. `index_spawn` is a **repair path only** — run ONLY if Phase 4 shows
`status='success'` but `indices_built` empty/short:
```bash
modal run --detach pipelines/usaspending/usaspending_fpds_canonical.py \
  --cmd index_spawn \
  2>&1 | tee /tmp/fpds_index_launch.log
IDX_APP_ID=$(grep -oE 'ap-[A-Za-z0-9]+' /tmp/fpds_index_launch.log | head -1)
modal app list | grep "$IDX_APP_ID"; modal app logs "$IDX_APP_ID" --tail 200   # require BTREE/BITMAP ✓
```
On a normal folded PASS do NOT run this — it re-downloads ~200+ GiB for nothing.

**Phase 6 — VERIFY (foreground; the index-corruption gate).**
```bash
modal run pipelines/usaspending/usaspending_fpds_canonical.py --cmd verify \
  2>&1 | tee /tmp/fpds_verify.log
# require: pk_unique true, rows_out ≈107M, canonical_source domain ⊆ {fresh,bulk}, read-back "pass": true
```
"Build published" is NON-TERMINAL until verify passes. On index presence/corruption failure, re-run
Phase 5 (repair) then re-verify.

### D.3 Idempotency, kill switch, mutual-exclusion

- **Idempotency:** `build()` is overwrite + retries=0. `_publish_local_to_r2` (L897–919) wipes the
  entire prod prefix (`delete_objects` ≤1000/batch) then uploads file-by-file. Publish is last +
  all-or-nothing (indices built before `_s3()` is touched) → any pre-publish failure leaves prod
  byte-untouched. This is what makes `--detach` safe.
- **Kill switch (any phase):** `modal app stop "$APP_ID"` (or `$SAMPLE_APP_ID`/`$IDX_APP_ID`). Safe
  anywhere — pre-publish = prod untouched; post-publish = dataset already complete.
- **Double-launch mutual-exclusion:** `max_containers=1` does NOT span two ephemeral `modal run`
  invocations. Before EVERY launch: `modal app list | grep usaspending-fpds-canonical` MUST be empty.
  Never launch the wrapper + inline together (name collision, §D.0.2). No schedule is armed today; if
  one is added later, pause it before any manual run.
- **Spill-dir hygiene:** `build()` pre-`rmtree`s the stage (L1154) + `finally` `rmtree`s `SCRATCH`
  (L1306); L1265 drops `DUCK_TMP` mid-run. A fresh container starts clean; a re-launch after a kill
  needs no manual `/tmp` cleanup.

---

## 8. Phase E — Verify + dictionary regen (fail-closed) + schema_catalog row

### E.1 Dictionary regen (fail-closed `--probe` against the NEW live 392-col dataset)

```bash
# E.1a — dump live probe.json (read-only) from the freshly-published R2 dataset:
doppler run -p core-x -c prd -- python3 - <<'PY'
import os, json, lance
from pipelines.usaspending import usaspending_fpds_canonical as f
so = f._r2_so()
ds = lance.dataset(f.CANONICAL_URI, storage_options=so)
sch = ds.schema
json.dump({"uri": ds.uri, "rows": ds.count_rows(), "ncols": len(sch),
           "n_indices": len(ds.list_indices()), "version": ds.version,
           "schema": [{"name": sch.field(i).name, "type": str(sch.field(i).type)} for i in range(len(sch))],
           "indices": ds.list_indices()},
          open("/tmp/fpds_probe.json", "w"), default=str, indent=2)
print("rows/ncols/n_indices:", ds.count_rows(), len(sch), len(ds.list_indices()))
PY

# E.1b — regenerate the dictionary, fail-closed against the probe:
doppler run -p core-x -c prd -- \
  python3 pipelines/usaspending/gen_fpds_canonical_dictionary.py \
    --probe /tmp/fpds_probe.json --verified-date "$(date +%F)"
```
`ncols` MUST be **392**; `--probe` refuses to emit (SystemExit) if live schema names ≠ the 392
`COLUMN_SPEC` canon order OR indexed cols ≠ `BTREE_COLS + BITMAP_COLS` (18). A green emit is the
schema-contract proof. The §6 (`FPDS_CANONICAL_FIELD_DICTIONARY.md`) §6 "not-carried" section will now
show 0 (all 261 carried); regen updates it automatically.

### E.2 schema_catalog row (note the BULK/MONTHLY catalog gap)

`usaspending_fpds_canonical_txn` is NOT in `schema_catalog.py::TARGETS` today (L134–160 lists only the
FRESH `contract_prime_txn` leaf; BULK and MONTHLY are also absent — a known catalog gap, out of scope
to fill here). Add the canonical URI under the `usaspending` group:
```bash
# E.2a — add to TARGETS (in the expansion PR): after the contract_prime_txn line, insert:
#   ("usaspending", "s3://data-sink/active/usaspending_fpds_canonical_txn/"),

# E.2b — capture the 392-col fingerprint:
doppler run -p core-x -c prd -- \
  python3 -m pipelines.catalog.schema_catalog \
    --target usaspending=s3://data-sink/active/usaspending_fpds_canonical_txn/
# confirm: n_columns=392 in the run report
```

---

## 9. Phase F — Ship (PR → squash-merge → pull → verify)

```bash
git add -A
git commit -m "feat(usaspending): FPDS canonical OBT — 131→392 cols (2-source BULK+FRESH, rebuilt+verified live)

- COLUMN_SPEC +261 BULK-native adds (143 VARCHAR/84 BOOL/13 BIGINT/11 DOUBLE/6 TS/4 DATE)
- BOOLEAN type vocabulary (DUCK2ARROW/_STOP); ingested_at tz-strip
- merge reduced 3→2 source (BULK+FRESH), canonical_source ∈ {fresh,bulk} tie→FRESH
- 12 monthly-unique cols → typed-NULL placeholders (reserved for monthly agent)
- fix pre-existing latent smoke-gate assertion (75→392) and verify() monthly gate
- dictionary regenerated fail-closed; schema_catalog row added

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
git push -u origin "$(git branch --show-current)"
gh pr create --base main --fill
gh pr merge <num> --squash --delete-branch
# then, in the operator's main checkout (DONE ≠ merged):
cd /Users/benjamincrane/core-x && git fetch && git pull && git log -1 --oneline
```

**Done = the operator's main checkout reflects the merged commit on disk.** Verify with
`git log -1 --oneline`. A PR landing server-side is a midpoint, not a terminal state.

---

## 10. Risk Register & Rollback

| Risk | Likelihood | Mitigation / Rollback |
|---|---|---|
| OOM / disk-ceiling on the 3×-wider projection | medium | SAMPLE gates the giant; `ephemeral_disk` 512→768 GiB knob; `memory_limit=160GB` reduces spill; spill + index-write do not co-peak (L1265 rmtree). |
| Publish fails mid-upload | low | Publish is last + all-or-nothing; prod byte-untouched on any pre-publish failure. Re-launch Phase 3 (overwrite-idempotent). |
| Half-uploaded / corrupt indices | low | `verify()` index-presence gate (Phase 6); repair via `index_spawn` (Phase 5) then re-verify. |
| PK duplication | very low | Fail-closed PK gate (L1210–1221) aborts the build before publish. |
| Stale ledger read (OOM writes no row) | medium | TWO-SOURCE AND sentinel — Modal app state AND new ledger id>baseline with `columns=392`. Never the ledger alone. |
| Schema drift (sidecar ≠ live BULK) | low | A.1 regen asserts count=261 + exact histogram; `--probe` dictionary regen (Phase E) fail-closes on any name/order/index divergence. |
| Double-launch (two apps share name) | medium | `modal app list | grep` MUST be empty before every launch; never run wrapper + inline together. |
| verify() false-fail on monthly gate | certain if unfixed | Blocker B2 removed in §B.8b before launch. |
| Monthly agent merge conflict | medium | Monthly legs deleted outright (not flag-guarded); coordination is on the 12 typed-NULL SPEC rows (declarative), not deleted SQL. |

**Rollback:** the build is overwrite-idempotent and publish-last. To revert to the 131-col dataset,
re-run the previous (131-col) builder commit's `build_spawn` — it wipes-and-republishes the old width.
Because prod is byte-untouched on any pre-publish failure, an aborted 392 run needs no cleanup.

---

## 11. Success Criteria / Definition of Done (exact, verifiable)

1. `len(COLUMN_SPEC) == 392`; canonicals are unique; column ORDER = `[existing non-prov] + [261 adds] +
   [canonical_source, built_at]` (prov last).
2. Merge is 2-source: `print_merge_sql` shows NO `archive_proj` / `monthly_latest` / `archive_delta_D`
   / `bulk_base` / tombstone / reinstatement fragments.
3. Live dataset `usaspending_fpds_canonical_txn`: **392 columns**, **rows_out ≈ 107–108M+**, **18
   indices** (11 BTREE + 7 BITMAP), `pk_unique = true`, `canonical_source ∈ {fresh, bulk}` only.
4. Ledger: a new `status='success'` row with `id > baseline`, `columns=392`, `indices_built` length 18;
   `rows_in_archive_full = deletes_tombstoned = monthly_corrections_applied = 0` (truthful 2-source
   state).
5. `verify()` (Phase 6): all gates green (PK-unique, read-back INV pass, domain ⊆ {fresh, bulk}); the
   monthly-corrections gate is removed (not merely 0).
6. Dictionary regenerated with `--probe`, fail-closed-passing at `ncols=392`, indexed cols = 18.
7. The 12 monthly-unique cols present as typed-NULL (`feed_expr=None` AND `bulk_expr=None`), 100% NULL
   in the live dataset.
8. `schema_catalog` has a row for the canonical URI with `n_columns=392`.
9. Smoke-gate assertion bumped `75 → 392`; `verify()` monthly gate removed (blockers B1/B2 cleared).
10. PR squash-merged; operator's main checkout pulled current; `git log -1 --oneline` shows the merged
    commit.

---

## 12. Appendix — the FULL 261-row enumeration

Every column below: `group="enrich"`, `feed_expr=None` (BULK-native, pg-only enrichment). Independently
reconstructed from `COLUMN_SPEC` (131) + the 378-entry sidecar; reproduces the mandate's asserted set
exactly (261 rows; histogram 143 VARCHAR / 84 BOOLEAN / 13 BIGINT / 11 DOUBLE / 6 TIMESTAMP / 4 DATE;
sole tz col `ingested_at`; 0 collisions). Paste-ready Python is at
`docs/reference/fpds_obt_261_additions_snippet.py` (regenerate via Phase A §A.1).

| canonical | duck_type | group | bulk_expr | feed_expr |
|---|---|---|---|---|
| `a_76_fair_act_action` | VARCHAR | enrich | `s(a_76_fair_act_action)` | None |
| `a_76_fair_act_action_desc` | VARCHAR | enrich | `s(a_76_fair_act_action_desc)` | None |
| `action_type_description` | VARCHAR | enrich | `s(action_type_description)` | None |
| `afa_generated_unique` | VARCHAR | enrich | `s(afa_generated_unique)` | None |
| `agency_id` | VARCHAR | enrich | `s(agency_id)` | None |
| `airport_authority` | BOOLEAN | enrich | `airport_authority` | None |
| `alaskan_native_owned_corpo` | BOOLEAN | enrich | `alaskan_native_owned_corpo` | None |
| `alaskan_native_servicing_i` | BOOLEAN | enrich | `alaskan_native_servicing_i` | None |
| `american_indian_owned_busi` | BOOLEAN | enrich | `american_indian_owned_busi` | None |
| `asian_pacific_american_own` | BOOLEAN | enrich | `asian_pacific_american_own` | None |
| `award_certified_date` | DATE | enrich | `award_certified_date` | None |
| `award_date_signed` | DATE | enrich | `award_date_signed` | None |
| `award_fiscal_year` | BIGINT | enrich | `award_fiscal_year` | None |
| `award_update_date` | TIMESTAMP | enrich | `award_update_date` | None |
| `awarding_office_code` | VARCHAR | enrich | `s(awarding_office_code)` | None |
| `awarding_office_name` | VARCHAR | enrich | `s(awarding_office_name)` | None |
| `awarding_subtier_agency_name_raw` | VARCHAR | enrich | `s(awarding_subtier_agency_name_raw)` | None |
| `awarding_toptier_agency_id` | BIGINT | enrich | `awarding_toptier_agency_id` | None |
| `awarding_toptier_agency_name_raw` | VARCHAR | enrich | `s(awarding_toptier_agency_name_raw)` | None |
| `black_american_owned_busin` | BOOLEAN | enrich | `black_american_owned_busin` | None |
| `business_funds_ind_desc` | VARCHAR | enrich | `s(business_funds_ind_desc)` | None |
| `business_funds_indicator` | VARCHAR | enrich | `s(business_funds_indicator)` | None |
| `business_types_desc` | VARCHAR | enrich | `s(business_types_desc)` | None |
| `c1862_land_grant_college` | BOOLEAN | enrich | `c1862_land_grant_college` | None |
| `c1890_land_grant_college` | BOOLEAN | enrich | `c1890_land_grant_college` | None |
| `c1994_land_grant_college` | BOOLEAN | enrich | `c1994_land_grant_college` | None |
| `cfda_id` | BIGINT | enrich | `cfda_id` | None |
| `cfda_title` | VARCHAR | enrich | `s(cfda_title)` | None |
| `city_local_government` | BOOLEAN | enrich | `city_local_government` | None |
| `clinger_cohen_act_pla_desc` | VARCHAR | enrich | `s(clinger_cohen_act_pla_desc)` | None |
| `commercial_item_acqui_desc` | VARCHAR | enrich | `s(commercial_item_acqui_desc)` | None |
| `commercial_item_test_desc` | VARCHAR | enrich | `s(commercial_item_test_desc)` | None |
| `commercial_item_test_progr` | VARCHAR | enrich | `s(commercial_item_test_progr)` | None |
| `community_developed_corpor` | BOOLEAN | enrich | `community_developed_corpor` | None |
| `community_development_corp` | BOOLEAN | enrich | `community_development_corp` | None |
| `consolidated_contract_desc` | VARCHAR | enrich | `s(consolidated_contract_desc)` | None |
| `construction_wage_rat_desc` | VARCHAR | enrich | `s(construction_wage_rat_desc)` | None |
| `contingency_humanitar_desc` | VARCHAR | enrich | `s(contingency_humanitar_desc)` | None |
| `contingency_humanitarian_o` | VARCHAR | enrich | `s(contingency_humanitarian_o)` | None |
| `contract_award_type_desc` | VARCHAR | enrich | `s(contract_award_type_desc)` | None |
| `contract_bundling_descrip` | VARCHAR | enrich | `s(contract_bundling_descrip)` | None |
| `contract_financing_descrip` | VARCHAR | enrich | `s(contract_financing_descrip)` | None |
| `contracting_officers_desc` | VARCHAR | enrich | `s(contracting_officers_desc)` | None |
| `contracts` | BOOLEAN | enrich | `contracts` | None |
| `corporate_entity_not_tax_e` | BOOLEAN | enrich | `corporate_entity_not_tax_e` | None |
| `corporate_entity_tax_exemp` | BOOLEAN | enrich | `corporate_entity_tax_exemp` | None |
| `correction_delete_ind_desc` | VARCHAR | enrich | `s(correction_delete_ind_desc)` | None |
| `correction_delete_indicatr` | VARCHAR | enrich | `s(correction_delete_indicatr)` | None |
| `cost_accounting_stand_desc` | VARCHAR | enrich | `s(cost_accounting_stand_desc)` | None |
| `cost_accounting_standards` | VARCHAR | enrich | `s(cost_accounting_standards)` | None |
| `cost_or_pricing_data_desc` | VARCHAR | enrich | `s(cost_or_pricing_data_desc)` | None |
| `council_of_governments` | BOOLEAN | enrich | `council_of_governments` | None |
| `country_of_product_or_desc` | VARCHAR | enrich | `s(country_of_product_or_desc)` | None |
| `country_of_product_or_serv` | VARCHAR | enrich | `s(country_of_product_or_serv)` | None |
| `county_local_government` | BOOLEAN | enrich | `county_local_government` | None |
| `create_date` | TIMESTAMP | enrich | `create_date` | None |
| `detached_award_procurement_id` | BIGINT | enrich | `detached_award_procurement_id` | None |
| `dod_claimant_prog_cod_desc` | VARCHAR | enrich | `s(dod_claimant_prog_cod_desc)` | None |
| `domestic_or_foreign_e_desc` | VARCHAR | enrich | `s(domestic_or_foreign_e_desc)` | None |
| `domestic_shelter` | BOOLEAN | enrich | `domestic_shelter` | None |
| `dot_certified_disadvantage` | BOOLEAN | enrich | `dot_certified_disadvantage` | None |
| `economically_disadvantaged` | BOOLEAN | enrich | `economically_disadvantaged` | None |
| `educational_institution` | BOOLEAN | enrich | `educational_institution` | None |
| `emerging_small_business` | BOOLEAN | enrich | `emerging_small_business` | None |
| `epa_designated_produc_desc` | VARCHAR | enrich | `s(epa_designated_produc_desc)` | None |
| `epa_designated_product` | VARCHAR | enrich | `s(epa_designated_product)` | None |
| `etl_update_date` | TIMESTAMP | enrich | `etl_update_date` | None |
| `evaluated_preference` | VARCHAR | enrich | `s(evaluated_preference)` | None |
| `evaluated_preference_desc` | VARCHAR | enrich | `s(evaluated_preference_desc)` | None |
| `extent_compete_description` | VARCHAR | enrich | `s(extent_compete_description)` | None |
| `face_value_loan_guarantee` | DOUBLE | enrich | `face_value_loan_guarantee` | None |
| `fain` | VARCHAR | enrich | `s(fain)` | None |
| `fair_opportunity_limi_desc` | VARCHAR | enrich | `s(fair_opportunity_limi_desc)` | None |
| `fed_biz_opps` | VARCHAR | enrich | `s(fed_biz_opps)` | None |
| `fed_biz_opps_description` | VARCHAR | enrich | `s(fed_biz_opps_description)` | None |
| `federal_agency` | BOOLEAN | enrich | `federal_agency` | None |
| `federally_funded_research` | BOOLEAN | enrich | `federally_funded_research` | None |
| `fiscal_action_date` | DATE | enrich | `fiscal_action_date` | None |
| `for_profit_organization` | BOOLEAN | enrich | `for_profit_organization` | None |
| `foreign_funding` | VARCHAR | enrich | `s(foreign_funding)` | None |
| `foreign_funding_desc` | VARCHAR | enrich | `s(foreign_funding_desc)` | None |
| `foreign_government` | BOOLEAN | enrich | `foreign_government` | None |
| `foreign_owned_and_located` | BOOLEAN | enrich | `foreign_owned_and_located` | None |
| `foundation` | BOOLEAN | enrich | `foundation` | None |
| `funding_amount` | DOUBLE | enrich | `funding_amount` | None |
| `funding_opportunity_goals` | VARCHAR | enrich | `s(funding_opportunity_goals)` | None |
| `funding_opportunity_number` | VARCHAR | enrich | `s(funding_opportunity_number)` | None |
| `funding_subtier_agency_abbreviation` | VARCHAR | enrich | `s(funding_subtier_agency_abbreviation)` | None |
| `funding_subtier_agency_name_raw` | VARCHAR | enrich | `s(funding_subtier_agency_name_raw)` | None |
| `funding_toptier_agency_abbreviation` | VARCHAR | enrich | `s(funding_toptier_agency_abbreviation)` | None |
| `funding_toptier_agency_id` | BIGINT | enrich | `funding_toptier_agency_id` | None |
| `funding_toptier_agency_name_raw` | VARCHAR | enrich | `s(funding_toptier_agency_name_raw)` | None |
| `generated_pragmatic_obligation` | DOUBLE | enrich | `generated_pragmatic_obligation` | None |
| `government_furnished_desc` | VARCHAR | enrich | `s(government_furnished_desc)` | None |
| `government_furnished_prope` | VARCHAR | enrich | `s(government_furnished_prope)` | None |
| `grants` | BOOLEAN | enrich | `grants` | None |
| `hispanic_american_owned_bu` | BOOLEAN | enrich | `hispanic_american_owned_bu` | None |
| `hispanic_servicing_institu` | BOOLEAN | enrich | `hispanic_servicing_institu` | None |
| `historically_black_college` | BOOLEAN | enrich | `historically_black_college` | None |
| `hospital_flag` | BOOLEAN | enrich | `hospital_flag` | None |
| `housing_authorities_public` | BOOLEAN | enrich | `housing_authorities_public` | None |
| `idv_type_description` | VARCHAR | enrich | `s(idv_type_description)` | None |
| `indian_tribe_federally_rec` | BOOLEAN | enrich | `indian_tribe_federally_rec` | None |
| `indirect_federal_sharing` | DOUBLE | enrich | `indirect_federal_sharing` | None |
| `information_technolog_desc` | VARCHAR | enrich | `s(information_technolog_desc)` | None |
| `information_technology_com` | VARCHAR | enrich | `s(information_technology_com)` | None |
| `ingested_at` | TIMESTAMP | enrich | `CAST(ingested_at AS TIMESTAMP)` | None |
| `inherently_government_desc` | VARCHAR | enrich | `s(inherently_government_desc)` | None |
| `initial_report_date` | TIMESTAMP | enrich | `initial_report_date` | None |
| `inter_municipal_local_gove` | BOOLEAN | enrich | `inter_municipal_local_gove` | None |
| `interagency_contract_desc` | VARCHAR | enrich | `s(interagency_contract_desc)` | None |
| `interagency_contracting_au` | VARCHAR | enrich | `s(interagency_contracting_au)` | None |
| `international_organization` | BOOLEAN | enrich | `international_organization` | None |
| `interstate_entity` | BOOLEAN | enrich | `interstate_entity` | None |
| `is_fpds` | BOOLEAN | enrich | `is_fpds` | None |
| `joint_venture_economically` | BOOLEAN | enrich | `joint_venture_economically` | None |
| `joint_venture_women_owned` | BOOLEAN | enrich | `joint_venture_women_owned` | None |
| `labor_standards_descrip` | VARCHAR | enrich | `s(labor_standards_descrip)` | None |
| `labor_surplus_area_firm` | BOOLEAN | enrich | `labor_surplus_area_firm` | None |
| `legal_entity_address_line2` | VARCHAR | enrich | `s(legal_entity_address_line2)` | None |
| `legal_entity_address_line3` | VARCHAR | enrich | `s(legal_entity_address_line3)` | None |
| `legal_entity_city_code` | VARCHAR | enrich | `s(legal_entity_city_code)` | None |
| `legal_entity_foreign_city` | VARCHAR | enrich | `s(legal_entity_foreign_city)` | None |
| `legal_entity_foreign_descr` | VARCHAR | enrich | `s(legal_entity_foreign_descr)` | None |
| `legal_entity_foreign_posta` | VARCHAR | enrich | `s(legal_entity_foreign_posta)` | None |
| `legal_entity_foreign_provi` | VARCHAR | enrich | `s(legal_entity_foreign_provi)` | None |
| `legal_entity_zip4` | VARCHAR | enrich | `s(legal_entity_zip4)` | None |
| `legal_entity_zip_last4` | VARCHAR | enrich | `s(legal_entity_zip_last4)` | None |
| `limited_liability_corporat` | BOOLEAN | enrich | `limited_liability_corporat` | None |
| `local_area_set_aside` | VARCHAR | enrich | `s(local_area_set_aside)` | None |
| `local_area_set_aside_desc` | VARCHAR | enrich | `s(local_area_set_aside_desc)` | None |
| `local_government_owned` | BOOLEAN | enrich | `local_government_owned` | None |
| `manufacturer_of_goods` | BOOLEAN | enrich | `manufacturer_of_goods` | None |
| `materials_supplies_article` | VARCHAR | enrich | `s(materials_supplies_article)` | None |
| `materials_supplies_descrip` | VARCHAR | enrich | `s(materials_supplies_descrip)` | None |
| `minority_institution` | BOOLEAN | enrich | `minority_institution` | None |
| `minority_owned_business` | BOOLEAN | enrich | `minority_owned_business` | None |
| `multi_year_contract_desc` | VARCHAR | enrich | `s(multi_year_contract_desc)` | None |
| `multiple_or_single_aw_desc` | VARCHAR | enrich | `s(multiple_or_single_aw_desc)` | None |
| `municipality_local_governm` | BOOLEAN | enrich | `municipality_local_governm` | None |
| `national_interest_desc` | VARCHAR | enrich | `s(national_interest_desc)` | None |
| `native_american_owned_busi` | BOOLEAN | enrich | `native_american_owned_busi` | None |
| `native_hawaiian_owned_busi` | BOOLEAN | enrich | `native_hawaiian_owned_busi` | None |
| `native_hawaiian_servicing` | BOOLEAN | enrich | `native_hawaiian_servicing` | None |
| `non_federal_funding_amount` | DOUBLE | enrich | `non_federal_funding_amount` | None |
| `nonprofit_organization` | BOOLEAN | enrich | `nonprofit_organization` | None |
| `officer_1_amount` | DOUBLE | enrich | `officer_1_amount` | None |
| `officer_1_name` | VARCHAR | enrich | `s(officer_1_name)` | None |
| `officer_2_amount` | DOUBLE | enrich | `officer_2_amount` | None |
| `officer_2_name` | VARCHAR | enrich | `s(officer_2_name)` | None |
| `officer_3_amount` | DOUBLE | enrich | `officer_3_amount` | None |
| `officer_3_name` | VARCHAR | enrich | `s(officer_3_name)` | None |
| `officer_4_amount` | DOUBLE | enrich | `officer_4_amount` | None |
| `officer_4_name` | VARCHAR | enrich | `s(officer_4_name)` | None |
| `officer_5_amount` | DOUBLE | enrich | `officer_5_amount` | None |
| `officer_5_name` | VARCHAR | enrich | `s(officer_5_name)` | None |
| `organizational_type` | VARCHAR | enrich | `s(organizational_type)` | None |
| `original_loan_subsidy_cost` | DOUBLE | enrich | `original_loan_subsidy_cost` | None |
| `other_minority_owned_busin` | BOOLEAN | enrich | `other_minority_owned_busin` | None |
| `other_not_for_profit_organ` | BOOLEAN | enrich | `other_not_for_profit_organ` | None |
| `other_statutory_authority` | VARCHAR | enrich | `s(other_statutory_authority)` | None |
| `other_than_full_and_o_desc` | VARCHAR | enrich | `s(other_than_full_and_o_desc)` | None |
| `parent_recipient_name_raw` | VARCHAR | enrich | `s(parent_recipient_name_raw)` | None |
| `parent_recipient_unique_id` | VARCHAR | enrich | `s(parent_recipient_unique_id)` | None |
| `partnership_or_limited_lia` | BOOLEAN | enrich | `partnership_or_limited_lia` | None |
| `performance_based_se_desc` | VARCHAR | enrich | `s(performance_based_se_desc)` | None |
| `period_of_perf_potential_e` | VARCHAR | enrich | `s(period_of_perf_potential_e)` | None |
| `place_of_manufacture` | VARCHAR | enrich | `s(place_of_manufacture)` | None |
| `place_of_manufacture_desc` | VARCHAR | enrich | `s(place_of_manufacture_desc)` | None |
| `place_of_perform_zip_last4` | VARCHAR | enrich | `s(place_of_perform_zip_last4)` | None |
| `place_of_performance_zip4a` | VARCHAR | enrich | `s(place_of_performance_zip4a)` | None |
| `planning_commission` | BOOLEAN | enrich | `planning_commission` | None |
| `pop_congressional_code_current` | VARCHAR | enrich | `s(pop_congressional_code_current)` | None |
| `pop_congressional_population` | BIGINT | enrich | `pop_congressional_population` | None |
| `pop_country_name` | VARCHAR | enrich | `s(pop_country_name)` | None |
| `pop_county_code` | VARCHAR | enrich | `s(pop_county_code)` | None |
| `pop_county_name` | VARCHAR | enrich | `s(pop_county_name)` | None |
| `pop_county_population` | BIGINT | enrich | `pop_county_population` | None |
| `pop_state_fips` | VARCHAR | enrich | `s(pop_state_fips)` | None |
| `pop_state_name` | VARCHAR | enrich | `s(pop_state_name)` | None |
| `pop_state_population` | BIGINT | enrich | `pop_state_population` | None |
| `port_authority` | BOOLEAN | enrich | `port_authority` | None |
| `potential_total_value_awar` | VARCHAR | enrich | `s(potential_total_value_awar)` | None |
| `private_university_or_coll` | BOOLEAN | enrich | `private_university_or_coll` | None |
| `program_system_or_equ_desc` | VARCHAR | enrich | `s(program_system_or_equ_desc)` | None |
| `program_system_or_equipmen` | VARCHAR | enrich | `s(program_system_or_equipmen)` | None |
| `published_fabs_id` | BIGINT | enrich | `published_fabs_id` | None |
| `pulled_from` | VARCHAR | enrich | `s(pulled_from)` | None |
| `purchase_card_as_paym_desc` | VARCHAR | enrich | `s(purchase_card_as_paym_desc)` | None |
| `receives_contracts_and_gra` | BOOLEAN | enrich | `receives_contracts_and_gra` | None |
| `recipient_location_congressional_code_current` | VARCHAR | enrich | `s(recipient_location_congressional_code_current)` | None |
| `recipient_location_congressional_population` | BIGINT | enrich | `recipient_location_congressional_population` | None |
| `recipient_location_country_name` | VARCHAR | enrich | `s(recipient_location_country_name)` | None |
| `recipient_location_county_code` | VARCHAR | enrich | `s(recipient_location_county_code)` | None |
| `recipient_location_county_population` | BIGINT | enrich | `recipient_location_county_population` | None |
| `recipient_location_state_fips` | VARCHAR | enrich | `s(recipient_location_state_fips)` | None |
| `recipient_location_state_name` | VARCHAR | enrich | `s(recipient_location_state_name)` | None |
| `recipient_location_state_population` | BIGINT | enrich | `recipient_location_state_population` | None |
| `recipient_name_raw` | VARCHAR | enrich | `s(recipient_name_raw)` | None |
| `recipient_unique_id` | VARCHAR | enrich | `s(recipient_unique_id)` | None |
| `record_type` | BIGINT | enrich | `record_type` | None |
| `record_type_description` | VARCHAR | enrich | `s(record_type_description)` | None |
| `recovered_materials_s_desc` | VARCHAR | enrich | `s(recovered_materials_s_desc)` | None |
| `recovered_materials_sustai` | VARCHAR | enrich | `s(recovered_materials_sustai)` | None |
| `referenced_idv_agency_desc` | VARCHAR | enrich | `s(referenced_idv_agency_desc)` | None |
| `referenced_idv_type_desc` | VARCHAR | enrich | `s(referenced_idv_type_desc)` | None |
| `referenced_mult_or_si_desc` | VARCHAR | enrich | `s(referenced_mult_or_si_desc)` | None |
| `referenced_mult_or_single` | VARCHAR | enrich | `s(referenced_mult_or_single)` | None |
| `research` | VARCHAR | enrich | `s(research)` | None |
| `research_description` | VARCHAR | enrich | `s(research_description)` | None |
| `sai_number` | VARCHAR | enrich | `s(sai_number)` | None |
| `sam_exception` | VARCHAR | enrich | `s(sam_exception)` | None |
| `sam_exception_description` | VARCHAR | enrich | `s(sam_exception_description)` | None |
| `sba_certified_8_a_joint_ve` | BOOLEAN | enrich | `sba_certified_8_a_joint_ve` | None |
| `school_district_local_gove` | BOOLEAN | enrich | `school_district_local_gove` | None |
| `school_of_forestry` | BOOLEAN | enrich | `school_of_forestry` | None |
| `sea_transportation` | VARCHAR | enrich | `s(sea_transportation)` | None |
| `sea_transportation_desc` | VARCHAR | enrich | `s(sea_transportation_desc)` | None |
| `self_certified_small_disad` | BOOLEAN | enrich | `self_certified_small_disad` | None |
| `small_agricultural_coopera` | BOOLEAN | enrich | `small_agricultural_coopera` | None |
| `small_business_competitive` | BOOLEAN | enrich | `small_business_competitive` | None |
| `small_disadvantaged_busine` | BOOLEAN | enrich | `small_disadvantaged_busine` | None |
| `sole_proprietorship` | BOOLEAN | enrich | `sole_proprietorship` | None |
| `solicitation_procedur_desc` | VARCHAR | enrich | `s(solicitation_procedur_desc)` | None |
| `source_schema` | VARCHAR | enrich | `s(source_schema)` | None |
| `source_table` | VARCHAR | enrich | `s(source_table)` | None |
| `state_controlled_instituti` | BOOLEAN | enrich | `state_controlled_instituti` | None |
| `subchapter_s_corporation` | BOOLEAN | enrich | `subchapter_s_corporation` | None |
| `subcontinent_asian_asian_i` | BOOLEAN | enrich | `subcontinent_asian_asian_i` | None |
| `subcontracting_plan_desc` | VARCHAR | enrich | `s(subcontracting_plan_desc)` | None |
| `tas_components` | VARCHAR | enrich | `s(tas_components)` | None |
| `the_ability_one_program` | BOOLEAN | enrich | `the_ability_one_program` | None |
| `township_local_government` | BOOLEAN | enrich | `township_local_government` | None |
| `transit_authority` | BOOLEAN | enrich | `transit_authority` | None |
| `tribal_college` | BOOLEAN | enrich | `tribal_college` | None |
| `tribally_owned_business` | BOOLEAN | enrich | `tribally_owned_business` | None |
| `type` | VARCHAR | enrich | `s(type)` | None |
| `type_description` | VARCHAR | enrich | `s(type_description)` | None |
| `type_description_raw` | VARCHAR | enrich | `s(type_description_raw)` | None |
| `type_of_contract_pric_desc` | VARCHAR | enrich | `s(type_of_contract_pric_desc)` | None |
| `type_of_idc` | VARCHAR | enrich | `s(type_of_idc)` | None |
| `type_of_idc_description` | VARCHAR | enrich | `s(type_of_idc_description)` | None |
| `type_raw` | VARCHAR | enrich | `s(type_raw)` | None |
| `type_set_aside_description` | VARCHAR | enrich | `s(type_set_aside_description)` | None |
| `undefinitized_action_desc` | VARCHAR | enrich | `s(undefinitized_action_desc)` | None |
| `update_date` | TIMESTAMP | enrich | `update_date` | None |
| `uri` | VARCHAR | enrich | `s(uri)` | None |
| `us_federal_government` | BOOLEAN | enrich | `us_federal_government` | None |
| `us_government_entity` | BOOLEAN | enrich | `us_government_entity` | None |
| `us_local_government` | BOOLEAN | enrich | `us_local_government` | None |
| `us_state_government` | BOOLEAN | enrich | `us_state_government` | None |
| `us_tribal_government` | BOOLEAN | enrich | `us_tribal_government` | None |
| `usaspending_snapshot_date` | DATE | enrich | `usaspending_snapshot_date` | None |
| `usaspending_unique_transaction_id` | VARCHAR | enrich | `s(usaspending_unique_transaction_id)` | None |
| `vendor_doing_as_business_n` | VARCHAR | enrich | `s(vendor_doing_as_business_n)` | None |
| `vendor_fax_number` | VARCHAR | enrich | `s(vendor_fax_number)` | None |
| `vendor_phone_number` | VARCHAR | enrich | `s(vendor_phone_number)` | None |
| `veteran_owned_business` | BOOLEAN | enrich | `veteran_owned_business` | None |
| `veterinary_college` | BOOLEAN | enrich | `veterinary_college` | None |
| `veterinary_hospital` | BOOLEAN | enrich | `veterinary_hospital` | None |
| `woman_owned_business` | BOOLEAN | enrich | `woman_owned_business` | None |

---

**End of plan.** Reconstruction verified against on-disk sources (`COLUMN_SPEC` 131 + sidecar 378 → 261 adds → 392 total). All commands literal. All line numbers against main @ `5ddd960`.
