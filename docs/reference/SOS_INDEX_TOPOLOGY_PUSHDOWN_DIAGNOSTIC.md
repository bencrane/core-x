# SoS Corporate Registry — Index Topology & Predicate-Pushdown Diagnostic

Read-only, mathematically-grounded interrogation of the **live R2-backed Lance system of
record** for the state-level Secretary-of-State corporate registry — the exact index
manifest committed to each dataset, the trained-row truth of every index, the
high-cardinality entity/agent resolution-key audit, and an empirical query-planner trace
proving how the engine executes raw vs. normalization-macro vs. flat-materialized predicates.

- **Targets interrogated (Gen-3 SoR):** the directive's guessed single `active/sos_master/`
  **does not exist.** The SoS corporate registry is a **state-by-state spine architecture
  with one unified normalization layer on top** (8 datasets):
  - `s3://data-sink/active/sos_normalized_master/` — Lance **v4**, **17,926,543 rows**, 18 frags, 11 cols. *(the cross-state entity-resolution layer)*
  - `s3://data-sink/active/ca_sos_entities/`   — **v12**, **9,389,688 rows**, 9 frags, 41 cols.
  - `s3://data-sink/active/ca_sos_agents/`      — **v5**,  **8,560,095 rows**, 9 frags, 18 cols.
  - `s3://data-sink/active/ca_sos_principals/`  — **v6**,  **18,670,722 rows**, 18 frags, 18 cols.
  - `s3://data-sink/active/ny_sos/`             — **v10**, **4,219,360 rows**, 5 frags, 33 cols.
  - `s3://data-sink/active/fl_sos_corporations/`— **v5**,  **1,260,599 rows**, 2 frags, 29 cols.
  - `s3://data-sink/active/fl_sos_events/`      — **v3**,  **14,455,118 rows**, 14 frags, 9 cols.
  - `s3://data-sink/active/co_sos/`             — **v8**,  **3,056,896 rows**, 3 frags, 38 cols.
  - Downstream consumer probed for join-key confirmation: `active/epa_to_sos_bridge/` (v3, 356,903 rows, BTREE on `normalized_legal_name`).
- **Live evidence harness (non-mutating, zero writes):** `pylance 7.0.0` direct reads —
  `boto3 list_objects_v2` (dataset discovery) · `dataset.list_indices()` (manifest) ·
  `dataset.stats.index_stats()` (BTREE/BITMAP + trained-row truth) ·
  `LanceScanner.explain_plan(verbose=True)` / `analyze_plan()` (physical plan + real
  `rows_scanned`/`bytes_read`) · `count_rows(filter=…)`. DuckDB 1.5.3 for cardinality
  (`approx_count_distinct`, full streamed scan) and materialized-key fidelity. The macro under
  test is imported verbatim from `core.name_norm.name_norm` — the single-source-of-truth blocking rule.
- **As-of:** probed 2026-06-05 against the committed Lance versions above. **No DDL, no index
  build, no `.lance` write, no delete.** Every figure is a live read of the committed dataset.

---

## 0. Headline posture — index topology is pristine; the materialized key is **one macro-revision stale**

The directive hunts three pathologies. Two are absent. The third (the SBA/Directive-29
materialization) is *deployed in mechanism but stale in value* — a failure mode the SBA
datasets did not have.

| Verdict | Detail |
|---|---|
| **FEC Trap (committed-but-untrained indices)** | ✅ **ABSENT on all 8 datasets.** Every one of the **45 committed indices** reports `num_unindexed_rows = 0` and `num_indexed_rows = full table cardinality`. Zero dead indices. No `replace=True` retrain required anywhere. |
| **Identifiers / name keys** | ✅ Every entity PK / file-number is BTree+trained and emits `ScalarIndexQuery` (`entity_num`, `entity_id`, `dos_id`, `document_number`, `last_si_file_number`). Every corporate-**name** key is BTree (`normalized_legal_name`, `corporate_name`, `current_entity_name`, `co.entity_name`, `ca.entity_name_clean`). |
| **MSHA Trap (missing index on high-card resolution string)** | ⚠️ **One genuine instance: `fl_sos_corporations.registered_agent_name`** — 968,025 distinct (76.8%), **unindexed** → point-lookups full-scan 1.26 M rows. NY (`registered_agent_name`, BTree) and CO (`agent_organization_name`, BTree) indexed their agent keys; **FL did not.** Secondary: `fl.principal_state` carries no BITMAP; `master.entity_status` carries no BITMAP. |
| **SBA Remediation (write-time normalized-name materialization)** | 🟡 **Mechanism deployed, values stale.** `sos_normalized_master.normalized_legal_name` is a stored, BTREE-indexed column and is the **only** predicate form that emits `ScalarIndexQuery` (Test C). **But** it was materialized by a macro that **predates** the current `core.name_norm` `&`/dash fix. **8.036% of stored keys (1,440,646 rows) disagree with the current canonical macro**; **100.000%** of that gap is the old `&`/dash rule (0 residual). **1,367,567 distinct current-macro keys (8.12%) have NO matching row** in the BTREE. |
| **`legal_name_base` secondary key** | 🛑 **Declared in code, absent in the SoR.** `pipelines/sos_normalized/normalize.py` projects `legal_name_base` and declares a mandatory BTREE on it; the live v4 master has **11 columns (no `legal_name_base`) and no such index.** Neither column nor index exists. |
| **Test B — `name_norm(col)` in the WHERE clause** | 🛑 **Never indexed — structurally.** `func(col)=lit` discards the scalar index and full-scans all 17.93 M rows on every dataset. |
| **Test C — flat `normalized_legal_name = '…'`** | ✅ **`ScalarIndexQuery@normalized_legal_name_idx(BTree)`** — reads **1** row / 76 KB vs. B's 17.93 M rows / 343 MB. |

**Bottom line.** The SoS topology is structurally optimal — no dead indices, no missing
corporate-name indices, the materialized-key *mechanism* in place. It is **semantically
stale**: the system-of-record `normalized_legal_name` holds the *pre-fix* normalization, so
every spine/bridge that computes `name_norm` under today's canonical rule and exact-joins the
master **silently drops ~8.12% of entities** — precisely the conjunction-named (`X & Y`) and
hyphenated (`COCA-COLA`, `33-23`) ones. **Unlike SBA, the corrective is NOT query routing —
it is a write-time re-materialization.** The defect is in the data plane's values, not the
query path.

---

## 1. Index manifest — exact, from `dataset.list_indices()` + `stats.index_stats()`

All **45** indices below report `num_unindexed_rows = 0` and `num_indexed_rows = total
cardinality` → **HEALTHY / fully trained.** (Contrast FEC, whose 6 BTREEs were `indexed_rows=0`.)

| Dataset | v | rows | frags | #idx | BTREE fields | BITMAP fields |
|---|--:|--:|--:|--:|---|---|
| **sos_normalized_master** | 4 | 17,926,543 | 18 | **3** | `normalized_legal_name`, `zip_code` | `source_state` |
| **ca_sos_entities** | 12 | 9,389,688 | 9 | **11** | `entity_num`, `entity_name_clean`, `last_si_file_number` | `entity_type`, `entity_status`, `jurisdiction`, `filing_type`, `standing_sos`, `standing_ftb`, `principal_state`, `mailing_state` |
| **ca_sos_agents** | 5 | 8,560,095 | 9 | **4** | `entity_num`, `entity_name_clean` | `agent_type`, `physical_state` |
| **ca_sos_principals** | 6 | 18,670,722 | 18 | **5** | `entity_num`, `entity_name_clean`, `last_name` | `position_type`, `state` |
| **ny_sos** | 10 | 4,219,360 | 5 | **9** | `dos_id`, `current_entity_name`, `initial_dos_filing_date`, `dos_process_name`, `registered_agent_name` | `entity_type`, `county`, `jurisdiction`, `dos_process_state` |
| **fl_sos_corporations** | 5 | 1,260,599 | 2 | **4** | `document_number`, `corporate_name` | `status`, `filing_type` |
| **fl_sos_events** | 3 | 14,455,118 | 14 | **2** | `document_number` | `event_code` |
| **co_sos** | 8 | 3,056,896 | 3 | **7** | `entity_id`, `entity_name`, `jurisdiction_of_formation`, `agent_organization_name` | `entity_status`, `entity_type`, `principal_state` |

> Every (field, index) pair above was confirmed three ways: `index_stats` reports full
> `num_indexed_rows`, `explain_plan` emits the expected node, `analyze_plan` reads only the
> matched rows. There is no pylance reporting quirk; the indices are real and trained.
> **Low-cardinality categoricals carry BITMAP** (`entity_status`, `entity_type`, `status`,
> `jurisdiction`, `*_state`) and **high-cardinality resolution strings carry BTREE** — the
> declared design rule holds across the fleet, with the single `fl.registered_agent_name`
> exception in §2.

**Architecture note.** The per-state spines (CA/NY/FL/CO) index their **native** name forms
(`entity_name_clean`, `current_entity_name`, `corporate_name`, `entity_name`) — they carry
**no** `normalized_legal_name`. Cross-state entity resolution is centralized in
`sos_normalized_master`, whose `normalized_legal_name` BTREE is the one canonical blocking key
spanning all four states. `master.original_entity_id` is carried for provenance and is
intentionally unindexed (the master's join key is the name, not the per-state id).

---

## 2. Resolution-key audit — cardinality vs. index coverage (the MSHA-Trap analysis)

`approx_count_distinct`, full streamed scan. A key is a genuine high-cardinality resolution
string (MSHA-Trap candidate) when distinct ≈ row count.

| Dataset | Key | Distinct | nonnull | % rows | Indexed? | Verdict |
|---|---|--:|--:|--:|---|---|
| sos_normalized_master | `normalized_legal_name` | 17,262,596 | 17,926,539 | **96.3%** | ✅ BTree | **canonical name key — indexed** (but stale, §3.4) |
| sos_normalized_master | `source_entity_name` (raw) | 16,848,579 | 17,926,540 | 94.0% | 🛑 none | raw provenance; not an access path |
| ca_sos_entities | `entity_name_clean` | 8,848,539 | 9,389,686 | 94.2% | ✅ BTree | indexed |
| ny_sos | `current_entity_name` | 3,867,870 | 4,219,360 | 91.7% | ✅ BTree | indexed |
| co_sos | `entity_name` | 2,940,132 | 3,056,895 | 96.2% | ✅ BTree | indexed |
| fl_sos_corporations | `corporate_name` | 1,120,291 | 1,260,599 | 88.9% | ✅ BTree | indexed |
| **fl_sos_corporations** | **`registered_agent_name`** | **968,025** | 1,227,038 | **76.8%** | 🛑 **none** | **MSHA gap — high-card agent key, unindexed** |
| ny_sos | `registered_agent_name` | 370,239 | 856,624 | 8.8%¹ | ✅ BTree | agent key indexed |
| co_sos | `agent_organization_name` | 176,937 | 691,525 | 5.8%¹ | ✅ BTree | agent-org key indexed |

¹ % of total rows; NY/CO agent columns are sparsely populated (856 K / 691 K non-null) but
still high-cardinality among populated rows (370 K / 177 K distinct) — and **both are indexed.**

**The entity/agent verdict.** Corporate-**name** resolution is fully indexed on every dataset.
**Agent** resolution is indexed on NY (`registered_agent_name`) and CO (`agent_organization_name`)
— and **missing on FL** (`registered_agent_name`, 968 K distinct, unindexed). CA models agents
as a separate dataset (`ca_sos_agents`) joined by `entity_num`/`entity_name_clean` (the
represented entity), so it has no agent-own-name index by design. **`fl.registered_agent_name`
is the lone high-cardinality resolution string in the corpus that lacks a BTREE.**

---

## 3. Query-planner diagnostic — physical plans (bare identifiers)

Real values sampled off the live columns. **A** = raw name; **B** = the canonical
`name_norm()` macro wrapping the raw column; **C** = the flat materialized
`normalized_legal_name`. `analyze_plan()` executed; figures are the engine's own metrics.
Wall is a single-shot read from a **non-in-region** client — the deterministic,
location-independent proof is `rows_scanned` / `bytes_read`.

| Dataset | Test | Predicate | `ScalarIndexQuery`? | `rows_scanned` | `bytes_read` | matched | wall |
|---|---|---|---|--:|--:|--:|--:|
| **sos_normalized_master** | A | `source_entity_name = 'GRACE SEAFOOD CORP.'` | 🛑 | **17.93 M** | 343.3 M | 1 | 5.23 s |
| | B | `name_norm(source_entity_name) = 'GRACE SEAFOOD CORP'` | 🛑 | **17.93 M** | 343.3 M | 1 | 12.38 s |
| | C | `normalized_legal_name = 'GRACE SEAFOOD CORP'` | ✅ | **1** | 76.2 K | 1 | 1.41 s² |
| **fl_sos_corporations** | — | `corporate_name = 'FAITH TEMPLE CHURCH, INC.'` (BTree) | ✅ | **1** | 83.6 K | 1 | 0.66 s |
| | — | `registered_agent_name = 'CORPORATION INFORMATION SERVICES INC.'` (no index) | 🛑 | **1.26 M** | 21.1 M | 398 | 0.69 s |
| **ny_sos** | — | `registered_agent_name = 'CORPORATE CREATIONS NETWORK INC.'` (BTree) | ✅ | **4.74 K**³ | 16.1 M | 4,743 | 7.06 s² |

² Cold `analyze_plan` includes the first index-page load from non-in-region R2
(`search_time` 1.13 s / 6.02 s); the warm count-only path for Master-C was **0.005 s**. The
location-independent figures are `rows_scanned` (17.93 M → **1**) and `bytes_read`
(343.3 M → **76 KB**, a **~4,500× collapse**).
³ NY agent is served by `ScalarIndexQuery`; `rows_scanned = 4,743` is the **matched-row take**
(the 4,743 corps this registered agent represents), **not** a table scan. An *un*indexed NY
agent lookup would scan all 4,219,360 rows.

### 3.1 Indexed path (Master Test C) — `analyze_plan()` excerpt

```
ScalarIndexQuery: query=[normalized_legal_name = GRACE SEAFOOD CORP]
                  @normalized_legal_name_idx(BTree), index_comparisons=8.19 K
LanceRead: full_filter=normalized_legal_name = Utf8("GRACE SEAFOOD CORP"), refine_filter=--,
           fragments_scanned=1, rows_scanned=1, bytes_read=76.24 K     ← reads only the matched row
```

### 3.2 Macro path (Master Test B) — `analyze_plan()` excerpt

```
LanceRead: num_fragments=18, row_id=true,
  full_filter=nullif(btrim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
    upper(CAST(source_entity_name AS Utf8)),'&',' AND ','g'),'[-\x{2013}\x{2014}]+',' ','g'),
    '[^A-Z0-9 ]+','','g'),'\s+',' ','g')),'') = Utf8("GRACE SEAFOOD CORP"),
  refine_filter=<same>, fragments_scanned=18, rows_scanned=17.93 M, bytes_read=343.3 M  ← full column scan
```

The macro **parses** in Lance's DataFusion filter (4-arg `regexp_replace`, `upper`, `nullif`,
`btrim` all bind) but resolves to **no** scalar index — a function-wrapped column is
structurally non-indexable. Identical logical result to Test C (1 row), **17.93 M× more rows read.**

### 3.3 The FL agent MSHA gap, made physical

`corporate_name` (BTree) → `ScalarIndexQuery`, 1 row / 83.6 KB. `registered_agent_name`
(no index) → `refine_filter` full scan, **1.26 M rows / 21.1 MB** to return 398 matches — a
**1.26 M× row-scan amplification** and ~253× the bytes, on the same dataset, for a key with
968 K distinct values. This is the MSHA Trap in a single contrast.

### 3.4 The stale-materialization finding — `normalized_legal_name` ≠ current `name_norm`

The directive's "is the SBA Remediation deployed" question. The materialized column exists and
is the only `ScalarIndexQuery` path — but its **values predate the current canonical macro.**
Full-scan comparison of stored `normalized_legal_name` against
`core.name_norm(source_entity_name)` recomputed under today's rule:

| source_state | rows | mismatches | mismatch % |
|---|--:|--:|--:|
| CA | 9,389,688 | 743,175 | 7.915% |
| CO | 3,056,896 | 210,907 | 6.899% |
| FL | 1,260,599 | 123,149 | 9.769% |
| NY | 4,219,360 | 363,415 | 8.613% |
| **ALL** | **17,926,543** | **1,440,646** | **8.036%** |

Root cause is **pinned, not inferred.** Reconstructing the *pre-fix* rule (strip `&` and `-`
as plain punctuation — no `&`→` AND `, no dash→space) and comparing to the stored column:

```
stored != CURRENT core.name_norm        : 1,440,646  (8.036%)
stored != OLD-rule reconstruction       :         0  (0.0000%)
  → 100.000% of the gap is stored == OLD-rule    (0 residual rows)
distinct CURRENT keys absent from stored BTREE : 1,367,567  (8.12%)
```

Every stored value is **byte-exact** to the old macro; the two rules that moved (matching the
fix documented verbatim in `core/name_norm.py`):

| raw | **stored (old)** | **current `name_norm`** | rule |
|---|---|---|---|
| `HERMAN & ROOF, P.A.` | `HERMAN ROOF PA` | `HERMAN AND ROOF PA` | `&` → (dropped) → ` AND ` |
| `M&E FAMILY HOLDINGS LLC` | `ME FAMILY HOLDINGS LLC` | `M AND E FAMILY HOLDINGS LLC` | `&` → (dropped) → ` AND ` |
| `THE GREELEY COCA-COLA BOTTLING COMPANY` | `…COCACOLA…` | `…COCA COLA…` | dash → (concatenated) → ` ` |
| `33-23 STEUBEN AVENUE CORP.` | `3323 STEUBEN AVENUE CORP` | `33 23 STEUBEN AVENUE CORP` | dash → (concatenated) → ` ` |

**Consequence.** The BTREE is healthy and the pushdown is perfect — but a joiner computing the
*current* `name_norm` and probing `WHERE normalized_legal_name = name_norm('BARNES & NOBLE')`
(= `'BARNES AND NOBLE'`) finds **0 rows**, because the stored key is `'BARNES NOBLE'`. This is
exactly the silent cross-layer-join break `core/name_norm.py`'s own docstring warns about,
realized against **1,367,567 distinct keys (8.12%)** — every conjunction-named and hyphenated
entity in the registry.

---

## 4. Structural verdict

| Question (directive) | Answer (measured) |
|---|---|
| Any index with `indexed_rows = 0` (FEC Trap)? | **No — zero across all 45 indices.** Every index `indexed_rows = full cardinality`, `unindexed_rows = 0`. |
| Do entity-name / agent-name / id keys have trained BTREEs? | **Names: yes, universally.** **IDs: yes** (`entity_num`/`entity_id`/`dos_id`/`document_number`). **Agents: NY ✅, CO ✅, FL 🛑** (`fl.registered_agent_name`, 968 K distinct, unindexed). |
| Low-card categoricals carry BITMAP? | **Yes** (`entity_status`/`entity_type`/`status`/`jurisdiction`/`*_state`). Gaps: `fl.principal_state` and `master.entity_status` carry no BITMAP. |
| Is `normalized_legal_name` materialized (SBA Remediation)? | **Yes — stored + BTREE, the only `ScalarIndexQuery` path. But the values are one macro-revision stale (8.036% drift; 8.12% distinct-key join-loss).** The declared `legal_name_base` key is **absent** (neither column nor index). |
| Test A vs B vs C — which emits `ScalarIndexQuery`? | **A** indexed on the spines' native name keys; **on the master `source_entity_name` is unindexed (raw).** **B** never (macro). **C** always (flat `normalized_legal_name`). |
| Exact rows-scanned differential (B → C)? | **17,926,543 → 1** (343.3 M → 76 KB bytes). FL agent (no index) → corporate (BTree): **1,260,599 → 1.** |

### 4.1 Precise architectural remediation

1. **Re-materialize `sos_normalized_master` — the load-bearing fix (write-time, not read-time).**
   Re-run `pipelines/sos_normalized/normalize.py` (mode=overwrite). It already imports the
   **current** `core.name_norm` and projects `legal_name_base`, so a single rebuild
   simultaneously (a) refreshes `normalized_legal_name` to today's `&`/dash rule — closing the
   **8.12% silent join-loss** — and (b) adds the missing **`legal_name_base`** column + its
   declared BTREE. This is the opposite of the SBA conclusion: there, query routing sufficed;
   **here the stored values are wrong and must be rewritten.** Until then, any
   `normalized_legal_name` exact-join against the master is lossy for conjunction/hyphenated names.
2. **Add `BTREE` on `fl_sos_corporations.registered_agent_name`** — the lone MSHA gap. The
   column exists; add it to `pipelines/fl_sos/sunbiz.py::INDEX_PLAN["master"]["btree"]`
   (`["document_number", "corporate_name", "registered_agent_name"]`) and re-index. Brings FL
   agent resolution to parity with NY/CO. *(Iff FL registered-agent lookups are exercised.)*
3. **Retrain dead indices (`replace=True`)?** — **Not required.** Zero dead indices; every
   index version post-dates its dataset's final write, so all fragments are folded in.
4. **Strict query routing (necessary, not sufficient).** Resolution queries must target the
   flat `normalized_legal_name`, never `name_norm(col)` wrappers (Test B is structurally
   non-indexable) and never raw `source_entity_name` (unindexed on the master). On the spines,
   route to the native indexed key (`entity_name_clean` / `corporate_name` /
   `current_entity_name` / `entity_name`). Routing alone does **not** fix the staleness in §1.
5. **Optional BITMAP additions** (no DDL authorized by this read-only probe; for the write-side
   owner): `fl.principal_state`, `master.entity_status` — iff those filters are hot.

**Net.** The SoS index topology is structurally optimal — no FEC trap, near-total MSHA
coverage, materialization mechanism in place. The actionable defects are **(1) a stale
system-of-record `normalized_legal_name` (re-materialize)**, **(2) a never-built
`legal_name_base` key (same rebuild closes it)**, and **(3) one missing FL agent BTREE.** A
Directive-29 override is **not** needed — the flat normalized column it would authorize already
exists; it simply needs to be rewritten with the current canonical macro.

---

## 5. Reproduction (read-only)

```
# pylance 7.0.0 / duckdb 1.5.3 / pyarrow 24; R2 creds via Doppler core-x/prd
doppler run --project core-x --config prd -- python diag1_manifest.py    # §1 discovery + manifest + trained-row truth (FEC scan)
doppler run --project core-x --config prd -- python diag1b_summary.py    # §1 compact per-dataset summary + key-col index status
doppler run --project core-x --config prd -- python diag2_cardinality.py # §2 resolution-key cardinality + master schema
PYTHONPATH=<repo> doppler run --project core-x --config prd -- python diag3_pushdown.py  # §3 A/B/C + FL/NY agent (BARE identifiers)
PYTHONPATH=<repo> doppler run --project core-x --config prd -- python diag3b_plans.py    # §3 verbatim analyze_plan (exact rows_scanned/bytes_read)
PYTHONPATH=<repo> doppler run --project core-x --config prd -- python diag4_fidelity.py  # §3.4 stored vs current macro, by state
PYTHONPATH=<repo> doppler run --project core-x --config prd -- python diag6_oldrule.py   # §3.4 pin to old-rule reconstruction + join-loss
```

Every script calls only `boto3 list_objects_v2`, `lance.dataset()`, `list_indices()`,
`stats.index_stats()`, `scanner().explain_plan()/analyze_plan()/count_rows()`, and lazy DuckDB
over the Arrow stream. **Zero mutation:** no `write_dataset`, no `create_scalar_index`, no
`add_columns`, no `delete`, no `.restore`. The `name_norm` macro is imported verbatim from
`core/name_norm.py`; the "old-rule reconstruction" in §3.4 is that macro minus the `&`→` AND `
and dash→space pre-steps, proving the stored column is byte-exact to the pre-fix rule.

> **Footnotes.** (i) `active/ca_ucc/` is **absent** (open error — no `_versions`); the
> `pipelines/ca_ucc/` pipeline exists but has not committed a dataset. (ii) The CO UCC lien
> layer (`co_ucc_transactions`, `ucc_co_collateral`, `ucc_co_debtors`,
> `ucc_co_secured_parties`) was also probed — all indices trained, `party_name_normalized`
> BTREEs present — but it is the lien layer, not the corporate registry, so it is out of scope
> for this diagnostic. (iii) **Harness footgun (operator-relevant):** Lance's DataFusion filter
> dialect treats **double-quoted identifiers as string literals** — `"corporate_name" = 'X'`
> constant-folds to `false` → 0-row full scan, which would wrongly read as "indexes never used."
> All filters above use **bare identifiers** (`corporate_name = 'X'`).
