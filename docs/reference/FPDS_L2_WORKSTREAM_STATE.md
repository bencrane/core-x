# FPDS L2 Satellite Workstream — Canonical State

**Scope:** the complete current state of the FPDS L2 satellite workstream — the two index-served access-path tables derived from the FPDS L1 transaction spine, the datasets they join into, the calibration and labor-crosswalk analysis conducted alongside them, the entity-dimension decision (recorded, not built), and the open items.

**Ground truth:** all row/column/index counts below were live-probed against the R2 system of record (`s3://data-sink/active/`) on 2026-07-04. Where a committed reference doc's inline prose disagrees with a live probe, the live number is used here and the discrepancy is noted. This document consolidates four committed reference docs (see [§13 Source map](#13-source-map)); it does not supersede their detail — it is the navigable summary and the reconciled fact set.

---

## Table of contents

1. [Architecture framing (why L2)](#1-architecture-framing-why-l2)
2. [The L1 spine](#2-the-l1-spine)
3. [L2 table — `usaspending_fpds_prime_award_state`](#3-l2-table--usaspending_fpds_prime_award_state)
4. [L2 table — `usaspending_fpds_mod_delta`](#4-l2-table--usaspending_fpds_mod_delta)
5. [Shared build mechanics & durability discipline](#5-shared-build-mechanics--durability-discipline)
6. [Cycles executed (chronological, with SHAs)](#6-cycles-executed-chronological-with-shas)
7. [The dataset constellation / join graph](#7-the-dataset-constellation--join-graph)
8. [Per-agency calibration (Cycle 2) verdict](#8-per-agency-calibration-cycle-2-verdict)
9. [Labor / wage substrate + crosswalk](#9-labor--wage-substrate--crosswalk)
10. [Entity Dimension (SCD2) — decision, not built](#10-entity-dimension-scd2--decision-not-built)
11. [FPDS domain facts established](#11-fpds-domain-facts-established)
12. [Open items / next steps](#12-open-items--next-steps)
13. [Source map](#13-source-map)

---

## 1. Architecture framing (why L2)

The plane is Gen-3: LanceDB on Cloudflare R2 is the absolute system of record. DuckDB reads Lance out-of-core; scalar indices (`BTREE` / `BITMAP`) are addressed by R2 URI (no catalog layer).

Two tiers:

- **L1 spine** = a **denormalized transaction ledger** (`usaspending_fpds_canonical_txn`). One row per FPDS transaction. It records *what the government wrote* at transaction grain. It cannot answer live operational questions (capacity, kinetics) without a full-table window pass every time, because those questions are per-award rollups over a transaction ladder.
- **L2** = **index-served access-path satellites** derived from the spine via **one shared award-partitioned window pass**. The 108M-row external sort is paid once and consumed two ways — a terminal-snapshot argmax + additive aggregate (→ `prime_award_state`) and a row-to-row `LAG()` first-difference (→ `mod_delta`). The satellites turn full-table-window questions into **index range-scans and BITMAP facet pushdowns**.

**Why L2 exists (the design rationale):** the spine answers "what was recorded" but cannot answer "which live awards are >85% consumed and expire within 90 days" or "which awards took a >$5M ceiling jump last month" without re-windowing 108M rows on every query. The L2 satellites precompute the award-grain capacity state and the per-mod kinetic delta, index them, and make both classes of question index-served. The business context these serve, stated as fact: a staffing-firm GTM use case — reaching companies that just won federal contracts and have imminent labor needs — consumes these tables plus the labor crosswalk (§9).

---

## 2. The L1 spine

| property | value |
|---|---|
| dataset | `usaspending_fpds_canonical_txn` |
| URI | `s3://data-sink/active/usaspending_fpds_canonical_txn/` |
| rows | ~107,962,341 |
| columns | 392 |
| grain | transaction |
| PK | `contract_transaction_unique_key` |
| award key | `contract_award_unique_key` |
| provenance | BULK ∪ FRESH reconciled |
| storage | Lance v2.1, manifest v19 |

The spine already carries USAspending's **resolved recipient keys**: `recipient_hash`, `parent_recipient_hash`, `recipient_levels`, `business_categories`. This matters for the entity dimension (§10) — the version-boundary work can key off resolved identities rather than raw address strings.

Refresh: overwrite rebuild (BULK ∪ FRESH reconcile). Ledger: `ops.usaspending_fpds_canonical_runs`. Both L2 tables carry `spine_manifest_version` so a build can be tied to the exact L1 version it read.

---

## 3. L2 table — `usaspending_fpds_prime_award_state`

**Purpose:** capacity / starvation. One row per prime-award root; the origination engine's "how much headroom, when does it expire" surface.

| property | value (live-probed 2026-07-04) |
|---|---|
| URI | `s3://data-sink/active/usaspending_fpds_prime_award_state/` |
| grain | prime award |
| PK | `contract_award_unique_key` |
| rows | 82,868,654 |
| columns | 43 |
| indices | 21 |
| Lance version | v22 |
| build_date | 2026-07-04 |

### `award_kind` (ternary, locked)

Derived from `idv_type_code` + parent linkage in the projection:

- `idv` — `idv_type_code` present and non-empty
- `order` — has a `parent_award_id_piid`
- `definitive` — neither

| kind | count |
|---|---|
| definitive | 17,068,433 |
| idv | 990,041 |
| order | 64,810,180 |

### Load-bearing columns

| column | meaning |
|---|---|
| `life_to_date_obligated` | **the ONLY summable measure** — `SUM(federal_action_obligation)` over the ladder |
| `current_authorized_ceiling` | current authorization = `base_and_exercised_options_value` (NULL for IDV) |
| `current_total_value_of_award` | obligated-to-date snapshot — a **reconciliation twin, NOT a ceiling** |
| `potential_ceiling` | max ceiling; IDV = `base_and_all_options_value`, else `COALESCE(potential_total_value_awar, base_and_all_options_value)` |
| `potential_ceiling_is_fallback` | true when `potential_total_value_awar` was NULL and the fallback fired |
| `remaining_ceiling_headroom` | `potential_ceiling − consumed` |
| `consumed_pct` | consumed / `potential_ceiling`, **unclamped** (>1 over-ceiling, <0 net de-obligated are legitimate) |
| `current_end_date` | IDV = `ordering_period_end_date`, else `period_of_performance_current_end_date` |
| `days_to_expiry` | `date_diff('day', build_date, current_end_date)` — a same-day convenience axis; the durable queue ranges on absolute `current_end_date` |
| `is_expired_no_followon` | past `current_end_date` and not terminally closed (`terminal_action_type_code` NOT IN E/F/X/K) |
| `is_terminated` | `BOOL_OR(action_type_code IN ('E','F','X'))` over the ladder |
| `terminal_action_type_code` | the argmax terminal row's action type |
| `idv_child_obligated` | **RECURSIVE subtree rollup** — Σ life-to-date over the entire resolved descendant subtree (through nested IDVs), not the IDV header's own ~$0 line |
| `idv_child_order_count` | distinct descendants in the subtree |
| `has_child_idv` | structural flag — this IDV is itself the resolved parent of another IDV (multi-tier vehicle) |
| `parent_award_key_resolved` | the resolved parent IDV key, or self, or NULL (dangling) |
| `parent_award_key_synth` | the constructed candidate `CONT_IDV_<piid>_<agency>` |
| `parent_match_flag` | `{self, resolved, dangling}` |
| `awarding_agency_code` | CGAC |
| `awarding_sub_agency_code` | sub-agency |
| `canonical_source_final` | BULK/FRESH provenance of the terminal row |
| `build_date` | injected literal (deterministic per build) |

### Parent resolution — construct-and-validate

The candidate parent key is `'CONT_IDV_' || parent_award_id_piid || '_' || parent_award_agency_id`, concatenated **VERBATIM — no case-fold**. Rationale: the spine's `contract_award_unique_key` is trim-only / case-preserving, and USAspending's Broker concatenates raw components, so a byte match *requires* verbatim; an `upper()` on one side would silently mis-flag every lower/mixed-case PIID as dangling and understate IDV consumption. The candidate is semi-joined against the real IDV-key set (`idv_keys`). A wrong candidate fails the semi-join → `dangling`, never a silent mis-rollup (grammar-self-checking). A NULL parent agency yields a NULL synth (`x || NULL = NULL`) → no match → dangling.

- `self` = no parent PIID (definitive / standalone IDV)
- `resolved` = candidate matched a real IDV key
- `dangling` = has a parent PIID but no matching IDV key

**Measured dangling rate: ~0.61%.**

### Recursive IDV rollup

The IDV capacity denominator is the Σ life-to-date over the **entire resolved subtree** — all descendant orders through any depth of nested IDVs — computed by a `WITH RECURSIVE` transitive fold that attributes each award's obligation to *every* ancestor IDV in its chain. The recursive step climbs only through nested IDVs (`nested_idv`, ~1e4 rows → a cheap hash probe, no second 108M scan), depth-capped at 12. `parent_award_key_resolved` is a single-valued tree edge, so there are no cross-path double-counts. Max descendants observed in a single subtree: **~1,017,447**. With the recursive rollup the IDV denominator is exact even for multi-tier vehicles; `has_child_idv` is retained as a structural flag, no longer a lower-bound warning.

### Capacity math discipline

Only `federal_action_obligation` is summable. Every `*_value` / `total_*` / ceiling field is a **cumulative snapshot** — the terminal (ladder-final argmax) value is taken, never `SUM` (summing multiplies value by mod count). Reconciliation: `life_to_date_obligated − total_dollars_obligated_snapshot`; measured **recon_delta p99 = $0** (SUM discipline holds end to end). A fail-closed gate aborts publish if any `|recon delta| > $1`.

---

## 4. L2 table — `usaspending_fpds_mod_delta`

**Purpose:** kinetic events. One row per **modifying** transaction (base / new-award rows excluded); the row-to-row first-difference of the footprint-relevant columns.

| property | value (live-probed 2026-07-04) |
|---|---|
| URI | `s3://data-sink/active/usaspending_fpds_mod_delta/` |
| grain | modifying transaction |
| PK | `contract_transaction_unique_key` (mods only) |
| rows | 25,017,209 |
| columns | 31 |
| indices | 14 |
| Lance version | v15 |

> Note: `USASPENDING_FPDS_L2_SATELLITES_AND_JOIN_GRAPH.md` §1 body text reads "29 cols, 12 indices"; that inline figure is stale. The doc's own §2 constellation table and the live probe both read **31 cols / 14 indices** (the +2 are `awarding_agency_code` and `award_pool`, added in Cycle 2.5). Use 31/14.

### `award_pool` (Cycle 2.5)

`CASE WHEN award_kind='idv' THEN 'parent' ELSE 'child' END`, BITMAP-indexed. Live split:

| pool | count |
|---|---|
| parent (IDV-vehicle mods) | 3,554,249 |
| child (order + definitive mods) | 21,462,960 |

### Load-bearing columns

| column | meaning |
|---|---|
| `delta_potential_ceiling` | Δ potential ceiling (NULL 45.7% — BULK-only field, NULL on FRESH-winning rows) |
| `delta_authorized_ceiling` | Δ `base_and_exercised_options_value` |
| `delta_base_and_all_options` | Δ `base_and_all_options_value` |
| `delta_federal_action_obligation` | this row's per-txn obligation (already a signed delta — **grain-safe to SUM**) |
| `delta_current_total_value_of_award` | Δ obligated-to-date snapshot |
| `delta_total_dollars_obligated` | Δ total-dollars-obligated snapshot |
| `delta_current_end_date_days` / `delta_potential_end_date_days` / `delta_ordering_end_date_days` / `delta_pop_start_date_days` | date first-differences (days) |
| `action_type_klass` | coarse operational class (see `_KLASS_CASE` below) |
| `is_scope_increase` | keys off the **ceiling delta only** (positive `potential`/`all-options` delta) — **NOT consumption**; consumption grows on an option exercise within the existing ceiling and is surfaced separately |
| `is_termination_event` | `action_type_code IN ('E','F','X')` |
| `identity_changed` | any of {recipient_uei, recipient_hash, recipient_name, parent_uei, cage_code, business_categories, business-size determination, funding_office_code, recipient_county_name} `IS DISTINCT FROM` the prior row |
| `identity_change_fields` | comma-list of which identity fields changed |
| `prev_recipient_uei` | prior row's UEI (SCD2 change-cut signal) |
| `awarding_agency_code` | CGAC, BITMAP (Cycle 2.5) |
| `award_pool` | `'parent'`/`'child'`, BITMAP (Cycle 2.5) |

Kind split of mods: parent 3,554,249 / child 21,462,960 (= `award_pool` above; the `award_kind` `idv` rows are the parent pool).

### `action_type_klass` — the mapping (`_KLASS_CASE`)

FAR-uniform enumeration; the mapping is portable across agencies (only base rates are agency-specific — §8). **ADVISORY only** — the computed delta sign, never klass membership, drives `is_scope_increase`.

| action_type_code | klass |
|---|---|
| `G` | `option_exercise` |
| `A B D H L` | `scope_change` |
| `C` | `funding_only` |
| `E F X N` | `termination` |
| `J P R T V W` | `identity_boundary` |
| `K M S` | `admin` |
| `Y` | `nonstandard` ⚠ (see below) |
| (any other) | `unclassified` |

⚠ **Open correction:** Cycle 1 classified `Y` as `nonstandard` from a data profile. That was incorrect — per the DEC (Data Element Catalog), `Y = "ADD SUBCONTRACT PLAN"`, a documented FPDS reason-for-modification. `fpds_action_type_ref` was corrected (PR #963). But the live `mod_delta.action_type_klass` **still labels the 8,736 `Y` rows `'nonstandard'`** (live-verified 2026-07-04). Reclassification is queued for the next `mod_delta` rebuild — not yet applied. See §12.

---

## 5. Shared build mechanics & durability discipline

**Builder:** `pipelines/usaspending/usaspending_fpds_l2.py` (CLI: `build | init_ops | index | verify | print_sql`). **Orchestrator:** `pipelines/usaspending/usaspending_fpds_l2_modal.py`.

### The shared pass

1. A ~37-col narrow projection of the spine (VARCHAR→numeric/date `TRY_CAST`s done once) → `spine_proj` temp table (single R2 pass).
2. One window spec over `spine_proj`: `PARTITION BY contract_award_unique_key ORDER BY (action_date, last_modified_date, modification_number, transaction_number, contract_transaction_unique_key)`. The **5-key total ORDER BY is mandatory** — same-`(action_date, last_modified_date)` correction rows exist; without the total ladder, snapshots/deltas flip across rebuilds. This gives `ROW_NUMBER()=1` (terminal argmax → STATE) and `LAG()` (first-diff → DELTA) from one sort.
3. Additive GROUP-BY aggregate (`life_to_date_obligated`, terminal flags) → STATE.
4. The only extra work is the IDV parent→child rollup: a recursive hash aggregate over the already-collapsed ~40-55M award-grain staging, **not** a second 108M scan.

### Write / publish discipline

- Module-top `os.environ.setdefault("LANCE_BYPASS_SPILLING","true")` **before any `import lance`** — the in-RAM scalar-index sort OOMs the DataFusion external-merge pool on a ≥40M-row BTREE build.
- **No direct-R2 write** (native R2 writer streams adaptive parts R2 rejects → 400 InvalidPart). Path: **LOCAL Lance write → boto3 uniform-part publish** (reuses the spine's proven `_publish_local_to_r2` / index-delta plumbing). Lance v2.1, `max_rows_per_file = 1,048,576`.
- Write and index phases are **deliberately split**: write data-only local → close + `malloc_trim` the DuckDB arena → build indices in-RAM → publish. A ≥40M-row BTREE build colliding with a still-resident 72GB DuckDB heap is an out-of-band OOM-SIGKILL that `except/finally` cannot catch.
- One naive-UTC `built_at` literal + one `build_date` literal injected per build (not `now()`) → deterministic, idempotent.
- **Fail-closed per-table PK gate BEFORE publish** (STATE: `count == distinct(contract_award_unique_key)`; DELTA: `count == distinct(contract_transaction_unique_key)`). Aborts publish on any leak.
- **No auto-retries** (`retries=0`); overwrite idempotency makes re-runs safe.

### Ops ledgers

One row per (table, build), written via psycopg to `HQX_DB_URL_POOLED`:

- `ops.usaspending_fpds_prime_award_state_runs` — lineage (`spine_manifest_version`), kind counts, `dangling_count`, `recon_delta_p99`, `recon_fail_count`, `max_current_end_date`, `indices_built`, `status`.
- `ops.usaspending_fpds_mod_delta_runs` — `mods_out`, `distinct_awards`, `novation_count`, `termination_count`, `scope_increase_count`, `identity_change_count`, `potential_delta_null_rate`, `indices_built`, `status`.

DDL: `pipelines/usaspending/ops_usaspending_fpds_l2_runs.sql` (idempotent, self-bootstrapping via `to_regclass`). `error_message` is never NULL when `status <> 'success'`.

### Modal orchestration & completion

Long builds run **detached** (`modal run --detach`): `retries=0` + overwrite makes detach safe (a re-run is operator-initiated, never a partial-state hazard). `max_containers=1` guards against a double-launch on the same R2 prefixes. Sizing: build 96 GiB / 16 CPU / 6h (`DUCKDB_MEM=72GB`, `THREADS=8`); index 48 GiB / 8 CPU / 3h; verify 32 GiB / 4 CPU / 1h. A cheap `smoke` gate (packaging + secrets + spine source-column contract) runs before the giant.

**Completion = two-source AND sentinel:** Modal app state **and** a fresh `status='success'` ledger row — never a held client process. Read-back validation via `verify --table state|delta` (independent scanner → DuckDB structural assertions: PK-unique, `award_kind` domain, `parent_match_flag` domain, `action_type_klass` domain, single `built_at`).

---

## 6. Cycles executed (chronological, with SHAs)

Commit sequence on `main` (worktree `/tmp/canon-wt`, HEAD `0e6e6bf`):

| SHA | PR | what landed |
|---|---|---|
| `df96557` | #955 | FPDS L2 satellites — prime-award capacity/state + mod-delta (shared window pass) — **code, pre-build** |
| `75913e8` | #956 | doc: FPDS L2 satellites — what landed + the DuckDB join graph |
| `de05c90` | #957 | doc: Labor × GovCon crosswalk — live inventory + 10 GTM unlocks |
| `5f8fe73` | #958 | **Cycle 1** — L2 recursive IDV rollup + `action_type='Y'` diagnosis |
| `a47101e` | #959 | **Cycle 2** doc — per-agency calibration; global thresholds disqualified |
| `4d2450d` | #960 | **Cycle 2.5** — `mod_delta` carries `awarding_agency_code` + `award_pool` — code, pre-rebuild |
| `68ab920` | #961 | doc: mark Cycle 2.5 ship-blocker resolved |
| `6fcd84f` | #962 | doc: FPDS L2 entity dimension (SCD2) — build approach & sequencing decision |
| `0e6e6bf` | #963 | fix: `fpds_action_type_ref` — source verbatim from DEC, drop editorial gloss, fix `Y` |

### PR #955 — initial L2 landing

Code for both satellites + the shared window pass (pre-build). The first live giant build followed, producing both tables (documented in #956).

### Cycle 1 (PR #958) — recursive IDV rollup + `Y` diagnosis

- **Recursive IDV rollup:** `idv_child_obligated` now folds the entire resolved subtree through nested IDVs (multi-tier vehicles — GWAC/FSS → BPA → orders). `has_child_idv` becomes a structural flag (denominator is now exact). Max descendants observed ~1,017,447.
- **`action_type='Y'` diagnosis:** classified `Y` as `nonstandard`. **This was incorrect** — see the correction in Cycle #963 below and §4.

### Cycle 2 (PR #959) — per-agency calibration (doc)

Per-agency threshold calibration over 13 top-tier CGAC agencies. Verdict and measured spreads: §8.

### Cycle 2.5 (PRs #960 code / #961 doc) — calibration-key columns

Added `awarding_agency_code` (CGAC, BITMAP) + `award_pool` (`'parent'`/`'child'`, BITMAP) to `mod_delta`. Resolves the calibration ship-blocker: per-agency percentile grouping is now a two-column BITMAP pushdown, no `mod_delta ⋈ prime_award_state` join. Live (non-null on 99.999% of the 25,017,209 mod rows).

### PR #963 — `fpds_action_type_ref` DEC correction

Corrected `fpds_action_type_ref` to source **verbatim from the DEC** (`usaspending_data_dictionary`, element `ActionType`, `Contracts:` domain block). 21 rows. Key corrections: `Y = "ADD SUBCONTRACT PLAN"` (was mis-diagnosed as nonstandard admin), plus authoritative `A`/`V`/`W` wording. Loader (`pipelines/reference/materialize_fpds_action_type_ref.py`) parses the DEC domain fail-closed (≥15 codes or abort). **Consequence still open:** `mod_delta.action_type_klass` is not yet rebuilt, so the `Y` rows are still labeled `nonstandard` in live data (§12).

### Reference docs

- #956 — join-graph doc
- #957 — labor crosswalk doc
- #962 — entity-dimension decision doc

---

## 7. The dataset constellation / join graph

The universal prime-award key is **one Broker value under four column names**:

```
contract_award_unique_key  ≡  generated_unique_award_id  ≡  prime_award_unique_key  ≡  unique_award_key
```

(Contract keys are `CONT_*`; a natural join intersects only shared contract awards. Byte-identical Broker keys — bridge the name explicitly in every `ON`.)

### Datasets (live-probed)

| dataset | URI (`s3://data-sink/active/…`) | grain | PK | rows | cols/idx | prime-award key column | entity key | freshness |
|---|---|---|---|--:|--:|---|---|---|
| L1 FPDS spine | `usaspending_fpds_canonical_txn/` | transaction | `contract_transaction_unique_key` | 107,962,341 | 392 / 18 | `contract_award_unique_key` | `recipient_uei` | BULK∪FRESH reconciled, manifest v19 |
| L2 prime_award_state | `usaspending_fpds_prime_award_state/` | prime award | `contract_award_unique_key` | 82,868,654 | 43 / 21 | `contract_award_unique_key` | `recipient_uei` | derived from L1 (v22, build 2026-07-04) |
| L2 mod_delta | `usaspending_fpds_mod_delta/` | modifying txn | `contract_transaction_unique_key` | 25,017,209 | 31 / 14 | `contract_award_unique_key` · `awarding_agency_code` · `award_pool` | `recipient_uei` | derived from L1 (v15) |
| award_search (BULK) | `usaspending/award_search/` | prime award | `generated_unique_award_id` | 78,636,657 | 154 / 3 | `generated_unique_award_id` | `recipient_uei` | **BULK pg_dump snapshot 2026-05-06 — NOT reconciled with live API** |
| subaward_canonical | `usaspending_subaward_canonical/` | subaward | `(prime_award_unique_key, subaward_number)` | 1,315,680 | 258 / 30 | `prime_award_unique_key` | `subawardee_uei` / `prime_awardee_uei` | BULK∪FRESH reconciled, contract-only |
| fpds_action_type_ref | `fpds_action_type_ref/` | action code | `action_type_code` | 21 | 5 / 1 | — (`action_type_code`) | — | static dim, DEC-sourced (v6) |

### The universal keys

- **Prime award:** the four-name key above.
- **Entity (UEI):** `recipient_uei` (spine / state / delta / award_search). On subawards, `prime_awardee_uei` is that same prime awardee; `subawardee_uei` is the sub.
- **Transaction:** `contract_transaction_unique_key` (L1 ⋈ L2 mod_delta, 1:1).
- **Action code:** `action_type_code` (mod_delta / L1 ⋈ `fpds_action_type_ref`, N:1).
- **Product/service name bridge:** `product_or_service_code` ≡ `psc_code` (labor side) — value identical, name differs.

### Two award-grain views, deliberately different

- **`prime_award_state`** — bottom-up, fresh: computed from the FPDS transaction spine (BULK∪FRESH), carries capacity math the government rollup does not (potential-ceiling headroom, IDV→child rollup, expiry horizon, `award_kind`).
- **`award_search` (BULK)** — top-down, stale: USAspending's own award-level rollup as of the 2026-05-06 pg snapshot, 154 award-level columns (`total_obligation`, recipient rollups, CFDA/program metadata) — **not** reconciled against the live API. Use for award-level metadata and cross-checks, never as the freshness source. `award_search_merged` (the bulk+delta reconcile in `usaspending_award_search_reconcile.py`) is **coded but NOT materialized** — the merged URI 404s.

### Grain hazards (correctness rules)

- **Only three amount columns SUM safely:** `prime_award_state.life_to_date_obligated` (award grain), `mod_delta.delta_federal_action_obligation` (mod grain), `subaward_canonical.subaward_amount` (sub grain). Every `*_value` / `total_*` / ceiling / wage column is a cumulative snapshot — use `MAX`/`ANY`, never `SUM`.
- **Never sum award-repeated fields at sub grain.** `subaward_canonical` repeats `prime_award_*` context across every sub of a prime — dedup to `prime_award_unique_key` before aggregating. The highest-blast-radius trap is `prime_award_amount`.
- **`delta_potential_ceiling` is NULL 45.7%** (BULK-only). For "did the ceiling grow" use `is_scope_increase`, not the raw column.
- **`consumed_pct` is unclamped** — filter with an upper bound (`<= 5`) for a clean cohort.
- **IDV denominators:** now exact via the recursive rollup; `has_child_idv` flags multi-tier vehicles structurally.
- **`max_current_end_date = 9999-12-31`** is FPDS's open-ended placeholder.

Full runnable DuckDB-over-Lance recipes (filter-then-join, projection + predicate pushed into Lance scalar indices) are in `USASPENDING_FPDS_L2_SATELLITES_AND_JOIN_GRAPH.md` §4.

---

## 8. Per-agency calibration (Cycle 2) verdict

**Status:** ship-blocking config finding, resolved on the data-availability side by Cycle 2.5. Live-probed 2026-07-04.

### Verdict

**Global alert thresholds are DISQUALIFIED. The mechanism is per-agency PERCENTILE NORMALIZATION — one model keyed on `(CGAC × parent/child-pool)`, ranking each signal by its percentile within its own agency's distribution — NOT a matrix of hand-curated per-agency threshold tables.** "Agency" enters only as a `GROUP BY` on a precompute step (like z-scoring). The curated-per-agency-tables alternative is rejected as over-engineering (near-zero marginal recall/precision, maintenance-drift risk).

### Portable mapping vs. non-portable base rates (do not conflate)

- **The klass MAPPING is PORTABLE** — `action_type_code → klass` is a FAR-uniform enumeration; same code, same meaning, every agency. Keep it global.
- **The base RATES are NON-PORTABLE** — novation, termination, funding-only rates baseline per CGAC. Fire on deviation from the agency's own baseline, never a shared cutoff. The Air Force seed (scope 29.7% / fund 21.2% / novation 1.45/1k) that originally calibrated the footprint recon is **not representative** of GSA, NASA, or DOJ on any klass axis.

### Measured spreads (why a global cut is provably broken)

| axis | spread | direction |
|---|---|---|
| "massive mod" Δceiling P99 | **226×** (GSA $0.36M → DoD $81.3M) | a global "$5M=massive" line floods DoD and sits 14× above GSA's entire P99 (0% GSA recall) |
| "big award" ceiling P99 | **47×** (DoD $0.80M → HHS $37.48M) | flags every HHS/DoT/NASA award as routine while burying an anomalous $2M DoD order |
| novation rate | **32×** (USDA 0.48/1k → GSA 15.57/1k) | a "novation is rare" prior silences GSA vehicle-holder transfers |
| termination rate | **117×** (NASA 0.1% → GSA 11.7%) | a "~1%=distress" line floods GSA and never fires NASA |

### Structural findings (inputs to the normalization, not extra tables)

1. **Split IDV-parent pool from order-child pool BEFORE computing percentiles** — highest-leverage fix; pooling is what manufactures DoD's $81.3M artifact. (One partition key → delivered as `award_pool`, Cycle 2.5.)
2. **`consumed_pct` DELETED from the starvation definition** — saturated at P50=P90=1.0 for every agency (FPDS `potential_ceiling == federal_action_obligation` on definitive/single-action lines) → zero discrimination. Starvation now ranks on `days_to_expiry` + `remaining_ceiling_headroom` per-agency percentile, plus funding-only mod cadence.
3. **One order-share router flag** — `mode = IDV if pct_order ≥ 55% else DEF`; changes *which* signal is scored.

### The 13-agency calibration table (P99 snapshot the CDF emits)

`massive_mod Δceiling` = agency P99 `|Δpotential_ceiling|` on scope-change mods; `big_award ceiling` = agency P99 award `potential_ceiling`; novation/term/fund are **expected base rates**, not alarms.

| CGAC | Agency | mode | massive_mod Δceiling P99 | big_award ceiling P99 | novation /1k | term % | fund % | notes |
|---|---|---|---|---|--:|--:|--:|---|
| 097 | DoD | IDV | $81.3M ⚠ artifact | $0.80M | 1.45 | 3.0 | 21.2 | rank on parent/def pool only — $81.3M is pool-mixing |
| 047 | GSA | IDV | $0.36M (floor) | $1.53M | **15.57** | **11.7** | 3.3 | schedules host; novation+termination churn is the signal |
| 036 | VA | DEF | $1.35M | $0.97M | 1.30 | 0.9 | 20.2 | opt 21.5% routine — options ≠ capacity expansion |
| 015 | DOJ | IDV | $2.15M | $1.74M | 0.72 | 0.4 | **49.2** | money-movement regime; under-weight scope |
| 019 | State | DEF | $1.84M | $1.37M | 0.68 | 0.9 | 24.0 | scope 41.6% (highest) — definitive-recompete surface |
| 012 | USDA | DEF | $1.06M | $3.56M | **0.48** (floor) | 1.5 | 17.8 | a 3/1k novation spike must still fire here |
| 075 | HHS | IDV | $7.04M | **$37.48M** (highest) | 0.79 | 0.8 | 18.2 | genuine mission scale (median $9.5K) — real fat tail |
| 014 | Interior | DEF | $1.11M | $1.39M | 1.08 | 1.3 | 15.6 | |
| 070 | DHS | IDV | $3.52M | $9.27M | 0.69 | 0.9 | 17.2 | ident_p1k 323 — do NOT threshold ident |
| 020 | Treasury | IDV | $4.28M | $9.77M | 0.67 | 1.1 | 24.1 | |
| 013 | Commerce | DEF | $3.36M | $4.77M | 2.15 | 0.6 | 17.3 | ident_p1k 334 (highest) — measurement noise, demote |
| 069 | DoT | IDV | $3.62M | $12.07M | 0.65 | 0.4 | 36.1 | highest expiry pressure (pct_starving 0.37%) |
| 080 | NASA | DEF | $23.28M (mission-real) | $7.91M | 0.88 | **0.1** (floor) | **43.1** | 1 termination = 10× event; funding cadence IS the tell |

### Artifacts NOT to chase (adversarially confirmed)

DoD's $81.3M is heavy-tail + pool-mixing (dissolved by the pool split); `consumed_pct` saturation is pure measurement artifact (deleted); DoD's 0.04% pct_starving is denominator dilution; `identity_changed` is a per-mod field-rewrite flag (measurement noise — demote to corroborator; novation `J` is the real succession signal); the 47× ceiling scale span is real scale, neutralized for free by the percentile transform.

**Bottom line:** one normalized model keyed by `(CGAC × parent/child-pool)`, ranking on within-group percentile, `consumed_pct` deleted, a single global floor formula (`min_Δ = max($100K, 0.25 × agency_ceil50)`), one order-share router. The pipeline-side ship-blocker (`awarding_agency_code` + `award_pool` on `mod_delta`) is **resolved** (Cycle 2.5); the percentile model itself is consumer-side (§12). Full 13-agency detail + method in `FPDS_L2_AGENCY_CALIBRATION.md`.

---

## 9. Labor / wage substrate + crosswalk

Built by prior work; inventoried and crosswalked this workstream (doc PR #957). Same Gen-3 plane, same filter-then-join discipline. Row counts from a live probe 2026-07-04.

### Live inventory

**Occupation ↔ wage (priced-labor core)**

| dataset | rows | grain / key |
|---|--:|---|
| `bls_oews_2025` | 413,527 | area × `occ_code`(SOC) × `naics` |
| `soc_state_wage` | 35,223 | `soc_code` × `state_fips`/`prim_state` |
| `soc_priced_skilled` | 830 | `soc_code` — national skilled-SOC wage + O*NET |
| `bls_employment_projections_2024_2034` | 1,113 | `occupation_code` — growth + median wage + openings |
| `bls_ep_industry_occupation_matrix_2024_2034` | 113,473 | `industry_code` × `occupation_code` × `naics_code` |

**SCA taxonomy + SCA↔SOC bridge**

| dataset | rows | key |
|---|--:|---|
| `dol_sca_occupations` | 502 | `occupation_code` — SCA labor categories |
| `sca_soc_crosswalk` | 424 | `occupation_code` ↔ `soc_code` (+ tier, confidence, dominance_ratio) |

**Statutory floors + union identity**

| dataset | rows | key |
|---|--:|---|
| `sam_wage_determinations` | 10,055 | `wd_id` / `cba_number` / `full_reference_number` |
| `sam_wd_cba_pointers` | 4,298 | `wd_id` ↔ `cba_number`, `contractor_union`, effective dates |
| `sam_wd_cba_coverage` | 4,270 | `wd_id` ↔ state/county |
| `olms_cba_crosswalk` | 4,844 | **`uei`** ↔ `union_name`, `exp_date`, `is_active` |
| `olms_cba_index` | 4,849 | `cba_pub_id` — employer/union/`naics`/`no_of_emp` |

**NAF prevailing wage + county geography**

| dataset | rows | key |
|---|--:|---|
| `naf_wage_rates` | 1,670,700 | `wage_area`×`schedule_number`×`grade`×`step` → `hourly_rate` |
| `naf_wage_area_county_fips` | 769 | `wage_area` ↔ `county_fips` |
| `view_county_wage_arbitrage_benchmark` | 502 | `county_fips` → NAF grade min/max benchmark |

**NAICS×PSC labor hub (pre-joined)**

| dataset | rows | key |
|---|--:|---|
| `naics_psc_labor_dim` | 16,291 | `naics_code`×`psc_code` → `is_labor_play`, `rank1_soc_code`, `rank1_sca_code` |
| `naics_psc_labor_profile_categories` | 54,235 | naics×psc×rank → `soc_code`, `sca_code`, `role_class`, `a_median`, `ep_growth_2024_2034_pct` |
| `naics_psc_labor_profile` | 16,291 | naics×psc → labor-play summary + `oews_industry_code` |
| `naics_reference` 2,125 · `psc_reference` 6,108 · `naics_vertical_taxonomy` 2,432 · `naics_psc_vertical_map` 279 | | reference / verticals |

**Award & solicitation demand (keys straight to spine/L2)**

| dataset | rows | key |
|---|--:|---|
| `active_award_labor_demand` | 1,080 | `contract_award_unique_key` + `recipient_uei` + `labor_role` |
| `govcon_labor_demand` | 20,598 | `contract_award_unique_key` + `notice_id` → `headcount`, `clearance_level`, `wage_floor` |
| `govcon_pricing` | 170,532 | `contract_award_unique_key` + `notice_id` — solicitation pricing text |
| `sam_labor_poc_people` | 29,464 | `uei` → staffing POC + `company_linkedin_url`, `in_our_staffing` |

### Crosswalk keys

`naics_code`; `product_or_service_code` ≡ `psc_code`; `soc_code` (≡ OEWS `occ_code` ≡ EP `occupation_code`); SCA `occupation_code` (bridged to SOC via `sca_soc_crosswalk`); `pop_county_fips` ≡ `county_fips`; `uei` (≡ `recipient_uei` prime / `subawardee_uei` sub); `notice_id` / `solicitation_number`.

**The accelerator:** `naics_psc_labor_dim` is pre-joined — one join from any award's `(naics_code, product_or_service_code)` yields `is_labor_play` + top SOC + top SCA (no 4-way chain). Reach `naics_psc_labor_profile_categories` only for the full ranked role mix + per-role wage/growth.

### Two live-truth corrections (vs. older labor docs)

- **`sca_soc_crosswalk` now EXISTS (424 rows).** An older doc (`01_LABOR_PRICING_FOUNDATION.md` §1.4, 2026-07-02) claims "no SCA↔SOC crosswalk landed" — stale. OEWS market wage and SCA statutory floor now meet at occupation identity.
- **`pop_county_fips` is on the L1 spine, NOT on L2 `prime_award_state`.** Any wage-*locality* join routes through `usaspending_fpds_canonical_txn` (or `award_search`) for county/state, then to the wage datasets. Adding `pop_county_fips` + `primary_place_of_performance_state_code` to a future `prime_award_state` rebuild would collapse that hop.

### Not materialized (404 on probe)

`psctool`, `govcon_labor_demand_90day`, `govcon_pricing_90day`.

Full crosswalk key graph + 10 DuckDB recipes in `LABOR_x_GOVCON_CROSSWALK_GTM.md`.

---

## 10. Entity Dimension (SCD2) — decision, not built

**Status: decision recorded (PR #962). Build NOT started.**

An SCD2 dimension is **two separable things** with different best sources:

- **(A) Version structure** — the `[valid_from, valid_to)` temporal boundaries (when an entity's tracked attributes changed).
- **(B) Attribute values** — what the attributes were in each version (address, geo, socioeconomic flags, org structure, parent linkage).

### The decision (stated neutrally)

**Build the version/history + change-event core from spine + `mod_delta` now; enrich current-canonical attribute values from the reconciled award_search spine (and/or SAM `entity_profile_gold` / `sam_master_entities`, the more authoritative current-attribute source) later, additively.** The two phases are decoupled — version *structure* and current attribute *values* do not gate each other.

### Rationale

- **Source fitness for (A) boundaries:** spine + `mod_delta` is the strongest source. `mod_delta.identity_changed` / `prev_recipient_uei` / `identity_change_fields` **already materialize** the entity-boundary cuts at transaction grain, keyed on `recipient_uei`, capturing intra-award changes; the change reason is the mod's `action_type` (novation `J`, re-rep `R`/`P`, vendor change `V`/`W`, transfer `T`). award_search is coarser (award-grain, one snapshot per award-update) and not pre-computed — boundaries must be reconstructed by diffing award snapshots.
- **Source fitness for (B) current values:** the reconciled award_search (award-context) is cleaner than raw per-transaction snapshots; **SAM is the most authoritative for registered current attributes** — `entity_profile_gold` and `sam_master_entities` already exist as the authoritative current-state registry. This narrows the FPDS entity dimension's distinctive value to the **award-context view** (what an entity looked like on its federal contracts over time, and when the contracting record changed) — which is history-first, favoring the spine + `mod_delta` path.

### The honest decision variable

The tie-breaker is the **award_search timeline × which output is primary**:

| condition | preferred plan |
|---|---|
| award_search imminent (days) **and** primary need is a clean current-attribute lookup | wait — build once, avoid a throwaway current-attribute pass |
| award_search weeks+ out, **or** primary need is the change-event stream / version history | history-core-now — value delivered at no correctness cost; enrich later |
| unsure of timeline | history-core-now — independent, additive, low-regret |

### Build plan for the "now" core (documented, not executed)

- **Grain / PK:** `recipient_uei` × `[valid_from, valid_to)` version (synthesized `entity_version_key`).
- **Boundaries:** derive from `mod_delta` where `identity_changed = true`; cut points are `(recipient_uei, action_date, prev_recipient_uei, identity_change_fields, action_type_code)`.
- **Values:** version on the spine's **resolved keys + coarse attributes** (`recipient_hash`, `parent_recipient_hash`, `business_categories`, `recipient_levels`, org-class flags, state/county, `cage_code`, `parent_uei`) — **not raw address strings** — and debounce (persistence across ≥2 transactions) to suppress single-mod noise.
- **Indices:** BTREE `recipient_uei` / `recipient_hash` / `valid_from` / `valid_to`; BITMAP `is_current` / `change_reason` / socioeconomic flags.
- **award_search enrichment (later, additive):** left-join the reconciled award_search current recipient snapshot onto the `is_current` version; no rebuild of the boundary structure.

Full reasoning (including conceded points where wait-for-award_search is correct) in `FPDS_L2_ENTITY_DIMENSION_BUILD_DECISION.md`.

---

## 11. FPDS domain facts established

Data-verified this workstream. These characterize the FPDS IDV/order/definitive structure and constrain how the L2 tables and consumers reason about vehicles.

### IDV completeness & set-aside

- IDVs carry NAICS **96.5%**, PSC **99.7%**, a set-aside field populated **68.7%**, a recipient **~100%**.
- **"Populated" ≠ "actually set aside":** the set-aside field has NULL, an explicit `'NONE'` (competed, no set-aside), and real codes. Actual set-aside rate: **order 3.4% / definitive 15.9% / idv 22.4%**.

### IDV ↔ order code relationship

- **NAICS is largely stable** — orders inherit the vehicle NAICS ~95–100%.
- **PSC diverges** — measured by sector: ~3% (wholesale/building-materials) up to ~69% (food), ~50% (machinery/electronics), ~39% (services).

### IDV ceilings

- Only **26.5%** of IDVs state a ceiling (`base_and_all_options_value > 0`).
- **92.2%** have $0 header obligation — money is obligated per-order, not reserved at the IDV.
- The stated ceiling is a **soft not-to-exceed:** 15.2% of ceiling-bearing IDVs have child obligations exceeding it. By type: GWAC/FSS ~12.5%; IDC/BPA/BOA ~27–32%.
- The IDIQ guaranteed-minimum is **not a standard FPDS field.**
- **Consequence:** real IDV spend is discovered **bottom-up** via the child-order rollup (`idv_child_obligated`), not from the IDV header line.

---

## 12. Open items / next steps

Factual list, no prioritization.

1. **`mod_delta.action_type_klass` `Y`-reclassification** — the 8,736 `Y` rows are still labeled `'nonstandard'` in live data (verified 2026-07-04). `Y = "ADD SUBCONTRACT PLAN"` per the DEC (`fpds_action_type_ref` corrected in PR #963). Reclassification to a meaningful klass is queued for the next `mod_delta` rebuild; the `_KLASS_CASE` in `usaspending_fpds_l2.py` still emits `'nonstandard'` for `Y`. **Not applied.**
2. **Cycle 3 — Entity Dimension (SCD2)** — decision documented (§10, PR #962); build not started.
3. **Origination-engine per-agency percentile calibration model** — consumer-side, not pipeline. The pipeline-side prerequisite (`awarding_agency_code` + `award_pool` on `mod_delta`) is delivered (Cycle 2.5); the percentile CDF model + scoring is specified in `FPDS_L2_AGENCY_CALIBRATION.md` §4.
4. **award_search-dependent work** — cross-source reconciliation (bottom-up `life_to_date_obligated` vs. top-down `total_obligation`) and entity-dimension current-attribute enrichment are deferred pending the reconciled `award_search` spine (owned by another agent; see `AWARD_API_PULL_HANDOFF.md`). `award_search_merged` is **coded** (`usaspending_award_search_reconcile.py`) but **not materialized** — the merged URI 404s.

---

## 13. Source map

### Reference docs (committed, `docs/reference/`)

| doc | covers |
|---|---|
| `USASPENDING_FPDS_L2_SATELLITES_AND_JOIN_GRAPH.md` | L2 tables + the dataset constellation + DuckDB join/union recipes |
| `FPDS_L2_AGENCY_CALIBRATION.md` | per-agency calibration verdict, 13-agency table, method |
| `LABOR_x_GOVCON_CROSSWALK_GTM.md` | labor/wage substrate inventory + crosswalk keys + 10 GTM recipes |
| `FPDS_L2_ENTITY_DIMENSION_BUILD_DECISION.md` | SCD2 sequencing decision, source-fitness matrix, "now" build plan |

### Pipeline files

| file | role |
|---|---|
| `pipelines/usaspending/usaspending_fpds_l2.py` | the builder — projection, `_KLASS_CASE`, shared window pass, recursive IDV rollup, capacity math, per-table index plans, PK gate, ops writes, `verify` |
| `pipelines/usaspending/usaspending_fpds_l2_modal.py` | Modal orchestrator — `smoke` / `build` / `index` / `verify` / `init_ops`; sizing knobs; detach-safe |
| `pipelines/usaspending/ops_usaspending_fpds_l2_runs.sql` | ops-ledger DDL for both tables |
| `pipelines/reference/materialize_fpds_action_type_ref.py` | DEC-sourced `fpds_action_type_ref` loader (verbatim `Contracts:` domain, fail-closed) |
| `pipelines/usaspending/usaspending_award_search_reconcile.py` | `award_search_merged` reconcile — coded, not materialized |

### Commits (PRs #955–#963, `main`)

| SHA | PR | summary |
|---|---|---|
| `df96557` | #955 | L2 satellites — shared window pass (code, pre-build) |
| `75913e8` | #956 | doc — L2 satellites + join graph |
| `de05c90` | #957 | doc — labor × govcon crosswalk |
| `5f8fe73` | #958 | Cycle 1 — recursive IDV rollup + `Y` diagnosis |
| `a47101e` | #959 | Cycle 2 doc — per-agency calibration |
| `4d2450d` | #960 | Cycle 2.5 — `awarding_agency_code` + `award_pool` on `mod_delta` (code) |
| `68ab920` | #961 | doc — Cycle 2.5 ship-blocker resolved |
| `6fcd84f` | #962 | doc — entity-dimension (SCD2) decision |
| `0e6e6bf` | #963 | fix — `fpds_action_type_ref` verbatim from DEC, fix `Y` |

### Live dataset facts (probed 2026-07-04, `s3://data-sink/active/`)

| dataset | rows | cols | idx | Lance version |
|---|--:|--:|--:|--:|
| `usaspending_fpds_prime_award_state` | 82,868,654 | 43 | 21 | v22 |
| `usaspending_fpds_mod_delta` | 25,017,209 | 31 | 14 | v15 |
| `fpds_action_type_ref` | 21 | 5 | 1 | v6 |

Verified spot-checks: `award_kind` = definitive 17,068,433 / idv 990,041 / order 64,810,180; dangling ~0.61%; recon_delta p99 = $0; `mod_delta.award_pool` = parent 3,554,249 / child 21,462,960; `Y`-row `action_type_klass` = `nonstandard` (8,736 rows, live — the open item); `fpds_action_type_ref` `Y = "ADD SUBCONTRACT PLAN"`, `V = "UNIQUE ENTITY ID (DUNS) OR LEGAL BUSINESS NAME CHANGE - NON-NOVATION"`, `W = "ENTITY ADDRESS CHANGE"`.

### Operational note

This repo runs many concurrent agent worktrees on one shared checkout; this workstream's commits were made from isolated `git worktree`s off `main` to avoid disturbing concurrent agents. Long Modal builds run `--detach`; completion is tracked by the two-source AND sentinel (Modal app state + fresh `status='success'` ledger row), never a held client.
