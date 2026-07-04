# USAspending FPDS L2 Satellites — What Landed, What It Unlocks, and How to Join It

**Status:** live, verified, on `main` (`df96557`). Build 2026-07-04 03:45→04:15 UTC, detached Modal run, two-source sentinel PASS, both read-back `verify` PASS.

This document covers (1) the two L2 satellite tables just built over the FPDS L1 spine, (2) the full USAspending Lance constellation they plug into — including the **subaward canonical spine** and the **award_search BULK Lance** — and (3) precise, runnable **DuckDB-over-Lance** join/union recipes that turn these separate datasets into one queryable graph.

---

## 1. What was built

Two derived, index-served **access-path materialized views** over the L1 spine `usaspending_fpds_canonical_txn` (108M×392 transaction ledger), produced by **one shared award-partitioned DuckDB window pass** — the 108M-row external sort is paid once and consumed two ways.

### `usaspending_fpds_prime_award_state` — capacity / starvation (award grain)
- **URI:** `s3://data-sink/active/usaspending_fpds_prime_award_state/`
- **Grain / PK:** one row per `contract_award_unique_key` — a **prime-award root**. 82,868,654 rows, 43 cols, 21 indices.
- **Ternary `award_kind`:** `definitive` (17,068,433) · `idv` (990,041) · `order` (64,810,180). Lets the origination engine separate a definitive recompete from a parent-IDV ceiling check.
- **What each row answers:** how much has been obligated life-to-date, what the current-authorization and potential (max) ceiling are, how much headroom remains, what % consumed, when it expires, and — for IDVs — the rolled-up obligation of all child orders.
- **Load-bearing columns:** `life_to_date_obligated`, `current_authorized_ceiling`, `potential_ceiling`, `remaining_ceiling_headroom`, `consumed_pct`, `current_end_date`, `days_to_expiry`, `is_expired_no_followon`, `is_terminated`, `terminal_action_type_code`, `idv_child_obligated`, `has_child_idv`, `parent_award_key_resolved`, `parent_match_flag`.

### `usaspending_fpds_mod_delta` — kinetic events (modifying-transaction grain)
- **URI:** `s3://data-sink/active/usaspending_fpds_mod_delta/`
- **Grain / PK:** one row per **modifying** `contract_transaction_unique_key` (base/new-award rows excluded). 25,017,209 rows, 29 cols, 12 indices.
- **What each row answers:** the mathematical first-difference this modification caused — Δceiling, Δobligation, Δexpiry-days, identity-boundary crossings — classified by `action_type_klass` (`scope_change` 7.03M · `funding_only` 5.31M · `admin` 9.43M · `option_exercise` 2.25M · `termination` 804k · `identity_boundary` 184k · `unclassified` 8.7k).
- **Load-bearing columns:** `delta_potential_ceiling`, `delta_authorized_ceiling`, `delta_base_and_all_options`, `delta_federal_action_obligation`, `delta_current_end_date_days`, `is_scope_increase`, `is_termination_event`, `identity_changed`, `identity_change_fields`, `prev_recipient_uei`, `action_type_klass`.

### Verified build metrics
| | prime_award_state | mod_delta |
|---|---|---|
| rows | 82,868,654 | 25,017,209 |
| parent resolution | self 18.02M · resolved 64.45M · **dangling 0.61%** | — |
| recon `p99` \|Σfao − cumulative\| | **$0.00** (SUM discipline holds) | — |
| `potential_delta_null_rate` | — | 45.7% (FRESH-winner BULK-only; falls back to core ceiling deltas) |
| distinct awards touched | — | 9,265,024 |

---

## 2. The USAspending Lance constellation

Every prime-award dataset keys on the **same value** — USAspending's Broker-generated unique award id (`CONT_AWD_<piid>_<agency>_<parentpiid>_<parentagency>` / `CONT_IDV_<piid>_<agency>`) — but **under different column names**. This is the spine of the join graph.

| dataset | URI (`s3://data-sink/active/…`) | grain | primary key | rows | cols/idx | prime-award key column | entity key | freshness |
|---|---|---|---|---|--:|---|---|---|
| **L1 FPDS spine** | `usaspending_fpds_canonical_txn/` | transaction | `contract_transaction_unique_key` | 107,962,341 | 392 / 18 | `contract_award_unique_key` | `recipient_uei` | BULK∪FRESH reconciled, manifest v19 |
| **L2 prime_award_state** | `usaspending_fpds_prime_award_state/` | prime award | `contract_award_unique_key` | 82,868,654 | 43 / 21 | `contract_award_unique_key` | `recipient_uei` | derived from L1 (build_date 2026-07-04) |
| **L2 mod_delta** | `usaspending_fpds_mod_delta/` | modifying txn | `contract_transaction_unique_key` | 25,017,209 | 31 / 14 | `contract_award_unique_key` · `awarding_agency_code` (CGAC) · `award_pool` | `recipient_uei` | derived from L1 |
| **award_search (BULK)** | `usaspending/award_search/` | prime award | `generated_unique_award_id` | 78,636,657 | 154 / 3 | `generated_unique_award_id` | `recipient_uei` | **BULK pg_dump snapshot 2026-05-06 — NOT reconciled with live API** |
| **subaward_canonical** | `usaspending_subaward_canonical/` | subaward | `(prime_award_unique_key, subaward_number)` | 1,315,680 | 258 / 30 | `prime_award_unique_key` | `subawardee_uei` / `prime_awardee_uei` | BULK∪FRESH reconciled, contract-only |
| **fpds_action_type_ref** | `fpds_action_type_ref/` | action code | `action_type_code` | 20 | 5 / 1 | — (`action_type_code`) | — | static dim |

### The universal keys
- **Prime award:** `contract_award_unique_key` **≡** `generated_unique_award_id` **≡** `prime_award_unique_key` **≡** `unique_award_key`. Same Broker value space; only the column name differs by dataset. (Contract keys are `CONT_*`; joining naturally intersects only shared contract awards.)
- **Entity (UEI):** `recipient_uei` (spine / state / delta / award_search) is the awardee. On subawards, `prime_awardee_uei` is that same prime awardee, and `subawardee_uei` is the *sub*.
- **Transaction:** `contract_transaction_unique_key` (L1 ⋈ L2 mod_delta, 1:1).
- **Action code:** `action_type_code` (L2 mod_delta / L1 ⋈ `fpds_action_type_ref`, N:1).

### Two award-grain views, deliberately different
There are now **two** award-grain rollups keyed on the same award id, and they are complementary, not redundant:
- **`prime_award_state`** — *bottom-up, fresh:* computed from the FPDS **transaction** spine (BULK∪FRESH), so it reflects the newest corrections, and it carries capacity math the government's rollup does not (potential-ceiling headroom, IDV→child rollup, expiry horizon, `award_kind`).
- **`award_search` (BULK)** — *top-down, stale:* USAspending's own award-level rollup as of the **2026-05-06 pg snapshot**, carrying 154 award-level columns (e.g. `total_obligation`, recipient rollups, CFDA/program metadata) — but **not** reconciled against the live API. Use it for award-level metadata and as a **cross-check**, never as the freshness source. (`award_search_merged`, the coded bulk+delta reconcile in `usaspending_award_search_reconcile.py`, is **not yet materialized** — the merged URI 404s.)

---

## 3. What this unlocks — concretely

The L1 spine answered *"what did the government record"* at transaction grain — useless for live operational questions without a full-table window every time. The L2 satellites + the constellation make these **index-served** and **joinable**:

1. **Capacity starvation, instantly.** "Which live awards/IDVs are >85% consumed and expire within 90 days, in NAICS 5415, held by a small business?" — a BTREE range-scan on `consumed_pct` × `current_end_date` composed with BITMAP facets on `prime_award_state`. No 108M window.
2. **Vehicle-level ceiling checks.** For an IDV, `idv_child_obligated / potential_ceiling` gives true vehicle burn (child-order rollup already computed), and `has_child_idv` flags multi-tier vehicles whose denominator is a lower bound.
3. **Kinetic events, instantly.** "Which awards took a >$5M ceiling jump or a novation or a termination in the last N days?" — a range-scan on `mod_delta.action_date` × `delta_potential_ceiling` × flag bitsets.
4. **Prime → sub reach.** Join `prime_award_state` → `subaward_canonical` on the prime key to see *who a starving/kinetic prime subcontracts to* — the teaming/origination surface (recipe §4.5).
5. **Entity portfolios.** Group `prime_award_state` (and `mod_delta`) by `recipient_uei` for a company's full capacity book and recent kinetic activity in one scan (recipe §4.6).
6. **Cross-source reconciliation.** Compare my bottom-up `life_to_date_obligated` against award_search's top-down `total_obligation` on the shared key to flag data drift or stale awards (recipe §4.7).
7. **Total federal exposure (prime + sub).** Union grain-safe amounts to size a company's full footprint as prime *and* sub (recipe §4.8).

---

## 4. How the unlock works — DuckDB-over-Lance recipes

### 4.0 Boilerplate (all recipes assume this)
DuckDB reads Lance out-of-core; a Lance **`scanner(columns=…, filter=…)`** pushes the projection and predicate down to the dataset's **scalar indices** before anything materializes. The discipline is **filter-then-join**: push the selective predicate into each side's scanner so only a small set crosses into DuckDB, then join the reduced tables.

```python
import os, duckdb, lance
os.environ.setdefault("LANCE_BYPASS_SPILLING", "true")          # before any lance call
so = {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
      "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
      "endpoint": os.environ["R2_ENDPOINT"], "region": "auto"}
A = "s3://data-sink/active"
con = duckdb.connect(); con.execute("SET memory_limit='8GB'")

def reg(name, uri, columns=None, filter=None):
    """Register a pushdown-filtered Lance scan as a DuckDB table."""
    ds = lance.dataset(uri, storage_options=so)
    con.register(name, ds.scanner(columns=columns, filter=filter).to_table())
```
> **Pushdown filters** use the scalar-index columns. On `prime_award_state`: BTREE `consumed_pct`, `current_end_date`, `days_to_expiry`, `remaining_ceiling_headroom`, `contract_award_unique_key`, `recipient_uei`, `parent_award_key_resolved`, `award_id_piid`, `solicitation_identifier`; BITMAP `award_kind`, `awarding_agency_code`, `type_of_set_aside_code`, `award_type_code`, `is_terminated`, `is_expired_no_followon`, `parent_match_flag`, `has_child_idv`. On `mod_delta`: BTREE `action_date`, `delta_potential_ceiling`, `delta_federal_action_obligation`, `contract_award_unique_key`, `recipient_uei`; BITMAP `action_type_code`, `award_kind`, `action_type_klass`, `is_scope_increase`, `is_termination_event`, `identity_changed`. Filter on these; everything else is a full column scan.

### 4.1 Capacity starvation (single table, index pushdown)
```python
reg("st", f"{A}/usaspending_fpds_prime_award_state/",
    columns=["contract_award_unique_key","award_kind","recipient_uei","naics_code",
             "consumed_pct","days_to_expiry","remaining_ceiling_headroom",
             "type_of_set_aside_code","awarding_agency_code"],
    filter="consumed_pct > 0.85 AND consumed_pct <= 5 "     # <=5 drops data-error outliers
           "AND days_to_expiry BETWEEN 0 AND 90 "
           "AND is_terminated = false AND award_kind = 'definitive'")
con.sql("""
  SELECT naics_code, count(*) starving, round(sum(remaining_ceiling_headroom)/1e6,1) headroom_M
  FROM st WHERE type_of_set_aside_code IS NOT NULL
  GROUP BY 1 ORDER BY 2 DESC LIMIT 25
""").show()
```

### 4.2 Kinetic events (delta ⋈ action_type_ref for the human label)
```python
reg("dl", f"{A}/usaspending_fpds_mod_delta/",
    columns=["contract_award_unique_key","recipient_uei","action_type_code","action_type_klass",
             "action_date","delta_potential_ceiling","delta_federal_action_obligation","is_scope_increase"],
    filter="action_date >= '2026-04-01' AND is_scope_increase = true "
           "AND delta_potential_ceiling > 5000000")
reg("atr", f"{A}/fpds_action_type_ref/")   # 20-row dim, no filter
con.sql("""
  SELECT d.action_date, d.recipient_uei, r.action_type_description,
         round(d.delta_potential_ceiling/1e6,1) ceiling_jump_M
  FROM dl d LEFT JOIN atr r USING (action_type_code)
  ORDER BY d.delta_potential_ceiling DESC LIMIT 50
""").show()
```

### 4.3 An award ⋈ its full modification history (state ⋈ delta, 1:N)
```python
reg("st", f"{A}/usaspending_fpds_prime_award_state/",
    columns=["contract_award_unique_key","recipient_uei","potential_ceiling","consumed_pct","current_end_date"],
    filter="consumed_pct > 0.9 AND days_to_expiry BETWEEN 0 AND 60")
# pull only the mods for those awards — pass the key set as the pushdown predicate
keys = [r[0] for r in con.sql("SELECT contract_award_unique_key FROM st").fetchall()]
inlist = ",".join("'" + k.replace("'","''") + "'" for k in keys[:5000])   # batch large key sets
reg("dl", f"{A}/usaspending_fpds_mod_delta/",
    columns=["contract_award_unique_key","action_date","action_type_klass",
             "delta_potential_ceiling","delta_federal_action_obligation"],
    filter=f"contract_award_unique_key IN ({inlist})")
con.sql("""
  SELECT s.contract_award_unique_key, s.consumed_pct, s.current_end_date,
         count(d.*) mods, round(sum(d.delta_federal_action_obligation)/1e6,2) net_moved_M
  FROM st s LEFT JOIN dl d USING (contract_award_unique_key)
  GROUP BY 1,2,3 ORDER BY 2 DESC
""").show()
```
> For very large key sets, register `st` and `dl` fully-filtered and join in DuckDB instead of an `IN (...)` list — `IN` lists are best under a few thousand keys.

### 4.4 IDV vehicle burn with the multi-tier caveat surfaced
```python
reg("idv", f"{A}/usaspending_fpds_prime_award_state/",
    columns=["contract_award_unique_key","idv_type_code","potential_ceiling",
             "idv_child_obligated","idv_child_order_count","consumed_pct","has_child_idv","current_end_date"],
    filter="award_kind = 'idv' AND consumed_pct > 0.7")
con.sql("""
  SELECT idv_type_code, count(*) vehicles,
         count(*) FILTER (WHERE has_child_idv) multitier_lowerbound,   -- denominator is partial here
         round(avg(consumed_pct),3) avg_burn
  FROM idv GROUP BY 1 ORDER BY 2 DESC
""").show()
```

### 4.5 Prime capacity ⋈ subaward footprint (state ⋈ subaward, key-name bridge)
The join key differs by name — bridge it explicitly: `state.contract_award_unique_key = subaward.prime_award_unique_key`.
```python
reg("st", f"{A}/usaspending_fpds_prime_award_state/",
    columns=["contract_award_unique_key","recipient_uei","naics_code","consumed_pct","days_to_expiry"],
    filter="consumed_pct > 0.8 AND days_to_expiry BETWEEN 0 AND 180 AND award_kind='definitive'")
reg("sub", f"{A}/usaspending_subaward_canonical/",
    columns=["prime_award_unique_key","subawardee_uei","subawardee_name","subaward_amount","subaward_action_date"])
con.sql("""
  SELECT s.contract_award_unique_key, s.recipient_uei prime_uei, s.consumed_pct,
         count(DISTINCT sub.subawardee_uei) n_subs,
         round(sum(sub.subaward_amount)/1e6,2) subcontracted_M   -- subaward_amount IS sub-grain-safe to SUM
  FROM st s JOIN sub ON sub.prime_award_unique_key = s.contract_award_unique_key
  GROUP BY 1,2,3 HAVING n_subs > 0 ORDER BY subcontracted_M DESC LIMIT 50
""").show()
```
> Signal: a prime that is **starving** *and* subcontracts heavily is a teaming/displacement target — the incumbent's subs are pre-qualified performers on that scope.

### 4.6 Entity portfolio — a company's whole capacity book (group by UEI)
```python
reg("st", f"{A}/usaspending_fpds_prime_award_state/",
    columns=["recipient_uei","award_kind","life_to_date_obligated","remaining_ceiling_headroom",
             "consumed_pct","days_to_expiry","is_terminated"],
    filter="recipient_uei = 'ABC123DEF456'")     # BTREE point-lookup
con.sql("""
  SELECT award_kind, count(*) awards,
         round(sum(life_to_date_obligated)/1e6,1) obligated_M,
         round(sum(remaining_ceiling_headroom) FILTER (WHERE NOT is_terminated)/1e6,1) live_headroom_M,
         count(*) FILTER (WHERE consumed_pct>0.85 AND days_to_expiry BETWEEN 0 AND 120) recompete_soon
  FROM st GROUP BY 1
""").show()
```

### 4.7 Cross-source reconciliation (state ⋈ award_search, bottom-up vs top-down)
```python
reg("st", f"{A}/usaspending_fpds_prime_award_state/",
    columns=["contract_award_unique_key","life_to_date_obligated","award_kind"],
    filter="award_kind='definitive' AND life_to_date_obligated > 1000000")
reg("aw", f"{A}/usaspending/award_search/",
    columns=["generated_unique_award_id","total_obligation"])
con.sql("""
  SELECT count(*) matched,
         count(*) FILTER (WHERE abs(s.life_to_date_obligated - a.total_obligation) > 1000) drift_gt_1k,
         round(quantile_cont(abs(s.life_to_date_obligated - a.total_obligation), 0.99),0) p99_abs_drift
  FROM st s JOIN aw a ON a.generated_unique_award_id = s.contract_award_unique_key
""").show()
```
> Expect drift on awards modified after the **2026-05-06** award_search snapshot (my spine is fresher). Large drift on an *old* award is a data-quality flag; small drift confirms the SUM discipline end-to-end.

### 4.8 Total federal exposure — UNION prime + sub (grain-safe amounts)
Combine two different grains into one exposure ledger. Only **grain-safe** amounts may be summed: `federal_action_obligation`-derived `life_to_date_obligated` at award grain, `subaward_amount` at sub grain. Never sum a cumulative `*_value` or an award-repeated field.
```python
reg("st", f"{A}/usaspending_fpds_prime_award_state/",
    columns=["recipient_uei","life_to_date_obligated"], filter="recipient_uei='ABC123DEF456'")
reg("sub", f"{A}/usaspending_subaward_canonical/",
    columns=["subawardee_uei","subaward_amount"], filter="subawardee_uei='ABC123DEF456'")
con.sql("""
  SELECT 'prime' role, round(sum(life_to_date_obligated)/1e6,1) dollars_M FROM st
  UNION ALL
  SELECT 'sub'  role, round(sum(subaward_amount)/1e6,1)          dollars_M FROM sub
""").show()
```

---

## 5. Grain hazards & correctness rules (read before writing any query)

- **Only three amount columns are safe to `SUM`:** `prime_award_state.life_to_date_obligated` (award grain), `mod_delta.delta_federal_action_obligation` (mod grain), `subaward_canonical.subaward_amount` (sub grain). Every `*_value` / `total_*` / ceiling column is a **cumulative snapshot** — use `MAX`/`ANY`, never `SUM` (summing multiplies by ladder length).
- **Never sum award-repeated fields at sub grain.** `subaward_canonical` carries `prime_award_*` context columns repeated across every sub of a prime — dedup to `prime_award_unique_key` (`MAX`/`ANY`) before aggregating, or you multiply by the sub count. The subaward dict flags `prime_award_amount` as the highest blast-radius trap.
- **The prime key is one value under four names.** `contract_award_unique_key` = `generated_unique_award_id` = `prime_award_unique_key` = `unique_award_key`. Always bridge names explicitly in the `ON` clause. They are byte-identical Broker keys; if a join under-matches, check for whitespace/case drift (the spine is trim-only, case-preserving).
- **`award_search` is stale (2026-05-06) and bulk-only.** Use it for award-level *metadata* and *cross-checks*, not freshness. For current capacity/kinetics, use the L1-derived L2 tables.
- **`delta_potential_ceiling` is NULL 45.7% of the time** (BULK-only field, NULL on FRESH-winning rows). For "did the ceiling grow" use `is_scope_increase` (already falls back to the core `base_and_all_options` delta), not the raw `delta_potential_ceiling`.
- **`consumed_pct` is unclamped:** `>1.0` = over-ceiling / data issue, `<0` = net de-obligated — both legitimate signals; filter with an upper bound (`<= 5`) when you want the clean cohort.
- **IDV denominators can be partial:** when `has_child_idv = true`, `idv_child_obligated` is a lower bound (one-level rollup); exclude or flag those rows in a starvation ranking.
- **`max_current_end_date = 9999-12-31`** is FPDS's open-ended placeholder; those rows carry a huge `days_to_expiry` and won't false-positive in near-expiry filters.

---

## 6. Freshness, rebuild, and lineage

| dataset | refresh | watermark / ledger |
|---|---|---|
| L1 FPDS spine | overwrite rebuild (BULK∪FRESH reconcile) | `ops.usaspending_fpds_canonical_runs` |
| L2 prime_award_state / mod_delta | overwrite rebuild, **rerun after each L1 advance** (`modal run --detach …_l2_modal.py::build`) | `ops.usaspending_fpds_prime_award_state_runs`, `ops.usaspending_fpds_mod_delta_runs` (one row/table/build; `spine_manifest_version` ties to L1) |
| award_search (BULK) | static pg_dump snapshot 2026-05-06 | — (merged reconcile coded, not built) |
| subaward_canonical | overwrite rebuild (BULK∪FRESH) | `ops.usaspending_fpds_subaward_canonical_runs` |

L2 rebuild is **detach-safe** (overwrite + `retries=0`): launch `modal run --detach`, gate completion on the **two-source AND sentinel** (Modal app state + fresh `status='success'` ledger rows), never a held process. Both tables carry `spine_manifest_version` so you can confirm they were built against the current L1 (compare to the spine's live `version`).

---

## 7. Open follow-ups (documented, none blocking)

1. **Entity dimension (SCD2 on `recipient_uei`).** The still-un-built normalization play: a UEI-keyed slowly-changing dimension for the family-② entity attributes (address/geo/contact/org-class) the L1 spine denormalizes across 108M rows. `mod_delta.identity_changed` / `prev_recipient_uei` are the ready-made version-cut signals.
2. **`award_search_merged` reconcile** is coded (`usaspending_award_search_reconcile.py`) but not materialized — running it would give a live-reconciled award-grain view to replace the stale BULK snapshot in the cross-checks above.
3. **Recursive IDV rollup** — the one-level `idv_child_obligated` under-counts multi-tier vehicles (flagged by `has_child_idv`); a recursive fold over `parent_award_key_resolved` would make ancestor denominators exact.
4. **`action_type='Y'`** (8,736 rows → `action_type_klass='unclassified'`) is undiagnosed; deltas are still computed, so it's non-blocking.
5. **Civilian-agency threshold validation** — the numeric alert cutoffs a consumer sets on these columns (not the `action_type_klass` labels, which are FAR-uniform) were premised on an Air-Force footprint; validate cutoffs against a civilian agency before wiring alerts.
