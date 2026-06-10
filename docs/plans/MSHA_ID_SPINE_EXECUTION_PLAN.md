# MSHA ID Spine — Execution Plan

> Deterministic core-`_ID` join tissue across all 17 active MSHA Lance datasets.
> **Status:** PLAN (read-only investigation complete; zero mutations performed).
> **Scope:** the deterministic spine only — `MINE_ID` (site), `CONTROLLER_ID`+`OPERATOR_ID`
> (entity), `VIOLATOR_ID` (cited party), `CONTRACTOR_ID` (third spine), and the within-tier
> link keys `EVENT_NO` / `VIOLATION_NO`·`CITATION_NO` / `DOCKET_NO` / `ISSUANCE_NO`. External
> entity resolution (MSHA → `companies` / `sos_normalized_master` / PPP / `name_norm` bridges)
> is **explicitly deferred** — see §11.

## 0. Attestation & probe provenance

Every count, column, index state, and resolution rate below is a **live read of
`s3://data-sink/active/` on 2026-06-10**, via the canonical harness:

```
doppler run -p core-x -c prd -- uv run --no-project --with pylance --with duckdb --with pyarrow python3 /tmp/<probe>.py
```

with `lance.dataset(uri, storage_options=so)` → `count_rows()`, `list_indices()`,
`scanner(columns=[…]).to_table()` projected into DuckDB for join/aggregation. Six probes
produced the evidence (inventory, MINE_ID resolution + hygiene, entity/contractor
resolution, hygiene drill-down, link keys, SCD determinism + mirror typing + cohorts). Where
a number is asserted, the **producing probe is named inline**. No web, no landing zone, no
memory — per the operator's rule: *"if it exists in landing but not in active … it is
equivalent to not existing for us right now."*

**Two corrections to the grounding the operator supplied** (both verified, both material):
1. **CONTROLLER_ID on enforcement is 99.99% *resolved*, not 93.25%.** The 93.25% figure in
   `MSHA_LANCE_STATE_DIAGNOSTIC.md` is *fill* (208K rows carry NULL CONTROLLER_ID). Of the
   2,868,650 populated values, **2,868,360 (99.99%)** resolve to `msha_corporate_history`.
   Fill ≠ resolution; the spine cares about resolution-of-populated. (probe3)
2. **Contractor enforcement→registry resolution is 89.32%, not 73.0%.** Of 205,788 populated
   enforcement `CONTRACTOR_ID` rows, **183,815 (89.32%)** resolve to the contractor registry.
   The "cited contractor never filed production" gap is ~10.7%, not 27%. (probe3, probe4)

---

## 1. Executive summary

- The **site spine is already perfect and deterministic**: `MINE_ID` is a 7-char,
  zero-padded, all-digit VARCHAR with **0 whitespace / 0 non-digit drift in every dataset
  tested**, and **100.00% of non-null `MINE_ID` resolves to `msha_mines`** from all 14
  children probed (probe2). No work required beyond holding the line.
- The **entity spine resolves at 99.84–100.00%** (CONTROLLER_ID, OPERATOR_ID,
  Operator-typed VIOLATOR_ID — probe3). The only thing standing between "present" and
  "deterministic, index-backed" is **three present-but-unindexed keys** and **three
  lowercase ID cells**; both are tiny, surgical, and isolatable.
- The **SCD resolver is NOT yet deterministic.** `CONTROLLER_END_DT IS NULL` alone maps
  **16,739 mines to >1 current controller**; a fixed tie-break (latest `CONTROLLER_START_DT`
  wins) collapses all but **778 same-day ties** (probe6). The spine plan must *define* the
  deterministic predicate, not assume one exists.
- The **contractor third spine is real, rich, and queryable** — 38,653 distinct contractors
  in the registry, pivotable across enforcement (21,966), accidents (5,131), personal-health
  (970), noise (195), area (162), orders (720) — and **CONTRACTOR_ID is BTREE-indexed
  everywhere it appears EXCEPT `msha_enforcement_ledger`**, the single highest-value gap
  (probe1, probe4).
- The work splits into **one blast-radius-isolated index-hardening phase** (Phase 2 — close
  3 spine-key index gaps, zero data rewrite) and an **optional, separately-gated
  materialization** of a current-state `msha_site_master` anchor (Phase 5). Index hardening
  is highest-leverage / lowest-risk and goes first.
- **Recommendation: build `msha_site_master`** (one row per `MINE_ID`, current
  controller/operator/contractor IDs + pre-computed signal rollups) **after** the spine is
  hardened. It is the market-map anchor; it bakes the non-trivial SCD tie-break logic once so
  every downstream consumer inherits a deterministic answer instead of re-deriving it. (§5, §7)

---

## 2. The deterministic spine — end state (what exists when done)

When this plan completes, the following are **true and continuously asserted**:

1. **Site spine.** Every dataset carrying `MINE_ID` (15 of 17) joins to `msha_mines` on a
   BTREE-backed, format-clean `MINE_ID` at 100.00% of non-null — already true; locked by the
   integrity harness (§8).
2. **Entity spine.** `CONTROLLER_ID`, `OPERATOR_ID`, and `VIOLATOR_ID` are BTREE-indexed on
   **every** dataset where they appear (closes `orders_issued.CONTROLLER_ID_VIOLATIONS`), and
   the three lowercase ID cells are remediated or documented as accepted (§4, §6).
3. **Contractor third spine.** `CONTRACTOR_ID` is BTREE-indexed on **every** dataset where it
   appears (closes `enforcement.CONTRACTOR_ID`), making "pivot on one contractor across all
   its activity" an index-backed point query everywhere.
4. **Link keys.** `EVENT_NO`, `VIOLATION_NO`/`CITATION_NO`, `DOCKET_NO`, `ISSUANCE_NO` are
   BTREE-indexed wherever present (closes `enforcement.DOCKET_NO`).
5. **SCD resolver.** A documented, deterministic `MINE_ID @ date → (CONTROLLER_ID,
   OPERATOR_ID)` predicate with an explicit tie-break, either materialized into
   `msha_site_master` (Phase 5) or shipped as a canonical SQL contract (§6, §9).
6. **Join contract.** A published, versioned table (§9) of every spine edge: from-key →
   to-dataset.key → cardinality → index-backed → live resolution %.
7. **Integrity harness.** A read-only Modal verifier (`verify_spine`) that re-asserts every
   rate in §9 on demand and fails loudly on drift (§8).

**Non-goals (this plan):** no typed promotion of mirror datasets except where a spine key
needs it (none do — all spine keys are VARCHAR by design); no external bridge column; no
`name_norm` cross-universe join; no new ingest.

---

## 3. Grounded state — the two authoritative matrices

### 3.1 Inventory (live, probe1 — `count_rows()` + `schema`)

| # | dataset | rows | cols | tier |
|--:|---|--:|--:|---|
| 1 | `msha_mines` | 91,803 | 83 | curated |
| 2 | `msha_enforcement_ledger` | 3,076,347 | 122 | curated |
| 3 | `msha_accidents` | 273,065 | 61 | curated |
| 4 | `msha_corporate_history` | 168,809 | 17 | curated |
| 5 | `msha_contractors` | 1,630,676 | 19 | curated |
| 6 | `msha_inspections` | 1,147,232 | 47 | mirror |
| 7 | `msha_mines_prod_quarterly` | 2,714,840 | 15 | mirror |
| 8 | `msha_mines_prod_yearly` | 657,546 | 13 | mirror |
| 9 | `msha_coal_dust_samples` | 2,985,614 | 32 | mirror |
| 10 | `msha_contested_violations` | 448,158 | 41 | mirror |
| 11 | `msha_civil_penalty_dockets_decisions` | 479,439 | 31 | mirror |
| 12 | `msha_personal_health_samples` | 310,908 | 22 | mirror |
| 13 | `msha_noise_samples` | 274,645 | 31 | mirror |
| 14 | `msha_quartz_samples` | 167,238 | 21 | mirror |
| 15 | `msha_conferences` | 161,623 | 9 | mirror |
| 16 | `msha_area_samples` | 8,368 | 19 | mirror |
| 17 | `msha_orders_issued` | 3,830 | 15 | mirror |
| | **TOTAL** | **14,600,141** | | |

(Column counts exceed the older diagnostic — `msha_mines` 83 vs 80, enforcement 122 vs 120 —
because the schema-hardening PR's persisted `*_norm` siblings are now committed. Rows match
the grounding exactly.)

### 3.2 Key-presence × index matrix (live, probe1 — `schema.names` ∩ keys, `list_indices()`)

`I` = present **and** BTREE-indexed · `!` = present but **NOT indexed** (a Phase-2 target) ·
`.` = absent.

| dataset | MINE_ID | CONTROLLER_ID | OPERATOR_ID | VIOLATOR_ID | CONTRACTOR_ID | EVENT_NO | VIOL/CIT_NO | DOCKET_NO | ISSUANCE_NO | CONF_NO |
|---|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
| `msha_mines` | I | .¹ | .¹ | . | . | . | . | . | . | . |
| `msha_enforcement_ledger` | I | I | . | I | **!** | I | I | **!** | . | . |
| `msha_accidents` | I | I | I | . | I | . | . | . | . | . |
| `msha_corporate_history` | I | I | I | . | . | . | . | . | . | . |
| `msha_contractors` | . | . | . | . | I | . | . | . | . | . |
| `msha_inspections` | I | I | I | . | . | I | . | . | . | . |
| `msha_mines_prod_quarterly` | I | . | . | . | . | . | . | . | . | . |
| `msha_mines_prod_yearly` | I | . | . | . | . | . | . | . | . | . |
| `msha_coal_dust_samples` | I | . | . | . | . | .² | . | . | . | . |
| `msha_contested_violations` | I | . | . | . | . | . | I (CIT) | I | . | . |
| `msha_civil_penalty_dockets_decisions` | I | . | . | I | . | . | I (VIOL) | I | . | . |
| `msha_personal_health_samples` | I | . | . | . | I | I | . | . | . | . |
| `msha_noise_samples` | I | . | . | . | I | I | I (VIOL)³ | . | . | . |
| `msha_quartz_samples` | I | . | . | . | . | .² | . | . | . | . |
| `msha_conferences` | . | . | . | . | . | . | . | . | I | I |
| `msha_area_samples` | I | . | . | . | I | I | . | . | . | . |
| `msha_orders_issued` | I | **!**⁴ | . | . | I | . | I (VIOL) | . | . | . |

¹ `msha_mines` carries `CURRENT_CONTROLLER_ID` / `CURRENT_OPERATOR_ID` (the denormalized
current state) — **both ARE BTREE-indexed** (probe1); shown `.` here only because the column
name differs from the bare spine key. They are first-class indexed entity keys.
² `coal_dust` / `quartz` carry **no `EVENT_NO`** — their only enforcement bridge is `MINE_ID`
(+ `CASS_NUM` / `CASSETTE_NO` as the within-set sample key). (probe6)
³ `noise_samples.VIOLATION_NO` is indexed but is a **survey-form number, not a citation** —
it resolves to enforcement at only **2.36%** (probe5). Do **not** join samples to enforcement
on `VIOLATION_NO`; use `EVENT_NO`. Flagged as a determinism trap, §10.
⁴ `orders_issued` uses the suffixed name `CONTROLLER_ID_VIOLATIONS` (the OrdersIssued Excel
export's column). Present, **NOT indexed** — Phase-2 target.

**The three present-but-unindexed spine keys (the entire Phase-2 surface):**
`msha_enforcement_ledger.CONTRACTOR_ID` · `msha_enforcement_ledger.DOCKET_NO` ·
`msha_orders_issued.CONTROLLER_ID_VIOLATIONS`.

### 3.3 Full committed index manifest (live, probe1)

| dataset | committed scalar indices (field(type)) |
|---|---|
| `msha_mines` | MINE_ID(B), CURRENT_CONTROLLER_ID(B), CURRENT_OPERATOR_ID(B), CURRENT_OPERATOR_NAME(B), CURRENT_CONTROLLER_NAME(B), BUSINESS_NAME(B), ZIP_CD(B), CURRENT_OPERATOR_NAME_norm(B), CURRENT_CONTROLLER_NAME_norm(B), BUSINESS_NAME_norm(B), COAL_METAL_IND(bit), STATE(bit), CURRENT_MINE_STATUS(bit) — **13** |
| `msha_enforcement_ledger` | MINE_ID(B), VIOLATOR_ID(B), VIOLATION_NO(B), CONTROLLER_ID(B), EVENT_NO(B), ASSESS_CASE_NO(B), VIOLATION_ISSUE_DT(B), PROPOSED_PENALTY_AMT(B), VIOLATOR_NAME(B), CONTROLLER_NAME(B), VIOLATOR_NAME_norm(B), CONTROLLER_NAME_norm(B), SIG_SUB(bit), CIT_ORD_SAFE(bit), VIOLATOR_TYPE_CD(bit), COAL_METAL_IND(bit) — **16** |
| `msha_accidents` | DOCUMENT_NO(B), MINE_ID(B), CONTROLLER_ID(B), OPERATOR_ID(B), CONTRACTOR_ID(B), ACCIDENT_DT(B), CONTROLLER_NAME(B), OPERATOR_NAME(B), CONTROLLER_NAME_norm(B), OPERATOR_NAME_norm(B), DEGREE_INJURY_CD(bit), CLASSIFICATION_CD(bit), ACCIDENT_TYPE_CD(bit), FIPS_STATE_CD(bit), COAL_METAL_IND(bit) — **15** |
| `msha_corporate_history` | CONTROLLER_ID(B), OPERATOR_ID(B), MINE_ID(B), OPERATOR_NAME(B), CONTROLLER_NAME(B), OPERATOR_NAME_norm(B), CONTROLLER_NAME_norm(B), CONTROLLER_TYPE(bit), COAL_METAL_IND(bit) — **9** |
| `msha_contractors` | CONTRACTOR_ID(B), CONTRACTOR_NAME(B), CONTRACTOR_NAME_norm(B), COAL_METAL_IND(bit), SUBUNIT_CD(bit) — **5** |
| `msha_inspections` | MINE_ID(B), EVENT_NO(B), CONTROLLER_ID(B), OPERATOR_ID(B) — **4** |
| `msha_mines_prod_quarterly` | MINE_ID(B) — **1** |
| `msha_mines_prod_yearly` | MINE_ID(B) — **1** |
| `msha_coal_dust_samples` | MINE_ID(B), CASS_NUM(B) — **2** |
| `msha_contested_violations` | MINE_ID(B), CITATION_NO(B), DOCKET_NO(B) — **3** |
| `msha_civil_penalty_dockets_decisions` | MINE_ID(B), VIOLATION_NO(B), VIOLATOR_ID(B), ASSESS_CASE_NO(B), DOCKET_NO(B) — **5** |
| `msha_personal_health_samples` | MINE_ID(B), EVENT_NO(B), SAMPLE_NO(B), CONTRACTOR_ID(B) — **4** |
| `msha_noise_samples` | MINE_ID(B), EVENT_NO(B), VIOLATION_NO(B), CONTRACTOR_ID(B) — **4** |
| `msha_quartz_samples` | MINE_ID(B), LABORATORY_NO(B) — **2** |
| `msha_conferences` | CONFERENCE_NO(B), ISSUANCE_NO(B) — **2** |
| `msha_area_samples` | MINE_ID(B), EVENT_NO(B), SAMPLE_NO(B), CONTRACTOR_ID(B) — **4** |
| `msha_orders_issued` | MINE_ID(B), VIOLATION_NO(B), CONTRACTOR_ID(B) — **3** |

The mirror auto-BTREE worked exactly as designed: `MIRROR_KEY_COLS` ∩ present-columns
produced the right indices on every mirror — **except** it does not include
`CONTROLLER_ID_VIOLATIONS` (the suffixed OrdersIssued name), which is why
`orders_issued.CONTROLLER_ID_VIOLATIONS` is unindexed. (Fix lives in Phase 2 + a one-line
`MIRROR_KEY_COLS` addition for future re-mirrors.)

---

## 4. Determinism — proven, and the exact residual risks

### 4.1 What is already deterministic (no work)

| Spine edge | Denominator | Live resolution | Probe |
|---|---|--:|:-:|
| `MINE_ID` → `msha_mines` (all 14 children) | non-null child MINE_ID | **100.00%** every set | probe2 |
| `CONTROLLER_ID` → `corp_history` (mines.CURRENT) | populated | **99.84%** | probe3 |
| `CONTROLLER_ID` → `corp_history` (enforcement) | populated | **99.99%** | probe3 |
| `CONTROLLER_ID` → `corp_history` (inspections) | populated | **99.99%** | probe3 |
| `CONTROLLER_ID` → `corp_history` (accidents) | populated | **100.00%** | probe3 |
| `OPERATOR_ID` → `corp_history` (mines.CURRENT) | populated | **99.86%** | probe3 |
| `OPERATOR_ID` → `corp_history` (inspections / accidents) | populated | **99.99% / 100.00%** | probe3 |
| `VIOLATOR_ID` (type=Operator) → `corp.OPERATOR_ID` | populated | **99.99%** | probe3 |
| `EVENT_NO` enforcement → inspections | populated | **100.00%** | probe5 |
| `EVENT_NO` personal-health / noise / area → inspections | populated | **99.79 / 99.98 / 99.94%** | probe5 |
| `CITATION_NO` contested → enforcement.VIOLATION_NO | populated | **94.96%** | probe5 |
| `VIOLATION_NO` dockets → enforcement | populated | **94.42%** | probe5 |
| `VIOLATION_NO` orders → enforcement | populated | **99.92%** | probe5 |
| `DOCKET_NO` contested → dockets | populated | **99.57%** | probe5 |
| `CONTRACTOR_ID` accidents → registry | populated | **99.81%** | probe3 |
| `CONTRACTOR_ID` enforcement → registry | populated | **89.32%** | probe3/4 |

**Format hygiene (probe2/probe3/probe4):** `MINE_ID` is uniformly 7-char zero-padded digits,
0 drift. `CONTROLLER_ID`/`OPERATOR_ID`/`CONTRACTOR_ID` carry **0 whitespace** in every
dataset (the mirror's `_base_expr` already strips wrapping quotes + surrounding whitespace).
The contractor registry is uniformly alpha-prefixed (`1AD`, `1AF`, …), lengths 3–5.

### 4.2 Residual determinism risks — the operator must decide on each

| # | Risk | Blast radius (live) | Recommended disposition |
|--:|---|---|---|
| **R1** | **SCD is not single-valued.** `CONTROLLER_END_DT IS NULL` maps **16,739 mines** to >1 distinct current controller; as-of 2025-01-01, 16,619 ambiguous. (probe6) | structural, affects every site↔entity rollup | **Define the deterministic predicate**: latest `CONTROLLER_START_DT` wins within the matching window. This collapses all but **778** mines (§6). Bake into `msha_site_master` (Phase 5) or the join contract (§9). |
| **R2** | **778 same-day SCD ties** remain after latest-start-wins (multiple controllers, identical `CONTROLLER_START_DT`). (probe6) | 778 of 90,690 mines (0.86%) | Add a deterministic final tie-break: `min(CONTROLLER_ID)` lexical. Arbitrary but **stable + reproducible** — the spine's contract is determinism, not business-correctness of the pick. Flag those 778 in a `multi_controller_flag` column. |
| **R3** | **1,113 mines have no current-controller row at all** (1,104 with zero corp rows + 9 with corp rows but none `END_DT IS NULL`). (probe2/probe6) | 1,113 of 91,803 (1.2%) | LEFT JOIN semantics → NULL controller, retained in `msha_site_master`. Never an inner join that silently drops them. |
| **R4** | **3 lowercase ID cells**: `enf` `f466`/`m837` (in both CONTRACTOR_ID and VIOLATOR_ID, 1 row each), `acc` `4kk`. `UPPER('f466')='F466'` and `UPPER('m837')='M837'` **are in the registry**; `4KK` is not. (probe4) | 3 rows across 5.9M | The mirror does not upper-case. **Decision:** either (a) accept (5 rows, documented), or (b) add `upper()` to the contractor/violator key projection in the curated worker — recovers 2 of 3. Recommend (b) for the curated `enforcement`/`accidents` sets (they are typed workers already), (a) is acceptable interim. |
| **R5** | **10 malformed enforcement CONTRACTOR_ID rows** (all-numeric, e.g. `0113025`; 5 distinct) resolve at 0% — a different ID scheme leaked into the column. (probe4) | 10 rows of 205,788 (0.005%) | Document as known source defect; not worth a special-case. They simply fail the registry join (LEFT JOIN → NULL). The 89.32% headline is **overwhelmingly "cited contractor never filed production," not format breakage** — only these 10 rows are format-broken. |
| **R6** | **`noise_samples.VIOLATION_NO` ≠ citation** (2.36% resolve). (probe5) | join-trap, not data defect | Documentation: the sample→enforcement bridge is **EVENT_NO**, never `VIOLATION_NO`. Encoded in the join contract (§9) and the harness (§8). |

None of R1–R6 blocks the spine; R1/R2 are the only ones requiring a *design decision* (the
SCD tie-break), and that decision is made once in Phase 5 / §6.

---

## 5. Canonical spine artifact — decision

**Recommendation: ship BOTH a hardened-index spine (Phase 2, mandatory) AND a materialized
`msha_site_master` current-state anchor (Phase 5, high-value), and DEFER entity/contractor
rollup tables to a fast-follow.** Rationale, grounded:

| Option | For | Against | Verdict |
|---|---|---|---|
| **A. Indexed datasets + documented join contract only** | zero rebuild cost; zero staleness; spine is "just the raw sets + indices"; every query hits live data | the SCD tie-break (R1/R2) must be re-implemented correctly by **every** consumer; "active mines + current controller + signal counts" is a 6-table join every time; easy to get the SCD wrong | **necessary but insufficient** — this is Phase 2, the floor |
| **B. + materialize `msha_site_master`** (1 row per MINE_ID: status, geo, commodity, current controller/operator/contractor IDs+names, **pre-computed rollup signal counts**) | bakes the SCD determinism **once** so no consumer can get it wrong; the market-map anchor is a single indexed point-lookup; 91,803 rows = trivial rebuild (~seconds), trivial footprint | introduces staleness (mitigated: full `overwrite` rebuild on each MSHA refresh, same cadence as the sources); one more artifact to own | **build it (Phase 5)** — the determinism-baking alone justifies it |
| **C. + entity-grain & contractor-grain rollup tables** (`msha_controller_master`, `msha_contractor_master`) | controller-portfolio / contractor-activity become point lookups | the operator deferred entity↔entity interpretation; these are thin over the spine; can wait | **defer** — fast-follow once §11's external-resolution sequencing is decided |

`msha_site_master` is the *market-map anchor*: "every active mine, who currently controls/
operates it, and how distressed it is" in one indexed row. It is pure spine (no external
bridge), it is deterministic by construction, and it is cheap. The rollup counts it carries
(total violations, S&S since 2025, open orders, Σ proposed penalty, last violation date,
accident count, silica-overexposure flag) are exactly the GTM cohort predicates demonstrated
in §7 — pre-computed so the market map is one scan, not seventeen joins.

---

## 6. The SCD resolution — deterministic predicate (grounded)

`msha_corporate_history` is the site↔entity resolver. Live shape (probe6): 168,809 rows, **0
NULL `CONTROLLER_START_DT`**, 131,464 rows with `CONTROLLER_END_DT IS NULL`, 115,057 with
`OPERATOR_END_DT IS NULL`.

**Point-in-time resolution** `MINE_ID @ asof_date → CONTROLLER_ID`:
```sql
-- window match: the row whose [start, end) bracket contains asof_date
WHERE MINE_ID = :mine
  AND CONTROLLER_START_DT <= :asof
  AND (CONTROLLER_END_DT IS NULL OR CONTROLLER_END_DT > :asof)
```
**This is NOT single-valued** (R1): 16,619 mines match >1 distinct controller at
2025-01-01. The deterministic resolver layers a stable ordering:
```sql
-- deterministic single pick
QUALIFY row_number() OVER (
  PARTITION BY MINE_ID
  ORDER BY CONTROLLER_START_DT DESC,   -- latest window wins (collapses 16,739 → 778 ties)
           CONTROLLER_ID ASC           -- lexical tie-break (collapses the final 778, R2)
) = 1
```
**Current state** (`asof = today`) uses `CONTROLLER_END_DT IS NULL` as the window filter,
then the same `ORDER BY`. Coverage (probe6): **90,690 mines resolve to exactly one current
controller** under this rule; **778 needed the lexical tie-break**; **1,113 mines have no
current controller** (R3, LEFT JOIN → NULL). The identical predicate applies to
`OPERATOR_*_DT` for the operator pick.

`msha_site_master` (Phase 5) **materializes this pick once**, plus a `multi_controller_flag`
boolean (true for the 778 + any mine with >1 current window) so consumers can detect the
arbitrated rows. Everywhere else, §9's join contract publishes the predicate verbatim.

---

## 7. GTM cohorts become deterministic, index-backed queries (live-verified, probe7)

Each cohort below **ran live** and returned the stated count — proof the spine answers GTM
questions on indexed keys.

1. **"Active mines with ≥1 S&S since 2025"** → `msha_mines` (BITMAP `CURRENT_MINE_STATUS`) ⋈
   `msha_enforcement_ledger` (BTREE `MINE_ID`, BITMAP `SIG_SUB`, BTREE `VIOLATION_ISSUE_DT`).
   **Live: 4,052** of 12,282 active/intermittent mines.
2. **"Controller X's portfolio penalties since 2024"** → `enforcement` (BTREE `CONTROLLER_ID`
   + `VIOLATION_ISSUE_DT`, range-scan `PROPOSED_PENALTY_AMT`). **Live top controller
   `0158381`: $7,246,940 across 7,590 violations.**
3. **"Silica overexposure by mine"** → `msha_quartz_samples` (BTREE `MINE_ID`) with
   `try_cast(QUARTZ_PCT AS DOUBLE) > 5` **at query time** (mirror is all-string, §10). **Live:
   3,024** of 3,430 quartz-sampled mines have a >5% sample.
4. **"All activity for contractor X"** → `msha_contractors` ⋈ {`enforcement`, `accidents`,
   exposure samples, orders} on BTREE `CONTRACTOR_ID`. **Live contractor `B453`: 968 registry
   rows + 16 violations + 13 accidents** — one point-lookup per dataset. (After Phase 2,
   the enforcement leg is also index-backed; today it full-scans, the one place the third
   spine is not yet point-query fast.)

Cohort 3 demonstrates the typing reality: the predicate is deterministic and index-pruned on
`MINE_ID`, but the threshold itself needs a `try_cast` because the mirror column is VARCHAR.
This is acceptable (the cast is on the already-`MINE_ID`-pruned rows), and is the basis for
the §10 typing decision: **leave mirrors as passthrough; cast at query time.**

---

## 8. Integrity / resolution harness (read-only, continuous)

**Artifact:** `pipelines/ingest_msha/verify_spine.py` — a new **read-only** Modal app
(`msha-spine-verify`), cloned structurally from the `verify_datasets` / `verify_mirror`
entrypoints already in the three workers (same `_r2_storage_options`, `lance.dataset`,
`scanner().to_table()` → DuckDB pattern). **Zero writes** — no `write_dataset`, no
`create_scalar_index`, no ledger DDL on the hot path.

**Asserts (fails the run if any gate regresses):**
- `MINE_ID` → `msha_mines` == **100.00%** of non-null, every child (hard gate).
- `CONTROLLER_ID` → `corp_history` ≥ **99.5%** of populated, each of {mines, enforcement,
  inspections, accidents}.
- `OPERATOR_ID` → `corp_history` ≥ **99.5%**; Operator-typed `VIOLATOR_ID` → `corp.OPERATOR_ID`
  ≥ **99.5%**.
- `EVENT_NO` enforcement → inspections == **100.00%**; samples → inspections ≥ **99.5%**.
- `CITATION_NO`/`VIOLATION_NO` cross-tier ≥ **94%**; `DOCKET_NO` contested → dockets ≥ **99%**.
- `CONTRACTOR_ID` accidents → registry ≥ **99%**; enforcement → registry ≥ **88%** (the
  89.32% reality, with headroom).
- **Index manifest** matches the §3.3 expected set per dataset (`list_indices()` diff) — the
  primary catch for "an index silently dropped on re-mirror."
- **Format invariants:** `MINE_ID` all 7-char digits, 0 whitespace; entity/contractor IDs 0
  whitespace; lowercase-ID count == the known 3 (alerts if a 4th appears).
- **SCD determinism:** count of mines with >1 current controller is reported (drift signal),
  and the latest-start-wins resolver still collapses to ≤ the known 778 ties.

**Cadence:** run after every MSHA refresh and on demand (`modal run …verify_spine::check`).
Output is a single pass/fail JSON + the per-edge table; on fail it names the regressed edge.
This is the durable guarantee that the spine stays deterministic.

---

## 9. The canonical JOIN CONTRACT (publish with the plan)

Every spine edge, its grain, its index backing **after Phase 2**, and its live resolution.
This table is the deterministic-join API for every downstream consumer.

| from (dataset.key) | → to (dataset.key) | cardinality | index-backed (post-P2) | live resolution (of populated) | probe |
|---|---|---|:-:|--:|:-:|
| `*.MINE_ID` (14 children) | `msha_mines.MINE_ID` | N:1 | ✅ both sides | **100.00%** | probe2 |
| `msha_mines.CURRENT_CONTROLLER_ID` | `corp_history.CONTROLLER_ID` | N:1 | ✅ | 99.84% | probe3 |
| `msha_mines.CURRENT_OPERATOR_ID` | `corp_history.OPERATOR_ID` | N:1 | ✅ | 99.86% | probe3 |
| `enforcement.CONTROLLER_ID` | `corp_history.CONTROLLER_ID` | N:1 | ✅ | 99.99% | probe3 |
| `enforcement.VIOLATOR_ID` (type=Operator) | `corp_history.OPERATOR_ID` | N:1 | ✅ | 99.99% | probe3 |
| `enforcement.VIOLATOR_ID` (type=Contractor) | `contractors.CONTRACTOR_ID` | N:1 | ✅ VIOLATOR_ID side | 89.32% | probe3/4 |
| `enforcement.CONTRACTOR_ID` | `contractors.CONTRACTOR_ID` | N:1 | ✅ **(P2 closes)** | 89.32% | probe3/4 |
| `accidents.{CONTROLLER_ID,OPERATOR_ID,CONTRACTOR_ID}` | `corp_history` / `contractors` | N:1 | ✅ | 100.00 / 100.00 / 99.81% | probe3 |
| `inspections.{CONTROLLER_ID,OPERATOR_ID}` | `corp_history` | N:1 | ✅ | 99.99% | probe3 |
| `enforcement.EVENT_NO` | `inspections.EVENT_NO` | N:1 | ✅ | 100.00% | probe5 |
| `{personal_health,noise,area}.EVENT_NO` | `inspections.EVENT_NO` | N:1 | ✅ | 99.79–99.98% | probe5 |
| `contested.CITATION_NO` | `enforcement.VIOLATION_NO` | N:1 | ✅ | 94.96% | probe5 |
| `dockets.VIOLATION_NO` | `enforcement.VIOLATION_NO` | N:1 | ✅ | 94.42% | probe5 |
| `orders_issued.VIOLATION_NO` | `enforcement.VIOLATION_NO` | N:1 | ✅ | 99.92% | probe5 |
| `contested.DOCKET_NO` | `dockets.DOCKET_NO` | N:1 | ✅ | 99.57% | probe5 |
| `enforcement.DOCKET_NO` | `dockets.DOCKET_NO` | N:1 | ✅ **(P2 closes)** | (litigated subset) | probe1 |
| `conferences.ISSUANCE_NO` | `enforcement.VIOLATION_NO` | N:1 | ✅ | 87.82% | probe5 |
| `orders_issued.CONTROLLER_ID_VIOLATIONS` | `corp_history.CONTROLLER_ID` | N:1 | ✅ **(P2 closes)** | — | probe1 |
| `MINE_ID @ date` (SCD) | `corp_history → (CONTROLLER_ID, OPERATOR_ID)` | 1:1 (after §6 predicate) | ✅ MINE_ID | 90,690/91,803 current; §6 | probe6 |

**Anti-joins (documented traps, never use):** samples → enforcement on `VIOLATION_NO`
(2.36%, R6) — use `EVENT_NO`. `enforcement.EVENT_NBR` is a redundant duplicate of `EVENT_NO`
(3,008,850 rows identical, 0 differ — probe5); ignore it.

---

## 10. Typing gaps — decision (grounded, probe6)

The 12 mirror datasets are **confirmed 100% all-VARCHAR** (only `ingested_at` is a timestamp;
every other column, including dates and numerics, is `string` — probe6 sampled inspections,
prod_quarterly, coal_dust, dockets: zero non-string non-`ingested_at` fields).

**Decision: leave mirrors as passthrough; cast at query time. Do NOT typed-promote any mirror
for the spine.** Grounding:
- **Every spine join key is VARCHAR by design** (leading-zero/alpha-prefix safety). The spine
  needs *zero* casts — `MINE_ID`, `EVENT_NO`, `VIOLATION_NO`, `CONTRACTOR_ID` etc. join as
  strings and are already BTREE-indexed. Typing the mirrors buys the spine nothing.
- **Cohort thresholds (dates for recency, numerics for penalty/exposure) cast cheaply at
  query time** *after* the spine has pruned on an indexed key (§7 cohort 3: the `try_cast` on
  `QUARTZ_PCT` runs on `MINE_ID`-pruned rows, not a full scan).
- A mirror dataset that later earns heavy analytical use **graduates** into a curated typed
  worker (the documented mirror→curated path) — that is a *per-dataset* decision driven by
  query patterns, **out of scope for the spine**.
- **Exception worth surfacing (not spine-blocking):** if `msha_site_master` (Phase 5)
  pre-computes recency/penalty rollups, those casts happen **once at materialization** inside
  the curated DuckDB transform — so the market-map anchor gets typed rollup columns for free,
  while the underlying mirrors stay passthrough. Best of both.

---

## 11. Out of scope — the explicit "next, not now" boundary

External entity resolution — MSHA legal entities (`CONTROLLER_NAME`/`OPERATOR_NAME`/
`CONTRACTOR_NAME` and their committed `*_norm` siblings) → `companies` /
`sos_normalized_master` / PPP / SBA / any cross-universe bridge — is **deferred by operator
directive**. The spine is the deterministic *internal* join tissue; external resolution is a
separate downstream effort sequenced later.

**Notably, the curated sets already carry the bridge-readiness substrate** (probe1):
`*_norm` BTREE indices exist on `msha_mines`, `enforcement`, `accidents`, `corp_history`,
`contractors`. That is *latent* capability — this plan neither builds nor uses any external
bridge on top of it. Whether a `CONTRACTOR_ID` resolves to a controller/operator is
**post-spine business interpretation**, explicitly not planned here (the contractor third
spine is a first-class *population* that may or may not later resolve to an entity).

---

## 12. Phased execution plan (gated, blast-radius-isolated)

Ordered lowest-risk / highest-leverage first. **Index hardening (P2) is fully isolated from
any materialization (P5).** Each phase has an explicit verification gate that must pass before
the next advances.

### Phase 0 — Baseline & freeze the evidence (read-only)
- **Objective:** lock the as-of-2026-06-10 spine state as the regression baseline.
- **Artifacts:** this document; the six `/tmp/msha_probe*.py` scripts promoted into
  `pipelines/ingest_msha/verify_spine.py` (Phase 1) as the canonical probe set.
- **Recipe:** none (read-only). Capture §3.1/§3.2/§3.3/§9 numbers as the baseline JSON.
- **Success gate:** matrices in §3 reproduce on a fresh probe run (they will — same data).
- **Blast radius:** none.

### Phase 1 — Integrity harness FIRST (read-only Modal app)
- **Objective:** the drift detector exists *before* any mutation, so Phase 2's effect is
  measured, not assumed.
- **Artifacts:** `pipelines/ingest_msha/verify_spine.py` (`msha-spine-verify` app), §8 gates.
- **Recipe:** clone `verify_datasets` (curated worker) + `verify_mirror` (mirror worker)
  structure; pure `lance.dataset` reads → DuckDB joins; emit the §9 contract table + pass/fail.
- **Success gate:** harness runs green against the **current** datasets and reproduces every
  §4.1 / §9 rate within ±0.01%. The enforcement `CONTRACTOR_ID` / `DOCKET_NO` and
  `orders_issued.CONTROLLER_ID_VIOLATIONS` edges are asserted on the **column** (resolution),
  independent of index state, so they pass now and stay green post-P2.
- **Blast radius:** none (read-only).

### Phase 2 — Index hardening (close the 3 spine-key gaps) — **isolated, no data rewrite**
- **Objective:** every present spine key is BTREE-indexed. Convert the 3 `!` cells to `I`.
- **Exact targets (probe1):**
  1. `msha_enforcement_ledger.CONTRACTOR_ID` → BTREE
  2. `msha_enforcement_ledger.DOCKET_NO` → BTREE
  3. `msha_orders_issued.CONTROLLER_ID_VIOLATIONS` → BTREE
- **Artifacts / recipe:**
  - Enforcement (curated): add `CONTRACTOR_ID`, `DOCKET_NO` to
    `materialize_msha.py::INDEX_PLAN["msha_enforcement_ledger"]["BTREE"]`, run the existing
    **`reindex` entrypoint** (`modal run materialize_msha.py::reindex_only --only
    msha_enforcement_ledger`). `create_scalar_index` defaults to `replace=True` → idempotent.
    `LANCE_BYPASS_SPILLING=true` (already set in the image) handles the 3.08M-row sort.
  - Orders (mirror): one `create_scalar_index("CONTROLLER_ID_VIOLATIONS","BTREE")` via the
    mirror's `_build_indexes` path — re-mirror `--only msha_orders_issued --force`, OR a
    targeted reindex. **Also add `CONTROLLER_ID_VIOLATIONS` to `MIRROR_KEY_COLS`** so future
    re-mirrors auto-index it (the root-cause fix; orders_issued is 3,830 rows — negligible).
- **Success gate (harness re-run):** `list_indices()` shows all 3 new BTREEs; each reports
  `num_indexed_rows == total, num_unindexed_rows == 0` (overwrite-then-index lifecycle
  guarantees full training — proven for MSHA in `MSHA_INDEX_TOPOLOGY_PUSHDOWN_DIAGNOSTIC.md`).
  An `explain_plan` on `enforcement WHERE CONTRACTOR_ID = '<reg id>'` emits a `ScalarIndexQuery`
  node (was full-scan). **No row count changes on any dataset** (index commits only).
- **Blast radius:** index-version commits only; **zero data rewrite, zero `_deletions/`**.
  Each dataset gains N index versions, nothing else. Enforcement and orders are independent —
  do them as two separate operations.

### Phase 3 — Hygiene remediation decision (R4/R5) — **operator-gated**
- **Objective:** resolve the 3 lowercase ID cells per the operator's R4 decision.
- **Artifacts / recipe (if operator chooses remediation):** wrap the contractor/violator key
  projection in the **curated** workers with `upper()` —
  `materialize_msha.py` (`VIOLATOR_ID` on enforcement) and
  `materialize_msha_extensions.py` (`CONTRACTOR_ID` on accidents) — then re-materialize those
  two datasets (`run --only msha_enforcement_ledger`, `run --only msha_accidents`). Recovers
  `f466→F466`, `m837→M837` (registry hits); `4kk→4KK` stays unresolved (not in registry, R5).
- **Success gate:** harness lowercase-ID count drops from 3 to ≤1; CONTRACTOR_ID
  enforcement→registry resolution rises by exactly the recovered rows (immaterial to the
  headline, but provably correct). Row counts unchanged (grain_ok).
- **Blast radius:** two curated datasets re-materialized (full `overwrite` + reindex); Lance
  versioning preserves prior state. **Skippable** — R4 disposition (a) accepts as-is with no
  code change. Decoupled from Phase 2 so index hardening is not held hostage to this decision.

### Phase 4 — Catalog surfacing
- **Objective:** the spine + harness are discoverable in `factory-state`.
- **Artifacts:** `scripts/data-factory-catalog.py` — register `verify_spine` and (when built)
  `msha_site_master` so `/factory-state` surfaces them; add a "MSHA spine" note alongside the
  Landing-zones section.
- **Success gate:** `/factory-state` lists the harness and the §9 contract location.
- **Blast radius:** docs/catalog only.

### Phase 5 — Materialize `msha_site_master` (the market-map anchor) — **separate worker, separate PR**
- **Objective:** one deterministic indexed row per `MINE_ID` with current entity IDs + names
  (via the §6 SCD predicate) + pre-computed signal rollups.
- **Artifacts:** new `pipelines/ingest_msha/materialize_msha_site_master.py`
  (`msha-site-master` app), cloned from `materialize_msha.py` (curated single-dataset path:
  same `_r2_storage_options`, `_write_lance` overwrite, `_create_indexes`, `_record_run`
  deadlock-safe ledger pattern, Trigger callback). Target
  `s3://data-sink/active/msha_site_master/`.
- **Recipe (100% DuckDB over the Lance Arrow streams):**
  - Spine = `msha_mines` (91,803 rows, the grain anchor).
  - LEFT JOIN the §6 deterministic current-controller / current-operator pick from
    `corp_history` (latest-start-wins + lexical tie-break; `multi_controller_flag`).
  - LEFT JOIN pre-aggregated rollups keyed on `MINE_ID`: total violations, S&S count,
    S&S-since-2025 count, open-order count (`CIT_ORD_SAFE='Order'`), Σ `PROPOSED_PENALTY_AMT`,
    `max(VIOLATION_ISSUE_DT)`, accident count, silica-overexposure flag
    (`max(try_cast(QUARTZ_PCT AS DOUBLE)) > 5`). Casts happen once here (the §10 exception).
  - Carry current `CONTRACTOR_ID` set per mine only if a single deterministic one exists;
    otherwise leave to the contractor-grain fast-follow (contractor↔mine is M:N by nature).
- **Indexing:** BTREE `MINE_ID`, `CURRENT_CONTROLLER_ID`, `CURRENT_OPERATOR_ID`; BITMAP
  `CURRENT_MINE_STATUS`, `COAL_METAL_IND`, `STATE`, `multi_controller_flag`.
- **Success gate:** `count_rows == 91,803` (grain == `msha_mines`, no fan-out); MINE_ID 1:1;
  current-controller populated on exactly 90,690 + tie-broken 778, NULL on 1,113 (matches §6);
  harness extended to assert these. Cohort §7.1 reproducible directly off `msha_site_master`
  in a single scan.
- **Blast radius:** net-new dataset; touches nothing existing. Independent PR.

### Phase 6 — Git lifecycle (house standard)
- **Per the durable workflow:** branch → commit → push → PR → self-verify (harness green) →
  `gh pr merge --squash --delete-branch` → **pull into the operator checkout
  `/Users/benjamincrane/core-x`** → `git log -1 --oneline`.
- **PR granularity (blast-radius hygiene):**
  - **PR-A** = Phase 1 (harness) + Phase 2 (index hardening) + Phase 4 (catalog). These are
    read-additive / index-only and verify together cleanly.
  - **PR-B** = Phase 3 (hygiene re-materialization) — *only if the operator elects R4(b)* —
    isolated because it rewrites two curated datasets.
  - **PR-C** = Phase 5 (`msha_site_master`) — net-new worker + dataset, isolated.
  - **No stacked PRs** (squash drops later commits). Each PR opens against `main` directly.

---

## 13. Sequencing logic (dependency + risk)

```
P0 baseline (read-only)
   └─> P1 harness (read-only)          ── must exist before any mutation
          └─> P2 index hardening       ── lowest-risk mutation: index commits, no data rewrite
                 ├─> P4 catalog         ── docs only
                 ├─> P3 hygiene (gated) ── optional curated re-materialize (operator decision R4)
                 └─> P5 site_master     ── net-new anchor; depends on P2 (CONTRACTOR_ID indexed)
                                            and on the §6 SCD predicate being fixed
   P6 git lifecycle wraps each landable unit
```

- **Index hardening before any materialization** — P2's BTREEs (esp. enforcement
  `CONTRACTOR_ID`) make P5's rollup aggregation index-pruned, and the harness proves the
  spine is sound before anything is baked into the anchor.
- **The deterministic spine (P1–P4) before any rollup table (P5)** — `msha_site_master` is
  only trustworthy if the SCD predicate (§6) and the spine resolution (§9) are proven first.
- **Hygiene (P3) is decoupled** so a pending operator decision on R4 never blocks the
  high-leverage index + anchor work.

---

## 14. Idempotency & durability (canonical to the repo)

- **Overwrite semantics:** any re-materialization (P3, P5) is full-snapshot
  `mode="overwrite"`; Lance versioning preserves history; no row-level deletes.
- **Index builds:** `create_scalar_index(..., replace=True)` — idempotent; re-running P2 is a
  no-op if already current.
- **Ledger:** every materialization writes `ops.msha_ingest_runs` (feed='msha') via the
  **deadlock-safe pattern** — `_ensure_ops_ledger()` once before any fan-out, `to_regclass`
  guard + INSERT-retry on `DeadlockDetected`/`SerializationFailure`, `conn.transaction()` (not
  `with conn:`) — the #377 fix, already in `materialize_msha_mirror.py`. P5's new worker
  clones it verbatim.
- **Harness (P1)** is the durable guarantee: it re-runs after every MSHA refresh and on
  demand, failing on any resolution-rate or index-manifest regression (§8).
- **Blast-radius isolation:** index hardening (P2, index-version commits) is structurally
  separate from materialization (P3/P5, data overwrites); they never share a PR except where
  read-additive (PR-A).

---

## 15. Appendix — exact anomalies for the harness allowlist

- **Known lowercase IDs (3):** `enf.CONTRACTOR_ID`/`VIOLATOR_ID` = `f466`, `m837`;
  `acc.CONTRACTOR_ID` = `4kk`. (probe4)
- **Known malformed enforcement CONTRACTOR_ID (10 rows, 5 distinct):** `0113025`, `0141673`,
  `0141713`, `0155553`, `0158134` (all-numeric, non-registry). (probe4)
- **Redundant column:** `enforcement.EVENT_NBR` == `EVENT_NO` (3,008,850 identical, 0 differ).
  (probe5)
- **Join trap:** `noise_samples.VIOLATION_NO` resolves to enforcement at 2.36% — it is a
  survey-form number; bridge samples on `EVENT_NO`. (probe5)
- **Naming drift:** dust sample key is `CASS_NUM` (coal_dust) vs `CASSETTE_NO` (quartz);
  `quartz.ENTITY_NO` ≠ `MINE_ID` (differ on all 167,238 rows) — a distinct unindexed entity
  key, not MINE_ID-redundant. (probe6) Out of scope for the spine (neither is a core `_ID`),
  noted for completeness.
- **SCD:** 16,739 mines >1 current controller; 778 same-day ties after latest-start-wins;
  1,113 mines with no current controller; 0 NULL `CONTROLLER_START_DT`. (probe6)
