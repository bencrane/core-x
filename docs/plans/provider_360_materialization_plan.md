# provider_360 — Materialization Plan (implementation-ready)

**Status:** ready to build — **v2, adversarial review applied + live-validated.** All input datasets are landed and verified in the Lance SoR.

> **v2 remediations (all verified against live data — see §13 for the full audit):**
> - **QPP fan-out fixed** (§4.6): collapse is a mandatory `GROUP BY npi` pre-agg CTE; `participation_type` is dead post-2022 → use `coalesce(participation_option, reporting_option)`.
> - **A2/C3 money math fixed** (§4.4): line totals computed in DOUBLE, final per-NPI scalar cast to `DECIMAL(18,2)` (the `avg×srvcs` product overflows `DECIMAL` and NULLs the biggest providers); `*_lines_suppressed` counters added.
> - **Practice "primary" inversion fixed** (§4.7): "largest fan-in" assigned solo docs to the hospital they're cross-credentialed at — carry BOTH poles + `is_independent_candidate`.
> - **ADDED Medicare panel economics** (§4.2): risk score / dual-share / chronic-disease % — the #1 practice-economics signals, previously omitted.
> - **CUT** redundant `cms_ownership` re-aggregation (the Open Payments rollup already carries it).
> - **+5 gates** (§7) and the **`practice_group_360`** companion — 1 row per buyable practice unit (§12).
**Owner of context:** this doc is self-contained. A fresh agent should be able to read it top-to-bottom and start building without prior session context.
**Companion specs:** [`docs/analysis/entity-360-master-plan.md`](../entity-360-master-plan.md), [`docs/analysis/nppes_analytical_implementation_plan.md`](../nppes_analytical_implementation_plan.md).
**Closest code precedent:** [`pipelines/cms_open_payments/materialize_resolution.py`](../../pipelines/cms_open_payments/materialize_resolution.py) (the 1-row-per-NPI rollup this generalizes) and [`pipelines/nppes/materialize_analytical.py`](../../pipelines/nppes/materialize_analytical.py) (the per-snapshot serving-layer pattern + gates).

---

## 1. Mission

Collapse the ~10-way per-NPI fan-out across the landed CMS/NPPES graph into **one wide row per NPI**, so the PE-facing query is:

```sql
SELECT * FROM provider_360 WHERE npi = ?                       -- single point-lookup, sub-100ms
-- or a cohort cut, all on indexed columns:
SELECT npi, ... FROM provider_360
WHERE primary_taxonomy_code = '207RC0000X' AND practice_state = 'TX'
  AND med_a1_pymt_growth_2019_latest_pct > 25
```

`provider_360` is a **derived serving layer**, a pure function of its inputs, idempotent, rebuilt per snapshot. It is **not** a system of record — it can be dropped and rebuilt from the landed datasets at any time.

### Hard scope boundaries (do not violate)
- **One row per NPI.** Base = `nppes_provider` (9,551,447 verified-unique NPIs). The output row count MUST equal exactly **9,551,447** — this is gate G1.
- **Deterministic joins only.** Every attach is on a published key: `npi`, or `ENRLMT_ID` for practice affiliation. **No fuzzy name/geo resolution. No corporate-identity columns.** `parent_organization_lbn` is carried as a raw string only — never used to derive a corporate id. "Who really owns this practice" is a separate downstream layer joined by its own key once it exists; it is **not** part of provider_360.
- **Suppression is sacred.** CMS `*_sprsn_ind` / `*_sprsn_flag` (VARCHAR) and suppressed-to-NULL numeric cells must survive. **Never coalesce a suppressed value to 0.** A null payment means "suppressed/absent", not "zero".
- **Deactivated providers are kept.** 343,321 NPPES NPIs are deactivated stubs (NULL entity_type/taxonomy/geo, `is_active=false`). They stay in the base; their CMS attaches are simply absent. Null ≠ drop.

---

## 2. Architecture — two tiers

The detail/giant datasets are too large to re-scan on every `provider_360` rebuild. Pre-roll them into their own reusable per-NPI datasets first; then `provider_360` only ever joins compact (~1–2M-row) inputs.

```
TIER 1 — per-NPI rollups (each a standalone active/ dataset, 1 row/NPI, BTREE npi)
  EXISTS:  cms_provider_payment_rollup            (Open Payments — already built, 1.6M rows)
  BUILD:   cms_physician_service_rollup           ← A2 cms_physician_provider_service (78.5M → ~1.1M)
  BUILD:   cms_partd_drug_rollup                  ← B2 cms_partd_provider_drug      (304M → ~1.3M)
  BUILD:   cms_dme_supplier_rollup                ← C3 cms_dme_supplier_service     (5.5M → ~99k)

TIER 2 — provider_360/snapshot=YYYY-MM/  (1 row/NPI, base = nppes_provider, LEFT JOINs)
  base    nppes_provider (+ taxonomy, identifier child rollups, aggregated inline — cheap)
  + A1    cms_physician_provider        (13.5M, GROUP BY npi inline — cheap)
  + B1    cms_partd_provider            (9.2M,  GROUP BY npi inline — cheap)
  + TIER-1 rollups (service, drug, dme, open-payments) — straight 1:1 LEFT JOINs
  + QPP   cms_qpp_experience            (6.2M → latest-year MIPS)
  + practice  enrollment ⨝ reassignment graph (ENRLMT_ID → group)
```

**Why this split:** A2/B2/C3 are scanned **once** to build their Tier-1 rollups (measured: A2 full GROUP BY = 5.2s, B2 = 10.7s — tractable but wasteful to repeat). After that, `provider_360` rebuilds are fast and decoupled from the giants. The Tier-1 rollups are also independently useful assets (e.g., "every prescriber's drug profile"). This mirrors the already-shipped `cms_provider_payment_rollup`.

**Build A1/B1/taxonomy/identifier/enrollment inline** in the Tier-2 build — they are mid-sized (≤13.5M rows), a single GROUP BY each, trivial at `memory_limit=20GB`.

---

## 3. Input datasets (all landed, all `s3://data-sink/active/…`)

| Input | Rows | NPI key | Vintage | Role |
|---|---:|---|---|---|
| `nppes_provider/snapshot=2026-05` | 9,551,447 | `npi` (1:1) | 2026-05 | **base** identity/specialty/geo |
| `nppes_provider_taxonomy/snapshot=2026-05` | 11,952,809 | `npi` (N) | 2026-05 | multi-specialty (child) |
| `nppes_provider_identifier/snapshot=2026-05` | 2,759,800 | `npi` (N, 16.3% cov) | 2026-05 | external IDs (child) |
| `cms_physician_provider` (A1) | 13,528,933 | `rndrng_npi` | 2013–2024 | Part B totals |
| `cms_partd_provider` (B1) | 9,219,683 | `prscrbr_npi` | 2013–**2020** | Part D totals |
| `cms_physician_provider_service` (A2) | 78,482,821 | `rndrng_npi` | 2017–2024 | NPI×HCPCS×POS (→ Tier-1) |
| `cms_partd_provider_drug` (B2) | 304,308,166 | `prscrbr_npi` | 2013–2024 | NPI×drug (→ Tier-1) |
| `cms_dme_supplier_service` (C3) | 5,542,054 | `suplr_npi` | 2014–2023 | DME supply (→ Tier-1) |
| `ref_rbcs_taxonomy` | 18,882 | `hcpcs_cd` | RY2025 | HCPCS→service-line ref |
| `cms_qpp_experience` (QPP) | 6,154,354 | `npi` | 2017–2024 | MIPS quality |
| `cms_provider_payment_rollup` | ~1.6M | `npi` (1:1) | (built) | Open Payments (Tier-1, exists) |
| `cms_ownership` | 27,480 | `physician_npi` | — | ownership detail |
| `cms_provider_enrollment` | 2,981,788 | `npi` (N: 281,965 NPIs >1 enrlmt) | 2026-Q1 | npi↔enrlmt_id↔pecos |
| `cms_provider_enrollment_reassignment` | 3,857,023 | `reasgn→rcv` ENRLMT_ID | 2026-Q1 | practice graph |

> **Vintage skew is real and expected.** Each per-NPI source has its own year horizon (B1 stops at 2020; A2 2017+; C3 ends 2023). Do **not** compute a single global "latest year" — every `latest_*` is that **source's per-NPI MAX(program_year)**. Record each source's max landed year in a provenance column / the ops ledger.

---

## 4. Output schema

Column families below. Types are exact. `arg_max(x, program_year)` = the value of `x` in the NPI's latest year for that source. Every block is a LEFT JOIN — absent → NULL (not 0), plus a `has_*` boolean.

### 4.1 Identity / specialty / geo (base — passthrough from `nppes_provider`, no aggregation)
`npi` (string, PK, NOT NULL) · `entity_type_code` · `entity_type` · `is_active` (bool) · `provider_name` · `organization_name` · `last_name` · `first_name` · `middle_name` · `name_prefix` · `name_suffix` · `credential` · `sex_code` · `is_sole_proprietor` (VARCHAR Y/N/X — **preserve, never boolean-coalesce**) · `is_organization_subpart` · `primary_taxonomy_code` (already denormalized on base, == taxonomy.is_primary, 0 mismatch — **no join needed**) · `practice_state` · `practice_zip5` · `practice_address_line1`/`line2`/`city`/`zip`/`country` · `mailing_state`/`city`/`zip5` · `enumeration_date` (date32) · `enumeration_year` (int16) · `last_update_date` · `deactivation_date` · `reactivation_date` · `authorized_official_last_name`/`first_name`/`title` · `parent_organization_lbn` (raw string only) · `snapshot_month`.

Child-table rollups (aggregate inline, route npi scan through the npi-clustered base):
- `all_taxonomy_codes` `LIST<string>` ← `array_agg(DISTINCT taxonomy_code)` from `nppes_provider_taxonomy` (1,334,147 NPIs have >1).
- `taxonomy_slot_count` `int` ← `count(*)` per npi.
- `primary_license_state` `string` ← `max(license_state) FILTER (is_primary)`.
- `has_medicaid_id` `bool` + `has_secondary_identifiers` `bool` ← `nppes_provider_identifier` (`bool_or(identifier_type_code='05')`). (Surfacing the raw id values is optional/v2.)

### 4.2 Medicare Part B totals (A1, inline GROUP BY `rndrng_npi`)
- `med_a1_has` `bool` (join hit)
- `med_a1_latest_year` `SMALLINT` ← `max(program_year)` (per-NPI; only 67.4% reach 2024)
- `med_a1_latest_mdcr_pymt` `DECIMAL(18,2)` ← `arg_max(tot_mdcr_pymt_amt, program_year)`
- `med_a1_latest_stdzd` `DECIMAL(18,2)` ← `arg_max(tot_mdcr_stdzd_amt, …)` *(cross-region-comparable — prefer for cohort ranking)*
- `med_a1_latest_benes` `BIGINT` · `med_a1_latest_srvcs` `BIGINT` · `med_a1_latest_allowed` `DECIMAL(18,2)`
- `med_a1_first_year` / `med_a1_last_year` `SMALLINT` · `med_a1_active_years` `SMALLINT` ← `count(DISTINCT program_year)`
- `med_a1_lifetime_mdcr_pymt` **`DECIMAL(38,2)`** ← `sum(...)` *(sum widens 18,2→38,2; max observed 2.59B — declare wide or it overflows)*
- `med_a1_lifetime_benes` `BIGINT` *(bene-YEARS, not distinct benes — document)*
- `med_a1_pymt_growth_2019_latest_pct` `DOUBLE` ← `100*(latest − v@2019)/nullif(v@2019,0)`; only the 878,572 NPIs (45.7%) present in **both** 2019 and latest, else NULL
- `med_a1_provider_type` `VARCHAR` ← `arg_max(rndrng_prvdr_type, …)` (CMS Part-B practitioner type)
- `med_a1_entity_code` `VARCHAR` (`I`/`O`; stable per NPI) · `med_a1_mdcr_participating` `VARCHAR`
- `med_a1_drug_sprsn_latest` / `med_a1_med_sprsn_latest` `VARCHAR` ← `arg_max(*_sprsn_ind, …)` **(preserve verbatim)**

### 4.3 Medicare Part D prescriber totals (B1, inline GROUP BY `prscrbr_npi`; **caps at 2020**)
- `med_b1_has` `bool`
- `med_b1_latest_year` `SMALLINT` (≤ 2020) · `med_b1_latest_drug_cost` `DECIMAL(18,2)` ← `arg_max(tot_drug_cst, …)`
- `med_b1_latest_clms` `BIGINT` · `med_b1_latest_30day_fills` `BIGINT` · `med_b1_latest_day_suply` `BIGINT`
- `med_b1_latest_benes` `BIGINT` **(nullable — B1 suppresses tot_benes <11; preserve NULL)**
- `med_b1_first_year`/`last_year`/`active_years` `SMALLINT`
- `med_b1_lifetime_drug_cost` `DECIMAL(38,2)` · `med_b1_lifetime_clms` `BIGINT`
- `med_b1_cost_growth_2019_2020_pct` `DOUBLE` *(B1's terminal anchor is 2019→2020, **not** →2024)*
- `med_b1_latest_opioid_clms` `BIGINT` · `med_b1_latest_opioid_rate` `VARCHAR` (stored as text — carry verbatim, don't cast) · `med_b1_latest_la_opioid_clms` `BIGINT` · `med_b1_latest_antbtc_clms` `BIGINT`
- `med_b1_prescriber_type` `VARCHAR` (flags the Rx-only Dentist/Student population A1 misses)

> B1's ~10 suppression-flag/money subline families (ge65/brnd/gnrc/lis/…) are **low value at 360 grain** — carry only the latest-year top-line + opioid signals above. Leave the rest in the B1 detail table.

### 4.4 Service / drug / DME detail (Tier-1 rollups — straight 1:1 LEFT JOINs)
From `cms_physician_service_rollup` (← A2): `svc_total_medicare_paid_usd` `DECIMAL(18,2)` (`sum(avg_mdcr_pymt_amt*tot_srvcs)`) · `svc_total_services` `BIGINT` · `svc_distinct_hcpcs` `BIGINT` · `svc_active_years`/`first`/`last` · `svc_top_rbcs_cat` `VARCHAR` (via RBCS join) · `svc_top_rbcs_cat_paid_usd` · `svc_partb_drug_paid_usd` (`hcpcs_drug_ind='Y'`) · `has_partb_administered_drugs` `bool`.

From `cms_partd_drug_rollup` (← B2): `rx_total_drug_cost_usd` `DECIMAL(18,2)` · `rx_total_claims` `BIGINT` · `rx_total_30day_fills` · `rx_total_day_supply` · `rx_distinct_generics` `BIGINT` · `rx_distinct_brands` · `rx_active_years`/`first`/`last` · `rx_top1_generic` `VARCHAR` · `rx_top1_generic_cost_usd` · `rx_top3_generics` `VARCHAR (json array)`.

From `cms_dme_supplier_rollup` (← C3): `dme_supplied_total_paid_usd` · `dme_supplied_claims` · `dme_distinct_hcpcs` · `dme_rental_share` `DOUBLE` · `is_dme_supplier` `bool`.

> **Money reconstruction (critical):** A2/C3 publish **average** payment per line (`avg_*_mdcr_pymt_amt`), not a line total. Multiply `avg × tot_srvcs` to get a line total before summing. B1/B2 totals (`tot_drug_cst`) are already totals.
> **tot_benes double-counts** across an NPI's HCPCS lines → `svc_total_benes_served` is a bene-LINE count, not distinct benes. Name/document it as such; do not present it as unique patients.

### 4.5 Industry payments (Open Payments rollup, 1:1 passthrough) + ownership
`op_total_payments_usd` · `op_general_total_usd` · `op_research_total_usd` · `op_distinct_manufacturers` `BIGINT` · `op_has_ownership_interest` `bool` · `op_ownership_total_value_usd` · `op_first_payment_year` / `op_last_payment_year` · `op_recipient_type` — all straight passthrough from `cms_provider_payment_rollup` (already 1-row-per-NPI). `cms_ownership` aggregates add `ownership_op_record_count` / `ownership_op_total_invested_usd` if not already covered by the rollup (check first — the rollup likely already carries them).

### 4.6 Quality — MIPS (QPP, latest-year collapse)
- `mips_final_score` `DOUBLE` ← within each NPI's **latest** program_year, `MAX(TRY_CAST(final_score AS DOUBLE))` (deterministic tiebreak across participation rows)
- `mips_final_score_year` `SMALLINT` · `mips_participation_type` `VARCHAR` (`arg_max(participation_type, final_score)` in that year)
- `has_mips` `bool` (1.28M distinct NPIs in QPP)

### 4.7 Practice affiliation (deterministic, ENRLMT_ID graph — the headline PE join)
- `enrollment_enrlmt_ids` `LIST<VARCHAR>` ← `list(DISTINCT enrlmt_id)` from `cms_provider_enrollment` (281,965 NPIs have >1)
- `pecos_asct_cntl_id` `VARCHAR` (constant per individual)
- `practice_group_enrlmt_ids` `LIST<VARCHAR>` ← traverse `enrollment.enrlmt_id → reassignment.reasgn_bnft_enrlmt_id → rcv_bnft_enrlmt_id`
- `practice_group_count` `BIGINT` (distinct groups billed to)
- `primary_practice_group_enrlmt_id` `VARCHAR` ← the group with the **largest fan-in** (deterministic primary selector)
- `primary_practice_org_name` `VARCHAR` ← that group's `org_name` (from enrollment; populated on the 433,491 org rows)
- `primary_practice_group_size` `BIGINT` ← that group's fan-in (distinct reassigners; ranges 1..20,825). **This is the independent-vs-rolled-up signal** (fan-in=1 → solo PC).
- `has_ffs_enrollment` `bool`

---

## 5. Build mechanics (reuse, don't reinvent)

New Modal app `provider-360-pipelines`. Copy these verbatim from the cited files:

| Building block | Source |
|---|---|
| Modal image + app skeleton (`init_state`/`materialize`/`verify`/`show_ledger`) | `pipelines/nppes/materialize_analytical.py` |
| `build_all()` read-each-source-once → DuckDB temp → assemble → local Lance | `pipelines/cms_open_payments/materialize_resolution.py` (**closest precedent**) |
| `_publish_full_swap()` (stage→verify→swap→verify) | `pipelines/cms_medicare/ingest.py` |
| `_verify_published()` (reopen from R2, assert rows+indices+npi probe) | `pipelines/cms_medicare/ingest.py` |
| `_create_indexes()` (BTREE npi FATAL-if-fails before publish; BITMAP categoricals) | `pipelines/cms_medicare/ingest.py` |
| `ORDER BY npi` sorted write (fragment clustering) | `pipelines/nppes/materialize_analytical.py:_stream_to_lance` |
| `ops.*_runs` ledger DDL + `_record_run` | any of the above |

**Assembly pattern** (Tier-2 `build_all`): base = `SELECT npi FROM nppes_provider` (the LEFT anchor). For each source, scan once from R2 into a DuckDB temp table projecting **only** the columns aggregated (NOT all 86/256 cols), build a `WITH <src> AS (SELECT npi, <aggs> ... GROUP BY npi)` CTE, then one final `SELECT base.* , <each src cols> FROM base LEFT JOIN <src> USING(npi) ... ORDER BY npi`. Stream to local Lance, index, publish, verify.

**DuckDB config:** `threads=8`, `memory_limit='20GB'`, `temp_directory` under `/tmp` SCRATCH, `preserve_insertion_order=false`. **Modal:** `memory=32768`, `cpu=8.0`, `ephemeral_disk=524288`, `LANCE_BYPASS_SPILLING=true`. (The Tier-1 giant rollups reuse the medicare giant envelope — `memory=49152`; B2 GROUP BY measured at 10.7s so even 49 GiB is ample.)

**Output:** `s3://data-sink/active/provider_360/snapshot=YYYY-MM/` (use the current month, e.g. `2026-06`; base NPPES is `2026-05` — record both vintages in a provenance column + the ledger).

---

## 6. Index plan
- **BTREE** `npi` (clustered via `ORDER BY npi` — the point-lookup + cohort-prune key; FATAL if build fails, abort before publish).
- **BITMAP** (low-card cohort filters): `primary_taxonomy_code`, `entity_type_code`, `is_active`, `practice_state`, `med_a1_provider_type`, `mips_final_score_year`, `med_a1_latest_year`.
- **BTREE** `practice_zip5`, `last_name` (cohort/geo/name search), `primary_practice_group_enrlmt_id` (practice rollup lookups).
- Define `EXPECTED_INDEX_COUNT = len(btree)+len(bitmap)` and hard-assert it in `_verify_published` (mirrors Open Payments).

---

## 7. Acceptance gates (run on local stage BEFORE publish, re-run on R2 after)
- **G1 — row count == exactly 9,551,447** (base, no fan-out — any deviation means the join multiplied rows; investigate before publish).
- **G2 — npi unique** (`count(*) == count(DISTINCT npi)`).
- **G3 — attach rates sane**: log `has_a1`/`has_b1`/`has_svc`/`has_rx`/`has_op`/`has_mips`/`has_ffs_enrollment` fractions; assert they're within expected bands (e.g. a1 ~20%, op present for payment-active NPIs). A sudden drop = a broken join.
- **G4 — suppression preserved**: assert each `*_sprsn_*` column exists in the output schema and carries non-coalesced VARCHAR values; assert no suppressed numeric was turned into 0.
- **G5 — no fuzzy attributes**: schema contains zero name/geo-resolved or corporate-identity columns (only published-key attaches).
- **G6 — money width**: `med_*_lifetime_*` declared `DECIMAL(38,2)`; assert 0 overflow nulls.
- **G7 — clustering / latency**: warm `WHERE npi=?` point-lookup asserted sub-100ms (cold recorded); a cohort `WHERE primary_taxonomy_code=? AND practice_state=?` prunes fragments.
- **G8 — deactivated kept**: `count(*) WHERE is_active=false == 343,321`.

Record every gate result (jsonb) in `ops.provider_360_runs`.

---

## 8. Resolved design decisions (already decided — do not re-litigate)
1. **Two tiers.** Pre-roll A2/B2/C3 into Tier-1 `*_rollup` datasets; aggregate A1/B1/taxonomy/identifier/enrollment inline in Tier-2.
2. **`latest_*` = per-NPI per-source MAX(program_year)**, never a global 2024.
3. **Growth anchors:** A1 = 2019→latest; B1 = 2019→2020 (its terminal year). NULL where the NPI lacks both endpoints.
4. **v1 = scalars** (latest + first/last/lifetime/growth + top-N). **Defer dense per-year `LIST<>` arrays to v2** (NPIs are non-contiguous; arrays need a parallel year list — not worth v1 bloat).
5. **QPP collapse:** latest program_year → `MAX(final_score)`.
6. **Practice "primary" = largest-fan-in group** (deterministic).
7. **Deactivated stubs kept**; their CMS attaches are NULL; `has_*` flags false.
8. **DECIMAL(38,2)** for all lifetime sums; suppression flags VARCHAR preserved.

## 9. Open questions for the implementer (decide + document; recommendation given)
- **Tier-1 rollup vintages** — the rollups are non-snapshot leaves at their own commit; provider_360 is snapshot-partitioned. *Rec:* stamp each Tier-1 rollup's source max-year into provider_360 provenance columns; rebuild Tier-1 when the underlying CMS dataset refreshes.
- **`rx_top3_generics` / `svc_rbcs_cat_mix` shape** — JSON string vs `LIST<STRUCT>`. *Rec:* JSON string in v1 (simplest, queryable via `json_extract`); structs in v2 if needed.
- **RBCS temporal drift** — RBCS is a single RY2025 snapshot; HCPCS→cat assignments drift across 2013–2024. *Rec:* accept the current mapping for v1 (the `rbcs_latest_assignment='1'` rows); note the drift.
- **C3 DME-supplier scope** — only 99,387 supplier NPIs (1% of base, mostly DME companies). *Rec:* include — it's a clean 1:1 attach and the `is_dme_supplier` flag is itself a useful segment.
- **`cms_ownership` vs the rollup** — verify whether `cms_provider_payment_rollup` already carries ownership totals before adding a second aggregate (avoid double-surfacing).

---

## 10. Deliverables
1. `pipelines/cms_open_payments/materialize_service_rollup.py` (or a new `pipelines/provider_360/` dir) — Tier-1 rollups for A2, B2, C3 (the Open Payments one already exists).
2. `pipelines/provider_360/materialize.py` — the Tier-2 Modal app (`provider-360-pipelines`).
3. `pipelines/provider_360/ops_provider_360_runs.sql` — ledger mirror.
4. Landed `active/cms_physician_service_rollup/`, `active/cms_partd_drug_rollup/`, `active/cms_dme_supplier_rollup/`, and `active/provider_360/snapshot=YYYY-MM/` — each verified (rows + indices + gates).
5. A verification script proving: (a) `WHERE npi=?` returns the full 360 in one sub-100ms lookup; (b) a cohort cut runs on indexed columns; (c) all 8 gates pass.

## 11. Sequence
1. Build + land the 3 Tier-1 rollups (A2, B2, C3) — each is the `materialize_resolution.py` pattern, one source.
2. Build the Tier-2 `provider_360` assembly (base + inline A1/B1/child + Tier-1 joins + QPP + practice).
3. Index → publish-swap → verify → run gates.
4. Ship (PR → merge → pull). Add a Trigger.dev cadence later (rebuild when NPPES + CMS sources refresh).

**Done = `active/provider_360/snapshot=YYYY-MM/` exists, 9,551,447 rows, all gates green, `SELECT * FROM provider_360 WHERE npi=?` returns the full picture in one indexed lookup.**

---

## 12. `practice_group_360` — the buyable-unit companion (strategic capstone)

provider_360 is 1-row-per-NPI, but **the unit a PE firm acquires is the practice group**, not the individual. Build a companion **`practice_group_360`** (1 row per `rcv_bnft_enrlmt_id`) that rolls the members' provider_360 economics up to the group — the table a PE associate actually queries:

```sql
SELECT * FROM practice_group_360
WHERE state='TX' AND member_count BETWEEN 3 AND 15
  AND top_specialty LIKE '%Nephrology%' AND avg_panel_risk_score > 1.2
  AND dual_share < 0.30 ORDER BY total_medicare_paid_usd DESC;     -- ranked acquisition targets
```

**Grain:** 1 row per group ENRLMT_ID (`rcv_bnft_enrlmt_id` from reassignment). **Keys only** (ENRLMT_ID, npi) — no fuzzy resolution. Build AFTER provider_360 (it joins the member NPIs to their provider_360 rows).

**Columns:** `group_enrlmt_id` (PK) · `org_name` (from enrollment, 99.2% populated) · `group_state` · `member_count` (fan-in, the size signal) · `member_npis` `LIST<string>` · `total_medicare_paid_usd` (sum members' `med_a1_lifetime_mdcr_pymt` — DECIMAL(38,2)) · `total_rx_cost_usd` · `avg_panel_risk_score` (avg members' `med_a1_panel_avg_risk_score`) · `avg_dual_share` · `top_specialty` / `specialty_mix` (mode/agg of members' `primary_taxonomy_code`) · `avg_mips_score` · `independent_member_count` (members with `is_independent_candidate`) · `total_op_payments_usd` · `n_states` (geographic spread). Index: BTREE `group_enrlmt_id`, BITMAP `group_state`, BTREE `org_name`, `member_count`.

**Acceptance:** 1 row per distinct `rcv_bnft_enrlmt_id` in the reassignment graph; `member_count == reassignment fan-in`; `total_medicare_paid_usd` reconciles to the sum of member provider_360 rows.

---

## 13. v2 validation audit (what the adversarial review verified against live data)
- QPP `participation_type` 100% NULL in 2023–24 (1,028,915 rows); `participation_option`/`reporting_option` live → B1 fix.
- QPP 86,952 `(npi, program_year)` pairs with >1 row (≤15) → mandatory pre-agg, else G1 fails → B2 fix.
- `avg_mdcr_pymt_amt × tot_srvcs` types to `DECIMAL(37,2)`; `sum` overflowed to NULL for the top NPI until cast to DOUBLE → B3 fix.
- A1 reconstruction `avg×tot_srvcs` matches published top-line within 0.02–0.08% (NPI 1538144910: $214,613,718 vs $214,653,948); `avg×tot_bene_day_srvcs` off 4–5% → confirms the multiplier.
- Practice graph: 836,559 NPIs (44%) reassign to >1 group; 66,880 have largest≥100 / smallest≤5 → "largest fan-in" inverts the independent signal → B4 fix.
- Attach rates (exact, for G3): a1 1,923,751 (20.1%) · b1 1,655,349 (17.3%) · svc 1,694,622 (17.7%) · op 1,603,039 (16.8%) · mips 1,277,404 (13.4%) · ffs-enrollment 2,556,645 (26.8%).
- Panel economics populated: `bene_avg_risk_scre` 845,753 NPIs/2023 (0.33–13.66); `bene_cc_ph_diabetes_v2_pct` 1.05M NPIs.
- `cms_provider_payment_rollup` already carries ownership totals (6,271 NPIs) → ownership re-agg cut.
