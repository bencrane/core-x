# Subaward Canonical — Field Dictionary & Rebuild Menu

_Authoritative field definitions from the USAspending Data Dictionary (122 subaward-relevant rows of 457, fetched 2026-07-02); joined to the 210-column BULK source `subaward_search` (rpt.* vocabulary) and the 118-column FRESH source `contract_subaward` (bulk_download/FSRS vocabulary). Canonical vocabulary = FRESH/bulk_download names; BULK rpt.* is crosswalked in. Native USAspending/SAM.gov column names are preserved verbatim._

**Dataset:** `s3://data-sink/active/usaspending_subaward_canonical/` (Lance v2.1) · worker [`usaspending_subaward_canonical.py`](../../pipelines/usaspending/usaspending_subaward_canonical.py).

**Scope:** CONTRACT-ONLY (`prime_award_group='procurement'`). BULK contract subset = 2,643,501 rows; FRESH = 321,204 rows. Grant subawards (7.16M, BULK-only) are a separate future canonical.

**Status:** **91 columns** on the spine · 7 key · 64 core (dual-sourced) · 18 enrich (single-source) · 2 prov.

**Legend:** ✅ IN = on the spine · ➕ RESCUE = added by the adversarial pass · ⬜ out = in a source, not carried · 🔒 internal = pg/ETL/derived, no shipped definition

### Build result (verified prod, 2026-07-02)
`rows_out = 1,315,680` (exactly the reconciliation-probe centerline: 1,301,358 BULK-contract composites ∪ 14,322 FRESH-only). `pk_unique=true`, 0 dupes, `subaward_unique_key` 1:1 with the composite. `canonical_source`: bulk 1,300,059 · fresh 15,621 (= 14,322 FRESH-only + 1,299 FRESH corrections over shared composites). `null_key_dropped=37,078`. 30 indices (11 BTREE + 19 BITMAP). `max(subaward_action_date)=2026-06-29` (clamped).

---

## Reconciliation contract (locked)

- **Two feeds only:** BULK `subaward_search` (TYPED rpt.*) + FRESH `contract_subaward` (all-VARCHAR bulk_download). **No monthly/delta/tombstone** (the prime spine's R5/R6) — a subaward is superseded, never deleted.
- **PK (composite, structural post-collapse):** `(prime_award_unique_key, subaward_number)`. BULK = `(unique_award_key, subaward_number)`. 90.27% FRESH containment in BULK; 14,322 FRESH-only tail; 1,168,453 BULK-only body. A synthesized single-column key `subaward_unique_key = prime_award_unique_key|subaward_number` is carried for BTREE point-lookup; the fail-closed gate uses the TUPLE + a `subaward_unique_key`-distinct == composite-distinct collision gate.
- **Collapse:** each source collapsed latest-per-composite, `row_number()=1`, `ORDER subaward_last_modified_date DESC, <native surrogate> DESC`. BULK surrogate = `broker_subaward_id`; FRESH surrogate = `subaward_sam_report_id` (report grain, non-unique — the gate, not the id, guarantees determinism). FRESH re-pulls overlapping windows on its daily append → 94,276 duplicate `subaward_sam_report_id` rows the collapse resolves.
- **Precedence:** two-source per-composite argmax on the unified mod-frontier `subaward_last_modified_date`, **FRESH(1) > BULK(2)** on tie. Frontiers: BULK `broker_updated_at`→2026-04-24, FRESH `subaward_sam_report_last_modified_date`→2026-06-29 (FRESH wins the recent tail). Cross-clock (broker ETL time vs SAM report mtime): FRESH generally ≥ BULK, and tie→FRESH, so FRESH is the freshness overlay by construction.
- **NULL-prime drop:** 37,078 BULK contract rows (1.4%) with NULL `unique_award_key` cannot form the PK → dropped. FRESH has 0 NULL prime.

---

## ✅ Key (7) — composite PK parts + resolution keys, BTREE

| canonical | BULK rpt | FRESH download | note |
|---|---|---|---|
| `subaward_unique_key` | `unique_award_key`\|`subaward_number` | `prime_award_unique_key`\|`subaward_number` | synthesized single-col canonical PK; BTREE point-lookup. Gate uses the TUPLE |
| `prime_award_unique_key` | `unique_award_key` | `prime_award_unique_key` | PK part 1 (prime link). 98.6/100 |
| `subaward_number` | `subaward_number` | `subaward_number` | PK part 2. 100/100. Unique only WITHIN a prime |
| `subawardee_uei` | `sub_awardee_or_recipient_uei` | `subawardee_uei` | joins govcon_subawardee_designations / sam_entity_master. 100/100 |
| `prime_awardee_uei` | `awardee_or_recipient_uei` | `prime_awardee_uei` | 98.5/100 |
| `prime_awardee_parent_uei` | `ultimate_parent_uei` | `prime_awardee_parent_uei` | 98.5/100 |
| `prime_award_piid` | `piid` | `prime_award_piid` | 98.6/100 |

## ✅ Core (64) — dual-sourced volatile facts

### Sub-award attribute / spending / recency
| canonical | BULK rpt | FRESH download | note |
|---|---|---|---|
| `subaward_amount` | `subaward_amount` | `subaward_amount` | DOUBLE. **ONLY sub-grain-safe SUM.** FSRS garbage sentinel (abs > $100B) NULLED on-spine — row survives, amount → NULL (safe to SUM directly, no consumer clamp) |
| `subaward_action_date` | `sub_action_date` | `subaward_action_date` | DATE. BTREE. FSRS sentinel (outside [1776-01-01, today], e.g. 2106) NULLED on-spine — safe for max()/date filters directly |
| `subaward_action_date_fiscal_year` | `sub_fiscal_year` | `subaward_action_date_fiscal_year` | BIGINT |
| `subaward_last_modified_date` | `broker_updated_at` | `subaward_sam_report_last_modified_date` (parsed) | TIMESTAMP. **The unified reconciliation mod-frontier = the 2-way argmax driver.** BTREE. Analog of FPDS `last_modified_date` |
| `subaward_description` | `subaward_description` | `subaward_description` | the LEAD capability signal (most-read field across consumers) |
| `subaward_type` | `subaward_type` | `subaward_type` | single-value ('sub-contract') in contract scope; NOT indexed |
| `subaward_sam_report_year` | `subaward_report_year` | `subaward_sam_report_year` | BIGINT |
| `subaward_sam_report_month` | `subaward_report_month` | `subaward_sam_report_month` | BIGINT |

### Sub-awardee entity
| canonical | BULK rpt | FRESH download | note |
|---|---|---|---|
| `subawardee_name` | `sub_awardee_or_recipient_legal` | `subawardee_name` | _raw twin cut |
| `subawardee_parent_uei` | `sub_ultimate_parent_uei` | `subawardee_parent_uei` | ~57% |
| `subawardee_parent_name` | `sub_ultimate_parent_legal_enti` | `subawardee_parent_name` | ~57% |
| `subawardee_dba_name` | `sub_dba_name` | `subawardee_dba_name` | ~62% |
| `subawardee_business_types` | `sub_business_types` | `subawardee_business_types` | RAW code string; the 12 decoded socio flags stay served UEI-keyed |
| `subawardee_address_line_1` | `sub_legal_entity_address_line1` | `subawardee_address_line_1` | heavily read by capability-profile consumers |
| `subawardee_city_name` | `sub_legal_entity_city_name` | `subawardee_city_name` | |
| `subawardee_state_code` | `sub_legal_entity_state_code` | `subawardee_state_code` | BITMAP |
| `subawardee_zip_code` | `sub_legal_entity_zip` | `subawardee_zip_code` | |
| `subawardee_country_code` | `sub_legal_entity_country_code` | `subawardee_country_code` | BITMAP |
| `subaward_recipient_cd_current` | `sub_legal_entity_congressional_current` | `subaward_recipient_cd_current` | current twin only |
| `subawardee_highly_compensated_officer_{1..5}_name` | `sub_high_comp_officer{n}_full_na` | `subawardee_highly_compensated_officer_{n}_name` | ~10-13% populated (only >$300k reporters) |
| `subawardee_highly_compensated_officer_{1..5}_amount` | `sub_high_comp_officer{n}_amount` | `subawardee_highly_compensated_officer_{n}_amount` | DOUBLE; snapshot, not additive; do not treat NULL as $0 |

### Sub-award place of performance
| canonical | BULK rpt | FRESH download | note |
|---|---|---|---|
| `subaward_primary_place_of_performance_city_name` | `sub_place_of_perform_city_name` | `subaward_primary_place_of_performance_city_name` | |
| `subaward_primary_place_of_performance_state_code` | `sub_place_of_perform_state_code` | `subaward_primary_place_of_performance_state_code` | BITMAP |
| `subaward_primary_place_of_performance_address_zip_code` | `sub_place_of_performance_zip` | `subaward_primary_place_of_performance_address_zip_code` | |
| `subaward_primary_place_of_performance_country_code` | `sub_place_of_perform_country_co` | `subaward_primary_place_of_performance_country_code` | BITMAP |
| `subaward_place_of_performance_cd_current` | `sub_place_of_performance_congressional_current` | `subaward_place_of_performance_cd_current` | current twin only |

### Prime context (award-grain-repeated — NEVER SUM at sub grain; dedup to `prime_award_unique_key`)
| canonical | BULK rpt | FRESH download | note |
|---|---|---|---|
| `prime_award_amount` | `award_amount` | `prime_award_amount` | DOUBLE. GRAIN HAZARD — MAX/ANY per prime key, highest blast radius |
| `prime_award_latest_action_date` | `action_date` | `prime_award_latest_action_date` | DATE |
| `prime_award_latest_action_date_fiscal_year` | `fy` (VARCHAR) | `prime_award_latest_action_date_fiscal_year` | BIGINT |
| `prime_award_naics_code` | `naics` | `prime_award_naics_code` | BTREE. `sub_naics` denorm twin cut |
| `prime_award_naics_description` | `naics_description` | `prime_award_naics_description` | |
| `prime_award_base_transaction_description` | `award_description` | `prime_award_base_transaction_description` | |
| `prime_award_parent_piid` | `parent_award_id` | `prime_award_parent_piid` | IDV-only ~35-50% |
| `prime_award_awarding_agency_code` | `awarding_agency_code` | `prime_award_awarding_agency_code` | BITMAP |
| `prime_award_awarding_agency_name` | `awarding_agency_name` | `prime_award_awarding_agency_name` | native name, not toptier |
| `prime_award_awarding_sub_agency_code` | `awarding_sub_tier_agency_c` | `prime_award_awarding_sub_agency_code` | BITMAP |
| `prime_award_awarding_sub_agency_name` | `awarding_sub_tier_agency_n` | `prime_award_awarding_sub_agency_name` | |
| `prime_award_awarding_office_code` | `awarding_office_code` | `prime_award_awarding_office_code` | |
| `prime_award_awarding_office_name` | `awarding_office_name` | `prime_award_awarding_office_name` | |
| `prime_award_funding_agency_code` | `funding_agency_code` | `prime_award_funding_agency_code` | BITMAP |
| `prime_award_funding_agency_name` | `funding_agency_name` | `prime_award_funding_agency_name` | |
| `prime_award_funding_sub_agency_code` | `funding_sub_tier_agency_co` | `prime_award_funding_sub_agency_code` | BITMAP |
| `prime_award_funding_sub_agency_name` | `funding_sub_tier_agency_na` | `prime_award_funding_sub_agency_name` | |
| `prime_award_funding_office_code` | `funding_office_code` | `prime_award_funding_office_code` | |
| `prime_award_funding_office_name` | `funding_office_name` | `prime_award_funding_office_name` | |
| `prime_awardee_name` | `awardee_or_recipient_legal` | `prime_awardee_name` | _raw twin cut |
| `prime_awardee_parent_name` | `ultimate_parent_legal_enti` | `prime_awardee_parent_name` | _raw twin cut |
| `prime_awardee_business_types` | `business_types` | `prime_awardee_business_types` | RAW code string |
| `prime_awardee_state_code` | `legal_entity_state_code` | `prime_awardee_state_code` | BITMAP. HQ street/city/zip CUT (redundant with sam_entity_master.physical_*) |
| `prime_awardee_country_code` | `legal_entity_country_code` | `prime_awardee_country_code` | BITMAP |
| `prime_award_summary_recipient_cd_current` | `legal_entity_congressional_current` | `prime_award_summary_recipient_cd_current` | current twin only |
| `prime_award_primary_place_of_performance_city_name` | `place_of_perform_city_name` | `prime_award_primary_place_of_performance_city_name` | |
| `prime_award_primary_place_of_performance_state_code` | `place_of_perform_state_code` | `prime_award_primary_place_of_performance_state_code` | BITMAP |
| `prime_award_primary_place_of_performance_address_zip_code` | `place_of_performance_zip` | `prime_award_primary_place_of_performance_address_zip_code` | |
| `prime_award_primary_place_of_performance_country_code` | `place_of_perform_country_co` | `prime_award_primary_place_of_performance_country_code` | BITMAP |
| `prime_award_summary_place_of_performance_cd_current` | `place_of_performance_congressional_current` | `prime_award_summary_place_of_performance_cd_current` | current twin only |

## ✅/➕ Enrich (18) — single-source, LEFT JOIN to the per-composite collapse, source-correlated NULL cliffs

### BULK-only (`feed_expr=None` → NULL on `canonical_source='fresh'` rows)
| canonical | BULK rpt | pct | note |
|---|---|---|---|
| `subawardee_county_code` | `sub_legal_entity_county_code` | 95.1 | single county anchor (prime/sub-PoP county cut) |
| `prime_award_type` | `prime_award_type` | 98.6 | BITMAP |
| `prime_award_product_or_service_code` | `product_or_service_code` | 98.6 | BITMAP. net-new (FRESH carries NO PSC) |
| `prime_award_product_or_service_description` | `product_or_service_description` | 98.6 | |
| `prime_award_type_of_set_aside_code` | `type_set_aside` | 62.6 | BITMAP |
| `prime_award_extent_competed` | `extent_competed` | 98.6 | BITMAP |
| `prime_award_type_of_contract_pricing` | `type_of_contract_pricing` | 98.6 | BITMAP |
| ➕ `broker_subaward_id` | `broker_subaward_id` | 100 | BIGINT. BULK collapse tie-break surrogate + provenance (FPDS `transaction_id` analog) |

### FRESH-only (`bulk_expr=None` → NULL on `canonical_source='bulk'` rows)
| canonical | FRESH download | pct | note |
|---|---|---|---|
| `prime_award_base_action_date` | `prime_award_base_action_date` | 100 | award vintage/start |
| `prime_award_base_action_date_fiscal_year` | `prime_award_base_action_date_fiscal_year` | 100 | |
| `prime_award_period_of_performance_start_date` | `prime_award_period_of_performance_start_date` | 100 | |
| `prime_award_period_of_performance_current_end_date` | `prime_award_period_of_performance_current_end_date` | 89.4 | active/closed timing (join the prime spine for authoritative PoP on BULK-only rows) |
| `prime_award_period_of_performance_potential_end_date` | `prime_award_period_of_performance_potential_end_date` | 89.4 | option-tail boundary |
| `prime_award_federal_accounts_funding_this_award` | `prime_award_federal_accounts_funding_this_award` | 90.4 | color-of-money |
| `prime_award_object_classes_funding_this_award` | `prime_award_object_classes_funding_this_award` | 90.4 | joins psctool maps |
| ➕ `prime_award_disaster_emergency_fund_codes` | `prime_award_disaster_emergency_fund_codes` | 87.8 | BITMAP. surge/contingency GTM lens (supersedes sparse COVID/IIJA money cols) |
| `subaward_sam_report_id` | `subaward_sam_report_id` | 100 | FRESH collapse tie-break surrogate + provenance. Report grain, non-unique, **NOT a PK, NEVER join/dedup on it** |
| `usaspending_permalink` | `usaspending_permalink` | 100 | deep-link serving surface |

## Prov (2)
| canonical | source | note |
|---|---|---|
| `canonical_source` | derived per-key = winning core row's src | ∈ {fresh, bulk}; BITMAP. The true per-key winner, never a partition literal |
| `built_at` | injected | ONE naive-UTC build literal (NOT now()) |

> The raw per-source mod-frontiers `broker_updated_at` / `subaward_sam_report_last_modified_date` are folded into the unified core `subaward_last_modified_date` (each is 100%-NULL on the opposite leg by construction, so they are not carried separately — the winning leg's frontier + `canonical_source` is the meaningful recency signal).

---

## Grain / reconciliation hazards (carry to serving)
1. **Sub-grain vs award-grain:** ONLY `subaward_amount` is subaward-grain and safe to SUM. Every `prime_award_*` column is prime-award-grain **repeated on every subaward row of that prime** → dedup to `prime_award_unique_key` before any award rollup; **NEVER SUM at sub grain.** `prime_award_amount` is the highest-blast-radius footgun.
2. **Sentinels NULLED on-spine** (not carried raw — the repoint analysis showed carrying raw pushes a clamp burden onto every aggregating consumer): `subaward_amount` → NULL when abs > $100B (4 rows); `subaward_action_date` → NULL outside [1776-01-01, today] (2 rows). The ROW survives (the subaward happened), only the garbage value → NULL, so downstream `SUM(amount)` / `max(date)` are safe with no consumer-side clamp. Mirrors `contractor_award_summary`'s "null from sums, keep the row".
3. **Source-correlated NULL cliffs:** every single-source enrich column is NULL on the opposite leg by construction → union-grain populatedness reads below the single-source pct. Read as `canonical_source` correlation, not data loss; do not impute.
4. **Officer comp:** ~10-13% populated (only >$300k reporters). ~88% NULL is inherent — a forward capability surface, not a load-bearing read yet.

---

## Dead-column inventory (recorded as coverage facts, not carried)

| native column(s) | source | pct | reason |
|---|---|---|---|
| `business_categories` | BULK | 100 | decoded socio array — redundant with UEI-keyed govcon_subawardee_designations |
| `sub_naics` | BULK | 98.6 | rpt denorm of prime NAICS; consumers derive sub NAICS as mode(prime_award_naics_code) |
| `award_piid_fain` | BULK | 98.6 | collapses to piid on contract scope |
| `prime_award_group` | BULK | 100 | scope-partition literal ('procurement'), not a fact |
| `sub_total_obl_bin`, `subaward_recipient_hash`, `subaward_recipient_level` | BULK | 100/59/59 | BULK-internal derivations |
| `program_activities` | BULK | 60.8 | prime program-activity string; superseded by object_classes/federal_accounts |
| `prime_award_treasury_accounts_funding_this_award` | FRESH | 90.4 | redundant decomposition of federal_accounts + object_classes; no join surface |
| `prime_award_program_activities_funding_this_award` | FRESH | 87.3 | Treasury-Account grouping redundant with funding strings kept |
| `prime_award_total_outlayed_amount` | FRESH | 77.7 | award-grain outlay, FY22+ vintage-biased, no consumer |
| `prime_award_*_COVID-19_supplementals`, `*_IIJA_supplemental` | FRESH | 1.8-13.9 | superseded by disaster_emergency_fund_codes |
| `prime_award_national_interest_action[_code]` | FRESH | 2.2 | sparse |
| `prime_award_project_title` / BULK `program_title` | FRESH/BULK | 0.0 | dead |
| `subawardee_duns`, `*_parent_duns`, `prime_awardee_duns`, DUNS `*_uniqu`/`*_unique_ide` | both | 0.9-95 | UEI supersedes DUNS |
| `*_foreign_postal_code`, BULK `*_foreign_posta` | both | 0.05-2.3 | below bar; ZIP+country carried |
| all `*_state_name`/`*_country_name`/`*_county_name`/`*_state_fips`/`*_county_fips` | both | — | geography name/FIPS derivable from codes |
| all `*_cd_original`/`_raw`/`_congressional` (non-current) | both | — | current twin carried |
| `sub_federal_agency_id/name`, `sub_funding_agency_id/name` | BULK | 98.6 | FSRS submitter dupes of prime hierarchy |
| `*_ts_vector`, `broker_created_at`, `date_submitted`, `ingested_at`, `internal_id`, `source_schema/table`, surrogate ids, `usaspending_snapshot_date`, `last_modified_date`, `pulled_from` | BULK | — | 🔒 internal/ETL/search |
| `cfda_*`, `grant_*`, `compensation_q*`, `recovery_model_*`, `sub_recovery_subcontract_amt`, `dunsplus4`, `fain`, `treasury_*`, `report_type`, `transaction_type`, `place_of_perform_scope\|street`, `sub_funding_office_*`, `business_type_code` | BULK | 0.0-5.5 | dead in contract scope |
| all agency `*_abbreviation` / toptier `_id` twins | BULK | — | pre-normalization dupes |

## Evidence
- [`usaspending_fpds_canonical.py`](../../pipelines/usaspending/usaspending_fpds_canonical.py) — reference COLUMN_SPEC shape + program-generation
- [`usaspending_api_subaward_fresh.py`](../../pipelines/usaspending/usaspending_api_subaward_fresh.py) — FRESH pull vocabulary (confirms subaward file carries NO PSC)
- [`materialize_subawardee_designations.py`](../../pipelines/serving/materialize_subawardee_designations.py) — 12 socio designations decoded UEI-keyed (the `business_categories` redundancy)
- [`sam_entity_master.py`](../../pipelines/sam_gov/sam_entity_master.py) — `business_types`/`naics_codes[]`/`psc_codes[]`/`physical_*` per UEI (prime-HQ redundancy)
- `build_subawardee_capability_profiles.py` + `materialize_sub_diversification.py` — subawardee NAICS = mode(prime_award_naics_code) (the `sub_naics` denorm proof)
