# `govcon_active_awards` column-expansion analysis — what to add before a single rebuild

**Mode:** READ-ONLY recon → recommendation. **Date:** 2026-06-20 (UTC). **Decision gate:** align on rebuild scope before executing.
**Source:** `usaspending_api_fresh/contract_prime_txn` (Lance v22, **297 cols**, 1,518,807 txn rows, all-VARCHAR FPDS download schema).
**Target:** `govcon_active_awards` (Lance v13, currently **38 source cols** consumed by `materialize_active_awards.py` `_SRC_COLS`).
**Gap:** **259 columns present in `contract_prime_txn` are NOT pulled into `govcon_active_awards`.**
All population %s and value distributions below are live-measured (txn grain, 1,518,807 rows). Probes: `/tmp/txn_gap.py`, `/tmp/txn_cand_values.py`.

---

## 0. Headline finds (things we'd otherwise leave on the table)

1. **`subcontracting_plan` / `subcontracting_plan_code` — 74.9% populated.** This is a **structured FPDS small-business-subcontracting-plan flag** — the demand signal we previously believed lived only in solicitation text. Verified code distribution (txn grain):

   | code | n | standard FPDS meaning (verify via the `subcontracting_plan` text col) |
   |---|---:|---|
   | B | 840,083 | plan not required (e.g. small-business prime / under threshold) |
   | (empty) | 380,594 | — |
   | F | 132,510 | **individual subcontract plan** |
   | G | 103,112 | **commercial subcontract plan** |
   | A | 34,402 | no plan — no subcontracting possibilities |
   | C | 16,899 | **plan required, incentive not included** |
   | H | 9,969 | **DoD comprehensive subcontract plan** |
   | D | 872 | plan required, incentive included |
   | E | 366 | plan required (pre-2004) |

   **~263,700 txns carry an actual subcontracting plan (C/D/E/F/G/H).** This is a direct, structured "this prime is obligated to subcontract to small business" marker — far better than the FAR-threshold inference we discussed earlier. **Add it.**

2. **`solicitation_identifier` — 21.8% populated** — real solicitation numbers (`W56HZV13R0022`, `89303021QFE000005`, …), same key space as `solicitation_number` in `govcon_scope_vectors` / `sam_opps_attachment_manifest`. This is the **structured bridge** from an active award to its harvested scope/requirements. **Add it** (it's the join key, even at 21.8%).

3. **`prime_award_base_transaction_description` — 100% populated** — the scope text (the original §7 ask). **Add it** + derived `scope_words` / `has_substantive_scope` flags.

4. **Competition + contract-structure enums are ~100% populated and clean** (`extent_competed`, `type_of_contract_pricing`, `contract_bundling`, `solicitation_procedures`) — directly relevant to subcontracting dynamics; cheap to add.

---

## 1. Tiered recommendation

Convention (matches existing): for FPDS categoricals, store **both** the human-readable text and the `_code`. Store derived booleans/ints for the heavy filters.

### TIER 1 — add now (directly serves scope-mining + sub-targeting). ~24 raw cols + 5 derived.

| column(s) | pop% | why |
|---|---:|---|
| `prime_award_base_transaction_description` | 100 | scope narrative (the §7 ask) |
| `transaction_description` | 100 | secondary narrative (latest-txn); keep for completeness |
| `subcontracting_plan` + `_code` | 74.9 | **structured subcontracting-plan demand signal** |
| `solicitation_identifier` | 21.8 | **structured bridge** to scope_vectors / opps / requirements |
| `solicitation_date` | 76.8 | solicitation timing |
| `extent_competed` + `_code` | 99.7 | competed vs not (teaming dynamics) |
| `number_of_offers_received` | 40.8 | competitive intensity (note: `999` is a sentinel; `''`=59%) |
| `type_of_contract_pricing` + `_code` | 99.9 | FFP / cost-plus / T&M — subcontracting economics |
| `contract_bundling` + `_code` | 99.9 | bundled/consolidated → higher subcontracting |
| `total_dollars_obligated` | 100 | cumulative obligation (we only have per-txn + CTV) |
| `base_and_exercised_options_value` | 94.4 | value through exercised options (between current & potential) |
| `usaspending_permalink` | 100 | public award URL (UI deep-link) |
| **curated prime socioeconomic flags** (12): `service_disabled_veteran_owned_business`, `veteran_owned_business`, `women_owned_small_business`, `economically_disadvantaged_women_owned_small_business`, `woman_owned_business`, `historically_underutilized_business_zone_hubzone_firm`, `c8a_program_participant`, `small_disadvantaged_business`, `self_certified_small_disadvantaged_business`, `sba_certified_8a_joint_venture`, `joint_venture_women_owned_small_business`, `emerging_small_business` | 100 | the **prime's** point-in-time certifications (clean `t`/`f`); complements `type_of_set_aside` |

**Derived (computed at build):** `scope_words` (int), `scope_chars` (int), `has_substantive_scope` (≥15 words & ≥120 chars & non-junk — per the description diagnostic), `has_directional_scope` (≥8 words & ≥1 scope term), `has_subcontracting_plan` (`subcontracting_plan_code` ∈ {C,D,E,F,G,H}).

### TIER 2 — add now if we're rebuilding anyway (context / work-character; low marginal cost). ~20 cols.

| column(s) | pop% | why |
|---|---:|---|
| `solicitation_procedures` + `_code` | 99.7 | negotiated / multiple-award fair-opp / SAP / sole-source |
| `commercial_item_acquisition_procedures` + `_code` | 99.9 | FAR 12 commercial |
| `performance_based_service_acquisition` (+`_code`) | 100 | PBSA — **155,995 YES** = service-heavy work |
| `construction_wage_rate_requirements` (+`_code`) | 100 | Davis-Bacon — **22,738 YES** = construction labor |
| `labor_standards` (+`_code`) | 100 | Service Contract Act — **108,908 YES** = service labor |
| `consolidated_contract` + `_code` | 99.2 | consolidation flag |
| `multi_year_contract` + `_code` | 54.4 | multi-year |
| `undefinitized_action` + `_code` | 97.3 | UCA |
| `fair_opportunity_limited_sources` + `_code` | 29.9 | IDIQ task-order competition |
| `other_than_full_and_open_competition` + `_code` | 17.6 | sole-source justification |
| `recipient_state_code`, `recipient_city_name`, `recipient_zip_4_code` | ~98 | **prime HQ** location (vs PoP location we already have) |
| `funding_agency_name`, `funding_sub_agency_name`, `funding_office_name` | 99.8 | the funding/buying office identity |
| `primary_place_of_performance_county_name` | 91.9 | finer PoP geo |
| `place_of_manufacture` (+`_code`), `domestic_or_foreign_entity` | 94+ | Buy-American / domestic execution |
| `organizational_type` | 98.8 | single summary of entity type (cheaper than the 60 flags) |
| `last_modified_date`, `action_date_fiscal_year` | 100 | provenance / FY filtering |

### TIER 3 — optional / niche (defer unless a use case appears).
`materials_supplies_articles_equipment` (Walsh-Healey), `cost_accounting_standards_clause` (78.5), `clinger_cohen_act_planning`, `government_furnished_property`, `recovered_materials_sustainability`, `epa_designated_product`, `information_technology_commercial_item_category` (47.5), `dod_acquisition_program` (47.0), `major_program` (19.9), `a76_fair_act_action` (56.1), `simplified_procedures_for_certain_commercial_items`, `purchase_card_as_payment_method`, IDV structure (`multiple_or_single_award_idv`, `type_of_idc`, `idv_type_code` — all <6% because most rows aren't IDV parents), `number_of_actions`, `transaction_number`, `modification_number`, `action_type`, `total_outlayed_amount_for_overall_award` (13.5), `recipient_name_raw`, `recipient_doing_business_as_name` (8.0), `contract_financing` (48.9).

### SKIP — dead, sparse, or noise for this substrate. ~180 cols.
- **Dead:** `recipient_duns`, `recipient_parent_duns` (0.0%), `sam_exception`/`_description` (0.0%), `research`/`_code` (0.2%), `program_acronym` (0.8%), `other_statutory_authority` (0.8%).
- **Sparse supplementals:** COVID-19 / IIJA obligated/outlayed amounts (0.3–1.1%), `price_evaluation_adjustment_preference_percent_difference` (3.6%).
- **~60 entity-taxonomy flags** (100% populated but low-signal here): all government-entity types (`us_state_government`, `city_local_government`, `school_district_local_government`, `township_local_government`, `port_authority`, `airport_authority`, `council_of_governments`, `transit_authority`, `housing_authorities_public_tribal`, …), university/education types (`1862_/1890_/1994_land_grant_college`, `historically_black_college`, `tribal_college`, `private_university_or_college`, `veterinary_college`, `school_of_forestry`), legal-form flags (`sole_proprietorship`, `partnership_or_limited_liability_partnership`, `corporate_entity_tax_exempt`/`not_tax_exempt`, `subchapter_scorporation`, `limited_liability_corporation`), and misc (`hospital_flag`, `veterinary_hospital`, `foundation`, `domestic_shelter`, `manufacturer_of_goods`, `nonprofit_organization`, `for_profit_organization`, `the_ability_one_program`, `receives_contracts*`). → folded into the single `organizational_type` (TIER 2) where needed; full detail is available in `sam_master_entities`.
- **Ethnic-ownership breakdowns** (`black_american_owned_business`, `hispanic_american_owned_business`, `native_american_owned_business`, `asian_pacific_american_owned_business`, `subcontinent_asian_asian_indian_american_owned_business`, `american_indian_owned_business`, `alaskan_native_*`, `native_hawaiian_*`, `tribally_owned_firm`, `minority_owned_business`, `other_minority_owned_business`): superseded by `small_disadvantaged_business` (TIER 1) for targeting; full detail in the registry.
- **Exec comp:** `highly_compensated_officer_1..5_name/_amount` (16–18%).
- **Accounting strings:** `treasury_accounts_funding_this_award`, `federal_accounts_funding_this_award`, `object_classes_funding_this_award`, `program_activities_funding_this_award` (19–30%).
- **Phone/fax/raw addr:** `recipient_phone_number`, `recipient_fax_number`, `recipient_address_line_2`.

---

## 2. Net rebuild footprint

- **TIER 1 + TIER 2** ≈ **44 raw columns + 5 derived** added → `govcon_active_awards` grows from 43 to ≈ **92 columns**. Still tiny (189,274 rows; single fragment).
- **New BTREE candidates:** `solicitation_identifier` (bridge join key), `scope_words`, `total_dollars_obligated`, `base_and_exercised_options_value`.
- **New BITMAP candidates:** `has_subcontracting_plan`, `subcontracting_plan_code`, `has_substantive_scope`, `has_directional_scope`, `extent_competed`, `type_of_contract_pricing`, `contract_bundling`, `performance_based_service_acquisition`, `construction_wage_rate_requirements`, `labor_standards`, the 12 curated socioeconomic flags.
- **Source-feed implication:** all additions are projections from `contract_prime_txn` — the build already scans that feed, so the only change is extending `_SRC_COLS` + adding the derived expressions. No new data source, no new join. (`subcontracting_plan` and the socioeconomic flags are pass-through; the scope flags are computed from `prime_award_base_transaction_description`.)

---

## 3. Decisions to align on (before rebuild)

1. **Scope of add:** TIER 1 only, or TIER 1 + TIER 2? (Recommendation: TIER 1 + TIER 2 — marginal cost is near-zero since we're already scanning the feed and rebuilding once.)
2. **Socioeconomic flags:** curated 12 (recommended) vs all ~80 entity flags vs none (rely on `sam_master_entities`). These are the **prime's** point-in-time FPDS certs; the registry has current certs. Some duplication is acceptable (point-in-time ≠ current).
3. **Derived scope flags:** confirm the `has_substantive_scope` / `has_directional_scope` thresholds (≥15w/≥120c and ≥8w/scope-term) from `PRIME_TXN_DESCRIPTION_CONTENT_DIAGNOSTIC.md` §7, or tune.
4. **`subcontracting_plan` decode:** store text + code (recommended) so the standard FPDS labels are verbatim, not inferred.
5. **CUI:** `prime_award_base_transaction_description` is the FPDS public download field (not CUI); confirm no marking gate needed (the harvested-attachment CUI rules do not apply to this FPDS column).

Once aligned, the rebuild is a single edit to `materialize_active_awards.py` (`_SRC_COLS` + derived SELECT + index lists) and one `--cmd build`. No second rebuild required if we lock the column set now.

---

## 4. Verify
`/tmp/txn_gap.py` (full 259-col population table) and `/tmp/txn_cand_values.py` (TIER-1 categorical distributions) reproduce every number above against the R2 SoR.
