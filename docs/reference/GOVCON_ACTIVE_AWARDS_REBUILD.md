# `govcon_active_awards` — rebuilt reference (zero-join GTM hunting ground)

**Date:** 2026-06-20 (UTC) · **SoR:** `s3://data-sink/active/govcon_active_awards/` (Lance v2.1, snapshot-overwrite)
**Shape:** **189,272 rows · 124 columns · 60 scalar indexes** · `as_of_date` = 2026-06-20
**Worker:** `pipelines/serving/materialize_active_awards.py` · **Ledger:** `ops.active_awards_serving_runs` · **Landed:** [PR #557](https://github.com/bencrane/core-x/pull/557)
All figures below are live-measured against the R2 SoR by the probe in §9.

---

## 1. What this is

The active prime-award **work substrate** — one row per prime award still in performance — rebuilt from a 43-column scalar table into a **124-column standalone, zero-join table** purpose-built as the frontend sales-demo hunting ground. Storage is negligible; the design goal was GTM filtering flexibility with no joins for the common path.

It carries, inline: liveness, scope narrative + derived substance flags, the structured subcontracting-plan signal, the solicitation bridge key, competition/contract-structure/work-character attributes, dollars, funding office, raw recipient contact, and 23 recipient socioeconomic + demographic ownership flags.

**Source:** projection of `usaspending_api_fresh/contract_prime_txn` (1,247,391 distinct awards), collapsed to award grain (latest txn per award; deterministic tiebreak `action_date DESC, last_modified_date DESC, contract_transaction_unique_key DESC`).

**Membership (unchanged from the prior build):** `GREATEST(pop_current_end, pop_potential_end) >= as_of` **OR** both PoP ends NULL. An award is excluded only when definitively done (both ends elapsed). No subjective filters baked in.

---

## 2. What changed in the rebuild (43 → 124 cols)

Added per `ACTIVE_AWARDS_COLUMN_EXPANSION.md` (TIER 1 + TIER 2) plus the GTM directive (raw contact + full demographic flags):

- **Scope narrative** + 4 derived substance columns.
- **Subcontracting-plan** structured signal (text + code + boolean).
- **Solicitation bridge** (`solicitation_identifier`, date, procedures).
- **Competition** (extent_competed, number_of_offers_received, fair-opportunity, OTFO, commercial-item).
- **Contract structure** (pricing, bundling, consolidated, multi-year, undefinitized).
- **Work-character / compliance** (PBSA, Davis-Bacon, Service Contract Act, place_of_manufacture, domestic/foreign, organizational_type).
- **Dollars** (total_dollars_obligated, base_and_exercised_options_value).
- **Funding office** + **pop_county** + **usaspending_permalink**.
- **Raw recipient contact** (phone, fax, address line 1/2 + city/state/zip).
- **23 socioeconomic + demographic ownership flags** (curated SBA 12 + demographic 11), cast to BOOLEAN.

Text **and** `_code` are stored for every FPDS categorical.

---

## 3. Membership & liveness (measured)

| flag (boolean) | TRUE rows |
|---|---:|
| `active_current` (committed end ≥ as_of) | 142,294 |
| `active_potential` (incl. option years) | 148,789 |
| `has_option_tail` (unexercised govt option) | 27,126 |
| `pop_unknown` (no PoP date; kept + flagged) | 40,483 |
| **total rows** | **189,272** (= distinct `contract_award_unique_key`) |

Total value carried: `current_total_value_of_award` Σ = **$1,607.5B**; `total_dollars_obligated` Σ = **$1,342.5B**. Business size: SMALL BUSINESS 112,576 · OTHER THAN SMALL 76,695 · (null) 1.

---

## 4. Derived columns (built at materialize time)

| column | type | definition | measured |
|---|---|---|---|
| `scope_chars` | int64 | `length(base_d)`; 0 when description absent | max 3,907 |
| `scope_words` | int64 | whitespace-token count of `base_d`; 0 when absent | min 0 · median 6 · avg 9.6 · max 563 |
| `has_substantive_scope` | bool | `base_d` non-null & `scope_chars ≥ 120` & `scope_words ≥ 15` & `upper(base_d) NOT IN ('NONE','N/A','NOT APPLICABLE','SEE SCHEDULE')` | **28,323** |
| `has_directional_scope` | bool | `base_d` non-null & `scope_words ≥ 8` & ≥1 of 29 scope terms | **48,092** |
| `has_subcontracting_plan` | bool | `subcontracting_plan_code ∈ {C,D,E,F,G,H}` (COALESCE→FALSE) | **32,049** (0 nulls) |

`base_d` = `nullif(trim(prime_award_base_transaction_description), '')`. Scope definitions are verbatim from `PRIME_TXN_DESCRIPTION_CONTENT_DIAGNOSTIC.md` (29-term lexicon: PROVIDE, SERVICE, MAINTENANCE, SUPPORT, INSTALL, FURNISH, DELIVER, REPAIR, CONSTRUCT, SOFTWARE, SYSTEM, TRAINING, ENGINEER, EQUIPMENT, SUPPLY, SUPPLIES, MANAGEMENT, OPERATION, TECHNICAL, DESIGN, DEVELOP, INSPECT, TEST, UPGRADE, MATERIAL, LABOR, STUDY, ANALYSIS, RESEARCH). The substantive/directional counts reproduce the diagnostic's active-subset figures exactly.

---

## 5. The subcontracting-plan signal (structured)

`subcontracting_plan_code` distribution (award grain):

| code | meaning (FPDS; `subcontracting_plan` text col carries verbatim labels) | rows | `has_subcontracting_plan` |
|---|---|---:|---|
| B | plan not required (small-biz prime / under threshold) | 127,444 | false |
| (empty) | not reported | 25,150 | false |
| G | commercial subcontract plan | 18,288 | **true** |
| F | individual subcontract plan | 12,058 | **true** |
| A | no plan — no subcontracting possibilities | 4,629 | false |
| H | DoD comprehensive subcontract plan | 821 | **true** |
| C | plan required, incentive not included | 806 | **true** |
| D | plan required, incentive included | 56 | **true** |
| E | plan required (pre-2004) | ~20 | **true** |

`has_subcontracting_plan` = 32,049 active awards whose prime carries an actual plan (C/D/E/F/G/H).

---

## 6. Socioeconomic + demographic ownership flags (boolean; 0 nulls across all 23)

Curated SBA / set-aside (12) and demographic ownership (11) — the prime recipient's FPDS self-certifications at award:

| flag | TRUE | flag | TRUE |
|---|---:|---|---:|
| self_certified_small_disadvantaged_business | 45,098 | subcontinent_asian_asian_indian_american_owned_business | 6,939 |
| minority_owned_business | 30,109 | hispanic_american_owned_business | 6,078 |
| woman_owned_business | 25,152 | economically_disadvantaged_women_owned_small_business | 5,702 |
| veteran_owned_business | 24,716 | asian_pacific_american_owned_business | 4,761 |
| women_owned_small_business | 22,223 | american_indian_owned_business | 4,452 |
| service_disabled_veteran_owned_business | 20,693 | alaskan_native_corporation_owned_firm | 2,877 |
| c8a_program_participant | 13,146 | tribally_owned_firm | 1,960 |
| native_american_owned_business | 8,285 | other_minority_owned_business | 1,538 |
| black_american_owned_business | 7,787 | joint_venture_women_owned_small_business | 1,104 |
| historically_underutilized_business_zone_hubzone_firm | 6,646 | sba_certified_8a_joint_venture | 1,009 |
| small_disadvantaged_business | 734 | native_hawaiian_organization_owned_firm | 268 |
| | | emerging_small_business | 33 |

Two directive names were corrected to the actual feed columns: `alaskan_native_corporation_owned_firm` (not `alaskan_native_owned_corporation_or_firm`), `native_hawaiian_organization_owned_firm` (not `native_hawaiian_owned_business`). A swap test confirmed correct sourcing vs the `*_servicing_institution` lookalikes.

---

## 7. Full column catalog (124, with types)

**Identity & structure:** `contract_award_unique_key`(string), `award_id_piid`, `parent_award_id_piid`, `award_or_idv_flag`, `award_type`, `award_type_code`, `idv_type`.
**Time / liveness:** `pop_start`/`pop_current_end`/`pop_potential_end`/`ordering_period_end`/`latest_action_date`(date32), `action_date_fiscal_year`(int32), `last_modified_date`(string), `active_current`/`active_potential`/`has_option_tail`/`pop_unknown`(bool).
**Recipient/prime:** `recipient_uei`, `recipient_name`, `recipient_parent_uei`, `recipient_parent_name`, `cage_code`, `business_size`, `business_size_code`.
**Set-aside / NAICS / PSC:** `type_of_set_aside`(+`_code`), `naics_code`, `naics_description`, `psc_code`, `psc_description`.
**Dollars (double):** `federal_action_obligation`, `current_total_value_of_award`, `base_and_all_options_value`, `potential_total_value_of_award`, `total_dollars_obligated`, `base_and_exercised_options_value`.
**Agency / funding office:** `awarding_agency_name`, `awarding_sub_agency_name`, `funding_agency_name`, `funding_sub_agency_name`, `funding_office_name`.
**Place of performance:** `pop_state_code`, `pop_state_name`, `pop_city`, `pop_zip`, `pop_country_code`, `pop_county`.
**Scope:** `prime_award_base_transaction_description`(string), `transaction_description`, `scope_chars`(int64), `scope_words`(int64), `has_substantive_scope`(bool), `has_directional_scope`(bool).
**Subcontracting plan:** `subcontracting_plan`, `subcontracting_plan_code`, `has_subcontracting_plan`(bool).
**Solicitation:** `solicitation_identifier`, `solicitation_date`(date32), `solicitation_procedures`(+`_code`).
**Competition:** `extent_competed`(+`_code`), `number_of_offers_received`(int32), `commercial_item_acquisition_procedures`(+`_code`), `fair_opportunity_limited_sources`(+`_code`), `other_than_full_and_open_competition`(+`_code`).
**Contract structure:** `type_of_contract_pricing`(+`_code`), `contract_bundling`(+`_code`), `consolidated_contract`(+`_code`), `multi_year_contract`(+`_code`), `undefinitized_action`(+`_code`).
**Work-character / compliance:** `performance_based_service_acquisition`(+`_code`), `construction_wage_rate_requirements`(+`_code`), `labor_standards`(+`_code`), `place_of_manufacture`(+`_code`), `domestic_or_foreign_entity`(+`_code`), `organizational_type`.
**Links / recipient contact (UI):** `usaspending_permalink`, `recipient_state_code`, `recipient_city_name`, `recipient_zip_4_code`, `recipient_phone_number`, `recipient_fax_number`, `recipient_address_line_1`, `recipient_address_line_2`.
**Socioeconomic + demographic flags (bool ×23):** see §6.
**Build stamps:** `as_of_date`(date32), `built_at`(timestamp µs UTC).

---

## 8. Indexes (60) & zero-join query patterns

**BTREE (11):** `contract_award_unique_key`, `recipient_uei`, `naics_code`, `pop_current_end`, `pop_potential_end`, `solicitation_identifier`, `scope_words`, `scope_chars`, `total_dollars_obligated`, `base_and_exercised_options_value`, `number_of_offers_received`.
**BITMAP (49):** `business_size`, `type_of_set_aside`, `award_or_idv_flag`, the 4 liveness flags, `has_subcontracting_plan`, `subcontracting_plan_code`, `has_substantive_scope`, `has_directional_scope`, `extent_competed`, `type_of_contract_pricing`, `contract_bundling`, `performance_based_service_acquisition`, `construction_wage_rate_requirements`, `labor_standards`, `commercial_item_acquisition_procedures`, `solicitation_procedures`, `consolidated_contract`, `multi_year_contract`, `undefinitized_action`, `fair_opportunity_limited_sources`, `other_than_full_and_open_competition`, `domestic_or_foreign_entity`, `organizational_type`, and all 23 socioeconomic/demographic flags.

Example zero-join GTM filters (single table, no joins), with measured counts:
```sql
-- large primes on live unrestricted work with substantive SOW text
WHERE active_potential AND has_substantive_scope AND business_size LIKE 'OTHER%'         -- 7,597

-- SDVOSB primes on live work that carry a subcontracting plan
WHERE active_potential AND service_disabled_veteran_owned_business AND has_subcontracting_plan   -- 79

-- HUBZone primes on live work
WHERE active_potential AND historically_underutilized_business_zone_hubzone_firm          -- 4,640
```
Returned rows carry scope text, recipient phone/address, NAICS, and $ inline — the standalone hunting-ground intent.

---

## 9. Boundaries (accurate scope of this table)

- **This table does NOT carry the detailed requirements rollup.** Clearance level, certification tags, labor categories, and the full 11-type requirement breakdown live in the separate `govcon_award_scope_requirements` (LEFT-joinable on `contract_award_unique_key`). `active_awards` carries the scope *narrative* + substance flags + the subcontracting-plan signal inline; deep requirements are one optional join.
- **Socioeconomic flags are the prime's point-in-time FPDS self-certs at award.** Current registry certs (with effective/expiry dates) live in `sam_master_entities`. POC name/title is in `sam_pocs` (`recipient_phone_number` here has no name attached; no email in either).
- **Snapshot-frozen liveness.** Membership decays daily as awards cross PoP end; bounded by `contract_prime_txn` freshness (max action_date 2026-06-07). Rebuild to refresh. No cadence registered.
- **`number_of_offers_received`** carries a `999` sentinel (unknown) and is NULL when the source was empty.

---

## 10. Build provenance & verification

- Built by two orchestrated multi-agent workflows: a spec-verification pass (bridge/lexicon/schema) and a 6-agent adversarial verification pass.
- **Hardening applied from adversarial review:** (a) deterministic collapse tiebreaker (`last_modified_date DESC`) resolving 36 ambiguous duplicate-txn awards; (b) all derived + socioeconomic booleans `COALESCE→FALSE` (no tri-state NULLs — `has_subcontracting_plan` went from 25,152 nulls to 0).
- **Adversarial checks (all pass):** substrate-no-regression (grain clean, liveness recompute matches), scope-flag fidelity (boolean flags bit-exact vs verbatim defs), subcontracting-plan consistency, all 23 socio flags 0-null and feed-exact (swap test confirmed renamed columns), schema completeness (124 cols, correct types, nothing dropped), 60-index integrity, and a zero-join composite returning fully-populated rows.

---

## 11. How to verify

`/tmp/aa_doc_facts.py` (read-only) emits every number in this doc as JSON. Per-dataset read-back: `doppler run --project core-x --config prd -- python pipelines/serving/materialize_active_awards.py --cmd verify`. Rebuild: `--cmd build`.
