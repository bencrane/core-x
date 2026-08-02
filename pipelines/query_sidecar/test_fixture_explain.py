"""EXPLAIN fixture — the offline join-plan gate for the query-sidecar builder.

Drives EVERY manifest spec (plus the agency_vocab rider build() appends) through
``_build_one``'s DISPATCH PATH on 4-row fixture tables and asserts that no CREATE
TABLE plan — and no ``_VIEWS`` query plan — contains a nested-loop or cross-product
operator. The rule this enforces was bought with three lost builds (2026-07-09/10):
a probe-side predicate mixed into a join ON clause planned as
PhysicalBlockwiseNLJoin at 83M x 83M — a zero-CPU "hang" only py-spy --native
exposed. A 4-row fixture executes the same pathological plan instantly.

Offline by construction: ``lance.dataset`` is monkeypatched to serve pyarrow
fixture tables; no network, no R2, no secrets. Runs green locally:

    python3 -m pytest pipelines/query_sidecar/test_fixture_explain.py -q

Wired into /sidecar-build preflight and /sidecar-gaps' builder-edit step — run it
green before every builder merge. When a new mart's SQL references a column the
fixtures lack, extend FIXTURE_SCHEMAS below (the binder error names it).
"""
from __future__ import annotations

import datetime as dt
import json
import re
import sys
from pathlib import Path

import duckdb
import pyarrow as pa
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import build_query_sidecar as b

# ── 4-row fixture schemas: every Lance dataset the manifest reads, with the union
# of columns any consumer (spec cols/sort/extra_select, special-case SQL, _VIEWS)
# references. Types converged by driving the real dispatch path to green.
FIXTURE_SCHEMAS: dict[str, dict[str, str]] = json.loads(r'''
{
 "bea_bls_klems": {
  "production_code": "VARCHAR",
  "sheet": "VARCHAR",
  "year": "BIGINT"
 },
 "bea_contingent_labor_intake": {
  "grain": "VARCHAR",
  "industry_code": "VARCHAR",
  "year": "BIGINT"
 },
 "bea_io_use_summary_annual": {
  "commodity_code": "VARCHAR",
  "industry_code": "VARCHAR",
  "year": "BIGINT"
 },
 "bea_naics_concordance": {
  "naics_code_clean": "VARCHAR"
 },
 "bls_ecec_burden": {
  "industry_group": "VARCHAR",
  "occupation_group": "VARCHAR",
  "ownership": "VARCHAR",
  "quarter": "VARCHAR",
  "subcell": "VARCHAR"
 },
 "bls_ecec_costs": {
  "area": "VARCHAR",
  "datatype": "VARCHAR",
  "estimate_code": "VARCHAR",
  "industry_group": "VARCHAR",
  "occupation_group": "VARCHAR",
  "ownership": "VARCHAR",
  "period": "VARCHAR",
  "subcell": "VARCHAR",
  "year": "BIGINT"
 },
 "bridge_dsbs_pdl_linkedin": {
  "uei": "VARCHAR"
 },
 "bridge_sam_pdl": {
  "country": "BIGINT",
  "duns": "VARCHAR",
  "employee_size_range": "VARCHAR",
  "industry": "VARCHAR",
  "locality": "VARCHAR",
  "normalized_domain": "VARCHAR",
  "pdl_company_id": "VARCHAR",
  "region": "VARCHAR",
  "uei": "VARCHAR",
  "year_founded": "BIGINT"
 },
 "census_county_adjacency": {
  "county_fips": "BIGINT",
  "neighbor_fips": "VARCHAR"
 },
 "census_county_cbsa_2023": {
  "county_fips": "BIGINT"
 },
 "census_county_gazetteer_2023": {
  "county_fips": "BIGINT"
 },
 "cms_provider_enrollment": {
  "enrlmt_id": "VARCHAR",
  "first_name": "VARCHAR",
  "last_name": "VARCHAR",
  "multiple_npi_flag": "VARCHAR",
  "npi": "VARCHAR",
  "org_name": "VARCHAR",
  "pecos_asct_cntl_id": "VARCHAR",
  "provider_type_desc": "VARCHAR",
  "state_cd": "VARCHAR"
 },
 "cms_provider_enrollment_practice": {
  "city_name": "VARCHAR",
  "enrlmt_id": "VARCHAR",
  "state_cd": "VARCHAR",
  "zip_cd": "VARCHAR"
 },
 "cms_provider_enrollment_reassignment": {
  "rcv_bnft_enrlmt_id": "VARCHAR",
  "reasgn_bnft_enrlmt_id": "VARCHAR"
 },
 "nppes_provider/snapshot=2026-07": {
  "certification_date": "DATE",
  "entity_type_code": "VARCHAR",
  "is_active": "BOOLEAN",
  "npi": "VARCHAR",
  "practice_address_line2": "VARCHAR",
  "practice_phone": "VARCHAR",
  "practice_state": "VARCHAR"
 },
 "nppes_provider_taxonomy/snapshot=2026-07": {
  "is_primary": "BOOLEAN",
  "license_number": "VARCHAR",
  "license_state": "VARCHAR",
  "npi": "VARCHAR",
  "taxonomy_code": "VARCHAR",
  "taxonomy_group": "VARCHAR",
  "taxonomy_rank": "BIGINT"
 },
 "nppes_taxonomy_ref": {
  "classification": "VARCHAR",
  "display_name": "VARCHAR",
  "grouping": "VARCHAR",
  "section": "VARCHAR",
  "specialization": "VARCHAR",
  "taxonomy_code": "VARCHAR"
 },
 "practice_group_360/snapshot=2026-07": {
  "active_member_count": "BIGINT",
  "avg_dual_share": "DOUBLE",
  "avg_mips_score": "DOUBLE",
  "avg_panel_risk_score": "DOUBLE",
  "avg_pymt_cagr_3yr_pct": "DOUBLE",
  "avg_pymt_yoy_pct": "DOUBLE",
  "distinct_specialties": "BIGINT",
  "group_enrlmt_id": "VARCHAR",
  "group_state": "VARCHAR",
  "independent_member_count": "BIGINT",
  "member_count": "BIGINT",
  "member_npis": "LIST_VARCHAR",
  "n_states": "BIGINT",
  "org_name": "VARCHAR",
  "top_specialty": "VARCHAR",
  "total_medicare_paid_usd": "DOUBLE",
  "total_op_payments_usd": "DOUBLE",
  "total_rx_cost_usd": "DOUBLE"
 },
 "provider_360/snapshot=2026-07": {
  "authorized_official_first_name": "VARCHAR",
  "authorized_official_last_name": "VARCHAR",
  "authorized_official_phone": "VARCHAR",
  "authorized_official_title": "VARCHAR",
  "credential": "VARCHAR",
  "deactivation_date": "DATE",
  "dme_supplied_total_paid_usd": "DOUBLE",
  "entity_type": "VARCHAR",
  "entity_type_code": "VARCHAR",
  "enumeration_date": "DATE",
  "enumeration_year": "BIGINT",
  "first_name": "VARCHAR",
  "has_a1": "BOOLEAN",
  "has_ffs_enrollment": "BOOLEAN",
  "has_mips": "BOOLEAN",
  "has_op": "BOOLEAN",
  "has_parent_org_tin": "BOOLEAN",
  "has_rx": "BOOLEAN",
  "is_active": "BOOLEAN",
  "is_dme_supplier": "BOOLEAN",
  "is_independent_candidate": "BOOLEAN",
  "is_organization_subpart": "VARCHAR",
  "is_sole_proprietor": "VARCHAR",
  "largest_practice_group_enrlmt_id": "VARCHAR",
  "largest_practice_group_size": "BIGINT",
  "largest_practice_org_name": "VARCHAR",
  "last_name": "VARCHAR",
  "last_update_date": "DATE",
  "mailing_address_line1": "VARCHAR",
  "mailing_address_line2": "VARCHAR",
  "mailing_city": "VARCHAR",
  "mailing_state": "VARCHAR",
  "mailing_zip5": "VARCHAR",
  "med_a1_active_latest": "BOOLEAN",
  "med_a1_dual_share": "DOUBLE",
  "med_a1_latest_benes": "BIGINT",
  "med_a1_latest_mdcr_pymt": "DOUBLE",
  "med_a1_latest_year": "BIGINT",
  "med_a1_lifetime_mdcr_pymt": "DOUBLE",
  "med_a1_panel_avg_risk_score": "DOUBLE",
  "med_a1_provider_type": "VARCHAR",
  "med_a1_pymt_cagr_3yr_pct": "DOUBLE",
  "med_a1_pymt_growth_2019_latest_pct": "DOUBLE",
  "med_a1_pymt_yoy_pct": "DOUBLE",
  "mips_clinician_specialty": "VARCHAR",
  "mips_final_score": "DOUBLE",
  "name_prefix": "VARCHAR",
  "name_suffix": "VARCHAR",
  "npi": "VARCHAR",
  "op_distinct_manufacturers": "BIGINT",
  "op_has_ownership_interest": "BOOLEAN",
  "op_total_payments_usd": "DOUBLE",
  "organization_name": "VARCHAR",
  "parent_organization_lbn": "VARCHAR",
  "pecos_asct_cntl_id": "VARCHAR",
  "practice_address_line1": "VARCHAR",
  "practice_city": "VARCHAR",
  "practice_group_count": "BIGINT",
  "practice_state": "VARCHAR",
  "practice_zip5": "VARCHAR",
  "primary_license_state": "VARCHAR",
  "primary_taxonomy_code": "VARCHAR",
  "provider_name": "VARCHAR",
  "reactivation_date": "DATE",
  "replacement_npi": "VARCHAR",
  "rx_top1_generic": "VARCHAR",
  "rx_total_drug_cost_usd": "DOUBLE",
  "sex_code": "VARCHAR",
  "smallest_practice_group_enrlmt_id": "VARCHAR",
  "smallest_practice_group_size": "BIGINT",
  "smallest_practice_org_name": "VARCHAR",
  "taxonomy_slot_count": "BIGINT"
 },
 "demo_region_catalog": {
  "county_fips": "VARCHAR",
  "demo_region": "VARCHAR",
  "state_usps": "VARCHAR"
 },
 "dol_sca_occupations": {
  "occupation_code": "VARCHAR"
 },
 "dsbs_poc_linkedin": {
  "uei": "VARCHAR"
 },
 "entity_hierarchy": {
  "uei": "VARCHAR"
 },
 "equipment_flowdown_factors": {
  "production_code": "VARCHAR"
 },
 "equipment_finance_candidates": {
  "company_domain": "VARCHAR",
  "company_name": "VARCHAR",
  "domain_norm": "VARCHAR",
  "landed_at": "TIMESTAMP",
  "linkedin_url": "VARCHAR",
  "linkedin_url_norm": "VARCHAR",
  "record_id": "VARCHAR",
  "source": "VARCHAR",
  "verdict": "VARCHAR"
 },
 "equipment_matchmaking": {
  "domain_norm": "VARCHAR",
  "matched_psc_count": "BIGINT",
  "materialized_at": "TIMESTAMP",
  "supported_pscs": "VARCHAR",
  "verified_inventory_matches": "VARCHAR"
 },
 "equipment_provider": {
  "company_domain": "VARCHAR",
  "confidence": "VARCHAR",
  "domain_norm": "VARCHAR",
  "evidence_snippet": "VARCHAR",
  "evidence_url": "VARCHAR",
  "is_equipment_provider": "BOOLEAN",
  "landed_at": "TIMESTAMP",
  "materialized_at": "TIMESTAMP",
  "mode": "VARCHAR",
  "reasoning": "VARCHAR",
  "record_id": "VARCHAR",
  "source": "VARCHAR",
  "steps_taken": "VARCHAR"
 },
 "equipment_rental_golden_overlap": {
  "all_nearby_award_count": "BIGINT",
  "capability_capture_ratio": "DOUBLE",
  "firm_domain": "VARCHAR",
  "qualified_nearby_award_count": "BIGINT",
  "qualified_psc_count": "BIGINT",
  "qualified_pscs": "VARCHAR",
  "qualified_value_exposure": "DOUBLE"
 },
 "exa_person_linkedin_candidates": {
  "aff_used": "VARCHAR",
  "company_domain": "VARCHAR",
  "company_name": "VARCHAR",
  "cost_usd": "DOUBLE",
  "exa_linkedin_url": "VARCHAR",
  "exa_title": "VARCHAR",
  "first_name": "VARCHAR",
  "is_in_profile": "BOOLEAN",
  "last_name": "VARCHAR",
  "n_results": "BIGINT",
  "query": "VARCHAR",
  "query_variant": "VARCHAR",
  "sam_person_id": "VARCHAR",
  "searched_at": "TIMESTAMP",
  "source_batch": "VARCHAR",
  "uei": "VARCHAR"
 },
 "fdic_institutions": {
  "active": "BOOLEAN",
  "asset": "VARCHAR",
  "cert": "VARCHAR",
  "charter": "VARCHAR",
  "city": "VARCHAR",
  "name": "VARCHAR",
  "stalp": "VARCHAR",
  "stname": "VARCHAR",
  "webaddr": "VARCHAR",
  "zip": "VARCHAR"
 },
 "federal_sites_lance": {
  "state_code": "VARCHAR",
  "zip5": "VARCHAR"
 },
 "firmographics_blitz": {
  "domain_norm": "VARCHAR",
  "employees_on_linkedin": "VARCHAR",
  "normalized_domain": "VARCHAR"
 },
 "govcon_subawardee_profiles": {
  "sub_uei": "VARCHAR"
 },
 "gtm_audience_entities": {
  "prime_obl_12mo": "DOUBLE",
  "prime_obl_24mo": "DOUBLE",
  "prime_obl_60mo": "DOUBLE",
  "prime_obl_lifetime": "DOUBLE",
  "sub_amt_12mo": "DOUBLE",
  "sub_amt_24mo": "DOUBLE",
  "sub_amt_60mo": "DOUBLE",
  "sub_amt_lifetime": "DOUBLE",
  "uei": "VARCHAR"
 },
 "gtm_award_expiry_months": {
  "end_month": "DATE",
  "uei": "VARCHAR"
 },
 "gtm_award_recipient_rollup": {
  "uei": "VARCHAR"
 },
 "gtm_entity_behavior_rollup": {
  "uei": "VARCHAR"
 },
 "gtm_entity_code_lanes": {
  "action_ct": "BIGINT",
  "code": "VARCHAR",
  "code_type": "VARCHAR",
  "context_code": "VARCHAR",
  "context_code_type": "VARCHAR",
  "last_action_date": "DATE",
  "obl_12mo": "DOUBLE",
  "obl_24mo": "DOUBLE",
  "obl_60mo": "DOUBLE",
  "obl_lifetime": "DOUBLE",
  "recipient_code_source": "VARCHAR",
  "recipient_code_type": "VARCHAR",
  "side": "VARCHAR",
  "subaward_amt_total": "DOUBLE",
  "uei": "VARCHAR"
 },
 "gtm_entity_geo": {
  "uei": "VARCHAR"
 },
 "gtm_entity_inferred_primeable_codes": {
  "code": "VARCHAR",
  "code_type": "VARCHAR"
 },
 "gtm_entity_inferred_subbable_codes": {
  "code": "VARCHAR",
  "code_type": "VARCHAR"
 },
 "gtm_fpds_entity_signal_events": {
  "uei": "VARCHAR"
 },
 "gtm_naics_psc_pairs": {
  "naics_code": "VARCHAR",
  "psc_code": "VARCHAR"
 },
 "gtm_open_awards": {
  "recipient_uei": "VARCHAR"
 },
 "gtm_prime_combo_lanes": {
  "employees_on_linkedin": "VARCHAR",
  "farmout_amt_24mo": "DOUBLE",
  "farmout_amt_60mo": "DOUBLE",
  "farmout_amt_lifetime": "DOUBLE",
  "n_txns_lifetime": "BIGINT",
  "naics_code": "VARCHAR",
  "normalized_domain": "VARCHAR",
  "prime_obl_24mo": "DOUBLE",
  "prime_obl_60mo": "DOUBLE",
  "prime_obl_lifetime": "DOUBLE",
  "psc_code": "VARCHAR",
  "uei": "VARCHAR"
 },
 "gtm_prime_demand_events": {
  "uei": "VARCHAR"
 },
 "gtm_prime_farmout_combo_lanes": {
  "employees_on_linkedin": "VARCHAR",
  "farmout_amt_24mo": "DOUBLE",
  "farmout_amt_60mo": "DOUBLE",
  "farmout_amt_lifetime": "DOUBLE",
  "naics_code": "VARCHAR",
  "normalized_domain": "VARCHAR",
  "psc_code": "VARCHAR",
  "uei": "VARCHAR"
 },
 "gtm_prime_pop_lanes": {
  "uei": "VARCHAR"
 },
 "gtm_prime_sub_pairs": {
  "prime_uei": "VARCHAR",
  "sub_uei": "VARCHAR"
 },
 "gtm_prime_subout_by_recipient_code": {
  "context_code": "VARCHAR",
  "context_code_type": "VARCHAR",
  "prime_awardee_uei": "VARCHAR",
  "recipient_code": "VARCHAR",
  "recipient_code_source": "VARCHAR",
  "recipient_code_type": "VARCHAR",
  "subaward_amt_total": "DOUBLE"
 },
 "gtm_prime_vehicle_lanes": {
  "uei": "VARCHAR"
 },
 "gtm_primes_by_recipient_code": {
  "recipient_code": "VARCHAR"
 },
 "gtm_sam_entities": {
  "physical_state": "VARCHAR",
  "uei": "VARCHAR"
 },
 "gtm_sam_people": {
  "best_title": "VARCHAR",
  "display_name": "VARCHAR",
  "email": "VARCHAR",
  "email_verification_status": "VARCHAR",
  "first_name": "VARCHAR",
  "is_ebiz_poc": "BOOLEAN",
  "is_govt_poc": "BOOLEAN",
  "last_name": "VARCHAR",
  "linkedin_match_score": "DOUBLE",
  "n_sources": "BIGINT",
  "person_linkedin_url_norm": "VARCHAR",
  "phone": "VARCHAR",
  "phone_status": "VARCHAR",
  "phone_type": "VARCHAR",
  "sam_person_id": "VARCHAR",
  "uei": "VARCHAR"
 },
 "gtm_sam_person_contactability": {
  "email": "VARCHAR",
  "email_verification_status": "VARCHAR",
  "linkedin_match_score": "DOUBLE",
  "person_linkedin_url_norm": "VARCHAR",
  "phone": "VARCHAR",
  "phone_status": "VARCHAR",
  "phone_type": "VARCHAR",
  "sam_person_id": "VARCHAR"
 },
 "gtm_sub_combo_lanes": {
  "uei": "VARCHAR"
 },
 "gtm_sub_profiles": {
  "uei": "VARCHAR"
 },
 "gtm_sub_universe_pairs": {
  "target_uei": "VARCHAR"
 },
 "gtm_sub_universe_targets": {
  "uei": "VARCHAR"
 },
 "gtm_subbed_under_to_primed_in_cooccurrence": {
  "subbed_under_code": "VARCHAR"
 },
 "gtm_txn_events_slim": {
  "action_date": "DATE",
  "action_type_code": "VARCHAR",
  "award_key": "VARCHAR",
  "awarding_agency_code": "VARCHAR",
  "naics_code": "VARCHAR",
  "obligation": "DOUBLE",
  "psc_code": "VARCHAR",
  "uei": "VARCHAR"
 },
 "gtm_txn_recipient_month_rollup": {
  "action_type_code": "VARCHAR",
  "month": "DATE",
  "uei": "VARCHAR"
 },
 "icypeas_company_scrapes": {
  "batch_label": "VARCHAR",
  "company_linkedin_url": "VARCHAR",
  "country": "BIGINT",
  "domain": "VARCHAR",
  "domain_norm": "VARCHAR",
  "employee_count": "BIGINT",
  "headcount_range": "BIGINT",
  "in_dsbs": "BOOLEAN",
  "industry": "VARCHAR",
  "li_source": "VARCHAR",
  "linkedin_url": "VARCHAR",
  "money24_usd": "DOUBLE",
  "name": "VARCHAR",
  "scraped_at": "TIMESTAMP",
  "source_class": "VARCHAR",
  "status": "VARCHAR",
  "uei": "VARCHAR",
  "website": "VARCHAR"
 },
 "icypeas_dsbs_company_profiles": {
  "uei": "VARCHAR"
 },
 "icypeas_person_profile_scrapes": {
  "person_linkedin_url_norm": "VARCHAR"
 },
 "icypeas_person_profiles": {
  "drained_at": "TIMESTAMP",
  "file_id": "VARCHAR",
  "file_name": "VARCHAR",
  "found": "VARCHAR",
  "icypeas_item_id": "VARCHAR",
  "input_url": "VARCHAR",
  "order_idx": "VARCHAR",
  "person_linkedin_url_norm": "VARCHAR",
  "sam_person_id": "VARCHAR",
  "status": "VARCHAR",
  "uei": "VARCHAR"
 },
 "military_installations_lance": {
  "state_code": "VARCHAR"
 },
 "naics_labor_share": {
  "burden_match_level": "VARCHAR",
  "burden_multiplier": "VARCHAR",
  "loaded_labor_share": "DOUBLE",
  "naics_code": "VARCHAR",
  "normalized_domain": "VARCHAR",
  "payroll_share": "DOUBLE",
  "payroll_share_level": "DOUBLE"
 },
 "naics_psc_deliverable": {
  "naics_code": "VARCHAR",
  "psc_code": "VARCHAR"
 },
 "naics_psc_equipment_needs": {
  "confidence": "VARCHAR",
  "core_phrase_count": "BIGINT",
  "equipment_buckets": "VARCHAR",
  "in_scope": "BOOLEAN",
  "naics_code": "VARCHAR",
  "other_phrase_count": "BIGINT",
  "primary_bucket": "VARCHAR",
  "proposed_equipment_needs": "VARCHAR",
  "psc_code": "VARCHAR"
 },
 "naics_psc_labor_profile": {
  "alias": "VARCHAR",
  "burden_match_level": "VARCHAR",
  "burden_multiplier": "VARCHAR",
  "code_type": "VARCHAR",
  "employees_on_linkedin": "VARCHAR",
  "in_combo_layer": "BOOLEAN",
  "loaded_labor_share": "DOUBLE",
  "naics_code": "VARCHAR",
  "normalized_domain": "VARCHAR",
  "occupation_title": "VARCHAR",
  "payroll_share": "DOUBLE",
  "payroll_share_level": "DOUBLE",
  "psc_code": "VARCHAR",
  "title_source": "VARCHAR"
 },
 "naics_psc_labor_profile_categories": {
  "a_median": "VARCHAR",
  "alias": "VARCHAR",
  "burden_match_level": "VARCHAR",
  "burden_multiplier": "VARCHAR",
  "code_type": "VARCHAR",
  "employees_on_linkedin": "VARCHAR",
  "ep_growth_2024_2034_pct": "VARCHAR",
  "in_combo_layer": "BOOLEAN",
  "loaded_labor_share": "DOUBLE",
  "naics_code": "VARCHAR",
  "normalized_domain": "VARCHAR",
  "occupation_title": "VARCHAR",
  "payroll_share": "DOUBLE",
  "payroll_share_level": "DOUBLE",
  "pct_of_industry": "DOUBLE",
  "psc_code": "VARCHAR",
  "rank": "BIGINT",
  "role_class": "VARCHAR",
  "sca_code": "VARCHAR",
  "sca_title": "VARCHAR",
  "soc_code": "VARCHAR",
  "soc_title": "VARCHAR",
  "title_source": "VARCHAR"
 },
 "naics_psc_vertical_map": {
  "naics_code": "VARCHAR",
  "psc_code": "VARCHAR"
 },
 "naics_reference": {
  "naics_code": "VARCHAR",
  "naics_title": "VARCHAR",
  "source_vintage": "VARCHAR"
 },
 "national_county2020": {
  "county_fips": "BIGINT"
 },
 "ncua_credit_unions": {
  "charter_number": "VARCHAR",
  "city_mailing_address": "VARCHAR",
  "credit_union_name": "VARCHAR",
  "credit_union_type": "VARCHAR",
  "members": "BIGINT",
  "state_mailing_address": "VARCHAR",
  "total_assets": "DOUBLE",
  "zip_code_mailing_address": "VARCHAR"
 },
 "occupation_alias_lookup": {
  "alias": "VARCHAR",
  "alias_norm": "VARCHAR",
  "burden_match_level": "VARCHAR",
  "burden_multiplier": "VARCHAR",
  "code": "VARCHAR",
  "code_type": "VARCHAR",
  "in_combo_layer": "BOOLEAN",
  "loaded_labor_share": "DOUBLE",
  "occupation_title": "VARCHAR",
  "payroll_share": "DOUBLE",
  "payroll_share_level": "DOUBLE",
  "title_source": "VARCHAR"
 },
 "olms_cba_crosswalk": {
  "uei": "VARCHAR"
 },
 "pdl_normalized_companies": {
  "company_name": "VARCHAR",
  "country": "BIGINT",
  "employee_size_range": "VARCHAR",
  "employees_on_linkedin": "VARCHAR",
  "industry": "VARCHAR",
  "is_generic_domain": "BOOLEAN",
  "linkedin_slug": "VARCHAR",
  "locality": "VARCHAR",
  "normalized_domain": "VARCHAR",
  "pdl_company_id": "VARCHAR",
  "region": "VARCHAR",
  "year_founded": "BIGINT"
 },
 "people_canonical": {
  "canonical_person_id": "VARCHAR"
 },
 "psc_reference": {
  "is_active": "BOOLEAN",
  "psc_code": "VARCHAR",
  "psc_name": "VARCHAR",
  "source_vintage": "VARCHAR"
 },
 "sam_county_fips_crosswalk": {
  "classification_title": "VARCHAR",
  "county_fips": "BIGINT",
  "county_name": "BIGINT",
  "fringe": "VARCHAR",
  "fringe_is_pct": "VARCHAR",
  "hw_rate": "DOUBLE",
  "resolution_status": "VARCHAR",
  "revision_number": "VARCHAR",
  "sam_county_name": "BIGINT",
  "state_code": "VARCHAR",
  "state_name": "VARCHAR",
  "wage_rate": "DOUBLE",
  "wd_type": "VARCHAR"
 },
 "sam_master_entities": {
  "is_active": "BOOLEAN",
  "naics_codes": "LIST_VARCHAR",
  "psc_codes": "LIST_VARCHAR",
  "uei": "VARCHAR"
 },
 "sam_master_profile_deltas": {
  "to_date": "DATE",
  "uei": "VARCHAR"
 },
 "sam_pocs": {
  "uei": "VARCHAR"
 },
 "sam_ucc_debtor_overlap": {
  "uei": "VARCHAR"
 },
 "sam_ucc_filings": {
  "first_filing_date": "DATE",
  "uei": "VARCHAR"
 },
 "sam_ucc_lenders": {
  "lender_key": "VARCHAR"
 },
 "sam_wd_county_coverage": {
  "county_name": "BIGINT",
  "state_code": "VARCHAR",
  "state_name": "VARCHAR",
  "wd_id": "VARCHAR"
 },
 "sam_wd_rates_structured": {
  "classification_title": "VARCHAR",
  "county_name": "BIGINT",
  "fringe": "VARCHAR",
  "fringe_is_pct": "VARCHAR",
  "hw_rate": "DOUBLE",
  "occupation_code": "VARCHAR",
  "revision_number": "VARCHAR",
  "state_code": "VARCHAR",
  "state_name": "VARCHAR",
  "wage_rate": "DOUBLE",
  "wd_id": "VARCHAR",
  "wd_type": "VARCHAR"
 },
 "sca_soc_crosswalk": {
  "occupation_code": "VARCHAR"
 },
 "soc_state_wage": {
  "prim_state": "VARCHAR",
  "soc_code": "VARCHAR"
 },
 "state_region_county_map": {
  "county_fips": "VARCHAR",
  "state_region": "VARCHAR"
 },
 "ucc_filings_all": {
  "debtor_city": "VARCHAR",
  "debtor_key": "VARCHAR",
  "debtor_name": "VARCHAR",
  "debtor_name_norm": "VARCHAR",
  "debtor_state": "VARCHAR",
  "debtor_zip": "VARCHAR",
  "filing_class": "VARCHAR",
  "filing_id": "VARCHAR",
  "first_filing_date": "DATE",
  "in_sam": "BOOLEAN",
  "is_active_financing": "BOOLEAN",
  "is_lease": "BOOLEAN",
  "is_org": "BOOLEAN",
  "lapse_date": "DATE",
  "last_filing_date": "DATE",
  "n_secured_parties": "BIGINT",
  "secured_parties": "VARCHAR",
  "sos_entity_key": "VARCHAR",
  "terminated": "BOOLEAN",
  "ucc_state": "VARCHAR",
  "uei": "VARCHAR"
 },
 "ucc_lenders_all": {
  "lender_key": "VARCHAR"
 },
 "us_software_companies": {
  "domain": "VARCHAR"
 },
 "usaspending_award_canonical": {
  "award_id_piid": "VARCHAR",
  "awarding_agency_code": "VARCHAR",
  "awarding_agency_name": "VARCHAR",
  "contract_award_unique_key": "VARCHAR",
  "description": "VARCHAR",
  "generated_unique_award_id": "VARCHAR",
  "recipient_uei": "VARCHAR",
  "solicitation_date": "DATE",
  "solicitation_identifier": "VARCHAR"
 },
 "usaspending_award_pop_centroids": {
  "generated_unique_award_id": "VARCHAR",
  "state_code": "VARCHAR",
  "zip5": "VARCHAR"
 },
 "usaspending_fpds_canonical_txn": {
  "action_date": "DATE",
  "action_type_code": "VARCHAR",
  "action_type_description": "VARCHAR",
  "award_id_piid": "VARCHAR",
  "award_topology": "VARCHAR",
  "award_type_code": "VARCHAR",
  "awarding_agency_code": "VARCHAR",
  "awarding_agency_name": "VARCHAR",
  "awarding_sub_agency_code": "VARCHAR",
  "awarding_sub_agency_name": "VARCHAR",
  "base_and_all_options_value": "DOUBLE",
  "contract_award_unique_key": "VARCHAR",
  "contract_financing": "VARCHAR",
  "contract_financing_descrip": "VARCHAR",
  "contract_transaction_unique_key": "VARCHAR",
  "contracting_officers_desc": "VARCHAR",
  "contracting_officers_determination_of_business_size": "VARCHAR",
  "federal_action_obligation": "DOUBLE",
  "funding_agency_code": "VARCHAR",
  "funding_sub_agency_code": "VARCHAR",
  "labor_standards_code": "VARCHAR",
  "labor_standards_descrip": "VARCHAR",
  "life_to_date_obligated": "DOUBLE",
  "naics_code": "VARCHAR",
  "ordering_period_end_date": "DATE",
  "parent_award_key_resolved": "VARCHAR",
  "parent_match_flag": "VARCHAR",
  "performance_based_se_desc": "VARCHAR",
  "performance_based_service_acquisition_code": "VARCHAR",
  "physical_state": "VARCHAR",
  "pop_city_name": "VARCHAR",
  "pop_congressional_code": "VARCHAR",
  "pop_country_name": "VARCHAR",
  "pop_county_fips": "BIGINT",
  "pop_county_name": "BIGINT",
  "primary_place_of_performance_country_code": "VARCHAR",
  "primary_place_of_performance_state_code": "VARCHAR",
  "primary_place_of_performance_zip_4": "VARCHAR",
  "product_or_service_code": "VARCHAR",
  "psc_code": "VARCHAR",
  "recipient_name": "VARCHAR",
  "recipient_state_code": "VARCHAR",
  "recipient_uei": "VARCHAR",
  "subcontracting_plan": "VARCHAR",
  "subcontracting_plan_desc": "VARCHAR",
  "type_of_contract_pric_desc": "VARCHAR",
  "type_of_contract_pricing_code": "VARCHAR",
  "type_of_set_aside_code": "VARCHAR"
 },
 "usaspending_fpds_prime_award_state": {
  "award_topology": "VARCHAR",
  "award_type_code": "VARCHAR",
  "awarding_agency_code": "VARCHAR",
  "awarding_sub_agency_code": "VARCHAR",
  "contract_award_unique_key": "VARCHAR",
  "current_end_date": "DATE",
  "current_total_value_of_award": "DOUBLE",
  "days_to_expiry": "BIGINT",
  "first_action_date": "DATE",
  "idv_type_code": "VARCHAR",
  "is_expired_no_followon": "BOOLEAN",
  "is_terminated": "BOOLEAN",
  "last_action_date": "DATE",
  "life_to_date_obligated": "DOUBLE",
  "naics_code": "VARCHAR",
  "parent_award_key_resolved": "VARCHAR",
  "parent_match_flag": "VARCHAR",
  "physical_state": "VARCHAR",
  "potential_ceiling": "BIGINT",
  "potential_end_date": "DATE",
  "product_or_service_code": "VARCHAR",
  "recipient_uei": "VARCHAR",
  "remaining_ceiling_headroom": "DOUBLE",
  "total_dollars_obligated_snapshot": "DOUBLE",
  "type_of_set_aside_code": "VARCHAR"
 },
 "usaspending_subaward_canonical": {
  "_": "VARCHAR",
  "prime_award_awarding_agency_code": "VARCHAR",
  "prime_award_awarding_agency_name": "VARCHAR",
  "prime_award_awarding_sub_agency_code": "VARCHAR",
  "prime_award_awarding_sub_agency_name": "VARCHAR",
  "prime_award_base_transaction_description": "VARCHAR",
  "prime_award_naics_code": "VARCHAR",
  "prime_award_parent_piid": "VARCHAR",
  "prime_award_piid": "VARCHAR",
  "prime_award_primary_place_of_performance_country_code": "BIGINT",
  "prime_award_primary_place_of_performance_state_code": "VARCHAR",
  "prime_award_product_or_service_code": "VARCHAR",
  "prime_award_unique_key": "VARCHAR",
  "prime_awardee_country_code": "BIGINT",
  "prime_awardee_name": "VARCHAR",
  "prime_awardee_parent_uei": "VARCHAR",
  "prime_awardee_state_code": "VARCHAR",
  "prime_awardee_uei": "VARCHAR",
  "sub_place_of_perform_county_code": "BIGINT",
  "sub_place_of_perform_county_name": "BIGINT",
  "subaward_action_date": "DATE",
  "subaward_action_date_fiscal_year": "DATE",
  "subaward_amount": "DOUBLE",
  "subaward_amount_num": "BIGINT",
  "subaward_description": "VARCHAR",
  "subaward_last_modified_date": "DATE",
  "subaward_number": "VARCHAR",
  "subaward_primary_place_of_performance_address_zip_code": "VARCHAR",
  "subaward_primary_place_of_performance_country_code": "BIGINT",
  "subaward_primary_place_of_performance_state_code": "VARCHAR",
  "subaward_unique_key": "VARCHAR",
  "subawardee_business_types": "VARCHAR",
  "subawardee_country_code": "BIGINT",
  "subawardee_name": "VARCHAR",
  "subawardee_parent_uei": "VARCHAR",
  "subawardee_state_code": "VARCHAR",
  "subawardee_uei": "VARCHAR",
  "subawardee_zip_code": "VARCHAR",
  "usaspending_permalink": "VARCHAR"
 },
 "va_disability_comp_county": {
  "fips": "VARCHAR",
  "fiscal_year": "BIGINT"
 },
 "va_vetpop_county_total": {
  "fips": "VARCHAR",
  "snapshot_year": "BIGINT"
 }
}
''')

# Columns whose VALUES are load-bearing for branch coverage (probe-side gates,
# UNNEST explode, parent-resolution CASE keys, watermark non-nulls).
SPECIAL_VALUES = {
    "parent_match_flag": ["resolved", "resolved", "self", "unresolved"],
    "side": ["prime", "prime", "sub", "prime"],
    "secured_parties": ["ALPHA BANK; BETA CREDIT", "ALPHA BANK",
                        "GAMMA FINANCE; ALPHA BANK", "DELTA LEASING"],
    "code_type": ["naics", "naics", "psc", "psc"],
    "recipient_code_source": ["fpds", "fpds", "sam", "sam"],
    "action_type_code": ["A", "B", "C", "D"],
}

PA_TYPES = {"VARCHAR": pa.string(), "DATE": pa.date32(), "TIMESTAMP": pa.timestamp("us"),
            "BOOLEAN": pa.bool_(), "BIGINT": pa.int64(), "DOUBLE": pa.float64(),
            "LIST_VARCHAR": pa.list_(pa.string())}

BAD_PLAN_NODES = ("NESTED_LOOP", "CROSS_PRODUCT", "NL_JOIN")


def _value(col: str, typ: str, i: int):
    """Deterministic per (column name, row index): the same column name yields the
    same values in every table, so equality joins overlap naturally."""
    if col in SPECIAL_VALUES:
        return SPECIAL_VALUES[col][i]
    if typ == "LIST_VARCHAR":
        return [f"{col[:12].upper()}_{i}", f"{col[:12].upper()}_{(i + 1) % 4}"]
    if typ == "DATE":
        return dt.date(2026, (i % 4) + 1, 15)
    if typ == "TIMESTAMP":
        return dt.datetime(2026, (i % 4) + 1, 15, 12, 0, 0)
    if typ == "BOOLEAN":
        return i % 2 == 0
    if typ == "BIGINT":
        return i + 1
    if typ == "DOUBLE":
        return float((i + 1) * 100)
    return f"{col[:24].upper()}_{i}"


def _make_table(schema: dict[str, str]) -> pa.Table:
    return pa.table({col: pa.array([_value(col, typ, i) for i in range(4)],
                                   type=PA_TYPES[typ])
                     for col, typ in schema.items()})


class FakeScanner:
    def __init__(self, table: pa.Table):
        self._t = table

    def to_reader(self):
        return self._t.to_reader()


class FakeDataset:
    """Duck-typed stand-in for lance.dataset(...) — version, count_rows, scanner."""

    def __init__(self, table: pa.Table):
        self._t = table
        self.version = 1

    def count_rows(self) -> int:
        return self._t.num_rows

    def scanner(self, columns=None, batch_size=None) -> FakeScanner:
        return FakeScanner(self._t.select(columns) if columns else self._t)


class ExplainingConnection:
    """Proxy that EXPLAINs every CREATE [TEMP] TABLE ... AS before executing it —
    this is what turns the dispatch drive into a plan gate."""

    CTAS_RE = re.compile(r"^\s*CREATE\s+(TEMP\s+)?TABLE\s", re.IGNORECASE)

    def __init__(self, con: duckdb.DuckDBPyConnection):
        self._con = con
        self.plans: list[tuple[str, str]] = []

    def execute(self, sql, *args):
        if self.CTAS_RE.match(sql):
            plan = "\n".join(str(r[1]) for r in self._con.execute("EXPLAIN " + sql).fetchall())
            m = re.search(r'TABLE\s+"?([A-Za-z_0-9]+)"?', sql)
            self.plans.append((m.group(1) if m else "?", plan))
        return self._con.execute(sql, *args)

    def __getattr__(self, name):
        return getattr(self._con, name)


def _all_specs() -> list[dict]:
    # Mirror build(): MANIFEST plus the Tier-D agency_vocab rider.
    return list(b.MANIFEST) + [{"ds": "usaspending_award_canonical", "tier": "D",
                                "cols": ["awarding_agency_code", "awarding_agency_name"],
                                "dest": "agency_vocab", "agency_vocab": True, "sort": []}]


@pytest.fixture()
def driven(monkeypatch):
    """Drive every spec through _build_one on fixtures; yield (plans, parity, con)."""
    import lance

    def _fake_dataset(uri, storage_options=None):
        # Resolve snapshot-partitioned paths (e.g. nppes_provider/snapshot=2026-07)
        # by the full LANCE_BASE-relative key first, then fall back to the last
        # path segment (the historical flat-name behavior).
        rel = uri[len(b.LANCE_BASE):].rstrip("/") if uri.startswith(b.LANCE_BASE) \
            else uri.rstrip("/")
        schema = FIXTURE_SCHEMAS.get(rel) or FIXTURE_SCHEMAS[rel.split("/")[-1]]
        return FakeDataset(_make_table(schema))

    monkeypatch.setattr(lance, "dataset", _fake_dataset)

    raw = duckdb.connect()
    con = ExplainingConnection(raw)
    parity = [b._build_one(con, {}, spec, None) for spec in _all_specs()]
    for vname, vsql in b._VIEWS.items():
        con.execute(vsql)
        plan = "\n".join(str(r[1])
                          for r in raw.execute(f"EXPLAIN SELECT * FROM {vname}").fetchall())
        con.plans.append((f"view:{vname}", plan))
    return con.plans, parity


def test_preflight_green():
    b._preflight({"A", "B", "C", "D"})


def test_no_pathological_joins(driven):
    plans, _ = driven
    offenders = [(t, p) for t, p in plans
                 if any(node in p.upper() for node in BAD_PLAN_NODES)]
    assert not offenders, "pathological join plan(s):\n" + "\n\n".join(
        f"== {t} ==\n{p}" for t, p in offenders)


def test_nonaggregate_parity_exact_on_fixtures(driven):
    _, parity = driven
    agg = {s.get("dest", s["ds"]) for s in _all_specs()
           if s.get("aggregate") or s.get("agency_vocab")}
    bad = [p["table"] for p in parity if p["table"] not in agg and not p["parity_ok"]]
    assert not bad, f"non-aggregate specs must be parity-exact even on fixtures: {bad}"
