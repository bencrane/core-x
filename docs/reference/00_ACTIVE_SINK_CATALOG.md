# 00 · Active Sink Catalog — the live Gen-3 system of record

> **Verified 2026-06-30** against the live `s3://data-sink/active/` bucket via a
> read-only probe: boto3 enumerated every prefix, `lance.dataset(uri).count_rows()`
> / `.schema` / `.list_indices()` recorded each dataset's grain. Row counts,
> column counts, and committed indices below are exact as of that probe. Re-run the
> probe to refresh; do not hand-edit the numbers.

> **Update 2026-07-01 — `person_id` migration.** The person id was renamed
> `contact_id`→`person_id` across `people` + the 6 person-FK datasets (`email_verifications`,
> `icp_people_for_email`, `phone_resolutions`, `work_email_mv_validations`,
> `work_email_vendor_responses`, `work_emails`) via zero-downtime expand/contract: each
> `contact_id`/`contact_id_idx` is now `person_id`/`person_id_idx`, and `people` gained a
> `BTREE(person_id)` (it never had a `contact_id` index). The 7 rows + marquee + headline index
> total below are updated for this; a full re-probe will refresh everything else.

This is the **dataset index** for the persistence plane whose write mechanics are
governed by [`02_lancedb_storage.md`](02_lancedb_storage.md). LanceDB written
directly to Cloudflare R2 under `s3://data-sink/active/<dataset>/` is the absolute
system of record ([`ARCHITECTURE.md` §4](../../ARCHITECTURE.md)); this file is the
authoritative enumeration of *what exists there* — every live dataset, its grain,
and its committed scalar/vector indices.

For the historical record of the **decommissioned** Gen-2 `dex-raw-landing-zone`
bucket (purged 2026-06-07) and the upstream-source coverage gap, see
[`DEX_R2_CATALOG_AND_GEN3_COVERAGE_GAP.md`](DEX_R2_CATALOG_AND_GEN3_COVERAGE_GAP.md).
That document is Gen-2 archaeology; **this** document is the live Gen-3 truth.

---

## Headline numbers (verified 2026-06-30)

| Metric | Value |
|---|--:|
| Live Lance datasets under `active/` | **472** |
| — top-level datasets | 389 |
| — nested datasets (under 8 container prefixes) | 83 |
| Total rows across all datasets | **2,857,205,647** |
| Non-Lance prefixes (blob stores / serving / staging) | 6 |
| Committed scalar/vector indices | 1,928 (1,068 BTREE · 856 BITMAP · 3 IVF_PQ · 1 LABEL_LIST) |
| Datasets with zero committed indices | 93 (mostly USASpending API reference dims) |

**Container prefixes** hold multiple nested Lance datasets rather than being a
dataset themselves: `usaspending/` (51), `fmcsa/` (8), `ca_ucc/` (5),
`sam_opps_attachment_manifest_play1/` (6 shards), and the snapshot-partitioned
health datasets (`nppes/`, `nppes_provider*/`, `provider_360/`, `practice_group_360/`
carry `snapshot=YYYY-MM` children). Addressed by their full nested URI, e.g.
`s3://data-sink/active/usaspending/award_search/`.

## Marquee grains (spot-check anchors)

| Dataset | Rows | Cols | Indices |
|---|--:|--:|---|
| `fec_individual_contributions` | 282,923,196 | 26 | 16 |
| `usaspending/financial_accounts_by_awards` | 454,215,610 | 64 | 1 (BTREE) |
| `usaspending/transaction_search_fabs` | 128,784,183 | 378 | 4 |
| `usaspending/transaction_search_fpds` | 107,250,527 | 378 | 4 |
| `usaspending/award_search` | 78,636,657 | 154 | 3 |
| `sam_master_entities` | 1,541,566 | 68 | 3 (BTREE) |
| `entity_profile_gold` | 1,541,566 | 22 | 6 |
| `companies` (GTM) | 25,405 | 21 | 6 (2 BTREE + 4 BITMAP) |
| `people` (GTM) | 69,242 | 9 | 4 (BTREE) |
| `company_target_industries` (GTM) | 2,050 | 6 | 3 (2 BTREE + 1 BITMAP) |

> The three **GTM identity grains** (`companies` / `people` /
> `company_target_industries`) are the standalone system of record described in
> [`pipelines/gtm/companies_people_bulk.py`](../../pipelines/gtm/companies_people_bulk.py).
> Their live schemas/indices above supersede any earlier documented figures; see
> that worker's docstring for the full grain definition and the code-vs-reality
> drift notes.

## Probe method

- Enumerate: `boto3 list_objects_v2(Prefix="active/", Delimiter="/")`, recursing
  one level where a child prefix is itself a container of Lance datasets.
- Classify: a prefix is a Lance dataset iff it carries a `_versions/` manifest
  directory. Prefixes without one and without nested Lance children are transport
  / serving / staging artifacts (blob stores keyed by content hash, tarball
  handoffs, JSON chart artifacts) — listed at the end, not counted as datasets.
- Record: `ds.count_rows()`, `ds.schema` (name · arrow type · nullable),
  `ds.list_indices()` (name · columns · type). Strictly read-only — no
  `write_dataset` / `create_scalar_index` / `delete`.

---
## Domain summary

| Domain | Datasets | Rows |
|---|--:|--:|
| USASpending | 59 | 852,454,963 |
| EPA | 125 | 606,669,415 |
| CMS | 24 | 528,437,349 |
| FEC | 1 | 282,923,196 |
| HMDA | 2 | 168,348,959 |
| State SoS/UCC | 18 | 113,354,162 |
| GTM identity | 31 | 75,121,275 |
| NPPES/Health | 8 | 53,228,256 |
| Places/Geo | 6 | 49,223,191 |
| Entity 360 | 3 | 21,188,538 |
| Crosswalks/Bridges | 15 | 19,721,635 |
| SAM.gov | 27 | 18,684,047 |
| MSHA | 19 | 14,736,562 |
| SBA | 7 | 14,425,142 |
| GovCon | 39 | 14,105,328 |
| FMCSA | 8 | 9,323,803 |
| GLEIF | 2 | 3,834,748 |
| Scratch/Staging | 16 | 3,517,228 |
| USPTO | 4 | 3,160,731 |
| Other | 6 | 1,499,570 |
| Epiq/Bankruptcy | 3 | 1,369,500 |
| Licensing | 3 | 898,684 |
| SEC/EDGAR | 6 | 354,695 |
| Form 5500 | 11 | 304,139 |
| FDIC | 6 | 190,045 |
| Reference | 9 | 62,475 |
| NCUA/NMLS | 8 | 52,832 |
| Equipment | 6 | 15,179 |
| **TOTAL** | **472** | **2,857,205,647** |

## Full per-dataset detail


### CMS

| Dataset (`active/…`) | Rows | Cols | Indices |
|---|--:|--:|---|
| `cms_dme_geography_service` | 285,891 | 22 | BTree(hcpcs_cd); Bitmap(program_year); Bitmap(rfrg_prvdr_geo_lvl) |
| `cms_dme_referring_provider` | 3,811,043 | 101 | BTree(rfrg_npi); Bitmap(program_year); Bitmap(rfrg_prvdr_spclty_desc); Bitmap(rfrg_prvdr_state_abrvtn) |
| `cms_dme_supplier` | 724,678 | 97 | BTree(suplr_npi); Bitmap(program_year); Bitmap(suplr_prvdr_spclty_desc); Bitmap(suplr_prvdr_state_abrvtn) |
| `cms_dme_supplier_rollup` | 99,387 | 9 | BTree(npi); Bitmap(is_dme_supplier) |
| `cms_dme_supplier_service` | 5,542,054 | 36 | BTree(suplr_npi); BTree(hcpcs_cd); Bitmap(program_year); Bitmap(suplr_prvdr_state_abrvtn); Bitmap(suplr_rentl_ind) |
| `cms_general_payments` | 82,290,893 | 95 | BTree(covered_recipient_npi); BTree(applicable_manufacturer_or_applicable_gpo_making_payment_id); BTree(date_of_payment); BTree(record_id); Bitmap(payment_year); Bitmap(covered_recipient_type); Bitmap(nature_of_payment_or_transfer_of_value); Bitmap(form_of_payment_or_transfer_of_value); Bitmap(recipient_state); Bitmap(dispute_status_for_publication) |
| `cms_manufacturer_dim` | 2,900 | 14 | BTree(manufacturer_id); BTree(manufacturer_name); Bitmap(manufacturer_state) |
| `cms_ownership` | 27,480 | 34 | BTree(physician_npi); BTree(applicable_manufacturer_or_applicable_gpo_making_payment_id); BTree(record_id); Bitmap(payment_year); Bitmap(physician_primary_type); Bitmap(recipient_state); Bitmap(dispute_status_for_publication); Bitmap(interest_held_by_physician_or_an_immediate_family_member) |
| `cms_partd_drug_rollup` | 1,721,485 | 15 | BTree(npi) |
| `cms_partd_geography_drug` | 1,244,452 | 26 | Bitmap(program_year); Bitmap(prscrbr_geo_lvl) |
| `cms_partd_provider` | 9,219,683 | 88 | BTree(prscrbr_npi); Bitmap(program_year); Bitmap(prscrbr_type); Bitmap(prscrbr_state_abrvtn) |
| `cms_partd_provider_drug` | 304,308,166 | 26 | BTree(prscrbr_npi); Bitmap(program_year); Bitmap(prscrbr_type); Bitmap(prscrbr_state_abrvtn) |
| `cms_physician_geography_service` | 3,228,031 | 19 | BTree(hcpcs_cd); Bitmap(program_year); Bitmap(rndrng_prvdr_geo_lvl); Bitmap(place_of_srvc) |
| `cms_physician_provider` | 13,528,933 | 85 | BTree(rndrng_npi); Bitmap(program_year); Bitmap(rndrng_prvdr_type); Bitmap(rndrng_prvdr_state_abrvtn); Bitmap(rndrng_prvdr_ent_cd) |
| `cms_physician_provider_service` | 78,482,821 | 32 | BTree(rndrng_npi); BTree(hcpcs_cd); Bitmap(program_year); Bitmap(place_of_srvc); Bitmap(rndrng_prvdr_state_abrvtn); Bitmap(rndrng_prvdr_type) |
| `cms_physician_service_rollup` | 1,694,622 | 15 | BTree(npi) |
| `cms_provider_enrollment` | 2,981,788 | 15 | BTree(npi); BTree(enrlmt_id); BTree(pecos_asct_cntl_id); Bitmap(provider_type_desc); Bitmap(state_cd) |
| `cms_provider_enrollment_npi` | 111,196 | 6 | BTree(npi); BTree(enrlmt_id) |
| `cms_provider_enrollment_practice` | 1,080,813 | 8 | BTree(enrlmt_id); Bitmap(state_cd) |
| `cms_provider_enrollment_reassignment` | 3,857,023 | 6 | BTree(reasgn_bnft_enrlmt_id); BTree(rcv_bnft_enrlmt_id) |
| `cms_provider_enrollment_specialty` | 500,163 | 7 | BTree(enrlmt_id); Bitmap(provider_type_desc) |
| `cms_provider_payment_rollup` | 1,603,039 | 15 | BTree(npi); Bitmap(recipient_type); Bitmap(has_ownership_interest); Bitmap(last_payment_year) |
| `cms_qpp_experience` | 6,154,354 | 241 | BTree(npi); Bitmap(program_year); Bitmap(practice_state_or_us_territory); Bitmap(clinician_specialty); Bitmap(participation_type) |
| `cms_research_payments` | 5,936,454 | 256 | BTree(covered_recipient_npi); BTree(applicable_manufacturer_or_applicable_gpo_making_payment_id); BTree(date_of_payment); BTree(principal_investigator_1_npi); BTree(record_id); Bitmap(payment_year); Bitmap(covered_recipient_type); Bitmap(related_product_indicator); Bitmap(recipient_state); Bitmap(dispute_status_for_publication) |

### Crosswalks/Bridges

| Dataset (`active/…`) | Rows | Cols | Indices |
|---|--:|--:|---|
| `bridge_dsbs_pdl_linkedin` | 28,936 | 6 | BTree(uei); BTree(matched_domain); BTree(pdl_company_linkedin_url); Bitmap(matched_source) |
| `bridge_sam_fmcsa_domain` | 263,076 | 5 | BTree(uei); BTree(dot_number); BTree(normalized_domain); BTree(mc_number) |
| `bridge_sam_pdl` | 801,831 | 7 | BTree(uei); BTree(pdl_company_id); BTree(duns); BTree(normalized_domain) |
| `crosswalk_epa_registry_air` | 279,103 | 2 | BTree(REGISTRY_ID); BTree(PGM_SYS_ID) |
| `crosswalk_epa_registry_enforcement` | 161,173 | 2 | BTree(REGISTRY_ID); BTree(ACTIVITY_ID) |
| `crosswalk_epa_registry_npdes` | 1,193,249 | 2 | BTree(REGISTRY_ID); BTree(NPDES_ID) |
| `crosswalk_epa_registry_program` | 4,360,148 | 3 | BTree(REGISTRY_ID); BTree(PGM_SYS_ID); Bitmap(PGM_SYS_ACRNM) |
| `crosswalk_epa_registry_rcra` | 1,578,620 | 2 | BTree(REGISTRY_ID); BTree(ID_NUMBER) |
| `crosswalk_epa_registry_sdwa` | 676,905 | 2 | BTree(REGISTRY_ID); BTree(PWSID) |
| `crosswalk_hmda_gleif` | 6,470 | 20 | BTree(lei); BTree(normalized_legal_name) |
| `crosswalk_sam_usaspending` | 1,028,144 | 15 | BTree(uei); BTree(cage_code); BTree(normalized_legal_name) |
| `crosswalk_sos_sam` | 941,838 | 24 | BTree(uei); BTree(sos_entity_key); BTree(sos_normalized_legal_name); Bitmap(match_tier); Bitmap(is_canonical); Bitmap(match_key) |
| `crosswalk_ucc_sos` | 1,920,960 | 21 | BTree(sos_entity_key); BTree(ucc_debtor_key); BTree(ucc_normalized_legal_name); Bitmap(match_tier); Bitmap(is_canonical); Bitmap(ucc_state) |
| `spine_epa_facility` | 3,240,591 | 30 | BTree(registry_id); BTree(fac_name); BTree(fac_zip5); Bitmap(fac_state); Bitmap(primary_naics); Bitmap(has_npdes); Bitmap(has_rcra); Bitmap(has_sdwa); Bitmap(has_air); Bitmap(has_enforcement); Bitmap(program_count); Bitmap(fac_compliance_status); Bitmap(fac_programs_with_snc); Bitmap(fac_major_flag); Bitmap(has_active_violation) |
| `spine_epa_facility_360` | 3,240,591 | 76 | BTree(registry_id); BTree(fac_name); BTree(fac_zip5); Bitmap(fac_state); Bitmap(primary_naics); Bitmap(fac_compliance_status); Bitmap(program_count); Bitmap(has_npdes); Bitmap(has_rcra); Bitmap(has_sdwa); Bitmap(has_air); Bitmap(has_enforcement); Bitmap(npdes_has_dmr_exceedance); Bitmap(rcra_rcra_snc_flag); Bitmap(sdwa_has_health_based_violation); Bitmap(air_caa_hpv_flag); Bitmap(enf_has_penalty) |

### EPA

| Dataset (`active/…`) | Rows | Cols | Indices |
|---|--:|--:|---|
| `epa_afs_actions` | 2,579,661 | 16 | — |
| `epa_afs_air_prg_hist_compliance` | 10,204,801 | 4 | — |
| `epa_afs_facilities` | 236,734 | 21 | — |
| `epa_afs_hpv_history` | 32,057 | 7 | — |
| `epa_aim_triggering_events` | 5,375 | 18 | BTree(NPDES_ID); Bitmap(ACTIVE_EXCEPTION) |
| `epa_air_facilities` | 278,944 | 20 | BTree(REGISTRY_ID); BTree(PGM_SYS_ID); BTree(normalized_facility_name); Bitmap(STATE); Bitmap(AIR_OPERATING_STATUS_CODE); Bitmap(CURRENT_HPV); Bitmap(AIR_POLLUTANT_CLASS_CODE) |
| `epa_air_program` | 1,139,429 | 11 | — |
| `epa_all_cso_downloads` | 9,641 | 40 | BTree(FACILITY_UIN); BTree(NPDES_ID) |
| `epa_attains_au_catchments` | 0 | 31 | — |
| `epa_case_defendants` | 200,159 | 5 | BTree(ACTIVITY_ID); BTree(CASE_NUMBER) |
| `epa_case_enforcement_conclusion_complying_actions` | 200,525 | 8 | BTree(ACTIVITY_ID); BTree(CASE_NUMBER); BTree(ENF_CONCLUSION_ID) |
| `epa_case_enforcement_conclusion_dollars` | 126,160 | 9 | BTree(ACTIVITY_ID); BTree(CASE_NUMBER); BTree(ENF_CONCLUSION_ID) |
| `epa_case_enforcement_conclusion_facilities` | 150,182 | 9 | BTree(ACTIVITY_ID); BTree(CASE_NUMBER); BTree(ENF_CONCLUSION_ID); BTree(FACILITY_UIN) |
| `epa_case_enforcement_conclusion_pollutants` | 65,079 | 13 | BTree(ACTIVITY_ID); BTree(CASE_NUMBER); BTree(ENF_CONCLUSION_ID) |
| `epa_case_enforcement_conclusion_sep` | 6,403 | 8 | BTree(ACTIVITY_ID); BTree(CASE_NUMBER); BTree(ENF_CONCLUSION_ID) |
| `epa_case_enforcement_conclusions` | 126,160 | 17 | BTree(ACTIVITY_ID); BTree(CASE_NUMBER); BTree(ENF_CONCLUSION_ID) |
| `epa_case_enforcement_type` | 143,406 | 4 | BTree(ACTIVITY_ID); BTree(CASE_NUMBER) |
| `epa_case_enforcements` | 135,053 | 25 | BTree(ACTIVITY_ID); BTree(CASE_NUMBER) |
| `epa_case_facilities` | 202,509 | 10 | BTree(ACTIVITY_ID); BTree(CASE_NUMBER); BTree(REGISTRY_ID) |
| `epa_case_law_sections` | 177,603 | 7 | BTree(ACTIVITY_ID); BTree(CASE_NUMBER) |
| `epa_case_milestones` | 508,088 | 5 | BTree(ACTIVITY_ID); BTree(ACTUAL_DATE) |
| `epa_case_penalties` | 123,490 | 9 | BTree(ACTIVITY_ID); BTree(CASE_NUMBER) |
| `epa_case_pollutants` | 92,293 | 5 | BTree(ACTIVITY_ID); BTree(CASE_NUMBER) |
| `epa_case_priorities` | 23,518 | 5 | BTree(ACTIVITY_ID); BTree(CASE_NUMBER) |
| `epa_case_programs` | 379,066 | 4 | BTree(ACTIVITY_ID); BTree(CASE_NUMBER) |
| `epa_case_regional_dockets` | 97,001 | 3 | BTree(ACTIVITY_ID); BTree(CASE_NUMBER) |
| `epa_case_related_activities` | 49,366 | 5 | BTree(ACTIVITY_ID); BTree(CASE_NUMBER) |
| `epa_case_relief_sought` | 55,328 | 4 | BTree(ACTIVITY_ID); BTree(CASE_NUMBER) |
| `epa_case_violations` | 109,678 | 5 | BTree(ACTIVITY_ID); BTree(CASE_NUMBER) |
| `epa_collection_system_permit` | 8,083 | 14 | — |
| `epa_collection_system_permits` | 4,080 | 10 | — |
| `epa_echo_demographics` | 6,783,459 | 36 | BTree(REGISTRY_ID) |
| `epa_echo_exporter` | 3,146,584 | 133 | BTree(REGISTRY_ID); Bitmap(FAC_SNC_FLG); Bitmap(FAC_COMPLIANCE_STATUS); Bitmap(FAC_MAJOR_FLAG); Bitmap(AIR_FLAG); Bitmap(NPDES_FLAG); Bitmap(RCRA_FLAG); Bitmap(SDWIS_FLAG); Bitmap(TRI_FLAG); Bitmap(GHG_FLAG); Bitmap(CAA_HPV_FLAG); Bitmap(CWA_SNC_FLAG); Bitmap(RCRA_SNC_FLAG); Bitmap(FAC_STATE) |
| `epa_entity_compliance` | 142,933 | 21 | BTree(REGISTRY_ID); BTree(normalized_legal_name); Bitmap(FAC_STATE); Bitmap(has_violations); Bitmap(is_active); Bitmap(violation_tier) |
| `epa_epa_informal_enforcement_actions` | 21,780 | 10 | BTree(PGM_SYS_ID); BTree(REGISTRY_ID) |
| `epa_facilities` | 3,240,591 | 10 | BTree(REGISTRY_ID); Bitmap(FAC_STATE) |
| `epa_facility_tribal_spatial` | 2,453,462 | 10 | — |
| `epa_frs_naics_codes` | 2,155,540 | 4 | BTree(PGM_SYS_ID); BTree(REGISTRY_ID) |
| `epa_frs_sic_codes` | 1,077,332 | 4 | BTree(PGM_SYS_ID); BTree(REGISTRY_ID) |
| `epa_icis_air_facilities` | 279,211 | 19 | BTree(PGM_SYS_ID); BTree(REGISTRY_ID) |
| `epa_icis_air_fces_pces` | 1,802,044 | 10 | BTree(ACTIVITY_ID); BTree(PGM_SYS_ID) |
| `epa_icis_air_formal_actions` | 105,656 | 10 | BTree(ACTIVITY_ID); BTree(PGM_SYS_ID) |
| `epa_icis_air_informal_actions` | 336,410 | 10 | BTree(ACTIVITY_ID); BTree(PGM_SYS_ID) |
| `epa_icis_air_pollutants` | 976,479 | 7 | BTree(PGM_SYS_ID) |
| `epa_icis_air_program_subparts` | 190,570 | 5 | BTree(PGM_SYS_ID) |
| `epa_icis_air_programs` | 456,601 | 7 | BTree(PGM_SYS_ID) |
| `epa_icis_air_stack_tests` | 646,332 | 10 | BTree(ACTIVITY_ID); BTree(PGM_SYS_ID) |
| `epa_icis_air_titlev_certs` | 2,563,435 | 7 | BTree(ACTIVITY_ID); BTree(PGM_SYS_ID) |
| `epa_icis_air_violation_history` | 101,147 | 16 | BTree(ACTIVITY_ID); BTree(PGM_SYS_ID) |
| `epa_icis_facilities` | 1,192,755 | 14 | BTree(FACILITY_UIN); BTree(NPDES_ID) |
| `epa_icis_fec_epa_inspections` | 258,597 | 16 | BTree(ACTIVITY_ID); BTree(PGM_SYS_ID); BTree(REGISTRY_ID) |
| `epa_icis_master_general_permits` | 2,823 | 27 | BTree(ACTIVITY_ID); BTree(EXTERNAL_PERMIT_NMBR) |
| `epa_icis_permits` | 1,694,646 | 28 | BTree(ACTIVITY_ID); BTree(EXTERNAL_PERMIT_NMBR) |
| `epa_npdes_attains_au_summaries` | 813,381 | 17 | BTree(NPDES_ID); BTree(REGISTRY_ID) |
| `epa_npdes_biosolids_formal_actions` | 61 | 22 | BTree(ACTIVITY_ID); BTree(NPDES_ID); BTree(REGISTRY_ID) |
| `epa_npdes_biosolids_infml_enf_actions` | 0 | 12 | BTree(ACTIVITY_ID); BTree(NPDES_ID); BTree(REGISTRY_ID) |
| `epa_npdes_biosolids_inspections` | 2,285 | 17 | BTree(ACTIVITY_ID); BTree(NPDES_ID); BTree(REGISTRY_ID) |
| `epa_npdes_biosolids_permits` | 12,454 | 25 | BTree(NPDES_ID); BTree(REGISTRY_ID) |
| `epa_npdes_biosolids_sev_violations` | 513 | 115 | BTree(REGISTRY_ID); BTree(SOURCE_ID) |
| `epa_npdes_catchments` | 1,248,092 | 20 | BTree(NPDES_ID) |
| `epa_npdes_cs_violations` | 81,176 | 18 | BTree(NPDES_ID) |
| `epa_npdes_data_groups` | 1,183,215 | 7 | BTree(ACTIVITY_ID); BTree(EXTERNAL_PERMIT_NMBR) |
| `epa_npdes_dmrs` | 422,447,436 | 58 | BTree(EXTERNAL_PERMIT_NMBR); BTree(MONITORING_PERIOD_END_DATE); Bitmap(FISCAL_YEAR); BTree(PARAMETER_CODE); Bitmap(VIOLATION_CODE); Bitmap(NODI_CODE) |
| `epa_npdes_eff_violations` | 46,361,587 | 43 | BTree(NPDES_ID); BTree(MONITORING_PERIOD_END_DATE) |
| `epa_npdes_formal_enforcement_actions` | 111,816 | 10 | BTree(ACTIVITY_ID); BTree(NPDES_ID) |
| `epa_npdes_informal_enforcement_actions` | 821,977 | 11 | BTree(ACTIVITY_ID); BTree(NPDES_ID); BTree(REGISTRY_ID) |
| `epa_npdes_inspections` | 1,889,402 | 11 | BTree(ACTIVITY_ID); BTree(NPDES_ID); BTree(REGISTRY_ID) |
| `epa_npdes_limits` | 16,575,018 | 51 | BTree(ACTIVITY_ID); BTree(EXTERNAL_PERMIT_NMBR) |
| `epa_npdes_naics` | 318,549 | 4 | BTree(NPDES_ID) |
| `epa_npdes_outfalls_layer` | 818,707 | 39 | BTree(EXTERNAL_PERMIT_NMBR) |
| `epa_npdes_perm_components` | 757,387 | 3 | BTree(EXTERNAL_PERMIT_NMBR) |
| `epa_npdes_perm_feature_coords` | 627,983 | 5 | BTree(EXTERNAL_PERMIT_NMBR) |
| `epa_npdes_ps_violations` | 395,599 | 17 | BTree(NPDES_ID) |
| `epa_npdes_qncr_history` | 7,866,031 | 8 | BTree(NPDES_ID); BTree(YEARQTR) |
| `epa_npdes_se_violations` | 300,690 | 15 | BTree(NPDES_ID) |
| `epa_npdes_sics` | 784,937 | 4 | BTree(NPDES_ID) |
| `epa_npdes_violation_enforcements` | 4,910,356 | 7 | BTree(ACTIVITY_ID) |
| `epa_permit_compliance` | 156,014 | 31 | BTree(EXTERNAL_PERMIT_NMBR); BTree(REGISTRY_ID); BTree(normalized_legal_name); Bitmap(FAC_STATE); Bitmap(PERMIT_STATUS_CODE); Bitmap(has_violations); Bitmap(is_active); Bitmap(violation_tier); Bitmap(entity_resolved) |
| `epa_permit_parameter_compliance` | 1,884,617 | 19 | BTree(EXTERNAL_PERMIT_NMBR); BTree(REGISTRY_ID); BTree(normalized_legal_name); Bitmap(PARAMETER_CODE); Bitmap(FAC_STATE); Bitmap(has_violations); Bitmap(has_exceedances); Bitmap(is_active) |
| `epa_permits` | 1,686,705 | 27 | BTree(REGISTRY_ID); BTree(EXTERNAL_PERMIT_NMBR); BTree(NPDES_ID); BTree(normalized_legal_name); Bitmap(FAC_STATE_CODE); Bitmap(PERMIT_STATUS_CODE); Bitmap(MAJOR_MINOR_STATUS_FLAG); Bitmap(PERMIT_TYPE_CODE) |
| `epa_pipeline_caa` | 66,655 | 35 | BTree(REGISTRY_ID); BTree(SOURCE_ID); Bitmap(FOUND_VIOLATION) |
| `epa_pipeline_rcra` | 456,773 | 30 | BTree(REGISTRY_ID); BTree(SOURCE_ID); Bitmap(FOUND_VIOLATION) |
| `epa_pipeline_rcra_01_evaluations` | 202,487 | 14 | BTree(REGISTRY_ID); BTree(SOURCE_ID) |
| `epa_pipeline_rcra_02_violations` | 375,400 | 13 | BTree(REGISTRY_ID); BTree(SOURCE_ID) |
| `epa_pipeline_rcra_03_enforcement_actions` | 218,947 | 13 | BTree(REGISTRY_ID); BTree(SOURCE_ID) |
| `epa_pipeline_rcra_read_me` | 5 | 3 | — |
| `epa_poll_rpt_combined_emissions` | 10,411,871 | 9 | BTree(PGM_SYS_ID); BTree(REGISTRY_ID) |
| `epa_program_links` | 4,360,148 | 13 | BTree(REGISTRY_ID); BTree(PGM_SYS_ID); Bitmap(PGM_SYS_ACRNM) |
| `epa_rcra_enforcements` | 382,172 | 11 | BTree(ID_NUMBER) |
| `epa_rcra_evaluations` | 1,162,239 | 8 | BTree(ID_NUMBER) |
| `epa_rcra_facilities` | 1,597,673 | 15 | BTree(ID_NUMBER) |
| `epa_rcra_handlers` | 1,578,504 | 17 | BTree(REGISTRY_ID); BTree(RCRA_ID); BTree(normalized_facility_name); Bitmap(STATE_CODE); Bitmap(ACTIVE_SITE); Bitmap(OPERATING_TSDF); Bitmap(FED_WASTE_GENERATOR) |
| `epa_rcra_naics` | 433,082 | 3 | BTree(ID_NUMBER) |
| `epa_rcra_violations` | 704,817 | 8 | BTree(ID_NUMBER) |
| `epa_rcra_viosnc_history` | 2,665,005 | 5 | BTree(ID_NUMBER) |
| `epa_sdwa_events_milestones` | 360,370 | 10 | BTree(PWSID) |
| `epa_sdwa_facilities` | 1,550,159 | 19 | BTree(FACILITY_ID); BTree(PWSID) |
| `epa_sdwa_geographic_areas` | 577,661 | 11 | BTree(PWSID) |
| `epa_sdwa_lcr_samples` | 924,498 | 15 | BTree(PWSID) |
| `epa_sdwa_pn_violation_assoc` | 378,063 | 12 | BTree(PWSID) |
| `epa_sdwa_pub_water_systems` | 433,698 | 51 | BTree(PWSID) |
| `epa_sdwa_ref_ansi_areas` | 3,235 | 4 | — |
| `epa_sdwa_ref_code_values` | 2,376 | 3 | — |
| `epa_sdwa_service_areas` | 422,099 | 6 | BTree(PWSID) |
| `epa_sdwa_site_visits` | 2,478,266 | 20 | BTree(PWSID) |
| `epa_sdwa_violations_enforcement` | 15,298,031 | 38 | BTree(FACILITY_ID); BTree(PWSID) |
| `epa_sewer_overflow_bypass_cause` | 5,043 | 14 | — |
| `epa_sewer_overflow_bypass_causes` | 4,387 | 9 | — |
| `epa_sewer_overflow_bypass_columns_metadata` | 102 | 7 | — |
| `epa_sewer_overflow_bypass_columns_metadata_all` | 143 | 7 | — |
| `epa_sewer_overflow_bypass_corrective_action` | 6,164 | 14 | — |
| `epa_sewer_overflow_bypass_corrective_actions` | 5,437 | 9 | — |
| `epa_sewer_overflow_bypass_event` | 4,722 | 31 | — |
| `epa_sewer_overflow_bypass_impact` | 5,186 | 14 | — |
| `epa_sewer_overflow_bypass_impacts` | 4,517 | 9 | — |
| `epa_sewer_overflow_bypass_receiving_water` | 2,185 | 12 | — |
| `epa_sewer_overflow_bypass_receiving_waters` | 1,863 | 7 | — |
| `epa_sewer_overflow_bypass_report` | 4,102 | 15 | — |
| `epa_sewer_overflow_bypass_report_events` | 4,078 | 39 | — |
| `epa_sewer_overflow_bypass_type` | 4,722 | 14 | — |
| `epa_sewer_overflow_bypass_types` | 4,078 | 9 | — |
| `epa_sewer_overflow_treatment_code` | 1,226 | 15 | — |
| `epa_sewer_overflow_treatment_codes` | 1,179 | 10 | — |
| `epa_to_sos_bridge` | 406,191 | 10 | BTree(REGISTRY_ID); BTree(normalized_legal_name) |
| `osha_daily_triggers` | 5,609 | 18 | BTree(trigger_uid); BTree(normalized_legal_name); BTree(activity_nr); Bitmap(site_state); Bitmap(viol_type) |

### Entity 360

| Dataset (`active/…`) | Rows | Cols | Indices |
|---|--:|--:|---|
| `entity_award_lines_gold` | 347,658 | 6 | BTree(uei) |
| `entity_profile_gold` | 1,541,566 | 22 | BTree(uei); BTree(cage_code); BTree(legal_name_base); BTree(total_active_obligations); BTree(primary_naics); Bitmap(is_active) |
| `entity_registrations` | 19,299,314 | 18 | BTree(uei); BTree(cage_code); BTree(extract_label) |

### Epiq/Bankruptcy

| Dataset (`active/…`) | Rows | Cols | Indices |
|---|--:|--:|---|
| `epiq_cases` | 946 | 11 | BTree(project_code); BTree(case_number); BTree(debtor_name); BTree(project_id); Bitmap(is_active); Bitmap(default_page) |
| `epiq_claims` | 605,236 | 31 | BTree(project_code); BTree(claim_number); BTree(case_number); BTree(creditor_name); BTree(claim_id); Bitmap(search_type); Bitmap(schedule_g) |
| `epiq_dockets` | 763,318 | 23 | BTree(project_code); BTree(docket_number); BTree(case_number); BTree(docket_id); Bitmap(is_adversary_proceeding); Bitmap(jurisdiction_name); Bitmap(is_project_active) |

### Equipment

| Dataset (`active/…`) | Rows | Cols | Indices |
|---|--:|--:|---|
| `equipment_catalog` | 4,358 | 20 | BTree(record_id); BTree(domain_norm); Bitmap(payload_kind); Bitmap(confidence) |
| `equipment_finance_candidates` | 429 | 11 | BTree(record_id); BTree(domain_norm); BTree(linkedin_url_norm); BTree(company_name); Bitmap(verdict) |
| `equipment_matchmaking` | 3,096 | 6 | BTree(domain_norm); Bitmap(matched_psc_count) |
| `equipment_provider` | 4,700 | 14 | BTree(record_id); BTree(domain_norm); Bitmap(is_equipment_provider); Bitmap(mode); Bitmap(confidence) |
| `equipment_rental_blitz_backfill_domains` | 1,717 | 1 | — |
| `equipment_rental_golden_overlap` | 879 | 10 | BTree(firm_domain); Bitmap(qualified_psc_count) |

### FDIC

| Dataset (`active/…`) | Rows | Cols | Indices |
|---|--:|--:|---|
| `fdic_failures` | 2 | 36 | BTree(cert); BTree(faildate); Bitmap(pstalp); Bitmap(chclass); Bitmap(restype) |
| `fdic_financial` | 4,287 | 164 | BTree(cert); BTree(repdte); Bitmap(stalp); Bitmap(bkclass); Bitmap(active) |
| `fdic_institutions` | 27,836 | 163 | BTree(cert); BTree(name); Bitmap(active); Bitmap(bkclass); Bitmap(stalp); Bitmap(stname); Bitmap(fdicregn); Bitmap(mutual); Bitmap(trust); Bitmap(specgrp) |
| `fdic_locations` | 78,298 | 41 | BTree(uninum); BTree(cert); Bitmap(stalp); Bitmap(bkclass); Bitmap(servtype); Bitmap(mainoff) |
| `fdic_sod` | 76,120 | 84 | BTree(uninumbr); BTree(cert); Bitmap(year); Bitmap(stalpbr); Bitmap(bkclass); Bitmap(bkmo) |
| `fdic_structure_changes` | 3,502 | 247 | BTree(cert); BTree(transnum); Bitmap(changecode); Bitmap(pstalp); Bitmap(class) |

### FEC

| Dataset (`active/…`) | Rows | Cols | Indices |
|---|--:|--:|---|
| `fec_individual_contributions` | 282,923,196 | 26 | Bitmap(cycle_year); Bitmap(entity_tp); Bitmap(state); Bitmap(rpt_tp); Bitmap(transaction_tp); Bitmap(transaction_pgi); Bitmap(amndt_ind); Bitmap(memo_cd); BTree(sub_id); BTree(name); BTree(cmte_id); BTree(other_id); BTree(employer); BTree(transaction_dt); BTree(employer_norm); BTree(name_norm) |

### FMCSA

| Dataset (`active/…`) | Rows | Cols | Indices |
|---|--:|--:|---|
| `fmcsa/auth_hist` | 15,830 | 16 | BTree(carrier_docket); BTree(carrier_dot) |
| `fmcsa/boc3` | 5,369 | 16 | BTree(carrier_docket); BTree(carrier_dot) |
| `fmcsa/carrier` | 5,369 | 50 | BTree(carrier_docket); BTree(carrier_dot) |
| `fmcsa/census` | 4,459,640 | 154 | BTree(carrier_dot) |
| `fmcsa/census_mail_ready` | 4,437,561 | 38 | BTree(carrier_dot); BTree(proxy_domain); BTree(status_code); BTree(carrier_operation); BTree(business_org_id); BTree(power_units) |
| `fmcsa/insurance` | 5,803 | 16 | BTree(carrier_docket) |
| `fmcsa/oos` | 392,428 | 14 | BTree(carrier_dot) |
| `fmcsa/revocation` | 1,803 | 13 | BTree(carrier_docket); BTree(carrier_dot) |

### Form 5500

| Dataset (`active/…`) | Rows | Cols | Indices |
|---|--:|--:|---|
| `form5500_main` | 19,114 | 143 | BTree(ACK_ID); BTree(SPONS_DFE_EIN); BTree(SPONS_DFE_PN) |
| `form5500_sch_a_broker` | 34,358 | 22 | BTree(ACK_ID); BTree(FORM_ID) |
| `form5500_sch_a_carrier` | 23,648 | 93 | BTree(ACK_ID); BTree(FORM_ID); BTree(SCH_A_EIN); BTree(SCH_A_PLAN_NUM) |
| `form5500_sch_c_eligible` | 1,611 | 18 | BTree(ACK_ID) |
| `form5500_sch_c_indirect` | 2,656 | 22 | BTree(ACK_ID) |
| `form5500_sch_c_provider` | 3,774 | 25 | BTree(ACK_ID) |
| `form5500_sch_c_provider_code` | 8,117 | 7 | BTree(ACK_ID) |
| `form5500_sch_c_terminated` | 81 | 22 | BTree(ACK_ID) |
| `form5500_sch_h` | 1,358 | 169 | BTree(ACK_ID) |
| `form5500_sch_i` | 10,059 | 80 | BTree(ACK_ID) |
| `form5500_sf` | 199,363 | 194 | BTree(ACK_ID); BTree(SF_SPONS_EIN); BTree(SF_PLAN_NUM) |

### GLEIF

| Dataset (`active/…`) | Rows | Cols | Indices |
|---|--:|--:|---|
| `gleif_l1_entities` | 3,357,435 | 11 | BTree(lei) |
| `gleif_l2_relationships` | 477,313 | 7 | BTree(lei); BTree(parent_lei) |

### GTM identity

| Dataset (`active/…`) | Rows | Cols | Indices |
|---|--:|--:|---|
| `blitz_find_people` | 354,385 | 18 | BTree(record_id); BTree(person_linkedin_norm); BTree(company_domain); Bitmap(loc_country_iso) |
| `booking_enrichment` | 8 | 16 | BTree(ical_uid); BTree(normalized_domain); BTree(run_id); BTree(parallel_run_id) |
| `booking_enrichment_raw` | 9 | 6 | BTree(parallel_run_id); BTree(ical_uid); BTree(run_id) |
| `capital_provider_signals` | 7,676 | 13 | BTree(normalized_domain) |
| `clay_find_companies` | 201,550 | 36 | BTree(record_id); BTree(domain_norm); BTree(linkedin_slug); BTree(linkedin_company_id); Bitmap(is_generic_domain); Bitmap(domain_is_live); Bitmap(hq_country_iso); Bitmap(hq_state); Bitmap(hq_region) |
| `clay_find_people` | 988,932 | 28 | BTree(record_id); BTree(person_id); BTree(linkedin_url_norm); BTree(domain_norm); Bitmap(loc_country_iso); Bitmap(loc_state); Bitmap(loc_region) |
| `close_sfnet_leads` | 168 | 7 | BTree(sfnet_company_id); BTree(normalized_domain); BTree(close_lead_id) |
| `companies` | 25,405 | 21 | Bitmap(industry); Bitmap(employee_size_band); Bitmap(company_type); Bitmap(hq_region); BTree(company_id); BTree(normalized_domain) |
| `company_addresses` | 1,584,946 | 53 | BTree(entity_key); BTree(uei); BTree(domain_norm); BTree(company_linkedin_url); BTree(primary_naics); BTree(legal_business_name); Bitmap(address_source); Bitmap(winner_state); Bitmap(winner_country_code); Bitmap(had_sam_physical); Bitmap(had_sam_mailing); Bitmap(had_prospeo); Bitmap(had_overture); Bitmap(had_blitz) |
| `company_target_industries` | 2,050 | 6 | BTree(company_id); BTree(normalized_domain); Bitmap(target_industry) |
| `demand_company_target_verticals` | 36,665 | 7 | BTree(company_id); BTree(normalized_domain); Bitmap(target_industry) |
| `discovered_websets` | 5 | 21 | BTree(discovered_domain); BTree(exa_webset_id); BTree(exa_item_id) |
| `email_verifications` | 1,971 | 14 | BTree(person_id); BTree(email); BTree(company_domain); Bitmap(verification_status); Bitmap(mv_resultcode) |
| `firmographics_blitz` | 255,418 | 23 | BTree(domain_norm); BTree(uei); Bitmap(industry); Bitmap(employee_size_band); Bitmap(company_type); Bitmap(hq_region) |
| `firmographics_company_map_serving` | 255,848 | 35 | BTree(uei); BTree(addr_hash); BTree(domain_norm); BTree(primary_naics); BTree(latest_award_action_date); BTree(founded_year); BTree(entity_active_obligated_usd); BTree(award_count); Bitmap(naics2); Bitmap(industry); Bitmap(employee_size_band); Bitmap(company_type); Bitmap(physical_address_state); Bitmap(has_federal_awards); Bitmap(is_active) |
| `gtm_prime_targets` | 4,011 | 25 | BTree(recipient_uei); BTree(primary_naics); BTree(dollar_growth); BTree(obligated_t12m); BTree(new_awards_t12m); BTree(award_growth); BTree(subaward_dollars); BTree(n_distinct_subs); Bitmap(vertical); Bitmap(known_subcontractor); Bitmap(outreach_motion) |
| `icp_people_for_email` | 35,708 | 13 | BTree(person_id); BTree(person_linkedin_url); BTree(company_domain) |
| `icp_waterfall_targets` | 40,417 | 7 | BTree(target_key); BTree(company_linkedin_url); BTree(normalized_domain) |
| `parallel_research` | 12 | 11 | BTree(company_id); BTree(run_id); BTree(parallel_run_id) |
| `parallel_research_raw` | 10 | 6 | BTree(run_id); BTree(parallel_run_id) |
| `pdl_companies` | 35,446,771 | 12 | BTree(pdl_company_id); BTree(company_name); BTree(linkedin_url); BTree(domain); BTree(locality); BTree(year_founded); Bitmap(industry); Bitmap(country); Bitmap(region); Bitmap(employee_size_range) |
| `pdl_normalized_companies` | 35,446,771 | 15 | BTree(pdl_company_id); BTree(company_name_norm); BTree(company_legal_base); BTree(normalized_domain); BTree(linkedin_slug); Bitmap(is_generic_domain) |
| `people` | 69,242 | 9 | BTree(company_id); BTree(normalized_domain); BTree(person_linkedin_url); BTree(person_id) |
| `phone_resolutions` | 62,128 | 13 | BTree(person_id); BTree(person_linkedin_url); BTree(company_domain); BTree(phone); Bitmap(phone_status); Bitmap(phone_type); Bitmap(source_vendor); Bitmap(country_code) |
| `prospeo_company_export` | 10,711 | 77 | BTree(record_id); BTree(prospeo_company_id); BTree(domain_norm); BTree(company_domain); Bitmap(company_country_code); Bitmap(company_state); Bitmap(capital_provider_json_capital_type); Bitmap(ag_financing_classification_is_ag_financing_provider) |
| `sfnet_main_contacts` | 168 | 18 | BTree(normalized_domain); BTree(linkedin_url); BTree(resolved_contact_id); BTree(resolved_company_id); BTree(sfnet_person_id); BTree(sfnet_company_id) |
| `staffing_agencies` | 24,398 | 16 | BTree(domain_norm); BTree(company_linkedin_url); Bitmap(employee_band); Bitmap(firmo_source) |
| `title_enrichment` | 15,668 | 14 | BTree(person_linkedin_url_norm); BTree(person_linkedin_url); BTree(record_id); BTree(title_norm); Bitmap(normalized_level); Bitmap(normalized_function); Bitmap(confidence); Bitmap(model); Bitmap(source) |
| `work_email_mv_validations` | 77,570 | 13 | BTree(person_id); BTree(email); Bitmap(verification_status); Bitmap(mv_resultcode); Bitmap(source_table) |
| `work_email_vendor_responses` | 62,442 | 13 | BTree(person_id); Bitmap(source_vendor) |
| `work_emails` | 110,212 | 16 | BTree(person_id); BTree(email_norm); BTree(company_domain); Bitmap(verification_status); Bitmap(source_vendor); Bitmap(mv_resultcode) |

### GovCon

| Dataset (`active/…`) | Rows | Cols | Indices |
|---|--:|--:|---|
| `active_award_labor_demand` | 1,080 | 28 | BTree(contract_award_unique_key); BTree(recipient_uei); BTree(naics_code); Bitmap(labor_role); Bitmap(naics_sector); Bitmap(req_clearance_level_max); Bitmap(active_current); Bitmap(active_potential); Bitmap(has_clearance); Bitmap(business_size); Bitmap(type_of_set_aside) |
| `capability_lanes` | 674,585 | 22 | BTree(uei); BTree(dst_combo); BTree(dst_naics); Bitmap(is_dsbs); Bitmap(evidence_tier) |
| `capability_profile` | 78,219 | 33 | BTree(uei); Bitmap(is_dsbs); Bitmap(federal_status) |
| `contractor_award_summary` | 581,923 | 28 | BTree(recipient_uei) |
| `ffata_exec_comp` | 29,615 | 7 | BTree(recipient_uei); BTree(name_key); Bitmap(officer_rank); Bitmap(source_channel) |
| `govcon_active_awards` | 189,272 | 127 | BTree(contract_award_unique_key); BTree(recipient_uei); BTree(naics_code); BTree(psc_code); BTree(pop_current_end); BTree(pop_potential_end); BTree(solicitation_identifier); BTree(scope_words); BTree(scope_chars); BTree(total_dollars_obligated); BTree(base_and_exercised_options_value); BTree(number_of_offers_received); Bitmap(business_size); Bitmap(type_of_set_aside); Bitmap(award_or_idv_flag); Bitmap(active_current); Bitmap(active_potential); Bitmap(has_option_tail); Bitmap(pop_unknown); Bitmap(has_subcontracting_plan); Bitmap(subcontracting_plan_code); Bitmap(has_substantive_scope); Bitmap(has_directional_scope); Bitmap(psc_category); Bitmap(psc_fsg); Bitmap(psc_is_service); Bitmap(extent_competed); Bitmap(type_of_contract_pricing); Bitmap(contract_bundling); Bitmap(performance_based_service_acquisition); Bitmap(construction_wage_rate_requirements); Bitmap(labor_standards); Bitmap(commercial_item_acquisition_procedures); Bitmap(solicitation_procedures); Bitmap(consolidated_contract); Bitmap(multi_year_contract); Bitmap(undefinitized_action); Bitmap(fair_opportunity_limited_sources); Bitmap(other_than_full_and_open_competition); Bitmap(domestic_or_foreign_entity); Bitmap(organizational_type); Bitmap(service_disabled_veteran_owned_business); Bitmap(veteran_owned_business); Bitmap(women_owned_small_business); Bitmap(economically_disadvantaged_women_owned_small_business); Bitmap(woman_owned_business); Bitmap(historically_underutilized_business_zone_hubzone_firm); Bitmap(c8a_program_participant); Bitmap(small_disadvantaged_business); Bitmap(self_certified_small_disadvantaged_business); Bitmap(sba_certified_8a_joint_venture); Bitmap(joint_venture_women_owned_small_business); Bitmap(emerging_small_business); Bitmap(black_american_owned_business); Bitmap(hispanic_american_owned_business); Bitmap(native_american_owned_business); Bitmap(asian_pacific_american_owned_business); Bitmap(subcontinent_asian_asian_indian_american_owned_business); Bitmap(american_indian_owned_business); Bitmap(alaskan_native_corporation_owned_firm); Bitmap(native_hawaiian_organization_owned_firm); Bitmap(tribally_owned_firm); Bitmap(minority_owned_business); Bitmap(other_minority_owned_business) |
| `govcon_active_awards_map_serving` | 189,270 | 36 | BTree(pop_current_end); BTree(pop_potential_end); BTree(contract_current_value_usd); BTree(contract_potential_value_usd); BTree(contract_obligated_usd); BTree(winner_uei); BTree(addr_hash); BTree(naics_code); BTree(psc_code); BTree(awarding_sub_agency); Bitmap(naics2); Bitmap(psc_category); Bitmap(state); Bitmap(pop_state); Bitmap(set_aside); Bitmap(business_size); Bitmap(has_option_tail); Bitmap(award_or_idv_flag); Bitmap(awarding_agency); Bitmap(vertical); Bitmap(work_type); Bitmap(equipment_intensity); Bitmap(has_extracted_scope); Bitmap(requires_clearance); Bitmap(requires_cmmc); Bitmap(req_clearance_level_max) |
| `govcon_active_subcontracting_obligations` | 26,573 | 62 | BTree(contract_award_unique_key); BTree(recipient_uei); BTree(recipient_parent_uei); BTree(naics_code); Bitmap(business_size_code); Bitmap(subcontracting_plan_code); Bitmap(has_subcontracting_plan); Bitmap(has_reported_subs); Bitmap(active_current); Bitmap(active_potential); Bitmap(prime_any_socioeconomic_designation); Bitmap(service_disabled_veteran_owned_business); Bitmap(veteran_owned_business); Bitmap(women_owned_small_business); Bitmap(economically_disadvantaged_women_owned_small_business); Bitmap(woman_owned_business); Bitmap(historically_underutilized_business_zone_hubzone_firm); Bitmap(c8a_program_participant); Bitmap(small_disadvantaged_business); Bitmap(self_certified_small_disadvantaged_business); Bitmap(sba_certified_8a_joint_venture); Bitmap(joint_venture_women_owned_small_business); Bitmap(emerging_small_business); Bitmap(minority_owned_business); Bitmap(other_minority_owned_business) |
| `govcon_award_capability_profiles` | 35,726 | 36 | BTree(contract_award_unique_key); BTree(recipient_uei); Bitmap(requires_clearance); Bitmap(requires_cmmc); Bitmap(has_extracted_scope); Bitmap(marked_award); Bitmap(coverage_truncated); Bitmap(is_primary_target); Bitmap(req_clearance_level_max); Bitmap(type_of_set_aside) |
| `govcon_award_requirements` | 242,641 | 26 | BTree(resource_id); BTree(contract_award_unique_key); Bitmap(requirement_type); Bitmap(clearance_level); Bitmap(mandatory); Bitmap(validated) |
| `govcon_award_scope_requirements` | 35,028 | 39 | BTree(contract_award_unique_key); BTree(labor_headcount_total); BTree(wage_floor_max); BTree(n_requirement_types); BTree(n_requirements); Bitmap(has_standard_compliance); Bitmap(has_vehicle_constraint); Bitmap(has_labor_category); Bitmap(has_deliverable); Bitmap(has_past_performance); Bitmap(has_equipment_capability); Bitmap(has_certification); Bitmap(has_clearance); Bitmap(has_staffing_constraint); Bitmap(has_insurance_bonding); Bitmap(has_license); Bitmap(requires_cmmc); Bitmap(req_clearance_level_max); Bitmap(has_extracted_scope); Bitmap(is_primary_target); Bitmap(req_lists_truncated); Bitmap(coverage_truncated) |
| `govcon_award_solicitation_profiles` | 35,028 | 36 | BTree(contract_award_unique_key); BTree(recipient_uei); Bitmap(requires_clearance); Bitmap(requires_cmmc); Bitmap(has_extracted_scope); Bitmap(marked_award); Bitmap(coverage_truncated); Bitmap(is_primary_target); Bitmap(req_clearance_level_max); Bitmap(type_of_set_aside) |
| `govcon_doc_scope` | 18,957 | 20 | — |
| `govcon_equipment_rental_construction_match` | 1,353,066 | 36 | BTree(sub_uei); BTree(prime_uei); BTree(road_miles); BTree(award_value); Bitmap(contract_award_unique_key); Bitmap(tier); Bitmap(supply_addr_is_hq_pin); Bitmap(sub_state); Bitmap(pop_state_code); Bitmap(supply_centroid_source); Bitmap(demand_centroid_source); Bitmap(business_size); Bitmap(has_subcontracting_plan); Bitmap(sub_sdvosb); Bitmap(sub_veteran_owned); Bitmap(sub_wosb); Bitmap(sub_edwosb); Bitmap(sub_woman_owned); Bitmap(sub_hubzone); Bitmap(sub_8a); Bitmap(sub_self_cert_sdb); Bitmap(sub_minority_owned); Bitmap(sub_jv_wosb); Bitmap(sub_any_designation) |
| `govcon_firm_construction_proximity` | 109,121 | 7 | BTree(firm_domain); BTree(firm_uei); BTree(psc_code) |
| `govcon_firm_military_proximity` | 4,585,357 | 18 | BTree(entity_key); BTree(uei); BTree(domain_norm); BTree(primary_naics); BTree(base_objectid); BTree(legal_business_name); BTree(base_site_name); Bitmap(winner_state); Bitmap(base_state_code); Bitmap(base_site_reporting_component_code); Bitmap(base_is_firrma_site); Bitmap(base_is_joint_base) |
| `govcon_labor_demand` | 20,598 | 18 | BTree(resource_id); BTree(contract_award_unique_key); Bitmap(naics_code); Bitmap(clearance_level) |
| `govcon_pricing` | 170,532 | 16 | — |
| `govcon_prime_subaward_propensity` | 2,106 | 16 | BTree(prime_awardee_uei); BTree(naics_code); BTree(last_subaward_date); Bitmap(is_primary_naics) |
| `govcon_prime_trajectories` | 769,264 | 16 | BTree(recipient_uei); Bitmap(is_bonded_vertical) |
| `govcon_requirements_extract_ledger` | 35,905 | 14 | — |
| `govcon_scope_vectors` | 1,894,737 | 16 | IVF_PQ(embedding); BTree(resource_id); BTree(contract_award_unique_key); Bitmap(naics_code); Bitmap(header_class) |
| `govcon_sub_capability_vectors` | 102,937 | 11 | BTree(subawardee_uei); IVF_PQ(embedding) |
| `govcon_sub_certifications_mv` | 90,210 | 58 | BTree(uei); BTree(naics_primary); BTree(zipcode); BTree(next_cert_expiration_date); BTree(normalized_domain); Bitmap(state); Bitmap(universe_tier); Bitmap(cert_lifecycle); Bitmap(contact_source); Bitmap(geo_source); Bitmap(in_dsbs); Bitmap(is_subawardee); Bitmap(is_targeting_candidate); Bitmap(is_greenfield); Bitmap(cert_any); Bitmap(cert_8a); Bitmap(cert_hubzone); Bitmap(cert_wosb); Bitmap(cert_edwosb); Bitmap(cert_vosb); Bitmap(cert_sdvosb); Bitmap(cert_8a_jv); Bitmap(sam_self_any); Bitmap(sam_self_minority_owned); Bitmap(sam_self_sdb); Bitmap(sam_self_woman_owned); Bitmap(sam_self_veteran_owned); Bitmap(sam_registration_active); Bitmap(sam_present); Bitmap(has_subaward_history); Bitmap(domain_is_generic) |
| `govcon_sub_diversification` | 234,999 | 25 | BTree(sub_uei); BTree(cand_prime_uei); BTree(award_key); BTree(award_action_date); BTree(n_incumbent_primes); Bitmap(naics2_aligned); Bitmap(naics4_aligned) |
| `govcon_sub_self_reported_tags` | 66,275 | 7 | BTree(desc_sha) |
| `govcon_sub_targeting` | 192,747 | 13 | BTree(contract_award_unique_key); BTree(candidate_sub_uei); BTree(prime_uei) |
| `govcon_subawardee_capability_profiles` | 6,586 | 45 | BTree(sub_uei); Bitmap(requires_clearance); Bitmap(requires_cmmc); Bitmap(has_extracted_scope); Bitmap(poc_available); Bitmap(marked_solicitation); Bitmap(req_clearance_level_max) |
| `govcon_subawardee_designations` | 25,450 | 22 | BTree(subawardee_uei); BTree(cage_code); BTree(primary_naics); BTree(designation_count); Bitmap(service_disabled_veteran_owned_business); Bitmap(veteran_owned_business); Bitmap(women_owned_small_business); Bitmap(economically_disadvantaged_women_owned_small_business); Bitmap(woman_owned_business); Bitmap(historically_underutilized_business_zone_hubzone_firm); Bitmap(c8a_program_participant); Bitmap(self_certified_small_disadvantaged_business); Bitmap(minority_owned_business); Bitmap(joint_venture_women_owned_small_business); Bitmap(any_socioeconomic_designation); Bitmap(matched_in_sam); Bitmap(sam_is_active) |
| `govcon_subawardee_profiles` | 25,450 | 52 | BTree(sub_uei); Bitmap(requires_clearance); Bitmap(requires_cmmc); Bitmap(has_extracted_scope); Bitmap(poc_available); Bitmap(marked_solicitation); Bitmap(req_clearance_level_max); Bitmap(tag_source); Bitmap(hq_state); Bitmap(pop_state) |
| `govcon_teaming_edges` | 115,366 | 12 | BTree(prime_uei); BTree(sub_uei) |
| `govcon_unknown` | 1,620,871 | 17 | IVF_PQ(embedding); BTree(resource_id); BTree(contract_award_unique_key); Bitmap(naics_code); Bitmap(header_class); Bitmap(lexicon_hit) |
| `subaward_combo_edges` | 64,508 | 9 | BTree(src_combo); BTree(dst_combo) |
| `subaward_combo_nodes` | 1,391 | 10 | BTree(combo_id); BTree(naics); BTree(psc) |
| `subaward_naics_psc` | 199,901 | 13 | BTree(subawardee_uei); BTree(prime_awardee_uei); BTree(prime_award_unique_key); BTree(prime_naics_code); Bitmap(prime_psc_code) |
| `subawardee_combo_expansion` | 230,427 | 18 | BTree(subawardee_uei); BTree(dst_combo); BTree(dst_naics); Bitmap(is_dsbs) |
| `subawardee_solicitations_bridge` | 17,633 | 10 | BTree(subawardee_uei); BTree(notice_id) |
| `subawardee_solicitations_manifest` | 6,524 | 10 | BTree(notice_id); BTree(resource_id) |
| `subawardee_work_profile` | 25,450 | 34 | BTree(subawardee_uei); BTree(subawardee_parent_uei); BTree(subawardee_state_code) |

### HMDA

| Dataset (`active/…`) | Rows | Cols | Indices |
|---|--:|--:|---|
| `hmda_lar` | 168,296,950 | 109 | BTree(lei); BTree(action_taken); BTree(property_state); BTree(county_code) |
| `hmda_panels` | 52,009 | 29 | BTree(lei); BTree(respondent_id) |

### Licensing

| Dataset (`active/…`) | Rows | Cols | Indices |
|---|--:|--:|---|
| `cslb_licenses` | 244,760 | 56 | BTree(license_number); BTree(business_name); BTree(full_business_name); Bitmap(primary_status); Bitmap(business_type); Bitmap(wc_coverage_type); Bitmap(state); Bitmap(county); Bitmap(asbestos_registration) |
| `cslb_personnel` | 406,192 | 24 | BTree(license_number); BTree(personnel_name); Bitmap(name_type); Bitmap(surety_type) |
| `cslb_workers_comp` | 247,732 | 13 | BTree(license_number); BTree(wc_policy_number); Bitmap(wc_coverage_type); Bitmap(wc_insurance_company) |

### MSHA

| Dataset (`active/…`) | Rows | Cols | Indices |
|---|--:|--:|---|
| `msha_accidents` | 273,065 | 61 | BTree(DOCUMENT_NO); BTree(MINE_ID); BTree(CONTROLLER_ID); BTree(OPERATOR_ID); BTree(CONTRACTOR_ID); BTree(ACCIDENT_DT); BTree(CONTROLLER_NAME); BTree(OPERATOR_NAME); BTree(CONTROLLER_NAME_norm); BTree(OPERATOR_NAME_norm); Bitmap(DEGREE_INJURY_CD); Bitmap(CLASSIFICATION_CD); Bitmap(ACCIDENT_TYPE_CD); Bitmap(FIPS_STATE_CD); Bitmap(COAL_METAL_IND) |
| `msha_area_samples` | 8,368 | 19 | BTree(CONTRACTOR_ID); BTree(EVENT_NO); BTree(MINE_ID); BTree(SAMPLE_NO) |
| `msha_civil_penalty_dockets_decisions` | 479,439 | 31 | BTree(ASSESS_CASE_NO); BTree(DOCKET_NO); BTree(MINE_ID); BTree(VIOLATION_NO); BTree(VIOLATOR_ID) |
| `msha_coal_dust_samples` | 2,985,614 | 32 | BTree(CASS_NUM); BTree(MINE_ID) |
| `msha_conferences` | 161,623 | 9 | BTree(CONFERENCE_NO); BTree(ISSUANCE_NO) |
| `msha_contested_violations` | 448,158 | 41 | BTree(CITATION_NO); BTree(DOCKET_NO); BTree(MINE_ID) |
| `msha_contractor_master` | 44,618 | 23 | BTree(CONTRACTOR_ID); BTree(CONTRACTOR_NAME); Bitmap(in_production_registry); Bitmap(primary_coal_metal) |
| `msha_contractors` | 1,630,676 | 19 | BTree(CONTRACTOR_ID); Bitmap(COAL_METAL_IND); Bitmap(SUBUNIT_CD); BTree(CONTRACTOR_NAME); BTree(CONTRACTOR_NAME_norm) |
| `msha_corporate_history` | 168,809 | 17 | BTree(CONTROLLER_ID); BTree(OPERATOR_ID); BTree(MINE_ID); Bitmap(CONTROLLER_TYPE); Bitmap(COAL_METAL_IND); BTree(OPERATOR_NAME); BTree(CONTROLLER_NAME); BTree(OPERATOR_NAME_norm); BTree(CONTROLLER_NAME_norm) |
| `msha_enforcement_ledger` | 3,076,347 | 122 | BTree(MINE_ID); BTree(VIOLATOR_ID); BTree(VIOLATION_NO); BTree(CONTROLLER_ID); BTree(EVENT_NO); BTree(ASSESS_CASE_NO); BTree(VIOLATION_ISSUE_DT); BTree(PROPOSED_PENALTY_AMT); BTree(VIOLATOR_NAME); BTree(CONTROLLER_NAME); BTree(CONTRACTOR_ID); BTree(DOCKET_NO); BTree(VIOLATOR_NAME_norm); BTree(CONTROLLER_NAME_norm); Bitmap(SIG_SUB); Bitmap(CIT_ORD_SAFE); Bitmap(VIOLATOR_TYPE_CD); Bitmap(COAL_METAL_IND) |
| `msha_inspections` | 1,147,232 | 47 | BTree(CONTROLLER_ID); BTree(EVENT_NO); BTree(MINE_ID); BTree(OPERATOR_ID) |
| `msha_mines` | 91,803 | 83 | BTree(MINE_ID); BTree(CURRENT_CONTROLLER_ID); BTree(CURRENT_OPERATOR_ID); Bitmap(COAL_METAL_IND); Bitmap(STATE); Bitmap(CURRENT_MINE_STATUS); BTree(CURRENT_OPERATOR_NAME); BTree(CURRENT_CONTROLLER_NAME); BTree(BUSINESS_NAME); BTree(ZIP_CD); BTree(CURRENT_OPERATOR_NAME_norm); BTree(CURRENT_CONTROLLER_NAME_norm); BTree(BUSINESS_NAME_norm) |
| `msha_mines_prod_quarterly` | 2,714,840 | 15 | BTree(MINE_ID) |
| `msha_mines_prod_yearly` | 657,546 | 13 | BTree(MINE_ID) |
| `msha_noise_samples` | 274,645 | 31 | BTree(CONTRACTOR_ID); BTree(EVENT_NO); BTree(MINE_ID); BTree(VIOLATION_NO) |
| `msha_orders_issued` | 3,830 | 15 | BTree(CONTRACTOR_ID); BTree(CONTROLLER_ID_VIOLATIONS); BTree(MINE_ID); BTree(VIOLATION_NO) |
| `msha_personal_health_samples` | 310,908 | 22 | BTree(CONTRACTOR_ID); BTree(EVENT_NO); BTree(MINE_ID); BTree(SAMPLE_NO) |
| `msha_quartz_samples` | 167,238 | 21 | BTree(LABORATORY_NO); BTree(MINE_ID) |
| `msha_site_master` | 91,803 | 35 | BTree(MINE_ID); BTree(CURRENT_CONTROLLER_ID); BTree(CURRENT_OPERATOR_ID); BTree(MSHA_REPORTED_CONTROLLER_ID); BTree(CURRENT_CONTROLLER_NAME); Bitmap(CURRENT_MINE_STATUS); Bitmap(COAL_METAL_IND); Bitmap(STATE); Bitmap(multi_controller_flag); Bitmap(multi_operator_flag); Bitmap(silica_overexposure) |

### NCUA/NMLS

| Dataset (`active/…`) | Rows | Cols | Indices |
|---|--:|--:|---|
| `ncua_credit_unions` | 4,250 | 26 | BTree(charter_number); BTree(join_number); Bitmap(credit_union_type); Bitmap(ncua_region); Bitmap(state_mailing_address); Bitmap(low_income_designation) |
| `nmls_mcr_applications_received` | 2,746 | 8 | BTree(filing_year); Bitmap(state); Bitmap(filing_quarter) |
| `nmls_mcr_forward_by_business_line` | 8,130 | 9 | BTree(filing_year); Bitmap(state); Bitmap(filing_quarter); Bitmap(business_line) |
| `nmls_mcr_forward_by_purpose` | 8,181 | 9 | BTree(filing_year); Bitmap(state); Bitmap(filing_quarter); Bitmap(loan_purpose) |
| `nmls_mcr_forward_by_type` | 10,836 | 9 | BTree(filing_year); Bitmap(state); Bitmap(filing_quarter); Bitmap(loan_type) |
| `nmls_mcr_license_activity` | 11,240 | 14 | BTree(status_start_year); Bitmap(state_regulator); Bitmap(entity_type); Bitmap(status_start_quarter) |
| `nmls_mcr_reverse_by_business_line` | 7,390 | 9 | BTree(filing_year); Bitmap(state); Bitmap(filing_quarter); Bitmap(business_line) |
| `nmls_state_entity_counts` | 59 | 12 | Bitmap(state_agency); Bitmap(report_period) |

### NPPES/Health

| Dataset (`active/…`) | Rows | Cols | Indices |
|---|--:|--:|---|
| `nppes/snapshot=2026-05` | 9,551,447 | 334 | BTree(npi); BTree(provider_first_line_business_practice_location_address); Bitmap(provider_business_practice_location_address_state_name) |
| `nppes/snapshot=2026-06` | 9,606,683 | 334 | BTree(npi); BTree(provider_first_line_business_practice_location_address); Bitmap(provider_business_practice_location_address_state_name) |
| `nppes_provider/snapshot=2026-05` | 9,551,447 | 39 | BTree(npi); BTree(last_name); BTree(practice_address_line1); BTree(practice_zip5); BTree(enumeration_date); BTree(last_update_date); Bitmap(entity_type_code); Bitmap(is_active); Bitmap(primary_taxonomy_code); Bitmap(practice_state); Bitmap(enumeration_year) |
| `nppes_provider_identifier/snapshot=2026-05` | 2,759,800 | 7 | BTree(npi); BTree(identifier_value); Bitmap(identifier_type_code); Bitmap(identifier_state) |
| `nppes_provider_taxonomy/snapshot=2026-05` | 11,952,809 | 8 | Bitmap(taxonomy_code); Bitmap(is_primary); Bitmap(license_state) |
| `nppes_taxonomy_ref` | 883 | 9 | BTree(taxonomy_code); Bitmap(grouping); Bitmap(section) |
| `practice_group_360/snapshot=2026-06` | 253,740 | 18 | BTree(group_enrlmt_id); BTree(org_name); Bitmap(group_state) |
| `provider_360/snapshot=2026-06` | 9,551,447 | 152 | BTree(npi); BTree(practice_zip5); BTree(last_name); BTree(smallest_practice_group_enrlmt_id); Bitmap(entity_type_code); Bitmap(is_active); Bitmap(practice_state); Bitmap(primary_taxonomy_code); Bitmap(med_a1_provider_type); Bitmap(med_a1_latest_year); Bitmap(mips_final_score_year); Bitmap(is_independent_candidate) |

### Other

| Dataset (`active/…`) | Rows | Cols | Indices |
|---|--:|--:|---|
| `epd_lec_status` | 3,408 | 12 | BTree(record_id); BTree(domain_norm); Bitmap(confidence); Bitmap(epd_lec_status) |
| `rollup_epa_air` | 34,689 | 6 | BTree(registry_id); Bitmap(caa_hpv_flag); Bitmap(has_air_violation) |
| `rollup_epa_enforcement` | 55,393 | 11 | BTree(registry_id); Bitmap(has_federal_case); Bitmap(has_penalty) |
| `rollup_epa_npdes` | 672,942 | 13 | BTree(registry_id); Bitmap(has_dmr_exceedance); Bitmap(npdes_compliance_tier) |
| `rollup_epa_rcra` | 301,396 | 12 | BTree(registry_id); Bitmap(rcra_snc_flag); Bitmap(has_rcra_violation) |
| `rollup_epa_sdwa` | 431,742 | 9 | BTree(registry_id); Bitmap(has_health_based_violation); Bitmap(pws_type) |

### Places/Geo

| Dataset (`active/…`) | Rows | Cols | Indices |
|---|--:|--:|---|
| `geocode_xwalk` | 369,218 | 11 | BTree(addr_hash) |
| `military_bases_lance` | 824 | 19 | BTree(objectid); BTree(site_name); BTree(feature_name); Bitmap(country); Bitmap(state_name_code); Bitmap(operational_status); Bitmap(site_reporting_component_code); Bitmap(is_firrma_site); Bitmap(is_joint_base) |
| `overture_places` | 16,273,123 | 14 | BTree(id); BTree(name); BTree(postcode); BTree(locality); BTree(hilbert); BTree(domain); BTree(phone); Bitmap(region); Bitmap(category) |
| `overture_places__bak_2026-05-20.0_20260606T192125Z` | 16,273,123 | 13 | BTree(id); BTree(longitude); BTree(latitude); BTree(name); BTree(postcode); BTree(locality); Bitmap(region) |
| `overture_places__bak_v3_2026-05-20.0_20260606T220113Z` | 16,273,123 | 10 | BTree(id); BTree(name); BTree(postcode); BTree(locality); BTree(hilbert); Bitmap(region); Bitmap(category) |
| `zcta_zip_centroids` | 33,780 | 5 | BTree(zcta5) |

### Reference

| Dataset (`active/…`) | Rows | Cols | Indices |
|---|--:|--:|---|
| `industries_served` | 4,358 | 13 | BTree(record_id); BTree(domain_norm); Bitmap(confidence) |
| `naics_index` | 20,398 | 5 | BTree(naics_code) |
| `naics_psc_vertical_map` | 279 | 10 | BTree(naics_code); BTree(psc_code) |
| `naics_reference` | 2,125 | 11 | BTree(naics_code); BTree(parent_code); Bitmap(level); Bitmap(code_len); Bitmap(sector_code); Bitmap(is_trilateral) |
| `psc_reference` | 6,108 | 21 | BTree(psc_code); BTree(parent_psc_code); Bitmap(is_active); Bitmap(is_product); Bitmap(psc_category); Bitmap(code_len); Bitmap(cm_level1) |
| `ref_rbcs_taxonomy` | 18,882 | 21 | BTree(hcpcs_cd); Bitmap(rbcs_cat) |
| `reference/psc_equipment_mapping` | 19 | 4 | BTree(psc_code) |
| `schema_catalog` | 10,284 | 15 | BTree(dataset_name); BTree(source_group); BTree(column_name); BTree(catalog_run_id); BTree(schema_fingerprint); BTree(captured_at) |
| `shovels_tags` | 22 | 9 | BTree(id) |

### SAM.gov

| Dataset (`active/…`) | Rows | Cols | Indices |
|---|--:|--:|---|
| `sam_attachment_content_dedup` | 126,901 | 6 | — |
| `sam_attachment_extraction` | 249,899 | 21 | — |
| `sam_attachment_files` | 127,607 | 22 | Bitmap(mime_declared); BTree(resource_id); BTree(sha256); Bitmap(status) |
| `sam_attachment_gtm_scope` | 126,901 | 9 | BTree(resource_id); Bitmap(gtm_scope); Bitmap(scope_reason) |
| `sam_attachment_worklist` | 127,576 | 10 | — |
| `sam_attachment_worklist_T0_T2` | 4,259 | 20 | — |
| `sam_attachment_worklist_T1` | 18,336 | 20 | — |
| `sam_attachment_worklist_T3` | 6,089 | 20 | — |
| `sam_business_type_code_dict` | 12 | 11 | BTree(code); BTree(namespace); BTree(designation_key) |
| `sam_master_contacts` | 4,373,319 | 13 | BTree(uei) |
| `sam_master_domains` | 709,546 | 5 | BTree(normalized_domain); BTree(uei) |
| `sam_master_entities` | 1,541,566 | 68 | BTree(uei); BTree(primary_naics); BTree(cage_code) |
| `sam_normalized_entities` | 1,541,566 | 11 | BTree(uei); BTree(normalized_legal_name); BTree(legal_name_base); BTree(cage_code); BTree(primary_naics); Bitmap(is_active) |
| `sam_opps_attachment_manifest` | 331,401 | 20 | BTree(notice_id); BTree(resource_id); BTree(naics_code); Bitmap(trigger_relevant); Bitmap(mime_type); Bitmap(access_level) |
| `sam_opps_attachment_manifest_equipment_rental/shard_000` | 6,994 | 20 | BTree(notice_id); BTree(resource_id); BTree(naics_code); Bitmap(trigger_relevant); Bitmap(mime_type); Bitmap(access_level) |
| `sam_opps_attachment_manifest_open_biddable` | 50,367 | 20 | BTree(notice_id); BTree(resource_id); BTree(naics_code); Bitmap(trigger_relevant); Bitmap(mime_type); Bitmap(access_level) |
| `sam_opps_attachment_manifest_play1/shard_000` | 167,485 | 20 | BTree(notice_id); BTree(resource_id); BTree(naics_code); Bitmap(trigger_relevant); Bitmap(mime_type); Bitmap(access_level) |
| `sam_opps_attachment_manifest_play1/shard_001` | 164,629 | 20 | BTree(notice_id); BTree(resource_id); BTree(naics_code); Bitmap(trigger_relevant); Bitmap(mime_type); Bitmap(access_level) |
| `sam_opps_attachment_manifest_play1/shard_002` | 167,118 | 20 | BTree(notice_id); BTree(resource_id); BTree(naics_code); Bitmap(trigger_relevant); Bitmap(mime_type); Bitmap(access_level) |
| `sam_opps_attachment_manifest_play1/shard_003` | 164,628 | 20 | BTree(notice_id); BTree(resource_id); BTree(naics_code); Bitmap(trigger_relevant); Bitmap(mime_type); Bitmap(access_level) |
| `sam_opps_attachment_manifest_play1/shard_004` | 167,287 | 20 | BTree(notice_id); BTree(resource_id); BTree(naics_code); Bitmap(trigger_relevant); Bitmap(mime_type); Bitmap(access_level) |
| `sam_opps_attachment_manifest_play1/shard_005` | 83,922 | 20 | BTree(notice_id); BTree(resource_id); BTree(naics_code); Bitmap(trigger_relevant); Bitmap(mime_type); Bitmap(access_level) |
| `sam_opps_attachment_manifest_remediation/shard_000` | 106,115 | 20 | BTree(notice_id); BTree(resource_id); BTree(naics_code); Bitmap(trigger_relevant); Bitmap(mime_type); Bitmap(access_level) |
| `sam_opps_attachment_manifest_sb500k` | 13,005 | 20 | BTree(notice_id); BTree(resource_id); BTree(naics_code); Bitmap(trigger_relevant); Bitmap(mime_type); Bitmap(access_level) |
| `sam_opps_attachment_manifest_winners` | 155,183 | 22 | BTree(notice_id); BTree(sol_norm); BTree(contract_award_unique_key); BTree(solicitation_identifier); BTree(resource_id) |
| `sam_pocs` | 8,065,679 | 19 | BTree(uei); BTree(cage_code); BTree(name_key); BTree(last_name); Bitmap(poc_type); Bitmap(source_family) |
| `sam_ucc_debtor_overlap` | 86,657 | 19 | BTree(uei); BTree(sos_entity_key); BTree(sam_legal_business_name); Bitmap(overlap_confidence); Bitmap(has_active_lien); Bitmap(officer_confirms); Bitmap(ucc_states) |

### SBA

| Dataset (`active/…`) | Rows | Cols | Indices |
|---|--:|--:|---|
| `dsbs_combo_coldstart` | 473,300 | 18 | BTree(subawardee_uei); BTree(dst_combo); BTree(dst_naics) |
| `dsbs_combo_expansion` | 22,394 | 17 | BTree(subawardee_uei); BTree(dst_combo); BTree(dst_naics) |
| `ppp` | 11,468,210 | 60 | BTree(loan_number); BTree(naics_code); BTree(servicing_lender_location_id); BTree(originating_lender_location_id); Bitmap(processing_method); Bitmap(loan_status); Bitmap(borrower_state); Bitmap(project_state); Bitmap(business_type); BTree(normalized_legal_name); BTree(zip_code); BTree(legal_name_base) |
| `sba_504` | 227,404 | 47 | BTree(borr_name); BTree(borr_state); BTree(project_state); BTree(naics_code); BTree(approval_fy); BTree(loan_status); BTree(location_id); BTree(congressional_district); BTree(business_type); BTree(sba_district_office); BTree(cdc_name); BTree(third_party_lender_name); BTree(sba_surrogate_id); BTree(normalized_legal_name); BTree(zip_code); BTree(legal_name_base) |
| `sba_7a` | 1,947,098 | 50 | BTree(borr_name); BTree(borr_state); BTree(project_state); BTree(naics_code); BTree(approval_fy); BTree(loan_status); BTree(location_id); BTree(congressional_district); BTree(business_type); BTree(sba_district_office); BTree(bank_name); BTree(bank_fdic_number); BTree(sba_surrogate_id); BTree(normalized_legal_name); BTree(zip_code); BTree(legal_name_base) |
| `sba_dsbs_certified_firms` | 67,234 | 132 | BTree(uei); BTree(cage_code); BTree(entity_detail_id); BTree(meili_primary_key); BTree(naics_primary); BTree(zipcode); BTree(county); Bitmap(state); Bitmap(county_code); Bitmap(concat_state_congressional_district); Bitmap(cert_programs); Bitmap(msa); Bitmap(active_8a_boolean); Bitmap(active_8a_jv_boolean); Bitmap(active_hz_boolean); Bitmap(active_wosb_boolean); Bitmap(active_edwosb_boolean); Bitmap(active_vosb_boolean); Bitmap(active_sdvosb_boolean); Bitmap(prev_8a_boolean); Bitmap(prev_hz_boolean); Bitmap(prev_wosb_boolean); Bitmap(prev_edwosb_boolean); Bitmap(prev_vosb_boolean); Bitmap(prev_sdvosb_boolean); Bitmap(self_native_owned_boolean); Bitmap(self_american_indian_owned_boolean); Bitmap(self_tribal_owned_boolean); Bitmap(self_anc_owned_boolean); Bitmap(self_nho_boolean); Bitmap(self_hubzone_jv_boolean); Bitmap(self_cdc_boolean) |
| `sbir_awards` | 219,502 | 46 | BTree(sbir_surrogate_id); BTree(uei); BTree(duns); BTree(company); BTree(agency_tracking_number); BTree(contract); Bitmap(phase); Bitmap(program); Bitmap(agency); Bitmap(state); Bitmap(award_year); Bitmap(hubzone_owned); Bitmap(woman_owned); Bitmap(socially_economically_disadvantaged) |

### SEC/EDGAR

| Dataset (`active/…`) | Rows | Cols | Indices |
|---|--:|--:|---|
| `edgar_cik_map` | 10,433 | 7 | BTree(cik_str); BTree(cik10); BTree(ticker); Bitmap(exchange) |
| `edgar_form_4` | 191,998 | 25 | BTree(accession_number); BTree(issuer_cik); BTree(issuer_trading_symbol); Bitmap(document_type); Bitmap(fiscal_quarter) |
| `edgar_form_d` | 57,496 | 35 | BTree(accession_number); BTree(primary_issuer_cik); Bitmap(submission_type); Bitmap(industry_group_type); Bitmap(fiscal_quarter) |
| `sec_adv_firm_profile` | 36,846 | 103 | BTree(crd_number); BTree(lei); BTree(total_regulatory_aum); BTree(discretionary_aum); BTree(non_discretionary_aum); BTree(total_employees); BTree(advisory_employees); BTree(num_clients); BTree(total_accounts); Bitmap(filer_type); Bitmap(is_ria); Bitmap(is_era); Bitmap(economics_reported); Bitmap(has_website); Bitmap(business_address_state); Bitmap(aum_band); Bitmap(employee_band); Bitmap(client_count_band); Bitmap(primary_client_type); Bitmap(large_fund_adviser_flag); Bitmap(advises_private_funds); Bitmap(has_wrap_program); Bitmap(has_smas); Bitmap(has_custody); Bitmap(is_broker_dealer); Bitmap(any_disciplinary); Bitmap(serves_individuals); Bitmap(serves_hnw_individuals); Bitmap(serves_banks); Bitmap(serves_investment_companies); Bitmap(serves_bdc); Bitmap(serves_pooled_vehicles); Bitmap(serves_pension); Bitmap(serves_charities); Bitmap(serves_state_muni); Bitmap(serves_other_advisers); Bitmap(serves_insurance_co); Bitmap(serves_sovereign_wealth); Bitmap(serves_corporations); Bitmap(serves_other); Bitmap(act_financial_planning); Bitmap(act_pm_individuals); Bitmap(act_pm_investment_companies); Bitmap(act_pm_pooled); Bitmap(act_pm_institutional); Bitmap(act_pension_consulting); Bitmap(act_adviser_selection); Bitmap(act_newsletters); Bitmap(act_ratings); Bitmap(act_market_timing); Bitmap(act_seminars); Bitmap(act_other); Bitmap(comp_pct_aum); Bitmap(comp_hourly); Bitmap(comp_subscription); Bitmap(comp_fixed); Bitmap(comp_commissions); Bitmap(comp_performance); Bitmap(comp_other) |
| `sec_adv_part1` | 36,846 | 21 | BTree(crd_number); BTree(lei); BTree(business_address_state) |
| `sec_adv_w` | 21,076 | 22 | BTree(crd_number); BTree(business_address_state) |

### Scratch/Staging

| Dataset (`active/…`) | Rows | Cols | Indices |
|---|--:|--:|---|
| `_dl_worklist_equipment` | 3,040 | 20 | — |
| `_dl_worklist_remediation` | 29,234 | 20 | — |
| `_play1_target_universe` | 275,630 | 7 | — |
| `_play1_target_universe_equipment_rental` | 11,712 | 7 | — |
| `_play1_target_universe_remediation` | 32,653 | 7 | — |
| `_sample/usaspending_fpds_canonical_txn_sample` | 3,114,940 | 75 | BTree(contract_transaction_unique_key); BTree(contract_award_unique_key); BTree(recipient_uei); BTree(action_date); BTree(last_modified_date); BTree(naics_code); BTree(product_or_service_code); BTree(federal_action_obligation); BTree(recipient_hash); BTree(award_id_piid); Bitmap(action_date_fiscal_year); Bitmap(type_of_set_aside_code); Bitmap(awarding_agency_code); Bitmap(award_type_code); Bitmap(idv_type_code); Bitmap(canonical_source) |
| `_seg_rechunk_dedup` | 8,728 | 6 | — |
| `_seg_rechunk_ledger` | 17,289 | 21 | — |
| `_smoke_stage2_sb500k` | 446 | 20 | BTree(notice_id); BTree(resource_id); BTree(naics_code); Bitmap(trigger_relevant); Bitmap(mime_type); Bitmap(access_level) |
| `_smoke_subk_files` | 40 | 17 | BTree(resource_id); BTree(sha256); Bitmap(status); Bitmap(worklist_tier) |
| `_smoke_subk_worklist` | 6,278 | 20 | — |
| `_stage2_target_sb500k` | 2,093 | 7 | — |
| `_subawardee_download_manifest` | 41 | 10 | — |
| `_subawardee_download_worklist_snapshot` | 31 | 10 | — |
| `_subk_attach_manifest` | 8,795 | 20 | — |
| `_subk_attach_worklist_T3_T4` | 6,278 | 20 | — |

### State SoS/UCC

| Dataset (`active/…`) | Rows | Cols | Indices |
|---|--:|--:|---|
| `ca_sos_agents` | 8,560,095 | 18 | BTree(entity_num); BTree(entity_name_clean); Bitmap(agent_type); Bitmap(physical_state) |
| `ca_sos_entities` | 9,389,688 | 41 | BTree(entity_num); BTree(entity_name_clean); BTree(last_si_file_number); Bitmap(entity_type); Bitmap(entity_status); Bitmap(jurisdiction); Bitmap(filing_type); Bitmap(standing_sos); Bitmap(standing_ftb); Bitmap(principal_state); Bitmap(mailing_state) |
| `ca_sos_principals` | 18,670,722 | 18 | BTree(entity_num); BTree(entity_name_clean); BTree(last_name); Bitmap(position_type); Bitmap(state) |
| `ca_ucc/debtor_index` | 5,855,416 | 26 | BTree(ucc1_num); BTree(ucc3_num); BTree(debtor_org_name); BTree(debtor_last_name); BTree(debtor_postal_code); Bitmap(debtor_type); Bitmap(debtor_state); Bitmap(action_type) |
| `ca_ucc/debtors` | 5,855,416 | 21 | BTree(ucc1_num); BTree(ucc3_num); BTree(org_name); BTree(last_name); BTree(postal_code); BTree(city); Bitmap(debtor_type); Bitmap(state); Bitmap(country); BTree(normalized_legal_name); BTree(zip_code); BTree(legal_name_base) |
| `ca_ucc/filing_amendments` | 3,305,823 | 6 | BTree(ucc1_num); BTree(ucc3_num); Bitmap(action_type) |
| `ca_ucc/filings` | 7,751,890 | 12 | BTree(ucc1_num); BTree(ucc3_num); Bitmap(action_type); Bitmap(filing_type); Bitmap(alt_designation_type) |
| `ca_ucc/secured_parties` | 4,743,627 | 18 | BTree(ucc1_num); BTree(ucc3_num); BTree(org_name); BTree(last_name); BTree(postal_code); BTree(city); Bitmap(secured_party_type); Bitmap(state); Bitmap(country) |
| `co_sos` | 3,056,896 | 38 | BTree(entity_id); BTree(entity_name); BTree(jurisdiction_of_formation); BTree(agent_organization_name); Bitmap(entity_status); Bitmap(entity_type); Bitmap(principal_state) |
| `co_ucc_transactions` | 2,555,824 | 15 | BTree(master_document_id); BTree(transaction_id); BTree(file_id); Bitmap(transaction_type); Bitmap(filing_type); Bitmap(document_type); Bitmap(continuation); Bitmap(termination_flag); Bitmap(manufactured_home_transactions) |
| `fl_federal_tax_liens` | 22,519 | 30 | BTree(normalized_legal_name); BTree(zip5); BTree(doc_number) |
| `fl_sos_corporations` | 1,260,599 | 29 | BTree(document_number); BTree(corporate_name); BTree(registered_agent_name); Bitmap(status); Bitmap(filing_type) |
| `fl_sos_events` | 14,455,118 | 9 | BTree(document_number); Bitmap(event_code) |
| `ny_sos` | 4,219,360 | 33 | BTree(dos_id); BTree(current_entity_name); BTree(initial_dos_filing_date); BTree(dos_process_name); BTree(registered_agent_name); Bitmap(entity_type); Bitmap(county); Bitmap(jurisdiction); Bitmap(dos_process_state) |
| `sos_normalized_master` | 17,926,543 | 12 | BTree(normalized_legal_name); BTree(legal_name_base); BTree(zip_code); Bitmap(source_state) |
| `ucc_co_collateral` | 1,682,948 | 19 | BTree(file_id); Bitmap(action_type); Bitmap(record_status); Bitmap(farm_product_flag); Bitmap(county) |
| `ucc_co_debtors` | 1,985,901 | 31 | BTree(file_id); BTree(party_name_normalized); BTree(organization_name); BTree(last_name); BTree(party_zip5); Bitmap(action_type); Bitmap(record_status); Bitmap(state); BTree(normalized_legal_name); BTree(zip_code); BTree(legal_name_base) |
| `ucc_co_secured_parties` | 2,055,777 | 24 | BTree(file_id); BTree(party_name_normalized); BTree(organization_name); BTree(last_name); BTree(party_zip5); Bitmap(action_type); Bitmap(record_status); Bitmap(state) |

### USASpending

| Dataset (`active/…`) | Rows | Cols | Indices |
|---|--:|--:|---|
| `usaspending/agency` | 1,530 | 11 | — |
| `usaspending/appropriation_account_balances` | 627,988 | 33 | — |
| `usaspending/award_category` | 14 | 6 | — |
| `usaspending/award_search` | 78,636,657 | 154 | BTree(parent_uei); BTree(naics_code); BTree(recipient_uei) |
| `usaspending/budget_authority` | 7,661 | 9 | — |
| `usaspending/bureau_title_lookup` | 5,496 | 7 | — |
| `usaspending/cgac` | 192 | 8 | — |
| `usaspending/covid_faba_spending` | 3,443 | 30 | — |
| `usaspending/dabs_submission_window_schedule` | 112 | 15 | — |
| `usaspending/disaster_emergency_fund_code` | 48 | 12 | — |
| `usaspending/duns` | 2,915,289 | 23 | BTree(awardee_or_recipient_uniqu) |
| `usaspending/federal_account` | 3,436 | 10 | — |
| `usaspending/financial_accounts_by_awards` | 454,215,610 | 64 | BTree(award_id) |
| `usaspending/financial_accounts_by_program_activity_object_class` | 10,235,016 | 57 | — |
| `usaspending/frec` | 166 | 8 | — |
| `usaspending/frec_map` | 13,464 | 10 | — |
| `usaspending/gtas_sf133_balances` | 968,328 | 34 | — |
| `usaspending/historic_parent_duns` | 3,198,417 | 10 | BTree(awardee_or_recipient_uniqu); BTree(ultimate_parent_unique_ide) |
| `usaspending/historical_appropriation_account_balances` | 249,643 | 29 | — |
| `usaspending/naics` | 1,741 | 9 | — |
| `usaspending/object_class` | 105 | 13 | — |
| `usaspending/office` | 86,510 | 15 | — |
| `usaspending/overall_totals` | 141 | 9 | — |
| `usaspending/parent_award` | 987,705 | 17 | — |
| `usaspending/program_activity_park` | 9,115 | 6 | — |
| `usaspending/psc` | 3,836 | 13 | — |
| `usaspending/recipient_lookup` | 17,754,022 | 24 | BTree(uei); BTree(parent_uei) |
| `usaspending/recipient_profile` | 18,275,944 | 20 | BTree(uei); BTree(parent_uei) |
| `usaspending/ref_city_county_state_code` | 202,520 | 21 | — |
| `usaspending/ref_country_code` | 260 | 12 | — |
| `usaspending/ref_population_cong_district` | 441 | 10 | — |
| `usaspending/ref_population_county` | 3,290 | 10 | — |
| `usaspending/ref_program_activity` | 79,173 | 16 | — |
| `usaspending/references_cfda` | 4,149 | 48 | — |
| `usaspending/references_definition` | 151 | 11 | — |
| `usaspending/reporting_agency_missing_tas` | 304,969 | 10 | — |
| `usaspending/reporting_agency_overview` | 10,989 | 17 | — |
| `usaspending/reporting_agency_tas` | 626,552 | 12 | — |
| `usaspending/rosetta` | 1 | 6 | — |
| `usaspending/state_data` | 448 | 14 | — |
| `usaspending/subaward_search` | 9,801,723 | 210 | BTree(awardee_or_recipient_uei); BTree(ultimate_parent_uei); BTree(sub_awardee_or_recipient_uei); BTree(sub_ultimate_parent_uei); BTree(naics); BTree(sub_naics) |
| `usaspending/submission_attributes` | 7,532 | 20 | — |
| `usaspending/subtier_agency` | 1,490 | 10 | — |
| `usaspending/summary_state_view` | 15,971 | 16 | — |
| `usaspending/toptier_agency` | 198 | 15 | — |
| `usaspending/transaction_search_fabs` | 128,784,183 | 378 | BTree(recipient_uei); BTree(parent_uei); BTree(naics_code); BTree(cage_code) |
| `usaspending/transaction_search_fpds` | 107,250,527 | 378 | BTree(recipient_uei); BTree(parent_uei); BTree(naics_code); BTree(cage_code) |
| `usaspending/treasury_appropriation_account` | 25,544 | 36 | — |
| `usaspending/uei_crosswalk` | 3,323,130 | 7 | BTree(uei) |
| `usaspending/uei_crosswalk_2021` | 3,279,911 | 7 | — |
| `usaspending/zips_grouped` | 53,646 | 11 | — |
| `usaspending_api_catalog` | 176 | 18 | BTree(endpoint_path) |
| `usaspending_api_fresh/contract_prime_txn` | 1,986,682 | 297 | BTree(federal_action_obligation); BTree(naics_code); BTree(contract_transaction_unique_key); BTree(last_modified_date); BTree(contract_award_unique_key); BTree(action_date); BTree(cage_code); BTree(product_or_service_code); BTree(recipient_uei) |
| `usaspending_api_fresh/contract_subaward` | 199,901 | 118 | BTree(subaward_sam_report_last_modified_date); BTree(prime_awardee_uei); BTree(subaward_number); BTree(subaward_action_date); BTree(subaward_amount); BTree(prime_award_naics_code); BTree(subawardee_uei); BTree(prime_award_unique_key); BTree(prime_award_piid) |
| `usaspending_archive_delta_fpds` | 3,060,070 | 302 | BTree(contract_transaction_unique_key); BTree(contract_award_unique_key); BTree(recipient_uei); BTree(action_date); BTree(last_modified_date); Bitmap(correction_delete_ind); Bitmap(archive_snapshot_stamp) |
| `usaspending_archive_full_fpds` | 2,975,677 | 300 | BTree(contract_transaction_unique_key); BTree(contract_award_unique_key); BTree(recipient_uei); BTree(action_date); BTree(last_modified_date); Bitmap(archive_snapshot_stamp) |
| `usaspending_awards_map_serving` | 1,111,438 | 42 | BTree(action_date); BTree(action_obligated_usd); BTree(winner_uei); BTree(addr_hash); BTree(city); BTree(county); BTree(pop_city); BTree(awarding_sub_agency); BTree(psc_code); Bitmap(naics2); Bitmap(state); Bitmap(winner_type); Bitmap(pop_state); Bitmap(awarding_agency); Bitmap(set_aside); Bitmap(is_active); Bitmap(psc_category); Bitmap(fiscal_year); Bitmap(business_size); Bitmap(action_type); Bitmap(is_option_exercise); Bitmap(vertical); Bitmap(work_type); Bitmap(equipment_intensity); Bitmap(has_extracted_scope); Bitmap(requires_clearance); Bitmap(requires_cmmc); Bitmap(req_clearance_level_max) |
| `usaspending_contracts_map_serving` | 1,075,214 | 36 | BTree(contract_obligated_usd); BTree(contract_ceiling_usd); BTree(naics_code); BTree(psc_code); BTree(awarding_sub_agency); BTree(last_action_date); BTree(winner_uei); BTree(contract_award_unique_key); BTree(action_count); Bitmap(naics2); Bitmap(psc_category); Bitmap(state); Bitmap(pop_state); Bitmap(awarding_agency); Bitmap(set_aside); Bitmap(business_size); Bitmap(vertical); Bitmap(work_type); Bitmap(equipment_intensity); Bitmap(is_active); Bitmap(fiscal_year); Bitmap(has_extracted_scope); Bitmap(requires_clearance); Bitmap(requires_cmmc); Bitmap(req_clearance_level_max) |
| `usaspending_winners_map_serving` | 67,378 | 31 | BTree(winner_uei); BTree(addr_hash); BTree(entity_obligated_usd); BTree(award_count); BTree(last_action_date); BTree(teaming_dollars_5y); BTree(n_teaming_primes); Bitmap(naics2); Bitmap(state); Bitmap(winner_type); Bitmap(has_extracted_scope); Bitmap(requires_clearance); Bitmap(requires_cmmc); Bitmap(req_clearance_level_max) |

### USPTO

| Dataset (`active/…`) | Rows | Cols | Indices |
|---|--:|--:|---|
| `uspto_tm_applications` | 66,331 | 32 | BTree(mark_identification); BTree(registration_number); BTree(serial_number); Bitmap(mark_drawing_code); Bitmap(status_code) |
| `uspto_tm_assignments` | 1,557,545 | 18 | BTree(assignment_id); BTree(reel_no); BTree(frame_no); Bitmap(action_key_code); Bitmap(purge_indicator) |
| `uspto_tm_assignments_historical` | 1,380,594 | 17 | BTree(rf_id); BTree(reel_no); BTree(frame_no); LabelList(property_serial_numbers) |
| `uspto_tm_ttab` | 156,261 | 16 | BTree(proceeding_key); Bitmap(type_code); Bitmap(status_code); BTree(proceeding_number) |

## Non-Lance prefixes under active/ (transport / serving / staging — not datasets)

| Prefix | Holds |
|---|---|
| `active/_seg_p2b_handoff_B/` | tar.gz handoff/result archives (segmentation staging) |
| `active/_seg_p2b_stage/` | tar.gz backup archive (segmentation staging) |
| `active/_smoke_subk_blobs/` | content-hash-keyed blob store (smoke test) |
| `active/federal_serving/` | JSON chart artifacts + map_entities.parquet + manifest.json (serving tier) |
| `active/sam_attachment_blobs/` | content-hash-keyed raw SAM attachment blob store |
| `active/sba_dsbs_raw/` | raw SBA DSBS JSON payloads (8a/edwosb/hubzone/sdvosb/vosb/wosb) |
