# dex R2 (`dex-raw-landing-zone`) — Full Catalog & Gen-3 Coverage Gap

**Captured:** 2026-06-07 (live `rclone` listing).  
**dex R2 total:** 52,869 objects / 1.7 TiB  
  - raw parquet landings: 29,204 objs / 392.5 GiB across 90 prefixes  
  - DEX Lance warehouse (`polaris-warehouse/`): 23,665 objs / 1.3 TiB across 46 namespaces  
**Gen-3 `data-sink/active`:** 142 datasets

## 1. GAP LIST — sources in dex R2 with NO Gen-3 equivalent (re-acquire from upstream)

> These are the only rows that matter for re-acquisition. Pull each from its
> **upstream source** — do NOT port the dex parquet (junk fragments).

| Upstream source | dex size | where in dex |
|---|---:|---|
| NOAA AIS vessel traffic | 88.6 GiB | raw:noaa_ais |
| USPTO patents (trademarks covered separately) | 27.9 GiB | pw:uspto_patent_lance, raw:uspto-patents |
| SEC DERA financial-statement datasets | 15.8 GiB | pw:sec_dera, raw:sec-dera |
| SEC IAPD investment-adviser reps | 9.0 GiB | raw:sec-iapd |
| openFDA | 3.1 GiB | pw:openfda, raw:openfda |
| ClinicalTrials.gov | 1.7 GiB | pw:clinicaltrials, raw:clinicaltrials-gov |
| misc federal (verify contents) | 1.7 GiB | raw:federal |
| IRS Form 990 | 1.7 GiB | raw:irs-990 |
| SEC BDC / SOI (business development cos) | 1.4 GiB | pw:sec_bdc, raw:sec-bdc |
| global corporate hierarchies (derived) | 728.5 MiB | pw:global_corporate_hierarchies_lance |
| Active-Outreach / Clay people enrichment | 690.3 MiB | pw:ae_jobs, raw:ae-jobs |
| FL DOR NAL (raw landed, never materialized) | 580.4 MiB | raw:fl-dor-nal |
| Google Maps places | 382.3 MiB | raw:google-maps |
| BLS OEWS wage stats | 371.9 MiB | raw:bls-oews |
| NCUA credit unions | 317.3 MiB | raw:ncua |
| NYC DOB NOW | 311.3 MiB | raw:nyc-dob-now |
| WARN notices | 264.9 MiB | pw:warn |
| Chicago licensing | 251.6 MiB | pw:chicago |
| NIPR/state insurance producers | 173.2 MiB | raw:insurance-producers |
| FDIC institutions | 138.2 MiB | raw:fdic |
| IRS Business Master File | 133.3 MiB | raw:irs-bmf |
| NYC property | 123.0 MiB | raw:nyc-property |
| Chicago permit contractors | 96.0 MiB | raw:chicago-permit-contractors |
| Grants.gov | 71.8 MiB | raw:grants-gov |
| FL CILB construction licensing | 61.7 MiB | raw:fl-cilb |
| WARN layoff notices | 44.2 MiB | raw:warn |
| TX TDLR licensing | 32.6 MiB | raw:tx-tdlr |
| TX state procurement | 32.1 MiB | raw:txstate |
| NY construction | 29.2 MiB | raw:ny-data-construction |
| JSearch jobs | 26.6 MiB | pw:jsearch, raw:jsearch |
| CA eProcure (archived) | 21.0 MiB | raw:cal-eprocure-archived |
| HUD multifamily | 19.6 MiB | raw:hud-multifamily |
| SBIR/STTR awards | 18.4 MiB | raw:sbir |
| NYC contract awards | 14.3 MiB | raw:nyc-contract-awards |
| USDA Rural Development | 13.7 MiB | raw:usda-rd |
| NY MTA procurement | 11.4 MiB | raw:ny-mta-procurements |
| WA L&I contractors | 9.1 MiB | raw:wa-lni-contractors |
| podcast index | 7.4 MiB | raw:podcasts |
| SFNet members | 7.0 MiB | raw:sfnet-members |
| CA DFPI | 5.4 MiB | raw:dfpi |
| NY local authority procurement | 4.8 MiB | raw:ny-local-authority-procurements |
| OR Construction Contractors Board | 2.8 MiB | raw:or-ccb |
| AZ Registrar of Contractors | 2.6 MiB | raw:az-roc |
| Chicago home-repair licensing | 1.7 MiB | raw:chicago-home-repair |
| Caltrans | 1.6 MiB | raw:caltrans |
| NYC DCWP home improvement | 1.2 MiB | raw:nyc-dcwp-home-improvement |
| IL IDFPR roofing | 1.1 MiB | raw:il-idfpr-roofing |
| CA OPSC school facilities | 831.3 KiB | raw:opsc |
| AZ state RFP | 678.6 KiB | pw:azstate |
| Glassdoor | 603.5 KiB | pw:glassdoor, raw:glassdoor |
| franchisor registry | 563.4 KiB | raw:franchisors |
| NCUA officers | 481.4 KiB | raw:ncua-officers |
| AZ state RFP/procurement | 200.4 KiB | raw:azstate |
| Clay people enrichment | 155.6 KiB | pw:clay |
| FL DOT | 139.9 KiB | raw:fdot |
| ELFA funding-source list | 40.3 KiB | raw:elfa-fundingsource |
| SBA SBIC directory | 33.2 KiB | raw:sba-sbic-directory |

**Gap total: ~155.8 GiB of genuinely uncovered source data.**

## 2. Full raw-landing catalog (top-level prefixes)

| Prefix | Status | Size | Objs | Range | Gen-3 |
|---|---|---:|---:|---|---|
| usaspending | COVERED | 103.3 GiB | 512 | 2026-05-08→2026-06-03 | usaspending,contractor_award_summary |
| noaa_ais | GAP | 88.6 GiB | 366 | 2026-05-07→2026-05-08 | NOAA AIS vessel traffic |
| fmcsa | COVERED | 47.5 GiB | 472 | 2026-05-07→2026-05-30 | fmcsa |
| uspto-patents | GAP | 27.2 GiB | 62 | 2026-05-21→2026-05-21 | USPTO patents (trademarks covered separately) |
| sec-edgar | COVERED | 23.5 GiB | 2006 | 2026-05-09→2026-05-30 | edgar_cik_map,edgar_form_4,edgar_form_d |
| fec | COVERED | 11.1 GiB | 24 | 2026-05-08→2026-05-09 | fec_individual_contributions |
| overture | COVERED | 9.6 GiB | 16 | 2026-05-07→2026-05-07 | overture_places |
| sec-iapd | GAP | 9.0 GiB | 17671 | 2026-05-10→2026-05-21 | SEC IAPD investment-adviser reps |
| hmda | COVERED | 8.7 GiB | 19 | 2026-05-07→2026-05-08 | hmda_lar,hmda_panels |
| usaspending-derived | DERIVED | 8.2 GiB | 2 | 2026-05-11→2026-05-11 | from usaspending |
| sam-gov | COVERED | 7.3 GiB | 50 | 2026-05-08→2026-05-11 | entity_registrations,sam_master_contacts |
| fmcsa-derived | DERIVED | 4.6 GiB | 52 | 2026-05-10→2026-05-30 | from fmcsa |
| sos-fl | COVERED | 4.5 GiB | 5 | 2026-05-16→2026-05-16 | fl_sos_corporations |
| pdl | COVERED | 4.3 GiB | 2 | 2026-05-07→2026-05-07 | pdl_companies,pdl_normalized_companies |
| sam-gov-opps | COVERED | 3.7 GiB | 65 | 2026-05-09→2026-05-30 | data-sink/sam-gov-opps tier |
| cms-open-payments | COVERED | 3.5 GiB | 343 | 2026-05-07→2026-05-08 | cms_general_payments,cms_research_payments |
| sec-dera | GAP | 3.4 GiB | 712 | 2026-05-18→2026-05-30 | SEC DERA financial-statement datasets |
| uspto-trademarks | COVERED | 3.1 GiB | 849 | 2026-05-08→2026-05-09 | uspto_tm_applications |
| sba | COVERED | 3.0 GiB | 240 | 2026-05-08→2026-05-08 | ppp,sba_504,sba_7a |
| sos-ca | COVERED | 2.7 GiB | 4 | 2026-05-15→2026-05-15 | ca_sos_entities |
| epiq | COVERED | 1.9 GiB | 3364 | 2026-05-08→2026-05-30 | epiq_cases,epiq_claims,epiq_dockets |
| federal | GAP | 1.7 GiB | 207 | 2026-05-08→2026-05-08 | misc federal (verify contents) |
| irs-990 | GAP | 1.7 GiB | 56 | 2026-05-09→2026-05-10 | IRS Form 990 |
| dol-5500 | COVERED | 1.6 GiB | 317 | 2026-05-07→2026-05-08 | form5500_main |
| sba-derived | DERIVED | 941.7 MiB | 2 | 2026-05-10→2026-05-11 | from sba |
| nppes | COVERED | 937.1 MiB | 4 | 2026-05-08→2026-05-08 | nppes,nppes_provider |
| ucc-ca | COVERED | 831.5 MiB | 5 | 2026-05-12→2026-05-12 | ca_ucc |
| fl-dor-nal | GAP | 580.4 MiB | 67 | 2026-05-29→2026-05-29 | FL DOR NAL (raw landed, never materialized) |
| bridges | DERIVED | 483.9 MiB | 10 | 2026-05-08→2026-05-10 | cross-source bridges |
| google-maps | GAP | 382.3 MiB | 13 | 2026-05-07→2026-05-07 | Google Maps places |
| bls-oews | GAP | 371.9 MiB | 70 | 2026-05-08→2026-05-08 | BLS OEWS wage stats |
| sec-adv | COVERED | 359.3 MiB | 178 | 2026-05-07→2026-05-07 | sec_adv_part1,sec_adv_w |
| fmcsa-carrier-essentials | DERIVED | 342.6 MiB | 1 | 2026-05-10→2026-05-10 | from fmcsa |
| ncua | GAP | 317.3 MiB | 868 | 2026-05-07→2026-05-12 | NCUA credit unions |
| ucc | COVERED | 312.3 MiB | 7 | 2026-05-08→2026-05-08 | co_ucc_transactions,ucc_co_debtors |
| nyc-dob-now | GAP | 311.3 MiB | 4 | 2026-05-08→2026-05-08 | NYC DOB NOW |
| cms-pecos | COVERED | 262.0 MiB | 5 | 2026-05-08→2026-05-08 | cms_provider_enrollment |
| ae-jobs | GAP | 224.3 MiB | 1 | 2026-05-19→2026-05-19 | Active-Outreach / Clay people enrichment |
| sec-bdc | GAP | 224.1 MiB | 230 | 2026-05-20→2026-05-22 | SEC BDC / SOI (business development cos) |
| sos-ny | COVERED | 196.2 MiB | 1 | 2026-05-25→2026-05-25 | ny_sos |
| gleif | COVERED | 193.0 MiB | 2 | 2026-05-08→2026-05-08 | gleif_l1_entities,gleif_l2_relationships |
| sos-co | COVERED | 188.6 MiB | 2 | 2026-05-21→2026-05-21 | co_sos |
| insurance-producers | GAP | 173.2 MiB | 5 | 2026-05-08→2026-05-08 | NIPR/state insurance producers |
| openfda | GAP | 141.4 MiB | 3 | 2026-05-20→2026-05-20 | openFDA |
| fdic | GAP | 138.2 MiB | 44 | 2026-05-08→2026-05-15 | FDIC institutions |
| irs-bmf | GAP | 133.3 MiB | 1 | 2026-05-08→2026-05-08 | IRS Business Master File |
| nyc-property | GAP | 123.0 MiB | 4 | 2026-05-08→2026-05-08 | NYC property |
| clinicaltrials-gov | GAP | 116.6 MiB | 2 | 2026-05-20→2026-05-25 | ClinicalTrials.gov |
| chicago-permit-contractors | GAP | 96.0 MiB | 1 | 2026-05-29→2026-05-29 | Chicago permit contractors |
| epa-npdes-cgp | COVERED | 74.1 MiB | 4 | 2026-05-08→2026-05-08 | epa_npdes_dmrs,epa_permits |
| grants-gov | GAP | 71.8 MiB | 2 | 2026-05-22→2026-05-22 | Grants.gov |
| fl-cilb | GAP | 61.7 MiB | 7 | 2026-05-18→2026-05-30 | FL CILB construction licensing |
| warn | GAP | 44.2 MiB | 10 | 2026-05-20→2026-05-30 | WARN layoff notices |
| tx-tdlr | GAP | 32.6 MiB | 1 | 2026-05-29→2026-05-29 | TX TDLR licensing |
| txstate | GAP | 32.1 MiB | 1 | 2026-05-18→2026-05-18 | TX state procurement |
| ny-data-construction | GAP | 29.2 MiB | 5 | 2026-05-18→2026-05-25 | NY construction |
| cal-eprocure-archived | GAP | 21.0 MiB | 2 | 2026-05-19→2026-05-19 | CA eProcure (archived) |
| hud-multifamily | GAP | 19.6 MiB | 4 | 2026-05-08→2026-05-08 | HUD multifamily |
| sbir | GAP | 18.4 MiB | 1 | 2026-05-10→2026-05-10 | SBIR/STTR awards |
| nyc-contract-awards | GAP | 14.3 MiB | 2 | 2026-05-18→2026-05-25 | NYC contract awards |
| cslb | COVERED | 14.2 MiB | 1 | 2026-05-17→2026-05-17 | cslb_licenses |
| usda-rd | GAP | 13.7 MiB | 26 | 2026-05-08→2026-05-08 | USDA Rural Development |
| ny-mta-procurements | GAP | 11.4 MiB | 2 | 2026-05-18→2026-05-25 | NY MTA procurement |
| audience-cohort-manifests | DERIVED | 9.4 MiB | 4 | 2026-05-12→2026-05-12 | GTM cohort manifests (app state) |
| wa-lni-contractors | GAP | 9.1 MiB | 1 | 2026-05-29→2026-05-29 | WA L&I contractors |
| podcasts | GAP | 7.4 MiB | 12 | 2026-05-10→2026-05-10 | podcast index |
| sfnet-members | GAP | 7.0 MiB | 6 | 2026-05-12→2026-05-13 | SFNet members |
| jsearch | GAP | 5.6 MiB | 1 | 2026-05-16→2026-05-16 | JSearch jobs |
| dfpi | GAP | 5.4 MiB | 3 | 2026-05-08→2026-05-08 | CA DFPI |
| ny-local-authority-procurements | GAP | 4.8 MiB | 2 | 2026-05-18→2026-05-25 | NY local authority procurement |
| fl-flr | COVERED | 3.8 MiB | 8 | 2026-05-21→2026-05-21 | fl_federal_tax_liens |
| or-ccb | GAP | 2.8 MiB | 1 | 2026-05-29→2026-05-29 | OR Construction Contractors Board |
| az-roc | GAP | 2.6 MiB | 1 | 2026-05-29→2026-05-29 | AZ Registrar of Contractors |
| chicago-home-repair | GAP | 1.7 MiB | 1 | 2026-05-29→2026-05-29 | Chicago home-repair licensing |
| iceberg-warehouse | DERIVED | 1.6 MiB | 88 | 2026-05-11→2026-05-11 | dead Iceberg |
| caltrans | GAP | 1.6 MiB | 13 | 2026-05-18→2026-05-30 | Caltrans |
| nyc-dcwp-home-improvement | GAP | 1.2 MiB | 1 | 2026-05-29→2026-05-29 | NYC DCWP home improvement |
| il-idfpr-roofing | GAP | 1.1 MiB | 1 | 2026-05-29→2026-05-29 | IL IDFPR roofing |
| opsc | GAP | 831.3 KiB | 2 | 2026-05-19→2026-05-25 | CA OPSC school facilities |
| franchisors | GAP | 563.4 KiB | 1 | 2026-05-10→2026-05-10 | franchisor registry |
| ncua-officers | GAP | 481.4 KiB | 1 | 2026-05-09→2026-05-09 | NCUA officers |
| iceberg-test | DERIVED | 425.2 KiB | 16 | 2026-05-11→2026-05-11 | dead Iceberg test |
| azstate | GAP | 200.4 KiB | 11 | 2026-05-19→2026-05-30 | AZ state RFP/procurement |
| glassdoor | GAP | 157.5 KiB | 4 | 2026-05-16→2026-05-16 | Glassdoor |
| fdot | GAP | 139.9 KiB | 1 | 2026-05-19→2026-05-19 | FL DOT |
| shovels | COVERED | 106.8 KiB | 7 | 2026-05-29→2026-05-29 | shovels_tags |
| elfa-fundingsource | GAP | 40.3 KiB | 1 | 2026-05-18→2026-05-18 | ELFA funding-source list |
| sba-sbic-directory | GAP | 33.2 KiB | 1 | 2026-05-18→2026-05-18 | SBA SBIC directory |
| castate | COVERED | 28.0 KiB | 1 | 2026-05-19→2026-05-19 | ca_sos_entities |
| blitz_contacts | DERIVED | 16.3 KiB | 1 | 2026-06-05→2026-06-05 | Blitz contact transport (un-promoted) |

## 3. DEX Lance warehouse catalog (`polaris-warehouse/<ns>/`)

| Namespace | Status | Size | Objs | Newest | Gen-3 |
|---|---|---:|---:|---|---|
| usaspending | COVERED | 691.6 GiB | 5042 | 2026-05-30 | usaspending |
| spines | MIXED | 287.0 GiB | 2539 | 2026-06-02 | see datasets |
| cms_open_payments | COVERED | 169.1 GiB | 196 | 2026-05-30 | cms_*_payments |
| bridges | DERIVED | 50.8 GiB | 2725 | 2026-05-30 | cross-source bridges |
| sos | COVERED | 26.1 GiB | 8599 | 2026-05-27 | *_sos |
| fmcsa | COVERED | 21.9 GiB | 570 | 2026-05-30 | fmcsa |
| sam_gov | COVERED | 18.2 GiB | 145 | 2026-05-30 | entity_registrations/sam_* |
| sec_dera | GAP | 12.4 GiB | 306 | 2026-05-18 | SEC DERA financial-statement datasets |
| sec_adv | COVERED | 8.3 GiB | 233 | 2026-05-21 | sec_adv_* |
| epiq | COVERED | 7.5 GiB | 274 | 2026-05-29 | epiq_* |
| uspto | COVERED | 7.5 GiB | 73 | 2026-05-13 | uspto_tm_* (trademark) |
| sba | COVERED | 4.4 GiB | 102 | 2026-05-28 | ppp/sba_* |
| nppes | COVERED | 4.4 GiB | 25 | 2026-05-29 | nppes_* |
| pdl | COVERED | 4.2 GiB | 39 | 2026-05-26 | pdl_* |
| openfda | GAP | 2.9 GiB | 264 | 2026-05-25 | openFDA |
| overture | COVERED | 2.8 GiB | 22 | 2026-05-27 | overture_places |
| sec_edgar | COVERED | 2.0 GiB | 33 | 2026-05-13 | edgar_* |
| clinicaltrials | GAP | 1.6 GiB | 148 | 2026-05-25 | ClinicalTrials.gov |
| gleif | COVERED | 1.4 GiB | 41 | 2026-05-17 | gleif_* |
| ucc_ca | COVERED | 1.2 GiB | 92 | 2026-05-27 | ca_ucc |
| sec_bdc | GAP | 1.2 GiB | 183 | 2026-05-21 | SEC BDC / SOI (business development cos) |
| ucc_co | COVERED | 831.2 MiB | 52 | 2026-05-27 | co_ucc_* |
| ae_jobs | GAP | 466.0 MiB | 23 | 2026-05-19 | Active-Outreach / Clay people enrichment |
| warn | GAP | 264.9 MiB | 288 | 2026-05-30 | WARN notices |
| chicago | GAP | 251.6 MiB | 16 | 2026-05-29 | Chicago licensing |
| licensure | ? | 219.6 MiB | 124 | 2026-05-29 | unmapped |
| borrowers | ? | 162.0 MiB | 14 | 2026-05-13 | unmapped |
| castate | ? | 148.1 MiB | 199 | 2026-05-25 | unmapped |
| cslb | ? | 121.2 MiB | 29 | 2026-05-17 | unmapped |
| txstate | ? | 106.8 MiB | 15 | 2026-05-18 | unmapped |
| grants_gov | ? | 103.8 MiB | 14 | 2026-05-22 | unmapped |
| nystate | ? | 91.5 MiB | 44 | 2026-05-18 | unmapped |
| sbir | ? | 52.7 MiB | 23 | 2026-05-27 | unmapped |
| cohorts | ? | 33.9 MiB | 537 | 2026-05-30 | unmapped |
| nyc | ? | 21.1 MiB | 11 | 2026-05-18 | unmapped |
| jsearch | GAP | 21.1 MiB | 54 | 2026-05-16 | JSearch jobs |
| lookup | ? | 9.8 MiB | 83 | 2026-05-25 | unmapped |
| caltrans | ? | 4.9 MiB | 136 | 2026-05-30 | unmapped |
| fdic | ? | 1.3 MiB | 7 | 2026-05-12 | unmapped |
| azstate | GAP | 678.6 KiB | 114 | 2026-05-30 | AZ state RFP |
| fdot | ? | 642.4 KiB | 23 | 2026-05-19 | unmapped |
| glassdoor | GAP | 446.0 KiB | 64 | 2026-05-16 | Glassdoor |
| shovels | ? | 298.9 KiB | 70 | 2026-05-29 | unmapped |
| clay | GAP | 155.6 KiB | 46 | 2026-05-16 | Clay people enrichment |
| _polaris_smoke_test | ? | 32.7 KiB | 21 | 2026-05-12 | unmapped |
| ncua | ? | 18.1 KiB | 7 | 2026-05-12 | unmapped |

### 3a. DEX warehouse — all datasets ≥0.25 GiB (root = ns/dataset)

| Dataset | Size | Objs | Newest |
|---|---:|---:|---|
| spines/fec_individual_contributions_lance | 264.7 GiB | 1552 | 2026-05-29 |
| usaspending/awards_lance | 219.4 GiB | 340 | 2026-05-22 |
| usaspending/transaction_fpds_lance | 188.4 GiB | 158 | 2026-05-22 |
| cms_open_payments/general_payments_lance | 151.9 GiB | 147 | 2026-05-30 |
| usaspending/transaction_fabs_lance | 141.3 GiB | 245 | 2026-05-22 |
| usaspending/winners_recent_lance | 79.2 GiB | 3614 | 2026-05-30 |
| usaspending/contracts_lance | 51.2 GiB | 55 | 2026-05-16 |
| bridges/fec_sba_employer_lance | 27.7 GiB | 708 | 2026-05-15 |
| cms_open_payments/research_payments_lance | 17.3 GiB | 49 | 2026-05-30 |
| sos/ny_active_corporations_lance | 12.4 GiB | 8354 | 2026-05-25 |
| sam_gov/entities_longitudinal_v2_lance | 11.0 GiB | 30 | 2026-05-22 |
| fmcsa/carrier_essentials_lance | 10.9 GiB | 110 | 2026-05-30 |
| sec_dera/fsds_num_lance | 9.1 GiB | 183 | 2026-05-18 |
| spines/sam_usaspending_capital_matrix_lance | 7.4 GiB | 86 | 2026-05-25 |
| sec_adv/part_2_brochures_lance | 6.9 GiB | 164 | 2026-05-21 |
| usaspending/subaward_lance | 6.8 GiB | 32 | 2026-05-22 |
| bridges/sba_overture_address_lance | 4.9 GiB | 64 | 2026-05-28 |
| nppes/npidata_lance | 4.2 GiB | 3 | 2026-05-29 |
| uspto/case_file_lance | 4.2 GiB | 18 | 2026-05-13 |
| pdl/free_companies_lance | 4.2 GiB | 39 | 2026-05-26 |
| sam_gov/entities_longitudinal_pre_v2_lance | 4.0 GiB | 18 | 2026-05-22 |
| fmcsa/inspections_recent_lance | 3.6 GiB | 98 | 2026-05-30 |
| epiq/claims_resolved_lance | 3.2 GiB | 83 | 2026-05-29 |
| sos/fl_officers_lance | 3.1 GiB | 34 | 2026-05-16 |
| epiq/claims_lance | 3.0 GiB | 47 | 2026-05-29 |
| usaspending/recipient_lookup_lance | 3.0 GiB | 32 | 2026-05-22 |
| overture/us_places_lance | 2.8 GiB | 22 | 2026-05-27 |
| bridges/ppp_overture_address_lance | 2.8 GiB | 35 | 2026-05-27 |
| spines/pdl_b2b_firmographics_lance | 2.8 GiB | 42 | 2026-05-25 |
| fmcsa/crash_essentials_lance | 2.5 GiB | 66 | 2026-05-30 |
| openfda/device_510k_lance | 2.5 GiB | 144 | 2026-05-25 |
| bridges/sba_overture_uspto_lance | 2.5 GiB | 61 | 2026-05-28 |
| sos/fl_entities_lance | 2.4 GiB | 23 | 2026-05-16 |
| uspto/case_file_owner_lance | 2.4 GiB | 37 | 2026-05-13 |
| sos/ca_principals_lance | 2.4 GiB | 48 | 2026-05-27 |
| fmcsa/insurance_history_lance | 2.2 GiB | 84 | 2026-05-30 |
| spines/federal_procurement_fleet_lance | 2.2 GiB | 68 | 2026-05-25 |
| usaspending/recipient_profile_lance | 2.1 GiB | 34 | 2026-05-22 |
| sec_dera/fsds_pre_lance | 2.1 GiB | 49 | 2026-05-18 |
| sos/ca_entities_lance | 2.0 GiB | 30 | 2026-05-27 |
| sba/borrowers_lance | 2.0 GiB | 36 | 2026-05-28 |
| sam_gov/entities_lance | 2.0 GiB | 31 | 2026-05-27 |
| sec_edgar/form_10k_lance | 2.0 GiB | 26 | 2026-05-13 |
| bridges/ppp_overture_uspto_lance | 1.9 GiB | 34 | 2026-05-28 |
| clinicaltrials/device_studies_lance | 1.6 GiB | 148 | 2026-05-25 |
| sos/fl_events_lance | 1.4 GiB | 24 | 2026-05-16 |
| spines/fec_individual_contributions_modern_lance | 1.3 GiB | 122 | 2026-05-25 |
| sba/loans_lance | 1.3 GiB | 20 | 2026-05-12 |
| sos/co_entities_lance | 1.3 GiB | 26 | 2026-05-21 |
| fmcsa/authhist_essentials_lance | 1.2 GiB | 77 | 2026-05-30 |
| bridges/contracts_with_subawards_lance | 1.2 GiB | 27 | 2026-05-16 |
| sec_bdc/soi_lance | 1.2 GiB | 183 | 2026-05-21 |
| epiq/dockets_lance | 1.1 GiB | 30 | 2026-05-28 |
| sos/ca_agents_lance | 1002.3 MiB | 30 | 2026-05-27 |
| bridges/epiq_claim_uspto_owner_lance | 1000.6 MiB | 98 | 2026-05-30 |
| spines/usaspending_sub_awards_lance | 996.5 MiB | 44 | 2026-05-25 |
| uspto/correspondent_domrep_attorney_lance | 924.2 MiB | 18 | 2026-05-13 |
| bridges/sos_ca_overture_address_lance | 917.7 MiB | 20 | 2026-05-27 |
| sam_gov/opps_active_lance | 886.5 MiB | 49 | 2026-05-30 |
| gleif/lei_records_lance | 857.4 MiB | 20 | 2026-05-17 |
| sba/ppp_borrowers_lance | 828.9 MiB | 16 | 2026-05-27 |
| bridges/sos_ny_overture_address_lance | 802.0 MiB | 18 | 2026-05-27 |
| spines/sba_ppp_borrower_registry | 763.7 MiB | 20 | 2026-05-25 |
| spines/sos_florida_entities_lance | 729.6 MiB | 27 | 2026-05-25 |
| spines/global_corporate_hierarchies_lance | 728.5 MiB | 22 | 2026-05-25 |
| fmcsa/carrier_essentials_embeddings_lance | 715.2 MiB | 51 | 2026-05-12 |
| spines/uspto_patent_lance | 708.3 MiB | 24 | 2026-05-25 |
| bridges/sos_fl_overture_address_lance | 698.0 MiB | 19 | 2026-05-27 |
| spines/sos_new_york_entities_lance | 691.1 MiB | 55 | 2026-05-25 |
| bridges/ucc_ca_debtor_sos_ca_owner_lance | 688.9 MiB | 21 | 2026-05-27 |
| spines/usaspending_new_awards_lance | 677.3 MiB | 26 | 2026-05-24 |
| sec_dera/fsds_tag_lance | 652.7 MiB | 11 | 2026-05-18 |
| bridges/sba_uspto_owner_lance | 651.9 MiB | 22 | 2026-05-27 |
| fmcsa/safety_basics_lance | 615.2 MiB | 42 | 2026-05-30 |
| spines/sos_california_entities_lance | 612.4 MiB | 23 | 2026-05-25 |
| bridges/sam_overture_address_lance | 557.7 MiB | 17 | 2026-05-27 |
| gleif/lei_with_parent_lance | 550.3 MiB | 14 | 2026-05-12 |
| bridges/ppp_uspto_owner_lance | 548.2 MiB | 21 | 2026-05-27 |
| bridges/sam_ppp_address_lance | 547.2 MiB | 18 | 2026-05-27 |
| ucc_ca/debtors_lance | 541.8 MiB | 16 | 2026-05-27 |
| sec_adv/base_a_lance | 536.9 MiB | 7 | 2026-05-18 |
| spines/ca_corporate_credit_lance | 486.8 MiB | 22 | 2026-05-25 |
| ae_jobs/jobs_lance | 466.0 MiB | 23 | 2026-05-19 |
| ucc_co/collateral_lance | 451.0 MiB | 12 | 2026-05-15 |
| spines/sam_recipients_lance | 450.5 MiB | 54 | 2026-05-27 |
| bridges/overture_pdl_domain_lance | 430.7 MiB | 17 | 2026-05-27 |
| sec_adv/schedule_a_b_lance | 414.1 MiB | 10 | 2026-05-18 |
| spines/sba_ppp_natural_persons | 392.9 MiB | 15 | 2026-05-25 |
| openfda/device_pma_lance | 383.0 MiB | 75 | 2026-05-25 |
| bridges/sos_co_overture_address_lance | 374.7 MiB | 16 | 2026-05-27 |
| ucc_ca/secured_parties_lance | 297.5 MiB | 14 | 2026-05-27 |
| sam_gov/entity_pocs_lance | 294.1 MiB | 17 | 2026-05-16 |
| ucc_ca/filings_lance | 292.3 MiB | 10 | 2026-05-12 |
| bridges/sba_sos_ca_owner_lance | 268.8 MiB | 8 | 2026-05-16 |
| sba/7a_loans_essentials_lance | 266.4 MiB | 8 | 2026-05-14 |
| warn/notices_lance | 264.9 MiB | 288 | 2026-05-30 |
| spines/federal_contractor_profile_lance | 261.9 MiB | 28 | 2026-05-25 |

## 4. Coverage method & caveats
- **Source of truth:** live `rclone` recursive listings of both buckets, 2026-06-07.
  Full object manifest archived to `s3://data-sink/archive/dex-raw-landing-zone-manifest-20260607.txt.gz`.
- **Coverage = source-level**, not table-level. A source is COVERED if *any* Gen-3
  `data-sink/active/` dataset carries that upstream feed. Derived/granular tables in dex
  (e.g. `usaspending/transaction_fpds_lance`) roll up to their source.
- **DERIVED** rows (bridges, `*-derived`, cohort manifests, dead Iceberg) are
  re-computable from a covered source — **not** re-acquisition targets.
- **Crosswalk is name-based** for the long tail. Confirm the ~50 GAP rows against
  upstream before treating any as authoritative "missing."
- **`uspto`**: trademarks (`case_file*`) ARE covered (`uspto_tm_*`); **patents are NOT**.

## 5. Deletion readiness & blast radius
**`dex-db` (Gen-2 Postgres) is OUT of scope** — not an R2 object; untouched.

Deleting the R2 bucket `dex-raw-landing-zone` is **irreversible** and severs a **still-live
service**. The `polaris-warehouse/` tier (1.3 TiB) is the active Lance warehouse for
**`data-engine-x` (DEX)** — newest write 2026-06-02. Code bound to it (will error on read/write
once the bucket is gone):
- `data-engine-x/app/services/lance_views.py` — GTM view materialize (writer)
- `data-engine-x/app/services/lance_cohort_exec.py` — cohort exec (writer)
- `data-engine-x/mcp/polaris_server.py` — Polaris catalog/query MCP (reader)
- `data-engine-x/risingwave/*` — RisingWave streaming sources
- `core-x` defaults: `pipelines/gtm/blitz_hydration_waterfall.py:82`, `pipelines/co_ucc/companions_bulk.py:70`

**Before purge:** confirm DEX/GTM-views + cohort features are decommissioned or repointed
(the firmographics/blitz spine is already rehydrated to Gen-3 `firmographics_blitz`).
Once confirmed, the data itself is disposable per directive — junk parquet fragments + a
re-derivable Lance warehouse. **The 50-row GAP list above is the only thing to capture first.**

*Generated 2026-06-07. Read-only catalog; no objects modified.*
