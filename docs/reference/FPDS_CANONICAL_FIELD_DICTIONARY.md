# FPDS Canonical Transaction Table — Field Dictionary & Reference

> **AUTO-GENERATED — do not hand-edit.** This file is a projection of `COLUMN_SPEC` in [`pipelines/usaspending/usaspending_fpds_canonical.py`](../../pipelines/usaspending/usaspending_fpds_canonical.py), cross-checked fail-closed against the live R2 dataset. To change it, change the pipeline contract and regenerate (see [§7](#7-regenerating-this-document)). Definitions are joined from the committed sidecar `fpds_field_definitions.json` (USAspending Data Dictionary) and may be length-capped verbatim from source.

**Live anchor:** `s3://data-sink/active/usaspending_fpds_canonical_txn/` — **108,181,354 rows · 131 cols · 17 indices** · manifest v18. Verified live: 2026-07-02.

---

## 1. What this table is

The typed, PK-grained **system-of-record read model** for U.S. federal contract (FPDS) prime-award **transactions**. One row = one contract transaction, uniquely keyed. It is the reconciliation of three upstream FPDS feeds into a single canonical vocabulary (the FPDS `bulk_download`/`awards` field names), stored append-only in Lance on R2.

| property | value |
|---|---|
| URI | `s3://data-sink/active/usaspending_fpds_canonical_txn/` |
| rows | 108,181,354 |
| columns | 131 |
| indices | 17 (10 BTREE + 7 BITMAP) |
| grain / PK | `contract_transaction_unique_key` (exactly one surviving row per key — structural uniqueness gate at build) |
| storage | Lance `data_storage_version=2.1`, `max_rows_per_file=1,048,576`, written LOCAL then boto3 uniform-part published to R2 (never a direct-R2 writer — Giants `400 InvalidPart`) |
| ops ledger | `ops.usaspending_fpds_canonical_runs` (Postgres; one row per build) |

### Upstream feeds (reconciled into this table)

| feed | source dataset | ~rows | role |
|---|---|---|---|
| **BULK** | `s3://data-sink/active/usaspending/transaction_search_fpds/` | 107.25M | pg-derived; 378 typed `rpt.*` cols; the enrichment source |
| **FRESH** | `s3://data-sink/active/usaspending_api_fresh/contract_prime_txn/` | 1.99M | USAspending API; canonical vocabulary; newest corrections |
| **MONTHLY** | `s3://data-sink/active/usaspending_archive_full_fpds/` | 2.98M | monthly bulk-download CSV (physical name `archive_full` — a tracked rename); carries 12 monthly-unique enrichment cols |
| **DELTA** | `s3://data-sink/active/usaspending_archive_delta_fpds/` | — | deletion ledger (`correction_delete_ind='D'`); drives R6 tombstones |

## 2. How to read / query it

Open with the R2 storage options and hand the scanner to DuckDB (out-of-core). The table is append-only and PK-unique, so no dedup is needed on read.

```python
import os, lance
os.environ.setdefault("LANCE_BYPASS_SPILLING", "true")  # before any lance call
so = {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
      "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
      "endpoint": os.environ["R2_ENDPOINT"], "region": "auto"}
ds = lance.dataset("s3://data-sink/active/usaspending_fpds_canonical_txn/", storage_options=so)
# pushdown filters on the 17 indexed columns are cheap; scans on the rest are full
```

**Indexed columns accelerate pushdown** (see [§4](#4-index-topology-17)). Point-lookups and range scans on the BTREE columns and equality/`IN` filters on the BITMAP columns are the fast paths; everything else is a full column scan of 108M rows.

## 3. Column reference (131)

Grouped by role. `source` shows the native upstream column(s) each canonical column projects from — `BULK` = pg `rpt.*` vocabulary, `FRESH/MO` = canonical vocabulary carried verbatim by the FRESH + monthly feeds. Enrichment columns are BULK-sourced except the 12 monthly-unique ones. `type` is the live Arrow type.

### 3.1 Primary keys  (2)

| # | canonical column | type | source | definition | domain / codes |
|---|---|---|---|---|---|
| 1 | `contract_transaction_unique_key` | `string` | BULK `detached_award_proc_unique`, `transaction_unique_id` · FRESH/MO `contract_transaction_unique_key` | Derived element and system-generated database key used to uniquely identify each contract transaction record and facilitate record lookup, correction, and deletion. A concatenation of agencyID, Refere | — |
| 2 | `contract_award_unique_key` | `string` | BULK `generated_unique_award_id` · FRESH/MO `contract_award_unique_key` | Derived unique record key used by the Broker to identify the prime award. Note that this element is different from the AssistanceTransactionUniqueKey and the ContractTransactionUniqueKey in that it id | — |

### 3.2 Volatile core — source-reconciled per key (argmax `last_modified_date`)  (88)

| # | canonical column | type | source | definition | domain / codes |
|---|---|---|---|---|---|
| 1 | `action_date` | `date32[day]` | `action_date` (BULK+FRESH) | The date the action being reported was issued / signed by the Government or a binding agreement was reached. | — |
| 2 | `last_modified_date` | `timestamp[us]` | `last_modified_date` (BULK+FRESH) | The last modified date captures the change date. | — |
| 3 | `period_of_performance_start_date` | `date32[day]` | `period_of_performance_start_date` (BULK+FRESH) | For procurement awards: Per the FPDS data dictionary, the date that the parties agree will be the starting date for the contract's requirements. This is the period of performance start date for the en | — |
| 4 | `period_of_performance_current_end_date` | `date32[day]` | `period_of_performance_current_end_date` (BULK+FRESH) | For procurement awards: The contract completion date based on the schedule in the contract. For an initial award, this is the scheduled completion date for the base contract and for any options exerci | — |
| 5 | `federal_action_obligation` | `double` | `federal_action_obligation` (BULK+FRESH) | Amount of Federal government’s obligation, de-obligation, or liability, in dollars, for an award transaction. | — |
| 6 | `base_and_all_options_value` | `double` | `base_and_all_options_value` (BULK+FRESH) | For the Award it is the mutually agreed upon total contract value including all options (if any). For IDVs the value is the mutually agreed upon total contract value including all options (if any) AND | — |
| 7 | `current_total_value_of_award` | `double` | BULK `current_total_value_award` · FRESH/MO `current_total_value_of_award` | For procurement, the total amount obligated to date on a contract, including the base and exercised options. | — |
| 8 | `modification_number` | `string` | `modification_number` (BULK+FRESH) | The identifier of an action being reported that indicates the specific subsequent change to the initial award. | — |
| 9 | `award_id_piid` | `string` | BULK `piid` · FRESH/MO `award_id_piid` | The unique identifier of the specific award being reported. | — |
| 10 | `parent_award_id_piid` | `string` | BULK `parent_award_id` · FRESH/MO `parent_award_id_piid` | The identifier of the procurement award under which the specific award is issued, such as a Federal Supply Schedule. This data element currently applies to procurement actions only. | — |
| 11 | `recipient_uei` | `string` | `recipient_uei` (BULK+FRESH) | The Unique Entity Identifier (UEI) for an awardee or recipient. A UEI is a unique alphanumeric code used to identify a specific commercial, nonprofit, or business entity. | — |
| 12 | `recipient_name` | `string` | `recipient_name` (BULK+FRESH) | The name of the awardee or recipient that relates to the unique identifier. For U.S. based companies, this name is what the business ordinarily files in formation documents with individual states (whe | — |
| 13 | `cage_code` | `string` | `cage_code` (BULK+FRESH) | The CAGE Code of the entity. Used as a key to SAM. Maps to the Unique Entity ID. | — |
| 14 | `recipient_address_line_1` | `string` | BULK `legal_entity_address_line1` · FRESH/MO `recipient_address_line_1` | First line of the awardee or recipient’s legal business address where the office represented by the Unique Entity Identifier (as registered in the System for Award Management) is located. | — |
| 15 | `recipient_city_name` | `string` | BULK `recipient_location_city_name` · FRESH/MO `recipient_city_name` | Name of the city in which the awardee or recipient’s legal business address is located. | — |
| 16 | `recipient_county_name` | `string` | BULK `recipient_location_county_name` · FRESH/MO `recipient_county_name` | Name of the county in which the awardee or recipient’s legal business address is located. | — |
| 17 | `recipient_state_code` | `string` | BULK `recipient_location_state_code` · FRESH/MO `recipient_state_code` | United States Postal Service (USPS) two-letter abbreviation for the state or territory in which the awardee or recipient’s legal business address is located. Identify States, the District of Columbia, | — |
| 18 | `recipient_zip_4_code` | `string` | FRESH/MO-only `recipient_zip_4_code` | — | — |
| 19 | `recipient_country_code` | `string` | BULK `recipient_location_country_code` · FRESH/MO `recipient_country_code` | Code for the country in which the awardee or recipient is located, using the International Standard for country codes (ISO) 3166-1 Alpha-3 GENC Profile, minus the codes listed for those territories an | — |
| 20 | `naics_code` | `string` | `naics_code` (BULK+FRESH) | The identifier that represents the North American Industrial Classification System (NAICS) Code assigned to the solicitation and resulting award identifying the industry in which the contract requirem | — |
| 21 | `naics_description` | `string` | `naics_description` (BULK+FRESH) | The title associated with the NAICS Code. | — |
| 22 | `product_or_service_code` | `string` | `product_or_service_code` (BULK+FRESH) | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Product or Service Code Field. | — |
| 23 | `product_or_service_code_description` | `string` | BULK `product_or_service_description` · FRESH/MO `product_or_service_code_description` | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Product or Service Code Field. | — |
| 24 | `type_of_set_aside_code` | `string` | BULK `type_set_aside` · FRESH/MO `type_of_set_aside_code` | The designator for type of set aside determined for the contract action. | — |
| 25 | `extent_competed` | `string` | `extent_competed` (BULK+FRESH) | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Extent Competed Field. | — |
| 26 | `type_of_contract_pricing_code` | `string` | BULK `type_of_contract_pricing` · FRESH/MO `type_of_contract_pricing_code` | The type of contract as defined in FAR Part 16 that applies to this procurement. | — |
| 27 | `award_type_code` | `string` | BULK `contract_award_type` · FRESH/MO `award_type_code` | The type of award being entered by this transaction. Types of awards include Purchase Orders (PO), Delivery Orders (DO), Blanket Purchase Agreements (BPA) Calls and Definitive Contracts. | — |
| 28 | `idv_type_code` | `string` | BULK `idv_type` · FRESH/MO `idv_type_code` | The type of Indefinite Delivery Vehicle being (IDV) loaded by this transaction. IDV Types include Government-Wide Acquisition Contract (GWAC), Multi-Agency Contract, Other Indefinite Delivery Contract | — |
| 29 | `subcontracting_plan` | `string` | BULK `subcontracting_plan` · FRESH/MO `subcontracting_plan_code` | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Subcontracting Plan Field. | — |
| 30 | `construction_wage_rate_requirements_code` | `string` | BULK `construction_wage_rate_req` · FRESH/MO `construction_wage_rate_requirements_code` | Indicates whether the transaction is subject to the Construction Wage Rate Requirements. The clause is 52.222-6 "Construction Wage Rate Requirements" -that goes with Wage Rate Requirements (Constructi | — |
| 31 | `labor_standards_code` | `string` | BULK `labor_standards` · FRESH/MO `labor_standards_code` | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Service Contract Labor Standards Field. | — |
| 32 | `awarding_agency_code` | `string` | `awarding_agency_code` (BULK+FRESH) | A department or establishment of the Government as used in the Treasury Account Fund Symbol (TAFS). | — |
| 33 | `awarding_agency_name` | `string` | BULK `awarding_toptier_agency_name` · FRESH/MO `awarding_agency_name` | The name associated with a department or establishment of the Government as used in the Treasury Account Fund Symbol (TAFS). | — |
| 34 | `awarding_sub_agency_code` | `string` | BULK `awarding_sub_tier_agency_c` · FRESH/MO `awarding_sub_agency_code` | Identifier of the level 2 organization that awarded, executed or is otherwise responsible for the transaction. | — |
| 35 | `awarding_sub_agency_name` | `string` | BULK `awarding_subtier_agency_name` · FRESH/MO `awarding_sub_agency_name` | Name of the level 2 organization that awarded, executed or is otherwise responsible for the transaction. | — |
| 36 | `funding_agency_code` | `string` | `funding_agency_code` (BULK+FRESH) | The 3-digit CGAC agency code of the department or establishment of the Government that provided the preponderance of the funds for an award and/or individual transactions related to an award. | — |
| 37 | `funding_agency_name` | `string` | BULK `funding_toptier_agency_name` · FRESH/MO `funding_agency_name` | Name of the department or establishment of the Government that provided the preponderance of the funds for an award and/or individual transactions related to an award. | — |
| 38 | `primary_place_of_performance_state_code` | `string` | BULK `pop_state_code` · FRESH/MO `primary_place_of_performance_state_code` | United States Postal Service (USPS) two-letter abbreviation for the state or territory indicating where the predominant performance of the award will be accomplished. Identify States, the District of | — |
| 39 | `primary_place_of_performance_country_code` | `string` | BULK `pop_country_code` · FRESH/MO `primary_place_of_performance_country_code` | Country code where the predominant performance of the award will be accomplished. | — |
| 40 | `primary_place_of_performance_zip_4` | `string` | BULK `pop_zip5` · FRESH/MO `primary_place_of_performance_zip_4` | United States ZIP code (five digits) concatenated with the additional +4 digits, identifying where the predominant performance of the award will be accomplished. | — |
| 41 | `contracting_officers_determination_of_business_size` | `string` | BULK `contracting_officers_deter` · FRESH/MO `contracting_officers_determination_of_business_size` | The Contracting Officer's determination of whether the selected contractor meets the small business size standard for award to a small business for the NAICS code that is applicable to the contract. | — |
| 42 | `action_type_code` | `string` | BULK `action_type` · FRESH/MO `action_type_code` | Code that provides information on any new (only applicable to financial assistance awards) or changes (applies to both procurement and financial assistance changes) made to the Federal prime award. Th | — |
| 43 | `transaction_description` | `string` | `transaction_description` (BULK+FRESH) | For procurement awards: Per the FPDS data dictionary, a brief, summary level, plain English, description of the contract, award, or modification. Additional information: the description field may also | — |
| 44 | `solicitation_identifier` | `string` | `solicitation_identifier` (BULK+FRESH) | Identifier used to link transactions in FPDS to solicitation information. | — |
| 45 | `action_date_fiscal_year` | `int64` | BULK `fiscal_year` · FRESH/MO `action_date_fiscal_year` | The fiscal year in which the ActionDate occurs. Note that the Federal fiscal year begins on October 1 and ends on September 30, thus October 1, 2018 is the first day of the 2019 fiscal year. | — |
| 46 | `women_owned_small_business` | `string` | `women_owned_small_business` (BULK+FRESH) *(first char)* | https://www.sam.gov OR List characteristic of the contractor such as whether the selected contractor is a Woman Owned Small Business or not. It can be derived from the SAM data element, 'Business Type | F = False T = True |
| 47 | `service_disabled_veteran_owned_business` | `string` | BULK `service_disabled_veteran_o` · FRESH/MO `service_disabled_veteran_owned_business` *(first char)* | List characteristic of the contractor such as whether the selected contractor is a Service-Related Disabled Veteran Owned Business or not. It can be derived from the SAM data element, 'Business Types' | F = False T = True |
| 48 | `historically_underutilized_business_zone_hubzone_firm` | `string` | BULK `historically_underutilized` · FRESH/MO `historically_underutilized_business_zone_hubzone_firm` *(first char)* | List characteristic of the contractor such as whether the selected contractor is a Historically Underutilized Business Zone (HUBZone) Firm or not. It can be derived from the SAM data element, 'Busines | — |
| 49 | `c8a_program_participant` | `string` | `c8a_program_participant` (BULK+FRESH) *(first char)* | List characteristic of the contractor such as whether the selected contractor is an 8(a) Program Participant Organization or not. It can be derived from the SAM data element, 'Business Types'. | — |
| 50 | `solicitation_procedures` | `string` | `solicitation_procedures` (BULK+FRESH) | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Solicitation Procedures Field. | NP = NEGOTIATED PROPOSAL/QUOTE SB = SEALED BID TS = TWO STEP SP1 = SIMPLIFIED AC |
| 51 | `other_than_full_and_open_competition_code` | `string` | BULK `other_than_full_and_open_c` · FRESH/MO `other_than_full_and_open_competition_code` | The designator for solicitation procedures other than full and open competition pursuant to FAR 6.3. | UNQ = UNIQUE SOURCE (FAR 6.302-1(B)(1)) FOC = FOLLOW-ON CONTRACT (FAR 6.302-1(A) |
| 52 | `fair_opportunity_limited_sources_code` | `string` | BULK `fair_opportunity_limited_s` · FRESH/MO `fair_opportunity_limited_sources_code` | The type of statutory exception to Fair Opportunity. | URG = URGENCY ONE = ONLY ONE SOURCE - OTHER FOO = FOLLOW-ON ACTION FOLLOWING COM |
| 53 | `commercial_item_acquisition_procedures_code` | `string` | BULK `commercial_item_acquisitio` · FRESH/MO `commercial_item_acquisition_procedures_code` | Designates whether the solicitation used the special requirements for the acquisition of commercial items (or other supplies or services authorized to use commercial item procedures) intended to more | A = COMMERCIAL ITEM B = SUPPLIES OR SERVICES PURSUANT TO FAR 12.102(F) C = SERVI |
| 54 | `multiple_or_single_award_idv_code` | `string` | BULK `multiple_or_single_award_i` · FRESH/MO `multiple_or_single_award_idv_code` | Indicates whether the contract is one of many that resulted from a single solicitation, all of the contracts are for the same or similar items, and contracting officers are required to compare their r | M = MULTIPLE AWARD S = SINGLE AWARD |
| 55 | `parent_award_agency_id` | `string` | BULK `referenced_idv_agency_iden` · FRESH/MO `parent_award_agency_id` | Identifier used to link agency in FPDS to referenced IDV information. | Refer to the GSA Federal Procurement Data System (FPDS) |
| 56 | `parent_award_type_code` | `string` | BULK `referenced_idv_type` · FRESH/MO `parent_award_type_code` | The type of Indefinite Delivery Vehicle (IDV) being loaded by the IDV referenced in this transaction. Referenced IDV Types include Government-Wide Acquisition Contract (GWAC), Multi-Agency Contract, O | A = GWAC B = IDC C = FSS D = BOA E = BPA |
| 57 | `parent_award_modification_number` | `string` | BULK `referenced_idv_modificatio` · FRESH/MO `parent_award_modification_number` | When reporting orders under Indefinite Delivery Vehicles (IDV) such as a GWAC, IDC, FSS, BOA, or BPA, report the Modification Number along with Procurement Instrument Identifier (Contract Number or Ag | — |
| 58 | `major_program` | `string` | `major_program` (BULK+FRESH) | The agency determined code for a major program within the agency. For an Indefinite Delivery Vehicle, this may be the name of a GWAC (e.g., ITOPS or COMMITS). | — |
| 59 | `program_acronym` | `string` | `program_acronym` (BULK+FRESH) | The short name or title used for a GWAC or other contracting program. Examples include COMMITS, ITOPS, SEWP. | — |
| 60 | `contract_bundling` | `string` | `contract_bundling` (BULK+FRESH) | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Contract Bundling Field. | A = MISSION CRITICAL B = OMB Circular A-76 C = OTHER D = NOT A BUNDLED REQUIREME |
| 61 | `consolidated_contract` | `string` | `consolidated_contract` (BULK+FRESH) | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Consolidated Contract Field. | A = CONSOLIDATED REQUIREMENTS B = CONSOLIDATED REQUIREMENTS WITH WRITTEN DETERMI |
| 62 | `performance_based_service_acquisition_code` | `string` | BULK `performance_based_service` · FRESH/MO `performance_based_service_acquisition_code` | Indicates whether the contract action is a PBA of services as defined by FAR 37.601. A PBSA: a. Describes the requirements in terms of results required rather than the methods of performance of the wo | Y = YES - SERVICE WHERE PBA IS USED. N = NO - SERVICE WHERE PBA IS NOT USED. X = |
| 63 | `undefinitized_action_code` | `string` | BULK `undefinitized_action` · FRESH/MO `undefinitized_action_code` | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Undefinitized Action Field. | A = LETTER CONTRACT B = OTHER UNDEFINITIZED ACTION X = NO |
| 64 | `multi_year_contract` | `string` | `multi_year_contract` (BULK+FRESH) | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Multi Year Contract Field. | Y = YES N = NO |
| 65 | `contract_financing` | `string` | `contract_financing` (BULK+FRESH) | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Contract Financing Field. | A = FAR 52.232-16 PROGRESS PAYMENTS C = PERCENTAGE OF COMPLETION PROGRESS PAYMEN |
| 66 | `cost_or_pricing_data` | `string` | `cost_or_pricing_data` (BULK+FRESH) | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Cost or Pricing Data Field. | Y = YES N = NO W = NOT OBTAINED - WAIVED |
| 67 | `dod_claimant_program_code` | `string` | `dod_claimant_program_code` (BULK+FRESH) | A claimant program number designates a grouping of supplies, construction, or other services. | According to the GSA Federal Procurement Data System (FPDS), these are listed in |
| 68 | `inherently_governmental_functions` | `string` | BULK `inherently_government_func` · FRESH/MO `inherently_governmental_functions` | Indicates the type of the "Inherently Governmental Function" used on the action. | CL = CLOSELY ASSOCIATED CT = CRITICAL FUNCTIONS OT = OTHER FUNCTIONS CL,CT = CLO |
| 69 | `purchase_card_as_payment_method_code` | `string` | BULK `purchase_card_as_payment_m` · FRESH/MO `purchase_card_as_payment_method_code` | Indicates whether the method of payment is the Purchase Card. Agencies may issue formal contract documents and make payment using the Purchase Card. It is also permitted that agencies may report Purch | Y = YES N = NO |
| 70 | `clinger_cohen_act_planning_code` | `string` | BULK `clinger_cohen_act_planning` · FRESH/MO `clinger_cohen_act_planning_code` | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Clinger-Cohen Act Planning Compliance Field. | Y = Yes N = No |
| 71 | `national_interest_action_code` | `string` | BULK `national_interest_action` · FRESH/MO `national_interest_action_code` | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the National Interest Action Field. | NONE = NONE H05K = HURRICANE KATRINA 2005 H05O = HURRICANE OPHELIA 2005 H05R = H |
| 72 | `domestic_or_foreign_entity_code` | `string` | BULK `domestic_or_foreign_entity` · FRESH/MO `domestic_or_foreign_entity_code` | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Domestic or Foreign Entity Field. | A = U.S. OWNED BUSINESS B = OTHER U.S. ENTITY (E.G. GOVERNMENT) C = FOREIGN-OWNE |
| 73 | `price_evaluation_adjustment_preference_percent_difference` | `string` | BULK `price_evaluation_adjustmen` · FRESH/MO `price_evaluation_adjustment_preference_percent_difference` | The percent difference between the award price and the lowest priced offer from a responsive, responsible non-HUBZone or non-SDB. | — |
| 74 | `place_of_performance_code` | `string` | BULK-only `place_of_performance_code` | A numeric code indicating where the predominant performance of the award will be accomplished. | 00***** = Multi-State 00FORGN = Foreign XX00000 = Single ZIP code XX##### = City |
| 75 | `place_of_performance_scope` | `string` | BULK-only `place_of_performance_scope` | A description of the geographic area to which the predominant performance of the award is applicable. | Multi-State State-wide County-wide City-wide Single ZIP code Foreign |
| 76 | `place_of_performance_forei` | `string` | BULK-only `place_of_performance_forei` | For foreign places of performance: identify where the predominant performance of the award will be accomplished, describing it as specifically as possible. | — |
| 77 | `pop_city_name` | `string` | BULK-only `pop_city_name` | — | — |
| 78 | `funding_office_code` | `string` | `funding_office_code` (BULK+FRESH) | Identifier of the level n organization that provided the preponderance of the funds obligated by this transaction. | Refer to the GSA IAE Federal Hierarchy from SAM.gov |
| 79 | `funding_office_name` | `string` | `funding_office_name` (BULK+FRESH) | Name of the level n organization that provided the preponderance of the funds obligated by this transaction. | Refer to the GSA IAE Federal Hierarchy from SAM.gov |
| 80 | `funding_sub_agency_code` | `string` | BULK `funding_sub_tier_agency_co` · FRESH/MO `funding_sub_agency_code` | Identifier of the level 2 organization that provided the preponderance of the funds obligated by this transaction. | See https://files.usaspending.gov/reference_data/agency_codes.csv (SUBTIER CODE |
| 81 | `funding_sub_agency_name` | `string` | BULK `funding_subtier_agency_name` · FRESH/MO `funding_sub_agency_name` | Name of the level 2 organization that provided the preponderance of the funds obligated by this transaction. | See https://files.usaspending.gov/reference_data/agency_codes.csv (SUBTIER NAME |
| 82 | `transaction_number` | `string` | `transaction_number` (BULK+FRESH) | Tie Breaker for legal, unique transactions that would otherwise have the same key. | — |
| 83 | `ordering_period_end_date` | `date32[day]` | `ordering_period_end_date` (BULK+FRESH) | For procurement, the date on which, for the award referred to by the action being reported, no additional orders referring to it may be placed. This date applies only to procurement indefinite deliver | — |
| 84 | `solicitation_date` | `date32[day]` | `solicitation_date` (BULK+FRESH) | For award of a new contract, purchase order, task\delivery order, or BPA Call valued above the SAT a solicitation issuance date must be provided, regardless of whether the new award: Was required to b | — |
| 85 | `total_dollars_obligated` | `double` | BULK `total_obligated_amount` · FRESH/MO `total_dollars_obligated` | This is a system generated element providing the sum of all the amounts entered in the "Action Obligation" field for a particular PIID and Agency. Example: Contract has 9 Modifications under "Transact | — |
| 86 | `base_and_exercised_options_value` | `double` | BULK `base_exercised_options_val` · FRESH/MO `base_and_exercised_options_value` | The contract value for the base contract and any options that have been exercised. | — |
| 87 | `number_of_offers_received` | `int64` | `number_of_offers_received` (BULK+FRESH) | The number of actual offers/bids received in response to the solicitation. | — |
| 88 | `number_of_actions` | `int64` | `number_of_actions` (BULK+FRESH) | The number input by the agency that identifies number of actions that are reported in one modification. | — |

### 3.3 Enrichment — BULK/pg only (overwritten from the reconciled base, key-independent of the core winner)  (27)

| # | canonical column | type | source | definition | domain / codes |
|---|---|---|---|---|---|
| 1 | `recipient_hash` | `string` | BULK-only `recipient_hash` | — | — |
| 2 | `recipient_levels` | `string` | BULK-only `recipient_levels` | — | — |
| 3 | `parent_uei` | `string` | BULK-only `parent_uei` | — | — |
| 4 | `parent_recipient_hash` | `string` | BULK-only `parent_recipient_hash` | — | — |
| 5 | `parent_recipient_name` | `string` | BULK-only `parent_recipient_name` | — | — |
| 6 | `business_categories` | `string` | BULK-only `business_categories` | — | — |
| 7 | `business_types` | `string` | BULK-only `business_types` | A code of a collection of indicators of different types of recipients based on socio-economic status and organization / business areas. | — |
| 8 | `federal_accounts` | `string` | BULK-only `federal_accounts` | The Federal Account Symbol is derived from concatenating the agency identifier and the main account code. | — |
| 9 | `treasury_account_identifiers` | `int64` | BULK-only `treasury_account_identifiers` | — | — |
| 10 | `tas_paths` | `string` | BULK-only `tas_paths` | — | — |
| 11 | `disaster_emergency_fund_codes` | `string` | BULK-only `disaster_emergency_fund_codes` | Distinguishes whether the budgetary resources, obligations incurred, unobligated and obligated balances, and outlays are classified as disaster, emergency, wildfire suppression or none of the three. ( | — |
| 12 | `program_activities` | `string` | BULK-only `program_activities` | A single field with associated program activities in order of funding dollars. | — |
| 13 | `cfda_number` | `string` | BULK-only `cfda_number` | The number assigned to an Assistance Listing in the Catalog of Federal Domestic Assistance (CFDA) and SAM.gov. | — |
| 14 | `awarding_toptier_agency_abbreviation` | `string` | BULK-only `awarding_toptier_agency_abbreviation` | — | — |
| 15 | `awarding_subtier_agency_abbreviation` | `string` | BULK-only `awarding_subtier_agency_abbreviation` | — | — |
| 16 | `awarding_agency_id` | `int64` | BULK-only `awarding_agency_id` | — | — |
| 17 | `funding_agency_id` | `int64` | BULK-only `funding_agency_id` | — | — |
| 18 | `award_id` | `int64` | BULK-only `award_id` | The unique identifying Award ID of the prime award (PIID or FAIN). | — |
| 19 | `transaction_id` | `int64` | BULK-only `transaction_id` | — | — |
| 20 | `recipient_location_zip5` | `string` | BULK-only `recipient_location_zip5` | — | — |
| 21 | `recipient_location_county_fips` | `string` | BULK-only `recipient_location_county_fips` | — | — |
| 22 | `recipient_location_congressional_code` | `string` | BULK-only `recipient_location_congressional_code` | — | — |
| 23 | `pop_county_fips` | `string` | BULK-only `pop_county_fips` | — | — |
| 24 | `pop_congressional_code` | `string` | BULK-only `pop_congressional_code` | — | — |
| 25 | `award_category` | `string` | BULK-only `award_category` | — | — |
| 26 | `award_amount` | `double` | BULK-only `award_amount` | The total amount awarded to the prime award recipient. | — |
| 27 | `total_funding_amount` | `double` | BULK-only `total_funding_amount` | The sum of the FederalActionObligation and the Non-Federal Funding Amount. | — |

### 3.4 Enrichment — monthly-unique (Treasury / federal-account funding + highly-compensated-officer comp; pg lacks all 12)  (12)

| # | canonical column | type | source | definition | domain / codes |
|---|---|---|---|---|---|
| 1 | `treasury_accounts_funding_this_award` | `string` | FRESH/MO-only `treasury_accounts_funding_this_award` | — | — |
| 2 | `federal_accounts_funding_this_award` | `string` | FRESH/MO-only `federal_accounts_funding_this_award` | — | — |
| 3 | `highly_compensated_officer_1_name` | `string` | FRESH/MO-only `highly_compensated_officer_1_name` | The name of an individual identified as one of the five most highly compensated "Executives." "Executive" means officers, managing partners, or any other employees in management positions. | — |
| 4 | `highly_compensated_officer_2_name` | `string` | FRESH/MO-only `highly_compensated_officer_2_name` | The name of an individual identified as one of the five most highly compensated "Executives." "Executive" means officers, managing partners, or any other employees in management positions. | — |
| 5 | `highly_compensated_officer_3_name` | `string` | FRESH/MO-only `highly_compensated_officer_3_name` | The name of an individual identified as one of the five most highly compensated "Executives." "Executive" means officers, managing partners, or any other employees in management positions. | — |
| 6 | `highly_compensated_officer_4_name` | `string` | FRESH/MO-only `highly_compensated_officer_4_name` | The name of an individual identified as one of the five most highly compensated "Executives." "Executive" means officers, managing partners, or any other employees in management positions. | — |
| 7 | `highly_compensated_officer_5_name` | `string` | FRESH/MO-only `highly_compensated_officer_5_name` | The name of an individual identified as one of the five most highly compensated "Executives." "Executive" means officers, managing partners, or any other employees in management positions. | — |
| 8 | `highly_compensated_officer_1_amount` | `double` | FRESH/MO-only `highly_compensated_officer_1_amount` | The cash and noncash dollar value earned by the one of the five most highly compensated “Executives” during the awardee's preceding fiscal year and includes the following (for more information see 17 | — |
| 9 | `highly_compensated_officer_2_amount` | `double` | FRESH/MO-only `highly_compensated_officer_2_amount` | The cash and noncash dollar value earned by the one of the five most highly compensated “Executives” during the awardee's preceding fiscal year and includes the following (for more information see 17 | — |
| 10 | `highly_compensated_officer_3_amount` | `double` | FRESH/MO-only `highly_compensated_officer_3_amount` | The cash and noncash dollar value earned by the one of the five most highly compensated “Executives” during the awardee's preceding fiscal year and includes the following (for more information see 17 | — |
| 11 | `highly_compensated_officer_4_amount` | `double` | FRESH/MO-only `highly_compensated_officer_4_amount` | The cash and noncash dollar value earned by the one of the five most highly compensated “Executives” during the awardee's preceding fiscal year and includes the following (for more information see 17 | — |
| 12 | `highly_compensated_officer_5_amount` | `double` | FRESH/MO-only `highly_compensated_officer_5_amount` | The cash and noncash dollar value earned by the one of the five most highly compensated “Executives” during the awardee's preceding fiscal year and includes the following (for more information see 17 | — |

### 3.5 Provenance  (2)

| # | canonical column | type | source | definition | domain / codes |
|---|---|---|---|---|---|
| 1 | `canonical_source` | `string` | derived per key (winning source tag) | Pipeline provenance — which upstream feed won this row's volatile core: one of `fresh`, `bulk`, or `monthly`. | — |
| 2 | `built_at` | `timestamp[us]` | injected UTC literal | Pipeline provenance — a single UTC build-timestamp literal injected into every row of a given build. | — |

## 4. Index topology (17)

All scalar indices. **BTREE** for high-cardinality / temporal / point-lookup columns; **BITMAP** for low-cardinality categoricals (cheap equality/`IN` bitset filters). Index name convention: `<column>_idx`. Rebuilt in-RAM (`LANCE_BYPASS_SPILLING=true`) — a 108M-row external merge sort OOMs the DataFusion spill pool.

| # | column | type | index name |
|---|---|---|---|
| 1 | `contract_transaction_unique_key` | BTREE | `contract_transaction_unique_key_idx` |
| 2 | `contract_award_unique_key` | BTREE | `contract_award_unique_key_idx` |
| 3 | `recipient_uei` | BTREE | `recipient_uei_idx` |
| 4 | `action_date` | BTREE | `action_date_idx` |
| 5 | `last_modified_date` | BTREE | `last_modified_date_idx` |
| 6 | `naics_code` | BTREE | `naics_code_idx` |
| 7 | `product_or_service_code` | BTREE | `product_or_service_code_idx` |
| 8 | `federal_action_obligation` | BTREE | `federal_action_obligation_idx` |
| 9 | `recipient_hash` | BTREE | `recipient_hash_idx` |
| 10 | `award_id_piid` | BTREE | `award_id_piid_idx` |
| 11 | `action_date_fiscal_year` | BITMAP | `action_date_fiscal_year_idx` |
| 12 | `type_of_set_aside_code` | BITMAP | `type_of_set_aside_code_idx` |
| 13 | `awarding_agency_code` | BITMAP | `awarding_agency_code_idx` |
| 14 | `award_type_code` | BITMAP | `award_type_code_idx` |
| 15 | `idv_type_code` | BITMAP | `idv_type_code_idx` |
| 16 | `canonical_source` | BITMAP | `canonical_source_idx` |
| 17 | `subcontracting_plan` | BITMAP | `subcontracting_plan_idx` |

## 5. Reconciliation semantics

Two-tier logical merge, one physical artifact. Executed as a single flat 3-way `row_number()` window (argmax is associative, so the flat window is byte-identical to the explicit two tiers):

- **Core (volatile) columns** — resolved per key by **argmax(`last_modified_date`)** across the three per-key-collapsed feeds. Cross-source tie precedence: **FRESH (1) > MONTHLY (2) > BULK (3)**. The winning feed's tag is recorded in **`canonical_source ∈ {fresh, bulk, monthly}`**.
- **Enrichment columns** — filled **independently of the core winner**, pg-preferred from the reconciled base. The 27 pg-only enrichment cols come straight from `bulk_latest`; the 12 monthly-unique cols are `COALESCE(pg, monthly)` from a separate enrichment-populatedness dedup (pg currently lacks all 12, so they resolve to monthly — the `COALESCE` future-proofs a pg schema add).
- **Deletes / reinstatement** — a `D` key in the DELTA ledger is honored only when scoped to the latest `archive_snapshot_stamp` and the reconciled winner is not strictly newer than the delete (R6 tombstone with R5 reinstatement).
- **PK-uniqueness** is structural (`row_number()=1` over ≤1-row-per-source collapses) and fail-closed-gated before publish.

`subcontracting_plan` carries the raw FPDS code (`A`–`H`); the `has_subcontracting_plan` boolean (`IN ('C','D','E','F','G','H')`) is derived downstream in serving, not on the spine.

## 6. Documented in BULK but NOT carried on the spine (261)

These native columns exist in the USAspending BULK source / FPDS Data Dictionary but are **not projected** onto the canonical spine. To carry one, add an entry to `COLUMN_SPEC` (canonical name, type, group, `bulk_expr`/`feed_expr`) and rebuild — do not hand-edit this doc. Definitions are from the committed sidecar (may be abbreviated).

| native column | type | definition |
|---|---|---|
| `a_76_fair_act_action` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the A-76 FAIR Act Action Field. |
| `a_76_fair_act_action_desc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the A-76 FAIR Act Action Field. |
| `action_type_description` | string | Description tag that explains the meaning of the code provided in the ActionType Field. |
| `afa_generated_unique` | string | System-generated database key used to uniquely identify each financial assistance transaction record and facilitate record lookup, correction, and deletion. A concatenation of AwardingSubTierAgencyCod |
| `agency_id` | string | The agency code identifies the department or agency that is responsible for the account. |
| `airport_authority` | bool | https://www.sam.gov |
| `alaskan_native_owned_corpo` | bool | https://www.sam.gov |
| `alaskan_native_servicing_i` | bool | https://www.sam.gov |
| `american_indian_owned_busi` | bool | List characteristic of the contractor such as whether the selected contractor is an American Indian Owned Business or not. It can be derived from the SAM data element, 'Business Types'. |
| `asian_pacific_american_own` | bool | List characteristic of the contractor such as whether the selected contractor is an Asian-Pacific American Owned Business or not. It can be derived from the SAM data element, 'Business Types'. |
| `award_certified_date` | date32[day] | — |
| `award_date_signed` | date32[day] | — |
| `award_fiscal_year` | int64 | — |
| `award_update_date` | timestamp[us] | — |
| `awarding_office_code` | string | Identifier of the level n organization that awarded, executed or is otherwise responsible for the transaction. |
| `awarding_office_name` | string | Name of the level n organization that awarded, executed or is otherwise responsible for the transaction. |
| `awarding_subtier_agency_name_raw` | string | — |
| `awarding_toptier_agency_id` | int64 | — |
| `awarding_toptier_agency_name_raw` | string | — |
| `black_american_owned_busin` | bool | List characteristic of the contractor such as whether the selected contractor is a Black American Owned Business or not. It can be derived from the SAM data element, 'Business Types'. |
| `business_funds_ind_desc` | string | Description tag (by way of the DATA Act Broker) that explains the meaning of the code provided in the BusinessFundsIndicator Field. |
| `business_funds_indicator` | string | The Business Funds Indicator sometimes abbreviated BFI. Code indicating the award's applicability to the Recovery Act. |
| `business_types_desc` | string | Description tag (by way of the DATA Act Broker) that explains the meaning of the code provided in the BusinessType Field. |
| `c1862_land_grant_college` | bool | https://www.sam.gov |
| `c1890_land_grant_college` | bool | https://www.sam.gov |
| `c1994_land_grant_college` | bool | https://www.sam.gov |
| `cfda_id` | int64 | — |
| `cfda_title` | string | The title of the Assistance Listing under which the Federal award was funded in the Catalog of Federal Domestic Assistance (CFDA) and SAM.gov. |
| `city_local_government` | bool | https://www.sam.gov |
| `clinger_cohen_act_pla_desc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Clinger-Cohen Act Planning Compliance Field. |
| `commercial_item_acqui_desc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Commercial Item Acquisition Procedures Field. |
| `commercial_item_test_desc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Commercial Item Test Program Field. |
| `commercial_item_test_progr` | string | This field designates whether the acquisition utilized FAR 13.5 Test Program for Certain Commercial Items. The FAR 13.5 Test Program provides for the use of simplified acquisition procedures for the a |
| `community_developed_corpor` | bool | https://www.sam.gov |
| `community_development_corp` | bool | https://www.sam.gov |
| `consolidated_contract_desc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Consolidated Contract Field. |
| `construction_wage_rat_desc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Wage Rate Requirements (Construction) Field. |
| `contingency_humanitar_desc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Emergency Acquisition Field. |
| `contingency_humanitarian_o` | string | A designator of contract actions that support a declared contingency operation, a declared humanitarian or peacekeeping operation, or a declared presidential issued emergency declaration or a major di |
| `contract_award_type_desc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the ContractAwardType Field. |
| `contract_bundling_descrip` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Contract Bundling Field. |
| `contract_financing_descrip` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Contract Financing Field. |
| `contracting_officers_desc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Contracting Officer's Determination of Business Size Field. |
| `contracts` | bool | https://www.sam.gov |
| `corporate_entity_not_tax_e` | bool | https://www.sam.gov |
| `corporate_entity_tax_exemp` | bool | https://www.sam.gov |
| `correction_delete_ind_desc` | string | Description tag (by way of the DATA Act Broker) that explains the meaning of the code provided in the CorrectionDeleteIndicator Field. |
| `correction_delete_indicatr` | string | A code to indicate how the record should be processed: correction to an existing record; deletion of a record; new record. |
| `cost_accounting_stand_desc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Cost Accounting Standards Clause Field. |
| `cost_accounting_standards` | string | Indicates whether the contract includes a Cost Accounting Standards clause. |
| `cost_or_pricing_data_desc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Cost or Pricing Data Field. |
| `council_of_governments` | bool | https://www.sam.gov |
| `country_of_product_or_desc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Country of Product or Service Origin Field. |
| `country_of_product_or_serv` | string | Identifies the country of product or service origin. |
| `county_local_government` | bool | https://www.sam.gov |
| `create_date` | timestamp[us] | — |
| `detached_award_procurement_id` | int64 | — |
| `dod_claimant_prog_cod_desc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the DoD Claimant Program Code Field. |
| `domestic_or_foreign_e_desc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Domestic or Foreign Entity Field. |
| `domestic_shelter` | bool | https://www.sam.gov |
| `dot_certified_disadvantage` | bool | https://www.sam.gov |
| `economically_disadvantaged` | bool | https://www.sam.gov OR List characteristic of the contractor such as whether the selected contractor is an Economically Disadvantaged Woman Owned Small Business or not. It can be derived from the SAM |
| `educational_institution` | bool | List characteristic of the contractor such as whether the selected contractor is an Educational Institution or not. It can be derived from the SAM data element, 'Business Types'. |
| `emerging_small_business` | bool | List characteristic of the contractor such as whether the selected contractor is an Emerging Small Business Organization or not. It can be derived from the SAM data element, 'Business Types'. |
| `epa_designated_produc_desc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the EPA-Designated Product Field. |
| `epa_designated_product` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the EPA-Designated Product Field. |
| `etl_update_date` | timestamp[us] | — |
| `evaluated_preference` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Evaluated Preference Field. |
| `evaluated_preference_desc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Evaluated Preference Field. |
| `extent_compete_description` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Extent Competed Field. |
| `face_value_loan_guarantee` | double | The face value of the direct loan or loan guarantee. |
| `fain` | string | The Federal Award Identification Number (FAIN) is the unique ID within the Federal agency for each (non-aggregate) financial assistance award. |
| `fair_opportunity_limi_desc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Fair Opportunity Limited Sources Field. |
| `fed_biz_opps` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the FedBizOpps Field. |
| `fed_biz_opps_description` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the FedBizOpps Field. |
| `federal_agency` | bool | https://www.sam.gov |
| `federally_funded_research` | bool | https://www.sam.gov |
| `fiscal_action_date` | date32[day] | — |
| `for_profit_organization` | bool | List characteristic of the contractor such as whether the selected contractor is a Profit Organization or not. It can be derived from the SAM data element, 'Business Types'. |
| `foreign_funding` | string | Indicates that a foreign government, international organization, or foreign military organization bears some of the cost of the acquisition. |
| `foreign_funding_desc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Foreign Funding Field. |
| `foreign_government` | bool | https://www.sam.gov |
| `foreign_owned_and_located` | bool | https://www.sam.gov |
| `foundation` | bool | https://www.sam.gov |
| `funding_amount` | double | — |
| `funding_opportunity_goals` | string | A brief summary of the intended outcomes associated with the notice of funding opportunity. Applicable to Competitive Discretionary Grants and Cooperative Agreements. |
| `funding_opportunity_number` | string | An alphanumeric identifier that a Federal agency assigns to its funding opportunity announcement as part of the Notice of Funding Opportunity posted on the OMB-designated government wide web site (cur |
| `funding_subtier_agency_abbreviation` | string | — |
| `funding_subtier_agency_name_raw` | string | — |
| `funding_toptier_agency_abbreviation` | string | — |
| `funding_toptier_agency_id` | int64 | — |
| `funding_toptier_agency_name_raw` | string | — |
| `generated_pragmatic_obligation` | double | — |
| `government_furnished_desc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Government Furnished Property GFP Field. |
| `government_furnished_prope` | string | The contract uses equipment or property furnished by the government, pursuant to FAR 45. |
| `grants` | bool | https://www.sam.gov |
| `hispanic_american_owned_bu` | bool | List characteristic of the contractor such as whether the selected contractor is a Hispanic American Owned Business or not. It can be derived from the SAM data element, 'Business Types'. |
| `hispanic_servicing_institu` | bool | https://www.sam.gov |
| `historically_black_college` | bool | List characteristic of the contractor such as whether the selected contractor is a Historically Black College or University or not. It can be derived from the SAM data element, 'Business Types'. |
| `hospital_flag` | bool | List characteristic of the contractor such as whether the selected contractor is a Hospital or not. It can be derived from the SAM data element, 'Business Types' |
| `housing_authorities_public` | bool | https://www.sam.gov |
| `idv_type_description` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the IDV_Type Field. |
| `indian_tribe_federally_rec` | bool | https://www.sam.gov |
| `indirect_federal_sharing` | double | The total amount of any single Federal award action that is allocated, per the award recipient’s approved award budget, to indirect costs. |
| `information_technolog_desc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Information Technology Commercial Item Category Field. |
| `information_technology_com` | string | A code that designates the commercial availability of an information technology product or service. |
| `ingested_at` | timestamp[us, tz=Etc/UTC] | — |
| `inherently_government_desc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Inherently Governmental Functions field. |
| `initial_report_date` | timestamp[us] | — |
| `inter_municipal_local_gove` | bool | https://www.sam.gov |
| `interagency_contract_desc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Interagency Contracting Authority Field. |
| `interagency_contracting_au` | string | Indicates whether the transaction is an Economy Act or Statutory Authority. |
| `international_organization` | bool | https://www.sam.gov |
| `interstate_entity` | bool | https://www.sam.gov |
| `is_fpds` | bool | — |
| `joint_venture_economically` | bool | https://www.sam.gov OR List characteristic of the contractor such as whether the selected contractor is a Joint Venture Economically Disadvantaged Woman Owned Small Business or not. It can be derived |
| `joint_venture_women_owned` | bool | https://www.sam.gov OR List characteristic of the contractor such as whether the selected contractor is a Joint Venture Woman Owned Small Business or not. It can be derived from the SAM data element, |
| `labor_standards_descrip` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Service Contract Labor Standards Field. |
| `labor_surplus_area_firm` | bool | https://www.sam.gov |
| `legal_entity_address_line2` | string | Second line of awardee or recipient’s legal business address. |
| `legal_entity_address_line3` | string | — |
| `legal_entity_city_code` | string | Five position city code from the validation authoritative list. |
| `legal_entity_foreign_city` | string | For foreign recipients only: name of the city in which the awardee or recipient’s legal business address is located. |
| `legal_entity_foreign_descr` | string | — |
| `legal_entity_foreign_posta` | string | For foreign recipients only: foreign postal code in which the awardee or recipient's legal business address is located. |
| `legal_entity_foreign_provi` | string | For foreign recipients only: name of the state or province in which the awardee or recipient’s legal business address is located. |
| `legal_entity_zip4` | string | USPS zoning code associated with the awardee or recipient’s legal business address. For domestic recipients only. |
| `legal_entity_zip_last4` | string | USPS four digit extension code associated with the awardee or recipient’s legal business address. This must be blank for non-US addresses |
| `limited_liability_corporat` | bool | https://www.sam.gov |
| `local_area_set_aside` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Local Area Set Aside Field. |
| `local_area_set_aside_desc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Local Area Set Aside Field. |
| `local_government_owned` | bool | https://www.sam.gov |
| `manufacturer_of_goods` | bool | https://www.sam.gov |
| `materials_supplies_article` | string | Indicates whether the transaction is subject to the Materials, Supplies, Articles, & Equip. The clause is 52.222-20 "Contracts for Materials, Supplies, Articles, and Equipment Exceeding $15,000" - tha |
| `materials_supplies_descrip` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Contracts for Materials, Supplies, Articles, and Equipment Exceeding $15,000 Field. |
| `minority_institution` | bool | List characteristic of the contractor such as whether the selected contractor is a Minority Institution or not. It can be derived from the SAM data element, 'Business Types'. |
| `minority_owned_business` | bool | https://www.sam.gov |
| `multi_year_contract_desc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Multi Year Contract Field. |
| `multiple_or_single_aw_desc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Multiple or Single Award IDV Field. |
| `municipality_local_governm` | bool | https://www.sam.gov |
| `national_interest_desc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the National Interest Action Field. |
| `native_american_owned_busi` | bool | List characteristic of the contractor such as whether the selected contractor is a Native American Owned Business or not. It can be derived from the SAM data element, 'Business Types'. |
| `native_hawaiian_owned_busi` | bool | https://www.sam.gov |
| `native_hawaiian_servicing` | bool | https://www.sam.gov |
| `non_federal_funding_amount` | double | The amount of the award funded by non-Federal source(s), in dollars. Program Income (as defined in 2 CFR § 200.1) is not included until such time that Program Income is generated and credited to the a |
| `nonprofit_organization` | bool | List characteristic of the contractor such as whether the selected contractor is a Nonprofit Organization or not. It can be derived from the SAM data element, 'Business Types'. |
| `officer_1_amount` | double | The cash and noncash dollar value earned by the one of the five most highly compensated “Executives” during the awardee's preceding fiscal year and includes the following (for more information see 17 |
| `officer_1_name` | string | The name of an individual identified as one of the five most highly compensated "Executives." "Executive" means officers, managing partners, or any other employees in management positions. |
| `officer_2_amount` | double | The cash and noncash dollar value earned by the one of the five most highly compensated “Executives” during the awardee's preceding fiscal year and includes the following (for more information see 17 |
| `officer_2_name` | string | The name of an individual identified as one of the five most highly compensated "Executives." "Executive" means officers, managing partners, or any other employees in management positions. |
| `officer_3_amount` | double | The cash and noncash dollar value earned by the one of the five most highly compensated “Executives” during the awardee's preceding fiscal year and includes the following (for more information see 17 |
| `officer_3_name` | string | The name of an individual identified as one of the five most highly compensated "Executives." "Executive" means officers, managing partners, or any other employees in management positions. |
| `officer_4_amount` | double | The cash and noncash dollar value earned by the one of the five most highly compensated “Executives” during the awardee's preceding fiscal year and includes the following (for more information see 17 |
| `officer_4_name` | string | The name of an individual identified as one of the five most highly compensated "Executives." "Executive" means officers, managing partners, or any other employees in management positions. |
| `officer_5_amount` | double | The cash and noncash dollar value earned by the one of the five most highly compensated “Executives” during the awardee's preceding fiscal year and includes the following (for more information see 17 |
| `officer_5_name` | string | The name of an individual identified as one of the five most highly compensated "Executives." "Executive" means officers, managing partners, or any other employees in management positions. |
| `organizational_type` | string | The structure of the entity as defined by the IRS. |
| `original_loan_subsidy_cost` | double | The estimated long-term cost to the Government of a direct loan or loan guarantee, or modification thereof, calculated on a net present value basis, excluding administrative costs. |
| `other_minority_owned_busin` | bool | https://www.sam.gov |
| `other_not_for_profit_organ` | bool | https://www.sam.gov |
| `other_statutory_authority` | string | Indicates whether the transaction is subject to other statutory authority. If "Interagency Contracting Authority" is "Other Statutory Authority" then an entry is required in this data element. |
| `other_than_full_and_o_desc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Other than Full and Open Competition Field. |
| `parent_recipient_name_raw` | string | — |
| `parent_recipient_unique_id` | string | — |
| `partnership_or_limited_lia` | bool | https://www.sam.gov |
| `performance_based_se_desc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Performance-Based Service Acquisition Field. |
| `period_of_perf_potential_e` | string | For procurement, the date on which, for the award referred to by the action being reported if all potential pre-determined or pre-negotiated options were exercised, awardee effort is completed or the |
| `place_of_manufacture` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Place of Manufacture Field. |
| `place_of_manufacture_desc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Place of Manufacture Field. |
| `place_of_perform_zip_last4` | string | — |
| `place_of_performance_zip4a` | string | United States ZIP code (five digits) concatenated with the additional +4 digits, identifying where the predominant performance of the award will be accomplished. |
| `planning_commission` | bool | https://www.sam.gov |
| `pop_congressional_code_current` | string | — |
| `pop_congressional_population` | int64 | — |
| `pop_country_name` | string | — |
| `pop_county_code` | string | — |
| `pop_county_name` | string | — |
| `pop_county_population` | int64 | — |
| `pop_state_fips` | string | — |
| `pop_state_name` | string | — |
| `pop_state_population` | int64 | — |
| `port_authority` | bool | https://www.sam.gov |
| `potential_total_value_awar` | string | For procurement, the total amount that could be obligated on a contract, if the base and all options are exercised. |
| `private_university_or_coll` | bool | https://www.sam.gov |
| `program_system_or_equ_desc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the DOD Acquisition Program field. |
| `program_system_or_equipmen` | string | Two codes that together identify the program and weapons system or equipment purchased by a DoD agency. The first character is a number 1-4 that identifies the DoD component. The last 3 characters ide |
| `published_fabs_id` | int64 | — |
| `pulled_from` | string | Flag indicating whether the record was pulled from the award Atom feed or the IDV (Indefinite Delivery Vehicle) Atom Feed provided by FPDS. |
| `purchase_card_as_paym_desc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Purchase Card as Payment Method Field. |
| `receives_contracts_and_gra` | bool | https://www.sam.gov |
| `recipient_location_congressional_code_current` | string | — |
| `recipient_location_congressional_population` | int64 | — |
| `recipient_location_country_name` | string | — |
| `recipient_location_county_code` | string | — |
| `recipient_location_county_population` | int64 | — |
| `recipient_location_state_fips` | string | — |
| `recipient_location_state_name` | string | — |
| `recipient_location_state_population` | int64 | — |
| `recipient_name_raw` | string | The name of the awardee or recipient that relates to the unique identifier. For U.S. based companies, this name is what the business ordinarily files in formation documents with individual states (whe |
| `recipient_unique_id` | string | — |
| `record_type` | int64 | Code indicating whether an action is an aggregate record, a non-aggregate record, or a non-aggregate record to an individual recipient (PII-Redacted). |
| `record_type_description` | string | Description tag (by way of the DATA Act Broker) that explains the meaning of the code provided in the RecordType Field. |
| `recovered_materials_s_desc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Recovered Materials/Sustainability Field. |
| `recovered_materials_sustai` | string | Designates whether Recovered Material Certification and/or Estimate of Percentage of Recovered Material Content for EPA-Designated Products clauses were included in the contract. |
| `referenced_idv_agency_desc` | string | Name of the agency associated with the code in the Referenced IDV Agency Identifier. |
| `referenced_idv_type_desc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Referenced_IDV_Type Field. |
| `referenced_mult_or_si_desc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Referenced IDV Multiple or Single Field. |
| `referenced_mult_or_single` | string | Indicates whether the contract of the referenced IDV is one of many that resulted from a single solicitation, all of the contracts are for the same or similar items, and contracting officers are requi |
| `research` | string | The designator for type of research determined for the contract action. |
| `research_description` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Research Field. |
| `sai_number` | string | A number assigned by state (as opposed to federal) review agencies to the award during the grant application process. |
| `sam_exception` | string | The reason a vendor/contractor not registered in the mandated SAM system may be used in a purchase. |
| `sam_exception_description` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the SAM Exception Field. |
| `sba_certified_8_a_joint_ve` | bool | https://www.sam.gov |
| `school_district_local_gove` | bool | https://www.sam.gov |
| `school_of_forestry` | bool | https://www.sam.gov |
| `sea_transportation` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Sea Transportation Field. |
| `sea_transportation_desc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Sea Transportation Field. |
| `self_certified_small_disad` | bool | https://www.sam.gov |
| `small_agricultural_coopera` | bool | https://www.sam.gov |
| `small_business_competitive` | bool | Indicates whether the contract was awarded to a U.S. business concern as a result of a solicitation issued on or after Jan 1, 1989 for the four designated industry groups or the ten targeted industry |
| `small_disadvantaged_busine` | bool | List characteristic of the contractor such as whether the selected contractor is a Small Disadvantaged Business Organization or not. It can be derived from the SAM data element, 'Business Types'. |
| `sole_proprietorship` | bool | https://www.sam.gov |
| `solicitation_procedur_desc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Solicitation Procedures Field. |
| `source_schema` | string | — |
| `source_table` | string | — |
| `state_controlled_instituti` | bool | https://www.sam.gov |
| `subchapter_s_corporation` | bool | https://www.sam.gov |
| `subcontinent_asian_asian_i` | bool | List characteristic of the contractor such as whether the selected contractor is a Subcontinent Asian (Asian- Indian) American Owned Business or not. It can be derived from the SAM data element, 'Busi |
| `subcontracting_plan_desc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Subcontracting Plan Field. |
| `tas_components` | string | — |
| `the_ability_one_program` | bool | List characteristic of the contractor such as whether the selected contractor is a Sheltered Workshop (JWOD Provider) Organization or not. It can be derived from the SAM data element, 'Business Types' |
| `township_local_government` | bool | https://www.sam.gov |
| `transit_authority` | bool | https://www.sam.gov |
| `tribal_college` | bool | https://www.sam.gov |
| `tribally_owned_business` | bool | https://www.sam.gov |
| `type` | string | — |
| `type_description` | string | — |
| `type_description_raw` | string | — |
| `type_of_contract_pric_desc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the TypeOfContractPricing Field. |
| `type_of_idc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Type of IDC Field. |
| `type_of_idc_description` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Type of IDC Field. |
| `type_raw` | string | — |
| `type_set_aside_description` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Type Set Aside Field. |
| `undefinitized_action_desc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Undefinitized Action Field. |
| `update_date` | timestamp[us] | — |
| `uri` | string | Unique Record Identifier. An agency defined identifier that (when provided) is unique for every financial assistance action reported by that agency. USAspending.gov and the Broker use URI as the Award |
| `us_federal_government` | bool | List characteristic of the contractor such as whether the selected contractor is a Federal Government Organization or not. It can be derived from the SAM data element, 'Business Types'. |
| `us_government_entity` | bool | https://www.sam.gov |
| `us_local_government` | bool | List characteristic of the contractor such as whether the selected contractor is a Local Government Organization or not. It can be derived from the SAM data element, 'Business Types'. |
| `us_state_government` | bool | List characteristic of the contractor such as whether the selected contractor is a State Government Organization or not. It can be derived from the SAM data element, 'Business Types'. |
| `us_tribal_government` | bool | List characteristic of the contractor such as whether the selected contractor is a Tribal Government Organization or not. It can be derived from the SAM data element, 'Business Types'. |
| `usaspending_snapshot_date` | date32[day] | — |
| `usaspending_unique_transaction_id` | string | — |
| `vendor_doing_as_business_n` | string | The doing business as name of the entity address. |
| `vendor_fax_number` | string | The fax number of the entity. |
| `vendor_phone_number` | string | The phone number of the entity. |
| `veteran_owned_business` | bool | List characteristic of the contractor such as whether the selected contractor is a Veteran Owned Business or not. It can be derived from the SAM data element, 'Business Types'. |
| `veterinary_college` | bool | https://www.sam.gov |
| `veterinary_hospital` | bool | https://www.sam.gov |
| `woman_owned_business` | bool | List characteristic of the contractor such as whether the selected contractor is a Woman Owned Business or not. It can be derived from the SAM data element, 'Business Types'. |

## 7. Regenerating this document

This doc is generated; the pipeline `COLUMN_SPEC` is the source of truth. After any spine change, regenerate (fail-closed against a live probe):

```bash
# 1. probe.json — dump live schema+indices+rowcount (read-only) from R2:
doppler run -p core-x -c prd -- uv run --no-project --with 'pylance>=7' python3 - <<'PY'
import os, json, lance
os.environ.setdefault("LANCE_BYPASS_SPILLING","true")
so={"aws_access_key_id":os.environ["R2_ACCESS_KEY_ID"],"aws_secret_access_key":os.environ["R2_SECRET_ACCESS_KEY"],"endpoint":os.environ["R2_ENDPOINT"],"region":"auto"}
ds=lance.dataset("s3://data-sink/active/usaspending_fpds_canonical_txn/",storage_options=so); sch=ds.schema
json.dump({"uri":ds.uri,"rows":ds.count_rows(),"ncols":len(sch),"n_indices":len(ds.list_indices()),"version":ds.version,"schema":[{"name":sch.field(i).name,"type":str(sch.field(i).type)} for i in range(len(sch))],"indices":ds.list_indices()},open("probe.json","w"),default=str,indent=2)
PY

# 2. regenerate (definitions come from the committed sidecar; no R2 needed for this step):
python3 -m pipelines.usaspending.gen_fpds_canonical_dictionary \
    --probe probe.json --verified-date $(date +%F)
```

To refresh **definitions** (e.g. after USAspending revises its Data Dictionary), rebuild the sidecar `pipelines/usaspending/fpds_field_definitions.json` (native column → `{definition, domain, type}`) and regenerate.
